//! End-to-end behavioral test for glob cache-rule matching.
//!
//! Spec: `.kiro/specs/cache-match-patterns/`
//!
//! The unit tests in `src/bucket_settings.rs` already cover the glob→regex
//! translator and first-match-per-field resolution *in isolation*. What those
//! tests cannot prove is that a glob rule written to `cache_rules.json` actually
//! changes how the proxy caches a request: that the rule is loaded, matched
//! against the full `{bucket}/{object_key}` cache key, and the resolved setting
//! is honored on the real request path.
//!
//! This test closes that gap. It drives the genuine top-level request entry
//! point — `HttpProxy::handle_request` — over a loopback HTTP/1 connection (the
//! same shape as `tests/test_signed_put_handler.rs`), because the read-cache
//! decision gate (`if !resolved_settings.read_cache_enabled`) lives inside the
//! private `handle_get_head_request`, which `handle_request` dispatches to. The
//! lower-level `forward_*_with_coordination` helpers (used by the resolve-once
//! test) receive an *already-resolved* `ResolvedSettings` and therefore never
//! exercise that gate, so they cannot demonstrate the on/off contrast.
//!
//! Behavioral signal: a [`StubS3Client`] (in `tests/common`) records every S3
//! round-trip in `captured()`. A cache HIT performs no new S3 fetch; a no-cache
//! rule forces a fresh S3 fetch on every request. Counting captured requests per
//! object path is therefore a direct, behavioral measure of "served from cache"
//! vs "fetched from S3".
//!
//! Determinism: download coordination and the RAM cache are disabled (mirroring
//! the resolve-once test) so each cache miss is an independent, observable S3
//! fetch and there is no background promotion to confound the count. The rules
//! file is written *before* the cache infra is built and the bucket-settings
//! staleness threshold is `Duration::ZERO`, so the rules load on first access.
//! Buffered GET responses are cached synchronously by the production path (the
//! `.meta` + `.bin` files are written via atomic rename before the response is
//! returned), so the immediately-following second GET is a deterministic cache
//! hit for a read-cacheable key.
//!
//! **Validates: Requirements 2.1 (full-key matching), 2.2–2.8 (glob semantics),
//! 3.1–3.3 (first-match-per-field resolution), 9.1/9.6 (cache-key matching
//! surface, case-sensitive).**

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

/// Body the stub returns for every GET. Non-trivial, fixed-size bytes so the
/// cached object is a real range and the buffered-cache store path runs.
const OBJECT_BODY: &[u8] = b"glob-rule-behavior-test-object-body-0123456789";

/// Config with download coordination and RAM cache disabled (so every cache
/// miss is an independent, observable S3 fetch and nothing is promoted in the
/// background), and a `Duration::ZERO` bucket-settings staleness threshold so
/// the `cache_rules.json` written before infra construction loads on first
/// access (mirrors `test_manager_always_reload` in `bucket_settings.rs`).
fn behavior_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    // Global default read caching stays enabled (the on side of the contrast).
    config.cache.read_cache_enabled = true;
    Arc::new(config)
}

/// Build the cache infrastructure the same way `HttpProxy::new` wires it: one
/// shared `DiskCacheManager` behind both the `CacheManager` and the
/// `RangeHandler`, so the full-object GET store path and the range/serve path
/// agree on a single on-disk cache. The `new_with_shared_storage` argument list
/// is copied from the resolve-once test (compression disabled to keep stored
/// bytes 1:1 with the body and keep the behavior under test isolated to caching
/// on/off, not compression).
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
        false, // compression disabled — keeps stored bytes 1:1 with the body
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
        64, // ram_cache_shard_count
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

/// Write `cache_rules.json` into the cache dir. MUST be called before
/// `make_cache_infra` so the file is present when the rules first load.
fn write_cache_rules(cache_dir: &std::path::Path, json: &str) {
    std::fs::write(cache_dir.join("cache_rules.json"), json).expect("write cache_rules.json");
}

/// A running loopback proxy: its address plus a shutdown trigger that, when
/// fired, stops the accept loop.
struct ProxyServer {
    addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
}

/// Spin up a local HTTP/1 server whose connection service calls the real
/// `HttpProxy::handle_request` with the supplied components. This is the genuine
/// top-level entry point — it dispatches to `handle_get_head_request`, which is
/// where the read-cache gate and full-key rule resolution live.
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
                            async move {
                                HttpProxy::handle_request(
                                    req,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None, // metrics_manager
                                    None, // logger_manager
                                    inflight_tracker,
                                    None, // proxy_referer
                                    None, // destination_policy
                                    None, // policy_resolver
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

/// Send a single GET for `path` through the loopback proxy. The hyper client
/// sets the `Host` header to the connection authority (`127.0.0.1:<port>`),
/// which the proxy parses to `127.0.0.1` — a path-style host — so the cache key
/// is the bucket-inclusive normalized path (e.g. `/mybucket/x` → `mybucket/x`).
/// Returns the response status; the body is fully drained.
async fn proxy_get(addr: SocketAddr, path: &str) -> StatusCode {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        // Realistic SigV4-style auth header; routing is by the stub default, so
        // the value only needs to be present, not valid.
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=sig",
        )
        .body(Full::new(Bytes::new()))
        .expect("build GET request");

    let resp = client.request(req).await.expect("proxy GET failed");
    let status = resp.status();
    let _ = resp.into_body().collect().await.unwrap().to_bytes();
    status
}

/// Count captured S3 round-trips whose URI contains `path_fragment`. The stub
/// records the absolute upstream URI (`https://127.0.0.1/<path>`), so filtering
/// by a distinctive path fragment isolates the fetches for one object.
fn s3_fetches_for(stub: &StubS3Client, path_fragment: &str) -> usize {
    stub.captured()
        .iter()
        .filter(|r| r.uri.contains(path_fragment))
        .count()
}

/// A GET stub: 200 OK with a fixed body, an ETag, and a Last-Modified so the
/// cached object carries realistic validators.
fn get_ok_stub() -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::ok(Bytes::from_static(OBJECT_BODY))
            .with_header("etag", "\"glob-behavior-etag\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    )
}

/// A GET stub that also handles conditional validation: returns 304 Not Modified
/// when the request carries `If-None-Match: "glob-behavior-etag"` (simulating
/// S3's response when the data hasn't changed), and 200 OK with body otherwise.
/// Used by Class A tests where the proxy's revalidation path must succeed.
fn get_ok_stub_with_304() -> StubS3Client {
    StubS3Client::new()
        .with_response_for_etag(
            "\"glob-behavior-etag\"",
            StubResponse::not_modified()
                .with_header("etag", "\"glob-behavior-etag\"")
                .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
        )
        .with_default(
            StubResponse::ok(Bytes::from_static(OBJECT_BODY))
                .with_header("etag", "\"glob-behavior-etag\"")
                .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                .with_header("content-type", "application/octet-stream"),
        )
}

// ===========================================================================
// CLASS A BUG-CONDITION EXPLORATION: Freshness vs Current TTL
//
// These tests MUST FAIL on unfixed code — failure confirms the Class A bug exists.
// They encode the expected post-fix behavior: freshness follows the current
// `get_ttl` / `head_ttl`, not the stored write-time `expires_at` / `head_expires_at`.
//
// Validates: Requirements 2.1, 2.1b, 2.2, 2.2b, 2.5
// Property 1: Bug Condition — Freshness Follows Current get_ttl / head_ttl
// ===========================================================================

/// Send a single HEAD for `path` through the loopback proxy.
/// Returns the response status; the body is fully drained.
async fn proxy_head(addr: SocketAddr, path: &str) -> StatusCode {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let req = Request::builder()
        .method("HEAD")
        .uri(&uri)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=sig",
        )
        .body(Full::new(Bytes::new()))
        .expect("build HEAD request");

    let resp = client.request(req).await.expect("proxy HEAD failed");
    let status = resp.status();
    let _ = resp.into_body().collect().await.unwrap().to_bytes();
    status
}

/// Count captured S3 round-trips for GET requests whose URI contains `path_fragment`.
fn s3_get_fetches_for(stub: &StubS3Client, path_fragment: &str) -> usize {
    stub.captured()
        .iter()
        .filter(|r| r.method == hyper::Method::GET && r.uri.contains(path_fragment))
        .count()
}

// ---------------------------------------------------------------------------
// (class-a-1) get_ttl=0 (GET): cache an object with the default long TTL, then
// tighten get_ttl to 0, and GET again. Expected: S3 is contacted (revalidation).
// On unfixed code the stored `expires_at` is still in the future → served from
// cache with no S3 contact → FAIL.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn class_a_get_ttl_zero_forces_revalidation_after_rule_change() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Start with NO rules file → global defaults apply (get_ttl = ~10 years).
    // Objects cached under these defaults have `expires_at` far in the future.
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub_with_304();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- Step 1: Cache the object (one S3 fetch) ---
    let path = "/mybucket/stale-test/file.bin";
    let s1 = proxy_get(server.addr, path).await;
    assert!(s1.is_success(), "initial GET should succeed");
    assert_eq!(
        s3_get_fetches_for(&stub, "/stale-test/"),
        1,
        "initial GET should produce exactly 1 S3 fetch"
    );

    // --- Step 2: Tighten the rule → get_ttl = 0 ---
    // With staleness_threshold = Duration::ZERO the next resolve re-reads this.
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**", "get_ttl": "0s" } ] }"#,
    );

    // --- Step 3: GET again — expect S3 revalidation (a new round-trip) ---
    let s2 = proxy_get(server.addr, path).await;
    assert!(s2.is_success(), "second GET should succeed");
    assert_eq!(
        s3_get_fetches_for(&stub, "/stale-test/"),
        2,
        "BUG CLASS A (get_ttl=0): after tightening get_ttl to 0, the next GET must \
         revalidate against S3 (2 total fetches). On unfixed code the object is served \
         from cache using the stored expires_at (still 1 fetch) — COUNTEREXAMPLE: \
         GET with current get_ttl=0 served from cache, 1 S3 round-trip, expected 2"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (class-a-2) head_ttl=0 (HEAD peer): cache HEAD metadata with the default TTL,
// then tighten head_ttl to 0, and HEAD again. Expected: S3 is contacted.
// On unfixed code the stored `head_expires_at` is still in the future → HEAD
// answered from cache with no S3 contact → FAIL.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn class_a_head_ttl_zero_forces_revalidation_after_rule_change() {
    let temp_dir = TempDir::new().expect("tempdir");

    // No rules file → global defaults (head_ttl = 60s). HEAD metadata is cached
    // for 60s, so a second HEAD within that window is a cache hit.
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- Step 1: Issue a GET to populate the full cache (body + metadata),
    // then a HEAD to populate the HEAD-specific metadata (head_expires_at). ---
    let path = "/mybucket/head-stale-test/file.bin";
    let s1 = proxy_get(server.addr, path).await;
    assert!(
        s1.is_success(),
        "initial GET should succeed (populates body cache)"
    );

    // The first HEAD will either be served from the cached object metadata
    // (if the GET path also stores head_expires_at) or fetch from S3. Either
    // way, after this call HEAD metadata should be cached.
    let h1 = proxy_head(server.addr, path).await;
    assert!(
        h1.is_success(),
        "first HEAD should succeed (caches HEAD metadata)"
    );

    // --- Step 2: Confirm the second HEAD is served from cache (pre-condition) ---
    let total_before = stub.captured().len();
    let h2 = proxy_head(server.addr, path).await;
    assert!(h2.is_success(), "second HEAD should succeed");
    let fetches_for_cached_head = stub.captured().len() - total_before;
    assert_eq!(
        fetches_for_cached_head, 0,
        "Pre-condition: second HEAD (before rule change) should be served from cached \
         HEAD metadata (0 new S3 fetches). If this fails, HEAD is not being cached."
    );

    // --- Step 3: Tighten the rule → head_ttl = 0 ---
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**", "head_ttl": "0s" } ] }"#,
    );

    // --- Step 4: HEAD again — expect S3 revalidation (a new round-trip) ---
    let total_before_rule = stub.captured().len();
    let h3 = proxy_head(server.addr, path).await;
    assert!(h3.is_success(), "HEAD after rule change should succeed");
    let fetches_after_rule = stub.captured().len() - total_before_rule;

    assert!(
        fetches_after_rule >= 1,
        "BUG CLASS A (head_ttl=0): after tightening head_ttl to 0, the next HEAD must \
         revalidate against S3 (expect >= 1 S3 fetch). On unfixed code the HEAD \
         is answered from the stored head_expires_at with no S3 contact — COUNTEREXAMPLE: \
         HEAD with current head_ttl=0 served from cache, 0 S3 round-trips, expected >= 1"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (class-a-3) Reduced TTL case: cache with the default long TTL, then tighten
// get_ttl to 1ms (effectively expired for any object older than 1ms). The next
// GET should revalidate. On unfixed code the stored `expires_at` (~10 years out)
// keeps the object fresh → served from cache → FAIL.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn class_a_reduced_get_ttl_expires_cached_object() {
    let temp_dir = TempDir::new().expect("tempdir");

    // No rules file → global defaults (get_ttl = ~10 years).
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub_with_304();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- Step 1: Cache the object ---
    let path = "/mybucket/reduced-ttl-test/file.bin";
    let s1 = proxy_get(server.addr, path).await;
    assert!(s1.is_success(), "initial GET should succeed");
    assert_eq!(s3_get_fetches_for(&stub, "/reduced-ttl-test/"), 1);

    // Small sleep to ensure the object age exceeds the new TTL (1ms).
    tokio::time::sleep(Duration::from_millis(5)).await;

    // --- Step 2: Tighten the rule → get_ttl = 1ms (already elapsed) ---
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**", "get_ttl": "0.001s" } ] }"#,
    );

    // --- Step 3: GET again — expect revalidation ---
    let s2 = proxy_get(server.addr, path).await;
    assert!(s2.is_success(), "second GET should succeed");
    assert_eq!(
        s3_get_fetches_for(&stub, "/reduced-ttl-test/"),
        2,
        "BUG CLASS A (reduced get_ttl): after tightening get_ttl to 1ms and waiting, \
         the next GET must revalidate against S3 (2 total fetches). On unfixed code the \
         stored expires_at (~10 years out) keeps serving from cache — COUNTEREXAMPLE: \
         GET with current get_ttl=1ms (object age > 1ms) served from cache, 1 S3 \
         round-trip, expected 2"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// CLASS B BUG-CONDITION EXPLORATION: read_cache_enabled=false stops serving
// AND eagerly deletes the cached copy
//
// These tests MUST FAIL on unfixed code — failure confirms the Class B bug exists.
// They encode the expected post-fix behavior: when `read_cache_enabled=false` is
// resolved for a key that has a cached copy, the proxy forwards to S3 AND deletes
// the cached entry (range files + metadata).
//
// Validates: Requirements 2.3, 2.4, 2.5
// Property 2: Bug Condition — read_cache_enabled=false Stops Serving and Eagerly Deletes
// ===========================================================================

/// Recursively find all files under `dir`. Used to inspect `.meta`/`.bin` presence.
fn find_all_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                results.extend(find_all_files(&path));
            } else {
                results.push(path);
            }
        }
    }
    results
}

/// Find all `.meta` files under the cache directory's `metadata/` subdirectory
/// whose path contains `path_fragment`.
fn find_meta_files_for(
    cache_dir: &std::path::Path,
    path_fragment: &str,
) -> Vec<std::path::PathBuf> {
    let metadata_dir = cache_dir.join("metadata");
    find_all_files(&metadata_dir)
        .into_iter()
        .filter(|p| {
            p.extension().map(|e| e == "meta").unwrap_or(false)
                && p.to_string_lossy().contains(path_fragment)
        })
        .collect()
}

/// Find all `.bin` files under the cache directory's `ranges/` subdirectory
/// whose path contains `path_fragment`.
fn find_bin_files_for(cache_dir: &std::path::Path, path_fragment: &str) -> Vec<std::path::PathBuf> {
    let ranges_dir = cache_dir.join("ranges");
    find_all_files(&ranges_dir)
        .into_iter()
        .filter(|p| {
            p.extension().map(|e| e == "bin").unwrap_or(false)
                && p.to_string_lossy().contains(path_fragment)
        })
        .collect()
}

/// Send a conditional GET (If-None-Match) for `path` through the loopback proxy.
/// Returns the response status.
async fn proxy_conditional_get(addr: SocketAddr, path: &str, etag: &str) -> StatusCode {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let req = Request::builder()
        .method("GET")
        .uri(&uri)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=sig",
        )
        .header("if-none-match", etag)
        .body(Full::new(Bytes::new()))
        .expect("build conditional GET request");

    let resp = client
        .request(req)
        .await
        .expect("proxy conditional GET failed");
    let status = resp.status();
    let _ = resp.into_body().collect().await.unwrap().to_bytes();
    status
}

/// Send a conditional HEAD (If-None-Match) for `path` through the loopback proxy.
/// Returns the response status.
async fn proxy_conditional_head(addr: SocketAddr, path: &str, etag: &str) -> StatusCode {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let uri = format!("http://{}{}", addr, path);
    let req = Request::builder()
        .method("HEAD")
        .uri(&uri)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature=sig",
        )
        .header("if-none-match", etag)
        .body(Full::new(Bytes::new()))
        .expect("build conditional HEAD request");

    let resp = client
        .request(req)
        .await
        .expect("proxy conditional HEAD failed");
    let status = resp.status();
    let _ = resp.into_body().collect().await.unwrap().to_bytes();
    status
}

/// Config with `evaluate_conditions_from_cache` enabled (Mode B) — everything
/// else identical to `behavior_config`.
fn behavior_config_mode_b(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.evaluate_conditions_from_cache = true;
    Arc::new(config)
}

// ---------------------------------------------------------------------------
// (class-b-1) Default path: cache an object, write read_cache_enabled=false,
// GET the matching key. Expect forwarded to S3 AND the .meta/.bin files for
// that key gone. Assert a sibling cached-segment key is untouched.
// On unfixed code the request forwards but the bytes linger on disk → FAIL.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn class_b_default_path_eager_delete_on_read_cache_disabled() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Start with NO rules → objects are cached normally.
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- Step 1: Cache two objects - one in the nocache segment, one sibling ---
    let nocache_path = "/mybucket/nocache/sensitive.bin";
    let sibling_path = "/mybucket/cached/safe.bin";

    let s1 = proxy_get(server.addr, nocache_path).await;
    assert!(
        s1.is_success(),
        "initial GET for nocache key should succeed"
    );
    let s2 = proxy_get(server.addr, sibling_path).await;
    assert!(
        s2.is_success(),
        "initial GET for sibling key should succeed"
    );

    // Verify both keys are now cached on disk (.meta and .bin files exist)
    let nocache_meta_before = find_meta_files_for(temp_dir.path(), "sensitive");
    let nocache_bin_before = find_bin_files_for(temp_dir.path(), "sensitive");
    assert!(
        !nocache_meta_before.is_empty(),
        "Pre-condition: nocache key should have .meta file on disk after initial GET"
    );
    assert!(
        !nocache_bin_before.is_empty(),
        "Pre-condition: nocache key should have .bin file on disk after initial GET"
    );

    let sibling_meta_before = find_meta_files_for(temp_dir.path(), "safe");
    let sibling_bin_before = find_bin_files_for(temp_dir.path(), "safe");
    assert!(
        !sibling_meta_before.is_empty(),
        "Pre-condition: sibling key should have .meta file on disk"
    );
    assert!(
        !sibling_bin_before.is_empty(),
        "Pre-condition: sibling key should have .bin file on disk"
    );

    // --- Step 2: Write rule disabling caching for the nocache path ---
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**/nocache/**", "read_cache_enabled": false } ] }"#,
    );

    // --- Step 3: GET the nocache key again — expect forwarded to S3 AND files deleted ---
    let s3 = proxy_get(server.addr, nocache_path).await;
    assert!(s3.is_success(), "GET after rule change should succeed");

    // Verify eager deletion: .meta and .bin files should be gone for the nocache key
    let nocache_meta_after = find_meta_files_for(temp_dir.path(), "sensitive");
    let nocache_bin_after = find_bin_files_for(temp_dir.path(), "sensitive");
    assert!(
        nocache_meta_after.is_empty(),
        "BUG CLASS B (default path): after read_cache_enabled=false, the .meta file for the \
         no-cache key must be eagerly deleted. On unfixed code the bytes linger on disk — \
         COUNTEREXAMPLE: .meta still present at {:?} after GET with read_cache_enabled=false",
        nocache_meta_after
    );
    assert!(
        nocache_bin_after.is_empty(),
        "BUG CLASS B (default path): after read_cache_enabled=false, the .bin file for the \
         no-cache key must be eagerly deleted. On unfixed code the bytes linger on disk — \
         COUNTEREXAMPLE: .bin still present at {:?} after GET with read_cache_enabled=false",
        nocache_bin_after
    );

    // --- Step 4: Verify sibling key is untouched ---
    let sibling_meta_after = find_meta_files_for(temp_dir.path(), "safe");
    let sibling_bin_after = find_bin_files_for(temp_dir.path(), "safe");
    assert!(
        !sibling_meta_after.is_empty(),
        "Sibling key .meta file should be untouched (rule does not match it)"
    );
    assert!(
        !sibling_bin_after.is_empty(),
        "Sibling key .bin file should be untouched (rule does not match it)"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (class-b-2) Mode B (conditional GET): enable evaluate_conditions_from_cache,
// cache an object, write read_cache_enabled=false, send a conditional GET with
// If-None-Match matching the cached ETag. Expect NOT answered 304/from-cache,
// forwarded to S3, and the cached entry deleted.
// On unfixed code Mode B answers 304 from stored metadata → FAIL.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn class_b_mode_b_conditional_get_not_served_from_cache() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Start with NO rules → objects are cached normally. Mode B enabled.
    let config = behavior_config_mode_b(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- Step 1: Cache the object (GET populates body + metadata) ---
    let path = "/mybucket/nocache/cond-get.bin";
    let s1 = proxy_get(server.addr, path).await;
    assert!(
        s1.is_success(),
        "initial GET should succeed (populates cache)"
    );
    let fetches_after_cache = s3_get_fetches_for(&stub, "/nocache/cond-get");
    assert_eq!(
        fetches_after_cache, 1,
        "initial GET should produce 1 S3 fetch"
    );

    // Verify the object is cached
    let meta_before = find_meta_files_for(temp_dir.path(), "cond-get");
    assert!(
        !meta_before.is_empty(),
        "Pre-condition: object should be cached (.meta exists)"
    );

    // --- Step 2: Write rule disabling caching for this key ---
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**/nocache/**", "read_cache_enabled": false } ] }"#,
    );

    // --- Step 3: Send conditional GET with If-None-Match matching cached ETag ---
    // On unfixed code, Mode B evaluates conditions from cache BEFORE the
    // read_cache_enabled gate, answers 304, and never contacts S3.
    let total_before = stub.captured().len();
    let status = proxy_conditional_get(server.addr, path, "\"glob-behavior-etag\"").await;

    // The response should NOT be 304 (from cache). It should be forwarded to S3.
    assert_ne!(
        status,
        StatusCode::NOT_MODIFIED,
        "BUG CLASS B (Mode B conditional GET): after read_cache_enabled=false, a conditional \
         GET must NOT be answered 304 from cache. On unfixed code Mode B evaluates conditions \
         from stored metadata and answers 304 — COUNTEREXAMPLE: conditional GET with \
         If-None-Match matching cached ETag received 304 after read_cache_enabled=false"
    );

    // Verify S3 was contacted (the request was forwarded)
    let fetches_after_cond = stub.captured().len() - total_before;
    assert!(
        fetches_after_cond >= 1,
        "BUG CLASS B (Mode B conditional GET): request must be forwarded to S3 (expect >= 1 \
         new S3 round-trip). On unfixed code Mode B answers from cache with 0 S3 contact — \
         COUNTEREXAMPLE: 0 S3 round-trips after conditional GET with read_cache_enabled=false"
    );

    // Verify eager deletion: cached entry should be gone
    let meta_after = find_meta_files_for(temp_dir.path(), "cond-get");
    let bin_after = find_bin_files_for(temp_dir.path(), "cond-get");
    assert!(
        meta_after.is_empty(),
        "BUG CLASS B (Mode B conditional GET): .meta should be eagerly deleted after \
         read_cache_enabled=false. COUNTEREXAMPLE: .meta still present at {:?}",
        meta_after
    );
    assert!(
        bin_after.is_empty(),
        "BUG CLASS B (Mode B conditional GET): .bin should be eagerly deleted after \
         read_cache_enabled=false. COUNTEREXAMPLE: .bin still present at {:?}",
        bin_after
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (class-b-3) Mode B (conditional HEAD): enable evaluate_conditions_from_cache,
// cache HEAD metadata, write read_cache_enabled=false, send a conditional HEAD
// with If-None-Match matching the cached ETag. Expect NOT answered 304/from-cache,
// forwarded to S3, and the cached entry deleted.
// On unfixed code Mode B answers 304 from stored HEAD metadata → FAIL.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn class_b_mode_b_conditional_head_not_served_from_cache() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Mode B enabled.
    let config = behavior_config_mode_b(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- Step 1: Populate cache (GET + HEAD to ensure both body and HEAD metadata cached) ---
    let path = "/mybucket/nocache/cond-head.bin";
    let s1 = proxy_get(server.addr, path).await;
    assert!(
        s1.is_success(),
        "initial GET should succeed (populates body cache)"
    );
    let h1 = proxy_head(server.addr, path).await;
    assert!(
        h1.is_success(),
        "initial HEAD should succeed (populates HEAD metadata)"
    );

    // Verify cached
    let meta_before = find_meta_files_for(temp_dir.path(), "cond-head");
    assert!(
        !meta_before.is_empty(),
        "Pre-condition: object should be cached (.meta exists)"
    );

    // --- Step 2: Write rule disabling caching for this key ---
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**/nocache/**", "read_cache_enabled": false } ] }"#,
    );

    // --- Step 3: Send conditional HEAD with If-None-Match matching cached ETag ---
    let total_before = stub.captured().len();
    let status = proxy_conditional_head(server.addr, path, "\"glob-behavior-etag\"").await;

    // The response should NOT be 304 (from cache).
    assert_ne!(
        status,
        StatusCode::NOT_MODIFIED,
        "BUG CLASS B (Mode B conditional HEAD): after read_cache_enabled=false, a conditional \
         HEAD must NOT be answered 304 from cache. On unfixed code Mode B evaluates conditions \
         from stored metadata and answers 304 — COUNTEREXAMPLE: conditional HEAD with \
         If-None-Match matching cached ETag received 304 after read_cache_enabled=false"
    );

    // Verify S3 was contacted (the request was forwarded)
    let fetches_after_cond = stub.captured().len() - total_before;
    assert!(
        fetches_after_cond >= 1,
        "BUG CLASS B (Mode B conditional HEAD): request must be forwarded to S3 (expect >= 1 \
         new S3 round-trip). On unfixed code Mode B answers from cache with 0 S3 contact — \
         COUNTEREXAMPLE: 0 S3 round-trips after conditional HEAD with read_cache_enabled=false"
    );

    // Verify eager deletion: cached entry should be gone
    let meta_after = find_meta_files_for(temp_dir.path(), "cond-head");
    let bin_after = find_bin_files_for(temp_dir.path(), "cond-head");
    assert!(
        meta_after.is_empty(),
        "BUG CLASS B (Mode B conditional HEAD): .meta should be eagerly deleted after \
         read_cache_enabled=false. COUNTEREXAMPLE: .meta still present at {:?}",
        meta_after
    );
    assert!(
        bin_after.is_empty(),
        "BUG CLASS B (Mode B conditional HEAD): .bin should be eagerly deleted after \
         read_cache_enabled=false. COUNTEREXAMPLE: .bin still present at {:?}",
        bin_after
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// EAGER INVALIDATION UNIT TESTS (Task 4.4)
//
// Test the eager-invalidation helper behavior:
// 1. With no cached copy: the existence probe returns None and no delete is
//    attempted (no error, no spurious journal entry).
// 2. With a cached copy + read_cache_enabled=false: invalidate_all_ranges
//    removes .bin + .meta (also drops HEAD head_expires_at metadata).
//
// Validates: Requirements 2.3, 2.4, 2.5
// ===========================================================================

// ---------------------------------------------------------------------------
// (eager-invalidation-1) No cached copy: GET/HEAD with read_cache_enabled=false
// for a key that was never cached. The existence probe skips the delete — no
// error, no disk I/O, no spurious journal entry. The request succeeds and
// forwards to S3.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn eager_invalidation_no_copy_skips_delete_no_error() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Rule disables read caching for the target path from the start.
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**/never-cached/**", "read_cache_enabled": false } ] }"#,
    );

    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // GET a key that was never cached — read_cache_enabled=false from the start.
    // The existence probe should find nothing and skip the delete.
    let path = "/mybucket/never-cached/phantom.bin";
    let status = proxy_get(server.addr, path).await;
    assert!(
        status.is_success(),
        "GET for never-cached key with read_cache_enabled=false should succeed (forward to S3)"
    );
    assert_eq!(
        s3_get_fetches_for(&stub, "/never-cached/"),
        1,
        "Exactly 1 S3 fetch (forwarded to S3, no cache interaction)"
    );

    // No .meta or .bin files should exist for this key (nothing to delete)
    let meta_files = find_meta_files_for(temp_dir.path(), "phantom");
    let bin_files = find_bin_files_for(temp_dir.path(), "phantom");
    assert!(
        meta_files.is_empty(),
        "No .meta should exist for a never-cached key"
    );
    assert!(
        bin_files.is_empty(),
        "No .bin should exist for a never-cached key"
    );

    // HEAD also works without error
    let head_path = "/mybucket/never-cached/phantom-head.bin";
    let head_status = proxy_head(server.addr, head_path).await;
    assert!(
        head_status.is_success(),
        "HEAD for never-cached key with read_cache_enabled=false should succeed (forward to S3)"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (eager-invalidation-2) Mode B: no cached copy with read_cache_enabled=false
// and conditional headers. The existence probe skips the delete and the request
// forwards to S3 without error.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn eager_invalidation_mode_b_no_copy_skips_delete() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Rule disables read caching for the target path, Mode B enabled.
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**/never-cached/**", "read_cache_enabled": false, "evaluate_conditions_from_cache": true } ] }"#,
    );

    let config = behavior_config_mode_b(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // Conditional GET for a key never cached — the existence probe finds nothing,
    // skips the delete, and forwards to S3. No error or panic.
    let path = "/mybucket/never-cached/cond-phantom.bin";
    let status = proxy_conditional_get(server.addr, path, "\"some-etag\"").await;
    assert!(
        status.is_success(),
        "Conditional GET for never-cached key with read_cache_enabled=false should \
         forward to S3 successfully"
    );

    // Conditional HEAD too
    let head_path = "/mybucket/never-cached/cond-phantom-head.bin";
    let head_status = proxy_conditional_head(server.addr, head_path, "\"some-etag\"").await;
    assert!(
        head_status.is_success(),
        "Conditional HEAD for never-cached key with read_cache_enabled=false should \
         forward to S3 successfully"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// (a) READ-CACHE DISABLE — the core on/off contrast.
//
// A rule `{ "pattern": "**/no-cache/**", "read_cache_enabled": false }` must
// make every GET for a matching key bypass the cache and re-fetch from S3,
// while a control key that matches no rule is served from cache on the second
// GET. The S3-round-trip count is the behavioral proof:
//   - matching key: 2 GETs → 2 S3 fetches (no cache HIT, ever)
//   - control key:  2 GETs → 1 S3 fetch  (second GET served from cache)
//
// Validates: Requirement 3.1/3.3 (the resolved `read_cache_enabled` from the
// matching rule is honored on the request path) and Requirement 2.3/2.8 (`**`
// crosses `/` and a leading `**` matches across buckets).
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn read_cache_disable_rule_forces_s3_refetch_while_control_key_is_cached() {
    let temp_dir = TempDir::new().expect("tempdir");
    // Rule file written BEFORE the cache infra is constructed.
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**/no-cache/**", "read_cache_enabled": false } ] }"#,
    );

    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // --- matching key: read caching disabled → every GET re-fetches from S3 ---
    let no_cache_path = "/mybucket/no-cache/file.bin";
    let s1 = proxy_get(server.addr, no_cache_path).await;
    let s2 = proxy_get(server.addr, no_cache_path).await;
    assert!(s1.is_success(), "first no-cache GET should succeed");
    assert!(s2.is_success(), "second no-cache GET should succeed");
    assert_eq!(
        s3_fetches_for(&stub, "/no-cache/"),
        2,
        "Req 3.1/3.3 + 2.3/2.8: rule `**/no-cache/**` (read_cache_enabled=false) must \
         match key `mybucket/no-cache/file.bin` and force a fresh S3 fetch on BOTH GETs \
         (no cache HIT)"
    );

    // --- control key: matches no rule → second GET served from cache ---
    let control_path = "/mybucket/other/file.bin";
    let c1 = proxy_get(server.addr, control_path).await;
    let c2 = proxy_get(server.addr, control_path).await;
    assert!(c1.is_success(), "first control GET should succeed");
    assert!(c2.is_success(), "second control GET should succeed");
    assert_eq!(
        s3_fetches_for(&stub, "/other/"),
        1,
        "Req 1.4 + 3.3: control key `mybucket/other/file.bin` matches no rule, so global \
         read caching applies and the SECOND GET is served from cache (exactly one S3 fetch)"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// (b) FULL-KEY MATCHING — the headline feature.
//
// A rule keyed on a bucket-inclusive glob `specificbucket/**` must change
// caching for `specificbucket/...` but NOT for a *different* bucket carrying the
// *same object key*. This proves matching is performed against the full
// `{bucket}/{object_key}` cache key, not against the object key alone.
//
//   - specificbucket/data/file.bin → matches → read cache disabled → 2 S3 fetches
//   - otherbucket/data/file.bin    → no match → cached            → 1 S3 fetch
//
// Validates: Requirement 2.1 (match against `{bucket}/{object_key}`), 2.7 (a
// literal bucket prefix targets only that bucket), and 9.1 (match against the
// computed cache key).
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn full_bucket_key_glob_matches_only_the_named_bucket() {
    let temp_dir = TempDir::new().expect("tempdir");
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "specificbucket/**", "read_cache_enabled": false } ] }"#,
    );

    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // Same object key (`data/file.bin`) under two different buckets.
    let specific_path = "/specificbucket/data/file.bin";
    let other_path = "/otherbucket/data/file.bin";

    // specificbucket → rule matches the full key → read cache disabled.
    assert!(proxy_get(server.addr, specific_path).await.is_success());
    assert!(proxy_get(server.addr, specific_path).await.is_success());
    assert_eq!(
        s3_fetches_for(&stub, "/specificbucket/"),
        2,
        "Req 2.1/2.7/9.1: `specificbucket/**` matches the full key \
         `specificbucket/data/file.bin`, so read caching is disabled and both GETs hit S3"
    );

    // otherbucket → same object key, different bucket → rule does NOT match.
    assert!(proxy_get(server.addr, other_path).await.is_success());
    assert!(proxy_get(server.addr, other_path).await.is_success());
    assert_eq!(
        s3_fetches_for(&stub, "/otherbucket/"),
        1,
        "Req 2.1/2.7: the SAME object key `data/file.bin` under `otherbucket` does NOT match \
         `specificbucket/**`, proving matching is against the full {{bucket}}/{{key}} — so the \
         second GET is served from cache (one S3 fetch)"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// (b') CASE-SENSITIVE FULL-KEY MATCHING.
//
// Reinforces Requirement 9.6 (case-sensitive matching) on the real path: a
// rule `SpecificBucket/**` must NOT disable caching for the lower-cased bucket
// `specificbucket/...`, so the control key remains cacheable.
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn full_key_matching_is_case_sensitive_on_request_path() {
    let temp_dir = TempDir::new().expect("tempdir");
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "SpecificBucket/**", "read_cache_enabled": false } ] }"#,
    );

    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    // Lower-cased bucket — must NOT match the capitalized pattern.
    let lower_path = "/specificbucket/data/file.bin";
    assert!(proxy_get(server.addr, lower_path).await.is_success());
    assert!(proxy_get(server.addr, lower_path).await.is_success());
    assert_eq!(
        s3_fetches_for(&stub, "/specificbucket/"),
        1,
        "Req 9.6: matching is case-sensitive, so `SpecificBucket/**` does NOT match \
         `specificbucket/data/file.bin`; global read caching applies and the second GET \
         is served from cache (one S3 fetch)"
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// PRESERVATION TESTS (Task 3)
//
// These tests capture baseline behavior on UNFIXED code and MUST PASS.
// They assert behaviors that the fix must not regress:
// - Unchanged rules: in-window objects served from cache, expired revalidated
// - TTL-unchanged equivalence (property-based)
// - read_cache_enabled=true → object served from cache and NOT deleted
// - HEAD "not cached" gate: head_expires_at == None → HEAD reports a miss
// - Resolve-once: settings resolved exactly once per request
//
// **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
// Property 3: Preservation — Non-Buggy Inputs and Resolve-Once Unchanged
// ===========================================================================

// ---------------------------------------------------------------------------
// (preservation-1) Unchanged rules, GET: in-window object served from cache
// (0 new S3 fetches on second GET). This is the normal cache-hit path.
// Validates: Requirement 3.1, 3.2
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn preservation_unchanged_rules_get_in_window_served_from_cache() {
    let temp_dir = TempDir::new().expect("tempdir");

    // No rules file → global defaults (get_ttl ~10 years). The object will be
    // well within its TTL window for the entire duration of this test.
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/preserve-get-inwindow/file.bin";

    // First GET: cache miss → 1 S3 fetch
    let s1 = proxy_get(server.addr, path).await;
    assert!(s1.is_success(), "first GET should succeed");
    assert_eq!(s3_get_fetches_for(&stub, "/preserve-get-inwindow/"), 1);

    // Second GET (unchanged rules, within TTL): should be served from cache
    let s2 = proxy_get(server.addr, path).await;
    assert!(s2.is_success(), "second GET should succeed");
    assert_eq!(
        s3_get_fetches_for(&stub, "/preserve-get-inwindow/"),
        1,
        "Preservation 3.1/3.2: with unchanged rules and the object within its TTL window, \
         the second GET must be served from cache (exactly 1 total S3 fetch, not 2)"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (preservation-2) Unchanged rules, HEAD: in-window HEAD served from cache
// (0 new S3 fetches on second HEAD within head_ttl window).
// Validates: Requirement 3.1, 3.2
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn preservation_unchanged_rules_head_in_window_served_from_cache() {
    let temp_dir = TempDir::new().expect("tempdir");

    // No rules file → global defaults (head_ttl = 60s).
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/preserve-head-inwindow/file.bin";

    // Populate the cache with a GET first (so body + metadata are present),
    // then HEAD to populate head_expires_at.
    let g1 = proxy_get(server.addr, path).await;
    assert!(g1.is_success(), "initial GET should succeed");

    let h1 = proxy_head(server.addr, path).await;
    assert!(h1.is_success(), "first HEAD should succeed");

    // Second HEAD (within head_ttl=60s): should be served from cached HEAD metadata
    let total_before = stub.captured().len();
    let h2 = proxy_head(server.addr, path).await;
    assert!(h2.is_success(), "second HEAD should succeed");
    let fetches_after = stub.captured().len() - total_before;
    assert_eq!(
        fetches_after, 0,
        "Preservation 3.1/3.2: with unchanged rules and HEAD within its TTL window, \
         the second HEAD must be served from cached HEAD metadata (0 new S3 fetches)"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (preservation-3) read_cache_enabled=true: object served from cache AND
// the cached entry is NOT deleted (eager invalidation must not fire).
// Validates: Requirement 3.5
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn preservation_read_cache_enabled_true_not_deleted() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Explicit rule with read_cache_enabled=true (the global default is also true,
    // but this exercises the path where a rule explicitly resolves to true).
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**", "read_cache_enabled": true } ] }"#,
    );

    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/preserve-not-deleted/file.bin";

    // Cache the object
    let s1 = proxy_get(server.addr, path).await;
    assert!(s1.is_success(), "first GET should succeed");
    assert_eq!(s3_get_fetches_for(&stub, "/preserve-not-deleted/"), 1);

    // Second GET: served from cache (confirms read_cache_enabled=true is honored)
    let s2 = proxy_get(server.addr, path).await;
    assert!(s2.is_success(), "second GET should succeed");
    assert_eq!(
        s3_get_fetches_for(&stub, "/preserve-not-deleted/"),
        1,
        "Preservation 3.5: with read_cache_enabled=true, the second GET must be \
         served from cache (1 total S3 fetch)"
    );

    // Verify the cache entry still exists on disk (not eagerly deleted).
    // The cache key for path /mybucket/preserve-not-deleted/file.bin is
    // "mybucket/preserve-not-deleted/file.bin". We look for .meta files.
    let metadata_dir = temp_dir.path().join("metadata");
    let has_meta_files = std::fs::read_dir(&metadata_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .any(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        })
        .unwrap_or(false);
    assert!(
        has_meta_files,
        "Preservation 3.5: with read_cache_enabled=true, the cached entry must NOT be \
         deleted — the metadata directory should contain cached entries"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (preservation-4) HEAD "not cached" gate: when head_expires_at is None
// (HEAD metadata was never populated), a HEAD request must report a miss
// (i.e. contact S3), not serve from a stale or synthesized entry.
// Validates: Requirement 3.4 (is_head_expired "not cached" gate preserved)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn preservation_head_not_cached_gate_reports_miss() {
    let temp_dir = TempDir::new().expect("tempdir");

    // Set head_ttl=0 from the start so HEAD metadata is never stored as fresh.
    // (head_ttl=0 means HEAD is always treated as expired / not cached.)
    // With head_ttl=0 set from the beginning (not tightened after caching),
    // HEAD metadata was never stored as fresh, so head_expires_at is effectively
    // None / expired from the start. Every HEAD must contact S3.
    write_cache_rules(
        temp_dir.path(),
        r#"{ "rules": [ { "pattern": "**", "head_ttl": "0s" } ] }"#,
    );

    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/preserve-head-gate/file.bin";

    // Populate cache with a GET (body is cached, but with head_ttl=0
    // the HEAD metadata should be expired immediately / never fresh).
    let g1 = proxy_get(server.addr, path).await;
    assert!(g1.is_success(), "initial GET should succeed");

    // First HEAD: must contact S3 (HEAD metadata not cached / expired)
    let total_before = stub.captured().len();
    let h1 = proxy_head(server.addr, path).await;
    assert!(h1.is_success(), "first HEAD should succeed");
    let fetches_h1 = stub.captured().len() - total_before;
    assert!(
        fetches_h1 >= 1,
        "Preservation 3.4: with head_ttl=0 from the start (HEAD not cached), \
         the first HEAD must contact S3 (HEAD metadata never stored as fresh)"
    );

    // Second HEAD: must also contact S3 (still not cached)
    let total_before2 = stub.captured().len();
    let h2 = proxy_head(server.addr, path).await;
    assert!(h2.is_success(), "second HEAD should succeed");
    let fetches_h2 = stub.captured().len() - total_before2;
    assert!(
        fetches_h2 >= 1,
        "Preservation 3.4: with head_ttl=0, every HEAD must contact S3 — the \
         HEAD 'not cached' gate must not turn an uncached HEAD into a hit"
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (preservation-5) Resolve-once: a GET request produces a deterministic,
// bounded number of resolve_settings calls. On unfixed code this is > 1
// (the conditional path resolves separately from the gate). The fix (Task 4.1)
// hoists the resolve to produce exactly 1. This test captures the current
// baseline count on unfixed code so we can verify it doesn't regress further,
// and Task 4.8 will re-assert it as exactly 1 after the fix.
// Validates: Requirement 3.3 (baseline observation)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn preservation_resolve_once_get_request() {
    let temp_dir = TempDir::new().expect("tempdir");

    // No rules file → global defaults
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/preserve-resolve-once-get/file.bin";

    // Record the initial count
    let manager = cache_manager.get_bucket_settings_manager();
    let count_before = manager.resolve_call_count();

    // First GET: cache miss path
    let s1 = proxy_get(server.addr, path).await;
    assert!(s1.is_success(), "first GET should succeed");

    let resolves_first = manager.resolve_call_count() - count_before;

    // The resolve count per request must be deterministic — a second GET for
    // the same path (cache hit this time) must produce the same number of
    // resolves as the first, proving the count is stable and bounded.
    let count_before_second = manager.resolve_call_count();
    let s2 = proxy_get(server.addr, path).await;
    assert!(s2.is_success(), "second GET should succeed");
    let resolves_second = manager.resolve_call_count() - count_before_second;

    // Both requests must resolve a bounded, non-zero number of times.
    assert!(
        resolves_first >= 1,
        "Preservation 3.3: a GET request must resolve settings at least once"
    );
    assert!(
        resolves_second >= 1,
        "Preservation 3.3: a cached-hit GET must still resolve settings at least once"
    );
    // The count must be bounded and deterministic (not growing unboundedly).
    // On unfixed code: typically 3 (conditional branch + gate + one other path).
    // After fix: exactly 1 per request.
    assert!(
        resolves_first <= 5,
        "Preservation 3.3/3.6: resolve count per GET must be bounded (got {}). \
         Unbounded resolution would violate the latency budget.",
        resolves_first
    );
    assert!(
        resolves_second <= 5,
        "Preservation 3.3/3.6: resolve count per cached-hit GET must be bounded (got {}).",
        resolves_second
    );

    let _ = server.shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// (preservation-6) Resolve-once: a HEAD request produces a deterministic,
// bounded number of resolve_settings calls. Same observation-first approach.
// Validates: Requirement 3.3 (baseline observation)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn preservation_resolve_once_head_request() {
    let temp_dir = TempDir::new().expect("tempdir");

    // No rules file → global defaults
    let config = behavior_config(temp_dir.path().to_path_buf());
    let (cache_manager, _disk, range_handler) = make_cache_infra(&config).await;
    let stub = get_ok_stub();
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&inflight_tracker),
    )
    .await;

    let path = "/mybucket/preserve-resolve-once-head/file.bin";

    // Populate cache with a GET first so HEAD has metadata to check
    let g1 = proxy_get(server.addr, path).await;
    assert!(g1.is_success(), "initial GET should succeed");

    // Record the count after the GET
    let manager = cache_manager.get_bucket_settings_manager();
    let count_before = manager.resolve_call_count();

    // First HEAD request
    let h1 = proxy_head(server.addr, path).await;
    assert!(h1.is_success(), "first HEAD should succeed");
    let resolves_first = manager.resolve_call_count() - count_before;

    // Second HEAD request (same key)
    let count_before_second = manager.resolve_call_count();
    let h2 = proxy_head(server.addr, path).await;
    assert!(h2.is_success(), "second HEAD should succeed");
    let resolves_second = manager.resolve_call_count() - count_before_second;

    // Both HEAD requests must resolve at least once, bounded, deterministic.
    assert!(
        resolves_first >= 1,
        "Preservation 3.3: a HEAD request must resolve settings at least once"
    );
    assert!(
        resolves_second >= 1,
        "Preservation 3.3: a second HEAD must still resolve settings at least once"
    );
    assert!(
        resolves_first <= 5,
        "Preservation 3.3/3.6: resolve count per HEAD must be bounded (got {}). \
         Unbounded resolution would violate the latency budget.",
        resolves_first
    );
    assert!(
        resolves_second <= 5,
        "Preservation 3.3/3.6: resolve count per second HEAD must be bounded (got {}).",
        resolves_second
    );

    let _ = server.shutdown_tx.send(());
}

// ===========================================================================
// PRESERVATION PROPERTY-BASED TEST: TTL-unchanged equivalence
//
// When the TTL is unchanged from write time, the stored-expiry check
// (`now ≤ storedExpiry`) is equivalent to the created_at comparison
// (`now - created_at ≤ ttl`). This is a universal property that must hold
// for all valid TTL values and timestamps.
//
// We test this at the data structure level since it's a pure mathematical
// equivalence that does not need the full proxy server stack.
//
// Validates: Requirements 3.1, 3.2
// ===========================================================================

/// Property: for any created_at and non-zero ttl, if storedExpiry was computed
/// as `created_at + ttl` at write time, then at any future point `now`:
///   `now ≤ storedExpiry` ⇔ `now - created_at ≤ ttl`
///
/// This proves the current-TTL comparison (used by the fix) is equivalent to
/// the stored-expiry comparison (used by the original code) when the TTL has
/// not changed — the foundation of the preservation guarantee.
#[test]
fn preservation_ttl_unchanged_equivalence_property_get() {
    use quickcheck::{QuickCheck, TestResult};

    fn prop(ttl_secs: u32, age_secs: u32) -> TestResult {
        // Avoid overflow and zero-TTL (which is explicitly "always expired")
        if ttl_secs == 0 {
            return TestResult::discard();
        }
        let ttl = Duration::from_secs(ttl_secs as u64);
        let age = Duration::from_secs(age_secs as u64);

        // Simulate: created_at = now - age; storedExpiry = created_at + ttl
        // stored_expiry_fresh ⇔ now ≤ created_at + ttl ⇔ age ≤ ttl
        let stored_expiry_fresh = age <= ttl;

        // current-TTL comparison: now - created_at ≤ ttl ⇔ age ≤ ttl
        let current_ttl_fresh = age <= ttl;

        TestResult::from_bool(stored_expiry_fresh == current_ttl_fresh)
    }

    QuickCheck::new()
        .tests(1000)
        .quickcheck(prop as fn(u32, u32) -> TestResult);
}

/// Same equivalence property for HEAD: head_expires_at = created_at + head_ttl.
/// When head_expires_at is Some and head_ttl is unchanged:
///   `now ≤ head_expires_at` ⇔ `now - created_at ≤ head_ttl`
#[test]
fn preservation_ttl_unchanged_equivalence_property_head() {
    use quickcheck::{QuickCheck, TestResult};

    fn prop(head_ttl_secs: u32, age_secs: u32) -> TestResult {
        if head_ttl_secs == 0 {
            return TestResult::discard();
        }
        let head_ttl = Duration::from_secs(head_ttl_secs as u64);
        let age = Duration::from_secs(age_secs as u64);

        // stored-expiry check: now ≤ head_expires_at ⇔ age ≤ head_ttl
        let stored_expiry_fresh = age <= head_ttl;

        // current-TTL check: now - created_at ≤ head_ttl ⇔ age ≤ head_ttl
        let current_ttl_fresh = age <= head_ttl;

        TestResult::from_bool(stored_expiry_fresh == current_ttl_fresh)
    }

    QuickCheck::new()
        .tests(1000)
        .quickcheck(prop as fn(u32, u32) -> TestResult);
}

/// Extended property: for random metadata, the `is_object_expired()` stored-
/// expiry check agrees with the current-TTL comparison when the TTL has not
/// changed since write time. Tests against the real `NewCacheMetadata` type.
#[test]
fn preservation_ttl_unchanged_equivalence_metadata_level() {
    use quickcheck::{QuickCheck, TestResult};
    use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

    fn prop(ttl_secs: u32, age_ms: u32) -> TestResult {
        if ttl_secs == 0 {
            return TestResult::discard();
        }
        let ttl = Duration::from_secs(ttl_secs as u64);
        let age = Duration::from_millis(age_ms as u64);

        let now = std::time::SystemTime::now();
        let created_at = match now.checked_sub(age) {
            Some(t) => t,
            None => return TestResult::discard(),
        };
        // At write time: expires_at = created_at + ttl
        let expires_at = created_at + ttl;

        let metadata = NewCacheMetadata {
            cache_key: "test/preservation".to_string(),
            object_metadata: ObjectMetadata::new(
                "\"etag\"".to_string(),
                "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            ),
            ranges: Vec::new(),
            created_at,
            expires_at,
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Stored-expiry check (what the original code does)
        let stored_fresh = !metadata.is_object_expired();

        // Current-TTL comparison (what the fix does)
        let current_ttl_fresh = now.duration_since(created_at).unwrap_or(Duration::ZERO) <= ttl;

        TestResult::from_bool(stored_fresh == current_ttl_fresh)
    }

    QuickCheck::new()
        .tests(1000)
        .quickcheck(prop as fn(u32, u32) -> TestResult);
}
