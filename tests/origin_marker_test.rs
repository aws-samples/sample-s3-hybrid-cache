//! A cached bucket is bound to the origin that cached it.
//!
//! Spec: the public security review readiness feature Requirement 2.
//!
//! # The defect
//!
//! `CacheManager::generate_cache_key` takes the bucket from the host for virtual-hosted
//! addressing and from the path for path-style, and a host matching no AWS S3 hostname
//! family contributes **nothing**. So `GET /mybucket/data.bin` to one S3-compatible origin
//! and the identical request to a second origin both key to `mybucket/data.bin` and shared
//! one cache entry: the second origin's request **was answered with the first origin's
//! bytes**.
//!
//! # What these tests assert, and what they deliberately do not
//!
//! The predicate is **the serve decision** — whether a request for a bucket held by a
//! different origin is forwarded rather than answered from cache. Not whether a marker file
//! was written, and not whether its contents are right: a test asserting the marker exists
//! passes against an implementation that writes it and then ignores it.
//!
//! So the two origins return **different bytes**, registered against different
//! `authorization` values, which is the mechanism `StubS3Client` offers for per-request
//! routing. The mismatched origin receiving its OWN bytes is the assertion; receiving the
//! recorded origin's bytes is the defect, observable byte-for-byte rather than inferred from
//! a header or a counter.
//!
//! Each test additionally asserts that the **recorded origin's cached bytes are still on
//! disk afterwards and still serve**, so "bypass" is distinguishable from "invalidate". That
//! distinction is the whole of acceptance criterion 4: reusing the
//! `read_cache_enabled: false` gate's eager invalidation here would let a second origin's
//! traffic evict the first origin's cache, which is strictly worse than the bug being fixed.
//!
//! # Both vacuity directions are guarded inside the test bodies
//!
//! A fixture built from AWS-shaped hosts never reaches the mismatch branch at all —
//! `origin_identity("bucket.s3.us-west-2.amazonaws.com")` and
//! `origin_identity("s3.us-west-2.amazonaws.com")` are both `amazonaws.com`, so such a test
//! asserts "no bypass" and passes whether or not the guard exists. The mismatch tests
//! therefore assert their two identities are **unequal**, and the collapsing test asserts
//! its two are **equal**, so a later matcher change that collapses or splits them fails
//! *there* rather than silently making the other test vacuous.
//!
//! Every test also asserts the two requests produce the **same cache key**. Without that,
//! two hosts that happened to key differently would not collide in the first place and the
//! guard would be untested — the collision is the precondition, not a detail.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::Semaphore;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::origin_identity::{
    origin_identity, origin_marker_mismatch_bypasses_total,
    origin_marker_unparseable_bypasses_total, uncacheable_aws_host_bypasses_total,
};
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

/// One bucket name, in use at two unrelated origins. This is the collision.
const BUCKET: &str = "mybucket";
const OBJECT_KEY: &str = "data.bin";

/// Two non-AWS origins. Both reduce to themselves, never to `example.com` — a suffix is not
/// a namespace boundary outside AWS, so collapsing these could only create false matches.
const ORIGIN_A: &str = "minio-a.example.com";
const ORIGIN_B: &str = "minio-b.example.com";

/// Distinct bodies, so "served the other origin's bytes" is a byte-level observation.
const BODY_A: &[u8] = b"AAAA-origin-a-content-AAAA";
const BODY_B: &[u8] = b"BBBB-origin-b-content-BBBB";

/// `StubS3Client` routes on `If-None-Match`, then `Range`, then `authorization`, then a
/// default. It has no host-based routing, so the two origins are disambiguated by sending
/// different `authorization` values and registering a body against each.
const AUTH_A: &str = "AWS4-HMAC-SHA256 Credential=AKIA-ORIGIN-A/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig";
const AUTH_B: &str = "AWS4-HMAC-SHA256 Credential=AKIA-ORIGIN-B/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=sig";

/// Serialises every test in this file, because the two bypass counters are **process-global**
/// atomics and libtest runs a binary's tests as parallel threads in one process.
///
/// Measured: without this, `an_entry_cached_before_markers_existed...`'s "no mismatch bypass was
/// counted" assertion read 2 against an expected 0 — two sibling tests' increments, observed
/// mid-run.
///
/// A `>` assertion is not a safe substitute for either direction. "This did not increment"
/// cannot be asserted against a shared singleton at all, and "this did increment" can pass on
/// somebody else's increment — which would be a false green on the very assertion that proves a
/// bypass is observable rather than silent. Serialising makes both directions exact.
static COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn object_path() -> String {
    format!("/{}/{}", BUCKET, OBJECT_KEY)
}

fn expected_cache_key() -> String {
    format!("{}/{}", BUCKET, OBJECT_KEY)
}

fn test_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    // Every miss must be an independently observable S3 fetch and nothing may be promoted in
    // the background, or "was this served from cache" is not decidable from the response.
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.write_cache_enabled = true;
    Arc::new(config)
}

async fn make_cache_infra(config: &Arc<Config>) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
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
    (cache_manager, range_handler)
}

struct ProxyServer {
    addr: SocketAddr,
    _shutdown_tx: oneshot::Sender<()>,
}

/// Serve the real `HttpProxy::handle_request` on a loopback port, so requests go through the
/// genuine dispatch — including `validate_host_header`, which is where the port-stripped host
/// the origin identity is derived from comes from.
async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    let inflight_tracker = Arc::new(InFlightTracker::new());

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

    ProxyServer {
        addr,
        _shutdown_tx: shutdown_tx,
    }
}

/// One request through the proxy with an explicit `Host`.
///
/// A raw `conn::http1` handshake rather than the pooled client, because the `Host` header is
/// the input under test: the pooled client derives it from the request URI's authority, so it
/// cannot be set to an arbitrary origin. This sends a relative URI and the `Host` verbatim,
/// which is exactly the shape a client points at an on-host proxy produces.
async fn proxy_request(
    addr: SocketAddr,
    method: &str,
    host: &str,
    path: &str,
    auth: &str,
) -> (StatusCode, HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect to proxy");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", host)
        .header("authorization", auth)
        .body(Full::new(Bytes::new()))
        .expect("build request");

    let resp = sender
        .send_request(req)
        .await
        .expect("proxy request failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    (status, headers, body)
}

async fn proxy_get(
    addr: SocketAddr,
    host: &str,
    path: &str,
    auth: &str,
) -> (StatusCode, HeaderMap, Bytes) {
    proxy_request(addr, "GET", host, path, auth).await
}

/// A request with **no `authorization` header**, so `is_aws_sigv4_signed` is false and it takes
/// the generic forward arm.
///
/// That arm forwards through `s3_client.forward_request` — the stub — and invalidates on
/// success, which makes it the only write path whose invalidation is reachable in this fixture.
/// The signed arms call `forward_signed_request`/`forward_signed_request_streaming`, which
/// return 502 here, so their success branches are never entered. Measured, not assumed: see the
/// docstring of `a_mismatched_delete_does_not_invalidate_but_a_matching_one_does`.
async fn unsigned_request(
    addr: SocketAddr,
    method: &str,
    host: &str,
    path: &str,
) -> (StatusCode, HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect to proxy");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", host)
        .body(Full::new(Bytes::new()))
        .expect("build request");
    let resp = sender
        .send_request(req)
        .await
        .expect("proxy request failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    (status, headers, body)
}

fn x_cache(headers: &HeaderMap) -> Option<&str> {
    headers.get("x-cache").and_then(|v| v.to_str().ok())
}

fn ok_body(body: &[u8]) -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("content-length", body.len().to_string())
        .with_header("etag", "\"origin-marker-etag\"")
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header("content-type", "application/octet-stream")
        .with_body(Bytes::copy_from_slice(body))
}

/// Every file beneath `metadata/` and `ranges/` whose path names this bucket, as a sorted
/// `(path, len)` set.
///
/// A SET rather than a count or an mtime: the forward path writes range data under a name
/// derived from the extent, so a re-cache can produce a *different* file and leave the
/// original's mtime untouched. Comparing the whole set catches that, where comparing one
/// file's mtime would not.
fn cache_files(cache_dir: &std::path::Path) -> Vec<(String, u64)> {
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
    walk(&cache_dir.join("ranges"), &mut all);
    let mut out: Vec<(String, u64)> = all
        .into_iter()
        .filter(|p| p.to_string_lossy().contains(BUCKET))
        // The marker itself is not cached data, and it is created by the very act under
        // test, so it must not count as "an entry was written".
        .filter(|p| p.file_name().map(|n| n != "_origin.json").unwrap_or(true))
        // A key's write-order marker (`{object}.wtok`) is stamped by every acknowledged write
        // or delete and is not cached data either.
        .filter(|p| p.extension().map(|e| e != "wtok").unwrap_or(true))
        .map(|p| {
            let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            (p.to_string_lossy().to_string(), len)
        })
        .collect();
    out.sort();
    out
}

fn marker_file(cache_dir: &std::path::Path) -> std::path::PathBuf {
    cache_dir.join("metadata").join(BUCKET).join("_origin.json")
}

const S3_HOST: &str = "s3.us-west-2.amazonaws.com";

/// A bucket the S3 host has claimed, with `object_path()` cached as `BODY_A` and confirmed a
/// hit, behind a proxy whose stub answers `AUTH_A` with `BODY_A` and `AUTH_B` with `BODY_B`.
struct S3ClaimedBucket {
    tmp: TempDir,
    server: ProxyServer,
}

async fn s3_claimed_bucket() -> S3ClaimedBucket {
    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(
        StubS3Client::new()
            .with_response_for_authorization(AUTH_A, ok_body(BODY_A))
            .with_response_for_authorization(AUTH_B, ok_body(BODY_B)),
    );
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let (status, _h, body) = proxy_get(server.addr, S3_HOST, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A);
    let marker = std::fs::read_to_string(marker_file(tmp.path()))
        .expect("fixture precondition: the S3 host's request must claim the marker");
    assert!(
        marker.contains("\"origin\":\"amazonaws.com\""),
        "fixture precondition: the marker must hold the partition identity, or the attack \
         request reaches the mismatch branch rather than the one under test: {}",
        marker
    );
    let (_s, headers, _b) = proxy_get(server.addr, S3_HOST, &object_path(), AUTH_A).await;
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "fixture precondition: the S3 entry must be a cache hit"
    );
    S3ClaimedBucket { tmp, server }
}

/// An absolute-form (forward-proxy) request with an explicit `Host` header.
///
/// A raw `conn::http1` handshake sends the request-target exactly as given, so the URI
/// authority and the `Host` header are independent inputs, which is the split under test.
async fn forward_proxy_get(
    addr: SocketAddr,
    absolute_uri: &str,
    host: &str,
    auth: &str,
) -> (StatusCode, HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect to proxy");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method("GET")
        .uri(absolute_uri)
        .header("host", host)
        .header("authorization", auth)
        .body(Full::new(Bytes::new()))
        .expect("build request");
    let resp = sender
        .send_request(req)
        .await
        .expect("proxy request failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    (status, headers, body)
}

/// A non-S3 AWS host cannot WRITE a bucket an S3 host claimed.
///
/// The poisoning shape: anyone can create an `execute-api` endpoint serving any bytes at any
/// path. With the partition identity it got `Matches`, so its response to a cold key was
/// cached under the victim's key and the victim was then served it. Asserted at the byte
/// level from the victim's side, plus the cache file set, plus the bypass counter.
#[tokio::test]
async fn a_non_s3_aws_host_does_not_write_an_s3_claimed_bucket() {
    let _serialised = COUNTER_LOCK.lock().await;
    const ATTACKER_HOST: &str = "abc.execute-api.us-east-1.amazonaws.com";
    let cold_path = format!("/{}/cold.bin", BUCKET);

    // Vacuity guard: both hosts key to the same entry, or there is nothing to poison.
    assert_eq!(
        CacheManager::generate_cache_key(&cold_path, Some(S3_HOST)),
        CacheManager::generate_cache_key(&cold_path, Some(ATTACKER_HOST)),
    );

    let s = s3_claimed_bucket().await;
    let files_before = cache_files(s.tmp.path());
    let bypass_before = uncacheable_aws_host_bypasses_total();

    let (status, _h, body) = proxy_get(s.server.addr, ATTACKER_HOST, &cold_path, AUTH_B).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_B);
    let files_after_attack = cache_files(s.tmp.path());
    let bypass_after = uncacheable_aws_host_bypasses_total();

    // The victim's own request for the cold key must reach its upstream, not the attacker's
    // cached bytes.
    let (status, headers, body) = proxy_get(s.server.addr, S3_HOST, &cold_path, AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_ref(),
        BODY_A,
        "the S3 host was served bytes a non-S3 AWS host's response cached under its key \
         (x-cache {:?})",
        x_cache(&headers)
    );
    assert_eq!(
        files_after_attack, files_before,
        "a non-S3 AWS host's responses must write no cache entry under the S3 bucket's keys"
    );
    assert!(
        bypass_after > bypass_before,
        "the attack request must be counted as an uncacheable-host bypass"
    );
}

/// A non-S3 AWS host cannot READ a bucket an S3 host claimed: it is forwarded to its own
/// upstream rather than served the S3 entry, and the entry still serves afterwards.
#[tokio::test]
async fn a_non_s3_aws_host_is_not_served_an_s3_claimed_entry() {
    let _serialised = COUNTER_LOCK.lock().await;
    const ATTACKER_HOST: &str = "abc.execute-api.us-east-1.amazonaws.com";
    assert_eq!(
        CacheManager::generate_cache_key(&object_path(), Some(S3_HOST)),
        CacheManager::generate_cache_key(&object_path(), Some(ATTACKER_HOST)),
    );

    let s = s3_claimed_bucket().await;
    let bypass_before = uncacheable_aws_host_bypasses_total();

    let (status, headers, body) =
        proxy_get(s.server.addr, ATTACKER_HOST, &object_path(), AUTH_B).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_ref(),
        BODY_B,
        "a non-S3 AWS host must be answered by its own upstream, not served the S3 entry"
    );
    assert_ne!(x_cache(&headers), Some("HIT"));
    assert!(
        uncacheable_aws_host_bypasses_total() > bypass_before,
        "the attack request must be counted as an uncacheable-host bypass"
    );

    let (_s, headers, body) = proxy_get(s.server.addr, S3_HOST, &object_path(), AUTH_A).await;
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(x_cache(&headers), Some("HIT"));
}

/// A forward-proxy request whose `Host` header names a different bucket's host than its
/// request target cannot WRITE the target's bucket.
///
/// The cache key and origin identity come from the request-target authority, but the `Host`
/// header is forwarded verbatim and S3 routes on it. So
/// `GET http://s3.us-west-2.amazonaws.com/mybucket/cold.bin` with
/// `Host: attacker.s3.us-west-2.amazonaws.com` would be keyed `mybucket/cold.bin` while S3
/// answered with the attacker bucket's object of that name. The proxy must refuse it, and the
/// victim's own request for the key must then reach its upstream.
#[tokio::test]
async fn a_forward_proxy_host_header_naming_another_host_cannot_write_the_target_bucket() {
    let _serialised = COUNTER_LOCK.lock().await;
    const ATTACKER_HOST: &str = "attacker.s3.us-west-2.amazonaws.com";
    let cold_path = format!("/{}/cold.bin", BUCKET);
    let target = format!("http://{}{}", S3_HOST, cold_path);

    let s = s3_claimed_bucket().await;

    // Positive control: a correctly-formed forward-proxy GET (Host equal to the target's
    // authority) reaches the cache and is served the claimed entry.
    let (status, headers, body) = forward_proxy_get(
        s.server.addr,
        &format!("http://{}{}", S3_HOST, object_path()),
        S3_HOST,
        AUTH_A,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "fixture precondition: a matching forward-proxy GET must share the direct-mode entry"
    );
    let files_before = cache_files(s.tmp.path());

    let (attack_status, _h, _b) =
        forward_proxy_get(s.server.addr, &target, ATTACKER_HOST, AUTH_B).await;
    let files_after_attack = cache_files(s.tmp.path());

    let (status, headers, body) = forward_proxy_get(s.server.addr, &target, S3_HOST, AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_ref(),
        BODY_A,
        "the victim was served bytes cached from a request whose Host named another bucket \
         (x-cache {:?})",
        x_cache(&headers)
    );
    assert_eq!(
        files_after_attack, files_before,
        "a Host/target mismatch must write no cache entry under the target's keys"
    );
    assert_eq!(
        attack_status,
        StatusCode::BAD_REQUEST,
        "a Host header naming another host than the request target must be refused"
    );
}

/// A direct-mode request with two `Host` headers cannot WRITE the first host's bucket.
///
/// The proxy classifies and keys on the first `Host` value, while the header map it forwards
/// keeps the last, so S3 would route on a host the proxy never classified.
#[tokio::test]
async fn a_second_host_header_cannot_write_the_first_hosts_bucket() {
    let _serialised = COUNTER_LOCK.lock().await;
    const ATTACKER_HOST: &str = "attacker.s3.us-west-2.amazonaws.com";
    let cold_path = format!("/{}/cold.bin", BUCKET);

    let s = s3_claimed_bucket().await;
    let files_before = cache_files(s.tmp.path());

    let stream = TcpStream::connect(s.server.addr)
        .await
        .expect("connect to proxy");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method("GET")
        .uri(cold_path.as_str())
        .header("host", S3_HOST)
        .header("host", ATTACKER_HOST)
        .header("authorization", AUTH_B)
        .body(Full::new(Bytes::new()))
        .expect("build request");
    assert_eq!(
        req.headers().get_all("host").iter().count(),
        2,
        "fixture precondition: the request must carry two Host headers"
    );
    let resp = sender
        .send_request(req)
        .await
        .expect("proxy request failed");
    let attack_status = resp.status();
    let _ = resp.into_body().collect().await;
    let files_after_attack = cache_files(s.tmp.path());

    let (status, headers, body) = proxy_get(s.server.addr, S3_HOST, &cold_path, AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_ref(),
        BODY_A,
        "the first host was served bytes cached from a request routed on a second Host \
         (x-cache {:?})",
        x_cache(&headers)
    );
    assert_eq!(files_after_attack, files_before);
    assert_eq!(attack_status, StatusCode::BAD_REQUEST);
}

/// A forward-proxy request whose `Host` header conflicts with its request target is not
/// served the target's cached entry either; it is refused before any cache lookup.
///
/// This pins the refusal contract (400, entry untouched). It is not a second red side for
/// the poisoning defect: without the conflict check this request would be served the
/// target's entry as a cache hit, which any client that can reach the proxy may already
/// read at TTL > 0, and nothing would be written under the wrong key. The poisoning red side
/// is `a_forward_proxy_host_header_naming_another_host_cannot_write_the_target_bucket`.
#[tokio::test]
async fn a_forward_proxy_host_header_naming_another_host_is_not_served_the_target_entry() {
    let _serialised = COUNTER_LOCK.lock().await;
    const ATTACKER_HOST: &str = "attacker.s3.us-west-2.amazonaws.com";
    let target = format!("http://{}{}", S3_HOST, object_path());

    let s = s3_claimed_bucket().await;

    let (status, headers, body) =
        forward_proxy_get(s.server.addr, &target, ATTACKER_HOST, AUTH_B).await;
    assert_ne!(
        body.as_ref(),
        BODY_A,
        "a Host/target mismatch was served the target's cached entry (status {}, x-cache {:?})",
        status,
        x_cache(&headers)
    );
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The entry is untouched and still serves a correctly-formed request.
    let (_s, headers, body) = forward_proxy_get(s.server.addr, &target, S3_HOST, AUTH_A).await;
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(x_cache(&headers), Some("HIT"));
}

/// The two origins collide on one cache key, and the SECOND is forwarded rather than
/// answered with the FIRST's bytes.
///
/// This is the defect Requirement 2 exists for, asserted at the byte level.
#[tokio::test]
async fn a_second_origin_is_forwarded_and_never_served_the_first_origins_bytes() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;
    // ── Vacuity guards, both directions ──────────────────────────────────────
    assert_ne!(
        origin_identity(ORIGIN_A),
        origin_identity(ORIGIN_B),
        "these two hosts must have DIFFERENT origin identities, or this test never reaches \
         the mismatch branch and passes whether or not the guard exists"
    );
    assert_eq!(
        CacheManager::generate_cache_key(&object_path(), Some(ORIGIN_A)),
        CacheManager::generate_cache_key(&object_path(), Some(ORIGIN_B)),
        "the two origins must produce the SAME cache key, or there is no collision to guard \
         and the test asserts nothing"
    );
    assert_eq!(
        CacheManager::generate_cache_key(&object_path(), Some(ORIGIN_A)),
        expected_cache_key()
    );

    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(
        StubS3Client::new()
            .with_response_for_authorization(AUTH_A, ok_body(BODY_A))
            .with_response_for_authorization(AUTH_B, ok_body(BODY_B)),
    );
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let mismatch_before = origin_marker_mismatch_bypasses_total();

    // ── Origin A: cold miss, then a warm hit proving the entry is real ────────
    let (status, _h, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A, "origin A must receive its own bytes");
    assert!(
        marker_file(tmp.path()).exists(),
        "the first request for an unclaimed bucket must claim the marker"
    );

    let (status, headers, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "fixture precondition: origin A's second read must be a cache HIT, or there is no \
         cached entry for origin B to be wrongly served and the assertion below is vacuous"
    );

    let files_after_a = cache_files(tmp.path());
    assert!(
        !files_after_a.is_empty(),
        "fixture precondition: origin A's read must have written cache files"
    );

    // ── Origin B: same bucket, same key, different origin ─────────────────────
    let (status, headers, body) = proxy_get(server.addr, ORIGIN_B, &object_path(), AUTH_B).await;
    assert_eq!(status, StatusCode::OK);

    // THE ASSERTION. Pre-fix this returns BODY_A from the cache.
    assert_eq!(
        body.as_ref(),
        BODY_B,
        "origin B must be answered by origin B. Receiving origin A's bytes ({:?}) is the \
         cross-origin collision this requirement closes: a non-AWS host contributes nothing \
         to the cache key, so both origins key to `{}`",
        String::from_utf8_lossy(BODY_A),
        expected_cache_key()
    );
    assert_ne!(
        x_cache(&headers),
        Some("HIT"),
        "origin B's request must not be reported as a cache hit"
    );

    // The bypass is observable rather than presenting as an unexplained hit-rate drop.
    assert!(
        origin_marker_mismatch_bypasses_total() > mismatch_before,
        "a mismatch bypass must increment cache.origin_marker_mismatch_bypasses_total"
    );

    // ── Bypass, not invalidate: origin A's entry is untouched and still serves ─
    assert_eq!(
        cache_files(tmp.path()),
        files_after_a,
        "origin B's request must write NO cache entry and must not remove origin A's. An \
         eager invalidation here would let a second origin's traffic EVICT the first \
         origin's cache, which is worse than the bug being fixed (acceptance criterion 4)"
    );

    let (status, headers, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_ref(),
        BODY_A,
        "origin A must still be served its own cached bytes after origin B's bypass"
    );
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "origin A's entry must still be a cache hit — the bypass forwarded, it did not \
         invalidate"
    );
}

/// A mismatched DELETE leaves the recorded origin's entry intact — and a matching one removes
/// it, which is what proves this fixture can tell the two apart.
///
/// This is acceptance criterion 4, and the half most easily got wrong by reusing the
/// `read_cache_enabled: false` gate wholesale: that gate forwards **and eagerly invalidates**,
/// and every successful DELETE ordinarily invalidates too. Doing either on a mismatch lets one
/// origin's traffic evict another origin's cache — strictly worse than the bug being fixed.
///
/// # The matching-origin arm is not decoration, it is what makes the other arm mean anything
///
/// An earlier version of this test asserted only that a mismatched DELETE changed nothing, and
/// it **passed against a build with the invalidation suppression deliberately removed** —
/// measured, not hypothesised. The cause was that it used a *signed* DELETE, and
/// `forward_signed_request` does not complete against the stub upstream: it returns 502, so
/// `response.status().is_success()` is false and the invalidation never runs either way. Two
/// entirely different states produced the same "nothing was deleted" reading.
///
/// So this test drives the **unsigned** DELETE arm, whose forward goes through
/// `s3_client.forward_request` and therefore through the stub, and it asserts BOTH directions:
/// the mismatched DELETE must change nothing, and the matching DELETE must remove the files.
/// The second is the control, and without it the first cannot be distinguished from an
/// invalidation that was never reachable.
///
/// # What this does NOT cover
///
/// The **signed** PUT and DELETE guards are not exercised end-to-end here, because neither
/// signed forward completes against a stub upstream (both 502 before reaching a success
/// branch). Their guards are the same shape as the one asserted here and sit on the same
/// predicate, but that is an argument, not a measurement. Direction of risk: a signed-write
/// path that invalidated on a mismatch would delete another origin's entry and nothing here
/// would fail.
#[tokio::test]
async fn a_mismatched_delete_does_not_invalidate_but_a_matching_one_does() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;
    assert_ne!(
        origin_identity(ORIGIN_A),
        origin_identity(ORIGIN_B),
        "vacuity guard: the two origins must have different identities, or this test never \
         reaches the mismatch branch"
    );

    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(
        StubS3Client::new()
            .with_response_for_authorization(AUTH_A, ok_body(BODY_A))
            .with_default(ok_body(BODY_A)),
    );
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    // Origin A caches the object.
    let (_s, _h, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(body.as_ref(), BODY_A);
    let (_s, headers, _b) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "fixture precondition: origin A must hold a real cached entry, or there is nothing \
         for a mismatched write to wrongly invalidate"
    );
    let files_after_a = cache_files(tmp.path());
    assert!(
        !files_after_a.is_empty(),
        "fixture precondition: cached files must exist"
    );

    // ── A mismatched DELETE: must change nothing ─────────────────────────────
    let (status, _h, _b) = unsigned_request(server.addr, "DELETE", ORIGIN_B, &object_path()).await;
    assert!(
        status.is_success(),
        "fixture precondition: the DELETE must reach S3 SUCCESSFULLY, or the invalidation it \
         would have performed is unreachable and this assertion cannot fail. Got {}",
        status
    );
    assert_eq!(
        cache_files(tmp.path()),
        files_after_a,
        "a mismatched DELETE must not invalidate the recorded origin's entry — those bytes \
         remain valid for the origin that cached them (acceptance criterion 4)"
    );
    let (_s, headers, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "origin A's entry must still serve after a mismatched DELETE"
    );

    // ── THE CONTROL: a MATCHING DELETE must remove it ────────────────────────
    // Same path, same fixture, same request shape — only the origin differs. This is what
    // establishes that the assertion above could have failed.
    let (status, _h, _b) = unsigned_request(server.addr, "DELETE", ORIGIN_A, &object_path()).await;
    assert!(status.is_success(), "the matching DELETE must reach S3");
    assert!(
        cache_files(tmp.path()).is_empty(),
        "the recorded origin's own DELETE must invalidate normally. If this fails, the \
         mismatch assertion above proves nothing: an invalidation that never runs looks \
         identical to one correctly suppressed. Files still present: {:?}",
        cache_files(tmp.path())
    );
}

/// One bucket reached path-style and virtual-hosted through AWS hosts does **not** bypass.
///
/// The two-sided control, and the assertion that fails if the matcher misses the path-style
/// families: `cache::extract_virtual_hosted_bucket` returns `None` for
/// `s3.<region>.amazonaws.com` by design, so a matcher assembled from the existing bucket
/// extractors would classify the most common AWS access form as a non-AWS origin, record it
/// verbatim, and then bypass — breaking the collapsing that roughly a thousand lines of
/// `tests/cache_key_virtual_hosted_test.rs` exist to pin.
#[tokio::test]
async fn one_aws_bucket_reached_two_ways_does_not_bypass() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;
    let path_style_host = "s3.us-east-1.amazonaws.com";
    let virtual_hosted_host = format!("{}.s3.us-east-1.amazonaws.com", BUCKET);

    // ── Vacuity guard, the mirror direction ──────────────────────────────────
    assert_eq!(
        origin_identity(path_style_host),
        origin_identity(&virtual_hosted_host),
        "these two hosts must have the SAME origin identity, or this test asserts nothing \
         about the guard — it would be observing two genuinely different origins"
    );
    assert_eq!(
        CacheManager::generate_cache_key(&object_path(), Some(path_style_host)),
        CacheManager::generate_cache_key(&format!("/{}", OBJECT_KEY), Some(&virtual_hosted_host)),
        "fixture precondition: the two addressing styles must key to the same entry"
    );

    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(BODY_A)));
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let mismatch_before = origin_marker_mismatch_bypasses_total();

    // Path-style caches it.
    let (status, _h, body) = proxy_get(server.addr, path_style_host, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A);

    // Virtual-hosted must be a HIT on the same entry.
    let (status, headers, body) = proxy_get(
        server.addr,
        &virtual_hosted_host,
        &format!("/{}", OBJECT_KEY),
        AUTH_A,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "a bucket reached path-style and then virtual-hosted through AWS hosts must be ONE \
         origin and hit the shared entry. A miss here means the matcher failed to collapse a \
         family — most likely the path-style forms, which the existing bucket extractors \
         deliberately return None for"
    );
    assert_eq!(
        origin_marker_mismatch_bypasses_total(),
        mismatch_before,
        "no bypass may be counted for two hostname forms of one AWS bucket"
    );
}

/// A bucket cached before markers existed keeps serving, and is claimed on first touch.
///
/// **This is the upgrade-safety property, and it is the one a careless fix breaks.** An
/// existing deployment has no markers at all. Treating an absent marker as a mismatch would
/// make the release bypass the cache for every bucket on the volume — a fleet-wide caching
/// outage presenting as a mysterious collapse in hit rate, from a change whose changelog says
/// it needs no manual step.
#[tokio::test]
async fn an_entry_cached_before_markers_existed_still_serves_and_the_bucket_is_claimed() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(BODY_A)));
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let mismatch_before = origin_marker_mismatch_bypasses_total();
    let unparseable_before = origin_marker_unparseable_bypasses_total();

    // Cache the object, then delete the marker and clear the process memo by building a
    // second CacheManager over the same directory — which is exactly the pre-upgrade state:
    // cached entries present, no marker anywhere.
    let (_s, _h, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(body.as_ref(), BODY_A);
    let files_before = cache_files(tmp.path());
    assert!(
        !files_before.is_empty(),
        "fixture precondition: an entry must exist to be served after the upgrade"
    );

    std::fs::remove_file(marker_file(tmp.path())).expect("remove the marker");
    assert!(
        !marker_file(tmp.path()).exists(),
        "fixture precondition: the pre-upgrade state is NO marker. Without this the test \
         exercises the match path, not the migration path"
    );

    // A fresh process over the same volume: no memo, no marker.
    let (fresh_manager, fresh_range_handler) = make_cache_infra(&config).await;
    let fresh_server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&fresh_manager),
        fresh_range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let (status, headers, body) =
        proxy_get(fresh_server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A);
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "an entry cached before any marker existed MUST still be served. An absent marker is \
         a no-op; treating it as a mismatch would bypass the cache for every bucket on an \
         upgraded deployment"
    );
    assert_eq!(
        origin_marker_mismatch_bypasses_total(),
        mismatch_before,
        "an absent marker must count no mismatch bypass"
    );
    assert_eq!(
        origin_marker_unparseable_bypasses_total(),
        unparseable_before,
        "an absent marker must not be counted as unparseable either"
    );
    assert_eq!(
        cache_files(tmp.path()),
        files_before,
        "the claim must not disturb the cached entry"
    );

    // And the bucket is now claimed, so the guard engages from here on.
    assert!(
        marker_file(tmp.path()).exists(),
        "first touch must claim the marker, or the bucket stays unguarded forever"
    );
    let recorded: serde_json::Value =
        serde_json::from_slice(&std::fs::read(marker_file(tmp.path())).unwrap()).unwrap();
    assert_eq!(
        recorded["origin"].as_str(),
        origin_identity(ORIGIN_A).as_deref()
    );
    assert_eq!(recorded["schema"], 1);
}

/// A present-but-unparseable marker bypasses, and is counted apart from a mismatch.
///
/// Neither reading is safe: treating it as absent reintroduces the collision, treating it as
/// a mismatch disables caching for the bucket. Bypassing is the conservative direction and
/// the separate counter is what stops it being silent.
#[tokio::test]
async fn an_unparseable_marker_bypasses_and_is_counted_apart() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(BODY_A)));
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    // Cache the object first, so a bypass is distinguishable from having nothing to serve.
    let (_s, _h, _b) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    let (_s, headers, _b) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "fixture precondition: a real cached entry must exist"
    );
    let files_before = cache_files(tmp.path());

    // Corrupt the marker, and use a fresh process so the memo is not already populated.
    std::fs::write(marker_file(tmp.path()), b"this is not a marker").unwrap();
    let (fresh_manager, fresh_range_handler) = make_cache_infra(&config).await;
    let fresh_server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&fresh_manager),
        fresh_range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let mismatch_before = origin_marker_mismatch_bypasses_total();
    let unparseable_before = origin_marker_unparseable_bypasses_total();

    let (status, headers, body) =
        proxy_get(fresh_server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_ref(),
        BODY_A,
        "the object must still be served — from S3"
    );
    assert_ne!(
        x_cache(&headers),
        Some("HIT"),
        "an unparseable marker must bypass the cache rather than be ignored: treating it as \
         absent would reintroduce the cross-origin collision"
    );
    assert!(
        origin_marker_unparseable_bypasses_total() > unparseable_before,
        "an unparseable marker must move its OWN counter — the cause and the operator's fix \
         differ from an ordinary origin mismatch"
    );
    assert_eq!(
        origin_marker_mismatch_bypasses_total(),
        mismatch_before,
        "and it must NOT be counted as an ordinary mismatch"
    );
    assert_eq!(
        cache_files(tmp.path()),
        files_before,
        "bypassing must not invalidate — the entry may still be the recorded origin's"
    );
    assert_eq!(
        std::fs::read(marker_file(tmp.path())).unwrap(),
        b"this is not a marker",
        "a damaged marker must not be silently healed by a claim: whichever origin arrived \
         first would rebind a bucket whose real owner is unknown"
    );
}

/// An operation the proxy never caches consults **no** origin marker and leaves **none** behind.
///
/// The marker read is a shared-storage round trip on the first touch of a bucket in a process, and
/// a claim leaves a file in the bucket's directory permanently — that directory is then never
/// removed when it empties, because no cleanup site protects it and none of them will delete a
/// non-empty one. Neither cost is worth paying for a request whose response is forwarded uncached,
/// so every `should_bypass_cache` condition and the `versionId` read are decided **above** the
/// cache key, the rule resolution and the origin guard.
///
/// # Why the absence of the FILE is the assertion, and the response is not
///
/// A bypassed operation is forwarded either way, so its status and body are identical whether or
/// not the marker was consulted — they cannot discriminate. The observable that can is the
/// **filesystem**: a claim creates `metadata/{bucket}/_origin.json`, so its absence after the
/// request is direct evidence that `read_or_claim` was never reached.
///
/// The control is what makes that non-vacuous. A cacheable `GET` for the **same bucket** runs at
/// the end and must create the marker, which establishes that this fixture's bucket is claimable
/// at all — without it, "no marker" is indistinguishable from a bucket the proxy could never have
/// claimed for an unrelated reason.
///
/// Each operation is driven against its **own bucket**, because a marker is per-bucket and one
/// claim would mask every subsequent case.
#[tokio::test]
async fn an_operation_the_proxy_never_caches_reads_no_marker_and_claims_none() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;

    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(BODY_A)));
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    // Object-scoped paths, so the cache key HAS a bucket segment and `parse_cache_key` accepts
    // it. That is the precondition: a single-segment key is rejected before the marker is
    // consulted anyway, so a bucket-scoped LIST would pass this test without the reorder and
    // prove nothing. Every one of these is `/{bucket}/{key}?...`.
    let object_scoped = [
        ("acl-bucket", "?acl"),
        ("attributes-bucket", "?attributes"),
        ("legalhold-bucket", "?legal-hold"),
        ("retention-bucket", "?retention"),
        ("tagging-bucket", "?tagging"),
        ("torrent-bucket", "?torrent"),
        ("versionid-bucket", "?versionId=abc123"),
    ];

    for (bucket, query) in object_scoped {
        let key = format!("/{}/{}", bucket, OBJECT_KEY);
        // Fixture precondition: this really is a multi-segment key, so the bucket is claimable.
        let ck = CacheManager::generate_cache_key(&key, Some(ORIGIN_A));
        assert!(
            s3_proxy::disk_cache::parse_cache_key(&ck).is_ok(),
            "fixture precondition: `{}` must yield a cache key with a bucket segment, or the \
             marker is skipped for an unrelated reason and this case proves nothing",
            key
        );

        let (status, _h, _b) =
            proxy_get(server.addr, ORIGIN_A, &format!("{}{}", key, query), AUTH_A).await;
        assert!(
            status.is_success(),
            "fixture precondition: `{}{}` must reach S3, got {}",
            key,
            query,
            status
        );

        let marker = tmp
            .path()
            .join("metadata")
            .join(bucket)
            .join("_origin.json");
        assert!(
            !marker.exists(),
            "`{}` is never cached, so it must not have claimed an origin marker at {:?}. A claim \
             here is a shared-storage write and a permanent file for a request the proxy forwards \
             uncached — and because no cleanup site protects a bucket directory under metadata/, \
             the directory it creates is never removed either.",
            query,
            marker
        );
        assert!(
            !tmp.path().join("metadata").join(bucket).exists(),
            "`{}` must not even create the bucket directory",
            query
        );
    }

    // ── THE CONTROL: a cacheable GET for one of those same buckets MUST claim ────
    // Without this, "no marker" is indistinguishable from a bucket that could never have been
    // claimed, and every assertion above would pass against a build that skips the marker
    // entirely.
    let cacheable = format!("/{}/{}", "tagging-bucket", OBJECT_KEY);
    let (status, _h, _b) = proxy_get(server.addr, ORIGIN_A, &cacheable, AUTH_A).await;
    assert!(status.is_success());
    let marker = tmp
        .path()
        .join("metadata")
        .join("tagging-bucket")
        .join("_origin.json");
    assert!(
        marker.exists(),
        "a CACHEABLE GET for the same bucket must claim the marker. If this fails, the assertions \
         above prove nothing — a bucket that is never claimable would satisfy all of them."
    );
}

/// A bypassed operation carrying an **expired presigned URL** is still rejected by the proxy, not
/// forwarded to S3.
///
/// This pins the one ordering constraint inside the hoisted preamble. The never-cached bypasses
/// were moved above the cache key and the origin marker; the presigned-URL check was moved **with
/// them, and stays ahead of them**. Hoisting the forwards alone would have sent an expired
/// presigned request upstream instead of refusing it here — the client still fails, since S3
/// rejects the stale signature too, but the proxy stops short-circuiting and burns an upstream
/// round trip on a request it is capable of refusing itself.
///
/// The discriminator is **`StubS3Client::captured()`**, not the status code. A forwarded request
/// would come back with the stub's own status, which in a real deployment would be S3's 403 — the
/// same code the proxy returns. So the status alone cannot distinguish "refused here" from
/// "refused upstream"; only the absence of an upstream call can. The status and the proxy's own
/// body text are asserted too, as corroboration rather than as the test.
#[tokio::test]
async fn a_bypassed_operation_with_an_expired_presigned_url_is_still_rejected_here() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;

    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(BODY_A)));
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    // `?tagging` is a `should_bypass_cache` condition on an object-scoped path, so it is one of
    // the requests the hoist moved above the marker. The presigned parameters are long expired:
    // signed in 2020 with a one-second window.
    let target = format!("/{}/{}", "presigned-bucket", OBJECT_KEY);
    let expired = format!(
        "{}?tagging&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20200101T000000Z&X-Amz-Expires=1",
        target
    );

    let before = stub.captured().len();
    let (status, _h, body) = proxy_get(server.addr, ORIGIN_A, &expired, AUTH_A).await;
    let upstream_calls = stub
        .captured()
        .iter()
        .filter(|r| r.uri.contains("presigned-bucket"))
        .count();

    // THE ASSERTION: nothing reached the upstream.
    assert_eq!(
        upstream_calls,
        0,
        "an expired presigned URL must be refused by the proxy, not forwarded. A forward here \
         means the bypass decision overtook the presigned check — which is exactly what hoisting \
         the never-cached forwards WITHOUT hoisting the presigned check with them would do. \
         (stub captured {} request(s) in total, {} before this one)",
        stub.captured().len(),
        before
    );
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "the proxy's own rejection is a 403"
    );
    assert!(
        String::from_utf8_lossy(&body).contains("Presigned URL has expired"),
        "and it carries the proxy's own body rather than an upstream response, got {:?}",
        String::from_utf8_lossy(&body)
    );

    // And it claimed no marker either — a request refused before the cache key exists cannot.
    assert!(
        !tmp.path()
            .join("metadata")
            .join("presigned-bucket")
            .exists(),
        "a request refused at the presigned check must not have created a bucket directory"
    );
}

/// A cache key with no bucket segment reports `Matches`, and **nothing can be cached under such
/// a key**, which is what makes that safe rather than a hole.
///
/// `CacheManager::origin_verdict` derives the bucket from the cache key's first segment, and
/// `disk_cache::parse_cache_key` rejects a single-segment key. Returning `Matches` there looks
/// like an unguarded path — a MinIO virtual-hosted request (`Host: mybucket.minio-a.example.com`,
/// `GET /key`) produces exactly such a key, and two origins would produce the same one.
///
/// The reason it is not a hole is asserted here rather than argued: `get_sharded_path` calls
/// `parse_cache_key` and propagates its error, so no `.meta` and no `.bin` can be written under a
/// bucketless key at all. There is no entry for a second origin to be served. Both halves are
/// asserted, because the verdict alone would be the adjacent predicate — it would pass equally
/// against a build where such keys *could* be stored.
#[tokio::test]
async fn a_bucketless_cache_key_matches_because_nothing_can_be_cached_under_it() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;

    // The shape a non-AWS virtual-hosted request produces: the host contributes nothing and the
    // path is a single segment.
    let bucketless = CacheManager::generate_cache_key("/key", Some("mybucket.minio-a.example.com"));
    assert_eq!(
        bucketless, "key",
        "fixture precondition: this host/path pair must produce a single-segment cache key"
    );

    // Half one: nothing can be stored under it. This is the load-bearing half.
    let tmp = TempDir::new().unwrap();
    for suffix in [".meta", "_0-1023.bin"] {
        assert!(
            s3_proxy::disk_cache::get_sharded_path(
                &tmp.path().join("metadata"),
                &bucketless,
                suffix
            )
            .is_err(),
            "a bucketless cache key must have NO storable path for `{}` — that is the entire \
             reason the origin guard can report Matches for it. If this starts succeeding, the \
             guard has an unprotected path and `origin_verdict` needs a real answer here",
            suffix
        );
    }

    // Half two: the verdict is Matches, for both of two different origins.
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, _rh) = make_cache_infra(&config).await;
    for origin_host in [ORIGIN_A, ORIGIN_B] {
        assert!(
            cache_manager
                .origin_verdict(&bucketless, origin_host)
                .await
                .permits_cache(),
            "a bucketless cache key must not bypass — there is no bucket to bind and no entry \
             to protect, so bypassing would only disable caching for a key that caches nothing"
        );
    }
    assert!(
        !tmp.path().join("metadata").join("key").exists(),
        "and no marker directory may be created for a non-bucket"
    );
}

/// Concurrent claims on a fresh bucket converge on one value.
///
/// `link` is atomic and fails `EEXIST`, so exactly one claim publishes and every other reads
/// the winner. There is no lock. Driven through the real request path so the convergence
/// asserted is the one production performs.
#[tokio::test]
async fn concurrent_claims_converge_on_one_origin() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;
    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(BODY_A)));

    // Two independent proxies over ONE cache volume, which is the shared-storage
    // deployment. A single process would serialise on its own memo and never race the file.
    let (manager_a, rh_a) = make_cache_infra(&config).await;
    let (manager_b, rh_b) = make_cache_infra(&config).await;
    let server_a = spawn_proxy_server(
        Arc::clone(&config),
        manager_a,
        rh_a,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;
    let server_b = spawn_proxy_server(
        Arc::clone(&config),
        manager_b,
        rh_b,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    assert!(
        !marker_file(tmp.path()).exists(),
        "fixture precondition: the bucket must be unclaimed, or there is no race"
    );

    // Two origins claiming the same fresh bucket at once. Whichever wins, both must agree,
    // and the file must be a single valid marker rather than a blend of two writes.
    let path = object_path();
    let (a, b) = tokio::join!(
        proxy_get(server_a.addr, ORIGIN_A, &path, AUTH_A),
        proxy_get(server_b.addr, ORIGIN_B, &path, AUTH_A),
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);

    let bytes = std::fs::read(marker_file(tmp.path())).expect("a marker must exist");
    let recorded: serde_json::Value =
        serde_json::from_slice(&bytes).expect("the marker must be valid JSON, never a blend");
    assert_eq!(recorded["schema"], 1);
    let origin = recorded["origin"]
        .as_str()
        .expect("origin must be a string");
    assert!(
        Some(origin) == origin_identity(ORIGIN_A).as_deref()
            || Some(origin) == origin_identity(ORIGIN_B).as_deref(),
        "the marker must record exactly one of the two racing origins, got {:?}",
        origin
    );

    // No temp file survives either outcome — a leak would accumulate one per instance per
    // bucket, and nothing sweeps this directory.
    let stray: Vec<String> = std::fs::read_dir(tmp.path().join("metadata").join(BUCKET))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains(".tmp."))
        .collect();
    assert!(
        stray.is_empty(),
        "the claim must unlink its temp file on both outcomes, found {:?}",
        stray
    );

    // Both proxies now agree, and the loser's origin bypasses on its next request.
    let both_agree = {
        let (_s, _h, _b) = proxy_get(server_a.addr, ORIGIN_A, &object_path(), AUTH_A).await;
        let (_s, _h, _b) = proxy_get(server_b.addr, ORIGIN_B, &object_path(), AUTH_A).await;
        std::fs::read(marker_file(tmp.path())).unwrap()
    };
    assert_eq!(
        both_agree, bytes,
        "a later request from either origin must not rewrite the recorded value"
    );
}

/// The origin that claims a bucket **is** served entries a different origin cached before any
/// marker existed.
///
/// **This test pins an ACCEPTED RESIDUAL, not a bug.** Do not "fix" it without a decision:
/// no marker design can retroactively attribute entries that predate the claim, because a
/// regular-bucket cache key is `{bucket}/{path}` with no origin in it, so the bytes origin A
/// wrote sit under exactly the keys origin B looks up. The guarantee starts at the claim.
/// Recorded against the public security review readiness feature task 2.15, whose own
/// wording ("A's entries are then not served") is true of requests from **A** only.
///
/// # This is the two-origin half of the upgrade fixture
///
/// `an_entry_cached_before_markers_existed_still_serves_and_the_bucket_is_claimed` builds the
/// same pre-upgrade state — entry cached, marker deleted, fresh `CacheManager` so the process
/// memo is empty — and then requests again **from the same origin**. It establishes upgrade
/// safety and says nothing about whose entry it was. This test changes exactly one thing: the
/// post-upgrade request comes from `ORIGIN_B`.
///
/// # Why the per-origin stub is load-bearing
///
/// The upgrade fixture uses `with_default(ok_body(BODY_A))` — one body for every request. Copy
/// that, and a cache HIT serving A's bytes and a MISS forwarded to B's own origin return
/// **byte-identical** responses, so the assertion cannot tell the two states apart. The bodies
/// are registered per `authorization` instead, and the assertion is on **which body came
/// back**, never on the status code.
///
/// # ORDERING IS THE EXPERIMENT
///
/// The marker must be gone and the memo cleared **before** B's request. Skip the fresh
/// `CacheManager` and the memo answers from A's claim, which exercises the **mismatch** path —
/// already covered by `a_second_origin_is_forwarded_and_never_served_the_first_origins_bytes`
/// — rather than the **migration** path under test here.
///
/// # The observability consequence
///
/// Neither bypass counter moves, because an absent marker is a no-op rather than a mismatch.
/// So this cross-serve is invisible to the metric an operator would watch for it.
#[tokio::test]
async fn the_claiming_origin_is_served_pre_existing_entries_it_did_not_write() {
    // Held for the whole body — see COUNTER_LOCK.
    let _serialised = COUNTER_LOCK.lock().await;

    // ── Vacuity guards, both directions ──────────────────────────────────────
    assert_ne!(
        origin_identity(ORIGIN_A),
        origin_identity(ORIGIN_B),
        "these two hosts must have DIFFERENT origin identities, or the second request is not \
         from a different origin at all and this test measures nothing"
    );
    assert_eq!(
        CacheManager::generate_cache_key(&object_path(), Some(ORIGIN_A)),
        CacheManager::generate_cache_key(&object_path(), Some(ORIGIN_B)),
        "the two origins must produce the SAME cache key, or the pre-existing entry is not \
         under the key the claiming origin looks up and there is no residual to measure"
    );

    let tmp = TempDir::new().unwrap();
    let config = test_config(tmp.path().to_path_buf());
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    // Per-origin bodies: without this, a HIT on A's entry and a MISS forwarded to B's own
    // origin are indistinguishable.
    let stub = Arc::new(
        StubS3Client::new()
            .with_response_for_authorization(AUTH_A, ok_body(BODY_A))
            .with_response_for_authorization(AUTH_B, ok_body(BODY_B)),
    );
    let server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&cache_manager),
        range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    let mismatch_before = origin_marker_mismatch_bypasses_total();
    let unparseable_before = origin_marker_unparseable_bypasses_total();

    // ── Origin A caches the object, pre-upgrade ──────────────────────────────
    let (status, _h, body) = proxy_get(server.addr, ORIGIN_A, &object_path(), AUTH_A).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), BODY_A, "origin A must receive its own bytes");
    let files_after_a = cache_files(tmp.path());
    assert!(
        !files_after_a.is_empty(),
        "fixture precondition: origin A's read must have written cache files, or there is no \
         pre-existing entry for origin B to be served"
    );

    // ── Erase every trace of the claim: the genuine pre-upgrade state ────────
    std::fs::remove_file(marker_file(tmp.path())).expect("remove the marker");
    assert!(
        !marker_file(tmp.path()).exists(),
        "fixture precondition: the pre-upgrade state is NO marker. With one present this \
         exercises the mismatch path, which is covered elsewhere"
    );
    // A fresh process over the same volume clears the in-process memo, so nothing remembers
    // A's claim.
    let (fresh_manager, fresh_range_handler) = make_cache_infra(&config).await;
    let fresh_server = spawn_proxy_server(
        Arc::clone(&config),
        Arc::clone(&fresh_manager),
        fresh_range_handler,
        Arc::clone(&stub) as Arc<dyn S3ClientApi + Send + Sync>,
    )
    .await;

    // ── Origin B is the FIRST request after the upgrade ──────────────────────
    let (status, headers, body) =
        proxy_get(fresh_server.addr, ORIGIN_B, &object_path(), AUTH_B).await;
    assert_eq!(status, StatusCode::OK);

    // THE MEASUREMENT. Both outcomes are meaningful: B receiving BODY_A is the residual the
    // documentation describes; B receiving BODY_B would mean the residual does not exist and
    // `docs/CACHING.md` is over-cautious.
    assert_eq!(
        body.as_ref(),
        BODY_A,
        "MEASURED WORLD CHANGED. This test asserts that origin B, claiming a bucket whose \
         entries origin A cached before any marker existed, IS served A's bytes — an accepted \
         residual, because a cache key is `{}` with no origin in it. Receiving B's own bytes \
         ({:?}) instead would mean the residual does not exist, in which case `docs/CACHING.md` \
         and task 2.15 both overstate the risk and this test should be inverted rather than \
         the code changed. Got {:?}",
        expected_cache_key(),
        String::from_utf8_lossy(BODY_B),
        String::from_utf8_lossy(body.as_ref()),
    );
    // An independent mechanism from the body: `x-cache` is the proxy's own serve decision.
    assert_eq!(
        x_cache(&headers),
        Some("HIT"),
        "origin B's request must be reported as a cache HIT — the serve decision, read \
         independently of the bytes"
    );

    // ── The cross-serve is invisible to both bypass counters ─────────────────
    assert_eq!(
        origin_marker_mismatch_bypasses_total(),
        mismatch_before,
        "an absent marker is a no-op, so no mismatch bypass is counted. This is the \
         observability consequence: the cross-serve does not appear in the metric an operator \
         would watch"
    );
    assert_eq!(
        origin_marker_unparseable_bypasses_total(),
        unparseable_before,
        "nor is an absent marker counted as unparseable"
    );

    // ── And B now owns the binding ───────────────────────────────────────────
    assert!(
        marker_file(tmp.path()).exists(),
        "the first request after the upgrade must claim the bucket"
    );
    let recorded: serde_json::Value =
        serde_json::from_slice(&std::fs::read(marker_file(tmp.path())).unwrap()).unwrap();
    assert_eq!(
        recorded["origin"].as_str(),
        origin_identity(ORIGIN_B).as_deref(),
        "the claiming origin is B, not the origin that actually wrote the entries — the second \
         half of the finding: B is served A's bytes AND B owns the binding from here on"
    );
    assert_eq!(recorded["schema"], 1);

    // A HIT writes nothing, so A's entry is undisturbed.
    assert_eq!(
        cache_files(tmp.path()),
        files_after_a,
        "being served from the pre-existing entry must not rewrite it"
    );
}
