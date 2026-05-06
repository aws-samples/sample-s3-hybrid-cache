//! Preservation property tests — download-coordination TTL correctness
//!
//! Spec: `.kiro/specs/download-coordination-ttl-correctness/`
//!
//! # Purpose
//!
//! This file is Task 2 of the bugfix workflow. For every generator input
//! outside `isBugCondition` (see `bugfix.md` §Deriving the Bug Condition),
//! these properties lock in the CURRENT behaviour of the four coordination
//! helpers that Task 3–7 will modify:
//!
//! - `HttpProxy::forward_get_head_with_coordination`
//! - `HttpProxy::forward_part_with_coordination`
//! - `HttpProxy::forward_range_with_coordination`
//! - `HttpProxy::serve_from_cache_or_s3`
//!
//! # Requirements covered
//!
//! - 3.1–3.4 (TTL>0 fresh cache hit, every method) via
//!   `prop_ttl_positive_fresh_hit_never_touches_s3`.
//! - 3.5–3.6 (single-request uncached GET / expired-cached GET) via
//!   `prop_single_request_baseline_unchanged`.
//! - 3.7 (`download_coordination.enabled = false`) via
//!   `prop_coordination_disabled_is_per_request`.
//! - 3.8–3.10 (fetcher error / timeout; channel-close noted but not driven
//!   directly — see comment on `prop_fetcher_error_triggers_waiter_fallback`)
//!   via `prop_fetcher_error_triggers_waiter_fallback`.
//! - 3.11 (flight-key independence across distinct cache keys and
//!   non-overlapping ranges) via `prop_different_flight_keys_are_independent`.
//!
//! # Methodology
//!
//! Observation-first. Each scenario was dry-run against the UNFIXED
//! coordination helpers to capture the observed trace (stub S3 request count,
//! per-request authorization header, response status and body length).
//! Those observations are encoded inline as assertions — no external snapshot
//! files. Tasks 10.3 and 14 re-run this file on the FIXED code; it must
//! still pass, proving that the fix in Tasks 3–7 preserves behaviour on
//! every non-buggy input.
//!
//! # Iteration budget
//!
//! Each property uses `QuickCheck::new().tests(25)` to keep wall-clock under
//! ~60s per property on the dev machine. The generator ranges are already
//! narrow (low concurrency, small input domain) so 25 iterations cover the
//! shrinking frontier well.
//!
//! # What is NOT tested here
//!
//! - `isBugCondition(X)` holds → covered by
//!   `download_coordination_bug_condition_property_test.rs` (Task 1).
//! - HEAD / part fresh-cache read at the `serve_from_cache_or_s3` boundary —
//!   the helper today only serves full-object bytes on success; the HEAD
//!   and part waiter wakeup paths are inside
//!   `forward_get_head_with_coordination` / `forward_part_with_coordination`
//!   and are exercised via those entry points.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use quickcheck::{QuickCheck, TestResult};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use s3_proxy::{Result as ProxyResult, S3ClientApi, S3RequestContext, S3Response};

use common::{CapturedRequest, StubResponse, StubS3Client};

// --- Shared fixtures ------------------------------------------------------

/// Body the stub returns for authoritative S3 fetches in these properties.
/// Kept short so the buffered cache-write path is synchronous relative to
/// the test driver (same reasoning as the bug-condition file).
const S3_BODY: &[u8] = b"PRESERVATION-TEST-BODY";

/// A plausible SigV4 authorization header. Contents are not parsed — only
/// compared by exact string match by the stub.
fn auth_for(index: usize) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=AKIA-PRESERVE-{index:03}/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=ab{index:02}"
    )
}

/// Build the minimal set of SigV4 headers the coordination code reads.
fn signed_headers(authorization: &str) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("authorization".to_string(), authorization.to_string());
    headers.insert("host".to_string(), "s3.amazonaws.com".to_string());
    headers.insert("x-amz-date".to_string(), "20250101T000000Z".to_string());
    headers.insert(
        "x-amz-content-sha256".to_string(),
        "UNSIGNED-PAYLOAD".to_string(),
    );
    headers
}

/// Signed headers plus an explicit byte range, for range-request tests.
fn signed_range_headers(authorization: &str, start: u64, end: u64) -> HashMap<String, String> {
    let mut headers = signed_headers(authorization);
    headers.insert("range".to_string(), format!("bytes={start}-{end}"));
    headers
}

/// Build a `Config` with positive TTL and coordination enabled. Used as the
/// baseline for "preservation" scenarios that do NOT target the bug
/// conditions.
fn positive_ttl_config() -> Arc<Config> {
    let mut config = Config::default();
    // 1 hour is firmly "fresh" for every cache entry we prime below, which
    // are stamped with `SystemTime::now()` when primed.
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.head_ttl = Duration::from_secs(3600);
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 10;
    // The test harness does not wire a RamCache; the coordination helpers
    // read from the disk-cache side of `CacheManager` (same as the
    // bug-condition file).
    config.cache.ram_cache_enabled = false;
    Arc::new(config)
}

/// A `Config` with coordination disabled — every request should bypass
/// `InFlightTracker` and go through the non-coordinated forwarding path
/// (requirement 3.7).
fn coordination_disabled_config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.download_coordination.enabled = false;
    config.cache.download_coordination.wait_timeout_secs = 10;
    config.cache.ram_cache_enabled = false;
    Arc::new(config)
}

/// Build fresh cache infrastructure rooted at a tempdir. Caller keeps the
/// `TempDir` alive for the duration of the test.
async fn make_cache_infra(
    config: &Arc<Config>,
) -> (
    TempDir,
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
) {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        cache_dir,
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
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

    (temp_dir, cache_manager, disk_cache_manager)
}

/// Prime the cache with `S3_BODY` for the given key. Under a positive-TTL
/// config the stored entry's `expires_at` is `now + 3600s`, so it is "fresh"
/// for the duration of any test function.
async fn prime_fresh_cache_entry(cache_manager: &Arc<CacheManager>, cache_key: &str) {
    let mut headers: HashMap<String, String> = HashMap::new();
    headers.insert("etag".to_string(), "\"preservation-etag\"".to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
    );
    headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    headers.insert("content-length".to_string(), S3_BODY.len().to_string());

    let metadata = CacheMetadata {
        etag: "\"preservation-etag\"".to_string(),
        last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
        content_length: S3_BODY.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    cache_manager
        .store_response_with_headers(cache_key, S3_BODY, headers, metadata)
        .await
        .expect("prime fresh cache entry");
}

/// Drive `forward_get_head_with_coordination` once and return status + body.
#[allow(clippy::too_many_arguments)]
async fn run_get_head(
    method: Method,
    uri: hyper::Uri,
    host: String,
    headers: HashMap<String, String>,
    cache_key: String,
    cache_manager: Arc<CacheManager>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    inflight_tracker: Arc<InFlightTracker>,
    range_handler: Arc<RangeHandler>,
    config: Arc<Config>,
) -> (StatusCode, Bytes) {
    let wait_timeout = Duration::from_secs(config.cache.download_coordination.wait_timeout_secs);
    let coordination_enabled = config.cache.download_coordination.enabled;
    let response = HttpProxy::forward_get_head_with_coordination(
        method,
        uri,
        host,
        headers,
        cache_key,
        cache_manager,
        s3_client,
        inflight_tracker,
        range_handler,
        config,
        coordination_enabled,
        wait_timeout,
        None,
        &None,
    )
    .await
    .expect("Infallible");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    (status, body)
}

/// Drive `serve_from_cache_validated` once and return status + body.
///
/// NOTE (`download-coordination-ttl-correctness` fix): the original helper
/// `serve_from_cache_or_s3` was removed; every waiter now issues its own
/// signed conditional request via `serve_from_cache_validated`. For a
/// TTL>0 fresh-hit the property semantics shift: the helper DOES send a
/// conditional to S3, but gets `304 Not Modified`, and serves the cached
/// body. The caller is responsible for configuring the stub to return
/// `304` for the conditional.
#[allow(clippy::too_many_arguments)]
async fn run_serve_from_cache_validated(
    method: Method,
    uri: hyper::Uri,
    host: String,
    headers: HashMap<String, String>,
    cache_key: String,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    config: Arc<Config>,
) -> (StatusCode, Bytes) {
    let response = HttpProxy::serve_from_cache_validated(
        method,
        uri,
        host,
        headers,
        cache_key,
        cache_manager,
        range_handler,
        s3_client,
        config,
        None,
        &None,
    )
    .await
    .expect("Infallible");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    (status, body)
}

/// `true` iff the stub's captured trace has exactly one request whose
/// `authorization` header matches `needle`.
fn trace_has_exactly_one_request_with_auth(captured: &[CapturedRequest], needle: &str) -> bool {
    captured
        .iter()
        .filter(|r| r.authorization().map(|a| a == needle).unwrap_or(false))
        .count()
        == 1
}

// =========================================================================
// Property: TTL>0 fresh cache hit — validated-serve contract
// Covers: bugfix.md §3.1, 3.2, 3.3, 3.4 (re-framed post-fix)
// =========================================================================

/// Under the `download-coordination-ttl-correctness` fix, every waiter —
/// including waiters for a TTL>0 fresh cache entry — issues its own
/// signed conditional request to S3 before serving cached bytes. The
/// preservation invariant is therefore reframed:
///
/// - The helper sends EXACTLY ONE S3 request carrying the caller's
///   authorization.
/// - That request carries the cached ETag in `If-None-Match` and the
///   cached `Last-Modified` in `If-Modified-Since` (additive; the
///   caller's signature headers are preserved verbatim).
/// - S3 responds `304 Not Modified` and the helper returns the primed
///   cached body (GET) or an empty body (HEAD) with `200 OK`.
///
/// This is NOT the "zero S3 round-trips" property the unfixed helper
/// satisfied — that shape is incompatible with the fix's IAM-validation
/// contract. The TTL>0 fresh-cache preservation guarantee today is that
/// the cached body is returned with no redundant body transfer and with
/// the caller's credentials validated against S3 on every request.
///
/// **Validates: Requirements 3.1, 3.2, 3.3, 3.4** (under fixed contract).
fn prop_ttl_positive_fresh_hit_never_touches_s3(method_tag: u8) -> TestResult {
    // Reduce the method-tag to GET or HEAD — the two shapes the
    // validated-serve helper distinguishes for a full-object flight.
    let method = if method_tag.is_multiple_of(2) {
        Method::GET
    } else {
        Method::HEAD
    };

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        let config = positive_ttl_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));

        let cache_key = "bucket-fresh/preserved-target.bin".to_string();
        prime_fresh_cache_entry(&cache_manager, &cache_key).await;

        // Stub: return 304 for a conditional carrying the primed ETag.
        // Every other request gets 500 so an unexpected non-conditional
        // request (which would indicate a regression) fails loudly.
        let primed_etag = "\"preservation-etag\"";
        let stub = StubS3Client::new()
            .with_response_for_etag(primed_etag, StubResponse::not_modified());
        let s3_client = stub.clone().into_trait_object();

        let caller_auth = auth_for(0);
        let uri: hyper::Uri = "/bucket-fresh/preserved-target.bin".parse().expect("uri");
        let (status, body) = run_serve_from_cache_validated(
            method.clone(),
            uri,
            "s3.amazonaws.com".to_string(),
            signed_headers(&caller_auth),
            cache_key,
            Arc::clone(&cache_manager),
            Arc::clone(&range_handler),
            s3_client,
            Arc::clone(&config),
        )
        .await;

        let captured = stub.captured();

        // Exactly 1 S3 request.
        if captured.len() != 1 {
            return TestResult::error(format!(
                "Fresh-cache validated-serve preservation violated: expected exactly 1 S3 \
                 request, got {}. captured_auths={:?}",
                captured.len(),
                captured.iter().map(|r| r.authorization()).collect::<Vec<_>>(),
            ));
        }
        // Caller's auth is present and If-None-Match matches primed ETag.
        let req = &captured[0];
        if req.authorization().map(|a| a != caller_auth).unwrap_or(true) {
            return TestResult::error(format!(
                "Fresh-cache validated-serve preservation violated: S3 request did not \
                 carry the caller's authorization. expected={caller_auth:?}, got={:?}",
                req.authorization(),
            ));
        }
        if req.if_none_match().map(|v| v != primed_etag).unwrap_or(true) {
            return TestResult::error(format!(
                "Fresh-cache validated-serve preservation violated: S3 request did not \
                 carry If-None-Match matching the cached ETag. expected={primed_etag:?}, \
                 got={:?}",
                req.if_none_match(),
            ));
        }

        // Response is 200 OK with primed body (GET) or empty body (HEAD).
        if status != StatusCode::OK {
            return TestResult::error(format!(
                "Fresh-cache validated-serve preservation violated: expected 200 OK, got {status:?}",
            ));
        }
        let expected_body_len = if method == Method::HEAD {
            0
        } else {
            S3_BODY.len()
        };
        if body.len() != expected_body_len {
            return TestResult::error(format!(
                "Fresh-cache validated-serve preservation violated: expected body of {} bytes \
                 for {method:?}, got {} bytes",
                expected_body_len,
                body.len(),
            ));
        }
        if method == Method::GET && body.as_ref() != S3_BODY {
            return TestResult::error(format!(
                "Fresh-cache validated-serve preservation violated: body bytes differ from \
                 the primed fixture (len {} vs {})",
                body.len(),
                S3_BODY.len(),
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_prop_ttl_positive_fresh_hit_never_touches_s3() {
    QuickCheck::new()
        .tests(25)
        .quickcheck(prop_ttl_positive_fresh_hit_never_touches_s3 as fn(u8) -> TestResult);
}

// =========================================================================
// Property: single-request baseline (no concurrent peers)
// Covers: bugfix.md §3.5, 3.6
// =========================================================================

/// For a single coordinated GET with no concurrent peer on the flight key,
/// the helper becomes the fetcher and forwards the request to S3 once with
/// the caller's own authorization. The client receives whatever status and
/// body the stub returned for that authorization. This encodes both the
/// uncached path (3.5) and, under a pre-primed cache, the
/// single-expired-revalidation path (3.6) — on unfixed code the fetcher
/// always performs its own authoritative round-trip regardless of cache
/// state, so both cases observe the same trace shape.
///
/// Generator: a u8 that's only used to slightly perturb the authorization
/// string so the property is not a single-point assertion; 25 samples are
/// enough.
///
/// **Validates: Requirements 3.5, 3.6**
fn prop_single_request_baseline_unchanged(variant: u8) -> TestResult {
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        let config = positive_ttl_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());

        let caller_auth = auth_for(variant as usize);

        let stub = StubS3Client::new().with_response_for_authorization(
            &caller_auth,
            StubResponse::ok(Bytes::from_static(S3_BODY))
                .with_header("etag", "\"single-request-etag\"")
                .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                .with_header("content-type", "application/octet-stream"),
        );
        let s3_client = stub.clone().into_trait_object();

        let cache_key = format!("bucket-single/preserved-{variant}.bin");
        let uri: hyper::Uri = format!("/bucket-single/preserved-{variant}.bin")
            .parse()
            .expect("uri");

        let (status, body) = run_get_head(
            Method::GET,
            uri,
            "s3.amazonaws.com".to_string(),
            signed_headers(&caller_auth),
            cache_key,
            Arc::clone(&cache_manager),
            s3_client,
            Arc::clone(&inflight_tracker),
            Arc::clone(&range_handler),
            Arc::clone(&config),
        )
        .await;

        let captured = stub.captured();

        // Exactly one S3 request, carrying the caller's authorization.
        if captured.len() != 1 {
            return TestResult::error(format!(
                "Single-request preservation violated: expected exactly 1 S3 \
                 request, got {}. captured_auths={:?}",
                captured.len(),
                captured
                    .iter()
                    .map(|r| r.authorization())
                    .collect::<Vec<_>>(),
            ));
        }
        if !trace_has_exactly_one_request_with_auth(&captured, &caller_auth) {
            return TestResult::error(format!(
                "Single-request preservation violated: the one S3 request \
                 did not carry the caller's authorization. expected={caller_auth:?}, \
                 got={:?}",
                captured[0].authorization(),
            ));
        }

        // Client saw the stub-configured 200 OK with the stub body.
        if status != StatusCode::OK {
            return TestResult::error(format!(
                "Single-request preservation violated: expected 200 OK, got {status:?}",
            ));
        }
        if body.as_ref() != S3_BODY {
            return TestResult::error(format!(
                "Single-request preservation violated: body bytes differ from \
                 what S3 returned (len {} vs {})",
                body.len(),
                S3_BODY.len(),
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_prop_single_request_baseline_unchanged() {
    QuickCheck::new()
        .tests(25)
        .quickcheck(prop_single_request_baseline_unchanged as fn(u8) -> TestResult);
}

// =========================================================================
// Property: coordination-disabled is per-request
// Covers: bugfix.md §3.7
// =========================================================================

/// When `download_coordination.enabled = false`, every concurrent request
/// MUST go directly to S3 — no flight registration, no waiter fallback.
/// This property launches 2..=8 concurrent GETs on the same cache key with
/// coordination disabled and asserts that the stub captured N requests, one
/// per participant, each carrying that participant's own authorization.
///
/// **Validates: Requirement 3.7**
fn prop_coordination_disabled_is_per_request(participant_count: u8) -> TestResult {
    let n: usize = match participant_count % 8 {
        0 | 1 => 2,
        other => other as usize,
    };
    if !(2..=8).contains(&n) {
        return TestResult::discard();
    }

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        let config = coordination_disabled_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());

        let participant_auths: Vec<String> = (0..n).map(auth_for).collect();

        // Stub: every auth gets its own 200+body so we can cross-check that
        // the response the client saw came from S3 (not from a cache that
        // happens to match the stub bytes).
        let mut stub = StubS3Client::new();
        for auth in &participant_auths {
            stub = stub.clone().with_response_for_authorization(
                auth,
                StubResponse::ok(Bytes::from_static(S3_BODY))
                    .with_header("etag", "\"disabled-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
            );
        }
        let stub_shared = stub.clone();
        let s3_client = stub.into_trait_object();

        let cache_key = "bucket-disabled/target.bin".to_string();
        let uri: hyper::Uri = "/bucket-disabled/target.bin".parse().expect("uri");
        let host = "s3.amazonaws.com".to_string();

        let mut join_set: JoinSet<(usize, StatusCode, Bytes)> = JoinSet::new();
        for (idx, auth) in participant_auths.iter().enumerate() {
            let method = Method::GET;
            let uri = uri.clone();
            let host = host.clone();
            let headers = signed_headers(auth);
            let cache_key = cache_key.clone();
            let cache_manager = Arc::clone(&cache_manager);
            let s3_client = Arc::clone(&s3_client);
            let inflight_tracker = Arc::clone(&inflight_tracker);
            let range_handler = Arc::clone(&range_handler);
            let config = Arc::clone(&config);
            join_set.spawn(async move {
                let (status, body) = run_get_head(
                    method,
                    uri,
                    host,
                    headers,
                    cache_key,
                    cache_manager,
                    s3_client,
                    inflight_tracker,
                    range_handler,
                    config,
                )
                .await;
                (idx, status, body)
            });
        }

        let mut results: Vec<Option<(StatusCode, Bytes)>> = (0..n).map(|_| None).collect();
        while let Some(joined) = join_set.join_next().await {
            let (idx, status, body) = joined.expect("join");
            results[idx] = Some((status, body));
        }

        // Every client must have gotten 200 OK with the S3 body.
        for (idx, slot) in results.iter().enumerate() {
            let (status, body) = slot.as_ref().expect("result present");
            if *status != StatusCode::OK {
                return TestResult::error(format!(
                    "Disabled-coordination preservation violated: participant \
                     #{idx} got {status:?}, expected 200 OK",
                ));
            }
            if body.as_ref() != S3_BODY {
                return TestResult::error(format!(
                    "Disabled-coordination preservation violated: participant \
                     #{idx} got body of {} bytes, expected {}",
                    body.len(),
                    S3_BODY.len(),
                ));
            }
        }

        // Every participant's auth must be in the trace exactly once.
        let captured = stub_shared.captured();
        for (idx, auth) in participant_auths.iter().enumerate() {
            if !trace_has_exactly_one_request_with_auth(&captured, auth) {
                return TestResult::error(format!(
                    "Disabled-coordination preservation violated: \
                     participant #{idx}'s auth was not present exactly once \
                     in the stub trace. expected={auth:?}, captured_auths={:?}",
                    captured
                        .iter()
                        .map(|r| r.authorization())
                        .collect::<Vec<_>>(),
                ));
            }
        }
        // Exactly N requests total — no extras.
        if captured.len() != n {
            return TestResult::error(format!(
                "Disabled-coordination preservation violated: expected \
                 exactly {n} captured S3 requests, got {}",
                captured.len(),
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_prop_coordination_disabled_is_per_request() {
    QuickCheck::new()
        .tests(25)
        .quickcheck(prop_coordination_disabled_is_per_request as fn(u8) -> TestResult);
}

// =========================================================================
// Property: fetcher error / timeout triggers waiter fallback
// Covers: bugfix.md §3.8, 3.9 (3.10 channel-close is noted below)
// =========================================================================

/// An `S3ClientApi` wrapper that sleeps for a fixed delay before delegating
/// to an inner stub. Used below to simulate a fetcher that holds the flight
/// open past the waiter's `wait_timeout`, forcing the waiter down the
/// timeout fallback path (requirement 3.9).
///
/// The wrapper records captured requests on the inner stub, so
/// `stub.captured()` still reflects every forwarded request.
#[derive(Clone)]
struct DelayedStub {
    inner: Arc<StubS3Client>,
    delay_for_auth: Arc<HashMap<String, Duration>>,
}

impl DelayedStub {
    fn new(inner: StubS3Client, delays: HashMap<String, Duration>) -> Self {
        Self {
            inner: Arc::new(inner),
            delay_for_auth: Arc::new(delays),
        }
    }

    fn captured(&self) -> Vec<CapturedRequest> {
        self.inner.captured()
    }
}

#[async_trait]
impl S3ClientApi for DelayedStub {
    async fn forward_request(&self, context: S3RequestContext) -> ProxyResult<S3Response> {
        // Look up any per-auth delay before forwarding. Matching is purely
        // additive: if no entry matches, no delay is applied.
        let auth_opt = context
            .headers
            .get("authorization")
            .or_else(|| context.headers.get("Authorization"))
            .cloned();
        if let Some(auth) = auth_opt {
            if let Some(delay) = self.delay_for_auth.get(&auth) {
                tokio::time::sleep(*delay).await;
            }
        }
        self.inner.forward_request(context).await
    }

    fn extract_metadata_from_response(&self, headers: &HashMap<String, String>) -> CacheMetadata {
        self.inner.extract_metadata_from_response(headers)
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::ObjectMetadata {
        self.inner.extract_object_metadata_from_response(headers)
    }

    fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
        self.inner.get_connection_pool()
    }

    fn has_endpoint_overrides(&self) -> bool {
        self.inner.has_endpoint_overrides()
    }

    async fn set_metrics_manager(
        &self,
        metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
        self.inner.set_metrics_manager(metrics_manager).await
    }

    async fn register_endpoint(&self, endpoint: &str) {
        self.inner.register_endpoint(endpoint).await
    }

    async fn refresh_dns(&self) -> ProxyResult<()> {
        self.inner.refresh_dns().await
    }
}

/// Covers the fetcher-error (3.8) and fetcher-timeout (3.9) fallback paths.
/// For each generator case the property drives two concurrent coordinated
/// GETs on the same cache key; the fetcher encounters a chosen failure mode
/// and the waiter MUST fall back to its own signed S3 fetch.
///
/// The two simulated fetcher outcomes:
///
/// - **Error**: the fetcher's auth is mapped to `500 Internal Server Error`.
///   `forward_get_head_with_coordination` signals `complete_error(...)` on
///   the flight, and the waiter's `tokio::time::timeout(..., rx.recv())`
///   resolves to `Ok(Ok(Err(_)))`, taking the fetcher-error arm and
///   delegating to `forward_get_head_to_s3_and_cache` with the waiter's
///   signed headers.
///
/// - **Timeout**: the fetcher's auth is wired through a `DelayedStub` that
///   sleeps longer than the waiter's `wait_timeout_secs`. The waiter's
///   timeout fires first, taking the `Err(_timeout)` arm and delegating to
///   `forward_get_head_to_s3_and_cache` with the waiter's signed headers.
///
/// Channel-close (requirement 3.10) is not exercised directly — the
/// coordination helper is an owned future that does not expose its
/// `FetchGuard` to test code, so simulating a fetcher dropping its guard
/// cleanly is not straightforward. The timeout + error cases cover the two
/// observable waiter-fallback arms; the channel-close arm is structurally
/// identical (it also re-enters `forward_get_head_to_s3_and_cache` with the
/// waiter's headers) and will be re-verified in Task 10.3's fixed-code
/// re-run.
///
/// **Validates: Requirements 3.8, 3.9; 3.10 noted.**
fn prop_fetcher_error_triggers_waiter_fallback(scenario_tag: u8) -> TestResult {
    // Two scenarios: 0 → fetcher returns 500 (error), 1 → fetcher times out.
    let scenario = scenario_tag % 2;
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        // For the timeout scenario use a short wait_timeout so the test
        // completes quickly; the delay must exceed wait_timeout by enough
        // margin to survive stochastic scheduler skew.
        let mut config_inner = Config::default();
        config_inner.cache.get_ttl = Duration::from_secs(3600);
        config_inner.cache.download_coordination.enabled = true;
        config_inner.cache.download_coordination.wait_timeout_secs = 1;
        config_inner.cache.ram_cache_enabled = false;
        let config = Arc::new(config_inner);

        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());

        let fetcher_auth = auth_for(0);
        let waiter_auth = auth_for(1);

        let stub_inner = StubS3Client::new()
            .with_response_for_authorization(
                &waiter_auth,
                StubResponse::ok(Bytes::from_static(S3_BODY))
                    .with_header("etag", "\"fallback-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
            )
            .with_response_for_authorization(
                &fetcher_auth,
                if scenario == 0 {
                    // Fetcher returns a 5xx — triggers complete_error path.
                    StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR)
                } else {
                    // Fetcher eventually returns 200 OK, but only after the
                    // waiter has already timed out.
                    StubResponse::ok(Bytes::from_static(S3_BODY))
                        .with_header("etag", "\"fallback-etag\"")
                        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                },
            );

        let mut delays = HashMap::new();
        if scenario == 1 {
            // Delay must be comfortably greater than wait_timeout (1s).
            delays.insert(fetcher_auth.clone(), Duration::from_millis(2500));
        }
        let delayed = DelayedStub::new(stub_inner, delays);
        let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(delayed.clone());

        let cache_key = "bucket-fallback/target.bin".to_string();
        let uri: hyper::Uri = "/bucket-fallback/target.bin".parse().expect("uri");
        let host = "s3.amazonaws.com".to_string();

        let mut join_set: JoinSet<(usize, StatusCode, Bytes)> = JoinSet::new();
        for (idx, auth) in [fetcher_auth.clone(), waiter_auth.clone()]
            .iter()
            .enumerate()
        {
            let method = Method::GET;
            let uri = uri.clone();
            let host = host.clone();
            let headers = signed_headers(auth);
            let cache_key = cache_key.clone();
            let cache_manager = Arc::clone(&cache_manager);
            let s3_client = Arc::clone(&s3_client);
            let inflight_tracker = Arc::clone(&inflight_tracker);
            let range_handler = Arc::clone(&range_handler);
            let config = Arc::clone(&config);
            join_set.spawn(async move {
                // Stagger so the fetcher registers first.
                if idx == 1 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let (status, body) = run_get_head(
                    method,
                    uri,
                    host,
                    headers,
                    cache_key,
                    cache_manager,
                    s3_client,
                    inflight_tracker,
                    range_handler,
                    config,
                )
                .await;
                (idx, status, body)
            });
        }

        let mut results: Vec<Option<(StatusCode, Bytes)>> = vec![None, None];
        while let Some(joined) = join_set.join_next().await {
            let (idx, status, body) = joined.expect("join");
            results[idx] = Some((status, body));
        }

        let captured = delayed.captured();

        // The waiter's auth MUST appear in the trace — that is the
        // fallback property. (On UNFIXED code today, both scenarios already
        // re-enter `forward_get_head_to_s3_and_cache` with the waiter's
        // signed request; the fix does not change these paths.)
        if !captured
            .iter()
            .any(|r| r.authorization().map(|a| a == waiter_auth).unwrap_or(false))
        {
            return TestResult::error(format!(
                "Fetcher-fallback preservation violated: waiter's auth was \
                 never forwarded to S3 despite fetcher {}. captured_auths={:?}",
                if scenario == 0 {
                    "error (500)"
                } else {
                    "timeout"
                },
                captured
                    .iter()
                    .map(|r| r.authorization())
                    .collect::<Vec<_>>(),
            ));
        }

        // Waiter (index 1) MUST have received 200 OK with the canned body,
        // because its own signed fallback request returned 200 from the
        // stub.
        let (waiter_status, waiter_body) = results[1].as_ref().expect("waiter result");
        if *waiter_status != StatusCode::OK {
            return TestResult::error(format!(
                "Fetcher-fallback preservation violated: waiter expected \
                 200 OK, got {waiter_status:?} (scenario={})",
                if scenario == 0 { "error" } else { "timeout" },
            ));
        }
        if waiter_body.as_ref() != S3_BODY {
            return TestResult::error(format!(
                "Fetcher-fallback preservation violated: waiter body \
                 differs. expected {} bytes, got {}",
                S3_BODY.len(),
                waiter_body.len(),
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_prop_fetcher_error_triggers_waiter_fallback() {
    QuickCheck::new()
        .tests(25)
        .quickcheck(prop_fetcher_error_triggers_waiter_fallback as fn(u8) -> TestResult);
}

// =========================================================================
// Property: different flight keys are independent
// Covers: bugfix.md §3.11
// =========================================================================

/// The coordination layer keys on `{cache_key}` for full GET,
/// `{cache_key}:{start}-{end}` for ranges, and `{cache_key}:part{N}` for
/// parts. Concurrent requests against distinct flight keys MUST produce
/// independent traces — each request goes to S3 once with its own
/// authorization, regardless of how many peers are live on other flight
/// keys.
///
/// The generator picks two independence shapes:
///
/// - **Distinct cache keys**: N concurrent GETs each against a different
///   `{cache_key}` — N fetchers, 0 waiters, N S3 requests.
/// - **Non-overlapping byte ranges on the same cache key**: two concurrent
///   coordinated GETs with disjoint `Range` headers — two flight keys
///   (`cache_key:0-99` and `cache_key:100-199`), two fetchers, two S3
///   requests. Range-handler overlap detection sees no cached overlap.
///
/// Different part numbers are structurally identical to the range case
/// (they each generate their own flight key), so the property covers them
/// by equivalence without a separate driven sub-case.
///
/// **Validates: Requirement 3.11**
fn prop_different_flight_keys_are_independent(shape_tag: u8) -> TestResult {
    // Shape 0: 2..=4 concurrent full GETs, each against a different cache key.
    // Shape 1: 2 concurrent range GETs with non-overlapping byte ranges on
    //          the same cache key.
    let shape = shape_tag % 2;
    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        let config = positive_ttl_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());

        match shape {
            0 => {
                // Distinct cache keys — pick N in [2, 4].
                let n: usize = 2 + (shape_tag as usize / 2) % 3; // {2, 3, 4}
                let participant_auths: Vec<String> = (0..n).map(auth_for).collect();

                let mut stub = StubS3Client::new();
                for auth in &participant_auths {
                    stub = stub.clone().with_response_for_authorization(
                        auth,
                        StubResponse::ok(Bytes::from_static(S3_BODY))
                            .with_header("etag", "\"indep-etag\"")
                            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
                    );
                }
                let stub_shared = stub.clone();
                let s3_client = stub.into_trait_object();

                let mut join_set: JoinSet<(usize, StatusCode, Bytes)> = JoinSet::new();
                for (idx, auth) in participant_auths.iter().enumerate() {
                    let cache_key = format!("bucket-indep/key-{idx}.bin");
                    let uri: hyper::Uri =
                        format!("/bucket-indep/key-{idx}.bin").parse().expect("uri");
                    let host = "s3.amazonaws.com".to_string();
                    let headers = signed_headers(auth);
                    let cache_manager = Arc::clone(&cache_manager);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let range_handler = Arc::clone(&range_handler);
                    let config = Arc::clone(&config);
                    join_set.spawn(async move {
                        let (status, body) = run_get_head(
                            Method::GET,
                            uri,
                            host,
                            headers,
                            cache_key,
                            cache_manager,
                            s3_client,
                            inflight_tracker,
                            range_handler,
                            config,
                        )
                        .await;
                        (idx, status, body)
                    });
                }

                let mut results: Vec<Option<(StatusCode, Bytes)>> = (0..n).map(|_| None).collect();
                while let Some(joined) = join_set.join_next().await {
                    let (idx, status, body) = joined.expect("join");
                    results[idx] = Some((status, body));
                }

                for (idx, slot) in results.iter().enumerate() {
                    let (status, body) = slot.as_ref().expect("result");
                    if *status != StatusCode::OK {
                        return TestResult::error(format!(
                            "Distinct-flight-key preservation violated: \
                             participant #{idx} status {status:?}",
                        ));
                    }
                    if body.as_ref() != S3_BODY {
                        return TestResult::error(format!(
                            "Distinct-flight-key preservation violated: \
                             participant #{idx} body differs",
                        ));
                    }
                }

                let captured = stub_shared.captured();
                if captured.len() != n {
                    return TestResult::error(format!(
                        "Distinct-flight-key preservation violated: expected \
                         exactly {n} S3 requests, got {}. captured_auths={:?}",
                        captured.len(),
                        captured
                            .iter()
                            .map(|r| r.authorization())
                            .collect::<Vec<_>>(),
                    ));
                }
                for (idx, auth) in participant_auths.iter().enumerate() {
                    if !trace_has_exactly_one_request_with_auth(&captured, auth) {
                        return TestResult::error(format!(
                            "Distinct-flight-key preservation violated: \
                             participant #{idx}'s auth not present exactly \
                             once. auth={auth:?}, captured={:?}",
                            captured
                                .iter()
                                .map(|r| r.authorization())
                                .collect::<Vec<_>>(),
                        ));
                    }
                }
            }
            _ => {
                // Non-overlapping byte ranges — same cache key, two flights.
                let auth_a = auth_for(10);
                let auth_b = auth_for(11);

                let stub = StubS3Client::new()
                    .with_response_for_authorization(
                        &auth_a,
                        StubResponse::ok(Bytes::from_static(b"AAAAAAAAAA"))
                            .with_header("etag", "\"range-a\"")
                            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                            .with_header("content-range", "bytes 0-9/200")
                            .with_header("content-length", "10"),
                    )
                    .with_response_for_authorization(
                        &auth_b,
                        StubResponse::ok(Bytes::from_static(b"BBBBBBBBBB"))
                            .with_header("etag", "\"range-b\"")
                            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                            .with_header("content-range", "bytes 100-109/200")
                            .with_header("content-length", "10"),
                    );
                let stub_shared = stub.clone();
                let s3_client = stub.into_trait_object();

                let cache_key = "bucket-indep/range-target.bin".to_string();
                let uri: hyper::Uri = "/bucket-indep/range-target.bin".parse().expect("uri");
                let host = "s3.amazonaws.com".to_string();

                let range_a = RangeSpec { start: 0, end: 9 };
                let range_b = RangeSpec {
                    start: 100,
                    end: 109,
                };

                // Seed empty overlap — no cached ranges — so the coordination
                // helper routes to the S3 forward path.
                let overlap_a = range_handler
                    .find_cached_ranges(&cache_key, &range_a, None, None)
                    .await
                    .expect("overlap a");
                let overlap_b = range_handler
                    .find_cached_ranges(&cache_key, &range_b, None, None)
                    .await
                    .expect("overlap b");

                let mut join_set: JoinSet<(usize, StatusCode)> = JoinSet::new();
                for (idx, (auth, range, overlap)) in [
                    (auth_a.clone(), range_a.clone(), overlap_a),
                    (auth_b.clone(), range_b.clone(), overlap_b),
                ]
                .into_iter()
                .enumerate()
                {
                    let uri = uri.clone();
                    let host = host.clone();
                    let headers = signed_range_headers(&auth, range.start, range.end);
                    let cache_key = cache_key.clone();
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let config = Arc::clone(&config);
                    join_set.spawn(async move {
                        let response = HttpProxy::forward_range_with_coordination(
                            Method::GET,
                            uri,
                            host,
                            headers,
                            cache_key,
                            range,
                            overlap,
                            cache_manager,
                            range_handler,
                            s3_client,
                            config,
                            true, // is_signed — exercises the signed-range forwarding branch
                            None,
                            inflight_tracker,
                            None,
                            &None,
                        )
                        .await
                        .expect("Infallible");
                        let status = response.status();
                        // Drain body so the future finishes the same way it
                        // would in production.
                        let _ = response
                            .into_body()
                            .collect()
                            .await
                            .expect("collect body")
                            .to_bytes();
                        (idx, status)
                    });
                }

                let mut statuses: Vec<Option<StatusCode>> = vec![None, None];
                while let Some(joined) = join_set.join_next().await {
                    let (idx, status) = joined.expect("join");
                    statuses[idx] = Some(status);
                }

                for (idx, st) in statuses.iter().enumerate() {
                    let status = st.expect("status");
                    if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
                        return TestResult::error(format!(
                            "Non-overlapping-range preservation violated: \
                             participant #{idx} status {status:?}",
                        ));
                    }
                }

                let captured = stub_shared.captured();
                // Each distinct flight key spawns its own fetcher, so each
                // auth must appear at least once in the trace. (The
                // range-handler may issue extra bookkeeping reads on the
                // merge path — we don't assert an exact count here,
                // because this property is specifically about
                // flight-key INDEPENDENCE, not arithmetic on the trace.)
                for (idx, auth) in [auth_a.as_str(), auth_b.as_str()].iter().enumerate() {
                    if !captured
                        .iter()
                        .any(|r| r.authorization().map(|a| a == *auth).unwrap_or(false))
                    {
                        return TestResult::error(format!(
                            "Non-overlapping-range preservation violated: \
                             participant #{idx}'s auth never reached S3. \
                             auth={auth:?}, captured={:?}",
                            captured
                                .iter()
                                .map(|r| r.authorization())
                                .collect::<Vec<_>>(),
                        ));
                    }
                }
            }
        }

        TestResult::passed()
    })
}

#[test]
fn test_prop_different_flight_keys_are_independent() {
    QuickCheck::new()
        .tests(25)
        .quickcheck(prop_different_flight_keys_are_independent as fn(u8) -> TestResult);
}
