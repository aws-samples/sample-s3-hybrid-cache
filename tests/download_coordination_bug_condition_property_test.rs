//! Bug condition exploration — download-coordination TTL correctness
//!
//! Spec: `.kiro/specs/download-coordination-ttl-correctness/`
//!
//! This test is Task 1 of the bugfix workflow. It exercises the two
//! interlocking defects described in `bugfix.md` and `design.md`:
//!
//! **Bug 1 — IAM bypass under `get_ttl = 0` coalescing** (Case A of
//! `isBugCondition`). When N concurrent signed GET requests arrive for the
//! same uncached object, the current `forward_get_head_with_coordination`
//! elects one fetcher to go to S3, and on the waiter-wakeup path routes
//! every other request through `serve_from_cache_or_s3`. The waiters never
//! send their signed request to S3 — they receive cached bytes regardless of
//! whether their own credentials would be authorised.
//!
//! **Bug 2 — no stampede protection for cached-but-expired objects**
//! (Case B of `isBugCondition`). When N concurrent requests hit an object
//! whose on-disk cache entry has `expires_at` in the past, the coalescer
//! continues to serve waiters from the stale cache without any per-waiter
//! conditional GET, so only the fetcher pays the S3 cost. On the full-object
//! GET handler this is amplified by an inline expired-revalidation branch
//! that runs *outside* `InFlightTracker`, but at the coordination-helper
//! level the symptom is the same: waiters issue zero S3 requests.
//!
//! Both symptoms reduce to: **the stub S3 trace contains no request carrying
//! the waiter's authorization header**. That is the single assertion this
//! file pivots on.
//!
//! # Harness
//!
//! The test uses Task 0's `StubS3Client` (a pure in-process `S3ClientApi`
//! implementation) wired into `forward_get_head_with_coordination` via the
//! `Arc<dyn S3ClientApi + Send + Sync>` seam. No TLS, no sockets, no real
//! S3. Each concurrent request is launched on its own `tokio::spawn` task so
//! the coalescer sees concurrent registrations on the same flight key.
//!
//! # Re-use in Task 13 (fix-checking)
//!
//! Task 13 of `tasks.md` re-runs this exact file against the fixed code.
//! The test body does not change — only the expected outcome flips:
//!
//! - **Task 1 (unfixed code, now)**: test FAILS with a counter-example
//!   showing a waiter received cached bytes while the stub saw no request
//!   with that waiter's `authorization` header.
//! - **Task 13 (fixed code, later)**: test PASSES because every waiter
//!   issues its own signed conditional GET before any cached bytes are
//!   returned.
//!
//! # COUNTEREXAMPLES DOCUMENTED
//!
//! When this test file was first run against the unfixed code (Task 1 of
//! the bugfix workflow), both properties failed with minimal,
//! deterministic counterexamples. These are the shape of the bug
//! conditions that the fix (Tasks 3–7) must eliminate.
//!
//! ## Case A — IAM bypass under `get_ttl = 0` coalescing
//!
//! ```text
//! [quickcheck] TEST FAILED (runtime error). Arguments: (0)
//! Error: Bug 1 triggered: waiter #0 received 200 OK with the fetcher's
//!   cached body while the stub's captured trace contains ZERO S3 requests
//!   with this waiter's auth.
//!     waiter_auth="AWS4-HMAC-SHA256 Credential=AKIA-WAITER-000/…"
//!     captured_auths=[Some("AWS4-HMAC-SHA256 Credential=AKIA-FETCHER/…")]
//!     waiter_status=200, waiter_body_len=23
//! ```
//!
//! With two concurrent requests (1 fetcher + 1 waiter), the fetcher reaches
//! S3 with its own SigV4 signature and receives `200 OK` with the canned
//! body (23 bytes, equal to `FETCHER_BODY`). The waiter is parked on the
//! `InFlightTracker` broadcast channel, wakes when the fetcher completes,
//! and is routed through `serve_from_cache_or_s3` — which returns the
//! fetcher-cached body directly. The stub trace shows exactly one captured
//! request, carrying the fetcher's `authorization` header, and none with
//! the waiter's. Under `get_ttl = 0` this is the IAM-bypass defect.
//!
//! ## Case B — expired-cache stampede
//!
//! ```text
//! [quickcheck] TEST FAILED (runtime error). Arguments: (0)
//! Error: Bug 2 triggered: expected 1 conditional round-trip(s) (one per
//!   non-authoritative participant) but saw 0. authoritative=1, N=2. On
//!   unfixed code waiters do not issue their own conditional GET — they
//!   read from cache silently.
//! ```
//!
//! With two concurrent requests and the cache pre-populated with an entry
//! whose `expires_at` is in the past (forced by `get_ttl = 0`), the fetcher
//! produces exactly one authoritative S3 round-trip (no conditional headers),
//! and the waiter produces zero round-trips. The fix must lift this to
//! `authoritative ≤ 1` AND `conditional = N − authoritative` — i.e. every
//! participant beyond the fetcher has to issue its own conditional GET so
//! S3 validates its credentials.
//!

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use tempfile::TempDir;
use tokio::task::JoinSet;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{CapturedRequest, StubResponse, StubS3Client};

// --- Test fixtures --------------------------------------------------------

/// The body the fetcher's S3 call will return. Intentionally small so the
/// stub response body goes through the buffered-cache path (synchronous
/// write) — this guarantees that by the time the fetcher's
/// `forward_request` future resolves, the cache entry is observable to
/// waiters.
const FETCHER_BODY: &[u8] = b"OK-CONTENT-FROM-FETCHER";

/// The authorization value we use for the fetcher. Any plausible SigV4
/// string is fine — the stub routes on exact match.
const FETCHER_AUTHORIZATION: &str =
    "AWS4-HMAC-SHA256 Credential=AKIA-FETCHER/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef";

/// Return an `Arc<Config>` with `get_ttl = 0`, `download_coordination.enabled
/// = true`, and a realistic wait timeout. Everything else is at its default.
fn zero_ttl_config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.get_ttl = Duration::from_secs(0);
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 10;
    // Disable the RAM cache — the bug condition lives on the disk-cache
    // waiter-serve path and the test harness doesn't wire a RamCache.
    config.cache.ram_cache_enabled = false;
    Arc::new(config)
}

/// Build a fresh `CacheManager` rooted at a temp directory, together with
/// the `DiskCacheManager` the `RangeHandler` needs to read/write ranges.
/// The caller keeps the `TempDir` alive for the duration of the test so
/// files don't get cleaned up mid-run.
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
        cache_dir.clone(),
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

    // Build the journal-backed DiskCacheManager *before* initializing the
    // cache manager — `CacheManager::initialize` fails if the journal
    // consolidator hasn't been wired up yet (see CacheError:
    // "JournalConsolidator not initialized").
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

/// Build the minimal set of headers a legitimate SigV4-signed S3 GET carries.
/// Only the fields that the stub matching key (`authorization`) and the
/// production coordination code actually read are set.
fn signed_get_headers(authorization: &str) -> HashMap<String, String> {
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

/// Pre-populate the cache with a synthetic object so Case B starts from the
/// "cache present" state. Under `get_ttl = 0` this entry is expired the
/// moment it's written — i.e. `expires_at` is in the past.
async fn prime_cache_with_expired_entry(cache_manager: &Arc<CacheManager>, cache_key: &str) {
    let headers: HashMap<String, String> = [
        ("etag".to_string(), "\"cached-etag\"".to_string()),
        (
            "last-modified".to_string(),
            "Wed, 01 Jan 2020 00:00:00 GMT".to_string(),
        ),
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("content-length".to_string(), FETCHER_BODY.len().to_string()),
    ]
    .into_iter()
    .collect();

    let metadata = CacheMetadata {
        etag: "\"cached-etag\"".to_string(),
        last_modified: "Wed, 01 Jan 2020 00:00:00 GMT".to_string(),
        content_length: FETCHER_BODY.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    cache_manager
        .store_response_with_headers(cache_key, FETCHER_BODY, headers, metadata)
        .await
        .expect("prime cache");
}

/// Run `forward_get_head_with_coordination` for a single signed GET. Returns
/// the response status and the fully-collected body bytes.
#[allow(clippy::too_many_arguments)]
async fn run_one_coordinated_get(
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
    wait_timeout: Duration,
) -> (StatusCode, Bytes) {
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
        true, // coordination_enabled
        wait_timeout,
        None,  // metrics_manager: not needed for this property
        &None, // proxy_referer
    )
    .await
    .expect("Infallible — coordination helper never returns Err");

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    (status, body)
}

/// Launch `participants.len()` concurrent coordinated GETs. Returns every
/// participant's (status, body) pair in the same order as the input slice.
#[allow(clippy::too_many_arguments)]
async fn launch_concurrent_gets(
    participants: Vec<HashMap<String, String>>,
    uri: hyper::Uri,
    host: String,
    cache_key: String,
    cache_manager: Arc<CacheManager>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    inflight_tracker: Arc<InFlightTracker>,
    range_handler: Arc<RangeHandler>,
    config: Arc<Config>,
) -> Vec<(StatusCode, Bytes)> {
    let wait_timeout = Duration::from_secs(config.cache.download_coordination.wait_timeout_secs);

    let mut join_set: JoinSet<(usize, StatusCode, Bytes)> = JoinSet::new();
    for (idx, headers) in participants.into_iter().enumerate() {
        let method = Method::GET;
        let uri = uri.clone();
        let host = host.clone();
        let cache_key = cache_key.clone();
        let cache_manager = Arc::clone(&cache_manager);
        let s3_client = Arc::clone(&s3_client);
        let inflight_tracker = Arc::clone(&inflight_tracker);
        let range_handler = Arc::clone(&range_handler);
        let config = Arc::clone(&config);

        join_set.spawn(async move {
            // Add a tiny deterministic stagger so the fetcher-election race
            // is reproducible across runs — index 0 enters first, the rest
            // enter after a microsecond so they land as waiters on the same
            // flight key.
            if idx != 0 {
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
            let (status, body) = run_one_coordinated_get(
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
                wait_timeout,
            )
            .await;
            (idx, status, body)
        });
    }

    let mut results: Vec<Option<(StatusCode, Bytes)>> = (0..join_set.len()).map(|_| None).collect();
    while let Some(joined) = join_set.join_next().await {
        let (idx, status, body) = joined.expect("task join");
        results[idx] = Some((status, body));
    }
    results
        .into_iter()
        .map(|slot| slot.expect("every task reported a result"))
        .collect()
}

/// `true` if the stub's captured trace contains a request whose
/// `authorization` header exactly matches `needle`.
fn trace_contains_auth(captured: &[CapturedRequest], needle: &str) -> bool {
    captured
        .iter()
        .any(|r| r.authorization().map(|a| a == needle).unwrap_or(false))
}

/// Counts an S3 request as "conditional" when it carries either
/// `If-None-Match` or `If-Modified-Since`. Anything else is "authoritative".
fn classify_request(r: &CapturedRequest) -> RequestClass {
    if r.if_none_match().is_some() || r.if_modified_since().is_some() {
        RequestClass::Conditional
    } else {
        RequestClass::Authoritative
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestClass {
    Authoritative,
    Conditional,
}

// --- Property 1 Case A: IAM bypass under TTL=0 coalescing ----------------

/// For any `N ∈ 2..=8` concurrent coordinated GETs on a cold cache where
/// the fetcher has a valid signature and every waiter has an invalid one,
/// each waiter MUST either (a) receive the error S3 returned for its own
/// credentials, or (b) have its signed request captured by the stub. The
/// forbidden outcome is: waiter got 200 OK carrying the fetcher-cached
/// body while the stub trace contains zero requests with the waiter's
/// authorization.
///
/// Validates: Requirements 2.1, 2.5 of `bugfix.md` (Bug 1).
#[quickcheck]
fn prop_case_a_iam_bypass_under_ttl_zero(waiter_count: u8) -> TestResult {
    // Scope the PBT to the bug-condition domain: 2..=8 concurrent requests.
    // Below 2 there is no coalescing; above 8 the scoped PBT adds time
    // without adding coverage.
    let n_waiters: usize = match waiter_count % 7 {
        0 => 1,
        other => other as usize,
    };
    let total: usize = n_waiters + 1; // +1 fetcher
    if !(2..=8).contains(&total) {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async move {
        let config = zero_ttl_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());

        // Build distinct, deterministic waiter credentials so every task's
        // trace lookup is unambiguous.
        let waiter_auths: Vec<String> = (0..n_waiters)
            .map(|i| {
                format!(
                    "AWS4-HMAC-SHA256 Credential=AKIA-WAITER-{i:03}/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=00"
                )
            })
            .collect();

        // Stub: fetcher gets 200+body, everybody else (default) gets 403.
        let stub = StubS3Client::new()
            .with_response_for_authorization(
                FETCHER_AUTHORIZATION,
                StubResponse::ok(Bytes::from_static(FETCHER_BODY))
                    .with_header("etag", "\"fetcher-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .with_header("content-type", "application/octet-stream"),
            )
            .with_default(StubResponse::forbidden());
        let s3_client = stub.clone().into_trait_object();

        let cache_key = "bucket-case-a/coalescing-target.bin".to_string();
        let uri: hyper::Uri = "/bucket-case-a/coalescing-target.bin"
            .parse()
            .expect("uri");
        let host = "s3.amazonaws.com".to_string();

        let mut participants = Vec::with_capacity(total);
        participants.push(signed_get_headers(FETCHER_AUTHORIZATION));
        for auth in &waiter_auths {
            participants.push(signed_get_headers(auth));
        }

        let results = launch_concurrent_gets(
            participants,
            uri,
            host,
            cache_key,
            Arc::clone(&cache_manager),
            s3_client,
            Arc::clone(&inflight_tracker),
            Arc::clone(&range_handler),
            Arc::clone(&config),
        )
        .await;

        let captured = stub.captured();

        // The fetcher is index 0: it MUST have reached S3 with its own auth.
        if !trace_contains_auth(&captured, FETCHER_AUTHORIZATION) {
            return TestResult::error(format!(
                "fetcher did not reach S3 (captured={} total, auths={:?})",
                captured.len(),
                captured.iter().map(|r| r.authorization()).collect::<Vec<_>>(),
            ));
        }

        // For every waiter: either its auth is in the trace, OR its response
        // is NOT `200 OK` with the fetcher body. The IAM-bypass bug produces
        // the forbidden outcome: `200 OK` + fetcher body + waiter auth absent
        // from the trace.
        for (i, waiter_auth) in waiter_auths.iter().enumerate() {
            let (status, body) = &results[i + 1];
            let saw_in_trace = trace_contains_auth(&captured, waiter_auth);
            let served_from_fetcher_cache = *status == StatusCode::OK
                && body.as_ref() == FETCHER_BODY;

            if !saw_in_trace && served_from_fetcher_cache {
                return TestResult::error(format!(
                    "Bug 1 triggered: waiter #{i} received 200 OK with the \
                     fetcher's cached body while the stub's captured trace \
                     contains ZERO S3 requests with this waiter's auth. \
                     waiter_auth={waiter_auth:?}. captured_auths={:?}. \
                     waiter_status={status:?}, waiter_body_len={}",
                    captured.iter().map(|r| r.authorization()).collect::<Vec<_>>(),
                    body.len(),
                ));
            }
        }

        TestResult::passed()
    })
}

// --- Property 1 Case B: expired-cache stampede ---------------------------

/// For any `N ∈ 2..=8` concurrent coordinated GETs where the on-disk cache
/// entry is already present with `expires_at` in the past, the fixed code
/// should produce at most one authoritative round-trip and the remaining
/// `N - count(authoritative)` round-trips should be conditional. On the
/// unfixed code this fails two ways: (a) waiters produce zero S3 round-trips
/// at all because `serve_from_cache_or_s3` returns cached bytes directly,
/// and (b) at the higher-level full-object GET handler, the expired branch
/// runs outside `InFlightTracker` and fires N independent conditionals. This
/// test observes (a) — the scoped-PBT counterpart that `forward_get_head_
/// with_coordination` can be driven against directly.
///
/// Validates: Requirements 2.8 of `bugfix.md` (Bug 2).
#[quickcheck]
fn prop_case_b_expired_cache_stampede(participant_count: u8) -> TestResult {
    let n: usize = match participant_count % 9 {
        0 | 1 => 2,
        other => other as usize,
    };
    if !(2..=8).contains(&n) {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async move {
        let config = zero_ttl_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());

        let cache_key = "bucket-case-b/expired-cache-target.bin".to_string();
        // Prime the cache BEFORE the stampede so every participant sees
        // `cache_state_at_entry = Expired`. With `get_ttl = 0` the entry is
        // written with `expires_at` in the past — the bug condition.
        prime_cache_with_expired_entry(&cache_manager, &cache_key).await;

        // All participants carry valid credentials — this property is
        // about coalescing arithmetic, not IAM. The stub returns 200 OK
        // with the body so that on the fixed-code path a waiter's
        // conditional GET that races past the fetcher's revalidation will
        // still get bytes.
        let stub = StubS3Client::new().with_default(
            StubResponse::ok(Bytes::from_static(FETCHER_BODY))
                .with_header("etag", "\"fetcher-etag\"")
                .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
        );
        let s3_client = stub.clone().into_trait_object();

        let uri: hyper::Uri = "/bucket-case-b/expired-cache-target.bin"
            .parse()
            .expect("uri");
        let host = "s3.amazonaws.com".to_string();

        // Distinct auth values so we can count requests by participant.
        let participant_auths: Vec<String> = (0..n)
            .map(|i| {
                format!(
                    "AWS4-HMAC-SHA256 Credential=AKIA-PART-{i:03}/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=01"
                )
            })
            .collect();
        let participants: Vec<HashMap<String, String>> = participant_auths
            .iter()
            .map(|a| signed_get_headers(a))
            .collect();

        let results = launch_concurrent_gets(
            participants,
            uri,
            host,
            cache_key,
            Arc::clone(&cache_manager),
            s3_client,
            Arc::clone(&inflight_tracker),
            Arc::clone(&range_handler),
            Arc::clone(&config),
        )
        .await;

        // Every client should have received a well-formed response (sanity).
        for (i, (status, _body)) in results.iter().enumerate() {
            if !status.is_success() && *status != StatusCode::NOT_MODIFIED {
                return TestResult::error(format!(
                    "Case B: participant #{i} received unexpected status \
                     {status:?}",
                ));
            }
        }

        let captured = stub.captured();
        let authoritative = captured
            .iter()
            .filter(|r| classify_request(r) == RequestClass::Authoritative)
            .count();
        let conditional = captured
            .iter()
            .filter(|r| classify_request(r) == RequestClass::Conditional)
            .count();

        // Fix invariant (Property 2): one authoritative + N-1 conditionals,
        // every participant's auth is in the trace.
        if authoritative > 1 {
            return TestResult::error(format!(
                "Case B: expected ≤1 authoritative S3 round-trip, \
                 got {authoritative}. captured={:?}",
                captured
                    .iter()
                    .map(|r| (r.authorization(), classify_request(r)))
                    .collect::<Vec<_>>(),
            ));
        }
        let expected_conditional = n - authoritative;
        if conditional != expected_conditional {
            return TestResult::error(format!(
                "Bug 2 triggered: expected {expected_conditional} conditional \
                 round-trip(s) (one per non-authoritative participant) but \
                 saw {conditional}. authoritative={authoritative}, N={n}. \
                 On unfixed code waiters do not issue their own conditional \
                 GET — they read from cache silently.",
            ));
        }
        for (i, auth) in participant_auths.iter().enumerate() {
            if !trace_contains_auth(&captured, auth) {
                return TestResult::error(format!(
                    "Bug 1/2 triggered in Case B: participant #{i} was \
                     served from cache without its signed request reaching \
                     S3. auth={auth:?}. captured_auths={:?}",
                    captured.iter().map(|r| r.authorization()).collect::<Vec<_>>(),
                ));
            }
        }

        TestResult::passed()
    })
}
