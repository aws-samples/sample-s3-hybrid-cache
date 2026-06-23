//! Integration tests for CRT conditional ranged-GET caching (Requirement 1).
//!
//! Spec: `.kiro/specs/crt-conditional-range-caching/`
//! Task: 3
//!
//! ## What these tests lock down
//!
//! The corrected Req 1 model dispatches a conditional request by header class:
//!   * **Mode A (default)** forwards every conditional to S3 (headers intact, S3
//!     evaluates the precondition) and caches the `200`/`206` success; `304`/`412`
//!     pass through uncached. No conditional is ever served from a cache hit.
//!   * **Mode B** serves an `If-Match` request from cache when the cached ETag
//!     matches; `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since` are
//!     forwarded like Mode A.
//!
//! These tests target the `forward_range_with_coordination` seam — the
//! **forward-and-cache leg** the model takes for Mode A (any conditional) and for
//! Mode B (`If-Match`-miss or the non-`If-Match` conditionals) — against the
//! in-process `StubS3Client` harness (no real S3, no credentials/network). They
//! lock the contract that leg relies on:
//!
//!   * a signed conditional range request is forwarded to S3 with its headers
//!     (notably `If-Match`, `Range`, and the SigV4 authorization) **intact** so S3
//!     evaluates the precondition (Req 1.5), and a success (`206`) is surfaced to
//!     the client; and
//!   * a signed conditional range request whose S3 response is `412 Precondition
//!     Failed` is forwarded to the client **without caching anything** (Req 1.2).
//!
//! ## Scope note: what is fleet-verified, not asserted here
//!
//! Two things are intentionally covered by the fleet `deployment-verification`
//! suite instead of in-process:
//!   1. **Streaming range-cache population.** `StubS3Client` returns only a
//!      `Buffered` body; the `Streaming` variant wraps `hyper::body::Incoming`,
//!      which has no public constructor. The real CRT path streams, and a buffered
//!      `206` partial is — correctly — not stored as a full object, so no
//!      in-process body shape caches a ranged response. T30 asserts a single CRT
//!      pass populates the cache (`.meta` ranges + `.bin` + byte-exact re-read).
//!   2. **The Mode B `If-Match` cache-serve decision.** That branch lives in the
//!      private `handle_get_head_request`, which needs a `Request<Incoming>` that
//!      cannot be constructed in-process. The fleet suite's Mode B `If-Match`
//!      assertion verifies a warm `If-Match` is served from cache with no S3 trip;
//!      Mode A's forward verdicts are guarded by T4–T8 (wrong `If-Match` → `412`,
//!      `If-None-Match` → `304`).
//!
//! What *is* deterministically checkable in-process — and the easy-to-regress part
//! — is header/signature preservation on the forward leg and the success/error
//! cache gate, which is what these three tests assert:
//!
//! * `conditional_range_206_forwards_headers_and_surfaces_206` — cold-cache
//!   populate: `206` surfaced, `If-Match`/`Range`/auth forwarded intact.
//! * `conditional_range_304_does_not_cache` — T8 regression guard: an
//!   `If-None-Match` that S3 answers `304` is relayed with no cache write.
//! * `conditional_range_412_does_not_cache` — T6 regression guard: a wrong
//!   `If-Match` that S3 answers `412` is relayed with no cache write.
//!
//! **Validates: Requirements 1.1, 1.2, 1.3, 1.5, 1.13 (forward-and-cache leg)**

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::{RangeHandler, RangeOverlap, RangeSpec};

use common::{StubResponse, StubS3Client};

// =========================================================================
// Fixtures
// =========================================================================

const RANGE_LEN: usize = 100;
const TEST_ETAG: &str = "\"crt-cond-etag-abc123\"";

/// A signed CRT-style conditional ranged GET: a `range` header plus the
/// `If-Match` precondition that the AWS CLI CRT client stamps on every part.
fn signed_conditional_range_headers(start: u64, end: u64) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIA-CRT/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;range;x-amz-date, Signature=crt01"
            .to_string(),
    );
    headers.insert("host".to_string(), "s3.amazonaws.com".to_string());
    headers.insert("x-amz-date".to_string(), "20250101T000000Z".to_string());
    headers.insert(
        "x-amz-content-sha256".to_string(),
        "UNSIGNED-PAYLOAD".to_string(),
    );
    headers.insert("range".to_string(), format!("bytes={}-{}", start, end));
    // The conditional precondition that triggered the original bug.
    headers.insert("if-match".to_string(), TEST_ETAG.to_string());
    headers
}

/// Coordination-enabled, zero-TTL config (forces every lookup to revalidate so
/// the cache write is the only thing under test).
fn zero_ttl_config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.get_ttl = Duration::from_secs(0);
    config.cache.head_ttl = Duration::from_secs(0);
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 10;
    config.cache.ram_cache_enabled = false;
    Arc::new(config)
}

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
        Duration::from_secs(10),
        64,
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

/// Recursively count cached object files (`.meta` metadata or `.bin` range
/// files) under the cache directory. This is storage-layout-agnostic: it counts
/// both full-object metadata and range files, so it works whether the cache
/// write took the buffered full-object path or the streaming range path.
/// Coordination/size-tracking bookkeeping files (`.json`, `.lock`, `.journal`)
/// are deliberately excluded.
fn count_cache_object_files(dir: &Path) -> usize {
    let mut count = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count += count_cache_object_files(&path);
        } else if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            if ext == "meta" || ext == "bin" {
                count += 1;
            }
        }
    }
    count
}

/// The synthetic all-missing overlap that the Req 1 forced-miss change passes
/// for a `conditional_forward` range request: nothing cached, the full range
/// missing, cannot serve from cache.
fn all_missing_overlap(range_spec: &RangeSpec) -> RangeOverlap {
    RangeOverlap {
        missing_ranges: vec![range_spec.clone()],
        cached_ranges: Vec::new(),
        can_serve_from_cache: false,
    }
}

// =========================================================================
// Req 1.1, 1.3, 1.5: a conditional ranged GET is forwarded with headers intact
// and its 206 surfaced (the forced-miss fall-through preserves the signature).
// Cache *population* on the streaming path is fleet-verified (see module note).
// =========================================================================

#[tokio::test]
async fn conditional_range_206_forwards_headers_and_surfaces_206() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let cache_key = "bucket/crt-conditional-206.bin".to_string();
    let range_spec = RangeSpec {
        start: 0,
        end: (RANGE_LEN as u64) - 1,
    };

    // S3 answers the conditional ranged GET with a fresh 206 (If-Match matched).
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![7u8; RANGE_LEN]))
            .with_header(
                "content-range",
                format!("bytes 0-{}/{}", RANGE_LEN - 1, RANGE_LEN),
            )
            .with_header("etag", TEST_ETAG),
    );
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        format!("/{}", cache_key).parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_conditional_range_headers(0, (RANGE_LEN as u64) - 1),
        cache_key.clone(),
        range_spec.clone(),
        all_missing_overlap(&range_spec),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true, // is_signed — CRT requests are SigV4-signed
        None,
        Arc::clone(&inflight_tracker),
        None,
        &ResolvedSettings::default(),
        &None,
    )
    .await
    .expect("range coordination");

    assert_eq!(
        response.status(),
        StatusCode::PARTIAL_CONTENT,
        "conditional range GET should surface the 206 from S3 (not short-circuit \
         to an error or strip the range)"
    );

    // Exactly one signed request reached S3, carrying the conditional headers
    // verbatim. This is the regression guard for the forced-miss fall-through:
    // the SigV4 signature (and the If-Match / Range it covers) must survive.
    let captured = stub.captured();
    assert_eq!(captured.len(), 1, "exactly one S3 call expected");
    assert_eq!(
        captured[0].headers.get("if-match").map(String::as_str),
        Some(TEST_ETAG),
        "the If-Match precondition must be forwarded to S3 verbatim (Req 1.5)"
    );
    assert_eq!(
        captured[0].headers.get("range").map(String::as_str),
        Some(format!("bytes=0-{}", RANGE_LEN - 1).as_str()),
        "the Range header must be forwarded to S3 verbatim (Req 1.5)"
    );
    assert!(
        captured[0].headers.contains_key("authorization"),
        "the SigV4 authorization header must reach S3 (signature preserved)"
    );
}

// =========================================================================
// Req 1.2: a conditional ranged GET with If-None-Match that S3 answers 304
// (T8 regression guard) caches NOTHING and relays the verdict to the client.
// This is the in-process complement to T8 in the fleet suite:
//   Mode A — If-None-Match current → S3 returns 304 → client gets 304, cache untouched.
// =========================================================================

#[tokio::test]
async fn conditional_range_304_does_not_cache() {
    let config = zero_ttl_config();
    let (temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let cache_key = "bucket/crt-conditional-304.bin".to_string();
    let range_spec = RangeSpec {
        start: 0,
        end: (RANGE_LEN as u64) - 1,
    };

    // S3 answers the If-None-Match conditional with 304 Not Modified —
    // the object has not changed since the client's ETag.
    let stub = StubS3Client::new().with_default(StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();

    // Use If-None-Match instead of If-Match for the T8 scenario.
    let mut headers = signed_conditional_range_headers(0, (RANGE_LEN as u64) - 1);
    headers.remove("if-match");
    headers.insert("if-none-match".to_string(), TEST_ETAG.to_string());

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        format!("/{}", cache_key).parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        headers,
        cache_key.clone(),
        range_spec.clone(),
        all_missing_overlap(&range_spec),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &ResolvedSettings::default(),
        &None,
    )
    .await
    .expect("range coordination");

    assert_eq!(
        response.status(),
        StatusCode::NOT_MODIFIED,
        "an If-None-Match match (304) must be relayed to the client — \
         never served as a 200 from the cache (T8 regression guard)"
    );

    // S3 was actually called — the proxy did not serve a stale cache hit.
    let captured = stub.captured();
    assert_eq!(captured.len(), 1, "exactly one S3 call expected");
    assert_eq!(
        captured[0].headers.get("if-none-match").map(String::as_str),
        Some(TEST_ETAG),
        "the If-None-Match header must be forwarded to S3 verbatim (Req 1.5)"
    );
    assert!(
        captured[0].headers.contains_key("authorization"),
        "the SigV4 authorization header must reach S3 (signature preserved)"
    );

    // A 304 carries no body — nothing must have been cached.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let cached_files = count_cache_object_files(temp_dir.path());
    assert_eq!(
        cached_files, 0,
        "a 304 Not Modified must not populate the cache; found {} cached object file(s)",
        cached_files
    );
}

// =========================================================================
// Req 1.2: a conditional ranged GET that S3 answers 412 caches NOTHING
// =========================================================================

#[tokio::test]
async fn conditional_range_412_does_not_cache() {
    let config = zero_ttl_config();
    let (temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());

    let cache_key = "bucket/crt-conditional-412.bin".to_string();
    let range_spec = RangeSpec {
        start: 0,
        end: (RANGE_LEN as u64) - 1,
    };

    // If-Match did not match the current object → S3 returns 412.
    let stub = StubS3Client::new()
        .with_default(StubResponse::with_status(StatusCode::PRECONDITION_FAILED));
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        format!("/{}", cache_key).parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_conditional_range_headers(0, (RANGE_LEN as u64) - 1),
        cache_key.clone(),
        range_spec.clone(),
        all_missing_overlap(&range_spec),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &ResolvedSettings::default(),
        &None,
    )
    .await
    .expect("range coordination");

    assert_eq!(
        response.status(),
        StatusCode::PRECONDITION_FAILED,
        "a failed precondition must be forwarded to the client as 412"
    );

    // Give any (erroneous) async cache write a chance to land, then assert none did.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let cached_files = count_cache_object_files(temp_dir.path());
    assert_eq!(
        cached_files, 0,
        "a 412 response must not populate the cache; found {} cached object file(s)",
        cached_files
    );
}
