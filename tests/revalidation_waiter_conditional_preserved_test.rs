//! A client's own conditional must survive to the wire at every revalidation
//! site — not only the mainline range site 2.8.4 fixed.
//!
//! Spec: the expired entry revalidation feature Requirement 8, extended
//! (R8.15-R8.17 below).
//!
//! # Why this is a separate file from 2.8.4's
//!
//! `tests/revalidation_etag_authority_test.rs` covers the MAINLINE range site
//! (`handle_range_request`'s inline revalidation). That is one of four sites
//! sharing the same defect. The other three are reached only through the
//! coalescing-waiter FALLBACK path: `HttpProxy`'s download-coordination layer
//! lets several concurrent requests for the same key elect one "fetcher" and
//! park the rest as "waiters"; a waiter that times out or whose fetcher errors
//! falls back to revalidating with **its own real client headers** — see
//! `waiter_headers = header_map.clone()` (full-object mainline) and the
//! `headers.clone()` calls inside `serve_from_cache_validated`,
//! `serve_range_from_cache_validated`, and `serve_cached_part_validated`
//! themselves. So this is genuinely client-reachable, not a theoretical
//! internal-only code path, and each of the three needed its own red/green
//! demonstration rather than inheriting the mainline site's.
//!
//! # The two defect classes, and which sites have which
//!
//! | Site | Overwrites client conditional | Status-only verdict on changed-object |
//! |---|---|---|
//! | `handle_get_head_request` (mainline full-object) | yes (fixed 2.8.5) | no such arm exists here |
//! | `serve_from_cache_validated` | yes (fixed 2.8.5) | no — its 200/206 arm always serves+caches fresh bytes, safe regardless |
//! | `serve_range_from_cache_validated` | yes (fixed 2.8.5) | **yes** (fixed 2.8.5) |
//! | `serve_cached_part_validated` | yes (fixed 2.8.5) | **yes** (fixed 2.8.5) |
//!
//! The two range/part sites needed a NEW explicit `Stale` arm, not just the
//! guard. Falling through to their generic "unexpected status" arm would have
//! served stale bytes from the degraded S3-unreachable fallback
//! (`serve_cached_fallback_range` / `serve_cached_part_response`), which is
//! wrong for a response that has just CONFIRMED the object changed.
//!
//! # Fixture shape
//!
//! Each test primes the cache directly (bypassing the proxy, matching this
//! repo's convention for seeding a version the client's request did not
//! produce) and drives the target function with the waiter's real headers,
//! using the harness from `download_coordination_validated_serve_unit_test.rs`.
//! The stub is keyed on the LITERAL client-sent value (e.g. `"*"`), because
//! keying it on the cached ETag reproduces exactly the false-pass this repo
//! has hit before: if the proxy still overwrote the header, the request would
//! never carry `"*"` and the stub's `"*"` rule would never match.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::Method;

use tempfile::TempDir;

use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};

use common::{StubResponse, StubS3Client};

const CACHED_BODY: &[u8] = b"CACHED-OBJECT-BODY-DATA";
const CACHED_ETAG: &str = "\"cached-etag-abc123\"";
const NEW_ETAG: &str = "\"origin-moved-on-v2\"";
const CACHED_LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";
const NEW_BODY: &[u8] = b"FRESH-BODY-AFTER-CHANGE-0000000";

fn waiter_auth() -> String {
    "AWS4-HMAC-SHA256 Credential=AKIA-WAITER/20250101/us-east-1/s3/aws4_request, \
     SignedHeaders=host;x-amz-date, Signature=waiter01"
        .to_string()
}

/// The waiter's REAL client headers, carrying its own conditional. This is
/// what a real client sent — not something the proxy is allowed to overwrite.
fn client_conditional_headers(authorization: &str, if_none_match: &str) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("authorization".to_string(), authorization.to_string());
    headers.insert("host".to_string(), "s3.amazonaws.com".to_string());
    headers.insert("x-amz-date".to_string(), "20250101T000000Z".to_string());
    headers.insert(
        "x-amz-content-sha256".to_string(),
        "UNSIGNED-PAYLOAD".to_string(),
    );
    headers.insert("if-none-match".to_string(), if_none_match.to_string());
    headers
}

fn client_conditional_range_headers(
    authorization: &str,
    if_none_match: &str,
    start: u64,
    end: u64,
) -> HashMap<String, String> {
    let mut headers = client_conditional_headers(authorization, if_none_match);
    headers.insert("range".to_string(), format!("bytes={}-{}", start, end));
    headers
}

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
        CacheEvictionAlgorithm::TinyLFU,
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
    (temp_dir, cache_manager, disk_cache_manager)
}

async fn prime_cache(cache_manager: &Arc<CacheManager>, cache_key: &str) {
    let mut headers: HashMap<String, String> = HashMap::new();
    headers.insert("etag".to_string(), CACHED_ETAG.to_string());
    headers.insert(
        "last-modified".to_string(),
        CACHED_LAST_MODIFIED.to_string(),
    );
    headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    headers.insert("content-length".to_string(), CACHED_BODY.len().to_string());
    let metadata = CacheMetadata {
        etag: CACHED_ETAG.to_string(),
        last_modified: CACHED_LAST_MODIFIED.to_string(),
        content_length: CACHED_BODY.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };
    cache_manager
        .store_response_with_headers(cache_key, CACHED_BODY, headers, metadata)
        .await
        .expect("prime cache");
}

fn new_body_response() -> StubResponse {
    StubResponse::ok(Bytes::from_static(NEW_BODY)).with_header("etag", NEW_ETAG)
}

// =========================================================================
// Site 1: handle_get_head_request (mainline full-object). Not directly
// callable — it is not `pub` — so this is a documentation-only entry mirroring
// 2.8.4's own note: the loopback-proxy harness in
// `tests/common/expired_fixture.rs` already exercises this exact call site for
// the RANGE path (`expired_full_object_mainline_test.rs`), and this file
// covers the three waiter-fallback sites that are `pub` and directly callable.
// See `src/http_proxy.rs`'s `client_sent_conditional` guard immediately before
// `handle_get_head_request`'s validation-header build for the fixed code.
// =========================================================================

// =========================================================================
// Site 2: serve_from_cache_validated
// =========================================================================

/// THE GUARD, shown failing first. A waiter retrying with its own `*`
/// conditional must have that conditional reach S3 unmodified. Before the fix,
/// this site injected the cached ETag over the client's value, so the stub's
/// `"*"`-keyed rule (which models what S3 answers to a wildcard on an existing
/// object) would never match — the request always carried the cached ETag.
#[tokio::test]
async fn full_object_waiter_conditional_reaches_the_wire_unmodified() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/full-wildcard.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    // The object has moved on; a real S3 answers `*` with a 304 regardless
    // (measured 2026-08-31). Keyed on the literal "*" the client sends, not on
    // the cached ETag — see the module docs on why that distinction matters.
    let stub = StubS3Client::new().with_response_for_etag(
        "*",
        StubResponse::not_modified().with_header("etag", NEW_ETAG),
    );
    let s3_client = stub.clone().into_trait_object();

    let _response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/full-wildcard.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        client_conditional_headers(&waiter_auth(), "*"),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .unwrap();

    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        1,
        "must reach the conditional-revalidation call"
    );
    assert_eq!(
        captured[0].if_none_match(),
        Some("*"),
        "R4.5: the client's own `*` must be forwarded unchanged; a different \
         value here means the proxy substituted its own validator"
    );
}

// =========================================================================
// Site 3: serve_range_from_cache_validated
// =========================================================================

/// THE GUARD + THE VERDICT. A range waiter's `If-None-Match: *` reaches S3
/// unmodified, gets a `304` carrying a DIFFERENT ETag (the object moved on),
/// and the response must not be served as "confirmed current" — the stale
/// cached range must not reach the client.
#[tokio::test]
async fn range_waiter_wildcard_conditional_does_not_serve_superseded_bytes() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/range-wildcard.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;
    let range_spec = RangeSpec {
        start: 0,
        end: CACHED_BODY.len() as u64 - 1,
    };

    let stub = StubS3Client::new()
        .with_response_for_etag(
            "*",
            StubResponse::not_modified().with_header("etag", NEW_ETAG),
        )
        .with_default(new_body_response());
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-wildcard.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        client_conditional_range_headers(&waiter_auth(), "*", 0, CACHED_BODY.len() as u64 - 1),
        cache_key.clone(),
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .unwrap();

    let captured = stub.captured();
    assert!(
        !captured.is_empty(),
        "must reach the conditional-revalidation call"
    );
    assert_eq!(
        captured[0].if_none_match(),
        Some("*"),
        "R4.5: the client's own `*` must be forwarded unchanged"
    );

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_ne!(
        body.as_ref(),
        CACHED_BODY,
        "the superseded CACHED bytes must not be served"
    );

    // The entry must not remain claiming the superseded version.
    let still_claims_old = cache_manager
        .get_object_etag(&cache_key)
        .await
        .ok()
        .flatten()
        .map(|e| e == CACHED_ETAG)
        .unwrap_or(false);
    assert!(
        !still_claims_old,
        "R8: a 304 carrying a DIFFERENT ETag proves the cache is superseded; the \
         entry must not survive still claiming {CACHED_ETAG}"
    );
}

/// The control for site 3: a waiter's OWN conditional that happens to equal the
/// cached ETag (a client re-reading through this proxy) is forwarded unchanged,
/// and S3's `304` to it is the client's answer. It used to be served as a cached
/// `206`, which answered a revalidating client with a body.
#[tokio::test]
async fn range_waiter_own_cached_etag_conditional_gets_s3s_304() {
    // `get_ttl` agrees with the `ResolvedSettings::default` passed below. Since the change a
    // fill under a zero `get_ttl` stores an already-expired entry, so this zero config
    // primed an entry the 1h settings below did not describe.
    let config = {
        let mut c = (*zero_ttl_config()).clone();
        c.cache.get_ttl = ResolvedSettings::default().get_ttl;
        Arc::new(c)
    };
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/range-own-etag.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;
    let range_spec = RangeSpec {
        start: 0,
        end: CACHED_BODY.len() as u64 - 1,
    };

    let stub =
        StubS3Client::new().with_response_for_etag(CACHED_ETAG, StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-own-etag.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        client_conditional_range_headers(
            &waiter_auth(),
            CACHED_ETAG,
            0,
            CACHED_BODY.len() as u64 - 1,
        ),
        cache_key,
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        response.status(),
        hyper::StatusCode::NOT_MODIFIED,
        "S3 answered 304 to the client's own If-None-Match; so must the proxy"
    );
    let captured = stub.captured();
    assert_eq!(
        captured[0].if_none_match(),
        Some(CACHED_ETAG),
        "the client's own validator, which happens to equal the cached one, must \
         still be forwarded unchanged rather than substituted"
    );
}

// =========================================================================
// Site 4: serve_cached_part_validated
// =========================================================================

/// THE GUARD + THE VERDICT for the part path. Same shape as the range site.
#[tokio::test]
async fn part_waiter_wildcard_conditional_does_not_serve_superseded_bytes() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/part-wildcard.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let part_body = b"PART-1-DATA-CONTENT";
    let mut part_headers: HashMap<String, String> = HashMap::new();
    part_headers.insert("etag".to_string(), CACHED_ETAG.to_string());
    part_headers.insert(
        "last-modified".to_string(),
        CACHED_LAST_MODIFIED.to_string(),
    );
    part_headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    part_headers.insert("content-length".to_string(), part_body.len().to_string());
    let content_range = format!("bytes 0-{}/{}", part_body.len() - 1, part_body.len());
    let _ = cache_manager
        .store_part_as_range(&cache_key, 1, &content_range, &part_headers, part_body)
        .await;

    let stub = StubS3Client::new()
        .with_response_for_etag(
            "*",
            StubResponse::not_modified().with_header("etag", NEW_ETAG),
        )
        .with_default(new_body_response());
    let s3_client = stub.clone().into_trait_object();

    let _response = HttpProxy::serve_cached_part_validated(
        Method::GET,
        "/bucket/part-wildcard.bin?partNumber=1".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        client_conditional_headers(&waiter_auth(), "*"),
        cache_key,
        1,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::new(Config::default()),
        None,
        &ResolvedSettings::default(),
        &None,
        None,
    )
    .await
    .unwrap();

    let captured = stub.captured();
    assert!(
        !captured.is_empty(),
        "must reach the conditional-revalidation call"
    );
    assert_eq!(
        captured[0].if_none_match(),
        Some("*"),
        "R4.5: the client's own `*` must be forwarded unchanged"
    );
}
