//! Unit tests for validated-serve dispatch correctness.
//!
//! Spec: `.kiro/specs/download-coordination-ttl-correctness/`
//! Task: 9
//!
//! These tests exercise the three validated-serve helpers introduced by
//! Tasks 3–5 (`serve_from_cache_validated`, `serve_range_from_cache_validated`,
//! `serve_cached_part_validated`) and the coordination entry points
//! (`forward_get_head_with_coordination`, `forward_range_with_coordination`,
//! `forward_part_with_coordination`) using the in-process `StubS3Client`
//! harness from `tests/common/mod.rs`. No real S3 connections.
//!
//! **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 3.6, 3.7**

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::metrics::MetricsManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};

use common::{StubResponse, StubS3Client};

// =========================================================================
// Shared fixtures
// =========================================================================

const CACHED_BODY: &[u8] = b"CACHED-OBJECT-BODY-DATA";
const S3_FRESH_BODY: &[u8] = b"FRESH-BODY-FROM-S3-200";
const CACHED_ETAG: &str = "\"cached-etag-abc123\"";
const CACHED_LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

fn waiter_auth() -> String {
    "AWS4-HMAC-SHA256 Credential=AKIA-WAITER/20250101/us-east-1/s3/aws4_request, \
     SignedHeaders=host;x-amz-date, Signature=waiter01"
        .to_string()
}

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

fn signed_range_headers(authorization: &str, start: u64, end: u64) -> HashMap<String, String> {
    let mut headers = signed_headers(authorization);
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

fn coordination_disabled_config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.get_ttl = Duration::from_secs(0);
    config.cache.head_ttl = Duration::from_secs(0);
    config.cache.download_coordination.enabled = false;
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

/// Prime the cache with a full object entry.
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

/// Create a MetricsManager wrapped in the expected Arc<RwLock<>> type.
fn make_metrics() -> Arc<tokio::sync::RwLock<MetricsManager>> {
    Arc::new(tokio::sync::RwLock::new(MetricsManager::new()))
}

/// Read the coalescing stats from the metrics manager.
async fn read_coalescing_stats(
    mm: &Arc<tokio::sync::RwLock<MetricsManager>>,
) -> (u64, u64, u64, u64) {
    let metrics = mm.read().await.collect_metrics().await;
    let c = metrics.coalescing.unwrap();
    (
        c.waiter_conditional_304,
        c.waiter_conditional_200,
        c.waiter_conditional_4xx,
        c.waiter_conditional_error,
    )
}

// =========================================================================
// Full GET validated-serve: 304 → cached body + metric
// Validates: Requirements 2.1, 2.6
// =========================================================================

#[tokio::test]
async fn full_get_validated_serve_304_serves_cached_body() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/full-304.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let stub =
        StubS3Client::new().with_response_for_etag(CACHED_ETAG, StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/full-304.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), CACHED_BODY);

    // Verify metric
    let (c304, c200, c4xx, cerr) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 1, "waiter_conditional_304 should be 1");
    assert_eq!(c200, 0);
    assert_eq!(c4xx, 0);
    assert_eq!(cerr, 0);

    // Verify the stub received exactly one request with the waiter's auth
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].authorization(), Some(waiter_auth().as_str()));
    assert_eq!(captured[0].if_none_match(), Some(CACHED_ETAG));
    assert_eq!(captured[0].if_modified_since(), Some(CACHED_LAST_MODIFIED));
}

// =========================================================================
// Full GET validated-serve: 200 → S3 body + metric
// Validates: Requirements 2.1, 2.7
// =========================================================================

#[tokio::test]
async fn full_get_validated_serve_200_serves_s3_body() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/full-200.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::ok(Bytes::from_static(S3_FRESH_BODY)),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/full-200.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), S3_FRESH_BODY);

    let (c304, c200, c4xx, cerr) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 0);
    assert_eq!(c200, 1, "waiter_conditional_200 should be 1");
    assert_eq!(c4xx, 0);
    assert_eq!(cerr, 0);
}

// =========================================================================
// Full GET validated-serve: 401/403 → S3 response unchanged, cache preserved
// Validates: Requirements 2.5
// =========================================================================

#[tokio::test]
async fn full_get_validated_serve_403_returns_s3_response_cache_preserved() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/full-403.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::forbidden().with_body(Bytes::from_static(b"<AccessDenied/>")),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/full-403.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key.clone(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let (c304, c200, c4xx, cerr) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 0);
    assert_eq!(c200, 0);
    assert_eq!(c4xx, 1, "waiter_conditional_4xx should be 1");
    assert_eq!(cerr, 0);

    // Cache should still be present (not invalidated)
    let meta = cache_manager.get_metadata_cached(&cache_key).await.unwrap();
    assert!(meta.is_some(), "cache should NOT be invalidated on 403");
}

// =========================================================================
// Full GET validated-serve: 500 → falls back to cache-serve
// Validates: Requirements 2.1 (degraded path)
// =========================================================================

#[tokio::test]
async fn full_get_validated_serve_500_falls_back_to_cache() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/full-500.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/full-500.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    // On 5xx the helper falls back to serving from cache (degraded path)
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), CACHED_BODY);

    let (c304, c200, c4xx, cerr) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 0);
    assert_eq!(c200, 0);
    assert_eq!(c4xx, 0);
    assert_eq!(cerr, 1, "waiter_conditional_error should be 1");
}

// =========================================================================
// Full GET validated-serve: metadata missing → delegates to forward
// Validates: Requirements 2.1
// =========================================================================

#[tokio::test]
async fn full_get_validated_serve_metadata_missing_delegates_to_forward() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    // Do NOT prime cache — metadata will be missing
    let cache_key = "bucket/no-metadata.bin".to_string();

    let stub =
        StubS3Client::new().with_default(StubResponse::ok(Bytes::from_static(b"FRESH-FROM-S3")));
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/no-metadata.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        None,
        &None,
    )
    .await
    .unwrap();

    // Should delegate to forward_get_head_to_s3_and_cache which hits S3
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"FRESH-FROM-S3");

    // The stub should have received the waiter's auth
    let captured = stub.captured();
    assert!(!captured.is_empty());
    assert_eq!(captured[0].authorization(), Some(waiter_auth().as_str()));
}

// =========================================================================
// Conditional request headers well-formed
// Validates: Requirements 2.1, 2.6
// =========================================================================

#[tokio::test]
async fn conditional_request_headers_well_formed() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/headers-check.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let stub =
        StubS3Client::new().with_response_for_etag(CACHED_ETAG, StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();

    let auth = waiter_auth();
    let _response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        "/bucket/headers-check.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&auth),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        None,
        &None,
    )
    .await
    .unwrap();

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    let req = &captured[0];

    // If-None-Match = cached ETag
    assert_eq!(
        req.if_none_match(),
        Some(CACHED_ETAG),
        "If-None-Match must equal cached ETag"
    );
    // If-Modified-Since = cached last-modified
    assert_eq!(
        req.if_modified_since(),
        Some(CACHED_LAST_MODIFIED),
        "If-Modified-Since must equal cached last-modified"
    );
    // Waiter's original authorization preserved verbatim
    assert_eq!(
        req.authorization(),
        Some(auth.as_str()),
        "authorization header must be preserved verbatim"
    );
}

// =========================================================================
// HEAD validated-serve: 304 → empty body with cached metadata headers
// Validates: Requirements 2.2
// =========================================================================

#[tokio::test]
async fn head_validated_serve_304_returns_empty_body_with_metadata() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/head-304.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let stub =
        StubS3Client::new().with_response_for_etag(CACHED_ETAG, StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_from_cache_validated(
        Method::HEAD,
        "/bucket/head-304.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(body.is_empty(), "HEAD response must have empty body");

    let (c304, _, _, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 1, "waiter_conditional_304 should be 1 for HEAD");
}

// =========================================================================
// Range validated-serve: 304 → cached range body + metric
// Validates: Requirements 2.4, 2.6
// =========================================================================

#[tokio::test]
async fn range_validated_serve_304_serves_cached_range() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/range-304.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let range_spec = RangeSpec {
        start: 0,
        end: CACHED_BODY.len() as u64 - 1,
    };

    let stub =
        StubS3Client::new().with_response_for_etag(CACHED_ETAG, StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-304.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, CACHED_BODY.len() as u64 - 1),
        cache_key,
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    // Should serve from cache (206 or 200 depending on implementation)
    assert!(
        response.status().is_success(),
        "range 304 should serve cached range successfully, got {}",
        response.status()
    );

    let (c304, _, _, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 1, "waiter_conditional_304 should be 1 for range");

    // Verify Range header was preserved in the conditional request
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    let range_header = captured[0].headers.get("range");
    assert!(
        range_header.is_some(),
        "Range header must be preserved in conditional request"
    );
}

// =========================================================================
// Range validated-serve: 200 → S3 body + metric
// Validates: Requirements 2.4, 2.7
// =========================================================================

#[tokio::test]
async fn range_validated_serve_200_serves_s3_body() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/range-200.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let range_spec = RangeSpec {
        start: 0,
        end: CACHED_BODY.len() as u64 - 1,
    };

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::ok(Bytes::from_static(S3_FRESH_BODY)),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-200.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, CACHED_BODY.len() as u64 - 1),
        cache_key,
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), S3_FRESH_BODY);

    let (_, c200, _, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c200, 1, "waiter_conditional_200 should be 1 for range");
}

// =========================================================================
// Range validated-serve: 403 → S3 response unchanged
// Validates: Requirements 2.5
// =========================================================================

#[tokio::test]
async fn range_validated_serve_403_returns_s3_response() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/range-403.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let range_spec = RangeSpec {
        start: 0,
        end: CACHED_BODY.len() as u64 - 1,
    };

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::forbidden().with_body(Bytes::from_static(b"<AccessDenied/>")),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-403.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, CACHED_BODY.len() as u64 - 1),
        cache_key,
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let (_, _, c4xx, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c4xx, 1, "waiter_conditional_4xx should be 1 for range");
}

// =========================================================================
// Range validated-serve: 500 → falls back to cache
// Validates: Requirements 2.4
// =========================================================================

#[tokio::test]
async fn range_validated_serve_500_falls_back_to_cache() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/range-500.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    let range_spec = RangeSpec {
        start: 0,
        end: CACHED_BODY.len() as u64 - 1,
    };

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-500.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, CACHED_BODY.len() as u64 - 1),
        cache_key,
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    // On 5xx the helper falls back to serving from cache
    assert!(response.status().is_success());

    let (_, _, _, cerr) = read_coalescing_stats(&mm).await;
    assert_eq!(cerr, 1, "waiter_conditional_error should be 1 for range");
}

// =========================================================================
// Range validated-serve: metadata missing → delegates to forward
// Validates: Requirements 2.4
// =========================================================================

#[tokio::test]
async fn range_validated_serve_metadata_missing_delegates_to_forward() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    // Do NOT prime cache
    let cache_key = "bucket/range-no-meta.bin".to_string();

    let range_spec = RangeSpec { start: 0, end: 99 };

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from_static(b"RANGE-DATA"))
            .with_header("content-range", "bytes 0-99/100"),
    );
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        "/bucket/range-no-meta.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, 99),
        cache_key,
        range_spec,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        &None,
    )
    .await
    .unwrap();

    // Should delegate to the signed range forwarding path
    assert!(
        response.status().is_success() || response.status() == StatusCode::PARTIAL_CONTENT,
        "expected success or 206, got {}",
        response.status()
    );

    // The stub should have received the waiter's auth
    let captured = stub.captured();
    assert!(!captured.is_empty());
    assert_eq!(captured[0].authorization(), Some(waiter_auth().as_str()));
}

// =========================================================================
// Part validated-serve: 304 → cached part body + metric
// Validates: Requirements 2.3, 2.6
// =========================================================================

#[tokio::test]
async fn part_validated_serve_304_serves_cached_part() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/part-304.bin".to_string();

    // Prime the cache with a full object AND store a part
    prime_cache(&cache_manager, &cache_key).await;

    // Store part 1 as a range (simulating what the proxy does for part caching)
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

    let stub =
        StubS3Client::new().with_response_for_etag(CACHED_ETAG, StubResponse::not_modified());
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_cached_part_validated(
        Method::GET,
        "/bucket/part-304.bin?partNumber=1".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        1,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert!(
        response.status().is_success(),
        "part 304 should serve cached part, got {}",
        response.status()
    );

    let (c304, _, _, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c304, 1, "waiter_conditional_304 should be 1 for part");
}

// =========================================================================
// Part validated-serve: 200 → S3 body + metric
// Validates: Requirements 2.3, 2.7
// =========================================================================

#[tokio::test]
async fn part_validated_serve_200_serves_s3_body() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/part-200.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    // Store part 1
    let part_body = b"PART-1-OLD";
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

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::ok(Bytes::from_static(b"PART-1-NEW-FROM-S3")),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_cached_part_validated(
        Method::GET,
        "/bucket/part-200.bin?partNumber=1".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        1,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"PART-1-NEW-FROM-S3");

    let (_, c200, _, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c200, 1, "waiter_conditional_200 should be 1 for part");
}

// =========================================================================
// Part validated-serve: 403 → S3 response unchanged
// Validates: Requirements 2.5
// =========================================================================

#[tokio::test]
async fn part_validated_serve_403_returns_s3_response() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/part-403.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    // Store part 1
    let part_body = b"PART-1-DATA";
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

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::forbidden().with_body(Bytes::from_static(b"<AccessDenied/>")),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_cached_part_validated(
        Method::GET,
        "/bucket/part-403.bin?partNumber=1".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        1,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let (_, _, c4xx, _) = read_coalescing_stats(&mm).await;
    assert_eq!(c4xx, 1, "waiter_conditional_4xx should be 1 for part");
}

// =========================================================================
// Part validated-serve: 500 → falls back to cache
// Validates: Requirements 2.3
// =========================================================================

#[tokio::test]
async fn part_validated_serve_500_falls_back_to_cache() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let cache_key = "bucket/part-500.bin".to_string();
    prime_cache(&cache_manager, &cache_key).await;

    // Store part 1
    let part_body = b"PART-1-CACHED";
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

    let stub = StubS3Client::new().with_response_for_etag(
        CACHED_ETAG,
        StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR),
    );
    let s3_client = stub.clone().into_trait_object();
    let mm = make_metrics();

    let response = HttpProxy::serve_cached_part_validated(
        Method::GET,
        "/bucket/part-500.bin?partNumber=1".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        1,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Some(Arc::clone(&mm)),
        &None,
    )
    .await
    .unwrap();

    // On 5xx the helper falls back to serving from cache
    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    // The fallback serves whatever the cache has for this part's byte range.
    // The important assertion is that the response is successful (not 5xx)
    // and the error metric is recorded.
    assert!(!body.is_empty(), "fallback should serve cached part data");

    let (_, _, _, cerr) = read_coalescing_stats(&mm).await;
    assert_eq!(cerr, 1, "waiter_conditional_error should be 1 for part");
}

// =========================================================================
// Part validated-serve: metadata missing → delegates to forward
// Validates: Requirements 2.3
// =========================================================================

#[tokio::test]
async fn part_validated_serve_metadata_missing_delegates_to_forward() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    // Do NOT prime cache — part lookup will fail
    let cache_key = "bucket/part-no-meta.bin".to_string();

    let stub =
        StubS3Client::new().with_default(StubResponse::ok(Bytes::from_static(b"PART-FROM-S3")));
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::serve_cached_part_validated(
        Method::GET,
        "/bucket/part-no-meta.bin?partNumber=1".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key,
        1,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"PART-FROM-S3");

    let captured = stub.captured();
    assert!(!captured.is_empty());
    assert_eq!(captured[0].authorization(), Some(waiter_auth().as_str()));
}

// =========================================================================
// forward_get_head_with_coordination: Fetcher branch calls
// guard.complete_success() on 2xx, guard.complete_error() on non-2xx
// Validates: Requirements 2.1
// =========================================================================

#[tokio::test]
async fn fetcher_branch_completes_success_on_2xx() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/fetcher-success.bin".to_string();

    let stub =
        StubS3Client::new().with_default(StubResponse::ok(Bytes::from_static(b"FETCHER-BODY")));
    let s3_client = stub.clone().into_trait_object();

    let wait_timeout = Duration::from_secs(10);
    let response = HttpProxy::forward_get_head_with_coordination(
        Method::GET,
        "/bucket/fetcher-success.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key.clone(),
        Arc::clone(&cache_manager),
        s3_client,
        Arc::clone(&inflight_tracker),
        Arc::clone(&range_handler),
        Arc::clone(&config),
        true,
        wait_timeout,
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // After fetcher completes successfully, the flight key should be removed
    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "flight key should be removed after fetcher success"
    );
}

#[tokio::test]
async fn fetcher_branch_completes_error_on_non_2xx() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/fetcher-error.bin".to_string();

    let stub = StubS3Client::new().with_default(StubResponse::forbidden());
    let s3_client = stub.clone().into_trait_object();

    let wait_timeout = Duration::from_secs(10);
    let response = HttpProxy::forward_get_head_with_coordination(
        Method::GET,
        "/bucket/fetcher-error.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key.clone(),
        Arc::clone(&cache_manager),
        s3_client,
        Arc::clone(&inflight_tracker),
        Arc::clone(&range_handler),
        Arc::clone(&config),
        true,
        wait_timeout,
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // After fetcher completes with error, the flight key should be removed
    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "flight key should be removed after fetcher error"
    );
}

// =========================================================================
// forward_range_with_coordination: Fetcher branch calls
// guard.complete_success() on 2xx, guard.complete_error() on non-2xx
// Validates: Requirements 2.4
// =========================================================================

#[tokio::test]
async fn range_fetcher_branch_completes_success_on_2xx() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/range-fetcher-success.bin".to_string();
    let range_spec = RangeSpec { start: 0, end: 99 };
    let overlap = s3_proxy::range_handler::RangeOverlap {
        missing_ranges: vec![range_spec.clone()],
        cached_ranges: Vec::new(),
        can_serve_from_cache: false,
    };

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![0u8; 100]))
            .with_header("content-range", "bytes 0-99/100"),
    );
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        "/bucket/range-fetcher-success.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, 99),
        cache_key.clone(),
        range_spec,
        overlap,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
    )
    .await
    .unwrap();

    assert!(
        response.status().is_success() || response.status() == StatusCode::PARTIAL_CONTENT,
        "expected success, got {}",
        response.status()
    );

    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "flight key should be removed after range fetcher success"
    );
}

#[tokio::test]
async fn range_fetcher_branch_completes_error_on_non_2xx() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/range-fetcher-error.bin".to_string();
    let range_spec = RangeSpec { start: 0, end: 99 };
    let overlap = s3_proxy::range_handler::RangeOverlap {
        missing_ranges: vec![range_spec.clone()],
        cached_ranges: Vec::new(),
        can_serve_from_cache: false,
    };

    let stub = StubS3Client::new().with_default(StubResponse::forbidden());
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        "/bucket/range-fetcher-error.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, 99),
        cache_key.clone(),
        range_spec,
        overlap,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "flight key should be removed after range fetcher error"
    );
}

// =========================================================================
// forward_part_with_coordination: Fetcher branch calls
// guard.complete_success() on 2xx, guard.complete_error() on non-2xx
// Validates: Requirements 2.3
// =========================================================================

#[tokio::test]
async fn part_fetcher_branch_completes_success_on_2xx() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/part-fetcher-success.bin".to_string();

    let stub = StubS3Client::new().with_default(StubResponse::ok(Bytes::from_static(b"PART-BODY")));
    let s3_client = stub.clone().into_trait_object();

    let wait_timeout = Duration::from_secs(10);
    let response = HttpProxy::forward_part_with_coordination(
        Method::GET,
        "/bucket/part-fetcher-success.bin?partNumber=1"
            .parse()
            .unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key.clone(),
        1,
        Arc::clone(&cache_manager),
        s3_client,
        Arc::clone(&inflight_tracker),
        Arc::clone(&range_handler),
        true,
        wait_timeout,
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "flight key should be removed after part fetcher success"
    );
}

#[tokio::test]
async fn part_fetcher_branch_completes_error_on_non_2xx() {
    let config = zero_ttl_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/part-fetcher-error.bin".to_string();

    let stub = StubS3Client::new().with_default(StubResponse::forbidden());
    let s3_client = stub.clone().into_trait_object();

    let wait_timeout = Duration::from_secs(10);
    let response = HttpProxy::forward_part_with_coordination(
        Method::GET,
        "/bucket/part-fetcher-error.bin?partNumber=1"
            .parse()
            .unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key.clone(),
        1,
        Arc::clone(&cache_manager),
        s3_client,
        Arc::clone(&inflight_tracker),
        Arc::clone(&range_handler),
        true,
        wait_timeout,
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "flight key should be removed after part fetcher error"
    );
}

// =========================================================================
// Inline full-object expired revalidation (task 6):
// config.cache.download_coordination.enabled=false bypasses the wrapper
// Validates: Requirements 3.7
// =========================================================================

#[tokio::test]
async fn coordination_disabled_bypasses_inflight_tracker_full_object() {
    let config = coordination_disabled_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/disabled-full.bin".to_string();

    let stub =
        StubS3Client::new().with_default(StubResponse::ok(Bytes::from_static(b"DIRECT-BODY")));
    let s3_client = stub.clone().into_trait_object();

    let wait_timeout = Duration::from_secs(10);
    let response = HttpProxy::forward_get_head_with_coordination(
        Method::GET,
        "/bucket/disabled-full.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_headers(&waiter_auth()),
        cache_key.clone(),
        Arc::clone(&cache_manager),
        s3_client,
        Arc::clone(&inflight_tracker),
        Arc::clone(&range_handler),
        Arc::clone(&config),
        false, // coordination_enabled = false
        wait_timeout,
        None,
        &None,
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"DIRECT-BODY");

    // InFlightTracker should never have been called
    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "InFlightTracker should not be used when coordination is disabled"
    );

    // Verify the stub received the request directly (no conditional headers)
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].authorization(), Some(waiter_auth().as_str()));
    // No If-None-Match should be present (direct forward, not conditional)
    assert_eq!(captured[0].if_none_match(), None);
}

// =========================================================================
// Inline range expired revalidation (task 7):
// config.cache.download_coordination.enabled=false bypasses the wrapper
// Validates: Requirements 3.7
// =========================================================================

#[tokio::test]
async fn coordination_disabled_bypasses_inflight_tracker_range() {
    let config = coordination_disabled_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/disabled-range.bin".to_string();
    let range_spec = RangeSpec { start: 0, end: 49 };
    let overlap = s3_proxy::range_handler::RangeOverlap {
        missing_ranges: vec![range_spec.clone()],
        cached_ranges: Vec::new(),
        can_serve_from_cache: false,
    };

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![0u8; 50]))
            .with_header("content-range", "bytes 0-49/50"),
    );
    let s3_client = stub.clone().into_trait_object();

    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        "/bucket/disabled-range.bin".parse().unwrap(),
        "s3.amazonaws.com".to_string(),
        signed_range_headers(&waiter_auth(), 0, 49),
        cache_key.clone(),
        range_spec,
        overlap,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        true,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
    )
    .await
    .unwrap();

    assert!(
        response.status().is_success() || response.status() == StatusCode::PARTIAL_CONTENT,
        "expected success, got {}",
        response.status()
    );

    // InFlightTracker should never have been called
    assert_eq!(
        inflight_tracker.in_flight_count(),
        0,
        "InFlightTracker should not be used when coordination is disabled (range)"
    );
}
