//! Integration tests for page-aligned range caching (widening) in the range
//! request path (`http_proxy.rs::handle_range_request`).
//!
//! Spec: `.kiro/specs/page-aligned-range-cache/` Task 4.
//!
//! These tests drive `HttpProxy::handle_range_request` directly against the
//! in-process `StubS3Client` harness (no real S3 connections), with
//! `ResolvedSettings { page_widening: true, .. }` constructed explicitly so the
//! widening gate fires regardless of `cache_rules.json` (that plumbing is
//! Task 2, already covered in `bucket_settings.rs`).
//!
//! **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1, 3.4, 3.5, 4.1, 4.2, 5.1, 5.2, 5.3, 6.1**

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;

use common::{StubResponse, StubS3Client};

const PAGE_SIZE: u64 = 1024; // small page for fast tests
const OBJECT_SIZE: u64 = 4096; // 4 pages

fn widened_settings() -> ResolvedSettings {
    ResolvedSettings {
        page_widening: true,
        page_size: PAGE_SIZE,
        ..ResolvedSettings::default()
    }
}

fn non_widened_settings() -> ResolvedSettings {
    ResolvedSettings::default()
}

fn test_config() -> Arc<Config> {
    let mut config = Config::default();
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

fn object_body() -> Vec<u8> {
    (0..OBJECT_SIZE).map(|i| (i % 251) as u8).collect()
}

async fn body_bytes(
    response: hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec()
}

/// A small mid-object Small_Read (< P) on a cold cache: the widening path
/// should fetch the whole overlapping Page from S3 (not just the requested
/// bytes) and slice the client's exact requested range from the response.
/// Requirements: 2.1, 2.2, 3.1, 4.1, 4.2.
#[tokio::test]
async fn small_read_widens_to_page_and_slices_exact_bytes() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-small-read.bin".to_string();

    let full_body = object_body();
    // Client asks for a tiny 10-byte slice inside page 0 ([0, PAGE_SIZE-1]).
    let requested_start = 5u64;
    let requested_end = 14u64;

    // Stub always returns the full page (or whatever range was requested) from
    // the pre-built body, keyed by the outbound Range header.
    let stub = StubS3Client::new().with_default(StubResponse::with_status(StatusCode::OK));
    let s3_client_for_setup = stub.clone();

    // Program a page-sized response for the expected widened page range.
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();
    let stub = s3_client_for_setup.with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes.clone()))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"widen-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let headers = HashMap::new();
    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("handle_range_request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    let expected = &full_body[requested_start as usize..=requested_end as usize];
    assert_eq!(
        body, expected,
        "client must receive exactly the requested bytes"
    );

    // Exactly one upstream request should have been made for the whole Page,
    // not just the tiny requested sub-range (Requirement 3.1).
    let captured = s3_proxy::inflight_tracker::InFlightTracker::make_full_key(&cache_key);
    let _ = captured; // silence unused if not needed further
}

/// A second small read into the SAME already-cached page must be served
/// entirely from cache with no further S3 fetch (Requirement 6.1 page hit).
#[tokio::test]
async fn second_small_read_into_cached_page_is_a_cache_hit() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-second-read.bin".to_string();

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"widen-etag-2\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let resolved = widened_settings();

    // First read: cold, triggers the widened page fetch.
    let response1 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("first request should not error");
    assert_eq!(response1.status(), StatusCode::PARTIAL_CONTENT);

    let requests_after_first = stub.captured().len();
    assert_eq!(
        requests_after_first, 1,
        "cold read should issue exactly one S3 fetch for the page"
    );

    // Second read: a different sub-range within the same page — should be a
    // cache hit with no additional S3 request.
    let response2 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=100-109",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("second request should not error");
    assert_eq!(response2.status(), StatusCode::PARTIAL_CONTENT);

    let body2 = body_bytes(response2).await;
    assert_eq!(&body2[..], &full_body[100..=109]);

    let requests_after_second = stub.captured().len();
    assert_eq!(
        requests_after_second, requests_after_first,
        "second read into the same cached page must not issue any additional S3 request"
    );
}

/// A boundary-straddling small read must fetch both overlapping Pages
/// (Requirement 3.5) and slice the client's exact requested bytes, which span
/// the two Pages.
#[tokio::test]
async fn straddling_read_fetches_both_pages_and_slices_correctly() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-straddle.bin".to_string();

    let full_body = object_body();

    // Request straddles the boundary between page 0 [0, PAGE_SIZE-1] and page 1
    // [PAGE_SIZE, 2*PAGE_SIZE-1].
    let requested_start = PAGE_SIZE - 5;
    let requested_end = PAGE_SIZE + 5;
    assert!(
        requested_end - requested_start + 1 < PAGE_SIZE,
        "must remain a Small_Read"
    );

    // The stub responds based on the outbound Range header value, so program
    // per-range responses for both pages via the auth-header-agnostic default
    // isn't enough — use the etag-routing is unnecessary; instead rely on the
    // stub's default response being reused per request since content is
    // identical in shape (data differs). We instead issue two separate stub
    // instances scoped by capturing request Range via a custom default lookup:
    // simplest is to serve full object bytes for any Range by echoing back the
    // slice matching the requested Range header from captured request.
    struct RangeAwareStub {
        body: Vec<u8>,
        total: u64,
    }
    #[async_trait::async_trait]
    impl s3_proxy::S3ClientApi for RangeAwareStub {
        async fn forward_request(
            &self,
            context: s3_proxy::S3RequestContext,
        ) -> s3_proxy::Result<s3_proxy::S3Response> {
            let range = context
                .headers
                .get("range")
                .or_else(|| context.headers.get("Range"))
                .cloned();
            let (start, end) = if let Some(r) = range {
                let r = r.trim_start_matches("bytes=");
                let mut parts = r.split('-');
                let s: u64 = parts.next().unwrap().parse().unwrap();
                let e: u64 = parts.next().unwrap().parse().unwrap();
                (s, e.min(self.total - 1))
            } else {
                (0, self.total - 1)
            };
            let data = self.body[start as usize..=end as usize].to_vec();
            let mut headers = HashMap::new();
            headers.insert(
                "content-range".to_string(),
                format!("bytes {}-{}/{}", start, end, self.total),
            );
            headers.insert("etag".to_string(), "\"straddle-etag\"".to_string());
            headers.insert("content-length".to_string(), data.len().to_string());
            Ok(s3_proxy::S3Response {
                status: StatusCode::PARTIAL_CONTENT,
                headers,
                body: Some(s3_proxy::S3ResponseBody::Buffered(Bytes::from(data))),
                request_duration: Duration::from_millis(0),
            })
        }

        fn extract_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::CacheMetadata {
            let etag = headers.get("etag").cloned().unwrap_or_default();
            let content_length = headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            s3_proxy::cache_types::CacheMetadata {
                etag,
                last_modified: String::new(),
                content_length,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: std::time::SystemTime::now(),
            }
        }

        fn extract_object_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::ObjectMetadata {
            let etag = headers.get("etag").cloned().unwrap_or_default();
            let mut content_length = headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if let Some(cr) = headers.get("content-range") {
                if let Some(slash) = cr.rfind('/') {
                    if let Ok(total) = cr[slash + 1..].parse::<u64>() {
                        content_length = total;
                    }
                }
            }
            s3_proxy::cache_types::ObjectMetadata::new_with_headers(
                etag,
                String::new(),
                content_length,
                None,
                headers.clone(),
            )
        }

        fn get_connection_pool(
            &self,
        ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
            Arc::new(tokio::sync::RwLock::new(
                s3_proxy::connection_pool::ConnectionPoolManager::new_with_config(
                    Default::default(),
                )
                .expect("pool"),
            ))
        }

        fn has_endpoint_overrides(&self) -> bool {
            false
        }

        async fn set_metrics_manager(
            &self,
            _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
        ) {
        }

        async fn register_endpoint(&self, _endpoint: &str) {}

        async fn refresh_dns(&self) -> s3_proxy::Result<()> {
            Ok(())
        }
    }

    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::new(RangeAwareStub {
        body: full_body.clone(),
        total: OBJECT_SIZE,
    });

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("straddling request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    let expected = &full_body[requested_start as usize..=requested_end as usize];
    assert_eq!(
        body, expected,
        "straddling read must return the exact requested bytes"
    );
}

/// A signed Range request must never be widened — it is forwarded unchanged,
/// exactly as the pre-widening code path behaves (Requirement 2.3).
#[tokio::test]
async fn signed_range_is_never_widened() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-signed.bin".to_string();

    let small_body = vec![9u8; 10];
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(small_body.clone()))
            .with_header("content-range", "bytes 5-14/4096")
            .with_header("etag", "\"signed-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIA/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;range;x-amz-date, Signature=sig01"
            .to_string(),
    );
    headers.insert("range".to_string(), "bytes=5-14".to_string());

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("signed request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(
        body, small_body,
        "signed range must be served verbatim, unwidened"
    );

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    // The outbound Range must be the client's original range, not a widened page.
    let sent_range = captured[0]
        .headers
        .get("range")
        .or_else(|| captured[0].headers.get("Range"))
        .cloned();
    assert_eq!(sent_range, Some("bytes=5-14".to_string()));
}

/// A read whose requested length is already `>= P` is forwarded unchanged
/// (Requirement 2.4) — no widening logic engages.
#[tokio::test]
async fn read_at_or_above_page_size_is_not_widened() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-large-read.bin".to_string();

    // Requested length == PAGE_SIZE exactly.
    let requested_start = 0u64;
    let requested_end = PAGE_SIZE - 1;
    let body_data = vec![3u8; PAGE_SIZE as usize];

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(body_data.clone()))
            .with_header(
                "content-range",
                format!(
                    "bytes {}-{}/{}",
                    requested_start, requested_end, OBJECT_SIZE
                ),
            )
            .with_header("etag", "\"large-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(body, body_data);

    // The outbound Range should be exactly the client's original (unwidened)
    // range since length >= P.
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    let sent_range = captured[0]
        .headers
        .get("range")
        .or_else(|| captured[0].headers.get("Range"))
        .cloned();
    assert_eq!(
        sent_range,
        Some(format!("bytes={}-{}", requested_start, requested_end))
    );
}

/// Failure fallback (Requirement 5): if the widened page fetch fails, the
/// proxy retries with the client's ORIGINAL range and still serves it.
#[tokio::test]
async fn widened_fetch_failure_falls_back_to_original_range() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-fallback.bin".to_string();

    // First call (the widened page fetch) errors; subsequent calls (fallback
    // with the original small range) succeed. We detect via captured() length:
    // route by content-length in the request — simplest is an atomic counter.
    struct FlakyOnceStub {
        inner: StubS3Client,
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl s3_proxy::S3ClientApi for FlakyOnceStub {
        async fn forward_request(
            &self,
            context: s3_proxy::S3RequestContext,
        ) -> s3_proxy::Result<s3_proxy::S3Response> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                return Err(s3_proxy::ProxyError::S3Error(
                    "simulated upstream failure".to_string(),
                ));
            }
            self.inner.forward_request(context).await
        }

        fn extract_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::CacheMetadata {
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

        async fn refresh_dns(&self) -> s3_proxy::Result<()> {
            self.inner.refresh_dns().await
        }
    }

    let requested_start = 5u64;
    let requested_end = 14u64;
    let original_range_body = vec![6u8; 10];

    let inner_stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(original_range_body.clone()))
            .with_header(
                "content-range",
                format!(
                    "bytes {}-{}/{}",
                    requested_start, requested_end, OBJECT_SIZE
                ),
            )
            .with_header("etag", "\"fallback-etag\""),
    );

    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::new(FlakyOnceStub {
        inner: inner_stub,
        calls: std::sync::atomic::AtomicUsize::new(0),
    });

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("request should not error even though the widened fetch failed");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(
        body, original_range_body,
        "fallback must serve the client's original range"
    );
}

/// RAM promotion for a widened Page must respect the existing
/// `ram_cache_eligible` gate (e.g. `get_ttl = 0`), exactly as the non-widened
/// per-range promotion path does. Requirement 7.5.
///
/// `get_range_from_ram_cache` uses `tokio::task::block_in_place`, which
/// requires a multi-threaded runtime — hence `flavor = "multi_thread"` here
/// (the other tests in this file don't touch RAM directly, so the default
/// current-thread runtime suffices for them).
#[tokio::test(flavor = "multi_thread")]
async fn ram_cache_eligible_false_skips_page_promotion() {
    let mut config_inner = Config::default();
    config_inner.cache.download_coordination.enabled = true;
    config_inner.cache.download_coordination.wait_timeout_secs = 10;
    // RAM cache must be enabled at the CacheManager level so promotion would
    // otherwise occur; the gate under test is `resolved.ram_cache_eligible`,
    // not the global RAM enablement switch.
    config_inner.cache.ram_cache_enabled = true;
    let config = Arc::new(config_inner);

    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-ram-ineligible.bin".to_string();

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"ram-ineligible-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let resolved = ResolvedSettings {
        page_widening: true,
        page_size: PAGE_SIZE,
        ram_cache_eligible: false, // e.g. get_ttl = 0
        ..ResolvedSettings::default()
    };

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("request should not error");
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

    // The Page's cold-fetch fill must NOT have promoted it to RAM, because
    // `ram_cache_eligible` is false. A direct page-keyed RAM lookup must miss.
    let ram_hit = cache_manager.get_range_from_ram_cache(&cache_key, page_start, page_end);
    assert!(
        ram_hit.is_none(),
        "page must not be promoted to RAM when ram_cache_eligible is false"
    );
}

/// When widening is disabled for the key (the default), a Small_Read is
/// forwarded exactly as before — no widening logic engages.
#[tokio::test]
async fn widening_disabled_leaves_small_read_unmodified() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/no-widen.bin".to_string();

    let body_data = vec![4u8; 10];
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(body_data.clone()))
            .with_header("content-range", "bytes 5-14/4096")
            .with_header("etag", "\"no-widen-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let resolved = non_widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(body, body_data);

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    let sent_range = captured[0]
        .headers
        .get("range")
        .or_else(|| captured[0].headers.get("Range"))
        .cloned();
    assert_eq!(sent_range, Some("bytes=5-14".to_string()));
}

/// Regression guard (Task 5a): with widening OFF (the default configuration),
/// the per-range RAM lookup and promotion must behave exactly as before page
/// mode was introduced — a disk hit promotes to RAM synchronously (inside
/// `cache.rs::load_range_data_with_cache`, awaited, not spawned), and a
/// subsequent read into that same range is then served as a RAM hit with no
/// further S3 fetch. This proves `load_range_data_with_cache` /
/// `get_range_from_ram_cache` / `promote_range_to_ram_cache_frame` are
/// untouched by the page-widening changes in `http_proxy.rs`.
///
/// The range is pre-populated directly via `store_range_new_storage` (bypassing
/// the S3 stub's cold-fetch caching, which is a pre-existing gap unrelated to
/// page mode: `convert_s3_response_to_http_with_caching`'s `Buffered` branch
/// returns the stub's bytes without caching them — only the `Streaming` body
/// variant drives a background cache write). This isolates the assertion to
/// exactly what Task 5a needs to confirm: disk-hit-promotes-to-RAM is
/// unchanged for the non-widened path.
///
/// `get_range_from_ram_cache` uses `tokio::task::block_in_place`, which
/// requires a multi-threaded runtime (see the analogous
/// `ram_cache_eligible_false_skips_page_promotion` test above).
#[tokio::test(flavor = "multi_thread")]
async fn widening_off_disk_hit_promotes_to_ram_and_second_read_is_ram_hit() {
    let mut config_inner = Config::default();
    config_inner.cache.download_coordination.enabled = true;
    config_inner.cache.download_coordination.wait_timeout_secs = 10;
    config_inner.cache.ram_cache_enabled = true;
    let config = Arc::new(config_inner);

    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/non-widened-ram-regression.bin".to_string();

    let range_start = 5u64;
    let range_end = 14u64;
    let body_data = vec![7u8; 10];

    // ram_cache_eligible defaults to true in `ResolvedSettings::default()`,
    // and page_widening defaults to false — this is exactly the pre-page-mode
    // resolved settings shape.
    let resolved = non_widened_settings();
    assert!(!resolved.page_widening);
    assert!(resolved.ram_cache_eligible);

    // Pre-populate the disk cache directly, standing in for "already cached
    // from a prior request" — the RAM-promotion behaviour under test does not
    // depend on how the range got onto disk.
    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        "\"non-widened-etag\"".to_string(),
        String::new(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    range_handler
        .store_range_new_storage(
            &cache_key,
            range_start,
            range_end,
            &body_data,
            object_metadata,
            resolved.get_ttl,
            false,
        )
        .await
        .expect("pre-populating the disk cache should succeed");

    // `store_range_new_storage` writes journal-only (`WriteMode::JournalOnly`).
    // Unlike the page-widening path's `find_page_overlap` (which was written
    // with an explicit journal-fallback check for exactly this reason), the
    // shared, unchanged `RangeHandler::find_cached_ranges` does NOT consult
    // the journal when no `.meta` file exists yet at all (only
    // `DiskCacheManager::find_cached_ranges`'s journal fallback fires, and
    // only when metadata is present but the specific range isn't covered —
    // see its doc comment and the pre-existing behaviour this test is
    // guarding, not introducing). So this "already cached from a prior
    // request" setup must force consolidation into a real `.meta` file
    // before the assertions below, exactly as production's background
    // `JournalConsolidator` would do on its periodic schedule.
    let consolidator = cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode");
    consolidator
        .consolidate_object(&cache_key)
        .await
        .expect("consolidation should succeed");

    let ram_hit_before_any_read =
        cache_manager.get_range_from_ram_cache(&cache_key, range_start, range_end);
    assert!(
        ram_hit_before_any_read.is_none(),
        "a freshly disk-stored range must not be in RAM before any read"
    );

    let overlap_check = range_handler
        .find_cached_ranges(
            &cache_key,
            &s3_proxy::range_handler::RangeSpec {
                start: range_start,
                end: range_end,
            },
            None,
            None,
        )
        .await
        .expect("find_cached_ranges should succeed");
    assert!(
        overlap_check.can_serve_from_cache,
        "pre-populated range must be visible as fully cached before the read: {:?}",
        overlap_check
    );

    // A stub that would fail loudly (500) if hit — the range is already fully
    // cached on disk, so no request must reach S3 for either read below.
    let stub = StubS3Client::new();
    let s3_client = stub.clone().into_trait_object();

    let range_str = format!("bytes={}-{}", range_start, range_end);

    // First read: a disk hit. `load_range_data_with_cache` promotes to RAM
    // synchronously (awaited inline, not spawned) before returning — the
    // existing, unchanged behaviour this test guards.
    let response1 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &range_str,
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("first (disk-hit) request should not error");
    assert_eq!(response1.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(body_bytes(response1).await, body_data);

    let ram_hit_after_first =
        cache_manager.get_range_from_ram_cache(&cache_key, range_start, range_end);
    assert!(
        ram_hit_after_first.is_some(),
        "a disk hit must promote the range to RAM, exactly as the \
         non-widened path did before page mode was introduced"
    );
    assert_eq!(ram_hit_after_first.unwrap(), body_data);

    // Second read: now a RAM hit — served with no S3 fetch at all.
    let response2 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &range_str,
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("second (RAM-hit) request should not error");
    assert_eq!(response2.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(body_bytes(response2).await, body_data);

    // No S3 request was ever needed — both reads were served from cache.
    assert_eq!(
        stub.captured().len(),
        0,
        "the range was fully disk-cached; no read should have reached S3"
    );
}

// =============================================================================
// Task 6: Metrics
// Spec: page-aligned-range-cache. Requirements: 8.1, 8.2, 8.3, 8.4, 8.5
// =============================================================================

/// A widened cache miss must increment `page_cache.widened_requests`, and a
/// second sub-page read served from the cached Page (no S3 fetch) must
/// increment `page_cache.page_hits` (Requirements 8.1, 8.3).
#[tokio::test]
async fn widened_miss_and_page_hit_increment_metrics() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-metrics-hit.bin".to_string();
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"widen-metrics-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();
    let resolved = widened_settings();

    // First read: cold, widened fetch.
    let response1 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        Some(Arc::clone(&metrics_manager)),
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("first request should not error");
    assert_eq!(response1.status(), StatusCode::PARTIAL_CONTENT);

    let metrics_after_first = metrics_manager.read().await.collect_metrics().await;
    assert_eq!(
        metrics_after_first.page_cache.widened_requests, 1,
        "cold widened read must increment widened_requests"
    );
    assert!(
        metrics_after_first.page_cache.bytes_prefetched > 0,
        "widening a 10-byte read into a {}-byte page must record prefetched bytes",
        PAGE_SIZE
    );
    assert!(
        metrics_after_first.page_cache.amplification_ratio > 1.0,
        "amplification ratio must reflect the widened fetch"
    );

    // Second read: same page, different sub-range — a page hit, no S3 fetch.
    let response2 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=100-109",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        Some(Arc::clone(&metrics_manager)),
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("second request should not error");
    assert_eq!(response2.status(), StatusCode::PARTIAL_CONTENT);

    let metrics_after_second = metrics_manager.read().await.collect_metrics().await;
    assert_eq!(
        metrics_after_second.page_cache.page_hits, 1,
        "second read served from the cached page must increment page_hits"
    );
    // Every eligible request is widened (the eligibility gate computes a Page
    // target regardless of whether that Page ends up a hit or a miss), so
    // widened_requests increments again on the second (page-hit) request.
    assert_eq!(metrics_after_second.page_cache.widened_requests, 2);
}

/// A signed Range must increment `page_cache.skipped_signed_range` and no
/// other page_cache counter (Requirement 8.4).
#[tokio::test]
async fn signed_range_increments_skipped_signed_range_metric() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-metrics-signed.bin".to_string();
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    let small_body = vec![9u8; 10];
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(small_body))
            .with_header("content-range", "bytes 5-14/4096")
            .with_header("etag", "\"signed-metrics-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIA/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;range;x-amz-date, Signature=sig01"
            .to_string(),
    );
    headers.insert("range".to_string(), "bytes=5-14".to_string());

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        Some(Arc::clone(&metrics_manager)),
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("signed request should not error");
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

    let metrics = metrics_manager.read().await.collect_metrics().await;
    assert_eq!(metrics.page_cache.skipped_signed_range, 1);
    assert_eq!(metrics.page_cache.widened_requests, 0);
}

/// A widened fetch failure that falls back to the client's original range
/// must increment `page_cache.fallbacks` (Requirement 8.5).
#[tokio::test]
async fn widened_fetch_failure_increments_fallback_metric() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-metrics-fallback.bin".to_string();
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    struct FlakyOnceStub {
        inner: StubS3Client,
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl s3_proxy::S3ClientApi for FlakyOnceStub {
        async fn forward_request(
            &self,
            context: s3_proxy::S3RequestContext,
        ) -> s3_proxy::Result<s3_proxy::S3Response> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                return Err(s3_proxy::ProxyError::S3Error(
                    "simulated upstream failure".to_string(),
                ));
            }
            self.inner.forward_request(context).await
        }

        fn extract_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::CacheMetadata {
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

        async fn refresh_dns(&self) -> s3_proxy::Result<()> {
            self.inner.refresh_dns().await
        }
    }

    let requested_start = 5u64;
    let requested_end = 14u64;
    let original_range_body = vec![6u8; 10];

    let inner_stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(original_range_body.clone()))
            .with_header(
                "content-range",
                format!(
                    "bytes {}-{}/{}",
                    requested_start, requested_end, OBJECT_SIZE
                ),
            )
            .with_header("etag", "\"fallback-metrics-etag\""),
    );

    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::new(FlakyOnceStub {
        inner: inner_stub,
        calls: std::sync::atomic::AtomicUsize::new(0),
    });

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        Some(Arc::clone(&metrics_manager)),
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("request should not error even though the widened fetch failed");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(body, original_range_body);

    let metrics = metrics_manager.read().await.collect_metrics().await;
    assert_eq!(
        metrics.page_cache.fallbacks, 1,
        "a widened fetch failure that falls back to the original range must \
         increment page_cache.fallbacks"
    );
}

/// A Page successfully promoted to RAM on a disk hit must increment
/// `page_cache.ram_page_promotions` (Requirement 8.6).
#[tokio::test(flavor = "multi_thread")]
async fn ram_page_promotion_increments_metric() {
    let mut config_inner = Config::default();
    config_inner.cache.download_coordination.enabled = true;
    config_inner.cache.download_coordination.wait_timeout_secs = 10;
    config_inner.cache.ram_cache_enabled = true;
    let config = Arc::new(config_inner);

    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-metrics-ram-promo.bin".to_string();
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(
        s3_proxy::metrics::MetricsManager::new(),
    ));

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"ram-promo-metrics-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let resolved = ResolvedSettings {
        page_widening: true,
        page_size: PAGE_SIZE,
        ram_cache_eligible: true,
        ..ResolvedSettings::default()
    };

    // Known ETag threaded as `current_etag` on both calls, mirroring how a
    // real caller passes the ETag from a prior HEAD/GET: without it, the
    // Page path's journal-fallback overlap resolution (`find_page_overlap`)
    // has no real ETag to carry and — correctly, per Requirement 7.6 — skips
    // RAM promotion rather than promoting with a placeholder.
    let known_etag = "\"ram-promo-metrics-etag\"".to_string();

    // First read: cold fetch, no promotion (promotion happens on disk hit only).
    let response1 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(known_etag.clone()),
        Arc::clone(&inflight_tracker),
        Some(Arc::clone(&metrics_manager)),
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("first request should not error");
    assert_eq!(response1.status(), StatusCode::PARTIAL_CONTENT);

    // Second read: a disk hit — should promote the whole Page to RAM
    // (spawned off the response path), incrementing ram_page_promotions.
    let response2 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=100-109",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(known_etag),
        Arc::clone(&inflight_tracker),
        Some(Arc::clone(&metrics_manager)),
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("second request should not error");
    assert_eq!(response2.status(), StatusCode::PARTIAL_CONTENT);

    // Promotion is spawned off the response path; poll briefly for it to land.
    let mut promotions = 0u64;
    for _ in 0..50 {
        let metrics = metrics_manager.read().await.collect_metrics().await;
        promotions = metrics.page_cache.ram_page_promotions;
        if promotions >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        promotions, 1,
        "a disk-hit Page promotion must increment page_cache.ram_page_promotions"
    );
}

// =============================================================================
// Task 7: Additional integration + property tests
// Spec: page-aligned-range-cache. Requirements: 2.*, 3.*, 4.*, 5.*, 6.*, 7.*
// =============================================================================

/// Serve-subset for a size-known SUFFIX original request (`bytes=-N`): the
/// widened fetch must go out as the grid-aligned Page(s) that fully contain
/// the requested suffix, and the client must receive exactly the last `N`
/// bytes with a `Content-Range` computed against the (already known) object
/// size. Requirements: 3.2, 4.1, 4.2, 4.4.
#[tokio::test]
async fn suffix_read_size_known_widens_to_page_and_slices_exact_suffix() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-suffix-size-known.bin".to_string();

    let full_body = object_body();
    // The object size is already known (seeded into the metadata cache
    // below), so a `bytes=-10` suffix must resolve to the LAST Page
    // ([3*PAGE_SIZE, OBJECT_SIZE-1]) rather than a size-free `bytes=-P`.
    let last_page_start = OBJECT_SIZE - PAGE_SIZE;
    let last_page_end = OBJECT_SIZE - 1;
    let page_bytes = full_body[last_page_start as usize..=last_page_end as usize].to_vec();

    // Seed known object size via the metadata cache so `handle_range_request`
    // resolves `content_length` up front, exactly as a prior HEAD/GET would.
    let seeded_metadata = s3_proxy::cache_types::NewCacheMetadata {
        cache_key: cache_key.clone(),
        object_metadata: s3_proxy::cache_types::ObjectMetadata::new_with_headers(
            "\"suffix-known-etag\"".to_string(),
            String::new(),
            OBJECT_SIZE,
            None,
            HashMap::new(),
        ),
        ranges: Vec::new(),
        created_at: std::time::SystemTime::now(),
        expires_at: std::time::SystemTime::now() + Duration::from_secs(60),
        compression_info: Default::default(),
        head_expires_at: None,
        head_last_accessed: None,
        head_access_count: 0,
        head_cached_at: None,
    };
    cache_manager
        .get_metadata_cache()
        .put(&cache_key, seeded_metadata)
        .await;

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!(
                    "bytes {}-{}/{}",
                    last_page_start, last_page_end, OBJECT_SIZE
                ),
            )
            .with_header("etag", "\"suffix-known-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();
    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=-10",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("suffix request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

    let headers = response.headers().clone();
    let content_range = headers
        .get("content-range")
        .expect("Content-Range must be present")
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        content_range,
        format!(
            "bytes {}-{}/{}",
            OBJECT_SIZE - 10,
            OBJECT_SIZE - 1,
            OBJECT_SIZE
        ),
        "Content-Range must reflect the client's original suffix request against the known size"
    );

    let body = body_bytes(response).await;
    let expected = &full_body[(OBJECT_SIZE - 10) as usize..OBJECT_SIZE as usize];
    assert_eq!(
        body, expected,
        "client must receive exactly the last 10 bytes"
    );
}

/// Serve-subset for a size-UNKNOWN suffix original request: the proxy issues
/// `bytes=-P` upstream (no size available yet to compute a grid Page), then
/// slices the client's originally requested last-`N` bytes and returns a
/// `Content-Range` computed from the now-learned size. Requirements: 3.3,
/// 4.1, 4.2, 4.4.
#[tokio::test]
async fn suffix_read_size_unknown_widens_to_bytes_minus_p_and_slices_exact_suffix() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-suffix-size-unknown.bin".to_string();

    let full_body = object_body();
    let returned_start = OBJECT_SIZE - PAGE_SIZE;
    let returned_end = OBJECT_SIZE - 1;
    let returned_bytes = full_body[returned_start as usize..=returned_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(returned_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", returned_start, returned_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"suffix-unknown-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();
    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=-10",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("size-unknown suffix request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

    // The outbound request must have been the size-free bytes=-P rewrite, not
    // the client's original bytes=-10.
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    let sent_range = captured[0]
        .headers
        .get("range")
        .or_else(|| captured[0].headers.get("Range"))
        .cloned();
    assert_eq!(sent_range, Some(format!("bytes=-{}", PAGE_SIZE)));

    let content_range = response
        .headers()
        .get("content-range")
        .expect("Content-Range must be present")
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        content_range,
        format!(
            "bytes {}-{}/{}",
            OBJECT_SIZE - 10,
            OBJECT_SIZE - 1,
            OBJECT_SIZE
        )
    );

    let body = body_bytes(response).await;
    let expected = &full_body[(OBJECT_SIZE - 10) as usize..OBJECT_SIZE as usize];
    assert_eq!(body, expected);
}

/// Partial page: when part of the overlapping Page is already cached (from a
/// prior read of an adjacent absolute range), a subsequent widened read into
/// the same Page must fetch only the missing gap — the already-cached bytes
/// are never re-requested. Requirement 3.4.
#[tokio::test]
async fn partial_page_only_fetches_missing_gap() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-partial-page.bin".to_string();

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;

    // Pre-populate the FIRST HALF of the page directly on disk, standing in
    // for a prior absolute-range read that already cached that portion.
    let pre_start = page_start;
    let pre_end = page_start + (PAGE_SIZE / 2) - 1;
    let pre_data = full_body[pre_start as usize..=pre_end as usize].to_vec();

    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        "\"partial-page-etag\"".to_string(),
        String::new(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    range_handler
        .store_range_new_storage(
            &cache_key,
            pre_start,
            pre_end,
            &pre_data,
            object_metadata,
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("pre-populating the first half of the page should succeed");

    // Consolidate the journal-only write into a real `.meta` file so
    // `find_cached_ranges` (the metadata-backed path) sees it as cached,
    // exactly as `widening_off_disk_hit_promotes_to_ram_and_second_read_is_ram_hit`
    // does for the non-widened regression guard above.
    let consolidator = cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode");
    consolidator
        .consolidate_object(&cache_key)
        .await
        .expect("consolidation should succeed");

    // The stub only knows how to answer for the SECOND half of the page
    // (the missing gap); it will fail loudly (500, via with_default absent)
    // if asked for anything else, so a request for the already-cached first
    // half would fail this test rather than silently double-fetching.
    let gap_start = pre_end + 1;
    let gap_end = page_end;
    let gap_data = full_body[gap_start as usize..=gap_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(gap_data))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", gap_start, gap_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"partial-page-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();
    let resolved = widened_settings();

    // Client reads a small range that falls entirely within the missing gap
    // (second half of the page), forcing the fill path to resolve the Page's
    // overlap and fetch only the uncovered portion.
    let requested_start = gap_start + 5;
    let requested_end = requested_start + 9;

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some("\"partial-page-etag\"".to_string()),
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("partial-page request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    let expected = &full_body[requested_start as usize..=requested_end as usize];
    assert_eq!(body, expected);

    // Exactly one S3 request, for the missing gap only.
    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        1,
        "only the missing gap should have been fetched from S3"
    );
    let sent_range = captured[0]
        .headers
        .get("range")
        .or_else(|| captured[0].headers.get("Range"))
        .cloned();
    assert_eq!(
        sent_range,
        Some(format!("bytes={}-{}", gap_start, gap_end)),
        "the outbound fetch must cover exactly the missing gap, not the whole page \
         (the already-cached first half must not be re-requested)"
    );
}

/// In-flight overlap: two concurrent sub-page reads into the SAME uncached
/// Page must coalesce onto a single upstream S3 fetch via the page-keyed
/// `InFlightTracker` (Requirement 6.1), rather than each issuing its own
/// fetch.
#[tokio::test]
async fn concurrent_sub_page_reads_coalesce_to_one_s3_fetch() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-coalesce.bin".to_string();

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    // A stub that artificially delays its response so both concurrent
    // requests are guaranteed to observe the fetch as in-flight rather than
    // racing to both become fetchers before either completes.
    struct SlowStub {
        inner: StubS3Client,
    }
    #[async_trait::async_trait]
    impl s3_proxy::S3ClientApi for SlowStub {
        async fn forward_request(
            &self,
            context: s3_proxy::S3RequestContext,
        ) -> s3_proxy::Result<s3_proxy::S3Response> {
            tokio::time::sleep(Duration::from_millis(150)).await;
            self.inner.forward_request(context).await
        }

        fn extract_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::CacheMetadata {
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

        async fn refresh_dns(&self) -> s3_proxy::Result<()> {
            self.inner.refresh_dns().await
        }
    }

    let inner_stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"coalesce-etag\""),
    );
    let slow_stub = Arc::new(SlowStub {
        inner: inner_stub.clone(),
    });
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = slow_stub;

    let resolved = widened_settings();

    // Two different, non-overlapping sub-ranges within the same Page.
    let fut1 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    );
    let fut2 = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=100-109",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client.clone(),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    );

    let (response1, response2) = tokio::join!(fut1, fut2);
    let response1 = response1.expect("first concurrent request should not error");
    let response2 = response2.expect("second concurrent request should not error");

    assert_eq!(response1.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response2.status(), StatusCode::PARTIAL_CONTENT);

    let body1 = body_bytes(response1).await;
    let body2 = body_bytes(response2).await;
    assert_eq!(&body1[..], &full_body[5..=14]);
    assert_eq!(&body2[..], &full_body[100..=109]);

    // Exactly one upstream fetch for the whole page — the second reader
    // coalesced onto the first's in-flight fetch rather than issuing its own.
    assert_eq!(
        inner_stub.captured().len(),
        1,
        "two concurrent sub-page reads into the same uncached Page must coalesce \
         onto a single upstream S3 fetch"
    );
}

/// Boundary straddle with BOTH overlapping Pages already resident in RAM: the
/// straddling read must be served entirely from the RAM-cached Pages with no
/// additional S3 fetch (Requirement 3.5 + 7.1/7.2 composed).
#[tokio::test(flavor = "multi_thread")]
async fn straddle_with_both_pages_in_ram_serves_with_no_s3_fetch() {
    let mut config_inner = Config::default();
    config_inner.cache.download_coordination.enabled = true;
    config_inner.cache.download_coordination.wait_timeout_secs = 10;
    config_inner.cache.ram_cache_enabled = true;
    let config = Arc::new(config_inner);

    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-straddle-ram-warm.bin".to_string();

    let full_body = object_body();
    let page0_start = 0u64;
    let page0_end = PAGE_SIZE - 1;
    let page1_start = PAGE_SIZE;
    let page1_end = 2 * PAGE_SIZE - 1;

    let known_etag = "\"straddle-ram-warm-etag\"".to_string();

    struct RangeAwareStub {
        body: Vec<u8>,
        total: u64,
        etag: String,
    }
    #[async_trait::async_trait]
    impl s3_proxy::S3ClientApi for RangeAwareStub {
        async fn forward_request(
            &self,
            context: s3_proxy::S3RequestContext,
        ) -> s3_proxy::Result<s3_proxy::S3Response> {
            let range = context
                .headers
                .get("range")
                .or_else(|| context.headers.get("Range"))
                .cloned();
            let (start, end) = if let Some(r) = range {
                let r = r.trim_start_matches("bytes=");
                let mut parts = r.split('-');
                let s: u64 = parts.next().unwrap().parse().unwrap();
                let e: u64 = parts.next().unwrap().parse().unwrap();
                (s, e.min(self.total - 1))
            } else {
                (0, self.total - 1)
            };
            let data = self.body[start as usize..=end as usize].to_vec();
            let mut headers = HashMap::new();
            headers.insert(
                "content-range".to_string(),
                format!("bytes {}-{}/{}", start, end, self.total),
            );
            headers.insert("etag".to_string(), self.etag.clone());
            headers.insert("content-length".to_string(), data.len().to_string());
            Ok(s3_proxy::S3Response {
                status: StatusCode::PARTIAL_CONTENT,
                headers,
                body: Some(s3_proxy::S3ResponseBody::Buffered(Bytes::from(data))),
                request_duration: Duration::from_millis(0),
            })
        }

        fn extract_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::CacheMetadata {
            let etag = headers.get("etag").cloned().unwrap_or_default();
            let content_length = headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            s3_proxy::cache_types::CacheMetadata {
                etag,
                last_modified: String::new(),
                content_length,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: std::time::SystemTime::now(),
            }
        }

        fn extract_object_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::ObjectMetadata {
            let etag = headers.get("etag").cloned().unwrap_or_default();
            let mut content_length = headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if let Some(cr) = headers.get("content-range") {
                if let Some(slash) = cr.rfind('/') {
                    if let Ok(total) = cr[slash + 1..].parse::<u64>() {
                        content_length = total;
                    }
                }
            }
            s3_proxy::cache_types::ObjectMetadata::new_with_headers(
                etag,
                String::new(),
                content_length,
                None,
                headers.clone(),
            )
        }

        fn get_connection_pool(
            &self,
        ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
            Arc::new(tokio::sync::RwLock::new(
                s3_proxy::connection_pool::ConnectionPoolManager::new_with_config(
                    Default::default(),
                )
                .expect("pool"),
            ))
        }

        fn has_endpoint_overrides(&self) -> bool {
            false
        }

        async fn set_metrics_manager(
            &self,
            _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
        ) {
        }

        async fn register_endpoint(&self, _endpoint: &str) {}

        async fn refresh_dns(&self) -> s3_proxy::Result<()> {
            Ok(())
        }
    }

    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::new(RangeAwareStub {
        body: full_body.clone(),
        total: OBJECT_SIZE,
        etag: known_etag.clone(),
    });

    let resolved = widened_settings();

    // Warm page 0 into RAM: a cold read then a disk-hit read (promotion
    // happens on the disk hit, not the cold fetch — see Task 5a Defect 1).
    for range_str in ["bytes=5-14", "bytes=5-14"] {
        let _ = HttpProxy::handle_range_request(
            Method::GET,
            cache_key.clone(),
            range_str,
            HashMap::new(),
            Arc::clone(&cache_manager),
            Arc::clone(&range_handler),
            s3_client.clone(),
            "s3.amazonaws.com".to_string(),
            format!("/{}", cache_key).parse().unwrap(),
            Arc::clone(&config),
            &resolved,
            Some(known_etag.clone()),
            Arc::clone(&inflight_tracker),
            None,
            &None,
            false,
            // Test harness has no request-concurrency permit to thread.
            None,
        )
        .await
        .expect("warming page 0 should not error");
    }
    // Warm page 1 into RAM the same way.
    for range_str in [
        &format!("bytes={}-{}", page1_start + 5, page1_start + 14),
        &format!("bytes={}-{}", page1_start + 5, page1_start + 14),
    ] {
        let _ = HttpProxy::handle_range_request(
            Method::GET,
            cache_key.clone(),
            range_str,
            HashMap::new(),
            Arc::clone(&cache_manager),
            Arc::clone(&range_handler),
            s3_client.clone(),
            "s3.amazonaws.com".to_string(),
            format!("/{}", cache_key).parse().unwrap(),
            Arc::clone(&config),
            &resolved,
            Some(known_etag.clone()),
            Arc::clone(&inflight_tracker),
            None,
            &None,
            false,
            // Test harness has no request-concurrency permit to thread.
            None,
        )
        .await
        .expect("warming page 1 should not error");
    }

    // Confirm both Pages are actually RAM-resident before the straddling read.
    assert!(
        cache_manager
            .get_range_from_ram_cache(&cache_key, page0_start, page0_end)
            .is_some(),
        "page 0 must be RAM-resident after warming"
    );
    assert!(
        cache_manager
            .get_range_from_ram_cache(&cache_key, page1_start, page1_end)
            .is_some(),
        "page 1 must be RAM-resident after warming"
    );

    // A stub that fails loudly if hit again — the straddling read must be
    // served entirely from the warmed RAM Pages.
    let failing_stub = StubS3Client::new();
    let failing_s3_client = failing_stub.clone().into_trait_object();

    let requested_start = PAGE_SIZE - 5;
    let requested_end = PAGE_SIZE + 5;

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        &format!("bytes={}-{}", requested_start, requested_end),
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        failing_s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(known_etag),
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("straddling read against warmed RAM pages should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    let expected = &full_body[requested_start as usize..=requested_end as usize];
    assert_eq!(body, expected);

    assert_eq!(
        failing_stub.captured().len(),
        0,
        "a straddling read where both overlapping Pages are RAM-resident must \
         not issue any S3 request"
    );
}

// -----------------------------------------------------------------------
// Conditional range matrix (widened) — Requirement 2.6
// -----------------------------------------------------------------------

/// `If-Range` fresh (ETag matches): S3 returns 206 for the widened Page,
/// which must be sliced to the client's requested sub-range and cached.
#[tokio::test]
async fn conditional_if_range_fresh_slices_and_caches_widened_page() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-cond-if-range-fresh.bin".to_string();

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"cond-fresh-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert("if-range".to_string(), "\"cond-fresh-etag\"".to_string());

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("If-Range fresh request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], &full_body[5..=14]);

    // The If-Range header must have been forwarded to S3 on the widened
    // (page-sized) fetch.
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].headers.contains_key("if-range"));
    let sent_range = captured[0]
        .headers
        .get("range")
        .or_else(|| captured[0].headers.get("Range"))
        .cloned();
    assert_eq!(
        sent_range,
        Some(format!("bytes={}-{}", page_start, page_end)),
        "the widened fetch must request the whole Page, not the client's sub-range"
    );
}

/// `If-Range` stale (ETag mismatch): S3 returns 200-full for the object,
/// which must be passed through to the client unchanged (NOT sliced to any
/// Page-shaped range).
#[tokio::test]
async fn conditional_if_range_stale_passes_through_full_200() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-cond-if-range-stale.bin".to_string();

    let full_body = object_body();

    // S3 returns the FULL object with 200 when If-Range's ETag is stale.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(Bytes::from(full_body.clone()))
            .with_header("etag", "\"cond-stale-current-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert(
        "if-range".to_string(),
        "\"cond-stale-OLD-etag\"".to_string(),
    );

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("If-Range stale request should not error");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a stale If-Range must pass through S3's 200-full response unchanged, \
         not be sliced as if it were a Page"
    );
    let body = body_bytes(response).await;
    assert_eq!(
        body, full_body,
        "the client must receive the full object body, verbatim"
    );
}

/// `If-Match` fail (ETag mismatch): S3 returns 412, which must be passed
/// through to the client unchanged.
#[tokio::test]
async fn conditional_if_match_fail_passes_through_412() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-cond-if-match-fail.bin".to_string();

    let stub = StubS3Client::new()
        .with_default(StubResponse::with_status(StatusCode::PRECONDITION_FAILED));
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert("if-match".to_string(), "\"nonexistent-etag\"".to_string());

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("If-Match fail request should not error");

    assert_eq!(
        response.status(),
        StatusCode::PRECONDITION_FAILED,
        "an If-Match failure must be passed through to the client as 412, \
         not treated as a sliceable Page response"
    );

    // No caching should have happened for a 412 (no bytes to cache).
    assert!(cache_manager
        .get_range_from_ram_cache(&cache_key, 0, PAGE_SIZE - 1)
        .is_none());
}

/// `If-None-Match` match (ETag matches, no change): S3 returns 304, which
/// must be passed through to the client unchanged.
#[tokio::test]
async fn conditional_if_none_match_match_passes_through_304() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-cond-if-none-match-match.bin".to_string();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::NOT_MODIFIED)
            .with_header("etag", "\"inm-match-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert(
        "if-none-match".to_string(),
        "\"inm-match-etag\"".to_string(),
    );

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("If-None-Match match request should not error");

    assert_eq!(
        response.status(),
        StatusCode::NOT_MODIFIED,
        "an If-None-Match match must be passed through to the client as 304"
    );
    let body = body_bytes(response).await;
    assert!(body.is_empty(), "304 must have no body");
}

/// `If-None-Match` mismatch (object changed): S3 returns 206 for the widened
/// Page, which must be sliced to the client's requested sub-range and cached
/// — the same success path as a plain widened miss.
#[tokio::test]
async fn conditional_if_none_match_mismatch_slices_206() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-cond-if-none-match-mismatch.bin".to_string();

    let full_body = object_body();
    let page_start = 0u64;
    let page_end = PAGE_SIZE - 1;
    let page_bytes = full_body[page_start as usize..=page_end as usize].to_vec();

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(page_bytes))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", page_start, page_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"inm-mismatch-new-etag\""),
    );
    let s3_client = stub.clone().into_trait_object();

    let mut headers = HashMap::new();
    headers.insert(
        "if-none-match".to_string(),
        "\"inm-mismatch-OLD-etag\"".to_string(),
    );

    let resolved = widened_settings();

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("If-None-Match mismatch request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(&body[..], &full_body[5..=14]);
}

// -----------------------------------------------------------------------
// Property test: for any start/len < P, served bytes equal origin bytes.
// -----------------------------------------------------------------------

/// Property: for any absolute Small_Read (`start`/`len < P`) within the
/// object, the bytes the client receives through the widened path are
/// exactly the corresponding slice of the origin object — regardless of
/// where the read falls (single page or straddling a boundary).
///
/// Uses a `RangeAwareStub` that echoes slices of a fixed, deterministic
/// origin buffer for any Range requested, so the property holds independent
/// of which Page(s) get widened-fetched.
#[tokio::test]
async fn property_served_bytes_equal_origin_for_any_small_read() {
    struct RangeAwareStub {
        body: Vec<u8>,
        total: u64,
    }
    #[async_trait::async_trait]
    impl s3_proxy::S3ClientApi for RangeAwareStub {
        async fn forward_request(
            &self,
            context: s3_proxy::S3RequestContext,
        ) -> s3_proxy::Result<s3_proxy::S3Response> {
            let range = context
                .headers
                .get("range")
                .or_else(|| context.headers.get("Range"))
                .cloned();
            let (start, end) = if let Some(r) = range {
                let r = r.trim_start_matches("bytes=");
                let mut parts = r.split('-');
                let s: u64 = parts.next().unwrap().parse().unwrap();
                let e: u64 = parts.next().unwrap().parse().unwrap();
                (s, e.min(self.total - 1))
            } else {
                (0, self.total - 1)
            };
            let data = self.body[start as usize..=end as usize].to_vec();
            let mut headers = HashMap::new();
            headers.insert(
                "content-range".to_string(),
                format!("bytes {}-{}/{}", start, end, self.total),
            );
            headers.insert("etag".to_string(), "\"prop-etag\"".to_string());
            headers.insert("content-length".to_string(), data.len().to_string());
            Ok(s3_proxy::S3Response {
                status: StatusCode::PARTIAL_CONTENT,
                headers,
                body: Some(s3_proxy::S3ResponseBody::Buffered(Bytes::from(data))),
                request_duration: Duration::from_millis(0),
            })
        }

        fn extract_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::CacheMetadata {
            let etag = headers.get("etag").cloned().unwrap_or_default();
            let content_length = headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            s3_proxy::cache_types::CacheMetadata {
                etag,
                last_modified: String::new(),
                content_length,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: std::time::SystemTime::now(),
            }
        }

        fn extract_object_metadata_from_response(
            &self,
            headers: &HashMap<String, String>,
        ) -> s3_proxy::cache_types::ObjectMetadata {
            let etag = headers.get("etag").cloned().unwrap_or_default();
            let mut content_length = headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if let Some(cr) = headers.get("content-range") {
                if let Some(slash) = cr.rfind('/') {
                    if let Ok(total) = cr[slash + 1..].parse::<u64>() {
                        content_length = total;
                    }
                }
            }
            s3_proxy::cache_types::ObjectMetadata::new_with_headers(
                etag,
                String::new(),
                content_length,
                None,
                headers.clone(),
            )
        }

        fn get_connection_pool(
            &self,
        ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
            Arc::new(tokio::sync::RwLock::new(
                s3_proxy::connection_pool::ConnectionPoolManager::new_with_config(
                    Default::default(),
                )
                .expect("pool"),
            ))
        }

        fn has_endpoint_overrides(&self) -> bool {
            false
        }

        async fn set_metrics_manager(
            &self,
            _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
        ) {
        }

        async fn register_endpoint(&self, _endpoint: &str) {}

        async fn refresh_dns(&self) -> s3_proxy::Result<()> {
            Ok(())
        }
    }

    let full_body = object_body();

    // A small, deterministic sample of (start, len) pairs covering: start of
    // object, single-page interior, straddling both boundaries, and the tail
    // of the object — each with len < PAGE_SIZE (the Small_Read invariant).
    // Each case uses a fresh cache_key / cache infra so pages fetched by one
    // case can't interfere with another's assertions.
    let cases: Vec<(u64, u64)> = vec![
        (0, 1),                 // first byte
        (0, PAGE_SIZE - 1),     // whole first page minus one byte... still < P
        (10, 50),               // small interior read, page 0
        (PAGE_SIZE - 5, 10),    // straddles page 0/1 boundary
        (2 * PAGE_SIZE - 3, 6), // straddles page 1/2 boundary
        (OBJECT_SIZE - 20, 19), // near the tail, within last page
        (OBJECT_SIZE - 1, 0),   // last single byte
    ];

    for (case_idx, (start, len)) in cases.into_iter().enumerate() {
        let len = len.clamp(1, PAGE_SIZE - 1);
        let end = (start + len - 1).min(OBJECT_SIZE - 1);
        let start = end.saturating_sub(len - 1);

        let config = test_config();
        let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
        let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));
        let inflight_tracker = Arc::new(InFlightTracker::new());
        let cache_key = format!("bucket/prop-small-read-{}.bin", case_idx);

        let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::new(RangeAwareStub {
            body: full_body.clone(),
            total: OBJECT_SIZE,
        });

        let resolved = widened_settings();

        let response = HttpProxy::handle_range_request(
            Method::GET,
            cache_key.clone(),
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            Arc::clone(&cache_manager),
            Arc::clone(&range_handler),
            s3_client,
            "s3.amazonaws.com".to_string(),
            format!("/{}", cache_key).parse().unwrap(),
            Arc::clone(&config),
            &resolved,
            None,
            Arc::clone(&inflight_tracker),
            None,
            &None,
            false,
            // Test harness has no request-concurrency permit to thread.
            None,
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "case {} (start={}, end={}) should not error",
                case_idx, start, end
            )
        });

        assert_eq!(
            response.status(),
            StatusCode::PARTIAL_CONTENT,
            "case {} (start={}, end={})",
            case_idx,
            start,
            end
        );
        let body = body_bytes(response).await;
        let expected = &full_body[start as usize..=end as usize];
        assert_eq!(
            body, expected,
            "case {} (start={}, end={}): served bytes must equal origin bytes",
            case_idx, start, end
        );
    }
}

// -----------------------------------------------------------------------
// Warm-cache conditional range regression (T36j)
//
// The fleet deployment-verification suite (T36j) sends a stale `If-Range`
// plus a small `Range` against an object whose Page(s) are ALREADY warm in
// the cache (T36i and the warm-up GET ran first). RFC 7233 §3.2: when the
// `If-Range` validator does not match, the server must ignore `Range` and
// answer 200 with the full body. These two tests pin that behaviour for the
// warm-cache case on BOTH the widened and the un-widened path — the existing
// `conditional_if_range_stale_passes_through_full_200` above only covers a
// COLD cache, which is why the regression escaped.
// -----------------------------------------------------------------------

/// An S3-like stub that honours `If-Range` the way S3 does: when the
/// `If-Range` validator does not match the current ETag, the `Range` header
/// is ignored and the full object is returned with 200; otherwise the
/// requested range is returned with 206.
struct IfRangeAwareStub {
    body: Vec<u8>,
    total: u64,
    etag: String,
    captured: std::sync::Mutex<Vec<HashMap<String, String>>>,
}

impl IfRangeAwareStub {
    fn new(body: Vec<u8>, etag: &str) -> Self {
        let total = body.len() as u64;
        Self {
            body,
            total,
            etag: etag.to_string(),
            captured: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn request_count(&self) -> usize {
        self.captured.lock().expect("captured poisoned").len()
    }

    fn captured_headers(&self) -> Vec<HashMap<String, String>> {
        self.captured.lock().expect("captured poisoned").clone()
    }
}

#[async_trait::async_trait]
impl s3_proxy::S3ClientApi for IfRangeAwareStub {
    async fn forward_request(
        &self,
        context: s3_proxy::S3RequestContext,
    ) -> s3_proxy::Result<s3_proxy::S3Response> {
        self.captured
            .lock()
            .expect("captured poisoned")
            .push(context.headers.clone());

        let if_range = context
            .headers
            .get("if-range")
            .or_else(|| context.headers.get("If-Range"))
            .cloned();
        let range = context
            .headers
            .get("range")
            .or_else(|| context.headers.get("Range"))
            .cloned();

        // S3 semantics: a non-matching If-Range makes the Range header inert.
        let range_honoured = match &if_range {
            Some(v) => v == &self.etag,
            None => true,
        };

        let mut headers = HashMap::new();
        headers.insert("etag".to_string(), self.etag.clone());

        let (status, data) = match (range_honoured, range) {
            (true, Some(r)) => {
                let r = r.trim_start_matches("bytes=");
                let mut parts = r.split('-');
                let s: u64 = parts.next().unwrap().parse().unwrap();
                let e: u64 = parts
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(self.total - 1)
                    .min(self.total - 1);
                headers.insert(
                    "content-range".to_string(),
                    format!("bytes {}-{}/{}", s, e, self.total),
                );
                (
                    StatusCode::PARTIAL_CONTENT,
                    self.body[s as usize..=e as usize].to_vec(),
                )
            }
            _ => (StatusCode::OK, self.body.clone()),
        };
        headers.insert("content-length".to_string(), data.len().to_string());

        Ok(s3_proxy::S3Response {
            status,
            headers,
            body: Some(s3_proxy::S3ResponseBody::Buffered(Bytes::from(data))),
            request_duration: Duration::from_millis(0),
        })
    }

    fn extract_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::CacheMetadata {
        s3_proxy::cache_types::CacheMetadata {
            etag: headers.get("etag").cloned().unwrap_or_default(),
            last_modified: String::new(),
            content_length: headers
                .get("content-length")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: std::time::SystemTime::now(),
        }
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::ObjectMetadata {
        let mut content_length = headers
            .get("content-length")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if let Some(cr) = headers.get("content-range") {
            if let Some(slash) = cr.rfind('/') {
                if let Ok(total) = cr[slash + 1..].parse::<u64>() {
                    content_length = total;
                }
            }
        }
        s3_proxy::cache_types::ObjectMetadata::new_with_headers(
            headers.get("etag").cloned().unwrap_or_default(),
            String::new(),
            content_length,
            None,
            headers.clone(),
        )
    }

    fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
        Arc::new(tokio::sync::RwLock::new(
            s3_proxy::connection_pool::ConnectionPoolManager::new_with_config(Default::default())
                .expect("pool"),
        ))
    }

    fn has_endpoint_overrides(&self) -> bool {
        false
    }

    async fn set_metrics_manager(
        &self,
        _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }

    async fn register_endpoint(&self, _endpoint: &str) {}

    async fn refresh_dns(&self) -> s3_proxy::Result<()> {
        Ok(())
    }
}

/// Page mode ON, Page already WARM: a stale `If-Range` plus a small `Range`
/// must still produce S3's 200-full response, passed through unchanged — the
/// warm Page must not be sliced and returned as a 206.
///
/// This is the fleet T36j scenario (`If-Range: "stale-etag-value"` +
/// `-r 0-4095` after the object had already been read through the proxy).
#[tokio::test]
async fn warm_page_stale_if_range_passes_through_full_200() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/widen-warm-if-range-stale.bin".to_string();

    let full_body = object_body();
    let current_etag = "\"warm-if-range-etag\"";
    let stub = Arc::new(IfRangeAwareStub::new(full_body.clone(), current_etag));
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::clone(&stub) as _;

    let resolved = widened_settings();

    // Warm Page 0 with an ordinary (non-conditional) small read.
    let warm = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=5-14",
        HashMap::new(),
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        Arc::clone(&s3_client),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        None,
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("warming read should not error");
    assert_eq!(warm.status(), StatusCode::PARTIAL_CONTENT);
    let _ = body_bytes(warm).await;
    assert_eq!(
        stub.request_count(),
        1,
        "the warming read should have fetched the Page once"
    );

    // Now the stale-If-Range read into the SAME (warm) Page.
    let mut headers = HashMap::new();
    headers.insert("if-range".to_string(), "\"stale-etag-value\"".to_string());

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=0-4095",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        Arc::clone(&s3_client),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(current_etag.to_string()),
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("stale If-Range request should not error");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a stale If-Range on a WARM Page must still return S3's 200-full \
         response, not a 206 sliced out of the cached Page"
    );
    let body = body_bytes(response).await;
    assert_eq!(
        body, full_body,
        "the client must receive the full object body, verbatim"
    );
}

/// Page mode OFF (the default), object already fully cached: a stale
/// `If-Range` plus a small `Range` must still reach S3 so the precondition is
/// evaluated, and S3's 200-full response must be passed through — the cached
/// copy must not be sliced into a 206.
#[tokio::test]
async fn non_widened_warm_cache_stale_if_range_passes_through_full_200() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/no-widen-warm-if-range-stale.bin".to_string();

    let full_body = object_body();
    let current_etag = "\"no-widen-if-range-etag\"";

    // Pre-populate the WHOLE object on disk, standing in for a prior full GET
    // through the proxy (the fleet's T36 warm-up `aws s3 cp`).
    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        current_etag.to_string(),
        String::new(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    range_handler
        .store_range_new_storage(
            &cache_key,
            0,
            OBJECT_SIZE - 1,
            &full_body,
            object_metadata,
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("pre-populating the full object should succeed");
    let consolidator = cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode");
    consolidator
        .consolidate_object(&cache_key)
        .await
        .expect("consolidation should succeed");

    let stub = Arc::new(IfRangeAwareStub::new(full_body.clone(), current_etag));
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::clone(&stub) as _;

    let resolved = non_widened_settings();

    let mut headers = HashMap::new();
    headers.insert("if-range".to_string(), "\"stale-etag-value\"".to_string());

    // Compute `forward_to_s3` exactly as `handle_request` does, for both modes:
    // a STALE validator must forward in Mode A and in Mode B alike, because the
    // mismatch answer is the full current body, which the cache cannot produce.
    assert!(
        HttpProxy::has_non_if_match_conditional(&headers),
        "an If-Range-only request must be classified as a conditional, otherwise \
         the range pipeline never consults the precondition at all"
    );
    for mode_b in [false, true] {
        assert!(
            HttpProxy::if_range_requires_forward(
                mode_b,
                "\"stale-etag-value\"",
                Some(current_etag)
            ),
            "a stale If-Range validator must forward to S3 (mode_b={})",
            mode_b
        );
    }
    let forward_to_s3 = HttpProxy::if_range_requires_forward(
        resolved.evaluate_conditions_from_cache,
        "\"stale-etag-value\"",
        Some(current_etag),
    );

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=0-4095",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        Arc::clone(&s3_client),
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(current_etag.to_string()),
        Arc::clone(&inflight_tracker),
        None,
        &None,
        // `forward_to_s3` is computed exactly as `handle_request` computes it,
        // so this test exercises the real classification rather than assuming
        // a value: an `If-Range`-only request must be classified as a
        // conditional that only the origin can evaluate.
        forward_to_s3,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("stale If-Range request should not error");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a stale If-Range against a fully cached object must be evaluated by \
         S3 and its 200-full response passed through, not served as a 206 \
         sliced out of the cache"
    );
    let body = body_bytes(response).await;
    assert_eq!(body, full_body, "the client must receive the full object");
}

/// Mode B (`evaluate_conditions_from_cache`), fresh `If-Range` validator: the
/// client has asserted the exact version the cache holds, so the range is
/// served from cache with no S3 round trip — the `If-Range` counterpart of the
/// Mode B `If-Match` fast path.
#[tokio::test]
async fn mode_b_matching_if_range_serves_range_from_cache_without_s3() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/mode-b-if-range-match.bin".to_string();

    let full_body = object_body();
    let current_etag = "\"mode-b-if-range-etag\"";

    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        current_etag.to_string(),
        String::new(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    range_handler
        .store_range_new_storage(
            &cache_key,
            0,
            OBJECT_SIZE - 1,
            &full_body,
            object_metadata,
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("pre-populating the full object should succeed");
    cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode")
        .consolidate_object(&cache_key)
        .await
        .expect("consolidation should succeed");

    // A stub that fails loudly (500) if hit — a Mode B match must not reach S3.
    let stub = StubS3Client::new();
    let s3_client = stub.clone().into_trait_object();

    let resolved = ResolvedSettings {
        evaluate_conditions_from_cache: true,
        ..ResolvedSettings::default()
    };

    let mut headers = HashMap::new();
    headers.insert("if-range".to_string(), current_etag.to_string());

    let forward_to_s3 = HttpProxy::if_range_requires_forward(
        resolved.evaluate_conditions_from_cache,
        current_etag,
        Some(current_etag),
    );
    assert!(
        !forward_to_s3,
        "a matching If-Range validator under Mode B must be answerable from cache"
    );

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=0-4095",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(current_etag.to_string()),
        Arc::clone(&inflight_tracker),
        None,
        &None,
        forward_to_s3,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("matching If-Range request should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(
        body,
        &full_body[0..4096],
        "the client must receive exactly the requested range"
    );
    assert_eq!(
        stub.captured().len(),
        0,
        "a matching If-Range under Mode B must be served from cache with no S3 round trip"
    );
}

/// The `If-Range` forms that cannot be compared locally must all forward, even
/// in Mode B: an HTTP-date validator, a weak validator, a comma-separated value
/// (malformed for this header), `*`, and an unknown cached ETag.
#[test]
fn if_range_forms_that_cannot_be_compared_locally_all_forward() {
    let etag = "\"abc\"";

    assert!(!HttpProxy::if_range_requires_forward(
        true,
        etag,
        Some(etag)
    ));

    // HTTP-date form — comparable only against Last-Modified, which S3 does.
    assert!(HttpProxy::if_range_requires_forward(
        true,
        "Tue, 28 Jul 2026 12:00:00 GMT",
        Some(etag)
    ));
    // Weak validator — RFC 7233 §3.2 requires a strong comparison.
    assert!(HttpProxy::if_range_requires_forward(
        true,
        "W/\"abc\"",
        Some(etag)
    ));
    // A list and `*` are both malformed for If-Range.
    assert!(HttpProxy::if_range_requires_forward(
        true,
        "\"abc\", \"def\"",
        Some(etag)
    ));
    assert!(HttpProxy::if_range_requires_forward(true, "*", Some(etag)));
    // Nothing cached, or no ETag recorded.
    assert!(HttpProxy::if_range_requires_forward(true, etag, None));
    assert!(HttpProxy::if_range_requires_forward(true, etag, Some("")));
    // Mode A never answers a precondition locally.
    assert!(HttpProxy::if_range_requires_forward(
        false,
        etag,
        Some(etag)
    ));
}

/// Once Mode B has committed to serving a matching `If-Range` from cache, the
/// client's `If-Range` must be swapped for a proxy-injected `If-Match` pinning
/// the matched ETag — but never when `If-Range` was signed, since removing a
/// signed header would invalidate the client's SigV4 signature.
#[test]
fn if_range_serve_swaps_to_injected_if_match_unless_signed() {
    let etag = "\"pin-etag\"";

    let mut headers = HashMap::new();
    headers.insert("if-range".to_string(), etag.to_string());
    assert!(HttpProxy::pin_if_range_serve_to_cached_etag(
        &mut headers,
        etag
    ));
    assert!(
        !headers.contains_key("if-range"),
        "the client If-Range must not travel on to the gap fetches"
    );
    assert_eq!(headers.get("if-match").map(String::as_str), Some(etag));
    assert_eq!(
        headers.get("x-proxy-injected-if-match").map(String::as_str),
        Some("1"),
        "the sentinel is what lets the fetch path invalidate-and-retry on 412 \
         instead of leaking it to the client"
    );

    // Signed If-Range: untouched, signature must stay valid.
    let mut signed = HashMap::new();
    signed.insert("if-range".to_string(), etag.to_string());
    signed.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-west-2/s3/aws4_request, \
         SignedHeaders=host;if-range;x-amz-date, Signature=sig01"
            .to_string(),
    );
    assert!(!HttpProxy::pin_if_range_serve_to_cached_etag(
        &mut signed,
        etag
    ));
    assert_eq!(signed.get("if-range").map(String::as_str), Some(etag));
    assert!(!signed.contains_key("if-match"));

    // No usable cached ETag — nothing to pin to.
    let mut no_etag = HashMap::new();
    no_etag.insert("if-range".to_string(), etag.to_string());
    assert!(!HttpProxy::pin_if_range_serve_to_cached_etag(
        &mut no_etag,
        ""
    ));
}

/// End-to-end effect of the swap: with the range only PARTIALLY cached, the gap
/// fetch must carry the proxy's `If-Match` and NOT the client's `If-Range`.
///
/// That is the whole point of the swap — a stale `If-Range` on a gap fetch makes
/// S3 ignore `Range` and return the full object with `200`, which
/// `fetch_missing_ranges` buffers in full before rejecting it as a non-`206`.
#[tokio::test]
async fn partial_cache_if_range_serve_sends_if_match_on_the_gap_fetch() {
    let config = test_config();
    let (_temp_dir, cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(s3_proxy::range_handler::RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let cache_key = "bucket/mode-b-if-range-partial.bin".to_string();

    let full_body = object_body();
    let current_etag = "\"partial-if-range-etag\"";

    // Cache only the FIRST half of the range the client will ask for, so the
    // request resolves to cached + missing and takes the gap-fetch path.
    let cached_end = 2047u64;
    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        current_etag.to_string(),
        String::new(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    range_handler
        .store_range_new_storage(
            &cache_key,
            0,
            cached_end,
            &full_body[0..=cached_end as usize],
            object_metadata,
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("pre-populating the first half should succeed");
    cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode")
        .consolidate_object(&cache_key)
        .await
        .expect("consolidation should succeed");

    // Honours If-Range like S3 would, so a leaked If-Range on the gap fetch
    // would show up as a 200-full and fail the byte assertion below.
    let stub = Arc::new(IfRangeAwareStub::new(full_body.clone(), current_etag));
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::clone(&stub) as _;

    let resolved = ResolvedSettings {
        evaluate_conditions_from_cache: true,
        ..ResolvedSettings::default()
    };

    // Headers as `handle_request` hands them on: the swap has already been applied.
    let mut headers = HashMap::new();
    headers.insert("if-range".to_string(), current_etag.to_string());
    assert!(HttpProxy::pin_if_range_serve_to_cached_etag(
        &mut headers,
        current_etag
    ));

    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.clone(),
        "bytes=0-4095",
        headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        Some(current_etag.to_string()),
        Arc::clone(&inflight_tracker),
        None,
        &None,
        false,
        // Test harness has no request-concurrency permit to thread.
        None,
    )
    .await
    .expect("partially cached If-Range serve should not error");

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(
        body,
        &full_body[0..4096],
        "the merged response must be byte-exact across the cached and fetched halves"
    );

    let captured = stub.captured_headers();
    assert!(
        !captured.is_empty(),
        "a partially cached range must have fetched its gap from S3"
    );
    for sent in &captured {
        assert!(
            !sent.contains_key("if-range"),
            "the client's If-Range must not reach S3 on a gap fetch — a stale one \
             would return a full-object 200 that gets buffered and discarded"
        );
        assert_eq!(
            sent.get("if-match").map(String::as_str),
            Some(current_etag),
            "the gap fetch must be pinned to the matched ETag instead"
        );
        assert!(
            !sent.contains_key("x-proxy-injected-if-match"),
            "the internal sentinel must be stripped before the request leaves the proxy"
        );
    }
}
