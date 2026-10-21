//! The missing-`.bin` recovery refetch must never serve another version's bytes
//! under the cached object's headers.
//!
//! When a listed extent's `.bin` is gone, `get_cached_range_data` refetches the
//! extent from S3 and assembles the response under the cached `.meta`'s ETag and
//! length. If the object changed at the origin, the refetched bytes are the new
//! version and the response was mixed: new bytes, old ETag. The fix pins the
//! refetch with `If-Match: <cached ETag>` when the signature allows adding it, and
//! compares the response ETag either way. On a change it invalidates the key and
//! forwards the client's original request, so bytes, ETag and length all come from
//! one version.
//!
//! Every test seeds a `.meta` that lists an extent whose `.bin` is absent, which is
//! the only state that reaches the recovery arm, and asserts the recovery request
//! was actually issued (it carries the cached extent's `Range`) before judging the
//! response.

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
use s3_proxy::http_proxy::{HttpProxy, RecoveryRefetchVerdict};
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;

use common::{CapturedRequest, StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const FULL_OBJECT_CHECK_THRESHOLD: u64 = 512;
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 1023;
const V1_ETAG: &str = "\"etag-v1\"";
const V2_ETAG: &str = "\"etag-v2\"";
const V1_FILL: u8 = 0x11;
const V2_FILL: u8 = 0x22;

fn range_header() -> String {
    format!("bytes={}-{}", RANGE_START, RANGE_END)
}

fn body_of_fill(fill: u8) -> Vec<u8> {
    vec![fill; (RANGE_END - RANGE_START + 1) as usize]
}

fn partial(fill: u8, etag: &str) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(body_of_fill(fill)))
        .with_header(
            "content-range",
            format!("bytes {}-{}/{}", RANGE_START, RANGE_END, OBJECT_SIZE),
        )
        .with_header("content-length", (RANGE_END - RANGE_START + 1).to_string())
        .with_header("etag", etag)
}

fn test_config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = FULL_OBJECT_CHECK_THRESHOLD;
    Arc::new(config)
}

async fn make_infra(
    config: &Arc<Config>,
) -> (
    TempDir,
    Arc<CacheManager>,
    Arc<RangeHandler>,
    Arc<InFlightTracker>,
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
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    (
        temp_dir,
        cache_manager,
        range_handler,
        Arc::new(InFlightTracker::new()),
    )
}

/// Seed a stored-fresh v1 `.meta` listing one extent whose `.bin` is absent.
async fn seed_meta_with_missing_bin(cache_key: &str, cache_manager: &Arc<CacheManager>) {
    use s3_proxy::cache_types::{
        CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
    };
    use s3_proxy::compression::CompressionAlgorithm;

    let now = std::time::SystemTime::now();
    let len = RANGE_END - RANGE_START + 1;
    let bin_relative = format!(
        "bucket/00/000/{}_{}-{}.bin",
        cache_key.replace(['/', ':'], "_"),
        RANGE_START,
        RANGE_END
    );
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: V1_ETAG.to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length: OBJECT_SIZE,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: OBJECT_SIZE,
            parts: Vec::new(),
            response_headers: HashMap::new(),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: len,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        },
        ranges: vec![RangeSpec {
            start: RANGE_START,
            end: RANGE_END,
            file_path: bin_relative.clone(),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: len,
            uncompressed_size: len,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: Some(s3_proxy::cache_types::RangeFileIdentity {
                size: 1,
                mtime_ns: 1,
            }), // As a 3.0.0 writer records it (`RangeSpec::is_unverified_prefix`).
        }],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: None,
        head_last_accessed: None,
        head_access_count: 0,
        head_cached_at: None,
        last_validated_at: None,
    };
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().expect("meta parent")).expect("mkdir meta");
    std::fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata).expect("serialize"),
    )
    .expect("write .meta");
    let bin = cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&bin_relative);
    assert!(
        !bin.exists(),
        "PRECONDITION MISSING: the .bin must be absent to reach the recovery arm"
    );
    cache_manager.invalidate_metadata_cache(cache_key).await;
}

#[allow(clippy::too_many_arguments)]
async fn range_get(
    cache_key: &str,
    uri: &str,
    headers: HashMap<String, String>,
    cache_manager: &Arc<CacheManager>,
    range_handler: &Arc<RangeHandler>,
    inflight_tracker: &Arc<InFlightTracker>,
    config: &Arc<Config>,
    s3_client: Arc<dyn s3_proxy::s3_client::S3ClientApi + Send + Sync>,
) -> hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>> {
    let resolved = ResolvedSettings::default();
    tokio::time::timeout(
        Duration::from_secs(20),
        HttpProxy::handle_range_request(
            Method::GET,
            cache_key.to_string(),
            &range_header(),
            headers,
            Arc::clone(cache_manager),
            Arc::clone(range_handler),
            s3_client,
            "s3.us-west-2.amazonaws.com".to_string(),
            uri.parse().expect("uri"),
            Arc::clone(config),
            &resolved,
            Some(V1_ETAG.to_string()),
            Arc::clone(inflight_tracker),
            None,
            &None,
            false,
            None,
        ),
    )
    .await
    .expect("handle_range_request timed out")
    .expect("Infallible")
}

/// The client's own headers: an unsigned range GET.
fn client_headers() -> HashMap<String, String> {
    let mut h = HashMap::new();
    h.insert("range".to_string(), range_header());
    h
}

/// Status, ETag header and body of a response.
async fn parts(
    response: hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
) -> (StatusCode, Option<String>, Vec<u8>) {
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes()
        .to_vec();
    (status, etag, body)
}

/// The response is self-consistent: its bytes are the version its ETag names.
fn assert_one_version(etag: &Option<String>, body: &[u8]) {
    let expected_fill = match etag.as_deref() {
        Some(V1_ETAG) => V1_FILL,
        Some(V2_ETAG) => V2_FILL,
        other => panic!("response carries an unexpected ETag {:?}", other),
    };
    assert_eq!(
        body.len() as u64,
        RANGE_END - RANGE_START + 1,
        "MIXED VERSION: response ETag {:?} with a {}-byte body for a {}-byte range",
        etag,
        body.len(),
        RANGE_END - RANGE_START + 1
    );
    assert!(
        body.iter().all(|b| *b == expected_fill),
        "MIXED VERSION: response ETag {:?} but body bytes are {:?}... (v1=0x11, v2=0x22)",
        etag,
        &body[..body.len().min(4)]
    );
}

fn recovery_requests(captured: &[CapturedRequest]) -> Vec<&CapturedRequest> {
    captured
        .iter()
        .filter(|r| {
            r.headers.get("Range").or_else(|| r.headers.get("range")) == Some(&range_header())
        })
        .collect()
}

fn header<'a>(r: &'a CapturedRequest, name: &str) -> Option<&'a str> {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// After the response: the cache must not hold v2 bytes under the v1 `.meta`.
async fn assert_no_v2_bytes_under_v1(cache_key: &str, cache_manager: &Arc<CacheManager>) {
    tokio::time::sleep(Duration::from_millis(300)).await;
    let path = cache_manager.get_new_metadata_file_path(cache_key);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return;
    };
    let metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&content).expect("parse .meta");
    if metadata.object_metadata.etag != V1_ETAG {
        return;
    }
    for r in &metadata.ranges {
        let bin = cache_manager
            .get_cache_dir()
            .join("ranges")
            .join(&r.file_path);
        if let Ok(bytes) = std::fs::read(&bin) {
            assert!(
                !bytes.contains(&V2_FILL),
                "MIXED VERSION STORED: v1 .meta lists {}-{} holding v2 bytes",
                r.start,
                r.end
            );
        }
    }
}

/// Positive control: an unchanged object recovers from the refetch, which is
/// pinned to the cached ETag.
#[tokio::test]
async fn unchanged_object_recovers_with_pinned_if_match() {
    let config = test_config();
    let (_t, cm, rh, it) = make_infra(&config).await;
    let key = "bucket/recovery-unchanged.bin";
    seed_meta_with_missing_bin(key, &cm).await;
    let stub = StubS3Client::new().with_default(partial(V1_FILL, V1_ETAG));

    let response = range_get(
        key,
        &format!("/{}", key),
        client_headers(),
        &cm,
        &rh,
        &it,
        &config,
        stub.clone().into_trait_object(),
    )
    .await;
    let (status, etag, body) = parts(response).await;
    let captured = stub.captured();
    let recovery = recovery_requests(&captured);

    assert!(
        !recovery.is_empty(),
        "PRECONDITION MISSING: no recovery refetch: {:#?}",
        captured
    );
    assert_eq!(
        header(recovery[0], "if-match"),
        Some(V1_ETAG),
        "refetch must pin the cached ETag"
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_one_version(&etag, &body);
    assert_eq!(body, body_of_fill(V1_FILL));
}

/// The object changed: S3 answers the pinned refetch `412`. The client gets one
/// consistent version from the forwarded original request, and the key is
/// invalidated.
#[tokio::test]
async fn changed_object_412_on_pinned_refetch_serves_one_version() {
    let config = test_config();
    let (_t, cm, rh, it) = make_infra(&config).await;
    let key = "bucket/recovery-changed-412.bin";
    seed_meta_with_missing_bin(key, &cm).await;
    let stub = StubS3Client::new()
        .with_response_for_if_match(
            V1_ETAG,
            StubResponse::with_status(StatusCode::PRECONDITION_FAILED),
        )
        .with_default(partial(V2_FILL, V2_ETAG));

    let response = range_get(
        key,
        &format!("/{}", key),
        client_headers(),
        &cm,
        &rh,
        &it,
        &config,
        stub.clone().into_trait_object(),
    )
    .await;
    let (status, etag, body) = parts(response).await;
    let captured = stub.captured();
    let recovery = recovery_requests(&captured);

    assert!(
        !recovery.is_empty(),
        "PRECONDITION MISSING: no recovery refetch: {:#?}",
        captured
    );
    assert_eq!(
        header(recovery[0], "if-match"),
        Some(V1_ETAG),
        "refetch must pin the cached ETag"
    );
    assert_ne!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "the proxy's own 412 must not reach the client"
    );
    assert_one_version(&etag, &body);
    assert!(
        recovery.len() >= 2 && header(recovery[1], "if-match").is_none(),
        "the fallback must forward the client's original request, without the proxy's If-Match: {:#?}",
        captured
    );
    assert_eq!(etag.as_deref(), Some(V2_ETAG));
    assert_no_v2_bytes_under_v1(key, &cm).await;
}

/// The signature forbids adding `If-Match` (a presigned URL listing it), so the
/// refetch goes out unpinned and S3 returns the new version's bytes. The ETag
/// comparison must reject them and forward the original request.
#[tokio::test]
async fn signed_if_match_falls_back_on_etag_mismatch() {
    let config = test_config();
    let (_t, cm, rh, it) = make_infra(&config).await;
    let key = "bucket/recovery-changed-signed.bin";
    seed_meta_with_missing_bin(key, &cm).await;
    let stub = StubS3Client::new().with_default(partial(V2_FILL, V2_ETAG));
    let uri = format!(
        "/{}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host%3Bif-match&X-Amz-Signature=abc",
        key
    );

    let response = range_get(
        key,
        &uri,
        client_headers(),
        &cm,
        &rh,
        &it,
        &config,
        stub.clone().into_trait_object(),
    )
    .await;
    let (_status, etag, body) = parts(response).await;
    let captured = stub.captured();
    let recovery = recovery_requests(&captured);

    assert!(
        !recovery.is_empty(),
        "PRECONDITION MISSING: no recovery refetch: {:#?}",
        captured
    );
    assert!(
        captured.iter().all(|r| header(r, "if-match").is_none()),
        "If-Match is signed: the proxy must not add it: {:#?}",
        captured
    );
    assert_one_version(&etag, &body);
    assert!(
        recovery.len() >= 2,
        "the mismatch must be answered by forwarding the original range request: {:#?}",
        captured
    );
    assert_eq!(etag.as_deref(), Some(V2_ETAG));
    assert_no_v2_bytes_under_v1(key, &cm).await;
}

#[test]
fn pin_predicate_respects_the_signature() {
    let none = HashMap::new();
    assert!(HttpProxy::recovery_refetch_may_pin_if_match(
        &none, "/b/k", V1_ETAG
    ));
    assert!(!HttpProxy::recovery_refetch_may_pin_if_match(
        &none, "/b/k", ""
    ));

    let mut client_if_match = HashMap::new();
    client_if_match.insert("if-match".to_string(), V2_ETAG.to_string());
    assert!(!HttpProxy::recovery_refetch_may_pin_if_match(
        &client_if_match,
        "/b/k",
        V1_ETAG
    ));

    let mut signed_without = HashMap::new();
    signed_without.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=abc".to_string(),
    );
    assert!(HttpProxy::recovery_refetch_may_pin_if_match(
        &signed_without,
        "/b/k",
        V1_ETAG
    ));

    let mut signed_with = HashMap::new();
    signed_with.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKID/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;if-match;range;x-amz-date, Signature=abc".to_string(),
    );
    assert!(!HttpProxy::recovery_refetch_may_pin_if_match(
        &signed_with,
        "/b/k",
        V1_ETAG
    ));

    assert!(HttpProxy::recovery_refetch_may_pin_if_match(
        &none,
        "/b/k?X-Amz-SignedHeaders=host&X-Amz-Signature=abc",
        V1_ETAG
    ));
    assert!(!HttpProxy::recovery_refetch_may_pin_if_match(
        &none,
        "/b/k?X-Amz-SignedHeaders=host;if-match&X-Amz-Signature=abc",
        V1_ETAG
    ));
}

#[test]
fn classification_of_refetch_responses() {
    let with_etag = |e: &str| {
        let mut h = HashMap::new();
        h.insert("ETag".to_string(), e.to_string());
        h
    };
    let c = HttpProxy::classify_recovery_refetch;
    assert_eq!(
        c(
            StatusCode::PARTIAL_CONTENT,
            &with_etag(V1_ETAG),
            V1_ETAG,
            true,
            10,
            10
        ),
        RecoveryRefetchVerdict::SameVersion
    );
    assert_eq!(
        c(
            StatusCode::PARTIAL_CONTENT,
            &with_etag("etag-v1"),
            V1_ETAG,
            false,
            10,
            10
        ),
        RecoveryRefetchVerdict::SameVersion,
        "quotes are not significant"
    );
    assert_eq!(
        c(
            StatusCode::PARTIAL_CONTENT,
            &with_etag(V2_ETAG),
            V1_ETAG,
            false,
            10,
            10
        ),
        RecoveryRefetchVerdict::ChangedObject
    );
    assert_eq!(
        c(
            StatusCode::PRECONDITION_FAILED,
            &HashMap::new(),
            V1_ETAG,
            true,
            0,
            10
        ),
        RecoveryRefetchVerdict::ChangedObject
    );
    assert_eq!(
        c(
            StatusCode::PRECONDITION_FAILED,
            &HashMap::new(),
            V1_ETAG,
            false,
            0,
            10
        ),
        RecoveryRefetchVerdict::Unusable,
        "a 412 on the client's own If-Match is not evidence about the cached version"
    );
    assert_eq!(
        c(
            StatusCode::FORBIDDEN,
            &with_etag(V1_ETAG),
            V1_ETAG,
            true,
            10,
            10
        ),
        RecoveryRefetchVerdict::Unusable
    );
    assert_eq!(
        c(
            StatusCode::PARTIAL_CONTENT,
            &HashMap::new(),
            V1_ETAG,
            true,
            10,
            10
        ),
        RecoveryRefetchVerdict::Unusable
    );
    assert_eq!(
        c(
            StatusCode::PARTIAL_CONTENT,
            &with_etag(V1_ETAG),
            V1_ETAG,
            true,
            9,
            10
        ),
        RecoveryRefetchVerdict::Unusable
    );
}

/// The fallback forward carries the proxy-identification `Referer`.
///
/// `recovery_refetch_fallback` forwards the client's original request to S3 when the
/// recovery refetch returned another version. That forward goes through
/// `forward_get_head_to_s3_without_caching` like every other forward, and must add the
/// configured `Referer` the same way rather than dropping it. The request reaches the
/// fallback through the 412 arm, so the second captured request with the client's
/// `Range` is the fallback forward (the first is the pinned refetch).
#[tokio::test]
async fn changed_object_fallback_forward_carries_proxy_referer() {
    const REFERER: &str = "Hybrid Cache for Amazon S3/test (scn1-host)";
    let config = test_config();
    let (_t, cm, rh, it) = make_infra(&config).await;
    let key = "bucket/recovery-changed-referer.bin";
    seed_meta_with_missing_bin(key, &cm).await;
    let stub = StubS3Client::new()
        .with_response_for_if_match(
            V1_ETAG,
            StubResponse::with_status(StatusCode::PRECONDITION_FAILED),
        )
        .with_default(partial(V2_FILL, V2_ETAG));

    let resolved = ResolvedSettings::default();
    let uri = format!("/{}", key);
    let response = tokio::time::timeout(
        Duration::from_secs(20),
        HttpProxy::handle_range_request(
            Method::GET,
            key.to_string(),
            &range_header(),
            client_headers(),
            Arc::clone(&cm),
            Arc::clone(&rh),
            stub.clone().into_trait_object(),
            "s3.us-west-2.amazonaws.com".to_string(),
            uri.parse().expect("uri"),
            Arc::clone(&config),
            &resolved,
            Some(V1_ETAG.to_string()),
            Arc::clone(&it),
            None,
            &Some(REFERER.to_string()),
            false,
            None,
        ),
    )
    .await
    .expect("handle_range_request timed out")
    .expect("Infallible");
    let (_status, etag, body) = parts(response).await;
    let captured = stub.captured();
    let recovery = recovery_requests(&captured);

    assert!(
        recovery.len() >= 2,
        "PRECONDITION MISSING: expected the pinned refetch and the fallback forward: {:#?}",
        captured
    );
    assert_eq!(
        header(recovery[0], "if-match"),
        Some(V1_ETAG),
        "PRECONDITION: the first request is the pinned recovery refetch"
    );
    assert!(
        header(recovery[1], "if-match").is_none(),
        "PRECONDITION: the second request is the fallback forward of the original request"
    );
    assert_one_version(&etag, &body);
    assert_eq!(
        header(recovery[1], "referer"),
        Some(REFERER),
        "the recovery fallback forward must carry the configured proxy Referer"
    );
}

/// The recovery refetch carries our `Via` like every other egress.
///
/// The refetch builds its upstream request through the shared request-context
/// builder, which appends one `Via` entry unless `via` is signed.
#[tokio::test]
async fn recovery_refetch_carries_our_via() {
    let config = test_config();
    let (_t, cm, rh, it) = make_infra(&config).await;
    let key = "bucket/recovery-via.bin";
    seed_meta_with_missing_bin(key, &cm).await;
    let stub = StubS3Client::new().with_default(partial(V1_FILL, V1_ETAG));

    let response = range_get(
        key,
        &format!("/{}", key),
        client_headers(),
        &cm,
        &rh,
        &it,
        &config,
        stub.clone().into_trait_object(),
    )
    .await;
    let (_status, _etag, _body) = parts(response).await;
    let captured = stub.captured();
    let recovery = recovery_requests(&captured);

    assert!(
        !recovery.is_empty(),
        "PRECONDITION MISSING: no recovery refetch: {:#?}",
        captured
    );
    assert_eq!(
        header(recovery[0], "via"),
        Some(format!("1.1 {}", s3_proxy::signed_request_proxy::via_pseudonym()).as_str()),
        "the recovery refetch must carry exactly our Via entry"
    );
}
