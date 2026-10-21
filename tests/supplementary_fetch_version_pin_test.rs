//! One response never combines bytes of two object versions.
//!
//! When cached bytes are combined with bytes fetched from S3, the fetch must be
//! pinned to the cached version with `If-Match: <cached ETag>`. When S3 refuses
//! the pin the proxy invalidates the cached copy and serves the request from S3,
//! so the body, ETag and Content-Range of the response all come from one version.
//!
//! Two supplementary fetches were unpinned:
//!
//! 1. Page widening (`fetch_and_cache_page_missing_ranges`): a read spanning a
//!    cached Page and an uncached Page fetched the second Page with no
//!    `If-Match`, and the response headers were resolved from the `.meta`
//!    independently of the bytes.
//! 2. The hole fetch after a range revalidation `304` with incomplete coverage:
//!    `forward_range_request_to_s3` was called with the client's headers, so the
//!    hole was fetched with no `If-Match`.
//!
//! The stub below models S3's conditional semantics and can change the object
//! after a given number of requests, which is how both states are built: the
//! cached bytes are version 1 and the object is version 2 by the time the
//! supplementary fetch arrives.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;

const PAGE_SIZE: u64 = 1024;
const OBJECT_SIZE: u64 = 4096;
const V1_ETAG: &str = "\"pin-v1\"";
const V2_ETAG: &str = "\"pin-v2\"";
const V1_FILL: u8 = b'A';
const V2_FILL: u8 = b'B';
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

fn unquote(s: &str) -> &str {
    s.trim().trim_matches('"')
}

/// One recorded upstream request.
#[derive(Clone, Debug)]
struct Seen {
    range: Option<String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
}

/// An S3 model with two versions of one object. Requests numbered below
/// `flip_after` see version 1, the rest see version 2. `If-Match` that does not
/// name the current version answers `412`; `If-None-Match` naming it answers
/// `304`; otherwise a `206` of the requested bytes, filled with the version's
/// byte so a mixed body is visible by value.
struct VersionedS3 {
    flip_after: usize,
    object_size: u64,
    seen: Mutex<Vec<Seen>>,
    /// An origin that sends no `ETag`.
    etagless: bool,
}

impl VersionedS3 {
    fn new(flip_after: usize) -> Arc<Self> {
        Self::sized(flip_after, OBJECT_SIZE)
    }
    fn sized(flip_after: usize, object_size: u64) -> Arc<Self> {
        Arc::new(Self {
            flip_after,
            object_size,
            seen: Mutex::new(Vec::new()),
            etagless: false,
        })
    }
    /// [`Self::new`] for an origin that sends no `ETag`.
    fn etagless(flip_after: usize) -> Arc<Self> {
        Arc::new(Self {
            flip_after,
            object_size: OBJECT_SIZE,
            seen: Mutex::new(Vec::new()),
            etagless: true,
        })
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

fn header<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

#[async_trait::async_trait]
impl s3_proxy::S3ClientApi for VersionedS3 {
    async fn forward_request(
        &self,
        context: s3_proxy::S3RequestContext,
    ) -> s3_proxy::Result<s3_proxy::S3Response> {
        let range = header(&context.headers, "range").cloned();
        let if_match = header(&context.headers, "if-match").cloned();
        let if_none_match = header(&context.headers, "if-none-match").cloned();
        let index = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(Seen {
                range: range.clone(),
                if_match: if_match.clone(),
                if_none_match: if_none_match.clone(),
            });
            seen.len() - 1
        };
        let (etag, fill) = if index < self.flip_after {
            (V1_ETAG, V1_FILL)
        } else {
            (V2_ETAG, V2_FILL)
        };
        let mut headers = HashMap::new();
        if !self.etagless {
            headers.insert("etag".to_string(), etag.to_string());
        }
        headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());
        let bodyless = |status: StatusCode, headers: HashMap<String, String>| {
            Ok(s3_proxy::S3Response {
                status,
                headers,
                body: None,
                request_duration: Duration::from_millis(0),
            })
        };
        if let Some(im) = &if_match {
            if unquote(im) != unquote(etag) {
                return bodyless(StatusCode::PRECONDITION_FAILED, HashMap::new());
            }
        }
        if let Some(inm) = &if_none_match {
            if unquote(inm) == unquote(etag) {
                return bodyless(StatusCode::NOT_MODIFIED, headers);
            }
        }
        let (status, start, end) = match &range {
            Some(r) => {
                let r = r.trim_start_matches("bytes=");
                let mut parts = r.split('-');
                let s: u64 = parts.next().unwrap().parse().unwrap();
                let e: u64 = parts.next().unwrap().parse().unwrap();
                (StatusCode::PARTIAL_CONTENT, s, e.min(self.object_size - 1))
            }
            None => (StatusCode::OK, 0, self.object_size - 1),
        };
        let data = vec![fill; (end - start + 1) as usize];
        if status == StatusCode::PARTIAL_CONTENT {
            headers.insert(
                "content-range".to_string(),
                format!("bytes {}-{}/{}", start, end, self.object_size),
            );
        }
        headers.insert("content-length".to_string(), data.len().to_string());
        if context.method == Method::HEAD {
            return bodyless(status, headers);
        }
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
            last_modified: headers.get("last-modified").cloned().unwrap_or_default(),
            content_length: self.object_size,
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
        s3_proxy::cache_types::ObjectMetadata::new_with_headers(
            headers.get("etag").cloned().unwrap_or_default(),
            headers.get("last-modified").cloned().unwrap_or_default(),
            self.object_size,
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

fn test_config() -> Arc<Config> {
    let mut config = Config::default();
    // One request takes one path: no coalescing layer, no RAM tier in front of
    // the disk lookup.
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    // Skip the range path's early full-object shortcut so the range lookup and
    // the revalidation branch run.
    config.cache.full_object_check_threshold = 512;
    Arc::new(config)
}

async fn make_infra(config: &Arc<Config>) -> (TempDir, Arc<CacheManager>, Arc<RangeHandler>) {
    let temp_dir = TempDir::new().expect("tempdir");
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
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
    (temp_dir, cache_manager, range_handler)
}

/// Lay down one version-1 extent (`.bin` of raw bytes plus a stored-fresh
/// `.meta` carrying `V1_ETAG`), written directly so the `.meta` exists without
/// waiting on journal consolidation.
async fn seed_v1_extent(cache_key: &str, cache_manager: &Arc<CacheManager>, start: u64, end: u64) {
    seed_v1_extent_of(cache_key, cache_manager, start, end, OBJECT_SIZE).await
}
/// [`seed_v1_extent`] for an object of `object_size` bytes.
async fn seed_v1_extent_of(
    cache_key: &str,
    cache_manager: &Arc<CacheManager>,
    start: u64,
    end: u64,
    object_size: u64,
) {
    use s3_proxy::cache_types::{
        CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
    };
    use s3_proxy::compression::CompressionAlgorithm;

    let now = std::time::SystemTime::now();
    let body = vec![V1_FILL; (end - start + 1) as usize];
    let bin_relative = format!(
        "bucket/00/000/{}_{}-{}.bin",
        cache_key.replace(['/', ':'], "_"),
        start,
        end
    );
    let bin_path = cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&bin_relative);
    std::fs::create_dir_all(bin_path.parent().unwrap()).unwrap();
    std::fs::write(&bin_path, &body).unwrap();
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: V1_ETAG.to_string(),
            last_modified: LAST_MODIFIED.to_string(),
            content_length: object_size,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: object_size,
            parts: Vec::new(),
            response_headers: HashMap::new(),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: body.len() as u64,
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
            start,
            end,
            file_path: bin_relative,
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: body.len() as u64,
            uncompressed_size: body.len() as u64,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            // As a 3.0.0 writer records it (`RangeSpec::is_unverified_prefix`).
            file_identity: s3_proxy::cache_types::RangeFileIdentity::of(
                &std::fs::metadata(&bin_path).expect("fixture: stat the planted .bin"),
            ),
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
    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata).unwrap(),
    )
    .unwrap();
    cache_manager.invalidate_metadata_cache(cache_key).await;
}

/// The ETag the `.meta` on disk carries now, `None` when it is gone.
fn stored_etag(cache_manager: &Arc<CacheManager>, cache_key: &str) -> Option<String> {
    let content =
        std::fs::read_to_string(cache_manager.get_new_metadata_file_path(cache_key)).ok()?;
    let metadata: s3_proxy::cache_types::NewCacheMetadata = serde_json::from_str(&content).ok()?;
    Some(metadata.object_metadata.etag)
}

/// The ranges the `.meta` on disk lists now (empty when it is gone).
fn stored_ranges(cache_manager: &Arc<CacheManager>, cache_key: &str) -> Vec<(u64, u64)> {
    let Ok(content) = std::fs::read_to_string(cache_manager.get_new_metadata_file_path(cache_key))
    else {
        return Vec::new();
    };
    let Ok(metadata) = serde_json::from_str::<s3_proxy::cache_types::NewCacheMetadata>(&content)
    else {
        return Vec::new();
    };
    metadata.ranges.iter().map(|r| (r.start, r.end)).collect()
}

struct Served {
    status: StatusCode,
    etag: Option<String>,
    content_range: Option<String>,
    body: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
async fn range_get(
    cache_key: &str,
    range: &str,
    client_headers: HashMap<String, String>,
    cache_manager: &Arc<CacheManager>,
    range_handler: &Arc<RangeHandler>,
    config: &Arc<Config>,
    resolved: &ResolvedSettings,
    current_etag: Option<String>,
    s3: Arc<VersionedS3>,
) -> Served {
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3;
    let response = HttpProxy::handle_range_request(
        Method::GET,
        cache_key.to_string(),
        range,
        client_headers,
        Arc::clone(cache_manager),
        Arc::clone(range_handler),
        s3_client,
        "s3.us-west-2.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().unwrap(),
        Arc::clone(config),
        resolved,
        current_etag,
        Arc::new(InFlightTracker::new()),
        None,
        &None,
        false,
        None,
    )
    .await
    .expect("handle_range_request returns Infallible");
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string());
    let content_range = response
        .headers()
        .get("content-range")
        .map(|v| v.to_str().unwrap().to_string());
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec();
    Served {
        status,
        etag,
        content_range,
        body,
    }
}

/// The version every byte of `body` came from, or a description of the mix.
fn body_version(body: &[u8]) -> std::result::Result<&'static str, String> {
    let a = body.iter().filter(|&&b| b == V1_FILL).count();
    let b = body.iter().filter(|&&b| b == V2_FILL).count();
    match (a, b) {
        (n, 0) if n == body.len() && n > 0 => Ok(V1_ETAG),
        (0, n) if n == body.len() && n > 0 => Ok(V2_ETAG),
        _ => Err(format!(
            "{} version-1 bytes and {} version-2 bytes in a {}-byte body",
            a,
            b,
            body.len()
        )),
    }
}

/// Assert the response is one version: every byte from one version and the
/// ETag header naming that same version.
fn assert_one_version(served: &Served, what: &str) -> &'static str {
    let version = match body_version(&served.body) {
        Ok(v) => v,
        Err(mix) => panic!(
            "MIXED VERSIONS ({}): {} under etag={:?} content-range={:?} status={}",
            what, mix, served.etag, served.content_range, served.status
        ),
    };
    let etag = served.etag.as_deref().unwrap_or("");
    assert_eq!(
        unquote(etag),
        unquote(version),
        "HEADERS FROM ANOTHER VERSION ({}): body is {} but etag={:?}, content-range={:?}",
        what,
        version,
        served.etag,
        served.content_range
    );
    version
}

fn widened() -> ResolvedSettings {
    ResolvedSettings {
        page_widening: true,
        page_size: PAGE_SIZE,
        ..ResolvedSettings::default()
    }
}

/// Defect 1, straddling read. Page 0 is cached as version 1; the object is
/// version 2 in S3. A read spanning pages 0 and 1 must not combine page 0's
/// cached version-1 bytes with page 1's version-2 bytes.
#[tokio::test]
async fn widened_straddle_never_combines_cached_page_with_another_version() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-widened-straddle.bin";
    seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
    let s3 = VersionedS3::new(0); // every request sees version 2

    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;

    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(served.body.len(), 12);
    let version = assert_one_version(&served, "widened straddle");
    assert_eq!(
        version, V2_ETAG,
        "S3 holds only version 2, so the response is version 2"
    );
    // The page-1 fetch was pinned to the cached version.
    let seen = s3.seen();
    let page1 = seen
        .iter()
        .find(|r| r.range.as_deref() == Some(&format!("bytes={}-{}", PAGE_SIZE, 2 * PAGE_SIZE - 1)))
        .unwrap_or_else(|| panic!("PRECONDITION MISSING: no page-1 fetch: {:?}", seen));
    assert_eq!(
        page1.if_match.as_deref(),
        Some(V1_ETAG),
        "the supplementary page fetch must carry If-Match with the cached ETag: {:?}",
        seen
    );
    // The refused pin invalidated the cached version-1 copy.
    assert_ne!(
        stored_etag(&cache_manager, key).as_deref(),
        Some(V1_ETAG),
        "the version-1 copy must be invalidated once S3 refuses the pin; ranges={:?}",
        stored_ranges(&cache_manager, key)
    );
}

/// Defect 1, read wholly inside an uncached page of a key whose `.meta` names
/// version 1. The bytes come from S3 (version 2), so the headers must too.
#[tokio::test]
async fn widened_read_in_uncached_page_never_serves_bytes_under_another_versions_headers() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-widened-page1.bin";
    seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
    let s3 = VersionedS3::new(0);

    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE + 100, PAGE_SIZE + 109),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;

    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(served.body.len(), 10);
    assert_one_version(&served, "widened read inside page 1");
    // And a second read is no different (the fill must not leave a state that
    // serves version-2 bytes under version-1 headers on every read).
    let again = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE + 100, PAGE_SIZE + 109),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;
    // The first read invalidated the key, so this one is a cold fill whose `.meta`
    // may not be visible yet: an absent ETag is allowed, a wrong one is not.
    let version = body_version(&again.body)
        .unwrap_or_else(|mix| panic!("MIXED VERSIONS (second read): {}", mix));
    if let Some(etag) = again.etag.as_deref() {
        assert_eq!(
            unquote(etag),
            unquote(version),
            "HEADERS FROM ANOTHER VERSION (second read): content-range={:?}",
            again.content_range
        );
    }
}

/// Control for defect 1: no version change. Page 0 is served from cache and
/// page 1 is fetched pinned; S3 accepts the pin, so the response is version 1
/// with exactly one upstream request, for page 1's bytes only.
#[tokio::test]
async fn widened_straddle_without_version_change_fetches_only_the_missing_page_pinned() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-widened-control.bin";
    seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
    let s3 = VersionedS3::new(usize::MAX); // never changes

    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;

    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(assert_one_version(&served, "control"), V1_ETAG);
    let seen = s3.seen();
    assert_eq!(seen.len(), 1, "one fetch, for page 1 only: {:?}", seen);
    assert_eq!(
        seen[0].range.as_deref(),
        Some(format!("bytes={}-{}", PAGE_SIZE, 2 * PAGE_SIZE - 1).as_str())
    );
    assert_eq!(seen[0].if_match.as_deref(), Some(V1_ETAG));
    assert_eq!(stored_etag(&cache_manager, key).as_deref(), Some(V1_ETAG));
}

/// Control for defect 1: a `412` on the CLIENT's own `If-Match` is the client's
/// answer, not a version change. It passes through and the cache is untouched.
#[tokio::test]
async fn widened_client_if_match_412_is_not_taken_for_a_version_change() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-widened-client-412.bin";
    seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
    let s3 = VersionedS3::new(usize::MAX);
    let mut client = HashMap::new();
    client.insert("if-match".to_string(), "\"someone-elses\"".to_string());

    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        client,
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;

    assert_eq!(served.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        stored_ranges(&cache_manager, key),
        vec![(0, PAGE_SIZE - 1)],
        "a client's own 412 must not invalidate the cached copy"
    );
    assert_eq!(stored_etag(&cache_manager, key).as_deref(), Some(V1_ETAG));
}

/// Defect 2. A non-widened range read whose cached extent covers only part of
/// the request, live-expired so it revalidates: S3 answers the revalidation
/// `304` (version 1), then the object changes before the hole fetch. The hole
/// must not be filled from version 2 under version 1's headers.
#[tokio::test]
async fn hole_fetch_after_revalidation_304_never_combines_two_versions() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-hole-after-304.bin";
    seed_v1_extent(key, &cache_manager, 0, 511).await;
    let s3 = VersionedS3::new(1); // the revalidation sees v1, everything after sees v2
    let resolved = ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    };

    let served = range_get(
        key,
        "bytes=0-1023",
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &resolved,
        stored_etag(&cache_manager, key),
        Arc::clone(&s3),
    )
    .await;

    let seen = s3.seen();
    assert!(
        seen.first().and_then(|r| r.if_none_match.as_deref()) == Some(V1_ETAG),
        "PRECONDITION MISSING: the first request must be the revalidation: {:?}",
        seen
    );
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(served.body.len(), 1024);
    let version = assert_one_version(&served, "hole fetch after 304");
    assert_eq!(version, V2_ETAG, "{:?}", seen);
    let hole = seen
        .iter()
        .find(|r| r.range.as_deref() == Some("bytes=512-1023"))
        .unwrap_or_else(|| panic!("PRECONDITION MISSING: no hole fetch: {:?}", seen));
    assert_eq!(
        hole.if_match.as_deref(),
        Some(V1_ETAG),
        "the hole fetch must carry If-Match with the cached ETag: {:?}",
        seen
    );
}

/// Control for defect 2: no version change after the `304`. The hole is fetched
/// pinned, S3 accepts it, and the response is version 1 from the cached extent
/// plus one hole fetch.
#[tokio::test]
async fn hole_fetch_after_revalidation_304_without_version_change_is_pinned_and_merged() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-hole-control.bin";
    seed_v1_extent(key, &cache_manager, 0, 511).await;
    let s3 = VersionedS3::new(usize::MAX);
    let resolved = ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    };

    let served = range_get(
        key,
        "bytes=0-1023",
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &resolved,
        stored_etag(&cache_manager, key),
        Arc::clone(&s3),
    )
    .await;

    let seen = s3.seen();
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(assert_one_version(&served, "hole control"), V1_ETAG);
    assert_eq!(
        seen.len(),
        2,
        "revalidation plus one hole fetch: {:?}",
        seen
    );
    assert_eq!(seen[1].range.as_deref(), Some("bytes=512-1023"));
    assert_eq!(seen[1].if_match.as_deref(), Some(V1_ETAG));
}

/// Store `fill` bytes of version `etag` for `start..=end` through the production
/// range-fill writer, which publishes the `.bin` and appends a journal `Add` only.
/// No consolidator runs in these tests, so the extent stays journal-pending: listed
/// by no `.meta`, visible only through the journal fallback.
async fn store_journal_pending(
    cache_key: &str,
    cache_manager: &Arc<CacheManager>,
    start: u64,
    end: u64,
    fill: u8,
    etag: &str,
) {
    let mut metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        etag.to_string(),
        LAST_MODIFIED.to_string(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    metadata.upload_state = s3_proxy::cache_types::UploadState::Complete;
    metadata.cumulative_size = end - start + 1;
    cache_manager
        .create_configured_disk_cache_manager()
        .store_range(
            cache_key,
            start,
            end,
            &vec![fill; (end - start + 1) as usize],
            metadata,
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("journal-only range fill");
    // Read through the ETag-carrying scan: it still lists an extent whose pending `Add`s
    // disagree on the version, which the plain lookup drops.
    let pending = cache_manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges_with_etags(cache_key, start, end)
        .await
        .expect("journal scan");
    assert!(
        pending
            .iter()
            .any(|(r, _)| r.start == start && r.end == end),
        "PRECONDITION MISSING: {}-{} is not journal-pending: {:?}",
        start,
        end,
        pending
    );
    assert!(
        !stored_ranges(cache_manager, cache_key).contains(&(start, end)),
        "PRECONDITION MISSING: {}-{} is already in the .meta",
        start,
        end
    );
}
/// The `.meta`-names-v1, page-1-pending-as-v2 state cannot be built by the range-fill
/// writer: publishing a fill reads the key's `.meta` under the slot lock and drops a fill
/// whose ETag differs from it, so nothing is journalled. This pins that guard.
#[tokio::test]
async fn range_fill_of_another_version_under_a_meta_is_never_journal_pending() {
    let config = test_config();
    let (_t, cache_manager, _range_handler) = make_infra(&config).await;
    let key = "bucket/pin-journal-guard.bin";
    seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
    let mut metadata = s3_proxy::cache_types::ObjectMetadata::new_with_headers(
        V2_ETAG.to_string(),
        LAST_MODIFIED.to_string(),
        OBJECT_SIZE,
        None,
        HashMap::new(),
    );
    metadata.upload_state = s3_proxy::cache_types::UploadState::Complete;
    metadata.cumulative_size = PAGE_SIZE;
    cache_manager
        .create_configured_disk_cache_manager()
        .store_range(
            key,
            PAGE_SIZE,
            2 * PAGE_SIZE - 1,
            &vec![V2_FILL; PAGE_SIZE as usize],
            metadata,
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("range fill");
    let pending = cache_manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges(key, PAGE_SIZE, 2 * PAGE_SIZE - 1)
        .await
        .expect("journal scan");
    assert!(
        pending.is_empty(),
        "a fill of another version than the .meta must journal nothing: {:?}",
        pending
    );
    assert_eq!(stored_ranges(&cache_manager, key), vec![(0, PAGE_SIZE - 1)]);
    assert_eq!(stored_etag(&cache_manager, key).as_deref(), Some(V1_ETAG));
}
/// The reachable journal-pending state: no `.meta` yet; page 0 was filled as version
/// 1, the object was overwritten directly in S3, and page 1 was filled as version 2
/// (on any proxy). Both extents are journal-pending. A widened read spanning both
/// pages must not serve page 0's version-1 bytes beside page 1's version-2 bytes.
async fn journal_pending_straddle(current_etag: Option<&str>, key: &str) -> (Served, Vec<Seen>) {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    store_journal_pending(key, &cache_manager, 0, PAGE_SIZE - 1, V1_FILL, V1_ETAG).await;
    store_journal_pending(
        key,
        &cache_manager,
        PAGE_SIZE,
        2 * PAGE_SIZE - 1,
        V2_FILL,
        V2_ETAG,
    )
    .await;
    let s3 = VersionedS3::new(0); // S3 holds only version 2
    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        current_etag.map(str::to_string),
        Arc::clone(&s3),
    )
    .await;
    (served, s3.seen())
}
/// One version in the body, and the ETag header (when there is one) names it. With
/// no `.meta` a cache-served widened response may carry no ETag at all.
fn assert_one_version_etag_optional(served: &Served, what: &str) -> &'static str {
    let version = body_version(&served.body).unwrap_or_else(|mix| {
        panic!(
            "MIXED VERSIONS ({}): {} under etag={:?} content-range={:?} status={}",
            what, mix, served.etag, served.content_range, served.status
        )
    });
    if let Some(etag) = served.etag.as_deref() {
        assert_eq!(
            unquote(etag),
            unquote(version),
            "HEADERS FROM ANOTHER VERSION ({}): body is {} but etag={:?}",
            what,
            version,
            served.etag
        );
    }
    version
}
/// The caller knows a version (`current_etag == Some(V1)`).
#[tokio::test]
async fn widened_straddle_never_serves_a_journal_pending_page_of_another_version_known_etag() {
    let (served, seen) =
        journal_pending_straddle(Some(V1_ETAG), "bucket/pin-journal-known.bin").await;
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(served.body.len(), 12);
    assert_one_version_etag_optional(&served, "journal-pending pages, current_etag known");
}
/// The caller does not know the version (`current_etag == None`).
#[tokio::test]
async fn widened_straddle_never_serves_a_journal_pending_page_of_another_version_unknown_etag() {
    let (served, seen) = journal_pending_straddle(None, "bucket/pin-journal-unknown.bin").await;
    assert_eq!(served.status, StatusCode::PARTIAL_CONTENT, "{:?}", seen);
    assert_eq!(served.body.len(), 12);
    assert_one_version_etag_optional(&served, "journal-pending pages, current_etag unknown");
}
/// Control: journal-pending pages of ONE version are still a cache hit,
/// with or without a known ETag, with or without a `.meta` naming that version: one
/// version, no S3 request.
#[tokio::test]
async fn widened_straddle_over_a_journal_pending_page_of_the_same_version_is_a_hit() {
    for (current_etag, with_meta, key) in [
        (
            Some(V1_ETAG),
            true,
            "bucket/pin-journal-same-known-meta.bin",
        ),
        (None, true, "bucket/pin-journal-same-unknown-meta.bin"),
        (Some(V1_ETAG), false, "bucket/pin-journal-same-known.bin"),
        (None, false, "bucket/pin-journal-same-unknown.bin"),
    ] {
        let config = test_config();
        let (_t, cache_manager, range_handler) = make_infra(&config).await;
        if with_meta {
            seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
        } else {
            store_journal_pending(key, &cache_manager, 0, PAGE_SIZE - 1, V1_FILL, V1_ETAG).await;
        }
        store_journal_pending(
            key,
            &cache_manager,
            PAGE_SIZE,
            2 * PAGE_SIZE - 1,
            V1_FILL,
            V1_ETAG,
        )
        .await;
        let s3 = VersionedS3::new(usize::MAX); // never changes
        let served = range_get(
            key,
            &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
            HashMap::new(),
            &cache_manager,
            &range_handler,
            &config,
            &widened(),
            current_etag.map(str::to_string),
            Arc::clone(&s3),
        )
        .await;
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            assert_one_version_etag_optional(&served, "journal-pending pages, one version"),
            V1_ETAG
        );
        assert!(
            s3.seen().is_empty(),
            "journal-pending pages of one version are a hit (current_etag={:?}, meta={}): {:?}",
            current_etag,
            with_meta,
            s3.seen()
        );
    }
}
/// Control: pages listed in a consolidated `.meta` are served from
/// cache with no S3 request.
#[tokio::test]
async fn widened_straddle_over_consolidated_pages_makes_no_s3_request() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-consolidated.bin";
    seed_v1_extent(key, &cache_manager, 0, 2 * PAGE_SIZE - 1).await;
    let s3 = VersionedS3::new(usize::MAX);
    for current_etag in [Some(V1_ETAG.to_string()), None] {
        let served = range_get(
            key,
            &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
            HashMap::new(),
            &cache_manager,
            &range_handler,
            &config,
            &widened(),
            current_etag,
            Arc::clone(&s3),
        )
        .await;
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(assert_one_version(&served, "consolidated pages"), V1_ETAG);
    }
    assert!(s3.seen().is_empty(), "{:?}", s3.seen());
}
/// An object over the buffered-operation bound (8 MiB), so a partial-cache merge
/// of a range this large is refused before any hole is fetched.
const LARGE_OBJECT: u64 = 9 * 1024 * 1024;
/// The bound arm after a revalidation `304`: a HEAD whose range is over the bound,
/// on a key whose cached extent covers only part of it, with the object changing
/// between the `304` and the forward. Returns the response and what S3 saw.
async fn bound_arm_after_304(
    client_headers: HashMap<String, String>,
    key: &str,
) -> (Served, Vec<Seen>) {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    seed_v1_extent_of(key, &cache_manager, 0, 511, LARGE_OBJECT).await;
    let s3 = VersionedS3::sized(1, LARGE_OBJECT); // the revalidation sees v1, then v2
    let resolved = ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    };
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = s3.clone();
    let response = HttpProxy::handle_range_request(
        Method::HEAD,
        key.to_string(),
        &format!("bytes=0-{}", LARGE_OBJECT - 1),
        client_headers,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        "s3.us-west-2.amazonaws.com".to_string(),
        format!("/{}", key).parse().unwrap(),
        Arc::clone(&config),
        &resolved,
        stored_etag(&cache_manager, key),
        Arc::new(InFlightTracker::new()),
        None,
        &None,
        false,
        None,
    )
    .await
    .expect("handle_range_request returns Infallible");
    let served = Served {
        status: response.status(),
        etag: response
            .headers()
            .get("etag")
            .map(|v| v.to_str().unwrap().to_string()),
        content_range: response
            .headers()
            .get("content-range")
            .map(|v| v.to_str().unwrap().to_string()),
        body: Vec::new(),
    };
    (served, s3.seen())
}
/// The client sent no precondition, so S3's answer to the forward
/// must not be a `412` to the proxy's own `If-Match`.
#[tokio::test]
async fn bound_arm_never_forwards_the_proxys_own_if_match_to_a_client_that_sent_none() {
    let (served, seen) = bound_arm_after_304(HashMap::new(), "bucket/pin-bound-arm.bin").await;
    assert!(
        seen.first().and_then(|r| r.if_none_match.as_deref()) == Some(V1_ETAG),
        "PRECONDITION MISSING: the first request must be the revalidation: {:?}",
        seen
    );
    assert!(
        seen.len() >= 2,
        "PRECONDITION MISSING: no forward after the revalidation: {:?}",
        seen
    );
    assert_ne!(
        served.status,
        StatusCode::PRECONDITION_FAILED,
        "a client that sent no precondition got a 412: {:?}",
        seen
    );
    assert_eq!(
        seen.last().unwrap().if_match,
        None,
        "the forward carried the proxy's own If-Match: {:?}",
        seen
    );
}
/// Control: the client's own `If-Match` is still forwarded on the bound
/// arm, and S3's `412` to it passes through.
#[tokio::test]
async fn bound_arm_still_forwards_the_clients_own_if_match() {
    let mut client = HashMap::new();
    client.insert("if-match".to_string(), V1_ETAG.to_string());
    let (served, seen) = bound_arm_after_304(client, "bucket/pin-bound-arm-client.bin").await;
    assert_eq!(
        seen.last().unwrap().if_match.as_deref(),
        Some(V1_ETAG),
        "{:?}",
        seen
    );
    assert_eq!(served.status, StatusCode::PRECONDITION_FAILED, "{:?}", seen);
}
/// The shared-`.bin` state. No `.meta`. Two proxies miss page 0 before either
/// journal line exists (in-flight coalescing is per process), one fetching version 1
/// and the other version 2 after a direct-to-S3 overwrite. The `.bin` path is
/// `(key, start, end)` only, so the second publish renames over the first: the file
/// holds version 2 and the journals hold an `Add` of each version. Page 1 is pending
/// as version 1. Modelled with two `store_range` calls on one extent.
async fn shared_bin_state(key: &str, cache_manager: &Arc<CacheManager>) {
    store_journal_pending(key, cache_manager, 0, PAGE_SIZE - 1, V1_FILL, V1_ETAG).await;
    store_journal_pending(key, cache_manager, 0, PAGE_SIZE - 1, V2_FILL, V2_ETAG).await;
    store_journal_pending(
        key,
        cache_manager,
        PAGE_SIZE,
        2 * PAGE_SIZE - 1,
        V1_FILL,
        V1_ETAG,
    )
    .await;
    let adds_for_page_0 = cache_manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges_with_etags(key, 0, 2 * PAGE_SIZE - 1)
        .await
        .expect("journal scan")
        .into_iter()
        .filter(|(r, _)| r.start == 0 && r.end == PAGE_SIZE - 1)
        .count();
    assert_eq!(
        adds_for_page_0, 2,
        "PRECONDITION MISSING: page 0 must carry two pending Adds"
    );
    assert_eq!(
        stored_etag(cache_manager, key),
        None,
        "PRECONDITION MISSING: no .meta may exist"
    );
}
///A widened straddle over the shared-`.bin` state with `current_etag == None`
/// (always `None` here, since it comes from the `.meta`) must serve one version.
#[tokio::test]
async fn widened_straddle_over_a_shared_bin_of_two_pending_versions_never_mixes() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-shared-bin-widened.bin";
    shared_bin_state(key, &cache_manager).await;
    let s3 = VersionedS3::new(0); // S3 holds only version 2
    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;
    assert_eq!(served.body.len(), 12, "{:?}", s3.seen());
    assert_one_version_etag_optional(&served, "shared .bin, widened straddle");
}
///The same state through the non-widened path: with no `.meta` the range
/// lookup reports nothing cached, so the read is served from S3, one version.
#[tokio::test]
async fn plain_range_over_a_shared_bin_of_two_pending_versions_never_mixes() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-shared-bin-plain.bin";
    shared_bin_state(key, &cache_manager).await;
    let s3 = VersionedS3::new(0);
    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &ResolvedSettings::default(),
        None,
        Arc::clone(&s3),
    )
    .await;
    assert_eq!(served.body.len(), 12, "{:?}", s3.seen());
    assert_eq!(
        assert_one_version_etag_optional(&served, "shared .bin, plain range"),
        V2_ETAG
    );
}
/// Control: two pending `Add`s of the SAME version for one extent (a re-fill,
/// quoted or not) are still coverage: the straddle is a hit with no S3 request.
#[tokio::test]
async fn widened_straddle_over_two_pending_adds_of_one_version_is_a_hit() {
    for (second_etag, key) in [
        (V1_ETAG, "bucket/pin-shared-bin-same.bin"),
        ("pin-v1", "bucket/pin-shared-bin-same-unquoted.bin"),
    ] {
        let config = test_config();
        let (_t, cache_manager, range_handler) = make_infra(&config).await;
        store_journal_pending(key, &cache_manager, 0, PAGE_SIZE - 1, V1_FILL, V1_ETAG).await;
        store_journal_pending(key, &cache_manager, 0, PAGE_SIZE - 1, V1_FILL, second_etag).await;
        store_journal_pending(
            key,
            &cache_manager,
            PAGE_SIZE,
            2 * PAGE_SIZE - 1,
            V1_FILL,
            V1_ETAG,
        )
        .await;
        let s3 = VersionedS3::new(usize::MAX);
        let served = range_get(
            key,
            &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
            HashMap::new(),
            &cache_manager,
            &range_handler,
            &config,
            &widened(),
            None,
            Arc::clone(&s3),
        )
        .await;
        assert_eq!(
            assert_one_version_etag_optional(&served, "two Adds of one version"),
            V1_ETAG
        );
        assert!(
            s3.seen().is_empty(),
            "two pending Adds of one version ({}) are a hit: {:?}",
            second_etag,
            s3.seen()
        );
        let pending = cache_manager
            .create_configured_disk_cache_manager()
            .find_pending_journal_ranges(key, 0, PAGE_SIZE - 1)
            .await
            .expect("journal scan");
        assert!(
            pending
                .iter()
                .any(|r| r.start == 0 && r.end == PAGE_SIZE - 1),
            "two Adds of one version stay pending coverage: {:?}",
            pending
        );
    }
}
///RAM interplay. A read of page 0 alone in the shared-`.bin` state must not
/// put a RAM page whose label names another version than its bytes, and a later
/// straddle that knows version 1 must still serve one version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_bin_page_is_never_promoted_to_ram_under_another_versions_label() {
    let mut config = (*test_config()).clone();
    config.cache.ram_cache_enabled = true;
    let config = Arc::new(config);
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-shared-bin-ram.bin";
    shared_bin_state(key, &cache_manager).await;
    let s3 = VersionedS3::new(0);
    let first = range_get(
        key,
        "bytes=0-11",
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;
    assert_one_version_etag_optional(&first, "shared .bin, page 0 alone");
    // Promotion may finish after the response; give it a bounded moment.
    for _ in 0..20 {
        if cache_manager
            .get_range_from_ram_cache_hit(key, 0, PAGE_SIZE - 1, Duration::from_secs(3600))
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if let Some(hit) =
        cache_manager.get_range_from_ram_cache_hit(key, 0, PAGE_SIZE - 1, Duration::from_secs(3600))
    {
        let bytes_version =
            body_version(&hit.data).unwrap_or_else(|mix| panic!("MIXED RAM PAGE: {}", mix));
        assert_eq!(
            unquote(&hit.etag),
            unquote(bytes_version),
            "RAM PAGE LABELLED WITH ANOTHER VERSION: label={} bytes={}",
            hit.etag,
            bytes_version
        );
    }
    let second = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        Some(V1_ETAG.to_string()),
        Arc::clone(&s3),
    )
    .await;
    assert_eq!(second.body.len(), 12, "{:?}", s3.seen());
    assert_one_version_etag_optional(&second, "shared .bin, straddle after a page-0 read");
}

/// Rewrite the `.meta` on disk so it records no ETag, as one cached from an origin that sends
/// none.
async fn strip_stored_etag(cache_manager: &Arc<CacheManager>, cache_key: &str) {
    let path = cache_manager.get_new_metadata_file_path(cache_key);
    let mut metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    metadata.object_metadata.etag = String::new();
    std::fs::write(&path, serde_json::to_string_pretty(&metadata).unwrap()).unwrap();
    cache_manager.invalidate_metadata_cache(cache_key).await;
}

/// An ETag-less origin: Page 0 is cached as version 1 with no
/// label, and S3 now holds version 2. A read straddling Pages 0 and 1 must not join Page 0's
/// cached bytes to Page 1 fetched from S3: nothing can show they are one version, so the
/// client's original range is served from S3.
#[tokio::test]
async fn etagless_widened_straddle_never_joins_a_cached_page_to_a_fetched_one() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-etagless-straddle.bin";
    seed_v1_extent(key, &cache_manager, 0, PAGE_SIZE - 1).await;
    strip_stored_etag(&cache_manager, key).await;
    let s3 = VersionedS3::etagless(0); // every request sees version 2, with no ETag
    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;
    assert_eq!(served.body.len(), 12, "status={}", served.status);
    if let Err(mix) = body_version(&served.body) {
        panic!(
            "MIXED VERSIONS (ETag-less straddle): {} under etag={:?} content-range={:?}",
            mix, served.etag, served.content_range
        );
    }
}

/// Control: an ETag-less key whose every Page is cached is still served from the cache,
/// with no S3 request.
#[tokio::test]
async fn etagless_widened_straddle_over_cached_pages_is_a_hit() {
    let config = test_config();
    let (_t, cache_manager, range_handler) = make_infra(&config).await;
    let key = "bucket/pin-etagless-cached.bin";
    seed_v1_extent(key, &cache_manager, 0, 2 * PAGE_SIZE - 1).await;
    strip_stored_etag(&cache_manager, key).await;
    let s3 = VersionedS3::etagless(0);
    let served = range_get(
        key,
        &format!("bytes={}-{}", PAGE_SIZE - 6, PAGE_SIZE + 5),
        HashMap::new(),
        &cache_manager,
        &range_handler,
        &config,
        &widened(),
        None,
        Arc::clone(&s3),
    )
    .await;
    assert_eq!(
        body_version(&served.body),
        Ok(V1_ETAG),
        "served the cached pages"
    );
    assert!(
        s3.seen().is_empty(),
        "an ETag-less key with every Page cached made S3 requests: {:?}",
        s3.seen()
    );
}
