//! Resolve-once-per-request regression test.
//!
//! Spec: `.kiro/specs/cache-match-patterns/`
//! Task: 5.3
//!
//! Locks in the resolve-once-per-request optimization (design "Performance" →
//! "Reduce the number of calls: resolve once per request", Property 7). A
//! multi-range request fans out into one spawned cache-write task per missing
//! range; the optimization resolves settings exactly once at the request
//! decision gate and threads the resulting `ResolvedSettings` into every spawned
//! per-range write instead of re-resolving per range.
//!
//! Counting mechanism: `BucketSettingsManager` carries an `AtomicUsize` that is
//! incremented at the top of the real `resolve()` method (the single
//! source-of-truth resolution path that `CacheManager::resolve_settings`
//! delegates to). The test observes the genuine production resolve path through
//! `CacheManager::get_bucket_settings_manager().resolve_call_count()` — not a
//! mock — so a regression that reintroduces a per-range resolve (e.g. inside the
//! spawned `store_range_new_storage` task) makes the count exceed 1 and fails
//! this test.
//!
//! Why the partial-cache merge path: it is the real multi-range fan-out. With a
//! single cached middle range and a request spanning the whole object, the
//! handler fetches the two missing ranges from S3 and spawns two independent
//! cache-write tasks. Before the optimization each task re-resolved; now they
//! reuse one threaded `ResolvedSettings`. The two spawned writes are awaited
//! (via polling the cache until the object is fully cached) *before* the
//! assertion, so any per-range resolve regression has had the chance to fire.
//!
//! **Validates: Requirements 8.1, 8.2**

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager, Range};
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::{RangeHandler, RangeOverlap, RangeSpec};

use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 300;
const CACHED_START: u64 = 100;
const CACHED_END: u64 = 199; // inclusive — a 100-byte middle range
const ETAG: &str = "\"resolve-once-etag\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

/// Config with coordination disabled (so the range request routes straight
/// through `forward_range_request_to_s3`), RAM cache disabled (so the only
/// internal resolve — RAM-promotion — is gated off), and a zero range-merge gap
/// threshold (so the two missing ranges stay separate and genuinely fan out into
/// two spawned cache-write tasks rather than being consolidated into one).
fn resolve_once_config(cache_dir: std::path::PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.range_merge_gap_threshold = 0;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    Arc::new(config)
}

async fn make_cache_infra(
    config: &Arc<Config>,
) -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
        false, // compression disabled — keeps stored bytes 1:1 with range size
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

    (cache_manager, disk_cache_manager)
}

/// Object metadata describing the full 300-byte object the cached middle range
/// belongs to.
fn full_object_metadata() -> ObjectMetadata {
    let mut headers = HashMap::new();
    headers.insert("etag".to_string(), ETAG.to_string());
    headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());
    headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    headers.insert("content-length".to_string(), OBJECT_SIZE.to_string());

    ObjectMetadata {
        etag: ETAG.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_SIZE,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: headers,
        upload_state: UploadState::Complete,
        cumulative_size: CACHED_END - CACHED_START + 1,
        ..ObjectMetadata::default()
    }
}

fn range_headers() -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("host".to_string(), "bucket.s3.amazonaws.com".to_string());
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=sig"
            .to_string(),
    );
    headers.insert("range".to_string(), format!("bytes=0-{}", OBJECT_SIZE - 1));
    headers
}

/// A multi-range request resolves settings exactly once, not once per spawned
/// per-range cache-write task.
///
/// **Validates: Requirements 8.1, 8.2**
#[tokio::test(flavor = "multi_thread")]
async fn multi_range_request_resolves_settings_exactly_once() {
    let temp_dir = TempDir::new().expect("tempdir");
    let config = resolve_once_config(temp_dir.path().to_path_buf());
    let (cache_manager, disk_cache_manager) = make_cache_infra(&config).await;
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    let cache_key = "bucket/multi-range-object.bin".to_string();

    // Prime a single cached middle range [100,199]. This write goes through the
    // range_handler, which does NOT resolve settings, so the resolve counter is
    // still 0 after priming.
    let cached_data = vec![0xABu8; (CACHED_END - CACHED_START + 1) as usize];
    range_handler
        .store_range_new_storage(
            &cache_key,
            CACHED_START,
            CACHED_END,
            &cached_data,
            full_object_metadata(),
            Duration::from_secs(3600),
            false, // compression disabled
        )
        .await
        .expect("prime cached middle range");

    let manager = cache_manager.get_bucket_settings_manager();
    assert_eq!(
        manager.resolve_call_count(),
        0,
        "priming the cache must not resolve settings"
    );

    // Build the genuine overlap for a request spanning the whole object: one
    // cached range [100,199] plus two separate missing ranges [0,99] and
    // [200,299]. The overlap is constructed directly (rather than via
    // find_cached_ranges) because the primed range's `.meta` file is written
    // through the async journal path and is not yet consolidated; the cached
    // range's `.bin` data and journal entry, however, ARE present, so the
    // partial-merge path can load it via the journal-aware fallback.
    let full_range = RangeSpec {
        start: 0,
        end: OBJECT_SIZE - 1,
    };
    let cached_range = Range {
        start: CACHED_START,
        end: CACHED_END,
        data: Vec::new(), // loaded on demand from disk/journal during merge
        etag: ETAG.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        compression_algorithm: CompressionAlgorithm::None,
    };
    let missing_ranges = vec![
        RangeSpec {
            start: 0,
            end: CACHED_START - 1,
        },
        RangeSpec {
            start: CACHED_END + 1,
            end: OBJECT_SIZE - 1,
        },
    ];
    let overlap = RangeOverlap {
        cached_ranges: vec![cached_range],
        missing_ranges,
        can_serve_from_cache: false,
    };
    assert_eq!(
        manager.resolve_call_count(),
        0,
        "constructing the overlap must not resolve settings"
    );

    // The request decision gate resolves settings exactly once. This mirrors what
    // the real handler does at the read-cache gate before threading the resolved
    // result into the spawned per-range cache-write tasks.
    let resolved = cache_manager.resolve_settings(&cache_key).await;
    assert_eq!(
        manager.resolve_call_count(),
        1,
        "the request decision gate resolves exactly once"
    );

    // Stub S3 returns 206 + 100 bytes for each missing-range fetch. Both missing
    // ranges are 100 bytes, so a single default body satisfies both, producing a
    // clean merge back to the full 300-byte object.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![0xCDu8; 100]))
            .with_header("content-range", format!("bytes 0-99/{}", OBJECT_SIZE)),
    );
    let s3_client = stub.clone().into_trait_object();
    let inflight_tracker = Arc::new(InFlightTracker::new());

    // Drive the multi-range request through the real range path. Coordination is
    // disabled and the request is unsigned, so this routes into
    // forward_range_request_to_s3's partial-cache merge branch, which spawns one
    // store_range_new_storage task per missing range — each reusing the single
    // threaded `resolved` rather than re-resolving.
    let uri: hyper::Uri = "https://bucket.s3.amazonaws.com/multi-range-object.bin"
        .parse()
        .expect("valid uri");
    let response = HttpProxy::forward_range_with_coordination(
        Method::GET,
        uri,
        "bucket.s3.amazonaws.com".to_string(),
        range_headers(),
        cache_key.clone(),
        full_range.clone(),
        overlap,
        Arc::clone(&cache_manager),
        Arc::clone(&range_handler),
        s3_client,
        Arc::clone(&config),
        false, // is_signed = false → unsigned range path
        None,  // preloaded_metadata
        Arc::clone(&inflight_tracker),
        None, // metrics_manager
        &resolved,
        &None,
    )
    .await
    .expect("range coordination");

    assert!(
        response.status() == StatusCode::PARTIAL_CONTENT || response.status() == StatusCode::OK,
        "multi-range request should succeed, got {}",
        response.status()
    );
    // Drain the body so the response is fully realized.
    let _ = response.into_body().collect().await.unwrap().to_bytes();

    // The S3 stub must have been hit once per missing range (the genuine fan-out).
    assert_eq!(
        stub.captured().len(),
        2,
        "expected two S3 range fetches (one per missing range)"
    );

    // Wait for both spawned per-range cache-write tasks to complete. This is the
    // crucial step: the writes run in tokio::spawn after the response is built,
    // so we must let them finish before asserting. A regression that re-resolves
    // inside a spawned write would bump the counter here. The writes land in the
    // journal (shared-storage mode), so poll the journal for both missing ranges.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let dc = disk_cache_manager.read().await;
        let front = dc
            .find_pending_journal_ranges(&cache_key, 0, CACHED_START - 1)
            .await
            .expect("poll journal front range");
        let back = dc
            .find_pending_journal_ranges(&cache_key, CACHED_END + 1, OBJECT_SIZE - 1)
            .await
            .expect("poll journal back range");
        drop(dc);
        let front_done = front
            .iter()
            .any(|r| r.start == 0 && r.end == CACHED_START - 1);
        let back_done = back
            .iter()
            .any(|r| r.start == CACHED_END + 1 && r.end == OBJECT_SIZE - 1);
        if front_done && back_done {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("spawned per-range cache writes did not complete within 15s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The decision-gate resolve is the ONLY resolution for this logical request.
    // The two per-range cache writes reused the threaded ResolvedSettings.
    assert_eq!(
        manager.resolve_call_count(),
        1,
        "a multi-range request must resolve settings exactly once, not once per range"
    );
}
