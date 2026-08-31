//! A changed-object response to a *range* revalidation must never re-serve the
//! superseded cached bytes.
//!
//! Spec: `.kiro/specs/expired-entry-revalidation/` Requirement 3.5, verified per
//! Requirement 7.4 (both `200` and `206`).
//!
//! # Why this file exists separately from the rest of the spec's tests
//!
//! The spec frames R3.5 as a hazard that *becomes* reachable once R1 makes
//! Stored_Expired entries discoverable. Tracing the code found it is reachable
//! **today**, which makes it a correctness defect rather than a latent one, and
//! correctness evidence should not arrive bundled with an API change. So this
//! file is written and run first, against unmodified `src/`.
//!
//! # The state that makes it reachable, and why it is not exotic
//!
//! `handle_range_request`'s conditional-revalidation branch is guarded by
//! `!overlap.cached_ranges.is_empty()`, and the lookup returns nothing once
//! **stored** expiry (`NewCacheMetadata::expires_at`) has passed. So the branch
//! needs the entry to be stored-FRESH while `check_object_expiration` reports
//! **live**-expired — `created_at` age against the *currently resolved*
//! `get_ttl`. That is not a contrived combination; it is precisely what live TTL
//! exists to produce (see `.kiro/steering/cache-coherency-invariants.md` § "The
//! two freshness mechanisms"). Two routine ways in:
//!
//! 1. An operator tightens or zeroes `get_ttl` in `cache_rules.json` for a key
//!    already cached under a generous TTL. This is what the tests below model:
//!    seed at the 3600s default, re-read with `get_ttl: 0`.
//! 2. Any prior `304`. `refresh_object_ttl` sets `expires_at = now + ttl` but
//!    leaves `created_at` untouched, so after one refresh under a tightened
//!    `get_ttl` an entry sits stored-fresh and live-expired *permanently*.
//!
//! # What today's code does
//!
//! There is no `206` arm. The branch handles `304`, `200`, and `403`/`401`, and
//! everything else falls into a generic "unexpected status" arm. A changed object
//! answering a request that carries `Range` replies **`206`**, not `200` — so the
//! ordinary changed-object case for a range revalidation lands in the arm written
//! for surprises. That arm then:
//!
//! 1. calls `remove_invalidated_range` for `overlap.cached_ranges[0]` **only**,
//!    not for every extent; and
//! 2. passes the *stale, pre-invalidation* `overlap` to
//!    `forward_range_request_to_s3`, whose first statement is
//!    `if overlap.missing_ranges.is_empty()` → `serve_range_from_cache`.
//!
//! ## The prediction that was wrong, kept because the correction is the point
//!
//! From that trace alone the conclusion was "a fully-covered changed range
//! re-serves cached bytes", and the first two tests below were written expecting
//! to go red. **They pass.** The trace was right about the control flow and wrong
//! about the outcome: step 1 deletes the `.bin` before step 2 runs, so the
//! cache-serve short-circuit *is* entered but the load fails and the request
//! falls through to a real fetch. Fresh bytes reach the client — by accident of
//! ordering, not by design.
//!
//! That accident holds only while `cached_ranges[0]` covers the whole request,
//! which is the single-extent case. The third test seeds **two** extents, and
//! there the defect is real and confirmed:
//!
//! - only `[0]` is invalidated, so extent `[1]`'s `.bin` survives;
//! - the `.meta` still **references** it, and still carries the **old** ETag;
//! - a later read of that sub-range serves the superseded bytes from cache.
//!
//! Measured: `etag="etag-multi-v1"`, `ranges=[(512, 1023)]`, and 512 of 512
//! returned bytes from the old version, after S3 had already reported the change.
//!
//! Nothing upstream catches it. `handle_get_head_request` derives `current_etag`
//! from `CacheManager::get_object_etag`, which reads the same cached `.meta`, so
//! `RangeHandler::find_cached_ranges` compares that ETag against itself and its
//! mismatch-invalidation branch cannot fire from this call site. See
//! [`range_get`]'s docs.
//!
//! So R3.5's final clause — "it SHALL NOT pass the old complete overlap to a
//! helper that can serve it" — is violated today, on a multi-extent overlap,
//! which is simply what any sequential reader leaves behind.
//!
//! # Why the assertions are built the way they are
//!
//! Per `.kiro/steering/pre-push-checklist.md` § "Assert the predicate the code
//! evaluates": a rejection or a wrong body can have more than one cause, and
//! asserting the outcome asserts none of them.
//!
//! - **The fresh-bytes assertion cannot be satisfied by a second fetch.** Both
//!   the conditional response *and* the fall-through response return the NEW
//!   bytes. R3.5 permits either processing the captured response or re-fetching
//!   against an all-missing overlap, so a test that only programmed the
//!   conditional would fail for a compliant implementation that chose the
//!   second-fetch deviation. With both arms fresh, the **only** way to observe
//!   the old bytes is the stale cache serve.
//! - **The conditional request is asserted to have happened.** Without it, a
//!   plain miss that forwarded and returned fresh bytes would pass while never
//!   reaching the branch under test — the vacuous-pass failure mode T15 had.
//! - **The validator is read from the `.meta`, not hardcoded.** The code
//!   branches on `metadata.object_metadata.etag`, so the stub is keyed on
//!   whatever was actually stored rather than on what the stub was told to send.
//! - **`skip_full_object_check` is forced true** via a small
//!   `full_object_check_threshold`. The range path's *early* full-object shortcut
//!   can direct-serve with no live-TTL check at all — a separate defect (this
//!   spec's R4.1) — and if it fired here it would short-circuit before the
//!   conditional branch, so these tests would silently measure R4.1 instead of
//!   R3.5. Forcing the skip keeps each defect on its own test.

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
// NOTE: this file predates `tests/common/expired_fixture.rs` and carries its own
// copy of the seeder and infra builder. Consolidating them is worthwhile but was
// deliberately not done in the same change as the purpose enum: the R3.5 evidence
// here is a recorded red/green result, and rewriting its fixture would require
// re-establishing that evidence to prove the refactor changed nothing.
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;

use common::{CapturedRequest, StubResponse, StubS3Client};

/// Object is larger than `FULL_OBJECT_CHECK_THRESHOLD` so the early
/// full-object shortcut is skipped — see the module docs.
const OBJECT_SIZE: u64 = 4096;
const FULL_OBJECT_CHECK_THRESHOLD: u64 = 512;

/// The single extent cached and then re-requested. Closed form, so today's
/// reconstructed `bytes=start-end` and the raw client header are byte-identical
/// — raw-Range preservation is a different requirement (R3.2) with its own test.
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 1023;

fn range_header() -> String {
    format!("bytes={}-{}", RANGE_START, RANGE_END)
}

/// Distinct fill bytes so a surviving stale extent is identifiable by value,
/// not merely by length. A length-only check would pass on a stale serve.
const OLD_FILL: u8 = b'A';
const NEW_FILL: u8 = b'B';

fn old_body() -> Vec<u8> {
    vec![OLD_FILL; (RANGE_END - RANGE_START + 1) as usize]
}

fn new_body() -> Vec<u8> {
    vec![NEW_FILL; (RANGE_END - RANGE_START + 1) as usize]
}

/// Cache keys contain `/`, which cannot appear in a range file name.
fn sanitise(cache_key: &str) -> String {
    cache_key.replace(['/', ':'], "_")
}

fn content_range() -> String {
    format!("bytes {}-{}/{}", RANGE_START, RANGE_END, OBJECT_SIZE)
}

fn test_config() -> Arc<Config> {
    let mut config = Config::default();
    // Coordination adds a fetcher/waiter layer that is not what these tests
    // measure; R7.8 owns it. Disabled so a single request takes the inline
    // revalidation path deterministically.
    config.cache.download_coordination.enabled = false;
    // RAM off: a RAM range hit would answer before the disk lookup and neither
    // the conditional request nor the serve decision under test would run.
    config.cache.ram_cache_enabled = false;
    // Force `skip_full_object_check = true`. See module docs.
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

/// Drive the real mainline range handler.
///
/// # `current_etag` reproduces production exactly, and that is load-bearing
///
/// `handle_get_head_request` computes this argument as
/// `cache_manager.get_object_etag(&cache_key)`, which reads
/// `metadata.object_metadata.etag` **from the cached `.meta`** — the very field
/// `RangeHandler::find_cached_ranges` then compares it against. So on the
/// ordinary range path the ETag-mismatch invalidation compares the cached ETag
/// with itself and cannot fire.
///
/// This is worth stating because it forecloses the obvious objection to the
/// stale-data finding below: "your fixture passed `None`, production passes the
/// ETag, and the mismatch branch would have invalidated the survivor." It would
/// not. Passing `None` (check skipped) and passing the cached value (check passes)
/// are the same outcome. The tests below pass the real cached value anyway, so
/// the evidence does not rest on that argument.
#[allow(clippy::too_many_arguments)]
async fn range_get(
    cache_key: &str,
    raw_range: &str,
    cache_manager: &Arc<CacheManager>,
    range_handler: &Arc<RangeHandler>,
    inflight_tracker: &Arc<InFlightTracker>,
    config: &Arc<Config>,
    resolved: &ResolvedSettings,
    current_etag: Option<String>,
    s3_client: Arc<dyn s3_proxy::s3_client::S3ClientApi + Send + Sync>,
) -> hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>> {
    HttpProxy::handle_range_request(
        Method::GET,
        cache_key.to_string(),
        raw_range,
        HashMap::new(),
        Arc::clone(cache_manager),
        Arc::clone(range_handler),
        s3_client,
        "s3.us-west-2.amazonaws.com".to_string(),
        format!("/{}", cache_key).parse().expect("uri"),
        Arc::clone(config),
        resolved,
        current_etag,
        Arc::clone(inflight_tracker),
        None,
        &None,
        false,
        None,
    )
    .await
    .expect("handle_range_request returns Infallible")
}

/// The ETag production would pass as `current_etag` for this key: read from the
/// cached `.meta`, exactly as `CacheManager::get_object_etag` does.
fn production_current_etag(cache_manager: &Arc<CacheManager>, cache_key: &str) -> Option<String> {
    let path = cache_manager.get_new_metadata_file_path(cache_key);
    let content = std::fs::read_to_string(path).ok()?;
    let metadata: s3_proxy::cache_types::NewCacheMetadata = serde_json::from_str(&content).ok()?;
    Some(metadata.object_metadata.etag)
}

async fn body_of(
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

/// Seed one cached extent — `.bin` on disk plus a stored-FRESH `.meta` — and
/// return the ETag as it was actually persisted.
///
/// # Why this writes the cache files directly instead of seeding via a GET
///
/// A first attempt seeded by driving a cold `handle_range_request` and waiting
/// for the `.meta`. It never appeared, and the reason is structural rather than a
/// timing flake: metadata writes go through the per-instance journal, so the
/// `.meta` only materialises once the background consolidator folds the journal
/// entry in. Waiting on it makes the fixture depend on `consolidation_interval`,
/// which is the mistake `pre-push-checklist.md` § "Wait on the mechanism you
/// MEASURE" describes — and here it would gate the whole test on a mechanism
/// that has nothing to do with R3.5. (`download_coordination_stampede_test.rs`
/// hits the same wall and prints a warning before falling through.)
///
/// Writing `ranges/<file_path>` and the `.meta` directly is deterministic and
/// is what the rest of the suite does for cache-state fixtures. It also lets the
/// stored `expires_at` be set explicitly rather than inferred from whichever TTL
/// the write path happened to consult.
///
/// `CompressionAlgorithm::None` is used so the `.bin` holds raw bytes: the tag
/// means "no frame, no checksum" and remains readable, so the fixture does not
/// have to produce a valid LZ4 frame to be loadable.
///
/// The returned ETag is read back from the `.meta` rather than assumed, because
/// `check_object_expiration` injects `metadata.object_metadata.etag` and that is
/// the value the stub must route on. Keying the stub on what the fixture *meant*
/// to store would be asserting a value adjacent to the one the code branches on.
/// `extents` is the list of `(start, end)` cached extents to lay down, in
/// metadata order. One extent models the ordinary case; several model the case
/// where `overlap.cached_ranges[0]` does **not** cover everything, which is
/// where invalidating only `[0]` leaves stale coverage behind.
async fn seed_cached_extents(
    cache_key: &str,
    cache_manager: &Arc<CacheManager>,
    seed_etag: &str,
    extents: &[(u64, u64)],
) -> String {
    use s3_proxy::cache_types::{
        CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
    };
    use s3_proxy::compression::CompressionAlgorithm;

    assert!(!extents.is_empty(), "fixture must seed at least one extent");
    let now = std::time::SystemTime::now();

    let mut range_specs = Vec::new();
    for (start, end) in extents.iter().copied() {
        // Old bytes, so any surviving stale extent is byte-distinguishable from
        // the fresh representation.
        let body = vec![OLD_FILL; (end - start + 1) as usize];
        let bin_relative = format!(
            "bucket/00/000/{}_{}-{}.bin",
            sanitise(cache_key),
            start,
            end
        );
        let bin_path = cache_manager
            .get_cache_dir()
            .join("ranges")
            .join(&bin_relative);
        std::fs::create_dir_all(bin_path.parent().expect("bin parent")).expect("create ranges dir");
        std::fs::write(&bin_path, &body).expect("write .bin");

        range_specs.push(RangeSpec {
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
        });
    }

    // Stored-FRESH: `expires_at` an hour out. This is the precondition that
    // makes the conditional branch reachable at all — see the module docs.
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: seed_etag.to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length: OBJECT_SIZE,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: OBJECT_SIZE,
            parts: Vec::new(),
            response_headers: HashMap::new(),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: range_specs.iter().map(|r| r.compressed_size).sum(),
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        },
        ranges: range_specs,
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: None,
        head_last_accessed: None,
        head_access_count: 0,
        head_cached_at: None,
    };

    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().expect("meta parent"))
        .expect("create metadata dir");
    std::fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata).expect("serialize .meta"),
    )
    .expect("write .meta");

    // Read it back, so every assertion below is against what is genuinely on
    // disk rather than against the value just written.
    let metadata: NewCacheMetadata =
        serde_json::from_str(&std::fs::read_to_string(&metadata_path).expect("read .meta"))
            .expect("parse .meta");

    // Candidate_Available, asserted rather than assumed: metadata coverage AND
    // the range file on disk. A test that proceeds without both would report a
    // stale-serve verdict about an entry that does not exist.
    assert!(
        !metadata.ranges.is_empty(),
        "PRECONDITION MISSING: .meta records no ranges, so there is no candidate coverage"
    );
    // Combined coverage, not per-extent: the multi-extent fixture deliberately
    // has no single extent spanning the request, and complete coverage is what
    // makes `can_serve_from_cache` true and the conditional branch reachable.
    let covers = {
        let mut sorted: Vec<(u64, u64)> =
            metadata.ranges.iter().map(|r| (r.start, r.end)).collect();
        sorted.sort_unstable();
        let mut cursor = RANGE_START;
        for (start, end) in sorted {
            if start > cursor {
                break;
            }
            cursor = cursor.max(end.saturating_add(1));
        }
        cursor > RANGE_END
    };
    assert!(
        covers,
        "PRECONDITION MISSING: cached extents {:?} do not completely cover {}-{}",
        metadata
            .ranges
            .iter()
            .map(|r| (r.start, r.end))
            .collect::<Vec<_>>(),
        RANGE_START,
        RANGE_END
    );
    for r in &metadata.ranges {
        let bin = cache_manager
            .get_cache_dir()
            .join("ranges")
            .join(&r.file_path);
        assert!(
            bin.exists(),
            "PRECONDITION MISSING: range file absent at {:?}; Candidate_Available is false",
            bin
        );
    }

    // Stored-FRESH is the precondition that makes the conditional branch
    // reachable at all. If this ever fails the entry is Stored_Expired, the
    // lookup returns nothing, and the branch under test is dead — which is the
    // *other* half of issue #17, not this one.
    assert!(
        metadata.expires_at > std::time::SystemTime::now(),
        "PRECONDITION MISSING: entry is already Stored_Expired, so the conditional \
         branch is unreachable and this test cannot observe R3.5 at all"
    );

    let stored_etag = metadata.object_metadata.etag.clone();
    assert!(
        !stored_etag.is_empty(),
        "PRECONDITION MISSING: no ETag stored, so no validator can be injected"
    );

    // The metadata cache is per-instance with a refresh interval; drop the entry
    // so the re-read below sees the committed `.meta` deterministically.
    cache_manager.invalidate_metadata_cache(cache_key).await;

    stored_etag
}

/// Requests that carried a proxy-injected validator, i.e. the conditional
/// revalidation attempts. Used to prove the branch under test was entered.
fn conditional_requests(captured: &[CapturedRequest]) -> Vec<&CapturedRequest> {
    captured
        .iter()
        .filter(|r| r.if_none_match().is_some() || r.if_modified_since().is_some())
        .collect()
}

/// A changed object answering a range revalidation with **`206`** must serve the
/// fresh bytes, never the superseded cached ones.
///
/// `206` is the status S3 actually returns here, because the revalidation request
/// carries `Range`. Today there is no `206` arm.
///
/// Requirements: 3.5, 7.4.
#[tokio::test]
async fn changed_206_range_revalidation_must_not_serve_stale_cached_bytes() {
    let config = test_config();
    let (_temp, cache_manager, range_handler, inflight_tracker) = make_infra(&config).await;
    let cache_key = "bucket/changed-206-range.bin";

    let stored_etag = seed_cached_extents(
        cache_key,
        &cache_manager,
        "\"etag-v1\"",
        &[(RANGE_START, RANGE_END)],
    )
    .await;

    // Changed object, answered as 206 because the request carries Range.
    let changed = StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(new_body()))
        .with_header("content-range", content_range())
        .with_header("etag", "\"etag-v2\"");

    // BOTH arms return the NEW bytes: the conditional (matched on the stored
    // validator) and the fall-through that a compliant second-fetch deviation
    // would take. So old bytes in the response can only come from a cache serve.
    let stub = StubS3Client::new()
        .with_response_for_etag(stored_etag.clone(), changed.clone())
        .with_default(changed);

    // get_ttl: 0 → live-expired while stored-fresh. See module docs.
    let resolved = ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    };

    let response = range_get(
        cache_key,
        &range_header(),
        &cache_manager,
        &range_handler,
        &inflight_tracker,
        &config,
        &resolved,
        production_current_etag(&cache_manager, cache_key),
        stub.clone().into_trait_object(),
    )
    .await;

    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    // Reaching the branch under test is a precondition, not the finding. Without
    // this a plain miss that forwarded and returned fresh bytes would pass while
    // never entering the conditional arm.
    assert!(
        !conditionals.is_empty(),
        "PRECONDITION MISSING: no conditional request was issued, so the changed-206 \
         arm was never entered. Captured requests: {:#?}",
        captured
    );
    let conditional = conditionals[0];
    assert_eq!(
        conditional.if_none_match(),
        Some(stored_etag.as_str()),
        "the conditional must carry the ETag stored in the .meta — that is the value \
         check_object_expiration injects and the value the stub routes on"
    );
    assert_eq!(
        conditional.headers.get("range").map(String::as_str),
        Some(range_header().as_str()),
        "R3.3: the cached validator must ride on the SAME request that carries the Range"
    );

    // The finding.
    assert_ne!(
        body,
        old_body(),
        "STALE SERVE: S3 answered the range revalidation 206 with a changed object, \
         but the proxy returned the superseded cached bytes. R3.5 forbids passing the \
         old complete overlap to a helper that can serve it."
    );
    assert_eq!(
        body,
        new_body(),
        "the client must receive the fresh representation (status={}, {} bytes)",
        status,
        body.len()
    );
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "range semantics must be retained across the changed-object refetch"
    );
}

/// The `200` counterpart. R7.4 requires both statuses covered, and this one has
/// a dedicated arm today — so it is the control: if `200` passes while `206`
/// fails, the difference is attributable to the missing `206` arm rather than to
/// changed-object handling being broken in general.
///
/// Note this arm is not obviously correct either: it invalidates only
/// `overlap.cached_ranges[0]` and then hands the same stale `overlap` to
/// `forward_range_request_to_s3`. With a single cached extent the invalidation
/// happens to cover everything, which is why this is expected to pass and why a
/// multi-extent case is a separate assertion below.
///
/// Requirements: 3.5, 7.4.
#[tokio::test]
async fn changed_200_range_revalidation_must_not_serve_stale_cached_bytes() {
    let config = test_config();
    let (_temp, cache_manager, range_handler, inflight_tracker) = make_infra(&config).await;
    let cache_key = "bucket/changed-200-range.bin";

    let stored_etag = seed_cached_extents(
        cache_key,
        &cache_manager,
        "\"etag-200-v1\"",
        &[(RANGE_START, RANGE_END)],
    )
    .await;

    // A changed object answering with the FULL representation.
    let changed_full = StubResponse::with_status(StatusCode::OK)
        .with_body(Bytes::from(new_body()))
        .with_header("etag", "\"etag-200-v2\"");
    let changed_partial = StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(new_body()))
        .with_header("content-range", content_range())
        .with_header("etag", "\"etag-200-v2\"");

    let stub = StubS3Client::new()
        .with_response_for_etag(stored_etag.clone(), changed_full)
        // The fall-through models the second-fetch deviation R3.5 permits: a
        // plain range request, answered 206 with the fresh bytes.
        .with_default(changed_partial);

    let resolved = ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    };

    let response = range_get(
        cache_key,
        &range_header(),
        &cache_manager,
        &range_handler,
        &inflight_tracker,
        &config,
        &resolved,
        production_current_etag(&cache_manager, cache_key),
        stub.clone().into_trait_object(),
    )
    .await;

    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    assert!(
        !conditionals.is_empty(),
        "PRECONDITION MISSING: no conditional request was issued, so the changed-200 \
         arm was never entered. Captured requests: {:#?}",
        captured
    );
    assert_eq!(
        conditionals[0].if_none_match(),
        Some(stored_etag.as_str()),
        "the conditional must carry the stored ETag"
    );

    assert_ne!(
        body,
        old_body(),
        "STALE SERVE: changed-200 range revalidation returned the superseded cached bytes \
         (status={})",
        status
    );
    assert_eq!(
        body,
        new_body(),
        "the client must receive the fresh representation (status={}, {} bytes)",
        status,
        body.len()
    );
}

/// The multi-extent changed-`206` case — where the single-extent masking cannot
/// apply.
///
/// # Why this test exists, and what the single-extent tests above actually proved
///
/// The two tests above were written expecting to go red, on a trace that said the
/// changed-`206` path re-serves cached bytes. **They passed.** That refutes the
/// hypothesis as stated, and the reason matters more than the verdict.
///
/// The "unexpected status" arm calls `remove_invalidated_range` for
/// `overlap.cached_ranges[0]` *before* handing the stale `overlap` to
/// `forward_range_request_to_s3`. That deletes the `.bin`. So although
/// `missing_ranges.is_empty()` is still true and the cache-serve short-circuit
/// *is* taken, the load fails and the request falls through to a real fetch.
/// Fresh bytes reach the client — by accident of ordering, not by design.
///
/// That accident holds only while `cached_ranges[0]` covers the whole requested
/// range, which is exactly the single-extent case. With two extents,
/// invalidating `[0]` leaves `[1]`'s `.bin` on disk while the stale `overlap`
/// still claims complete coverage, so the short-circuit can find loadable stale
/// data for part of the range. That is the state R3.5's final clause names: "it
/// SHALL NOT pass the old complete overlap to a helper that can serve it."
///
/// Reachability of a multi-extent overlap is ordinary, not contrived — it is what
/// any sequential reader produces. Two adjacent reads leave two adjacent extents,
/// and a later read spanning both gets a two-element `cached_ranges`.
///
/// Both arms of the stub again return fresh bytes, so old bytes anywhere in the
/// response can only have come from cache.
///
/// Requirements: 3.5, 7.4.
#[tokio::test]
async fn changed_206_range_revalidation_must_not_serve_surviving_stale_extents() {
    let config = test_config();
    let (_temp, cache_manager, range_handler, inflight_tracker) = make_infra(&config).await;
    let cache_key = "bucket/changed-206-multi-extent.bin";

    // Two adjacent extents that together cover the requested range exactly.
    let mid = RANGE_START + (RANGE_END - RANGE_START) / 2;
    let extents = [(RANGE_START, mid), (mid + 1, RANGE_END)];
    let stored_etag =
        seed_cached_extents(cache_key, &cache_manager, "\"etag-multi-v1\"", &extents).await;

    let changed = StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(new_body()))
        .with_header("content-range", content_range())
        .with_header("etag", "\"etag-multi-v2\"");

    let stub = StubS3Client::new()
        .with_response_for_etag(stored_etag.clone(), changed.clone())
        .with_default(changed);

    let resolved = ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    };

    let response = range_get(
        cache_key,
        &range_header(),
        &cache_manager,
        &range_handler,
        &inflight_tracker,
        &config,
        &resolved,
        production_current_etag(&cache_manager, cache_key),
        stub.clone().into_trait_object(),
    )
    .await;

    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    assert!(
        !conditionals.is_empty(),
        "PRECONDITION MISSING: no conditional request was issued, so the changed-206 \
         arm was never entered. Captured requests: {:#?}",
        captured
    );

    // Report the surviving `.bin` files. R3.5 requires ALL old-version coverage
    // to be invalidated, so a survivor is a finding in its own right even if the
    // response happened to come out fresh.
    let surviving: Vec<String> = extents
        .iter()
        .filter(|(s, e)| {
            cache_manager
                .get_cache_dir()
                .join("ranges")
                .join(format!(
                    "bucket/00/000/{}_{}-{}.bin",
                    sanitise(cache_key),
                    s,
                    e
                ))
                .exists()
        })
        .map(|(s, e)| format!("{}-{}", s, e))
        .collect();

    // The response must contain no old bytes at all — not "mostly fresh".
    let stale_byte_count = body.iter().filter(|b| **b == OLD_FILL).count();
    assert_eq!(
        stale_byte_count,
        0,
        "STALE SERVE: {} of {} returned bytes are from the superseded version. \
         S3 answered the range revalidation 206 with a changed object, but old-version \
         coverage reached a cache-serving helper. Surviving old .bin extents: {:?}. \
         R3.5 requires ALL old-version coverage to be invalidated before any serve decision.",
        stale_byte_count,
        body.len(),
        surviving
    );
    assert_eq!(
        body,
        new_body(),
        "the client must receive the fresh representation (status={}, {} bytes, \
         surviving old extents: {:?})",
        status,
        body.len(),
        surviving
    );
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "range semantics must be retained across the changed-object refetch"
    );

    // ---------------------------------------------------------------------
    // The decisive check: is the surviving old-version extent REACHABLE?
    //
    // A surviving `.bin` matters only insofar as something can serve it. If the
    // refetch rewrote the `.meta` and the survivor is now unreferenced, this is
    // an orphaned-file and accounting problem — real, but owned elsewhere and not
    // a coherency defect. If the `.meta` still lists that extent, a later read of
    // those bytes serves the superseded version, and under the NEW ETag, which is
    // a coherency defect of the first order.
    //
    // Distinguishing them requires reading the post-request `.meta` and then
    // actually issuing the read, rather than reasoning about it. The sub-range
    // read below is live-FRESH (`get_ttl` 3600) so it takes the ordinary
    // cache-serve path with no revalidation, and the stub returns FRESH bytes —
    // so old bytes in its response can only be a stale cache serve.
    // ---------------------------------------------------------------------
    let post_meta_path = cache_manager.get_new_metadata_file_path(cache_key);
    let post_meta: Option<s3_proxy::cache_types::NewCacheMetadata> =
        std::fs::read_to_string(&post_meta_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
    let post_state = match &post_meta {
        Some(m) => format!(
            "etag={:?} ranges={:?}",
            m.object_metadata.etag,
            m.ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>()
        ),
        None => "no .meta on disk".to_string(),
    };

    cache_manager.invalidate_metadata_cache(cache_key).await;

    let survivor_start = mid + 1;
    let survivor_end = RANGE_END;
    let survivor_len = (survivor_end - survivor_start + 1) as usize;
    let fresh_survivor_bytes = vec![NEW_FILL; survivor_len];
    let survivor_stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(fresh_survivor_bytes.clone()))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", survivor_start, survivor_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"etag-multi-v2\""),
    );

    let fresh_settings = ResolvedSettings::default(); // get_ttl 3600 → live-fresh
    let survivor_response = range_get(
        cache_key,
        &format!("bytes={}-{}", survivor_start, survivor_end),
        &cache_manager,
        &range_handler,
        &inflight_tracker,
        &config,
        &fresh_settings,
        production_current_etag(&cache_manager, cache_key),
        survivor_stub.clone().into_trait_object(),
    )
    .await;
    let survivor_body = body_of(survivor_response).await;
    let survivor_stale = survivor_body.iter().filter(|b| **b == OLD_FILL).count();

    assert_eq!(
        survivor_stale, 0,
        "REACHABLE STALE DATA: a later read of {}-{} returned {} old-version bytes \
         after S3 had already reported the object changed. Post-revalidation cache \
         state: {}. Surviving old .bin extents: {:?}.",
        survivor_start, survivor_end, survivor_stale, post_state, surviving
    );

    // Weakest of the three, asserted last so a failure here is not mistaken for
    // reachable stale data above.
    assert!(
        surviving.is_empty(),
        "INCOMPLETE INVALIDATION: old-version extents {:?} survived a changed-object 206 \
         (post-revalidation cache state: {}). Only overlap.cached_ranges[0] is invalidated \
         today; R3.5 requires all old-version coverage to go before any serve decision. \
         Note the reachability assertion above PASSED, so as of this run the survivor is \
         not servable — it is a leak rather than a coherency break. Do not downgrade this \
         to a warning on that basis: reachability depends on the refetch's cache write \
         succeeding, which is not guaranteed.",
        surviving,
        post_state
    );
}
