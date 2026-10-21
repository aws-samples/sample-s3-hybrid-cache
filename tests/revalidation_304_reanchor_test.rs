//! A `304` revalidation must re-anchor the live GET age.
//!
//! Live GET freshness is `now - anchor <= get_ttl` (`DiskCacheManager::check_object_expiration`).
//! The anchor used to be `created_at` alone, and no `304` path moved it: the mainline `304`
//! arms call `DiskCacheManager::refresh_object_ttl`, which moves only `expires_at`, and the
//! coalescing waiters call `CacheManager::refresh_cache_ttl`, which moves only `expires_at`
//! and `head_expires_at`. So an entry older than `get_ttl` revalidated on every GET for the
//! rest of its life: one S3 round trip per read, each answered `304`.
//!
//! # What is asserted
//!
//! The observable is the number of **upstream requests**, not the body: the object is
//! unchanged, so a cache serve and a revalidated serve return the same bytes. Each test
//! issues a first GET that must revalidate, then a second GET well inside `get_ttl`, and
//! counts what went upstream across both.
//!
//! The Mode B test is the guard on the fix's width. A client `If-Match` answered from cache
//! compares the client's ETag against the cache's own, which establishes nothing about the
//! origin, so it must NOT re-anchor. Re-anchoring there would let one Mode B hit extend
//! unvalidated serving for another full `get_ttl`.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;
use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache_types::ObjectExpirationResult;

use common::expired_fixture::{
    assert_all_old, body_of, conditional_requests, not_modified, proxy_get,
    test_config_full_object_checks_with_get_ttl, Fixture, SeedSpec, NEW_FILL, OLD_FILL,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const BUCKET: &str = "test-bucket";

/// One hour. The seed's `created_at` is two hours old, so the entry is Live_Expired on the
/// first GET, and a second GET seconds after a `304` is well inside the window.
const GET_TTL: Duration = Duration::from_secs(3600);

fn new_bytes(len: usize) -> Vec<u8> {
    vec![NEW_FILL; len]
}

/// Full-object mainline path (`handle_get_head_request`), loopback proxy.
#[tokio::test]
async fn full_object_second_get_after_304_is_served_without_upstream() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, GET_TTL);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/reanchor-full.bin", BUCKET);
    let path = format!("/{}", key);

    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"reanchor-full-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(&key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(new_bytes(OBJECT_SIZE as usize)))
                .with_header("etag", "\"reanchor-full-v2\""),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status1, _, body1) = proxy_get(server.addr, &path, &[]).await;
    let after_first = stub.captured().len();
    assert_eq!(status1, StatusCode::OK);
    assert_all_old(
        &body1,
        OBJECT_SIZE as usize,
        "first GET: 304 serves the cached bytes",
    );
    assert_eq!(
        conditional_requests(&stub.captured()).len(),
        1,
        "PRECONDITION: the first GET must revalidate exactly once (created_at is 2h old, \
         get_ttl is 1h). Captured: {:#?}",
        stub.captured()
    );

    fixture.invalidate_metadata_cache(&key).await;
    let (status2, _, body2) = proxy_get(server.addr, &path, &[]).await;
    let captured = stub.captured();
    assert_eq!(status2, StatusCode::OK);
    assert_all_old(
        &body2,
        OBJECT_SIZE as usize,
        "second GET serves the cached bytes",
    );
    assert_eq!(
        captured.len(),
        after_first,
        "a GET inside get_ttl after a 304 must not go upstream: upstream requests \
         after GET 1 = {}, after GET 2 = {}. The 304 did not re-anchor the live age. \
         Captured: {:#?}",
        after_first,
        captured.len(),
        captured
    );
}

/// Range mainline path (`handle_range_request`), driven directly with a resolved 1h `get_ttl`.
#[tokio::test]
async fn range_second_get_after_304_is_served_without_upstream() {
    // `test_config` semantics (full-object shortcut disabled) so the range-specific lookup
    // and its own `304` arm are what run.
    let mut config = s3_proxy::config::Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = OBJECT_SIZE / 8;
    config.cache.get_ttl = GET_TTL;
    let fixture = Fixture::new(std::sync::Arc::new(config)).await;
    let cache_key = "bucket/reanchor-range.bin";
    let (start, end) = (1024u64, 2047u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"reanchor-range-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    let raw_range = format!("bytes={}-{}", start, end);
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"reanchor-range-v2\""),
        );
    let resolved = ResolvedSettings {
        get_ttl: GET_TTL,
        ..ResolvedSettings::default()
    };

    let body1 = body_of(
        fixture
            .range_get(
                cache_key,
                &raw_range,
                HashMap::new(),
                &resolved,
                fixture.production_current_etag(cache_key),
                stub.clone().into_trait_object(),
            )
            .await,
    )
    .await;
    assert_eq!(
        body1,
        vec![OLD_FILL; len],
        "first GET: 304 serves the cached bytes"
    );
    let after_first = stub.captured().len();
    assert_eq!(
        conditional_requests(&stub.captured()).len(),
        1,
        "PRECONDITION: the first ranged GET must revalidate exactly once. Captured: {:#?}",
        stub.captured()
    );

    fixture.invalidate_metadata_cache(cache_key).await;
    let body2 = body_of(
        fixture
            .range_get(
                cache_key,
                &raw_range,
                HashMap::new(),
                &resolved,
                fixture.production_current_etag(cache_key),
                stub.clone().into_trait_object(),
            )
            .await,
    )
    .await;
    let captured = stub.captured();
    assert_eq!(
        body2,
        vec![OLD_FILL; len],
        "second GET serves the cached bytes"
    );
    assert_eq!(
        captured.len(),
        after_first,
        "a ranged GET inside get_ttl after a 304 must not go upstream: upstream requests \
         after GET 1 = {}, after GET 2 = {}. Captured: {:#?}",
        after_first,
        captured.len(),
        captured
    );
}

/// The coalescing waiters' `304` step (`CacheManager::refresh_cache_ttl`) must re-anchor too.
#[tokio::test]
async fn waiter_refresh_cache_ttl_reanchors_live_age() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, GET_TTL);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/reanchor-waiter.bin", BUCKET);
    fixture.seed(
        &key,
        &SeedSpec::expired(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"reanchor-waiter-v1\"",
        ),
    );
    let disk = fixture.cache_manager.create_configured_disk_cache_manager();

    let before = disk.check_object_expiration(&key, GET_TTL).await.unwrap();
    assert!(
        matches!(before, ObjectExpirationResult::Expired { .. }),
        "PRECONDITION: a 2h-old entry is Live_Expired under a 1h get_ttl, got {:?}",
        before
    );

    fixture.cache_manager.refresh_cache_ttl(&key).await.unwrap();

    let after = disk.check_object_expiration(&key, GET_TTL).await.unwrap();
    assert!(
        matches!(after, ObjectExpirationResult::Fresh),
        "after a waiter's 304 refresh the entry must be Fresh for get_ttl, got {:?}",
        after
    );
    // `created_at` keeps its other meanings (eviction age, ordering): it must not move.
    let meta = fixture.read_meta(&key).expect(".meta");
    assert!(
        meta.created_at.elapsed().unwrap() > Duration::from_secs(7000),
        "created_at must be left alone by a 304 refresh"
    );
}

/// Width guard: only an origin `304` re-anchors. Since the change a Mode B `If-Match` GET is
/// served locally only within the live `get_ttl`, so on a live-expired entry it revalidates
/// and the `304` it receives is what re-anchors; the plain GET after it then needs no S3
/// request. (The Mode B local serve itself writes no TTL at all.)
#[tokio::test]
async fn mode_b_on_a_live_expired_entry_revalidates_and_the_304_reanchors() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, GET_TTL);
    let fixture = Fixture::new(config).await;
    assert!(
        fixture.config.cache.evaluate_conditions_from_cache,
        "PRECONDITION MISSING: Mode B is off in this config"
    );
    let key = format!("{}/reanchor-modeb.bin", BUCKET);
    let path = format!("/{}", key);

    // Stored-fresh, live-expired: the Mode B GET revalidates; the plain GET is then fresh.
    let etag = fixture.seed(
        &key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"reanchor-modeb-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(&key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(new_bytes(OBJECT_SIZE as usize)))
                .with_header("etag", "\"reanchor-modeb-v2\""),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status1, _, _) = proxy_get(server.addr, &path, &[("if-match", etag.as_str())]).await;
    assert_eq!(status1, StatusCode::OK);
    assert_eq!(
        conditional_requests(&stub.captured()).len(),
        1,
        "the Mode B If-Match GET on a live-expired entry must revalidate. Captured: {:#?}",
        stub.captured()
    );

    fixture.invalidate_metadata_cache(&key).await;
    let (status2, _, body2) = proxy_get(server.addr, &path, &[]).await;
    assert_eq!(status2, StatusCode::OK);
    assert_all_old(
        &body2,
        OBJECT_SIZE as usize,
        "plain GET after Mode B, 304 serve",
    );
    assert_eq!(
        conditional_requests(&stub.captured()).len(),
        1,
        "the Mode B GET's 304 must re-anchor the live age: the plain GET after it must not \
         revalidate again. Captured: {:#?}",
        stub.captured()
    );
}
