//! write-cache-last-modified R9.3: a revalidating `304` that carries `Last-Modified`
//! must put that header on the response even when the backfill cannot persist it.
//!
//! # The state constructed
//!
//! A write-cached entry with an empty `last_modified` (what `PutObject` and
//! `CompleteMultipartUpload` leave, since neither response carries the header). Its next
//! GET revalidates, S3 answers `304` with `Last-Modified`, and the proxy tries to
//! backfill the value under the key's metadata slot lock. Each test holds that slot lock
//! for the whole request, the way a journal fold of the same key holds it, so the
//! backfill's single-attempt `try_acquire_lock` (R7.1a, R7.3) is refused and returns
//! `Err` deterministically.
//!
//! The first two tests drive the mainline arms; the last two drive the
//! coalescing-waiter arms. Before the fix every `304` arm served from cached metadata that still had no
//! `Last-Modified`, so the response omitted it. The tests assert the response header and,
//! because the lock is held throughout, that the `.meta` is still unhealed: the second
//! half proves the header came from the `304` and not from a backfill that slipped in.
//!
//! Spec: write-cache-last-modified. Requirements: 9.3

mod common;

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    assert_all_old, body_of, conditional_requests, not_modified_with_last_modified, proxy_get,
    test_config, test_config_full_object_checks_with_get_ttl, Fixture, SeedSpec, NEW_FILL,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const LEARNED_LM: &str = "Mon, 05 Oct 2026 23:44:12 GMT";
/// Bounds each request so a wedged serve fails with a message instead of hanging.
const REQUEST_BOUND: Duration = Duration::from_secs(20);

/// Full-object mainline arm (`handle_get_head_request`), through the loopback proxy.
#[tokio::test(flavor = "multi_thread")]
async fn full_object_304_carries_last_modified_when_backfill_lock_is_held() {
    // A generous get_ttl: the revalidation must come from the write-cache trigger, not
    // from live-TTL expiry.
    let config =
        test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::from_secs(86400));
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/scn1-full-object-contended.bin";
    let path = format!("/{}", key);

    let etag = fixture.seed(
        key,
        &SeedSpec::write_cached_no_last_modified(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"scn1-fo-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(key).await;
    let before = fixture.read_meta(key).expect("fixture: .meta must exist");
    assert!(
        before.object_metadata.is_write_cached
            && before.object_metadata.effective_last_modified().is_none(),
        "fixture: the entry must be write-cached with no Last-Modified"
    );

    let stub = StubS3Client::new()
        .with_response_for_etag(
            etag.clone(),
            not_modified_with_last_modified(&etag, LEARNED_LM),
        )
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
                .with_header("etag", "\"scn1-fo-v2\""),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let slot = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(
        fixture.cache_manager.get_cache_dir(),
        key,
    )
    .expect("fixture: take the key's slot lock");
    let (status, headers, body) =
        tokio::time::timeout(REQUEST_BOUND, proxy_get(server.addr, &path, &[]))
            .await
            .expect("the GET must complete within the bound");
    drop(slot);

    let captured = stub.captured();
    assert!(
        !conditional_requests(&captured).is_empty(),
        "fixture: the write-cache trigger must have sent a conditional. status={} \
         body_len={} captured={:#?}",
        status,
        body.len(),
        captured
    );
    assert_eq!(status, StatusCode::OK, "a validated serve returns 200");
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "after the 304 the cached bytes must be served",
    );
    assert_eq!(
        headers.get("last-modified").and_then(|v| v.to_str().ok()),
        Some(LEARNED_LM),
        "R9.3: the 304 carried Last-Modified, so the full-object response must carry it \
         even though the backfill could not take the slot lock. Response headers: {:#?}",
        headers
    );

    let after = fixture.read_meta(key).expect(".meta must still exist");
    assert!(
        after.object_metadata.effective_last_modified().is_none()
            && after.object_metadata.is_write_cached,
        "the slot lock was held for the whole request, so the backfill must not have \
         persisted; a healed .meta here means the header could have come from the \
         backfill and this test did not measure R9.3. last_modified={:?} \
         is_write_cached={}",
        after.object_metadata.last_modified,
        after.object_metadata.is_write_cached
    );
}

/// Range mainline arm (`handle_range_request`), serving with the pre-`304`
/// `preloaded_metadata` after a failed backfill.
#[tokio::test(flavor = "multi_thread")]
async fn range_304_carries_last_modified_when_backfill_lock_is_held() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "bucket/scn1-range-contended.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);

    let etag = fixture.seed(
        key,
        &SeedSpec::write_cached_no_last_modified(
            vec![(start, end)],
            OBJECT_SIZE,
            "\"scn1-range-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(
            etag.clone(),
            not_modified_with_last_modified(&etag, LEARNED_LM),
        )
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"scn1-range-v2\""),
        );
    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    let slot = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(
        fixture.cache_manager.get_cache_dir(),
        key,
    )
    .expect("fixture: take the key's slot lock");
    let response = tokio::time::timeout(
        REQUEST_BOUND,
        fixture.range_get(
            key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(key),
            stub.clone().into_trait_object(),
        ),
    )
    .await
    .expect("the range GET must complete within the bound");
    drop(slot);

    let status = response.status();
    let last_modified = response
        .headers()
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let headers = response.headers().clone();
    let body = body_of(response).await;

    assert!(
        !conditional_requests(&stub.captured()).is_empty(),
        "fixture: the write-cache trigger must have sent a conditional"
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "after the 304 the cached bytes must be served",
    );
    assert_eq!(
        last_modified.as_deref(),
        Some(LEARNED_LM),
        "R9.3: the 304 carried Last-Modified, so the range response must carry it even \
         though the backfill could not take the slot lock. Response headers: {:#?}",
        headers
    );

    let after = fixture.read_meta(key).expect(".meta must still exist");
    assert!(
        after.object_metadata.effective_last_modified().is_none()
            && after.object_metadata.is_write_cached,
        "the slot lock was held for the whole request, so the backfill must not have \
         persisted. last_modified={:?} is_write_cached={}",
        after.object_metadata.last_modified,
        after.object_metadata.is_write_cached
    );
}

// =========================================================================
// Coalescing-waiter `304` arms
// =========================================================================
//
// A waiter that lost the coalescing race revalidates with its own headers through
// `serve_from_cache_validated` (full-object) or `serve_range_from_cache_validated`
// (range). Both are `pub` and take everything the coordination layer would pass, so
// these tests call them directly with the slot lock held, the same state as above.
// The coordination layer's election and wait are not exercised; what is under test is
// the waiter handler's own `304` arm and its serve.

/// Full-object coalescing-waiter arm (`serve_from_cache_validated`).
#[tokio::test(flavor = "multi_thread")]
async fn full_object_waiter_304_carries_last_modified_when_backfill_lock_is_held() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "bucket/scn2-full-object-waiter-contended.bin";

    let etag = fixture.seed(
        key,
        &SeedSpec::write_cached_no_last_modified(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"scn2-fo-waiter-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(
            etag.clone(),
            not_modified_with_last_modified(&etag, LEARNED_LM),
        )
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
                .with_header("etag", "\"scn2-fo-waiter-v2\""),
        );

    let slot = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(
        fixture.cache_manager.get_cache_dir(),
        key,
    )
    .expect("fixture: take the key's slot lock");
    let response = tokio::time::timeout(
        REQUEST_BOUND,
        s3_proxy::http_proxy::HttpProxy::serve_from_cache_validated(
            hyper::Method::GET,
            format!("/{}", key).parse().expect("uri"),
            "s3.us-west-2.amazonaws.com".to_string(),
            HashMap::new(),
            key.to_string(),
            std::sync::Arc::clone(&fixture.cache_manager),
            std::sync::Arc::clone(&fixture.range_handler),
            stub.clone().into_trait_object(),
            std::sync::Arc::clone(&fixture.config),
            None,
            &s3_proxy::bucket_settings::ResolvedSettings::default(),
            &None,
            None,
        ),
    )
    .await
    .expect("the waiter GET must complete within the bound")
    .expect("serve_from_cache_validated returns Infallible");
    drop(slot);

    let status = response.status();
    let last_modified = response
        .headers()
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let headers = response.headers().clone();
    let body = body_of(response).await;

    assert!(
        !conditional_requests(&stub.captured()).is_empty(),
        "fixture: the waiter must have sent a conditional"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "a validated waiter serve returns 200"
    );
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "after the 304 the waiter must serve the cached bytes",
    );
    assert_eq!(
        last_modified.as_deref(),
        Some(LEARNED_LM),
        "R9.3: the 304 carried Last-Modified, so the full-object waiter's response must \
         carry it even though the backfill could not take the slot lock. Response \
         headers: {:#?}",
        headers
    );

    let after = fixture.read_meta(key).expect(".meta must still exist");
    assert!(
        after.object_metadata.effective_last_modified().is_none()
            && after.object_metadata.is_write_cached,
        "the slot lock was held for the whole request, so the backfill must not have \
         persisted. last_modified={:?} is_write_cached={}",
        after.object_metadata.last_modified,
        after.object_metadata.is_write_cached
    );
}

/// Range coalescing-waiter arm (`serve_range_from_cache_validated`).
#[tokio::test(flavor = "multi_thread")]
async fn range_waiter_304_carries_last_modified_when_backfill_lock_is_held() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "bucket/scn2-range-waiter-contended.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);

    let etag = fixture.seed(
        key,
        &SeedSpec::write_cached_no_last_modified(
            vec![(start, end)],
            OBJECT_SIZE,
            "\"scn2-range-waiter-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(
            etag.clone(),
            not_modified_with_last_modified(&etag, LEARNED_LM),
        )
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"scn2-range-waiter-v2\""),
        );
    let mut waiter_headers = HashMap::new();
    waiter_headers.insert("range".to_string(), format!("bytes={}-{}", start, end));

    let slot = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(
        fixture.cache_manager.get_cache_dir(),
        key,
    )
    .expect("fixture: take the key's slot lock");
    let response = tokio::time::timeout(
        REQUEST_BOUND,
        s3_proxy::http_proxy::HttpProxy::serve_range_from_cache_validated(
            hyper::Method::GET,
            format!("/{}", key).parse().expect("uri"),
            "s3.us-west-2.amazonaws.com".to_string(),
            waiter_headers,
            key.to_string(),
            s3_proxy::range_handler::RangeSpec { start, end },
            std::sync::Arc::clone(&fixture.cache_manager),
            std::sync::Arc::clone(&fixture.range_handler),
            stub.clone().into_trait_object(),
            std::sync::Arc::clone(&fixture.config),
            false,
            None,
            &s3_proxy::bucket_settings::ResolvedSettings::default(),
            &None,
            None,
        ),
    )
    .await
    .expect("the waiter range GET must complete within the bound")
    .expect("serve_range_from_cache_validated returns Infallible");
    drop(slot);

    let status = response.status();
    let last_modified = response
        .headers()
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let headers = response.headers().clone();
    let body = body_of(response).await;

    assert!(
        !conditional_requests(&stub.captured()).is_empty(),
        "fixture: the waiter must have sent a conditional"
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "after the 304 the waiter must serve the cached bytes",
    );
    assert_eq!(
        last_modified.as_deref(),
        Some(LEARNED_LM),
        "R9.3: the 304 carried Last-Modified, so the range waiter's response must carry \
         it even though the backfill could not take the slot lock. Response headers: {:#?}",
        headers
    );

    let after = fixture.read_meta(key).expect(".meta must still exist");
    assert!(
        after.object_metadata.effective_last_modified().is_none()
            && after.object_metadata.is_write_cached,
        "the slot lock was held for the whole request, so the backfill must not have \
         persisted. last_modified={:?} is_write_cached={}",
        after.object_metadata.last_modified,
        after.object_metadata.is_write_cached
    );
}
