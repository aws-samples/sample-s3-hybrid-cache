//! Failing-first regressions for `.kiro/specs/expired-entry-revalidation/`.
//!
//! Spec: Requirements 2, 3, 7.1, 7.2, 7.3. GitHub issue #17.
//!
//! # The defect, named at the line
//!
//! `DiskCacheManager::find_cached_ranges` returns `Ok(Vec::new())` when
//! `SystemTime::now() > metadata.expires_at`. That empty vector becomes
//! `cached_ranges: []` / `can_serve_from_cache: false` in
//! `RangeHandler::find_cached_ranges`, which fails the
//! `!overlap.cached_ranges.is_empty()` guard in **both** mainline handlers — and
//! `check_object_expiration`, the `ttl_revalidations_total` metric, the validator
//! injection, the `304` handling and the cached serve all sit *inside* that
//! guard.
//!
//! So once stored expiry passes, the cached metadata, the ETag and the bytes are
//! all still on disk and none of them are considered. The request degrades to a
//! full transfer plus a cache rewrite. With `get_ttl: 0` that is *every*
//! sequential re-read.
//!
//! # What these tests assert, and why it is not the response body
//!
//! A test that only checked the returned bytes would pass today: the miss path
//! fetches from S3 and returns correct data. The observable that distinguishes
//! the defect from the fix is **whether a conditional request was issued at
//! all**, so every test here asserts on the captured upstream request:
//!
//! - a conditional exists (R7.1);
//! - it carries the cached validator (R2.1);
//! - on the range path it carries the client's **raw** `Range`, on the **same**
//!   request as the validator (R3.2, R3.3);
//! - the response was served from cache rather than from a body transfer (R2.2).
//!
//! Per `pre-push-checklist.md` § "Assert the predicate the code evaluates", the
//! stub returns `304` **only** for the expected validator. A `304` requires the
//! proxy to have sent the right ETag, so the served-from-cache assertion cannot
//! be satisfied by a request that guessed.
//!
//! # Range first
//!
//! The range cases lead because that is the customer-blocking half: a sequential
//! ranged reader re-fetches the whole object body from S3 on every pass while its
//! bytes sit unread on disk.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    body_of, conditional_requests, not_modified, short_ttl, signed_range_authorization,
    test_config, test_config_coordinated, test_config_full_object_checks, zero_ttl, Fixture,
    SeedSpec, NEW_FILL, OLD_FILL,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;

fn old_bytes(len: usize) -> Vec<u8> {
    vec![OLD_FILL; len]
}

fn new_bytes(len: usize) -> Vec<u8> {
    vec![NEW_FILL; len]
}

// =====================================================================
// Requirement 3 — byte-range path
// =====================================================================

/// R7.2 / R7.1, the headline range regression: a Stored_Expired,
/// Live_Expired, Candidate_Available entry must reach conditional validation on
/// an ordinary sequential range GET.
///
/// Fails today because the lookup hides the extents, so the branch that would
/// build the conditional is never entered.
///
/// Requirements: 3.1, 3.2, 3.3, 3.4, 7.1, 7.2, 2.6.
#[tokio::test]
async fn expired_range_get_issues_conditional_with_raw_range_and_validator() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-zero-ttl.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"range-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let raw_range = format!("bytes={}-{}", start, end);

    // 304 ONLY for the cached validator. Any other request falls through to a
    // body transfer, which is what the defect produces — so "served from cache"
    // cannot be reached by accident.
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"range-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &raw_range,
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    assert!(
        !conditionals.is_empty(),
        "R7.1: no conditional upstream request was issued for a Stored_Expired, \
         Live_Expired, Candidate_Available entry. The stored-expiry check in \
         DiskCacheManager::find_cached_ranges returned no ranges, so the \
         non-empty-overlap guard in handle_range_request was false and the \
         revalidation branch behind it was unreachable. Captured requests: {:#?}",
        captured
    );

    let conditional = conditionals[0];
    assert_eq!(
        conditional.if_none_match(),
        Some(etag.as_str()),
        "R2.1: the conditional must carry the cached ETag"
    );
    assert_eq!(
        conditional.headers.get("range").map(String::as_str),
        Some(raw_range.as_str()),
        "R3.3: the cached validator must ride on the SAME request that carries the Range"
    );

    assert_eq!(
        body,
        old_bytes(len),
        "R2.2/R3.4: after 304 the cached bytes must be served"
    );
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "R3.4: range response semantics must be retained across a 304 Validated_Serve"
    );
}

/// R2.7 / R7.3: the same path with a **non-zero** `get_ttl` whose window has
/// elapsed. Kept separate because `get_ttl: 0` short-circuits
/// `check_object_expiration` before it looks at `created_at`, so a zero-TTL test
/// can pass while the elapsed-TTL arithmetic is wrong.
///
/// Requirements: 2.7, 3.1, 7.3.
#[tokio::test]
async fn expired_range_get_with_elapsed_nonzero_ttl_issues_conditional() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-nonzero-ttl.bin";
    let (start, end) = (1024u64, 2047u64);
    let len = (end - start + 1) as usize;

    // created_at is 2h old by default; short_ttl() is 60s. Both stored and live
    // expiry have therefore elapsed, which is what R2.7 requires.
    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"range-ttl-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
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
                .with_header("etag", "\"range-ttl-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &raw_range,
            HashMap::new(),
            &short_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    let captured = stub.captured();

    assert!(
        !conditional_requests(&captured).is_empty(),
        "R2.7: a nonzero get_ttl whose window has elapsed must also reach conditional \
         validation. Captured requests: {:#?}",
        captured
    );
    assert_eq!(
        body,
        old_bytes(len),
        "R2.2: after 304 the cached bytes must be served"
    );
}

/// R3.2, suffix form. `bytes=-512` must be forwarded verbatim.
///
/// This is the case a reconstructed header cannot get right: today the code does
/// `format!("bytes={}-{}", range_spec.start, range_spec.end)`, which for a suffix
/// request on a 4096-byte object emits `bytes=3584-4095`. Semantically equivalent
/// against this object, and a different string — which is all that matters once
/// `range` is in `SignedHeaders`.
///
/// Requirements: 3.2, 7.2.
#[tokio::test]
async fn expired_range_get_preserves_raw_suffix_range_on_conditional() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-suffix.bin";

    // bytes=-512 resolves to the final 512 bytes.
    let suffix_len = 512u64;
    let start = OBJECT_SIZE - suffix_len;
    let end = OBJECT_SIZE - 1;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"suffix-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let raw_range = format!("bytes=-{}", suffix_len);
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(suffix_len as usize)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"suffix-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &raw_range,
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);
    assert!(
        !conditionals.is_empty(),
        "R7.1: no conditional was issued for the suffix range. Captured: {:#?}",
        captured
    );
    assert_eq!(
        conditionals[0].headers.get("range").map(String::as_str),
        Some(raw_range.as_str()),
        "R3.2: the suffix Range must be forwarded verbatim, not normalised to \
         absolute offsets. Reconstruction would emit bytes={}-{}",
        start,
        end
    );
}

/// R3.2, open-ended form. `bytes=1024-` must be forwarded verbatim.
///
/// Requirements: 3.2, 7.2.
#[tokio::test]
async fn expired_range_get_preserves_raw_open_ended_range_on_conditional() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-open-ended.bin";
    let start = 1024u64;
    let end = OBJECT_SIZE - 1;
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"open-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let raw_range = format!("bytes={}-", start);
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"open-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &raw_range,
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);
    assert!(
        !conditionals.is_empty(),
        "R7.1: no conditional was issued for the open-ended range. Captured: {:#?}",
        captured
    );
    assert_eq!(
        conditionals[0].headers.get("range").map(String::as_str),
        Some(raw_range.as_str()),
        "R3.2: the open-ended Range must be forwarded verbatim. Reconstruction \
         would emit bytes={}-{}",
        start,
        end
    );
}

/// R3.2, signed form — the case that makes raw-Range preservation mandatory
/// rather than tidy.
///
/// When `range` appears in `SignedHeaders`, rewriting the header invalidates the
/// client's SigV4 signature. The assertion is two-sided so a future change cannot
/// make it vacuous: it asserts the request the code branches on really is signed
/// over `range`, and then that the header was left alone.
///
/// Requirements: 3.2, 7.2.
#[tokio::test]
async fn expired_signed_range_get_preserves_raw_range_on_conditional() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-signed.bin";
    let (start, end) = (256u64, 1279u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"signed-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let raw_range = format!("bytes={}-{}", start, end);
    let authorization = signed_range_authorization();

    // Precondition, asserted rather than assumed: the fixture's Authorization
    // really does sign `range`. If this ever stopped being true the test would
    // silently become the unsigned case, which the tests above already cover.
    let mut client_headers = HashMap::new();
    client_headers.insert("authorization".to_string(), authorization.clone());
    client_headers.insert("range".to_string(), raw_range.clone());
    assert!(
        s3_proxy::signed_request_proxy::is_range_signed(&client_headers),
        "PRECONDITION MISSING: the fixture Authorization header does not sign `range`, \
         so this test would measure the unsigned case"
    );

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"signed-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &raw_range,
            client_headers,
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);
    assert!(
        !conditionals.is_empty(),
        "R7.1: no conditional was issued for the signed range. Captured: {:#?}",
        captured
    );
    let conditional = conditionals[0];
    assert_eq!(
        conditional.headers.get("range").map(String::as_str),
        Some(raw_range.as_str()),
        "R3.2: a Range covered by SignedHeaders must not be rewritten — doing so \
         invalidates the client signature"
    );
    assert_eq!(
        conditional.authorization(),
        Some(authorization.as_str()),
        "the client's Authorization must be forwarded unchanged alongside the \
         proxy-injected validator"
    );
}

/// R3.6: partial coverage is not a complete conditional hit. The existing
/// missing-range/repair machinery owns it, and no cached bytes may be served for
/// the uncovered part.
///
/// This is the negative half of R3: making expired entries discoverable must not
/// turn an incomplete overlap into a serve.
///
/// Requirements: 3.6, 1.6.
#[tokio::test]
async fn expired_range_get_with_partial_coverage_does_not_serve_cached_bytes() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-partial.bin";

    // Cache only the first half of what the client asks for.
    let (req_start, req_end) = (0u64, 1023u64);
    let cached_end = 511u64;
    let req_len = (req_end - req_start + 1) as usize;

    fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(req_start, cached_end)], OBJECT_SIZE, "\"partial-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    // Every upstream answer is fresh bytes, so any OLD byte in the response came
    // from the cache.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(new_bytes(req_len)))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", req_start, req_end, OBJECT_SIZE),
            )
            .with_header("etag", "\"partial-v1\""),
    );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", req_start, req_end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    assert_eq!(
        body.len(),
        req_len,
        "the client must receive the full requested length"
    );
    assert!(
        !body.is_empty(),
        "the response must not be empty for a partially cached range"
    );
    // The uncovered tail can only have come from S3.
    assert_eq!(
        body[req_len - 1],
        NEW_FILL,
        "R3.6: the uncovered tail must be fetched, not fabricated from cache"
    );
}

// =====================================================================
// Requirement 2 — full-object path
// =====================================================================

/// R7.1, full-object counterpart: a Stored_Expired, Live_Expired,
/// Candidate_Available entry must reach conditional validation on an ordinary
/// full-object GET.
///
/// Driven through `handle_range_request` with the full object as the requested
/// range, and with the early full-object shortcut left **enabled** — that
/// shortcut is the range path's own full-object lookup, so this exercises the
/// full-object overlap decision without needing a `Request<Incoming>`. The
/// `handle_get_head_request` entry point cannot be called in-process (it takes a
/// real `Incoming` body, which has no public constructor); `conditional_range_caching_test.rs`
/// records the same constraint. The fleet group in task 9 covers that entry.
///
/// Requirements: 2.1, 2.2, 2.6, 7.1, 7.3.
#[tokio::test]
async fn expired_full_object_get_issues_conditional_with_validator() {
    let config = test_config_full_object_checks(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-full-object.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);
    let len = OBJECT_SIZE as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"full-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"full-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    assert!(
        !conditionals.is_empty(),
        "R7.1: no conditional upstream request was issued for a Stored_Expired \
         full-object entry. Captured requests: {:#?}",
        captured
    );
    assert_eq!(
        conditionals[0].if_none_match(),
        Some(etag.as_str()),
        "R2.1: the conditional must carry the cached ETag"
    );
    assert_eq!(
        body,
        old_bytes(len),
        "R2.2: after 304 the complete cached object must be served"
    );
}

/// R2.4 / R7.3: a changed object must not serve the expired bytes, and the
/// validator must be present on the request that produced that verdict.
///
/// The validator assertion is what stops a plain miss from passing: without it,
/// today's defective path forwards, gets fresh bytes and looks correct.
///
/// Requirements: 2.4, 7.3.
#[tokio::test]
async fn expired_range_get_changed_object_serves_fresh_bytes_with_validator_present() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-changed.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"changed-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    let changed = StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(new_bytes(len)))
        .with_header(
            "content-range",
            format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
        )
        .with_header("etag", "\"changed-v2\"");

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), changed.clone())
        .with_default(changed);

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    assert!(
        !conditionals.is_empty(),
        "R7.3: the changed-object verdict must come from a request that carried the \
         cached validator. Without one this test would pass on a plain miss and \
         prove nothing about the branch. Captured requests: {:#?}",
        captured
    );
    assert_eq!(
        conditionals[0].if_none_match(),
        Some(etag.as_str()),
        "the validator on the deciding request must be the cached ETag"
    );
    assert_eq!(
        body,
        new_bytes(len),
        "R2.4: a changed representation must be returned, never the expired bytes"
    );
}

/// R2.5 / R7.3: an authorization error must not fail open to expired data, and
/// the validator must be present on the deciding request.
///
/// Requirements: 2.5, 7.3.
#[tokio::test]
async fn expired_range_get_authorization_error_does_not_serve_expired_bytes() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-auth-error.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"auth-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    let forbidden = StubResponse::with_status(StatusCode::FORBIDDEN).with_body(Bytes::from_static(
        b"<Error><Code>AccessDenied</Code></Error>",
    ));
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), forbidden.clone())
        .with_default(forbidden);

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();

    assert!(
        !conditional_requests(&captured).is_empty(),
        "R7.3: the authorization verdict must come from a request that carried the \
         cached validator. Captured requests: {:#?}",
        captured
    );
    assert_ne!(
        body,
        old_bytes(len),
        "R2.5: a 403 must not fail open to expired cached data"
    );
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "R2.5: the client must see the authorization error"
    );
}

/// R7.3 / R1.6: metadata claims coverage but the range file is gone. No cached
/// bytes may be served, whatever the conditional says.
///
/// This is the case that makes "expired entries are discoverable" safe: a
/// candidate is only a candidate while its data exists.
///
/// Requirements: 1.6, 7.3.
#[tokio::test]
async fn expired_range_get_with_missing_range_file_does_not_serve_cached_bytes() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-missing-bin.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"missing-v1\""),
    );

    // Delete the .bin but LEAVE the metadata entry claiming coverage. Removing
    // the metadata entry too would make coverage incomplete and reclassify the
    // request as an ordinary miss, which is a different path — the same trap
    // recorded for the T45 fleet group.
    let bin = fixture.bin_path(cache_key, start, end);
    assert!(bin.exists(), "PRECONDITION MISSING: fixture .bin absent");
    std::fs::remove_file(&bin).expect("remove .bin");
    fixture.invalidate_metadata_cache(cache_key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"missing-v1\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    assert_ne!(
        body,
        old_bytes(len),
        "R1.6: with the range file gone the proxy must not serve bytes it cannot prove \
         it has, even after a 304"
    );
}

/// R2.3 / R7.9 companion: a `304` authorises exactly one Validated_Serve, and
/// the next request revalidates again rather than treating a failed or absent
/// TTL persistence as durable.
///
/// Two sequential expired GETs; both must produce a conditional. Task 7 owns the
/// write-failure variant, which needs a read-only `.meta`.
///
/// Requirements: 2.3, 2.6.
#[tokio::test]
async fn zero_ttl_revalidates_on_every_sequential_range_read() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-every-read.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"every-v1\""),
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
                .with_header("etag", "\"every-v2\""),
        );

    for pass in 1..=2 {
        fixture.invalidate_metadata_cache(cache_key).await;
        let response = fixture
            .range_get(
                cache_key,
                &raw_range,
                HashMap::new(),
                &zero_ttl(),
                fixture.production_current_etag(cache_key),
                stub.clone().into_trait_object(),
            )
            .await;
        let body = body_of(response).await;
        assert_eq!(
            body,
            old_bytes(len),
            "R2.2: pass {} must serve the cached bytes after 304",
            pass
        );
    }

    let conditionals = conditional_requests(&stub.captured()).len();
    assert!(
        conditionals >= 2,
        "R2.6: with get_ttl: 0 EVERY sequential re-read must revalidate; observed {} \
         conditional requests across 2 passes",
        conditionals
    );
}

/// R2.6's other half, and the customer's actual complaint: an unchanged object
/// must not transfer its body.
///
/// Asserted on the upstream requests rather than on elapsed time or on the
/// response, because "no body transfer" is a statement about what went to S3. A
/// conditional that returns `304` has no body; a miss forwards and receives one.
///
/// Requirements: 2.6, 3.1.
#[tokio::test]
async fn unchanged_expired_range_read_transfers_no_body_from_s3() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-no-body.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"nobody-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    // The fall-through returns a full body. If the proxy takes it, the count of
    // body-bearing responses is non-zero and this test fails — which is exactly
    // the wasted transfer the customer is blocked on.
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"nobody-v1\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let body = body_of(response).await;
    let captured = stub.captured();

    let unconditional: Vec<_> = captured
        .iter()
        .filter(|r| r.if_none_match().is_none() && r.if_modified_since().is_none())
        .collect();

    assert_eq!(
        body,
        old_bytes(len),
        "R2.2: the cached bytes must be served after 304"
    );
    assert!(
        unconditional.is_empty(),
        "R2.6: an unchanged object must avoid a full body transfer, but {} \
         unconditional upstream request(s) were made: {:#?}",
        unconditional.len(),
        unconditional
    );
}

/// R4.1: the range path's early full-object shortcut can direct-serve with no
/// live-TTL check at all.
///
/// Separate from everything above because it is a *different* defect in the same
/// function, and one the spec calls out explicitly: the shortcut is bounded only
/// by stored expiry, so a stored-FRESH entry is served even when the currently
/// resolved `get_ttl` says it must be revalidated. That is a silent
/// configuration failure — the operator sets `get_ttl: 0` and it does nothing on
/// this path.
///
/// The fixture is stored-fresh and live-expired, with the shortcut **enabled**.
/// A conditional request must still be issued.
///
/// Requirements: 4.1, 4.2, 4.3.
#[tokio::test]
async fn range_paths_early_full_object_shortcut_must_honour_live_ttl() {
    let config = test_config_full_object_checks(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/early-shortcut-live-ttl.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);

    // Stored-FRESH so the lookup returns the extents and the shortcut is
    // reachable; live-EXPIRED so the resolved get_ttl demands revalidation.
    let etag = fixture.seed(
        cache_key,
        &SeedSpec::stored_fresh_live_expired(vec![(start, end)], OBJECT_SIZE, "\"shortcut-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(256)))
                .with_header("content-range", format!("bytes 0-255/{}", OBJECT_SIZE))
                .with_header("etag", "\"shortcut-v2\""),
        );

    // A sub-range, so the shortcut's "full object can serve this range" arm is
    // the one taken.
    let response = fixture
        .range_get(
            cache_key,
            "bytes=0-255",
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let captured = stub.captured();
    assert!(
        !conditional_requests(&captured).is_empty(),
        "R4.1/R4.3: the early full-object shortcut direct-served a stored-fresh entry \
         without consulting the currently resolved get_ttl, so get_ttl: 0 had no effect \
         on this path. The live-TTL verdict must be authoritative on mainline GETs. \
         Captured requests: {:#?}",
        captured
    );
}

/// Guard for R1.3: making expired entries discoverable must not weaken the
/// **part-scoped** serve path, which has no live-TTL gate at all today.
///
/// `CacheManager::lookup_part` calls `DiskCacheManager::find_cached_ranges`
/// directly with no `current_etag` and no `check_object_expiration`, so its only
/// freshness bound is stored expiry. This test pins that bound so a
/// `RevalidationCandidate` default could not silently reach it: a Stored_Expired
/// entry must remain a part-cache miss.
///
/// This documents existing behaviour rather than a defect. `lookup_part` ignoring
/// `get_ttl` entirely is a separate invariant-1 gap, recorded in
/// `.kiro/steering/cache-coherency-invariants.md` and out of scope here.
///
/// Requirements: 1.3, 1.6.
#[tokio::test]
async fn part_scoped_lookup_remains_fresh_only_for_expired_entries() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-part-lookup.bin";
    let (start, end) = (0u64, 1023u64);

    let mut spec = SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"part-v1\"");
    spec.extents = vec![(start, end)];
    fixture.seed(cache_key, &spec);

    // Register part 1 as covering the cached extent, so the only thing standing
    // between the lookup and a serve is the stored-expiry check.
    let meta_path = fixture.meta_path(cache_key);
    let mut metadata = fixture.read_meta(cache_key).expect(".meta");
    metadata.object_metadata.parts_count = Some(1);
    metadata.object_metadata.part_ranges.insert(1, (start, end));
    std::fs::write(
        &meta_path,
        serde_json::to_string_pretty(&metadata).expect("serialize"),
    )
    .expect("write .meta");
    fixture.invalidate_metadata_cache(cache_key).await;

    let cached_part = fixture
        .cache_manager
        .lookup_part(cache_key, 1)
        .await
        .expect("lookup_part must not error");

    assert!(
        cached_part.is_none(),
        "R1.3: the part-scoped lookup is Fresh_Only — a Stored_Expired entry must stay a \
         miss there, because that path applies no live-TTL gate and no conditional \
         validation before serving"
    );
}

/// Sanity: an entry that is neither stored- nor live-expired is served from cache
/// with no upstream request at all.
///
/// The two-sided half of every test above. Without it they could all be satisfied
/// by a proxy that revalidated unconditionally, which would be a different defect
/// with the same test results.
///
/// Requirements: 1.1, 1.2.
#[tokio::test]
async fn fresh_range_entry_is_served_from_cache_without_any_upstream_request() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/fresh-range.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let mut spec =
        SeedSpec::stored_fresh_live_expired(vec![(start, end)], OBJECT_SIZE, "\"fresh-v1\"");
    // Young enough that a 60s get_ttl leaves it live-FRESH too.
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(cache_key, &spec);
    fixture.invalidate_metadata_cache(cache_key).await;

    // Any upstream call at all fails the test, so the stub is given a response
    // that would be wrong if used.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(new_bytes(len)))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
            )
            .with_header("etag", "\"fresh-v2\""),
    );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &short_ttl(),
            Some(etag),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    let captured = stub.captured();

    assert_eq!(
        body,
        old_bytes(len),
        "a fresh entry must be served from cache"
    );
    assert!(
        captured.is_empty(),
        "R1.1/R1.2: a fresh entry must not be revalidated; {} upstream request(s) were \
         made: {:#?}",
        captured.len(),
        captured
    );
}

// =====================================================================
// Task 7 — coordination and failure boundaries
// =====================================================================

/// R4.6 / R7.8: coordination still holds on the newly-reachable path.
///
/// # What this adds over the existing coordination suite
///
/// `download_coordination_property_test.rs` already proves the one-authoritative-
/// fetcher boundary and that waiter conditionals are well formed. Those tests are
/// not re-derived here. What they could not cover is this path *at all*: before the
/// fix, a Stored_Expired entry produced an empty overlap, so the expired-revalidation
/// branch — and the `InFlightTracker` registration inside it — were unreachable.
/// Coordination on a Stored_Expired candidate is therefore new surface, and this
/// asserts the two properties that matter for it.
///
/// Requirements: 4.6, 7.8, 2.6.
#[tokio::test]
async fn concurrent_expired_range_reads_serve_cached_bytes_with_no_body_transfer() {
    let config = test_config_coordinated(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-coordinated.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"coord-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let raw_range = format!("bytes={}-{}", start, end);

    // The delay is load-bearing, not padding. The stub is in-process, so without it
    // the fetcher's flight can open and close before the second request is even
    // scheduled — the late arrival then correctly elects itself a new fetcher and
    // the test measures two independent requests rather than coordination. See
    // `StubResponse::with_delay`'s docs.
    let stub = StubS3Client::new()
        .with_response_for_etag(
            etag.clone(),
            not_modified(&etag).with_delay(Duration::from_millis(300)),
        )
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(new_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"coord-v2\""),
        );

    let resolved = zero_ttl();
    let (first, second) = tokio::join!(
        fixture.range_get(
            cache_key,
            &raw_range,
            HashMap::new(),
            &resolved,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        ),
        fixture.range_get(
            cache_key,
            &raw_range,
            HashMap::new(),
            &resolved,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
    );

    let first_body = body_of(first).await;
    let second_body = body_of(second).await;
    let captured = stub.captured();

    assert!(
        !conditional_requests(&captured).is_empty(),
        "R4.6: at least one authoritative conditional must be issued. Captured: {:#?}",
        captured
    );

    // The headline property, and the one the customer cares about: an unchanged
    // object costs no body transfer no matter how many readers arrive at once.
    let unconditional: Vec<_> = captured
        .iter()
        .filter(|r| r.if_none_match().is_none() && r.if_modified_since().is_none())
        .collect();
    assert!(
        unconditional.is_empty(),
        "R7.8: an unchanged coordinated read must transfer no body, but {} \
         unconditional upstream request(s) were made: {:#?}",
        unconditional.len(),
        unconditional
    );

    // Both participants get correct bytes. Asserted for BOTH, because a waiter
    // taking a different serve path from the fetcher is exactly the asymmetry
    // coordination bugs produce.
    assert_eq!(
        first_body,
        old_bytes(len),
        "the fetcher must serve the cached bytes after 304"
    );
    assert_eq!(
        second_body,
        old_bytes(len),
        "the waiter must serve the same cached bytes as the fetcher"
    );
}

/// R2.3: a `304` authorises exactly ONE Validated_Serve. If the refreshed TTL is not
/// durable, the next request revalidates again rather than serving on the strength of
/// a refresh that did not land.
///
/// # Why this is testable without forcing a write to fail
///
/// `refresh_object_ttl` records a TTL-refresh **journal entry** rather than writing
/// the `.meta`, so the persisted `expires_at` does not move until the background
/// consolidator applies it. Inside that window the refresh is exactly as
/// non-durable as a failed write, and it occurs naturally on every `304` — no
/// fault injection, no read-only filesystem, no platform-specific permission
/// trickery.
///
/// This is the case R2.3's "SHALL revalidate again on the next request rather than
/// treating the failed refresh as durable" is written for, and it uses a NON-ZERO
/// TTL deliberately: with `get_ttl: 0` every read revalidates regardless, so a
/// zero-TTL version of this test would pass whatever the refresh did and prove
/// nothing about durability.
///
/// Requirements: 2.3, 7.9.
#[tokio::test]
async fn nonzero_ttl_second_read_revalidates_again_while_refresh_is_undurable() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-undurable-refresh.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"undurable-v1\""),
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
                .with_header("etag", "\"undurable-v2\""),
        );

    for pass in 1..=2 {
        fixture.invalidate_metadata_cache(cache_key).await;
        let response = fixture
            .range_get(
                cache_key,
                &raw_range,
                HashMap::new(),
                &short_ttl(),
                fixture.production_current_etag(cache_key),
                stub.clone().into_trait_object(),
            )
            .await;
        assert_eq!(
            body_of(response).await,
            old_bytes(len),
            "R2.2: pass {} must serve the cached bytes after 304",
            pass
        );
    }

    // Two passes, two conditionals. One would mean the second read served on the
    // strength of a refresh that is not yet on disk.
    let conditionals = conditional_requests(&stub.captured()).len();
    assert!(
        conditionals >= 2,
        "R2.3: a 304 authorises ONE Validated_Serve; with the refreshed TTL not yet \
         persisted the next request must revalidate again. Observed {} conditional \
         request(s) across 2 passes",
        conditionals
    );

    // The `.meta` still carries the pre-refresh expiry, which is what makes the
    // second revalidation correct rather than wasteful. Asserted so a future change
    // that makes the refresh synchronous fails HERE, naming the reason, instead of
    // making the count assertion above mysteriously flaky.
    let metadata = fixture.read_meta(cache_key).expect(".meta must exist");
    assert!(
        std::time::SystemTime::now() > metadata.expires_at,
        "the persisted expires_at is expected to be UNCHANGED inside the \
         consolidation window — if the TTL refresh has become synchronous, this test's \
         premise no longer holds and it needs rewriting against a real write failure"
    );
}

/// R2.5 / R7.8: the stale-if-error exception does not extend to this path.
///
/// The coordinated waiter path deliberately permits a stale serve after a transport
/// failure or a retryable upstream `5xx` — an availability trade-off this spec
/// preserves and does not broaden. This asserts the boundary from the other side: on
/// the ordinary inline revalidation path, a `5xx` returns fresh-or-error and never
/// the expired bytes.
///
/// Written to fail if the exception broadens. A future change that added a
/// stale-on-error fallback here would turn this red, which is the point.
///
/// Requirements: 2.5, 7.8.
#[tokio::test]
async fn inline_revalidation_does_not_serve_stale_on_upstream_5xx() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/expired-range-5xx.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"fivexx-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    // A retryable 5xx to the conditional AND to any follow-up, so the request cannot
    // succeed by any route. Anything other than an error response means expired
    // bytes were served.
    let server_error = StubResponse::with_status(StatusCode::SERVICE_UNAVAILABLE)
        .with_body(Bytes::from_static(b"<Error><Code>SlowDown</Code></Error>"));
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), server_error.clone())
        .with_default(server_error);

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();

    assert!(
        !conditional_requests(&captured).is_empty(),
        "the 5xx verdict must come from a request that carried the cached validator. \
         Captured: {:#?}",
        captured
    );
    assert_ne!(
        body,
        old_bytes(len),
        "R2.5: the inline revalidation path must not serve expired bytes after an \
         upstream 5xx. The coordinated waiter path permits that as a documented \
         availability exception; this path is not it, and this spec does not broaden \
         the exception. Status returned: {}",
        status
    );
    assert!(
        status.is_server_error() || status.is_client_error(),
        "R2.5: an unrecoverable upstream error must reach the client as an error, \
         got {} with {} bytes",
        status,
        body.len()
    );
}
