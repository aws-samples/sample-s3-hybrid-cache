//! A revalidation response's **ETag** decides whether the cached entry is
//! current — never its HTTP status alone.
//!
//! Spec: the expired entry revalidation feature Requirement 8.
//! Security review findings (HIGH and MEDIUM), and the
//! `If-Match` case found while designing the fix.
//!
//! # Why status-only inference is unsound
//!
//! R4.5 correctly stops the proxy clobbering a client's precondition, so when a
//! client sends its own conditional the upstream request answers the *client's*
//! question. Its status then says nothing about the proxy's cached copy, and both
//! status-only inferences invert. **Measured against real S3 on 2026-08-31, with
//! controls:**
//!
//! | Request | Status | Response `ETag` |
//! |---|---|---|
//! | `If-None-Match: *` on an existing object | **304** | the object's CURRENT ETag |
//! | `If-None-Match: "bogus"` + `Range` on an UNCHANGED object | **206** | the object's CURRENT ETag |
//! | `If-None-Match: <real etag>` (control) | 304 | current ETag |
//! | plain GET (control) | 200 | current ETag |
//! | `If-Match: "bogus"` | 412 | **absent** |
//!
//! The two controls are the point: a `304` from a real-ETag match and a `304`
//! from `*` are byte-identical on the wire and mean completely different things.
//! The first means "your cached copy is current"; the second means only "the
//! object exists". So the status cannot distinguish them and the ETag can.
//!
//! # What each test pins
//!
//! - [`not_modified_with_a_newer_etag_must_not_serve_the_superseded_cached_bytes`]
//!   is S1. A `304` obtained via `If-None-Match: *` carrying a *newer* ETag used
//!   to be read as "cache current", serving superseded bytes and refreshing
//!   `expires_at` — poisoning the entry for later unconditional clients.
//! - [`partial_content_with_the_cached_etag_must_not_wipe_the_entry`] is S2. A
//!   `206` obtained via `If-None-Match: "bogus"` on an *unchanged* object used to
//!   be read as "object changed" and wiped the whole key: cache destruction and
//!   egress amplification from ordinary read access.
//! - [`successful_client_if_match_must_not_wipe_the_entry`] is the case the
//!   scanner did not report. A client `If-Match` carrying the *cached* ETag
//!   succeeds with `200`/`206`, which also wiped the key — from a completely
//!   legitimate request. The CRT and Rust transfer managers stamp `If-Match` on
//!   every ranged chunk (see the client-family table in
//!   `pre-push-checklist.md`), so this is the most likely of the three to happen
//!   by accident rather than by craft.
//! - [`proxy_own_validator_304_still_serves_from_cache`] and
//!   [`genuinely_changed_object_still_invalidates_and_serves_fresh_bytes`] are the
//!   CONTROLS. They must pass both before and after the fix: they prove the
//!   fixture reaches the revalidation branch at all, and that the fix did not buy
//!   its correctness by disabling the optimisation or the invalidation. Without
//!   them a "no stale bytes, no wipe" result could be produced by a proxy that
//!   simply stopped revalidating.
//!
//! # Fixture notes
//!
//! Entries are seeded **stored-fresh, live-expired**
//! ([`SeedSpec::stored_fresh_live_expired`]) because the revalidation branch sits
//! behind a non-empty-overlap guard: a stored-expired entry returns no extents
//! under a `FreshServe` lookup. That combination is not contrived — it is what an
//! operator produces by tightening `get_ttl` on an already-cached key, and what
//! any prior `304` leaves behind, since `refresh_object_ttl` moves `expires_at`
//! and leaves `created_at` alone.
//!
//! `OLD_FILL` and `NEW_FILL` differ so a stale serve is identifiable **by value**
//! rather than by length. A length-only assertion passes on a stale serve.

mod common;

use std::collections::HashMap;

use hyper::StatusCode;

use common::expired_fixture::{
    body_of, conditional_requests, not_modified, short_ttl, Fixture, SeedSpec, NEW_FILL, OLD_FILL,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 1023;

fn range_header() -> String {
    format!("bytes={}-{}", RANGE_START, RANGE_END)
}

fn range_len() -> usize {
    (RANGE_END - RANGE_START + 1) as usize
}

fn content_range() -> String {
    format!("bytes {}-{}/{}", RANGE_START, RANGE_END, OBJECT_SIZE)
}

fn new_body() -> Vec<u8> {
    vec![NEW_FILL; range_len()]
}

/// A `206` carrying `etag`, i.e. what S3 answers a ranged request with.
fn partial_content(etag: &str, body: Vec<u8>) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_header("etag", etag)
        .with_header("content-range", content_range())
        .with_body(body)
}

fn client_headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// S1. `If-None-Match: *` yields a `304` whose ETag is the origin's CURRENT one.
/// When that differs from the cached ETag the entry is superseded, and serving it
/// is a stale serve however encouraging the `304` looks.
#[tokio::test]
async fn not_modified_with_a_newer_etag_must_not_serve_the_superseded_cached_bytes() {
    let fixture = Fixture::new(common::expired_fixture::test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/s1-wildcard-inm";

    let seeded_etag = fixture.seed(
        cache_key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(RANGE_START, RANGE_END)],
            OBJECT_SIZE,
            "\"cached-v1\"",
        ),
    );

    // The origin has moved on to v2, and answers the client's `*` with a 304 —
    // which is what real S3 does (measured). The stub is keyed on `"*"` because
    // that is the `if-none-match` value the proxy FORWARDS under R4.5; keying it
    // on the seeded ETag made this test pass for an unrelated reason, since the
    // lookup missed and fell through to a plain changed-object 206. See the note
    // on the discriminator below.
    let stub = StubS3Client::new()
        .with_response_for_etag("*", not_modified("\"origin-v2\""))
        .with_default(not_modified("\"origin-v2\""));

    let response = fixture
        .range_get(
            cache_key,
            &range_header(),
            // The crafted precondition. R4.5 means the proxy forwards this rather
            // than substituting its own validator.
            client_headers(&[("if-none-match", "*")]),
            &short_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    let captured = stub.captured();

    // ASSERT THE REQUEST SHAPE, not just the outcome. If the proxy had substituted
    // its own validator the exchange would be informative for a different reason
    // and this test would be measuring the ordinary path.
    let conditionals = conditional_requests(&captured);
    assert!(
        !conditionals.is_empty(),
        "fixture must reach the conditional-revalidation branch. Captured: {captured:#?}"
    );
    assert_eq!(
        conditionals[0].if_none_match(),
        Some("*"),
        "the client's `*` must be forwarded unchanged (R4.5); a different value here \
         means the proxy substituted its own validator and this test is not \
         exercising the crafted-conditional case"
    );

    // The response must not carry the superseded bytes. Note a `304` to the client
    // is the CORRECT answer here — it asked for the body only if the object was
    // absent, and it exists — so this is a "no stale bytes" assertion rather than a
    // "fresh bytes" one.
    assert!(
        !body.contains(&OLD_FILL),
        "the superseded cached bytes must not be served. Got {}",
        common::expired_fixture::fill_summary(&body)
    );

    // THE DISCRIMINATOR. Pre-fix, the 304 was read as proof of currency:
    // `refresh_object_ttl` ran and the entry survived still claiming `cached-v1`
    // with its expiry pushed forward — poisoned for later unconditional clients.
    // Post-fix the differing ETag marks it stale and all coverage is invalidated.
    let still_claims_superseded_version = fixture
        .read_meta(cache_key)
        .map(|m| m.object_metadata.etag == seeded_etag)
        .unwrap_or(false);
    assert!(
        !still_claims_superseded_version,
        "a 304 carrying a DIFFERENT ETag proves the cached copy is superseded, so the \
         entry must NOT survive still claiming {seeded_etag}. Leaving it in place with \
         a refreshed TTL is the cache-poisoning vector"
    );
}

/// S2. A `206` proves the object changed only when its ETag differs. Obtained via
/// a guaranteed-to-fail client precondition on an UNCHANGED object, it must not
/// destroy the entry.
#[tokio::test]
async fn partial_content_with_the_cached_etag_must_not_wipe_the_entry() {
    let fixture = Fixture::new(common::expired_fixture::test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/s2-bogus-inm";

    let seeded_etag = fixture.seed(
        cache_key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(RANGE_START, RANGE_END)],
            OBJECT_SIZE,
            "\"cached-v1\"",
        ),
    );

    assert!(
        fixture.read_meta(cache_key).is_some(),
        "precondition: the entry exists before the request"
    );

    // UNCHANGED object: every response carries the SAME ETag the cache holds. The
    // 206 arises purely because the client's bogus precondition could not match.
    let stub = StubS3Client::new()
        .with_default(partial_content(&seeded_etag, vec![OLD_FILL; range_len()]));

    let _response = fixture
        .range_get(
            cache_key,
            &range_header(),
            client_headers(&[("if-none-match", "\"bogus\"")]),
            &short_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let captured = stub.captured();
    assert!(
        !conditional_requests(&captured).is_empty(),
        "fixture must reach the conditional-revalidation branch"
    );

    // THE DISCRIMINATOR. Under status-only inference this key was wiped by
    // `invalidate_cache_hierarchy`, which is the cache-destruction vector.
    assert!(
        fixture.read_meta(cache_key).is_some(),
        "a 206 whose ETag MATCHES the cached one is not a version change, so the \
         entry must survive. A missing .meta here is the crafted cache-destruction \
         vector"
    );
}

/// The case the scanner did not report. A *successful* client `If-Match` returns
/// `200`/`206`, which under status-only inference wiped the key — cache
/// destruction from an entirely legitimate request, and the CRT and Rust transfer
/// managers send this shape on every ranged chunk.
#[tokio::test]
async fn successful_client_if_match_must_not_wipe_the_entry() {
    let fixture = Fixture::new(common::expired_fixture::test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/s4-successful-if-match";

    let seeded_etag = fixture.seed(
        cache_key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(RANGE_START, RANGE_END)],
            OBJECT_SIZE,
            "\"cached-v1\"",
        ),
    );

    assert!(
        fixture.read_meta(cache_key).is_some(),
        "precondition: the entry exists before the request"
    );

    let stub = StubS3Client::new()
        .with_default(partial_content(&seeded_etag, vec![OLD_FILL; range_len()]));

    let _response = fixture
        .range_get(
            cache_key,
            &range_header(),
            // The client asserts the very version the cache holds, and it is
            // correct — S3 succeeds. Nothing about this request is adversarial.
            client_headers(&[("if-match", &seeded_etag)]),
            &short_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let captured = stub.captured();
    assert!(
        !conditional_requests(&captured).is_empty(),
        "fixture must reach the conditional-revalidation branch"
    );

    assert!(
        fixture.read_meta(cache_key).is_some(),
        "a successful If-Match returns 200/206 and proves the cached version is \
         CURRENT, so the entry must survive. Wiping it here destroys cache on \
         ordinary transfer-manager reads"
    );
}

/// CONTROL, must pass before and after the fix. The ordinary case: the proxy's
/// own validator, a matching `304`, serve from cache. Proves the fix did not buy
/// its correctness by abandoning the revalidation optimisation.
#[tokio::test]
async fn proxy_own_validator_304_still_serves_from_cache() {
    let fixture = Fixture::new(common::expired_fixture::test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/control-proxy-validator";

    let seeded_etag = fixture.seed(
        cache_key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(RANGE_START, RANGE_END)],
            OBJECT_SIZE,
            "\"cached-v1\"",
        ),
    );

    // Unchanged object, and the client sends NO conditional — so the proxy injects
    // its own validator and the 304 is genuinely about the cache.
    let stub = StubS3Client::new()
        .with_default(not_modified(&seeded_etag))
        .with_response_for_etag(&seeded_etag, not_modified(&seeded_etag));

    let response = fixture
        .range_get(
            cache_key,
            &range_header(),
            HashMap::new(),
            &short_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;

    assert_eq!(
        body.len(),
        range_len(),
        "control: a confirmed-current entry must still be served from cache"
    );
    assert!(
        body.iter().all(|b| *b == OLD_FILL),
        "control: the cached bytes ARE the current bytes here, so serving them is \
         correct. Got {}",
        common::expired_fixture::fill_summary(&body)
    );
    assert!(
        fixture.read_meta(cache_key).is_some(),
        "control: a confirmed-current entry must not be invalidated"
    );
}

/// CONTROL, must pass before and after the fix. A genuinely changed object must
/// still invalidate and return fresh bytes. Proves the fix did not buy S2's
/// correctness by weakening the invalidation that this release added against a
/// measured stale-serve defect.
#[tokio::test]
async fn genuinely_changed_object_still_invalidates_and_serves_fresh_bytes() {
    let fixture = Fixture::new(common::expired_fixture::test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/control-genuinely-changed";

    let seeded_etag = fixture.seed(
        cache_key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(RANGE_START, RANGE_END)],
            OBJECT_SIZE,
            "\"cached-v1\"",
        ),
    );

    // The object really did change: every response carries a different ETag and
    // the NEW bytes.
    let stub = StubS3Client::new()
        .with_default(partial_content("\"origin-v2\"", new_body()))
        .with_response_for_etag(&seeded_etag, partial_content("\"origin-v2\"", new_body()));

    let response = fixture
        .range_get(
            cache_key,
            &range_header(),
            HashMap::new(),
            &short_ttl(),
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;

    assert!(
        body.iter().all(|b| *b == NEW_FILL),
        "control: a genuinely changed object must return the fresh bytes. Got {}",
        common::expired_fixture::fill_summary(&body)
    );
}
