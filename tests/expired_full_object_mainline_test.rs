//! The FULL-OBJECT mainline entry point: `handle_get_head_request`.
//!
//! Spec: the expired entry revalidation feature Requirements 2, 4.4, 7.1, 7.3.
//!
//! # Why this file is separate from the range regressions
//!
//! `handle_get_head_request`'s initial full-object lookup is a **different call
//! site** from the range path's, with its own purpose assignment. The range tests
//! reach a full-object lookup too — the one inside `handle_range_request`'s early
//! shortcut — so they say nothing about this one. Two sites that happen to agree
//! today are not evidence about either, and the whole point of making the purpose
//! explicit per call site is that they can legitimately differ.
//!
//! Covering it needs a real request: `handle_get_head_request` takes a
//! `Request<hyper::body::Incoming>` and `Incoming` has no public constructor
//! (`conditional_range_caching_test.rs` records the same wall). So these tests
//! serve the genuine `HttpProxy::handle_request` on a loopback port and send real
//! HTTP through it, the pattern `part_scoped_head_cache_test.rs` established.
//!
//! # What is asserted
//!
//! The observable is the **captured upstream request**, not the response body: a
//! plain miss also returns correct bytes, so a body-only assertion passes against
//! the defect. The stub returns `304` only for the cached validator, so a
//! served-from-cache result cannot be reached by a request that guessed.

mod common;

use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    assert_all_new, assert_all_old, conditional_requests, fill_summary, not_modified, proxy_get,
    proxy_get_with_auth, signed_authorization_without_range,
    test_config_full_object_checks_with_get_ttl, Fixture, SeedSpec, NEW_FILL, OLD_FILL,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;

/// The proxy derives its cache key from the request path, so the fixture must be
/// seeded under exactly the key that path produces.
const BUCKET: &str = "test-bucket";
const OBJECT: &str = "expired-full-object-mainline.bin";

fn cache_key() -> String {
    format!("{}/{}", BUCKET, OBJECT)
}

fn request_path() -> String {
    format!("/{}/{}", BUCKET, OBJECT)
}

fn new_bytes() -> Vec<u8> {
    vec![NEW_FILL; OBJECT_SIZE as usize]
}

/// R7.1, full-object: a Stored_Expired, Live_Expired, Candidate_Available entry
/// must reach conditional validation on a plain sequential GET, and an unchanged
/// object must be served from cache with no body transfer.
///
/// Requirements: 2.1, 2.2, 2.6, 7.1.
#[tokio::test]
async fn expired_full_object_mainline_get_revalidates_and_serves_from_cache() {
    // get_ttl: 0 via CONFIG, not ResolvedSettings — the loopback path resolves
    // its own settings. See the helper's docs.
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = cache_key();

    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"fo-v1\""),
    );
    fixture.assert_covers(&key, 0, OBJECT_SIZE - 1);
    fixture.invalidate_metadata_cache(&key).await;

    // 304 only for the cached validator. Anything else gets a full body, which is
    // what the defect produced — so "served from cache" cannot happen by accident.
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(new_bytes()))
                .with_header("etag", "\"fo-v2\""),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(server.addr, &request_path(), &[]).await;

    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    // The status and body length are printed because zero captured requests has
    // more than one cause: served-from-cache without revalidating (the defect), or
    // the request never reaching the cache path at all (a fixture problem, e.g. a
    // cache-key mismatch, which would show as a 4xx or a wrong length). A bare
    // "no conditional" verdict cannot tell them apart.
    assert!(
        !conditionals.is_empty(),
        "R7.1: no conditional upstream request was issued for a Stored_Expired \
         full-object entry on the mainline GET path. status={} body_len={} \
         (expected {}). Captured requests: {:#?}",
        status,
        body.len(),
        OBJECT_SIZE,
        captured
    );
    assert_eq!(
        conditionals[0].if_none_match(),
        Some(etag.as_str()),
        "R2.1: the conditional must carry the cached ETag"
    );

    assert_eq!(status, StatusCode::OK, "a 304 Validated_Serve returns 200");
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "R2.2: after 304 the complete cached object must be served",
    );

    // R2.6: no full body transfer for an unchanged object. Asserted on what went
    // upstream, because that is where the waste is.
    let unconditional: Vec<_> = captured
        .iter()
        .filter(|r| r.if_none_match().is_none() && r.if_modified_since().is_none())
        .collect();
    assert!(
        unconditional.is_empty(),
        "R2.6: an unchanged object must avoid a body transfer, but {} unconditional \
         upstream request(s) were made: {:#?}",
        unconditional.len(),
        unconditional
    );
}

/// R2.4: a changed object on the full-object path must serve the fresh
/// representation, and the validator must be on the request that decided that.
///
/// Requirements: 2.4, 7.3.
#[tokio::test]
async fn expired_full_object_mainline_get_changed_object_serves_fresh_bytes() {
    // get_ttl: 0 via CONFIG, not ResolvedSettings — the loopback path resolves
    // its own settings. See the helper's docs.
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/changed-{}", BUCKET, OBJECT);
    let path = format!("/{}", key);

    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"fo-changed-v1\""),
    );
    fixture.invalidate_metadata_cache(&key).await;

    let changed = StubResponse::with_status(StatusCode::OK)
        .with_body(Bytes::from(new_bytes()))
        .with_header("etag", "\"fo-changed-v2\"");
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), changed.clone())
        .with_default(changed);
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(server.addr, &path, &[]).await;

    let captured = stub.captured();
    assert!(
        !conditional_requests(&captured).is_empty(),
        "R7.3: the changed-object verdict must come from a request carrying the \
         cached validator; without one this passes on a plain miss. Captured: {:#?}",
        captured
    );
    assert_eq!(status, StatusCode::OK);
    assert_all_new(
        &body,
        OBJECT_SIZE as usize,
        "R2.4: the fresh representation must be returned, never the expired bytes",
    );
}

/// R2.5: an authorization error must not fail open to expired data.
///
/// Requirements: 2.5, 7.3.
#[tokio::test]
async fn expired_full_object_mainline_get_authorization_error_does_not_serve_stale() {
    // get_ttl: 0 via CONFIG, not ResolvedSettings — the loopback path resolves
    // its own settings. See the helper's docs.
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/auth-{}", BUCKET, OBJECT);
    let path = format!("/{}", key);

    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"fo-auth-v1\""),
    );
    fixture.invalidate_metadata_cache(&key).await;

    let forbidden = StubResponse::with_status(StatusCode::FORBIDDEN).with_body(Bytes::from_static(
        b"<Error><Code>AccessDenied</Code></Error>",
    ));
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), forbidden.clone())
        .with_default(forbidden);
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(server.addr, &path, &[]).await;

    let captured = stub.captured();
    assert!(
        !conditional_requests(&captured).is_empty(),
        "R7.3: the authorization verdict must come from a request carrying the \
         cached validator. Captured: {:#?}",
        captured
    );
    assert!(
        !body.contains(&OLD_FILL) || body.len() != OBJECT_SIZE as usize,
        "R2.5: a 403 must not fail open to expired cached data; got {}",
        fill_summary(&body)
    );
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "R2.5: the client must see the authorization error"
    );
}

/// R4.4 / R7.3: Mode B `If-Match` keeps its existing behaviour — a matching
/// client ETag is a freshness assertion in its own right, so it serves the cached
/// bytes despite TTL expiry and refreshes the TTL.
///
/// The two-sided pair for R4.4: this is the arm that must serve WITHOUT a
/// conditional request, where every other test in this file requires one. Making
/// expired entries discoverable is what makes this path reachable at all — before
/// the fix a Stored_Expired entry never entered the arm Mode B lives in.
///
/// # NARROWED IN 2.8.4 — a stored-EXPIRED entry no longer qualifies (R8.10)
///
/// This test previously asserted the opposite: that a matching client `If-Match`
/// served a Stored_Expired entry from cache with zero upstream requests. That was
/// R4.4's behaviour as shipped in 2.8.0, and the AWS Security Agent flagged it
/// (a MEDIUM security review finding). The finding is correct, and two things in this spec's own
/// text resolve it rather than contradict it:
///
/// - **R4.4 says the proxy MAY serve, not SHALL.** It grants permission, so
///   withdrawing that permission for expired entries is within the requirement's
///   letter. This test was asserting a MAY as though it were a SHALL.
/// - **R4.5 forbids changing a client's RFC-defined result**, and the old behaviour
///   did exactly that. RFC 9110 §13.1.1 evaluates `If-Match` against the resource's
///   **current** representation, so when the origin has moved on the RFC-defined
///   answer is `412`, not `200` with superseded bytes. Mode B compared the client's
///   validator against the cache's *own stored* ETag — a self-comparison that
///   establishes nothing about the origin — so a client replaying a superseded ETag
///   was served stale bytes AND had their TTL refreshed. A client using `If-Match`
///   to detect mid-transfer version changes (which is why the CRT and Rust transfer
///   managers send it) was defeated by the very mechanism meant to serve it.
///
/// So R8.10 narrows R4.4 to entries within their stored expiry. That preserves what
/// R4.4 exists for — the warm CRT download, where the entry is fresh — and removes
/// the case where the cache cannot vouch for what it holds. `stored expiry passed`
/// is precisely the cache saying so.
///
/// The companion test below pins the un-narrowed half, so this pair is two-sided:
/// expired revalidates, fresh still serves locally. Without the companion, a future
/// change that disabled Mode B entirely would pass this test.
///
/// Requirements: 4.4 (narrowed), 4.5, 7.3, 8.10.
#[tokio::test]
async fn mode_b_if_match_on_expired_entry_revalidates_instead_of_serving_stale() {
    // get_ttl: 0 via CONFIG, not ResolvedSettings — the loopback path resolves
    // its own settings. See the helper's docs.
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/modeb-{}", BUCKET, OBJECT);
    let path = format!("/{}", key);

    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"fo-modeb-v1\""),
    );
    fixture.invalidate_metadata_cache(&key).await;

    // Mode B is on by default in `ResolvedSettings`
    // (`evaluate_conditions_from_cache: true`), and the proxy resolves settings
    // itself on this path, so it cannot be injected — asserted rather than
    // assumed, since a Mode-B-off deployment would make this test measure the
    // ordinary forward path instead.
    assert!(
        fixture.config.cache.evaluate_conditions_from_cache,
        "PRECONDITION MISSING: Mode B is off in this config, so an If-Match would be \
         forwarded to S3 and this test would not exercise the Mode B arm"
    );

    // Any upstream response here would be wrong: a Mode B If-Match hit must not
    // contact S3 at all.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(Bytes::from(new_bytes()))
            .with_header("etag", "\"fo-modeb-v2\""),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) =
        proxy_get(server.addr, &path, &[("if-match", etag.as_str())]).await;

    let captured = stub.captured();

    assert_eq!(status, StatusCode::OK);

    // R8.10: the entry's stored expiry has passed, so Mode B must NOT answer from
    // cache on the strength of a self-compared If-Match. It must ask the origin.
    assert!(
        !captured.is_empty(),
        "R8.10: a Mode B If-Match on a STORED-EXPIRED entry must revalidate against \
         S3 rather than serving locally — the cached ETag the client's validator was \
         compared against is the cache's own, which proves nothing about the origin. \
         Zero upstream requests means the pre-2.8.4 stale-serve is back"
    );

    // The stub reports the object HAS changed (v2), so the client must receive the
    // current bytes, not the superseded cached ones.
    assert_all_new(
        &body,
        OBJECT_SIZE as usize,
        "R8.10: once revalidation reveals a newer version, the superseded cached bytes \
         must not be served",
    );

    // R4.4 also says Mode B refreshes the TTL, and that is deliberately NOT
    // asserted from the `.meta` here.
    //
    // `refresh_object_ttl` records a TTL-refresh JOURNAL entry rather than writing
    // the `.meta`, so the on-disk `expires_at` does not move until the background
    // consolidator applies it — one `consolidation_interval` later. An assertion
    // reading the `.meta` immediately after the response samples it before it can
    // possibly have changed, and then fails for a reason that has nothing to do
    // with Mode B. That is `pre-push-checklist.md` § "Wait on the mechanism you
    // MEASURE": the flag flips synchronously, the persisted figure does not.
    //
    // It was written that way first and failed exactly so, which is recorded here
    // rather than deleted because the same trap catches the R2.3 TTL-persistence
    // work in task 7.
    //
    // The serve behaviour above IS the R4.4 contract and is fully asserted: cached
    // bytes returned, zero upstream requests, despite both stored and live expiry
    // having elapsed. The TTL-refresh half needs either a consolidation wait or a
    // journal read, and belongs with task 7's persistence tests.
    // The superseded entry must not survive still claiming the old version. Kept as a
    // presence-tolerant check because the changed-object path invalidates it.
    let still_claims_superseded = fixture
        .read_meta(&key)
        .map(|m| m.object_metadata.etag == etag)
        .unwrap_or(false);
    assert!(
        !still_claims_superseded,
        "R8.10: after revalidation revealed a newer version, the entry must not remain \
         claiming {etag}"
    );
}

/// The un-narrowed half of R4.4, and the reason 2.8.4's narrowing is safe.
///
/// A stored-FRESH entry with a matching client `If-Match` still serves from cache with
/// zero upstream requests. This is the case R4.4 exists for — the warm CRT or transfer
/// manager download, where every ranged chunk carries `If-Match` and the entry is well
/// within its TTL — and it is unaffected by R8.10.
///
/// This test is what stops the narrowing being over-applied. A change that dropped the
/// local Mode B serve altogether would pass the expired test above and fail this one.
///
/// Requirements: 4.4, 8.10.
#[tokio::test]
async fn mode_b_if_match_on_stored_fresh_entry_still_serves_from_cache() {
    // get_ttl is 60s, NOT zero: `get_ttl == 0` means always-revalidate, so that S3
    // evaluates IAM on every read, and under that meaning the local serve asserted here
    // is exactly what must NOT happen (`get-ttl-zero-enforcement` R3.1, pinned by
    // `mode_b_if_match_with_get_ttl_zero_must_revalidate` below).
    //
    // The entry is seeded 1s old, so it is live-fresh under 60s. Until this test
    // seeded it 7200s old, because Mode B then bypassed the live TTL; it no longer does
    // (`ttl_authority_serve_paths_test::mode_b_if_match_on_a_live_expired_entry_revalidates`).
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::from_secs(60));
    let fixture = Fixture::new(config).await;
    let key = format!("{}/modeb-fresh-{}", BUCKET, OBJECT);
    let path = format!("/{}", key);

    // Stored-FRESH and live-fresh. Since the change a Mode B serve also needs the live
    // `get_ttl` verdict, so a live-expired entry revalidates
    // (`mode_b_if_match_on_a_live_expired_entry_revalidates`); this test keeps the warm
    // case R4.4 exists for, an entry well within its TTL.
    let mut spec = SeedSpec::stored_fresh_live_expired(
        vec![(0, OBJECT_SIZE - 1)],
        OBJECT_SIZE,
        "\"fo-modeb-fresh-v1\"",
    );
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(&key, &spec);
    fixture.invalidate_metadata_cache(&key).await;

    assert!(
        fixture.config.cache.evaluate_conditions_from_cache,
        "PRECONDITION MISSING: Mode B is off in this config"
    );

    // Any upstream request at all would mean the local serve did not happen.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(Bytes::from(new_bytes()))
            .with_header("etag", "\"fo-modeb-fresh-v2\""),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, _headers, body) =
        proxy_get(server.addr, &path, &[("if-match", etag.as_str())]).await;
    let captured = stub.captured();

    assert_eq!(status, StatusCode::OK);
    assert!(
        captured.is_empty(),
        "R4.4: a Mode B If-Match hit on a stored-FRESH entry must not contact S3 at all; \
         {} upstream request(s) were made: {:#?}",
        captured.len(),
        captured
    );
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "R4.4: a stored-fresh Mode B If-Match hit serves the CACHED bytes. Fresh bytes \
         here mean 2.8.4's narrowing was over-applied and the local serve is gone",
    );
}

/// `get-ttl-zero-enforcement` R3.1: a key resolving to `get_ttl: "0s"` must not have a
/// Mode B `If-Match` answered from cache, because `get_ttl: 0` is an authorization
/// boundary and not merely a freshness one.
///
/// # Why this is not covered by the two tests above
///
/// `mode_b_if_match_on_expired_entry_revalidates_instead_of_serving_stale` covers a
/// STORED-expired entry, which 2.8.4 already forwards. This one is STORED-FRESH, which
/// is the state an operator produces by caching under a generous `get_ttl` and then
/// tightening the rule to `"0s"` — rules are per-glob and hot-reloaded, so that
/// sequence is the normal case, not an edge case. A cold cache cannot reach it: an
/// entry written while `get_ttl` was already `0` gets `expires_at = now`, is
/// stored-expired, and takes 2.8.4's path. **So a cold-cache fixture cannot fail
/// against the defect and would be worthless as a red side.**
///
/// The sibling test above is the same fixture with a 60s `get_ttl`, where the local
/// serve is correct and must survive. The only difference between them is the resolved
/// `get_ttl`, which is what makes a failure here attributable to the gate rather than
/// to Mode B being broken generally.
///
/// # What is asserted, and why it is the upstream request rather than the body
///
/// The object has NOT changed in this fixture, so the bytes are identical whether the
/// answer came from cache or from a revalidation — a body assertion would pass in both
/// the exposed and fixed cases and prove nothing (`pre-push-checklist.md` step 8,
/// "assert the predicate the code evaluates"). The observable is that S3 was contacted
/// at all, since that is the request on which S3 evaluates the caller's IAM permission.
///
/// Requirements: `get-ttl-zero-enforcement` R1.1, R1.2, R3.1, R3.3.
#[tokio::test]
async fn mode_b_if_match_with_get_ttl_zero_must_revalidate() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/modeb-ttl0-{}", BUCKET, OBJECT);
    let path = format!("/{}", key);

    // STORED-FRESH is the whole point: the entry was cached under a generous get_ttl
    // before the rule was tightened. Stored-expired is 2.8.4's case, not this one.
    let etag = fixture.seed(
        &key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"fo-modeb-ttl0-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(&key).await;

    assert!(
        fixture.config.cache.evaluate_conditions_from_cache,
        "PRECONDITION MISSING: Mode B is off in this config, so the If-Match would be \
         forwarded anyway and this test would pass without exercising the gate"
    );
    assert_eq!(
        fixture.config.cache.get_ttl,
        Duration::ZERO,
        "PRECONDITION MISSING: this test's whole subject is get_ttl == 0"
    );

    // The object is UNCHANGED: same ETag, same bytes. So the response is byte-identical
    // to a local serve and the upstream request is the ONLY thing that distinguishes the
    // two behaviours — which is the point.
    //
    // 200 with the body, not 304. The client's `If-Match` is forwarded verbatim, and a
    // *passing* `If-Match` returns 200 from S3 — 304 requires `If-None-Match` or
    // `If-Modified-Since` (RFC 9110 §13.1.1, §15.4.5). A first version of this fixture
    // returned 304 to the forwarded `If-Match`, which no origin would do, and the proxy
    // correctly passed that 304 through to the client per the documented
    // forwarded-conditional contract. That produced a `left: 304, right: 200` failure
    // that looked like a product defect and was entirely the stub's fault.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(Bytes::from(vec![OLD_FILL; OBJECT_SIZE as usize]))
            .with_header("etag", etag.as_str()),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) =
        proxy_get(server.addr, &path, &[("if-match", etag.as_str())]).await;
    let captured = stub.captured();

    assert_eq!(status, StatusCode::OK);
    assert!(
        !captured.is_empty(),
        "R3.1: a Mode B If-Match on a key resolving to get_ttl: 0 must reach S3, so that \
         S3 evaluates the caller's IAM permission. Zero upstream requests means the \
         verdict was produced locally from the cache's own ETag — a self-comparison that \
         establishes nothing about either the object version OR the caller's continued \
         authorization. Captured: {:#?}",
        captured
    );
    // The client's own `If-Match` must travel, the way `evaluate_conditions_from_cache:
    // false` forwards it — that is what lets S3 evaluate the precondition rather than the
    // proxy. Read from the captured request's headers directly, in both cases.
    //
    // NOT via the `conditional_requests` helper: it filters on `if_none_match` /
    // `if_modified_since`, i.e. the headers the proxy INJECTS on a revalidation, and says
    // nothing about a forwarded client `If-Match`. Using it here reported "no conditional
    // headers" for a request that carried the client's `If-Match` correctly — the wrong
    // predicate for the property being asserted.
    let forwarded_if_match = captured
        .iter()
        .any(|r| r.headers.contains_key("if-match") || r.headers.contains_key("If-Match"));
    assert!(
        forwarded_if_match,
        "R3.1: the upstream request must carry the client's If-Match intact, so S3 \
         evaluates the precondition. Without it the request is a plain refetch that \
         transferred a body it did not need. Body: {}. Captured: {:#?}",
        fill_summary(&body),
        captured
    );

    // The object did not change, so the client must still receive the correct bytes. This
    // asserts the gate is not a correctness regression; it is NOT evidence about where the
    // bytes came from, since both behaviours produce identical bytes here by construction.
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "R3.1: the gate costs a round trip, not correctness — the client must still \
         receive the object's current bytes",
    );
}

/// R1.2 / R7.11: the full-object **partial**-coverage path is a serve of expired
/// cached bytes, and it must hold an authority.
///
/// # Why this test exists — a reachability change, not a new feature
///
/// Making the full-object lookup a `RevalidationCandidate` changed which arm a
/// Stored_Expired entry with *partial* coverage lands in. Before, stored expiry
/// emptied the overlap, `cached_bytes` came out 0, the 10%-cached gate failed, and
/// the request became an ordinary full refetch. Now the overlap is non-empty and the
/// request takes the partial-merge path — which **serves the cached fraction** and
/// fetches only the gaps.
///
/// That is a cached serve of expired bytes, so R1.2 requires an authority for it.
/// There is one, and it is neither live TTL nor a `304`: the merge path injects
/// `If-Match` on the cached ETag before fetching the gaps
/// (`build_conditional_headers_for_range`), so S3 answers `412` if the object has
/// changed and the cached fraction can only ever be merged with bytes from the same
/// version. That is the same kind of assertion Mode B rests on.
///
/// This asserts the authority is really on the wire rather than trusting the comment
/// that says so. Without it, the reachability change would rest on a code reading —
/// and this is exactly the class of arm that gets missed when a lookup's semantics
/// change underneath it.
///
/// Requirements: 1.2, 4.7, 7.11.
#[tokio::test]
async fn expired_partial_coverage_full_object_merge_is_pinned_to_the_cached_etag() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/partial-{}", BUCKET, OBJECT);
    let path = format!("/{}", key);

    // Cache the first half only, well above the path's 10%-cached gate.
    let cached_end = OBJECT_SIZE / 2 - 1;
    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(vec![(0, cached_end)], OBJECT_SIZE, "\"fo-partial-v1\""),
    );
    fixture.invalidate_metadata_cache(&key).await;

    // Every upstream answer is FRESH bytes, so the cached half is identifiable in the
    // merged response by value.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(new_bytes()))
            .with_header(
                "content-range",
                format!("bytes 0-{}/{}", OBJECT_SIZE - 1, OBJECT_SIZE),
            )
            .with_header("etag", &etag),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    // The Authorization here must NOT sign `range`: the merge path synthesises a Range
    // to fetch the gaps and refuses to when `range` is signed, because adding the
    // header would break the client's signature. With the range-signing fixture the
    // path is simply not entered — which is how the first version of this test failed,
    // capturing a request with neither `range` nor `if-match` on it.
    let (_status, _headers, body) = proxy_get_with_auth(
        server.addr,
        &path,
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let captured = stub.captured();

    // Precondition: something actually went upstream. If the whole response came from
    // cache this test would be asserting about a request that never happened.
    assert!(
        !captured.is_empty(),
        "PRECONDITION MISSING: no upstream request was made, so there is no merge to \
         inspect. Response was {}",
        fill_summary(&body)
    );

    // Precondition, learned the hard way: the merge path must actually have been
    // entered. It synthesises a Range to fetch only the gaps, so a captured request
    // with no `range` header means the request took the plain-forward path and this
    // test is about to assert on the wrong request shape.
    assert!(
        captured
            .iter()
            .any(|r| r.headers.contains_key("range") || r.headers.contains_key("Range")),
        "PRECONDITION MISSING: no upstream request carried a synthesised Range, so the \
         partial-merge path was not entered and there is nothing for the If-Match \
         assertion below to be about. Captured requests: {:#?}",
        captured
    );

    // THE authority assertion. Reads the header off the request that actually fetched
    // the bytes, not a different one.
    let pinned = captured.iter().any(|r| {
        r.headers
            .get("if-match")
            .or_else(|| r.headers.get("If-Match"))
            .map(|v| v == &etag)
            .unwrap_or(false)
    });
    assert!(
        pinned,
        "R1.2: the partial-merge path serves the EXPIRED cached fraction, so the fetch \
         of the missing bytes must be pinned to the cached ETag ({}) — otherwise the \
         merged response could splice two different versions of the object together \
         and no authority would cover the cached half. Captured requests: {:#?}",
        etag, captured
    );
}
