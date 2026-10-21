//! `head_ttl: "0s"` is the operator's control for the HEAD half of the
//! always-revalidate contract, and it must not silently regress.
//!
//! Spec: the get ttl zero enforcement feature R1.4, R1.5, R2.1, R2.3.
//!
//! # Why this pin exists, and why it is a pin rather than a fix
//!
//! `get_ttl: "0s"` is documented as always-revalidate, and operators reach for it
//! on revocation-sensitive prefixes so that S3 evaluates IAM on every read. It
//! governs **GET only**. HEAD is governed solely by `head_ttl`
//! (`is_head_fresh`, `src/cache.rs`, which requires `!current_head_ttl.is_zero()`),
//! and `get_head_cache_entry_unified` never reads `get_ttl` at all — so a key with
//! `get_ttl: "0s"` and a default 60s `head_ttl` is fully HEAD-servable out of the
//! RAM metadata tier, returning etag, content-length and stored response headers
//! with no S3 request. That is Path C in the spec, and it is the **larger** of the
//! two exposures the spec found: live on default configuration, recurring for the
//! whole of `head_ttl`, and needing no pre-existing entry.
//!
//! Ed's decision (R2.2, R1.5) is that `get_ttl` will NOT be made to govern HEAD.
//! `head_ttl > get_ttl` is a coherent and useful configuration — immutable objects
//! whose existence is polled — and silently overriding an explicit operator value
//! is worse than requiring both fields be named. Path C is therefore closed by
//! documentation and by the three-field recipe
//! (`get_ttl: "0s"` + `head_ttl: "0s"` + `evaluate_conditions_from_cache: false`),
//! which shipped in 2.8.8.
//!
//! What that decision leaves owing is exactly this file: if the operator's control
//! is the whole mitigation, the control must be pinned so a later change cannot
//! quietly remove it.
//!
//! # THERE IS NO RED SIDE FOR THE FIRST TEST, AND THAT IS STATED RATHER THAN IMPLIED
//!
//! `head_ttl_zero_forwards_every_head_even_with_a_generous_get_ttl` pins existing,
//! correct behaviour. No binary has ever failed it and none is expected to. Per
//! `pre-push-checklist.md` § "New groups must be shown failing first", a passing
//! assertion whose red side has never been seen is decoration unless something
//! else establishes that it *could* fail — so do not read its pass as evidence
//! that a defect was fixed. It is a regression guard, nothing more.
//!
//! # What DOES establish that it could fail: the control below it
//!
//! `head_ttl_nonzero_serves_a_repeat_head_from_cache` is the same fixture with a
//! generous `head_ttl`, and it asserts the opposite: the second HEAD must be
//! answered locally. Without it, "every HEAD reached S3" would be satisfied just as
//! well by HEAD caching being broken outright, and the first test would pass while
//! measuring nothing about `head_ttl`. The pair is what makes either half mean
//! something — the same two-sided shape the fleet test uses, and the same
//! reasoning that made `mode_b_if_match_on_stored_fresh_entry_still_serves_from_cache`
//! worth keeping alongside the get_ttl-zero test.
//!
//! # What is asserted
//!
//! The captured **upstream request count**, not the response. The object never
//! changes in these fixtures, so a locally-answered HEAD and a forwarded one return
//! identical status, etag and content-length — a response assertion passes in both
//! cases and discriminates nothing. The observable that matters is whether S3 was
//! contacted, because that is the request on which S3 evaluates the caller's
//! permission.

mod common;

use std::time::Duration;

use hyper::{Method, StatusCode};

use common::expired_fixture::{
    proxy_head, test_config_full_object_checks_with_ttls, Fixture, SeedSpec,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const BUCKET: &str = "test-bucket";

fn cache_key(name: &str) -> String {
    format!("{}/{}", BUCKET, name)
}

/// A HEAD-shaped origin response: no body, but the metadata a HEAD serve would
/// need to be reconstructed from cache.
fn head_response(etag: &str) -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("etag", etag)
        .with_header("content-length", OBJECT_SIZE.to_string())
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header("content-type", "application/octet-stream")
}

fn head_count(captured: &[common::CapturedRequest]) -> usize {
    captured.iter().filter(|r| r.method == Method::HEAD).count()
}

/// R2.3: with `head_ttl: "0s"`, every HEAD reaches S3 — even though `get_ttl` is
/// generous, the entry is stored-fresh, and a RAM metadata entry exists by the
/// second request.
///
/// The `get_ttl` here is deliberately **generous**, not zero. A zero `get_ttl`
/// would make the configuration the one the rest of the spec is about and would
/// leave open whether `head_ttl` did any work at all; a generous one means the only
/// thing that can be forcing these HEADs upstream is `head_ttl`.
///
/// Three HEADs rather than two, because the state changes between them: the first
/// is a cold miss (the seed leaves `head_expires_at`/`head_cached_at` unset), and it
/// is the first forward that populates the RAM metadata entry. So HEAD #2 and #3
/// are the ones asked in the presence of the tier the spec is concerned about, and
/// a fixture that stopped at two would only ever have exercised one of them.
///
/// NO RED SIDE — see the module docs. The control test below is what shows the
/// assertion is capable of failing.
#[tokio::test]
async fn head_ttl_zero_forwards_every_head_even_with_a_generous_get_ttl() {
    let config = test_config_full_object_checks_with_ttls(
        OBJECT_SIZE,
        Duration::from_secs(3600), // generous get_ttl — NOT the thing under test
        Duration::ZERO,            // head_ttl: "0s" — the operator control
    );
    let fixture = Fixture::new(config).await;
    let key = cache_key("head-ttl-zero.bin");
    let path = format!("/{}", key);

    // Stored-FRESH: a stored-expired entry would be refused for a reason unrelated
    // to head_ttl, and the test would pass without the control ever being consulted.
    let etag = fixture.seed(
        &key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"head-ttl-zero-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(&key).await;

    assert_eq!(
        fixture.config.cache.head_ttl,
        Duration::ZERO,
        "PRECONDITION MISSING: head_ttl is not zero, so this test is not exercising \
         the operator control it exists to pin"
    );
    assert!(
        !fixture.config.cache.get_ttl.is_zero(),
        "PRECONDITION MISSING: get_ttl must be GENEROUS here. With a zero get_ttl \
         there would be two candidate reasons for a forwarded HEAD and the test \
         could not attribute the behaviour to head_ttl"
    );
    assert!(
        fixture.config.cache.metadata_cache.enabled,
        "PRECONDITION MISSING: the RAM metadata tier is disabled, so the tier R2.3 \
         is about is not present and the assertion is vacuous"
    );

    let stub = StubS3Client::new().with_default(head_response(&etag));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    for i in 1..=3 {
        let (status, _headers, body) = proxy_head(server.addr, &path, &[]).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "HEAD #{} did not return 200, so the count below describes failed \
             requests rather than serve decisions",
            i
        );
        assert!(
            body.is_empty(),
            "HEAD #{} returned a body of {} bytes; HEAD carries no body",
            i,
            body.len()
        );
    }

    let captured = stub.captured();
    assert_eq!(
        head_count(&captured),
        3,
        "R2.3: with head_ttl: \"0s\", all three HEADs must reach S3 so S3 evaluates \
         the caller's permission on each. {} of 3 did. A short count means a HEAD was \
         answered from the RAM metadata tier or the disk .meta — both no-S3 serves — \
         which would mean `head_ttl: \"0s\"` no longer closes Path C and the \
         three-field recipe in docs/CACHING.md no longer works as documented. \
         Captured: {:#?}",
        head_count(&captured),
        captured
    );
}

/// CONTROL for the test above, and the reason its assertion is not vacuous.
///
/// Same fixture, same key shape, same client, one field different: a generous
/// `head_ttl`. The second HEAD must now be answered **locally**. If HEAD caching
/// were broken outright — or if the stub, the loopback harness or the seed were
/// wrong in a way that forced every request upstream — this test fails and the test
/// above stops being evidence about `head_ttl`.
///
/// This is deliberately NOT merged into the test above as a second phase. The two
/// configurations cannot coexist in one `Fixture`: the loopback path resolves its
/// own `ResolvedSettings` from config, so `head_ttl` is fixed for the lifetime of
/// the proxy under test and cannot be changed mid-test.
#[tokio::test]
async fn head_ttl_nonzero_serves_a_repeat_head_from_cache() {
    let config = test_config_full_object_checks_with_ttls(
        OBJECT_SIZE,
        Duration::from_secs(3600),
        Duration::from_secs(600), // generous head_ttl — the ONLY difference
    );
    let fixture = Fixture::new(config).await;
    let key = cache_key("head-ttl-nonzero.bin");
    let path = format!("/{}", key);

    let etag = fixture.seed(
        &key,
        &SeedSpec::stored_fresh_live_expired(
            vec![(0, OBJECT_SIZE - 1)],
            OBJECT_SIZE,
            "\"head-ttl-nonzero-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(&key).await;

    assert!(
        !fixture.config.cache.head_ttl.is_zero(),
        "PRECONDITION MISSING: head_ttl must be non-zero for this control"
    );

    let stub = StubS3Client::new().with_default(head_response(&etag));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    // HEAD #1 is a cold miss whichever way head_ttl is set — the seed leaves the
    // head anchors unset — so it is the request that establishes the cached HEAD
    // metadata rather than a measurement.
    let (status1, _h1, _b1) = proxy_head(server.addr, &path, &[]).await;
    assert_eq!(status1, StatusCode::OK);
    let after_first = head_count(&stub.captured());
    assert_eq!(
        after_first, 1,
        "PRECONDITION MISSING: the first HEAD should be a single upstream miss; got \
         {} upstream HEADs, so the fixture is not in the state this control assumes",
        after_first
    );

    let (status2, _h2, _b2) = proxy_head(server.addr, &path, &[]).await;
    assert_eq!(status2, StatusCode::OK);
    let captured = stub.captured();

    assert_eq!(
        head_count(&captured),
        1,
        "CONTROL FAILED: with a generous head_ttl the SECOND HEAD must be answered \
         locally, so the upstream HEAD count must stay at 1. It is {}. Either HEAD \
         caching is not working at all — in which case \
         `head_ttl_zero_forwards_every_head_even_with_a_generous_get_ttl` passes for \
         the wrong reason and proves nothing about head_ttl — or this fixture no \
         longer reaches the HEAD cache. Fix this before trusting that test. \
         Captured: {:#?}",
        head_count(&captured),
        captured
    );
}
