//! End-to-end coverage for `.kiro/specs/write-cache-last-modified/`.
//!
//! Spec: Requirements R1-R9, R11. GitHub issue #19, reported by James Connor
//! (@megakid), who also implemented a working fix on a 2.8.0 base
//! ([`e8a47a5`](https://github.com/kingstonsporting/sample-s3-hybrid-cache/commit/e8a47a565f5b416dde4fda39f9e65fbcd1330c44)).
//!
//! # The defect
//!
//! `PutObject` and `CompleteMultipartUpload` never return `Last-Modified`, so a
//! write-through cache entry stores an empty `object_metadata.last_modified`.
//! The first GET after the write is a cache HIT, so it never reaches S3 and the
//! field is never learned — the proxy holds no AWS credentials and cannot
//! originate a request to go and fetch it. The fix: treat such an entry as
//! requiring revalidation, inject `If-None-Match` (never `If-Modified-Since`,
//! since there is no cached value to send), and backfill the learned
//! `Last-Modified` from a `304`'s response headers.
//!
//! # Harness
//!
//! Reuses `common::expired_fixture`'s direct-.meta-write approach — driving a
//! cold PUT through the real write-through path would work too, but writing the
//! `.meta` directly is what lets `SeedSpec::write_cached_no_last_modified` set
//! `is_write_cached: true` with a stored-fresh, live-fresh TTL, which is exactly
//! the point: this spec's trigger must fire even when EVERY existing freshness
//! mechanism says the entry is fresh.

mod common;

use std::collections::HashMap;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    body_of, conditional_requests, not_modified_with_last_modified, test_config, Fixture, SeedSpec,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;

fn old_bytes(len: usize) -> Vec<u8> {
    vec![b'A'; len]
}

// =====================================================================
// R1 — the trigger fires even under generous TTLs
// =====================================================================

/// R1.1/R1.3/R1.4: a write-cached entry with no effective `Last-Modified` must
/// revalidate on GET even though it is Stored_Fresh and Live_Fresh by every
/// existing mechanism. This is what distinguishes the new trigger from ordinary
/// TTL-driven expiry — a `get_ttl` set to `zero_ttl()` here would not
/// distinguish the two, so this test deliberately does NOT use it; the fixture's
/// own `created_age: 5s` plus a real (non-zero) `get_ttl` from `test_config` is
/// what proves the trigger is independent of live-TTL expiry.
///
/// Requirements: 1.1, 1.3, 1.4, 11.3, 11.6.
#[tokio::test]
async fn write_cached_entry_revalidates_despite_fresh_ttl() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/write-cached-fresh-ttl.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);
    let len = OBJECT_SIZE as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::write_cached_no_last_modified(vec![(start, end)], OBJECT_SIZE, "\"wc-v1\""),
    );
    fixture.assert_covers(cache_key, start, end);
    fixture.invalidate_metadata_cache(cache_key).await;

    let meta = fixture.read_meta(cache_key).expect(".meta must exist");
    assert!(
        meta.object_metadata.is_write_cached,
        "fixture: entry must be write-cached, or the new trigger's is_write_cached \
         conjunct never engages and this test measures nothing"
    );
    assert!(
        meta.object_metadata.last_modified.is_empty(),
        "fixture: entry must have no stored Last-Modified"
    );
    assert!(
        meta.expires_at > std::time::SystemTime::now(),
        "fixture: entry must be Stored_Fresh — expires_at in the future"
    );

    // A generous non-zero get_ttl: ordinary Live_Expired cannot explain a
    // revalidation here on a 5-second-old entry.
    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: std::time::Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    let stub = StubS3Client::new()
        .with_response_for_etag(
            etag.clone(),
            not_modified_with_last_modified(&etag, "Wed, 09 Sep 2026 16:51:54 GMT"),
        )
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(old_bytes(len)))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", "\"wc-v2\""),
        );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;

    let body = body_of(response).await;
    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);

    assert!(
        !conditionals.is_empty(),
        "R1.1/R1.3: a write-cached entry with no effective Last-Modified must \
         revalidate despite a fresh TTL by every existing mechanism. Captured \
         requests: {:#?}",
        captured
    );
    assert_eq!(
        conditionals[0].if_none_match(),
        Some(etag.as_str()),
        "the conditional must carry the cached ETag"
    );
    assert_eq!(
        conditionals[0].if_modified_since(),
        None,
        "R2.1: with no cached Last-Modified, only If-None-Match may be injected"
    );
    assert_eq!(
        body,
        old_bytes(len),
        "R2.2/R3.4: after 304 the cached bytes must be served"
    );
}

/// R1.4 (negative control): a READ-cache entry with no effective Last-Modified —
/// legitimately possible from an older release — must retain its EXISTING TTL
/// behaviour and must NOT be forced to revalidate by this new trigger. Uses
/// `SeedSpec::expired`, which sets `is_write_cached: false` and a real
/// Last-Modified — swapped here for an empty one to isolate exactly the R1.4
/// conjunct.
///
/// Requirements: 1.4.
#[tokio::test]
async fn read_cached_entry_with_no_last_modified_is_not_forced_to_revalidate() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/read-cached-no-lm-fresh.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);

    // Stored-fresh, live-fresh, is_write_cached: false, empty last_modified —
    // constructed directly since no existing SeedSpec constructor combines
    // exactly this. Mirrors write_cached_no_last_modified but for the
    // read-cache case.
    let spec = SeedSpec {
        extents: vec![(start, end)],
        etag: "\"rc-v1\"".to_string(),
        last_modified: String::new(),
        content_length: OBJECT_SIZE,
        stored_expiry: common::expired_fixture::StoredExpiry::Fresh,
        created_age: std::time::Duration::from_secs(5),
        is_write_cached: Some(false),
    };
    fixture.seed(cache_key, &spec);
    fixture.invalidate_metadata_cache(cache_key).await;

    let meta = fixture.read_meta(cache_key).expect(".meta must exist");
    assert!(
        !meta.object_metadata.is_write_cached,
        "fixture: entry must NOT be write-cached, or this test measures R1.1 \
         rather than the R1.4 negative case"
    );

    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: std::time::Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    // Every response is a full 200 with fresh bytes; if a conditional is
    // (incorrectly) sent it would return 304 and this stub would have nothing to
    // distinguish it from — but the point is a conditional must not be sent at
    // all, so no with_response_for_etag stub is registered.
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![b'B'; OBJECT_SIZE as usize]))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
            )
            .with_header("etag", "\"rc-v1\""),
    );

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let captured = stub.captured();
    assert!(
        conditional_requests(&captured).is_empty(),
        "R1.4: a non-write-cached entry with no Last-Modified must retain its \
         existing TTL-only behaviour (Stored_Fresh, Live_Fresh here) and must NOT \
         be revalidated on this ground. A conditional here means the new trigger \
         fired on a read-cache entry it must not touch. Captured: {:#?}",
        captured
    );
}

// =====================================================================
// R3/R4 — the 304 backfill transaction
// =====================================================================

/// R3.1/R3.3: a `304` carrying `Last-Modified` must backfill the field and serve
/// the response with the learned header — a client must not need a second
/// request to see it.
///
/// Requirements: 3.1, 3.3.
#[tokio::test]
async fn successful_304_backfills_last_modified_and_serves_immediately() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/write-cached-backfill.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);
    let len = OBJECT_SIZE as usize;
    let learned_lm = "Wed, 09 Sep 2026 10:28:17 GMT";

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::write_cached_no_last_modified(vec![(start, end)], OBJECT_SIZE, "\"bf-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    let stub = StubS3Client::new().with_response_for_etag(
        etag.clone(),
        not_modified_with_last_modified(&etag, learned_lm),
    );

    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: std::time::Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let body = body_of(response).await;
    assert_eq!(body, old_bytes(len), "cached bytes must still be served");

    // The persisted .meta must now carry the learned value.
    let meta = fixture
        .read_meta(cache_key)
        .expect(".meta must exist after backfill");
    assert_eq!(
        meta.object_metadata.effective_last_modified(),
        Some(learned_lm),
        "R3.1: the learned Last-Modified must be persisted into the entry"
    );

    // R6.4: graduation must have run in the same operation.
    assert!(
        !meta.object_metadata.is_write_cached,
        "R6.4: once Last-Modified is learned, the entry must graduate in the \
         same operation rather than being left staged for a later GET"
    );
}

/// R4.1/R4.2: a `304` that omits `Last-Modified` must degrade safely — persist
/// nothing, WARN, and **fall through to forwarding the client's original
/// request** — and must NOT retry in a loop on a single request.
///
/// The forward is the whole point, and is what R4.3's termination property rests
/// on: the forward returns a full body, re-caches through the read-cache path,
/// and `is_write_cached` goes false, so R1.4's conjunct makes the trigger
/// unreachable for that entry thereafter. Serving the cached bytes here instead
/// would leave the entry byte-for-byte unchanged, so the trigger would fire
/// again on the very next read — forever — and the header would never be
/// delivered. That is precisely the "innocent-looking simplification" R4.3 warns
/// converts a one-off cost into an unbounded one.
///
/// Requirements: 4.1, 4.2.
#[tokio::test]
async fn undurable_304_persists_nothing_and_forwards_the_original_request() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/write-cached-undurable.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);
    let len = OBJECT_SIZE as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::write_cached_no_last_modified(
            vec![(start, end)],
            OBJECT_SIZE,
            "\"undurable-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    // The ORIGIN never returns Last-Modified on its 304 — the R4 case, using the
    // distinct-from-not_modified_with_last_modified helper per R11.5.
    //
    // The default arm is what the R4.1 forward lands on, and it deliberately
    // serves a DISTINCT fill byte from the seeded cache contents. That is the
    // discriminator: identical bytes could not tell "forwarded and served from
    // S3" apart from "served the cached copy", which is the exact substitution
    // this test exists to detect.
    let forwarded_bytes = vec![b'B'; len];
    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), common::expired_fixture::not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(forwarded_bytes.clone()))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", &etag),
        );

    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: std::time::Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let body = body_of(response).await;
    assert_eq!(
        body, forwarded_bytes,
        "R4.1: the response must come from the FORWARDED original request, not \
         from the cached copy. Receiving the seeded bytes here means the 304's \
         missing header was absorbed and the request was served from cache, \
         leaving the entry unhealed and the trigger armed forever."
    );

    // The conditional was attempted, and an unconditional request followed it in
    // the SAME call. Asserting only that a conditional happened would pass
    // against an implementation that absorbs the 304 and serves from cache.
    let captured = stub.captured();
    let conditional_count = conditional_requests(&captured).len();
    assert!(
        conditional_count >= 1,
        "fixture: the revalidation must have been attempted at all"
    );
    assert!(
        captured.len() > conditional_count,
        "R4.1: an UNCONDITIONAL request must follow the 304 in the same call — \
         {} captured request(s), {} of them conditional, so no forward was made",
        captured.len(),
        conditional_count
    );

    // R4.2: the unhealed 304 invalidates the stale write-cached entry and
    // forwards, so the entry re-caches through the read-cache path. The
    // observable post-condition is that it is NO LONGER write-cached — satisfied
    // both by the entry being absent (invalidated, its range re-cache not yet
    // materialised in this harness) and by a re-cached `.meta` carrying
    // `is_write_cached: false`. What must never hold is a surviving write-cached
    // entry, because that keeps R1.4's conjunct armed and turns R4.3's one-off
    // cost into a per-read one. The sibling
    // `..._costs_one_extra_fetch_per_entry_once...` test pins the behavioural
    // consequence (no second conditional on the next read); this asserts the
    // stored state behind it.
    match fixture.read_meta(cache_key) {
        Some(meta) => {
            assert!(
                !meta.object_metadata.is_write_cached,
                "R4.2: after the R4.1 forward the entry must not be write-cached; \
                 a surviving write-cached flag keeps the trigger armed forever"
            );
            assert!(
                meta.object_metadata.effective_last_modified().is_none(),
                "R3.4/R4.1: the origin never supplied a Last-Modified, so none \
                 must be fabricated onto a re-cached entry either"
            );
        }
        None => {
            // Invalidated and not yet re-materialised — the write-cached entry
            // is gone, which is the property under test.
        }
    }
}

/// R11.4/R4.3: an origin that never returns `Last-Modified` on a `304` costs one
/// extra full-body fetch **per entry, once** — never a per-read penalty. The
/// first read revalidates, gets an undurable `304`, forwards (R4.1), and the
/// re-cache through the read-cache path clears `is_write_cached`; from then on
/// R1.4's conjunct makes the trigger unreachable for that entry.
///
/// R4.3 names this test specifically, and says why it exists: the property
/// depends on R1.4's conjunct, and "an innocent-looking simplification of the
/// trigger would silently convert a one-off cost into an unbounded one". So the
/// second pass asserting **no new conditional** is the load-bearing half. A test
/// that asserted a conditional on every pass would pin the unbounded cost as if
/// it were the contract, and would go green against exactly the defect R4.3
/// warns about.
///
/// Requirements: 4.3, 11.4.
#[tokio::test]
async fn undurable_304_costs_one_extra_fetch_per_entry_once_not_per_read_forever() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/write-cached-undurable-repeat.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);
    let len = OBJECT_SIZE as usize;

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::write_cached_no_last_modified(vec![(start, end)], OBJECT_SIZE, "\"repeat-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), common::expired_fixture::not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![b'B'; len]))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                )
                .with_header("etag", &etag),
        );

    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: std::time::Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    // Pass 1: the trigger fires, the 304 is undurable, the request is forwarded.
    fixture.invalidate_metadata_cache(cache_key).await;
    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let after_first = conditional_requests(&stub.captured()).len();
    assert!(
        after_first >= 1,
        "pass 1: the first read of an entry with no known Last-Modified must \
         attempt revalidation — that is R1.1's trigger"
    );

    // Pass 2: the entry has been re-cached as a read-cache entry, so R1.4's
    // conjunct no longer holds and NO further conditional may be issued.
    fixture.invalidate_metadata_cache(cache_key).await;
    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;

    let after_second = conditional_requests(&stub.captured()).len();
    assert_eq!(
        after_second,
        after_first,
        "R4.3: the cost must be one extra fetch PER ENTRY, once. The second read \
         issued {} further conditional(s) ({} -> {}), which means the entry is \
         still write-cached and every future read of it will pay a round trip \
         forever — the unbounded cost R4.3 exists to forbid.",
        after_second - after_first,
        after_first,
        after_second
    );
}

// =====================================================================
// R8 — signature safety
// =====================================================================

/// R8.1/R8.2: when the client sent its own `If-None-Match`, the proxy must NOT
/// inject a second validator, and the resulting `304` (from the client's own
/// precondition) must still backfill under R3 — the backfill is driven by the
/// response's headers, not by which party supplied the validator.
///
/// Requirements: 8.1, 8.2, 3.1.
#[tokio::test]
async fn client_supplied_conditional_still_backfills_on_304() {
    let config = test_config(OBJECT_SIZE);
    let fixture = Fixture::new(config).await;
    let cache_key = "bucket/write-cached-client-conditional.bin";
    let (start, end) = (0u64, OBJECT_SIZE - 1);
    let len = OBJECT_SIZE as usize;
    let learned_lm = "Wed, 09 Sep 2026 13:30:50 GMT";

    let etag = fixture.seed(
        cache_key,
        &SeedSpec::write_cached_no_last_modified(
            vec![(start, end)],
            OBJECT_SIZE,
            "\"client-cond-v1\"",
        ),
    );
    fixture.invalidate_metadata_cache(cache_key).await;

    let mut client_headers = HashMap::new();
    client_headers.insert("if-none-match".to_string(), etag.clone());

    let stub = StubS3Client::new().with_response_for_etag(
        etag.clone(),
        not_modified_with_last_modified(&etag, learned_lm),
    );

    let generous_ttl = s3_proxy::bucket_settings::ResolvedSettings {
        get_ttl: std::time::Duration::from_secs(86400),
        ..s3_proxy::bucket_settings::ResolvedSettings::default()
    };

    let response = fixture
        .range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            client_headers,
            &generous_ttl,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        )
        .await;
    let body = body_of(response).await;
    assert_eq!(body, old_bytes(len));

    let meta = fixture.read_meta(cache_key).expect(".meta must exist");
    assert_eq!(
        meta.object_metadata.effective_last_modified(),
        Some(learned_lm),
        "R3.1/R8.1: a 304 produced by the CLIENT's own precondition must still \
         backfill — the backfill is driven by the response, not by who supplied \
         the validator"
    );
}
