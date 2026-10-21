//! Signed-`Range` integrity on the cache repair and partial-coverage paths.
//!
//! A SigV4 client that lists `range` in `SignedHeaders` (every SDK ranged download)
//! must reach S3 with its `Range` header byte-for-byte: any rewrite, including
//! normalising `bytes=X-` or `bytes=-N` to `bytes=a-b`, is a different canonical
//! request and S3 answers `403 SignatureDoesNotMatch`. The AWS CLI's classic
//! transfer client sends a multipart download's final chunk open-ended
//! (`bytes=16777216-`), so the open-ended form is the common case, not an edge.
//!
//! The handler-level tests below use a stub S3 that answers only the client's
//! verbatim `Range` and returns `403` for anything else, which is how S3 treats a
//! rewritten signed header. The repair-API tests pin the guards inside
//! `RangeHandler`, so a caller that skips the routing check still cannot rewrite a
//! signed header. Repair-API cases are adapted from an earlier fork commit
//! (`tests/signed_range_rewrite_refusal_test.rs`).

mod common;

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    body_of, presigned_query, presigned_query_without_range, presigned_range_query,
    signed_authorization_without_range, signed_range_authorization, test_config, zero_ttl, Fixture,
    SeedSpec, OLD_FILL,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::Range;
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::range_handler::RangeSpec;
use s3_proxy::ProxyError;

const HOST: &str = "s3.us-west-2.amazonaws.com";
const OBJECT_SIZE: u64 = 4096;
const ETAG: &str = "\"signed-range-etag\"";
const UPSTREAM_FILL: u8 = b'U';

fn uri(cache_key: &str) -> hyper::Uri {
    format!("/{cache_key}").parse().unwrap()
}

fn headers(authorization: &str, range: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("authorization".to_string(), authorization.to_string());
    headers.insert("host".to_string(), HOST.to_string());
    if let Some(range) = range {
        headers.insert("range".to_string(), range.to_string());
    }
    headers
}

/// S3's answer to a signed request whose canonical request no longer matches.
fn signature_does_not_match() -> StubResponse {
    StubResponse::with_status(StatusCode::FORBIDDEN)
        .with_body(Bytes::from_static(
            b"<Error><Code>SignatureDoesNotMatch</Code></Error>",
        ))
        .with_header("content-type", "application/xml")
}

/// A `206` for `start..=end` of an `OBJECT_SIZE` object, filled with `UPSTREAM_FILL`.
fn partial(start: u64, end: u64) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(vec![UPSTREAM_FILL; (end - start + 1) as usize]))
        .with_header("etag", ETAG)
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header(
            "content-range",
            format!("bytes {start}-{end}/{OBJECT_SIZE}"),
        )
}

/// A stub that accepts only `raw_range` verbatim (answering `start..=end`), revalidates
/// with `304`, and rejects every other request as S3 rejects a rewritten signature.
fn verbatim_only_s3(raw_range: &str, start: u64, end: u64) -> StubS3Client {
    StubS3Client::new()
        .with_response_for_etag(ETAG, common::expired_fixture::not_modified(ETAG))
        .with_response_for_range(raw_range, partial(start, end))
        .with_default(signature_does_not_match())
}

/// Every non-conditional upstream request carried the client's `Range` verbatim.
fn assert_every_fetch_kept_the_signed_range(stub: &StubS3Client, raw_range: &str) {
    let captured = stub.captured();
    let fetches: Vec<_> = captured
        .iter()
        .filter(|r| r.if_none_match().is_none() && r.if_modified_since().is_none())
        .collect();
    assert!(
        !fetches.is_empty(),
        "the request never reached S3, so the path under test was not exercised: {captured:?}"
    );
    for fetch in fetches {
        let range = fetch
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("range"))
            .map(|(_, v)| v.as_str());
        assert_eq!(
            range,
            Some(raw_range),
            "a signed Range was rewritten on its way to S3: {fetch:?}"
        );
    }
}

/// A cached extent whose `.bin` is missing, so any merge over it must fall back.
fn missing_cached_range(start: u64, end: u64) -> Range {
    Range {
        start,
        end,
        data: Vec::new(),
        etag: ETAG.to_string(),
        last_modified: String::new(),
        compression_algorithm: CompressionAlgorithm::None,
    }
}

async fn signed_range_get(
    fixture: &Fixture,
    cache_key: &str,
    raw_range: &str,
    resolved: &ResolvedSettings,
    stub: &StubS3Client,
) -> (StatusCode, Vec<u8>) {
    let response = tokio::time::timeout(
        Duration::from_secs(20),
        fixture.range_get(
            cache_key,
            raw_range,
            headers(&signed_range_authorization(), Some(raw_range)),
            resolved,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        ),
    )
    .await
    .expect("the range GET must not hang");
    let status = response.status();
    (status, body_of(response).await)
}

// ---------------------------------------------------------------------------
// Handler level: a partly cached object under `get_ttl: 0s` (D3, D5)
//
// The 304 arm's partial-coverage forward now streams the client's whole range
// (`stream_range_from_s3_with_caching`), so these two tests are also the red side
// for the verbatim-Range guard there (D5). Removing the D3 guard alone does not
// turn them red: `fetch_missing_ranges` then refuses and its caller falls back to
// `fetch_complete_range_from_s3`, which keeps the signed header too. They go red
// when the D3 guard and either of those two layers are removed.
// ---------------------------------------------------------------------------

/// D3: the 304 incomplete-coverage arm. The object is cached for 0-1023 only and
/// `get_ttl` is zero, so the GET revalidates, gets a `304`, and finds the request
/// only partly covered. Fetching each hole rewrites `Range`; the client's own
/// request must go upstream instead. Open-ended form, as the CLI's final chunk.
#[tokio::test]
async fn a_signed_open_ended_range_over_a_partly_cached_object_keeps_its_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-open-ended-partial.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=512-";
    let stub = verbatim_only_s3(raw, 512, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &zero_ttl(), &stub).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 512);
    assert!(
        body.iter().all(|&b| b == UPSTREAM_FILL),
        "the response must be S3's bytes for the client's own request"
    );
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}

/// D3, suffix form: `bytes=-3584` resolves to 512-4095 against a 0-1023 extent.
#[tokio::test]
async fn a_signed_suffix_range_over_a_partly_cached_object_keeps_its_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-suffix-partial.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=-3584";
    let stub = verbatim_only_s3(raw, 512, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &zero_ttl(), &stub).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 512);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}

/// Control for the two partial-coverage tests above: a signed open-ended range with
/// no cached overlap is routed to the signed-range forward, which never rewrote
/// `Range`. It passes with the D5 guard removed; the partial-coverage tests are the
/// ones that reach `stream_range_from_s3_with_caching` with a signed request.
#[tokio::test]
async fn control_a_signed_open_ended_range_with_no_cached_overlap_keeps_its_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-open-ended-miss.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=2048-";
    let stub = verbatim_only_s3(raw, 2048, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &zero_ttl(), &stub).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 2048);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}

/// Control, suffix form.
#[tokio::test]
async fn control_a_signed_suffix_range_with_no_cached_overlap_keeps_its_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-suffix-miss.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=-1024";
    let stub = verbatim_only_s3(raw, 3072, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &zero_ttl(), &stub).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len(), 1024);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}

// ---------------------------------------------------------------------------
// Handler level: a fully cached object whose range file is unusable (D1, D4)
// ---------------------------------------------------------------------------

fn fresh_live_settings() -> ResolvedSettings {
    ResolvedSettings {
        get_ttl: Duration::from_secs(3600),
        ..ResolvedSettings::default()
    }
}

/// D1: the `.meta` names a range whose `.bin` is gone. The single-extent recovery
/// fetched the cached extent with a rewritten `Range`; for a signed request that is
/// a guaranteed `403` before the proxy forwards the client's request.
#[tokio::test]
async fn a_signed_range_over_a_missing_range_file_never_sends_a_rewritten_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-missing-bin.bin";
    let mut spec = SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, ETAG);
    spec.stored_expiry = common::expired_fixture::StoredExpiry::Fresh;
    spec.created_age = Duration::from_secs(1);
    fixture.seed(key, &spec);
    std::fs::remove_file(fixture.bin_path(key, 0, OBJECT_SIZE - 1)).unwrap();
    let raw = "bytes=1024-";
    let stub = verbatim_only_s3(raw, 1024, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &fresh_live_settings(), &stub).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 1024);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}

/// D4 at the handler: a signed absolute range past the end of the object
/// (`bytes=0-99999` on 4096 bytes) parses to 0-4095, so the merge fallback's
/// normalised `bytes=0-4095` differs from what the client signed and is refused.
/// The refusal must fail open to the client's own request, not answer `502`.
#[tokio::test]
async fn a_refused_signed_rewrite_fails_open_instead_of_answering_502() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-refused-rewrite.bin";
    let mut spec = SeedSpec::expired(vec![(0, 2047), (2048, OBJECT_SIZE - 1)], OBJECT_SIZE, ETAG);
    spec.stored_expiry = common::expired_fixture::StoredExpiry::Fresh;
    spec.created_age = Duration::from_secs(1);
    fixture.seed(key, &spec);
    std::fs::remove_file(fixture.bin_path(key, 2048, OBJECT_SIZE - 1)).unwrap();
    let raw = "bytes=0-99999";
    let stub = verbatim_only_s3(raw, 0, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &fresh_live_settings(), &stub).await;

    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "a refused rewrite must fail open: {:?}",
        stub.captured()
    );
    assert_eq!(body.len() as u64, OBJECT_SIZE);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}

/// Control: with every range file present the same signed request is served from
/// cache, so the tests above measure the repair path rather than a plain miss.
#[tokio::test]
async fn control_a_fully_cached_signed_range_is_served_from_cache() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/signed-control.bin";
    let mut spec = SeedSpec::expired(vec![(0, 2047), (2048, OBJECT_SIZE - 1)], OBJECT_SIZE, ETAG);
    spec.stored_expiry = common::expired_fixture::StoredExpiry::Fresh;
    spec.created_age = Duration::from_secs(1);
    fixture.seed(key, &spec);
    let raw = "bytes=1024-";
    let stub = verbatim_only_s3(raw, 1024, OBJECT_SIZE - 1);

    let (status, body) = signed_range_get(&fixture, key, raw, &fresh_live_settings(), &stub).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(body.iter().all(|&b| b == OLD_FILL), "served from cache");
    assert!(stub.captured().is_empty(), "{:?}", stub.captured());
}

// ---------------------------------------------------------------------------
// Repair APIs (adapted from an earlier fork commit)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_missing_ranges_refuses_to_rewrite_a_signed_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/fmr-signed.bin";
    let stub = StubS3Client::new().with_default(partial(0, 99));
    let s3 = stub.clone().into_trait_object();

    let result = fixture
        .range_handler
        .fetch_missing_ranges(
            key,
            &[RangeSpec { start: 0, end: 99 }],
            &s3,
            HOST,
            &uri(key),
            &headers(&signed_range_authorization(), Some("bytes=0-4095")),
            None,
            Duration::from_secs(1),
            1.0,
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ProxyError::SignedHeaderRewriteRefused { ref header, .. }) if header == "range"
        ),
        "a signed Range must be refused, got {:?}",
        result.map(|r| r.len())
    );
    assert!(stub.captured().is_empty(), "{:?}", stub.captured());
}

#[tokio::test]
async fn fetch_missing_ranges_still_rewrites_when_range_is_not_signed() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/fmr-unsigned.bin";
    let stub = StubS3Client::new().with_default(partial(0, 99));
    let s3 = stub.clone().into_trait_object();

    let fetched = fixture
        .range_handler
        .fetch_missing_ranges(
            key,
            &[RangeSpec { start: 0, end: 99 }],
            &s3,
            HOST,
            &uri(key),
            &headers(&signed_authorization_without_range(), None),
            None,
            Duration::from_secs(1),
            1.0,
        )
        .await
        .expect("an unsigned Range may be synthesised");

    assert_eq!(fetched.len(), 1);
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].headers.get("Range").map(String::as_str),
        Some("bytes=0-99")
    );
}

async fn fallback_keeps_signed_form(raw: &str, start: u64) {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/fallback-signed-verbatim.bin";
    let stub = StubS3Client::new()
        .with_response_for_range(raw, partial(start, OBJECT_SIZE - 1))
        .with_default(signature_does_not_match());
    let s3 = stub.clone().into_trait_object();
    let requested = RangeSpec {
        start,
        end: OBJECT_SIZE - 1,
    };

    let merged = fixture
        .range_handler
        .merge_ranges_with_fallback(
            key,
            &requested,
            &[missing_cached_range(start, OBJECT_SIZE - 1)],
            &[],
            &s3,
            HOST,
            &uri(key),
            &headers(&signed_range_authorization(), Some(raw)),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("fallback for {raw} must succeed: {e}"));

    assert_eq!(merged.data.len() as u64, OBJECT_SIZE - start);
    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        1,
        "exactly one fallback fetch: {captured:?}"
    );
    assert_eq!(
        captured[0].headers.get("range").map(String::as_str),
        Some(raw),
        "the client's signed Range must reach S3 byte-for-byte"
    );
}

#[tokio::test]
async fn fallback_fetch_forwards_a_signed_suffix_range_verbatim() {
    fallback_keeps_signed_form("bytes=-512", OBJECT_SIZE - 512).await;
}

#[tokio::test]
async fn fallback_fetch_forwards_a_signed_open_ended_range_verbatim() {
    fallback_keeps_signed_form("bytes=1024-", 1024).await;
}

#[tokio::test]
async fn fallback_fetch_refuses_a_signed_range_that_differs_from_the_client_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/fallback-signed-refused.bin";
    let stub = StubS3Client::new().with_default(partial(0, 99));
    let s3 = stub.clone().into_trait_object();

    let result = fixture
        .range_handler
        .merge_ranges_with_fallback(
            key,
            &RangeSpec { start: 0, end: 99 },
            &[missing_cached_range(0, 99)],
            &[],
            &s3,
            HOST,
            &uri(key),
            &headers(&signed_range_authorization(), Some("bytes=0-4095")),
            None,
        )
        .await;

    assert!(
        matches!(result, Err(ProxyError::SignedHeaderRewriteRefused { .. })),
        "a sub-range fetch under a different signed Range must be refused"
    );
    assert!(stub.captured().is_empty(), "{:?}", stub.captured());
}

#[tokio::test]
async fn fallback_fetch_normalises_the_range_when_it_is_not_signed() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/fallback-unsigned.bin";
    let stub = StubS3Client::new().with_default(partial(0, 99));
    let s3 = stub.clone().into_trait_object();

    fixture
        .range_handler
        .merge_ranges_with_fallback(
            key,
            &RangeSpec { start: 0, end: 99 },
            &[missing_cached_range(0, 99)],
            &[],
            &s3,
            HOST,
            &uri(key),
            &headers(&signed_authorization_without_range(), Some("bytes=0-4095")),
            None,
        )
        .await
        .expect("unsigned fallback succeeds");

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].headers.get("range").map(String::as_str),
        Some("bytes=0-99")
    );
}

// ---------------------------------------------------------------------------
// store_range guards (D6, D7)
// ---------------------------------------------------------------------------

/// D7: an ETag-less range (an S3 error body reaching a store path) is refused over
/// a `.meta` that carries an ETag, and the consolidated record is unchanged.
#[tokio::test]
async fn a_range_without_an_etag_never_lands_over_metadata_that_has_one() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/etagless-store.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 4095)], OBJECT_SIZE, ETAG));

    let error_body = b"<Error><Code>SignatureDoesNotMatch</Code></Error>".to_vec();
    let result = fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            key,
            0,
            (error_body.len() - 1) as u64,
            &error_body,
            ObjectMetadata::default(),
            Duration::from_secs(60),
            false,
        )
        .await;
    assert!(result.is_err(), "an ETag-less range must be refused");

    fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("shared-storage consolidator")
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    let after = fixture.read_meta(key).expect("metadata survives");
    assert_eq!(after.object_metadata.etag, ETAG);
    assert_eq!(
        after
            .ranges
            .iter()
            .map(|r| (r.start, r.end))
            .collect::<Vec<_>>(),
        vec![(0, 4095)],
        "the error body must not have been folded in as a range"
    );
    assert!(fixture.bin_path(key, 0, 4095).exists());
}

async fn consolidate(fixture: &Fixture) {
    fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("shared-storage consolidator")
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
}

/// D6: a short body is clamped only when S3's `Content-Range` names the object end
/// at the clamped byte (a request past EOF), or when it is a `200` carrying the
/// whole object. Any other short body is refused.
#[tokio::test]
async fn a_short_body_is_clamped_only_when_upstream_confirms_the_object_end() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let body = vec![b'A'; 10];

    let confirmed_key = "test-bucket/ten-bytes-confirmed.bin";
    let confirmed = HashMap::from([
        ("etag".to_string(), ETAG.to_string()),
        ("content-range".to_string(), "bytes 0-9/10".to_string()),
    ]);
    fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            confirmed_key,
            0,
            99,
            &body,
            ObjectMetadata::new_with_headers(ETAG.to_string(), String::new(), 10, None, confirmed),
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("a past-EOF request confirmed by Content-Range is cached clamped");

    let whole_key = "test-bucket/ten-bytes-whole-200.bin";
    let whole = HashMap::from([("etag".to_string(), ETAG.to_string())]);
    fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            whole_key,
            0,
            99,
            &body,
            ObjectMetadata::new_with_headers(ETAG.to_string(), String::new(), 10, None, whole),
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("a 200 carrying the whole 10-byte object is cached clamped");

    consolidate(&fixture).await;
    for key in [confirmed_key, whole_key] {
        let meta = fixture.read_meta(key).expect("clamped metadata");
        assert_eq!(
            meta.ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>(),
            vec![(0, 9)],
            "{key}"
        );
    }

    let unconfirmed_key = "test-bucket/ten-bytes-unconfirmed.bin";
    let unconfirmed = HashMap::from([("etag".to_string(), ETAG.to_string())]);
    let result = fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            unconfirmed_key,
            0,
            99,
            &body,
            ObjectMetadata::new_with_headers(
                ETAG.to_string(),
                String::new(),
                100,
                None,
                unconfirmed,
            ),
            Duration::from_secs(60),
            false,
        )
        .await;
    assert!(
        result.is_err(),
        "a short body without upstream confirmation must be refused, not clamped"
    );

    let wrong_end_key = "test-bucket/ten-bytes-wrong-end.bin";
    let wrong_end = HashMap::from([
        ("etag".to_string(), ETAG.to_string()),
        ("content-range".to_string(), "bytes 0-99/4096".to_string()),
    ]);
    let result = fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            wrong_end_key,
            0,
            99,
            &body,
            ObjectMetadata::new_with_headers(
                ETAG.to_string(),
                String::new(),
                OBJECT_SIZE,
                None,
                wrong_end,
            ),
            Duration::from_secs(60),
            false,
        )
        .await;
    assert!(
        result.is_err(),
        "a body truncated short of its own Content-Range must be refused"
    );

    consolidate(&fixture).await;
    for key in [unconfirmed_key, wrong_end_key] {
        assert!(
            fixture.read_meta(key).is_none(),
            "nothing may be published for the refused store {key}"
        );
    }
}
// ---------------------------------------------------------------------------
// Presigned URLs: `range` signed in `X-Amz-SignedHeaders`, no `Authorization`
//
// A presigned URL records its signed-header list in the query string. The
// handler-level tests above sign `range` in the `Authorization` header; these send
// the same requests with the list in the query instead, so the proxy must read
// both forms before it rewrites `Range`.
// ---------------------------------------------------------------------------
/// The client's headers for a presigned GET: no `Authorization`.
fn presigned_headers(range: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("host".to_string(), HOST.to_string());
    if let Some(range) = range {
        headers.insert("range".to_string(), range.to_string());
    }
    headers
}
async fn presigned_get(
    fixture: &Fixture,
    cache_key: &str,
    query: &str,
    headers: HashMap<String, String>,
    raw_range: &str,
    resolved: &ResolvedSettings,
    stub: &StubS3Client,
) -> (StatusCode, Vec<u8>) {
    let response = tokio::time::timeout(
        Duration::from_secs(20),
        fixture.range_get_with_uri(
            cache_key,
            raw_range,
            format!("/{cache_key}?{query}")
                .parse()
                .expect("presigned uri"),
            headers,
            resolved,
            fixture.production_current_etag(cache_key),
            stub.clone().into_trait_object(),
        ),
    )
    .await
    .expect("the range GET must not hang");
    let status = response.status();
    (status, body_of(response).await)
}
async fn presigned_range_get(
    fixture: &Fixture,
    cache_key: &str,
    raw_range: &str,
    resolved: &ResolvedSettings,
    stub: &StubS3Client,
) -> (StatusCode, Vec<u8>) {
    presigned_get(
        fixture,
        cache_key,
        &presigned_range_query(),
        presigned_headers(Some(raw_range)),
        raw_range,
        resolved,
        stub,
    )
    .await
}
/// The partly cached open-ended case for a presigned URL. Fetching the holes, and
/// then the normalised `bytes=512-4095`, both change the signed `Range`.
#[tokio::test]
async fn a_presigned_open_ended_range_over_a_partly_cached_object_keeps_its_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-open-ended-partial.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=512-";
    let stub = verbatim_only_s3(raw, 512, OBJECT_SIZE - 1);
    let (status, body) = presigned_range_get(&fixture, key, raw, &zero_ttl(), &stub).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 512);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}
/// A canonical `bytes=a-b` survives the merge fallback by string coincidence, so the
/// client gets a `206` either way; the hole fetch before it must not happen.
#[tokio::test]
async fn a_presigned_canonical_range_over_a_partly_cached_object_never_fetches_a_hole() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-canonical-partial.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=0-4095";
    let stub = verbatim_only_s3(raw, 0, OBJECT_SIZE - 1);
    let (status, body) = presigned_range_get(&fixture, key, raw, &zero_ttl(), &stub).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE);
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}
/// Control: the same partly cached request with `range` absent from the presigned
/// list takes the rewriting path: the hole is fetched with a proxy-built `Range`
/// and merged with the cached bytes.
#[tokio::test]
async fn control_a_presigned_range_without_range_signed_fetches_the_hole_and_merges() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-unsigned-partial.bin";
    fixture.seed(key, &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG));
    let raw = "bytes=512-";
    let stub = StubS3Client::new()
        .with_response_for_etag(ETAG, common::expired_fixture::not_modified(ETAG))
        .with_response_for_range("bytes=1024-4095", partial(1024, OBJECT_SIZE - 1))
        .with_default(signature_does_not_match());
    let (status, body) = presigned_get(
        &fixture,
        key,
        &presigned_query_without_range(),
        presigned_headers(Some(raw)),
        raw,
        &zero_ttl(),
        &stub,
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 512);
    assert!(
        body[..512].iter().all(|&b| b == OLD_FILL),
        "the cached bytes must be merged into the response"
    );
    assert!(body[512..].iter().all(|&b| b == UPSTREAM_FILL));
    assert!(
        stub.captured().iter().any(|r| r
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("range") && v == "bytes=1024-4095")),
        "the hole must be fetched with a rewritten Range: {:?}",
        stub.captured()
    );
}
/// A presigned range over a cached extent whose `.bin` is gone. The recovery
/// refetch of the whole extent rewrites `Range`; the proxy must instead forward the
/// client's own request, and invalidate the entry so the next request is not
/// routed through the missing file again.
#[tokio::test]
async fn a_presigned_range_over_a_missing_range_file_is_forwarded_as_sent_and_repaired() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-missing-bin.bin";
    let mut spec = SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, ETAG);
    spec.stored_expiry = common::expired_fixture::StoredExpiry::Fresh;
    spec.created_age = Duration::from_secs(1);
    fixture.seed(key, &spec);
    let missing_bin = fixture.bin_path(key, 0, OBJECT_SIZE - 1);
    std::fs::remove_file(&missing_bin).unwrap();
    let raw = "bytes=1024-";
    let stub = verbatim_only_s3(raw, 1024, OBJECT_SIZE - 1);
    let (status, body) =
        presigned_range_get(&fixture, key, raw, &fresh_live_settings(), &stub).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 1024);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
    let still_claims_missing = fixture.read_meta(key).is_some_and(|m| {
        m.ranges
            .iter()
            .any(|r| r.start == 0 && r.end == OBJECT_SIZE - 1)
    }) && !missing_bin.exists();
    assert!(
        !still_claims_missing,
        "the entry still lists the extent 0-{} whose range file is missing, so it was not repaired",
        OBJECT_SIZE - 1
    );
    let (status, body) =
        presigned_range_get(&fixture, key, raw, &fresh_live_settings(), &stub).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    assert_eq!(body.len() as u64, OBJECT_SIZE - 1024);
    assert!(body.iter().all(|&b| b == UPSTREAM_FILL));
    assert_every_fetch_kept_the_signed_range(&stub, raw);
}
// ---------------------------------------------------------------------------
// Presigned URLs: `via` signed in `X-Amz-SignedHeaders`
// ---------------------------------------------------------------------------
const CLIENT_VIA: &str = "1.1 upstream-hop";
fn captured_via(stub: &StubS3Client) -> Vec<String> {
    stub.captured()
        .iter()
        .map(|r| {
            r.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("via"))
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        })
        .collect()
}
/// A presigned URL that signs `via` reaches S3 with the client's `Via` unchanged.
#[tokio::test]
async fn a_presigned_via_reaches_s3_unchanged() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-via.bin";
    let mut headers = presigned_headers(Some("bytes=0-99"));
    headers.insert("via".to_string(), CLIENT_VIA.to_string());
    let stub = StubS3Client::new().with_default(partial(0, 99));
    let (status, _) = presigned_get(
        &fixture,
        key,
        &presigned_query("host%3Bvia"),
        headers,
        "bytes=0-99",
        &fresh_live_settings(),
        &stub,
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    let vias = captured_via(&stub);
    assert!(!vias.is_empty(), "the request never reached S3");
    for via in vias {
        assert_eq!(
            via, CLIENT_VIA,
            "a signed Via was modified on its way to S3"
        );
    }
}
/// Control: with `via` unsigned the proxy appends itself exactly once, so the
/// request-context builder still adds the entry.
#[tokio::test]
async fn control_an_unsigned_via_gets_one_proxy_entry() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-via-unsigned.bin";
    let mut headers = presigned_headers(Some("bytes=0-99"));
    headers.insert("via".to_string(), CLIENT_VIA.to_string());
    let stub = StubS3Client::new().with_default(partial(0, 99));
    let (status, _) = presigned_get(
        &fixture,
        key,
        &presigned_query_without_range(),
        headers,
        "bytes=0-99",
        &fresh_live_settings(),
        &stub,
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    let vias = captured_via(&stub);
    assert!(!vias.is_empty(), "the request never reached S3");
    for via in vias {
        assert!(
            via.starts_with(&format!("{CLIENT_VIA}, ")) && via.matches(',').count() == 1,
            "an unsigned Via must carry the client's entry and one proxy entry: {via:?}"
        );
    }
}
