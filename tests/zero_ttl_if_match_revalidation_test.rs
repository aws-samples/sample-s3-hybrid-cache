//! A client `If-Match` on a key whose `get_ttl` is `0s` is revalidated, not relayed.
//!
//! Transfer managers (boto3's s3transfer, the CRT) put `If-Match: <etag>` on every
//! ranged part of a split download. On a `get_ttl: "0s"` key the proxy used to forward
//! such a request to S3 unchanged. A passing `If-Match` makes S3 send the bytes, so
//! every part of every split download crossed from S3 even when the cache held it.
//!
//! When the client's `If-Match` names the cached ETag, the proxy now adds its own
//! `If-None-Match` (the cached ETag) to the client's request, as it does for a
//! request with no precondition. S3 still judges the client's `If-Match` and the
//! client's signature, and answers `304` with no body when the object is unchanged.
//! Measured against real S3 with a SigV4 request whose `If-Match` and `Range` are
//! signed and whose `If-None-Match` was added after signing:
//!
//! | client `If-Match` | added `If-None-Match` | S3 answer        |
//! |-------------------|-----------------------|------------------|
//! | current ETag      | none                  | `206` with bytes |
//! | current ETag      | current ETag          | `304`, no body   |
//! | stale ETag        | current ETag          | `412`            |
//! | current ETag      | stale ETag            | `206` with bytes |
//! | (unsigned request) | current ETag          | `403`            |
//!
//! The stub below answers the same way: `304` for the cached ETag in `If-None-Match`,
//! `412` for a stale ETag in `If-Match`, and otherwise the object's bytes, filled with
//! [`NEW_FILL`] so that bytes relayed from S3 are told apart from the cached
//! [`OLD_FILL`] bytes.

mod common;

use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    assert_all_new, assert_all_old, not_modified, proxy_get, test_config_coordinated,
    test_config_full_object_checks_with_get_ttl, Fixture, SeedSpec, NEW_FILL,
};
use common::{CapturedRequest, StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const BUCKET: &str = "test-bucket";
const CACHED_ETAG: &str = "\"zero-ttl-if-match-v1\"";
const OTHER_ETAG: &str = "\"zero-ttl-if-match-v2\"";

/// A `get_ttl: "0s"` proxy holding the whole object, cached under [`CACHED_ETAG`].
async fn fixture_holding_the_object(object: &str) -> (Fixture, String) {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    seed_the_object(Fixture::new(config).await, object).await
}

async fn seed_the_object(fixture: Fixture, object: &str) -> (Fixture, String) {
    let key = format!("{}/{}", BUCKET, object);
    let etag = fixture.seed(
        &key,
        &SeedSpec::expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, CACHED_ETAG),
    );
    fixture.assert_covers(&key, 0, OBJECT_SIZE - 1);
    fixture.invalidate_metadata_cache(&key).await;
    assert!(
        fixture.config.cache.evaluate_conditions_from_cache,
        "PRECONDITION MISSING: evaluate_conditions_from_cache is off in this config"
    );
    (fixture, etag)
}

/// S3 for an object still at [`CACHED_ETAG`]: `304` when `If-None-Match` names it,
/// and otherwise the bytes (`If-Match` passes).
fn s3_holding_the_cached_version(range: Option<(u64, u64)>) -> StubS3Client {
    let bytes =
        match range {
            Some((start, end)) => StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW_FILL; (end - start + 1) as usize]))
                .with_header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
                ),
            None => StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize])),
        };
    StubS3Client::new()
        .with_response_for_etag(CACHED_ETAG, not_modified(CACHED_ETAG))
        .with_default(bytes.with_header("etag", CACHED_ETAG))
}

fn header<'a>(request: &'a CapturedRequest, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Every upstream request must carry the client's own `If-Match`, so S3 judges the
/// client's precondition, and at least one must also carry the cached ETag in
/// `If-None-Match`, which is what lets S3 answer without the body.
fn assert_revalidated_with_the_clients_if_match(captured: &[CapturedRequest]) {
    assert!(
        !captured.is_empty(),
        "a get_ttl: 0s read must reach S3, so that S3 checks the caller's permission"
    );
    assert!(
        captured
            .iter()
            .all(|r| header(r, "if-match") == Some(CACHED_ETAG)),
        "every upstream request must carry the client's If-Match: {:#?}",
        captured
    );
    assert!(
        captured
            .iter()
            .any(|r| header(r, "if-none-match") == Some(CACHED_ETAG)),
        "the client's If-Match was relayed to S3 without the cached ETag in \
         If-None-Match, so S3 sent the bytes the cache already held: {:#?}",
        captured
    );
}

#[tokio::test]
async fn a_ranged_if_match_part_on_a_zero_ttl_key_is_served_from_cache() {
    let (fixture, etag) = fixture_holding_the_object("ranged-part.bin").await;
    let stub = s3_holding_the_cached_version(Some((0, 1023)));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/ranged-part.bin", BUCKET),
        &[("range", "bytes=0-1023"), ("if-match", etag.as_str())],
    )
    .await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_revalidated_with_the_clients_if_match(&stub.captured());
    assert_all_old(&body, 1024, "the part must come from the cache's copy");
}

#[tokio::test]
async fn a_whole_object_if_match_get_on_a_zero_ttl_key_is_served_from_cache() {
    let (fixture, etag) = fixture_holding_the_object("whole-object.bin").await;
    let stub = s3_holding_the_cached_version(None);
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/whole-object.bin", BUCKET),
        &[("if-match", etag.as_str())],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_revalidated_with_the_clients_if_match(&stub.captured());
    assert_all_old(
        &body,
        OBJECT_SIZE as usize,
        "the object must come from the cache's copy",
    );
}

/// The object changed after it was cached: S3 fails the client's `If-Match` with
/// `412`, and the client gets that `412`, never the cached bytes.
#[tokio::test]
async fn a_zero_ttl_if_match_part_of_a_changed_object_gets_s3s_412() {
    let (fixture, etag) = fixture_holding_the_object("changed.bin").await;
    let stub = StubS3Client::new()
        .with_response_for_if_match(
            CACHED_ETAG,
            StubResponse::with_status(StatusCode::PRECONDITION_FAILED),
        )
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW_FILL; 1024]))
                .with_header("etag", OTHER_ETAG)
                .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE)),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/changed.bin", BUCKET),
        &[("range", "bytes=0-1023"), ("if-match", etag.as_str())],
    )
    .await;

    assert_eq!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "body: {:?}",
        String::from_utf8_lossy(&body)
    );
}

/// A client `If-Match` naming a different ETag than the cache holds is S3's to judge
/// alone: it is forwarded as sent, with no `If-None-Match` added.
#[tokio::test]
async fn a_zero_ttl_if_match_naming_another_version_is_forwarded_as_sent() {
    let (fixture, _etag) = fixture_holding_the_object("other-version.bin").await;
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![NEW_FILL; 1024]))
            .with_header("etag", OTHER_ETAG)
            .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE)),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/other-version.bin", BUCKET),
        &[("range", "bytes=0-1023"), ("if-match", OTHER_ETAG)],
    )
    .await;

    let captured = stub.captured();
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(
        captured
            .iter()
            .all(|r| header(r, "if-none-match").is_none()
                && header(r, "if-match") == Some(OTHER_ETAG)),
        "{:#?}",
        captured
    );
    assert_all_new(&body, 1024, "the bytes come from S3");
}

/// With download coordination on, as it is by default, concurrent parts naming the
/// cached ETag are all served from the cache's copy.
#[tokio::test]
async fn concurrent_if_match_parts_with_coordination_are_served_from_cache() {
    let mut config = (*test_config_coordinated(OBJECT_SIZE)).clone();
    config.cache.get_ttl = Duration::ZERO;
    let (fixture, etag) = seed_the_object(
        Fixture::new(std::sync::Arc::new(config)).await,
        "coordinated.bin",
    )
    .await;
    let stub = s3_holding_the_cached_version(Some((0, 1023)));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let path = format!("/{}/coordinated.bin", BUCKET);
    let headers = [("range", "bytes=0-1023"), ("if-match", etag.as_str())];

    let responses =
        futures::future::join_all((0..4).map(|_| proxy_get(server.addr, &path, &headers))).await;

    for (status, _headers, body) in responses {
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_all_old(&body, 1024, "every part must come from the cache's copy");
    }
    assert_revalidated_with_the_clients_if_match(&stub.captured());
}
