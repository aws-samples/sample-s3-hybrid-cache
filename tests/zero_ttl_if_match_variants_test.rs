//! A client `If-Match` on a `get_ttl: "0s"` key, in the shapes `zero_ttl_if_match_revalidation_test`
//! does not cover: a HEAD, a partly cached range, `*`, a list, a weak ETag, a presigned URL and a
//! refused validation request.
//!
//! The rule under test: only a GET whose lone `If-Match` strong-matches the cached ETag is
//! revalidated (the client's request plus the proxy's `If-None-Match`). That includes `*` and a
//! list that contains the cached ETag. Every other shape reaches S3 as the client sent it. In every shape the client's own `If-Match` must be on each upstream
//! request, so S3 judges the client's precondition and permission.

mod common;

use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    assert_all_new, assert_all_old, not_modified, presigned_query_without_range, proxy_get,
    proxy_head, test_config_full_object_checks_with_get_ttl, Fixture, SeedSpec, NEW_FILL,
};
use common::{CapturedRequest, StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 4096;
const BUCKET: &str = "test-bucket";
const CACHED_ETAG: &str = "\"zero-ttl-if-match-v1\"";
const OTHER_ETAG: &str = "\"zero-ttl-if-match-v2\"";

async fn fixture_holding(object: &str, extents: Vec<(u64, u64)>) -> Fixture {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = format!("{}/{}", BUCKET, object);
    fixture.seed(
        &key,
        &SeedSpec::expired(extents.clone(), OBJECT_SIZE, CACHED_ETAG),
    );
    for (start, end) in extents {
        fixture.assert_covers(&key, start, end);
    }
    fixture.invalidate_metadata_cache(&key).await;
    fixture
}

async fn fixture_holding_the_object(object: &str) -> Fixture {
    fixture_holding(object, vec![(0, OBJECT_SIZE - 1)]).await
}

/// S3 for an object still at [`CACHED_ETAG`]: `304` when `If-None-Match` names it, else the bytes.
fn s3_holding_the_cached_version() -> StubS3Client {
    StubS3Client::new()
        .with_response_for_etag(CACHED_ETAG, not_modified(CACHED_ETAG))
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
                .with_header("etag", CACHED_ETAG),
        )
}

fn header<'a>(request: &'a CapturedRequest, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn assert_every_request_carries_if_match(captured: &[CapturedRequest], expected: &str) {
    assert!(
        !captured.is_empty(),
        "a get_ttl: 0s read must reach S3, so that S3 checks the caller's permission"
    );
    assert!(
        captured
            .iter()
            .all(|r| header(r, "if-match") == Some(expected)),
        "every upstream request must carry the client's If-Match {expected}: {captured:#?}"
    );
}

fn assert_relayed_as_sent(captured: &[CapturedRequest], if_match: &str) {
    assert_every_request_carries_if_match(captured, if_match);
    assert!(
        captured
            .iter()
            .all(|r| header(r, "if-none-match").is_none()),
        "the request was revalidated, not relayed as sent: {captured:#?}"
    );
}

fn assert_revalidated(captured: &[CapturedRequest], if_match: &str) {
    assert_every_request_carries_if_match(captured, if_match);
    assert!(
        captured
            .iter()
            .any(|r| header(r, "if-none-match") == Some(CACHED_ETAG)),
        "the request was relayed as sent, not revalidated: {captured:#?}"
    );
}

/// A HEAD keeps the 3.0.1 rule: its lone `If-Match` reaches S3 as sent, with no `If-None-Match`.
/// This fixture's HEAD cache is not fresh, so the request reaches S3 either way and the test
/// pins the relayed shape; the GET-only term itself is pinned by
/// `zero_ttl_if_match_revalidation_is_for_a_get_only` in `src/http_proxy.rs`.
#[tokio::test]
async fn a_zero_ttl_head_with_a_lone_if_match_reaches_s3_as_sent() {
    let fixture = fixture_holding_the_object("head.bin").await;
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_header("etag", CACHED_ETAG)
            .with_header("content-length", OBJECT_SIZE.to_string()),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, _body) = proxy_head(
        server.addr,
        &format!("/{}/head.bin", BUCKET),
        &[("if-match", CACHED_ETAG)],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let captured = stub.captured();
    assert_relayed_as_sent(&captured, CACHED_ETAG);
    assert!(
        captured.iter().all(|r| r.method == hyper::Method::HEAD),
        "{captured:#?}"
    );
}

/// Half the object is cached. Whatever the proxy fetches to fill the rest carries the client's
/// `If-Match`, so S3 refuses it (`412`) if the object changed rather than the client receiving
/// two versions.
#[tokio::test]
async fn a_partly_cached_if_match_range_fetches_the_gap_with_the_clients_if_match() {
    let fixture = fixture_holding("partial.bin", vec![(0, 2047)]).await;
    let stub = StubS3Client::new()
        .with_response_for_etag(CACHED_ETAG, not_modified(CACHED_ETAG))
        .with_response_for_range(
            "bytes=2048-4095",
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW_FILL; 2048]))
                .with_header("etag", CACHED_ETAG)
                .with_header("content-range", format!("bytes 2048-4095/{}", OBJECT_SIZE)),
        )
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
                .with_header("etag", CACHED_ETAG)
                .with_header("content-range", format!("bytes 0-4095/{}", OBJECT_SIZE)),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/partial.bin", BUCKET),
        &[("range", "bytes=0-4095"), ("if-match", CACHED_ETAG)],
    )
    .await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{:?}", stub.captured());
    let captured = stub.captured();
    assert_every_request_carries_if_match(&captured, CACHED_ETAG);
    assert_all_new(&body, OBJECT_SIZE as usize, "one version, from S3");
    assert!(
        captured
            .iter()
            .any(|r| header(r, "range").is_some_and(|v| v != "bytes=0-2047")),
        "fixture: the object's uncached half must be fetched from S3: {captured:#?}"
    );
}

/// `If-Match: *` strong-matches any cached ETag, so it revalidates: S3 gets `*` intact plus the
/// proxy's `If-None-Match` for the cached ETag, and the cached bytes are served on the `304`.
#[tokio::test]
async fn a_zero_ttl_if_match_star_is_revalidated() {
    let fixture = fixture_holding_the_object("star.bin").await;
    let stub = s3_holding_the_cached_version();
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/star.bin", BUCKET),
        &[("if-match", "*")],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_revalidated(&stub.captured(), "*");
    assert_all_old(&body, OBJECT_SIZE as usize, "the cached copy");
}

/// A list that contains the cached ETag revalidates, with the list intact for S3.
#[tokio::test]
async fn a_zero_ttl_if_match_list_containing_the_cached_etag_is_revalidated() {
    let fixture = fixture_holding_the_object("list-in.bin").await;
    let stub = s3_holding_the_cached_version();
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let list = format!("{OTHER_ETAG}, {CACHED_ETAG}");

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/list-in.bin", BUCKET),
        &[("if-match", list.as_str())],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_revalidated(&stub.captured(), &list);
    assert_all_old(&body, OBJECT_SIZE as usize, "the cached copy");
}

/// A list that does not contain the cached ETag is S3's to judge alone.
#[tokio::test]
async fn a_zero_ttl_if_match_list_without_the_cached_etag_reaches_s3_as_sent() {
    let fixture = fixture_holding_the_object("list-out.bin").await;
    let stub = s3_holding_the_cached_version();
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let list = format!("{OTHER_ETAG}, \"zero-ttl-if-match-v3\"");

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/list-out.bin", BUCKET),
        &[("if-match", list.as_str())],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_relayed_as_sent(&stub.captured(), &list);
    assert_all_new(&body, OBJECT_SIZE as usize, "the bytes come from S3");
}

/// A weak ETag never strong-matches, so it is forwarded as sent.
#[tokio::test]
async fn a_zero_ttl_weak_if_match_reaches_s3_as_sent() {
    let fixture = fixture_holding_the_object("weak.bin").await;
    let stub = s3_holding_the_cached_version();
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let weak = format!("W/{CACHED_ETAG}");

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/weak.bin", BUCKET),
        &[("if-match", weak.as_str())],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_relayed_as_sent(&stub.captured(), &weak);
    assert_all_new(&body, OBJECT_SIZE as usize, "the bytes come from S3");
}

/// A presigned URL has no `Authorization` header; its query string must reach S3 untouched.
#[tokio::test]
async fn a_zero_ttl_presigned_if_match_get_keeps_its_query_and_if_match() {
    let fixture = fixture_holding_the_object("presigned.bin").await;
    let stub = s3_holding_the_cached_version();
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let query = presigned_query_without_range();

    let (status, body) = presigned_get(
        server.addr,
        &format!("/{}/presigned.bin?{}", BUCKET, query),
        CACHED_ETAG,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let captured = stub.captured();
    assert_every_request_carries_if_match(&captured, CACHED_ETAG);
    assert!(
        captured.iter().all(|r| r.uri.contains(&query)),
        "the presigned query must reach S3 unchanged: {captured:#?}"
    );
    assert_eq!(body.len(), OBJECT_SIZE as usize);
}

/// S3 refuses the validation request: the client gets the `403`, never the cached bytes.
#[tokio::test]
async fn a_refused_zero_ttl_validation_reaches_the_client_as_403() {
    let fixture = fixture_holding_the_object("refused.bin").await;
    let stub = StubS3Client::new()
        .with_response_for_etag(
            CACHED_ETAG,
            StubResponse::with_status(StatusCode::FORBIDDEN),
        )
        .with_default(StubResponse::with_status(StatusCode::FORBIDDEN));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _headers, body) = proxy_get(
        server.addr,
        &format!("/{}/refused.bin", BUCKET),
        &[("if-match", CACHED_ETAG)],
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{:?}", stub.captured());
    assert_revalidated(&stub.captured(), CACHED_ETAG);
    assert!(
        body.iter().all(|b| *b != common::expired_fixture::OLD_FILL) || body.is_empty(),
        "the cached bytes reached a caller S3 refused"
    );
}

/// GET with no `Authorization` header and the given `If-Match`, as a presigned-URL client sends it.
async fn presigned_get(
    addr: std::net::SocketAddr,
    path_and_query: &str,
    if_match: &str,
) -> (StatusCode, Vec<u8>) {
    use http_body_util::{BodyExt, Full};
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let req = hyper::Request::builder()
        .method("GET")
        .uri(format!("http://{}{}", addr, path_and_query))
        .header("if-match", if_match)
        .body(Full::new(Bytes::new()))
        .expect("build request");
    let resp = client.request(req).await.expect("proxy request failed");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    (status, body.to_vec())
}
