//! A ranged GET after the object was overwritten outside this proxy, with a `Range` the
//! client did not sign (a presigned URL, an anonymous reader).
//!
//! The proxy resolves the client's `Range` against the cached version's length, so
//! `range_spec` for `bytes=512-5999` on a cached 4,096-byte version is `512-4095`, and for
//! `bytes=-1000` it is `3096-4095`. Once S3 has shown that version is gone, a request
//! rebuilt from `range_spec` asks for a shorter window than the client did, or, for a
//! suffix, for bytes at another offset, and S3 answers it truthfully. These tests drive the
//! real request path on a loopback port against a model origin that holds only the new
//! version and answers as S3 does (`model_s3_responder`). Each asserts the response the
//! client gets (status, `Content-Range`, the new version's bytes over the window S3
//! resolves the client's own `Range` to) and that S3 was sent the client's `Range` verbatim.
//!
//! Three routes to S3 are covered: partial coverage inside the TTL, whose hole fetches are
//! pinned to the cached ETag and answered `412`; a revalidation that returns another ETag
//! (a write-through entry, which has no `Last-Modified` and so always revalidates); and a
//! revalidation S3 answers with an error status, where nothing proves the version changed.

mod common;

use std::time::Duration;

use hyper::StatusCode;

use common::expired_fixture::{
    fill_summary, proxy_get_with_auth, s3_resolve_range, signed_authorization_without_range,
    test_config, Fixture, OriginVersion, SeedSpec,
};
use common::{CapturedRequest, StubResponse, StubS3Client};

const OLD_LEN: u64 = 4096;
const GROWN: u64 = 6000;
const SHRUNK: u64 = 3000;
const BUCKET: &str = "test-bucket";

fn range_of(r: &CapturedRequest) -> Option<&str> {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("range"))
        .map(|(_, v)| v.as_str())
}

fn if_match_of(r: &CapturedRequest) -> Option<&str> {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        .map(|(_, v)| v.as_str())
}

/// Which route takes the request to S3.
#[derive(Clone, Copy, Debug)]
enum Route {
    /// Extents `0-1023` and `3500-4095` cached and fresh; the hole fetches are pinned.
    PartialPinned,
    /// The whole old version cached by a write-through PUT; the revalidation returns the
    /// new ETag.
    ChangedOnRevalidation,
    /// As `ChangedOnRevalidation`, but S3 answers the revalidation `503`.
    RevalidationError,
}

struct Outcome {
    status: StatusCode,
    content_range: Option<String>,
    etag: Option<String>,
    body: Vec<u8>,
    captured: Vec<CapturedRequest>,
    old_etag: String,
}

async fn ranged_get_after_overwrite(
    route: Route,
    client_range: &str,
    new_len: u64,
    name: &str,
) -> (Outcome, OriginVersion) {
    let fixture = Fixture::new(test_config(OLD_LEN)).await;
    let key = format!("{}/{}", BUCKET, name);
    let path = format!("/{}", key);
    let old_etag = format!("\"{}-v1\"", name);
    let spec = match route {
        Route::PartialPinned => {
            SeedSpec::stored_fresh_live_expired(vec![(0, 1023), (3500, 4095)], OLD_LEN, &old_etag)
        }
        Route::ChangedOnRevalidation | Route::RevalidationError => {
            SeedSpec::write_cached_no_last_modified(vec![(0, OLD_LEN - 1)], OLD_LEN, &old_etag)
        }
    };
    let old_etag = fixture.seed(&key, &spec);
    fixture.invalidate_metadata_cache(&key).await;

    let new = OriginVersion::patterned(&format!("\"{}-v2\"", name), new_len, 7);
    let model = common::expired_fixture::model_s3_responder(new.clone());
    let revalidation_error = matches!(route, Route::RevalidationError);
    let stub = StubS3Client::new().with_responder(move |context| {
        let revalidation = context
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("if-none-match"));
        if revalidation_error && revalidation {
            return Some(StubResponse::with_status(StatusCode::SERVICE_UNAVAILABLE));
        }
        model(context)
    });
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, headers, body) = tokio::time::timeout(
        Duration::from_secs(30),
        proxy_get_with_auth(
            server.addr,
            &path,
            &signed_authorization_without_range(),
            &[("range", client_range)],
        ),
    )
    .await
    .expect("the ranged GET must complete within 30 s");
    let header = |n: &str| {
        headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    (
        Outcome {
            status,
            content_range: header("content-range"),
            etag: header("etag"),
            body,
            captured: stub.captured(),
            old_etag,
        },
        new,
    )
}

/// The route's precondition, then the client's answer and the request S3 was sent.
fn assert_client_range_answered(
    route: Route,
    client_range: &str,
    out: &Outcome,
    new: &OriginVersion,
) {
    let requests: Vec<(Option<&str>, Option<&str>)> = out
        .captured
        .iter()
        .map(|r| (range_of(r), if_match_of(r)))
        .collect();
    match route {
        Route::PartialPinned => assert!(
            out.captured
                .iter()
                .any(|r| if_match_of(r) == Some(out.old_etag.as_str())),
            "PRECONDITION MISSING: no hole fetch was pinned to the cached ETag {}, so the \
             partial-coverage route was not taken. Requests (range, if-match): {:?}",
            out.old_etag,
            requests
        ),
        Route::ChangedOnRevalidation | Route::RevalidationError => assert!(
            out.captured.first().is_some_and(|r| r
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("if-none-match")))
                && out.captured.iter().all(|r| if_match_of(r).is_none()),
            "PRECONDITION MISSING: the first upstream request must be the revalidation \
             (If-None-Match) and none may carry an If-Match pin, or this is not the \
             revalidation route. Requests (range, if-match): {:?}",
            requests
        ),
    }

    let len = new.body.len() as u64;
    let (start, end) = s3_resolve_range(client_range, len)
        .flatten()
        .expect("fixture: the client's Range must be satisfiable against the new version");
    let want_range = format!("bytes {}-{}/{}", start, end, len);
    let want_body = &new.body[start as usize..=end as usize];
    let last = out.captured.last().expect("an upstream request was made");
    assert_eq!(
        (
            out.status,
            out.content_range.as_deref(),
            out.etag.as_deref(),
            out.body.len(),
            out.body.as_slice() == want_body,
            range_of(last),
        ),
        (
            StatusCode::PARTIAL_CONTENT,
            Some(want_range.as_str()),
            Some(new.etag.as_str()),
            want_body.len(),
            true,
            Some(client_range),
        ),
        "{:?}, client Range {:?}, cached length {}, new length {}: the client must get S3's \
         answer to ITS Range for the new version (status, content-range, etag, body length, \
         body equals the new version over that window, the Range S3 was last sent). Body: \
         {}. Requests (range, if-match): {:?}",
        route,
        client_range,
        OLD_LEN,
        len,
        fill_summary(&out.body),
        requests
    );
    // Once S3 has shown the cached version is gone (the pin's `412`, or the revalidation's
    // answer), every later request is the client's own: its Range verbatim and no `If-Match`
    // of the proxy's.
    let proof = match route {
        Route::PartialPinned => out
            .captured
            .iter()
            .position(|r| if_match_of(r) == Some(out.old_etag.as_str()))
            .expect("checked above"),
        Route::ChangedOnRevalidation | Route::RevalidationError => 0,
    };
    let not_the_clients: Vec<&(Option<&str>, Option<&str>)> = requests[proof + 1..]
        .iter()
        .filter(|(range, if_match)| *range != Some(client_range) || if_match.is_some())
        .collect();
    assert!(
        proof + 1 < requests.len() && not_the_clients.is_empty(),
        "{:?}, client Range {:?}: every upstream request after S3 showed the cached version \
         gone must be the client's own (its Range verbatim, no If-Match of the proxy's); \
         these are not: {:?}. Requests (range, if-match): {:?}",
        route,
        client_range,
        not_the_clients,
        requests
    );
}

#[tokio::test]
async fn partial_coverage_range_past_the_old_end_after_a_longer_overwrite() {
    let r = "bytes=512-5999";
    let (out, new) =
        ranged_get_after_overwrite(Route::PartialPinned, r, GROWN, "ow-partial-grown").await;
    assert_client_range_answered(Route::PartialPinned, r, &out, &new);
}

#[tokio::test]
async fn partial_coverage_range_past_the_old_end_after_a_shorter_overwrite() {
    let r = "bytes=512-5999";
    let (out, new) =
        ranged_get_after_overwrite(Route::PartialPinned, r, SHRUNK, "ow-partial-shrunk").await;
    assert_client_range_answered(Route::PartialPinned, r, &out, &new);
}

#[tokio::test]
async fn partial_coverage_range_past_the_old_end_after_a_same_length_overwrite() {
    let r = "bytes=512-5999";
    let (out, new) =
        ranged_get_after_overwrite(Route::PartialPinned, r, OLD_LEN, "ow-partial-same").await;
    assert_client_range_answered(Route::PartialPinned, r, &out, &new);
}

#[tokio::test]
async fn partial_coverage_open_ended_range_after_a_longer_overwrite() {
    let r = "bytes=512-";
    let (out, new) =
        ranged_get_after_overwrite(Route::PartialPinned, r, GROWN, "ow-partial-open").await;
    assert_client_range_answered(Route::PartialPinned, r, &out, &new);
}

/// A suffix resolved against the cached length names other bytes of the new version:
/// `bytes=-1000` on a cached 4,096 bytes is `3096-4095`, the middle of a 6,000-byte object.
#[tokio::test]
async fn partial_coverage_suffix_range_after_a_longer_overwrite() {
    let r = "bytes=-1000";
    let (out, new) =
        ranged_get_after_overwrite(Route::PartialPinned, r, GROWN, "ow-partial-suffix").await;
    assert_client_range_answered(Route::PartialPinned, r, &out, &new);
}

/// Resolved against the cached 4,096 bytes, `bytes=-1000` is `3096-4095`, which a
/// 3,000-byte object cannot satisfy (`416`); S3 answers the client's own suffix with the
/// last 1,000 bytes.
#[tokio::test]
async fn partial_coverage_suffix_range_after_a_shorter_overwrite() {
    let r = "bytes=-1000";
    let (out, new) =
        ranged_get_after_overwrite(Route::PartialPinned, r, SHRUNK, "ow-partial-suffix-shrunk")
            .await;
    assert_client_range_answered(Route::PartialPinned, r, &out, &new);
}

/// The shape of a write-through PUT, an overwrite at the origin, then an unsigned ranged
/// read that crosses the old length.
#[tokio::test]
async fn changed_etag_on_revalidation_forwards_the_clients_range() {
    let r = "bytes=3000-5999";
    let route = Route::ChangedOnRevalidation;
    let (out, new) = ranged_get_after_overwrite(route, r, GROWN, "ow-changed").await;
    assert_client_range_answered(route, r, &out, &new);
}

/// Nothing proves the version changed here, yet the request S3 is sent must still be the
/// client's: the cached length is not evidence of the current one.
#[tokio::test]
async fn revalidation_error_status_forwards_the_clients_range() {
    let r = "bytes=3000-5999";
    let route = Route::RevalidationError;
    let (out, new) = ranged_get_after_overwrite(route, r, GROWN, "ow-revalidation-503").await;
    assert_client_range_answered(route, r, &out, &new);
}
