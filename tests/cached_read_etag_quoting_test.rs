//! Part A — a cached read serves a QUOTED `ETag` header for an entry stored with a
//! bare one.
//!
//! # The defect
//!
//! Until CompleteMultipartUpload was fixed to store the quoted form, every multipart
//! entry was written with `object_metadata.etag` unquoted (`abc-2`). S3 records no ETag
//! response HEADER on that request (the ETag is in the XML body), so the entry's stored
//! `response_headers` hold no `etag`, and the cached-read sites fall back to emitting
//! `object_metadata.etag` verbatim. Those entries are still on disk after an upgrade, so
//! every cached read of one handed the client `ETag: abc-2` where S3 sends
//! `ETag: "abc-2"`. The bytes were correct; the validator differed byte-wise.
//!
//! The fix is emit-side only (`http_proxy::format_etag_header`). Nothing stored changes
//! and no comparison changes.
//!
//! # The vacuity trap every fixture here avoids
//!
//! An entry whose stored `response_headers` carry an `etag` takes the replay branch and
//! never reaches the fallback that emits `object_metadata.etag`. So each fixture is
//! seeded with an EMPTY `response_headers` map (`SeedSpec` does this), and
//! [`assert_no_stored_etag_header`] checks the persisted `.meta` rather than trusting the
//! seeder. The one exception is the HEAD test, where the replay IS the only emission and
//! the fixture puts the bare value in the map on purpose; its doc says why.
//!
//! # Served from cache, asserted
//!
//! Each test asserts zero upstream requests and `x-cache: HIT` (or, for the merge path,
//! that the header came from the cached extent), because the stub answers with a
//! DIFFERENT quoted ETag: a request that reached S3 would show that value instead, and
//! would pass a bare "is it quoted?" check while measuring nothing about the cache.

mod common;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    proxy_get, proxy_get_with_auth, proxy_head, signed_authorization_without_range, test_config,
    test_config_full_object_checks, Fixture, SeedSpec, StoredExpiry,
};
use common::{StubResponse, StubS3Client};

const OBJECT_SIZE: u64 = 8192;
const BUCKET: &str = "test-bucket";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

/// An on-disk multipart entry written before the producer fix: bare hex, `-<parts>`.
const STORED_BARE: &str = "3858f62230ac3c915f300c664312c11f-2";
/// The same ETag as S3 sends it, and what a cached read must now emit.
const EMITTED_QUOTED: &str = "\"3858f62230ac3c915f300c664312c11f-2\"";
/// What the stub answers with. Seeing it in a response means the request reached S3.
const UPSTREAM_ETAG: &str = "\"upstream-was-contacted\"";

/// A stored-fresh, live-fresh entry: nothing forces revalidation, so a GET is served
/// from cache under the default (1 day) `get_ttl`.
fn fresh(extents: Vec<(u64, u64)>, etag: &str) -> SeedSpec {
    SeedSpec {
        extents,
        etag: etag.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_SIZE,
        stored_expiry: StoredExpiry::Fresh,
        created_age: Duration::from_secs(5),
        is_write_cached: Some(false),
    }
}

/// Any upstream request is a fixture failure for these tests; answer it with a body and
/// ETag that are recognisably not the cache's.
fn upstream_default() -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_body(Bytes::from(vec![b'Z'; OBJECT_SIZE as usize]))
        .with_header("etag", UPSTREAM_ETAG)
        .with_header("last-modified", LAST_MODIFIED)
}

fn etag_of(headers: &hyper::HeaderMap) -> Option<String> {
    headers
        .get("etag")
        .map(|v| v.to_str().expect("etag header is ASCII").to_string())
}

/// The vacuity guard: the persisted `.meta` must hold no stored `etag` response header,
/// or the request takes the replay branch and never reaches the fallback under test.
fn assert_no_stored_etag_header(fixture: &Fixture, key: &str) {
    let meta = fixture.read_meta(key).expect(".meta must exist");
    assert!(
        !meta
            .object_metadata
            .response_headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("etag")),
        "PRECONDITION MISSING: the stored response_headers carry an etag {:?}, so the \
         fallback that emits object_metadata.etag is not reached and this test measures \
         the replay branch instead",
        meta.object_metadata.response_headers
    );
    assert_eq!(
        meta.object_metadata.etag, STORED_BARE,
        "PRECONDITION MISSING: the entry must be stored with the BARE ETag an old \
         multipart completion wrote"
    );
}

fn assert_served_from_cache(stub: &StubS3Client, headers: &hyper::HeaderMap, site: &str) {
    let captured = stub.captured();
    assert!(
        captured.is_empty(),
        "{site}: FIXTURE: the request reached upstream, so the emitted ETag is S3's and says \
         nothing about the cached-read site. Captured: {captured:#?}"
    );
    assert_eq!(
        headers.get("x-cache").and_then(|v| v.to_str().ok()),
        Some("HIT"),
        "{site}: FIXTURE: the response is not a cache hit"
    );
}

/// Full-object GET: `serve_full_object_from_cache`'s fallback emission.
#[tokio::test]
async fn full_object_get_emits_quoted_etag_for_bare_stored_entry() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = format!("{}/scn1-full-object.bin", BUCKET);
    fixture.seed(&key, &fresh(vec![(0, OBJECT_SIZE - 1)], STORED_BARE));
    assert_no_stored_etag_header(&fixture, &key);
    fixture.invalidate_metadata_cache(&key).await;

    let stub = StubS3Client::new().with_default(upstream_default());
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, headers, body) = proxy_get(server.addr, &format!("/{}", key), &[]).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.len(), OBJECT_SIZE as usize);
    assert_served_from_cache(&stub, &headers, "full-object GET");
    assert_eq!(
        etag_of(&headers).as_deref(),
        Some(EMITTED_QUOTED),
        "full-object GET: a bare stored ETag must be emitted quoted, as S3 sends it"
    );
}

/// Range GET fully served from cache: `add_cached_s3_headers`' fallback emission (the
/// function `serve_range_from_cache`, `serve_range_from_cache_buffered` and
/// `serve_widened_pages` all build their headers through).
#[tokio::test]
async fn cached_range_get_emits_quoted_etag_for_bare_stored_entry() {
    // `test_config` sets full_object_check_threshold below the object size, so the
    // early full-object shortcut cannot answer and the range serve is what runs.
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = format!("{}/scn1-range.bin", BUCKET);
    fixture.seed(&key, &fresh(vec![(0, OBJECT_SIZE - 1)], STORED_BARE));
    assert_no_stored_etag_header(&fixture, &key);
    fixture.invalidate_metadata_cache(&key).await;

    let stub = StubS3Client::new().with_default(upstream_default());
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, headers, body) = proxy_get(
        server.addr,
        &format!("/{}", key),
        &[("range", "bytes=1024-2047")],
    )
    .await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.len(), 1024);
    assert_served_from_cache(&stub, &headers, "range GET");
    assert_eq!(
        etag_of(&headers).as_deref(),
        Some(EMITTED_QUOTED),
        "range GET: a bare stored ETag must be emitted quoted, as S3 sends it"
    );
}

/// Range GET over PARTIAL coverage: the merge path's `cached_range.etag` emission, which
/// copies `object_metadata.etag` into each cached extent. The missing half is fetched,
/// so this test cannot assert zero upstream requests; it asserts instead that the header
/// is the CACHED validator (quoted), not the bare spelling the stub returned.
#[tokio::test]
async fn partial_range_merge_emits_quoted_cached_etag_for_bare_stored_entry() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = format!("{}/scn1-merge.bin", BUCKET);
    let half = OBJECT_SIZE / 2;
    fixture.seed(&key, &fresh(vec![(0, half - 1)], STORED_BARE));
    assert_no_stored_etag_header(&fixture, &key);
    fixture.invalidate_metadata_cache(&key).await;

    // The missing half is the cached version: the proxy pins the fetch with `If-Match`
    // and refuses to merge bytes of another version. S3 would send it quoted; the stub
    // sends it bare, so a quoted header in the response can only be the cache's own.
    let missing = StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(vec![b'B'; half as usize]))
        .with_header("etag", STORED_BARE)
        .with_header("last-modified", LAST_MODIFIED)
        .with_header(
            "content-range",
            format!("bytes {}-{}/{}", half, OBJECT_SIZE - 1, OBJECT_SIZE),
        )
        .with_header("content-length", half.to_string());
    let stub = StubS3Client::new().with_default(missing);
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    // `range` must NOT be in SignedHeaders: the proxy cannot fetch only the missing
    // half of a signed range without invalidating the client's signature, so a signed
    // range is forwarded whole and the merge path never runs.
    let (status, headers, body) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[("range", format!("bytes=0-{}", OBJECT_SIZE - 1).as_str())],
    )
    .await;

    let captured = stub.captured();
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "FIXTURE: expected a merged 206; captured upstream: {captured:#?}"
    );
    assert_eq!(
        body.len(),
        OBJECT_SIZE as usize,
        "FIXTURE: merged body length; headers={headers:?} captured upstream: {captured:#?}"
    );
    assert_eq!(
        &body[..half as usize],
        &vec![b'A'; half as usize][..],
        "FIXTURE: the first half must come from the cached extent, or the merge path \
         did not run"
    );
    assert!(
        captured
            .iter()
            .all(|r| r.headers.get("range").map(String::as_str)
                != Some(format!("bytes=0-{}", OBJECT_SIZE - 1).as_str())),
        "FIXTURE: the whole range was forwarded, so no merge happened: {captured:#?}"
    );
    assert_eq!(
        etag_of(&headers).as_deref(),
        Some(EMITTED_QUOTED),
        "partial-range merge: the cached extent's bare ETag must be emitted quoted"
    );
}

/// Part-scoped GET (`?partNumber=N`): `lookup_part` puts `object_metadata.etag` in the
/// part's header map and `serve_cached_part_response` replays it.
#[tokio::test]
async fn part_scoped_get_emits_quoted_etag_for_bare_stored_entry() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = format!("{}/scn1-part.bin", BUCKET);
    let half = OBJECT_SIZE / 2;
    fixture.seed(&key, &fresh(vec![(0, OBJECT_SIZE - 1)], STORED_BARE));

    // The multipart shape `lookup_part` resolves parts from.
    let mut meta = fixture.read_meta(&key).expect(".meta");
    meta.object_metadata.parts_count = Some(2);
    meta.object_metadata.part_ranges =
        HashMap::from([(1u32, (0u64, half - 1)), (2u32, (half, OBJECT_SIZE - 1))]);
    std::fs::write(
        fixture.meta_path(&key),
        serde_json::to_string_pretty(&meta).expect("serialize"),
    )
    .expect("rewrite .meta");
    assert_no_stored_etag_header(&fixture, &key);
    fixture.invalidate_metadata_cache(&key).await;

    let stub = StubS3Client::new().with_default(upstream_default());
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, headers, body) =
        proxy_get(server.addr, &format!("/{}?partNumber=2", key), &[]).await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.len(), half as usize);
    assert_served_from_cache(&stub, &headers, "part-scoped GET");
    assert_eq!(
        etag_of(&headers).as_deref(),
        Some(EMITTED_QUOTED),
        "part-scoped GET: a bare stored ETag must be emitted quoted, as S3 sends it"
    );
}

/// HEAD replays `object_metadata.response_headers` and has NO fallback to
/// `object_metadata.etag`, so the vacuity trap is inverted here: the only way a HEAD can
/// emit a bare ETag is for the stored map itself to hold one.
///
/// Against S3 that is not reachable — the map a HEAD serve replays was written from S3's
/// own quoted HEAD/GET response headers, and a multipart completion has
/// `head_expires_at: None`, so its first HEAD misses and rewrites the entry from S3. The
/// state below is synthetic and stands for an S3-compatible origin (or a future writer)
/// that stores a bare `etag` header. It pins that the replay goes through the helper.
#[tokio::test]
async fn head_replay_emits_quoted_etag_when_stored_header_is_bare() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = format!("{}/scn1-head.bin", BUCKET);
    fixture.seed(&key, &fresh(vec![(0, OBJECT_SIZE - 1)], STORED_BARE));

    let mut meta = fixture.read_meta(&key).expect(".meta");
    let now = SystemTime::now();
    meta.head_expires_at = Some(now + Duration::from_secs(3600));
    meta.head_cached_at = Some(now);
    meta.object_metadata.response_headers = HashMap::from([
        ("etag".to_string(), STORED_BARE.to_string()),
        ("last-modified".to_string(), LAST_MODIFIED.to_string()),
    ]);
    std::fs::write(
        fixture.meta_path(&key),
        serde_json::to_string_pretty(&meta).expect("serialize"),
    )
    .expect("rewrite .meta");
    fixture.invalidate_metadata_cache(&key).await;

    let stub = StubS3Client::new().with_default(upstream_default());
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, headers, _body) = proxy_head(server.addr, &format!("/{}", key), &[]).await;

    assert_eq!(status, StatusCode::OK);
    assert_served_from_cache(&stub, &headers, "HEAD");
    assert_eq!(
        etag_of(&headers).as_deref(),
        Some(EMITTED_QUOTED),
        "HEAD: a bare stored etag header must be replayed quoted"
    );
}

/// The negative, and the guard that the helper never alters a correct value: an entry
/// stored quoted, and one stored weak, are emitted BYTE-IDENTICAL on both the
/// full-object and range fallbacks. Passes with and without the helper by design.
#[tokio::test]
async fn already_quoted_and_weak_stored_etags_are_emitted_unchanged() {
    for (label, stored) in [("quoted", "\"abc-2\""), ("weak", "W/\"abc\"")] {
        for range in [None, Some("bytes=0-1023")] {
            let config = if range.is_some() {
                test_config(OBJECT_SIZE)
            } else {
                test_config_full_object_checks(OBJECT_SIZE)
            };
            let fixture = Fixture::new(config).await;
            let key = format!("{}/scn1-negative-{}.bin", BUCKET, label);
            fixture.seed(&key, &fresh(vec![(0, OBJECT_SIZE - 1)], stored));
            fixture.invalidate_metadata_cache(&key).await;

            let stub = StubS3Client::new().with_default(upstream_default());
            let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
            let extra: Vec<(&str, &str)> = range.map(|r| vec![("range", r)]).unwrap_or_default();
            let (_status, headers, _body) =
                proxy_get(server.addr, &format!("/{}", key), &extra).await;

            let site = format!("{label} stored etag, range={range:?}");
            assert_served_from_cache(&stub, &headers, &site);
            assert_eq!(
                etag_of(&headers).as_deref(),
                Some(stored),
                "{site}: an already-correct stored ETag must be emitted byte-identical"
            );
        }
    }
}
