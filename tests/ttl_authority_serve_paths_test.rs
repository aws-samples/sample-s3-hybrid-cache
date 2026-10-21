//! Every path that serves cached bytes honours the key's resolved `get_ttl`.
//!
//! `get_ttl: "0s"` is an authorization boundary: no cached byte or header is served for
//! such a key without S3 having answered the request. These tests pin it on the paths
//! that used to bypass the live verdict, and pin the stored expiry a fill commits so that
//! a path reading only stored expiry is right too.
//!
//! - fills commit stored expiry from the resolved rule, not the global TTL;
//! - the `?partNumber=N` lookup applies the live verdict, and its conditionals reach S3;
//! - a coalesced waiter's stale-if-error fallback is off for a zero `get_ttl` key and
//!   still on for a non-zero one;
//! - page widening never widens a zero `get_ttl` key, and its RAM arm is bounded by
//!   `ram_cache_verification_interval`;
//! - an expired part is revalidated, not re-transferred;
//! - a Mode B `If-Match` serve is bounded by the live `get_ttl`;
//! - a HEAD carrying `Range` is bounded by `head_ttl`;
//! - each of those guards has a test of its own that fails without it.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use hyper::{Method, StatusCode};

use common::expired_fixture::{
    body_of, proxy_get, proxy_head, short_ttl, test_config, test_config_full_object_checks,
    test_config_full_object_checks_with_get_ttl, test_config_full_object_checks_with_ttls,
    zero_ttl, Fixture, SeedSpec, OLD_FILL,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::origin_identity::OriginVerdict;

const OBJECT_SIZE: u64 = 4096;
const ZERO_PREFIX: &str = "bucket/zero";

/// `test_config`, with the rules treated as stale on every access, so each lookup tries
/// to re-read `cache_rules.json`. That does not make a rule written after the fixture is
/// built apply to the next lookup: a stale reload waits at most `RULES_RELOAD_WAIT_BOUND`
/// (100 ms) and a lookup that joins a read already in flight does not wait. A test that
/// writes a rule mid-test must wait until the key resolves to it, as `install_zero_rule` does.
fn rules_config() -> Arc<Config> {
    let mut config = (*test_config(OBJECT_SIZE)).clone();
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    Arc::new(config)
}

/// Write a rule giving every key under `bucket/zero/` `get_ttl: "0s"`, then poll every
/// 20 ms for up to 5 s until `probe_key` resolves to it, so no test below can pass
/// because the rule never loaded and none races the reload. This copies the shape of
/// `wait_for_rules_reload` in `cache_match_patterns_behavior_test.rs`, which is private
/// to a file in a different test group; sharing it would need a new `tests/common` module.
async fn install_zero_rule(fixture: &Fixture, probe_key: &str) {
    let rules = format!(
        r#"{{ "rules": [ {{ "pattern": "{}/**", "get_ttl": "0s" }} ] }}"#,
        ZERO_PREFIX
    );
    std::fs::write(
        fixture
            .cache_manager
            .get_cache_dir()
            .join("cache_rules.json"),
        rules,
    )
    .expect("write cache_rules.json");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resolved = fixture.cache_manager.get_effective_get_ttl(probe_key).await;
        if resolved == Duration::ZERO {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rules reload did not apply within 5s: apparatus, not the behaviour under test \
             (get_effective_get_ttl for {probe_key} never reported 0s; last resolved: {resolved:?})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn object_headers(etag: &str, len: u64) -> HashMap<String, String> {
    let mut h = HashMap::new();
    h.insert("etag".to_string(), etag.to_string());
    h.insert(
        "last-modified".to_string(),
        "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
    );
    h.insert("content-length".to_string(), len.to_string());
    h.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    h
}

fn cache_metadata(etag: &str, len: u64) -> s3_proxy::cache_types::CacheMetadata {
    s3_proxy::cache_types::CacheMetadata {
        etag: etag.to_string(),
        last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
        content_length: len,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    }
}

/// Seed a live-fresh single-extent entry and register `part_number` over it.
async fn seed_part(fixture: &Fixture, cache_key: &str, etag: &str) -> String {
    let mut spec = SeedSpec::stored_fresh_live_expired(vec![(0, 1023)], OBJECT_SIZE, etag);
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(cache_key, &spec);
    let mut metadata = fixture.read_meta(cache_key).expect(".meta");
    metadata.object_metadata.parts_count = Some(4);
    metadata.object_metadata.part_ranges.insert(1, (0, 1023));
    std::fs::write(
        fixture.meta_path(cache_key),
        serde_json::to_string_pretty(&metadata).expect("serialize"),
    )
    .expect("write .meta");
    fixture.invalidate_metadata_cache(cache_key).await;
    etag
}

fn not_in_future(t: SystemTime) -> bool {
    t <= SystemTime::now()
}

/// The `object_ttl_secs` of every journal `Add` this instance wrote for `cache_key`. In
/// shared-storage mode a range fill reaches the `.meta` through the journal, and this
/// field is the stored expiry the consolidator gives a `.meta` it creates.
fn journal_add_ttls(fixture: &Fixture, cache_key: &str) -> Vec<Option<u64>> {
    let mut out = Vec::new();
    let dir = fixture
        .cache_manager
        .get_cache_dir()
        .join("metadata")
        .join("_journals");
    let mut stack = vec![dir];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                if v.get("cache_key").and_then(|k| k.as_str()) == Some(cache_key)
                    && v.get("operation").and_then(|o| o.as_str()) == Some("Add")
                {
                    out.push(v.get("object_ttl_secs").and_then(|t| t.as_u64()));
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Stored expiry from the resolved rule
// ---------------------------------------------------------------------------

/// A `?partNumber=` fill under a `"0s"` rule used to store the global `get_ttl` as its
/// expiry, so the part lookup's `FreshServe` and the waiter fallbacks saw a long-lived
/// entry for a key whose operator required S3 on every read.
#[tokio::test(flavor = "multi_thread")]
async fn part_fill_under_zero_rule_stores_no_future_expiry() {
    let fixture = Fixture::new(rules_config()).await;
    let key = format!("{}/part-fill.bin", ZERO_PREFIX);
    install_zero_rule(&fixture, &key).await;

    let mut headers = object_headers("\"pf-v1\"", 1024);
    headers.insert("x-amz-mp-parts-count".to_string(), "4".to_string());
    fixture
        .cache_manager
        .store_part_as_range(&key, 1, "bytes 0-1023/4096", &headers, &[OLD_FILL; 1024])
        .await
        .expect("store part");

    let ttls = journal_add_ttls(&fixture, &key);
    if let Some(meta) = fixture.read_meta(&key) {
        assert!(
            not_in_future(meta.expires_at),
            "a part fill under get_ttl \"0s\" stored expires_at {:?}, in the future (now \
             {:?}): it took the global get_ttl, not the resolved rule",
            meta.expires_at,
            SystemTime::now()
        );
    } else {
        assert!(
            !ttls.is_empty(),
            "the part fill wrote neither a .meta nor a journal Add"
        );
    }
    assert!(
        ttls.iter().all(|t| *t == Some(0)),
        "a part fill under get_ttl \"0s\" journalled object TTLs {:?}; the resolved rule is 0s",
        ttls
    );
}

/// The full-object GET fill wrote the global `put_ttl` as stored expiry.
#[tokio::test(flavor = "multi_thread")]
async fn full_object_fill_under_zero_rule_stores_no_future_expiry() {
    let fixture = Fixture::new(rules_config()).await;
    let key = format!("{}/full-fill.bin", ZERO_PREFIX);
    install_zero_rule(&fixture, &key).await;

    fixture
        .cache_manager
        .store_response_with_headers(
            &key,
            &vec![OLD_FILL; OBJECT_SIZE as usize],
            object_headers("\"ff-v1\"", OBJECT_SIZE),
            cache_metadata("\"ff-v1\"", OBJECT_SIZE),
        )
        .await
        .expect("store full object");

    let meta = fixture
        .read_meta(&key)
        .expect("full-object fill must publish a .meta");
    assert!(
        not_in_future(meta.expires_at),
        "a full-object fill under get_ttl \"0s\" stored expires_at {:?}, in the future \
         (now {:?}): it took the global put_ttl, not the resolved rule",
        meta.expires_at,
        SystemTime::now()
    );
}

/// The range path's recovery refetch (cached `.bin` unreadable) stored the global
/// `get_ttl`. The request resolves a 60s `get_ttl`; the global value is a day or more, so
/// the stored expiry after the refetch says which one the fill used.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_fill_stores_the_resolved_ttl_not_the_global_one() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "bucket/recovery-fill.bin";
    let mut spec = SeedSpec::stored_fresh_live_expired(vec![(0, 1023)], OBJECT_SIZE, "\"rf-v1\"");
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(key, &spec);
    assert!(
        fixture.config.cache.get_ttl > Duration::from_secs(600),
        "PRECONDITION: the global get_ttl must differ from the resolved 60s"
    );
    std::fs::remove_file(fixture.bin_path(key, 0, 1023)).expect("remove .bin");
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![OLD_FILL; 1024]))
            .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE))
            .with_header("etag", etag.clone())
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
    );
    let response = fixture
        .range_get(
            key,
            "bytes=0-1023",
            HashMap::new(),
            &short_ttl(),
            Some(etag.clone()),
            stub.clone().into_trait_object(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let _ = body_of(response).await;
    assert!(
        !stub.captured().is_empty(),
        "PRECONDITION: the recovery refetch must reach S3"
    );

    // The refetched range is stored by a spawned task, through the journal.
    let mut ttls = Vec::new();
    for _ in 0..100 {
        ttls = journal_add_ttls(&fixture, key);
        if !ttls.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !ttls.is_empty(),
        "PRECONDITION: the recovery fill must journal its range"
    );
    assert!(
        ttls.iter().all(|t| *t == Some(60)),
        "the recovery fill journalled object TTLs {:?}: it used the global get_ttl ({:?}) \
         rather than the request's resolved 60s",
        ttls,
        fixture.config.cache.get_ttl
    );
}

// ---------------------------------------------------------------------------
// The part lookup
// ---------------------------------------------------------------------------

/// Under a `"0s"` rule the part lookup reports a miss, so every part GET reaches S3; a key
/// with a generous TTL still serves from the cache.
#[tokio::test(flavor = "multi_thread")]
async fn zero_rule_part_lookup_is_a_miss_and_a_generous_key_still_hits() {
    let fixture = Fixture::new(rules_config()).await;
    let zero_key = format!("{}/part-lookup.bin", ZERO_PREFIX);
    let warm_key = "bucket/warm/part-lookup.bin";
    install_zero_rule(&fixture, &zero_key).await;
    seed_part(&fixture, &zero_key, "\"pl-v1\"").await;
    seed_part(&fixture, warm_key, "\"pl-v1\"").await;

    let zero = fixture
        .cache_manager
        .lookup_part(&zero_key, 1, OriginVerdict::Matches)
        .await
        .expect("lookup_part");
    assert!(
        zero.is_none(),
        "a get_ttl \"0s\" key's cached part was served by lookup_part with no S3 request"
    );
    let warm = fixture
        .cache_manager
        .lookup_part(warm_key, 1, OriginVerdict::Matches)
        .await
        .expect("lookup_part");
    assert!(
        warm.is_some(),
        "CONTROL: a live-fresh part under a generous get_ttl must still be served"
    );
}

/// A part GET carrying a conditional the cache must not answer gets S3's answer: `412`
/// for an `If-Match` naming another ETag, `304` for an `If-None-Match` naming the current
/// one. Both used to be answered with the cached `206`.
#[tokio::test(flavor = "multi_thread")]
async fn part_get_conditionals_get_the_answer_s3_gives() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "test-bucket/part-conditional.bin";
    let etag = seed_part(&fixture, key, "\"pc-v1\"").await;

    let stub = StubS3Client::new()
        .with_response_for_if_match(
            "\"someone-else\"",
            StubResponse::with_status(StatusCode::PRECONDITION_FAILED),
        )
        .with_default(
            StubResponse::with_status(StatusCode::NOT_MODIFIED).with_header("etag", etag.clone()),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let path = format!("/{}?partNumber=1", key);

    let (status, _h, body) =
        proxy_get(server.addr, &path, &[("if-match", "\"someone-else\"")]).await;
    assert_eq!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "a part GET with If-Match naming another ETag got {} ({} body bytes); S3 answers 412",
        status,
        body.len()
    );

    let (status, _h, body) =
        proxy_get(server.addr, &path, &[("if-none-match", etag.as_str())]).await;
    assert_eq!(
        status,
        StatusCode::NOT_MODIFIED,
        "a part GET with If-None-Match naming the current ETag got {} ({} body bytes); S3 \
         answers 304",
        status,
        body.len()
    );
    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        2,
        "both conditionals must reach S3: {:#?}",
        captured
    );
}

/// An expired part under a non-zero `get_ttl` is revalidated with one conditional request,
/// not re-transferred: a `304` serves the cached part and starts a new `get_ttl` window, so
/// the next part GET needs no S3 request at all.
#[tokio::test(flavor = "multi_thread")]
async fn expired_part_revalidates_and_a_304_starts_a_new_window() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::from_secs(60));
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/part-revalidate.bin";
    let mut spec = SeedSpec::stored_fresh_live_expired(vec![(0, 1023)], OBJECT_SIZE, "\"prv-v1\"");
    spec.created_age = Duration::from_secs(7200);
    let etag = fixture.seed(key, &spec);
    let mut metadata = fixture.read_meta(key).expect(".meta");
    metadata.object_metadata.parts_count = Some(4);
    metadata.object_metadata.part_ranges.insert(1, (0, 1023));
    std::fs::write(
        fixture.meta_path(key),
        serde_json::to_string_pretty(&metadata).expect("serialize"),
    )
    .expect("write .meta");
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), common::expired_fixture::not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![b'B'; 1024]))
                .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE))
                .with_header("etag", "\"prv-v2\""),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let path = format!("/{}?partNumber=1", key);

    let (status, _h, body) = proxy_get(server.addr, &path, &[]).await;
    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        1,
        "an expired part must cost exactly one S3 request: {:#?}",
        captured
    );
    assert_eq!(
        captured[0].if_none_match(),
        Some(etag.as_str()),
        "the expired part's request must be a conditional carrying the cached ETag, not a \
         re-transfer: {:#?}",
        captured
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(
        body.len() == 1024 && body.iter().all(|b| *b == OLD_FILL),
        "after a 304 the cached part is served"
    );

    let (status, _h, _b) = proxy_get(server.addr, &path, &[]).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        stub.captured().len(),
        1,
        "the 304 must start a new get_ttl window: the next part GET reached S3 again"
    );
}

// ---------------------------------------------------------------------------
// Coalesced stale-if-error fallbacks
// ---------------------------------------------------------------------------

fn s3_503() -> StubResponse {
    StubResponse::with_status(StatusCode::SERVICE_UNAVAILABLE)
        .with_body(Bytes::from_static(b"<Error><Code>SlowDown</Code></Error>"))
}

async fn full_waiter(
    fixture: &Fixture,
    key: &str,
    resolved: &ResolvedSettings,
    stub: &StubS3Client,
) -> (StatusCode, Vec<u8>) {
    let response = HttpProxy::serve_from_cache_validated(
        Method::GET,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        HashMap::new(),
        key.to_string(),
        Arc::clone(&fixture.cache_manager),
        Arc::clone(&fixture.range_handler),
        stub.clone().into_trait_object(),
        Arc::clone(&fixture.config),
        None,
        resolved,
        &None,
        None,
    )
    .await
    .expect("infallible");
    let status = response.status();
    (status, body_of(response).await)
}

/// A full-object, range and part waiter for a `"0s"` key gets S3's `5xx`, never the cached
/// bytes; for a non-zero key the full-object waiter still serves the cached copy.
#[tokio::test(flavor = "multi_thread")]
async fn zero_ttl_waiters_get_the_5xx_and_non_zero_keep_stale_if_error() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "bucket/fallback.bin";
    // Stored-fresh, as a fill under the old global TTL left it.
    let mut spec =
        SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"fb-v1\"");
    spec.created_age = Duration::from_secs(1);
    fixture.seed(key, &spec);
    let mut metadata = fixture.read_meta(key).expect(".meta");
    metadata.object_metadata.part_ranges.insert(1, (0, 1023));
    std::fs::write(
        fixture.meta_path(key),
        serde_json::to_string_pretty(&metadata).expect("serialize"),
    )
    .expect("write .meta");
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new().with_default(s3_503());

    let (status, body) = full_waiter(&fixture, key, &zero_ttl(), &stub).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "full-object waiter for a \"0s\" key: S3 answered 503 and the waiter got {} with {} \
         cached bytes",
        status,
        body.len()
    );

    let range = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        HashMap::new(),
        key.to_string(),
        s3_proxy::range_handler::RangeSpec {
            start: 0,
            end: 1023,
        },
        Arc::clone(&fixture.cache_manager),
        Arc::clone(&fixture.range_handler),
        stub.clone().into_trait_object(),
        Arc::clone(&fixture.config),
        false,
        None,
        &zero_ttl(),
        &None,
        None,
    )
    .await
    .expect("infallible");
    assert_eq!(
        range.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "range waiter for a \"0s\" key must get S3's 503, not cached bytes"
    );

    // The part waiter resolves the key's TTL itself (`lookup_part`), so the zero TTL comes
    // from config here rather than from the `ResolvedSettings` argument.
    let zero_cfg = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let zero_fixture = Fixture::new(zero_cfg).await;
    seed_part(&zero_fixture, key, "\"fb-v1\"").await;
    let part = HttpProxy::serve_cached_part_validated(
        Method::GET,
        format!("/{}?partNumber=1", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        HashMap::new(),
        key.to_string(),
        1,
        Arc::clone(&zero_fixture.cache_manager),
        Arc::clone(&zero_fixture.range_handler),
        stub.clone().into_trait_object(),
        Arc::clone(&zero_fixture.config),
        None,
        &zero_ttl(),
        &None,
        None,
    )
    .await
    .expect("infallible");
    assert_eq!(
        part.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "part waiter for a \"0s\" key must get S3's 503, not the cached part"
    );

    // Control: a non-zero key keeps the availability exception.
    let (status, body) = full_waiter(&fixture, key, &short_ttl(), &stub).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "CONTROL: a non-zero get_ttl waiter must still fall back to the cached copy on a 5xx"
    );
    assert_eq!(body.len(), OBJECT_SIZE as usize);
}

/// A non-zero key's waiter falls back to the cached copy only while the entry is within the
/// resolved `get_ttl` on the live verdict. An entry cached 2h ago with stored expiry an hour
/// out (as an older, longer `get_ttl` leaves it) gets S3's `5xx` under a 60s `get_ttl`.
#[tokio::test(flavor = "multi_thread")]
async fn non_zero_waiter_does_not_fall_back_to_a_live_expired_entry() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "bucket/fallback-live-expired.bin";
    fixture.seed(
        key,
        &SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"fle-v1\""),
    );
    fixture.invalidate_metadata_cache(key).await;
    let stub = StubS3Client::new().with_default(s3_503());
    let (status, body) = full_waiter(&fixture, key, &short_ttl(), &stub).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a waiter served {} with {} bytes from an entry 2h past a 60s get_ttl after S3's 503",
        status,
        body.len()
    );
}

/// The zero-length arm of the full-object fallback had no check at all.
#[tokio::test(flavor = "multi_thread")]
async fn zero_ttl_waiter_for_a_zero_length_object_gets_the_5xx() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "bucket/fallback-empty.bin";
    fixture
        .cache_manager
        .store_response_with_headers(
            key,
            &[],
            object_headers("\"empty-v1\"", 0),
            cache_metadata("\"empty-v1\"", 0),
        )
        .await
        .expect("store empty object");
    fixture.invalidate_metadata_cache(key).await;
    assert!(fixture.read_meta(key).is_some(), "PRECONDITION: .meta");

    let stub = StubS3Client::new().with_default(s3_503());
    let (status, _) = full_waiter(&fixture, key, &zero_ttl(), &stub).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a \"0s\" waiter for a zero-length object was answered from cache after S3's 503"
    );
}

// ---------------------------------------------------------------------------
// Page widening, Mode B, HEAD with Range
// ---------------------------------------------------------------------------

fn widened(get_ttl: Duration) -> ResolvedSettings {
    ResolvedSettings {
        page_widening: true,
        page_size: 1024,
        get_ttl,
        ..ResolvedSettings::default()
    }
}

/// A zero `get_ttl` key is never widened: S3 receives the client's own range. Before,
/// with no `.meta` (a cold key, or a fill still only in this instance's journal) the
/// widened path ran and could serve a page.
#[tokio::test(flavor = "multi_thread")]
async fn zero_ttl_key_is_never_widened() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "bucket/widen-zero.bin";
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![OLD_FILL; 10]))
            .with_header("content-range", format!("bytes 5-14/{}", OBJECT_SIZE))
            .with_header("etag", "\"wz-v1\""),
    );
    let response = fixture
        .range_get(
            key,
            "bytes=5-14",
            HashMap::new(),
            &widened(Duration::ZERO),
            None,
            stub.clone().into_trait_object(),
        )
        .await;
    let _ = body_of(response).await;
    let captured = stub.captured();
    assert!(!captured.is_empty(), "PRECONDITION: the read must reach S3");
    let ranges: Vec<Option<String>> = captured
        .iter()
        .map(|r| {
            r.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("range"))
                .map(|(_, v)| v.clone())
        })
        .collect();
    assert!(
        ranges.iter().all(|r| r.as_deref() != Some("bytes=0-1023")),
        "a get_ttl \"0s\" key was widened to its page: S3 received ranges {:?}",
        ranges
    );
    assert!(
        ranges.iter().any(|r| r.as_deref() == Some("bytes=5-14")),
        "PRECONDITION: S3 must receive the client's own range; it received {:?}",
        ranges
    );
}

/// The page-widening RAM arm is bounded by `ram_cache_verification_interval`. A page in
/// RAM whose object a peer overwrote is not served once the interval has passed, even
/// with no `current_etag` known to the caller.
#[tokio::test(flavor = "multi_thread")]
async fn ram_page_is_not_served_past_the_verification_interval_after_an_overwrite() {
    let mut config = (*test_config(OBJECT_SIZE)).clone();
    config.cache.ram_cache_enabled = true;
    let fixture = Fixture::new(Arc::new(config)).await;
    let key = "bucket/widen-ram-overwritten.bin";
    let stale_page = vec![b'Z'; 1024];
    assert!(
        fixture.cache_manager.promote_range_to_ram_cache_frame(
            key,
            (0, 1023),
            stale_page.clone(),
            s3_proxy::compression::CompressionAlgorithm::None,
            "\"ram-v1\"".to_string(),
            String::new(),
            1u64 << 40,
        ),
        "PRECONDITION: the page must be in RAM"
    );
    // A peer overwrote the object: the shared `.meta` now names another version.
    let mut spec = SeedSpec::stored_fresh_live_expired(vec![(0, 1023)], OBJECT_SIZE, "\"ram-v2\"");
    spec.created_age = Duration::from_secs(1);
    fixture.seed(key, &spec);
    fixture.invalidate_metadata_cache(key).await;

    tokio::time::sleep(
        fixture.cache_manager.ram_cache_verification_interval() + Duration::from_millis(200),
    )
    .await;

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![OLD_FILL; 1024]))
            .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE))
            .with_header("etag", "\"ram-v2\""),
    );
    let response = fixture
        .range_get(
            key,
            "bytes=5-14",
            HashMap::new(),
            &widened(Duration::from_secs(3600)),
            None,
            stub.clone().into_trait_object(),
        )
        .await;
    let body = body_of(response).await;
    assert!(
        !body.iter().all(|b| *b == b'Z'),
        "the overwritten RAM page was served {:?} past the verification interval",
        fixture.cache_manager.ram_cache_verification_interval()
    );
}

/// The same bound after a peer DELETED the object, so no `.meta` remains. The widened path's
/// header check (`serve_widened_pages`, which refuses bytes whose ETag differs from a non-empty
/// cached ETag) catches the overwrite above even without the verification interval, so that
/// test alone does not show the interval is enforced. With no `.meta` that check has nothing
/// to compare, and the interval is the only bound.
#[tokio::test(flavor = "multi_thread")]
async fn ram_page_is_not_served_past_the_verification_interval_after_a_delete() {
    let mut config = (*test_config(OBJECT_SIZE)).clone();
    config.cache.ram_cache_enabled = true;
    let fixture = Fixture::new(Arc::new(config)).await;
    let key = "bucket/widen-ram-deleted.bin";
    assert!(
        fixture.cache_manager.promote_range_to_ram_cache_frame(
            key,
            (0, 1023),
            vec![b'Z'; 1024],
            s3_proxy::compression::CompressionAlgorithm::None,
            "\"ram-del-v1\"".to_string(),
            String::new(),
            1u64 << 40,
        ),
        "PRECONDITION: the page must be in RAM"
    );
    assert!(
        fixture.read_meta(key).is_none(),
        "PRECONDITION: no .meta (a peer deleted the object)"
    );

    tokio::time::sleep(
        fixture.cache_manager.ram_cache_verification_interval() + Duration::from_millis(200),
    )
    .await;

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![OLD_FILL; 1024]))
            .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE))
            .with_header("etag", "\"ram-del-v2\""),
    );
    let response = fixture
        .range_get(
            key,
            "bytes=5-14",
            HashMap::new(),
            &widened(Duration::from_secs(3600)),
            None,
            stub.clone().into_trait_object(),
        )
        .await;
    let body = body_of(response).await;
    assert!(
        !body.iter().all(|b| *b == b'Z'),
        "the RAM page of a deleted object was served {:?} past the verification interval",
        fixture.cache_manager.ram_cache_verification_interval()
    );
}

/// A Mode B `If-Match` GET on an entry older than the resolved `get_ttl` revalidates with
/// S3 even while stored expiry is in the future (an entry cached under a longer TTL), and
/// a `304` serves the cached bytes. Before, stored expiry alone decided, and every serve
/// moved it forward.
#[tokio::test(flavor = "multi_thread")]
async fn mode_b_if_match_on_a_live_expired_entry_revalidates() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::from_secs(60));
    assert!(
        config.cache.evaluate_conditions_from_cache,
        "PRECONDITION: Mode B"
    );
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/mode-b-live-expired.bin";
    // Cached 2h ago, stored expiry an hour out.
    let etag = fixture.seed(
        key,
        &SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"mbl-v1\""),
    );
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), common::expired_fixture::not_modified(&etag))
        .with_default(StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, _h, body) = proxy_get(
        server.addr,
        &format!("/{}", key),
        &[("if-match", etag.as_str())],
    )
    .await;
    let captured = stub.captured();
    assert!(
        captured
            .iter()
            .any(|r| r.if_none_match() == Some(etag.as_str())),
        "a Mode B If-Match GET on an entry 2h old under a 60s get_ttl was answered ({} with \
         {} bytes) with no revalidation: {:#?}",
        status,
        body.len(),
        captured
    );
    assert_eq!(status, StatusCode::OK, "a 304 serves the cached object");
    assert_eq!(body.len(), OBJECT_SIZE as usize);
}

/// A HEAD carrying `Range` under `head_ttl: "0s"` reaches S3, as a plain HEAD does.
#[tokio::test(flavor = "multi_thread")]
async fn head_with_range_is_bounded_by_head_ttl() {
    let config = test_config_full_object_checks_with_ttls(
        OBJECT_SIZE,
        Duration::from_secs(3600),
        Duration::ZERO,
    );
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/head-range.bin";
    let mut spec =
        SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"hr-v1\"");
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(key, &spec);
    fixture.invalidate_metadata_cache(key).await;

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_header("content-range", format!("bytes 0-99/{}", OBJECT_SIZE))
            .with_header("content-length", "100")
            .with_header("etag", etag.clone()),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, _h, _b) = proxy_head(
        server.addr,
        &format!("/{}", key),
        &[("range", "bytes=0-99")],
    )
    .await;
    let captured = stub.captured();
    assert!(
        captured.iter().any(|r| r.method == Method::HEAD),
        "a HEAD with Range under head_ttl \"0s\" was answered from cache ({}) with no HEAD \
         to S3: {:#?}",
        status,
        captured
    );
}

// ---------------------------------------------------------------------------
// Each guard on its own: a red side per guard, not per patch
// ---------------------------------------------------------------------------

/// The zero-length arm of the full-object fallback checks stored expiry on its own. The
/// entry was cached a moment ago, so it is live-fresh under the waiter's 60s `get_ttl` and
/// `waiter_may_serve_stale_on_error` lets the fallback run; its stored expiry has passed
/// (as a fill under a shorter rule leaves it). Only the zero-length arm's stored-expiry
/// check then stands between S3's `503` and a cached `200`.
#[tokio::test(flavor = "multi_thread")]
async fn zero_length_fallback_checks_stored_expiry_on_a_live_fresh_entry() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "bucket/fallback-empty-stored-expired.bin";
    fixture
        .cache_manager
        .store_response_with_headers(
            key,
            &[],
            object_headers("\"ese-v1\"", 0),
            cache_metadata("\"ese-v1\"", 0),
        )
        .await
        .expect("store empty object");
    let set_expiry = |expires_at: SystemTime| {
        let mut meta = fixture.read_meta(key).expect("PRECONDITION: .meta");
        meta.expires_at = expires_at;
        std::fs::write(
            fixture.meta_path(key),
            serde_json::to_string_pretty(&meta).expect("serialize"),
        )
        .expect("write .meta");
    };
    set_expiry(SystemTime::now() - Duration::from_secs(1));
    fixture.invalidate_metadata_cache(key).await;
    let meta = fixture.read_meta(key).expect("PRECONDITION: .meta");
    assert_eq!(meta.object_metadata.content_length, 0, "PRECONDITION");
    assert!(
        not_in_future(meta.expires_at),
        "PRECONDITION: stored expiry has passed"
    );
    let age = SystemTime::now()
        .duration_since(meta.freshness_anchor())
        .unwrap_or_default();
    assert!(
        age < Duration::from_secs(30),
        "PRECONDITION: live-fresh under the waiter's 60s get_ttl (age {:?})",
        age
    );

    let stub = StubS3Client::new().with_default(s3_503());
    let (status, _) = full_waiter(&fixture, key, &short_ttl(), &stub).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a zero-length entry past its stored expiry (live-fresh, age {:?} under 60s) was \
         answered {} from cache after S3's 503",
        age,
        status
    );

    // Control: the same entry within its stored expiry keeps stale-if-error.
    set_expiry(SystemTime::now() + Duration::from_secs(3600));
    fixture.invalidate_metadata_cache(key).await;
    let (status, _) = full_waiter(&fixture, key, &short_ttl(), &stub).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "CONTROL: a live-fresh zero-length entry within its stored expiry must still fall \
         back to the cached copy on a 5xx"
    );
}

/// The part waiter's own zero gate. `lookup_part` resolves the key's `get_ttl` from the
/// cache's rules; the waiter holds the request's `ResolvedSettings`. They disagree only
/// when a rule changes between the request's resolution and the waiter's lookup, which is
/// the state built here: the cache resolves a day, the request resolved `"0s"`. The part is
/// found, S3 answers `503`, and only `waiter_may_serve_stale_on_error(resolved, None)` keeps
/// the cached part from being served.
#[tokio::test(flavor = "multi_thread")]
async fn part_waiter_zero_gate_holds_when_lookup_part_finds_the_part() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "bucket/part-waiter-zero-gate.bin";
    let host = "s3.us-west-2.amazonaws.com";
    seed_part(&fixture, key, "\"pwz-v1\"").await;
    assert_eq!(
        fixture.cache_manager.origin_verdict(key, host).await,
        OriginVerdict::Matches,
        "PRECONDITION: the waiter's own origin verdict must permit the cache"
    );
    assert!(
        fixture
            .cache_manager
            .lookup_part(key, 1, OriginVerdict::Matches)
            .await
            .expect("lookup_part")
            .is_some(),
        "PRECONDITION: lookup_part must find the part, or the waiter forwards before its \
         zero gate"
    );

    let stub = StubS3Client::new().with_default(s3_503());
    let part = HttpProxy::serve_cached_part_validated(
        Method::GET,
        format!("/{}?partNumber=1", key).parse().unwrap(),
        host.to_string(),
        HashMap::new(),
        key.to_string(),
        1,
        Arc::clone(&fixture.cache_manager),
        Arc::clone(&fixture.range_handler),
        stub.clone().into_trait_object(),
        Arc::clone(&fixture.config),
        None,
        &zero_ttl(),
        &None,
        None,
    )
    .await
    .expect("infallible");
    let status = part.status();
    let body = body_of(part).await;
    assert_eq!(
        stub.captured().len(),
        1,
        "PRECONDITION: the waiter's revalidation must reach S3"
    );
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a part waiter whose request resolved get_ttl \"0s\" got {} with {} cached bytes \
         after S3's 503",
        status,
        body.len()
    );
}

/// A Mode B `If-Match` serve on a live-fresh entry moves neither stored expiry nor the live
/// anchor. In shared-storage mode a stored-expiry refresh is a buffered journal `TtlRefresh`
/// that reaches the `.meta` only once the buffer flushes and the consolidator folds it, so
/// the test flushes and consolidates the key before reading the `.meta` back.
#[tokio::test(flavor = "multi_thread")]
async fn mode_b_local_serve_moves_neither_stored_expiry_nor_the_live_anchor() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::from_secs(600));
    assert!(
        config.cache.evaluate_conditions_from_cache,
        "PRECONDITION: Mode B"
    );
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/mode-b-live-fresh.bin";
    // Cached 5s ago; stored expiry an hour out, so a refresh to now + 600s would move it.
    let mut spec =
        SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"mbf-v1\"");
    spec.created_age = Duration::from_secs(5);
    let etag = fixture.seed(key, &spec);
    fixture.invalidate_metadata_cache(key).await;
    let before = fixture.read_meta(key).expect(".meta");

    // The journal components are process-wide per `CacheManager`, so this is the buffer the
    // request path's disk cache manager records into.
    let buffer = fixture
        .cache_manager
        .create_configured_disk_cache_manager()
        .get_cache_hit_update_buffer()
        .expect("PRECONDITION: shared-storage mode buffers cache-hit updates");
    let consolidator = fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("PRECONDITION: shared-storage mode has a journal consolidator");

    let stub = StubS3Client::new()
        .with_default(StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, _h, body) = proxy_get(
        server.addr,
        &format!("/{}", key),
        &[("if-match", etag.as_str())],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "PRECONDITION: a Mode B serve");
    assert_eq!(body.len(), OBJECT_SIZE as usize);
    assert!(
        stub.captured().is_empty(),
        "PRECONDITION: a Mode B If-Match on a live-fresh entry is served with no S3 request: \
         {:#?}",
        stub.captured()
    );

    buffer
        .force_flush()
        .await
        .expect("flush the cache-hit buffer");
    consolidator
        .consolidate_object(key)
        .await
        .expect("consolidate the key");
    let after = fixture.read_meta(key).expect(".meta after the serve");
    assert_eq!(
        after.expires_at, before.expires_at,
        "a Mode B local serve moved stored expiry from {:?} to {:?}",
        before.expires_at, after.expires_at
    );
    assert_eq!(
        (after.created_at, after.last_validated_at),
        (before.created_at, before.last_validated_at),
        "a Mode B local serve moved the live anchor (created_at, last_validated_at)"
    );
}

/// A HEAD carrying `Range` under a non-zero `head_ttl`, on a GET-warmed entry (no HEAD
/// record): confirmed longer ago than `head_ttl`, it is forwarded to S3 uncached and S3's
/// ranged answer does not become the HEAD entry; confirmed within `head_ttl`, the same
/// request is answered from the cache.
#[tokio::test(flavor = "multi_thread")]
async fn head_with_range_past_a_non_zero_head_ttl_forwards_uncached_and_within_it_serves() {
    let head_ttl = Duration::from_secs(60);
    let config =
        test_config_full_object_checks_with_ttls(OBJECT_SIZE, Duration::from_secs(3600), head_ttl);
    let fixture = Fixture::new(config).await;
    let old_key = "test-bucket/head-range-old.bin";
    let new_key = "test-bucket/head-range-new.bin";
    // Within get_ttl either way; past head_ttl for the old key only.
    let mut old_spec =
        SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"hro-v1\"");
    old_spec.created_age = Duration::from_secs(600);
    let old_etag = fixture.seed(old_key, &old_spec);
    let mut new_spec =
        SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, "\"hrn-v1\"");
    new_spec.created_age = Duration::from_secs(1);
    fixture.seed(new_key, &new_spec);
    for k in [old_key, new_key] {
        fixture.invalidate_metadata_cache(k).await;
        assert!(
            fixture
                .read_meta(k)
                .expect(".meta")
                .head_expires_at
                .is_none(),
            "PRECONDITION: a GET-warmed entry has no HEAD record"
        );
    }

    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_header("content-range", format!("bytes 0-99/{}", OBJECT_SIZE))
            .with_header("content-length", "100")
            .with_header("etag", old_etag.clone())
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, _h, _b) = proxy_head(
        server.addr,
        &format!("/{}", old_key),
        &[("range", "bytes=0-99")],
    )
    .await;
    let captured = stub.captured();
    assert!(
        captured.iter().any(|r| r.method == Method::HEAD),
        "a HEAD with Range on an entry confirmed {:?} ago, past a {:?} head_ttl, with no HEAD \
         entry, was answered from cache ({}) with no HEAD to S3: {:#?}",
        old_spec.created_age,
        head_ttl,
        status,
        captured
    );
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "S3's answer is returned"
    );
    let meta = fixture.read_meta(old_key).expect(".meta");
    assert!(
        meta.head_expires_at.is_none() && meta.head_cached_at.is_none(),
        "the forwarded ranged HEAD became the HEAD entry: head_expires_at {:?}",
        meta.head_expires_at
    );
    assert!(
        fixture
            .cache_manager
            .get_head_cache_entry_unified(old_key, head_ttl)
            .await
            .expect("HEAD lookup")
            .is_none(),
        "the forwarded ranged HEAD's 206 headers became the key's HEAD entry"
    );

    let before = stub.captured().len();
    let (status, _h, _b) = proxy_head(
        server.addr,
        &format!("/{}", new_key),
        &[("range", "bytes=0-99")],
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "CONTROL: within head_ttl a HEAD with Range is answered from the cache"
    );
    assert_eq!(
        stub.captured().len(),
        before,
        "CONTROL: a HEAD with Range on an entry confirmed 1s ago, within a {:?} head_ttl, \
         reached S3: {:#?}",
        head_ttl,
        stub.captured()
    );
}

/// A range waiter under a consistent `"0s"`: the `Config` and the `ResolvedSettings` are
/// both zero. A `"0s"` fill stores an expiry that has already passed; S3's `304` is this
/// request's answer, so the waiter serves the cached range on it, with exactly one S3
/// request.
#[tokio::test(flavor = "multi_thread")]
async fn consistent_zero_range_waiter_serves_the_cached_range_on_s3s_304() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    let key = "bucket/zero-range-waiter.bin";
    fixture
        .cache_manager
        .store_response_with_headers(
            key,
            &vec![OLD_FILL; OBJECT_SIZE as usize],
            object_headers("\"zrw-v1\"", OBJECT_SIZE),
            cache_metadata("\"zrw-v1\"", OBJECT_SIZE),
        )
        .await
        .expect("store full object");
    fixture.invalidate_metadata_cache(key).await;
    let meta = fixture
        .read_meta(key)
        .expect("PRECONDITION: the fill published a .meta");
    assert!(
        not_in_future(meta.expires_at),
        "PRECONDITION: a \"0s\" fill stores an expiry already passed: {:?}",
        meta.expires_at
    );
    let etag = meta.object_metadata.etag.clone();

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), common::expired_fixture::not_modified(&etag))
        .with_default(StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR));
    let response = HttpProxy::serve_range_from_cache_validated(
        Method::GET,
        format!("/{}", key).parse().unwrap(),
        "s3.us-west-2.amazonaws.com".to_string(),
        HashMap::new(),
        key.to_string(),
        s3_proxy::range_handler::RangeSpec {
            start: 0,
            end: 1023,
        },
        Arc::clone(&fixture.cache_manager),
        Arc::clone(&fixture.range_handler),
        stub.clone().into_trait_object(),
        Arc::clone(&fixture.config),
        false,
        None,
        &zero_ttl(),
        &None,
        None,
    )
    .await
    .expect("infallible");
    let status = response.status();
    let body = body_of(response).await;
    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        1,
        "a \"0s\" range waiter must cost exactly one S3 request, its conditional; status {} \
         ({} bytes): {:#?}",
        status,
        body.len(),
        captured
    );
    assert_eq!(
        captured[0].if_none_match(),
        Some(etag.as_str()),
        "the waiter's one request is the conditional carrying the cached ETag"
    );
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "after S3's 304 the \"0s\" range waiter got {} ({} bytes)",
        status,
        body.len()
    );
    assert!(
        body.len() == 1024 && body.iter().all(|b| *b == OLD_FILL),
        "after S3's 304 the cached range is served"
    );
}

/// A write-cached entry with no `Last-Modified` never reaches the part revalidation, so that
/// path needs none of the waiter `304` arms' backfill: `lookup_part`'s write-cache guard
/// reports a miss for both the serve and the revalidation lookup, and the part GET is a
/// plain fetch whose answer carries S3's `Last-Modified`. Were it revalidated, S3's `304`
/// would serve the cached part with no `Last-Modified` at all.
#[tokio::test(flavor = "multi_thread")]
async fn write_cached_part_with_no_last_modified_is_fetched_not_revalidated() {
    let config = test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::from_secs(60));
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/part-write-cached-no-lm.bin";
    let mut spec =
        SeedSpec::write_cached_no_last_modified(vec![(0, 1023)], OBJECT_SIZE, "\"wcp-v1\"");
    // Past the 60s get_ttl, so the part lookup misses and the revalidation lookup is asked.
    spec.created_age = Duration::from_secs(7200);
    let etag = fixture.seed(key, &spec);
    let mut metadata = fixture.read_meta(key).expect(".meta");
    metadata.object_metadata.parts_count = Some(4);
    metadata.object_metadata.part_ranges.insert(1, (0, 1023));
    std::fs::write(
        fixture.meta_path(key),
        serde_json::to_string_pretty(&metadata).expect("serialize"),
    )
    .expect("write .meta");
    fixture.invalidate_metadata_cache(key).await;
    assert!(
        metadata.object_metadata.is_write_cached
            && metadata.object_metadata.effective_last_modified().is_none(),
        "PRECONDITION: write-cached with no effective Last-Modified"
    );

    let stub = StubS3Client::new()
        .with_response_for_etag(etag.clone(), common::expired_fixture::not_modified(&etag))
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![OLD_FILL; 1024]))
                .with_header("content-range", format!("bytes 0-1023/{}", OBJECT_SIZE))
                .with_header("etag", etag.clone())
                .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                .with_header("x-amz-mp-parts-count", "4"),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, headers, _b) =
        proxy_get(server.addr, &format!("/{}?partNumber=1", key), &[]).await;
    let captured = stub.captured();
    assert_eq!(captured.len(), 1, "one S3 request: {:#?}", captured);
    assert_eq!(
        captured[0].if_none_match(),
        None,
        "a write-cached part with no Last-Modified was revalidated (conditional request) \
         instead of fetched; answer {} with last-modified {:?}",
        status,
        headers.get("last-modified")
    );
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(
        headers.get("last-modified").is_some(),
        "the part GET's answer carries no Last-Modified"
    );
}
