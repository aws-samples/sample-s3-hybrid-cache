//! An expired entry whose `check_object_expiration` verdict carries no validator (no ETag, no
//! `Last-Modified`) is forwarded to S3 once, as a miss.
//!
//! The production shape: two readers of a key under repair, where one removes the `.meta`
//! after the other's range lookup found coverage. The second reader's expiration check then
//! finds no `.meta` and returns `Expired { etag: None, last_modified: None }`. Before the fix
//! the proxy sent that revalidation with no validator, S3 answered it with the body, the
//! verdict was `Undetermined`, and the unexpected-status arm discarded the body and forwarded
//! the request again: two upstream body fetches for one client request.
//!
//! The fixture reproduces it deterministically: seed an entry, load it into the per-instance
//! metadata cache (which the range lookup reads), then remove the `.meta` on disk (which the
//! expiration check reads). The assertion is the number of upstream requests and whether the
//! first carried a validator.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use hyper::{Method, StatusCode};

use common::expired_fixture::{
    body_of, proxy_get, short_ttl, test_config, test_config_full_object_checks_with_get_ttl,
    zero_ttl, Fixture, SeedSpec, NEW_FILL,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::{S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};

/// A `304` from an origin that sends no ETag.
fn not_modified_without_etag(last_modified: &str) -> StubResponse {
    StubResponse::with_status(StatusCode::NOT_MODIFIED).with_header("last-modified", last_modified)
}

const OBJECT_SIZE: u64 = 4096;

/// Seed `cache_key`, load its metadata into the per-instance metadata cache, then remove the
/// `.meta` on disk, so the range lookup still finds coverage and the expiration check does
/// not find a `.meta`.
async fn seed_then_remove_meta_behind_the_metadata_cache(
    fixture: &Fixture,
    cache_key: &str,
    extent: (u64, u64),
    etag: &str,
) {
    fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![extent], OBJECT_SIZE, etag),
    );
    fixture.assert_covers(cache_key, extent.0, extent.1);
    fixture.invalidate_metadata_cache(cache_key).await;
    let loaded = fixture
        .cache_manager
        .get_metadata_cached(cache_key)
        .await
        .expect("fixture: load the metadata cache");
    assert!(
        loaded.is_some(),
        "fixture: the seeded .meta must load into the metadata cache"
    );
    std::fs::remove_file(fixture.meta_path(cache_key)).expect("fixture: remove the .meta");
    let still_cached = fixture
        .cache_manager
        .get_metadata_cached(cache_key)
        .await
        .expect("fixture: read the metadata cache");
    assert!(
        still_cached.is_some(),
        "fixture: the metadata cache must still hold the entry after the .meta is removed, or \
         the request takes the plain-miss path and this test measures nothing"
    );
}

fn upstream_gets(stub: &StubS3Client) -> Vec<(bool, Option<String>)> {
    stub.captured()
        .iter()
        .filter(|r| r.method == Method::GET)
        .map(|r| {
            (
                r.if_none_match().is_some()
                    || r.headers.contains_key("if-modified-since")
                    || r.headers.contains_key("If-Modified-Since"),
                r.headers.get("range").cloned(),
            )
        })
        .collect()
}

/// The range path (`handle_range_request`), signed-range-free GET of a cached extent.
#[tokio::test]
async fn validator_less_expired_range_is_fetched_from_s3_once() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/scn1-validator-less-range.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;
    seed_then_remove_meta_behind_the_metadata_cache(
        &fixture,
        cache_key,
        (start, end),
        "\"scn1-v1\"",
    )
    .await;
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![NEW_FILL; len]))
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", start, end, OBJECT_SIZE),
            )
            .with_header("etag", "\"scn1-v1\""),
    );
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        fixture.range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            None,
            stub.clone().into_trait_object(),
        ),
    )
    .await
    .expect("range GET timed out");
    let status = response.status();
    let body = body_of(response).await;
    let gets = upstream_gets(&stub);
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "upstream GETs: {:?}",
        gets
    );
    assert_eq!(body, vec![NEW_FILL; len], "the client must get S3's bytes");
    assert_eq!(
        gets.len(),
        1,
        "SCN1: a validator-less expired range must be fetched from S3 once, as a miss; \
         upstream GETs (carried a validator, Range): {:?}",
        gets
    );
}

/// A cached entry with a `Last-Modified` but no ETag: the entry the guard must NOT treat as
/// validator-less, because `If-Modified-Since` can still be sent.
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

fn seed_etagless(fixture: &Fixture, cache_key: &str, extent: (u64, u64)) {
    fixture.seed(
        cache_key,
        &SeedSpec {
            last_modified: LAST_MODIFIED.to_string(),
            is_write_cached: Some(false),
            ..SeedSpec::expired(vec![extent], OBJECT_SIZE, "\"scn1-seeded\"")
        },
    );
    // `seed` insists on an ETag; drop it from the document afterwards.
    let mut seeded = fixture
        .read_meta(cache_key)
        .expect("fixture: the seeded .meta must parse");
    seeded.object_metadata.etag = String::new();
    std::fs::write(
        fixture.meta_path(cache_key),
        serde_json::to_string(&seeded).expect("fixture: serialize the .meta"),
    )
    .expect("fixture: rewrite the .meta without its ETag");
    fixture.assert_covers(cache_key, extent.0, extent.1);
    let meta = fixture
        .read_meta(cache_key)
        .expect("fixture: the seeded .meta must parse");
    assert!(
        meta.object_metadata.etag.is_empty()
            && meta.object_metadata.effective_last_modified().is_some(),
        "fixture: the entry carries a Last-Modified and no ETag"
    );
}

/// The range path: an ETag-less entry with a `Last-Modified` still revalidates conditionally.
#[tokio::test]
async fn etagless_expired_range_with_last_modified_revalidates_conditionally() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/scn1-etagless-range.bin";
    let (start, end) = (0u64, 1023u64);
    seed_etagless(&fixture, cache_key, (start, end));
    let stub = StubS3Client::new().with_default(not_modified_without_etag(LAST_MODIFIED));
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        fixture.range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &zero_ttl(),
            None,
            stub.clone().into_trait_object(),
        ),
    )
    .await
    .expect("range GET timed out");
    let gets = upstream_gets(&stub);
    assert!(
        gets.first().is_some_and(|(conditional, _)| *conditional),
        "an expired range with a Last-Modified and no ETag must be revalidated with \
         If-Modified-Since, not forwarded as a miss; upstream GETs (carried a validator, \
         Range): {:?}",
        gets
    );
}

/// The full-object path: the same entry still revalidates conditionally.
#[tokio::test]
async fn etagless_expired_full_object_with_last_modified_revalidates_conditionally() {
    let fixture = Fixture::new(test_config_full_object_checks_with_get_ttl(
        OBJECT_SIZE,
        Duration::ZERO,
    ))
    .await;
    let cache_key = "test-bucket/scn1-etagless-full.bin";
    seed_etagless(&fixture, cache_key, (0, OBJECT_SIZE - 1));
    let stub = StubS3Client::new().with_default(not_modified_without_etag(LAST_MODIFIED));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        proxy_get(server.addr, &format!("/{}", cache_key), &[]),
    )
    .await
    .expect("full-object GET timed out");
    let gets = upstream_gets(&stub);
    assert!(
        gets.first().is_some_and(|(conditional, _)| *conditional),
        "an expired full object with a Last-Modified and no ETag must be revalidated with \
         If-Modified-Since, not forwarded as a miss; upstream GETs (carried a validator, \
         Range): {:?}",
        gets
    );
}

/// What one upstream request from the stub below carried.
#[derive(Debug, Clone)]
struct UpstreamCall {
    /// `If-None-Match` or `If-Modified-Since`: the proxy revalidated rather than forwarding.
    conditional: bool,
    range: Option<String>,
    /// `S3RequestContext::allow_streaming`, the field `S3Client` branches on to decide whether
    /// the body may stream. The caching converter's streaming arm is reached only when it is set.
    allow_streaming: bool,
}

/// An `S3ClientApi` whose answers stream (`S3ResponseBody::Streaming`), which the shared
/// `StubS3Client` cannot: `Incoming` has no public constructor. The shape is copied from
/// `tests/get_miss_fill_same_instance_visibility_test.rs`, narrowed to one range: each
/// `forward_request` makes a real loopback HTTP/1 request to a tiny origin that answers `206`
/// with `content-range` and an ETag, and hands back that response's `Incoming`. The client
/// connection task lives until the body is drained, so the body keeps yielding
/// (`testing.md` → "A real `Incoming` needs its connection kept alive for the whole body").
/// Everything but `forward_request` delegates to a `StubS3Client`, so header and metadata
/// extraction match the buffered tests in this file exactly.
#[derive(Clone)]
struct StreamingRangeStub {
    origin: std::net::SocketAddr,
    calls: Arc<Mutex<Vec<UpstreamCall>>>,
    inner: StubS3Client,
}

/// The ETag S3 answers the miss with. Deliberately not the expired entry's, so "cached under
/// S3's ETag" is distinguishable from "kept the old document".
const S3_ETAG: &str = "\"scn2-s3-v2\"";

impl StreamingRangeStub {
    async fn start(data: Vec<u8>, start: u64, end: u64, total: u64) -> Self {
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind origin");
        let origin = listener.local_addr().expect("origin addr");
        let data = Bytes::from(data);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let data = data.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |_req: hyper::Request<hyper::body::Incoming>| {
                        let data = data.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(
                                hyper::Response::builder()
                                    .status(StatusCode::PARTIAL_CONTENT)
                                    .header("content-length", data.len().to_string())
                                    .header(
                                        "content-range",
                                        format!("bytes {}-{}/{}", start, end, total),
                                    )
                                    .header("etag", S3_ETAG)
                                    .header("last-modified", LAST_MODIFIED)
                                    .header("content-type", "application/octet-stream")
                                    .body(http_body_util::Full::new(data))
                                    .expect("origin response"),
                            )
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self {
            origin,
            calls: Arc::new(Mutex::new(Vec::new())),
            inner: StubS3Client::new(),
        }
    }

    fn calls(&self) -> Vec<UpstreamCall> {
        self.calls.lock().expect("calls").clone()
    }
}

#[async_trait::async_trait]
impl S3ClientApi for StreamingRangeStub {
    async fn forward_request(&self, context: S3RequestContext) -> s3_proxy::Result<S3Response> {
        let has = |name: &str| context.headers.keys().any(|k| k.eq_ignore_ascii_case(name));
        self.calls.lock().expect("calls").push(UpstreamCall {
            conditional: has("if-none-match") || has("if-modified-since"),
            range: context
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("range"))
                .map(|(_, v)| v.clone()),
            allow_streaming: context.allow_streaming,
        });
        let stream = tokio::net::TcpStream::connect(self.origin)
            .await
            .expect("connect origin");
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .expect("origin handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = hyper::Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "origin")
            .body(http_body_util::Empty::<Bytes>::new())
            .expect("origin request");
        let resp = sender.send_request(req).await.expect("origin response");
        let status = resp.status();
        let headers: HashMap<String, String> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        Ok(S3Response {
            status,
            headers,
            body: Some(S3ResponseBody::Streaming(resp.into_body())),
            request_duration: Duration::from_millis(0),
        })
    }
    fn extract_metadata_from_response(&self, headers: &HashMap<String, String>) -> CacheMetadata {
        self.inner.extract_metadata_from_response(headers)
    }
    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> ObjectMetadata {
        self.inner.extract_object_metadata_from_response(headers)
    }
    fn get_connection_pool(&self) -> Arc<tokio::sync::RwLock<ConnectionPoolManager>> {
        self.inner.get_connection_pool()
    }
    fn has_endpoint_overrides(&self) -> bool {
        false
    }
    async fn set_metrics_manager(
        &self,
        _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }
    async fn register_endpoint(&self, _endpoint: &str) {}
    async fn refresh_dns(&self) -> s3_proxy::Result<()> {
        Ok(())
    }
}

/// The convergence claim, which was established only in the
/// negative. A validator-less expired range miss whose upstream body **streams** must be
/// forwarded once, cached under S3's ETag, and a hit on the next read. The buffered stub the
/// other tests in this file use caches nothing on this path (the buffered arm of
/// `convert_s3_response_to_http_with_caching` serves the bytes and spawns no cache write), so
/// the claim needs a streaming body to be tested at all.
#[tokio::test]
async fn validator_less_expired_range_converges_when_the_upstream_body_streams() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/scn2-validator-less-streamed.bin";
    let (start, end) = (0u64, 1023u64);
    let len = (end - start + 1) as usize;
    seed_then_remove_meta_behind_the_metadata_cache(
        &fixture,
        cache_key,
        (start, end),
        "\"scn2-cached-v1\"",
    )
    .await;
    let stub = StreamingRangeStub::start(vec![NEW_FILL; len], start, end, OBJECT_SIZE).await;
    let client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(stub.clone());
    let first = tokio::time::timeout(
        Duration::from_secs(30),
        fixture.range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &short_ttl(),
            None,
            Arc::clone(&client),
        ),
    )
    .await
    .expect("the first range GET timed out");
    let first_status = first.status();
    let first_body = body_of(first).await;
    // Wait on the mechanism asserted: the fill's journal entry folded into a `.meta`. A
    // streamed fill commits `WriteMode::JournalOnly`, so nothing appears until a cycle runs.
    let consolidator = fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("fixture: the consolidator must be wired");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while fixture.read_meta(cache_key).is_none() && tokio::time::Instant::now() < deadline {
        let _ = consolidator.run_consolidation_cycle().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let cached_etag = fixture
        .read_meta(cache_key)
        .map(|m| m.object_metadata.etag.clone());
    let second = tokio::time::timeout(
        Duration::from_secs(30),
        fixture.range_get(
            cache_key,
            &format!("bytes={}-{}", start, end),
            HashMap::new(),
            &short_ttl(),
            None,
            Arc::clone(&client),
        ),
    )
    .await
    .expect("the second range GET timed out");
    let second_status = second.status();
    let second_etag = second
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let second_body = body_of(second).await;
    let calls = stub.calls();
    eprintln!(
        "SCN2 validator-less expired range over a streaming body: first {} ({} bytes), cached \
         ETag {:?}, second {} ({} bytes), upstream calls {:?}",
        first_status,
        first_body.len(),
        cached_etag,
        second_status,
        second_body.len(),
        calls
    );
    assert_eq!(
        first_status,
        StatusCode::PARTIAL_CONTENT,
        "upstream calls: {:?}",
        calls
    );
    assert_eq!(
        first_body,
        vec![NEW_FILL; len],
        "the client must get S3's bytes"
    );
    assert!(
        calls.first().is_some_and(|c| !c.conditional
            && c.allow_streaming
            && c.range.as_deref() == Some(format!("bytes={}-{}", start, end).as_str())),
        "fixture: the miss must be forwarded as the client's range, with no validator and with \
         streaming allowed, or this measures another path; upstream calls: {:?}",
        calls
    );
    assert_eq!(
        cached_etag.as_deref(),
        Some(S3_ETAG),
        "SCN2: a validator-less expired range miss over a streaming body must be cached under \
         S3's ETag; upstream calls: {:?}",
        calls
    );
    assert_eq!(
        (second_status, second_body),
        (StatusCode::PARTIAL_CONTENT, vec![NEW_FILL; len]),
        "the second read must serve S3's bytes; upstream calls: {:?}",
        calls
    );
    assert!(
        second_etag
            .as_deref()
            .is_some_and(|e| e.trim_matches('"') == S3_ETAG.trim_matches('"')),
        "SCN3: the second read's head must name the version its bytes are ({}), it named {:?}; \
         upstream calls: {:?}",
        S3_ETAG,
        second_etag,
        calls
    );
    assert_eq!(
        calls.len(),
        1,
        "SCN2: the next read of a validator-less expired range cached from a streaming body \
         must be a cache hit, so it sends nothing upstream; upstream calls: {:?}",
        calls
    );
}

/// The full-object path (`handle_get_head_request`), a plain GET through the loopback proxy.
#[tokio::test]
async fn validator_less_expired_full_object_is_fetched_from_s3_once() {
    let fixture = Fixture::new(test_config_full_object_checks_with_get_ttl(
        OBJECT_SIZE,
        Duration::ZERO,
    ))
    .await;
    let cache_key = "test-bucket/scn1-validator-less-full.bin";
    seed_then_remove_meta_behind_the_metadata_cache(
        &fixture,
        cache_key,
        (0, OBJECT_SIZE - 1),
        "\"scn1-full-v1\"",
    )
    .await;
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(Bytes::from(vec![NEW_FILL; OBJECT_SIZE as usize]))
            .with_header("etag", "\"scn1-full-v1\""),
    );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let (status, _headers, body) = tokio::time::timeout(
        Duration::from_secs(30),
        proxy_get(server.addr, &format!("/{}", cache_key), &[]),
    )
    .await
    .expect("full-object GET timed out");
    let gets = upstream_gets(&stub);
    assert_eq!(status, StatusCode::OK, "upstream GETs: {:?}", gets);
    assert_eq!(
        body,
        vec![NEW_FILL; OBJECT_SIZE as usize],
        "the client must get S3's bytes"
    );
    assert_eq!(
        gets.len(),
        1,
        "SCN1: a validator-less expired full object must be fetched from S3 once, as a miss; \
         upstream GETs (carried a validator, Range): {:?}",
        gets
    );
}
