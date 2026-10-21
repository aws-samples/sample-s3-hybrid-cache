//! A client that signed its request correctly gets S3's answer to that request,
//! never S3's refusal of a request the proxy changed.
//!
//! SigV4 signs the HTTP method and the canonical query string as well as the
//! signed headers. A presigned URL carries its whole signature in the query, so a
//! request the proxy sends for it without that query is anonymous, and S3 answers
//! `403`. A request sent with another method than the client's is a different
//! canonical request, and S3 answers `403 SignatureDoesNotMatch`.
//!
//! The stub below models exactly that: it answers `403` to any request whose
//! method or query string differs from the client's, and otherwise returns the
//! response it was given. A stub that answered whatever it received would pass
//! these tests on requests real S3 refuses.
//!
//! Every test drives the real `HttpProxy::handle_request` over a loopback
//! connection, so the request goes through the same dispatch a client's does.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode};

use common::expired_fixture::{
    assert_all_old, presigned_query_without_range, signed_authorization_without_range,
    test_config_full_object_checks, test_config_full_object_checks_with_ttls, Fixture, SeedSpec,
    OLD_FILL,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response};

const OBJECT_SIZE: u64 = 4096;
const ETAG: &str = "\"spurious-status-etag\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

/// One request the stub received.
#[derive(Debug, Clone)]
struct Seen {
    method: Method,
    query: String,
    range: Option<String>,
    status: StatusCode,
}

/// S3 as it treats a signed request: the method and the query string are part of
/// the signature, so a request whose method or query differs from the client's is
/// answered `403`. Anything else gets the routed response of `inner`.
struct SignatureCheckedS3 {
    inner: StubS3Client,
    client_method: Method,
    client_query: String,
    seen: Mutex<Vec<Seen>>,
}

impl SignatureCheckedS3 {
    fn new(inner: StubS3Client, client_method: Method, client_query: &str) -> Arc<Self> {
        Arc::new(Self {
            inner,
            client_method,
            client_query: client_query.to_string(),
            seen: Mutex::new(Vec::new()),
        })
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("seen poisoned").clone()
    }
}

#[async_trait]
impl S3ClientApi for SignatureCheckedS3 {
    async fn forward_request(&self, context: S3RequestContext) -> Result<S3Response> {
        let method = context.method.clone();
        let query = context.uri.query().unwrap_or("").to_string();
        let range = context
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("range"))
            .map(|(_, v)| v.clone());
        let response = if method != self.client_method || query != self.client_query {
            Ok(S3Response {
                status: StatusCode::FORBIDDEN,
                headers: HashMap::new(),
                body: Some(s3_proxy::S3ResponseBody::Buffered(Bytes::from_static(
                    b"<Error><Code>SignatureDoesNotMatch</Code></Error>",
                ))),
                request_duration: Duration::from_millis(0),
            })
        } else {
            self.inner.forward_request(context).await
        };
        let status = response
            .as_ref()
            .map(|r| r.status)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        self.seen.lock().expect("seen poisoned").push(Seen {
            method,
            query,
            range,
            status,
        });
        response
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

    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }
}

/// Send one request through the loopback proxy with exactly `headers` (no
/// `Authorization` is added, so a presigned request stays presigned).
async fn send(
    addr: SocketAddr,
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{}{}", addr, path_and_query));
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder
        .body(Full::new(Bytes::new()))
        .expect("build request");
    let resp = tokio::time::timeout(Duration::from_secs(20), client.request(req))
        .await
        .expect("the request must not hang")
        .expect("proxy request failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

/// A presigned GET with no `Range` over a cached object whose range file has gone
/// missing after its `.meta` listed it. The proxy refetches the extent; that
/// refetch, and the forward that answers the client if it fails, must carry the
/// presigned URL's query, which is the client's only signature.
#[tokio::test(flavor = "multi_thread")]
async fn a_presigned_get_whose_range_file_is_missing_is_refetched_with_its_signature() {
    let fixture = Fixture::new(test_config_full_object_checks(OBJECT_SIZE)).await;
    let key = "test-bucket/presigned-full-object-missing-bin.bin";
    let mut spec =
        SeedSpec::stored_fresh_live_expired(vec![(0, OBJECT_SIZE - 1)], OBJECT_SIZE, ETAG);
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(key, &spec);
    std::fs::remove_file(fixture.bin_path(key, 0, OBJECT_SIZE - 1)).expect("remove .bin");
    fixture.invalidate_metadata_cache(key).await;

    let query = presigned_query_without_range();
    let inner = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![OLD_FILL; OBJECT_SIZE as usize]))
            .with_header(
                "content-range",
                format!("bytes 0-{}/{}", OBJECT_SIZE - 1, OBJECT_SIZE),
            )
            .with_header("etag", etag.clone())
            .with_header("last-modified", LAST_MODIFIED),
    );
    let s3 = SignatureCheckedS3::new(inner, Method::GET, &query);
    let server = fixture.spawn_proxy(s3.clone()).await;

    let (status, _headers, body) = send(
        server.addr,
        Method::GET,
        &format!("/{}?{}", key, query),
        &[],
    )
    .await;
    let seen = s3.seen();
    let refetch = format!("bytes=0-{}", OBJECT_SIZE - 1);
    assert!(
        seen.iter()
            .any(|r| r.range.as_deref() == Some(refetch.as_str())),
        "PRECONDITION: the request never reached the missing-file recovery refetch (a {} the \
         client did not send); client got {}; upstream: {:?}",
        refetch,
        status,
        seen
    );
    assert!(
        seen.iter().all(|r| r.query == query),
        "A PRESIGNED REQUEST REACHED S3 WITHOUT ITS SIGNATURE: client got {}; upstream \
         (method, status, range, query): {:?}",
        status,
        seen.iter()
            .map(|r| (r.method.clone(), r.status, r.range.clone(), r.query.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(status, StatusCode::OK, "upstream: {:?}", seen);
    assert_all_old(&body, OBJECT_SIZE as usize, "presigned recovery refetch");
}

/// A SigV4 HEAD whose `Range` is not in its signed headers, over an object cached
/// only in part, within `head_ttl`. The client gets S3's answer to its own HEAD.
/// The proxy's hole fetch for it is a GET, which S3 refuses because the method is
/// signed; the request then falls back to the client's HEAD, so the refusal never
/// reaches the client.
#[tokio::test(flavor = "multi_thread")]
async fn a_head_with_an_unsigned_range_over_a_partly_cached_object_gets_s3s_answer_to_its_head() {
    let config = test_config_full_object_checks_with_ttls(
        OBJECT_SIZE,
        Duration::from_secs(3600),
        Duration::from_secs(60),
    );
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/head-unsigned-range-partial.bin";
    let mut spec = SeedSpec::stored_fresh_live_expired(vec![(0, 1023)], OBJECT_SIZE, ETAG);
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(key, &spec);
    fixture.invalidate_metadata_cache(key).await;

    let inner = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_header("content-range", format!("bytes 0-2047/{}", OBJECT_SIZE))
            .with_header("content-length", "2048")
            .with_header("etag", etag.clone())
            .with_header("last-modified", LAST_MODIFIED),
    );
    let s3 = SignatureCheckedS3::new(inner, Method::HEAD, "");
    let server = fixture.spawn_proxy(s3.clone()).await;

    let authorization = signed_authorization_without_range();
    let (status, headers, body) = send(
        server.addr,
        Method::HEAD,
        &format!("/{}", key),
        &[
            ("authorization", authorization.as_str()),
            ("range", "bytes=0-2047"),
        ],
    )
    .await;
    let seen = s3.seen();
    assert!(
        !seen.is_empty(),
        "PRECONDITION: a partly cached ranged HEAD must reach S3; client got {} with no \
         upstream request",
        status
    );
    let last = seen.last().expect("an upstream request");
    assert!(
        status == StatusCode::PARTIAL_CONTENT
            && last.method == Method::HEAD
            && last.status == StatusCode::PARTIAL_CONTENT,
        "A HEAD WAS NOT ANSWERED BY S3'S ANSWER TO A HEAD: client got {}; upstream (method, \
         status, range): {:?}",
        status,
        seen.iter()
            .map(|r| (r.method.clone(), r.status, r.range.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        headers.get("content-range").and_then(|v| v.to_str().ok()),
        Some(format!("bytes 0-2047/{}", OBJECT_SIZE).as_str())
    );
    assert!(
        body.is_empty(),
        "a HEAD response carried {} bytes",
        body.len()
    );
}

/// Control for the HEAD test: the same partly cached range as a GET still fetches
/// only the hole and merges it with the cached bytes.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_get_with_an_unsigned_range_over_a_partly_cached_object_fetches_the_hole() {
    let config = test_config_full_object_checks_with_ttls(
        OBJECT_SIZE,
        Duration::from_secs(3600),
        Duration::from_secs(60),
    );
    let fixture = Fixture::new(config).await;
    let key = "test-bucket/get-unsigned-range-partial.bin";
    let mut spec = SeedSpec::stored_fresh_live_expired(vec![(0, 1023)], OBJECT_SIZE, ETAG);
    spec.created_age = Duration::from_secs(1);
    let etag = fixture.seed(key, &spec);
    fixture.invalidate_metadata_cache(key).await;

    let inner = StubS3Client::new().with_response_for_range(
        "bytes=1024-2047",
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![OLD_FILL; 1024]))
            .with_header("content-range", format!("bytes 1024-2047/{}", OBJECT_SIZE))
            .with_header("etag", etag.clone())
            .with_header("last-modified", LAST_MODIFIED),
    );
    let s3 = SignatureCheckedS3::new(inner, Method::GET, "");
    let server = fixture.spawn_proxy(s3.clone()).await;

    let authorization = signed_authorization_without_range();
    let (status, _headers, body) = send(
        server.addr,
        Method::GET,
        &format!("/{}", key),
        &[
            ("authorization", authorization.as_str()),
            ("range", "bytes=0-2047"),
        ],
    )
    .await;
    let seen = s3.seen();
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "upstream: {:?}", seen);
    assert_eq!(
        seen.iter().map(|r| r.range.clone()).collect::<Vec<_>>(),
        vec![Some("bytes=1024-2047".to_string())],
        "CONTROL: a partly cached GET fetches only its hole: {:?}",
        seen
    );
    assert_all_old(&body, 2048, "partly cached GET");
}
