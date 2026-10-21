//! End-to-end regression for the multipart write-cache publication race and the
//! signed-`Range` rewrite it exposed in production (s3-proxy 2.8.0, 2026-09-03).
//!
//! # Production topology reproduced here
//!
//! * `write_cache_enabled: true`, `get_ttl: 0s`, `head_ttl: 0s`, RAM range cache off,
//!   metadata RAM cache on (all defaults except the write cache, matching the on-prem
//!   worker config in ksg-infra `scripts/s3-hybrid-cache/install.sh`).
//! * A multipart upload streamed through the proxy's `SignedPutHandler` and completed,
//!   so the object is published to the cache by `finalize_multipart_upload`.
//! * Immediately afterwards, two parallel Botocore-shaped downloads: a signed `HEAD`,
//!   then one signed ranged `GET` per part-aligned chunk, all in flight at once, with
//!   `range` in SigV4 `SignedHeaders`.
//! * A production-style journal consolidation loop running concurrently.
//! * In half of the iterations the final cached range is made inconsistent (its `.bin`
//!   truncated) after publication, which is the state the production cache reached
//!   once a recovery fetch had stored an S3 error body as range data.
//!
//! # What the mock S3 checks
//!
//! The mock verifies every SigV4 signature it receives against the request it actually
//! got, exactly as S3 does. Any header the proxy rewrites that is listed in
//! `SignedHeaders` therefore produces a real `403 SignatureDoesNotMatch` with an
//! S3-shaped XML body, and the test counts those. That is the invariant under test:
//! the proxy must never forward a request whose signed headers differ from the
//! client's.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::NewCacheMetadata;
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use s3_proxy::S3ClientApi;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE"; // gitleaks:allow
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"; // gitleaks:allow
const REGION: &str = "us-east-1";
const BUCKET: &str = "ksg-metaflow-e2e";

/// Part size for the multipart upload. Botocore downloads in part-aligned chunks of
/// the same size, exactly as `download_file` did against the production object.
const PART_SIZE: usize = 32 * 1024;
const FULL_PARTS: usize = 6;
const TAIL_SIZE: usize = 12 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// SigV4 primitives shared by the client signer and the mock S3 verifier
// ─────────────────────────────────────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&key, data).as_ref().to_vec()
}

fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        let keep = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~');
        if keep || (byte == b'/' && !encode_slash) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&input[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn canonical_query(raw_query: Option<&str>) -> String {
    let mut pairs: Vec<(String, String)> = raw_query
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(p), String::new()),
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect::<Vec<_>>()
        .join("&")
}

fn amz_date_now() -> (String, String) {
    let now = chrono::Utc::now();
    (
        now.format("%Y%m%dT%H%M%SZ").to_string(),
        now.format("%Y%m%d").to_string(),
    )
}

fn canonical_request(
    method: &str,
    path: &str,
    raw_query: Option<&str>,
    header_values: &dyn Fn(&str) -> Option<String>,
    signed_headers: &[String],
    payload_hash: &str,
) -> Option<String> {
    let mut canonical_headers = String::new();
    for name in signed_headers {
        let value = header_values(name)?;
        let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(collapsed.trim());
        canonical_headers.push('\n');
    }
    Some(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        uri_encode(path, false),
        canonical_query(raw_query),
        canonical_headers,
        signed_headers.join(";"),
        payload_hash
    ))
}

fn signing_key(secret: &str, date: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{}", secret).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, REGION.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

fn string_to_sign(amz_date: &str, scope: &str, creq: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        sha256_hex(creq.as_bytes())
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Botocore-shaped client
// ─────────────────────────────────────────────────────────────────────────────

/// A signed request ready to send: origin-form path+query and the full header set,
/// including `authorization`. Headers listed in `signed` are covered by the signature.
struct SignedRequest {
    method: Method,
    path_and_query: String,
    headers: Vec<(String, String)>,
    body: Bytes,
}

/// Sign the way botocore does for S3 over a plaintext endpoint: every header we send is
/// in `SignedHeaders` (host, range, x-amz-content-sha256, x-amz-date), and
/// `x-amz-content-sha256` carries the real payload hash.
fn sign_request(
    method: Method,
    host_header: &str,
    path: &str,
    query: &[(&str, &str)],
    range: Option<&str>,
    body: Bytes,
) -> SignedRequest {
    let (amz_date, date) = amz_date_now();
    let payload_hash = sha256_hex(&body);

    let mut headers: Vec<(String, String)> = vec![
        ("host".to_string(), host_header.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(r) = range {
        headers.push(("range".to_string(), r.to_string()));
    }
    let mut signed: Vec<String> = headers.iter().map(|(k, _)| k.clone()).collect();
    signed.sort();

    let raw_query = if query.is_empty() {
        None
    } else {
        Some(
            query
                .iter()
                .map(|(k, v)| {
                    if v.is_empty() {
                        uri_encode(k, true)
                    } else {
                        format!("{}={}", uri_encode(k, true), uri_encode(v, true))
                    }
                })
                .collect::<Vec<_>>()
                .join("&"),
        )
    };

    let lookup = |name: &str| -> Option<String> {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let creq = canonical_request(
        method.as_str(),
        path,
        raw_query.as_deref(),
        &lookup,
        &signed,
        &payload_hash,
    )
    .expect("all signed headers present");
    let scope = format!("{}/{}/s3/aws4_request", date, REGION);
    let sts = string_to_sign(&amz_date, &scope, &creq);
    let sig = hex::encode(hmac_sha256(&signing_key(SECRET_KEY, &date), sts.as_bytes()));
    headers.push((
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            ACCESS_KEY,
            scope,
            signed.join(";"),
            sig
        ),
    ));

    let path_and_query = match raw_query {
        Some(q) => format!("{}?{}", path, q),
        None => path.to_string(),
    };
    SignedRequest {
        method,
        path_and_query,
        headers,
        body,
    }
}

struct ClientResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

async fn send(
    client: &Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    proxy: SocketAddr,
    req: SignedRequest,
) -> ClientResponse {
    let mut builder = Request::builder()
        .method(req.method)
        .uri(format!("http://{}{}", proxy, req.path_and_query));
    for (k, v) in &req.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let request = builder
        .body(Full::new(req.body))
        .expect("build client request");
    let response = client
        .request(request)
        .await
        .expect("proxy request transport failure");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect proxy response body")
        .to_bytes();
    ClientResponse {
        status,
        headers,
        body,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Mock S3 with real SigV4 verification
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct StoredObject {
    data: Bytes,
    etag: String,
    last_modified: String,
}

#[derive(Debug, Clone)]
struct UpstreamRequest {
    method: String,
    path: String,
    range: Option<String>,
    if_none_match: Option<String>,
    status: u16,
    /// A 403 the test asked the mock to return (see `Fault::TruncateFinalRangeThenUpstreamError`),
    /// as opposed to a real signature mismatch on the request the proxy sent.
    injected: bool,
}

#[derive(Default)]
struct MockState {
    objects: HashMap<String, StoredObject>,
    uploads: HashMap<String, BTreeMap<u32, (Bytes, String)>>,
    log: Vec<UpstreamRequest>,
    upload_counter: usize,
}

#[derive(Clone)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
    signature_failures: Arc<AtomicUsize>,
    /// When set, the next correctly signed, unconditional GET for exactly this Range is
    /// answered with an S3-shaped `403 SignatureDoesNotMatch` document instead of the
    /// bytes. This is the upstream error body that the pre-fix recovery path returned
    /// to the client and stored as range data.
    inject_error_for_range_once: Arc<Mutex<Option<String>>>,
}

impl MockS3 {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MockState::default())),
            signature_failures: Arc::new(AtomicUsize::new(0)),
            inject_error_for_range_once: Arc::new(Mutex::new(None)),
        }
    }

    fn inject_upstream_error_once(&self, range: &str) {
        *self.inject_error_for_range_once.lock().unwrap() = Some(range.to_string());
    }

    fn signature_mismatch_body(detail: &str) -> Response<Full<Bytes>> {
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message><AWSAccessKeyId>{}</AWSAccessKeyId>{}<RequestId>E2E0000000000001</RequestId><HostId>e2e-mock-s3</HostId></Error>",
            ACCESS_KEY, detail
        );
        Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("content-type", "application/xml")
            .header("x-amz-request-id", "E2E0000000000001")
            .body(Full::new(Bytes::from(xml)))
            .unwrap()
    }

    fn log(&self) -> Vec<UpstreamRequest> {
        self.state.lock().unwrap().log.clone()
    }

    fn signature_failures(&self) -> usize {
        self.signature_failures.load(Ordering::SeqCst)
    }

    /// Verify the SigV4 signature against the request as received. Returns the
    /// canonical request and string-to-sign on failure so the 403 body can carry
    /// them, as S3's does.
    fn verify(
        method: &Method,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<(), String> {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| "missing authorization".to_string())?;
        let rest = auth
            .strip_prefix("AWS4-HMAC-SHA256 ")
            .ok_or_else(|| "not sigv4".to_string())?;
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;
        for part in rest.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("Credential=") {
                credential = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
                signed_headers = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("Signature=") {
                signature = Some(v.to_string());
            }
        }
        let credential = credential.ok_or("missing Credential")?;
        let signed_headers = signed_headers.ok_or("missing SignedHeaders")?;
        let signature = signature.ok_or("missing Signature")?;
        let mut cred_parts = credential.split('/');
        let _access_key = cred_parts.next().ok_or("bad credential")?;
        let date = cred_parts.next().ok_or("bad credential date")?;
        let scope = format!("{}/{}/s3/aws4_request", date, REGION);

        let payload_header = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
            .to_string();
        if payload_header != "UNSIGNED-PAYLOAD" && payload_header != sha256_hex(body) {
            return Err("payload hash mismatch".to_string());
        }

        let signed: Vec<String> = signed_headers.split(';').map(str::to_string).collect();
        let amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or("missing x-amz-date")?
            .to_string();
        let lookup = |name: &str| -> Option<String> {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let creq = canonical_request(
            method.as_str(),
            path,
            query,
            &lookup,
            &signed,
            &payload_header,
        )
        .ok_or("a signed header is missing from the request")?;
        let sts = string_to_sign(&amz_date, &scope, &creq);
        let expected = hex::encode(hmac_sha256(&signing_key(SECRET_KEY, date), sts.as_bytes()));
        if expected == signature {
            Ok(())
        } else {
            Err(format!(
                "<CanonicalRequest>{}</CanonicalRequest><CanonicalRequestBytes>{}</CanonicalRequestBytes><StringToSign>{}</StringToSign><StringToSignBytes>{}</StringToSignBytes><SignatureProvided>{}</SignatureProvided>",
                creq,
                hex_spaced(creq.as_bytes()),
                sts,
                hex_spaced(sts.as_bytes()),
                signature
            ))
        }
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let body = req.into_body().collect().await?.to_bytes();
        let path = uri.path().to_string();
        let query = uri.query().map(str::to_string);
        let range = headers
            .get("range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let if_none_match = headers
            .get("if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);

        let mut injected = false;
        let response = match Self::verify(&method, &path, query.as_deref(), &headers, &body) {
            Err(detail) => {
                self.signature_failures.fetch_add(1, Ordering::SeqCst);
                Self::signature_mismatch_body(&detail)
            }
            Ok(()) => {
                let inject = {
                    let mut slot = self.inject_error_for_range_once.lock().unwrap();
                    // Only an unconditional fetch (a repair or a plain miss) is refused;
                    // a conditional revalidation still answers 304 so the cached entry
                    // stays the thing under test.
                    if method == Method::GET
                        && if_none_match.is_none()
                        && slot.as_deref() == range.as_deref()
                    {
                        slot.take()
                    } else {
                        None
                    }
                };
                match inject {
                    Some(_) => {
                        injected = true;
                        Self::signature_mismatch_body(
                            "<CanonicalRequest>injected by the test</CanonicalRequest>",
                        )
                    }
                    None => self.dispatch(&method, &path, query.as_deref(), &headers, body),
                }
            }
        };

        self.state.lock().unwrap().log.push(UpstreamRequest {
            method: method.to_string(),
            path,
            range,
            if_none_match,
            status: response.status().as_u16(),
            injected,
        });
        Ok(response)
    }

    fn dispatch(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Response<Full<Bytes>> {
        let params: HashMap<String, String> = query
            .unwrap_or("")
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| match p.split_once('=') {
                Some((k, v)) => (percent_decode(k), percent_decode(v)),
                None => (percent_decode(p), String::new()),
            })
            .collect();
        let mut state = self.state.lock().unwrap();

        if *method == Method::POST && params.contains_key("uploads") {
            state.upload_counter += 1;
            let upload_id = format!("e2eupload{:04}", state.upload_counter);
            state.uploads.insert(upload_id.clone(), BTreeMap::new());
            let (bucket, key) = split_bucket_key(path);
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                bucket, key, upload_id
            );
            return xml_response(StatusCode::OK, xml);
        }

        if *method == Method::PUT && params.contains_key("partNumber") {
            let upload_id = params.get("uploadId").cloned().unwrap_or_default();
            let part_number: u32 = params
                .get("partNumber")
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            let etag = format!("\"{}\"", &sha256_hex(&body)[..32]);
            match state.uploads.get_mut(&upload_id) {
                Some(parts) => {
                    parts.insert(part_number, (body, etag.clone()));
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header("etag", etag)
                        .header("x-amz-request-id", "E2EPART")
                        .body(Full::new(Bytes::new()))
                        .unwrap();
                }
                None => return xml_error(StatusCode::NOT_FOUND, "NoSuchUpload"),
            }
        }

        if *method == Method::POST && params.contains_key("uploadId") {
            let upload_id = params.get("uploadId").cloned().unwrap_or_default();
            let Some(parts) = state.uploads.remove(&upload_id) else {
                return xml_error(StatusCode::NOT_FOUND, "NoSuchUpload");
            };
            let mut data = Vec::new();
            let mut etag_concat = String::new();
            for (bytes, etag) in parts.values() {
                data.extend_from_slice(bytes);
                etag_concat.push_str(etag.trim_matches('"'));
            }
            let etag = format!(
                "\"{}-{}\"",
                &sha256_hex(etag_concat.as_bytes())[..32],
                parts.len()
            );
            let last_modified = httpdate::fmt_http_date(SystemTime::now());
            state.objects.insert(
                path.to_string(),
                StoredObject {
                    data: Bytes::from(data),
                    etag: etag.clone(),
                    last_modified,
                },
            );
            let (bucket, key) = split_bucket_key(path);
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Location>http://{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                headers
                    .get("host")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("mock"),
                key,
                bucket,
                key,
                etag
            );
            let mut response = xml_response(StatusCode::OK, xml);
            response.headers_mut().insert(
                "x-amz-version-id",
                "pmX99TUTvBHpRRWPOzmPZmlaU19UzTa7".parse().unwrap(),
            );
            return response;
        }

        if *method == Method::GET || *method == Method::HEAD {
            let Some(object) = state.objects.get(path).cloned() else {
                return xml_error(StatusCode::NOT_FOUND, "NoSuchKey");
            };
            let total = object.data.len() as u64;
            let base = |status: StatusCode| {
                Response::builder()
                    .status(status)
                    .header("etag", object.etag.clone())
                    .header("last-modified", object.last_modified.clone())
                    .header("accept-ranges", "bytes")
                    .header("content-type", "application/octet-stream")
                    .header("x-amz-request-id", "E2EGET")
            };
            if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
                if inm
                    .split(',')
                    .any(|e| e.trim() == object.etag || e.trim() == "*")
                {
                    return base(StatusCode::NOT_MODIFIED)
                        .body(Full::new(Bytes::new()))
                        .unwrap();
                }
            }
            if *method == Method::HEAD {
                return base(StatusCode::OK)
                    .header("content-length", total.to_string())
                    .body(Full::new(Bytes::new()))
                    .unwrap();
            }
            if let Some(range) = headers.get("range").and_then(|v| v.to_str().ok()) {
                let spec = range.trim_start_matches("bytes=");
                let (start, end) = match spec.split_once('-') {
                    Some(("", suffix)) => {
                        let n: u64 = suffix.parse().unwrap_or(0);
                        (total.saturating_sub(n), total - 1)
                    }
                    Some((s, "")) => (s.parse().unwrap_or(0), total - 1),
                    Some((s, e)) => (
                        s.parse().unwrap_or(0),
                        e.parse::<u64>().unwrap_or(total - 1).min(total - 1),
                    ),
                    None => (0, total - 1),
                };
                if start >= total || start > end {
                    return xml_error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange");
                }
                let slice = object.data.slice(start as usize..=end as usize);
                return base(StatusCode::PARTIAL_CONTENT)
                    .header("content-length", slice.len().to_string())
                    .header(
                        "content-range",
                        format!("bytes {}-{}/{}", start, end, total),
                    )
                    .body(Full::new(slice))
                    .unwrap();
            }
            return base(StatusCode::OK)
                .header("content-length", total.to_string())
                .body(Full::new(object.data))
                .unwrap();
        }

        xml_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented")
    }

    async fn spawn(self) -> (SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock s3");
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, _)) = accept else { break };
                        let mock = self.clone();
                        tokio::spawn(async move {
                            let service = service_fn(move |req| mock.clone().handle(req));
                            let _ = http1::Builder::new()
                                .keep_alive(true)
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });
        (addr, shutdown_tx)
    }
}

fn hex_spaced(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

fn split_bucket_key(path: &str) -> (String, String) {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((b, k)) => (b.to_string(), k.to_string()),
        None => (trimmed.to_string(), String::new()),
    }
}

fn xml_response(status: StatusCode, xml: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .header("x-amz-request-id", "E2EXML")
        .body(Full::new(Bytes::from(xml)))
        .unwrap()
}

fn xml_error(status: StatusCode, code: &str) -> Response<Full<Bytes>> {
    xml_response(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><RequestId>E2EERR</RequestId></Error>",
            code, code
        ),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Proxy under test: production wiring, loopback listener, consolidation loop
// ─────────────────────────────────────────────────────────────────────────────

struct ProxyUnderTest {
    _temp_dir: TempDir,
    cache_dir: std::path::PathBuf,
    addr: SocketAddr,
    cache_manager: Arc<CacheManager>,
    _shutdown_tx: oneshot::Sender<()>,
}

async fn spawn_proxy(mock_addr: SocketAddr) -> ProxyUnderTest {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let temp_dir = TempDir::new().expect("tempdir");
    let cache_dir = temp_dir.path().to_path_buf();

    let mut config = Config::default();
    config.cache.cache_dir = cache_dir.clone();
    config.cache.write_cache_enabled = true;
    config.cache.get_ttl = Duration::ZERO;
    config.cache.head_ttl = Duration::ZERO;
    config.cache.ram_cache_enabled = false;
    config.cache.actively_remove_cached_data = false;
    config.connection_pool.upstream_overrides.insert(
        format!("127.0.0.1:{}", mock_addr.port()),
        UpstreamOverrideConfig {
            scheme: UpstreamScheme::Http,
            validate_tls: true,
        },
    );
    let config = Arc::new(config);

    let cache_manager_inner = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        config.compression.threshold,
        config.compression.enabled,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        config.cache.ram_cache_flush_interval,
        config.cache.ram_cache_shard_count,
        config.connection_pool.upstream_first_byte_timeout,
    );
    let cache_manager = Arc::new(cache_manager_inner);

    let inflight_ledger = Arc::new(s3_proxy::inflight_ledger::InflightLedger::new(
        config.server.max_inflight_buffer_bytes,
    ));
    let s3_client: Arc<dyn S3ClientApi + Send + Sync> = Arc::new(
        S3Client::new(&config.connection_pool, None)
            .expect("real S3 client")
            .with_inflight_ledger(Arc::clone(&inflight_ledger)),
    );

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");
    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));

    // Production runs the consolidator every `consolidation_interval` (5s default) from
    // main.rs; a tighter cadence here widens the chance of interleaving with readers.
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(150));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let _ = consolidator.run_consolidation_cycle().await;
            }
        });
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    {
        let config = Arc::clone(&config);
        let cache_manager = Arc::clone(&cache_manager);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let Ok((stream, peer)) = accept else { break };
                        let config = Arc::clone(&config);
                        let cache_manager = Arc::clone(&cache_manager);
                        let range_handler = Arc::clone(&range_handler);
                        let s3_client = Arc::clone(&s3_client);
                        let inflight_tracker = Arc::clone(&inflight_tracker);
                        let request_semaphore = Arc::clone(&request_semaphore);
                        tokio::spawn(async move {
                            let service = service_fn(move |req: Request<Incoming>| {
                                let config = Arc::clone(&config);
                                let cache_manager = Arc::clone(&cache_manager);
                                let range_handler = Arc::clone(&range_handler);
                                let s3_client = Arc::clone(&s3_client);
                                let inflight_tracker = Arc::clone(&inflight_tracker);
                                let request_semaphore = Arc::clone(&request_semaphore);
                                let inflight_ledger = s3_client.get_inflight_ledger();
                                async move {
                                    HttpProxy::handle_request(
                                        req,
                                        peer,
                                        config,
                                        cache_manager,
                                        s3_client,
                                        range_handler,
                                        request_semaphore,
                                        None,
                                        None,
                                        inflight_tracker,
                                        None,
                                        None,
                                        None,
                                        inflight_ledger,
                                    )
                                    .await
                                }
                            });
                            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });
    }

    ProxyUnderTest {
        _temp_dir: temp_dir,
        cache_dir,
        addr,
        cache_manager,
        _shutdown_tx: shutdown_tx,
    }
}

impl ProxyUnderTest {
    fn read_meta(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        let path = self.cache_manager.get_new_metadata_file_path(cache_key);
        let content = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&content).ok()
    }

    async fn wait_for_published_meta(
        &self,
        cache_key: &str,
        parts: usize,
    ) -> Option<NewCacheMetadata> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(meta) = self.read_meta(cache_key) {
                if meta.ranges.len() == parts && !meta.object_metadata.etag.is_empty() {
                    return Some(meta);
                }
            }
            if tokio::time::Instant::now() > deadline {
                return self.read_meta(cache_key);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario
// ─────────────────────────────────────────────────────────────────────────────

fn pseudo_random_bytes(len: usize, seed: u64) -> Bytes {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

fn extract_xml(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

/// Damage applied to the final cached range after publication and before the reads.
///
/// `TruncateFinalRange` leaves a `.bin` shorter than its recorded extent (the state
/// production reached once an error body had been stored as range data);
/// `DeleteFinalRange` removes the `.bin` a concurrent invalidation would have deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    TruncateFinalRange,
    DeleteFinalRange,
    /// The production shape: the final range's bytes are inconsistent with the `.meta`
    /// (present but short) AND the upstream answers the repair fetch with an error
    /// document. Before the fix the proxy returned that document as range data, stored
    /// it clamped, and every later reader tripped over it.
    TruncateFinalRangeThenUpstreamError,
}

#[derive(Debug)]
struct IterationReport {
    iteration: usize,
    fault: Fault,
    client_failures: Vec<String>,
    signature_failures: usize,
    rewritten_ranges: Vec<String>,
    metadata_problems: Vec<String>,
    /// Upstream range GETs that returned a body during the downloads. A healthy
    /// write-through cache serves every byte itself, so this must be empty unless a
    /// fault was injected, and then it may only name the damaged range.
    upstream_body_fetches: Vec<String>,
    revalidations: usize,
}

impl IterationReport {
    fn new(iteration: usize, fault: Fault) -> Self {
        Self {
            iteration,
            fault,
            client_failures: Vec::new(),
            signature_failures: 0,
            rewritten_ranges: Vec::new(),
            metadata_problems: Vec::new(),
            upstream_body_fetches: Vec::new(),
            revalidations: 0,
        }
    }

    fn violations(&self) -> Vec<String> {
        let mut out = Vec::new();
        out.extend(self.client_failures.iter().cloned());
        if self.signature_failures > 0 {
            out.push(format!(
                "{} upstream SignatureDoesNotMatch response(s)",
                self.signature_failures
            ));
        }
        out.extend(
            self.rewritten_ranges
                .iter()
                .map(|r| format!("rewritten Range header reached S3: {}", r)),
        );
        out.extend(self.metadata_problems.iter().cloned());
        if self.fault == Fault::None && !self.upstream_body_fetches.is_empty() {
            out.push(format!(
                "cache did not serve the published object; {} upstream body fetch(es): {:?}",
                self.upstream_body_fetches.len(),
                self.upstream_body_fetches
            ));
        }
        if self.revalidations == 0 {
            out.push("no zero-TTL revalidation reached S3".to_string());
        }
        out
    }

    fn is_clean(&self) -> bool {
        self.violations().is_empty()
    }

    fn summary(&self) -> String {
        format!(
            "iteration {} (fault {:?}): {} revalidation(s), {} upstream body fetch(es), violations: {:#?}",
            self.iteration,
            self.fault,
            self.revalidations,
            self.upstream_body_fetches.len(),
            self.violations()
        )
    }
}

async fn run_iteration(
    proxy: &ProxyUnderTest,
    mock: &MockS3,
    mock_addr: SocketAddr,
    iteration: usize,
    fault: Fault,
) -> IterationReport {
    let mut report = IterationReport::new(iteration, fault);
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let host_header = format!("127.0.0.1:{}", mock_addr.port());
    let key = format!("metaflow/features/{:02}/artifact-{}", iteration, iteration);
    let path = format!("/{}/{}", BUCKET, key);
    let cache_key = format!("{}/{}", BUCKET, key);
    let total = FULL_PARTS * PART_SIZE + TAIL_SIZE;
    let data = pseudo_random_bytes(total, 0x5eed_0000 + iteration as u64);

    // 1. CreateMultipartUpload through the proxy.
    let create = send(
        &client,
        proxy.addr,
        sign_request(
            Method::POST,
            &host_header,
            &path,
            &[("uploads", "")],
            None,
            Bytes::new(),
        ),
    )
    .await;
    assert_eq!(
        create.status,
        StatusCode::OK,
        "create multipart: {:?}",
        create.body
    );
    let upload_id = extract_xml(&String::from_utf8_lossy(&create.body), "UploadId")
        .expect("UploadId in CreateMultipartUpload response");

    // 2. Upload every part through the write cache.
    let part_count = FULL_PARTS + 1;
    let mut part_etags = Vec::with_capacity(part_count);
    for part_number in 1..=part_count {
        let start = (part_number - 1) * PART_SIZE;
        let end = (start + PART_SIZE).min(total);
        let part = data.slice(start..end);
        let response = send(
            &client,
            proxy.addr,
            sign_request(
                Method::PUT,
                &host_header,
                &path,
                &[
                    ("partNumber", &part_number.to_string()),
                    ("uploadId", &upload_id),
                ],
                None,
                part,
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "upload part {}: {:?}",
            part_number,
            response.body
        );
        let etag = response
            .headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .expect("part ETag")
            .to_string();
        part_etags.push((part_number as u32, etag));
    }

    // 3. CompleteMultipartUpload.
    let mut complete_xml = String::from("<CompleteMultipartUpload>");
    for (n, etag) in &part_etags {
        complete_xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            n, etag
        ));
    }
    complete_xml.push_str("</CompleteMultipartUpload>");
    let complete = send(
        &client,
        proxy.addr,
        sign_request(
            Method::POST,
            &host_header,
            &path,
            &[("uploadId", &upload_id)],
            None,
            Bytes::from(complete_xml),
        ),
    )
    .await;
    assert_eq!(
        complete.status,
        StatusCode::OK,
        "complete: {:?}",
        complete.body
    );
    let object_etag = extract_xml(&String::from_utf8_lossy(&complete.body), "ETag")
        .expect("ETag in CompleteMultipartUpload response");

    // The proxy finalises the cache entry inline with the Complete response. Wait for
    // the published `.meta`, as production had (completion 20:55:52, first reads ~1 min
    // later), then optionally corrupt the final range on disk.
    let damaged_range = format!("bytes={}-{}", FULL_PARTS * PART_SIZE, total - 1);
    let published = proxy.wait_for_published_meta(&cache_key, part_count).await;
    match &published {
        Some(meta) => {
            if meta.object_metadata.etag != object_etag {
                report.metadata_problems.push(format!(
                    "published .meta etag {:?} != S3 etag {:?}",
                    meta.object_metadata.etag, object_etag
                ));
            }
            // Every part range (the streamed `UploadPart` sink, linked into the object by
            // `finalize_multipart_upload`) records the identity (length, modification time) of
            // the `.bin` it names, and that file holds the bytes the client uploaded.
            if fault == Fault::None {
                for r in &meta.ranges {
                    let bin = proxy.cache_dir.join("ranges").join(&r.file_path);
                    let raw = std::fs::read(&bin).unwrap_or_default();
                    let mut decoded = Vec::new();
                    let _ = std::io::Read::read_to_end(
                        &mut lz4_flex::frame::FrameDecoder::new(&raw[..]),
                        &mut decoded,
                    );
                    let found = std::fs::metadata(&bin)
                        .ok()
                        .and_then(|m| s3_proxy::cache_types::RangeFileIdentity::of(&m));
                    let recorded = r.file_identity;
                    let matches_upload =
                        decoded.as_slice() == &data[r.start as usize..=r.end as usize];
                    if !matches_upload || found.is_none() || recorded != found {
                        report.metadata_problems.push(format!(
                            "RECORDED IDENTITY WRONG for part range {}-{}: listing records {:?}, \
                             file is {:?} ({} bytes, matches upload: {})",
                            r.start,
                            r.end,
                            recorded,
                            found,
                            decoded.len(),
                            matches_upload
                        ));
                    }
                }
            }
            if fault != Fault::None {
                let last = meta
                    .ranges
                    .iter()
                    .max_by_key(|r| r.start)
                    .expect("at least one range");
                let bin = proxy.cache_dir.join("ranges").join(&last.file_path);
                match fault {
                    Fault::TruncateFinalRange => {
                        let len = std::fs::metadata(&bin).map(|m| m.len()).unwrap_or(0);
                        let keep = (len / 4).max(64).min(len.saturating_sub(1));
                        let file = std::fs::OpenOptions::new()
                            .write(true)
                            .open(&bin)
                            .expect("open final range file");
                        file.set_len(keep).expect("truncate final range file");
                    }
                    Fault::DeleteFinalRange => {
                        std::fs::remove_file(&bin).expect("delete final range file");
                    }
                    Fault::TruncateFinalRangeThenUpstreamError => {
                        let len = std::fs::metadata(&bin).map(|m| m.len()).unwrap_or(0);
                        let keep = (len / 4).max(64).min(len.saturating_sub(1));
                        let file = std::fs::OpenOptions::new()
                            .write(true)
                            .open(&bin)
                            .expect("open final range file");
                        file.set_len(keep).expect("truncate final range file");
                        mock.inject_upstream_error_once(&damaged_range);
                    }
                    Fault::None => unreachable!(),
                }
            }
        }
        None => report
            .metadata_problems
            .push("multipart completion never published a .meta".to_string()),
    }

    // 4. Two parallel Botocore-shaped downloads, started immediately: HEAD, then every
    //    part-aligned chunk as its own signed ranged GET, all concurrent.
    let log_start = mock.log().len();
    let mut client_ranges: Vec<String> = Vec::new();
    let mut tasks = Vec::new();
    let proxy_addr = proxy.addr;
    let chunk_count = total.div_ceil(PART_SIZE);
    for downloader in 0..2 {
        let client = client.clone();
        let host_header = host_header.clone();
        let path = path.clone();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            let mut failures = Vec::new();
            let head = send(
                &client,
                proxy_addr,
                sign_request(Method::HEAD, &host_header, &path, &[], None, Bytes::new()),
            )
            .await;
            if head.status != StatusCode::OK {
                failures.push(format!("downloader {} HEAD -> {}", downloader, head.status));
            }
            let mut gets = Vec::new();
            for chunk in 0..chunk_count {
                let start = chunk * PART_SIZE;
                let end = ((chunk + 1) * PART_SIZE).min(total) - 1;
                let range = format!("bytes={}-{}", start, end);
                let client = client.clone();
                let host_header = host_header.clone();
                let path = path.clone();
                let expected = data.slice(start..=end);
                gets.push(async move {
                    let response = send(
                        &client,
                        proxy_addr,
                        sign_request(
                            Method::GET,
                            &host_header,
                            &path,
                            &[],
                            Some(&range),
                            Bytes::new(),
                        ),
                    )
                    .await;
                    if response.status != StatusCode::PARTIAL_CONTENT {
                        return Err(format!(
                            "downloader {} {} -> {} ({} bytes: {:?})",
                            downloader,
                            range,
                            response.status,
                            response.body.len(),
                            String::from_utf8_lossy(&response.body[..response.body.len().min(160)])
                        ));
                    }
                    if response.body != expected {
                        return Err(format!(
                            "downloader {} {} -> corrupt body ({} bytes, expected {})",
                            downloader,
                            range,
                            response.body.len(),
                            expected.len()
                        ));
                    }
                    Ok(())
                });
            }
            for result in futures::future::join_all(gets).await {
                if let Err(e) = result {
                    failures.push(e);
                }
            }
            failures
        }));
    }
    for chunk in 0..chunk_count {
        let start = chunk * PART_SIZE;
        let end = ((chunk + 1) * PART_SIZE).min(total) - 1;
        client_ranges.push(format!("bytes={}-{}", start, end));
    }
    for task in tasks {
        report
            .client_failures
            .extend(task.await.expect("downloader task panicked"));
    }

    // 5. Let the background consolidator settle, then inspect the upstream trace and
    //    the final cache state.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let trace = &mock.log()[log_start..];
    report.signature_failures = trace
        .iter()
        .filter(|r| r.status == 403 && !r.injected)
        .count();
    if fault == Fault::TruncateFinalRangeThenUpstreamError {
        // S3 itself refused exactly one repair fetch; the client that owned that
        // request sees the 403, as it would without a cache. Everything else must
        // still be correct, and the cache must not have kept the error document.
        let surfaced: Vec<String> = report
            .client_failures
            .iter()
            .filter(|f| f.contains(&damaged_range) && f.contains("-> 403"))
            .cloned()
            .collect();
        if surfaced.len() > 1 {
            report.metadata_problems.push(format!(
                "the single injected upstream error surfaced to {} client requests",
                surfaced.len()
            ));
        }
        report
            .client_failures
            .retain(|f| !(f.contains(&damaged_range) && f.contains("-> 403")));
    }
    report.revalidations = trace
        .iter()
        .filter(|r| r.if_none_match.is_some() && r.status == 304)
        .count();
    for request in trace {
        if let Some(range) = &request.range {
            if !client_ranges.contains(range) {
                report.rewritten_ranges.push(format!(
                    "{} {} Range: {} (status {})",
                    request.method, request.path, range, request.status
                ));
            }
            if request.method == "GET" && request.status == 206 {
                // v3 answers a damaged range by invalidating the whole cached object
                // through the accounted invalidation path, not by evicting one range,
                // so undamaged ranges may be re-fetched after a fault. Each re-fetch
                // still carries the client's own signed Range (checked above).
                report.upstream_body_fetches.push(range.clone());
            }
        }
    }
    // Bound, not count: after the whole-object invalidation each of the two
    // downloaders may fetch each of its ranges from S3 once. More than that is a
    // repair loop.
    let refetch_bound = 2 * client_ranges.len();
    if fault != Fault::None && report.upstream_body_fetches.len() > refetch_bound {
        report.metadata_problems.push(format!(
            "{} upstream body fetches after the fault for two downloaders (bound {})",
            report.upstream_body_fetches.len(),
            refetch_bound
        ));
    }
    match proxy.read_meta(&cache_key) {
        Some(meta) => {
            if meta.object_metadata.etag.is_empty() {
                report
                    .metadata_problems
                    .push("final .meta has an empty ETag".to_string());
            } else if meta.object_metadata.etag != object_etag {
                report.metadata_problems.push(format!(
                    "final .meta etag {:?} != S3 etag {:?}",
                    meta.object_metadata.etag, object_etag
                ));
            }
            for range in &meta.ranges {
                let expected_len = range.end - range.start + 1;
                if range.uncompressed_size != expected_len {
                    report.metadata_problems.push(format!(
                        "range {}-{} records {} uncompressed bytes for a {}-byte extent",
                        range.start, range.end, range.uncompressed_size, expected_len
                    ));
                }
                let bin = proxy.cache_dir.join("ranges").join(&range.file_path);
                match std::fs::metadata(&bin) {
                    Ok(m) if m.len() == range.compressed_size => {}
                    Ok(m) => report.metadata_problems.push(format!(
                        "range {}-{} file is {} bytes but .meta records compressed_size {}",
                        range.start,
                        range.end,
                        m.len(),
                        range.compressed_size
                    )),
                    Err(_) => report.metadata_problems.push(format!(
                        "range {}-{} listed in .meta but {} is missing",
                        range.start,
                        range.end,
                        bin.display()
                    )),
                }
            }
        }
        None => report
            .metadata_problems
            .push("no .meta after the downloads settled".to_string()),
    }
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_publication_then_parallel_signed_ranged_downloads_stay_consistent() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let mock = MockS3::new();
    let (mock_addr, _mock_shutdown) = mock.clone().spawn().await;
    let proxy = spawn_proxy(mock_addr).await;

    // Enough repetitions, with and without a damaged final range, to expose ordering
    // races between the publication, the consolidator, and the parallel readers.
    let faults = [
        Fault::None,
        Fault::TruncateFinalRange,
        Fault::None,
        Fault::DeleteFinalRange,
        Fault::None,
        Fault::TruncateFinalRangeThenUpstreamError,
        Fault::None,
        Fault::TruncateFinalRange,
        Fault::None,
        Fault::TruncateFinalRangeThenUpstreamError,
        Fault::None,
        Fault::DeleteFinalRange,
    ];
    let mut reports = Vec::new();
    for (iteration, fault) in faults.into_iter().enumerate() {
        let report = tokio::time::timeout(
            Duration::from_secs(90),
            run_iteration(&proxy, &mock, mock_addr, iteration, fault),
        )
        .await
        .unwrap_or_else(|_| panic!("iteration {} timed out", iteration));
        reports.push(report);
    }

    let failing: Vec<String> = reports
        .iter()
        .filter(|r| !r.is_clean())
        .map(IterationReport::summary)
        .collect();
    assert!(
        failing.is_empty(),
        "{} of {} iterations violated the cache invariants (total upstream signature failures: {}):\n{}",
        failing.len(),
        reports.len(),
        mock.signature_failures(),
        failing.join("\n")
    );
}
