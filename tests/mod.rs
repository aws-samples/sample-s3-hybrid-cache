//! Shared test support harness for download-coordination tests.
//!
//! This module is introduced by Task 0 of the `download-coordination-ttl-correctness`
//! spec. It provides a dependency-injectable `StubS3Client` that implements
//! [`s3_proxy::S3ClientApi`], together with a small response builder, so that
//! subsequent tasks (1, 2, 9, and 10.x) can author in-process tests against the
//! coordination helpers without opening real TLS connections to S3.
//!
//! The harness is deliberately minimal: it records every call to
//! `forward_request` into a shared `Vec<CapturedRequest>` (headers preserved,
//! including `authorization`) and returns a pre-programmed [`StubResponse`]
//! selected by ETag match, authorization-header match, or a global default.
//!
//! Nothing in this module is wired into a test yet. The accompanying
//! `common_compiles.rs` integration-test stub merely forces this module to
//! compile so the `cargo build --release` + `cargo test` + `cargo clippy`
//! acceptance criteria on Task 0 can pass.

#![allow(dead_code)]

use async_trait::async_trait;
use bytes::Bytes;
use hyper::{Method, StatusCode};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::config::TlsConfig;
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// A single S3 call captured by [`StubS3Client`].
///
/// Tests compare `authorization` / `host` / `uri` / header set / body size to
/// verify that the production code forwarded the expected request to S3.
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    pub method: Method,
    pub uri: String,
    pub host: String,
    pub headers: HashMap<String, String>,
    pub body_size: Option<usize>,
}

impl CapturedRequest {
    /// The `authorization` header value, if present. Tests use this to verify
    /// that each waiter's own signed request reached S3.
    pub fn authorization(&self) -> Option<&str> {
        self.headers
            .get("authorization")
            .or_else(|| self.headers.get("Authorization"))
            .map(String::as_str)
    }

    /// The `if-none-match` header value, if present.
    pub fn if_none_match(&self) -> Option<&str> {
        self.headers
            .get("if-none-match")
            .or_else(|| self.headers.get("If-None-Match"))
            .map(String::as_str)
    }

    /// The `if-modified-since` header value, if present.
    pub fn if_modified_since(&self) -> Option<&str> {
        self.headers
            .get("if-modified-since")
            .or_else(|| self.headers.get("If-Modified-Since"))
            .map(String::as_str)
    }
}

/// Pre-programmed response returned by [`StubS3Client`].
///
/// Build via [`StubResponse::ok`], [`StubResponse::not_modified`],
/// [`StubResponse::forbidden`], or [`StubResponse::with_status`] and chain
/// [`StubResponse::with_header`] / [`StubResponse::with_body`] as needed.
#[derive(Debug, Clone)]
pub struct StubResponse {
    pub status: StatusCode,
    pub headers: HashMap<String, String>,
    pub body: Option<Bytes>,
}

impl StubResponse {
    /// Build a 200 OK response with the given body bytes.
    pub fn ok(body: impl Into<Bytes>) -> Self {
        let bytes = body.into();
        let mut headers = HashMap::new();
        headers.insert("content-length".to_string(), bytes.len().to_string());
        Self {
            status: StatusCode::OK,
            headers,
            body: Some(bytes),
        }
    }

    /// Build a 304 Not Modified response. S3 returns 304 with the ETag and
    /// Last-Modified of the current object but no body.
    pub fn not_modified() -> Self {
        Self {
            status: StatusCode::NOT_MODIFIED,
            headers: HashMap::new(),
            body: None,
        }
    }

    /// Build a 403 Forbidden response. Used to simulate a waiter whose
    /// credentials are invalid / revoked.
    pub fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            headers: HashMap::new(),
            body: None,
        }
    }

    /// Build a response with an arbitrary status code.
    pub fn with_status(status: StatusCode) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: None,
        }
    }

    /// Add or overwrite a response header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Set or replace the response body.
    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        let bytes = body.into();
        self.headers
            .insert("content-length".to_string(), bytes.len().to_string());
        self.body = Some(bytes);
        self
    }

    /// Convert this stub into an [`S3Response`] suitable for returning from
    /// [`S3ClientApi::forward_request`]. The body is always buffered.
    fn into_s3_response(self) -> S3Response {
        S3Response {
            status: self.status,
            headers: self.headers,
            body: self.body.map(S3ResponseBody::Buffered),
            request_duration: Duration::from_millis(0),
        }
    }
}

/// Builder-style [`S3ClientApi`] stub for in-process tests.
///
/// Matching order: first by exact `If-None-Match` value, then by exact
/// `authorization` header value, then the global default. If no match and no
/// default is configured, a 500 Internal Server Error is returned so the test
/// fails loudly.
#[derive(Clone)]
pub struct StubS3Client {
    inner: Arc<StubInner>,
}

struct StubInner {
    captured: Mutex<Vec<CapturedRequest>>,
    etag_responses: Mutex<HashMap<String, StubResponse>>,
    auth_responses: Mutex<HashMap<String, StubResponse>>,
    default_response: Mutex<Option<StubResponse>>,
}

impl Default for StubS3Client {
    fn default() -> Self {
        Self::new()
    }
}

impl StubS3Client {
    /// Create a new stub with no responses configured. Calls will return
    /// `500 Internal Server Error` until [`with_default`](Self::with_default)
    /// or one of the targeted routing methods is called.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StubInner {
                captured: Mutex::new(Vec::new()),
                etag_responses: Mutex::new(HashMap::new()),
                auth_responses: Mutex::new(HashMap::new()),
                default_response: Mutex::new(None),
            }),
        }
    }

    /// Route requests that carry a matching `If-None-Match` header to `response`.
    pub fn with_response_for_etag(self, etag: impl Into<String>, response: StubResponse) -> Self {
        self.inner
            .etag_responses
            .lock()
            .expect("stub etag map poisoned")
            .insert(etag.into(), response);
        self
    }

    /// Route requests that carry a matching `authorization` header to `response`.
    pub fn with_response_for_authorization(
        self,
        auth: impl Into<String>,
        response: StubResponse,
    ) -> Self {
        self.inner
            .auth_responses
            .lock()
            .expect("stub auth map poisoned")
            .insert(auth.into(), response);
        self
    }

    /// Set the fall-through response for requests that match neither an ETag
    /// nor an authorization rule.
    pub fn with_default(self, response: StubResponse) -> Self {
        *self
            .inner
            .default_response
            .lock()
            .expect("stub default poisoned") = Some(response);
        self
    }

    /// Read-only snapshot of every request captured so far, in arrival order.
    pub fn captured(&self) -> Vec<CapturedRequest> {
        self.inner
            .captured
            .lock()
            .expect("stub captured poisoned")
            .clone()
    }

    /// Convenience: convert `self` into the trait-object type stored by
    /// `HttpProxy::s3_client`, ready to hand to production code.
    pub fn into_trait_object(self) -> Arc<dyn S3ClientApi + Send + Sync> {
        Arc::new(self)
    }

    fn resolve_response(&self, context: &S3RequestContext) -> StubResponse {
        // Prefer an ETag-targeted rule.
        if let Some(etag) = context
            .headers
            .get("if-none-match")
            .or_else(|| context.headers.get("If-None-Match"))
        {
            if let Some(resp) = self
                .inner
                .etag_responses
                .lock()
                .expect("stub etag map poisoned")
                .get(etag)
            {
                return resp.clone();
            }
        }

        // Next try an authorization-targeted rule.
        if let Some(auth) = context
            .headers
            .get("authorization")
            .or_else(|| context.headers.get("Authorization"))
        {
            if let Some(resp) = self
                .inner
                .auth_responses
                .lock()
                .expect("stub auth map poisoned")
                .get(auth)
            {
                return resp.clone();
            }
        }

        // Fall through to the default.
        self.inner
            .default_response
            .lock()
            .expect("stub default poisoned")
            .clone()
            .unwrap_or_else(|| {
                // No rule and no default — return 500 so the test fails loudly.
                StubResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR)
            })
    }

    fn record(&self, context: &S3RequestContext) {
        let captured = CapturedRequest {
            method: context.method.clone(),
            uri: context.uri.to_string(),
            host: context.host.clone(),
            headers: context.headers.clone(),
            body_size: context.body.as_ref().map(|b| b.len()),
        };
        self.inner
            .captured
            .lock()
            .expect("stub captured poisoned")
            .push(captured);
    }
}

#[async_trait]
impl S3ClientApi for StubS3Client {
    async fn forward_request(&self, context: S3RequestContext) -> Result<S3Response> {
        self.record(&context);
        Ok(self.resolve_response(&context).into_s3_response())
    }

    fn extract_metadata_from_response(&self, headers: &HashMap<String, String>) -> CacheMetadata {
        let etag = headers
            .get("etag")
            .or_else(|| headers.get("ETag"))
            .cloned()
            .unwrap_or_default();
        let last_modified = headers
            .get("last-modified")
            .or_else(|| headers.get("Last-Modified"))
            .cloned()
            .unwrap_or_default();
        let content_length = headers
            .get("content-length")
            .or_else(|| headers.get("Content-Length"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let cache_control = headers
            .get("cache-control")
            .or_else(|| headers.get("Cache-Control"))
            .cloned();
        CacheMetadata {
            etag,
            last_modified,
            content_length,
            part_number: None,
            cache_control,
            access_count: 0,
            last_accessed: SystemTime::now(),
        }
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> ObjectMetadata {
        let etag = headers
            .get("etag")
            .or_else(|| headers.get("ETag"))
            .cloned()
            .unwrap_or_default();
        let last_modified = headers
            .get("last-modified")
            .or_else(|| headers.get("Last-Modified"))
            .cloned()
            .unwrap_or_default();
        let content_length = headers
            .get("content-length")
            .or_else(|| headers.get("Content-Length"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let content_type = headers
            .get("content-type")
            .or_else(|| headers.get("Content-Type"))
            .cloned();
        ObjectMetadata::new_with_headers(
            etag,
            last_modified,
            content_length,
            content_type,
            headers.clone(),
        )
    }

    fn get_connection_pool(&self) -> Arc<tokio::sync::RwLock<ConnectionPoolManager>> {
        // The stub does not service any real connections, so we hand back a
        // fresh `ConnectionPoolManager` built from defaults. Tests that rely
        // on pool state should construct their own and inject via
        // `HttpProxy`, not through the S3 client.
        Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(Default::default())
                .expect("default ConnectionPoolConfig should build a pool manager"),
        ))
    }

    fn has_endpoint_overrides(&self) -> bool {
        false
    }

    async fn set_metrics_manager(
        &self,
        _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
        // No-op: metrics are not exercised by the stub.
    }

    async fn register_endpoint(&self, _endpoint: &str) {
        // No-op: the stub has no DNS refresh loop.
    }

    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }
}

/// Placeholder helper that returns `None` — the stub harness does not need
/// TLS to reach real S3. Present because Task 0 explicitly requests it;
/// later tests that stand up a real `TlsProxyListener` can swap in a concrete
/// [`TlsConfig`] if needed.
pub fn test_tls_config() -> Option<TlsConfig> {
    None
}
