//! Tests for `S3ClientApi::forward_request_pinned`.
//!
//! Validates Requirements 2.4, 4.1, 4.2:
//! - The default trait impl delegates to `forward_request` regardless of `pinned_ip`.
//! - Calling with `None` is behaviorally identical to `forward_request`.
//!
//! # What is deliberately NOT covered here, and why no unit test covers it
//!
//! The concrete `S3Client::forward_request_pinned` override — the authority
//! rewrite, `Host`-header preservation for SigV4, and `upstream_overrides`
//! winning over the pin (Requirement 4.3) — is **not** unit-testable at the
//! socket level, so do not add a test here expecting it to work.
//!
//! `https_connector::connect_port` dials the literal **443** with TLS whenever no
//! `upstream_overrides` entry matches the request authority (the
//! Secure_Default_Behaviour path), ignoring any port in the URI. So a plaintext
//! listener on an ephemeral loopback port is unreachable unless an override is
//! configured for it — and configuring that override is exactly what makes
//! `forward_request_pinned` discard the pin. The two preconditions are mutually
//! exclusive: pin-honoured and pin-ignored both fail to connect, so a
//! differential assertion passes for the wrong reason. Covering it honestly would
//! need a TLS listener on privileged port 443 with an SNI-matching cert and a
//! `get_hostname_for_ip` mapping.
//!
//! It is covered instead by fleet deployment verification, which has a real
//! origin and real overrides: T22/T23/T24 exercise the override transport paths,
//! and T38 exercises hedging (and therefore pinning) end to end. See
//! `.kiro/steering/pre-push-checklist.md` step 8.

mod common;

use common::{StubResponse, StubS3Client};
use hyper::{Method, StatusCode};
use s3_proxy::{S3ClientApi, S3RequestContext};
use std::collections::HashMap;
use std::net::IpAddr;

fn make_context(host: &str, path: &str) -> S3RequestContext {
    let uri = format!("http://{}{}", host, path).parse().unwrap();
    S3RequestContext {
        method: Method::GET,
        uri,
        headers: HashMap::new(),
        body: None,
        host: host.to_string(),
        request_size: None,
        operation_type: None,
        allow_streaming: false,
    }
}

/// The default trait impl ignores `pinned_ip` and delegates to `forward_request`.
/// Validates: Requirements 2.4, 4.1
#[tokio::test]
async fn default_impl_with_some_ip_delegates_to_forward_request() {
    let stub = StubS3Client::new().with_default(StubResponse::ok(b"hello".as_ref()));

    let ctx = make_context("s3.us-east-1.amazonaws.com", "/bucket/key");
    let ip: IpAddr = "10.0.0.42".parse().unwrap();

    let response = stub.forward_request_pinned(ctx, Some(ip)).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);

    // The stub should have recorded the request via forward_request
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].host, "s3.us-east-1.amazonaws.com");
    // URI is unchanged (the default impl does not rewrite authority)
    assert!(captured[0].uri.contains("s3.us-east-1.amazonaws.com"));
}

/// Calling with `None` behaves identically to `forward_request`.
/// Validates: Requirements 4.2
#[tokio::test]
async fn default_impl_with_none_delegates_to_forward_request() {
    let stub = StubS3Client::new().with_default(StubResponse::ok(b"world".as_ref()));

    let ctx = make_context("s3.us-east-1.amazonaws.com", "/bucket/key2");

    let response = stub.forward_request_pinned(ctx, None).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].host, "s3.us-east-1.amazonaws.com");
}

/// Both `forward_request` and `forward_request_pinned(ctx, None)` produce the
/// same observable result (same captured request, same response).
/// Validates: Requirements 2.4
#[tokio::test]
async fn pinned_none_identical_to_forward_request() {
    let stub = StubS3Client::new().with_default(StubResponse::ok(b"data".as_ref()));

    let ctx1 = make_context("s3.us-west-2.amazonaws.com", "/bucket/obj");
    let ctx2 = make_context("s3.us-west-2.amazonaws.com", "/bucket/obj");

    let resp1 = stub.forward_request(ctx1).await.unwrap();
    let resp2 = stub.forward_request_pinned(ctx2, None).await.unwrap();

    assert_eq!(resp1.status, resp2.status);

    let captured = stub.captured();
    assert_eq!(captured.len(), 2);
    // Both calls produced identical captured requests
    assert_eq!(captured[0].uri, captured[1].uri);
    assert_eq!(captured[0].host, captured[1].host);
    assert_eq!(captured[0].method, captured[1].method);
}

/// The Host header from the request context is preserved by the default impl
/// (it doesn't touch headers at all).
/// Validates: Requirements 2.4 (SigV4 compatibility)
#[tokio::test]
async fn default_impl_preserves_host_header() {
    let stub = StubS3Client::new().with_default(StubResponse::ok(b"x".as_ref()));

    let mut headers = HashMap::new();
    headers.insert("host".to_string(), "s3.eu-west-1.amazonaws.com".to_string());
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256 Credential=AKIA.../s3/aws4_request".to_string(),
    );

    let ctx = S3RequestContext {
        method: Method::GET,
        uri: "http://s3.eu-west-1.amazonaws.com/bucket/key"
            .parse()
            .unwrap(),
        headers,
        body: None,
        host: "s3.eu-west-1.amazonaws.com".to_string(),
        request_size: None,
        operation_type: None,
        allow_streaming: false,
    };

    let ip: IpAddr = "192.168.1.1".parse().unwrap();
    let _ = stub.forward_request_pinned(ctx, Some(ip)).await.unwrap();

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    // Host header is preserved (the default impl doesn't rewrite anything)
    assert_eq!(
        captured[0].headers.get("host").unwrap(),
        "s3.eu-west-1.amazonaws.com"
    );
}
