//! Integration tests for S3 client functionality

use bytes::Bytes;
use hyper::{Method, Uri};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::ConnectionPoolConfig;
use s3_proxy::s3_client::{
    build_s3_request_context, ConditionalHeaders, ConditionalValidationResult, S3Client,
};
use std::collections::HashMap;
use std::time::Duration;

/// Helper function to create test metadata
fn create_test_cache_metadata(
    etag: &str,
    last_modified: &str,
    content_length: u64,
) -> CacheMetadata {
    CacheMetadata {
        etag: etag.to_string(),
        last_modified: last_modified.to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    }
}

fn create_test_config() -> ConnectionPoolConfig {
    ConnectionPoolConfig {
        max_connections_per_ip: 10,
        dns_refresh_interval: Duration::from_secs(60),
        connection_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(60),
        keepalive_enabled: true,
        max_idle_per_host: 1,
        max_lifetime: Duration::from_secs(300),
        pool_check_interval: Duration::from_secs(10),
        dns_servers: Vec::new(),
        endpoint_overrides: std::collections::HashMap::new(),
    }
}

#[tokio::test]
async fn test_s3_client_creation() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = create_test_config();
    let client = S3Client::new(&config, None);
    assert!(client.is_ok(), "S3Client should be created successfully");
}

#[test]
fn test_conditional_headers_extraction() {
    let mut headers = HashMap::new();
    headers.insert("if-match".to_string(), "\"etag123\"".to_string());
    headers.insert("if-none-match".to_string(), "\"etag456\"".to_string());
    headers.insert(
        "if-modified-since".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );

    let conditional_headers = S3Client::extract_conditional_headers(&headers);

    assert!(conditional_headers.is_some());
    let headers = conditional_headers.unwrap();
    assert_eq!(headers.if_match, Some("\"etag123\"".to_string()));
    assert_eq!(headers.if_none_match, Some("\"etag456\"".to_string()));
    assert_eq!(
        headers.if_modified_since,
        Some("Wed, 21 Oct 2015 07:28:00 GMT".to_string())
    );
    assert_eq!(headers.if_unmodified_since, None);
}

#[test]
fn test_conditional_headers_extraction_empty() {
    let headers = HashMap::new();
    let conditional_headers = S3Client::extract_conditional_headers(&headers);
    assert!(conditional_headers.is_none());
}

#[tokio::test]
async fn test_conditional_validation_if_match() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let conditional_headers = ConditionalHeaders {
        if_match: Some("\"etag123\"".to_string()),
        if_none_match: None,
        if_modified_since: None,
        if_unmodified_since: None,
    };

    let cache_metadata =
        create_test_cache_metadata("\"etag123\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let result = client
        .validate_conditional_headers(&conditional_headers, &cache_metadata)
        .unwrap();
    assert_eq!(result, ConditionalValidationResult::Valid);
}

#[tokio::test]
async fn test_conditional_validation_if_match_failed() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let conditional_headers = ConditionalHeaders {
        if_match: Some("\"etag123\"".to_string()),
        if_none_match: None,
        if_modified_since: None,
        if_unmodified_since: None,
    };

    let cache_metadata =
        create_test_cache_metadata("\"etag456\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024); // Different ETag

    let result = client
        .validate_conditional_headers(&conditional_headers, &cache_metadata)
        .unwrap();
    assert_eq!(result, ConditionalValidationResult::PreconditionFailed);
}

#[tokio::test]
async fn test_conditional_validation_if_none_match() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let conditional_headers = ConditionalHeaders {
        if_match: None,
        if_none_match: Some("\"etag123\"".to_string()),
        if_modified_since: None,
        if_unmodified_since: None,
    };

    let cache_metadata =
        create_test_cache_metadata("\"etag123\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024); // Same ETag

    let result = client
        .validate_conditional_headers(&conditional_headers, &cache_metadata)
        .unwrap();
    assert_eq!(result, ConditionalValidationResult::NotModified);
}

#[test]
fn test_build_s3_request_context() {
    let method = Method::GET;
    let uri: Uri = "https://example.com/bucket/key".parse().unwrap();
    let mut headers = HashMap::new();
    headers.insert("host".to_string(), "example.com".to_string());
    headers.insert("if-match".to_string(), "\"etag123\"".to_string());
    let body = Some(Bytes::from("test body"));
    let host = "example.com".to_string();

    let context = build_s3_request_context(
        method,
        uri.clone(),
        headers.clone(),
        body.clone(),
        host.clone(),
    );

    assert_eq!(context.method, Method::GET);
    assert_eq!(context.uri, uri);
    assert_eq!(context.headers, headers);
    assert_eq!(context.body, body);
    assert_eq!(context.host, host);
    assert_eq!(context.request_size, Some(9)); // "test body".len()
    assert!(context.conditional_headers.is_some());

    let conditional = context.conditional_headers.unwrap();
    assert_eq!(conditional.if_match, Some("\"etag123\"".to_string()));
}

#[tokio::test]
async fn test_build_conditional_headers() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let _client = S3Client::new(&create_test_config(), None).unwrap();

    let mut original_headers = HashMap::new();
    original_headers.insert("host".to_string(), "example.com".to_string());
    original_headers.insert("user-agent".to_string(), "test-agent".to_string());

    let cache_metadata =
        create_test_cache_metadata("\"etag123\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let headers = S3Client::build_conditional_headers(&original_headers, Some(&cache_metadata));

    // Should include original headers
    assert_eq!(headers.get("host"), Some(&"example.com".to_string()));
    assert_eq!(headers.get("user-agent"), Some(&"test-agent".to_string()));

    // Should add conditional headers from cache metadata when not specified by client
    assert_eq!(
        headers.get("if-unmodified-since"),
        Some(&"Wed, 21 Oct 2015 07:28:00 GMT".to_string())
    );
    assert_eq!(headers.get("if-match"), Some(&"\"etag123\"".to_string()));
}

#[tokio::test]
async fn test_build_conditional_headers_preserves_client_headers() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let _client = S3Client::new(&create_test_config(), None).unwrap();

    let mut original_headers = HashMap::new();
    original_headers.insert("host".to_string(), "example.com".to_string());
    // Client specifies their own conditional headers
    original_headers.insert("if-match".to_string(), "\"client-etag\"".to_string());
    original_headers.insert(
        "if-unmodified-since".to_string(),
        "Thu, 22 Oct 2015 08:00:00 GMT".to_string(),
    );

    let cache_metadata =
        create_test_cache_metadata("\"cache-etag\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let headers = S3Client::build_conditional_headers(&original_headers, Some(&cache_metadata));

    // Should preserve client-specified conditional headers exactly
    assert_eq!(
        headers.get("if-match"),
        Some(&"\"client-etag\"".to_string())
    );
    assert_eq!(
        headers.get("if-unmodified-since"),
        Some(&"Thu, 22 Oct 2015 08:00:00 GMT".to_string())
    );

    // Should NOT override with cache metadata values
    assert_ne!(headers.get("if-match"), Some(&"\"cache-etag\"".to_string()));
    assert_ne!(
        headers.get("if-unmodified-since"),
        Some(&"Wed, 21 Oct 2015 07:28:00 GMT".to_string())
    );
}

#[tokio::test]
async fn test_build_conditional_headers_partial_client_specification() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let _client = S3Client::new(&create_test_config(), None).unwrap();

    let mut original_headers = HashMap::new();
    original_headers.insert("host".to_string(), "example.com".to_string());
    // Client only specifies If-Match, not If-Unmodified-Since
    original_headers.insert("if-match".to_string(), "\"client-etag\"".to_string());

    let cache_metadata =
        create_test_cache_metadata("\"cache-etag\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let headers = S3Client::build_conditional_headers(&original_headers, Some(&cache_metadata));

    // Should preserve client-specified If-Match
    assert_eq!(
        headers.get("if-match"),
        Some(&"\"client-etag\"".to_string())
    );

    // Should add cache validation for If-Unmodified-Since since client didn't specify it
    assert_eq!(
        headers.get("if-unmodified-since"),
        Some(&"Wed, 21 Oct 2015 07:28:00 GMT".to_string())
    );
}

#[tokio::test]
async fn test_detect_metadata_mismatch_etag() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let mut s3_headers = HashMap::new();
    s3_headers.insert("ETag".to_string(), "\"new-etag\"".to_string());
    s3_headers.insert(
        "Last-Modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );

    let cached_metadata =
        create_test_cache_metadata("\"old-etag\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let mismatch = client.detect_metadata_mismatch(&s3_headers, &cached_metadata);
    assert!(mismatch, "Should detect ETag mismatch");
}

#[tokio::test]
async fn test_detect_metadata_mismatch_last_modified() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let mut s3_headers = HashMap::new();
    s3_headers.insert("ETag".to_string(), "\"same-etag\"".to_string());
    s3_headers.insert(
        "Last-Modified".to_string(),
        "Thu, 22 Oct 2015 08:00:00 GMT".to_string(),
    );

    let cached_metadata =
        create_test_cache_metadata("\"same-etag\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let mismatch = client.detect_metadata_mismatch(&s3_headers, &cached_metadata);
    assert!(mismatch, "Should detect Last-Modified mismatch");
}

#[tokio::test]
async fn test_detect_metadata_no_mismatch() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let mut s3_headers = HashMap::new();
    s3_headers.insert("ETag".to_string(), "\"same-etag\"".to_string());
    s3_headers.insert(
        "Last-Modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );

    let cached_metadata =
        create_test_cache_metadata("\"same-etag\"", "Wed, 21 Oct 2015 07:28:00 GMT", 1024);

    let mismatch = client.detect_metadata_mismatch(&s3_headers, &cached_metadata);
    assert!(
        !mismatch,
        "Should not detect mismatch when metadata matches"
    );
}

#[tokio::test]
async fn test_extract_metadata_from_response() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = S3Client::new(&create_test_config(), None).unwrap();

    let mut headers = HashMap::new();
    headers.insert("ETag".to_string(), "\"response-etag\"".to_string());
    headers.insert(
        "Last-Modified".to_string(),
        "Thu, 22 Oct 2015 08:00:00 GMT".to_string(),
    );
    headers.insert("Content-Length".to_string(), "2048".to_string());
    headers.insert("Cache-Control".to_string(), "max-age=3600".to_string());

    let metadata = client.extract_metadata_from_response(&headers);

    assert_eq!(metadata.etag, "\"response-etag\"");
    assert_eq!(metadata.last_modified, "Thu, 22 Oct 2015 08:00:00 GMT");
    assert_eq!(metadata.content_length, 2048);
    assert_eq!(metadata.cache_control, Some("max-age=3600".to_string()));
    assert_eq!(metadata.part_number, None);
}
