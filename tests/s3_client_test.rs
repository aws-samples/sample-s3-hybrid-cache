//! Integration tests for S3 client functionality

use bytes::Bytes;
use hyper::{Method, Uri};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::ConnectionPoolConfig;
use s3_proxy::s3_client::{build_s3_request_context, S3Client};
use std::collections::HashMap;
use std::time::Duration;

/// Helper function to create test metadata
#[allow(dead_code)]
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
        ip_distribution_enabled: false,
        max_idle_per_ip: 10,
        ..Default::default()
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
    // Conditional request handling lives in http_proxy.rs; s3_client forwards headers as-is.
    assert_eq!(
        context.headers.get("if-match"),
        Some(&"\"etag123\"".to_string())
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
