//! Integration tests for S3 Proxy
//!
//! These tests verify the basic functionality and module structure.

use s3_proxy::{
    cache::CacheManager, compression::CompressionHandler, config::Config,
    connection_pool::ConnectionPoolManager, http_proxy::HttpProxy, tcp_proxy::TcpProxy,
};
use std::net::SocketAddr;
use std::path::PathBuf;

#[test]
fn test_basic_module_instantiation() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Test that we can create instances of all major components

    // Config
    let config = Config::default();
    assert_eq!(config.server.http_port, 80);
    assert_eq!(config.server.https_port, 443);

    // HTTP Proxy
    let http_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let config = std::sync::Arc::new(Config::default());
    let _http_proxy = HttpProxy::new(http_addr, config);

    // TCP Proxy
    let tcp_addr = SocketAddr::from(([127, 0, 0, 1], 8443));
    let _tcp_proxy = TcpProxy::new(tcp_addr, std::collections::HashMap::new());

    // Cache Manager
    let cache_dir = PathBuf::from("/tmp/test_cache");
    let _cache_manager = CacheManager::new_with_defaults(cache_dir, false, 0);

    // Compression Handler
    let _compression_handler = CompressionHandler::new(1024, true);

    // Connection Pool Manager
    let _pool_manager =
        ConnectionPoolManager::new().expect("Failed to create connection pool manager");
}

#[test]
fn test_cache_key_generation() {
    // Test basic cache key generation functionality
    // Note: normalize_cache_key strips leading slashes
    let key1 = CacheManager::generate_cache_key("/bucket/object", None);
    assert_eq!(key1, "bucket/object");

    let key2 = CacheManager::generate_cache_key("/my-bucket/my-object", None);
    assert_eq!(key2, "my-bucket/my-object");

    // Test cache key generation with part number and range
    let key3 = CacheManager::generate_cache_key_with_params(
        "/bucket/object",
        None,
        None,
        None,
    );
    assert_eq!(key3, "bucket/object");

    let key4 = CacheManager::generate_cache_key_with_params(
        "/bucket/object",
        Some(1),
        Some((0, 1023)),
        None,
    );
    assert_eq!(key4, "bucket/object:part:1:range:0-1023");
}

#[test]
fn test_compression_threshold() {
    let handler = CompressionHandler::new(1024, true);

    // Small data should not be compressed
    assert!(!handler.should_compress(512));

    // Large data should be compressed
    assert!(handler.should_compress(2048));

    // Disabled compression should never compress
    let disabled_handler = CompressionHandler::new(1024, false);
    assert!(!disabled_handler.should_compress(2048));
}

#[test]
fn test_tcp_proxy_creation() {
    // Test that TCP proxy can be created with different addresses
    let tcp_addr1 = SocketAddr::from(([127, 0, 0, 1], 443));
    let _tcp_proxy1 = TcpProxy::new(tcp_addr1, std::collections::HashMap::new());

    let tcp_addr2 = SocketAddr::from(([0, 0, 0, 0], 8443));
    let _tcp_proxy2 = TcpProxy::new(tcp_addr2, std::collections::HashMap::new());

    // Verify that the proxy can be created without errors
    // The actual functionality will be tested in integration tests
}
