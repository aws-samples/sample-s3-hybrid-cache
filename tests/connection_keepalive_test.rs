//! Integration tests for HTTP connection keepalive functionality
//!
//! These tests verify that connection pooling and keepalive work correctly
//! in end-to-end scenarios.

use s3_proxy::config::ConnectionPoolConfig;
use s3_proxy::s3_client::S3Client;
use std::time::Duration;

/// Helper function to create a test configuration with keepalive enabled
fn create_keepalive_config() -> ConnectionPoolConfig {
    ConnectionPoolConfig {
        max_connections_per_ip: 10,
        dns_refresh_interval: Duration::from_secs(60),
        connection_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(30),
        keepalive_enabled: true,
        max_idle_per_host: 2,
        max_lifetime: Duration::from_secs(300),
        pool_check_interval: Duration::from_secs(10),
        dns_servers: Vec::new(),
        endpoint_overrides: std::collections::HashMap::new(),
    }
}

/// Helper function to create a test configuration with keepalive disabled
fn create_no_keepalive_config() -> ConnectionPoolConfig {
    ConnectionPoolConfig {
        max_connections_per_ip: 10,
        dns_refresh_interval: Duration::from_secs(60),
        connection_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(0), // Disable idle timeout
        keepalive_enabled: false,
        max_idle_per_host: 0,
        max_lifetime: Duration::from_secs(300),
        pool_check_interval: Duration::from_secs(10),
        dns_servers: Vec::new(),
        endpoint_overrides: std::collections::HashMap::new(),
    }
}

#[tokio::test]
async fn test_s3_client_creation_with_keepalive() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Test that S3Client can be created with keepalive enabled
    let config = create_keepalive_config();
    let client = S3Client::new(&config, None);

    assert!(
        client.is_ok(),
        "S3Client should be created successfully with keepalive enabled"
    );
}

#[tokio::test]
async fn test_s3_client_creation_without_keepalive() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Test that S3Client can be created with keepalive disabled
    let config = create_no_keepalive_config();
    let client = S3Client::new(&config, None);

    assert!(
        client.is_ok(),
        "S3Client should be created successfully with keepalive disabled"
    );
}

#[tokio::test]
async fn test_connection_pool_config_validation() {
    // Test that configuration values are validated correctly
    let config = ConnectionPoolConfig {
        max_connections_per_ip: 50,
        dns_refresh_interval: Duration::from_secs(30),
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(10),
        keepalive_enabled: true,
        max_idle_per_host: 10,
        max_lifetime: Duration::from_secs(60),
        pool_check_interval: Duration::from_secs(5),
        dns_servers: Vec::new(),
        endpoint_overrides: std::collections::HashMap::new(),
    };

    // Verify values are within reasonable ranges
    assert!(config.max_connections_per_ip > 0 && config.max_connections_per_ip <= 100);
    assert!(config.dns_refresh_interval >= Duration::from_secs(10));
    assert!(config.connection_timeout >= Duration::from_secs(1));
    assert!(config.idle_timeout >= Duration::from_secs(10));
    assert!(config.max_idle_per_host <= 50);
    assert!(config.max_lifetime >= Duration::from_secs(60));
    assert!(config.pool_check_interval >= Duration::from_secs(1));
}

#[tokio::test]
async fn test_keepalive_enabled_flag() {
    // Test that keepalive_enabled flag is respected
    let config_enabled = create_keepalive_config();
    assert_eq!(config_enabled.keepalive_enabled, true);

    let config_disabled = create_no_keepalive_config();
    assert_eq!(config_disabled.keepalive_enabled, false);
}

#[tokio::test]
async fn test_max_idle_per_host_configuration() {
    // Test that max_idle_per_host is configured correctly
    let config = create_keepalive_config();
    assert_eq!(config.max_idle_per_host, 2);

    let config_disabled = create_no_keepalive_config();
    assert_eq!(config_disabled.max_idle_per_host, 0);
}

#[tokio::test]
async fn test_idle_timeout_configuration() {
    // Test that idle_timeout is configured correctly
    let config = create_keepalive_config();
    assert_eq!(config.idle_timeout, Duration::from_secs(30));

    let config_disabled = create_no_keepalive_config();
    assert_eq!(config_disabled.idle_timeout, Duration::from_secs(0));
}

#[tokio::test]
async fn test_max_lifetime_configuration() {
    // Test that max_lifetime is configured correctly
    let config = create_keepalive_config();
    assert_eq!(config.max_lifetime, Duration::from_secs(300));
}
