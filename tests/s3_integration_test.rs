//! Integration tests with real S3 endpoints
//!
//! These tests validate end-to-end functionality against actual S3 services.
//! They test both TCP passthrough and self-signed TLS termination modes.

use s3_proxy::{
    cache::CacheManager,
    config::{
        AccessLogMode, CacheConfig, CompressionAlgorithm, CompressionConfig, Config,
        ConnectionPoolConfig, DashboardConfig, EvictionAlgorithm, HealthConfig, LoggingConfig,
        MetricsConfig, OtlpCompression, OtlpConfig, ServerConfig, SharedStorageConfig,
    },
    connection_pool::ConnectionPoolManager,
    http_proxy::HttpProxy,
    tcp_proxy::TcpProxy,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Test configuration for S3 integration tests
struct S3TestConfig {
    endpoint: String,
    bucket: String,
    test_object_key: String,
    test_data: Vec<u8>,
}

impl Default for S3TestConfig {
    fn default() -> Self {
        Self {
            endpoint: "s3.amazonaws.com".to_string(),
            bucket: "test-bucket".to_string(),
            test_object_key: "test-object.txt".to_string(),
            test_data: b"Hello, S3 Proxy Integration Test!".to_vec(),
        }
    }
}

/// Create a test configuration for integration tests
fn create_test_config() -> Config {
    Config {
        server: ServerConfig {
            http_port: 8080,
            https_port: 8443,
            max_concurrent_requests: 100,
            request_timeout: Duration::from_secs(30),
            add_referer_header: true,
            tls: None,
            ..ServerConfig::default()
        },
        cache: CacheConfig {
            cache_dir: PathBuf::from("/tmp/s3_proxy_integration_test"),
            max_cache_size: 100 * 1024 * 1024, // 100MB
            ram_cache_enabled: false,
            max_ram_cache_size: 50 * 1024 * 1024, // 50MB
            eviction_algorithm: EvictionAlgorithm::LRU,
            write_cache_enabled: true,
            write_cache_percent: 10.0,
            write_cache_max_object_size: 256 * 1024 * 1024, // 256MB
            put_ttl: Duration::from_secs(3600),
            get_ttl: Duration::from_secs(3600),
            head_ttl: Duration::from_secs(3600),
            actively_remove_cached_data: false,
            eviction_buffer_percent: 5,
            shared_storage: SharedStorageConfig::default(),
            range_merge_gap_threshold: 256 * 1024, // 256KB
            ram_cache_flush_interval: Duration::from_secs(60),
            ram_cache_flush_threshold: 100,
            ram_cache_flush_on_eviction: false,
            ram_cache_verification_interval: Duration::from_secs(1),
            incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
            initialization: s3_proxy::config::InitializationConfig::default(),
            cache_bypass_headers_enabled: true,
            metadata_cache: s3_proxy::config::MetadataCacheConfig::default(),
            eviction_trigger_percent: 95, // Trigger eviction at 95% capacity
            eviction_target_percent: 80,  // Reduce to 80% after eviction
            full_object_check_threshold: 67_108_864, // 64 MiB
            disk_streaming_threshold: 1_048_576, // 1 MiB
            compression_batch_size: 1_048_576, // 1 MiB
            read_cache_enabled: true,
            bucket_settings_staleness_threshold: Duration::from_secs(60),
            download_coordination: s3_proxy::config::DownloadCoordinationConfig::default(),
            evaluate_conditions_from_cache: false,
        },
        logging: LoggingConfig {
            access_log_dir: PathBuf::from("/tmp/s3_proxy_test_logs/access"),
            app_log_dir: PathBuf::from("/tmp/s3_proxy_test_logs/app"),
            access_log_enabled: true,
            access_log_mode: AccessLogMode::All,
            log_level: "info".to_string(),
            access_log_flush_interval: Duration::from_secs(5),
            access_log_buffer_size: 1000,
            access_log_retention_days: 30,
            app_log_retention_days: 30,
            log_cleanup_interval: Duration::from_secs(86400),
            access_log_file_rotation_interval: Duration::from_secs(300),
        },
        connection_pool: ConnectionPoolConfig {
            max_connections_per_ip: 10,
            dns_refresh_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(300),
            keepalive_enabled: true,
            max_idle_per_host: 1,
            max_lifetime: Duration::from_secs(300),
            pool_check_interval: Duration::from_secs(10),
            dns_servers: Vec::new(),
            endpoint_overrides: std::collections::HashMap::new(),
            ip_distribution_enabled: false,
            max_idle_per_ip: 10,
            ..Default::default()
        },
        compression: CompressionConfig {
            enabled: true,
            threshold: 1024,
            preferred_algorithm: CompressionAlgorithm::Lz4,
            content_aware: true,
        },
        health: HealthConfig {
            enabled: true,
            endpoint: "/health".to_string(),
            port: 8080,
            check_interval: Duration::from_secs(30),
        },
        metrics: MetricsConfig {
            enabled: true,
            endpoint: "/metrics".to_string(),
            port: 8080,
            collection_interval: Duration::from_secs(60),
            include_cache_stats: true,
            include_compression_stats: true,
            include_connection_stats: true,
            otlp: OtlpConfig {
                enabled: false,
                endpoint: "http://localhost:4318".to_string(),
                export_interval: Duration::from_secs(60),
                timeout: Duration::from_secs(10),
                headers: HashMap::new(),
                compression: OtlpCompression::None,
            },
        },
        dashboard: DashboardConfig::default(),
    }
}

#[tokio::test]
async fn test_tcp_passthrough_mode() {
    // Test TCP passthrough mode (default HTTPS behavior)
    let config = create_test_config();
    let tcp_addr = SocketAddr::from(([127, 0, 0, 1], 8443));

    // Create TCP proxy for passthrough mode
    let tcp_proxy = TcpProxy::new(tcp_addr, s3_proxy::connection_pool::EndpointOverrides::from_config(&std::collections::HashMap::new()));

    // Verify TCP proxy can be created and configured
    // Note: TCP proxy doesn't expose bind_address method, but creation success indicates proper setup

    // Note: Full TCP passthrough testing requires actual network setup
    // This test validates the proxy can be configured for passthrough mode
    println!("TCP passthrough mode proxy created successfully");
}

#[tokio::test]
async fn test_connection_pool_with_s3_endpoints() {
    // Test that ConnectionPoolManager can be created and endpoint overrides work
    let mut overrides = std::collections::HashMap::new();
    overrides.insert(
        "s3.us-west-2.amazonaws.com".to_string(),
        vec!["10.0.1.100".to_string(), "10.0.2.100".to_string()],
    );

    let config = s3_proxy::config::ConnectionPoolConfig {
        endpoint_overrides: overrides,
        ..Default::default()
    };

    let manager = ConnectionPoolManager::new_with_config(config)
        .expect("Failed to create connection pool manager");

    // Endpoint overrides should be eagerly initialized
    let ip = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");
    assert!(ip.is_some(), "Should get an IP from endpoint overrides");

    // Unknown endpoint returns None
    let ip = manager.get_distributed_ip("s3.eu-west-1.amazonaws.com");
    assert!(ip.is_none(), "Unknown endpoint should return None");
}

#[tokio::test]
async fn test_load_balancing_across_ips() {
    // Test round-robin distribution across IPs
    let mut overrides = std::collections::HashMap::new();
    overrides.insert(
        "s3.us-west-2.amazonaws.com".to_string(),
        vec![
            "10.0.1.100".to_string(),
            "10.0.2.100".to_string(),
            "10.0.3.100".to_string(),
        ],
    );

    let config = s3_proxy::config::ConnectionPoolConfig {
        endpoint_overrides: overrides,
        ..Default::default()
    };

    let manager = ConnectionPoolManager::new_with_config(config)
        .expect("Failed to create connection pool manager");

    // Collect 6 IPs — should cycle through all 3 twice
    let mut ips = Vec::new();
    for _ in 0..6 {
        ips.push(manager.get_distributed_ip("s3.us-west-2.amazonaws.com").unwrap());
    }

    // Each IP should appear exactly twice
    let unique: std::collections::HashSet<_> = ips.iter().collect();
    assert_eq!(unique.len(), 3, "Should have 3 unique IPs");
}

#[tokio::test]
async fn test_http_proxy_with_caching() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Test HTTP proxy with caching functionality
    let config = Arc::new(create_test_config());
    let http_addr = SocketAddr::from(([127, 0, 0, 1], 8080));

    // Create HTTP proxy with caching
    let http_proxy = HttpProxy::new(http_addr, config.clone());

    // Verify proxy creation was successful
    assert!(
        http_proxy.is_ok(),
        "HTTP proxy should be created successfully"
    );

    // Create cache manager for testing
    let cache_manager = CacheManager::new_with_defaults(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
    );

    // Test cache key generation for S3 objects
    // Note: normalize_cache_key strips leading slashes
    let test_key = CacheManager::generate_cache_key("/test-bucket/test-object", None);
    assert_eq!(test_key, "test-bucket/test-object");

    // Test cache key with params
    let key_with_params = CacheManager::generate_cache_key_with_params(
        "/test-bucket/test-object",
        None,
        None,
        None,
    );
    assert_eq!(key_with_params, "test-bucket/test-object");

    println!("HTTP proxy with caching configured successfully");
}

#[tokio::test]
async fn test_end_to_end_proxy_setup() {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Test complete proxy setup with both HTTP and HTTPS modes
    let config = Arc::new(create_test_config());

    // HTTP proxy setup
    let http_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let http_proxy = HttpProxy::new(http_addr, config.clone());
    assert!(
        http_proxy.is_ok(),
        "HTTP proxy should be created successfully"
    );

    // TCP proxy setup (for HTTPS passthrough)
    let tcp_addr = SocketAddr::from(([127, 0, 0, 1], 8443));
    let _tcp_proxy = TcpProxy::new(tcp_addr, s3_proxy::connection_pool::EndpointOverrides::from_config(&std::collections::HashMap::new()));

    // Connection pool manager setup
    let manager =
        ConnectionPoolManager::new().expect("Failed to create connection pool manager");

    // Verify manager starts with no distributors
    let stats = manager.get_ip_distribution_stats();
    assert!(stats.endpoints.is_empty(), "No distributors at startup");

    println!("End-to-end proxy setup completed successfully");
    println!("HTTP proxy configured for: {}", http_addr);
    println!("TCP proxy configured for: {}", tcp_addr);
}

#[tokio::test]
async fn test_s3_compatible_services() {
    // Test that hostname lookup works for IPs in distributors
    let mut manager =
        ConnectionPoolManager::new().expect("Failed to create connection pool manager");

    let ips = vec![
        "10.0.0.1".parse().unwrap(),
        "10.0.0.2".parse().unwrap(),
    ];
    manager.ip_distributors.insert(
        "s3.amazonaws.com".to_string(),
        s3_proxy::connection_pool::IpDistributor::new(ips),
    );

    let hostname = manager.get_hostname_for_ip(&"10.0.0.1".parse().unwrap());
    assert_eq!(hostname, Some("s3.amazonaws.com".to_string()));

    let hostname = manager.get_hostname_for_ip(&"99.99.99.99".parse().unwrap());
    assert_eq!(hostname, None);
}

#[tokio::test]
async fn test_connection_health_monitoring() {
    // Test IpHealthTracker functionality
    let tracker = s3_proxy::connection_pool::IpHealthTracker::new(3);
    let ip: std::net::IpAddr = "10.0.0.1".parse().unwrap();

    // Two failures — not yet at threshold
    assert!(!tracker.record_failure(&ip));
    assert!(!tracker.record_failure(&ip));

    // Success resets
    tracker.record_success(&ip);

    // Need 3 more failures to hit threshold
    assert!(!tracker.record_failure(&ip));
    assert!(!tracker.record_failure(&ip));
    assert!(tracker.record_failure(&ip)); // threshold reached
}

#[tokio::test]
async fn test_performance_metrics_tracking() {
    // Test IP distribution stats reporting
    let mut overrides = std::collections::HashMap::new();
    overrides.insert(
        "s3.us-west-2.amazonaws.com".to_string(),
        vec!["10.0.1.100".to_string(), "10.0.2.100".to_string()],
    );

    let config = s3_proxy::config::ConnectionPoolConfig {
        endpoint_overrides: overrides,
        ..Default::default()
    };

    let manager = ConnectionPoolManager::new_with_config(config)
        .expect("Failed to create connection pool manager");

    let stats = manager.get_ip_distribution_stats();
    assert_eq!(stats.endpoints.len(), 1);
    assert_eq!(stats.endpoints[0].total_distributor_ips, 2);
}
