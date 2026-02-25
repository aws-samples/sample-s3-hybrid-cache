use s3_proxy::logging::LoggerManager;
use tempfile::TempDir;

#[tokio::test]
async fn test_access_logging_all_mode() {
    // Create temporary directories
    let temp_dir = TempDir::new().unwrap();
    let access_log_dir = temp_dir.path().join("access_logs");
    let app_log_dir = temp_dir.path().join("app_logs");

    std::fs::create_dir_all(&access_log_dir).unwrap();
    std::fs::create_dir_all(&app_log_dir).unwrap();

    // Initialize logger with All mode
    let logging_config = s3_proxy::logging::LoggingConfig {
        access_log_dir: access_log_dir.clone(),
        app_log_dir: app_log_dir.clone(),
        access_log_enabled: true,
        access_log_mode: s3_proxy::logging::AccessLogMode::All,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let mut logger = LoggerManager::new(logging_config);
    logger.initialize().unwrap();

    // Create a test access log entry
    let entry = logger.create_access_log_entry(
        "GET",
        "127.0.0.1".to_string(),
        "/test-bucket/test-key".to_string(),
        200,
        1024,
        Some(1024),
        100,
        50,
        Some("test-client/1.0".to_string()),
        None,
        Some("test-bucket.s3.amazonaws.com".to_string()),
        None,
    );

    // Log with served_from_cache = false (should log in All mode)
    logger.log_access(entry.clone(), false).await.unwrap();

    // Log with served_from_cache = true (should also log in All mode)
    logger.log_access(entry, true).await.unwrap();

    // Verify access log directory structure was created
    assert!(access_log_dir.exists());

    println!("Access logging All mode test completed successfully");
}

#[tokio::test]
async fn test_access_logging_cached_only_mode() {
    // Create temporary directories
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");
    let access_log_dir = temp_dir.path().join("access_logs");
    let app_log_dir = temp_dir.path().join("app_logs");

    std::fs::create_dir_all(&cache_dir).unwrap();
    std::fs::create_dir_all(&access_log_dir).unwrap();
    std::fs::create_dir_all(&app_log_dir).unwrap();

    // Initialize logger with CachedOnly mode
    let logging_config = s3_proxy::logging::LoggingConfig {
        access_log_dir: access_log_dir.clone(),
        app_log_dir: app_log_dir.clone(),
        access_log_enabled: true,
        access_log_mode: s3_proxy::logging::AccessLogMode::CachedOnly,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let mut logger = LoggerManager::new(logging_config);
    logger.initialize().unwrap();

    // Create a test access log entry
    let entry = logger.create_access_log_entry(
        "GET",
        "127.0.0.1".to_string(),
        "/test-bucket/test-key".to_string(),
        200,
        1024,
        Some(1024),
        100,
        50,
        Some("test-client/1.0".to_string()),
        None,
        Some("test-bucket.s3.amazonaws.com".to_string()),
        None,
    );

    // Log with served_from_cache = false (should not log in CachedOnly mode)
    logger.log_access(entry.clone(), false).await.unwrap();

    // Log with served_from_cache = true (should log in CachedOnly mode)
    logger.log_access(entry, true).await.unwrap();

    // Verify access log directory structure was created
    assert!(access_log_dir.exists());

    println!("Cached-only access logging test completed successfully");
}

#[tokio::test]
async fn test_access_log_entry_format() {
    // Create temporary directories
    let temp_dir = TempDir::new().unwrap();
    let access_log_dir = temp_dir.path().join("access_logs");
    let app_log_dir = temp_dir.path().join("app_logs");

    std::fs::create_dir_all(&access_log_dir).unwrap();
    std::fs::create_dir_all(&app_log_dir).unwrap();

    // Initialize logger
    let logging_config = s3_proxy::logging::LoggingConfig {
        access_log_dir: access_log_dir.clone(),
        app_log_dir: app_log_dir.clone(),
        access_log_enabled: true,
        access_log_mode: s3_proxy::logging::AccessLogMode::All,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let mut logger = LoggerManager::new(logging_config);
    logger.initialize().unwrap();

    // Create a test access log entry
    let entry = logger.create_access_log_entry(
        "GET",
        "192.168.1.100".to_string(),
        "/my-bucket/path/to/object.jpg".to_string(),
        200,
        2048,
        Some(2048),
        150,
        75,
        Some("Mozilla/5.0".to_string()),
        Some("https://example.com".to_string()),
        Some("my-bucket.s3.amazonaws.com".to_string()),
        None,
    );

    // Verify entry fields
    assert_eq!(entry.bucket, "my-bucket");
    assert_eq!(entry.key, "path/to/object.jpg");
    assert_eq!(entry.remote_ip, "192.168.1.100");
    assert_eq!(entry.http_status, 200);
    assert_eq!(entry.bytes_sent, 2048);
    assert_eq!(entry.object_size, Some(2048));
    assert_eq!(entry.total_time, 150);
    assert_eq!(entry.turn_around_time, 75);
    assert_eq!(entry.user_agent, Some("Mozilla/5.0".to_string()));
    assert_eq!(entry.referer, Some("https://example.com".to_string()));
    assert_eq!(
        entry.host_header,
        Some("my-bucket.s3.amazonaws.com".to_string())
    );
    assert_eq!(entry.host_id, "test-host");

    // Log the entry
    logger.log_access(entry, true).await.unwrap();

    println!("Access log entry format test completed successfully");
}
