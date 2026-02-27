//! Tests for the logging system
//!
//! Tests S3-compatible access logging and application logging functionality.

use chrono::Utc;
use s3_proxy::logging::{AccessLogEntry, AccessLogMode, LoggerManager, LoggingConfig};
use tempfile::TempDir;

#[tokio::test]
async fn test_access_log_creation() {
    let temp_dir = TempDir::new().unwrap();
    let access_log_dir = temp_dir.path().join("access");
    let app_log_dir = temp_dir.path().join("app");

    let config = LoggingConfig {
        access_log_dir,
        app_log_dir,
        access_log_enabled: true,
        access_log_mode: AccessLogMode::All,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let mut logger = LoggerManager::new(config);
    logger.initialize().unwrap();

    // Create a test access log entry
    let entry = AccessLogEntry {
        bucket_owner: "test-bucket".to_string(),
        bucket: "test-bucket".to_string(),
        time: Utc::now(),
        remote_ip: "192.168.1.1".to_string(),
        requester: "-".to_string(),
        request_id: "test-request-id".to_string(),
        operation: "REST.GET.OBJECT".to_string(),
        key: "test-key".to_string(),
        request_uri: "/test-bucket/test-key".to_string(),
        http_status: 200,
        error_code: None,
        bytes_sent: 1024,
        object_size: Some(1024),
        total_time: 100,
        turn_around_time: 50,
        referer: None,
        user_agent: Some("test-agent".to_string()),
        host_id: "test-host".to_string(),
        signature_version: None,
        cipher_suite: None,
        authentication_type: Some("Anonymous".to_string()),
        host_header: Some("test-bucket.s3.amazonaws.com".to_string()),
        tls_version: None,
        access_point_arn: None,
        acl_required: None,
        source_region: None,
    };

    // Log the entry
    logger.log_access(entry, false).await.unwrap();

    // Force flush to write buffered entries to disk
    let _ = logger.force_flush().await;

    // Verify the log file was created
    let access_log_files = std::fs::read_dir(temp_dir.path().join("access"))
        .unwrap()
        .flatten()
        .collect::<Vec<_>>();

    assert!(
        !access_log_files.is_empty(),
        "Access log file should be created"
    );
}

#[test]
fn test_access_log_entry_creation() {
    let temp_dir = TempDir::new().unwrap();
    let access_log_dir = temp_dir.path().join("access");
    let app_log_dir = temp_dir.path().join("app");

    let config = LoggingConfig {
        access_log_dir,
        app_log_dir,
        access_log_enabled: true,
        access_log_mode: AccessLogMode::All,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let logger = LoggerManager::new(config);

    let entry = logger.create_access_log_entry(
        "GET",
        "192.168.1.1".to_string(),
        "/test-bucket/test-key".to_string(),
        200,
        1024,
        Some(1024),
        100,
        50,
        Some("test-agent".to_string()),
        None,
        Some("test-bucket.s3.amazonaws.com".to_string()),
        None,
    );

    assert_eq!(entry.bucket, "test-bucket");
    assert_eq!(entry.key, "test-key");
    assert_eq!(entry.remote_ip, "192.168.1.1");
    assert_eq!(entry.http_status, 200);
    assert_eq!(entry.bytes_sent, 1024);
    assert_eq!(entry.host_id, "test-host");
}

#[test]
fn test_cached_only_logging_mode() {
    let temp_dir = TempDir::new().unwrap();
    let access_log_dir = temp_dir.path().join("access");
    let app_log_dir = temp_dir.path().join("app");

    let config = LoggingConfig {
        access_log_dir,
        app_log_dir,
        access_log_enabled: true,
        access_log_mode: AccessLogMode::CachedOnly,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let logger = LoggerManager::new(config);

    // Test that cached-only mode is properly configured
    assert_eq!(logger.config.access_log_mode, AccessLogMode::CachedOnly);
}

#[test]
fn test_s3_uri_parsing() {
    let temp_dir = TempDir::new().unwrap();
    let access_log_dir = temp_dir.path().join("access");
    let app_log_dir = temp_dir.path().join("app");

    let config = LoggingConfig {
        access_log_dir,
        app_log_dir,
        access_log_enabled: true,
        access_log_mode: AccessLogMode::All,
        hostname: "test-host".to_string(),
        log_level: "info".to_string(),
        access_log_flush_interval: std::time::Duration::from_secs(5),
        access_log_buffer_size: 1000,
    };

    let logger = LoggerManager::new(config);

    // Test various URI formats
    let (bucket, key) = logger.parse_s3_uri("/test-bucket/path/to/object.txt");
    assert_eq!(bucket, "test-bucket");
    assert_eq!(key, "path/to/object.txt");

    let (bucket, key) = logger.parse_s3_uri("/test-bucket/");
    assert_eq!(bucket, "test-bucket");
    assert_eq!(key, "");

    let (bucket, key) = logger.parse_s3_uri("/test-bucket");
    assert_eq!(bucket, "test-bucket");
    assert_eq!(key, "-");

    let (bucket, key) = logger.parse_s3_uri("/test-bucket/object.txt?versionId=123");
    assert_eq!(bucket, "test-bucket");
    assert_eq!(key, "object.txt");
}

#[tokio::test]
async fn test_log_filename_format_compliance() {
    use chrono::{TimeZone, Utc};
    use s3_proxy::logging::{AccessLogBuffer, AccessLogEntry};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();
    let hostname = "test-host".to_string();

    let buffer = AccessLogBuffer::new(log_dir.clone(), hostname.clone(), None, None);

    // Create a test entry with a specific timestamp: 2024-01-15 14:30:25 UTC
    let test_time = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 25).unwrap();
    let entry = AccessLogEntry {
        bucket_owner: "test-bucket".to_string(),
        bucket: "test-bucket".to_string(),
        time: test_time,
        remote_ip: "192.168.1.1".to_string(),
        requester: "-".to_string(),
        request_id: "test-request-id".to_string(),
        operation: "REST.GET.OBJECT".to_string(),
        key: "test-key".to_string(),
        request_uri: "/test-bucket/test-key".to_string(),
        http_status: 200,
        error_code: None,
        bytes_sent: 1024,
        object_size: Some(1024),
        total_time: 100,
        turn_around_time: 50,
        referer: None,
        user_agent: Some("test-agent".to_string()),
        host_id: "test-host".to_string(),
        signature_version: None,
        cipher_suite: None,
        authentication_type: Some("Anonymous".to_string()),
        host_header: Some("test-bucket.s3.amazonaws.com".to_string()),
        tls_version: None,
        access_point_arn: None,
        acl_required: None,
        source_region: None,
    };

    // Log the entry and flush
    buffer.log(entry).await.unwrap();
    buffer.force_flush().await.unwrap();

    // Verify directory structure: /logs/access/2024/01/15/
    let expected_dir = log_dir.join("2024").join("01").join("15");
    assert!(
        expected_dir.exists(),
        "Date-partitioned directory should exist"
    );

    // Find the log file and verify filename format
    let entries: Vec<_> = std::fs::read_dir(&expected_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(entries.len(), 1, "Should have exactly one log file");

    let log_file = &entries[0];
    let filename = log_file.file_name().to_str().unwrap().to_string();

    // Verify filename format: 2024-01-15-14-30-25-test-host
    let expected_filename = "2024-01-15-14-30-25-test-host";
    assert_eq!(filename, expected_filename);

    // Verify it matches requirement 8.5 format: [YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[hostname]
    let parts: Vec<&str> = filename.splitn(7, '-').collect();
    assert!(parts.len() >= 7); // At least YYYY, MM, DD, hh, mm, ss, and hostname parts
    assert_eq!(parts[0], "2024"); // YYYY
    assert_eq!(parts[1], "01"); // MM
    assert_eq!(parts[2], "15"); // DD
    assert_eq!(parts[3], "14"); // hh (24-hour format)
    assert_eq!(parts[4], "30"); // mm
    assert_eq!(parts[5], "25"); // ss

    // The hostname is everything after the 6th dash (parts[6] and beyond joined by '-')
    let hostname_parts = &parts[6..];
    let reconstructed_hostname = hostname_parts.join("-");
    assert_eq!(reconstructed_hostname, "test-host"); // hostname
}
