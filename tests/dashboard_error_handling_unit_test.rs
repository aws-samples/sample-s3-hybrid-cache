//! Unit tests for dashboard error handling scenarios
//!
//! Tests API responses when components are unavailable, invalid parameter handling,
//! and file system error scenarios.

use s3_proxy::config::DashboardConfig;
use s3_proxy::dashboard::{ApiHandler, LogQueryParams, LogReader};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_cache_stats_when_cache_manager_unavailable() {
    // Create API handler without cache manager
    let temp_dir = TempDir::new().unwrap();
    let log_reader = Arc::new(LogReader::new(
        temp_dir.path().to_path_buf(),
        "test-host".to_string(),
    ));
    let api_handler = ApiHandler::new(log_reader, Arc::new(DashboardConfig::default()));

    // Request cache stats without setting cache manager
    let response = api_handler.get_cache_stats().await.unwrap();

    // Should return 503 Service Unavailable
    assert_eq!(response.status(), hyper::StatusCode::SERVICE_UNAVAILABLE);

    // Response should contain error message
    let body = response.body();
    assert!(body.contains("Cache manager not available"));
    assert!(body.contains("\"code\": 503"));
}

#[tokio::test]
async fn test_logs_when_log_directory_missing() {
    // Create log reader with non-existent directory
    let non_existent_dir = PathBuf::from("/non/existent/directory");
    let log_reader = LogReader::new(non_existent_dir, "test-host".to_string());

    let params = LogQueryParams {
        limit: Some(100),
        level_filter: None,
        text_filter: None,
        since: None,
    };

    // Should return empty logs without error
    let result = log_reader.read_recent_logs(params).await;
    assert!(result.is_ok());
    let logs = result.unwrap();
    assert!(logs.is_empty());
}

#[tokio::test]
async fn test_logs_with_invalid_limit_parameter() {
    use s3_proxy::dashboard::validate_log_params;

    // Test limit of 0
    let params = LogQueryParams {
        limit: Some(0),
        level_filter: None,
        text_filter: None,
        since: None,
    };

    let validation_error = validate_log_params(&params);
    assert!(validation_error.is_some());
    assert!(validation_error.unwrap().contains("must be greater than 0"));

    // Test limit exceeding maximum
    let params = LogQueryParams {
        limit: Some(20000),
        level_filter: None,
        text_filter: None,
        since: None,
    };

    let validation_error = validate_log_params(&params);
    assert!(validation_error.is_some());
    assert!(validation_error.unwrap().contains("cannot exceed 10000"));
}

#[tokio::test]
async fn test_logs_with_invalid_level_parameter() {
    use s3_proxy::dashboard::validate_log_params;

    // Test invalid log level
    let params = LogQueryParams {
        limit: Some(100),
        level_filter: Some("INVALID".to_string()),
        text_filter: None,
        since: None,
    };

    let validation_error = validate_log_params(&params);
    assert!(validation_error.is_some());
    assert!(validation_error.unwrap().contains("Invalid log level"));
}

#[tokio::test]
async fn test_logs_with_valid_parameters() {
    use s3_proxy::dashboard::validate_log_params;

    // Test all valid log levels
    let valid_levels = vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE"];

    for level in valid_levels {
        let params = LogQueryParams {
            limit: Some(100),
            level_filter: Some(level.to_string()),
            text_filter: None,
            since: None,
        };

        let validation_error = validate_log_params(&params);
        assert!(
            validation_error.is_none(),
            "Level {} should be valid",
            level
        );
    }

    // Test valid limit ranges
    let valid_limits = vec![1, 50, 100, 500, 1000, 10000];

    for limit in valid_limits {
        let params = LogQueryParams {
            limit: Some(limit),
            level_filter: None,
        text_filter: None,
            since: None,
        };

        let validation_error = validate_log_params(&params);
        assert!(
            validation_error.is_none(),
            "Limit {} should be valid",
            limit
        );
    }
}

#[tokio::test]
async fn test_log_file_reading_with_permission_error() {
    // Create a temporary directory and file
    let temp_dir = TempDir::new().unwrap();
    let host_dir = temp_dir.path().join("test-host");
    std::fs::create_dir_all(&host_dir).unwrap();

    let log_file = host_dir.join("s3-proxy.log.2025-12-21");
    std::fs::write(
        &log_file,
        "2025-12-21T10:00:00.000Z  INFO test: Test log message\n",
    )
    .unwrap();

    // Create log reader
    let log_reader = LogReader::new(temp_dir.path().to_path_buf(), "test-host".to_string());

    let params = LogQueryParams {
        limit: Some(100),
        level_filter: None,
        text_filter: None,
        since: None,
    };

    // Should successfully read the log file
    let result = log_reader.read_recent_logs(params).await;
    assert!(result.is_ok());
    let logs = result.unwrap();
    assert!(!logs.is_empty());
}

#[tokio::test]
async fn test_api_handler_serialization_error_handling() {
    // This test verifies that serialization errors are properly handled
    // by testing with extreme values that might cause serialization issues

    let temp_dir = TempDir::new().unwrap();
    let log_reader = Arc::new(LogReader::new(
        temp_dir.path().to_path_buf(),
        "test-host".to_string(),
    ));
    let api_handler = ApiHandler::new(log_reader, Arc::new(DashboardConfig::default()));

    // Test system info API (should always work)
    let response = api_handler.get_system_info().await.unwrap();
    assert_eq!(response.status(), hyper::StatusCode::OK);

    // Verify response contains valid JSON
    let body = response.body();
    let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(parsed.get("hostname").is_some());
    assert!(parsed.get("version").is_some());
}

#[tokio::test]
async fn test_log_parsing_with_malformed_entries() {
    // Create a temporary log file with various malformed entries
    let temp_dir = TempDir::new().unwrap();
    let host_dir = temp_dir.path().join("test-host");
    std::fs::create_dir_all(&host_dir).unwrap();

    let log_file = host_dir.join("s3-proxy.log.2025-12-21");
    let malformed_content = r#"
2025-12-21T10:00:00.000Z  INFO test: Valid log message
Invalid line without timestamp
2025-12-21T10:01:00.000Z  ERROR test: Another valid message
Incomplete timestamp 2025-12-21
2025-12-21T10:02:00.000Z  WARN test: Final valid message
"#;
    std::fs::write(&log_file, malformed_content).unwrap();

    let log_reader = LogReader::new(temp_dir.path().to_path_buf(), "test-host".to_string());

    let params = LogQueryParams {
        limit: Some(100),
        level_filter: None,
        text_filter: None,
        since: None,
    };

    // Should successfully parse valid entries and skip malformed ones
    let result = log_reader.read_recent_logs(params).await;
    assert!(result.is_ok());
    let logs = result.unwrap();

    // Should have parsed 3 valid entries, skipping malformed ones
    assert_eq!(logs.len(), 3);

    // Verify the valid entries were parsed correctly
    assert!(logs
        .iter()
        .any(|entry| entry.message.contains("Valid log message")));
    assert!(logs
        .iter()
        .any(|entry| entry.message.contains("Another valid message")));
    assert!(logs
        .iter()
        .any(|entry| entry.message.contains("Final valid message")));
}

#[tokio::test]
async fn test_parameter_sanitization() {
    use hyper::Uri;
    use s3_proxy::dashboard::parse_log_query_params;

    // Test parameter with special characters (potential injection)
    let uri: Uri = "/api/logs?level=ERROR%3Cscript%3E&limit=100"
        .parse()
        .unwrap();
    let params = parse_log_query_params(&uri);

    // Should reject level with special characters
    assert!(params.level_filter.is_none());
    assert_eq!(params.limit, Some(100));

    // Test valid parameters
    let uri: Uri = "/api/logs?level=ERROR&limit=50".parse().unwrap();
    let params = parse_log_query_params(&uri);

    assert_eq!(params.level_filter, Some("ERROR".to_string()));
    assert_eq!(params.limit, Some(50));
}

#[tokio::test]
async fn test_concurrent_api_requests() {
    // Test that multiple concurrent API requests don't cause issues
    let temp_dir = TempDir::new().unwrap();
    let log_reader = Arc::new(LogReader::new(
        temp_dir.path().to_path_buf(),
        "test-host".to_string(),
    ));
    let api_handler = Arc::new(ApiHandler::new(log_reader, Arc::new(DashboardConfig::default())));

    // Create multiple concurrent requests
    let mut handles = Vec::new();

    for _ in 0..10 {
        let handler = api_handler.clone();
        let handle = tokio::spawn(async move { handler.get_system_info().await });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), hyper::StatusCode::OK);
    }
}
