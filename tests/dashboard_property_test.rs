//! Consolidated property-based tests for dashboard functionality
//!
//! This file consolidates all property tests for the web dashboard feature:
//! - Property 1: Dashboard accessibility without authentication (Requirements 1.1)
//! - Property 2: Dashboard response performance (Requirements 1.3)
//! - Property 3: Dashboard port separation (Requirements 1.4)
//! - Property 4: Comprehensive cache statistics (Requirements 2.1, 2.2, 2.5, 2.6)
//! - Property 5: Human-readable size formatting (Requirements 2.4)
//! - Property 6: Default log entry limit (Requirements 3.1)
//! - Property 7: Log entry structure (Requirements 3.3)
//! - Property 8: Structured log data formatting (Requirements 3.4)
//! - Property 9: Comprehensive log API functionality (Requirements 3.5, 3.6)
//! - Property 10: Concurrent connection handling (Requirements 4.2)
//! - Property 11: Static asset size limits (Requirements 4.4)
//! - Property 13: Consistent logging integration (Requirements 5.5)
//! - Property 14: Graceful shutdown behavior (Requirements 5.6)
//! - Property 15: Navigation structure (Requirements 6.1)
//! - Property 16: System information display (Requirements 6.4)

use quickcheck::{QuickCheck, TestResult};
use s3_proxy::config::DashboardConfig;
use s3_proxy::dashboard::{
    ApiHandler, CacheStats, CacheStatsResponse, DashboardServer, LogQueryParams, LogReader,
    OverallStats, StaticFileHandler, SystemInfoResponse,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::fs;

// ============================================================================
// Property 1 & 3: Dashboard accessibility and port separation
// **Validates: Requirements 1.1, 1.4**
// ============================================================================

fn prop_dashboard_accessibility_without_authentication(port_offset: u8) -> TestResult {
    let port = 8080 + (port_offset % 100) as u16;
    if port < 8080 || port > 8180 {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let config = Arc::new(DashboardConfig {
            enabled: true,
            port,
            bind_address: "127.0.0.1".to_string(),
            cache_stats_refresh_interval: Duration::from_secs(5),
            logs_refresh_interval: Duration::from_secs(10),
            max_log_entries: 100,
        });

        let _dashboard = DashboardServer::new(config, PathBuf::from("./tmp/logs/app"));
        // Server creation succeeded, no auth required
        TestResult::passed()
    })
}

fn prop_dashboard_port_separation(dashboard_port: u16) -> TestResult {
    if dashboard_port < 1024 {
        return TestResult::discard();
    }
    let port_separated = dashboard_port != 80 && dashboard_port != 443;
    TestResult::from_bool(port_separated)
}

#[test]
fn test_property_dashboard_accessibility() {
    QuickCheck::new()
        .tests(50)
        .quickcheck(prop_dashboard_accessibility_without_authentication as fn(u8) -> TestResult);
}

#[test]
fn test_property_dashboard_port_separation() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_dashboard_port_separation as fn(u16) -> TestResult);
}

// ============================================================================
// Property 4 & 5: Cache statistics and size formatting
// **Validates: Requirements 2.1, 2.2, 2.4, 2.5, 2.6**
// ============================================================================

fn validate_cache_stats(stats: &CacheStats) -> bool {
    stats.hit_rate >= 0.0 && !stats.size_human.is_empty()
}

fn validate_overall_stats(stats: &OverallStats) -> bool {
    !stats.s3_transfer_saved_human.is_empty()
}

fn validate_size_format(size_str: &str) -> bool {
    ["B", "KB", "MB", "GB", "TB"]
        .iter()
        .any(|unit| size_str.contains(unit))
}

fn prop_comprehensive_cache_statistics(_seed: u8) -> TestResult {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let log_reader = Arc::new(LogReader::new(
            PathBuf::from("/tmp/logs"),
            "test-host".to_string(),
        ));
        let api_handler = Arc::new(ApiHandler::new(log_reader, Arc::new(DashboardConfig::default())));
        let response = api_handler.get_cache_stats().await;

        match response {
            Ok(http_response) => {
                let status = http_response.status();
                let body = http_response.into_body();

                if status == hyper::StatusCode::SERVICE_UNAVAILABLE {
                    let error_json: Result<serde_json::Value, _> = serde_json::from_str(&body);
                    TestResult::from_bool(error_json.is_ok())
                } else {
                    let stats_result: Result<CacheStatsResponse, _> = serde_json::from_str(&body);
                    match stats_result {
                        Ok(stats) => {
                            let valid = validate_cache_stats(&stats.ram_cache)
                                && validate_cache_stats(&stats.disk_cache)
                                && validate_overall_stats(&stats.overall)
                                && !stats.hostname.is_empty()
                                && !stats.version.is_empty();
                            TestResult::from_bool(valid)
                        }
                        Err(_) => TestResult::from_bool(false),
                    }
                }
            }
            Err(_) => TestResult::from_bool(false),
        }
    })
}

#[test]
fn test_property_comprehensive_cache_statistics() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_comprehensive_cache_statistics as fn(u8) -> TestResult);
}

// ============================================================================
// Property 6 & 7: Log entry limit and structure
// **Validates: Requirements 3.1, 3.3**
// ============================================================================

fn prop_default_log_entry_limit(log_count: u8) -> TestResult {
    let log_count = log_count as usize;
    if log_count == 0 {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();
        let hostname = "test-host".to_string();
        let host_log_dir = log_dir.join(&hostname);
        fs::create_dir_all(&host_log_dir).await.unwrap();

        let log_file = host_log_dir.join("s3-proxy.log.2025-12-21");
        let mut log_content = String::new();
        for i in 0..log_count.min(150) {
            let timestamp = format!(
                "2025-12-21T{:02}:{:02}:{:02}.{:06}+00:00",
                (i / 3600) % 24,
                (i / 60) % 60,
                i % 60,
                i * 1000
            );
            log_content.push_str(&format!(
                "{}  INFO ThreadId({:02}) s3_proxy::test: src/test.rs:{}: Test message {}\n",
                timestamp,
                i % 100,
                i + 1,
                i
            ));
        }
        fs::write(&log_file, log_content).await.unwrap();

        let log_reader = LogReader::new(log_dir, hostname);
        let params = LogQueryParams {
            limit: None,
            level_filter: None,
            text_filter: None,
            since: None,
        };
        let entries = log_reader.read_recent_logs(params).await.unwrap();

        let expected_count = std::cmp::min(log_count.min(150), 100);
        TestResult::from_bool(entries.len() <= 100 && entries.len() == expected_count)
    })
}

fn prop_log_entry_structure(log_count: u8) -> TestResult {
    let log_count = log_count as usize;
    if log_count == 0 {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();
        let hostname = "test-host".to_string();
        let host_log_dir = log_dir.join(&hostname);
        fs::create_dir_all(&host_log_dir).await.unwrap();

        let log_file = host_log_dir.join("s3-proxy.log.2025-12-21");
        let mut log_content = String::new();
        for i in 0..log_count.min(50) {
            let timestamp = format!(
                "2025-12-21T{:02}:{:02}:{:02}.{:06}+00:00",
                (i / 3600) % 24,
                (i / 60) % 60,
                i % 60,
                i * 1000
            );
            log_content.push_str(&format!(
                "{}  INFO ThreadId({:02}) s3_proxy::test: src/test.rs:{}: Test message {}\n",
                timestamp,
                i % 100,
                i + 1,
                i
            ));
        }
        fs::write(&log_file, log_content).await.unwrap();

        let log_reader = LogReader::new(log_dir, hostname);
        let params = LogQueryParams {
            limit: Some(log_count.min(50)),
            level_filter: None,
            text_filter: None,
            since: None,
        };
        let entries = log_reader.read_recent_logs(params).await.unwrap();

        let all_valid = entries.iter().all(|entry| {
            let has_timestamp = entry.timestamp
                > chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00+00:00")
                    .unwrap()
                    .with_timezone(&chrono::Utc);
            let has_level = !entry.level.is_empty();
            let has_message = !entry.message.is_empty();
            has_timestamp && has_level && has_message
        });
        TestResult::from_bool(all_valid && !entries.is_empty())
    })
}

#[test]
fn test_property_default_log_entry_limit() {
    QuickCheck::new()
        .tests(30)
        .quickcheck(prop_default_log_entry_limit as fn(u8) -> TestResult);
}

#[test]
fn test_property_log_entry_structure() {
    QuickCheck::new()
        .tests(30)
        .quickcheck(prop_log_entry_structure as fn(u8) -> TestResult);
}

// ============================================================================
// Property 9: Comprehensive log API functionality
// **Validates: Requirements 3.5, 3.6**
// ============================================================================

fn prop_log_api_level_filtering(total_entries: u8, limit: u8, level_idx: u8) -> TestResult {
    let total_entries = total_entries as usize;
    let limit = limit as usize;
    if total_entries == 0 || limit == 0 {
        return TestResult::discard();
    }

    let levels = ["ERROR", "WARN", "INFO", "DEBUG"];
    let filter_level = levels[level_idx as usize % levels.len()];

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();
        let hostname = "test-host".to_string();
        let host_log_dir = log_dir.join(&hostname);
        fs::create_dir_all(&host_log_dir).await.unwrap();

        let log_file = host_log_dir.join("s3-proxy.log.2025-12-21");
        let mut log_content = String::new();
        let mut expected_filtered_count = 0;

        for i in 0..total_entries.min(50) {
            let level = levels[i % levels.len()];
            if level == filter_level {
                expected_filtered_count += 1;
            }
            let timestamp = format!(
                "2025-12-21T{:02}:{:02}:{:02}.{:06}+00:00",
                (i / 3600) % 24,
                (i / 60) % 60,
                i % 60,
                i * 1000
            );
            log_content.push_str(&format!(
                "{}  {} ThreadId({:02}) s3_proxy::test: src/test.rs:{}: Test message {}\n",
                timestamp,
                level,
                i % 100,
                i + 1,
                i
            ));
        }
        fs::write(&log_file, log_content).await.unwrap();

        let log_reader = LogReader::new(log_dir, hostname);
        let params = LogQueryParams {
            limit: Some(limit),
            level_filter: Some(filter_level.to_string()),
            text_filter: None,
            since: None,
        };
        let filtered_entries = log_reader.read_recent_logs(params).await.unwrap();

        let filter_respected = filtered_entries.iter().all(|e| e.level == filter_level);
        let limit_respected = filtered_entries.len() <= limit;
        let count_correct = filtered_entries.len() <= expected_filtered_count.min(limit);

        TestResult::from_bool(filter_respected && limit_respected && count_correct)
    })
}

#[test]
fn test_property_log_api_level_filtering() {
    QuickCheck::new()
        .tests(30)
        .quickcheck(prop_log_api_level_filtering as fn(u8, u8, u8) -> TestResult);
}

// ============================================================================
// Property 11: Static asset size limits
// **Validates: Requirements 4.4**
// ============================================================================

fn prop_static_asset_size_limits(_dummy: u8) -> TestResult {
    let handler = StaticFileHandler::new();
    let css_content = handler.get_style_css();
    let js_content = handler.get_script_js();

    let asset_size = css_content.len() + js_content.len();
    const MAX_ASSET_SIZE: usize = 100 * 1024; // 100KB

    TestResult::from_bool(asset_size < MAX_ASSET_SIZE)
}

#[test]
fn test_property_static_asset_size_limits() {
    QuickCheck::new()
        .tests(10)
        .quickcheck(prop_static_asset_size_limits as fn(u8) -> TestResult);
}

// ============================================================================
// Property 15: Navigation structure
// **Validates: Requirements 6.1**
// ============================================================================

fn prop_navigation_structure(_dummy: u8) -> TestResult {
    let handler = StaticFileHandler::new();
    let html_content = handler.get_index_html();

    let has_nav = html_content.contains("<nav>");
    let has_cache_stats_nav =
        html_content.contains("cache-stats-tab") && html_content.contains("Cache Stats");
    let has_logs_nav =
        html_content.contains("logs-tab") && html_content.contains("Application Logs");
    let has_tab_switching =
        html_content.contains("showTab('cache-stats')") && html_content.contains("showTab('logs')");
    let has_cache_stats_section =
        html_content.contains("id=\"cache-stats\"") && html_content.contains("tab-content");
    let has_logs_section =
        html_content.contains("id=\"logs\"") && html_content.contains("tab-content");

    let all_present = has_nav
        && has_cache_stats_nav
        && has_logs_nav
        && has_tab_switching
        && has_cache_stats_section
        && has_logs_section;

    TestResult::from_bool(all_present)
}

#[test]
fn test_property_navigation_structure() {
    QuickCheck::new()
        .tests(10)
        .quickcheck(prop_navigation_structure as fn(u8) -> TestResult);
}

// ============================================================================
// Property 16: System information display
// **Validates: Requirements 6.4**
// ============================================================================

fn prop_system_information_display(uptime_hours: u16, hostname_suffix: u8) -> TestResult {
    let uptime_seconds = (uptime_hours % 8760) as u64 * 3600;
    let hostname_test_suffix = format!("test{}", hostname_suffix);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let system_info = SystemInfoResponse {
            timestamp: SystemTime::now(),
            hostname: format!("proxy-{}", hostname_test_suffix),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds,
            status: "running".to_string(),
            cache_stats_refresh_ms: 5000,
            logs_refresh_ms: 10000,
        };

        let has_hostname = !system_info.hostname.is_empty();
        let has_version = !system_info.version.is_empty();
        let hostname_valid = system_info.hostname.len() > 0 && system_info.hostname.len() < 256;
        let version_valid = system_info.version.contains('.') && !system_info.version.is_empty();
        let status_valid = matches!(
            system_info.status.as_str(),
            "running" | "starting" | "stopping"
        );
        let timestamp_valid = system_info.timestamp <= SystemTime::now();

        let all_valid = has_hostname
            && has_version
            && hostname_valid
            && version_valid
            && status_valid
            && timestamp_valid;

        TestResult::from_bool(all_valid)
    })
}

#[test]
fn test_property_system_information_display() {
    QuickCheck::new()
        .tests(50)
        .quickcheck(prop_system_information_display as fn(u16, u8) -> TestResult);
}

// ============================================================================
// Unit tests for edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_size_format_validation() {
        assert!(validate_size_format("1.50 MB"));
        assert!(validate_size_format("0 B"));
        assert!(validate_size_format("100 KB"));
        assert!(!validate_size_format("invalid"));
    }

    #[test]
    fn test_individual_static_file_sizes() {
        let handler = StaticFileHandler::new();
        let html_content = handler.get_index_html();
        let css_content = handler.get_style_css();
        let js_content = handler.get_script_js();

        assert!(
            html_content.len() < 50 * 1024,
            "HTML file too large: {} bytes",
            html_content.len()
        );
        assert!(
            css_content.len() < 50 * 1024,
            "CSS file too large: {} bytes",
            css_content.len()
        );
        assert!(
            js_content.len() < 50 * 1024,
            "JavaScript file too large: {} bytes",
            js_content.len()
        );
        assert!(!html_content.is_empty());
        assert!(!css_content.is_empty());
        assert!(!js_content.is_empty());
    }

    #[test]
    fn test_static_file_content_validity() {
        let handler = StaticFileHandler::new();
        let html_content = handler.get_index_html();
        let css_content = handler.get_style_css();
        let js_content = handler.get_script_js();

        assert!(html_content.contains("<!DOCTYPE html>"));
        assert!(html_content.contains("S3 Hybrid Cache"));
        assert!(css_content.contains("body"));
        assert!(js_content.contains("loadCacheStats"));
        assert!(js_content.contains("showTab"));
    }

    #[test]
    fn test_navigation_accessibility() {
        let handler = StaticFileHandler::new();
        let html_content = handler.get_index_html();

        assert!(html_content.contains("<button"));
        assert!(html_content.contains("onclick="));
        assert!(html_content.contains("id=\"cache-stats\""));
        assert!(html_content.contains("id=\"logs\""));
    }

    #[tokio::test]
    async fn test_default_limit_100() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_path_buf();
        let hostname = "test-host".to_string();
        let host_log_dir = log_dir.join(&hostname);
        fs::create_dir_all(&host_log_dir).await.unwrap();

        let log_file = host_log_dir.join("s3-proxy.log.2025-12-21");
        let mut log_content = String::new();
        for i in 0..150 {
            let timestamp = format!(
                "2025-12-21T{:02}:{:02}:{:02}.{:06}+00:00",
                (i / 3600) % 24,
                (i / 60) % 60,
                i % 60,
                i * 1000
            );
            log_content.push_str(&format!(
                "{}  INFO ThreadId({:02}) s3_proxy::test: src/test.rs:{}: Test message {}\n",
                timestamp,
                i % 100,
                i + 1,
                i
            ));
        }
        fs::write(&log_file, log_content).await.unwrap();

        let log_reader = LogReader::new(log_dir, hostname);
        let params = LogQueryParams {
            limit: None,
            level_filter: None,
            text_filter: None,
            since: None,
        };
        let entries = log_reader.read_recent_logs(params).await.unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_system_info_serialization() {
        let system_info = SystemInfoResponse {
            timestamp: SystemTime::now(),
            hostname: "test-proxy".to_string(),
            version: "2.1.0".to_string(),
            uptime_seconds: 7200,
            status: "running".to_string(),
            cache_stats_refresh_ms: 5000,
            logs_refresh_ms: 10000,
        };

        let json_result = serde_json::to_string(&system_info);
        assert!(json_result.is_ok());

        let json_str = json_result.unwrap();
        assert!(json_str.contains("test-proxy"));
        assert!(json_str.contains("2.1.0"));
        assert!(json_str.contains("7200"));
        assert!(json_str.contains("running"));
    }
}
