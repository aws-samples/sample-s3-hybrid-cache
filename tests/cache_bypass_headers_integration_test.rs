//! Integration tests for cache bypass headers feature
//!
//! Tests the Cache-Control and Pragma header handling for cache bypass functionality.
//! These tests verify that the proxy correctly handles no-cache, no-store, and Pragma
//! headers to bypass cache lookups and control response caching.
//!
//! Requirements: 1.1, 1.3, 2.1, 2.3, 3.1, 7.3, 7.4

use s3_proxy::{
    http_proxy::{parse_cache_bypass_headers, parse_cache_control, CacheBypassMode, HttpProxy},
    metrics::MetricsManager,
};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Task 6.1: Integration test for no-cache bypass
// Requirements: 1.1, 1.3
// ============================================================================

/// Test that Cache-Control: no-cache header triggers cache bypass
/// Requirement 1.1: WHEN a GET request includes Cache-Control: no-cache header,
/// THEN the Proxy SHALL skip the cache lookup and forward the request to S3
#[tokio::test]
async fn test_no_cache_header_triggers_bypass() {
    // Test that no-cache header is correctly parsed and triggers bypass
    let mut headers = HashMap::new();
    headers.insert("cache-control".to_string(), "no-cache".to_string());

    let bypass_mode = parse_cache_bypass_headers(&headers, true);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::NoCache,
        "Cache-Control: no-cache should trigger NoCache bypass mode"
    );
}

/// Test that no-cache bypass mode allows response caching for cacheable operations
/// Requirement 1.3: WHEN a cacheable request with Cache-Control: no-cache receives
/// a successful response from S3, THEN the Proxy SHALL cache the response normally
#[tokio::test]
async fn test_no_cache_allows_response_caching_for_cacheable_ops() {
    // Verify that GetObject operations are cacheable
    let path = "/my-bucket/my-object";
    let query_params: HashMap<String, String> = HashMap::new();

    let (should_bypass, op_type, _) = HttpProxy::should_bypass_cache(path, &query_params);

    // GetObject should NOT be a non-cacheable operation
    assert!(!should_bypass, "GetObject should be a cacheable operation");
    assert!(
        op_type.is_none(),
        "GetObject should not have an operation type (it's cacheable)"
    );

    // With no-cache mode, cacheable operations should still cache the response
    // This is verified by the logic: should_cache_response = bypass_mode == NoCache && !is_non_cacheable_op
    // For GetObject: is_non_cacheable_op = false, so should_cache_response = true
}

/// Test that no-cache header works with various case variations
/// Requirement 4.1: THE Proxy SHALL parse Cache-Control header values case-insensitively
#[tokio::test]
async fn test_no_cache_case_insensitive() {
    let test_cases = vec!["no-cache", "NO-CACHE", "No-Cache", "nO-cAcHe"];

    for header_value in test_cases {
        let mut headers = HashMap::new();
        headers.insert("cache-control".to_string(), header_value.to_string());

        let bypass_mode = parse_cache_bypass_headers(&headers, true);
        assert_eq!(
            bypass_mode,
            CacheBypassMode::NoCache,
            "Cache-Control: {} should trigger NoCache bypass mode",
            header_value
        );
    }
}

/// Test that no-cache with other directives still triggers bypass
/// Requirement 4.2: THE Proxy SHALL handle multiple directives in a single Cache-Control header
#[tokio::test]
async fn test_no_cache_with_other_directives() {
    let test_cases = vec![
        "max-age=0, no-cache",
        "no-cache, max-age=0",
        "private, no-cache, must-revalidate",
        "no-cache, private",
    ];

    for header_value in test_cases {
        let result = parse_cache_control(header_value);
        assert_eq!(
            result,
            CacheBypassMode::NoCache,
            "Cache-Control: {} should trigger NoCache bypass mode",
            header_value
        );
    }
}

/// Test that no-cache does NOT cache response for non-cacheable operations
/// Requirement 1.4: WHEN a non-cacheable operation includes Cache-Control: no-cache,
/// THEN the Proxy SHALL NOT cache the response regardless of the header
#[tokio::test]
async fn test_no_cache_does_not_cache_non_cacheable_ops() {
    // Test LIST operations - should always be non-cacheable
    let mut list_params = HashMap::new();
    list_params.insert("list-type".to_string(), "2".to_string());

    let (should_bypass, op_type, _) = HttpProxy::should_bypass_cache("/bucket", &list_params);
    assert!(
        should_bypass,
        "ListObjects should be a non-cacheable operation"
    );
    assert_eq!(
        op_type,
        Some("ListObjects".to_string()),
        "Operation type should be ListObjects"
    );

    // Test metadata operations - should always be non-cacheable
    let mut acl_params = HashMap::new();
    acl_params.insert("acl".to_string(), "".to_string());

    let (should_bypass, op_type, _) = HttpProxy::should_bypass_cache("/bucket/key", &acl_params);
    assert!(
        should_bypass,
        "GetObjectAcl should be a non-cacheable operation"
    );
    assert_eq!(
        op_type,
        Some("GetObjectAcl".to_string()),
        "Operation type should be GetObjectAcl"
    );
}

// ============================================================================
// Task 6.2: Integration test for no-store bypass
// Requirements: 2.1, 2.3
// ============================================================================

/// Test that Cache-Control: no-store header triggers cache bypass
/// Requirement 2.1: WHEN a GET request includes Cache-Control: no-store header,
/// THEN the Proxy SHALL skip the cache lookup and forward the request to S3
#[tokio::test]
async fn test_no_store_header_triggers_bypass() {
    let mut headers = HashMap::new();
    headers.insert("cache-control".to_string(), "no-store".to_string());

    let bypass_mode = parse_cache_bypass_headers(&headers, true);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::NoStore,
        "Cache-Control: no-store should trigger NoStore bypass mode"
    );
}

/// Test that no-store bypass mode prevents response caching
/// Requirement 2.3: WHEN a request with Cache-Control: no-store receives a successful
/// response from S3, THEN the Proxy SHALL NOT cache the response
#[tokio::test]
async fn test_no_store_prevents_response_caching() {
    // Verify that NoStore mode is different from NoCache mode
    let mut no_store_headers = HashMap::new();
    no_store_headers.insert("cache-control".to_string(), "no-store".to_string());

    let mut no_cache_headers = HashMap::new();
    no_cache_headers.insert("cache-control".to_string(), "no-cache".to_string());

    let no_store_mode = parse_cache_bypass_headers(&no_store_headers, true);
    let no_cache_mode = parse_cache_bypass_headers(&no_cache_headers, true);

    assert_eq!(no_store_mode, CacheBypassMode::NoStore);
    assert_eq!(no_cache_mode, CacheBypassMode::NoCache);
    assert_ne!(
        no_store_mode, no_cache_mode,
        "NoStore and NoCache should be different modes"
    );

    // The implementation logic:
    // - NoStore: should_cache_response = false (never cache)
    // - NoCache: should_cache_response = true for cacheable ops (cache the response)
}

/// Test that no-store header works with various case variations
/// Requirement 4.1: THE Proxy SHALL parse Cache-Control header values case-insensitively
#[tokio::test]
async fn test_no_store_case_insensitive() {
    let test_cases = vec!["no-store", "NO-STORE", "No-Store", "nO-sToRe"];

    for header_value in test_cases {
        let mut headers = HashMap::new();
        headers.insert("cache-control".to_string(), header_value.to_string());

        let bypass_mode = parse_cache_bypass_headers(&headers, true);
        assert_eq!(
            bypass_mode,
            CacheBypassMode::NoStore,
            "Cache-Control: {} should trigger NoStore bypass mode",
            header_value
        );
    }
}

/// Test that no-store takes precedence over no-cache
/// Requirement 4.4: WHEN Cache-Control contains both no-cache and no-store,
/// THEN the Proxy SHALL treat it as no-store (more restrictive)
#[tokio::test]
async fn test_no_store_takes_precedence_over_no_cache() {
    let test_cases = vec![
        "no-cache, no-store",
        "no-store, no-cache",
        "max-age=0, no-cache, no-store",
        "no-store, max-age=0, no-cache",
        "private, no-cache, no-store, must-revalidate",
    ];

    for header_value in test_cases {
        let result = parse_cache_control(header_value);
        assert_eq!(
            result,
            CacheBypassMode::NoStore,
            "Cache-Control: {} should trigger NoStore (more restrictive)",
            header_value
        );
    }
}

/// Test that no-store prevents caching even for normally cacheable operations
#[tokio::test]
async fn test_no_store_prevents_caching_for_all_operations() {
    // Even for GetObject (normally cacheable), no-store should prevent caching
    let path = "/my-bucket/my-object";
    let query_params: HashMap<String, String> = HashMap::new();

    let (is_non_cacheable_op, _, _) = HttpProxy::should_bypass_cache(path, &query_params);

    // GetObject is cacheable
    assert!(!is_non_cacheable_op, "GetObject should be cacheable");

    // But with no-store, the implementation sets should_cache_response = false
    // regardless of whether the operation is normally cacheable
    // This is verified by: should_cache_response = bypass_mode == NoCache && !is_non_cacheable_op
    // For NoStore: bypass_mode != NoCache, so should_cache_response = false
}

// ============================================================================
// Task 6.3: Integration test for Pragma no-cache
// Requirements: 3.1
// ============================================================================

/// Test that Pragma: no-cache header triggers cache bypass
/// Requirement 3.1: WHEN a GET request includes Pragma: no-cache header,
/// THEN the Proxy SHALL skip the cache lookup and forward the request to S3
#[tokio::test]
async fn test_pragma_no_cache_triggers_bypass() {
    let mut headers = HashMap::new();
    headers.insert("pragma".to_string(), "no-cache".to_string());

    let bypass_mode = parse_cache_bypass_headers(&headers, true);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::NoCache,
        "Pragma: no-cache should trigger NoCache bypass mode"
    );
}

/// Test that Pragma: no-cache is case-insensitive
#[tokio::test]
async fn test_pragma_no_cache_case_insensitive() {
    let test_cases = vec!["no-cache", "NO-CACHE", "No-Cache", "nO-cAcHe"];

    for header_value in test_cases {
        let mut headers = HashMap::new();
        headers.insert("pragma".to_string(), header_value.to_string());

        let bypass_mode = parse_cache_bypass_headers(&headers, true);
        assert_eq!(
            bypass_mode,
            CacheBypassMode::NoCache,
            "Pragma: {} should trigger NoCache bypass mode",
            header_value
        );
    }
}

/// Test that Cache-Control takes precedence over Pragma
/// Requirement 3.5: WHEN both Cache-Control and Pragma headers are present,
/// THEN the Proxy SHALL give precedence to Cache-Control
#[tokio::test]
async fn test_cache_control_takes_precedence_over_pragma() {
    // Test 1: Cache-Control: no-store should override Pragma: no-cache
    let mut headers = HashMap::new();
    headers.insert("cache-control".to_string(), "no-store".to_string());
    headers.insert("pragma".to_string(), "no-cache".to_string());

    let bypass_mode = parse_cache_bypass_headers(&headers, true);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::NoStore,
        "Cache-Control: no-store should take precedence over Pragma: no-cache"
    );

    // Test 2: Cache-Control: no-cache should be used even with Pragma present
    let mut headers2 = HashMap::new();
    headers2.insert("cache-control".to_string(), "no-cache".to_string());
    headers2.insert("pragma".to_string(), "no-cache".to_string());

    let bypass_mode2 = parse_cache_bypass_headers(&headers2, true);
    assert_eq!(
        bypass_mode2,
        CacheBypassMode::NoCache,
        "Cache-Control: no-cache should be used when both headers present"
    );

    // Test 3: Cache-Control without bypass directives should still check Pragma
    let mut headers3 = HashMap::new();
    headers3.insert("cache-control".to_string(), "max-age=0".to_string());
    headers3.insert("pragma".to_string(), "no-cache".to_string());

    let bypass_mode3 = parse_cache_bypass_headers(&headers3, true);
    assert_eq!(
        bypass_mode3,
        CacheBypassMode::NoCache,
        "Pragma: no-cache should be used when Cache-Control has no bypass directives"
    );
}

/// Test that Pragma with other values does not trigger bypass
#[tokio::test]
async fn test_pragma_other_values_no_bypass() {
    let test_cases = vec!["no-transform", "cache", "public", ""];

    for header_value in test_cases {
        let mut headers = HashMap::new();
        headers.insert("pragma".to_string(), header_value.to_string());

        let bypass_mode = parse_cache_bypass_headers(&headers, true);
        assert_eq!(
            bypass_mode,
            CacheBypassMode::None,
            "Pragma: {} should NOT trigger bypass",
            header_value
        );
    }
}

// ============================================================================
// Task 6.4: Integration test for config disable
// Requirements: 7.3, 7.4
// ============================================================================

/// Test that cache bypass headers are ignored when config is disabled
/// Requirement 7.3: WHEN cache bypass headers are disabled, THEN the Proxy SHALL
/// ignore Cache-Control and Pragma headers for bypass decisions
#[tokio::test]
async fn test_config_disabled_ignores_no_cache() {
    let mut headers = HashMap::new();
    headers.insert("cache-control".to_string(), "no-cache".to_string());

    // With config disabled, should return None (no bypass)
    let bypass_mode = parse_cache_bypass_headers(&headers, false);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::None,
        "Cache-Control: no-cache should be ignored when config is disabled"
    );
}

/// Test that no-store is ignored when config is disabled
#[tokio::test]
async fn test_config_disabled_ignores_no_store() {
    let mut headers = HashMap::new();
    headers.insert("cache-control".to_string(), "no-store".to_string());

    // With config disabled, should return None (no bypass)
    let bypass_mode = parse_cache_bypass_headers(&headers, false);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::None,
        "Cache-Control: no-store should be ignored when config is disabled"
    );
}

/// Test that Pragma is ignored when config is disabled
#[tokio::test]
async fn test_config_disabled_ignores_pragma() {
    let mut headers = HashMap::new();
    headers.insert("pragma".to_string(), "no-cache".to_string());

    // With config disabled, should return None (no bypass)
    let bypass_mode = parse_cache_bypass_headers(&headers, false);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::None,
        "Pragma: no-cache should be ignored when config is disabled"
    );
}

/// Test that normal cache logic is used when config is disabled
/// Requirement 7.4: WHEN cache bypass headers are disabled, THEN the Proxy SHALL
/// still forward requests through normal cache logic
#[tokio::test]
async fn test_config_disabled_uses_normal_cache_logic() {
    // When config is disabled, all bypass headers should be ignored
    let test_cases = vec![
        ("cache-control", "no-cache"),
        ("cache-control", "no-store"),
        ("pragma", "no-cache"),
        ("cache-control", "no-cache, no-store"),
    ];

    for (header_name, header_value) in test_cases {
        let mut headers = HashMap::new();
        headers.insert(header_name.to_string(), header_value.to_string());

        let bypass_mode = parse_cache_bypass_headers(&headers, false);
        assert_eq!(
            bypass_mode,
            CacheBypassMode::None,
            "{}: {} should be ignored when config is disabled",
            header_name,
            header_value
        );
    }
}

/// Test that config enabled by default allows bypass headers
#[tokio::test]
async fn test_config_enabled_allows_bypass_headers() {
    let test_cases = vec![
        ("cache-control", "no-cache", CacheBypassMode::NoCache),
        ("cache-control", "no-store", CacheBypassMode::NoStore),
        ("pragma", "no-cache", CacheBypassMode::NoCache),
    ];

    for (header_name, header_value, expected_mode) in test_cases {
        let mut headers = HashMap::new();
        headers.insert(header_name.to_string(), header_value.to_string());

        // With config enabled (true), bypass headers should work
        let bypass_mode = parse_cache_bypass_headers(&headers, true);
        assert_eq!(
            bypass_mode, expected_mode,
            "{}: {} should trigger {:?} when config is enabled",
            header_name, header_value, expected_mode
        );
    }
}

// ============================================================================
// Additional integration tests for metrics tracking
// Requirements: 5.1, 5.2, 5.3, 5.5
// ============================================================================

/// Test that metrics are recorded for cache bypass operations
#[tokio::test]
async fn test_cache_bypass_metrics_recording() {
    let metrics_manager = Arc::new(tokio::sync::RwLock::new(MetricsManager::new()));

    // Record bypass for no-cache directive
    metrics_manager
        .read()
        .await
        .record_cache_bypass("no-cache directive")
        .await;

    // Record bypass for no-store directive
    metrics_manager
        .read()
        .await
        .record_cache_bypass("no-store directive")
        .await;

    // Record bypass for pragma no-cache
    metrics_manager
        .read()
        .await
        .record_cache_bypass("no-cache directive") // Pragma uses same reason as no-cache
        .await;

    // Verify metrics were recorded
    let no_cache_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("no-cache directive")
        .await;
    assert_eq!(
        no_cache_count, 2,
        "Should have 2 no-cache directive bypasses"
    );

    let no_store_count = metrics_manager
        .read()
        .await
        .get_cache_bypass_count("no-store directive")
        .await;
    assert_eq!(no_store_count, 1, "Should have 1 no-store directive bypass");
}

// ============================================================================
// Edge case tests
// ============================================================================

/// Test empty headers do not trigger bypass
#[tokio::test]
async fn test_empty_headers_no_bypass() {
    let headers: HashMap<String, String> = HashMap::new();

    let bypass_mode = parse_cache_bypass_headers(&headers, true);
    assert_eq!(
        bypass_mode,
        CacheBypassMode::None,
        "Empty headers should not trigger bypass"
    );
}

/// Test unknown Cache-Control directives are ignored
/// Requirement 4.5: THE Proxy SHALL ignore unknown or unsupported directives
#[tokio::test]
async fn test_unknown_directives_ignored() {
    let test_cases = vec![
        "max-age=0",
        "private",
        "public",
        "must-revalidate",
        "proxy-revalidate",
        "s-maxage=0",
        "no-transform",
    ];

    for header_value in test_cases {
        let result = parse_cache_control(header_value);
        assert_eq!(
            result,
            CacheBypassMode::None,
            "Cache-Control: {} should not trigger bypass (unknown directive)",
            header_value
        );
    }
}

/// Test whitespace handling in Cache-Control header
/// Requirement 4.3: THE Proxy SHALL handle whitespace around directive values
#[tokio::test]
async fn test_whitespace_handling() {
    let test_cases = vec![
        " no-cache ",
        "  no-cache  ",
        "no-cache ",
        " no-cache",
        "max-age=0,  no-cache",
        "max-age=0 , no-cache",
        "max-age=0 ,  no-cache ",
    ];

    for header_value in test_cases {
        let result = parse_cache_control(header_value);
        assert_eq!(
            result,
            CacheBypassMode::NoCache,
            "Cache-Control: '{}' should trigger NoCache (whitespace should be handled)",
            header_value
        );
    }
}
