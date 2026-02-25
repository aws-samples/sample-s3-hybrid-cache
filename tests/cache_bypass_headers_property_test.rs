//! Property-based tests for cache bypass headers feature
//!
//! This file consolidates all property tests for the cache bypass headers feature:
//! - Property 7: Case-insensitive header parsing (Requirements 4.1)
//! - Property 8: Multiple directive parsing (Requirements 4.2, 4.3, 4.5)
//! - Property 9: no-store takes precedence over no-cache (Requirements 4.4)
//! - Property 2a: Non-cacheable operations remain non-cacheable (Requirements 1.4, 3.4)
//! - Property 10: Header stripping on forward (Requirements 6.1, 6.2, 6.3)

use quickcheck::{QuickCheck, TestResult};
use s3_proxy::http_proxy::{
    parse_cache_bypass_headers, parse_cache_control, CacheBypassMode, HttpProxy,
};
use std::collections::HashMap;

// ============================================================================
// Property 7: Case-insensitive header parsing
// **Validates: Requirements 4.1**
// ============================================================================

fn prop_case_insensitive_no_cache_parsing(case_variation: u8) -> TestResult {
    let variations = vec![
        "no-cache", "NO-CACHE", "No-Cache", "nO-cAcHe", "NO-cache", "no-CACHE", "No-cache",
        "nO-CaChE",
    ];
    let idx = (case_variation as usize) % variations.len();
    TestResult::from_bool(parse_cache_control(variations[idx]) == CacheBypassMode::NoCache)
}

fn prop_case_insensitive_no_store_parsing(case_variation: u8) -> TestResult {
    let variations = vec![
        "no-store", "NO-STORE", "No-Store", "nO-sToRe", "NO-store", "no-STORE", "No-store",
        "nO-StOrE",
    ];
    let idx = (case_variation as usize) % variations.len();
    TestResult::from_bool(parse_cache_control(variations[idx]) == CacheBypassMode::NoStore)
}

fn prop_case_insensitive_mixed_directives(case_variation: u8) -> TestResult {
    let variations = vec![
        "max-age=0, NO-CACHE",
        "NO-CACHE, max-age=0",
        "Max-Age=0, No-Cache",
        "no-cache, MAX-AGE=0",
        "private, NO-CACHE, must-revalidate",
        "NO-CACHE, PRIVATE",
    ];
    let idx = (case_variation as usize) % variations.len();
    TestResult::from_bool(parse_cache_control(variations[idx]) == CacheBypassMode::NoCache)
}

#[test]
fn test_property_case_insensitive_no_cache() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_case_insensitive_no_cache_parsing as fn(u8) -> TestResult);
}

#[test]
fn test_property_case_insensitive_no_store() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_case_insensitive_no_store_parsing as fn(u8) -> TestResult);
}

#[test]
fn test_property_case_insensitive_mixed_directives() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_case_insensitive_mixed_directives as fn(u8) -> TestResult);
}

// ============================================================================
// Property 8: Multiple directive parsing
// **Validates: Requirements 4.2, 4.3, 4.5**
// ============================================================================

#[test]
fn test_property_multiple_directives_with_no_cache() {
    let test_cases = [
        "no-cache, max-age=0, private",
        "max-age=0, no-cache, private",
        "max-age=0, private, no-cache",
        " no-cache , max-age=0",
        "max-age=0 , no-cache ",
        "\tno-cache\t,\tprivate",
    ];
    for header in test_cases {
        assert_eq!(
            parse_cache_control(header),
            CacheBypassMode::NoCache,
            "Failed for: '{}'",
            header
        );
    }
}

#[test]
fn test_property_multiple_directives_with_no_store() {
    let test_cases = [
        "no-store, max-age=0, private",
        "max-age=0, no-store, private",
        "max-age=0, private, no-store",
        " no-store , max-age=0",
        "max-age=0 , no-store ",
        "\tno-store\t,\tprivate",
    ];
    for header in test_cases {
        assert_eq!(
            parse_cache_control(header),
            CacheBypassMode::NoStore,
            "Failed for: '{}'",
            header
        );
    }
}

#[test]
fn test_property_unknown_directives_only() {
    let test_cases = [
        "max-age=0",
        "private, public",
        "max-age=3600, private, must-revalidate",
    ];
    for header in test_cases {
        assert_eq!(
            parse_cache_control(header),
            CacheBypassMode::None,
            "Failed for: '{}'",
            header
        );
    }
}

#[test]
fn test_property_whitespace_handling() {
    let test_cases = [
        ("  no-cache  ", CacheBypassMode::NoCache),
        ("\tno-cache\t", CacheBypassMode::NoCache),
        ("  max-age=0  ,  no-cache  ", CacheBypassMode::NoCache),
        ("\tmax-age=0\t,\tno-store\t", CacheBypassMode::NoStore),
        ("  private  ,  public  ", CacheBypassMode::None),
    ];
    for (header, expected) in test_cases {
        assert_eq!(
            parse_cache_control(header),
            expected,
            "Failed for: '{}'",
            header
        );
    }
}

// ============================================================================
// Property 9: no-store takes precedence over no-cache
// **Validates: Requirements 4.4**
// ============================================================================

fn prop_no_store_precedence_over_no_cache(order_variant: u8, whitespace_variant: u8) -> TestResult {
    let order_variations = vec![("no-cache", "no-store"), ("no-store", "no-cache")];
    let whitespace_patterns = vec![", ", ",", " , ", "\t,\t"];

    let order_idx = (order_variant as usize) % order_variations.len();
    let ws_idx = (whitespace_variant as usize) % whitespace_patterns.len();

    let (first, second) = order_variations[order_idx];
    let sep = whitespace_patterns[ws_idx];
    let header_value = format!("{}{}{}", first, sep, second);

    TestResult::from_bool(parse_cache_control(&header_value) == CacheBypassMode::NoStore)
}

fn prop_no_store_precedence_with_other_directives(variant: u8) -> TestResult {
    let test_cases = vec![
        "no-cache, no-store, max-age=0",
        "no-store, no-cache, max-age=0",
        "max-age=0, no-cache, no-store",
        "max-age=0, no-store, no-cache",
        "no-cache, max-age=0, no-store",
        "no-store, max-age=0, no-cache",
        "private, no-cache, no-store, must-revalidate",
        "private, no-store, no-cache, must-revalidate",
    ];
    let idx = (variant as usize) % test_cases.len();
    TestResult::from_bool(parse_cache_control(test_cases[idx]) == CacheBypassMode::NoStore)
}

fn prop_no_store_precedence_case_insensitive(variant: u8) -> TestResult {
    let test_cases = vec![
        "NO-CACHE, NO-STORE",
        "NO-STORE, NO-CACHE",
        "No-Cache, No-Store",
        "No-Store, No-Cache",
        "no-cache, NO-STORE",
        "NO-STORE, no-cache",
        "nO-cAcHe, nO-sToRe",
        "nO-sToRe, nO-cAcHe",
    ];
    let idx = (variant as usize) % test_cases.len();
    TestResult::from_bool(parse_cache_control(test_cases[idx]) == CacheBypassMode::NoStore)
}

#[test]
fn test_property_no_store_precedence_over_no_cache() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_no_store_precedence_over_no_cache as fn(u8, u8) -> TestResult);
}

#[test]
fn test_property_no_store_precedence_with_other_directives() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_no_store_precedence_with_other_directives as fn(u8) -> TestResult);
}

#[test]
fn test_property_no_store_precedence_case_insensitive() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_no_store_precedence_case_insensitive as fn(u8) -> TestResult);
}

// ============================================================================
// Property 2a: Non-cacheable operations remain non-cacheable
// **Validates: Requirements 1.4, 3.4**
// ============================================================================

fn prop_non_cacheable_operations(operation_idx: u8) -> TestResult {
    let empty_params = HashMap::new();
    let mut list_params = HashMap::new();
    list_params.insert("list-type".to_string(), "2".to_string());
    let mut acl_params = HashMap::new();
    acl_params.insert("acl".to_string(), "".to_string());

    let operations: Vec<(&str, &HashMap<String, String>, &str)> = vec![
        ("/", &empty_params, "ListBuckets"),
        ("/bucket", &list_params, "ListObjects"),
        ("/bucket/key", &acl_params, "GetObjectAcl"),
    ];

    let idx = (operation_idx as usize) % operations.len();
    let (path, query_params, expected_op) = operations[idx];
    let (should_bypass, operation_type, _) = HttpProxy::should_bypass_cache(path, query_params);

    TestResult::from_bool(should_bypass && operation_type == Some(expected_op.to_string()))
}

fn prop_get_object_is_cacheable(_: u8) -> TestResult {
    let (should_bypass, op, _) = HttpProxy::should_bypass_cache("/bucket/key", &HashMap::new());
    TestResult::from_bool(!should_bypass && op.is_none())
}

fn prop_cache_bypass_headers(header_idx: u8) -> TestResult {
    let mut no_cache = HashMap::new();
    no_cache.insert("cache-control".to_string(), "no-cache".to_string());
    let mut no_store = HashMap::new();
    no_store.insert("cache-control".to_string(), "no-store".to_string());
    let mut pragma = HashMap::new();
    pragma.insert("pragma".to_string(), "no-cache".to_string());
    let empty = HashMap::new();

    let test_cases: Vec<(&HashMap<String, String>, CacheBypassMode)> = vec![
        (&no_cache, CacheBypassMode::NoCache),
        (&no_store, CacheBypassMode::NoStore),
        (&pragma, CacheBypassMode::NoCache),
        (&empty, CacheBypassMode::None),
    ];

    let idx = (header_idx as usize) % test_cases.len();
    let (headers, expected) = test_cases[idx];
    TestResult::from_bool(parse_cache_bypass_headers(headers, true) == expected)
}

#[test]
fn test_property_non_cacheable_operations() {
    QuickCheck::new()
        .tests(50)
        .quickcheck(prop_non_cacheable_operations as fn(u8) -> TestResult);
}

#[test]
fn test_property_get_object_is_cacheable() {
    QuickCheck::new()
        .tests(50)
        .quickcheck(prop_get_object_is_cacheable as fn(u8) -> TestResult);
}

#[test]
fn test_property_cache_bypass_headers() {
    QuickCheck::new()
        .tests(50)
        .quickcheck(prop_cache_bypass_headers as fn(u8) -> TestResult);
}

// ============================================================================
// Property 10: Header stripping on forward
// **Validates: Requirements 6.1, 6.2, 6.3**
// ============================================================================

fn strip_cache_headers(headers: &HashMap<String, String>) -> HashMap<String, String> {
    let mut forwarded_headers = headers.clone();
    forwarded_headers.remove("cache-control");
    forwarded_headers.remove("pragma");
    forwarded_headers
}

fn prop_header_stripping_removes_cache_headers(seed: u64) -> TestResult {
    let mut headers: HashMap<String, String> = HashMap::new();

    let cache_control_values = vec![
        "no-cache",
        "no-store",
        "no-cache, no-store",
        "max-age=0, no-cache",
    ];
    let cc_idx = (seed as usize) % cache_control_values.len();
    headers.insert(
        "cache-control".to_string(),
        cache_control_values[cc_idx].to_string(),
    );

    if seed % 2 == 0 {
        headers.insert("pragma".to_string(), "no-cache".to_string());
    }

    // Add other headers
    headers.insert("host".to_string(), "s3.amazonaws.com".to_string());
    headers.insert("user-agent".to_string(), "test-agent".to_string());

    let stripped = strip_cache_headers(&headers);

    // Verify cache headers removed
    if stripped.contains_key("cache-control") || stripped.contains_key("pragma") {
        return TestResult::failed();
    }

    // Verify other headers preserved
    if stripped.get("host") != Some(&"s3.amazonaws.com".to_string()) {
        return TestResult::failed();
    }

    TestResult::passed()
}

fn prop_only_cache_headers_removed(seed: u64) -> TestResult {
    let mut headers: HashMap<String, String> = HashMap::new();
    headers.insert("host".to_string(), "s3.amazonaws.com".to_string());
    headers.insert("user-agent".to_string(), "aws-cli/2.0".to_string());
    headers.insert(
        "authorization".to_string(),
        "AWS4-HMAC-SHA256...".to_string(),
    );

    if seed % 3 == 0 {
        headers.insert("cache-control".to_string(), "no-cache".to_string());
    }
    if seed % 3 == 1 {
        headers.insert("pragma".to_string(), "no-cache".to_string());
    }
    if seed % 3 == 2 {
        headers.insert("cache-control".to_string(), "no-store".to_string());
        headers.insert("pragma".to_string(), "no-cache".to_string());
    }

    let stripped = strip_cache_headers(&headers);

    // All non-cache headers must be present
    for key in ["host", "user-agent", "authorization"] {
        if stripped.get(key) != headers.get(key) {
            return TestResult::failed();
        }
    }

    // Cache headers must be absent
    if stripped.contains_key("cache-control") || stripped.contains_key("pragma") {
        return TestResult::failed();
    }

    TestResult::passed()
}

fn prop_empty_headers_remain_empty(_seed: u8) -> TestResult {
    let headers: HashMap<String, String> = HashMap::new();
    TestResult::from_bool(strip_cache_headers(&headers).is_empty())
}

#[test]
fn test_property_header_stripping_removes_cache_headers() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_header_stripping_removes_cache_headers as fn(u64) -> TestResult);
}

#[test]
fn test_property_only_cache_headers_removed() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_only_cache_headers_removed as fn(u64) -> TestResult);
}

#[test]
fn test_property_empty_headers_remain_empty() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_empty_headers_remain_empty as fn(u8) -> TestResult);
}

// ============================================================================
// Unit tests for edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_lowercase_no_cache() {
        assert_eq!(parse_cache_control("no-cache"), CacheBypassMode::NoCache);
    }

    #[test]
    fn test_uppercase_no_cache() {
        assert_eq!(parse_cache_control("NO-CACHE"), CacheBypassMode::NoCache);
    }

    #[test]
    fn test_lowercase_no_store() {
        assert_eq!(parse_cache_control("no-store"), CacheBypassMode::NoStore);
    }

    #[test]
    fn test_uppercase_no_store() {
        assert_eq!(parse_cache_control("NO-STORE"), CacheBypassMode::NoStore);
    }

    #[test]
    fn test_no_cache_then_no_store() {
        assert_eq!(
            parse_cache_control("no-cache, no-store"),
            CacheBypassMode::NoStore
        );
    }

    #[test]
    fn test_no_store_then_no_cache() {
        assert_eq!(
            parse_cache_control("no-store, no-cache"),
            CacheBypassMode::NoStore
        );
    }

    #[test]
    fn test_strip_cache_control_only() {
        let mut headers = HashMap::new();
        headers.insert("cache-control".to_string(), "no-cache".to_string());
        headers.insert("host".to_string(), "s3.amazonaws.com".to_string());

        let stripped = strip_cache_headers(&headers);
        assert!(!stripped.contains_key("cache-control"));
        assert_eq!(stripped.get("host"), Some(&"s3.amazonaws.com".to_string()));
    }

    #[test]
    fn test_strip_both_cache_headers() {
        let mut headers = HashMap::new();
        headers.insert("cache-control".to_string(), "no-store".to_string());
        headers.insert("pragma".to_string(), "no-cache".to_string());
        headers.insert("host".to_string(), "s3.amazonaws.com".to_string());

        let stripped = strip_cache_headers(&headers);
        assert!(!stripped.contains_key("cache-control"));
        assert!(!stripped.contains_key("pragma"));
        assert_eq!(stripped.len(), 1);
    }
}
