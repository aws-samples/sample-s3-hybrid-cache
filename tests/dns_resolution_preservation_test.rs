//! DNS resolution preservation tests
//!
//! Two cases:
//!
//! (a) Live DNS resolution — gated with `#[ignore]` (requires network).
//!     Builds a `ConnectionPoolManager` with the default Google + Cloudflare
//!     resolvers and resolves two well-known S3 endpoints, asserting a
//!     non-empty list of parseable `IpAddr`s.
//!
//! (b) EndpointOverrides — no network, always runs.
//!     Builds a `ConnectionPoolManager` with a stub `endpoint_overrides` map
//!     containing one exact-match entry and one `*.suffix` entry, then asserts
//!     that `resolve_override` returns the configured IPs for both match styles
//!     and `None` for a non-matching hostname.

use s3_proxy::config::ConnectionPoolConfig;
use s3_proxy::connection_pool::ConnectionPoolManager;
use std::collections::HashMap;
use std::net::IpAddr;

// ============================================================================
// Case (a) — live DNS resolution (requires network, gated with #[ignore])
// ============================================================================

/// Resolve two canonical S3 endpoints via the default Google + Cloudflare
/// resolvers and assert each returns a non-empty list of valid `IpAddr`s.
///
/// Run explicitly with: `cargo test -- --ignored`
#[tokio::test]
#[ignore]
async fn test_live_dns_resolves_s3_endpoints() {
    let manager = ConnectionPoolManager::new()
        .expect("ConnectionPoolManager::new() should succeed with default resolvers");

    for endpoint in &["s3.amazonaws.com", "s3.us-east-1.amazonaws.com"] {
        let ips = manager
            .resolve_endpoint(endpoint)
            .await
            .unwrap_or_else(|e| panic!("resolve_endpoint({endpoint}) failed: {e}"));

        assert!(
            !ips.is_empty(),
            "resolve_endpoint({endpoint}) returned an empty IP list"
        );

        for ip in &ips {
            // Verify every element is a parseable IpAddr by round-tripping through
            // its Display representation.
            let reparsed: IpAddr = ip
                .to_string()
                .parse()
                .unwrap_or_else(|e| panic!("IP {ip} from {endpoint} did not re-parse: {e}"));
            assert_eq!(
                *ip, reparsed,
                "IP round-trip mismatch for {endpoint}: {ip} != {reparsed}"
            );
        }
    }
}

// ============================================================================
// Case (b) — EndpointOverrides (no network, always runs)
// ============================================================================

/// Build a `ConnectionPoolManager` with a stub `endpoint_overrides` map and
/// verify that `resolve_override` returns the correct IPs for exact-match and
/// suffix-match entries, and `None` for a non-matching hostname.
#[test]
fn test_endpoint_overrides_exact_and_suffix_match() {
    let mut overrides: HashMap<String, Vec<String>> = HashMap::new();

    // Exact match entry
    overrides.insert(
        "s3.us-west-2.amazonaws.com".to_string(),
        vec!["10.0.1.1".to_string(), "10.0.1.2".to_string()],
    );

    // Suffix (wildcard) match entry
    overrides.insert("*.amazonaws.com".to_string(), vec!["10.0.2.1".to_string()]);

    let config = ConnectionPoolConfig {
        endpoint_overrides: overrides,
        ..ConnectionPoolConfig::default()
    };

    let manager = ConnectionPoolManager::new_with_config(config)
        .expect("ConnectionPoolManager::new_with_config() should succeed");

    // --- Exact match ---
    let exact_ips = manager
        .resolve_override("s3.us-west-2.amazonaws.com")
        .expect("exact match 's3.us-west-2.amazonaws.com' should return Some");

    let expected_exact: Vec<IpAddr> =
        vec!["10.0.1.1".parse().unwrap(), "10.0.1.2".parse().unwrap()];
    assert_eq!(
        *exact_ips, expected_exact,
        "exact match returned unexpected IPs"
    );

    // --- Suffix match ---
    // "s3.eu-west-1.amazonaws.com" does not have an exact entry but ends with
    // ".amazonaws.com", so it should match the "*.amazonaws.com" suffix override.
    let suffix_ips = manager
        .resolve_override("s3.eu-west-1.amazonaws.com")
        .expect("suffix match 's3.eu-west-1.amazonaws.com' should return Some");

    let expected_suffix: Vec<IpAddr> = vec!["10.0.2.1".parse().unwrap()];
    assert_eq!(
        *suffix_ips, expected_suffix,
        "suffix match returned unexpected IPs"
    );

    // --- No match ---
    let no_match = manager.resolve_override("not-amazonaws.com");
    assert!(
        no_match.is_none(),
        "non-matching hostname 'not-amazonaws.com' should return None, got {no_match:?}"
    );
}
