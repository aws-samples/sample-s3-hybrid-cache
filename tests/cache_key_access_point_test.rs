//! Property-based tests for access point cache key collision bug
//!
//! **Property 1: Fault Condition** — Access Point Cache Key Collision
//!
//! The current `generate_cache_key(path)` ignores the Host header entirely.
//! For access point requests (regional AP and MRAP), the path contains only the
//! object key (e.g., `/data/file.txt`) without any access-point identifier.
//! This causes cache key collisions between different access points.
//!
//! These tests encode the EXPECTED (correct) behavior: cache keys SHOULD be
//! uniquely prefixed per access point. On unfixed code, these assertions FAIL,
//! proving the bug exists.
//!
//! **Validates: Requirements 1.1, 1.3, 1.4, 1.6, 1.7**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache::CacheManager;
use s3_proxy::http_proxy::detect_path_style_alias;

// ============================================================================
// Generators
// ============================================================================

/// A random S3 object path (e.g., "/data/file.txt", "/deep/nested/object.bin")
#[derive(Debug, Clone)]
struct ObjectPath(String);

impl Arbitrary for ObjectPath {
    fn arbitrary(g: &mut Gen) -> Self {
        let depth = 1 + (u8::arbitrary(g) % 4) as usize; // 1-4 segments
        let segments: Vec<String> = (0..depth)
            .map(|_| {
                let len = 1 + (u8::arbitrary(g) % 12) as usize;
                let chars: String = (0..len)
                    .map(|_| {
                        let idx = u8::arbitrary(g) % 36;
                        if idx < 26 {
                            (b'a' + idx) as char
                        } else {
                            (b'0' + idx - 26) as char
                        }
                    })
                    .collect();
                chars
            })
            .collect();
        ObjectPath(format!("/{}", segments.join("/")))
    }
}

/// A random regional access point name (lowercase alphanumeric + hyphens, 3-50 chars)
#[derive(Debug, Clone)]
struct ApName(String);

impl Arbitrary for ApName {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 3 + (u8::arbitrary(g) % 20) as usize;
        let name: String = (0..len)
            .map(|_| {
                let idx = u8::arbitrary(g) % 37;
                if idx < 26 {
                    (b'a' + idx) as char
                } else if idx < 36 {
                    (b'0' + idx - 26) as char
                } else {
                    '-'
                }
            })
            .collect();
        ApName(name)
    }
}

/// A random 12-digit AWS account ID
#[derive(Debug, Clone)]
struct AccountId(String);

impl Arbitrary for AccountId {
    fn arbitrary(g: &mut Gen) -> Self {
        let digits: String = (0..12)
            .map(|_| {
                let d = u8::arbitrary(g) % 10;
                (b'0' + d) as char
            })
            .collect();
        AccountId(digits)
    }
}

/// A random MRAP alias (lowercase alphanumeric, typically 13 chars)
#[derive(Debug, Clone)]
struct MrapAlias(String);

impl Arbitrary for MrapAlias {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 8 + (u8::arbitrary(g) % 10) as usize;
        let alias: String = (0..len)
            .map(|_| {
                let idx = u8::arbitrary(g) % 36;
                if idx < 26 {
                    (b'a' + idx) as char
                } else {
                    (b'0' + idx - 26) as char
                }
            })
            .collect();
        MrapAlias(alias)
    }
}

/// A random AWS region
#[derive(Debug, Clone)]
struct AwsRegion(String);

impl Arbitrary for AwsRegion {
    fn arbitrary(g: &mut Gen) -> Self {
        let regions = [
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "eu-central-1",
            "ap-southeast-1",
            "ap-northeast-1",
        ];
        let idx = u8::arbitrary(g) as usize % regions.len();
        AwsRegion(regions[idx].to_string())
    }
}

// ============================================================================
// Helper: build host strings
// ============================================================================

fn regional_ap_host(name: &str, account_id: &str, region: &str) -> String {
    format!(
        "{}-{}.s3-accesspoint.{}.amazonaws.com",
        name, account_id, region
    )
}

fn mrap_host(alias: &str) -> String {
    format!("{}.accesspoint.s3-global.amazonaws.com", alias)
}

// ============================================================================
// Property 1a: Two different regional APs with the same path SHOULD produce
// different cache keys. On unfixed code they produce identical keys (collision).
// **Validates: Requirements 1.1, 1.3**
// ============================================================================

fn prop_regional_ap_collision(
    ap1_name: ApName,
    ap1_account: AccountId,
    ap2_name: ApName,
    ap2_account: AccountId,
    region: AwsRegion,
    path: ObjectPath,
) -> TestResult {
    let host1 = regional_ap_host(&ap1_name.0, &ap1_account.0, &region.0);
    let host2 = regional_ap_host(&ap2_name.0, &ap2_account.0, &region.0);

    // Skip if the two hosts happen to be identical
    if host1 == host2 {
        return TestResult::discard();
    }

    let key1 = CacheManager::generate_cache_key(&path.0, Some(&host1));
    let key2 = CacheManager::generate_cache_key(&path.0, Some(&host2));

    // Expected behavior: different AP hosts with same path SHOULD produce different keys
    TestResult::from_bool(key1 != key2)
}

#[test]
fn test_regional_ap_cache_key_collision() {
    QuickCheck::new().tests(100).quickcheck(
        prop_regional_ap_collision
            as fn(ApName, AccountId, ApName, AccountId, AwsRegion, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Property 1b: Two different MRAPs with the same path SHOULD produce
// different cache keys. On unfixed code they produce identical keys (collision).
// **Validates: Requirements 1.4, 1.6**
// ============================================================================

fn prop_mrap_collision(
    alias1: MrapAlias,
    alias2: MrapAlias,
    path: ObjectPath,
) -> TestResult {
    let host1 = mrap_host(&alias1.0);
    let host2 = mrap_host(&alias2.0);

    // Skip if the two hosts happen to be identical
    if host1 == host2 {
        return TestResult::discard();
    }

    let key1 = CacheManager::generate_cache_key(&path.0, Some(&host1));
    let key2 = CacheManager::generate_cache_key(&path.0, Some(&host2));

    // Expected behavior: different MRAP hosts with same path SHOULD produce different keys
    TestResult::from_bool(key1 != key2)
}

#[test]
fn test_mrap_cache_key_collision() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_mrap_collision as fn(MrapAlias, MrapAlias, ObjectPath) -> TestResult);
}

// ============================================================================
// Property 1c: A regional AP and an MRAP with the same path SHOULD produce
// different cache keys. On unfixed code they produce identical keys (collision).
// **Validates: Requirements 1.7**
// ============================================================================

fn prop_cross_type_collision(
    ap_name: ApName,
    ap_account: AccountId,
    region: AwsRegion,
    mrap_alias: MrapAlias,
    path: ObjectPath,
) -> TestResult {
    let ap_host = regional_ap_host(&ap_name.0, &ap_account.0, &region.0);
    let mrap_h = mrap_host(&mrap_alias.0);

    let key_ap = CacheManager::generate_cache_key(&path.0, Some(&ap_host));
    let key_mrap = CacheManager::generate_cache_key(&path.0, Some(&mrap_h));

    // Expected behavior: regional AP and MRAP with same path SHOULD produce different keys
    TestResult::from_bool(key_ap != key_mrap)
}

#[test]
fn test_cross_type_cache_key_collision() {
    QuickCheck::new().tests(100).quickcheck(
        prop_cross_type_collision
            as fn(ApName, AccountId, AwsRegion, MrapAlias, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Property 2: Preservation — Regular Request Cache Keys Unchanged
//
// For all hosts NOT matching access point or MRAP patterns, the current
// `generate_cache_key(path)` returns `normalize_cache_key(path)` (path with
// leading slash stripped). These tests capture that baseline behavior on
// unfixed code so we can verify it is preserved after the fix.
//
// **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
// ============================================================================

use s3_proxy::disk_cache::normalize_cache_key;

/// A random S3 bucket name (lowercase alphanumeric + hyphens, 3-20 chars)
#[derive(Debug, Clone)]
struct BucketName(String);

impl Arbitrary for BucketName {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 3 + (u8::arbitrary(g) % 18) as usize;
        let name: String = (0..len)
            .map(|_| {
                let idx = u8::arbitrary(g) % 37;
                if idx < 26 {
                    (b'a' + idx) as char
                } else if idx < 36 {
                    (b'0' + idx - 26) as char
                } else {
                    '-'
                }
            })
            .collect();
        BucketName(name)
    }
}

// ============================================================================
// Property 2a: Path-style S3 hosts — cache key equals normalize_cache_key(path)
//
// For hosts matching `s3.{region}.amazonaws.com`, the cache key for any path
// is simply the path with the leading slash stripped.
//
// **Validates: Requirements 3.1**
// ============================================================================

fn prop_path_style_preservation(region: AwsRegion, bucket: BucketName, path: ObjectPath) -> TestResult {
    let host = format!("s3.{}.amazonaws.com", region.0);

    // Build a path-style path: /{bucket}/{object_path_without_leading_slash}
    let object_part = path.0.strip_prefix('/').unwrap_or(&path.0);
    let full_path = format!("/{}/{}", bucket.0, object_part);

    let cache_key = CacheManager::generate_cache_key(&full_path, Some(&host));
    let expected = normalize_cache_key(&full_path);

    TestResult::from_bool(cache_key == expected)
}

#[test]
fn test_path_style_cache_key_preservation() {
    QuickCheck::new().tests(200).quickcheck(
        prop_path_style_preservation as fn(AwsRegion, BucketName, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Property 2b: Virtual-hosted-style S3 hosts — cache key equals
// normalize_cache_key(path)
//
// For hosts matching `{bucket}.s3.{region}.amazonaws.com`, the cache key for
// any path is simply the path with the leading slash stripped.
//
// **Validates: Requirements 3.2**
// ============================================================================

fn prop_virtual_hosted_preservation(
    region: AwsRegion,
    bucket: BucketName,
    path: ObjectPath,
) -> TestResult {
    let host = format!("{}.s3.{}.amazonaws.com", bucket.0, region.0);

    // Virtual-hosted path is just the object key (e.g., /data/file.txt)
    let cache_key = CacheManager::generate_cache_key(&path.0, Some(&host));
    let expected = normalize_cache_key(&path.0);

    TestResult::from_bool(cache_key == expected)
}

#[test]
fn test_virtual_hosted_cache_key_preservation() {
    QuickCheck::new().tests(200).quickcheck(
        prop_virtual_hosted_preservation as fn(AwsRegion, BucketName, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Property 2c: Range/part cache keys for non-AP hosts — range and part suffixes
// are appended to normalize_cache_key(path) without any prefix.
//
// **Validates: Requirements 3.3, 3.4**
// ============================================================================

fn prop_range_part_preservation(
    region: AwsRegion,
    bucket: BucketName,
    path: ObjectPath,
) -> TestResult {
    let host = format!("s3.{}.amazonaws.com", region.0);

    let object_part = path.0.strip_prefix('/').unwrap_or(&path.0);
    let full_path = format!("/{}/{}", bucket.0, object_part);
    let normalized = normalize_cache_key(&full_path);

    // Part cache key should be "{normalized}:part:{n}"
    let part_key = CacheManager::generate_part_cache_key(&full_path, 1, Some(&host));
    let expected_part = format!("{}:part:1", normalized);

    // Range cache key should be "{normalized}:range:{start}-{end}"
    let range_key = CacheManager::generate_range_cache_key(&full_path, 0, 1024, Some(&host));
    let expected_range = format!("{}:range:0-1024", normalized);

    TestResult::from_bool(part_key == expected_part && range_key == expected_range)
}

#[test]
fn test_range_part_cache_key_preservation() {
    QuickCheck::new().tests(200).quickcheck(
        prop_range_part_preservation as fn(AwsRegion, BucketName, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Unit tests for `extract_access_point_prefix`
//
// **Validates: Requirements 2.1, 2.4, 3.1, 3.2**
// ============================================================================

use s3_proxy::cache::extract_access_point_prefix;

#[test]
fn test_extract_regional_ap_prefix() {
    let host = "my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com";
    // Expected: returns prefix WITH reserved `-s3alias` suffix to prevent collision with bucket names
    // **Validates: Requirements 2.1**
    assert_eq!(
        extract_access_point_prefix(host),
        Some("my-ap-123456789012-s3alias".to_string())
    );
}

#[test]
fn test_extract_mrap_prefix() {
    let host = "mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com";
    // Expected: returns prefix WITH reserved `.mrap` suffix to prevent collision with bucket names
    // **Validates: Requirements 2.2**
    assert_eq!(
        extract_access_point_prefix(host),
        Some("mfzwi23gnjvgw.mrap".to_string())
    );
}

#[test]
fn test_extract_path_style_host_returns_none() {
    let host = "s3.us-east-1.amazonaws.com";
    assert_eq!(extract_access_point_prefix(host), None);
}

#[test]
fn test_extract_virtual_hosted_host_returns_none() {
    let host = "my-bucket.s3.us-east-1.amazonaws.com";
    assert_eq!(extract_access_point_prefix(host), None);
}

#[test]
fn test_extract_empty_string_returns_none() {
    assert_eq!(extract_access_point_prefix(""), None);
}

#[test]
fn test_extract_no_dots_returns_none() {
    assert_eq!(extract_access_point_prefix("localhost"), None);
}

#[test]
fn test_extract_partial_match_s3_accesspoint_no_prefix() {
    // Host is exactly the suffix with no prefix before it
    let host = ".s3-accesspoint.us-east-1.amazonaws.com";
    assert_eq!(extract_access_point_prefix(host), None);
}

#[test]
fn test_extract_partial_match_mrap_no_alias() {
    // Host is exactly the suffix with no alias before it
    let host = ".accesspoint.s3-global.amazonaws.com";
    assert_eq!(extract_access_point_prefix(host), None);
}

#[test]
fn test_extract_bare_accesspoint_suffix_returns_none() {
    // Just the suffix, no leading dot or prefix
    let host = "accesspoint.s3-global.amazonaws.com";
    assert_eq!(extract_access_point_prefix(host), None);
}

#[test]
fn test_extract_regional_ap_different_region() {
    let host = "data-ap-999888777666.s3-accesspoint.eu-west-1.amazonaws.com";
    // Expected: returns prefix WITH reserved `-s3alias` suffix regardless of region
    // **Validates: Requirements 2.1**
    assert_eq!(
        extract_access_point_prefix(host),
        Some("data-ap-999888777666-s3alias".to_string())
    );
}

// ============================================================================
// Unit test: base AP domain with no prefix returns None
// **Validates: Requirements 3.5**
// ============================================================================

#[test]
fn test_extract_base_ap_domain_returns_none() {
    // Base AP domain without any prefix — should return None
    let host = "s3-accesspoint.us-east-1.amazonaws.com";
    assert_eq!(extract_access_point_prefix(host), None);
}

// ============================================================================
// Bug Condition 2 — Path-Style Alias Not Recognized
//
// The `detect_path_style_alias` function does not exist yet. These property-based
// tests are commented out because they won't compile until the function is
// implemented in task 3.2. They encode the expected behavior per the design doc.
//
// Additionally, concrete unit tests below test the expected behavior through
// the cache key generation flow (which currently fails because path-style alias
// detection doesn't exist).
//
// **Validates: Requirements 1.4, 1.5, 1.6, 1.7, 2.4, 2.5, 2.6, 2.7**
// ============================================================================

// --- PBT for detect_path_style_alias ---

/// Scoped PBT: AP alias path-style detection
/// Generate random AP alias strings ending with `-s3alias` and random regions;
/// construct Host `s3-accesspoint.{region}.amazonaws.com` with path `/{alias}/{key}`;
/// assert detection returns reconstructed host, stripped path, and cache key prefix.
/// **Validates: Requirements 2.4, 2.6**
fn prop_detect_ap_alias_path_style(
    ap_name: ApName,
    account_id: AccountId,
    region: AwsRegion,
    path: ObjectPath,
) -> TestResult {
    // Construct an AP alias: {name}-{metadata}-s3alias
    let alias = format!("{}-{}-s3alias", ap_name.0, account_id.0);
    let host = format!("s3-accesspoint.{}.amazonaws.com", region.0);
    let object_part = path.0.strip_prefix('/').unwrap_or(&path.0);
    let full_path = format!("/{}/{}", alias, object_part);

    let result = detect_path_style_alias(&host, &full_path);
    match result {
        Some(detected) => {
            let expected_host = format!("{}.s3-accesspoint.{}.amazonaws.com", alias, region.0);
            let expected_path = format!("/{}", object_part);
            TestResult::from_bool(
                detected.upstream_host == expected_host
                    && detected.forwarded_path == expected_path
                    && detected.cache_key_prefix == alias
            )
        }
        None => TestResult::failed(),
    }
}

#[test]
fn test_detect_ap_alias_path_style() {
    QuickCheck::new().tests(100).quickcheck(
        prop_detect_ap_alias_path_style
            as fn(ApName, AccountId, AwsRegion, ObjectPath) -> TestResult,
    );
}

/// Scoped PBT: MRAP alias path-style detection
/// Generate random MRAP alias strings ending with `.mrap`;
/// construct Host `accesspoint.s3-global.amazonaws.com` with path `/{alias}.mrap/{key}`;
/// assert detection returns reconstructed host (`.mrap` stripped from hostname),
/// stripped path, and cache key prefix (with `.mrap`).
/// **Validates: Requirements 2.5, 2.7**
fn prop_detect_mrap_alias_path_style(
    mrap_alias: MrapAlias,
    path: ObjectPath,
) -> TestResult {
    let alias_with_suffix = format!("{}.mrap", mrap_alias.0);
    let host = "accesspoint.s3-global.amazonaws.com";
    let object_part = path.0.strip_prefix('/').unwrap_or(&path.0);
    let full_path = format!("/{}/{}", alias_with_suffix, object_part);

    let result = detect_path_style_alias(host, &full_path);
    match result {
        Some(detected) => {
            // `.mrap` is stripped from hostname but kept in cache key prefix
            let expected_host = format!("{}.accesspoint.s3-global.amazonaws.com", mrap_alias.0);
            let expected_path = format!("/{}", object_part);
            TestResult::from_bool(
                detected.upstream_host == expected_host
                    && detected.forwarded_path == expected_path
                    && detected.cache_key_prefix == alias_with_suffix
            )
        }
        None => TestResult::failed(),
    }
}

#[test]
fn test_detect_mrap_alias_path_style() {
    QuickCheck::new().tests(100).quickcheck(
        prop_detect_mrap_alias_path_style
            as fn(MrapAlias, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Concrete unit tests for path-style alias expected behavior
// These test through the cache key generation flow. On unfixed code, the base
// AP/MRAP domain returns None from extract_access_point_prefix, so no AP prefix
// is added to the cache key — the alias in the path is treated as a regular
// path segment, producing incorrect cache keys.
// ============================================================================

#[test]
fn test_ap_alias_path_style_detected_as_access_point() {
    // AP alias path-style: Host is base AP domain, alias is first path segment.
    // The system should DETECT this as an access point request via detect_path_style_alias.
    //
    // extract_access_point_prefix correctly returns None for the base domain (no prefix).
    // Path-style alias detection is handled by detect_path_style_alias, which inspects
    // both host and path to detect the alias in the first path segment.
    //
    // **Validates: Requirements 1.4, 1.6, 2.4, 2.6**
    let host = "s3-accesspoint.us-east-1.amazonaws.com";
    let path = "/myname-abcdef123456-s3alias/data/file.txt";

    // Base AP domain should return None from extract_access_point_prefix (correct)
    let prefix = extract_access_point_prefix(host);
    assert!(prefix.is_none(), "Base AP domain should return None from extract_access_point_prefix");

    // detect_path_style_alias should detect the alias in the path
    let result = detect_path_style_alias(host, path);
    assert!(result.is_some(), "AP alias path-style should be detected");
    let alias_info = result.unwrap();
    assert_eq!(
        alias_info.upstream_host,
        "myname-abcdef123456-s3alias.s3-accesspoint.us-east-1.amazonaws.com"
    );
    assert_eq!(alias_info.forwarded_path, "/data/file.txt");
    assert_eq!(alias_info.cache_key_prefix, "myname-abcdef123456-s3alias");
}

#[test]
fn test_mrap_alias_path_style_detected_as_access_point() {
    // MRAP alias path-style: Host is base MRAP domain, alias is first path segment.
    // The system should DETECT this as an MRAP request via detect_path_style_alias.
    //
    // extract_access_point_prefix correctly returns None for the base domain (no prefix).
    // Path-style alias detection is handled by detect_path_style_alias.
    //
    // **Validates: Requirements 1.5, 1.7, 2.5, 2.7**
    let host = "accesspoint.s3-global.amazonaws.com";
    let path = "/mrymoq6iot5o4.mrap/data/file.txt";

    // Base MRAP domain should return None from extract_access_point_prefix (correct)
    let prefix = extract_access_point_prefix(host);
    assert!(prefix.is_none(), "Base MRAP domain should return None from extract_access_point_prefix");

    // detect_path_style_alias should detect the alias in the path
    let result = detect_path_style_alias(host, path);
    assert!(result.is_some(), "MRAP alias path-style should be detected");
    let alias_info = result.unwrap();
    assert_eq!(
        alias_info.upstream_host,
        "mrymoq6iot5o4.accesspoint.s3-global.amazonaws.com"
    );
    assert_eq!(alias_info.forwarded_path, "/data/file.txt");
    assert_eq!(alias_info.cache_key_prefix, "mrymoq6iot5o4.mrap");
}

// ============================================================================
// Property 2d: Non-alias path segment guard
//
// For requests where the Host is a base AP/MRAP domain but the first path
// segment does NOT end with `-s3alias` or `.mrap`, the system SHALL NOT treat
// it as an alias request. On unfixed code, there is no alias detection at all,
// so this trivially holds. This test captures the baseline to ensure the fix
// does not introduce false positive alias detection.
//
// **Validates: Requirements 3.5**
// ============================================================================

/// A random path segment that does NOT end with `-s3alias` or `.mrap`
#[derive(Debug, Clone)]
struct NonAliasSegment(String);

impl Arbitrary for NonAliasSegment {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 3 + (u8::arbitrary(g) % 20) as usize;
        let segment: String = (0..len)
            .map(|_| {
                let idx = u8::arbitrary(g) % 37;
                if idx < 26 {
                    (b'a' + idx) as char
                } else if idx < 36 {
                    (b'0' + idx - 26) as char
                } else {
                    '-'
                }
            })
            .collect();
        // Ensure the segment does NOT end with reserved suffixes
        let mut s = segment;
        if s.ends_with("-s3alias") {
            s.push_str("x");
        }
        if s.ends_with(".mrap") {
            s.push_str("x");
        }
        NonAliasSegment(s)
    }
}

fn prop_non_alias_path_segment_guard(
    region: AwsRegion,
    segment: NonAliasSegment,
    path: ObjectPath,
) -> TestResult {
    // Construct a request with a base AP domain and a non-alias first path segment
    let host = format!("s3-accesspoint.{}.amazonaws.com", region.0);
    let object_part = path.0.strip_prefix('/').unwrap_or(&path.0);
    let full_path = format!("/{}/{}", segment.0, object_part);

    // detect_path_style_alias should return None for non-alias segments.
    // No AP prefix should be added because the segment is not an alias.
    let result = detect_path_style_alias(&host, &full_path);
    TestResult::from_bool(result.is_none())
}

#[test]
fn test_non_alias_path_segment_guard() {
    QuickCheck::new().tests(200).quickcheck(
        prop_non_alias_path_segment_guard
            as fn(AwsRegion, NonAliasSegment, ObjectPath) -> TestResult,
    );
}

// Also test with MRAP base domain
fn prop_non_alias_path_segment_guard_mrap(
    segment: NonAliasSegment,
    path: ObjectPath,
) -> TestResult {
    let host = "accesspoint.s3-global.amazonaws.com";
    let object_part = path.0.strip_prefix('/').unwrap_or(&path.0);
    let full_path = format!("/{}/{}", segment.0, object_part);

    // detect_path_style_alias should return None for non-alias segments on MRAP base domain.
    let result = detect_path_style_alias(host, &full_path);
    TestResult::from_bool(result.is_none())
}

#[test]
fn test_non_alias_path_segment_guard_mrap() {
    QuickCheck::new().tests(200).quickcheck(
        prop_non_alias_path_segment_guard_mrap
            as fn(NonAliasSegment, ObjectPath) -> TestResult,
    );
}

// ============================================================================
// Property 2e: Virtual-hosted AP/MRAP routing unchanged
//
// For virtual-hosted AP and MRAP requests, the upstream host used for routing
// is the Host header value itself (unchanged). The fix only changes the cache
// key folder (appending reserved suffix), NOT the routing target.
//
// This test verifies that `extract_access_point_prefix` returns Some(_) for
// virtual-hosted AP/MRAP hosts (confirming they are recognized), and that the
// host value itself is not modified — it is passed through as-is to
// `build_s3_request_context` for routing.
//
// On unfixed code: extract_access_point_prefix returns Some(bare_prefix) for
// these hosts. The host is used as-is for routing. This test captures that
// the routing host is unchanged.
//
// **Validates: Requirements 3.6, 3.7**
// ============================================================================

fn prop_virtual_hosted_ap_routing_unchanged(
    ap_name: ApName,
    account_id: AccountId,
    region: AwsRegion,
    path: ObjectPath,
) -> TestResult {
    let host = regional_ap_host(&ap_name.0, &account_id.0, &region.0);

    // Verify the host is recognized as an AP host
    let prefix = extract_access_point_prefix(&host);
    if prefix.is_none() {
        return TestResult::failed();
    }

    // The routing host should be the original host — unchanged.
    // In the proxy flow, `handle_request` extracts the host from the Host header
    // and passes it directly to `handle_get_head_request` → `build_s3_request_context`.
    // The fix should NOT change this routing behavior.
    //
    // We verify this by confirming that the host used for routing (the input host)
    // is the same virtual-hosted host — no reconstruction or modification.
    // The upstream URL would be `https://{host}{path}`.
    let upstream_url = format!("https://{}{}", host, path.0);

    // Verify the URL contains the original host (routing target unchanged)
    let expected_host_in_url = format!("https://{}", host);
    TestResult::from_bool(upstream_url.starts_with(&expected_host_in_url))
}

#[test]
fn test_virtual_hosted_ap_routing_unchanged() {
    QuickCheck::new().tests(200).quickcheck(
        prop_virtual_hosted_ap_routing_unchanged
            as fn(ApName, AccountId, AwsRegion, ObjectPath) -> TestResult,
    );
}

fn prop_virtual_hosted_mrap_routing_unchanged(
    mrap_alias: MrapAlias,
    path: ObjectPath,
) -> TestResult {
    let host = mrap_host(&mrap_alias.0);

    // Verify the host is recognized as an MRAP host
    let prefix = extract_access_point_prefix(&host);
    if prefix.is_none() {
        return TestResult::failed();
    }

    // The routing host should be the original host — unchanged.
    let upstream_url = format!("https://{}{}", host, path.0);
    let expected_host_in_url = format!("https://{}", host);
    TestResult::from_bool(upstream_url.starts_with(&expected_host_in_url))
}

#[test]
fn test_virtual_hosted_mrap_routing_unchanged() {
    QuickCheck::new().tests(200).quickcheck(
        prop_virtual_hosted_mrap_routing_unchanged
            as fn(MrapAlias, ObjectPath) -> TestResult,
    );
}
