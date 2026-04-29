//! Bug condition exploration tests for virtual-hosted-style bucket extraction.
//!
//! **Property 1: Fault Condition** — Virtual-hosted-style bucket extraction for
//! accelerate, regional (incl. dualstack), and legacy hostnames.
//!
//! These tests encode the EXPECTED (correct) behavior per the design doc:
//! for any virtual-hosted S3 Host header, the cache key SHOULD be bucket-prefixed
//! so path-style and virtual-hosted requests for the same bucket+key share a
//! cache entry.
//!
//! On UNFIXED code, `CacheManager::generate_cache_key` only checks
//! `extract_access_point_prefix` — any non-AP virtual-hosted Host falls through
//! to `normalize_cache_key(path)` with no bucket segment, so these assertions
//! FAIL. Failure here is the SUCCESS case for this task: it confirms the bug
//! exists.
//!
//! After task 3 lands, these tests should all pass.
//!
//! **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 3.1, 3.2, 4.1,
//! 4.2, 4.3, 4.4, 5.1**

use s3_proxy::cache::CacheManager;

// ============================================================================
// Helpers — construct Host strings for each addressing style
// ============================================================================

const BUCKET: &str = "mybucket";
const OBJECT_KEY: &str = "test-obj.bin";

fn path_style_host() -> &'static str {
    "s3.us-east-1.amazonaws.com"
}

fn path_style_path() -> String {
    format!("/{}/{}", BUCKET, OBJECT_KEY)
}

fn regional_virtual_hosted_host() -> String {
    format!("{}.s3.us-east-1.amazonaws.com", BUCKET)
}

fn dualstack_regional_virtual_hosted_host() -> String {
    format!("{}.s3.dualstack.us-east-1.amazonaws.com", BUCKET)
}

fn accelerate_host() -> String {
    format!("{}.s3-accelerate.amazonaws.com", BUCKET)
}

fn accelerate_dualstack_host() -> String {
    format!("{}.s3-accelerate.dualstack.amazonaws.com", BUCKET)
}

fn legacy_global_host() -> String {
    format!("{}.s3.amazonaws.com", BUCKET)
}

fn virtual_hosted_path() -> String {
    format!("/{}", OBJECT_KEY)
}

fn expected_cache_key() -> String {
    format!("{}/{}", BUCKET, OBJECT_KEY)
}

// ============================================================================
// Cache-key equivalence — generate_cache_key
//
// All four addressing styles SHOULD produce `mybucket/test-obj.bin`.
// On unfixed code, virtual-hosted variants produce `test-obj.bin` (flat key).
//
// **Validates: Requirements 4.1, 5.1**
// ============================================================================

#[test]
fn test_cache_key_equivalence_path_style_baseline() {
    // Path-style is the baseline — this test should PASS on unfixed code because
    // the bucket is embedded in the path.
    let key = CacheManager::generate_cache_key(&path_style_path(), Some(path_style_host()));
    assert_eq!(
        key,
        expected_cache_key(),
        "path-style baseline — cache key should be bucket-prefixed"
    );
}

#[test]
fn test_cache_key_equivalence_regional_virtual_hosted() {
    // EXPECTED TO FAIL on unfixed code.
    // Unfixed: key = "test-obj.bin" (no bucket prefix).
    // Fixed:   key = "mybucket/test-obj.bin".
    let host = regional_virtual_hosted_host();
    let key = CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&host));
    assert_eq!(
        key,
        expected_cache_key(),
        "regional virtual-hosted Host `{}` should produce bucket-prefixed cache key",
        host
    );
}

#[test]
fn test_cache_key_equivalence_regional_dualstack_virtual_hosted() {
    // EXPECTED TO FAIL on unfixed code.
    let host = dualstack_regional_virtual_hosted_host();
    let key = CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&host));
    assert_eq!(
        key,
        expected_cache_key(),
        "regional dualstack virtual-hosted Host `{}` should produce bucket-prefixed cache key",
        host
    );
}

#[test]
fn test_cache_key_equivalence_accelerate() {
    // EXPECTED TO FAIL on unfixed code.
    let host = accelerate_host();
    let key = CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&host));
    assert_eq!(
        key,
        expected_cache_key(),
        "accelerate Host `{}` should produce bucket-prefixed cache key",
        host
    );
}

#[test]
fn test_cache_key_equivalence_accelerate_dualstack() {
    // EXPECTED TO FAIL on unfixed code.
    let host = accelerate_dualstack_host();
    let key = CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&host));
    assert_eq!(
        key,
        expected_cache_key(),
        "accelerate dualstack Host `{}` should produce bucket-prefixed cache key",
        host
    );
}

#[test]
fn test_cache_key_equivalence_legacy_global() {
    // EXPECTED TO FAIL on unfixed code.
    let host = legacy_global_host();
    let key = CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&host));
    assert_eq!(
        key,
        expected_cache_key(),
        "legacy global Host `{}` should produce bucket-prefixed cache key",
        host
    );
}

// ============================================================================
// Cross-style equivalence — all four styles MUST produce the identical key
// so cache entries are shared across addressing styles.
//
// **Validates: Requirement 5.1**
// ============================================================================

#[test]
fn test_cache_key_all_styles_produce_identical_key() {
    // EXPECTED TO FAIL on unfixed code: the four virtual-hosted variants produce
    // `test-obj.bin` while path-style produces `mybucket/test-obj.bin`.
    let path_style_key =
        CacheManager::generate_cache_key(&path_style_path(), Some(path_style_host()));
    let regional_key = CacheManager::generate_cache_key(
        &virtual_hosted_path(),
        Some(&regional_virtual_hosted_host()),
    );
    let accelerate_key =
        CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&accelerate_host()));
    let legacy_key =
        CacheManager::generate_cache_key(&virtual_hosted_path(), Some(&legacy_global_host()));

    assert_eq!(
        path_style_key, regional_key,
        "path-style and regional virtual-hosted keys should match"
    );
    assert_eq!(
        path_style_key, accelerate_key,
        "path-style and accelerate keys should match"
    );
    assert_eq!(
        path_style_key, legacy_key,
        "path-style and legacy global keys should match"
    );
}

// ============================================================================
// Range cache-key equivalence — generate_range_cache_key
//
// **Validates: Requirement 4.3**
// ============================================================================

#[test]
fn test_range_cache_key_equivalence_regional_virtual_hosted() {
    // EXPECTED TO FAIL on unfixed code.
    let host = regional_virtual_hosted_host();
    let key = CacheManager::generate_range_cache_key(&virtual_hosted_path(), 0, 1023, Some(&host));
    let expected = format!("{}/{}:range:0-1023", BUCKET, OBJECT_KEY);
    assert_eq!(
        key, expected,
        "regional virtual-hosted range key should be bucket-prefixed"
    );
}

#[test]
fn test_range_cache_key_equivalence_accelerate() {
    // EXPECTED TO FAIL on unfixed code.
    let host = accelerate_host();
    let key = CacheManager::generate_range_cache_key(&virtual_hosted_path(), 0, 1023, Some(&host));
    let expected = format!("{}/{}:range:0-1023", BUCKET, OBJECT_KEY);
    assert_eq!(
        key, expected,
        "accelerate range key should be bucket-prefixed"
    );
}

#[test]
fn test_range_cache_key_equivalence_legacy_global() {
    // EXPECTED TO FAIL on unfixed code.
    let host = legacy_global_host();
    let key = CacheManager::generate_range_cache_key(&virtual_hosted_path(), 0, 1023, Some(&host));
    let expected = format!("{}/{}:range:0-1023", BUCKET, OBJECT_KEY);
    assert_eq!(
        key, expected,
        "legacy global range key should be bucket-prefixed"
    );
}

#[test]
fn test_range_cache_key_all_styles_identical() {
    // EXPECTED TO FAIL on unfixed code.
    let path_style_key = CacheManager::generate_range_cache_key(
        &path_style_path(),
        0,
        1023,
        Some(path_style_host()),
    );
    let regional_key = CacheManager::generate_range_cache_key(
        &virtual_hosted_path(),
        0,
        1023,
        Some(&regional_virtual_hosted_host()),
    );
    let accelerate_key = CacheManager::generate_range_cache_key(
        &virtual_hosted_path(),
        0,
        1023,
        Some(&accelerate_host()),
    );
    let legacy_key = CacheManager::generate_range_cache_key(
        &virtual_hosted_path(),
        0,
        1023,
        Some(&legacy_global_host()),
    );

    assert_eq!(path_style_key, regional_key);
    assert_eq!(path_style_key, accelerate_key);
    assert_eq!(path_style_key, legacy_key);
}

// ============================================================================
// Part cache-key equivalence — generate_part_cache_key
//
// **Validates: Requirement 4.4**
// ============================================================================

#[test]
fn test_part_cache_key_equivalence_regional_virtual_hosted() {
    // EXPECTED TO FAIL on unfixed code.
    let host = regional_virtual_hosted_host();
    let key = CacheManager::generate_part_cache_key(&virtual_hosted_path(), 1, Some(&host));
    let expected = format!("{}/{}:part:1", BUCKET, OBJECT_KEY);
    assert_eq!(
        key, expected,
        "regional virtual-hosted part key should be bucket-prefixed"
    );
}

#[test]
fn test_part_cache_key_equivalence_accelerate() {
    // EXPECTED TO FAIL on unfixed code.
    let host = accelerate_host();
    let key = CacheManager::generate_part_cache_key(&virtual_hosted_path(), 1, Some(&host));
    let expected = format!("{}/{}:part:1", BUCKET, OBJECT_KEY);
    assert_eq!(
        key, expected,
        "accelerate part key should be bucket-prefixed"
    );
}

#[test]
fn test_part_cache_key_equivalence_legacy_global() {
    // EXPECTED TO FAIL on unfixed code.
    let host = legacy_global_host();
    let key = CacheManager::generate_part_cache_key(&virtual_hosted_path(), 1, Some(&host));
    let expected = format!("{}/{}:part:1", BUCKET, OBJECT_KEY);
    assert_eq!(
        key, expected,
        "legacy global part key should be bucket-prefixed"
    );
}

#[test]
fn test_part_cache_key_all_styles_identical() {
    // EXPECTED TO FAIL on unfixed code.
    let path_style_key =
        CacheManager::generate_part_cache_key(&path_style_path(), 1, Some(path_style_host()));
    let regional_key = CacheManager::generate_part_cache_key(
        &virtual_hosted_path(),
        1,
        Some(&regional_virtual_hosted_host()),
    );
    let accelerate_key = CacheManager::generate_part_cache_key(
        &virtual_hosted_path(),
        1,
        Some(&accelerate_host()),
    );
    let legacy_key = CacheManager::generate_part_cache_key(
        &virtual_hosted_path(),
        1,
        Some(&legacy_global_host()),
    );

    assert_eq!(path_style_key, regional_key);
    assert_eq!(path_style_key, accelerate_key);
    assert_eq!(path_style_key, legacy_key);
}

// ============================================================================
// Unit tests for the expected helper `extract_virtual_hosted_bucket`.
//
// The helper does not exist on unfixed code — these tests would not compile.
// They are commented out per the task instructions. Task 3.3 will uncomment
// them once the helper lands in task 3.1.
//
// TODO(task 3.3): enable after extract_virtual_hosted_bucket lands in src/cache.rs.
//
// **Validates: Requirements 1.1, 1.2, 1.3, 2.1, 2.2, 3.1**
// ============================================================================

use s3_proxy::cache::extract_virtual_hosted_bucket;

#[test]
fn test_extract_vhs_accelerate_standard() {
    // Requirement 1.1
    assert_eq!(
        extract_virtual_hosted_bucket("mybucket.s3-accelerate.amazonaws.com"),
        Some("mybucket")
    );
}

#[test]
fn test_extract_vhs_accelerate_dualstack() {
    // Requirement 1.2
    assert_eq!(
        extract_virtual_hosted_bucket("mybucket.s3-accelerate.dualstack.amazonaws.com"),
        Some("mybucket")
    );
}

#[test]
fn test_extract_vhs_regional_standard() {
    // Requirement 2.1
    assert_eq!(
        extract_virtual_hosted_bucket("mybucket.s3.us-east-1.amazonaws.com"),
        Some("mybucket")
    );
}

#[test]
fn test_extract_vhs_regional_dualstack() {
    // Requirement 2.2
    assert_eq!(
        extract_virtual_hosted_bucket("mybucket.s3.dualstack.us-east-1.amazonaws.com"),
        Some("mybucket")
    );
}

#[test]
fn test_extract_vhs_legacy_global() {
    // Requirement 3.1
    assert_eq!(
        extract_virtual_hosted_bucket("mybucket.s3.amazonaws.com"),
        Some("mybucket")
    );
}

#[test]
fn test_extract_vhs_regional_with_dotted_bucket() {
    // Regional virtual-hosted — dots allowed in bucket name.
    // Requirement 2.1
    assert_eq!(
        extract_virtual_hosted_bucket("my.company.logs.s3.us-east-1.amazonaws.com"),
        Some("my.company.logs")
    );
}

#[test]
fn test_extract_vhs_accelerate_with_dots_rejected() {
    // Accelerate requires DNS-compliant bucket names — dots disallowed.
    // Requirement 1.3
    assert_eq!(
        extract_virtual_hosted_bucket("foo.bar.s3-accelerate.amazonaws.com"),
        None
    );
}

// ============================================================================
// Property 2: Preservation — Path-style, AP/MRAP, and unknown Host cache keys
// are unchanged by the forthcoming virtual-hosted fix.
//
// All tests in this section use the shared prefix `test_preservation_` so they
// can be filtered with `cargo test test_preservation_`.
//
// These tests MUST PASS on unfixed code — they document the baseline behaviour
// that MUST continue to pass after task 3 lands.
//
// **Validates: Requirements 7.1, 7.2, 7.3, 8.1, 8.2**
// ============================================================================

use s3_proxy::http_proxy::detect_path_style_alias;

// ----------------------------------------------------------------------------
// Path-style preservation (Requirement 7.1)
//
// Path-style Hosts carry the bucket in the URL path, so the cache key is just
// the normalized path (`strip leading slash`). Unchanged by this spec.
// ----------------------------------------------------------------------------

#[test]
fn test_preservation_path_style_regional() {
    // Regional path-style: s3.<region>.amazonaws.com
    let key = CacheManager::generate_cache_key(
        "/mybucket/key",
        Some("s3.us-east-1.amazonaws.com"),
    );
    assert_eq!(
        key, "mybucket/key",
        "regional path-style Host should produce bucket-prefixed cache key from the URL path"
    );
}

#[test]
fn test_preservation_path_style_legacy_global() {
    // Legacy global path-style: s3.amazonaws.com
    let key = CacheManager::generate_cache_key(
        "/mybucket/key",
        Some("s3.amazonaws.com"),
    );
    assert_eq!(
        key, "mybucket/key",
        "legacy global path-style Host should produce bucket-prefixed cache key from the URL path"
    );
}

#[test]
fn test_preservation_path_style_dualstack() {
    // Dualstack path-style: s3.dualstack.<region>.amazonaws.com
    let key = CacheManager::generate_cache_key(
        "/mybucket/key",
        Some("s3.dualstack.us-east-1.amazonaws.com"),
    );
    assert_eq!(
        key, "mybucket/key",
        "dualstack path-style Host should produce bucket-prefixed cache key from the URL path"
    );
}

// ----------------------------------------------------------------------------
// AP / MRAP preservation (Requirement 7.2)
//
// `extract_access_point_prefix` runs BEFORE `extract_virtual_hosted_bucket` in
// `generate_cache_key`, so AP/MRAP hostnames continue to apply their reserved
// suffixes (`-s3alias` / `.mrap`) and never fall through to the virtual-hosted
// branch.
// ----------------------------------------------------------------------------

#[test]
fn test_preservation_regional_access_point_cache_key() {
    // Regional AP host: <name>-<account>.s3-accesspoint.<region>.amazonaws.com
    let key = CacheManager::generate_cache_key(
        "/key",
        Some("myap-123456789012.s3-accesspoint.us-east-1.amazonaws.com"),
    );
    assert_eq!(
        key, "myap-123456789012-s3alias/key",
        "regional AP host should keep the -s3alias reserved-suffix cache key"
    );
}

#[test]
fn test_preservation_mrap_cache_key() {
    // MRAP host: <alias>.accesspoint.s3-global.amazonaws.com
    let key = CacheManager::generate_cache_key(
        "/key",
        Some("mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com"),
    );
    assert_eq!(
        key, "mfzwi23gnjvgw.mrap/key",
        "MRAP host should keep the .mrap reserved-suffix cache key"
    );
}

#[test]
fn test_preservation_path_style_ap_alias_detection_runs_first() {
    // Path-style AP alias: Host is the base AP domain and the first path
    // segment is the alias ending in `-s3alias`. The alias router
    // (`detect_path_style_alias`) is invoked in `handle_request` BEFORE any
    // cache-key generation, so it must still detect this shape and return the
    // reconstructed upstream host + cache-key prefix unchanged.
    //
    // This is the guard that ensures AP path-style detection continues to run
    // BEFORE virtual-hosted bucket extraction — the virtual-hosted helper is
    // never reached for this Host because (a) the alias router rewrites the
    // Host before cache-key generation, and (b) even if reached, the base AP
    // domain doesn't match any virtual-hosted suffix.
    let host = "s3-accesspoint.us-east-1.amazonaws.com";
    let path = "/myalias-s3alias/key";

    let alias = detect_path_style_alias(host, path)
        .expect("path-style AP alias should still be detected");
    assert_eq!(
        alias.upstream_host,
        "myalias-s3alias.s3-accesspoint.us-east-1.amazonaws.com",
        "AP path-style detection should reconstruct upstream host from the alias segment"
    );
    assert_eq!(alias.forwarded_path, "/key");
    assert_eq!(alias.cache_key_prefix, "myalias-s3alias");
}

// ----------------------------------------------------------------------------
// Unknown hostname fall-through (Requirement 7.3)
//
// When the Host header does not match any known S3 hostname pattern — AP,
// MRAP, path-style, regional virtual-hosted, accelerate, legacy — the system
// falls back to path-only cache-key behaviour (`normalize_cache_key(path)`).
// ----------------------------------------------------------------------------

#[test]
fn test_preservation_fallthrough_unknown_domain() {
    let key = CacheManager::generate_cache_key("/bucket/key", Some("example.com"));
    assert_eq!(
        key, "bucket/key",
        "unknown domain should fall through to path-only cache key"
    );
}

#[test]
fn test_preservation_fallthrough_localhost() {
    let key = CacheManager::generate_cache_key("/bucket/key", Some("localhost"));
    assert_eq!(
        key, "bucket/key",
        "localhost should fall through to path-only cache key"
    );
}

#[test]
fn test_preservation_fallthrough_non_aws_s3_lookalike() {
    // Non-AWS S3-compatible hostname (e.g. MinIO, R2) — must NOT be parsed as
    // virtual-hosted AWS S3. Falls through to path-only behaviour.
    let key = CacheManager::generate_cache_key("/key", Some("mybucket.s3.example.com"));
    assert_eq!(
        key, "key",
        "non-AWS hostname that looks virtual-hosted must NOT be extracted — falls through to path"
    );
}

#[test]
fn test_preservation_fallthrough_empty_host() {
    // Empty Host string (not `None` — `Some("")`). Still hits the AP/MRAP
    // checks, matches nothing, falls through.
    let key = CacheManager::generate_cache_key("/key", Some(""));
    assert_eq!(
        key, "key",
        "empty Host string should fall through to path-only cache key"
    );
}

#[test]
fn test_preservation_fallthrough_no_host_header() {
    // `None` host (Host header absent entirely).
    let key = CacheManager::generate_cache_key("/key", None);
    assert_eq!(
        key, "key",
        "missing Host header should produce path-only cache key"
    );
}

// ----------------------------------------------------------------------------
// Scoped PBT (Requirement 8.2)
//
// Generate random AP/MRAP hostnames and random virtual-hosted S3 hostnames
// and assert the two never collide: AP/MRAP hostnames always produce keys
// containing the reserved suffix (`-s3alias/` or `.mrap/`), and virtual-
// hosted hostnames on unfixed code produce the bare normalized path.
//
// These are preservation properties: on unfixed code they pass; after the
// virtual-hosted fix lands, the AP/MRAP branch still wins (the AP/MRAP check
// runs first in `generate_cache_key`), so the non-collision invariant holds.
// ----------------------------------------------------------------------------

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

/// A random S3-legal bucket-label (lowercase alphanumeric + hyphens, 3-20
/// chars, no leading/trailing hyphen).
#[derive(Debug, Clone)]
struct BucketLabel(String);

impl Arbitrary for BucketLabel {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 3 + (u8::arbitrary(g) % 18) as usize;
        let bytes: Vec<u8> = (0..len)
            .map(|i| {
                // Force first and last char to be alphanumeric (no hyphen).
                let allow_hyphen = i != 0 && i != len - 1;
                let pool = if allow_hyphen { 37 } else { 36 };
                let idx = u8::arbitrary(g) % pool;
                if idx < 26 {
                    b'a' + idx
                } else if idx < 36 {
                    b'0' + (idx - 26)
                } else {
                    b'-'
                }
            })
            .collect();
        BucketLabel(String::from_utf8(bytes).unwrap())
    }
}

/// A random 12-digit AWS account ID.
#[derive(Debug, Clone)]
struct AcctId(String);

impl Arbitrary for AcctId {
    fn arbitrary(g: &mut Gen) -> Self {
        let digits: String = (0..12)
            .map(|_| (b'0' + (u8::arbitrary(g) % 10)) as char)
            .collect();
        AcctId(digits)
    }
}

/// A random AWS region.
#[derive(Debug, Clone)]
struct Region(String);

impl Arbitrary for Region {
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
        Region(regions[idx].to_string())
    }
}

/// A random MRAP alias (lowercase alphanumeric, 8-17 chars).
#[derive(Debug, Clone)]
struct MrapAlias(String);

impl Arbitrary for MrapAlias {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 8 + (u8::arbitrary(g) % 10) as usize;
        let s: String = (0..len)
            .map(|_| {
                let idx = u8::arbitrary(g) % 36;
                if idx < 26 {
                    (b'a' + idx) as char
                } else {
                    (b'0' + idx - 26) as char
                }
            })
            .collect();
        MrapAlias(s)
    }
}

/// A random object path (no leading slash on the object key itself).
#[derive(Debug, Clone)]
struct ObjPath(String);

impl Arbitrary for ObjPath {
    fn arbitrary(g: &mut Gen) -> Self {
        let depth = 1 + (u8::arbitrary(g) % 4) as usize;
        let segments: Vec<String> = (0..depth)
            .map(|_| {
                let len = 1 + (u8::arbitrary(g) % 10) as usize;
                (0..len)
                    .map(|_| {
                        let idx = u8::arbitrary(g) % 36;
                        if idx < 26 {
                            (b'a' + idx) as char
                        } else {
                            (b'0' + idx - 26) as char
                        }
                    })
                    .collect::<String>()
            })
            .collect();
        ObjPath(format!("/{}", segments.join("/")))
    }
}

/// Helper: build a regional AP host.
fn regional_ap_host(name: &str, account: &str, region: &str) -> String {
    format!("{}-{}.s3-accesspoint.{}.amazonaws.com", name, account, region)
}

/// Helper: build an MRAP host.
fn mrap_host(alias: &str) -> String {
    format!("{}.accesspoint.s3-global.amazonaws.com", alias)
}

/// Helper: build a regional virtual-hosted S3 host.
fn vhs_regional_host(bucket: &str, region: &str) -> String {
    format!("{}.s3.{}.amazonaws.com", bucket, region)
}

/// Helper: build an accelerate host.
fn vhs_accelerate_host(bucket: &str) -> String {
    format!("{}.s3-accelerate.amazonaws.com", bucket)
}

/// **Property: AP hosts always produce keys containing `-s3alias/`.**
///
/// Preserves Requirement 7.2: AP/MRAP reserved-suffix cache keys survive the
/// virtual-hosted fix because `extract_access_point_prefix` runs first.
fn prop_preservation_ap_key_contains_s3alias(
    name: BucketLabel,
    account: AcctId,
    region: Region,
    path: ObjPath,
) -> TestResult {
    let host = regional_ap_host(&name.0, &account.0, &region.0);
    let key = CacheManager::generate_cache_key(&path.0, Some(&host));
    TestResult::from_bool(key.contains("-s3alias/"))
}

#[test]
fn test_preservation_pbt_ap_key_contains_s3alias() {
    QuickCheck::new().tests(100).quickcheck(
        prop_preservation_ap_key_contains_s3alias
            as fn(BucketLabel, AcctId, Region, ObjPath) -> TestResult,
    );
}

/// **Property: MRAP hosts always produce keys containing `.mrap/`.**
fn prop_preservation_mrap_key_contains_mrap(
    alias: MrapAlias,
    path: ObjPath,
) -> TestResult {
    let host = mrap_host(&alias.0);
    let key = CacheManager::generate_cache_key(&path.0, Some(&host));
    TestResult::from_bool(key.contains(".mrap/"))
}

#[test]
fn test_preservation_pbt_mrap_key_contains_mrap() {
    QuickCheck::new().tests(100).quickcheck(
        prop_preservation_mrap_key_contains_mrap
            as fn(MrapAlias, ObjPath) -> TestResult,
    );
}

/// **Property: AP and virtual-hosted S3 cache keys never collide.**
///
/// For any AP host and any virtual-hosted regional S3 host with any path, the
/// generated cache keys must differ. AP keys always contain `-s3alias/`;
/// virtual-hosted keys on unfixed code are the bare normalized path, and
/// after the fix they are `<bucket>/<path>` — neither form contains
/// `-s3alias/`, so the two never collide.
fn prop_preservation_ap_vs_vhs_no_collision(
    ap_name: BucketLabel,
    ap_account: AcctId,
    ap_region: Region,
    vhs_bucket: BucketLabel,
    vhs_region: Region,
    path: ObjPath,
) -> TestResult {
    let ap = regional_ap_host(&ap_name.0, &ap_account.0, &ap_region.0);
    let vhs = vhs_regional_host(&vhs_bucket.0, &vhs_region.0);
    let ap_key = CacheManager::generate_cache_key(&path.0, Some(&ap));
    let vhs_key = CacheManager::generate_cache_key(&path.0, Some(&vhs));
    TestResult::from_bool(ap_key != vhs_key)
}

#[test]
fn test_preservation_pbt_ap_vs_vhs_no_collision() {
    QuickCheck::new().tests(100).quickcheck(
        prop_preservation_ap_vs_vhs_no_collision
            as fn(BucketLabel, AcctId, Region, BucketLabel, Region, ObjPath) -> TestResult,
    );
}

/// **Property: MRAP and virtual-hosted S3 cache keys never collide.**
fn prop_preservation_mrap_vs_vhs_no_collision(
    mrap_alias: MrapAlias,
    vhs_bucket: BucketLabel,
    vhs_region: Region,
    path: ObjPath,
) -> TestResult {
    let mrap = mrap_host(&mrap_alias.0);
    let vhs = vhs_regional_host(&vhs_bucket.0, &vhs_region.0);
    let mrap_key = CacheManager::generate_cache_key(&path.0, Some(&mrap));
    let vhs_key = CacheManager::generate_cache_key(&path.0, Some(&vhs));
    TestResult::from_bool(mrap_key != vhs_key)
}

#[test]
fn test_preservation_pbt_mrap_vs_vhs_no_collision() {
    QuickCheck::new().tests(100).quickcheck(
        prop_preservation_mrap_vs_vhs_no_collision
            as fn(MrapAlias, BucketLabel, Region, ObjPath) -> TestResult,
    );
}

/// **Property: AP and virtual-hosted accelerate cache keys never collide.**
///
/// Accelerate hostnames are also virtual-hosted, so the same AP/MRAP-wins-
/// first invariant must hold.
fn prop_preservation_ap_vs_accelerate_no_collision(
    ap_name: BucketLabel,
    ap_account: AcctId,
    ap_region: Region,
    accel_bucket: BucketLabel,
    path: ObjPath,
) -> TestResult {
    let ap = regional_ap_host(&ap_name.0, &ap_account.0, &ap_region.0);
    let accel = vhs_accelerate_host(&accel_bucket.0);
    let ap_key = CacheManager::generate_cache_key(&path.0, Some(&ap));
    let accel_key = CacheManager::generate_cache_key(&path.0, Some(&accel));
    TestResult::from_bool(ap_key != accel_key)
}

#[test]
fn test_preservation_pbt_ap_vs_accelerate_no_collision() {
    QuickCheck::new().tests(100).quickcheck(
        prop_preservation_ap_vs_accelerate_no_collision
            as fn(BucketLabel, AcctId, Region, BucketLabel, ObjPath) -> TestResult,
    );
}

// ============================================================================
// Upstream forwarding — confirm Host header is used unchanged when the proxy
// builds the upstream request URI (no rewriting introduced by this spec).
//
// **Static review summary (2025) — `src/http_proxy.rs`, `src/s3_client.rs`,
// `src/connection_pool.rs`:**
//
// 1. `HttpProxy::validate_host_header` reads the client Host header and
//    returns the hostname (stripping port, unwrapping bracketed IPv6). It is
//    the sole source of the `host: String` value that downstream handlers
//    receive.
//
// 2. The `host` value is passed as-is to `handle_get_head_request`,
//    `handle_put_request`, and `handle_other_request`. Every upstream URI
//    construction site in `http_proxy.rs` uses `format!("https://{}{}", host,
//    path)` with that unmodified `host` — see the PUT forward path
//    (`forward_put_to_s3_with_body`), the range refill path (missing-range
//    fetch), and the streaming GET path. None of these sites substitute or
//    rewrite the Host based on its shape (accelerate / regional / legacy).
//
// 3. `build_s3_request_context` in `src/s3_client.rs` constructs the absolute
//    URI from `format!("https://{}{}", format_authority_host(&host, None),
//    path_and_query)` when given a relative URI, and otherwise preserves the
//    supplied absolute URI. `format_authority_host` only brackets IPv6
//    literals — it does not rewrite the hostname string.
//
// 4. `S3Client::try_forward_request` may rewrite the URI *authority* (not the
//    `Host` header) to an IP address when `ip_distribution_enabled` is on, and
//    in that case explicitly re-sets the outbound `Host` header back to
//    `context.host` (the original client hostname) for SigV4 compatibility.
//    The hostname value itself is never transformed.
//
// 5. `src/connection_pool.rs` owns DNS resolution and endpoint-override IP
//    distribution. It accepts the hostname as input and returns IPs — it does
//    not read or modify any Host header.
//
// 6. The existing `detect_path_style_alias` helper in `http_proxy.rs` DOES
//    reconstruct an upstream Host from a path prefix for AP/MRAP path-style
//    aliases (`s3-accesspoint.<region>.amazonaws.com` + `/myalias-s3alias/…`
//    becomes Host `myalias-s3alias.s3-accesspoint.<region>.amazonaws.com`).
//    This behaviour is orthogonal to this spec — it only fires on the base
//    AP/MRAP domains, never on accelerate / regional virtual-hosted /
//    legacy Hosts — and is unchanged.
//
// **Conclusion:** for every supported virtual-hosted Host variant in this
// spec (`*.s3-accelerate.amazonaws.com`, `*.s3-accelerate.dualstack.amazonaws.com`,
// `*.s3.<region>.amazonaws.com`, `*.s3.dualstack.<region>.amazonaws.com`,
// `*.s3.amazonaws.com`), the upstream request URI's hostname is byte-identical
// to the client Host header. No code path rewrites it.
//
// The tests below lock in that invariant through the public
// `build_s3_request_context` helper, which is the single funnel every upstream
// request passes through.
//
// **Validates: Requirements 6.1, 6.2**
// ============================================================================

use bytes::Bytes;
use hyper::{Method, Uri};
use s3_proxy::s3_client::build_s3_request_context;
use std::collections::HashMap;

/// Supported virtual-hosted Host variants per the spec.
///
/// Order matches the bucket-extraction suffix priority in the design doc
/// (accelerate dualstack → accelerate standard → regional dualstack → regional
/// standard → legacy global).
fn supported_virtual_hosted_hosts() -> Vec<String> {
    vec![
        format!("{}.s3-accelerate.dualstack.amazonaws.com", BUCKET),
        format!("{}.s3-accelerate.amazonaws.com", BUCKET),
        format!("{}.s3.dualstack.us-east-1.amazonaws.com", BUCKET),
        format!("{}.s3.us-east-1.amazonaws.com", BUCKET),
        format!("{}.s3.amazonaws.com", BUCKET),
    ]
}

/// Build a request context with a relative URI (the common case — downstream
/// handlers get the URI straight off the incoming request, which is relative
/// by the time it reaches `handle_get_head_request`) and assert the context's
/// upstream URI host equals the input Host exactly.
fn assert_upstream_host_matches(host: &str) {
    let relative_uri: Uri = format!("/{}", OBJECT_KEY).parse().expect("parse relative URI");
    let ctx = build_s3_request_context(
        Method::GET,
        relative_uri,
        HashMap::new(),
        None,
        host.to_string(),
    );

    // 1. The context's `host` field MUST be byte-identical to the input.
    assert_eq!(
        ctx.host, host,
        "S3RequestContext.host should equal the client Host header verbatim for `{}`",
        host
    );

    // 2. The upstream URI's authority MUST embed the same Host — no rewrite.
    let upstream_host = ctx
        .uri
        .host()
        .unwrap_or_else(|| panic!("upstream URI for `{}` has no host component", host));
    assert_eq!(
        upstream_host, host,
        "upstream URI host should be byte-identical to the client Host header for `{}` (got URI `{}`)",
        host, ctx.uri
    );

    // 3. The upstream URI's scheme MUST be https (proxy forwards over TLS to S3).
    assert_eq!(
        ctx.uri.scheme_str(),
        Some("https"),
        "upstream URI scheme for `{}` should be https",
        host
    );

    // 4. The path MUST be preserved exactly (no path rewriting either).
    assert_eq!(
        ctx.uri.path(),
        format!("/{}", OBJECT_KEY),
        "upstream URI path should be preserved for `{}`",
        host
    );
}

#[test]
fn test_upstream_forwarding_accelerate_standard() {
    // Requirement 6.1: accelerate Host MUST be forwarded unchanged.
    assert_upstream_host_matches("mybucket.s3-accelerate.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_accelerate_dualstack() {
    // Requirement 6.1
    assert_upstream_host_matches("mybucket.s3-accelerate.dualstack.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_regional_virtual_hosted() {
    // Requirement 6.1
    assert_upstream_host_matches("mybucket.s3.us-east-1.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_regional_dualstack_virtual_hosted() {
    // Requirement 6.1
    assert_upstream_host_matches("mybucket.s3.dualstack.us-east-1.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_legacy_global() {
    // Requirement 6.1
    assert_upstream_host_matches("mybucket.s3.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_all_supported_variants_identical() {
    // Requirement 6.1, 6.2: every supported virtual-hosted variant produces an
    // upstream URI whose host is byte-identical to the input Host. No variant
    // is transformed, collapsed, or substituted.
    for host in supported_virtual_hosted_hosts() {
        assert_upstream_host_matches(&host);
    }
}

#[test]
fn test_upstream_forwarding_dotted_bucket_regional_preserved() {
    // Requirement 6.1: a regional virtual-hosted Host with a dotted bucket
    // name (general-purpose bucket naming rules allow dots) MUST also be
    // forwarded unchanged. This is the S3TA counter-case — dots are allowed
    // in regional naming, disallowed in accelerate, but the upstream-forwarding
    // invariant is style-agnostic.
    assert_upstream_host_matches("my.company.logs.s3.us-east-1.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_with_absolute_uri_preserves_host() {
    // Requirement 6.1: when the request URI is already absolute (forward-proxy
    // request path), `build_s3_request_context` MUST leave the URI untouched,
    // which trivially preserves the Host.
    let absolute: Uri = "https://mybucket.s3-accelerate.amazonaws.com/test-obj.bin"
        .parse()
        .expect("parse absolute URI");
    let ctx = build_s3_request_context(
        Method::GET,
        absolute.clone(),
        HashMap::new(),
        None,
        "mybucket.s3-accelerate.amazonaws.com".to_string(),
    );

    assert_eq!(
        ctx.uri, absolute,
        "absolute URI should be preserved verbatim"
    );
    assert_eq!(
        ctx.uri.host(),
        Some("mybucket.s3-accelerate.amazonaws.com"),
        "upstream URI host should equal the client Host for absolute URIs"
    );
    assert_eq!(ctx.host, "mybucket.s3-accelerate.amazonaws.com");
}

#[test]
fn test_upstream_forwarding_put_method_also_preserves_host() {
    // Requirement 6.1: PUT requests travel the same path through
    // `build_s3_request_context` — confirm the Host preservation invariant
    // holds for write methods too.
    let relative_uri: Uri = format!("/{}", OBJECT_KEY).parse().expect("parse relative URI");
    let host = "mybucket.s3-accelerate.amazonaws.com";
    let ctx = build_s3_request_context(
        Method::PUT,
        relative_uri,
        HashMap::new(),
        Some(Bytes::from_static(b"payload")),
        host.to_string(),
    );

    assert_eq!(ctx.host, host);
    assert_eq!(ctx.uri.host(), Some(host));
    assert_eq!(ctx.method, Method::PUT);
}
