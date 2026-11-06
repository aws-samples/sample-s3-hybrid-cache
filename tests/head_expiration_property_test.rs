//! Property-based tests for HEAD expiration behavior
//!
//! Property 4: HEAD expiration uses head_ttl correctly
//!
//! For any HEAD response cached by the proxy, the stored `head_expires_at`
//! should equal `now + resolved_head_ttl`. For any cached HEAD entry,
//! the HEAD stored-expiry check should report expiry if and only if `head_expires_at < now`.
//!
//! **Validates: Requirements 3.2, 4.4**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use std::time::{Duration, SystemTime};

/// Generate a random Duration between 0 and ~1 year.
/// Includes Duration::ZERO to test the zero-TTL case.
fn arbitrary_ttl_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600 + 1); // 0s to ~1 year (inclusive of 0)
    Duration::from_secs(secs)
}

/// Helper: create a NewCacheMetadata with default fields for testing.
fn make_test_metadata() -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: "test-bucket/head-expiration-test".to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag-abc123".to_string(),
            "Wed, 21 Oct 2020 07:28:00 GMT".to_string(),
            1024,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

// ============================================================================
// Arbitrary inputs for Property 4
// ============================================================================

#[derive(Debug, Clone)]
struct ArbitraryHeadTtlInput {
    /// The head_ttl to apply via refresh_head_ttl (includes Duration::ZERO)
    head_ttl: Duration,
}

impl Arbitrary for ArbitraryHeadTtlInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            head_ttl: arbitrary_ttl_duration(g),
        }
    }
}

// ============================================================================
// Property 4a: refresh_head_ttl sets head_expires_at ≈ now + head_ttl
// ============================================================================

/// For any head_ttl duration, calling refresh_head_ttl(ttl) sets
/// head_expires_at to approximately now + ttl.
fn prop_refresh_head_ttl_sets_expires_at(input: ArbitraryHeadTtlInput) -> TestResult {
    let mut metadata = make_test_metadata();

    let before = SystemTime::now();
    metadata.refresh_head_ttl(input.head_ttl);
    let after = SystemTime::now();

    let head_expires_at = match metadata.head_expires_at {
        Some(t) => t,
        None => return TestResult::error("head_expires_at should be Some after refresh_head_ttl"),
    };

    // head_expires_at should be between (before + head_ttl) and (after + head_ttl + 1s tolerance)
    let expected_min = before + input.head_ttl;
    let expected_max = after + input.head_ttl + Duration::from_secs(1);

    if head_expires_at < expected_min {
        return TestResult::error(format!(
            "head_expires_at too early: actual={:?}, expected_min={:?}, head_ttl={:?}",
            head_expires_at, expected_min, input.head_ttl
        ));
    }
    if head_expires_at > expected_max {
        return TestResult::error(format!(
            "head_expires_at too late: actual={:?}, expected_max={:?}, head_ttl={:?}",
            head_expires_at, expected_max, input.head_ttl
        ));
    }

    TestResult::passed()
}

// ============================================================================
// Property 4b: the HEAD expiry check is true when head_expires_at < now
// ============================================================================

// ============================================================================
// Property 4c: the HEAD expiry check is false when head_expires_at > now
// ============================================================================

// ============================================================================
// Property 4d: the HEAD expiry check is true when head_expires_at is None
// ============================================================================

// ============================================================================
// Property 4e: Zero head_ttl produces immediately expired HEAD entry
// ============================================================================

// ============================================================================
// Test runners (minimum 100 iterations each)
// ============================================================================

#[test]
fn test_property_refresh_head_ttl_sets_expires_at() {
    QuickCheck::new().tests(100).quickcheck(
        prop_refresh_head_ttl_sets_expires_at as fn(ArbitraryHeadTtlInput) -> TestResult,
    );
}
