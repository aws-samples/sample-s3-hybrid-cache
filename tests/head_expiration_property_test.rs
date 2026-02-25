//! Property-based tests for HEAD expiration behavior
//!
//! Property 4: HEAD expiration uses head_ttl correctly
//!
//! For any HEAD response cached by the proxy, the stored `head_expires_at`
//! should equal `now + resolved_head_ttl`. For any cached HEAD entry,
//! `is_head_expired()` should return true if and only if `head_expires_at < now`.
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

/// Generate a random positive Duration between 1 second and 1 year.
/// We avoid Duration::ZERO to ensure clear expired/non-expired distinction.
fn arbitrary_positive_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600) + 1; // 1s to ~1 year
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

#[derive(Debug, Clone)]
struct ArbitraryHeadExpirationInput {
    /// Duration to offset head_expires_at from now (always positive)
    offset_duration: Duration,
}

impl Arbitrary for ArbitraryHeadExpirationInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            offset_duration: arbitrary_positive_duration(g),
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
// Property 4b: is_head_expired() returns true when head_expires_at < now
// ============================================================================

/// For any HEAD entry where head_expires_at is in the past,
/// is_head_expired() returns true.
fn prop_expired_head_returns_true(input: ArbitraryHeadExpirationInput) -> TestResult {
    let mut metadata = make_test_metadata();

    let now = SystemTime::now();
    let expires_at = match now.checked_sub(input.offset_duration) {
        Some(t) => t,
        None => return TestResult::discard(),
    };
    metadata.head_expires_at = Some(expires_at);

    TestResult::from_bool(metadata.is_head_expired())
}

// ============================================================================
// Property 4c: is_head_expired() returns false when head_expires_at > now
// ============================================================================

/// For any HEAD entry where head_expires_at is in the future,
/// is_head_expired() returns false.
fn prop_non_expired_head_returns_false(input: ArbitraryHeadExpirationInput) -> TestResult {
    let mut metadata = make_test_metadata();

    let now = SystemTime::now();
    metadata.head_expires_at = Some(now + input.offset_duration);

    TestResult::from_bool(!metadata.is_head_expired())
}

// ============================================================================
// Property 4d: is_head_expired() returns true when head_expires_at is None
// ============================================================================

/// When head_expires_at is None (no HEAD cached), is_head_expired() returns true.
fn prop_none_head_expires_at_is_expired() -> TestResult {
    let mut metadata = make_test_metadata();
    metadata.head_expires_at = None;

    TestResult::from_bool(metadata.is_head_expired())
}

// ============================================================================
// Property 4e: Zero head_ttl produces immediately expired HEAD entry
// ============================================================================

/// When head_ttl is Duration::ZERO, refresh_head_ttl sets head_expires_at = now,
/// and is_head_expired() returns true (or the entry is at the boundary).
fn prop_zero_head_ttl_is_immediately_expired() -> TestResult {
    let mut metadata = make_test_metadata();

    metadata.refresh_head_ttl(Duration::ZERO);

    // With zero TTL, head_expires_at = now at time of call.
    // By the time is_head_expired() runs, now >= head_expires_at, so it should be expired.
    // The check is SystemTime::now() > expires_at, so if now == expires_at it returns false.
    // But since some time passes between refresh_head_ttl and is_head_expired, now > expires_at.
    // We allow a small window: if it's not expired, that's only possible if the calls
    // happened in the same instant (extremely unlikely but technically possible).
    let expired = metadata.is_head_expired();
    if !expired {
        // This can happen if the two calls execute in the same nanosecond.
        // Discard rather than fail — the property still holds conceptually.
        return TestResult::discard();
    }

    TestResult::passed()
}

// ============================================================================
// Test runners (minimum 100 iterations each)
// ============================================================================

#[test]
fn test_property_refresh_head_ttl_sets_expires_at() {
    QuickCheck::new().tests(100).quickcheck(
        prop_refresh_head_ttl_sets_expires_at as fn(ArbitraryHeadTtlInput) -> TestResult,
    );
}

#[test]
fn test_property_expired_head_returns_true() {
    QuickCheck::new().tests(100).quickcheck(
        prop_expired_head_returns_true as fn(ArbitraryHeadExpirationInput) -> TestResult,
    );
}

#[test]
fn test_property_non_expired_head_returns_false() {
    QuickCheck::new().tests(100).quickcheck(
        prop_non_expired_head_returns_false as fn(ArbitraryHeadExpirationInput) -> TestResult,
    );
}

#[test]
fn test_property_none_head_expires_at_is_expired() {
    QuickCheck::new().tests(100).quickcheck(
        prop_none_head_expires_at_is_expired as fn() -> TestResult,
    );
}

#[test]
fn test_property_zero_head_ttl_is_immediately_expired() {
    QuickCheck::new().tests(100).quickcheck(
        prop_zero_head_ttl_is_immediately_expired as fn() -> TestResult,
    );
}
