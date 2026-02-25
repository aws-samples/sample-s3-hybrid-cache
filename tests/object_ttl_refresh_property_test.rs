//! Property-based tests for object TTL refresh behavior
//!
//! Property 2: Object TTL refresh sets expires_at correctly
//!
//! For any TTL duration (including zero), calling `refresh_object_ttl(ttl)` on a
//! `NewCacheMetadata` sets `expires_at` to approximately `now + ttl` (within a
//! 1-second tolerance window).
//!
//! **Validates: Requirements 1.3, 3.1**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use std::time::{Duration, SystemTime};

/// Generate a random Duration between 0 and ~1 year.
fn arbitrary_ttl_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600 + 1);
    Duration::from_secs(secs)
}

/// Helper: create a NewCacheMetadata with default fields for testing.
fn make_test_metadata() -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: "test-bucket/object-ttl-refresh-test".to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag-abc123".to_string(),
            "Wed, 21 Oct 2020 07:28:00 GMT".to_string(),
            1024,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![],
        created_at: now,
        expires_at: now + Duration::from_secs(3600), // 1 hour TTL
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

// ============================================================================
// Arbitrary inputs for Property 2
// ============================================================================

#[derive(Debug, Clone)]
struct ArbitraryObjectTtlInput {
    /// The TTL duration to apply via refresh_object_ttl (includes Duration::ZERO)
    ttl: Duration,
}

impl Arbitrary for ArbitraryObjectTtlInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ttl: arbitrary_ttl_duration(g),
        }
    }
}

// ============================================================================
// Property 2: refresh_object_ttl sets expires_at ≈ now + ttl
// ============================================================================

/// For any TTL duration, calling refresh_object_ttl(ttl) sets expires_at
/// to approximately now + ttl (within a 1-second tolerance).
fn prop_refresh_object_ttl_sets_expires_at(input: ArbitraryObjectTtlInput) -> TestResult {
    let mut metadata = make_test_metadata();

    let before = SystemTime::now();
    metadata.refresh_object_ttl(input.ttl);
    let after = SystemTime::now();

    let expected_min = before + input.ttl;
    let expected_max = after + input.ttl + Duration::from_secs(1);

    if metadata.expires_at < expected_min {
        return TestResult::error(format!(
            "expires_at too early: actual={:?}, expected_min={:?}, ttl={:?}",
            metadata.expires_at, expected_min, input.ttl
        ));
    }
    if metadata.expires_at > expected_max {
        return TestResult::error(format!(
            "expires_at too late: actual={:?}, expected_max={:?}, ttl={:?}",
            metadata.expires_at, expected_max, input.ttl
        ));
    }

    TestResult::passed()
}

// ============================================================================
// Test runners (minimum 100 iterations each)
// ============================================================================

#[test]
fn test_property_refresh_object_ttl_sets_expires_at() {
    QuickCheck::new().tests(100).quickcheck(
        prop_refresh_object_ttl_sets_expires_at as fn(ArbitraryObjectTtlInput) -> TestResult,
    );
}
