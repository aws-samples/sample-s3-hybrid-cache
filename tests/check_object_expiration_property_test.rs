//! Property-based tests for object-level expiration check
//!
//! Property 1: Object-level expiration determines freshness
//!
//! For any `NewCacheMetadata` with `expires_at` in the past, `is_object_expired()` returns true.
//! For any `NewCacheMetadata` with `expires_at` in the future, `is_object_expired()` returns false.
//!
//! **Validates: Requirements 1.2, 2.1, 2.2, 2.3**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use std::time::{Duration, SystemTime};

/// Generate a random positive Duration between 1 second and ~1 year.
/// We avoid Duration::ZERO to ensure clear expired/non-expired distinction.
fn arbitrary_positive_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600) + 1; // 1s to ~1 year
    Duration::from_secs(secs)
}

/// Generate a random last_modified string (RFC 2822-ish format).
fn arbitrary_last_modified(g: &mut Gen) -> String {
    let day = (u32::arbitrary(g) % 28) + 1;
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let month = months[(u32::arbitrary(g) % 12) as usize];
    let year = 2020 + (u32::arbitrary(g) % 5);
    let hour = u32::arbitrary(g) % 24;
    let min = u32::arbitrary(g) % 60;
    let sec = u32::arbitrary(g) % 60;
    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} GMT",
        ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][(u32::arbitrary(g) % 7) as usize],
        day,
        month,
        year,
        hour,
        min,
        sec
    )
}

// ============================================================================
// Arbitrary input for Property 1
// ============================================================================

#[derive(Debug, Clone)]
struct ArbitraryObjectExpirationInput {
    /// Duration to offset expires_at from now (always positive)
    offset_duration: Duration,
    /// The last_modified value stored in object metadata
    last_modified: String,
}

impl Arbitrary for ArbitraryObjectExpirationInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            offset_duration: arbitrary_positive_duration(g),
            last_modified: arbitrary_last_modified(g),
        }
    }
}

/// Helper: create a NewCacheMetadata with the given expires_at.
fn create_metadata(input: &ArbitraryObjectExpirationInput, expires_at: SystemTime) -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: "test-bucket/object-expiration-test".to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag-abc123".to_string(),
            input.last_modified.clone(),
            1024,
            Some("application/octet-stream".to_string()),
        ),
        ranges: Vec::new(),
        created_at: now,
        expires_at,
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

// ============================================================================
// Property 1a: Expired metadata (expires_at in the past) → is_object_expired() returns true
// ============================================================================

fn prop_expired_object_returns_true(input: ArbitraryObjectExpirationInput) -> TestResult {
    let now = SystemTime::now();
    // expires_at in the past: now - offset_duration
    let expires_at = match now.checked_sub(input.offset_duration) {
        Some(t) => t,
        None => return TestResult::discard(),
    };

    let metadata = create_metadata(&input, expires_at);
    TestResult::from_bool(metadata.is_object_expired())
}

// ============================================================================
// Property 1b: Fresh metadata (expires_at in the future) → is_object_expired() returns false
// ============================================================================

fn prop_fresh_object_returns_false(input: ArbitraryObjectExpirationInput) -> TestResult {
    let now = SystemTime::now();
    // expires_at in the future: now + offset_duration
    let expires_at = now + input.offset_duration;

    let metadata = create_metadata(&input, expires_at);
    TestResult::from_bool(!metadata.is_object_expired())
}

// ============================================================================
// Test runners (minimum 100 iterations each)
// ============================================================================

#[test]
fn test_property_expired_object_is_detected() {
    QuickCheck::new().tests(100).quickcheck(
        prop_expired_object_returns_true as fn(ArbitraryObjectExpirationInput) -> TestResult,
    );
}

#[test]
fn test_property_fresh_object_is_not_expired() {
    QuickCheck::new().tests(100).quickcheck(
        prop_fresh_object_returns_false as fn(ArbitraryObjectExpirationInput) -> TestResult,
    );
}
