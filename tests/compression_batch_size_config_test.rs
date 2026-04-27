//! Property-based tests for `CacheConfig::compression_batch_size` validation.
//!
//! **Property 5: Compression batch size validation**
//!
//! *For any* `usize` value `V`, `CacheConfig::validate()` SHALL accept the
//! config iff `65536 ≤ V ≤ 16777216` (i.e. 64 KiB to 16 MiB inclusive).
//!
//! **Validates: Requirements 2.6, 5.3, 5.4**
//!
//! WARNING: This file contains property-based tests (PBT). Running it may
//! take longer than unit tests due to random input generation.

use quickcheck::{QuickCheck, TestResult};
use s3_proxy::config::CacheConfig;

/// Lower bound (inclusive): 64 KiB.
const MIN_BATCH_SIZE: usize = 64 * 1024;
/// Upper bound (inclusive): 16 MiB.
const MAX_BATCH_SIZE: usize = 16 * 1024 * 1024;

/// Build a `CacheConfig` with only `compression_batch_size` overridden, so
/// the property isolates the field under test.
fn config_with_batch_size(v: usize) -> CacheConfig {
    CacheConfig {
        compression_batch_size: v,
        ..CacheConfig::default()
    }
}

// ============================================================================
// Property 5: Compression batch size validation
// **Validates: Requirements 2.6, 5.3, 5.4**
// ============================================================================

/// Property: `validate()` accepts iff the value is in the inclusive range
/// `[65536, 16777216]`.
fn prop_validate_iff_in_range(v: usize) -> TestResult {
    let cfg = config_with_batch_size(v);
    let accepted = cfg.validate().is_ok();
    let in_range = (MIN_BATCH_SIZE..=MAX_BATCH_SIZE).contains(&v);
    TestResult::from_bool(accepted == in_range)
}

#[test]
fn test_property_validate_iff_in_range() {
    QuickCheck::new()
        .tests(500)
        .quickcheck(prop_validate_iff_in_range as fn(usize) -> TestResult);
}

/// Property: every value inside the range is accepted.
/// Narrows the generator via `discard` so we stress the in-range arm even
/// though raw `usize` skews wildly above 16 MiB.
fn prop_in_range_accepted(v: usize) -> TestResult {
    if !(MIN_BATCH_SIZE..=MAX_BATCH_SIZE).contains(&v) {
        return TestResult::discard();
    }
    TestResult::from_bool(config_with_batch_size(v).validate().is_ok())
}

#[test]
fn test_property_in_range_accepted() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_in_range_accepted as fn(usize) -> TestResult);
}

/// Property: every value below the minimum is rejected.
fn prop_below_min_rejected(v: usize) -> TestResult {
    if v >= MIN_BATCH_SIZE {
        return TestResult::discard();
    }
    TestResult::from_bool(config_with_batch_size(v).validate().is_err())
}

#[test]
fn test_property_below_min_rejected() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_below_min_rejected as fn(usize) -> TestResult);
}

/// Property: every value above the maximum is rejected.
fn prop_above_max_rejected(v: usize) -> TestResult {
    if v <= MAX_BATCH_SIZE {
        return TestResult::discard();
    }
    TestResult::from_bool(config_with_batch_size(v).validate().is_err())
}

#[test]
fn test_property_above_max_rejected() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_above_max_rejected as fn(usize) -> TestResult);
}

// ============================================================================
// Explicit edge cases
// ============================================================================

#[test]
fn test_one_below_min_rejected() {
    // 65535 — one byte under the 64 KiB floor.
    assert!(config_with_batch_size(MIN_BATCH_SIZE - 1).validate().is_err());
}

#[test]
fn test_min_boundary_accepted() {
    // 65536 — the 64 KiB floor itself.
    assert!(config_with_batch_size(MIN_BATCH_SIZE).validate().is_ok());
}

#[test]
fn test_max_boundary_accepted() {
    // 16777216 — the 16 MiB ceiling itself.
    assert!(config_with_batch_size(MAX_BATCH_SIZE).validate().is_ok());
}

#[test]
fn test_one_above_max_rejected() {
    // 16777217 — one byte over the 16 MiB ceiling.
    assert!(config_with_batch_size(MAX_BATCH_SIZE + 1).validate().is_err());
}

#[test]
fn test_zero_rejected() {
    assert!(config_with_batch_size(0).validate().is_err());
}

#[test]
fn test_usize_max_rejected() {
    assert!(config_with_batch_size(usize::MAX).validate().is_err());
}

#[test]
fn test_default_is_accepted() {
    // The default (1 MiB) must always be a valid configuration.
    assert!(CacheConfig::default().validate().is_ok());
}
