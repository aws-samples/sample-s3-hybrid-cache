//! Property-based tests for eviction target percentage
//!
//! **Property 4: Eviction Targets Configured Percentage**
//!
//! *For any* eviction operation, the target size SHALL be calculated as
//! `max_size * (eviction_target_percent / 100)`. The eviction SHALL attempt
//! to free bytes until `current_size <= target_size`.
//!
//! **Validates: Requirements 3.4**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

// ============================================================================
// Test Data Structures
// ============================================================================

/// Represents an eviction scenario for testing target percentage logic
#[derive(Debug, Clone)]
struct EvictionScenario {
    /// Current size of the cache in bytes (before eviction)
    current_size: u64,
    /// Maximum cache size in bytes
    max_size: u64,
    /// Eviction target percentage (50-99)
    target_percent: u8,
}

impl Arbitrary for EvictionScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate max_size in a reasonable range (1KB to 100GB)
        // Use non-zero values to avoid division issues
        let max_size = (u64::arbitrary(g) % 100_000_000_000).max(1024);
        
        // Generate current_size in range from target to 2x max_size
        // (eviction only happens when cache is over threshold)
        let current_size = u64::arbitrary(g) % (max_size * 2 + 1);
        
        // Generate target_percent in valid range (50-99)
        let target_percent = (u8::arbitrary(g) % 50) + 50; // 50-99
        
        EvictionScenario {
            current_size,
            max_size,
            target_percent,
        }
    }
}

/// Represents a specific target percentage for focused testing
#[derive(Debug, Clone, Copy)]
struct TargetPercent(u8);

impl Arbitrary for TargetPercent {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate values in valid range 50-99
        let percent = (u8::arbitrary(g) % 50) + 50;
        TargetPercent(percent)
    }
}

// ============================================================================
// Eviction Target Logic (mirrors src/journal_consolidator.rs)
// ============================================================================

/// Calculates the target size for eviction.
///
/// Formula: max_size * (target_percent / 100)
///
/// This function implements the same calculation as perform_eviction_with_lock()
/// but as a pure function for property testing.
///
/// # Arguments
/// * `max_size` - Maximum cache size in bytes
/// * `target_percent` - Eviction target percentage (50-99)
///
/// # Returns
/// The target size in bytes that eviction should reduce the cache to
fn calculate_target_size(max_size: u64, target_percent: u8) -> u64 {
    // Use f64 for calculation to match the actual implementation
    // Requirement 3.4: target_size = max_size * target_percent / 100
    (max_size as f64 * (target_percent as f64 / 100.0)) as u64
}

/// Calculates the number of bytes that need to be freed during eviction.
///
/// Formula: current_size - target_size (if current_size > target_size)
///
/// # Arguments
/// * `current_size` - Current cache size in bytes
/// * `target_size` - Target size to reduce to
///
/// # Returns
/// The number of bytes to free, or 0 if current_size <= target_size
fn calculate_bytes_to_free(current_size: u64, target_size: u64) -> u64 {
    if current_size > target_size {
        current_size - target_size
    } else {
        0
    }
}

/// Determines if eviction has reached its target.
///
/// # Arguments
/// * `current_size` - Current cache size after some eviction
/// * `target_size` - Target size to reduce to
///
/// # Returns
/// true if current_size <= target_size (eviction complete)
fn eviction_target_reached(current_size: u64, target_size: u64) -> bool {
    current_size <= target_size
}

// ============================================================================
// Property 4: Eviction Targets Configured Percentage
// **Validates: Requirements 3.4**
// ============================================================================

/// Property: Target size is calculated correctly as max_size * target_percent / 100
/// **Validates: Requirements 3.4**
fn prop_target_size_calculation(max_size: u64, target_percent: TargetPercent) -> TestResult {
    // Skip edge case where max_size is 0
    if max_size == 0 {
        return TestResult::discard();
    }
    
    // Skip extremely large values that would cause floating point precision issues
    const MAX_SAFE_SIZE: u64 = 1 << 52; // ~4.5 petabytes
    if max_size > MAX_SAFE_SIZE {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(max_size, target_percent.0);
    
    // Verify using integer math (with rounding tolerance)
    let expected = (max_size as f64 * (target_percent.0 as f64 / 100.0)) as u64;
    
    // Allow for floating point rounding (should be exact or off by 1)
    let close_enough = (target_size as i64 - expected as i64).abs() <= 1;
    
    TestResult::from_bool(close_enough)
}

/// Property: Bytes to free is calculated correctly as current_size - target_size
/// **Validates: Requirements 3.4**
fn prop_bytes_to_free_calculation(scenario: EvictionScenario) -> TestResult {
    // Skip edge case where max_size is 0
    if scenario.max_size == 0 {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(scenario.max_size, scenario.target_percent);
    let bytes_to_free = calculate_bytes_to_free(scenario.current_size, target_size);
    
    if scenario.current_size > target_size {
        // Should free exactly current_size - target_size
        let expected = scenario.current_size - target_size;
        TestResult::from_bool(bytes_to_free == expected)
    } else {
        // Should free 0 bytes (already at or below target)
        TestResult::from_bool(bytes_to_free == 0)
    }
}

/// Property: Target size is always less than max_size (since target_percent is 50-99)
/// **Validates: Requirements 3.4**
fn prop_target_size_less_than_max(max_size: u64, target_percent: TargetPercent) -> TestResult {
    // Skip edge case where max_size is 0
    if max_size == 0 {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(max_size, target_percent.0);
    
    // Target percent is 50-99, so target_size should always be < max_size
    // (99% of max_size is still less than max_size)
    TestResult::from_bool(target_size < max_size)
}

/// Property: Target size is at least 50% of max_size (since target_percent >= 50)
/// **Validates: Requirements 3.4**
fn prop_target_size_at_least_half(max_size: u64, target_percent: TargetPercent) -> TestResult {
    // Skip edge case where max_size is 0
    if max_size == 0 {
        return TestResult::discard();
    }
    
    // Skip extremely large values that would cause floating point precision issues
    const MAX_SAFE_SIZE: u64 = 1 << 52; // ~4.5 petabytes
    if max_size > MAX_SAFE_SIZE {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(max_size, target_percent.0);
    let half_max = max_size / 2;
    
    // Target percent is 50-99, so target_size should be >= 50% of max_size
    // Allow for rounding (target_size should be at least half_max - 1)
    TestResult::from_bool(target_size >= half_max.saturating_sub(1))
}

/// Property: Eviction target is reached when current_size <= target_size
/// **Validates: Requirements 3.4**
fn prop_eviction_target_reached(scenario: EvictionScenario) -> TestResult {
    if scenario.max_size == 0 {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(scenario.max_size, scenario.target_percent);
    let reached = eviction_target_reached(scenario.current_size, target_size);
    
    // Verify the condition matches
    let expected = scenario.current_size <= target_size;
    TestResult::from_bool(reached == expected)
}

/// Property: Target size calculation is deterministic
/// **Validates: Requirements 3.4**
fn prop_target_calculation_deterministic(max_size: u64, target_percent: TargetPercent) -> TestResult {
    if max_size == 0 {
        return TestResult::discard();
    }
    
    let target1 = calculate_target_size(max_size, target_percent.0);
    let target2 = calculate_target_size(max_size, target_percent.0);
    
    TestResult::from_bool(target1 == target2)
}

/// Property: Higher target_percent means higher target_size (less aggressive eviction)
/// **Validates: Requirements 3.4**
fn prop_higher_percent_higher_target(max_size: u64) -> TestResult {
    // Skip edge cases
    if max_size < 100 {
        return TestResult::discard();
    }
    
    let target_50 = calculate_target_size(max_size, 50);
    let target_60 = calculate_target_size(max_size, 60);
    let target_70 = calculate_target_size(max_size, 70);
    let target_80 = calculate_target_size(max_size, 80);
    let target_90 = calculate_target_size(max_size, 90);
    let target_99 = calculate_target_size(max_size, 99);
    
    // Higher percentage should result in higher target (less eviction)
    let monotonic = target_50 <= target_60
        && target_60 <= target_70
        && target_70 <= target_80
        && target_80 <= target_90
        && target_90 <= target_99;
    
    TestResult::from_bool(monotonic)
}

/// Property: Default target percent (80%) targets 80% of max_size
/// **Validates: Requirements 3.4**
fn prop_default_target_percent(max_size: u64) -> TestResult {
    // Skip edge cases
    if max_size < 100 {
        return TestResult::discard();
    }
    
    let default_target_percent = 80u8;
    let target = calculate_target_size(max_size, default_target_percent);
    
    // Target should be approximately 80% of max_size
    let expected = (max_size as f64 * 0.80) as u64;
    
    // Allow for floating point rounding
    let close_enough = (target as i64 - expected as i64).abs() <= 1;
    
    TestResult::from_bool(close_enough)
}

/// Property: Bytes to free is non-negative
/// **Validates: Requirements 3.4**
fn prop_bytes_to_free_non_negative(scenario: EvictionScenario) -> TestResult {
    if scenario.max_size == 0 {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(scenario.max_size, scenario.target_percent);
    let bytes_to_free = calculate_bytes_to_free(scenario.current_size, target_size);
    
    // bytes_to_free is u64, so it's always >= 0, but verify the logic is correct
    // (no underflow when current_size < target_size)
    TestResult::from_bool(bytes_to_free <= scenario.current_size)
}

/// Property: After freeing bytes_to_free, current_size should be at target
/// **Validates: Requirements 3.4**
fn prop_after_eviction_at_target(scenario: EvictionScenario) -> TestResult {
    if scenario.max_size == 0 {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(scenario.max_size, scenario.target_percent);
    let bytes_to_free = calculate_bytes_to_free(scenario.current_size, target_size);
    
    // Simulate eviction: new_size = current_size - bytes_to_free
    let new_size = scenario.current_size.saturating_sub(bytes_to_free);
    
    // After eviction, should be at or below target
    TestResult::from_bool(new_size <= target_size)
}

/// Property: Combined test - all eviction target invariants hold
/// **Validates: Requirements 3.4**
fn prop_all_eviction_target_invariants(scenario: EvictionScenario) -> TestResult {
    if scenario.max_size == 0 {
        return TestResult::discard();
    }
    
    let target_size = calculate_target_size(scenario.max_size, scenario.target_percent);
    let bytes_to_free = calculate_bytes_to_free(scenario.current_size, target_size);
    
    // Invariant 1: target_size < max_size (since target_percent is 50-99)
    if target_size >= scenario.max_size {
        return TestResult::failed();
    }
    
    // Invariant 2: target_size >= 50% of max_size
    let half_max = scenario.max_size / 2;
    if target_size < half_max.saturating_sub(1) {
        return TestResult::failed();
    }
    
    // Invariant 3: bytes_to_free is correct
    if scenario.current_size > target_size {
        if bytes_to_free != scenario.current_size - target_size {
            return TestResult::failed();
        }
    } else if bytes_to_free != 0 {
        return TestResult::failed();
    }
    
    // Invariant 4: After eviction, at or below target
    let new_size = scenario.current_size.saturating_sub(bytes_to_free);
    if new_size > target_size {
        return TestResult::failed();
    }
    
    TestResult::passed()
}

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn test_property_target_size_calculation() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_size_calculation as fn(u64, TargetPercent) -> TestResult);
}

#[test]
fn test_property_bytes_to_free_calculation() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_bytes_to_free_calculation as fn(EvictionScenario) -> TestResult);
}

#[test]
fn test_property_target_size_less_than_max() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_size_less_than_max as fn(u64, TargetPercent) -> TestResult);
}

#[test]
fn test_property_target_size_at_least_half() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_size_at_least_half as fn(u64, TargetPercent) -> TestResult);
}

#[test]
fn test_property_eviction_target_reached() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_eviction_target_reached as fn(EvictionScenario) -> TestResult);
}

#[test]
fn test_property_target_calculation_deterministic() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_calculation_deterministic as fn(u64, TargetPercent) -> TestResult);
}

#[test]
fn test_property_higher_percent_higher_target() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_higher_percent_higher_target as fn(u64) -> TestResult);
}

#[test]
fn test_property_default_target_percent() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_default_target_percent as fn(u64) -> TestResult);
}

#[test]
fn test_property_bytes_to_free_non_negative() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_bytes_to_free_non_negative as fn(EvictionScenario) -> TestResult);
}

#[test]
fn test_property_after_eviction_at_target() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_after_eviction_at_target as fn(EvictionScenario) -> TestResult);
}

#[test]
fn test_property_all_eviction_target_invariants() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_all_eviction_target_invariants as fn(EvictionScenario) -> TestResult);
}

// ============================================================================
// Unit tests for specific edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_default_target_80_percent() {
        // Default: target at 80% of max_size
        let max_size = 10_000_000_000u64; // 10GB
        let target = calculate_target_size(max_size, 80);
        
        // 80% of 10GB = 8GB
        let expected = 8_000_000_000u64;
        assert_eq!(target, expected);
    }

    #[test]
    fn test_target_50_percent() {
        let max_size = 10_000_000_000u64; // 10GB
        let target = calculate_target_size(max_size, 50);
        
        // 50% of 10GB = 5GB
        let expected = 5_000_000_000u64;
        assert_eq!(target, expected);
    }

    #[test]
    fn test_target_99_percent() {
        let max_size = 10_000_000_000u64; // 10GB
        let target = calculate_target_size(max_size, 99);
        
        // 99% of 10GB = 9.9GB
        let expected = 9_900_000_000u64;
        assert_eq!(target, expected);
    }

    #[test]
    fn test_bytes_to_free_when_over_target() {
        let max_size = 10_000_000_000u64; // 10GB
        let current_size = 9_500_000_000u64; // 9.5GB
        let target_percent = 80u8;
        
        let target_size = calculate_target_size(max_size, target_percent);
        // target_size = 8GB
        assert_eq!(target_size, 8_000_000_000);
        
        let bytes_to_free = calculate_bytes_to_free(current_size, target_size);
        // Need to free 9.5GB - 8GB = 1.5GB
        assert_eq!(bytes_to_free, 1_500_000_000);
    }

    #[test]
    fn test_bytes_to_free_when_at_target() {
        let max_size = 10_000_000_000u64;
        let target_percent = 80u8;
        let target_size = calculate_target_size(max_size, target_percent);
        
        // Current size exactly at target
        let current_size = target_size;
        let bytes_to_free = calculate_bytes_to_free(current_size, target_size);
        
        // Should free 0 bytes
        assert_eq!(bytes_to_free, 0);
    }

    #[test]
    fn test_bytes_to_free_when_below_target() {
        let max_size = 10_000_000_000u64;
        let target_percent = 80u8;
        let target_size = calculate_target_size(max_size, target_percent);
        
        // Current size below target
        let current_size = 5_000_000_000u64; // 5GB < 8GB target
        let bytes_to_free = calculate_bytes_to_free(current_size, target_size);
        
        // Should free 0 bytes
        assert_eq!(bytes_to_free, 0);
    }

    #[test]
    fn test_eviction_target_reached_true() {
        let target_size = 8_000_000_000u64;
        
        // At target
        assert!(eviction_target_reached(target_size, target_size));
        
        // Below target
        assert!(eviction_target_reached(7_000_000_000, target_size));
        
        // Way below target
        assert!(eviction_target_reached(0, target_size));
    }

    #[test]
    fn test_eviction_target_reached_false() {
        let target_size = 8_000_000_000u64;
        
        // Above target
        assert!(!eviction_target_reached(8_000_000_001, target_size));
        
        // Way above target
        assert!(!eviction_target_reached(10_000_000_000, target_size));
    }

    #[test]
    fn test_target_always_less_than_max() {
        let max_size = 10_000_000_000u64;
        
        // Test all valid target percentages (50-99)
        for percent in 50..=99 {
            let target = calculate_target_size(max_size, percent);
            assert!(
                target < max_size,
                "Target {} should be less than max {} for percent {}",
                target,
                max_size,
                percent
            );
        }
    }

    #[test]
    fn test_small_cache_target_calculation() {
        // Test with small cache to verify no overflow issues
        let max_size = 1000u64; // 1KB
        let target = calculate_target_size(max_size, 80);
        
        // 80% of 1000 = 800
        assert_eq!(target, 800);
    }

    #[test]
    fn test_large_cache_target_calculation() {
        // Test with large cache (100TB)
        let max_size = 100_000_000_000_000u64; // 100TB
        let target = calculate_target_size(max_size, 80);
        
        // 80% of 100TB = 80TB
        let expected = 80_000_000_000_000u64;
        assert_eq!(target, expected);
    }

    #[test]
    fn test_real_world_scenario_10gb_cache() {
        // Simulate a 10GB cache with 80% target
        let max_size = 10_737_418_240u64; // 10GB in bytes
        let target_percent = 80u8;
        let target_size = calculate_target_size(max_size, target_percent);
        
        // 80% of 10GB ≈ 8GB
        let expected_target = 8_589_934_592u64; // 8GB in bytes
        assert_eq!(target_size, expected_target);
        
        // If current size is 9.5GB, need to free 1.5GB
        let current_size = 10_200_547_328u64; // ~9.5GB
        let bytes_to_free = calculate_bytes_to_free(current_size, target_size);
        
        // Should free ~1.5GB
        let expected_to_free = current_size - target_size;
        assert_eq!(bytes_to_free, expected_to_free);
    }

    #[test]
    fn test_comparison_with_old_80_percent_hardcoded() {
        // Old behavior: target = max_size * 0.8 (hardcoded)
        // New behavior: target = max_size * target_percent / 100 (configurable)
        
        let max_size = 10_000_000_000u64; // 10GB
        
        // Old hardcoded behavior (80%)
        let old_target = (max_size as f64 * 0.8) as u64;
        
        // New configurable behavior with default (80%)
        let new_target = calculate_target_size(max_size, 80);
        
        // Should be identical
        assert_eq!(old_target, new_target);
        
        // But now we can configure different targets
        let aggressive_target = calculate_target_size(max_size, 50); // 50%
        let conservative_target = calculate_target_size(max_size, 90); // 90%
        
        assert_eq!(aggressive_target, 5_000_000_000); // 5GB
        assert_eq!(conservative_target, 9_000_000_000); // 9GB
    }

    #[test]
    fn test_target_monotonicity() {
        let max_size = 10_000_000u64;
        
        // Verify targets increase with percentage
        let t50 = calculate_target_size(max_size, 50);
        let t60 = calculate_target_size(max_size, 60);
        let t70 = calculate_target_size(max_size, 70);
        let t80 = calculate_target_size(max_size, 80);
        let t90 = calculate_target_size(max_size, 90);
        let t99 = calculate_target_size(max_size, 99);
        
        assert!(t50 < t60);
        assert!(t60 < t70);
        assert!(t70 < t80);
        assert!(t80 < t90);
        assert!(t90 < t99);
    }

    #[test]
    fn test_eviction_simulation() {
        // Simulate a complete eviction cycle
        let max_size = 10_000_000_000u64; // 10GB
        let target_percent = 80u8;
        let mut current_size = 9_800_000_000u64; // 9.8GB (over 95% trigger)
        
        let target_size = calculate_target_size(max_size, target_percent);
        assert_eq!(target_size, 8_000_000_000); // 8GB
        
        let bytes_to_free = calculate_bytes_to_free(current_size, target_size);
        assert_eq!(bytes_to_free, 1_800_000_000); // 1.8GB to free
        
        // Simulate eviction
        current_size -= bytes_to_free;
        
        // Verify target reached
        assert!(eviction_target_reached(current_size, target_size));
        assert_eq!(current_size, target_size);
    }

    #[test]
    fn test_boundary_one_byte_difference() {
        let max_size = 1_000_000u64; // 1MB
        let target_percent = 80u8;
        let target_size = calculate_target_size(max_size, target_percent);
        
        // At target: reached
        assert!(eviction_target_reached(target_size, target_size));
        
        // One byte above: not reached
        assert!(!eviction_target_reached(target_size + 1, target_size));
        
        // One byte below: reached
        assert!(eviction_target_reached(target_size - 1, target_size));
    }

    #[test]
    fn test_zero_current_size() {
        let max_size = 10_000_000_000u64;
        let target_percent = 80u8;
        let target_size = calculate_target_size(max_size, target_percent);
        
        // Empty cache
        let current_size = 0u64;
        let bytes_to_free = calculate_bytes_to_free(current_size, target_size);
        
        // Should free 0 bytes (already below target)
        assert_eq!(bytes_to_free, 0);
        assert!(eviction_target_reached(current_size, target_size));
    }
}
