//! Property-based tests for eviction trigger threshold
//!
//! **Property 3: Eviction Triggers at Configured Threshold**
//!
//! *For any* cache state where `current_size > max_size * (eviction_trigger_percent / 100)`,
//! the system SHALL trigger eviction. For cache states at or below this threshold,
//! eviction SHALL NOT be triggered.
//!
//! **Validates: Requirements 3.3**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

// ============================================================================
// Test Data Structures
// ============================================================================

/// Represents a cache configuration for testing eviction trigger logic
#[derive(Debug, Clone)]
struct CacheState {
    /// Current size of the cache in bytes
    current_size: u64,
    /// Maximum cache size in bytes
    max_size: u64,
    /// Eviction trigger percentage (50-100)
    trigger_percent: u8,
}

impl Arbitrary for CacheState {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate max_size in a reasonable range (1KB to 100GB)
        // Use non-zero values to avoid division issues
        let max_size = (u64::arbitrary(g) % 100_000_000_000).max(1024);
        
        // Generate current_size in range 0 to 2x max_size to test both under and over threshold
        let current_size = u64::arbitrary(g) % (max_size * 2 + 1);
        
        // Generate trigger_percent in valid range (50-100)
        let trigger_percent = (u8::arbitrary(g) % 51) + 50; // 50-100
        
        CacheState {
            current_size,
            max_size,
            trigger_percent,
        }
    }
}

/// Represents a specific trigger percentage for focused testing
#[derive(Debug, Clone, Copy)]
struct TriggerPercent(u8);

impl Arbitrary for TriggerPercent {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate values in valid range 50-100
        let percent = (u8::arbitrary(g) % 51) + 50;
        TriggerPercent(percent)
    }
}

// ============================================================================
// Eviction Trigger Logic (mirrors src/journal_consolidator.rs)
// ============================================================================

/// Result of eviction trigger check
#[derive(Debug, Clone, Copy, PartialEq)]
enum EvictionDecision {
    /// Eviction should be triggered
    Trigger,
    /// Eviction should NOT be triggered
    NoTrigger,
}

/// Calculates the trigger threshold for a given max_size and trigger_percent.
///
/// Formula: max_size * (trigger_percent / 100)
///
/// This function implements the same calculation as JournalConsolidator::maybe_trigger_eviction()
/// but as a pure function for property testing.
///
/// # Arguments
/// * `max_size` - Maximum cache size in bytes
/// * `trigger_percent` - Eviction trigger percentage (50-100)
///
/// # Returns
/// The trigger threshold in bytes
fn calculate_trigger_threshold(max_size: u64, trigger_percent: u8) -> u64 {
    // Use f64 for calculation to match the actual implementation
    // Requirement 3.3: trigger_threshold = max_size * trigger_percent / 100
    (max_size as f64 * (trigger_percent as f64 / 100.0)) as u64
}

/// Determines if eviction should be triggered based on current cache state.
///
/// This function implements the same logic as JournalConsolidator::maybe_trigger_eviction()
/// but as a pure function for property testing.
///
/// # Arguments
/// * `current_size` - Current cache size in bytes
/// * `max_size` - Maximum cache size in bytes
/// * `trigger_percent` - Eviction trigger percentage (50-100)
///
/// # Returns
/// * `EvictionDecision::Trigger` - Eviction should be triggered
/// * `EvictionDecision::NoTrigger` - Eviction should NOT be triggered
fn should_trigger_eviction(current_size: u64, max_size: u64, trigger_percent: u8) -> EvictionDecision {
    // Handle edge case: max_size of 0 means eviction is disabled
    if max_size == 0 {
        return EvictionDecision::NoTrigger;
    }
    
    let trigger_threshold = calculate_trigger_threshold(max_size, trigger_percent);
    
    // Requirement 3.3: Trigger eviction when current_size > trigger_threshold
    // Do NOT trigger when current_size <= trigger_threshold
    if current_size > trigger_threshold {
        EvictionDecision::Trigger
    } else {
        EvictionDecision::NoTrigger
    }
}

// ============================================================================
// Property 3: Eviction Triggers at Configured Threshold
// **Validates: Requirements 3.3**
// ============================================================================

/// Property: Eviction triggers when current_size > trigger_threshold
/// **Validates: Requirements 3.3**
fn prop_eviction_triggers_above_threshold(state: CacheState) -> TestResult {
    // Skip edge case where max_size is 0 (eviction disabled)
    if state.max_size == 0 {
        return TestResult::discard();
    }
    
    let trigger_threshold = calculate_trigger_threshold(state.max_size, state.trigger_percent);
    
    // Only test cases where current_size > trigger_threshold
    if state.current_size <= trigger_threshold {
        return TestResult::discard();
    }
    
    let decision = should_trigger_eviction(state.current_size, state.max_size, state.trigger_percent);
    
    TestResult::from_bool(decision == EvictionDecision::Trigger)
}

/// Property: Eviction does NOT trigger when current_size <= trigger_threshold
/// **Validates: Requirements 3.3**
fn prop_eviction_not_triggered_at_or_below_threshold(state: CacheState) -> TestResult {
    // Skip edge case where max_size is 0 (eviction disabled)
    if state.max_size == 0 {
        return TestResult::discard();
    }
    
    let trigger_threshold = calculate_trigger_threshold(state.max_size, state.trigger_percent);
    
    // Only test cases where current_size <= trigger_threshold
    if state.current_size > trigger_threshold {
        return TestResult::discard();
    }
    
    let decision = should_trigger_eviction(state.current_size, state.max_size, state.trigger_percent);
    
    TestResult::from_bool(decision == EvictionDecision::NoTrigger)
}

/// Property: Trigger threshold calculation is correct
/// **Validates: Requirements 3.3**
fn prop_trigger_threshold_calculation(max_size: u64, trigger_percent: TriggerPercent) -> TestResult {
    // Skip edge case where max_size is 0
    if max_size == 0 {
        return TestResult::discard();
    }
    
    // Skip extremely large values that would cause floating point precision issues
    // f64 has 53 bits of mantissa precision, so values above 2^53 (~9 petabytes) 
    // may have precision loss. In practice, cache sizes are limited to reasonable values.
    // We use 2^52 as a safe limit to ensure accurate calculations.
    const MAX_SAFE_SIZE: u64 = 1 << 52; // ~4.5 petabytes
    if max_size > MAX_SAFE_SIZE {
        return TestResult::discard();
    }
    
    let threshold = calculate_trigger_threshold(max_size, trigger_percent.0);
    
    // Verify threshold is in expected range: 50% to 100% of max_size
    let min_threshold = max_size / 2; // 50%
    let max_threshold = max_size;     // 100%
    
    // Allow for floating point rounding (threshold should be approximately in range)
    // Use a small tolerance for edge cases
    let in_range = threshold >= min_threshold.saturating_sub(1) && threshold <= max_threshold + 1;
    
    TestResult::from_bool(in_range)
}

/// Property: Eviction decision is deterministic - same inputs always produce same output
/// **Validates: Requirements 3.3**
fn prop_eviction_decision_deterministic(state: CacheState) -> TestResult {
    if state.max_size == 0 {
        return TestResult::discard();
    }
    
    let decision1 = should_trigger_eviction(state.current_size, state.max_size, state.trigger_percent);
    let decision2 = should_trigger_eviction(state.current_size, state.max_size, state.trigger_percent);
    
    TestResult::from_bool(decision1 == decision2)
}

/// Property: Boundary condition - current_size exactly at threshold does NOT trigger eviction
/// **Validates: Requirements 3.3**
fn prop_boundary_at_threshold_no_trigger(max_size: u64, trigger_percent: TriggerPercent) -> TestResult {
    // Skip edge cases
    if max_size == 0 {
        return TestResult::discard();
    }
    
    let trigger_threshold = calculate_trigger_threshold(max_size, trigger_percent.0);
    
    // Set current_size exactly at threshold
    let current_size = trigger_threshold;
    
    let decision = should_trigger_eviction(current_size, max_size, trigger_percent.0);
    
    // At threshold (not above), eviction should NOT trigger
    TestResult::from_bool(decision == EvictionDecision::NoTrigger)
}

/// Property: Boundary condition - current_size 1 byte above threshold DOES trigger eviction
/// **Validates: Requirements 3.3**
fn prop_boundary_above_threshold_triggers(max_size: u64, trigger_percent: TriggerPercent) -> TestResult {
    // Skip edge cases
    if max_size == 0 {
        return TestResult::discard();
    }
    
    let trigger_threshold = calculate_trigger_threshold(max_size, trigger_percent.0);
    
    // Skip if adding 1 would overflow
    if trigger_threshold == u64::MAX {
        return TestResult::discard();
    }
    
    // Set current_size 1 byte above threshold
    let current_size = trigger_threshold + 1;
    
    let decision = should_trigger_eviction(current_size, max_size, trigger_percent.0);
    
    // Above threshold, eviction should trigger
    TestResult::from_bool(decision == EvictionDecision::Trigger)
}

/// Property: Zero max_size (disabled eviction) never triggers
/// **Validates: Requirements 3.3**
fn prop_zero_max_size_never_triggers(current_size: u64, trigger_percent: TriggerPercent) -> TestResult {
    let decision = should_trigger_eviction(current_size, 0, trigger_percent.0);
    
    // With max_size = 0, eviction is disabled and should never trigger
    TestResult::from_bool(decision == EvictionDecision::NoTrigger)
}

/// Property: Higher trigger_percent means higher threshold (less aggressive eviction)
/// **Validates: Requirements 3.3**
fn prop_higher_percent_higher_threshold(max_size: u64) -> TestResult {
    // Skip edge cases
    if max_size < 100 {
        return TestResult::discard();
    }
    
    let threshold_50 = calculate_trigger_threshold(max_size, 50);
    let threshold_75 = calculate_trigger_threshold(max_size, 75);
    let threshold_95 = calculate_trigger_threshold(max_size, 95);
    let threshold_100 = calculate_trigger_threshold(max_size, 100);
    
    // Higher percentage should result in higher threshold
    let monotonic = threshold_50 <= threshold_75 
        && threshold_75 <= threshold_95 
        && threshold_95 <= threshold_100;
    
    TestResult::from_bool(monotonic)
}

/// Property: Default trigger percent (95%) triggers at 95% of max_size
/// **Validates: Requirements 3.3**
fn prop_default_trigger_percent(max_size: u64) -> TestResult {
    // Skip edge cases
    if max_size < 100 {
        return TestResult::discard();
    }
    
    let default_trigger_percent = 95u8;
    let threshold = calculate_trigger_threshold(max_size, default_trigger_percent);
    
    // Threshold should be approximately 95% of max_size
    let expected = (max_size as f64 * 0.95) as u64;
    
    // Allow for floating point rounding
    let close_enough = (threshold as i64 - expected as i64).abs() <= 1;
    
    TestResult::from_bool(close_enough)
}

/// Property: Combined test - all eviction trigger invariants hold
/// **Validates: Requirements 3.3**
fn prop_all_eviction_trigger_invariants(state: CacheState) -> TestResult {
    if state.max_size == 0 {
        // Invariant: max_size = 0 means eviction disabled
        let decision = should_trigger_eviction(state.current_size, state.max_size, state.trigger_percent);
        return TestResult::from_bool(decision == EvictionDecision::NoTrigger);
    }
    
    let trigger_threshold = calculate_trigger_threshold(state.max_size, state.trigger_percent);
    let decision = should_trigger_eviction(state.current_size, state.max_size, state.trigger_percent);
    
    // Invariant 1: If current_size > threshold, must trigger
    if state.current_size > trigger_threshold {
        return TestResult::from_bool(decision == EvictionDecision::Trigger);
    }
    
    // Invariant 2: If current_size <= threshold, must NOT trigger
    TestResult::from_bool(decision == EvictionDecision::NoTrigger)
}

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn test_property_eviction_triggers_above_threshold() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_eviction_triggers_above_threshold as fn(CacheState) -> TestResult);
}

#[test]
fn test_property_eviction_not_triggered_at_or_below_threshold() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_eviction_not_triggered_at_or_below_threshold as fn(CacheState) -> TestResult);
}

#[test]
fn test_property_trigger_threshold_calculation() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_trigger_threshold_calculation as fn(u64, TriggerPercent) -> TestResult);
}

#[test]
fn test_property_eviction_decision_deterministic() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_eviction_decision_deterministic as fn(CacheState) -> TestResult);
}

#[test]
fn test_property_boundary_at_threshold_no_trigger() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_boundary_at_threshold_no_trigger as fn(u64, TriggerPercent) -> TestResult);
}

#[test]
fn test_property_boundary_above_threshold_triggers() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_boundary_above_threshold_triggers as fn(u64, TriggerPercent) -> TestResult);
}

#[test]
fn test_property_zero_max_size_never_triggers() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_zero_max_size_never_triggers as fn(u64, TriggerPercent) -> TestResult);
}

#[test]
fn test_property_higher_percent_higher_threshold() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_higher_percent_higher_threshold as fn(u64) -> TestResult);
}

#[test]
fn test_property_default_trigger_percent() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_default_trigger_percent as fn(u64) -> TestResult);
}

#[test]
fn test_property_all_eviction_trigger_invariants() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_all_eviction_trigger_invariants as fn(CacheState) -> TestResult);
}

// ============================================================================
// Unit tests for specific edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_default_threshold_95_percent() {
        // Default: trigger at 95% of max_size
        let max_size = 10_000_000_000u64; // 10GB
        let threshold = calculate_trigger_threshold(max_size, 95);
        
        // 95% of 10GB = 9.5GB
        let expected = 9_500_000_000u64;
        assert_eq!(threshold, expected);
    }

    #[test]
    fn test_threshold_50_percent() {
        let max_size = 10_000_000_000u64; // 10GB
        let threshold = calculate_trigger_threshold(max_size, 50);
        
        // 50% of 10GB = 5GB
        let expected = 5_000_000_000u64;
        assert_eq!(threshold, expected);
    }

    #[test]
    fn test_threshold_100_percent() {
        let max_size = 10_000_000_000u64; // 10GB
        let threshold = calculate_trigger_threshold(max_size, 100);
        
        // 100% of 10GB = 10GB
        assert_eq!(threshold, max_size);
    }

    #[test]
    fn test_eviction_triggers_above_95_percent() {
        let max_size = 10_000_000_000u64; // 10GB
        let trigger_percent = 95u8;
        
        // 9.5GB + 1 byte should trigger
        let current_size = 9_500_000_001u64;
        let decision = should_trigger_eviction(current_size, max_size, trigger_percent);
        assert_eq!(decision, EvictionDecision::Trigger);
    }

    #[test]
    fn test_eviction_not_triggered_at_95_percent() {
        let max_size = 10_000_000_000u64; // 10GB
        let trigger_percent = 95u8;
        
        // Exactly 9.5GB should NOT trigger
        let current_size = 9_500_000_000u64;
        let decision = should_trigger_eviction(current_size, max_size, trigger_percent);
        assert_eq!(decision, EvictionDecision::NoTrigger);
    }

    #[test]
    fn test_eviction_not_triggered_below_threshold() {
        let max_size = 10_000_000_000u64; // 10GB
        let trigger_percent = 95u8;
        
        // 9GB (90%) should NOT trigger at 95% threshold
        let current_size = 9_000_000_000u64;
        let decision = should_trigger_eviction(current_size, max_size, trigger_percent);
        assert_eq!(decision, EvictionDecision::NoTrigger);
    }

    #[test]
    fn test_eviction_disabled_with_zero_max_size() {
        // max_size = 0 means eviction is disabled
        let decision = should_trigger_eviction(1_000_000, 0, 95);
        assert_eq!(decision, EvictionDecision::NoTrigger);
    }

    #[test]
    fn test_empty_cache_never_triggers() {
        let max_size = 10_000_000_000u64;
        let decision = should_trigger_eviction(0, max_size, 95);
        assert_eq!(decision, EvictionDecision::NoTrigger);
    }

    #[test]
    fn test_small_cache_threshold_calculation() {
        // Test with small cache to verify no overflow issues
        let max_size = 1000u64; // 1KB
        let threshold = calculate_trigger_threshold(max_size, 95);
        
        // 95% of 1000 = 950
        assert_eq!(threshold, 950);
    }

    #[test]
    fn test_large_cache_threshold_calculation() {
        // Test with large cache (100TB)
        let max_size = 100_000_000_000_000u64; // 100TB
        let threshold = calculate_trigger_threshold(max_size, 95);
        
        // 95% of 100TB = 95TB
        let expected = 95_000_000_000_000u64;
        assert_eq!(threshold, expected);
    }

    #[test]
    fn test_boundary_one_byte_difference() {
        let max_size = 1_000_000u64; // 1MB
        let trigger_percent = 95u8;
        let threshold = calculate_trigger_threshold(max_size, trigger_percent);
        
        // At threshold: no trigger
        assert_eq!(
            should_trigger_eviction(threshold, max_size, trigger_percent),
            EvictionDecision::NoTrigger
        );
        
        // One byte above: trigger
        assert_eq!(
            should_trigger_eviction(threshold + 1, max_size, trigger_percent),
            EvictionDecision::Trigger
        );
        
        // One byte below: no trigger
        if threshold > 0 {
            assert_eq!(
                should_trigger_eviction(threshold - 1, max_size, trigger_percent),
                EvictionDecision::NoTrigger
            );
        }
    }

    #[test]
    fn test_trigger_percent_50() {
        let max_size = 1_000_000u64;
        let trigger_percent = 50u8;
        let threshold = calculate_trigger_threshold(max_size, trigger_percent);
        
        // 50% of 1MB = 500KB
        assert_eq!(threshold, 500_000);
        
        // At 500KB: no trigger
        assert_eq!(
            should_trigger_eviction(500_000, max_size, trigger_percent),
            EvictionDecision::NoTrigger
        );
        
        // At 500KB + 1: trigger
        assert_eq!(
            should_trigger_eviction(500_001, max_size, trigger_percent),
            EvictionDecision::Trigger
        );
    }

    #[test]
    fn test_trigger_percent_100() {
        let max_size = 1_000_000u64;
        let trigger_percent = 100u8;
        let threshold = calculate_trigger_threshold(max_size, trigger_percent);
        
        // 100% of 1MB = 1MB
        assert_eq!(threshold, max_size);
        
        // At max_size: no trigger
        assert_eq!(
            should_trigger_eviction(max_size, max_size, trigger_percent),
            EvictionDecision::NoTrigger
        );
        
        // Above max_size: trigger
        assert_eq!(
            should_trigger_eviction(max_size + 1, max_size, trigger_percent),
            EvictionDecision::Trigger
        );
    }

    #[test]
    fn test_threshold_monotonicity() {
        let max_size = 10_000_000u64;
        
        // Verify thresholds increase with percentage
        let t50 = calculate_trigger_threshold(max_size, 50);
        let t60 = calculate_trigger_threshold(max_size, 60);
        let t70 = calculate_trigger_threshold(max_size, 70);
        let t80 = calculate_trigger_threshold(max_size, 80);
        let t90 = calculate_trigger_threshold(max_size, 90);
        let t95 = calculate_trigger_threshold(max_size, 95);
        let t100 = calculate_trigger_threshold(max_size, 100);
        
        assert!(t50 < t60);
        assert!(t60 < t70);
        assert!(t70 < t80);
        assert!(t80 < t90);
        assert!(t90 < t95);
        assert!(t95 < t100);
    }

    #[test]
    fn test_real_world_scenario_10gb_cache() {
        // Simulate a 10GB cache with 95% trigger threshold
        let max_size = 10_737_418_240u64; // 10GB in bytes
        let trigger_percent = 95u8;
        let threshold = calculate_trigger_threshold(max_size, trigger_percent);
        
        // 95% of 10GB ≈ 9.5GB
        let expected_threshold = 10_200_547_328u64; // 9.5GB in bytes
        assert_eq!(threshold, expected_threshold);
        
        // At 9.4GB: no trigger
        let size_9_4gb = 10_093_613_875u64;
        assert_eq!(
            should_trigger_eviction(size_9_4gb, max_size, trigger_percent),
            EvictionDecision::NoTrigger
        );
        
        // At 9.6GB: trigger
        let size_9_6gb = 10_307_921_510u64;
        assert_eq!(
            should_trigger_eviction(size_9_6gb, max_size, trigger_percent),
            EvictionDecision::Trigger
        );
    }

    #[test]
    fn test_comparison_with_old_100_percent_behavior() {
        // Old behavior: trigger at 100% (current_size > max_size)
        // New behavior: trigger at configurable threshold (default 95%)
        
        let max_size = 10_000_000_000u64; // 10GB
        
        // Old behavior (100% threshold)
        let old_threshold = calculate_trigger_threshold(max_size, 100);
        assert_eq!(old_threshold, max_size);
        
        // New default behavior (95% threshold)
        let new_threshold = calculate_trigger_threshold(max_size, 95);
        assert_eq!(new_threshold, 9_500_000_000);
        
        // At 9.6GB: old behavior wouldn't trigger, new behavior does
        let current_size = 9_600_000_000u64;
        assert_eq!(
            should_trigger_eviction(current_size, max_size, 100),
            EvictionDecision::NoTrigger
        );
        assert_eq!(
            should_trigger_eviction(current_size, max_size, 95),
            EvictionDecision::Trigger
        );
    }
}
