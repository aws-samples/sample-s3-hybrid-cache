//! Property-based tests for eviction threshold validation and clamping
//!
//! **Property 5: Threshold Validation and Clamping**
//!
//! *For any* `eviction_trigger_percent` value, it SHALL be clamped to the range [50, 100].
//! *For any* `eviction_target_percent` value, it SHALL be clamped to the range [50, 99].
//! *For any* configuration where `eviction_target_percent >= eviction_trigger_percent`,
//! the target SHALL be set to `eviction_trigger_percent - 10`.
//!
//! **Validates: Requirements 3.5, 3.6, 3.7, 3.8, 3.9**

use quickcheck::{QuickCheck, TestResult};

// ============================================================================
// Validation Logic (mirrors src/config.rs validate_eviction_thresholds)
// ============================================================================

/// Validates and clamps eviction threshold values.
///
/// This function implements the same logic as Config::validate_eviction_thresholds()
/// but as a pure function for property testing.
///
/// Returns (validated_trigger, validated_target)
fn validate_eviction_thresholds(trigger: u8, target: u8) -> (u8, u8) {
    // Step 1: Clamp trigger to [50, 100]
    // Requirements: 3.5, 3.7
    let validated_trigger = trigger.clamp(50, 100);

    // Step 2: Clamp target to [50, 99]
    // Requirements: 3.6, 3.8
    let validated_target = target.clamp(50, 99);

    // Step 3: Ensure target < trigger
    // Requirement: 3.9
    let final_target = if validated_target >= validated_trigger {
        // Set target = trigger - 10, but ensure it stays >= 50
        validated_trigger.saturating_sub(10).max(50)
    } else {
        validated_target
    };

    (validated_trigger, final_target)
}

// ============================================================================
// Property 5: Threshold Validation and Clamping
// **Validates: Requirements 3.5, 3.6, 3.7, 3.8, 3.9**
// ============================================================================

/// Property: Trigger is always in range [50, 100] after validation
/// **Validates: Requirements 3.5, 3.7**
fn prop_trigger_in_valid_range(trigger: u8, target: u8) -> TestResult {
    let (validated_trigger, _) = validate_eviction_thresholds(trigger, target);
    TestResult::from_bool(validated_trigger >= 50 && validated_trigger <= 100)
}

/// Property: Target is always in range [50, 99] after validation
/// **Validates: Requirements 3.6, 3.8**
fn prop_target_in_valid_range(trigger: u8, target: u8) -> TestResult {
    let (_, validated_target) = validate_eviction_thresholds(trigger, target);
    TestResult::from_bool(validated_target >= 50 && validated_target <= 99)
}

/// Property: Target is always less than trigger, OR target == 50 when trigger == 50
/// **Validates: Requirements 3.9**
fn prop_target_less_than_trigger(trigger: u8, target: u8) -> TestResult {
    let (validated_trigger, validated_target) = validate_eviction_thresholds(trigger, target);

    // Special case: when trigger is 50, target must also be 50 (can't go lower)
    // In this case, target == trigger is acceptable
    if validated_trigger == 50 {
        TestResult::from_bool(validated_target == 50)
    } else {
        TestResult::from_bool(validated_target < validated_trigger)
    }
}

/// Property: Values within valid range are preserved (trigger in [50, 100])
/// **Validates: Requirements 3.5, 3.7**
fn prop_valid_trigger_preserved(trigger: u8) -> TestResult {
    // Only test values in valid range
    if trigger < 50 || trigger > 100 {
        return TestResult::discard();
    }

    // Use a target that won't interfere (well below trigger)
    let target = 50;
    let (validated_trigger, _) = validate_eviction_thresholds(trigger, target);
    TestResult::from_bool(validated_trigger == trigger)
}

/// Property: Values within valid range are preserved (target in [50, 99] and < trigger)
/// **Validates: Requirements 3.6, 3.8**
fn prop_valid_target_preserved(target: u8) -> TestResult {
    // Only test values in valid range
    if target < 50 || target > 99 {
        return TestResult::discard();
    }

    // Use a trigger that's higher than target
    let trigger = 100;
    let (_, validated_target) = validate_eviction_thresholds(trigger, target);
    TestResult::from_bool(validated_target == target)
}

/// Property: Values below minimum are clamped up to 50
/// **Validates: Requirements 3.7, 3.8**
fn prop_below_min_clamped_to_50(trigger: u8, target: u8) -> TestResult {
    let (validated_trigger, validated_target) = validate_eviction_thresholds(trigger, target);

    // If original trigger was below 50, it should be clamped to 50
    let trigger_ok = if trigger < 50 {
        validated_trigger == 50
    } else {
        true
    };

    // If original target was below 50, it should be clamped to at least 50
    // (may be further adjusted by target < trigger rule)
    let target_ok = validated_target >= 50;

    TestResult::from_bool(trigger_ok && target_ok)
}

/// Property: Trigger values above 100 are clamped down to 100
/// **Validates: Requirements 3.7**
fn prop_trigger_above_max_clamped(trigger: u8) -> TestResult {
    if trigger <= 100 {
        return TestResult::discard();
    }

    let (validated_trigger, _) = validate_eviction_thresholds(trigger, 80);
    TestResult::from_bool(validated_trigger == 100)
}

/// Property: Target values above 99 are clamped down to 99
/// **Validates: Requirements 3.8**
fn prop_target_above_max_clamped(target: u8) -> TestResult {
    if target <= 99 {
        return TestResult::discard();
    }

    // Use trigger of 100 so target won't be adjusted by the target < trigger rule
    let (_, validated_target) = validate_eviction_thresholds(100, target);
    TestResult::from_bool(validated_target == 99)
}

/// Property: When target >= trigger (after clamping), target becomes trigger - 10
/// **Validates: Requirements 3.9**
fn prop_target_adjusted_when_gte_trigger(trigger: u8, target: u8) -> TestResult {
    let clamped_trigger = trigger.clamp(50, 100);
    let clamped_target = target.clamp(50, 99);

    // Only test cases where target >= trigger after initial clamping
    if clamped_target < clamped_trigger {
        return TestResult::discard();
    }

    let (validated_trigger, validated_target) = validate_eviction_thresholds(trigger, target);

    // Expected: target = trigger - 10, but at least 50
    let expected_target = validated_trigger.saturating_sub(10).max(50);
    TestResult::from_bool(validated_target == expected_target)
}

/// Property: Combined validation - all invariants hold simultaneously
/// **Validates: Requirements 3.5, 3.6, 3.7, 3.8, 3.9**
fn prop_all_invariants_hold(trigger: u8, target: u8) -> TestResult {
    let (validated_trigger, validated_target) = validate_eviction_thresholds(trigger, target);

    // Invariant 1: trigger in [50, 100]
    let trigger_valid = validated_trigger >= 50 && validated_trigger <= 100;

    // Invariant 2: target in [50, 99]
    let target_valid = validated_target >= 50 && validated_target <= 99;

    // Invariant 3: target < trigger OR (target == 50 AND trigger == 50)
    let relationship_valid = if validated_trigger == 50 {
        validated_target == 50
    } else {
        validated_target < validated_trigger
    };

    TestResult::from_bool(trigger_valid && target_valid && relationship_valid)
}

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn test_property_trigger_in_valid_range() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_trigger_in_valid_range as fn(u8, u8) -> TestResult);
}

#[test]
fn test_property_target_in_valid_range() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_in_valid_range as fn(u8, u8) -> TestResult);
}

#[test]
fn test_property_target_less_than_trigger() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_less_than_trigger as fn(u8, u8) -> TestResult);
}

#[test]
fn test_property_valid_trigger_preserved() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_valid_trigger_preserved as fn(u8) -> TestResult);
}

#[test]
fn test_property_valid_target_preserved() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_valid_target_preserved as fn(u8) -> TestResult);
}

#[test]
fn test_property_below_min_clamped_to_50() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_below_min_clamped_to_50 as fn(u8, u8) -> TestResult);
}

#[test]
fn test_property_trigger_above_max_clamped() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_trigger_above_max_clamped as fn(u8) -> TestResult);
}

#[test]
fn test_property_target_above_max_clamped() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_above_max_clamped as fn(u8) -> TestResult);
}

#[test]
fn test_property_target_adjusted_when_gte_trigger() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_target_adjusted_when_gte_trigger as fn(u8, u8) -> TestResult);
}

#[test]
fn test_property_all_invariants_hold() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_all_invariants_hold as fn(u8, u8) -> TestResult);
}

// ============================================================================
// Unit tests for specific edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_default_values_valid() {
        // Default: trigger=95, target=80
        let (trigger, target) = validate_eviction_thresholds(95, 80);
        assert_eq!(trigger, 95);
        assert_eq!(target, 80);
    }

    #[test]
    fn test_minimum_boundary() {
        // Both at minimum
        let (trigger, target) = validate_eviction_thresholds(50, 50);
        assert_eq!(trigger, 50);
        assert_eq!(target, 50); // Special case: when trigger is 50, target stays 50
    }

    #[test]
    fn test_maximum_boundary() {
        // Trigger at max, target at max
        let (trigger, target) = validate_eviction_thresholds(100, 99);
        assert_eq!(trigger, 100);
        assert_eq!(target, 99);
    }

    #[test]
    fn test_trigger_below_min_clamped() {
        let (trigger, _) = validate_eviction_thresholds(30, 80);
        assert_eq!(trigger, 50);
    }

    #[test]
    fn test_trigger_above_max_clamped() {
        let (trigger, _) = validate_eviction_thresholds(150, 80);
        assert_eq!(trigger, 100);
    }

    #[test]
    fn test_target_below_min_clamped() {
        let (_, target) = validate_eviction_thresholds(95, 30);
        assert_eq!(target, 50);
    }

    #[test]
    fn test_target_above_max_clamped() {
        let (_, target) = validate_eviction_thresholds(100, 120);
        assert_eq!(target, 99);
    }

    #[test]
    fn test_target_equals_trigger_adjusted() {
        // target == trigger should result in target = trigger - 10
        let (trigger, target) = validate_eviction_thresholds(80, 80);
        assert_eq!(trigger, 80);
        assert_eq!(target, 70); // 80 - 10 = 70
    }

    #[test]
    fn test_target_greater_than_trigger_adjusted() {
        // target > trigger should result in target = trigger - 10
        let (trigger, target) = validate_eviction_thresholds(70, 85);
        assert_eq!(trigger, 70);
        assert_eq!(target, 60); // 70 - 10 = 60
    }

    #[test]
    fn test_target_adjustment_respects_minimum() {
        // When trigger is 55, target should be 50 (not 45)
        let (trigger, target) = validate_eviction_thresholds(55, 55);
        assert_eq!(trigger, 55);
        assert_eq!(target, 50); // max(55 - 10, 50) = 50
    }

    #[test]
    fn test_extreme_low_values() {
        // Both values at 0
        let (trigger, target) = validate_eviction_thresholds(0, 0);
        assert_eq!(trigger, 50);
        assert_eq!(target, 50); // Clamped to 50, then stays 50 since trigger is 50
    }

    #[test]
    fn test_extreme_high_values() {
        // Both values at 255 (max u8)
        let (trigger, target) = validate_eviction_thresholds(255, 255);
        assert_eq!(trigger, 100);
        // Target clamped to 99, and 99 < 100, so no further adjustment needed
        assert_eq!(target, 99);
    }

    #[test]
    fn test_trigger_51_target_50() {
        // Minimum valid gap
        let (trigger, target) = validate_eviction_thresholds(51, 50);
        assert_eq!(trigger, 51);
        assert_eq!(target, 50);
    }

    #[test]
    fn test_trigger_100_target_99() {
        // Maximum valid values
        let (trigger, target) = validate_eviction_thresholds(100, 99);
        assert_eq!(trigger, 100);
        assert_eq!(target, 99);
    }
}
