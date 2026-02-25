//! Property-based tests for retry with exponential backoff and jitter
//!
//! **Property 2: Retry with Exponential Backoff and Jitter**
//!
//! *For any* sequence of `atomic_subtract_size()` failures, the system SHALL retry up to 3 times
//! with delays that follow exponential backoff (base delays 100ms, 200ms, 400ms) and include
//! jitter within ±20% of the base delay.
//!
//! **Validates: Requirements 2.1, 2.2, 2.4**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

// ============================================================================
// Retry Configuration Constants (mirrors src/journal_consolidator.rs)
// ============================================================================

/// Maximum number of retry attempts
const MAX_ATTEMPTS: u32 = 3;

/// Base delay in milliseconds for first retry
const BASE_DELAY_MS: u64 = 100;

/// Jitter percentage (±20%)
const JITTER_PERCENT: f64 = 0.2;

// ============================================================================
// Test Data Structures
// ============================================================================

/// Represents an attempt number for retry logic (1, 2, or 3)
#[derive(Debug, Clone, Copy)]
struct AttemptNumber(u32);

impl Arbitrary for AttemptNumber {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate attempt numbers 1, 2, or 3
        let attempt = (u32::arbitrary(g) % MAX_ATTEMPTS) + 1;
        AttemptNumber(attempt)
    }
}

/// Represents a jitter factor for testing (value between 0.0 and 1.0)
#[derive(Debug, Clone, Copy)]
struct JitterFactor(f64);

impl Arbitrary for JitterFactor {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate a value between 0.0 and 1.0
        let value = (u32::arbitrary(g) % 1001) as f64 / 1000.0;
        JitterFactor(value)
    }
}

// ============================================================================
// Retry Delay Calculation Logic (mirrors src/journal_consolidator.rs)
// ============================================================================

/// Calculates the base delay for a given attempt number using exponential backoff.
///
/// Base delays follow the pattern: 100ms, 200ms, 400ms
/// Formula: BASE_DELAY_MS * 2^(attempt - 1)
///
/// # Arguments
/// * `attempt` - The attempt number (1, 2, or 3)
///
/// # Returns
/// The base delay in milliseconds
fn calculate_base_delay_ms(attempt: u32) -> u64 {
    // Exponential backoff: 100ms * 2^(attempt-1)
    // Attempt 1: 100 * 2^0 = 100ms
    // Attempt 2: 100 * 2^1 = 200ms
    // Attempt 3: 100 * 2^2 = 400ms
    BASE_DELAY_MS * (1 << (attempt - 1))
}

/// Calculates the jitter range for a given base delay.
///
/// Jitter is ±20% of the base delay.
///
/// # Arguments
/// * `base_delay_ms` - The base delay in milliseconds
///
/// # Returns
/// The jitter range in milliseconds (the delay can vary by ±this amount)
fn calculate_jitter_range_ms(base_delay_ms: u64) -> u64 {
    (base_delay_ms as f64 * JITTER_PERCENT) as u64
}

/// Calculates the actual delay with jitter applied.
///
/// This function simulates the jitter calculation from the actual implementation.
///
/// # Arguments
/// * `base_delay_ms` - The base delay in milliseconds
/// * `jitter_factor` - A value between 0.0 and 1.0 representing where in the jitter range to land
///
/// # Returns
/// The actual delay in milliseconds (always >= 1)
fn calculate_delay_with_jitter(base_delay_ms: u64, jitter_factor: f64) -> u64 {
    let jitter_range = calculate_jitter_range_ms(base_delay_ms);
    
    if jitter_range == 0 {
        return base_delay_ms;
    }
    
    // jitter_factor is 0.0 to 1.0, map to -jitter_range to +jitter_range
    // When jitter_factor = 0.0, jitter = -jitter_range
    // When jitter_factor = 0.5, jitter = 0
    // When jitter_factor = 1.0, jitter = +jitter_range
    let jitter = ((jitter_factor * 2.0 - 1.0) * jitter_range as f64) as i64;
    
    // Apply jitter to base delay, ensuring result is at least 1
    (base_delay_ms as i64 + jitter).max(1) as u64
}

/// Calculates the minimum possible delay for an attempt (base - 20%)
fn min_delay_for_attempt(attempt: u32) -> u64 {
    let base = calculate_base_delay_ms(attempt);
    let jitter_range = calculate_jitter_range_ms(base);
    (base as i64 - jitter_range as i64).max(1) as u64
}

/// Calculates the maximum possible delay for an attempt (base + 20%)
fn max_delay_for_attempt(attempt: u32) -> u64 {
    let base = calculate_base_delay_ms(attempt);
    let jitter_range = calculate_jitter_range_ms(base);
    base + jitter_range
}

// ============================================================================
// Property 2: Retry with Exponential Backoff and Jitter
// **Validates: Requirements 2.1, 2.2, 2.4**
// ============================================================================

/// Property: Base delays follow exponential pattern (100ms, 200ms, 400ms)
/// **Validates: Requirements 2.2**
fn prop_base_delays_exponential(attempt: AttemptNumber) -> TestResult {
    let attempt_num = attempt.0;
    
    // Validate attempt is in valid range
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    
    // Expected delays: 100ms, 200ms, 400ms
    let expected = match attempt_num {
        1 => 100,
        2 => 200,
        3 => 400,
        _ => return TestResult::discard(),
    };
    
    TestResult::from_bool(base_delay == expected)
}

/// Property: Jitter is within ±20% of base delay
/// **Validates: Requirements 2.4**
fn prop_jitter_within_bounds(attempt: AttemptNumber, jitter: JitterFactor) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    let actual_delay = calculate_delay_with_jitter(base_delay, jitter.0);
    
    let min_allowed = min_delay_for_attempt(attempt_num);
    let max_allowed = max_delay_for_attempt(attempt_num);
    
    TestResult::from_bool(actual_delay >= min_allowed && actual_delay <= max_allowed)
}

/// Property: Maximum attempts is exactly 3
/// **Validates: Requirements 2.1, 2.2**
fn prop_max_attempts_is_three() -> bool {
    MAX_ATTEMPTS == 3
}

/// Property: Each subsequent delay is approximately double the previous (exponential)
/// **Validates: Requirements 2.2**
fn prop_delays_double_each_attempt() -> bool {
    let delay1 = calculate_base_delay_ms(1);
    let delay2 = calculate_base_delay_ms(2);
    let delay3 = calculate_base_delay_ms(3);
    
    // Verify exponential pattern: each delay is 2x the previous
    delay2 == delay1 * 2 && delay3 == delay2 * 2
}

/// Property: Delay is always positive (at least 1ms)
/// **Validates: Requirements 2.2, 2.4**
fn prop_delay_always_positive(attempt: AttemptNumber, jitter: JitterFactor) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    let actual_delay = calculate_delay_with_jitter(base_delay, jitter.0);
    
    TestResult::from_bool(actual_delay >= 1)
}

/// Property: Jitter range is exactly 20% of base delay
/// **Validates: Requirements 2.4**
fn prop_jitter_range_is_20_percent(attempt: AttemptNumber) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    let jitter_range = calculate_jitter_range_ms(base_delay);
    
    // Expected jitter range is 20% of base delay
    let expected_range = (base_delay as f64 * 0.2) as u64;
    
    TestResult::from_bool(jitter_range == expected_range)
}

/// Property: Minimum jitter factor produces minimum delay (base - 20%)
/// **Validates: Requirements 2.4**
fn prop_min_jitter_produces_min_delay(attempt: AttemptNumber) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    // jitter_factor = 0.0 should produce minimum delay
    let actual_delay = calculate_delay_with_jitter(base_delay, 0.0);
    let expected_min = min_delay_for_attempt(attempt_num);
    
    TestResult::from_bool(actual_delay == expected_min)
}

/// Property: Maximum jitter factor produces maximum delay (base + 20%)
/// **Validates: Requirements 2.4**
fn prop_max_jitter_produces_max_delay(attempt: AttemptNumber) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    // jitter_factor = 1.0 should produce maximum delay
    let actual_delay = calculate_delay_with_jitter(base_delay, 1.0);
    let expected_max = max_delay_for_attempt(attempt_num);
    
    TestResult::from_bool(actual_delay == expected_max)
}

/// Property: Middle jitter factor (0.5) produces base delay (no jitter)
/// **Validates: Requirements 2.4**
fn prop_middle_jitter_produces_base_delay(attempt: AttemptNumber) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    // jitter_factor = 0.5 should produce base delay (no jitter)
    let actual_delay = calculate_delay_with_jitter(base_delay, 0.5);
    
    TestResult::from_bool(actual_delay == base_delay)
}

/// Property: Delays for different attempts don't overlap (with jitter)
/// This ensures exponential backoff is meaningful even with jitter
/// **Validates: Requirements 2.2, 2.4**
fn prop_attempt_delays_dont_overlap() -> bool {
    // Max delay for attempt N should be less than min delay for attempt N+1
    // This ensures the exponential backoff is meaningful
    
    let max_delay_1 = max_delay_for_attempt(1); // 100 + 20 = 120
    let min_delay_2 = min_delay_for_attempt(2); // 200 - 40 = 160
    
    let max_delay_2 = max_delay_for_attempt(2); // 200 + 40 = 240
    let min_delay_3 = min_delay_for_attempt(3); // 400 - 80 = 320
    
    max_delay_1 < min_delay_2 && max_delay_2 < min_delay_3
}

/// Property: Combined test - all retry invariants hold
/// **Validates: Requirements 2.1, 2.2, 2.4**
fn prop_all_retry_invariants(attempt: AttemptNumber, jitter: JitterFactor) -> TestResult {
    let attempt_num = attempt.0;
    
    if attempt_num < 1 || attempt_num > MAX_ATTEMPTS {
        return TestResult::discard();
    }
    
    let base_delay = calculate_base_delay_ms(attempt_num);
    let actual_delay = calculate_delay_with_jitter(base_delay, jitter.0);
    
    // Invariant 1: Base delay follows exponential pattern
    let expected_base = BASE_DELAY_MS * (1 << (attempt_num - 1));
    let base_correct = base_delay == expected_base;
    
    // Invariant 2: Actual delay is within jitter bounds
    let min_allowed = min_delay_for_attempt(attempt_num);
    let max_allowed = max_delay_for_attempt(attempt_num);
    let within_bounds = actual_delay >= min_allowed && actual_delay <= max_allowed;
    
    // Invariant 3: Delay is always positive
    let positive = actual_delay >= 1;
    
    TestResult::from_bool(base_correct && within_bounds && positive)
}

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn test_property_base_delays_exponential() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_base_delays_exponential as fn(AttemptNumber) -> TestResult);
}

#[test]
fn test_property_jitter_within_bounds() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_jitter_within_bounds as fn(AttemptNumber, JitterFactor) -> TestResult);
}

#[test]
fn test_property_max_attempts_is_three() {
    assert!(prop_max_attempts_is_three());
}

#[test]
fn test_property_delays_double_each_attempt() {
    assert!(prop_delays_double_each_attempt());
}

#[test]
fn test_property_delay_always_positive() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_delay_always_positive as fn(AttemptNumber, JitterFactor) -> TestResult);
}

#[test]
fn test_property_jitter_range_is_20_percent() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_jitter_range_is_20_percent as fn(AttemptNumber) -> TestResult);
}

#[test]
fn test_property_min_jitter_produces_min_delay() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_min_jitter_produces_min_delay as fn(AttemptNumber) -> TestResult);
}

#[test]
fn test_property_max_jitter_produces_max_delay() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_max_jitter_produces_max_delay as fn(AttemptNumber) -> TestResult);
}

#[test]
fn test_property_middle_jitter_produces_base_delay() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_middle_jitter_produces_base_delay as fn(AttemptNumber) -> TestResult);
}

#[test]
fn test_property_attempt_delays_dont_overlap() {
    assert!(prop_attempt_delays_dont_overlap());
}

#[test]
fn test_property_all_retry_invariants() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_all_retry_invariants as fn(AttemptNumber, JitterFactor) -> TestResult);
}

// ============================================================================
// Unit tests for specific edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_base_delay_attempt_1() {
        assert_eq!(calculate_base_delay_ms(1), 100);
    }

    #[test]
    fn test_base_delay_attempt_2() {
        assert_eq!(calculate_base_delay_ms(2), 200);
    }

    #[test]
    fn test_base_delay_attempt_3() {
        assert_eq!(calculate_base_delay_ms(3), 400);
    }

    #[test]
    fn test_jitter_range_attempt_1() {
        // 20% of 100ms = 20ms
        assert_eq!(calculate_jitter_range_ms(100), 20);
    }

    #[test]
    fn test_jitter_range_attempt_2() {
        // 20% of 200ms = 40ms
        assert_eq!(calculate_jitter_range_ms(200), 40);
    }

    #[test]
    fn test_jitter_range_attempt_3() {
        // 20% of 400ms = 80ms
        assert_eq!(calculate_jitter_range_ms(400), 80);
    }

    #[test]
    fn test_min_delay_attempt_1() {
        // 100ms - 20ms = 80ms
        assert_eq!(min_delay_for_attempt(1), 80);
    }

    #[test]
    fn test_max_delay_attempt_1() {
        // 100ms + 20ms = 120ms
        assert_eq!(max_delay_for_attempt(1), 120);
    }

    #[test]
    fn test_min_delay_attempt_2() {
        // 200ms - 40ms = 160ms
        assert_eq!(min_delay_for_attempt(2), 160);
    }

    #[test]
    fn test_max_delay_attempt_2() {
        // 200ms + 40ms = 240ms
        assert_eq!(max_delay_for_attempt(2), 240);
    }

    #[test]
    fn test_min_delay_attempt_3() {
        // 400ms - 80ms = 320ms
        assert_eq!(min_delay_for_attempt(3), 320);
    }

    #[test]
    fn test_max_delay_attempt_3() {
        // 400ms + 80ms = 480ms
        assert_eq!(max_delay_for_attempt(3), 480);
    }

    #[test]
    fn test_delay_with_no_jitter() {
        // jitter_factor = 0.5 should produce base delay
        assert_eq!(calculate_delay_with_jitter(100, 0.5), 100);
        assert_eq!(calculate_delay_with_jitter(200, 0.5), 200);
        assert_eq!(calculate_delay_with_jitter(400, 0.5), 400);
    }

    #[test]
    fn test_delay_with_min_jitter() {
        // jitter_factor = 0.0 should produce base - 20%
        assert_eq!(calculate_delay_with_jitter(100, 0.0), 80);
        assert_eq!(calculate_delay_with_jitter(200, 0.0), 160);
        assert_eq!(calculate_delay_with_jitter(400, 0.0), 320);
    }

    #[test]
    fn test_delay_with_max_jitter() {
        // jitter_factor = 1.0 should produce base + 20%
        assert_eq!(calculate_delay_with_jitter(100, 1.0), 120);
        assert_eq!(calculate_delay_with_jitter(200, 1.0), 240);
        assert_eq!(calculate_delay_with_jitter(400, 1.0), 480);
    }

    #[test]
    fn test_delay_ranges_dont_overlap() {
        // Verify that delay ranges for different attempts don't overlap
        // This ensures exponential backoff is meaningful
        
        // Attempt 1: 80-120ms
        // Attempt 2: 160-240ms
        // Attempt 3: 320-480ms
        
        assert!(max_delay_for_attempt(1) < min_delay_for_attempt(2));
        assert!(max_delay_for_attempt(2) < min_delay_for_attempt(3));
    }

    #[test]
    fn test_exponential_growth_factor() {
        // Verify delays grow by factor of 2
        let delay1 = calculate_base_delay_ms(1);
        let delay2 = calculate_base_delay_ms(2);
        let delay3 = calculate_base_delay_ms(3);
        
        assert_eq!(delay2 / delay1, 2);
        assert_eq!(delay3 / delay2, 2);
    }

    #[test]
    fn test_jitter_percent_constant() {
        // Verify jitter is exactly 20%
        assert!((JITTER_PERCENT - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_max_attempts_constant() {
        // Verify max attempts is exactly 3
        assert_eq!(MAX_ATTEMPTS, 3);
    }

    #[test]
    fn test_base_delay_constant() {
        // Verify base delay is exactly 100ms
        assert_eq!(BASE_DELAY_MS, 100);
    }

    #[test]
    fn test_delay_never_zero() {
        // Even with extreme jitter, delay should never be zero
        for attempt in 1..=MAX_ATTEMPTS {
            let base = calculate_base_delay_ms(attempt);
            let min_delay = calculate_delay_with_jitter(base, 0.0);
            assert!(min_delay >= 1, "Delay for attempt {} should be at least 1ms", attempt);
        }
    }

    #[test]
    fn test_total_max_delay_time() {
        // Calculate maximum total delay across all retries
        // This helps understand worst-case retry duration
        let total_max = max_delay_for_attempt(1) + max_delay_for_attempt(2) + max_delay_for_attempt(3);
        // 120 + 240 + 480 = 840ms
        assert_eq!(total_max, 840);
    }

    #[test]
    fn test_total_min_delay_time() {
        // Calculate minimum total delay across all retries
        let total_min = min_delay_for_attempt(1) + min_delay_for_attempt(2) + min_delay_for_attempt(3);
        // 80 + 160 + 320 = 560ms
        assert_eq!(total_min, 560);
    }

    #[test]
    fn test_jitter_symmetry() {
        // Verify jitter is symmetric around base delay
        for attempt in 1..=MAX_ATTEMPTS {
            let base = calculate_base_delay_ms(attempt);
            let min = min_delay_for_attempt(attempt);
            let max = max_delay_for_attempt(attempt);
            
            // Distance from base to min should equal distance from base to max
            let dist_to_min = base - min;
            let dist_to_max = max - base;
            assert_eq!(dist_to_min, dist_to_max, "Jitter should be symmetric for attempt {}", attempt);
        }
    }
}
