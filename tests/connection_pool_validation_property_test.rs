// Feature: throughput-optimization, Property 4: Connection pool validation range
//
// For any `max_idle_per_host` value of type `usize`, `ConnectionPoolConfig` validation
// passes if and only if the value is in the range 1..=500. Values outside this range
// produce an error.
//
// **Validates: Requirements 4.2, 4.3**

use quickcheck::{quickcheck, TestResult};
use s3_proxy::config::ConnectionPoolConfig;

/// Property 4: Connection pool validation range
///
/// Generate random usize values, create a ConnectionPoolConfig with that max_idle_per_host
/// value (using defaults for other fields), call validate(), and verify: validation passes
/// if and only if the value is in 1..=500.
#[test]
fn prop_connection_pool_validation_range() {
    fn property(max_idle: usize) -> TestResult {
        let config = ConnectionPoolConfig {
            max_idle_per_host: max_idle,
            ..ConnectionPoolConfig::default()
        };

        let result = config.validate();
        let expected_valid = (1..=500).contains(&max_idle);

        match (result.is_ok(), expected_valid) {
            (true, true) => TestResult::passed(),
            (false, false) => TestResult::passed(),
            (true, false) => TestResult::error(format!(
                "Validation passed for max_idle_per_host={}, but expected failure (outside 1..=500)",
                max_idle
            )),
            (false, true) => TestResult::error(format!(
                "Validation failed for max_idle_per_host={}, but expected success (within 1..=500): {}",
                max_idle,
                result.unwrap_err()
            )),
        }
    }

    quickcheck(property as fn(usize) -> TestResult);
}
