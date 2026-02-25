//! Property-based tests for stale journal entry detection based on timestamp
//!
//! **Property 1: Stale Entry Detection Based on Timestamp**
//!
//! *For any* journal entry where the range file doesn't exist:
//! - If `entry.timestamp + 5 minutes < current_time`: Entry is STALE (removed from journal)
//! - If `entry.timestamp + 5 minutes >= current_time`: Entry is PENDING (stays in journal for retry)
//!
//! Entries with existing range files are always VALID (processed for size delta).
//!
//! **Key Behavior Change (v1.1.10):**
//! Pending entries (recent with missing files) are NO LONGER added to valid_entries.
//! This prevents counting size for ranges that don't exist on disk yet (e.g., due to
//! NFS caching delays). Previously, these entries were processed and their size was
//! counted, leading to massive size tracking discrepancies.
//!
//! **Validates: Requirements 1.1, 1.3, 1.6**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

// ============================================================================
// Test Data Structures
// ============================================================================

/// Represents the state of a range file (exists or not)
#[derive(Debug, Clone, Copy, PartialEq)]
enum RangeFileState {
    Exists,
    Missing,
}

impl Arbitrary for RangeFileState {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            RangeFileState::Exists
        } else {
            RangeFileState::Missing
        }
    }
}

/// Represents a journal entry for testing staleness detection
#[derive(Debug, Clone)]
struct TestJournalEntry {
    /// Timestamp of the journal entry (seconds since UNIX_EPOCH)
    timestamp_secs: u64,
    /// Whether the range file exists
    range_file_state: RangeFileState,
}

impl Arbitrary for TestJournalEntry {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate timestamps within a reasonable range
        // Current time is approximately 1700000000 seconds since epoch (Nov 2023)
        // Generate timestamps from 0 to 2000000000 to cover various scenarios
        let timestamp_secs = u64::arbitrary(g) % 2_000_000_000;
        let range_file_state = RangeFileState::arbitrary(g);

        TestJournalEntry {
            timestamp_secs,
            range_file_state,
        }
    }
}

// ============================================================================
// Staleness Detection Logic (mirrors src/journal_consolidator.rs)
// ============================================================================

/// Default stale entry timeout in seconds (5 minutes)
const STALE_ENTRY_TIMEOUT_SECS: u64 = 300;

/// Result of staleness detection for a single entry
#[derive(Debug, Clone, Copy, PartialEq)]
enum StalenessResult {
    /// Entry is valid (range file exists) - process for size delta, remove from journal
    Valid,
    /// Entry is stale (range file missing AND timestamp older than threshold) - remove from journal
    Stale,
    /// Entry is pending (range file missing AND timestamp recent) - keep in journal for retry
    Pending,
}

/// Determines if a journal entry is stale based on:
/// 1. Whether the range file exists
/// 2. The entry's timestamp relative to current time
///
/// This function implements the same logic as JournalConsolidator::validate_journal_entries_with_staleness()
/// but as a pure function for property testing.
///
/// # Arguments
/// * `entry_timestamp_secs` - Entry timestamp in seconds since UNIX_EPOCH
/// * `current_time_secs` - Current time in seconds since UNIX_EPOCH
/// * `range_file_exists` - Whether the range file exists on disk
/// * `stale_timeout_secs` - Timeout threshold in seconds (default: 300 = 5 minutes)
///
/// # Returns
/// * `StalenessResult::Valid` - Range file exists, process for size delta
/// * `StalenessResult::Stale` - Range file missing + old, remove from journal
/// * `StalenessResult::Pending` - Range file missing + recent, keep in journal for retry
fn detect_staleness(
    entry_timestamp_secs: u64,
    current_time_secs: u64,
    range_file_exists: bool,
    stale_timeout_secs: u64,
) -> StalenessResult {
    // Rule 1: If range file exists, entry is always valid regardless of timestamp
    if range_file_exists {
        return StalenessResult::Valid;
    }

    // Rule 2: Range file doesn't exist - check timestamp
    // Entry is stale if: entry.timestamp + stale_timeout < current_time
    // Which is equivalent to: current_time - entry.timestamp > stale_timeout
    // Which is equivalent to: entry_age > stale_timeout

    // Calculate entry age (handle case where entry timestamp is in the future)
    let entry_age_secs = current_time_secs.saturating_sub(entry_timestamp_secs);

    if entry_age_secs > stale_timeout_secs {
        // Entry is stale - mark for removal
        // Requirements 1.1, 1.3: Remove entries with missing files AND old timestamps
        StalenessResult::Stale
    } else {
        // Entry is recent - may still be streaming
        // BUG FIX (v1.1.10): Return Pending instead of Valid
        // This prevents counting size for ranges that don't exist on disk yet
        // Requirement 1.6: Works correctly across instances (uses entry's own timestamp)
        StalenessResult::Pending
    }
}

// ============================================================================
// Property 1: Stale Entry Detection Based on Timestamp
// **Validates: Requirements 1.1, 1.3, 1.6**
// ============================================================================

/// Property: Entries with existing range files are ALWAYS valid regardless of timestamp
/// **Validates: Requirements 1.1, 1.3**
fn prop_existing_file_always_valid(entry_timestamp_secs: u64, current_time_secs: u64) -> TestResult {
    // Ensure current_time is reasonable (not before UNIX_EPOCH)
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        true, // range file exists
        STALE_ENTRY_TIMEOUT_SECS,
    );

    TestResult::from_bool(result == StalenessResult::Valid)
}

/// Property: Entries with missing files AND timestamp older than 5 minutes are STALE
/// **Validates: Requirements 1.1, 1.3**
fn prop_old_missing_file_is_stale(entry_timestamp_secs: u64, current_time_secs: u64) -> TestResult {
    // Ensure current_time is reasonable
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    // Calculate entry age
    let entry_age_secs = current_time_secs.saturating_sub(entry_timestamp_secs);

    // Only test cases where entry is older than threshold
    if entry_age_secs <= STALE_ENTRY_TIMEOUT_SECS {
        return TestResult::discard();
    }

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        false, // range file missing
        STALE_ENTRY_TIMEOUT_SECS,
    );

    TestResult::from_bool(result == StalenessResult::Stale)
}

/// Property: Entries with missing files AND timestamp newer than 5 minutes are PENDING (kept in journal for retry)
/// **Validates: Requirements 1.1, 1.3, 1.6**
/// 
/// BUG FIX (v1.1.10): Changed from Valid to Pending. Previously, these entries were added to
/// valid_entries and processed for size delta, causing size tracking to count ranges that
/// don't exist on disk yet. Now they are kept in journal for retry on next consolidation cycle.
fn prop_recent_missing_file_is_pending(entry_timestamp_secs: u64, current_time_secs: u64) -> TestResult {
    // Ensure current_time is reasonable
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    // Calculate entry age
    let entry_age_secs = current_time_secs.saturating_sub(entry_timestamp_secs);

    // Only test cases where entry is newer than or equal to threshold
    if entry_age_secs > STALE_ENTRY_TIMEOUT_SECS {
        return TestResult::discard();
    }

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        false, // range file missing
        STALE_ENTRY_TIMEOUT_SECS,
    );

    TestResult::from_bool(result == StalenessResult::Pending)
}

/// Property: Staleness detection is deterministic - same inputs always produce same output
/// **Validates: Requirements 1.6** (cross-instance consistency)
fn prop_staleness_is_deterministic(
    entry_timestamp_secs: u64,
    current_time_secs: u64,
    range_file_exists: bool,
) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    let result1 = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        range_file_exists,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    let result2 = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        range_file_exists,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    TestResult::from_bool(result1 == result2)
}

/// Property: Boundary condition - entry exactly at threshold is PENDING (not stale, but not valid either)
/// **Validates: Requirements 1.1, 1.3**
/// 
/// BUG FIX (v1.1.10): Changed from Valid to Pending. Entries with missing files at the
/// threshold boundary are kept in journal for retry, not processed for size delta.
fn prop_boundary_at_threshold_is_pending(current_time_secs: u64) -> TestResult {
    // Ensure current_time is large enough to have a valid entry timestamp
    if current_time_secs <= STALE_ENTRY_TIMEOUT_SECS {
        return TestResult::discard();
    }

    // Entry timestamp exactly at threshold boundary
    let entry_timestamp_secs = current_time_secs - STALE_ENTRY_TIMEOUT_SECS;

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        false, // range file missing
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Entry age == threshold, so entry_age > threshold is false, entry should be pending
    TestResult::from_bool(result == StalenessResult::Pending)
}

/// Property: Boundary condition - entry 1 second past threshold is STALE
/// **Validates: Requirements 1.1, 1.3**
fn prop_boundary_past_threshold_is_stale(current_time_secs: u64) -> TestResult {
    // Ensure current_time is large enough
    if current_time_secs <= STALE_ENTRY_TIMEOUT_SECS + 1 {
        return TestResult::discard();
    }

    // Entry timestamp 1 second past threshold
    let entry_timestamp_secs = current_time_secs - STALE_ENTRY_TIMEOUT_SECS - 1;

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        false, // range file missing
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Entry age == threshold + 1, so entry_age > threshold is true, entry should be stale
    TestResult::from_bool(result == StalenessResult::Stale)
}

/// Property: Future timestamps (entry timestamp > current time) are PENDING (kept for retry)
/// This handles clock skew scenarios across instances
/// **Validates: Requirements 1.6**
/// 
/// BUG FIX (v1.1.10): Changed from Valid to Pending. Future timestamps with missing files
/// are kept in journal for retry, not processed for size delta.
fn prop_future_timestamp_is_pending(entry_timestamp_secs: u64, current_time_secs: u64) -> TestResult {
    // Only test cases where entry timestamp is in the future
    if entry_timestamp_secs <= current_time_secs {
        return TestResult::discard();
    }

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        false, // range file missing
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Future timestamps have age 0 (saturating_sub), so they're pending (file missing)
    TestResult::from_bool(result == StalenessResult::Pending)
}

/// Property: Combined test - all invariants hold for any input
/// **Validates: Requirements 1.1, 1.3, 1.6**
/// 
/// BUG FIX (v1.1.10): Updated Invariant 3 - entries with missing files and age <= threshold
/// are now Pending (kept for retry), not Valid.
fn prop_all_staleness_invariants(entry: TestJournalEntry, current_time_secs: u64) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    let range_file_exists = entry.range_file_state == RangeFileState::Exists;
    let entry_age_secs = current_time_secs.saturating_sub(entry.timestamp_secs);

    let result = detect_staleness(
        entry.timestamp_secs,
        current_time_secs,
        range_file_exists,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Invariant 1: If range file exists, result must be Valid
    if range_file_exists {
        return TestResult::from_bool(result == StalenessResult::Valid);
    }

    // Invariant 2: If range file missing AND age > threshold, result must be Stale
    if entry_age_secs > STALE_ENTRY_TIMEOUT_SECS {
        return TestResult::from_bool(result == StalenessResult::Stale);
    }

    // Invariant 3: If range file missing AND age <= threshold, result must be Pending
    TestResult::from_bool(result == StalenessResult::Pending)
}

/// Property: Configurable timeout is respected
/// **Validates: Requirements 1.3** (uses timestamp comparison)
/// 
/// BUG FIX (v1.1.10): Updated expected result - entries with missing files and age <= timeout
/// are now Pending (kept for retry), not Valid.
fn prop_configurable_timeout_respected(
    entry_timestamp_secs: u64,
    current_time_secs: u64,
    timeout_secs: u64,
) -> TestResult {
    if current_time_secs == 0 || timeout_secs == 0 {
        return TestResult::discard();
    }

    let entry_age_secs = current_time_secs.saturating_sub(entry_timestamp_secs);

    let result = detect_staleness(
        entry_timestamp_secs,
        current_time_secs,
        false, // range file missing
        timeout_secs,
    );

    // With missing file: stale iff age > timeout, otherwise pending
    let expected = if entry_age_secs > timeout_secs {
        StalenessResult::Stale
    } else {
        StalenessResult::Pending
    };

    TestResult::from_bool(result == expected)
}

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn test_property_existing_file_always_valid() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_existing_file_always_valid as fn(u64, u64) -> TestResult);
}

#[test]
fn test_property_old_missing_file_is_stale() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_old_missing_file_is_stale as fn(u64, u64) -> TestResult);
}

#[test]
fn test_property_recent_missing_file_is_pending() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_recent_missing_file_is_pending as fn(u64, u64) -> TestResult);
}

#[test]
fn test_property_staleness_is_deterministic() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_staleness_is_deterministic as fn(u64, u64, bool) -> TestResult);
}

#[test]
fn test_property_boundary_at_threshold_is_pending() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_boundary_at_threshold_is_pending as fn(u64) -> TestResult);
}

#[test]
fn test_property_boundary_past_threshold_is_stale() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_boundary_past_threshold_is_stale as fn(u64) -> TestResult);
}

#[test]
fn test_property_future_timestamp_is_pending() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_future_timestamp_is_pending as fn(u64, u64) -> TestResult);
}

#[test]
fn test_property_all_staleness_invariants() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_all_staleness_invariants as fn(TestJournalEntry, u64) -> TestResult);
}

#[test]
fn test_property_configurable_timeout_respected() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_configurable_timeout_respected as fn(u64, u64, u64) -> TestResult);
}

// ============================================================================
// Unit tests for specific edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_existing_file_with_old_timestamp() {
        // Even with very old timestamp, existing file means valid
        let result = detect_staleness(
            0,          // Very old timestamp (UNIX_EPOCH)
            1700000000, // Current time (Nov 2023)
            true,       // File exists
            STALE_ENTRY_TIMEOUT_SECS,
        );
        assert_eq!(result, StalenessResult::Valid);
    }

    #[test]
    fn test_existing_file_with_recent_timestamp() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time - 60, // 1 minute ago
            current_time,
            true, // File exists
            STALE_ENTRY_TIMEOUT_SECS,
        );
        assert_eq!(result, StalenessResult::Valid);
    }

    #[test]
    fn test_missing_file_with_old_timestamp() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time - 600, // 10 minutes ago (> 5 min threshold)
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        assert_eq!(result, StalenessResult::Stale);
    }

    #[test]
    fn test_missing_file_with_recent_timestamp() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time - 60, // 1 minute ago (< 5 min threshold)
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // BUG FIX (v1.1.10): Recent entries with missing files are now Pending, not Valid
        assert_eq!(result, StalenessResult::Pending);
    }

    #[test]
    fn test_missing_file_exactly_at_threshold() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time - STALE_ENTRY_TIMEOUT_SECS, // Exactly 5 minutes ago
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // Age == threshold, so age > threshold is false, entry is pending
        // BUG FIX (v1.1.10): Changed from Valid to Pending
        assert_eq!(result, StalenessResult::Pending);
    }

    #[test]
    fn test_missing_file_one_second_past_threshold() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time - STALE_ENTRY_TIMEOUT_SECS - 1, // 5 minutes + 1 second ago
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // Age > threshold, entry is stale
        assert_eq!(result, StalenessResult::Stale);
    }

    #[test]
    fn test_future_timestamp_missing_file() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time + 1000, // Future timestamp (clock skew)
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // Future timestamps have age 0, so they're pending (file missing)
        // BUG FIX (v1.1.10): Changed from Valid to Pending
        assert_eq!(result, StalenessResult::Pending);
    }

    #[test]
    fn test_custom_timeout_shorter() {
        let current_time = 1700000000u64;
        let custom_timeout = 60u64; // 1 minute timeout

        // Entry 2 minutes old with 1 minute timeout -> stale
        let result = detect_staleness(
            current_time - 120, // 2 minutes ago
            current_time,
            false,
            custom_timeout,
        );
        assert_eq!(result, StalenessResult::Stale);

        // Entry 30 seconds old with 1 minute timeout -> pending (file missing)
        // BUG FIX (v1.1.10): Changed from Valid to Pending
        let result = detect_staleness(
            current_time - 30, // 30 seconds ago
            current_time,
            false,
            custom_timeout,
        );
        assert_eq!(result, StalenessResult::Pending);
    }

    #[test]
    fn test_custom_timeout_longer() {
        let current_time = 1700000000u64;
        let custom_timeout = 3600u64; // 1 hour timeout

        // Entry 30 minutes old with 1 hour timeout -> pending (file missing)
        // BUG FIX (v1.1.10): Changed from Valid to Pending
        let result = detect_staleness(
            current_time - 1800, // 30 minutes ago
            current_time,
            false,
            custom_timeout,
        );
        assert_eq!(result, StalenessResult::Pending);

        // Entry 2 hours old with 1 hour timeout -> stale
        let result = detect_staleness(
            current_time - 7200, // 2 hours ago
            current_time,
            false,
            custom_timeout,
        );
        assert_eq!(result, StalenessResult::Stale);
    }

    #[test]
    fn test_zero_age_missing_file() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            current_time, // Same as current time (age = 0)
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // Age 0 <= threshold, entry is pending (file missing)
        // BUG FIX (v1.1.10): Changed from Valid to Pending
        assert_eq!(result, StalenessResult::Pending);
    }

    #[test]
    fn test_very_old_entry_missing_file() {
        let current_time = 1700000000u64;
        let result = detect_staleness(
            0, // UNIX_EPOCH (very old)
            current_time,
            false, // File missing
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // Very old entry with missing file is stale
        assert_eq!(result, StalenessResult::Stale);
    }

    #[test]
    fn test_cross_instance_consistency() {
        // Requirement 1.6: Works correctly across multiple proxy instances
        // since it uses the entry's own timestamp (no cross-instance state required)
        
        let entry_timestamp = 1700000000u64 - 400; // 400 seconds ago
        let current_time = 1700000000u64;

        // Instance 1 evaluates
        let result1 = detect_staleness(
            entry_timestamp,
            current_time,
            false,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Instance 2 evaluates (same inputs)
        let result2 = detect_staleness(
            entry_timestamp,
            current_time,
            false,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Both instances should reach the same conclusion
        assert_eq!(result1, result2);
        assert_eq!(result1, StalenessResult::Stale); // 400s > 300s threshold
    }
}
