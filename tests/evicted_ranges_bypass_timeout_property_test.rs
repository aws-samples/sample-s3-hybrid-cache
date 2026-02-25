//! Property-based tests for evicted ranges bypassing the staleness timeout
//!
//! **Property 6: Evicted Ranges Bypass Timeout**
//!
//! *For any* journal entry whose (cache_key, start, end) matches a range that was just evicted,
//! the entry SHALL be removed immediately regardless of its timestamp (bypassing the 5-minute timeout).
//!
//! **Validates: Requirements 4.1, 4.3**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::HashMap;

// ============================================================================
// Test Data Structures
// ============================================================================

/// Represents a range that may or may not have been evicted
#[derive(Debug, Clone)]
struct TestRange {
    cache_key: String,
    start: u64,
    end: u64,
}

impl Arbitrary for TestRange {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate cache keys with bucket/object format
        let bucket_num = u8::arbitrary(g) % 10;
        let object_num = u16::arbitrary(g) % 1000;
        let cache_key = format!("bucket-{}/object-{}.bin", bucket_num, object_num);
        
        // Generate valid range (start <= end)
        let start = u64::arbitrary(g) % 1_000_000_000;
        let end_offset = u64::arbitrary(g) % 10_000_000;
        let end = start + end_offset;
        
        TestRange {
            cache_key,
            start,
            end,
        }
    }
}

/// Represents a journal entry for testing eviction bypass
#[derive(Debug, Clone)]
struct TestJournalEntry {
    range: TestRange,
    /// Timestamp of the journal entry (seconds since UNIX_EPOCH)
    timestamp_secs: u64,
    /// Whether the range file exists on disk
    range_file_exists: bool,
}

impl Arbitrary for TestJournalEntry {
    fn arbitrary(g: &mut Gen) -> Self {
        let range = TestRange::arbitrary(g);
        // Generate timestamps within a reasonable range
        let timestamp_secs = u64::arbitrary(g) % 2_000_000_000;
        let range_file_exists = bool::arbitrary(g);
        
        TestJournalEntry {
            range,
            timestamp_secs,
            range_file_exists,
        }
    }
}

// ============================================================================
// Eviction Bypass Logic (mirrors src/journal_consolidator.rs)
// ============================================================================

/// Default stale entry timeout in seconds (5 minutes)
const STALE_ENTRY_TIMEOUT_SECS: u64 = 300;

/// Result of staleness detection for a single entry
#[derive(Debug, Clone, Copy, PartialEq)]
enum StalenessResult {
    /// Entry is valid (should be retained)
    Valid,
    /// Entry is stale (should be removed)
    Stale,
    /// Entry matches evicted range (should be removed immediately)
    EvictedBypass,
}

/// Simulates the evicted ranges tracker in JournalConsolidator
struct EvictedRangesTracker {
    /// Ranges that were evicted, keyed by cache_key
    evicted_ranges: HashMap<String, Vec<(u64, u64)>>,
}

impl EvictedRangesTracker {
    fn new() -> Self {
        Self {
            evicted_ranges: HashMap::new(),
        }
    }

    /// Mark ranges as evicted (mirrors JournalConsolidator::mark_ranges_evicted)
    fn mark_ranges_evicted(&mut self, ranges: Vec<(String, u64, u64)>) {
        for (cache_key, start, end) in ranges {
            self.evicted_ranges
                .entry(cache_key)
                .or_default()
                .push((start, end));
        }
    }

    /// Check if a range was evicted and clear it (mirrors check_and_clear_evicted_range)
    fn check_and_clear_evicted_range(&mut self, cache_key: &str, start: u64, end: u64) -> bool {
        if let Some(ranges) = self.evicted_ranges.get_mut(cache_key) {
            if let Some(pos) = ranges.iter().position(|&(s, e)| s == start && e == end) {
                ranges.remove(pos);
                if ranges.is_empty() {
                    self.evicted_ranges.remove(cache_key);
                }
                return true;
            }
        }
        false
    }
}

/// Determines if a journal entry should be removed based on:
/// 1. Whether the range was recently evicted (bypasses timeout)
/// 2. Whether the range file exists
/// 3. The entry's timestamp relative to current time
///
/// This function implements the same logic as JournalConsolidator::validate_journal_entries_with_staleness()
/// but as a pure function for property testing.
///
/// **Validates: Requirements 4.1, 4.3**
fn detect_staleness_with_eviction_bypass(
    entry: &TestJournalEntry,
    current_time_secs: u64,
    evicted_tracker: &mut EvictedRangesTracker,
    stale_timeout_secs: u64,
) -> StalenessResult {
    // Rule 1: Check if range was recently evicted (bypasses timeout)
    // Requirement 4.3: Immediately mark journal entries for evicted ranges as stale
    if evicted_tracker.check_and_clear_evicted_range(
        &entry.range.cache_key,
        entry.range.start,
        entry.range.end,
    ) {
        return StalenessResult::EvictedBypass;
    }

    // Rule 2: If range file exists, entry is always valid
    if entry.range_file_exists {
        return StalenessResult::Valid;
    }

    // Rule 3: Range file doesn't exist - check timestamp
    let entry_age_secs = current_time_secs.saturating_sub(entry.timestamp_secs);

    if entry_age_secs > stale_timeout_secs {
        StalenessResult::Stale
    } else {
        StalenessResult::Valid
    }
}

// ============================================================================
// Property 6: Evicted Ranges Bypass Timeout
// **Validates: Requirements 4.1, 4.3**
// ============================================================================

/// Property: Evicted ranges are ALWAYS removed regardless of timestamp
/// **Validates: Requirements 4.1, 4.3**
fn prop_evicted_range_always_removed(entry: TestJournalEntry, current_time_secs: u64) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    let mut tracker = EvictedRangesTracker::new();
    
    // Mark this entry's range as evicted
    tracker.mark_ranges_evicted(vec![(
        entry.range.cache_key.clone(),
        entry.range.start,
        entry.range.end,
    )]);

    let result = detect_staleness_with_eviction_bypass(
        &entry,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Evicted ranges should always be removed via EvictedBypass
    TestResult::from_bool(result == StalenessResult::EvictedBypass)
}

/// Property: Non-evicted ranges follow normal staleness rules
/// **Validates: Requirements 4.1, 4.3**
fn prop_non_evicted_range_follows_normal_rules(
    entry: TestJournalEntry,
    current_time_secs: u64,
) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    let mut tracker = EvictedRangesTracker::new();
    // Don't mark this range as evicted

    let result = detect_staleness_with_eviction_bypass(
        &entry,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Should NOT be EvictedBypass since range wasn't evicted
    if result == StalenessResult::EvictedBypass {
        return TestResult::failed();
    }

    // Verify normal staleness rules apply
    let entry_age_secs = current_time_secs.saturating_sub(entry.timestamp_secs);
    
    let expected = if entry.range_file_exists {
        StalenessResult::Valid
    } else if entry_age_secs > STALE_ENTRY_TIMEOUT_SECS {
        StalenessResult::Stale
    } else {
        StalenessResult::Valid
    };

    TestResult::from_bool(result == expected)
}

/// Property: Evicted range is only matched once (cleared after first check)
/// **Validates: Requirements 4.3**
fn prop_evicted_range_cleared_after_match(entry: TestJournalEntry, current_time_secs: u64) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    let mut tracker = EvictedRangesTracker::new();
    
    // Mark this entry's range as evicted
    tracker.mark_ranges_evicted(vec![(
        entry.range.cache_key.clone(),
        entry.range.start,
        entry.range.end,
    )]);

    // First check should match and clear
    let result1 = detect_staleness_with_eviction_bypass(
        &entry,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Second check should NOT match (already cleared)
    let result2 = detect_staleness_with_eviction_bypass(
        &entry,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // First should be EvictedBypass, second should follow normal rules
    let first_ok = result1 == StalenessResult::EvictedBypass;
    let second_ok = result2 != StalenessResult::EvictedBypass;

    TestResult::from_bool(first_ok && second_ok)
}

/// Property: Eviction bypass works regardless of entry timestamp (even very recent entries)
/// **Validates: Requirements 4.1, 4.3**
fn prop_eviction_bypass_ignores_timestamp(current_time_secs: u64) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    // Create a very recent entry (just created, timestamp = current_time)
    let entry = TestJournalEntry {
        range: TestRange {
            cache_key: "test-bucket/test-object.bin".to_string(),
            start: 0,
            end: 1000,
        },
        timestamp_secs: current_time_secs, // Very recent
        range_file_exists: false,
    };

    let mut tracker = EvictedRangesTracker::new();
    
    // Mark as evicted
    tracker.mark_ranges_evicted(vec![(
        entry.range.cache_key.clone(),
        entry.range.start,
        entry.range.end,
    )]);

    let result = detect_staleness_with_eviction_bypass(
        &entry,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Even though entry is very recent (would normally be Valid), eviction bypass should apply
    TestResult::from_bool(result == StalenessResult::EvictedBypass)
}

/// Property: Eviction bypass works regardless of file existence
/// **Validates: Requirements 4.1, 4.3**
fn prop_eviction_bypass_ignores_file_existence(entry: TestJournalEntry, current_time_secs: u64) -> TestResult {
    if current_time_secs == 0 {
        return TestResult::discard();
    }

    // Test with file existing
    let entry_with_file = TestJournalEntry {
        range: entry.range.clone(),
        timestamp_secs: entry.timestamp_secs,
        range_file_exists: true,
    };

    let mut tracker1 = EvictedRangesTracker::new();
    tracker1.mark_ranges_evicted(vec![(
        entry_with_file.range.cache_key.clone(),
        entry_with_file.range.start,
        entry_with_file.range.end,
    )]);

    let result_with_file = detect_staleness_with_eviction_bypass(
        &entry_with_file,
        current_time_secs,
        &mut tracker1,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Test with file missing
    let entry_without_file = TestJournalEntry {
        range: entry.range.clone(),
        timestamp_secs: entry.timestamp_secs,
        range_file_exists: false,
    };

    let mut tracker2 = EvictedRangesTracker::new();
    tracker2.mark_ranges_evicted(vec![(
        entry_without_file.range.cache_key.clone(),
        entry_without_file.range.start,
        entry_without_file.range.end,
    )]);

    let result_without_file = detect_staleness_with_eviction_bypass(
        &entry_without_file,
        current_time_secs,
        &mut tracker2,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // Both should be EvictedBypass regardless of file existence
    TestResult::from_bool(
        result_with_file == StalenessResult::EvictedBypass
            && result_without_file == StalenessResult::EvictedBypass,
    )
}

/// Property: Different ranges for same cache_key are tracked independently
/// **Validates: Requirements 4.1, 4.3**
fn prop_different_ranges_tracked_independently(current_time_secs: u64) -> TestResult {
    // Need enough time to subtract 100 without overflow
    if current_time_secs < 200 {
        return TestResult::discard();
    }

    let cache_key = "test-bucket/test-object.bin".to_string();

    let entry1 = TestJournalEntry {
        range: TestRange {
            cache_key: cache_key.clone(),
            start: 0,
            end: 1000,
        },
        timestamp_secs: current_time_secs - 100, // Recent
        range_file_exists: false,
    };

    let entry2 = TestJournalEntry {
        range: TestRange {
            cache_key: cache_key.clone(),
            start: 1001,
            end: 2000,
        },
        timestamp_secs: current_time_secs - 100, // Recent
        range_file_exists: false,
    };

    let mut tracker = EvictedRangesTracker::new();
    
    // Only mark entry1's range as evicted
    tracker.mark_ranges_evicted(vec![(
        entry1.range.cache_key.clone(),
        entry1.range.start,
        entry1.range.end,
    )]);

    let result1 = detect_staleness_with_eviction_bypass(
        &entry1,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    let result2 = detect_staleness_with_eviction_bypass(
        &entry2,
        current_time_secs,
        &mut tracker,
        STALE_ENTRY_TIMEOUT_SECS,
    );

    // entry1 should be EvictedBypass, entry2 should follow normal rules (Valid since recent)
    TestResult::from_bool(
        result1 == StalenessResult::EvictedBypass && result2 == StalenessResult::Valid,
    )
}

/// Property: Multiple evicted ranges can be tracked simultaneously
/// **Validates: Requirements 4.1, 4.2**
fn prop_multiple_evicted_ranges_tracked(current_time_secs: u64) -> TestResult {
    // Need enough time to subtract 100 without overflow
    if current_time_secs < 200 {
        return TestResult::discard();
    }

    let entries: Vec<TestJournalEntry> = (0..5)
        .map(|i| TestJournalEntry {
            range: TestRange {
                cache_key: format!("bucket-{}/object.bin", i),
                start: i as u64 * 1000,
                end: (i as u64 + 1) * 1000 - 1,
            },
            timestamp_secs: current_time_secs - 100,
            range_file_exists: false,
        })
        .collect();

    let mut tracker = EvictedRangesTracker::new();
    
    // Mark all ranges as evicted
    let evicted: Vec<(String, u64, u64)> = entries
        .iter()
        .map(|e| (e.range.cache_key.clone(), e.range.start, e.range.end))
        .collect();
    tracker.mark_ranges_evicted(evicted);

    // All should be detected as evicted
    let all_evicted = entries.iter().all(|entry| {
        let result = detect_staleness_with_eviction_bypass(
            entry,
            current_time_secs,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );
        result == StalenessResult::EvictedBypass
    });

    TestResult::from_bool(all_evicted)
}

// ============================================================================
// Test Functions
// ============================================================================

#[test]
fn test_property_evicted_range_always_removed() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_evicted_range_always_removed as fn(TestJournalEntry, u64) -> TestResult);
}

#[test]
fn test_property_non_evicted_range_follows_normal_rules() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_non_evicted_range_follows_normal_rules as fn(TestJournalEntry, u64) -> TestResult);
}

#[test]
fn test_property_evicted_range_cleared_after_match() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_evicted_range_cleared_after_match as fn(TestJournalEntry, u64) -> TestResult);
}

#[test]
fn test_property_eviction_bypass_ignores_timestamp() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_eviction_bypass_ignores_timestamp as fn(u64) -> TestResult);
}

#[test]
fn test_property_eviction_bypass_ignores_file_existence() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_eviction_bypass_ignores_file_existence as fn(TestJournalEntry, u64) -> TestResult);
}

#[test]
fn test_property_different_ranges_tracked_independently() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_different_ranges_tracked_independently as fn(u64) -> TestResult);
}

#[test]
fn test_property_multiple_evicted_ranges_tracked() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_multiple_evicted_ranges_tracked as fn(u64) -> TestResult);
}

// ============================================================================
// Unit tests for specific edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_evicted_range_with_very_recent_timestamp() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time, // Just created
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        tracker.mark_ranges_evicted(vec![(
            entry.range.cache_key.clone(),
            entry.range.start,
            entry.range.end,
        )]);

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Even very recent entries should be removed if evicted
        assert_eq!(result, StalenessResult::EvictedBypass);
    }

    #[test]
    fn test_evicted_range_with_existing_file() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 100,
            range_file_exists: true, // File exists
        };

        let mut tracker = EvictedRangesTracker::new();
        tracker.mark_ranges_evicted(vec![(
            entry.range.cache_key.clone(),
            entry.range.start,
            entry.range.end,
        )]);

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Eviction bypass takes precedence over file existence
        assert_eq!(result, StalenessResult::EvictedBypass);
    }

    #[test]
    fn test_non_evicted_range_with_existing_file() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 1000, // Old timestamp
            range_file_exists: true,
        };

        let mut tracker = EvictedRangesTracker::new();
        // Don't mark as evicted

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // File exists, so entry is valid
        assert_eq!(result, StalenessResult::Valid);
    }

    #[test]
    fn test_non_evicted_range_old_missing_file() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 600, // 10 minutes ago
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        // Don't mark as evicted

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Old entry with missing file is stale (normal rules)
        assert_eq!(result, StalenessResult::Stale);
    }

    #[test]
    fn test_non_evicted_range_recent_missing_file() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 60, // 1 minute ago
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        // Don't mark as evicted

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Recent entry with missing file is valid (may still be streaming)
        assert_eq!(result, StalenessResult::Valid);
    }

    #[test]
    fn test_evicted_range_cleared_after_first_check() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 60, // Recent
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        tracker.mark_ranges_evicted(vec![(
            entry.range.cache_key.clone(),
            entry.range.start,
            entry.range.end,
        )]);

        // First check
        let result1 = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );
        assert_eq!(result1, StalenessResult::EvictedBypass);

        // Second check - should follow normal rules now
        let result2 = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );
        // Recent entry with missing file is valid
        assert_eq!(result2, StalenessResult::Valid);
    }

    #[test]
    fn test_partial_range_match_not_evicted() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 60,
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        // Mark a different range (same cache_key but different start/end)
        tracker.mark_ranges_evicted(vec![(
            entry.range.cache_key.clone(),
            500,  // Different start
            1500, // Different end
        )]);

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Should NOT match - different range bounds
        assert_eq!(result, StalenessResult::Valid);
    }

    #[test]
    fn test_different_cache_key_not_evicted() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object1.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 60,
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        // Mark a different cache_key
        tracker.mark_ranges_evicted(vec![(
            "bucket/object2.bin".to_string(), // Different cache_key
            0,
            1000,
        )]);

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Should NOT match - different cache_key
        assert_eq!(result, StalenessResult::Valid);
    }

    #[test]
    fn test_empty_tracker_follows_normal_rules() {
        let current_time = 1700000000u64;
        let entry = TestJournalEntry {
            range: TestRange {
                cache_key: "bucket/object.bin".to_string(),
                start: 0,
                end: 1000,
            },
            timestamp_secs: current_time - 600, // Old
            range_file_exists: false,
        };

        let mut tracker = EvictedRangesTracker::new();
        // Empty tracker

        let result = detect_staleness_with_eviction_bypass(
            &entry,
            current_time,
            &mut tracker,
            STALE_ENTRY_TIMEOUT_SECS,
        );

        // Old entry with missing file is stale
        assert_eq!(result, StalenessResult::Stale);
    }
}
