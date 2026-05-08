//! In-flight request tracker for download coalescing.
//!
//! This module provides the `InFlightTracker` which coordinates concurrent requests
//! for the same resource, ensuring only one request fetches from S3 while others wait.
//!
//! # Architecture
//!
//! When multiple requests arrive for the same uncached resource:
//! 1. First request becomes the "Fetcher" and performs the S3 fetch
//! 2. Subsequent requests become "Waiters" and subscribe to a broadcast channel
//! 3. When the Fetcher completes, it notifies all Waiters via the broadcast
//! 4. Waiters then serve the response from disk cache
//!
//! # Flight Key Format
//!
//! | Request Type | Flight Key Format | Example |
//! |---|---|---|
//! | Full object GET | `"{cache_key}"` | `"my-bucket/path/to/file.txt"` |
//! | Range request | `"{cache_key}:{start}-{end}"` | `"my-bucket/path/to/file.txt:0-8388607"` |
//! | Part number request | `"{cache_key}:part{N}"` | `"my-bucket/path/to/file.txt:part2"` |

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Result type for fetch completion notification.
/// `Ok(())` indicates success, `Err(String)` contains the error message.
pub type FetchResult = Result<(), String>;

/// Channel capacity for broadcast sender.
/// Only needs to hold one message (the completion notification).
const BROADCAST_CHANNEL_CAPACITY: usize = 1;

/// Tracks in-flight S3 fetches to enable request coalescing.
///
/// Uses a `DashMap` for concurrent access from multiple Tokio tasks.
/// Each entry maps a flight key to a broadcast sender that notifies
/// waiters when the fetch completes.
pub struct InFlightTracker {
    /// Maps flight keys to broadcast senders for pending fetches.
    pending: Arc<DashMap<String, broadcast::Sender<FetchResult>>>,
}

/// Role assigned to a request after registration.
pub enum FetchRole {
    /// First request for this flight key - responsible for fetching from S3.
    Fetcher(FetchGuard),
    /// Subsequent request - should wait for the fetcher to complete.
    Waiter(broadcast::Receiver<FetchResult>),
}

/// RAII guard that removes the flight key entry on drop.
///
/// This ensures cleanup happens even if the fetcher task panics or is cancelled.
/// The guard holds a broadcast sender to notify waiters of completion.
pub struct FetchGuard {
    /// The flight key this guard is responsible for.
    flight_key: String,
    /// Broadcast sender to notify waiters.
    sender: broadcast::Sender<FetchResult>,
    /// Reference to the pending map for cleanup.
    pending: Arc<DashMap<String, broadcast::Sender<FetchResult>>>,
    /// Whether completion has been signaled (success or error).
    completed: bool,
}

impl InFlightTracker {
    /// Creates a new `InFlightTracker`.
    pub fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::new()),
        }
    }

    /// Attempts to register a request for the given flight key.
    ///
    /// # Returns
    ///
    /// - `FetchRole::Fetcher(guard)` if this is the first request for the key.
    ///   The caller is responsible for fetching from S3 and calling
    ///   `guard.complete_success()` or `guard.complete_error()`.
    ///
    /// - `FetchRole::Waiter(receiver)` if another request is already fetching.
    ///   The caller should wait on the receiver for the fetch result.
    ///
    /// # Requirements
    ///
    /// - 15.1: First cache-miss registers as fetcher with broadcast sender
    /// - 15.2: Subsequent cache-miss returns broadcast receiver for waiter
    pub fn try_register(&self, flight_key: &str) -> FetchRole {
        // Use entry API for atomic check-and-insert
        let entry = self.pending.entry(flight_key.to_string());

        match entry {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                // First request - create broadcast channel and become fetcher
                let (tx, _rx) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
                vacant.insert(tx.clone());

                let guard = FetchGuard {
                    flight_key: flight_key.to_string(),
                    sender: tx,
                    pending: Arc::clone(&self.pending),
                    completed: false,
                };

                FetchRole::Fetcher(guard)
            }
            dashmap::mapref::entry::Entry::Occupied(occupied) => {
                // Another request is already fetching - subscribe to its broadcast
                let rx = occupied.get().subscribe();
                FetchRole::Waiter(rx)
            }
        }
    }

    /// Returns the number of in-flight fetches currently being tracked.
    pub fn in_flight_count(&self) -> usize {
        self.pending.len()
    }

    /// Creates a flight key for a full object GET request.
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object (e.g., "bucket/path/to/file.txt")
    ///
    /// # Returns
    ///
    /// The cache key unchanged, as full object requests use the cache key directly.
    ///
    /// # Requirements
    ///
    /// - 15.4: Full-object GET and range GET are independent flight keys
    pub fn make_full_key(cache_key: &str) -> String {
        cache_key.to_string()
    }

    /// Creates a flight key for a range request.
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object
    /// * `start` - Start byte offset (inclusive)
    /// * `end` - End byte offset (inclusive)
    ///
    /// # Returns
    ///
    /// A flight key in the format `"{cache_key}:{start}-{end}"`.
    ///
    /// # Requirements
    ///
    /// - 15.3: Use exact byte range to construct flight key
    /// - 15.4: Full-object GET and range GET are independent flight keys
    pub fn make_range_key(cache_key: &str, start: u64, end: u64) -> String {
        format!("{}:{}-{}", cache_key, start, end)
    }

    /// Creates a flight key for a part number request.
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object
    /// * `part_number` - The part number (1-indexed)
    ///
    /// # Returns
    ///
    /// A flight key in the format `"{cache_key}:part{N}"`.
    pub fn make_part_key(cache_key: &str, part_number: u32) -> String {
        format!("{}:part{}", cache_key, part_number)
    }
}

impl Default for InFlightTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchGuard {
    /// Signals successful completion and notifies all waiters.
    ///
    /// This removes the flight key from the tracker and broadcasts `Ok(())`
    /// to all waiting requests.
    ///
    /// # Requirements
    ///
    /// - 16.1: Broadcast success notification to all waiters
    /// - 16.3: Remove flight key entry on completion
    pub fn complete_success(mut self) {
        self.completed = true;
        // Broadcast success - ignore send errors (no receivers is fine)
        let _ = self.sender.send(Ok(()));
        // Remove entry from pending map
        self.pending.remove(&self.flight_key);
    }

    /// Signals error completion and notifies all waiters.
    ///
    /// This removes the flight key from the tracker and broadcasts `Err(error)`
    /// to all waiting requests.
    ///
    /// # Arguments
    ///
    /// * `error` - Error message describing what went wrong
    ///
    /// # Requirements
    ///
    /// - 16.2: Broadcast error notification to all waiters
    /// - 16.3: Remove flight key entry on completion
    pub fn complete_error(mut self, error: String) {
        self.completed = true;
        // Broadcast error - ignore send errors (no receivers is fine)
        let _ = self.sender.send(Err(error));
        // Remove entry from pending map
        self.pending.remove(&self.flight_key);
    }

    /// Returns the flight key this guard is responsible for.
    pub fn flight_key(&self) -> &str {
        &self.flight_key
    }
}

impl Drop for FetchGuard {
    /// Safety net cleanup for panics or cancellation.
    ///
    /// If the guard is dropped without calling `complete_success()` or
    /// `complete_error()`, this removes the entry from the tracker.
    /// Waiters will detect channel closure and fall back to their own S3 fetch.
    ///
    /// # Requirements
    ///
    /// - 20.1: Remove flight key entry if fetcher panics or is cancelled
    /// - 20.2: Waiters detect channel closure and fall back
    fn drop(&mut self) {
        if !self.completed {
            // Fetcher didn't complete normally - remove entry
            // Channel will be closed, waiters will detect this
            self.pending.remove(&self.flight_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_register_returns_fetcher() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        let _guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher, got Waiter"),
        };

        assert_eq!(tracker.in_flight_count(), 1);
    }

    #[test]
    fn test_second_register_returns_waiter() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        // First registration
        let _guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for first registration"),
        };

        // Second registration should return Waiter
        match tracker.try_register(flight_key) {
            FetchRole::Fetcher(_) => panic!("Expected Waiter, got Fetcher"),
            FetchRole::Waiter(_) => {} // Expected
        }

        assert_eq!(tracker.in_flight_count(), 1);
    }

    #[test]
    fn test_complete_success_removes_entry() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        let guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher"),
        };

        assert_eq!(tracker.in_flight_count(), 1);

        guard.complete_success();

        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_complete_error_removes_entry() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        let guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher"),
        };

        assert_eq!(tracker.in_flight_count(), 1);

        guard.complete_error("S3 error".to_string());

        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_drop_without_complete_removes_entry() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        {
            let _guard = match tracker.try_register(flight_key) {
                FetchRole::Fetcher(guard) => guard,
                FetchRole::Waiter(_) => panic!("Expected Fetcher"),
            };

            assert_eq!(tracker.in_flight_count(), 1);
            // guard dropped here without calling complete_*
        }

        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_new_register_after_completion() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        // First fetch cycle
        let guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher"),
        };
        guard.complete_success();

        // New registration should return Fetcher again
        match tracker.try_register(flight_key) {
            FetchRole::Fetcher(_) => {} // Expected
            FetchRole::Waiter(_) => panic!("Expected Fetcher after completion"),
        }
    }

    #[tokio::test]
    async fn test_waiter_receives_success_notification() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        let guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher"),
        };

        let mut rx = match tracker.try_register(flight_key) {
            FetchRole::Waiter(rx) => rx,
            FetchRole::Fetcher(_) => panic!("Expected Waiter"),
        };

        // Complete in a separate task
        tokio::spawn(async move {
            guard.complete_success();
        });

        // Waiter should receive success
        let result = rx.recv().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_waiter_receives_error_notification() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        let guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher"),
        };

        let mut rx = match tracker.try_register(flight_key) {
            FetchRole::Waiter(rx) => rx,
            FetchRole::Fetcher(_) => panic!("Expected Waiter"),
        };

        // Complete with error in a separate task
        tokio::spawn(async move {
            guard.complete_error("S3 connection failed".to_string());
        });

        // Waiter should receive error
        let result = rx.recv().await;
        assert!(result.is_ok());
        let fetch_result = result.unwrap();
        assert!(fetch_result.is_err());
        assert_eq!(fetch_result.unwrap_err(), "S3 connection failed");
    }

    #[tokio::test]
    async fn test_waiter_detects_channel_closure_on_drop() {
        let tracker = InFlightTracker::new();
        let flight_key = "bucket/object.txt";

        let guard = match tracker.try_register(flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher"),
        };

        let mut rx = match tracker.try_register(flight_key) {
            FetchRole::Waiter(rx) => rx,
            FetchRole::Fetcher(_) => panic!("Expected Waiter"),
        };

        // Drop guard without completing (simulates panic/cancellation)
        drop(guard);

        // Waiter should detect channel closure
        let result = rx.recv().await;
        assert!(result.is_err()); // RecvError indicates channel closed
    }

    #[test]
    fn test_make_full_key() {
        let cache_key = "my-bucket/path/to/file.txt";
        let flight_key = InFlightTracker::make_full_key(cache_key);
        assert_eq!(flight_key, "my-bucket/path/to/file.txt");
    }

    #[test]
    fn test_make_range_key() {
        let cache_key = "my-bucket/path/to/file.txt";
        let flight_key = InFlightTracker::make_range_key(cache_key, 0, 8388607);
        assert_eq!(flight_key, "my-bucket/path/to/file.txt:0-8388607");
    }

    #[test]
    fn test_make_part_key() {
        let cache_key = "my-bucket/path/to/file.txt";
        let flight_key = InFlightTracker::make_part_key(cache_key, 2);
        assert_eq!(flight_key, "my-bucket/path/to/file.txt:part2");
    }

    #[test]
    fn test_different_flight_keys_are_independent() {
        let tracker = InFlightTracker::new();
        let cache_key = "bucket/object.txt";

        // Register full object fetch
        let full_key = InFlightTracker::make_full_key(cache_key);
        let _guard1 = match tracker.try_register(&full_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for full key"),
        };

        // Register range fetch - should be independent
        let range_key = InFlightTracker::make_range_key(cache_key, 0, 1000);
        let _guard2 = match tracker.try_register(&range_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for range key"),
        };

        // Register part fetch - should be independent
        let part_key = InFlightTracker::make_part_key(cache_key, 1);
        let _guard3 = match tracker.try_register(&part_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for part key"),
        };

        assert_eq!(tracker.in_flight_count(), 3);
    }

    #[test]
    fn test_same_range_different_objects_are_independent() {
        let tracker = InFlightTracker::new();

        let key1 = InFlightTracker::make_range_key("bucket/file1.txt", 0, 1000);
        let key2 = InFlightTracker::make_range_key("bucket/file2.txt", 0, 1000);

        let _guard1 = match tracker.try_register(&key1) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for key1"),
        };

        let _guard2 = match tracker.try_register(&key2) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for key2"),
        };

        assert_eq!(tracker.in_flight_count(), 2);
    }

    #[test]
    fn test_different_ranges_same_object_are_independent() {
        let tracker = InFlightTracker::new();
        let cache_key = "bucket/object.txt";

        let key1 = InFlightTracker::make_range_key(cache_key, 0, 1000);
        let key2 = InFlightTracker::make_range_key(cache_key, 1001, 2000);

        let _guard1 = match tracker.try_register(&key1) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for range 0-1000"),
        };

        let _guard2 = match tracker.try_register(&key2) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => panic!("Expected Fetcher for range 1001-2000"),
        };

        assert_eq!(tracker.in_flight_count(), 2);
    }
}

// =========================================================================
// Property-Based Tests
// Requirements: 15.1, 15.2, 20.1
// =========================================================================

#[cfg(test)]
mod property_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// Generate a valid flight key from arbitrary strings
    fn make_flight_key(bucket: &str, path: &str) -> String {
        // Sanitize inputs to create valid flight keys
        let bucket = if bucket.is_empty() { "bucket" } else { bucket };
        let path = if path.is_empty() { "object.txt" } else { path };
        format!("{}/{}", bucket, path)
    }

    /// Property 6: InFlightTracker Registration Determinism
    ///
    /// For any flight key string, the first call to `try_register` returns `Fetcher`;
    /// all subsequent calls before completion return `Waiter`.
    ///
    /// Validates: Requirements 15.1, 15.2
    #[quickcheck]
    fn prop_registration_determinism(bucket: String, path: String) -> TestResult {
        let tracker = InFlightTracker::new();
        let flight_key = make_flight_key(&bucket, &path);

        // First registration must return Fetcher
        let guard = match tracker.try_register(&flight_key) {
            FetchRole::Fetcher(guard) => guard,
            FetchRole::Waiter(_) => return TestResult::failed(),
        };

        // All subsequent registrations must return Waiter
        for _ in 0..5 {
            match tracker.try_register(&flight_key) {
                FetchRole::Fetcher(_) => return TestResult::failed(),
                FetchRole::Waiter(_) => {} // Expected
            }
        }

        // Count should still be 1 (only one entry for this key)
        if tracker.in_flight_count() != 1 {
            return TestResult::failed();
        }

        // Clean up
        guard.complete_success();
        TestResult::passed()
    }

    /// Property 7: FetchGuard Cleanup on Drop
    ///
    /// For any flight key, when a `FetchGuard` is dropped without calling
    /// `complete_success` or `complete_error`, the flight key is removed from
    /// the tracker and a new `try_register` returns `Fetcher`.
    ///
    /// Validates: Requirement 20.1
    #[quickcheck]
    fn prop_fetch_guard_cleanup_on_drop(bucket: String, path: String) -> TestResult {
        let tracker = InFlightTracker::new();
        let flight_key = make_flight_key(&bucket, &path);

        // Register and get guard
        {
            let _guard = match tracker.try_register(&flight_key) {
                FetchRole::Fetcher(guard) => guard,
                FetchRole::Waiter(_) => return TestResult::failed(),
            };

            if tracker.in_flight_count() != 1 {
                return TestResult::failed();
            }

            // Guard dropped here without calling complete_*
        }

        // Entry should be removed
        if tracker.in_flight_count() != 0 {
            return TestResult::failed();
        }

        // New registration should return Fetcher again
        match tracker.try_register(&flight_key) {
            FetchRole::Fetcher(guard) => {
                guard.complete_success(); // Clean up
            }
            FetchRole::Waiter(_) => return TestResult::failed(),
        }

        TestResult::passed()
    }

    /// Property: Flight key format is deterministic
    ///
    /// For any cache_key, start, and end values, the generated flight key
    /// is always the same.
    #[quickcheck]
    fn prop_flight_key_format_deterministic(
        bucket: String,
        path: String,
        start: u64,
        end: u64,
        part_number: u32,
    ) -> TestResult {
        let cache_key = make_flight_key(&bucket, &path);

        // Full key is deterministic
        let full1 = InFlightTracker::make_full_key(&cache_key);
        let full2 = InFlightTracker::make_full_key(&cache_key);
        if full1 != full2 || full1 != cache_key {
            return TestResult::failed();
        }

        // Range key is deterministic
        let range1 = InFlightTracker::make_range_key(&cache_key, start, end);
        let range2 = InFlightTracker::make_range_key(&cache_key, start, end);
        if range1 != range2 {
            return TestResult::failed();
        }
        if !range1.contains(&cache_key) {
            return TestResult::failed();
        }
        if !range1.contains(&format!("{}-{}", start, end)) {
            return TestResult::failed();
        }

        // Part key is deterministic (skip if part_number is 0)
        if part_number == 0 {
            return TestResult::discard();
        }
        let part1 = InFlightTracker::make_part_key(&cache_key, part_number);
        let part2 = InFlightTracker::make_part_key(&cache_key, part_number);
        if part1 != part2 {
            return TestResult::failed();
        }
        if !part1.contains(&cache_key) {
            return TestResult::failed();
        }
        if !part1.contains(&format!("part{}", part_number)) {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    /// Property: Multiple independent flight keys can coexist
    ///
    /// For any set of distinct flight keys, each can have its own independent
    /// fetcher without interference.
    #[quickcheck]
    fn prop_independent_flight_keys(keys: Vec<(String, String)>) -> TestResult {
        if keys.is_empty() || keys.len() > 20 {
            return TestResult::discard();
        }

        let tracker = InFlightTracker::new();
        let mut guards = Vec::new();

        // Make keys unique by appending index
        let unique_keys: Vec<String> = keys
            .iter()
            .enumerate()
            .map(|(i, (bucket, path))| {
                let key = make_flight_key(bucket, path);
                format!("{}_{}", key, i)
            })
            .collect();

        // Register all keys - each should return Fetcher
        for key in &unique_keys {
            match tracker.try_register(key) {
                FetchRole::Fetcher(guard) => guards.push(guard),
                FetchRole::Waiter(_) => return TestResult::failed(),
            }
        }

        if tracker.in_flight_count() != unique_keys.len() {
            return TestResult::failed();
        }

        // Complete all guards
        for guard in guards {
            guard.complete_success();
        }

        if tracker.in_flight_count() != 0 {
            return TestResult::failed();
        }

        TestResult::passed()
    }
}
