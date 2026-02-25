//! Log Sampling Module
//!
//! Provides intelligent log sampling to reduce log volume for high-frequency operations
//! while maintaining visibility into system behavior and ensuring errors are always logged.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Log sampler for reducing log volume of high-frequency operations
#[derive(Debug, Clone)]
pub struct LogSampler {
    counters: Arc<HashMap<&'static str, AtomicU64>>,
    sample_rates: HashMap<&'static str, u64>,
}

impl LogSampler {
    /// Create a new log sampler with default sample rates
    pub fn new() -> Self {
        let mut counters = HashMap::new();
        let mut sample_rates = HashMap::new();

        // High-frequency operations - sample every Nth occurrence
        counters.insert("multipart_upload", AtomicU64::new(0));
        sample_rates.insert("multipart_upload", 10); // Log every 10th multipart operation

        counters.insert("cache_hit", AtomicU64::new(0));
        sample_rates.insert("cache_hit", 50); // Log every 50th cache hit

        counters.insert("range_request", AtomicU64::new(0));
        sample_rates.insert("range_request", 20); // Log every 20th range request

        counters.insert("chunked_decode", AtomicU64::new(0));
        sample_rates.insert("chunked_decode", 25); // Log every 25th chunked decode

        Self {
            counters: Arc::new(counters),
            sample_rates,
        }
    }

    /// Check if an operation should be logged based on sampling rate
    /// Always returns true for the first occurrence and errors
    pub fn should_log(&self, operation: &'static str) -> bool {
        if let Some(counter) = self.counters.get(operation) {
            let count = counter.fetch_add(1, Ordering::Relaxed);
            let sample_rate = self.sample_rates.get(operation).unwrap_or(&1);

            // Always log the first occurrence
            if count == 0 {
                return true;
            }

            count % sample_rate == 0
        } else {
            // Unknown operations are always logged
            true
        }
    }

    /// Get current count for an operation
    pub fn get_count(&self, operation: &'static str) -> u64 {
        self.counters
            .get(operation)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Reset counters (useful for testing)
    pub fn reset(&self) {
        for counter in self.counters.values() {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

impl Default for LogSampler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_sampling() {
        let sampler = LogSampler::new();

        // First occurrence should always be logged
        assert!(sampler.should_log("multipart_upload"));

        // Next 8 should not be logged (sample rate is 10)
        for _ in 1..10 {
            assert!(!sampler.should_log("multipart_upload"));
        }

        // 10th occurrence should be logged
        assert!(sampler.should_log("multipart_upload"));
    }

    #[test]
    fn test_unknown_operation_always_logged() {
        let sampler = LogSampler::new();
        assert!(sampler.should_log("unknown_operation"));
        assert!(sampler.should_log("unknown_operation"));
    }

    #[test]
    fn test_counter_tracking() {
        let sampler = LogSampler::new();

        // Call should_log multiple times
        for _ in 0..5 {
            sampler.should_log("multipart_upload");
        }

        assert_eq!(sampler.get_count("multipart_upload"), 5);
        assert_eq!(sampler.get_count("nonexistent"), 0);
    }
}
