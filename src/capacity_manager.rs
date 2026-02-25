//! Capacity Manager Module
//!
//! Provides capacity checking and management for signed PUT request caching.
//! Implements logic to determine whether requests should be cached based on
//! available capacity and Content-Length headers.

use crate::{ProxyError, Result};
use tracing::{debug, warn};

/// Decision on whether to cache a PUT request
#[derive(Debug, Clone, PartialEq)]
pub enum CacheDecision {
    /// Cache the request
    Cache,
    /// Bypass caching due to capacity constraints
    Bypass(BypassReason),
    /// Stream and check capacity during upload
    StreamWithCapacityCheck,
}

/// Reason for bypassing cache
#[derive(Debug, Clone, PartialEq)]
pub enum BypassReason {
    /// Content-Length exceeds available capacity
    ContentLengthExceedsCapacity {
        content_length: u64,
        available_capacity: u64,
    },
    /// No Content-Length header provided
    NoContentLength,
    /// Cache is disabled
    CacheDisabled,
}

impl std::fmt::Display for BypassReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BypassReason::ContentLengthExceedsCapacity {
                content_length,
                available_capacity,
            } => write!(
                f,
                "Content-Length ({} bytes) exceeds available capacity ({} bytes)",
                content_length, available_capacity
            ),
            BypassReason::NoContentLength => {
                write!(
                    f,
                    "No Content-Length header, will stream with capacity check"
                )
            }
            BypassReason::CacheDisabled => write!(f, "Cache is disabled"),
        }
    }
}

/// Check if a PUT request should be cached based on capacity
///
/// # Arguments
///
/// * `content_length` - Optional Content-Length from request headers
/// * `current_usage` - Current cache usage in bytes
/// * `max_capacity` - Maximum cache capacity in bytes
///
/// # Returns
///
/// Returns a `CacheDecision` indicating whether to cache, bypass, or stream with checks
///
/// # Requirements
///
/// - Requirement 2.1: Check if Content-Length fits within available cache capacity
/// - Requirement 2.2: Bypass cache if Content-Length exceeds capacity
/// - Requirement 2.3: Stream to cache until completion or capacity reached if no Content-Length
///
/// # Examples
///
/// ```
/// use s3_proxy::capacity_manager::check_cache_capacity;
///
/// // Request with Content-Length that fits
/// let decision = check_cache_capacity(Some(1000), 500, 2000);
/// // Should return CacheDecision::Cache
///
/// // Request with Content-Length that exceeds capacity
/// let decision = check_cache_capacity(Some(2000), 500, 1000);
/// // Should return CacheDecision::Bypass
///
/// // Request without Content-Length
/// let decision = check_cache_capacity(None, 500, 2000);
/// // Should return CacheDecision::StreamWithCapacityCheck
/// ```
pub fn check_cache_capacity(
    content_length: Option<u64>,
    current_usage: u64,
    max_capacity: u64,
) -> CacheDecision {
    // Calculate available capacity
    let available_capacity = max_capacity.saturating_sub(current_usage);

    debug!(
        "Checking cache capacity: content_length={:?}, current_usage={}, max_capacity={}, available={}",
        content_length, current_usage, max_capacity, available_capacity
    );

    match content_length {
        Some(len) => {
            // Requirement 2.1: Check if Content-Length fits within available capacity
            if len <= available_capacity {
                debug!(
                    "Content-Length ({} bytes) fits within available capacity ({} bytes), will cache",
                    len, available_capacity
                );
                CacheDecision::Cache
            } else {
                // Requirement 2.2: Bypass cache if Content-Length exceeds capacity
                warn!(
                    "Content-Length ({} bytes) exceeds available capacity ({} bytes), bypassing cache",
                    len, available_capacity
                );
                CacheDecision::Bypass(BypassReason::ContentLengthExceedsCapacity {
                    content_length: len,
                    available_capacity,
                })
            }
        }
        None => {
            // Requirement 2.3: Stream to cache until completion or capacity reached
            debug!(
                "No Content-Length header, will stream with capacity check (available: {} bytes)",
                available_capacity
            );
            CacheDecision::StreamWithCapacityCheck
        }
    }
}

/// Check if streaming should continue based on current bytes written
///
/// # Arguments
///
/// * `bytes_written` - Number of bytes written so far
/// * `current_usage` - Current cache usage in bytes (excluding this upload)
/// * `max_capacity` - Maximum cache capacity in bytes
///
/// # Returns
///
/// Returns `Ok(())` if streaming can continue, or an error if capacity is exceeded
///
/// # Requirements
///
/// - Requirement 2.4: Check capacity during streaming without Content-Length
pub fn check_streaming_capacity(
    bytes_written: u64,
    current_usage: u64,
    max_capacity: u64,
) -> Result<()> {
    let total_usage = current_usage.saturating_add(bytes_written);

    if total_usage > max_capacity {
        warn!(
            "Streaming capacity exceeded: bytes_written={}, current_usage={}, total={}, max_capacity={}",
            bytes_written, current_usage, total_usage, max_capacity
        );
        Err(ProxyError::CacheError(format!(
            "Streaming capacity exceeded: {} bytes written, total usage {} exceeds capacity {}",
            bytes_written, total_usage, max_capacity
        )))
    } else {
        debug!(
            "Streaming capacity check passed: bytes_written={}, total_usage={}, max_capacity={}",
            bytes_written, total_usage, max_capacity
        );
        Ok(())
    }
}

/// Log a cache bypass decision
///
/// # Arguments
///
/// * `cache_key` - The cache key for the request
/// * `reason` - The reason for bypassing cache
///
/// # Requirements
///
/// - Requirement 2.5: Log bypass decisions
pub fn log_bypass_decision(cache_key: &str, reason: &BypassReason) {
    warn!("Bypassing cache for key '{}': {}", cache_key, reason);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_cache_capacity_with_content_length_fits() {
        // Content-Length fits within available capacity
        let decision = check_cache_capacity(Some(1000), 500, 2000);
        assert_eq!(decision, CacheDecision::Cache);
    }

    #[test]
    fn test_check_cache_capacity_with_content_length_exceeds() {
        // Content-Length exceeds available capacity
        let decision = check_cache_capacity(Some(2000), 500, 1000);
        match decision {
            CacheDecision::Bypass(BypassReason::ContentLengthExceedsCapacity {
                content_length,
                available_capacity,
            }) => {
                assert_eq!(content_length, 2000);
                assert_eq!(available_capacity, 500);
            }
            _ => panic!("Expected Bypass decision"),
        }
    }

    #[test]
    fn test_check_cache_capacity_without_content_length() {
        // No Content-Length header
        let decision = check_cache_capacity(None, 500, 2000);
        assert_eq!(decision, CacheDecision::StreamWithCapacityCheck);
    }

    #[test]
    fn test_check_cache_capacity_exact_fit() {
        // Content-Length exactly matches available capacity
        let decision = check_cache_capacity(Some(1500), 500, 2000);
        assert_eq!(decision, CacheDecision::Cache);
    }

    #[test]
    fn test_check_cache_capacity_one_byte_over() {
        // Content-Length is one byte over available capacity
        let decision = check_cache_capacity(Some(1501), 500, 2000);
        match decision {
            CacheDecision::Bypass(BypassReason::ContentLengthExceedsCapacity { .. }) => {}
            _ => panic!("Expected Bypass decision"),
        }
    }

    #[test]
    fn test_check_streaming_capacity_within_limit() {
        // Streaming within capacity
        let result = check_streaming_capacity(500, 500, 2000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_streaming_capacity_exceeds_limit() {
        // Streaming exceeds capacity
        let result = check_streaming_capacity(1600, 500, 2000);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_streaming_capacity_exact_limit() {
        // Streaming exactly at capacity
        let result = check_streaming_capacity(1500, 500, 2000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_streaming_capacity_one_byte_over() {
        // Streaming one byte over capacity
        let result = check_streaming_capacity(1501, 500, 2000);
        assert!(result.is_err());
    }

    #[test]
    fn test_bypass_reason_display() {
        let reason = BypassReason::ContentLengthExceedsCapacity {
            content_length: 2000,
            available_capacity: 1000,
        };
        let display = format!("{}", reason);
        assert!(display.contains("2000"));
        assert!(display.contains("1000"));
    }

    #[test]
    fn test_check_cache_capacity_zero_capacity() {
        // Zero available capacity
        let decision = check_cache_capacity(Some(100), 1000, 1000);
        match decision {
            CacheDecision::Bypass(BypassReason::ContentLengthExceedsCapacity {
                content_length,
                available_capacity,
            }) => {
                assert_eq!(content_length, 100);
                assert_eq!(available_capacity, 0);
            }
            _ => panic!("Expected Bypass decision"),
        }
    }

    #[test]
    fn test_check_cache_capacity_overflow_protection() {
        // Test that saturating_sub prevents overflow
        let decision = check_cache_capacity(Some(100), u64::MAX, 1000);
        match decision {
            CacheDecision::Bypass(BypassReason::ContentLengthExceedsCapacity {
                available_capacity,
                ..
            }) => {
                assert_eq!(available_capacity, 0);
            }
            _ => panic!("Expected Bypass decision"),
        }
    }
}
