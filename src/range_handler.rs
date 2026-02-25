//! Range Handler Module
//!
//! Handles HTTP Range requests for partial content retrieval with caching support.
//! Implements range header parsing, validation, overlap detection, and merging.

use crate::cache::{CacheManager, Range};
use crate::cache_types::ObjectMetadata;
use crate::disk_cache::DiskCacheManager;
use crate::{ProxyError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Parsed HTTP Range specification
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSpec {
    pub start: u64,
    pub end: u64,
}

impl RangeSpec {
    /// Calculate the number of bytes in this range
    ///
    /// # Returns
    /// The total number of bytes from start to end (inclusive)
    ///
    /// # Example
    /// ```
    /// # use s3_proxy::range_handler::RangeSpec;
    /// # let range = RangeSpec { start: 0, end: 99 };
    /// # assert_eq!(range.len(), 100);
    /// ```
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    /// Returns true if the range has zero length (which is invalid for HTTP ranges)
    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    /// Calculate the slice offset within a containing range
    ///
    /// This method calculates where to start reading within a cached range
    /// to extract the bytes for this range.
    ///
    /// # Arguments
    /// * `container` - The larger range that contains this range
    ///
    /// # Returns
    /// The byte offset within the container where this range starts
    ///
    /// # Example
    /// ```
    /// # use s3_proxy::range_handler::RangeSpec;
    /// # let requested = RangeSpec { start: 100, end: 200 };
    /// # let cached = RangeSpec { start: 0, end: 500 };
    /// # assert_eq!(requested.slice_offset_in(&cached), 100);
    /// ```
    ///
    /// Requirements: 1.2, 3.2, 3.3, 3.4
    pub fn slice_offset_in(&self, container: &RangeSpec) -> u64 {
        self.start - container.start
    }

    /// Calculate the slice length to extract from a containing range
    ///
    /// This method calculates how many bytes to read from a cached range
    /// to extract the bytes for this range.
    ///
    /// # Returns
    /// The number of bytes to extract as a usize for direct use with slice operations
    ///
    /// # Example
    /// ```
    /// # use s3_proxy::range_handler::RangeSpec;
    /// # let range = RangeSpec { start: 100, end: 200 };
    /// # assert_eq!(range.slice_length(), 101);
    /// ```
    ///
    /// Requirements: 1.2, 3.2, 3.3, 3.4
    pub fn slice_length(&self) -> usize {
        (self.end - self.start + 1) as usize
    }

    /// Validate that this range can be sliced from a container range
    ///
    /// Checks that:
    /// 1. This range is fully contained within the container range
    /// 2. The slice offset and length don't exceed the container's data length
    ///
    /// # Arguments
    /// * `container` - The larger range that should contain this range
    /// * `container_data_len` - The actual length of data available in the container
    ///
    /// # Returns
    /// Ok(()) if the slice is valid, Err with details if invalid
    ///
    /// # Example
    /// ```
    /// # use s3_proxy::range_handler::RangeSpec;
    /// # let requested = RangeSpec { start: 100, end: 200 };
    /// # let cached = RangeSpec { start: 0, end: 500 };
    /// # assert!(requested.validate_slice_bounds(&cached, 501).is_ok());
    /// # assert!(requested.validate_slice_bounds(&cached, 100).is_err());
    /// ```
    ///
    /// Requirements: 1.2, 3.2, 3.3, 3.4
    pub fn validate_slice_bounds(
        &self,
        container: &RangeSpec,
        container_data_len: usize,
    ) -> Result<()> {
        // Ensure requested range is within container range
        if self.start < container.start {
            return Err(ProxyError::InvalidRange(format!(
                "Requested range start {} is before container range start {}",
                self.start, container.start
            )));
        }

        if self.end > container.end {
            return Err(ProxyError::InvalidRange(format!(
                "Requested range end {} is after container range end {}",
                self.end, container.end
            )));
        }

        // Calculate slice parameters
        let slice_offset = self.slice_offset_in(container);
        let slice_length = self.slice_length();

        // Ensure slice doesn't exceed container data length
        let slice_end = slice_offset as usize + slice_length;
        if slice_end > container_data_len {
            return Err(ProxyError::InvalidRange(format!(
                "Slice end {} exceeds container data length {}",
                slice_end, container_data_len
            )));
        }

        // Ensure the container data length matches the container range size
        let expected_container_len = (container.end - container.start + 1) as usize;
        if container_data_len != expected_container_len {
            return Err(ProxyError::InvalidRange(format!(
                "Container data length {} does not match container range size {}",
                container_data_len, expected_container_len
            )));
        }

        Ok(())
    }
}

/// Range request parsing result
#[derive(Debug, Clone)]
pub enum RangeParseResult {
    /// Valid single range request
    SingleRange(RangeSpec),
    /// Multiple ranges (not supported yet)
    MultipleRanges(Vec<RangeSpec>),
    /// Invalid range specification
    Invalid(String),
    /// No range header present
    None,
}

/// Range overlap detection result
#[derive(Debug, Clone)]
pub struct RangeOverlap {
    pub cached_ranges: Vec<Range>,
    pub missing_ranges: Vec<RangeSpec>,
    pub can_serve_from_cache: bool,
}

/// Represents a segment of data to be merged
/// Used in range merging operations to track data sources and positions
#[derive(Debug, Clone)]
pub struct RangeMergeSegment {
    /// Absolute start position in the final merged output
    pub output_start: u64,
    /// Absolute end position in the final merged output
    pub output_end: u64,
    /// Source of this segment (cached or fetched)
    pub source: RangeMergeSource,
}

/// Source of data for a merge segment
#[derive(Debug, Clone)]
pub enum RangeMergeSource {
    /// Data from cache with range metadata
    Cached {
        /// The cached range containing the data
        range: Range,
        /// Offset within the cached range to start reading
        read_offset: u64,
        /// Number of bytes to read from cached range
        read_length: u64,
    },
    /// Data fetched from S3
    Fetched {
        /// The actual data bytes
        data: Vec<u8>,
        /// Range specification for this data
        range_spec: RangeSpec,
    },
}

/// Result of range merging operation with metrics
#[derive(Debug)]
pub struct RangeMergeResult {
    /// Merged data ready to send to client
    pub data: Vec<u8>,
    /// Number of bytes served from cache
    pub bytes_from_cache: u64,
    /// Number of bytes fetched from S3
    pub bytes_from_s3: u64,
    /// Number of segments merged
    pub segments_merged: usize,
    /// Cache efficiency percentage (0-100)
    pub cache_efficiency: f64,
    /// Whether all cached segments came from RAM cache
    pub ram_hit: bool,
}

/// Range handler for processing HTTP range requests
pub struct RangeHandler {
    cache_manager: std::sync::Arc<CacheManager>,
    disk_cache_manager: Arc<tokio::sync::RwLock<DiskCacheManager>>,
}

impl RangeHandler {
    /// Create a new range handler
    pub fn new(
        cache_manager: std::sync::Arc<CacheManager>,
        disk_cache_manager: Arc<tokio::sync::RwLock<DiskCacheManager>>,
    ) -> Self {
        Self {
            cache_manager,
            disk_cache_manager,
        }
    }

    /// Get reference to disk cache manager for TTL operations
    pub fn get_disk_cache_manager(&self) -> &Arc<tokio::sync::RwLock<DiskCacheManager>> {
        &self.disk_cache_manager
    }

    /// Get reference to cache manager for eviction operations
    pub fn get_cache_manager(&self) -> &std::sync::Arc<CacheManager> {
        &self.cache_manager
    }

    /// Parse HTTP Range header and validate byte ranges - Requirements 3.1, 3.5
    pub fn parse_range_header(
        &self,
        range_header: &str,
        content_length: Option<u64>,
    ) -> RangeParseResult {
        debug!("Parsing Range header: {}", range_header);

        // Range header format: "bytes=start-end" or "bytes=start-" or "bytes=-suffix"
        if !range_header.starts_with("bytes=") {
            warn!("Invalid Range header format: {}", range_header);
            return RangeParseResult::Invalid("Range header must start with 'bytes='".to_string());
        }

        let ranges_str = &range_header[6..]; // Remove "bytes=" prefix
        let range_specs: Vec<&str> = ranges_str.split(',').collect();

        if range_specs.is_empty() {
            return RangeParseResult::Invalid("No range specifications found".to_string());
        }

        let mut parsed_ranges = Vec::new();

        for range_spec in range_specs {
            let range_spec = range_spec.trim();

            match self.parse_single_range_spec(range_spec, content_length) {
                Ok(range) => parsed_ranges.push(range),
                Err(e) => {
                    debug!("Range parse error '{}': {}, content_length={:?}", range_spec, e, content_length);
                    return RangeParseResult::Invalid(format!(
                        "Invalid range specification: {}",
                        e
                    ));
                }
            }
        }

        if parsed_ranges.is_empty() {
            return RangeParseResult::Invalid("No valid range specifications".to_string());
        }

        if parsed_ranges.len() == 1 {
            RangeParseResult::SingleRange(parsed_ranges.into_iter().next().unwrap())
        } else {
            RangeParseResult::MultipleRanges(parsed_ranges)
        }
    }
    /// Parse a single range specification
    fn parse_single_range_spec(
        &self,
        range_spec: &str,
        content_length: Option<u64>,
    ) -> Result<RangeSpec> {
        if range_spec.contains('-') {
            let parts: Vec<&str> = range_spec.split('-').collect();
            if parts.len() != 2 {
                return Err(ProxyError::InvalidRange(
                    "Range specification must contain exactly one dash".to_string(),
                ));
            }

            let start_str = parts[0].trim();
            let end_str = parts[1].trim();

            match (start_str.is_empty(), end_str.is_empty()) {
                (false, false) => {
                    // Both start and end specified: "start-end"
                    let start = start_str.parse::<u64>().map_err(|_| {
                        ProxyError::InvalidRange("Invalid start position".to_string())
                    })?;
                    let end = end_str.parse::<u64>().map_err(|_| {
                        ProxyError::InvalidRange("Invalid end position".to_string())
                    })?;

                    if start > end {
                        return Err(ProxyError::InvalidRange(
                            "Start position cannot be greater than end position".to_string(),
                        ));
                    }

                    // Validate against content length if known
                    if let Some(length) = content_length {
                        if start >= length {
                            return Err(ProxyError::InvalidRange(
                                "Start position exceeds content length".to_string(),
                            ));
                        }
                        // Clamp end to content length - 1
                        let end = std::cmp::min(end, length - 1);
                        Ok(RangeSpec { start, end })
                    } else {
                        Ok(RangeSpec { start, end })
                    }
                }
                (false, true) => {
                    // Only start specified: "start-" (from start to end of file)
                    let start = start_str.parse::<u64>().map_err(|_| {
                        ProxyError::InvalidRange("Invalid start position".to_string())
                    })?;

                    if let Some(length) = content_length {
                        if start >= length {
                            return Err(ProxyError::InvalidRange(
                                "Start position exceeds content length".to_string(),
                            ));
                        }
                        Ok(RangeSpec {
                            start,
                            end: length - 1,
                        })
                    } else {
                        // Without content length, we can't determine the end
                        Err(ProxyError::InvalidRange(
                            "Cannot determine end position without content length".to_string(),
                        ))
                    }
                }
                (true, false) => {
                    // Only end specified: "-suffix" (last N bytes)
                    let suffix = end_str.parse::<u64>().map_err(|_| {
                        ProxyError::InvalidRange("Invalid suffix length".to_string())
                    })?;

                    if let Some(length) = content_length {
                        if suffix == 0 {
                            return Err(ProxyError::InvalidRange(
                                "Suffix length cannot be zero".to_string(),
                            ));
                        }
                        let start = length.saturating_sub(suffix);
                        Ok(RangeSpec {
                            start,
                            end: length - 1,
                        })
                    } else {
                        Err(ProxyError::InvalidRange(
                            "Cannot determine suffix range without content length".to_string(),
                        ))
                    }
                }
                (true, true) => {
                    // Both empty: "-"
                    Err(ProxyError::InvalidRange(
                        "Empty range specification".to_string(),
                    ))
                }
            }
        } else {
            Err(ProxyError::InvalidRange(
                "Range specification must contain a dash".to_string(),
            ))
        }
    }

    /// Validate range request against object constraints
    pub fn validate_range_request(&self, range: &RangeSpec, content_length: u64) -> Result<()> {
        if range.start >= content_length {
            return Err(ProxyError::InvalidRange(
                "Range start exceeds content length".to_string(),
            ));
        }

        if range.end >= content_length {
            return Err(ProxyError::InvalidRange(
                "Range end exceeds content length".to_string(),
            ));
        }

        if range.start > range.end {
            return Err(ProxyError::InvalidRange(
                "Range start cannot be greater than end".to_string(),
            ));
        }

        Ok(())
    }

    /// Check if range request is satisfiable
    pub fn is_range_satisfiable(&self, range: &RangeSpec, content_length: u64) -> bool {
        range.start < content_length && range.end < content_length && range.start <= range.end
    }

    /// Convert range specification to HTTP Content-Range header value
    pub fn build_content_range_header(&self, range: &RangeSpec, content_length: u64) -> String {
        format!("bytes {}-{}/{}", range.start, range.end, content_length)
    }

    /// Calculate range size in bytes
    pub fn calculate_range_size(&self, range: &RangeSpec) -> u64 {
        range.end - range.start + 1
    }

    /// Check if two ranges overlap
    pub fn ranges_overlap(&self, range1: &RangeSpec, range2: &RangeSpec) -> bool {
        !(range1.end < range2.start || range2.end < range1.start)
    }

    /// Check if one range contains another
    pub fn range_contains(&self, container: &RangeSpec, contained: &RangeSpec) -> bool {
        container.start <= contained.start && container.end >= contained.end
    }

    /// Merge overlapping or adjacent ranges
    pub fn merge_ranges(&self, ranges: &[RangeSpec]) -> Vec<RangeSpec> {
        if ranges.is_empty() {
            return Vec::new();
        }

        let mut sorted_ranges = ranges.to_vec();
        sorted_ranges.sort_by_key(|r| r.start);

        let mut merged = Vec::new();
        let mut current = sorted_ranges[0].clone();

        for range in sorted_ranges.iter().skip(1) {
            // Check if ranges overlap or are adjacent
            if current.end + 1 >= range.start {
                // Merge ranges
                current.end = std::cmp::max(current.end, range.end);
            } else {
                // No overlap, add current range and start new one
                merged.push(current);
                current = range.clone();
            }
        }

        merged.push(current);
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_range_handler() -> RangeHandler {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let cache_manager = Arc::new(CacheManager::new_with_defaults(cache_dir.clone(), false, 0));
        let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
            cache_dir, true, 1024, false,
        )));
        RangeHandler::new(cache_manager, disk_cache_manager)
    }

    #[test]
    fn test_parse_valid_range_header() {
        let handler = create_test_range_handler();

        // Test "bytes=0-499"
        match handler.parse_range_header("bytes=0-499", Some(1000)) {
            RangeParseResult::SingleRange(range) => {
                assert_eq!(range.start, 0);
                assert_eq!(range.end, 499);
            }
            _ => panic!("Expected SingleRange"),
        }

        // Test "bytes=500-"
        match handler.parse_range_header("bytes=500-", Some(1000)) {
            RangeParseResult::SingleRange(range) => {
                assert_eq!(range.start, 500);
                assert_eq!(range.end, 999);
            }
            _ => panic!("Expected SingleRange"),
        }

        // Test "bytes=-200"
        match handler.parse_range_header("bytes=-200", Some(1000)) {
            RangeParseResult::SingleRange(range) => {
                assert_eq!(range.start, 800);
                assert_eq!(range.end, 999);
            }
            _ => panic!("Expected SingleRange"),
        }
    }

    #[test]
    fn test_parse_invalid_range_header() {
        let handler = create_test_range_handler();

        // Test invalid format
        match handler.parse_range_header("invalid", Some(1000)) {
            RangeParseResult::Invalid(_) => {}
            _ => panic!("Expected Invalid"),
        }

        // Test invalid range
        match handler.parse_range_header("bytes=500-100", Some(1000)) {
            RangeParseResult::Invalid(_) => {}
            _ => panic!("Expected Invalid"),
        }
    }

    #[test]
    fn test_range_validation() {
        let handler = create_test_range_handler();
        let range = RangeSpec { start: 0, end: 499 };

        // Valid range
        assert!(handler.validate_range_request(&range, 1000).is_ok());

        // Invalid range - exceeds content length
        assert!(handler.validate_range_request(&range, 400).is_err());
    }

    #[test]
    fn test_range_overlap_detection() {
        let handler = create_test_range_handler();
        let range1 = RangeSpec { start: 0, end: 100 };
        let range2 = RangeSpec {
            start: 50,
            end: 150,
        };
        let range3 = RangeSpec {
            start: 200,
            end: 300,
        };

        assert!(handler.ranges_overlap(&range1, &range2));
        assert!(!handler.ranges_overlap(&range1, &range3));
    }

    #[test]
    fn test_range_merging() {
        let handler = create_test_range_handler();
        let ranges = vec![
            RangeSpec { start: 0, end: 100 },
            RangeSpec {
                start: 50,
                end: 150,
            },
            RangeSpec {
                start: 200,
                end: 300,
            },
        ];

        let merged = handler.merge_ranges(&ranges);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0], RangeSpec { start: 0, end: 150 });
        assert_eq!(
            merged[1],
            RangeSpec {
                start: 200,
                end: 300
            }
        );
    }

    #[test]
    fn test_range_spec_len() {
        let range = RangeSpec { start: 0, end: 99 };
        assert_eq!(range.len(), 100);

        let range = RangeSpec {
            start: 100,
            end: 200,
        };
        assert_eq!(range.len(), 101);

        let range = RangeSpec { start: 0, end: 0 };
        assert_eq!(range.len(), 1);
    }

    #[test]
    fn test_slice_offset_in() {
        // Test at start of cached range
        let requested = RangeSpec { start: 0, end: 100 };
        let cached = RangeSpec { start: 0, end: 500 };
        assert_eq!(requested.slice_offset_in(&cached), 0);

        // Test in middle of cached range
        let requested = RangeSpec {
            start: 100,
            end: 200,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert_eq!(requested.slice_offset_in(&cached), 100);

        // Test at end of cached range
        let requested = RangeSpec {
            start: 400,
            end: 500,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert_eq!(requested.slice_offset_in(&cached), 400);

        // Test with non-zero cached start
        let requested = RangeSpec {
            start: 8388608,
            end: 16777215,
        };
        let cached = RangeSpec {
            start: 8388608,
            end: 16777269,
        };
        assert_eq!(requested.slice_offset_in(&cached), 0);
    }

    #[test]
    fn test_slice_length() {
        let range = RangeSpec { start: 0, end: 99 };
        assert_eq!(range.slice_length(), 100);

        let range = RangeSpec {
            start: 100,
            end: 200,
        };
        assert_eq!(range.slice_length(), 101);

        let range = RangeSpec {
            start: 0,
            end: 8388607,
        };
        assert_eq!(range.slice_length(), 8388608);
    }

    #[test]
    fn test_validate_slice_bounds_valid() {
        // Test exact match
        let requested = RangeSpec { start: 0, end: 99 };
        let cached = RangeSpec { start: 0, end: 99 };
        assert!(requested.validate_slice_bounds(&cached, 100).is_ok());

        // Test at start of cached range
        let requested = RangeSpec { start: 0, end: 99 };
        let cached = RangeSpec { start: 0, end: 500 };
        assert!(requested.validate_slice_bounds(&cached, 501).is_ok());

        // Test in middle of cached range
        let requested = RangeSpec {
            start: 100,
            end: 200,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert!(requested.validate_slice_bounds(&cached, 501).is_ok());

        // Test at end of cached range
        let requested = RangeSpec {
            start: 400,
            end: 500,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert!(requested.validate_slice_bounds(&cached, 501).is_ok());
    }

    #[test]
    fn test_validate_slice_bounds_invalid() {
        // Test requested range starts before container
        let requested = RangeSpec { start: 0, end: 100 };
        let cached = RangeSpec {
            start: 50,
            end: 500,
        };
        assert!(requested.validate_slice_bounds(&cached, 451).is_err());

        // Test requested range ends after container
        let requested = RangeSpec {
            start: 100,
            end: 600,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert!(requested.validate_slice_bounds(&cached, 501).is_err());

        // Test slice exceeds container data length
        let requested = RangeSpec {
            start: 100,
            end: 200,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert!(requested.validate_slice_bounds(&cached, 150).is_err());

        // Test container data length mismatch
        let requested = RangeSpec {
            start: 100,
            end: 200,
        };
        let cached = RangeSpec { start: 0, end: 500 };
        assert!(requested.validate_slice_bounds(&cached, 400).is_err());
    }

    #[test]
    fn test_slice_bounds_multipart_scenario() {
        // Test the specific bug scenario: requested 0-8388607, cached 0-8388661
        let requested = RangeSpec {
            start: 0,
            end: 8388607,
        };
        let cached = RangeSpec {
            start: 0,
            end: 8388661,
        };

        // With correct cached data length (8388662 bytes)
        assert!(requested.validate_slice_bounds(&cached, 8388662).is_ok());
        assert_eq!(requested.slice_offset_in(&cached), 0);
        assert_eq!(requested.slice_length(), 8388608);

        // Verify we can extract the correct slice
        let offset = requested.slice_offset_in(&cached) as usize;
        let length = requested.slice_length();
        assert_eq!(offset, 0);
        assert_eq!(length, 8388608);
        assert_eq!(offset + length, 8388608);
    }
}

impl RangeHandler {
    /// Find cached ranges that overlap with requested range - Requirements 3.2, 3.3
    ///
    /// This method now uses the new range storage architecture:
    /// 1. First checks the new storage (metadata + separate range files)
    /// 2. Falls back to old storage for backward compatibility
    /// 3. Returns overlapping ranges that can be used to serve the request
    /// 4. Validates ETag if current_etag is provided (Requirement 3.3)
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `requested_range` - The range being requested
    /// * `current_etag` - Optional current ETag from S3 HEAD/GET response for validation
    ///
    /// # ETag Validation (Requirement 3.3)
    /// If current_etag is provided and doesn't match the cached metadata ETag:
    /// - Logs ETag mismatch warning
    /// - Treats all cached ranges as missing (returns empty cached_ranges)
    /// - Caller should invalidate stale ranges and fetch fresh data from S3
    pub async fn find_cached_ranges(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        current_etag: Option<&str>,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
    ) -> Result<RangeOverlap> {
        // Requirement 1.1: Log cache key, requested range, and current ETag
        debug!(
            "[CACHE_LOOKUP] Starting cache lookup: cache_key={}, requested_range={}-{}, current_etag={:?}",
            cache_key, requested_range.start, requested_range.end, current_etag
        );

        // Try new storage architecture first
        debug!(
            "[CACHE_LOOKUP] Checking new storage architecture for cache_key: {}",
            cache_key
        );
        let disk_cache = self.disk_cache_manager.read().await;

        // Check if we have metadata: use preloaded if provided, otherwise read from disk
        let metadata_opt = if let Some(preloaded) = preloaded_metadata {
            debug!(
                "[CACHE_LOOKUP] Using preloaded metadata: cache_key={}, ranges_count={}, etag={}",
                cache_key,
                preloaded.ranges.len(),
                preloaded.object_metadata.etag
            );
            Some(preloaded.clone())
        } else {
            disk_cache.get_metadata(cache_key).await?
        };

        if let Some(metadata) = metadata_opt {
            debug!(
                "[CACHE_LOOKUP] Found new-format metadata: cache_key={}, ranges_count={}, etag={}",
                cache_key,
                metadata.ranges.len(),
                metadata.object_metadata.etag
            );

            // Requirement 3.3: ETag mismatch detection and invalidation
            // If current_etag is provided, compare with cached metadata ETag
            if let Some(current_etag) = current_etag {
                if current_etag != metadata.object_metadata.etag {
                    warn!(
                        "ETag mismatch detected for cache_key: {}, cached_etag: {}, current_etag: {}, action: invalidating stale ranges",
                        cache_key, metadata.object_metadata.etag, current_etag
                    );

                    // Requirements 2.2, 2.3: Invalidate stale ranges and log the action
                    let ranges_count = metadata.ranges.len();
                    if let Err(e) = disk_cache.invalidate_all_ranges(cache_key).await {
                        warn!(
                            "Failed to invalidate stale ranges: cache_key={}, error={}, treating as missing anyway",
                            cache_key, e
                        );
                    } else {
                        info!(
                            "ETag validation failed: invalidated {} stale ranges for cache_key={}, cached_etag={}, current_etag={}",
                            ranges_count, cache_key, metadata.object_metadata.etag, current_etag
                        );
                    }

                    // Return empty cached ranges, forcing a fetch from S3
                    return Ok(RangeOverlap {
                        cached_ranges: Vec::new(),
                        missing_ranges: vec![requested_range.clone()],
                        can_serve_from_cache: false,
                    });
                } else {
                    debug!(
                        "ETag validation passed for cache_key: {}, etag: {}",
                        cache_key, current_etag
                    );
                }
            }

            // Requirement 1.4: Log each range in metadata for debugging
            for (i, range) in metadata.ranges.iter().enumerate() {
                debug!(
                    "[CACHE_LOOKUP] Metadata range {}: cache_key={}, start={}, end={}",
                    i,
                    cache_key,
                    range.start,
                    range.end,
                );
            }

            // Find overlapping ranges using the new storage
            debug!(
                "[CACHE_LOOKUP] Calling disk_cache.find_cached_ranges: cache_key={}, requested_range={}-{}",
                cache_key, requested_range.start, requested_range.end
            );
            let overlapping_range_specs = disk_cache
                .find_cached_ranges(cache_key, requested_range.start, requested_range.end, preloaded_metadata)
                .await?;

            debug!(
                "[CACHE_LOOKUP] disk_cache.find_cached_ranges returned: cache_key={}, overlapping_ranges_count={}",
                cache_key, overlapping_range_specs.len()
            );

            if !overlapping_range_specs.is_empty() {
                debug!(
                    "[CACHE_LOOKUP] Found overlapping ranges in new storage: cache_key={}, count={}",
                    cache_key, overlapping_range_specs.len()
                );

                // Requirement 1.4: Log each overlapping range for debugging
                for (i, range_spec) in overlapping_range_specs.iter().enumerate() {
                    debug!(
                        "[CACHE_LOOKUP] Overlapping range {}: cache_key={}, start={}, end={}, compression={:?}",
                        i, cache_key, range_spec.start, range_spec.end, range_spec.compression_algorithm
                    );
                }

                // Convert NewRangeSpec to old Range format for compatibility
                // Note: We don't load the data yet - that happens when serving
                let mut overlapping_ranges = Vec::new();
                let mut covered_ranges = Vec::new();

                for range_spec in &overlapping_range_specs {
                    // Create a Range struct without data (will be loaded on demand)
                    overlapping_ranges.push(Range {
                        start: range_spec.start,
                        end: range_spec.end,
                        data: Vec::new(), // Data will be loaded when needed
                        etag: metadata.object_metadata.etag.clone(),
                        last_modified: metadata.object_metadata.last_modified.clone(),
                        compression_algorithm: range_spec.compression_algorithm.clone(),
                    });

                    // Calculate the intersection with requested range
                    let intersection_start = std::cmp::max(requested_range.start, range_spec.start);
                    let intersection_end = std::cmp::min(requested_range.end, range_spec.end);

                    covered_ranges.push(RangeSpec {
                        start: intersection_start,
                        end: intersection_end,
                    });
                }

                // Calculate missing ranges
                let missing_ranges =
                    self.calculate_missing_ranges(requested_range, &covered_ranges);
                let can_serve_from_cache = missing_ranges.is_empty();

                debug!(
                    "[CACHE_LOOKUP] Cache lookup result: cache_key={}, overlapping_ranges={}, missing_ranges={}, requested_range={}-{}, can_serve_from_cache={}",
                    cache_key, overlapping_ranges.len(), missing_ranges.len(), requested_range.start, requested_range.end, can_serve_from_cache
                );

                return Ok(RangeOverlap {
                    cached_ranges: overlapping_ranges,
                    missing_ranges,
                    can_serve_from_cache,
                });
            } else {
                debug!(
                    "[CACHE_LOOKUP] No overlapping ranges found in new storage: cache_key={}",
                    cache_key
                );
            }
        } else {
            debug!(
                "[CACHE_LOOKUP] No new-format metadata found: cache_key={}",
                cache_key
            );
        }

        // Release the lock
        drop(disk_cache);

        // No cached ranges found in new storage
        debug!(
            "[CACHE_LOOKUP] No cached entry found: cache_key={}",
            cache_key
        );
        Ok(RangeOverlap {
            cached_ranges: Vec::new(),
            missing_ranges: vec![requested_range.clone()],
            can_serve_from_cache: false,
        })
    }

    /// Calculate missing ranges that are not covered by cached ranges
    fn calculate_missing_ranges(
        &self,
        requested: &RangeSpec,
        covered: &[RangeSpec],
    ) -> Vec<RangeSpec> {
        if covered.is_empty() {
            return vec![requested.clone()];
        }

        // Merge and sort covered ranges
        let merged_covered = self.merge_ranges(covered);
        let mut missing = Vec::new();
        let mut current_pos = requested.start;

        for covered_range in &merged_covered {
            // If there's a gap before this covered range
            if current_pos < covered_range.start {
                missing.push(RangeSpec {
                    start: current_pos,
                    end: covered_range.start - 1,
                });
            }

            // Move past this covered range
            current_pos = std::cmp::max(current_pos, covered_range.end + 1);
        }

        // If there's a gap after the last covered range
        if current_pos <= requested.end {
            missing.push(RangeSpec {
                start: current_pos,
                end: requested.end,
            });
        }

        missing
    }

    /// Extract range data from full object body
    pub fn extract_range_from_body(&self, body: &[u8], range: &RangeSpec) -> Result<Vec<u8>> {
        let body_len = body.len() as u64;

        if range.start >= body_len {
            return Err(ProxyError::InvalidRange(
                "Range start exceeds body length".to_string(),
            ));
        }

        let end = std::cmp::min(range.end, body_len - 1);
        let start = range.start as usize;
        let end = end as usize;

        if start > end {
            return Err(ProxyError::InvalidRange("Invalid range bounds".to_string()));
        }

        Ok(body[start..=end].to_vec())
    }

    /// Load range data from new storage architecture
    ///
    /// This method loads the actual range data from the separate binary files
    /// in the new storage architecture. It should be called when serving a range
    /// that was found in the new storage format.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `range` - The Range struct (which may have empty data field)
    ///
    /// # Returns
    /// The actual range data loaded from disk
    pub async fn load_range_data_from_new_storage(
        &self,
        cache_key: &str,
        range: &Range,
    ) -> Result<Vec<u8>> {
        debug!(
            "Loading range data from new storage: key={}, range={}-{}",
            cache_key, range.start, range.end
        );

        let disk_cache = self.disk_cache_manager.read().await;

        // Get metadata to find the range spec
        let metadata = disk_cache.get_metadata(cache_key).await?;

        // Find the matching range spec in metadata
        let range_spec = if let Some(ref meta) = metadata {
            meta.ranges
                .iter()
                .find(|r| r.start == range.start && r.end == range.end)
                .cloned()
        } else {
            None
        };

        // If not found in metadata, check journals (shared storage mode)
        // This handles the case where find_cached_ranges returned a range from the journal
        // but the journal hasn't been consolidated into the .meta file yet
        let range_spec = match range_spec {
            Some(spec) => spec,
            None => {
                // Check journals for pending ranges
                let journal_ranges = disk_cache
                    .find_pending_journal_ranges(cache_key, range.start, range.end)
                    .await?;

                journal_ranges
                    .into_iter()
                    .find(|r| r.start == range.start && r.end == range.end)
                    .ok_or_else(|| {
                        ProxyError::CacheError(format!(
                            "Range spec not found for range {}-{} in key: {} (checked metadata and journals)",
                            range.start, range.end, cache_key
                        ))
                    })?
            }
        };

        // Load the range data
        let data = disk_cache.load_range_data(&range_spec).await?;

        // Release the lock before spawning async task
        drop(disk_cache);

        // Spawn async task to update range access statistics (Requirement 1.2)
        // Don't block the response on this update
        // Uses non-blocking access tracker instead of synchronous metadata update
        let disk_cache_manager = self.disk_cache_manager.clone();
        let cache_key_owned = cache_key.to_string();
        let range_start = range.start;
        let range_end = range.end;

        tokio::spawn(async move {
            let disk_cache = disk_cache_manager.read().await;
            if let Err(e) = disk_cache
                .record_range_access(&cache_key_owned, range_start, range_end)
                .await
            {
                tracing::debug!(
                    "Failed to record range access: key={}, range={}-{}, error={}",
                    cache_key_owned,
                    range_start,
                    range_end,
                    e
                );
            }
        });

        debug!(
            "Successfully loaded {} bytes for range {}-{}",
            data.len(),
            range.start,
            range.end
        );
        Ok(data)
    }

    /// Store range data using new storage architecture
    ///
    /// This method stores a range using the new separated storage architecture.
    /// It should be used when caching new range data.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `start` - Start offset of the range
    /// * `end` - End offset of the range
    /// * `data` - The range data to store
    /// * `object_metadata` - Metadata about the S3 object
    pub async fn store_range_new_storage(
        &self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: ObjectMetadata,
        ttl: std::time::Duration,
    ) -> Result<()> {
        debug!(
            "Storing range using new storage: key={}, range={}-{}, size={}",
            cache_key,
            start,
            end,
            data.len()
        );

        // Check capacity and evict if needed before caching new data (Requirement 1.6)
        let required_space = data.len() as u64;
        if let Err(e) = self.cache_manager.evict_if_needed(required_space).await {
            warn!("Eviction failed before caching range: {}", e);
            // Continue anyway - store_range will handle capacity issues
        }

        let mut disk_cache = self.disk_cache_manager.write().await;
        // Resolve per-bucket compression settings (Requirements 5.1, 5.2, 5.3)
        let resolved = self.cache_manager.resolve_settings(cache_key).await;
        disk_cache
            .store_range(cache_key, start, end, data, object_metadata, ttl, resolved.compression_enabled)
            .await?;

        debug!(
            "Successfully stored range {}-{} for key: {}",
            start, end, cache_key
        );
        Ok(())
    }

    /// Store full object using new storage architecture
    ///
    /// This method stores a full object as a range (0 to content_length-1)
    /// using the new storage architecture.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `data` - The full object data
    /// * `object_metadata` - Metadata about the S3 object
    pub async fn store_full_object_new_storage(
        &self,
        cache_key: &str,
        data: &[u8],
        object_metadata: ObjectMetadata,
        ttl: std::time::Duration,
    ) -> Result<()> {
        debug!(
            "Storing full object using new storage: key={}, size={}",
            cache_key,
            data.len()
        );

        // Check capacity and evict if needed before caching new data (Requirement 1.6)
        let required_space = data.len() as u64;
        if let Err(e) = self.cache_manager.evict_if_needed(required_space).await {
            warn!("Eviction failed before caching full object: {}", e);
            // Continue anyway - store_full_object_as_range will handle capacity issues
        }

        let mut disk_cache = self.disk_cache_manager.write().await;
        // Resolve per-bucket compression settings (Requirements 5.1, 5.2, 5.3)
        let resolved = self.cache_manager.resolve_settings(cache_key).await;
        disk_cache
            .store_full_object_as_range(cache_key, data, object_metadata, ttl, resolved.compression_enabled)
            .await?;

        info!("Successfully stored full object: {}", cache_key);
        Ok(())
    }

    /// Build conditional headers for uncached range portions - Requirements 3.6, 3.8
    pub fn build_conditional_headers_for_range(
        &self,
        client_headers: &HashMap<String, String>,
        cached_ranges: &[Range],
    ) -> HashMap<String, String> {
        let mut conditional_headers = HashMap::new();

        // Extract client-specified conditional headers
        let client_if_match = client_headers.get("if-match");
        let client_if_none_match = client_headers.get("if-none-match");
        let client_if_modified_since = client_headers.get("if-modified-since");
        let client_if_unmodified_since = client_headers.get("if-unmodified-since");

        // Get cache validation metadata from cached ranges
        let (cache_etag, cache_last_modified) = if let Some(cached_range) = cached_ranges.first() {
            (
                Some(cached_range.etag.clone()),
                Some(cached_range.last_modified.clone()),
            )
        } else {
            (None, None)
        };

        // Preserve client-specified conditional headers exactly - Requirement 3.8
        if let Some(if_match) = client_if_match {
            conditional_headers.insert("if-match".to_string(), if_match.clone());
        } else if let Some(etag) = &cache_etag {
            // Add cache validation header only if client didn't specify one
            conditional_headers.insert("if-match".to_string(), etag.clone());
        }

        if let Some(if_none_match) = client_if_none_match {
            conditional_headers.insert("if-none-match".to_string(), if_none_match.clone());
        }

        if let Some(if_modified_since) = client_if_modified_since {
            conditional_headers.insert("if-modified-since".to_string(), if_modified_since.clone());
        }

        if let Some(if_unmodified_since) = client_if_unmodified_since {
            conditional_headers.insert(
                "if-unmodified-since".to_string(),
                if_unmodified_since.clone(),
            );
        } else if let Some(last_modified) = &cache_last_modified {
            // Add cache validation header only if client didn't specify one
            conditional_headers.insert("if-unmodified-since".to_string(), last_modified.clone());
        }

        debug!(
            "Built conditional headers for range request: {:?}",
            conditional_headers
        );
        conditional_headers
    }

    /// Check if range request needs cache validation
    pub fn needs_cache_validation(
        &self,
        cached_ranges: &[Range],
        missing_ranges: &[RangeSpec],
    ) -> bool {
        // If we have cached ranges and missing ranges, we need validation
        // to ensure cached data is still valid when fetching missing portions
        !cached_ranges.is_empty() && !missing_ranges.is_empty()
    }

    /// Build partial range request for missing portions
    pub fn build_partial_range_request(
        &self,
        missing_ranges: &[RangeSpec],
        conditional_headers: &HashMap<String, String>,
    ) -> (String, HashMap<String, String>) {
        // Build Range header for missing portions
        let range_specs: Vec<String> = missing_ranges
            .iter()
            .map(|r| format!("{}-{}", r.start, r.end))
            .collect();
        let range_header = format!("bytes={}", range_specs.join(","));

        // Combine with conditional headers
        let mut request_headers = conditional_headers.clone();
        // Remove any existing Range header (case-insensitive) to avoid duplicates
        request_headers.retain(|k, _| k.to_lowercase() != "range");
        // Insert new Range header with proper capitalization
        request_headers.insert("Range".to_string(), range_header.clone());

        debug!(
            "Built partial range request: Range: {}, Conditionals: {:?}",
            range_header, conditional_headers
        );

        (range_header, request_headers)
    }

    // ============================================================================
    // Range Merging Helper Functions
    // ============================================================================

    /// Extract bytes from a cached range that overlap with requested range
    ///
    /// Handles three cases:
    /// 1. Full containment: cached range fully contains requested range
    /// 2. Partial overlap: cached range partially overlaps requested range
    /// 3. Boundary cases: start/end alignment
    ///
    /// Requirements: 2.2, 2.3
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `cached_range` - The cached range metadata
    /// * `requested_start` - Start offset of the requested range
    /// * `requested_end` - End offset of the requested range
    ///
    /// # Returns
    /// The extracted bytes from the cached range that overlap with the requested range
    ///
    /// # Errors
    /// Returns error if:
    /// - Cached range doesn't overlap with requested range
    /// - Failed to load cached range data
    /// - Offset or length parameters are invalid
    ///
    /// Returns (data, ram_hit) where ram_hit indicates if data came from RAM cache
    pub async fn extract_bytes_from_cached_range(
        &self,
        cache_key: &str,
        cached_range: &Range,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<(Vec<u8>, bool)> {
        debug!(
            "Extracting bytes from cached range: cached_range={}-{}, requested={}-{}, cache_key={}",
            cached_range.start, cached_range.end, requested_start, requested_end, cache_key
        );

        // Validate that ranges overlap
        if cached_range.end < requested_start || cached_range.start > requested_end {
            return Err(ProxyError::InvalidRange(format!(
                "Cached range {}-{} does not overlap with requested range {}-{}",
                cached_range.start, cached_range.end, requested_start, requested_end
            )));
        }

        // Calculate the intersection (overlap) between cached and requested ranges
        let intersection_start = std::cmp::max(cached_range.start, requested_start);
        let intersection_end = std::cmp::min(cached_range.end, requested_end);

        // Calculate offset within the cached range
        let read_offset = intersection_start - cached_range.start;
        let read_length = intersection_end - intersection_start + 1;

        debug!(
            "Extracting bytes: intersection={}-{}, read_offset={}, read_length={}",
            intersection_start, intersection_end, read_offset, read_length
        );

        // Validate offset and length parameters
        let cached_range_size = cached_range.end - cached_range.start + 1;
        if read_offset >= cached_range_size {
            return Err(ProxyError::InvalidRange(format!(
                "Read offset {} exceeds cached range size {}",
                read_offset, cached_range_size
            )));
        }

        if read_offset + read_length > cached_range_size {
            return Err(ProxyError::InvalidRange(format!(
                "Read offset {} + length {} exceeds cached range size {}",
                read_offset, read_length, cached_range_size
            )));
        }

        // Load the cached range data with RAM cache support
        debug!("Loading range data with RAM cache support");
        let (range_data, ram_hit) = self
            .cache_manager
            .load_range_data_with_cache(cache_key, cached_range, self)
            .await?;

        // Validate that we have enough data
        if range_data.len() as u64 != cached_range_size {
            return Err(ProxyError::CacheError(format!(
                "Cached range data size mismatch: expected {} bytes, got {} bytes",
                cached_range_size,
                range_data.len()
            )));
        }

        // Extract the requested portion
        let start_idx = read_offset as usize;
        let end_idx = (read_offset + read_length) as usize;

        if end_idx > range_data.len() {
            return Err(ProxyError::InvalidRange(format!(
                "Extraction range {}-{} exceeds data length {}",
                start_idx,
                end_idx,
                range_data.len()
            )));
        }

        let extracted_bytes = range_data[start_idx..end_idx].to_vec();

        debug!(
            "Successfully extracted {} bytes from cached range {}-{} (RAM hit: {})",
            extracted_bytes.len(),
            cached_range.start,
            cached_range.end,
            ram_hit
        );

        Ok((extracted_bytes, ram_hit))
    }

    /// Calculate the overlap between two ranges
    /// Returns None if ranges don't overlap, otherwise returns (overlap_start, overlap_end)
    pub fn calculate_overlap(
        range1_start: u64,
        range1_end: u64,
        range2_start: u64,
        range2_end: u64,
    ) -> Option<(u64, u64)> {
        // Check if ranges overlap
        if range1_end < range2_start || range2_end < range1_start {
            return None;
        }

        let overlap_start = std::cmp::max(range1_start, range2_start);
        let overlap_end = std::cmp::min(range1_end, range2_end);

        Some((overlap_start, overlap_end))
    }

    /// Check if a set of ranges fully covers a requested range
    /// Returns true if every byte in the requested range is covered by at least one range
    pub fn ranges_fully_cover(
        ranges: &[(u64, u64)],
        requested_start: u64,
        requested_end: u64,
    ) -> bool {
        if ranges.is_empty() {
            return false;
        }

        // Sort ranges by start position
        let mut sorted_ranges = ranges.to_vec();
        sorted_ranges.sort_by_key(|r| r.0);

        // Check if first range covers the start
        if sorted_ranges[0].0 > requested_start {
            return false;
        }

        // Track the furthest point we've covered
        let mut covered_to = sorted_ranges[0].1;

        // Check each subsequent range
        for range in sorted_ranges.iter().skip(1) {
            // If there's a gap, we don't have full coverage
            if range.0 > covered_to + 1 {
                return false;
            }
            // Extend coverage
            covered_to = std::cmp::max(covered_to, range.1);
        }

        // Check if we covered all the way to the end
        covered_to >= requested_end
    }

    /// Merge adjacent or overlapping ranges into minimal set
    /// Returns a sorted, non-overlapping list of ranges
    pub fn merge_adjacent_ranges(ranges: &[(u64, u64)]) -> Vec<(u64, u64)> {
        if ranges.is_empty() {
            return Vec::new();
        }

        // Sort ranges by start position
        let mut sorted_ranges = ranges.to_vec();
        sorted_ranges.sort_by_key(|r| r.0);

        let mut merged = Vec::new();
        let mut current = sorted_ranges[0];

        for range in sorted_ranges.iter().skip(1) {
            // If ranges overlap or are adjacent, merge them
            if range.0 <= current.1 + 1 {
                current.1 = std::cmp::max(current.1, range.1);
            } else {
                // No overlap, save current and start new range
                merged.push(current);
                current = *range;
            }
        }

        // Don't forget the last range
        merged.push(current);

        merged
    }
    /// Check if range merge is actually needed
    ///
    /// Returns false (no merge needed) if:
    /// - Requested range exactly matches a single cached range (Requirement 8.1)
    /// - Requested range is fully contained within a single cached range (Requirement 8.2)
    ///
    /// Returns true (merge needed) if:
    /// - Multiple segments need to be combined (Requirement 8.3, 8.4)
    ///
    /// # Arguments
    /// * `requested_range` - The range requested by the client
    /// * `cached_ranges` - Ranges available in cache
    /// * `fetched_ranges` - Ranges fetched from S3
    ///
    /// # Returns
    /// true if merge is needed, false if exact match or full containment
    ///
    /// Requirements: 8.1, 8.2, 8.3, 8.4
    fn check_if_merge_needed(
        &self,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Vec<u8>, HashMap<String, String>)],
    ) -> bool {
        // If we have both cached and fetched ranges, merge is needed
        if !cached_ranges.is_empty() && !fetched_ranges.is_empty() {
            return true;
        }

        // If we have multiple cached ranges, merge is needed
        if cached_ranges.len() > 1 {
            return true;
        }

        // If we have multiple fetched ranges, merge is needed
        if fetched_ranges.len() > 1 {
            return true;
        }

        // If we have exactly one cached range, check for exact match or full containment
        if cached_ranges.len() == 1 {
            let cached_range = &cached_ranges[0];

            // Requirement 8.1: Check for exact match
            if cached_range.start == requested_range.start
                && cached_range.end == requested_range.end
            {
                debug!(
                    "Exact match detected: requested={}-{}, cached={}-{}",
                    requested_range.start,
                    requested_range.end,
                    cached_range.start,
                    cached_range.end
                );
                return false;
            }

            // Requirement 8.2: Check for full containment
            if cached_range.start <= requested_range.start
                && cached_range.end >= requested_range.end
            {
                debug!(
                    "Full containment detected: requested={}-{}, cached={}-{}",
                    requested_range.start,
                    requested_range.end,
                    cached_range.start,
                    cached_range.end
                );
                return false;
            }
        }

        // If we have exactly one fetched range, check for exact match
        if fetched_ranges.len() == 1 {
            let fetched_range = &fetched_ranges[0].0;

            // Check for exact match
            if fetched_range.start == requested_range.start
                && fetched_range.end == requested_range.end
            {
                debug!(
                    "Exact match detected (fetched): requested={}-{}, fetched={}-{}",
                    requested_range.start,
                    requested_range.end,
                    fetched_range.start,
                    fetched_range.end
                );
                return false;
            }
        }

        // Default: merge is needed
        true
    }

    /// Consolidate missing ranges to minimize S3 requests
    /// Merges ranges with gaps smaller than threshold to reduce request count
    ///
    /// # Arguments
    /// * `missing_ranges` - List of missing byte ranges
    /// * `max_gap_size` - Maximum gap size to consolidate (default: 256KB)
    ///
    /// # Returns
    /// Minimal set of consolidated ranges that cover the same bytes
    ///
    /// Requirements: 1.2
    pub fn consolidate_missing_ranges(
        &self,
        missing_ranges: Vec<RangeSpec>,
        max_gap_size: u64,
    ) -> Vec<RangeSpec> {
        if missing_ranges.is_empty() {
            debug!("No missing ranges to consolidate");
            return Vec::new();
        }

        if missing_ranges.len() == 1 {
            debug!("Single missing range, no consolidation needed");
            return missing_ranges;
        }

        // Sort ranges by start position
        let mut sorted_ranges = missing_ranges;
        sorted_ranges.sort_by_key(|r| r.start);

        let mut consolidated = Vec::new();
        let mut current = sorted_ranges[0].clone();

        debug!(
            "Consolidating {} missing ranges with max_gap_size={} bytes",
            sorted_ranges.len(),
            max_gap_size
        );

        for range in sorted_ranges.iter().skip(1) {
            let gap = if range.start > current.end {
                range.start - current.end - 1
            } else {
                0 // Overlapping or adjacent
            };

            debug!(
                "Checking gap between {}-{} and {}-{}: {} bytes",
                current.start, current.end, range.start, range.end, gap
            );

            // If gap is small enough or ranges overlap/adjacent, merge them
            if gap <= max_gap_size {
                debug!(
                    "Merging ranges: {}-{} and {}-{} (gap: {} bytes)",
                    current.start, current.end, range.start, range.end, gap
                );
                // Extend current range to include this range
                current.end = std::cmp::max(current.end, range.end);
            } else {
                // Gap is too large, save current and start new range
                debug!("Gap too large ({}), keeping ranges separate", gap);
                consolidated.push(current);
                current = range.clone();
            }
        }

        // Don't forget the last range
        consolidated.push(current);

        let reduction = sorted_ranges.len() - consolidated.len();
        info!(
            "Consolidated {} missing ranges into {} ranges (reduction: {} ranges, {:.1}%)",
            sorted_ranges.len(),
            consolidated.len(),
            reduction,
            (reduction as f64 / sorted_ranges.len() as f64) * 100.0
        );

        consolidated
    }

    /// Merge multiple range segments into a single contiguous byte vector
    /// Validates alignment and extracts correct byte offsets from each range
    ///
    /// # Arguments
    /// * `cache_key` - Cache key for the object
    /// * `requested_range` - The range requested by the client
    /// * `cached_ranges` - Ranges available in cache
    /// * `fetched_ranges` - Ranges fetched from S3 with their data and headers (headers are ignored in merge)
    ///
    /// # Returns
    /// RangeMergeResult containing merged data and efficiency metrics
    ///
    /// Requirements: 1.5, 2.1, 2.4, 3.5, 8.1, 8.2, 8.3, 8.4
    pub async fn merge_range_segments(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Vec<u8>, HashMap<String, String>)],
    ) -> Result<RangeMergeResult> {
        let merge_start = std::time::Instant::now();

        // Requirement 8.1, 8.2: Check if merge is actually needed
        // Skip merge logging for exact match or full containment in single cached range
        let needs_merge =
            self.check_if_merge_needed(requested_range, cached_ranges, fetched_ranges);

        if needs_merge {
            // Log start only at debug level to reduce noise
            debug!(
                range_start = requested_range.start,
                range_end = requested_range.end,
                cached_segments = cached_ranges.len(),
                fetched_segments = fetched_ranges.len(),
                cache_key = %cache_key,
                "Range merge starting"
            );
        } else {
            // No merge needed - exact match or full containment
            debug!(
                "Range merge not needed (exact match or full containment): cache_key={}, requested={}-{}, cached_segments={}, fetched_segments={}",
                cache_key,
                requested_range.start,
                requested_range.end,
                cached_ranges.len(),
                fetched_ranges.len()
            );
        }

        // Calculate expected output size
        let expected_size = (requested_range.end - requested_range.start + 1) as usize;
        let mut output = Vec::with_capacity(expected_size);

        // Create merge plan: list of segments sorted by position
        let mut segments: Vec<RangeMergeSegment> = Vec::new();

        // Add cached range segments
        for cached_range in cached_ranges {
            // Calculate overlap with requested range
            if let Some((overlap_start, overlap_end)) = Self::calculate_overlap(
                cached_range.start,
                cached_range.end,
                requested_range.start,
                requested_range.end,
            ) {
                let read_offset = overlap_start - cached_range.start;
                let read_length = overlap_end - overlap_start + 1;

                segments.push(RangeMergeSegment {
                    output_start: overlap_start,
                    output_end: overlap_end,
                    source: RangeMergeSource::Cached {
                        range: cached_range.clone(),
                        read_offset,
                        read_length,
                    },
                });

                debug!(
                    "Added cached segment: output={}-{}, cached_range={}-{}, read_offset={}, read_length={}",
                    overlap_start, overlap_end, cached_range.start, cached_range.end, read_offset, read_length
                );
            }
        }

        // Add fetched range segments
        for (range_spec, data, _headers) in fetched_ranges {
            // Calculate overlap with requested range
            if let Some((overlap_start, overlap_end)) = Self::calculate_overlap(
                range_spec.start,
                range_spec.end,
                requested_range.start,
                requested_range.end,
            ) {
                segments.push(RangeMergeSegment {
                    output_start: overlap_start,
                    output_end: overlap_end,
                    source: RangeMergeSource::Fetched {
                        data: data.clone(),
                        range_spec: range_spec.clone(),
                    },
                });

                debug!(
                    "Added fetched segment: output={}-{}, fetched_range={}-{}",
                    overlap_start, overlap_end, range_spec.start, range_spec.end
                );
            }
        }

        // Sort segments by output position
        segments.sort_by_key(|s| s.output_start);

        debug!(
            "Merge plan created: {} segments sorted by position",
            segments.len()
        );

        // Validate that segments cover the entire requested range without gaps
        if !segments.is_empty() {
            // Check if first segment starts at requested start
            if segments[0].output_start != requested_range.start {
                return Err(ProxyError::CacheError(format!(
                    "Gap at start: first segment starts at {}, requested starts at {}",
                    segments[0].output_start, requested_range.start
                )));
            }

            // Check for gaps between segments
            for i in 0..segments.len() - 1 {
                let current_end = segments[i].output_end;
                let next_start = segments[i + 1].output_start;

                if next_start > current_end + 1 {
                    return Err(ProxyError::CacheError(format!(
                        "Gap detected between segments: segment {} ends at {}, segment {} starts at {}",
                        i, current_end, i + 1, next_start
                    )));
                }
            }

            // Check if last segment ends at requested end
            let last_segment = &segments[segments.len() - 1];
            if last_segment.output_end != requested_range.end {
                return Err(ProxyError::CacheError(format!(
                    "Gap at end: last segment ends at {}, requested ends at {}",
                    last_segment.output_end, requested_range.end
                )));
            }
        } else {
            return Err(ProxyError::CacheError(
                "No segments available for merge".to_string(),
            ));
        }

        // Merge segments into output buffer
        let mut bytes_from_cache = 0u64;
        let mut bytes_from_s3 = 0u64;
        let mut current_position = requested_range.start;
        let mut all_ram_hits = true; // Track if all cached segments came from RAM

        for (idx, segment) in segments.iter().enumerate() {
            debug!(
                "Processing segment {}/{}: output={}-{}",
                idx + 1,
                segments.len(),
                segment.output_start,
                segment.output_end
            );

            // Handle overlapping segments by skipping already-written bytes
            if segment.output_start < current_position {
                debug!(
                    "Skipping overlapping bytes: segment starts at {}, current position is {}",
                    segment.output_start, current_position
                );

                // Calculate how many bytes to skip
                let skip_bytes = current_position - segment.output_start;
                let segment_length = segment.output_end - segment.output_start + 1;

                if skip_bytes >= segment_length {
                    // Entire segment is already covered, skip it
                    debug!("Entire segment already covered, skipping");
                    continue;
                }

                // Adjust segment to start from current position
                let adjusted_start = current_position;
                let adjusted_length = segment.output_end - current_position + 1;

                match &segment.source {
                    RangeMergeSource::Cached {
                        range,
                        read_offset,
                        read_length,
                    } => {
                        let adjusted_offset = read_offset + skip_bytes;
                        let adjusted_read_length = read_length - skip_bytes;

                        // Log detailed slice operation for overlapping cached range (Requirement 2.1, 2.2)
                        debug!(
                            "Slice overlap: cached={}-{} segment={}-{} adjusted={}-{} offset={} len={}",
                            range.start, range.end,
                            segment.output_start, segment.output_end,
                            adjusted_start, segment.output_end,
                            adjusted_offset, adjusted_read_length
                        );

                        let (bytes, ram_hit) = self
                            .extract_bytes_from_cached_range(
                                cache_key,
                                range,
                                adjusted_start,
                                segment.output_end,
                            )
                            .await?;

                        // Track if any segment is not from RAM
                        if !ram_hit {
                            all_ram_hits = false;
                        }

                        // Validate extracted bytes match expected length (Requirement 3.5)
                        let expected_bytes = (segment.output_end - adjusted_start + 1) as usize;
                        if bytes.len() != expected_bytes {
                            error!(
                                "Overlapping slice validation failed: expected {} bytes, got {} bytes, cache_key={}, cached_range={}-{}, adjusted_segment={}-{}",
                                expected_bytes, bytes.len(), cache_key, range.start, range.end, adjusted_start, segment.output_end
                            );
                            return Err(ProxyError::CacheError(format!(
                                "Overlapping sliced data size mismatch: expected {} bytes, got {} bytes",
                                expected_bytes, bytes.len()
                            )));
                        }

                        output.extend_from_slice(&bytes);
                        bytes_from_cache += adjusted_read_length;
                        current_position = segment.output_end + 1;
                    }
                    RangeMergeSource::Fetched { data, range_spec } => {
                        let data_offset = (adjusted_start - range_spec.start) as usize;
                        let data_end = data_offset + adjusted_length as usize;

                        if data_end > data.len() {
                            return Err(ProxyError::CacheError(format!(
                                "Fetched data bounds error: offset={}, end={}, data_len={}",
                                data_offset,
                                data_end,
                                data.len()
                            )));
                        }

                        output.extend_from_slice(&data[data_offset..data_end]);
                        bytes_from_s3 += adjusted_length;
                        current_position = segment.output_end + 1;
                    }
                }
            } else {
                // No overlap, extract normally
                match &segment.source {
                    RangeMergeSource::Cached {
                        range,
                        read_offset,
                        read_length,
                    } => {
                        // Log detailed slice operation for each cached range (Requirement 2.1, 2.2)
                        debug!(
                            "Slice: cached={}-{} segment={}-{} offset={} len={}",
                            range.start,
                            range.end,
                            segment.output_start,
                            segment.output_end,
                            read_offset,
                            read_length
                        );

                        let (bytes, ram_hit) = self
                            .extract_bytes_from_cached_range(
                                cache_key,
                                range,
                                segment.output_start,
                                segment.output_end,
                            )
                            .await?;

                        // Track if any segment is not from RAM
                        if !ram_hit {
                            all_ram_hits = false;
                        }

                        // Validate extracted bytes match expected length (Requirement 3.5)
                        let expected_bytes =
                            (segment.output_end - segment.output_start + 1) as usize;
                        if bytes.len() != expected_bytes {
                            error!(
                                "Slice validation failed: expected {} bytes, got {} bytes, cache_key={}, cached_range={}-{}, requested_segment={}-{}",
                                expected_bytes, bytes.len(), cache_key, range.start, range.end, segment.output_start, segment.output_end
                            );
                            return Err(ProxyError::CacheError(format!(
                                "Sliced data size mismatch: expected {} bytes, got {} bytes",
                                expected_bytes,
                                bytes.len()
                            )));
                        }

                        output.extend_from_slice(&bytes);
                        bytes_from_cache += *read_length;
                        current_position = segment.output_end + 1;

                        debug!(
                            "Extracted {} bytes from cache (total from cache: {})",
                            bytes.len(),
                            bytes_from_cache
                        );
                    }
                    RangeMergeSource::Fetched { data, range_spec } => {
                        // Calculate offset within fetched data
                        let data_offset = (segment.output_start - range_spec.start) as usize;
                        let data_length = (segment.output_end - segment.output_start + 1) as usize;
                        let data_end = data_offset + data_length;

                        if data_end > data.len() {
                            return Err(ProxyError::CacheError(format!(
                                "Fetched data bounds error: offset={}, end={}, data_len={}",
                                data_offset,
                                data_end,
                                data.len()
                            )));
                        }

                        output.extend_from_slice(&data[data_offset..data_end]);
                        bytes_from_s3 += data_length as u64;
                        current_position = segment.output_end + 1;

                        debug!(
                            "Extracted {} bytes from S3 (total from S3: {})",
                            data_length, bytes_from_s3
                        );
                    }
                }
            }
        }

        // Validate final output size (Requirement 3.5: verify total bytes returned equals requested byte count)
        if output.len() != expected_size {
            error!(
                "Multiple range merge validation failed: cache_key={}, requested={}-{}, expected {} bytes, got {} bytes, segments={}",
                cache_key, requested_range.start, requested_range.end, expected_size, output.len(), segments.len()
            );
            return Err(ProxyError::CacheError(format!(
                "Merge size mismatch: expected {} bytes, got {} bytes",
                expected_size,
                output.len()
            )));
        }

        // Log successful validation (Requirement 3.5)
        debug!(
            "Multiple range merge validation passed: cache_key={}, requested={}-{}, total_bytes={}, segments={}, all sliced ranges concatenated correctly",
            cache_key, requested_range.start, requested_range.end, output.len(), segments.len()
        );

        let total_bytes = bytes_from_cache + bytes_from_s3;
        let cache_efficiency = if total_bytes > 0 {
            (bytes_from_cache as f64 / total_bytes as f64) * 100.0
        } else {
            0.0
        };

        let merge_duration = merge_start.elapsed();

        // Requirement 8.3, 8.4, 8.5: Only log merge completion when merge was actually needed
        // Include cache efficiency metrics (bytes from cache vs S3)
        let needs_merge =
            self.check_if_merge_needed(requested_range, cached_ranges, fetched_ranges);
        let size_mib = total_bytes as f64 / (1024.0 * 1024.0);

        if needs_merge {
            // Concise log for range merge - only when merge was actually needed
            info!(
                "RangeMerge: key={}, range={}-{}, size={:.1}MB, cached={}, fetched={}, duration={:.0}ms",
                cache_key,
                requested_range.start,
                requested_range.end,
                size_mib,
                cached_ranges.len(),
                fetched_ranges.len(),
                merge_duration.as_secs_f64() * 1000.0
            );
        } else {
            debug!(
                "Range served without merge: {} range={}-{} size={:.2}MiB segments={}",
                cache_key,
                requested_range.start,
                requested_range.end,
                size_mib,
                segments.len()
            );
        }

        Ok(RangeMergeResult {
            data: output,
            bytes_from_cache,
            bytes_from_s3,
            segments_merged: segments.len(),
            cache_efficiency,
            ram_hit: all_ram_hits && bytes_from_s3 == 0, // RAM hit only if all segments from RAM and no S3 fetches
        })
    }

    /// Fetch multiple missing ranges from S3 in parallel
    /// Returns fetched data with their corresponding range specs and response headers
    ///
    /// # Arguments
    /// * `cache_key` - Cache key for the object
    /// * `missing_ranges` - List of ranges to fetch from S3
    /// * `s3_client` - S3 client for making requests
    /// * `host` - S3 host (e.g., "s3.amazonaws.com")
    /// * `uri` - Original request URI
    /// * `headers` - Request headers (including Authorization)
    ///
    /// # Returns
    /// Vector of (RangeSpec, Vec<u8>, HashMap<String, String>) tuples for successfully fetched ranges
    /// The HashMap contains S3 response headers including ETag
    ///
    /// Requirements: 1.1, 1.4
    pub async fn fetch_missing_ranges(
        &self,
        cache_key: &str,
        missing_ranges: &[RangeSpec],
        s3_client: &Arc<crate::s3_client::S3Client>,
        host: &str,
        uri: &hyper::Uri,
        headers: &HashMap<String, String>,
    ) -> Result<Vec<(RangeSpec, Vec<u8>, HashMap<String, String>)>> {
        if missing_ranges.is_empty() {
            debug!("No missing ranges to fetch");
            return Ok(Vec::new());
        }

        debug!(
            "Fetching {} missing ranges from S3 in parallel: cache_key={}",
            missing_ranges.len(),
            cache_key
        );

        // Create fetch tasks for each missing range
        let fetch_futures: Vec<_> = missing_ranges
            .iter()
            .map(|range_spec| {
                let s3_client = Arc::clone(s3_client);
                let host = host.to_string();
                let uri = uri.clone();
                let headers = headers.clone();
                let range_spec = range_spec.clone();
                let cache_key = cache_key.to_string();

                async move {
                    let fetch_start = std::time::Instant::now();

                    debug!(
                        "Fetching range from S3: cache_key={}, range={}-{}",
                        cache_key, range_spec.start, range_spec.end
                    );

                    // Build S3 request with Range header
                    let mut s3_headers = headers.clone();
                    // Remove any existing Range header (case-insensitive) to avoid duplicates
                    s3_headers.retain(|k, _| k.to_lowercase() != "range");
                    // Insert new Range header with proper capitalization
                    s3_headers.insert(
                        "Range".to_string(),
                        format!("bytes={}-{}", range_spec.start, range_spec.end),
                    );

                    let mut context = crate::s3_client::build_s3_request_context(
                        hyper::Method::GET,
                        uri,
                        s3_headers,
                        None,
                        host,
                    );
                    // Don't use streaming for fetch_missing_ranges - we need to buffer the data
                    // to merge it with cached data before sending to client
                    context.allow_streaming = false;

                    match s3_client.forward_request(context).await {
                        Ok(s3_response) => {
                            if s3_response.status == hyper::StatusCode::PARTIAL_CONTENT {
                                if let Some(body) = s3_response.body {
                                    let fetch_duration = fetch_start.elapsed();
                                    let body_bytes = body.into_bytes().await?;
                                    let body_vec = body_bytes.to_vec(); // Convert Bytes to Vec<u8>
                                    let response_headers = s3_response.headers.clone();
                                    debug!(
                                        "Successfully fetched range from S3: cache_key={}, range={}-{}, size={} bytes, duration={:.2}ms",
                                        cache_key,
                                        range_spec.start,
                                        range_spec.end,
                                        body_vec.len(),
                                        fetch_duration.as_secs_f64() * 1000.0
                                    );
                                    Ok((range_spec, body_vec, response_headers))
                                } else {
                                    warn!(
                                        "S3 response has no body: cache_key={}, range={}-{}",
                                        cache_key, range_spec.start, range_spec.end
                                    );
                                    Err(crate::ProxyError::S3Error(
                                        "S3 response has no body".to_string()
                                    ))
                                }
                            } else {
                                warn!(
                                    "S3 returned unexpected status: cache_key={}, range={}-{}, status={}",
                                    cache_key, range_spec.start, range_spec.end, s3_response.status
                                );
                                Err(crate::ProxyError::S3Error(format!(
                                    "Unexpected status: {}",
                                    s3_response.status
                                )))
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to fetch range from S3: cache_key={}, range={}-{}, error={}",
                                cache_key, range_spec.start, range_spec.end, e
                            );
                            Err(e)
                        }
                    }
                }
            })
            .collect();

        // Execute all fetches in parallel
        let fetch_start = std::time::Instant::now();
        let results = futures::future::join_all(fetch_futures).await;
        let fetch_duration = fetch_start.elapsed();

        // Collect successful fetches
        let mut fetched_ranges = Vec::new();
        let mut failed_count = 0;

        for result in results {
            match result {
                Ok((range_spec, data, headers)) => {
                    fetched_ranges.push((range_spec, data, headers));
                }
                Err(e) => {
                    failed_count += 1;
                    warn!("Range fetch failed: {}", e);
                }
            }
        }

        if fetched_ranges.is_empty() && !missing_ranges.is_empty() {
            return Err(crate::ProxyError::S3Error(
                "Failed to fetch any missing ranges from S3".to_string(),
            ));
        }

        debug!(
            "Parallel fetch completed: cache_key={}, requested={}, succeeded={}, failed={}, duration={:.2}ms",
            cache_key,
            missing_ranges.len(),
            fetched_ranges.len(),
            failed_count,
            fetch_duration.as_secs_f64() * 1000.0
        );

        // Cache the fetched ranges immediately for future requests
        if !fetched_ranges.is_empty() {
            debug!(
                "Caching {} fetched ranges for future requests",
                fetched_ranges.len()
            );

            // We'll cache these in the calling function to avoid circular dependencies
            // The caller (forward_range_request_to_s3) will handle caching
        }

        Ok(fetched_ranges)
    }

    /// Merge ranges with comprehensive error handling and fallback logic
    ///
    /// This wrapper function provides robust error handling for range merging operations:
    /// - Validates merged data size matches requested range size
    /// - Falls back to complete S3 fetch on merge failure
    /// - Handles cached range file missing errors
    /// - Handles decompression failures
    /// - Handles byte alignment mismatches
    /// - Provides detailed error logging with context
    ///
    /// # Arguments
    /// * `cache_key` - Cache key for the object
    /// * `requested_range` - The range requested by the client
    /// * `cached_ranges` - Ranges available in cache
    /// * `fetched_ranges` - Ranges fetched from S3 with their data and headers
    /// * `s3_client` - S3 client for fallback fetch
    /// * `host` - S3 host for fallback fetch
    /// * `uri` - Request URI for fallback fetch
    /// * `headers` - Request headers for fallback fetch
    ///
    /// # Returns
    /// RangeMergeResult containing merged data and efficiency metrics
    ///
    /// # Fallback Behavior
    /// If merge fails for any reason, this function will:
    /// 1. Log detailed error information
    /// 2. Fetch the complete requested range from S3
    /// 3. Return the S3 data with appropriate metrics (0% cache efficiency)
    ///
    /// Requirements: 2.5, 8.2 (from design)
    pub async fn merge_ranges_with_fallback(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Vec<u8>, HashMap<String, String>)],
        s3_client: &Arc<crate::s3_client::S3Client>,
        host: &str,
        uri: &hyper::Uri,
        headers: &HashMap<String, String>,
    ) -> Result<RangeMergeResult> {
        // Attempt to merge ranges
        match self
            .merge_range_segments(cache_key, requested_range, cached_ranges, fetched_ranges)
            .await
        {
            Ok(merge_result) => {
                // Validate merged data size - Requirement 2.5
                let expected_size = requested_range.end - requested_range.start + 1;
                if merge_result.data.len() as u64 != expected_size {
                    warn!(
                        "Merge validation failed: cache_key={}, expected {} bytes, got {} bytes, falling back to complete S3 fetch",
                        cache_key, expected_size, merge_result.data.len()
                    );

                    // Fall back to complete S3 fetch
                    return self
                        .fallback_to_complete_s3_fetch(
                            cache_key,
                            requested_range,
                            s3_client,
                            host,
                            uri,
                            headers,
                            "size validation failed",
                        )
                        .await;
                }

                // Merge succeeded and validation passed
                debug!(
                    "Range merge succeeded: cache_key={}, size={} bytes, cache_efficiency={:.2}%",
                    cache_key,
                    merge_result.data.len(),
                    merge_result.cache_efficiency
                );
                Ok(merge_result)
            }
            Err(e) => {
                // Determine error type and log appropriately
                let error_context = match &e {
                    ProxyError::CacheError(msg)
                        if msg.contains("not found") || msg.contains("Metadata not found") =>
                    {
                        "cached range file missing"
                    }
                    ProxyError::CacheError(msg)
                        if msg.contains("decompression") || msg.contains("decompress") =>
                    {
                        "decompression failure"
                    }
                    ProxyError::CacheError(msg)
                        if msg.contains("Gap") || msg.contains("alignment") =>
                    {
                        "byte alignment mismatch"
                    }
                    ProxyError::CacheError(msg) if msg.contains("size mismatch") => {
                        "cached data size mismatch"
                    }
                    ProxyError::InvalidRange(msg) => {
                        warn!(
                            "Range merge failed due to invalid range: cache_key={}, error={}, falling back to complete S3 fetch",
                            cache_key, msg
                        );
                        "invalid range specification"
                    }
                    _ => "unknown merge error",
                };

                warn!(
                    "Range merge failed: cache_key={}, error_type={}, error={}, falling back to complete S3 fetch",
                    cache_key, error_context, e
                );

                // Fall back to complete S3 fetch
                self.fallback_to_complete_s3_fetch(
                    cache_key,
                    requested_range,
                    s3_client,
                    host,
                    uri,
                    headers,
                    error_context,
                )
                .await
            }
        }
    }

    /// Fallback to fetching complete range from S3
    ///
    /// This is called when range merging fails for any reason.
    /// It fetches the complete requested range from S3 and returns it
    /// with 0% cache efficiency metrics.
    ///
    /// # Arguments
    /// * `cache_key` - Cache key for the object
    /// * `requested_range` - The range to fetch
    /// * `s3_client` - S3 client for making the request
    /// * `host` - S3 host
    /// * `uri` - Request URI
    /// * `headers` - Request headers
    /// * `reason` - Reason for fallback (for logging)
    ///
    /// # Returns
    /// RangeMergeResult with data from S3 and 0% cache efficiency
    async fn fallback_to_complete_s3_fetch(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        s3_client: &Arc<crate::s3_client::S3Client>,
        host: &str,
        uri: &hyper::Uri,
        headers: &HashMap<String, String>,
        reason: &str,
    ) -> Result<RangeMergeResult> {
        info!(
            "Falling back to complete S3 fetch: range={}-{} reason={} cache_key={}",
            requested_range.start, requested_range.end, reason, cache_key
        );

        let fallback_start = std::time::Instant::now();

        // Build S3 request with Range header
        let mut s3_headers = headers.clone();
        s3_headers.insert(
            "range".to_string(),
            format!("bytes={}-{}", requested_range.start, requested_range.end),
        );

        let mut context = crate::s3_client::build_s3_request_context(
            hyper::Method::GET,
            uri.clone(),
            s3_headers,
            None,
            host.to_string(),
        );
        context.allow_streaming = true; // Enable streaming for fallback fetches

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                if s3_response.status == hyper::StatusCode::PARTIAL_CONTENT {
                    if let Some(body) = s3_response.body {
                        let body_bytes = body.into_bytes().await?;
                        let body_vec = body_bytes.to_vec();
                        let expected_size = requested_range.end - requested_range.start + 1;

                        // Validate response size
                        if body_vec.len() as u64 != expected_size {
                            return Err(ProxyError::S3Error(format!(
                                "S3 fallback fetch size mismatch: expected {} bytes, got {} bytes",
                                expected_size,
                                body_vec.len()
                            )));
                        }

                        let fallback_duration = fallback_start.elapsed();

                        info!(
                            "Fallback S3 fetch succeeded: range={}-{} size={} bytes duration={:.2}ms cache_key={}",
                            requested_range.start,
                            requested_range.end,
                            body_vec.len(),
                            fallback_duration.as_secs_f64() * 1000.0,
                            cache_key
                        );

                        // Return result with 0% cache efficiency (all from S3)
                        Ok(RangeMergeResult {
                            data: body_vec.clone(),
                            bytes_from_cache: 0,
                            bytes_from_s3: body_vec.len() as u64,
                            segments_merged: 1,
                            cache_efficiency: 0.0,
                            ram_hit: false, // Data came from S3, not cache
                        })
                    } else {
                        Err(ProxyError::S3Error(
                            "S3 fallback fetch returned no body".to_string(),
                        ))
                    }
                } else {
                    Err(ProxyError::S3Error(format!(
                        "S3 fallback fetch failed with status: {}",
                        s3_response.status
                    )))
                }
            }
            Err(e) => {
                error!(
                    "Fallback S3 fetch failed: cache_key={}, range={}-{}, error={}",
                    cache_key, requested_range.start, requested_range.end, e
                );
                Err(e)
            }
        }
    }
}
