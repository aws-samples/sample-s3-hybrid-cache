//! Range Handler Module
//!
//! Handles HTTP Range requests for partial content retrieval with caching support.
//! Implements range header parsing, validation, overlap detection, and merging.
//!
//! Also home to the page-geometry helpers used by page-aligned range caching
//! (widening) — see [`page_bounds`], [`overlapping_pages`], and
//! [`suffix_page_target`]. Spec: `page-aligned-range-cache`.

use crate::cache::{CacheManager, Range};
use crate::cache_types::StoredFreshness;
use crate::disk_cache::DiskCacheManager;
use crate::{ProxyError, Result};
use bytes::Bytes;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
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

/// Sentinel returned by [`suffix_page_target`] when the object size is not yet
/// known and the caller must fall back to issuing `bytes=-P` upstream (a
/// size-free, end-anchored widening — Requirement 3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuffixPageTarget {
    /// Size is known; these are the overlapping grid page(s) (1 or 2) that
    /// cover the absolutized suffix range `[size - n, size - 1]`.
    Pages(SmallVec<[(u64, u64); 2]>),
    /// Size is unknown. The caller MUST issue `bytes=-page_size` upstream
    /// instead of computing grid pages (Requirement 3.3).
    SizeUnknown,
}

/// Compute the page grid bounds `(page_start, page_end)` containing `start`.
///
/// The grid is anchored at offset 0: `page_index = floor(start / page_size)`,
/// `page_start = page_index * page_size`, `page_end = page_start + page_size - 1`.
/// The last page is clamped so `page_end` never exceeds `size - 1` — the grid
/// itself is uniform, but the returned bounds are truncated to the object's
/// actual extent (S3 clamps the fetch to the object; we cache what returns).
///
/// # Panics
/// Panics if `page_size` is 0 or `size` is 0 (both are validated at config
/// load time — see `bucket_settings::CacheRules::validate` — so a 0 reaching
/// here indicates a caller bug, not a runtime/user-input condition).
///
/// # Example
/// Not a doctest: this function is `pub(crate)`, and a doctest compiles as a
/// separate crate, so it could only reach this name by widening the visibility
/// back to `pub`. The executable equivalent is `page_bounds_second_page` in this
/// file's test module, alongside five siblings and the property test
/// `prop_page_bounds_contains_start_and_never_exceeds_size`.
/// ```text
/// // A 100 MiB object, 16 MiB pages: offset 20 MiB falls in the second page.
/// let p = 16 * 1024 * 1024;
/// let size = 100 * 1024 * 1024;
/// assert_eq!(page_bounds(20 * 1024 * 1024, p, size), (p, 2 * p - 1));
/// ```
///
/// Spec: page-aligned-range-cache Requirement 3.1, Page model (design.md).
pub(crate) fn page_bounds(start: u64, page_size: u64, size: u64) -> (u64, u64) {
    assert!(page_size > 0, "page_size must be > 0");
    assert!(size > 0, "size must be > 0");

    let page_index = start / page_size;
    let page_start = page_index * page_size;
    let page_end = (page_start + page_size - 1).min(size - 1);
    (page_start, page_end)
}

/// Compute the grid page(s) that an absolute-offset range `[start, end]`
/// overlaps: normally one page, or two when the range straddles a page
/// boundary.
///
/// Returns a `SmallVec` sized so the common (single-page) case never
/// allocates on the heap.
///
/// # Panics
/// Panics if `end < start` (empty/invalid range), or if `page_size` / `size`
/// is 0 — see [`page_bounds`].
///
/// # Example
/// Not a doctest: this function is `pub(crate)`, and a doctest compiles as a
/// separate crate, so it could only reach this name by widening the visibility
/// back to `pub`. The executable equivalent is
/// `overlapping_pages_straddle_returns_two_pages_in_order` in this file's test
/// module, alongside four siblings and two property tests.
/// ```text
/// let p = 16 * 1024 * 1024;
/// let size = 100 * 1024 * 1024;
/// // A read that straddles the boundary between page 0 and page 1.
/// let pages = overlapping_pages(p - 10, p + 10, p, size);
/// assert_eq!(pages.len(), 2);
/// assert_eq!(pages[0], (0, p - 1));
/// assert_eq!(pages[1], (p, 2 * p - 1));
/// ```
///
/// Spec: page-aligned-range-cache Requirement 3.1, 3.5.
pub(crate) fn overlapping_pages(
    start: u64,
    end: u64,
    page_size: u64,
    size: u64,
) -> SmallVec<[(u64, u64); 2]> {
    assert!(end >= start, "range end must be >= start");

    let first = page_bounds(start, page_size, size);
    let last = page_bounds(end, page_size, size);

    let mut pages = SmallVec::new();
    pages.push(first);
    if last != first {
        pages.push(last);
    }
    pages
}

/// Compute the widening target for an eligible Suffix_Range (`bytes=-n`).
///
/// When the object size is already known, the suffix is absolutized to
/// `[size - n, size - 1]` and mapped to its overlapping grid page(s) via
/// [`overlapping_pages`] — never a single fixed "last page", because when the
/// object size is just over a page multiple, the last grid page's remainder
/// can be *smaller* than the requested suffix, which would under-fetch
/// (widening must always be a superset of the requested bytes, never a
/// subset — Requirement 3.2).
///
/// When the size is unknown, returns [`SuffixPageTarget::SizeUnknown`]: the
/// caller must issue `bytes=-page_size` upstream instead (Requirement 3.3).
///
/// `n` is clamped to `size` if it exceeds it, matching how a suffix range
/// larger than the object resolves to the whole object.
///
/// # Panics
/// Panics if `n` is 0, or if `page_size` is 0 when `size` is `Some`.
///
/// # Example
/// Not a doctest: this function is `pub(crate)`, and a doctest compiles as a
/// separate crate, so it could only reach this name by widening the visibility
/// back to `pub`. The executable equivalents are
/// `suffix_page_target_size_unknown_returns_sentinel` and
/// `suffix_page_target_size_known_cold_maps_to_last_page` in this file's test
/// module, alongside three siblings and three property tests.
/// ```text
/// let p = 16 * 1024 * 1024;
/// // Size unknown: caller must fall back to bytes=-P.
/// assert_eq!(suffix_page_target(1024, p, None), SuffixPageTarget::SizeUnknown);
///
/// // Size known: resolves to the overlapping grid page(s).
/// match suffix_page_target(1024, p, Some(100 * p)) {
///     SuffixPageTarget::Pages(pages) => assert_eq!(pages.len(), 1),
///     SuffixPageTarget::SizeUnknown => panic!("size was known"),
/// }
/// ```
///
/// Spec: page-aligned-range-cache Requirement 3.2, 3.3.
pub(crate) fn suffix_page_target(n: u64, page_size: u64, size: Option<u64>) -> SuffixPageTarget {
    assert!(n > 0, "suffix length must be > 0");

    let Some(size) = size else {
        return SuffixPageTarget::SizeUnknown;
    };
    assert!(page_size > 0, "page_size must be > 0");
    assert!(size > 0, "size must be > 0");

    let n = n.min(size);
    let start = size - n;
    let end = size - 1;
    SuffixPageTarget::Pages(overlapping_pages(start, end, page_size, size))
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
    /// **Coverage completeness only** — `missing_ranges.is_empty()`. It does not
    /// mean the bytes may be served; see [`RangeOverlap::is_serveable_unvalidated`].
    ///
    /// The name predates the freshness marker below and is left alone
    /// deliberately: it is read at a dozen sites and its meaning has not changed.
    pub can_serve_from_cache: bool,
    /// Stored-expiry state of the entry this coverage came from
    /// (the expired entry revalidation feature R1.2).
    ///
    /// Only ever [`crate::cache_types::StoredFreshness::Expired`] when the lookup was made with
    /// [`crate::cache_types::RangeLookupPurpose::RevalidationCandidate`]; a `FreshServe` lookup
    /// returns empty coverage past stored expiry, so its overlaps are always
    /// `Fresh`. Existing `FreshServe` callers therefore see no behaviour change.
    pub stored_freshness: StoredFreshness,
}

impl RangeOverlap {
    /// An all-missing overlap: nothing cached, everything requested.
    pub fn all_missing(requested: &RangeSpec) -> Self {
        Self {
            cached_ranges: Vec::new(),
            missing_ranges: vec![requested.clone()],
            can_serve_from_cache: false,
            // No entry was consulted, so no stored expiry could have passed.
            stored_freshness: StoredFreshness::Fresh,
        }
    }

    /// May these bytes be served on the cache's own authority?
    ///
    /// True only when coverage is complete **and** the entry was within its
    /// stored expiry. A Stored_Expired candidate returns `false` even with
    /// complete coverage, which is Requirement 1.2: discovering an expired range
    /// must not by itself authorise serving it.
    ///
    /// Callers that hold a *separate* authority — a successful conditional `304`,
    /// a matching client `If-Match` (Mode B), or a live-TTL `Fresh` verdict —
    /// must use [`RangeOverlap::has_complete_coverage`] and name that authority
    /// at the call site. That asymmetry is the point: serving expired bytes
    /// requires writing down why it is allowed.
    pub fn is_serveable_unvalidated(&self) -> bool {
        self.can_serve_from_cache && self.stored_freshness == StoredFreshness::Fresh
    }

    /// Is every requested byte covered, regardless of freshness?
    ///
    /// Use only where the caller holds an explicit freshness authority. A `304`
    /// proves the cached representation is current but says nothing about whether
    /// the cache still *covers* the request, so that check is still needed.
    pub fn has_complete_coverage(&self) -> bool {
        self.can_serve_from_cache
    }
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
        /// The actual data bytes.
        ///
        /// `Bytes` so carrying a fetched segment into a merge segment is a refcount
        /// bump rather than a copy of the segment. Requirement: IMA 5.2
        data: Bytes,
        /// Range specification for this data
        range_spec: RangeSpec,
    },
}

/// Message prefix of the `ProxyError::S3Error` that
/// [`RangeHandler::merge_ranges_with_fallback`] returns when its complete refetch
/// shows the object changed (a `412` on the proxy-added `If-Match`, or a different
/// ETag). Recognise it with [`is_merge_refetch_version_change`].
pub const MERGE_REFETCH_VERSION_CHANGED: &str =
    "merge fallback refetch returned another version of the object";

/// Whether `error` is the merge fallback's version-change refusal. A caller must
/// not serve anything under the cached headers on it; the cached version is gone
/// from S3, so the key is invalidated and the client's request forwarded.
pub fn is_merge_refetch_version_change(error: &ProxyError) -> bool {
    matches!(error, ProxyError::S3Error(msg) if msg.starts_with(MERGE_REFETCH_VERSION_CHANGED))
}

/// Prefix of the error the disk loaders return when the `.meta` they re-read lists the
/// extent under another version than the lookup labelled it with, or does not list it while
/// pending journal `Add`s for it record only other versions. An extent nothing lists is the
/// missing-file error instead, so the caller's recovery refetches it. Recognise it with
/// [`is_listed_under_another_version`].
pub(crate) const LISTED_UNDER_ANOTHER_VERSION: &str =
    "cached extent is listed under another version than the lookup's";

/// Whether two documents of an object, neither recording an ETag, name different versions by
/// the `Last-Modified` each records. The ETag identifies a version; an origin that sends
/// none is judged by the `Last-Modified` it does send, compared as an instant so that two
/// spellings of one date are one version. A side with no value is no evidence either way, and
/// two versions with the same `Last-Modified` second cannot be told apart. False whenever
/// either side records an ETag: the ETag comparison decides then.
pub(crate) fn etagless_versions_differ(
    etag_a: &str,
    last_modified_a: &str,
    etag_b: &str,
    last_modified_b: &str,
) -> bool {
    if !etag_a.is_empty() || !etag_b.is_empty() {
        return false;
    }
    let (a, b) = (last_modified_a.trim(), last_modified_b.trim());
    if a.is_empty() || b.is_empty() {
        return false;
    }
    match (httpdate::parse_http_date(a), httpdate::parse_http_date(b)) {
        (Ok(a), Ok(b)) => a != b,
        _ => a != b,
    }
}

/// Whether `error` is the loaders' refusal of a listing of another version. The cached copy is
/// not shown stale (a newer version may be what is listed), so a caller forwards the client's
/// request uncached rather than refetching under the old version's `If-Match` and invalidating.
pub(crate) fn is_listed_under_another_version(error: &ProxyError) -> bool {
    matches!(error, ProxyError::CacheError(msg) if msg.starts_with(LISTED_UNDER_ANOTHER_VERSION))
}

/// Result of range merging operation with metrics
#[derive(Debug)]
pub struct RangeMergeResult {
    /// Merged data ready to send to client.
    ///
    /// `Bytes` rather than `Vec<u8>` so the fetch and fallback paths can hand over
    /// the body they already hold instead of memcpy'ing it, and so slicing a
    /// sub-range out of it is a refcount bump rather than a copy.
    /// Requirement: IMA 5.2
    pub data: Bytes,
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
    pub(crate) fn get_disk_cache_manager(&self) -> &Arc<tokio::sync::RwLock<DiskCacheManager>> {
        &self.disk_cache_manager
    }

    /// Get reference to cache manager for eviction operations
    pub fn get_cache_manager(&self) -> &std::sync::Arc<CacheManager> {
        &self.cache_manager
    }

    /// Parse HTTP Range header and validate byte ranges - Requirements 3.1, 3.5
    pub(crate) fn parse_range_header(
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
                    debug!(
                        "Range parse error '{}': {}, content_length={:?}",
                        range_spec, e, content_length
                    );
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
    /// Convert range specification to HTTP Content-Range header value
    pub(crate) fn build_content_range_header(
        &self,
        range: &RangeSpec,
        content_length: u64,
    ) -> String {
        format!("bytes {}-{}/{}", range.start, range.end, content_length)
    }
    /// Merge overlapping or adjacent ranges
    pub(crate) fn merge_ranges(&self, ranges: &[RangeSpec]) -> Vec<RangeSpec> {
        if ranges.is_empty() {
            return Vec::new();
        }

        let mut sorted_ranges = ranges.to_vec();
        sorted_ranges.sort_by_key(|r| r.start);

        let mut merged = Vec::new();
        let mut current = sorted_ranges[0].clone();

        for range in sorted_ranges.iter().skip(1) {
            // Check if ranges overlap or are adjacent (saturating to avoid overflow on u64::MAX)
            if current.end.saturating_add(1) >= range.start {
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

/// Whether a client's raw `Range` header names exactly `requested`.
///
/// `Some(true)`/`Some(false)` for the absolute `bytes=a-b` form. `None` for the suffix
/// (`bytes=-n`) and open-ended (`bytes=a-`) forms, which cannot be compared without the
/// object length; a caller then forwards the header verbatim and relies on the
/// response-size validation that follows every fallback fetch.
pub(crate) fn signed_client_range_covers(raw: &str, requested: &RangeSpec) -> Option<bool> {
    let spec = raw.trim().strip_prefix("bytes=")?.trim();
    let (start, end) = spec.split_once('-')?;
    if start.trim().is_empty() || end.trim().is_empty() {
        return None;
    }
    let start: u64 = start.trim().parse().ok()?;
    let end: u64 = end.trim().parse().ok()?;
    Some(start == requested.start && end == requested.end)
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
    /// # `purpose` (the expired entry revalidation feature, R1.1-R1.5)
    ///
    /// Passed straight through to [`crate::disk_cache::DiskCacheManager::find_cached_ranges`], which
    /// owns the stored-expiry decision. The resulting
    /// [`StoredFreshness`] is carried on the returned [`RangeOverlap`], so an
    /// expired candidate cannot reach a caller as ordinary serveable coverage.
    ///
    /// Use [`RangeOverlap::is_serveable_unvalidated`] rather than reading
    /// `can_serve_from_cache` directly unless you hold a separate freshness
    /// authority and can name it.
    pub async fn find_cached_ranges(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        current_etag: Option<&str>,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
        purpose: crate::cache_types::RangeLookupPurpose,
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

        // The preloaded document may be this instance's RAM metadata snapshot, which can lag
        // the shared `.meta` by up to `metadata_cache.refresh_interval`, while `current_etag`
        // is read from the shared `.meta`. When they disagree, re-read the shared `.meta`
        // before treating it as a version change: if it agrees with `current_etag`, the RAM
        // snapshot is the stale side (typically a peer's overwrite), so drop it and its RAM
        // ranges and continue with the shared document. Invalidating on the snapshot's word
        // would delete the NEW version's range files from shared storage.
        let mut lookup_preloaded = preloaded_metadata;
        let mut metadata_opt = metadata_opt;
        if let (Some(current), Some(preloaded)) = (current_etag, preloaded_metadata) {
            if !crate::revalidation::etags_equal_ignoring_quotes(
                current,
                &preloaded.object_metadata.etag,
            ) {
                let shared = disk_cache.get_metadata(cache_key).await?;
                if let Some(shared) = shared.filter(|m| {
                    crate::revalidation::etags_equal_ignoring_quotes(
                        current,
                        &m.object_metadata.etag,
                    )
                }) {
                    info!(
                        "Preloaded metadata is stale against the shared .meta; dropping the RAM snapshot and using the shared document: cache_key={}, snapshot_etag={}, shared_etag={}",
                        cache_key, preloaded.object_metadata.etag, shared.object_metadata.etag
                    );
                    self.cache_manager
                        .invalidate_metadata_cache(cache_key)
                        .await;
                    if let Err(e) = self.cache_manager.invalidate_ram_ranges(cache_key).await {
                        warn!(
                            "Failed to drop RAM ranges of a stale metadata snapshot: cache_key={}, error={}",
                            cache_key, e
                        );
                    }
                    metadata_opt = Some(shared);
                    lookup_preloaded = None;
                }
            }
        }

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
                // Quote-tolerant: older multipart entries were stored unquoted.
                if !crate::revalidation::etags_equal_ignoring_quotes(
                    current_etag,
                    &metadata.object_metadata.etag,
                ) {
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

                    // The disk invalidation above cannot reach the RAM tier —
                    // `invalidate_all_ranges` is on `DiskCacheManager`, which has no
                    // handle on it. Without this, the superseded version stays
                    // readable from RAM with no expiry, and the widened range path
                    // consults RAM before reaching this ETag check at all.
                    if let Err(e) = self.cache_manager.invalidate_ram_ranges(cache_key).await {
                        warn!(
                            "Failed to invalidate stale RAM ranges after ETag mismatch: cache_key={}, error={}",
                            cache_key, e
                        );
                    }
                    // And the RAM metadata snapshot of the document just invalidated.
                    self.cache_manager
                        .invalidate_metadata_cache(cache_key)
                        .await;

                    // Return empty cached ranges, forcing a fetch from S3
                    return Ok(RangeOverlap::all_missing(requested_range));
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
                    i, cache_key, range.start, range.end,
                );
            }

            // Find overlapping ranges using the new storage
            debug!(
                "[CACHE_LOOKUP] Calling disk_cache.find_cached_ranges: cache_key={}, requested_range={}-{}",
                cache_key, requested_range.start, requested_range.end
            );
            // Coverage from the document the lookup is labelled from (`metadata`): the shared
            // `.meta` when the snapshot was dropped above, else the caller's preload. Passing
            // the caller's preload there took one document's extents under the other's ETag
            // No preload stays `None`, so the callee's own `.meta` read is unchanged.
            let lookup = disk_cache
                .find_cached_ranges(
                    cache_key,
                    requested_range.start,
                    requested_range.end,
                    preloaded_metadata.map(|_| &metadata),
                    purpose,
                )
                .await?;
            debug!(
                "[CACHE_LOOKUP] disk_cache.find_cached_ranges returned: cache_key={}, overlapping_ranges_count={}, purpose={:?}, stored_freshness={:?}",
                cache_key,
                lookup.ranges.len(),
                purpose,
                lookup.stored_freshness
            );

            let overlap = self.overlap_from_lookup(cache_key, requested_range, &metadata, lookup);

            // `ram-tier-effectiveness:R1.2`. A Metadata_Cache entry is NOT
            // authoritative for the ABSENCE of cached ranges, so a verdict of
            // "not covered" taken from a possibly-resident document has to be
            // confirmed against Shared_Storage before it becomes a miss. The
            // resident copy is a snapshot bounded by
            // `metadata_cache.refresh_interval`, and inside that window another
            // instance's consolidation can have extended the shared `.meta` with
            // ranges whose `.bin` files are already on this volume. Without the
            // confirmation the GET refetches from S3 bytes that are cached
            // locally — a cost defect, never a correctness one.
            //
            // THE COVERED BRANCH IS UNTOUCHED, AND THAT IS THE POINT.
            // The v1.9.4 fix rejected re-reading the shared `.meta` on every
            // GET for exactly this class of entry, and re-introducing that would
            // be a regression dressed as a fix. The guard below is structurally
            // inside `!overlap.can_serve_from_cache`: nothing was added before
            // the lookup or before the coverage computation, so a covered
            // request executes the same statements and performs the same reads as
            // it did before this change. The branch it does sit on is the one
            // that proceeds to a full S3 fetch today, so one `.meta` read there
            // is strictly cheaper than the status quo.
            if !overlap.can_serve_from_cache && lookup_preloaded.is_some() {
                if let Some(confirmed) = self
                    .confirm_absence_against_shared_storage(
                        &disk_cache,
                        cache_key,
                        requested_range,
                        current_etag,
                        &metadata,
                        purpose,
                    )
                    .await
                {
                    return Ok(confirmed);
                }
            }

            return Ok(overlap);
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
        Ok(RangeOverlap::all_missing(requested_range))
    }

    /// Turn a completed range lookup into the coverage verdict callers branch on.
    ///
    /// Split out of [`RangeHandler::find_cached_ranges`] so the same computation can be
    /// run twice against two metadata sources — the possibly-resident document the
    /// caller preloaded, and the shared `.meta` — without the second run being a
    /// paraphrase of the first. Two copies of this arithmetic would be two chances for
    /// the confirmation in `R1.2` to disagree with the verdict it is confirming.
    fn overlap_from_lookup(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        metadata: &crate::cache_types::NewCacheMetadata,
        lookup: crate::cache_types::RangeLookupResult,
    ) -> RangeOverlap {
        let stored_freshness = lookup.stored_freshness;
        let overlapping_range_specs = lookup.ranges;

        if overlapping_range_specs.is_empty() {
            debug!(
                "[CACHE_LOOKUP] No overlapping ranges found in new storage: cache_key={}",
                cache_key
            );
            return RangeOverlap::all_missing(requested_range);
        }

        debug!(
            "[CACHE_LOOKUP] Found overlapping ranges in new storage: cache_key={}, count={}",
            cache_key,
            overlapping_range_specs.len()
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
        let missing_ranges = self.calculate_missing_ranges(requested_range, &covered_ranges);
        let can_serve_from_cache = missing_ranges.is_empty();

        debug!(
                    "[CACHE_LOOKUP] Cache lookup result: cache_key={}, overlapping_ranges={}, missing_ranges={}, requested_range={}-{}, can_serve_from_cache={}, stored_freshness={:?}",
                    cache_key, overlapping_ranges.len(), missing_ranges.len(), requested_range.start, requested_range.end, can_serve_from_cache, stored_freshness
                );

        RangeOverlap {
            cached_ranges: overlapping_ranges,
            missing_ranges,
            can_serve_from_cache,
            stored_freshness,
        }
    }

    /// `ram-tier-effectiveness:R1.2`: confirm a not-covered verdict against Shared_Storage
    /// before it becomes a miss.
    ///
    /// Returns `Some(overlap)` only when the shared `.meta` covers the request in full —
    /// i.e. only when the verdict the caller was about to take is demonstrably wrong.
    /// `None` means "nothing learned, keep the verdict you have", which is every other
    /// outcome: the shared document is the one already evaluated, the object is gone, the
    /// read failed, or the disk document does not cover either.
    ///
    /// # Why a full-coverage test and not "whatever disk covers"
    ///
    /// Taking disk's *partial* coverage instead would also shrink the S3 fetch, and it is
    /// deliberately not done here. `missing_ranges` is consumed by the merge, gap-repair
    /// and page-widening paths, so changing its shape on a branch that stays a miss either
    /// way widens this change from "a miss becomes a hit" to "every partial miss is
    /// reshaped". The requirement is about the miss *classification*; the classification is
    /// what this returns.
    ///
    /// # Exactly one extra Shared_Storage read, and only on this branch
    ///
    /// `get_metadata` reads the `.meta`; the second `find_cached_ranges` is then handed
    /// that document as `preloaded`, so it does no I/O of its own beyond the journal
    /// fallback the first lookup would have taken anyway. The `ranges`-equality early
    /// return below skips even that when the shared document is the one already evaluated.
    async fn confirm_absence_against_shared_storage(
        &self,
        disk_cache: &DiskCacheManager,
        cache_key: &str,
        requested_range: &RangeSpec,
        current_etag: Option<&str>,
        evaluated: &crate::cache_types::NewCacheMetadata,
        purpose: crate::cache_types::RangeLookupPurpose,
    ) -> Option<RangeOverlap> {
        // A preloaded document cannot have come from a tier that is switched off, so
        // there is nothing for this instance to be behind. Config-static, so this cannot
        // silently disable the guard on a running proxy the way an eviction-sensitive
        // residency test could.
        if !self.cache_manager.get_metadata_cache().is_enabled() {
            return None;
        }

        let shared = match disk_cache.get_metadata(cache_key).await {
            Ok(Some(shared)) => shared,
            Ok(None) => {
                // The shared `.meta` is gone. Disk is authoritative for presence, so the
                // caller's miss stands.
                debug!(
                    "[R1.2] Shared .meta absent, not-covered verdict confirmed: cache_key={}",
                    cache_key
                );
                return None;
            }
            Err(e) => {
                // A transient read failure must not manufacture coverage. Keep the
                // caller's verdict, which is the pre-change behaviour.
                warn!(
                    "[R1.2] Could not read shared .meta to confirm a not-covered verdict, \
                     keeping it: cache_key={}, error={}",
                    cache_key, e
                );
                return None;
            }
        };

        if shared.ranges == evaluated.ranges {
            debug!(
                "[R1.2] Shared .meta carries the same {} range(s) already evaluated, \
                 not-covered verdict confirmed: cache_key={}",
                shared.ranges.len(),
                cache_key
            );
            return None;
        }

        // The caller's own ETag check ran against the document it preloaded. Re-apply it
        // to the shared document rather than inheriting that result: this is a different
        // document, and coverage from a superseded version must not be served.
        if let Some(current_etag) = current_etag {
            if !crate::revalidation::etags_equal_ignoring_quotes(
                current_etag,
                &shared.object_metadata.etag,
            ) {
                debug!(
                    "[R1.2] Shared .meta carries a different ETag than the request \
                     validator, not-covered verdict confirmed: cache_key={}, \
                     shared_etag={}, current_etag={}",
                    cache_key, shared.object_metadata.etag, current_etag
                );
                return None;
            }
        }

        let lookup = match disk_cache
            .find_cached_ranges(
                cache_key,
                requested_range.start,
                requested_range.end,
                Some(&shared),
                purpose,
            )
            .await
        {
            Ok(lookup) => lookup,
            Err(e) => {
                warn!(
                    "[R1.2] Range lookup against the shared .meta failed, keeping the \
                     not-covered verdict: cache_key={}, error={}",
                    cache_key, e
                );
                return None;
            }
        };

        let confirmed = self.overlap_from_lookup(cache_key, requested_range, &shared, lookup);
        if !confirmed.can_serve_from_cache {
            debug!(
                "[R1.2] Shared .meta does not cover the request either, not-covered \
                 verdict confirmed: cache_key={}, shared_ranges={}, requested_range={}-{}",
                cache_key,
                shared.ranges.len(),
                requested_range.start,
                requested_range.end
            );
            return None;
        }

        // The resident copy is behind the shared `.meta` for this key, and it is the only
        // reason the request was about to be classified a miss. Dropping it is the same
        // convergence argument the two `has_cached_ranges` guards make: without it every
        // request inside the remaining `refresh_interval` re-reads the same superseded
        // document and pays this confirmation again.
        self.cache_manager
            .invalidate_metadata_cache(cache_key)
            .await;

        info!(
            "[R1.2] Shared .meta covers a request a preloaded metadata document reported as \
             missing: cache_key={}, requested_range={}-{}, evaluated_ranges={}, \
             shared_ranges={}. Serving from cache instead of refetching from S3; the stale \
             resident metadata entry has been dropped",
            cache_key,
            requested_range.start,
            requested_range.end,
            evaluated.ranges.len(),
            shared.ranges.len()
        );

        Some(confirmed)
    }

    /// Calculate missing ranges that are not covered by cached ranges
    pub(crate) fn calculate_missing_ranges(
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

    /// Resolve the on-disk spec for an extent a lookup chose, by re-reading its `.meta` and
    /// falling back to the pending journals.
    ///
    /// A consolidation can mark the extent `unservable` between the lookup and this re-read;
    /// its file may then hold another version's bytes. That case is an `Err`, never a
    /// journal lookup (a journal line for the same offsets names the same file), so both
    /// loaders below refuse it before touching the file. Their callers route the `Err` to an
    /// S3 refetch: `get_cached_range_data`'s `If-Match`-pinned arm for a single extent, and
    /// `merge_ranges_with_fallback`'s complete fetch for a merge.
    ///
    /// The bytes are answered under the head of the document the lookup used, whose ETag is
    /// `range.etag`. So the re-read is bound to that version: a `.meta` naming another
    /// version (an overwrite landed since the lookup's document was read, possibly a RAM copy
    /// up to `metadata_cache.refresh_interval` old) and a pending `Add` recording another are
    /// both refused, with [`LISTED_UNDER_ANOTHER_VERSION`]. An empty label matches only a
    /// `.meta` or `Add` that records no ETag, and a `.meta` only when its `Last-Modified` does
    /// not differ from the label's. The cost is a hit lost to an overwrite.
    async fn resolve_extent_spec_for_read(
        disk_cache: &DiskCacheManager,
        cache_key: &str,
        range: &Range,
    ) -> Result<crate::cache_types::RangeSpec> {
        let metadata = disk_cache.get_metadata(cache_key).await?;
        let listed_under_another = || {
            #[cfg(test)]
            crate::disk_cache::serve_refusal_log::note(cache_key, "listed_under_another_version");
            debug!(
                "Cached extent {}-{} is not listed under the lookup's version {}, refusing the \
                 disk read: cache_key={}",
                range.start, range.end, range.etag, cache_key
            );
            ProxyError::CacheError(format!(
                "{} ({}): extent {}-{} of key {}",
                LISTED_UNDER_ANOTHER_VERSION, range.etag, range.start, range.end, cache_key
            ))
        };
        match crate::http_proxy::reread_extent_spec(metadata.as_ref(), range.start, range.end) {
            crate::http_proxy::ExtentSpecReread::Listed(spec) => {
                let listed = metadata.as_ref().map(|m| &m.object_metadata);
                let listed_etag = listed.map(|o| o.etag.as_str()).unwrap_or_default();
                let listed_last_modified =
                    listed.map(|o| o.last_modified.as_str()).unwrap_or_default();
                if crate::revalidation::etags_equal_ignoring_quotes(listed_etag, &range.etag)
                    && !etagless_versions_differ(
                        listed_etag,
                        listed_last_modified,
                        &range.etag,
                        &range.last_modified,
                    )
                {
                    Ok(spec)
                } else {
                    Err(listed_under_another())
                }
            }
            crate::http_proxy::ExtentSpecReread::Unservable => {
                debug!(
                    "Cached extent {}-{} was marked unservable after the lookup, refusing the disk read: cache_key={}",
                    range.start, range.end, cache_key
                );
                Err(ProxyError::CacheError(format!(
                    "Cached extent {}-{} for key {} was marked unservable after the lookup",
                    range.start, range.end, cache_key
                )))
            }
            // Not in the document: find_cached_ranges may have returned it from a journal that
            // has not been consolidated yet (shared storage mode). Only an `Add` of the label's
            // version, as the streaming arms take. When nothing lists the extent at all
            // (its `.meta` was dropped since the lookup), the missing-file error, so the caller's
            // recovery refetches the extent under the label's `If-Match` and re-caches it.
            crate::http_proxy::ExtentSpecReread::NotListed => match disk_cache
                .pending_journal_range_for_label(cache_key, range.start, range.end, &range.etag)
                .await
            {
                crate::disk_cache::PendingForLabel::Found(spec) => Ok(spec),
                crate::disk_cache::PendingForLabel::OtherVersion => Err(listed_under_another()),
                crate::disk_cache::PendingForLabel::Absent => Err(ProxyError::CacheError(format!(
                    "Range spec not found for range {}-{} in key: {} (checked metadata and journals)",
                    range.start, range.end, cache_key
                ))),
            },
        }
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
    pub(crate) async fn load_range_data_from_new_storage(
        &self,
        cache_key: &str,
        range: &Range,
    ) -> Result<Vec<u8>> {
        debug!(
            "Loading range data from new storage: key={}, range={}-{}",
            cache_key, range.start, range.end
        );

        let disk_cache = self.disk_cache_manager.read().await;

        let range_spec = Self::resolve_extent_spec_for_read(&disk_cache, cache_key, range).await?;

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

    /// Load the raw (still-encoded) frame bytes for a range from new storage,
    /// without decompressing.
    ///
    /// Mirrors `load_range_data_from_new_storage`'s metadata/journal range-spec
    /// lookup, but delegates to `DiskCacheManager::load_range_frame` instead of
    /// `load_range_data`, so the caller gets the on-disk frame bytes verbatim
    /// plus the range's tagged compression algorithm. Used by the RAM-tier
    /// range-promotion path (compression-content-aware-fix Requirement 9) to
    /// avoid a decompress-then-store-uncompressed round trip.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `range` - The Range struct (which may have empty data field)
    ///
    /// # Returns
    /// The raw frame bytes and the range's compression algorithm.
    pub(crate) async fn load_range_frame_from_new_storage(
        &self,
        cache_key: &str,
        range: &Range,
    ) -> Result<(Vec<u8>, crate::compression::CompressionAlgorithm)> {
        debug!(
            "Loading range frame from new storage: key={}, range={}-{}",
            cache_key, range.start, range.end
        );

        let disk_cache = self.disk_cache_manager.read().await;

        let range_spec = Self::resolve_extent_spec_for_read(&disk_cache, cache_key, range).await?;

        // Load the raw frame bytes (no decompression)
        let (frame_data, algorithm) = disk_cache.load_range_frame(&range_spec).await?;

        debug!(
            "Successfully loaded {} byte frame for range {}-{}",
            frame_data.len(),
            range.start,
            range.end
        );
        Ok((frame_data, algorithm))
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
        } else if let Some(etag) = cache_etag.as_ref().filter(|etag| !etag.is_empty()) {
            // Add cache validation header only if client didn't specify one. Never an
            // empty one: S3 answers `If-Match:` with no value with `412`, which would
            // turn every gap fetch of an entry with no recorded ETag into a retry.
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
        } else if let Some(last_modified) = cache_last_modified.as_ref().filter(|lm| !lm.is_empty())
        {
            // Add cache validation header only if client didn't specify one, and never
            // an empty one (S3 ignores it, so it would carry a sentinel for nothing).
            conditional_headers.insert("if-unmodified-since".to_string(), last_modified.clone());
        }

        debug!(
            "Built conditional headers for range request: {:?}",
            conditional_headers
        );
        conditional_headers
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
    ///
    /// **This is the RAM cache entry point for merged reads.** Despite the
    /// "extract bytes" name suggesting pure in-memory byte slicing, this
    /// method calls `CacheManager::load_range_data_with_cache` below, which
    /// checks RAM first, falls back to disk on a miss, and promotes disk hits
    /// to RAM — the same tiered lookup any other range read gets. Every
    /// segment `merge_range_segments` assembles from a cached range goes
    /// through here, so a merged/multi-range read participates in the RAM
    /// tier per segment, keyed on that segment's stored-range bounds. A plain
    /// `grep ram_cache src/range_handler.rs` finds nothing in this file
    /// because the RAM access happens one level down, inside
    /// `CacheManager` — see the steering note on tracing call chains instead
    /// of grepping for tier participation (page-aligned-range-cache Task 10).
    pub async fn extract_bytes_from_cached_range(
        &self,
        cache_key: &str,
        cached_range: &Range,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<(Bytes, bool)> {
        self.extract_bytes_from_cached_range_tiered(
            cache_key,
            cached_range,
            requested_start,
            requested_end,
            true,
        )
        .await
    }

    /// `extract_bytes_from_cached_range`, with the disk-hit RAM promotion chosen by
    /// `promote`. `false` still consults RAM; it only declines to store the extent under
    /// its own RAM key, for a caller that stores the assembled bytes itself.
    async fn extract_bytes_from_cached_range_tiered(
        &self,
        cache_key: &str,
        cached_range: &Range,
        requested_start: u64,
        requested_end: u64,
        promote: bool,
    ) -> Result<(Bytes, bool)> {
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

        // Load the cached range data with RAM cache support. This is the RAM
        // tier access described in the doc comment above: RAM hit, disk hit
        // + promote, or disk miss, per segment.
        debug!(
            "Loading range data with RAM cache support (promote={})",
            promote
        );
        let (range_data, ram_hit) = if promote {
            self.cache_manager
                .load_range_data_with_cache(cache_key, cached_range, self)
                .await?
        } else {
            self.cache_manager
                .load_range_data_without_promotion(cache_key, cached_range, self)
                .await?
        };

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

        // A view of the loaded extent, not a copy: the caller appends it to
        // its output and drops it, which drops the extent, so a merge holds its output
        // plus one extent rather than an extent and a copy of its slice as well.
        let extracted_bytes = range_data.slice(start_idx..end_idx);

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
    pub(crate) fn calculate_overlap(
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
        fetched_ranges: &[(RangeSpec, Bytes, HashMap<String, String>)],
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
    /// **Every cached segment consults the RAM cache tier.** Each entry in
    /// `cached_ranges` is loaded through [`Self::extract_bytes_from_cached_range`],
    /// which calls `CacheManager::load_range_data_with_cache` — checking RAM,
    /// falling back to disk on a miss, and promoting disk hits to RAM. So a
    /// merged/multi-range read is not disk-only: it consults RAM, may promote
    /// to RAM, and records hit statistics, once per cached segment, keyed on
    /// that segment's stored-range bounds (not the overall requested range).
    /// `RangeMergeResult::ram_hit` (threaded from each segment's `ram_hit`
    /// into `all_ram_hits` below) is the only signal of this at this call
    /// site — nothing else here mentions RAM. Page widening uses
    /// `merge_range_segments_without_promotion` instead, which consults RAM
    /// per segment but does not promote.
    ///
    /// Requirements: 1.5, 2.1, 2.4, 3.5, 8.1, 8.2, 8.3, 8.4
    pub async fn merge_range_segments(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Bytes, HashMap<String, String>)],
    ) -> Result<RangeMergeResult> {
        self.merge_range_segments_tiered(
            cache_key,
            requested_range,
            cached_ranges,
            fetched_ranges,
            true,
        )
        .await
    }

    /// As [`Self::merge_range_segments`], except that a cached segment read from disk is
    /// NOT promoted to RAM under its own extent key. Each segment still consults RAM.
    ///
    /// For page widening, which assembles a Page from several extents and then stores the
    /// Page itself (`promote_page_to_ram`) or, on a cold fill, deliberately stores
    /// nothing. Promoting each extent as well held the same bytes in RAM twice, and on the
    /// journal-fallback arm stored extents with an empty etag.
    pub(crate) async fn merge_range_segments_without_promotion(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Bytes, HashMap<String, String>)],
    ) -> Result<RangeMergeResult> {
        self.merge_range_segments_tiered(
            cache_key,
            requested_range,
            cached_ranges,
            fetched_ranges,
            false,
        )
        .await
    }

    async fn merge_range_segments_tiered(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Bytes, HashMap<String, String>)],
        promote: bool,
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
                            .extract_bytes_from_cached_range_tiered(
                                cache_key,
                                range,
                                adjusted_start,
                                segment.output_end,
                                promote,
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
                            .extract_bytes_from_cached_range_tiered(
                                cache_key,
                                range,
                                segment.output_start,
                                segment.output_end,
                                promote,
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
            // The merge assembles into a Vec because it extends incrementally;
            // `Bytes::from` then takes that allocation without copying.
            data: Bytes::from(output),
            bytes_from_cache,
            bytes_from_s3,
            segments_merged: segments.len(),
            cache_efficiency,
            ram_hit: all_ram_hits && bytes_from_s3 == 0, // RAM hit only if all segments from RAM and no S3 fetches
        })
    }

    /// Drop a client `If-Range` from a gap sub-fetch that the proxy has pinned to the
    /// same entity tag.
    ///
    /// Each sub-fetch is buffered (`allow_streaming = false`) and only a `206` is used. A
    /// stale `If-Range` makes S3 ignore `Range` and answer `200` with the whole object,
    /// which would be read into memory and then discarded. When the sub-fetch also
    /// carries the proxy's own `If-Match` (marked by its sentinel) on the same strong
    /// tag, the `If-Range` decides nothing: S3 evaluates `If-Match` first, a failing one
    /// is a bodyless `412` that [`Self::fetch_missing_ranges`] reports as
    /// `MERGE_REFETCH_VERSION_CHANGED`, and a passing one means the object is that
    /// version, so the `If-Range` would match too. The caller then forwards the client's
    /// own request, `If-Range` intact, and S3 answers it.
    ///
    /// Applied to the sub-fetch copy only, never to the client's header map. Nothing is
    /// changed when `If-Range` is signed (header or query form), when the request has no
    /// proxy pin, or when the pin names another tag. A client `If-Range` is never turned
    /// into an `If-Match`.
    ///
    /// `pub` so the decision is asserted directly by tests.
    pub fn drop_if_range_redundant_with_pin(
        headers: &mut HashMap<String, String>,
        uri: &hyper::Uri,
    ) {
        if !headers.contains_key("x-proxy-injected-if-match") {
            return;
        }
        let (Some(if_range), Some(if_match)) = (headers.get("if-range"), headers.get("if-match"))
        else {
            return;
        };
        let if_range = if_range.trim();
        if !if_range.starts_with('"') || if_range != if_match.trim() {
            return;
        }
        let uri = uri.to_string();
        if crate::signed_request_proxy::is_header_signed(headers, "if-range")
            || crate::signed_request_proxy::is_header_signed_in_query(&uri, "if-range")
        {
            return;
        }
        headers.remove("if-range");
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
    /// Vector of (RangeSpec, Bytes, HashMap<String, String>) tuples for successfully fetched ranges
    /// The HashMap contains S3 response headers including ETag
    ///
    /// Requirements: 1.1, 1.4
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_missing_ranges(
        &self,
        cache_key: &str,
        missing_ranges: &[RangeSpec],
        s3_client: &Arc<dyn crate::s3_client::S3ClientApi + Send + Sync>,
        host: &str,
        uri: &hyper::Uri,
        headers: &HashMap<String, String>,
        hedge_budget: Option<&Arc<AtomicUsize>>,
        hedge_trigger_after: Duration,
        max_inflight_fraction: f64,
    ) -> Result<Vec<(RangeSpec, Bytes, HashMap<String, String>)>> {
        if missing_ranges.is_empty() {
            debug!("No missing ranges to fetch");
            return Ok(Vec::new());
        }

        // Every sub-fetch below replaces the `Range` header. When the client signed
        // `range`, that rewrite invalidates its signature and S3 answers `403
        // SignatureDoesNotMatch`. Refused here whichever caller forgot to route the
        // request elsewhere, so the caller can fail open with the client's own request.
        if crate::signed_request_proxy::is_range_signed(
            headers,
            crate::signed_request_proxy::request_target(uri),
        ) {
            warn!(
                "Refusing to fetch {} missing range(s) with a rewritten Range header: the client signed 'range' (cache_key={})",
                missing_ranges.len(),
                cache_key
            );
            return Err(ProxyError::SignedHeaderRewriteRefused {
                header: "range".to_string(),
                cache_key: cache_key.to_string(),
            });
        }

        debug!(
            "Fetching {} missing ranges from S3 in parallel: cache_key={}",
            missing_ranges.len(),
            cache_key
        );

        // A client `If-Range` the proxy's pin makes redundant is left off these
        // sub-fetches only; the caller's map keeps it.
        let mut sub_fetch_headers = headers.clone();
        Self::drop_if_range_redundant_with_pin(&mut sub_fetch_headers, uri);
        let headers = &sub_fetch_headers;

        // A `412` on a proxy-injected `If-Match` (marked by its sentinel) means the
        // cached version these bytes would be merged with is gone from S3.
        let proxy_pinned = headers.contains_key("x-proxy-injected-if-match");

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
                let hedge_budget = hedge_budget.cloned();

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
                    // Strip the proxy's internal sentinels before sending to S3.
                    s3_headers.remove("x-proxy-injected-if-match");
                    s3_headers.remove("x-proxy-injected-if-unmodified-since");

                    let mut context = crate::s3_client::build_s3_request_context(
                        hyper::Method::GET,
                        uri,
                        s3_headers,
                        None,
                        host.clone(),
                    );
                    // Don't use streaming for fetch_missing_ranges - we need to buffer the data
                    // to merge it with cached data before sending to client
                    context.allow_streaming = false;

                    // Hedging path: when a budget is provided (rule-enabled), race this
                    // sub-fetch against a hedge. Each sub-fetch selects its own IP pair
                    // and claims from the single per-request budget shared across all N
                    // parallel sub-fetches. `None` budget → the existing unhedged path,
                    // byte-identical to previous behaviour (Req 1.3, 2.3, 6.1, 6.5).
                    let s3_response = crate::hedged_fetch::fetch_maybe_hedged(
                        s3_client.as_ref(),
                        context,
                        &host,
                        hedge_budget.as_deref(),
                        hedge_trigger_after,
                        max_inflight_fraction,
                        &cache_key,
                    )
                    .await;

                    match s3_response {
                        Ok(s3_response) => {
                            if s3_response.status == hyper::StatusCode::PARTIAL_CONTENT {
                                if let Some(body) = s3_response.body {
                                    let fetch_duration = fetch_start.elapsed();
                                    // Hand the collected Bytes straight through. This
                                    // previously did `body_bytes.to_vec()`, holding the
                                    // fetched range twice for the rest of the scope.
                                    // Requirement: IMA 5.2
                                    let body_bytes = body.into_bytes().await?;
                                    let response_headers = s3_response.headers.clone();
                                    debug!(
                                        "Successfully fetched range from S3: cache_key={}, range={}-{}, size={} bytes, duration={:.2}ms",
                                        cache_key,
                                        range_spec.start,
                                        range_spec.end,
                                        body_bytes.len(),
                                        fetch_duration.as_secs_f64() * 1000.0
                                    );
                                    Ok((range_spec, body_bytes, response_headers))
                                } else {
                                    warn!(
                                        "S3 response has no body: cache_key={}, range={}-{}",
                                        cache_key, range_spec.start, range_spec.end
                                    );
                                    Err(crate::ProxyError::S3Error(
                                        "S3 response has no body".to_string()
                                    ))
                                }
                            } else if s3_response.status == hyper::StatusCode::PRECONDITION_FAILED
                                && proxy_pinned
                            {
                                warn!(
                                    "S3 refused the proxy's If-Match pin on a missing-range fetch: cache_key={}, range={}-{}",
                                    cache_key, range_spec.start, range_spec.end
                                );
                                Err(crate::ProxyError::S3Error(format!(
                                    "{}: cache_key={}",
                                    MERGE_REFETCH_VERSION_CHANGED, cache_key
                                )))
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
        let mut version_change: Option<ProxyError> = None;

        for result in results {
            match result {
                Ok((range_spec, data, headers)) => {
                    fetched_ranges.push((range_spec, data, headers));
                }
                Err(e) => {
                    failed_count += 1;
                    warn!("Range fetch failed: {}", e);
                    if is_merge_refetch_version_change(&e) {
                        version_change = Some(e);
                    }
                }
            }
        }

        if fetched_ranges.is_empty() && !missing_ranges.is_empty() {
            // Every sub-fetch failed. When one of them was a refused pin, say so, so
            // a caller can tell "the cached version is gone" from an S3 failure.
            if let Some(e) = version_change {
                return Err(e);
            }
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
    /// * `caller_reservation` - An in-flight ledger reservation the caller
    ///   already holds covering the requested range's bytes, if any. Pass it
    ///   when the caller reserved before calling (both cached-serve paths do),
    ///   so a fallback fetch of those same bytes reuses that claim instead of
    ///   taking a second one for the same memory. Pass `None` when the caller
    ///   holds nothing, and the fallback will reserve for itself.
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
    #[allow(clippy::too_many_arguments)]
    pub async fn merge_ranges_with_fallback(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        cached_ranges: &[Range],
        fetched_ranges: &[(RangeSpec, Bytes, HashMap<String, String>)],
        s3_client: &Arc<dyn crate::s3_client::S3ClientApi + Send + Sync>,
        host: &str,
        uri: &hyper::Uri,
        headers: &HashMap<String, String>,
        mut caller_reservation: Option<&mut crate::inflight_ledger::Reservation>,
    ) -> Result<RangeMergeResult> {
        // Backstop (buffered-serve-memory-bound R1.2): refuse to materialise a range, or a
        // cached extent, above the buffered-operation bound. Checked here rather than in
        // `merge_range_segments` because page widening calls that directly with a
        // Page-sized range (16 MiB by default) and has its own bound; and checked BEFORE
        // the merge so the complete-refetch fallback below cannot buffer it either.
        let requested_bytes = requested_range.end - requested_range.start + 1;
        let largest_extent_bytes = cached_ranges
            .iter()
            .map(|r| r.end - r.start + 1)
            .max()
            .unwrap_or(0);
        if !crate::http_proxy::may_materialise(requested_bytes, cached_ranges) {
            warn!(
                "Range merge refused over the buffered-operation bound: cache_key={}, requested={} bytes, largest extent={} bytes",
                cache_key, requested_bytes, largest_extent_bytes
            );
            return Err(ProxyError::BufferedOperationBoundExceeded {
                requested_bytes,
                largest_extent_bytes,
            });
        }
        // The ETag every caller serves the merged bytes under (`cached_ranges[0]`),
        // which the fallback refetch is pinned to.
        let cached_etag = cached_ranges.first().map(|r| r.etag.as_str()).unwrap_or("");
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
                            cached_etag,
                            caller_reservation.as_deref_mut(),
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

                // Fall back to complete S3 fetch. A gap means the cached
                // extents did not cover the request, so expose it separately
                // from ordinary misses in cache statistics.
                if error_context == "byte alignment mismatch" {
                    self.cache_manager.record_incomplete_range_fallback();
                }
                self.fallback_to_complete_s3_fetch(
                    cache_key,
                    requested_range,
                    s3_client,
                    host,
                    uri,
                    headers,
                    error_context,
                    cached_etag,
                    caller_reservation,
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
    /// * `caller_reservation` - An in-flight ledger reservation the caller
    ///   already holds for the same bytes this fetch will buffer, if any
    ///
    /// # Returns
    /// RangeMergeResult with data from S3 and 0% cache efficiency
    #[allow(clippy::too_many_arguments)]
    async fn fallback_to_complete_s3_fetch(
        &self,
        cache_key: &str,
        requested_range: &RangeSpec,
        s3_client: &Arc<dyn crate::s3_client::S3ClientApi + Send + Sync>,
        host: &str,
        uri: &hyper::Uri,
        headers: &HashMap<String, String>,
        reason: &str,
        cached_etag: &str,
        caller_reservation: Option<&mut crate::inflight_ledger::Reservation>,
    ) -> Result<RangeMergeResult> {
        info!(
            "Falling back to complete S3 fetch: range={}-{} reason={} cache_key={}",
            requested_range.start, requested_range.end, reason, cache_key
        );

        let fallback_start = std::time::Instant::now();

        // Build S3 request with Range header.
        //
        // A client that signed `range` gets its own header value forwarded
        // byte-for-byte: a normalised `bytes=a-b` for a suffix, open-ended or
        // past-the-end request is a different string and so a different signature.
        // That is valid only when `requested_range` is the client's range; a caller
        // asking for some other extent under a signed `range` is refused, and the
        // caller fails open with the client's own request.
        let mut s3_headers = headers.clone();
        if crate::signed_request_proxy::is_range_signed(
            &s3_headers,
            crate::signed_request_proxy::request_target(uri),
        ) {
            let client_range = s3_headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("range"))
                .map(|(_, v)| v.clone());
            let covers = client_range
                .as_deref()
                .map(|raw| signed_client_range_covers(raw, requested_range).unwrap_or(true));
            if covers != Some(true) {
                warn!(
                    "Refusing fallback fetch of range {}-{}: the client signed a different Range header {:?} (cache_key={})",
                    requested_range.start, requested_range.end, client_range, cache_key
                );
                return Err(ProxyError::SignedHeaderRewriteRefused {
                    header: "range".to_string(),
                    cache_key: cache_key.to_string(),
                });
            }
            debug!(
                "Fallback fetch keeps the client's signed Range header verbatim: range={}-{} cache_key={}",
                requested_range.start, requested_range.end, cache_key
            );
        } else {
            s3_headers.retain(|k, _| !k.eq_ignore_ascii_case("range"));
            s3_headers.insert(
                "range".to_string(),
                format!("bytes={}-{}", requested_range.start, requested_range.end),
            );
        }
        // An `If-Match` already present was put there by the proxy (the Mode B
        // `If-Range` swap) when this sentinel accompanies it, so a `412` on it is
        // evidence the cached version is gone, exactly like the pin below.
        let sentinel_if_match = s3_headers.remove("x-proxy-injected-if-match").is_some();
        // Strip the proxy's other internal sentinel before sending to S3.
        s3_headers.remove("x-proxy-injected-if-unmodified-since");

        // Every caller assembles the response under the cached object's headers,
        // so the refetched bytes must be that version. Pin the refetch with
        // `If-Match: <cached ETag>` on the same terms as the single-extent recovery
        // refetch in `HttpProxy::get_cached_range_data`: only when the request has
        // no `If-Match` of its own and `if-match` is outside its SigV4 signature.
        // A request that cannot be pinned is still checked by the ETag comparison
        // on the response below.
        let pinned_if_match = !cached_etag.is_empty()
            && crate::http_proxy::HttpProxy::recovery_refetch_may_pin_if_match(
                &s3_headers,
                &uri.to_string(),
                cached_etag,
            );
        if pinned_if_match {
            s3_headers.insert("if-match".to_string(), cached_etag.to_string());
        }
        let proxy_if_match = pinned_if_match || sentinel_if_match;

        let mut context = crate::s3_client::build_s3_request_context(
            hyper::Method::GET,
            uri.clone(),
            s3_headers,
            None,
            host.to_string(),
        );
        context.allow_streaming = true; // Enable streaming for fallback fetches

        // Admission_Check before buffering: `allow_streaming = true` above means
        // `S3Client` itself does not reserve (it only reserves on its
        // `allow_streaming == false` buffering path), but this call site
        // immediately calls `into_bytes()` on the "streaming" body below,
        // buffering it here instead — one of the design's explicitly-named
        // exceptions ("range_handler.rs:2285 sets allow_streaming = true but
        // then calls into_bytes() anyway, so it needs its own reservation").
        // The requested range's byte length is known up front.
        //
        // `claim_overlapping`, not `try_reserve`, because this repair fetch is
        // frequently nested inside a caller that has ALREADY reserved for the
        // very same bytes — `serve_range_from_cache_buffered` and
        // `serve_full_object_from_cache` both reserve `range_spec`'s length and
        // then hand the merged result straight to the response body, which is
        // this fetch's buffer (refcounted `Bytes`, so the two are one
        // allocation). Reserving again there counted the same memory twice, so a
        // ceiling that comfortably fits the range refused the repair and kept
        // refusing it on every retry — the request's own claim was the thing in
        // the way, so waiting never helped.
        //
        // Neither under- nor double-counts: with a caller claim the bytes are
        // counted once (grown only if this buffer is genuinely larger, so the
        // ledger holds the real peak rather than a sum); with no caller claim —
        // the partial-cache merge path in `forward_range_request_to_s3`, which
        // holds no reservation of its own — this is an ordinary reservation for
        // bytes nothing else accounts for.
        // Requirements: IMA 1.2, 1.3, 2.1, 2.5, 4.1.
        let fallback_fetch_bytes = requested_range.end - requested_range.start + 1;
        let ledger = s3_client.get_inflight_ledger();
        let _fallback_claim =
            match ledger.claim_overlapping(fallback_fetch_bytes, caller_reservation) {
                Some(claim) => claim,
                None => {
                    return Err(ProxyError::InflightCeilingExceeded {
                        ceiling_bytes: ledger.ceiling_bytes(),
                        requested_bytes: fallback_fetch_bytes,
                    });
                }
            };

        match s3_client.forward_request(context).await {
            Ok(s3_response) => {
                // A version change is refused before any body is collected: a `412`
                // on a proxy-added `If-Match`, or a success carrying an ETag other
                // than the cached one. A response with no ETag is not evidence of a
                // change and keeps the status and length checks below.
                if !cached_etag.is_empty() {
                    let response_etag = s3_response
                        .headers
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
                        .map(|(_, v)| v.as_str())
                        .filter(|v| !v.is_empty());
                    let changed = (s3_response.status == hyper::StatusCode::PRECONDITION_FAILED
                        && proxy_if_match)
                        || matches!(
                            (s3_response.status, response_etag),
                            (hyper::StatusCode::PARTIAL_CONTENT | hyper::StatusCode::OK, Some(etag))
                                if !crate::revalidation::etags_equal_ignoring_quotes(etag, cached_etag)
                        );
                    if changed {
                        warn!(
                            "Fallback S3 fetch returned another version of the object: cache_key={}, status={}, cached_etag={}, response_etag={:?}",
                            cache_key, s3_response.status, cached_etag, response_etag
                        );
                        return Err(ProxyError::S3Error(format!(
                            "{}: cache_key={}",
                            MERGE_REFETCH_VERSION_CHANGED, cache_key
                        )));
                    }
                }
                if s3_response.status == hyper::StatusCode::PARTIAL_CONTENT {
                    if let Some(body) = s3_response.body {
                        // Keep the collected Bytes and move it into the result. This
                        // previously did `.to_vec()` and then `body_vec.clone()` into
                        // `data`, so a fallback fetch held the range three times at
                        // peak. Requirement: IMA 5.2
                        let body_bytes = body.into_bytes().await?;
                        let body_len = body_bytes.len() as u64;
                        let expected_size = requested_range.end - requested_range.start + 1;

                        // Validate response size
                        if body_len != expected_size {
                            return Err(ProxyError::S3Error(format!(
                                "S3 fallback fetch size mismatch: expected {} bytes, got {} bytes",
                                expected_size, body_len
                            )));
                        }

                        let fallback_duration = fallback_start.elapsed();

                        info!(
                            "Fallback S3 fetch succeeded: range={}-{} size={} bytes duration={:.2}ms cache_key={}",
                            requested_range.start,
                            requested_range.end,
                            body_len,
                            fallback_duration.as_secs_f64() * 1000.0,
                            cache_key
                        );

                        // Return result with 0% cache efficiency (all from S3)
                        Ok(RangeMergeResult {
                            data: body_bytes,
                            bytes_from_cache: 0,
                            bytes_from_s3: body_len,
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
            cache_dir, true, 1024, false, 1_048_576,
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

#[cfg(test)]
mod page_geometry_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    const P: u64 = 16 * 1024 * 1024; // default page size
    const SIZE: u64 = 100 * 1024 * 1024; // object larger than several pages

    #[test]
    fn page_bounds_first_page() {
        assert_eq!(page_bounds(0, P, SIZE), (0, P - 1));
        assert_eq!(page_bounds(P - 1, P, SIZE), (0, P - 1));
    }

    #[test]
    fn page_bounds_second_page() {
        assert_eq!(page_bounds(P, P, SIZE), (P, 2 * P - 1));
        assert_eq!(page_bounds(P + 100, P, SIZE), (P, 2 * P - 1));
        assert_eq!(page_bounds(2 * P - 1, P, SIZE), (P, 2 * P - 1));
    }

    #[test]
    fn page_bounds_last_page_clamped_to_size_minus_one() {
        // Object size is not an exact multiple of P; last page must clamp.
        let size = 3 * P + 100; // remainder page is only 100 bytes
        let last_page_start = 3 * P;
        assert_eq!(
            page_bounds(last_page_start, P, size),
            (last_page_start, size - 1)
        );
        // A start within the remainder still clamps to size-1, not
        // last_page_start + P - 1 (which would exceed the object).
        assert_eq!(page_bounds(size - 1, P, size), (last_page_start, size - 1));
    }

    #[test]
    fn page_bounds_object_smaller_than_a_page() {
        // Object entirely within a single (clamped) page.
        let size = 1000;
        assert_eq!(page_bounds(0, P, size), (0, size - 1));
        assert_eq!(page_bounds(500, P, size), (0, size - 1));
    }

    #[test]
    #[should_panic(expected = "page_size must be > 0")]
    fn page_bounds_panics_on_zero_page_size() {
        page_bounds(0, 0, SIZE);
    }

    #[test]
    #[should_panic(expected = "size must be > 0")]
    fn page_bounds_panics_on_zero_size() {
        page_bounds(0, P, 0);
    }

    #[test]
    fn overlapping_pages_single_page_read() {
        let pages = overlapping_pages(10, 20, P, SIZE);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0], (0, P - 1));
    }

    #[test]
    fn overlapping_pages_straddle_returns_two_pages_in_order() {
        // Read spans the boundary between page 0 and page 1.
        let pages = overlapping_pages(P - 10, P + 10, P, SIZE);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0], (0, P - 1));
        assert_eq!(pages[1], (P, 2 * P - 1));
    }

    #[test]
    fn overlapping_pages_straddle_across_three_or_more_pages_still_returns_endpoints() {
        // A read spanning page 0 through page 2 (unusual for a Small_Read,
        // since Small_Read is length < P, but the helper itself is generic
        // over any [start, end] and must still return the correct first/last
        // page bounds without panicking).
        let pages = overlapping_pages(10, 2 * P + 10, P, SIZE);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0], (0, P - 1));
        assert_eq!(pages[1], (2 * P, 3 * P - 1));
    }

    #[test]
    fn overlapping_pages_exactly_at_boundary_start_is_single_page() {
        // [P, P+10] starts exactly on a page boundary and stays within it.
        let pages = overlapping_pages(P, P + 10, P, SIZE);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0], (P, 2 * P - 1));
    }

    #[test]
    fn overlapping_pages_last_page_clamp_reflected_in_result() {
        let size = 3 * P + 100;
        let pages = overlapping_pages(size - 50, size - 1, P, size);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0], (3 * P, size - 1));
    }

    #[test]
    #[should_panic(expected = "range end must be >= start")]
    fn overlapping_pages_panics_on_inverted_range() {
        overlapping_pages(10, 5, P, SIZE);
    }

    #[test]
    fn suffix_page_target_size_unknown_returns_sentinel() {
        assert_eq!(
            suffix_page_target(1024, P, None),
            SuffixPageTarget::SizeUnknown
        );
    }

    #[test]
    fn suffix_page_target_size_known_cold_maps_to_last_page() {
        // A small footer read comfortably within the last page's remainder.
        match suffix_page_target(1024, P, Some(SIZE)) {
            SuffixPageTarget::Pages(pages) => {
                assert_eq!(pages.len(), 1);
                // SIZE = 100 MiB is not an exact multiple of P = 16 MiB, so
                // the last grid page starts at floor(SIZE / P) * P.
                let expected_last_page_start = (SIZE / P) * P;
                assert_eq!(pages[0], (expected_last_page_start, SIZE - 1));
            }
            SuffixPageTarget::SizeUnknown => panic!("size was provided"),
        }
    }

    #[test]
    fn suffix_page_target_warm_footer_larger_than_remainder_spans_two_pages() {
        // Object size is just over a page multiple: the last grid page's
        // remainder (100 bytes) is smaller than the requested footer
        // (1000 bytes), so the suffix must straddle into the prior page too
        // (never under-fetch — Requirement 3.2).
        let size = 3 * P + 100;
        match suffix_page_target(1000, P, Some(size)) {
            SuffixPageTarget::Pages(pages) => {
                assert_eq!(pages.len(), 2);
                assert_eq!(pages[0], (2 * P, 3 * P - 1));
                assert_eq!(pages[1], (3 * P, size - 1));
            }
            SuffixPageTarget::SizeUnknown => panic!("size was provided"),
        }
    }

    #[test]
    fn suffix_page_target_n_larger_than_size_clamps_to_whole_object() {
        let size = 1000u64;
        match suffix_page_target(5000, P, Some(size)) {
            SuffixPageTarget::Pages(pages) => {
                assert_eq!(pages.len(), 1);
                assert_eq!(pages[0], (0, size - 1));
            }
            SuffixPageTarget::SizeUnknown => panic!("size was provided"),
        }
    }

    #[test]
    #[should_panic(expected = "suffix length must be > 0")]
    fn suffix_page_target_panics_on_zero_n() {
        suffix_page_target(0, P, Some(SIZE));
    }

    // ---- Property tests ----
    // **Validates: Requirements 3.1, 3.2, 3.3**

    /// For any start offset and page size/object size (all non-zero, start
    /// within the object), `page_bounds` returns a page that contains `start`
    /// and never extends past `size - 1`.
    #[quickcheck]
    fn prop_page_bounds_contains_start_and_never_exceeds_size(
        start: u64,
        page_size: u64,
        size: u64,
    ) -> TestResult {
        if page_size == 0 || size == 0 {
            return TestResult::discard();
        }
        // Keep inputs in a sane range to avoid overflow in the test itself.
        let page_size = (page_size % (64 * 1024 * 1024)).max(1);
        let size = (size % (1024 * 1024 * 1024)).max(1);
        let start = start % size;

        let (page_start, page_end) = page_bounds(start, page_size, size);

        TestResult::from_bool(
            page_start <= start && start <= page_end && page_end < size && page_start <= page_end,
        )
    }

    /// A read that straddles a page boundary always yields exactly 2 pages;
    /// a read fully within one page always yields exactly 1.
    #[quickcheck]
    fn prop_straddle_detection_matches_page_index_equality(
        start: u64,
        len: u64,
        page_size: u64,
    ) -> TestResult {
        if page_size == 0 || len == 0 {
            return TestResult::discard();
        }
        let page_size = (page_size % (64 * 1024 * 1024)).max(1);
        // Keep the read shorter than the page so this mirrors a real
        // Small_Read, and keep the whole thing comfortably within bounds.
        let len = (len % page_size).max(1);
        let size = 1024 * page_size; // plenty of pages, no clamp interference
        let start = start % (size - len);
        let end = start + len - 1;

        let pages = overlapping_pages(start, end, page_size, size);
        let start_page_index = start / page_size;
        let end_page_index = end / page_size;

        let expected_len = if start_page_index == end_page_index {
            1
        } else {
            2
        };

        TestResult::from_bool(pages.len() == expected_len)
    }

    /// The widened target from `overlapping_pages` is always a superset of
    /// the requested `[start, end]` range — the global "never a subset"
    /// invariant from the requirements' Resolved Decisions.
    #[quickcheck]
    fn prop_overlapping_pages_is_superset_of_requested_range(
        start: u64,
        len: u64,
        page_size: u64,
    ) -> TestResult {
        if page_size == 0 || len == 0 {
            return TestResult::discard();
        }
        let page_size = (page_size % (64 * 1024 * 1024)).max(1);
        let len = (len % (4 * page_size)).max(1);
        let size = 1024 * page_size;
        let start = start % (size - len);
        let end = start + len - 1;

        let pages = overlapping_pages(start, end, page_size, size);
        let target_start = pages.first().unwrap().0;
        let target_end = pages.last().unwrap().1;

        TestResult::from_bool(target_start <= start && target_end >= end)
    }

    /// Suffix widening (size known) is always a superset of the requested
    /// `[size - n, size - 1]` bytes, never a subset — even when the object
    /// size is just over a page multiple and the last page's remainder is
    /// smaller than `n`.
    #[quickcheck]
    fn prop_suffix_page_target_is_superset_of_requested_suffix(
        n: u64,
        page_size: u64,
        size: u64,
    ) -> TestResult {
        if n == 0 || page_size == 0 || size == 0 {
            return TestResult::discard();
        }
        let page_size = (page_size % (64 * 1024 * 1024)).max(1);
        let size = (size % (1024 * 1024 * 1024)).max(page_size);
        let n = (n % size).max(1);

        let requested_start = size - n;
        let requested_end = size - 1;

        match suffix_page_target(n, page_size, Some(size)) {
            SuffixPageTarget::Pages(pages) => {
                let target_start = pages.first().unwrap().0;
                let target_end = pages.last().unwrap().1;
                TestResult::from_bool(
                    target_start <= requested_start && target_end >= requested_end,
                )
            }
            SuffixPageTarget::SizeUnknown => TestResult::failed(),
        }
    }

    /// Suffix widening never returns more than 2 pages (Requirement 3.2:
    /// "the overlapping grid page(s) — 1 or 2").
    #[quickcheck]
    fn prop_suffix_page_target_returns_one_or_two_pages(
        n: u64,
        page_size: u64,
        size: u64,
    ) -> TestResult {
        if n == 0 || page_size == 0 || size == 0 {
            return TestResult::discard();
        }
        let page_size = (page_size % (64 * 1024 * 1024)).max(1);
        let size = (size % (1024 * 1024 * 1024)).max(page_size);
        let n = (n % size).max(1);

        match suffix_page_target(n, page_size, Some(size)) {
            SuffixPageTarget::Pages(pages) => {
                TestResult::from_bool(pages.len() == 1 || pages.len() == 2)
            }
            SuffixPageTarget::SizeUnknown => TestResult::failed(),
        }
    }

    /// Size-unknown suffix widening always returns the sentinel, regardless
    /// of `n`/`page_size` (Requirement 3.3).
    #[quickcheck]
    fn prop_suffix_page_target_size_unknown_always_sentinel(n: u64, page_size: u64) -> TestResult {
        if n == 0 {
            return TestResult::discard();
        }
        TestResult::from_bool(
            suffix_page_target(n, page_size, None) == SuffixPageTarget::SizeUnknown,
        )
    }
}
