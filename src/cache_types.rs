//! Cache Types Module
//!
//! Provides basic cache data structures used by the disk cache system.

use crate::compression::CompressionAlgorithm;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Maximum TTL duration: 10 years in seconds.
const MAX_TTL_SECS: u64 = 315_360_000;

/// Compute expiry without panicking. Clamps to 10 years from base on overflow.
///
/// This function replaces direct `SystemTime + Duration` arithmetic in TTL paths
/// to prevent panics when a large or overflowing duration is supplied.
///
/// Only call this when TTL > 0. TTL=0 means "do not cache / per-request auth"
/// and should bypass expiry computation entirely.
pub(crate) fn safe_expiry(base: SystemTime, ttl: Duration) -> SystemTime {
    const MAX_TTL: Duration = Duration::from_secs(MAX_TTL_SECS);
    let clamped_ttl = if ttl > MAX_TTL { MAX_TTL } else { ttl };
    base.checked_add(clamped_ttl)
        .unwrap_or_else(|| base.checked_add(MAX_TTL).unwrap_or(base))
}

/// Result of checking object-level cache expiration.
/// Used by `check_object_expiration` to indicate whether cached data is fresh
/// or needs revalidation against S3.
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectExpirationResult {
    /// Object is fresh, no revalidation needed
    Fresh,
    /// Object is expired or metadata unreadable, revalidation needed
    Expired {
        /// last_modified for If-Modified-Since header, None if metadata was unreadable
        last_modified: Option<String>,
        /// etag for If-None-Match header, None if not available
        etag: Option<String>,
    },
}

/// Why a caller is looking up cached range coverage.
///
/// The two mechanisms this distinguishes are documented in
/// the cache coherency invariants § "The two freshness
/// mechanisms". Stored expiry (`NewCacheMetadata::expires_at`) bounds the lookup;
/// live TTL (`created_at` against the currently resolved `get_ttl`,
/// `DiskCacheManager::check_object_expiration`) is the mainline serve gate.
///
/// Before this existed, the lookup applied stored expiry unconditionally and
/// returned nothing once it had passed. That is correct for a caller about to
/// serve the bytes and wrong for one about to *revalidate* them: the metadata,
/// the ETag and the range files are all still there, and hiding them made the
/// live-TTL check and the whole conditional-request path unreachable. Every
/// sequential re-read of an expired-but-present entry became a full body
/// transfer plus a cache rewrite. See
/// the expired entry revalidation feature and GitHub issue #17.
///
/// # Choosing one
///
/// There is deliberately no `Default`. A new call site must state which contract
/// it holds, because the safety condition is not recoverable from the
/// surrounding code — the spec's Requirement 1.4 forbids a boolean whose meaning
/// has to be inferred from a call-site literal.
///
/// Ask: **between this lookup and returning bytes to a client, what bounds the
/// staleness?**
///
/// - a live-TTL verdict, a client validator (Mode B), or a successful `304` →
///   [`RangeLookupPurpose::RevalidationCandidate`];
/// - nothing but the lookup itself → [`RangeLookupPurpose::FreshServe`].
///
/// If the answer is "nothing at all", neither variant is right and the call site
/// is a defect regardless of which one it picks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeLookupPurpose {
    /// The caller may serve what it receives without applying any further
    /// freshness gate, so the lookup must apply stored expiry itself and return
    /// no ranges once it has passed.
    ///
    /// This is the conservative choice and the correct one for page widening,
    /// direct cache reads, part-scoped lookups, and the degraded coalescing
    /// fallbacks.
    FreshServe,

    /// The caller will evaluate freshness itself — live TTL, then a conditional
    /// request against S3 if the entry is expired — before serving anything.
    ///
    /// The lookup therefore continues past stored expiry and reports the
    /// condition via [`StoredFreshness`] instead of hiding the coverage.
    ///
    /// **This grants discovery, not permission.** A caller that receives
    /// [`StoredFreshness::Expired`] holds no authority to serve those bytes; it
    /// must first obtain one (a live-TTL `Fresh` verdict, a matching client
    /// `If-Match`, or a `304`). [`crate::range_handler::RangeOverlap`] enforces
    /// the distinction in its accessors rather than leaving it to convention.
    RevalidationCandidate,
}

/// Whether the entry backing a lookup result was within its stored expiry.
///
/// Carried alongside the coverage so an expired candidate cannot be mistaken for
/// ordinary serveable data by a caller that never looked. Requirement 1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredFreshness {
    /// `SystemTime::now() <= NewCacheMetadata::expires_at`.
    Fresh,
    /// `SystemTime::now() > NewCacheMetadata::expires_at`.
    ///
    /// Only ever returned with non-empty coverage under
    /// [`RangeLookupPurpose::RevalidationCandidate`]. Under `FreshServe` the
    /// coverage is empty, so the two variants are behaviourally identical there
    /// and existing `FreshServe` callers are unaffected.
    Expired,
}

/// What a range lookup found, and whether the entry it came from was stored-fresh.
///
/// Returned instead of a bare `Vec<RangeSpec>` so the expiry condition travels
/// with the coverage. A caller cannot accidentally treat an expired candidate as
/// serveable data, because it has to destructure this to get at the ranges at
/// all.
#[derive(Debug, Clone)]
#[must_use]
pub struct RangeLookupResult {
    /// Cached extents overlapping the requested interval, sorted by `start`.
    /// Empty means no usable coverage was found.
    pub ranges: Vec<RangeSpec>,
    /// Stored-expiry state of the entry these extents came from.
    ///
    /// `Fresh` when no metadata was consulted at all (no entry, or a
    /// journal-only fallback), since there is no `expires_at` to have passed —
    /// journal records carry neither expiry nor validators and keep their
    /// existing semantics under this spec.
    pub stored_freshness: StoredFreshness,
}

impl RangeLookupResult {
    /// An empty result from a lookup that consulted no expiry.
    pub fn empty() -> Self {
        Self {
            ranges: Vec::new(),
            stored_freshness: StoredFreshness::Fresh,
        }
    }

    /// Coverage from a stored-fresh entry.
    pub fn fresh(ranges: Vec<RangeSpec>) -> Self {
        Self {
            ranges,
            stored_freshness: StoredFreshness::Fresh,
        }
    }

    /// Was any overlapping coverage found?
    ///
    /// Kept (rather than making callers write `.ranges.is_empty()`) because it reads
    /// the same as the `Vec` this type replaced, which is what most call sites want.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

/// Cache metadata for S3 objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    pub etag: String,
    pub last_modified: String,
    pub content_length: u64,
    pub part_number: Option<u32>,
    pub cache_control: Option<String>,
    /// Access count for cache statistics (used for HEAD cache access tracking)
    #[serde(default)]
    pub access_count: u64,
    /// Last accessed time for cache statistics (used for HEAD cache access tracking)
    #[serde(default = "SystemTime::now")]
    pub last_accessed: SystemTime,
}

/// Byte range for partial object caching
/// Note: Data is stored separately in range files, not in this struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Range {
    pub start: u64,
    pub end: u64,
    pub etag: String,
    pub last_modified: String,
    pub compression_algorithm: CompressionAlgorithm,
}

/// Compression information for cache entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionInfo {
    pub body_algorithm: CompressionAlgorithm, // Algorithm used for body
    pub original_size: Option<u64>,           // Original size before compression
    pub compressed_size: Option<u64>,         // Size after compression
    pub file_extension: Option<String>,       // File extension when cached
}

impl Default for CompressionInfo {
    fn default() -> Self {
        Self {
            body_algorithm: CompressionAlgorithm::Lz4,
            original_size: None,
            compressed_size: None,
            file_extension: None,
        }
    }
}

// ============================================================================
// New Range Storage Architecture Data Structures
// ============================================================================

/// Upload state for multipart tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum UploadState {
    #[default]
    Complete, // Regular PUT or completed multipart
    InProgress, // Multipart upload in progress
    Bypassed,   // Upload too large, not caching
}

/// Part information stored temporarily during upload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartInfo {
    pub part_number: u32,
    pub size: u64,
    pub etag: String,
    pub data: Vec<u8>, // Temporary storage until CompleteMultipartUpload
}

// ============================================================================
// Write Cache Multipart Upload Tracking (Requirements 2.2, 8.3)
// ============================================================================

/// Information about a cached part in a multipart upload
/// Stored in mpus_in_progress/{uploadId}/upload.meta
/// Part data is stored at mpus_in_progress/{uploadId}/part{N}.bin
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedPartInfo {
    /// Part number (1-10000 per S3 spec)
    pub part_number: u32,
    /// **Uncompressed** size of the part in bytes, i.e. the number of object bytes
    /// this part contributes.
    ///
    /// This is the S3-facing figure: it is what the completed object's byte offsets
    /// and `total_size` are built from (`finalize_multipart_upload`). It is
    /// deliberately NOT the length of `part{N}.bin` on disk, which is compressed
    /// whenever compression applies — see [`Self::compressed_size`]. The two are
    /// separate figures in separate units and must not be harmonised.
    pub size: u64,
    /// **Compressed on-disk length** of `mpus_in_progress/{uploadId}/part{N}.bin`,
    /// `stat`-ed at part-finalize time inside the `part{N}.lock` hold, immediately
    /// after the rename that published the file.
    ///
    /// This is the unit `in_progress_part_bytes` is summed in: real bytes occupying
    /// the shared volume. It is measured here rather than derived from [`Self::size`]
    /// because the on-disk file is LZ4-framed, and it is measured *here* rather than
    /// when the sum is taken because on a retried `CompleteMultipartUpload` the
    /// `.bin` has already been renamed into `ranges/` and a later `stat` would fail.
    ///
    /// # `#[serde(default)]` here is a struct-literal convenience and MUST NOT be used to
    /// read the byte figure
    ///
    /// A record written by a build predating this field has no such key, and this default
    /// maps that to `0` — the same value a measured zero produces. Summing `P` through this
    /// type therefore **under-counts** a mixed-version fleet's in-progress bytes, and
    /// § 16.6.2 is explicit that under-counting is the unsafe direction because it makes the
    /// staging eviction trigger fire late.
    ///
    /// `in_progress_part_bytes` consequently reads part records through its own
    /// presence-preserving `PartRecordSizeProbe` (`Option<u64>`) and reports UNREADABLE for
    /// a record with no field, so the fold retains its previous figure rather than
    /// installing a short one. Do not "simplify" that back to this type.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.2, 16.6.6.
    #[serde(default)]
    pub compressed_size: u64,
    /// ETag returned by S3 for this part
    pub etag: String,
    /// Compression algorithm used for this part (None if uncompressed)
    #[serde(default)]
    pub compression_algorithm: crate::compression::CompressionAlgorithm,
    /// Identity of the part's `.bin` as its writer published it
    /// ([`RangeSpec::file_identity`]). The completion renames that file into `ranges/`
    /// unchanged, which keeps its length and modification time, so it is carried onto the
    /// range the part becomes. `None` for a record written before the field existed, or one
    /// whose recorded identity is incomplete ([`lenient_file_identity`]), whose range is then
    /// unverified. Omitted from the record when `None`.
    #[serde(
        default,
        deserialize_with = "lenient_file_identity",
        skip_serializing_if = "Option::is_none"
    )]
    pub file_identity: Option<RangeFileIdentity>,
}
impl CachedPartInfo {
    /// Create a new CachedPartInfo
    pub fn new(
        part_number: u32,
        size: u64,
        etag: String,
        compression_algorithm: crate::compression::CompressionAlgorithm,
    ) -> Self {
        Self {
            part_number,
            size,
            compressed_size: 0,
            etag,
            compression_algorithm,
            file_identity: None,
        }
    }

    /// Create a new CachedPartInfo with default compression (Lz4)
    ///
    /// Test seam: production builds parts with [`Self::new`] and an explicit algorithm.
    #[cfg(test)]
    pub(crate) fn new_uncompressed(part_number: u32, size: u64, etag: String) -> Self {
        Self {
            part_number,
            size,
            compressed_size: 0,
            etag,
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
            file_identity: None,
        }
    }

    /// Attach the measured compressed on-disk length of `part{N}.bin`.
    ///
    /// A builder rather than a constructor parameter so that the measurement site is
    /// visibly separate from the S3-facing [`Self::size`]: the only correct source is
    /// a `stat` of the finalized part file, and no caller should be able to pass
    /// `size` here by mistake.
    pub(crate) fn with_compressed_size(mut self, compressed_size: u64) -> Self {
        self.compressed_size = compressed_size;
        self
    }
    /// Attach the identity of the part file its writer published.
    pub(crate) fn with_file_identity(mut self, file_identity: Option<RangeFileIdentity>) -> Self {
        self.file_identity = file_identity;
        self
    }
}

/// Tracks an in-progress multipart upload for write caching
/// Stored as JSON in mpus_in_progress/{uploadId}/upload.meta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadTracker {
    /// Unique identifier for the multipart upload (from S3)
    pub upload_id: String,
    /// Cache key for the object being uploaded (bucket/key)
    pub cache_key: String,
    /// When the multipart upload was initiated
    pub started_at: SystemTime,
    /// List of cached parts
    pub parts: Vec<CachedPartInfo>,
    /// Total size of all cached parts so far
    #[serde(default)]
    pub total_size: u64,
    /// Content-Type from CreateMultipartUpload request (optional)
    #[serde(default)]
    pub content_type: Option<String>,
}

impl MultipartUploadTracker {
    /// Create a new MultipartUploadTracker for a new multipart upload
    pub fn new(upload_id: String, cache_key: String) -> Self {
        Self {
            upload_id,
            cache_key,
            started_at: SystemTime::now(),
            parts: Vec::new(),
            total_size: 0,
            content_type: None,
        }
    }

    /// Create a new MultipartUploadTracker with content-type
    pub(crate) fn new_with_content_type(
        upload_id: String,
        cache_key: String,
        content_type: Option<String>,
    ) -> Self {
        Self {
            upload_id,
            cache_key,
            started_at: SystemTime::now(),
            parts: Vec::new(),
            total_size: 0,
            content_type,
        }
    }

    /// Add a part to the tracker
    pub(crate) fn add_part(&mut self, part_info: CachedPartInfo) {
        self.total_size += part_info.size;

        // Check if part already exists (re-upload of same part number)
        if let Some(existing) = self
            .parts
            .iter_mut()
            .find(|p| p.part_number == part_info.part_number)
        {
            // Subtract old size, add new size
            self.total_size -= existing.size;
            *existing = part_info;
        } else {
            self.parts.push(part_info);
        }
    }

    /// Get parts sorted by part number (required for CompleteMultipartUpload)
    pub(crate) fn get_sorted_parts(&self) -> Vec<&CachedPartInfo> {
        let mut sorted: Vec<&CachedPartInfo> = self.parts.iter().collect();
        sorted.sort_by_key(|p| p.part_number);
        sorted
    }

    /// Check if the upload has exceeded the given TTL
    pub fn is_expired(&self, ttl: std::time::Duration) -> bool {
        match self.started_at.elapsed() {
            Ok(elapsed) => elapsed > ttl,
            Err(_) => false, // Clock went backwards, don't expire
        }
    }

    /// Serialize to JSON for storage
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON
    pub(crate) fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Object-level metadata for S3 objects (new architecture)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObjectMetadata {
    pub etag: String,
    pub last_modified: String,
    pub content_length: u64,
    pub content_type: Option<String>,

    // NEW: Store all S3 response headers for complete response reconstruction
    #[serde(default)]
    pub response_headers: HashMap<String, String>,

    // NEW: Upload state tracking
    #[serde(default)]
    pub upload_state: UploadState,

    // NEW: Multipart upload tracking
    #[serde(default)]
    pub cumulative_size: u64,

    #[serde(default)]
    pub parts: Vec<PartInfo>,

    // NEW: Compression metadata (Requirement 7.2)
    #[serde(default)]
    pub compression_algorithm: CompressionAlgorithm,

    #[serde(default)]
    pub compressed_size: u64,

    // NEW: Multipart download metadata for part-number caching
    #[serde(default)]
    pub parts_count: Option<u32>,

    /// Maps part number to (start_offset, end_offset) byte range
    /// Example: {1: (0, 10485759), 2: (10485760, 15728639)}
    #[serde(default)]
    pub part_ranges: HashMap<u32, (u64, u64)>,

    // For multipart upload tracking
    #[serde(default)]
    pub upload_id: Option<String>,

    // Write cache tracking fields (Requirements 1.2, 1.3, 5.1)
    /// Whether this object was cached via PUT (write-through cache)
    #[serde(default)]
    pub is_write_cached: bool,

    /// When the write cache entry expires (refreshed on read access)
    #[serde(default)]
    pub write_cache_expires_at: Option<SystemTime>,

    /// When the write cache entry was created
    #[serde(default)]
    pub write_cache_created_at: Option<SystemTime>,

    /// When the write cache entry was last accessed
    #[serde(default)]
    pub write_cache_last_accessed: Option<SystemTime>,

    /// Whether this entry's graduation has already been charged against
    /// `SizeState::write_cache_size`.
    ///
    /// This is the **exactly-once token** for the graduation decrement (Requirement
    /// 1.2). The metadata transition itself is harmlessly idempotent — two proxies may
    /// both see `is_write_cached` set and both clear it — but a decrement is not, and
    /// the `.meta` read-modify-write in `refresh_write_cache_ttl` offers no protection
    /// on NFS.
    ///
    /// Written **only** by the Journal_Consolidator, while it holds the per-key metadata
    /// lock, as it applies a `Graduation` journal entry. Two `Graduation` entries for the
    /// same key are collapsed to one decrement: whichever is applied first sets this,
    /// and the rest are skipped. Because the consolidator is the only writer and it is
    /// serialised by the lock, this holds fleet-wide and across cycles.
    ///
    /// `#[serde(default)]` means every `.meta` written before this field existed reads
    /// as `false`. That is correct rather than merely convenient: no `Graduation` entry
    /// exists for an entry that graduated before this release, so nothing will debit it.
    /// Those already-lost bytes are re-grounded by the full Validation_Scan (R6), which
    /// is the only mechanism that can un-inflate a figure that is already inflated.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 1.1, 1.2, 1.3
    #[serde(default)]
    pub graduation_accounted: bool,
}

impl Default for ObjectMetadata {
    fn default() -> Self {
        Self {
            etag: String::new(),
            last_modified: String::new(),
            content_length: 0,
            content_type: None,
            response_headers: HashMap::new(),
            upload_state: UploadState::default(),
            cumulative_size: 0,
            parts: Vec::new(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 0,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        }
    }
}

impl ObjectMetadata {
    /// The `Last-Modified` value this entry actually has, from either source.
    ///
    /// Two places can hold this value and they disagree by construction today: the
    /// typed `last_modified` field, and a `last-modified` entry in `response_headers`
    /// (matched case-insensitively, since S3 and other callers write it in different
    /// cases). GET serve paths already check the map first and fall back to the
    /// field; the mainline HEAD hit replays only the map. This accessor is the single
    /// place that question is asked, so a predicate and the serve path it gates can
    /// never read different sources for the same entry.
    ///
    /// Returns `None` when both sources are absent or empty — never an empty string —
    /// so callers can use `.is_none()` directly as the "we have no validator" test.
    ///
    /// Spec: write-cache-last-modified. Requirements: 1.2
    pub fn effective_last_modified(&self) -> Option<&str> {
        if !self.last_modified.is_empty() {
            return Some(self.last_modified.as_str());
        }
        self.response_headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("last-modified"))
            .map(|(_, v)| v.as_str())
            .filter(|v| !v.is_empty())
    }

    /// Persist a learned `Last-Modified` value into both sources at once.
    ///
    /// The typed field and the header map must never be allowed to disagree, so this
    /// is the only writer either should go through. Any existing case-variant of the
    /// `last-modified` key is removed first, so a later read of `response_headers`
    /// cannot find a stale duplicate under a different case.
    ///
    /// No value is ever synthesised here or by any caller of this method — only a
    /// value S3 actually returned is acceptable (R3.4). This function does not enforce
    /// that; it is enforced by never calling it with anything else.
    ///
    /// Spec: write-cache-last-modified. Requirements: 3.1, 3.2
    pub(crate) fn set_last_modified(&mut self, last_modified: String) {
        self.last_modified.clone_from(&last_modified);
        self.response_headers
            .retain(|name, _| !name.eq_ignore_ascii_case("last-modified"));
        self.response_headers
            .insert("last-modified".to_string(), last_modified);
    }

    /// Create a new ObjectMetadata with default values for new fields
    pub fn new(
        etag: String,
        last_modified: String,
        content_length: u64,
        content_type: Option<String>,
    ) -> Self {
        Self {
            etag,
            last_modified,
            content_length,
            content_type,
            response_headers: HashMap::new(),
            upload_state: UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: content_length,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        }
    }

    /// Create a new ObjectMetadata with complete S3 response headers
    pub fn new_with_headers(
        etag: String,
        last_modified: String,
        content_length: u64,
        content_type: Option<String>,
        response_headers: HashMap<String, String>,
    ) -> Self {
        Self {
            etag,
            last_modified,
            content_length,
            content_type,
            response_headers,
            upload_state: UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: content_length,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        }
    }
    /// Check if write cache entry is expired
    pub(crate) fn is_write_cache_expired(&self) -> bool {
        if !self.is_write_cached {
            return false;
        }
        match self.write_cache_expires_at {
            Some(expires_at) => SystemTime::now() > expires_at,
            None => false, // No expiration set means not expired
        }
    }
}

/// Decide whether a **newly written** range counts toward the write (staging) tier.
///
/// A new range is staged when its object carries `is_write_cached` **or** its range
/// file sits under `mpus_in_progress/`. This is the *decision*, made once at the
/// moment a range is created, and its result is recorded in [`RangeSpec::staged`].
/// Reading an *existing* range's tier is a different operation — use
/// [`is_staged_range_spec`], which consults the recorded value and only falls back
/// to this function when there is none.
///
/// # Renamed from `is_staged_range` (2026-08-27, R12.4)
///
/// The old name and signature — `classify_new_range_as_staged(range_file_path, is_write_cached)` —
/// read as a per-range predicate and was not one. Its only per-range input is the
/// path arm below, which cannot fire for a freshly-built `RangeSpec` (see the next
/// section), so for all live data it reduced to a single object-level bool while
/// every credit and debit was applied per range. Every reader of it therefore
/// classified a flagged object's read-tier ranges as staged although nothing had
/// credited them, and graduation debited more than it credited. Requirements.md R12
/// has the full mechanism and both error directions.
///
/// The function survived the fix because deciding a *new* range's tier genuinely is
/// this expression; what was retired is its use as a *reader*. The name now says
/// which of the two it is, so a future reader cannot reach for it by accident, and
/// `is_staged_range_spec` calls it for the unrecorded case so the union rule still
/// has exactly one definition.
///
/// # Why the union, when the path arm looks dead
///
/// Traced 2026-08-25: every `RangeSpec` built by a live write path derives
/// `file_path` from `strip_prefix(cache_dir/ranges)`, which returns `Err` (and is
/// propagated) for anything outside `ranges/`. The four producers are
/// `DiskCacheManager::store_range`, `DiskCacheManager::finalize_incremental_range`,
/// `CacheManager::store_full_object_as_range_new`, and the multipart-completion
/// finalizer in `signed_put_handler` — and that last one *renames* each part out
/// of `mpus_in_progress/` into `ranges/` **before** building its `RangeSpec`.
/// Multipart parts are finalized by `finalize_incremental_part`, which
/// deliberately builds no `RangeSpec` at all while the part is still staged.
///
/// So the path arm cannot fire for a freshly-constructed range. It is retained
/// because a `RangeSpec` is also **deserialised from a persisted `.meta`**, which
/// carries whatever string was written to disk by whatever version wrote it. That
/// is the reachable case, and it is the one the path-only unit test covers — and
/// since R12 that case is reached exclusively through the `None` fallback of
/// [`is_staged_range_spec`], because any range this function classifies is recorded
/// immediately and never re-derived.
///
/// This corrects the spec's original framing, which described the predicate
/// disagreement as an active "opposite-direction leak" driving `write_cache_size`
/// toward undershoot. On the current tree it is not reachable from a live write —
/// unifying the predicate is consistency hardening plus legacy-data tolerance, not
/// the repair of a running leak. The repair of the running leak is R5/R6.
///
/// Spec: write-cache-accounting-and-eviction. Requirements: 6.2, 12.2
pub fn classify_new_range_as_staged(range_file_path: &str, is_write_cached: bool) -> bool {
    is_write_cached || range_file_path.contains("mpus_in_progress/")
}

/// The single definition of "this **existing** range counts toward the write
/// (staging) tier", per range rather than per object.
///
/// Takes the recorded membership rather than a `&RangeSpec` so that the handful of
/// callers holding the parts separately — read-tier eviction's Step 5 carries a
/// [`crate::cache::RangeEvictionCandidate`], not a `RangeSpec` — read the tier
/// through the same rule rather than reimplementing the fallback. An inline
/// `match staged { Some(s) => s, None => … }` at a call site is a second definition
/// of that rule, which is what R12.4 forbids.
///
/// # The fallback is the migration
///
/// `staged == None` means the range predates the field, so the answer comes from
/// [`classify_new_range_as_staged`] — reproducing exactly what the pre-R12 code
/// returned for the same inputs. That is why the field is `Option<bool>`: see
/// [`RangeSpec::staged`] for why a bare `bool` would drive `write_cache_size` to near
/// zero fleet-wide on the first post-upgrade scan.
///
/// Recorded membership is **never** second-guessed: a `Some(false)` range under an
/// `mpus_in_progress/` path stays unstaged, and a `Some(false)` range on a flagged
/// object stays unstaged. That last case is the whole defect, so OR-ing the object
/// flag in here would leave the fix inert while passing every migration test.
///
/// Spec: write-cache-accounting-and-eviction. Requirements: 12.3
/// `pub(crate)` rather than `pub`, deliberately: its only caller outside this module is
/// read-tier eviction's Step 5, and `pub` would hide it from the `dead_code` lint (see
/// `pre-push-checklist.md` step 3). Prefer [`is_staged_range_spec`], which is the form
/// `tests/` uses.
pub(crate) fn is_staged_range_parts(
    staged: Option<bool>,
    range_file_path: &str,
    object_is_write_cached: bool,
) -> bool {
    match staged {
        Some(staged) => staged,
        None => classify_new_range_as_staged(range_file_path, object_is_write_cached),
    }
}

/// [`is_staged_range_parts`] for a caller holding the range itself, which is most of
/// them.
///
/// Preferred over the parts form wherever a `&RangeSpec` is available: a caller
/// holding the range cannot pair it with a different object's flag by accident, which
/// the retired path-taking signature allowed.
///
/// Spec: write-cache-accounting-and-eviction. Requirements: 12.3
pub fn is_staged_range_spec(range: &RangeSpec, object_is_write_cached: bool) -> bool {
    is_staged_range_parts(range.staged, &range.file_path, object_is_write_cached)
}

/// Range specification without data (metadata only)
/// Points to a separate binary file containing the actual range data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RangeSpec {
    pub start: u64,
    pub end: u64,
    pub file_path: String, // Relative path to .bin file
    pub compression_algorithm: CompressionAlgorithm,
    pub compressed_size: u64,
    pub uncompressed_size: u64,

    // Per-range access tracking
    pub created_at: SystemTime,
    pub last_accessed: SystemTime,
    pub access_count: u64,

    /// Whether this range counts toward the write (staging) tier, recorded at the
    /// moment it was credited. Read through
    /// [`is_staged_range_spec`], never directly.
    ///
    /// `Option`, not `bool`, and the reason is the migration rather than
    /// expressiveness. A bare `#[serde(default)] bool` would read `false` for every
    /// range in every `.meta` written by an earlier release, so the first
    /// post-upgrade Validation_Scan would compute `write_cache_size` ≈ 0 fleet-wide
    /// and **persist it** — a silent, confident, wrong re-grounding indistinguishable
    /// from a healthily drained tier, and the same failure class as the outage this
    /// spec was opened for. `None` means "this range predates the field" and falls
    /// back to the object flag, which reproduces today's answer exactly.
    ///
    /// # The `Option` is the guard, not the `#[serde(default)]`
    ///
    /// Measured 2026-08-27 while showing design test 16 failing first: **removing
    /// `#[serde(default)]` from this field changes nothing.** Serde treats a missing
    /// `Option<T>` field as `None` implicitly, so all fourteen tests still passed.
    /// The attribute is kept because it is self-documenting and because the spec
    /// specifies it, but it must not be mistaken for the thing that makes the
    /// migration safe.
    ///
    /// What *is* load-bearing is the `Option` itself. Making the serde default yield
    /// `Some(false)` — which is precisely what a bare `bool` does at the
    /// deserialisation boundary — reds design test 16 at `left: Some(false)`. So a
    /// future change that "simplifies" this to `#[serde(default)] bool`, keeping the
    /// attribute and looking safer for it, is the dangerous one.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 12.1
    #[serde(default)]
    pub staged: Option<bool>,

    /// The extent is listed only so its `.bin` stays named and evictable; its bytes may
    /// belong to another object version and must never be served.
    ///
    /// Set by `retire_other_version_fills` when an other-version fill's `.bin` cannot be
    /// unlinked. Every range lookup ([`crate::disk_cache::DiskCacheManager::find_cached_ranges`])
    /// skips such an extent, so it reads as missing and the request refetches. A later
    /// consolidation that unlinks the file, an eviction of the extent, or a current-version
    /// fill that republishes the extent removes or replaces the listing.
    ///
    /// A field rather than a sentinel in an existing field: every existing field (`file_path`,
    /// sizes, timestamps, `access_count`) is read by eviction, accounting or LRU ordering, which
    /// must keep working on this extent. A `bool` default of `false` is correct for every
    /// `.meta` written before the field existed, since no earlier release marked an extent.
    /// Omitted from the document when `false`, so ordinary `.meta` bytes are unchanged.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unservable: bool,

    /// Identity of the `.bin` its writer published ([`RangeFileIdentity`]): the file's length
    /// and modification time, taken by `fstat` of a descriptor the writer opens after it has
    /// synced and closed the one it wrote through. The listing's ETag says which version the
    /// range is; this says which file holds it. A read serves the range only when `fstat` of
    /// the descriptor it reads through reports a matching identity
    /// ([`RangeFileIdentity::matches`]), so another version's file renamed over the same path
    /// (the `.bin` path is version-independent) is a miss, not a wrong-version serve.
    ///
    /// Carried unchanged through the journal `Add` (whose `range_spec` is this struct), the
    /// multipart part record ([`CachedPartInfo::file_identity`]) and every consolidation that
    /// clones a listing. A listing rebuilt without it never invents one.
    ///
    /// `None` for every listing written before 3.0.0, for a writer whose `fstat` failed, and
    /// for carriers that name an extent without describing a file (`Remove`, `Graduation`,
    /// cache-hit updates). A `None` listing that starts at offset 0 and stops short of the object is
    /// refetched once on its first read (see `is_unverified_prefix`); every other `None` listing is
    /// served unverified, as every listing was before.
    ///
    /// Additive: omitted from the document when `None`, and no struct that parses a `.meta`
    /// or a journal line rejects unknown fields, so a pre-3.0 proxy parses a listing carrying
    /// it. That proxy drops it when it rewrites the `.meta` (it has no field to keep it in),
    /// and the listing is then unverified. A `content_hash` written by an unreleased 3.0
    /// development build is an unknown field here and is ignored the same way, and so is an
    /// identity that build recorded as inode number and length: it records no modification
    /// time, so it parses to `None` ([`lenient_file_identity`]).
    #[serde(
        default,
        deserialize_with = "lenient_file_identity",
        skip_serializing_if = "Option::is_none"
    )]
    pub file_identity: Option<RangeFileIdentity>,
}

/// Which file a range listing describes: length and modification time of the published `.bin`
/// ([`RangeSpec::file_identity`]).
///
/// - `size` is the file's length after the writer's sync, which every client sees once it
///   opens the file (close-to-open). Two versions of one extent can share a length (store-mode
///   frames of equal-length data), so it is never enough alone.
/// - `mtime_ns` is the file's modification time, in nanoseconds since the Unix epoch. The
///   rename that publishes the file keeps it, and so does a copy of the cache made with a tool
///   that preserves modification times (`cp -a`, `rsync -a`), so a copied cache stays valid.
///   A file renamed over the path was written at another time.
/// - When the writer takes it: from a descriptor opened after the writing descriptor is
///   closed ([`crate::disk_cache::identity_after_close`]). An NFS server stamps the
///   modification time on each WRITE it applies. Closing flushes and releases the writer's
///   open, and the next open revalidates the attributes with the server, so the writer
///   records the server's value, which is what a reader's open on any client sees. An
///   `fstat` of the still-open writer descriptor would report the client's cached
///   attributes, current only if the bookkeeping of every WRITE reply was.
/// - Not the inode number: a copy or restore of the volume gives every file a new one, so
///   every range of a copied cache would miss once, and a file system can hand a freed number
///   to the next file it creates.
/// - Not `st_dev`: an NFS client's device number is local to that client.
///
/// [`Self::matches`] compares `size` exactly and `mtime_ns` within [`MTIME_MATCH_WINDOW_NS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeFileIdentity {
    /// `st_size` of the published file, in bytes on disk (the encoded frames).
    pub size: u64,
    /// `st_mtime` of the published file, in nanoseconds since the Unix epoch.
    pub mtime_ns: i64,
}

/// How far apart a recorded and an observed modification time may be and still describe one
/// file: 1 ms. Amazon EFS stores modification times to the millisecond, so a cache copied with
/// `cp -a` from a finer file system (ext4, OpenZFS) onto EFS keeps each time only to the
/// millisecond; exact equality would make about 999 of 1000 of its ranges miss once. The
/// window costs no discrimination: on EFS 1 ms is the stored granule, and ext4 and OpenZFS
/// stamp from the kernel clock tick (about 4 to 10 ms), which is coarser, so two fills of one
/// extent fall inside it only if they share a tick and a compressed length. A copy tool that
/// truncates to whole seconds (default `tar`, older `rsync`) leaves most times outside it:
/// those ranges miss, are healed and refill, never served from the wrong file. Not a
/// configuration field.
pub const MTIME_MATCH_WINDOW_NS: u64 = 1_000_000;

impl RangeFileIdentity {
    /// The identity `fstat` (or `stat`) reported. `None` off Unix, or when the modification
    /// time does not fit `i64` nanoseconds: the range is then recorded unverified.
    pub fn of(meta: &std::fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let mtime_ns = meta
                .mtime()
                .checked_mul(1_000_000_000)?
                .checked_add(meta.mtime_nsec())?;
            Some(Self {
                size: meta.len(),
                mtime_ns,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = meta;
            None
        }
    }

    /// Whether `found` (what `fstat` of a read's descriptor reports) is the file this recorded
    /// identity describes: the same length, and modification times less than
    /// [`MTIME_MATCH_WINDOW_NS`] apart.
    pub fn matches(&self, found: &Self) -> bool {
        self.size == found.size && self.mtime_ns.abs_diff(found.mtime_ns) < MTIME_MATCH_WINDOW_NS
    }
}

/// Read a recorded [`RangeFileIdentity`] without ever failing the document that carries it. A
/// value that is not a complete identity reads as `None`, so its range is unverified rather
/// than its `.meta`, journal line or part record unreadable. That covers the inode-and-length
/// identity an earlier 3.0 development build recorded (`{"ino":..,"size":..}`, no `mtime_ns`),
/// whose inode number is ignored.
fn lenient_file_identity<'de, D>(deserializer: D) -> Result<Option<RangeFileIdentity>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Recorded {
        size: Option<u64>,
        mtime_ns: Option<i64>,
    }
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Recorded(Recorded),
        Other(serde::de::IgnoredAny),
    }
    Ok(match Option::<Value>::deserialize(deserializer)? {
        Some(Value::Recorded(Recorded {
            size: Some(size),
            mtime_ns: Some(mtime_ns),
        })) => Some(RangeFileIdentity { size, mtime_ns }),
        _ => None,
    })
}

impl RangeSpec {
    /// Create a new RangeSpec with initial access tracking.
    ///
    /// Leaves `staged` as `None`. Every **live write path** must record membership
    /// explicitly (R12.2) — use [`RangeSpec::new_staged`] there rather than this.
    /// This constructor remains for the paths that reconstruct a range without
    /// knowing its tier (recovery sweeps, tests, and read-path plumbing).
    pub fn new(
        start: u64,
        end: u64,
        file_path: String,
        compression_algorithm: CompressionAlgorithm,
        compressed_size: u64,
        uncompressed_size: u64,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            start,
            end,
            file_path,
            compression_algorithm,
            compressed_size,
            uncompressed_size,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        }
    }
    /// Record the identity of the file this listing describes ([`Self::file_identity`]).
    /// Writers call it with the identity of the file they published; `None` leaves the
    /// listing unverified.
    pub fn with_file_identity(mut self, file_identity: Option<RangeFileIdentity>) -> Self {
        self.file_identity = file_identity;
        self
    }

    /// Is this a listing the read paths must not serve, because a release before 3.0.0 may
    /// have stored another offset's bytes under it? `content_length` is the object's length
    /// as the listing's document (or journal `Add`) records it.
    ///
    /// True for an extent that records no file identity, starts at offset 0, and does not
    /// reach the end of the object (or whose document records no length at all).
    ///
    /// # Why this shape
    ///
    /// Before 3.0.0 an open-ended `Range: bytes=N-` or a suffix `Range: bytes=-N` on a key
    /// with no cached length was forwarded with the client's header, and the `206` body was
    /// cached as the extent `0..=(len - 1)`: the bytes at offset `N` named as the object's
    /// first bytes, under the object's true ETag and length. Every such extent starts at 0
    /// and stops short of the object's end, and every one was written by a release that
    /// recorded no file identity. Every 3.0.0 writer that publishes a range file records
    /// one ([`Self::file_identity`]), so a 3.0.0 listing never matches.
    ///
    /// The stored response headers cannot tell a shifted extent from a legitimate first
    /// chunk: a HEAD or a part GET rewrites them, a refill does not refresh them, and a
    /// document's headers can come from a different fill than the extent. So the rule
    /// trusts no pre-3.0.0 extent of this shape. A legitimate one (a transfer manager's first
    /// chunk, a player's opening range, a `bytes=0-99` probe) is fetched from S3 once and
    /// listed again with an identity, after which the rule never matches it.
    ///
    /// Not matched, and served as before: an extent with an identity, a whole-object extent
    /// (`end + 1 == content_length`), and an extent that starts above 0.
    ///
    /// # Where it is applied
    ///
    /// `DiskCacheManager::find_cached_ranges` skips a matching `.meta` listing and removes it
    /// (with its `.bin` and its accounting) under the key's slot lock, so the refill is
    /// listed in its place; the pending-journal scan behind every journal fallback skips a
    /// matching `Add`; and the streaming serves' re-read of the `.meta` treats a matching
    /// listing as unservable.
    ///
    /// Off Unix no writer can record an identity ([`RangeFileIdentity::of`]), so the rule
    /// would refuse every first chunk forever; it is compiled to `false` there.
    ///
    /// Compatibility code with an expiry: it can be deleted once no listing written before
    /// 3.0.0 can remain on a cache volume, one release after every fleet has upgraded.
    pub fn is_unverified_prefix(&self, content_length: u64) -> bool {
        #[cfg(unix)]
        {
            self.file_identity.is_none()
                && self.start == 0
                && (content_length == 0 || self.end.saturating_add(1) < content_length)
        }
        #[cfg(not(unix))]
        {
            let _ = content_length;
            false
        }
    }

    /// Create a new RangeSpec with staging membership recorded explicitly.
    ///
    /// Every live write path uses this, so a range's tier is recorded once at the
    /// only moment it is known for certain and is never re-derived from an object
    /// flag that may since have gained ranges from a different tier.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 12.2
    pub fn new_staged(
        start: u64,
        end: u64,
        file_path: String,
        compression_algorithm: CompressionAlgorithm,
        compressed_size: u64,
        uncompressed_size: u64,
        staged: bool,
    ) -> Self {
        Self {
            staged: Some(staged),
            ..Self::new(
                start,
                end,
                file_path,
                compression_algorithm,
                compressed_size,
                uncompressed_size,
            )
        }
    }

    /// Calculate LRU score (lower = evict first)
    /// Returns seconds since Unix epoch for last access time
    pub fn lru_score(&self) -> u64 {
        self.last_accessed
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Decayed-frequency eviction score (lower = evict first). Testable via injected `now`.
    ///
    /// Halves `access_count` once per `TINYLFU_HALF_LIFE_SECS` of idle time (see
    /// `crate::cache::decayed_frequency`). Monotonic non-increasing in idle time and never
    /// divides by recency, which avoids the inversion where an idle-hot range is evicted
    /// before a fresh one-hit-wonder.
    pub fn tinylfu_score(&self, now: SystemTime) -> u64 {
        let idle_secs = now
            .duration_since(self.last_accessed)
            .unwrap_or_default()
            .as_secs();
        crate::cache::decayed_frequency(self.access_count, idle_secs)
    }

    /// Update access statistics
    pub fn record_access(&mut self) {
        self.last_accessed = SystemTime::now();
        self.access_count += 1;
    }
}

/// Lightweight metadata file for new range storage architecture
/// Stored as JSON in metadata/{key}.meta
/// Used for both HEAD and GET caching with independent TTLs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewCacheMetadata {
    pub cache_key: String,
    pub object_metadata: ObjectMetadata,
    pub ranges: Vec<RangeSpec>,
    pub created_at: SystemTime,
    // Object-level expires_at — the authoritative TTL for this cached object
    pub expires_at: SystemTime,
    pub compression_info: CompressionInfo,

    // HEAD-specific fields for unified HEAD/GET caching
    // HEAD has independent TTL from ranges (typically shorter)
    #[serde(default)]
    pub head_expires_at: Option<SystemTime>,
    #[serde(default)]
    pub head_last_accessed: Option<SystemTime>,
    #[serde(default)]
    pub head_access_count: u64,
    /// When the HEAD entry was last refreshed from an S3 response. Legacy
    /// metadata has no anchor and falls back to `created_at` for freshness.
    #[serde(default)]
    pub head_cached_at: Option<SystemTime>,
    /// When S3 last confirmed this representation with a `304`. The live GET age is
    /// measured from the later of this and `created_at` ([`Self::freshness_anchor`]), so a
    /// revalidated entry is fresh for another `get_ttl` instead of revalidating on every
    /// read. `created_at` is left alone because eviction, upload timeouts and write-ledger
    /// ordering read it. `None` for an entry never revalidated, and for metadata written
    /// before the field existed; omitted from the `.meta` when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_validated_at: Option<SystemTime>,
}

impl NewCacheMetadata {
    /// The instant the live GET age is measured from: the later of `created_at` and
    /// `last_validated_at`.
    pub fn freshness_anchor(&self) -> SystemTime {
        match self.last_validated_at {
            Some(validated) if validated > self.created_at => validated,
            _ => self.created_at,
        }
    }

    /// Record that S3 confirmed this representation with a `304`: stored expiry moves to
    /// `now + ttl` and the live GET age re-anchors to now. Only an origin `304` may call this.
    /// A Mode B `If-Match` served from cache compares the client's ETag against the cache's
    /// own and must use [`Self::refresh_object_ttl`] instead.
    pub fn record_origin_validation(&mut self, ttl: Duration) {
        let now = SystemTime::now();
        self.expires_at = safe_expiry(now, ttl);
        self.last_validated_at = Some(now);
    }

    /// Check if HEAD cache has expired
    /// Returns true if head_expires_at is None or in the past
    pub fn is_head_expired(&self) -> bool {
        match self.head_expires_at {
            Some(expires_at) => SystemTime::now() > expires_at,
            None => true, // No HEAD expiry set means HEAD is not cached
        }
    }

    /// Refresh HEAD TTL and freshness anchor from the same instant.
    pub fn refresh_head_ttl(&mut self, ttl: std::time::Duration) {
        let now = SystemTime::now();
        self.head_expires_at = Some(now + ttl);
        self.head_last_accessed = Some(now);
        self.head_cached_at = Some(now);
    }

    /// Record a HEAD access (increment count and update last_accessed)
    pub fn record_head_access(&mut self) {
        self.head_access_count += 1;
        self.head_last_accessed = Some(SystemTime::now());
    }

    /// Check if the cached object has expired.
    /// Checks object-level expires_at against now.
    pub fn is_object_expired(&self) -> bool {
        SystemTime::now() > self.expires_at
    }

    /// Sum of `compressed_size` over the ranges that count toward the write
    /// (staging) tier, classified through [`is_staged_range_spec`].
    ///
    /// This is the **per-range** figure, which is what any scan feeding
    /// `SizeState::write_cache_size` must report, because the accumulator credits
    /// and debits per range. Classifying at object granularity instead (all ranges
    /// or none, from the object's flag alone) is what let the coordinated scan and
    /// the accumulator report different figures for the same on-disk state.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 6.1, 6.2
    pub fn staged_compressed_size(&self) -> u64 {
        self.ranges
            .iter()
            .filter(|r| is_staged_range_spec(r, self.object_metadata.is_write_cached))
            .map(|r| r.compressed_size)
            .sum()
    }

    /// Whether any range of this object counts toward the write (staging) tier.
    /// Used for **object counts** where `staged_compressed_size` gives bytes.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 6.1, 6.2
    pub fn has_staged_range(&self) -> bool {
        self.ranges
            .iter()
            .any(|r| is_staged_range_spec(r, self.object_metadata.is_write_cached))
    }

    /// Refresh the object-level TTL by setting `expires_at = now + ttl`.
    pub fn refresh_object_ttl(&mut self, ttl: Duration) {
        self.expires_at = safe_expiry(SystemTime::now(), ttl);
    }
}

impl Default for NewCacheMetadata {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            cache_key: String::new(),
            object_metadata: ObjectMetadata::default(),
            ranges: Vec::new(),
            created_at: now,
            expires_at: now,
            compression_info: CompressionInfo::default(),
            head_expires_at: None,
            head_last_accessed: None,
            head_access_count: 0,
            head_cached_at: None,
            last_validated_at: None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;
    use serde_json::json;

    #[test]
    fn test_upload_state_serialization() {
        // Test Complete state
        let complete = UploadState::Complete;
        let json = serde_json::to_string(&complete).unwrap();
        let deserialized: UploadState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, UploadState::Complete);

        // Test InProgress state
        let in_progress = UploadState::InProgress;
        let json = serde_json::to_string(&in_progress).unwrap();
        let deserialized: UploadState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, UploadState::InProgress);

        // Test Bypassed state
        let bypassed = UploadState::Bypassed;
        let json = serde_json::to_string(&bypassed).unwrap();
        let deserialized: UploadState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, UploadState::Bypassed);

        // Test default
        assert_eq!(UploadState::default(), UploadState::Complete);
    }

    #[test]
    fn test_part_info_serialization() {
        let part_info = PartInfo {
            part_number: 1,
            size: 5242880, // 5MB
            etag: "part-etag-1".to_string(),
            data: vec![0u8; 100], // Small test data
        };

        // Serialize
        let json = serde_json::to_string(&part_info).unwrap();

        // Deserialize
        let deserialized: PartInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.part_number, 1);
        assert_eq!(deserialized.size, 5242880);
        assert_eq!(deserialized.etag, "part-etag-1");
        assert_eq!(deserialized.data.len(), 100);
    }

    #[test]
    fn test_object_metadata_with_multipart_fields() {
        // Test metadata with in-progress multipart upload
        let part1 = PartInfo {
            part_number: 1,
            size: 5242880,
            etag: "part-etag-1".to_string(),
            data: vec![0u8; 5242880],
        };

        let part2 = PartInfo {
            part_number: 2,
            size: 5242880,
            etag: "part-etag-2".to_string(),
            data: vec![0u8; 5242880],
        };

        let metadata = ObjectMetadata {
            etag: "".to_string(), // Empty for in-progress uploads
            last_modified: "".to_string(),
            content_length: 0,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::InProgress,
            cumulative_size: 10485760, // 10MB
            parts: vec![part1, part2],
            ..Default::default()
        };

        // Serialize
        let json = serde_json::to_string(&metadata).unwrap();

        // Deserialize
        let deserialized: ObjectMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.upload_state, UploadState::InProgress);
        assert_eq!(deserialized.cumulative_size, 10485760);
        assert_eq!(deserialized.parts.len(), 2);
        assert_eq!(deserialized.parts[0].part_number, 1);
        assert_eq!(deserialized.parts[1].part_number, 2);
    }

    #[test]
    fn test_object_metadata_serialization() {
        let metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Serialize
        let json = serde_json::to_string(&metadata).unwrap();

        // Deserialize
        let deserialized: ObjectMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.etag, metadata.etag);
        assert_eq!(deserialized.last_modified, metadata.last_modified);
        assert_eq!(deserialized.content_length, metadata.content_length);
        assert_eq!(deserialized.content_type, metadata.content_type);
        assert_eq!(deserialized.upload_state, UploadState::Complete);
        assert_eq!(deserialized.cumulative_size, 1024);
        assert_eq!(deserialized.parts.len(), 0);
    }

    #[test]
    fn test_range_spec_serialization() {
        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: 0,
            end: 8388607,
            file_path: "test_bucket_object_0-8388607.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 2048,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        // Serialize
        let json = serde_json::to_string(&range_spec).unwrap();

        // Deserialize
        let deserialized: RangeSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.start, range_spec.start);
        assert_eq!(deserialized.end, range_spec.end);
        assert_eq!(deserialized.file_path, range_spec.file_path);
        assert_eq!(
            deserialized.compression_algorithm,
            range_spec.compression_algorithm
        );
        assert_eq!(deserialized.compressed_size, range_spec.compressed_size);
        assert_eq!(deserialized.uncompressed_size, range_spec.uncompressed_size);
        assert_eq!(deserialized.access_count, range_spec.access_count);
    }

    #[test]
    fn test_new_cache_metadata_serialization() {
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10485760, // 10MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: 10485760,
            parts: Vec::new(),
            ..Default::default()
        };

        let now = SystemTime::now();
        let range_spec = RangeSpec::new(
            0,
            8388607,
            "test_bucket_object_0-8388607.bin".to_string(),
            CompressionAlgorithm::Lz4,
            1024000,
            8388608,
        );

        let cache_metadata = NewCacheMetadata {
            cache_key: "test-bucket:test-object".to_string(),
            object_metadata,
            ranges: vec![range_spec],
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Serialize
        let json = serde_json::to_string_pretty(&cache_metadata).unwrap();

        // Verify JSON doesn't contain binary data (no "data" field)
        assert!(!json.contains("\"data\""));

        // Deserialize
        let deserialized: NewCacheMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cache_key, cache_metadata.cache_key);
        assert_eq!(
            deserialized.object_metadata.etag,
            cache_metadata.object_metadata.etag
        );
        assert_eq!(deserialized.ranges.len(), 1);
        assert_eq!(deserialized.ranges[0].start, 0);
        assert_eq!(deserialized.ranges[0].end, 8388607);
    }

    #[test]
    fn test_range_spec_size_estimate() {
        // Test that RangeSpec JSON size is approximately 150-200 bytes as per design
        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: 1234567890,
            end: 9876543210,
            file_path: "test_bucket_with_long_name_object_with_long_name_1234567890-9876543210.bin"
                .to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1234567890,
            uncompressed_size: 9876543210,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        let json = serde_json::to_string(&range_spec).unwrap();
        let size = json.len();

        // With added TTL fields, should be roughly 250-400 bytes (allowing margin for timestamps)
        assert!(
            size < 500,
            "RangeSpec JSON size {} exceeds expected maximum of 500 bytes",
            size
        );
        println!("RangeSpec JSON size: {} bytes", size);
    }

    #[test]
    fn test_new_cache_metadata_with_multiple_ranges() {
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024 * 1024, // 100MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: 100 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        let now = SystemTime::now();
        // Create 100 range specs using RangeSpec::new
        let mut ranges = Vec::new();
        for i in 0..100 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            ranges.push(RangeSpec::new(
                start,
                end,
                format!("test_bucket_object_{}-{}.bin", start, end),
                CompressionAlgorithm::Lz4,
                512 * 1024,
                1024 * 1024,
            ));
        }

        let cache_metadata = NewCacheMetadata {
            cache_key: "test-bucket:test-object".to_string(),
            object_metadata,
            ranges,
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Serialize
        let json = serde_json::to_string_pretty(&cache_metadata).unwrap();
        let size = json.len();

        // With 100 ranges, should be under 100KB
        assert!(
            size < 100 * 1024,
            "Metadata with 100 ranges is {} bytes, exceeds 100KB limit",
            size
        );
        println!(
            "Metadata with 100 ranges: {} bytes ({:.2} KB)",
            size,
            size as f64 / 1024.0
        );

        // Verify no binary data in JSON
        assert!(!json.contains("\"data\""));
    }

    #[test]
    fn test_range_spec_new() {
        let range = RangeSpec::new(
            0,
            1024,
            "test.bin".to_string(),
            CompressionAlgorithm::Lz4,
            1024,
            1024,
        );

        assert_eq!(range.start, 0);
        assert_eq!(range.end, 1024);
        assert_eq!(range.file_path, "test.bin");
        assert_eq!(range.compression_algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(range.compressed_size, 1024);
        assert_eq!(range.uncompressed_size, 1024);
        assert_eq!(range.access_count, 1);
        // Verify no expires_at field exists on RangeSpec (removed in object-level expiry refactor)
        // The struct no longer has this field — compilation proves it.
    }

    #[test]
    fn test_is_object_expired() {
        let now = SystemTime::now();

        // expires_at in the future → not expired
        let fresh = NewCacheMetadata {
            cache_key: "k".to_string(),
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            ..Default::default()
        };
        assert!(!fresh.is_object_expired());

        // expires_at in the past → expired
        let expired = NewCacheMetadata {
            cache_key: "k".to_string(),
            created_at: now - Duration::from_secs(7200),
            expires_at: now - Duration::from_secs(1),
            ..Default::default()
        };
        assert!(expired.is_object_expired());
    }

    #[test]
    fn test_refresh_object_ttl() {
        let now = SystemTime::now();
        let mut meta = NewCacheMetadata {
            cache_key: "k".to_string(),
            created_at: now - Duration::from_secs(7200),
            expires_at: now - Duration::from_secs(1), // expired
            ..Default::default()
        };
        assert!(meta.is_object_expired());

        meta.refresh_object_ttl(Duration::from_secs(3600));

        // expires_at should be approximately now + 3600s (within 2s tolerance)
        let expected = SystemTime::now() + Duration::from_secs(3600);
        let diff = if meta.expires_at > expected {
            meta.expires_at.duration_since(expected).unwrap()
        } else {
            expected.duration_since(meta.expires_at).unwrap()
        };
        assert!(
            diff < Duration::from_secs(2),
            "expires_at not within tolerance"
        );
        assert!(!meta.is_object_expired());
    }

    #[test]
    fn test_range_spec_lru_score() {
        let now = SystemTime::now();
        let range = RangeSpec {
            start: 0,
            end: 1024,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        let score = range.lru_score();
        // Score should be seconds since Unix epoch
        assert!(score > 0);

        // More recently accessed range should have higher score
        let older_range = RangeSpec {
            start: 0,
            end: 1024,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now - std::time::Duration::from_secs(3600),
            last_accessed: now - std::time::Duration::from_secs(3600),
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        assert!(range.lru_score() > older_range.lru_score());
    }

    #[test]
    fn test_range_spec_tinylfu_score() {
        let now = SystemTime::now();

        // Frequently accessed, recently accessed range
        let hot_range = RangeSpec {
            start: 0,
            end: 1024,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now - std::time::Duration::from_secs(3600),
            last_accessed: now - std::time::Duration::from_secs(10),
            access_count: 100,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        // Infrequently accessed, old range
        let cold_range = RangeSpec {
            start: 0,
            end: 1024,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now - std::time::Duration::from_secs(7200),
            last_accessed: now - std::time::Duration::from_secs(3600),
            access_count: 5,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        // Hot range should have higher score (less likely to be evicted)
        assert!(hot_range.tinylfu_score(now) > cold_range.tinylfu_score(now));
    }

    #[test]
    fn test_range_spec_record_access() {
        let now = SystemTime::now();
        let mut range = RangeSpec {
            start: 0,
            end: 1024,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 1024,
            created_at: now - std::time::Duration::from_secs(3600),
            last_accessed: now - std::time::Duration::from_secs(1800),
            access_count: 5,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        let old_last_accessed = range.last_accessed;
        let old_access_count = range.access_count;

        // Record access
        range.record_access();

        // last_accessed should be updated
        assert!(range.last_accessed > old_last_accessed);

        // access_count should be incremented
        assert_eq!(range.access_count, old_access_count + 1);
    }

    /// `.meta` compatibility: a JSON `RangeSpec` containing the now-removed
    /// `frequency_score` field (as an old-format `.meta` would have on disk) still
    /// deserializes successfully, with serde silently ignoring the unknown field.
    /// **Validates: Requirements 5.2, 6.5**
    #[test]
    fn test_range_spec_meta_compat_ignores_removed_frequency_score() {
        let range = RangeSpec::new(
            0,
            8388607,
            "test_bucket_object_0-8388607.bin".to_string(),
            CompressionAlgorithm::Lz4,
            1024,
            2048,
        );

        // Serialize the current (field-less) RangeSpec to a JSON value, then splice in a
        // `frequency_score` key to simulate what an old `.meta` file on disk would contain.
        let mut value = serde_json::to_value(&range).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("frequency_score".to_string(), json!(42));
        let json_with_frequency_score = serde_json::to_string(&value).unwrap();
        assert!(json_with_frequency_score.contains("frequency_score"));

        // Deserializing must succeed even though `RangeSpec` no longer has this field.
        let deserialized: RangeSpec = serde_json::from_str(&json_with_frequency_score).unwrap();

        assert_eq!(deserialized.start, range.start);
        assert_eq!(deserialized.end, range.end);
        assert_eq!(deserialized.file_path, range.file_path);
        assert_eq!(deserialized.access_count, range.access_count);
        assert_eq!(deserialized.created_at, range.created_at);
        assert_eq!(deserialized.last_accessed, range.last_accessed);
    }

    // ===== PROPERTY TESTS FOR PART-NUMBER CACHING =====

    /// **Feature: part-number-caching, Property 17: Backward compatibility with old metadata**
    /// For any ObjectMetadata created without multipart fields, deserializing should succeed
    /// with default values (None/empty) for parts_count, part_ranges, and upload_id.
    /// **Validates: Requirements 9.3, 9.4**
    #[quickcheck]
    fn prop_object_metadata_backward_compatibility(
        etag: String,
        last_modified: String,
        content_length: u64,
    ) -> TestResult {
        // Filter out invalid inputs - only allow safe ASCII characters
        if etag.is_empty()
            || last_modified.is_empty()
            || !etag
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
            || !last_modified.chars().all(|c| {
                c.is_ascii_alphanumeric()
                    || c == '-'
                    || c == '_'
                    || c == '.'
                    || c == ' '
                    || c == ':'
            })
        {
            return TestResult::discard();
        }

        // Create old-format JSON without multipart fields
        let old_format_json = format!(
            r#"{{
                "etag": "{}",
                "last_modified": "{}",
                "content_length": {},
                "content_type": null,
                "response_headers": {{}},
                "upload_state": "Complete",
                "cumulative_size": {},
                "parts": [],
                "compression_algorithm": "None",
                "compressed_size": {}
            }}"#,
            etag, last_modified, content_length, content_length, content_length
        );

        // Deserialize should succeed
        let deserialized: Result<ObjectMetadata, _> = serde_json::from_str(&old_format_json);

        match deserialized {
            Ok(metadata) => {
                // Verify multipart fields have default values
                TestResult::from_bool(
                    metadata.etag == etag
                        && metadata.last_modified == last_modified
                        && metadata.content_length == content_length
                        && metadata.parts_count.is_none()
                        && metadata.part_ranges.is_empty()
                        && metadata.upload_id.is_none(),
                )
            }
            Err(_) => TestResult::failed(),
        }
    }

    /// **Feature: part-number-caching, Property 13: Metadata persistence round-trip**
    /// For any ObjectMetadata with multipart fields, serializing then deserializing
    /// should yield an equivalent ObjectMetadata with the same values.
    /// **Validates: Requirements 6.3, 6.5**
    #[quickcheck]
    fn prop_metadata_persistence_round_trip(
        etag: String,
        last_modified: String,
        content_length: u64,
        parts_count: Option<u32>,
        upload_id: Option<String>,
    ) -> TestResult {
        // Filter out invalid inputs - only allow printable ASCII
        if etag.is_empty()
            || last_modified.is_empty()
            || !etag.chars().all(|c| c.is_ascii_graphic() || c == ' ')
            || !last_modified
                .chars()
                .all(|c| c.is_ascii_graphic() || c == ' ')
        {
            return TestResult::discard();
        }

        // Filter out invalid upload_id if present
        if let Some(ref id) = upload_id {
            if !id.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
                return TestResult::discard();
            }
        }

        // Create ObjectMetadata with multipart fields
        let original_metadata = ObjectMetadata {
            etag: etag.clone(),
            last_modified: last_modified.clone(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            response_headers: std::collections::HashMap::new(),
            upload_state: UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
            compressed_size: content_length,
            parts_count,
            part_ranges: HashMap::new(),
            upload_id: upload_id.clone(),
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
            graduation_accounted: false,
        };

        // Serialize
        let serialized = match serde_json::to_string(&original_metadata) {
            Ok(json) => json,
            Err(_) => return TestResult::failed(),
        };

        // Deserialize
        let deserialized: ObjectMetadata = match serde_json::from_str(&serialized) {
            Ok(metadata) => metadata,
            Err(_) => return TestResult::failed(),
        };

        // Verify all fields match
        TestResult::from_bool(
            deserialized.etag == etag
                && deserialized.last_modified == last_modified
                && deserialized.content_length == content_length
                && deserialized.parts_count == parts_count
                && deserialized.part_ranges.is_empty()
                && deserialized.upload_id == upload_id,
        )
    }

    /// **Feature: write-through-cache-finalization, Property 1: Full PUT creates single range**
    /// For any ObjectMetadata with write cache fields, serializing then deserializing
    /// should yield an equivalent ObjectMetadata with the same write cache values.
    /// This validates that write-cached objects can be persisted and recovered correctly.
    /// **Validates: Requirements 1.1, 1.2**
    #[quickcheck]
    fn prop_write_cache_metadata_round_trip(
        etag: String,
        last_modified: String,
        content_length: u64,
        is_write_cached: bool,
    ) -> TestResult {
        // Filter out invalid inputs - only allow printable ASCII
        if etag.is_empty()
            || last_modified.is_empty()
            || !etag.chars().all(|c| c.is_ascii_graphic() || c == ' ')
            || !last_modified
                .chars()
                .all(|c| c.is_ascii_graphic() || c == ' ')
        {
            return TestResult::discard();
        }

        // Create ObjectMetadata with write cache fields
        let now = std::time::SystemTime::now();
        let write_ttl = std::time::Duration::from_secs(86400); // 1 day

        let original_metadata = ObjectMetadata {
            etag: etag.clone(),
            last_modified: last_modified.clone(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            response_headers: std::collections::HashMap::new(),
            upload_state: UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
            compressed_size: content_length,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached,
            write_cache_expires_at: if is_write_cached {
                Some(now + write_ttl)
            } else {
                None
            },
            write_cache_created_at: if is_write_cached { Some(now) } else { None },
            write_cache_last_accessed: if is_write_cached { Some(now) } else { None },
            graduation_accounted: false,
        };

        // Serialize
        let serialized = match serde_json::to_string(&original_metadata) {
            Ok(json) => json,
            Err(_) => return TestResult::failed(),
        };

        // Deserialize
        let deserialized: ObjectMetadata = match serde_json::from_str(&serialized) {
            Ok(metadata) => metadata,
            Err(_) => return TestResult::failed(),
        };

        // Verify all write cache fields match
        let write_cache_fields_match = deserialized.is_write_cached == is_write_cached
            && deserialized.write_cache_expires_at.is_some()
                == original_metadata.write_cache_expires_at.is_some()
            && deserialized.write_cache_created_at.is_some()
                == original_metadata.write_cache_created_at.is_some()
            && deserialized.write_cache_last_accessed.is_some()
                == original_metadata.write_cache_last_accessed.is_some();

        // Verify basic fields also match
        let basic_fields_match = deserialized.etag == etag
            && deserialized.last_modified == last_modified
            && deserialized.content_length == content_length;

        TestResult::from_bool(write_cache_fields_match && basic_fields_match)
    }

    /// **Feature: write-through-cache-finalization, Property: Backward compatibility with write cache fields**
    /// For any ObjectMetadata created without write cache fields (old format),
    /// deserializing should succeed with default values (false/None) for write cache fields.
    /// **Validates: Requirements 1.2 (backward compatibility)**
    #[quickcheck]
    fn prop_write_cache_backward_compatibility(
        etag: String,
        last_modified: String,
        content_length: u64,
    ) -> TestResult {
        // Helper to check if a character is safe for JSON strings
        fn is_json_safe(c: char) -> bool {
            c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ' '
        }

        // Filter out invalid inputs - only allow JSON-safe characters
        if etag.is_empty()
            || last_modified.is_empty()
            || !etag.chars().all(is_json_safe)
            || !last_modified.chars().all(is_json_safe)
        {
            return TestResult::discard();
        }

        // Create old-format JSON without write cache fields
        let old_format_json = format!(
            r#"{{
                "etag": "{}",
                "last_modified": "{}",
                "content_length": {},
                "content_type": null,
                "response_headers": {{}},
                "upload_state": "Complete",
                "cumulative_size": {},
                "parts": [],
                "compression_algorithm": "None",
                "compressed_size": {}
            }}"#,
            etag, last_modified, content_length, content_length, content_length
        );

        // Deserialize should succeed
        let deserialized: Result<ObjectMetadata, _> = serde_json::from_str(&old_format_json);

        match deserialized {
            Ok(metadata) => {
                // Verify write cache fields have default values
                TestResult::from_bool(
                    metadata.etag == etag
                        && metadata.last_modified == last_modified
                        && metadata.content_length == content_length
                        && !metadata.is_write_cached
                        && metadata.write_cache_expires_at.is_none()
                        && metadata.write_cache_created_at.is_none()
                        && metadata.write_cache_last_accessed.is_none(),
                )
            }
            Err(_) => TestResult::failed(),
        }
    }

    // ===== TESTS FOR MULTIPART UPLOAD TRACKER =====

    #[test]
    fn test_cached_part_info_serialization() {
        let part_info = CachedPartInfo::new_uncompressed(
            1,
            5242880, // 5MB
            "\"d8e8fca2dc0f896fd7cb4cb0031ba249\"".to_string(),
        );

        // Serialize
        let json = serde_json::to_string(&part_info).unwrap();

        // Deserialize
        let deserialized: CachedPartInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.part_number, 1);
        assert_eq!(deserialized.size, 5242880);
        assert_eq!(deserialized.etag, "\"d8e8fca2dc0f896fd7cb4cb0031ba249\"");
    }

    #[test]
    fn test_multipart_upload_tracker_new() {
        let tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        assert_eq!(tracker.upload_id, "abc123xyz");
        assert_eq!(tracker.cache_key, "test-bucket/test-object");
        assert!(tracker.parts.is_empty());
        assert_eq!(tracker.total_size, 0);
    }

    #[test]
    fn test_multipart_upload_tracker_add_part() {
        let mut tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Add part 1
        let part1 = CachedPartInfo::new_uncompressed(1, 5242880, "\"etag1\"".to_string());
        tracker.add_part(part1);

        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.total_size, 5242880);

        // Add part 2
        let part2 = CachedPartInfo::new_uncompressed(2, 5242880, "\"etag2\"".to_string());
        tracker.add_part(part2);

        assert_eq!(tracker.parts.len(), 2);
        assert_eq!(tracker.total_size, 10485760);
    }

    #[test]
    fn test_multipart_upload_tracker_replace_part() {
        let mut tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Add part 1
        let part1 = CachedPartInfo::new_uncompressed(1, 5242880, "\"etag1\"".to_string());
        tracker.add_part(part1);

        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.total_size, 5242880);

        // Re-upload part 1 with different size
        let part1_new = CachedPartInfo::new_uncompressed(1, 6000000, "\"etag1_new\"".to_string());
        tracker.add_part(part1_new);

        // Should still have 1 part, but with updated size
        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.total_size, 6000000);
        assert_eq!(tracker.parts[0].etag, "\"etag1_new\"");
    }

    #[test]
    fn test_multipart_upload_tracker_get_sorted_parts() {
        let mut tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Add parts out of order
        tracker.add_part(CachedPartInfo::new_uncompressed(
            3,
            1000,
            "\"e3\"".to_string(),
        ));
        tracker.add_part(CachedPartInfo::new_uncompressed(
            1,
            1000,
            "\"e1\"".to_string(),
        ));
        tracker.add_part(CachedPartInfo::new_uncompressed(
            2,
            1000,
            "\"e2\"".to_string(),
        ));

        let sorted = tracker.get_sorted_parts();

        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].part_number, 1);
        assert_eq!(sorted[1].part_number, 2);
        assert_eq!(sorted[2].part_number, 3);
    }

    #[test]
    fn test_multipart_upload_tracker_serialization() {
        let mut tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        tracker.add_part(CachedPartInfo::new_uncompressed(
            1,
            5242880,
            "\"e1\"".to_string(),
        ));
        tracker.add_part(CachedPartInfo::new_uncompressed(
            2,
            5242880,
            "\"e2\"".to_string(),
        ));

        // Serialize
        let json = tracker.to_json().unwrap();

        // Deserialize
        let deserialized = MultipartUploadTracker::from_json(&json).unwrap();

        assert_eq!(deserialized.upload_id, "abc123xyz");
        assert_eq!(deserialized.cache_key, "test-bucket/test-object");
        assert_eq!(deserialized.parts.len(), 2);
        assert_eq!(deserialized.total_size, 10485760);
    }

    #[test]
    fn test_multipart_upload_tracker_is_expired() {
        // Create a tracker with a past start time to test expiration
        let mut tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Set started_at to 2 hours ago
        tracker.started_at = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);

        // Should not be expired with 3 hour TTL
        assert!(!tracker.is_expired(std::time::Duration::from_secs(10800)));

        // Should be expired with 1 hour TTL (started 2 hours ago)
        assert!(tracker.is_expired(std::time::Duration::from_secs(3600)));
    }

    /// **Feature: write-through-cache-finalization, Property: MultipartUploadTracker round-trip**
    /// For any MultipartUploadTracker, serializing then deserializing should yield
    /// an equivalent tracker with the same parts and metadata.
    /// **Validates: Requirements 2.2, 8.3**
    #[quickcheck]
    fn prop_multipart_upload_tracker_round_trip(
        upload_id: String,
        cache_key: String,
        part_count: u8,
    ) -> TestResult {
        // Filter out invalid inputs
        if upload_id.is_empty()
            || cache_key.is_empty()
            || !upload_id.chars().all(|c| c.is_ascii_graphic())
            || !cache_key.chars().all(|c| c.is_ascii_graphic() || c == '/')
        {
            return TestResult::discard();
        }

        // Limit part count to reasonable range
        let part_count = (part_count % 10) + 1; // 1-10 parts

        let mut tracker = MultipartUploadTracker::new(upload_id.clone(), cache_key.clone());

        // Add parts
        for i in 1..=part_count {
            let part = CachedPartInfo::new_uncompressed(
                i as u32,
                5242880, // 5MB each
                format!("\"etag{}\"", i),
            );
            tracker.add_part(part);
        }

        // Serialize
        let json = match tracker.to_json() {
            Ok(j) => j,
            Err(_) => return TestResult::failed(),
        };

        // Deserialize
        let deserialized = match MultipartUploadTracker::from_json(&json) {
            Ok(t) => t,
            Err(_) => return TestResult::failed(),
        };

        // Verify fields match
        TestResult::from_bool(
            deserialized.upload_id == upload_id
                && deserialized.cache_key == cache_key
                && deserialized.parts.len() == part_count as usize
                && deserialized.total_size == (part_count as u64) * 5242880,
        )
    }

    /// **Feature: write-through-cache-finalization, Property 9: Abort upload cleanup**
    /// *For any* AbortMultipartUpload request, all cached parts and tracking metadata for that uploadId SHALL be immediately removed.
    /// **Validates: Requirements 4.5, 8.5**
    #[quickcheck]
    fn prop_abort_upload_cleanup(part_count: u8, data_size_seed: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 5) + 1; // 1-5 parts
        let data_size = (data_size_seed % 100) + 10; // 10-109 bytes per part

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            use crate::compression::CompressionHandler;
            use crate::signed_put_handler::SignedPutHandler;
            use tempfile::TempDir;

            let temp_dir = TempDir::new().unwrap();
            let compression_handler = CompressionHandler::new(1024, true);
            let mut handler = SignedPutHandler::new(
                temp_dir.path().to_path_buf(),
                compression_handler,
                0,
                10 * 1024 * 1024, // 10MB capacity
                None,
                10 * 1024 * 1024, // 10 MiB max complete body
                5,                // write_cache_tee_channel_depth
            );

            let cache_key = "test-bucket/test-object";
            let upload_id = "test-upload-abort";

            // Populate the in-progress upload directory directly on disk (part
            // files + tracker), mirroring what a part write produces. The
            // buffered `cache_upload_part` helper was removed as dead code; this
            // test targets abort cleanup (`cleanup_multipart_upload`), so it only
            // needs parts present on disk, not the write path itself.
            let multipart_dir =
                crate::signed_put_handler::mpu_upload_dir(temp_dir.path(), upload_id);
            tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

            let mut tracker =
                MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string());
            for part_num in 1..=part_count {
                let data: Vec<u8> = (0..data_size).map(|i| i + part_num).collect();
                let part_file = multipart_dir.join(format!("part{}.bin", part_num));
                tokio::fs::write(&part_file, &data).await.unwrap();
                tracker.add_part(CachedPartInfo::new(
                    part_num as u32,
                    data.len() as u64,
                    format!("\"part-etag-{}\"", part_num),
                    crate::compression::CompressionAlgorithm::Lz4,
                ));
            }

            let upload_meta_file = multipart_dir.join("upload.meta");
            tokio::fs::write(&upload_meta_file, tracker.to_json().unwrap())
                .await
                .unwrap();

            if !upload_meta_file.exists() {
                return TestResult::failed();
            }

            // Read tracker to get part file paths
            let meta_content = match std::fs::read_to_string(&upload_meta_file) {
                Ok(c) => c,
                Err(_) => return TestResult::failed(),
            };

            let tracker: MultipartUploadTracker = match serde_json::from_str(&meta_content) {
                Ok(t) => t,
                Err(_) => return TestResult::failed(),
            };

            // Verify all parts exist on disk (parts are now in the upload directory)
            for part_info in &tracker.parts {
                let part_file = multipart_dir.join(format!("part{}.bin", part_info.part_number));
                if !part_file.exists() {
                    return TestResult::failed();
                }
            }

            // Perform abort cleanup
            let result = handler.cleanup_multipart_upload(upload_id).await;
            if result.is_err() {
                return TestResult::failed();
            }

            // Verify multipart directory is completely removed (all parts gone with it)
            if multipart_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// **Feature: write-through-cache-finalization, Property 8: Incomplete upload eviction**
    /// *For any* multipart upload that exceeds the incomplete upload TTL without completion, all cached parts and tracking metadata SHALL be actively removed.
    /// **Validates: Requirements 4.2, 4.3**
    #[quickcheck]
    fn prop_incomplete_upload_eviction(part_count: u8, ttl_seconds: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 3) + 1; // 1-3 parts
        let ttl_seconds = (ttl_seconds % 10) + 1; // 1-10 seconds

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            use crate::cache::CacheEvictionAlgorithm;
            use crate::write_cache_manager::WriteCacheManager;
            use std::time::Duration;
            use tempfile::TempDir;

            let temp_dir = TempDir::new().unwrap();
            let ttl = Duration::from_secs(ttl_seconds as u64);

            let manager = WriteCacheManager::new(
                temp_dir.path().to_path_buf(),
                10 * 1024 * 1024,           // 10MB total cache
                10.0,                       // 10% for write cache
                Duration::from_secs(86400), // 1 day write TTL
                ttl,                        // Short incomplete upload TTL for testing
                CacheEvictionAlgorithm::TinyLFU,
                256 * 1024 * 1024, // 256MB max object size
            );

            // Create an incomplete multipart upload
            let upload_id = "test-upload-eviction";
            let multipart_dir =
                crate::signed_put_handler::mpu_upload_dir(temp_dir.path(), upload_id);

            if tokio::fs::create_dir_all(&multipart_dir).await.is_err() {
                return TestResult::failed();
            }

            // Create upload.meta with some parts
            let mut tracker = MultipartUploadTracker::new(
                upload_id.to_string(),
                "test-bucket/test-object".to_string(),
            );

            // Add some parts to the tracker and create part files in the upload directory
            for part_num in 1..=part_count {
                let data = vec![part_num; 100]; // 100 bytes per part
                let etag = format!("\"etag-{}\"", part_num);

                // Create part file in the upload directory
                let part_file = multipart_dir.join(format!("part{}.bin", part_num));

                if tokio::fs::write(&part_file, &data).await.is_err() {
                    return TestResult::failed();
                }

                let part_info =
                    CachedPartInfo::new_uncompressed(part_num as u32, data.len() as u64, etag);

                tracker.add_part(part_info);
            }

            // Write the tracker to upload.meta
            let upload_meta_file = multipart_dir.join("upload.meta");
            let tracker_json = match tracker.to_json() {
                Ok(json) => json,
                Err(_) => return TestResult::failed(),
            };

            if tokio::fs::write(&upload_meta_file, tracker_json)
                .await
                .is_err()
            {
                return TestResult::failed();
            }

            // Verify parts exist before eviction
            for part_info in &tracker.parts {
                let part_file = multipart_dir.join(format!("part{}.bin", part_info.part_number));
                if !part_file.exists() {
                    return TestResult::failed();
                }
            }

            // Set file mtime to be older than TTL (instead of sleeping)
            let old_time = std::time::SystemTime::now() - ttl - Duration::from_secs(10);
            let old_filetime = filetime::FileTime::from_system_time(old_time);
            if filetime::set_file_mtime(&upload_meta_file, old_filetime).is_err() {
                return TestResult::failed();
            }
            // Backdate the DIRECTORY too, for the same reason as the sibling fixture in
            // `write_cache_manager.rs`: the sweep takes the newest of the directory's
            // mtime and `upload.meta`'s, because that file is written once at
            // CreateMultipartUpload and so records the upload's START. A fresh
            // directory is what a live long-running upload looks like, and the sweep
            // must not evict one of those.
            if filetime::set_file_mtime(&multipart_dir, old_filetime).is_err() {
                return TestResult::failed();
            }

            // Run eviction
            let freed_bytes = match manager.evict_incomplete_uploads().await {
                Ok(freed) => freed,
                Err(_) => return TestResult::failed(),
            };

            // Verify eviction occurred
            if freed_bytes == 0 {
                return TestResult::failed();
            }

            // Verify multipart directory is removed (all parts gone with it)
            if multipart_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// Property 8: No-panic TTL arithmetic — for every generated (SystemTime, Duration) pair
    /// across the full u64 range, assert no panic and valid result.
    ///
    /// The property verifies:
    /// 1. safe_expiry never panics (implicit — if it panics, the test fails)
    /// 2. The result is >= base (expiry is never before the base time)
    /// 3. The result is <= base + 10 years (the clamp works)
    ///
    /// **Validates: Requirements 8.1, 8.2, 8.3, 8.4**
    #[quickcheck]
    fn prop_safe_expiry_no_panic(base_secs: u64, ttl_secs: u64) -> TestResult {
        // Construct a SystemTime from the base timestamp (seconds since UNIX_EPOCH)
        // Cap base_secs to avoid SystemTime construction overflow on platforms where
        // SystemTime cannot represent the full u64 range of seconds.
        let base = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(base_secs))
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let ttl = Duration::from_secs(ttl_secs);

        // Call safe_expiry — if it panics, the test fails automatically
        let result = safe_expiry(base, ttl);

        // Assert: result >= base (expiry is never before the base time)
        if result < base {
            return TestResult::failed();
        }

        // Assert: result <= base + 10 years (the clamp works)
        let max_ttl = Duration::from_secs(MAX_TTL_SECS);
        let max_expiry = base.checked_add(max_ttl).unwrap_or(base);
        if result > max_expiry {
            return TestResult::failed();
        }

        TestResult::passed()
    }
}

#[cfg(test)]
mod head_cached_at_tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn refresh_head_ttl_updates_the_expiry_access_time_and_anchor_together() {
        let before = SystemTime::now();
        let mut metadata = NewCacheMetadata::default();
        metadata.refresh_head_ttl(Duration::from_secs(60));
        let after = SystemTime::now();

        let anchor = metadata.head_cached_at.expect("refresh must set an anchor");
        let accessed = metadata
            .head_last_accessed
            .expect("refresh must record the access time");
        let expiry = metadata
            .head_expires_at
            .expect("refresh must set an expiry");
        assert!(anchor >= before && anchor <= after);
        assert_eq!(anchor, accessed);
        assert!(expiry.duration_since(anchor).unwrap() >= Duration::from_secs(60));
    }

    #[test]
    fn legacy_metadata_without_head_cached_at_deserializes_to_none() {
        let mut value = serde_json::to_value(NewCacheMetadata::default()).unwrap();
        value.as_object_mut().unwrap().remove("head_cached_at");
        let metadata: NewCacheMetadata = serde_json::from_value(value).unwrap();
        assert_eq!(metadata.head_cached_at, None);
    }

    #[test]
    fn metadata_with_head_cached_at_is_readable_by_a_prior_schema() {
        #[derive(Deserialize)]
        struct PriorMetadata {
            cache_key: String,
        }

        let metadata = NewCacheMetadata {
            cache_key: "bucket/key".to_string(),
            head_cached_at: Some(SystemTime::now()),
            ..Default::default()
        };
        let parsed: PriorMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap())
                .expect("prior schemas must ignore the additive anchor field");
        assert_eq!(parsed.cache_key, "bucket/key");
    }

    #[test]
    fn metadata_without_last_validated_at_parses_and_serializes_unchanged() {
        let value = serde_json::to_value(NewCacheMetadata::default()).unwrap();
        assert!(
            value.get("last_validated_at").is_none(),
            "an unvalidated entry must not grow the .meta"
        );
        let metadata: NewCacheMetadata = serde_json::from_value(value).unwrap();
        assert_eq!(metadata.last_validated_at, None);
        assert_eq!(metadata.freshness_anchor(), metadata.created_at);
    }

    /// a `.meta` range written before `unservable` existed parses as servable, and a
    /// servable range does not grow the document. A marked range round-trips.
    #[test]
    fn range_spec_without_unservable_parses_as_servable_and_round_trips() {
        let mut value = serde_json::to_value(RangeSpec::new(
            0,
            9,
            "b/x_0-9.bin".into(),
            CompressionAlgorithm::Lz4,
            10,
            10,
        ))
        .unwrap();
        assert!(
            value.get("unservable").is_none(),
            "a servable range must not grow the .meta"
        );
        value.as_object_mut().unwrap().remove("unservable");
        let parsed: RangeSpec = serde_json::from_value(value).unwrap();
        assert!(!parsed.unservable, "a pre-field range is servable");

        let marked = RangeSpec {
            unservable: true,
            file_identity: None,
            ..parsed
        };
        let text = serde_json::to_string(&marked).unwrap();
        assert!(text.contains("\"unservable\":true"));
        let back: RangeSpec = serde_json::from_str(&text).unwrap();
        assert!(back.unservable, "the marker survives a .meta round trip");
    }

    #[test]
    fn record_origin_validation_reanchors_age_but_not_created_at() {
        let created = SystemTime::now() - Duration::from_secs(7200);
        let mut metadata = NewCacheMetadata {
            created_at: created,
            ..Default::default()
        };
        metadata.record_origin_validation(Duration::from_secs(3600));
        assert_eq!(metadata.created_at, created, "created_at keeps its meaning");
        assert!(metadata.freshness_anchor().elapsed().unwrap() < Duration::from_secs(5));
        assert!(metadata.expires_at > SystemTime::now());

        // Mode B's refresh moves stored expiry only.
        let mut mode_b = NewCacheMetadata {
            created_at: created,
            ..Default::default()
        };
        mode_b.refresh_object_ttl(Duration::from_secs(3600));
        assert_eq!(mode_b.freshness_anchor(), created);

        // An anchor older than created_at (a stale field on a re-cached entry) never makes
        // an entry look older than it is.
        let recached = NewCacheMetadata {
            last_validated_at: Some(created),
            ..Default::default()
        };
        assert_eq!(recached.freshness_anchor(), recached.created_at);
    }

    // Spec: write-cache-last-modified. Requirements: 1.2, 11.11 (task 1.1)

    #[test]
    fn effective_last_modified_field_only() {
        let om = ObjectMetadata {
            last_modified: "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
            ..Default::default()
        };
        assert_eq!(
            om.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn effective_last_modified_map_only() {
        let mut om = ObjectMetadata::default();
        om.response_headers.insert(
            "last-modified".to_string(),
            "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
        );
        assert_eq!(
            om.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn effective_last_modified_both_present_prefers_typed_field() {
        let mut om = ObjectMetadata {
            last_modified: "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
            ..Default::default()
        };
        om.response_headers.insert(
            "last-modified".to_string(),
            "Thu, 10 Sep 2026 00:00:00 GMT".to_string(),
        );
        assert_eq!(
            om.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn effective_last_modified_neither_present_is_none() {
        let om = ObjectMetadata::default();
        assert_eq!(om.effective_last_modified(), None);
    }

    #[test]
    fn effective_last_modified_map_key_is_case_insensitive() {
        let mut om = ObjectMetadata::default();
        om.response_headers.insert(
            "Last-Modified".to_string(),
            "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
        );
        assert_eq!(
            om.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );

        let mut om2 = ObjectMetadata::default();
        om2.response_headers.insert(
            "LAST-MODIFIED".to_string(),
            "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
        );
        assert_eq!(
            om2.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn effective_last_modified_empty_field_falls_back_to_map() {
        // The empty-string default is the write-through PUT's own state (R1).
        let mut om = ObjectMetadata {
            last_modified: String::new(),
            ..Default::default()
        };
        om.response_headers.insert(
            "last-modified".to_string(),
            "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
        );
        assert_eq!(
            om.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn effective_last_modified_empty_map_value_is_none() {
        let mut om = ObjectMetadata::default();
        om.response_headers
            .insert("last-modified".to_string(), String::new());
        assert_eq!(om.effective_last_modified(), None);
    }

    // Spec: write-cache-last-modified. Requirements: 3.1, 3.2 (task 1.2)

    #[test]
    fn set_last_modified_writes_both_sources() {
        let mut om = ObjectMetadata::default();
        om.set_last_modified("Wed, 09 Sep 2026 10:28:17 GMT".to_string());
        assert_eq!(om.last_modified, "Wed, 09 Sep 2026 10:28:17 GMT");
        assert_eq!(
            om.response_headers.get("last-modified").map(String::as_str),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn set_last_modified_removes_existing_case_variant_first() {
        let mut om = ObjectMetadata::default();
        om.response_headers
            .insert("Last-Modified".to_string(), "stale-value".to_string());
        om.set_last_modified("Wed, 09 Sep 2026 10:28:17 GMT".to_string());

        // No stray case-variant key left behind.
        assert_eq!(
            om.response_headers
                .keys()
                .filter(|k| k.eq_ignore_ascii_case("last-modified"))
                .count(),
            1
        );
        assert_eq!(
            om.response_headers.get("last-modified").map(String::as_str),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }

    #[test]
    fn set_last_modified_then_effective_last_modified_agrees() {
        let mut om = ObjectMetadata::default();
        om.set_last_modified("Wed, 09 Sep 2026 10:28:17 GMT".to_string());
        assert_eq!(
            om.effective_last_modified(),
            Some("Wed, 09 Sep 2026 10:28:17 GMT")
        );
    }
}

/// [`RangeSpec::is_unverified_prefix`] at each boundary it branches on, beside the value one
/// step across it.
#[cfg(all(test, unix))]
mod unverified_prefix_tests {
    use super::*;

    const LEN: u64 = 4_194_304;

    fn spec(start: u64, end: u64, identity: bool) -> RangeSpec {
        RangeSpec::new(
            start,
            end,
            format!("b/XX/YYY/k_{}-{}.bin", start, end),
            CompressionAlgorithm::Lz4,
            end - start + 1,
            end - start + 1,
        )
        .with_file_identity(identity.then_some(RangeFileIdentity {
            size: end - start + 1,
            mtime_ns: 1,
        }))
    }

    #[test]
    fn a_pre_3_0_0_extent_at_zero_short_of_the_end_is_refused() {
        assert!(spec(0, 99, false).is_unverified_prefix(LEN));
        // One byte short of the whole object: still refused.
        assert!(spec(0, LEN - 2, false).is_unverified_prefix(LEN));
        // A document that records no length.
        assert!(spec(0, 99, false).is_unverified_prefix(0));
    }

    #[test]
    fn the_adjacent_shapes_are_served() {
        // The whole object.
        assert!(!spec(0, LEN - 1, false).is_unverified_prefix(LEN));
        // Starts one byte later.
        assert!(!spec(1, 99, false).is_unverified_prefix(LEN));
        // Written by 3.0.0.
        assert!(!spec(0, 99, true).is_unverified_prefix(LEN));
        assert!(!spec(0, 99, true).is_unverified_prefix(0));
    }

    /// A listing a pre-3.0.0 writer left has no `file_identity` key at all, and one an
    /// earlier 3.0 development build left has an inode-and-length identity; both parse to
    /// `None` and are refused.
    #[test]
    fn pre_3_0_0_listings_parse_to_no_identity() {
        let mut value = serde_json::to_value(spec(0, 99, true)).unwrap();
        value.as_object_mut().unwrap().remove("file_identity");
        let legacy: RangeSpec = serde_json::from_value(value.clone()).unwrap();
        assert!(legacy.is_unverified_prefix(LEN));
        value["file_identity"] = serde_json::json!({"ino": 7, "size": 100});
        let dev: RangeSpec = serde_json::from_value(value).unwrap();
        assert!(dev.is_unverified_prefix(LEN));
    }
}
