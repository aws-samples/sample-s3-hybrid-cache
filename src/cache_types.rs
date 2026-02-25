//! Cache Types Module
//!
//! Provides basic cache data structures used by the disk cache system.

use crate::compression::CompressionAlgorithm;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

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
    },
}

/// Cache entry for storing S3 objects and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub cache_key: String,
    pub headers: HashMap<String, String>,
    pub body: Option<Vec<u8>>,
    pub ranges: Vec<Range>,
    pub metadata: CacheMetadata,
    pub created_at: SystemTime,
    pub expires_at: SystemTime,
    /// Separate expiration for metadata validation (typically shorter than data TTL)
    /// Allows HEAD requests to validate object freshness without invalidating cached body
    pub metadata_expires_at: SystemTime,
    pub compression_info: CompressionInfo,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UploadState {
    Complete,   // Regular PUT or completed multipart
    InProgress, // Multipart upload in progress
    Bypassed,   // Upload too large, not caching
}

impl Default for UploadState {
    fn default() -> Self {
        UploadState::Complete
    }
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
    /// Size of the part in bytes
    pub size: u64,
    /// ETag returned by S3 for this part
    pub etag: String,
    /// Compression algorithm used for this part (None if uncompressed)
    #[serde(default)]
    pub compression_algorithm: crate::compression::CompressionAlgorithm,
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
            etag,
            compression_algorithm,
        }
    }

    /// Create a new CachedPartInfo with default compression (Lz4)
    pub fn new_uncompressed(
        part_number: u32,
        size: u64,
        etag: String,
    ) -> Self {
        Self {
            part_number,
            size,
            etag,
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        }
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
    pub fn new_with_content_type(
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
    pub fn add_part(&mut self, part_info: CachedPartInfo) {
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
    pub fn get_sorted_parts(&self) -> Vec<&CachedPartInfo> {
        let mut sorted: Vec<&CachedPartInfo> = self.parts.iter().collect();
        sorted.sort_by_key(|p| p.part_number);
        sorted
    }

    /// Calculate byte offsets for all parts after sorting
    /// Returns Vec of (part_number, start_offset, end_offset)
    pub fn calculate_byte_offsets(&self) -> Vec<(u32, u64, u64)> {
        let sorted_parts = self.get_sorted_parts();
        let mut offsets = Vec::with_capacity(sorted_parts.len());
        let mut current_offset: u64 = 0;

        for part in sorted_parts {
            let start = current_offset;
            let end = current_offset + part.size - 1;
            offsets.push((part.part_number, start, end));
            current_offset += part.size;
        }

        offsets
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
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
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
        }
    }
}

impl ObjectMetadata {
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
        }
    }

    /// Create a new ObjectMetadata with compression information
    pub fn new_with_compression(
        etag: String,
        last_modified: String,
        content_length: u64,
        content_type: Option<String>,
        compression_algorithm: CompressionAlgorithm,
        compressed_size: u64,
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
            compression_algorithm,
            compressed_size,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
        }
    }

    /// Create a new ObjectMetadata with compression information and complete headers
    pub fn new_with_compression_and_headers(
        etag: String,
        last_modified: String,
        content_length: u64,
        content_type: Option<String>,
        response_headers: HashMap<String, String>,
        compression_algorithm: CompressionAlgorithm,
        compressed_size: u64,
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
            compression_algorithm,
            compressed_size,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
        }
    }

    /// Create a new ObjectMetadata for write-cached objects (PUT operations)
    /// Sets is_write_cached=true and initializes write cache TTL fields
    pub fn new_write_cached(
        etag: String,
        last_modified: String,
        content_length: u64,
        content_type: Option<String>,
        response_headers: HashMap<String, String>,
        compression_algorithm: CompressionAlgorithm,
        compressed_size: u64,
        write_ttl: std::time::Duration,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            etag,
            last_modified,
            content_length,
            content_type,
            response_headers,
            upload_state: UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            compression_algorithm,
            compressed_size,
            parts_count: None,
            part_ranges: HashMap::new(),
            upload_id: None,
            is_write_cached: true,
            write_cache_expires_at: Some(now + write_ttl),
            write_cache_created_at: Some(now),
            write_cache_last_accessed: Some(now),
        }
    }

    /// Check if write cache entry is expired
    pub fn is_write_cache_expired(&self) -> bool {
        if !self.is_write_cached {
            return false;
        }
        match self.write_cache_expires_at {
            Some(expires_at) => SystemTime::now() > expires_at,
            None => false, // No expiration set means not expired
        }
    }
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

    // TinyLFU frequency tracking
    #[serde(default)]
    pub frequency_score: u64, // Frequency estimate for TinyLFU algorithm
}

impl RangeSpec {
    /// Create a new RangeSpec with initial access tracking
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
            frequency_score: 1, // Initial frequency score
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

    /// Calculate TinyLFU score (lower = evict first)
    /// Combines frequency and recency: frequency weighted by recency
    pub fn tinylfu_score(&self) -> u64 {
        let now = SystemTime::now();
        let recency_factor = now
            .duration_since(self.last_accessed)
            .unwrap_or_default()
            .as_secs()
            .max(1);

        // Frequency weighted by recency
        self.access_count * 1000 / recency_factor
    }

    /// Update access statistics
    pub fn record_access(&mut self) {
        self.last_accessed = SystemTime::now();
        self.access_count += 1;

        // Update frequency score for TinyLFU algorithm
        // Use a simple increment with decay to prevent unbounded growth
        self.frequency_score = (self.frequency_score + 1).min(1000);
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
}

impl NewCacheMetadata {
    /// Check if HEAD cache has expired
    /// Returns true if head_expires_at is None or in the past
    pub fn is_head_expired(&self) -> bool {
        match self.head_expires_at {
            Some(expires_at) => SystemTime::now() > expires_at,
            None => true, // No HEAD expiry set means HEAD is not cached
        }
    }

    /// Refresh HEAD TTL by setting head_expires_at to now + ttl
    pub fn refresh_head_ttl(&mut self, ttl: std::time::Duration) {
        let now = SystemTime::now();
        self.head_expires_at = Some(now + ttl);
        self.head_last_accessed = Some(now);
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

    /// Refresh the object-level TTL by setting `expires_at = now + ttl`.
    pub fn refresh_object_ttl(&mut self, ttl: Duration) {
        self.expires_at = SystemTime::now() + ttl;
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
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

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
            frequency_score: 1,
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
            frequency_score: 1,
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
        assert!(diff < Duration::from_secs(2), "expires_at not within tolerance");
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
            frequency_score: 1,
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
            frequency_score: 1,
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
            frequency_score: 100,
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
            frequency_score: 5,
        };

        // Hot range should have higher score (less likely to be evicted)
        assert!(hot_range.tinylfu_score() > cold_range.tinylfu_score());
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
            frequency_score: 5,
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
                        && metadata.is_write_cached == false
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
        let part1 = CachedPartInfo::new_uncompressed(
            1,
            5242880,
            "\"etag1\"".to_string(),
        );
        tracker.add_part(part1);

        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.total_size, 5242880);

        // Add part 2
        let part2 = CachedPartInfo::new_uncompressed(
            2,
            5242880,
            "\"etag2\"".to_string(),
        );
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
        let part1 = CachedPartInfo::new_uncompressed(
            1,
            5242880,
            "\"etag1\"".to_string(),
        );
        tracker.add_part(part1);

        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.total_size, 5242880);

        // Re-upload part 1 with different size
        let part1_new = CachedPartInfo::new_uncompressed(
            1,
            6000000,
            "\"etag1_new\"".to_string(),
        );
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
    fn test_multipart_upload_tracker_calculate_byte_offsets() {
        let mut tracker = MultipartUploadTracker::new(
            "abc123xyz".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Add parts with different sizes
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
        tracker.add_part(CachedPartInfo::new_uncompressed(
            3,
            1000000,
            "\"e3\"".to_string(),
        ));

        let offsets = tracker.calculate_byte_offsets();

        assert_eq!(offsets.len(), 3);
        // Part 1: 0 to 5242879
        assert_eq!(offsets[0], (1, 0, 5242879));
        // Part 2: 5242880 to 10485759
        assert_eq!(offsets[1], (2, 5242880, 10485759));
        // Part 3: 10485760 to 11485759
        assert_eq!(offsets[2], (3, 10485760, 11485759));
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

    /// **Feature: write-through-cache-finalization, Property 6: Multipart byte offset calculation**
    /// *For any* multipart upload with N parts, the final byte offsets SHALL be calculated such that
    /// part K starts at sum(size of parts 1 to K-1) and ends at sum(size of parts 1 to K) - 1.
    /// **Validates: Requirements 2.3, 3.2**
    #[quickcheck]
    fn prop_multipart_byte_offset_calculation(part_sizes: Vec<u16>) -> TestResult {
        // Filter out invalid inputs
        if part_sizes.is_empty() || part_sizes.len() > 20 {
            return TestResult::discard();
        }

        // Ensure all parts have non-zero size
        let part_sizes: Vec<u64> = part_sizes
            .iter()
            .map(|&s| (s as u64).max(1) * 1024) // At least 1KB per part
            .collect();

        let mut tracker = MultipartUploadTracker::new(
            "test-upload".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Add parts with the given sizes
        for (i, &size) in part_sizes.iter().enumerate() {
            let part_number = (i + 1) as u32;
            let part = CachedPartInfo::new_uncompressed(
                part_number,
                size,
                format!("\"etag{}\"", part_number),
            );
            tracker.add_part(part);
        }

        // Calculate byte offsets
        let offsets = tracker.calculate_byte_offsets();

        // Verify the number of offsets matches the number of parts
        if offsets.len() != part_sizes.len() {
            return TestResult::failed();
        }

        // Verify byte offset properties:
        // 1. Part K starts at sum(size of parts 1 to K-1)
        // 2. Part K ends at sum(size of parts 1 to K) - 1
        // 3. No gaps or overlaps between consecutive parts
        let mut expected_start: u64 = 0;

        for (i, (part_number, start, end)) in offsets.iter().enumerate() {
            let expected_size = part_sizes[i];
            let expected_end = expected_start + expected_size - 1;

            // Verify part number is correct (1-indexed)
            if *part_number != (i + 1) as u32 {
                return TestResult::failed();
            }

            // Verify start offset
            if *start != expected_start {
                return TestResult::failed();
            }

            // Verify end offset
            if *end != expected_end {
                return TestResult::failed();
            }

            // Update expected start for next part
            expected_start = expected_end + 1;
        }

        // Verify total size matches sum of all parts
        let total_size: u64 = part_sizes.iter().sum();
        if tracker.total_size != total_size {
            return TestResult::failed();
        }

        TestResult::passed()
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
            );

            let cache_key = "test-bucket/test-object";
            let upload_id = "test-upload-abort";

            // Cache multiple parts
            let mut expected_parts = Vec::new();
            for part_num in 1..=part_count {
                let data: Vec<u8> = (0..data_size).map(|i| (i + part_num as u8) as u8).collect();
                let etag = format!("\"part-etag-{}\"", part_num);

                let result = handler
                    .cache_upload_part(cache_key, upload_id, part_num as u32, &data, &etag)
                    .await;

                if result.is_err() {
                    return TestResult::failed();
                }

                expected_parts.push((part_num, data.len()));
            }

            // Verify parts exist before cleanup
            let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);
            let upload_meta_file = multipart_dir.join("upload.meta");

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
                CacheEvictionAlgorithm::LRU,
                256 * 1024 * 1024, // 256MB max object size
            );

            // Create an incomplete multipart upload
            let upload_id = "test-upload-eviction";
            let multipart_dir = temp_dir.path().join("mpus_in_progress").join(upload_id);

            if let Err(_) = tokio::fs::create_dir_all(&multipart_dir).await {
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

                if let Err(_) = tokio::fs::write(&part_file, &data).await {
                    return TestResult::failed();
                }

                let part_info = CachedPartInfo::new_uncompressed(
                    part_num as u32,
                    data.len() as u64,
                    etag,
                );

                tracker.add_part(part_info);
            }

            // Write the tracker to upload.meta
            let upload_meta_file = multipart_dir.join("upload.meta");
            let tracker_json = match tracker.to_json() {
                Ok(json) => json,
                Err(_) => return TestResult::failed(),
            };

            if let Err(_) = tokio::fs::write(&upload_meta_file, tracker_json).await {
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
            if let Err(_) = filetime::set_file_mtime(&upload_meta_file, old_filetime) {
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
}
