//! Cache Validator Module
//!
//! Shared validation utilities for cache subsystems. Covers structural integrity of
//! the on-disk cache metadata: JSON parses cleanly, ranges don't overlap, sizes
//! in metadata are arithmetically consistent (`end - start + 1`), referenced
//! range files exist, and so on.
//!
//! This is structural validation only. Byte-level content integrity is provided
//! separately by the LZ4 frame content checksum (xxhash32) which the compressor
//! attaches to every cached range file and which the decompressor verifies on
//! every read. See `crate::compression` and `docs/COMPRESSION.md`.
//!
//! # When to use this module
//!
//! - Periodic cache-consistency sweeps.
//! - Diagnostic tooling (dashboard, ops scripts) that needs to enumerate and
//!   characterize cache contents.
//! - Any code path that needs to decide whether a metadata file is safe to
//!   trust before reading its referenced ranges.
//!
//! # When NOT to roll your own
//!
//! If you find yourself writing ad-hoc checks for "does this metadata file
//! parse", "do its range offsets add up", or "are its range files present",
//! use [`CacheValidator`] instead. It has been tuned for parallel scan
//! performance and has established error types.
//!
//! # Requirements
//! - Requirement 5.1: Shared metadata validation logic
//! - Requirement 5.2: Shared directory scanning and error handling
//! - Requirement 5.3: Shared size calculation methods
//! - Requirement 5.4: Shared file filtering and categorization

use crate::cache_types::{NewCacheMetadata, RangeSpec};
use crate::{ProxyError, Result};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use walkdir::WalkDir;

/// Shared validation utilities for cache subsystems
pub struct CacheValidator {
    cache_dir: PathBuf,
}

/// Result of validating a metadata file
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the metadata file is valid
    pub is_valid: bool,
    /// Error message if validation failed
    pub error_message: Option<String>,
    /// Parsed metadata if validation succeeded
    pub metadata: Option<NewCacheMetadata>,
}

/// Result of scanning a cache file during directory traversal
#[derive(Debug, Clone)]
pub struct CacheFileResult {
    /// Path to the file
    pub file_path: PathBuf,
    /// Compressed size in bytes (for metadata files)
    pub compressed_size: u64,
    /// Whether this is a write-cached object
    pub is_write_cached: bool,
    /// Parsed metadata if available
    pub metadata: Option<NewCacheMetadata>,
    /// Any errors encountered during parsing
    pub parse_errors: Vec<String>,
}

/// Categorization of cache files
#[derive(Debug, Clone, PartialEq)]
pub enum CacheFileCategory {
    /// Write-cached object (PUT operation)
    WriteCached,
    /// Read-cached object (GET operation)
    ReadCached,
    /// Invalid or corrupted file
    Invalid,
}

/// Size statistics for cache file analysis
#[derive(Debug, Clone)]
pub struct SizeStatistics {
    /// Total compressed size of all files
    pub total_compressed_size: u64,
    /// Total compressed size of write-cached files
    pub write_cached_size: u64,
    /// Total compressed size of read-cached files
    pub read_cached_size: u64,
    /// Total number of files
    pub total_count: usize,
    /// Number of write-cached files
    pub write_cached_count: usize,
    /// Number of read-cached files
    pub read_cached_count: usize,
    /// Number of invalid files
    pub invalid_count: usize,
    /// Average file size
    pub average_size: u64,
}

/// Compression statistics for cache analysis
#[derive(Debug, Clone)]
pub struct CompressionStatistics {
    /// Total compressed size
    pub total_compressed_size: u64,
    /// Total uncompressed size
    pub total_uncompressed_size: u64,
    /// Overall compression ratio (compressed/uncompressed)
    pub overall_compression_ratio: f64,
    /// Average compression ratio across all objects
    pub average_compression_ratio: f64,
    /// Bytes saved through compression
    pub space_saved_bytes: u64,
    /// Percentage of space saved
    pub space_saved_percentage: f64,
    /// Number of objects analyzed
    pub objects_analyzed: usize,
}

impl Default for CompressionStatistics {
    fn default() -> Self {
        Self {
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            overall_compression_ratio: 1.0,
            average_compression_ratio: 1.0,
            space_saved_bytes: 0,
            space_saved_percentage: 0.0,
            objects_analyzed: 0,
        }
    }
}

/// Size distribution bucket for histogram analysis
#[derive(Debug, Clone)]
pub struct SizeBucket {
    /// Minimum size for this bucket (inclusive)
    pub min_size: u64,
    /// Maximum size for this bucket (inclusive)
    pub max_size: u64,
    /// Number of files in this bucket
    pub count: usize,
}

// =========================================================================
// Atomic Metadata Writes Validation Types (Requirements 7.1, 7.2, 7.3, 7.4, 7.5)
// =========================================================================

/// Result of metadata integrity validation
#[derive(Debug, Clone)]
pub struct MetadataIntegrityResult {
    pub is_valid: bool,
    pub errors: Vec<MetadataIntegrityError>,
    pub warnings: Vec<MetadataIntegrityWarning>,
}

/// Result of range integrity validation
#[derive(Debug, Clone)]
pub struct RangeIntegrityResult {
    pub is_valid: bool,
    pub overlapping_ranges: Vec<(usize, usize)>, // Indices of overlapping ranges
    pub size_mismatches: Vec<usize>,             // Indices of ranges with size mismatches
    pub errors: Vec<MetadataIntegrityError>,
}

/// Result of consistency integrity validation
///
/// There is no orphan list here. See
/// [`CacheValidator::validate_consistency_integrity`] for why the one that used to be
/// reported — always empty, never computed — was removed rather than implemented.
#[derive(Debug, Clone)]
pub struct ConsistencyIntegrityResult {
    pub is_consistent: bool,
    pub missing_range_files: Vec<String>,
    pub metadata_errors: Vec<MetadataIntegrityError>,
}

/// Metadata integrity validation error types
#[derive(Debug, Clone)]
pub enum MetadataIntegrityError {
    InvalidJson(String),
    MissingRequiredField(String),
    InvalidFieldValue {
        field: String,
        reason: String,
    },
    RangeOverlap {
        range1_index: usize,
        range2_index: usize,
    },
    SizeInconsistency {
        expected: u64,
        actual: u64,
    },
    InvalidTimestamp {
        field: String,
        timestamp: String,
    },
    RangeFileMissing {
        range_index: usize,
        file_path: String,
    },
}

/// Metadata integrity validation warning types
#[derive(Debug, Clone)]
pub enum MetadataIntegrityWarning {
    SuspiciousTimestamp { field: String, reason: String },
    LargeRangeCount { count: usize },
    UnusualRangeSize { range_index: usize, size: u64 },
}

impl MetadataIntegrityResult {
    /// Create a new validation result
    pub fn new(
        is_valid: bool,
        errors: Vec<MetadataIntegrityError>,
        warnings: Vec<MetadataIntegrityWarning>,
    ) -> Self {
        Self {
            is_valid,
            errors,
            warnings,
        }
    }

    /// Create a successful validation result
    pub fn success() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Create a failed validation result with errors
    pub fn failure(errors: Vec<MetadataIntegrityError>) -> Self {
        Self {
            is_valid: false,
            errors,
            warnings: Vec::new(),
        }
    }
    /// Get summary string
    pub fn summary(&self) -> String {
        if self.is_valid {
            if self.warnings.is_empty() {
                "Valid".to_string()
            } else {
                format!("Valid with {} warnings", self.warnings.len())
            }
        } else {
            format!("Invalid: {} errors", self.errors.len())
        }
    }
}

impl std::fmt::Display for MetadataIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataIntegrityError::InvalidJson(msg) => write!(f, "Invalid JSON: {}", msg),
            MetadataIntegrityError::MissingRequiredField(field) => {
                write!(f, "Missing required field: {}", field)
            }
            MetadataIntegrityError::InvalidFieldValue { field, reason } => {
                write!(f, "Invalid field '{}': {}", field, reason)
            }
            MetadataIntegrityError::RangeOverlap {
                range1_index,
                range2_index,
            } => {
                write!(
                    f,
                    "Range overlap between ranges {} and {}",
                    range1_index, range2_index
                )
            }
            MetadataIntegrityError::SizeInconsistency { expected, actual } => {
                write!(
                    f,
                    "Size inconsistency: expected {}, actual {}",
                    expected, actual
                )
            }
            MetadataIntegrityError::InvalidTimestamp { field, timestamp } => {
                write!(f, "Invalid timestamp in field '{}': {}", field, timestamp)
            }
            MetadataIntegrityError::RangeFileMissing {
                range_index,
                file_path,
            } => {
                write!(
                    f,
                    "Range file missing for range {}: {}",
                    range_index, file_path
                )
            }
        }
    }
}

impl std::fmt::Display for MetadataIntegrityWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataIntegrityWarning::SuspiciousTimestamp { field, reason } => {
                write!(f, "Suspicious timestamp in field '{}': {}", field, reason)
            }
            MetadataIntegrityWarning::LargeRangeCount { count } => {
                write!(f, "Large number of ranges: {}", count)
            }
            MetadataIntegrityWarning::UnusualRangeSize { range_index, size } => {
                write!(
                    f,
                    "Unusual range size for range {}: {} bytes",
                    range_index, size
                )
            }
        }
    }
}

impl CacheValidator {
    /// Create a new CacheValidator
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// Get the cache directory
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }

    // =========================================================================
    // Metadata Validation Methods (Requirement 5.1)
    // =========================================================================

    /// Validate metadata file consistency with error recovery
    ///
    /// This method attempts to parse a metadata file and validates its structure.
    /// It handles corrupted files gracefully by returning validation results
    /// rather than failing completely.
    ///
    /// # Arguments
    /// * `path` - Path to the metadata file to validate
    ///
    /// # Returns
    /// * `ValidationResult` - Contains validation status, error messages, and parsed metadata
    ///
    /// # Requirements
    /// Implements Requirement 5.1: Shared metadata validation logic
    pub async fn validate_metadata_file(&self, path: &Path) -> Result<ValidationResult> {
        debug!("Validating metadata file: {:?}", path);

        // Check if file exists
        if !path.exists() {
            return Ok(ValidationResult {
                is_valid: false,
                error_message: Some("File does not exist".to_string()),
                metadata: None,
            });
        }

        // Try to parse metadata with error recovery
        match self.parse_metadata_safe(path).await {
            Ok(Some(metadata)) => {
                // Additional validation checks
                if let Err(e) = self.validate_metadata_structure(&metadata) {
                    Ok(ValidationResult {
                        is_valid: false,
                        error_message: Some(format!("Structure validation failed: {}", e)),
                        metadata: Some(metadata),
                    })
                } else {
                    Ok(ValidationResult {
                        is_valid: true,
                        error_message: None,
                        metadata: Some(metadata),
                    })
                }
            }
            Ok(None) => Ok(ValidationResult {
                is_valid: false,
                error_message: Some("Failed to parse metadata".to_string()),
                metadata: None,
            }),
            Err(e) => Ok(ValidationResult {
                is_valid: false,
                error_message: Some(format!("Validation error: {}", e)),
                metadata: None,
            }),
        }
    }

    /// Parse metadata with error recovery
    ///
    /// Attempts to parse a metadata file, handling various error conditions gracefully.
    /// Returns None if parsing fails rather than propagating errors.
    ///
    /// # Arguments
    /// * `path` - Path to the metadata file
    ///
    /// # Returns
    /// * `Ok(Some(metadata))` - Successfully parsed metadata
    /// * `Ok(None)` - Parsing failed but error was handled
    /// * `Err(ProxyError)` - Unrecoverable error
    ///
    /// # Requirements
    /// Implements Requirement 5.1: Shared metadata validation logic
    pub async fn parse_metadata_safe(&self, path: &Path) -> Result<Option<NewCacheMetadata>> {
        // Read file content
        let content = match tokio::fs::read_to_string(path).await {
            Ok(content) => content,
            Err(e) => {
                warn!("Failed to read metadata file {:?}: {}", path, e);
                return Ok(None);
            }
        };

        // Parse JSON with error recovery
        match serde_json::from_str::<NewCacheMetadata>(&content) {
            Ok(metadata) => {
                debug!("Successfully parsed metadata file: {:?}", path);
                Ok(Some(metadata))
            }
            Err(e) => {
                warn!("Failed to parse metadata file {:?}: {}", path, e);
                Ok(None)
            }
        }
    }

    /// Validate the structure of parsed metadata
    fn validate_metadata_structure(&self, metadata: &NewCacheMetadata) -> Result<()> {
        // Check required fields
        if metadata.cache_key.is_empty() {
            return Err(ProxyError::CacheError(
                "Cache key cannot be empty".to_string(),
            ));
        }

        if metadata.object_metadata.etag.is_empty() {
            return Err(ProxyError::CacheError("ETag cannot be empty".to_string()));
        }

        // Validate ranges
        for (i, range) in metadata.ranges.iter().enumerate() {
            if range.start > range.end {
                return Err(ProxyError::CacheError(format!(
                    "Invalid range {}: start {} > end {}",
                    i, range.start, range.end
                )));
            }

            if range.compressed_size == 0 {
                return Err(ProxyError::CacheError(format!(
                    "Invalid range {}: compressed_size cannot be zero",
                    i
                )));
            }

            if range.file_path.is_empty() {
                return Err(ProxyError::CacheError(format!(
                    "Invalid range {}: file_path cannot be empty",
                    i
                )));
            }
        }

        Ok(())
    }

    // =========================================================================
    // Directory Scanning Methods (Requirement 5.2)
    // =========================================================================

    /// Scan directory with parallel processing
    ///
    /// Performs a parallel scan of the specified directory, collecting metadata files
    /// and processing them concurrently for improved performance.
    ///
    /// # Arguments
    /// * `dir` - Directory to scan
    ///
    /// # Returns
    /// * `Vec<PathBuf>` - List of metadata file paths found
    ///
    /// # Requirements
    /// Implements Requirement 5.2: Shared directory scanning with parallel processing
    pub async fn scan_directory_parallel(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let scan_start = Instant::now();
        debug!("Starting parallel directory scan: {:?}", dir);

        if !dir.exists() {
            debug!("Directory does not exist: {:?}", dir);
            return Ok(Vec::new());
        }

        // Collect all .meta files using walkdir
        let metadata_files: Vec<PathBuf> = WalkDir::new(dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|entry| {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        // Only include .meta files
                        if path.extension().is_some_and(|ext| ext == "meta") {
                            Some(path.to_path_buf())
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        warn!("Directory walk error: {}", e);
                        None
                    }
                }
            })
            .collect();

        let scan_duration = scan_start.elapsed();
        debug!(
            "Directory scan completed: {:?}, found {} metadata files in {:?}",
            dir,
            metadata_files.len(),
            scan_duration
        );

        Ok(metadata_files)
    }

    /// Scan and process metadata files in parallel
    ///
    /// Combines directory scanning with parallel metadata processing for maximum efficiency.
    /// This method is the primary entry point for coordinated cache initialization.
    ///
    /// # Arguments
    /// * `dir` - Directory to scan
    ///
    /// # Returns
    /// * `Vec<CacheFileResult>` - Results from processing each metadata file
    ///
    /// # Requirements
    /// Implements Requirement 5.2: Shared directory scanning with parallel processing
    pub async fn scan_and_process_parallel(&self, dir: &Path) -> Result<Vec<CacheFileResult>> {
        let scan_start = Instant::now();
        debug!("Starting parallel scan and process: {:?}", dir);

        // First, collect all metadata file paths
        let metadata_files = self.scan_directory_parallel(dir).await?;

        if metadata_files.is_empty() {
            debug!("No metadata files found in directory: {:?}", dir);
            return Ok(Vec::new());
        }

        // Process files in parallel using rayon
        let results: Vec<CacheFileResult> = metadata_files
            .par_iter()
            .map(|path| {
                // Use blocking I/O in the parallel context
                self.process_metadata_file_sync(path)
            })
            .collect();

        let total_duration = scan_start.elapsed();
        let valid_count = results.iter().filter(|r| r.metadata.is_some()).count();
        let error_count = results
            .iter()
            .filter(|r| !r.parse_errors.is_empty())
            .count();

        debug!(
            "Parallel scan and process completed: {:?}, processed {} files, {} valid, {} errors in {:?}",
            dir,
            results.len(),
            valid_count,
            error_count,
            total_duration
        );

        Ok(results)
    }

    /// Process a single metadata file synchronously (for use in parallel context)
    fn process_metadata_file_sync(&self, path: &PathBuf) -> CacheFileResult {
        let mut result = CacheFileResult {
            file_path: path.clone(),
            compressed_size: 0,
            is_write_cached: false,
            metadata: None,
            parse_errors: Vec::new(),
        };

        // Read and parse metadata file
        match std::fs::read_to_string(path) {
            Ok(content) => {
                match serde_json::from_str::<NewCacheMetadata>(&content) {
                    Ok(metadata) => {
                        // Calculate compressed size from ranges
                        result.compressed_size = self.calculate_compressed_size(&metadata.ranges);
                        result.is_write_cached = metadata.object_metadata.is_write_cached;
                        result.metadata = Some(metadata);
                    }
                    Err(e) => {
                        result.parse_errors.push(format!("JSON parse error: {}", e));
                    }
                }
            }
            Err(e) => {
                result.parse_errors.push(format!("File read error: {}", e));
            }
        }

        result
    }

    // =========================================================================
    // Size Calculation Methods (Requirement 5.3)
    // =========================================================================

    /// Calculate compressed size from ranges
    ///
    /// Sums up the compressed sizes of all ranges in a metadata entry.
    /// This provides the actual disk usage for the cached object.
    ///
    /// # Arguments
    /// * `ranges` - Vector of range specifications
    ///
    /// # Returns
    /// * `u64` - Total compressed size in bytes
    ///
    /// # Requirements
    /// Implements Requirement 5.3: Shared size calculation methods
    pub fn calculate_compressed_size(&self, ranges: &[RangeSpec]) -> u64 {
        ranges.iter().map(|range| range.compressed_size).sum()
    }

    /// Calculate uncompressed size from ranges
    ///
    /// Sums up the uncompressed sizes of all ranges in a metadata entry.
    /// This provides the original object size before compression.
    ///
    /// # Arguments
    /// * `ranges` - Vector of range specifications
    ///
    /// # Returns
    /// * `u64` - Total uncompressed size in bytes
    pub fn calculate_uncompressed_size(&self, ranges: &[RangeSpec]) -> u64 {
        ranges.iter().map(|range| range.uncompressed_size).sum()
    }

    /// Calculate compression ratio for ranges
    ///
    /// Computes the compression ratio as compressed_size / uncompressed_size.
    /// Returns 1.0 if uncompressed_size is 0 to avoid division by zero.
    ///
    /// # Arguments
    /// * `ranges` - Vector of range specifications
    ///
    /// # Returns
    /// * `f64` - Compression ratio (0.0 to 1.0, where lower is better compression)
    pub fn calculate_compression_ratio(&self, ranges: &[RangeSpec]) -> f64 {
        let compressed = self.calculate_compressed_size(ranges);
        let uncompressed = self.calculate_uncompressed_size(ranges);

        if uncompressed == 0 {
            1.0
        } else {
            compressed as f64 / uncompressed as f64
        }
    }

    // =========================================================================
    // File Filtering and Categorization Methods (Requirement 5.4)
    // =========================================================================

    /// Filter cache file results by write-cached status
    ///
    /// Returns only the cache file results that match the specified write-cached status.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results to filter
    /// * `is_write_cached` - Whether to include write-cached (true) or read-cached (false) files
    ///
    /// # Returns
    /// * `Vec<&CacheFileResult>` - Filtered results
    ///
    /// # Requirements
    /// Implements Requirement 5.4: Shared file filtering and categorization
    pub fn filter_by_write_cached<'a>(
        &self,
        results: &'a [CacheFileResult],
        is_write_cached: bool,
    ) -> Vec<&'a CacheFileResult> {
        results
            .iter()
            .filter(|result| result.is_write_cached == is_write_cached)
            .collect()
    }

    /// Filter cache file results by validity
    ///
    /// Returns only the cache file results that have successfully parsed metadata.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results to filter
    ///
    /// # Returns
    /// * `Vec<&CacheFileResult>` - Valid results only
    pub fn filter_valid_results<'a>(
        &self,
        results: &'a [CacheFileResult],
    ) -> Vec<&'a CacheFileResult> {
        results
            .iter()
            .filter(|result| result.metadata.is_some())
            .collect()
    }

    /// Filter cache file results by errors
    ///
    /// Returns only the cache file results that encountered parsing errors.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results to filter
    ///
    /// # Returns
    /// * `Vec<&CacheFileResult>` - Results with errors only
    pub fn filter_error_results<'a>(
        &self,
        results: &'a [CacheFileResult],
    ) -> Vec<&'a CacheFileResult> {
        results
            .iter()
            .filter(|result| !result.parse_errors.is_empty())
            .collect()
    }

    /// Categorize a cache file result
    ///
    /// Determines the category of a cache file based on its metadata and status.
    ///
    /// # Arguments
    /// * `result` - Cache file result to categorize
    ///
    /// # Returns
    /// * `CacheFileCategory` - Category of the cache file
    ///
    /// # Requirements
    /// Implements Requirement 5.4: Shared file filtering and categorization
    pub fn categorize_cache_file(&self, result: &CacheFileResult) -> CacheFileCategory {
        if result.metadata.is_none() || !result.parse_errors.is_empty() {
            CacheFileCategory::Invalid
        } else if result.is_write_cached {
            CacheFileCategory::WriteCached
        } else {
            CacheFileCategory::ReadCached
        }
    }

    /// Group cache file results by category
    ///
    /// Organizes cache file results into categories for easier processing by subsystems.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results to group
    ///
    /// # Returns
    /// * `(Vec<&CacheFileResult>, Vec<&CacheFileResult>, Vec<&CacheFileResult>)` -
    ///   Tuple of (write_cached, read_cached, invalid) results
    pub fn group_by_category<'a>(
        &self,
        results: &'a [CacheFileResult],
    ) -> (
        Vec<&'a CacheFileResult>,
        Vec<&'a CacheFileResult>,
        Vec<&'a CacheFileResult>,
    ) {
        let mut write_cached = Vec::new();
        let mut read_cached = Vec::new();
        let mut invalid = Vec::new();

        for result in results {
            match self.categorize_cache_file(result) {
                CacheFileCategory::WriteCached => write_cached.push(result),
                CacheFileCategory::ReadCached => read_cached.push(result),
                CacheFileCategory::Invalid => invalid.push(result),
            }
        }

        (write_cached, read_cached, invalid)
    }

    // =========================================================================
    // Advanced Size Calculation Methods (Requirement 5.3, 5.4)
    // =========================================================================

    /// Calculate size statistics for a collection of cache file results
    ///
    /// Provides comprehensive size statistics including totals, averages, and breakdowns.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results
    ///
    /// # Returns
    /// * `SizeStatistics` - Detailed size statistics
    pub fn calculate_size_statistics(&self, results: &[CacheFileResult]) -> SizeStatistics {
        let (write_cached, read_cached, invalid) = self.group_by_category(results);

        let total_compressed = self.calculate_total_size(results);
        let write_cached_size: u64 = write_cached.iter().map(|r| r.compressed_size).sum();
        let read_cached_size: u64 = read_cached.iter().map(|r| r.compressed_size).sum();

        let total_count = results.len();
        let write_cached_count = write_cached.len();
        let read_cached_count = read_cached.len();
        let invalid_count = invalid.len();

        let average_size = if total_count > 0 {
            total_compressed / total_count as u64
        } else {
            0
        };

        SizeStatistics {
            total_compressed_size: total_compressed,
            write_cached_size,
            read_cached_size,
            total_count,
            write_cached_count,
            read_cached_count,
            invalid_count,
            average_size,
        }
    }

    /// Calculate compression statistics for cache file results
    ///
    /// Analyzes compression effectiveness across all cached objects.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results
    ///
    /// # Returns
    /// * `CompressionStatistics` - Compression analysis results
    pub fn calculate_compression_statistics(
        &self,
        results: &[CacheFileResult],
    ) -> CompressionStatistics {
        let valid_results = self.filter_valid_results(results);

        if valid_results.is_empty() {
            return CompressionStatistics::default();
        }

        let mut total_compressed = 0u64;
        let mut total_uncompressed = 0u64;
        let mut compression_ratios = Vec::new();
        let objects_analyzed = valid_results.len();

        for result in &valid_results {
            if let Some(metadata) = &result.metadata {
                let compressed = self.calculate_compressed_size(&metadata.ranges);
                let uncompressed = self.calculate_uncompressed_size(&metadata.ranges);

                total_compressed += compressed;
                total_uncompressed += uncompressed;

                if uncompressed > 0 {
                    compression_ratios.push(compressed as f64 / uncompressed as f64);
                }
            }
        }

        let overall_ratio = if total_uncompressed > 0 {
            total_compressed as f64 / total_uncompressed as f64
        } else {
            1.0
        };

        let average_ratio = if !compression_ratios.is_empty() {
            compression_ratios.iter().sum::<f64>() / compression_ratios.len() as f64
        } else {
            1.0
        };

        let space_saved = total_uncompressed.saturating_sub(total_compressed);
        let space_saved_percentage = if total_uncompressed > 0 {
            (space_saved as f64 / total_uncompressed as f64) * 100.0
        } else {
            0.0
        };

        CompressionStatistics {
            total_compressed_size: total_compressed,
            total_uncompressed_size: total_uncompressed,
            overall_compression_ratio: overall_ratio,
            average_compression_ratio: average_ratio,
            space_saved_bytes: space_saved,
            space_saved_percentage,
            objects_analyzed,
        }
    }

    /// Filter cache file results by size range
    ///
    /// Returns cache file results within the specified size range.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results to filter
    /// * `min_size` - Minimum compressed size (inclusive)
    /// * `max_size` - Maximum compressed size (inclusive)
    ///
    /// # Returns
    /// * `Vec<&CacheFileResult>` - Filtered results within size range
    pub fn filter_by_size_range<'a>(
        &self,
        results: &'a [CacheFileResult],
        min_size: u64,
        max_size: u64,
    ) -> Vec<&'a CacheFileResult> {
        results
            .iter()
            .filter(|result| {
                result.compressed_size >= min_size && result.compressed_size <= max_size
            })
            .collect()
    }

    /// Find largest cache file results
    ///
    /// Returns the N largest cache file results by compressed size.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results
    /// * `count` - Number of largest results to return
    ///
    /// # Returns
    /// * `Vec<&CacheFileResult>` - Largest results, sorted by size descending
    pub fn find_largest_files<'a>(
        &self,
        results: &'a [CacheFileResult],
        count: usize,
    ) -> Vec<&'a CacheFileResult> {
        let mut sorted_results: Vec<&CacheFileResult> = results.iter().collect();
        sorted_results.sort_by_key(|b| std::cmp::Reverse(b.compressed_size));
        sorted_results.into_iter().take(count).collect()
    }

    /// Calculate size distribution histogram
    ///
    /// Creates a histogram of file sizes for analysis.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results
    /// * `bucket_count` - Number of histogram buckets
    ///
    /// # Returns
    /// * `Vec<SizeBucket>` - Histogram buckets with counts
    pub fn calculate_size_distribution(
        &self,
        results: &[CacheFileResult],
        bucket_count: usize,
    ) -> Vec<SizeBucket> {
        if results.is_empty() || bucket_count == 0 {
            return Vec::new();
        }

        let max_size = results.iter().map(|r| r.compressed_size).max().unwrap_or(0);
        if max_size == 0 {
            return Vec::new();
        }

        let bucket_size = max_size.div_ceil(bucket_count as u64); // Ceiling division
        let mut buckets = vec![
            SizeBucket {
                min_size: 0,
                max_size: 0,
                count: 0
            };
            bucket_count
        ];

        // Initialize bucket ranges
        for (i, bucket) in buckets.iter_mut().enumerate() {
            bucket.min_size = i as u64 * bucket_size;
            bucket.max_size = if i == bucket_count - 1 {
                max_size
            } else {
                (i + 1) as u64 * bucket_size - 1
            };
        }

        // Count files in each bucket
        for result in results {
            let bucket_index = result
                .compressed_size
                .checked_div(bucket_size)
                .map(|i| (i as usize).min(bucket_count - 1))
                .unwrap_or(0);
            buckets[bucket_index].count += 1;
        }

        buckets
    }

    // =========================================================================
    // Atomic Metadata Writes Validation Methods (Requirements 7.1, 7.2, 7.3, 7.4, 7.5)
    // =========================================================================

    /// Validate metadata integrity for atomic metadata writes
    ///
    /// Performs comprehensive validation including:
    /// - JSON structure validation
    /// - Required field verification
    /// - Range overlap detection
    /// - Size consistency validation
    /// - Timestamp validation
    ///
    /// **Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5**
    pub async fn validate_metadata_integrity(
        &self,
        metadata: &NewCacheMetadata,
    ) -> Result<MetadataIntegrityResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Validate JSON structure and required fields (Requirement 7.1)
        self.validate_json_structure_integrity(metadata, &mut errors);

        // Validate ranges (Requirement 7.2)
        let range_result = self.validate_ranges_integrity(&metadata.ranges).await?;
        errors.extend(range_result.errors);

        // Validate size consistency (Requirement 7.3)
        self.validate_size_consistency_integrity(metadata, &mut errors);

        // Validate timestamps (Requirement 7.4, 7.5)
        self.validate_timestamps_integrity(metadata, &mut errors, &mut warnings);

        // Check for suspicious patterns
        self.check_suspicious_patterns_integrity(metadata, &mut warnings);

        let is_valid = errors.is_empty();

        debug!(
            "Metadata integrity validation completed: key={}, valid={}, errors={}, warnings={}",
            metadata.cache_key,
            is_valid,
            errors.len(),
            warnings.len()
        );

        Ok(MetadataIntegrityResult {
            is_valid,
            errors,
            warnings,
        })
    }

    /// Validate range specifications for overlaps and consistency
    ///
    /// Checks for:
    /// - Range overlaps
    /// - Invalid byte positions
    /// - Size consistency within ranges
    ///
    /// **Validates: Requirements 7.2**
    pub async fn validate_ranges_integrity(
        &self,
        ranges: &[RangeSpec],
    ) -> Result<RangeIntegrityResult> {
        let mut errors = Vec::new();
        let mut overlapping_ranges = Vec::new();
        let mut size_mismatches = Vec::new();

        // Check for range overlaps
        for (i, range1) in ranges.iter().enumerate() {
            // Validate individual range
            if range1.start > range1.end {
                errors.push(MetadataIntegrityError::InvalidFieldValue {
                    field: format!("ranges[{}]", i),
                    reason: format!("start ({}) > end ({})", range1.start, range1.end),
                });
            }

            // Check size consistency within range
            let expected_size = range1.end - range1.start + 1;
            if range1.uncompressed_size != expected_size {
                size_mismatches.push(i);
                errors.push(MetadataIntegrityError::SizeInconsistency {
                    expected: expected_size,
                    actual: range1.uncompressed_size,
                });
            }

            // Check for overlaps with other ranges
            for (j, range2) in ranges.iter().enumerate().skip(i + 1) {
                if self.ranges_overlap_integrity(range1, range2) {
                    overlapping_ranges.push((i, j));
                    errors.push(MetadataIntegrityError::RangeOverlap {
                        range1_index: i,
                        range2_index: j,
                    });
                }
            }
        }

        let is_valid = errors.is_empty();

        debug!(
            "Range integrity validation completed: count={}, valid={}, overlaps={}, size_mismatches={}",
            ranges.len(),
            is_valid,
            overlapping_ranges.len(),
            size_mismatches.len()
        );

        Ok(RangeIntegrityResult {
            is_valid,
            overlapping_ranges,
            size_mismatches,
            errors,
        })
    }

    /// Validate consistency between an entry's metadata and its range files
    ///
    /// Checks that the `.meta` resolves and parses, and that every range it references
    /// exists on disk. It reports; it deletes and repairs nothing.
    ///
    /// # It does NOT look for orphaned range files, deliberately
    ///
    /// It used to claim to. `orphaned_range_files` was bound with `let`, never pushed to,
    /// returned in the result and logged with `.len()`, so the caller reported a count of
    /// zero orphans for every entry whatever was on disk. The field is gone rather than
    /// implemented, for three reasons:
    ///
    /// - **The job already exists.** `background_recovery::cleanup_orphan_bin_files`
    ///   walks `ranges/{bucket}/{XX}/{YYY}` and unlinks any `.bin` whose sibling `.meta`
    ///   is absent. A second orphan determination here would be a fourth reconstruction
    ///   of the cache layout, which is the class of mistake this function was part of.
    /// - **The cost is quadratic.** An orphan verdict for one entry needs a readdir of
    ///   its shard leaf, so doing it per validated entry is O(entries in shard) per entry
    ///   across a startup sweep that visits every entry. At this project's O(100M) range
    ///   design point that is not a slower correct check, it is an unusable one. An
    ///   orphan sweep has to be driven per shard, which is what the existing one does.
    /// - **A local verdict about a shared volume is unsafe to hand onward.** `cache_dir`
    ///   is shared storage. A `.bin` with no `.meta` from this instance's view may be a
    ///   peer's in-progress write, and a consumer that saw a populated list would be one
    ///   `remove_file` away from the deletion incidents already recorded in this
    ///   workstream.
    ///
    /// **Validates: Requirements 7.1, 7.2, 7.3**
    pub async fn validate_consistency_integrity(
        &self,
        cache_key: &str,
    ) -> Result<ConsistencyIntegrityResult> {
        let mut missing_range_files = Vec::new();
        let mut metadata_errors = Vec::new();

        // Resolve the metadata path through the production sharded helper. A cache key
        // the helper will not parse is reported as such rather than as unreadable JSON.
        let metadata_path = match self.get_metadata_path_integrity(cache_key) {
            Ok(path) => path,
            Err(e) => {
                metadata_errors.push(MetadataIntegrityError::InvalidFieldValue {
                    field: "cache_key".to_string(),
                    reason: format!("cannot resolve a metadata path for this key: {}", e),
                });
                return Ok(ConsistencyIntegrityResult {
                    is_consistent: false,
                    missing_range_files,
                    metadata_errors,
                });
            }
        };
        let metadata = match self.load_metadata_integrity(&metadata_path).await {
            Ok(meta) => meta,
            Err(e) => {
                metadata_errors.push(MetadataIntegrityError::InvalidJson(format!(
                    "Failed to load metadata: {}",
                    e
                )));
                return Ok(ConsistencyIntegrityResult {
                    is_consistent: false,
                    missing_range_files,
                    metadata_errors,
                });
            }
        };

        // Check that all ranges have corresponding files.
        //
        // `RangeSpec::file_path` is relative to `cache_dir/ranges`, not to `cache_dir`.
        // Every read site in `src/cache.rs` joins it that way, and
        // `cache_types::classify_new_range_as_staged`'s doc comment records that every
        // live write path derives it from `strip_prefix(cache_dir/ranges)`. This joined
        // `cache_dir` alone, so once the metadata path above was corrected the check
        // reported every range of every entry as missing — a second, independent instance
        // of the same mistake, hidden until then by the early return above it.
        let ranges_dir = self.cache_dir.join("ranges");
        for (i, range) in metadata.ranges.iter().enumerate() {
            let range_file_path = ranges_dir.join(&range.file_path);
            if !range_file_path.exists() {
                missing_range_files.push(range.file_path.clone());
                metadata_errors.push(MetadataIntegrityError::RangeFileMissing {
                    range_index: i,
                    file_path: range.file_path.clone(),
                });
            }
        }

        let is_consistent = metadata_errors.is_empty();

        debug!(
            "Consistency integrity validation completed: key={}, consistent={}, missing_files={}",
            cache_key,
            is_consistent,
            missing_range_files.len()
        );

        Ok(ConsistencyIntegrityResult {
            is_consistent,
            missing_range_files,
            metadata_errors,
        })
    }

    /// Validate JSON structure and required fields for integrity
    ///
    /// # `etag` and `last_modified` are NOT required, and requiring them counted a healthy cache as corrupt
    ///
    /// This used to push `MissingRequiredField` for an empty `object_metadata.etag` and
    /// for an empty `object_metadata.last_modified`. Neither is an invariant this codebase
    /// holds, and three production writers refute them:
    ///
    /// - **Every write-through PUT and `CompleteMultipartUpload` entry** carries an empty
    ///   `last_modified`. `S3Client::extract_object_metadata_from_response` reads the
    ///   header with `.unwrap_or_default()` and S3 sends no `Last-Modified` on a write
    ///   response, so the field is empty by construction and is learned later, from a
    ///   `304`, by `CacheManager::backfill_write_cache_last_modified`. Nothing may
    ///   synthesise a value (only one S3 actually returned is acceptable), so there is no
    ///   producer fix available — the requirement was simply wrong. That made the entire
    ///   write-cache population count one corruption per object.
    /// - **Any origin that omits `ETag`** yields an empty stored etag through the same
    ///   `.unwrap_or_default()`. `DiskCacheManager::check_object_expiration` records this
    ///   as "an ordinary, correctly cached object" for such an origin, which is exactly
    ///   why its own guard requires BOTH validators to be absent rather than testing the
    ///   etag alone.
    /// - **Every re-adopted orphaned range.**
    ///   `OrphanedRangeRecovery::add_range_to_metadata` publishes with
    ///   `object_metadata: None`, and `HybridMetadataWriter::load_or_create_metadata`'s
    ///   create-new branch fills that with `ObjectMetadata::default()` — empty etag, empty
    ///   `last_modified`, `content_length: 0` — while `add_or_update_range` attaches the
    ///   adopted range.
    ///
    /// What detects a document that genuinely lost its validators is the read path, not
    /// this sweep: `check_object_expiration`'s `no_validator_at_all` predicate refuses to
    /// serve an entry carrying neither an etag nor an effective `Last-Modified`, warns
    /// naming the key, and converges (the forwarded request re-caches under a real etag).
    /// Its `missing_write_cache_last_modified` predicate forces revalidation of a
    /// write-cached entry until the field is learned. A document that lost only one of the
    /// two is indistinguishable from a legitimate terse-origin or write-through entry, so
    /// no check here can separate them.
    ///
    /// # The length check is shape-aware on that same predicate
    ///
    /// `content_length == 0` beside a non-empty `ranges` is exactly what the re-adoption
    /// producer publishes, where the length is unknown by construction rather than wrong.
    /// The check now skips a document carrying no validator at all — the re-adoption
    /// signature, and the same condition `check_object_expiration` uses to refuse the
    /// serve — and stays live for every document an origin was actually observed to
    /// produce. `validate_size_consistency_integrity` independently reports the same shape,
    /// as over-coverage: a recorded length of 0 is exceeded by any range at all, and it
    /// carries the same `no_validator_at_all` guard, so the two agree on which documents
    /// are exempt.
    fn validate_json_structure_integrity(
        &self,
        metadata: &NewCacheMetadata,
        errors: &mut Vec<MetadataIntegrityError>,
    ) {
        // Check required fields
        if metadata.cache_key.is_empty() {
            errors.push(MetadataIntegrityError::MissingRequiredField(
                "cache_key".to_string(),
            ));
        }

        // The re-adoption signature: a document published with `ObjectMetadata::default()`
        // because no observed object metadata was available. Same predicate as
        // `DiskCacheManager::check_object_expiration`'s `no_validator_at_all`.
        let no_validator_at_all = metadata.object_metadata.etag.trim().is_empty()
            && metadata.object_metadata.effective_last_modified().is_none();

        // Validate field values
        if !no_validator_at_all
            && metadata.object_metadata.content_length == 0
            && !metadata.ranges.is_empty()
        {
            errors.push(MetadataIntegrityError::InvalidFieldValue {
                field: "object_metadata.content_length".to_string(),
                reason: "Content length is 0 but ranges exist".to_string(),
            });
        }
    }

    /// Report a cached entry whose ranges claim bytes the object does not have.
    ///
    /// That is the only size disagreement between `object_metadata` and `ranges` this
    /// cache can decide. Two arms that tried to decide more have been retired, and both
    /// fired on documents the tree's own writers publish deliberately.
    ///
    /// # Retired: empty `ranges` required a zero `content_length`
    ///
    /// It reported `SizeInconsistency { expected: 0, .. }` for every entry with no ranges
    /// and a non-zero length. Empty `ranges` in this cache means "no body data cached",
    /// **not** "the object is empty" — which is how `stale_head_only_entry` in
    /// `cache_initialization_coordinator` defines the shape too — and
    /// `CacheManager::create_head_only_metadata` publishes exactly that: `ranges:
    /// Vec::new()` beside the object's real `content_length`, renamed into place itself.
    /// So the arm fired once per HEAD-only entry on the volume.
    ///
    /// It was not made shape-aware, because there is no second shape to keep it for. The
    /// only wrong version — an entry whose `ranges` array was emptied by a bad write while
    /// a real length survived — is byte-identical to the legitimate one, so nothing here
    /// can separate them. **What detects it instead**: nothing needs to.
    /// `find_cached_ranges` reports no coverage for such an entry, so the request forwards
    /// to S3 and the entry is re-cached — the condition is unobservable to this sweep and
    /// self-correcting on the read path.
    ///
    /// # Retired: a 0-anchored entry required complete coverage
    ///
    /// It required `content_length == last_range.end + 1` whenever
    /// `ranges.first().start == 0`. Partial caching is ordinary here:
    /// `RangeHandler::store_range_new_storage` stores one fetched range while
    /// `S3Client::extract_object_metadata_from_response` takes `content_length` from
    /// `Content-Range`'s **total** — its call site in `http_proxy` carries a comment saying
    /// that total must not be overridden — so a 4 KiB range cached out of a 1 MiB object
    /// reported `expected: 4096, actual: 1048576`. Any client whose first read starts at
    /// offset 0 produces it: a header read, or a transfer client's first chunk.
    ///
    /// **Under-coverage cannot be made correct, so it is gone rather than narrowed.** A
    /// partially cached object and a complete object that lost a range record are the same
    /// document: `NewCacheMetadata` carries no claim of completeness, so "1 MiB recorded,
    /// 512 KiB covered" is both shapes at once. **What detects a truncated entry instead**:
    /// per-range truncation is `validate_ranges_integrity`'s (`uncompressed_size` must
    /// equal `end - start + 1`), a vanished range file is
    /// `validate_consistency_integrity`'s, and a missing range is simply a miss on the read
    /// path — `find_cached_ranges` reports the gap and the request fetches it.
    ///
    /// # What is kept
    ///
    /// Over-coverage, spelled the way the tree already spells it:
    /// `RangeHandler::validate_range_request` rejects a request whose
    /// `range.end >= content_length` as "Range end exceeds content length". No writer can
    /// produce it — that check rejects it on the request path, and
    /// `DiskCacheManager::store_range` additionally clamps `end` to the bytes actually
    /// received — so it is real corruption. The retired arm's `first.start == 0` gate goes
    /// with the equality, which makes this strictly wider at the condition that is
    /// decidable: over-coverage is corruption wherever coverage begins.
    ///
    /// `no_validator_at_all` is the same guard, spelled the same way, that
    /// `validate_json_structure_integrity` and `DiskCacheManager::check_object_expiration`
    /// use. A document the orphan sweep re-adopted holds `ObjectMetadata::default()`, whose
    /// `content_length: 0` is unknown rather than zero, and every range in it would
    /// otherwise read as over-coverage.
    ///
    /// # `total_range_size` is diagnostic, NOT the comparison
    ///
    /// The retired code summed `uncompressed_size` across ranges and then logged the sum
    /// without comparing it, and the arm's comment ("total range size should match content
    /// length") reads as though the sum was meant to be the comparison. It was not usable
    /// as one: the sum equals `content_length` only for coverage that is both complete
    /// **and** gapless, so it would fire on every partially cached object regardless of
    /// where coverage starts — a strictly larger false-positive population than the
    /// equality it sits beside. It stays in the `debug!` because a gap is visible in the
    /// difference between it and the covered span, which is worth having in a log and is
    /// not a verdict.
    fn validate_size_consistency_integrity(
        &self,
        metadata: &NewCacheMetadata,
        errors: &mut Vec<MetadataIntegrityError>,
    ) {
        // Same predicate, same spelling, as `validate_json_structure_integrity` and
        // `DiskCacheManager::check_object_expiration`: a document no origin metadata was
        // ever observed for has an unknown length, not a length of zero.
        let no_validator_at_all = metadata.object_metadata.etag.trim().is_empty()
            && metadata.object_metadata.effective_last_modified().is_none();

        let total_range_size: u64 = metadata.ranges.iter().map(|r| r.uncompressed_size).sum();

        if let Some(last_range) = metadata.ranges.last() {
            if !no_validator_at_all && last_range.end >= metadata.object_metadata.content_length {
                errors.push(MetadataIntegrityError::SizeInconsistency {
                    // Field convention unchanged: `expected` is the figure derived from
                    // the ranges, `actual` the length recorded on the object.
                    expected: last_range.end + 1,
                    actual: metadata.object_metadata.content_length,
                });
            }
        }

        debug!(
            "Size integrity validation: content_length={}, total_range_size={}, ranges={}",
            metadata.object_metadata.content_length,
            total_range_size,
            metadata.ranges.len()
        );
    }

    /// Validate timestamps for reasonableness in integrity checks
    fn validate_timestamps_integrity(
        &self,
        metadata: &NewCacheMetadata,
        errors: &mut Vec<MetadataIntegrityError>,
        warnings: &mut Vec<MetadataIntegrityWarning>,
    ) {
        use std::time::{Duration, SystemTime};

        let now = SystemTime::now();
        let one_year_ago = now - Duration::from_secs(365 * 24 * 3600);
        let one_day_future = now + Duration::from_secs(24 * 3600);

        // Validate created_at
        if metadata.created_at < one_year_ago {
            warnings.push(MetadataIntegrityWarning::SuspiciousTimestamp {
                field: "created_at".to_string(),
                reason: "Timestamp is more than 1 year old".to_string(),
            });
        }

        if metadata.created_at > one_day_future {
            errors.push(MetadataIntegrityError::InvalidTimestamp {
                field: "created_at".to_string(),
                timestamp: format!("{:?}", metadata.created_at),
            });
        }

        // Validate expires_at
        if metadata.expires_at < metadata.created_at {
            errors.push(MetadataIntegrityError::InvalidTimestamp {
                field: "expires_at".to_string(),
                timestamp: "expires_at is before created_at".to_string(),
            });
        }

        // Validate range timestamps
        for (i, range) in metadata.ranges.iter().enumerate() {
            if range.created_at > one_day_future {
                errors.push(MetadataIntegrityError::InvalidTimestamp {
                    field: format!("ranges[{}].created_at", i),
                    timestamp: format!("{:?}", range.created_at),
                });
            }

            if range.last_accessed > now + Duration::from_secs(60) {
                warnings.push(MetadataIntegrityWarning::SuspiciousTimestamp {
                    field: format!("ranges[{}].last_accessed", i),
                    reason: "Last accessed time is in the future".to_string(),
                });
            }
        }
    }

    /// Check for suspicious patterns that might indicate issues in integrity validation
    fn check_suspicious_patterns_integrity(
        &self,
        metadata: &NewCacheMetadata,
        warnings: &mut Vec<MetadataIntegrityWarning>,
    ) {
        // Warn about large number of ranges
        if metadata.ranges.len() > 1000 {
            warnings.push(MetadataIntegrityWarning::LargeRangeCount {
                count: metadata.ranges.len(),
            });
        }

        // Warn about unusually small or large ranges
        for (i, range) in metadata.ranges.iter().enumerate() {
            let range_size = range.end - range.start + 1;

            // Very small ranges (< 1KB) might indicate fragmentation
            if range_size < 1024 {
                warnings.push(MetadataIntegrityWarning::UnusualRangeSize {
                    range_index: i,
                    size: range_size,
                });
            }

            // Very large ranges (> 1GB) might indicate issues
            if range_size > 1024 * 1024 * 1024 {
                warnings.push(MetadataIntegrityWarning::UnusualRangeSize {
                    range_index: i,
                    size: range_size,
                });
            }
        }
    }

    /// Check if two ranges overlap for integrity validation
    fn ranges_overlap_integrity(&self, range1: &RangeSpec, range2: &RangeSpec) -> bool {
        // Ranges overlap if one starts before the other ends
        !(range1.end < range2.start || range2.end < range1.start)
    }

    /// Get metadata file path for a cache key in integrity validation
    ///
    /// Delegates to `disk_cache::get_sharded_path`, the single production helper every
    /// `.meta` producer uses (`HybridMetadataWriter::get_metadata_file_path`,
    /// `JournalConsolidator::get_metadata_file_path`,
    /// `WriteCacheManager::get_metadata_path`).
    ///
    /// # This used to build its own path, and so validated nothing
    ///
    /// It hand-rolled `metadata/{cache_key with / : \ replaced by _}.meta` — flat, and
    /// with a sanitizer that is not the one production uses either (production
    /// percent-encodes). Production writes
    /// `metadata/{bucket}/{XX}/{YYY}/{percent-encoded key}.meta`, where `XX`/`YYY` come
    /// from BLAKE3 over the **object key alone**. So `load_metadata_integrity` took its
    /// `Err` arm for every entry, `validate_consistency_integrity` reported
    /// `is_consistent: false` with an `InvalidJson` error for a file it had never
    /// opened, and `CacheInitializationCoordinator::perform_metadata_consistency_validation`
    /// counted a corruption per entry and logged two zero counts beside it.
    ///
    /// The flat layout it built was never correct. `get_sharded_path` landed before this
    /// helper was written, and even the pre-sharding flat layout — `metadata/{key}.meta`
    /// directly under the metadata root, whose builder has since been deleted —
    /// percent-encodes the key rather than replacing separators with `_`.
    ///
    /// Returning `Result` rather than swallowing the parse failure is deliberate: a
    /// malformed cache key is a different condition from a missing or corrupt file, and
    /// reporting both as `InvalidJson` is what made the original failure mode
    /// unreadable.
    fn get_metadata_path_integrity(&self, cache_key: &str) -> Result<PathBuf> {
        crate::disk_cache::get_sharded_path(&self.cache_dir.join("metadata"), cache_key, ".meta")
    }

    /// Load metadata from file for integrity validation
    async fn load_metadata_integrity(&self, path: &PathBuf) -> Result<NewCacheMetadata> {
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to read metadata file: {}", e)))?;

        serde_json::from_str(&content)
            .map_err(|e| ProxyError::CacheError(format!("Failed to parse metadata JSON: {}", e)))
    }

    // =========================================================================
    // Utility Methods
    // =========================================================================

    /// Calculate total size for a collection of cache file results
    ///
    /// Sums up the compressed sizes of all valid cache file results.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results
    ///
    /// # Returns
    /// * `u64` - Total compressed size in bytes
    pub fn calculate_total_size(&self, results: &[CacheFileResult]) -> u64 {
        results.iter().map(|result| result.compressed_size).sum()
    }

    /// Count cache file results by category
    ///
    /// Returns counts of cache files in each category.
    ///
    /// # Arguments
    /// * `results` - Vector of cache file results
    ///
    /// # Returns
    /// * `(usize, usize, usize)` - Tuple of (write_cached_count, read_cached_count, invalid_count)
    pub fn count_by_category(&self, results: &[CacheFileResult]) -> (usize, usize, usize) {
        let (write_cached, read_cached, invalid) = self.group_by_category(results);
        (write_cached.len(), read_cached.len(), invalid.len())
    }

    /// Format size in human-readable format
    ///
    /// Converts byte counts to human-readable format (B, KiB, MiB, GiB, TiB).
    ///
    /// # Arguments
    /// * `bytes` - Size in bytes
    ///
    /// # Returns
    /// * `String` - Human-readable size string
    pub fn format_size_human(&self, bytes: u64) -> String {
        const KIB: u64 = 1024;
        const MIB: u64 = KIB * 1024;
        const GIB: u64 = MIB * 1024;
        const TIB: u64 = GIB * 1024;

        if bytes >= TIB {
            format!("{:.1} TiB", bytes as f64 / TIB as f64)
        } else if bytes >= GIB {
            format!("{:.1} GiB", bytes as f64 / GIB as f64)
        } else if bytes >= MIB {
            format!("{:.1} MiB", bytes as f64 / MIB as f64)
        } else if bytes >= KIB {
            format!("{:.1} KiB", bytes as f64 / KIB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    /// Format duration in human-readable format
    ///
    /// Converts duration to human-readable format (ms, s, m, h).
    ///
    /// # Arguments
    /// * `duration` - Duration to format
    ///
    /// # Returns
    /// * `String` - Human-readable duration string
    pub fn format_duration_human(&self, duration: Duration) -> String {
        let total_secs = duration.as_secs();
        let millis = duration.as_millis();

        if total_secs >= 3600 {
            let hours = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            format!("{}h {}m", hours, mins)
        } else if total_secs >= 60 {
            let mins = total_secs / 60;
            let secs = total_secs % 60;
            format!("{}m {}s", mins, secs)
        } else if total_secs > 0 {
            format!("{}s", total_secs)
        } else {
            format!("{}ms", millis)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::{ObjectMetadata, RangeSpec};
    use crate::compression::CompressionAlgorithm;
    use std::time::SystemTime;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_cache_validator_new() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        let validator = CacheValidator::new(cache_dir.clone());
        assert_eq!(validator.cache_dir(), &cache_dir);
    }

    #[tokio::test]
    async fn test_validate_metadata_file_not_exists() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let non_existent_path = temp_dir.path().join("non_existent.meta");
        let result = validator
            .validate_metadata_file(&non_existent_path)
            .await
            .unwrap();

        assert!(!result.is_valid);
        assert!(result.error_message.is_some());
        assert!(result.metadata.is_none());
    }

    #[tokio::test]
    async fn test_parse_metadata_safe_invalid_json() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        // Create a file with invalid JSON
        let invalid_file = temp_dir.path().join("invalid.meta");
        tokio::fs::write(&invalid_file, "invalid json content")
            .await
            .unwrap();

        let result = validator.parse_metadata_safe(&invalid_file).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_scan_directory_parallel_empty() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir.clone());

        let results = validator.scan_directory_parallel(&cache_dir).await.unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_calculate_compressed_size() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let ranges = vec![
            RangeSpec {
                start: 0,
                end: 1023,
                file_path: "test1.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: 512,
                uncompressed_size: 1024,
                created_at: SystemTime::now(),
                last_accessed: SystemTime::now(),
                access_count: 1,
                staged: None,
                unservable: false,
                file_identity: None,
            },
            RangeSpec {
                start: 1024,
                end: 2047,
                file_path: "test2.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: 256,
                uncompressed_size: 1024,
                created_at: SystemTime::now(),
                last_accessed: SystemTime::now(),
                access_count: 1,
                staged: None,
                unservable: false,
                file_identity: None,
            },
        ];

        let total_compressed = validator.calculate_compressed_size(&ranges);
        assert_eq!(total_compressed, 768); // 512 + 256

        let total_uncompressed = validator.calculate_uncompressed_size(&ranges);
        assert_eq!(total_uncompressed, 2048); // 1024 + 1024

        let ratio = validator.calculate_compression_ratio(&ranges);
        assert_eq!(ratio, 0.375); // 768 / 2048
    }

    #[test]
    fn test_filter_by_write_cached() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let results = vec![
            CacheFileResult {
                file_path: PathBuf::from("write1.meta"),
                compressed_size: 1024,
                is_write_cached: true,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("read1.meta"),
                compressed_size: 2048,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("write2.meta"),
                compressed_size: 512,
                is_write_cached: true,
                metadata: None,
                parse_errors: Vec::new(),
            },
        ];

        let write_cached = validator.filter_by_write_cached(&results, true);
        assert_eq!(write_cached.len(), 2);

        let read_cached = validator.filter_by_write_cached(&results, false);
        assert_eq!(read_cached.len(), 1);
    }

    #[test]
    fn test_categorize_cache_file() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        // Valid write-cached file
        let write_cached = CacheFileResult {
            file_path: PathBuf::from("write.meta"),
            compressed_size: 1024,
            is_write_cached: true,
            metadata: Some(NewCacheMetadata {
                cache_key: "test".to_string(),
                object_metadata: ObjectMetadata::default(),
                ranges: Vec::new(),
                created_at: SystemTime::now(),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
                compression_info: Default::default(),
                ..Default::default()
            }),
            parse_errors: Vec::new(),
        };
        assert_eq!(
            validator.categorize_cache_file(&write_cached),
            CacheFileCategory::WriteCached
        );

        // Valid read-cached file
        let read_cached = CacheFileResult {
            file_path: PathBuf::from("read.meta"),
            compressed_size: 1024,
            is_write_cached: false,
            metadata: Some(NewCacheMetadata {
                cache_key: "test".to_string(),
                object_metadata: ObjectMetadata::default(),
                ranges: Vec::new(),
                created_at: SystemTime::now(),
                expires_at: SystemTime::now() + Duration::from_secs(3600),
                compression_info: Default::default(),
                ..Default::default()
            }),
            parse_errors: Vec::new(),
        };
        assert_eq!(
            validator.categorize_cache_file(&read_cached),
            CacheFileCategory::ReadCached
        );

        // Invalid file
        let invalid = CacheFileResult {
            file_path: PathBuf::from("invalid.meta"),
            compressed_size: 0,
            is_write_cached: false,
            metadata: None,
            parse_errors: vec!["Parse error".to_string()],
        };
        assert_eq!(
            validator.categorize_cache_file(&invalid),
            CacheFileCategory::Invalid
        );
    }

    #[test]
    fn test_format_size_human() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        assert_eq!(validator.format_size_human(512), "512 B");
        assert_eq!(validator.format_size_human(1024), "1.0 KiB");
        assert_eq!(validator.format_size_human(1536), "1.5 KiB");
        assert_eq!(validator.format_size_human(1024 * 1024), "1.0 MiB");
        assert_eq!(validator.format_size_human(1024 * 1024 * 1024), "1.0 GiB");
        assert_eq!(validator.format_size_human(1024_u64.pow(4)), "1.0 TiB");
    }

    #[test]
    fn test_format_duration_human() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        assert_eq!(
            validator.format_duration_human(Duration::from_millis(500)),
            "500ms"
        );
        assert_eq!(
            validator.format_duration_human(Duration::from_secs(30)),
            "30s"
        );
        assert_eq!(
            validator.format_duration_human(Duration::from_secs(90)),
            "1m 30s"
        );
        assert_eq!(
            validator.format_duration_human(Duration::from_secs(3661)),
            "1h 1m"
        );
    }

    #[test]
    fn test_calculate_size_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let results = vec![
            CacheFileResult {
                file_path: PathBuf::from("write1.meta"),
                compressed_size: 1024,
                is_write_cached: true,
                metadata: Some(NewCacheMetadata {
                    cache_key: "test1".to_string(),
                    object_metadata: ObjectMetadata::default(),
                    ranges: Vec::new(),
                    created_at: SystemTime::now(),
                    expires_at: SystemTime::now() + Duration::from_secs(3600),
                    compression_info: Default::default(),
                    ..Default::default()
                }),
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("read1.meta"),
                compressed_size: 2048,
                is_write_cached: false,
                metadata: Some(NewCacheMetadata {
                    cache_key: "test2".to_string(),
                    object_metadata: ObjectMetadata::default(),
                    ranges: Vec::new(),
                    created_at: SystemTime::now(),
                    expires_at: SystemTime::now() + Duration::from_secs(3600),
                    compression_info: Default::default(),
                    ..Default::default()
                }),
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("invalid.meta"),
                compressed_size: 0,
                is_write_cached: false,
                metadata: None,
                parse_errors: vec!["Parse error".to_string()],
            },
        ];

        let stats = validator.calculate_size_statistics(&results);

        assert_eq!(stats.total_compressed_size, 3072); // 1024 + 2048 + 0
        assert_eq!(stats.write_cached_size, 1024);
        assert_eq!(stats.read_cached_size, 2048);
        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.write_cached_count, 1);
        assert_eq!(stats.read_cached_count, 1);
        assert_eq!(stats.invalid_count, 1);
        assert_eq!(stats.average_size, 1024); // 3072 / 3
    }

    #[test]
    fn test_filter_by_size_range() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let results = vec![
            CacheFileResult {
                file_path: PathBuf::from("small.meta"),
                compressed_size: 512,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("medium.meta"),
                compressed_size: 1024,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("large.meta"),
                compressed_size: 2048,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
        ];

        let filtered = validator.filter_by_size_range(&results, 1000, 1500);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].compressed_size, 1024);
    }

    #[test]
    fn test_find_largest_files() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let results = vec![
            CacheFileResult {
                file_path: PathBuf::from("small.meta"),
                compressed_size: 512,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("large.meta"),
                compressed_size: 2048,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("medium.meta"),
                compressed_size: 1024,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
        ];

        let largest = validator.find_largest_files(&results, 2);
        assert_eq!(largest.len(), 2);
        assert_eq!(largest[0].compressed_size, 2048); // Largest first
        assert_eq!(largest[1].compressed_size, 1024); // Second largest
    }

    #[test]
    fn test_calculate_size_distribution() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let validator = CacheValidator::new(cache_dir);

        let results = vec![
            CacheFileResult {
                file_path: PathBuf::from("file1.meta"),
                compressed_size: 100,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("file2.meta"),
                compressed_size: 250,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("file3.meta"),
                compressed_size: 400,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
            CacheFileResult {
                file_path: PathBuf::from("file4.meta"),
                compressed_size: 500,
                is_write_cached: false,
                metadata: None,
                parse_errors: Vec::new(),
            },
        ];

        let distribution = validator.calculate_size_distribution(&results, 2);
        assert_eq!(distribution.len(), 2);

        // First bucket: 0-249 (should contain files with size 100)
        assert_eq!(distribution[0].min_size, 0);
        assert_eq!(distribution[0].max_size, 249);
        assert_eq!(distribution[0].count, 1);

        // Second bucket: 250-500 (should contain files with sizes 250, 400, 500)
        assert_eq!(distribution[1].min_size, 250);
        assert_eq!(distribution[1].max_size, 500);
        assert_eq!(distribution[1].count, 3);
    }
}
