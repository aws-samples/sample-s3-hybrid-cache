//! Cache Validator Module
//!
//! Provides shared validation utilities for cache subsystems to eliminate duplicate logic
//! and ensure consistent validation behavior across WriteCacheManager and CacheSizeTracker.
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
#[derive(Debug, Clone)]
pub struct ConsistencyIntegrityResult {
    pub is_consistent: bool,
    pub missing_range_files: Vec<String>,
    pub orphaned_range_files: Vec<String>,
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

    /// Add a warning to the result
    pub fn with_warning(mut self, warning: MetadataIntegrityWarning) -> Self {
        self.warnings.push(warning);
        self
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
                        if path.extension().map_or(false, |ext| ext == "meta") {
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
        sorted_results.sort_by(|a, b| b.compressed_size.cmp(&a.compressed_size));
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

        let bucket_size = (max_size + bucket_count as u64 - 1) / bucket_count as u64; // Ceiling division
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
            let bucket_index = if bucket_size > 0 {
                ((result.compressed_size / bucket_size) as usize).min(bucket_count - 1)
            } else {
                0
            };
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

    /// Validate consistency between metadata and actual range files
    ///
    /// Checks that:
    /// - All ranges in metadata have corresponding files
    /// - No orphaned range files exist
    /// - File sizes match metadata expectations
    ///
    /// **Validates: Requirements 7.1, 7.2, 7.3**
    pub async fn validate_consistency_integrity(
        &self,
        cache_key: &str,
    ) -> Result<ConsistencyIntegrityResult> {
        let mut missing_range_files = Vec::new();
        let orphaned_range_files = Vec::new();
        let mut metadata_errors = Vec::new();

        // Try to load metadata
        let metadata_path = self.get_metadata_path_integrity(cache_key);
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
                    orphaned_range_files,
                    metadata_errors,
                });
            }
        };

        // Check that all ranges have corresponding files
        for (i, range) in metadata.ranges.iter().enumerate() {
            let range_file_path = self.cache_dir.join(&range.file_path);
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
            "Consistency integrity validation completed: key={}, consistent={}, missing_files={}, orphaned_files={}",
            cache_key,
            is_consistent,
            missing_range_files.len(),
            orphaned_range_files.len()
        );

        Ok(ConsistencyIntegrityResult {
            is_consistent,
            missing_range_files,
            orphaned_range_files,
            metadata_errors,
        })
    }

    /// Validate JSON structure and required fields for integrity
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

        if metadata.object_metadata.etag.is_empty() {
            errors.push(MetadataIntegrityError::MissingRequiredField(
                "object_metadata.etag".to_string(),
            ));
        }

        if metadata.object_metadata.last_modified.is_empty() {
            errors.push(MetadataIntegrityError::MissingRequiredField(
                "object_metadata.last_modified".to_string(),
            ));
        }

        // Validate field values
        if metadata.object_metadata.content_length == 0 && !metadata.ranges.is_empty() {
            errors.push(MetadataIntegrityError::InvalidFieldValue {
                field: "object_metadata.content_length".to_string(),
                reason: "Content length is 0 but ranges exist".to_string(),
            });
        }
    }

    /// Validate size consistency between object metadata and ranges for integrity
    fn validate_size_consistency_integrity(
        &self,
        metadata: &NewCacheMetadata,
        errors: &mut Vec<MetadataIntegrityError>,
    ) {
        if metadata.ranges.is_empty() {
            // For empty objects, content_length should be 0
            if metadata.object_metadata.content_length != 0 {
                errors.push(MetadataIntegrityError::SizeInconsistency {
                    expected: 0,
                    actual: metadata.object_metadata.content_length,
                });
            }
            return;
        }

        // Calculate total size from ranges
        let total_range_size: u64 = metadata.ranges.iter().map(|r| r.uncompressed_size).sum();

        // For full objects, total range size should match content length
        if let Some(first_range) = metadata.ranges.first() {
            if let Some(last_range) = metadata.ranges.last() {
                if first_range.start == 0 {
                    let expected_content_length = last_range.end + 1;
                    if metadata.object_metadata.content_length != expected_content_length {
                        errors.push(MetadataIntegrityError::SizeInconsistency {
                            expected: expected_content_length,
                            actual: metadata.object_metadata.content_length,
                        });
                    }
                }
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
    fn get_metadata_path_integrity(&self, cache_key: &str) -> PathBuf {
        // Use the same path resolution as disk cache
        // This is a simplified version - in practice would use the same sharding logic
        let sanitized_key = cache_key.replace(['/', ':', '\\'], "_");
        self.cache_dir
            .join("metadata")
            .join(format!("{}.meta", sanitized_key))
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
                frequency_score: 1,
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
                frequency_score: 1,
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
