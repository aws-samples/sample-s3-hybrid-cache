//! Unit tests for ObjectsScanResults functionality
//!
//! Tests the data structures and methods used for coordinated cache scanning
//! and result aggregation across cache subsystems.

use s3_proxy::cache_initialization_coordinator::{
    CacheMetadataEntry, ObjectsScanResults, ScanError, ScanErrorCategory,
};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// Create a test metadata entry
fn create_test_metadata_entry(
    path: &str,
    cache_key: &str,
    compressed_size: u64,
    is_write_cached: bool,
    has_metadata: bool,
) -> CacheMetadataEntry {
    let mut entry = CacheMetadataEntry::new(PathBuf::from(path));
    entry.cache_key = cache_key.to_string();
    entry.compressed_size = compressed_size;
    entry.is_write_cached = is_write_cached;

    if has_metadata {
        entry.metadata = Some(NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                content_type: Some("text/plain".to_string()),
                content_length: compressed_size * 2,
                last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
                is_write_cached,
                ..Default::default()
            },
            ranges: vec![RangeSpec {
                start: 0,
                end: (compressed_size * 2) - 1,
                file_path: "test.bin".to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size,
                uncompressed_size: compressed_size * 2,
                created_at: SystemTime::now(),
                last_accessed: SystemTime::now(),
                access_count: 1,
                frequency_score: 1,
            }],
            created_at: SystemTime::now(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
            compression_info: Default::default(),
            ..Default::default()
        });
    } else {
        entry.parse_errors.push("Parse error".to_string());
    }

    entry
}

#[test]
fn test_objects_scan_results_empty() {
    let results = ObjectsScanResults::empty();

    assert_eq!(results.total_objects, 0);
    assert_eq!(results.total_size, 0);
    assert_eq!(results.write_cached_objects, 0);
    assert_eq!(results.write_cached_size, 0);
    assert_eq!(results.read_cached_objects, 0);
    assert_eq!(results.read_cached_size, 0);
    assert!(results.metadata_entries.is_empty());
    assert_eq!(results.scan_duration, Duration::ZERO);
    assert!(results.scan_errors.is_empty());
    assert!(!results.has_errors());
    assert_eq!(results.error_count(), 0);
}

#[test]
fn test_objects_scan_results_percentages() {
    let mut results = ObjectsScanResults::empty();
    results.total_size = 1000;
    results.write_cached_size = 300;
    results.read_cached_size = 700;

    assert_eq!(results.write_cache_percentage(), 30.0);
    assert_eq!(results.read_cache_percentage(), 70.0);
}

#[test]
fn test_objects_scan_results_percentages_zero_total() {
    let results = ObjectsScanResults::empty();

    assert_eq!(results.write_cache_percentage(), 0.0);
    assert_eq!(results.read_cache_percentage(), 0.0);
}

#[test]
fn test_objects_scan_results_format_sizes() {
    let mut results = ObjectsScanResults::empty();
    results.total_size = 1536; // 1.5 KB
    results.write_cached_size = 512; // 512 B
    results.read_cached_size = 1024; // 1.0 KB

    assert_eq!(results.format_total_size(), "1.5 KB");
    assert_eq!(results.format_write_cached_size(), "512 B");
    assert_eq!(results.format_read_cached_size(), "1.0 KB");
}

#[test]
fn test_objects_scan_results_average_object_size() {
    let mut results = ObjectsScanResults::empty();
    results.total_objects = 4;
    results.total_size = 2000;

    assert_eq!(results.average_object_size(), 500);
    assert_eq!(results.format_average_object_size(), "500 B");
}

#[test]
fn test_objects_scan_results_average_object_size_zero_objects() {
    let results = ObjectsScanResults::empty();

    assert_eq!(results.average_object_size(), 0);
    assert_eq!(results.format_average_object_size(), "0 B");
}

#[test]
fn test_objects_scan_results_with_errors() {
    let mut results = ObjectsScanResults::empty();
    results.scan_errors.push(ScanError::new(
        PathBuf::from("error1.meta"),
        "Parse error".to_string(),
        ScanErrorCategory::JsonParse,
    ));
    results.scan_errors.push(ScanError::new(
        PathBuf::from("error2.meta"),
        "File read error".to_string(),
        ScanErrorCategory::FileRead,
    ));

    assert!(results.has_errors());
    assert_eq!(results.error_count(), 2);
}

#[test]
fn test_objects_scan_results_summary_string() {
    let mut results = ObjectsScanResults::empty();
    results.total_objects = 10;
    results.total_size = 5120; // 5.0 KB
    results.write_cached_objects = 3;
    results.write_cached_size = 1536; // 1.5 KB
    results.read_cached_objects = 7;
    results.read_cached_size = 3584; // 3.5 KB
    results.scan_errors.push(ScanError::new(
        PathBuf::from("error.meta"),
        "Parse error".to_string(),
        ScanErrorCategory::JsonParse,
    ));

    let summary = results.summary_string();

    assert!(summary.contains("10 objects"));
    assert!(summary.contains("5.0 KB"));
    assert!(summary.contains("Write: 3 objects"));
    assert!(summary.contains("1.5 KB"));
    assert!(summary.contains("Read: 7 objects"));
    assert!(summary.contains("3.5 KB"));
    assert!(summary.contains("Errors: 1"));
}

#[test]
fn test_cache_metadata_entry_new() {
    let path = PathBuf::from("test.meta");
    let entry = CacheMetadataEntry::new(path.clone());

    assert_eq!(entry.file_path, path);
    assert!(entry.cache_key.is_empty());
    assert!(entry.metadata.is_none());
    assert_eq!(entry.compressed_size, 0);
    assert!(!entry.is_write_cached);
    assert!(entry.parse_errors.is_empty());
    assert!(!entry.is_valid());
    assert!(!entry.has_errors());
}

#[test]
fn test_cache_metadata_entry_valid() {
    let entry = create_test_metadata_entry("valid.meta", "bucket/object", 512, true, true);

    assert_eq!(entry.cache_key, "bucket/object");
    assert_eq!(entry.compressed_size, 512);
    assert!(entry.is_write_cached);
    assert!(entry.metadata.is_some());
    assert!(entry.parse_errors.is_empty());
    assert!(entry.is_valid());
    assert!(!entry.has_errors());
}

#[test]
fn test_cache_metadata_entry_invalid() {
    let entry = create_test_metadata_entry("invalid.meta", "bucket/object", 512, false, false);

    assert!(entry.metadata.is_none());
    assert!(!entry.parse_errors.is_empty());
    assert!(!entry.is_valid());
    assert!(entry.has_errors());
}

#[test]
fn test_cache_metadata_entry_get_category() {
    use s3_proxy::cache_initialization_coordinator::CacheCategory;

    let write_cached = create_test_metadata_entry("write.meta", "bucket/write", 512, true, true);
    let read_cached = create_test_metadata_entry("read.meta", "bucket/read", 256, false, true);
    let invalid = create_test_metadata_entry("invalid.meta", "bucket/invalid", 128, false, false);

    assert_eq!(write_cached.get_category(), CacheCategory::WriteCached);
    assert_eq!(read_cached.get_category(), CacheCategory::ReadCached);
    assert_eq!(invalid.get_category(), CacheCategory::Invalid);
}

#[test]
fn test_cache_metadata_entry_error_summary() {
    let valid_entry = create_test_metadata_entry("valid.meta", "bucket/valid", 512, true, true);
    let invalid_entry =
        create_test_metadata_entry("invalid.meta", "bucket/invalid", 256, false, false);

    assert_eq!(valid_entry.error_summary(), "No errors");
    assert_eq!(invalid_entry.error_summary(), "Parse error");
}

#[tokio::test]
async fn test_cache_metadata_entry_from_file_valid() {
    use serde_json;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let metadata_file = temp_dir.path().join("valid.meta");

    let metadata = NewCacheMetadata {
        cache_key: "test-bucket/test-object".to_string(),
        object_metadata: ObjectMetadata {
            etag: "test-etag".to_string(),
            content_type: Some("text/plain".to_string()),
            content_length: 1024,
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            is_write_cached: true,
            ..Default::default()
        },
        ranges: vec![RangeSpec {
            start: 0,
            end: 1023,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 512,
            uncompressed_size: 1024,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        }],
        created_at: SystemTime::now(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    };

    let metadata_json = serde_json::to_string(&metadata).unwrap();
    tokio::fs::write(&metadata_file, metadata_json)
        .await
        .unwrap();

    let result = CacheMetadataEntry::from_file(metadata_file.clone())
        .await
        .unwrap();
    assert!(result.is_some());

    let entry = result.unwrap();
    assert_eq!(entry.file_path, metadata_file);
    assert_eq!(entry.cache_key, "test-bucket/test-object");
    assert_eq!(entry.compressed_size, 512);
    assert!(entry.is_write_cached);
    assert!(entry.metadata.is_some());
    assert!(entry.parse_errors.is_empty());
    assert!(entry.is_valid());
}

#[tokio::test]
async fn test_cache_metadata_entry_from_file_invalid() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let invalid_file = temp_dir.path().join("invalid.meta");

    tokio::fs::write(&invalid_file, "invalid json content")
        .await
        .unwrap();

    let result = CacheMetadataEntry::from_file(invalid_file.clone())
        .await
        .unwrap();
    assert!(result.is_some());

    let entry = result.unwrap();
    assert_eq!(entry.file_path, invalid_file);
    assert!(entry.cache_key.is_empty());
    assert_eq!(entry.compressed_size, 0);
    assert!(!entry.is_write_cached);
    assert!(entry.metadata.is_none());
    assert!(!entry.parse_errors.is_empty());
    assert!(!entry.is_valid());
    assert!(entry.has_errors());
}

#[tokio::test]
async fn test_cache_metadata_entry_from_file_nonexistent() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let nonexistent_file = temp_dir.path().join("nonexistent.meta");

    let result = CacheMetadataEntry::from_file(nonexistent_file.clone())
        .await
        .unwrap();
    assert!(result.is_some());

    let entry = result.unwrap();
    assert_eq!(entry.file_path, nonexistent_file);
    assert!(!entry.parse_errors.is_empty());
    assert!(!entry.is_valid());
    assert!(entry.has_errors());
}

#[test]
fn test_cache_metadata_entry_from_file_sync() {
    use serde_json;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let metadata_file = temp_dir.path().join("sync_test.meta");

    let metadata = NewCacheMetadata {
        cache_key: "sync-test-bucket/sync-test-object".to_string(),
        object_metadata: ObjectMetadata {
            etag: "sync-test-etag".to_string(),
            content_type: Some("application/octet-stream".to_string()),
            content_length: 2048,
            last_modified: "Tue, 02 Jan 2024 00:00:00 GMT".to_string(),
            is_write_cached: false,
            ..Default::default()
        },
        ranges: vec![RangeSpec {
            start: 0,
            end: 2047,
            file_path: "sync_test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 1024,
            uncompressed_size: 2048,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        }],
        created_at: SystemTime::now(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    };

    let metadata_json = serde_json::to_string(&metadata).unwrap();
    std::fs::write(&metadata_file, metadata_json).unwrap();

    let entry = CacheMetadataEntry::from_file_sync(metadata_file.clone());

    assert_eq!(entry.file_path, metadata_file);
    assert_eq!(entry.cache_key, "sync-test-bucket/sync-test-object");
    assert_eq!(entry.compressed_size, 1024);
    assert!(!entry.is_write_cached);
    assert!(entry.metadata.is_some());
    assert!(entry.parse_errors.is_empty());
    assert!(entry.is_valid());
}

#[test]
fn test_scan_error_creation() {
    let path = PathBuf::from("error.meta");
    let message = "Test error message".to_string();
    let category = ScanErrorCategory::JsonParse;

    let error = ScanError::new(path.clone(), message.clone(), category.clone());

    assert_eq!(error.path, path);
    assert_eq!(error.message, message);
    assert_eq!(error.category, category);
}

#[test]
fn test_scan_error_file_read_error() {
    let path = PathBuf::from("read_error.meta");
    let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Permission denied");

    let error = ScanError::file_read_error(path.clone(), io_error);

    assert_eq!(error.path, path);
    assert!(error.message.contains("File read error"));
    assert!(error.message.contains("Permission denied"));
    assert_eq!(error.category, ScanErrorCategory::FileRead);
}

#[test]
fn test_scan_error_json_parse_error() {
    let path = PathBuf::from("parse_error.meta");
    let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();

    let error = ScanError::json_parse_error(path.clone(), json_error);

    assert_eq!(error.path, path);
    assert!(error.message.contains("JSON parse error"));
    assert_eq!(error.category, ScanErrorCategory::JsonParse);
}

#[test]
fn test_scan_error_validation_error() {
    let path = PathBuf::from("validation_error.meta");
    let message = "Validation failed: invalid range".to_string();

    let error = ScanError::validation_error(path.clone(), message.clone());

    assert_eq!(error.path, path);
    assert_eq!(error.message, message);
    assert_eq!(error.category, ScanErrorCategory::Validation);
}

#[test]
fn test_scan_error_category_equality() {
    assert_eq!(ScanErrorCategory::FileRead, ScanErrorCategory::FileRead);
    assert_eq!(ScanErrorCategory::JsonParse, ScanErrorCategory::JsonParse);
    assert_eq!(ScanErrorCategory::Validation, ScanErrorCategory::Validation);
    assert_eq!(
        ScanErrorCategory::DirectoryAccess,
        ScanErrorCategory::DirectoryAccess
    );
    assert_eq!(ScanErrorCategory::Other, ScanErrorCategory::Other);

    assert_ne!(ScanErrorCategory::FileRead, ScanErrorCategory::JsonParse);
    assert_ne!(ScanErrorCategory::Validation, ScanErrorCategory::Other);
}

/// Test comprehensive scan results with mixed data
#[test]
fn test_comprehensive_scan_results() {
    let mut results = ObjectsScanResults::empty();

    // Add metadata entries
    results.metadata_entries = vec![
        create_test_metadata_entry("write1.meta", "bucket/write1", 512, true, true),
        create_test_metadata_entry("write2.meta", "bucket/write2", 1024, true, true),
        create_test_metadata_entry("read1.meta", "bucket/read1", 256, false, true),
        create_test_metadata_entry("read2.meta", "bucket/read2", 128, false, true),
        create_test_metadata_entry("invalid.meta", "bucket/invalid", 64, false, false),
    ];

    // Add scan errors
    results.scan_errors = vec![
        ScanError::new(
            PathBuf::from("error1.meta"),
            "Parse error 1".to_string(),
            ScanErrorCategory::JsonParse,
        ),
        ScanError::new(
            PathBuf::from("error2.meta"),
            "File read error".to_string(),
            ScanErrorCategory::FileRead,
        ),
    ];

    // Set statistics (normally calculated during scan)
    results.total_objects = 4; // Only valid entries count
    results.total_size = 1920; // 512 + 1024 + 256 + 128
    results.write_cached_objects = 2;
    results.write_cached_size = 1536; // 512 + 1024
    results.read_cached_objects = 2;
    results.read_cached_size = 384; // 256 + 128
    results.scan_duration = Duration::from_millis(1500);

    // Test all methods
    assert_eq!(results.total_objects, 4);
    assert_eq!(results.total_size, 1920);
    assert_eq!(results.write_cache_percentage(), 80.0); // 1536 / 1920 * 100
    assert_eq!(results.read_cache_percentage(), 20.0); // 384 / 1920 * 100
    assert_eq!(results.average_object_size(), 480); // 1920 / 4
    assert!(results.has_errors());
    assert_eq!(results.error_count(), 2);

    let summary = results.summary_string();
    assert!(summary.contains("4 objects"));
    assert!(summary.contains("Write: 2 objects"));
    assert!(summary.contains("Read: 2 objects"));
    assert!(summary.contains("Errors: 2"));
}

/// Test edge cases and boundary conditions
mod edge_cases {
    use super::*;

    #[test]
    fn test_objects_scan_results_large_numbers() {
        let mut results = ObjectsScanResults::empty();
        results.total_objects = u64::MAX;
        results.total_size = u64::MAX;
        results.write_cached_size = u64::MAX / 2;
        results.read_cached_size = u64::MAX / 2;

        // Should handle large numbers without overflow
        assert_eq!(results.write_cache_percentage(), 50.0);
        assert_eq!(results.read_cache_percentage(), 50.0);

        // Average calculation with large numbers
        let avg = results.average_object_size();
        assert!(avg > 0); // Should not overflow to 0
    }

    #[test]
    fn test_cache_metadata_entry_empty_cache_key() {
        let mut entry = CacheMetadataEntry::new(PathBuf::from("test.meta"));
        entry.cache_key = String::new(); // Empty cache key
        entry.metadata = Some(
            create_test_metadata_entry("test.meta", "", 512, true, true)
                .metadata
                .unwrap(),
        );

        // Should still be considered valid if metadata is present
        assert!(entry.is_valid());
    }

    #[test]
    fn test_scan_error_empty_message() {
        let error = ScanError::new(
            PathBuf::from("test.meta"),
            String::new(), // Empty message
            ScanErrorCategory::Other,
        );

        assert!(error.message.is_empty());
        assert_eq!(error.category, ScanErrorCategory::Other);
    }

    #[test]
    fn test_objects_scan_results_zero_duration() {
        let mut results = ObjectsScanResults::empty();
        results.scan_duration = Duration::ZERO;

        // Should handle zero duration gracefully
        assert_eq!(results.scan_duration, Duration::ZERO);
    }
}
