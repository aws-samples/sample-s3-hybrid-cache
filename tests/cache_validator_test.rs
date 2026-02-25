//! Unit tests for CacheValidator shared utilities
//!
//! Tests the shared validation utilities used by cache subsystems to ensure
//! consistent behavior across WriteCacheManager and CacheSizeTracker.

use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::cache_validator::{
    CacheFileCategory, CacheFileResult, CacheValidator,
};
use s3_proxy::compression::CompressionAlgorithm;
use serde_json;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio;

/// Create a test metadata object for testing
fn create_test_metadata(
    cache_key: &str,
    is_write_cached: bool,
    compressed_size: u64,
    uncompressed_size: u64,
) -> NewCacheMetadata {
    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: "test-etag".to_string(),
            content_type: Some("text/plain".to_string()),
            content_length: uncompressed_size,
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            is_write_cached,
            ..Default::default()
        },
        ranges: vec![RangeSpec {
            start: 0,
            end: uncompressed_size - 1,
            file_path: "test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size,
            uncompressed_size,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        }],
        created_at: SystemTime::now(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    }
}

/// Create a test cache file result
fn create_test_cache_file_result(
    path: &str,
    compressed_size: u64,
    is_write_cached: bool,
    has_metadata: bool,
) -> CacheFileResult {
    let file_path = PathBuf::from(path);
    let metadata = if has_metadata {
        Some(create_test_metadata(
            "test-bucket/test-object",
            is_write_cached,
            compressed_size,
            compressed_size * 2, // 50% compression ratio
        ))
    } else {
        None
    };

    CacheFileResult {
        file_path,
        compressed_size,
        is_write_cached,
        metadata,
        parse_errors: if has_metadata {
            Vec::new()
        } else {
            vec!["Parse error".to_string()]
        },
    }
}

#[tokio::test]
async fn test_cache_validator_creation() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    assert_eq!(validator.cache_dir(), temp_dir.path());
}

#[tokio::test]
async fn test_validate_metadata_file_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());
    let nonexistent_file = temp_dir.path().join("nonexistent.meta");

    let result = validator
        .validate_metadata_file(&nonexistent_file)
        .await
        .unwrap();

    assert!(!result.is_valid);
    assert!(result.error_message.is_some());
    assert!(result
        .error_message
        .unwrap()
        .contains("File does not exist"));
    assert!(result.metadata.is_none());
}

#[tokio::test]
async fn test_validate_metadata_file_valid() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    // Create a valid metadata file
    let metadata = create_test_metadata("test-bucket/test-object", true, 512, 1024);
    let metadata_file = temp_dir.path().join("valid.meta");
    let metadata_json = serde_json::to_string(&metadata).unwrap();
    tokio::fs::write(&metadata_file, metadata_json)
        .await
        .unwrap();

    let result = validator
        .validate_metadata_file(&metadata_file)
        .await
        .unwrap();

    assert!(result.is_valid);
    assert!(result.error_message.is_none());
    assert!(result.metadata.is_some());

    let parsed_metadata = result.metadata.unwrap();
    assert_eq!(parsed_metadata.cache_key, "test-bucket/test-object");
    assert!(parsed_metadata.object_metadata.is_write_cached);
}

#[tokio::test]
async fn test_validate_metadata_file_invalid_json() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    // Create an invalid JSON file
    let invalid_file = temp_dir.path().join("invalid.meta");
    tokio::fs::write(&invalid_file, "invalid json content")
        .await
        .unwrap();

    let result = validator
        .validate_metadata_file(&invalid_file)
        .await
        .unwrap();

    assert!(!result.is_valid);
    assert!(result.error_message.is_some());
    assert!(result.metadata.is_none());
}

#[tokio::test]
async fn test_parse_metadata_safe_valid() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    // Create a valid metadata file
    let metadata = create_test_metadata("test-bucket/test-object", false, 256, 512);
    let metadata_file = temp_dir.path().join("valid.meta");
    let metadata_json = serde_json::to_string(&metadata).unwrap();
    tokio::fs::write(&metadata_file, metadata_json)
        .await
        .unwrap();

    let result = validator.parse_metadata_safe(&metadata_file).await.unwrap();

    assert!(result.is_some());
    let parsed_metadata = result.unwrap();
    assert_eq!(parsed_metadata.cache_key, "test-bucket/test-object");
    assert!(!parsed_metadata.object_metadata.is_write_cached);
}

#[tokio::test]
async fn test_parse_metadata_safe_invalid() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    // Create an invalid JSON file
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
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());
    let empty_dir = temp_dir.path().join("empty");
    tokio::fs::create_dir_all(&empty_dir).await.unwrap();

    let result = validator.scan_directory_parallel(&empty_dir).await.unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn test_scan_directory_parallel_with_metadata_files() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());
    let test_dir = temp_dir.path().join("test");
    tokio::fs::create_dir_all(&test_dir).await.unwrap();

    // Create some metadata files
    let metadata1 = create_test_metadata("bucket/object1", true, 512, 1024);
    let metadata2 = create_test_metadata("bucket/object2", false, 256, 512);

    let file1 = test_dir.join("object1.meta");
    let file2 = test_dir.join("object2.meta");
    let non_meta_file = test_dir.join("other.txt");

    tokio::fs::write(&file1, serde_json::to_string(&metadata1).unwrap())
        .await
        .unwrap();
    tokio::fs::write(&file2, serde_json::to_string(&metadata2).unwrap())
        .await
        .unwrap();
    tokio::fs::write(&non_meta_file, "not a metadata file")
        .await
        .unwrap();

    let result = validator.scan_directory_parallel(&test_dir).await.unwrap();

    assert_eq!(result.len(), 2); // Only .meta files should be included
    assert!(result.contains(&file1));
    assert!(result.contains(&file2));
    assert!(!result.contains(&non_meta_file));
}

#[tokio::test]
async fn test_scan_and_process_parallel() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());
    let test_dir = temp_dir.path().join("test");
    tokio::fs::create_dir_all(&test_dir).await.unwrap();

    // Create valid and invalid metadata files
    let valid_metadata = create_test_metadata("bucket/valid", true, 512, 1024);
    let valid_file = test_dir.join("valid.meta");
    let invalid_file = test_dir.join("invalid.meta");

    tokio::fs::write(&valid_file, serde_json::to_string(&valid_metadata).unwrap())
        .await
        .unwrap();
    tokio::fs::write(&invalid_file, "invalid json")
        .await
        .unwrap();

    let results = validator
        .scan_and_process_parallel(&test_dir)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);

    // Find valid and invalid results
    let valid_result = results.iter().find(|r| r.file_path == valid_file).unwrap();
    let invalid_result = results
        .iter()
        .find(|r| r.file_path == invalid_file)
        .unwrap();

    // Check valid result
    assert!(valid_result.metadata.is_some());
    assert_eq!(valid_result.compressed_size, 512);
    assert!(valid_result.is_write_cached);
    assert!(valid_result.parse_errors.is_empty());

    // Check invalid result
    assert!(invalid_result.metadata.is_none());
    assert_eq!(invalid_result.compressed_size, 0);
    assert!(!invalid_result.parse_errors.is_empty());
}

#[test]
fn test_calculate_compressed_size() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let ranges = vec![
        RangeSpec {
            start: 0,
            end: 511,
            file_path: "range1.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 256,
            uncompressed_size: 512,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        },
        RangeSpec {
            start: 512,
            end: 1023,
            file_path: "range2.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 300,
            uncompressed_size: 512,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        },
    ];

    let total_compressed = validator.calculate_compressed_size(&ranges);
    assert_eq!(total_compressed, 556); // 256 + 300
}

#[test]
fn test_calculate_uncompressed_size() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let ranges = vec![
        RangeSpec {
            start: 0,
            end: 511,
            file_path: "range1.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 256,
            uncompressed_size: 512,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        },
        RangeSpec {
            start: 512,
            end: 1023,
            file_path: "range2.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 300,
            uncompressed_size: 512,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            frequency_score: 1,
        },
    ];

    let total_uncompressed = validator.calculate_uncompressed_size(&ranges);
    assert_eq!(total_uncompressed, 1024); // 512 + 512
}

#[test]
fn test_calculate_compression_ratio() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let ranges = vec![RangeSpec {
        start: 0,
        end: 1023,
        file_path: "range.bin".to_string(),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 512,
        uncompressed_size: 1024,
        created_at: SystemTime::now(),
        last_accessed: SystemTime::now(),
        access_count: 1,
        frequency_score: 1,
    }];

    let ratio = validator.calculate_compression_ratio(&ranges);
    assert_eq!(ratio, 0.5); // 512 / 1024 = 0.5
}

#[test]
fn test_calculate_compression_ratio_zero_uncompressed() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let ranges = vec![RangeSpec {
        start: 0,
        end: 0,
        file_path: "range.bin".to_string(),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 100,
        uncompressed_size: 0, // Edge case
        created_at: SystemTime::now(),
        last_accessed: SystemTime::now(),
        access_count: 1,
        frequency_score: 1,
    }];

    let ratio = validator.calculate_compression_ratio(&ranges);
    assert_eq!(ratio, 1.0); // Should return 1.0 to avoid division by zero
}

#[test]
fn test_filter_by_write_cached() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("write1.meta", 512, true, true),
        create_test_cache_file_result("read1.meta", 256, false, true),
        create_test_cache_file_result("write2.meta", 1024, true, true),
        create_test_cache_file_result("read2.meta", 128, false, true),
    ];

    // Filter for write-cached files
    let write_cached = validator.filter_by_write_cached(&results, true);
    assert_eq!(write_cached.len(), 2);
    assert!(write_cached.iter().all(|r| r.is_write_cached));

    // Filter for read-cached files
    let read_cached = validator.filter_by_write_cached(&results, false);
    assert_eq!(read_cached.len(), 2);
    assert!(read_cached.iter().all(|r| !r.is_write_cached));
}

#[test]
fn test_filter_valid_results() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("valid1.meta", 512, true, true),
        create_test_cache_file_result("invalid1.meta", 256, false, false),
        create_test_cache_file_result("valid2.meta", 1024, false, true),
        create_test_cache_file_result("invalid2.meta", 128, true, false),
    ];

    let valid_results = validator.filter_valid_results(&results);
    assert_eq!(valid_results.len(), 2);
    assert!(valid_results.iter().all(|r| r.metadata.is_some()));
}

#[test]
fn test_filter_error_results() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("valid1.meta", 512, true, true),
        create_test_cache_file_result("invalid1.meta", 256, false, false),
        create_test_cache_file_result("valid2.meta", 1024, false, true),
        create_test_cache_file_result("invalid2.meta", 128, true, false),
    ];

    let error_results = validator.filter_error_results(&results);
    assert_eq!(error_results.len(), 2);
    assert!(error_results.iter().all(|r| !r.parse_errors.is_empty()));
}

#[test]
fn test_categorize_cache_file() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let write_cached = create_test_cache_file_result("write.meta", 512, true, true);
    let read_cached = create_test_cache_file_result("read.meta", 256, false, true);
    let invalid = create_test_cache_file_result("invalid.meta", 128, false, false);

    assert_eq!(
        validator.categorize_cache_file(&write_cached),
        CacheFileCategory::WriteCached
    );
    assert_eq!(
        validator.categorize_cache_file(&read_cached),
        CacheFileCategory::ReadCached
    );
    assert_eq!(
        validator.categorize_cache_file(&invalid),
        CacheFileCategory::Invalid
    );
}

#[test]
fn test_group_by_category() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("write1.meta", 512, true, true),
        create_test_cache_file_result("read1.meta", 256, false, true),
        create_test_cache_file_result("invalid1.meta", 128, false, false),
        create_test_cache_file_result("write2.meta", 1024, true, true),
        create_test_cache_file_result("read2.meta", 64, false, true),
    ];

    let (write_cached, read_cached, invalid) = validator.group_by_category(&results);

    assert_eq!(write_cached.len(), 2);
    assert_eq!(read_cached.len(), 2);
    assert_eq!(invalid.len(), 1);

    assert!(write_cached
        .iter()
        .all(|r| r.is_write_cached && r.metadata.is_some()));
    assert!(read_cached
        .iter()
        .all(|r| !r.is_write_cached && r.metadata.is_some()));
    assert!(invalid.iter().all(|r| r.metadata.is_none()));
}

#[test]
fn test_calculate_size_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("write1.meta", 512, true, true),
        create_test_cache_file_result("read1.meta", 256, false, true),
        create_test_cache_file_result("invalid1.meta", 128, false, false),
        create_test_cache_file_result("write2.meta", 1024, true, true),
    ];

    let stats = validator.calculate_size_statistics(&results);

    assert_eq!(stats.total_compressed_size, 1920); // 512 + 256 + 128 + 1024
    assert_eq!(stats.write_cached_size, 1536); // 512 + 1024
    assert_eq!(stats.read_cached_size, 256); // 256
    assert_eq!(stats.total_count, 4);
    assert_eq!(stats.write_cached_count, 2);
    assert_eq!(stats.read_cached_count, 1);
    assert_eq!(stats.invalid_count, 1);
    assert_eq!(stats.average_size, 480); // 1920 / 4
}

#[test]
fn test_calculate_compression_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("file1.meta", 512, true, true), // 50% compression (512/1024)
        create_test_cache_file_result("file2.meta", 256, false, true), // 50% compression (256/512)
        create_test_cache_file_result("invalid.meta", 128, false, false), // Should be ignored
    ];

    let stats = validator.calculate_compression_statistics(&results);

    assert_eq!(stats.total_compressed_size, 768); // 512 + 256
    assert_eq!(stats.total_uncompressed_size, 1536); // 1024 + 512
    assert_eq!(stats.overall_compression_ratio, 0.5); // 768 / 1536
    assert_eq!(stats.average_compression_ratio, 0.5); // (0.5 + 0.5) / 2
    assert_eq!(stats.space_saved_bytes, 768); // 1536 - 768
    assert_eq!(stats.space_saved_percentage, 50.0); // (768 / 1536) * 100
    assert_eq!(stats.objects_analyzed, 2);
}

#[test]
fn test_calculate_compression_statistics_empty() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![];
    let stats = validator.calculate_compression_statistics(&results);

    assert_eq!(stats.total_compressed_size, 0);
    assert_eq!(stats.total_uncompressed_size, 0);
    assert_eq!(stats.overall_compression_ratio, 1.0);
    assert_eq!(stats.average_compression_ratio, 1.0);
    assert_eq!(stats.space_saved_bytes, 0);
    assert_eq!(stats.space_saved_percentage, 0.0);
    assert_eq!(stats.objects_analyzed, 0);
}

#[test]
fn test_filter_by_size_range() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("small.meta", 100, true, true),
        create_test_cache_file_result("medium.meta", 500, false, true),
        create_test_cache_file_result("large.meta", 1000, true, true),
        create_test_cache_file_result("huge.meta", 2000, false, true),
    ];

    let filtered = validator.filter_by_size_range(&results, 200, 1500);
    assert_eq!(filtered.len(), 2);
    assert!(filtered
        .iter()
        .all(|r| r.compressed_size >= 200 && r.compressed_size <= 1500));
}

#[test]
fn test_find_largest_files() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("small.meta", 100, true, true),
        create_test_cache_file_result("medium.meta", 500, false, true),
        create_test_cache_file_result("large.meta", 1000, true, true),
        create_test_cache_file_result("huge.meta", 2000, false, true),
    ];

    let largest = validator.find_largest_files(&results, 2);
    assert_eq!(largest.len(), 2);
    assert_eq!(largest[0].compressed_size, 2000); // huge.meta
    assert_eq!(largest[1].compressed_size, 1000); // large.meta
}

#[test]
fn test_calculate_size_distribution() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("file1.meta", 100, true, true),
        create_test_cache_file_result("file2.meta", 200, false, true),
        create_test_cache_file_result("file3.meta", 300, true, true),
        create_test_cache_file_result("file4.meta", 400, false, true),
    ];

    let buckets = validator.calculate_size_distribution(&results, 2);
    assert_eq!(buckets.len(), 2);

    // First bucket: 0-199
    assert_eq!(buckets[0].min_size, 0);
    assert_eq!(buckets[0].max_size, 199);
    assert_eq!(buckets[0].count, 1); // Only 100 (200 goes to second bucket)

    // Second bucket: 200-400
    assert_eq!(buckets[1].min_size, 200);
    assert_eq!(buckets[1].max_size, 400);
    assert_eq!(buckets[1].count, 3); // 200, 300, 400
}

#[test]
fn test_calculate_total_size() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("file1.meta", 100, true, true),
        create_test_cache_file_result("file2.meta", 200, false, true),
        create_test_cache_file_result("file3.meta", 300, true, false),
    ];

    let total = validator.calculate_total_size(&results);
    assert_eq!(total, 600); // 100 + 200 + 300
}

#[test]
fn test_count_by_category() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let results = vec![
        create_test_cache_file_result("write1.meta", 512, true, true),
        create_test_cache_file_result("read1.meta", 256, false, true),
        create_test_cache_file_result("invalid1.meta", 128, false, false),
        create_test_cache_file_result("write2.meta", 1024, true, true),
        create_test_cache_file_result("read2.meta", 64, false, true),
    ];

    let (write_count, read_count, invalid_count) = validator.count_by_category(&results);

    assert_eq!(write_count, 2);
    assert_eq!(read_count, 2);
    assert_eq!(invalid_count, 1);
}

#[test]
fn test_format_size_human() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    assert_eq!(validator.format_size_human(512), "512 B");
    assert_eq!(validator.format_size_human(1536), "1.5 KiB");
    assert_eq!(validator.format_size_human(2097152), "2.0 MiB");
    assert_eq!(validator.format_size_human(3221225472), "3.0 GiB");
    assert_eq!(validator.format_size_human(1099511627776), "1.0 TiB");
}

#[test]
fn test_format_duration_human() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    assert_eq!(
        validator.format_duration_human(Duration::from_millis(500)),
        "500ms"
    );
    assert_eq!(
        validator.format_duration_human(Duration::from_millis(1500)),
        "1s"
    ); // The implementation rounds down
    assert_eq!(
        validator.format_duration_human(Duration::from_secs(90)),
        "1m 30s"
    );
    assert_eq!(
        validator.format_duration_human(Duration::from_secs(3661)),
        "1h 1m"
    ); // 3661s = 1h 1m 1s, but impl shows hours and minutes only
}

/// Test error handling scenarios
mod error_handling_tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_metadata_file_permission_denied() {
        let temp_dir = TempDir::new().unwrap();
        let validator = CacheValidator::new(temp_dir.path().to_path_buf());

        // Create a file and then make it unreadable (Unix only)
        let restricted_file = temp_dir.path().join("restricted.meta");
        tokio::fs::write(&restricted_file, "test content")
            .await
            .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = tokio::fs::metadata(&restricted_file)
                .await
                .unwrap()
                .permissions();
            perms.set_mode(0o000); // No permissions
            tokio::fs::set_permissions(&restricted_file, perms)
                .await
                .unwrap();

            let result = validator
                .validate_metadata_file(&restricted_file)
                .await
                .unwrap();
            assert!(!result.is_valid);
            assert!(result.error_message.is_some());
        }
    }

    #[tokio::test]
    async fn test_scan_directory_parallel_permission_denied() {
        let temp_dir = TempDir::new().unwrap();
        let validator = CacheValidator::new(temp_dir.path().to_path_buf());

        let restricted_dir = temp_dir.path().join("restricted");
        tokio::fs::create_dir_all(&restricted_dir).await.unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = tokio::fs::metadata(&restricted_dir)
                .await
                .unwrap()
                .permissions();
            perms.set_mode(0o000); // No permissions
            tokio::fs::set_permissions(&restricted_dir, perms)
                .await
                .unwrap();

            // Should handle permission errors gracefully
            let result = validator
                .scan_directory_parallel(&restricted_dir)
                .await
                .unwrap();
            // May be empty due to permission issues, but shouldn't panic
            assert!(result.len() == 0);
        }
    }

    #[tokio::test]
    async fn test_scan_and_process_parallel_mixed_errors() {
        let temp_dir = TempDir::new().unwrap();
        let validator = CacheValidator::new(temp_dir.path().to_path_buf());
        let test_dir = temp_dir.path().join("test");
        tokio::fs::create_dir_all(&test_dir).await.unwrap();

        // Create a mix of valid, invalid, and problematic files
        let valid_metadata = create_test_metadata("bucket/valid", true, 512, 1024);
        let valid_file = test_dir.join("valid.meta");
        let invalid_json_file = test_dir.join("invalid_json.meta");
        let empty_file = test_dir.join("empty.meta");

        tokio::fs::write(&valid_file, serde_json::to_string(&valid_metadata).unwrap())
            .await
            .unwrap();
        tokio::fs::write(&invalid_json_file, "invalid json content")
            .await
            .unwrap();
        tokio::fs::write(&empty_file, "").await.unwrap();

        let results = validator
            .scan_and_process_parallel(&test_dir)
            .await
            .unwrap();

        assert_eq!(results.len(), 3);

        // Should have one valid result and two with errors
        let valid_results = results.iter().filter(|r| r.metadata.is_some()).count();
        let error_results = results
            .iter()
            .filter(|r| !r.parse_errors.is_empty())
            .count();

        assert_eq!(valid_results, 1);
        assert_eq!(error_results, 2);
    }
}
