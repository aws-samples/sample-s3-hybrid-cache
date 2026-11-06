//! Unit tests for ObjectsScanResults functionality
//!
//! Tests the data structures and methods used for coordinated cache scanning
//! and result aggregation across cache subsystems.

use s3_proxy::cache_initialization_coordinator::{
    ObjectsScanResults, ScanError, ScanErrorCategory,
};
use std::path::PathBuf;
use std::time::Duration;

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

/// Test edge cases and boundary conditions
mod edge_cases {
    use super::*;

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
