//! Test per-range access tracking functionality
//! This test validates the RangeSpec fields and methods
//! Note: expires_at, is_expired(), and refresh_ttl() have been removed
//! as part of the object-level expiry refactor.

use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use std::time::{Duration, SystemTime};

#[test]
fn test_range_spec_new_constructor() {
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
        created_at: now - Duration::from_secs(3600),
        last_accessed: now - Duration::from_secs(1800),
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

#[test]
fn test_range_spec_serialization_with_access_fields() {
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
