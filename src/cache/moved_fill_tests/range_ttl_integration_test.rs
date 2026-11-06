//! Integration test for range TTL configuration
//!
//! This test verifies that ranges are stored with the configured TTL
//! at the object level (NewCacheMetadata.expires_at), not per-range.
//!
//! The object-level expiry refactor moved expires_at from RangeSpec to
//! NewCacheMetadata. Per-range is_expired() and refresh_ttl() were removed.

use super::support::StoreRangeExt as _;
use crate::cache_types::ObjectMetadata;
use crate::disk_cache::DiskCacheManager;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

#[tokio::test]
async fn test_range_stored_with_configured_ttl() {
    // This test verifies that the object-level expires_at is set from the TTL
    // passed to store_range, not a hardcoded value

    let temp_dir = TempDir::new().unwrap();
    let mut disk_cache = DiskCacheManager::new(
        temp_dir.path().to_path_buf(),
        true,      // compression_enabled
        1024,      // compression_threshold
        false,     // write_cache_enabled
        1_048_576, // compression_batch_size
    );

    disk_cache.initialize().await.unwrap();

    let cache_key = "/test-bucket/test-object.txt";
    let data = b"Test data for TTL verification";

    let object_metadata = ObjectMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: data.len() as u64,
        content_type: Some("text/plain".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: crate::cache_types::UploadState::Complete,
        cumulative_size: data.len() as u64,
        parts: Vec::new(),
        compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        part_layout_verified: false,
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
        graduation_accounted: false,
    };

    // Store range with a 10-year TTL (the validated maximum)
    let ten_years = Duration::from_secs(315360000); // ~10 years
    disk_cache
        .store_range(
            cache_key,
            0,
            data.len() as u64 - 1,
            data,
            object_metadata,
            ten_years,
            true,
        )
        .await
        .unwrap();

    // Retrieve metadata and verify the object-level TTL was used
    let metadata = disk_cache.get_metadata(cache_key).await.unwrap().unwrap();

    assert_eq!(metadata.ranges.len(), 1);

    // Check object-level expires_at (not per-range)
    let now = SystemTime::now();
    let time_until_expiration = metadata
        .expires_at
        .duration_since(now)
        .unwrap_or(Duration::ZERO);

    // Should be close to 10 years (within 10 seconds)
    assert!(
        time_until_expiration > Duration::from_secs(315360000 - 10),
        "Object should expire in approximately 10 years, but expires in {:?}",
        time_until_expiration
    );

    assert!(
        time_until_expiration < Duration::from_secs(315360000 + 10),
        "Object should expire in approximately 10 years, but expires in {:?}",
        time_until_expiration
    );

    // Verify the object is not expired
    assert!(
        metadata.expires_at >= std::time::SystemTime::now(),
        "Object should not be expired"
    );
}

#[tokio::test]
async fn test_range_with_short_ttl_expires() {
    // This test verifies that objects with short TTL actually expire

    let temp_dir = TempDir::new().unwrap();
    let mut disk_cache = DiskCacheManager::new(
        temp_dir.path().to_path_buf(),
        true,      // compression_enabled
        1024,      // compression_threshold
        false,     // write_cache_enabled
        1_048_576, // compression_batch_size
    );

    disk_cache.initialize().await.unwrap();

    let cache_key = "/test-bucket/short-ttl-object.txt";
    let data = b"Test data with short TTL";

    let object_metadata = ObjectMetadata {
        etag: "test-etag-2".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: data.len() as u64,
        content_type: Some("text/plain".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: crate::cache_types::UploadState::Complete,
        cumulative_size: data.len() as u64,
        parts: Vec::new(),
        compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        part_layout_verified: false,
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
        graduation_accounted: false,
    };

    // Store range with a 2-second TTL
    let short_ttl = Duration::from_secs(2);
    disk_cache
        .store_range(
            cache_key,
            0,
            data.len() as u64 - 1,
            data,
            object_metadata,
            short_ttl,
            true,
        )
        .await
        .unwrap();

    // Immediately after storage, object should not be expired
    let metadata = disk_cache.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(metadata.ranges.len(), 1);
    assert!(
        metadata.expires_at >= std::time::SystemTime::now(),
        "Object should not be expired immediately after storage"
    );

    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // After TTL expires, the object should be expired
    let metadata_result = disk_cache.get_metadata(cache_key).await.unwrap();

    if let Some(metadata) = metadata_result {
        assert!(
            std::time::SystemTime::now() > metadata.expires_at,
            "Object should be expired after TTL"
        );
    }
}

#[tokio::test]
async fn a_later_fill_does_not_change_the_objects_expiry() {
    // This test verifies that the object-level expiry applies uniformly to all ranges and is
    // fixed when the `.meta` is created

    let temp_dir = TempDir::new().unwrap();
    let mut disk_cache = DiskCacheManager::new(
        temp_dir.path().to_path_buf(),
        true,      // compression_enabled
        1024,      // compression_threshold
        false,     // write_cache_enabled
        1_048_576, // compression_batch_size
    );

    disk_cache.initialize().await.unwrap();

    let cache_key = "/test-bucket/multi-range-object.txt";

    let object_metadata = ObjectMetadata {
        etag: "test-etag-3".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 2048,
        content_type: Some("text/plain".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: crate::cache_types::UploadState::Complete,
        cumulative_size: 2048,
        parts: Vec::new(),
        compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        part_layout_verified: false,
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
        graduation_accounted: false,
    };

    // Store first range with long TTL
    let data1 = vec![1u8; 1024];
    let long_ttl = Duration::from_secs(3600); // 1 hour
    disk_cache
        .store_range(
            cache_key,
            0,
            1023,
            &data1,
            object_metadata.clone(),
            long_ttl,
            true,
        )
        .await
        .unwrap();

    // Store the second range with a much shorter TTL. The `.meta`'s expiry is set once, when
    // its first fill is folded in; a later fill of another extent neither shortens nor
    // extends it.
    let expires_after_first = disk_cache
        .get_metadata(cache_key)
        .await
        .unwrap()
        .unwrap()
        .expires_at;
    let data2 = vec![2u8; 1024];
    let short_ttl = Duration::from_secs(2); // 2 seconds
    disk_cache
        .store_range(
            cache_key,
            1024,
            2047,
            &data2,
            object_metadata,
            short_ttl,
            true,
        )
        .await
        .unwrap();
    let metadata = disk_cache.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(metadata.ranges.len(), 2);
    assert_eq!(
        metadata.expires_at, expires_after_first,
        "a later fill of another extent must not change the object's expiry"
    );
    assert!(
        metadata.expires_at > std::time::SystemTime::now() + Duration::from_secs(3000),
        "the expiry must still be the first fill's one-hour TTL"
    );
}
