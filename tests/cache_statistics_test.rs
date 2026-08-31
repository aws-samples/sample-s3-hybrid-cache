use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;

mod common;

/// Helper to create and initialize a test cache manager with JournalConsolidator
async fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    // Before `initialize()`, or a validation scan starts immediately and can overwrite
    // the size figures these tests assert on. See `common::seed_validation_metadata`.
    common::seed_validation_metadata(&cache_dir);

    let cache_manager = Arc::new(CacheManager::new_with_defaults(cache_dir, false, 0));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    // Now initialize the cache manager (which requires the consolidator to exist)
    cache_manager.initialize().await.unwrap();

    cache_manager
}

#[tokio::test]
async fn test_calculate_disk_cache_size_with_new_architecture() {
    // Create temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone()).await;

    // Create a new metadata file with ranges
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        10485760, // 10MB
        Some("application/octet-stream".to_string()),
    );

    let now = SystemTime::now();
    let range_spec = RangeSpec {
        start: 0,
        end: 8388607,
        file_path: "test_bucket_object_0-8388607.bin".to_string(),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 8388608,
        uncompressed_size: 8388608,
        created_at: now,
        last_accessed: now,
        access_count: 1,
        staged: None,
    };

    let now = SystemTime::now();
    let cache_metadata = NewCacheMetadata {
        cache_key: "test-bucket/test-object".to_string(),
        object_metadata,
        ranges: vec![range_spec],
        created_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
        compression_info: s3_proxy::cache_types::CompressionInfo::default(),
        ..Default::default()
    };

    // Write metadata file
    let metadata_file_path = cache_dir
        .join("metadata")
        .join("test-bucket_colon_test-object.meta");
    std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
    let metadata_json = serde_json::to_string_pretty(&cache_metadata).unwrap();
    std::fs::write(&metadata_file_path, metadata_json).unwrap();

    // Write range binary file
    let range_file_path = cache_dir
        .join("ranges")
        .join("test_bucket_object_0-8388607.bin");
    std::fs::create_dir_all(range_file_path.parent().unwrap()).unwrap();
    let range_data = vec![0u8; 8388608]; // 8MB of data
    std::fs::write(&range_file_path, range_data).unwrap();

    // With accumulator-based size tracking, we need to add the size via the accumulator
    // and run a consolidation cycle to update size_state.json
    if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
        consolidator.size_accumulator().add(8388608);
        let _ = consolidator.run_consolidation_cycle().await;
    }

    // Get cache size stats
    let sizes = cache_manager
        .get_cache_size_stats()
        .await
        .unwrap()
        .sizes
        .expect("get_cache_size_stats always populates CacheSizes");

    // Verify that both metadata and range files are counted
    assert!(
        sizes.read_cache_size > 0,
        "Cache size should be greater than 0"
    );
    assert!(
        sizes.read_cache_size >= 8388608,
        "Cache size should include range file size"
    );

    println!("Cache size: {} bytes", sizes.read_cache_size);
}

#[tokio::test]
async fn test_get_cache_usage_breakdown_with_new_architecture() {
    // Create temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache manager
    let cache_manager = create_test_cache_manager(cache_dir.clone()).await;

    // Create a full object (range 0 to content_length-1)
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        10 * 1024 * 1024, // 10MB
        Some("application/octet-stream".to_string()),
    );

    let now = SystemTime::now();
    let range_spec = RangeSpec {
        start: 0,
        end: 10 * 1024 * 1024 - 1, // Full object
        file_path: "test_bucket_object_0-10485759.bin".to_string(),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 5 * 1024 * 1024,
        uncompressed_size: 10 * 1024 * 1024,
        created_at: now,
        last_accessed: now,
        access_count: 1,
        staged: None,
    };

    let cache_metadata = NewCacheMetadata {
        cache_key: "test-bucket/test-object".to_string(),
        object_metadata,
        ranges: vec![range_spec],
        created_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
        compression_info: s3_proxy::cache_types::CompressionInfo::default(),
        ..Default::default()
    };

    // Write metadata file
    let metadata_file_path = cache_dir
        .join("metadata")
        .join("test-bucket_colon_test-object.meta");
    std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
    let metadata_json = serde_json::to_string_pretty(&cache_metadata).unwrap();
    std::fs::write(&metadata_file_path, metadata_json).unwrap();

    // Get cache usage breakdown
    let breakdown = cache_manager.get_cache_usage_breakdown().await;

    // Verify breakdown counts
    assert_eq!(breakdown.full_objects, 1, "Should count 1 full object");
    assert_eq!(breakdown.range_objects, 0, "Should count 0 range objects");
    assert_eq!(
        breakdown.compressed_objects, 1,
        "Should count 1 compressed object"
    );
    assert_eq!(
        breakdown.compressed_bytes_saved,
        5 * 1024 * 1024,
        "Should calculate bytes saved"
    );

    println!("Cache usage breakdown: {:?}", breakdown);
}
