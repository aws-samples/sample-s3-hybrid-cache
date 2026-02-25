use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::disk_cache::DiskCacheManager;
use tempfile::TempDir;

#[tokio::test]
async fn test_disk_cache_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    // Initialize
    cache_manager.initialize().await.unwrap();

    // Test data
    let cache_key = "test-bucket/test-object";
    let test_data = b"This is test data for caching";
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        test_data.len() as u64,
        Some("application/octet-stream".to_string()),
    );

    // Store range (full object as single range)
    cache_manager
        .store_range(
            cache_key,
            0,
            test_data.len() as u64 - 1,
            test_data,
            object_metadata.clone(),
            std::time::Duration::from_secs(3600), true)
        .await
        .unwrap();

    // Retrieve metadata
    let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata.is_some());

    let metadata = metadata.unwrap();
    assert_eq!(metadata.cache_key, cache_key);
    assert_eq!(metadata.ranges.len(), 1);

    // Load range data
    let range_data = cache_manager
        .load_range_data(&metadata.ranges[0])
        .await
        .unwrap();
    assert_eq!(&range_data[..], test_data);

    // Invalidate cache entry
    cache_manager
        .invalidate_cache_entry(cache_key)
        .await
        .unwrap();

    // Verify it's gone
    let metadata_after = cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata_after.is_none());
}

#[tokio::test]
async fn test_cache_key_sanitization() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    let unsafe_key = "bucket/path:with*special?chars<>|";
    let safe_key = cache_manager.sanitize_cache_key(unsafe_key);

    assert!(!safe_key.contains('/'));
    assert!(!safe_key.contains('*'));
    assert!(!safe_key.contains('?'));
    assert!(!safe_key.contains('<'));
    assert!(!safe_key.contains('>'));
    assert!(!safe_key.contains('|'));
}

#[tokio::test]
async fn test_cache_type_determination() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    // Test basic cache types
    assert_eq!(
        cache_manager.determine_cache_type("bucket/object"),
        "metadata"
    );
    assert_eq!(
        cache_manager.determine_cache_type("bucket/object:part:1"),
        "parts"
    );
    assert_eq!(
        cache_manager.determine_cache_type("bucket/object:range:0-1023"),
        "ranges"
    );

    // Version-related keys are now treated as regular metadata/parts/ranges
    // (versioning support has been removed)
    assert_eq!(
        cache_manager.determine_cache_type("bucket/object:version:123"),
        "metadata"
    );
    assert_eq!(
        cache_manager.determine_cache_type("bucket/object:version:123:part:1"),
        "parts"
    );
    assert_eq!(
        cache_manager.determine_cache_type("bucket/object:version:123:range:0-1023"),
        "ranges"
    );
}
#[tokio::test]
async fn test_atomic_write_operations_with_sharded_paths() {
    // Test Requirements 6.3, 7.3: Verify atomic write operations work with sharded paths
    // This test verifies:
    // 1. .tmp files are created in correct sharded paths
    // 2. Atomic rename to final path works correctly
    // 3. Operations work with multiple buckets

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    // Initialize cache
    cache_manager.initialize().await.unwrap();

    // Test data for multiple buckets
    let test_cases = vec![
        ("bucket1/path/to/object1.txt", "Data for bucket1 object1"),
        ("bucket2/path/to/object2.txt", "Data for bucket2 object2"),
        ("bucket1/another/object.txt", "Another object in bucket1"),
    ];

    for (cache_key, data) in &test_cases {
        let data_bytes = data.as_bytes();
        let metadata = s3_proxy::cache_types::ObjectMetadata {
            etag: format!("etag-{}", cache_key),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: data_bytes.len() as u64,
            content_type: Some("text/plain".to_string()),
            response_headers: std::collections::HashMap::new(),
            upload_state: s3_proxy::cache_types::UploadState::Complete,
            cumulative_size: data_bytes.len() as u64,
            parts: Vec::new(),
            compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
            compressed_size: 0,
            parts_count: None,
            part_ranges: std::collections::HashMap::new(),
            upload_id: None,
            is_write_cached: false,
            write_cache_expires_at: None,
            write_cache_created_at: None,
            write_cache_last_accessed: None,
        };

        // Store range using atomic operations
        cache_manager
            .store_range(
                cache_key,
                0,
                data_bytes.len() as u64 - 1,
                data_bytes,
                metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();

        // Verify the final files exist in sharded paths
        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        let range_path = cache_manager.get_new_range_file_path(cache_key, 0, data.len() as u64 - 1);

        assert!(
            metadata_path.exists(),
            "Metadata file should exist at {:?}",
            metadata_path
        );
        assert!(
            range_path.exists(),
            "Range file should exist at {:?}",
            range_path
        );

        // Verify no .tmp files remain (atomic rename completed)
        let metadata_tmp = metadata_path.with_extension("meta.tmp");
        let range_tmp = range_path.with_extension("bin.tmp");

        assert!(
            !metadata_tmp.exists(),
            "Temporary metadata file should not exist at {:?}",
            metadata_tmp
        );
        assert!(
            !range_tmp.exists(),
            "Temporary range file should not exist at {:?}",
            range_tmp
        );

        // Verify bucket-first structure in paths
        let metadata_path_str = metadata_path.to_string_lossy();
        let range_path_str = range_path.to_string_lossy();

        // Extract bucket name from cache key (format: bucket/object/path)
        let bucket = cache_key.split('/').next().unwrap();

        // Verify bucket appears in path
        assert!(
            metadata_path_str.contains(bucket),
            "Metadata path should contain bucket '{}': {:?}",
            bucket,
            metadata_path
        );
        assert!(
            range_path_str.contains(bucket),
            "Range path should contain bucket '{}': {:?}",
            bucket,
            range_path
        );

        // Verify hash directories exist (XX/YYY pattern)
        // The path should have structure: .../bucket/XX/YYY/filename
        let metadata_components: Vec<&str> = metadata_path_str.split('/').collect();
        let range_components: Vec<&str> = range_path_str.split('/').collect();

        // Find bucket in path and verify next two components are hash directories
        let bucket_idx_meta = metadata_components.iter().position(|&c| c == bucket);
        let bucket_idx_range = range_components.iter().position(|&c| c == bucket);

        assert!(
            bucket_idx_meta.is_some(),
            "Bucket should be in metadata path"
        );
        assert!(bucket_idx_range.is_some(), "Bucket should be in range path");

        let bucket_idx_meta = bucket_idx_meta.unwrap();
        let bucket_idx_range = bucket_idx_range.unwrap();

        // Verify we have at least 3 more components after bucket (XX, YYY, filename)
        assert!(
            metadata_components.len() > bucket_idx_meta + 3,
            "Metadata path should have hash directories after bucket"
        );
        assert!(
            range_components.len() > bucket_idx_range + 3,
            "Range path should have hash directories after bucket"
        );

        // Verify XX is 2 hex digits
        let level1_meta = metadata_components[bucket_idx_meta + 1];
        let level1_range = range_components[bucket_idx_range + 1];
        assert_eq!(
            level1_meta.len(),
            2,
            "Level 1 hash directory should be 2 characters"
        );
        assert_eq!(
            level1_range.len(),
            2,
            "Level 1 hash directory should be 2 characters"
        );
        assert!(
            level1_meta.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 1 should be hex digits: {}",
            level1_meta
        );
        assert!(
            level1_range.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 1 should be hex digits: {}",
            level1_range
        );

        // Verify YYY is 3 hex digits
        let level2_meta = metadata_components[bucket_idx_meta + 2];
        let level2_range = range_components[bucket_idx_range + 2];
        assert_eq!(
            level2_meta.len(),
            3,
            "Level 2 hash directory should be 3 characters"
        );
        assert_eq!(
            level2_range.len(),
            3,
            "Level 2 hash directory should be 3 characters"
        );
        assert!(
            level2_meta.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 2 should be hex digits: {}",
            level2_meta
        );
        assert!(
            level2_range.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 2 should be hex digits: {}",
            level2_range
        );
    }

    // Verify we can retrieve all stored ranges
    for (cache_key, expected_data) in &test_cases {
        let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            metadata.is_some(),
            "Metadata should exist for {}",
            cache_key
        );

        let metadata = metadata.unwrap();
        assert_eq!(
            metadata.ranges.len(),
            1,
            "Should have one range for {}",
            cache_key
        );

        let range_spec = &metadata.ranges[0];
        let range_data = cache_manager.load_range_data(range_spec).await.unwrap();
        assert_eq!(
            &range_data[..],
            expected_data.as_bytes(),
            "Range data should match for {}",
            cache_key
        );
    }

    // Verify different buckets have separate directories
    let bucket1_metadata_dir = temp_dir.path().join("metadata").join("bucket1");
    let bucket2_metadata_dir = temp_dir.path().join("metadata").join("bucket2");
    let bucket1_ranges_dir = temp_dir.path().join("ranges").join("bucket1");
    let bucket2_ranges_dir = temp_dir.path().join("ranges").join("bucket2");

    assert!(
        bucket1_metadata_dir.exists(),
        "Bucket1 metadata directory should exist"
    );
    assert!(
        bucket2_metadata_dir.exists(),
        "Bucket2 metadata directory should exist"
    );
    assert!(
        bucket1_ranges_dir.exists(),
        "Bucket1 ranges directory should exist"
    );
    assert!(
        bucket2_ranges_dir.exists(),
        "Bucket2 ranges directory should exist"
    );
}

#[tokio::test]
async fn test_range_spec_file_path_is_relative() {
    use s3_proxy::cache_types::ObjectMetadata;

    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    // Initialize
    cache_manager.initialize().await.unwrap();

    // Test data
    let cache_key = "test-bucket/test-object";
    let test_data = b"This is test data for verifying relative paths in RangeSpec";
    let object_metadata = ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        test_data.len() as u64,
        Some("application/octet-stream".to_string()),
    );

    // Store a range
    cache_manager
        .store_range(
            cache_key,
            0,
            test_data.len() as u64 - 1,
            test_data,
            object_metadata.clone(),
            std::time::Duration::from_secs(315360000), // 10 years TTL
            true)
        .await
        .unwrap();

    // Get metadata and verify the range spec file_path is relative
    let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
    assert!(metadata.is_some(), "Metadata should exist");

    let metadata = metadata.unwrap();
    assert_eq!(metadata.ranges.len(), 1, "Should have exactly one range");

    let range_spec = &metadata.ranges[0];

    // Verify file_path is relative (doesn't start with /)
    assert!(
        !range_spec.file_path.starts_with('/'),
        "file_path should be relative, not absolute: {}",
        range_spec.file_path
    );

    // Verify file_path includes bucket name
    assert!(
        range_spec.file_path.starts_with("test-bucket/"),
        "file_path should start with bucket name: {}",
        range_spec.file_path
    );

    // Verify file_path includes hash directories (XX/YYY pattern)
    let parts: Vec<&str> = range_spec.file_path.split('/').collect();
    assert!(
        parts.len() >= 4,
        "file_path should have at least 4 parts (bucket/XX/YYY/filename): {}",
        range_spec.file_path
    );

    // Verify second part is 2 hex digits (XX)
    assert_eq!(
        parts[1].len(),
        2,
        "Second part should be 2 hex digits: {}",
        parts[1]
    );
    assert!(
        parts[1].chars().all(|c| c.is_ascii_hexdigit()),
        "Second part should be hex digits: {}",
        parts[1]
    );

    // Verify third part is 3 hex digits (YYY)
    assert_eq!(
        parts[2].len(),
        3,
        "Third part should be 3 hex digits: {}",
        parts[2]
    );
    assert!(
        parts[2].chars().all(|c| c.is_ascii_hexdigit()),
        "Third part should be hex digits: {}",
        parts[2]
    );

    // Verify the file actually exists at the expected location
    let full_path = temp_dir.path().join("ranges").join(&range_spec.file_path);
    assert!(
        full_path.exists(),
        "Range file should exist at: {:?}",
        full_path
    );

    println!(
        "✓ RangeSpec file_path is correctly relative: {}",
        range_spec.file_path
    );
}
