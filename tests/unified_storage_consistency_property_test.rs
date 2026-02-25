//! Property-based tests for unified storage consistency
//! These tests use quickcheck to verify correctness properties across random inputs
//!
//! **Feature: legacy-write-cache-removal, Property 1: Unified storage consistency**
//! **Validates: Requirements 1.1, 1.2, 3.1**

use s3_proxy::cache::CacheEvictionAlgorithm;
use s3_proxy::cache::CacheManager;
use s3_proxy::config::SharedStorageConfig;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use walkdir::WalkDir;

/// Create a test CacheManager with write caching enabled and validation disabled
fn create_test_cache_manager(temp_dir: &TempDir) -> CacheManager {
    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,                       // ram_cache_enabled
        0,                           // max_ram_cache_size
        1024 * 1024 * 1024,          // max_cache_size (1GB)
        CacheEvictionAlgorithm::LRU, // eviction_algorithm
        1024,                        // compression_threshold
        true,                        // compression_enabled
        Duration::from_secs(3600),   // get_ttl
        Duration::from_secs(60),     // head_ttl
        Duration::from_secs(86400),  // put_ttl (1 day)
        false,                       // actively_remove_cached_data
        SharedStorageConfig::default(),
        0.5,                       // write_cache_percent
        true,                      // write_cache_enabled - RE-ENABLED
        Duration::from_secs(3600), // incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    )
}

/// Check if a directory exists and contains any files
fn directory_has_files(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }

    WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
        .any(|e| e.file_type().is_file())
}

/// **Feature: legacy-write-cache-removal, Property 1: Unified storage consistency**
/// **Validates: Requirements 1.1, 1.2, 3.1**
///
/// For any write-cached PUT operation, the cached data SHALL exist only in
/// `metadata/` (metadata) and `ranges/` (data) directories, and SHALL NOT
/// exist in `write_cache/` directory.
#[tokio::test]
async fn test_property_unified_storage_consistency() {
    println!("Starting property test with proxy stopped...");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    println!("Created isolated temp dir: {:?}", temp_dir.path());

    let cache_manager = create_test_cache_manager(&temp_dir);
    println!("Created cache manager");

    let cache_key = "test-bucket/test-object".to_string();
    let test_data = vec![0xABu8; 1024];
    let etag = "\"test-etag\"".to_string();
    let last_modified = "Wed, 21 Oct 2015 07:28:00 GMT".to_string();
    let content_type = Some("application/octet-stream".to_string());
    let response_headers: HashMap<String, String> = HashMap::new();

    cache_manager
        .store_put_as_write_cached_range(
            &cache_key,
            &test_data,
            etag.clone(),
            last_modified.clone(),
            content_type.clone(),
            response_headers,
        )
        .await
        .expect("Failed to store PUT");

    // Verify unified storage properties
    let metadata_dir = temp_dir.path().join("metadata");
    assert!(
        directory_has_files(&metadata_dir),
        "No files found in metadata/ directory"
    );

    let ranges_dir = temp_dir.path().join("ranges");
    assert!(
        directory_has_files(&ranges_dir),
        "No files found in ranges/ directory"
    );

    let write_cache_dir = temp_dir.path().join("write_cache");
    assert!(
        !directory_has_files(&write_cache_dir),
        "Files found in write_cache/ directory"
    );

    let metadata_path = cache_manager.get_new_metadata_file_path(&cache_key);
    assert!(metadata_path.exists(), "Metadata file does not exist");

    let metadata_content =
        std::fs::read_to_string(&metadata_path).expect("Could not read metadata file");
    let metadata: s3_proxy::cache_types::NewCacheMetadata =
        serde_json::from_str(&metadata_content).expect("Could not parse metadata");

    assert!(
        metadata.object_metadata.is_write_cached,
        "is_write_cached flag is not set"
    );
    assert!(!metadata.ranges.is_empty(), "No ranges in metadata");

    println!("✓ All unified storage consistency properties verified!");
}

/// Additional test: Verify that multiple PUT operations all use unified storage
#[tokio::test]
async fn test_property_multiple_puts_use_unified_storage() {
    use rand::Rng;

    let mut rng = rand::thread_rng();

    for iteration in 0..20 {
        let actual_count: usize = rng.gen_range(1..=10);

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_manager = create_test_cache_manager(&temp_dir);

        for i in 0..actual_count {
            let cache_key = format!("test-bucket/multi-object-{}-{}", iteration, i);
            let test_data = vec![0xCDu8; 1024 + i * 100];
            let etag = format!("\"multi-etag-{}-{}\"", iteration, i);
            let last_modified = "Wed, 21 Oct 2015 07:28:00 GMT".to_string();
            let content_type = Some("application/octet-stream".to_string());
            let response_headers: HashMap<String, String> = HashMap::new();

            cache_manager
                .store_put_as_write_cached_range(
                    &cache_key,
                    &test_data,
                    etag,
                    last_modified,
                    content_type,
                    response_headers,
                )
                .await
                .expect(&format!(
                    "Failed to store PUT {} at iteration {}",
                    i, iteration
                ));
        }

        let metadata_dir = temp_dir.path().join("metadata");
        let ranges_dir = temp_dir.path().join("ranges");
        let write_cache_dir = temp_dir.path().join("write_cache");

        let metadata_file_count = WalkDir::new(&metadata_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == "meta")
            })
            .count();

        assert!(
            metadata_file_count >= actual_count,
            "Iteration {}: Expected at least {} metadata files, found {}",
            iteration,
            actual_count,
            metadata_file_count
        );

        assert!(
            directory_has_files(&ranges_dir),
            "Iteration {}: No files found in ranges/ directory",
            iteration
        );

        assert!(
            !directory_has_files(&write_cache_dir),
            "Iteration {}: Files found in write_cache/ directory",
            iteration
        );
    }
}

/// **Feature: legacy-write-cache-removal, Property 3: Invalidation completeness**
#[tokio::test]
async fn test_property_invalidation_completeness() {
    use rand::Rng;

    let mut rng = rand::thread_rng();

    for iteration in 0..20 {
        let data_size: u16 = rng.gen_range(1..4096);
        let key_suffix: u8 = rng.gen();

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_manager = create_test_cache_manager(&temp_dir);

        let cache_key = format!("test-bucket/invalidation-test-{}-{}", iteration, key_suffix);
        let test_data = vec![0xABu8; data_size as usize];
        let etag = format!("\"invalidation-etag-{}-{}\"", iteration, key_suffix);
        let last_modified = "Wed, 21 Oct 2015 07:28:00 GMT".to_string();
        let content_type = Some("application/octet-stream".to_string());
        let response_headers: HashMap<String, String> = HashMap::new();

        cache_manager
            .store_put_as_write_cached_range(
                &cache_key,
                &test_data,
                etag.clone(),
                last_modified.clone(),
                content_type.clone(),
                response_headers,
            )
            .await
            .expect(&format!("Failed to store PUT at iteration {}", iteration));

        let metadata_path = cache_manager.get_new_metadata_file_path(&cache_key);
        assert!(
            metadata_path.exists(),
            "Iteration {}: Metadata file should exist before invalidation",
            iteration
        );

        let metadata_content = std::fs::read_to_string(&metadata_path).expect(&format!(
            "Iteration {}: Could not read metadata file",
            iteration
        ));
        let metadata: s3_proxy::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).expect(&format!(
                "Iteration {}: Could not parse metadata",
                iteration
            ));

        let range_file_paths: Vec<_> = metadata
            .ranges
            .iter()
            .map(|r| temp_dir.path().join("ranges").join(&r.file_path))
            .collect();

        for range_path in &range_file_paths {
            assert!(
                range_path.exists(),
                "Iteration {}: Range file {:?} should exist before invalidation",
                iteration,
                range_path
            );
        }

        let write_cache_dir = temp_dir.path().join("write_cache");
        assert!(
            !directory_has_files(&write_cache_dir),
            "Iteration {}: write_cache/ should not have files before invalidation",
            iteration
        );

        cache_manager
            .invalidate_write_cache_entry(&cache_key)
            .await
            .expect(&format!("Failed to invalidate at iteration {}", iteration));

        assert!(
            !metadata_path.exists(),
            "Iteration {}: Metadata file should be removed after invalidation",
            iteration
        );

        for range_path in &range_file_paths {
            assert!(
                !range_path.exists(),
                "Iteration {}: Range file {:?} should be removed after invalidation",
                iteration,
                range_path
            );
        }

        assert!(
            !directory_has_files(&write_cache_dir),
            "Iteration {}: write_cache/ should not have files after invalidation",
            iteration
        );
    }
}

/// Additional test: Verify invalidation of multiple entries uses only unified storage
#[tokio::test]
async fn test_property_multiple_invalidations_use_unified_storage() {
    use rand::Rng;

    let mut rng = rand::thread_rng();

    for iteration in 0..20 {
        let entry_count: usize = rng.gen_range(1..=5);

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_manager = create_test_cache_manager(&temp_dir);

        let mut cache_keys = Vec::new();

        for i in 0..entry_count {
            let cache_key = format!("test-bucket/multi-invalidation-{}-{}", iteration, i);
            let test_data = vec![0xEFu8; 512 + i * 100];
            let etag = format!("\"multi-inv-etag-{}-{}\"", iteration, i);
            let last_modified = "Wed, 21 Oct 2015 07:28:00 GMT".to_string();
            let content_type = Some("application/octet-stream".to_string());
            let response_headers: HashMap<String, String> = HashMap::new();

            cache_manager
                .store_put_as_write_cached_range(
                    &cache_key,
                    &test_data,
                    etag,
                    last_modified,
                    content_type,
                    response_headers,
                )
                .await
                .expect(&format!(
                    "Failed to store entry {} at iteration {}",
                    i, iteration
                ));

            cache_keys.push(cache_key);
        }

        for cache_key in &cache_keys {
            let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
            assert!(
                metadata_path.exists(),
                "Iteration {}: Metadata for {} should exist",
                iteration,
                cache_key
            );
        }

        for cache_key in &cache_keys {
            cache_manager
                .invalidate_write_cache_entry(cache_key)
                .await
                .expect(&format!(
                    "Failed to invalidate {} at iteration {}",
                    cache_key, iteration
                ));
        }

        for cache_key in &cache_keys {
            let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
            assert!(
                !metadata_path.exists(),
                "Iteration {}: Metadata for {} should be removed",
                iteration,
                cache_key
            );
        }

        let write_cache_dir = temp_dir.path().join("write_cache");
        assert!(
            !directory_has_files(&write_cache_dir),
            "Iteration {}: write_cache/ should not have files after multiple invalidations",
            iteration
        );
    }
}
