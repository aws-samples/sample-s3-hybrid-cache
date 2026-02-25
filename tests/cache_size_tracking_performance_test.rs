//! Cache Size Tracking Performance Tests
//!
//! NOTE: Many tests in this file have been removed or simplified because
//! size tracking is now handled by JournalConsolidator through journal entries.
//! See the journal-based-size-tracking spec for details.
//!
//! The remaining tests focus on:
//! - Metadata file creation performance

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tempfile::TempDir;

/// Helper to create a test cache manager with size tracking
async fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir,
        false, // RAM cache disabled for simplicity
        0,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false, // actively_remove_cached_data
    ));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    cache_manager.initialize().await.unwrap();
    cache_manager.set_cache_manager_in_tracker().await;

    cache_manager
}

/// Helper to create mock metadata files for validation testing
async fn create_mock_metadata_files(
    cache_dir: &std::path::Path,
    count: usize,
    size_per_file: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;

    // Create metadata directory structure
    let metadata_dir = cache_dir.join("metadata").join("test-bucket");
    fs::create_dir_all(&metadata_dir)?;

    for i in 0..count {
        // Create sharded directory structure (first 2 chars of hash)
        let shard = format!("{:02x}", i % 256);
        let subshard = format!("{:03x}", i % 4096);
        let shard_dir = metadata_dir.join(&shard).join(&subshard);
        fs::create_dir_all(&shard_dir)?;

        // Create metadata file
        let object_metadata = ObjectMetadata::new(
            format!("etag-{}", i),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            size_per_file,
            Some("application/octet-stream".to_string()),
        );

        let metadata = NewCacheMetadata {
            cache_key: format!("test-key-{}", i),
            object_metadata,
            ranges: vec![],
            created_at: SystemTime::now(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = shard_dir.join(format!("test-key-{}.meta", i));
        let json = serde_json::to_string(&metadata)?;
        fs::write(metadata_path, json)?;
    }

    Ok(())
}

#[tokio::test]
async fn test_metadata_file_creation_performance() {
    // Test the performance of creating metadata files (simulates cache write operations)
    // This indirectly tests validation scan performance by creating realistic test data

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let _cache_manager = create_test_cache_manager(cache_dir.clone()).await;

    // Create 1000 mock metadata files (scaled down from 100M for test speed)
    let file_count = 1000;
    let size_per_file = 1024 * 1024; // 1MB per file

    let start = Instant::now();
    create_mock_metadata_files(cache_dir.as_path(), file_count, size_per_file)
        .await
        .unwrap();
    let creation_duration = start.elapsed();

    println!("Metadata file creation performance:");
    println!("  Files created: {}", file_count);
    println!("  Creation duration: {:?}", creation_duration);
    println!(
        "  Files per second: {:.0}",
        file_count as f64 / creation_duration.as_secs_f64()
    );

    // File creation should be reasonably fast (< 10 seconds for 1000 files)
    assert!(
        creation_duration < Duration::from_secs(10),
        "Metadata file creation took too long: {:?}",
        creation_duration
    );
}
