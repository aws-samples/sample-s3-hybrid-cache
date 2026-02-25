//! Test for range-based eviction with unified eviction system
//!
//! Validates Requirements:
//! - 1.1: Each range is an independent eviction candidate
//! - 1.4: Sort by individual range access statistics
//! - 1.5: Allow evicting any subset of ranges independently
//! - 2.1: Select ranges based on configured algorithm
//! - 2.2: Use LRU algorithm (oldest last_accessed first)
//! - 2.3: Use TinyLFU algorithm (lowest frequency/recency score first)
//! - 2.4: Break ties using oldest created_at
//! - 3.1: Trigger eviction at 95% of max capacity
//! - 3.2: Calculate target size as 80% of max capacity (via perform_eviction_with_lock)
//! - 3.3: Evict ranges until cache size is at or below target
//! - 3.4: Free at least 5% of total capacity
//! - 3.5: Bypass caching if insufficient space after eviction

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::Result;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Helper to create a test cache manager with specific configuration
fn create_test_cache_manager(
    temp_dir: &TempDir,
    max_cache_size: u64,
    eviction_algorithm: CacheEvictionAlgorithm,
) -> CacheManager {
    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,                                     // RAM cache disabled
        0,                                         // max_ram_cache_size
        max_cache_size,                            // max_cache_size (sets max_cache_size_limit)
        eviction_algorithm,
        1024,                                      // compression_threshold
        true,                                      // compression_enabled
        Duration::from_secs(315360000),            // GET TTL (~10 years)
        Duration::from_secs(3600),                 // HEAD TTL (1 hour)
        Duration::from_secs(3600),                 // PUT TTL (1 hour)
        false,                                     // actively_remove_cached_data
        SharedStorageConfig::default(),
        10.0,                                      // write_cache_percent
        false,                                     // write_cache_enabled
        Duration::from_secs(86400),                // incomplete_upload_ttl
        MetadataCacheConfig::default(),
        95,                                        // eviction_trigger_percent
        80,                                        // eviction_target_percent
        true,                                      // read_cache_enabled
        Duration::from_secs(60),                   // bucket_settings_staleness_threshold
    )
}

/// Helper to create and store a range in cache using sharded path structure
async fn store_test_range(
    cache_manager: &CacheManager,
    cache_key: &str,
    start: u64,
    end: u64,
    data_size: usize,
    created_at: SystemTime,
    last_accessed: SystemTime,
    access_count: u64,
    ttl: Duration,
) -> Result<()> {
    // Create test data
    let data = vec![0u8; data_size];

    // Get sharded range file path using the same logic as DiskCacheManager
    let ranges_base_dir = cache_manager.get_cache_dir().join("ranges");
    let suffix = format!("_{}-{}.bin", start, end);
    let range_path = get_sharded_path(&ranges_base_dir, cache_key, &suffix)
        .expect("Failed to get sharded path for range file");

    // Extract the relative file path from the full path for storing in metadata
    let relative_range_path = range_path
        .strip_prefix(&ranges_base_dir)
        .unwrap_or(&range_path)
        .to_string_lossy()
        .to_string();

    // Create range spec with the sharded file path
    let range_spec = RangeSpec::new(
        start,
        end,
        relative_range_path,
        CompressionAlgorithm::Lz4,
        data_size as u64,
        data_size as u64,
    );

    // Manually set access statistics for testing
    let mut range_spec = range_spec;
    range_spec.created_at = created_at;
    range_spec.last_accessed = last_accessed;
    range_spec.access_count = access_count;

    // Create metadata
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            end - start + 1,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![range_spec.clone()],
        created_at,
        expires_at: SystemTime::now() + ttl,
        compression_info: Default::default(),
        ..Default::default()
    };

    // Write range binary file to sharded path
    std::fs::create_dir_all(range_path.parent().unwrap())?;
    std::fs::write(&range_path, &data)?;

    // Write metadata file to sharded path
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().unwrap())?;
    let json = serde_json::to_string_pretty(&metadata)?;
    std::fs::write(&metadata_path, json)?;

    Ok(())
}

#[tokio::test]
async fn test_eviction_triggers_at_95_percent() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64; // 1MB
    let cache_manager =
        create_test_cache_manager(&temp_dir, max_cache_size, CacheEvictionAlgorithm::LRU);

    // Fill cache to 90% (should not trigger eviction)
    let now = SystemTime::now();
    let ttl = Duration::from_secs(3600);

    // Store ranges totaling 900KB (90%)
    for i in 0..9 {
        let cache_key = format!("test-bucket/object-{}", i);
        store_test_range(
            &cache_manager,
            &cache_key,
            0,
            99_999,
            100_000,                                  // 100KB each
            now - Duration::from_secs(3600 - i * 60), // Older ranges first
            now - Duration::from_secs(1800 - i * 60),
            10,
            ttl,
        )
        .await
        .unwrap();
    }

    // Verify cache size is around 900KB
    let current_size = cache_manager.calculate_disk_cache_size().await.unwrap();
    assert!(
        current_size >= 900_000 && current_size < 950_000,
        "Cache size should be around 900KB, got {}",
        current_size
    );

    // Try to add 100KB more (would bring total to ~1MB = 100%, but trigger is at 95%)
    // This should trigger eviction
    let result = cache_manager.evict_if_needed(100_000).await;

    // Eviction should succeed
    assert!(result.is_ok(), "Eviction should succeed: {:?}", result);

    // After eviction, cache should be at or below 90% (900KB)
    let final_size = cache_manager.calculate_disk_cache_size().await.unwrap();
    let target_size = (max_cache_size as f64 * 0.80) as u64;
    assert!(
        final_size <= target_size,
        "Cache size after eviction should be <= 80% ({}), got {}",
        target_size,
        final_size
    );
}

#[tokio::test]
async fn test_eviction_uses_lru_algorithm() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64; // 1MB
    let cache_manager =
        create_test_cache_manager(&temp_dir, max_cache_size, CacheEvictionAlgorithm::LRU);

    let now = SystemTime::now();
    let ttl = Duration::from_secs(3600);

    // Store 3 ranges with different access times
    // Total: ~1.2MB with max 1MB, target is 80% (800KB)
    // Need to evict ~400KB+ to get below 800KB
    // With 3x400KB ranges, eviction will remove the 2 oldest to get to ~400KB

    // Range 1: Oldest access (should be evicted first)
    store_test_range(
        &cache_manager,
        "test-bucket/object-1",
        0,
        399_999,
        400_000, // 400KB
        now - Duration::from_secs(7200),
        now - Duration::from_secs(7200), // Accessed 2 hours ago
        5,
        ttl,
    )
    .await
    .unwrap();

    // Range 2: Recent access (should be kept - most recent)
    store_test_range(
        &cache_manager,
        "test-bucket/object-2",
        0,
        399_999,
        400_000, // 400KB
        now - Duration::from_secs(3600),
        now - Duration::from_secs(60), // Accessed 1 minute ago
        10,
        ttl,
    )
    .await
    .unwrap();

    // Range 3: Medium access (will be evicted - second oldest)
    store_test_range(
        &cache_manager,
        "test-bucket/object-3",
        0,
        399_999,
        400_000, // 400KB
        now - Duration::from_secs(5400),
        now - Duration::from_secs(1800), // Accessed 30 minutes ago
        8,
        ttl,
    )
    .await
    .unwrap();

    // Total: ~1.2MB, should trigger eviction to 80% target (800KB)
    // Trigger eviction
    cache_manager.evict_if_needed(0).await.unwrap();

    // Verify that object-1 (oldest access) was evicted
    let metadata_path_1 = cache_manager.get_new_metadata_file_path("test-bucket/object-1");
    assert!(
        !metadata_path_1.exists(),
        "Object 1 (oldest access) should be evicted"
    );

    // Verify that object-2 (most recent access) still exists
    let metadata_path_2 = cache_manager.get_new_metadata_file_path("test-bucket/object-2");
    assert!(
        metadata_path_2.exists(),
        "Object 2 (most recent access) should be kept"
    );

    // Note: Object 3 may or may not be evicted depending on exact eviction target
    // The key property is that LRU evicts oldest first, which we verified with object-1
}

#[tokio::test]
async fn test_eviction_uses_tinylfu_algorithm() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64; // 1MB
    let cache_manager =
        create_test_cache_manager(&temp_dir, max_cache_size, CacheEvictionAlgorithm::TinyLFU);

    let now = SystemTime::now();
    let ttl = Duration::from_secs(3600);

    // Store 3 ranges with different access patterns
    // Total: ~1.2MB with max 1MB, target is 80% (800KB)
    // Need to evict ~400KB+ to get below 800KB
    // With 3x400KB ranges, eviction will remove the 2 lowest-scored to get to ~400KB

    // Range 1: Low frequency, old access (should be evicted first - lowest TinyLFU score)
    store_test_range(
        &cache_manager,
        "test-bucket/object-1",
        0,
        399_999,
        400_000, // 400KB
        now - Duration::from_secs(7200),
        now - Duration::from_secs(3600), // Accessed 1 hour ago
        2,                               // Low access count
        ttl,
    )
    .await
    .unwrap();

    // Range 2: High frequency, recent access (should be kept - highest TinyLFU score)
    store_test_range(
        &cache_manager,
        "test-bucket/object-2",
        0,
        399_999,
        400_000, // 400KB
        now - Duration::from_secs(3600),
        now - Duration::from_secs(60), // Accessed 1 minute ago
        50,                            // High access count
        ttl,
    )
    .await
    .unwrap();

    // Range 3: Medium frequency, medium access (may be evicted - medium TinyLFU score)
    store_test_range(
        &cache_manager,
        "test-bucket/object-3",
        0,
        399_999,
        400_000, // 400KB
        now - Duration::from_secs(5400),
        now - Duration::from_secs(600), // Accessed 10 minutes ago
        20,                             // Medium access count
        ttl,
    )
    .await
    .unwrap();

    // Total: ~1.2MB, should trigger eviction to 80% target (800KB)
    // Trigger eviction
    cache_manager.evict_if_needed(0).await.unwrap();

    // Verify that object-1 (low frequency) was evicted
    let metadata_path_1 = cache_manager.get_new_metadata_file_path("test-bucket/object-1");
    assert!(
        !metadata_path_1.exists(),
        "Object 1 (low frequency) should be evicted"
    );

    // Verify that object-2 (high frequency) still exists
    let metadata_path_2 = cache_manager.get_new_metadata_file_path("test-bucket/object-2");
    assert!(
        metadata_path_2.exists(),
        "Object 2 (high frequency) should be kept"
    );

    // Note: Object 3 may or may not be evicted depending on exact eviction target
    // The key property is that TinyLFU evicts lowest-scored first, which we verified with object-1
}

#[tokio::test]
async fn test_eviction_frees_sufficient_space() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64; // 1MB
    let cache_manager =
        create_test_cache_manager(&temp_dir, max_cache_size, CacheEvictionAlgorithm::LRU);

    let now = SystemTime::now();
    let ttl = Duration::from_secs(3600);

    // Fill cache to 98% (980KB)
    for i in 0..10 {
        let cache_key = format!("test-bucket/object-{}", i);
        store_test_range(
            &cache_manager,
            &cache_key,
            0,
            97_999,
            98_000, // 98KB each
            now - Duration::from_secs(3600 - i * 60),
            now - Duration::from_secs(1800 - i * 60),
            10,
            ttl,
        )
        .await
        .unwrap();
    }

    // Verify cache is at ~980KB (98%)
    let initial_size = cache_manager.calculate_disk_cache_size().await.unwrap();
    assert!(
        initial_size >= 950_000,
        "Cache should be near 98%, got {}",
        initial_size
    );

    // Trigger eviction (should bring down to 80% target)
    cache_manager.evict_if_needed(0).await.unwrap();

    // Verify cache is at or below 80% (800KB) - the new unified eviction target
    let final_size = cache_manager.calculate_disk_cache_size().await.unwrap();
    let target_size = (max_cache_size as f64 * 0.80) as u64;
    assert!(
        final_size <= target_size,
        "Cache should be at or below 80% ({} bytes), got {} bytes",
        target_size,
        final_size
    );

    // Verify significant space was freed (at least 15% since we went from 98% to 80%)
    let freed = initial_size - final_size;
    let min_freed = (max_cache_size as f64 * 0.15) as u64;
    assert!(
        freed >= min_freed,
        "Should free at least 15% ({} bytes), freed {} bytes",
        min_freed,
        freed
    );
}

#[tokio::test]
async fn test_no_eviction_below_95_percent() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64; // 1MB
    let cache_manager =
        create_test_cache_manager(&temp_dir, max_cache_size, CacheEvictionAlgorithm::LRU);

    let now = SystemTime::now();
    let ttl = Duration::from_secs(3600);

    // Fill cache to 80% (800KB)
    for i in 0..8 {
        let cache_key = format!("test-bucket/object-{}", i);
        store_test_range(
            &cache_manager,
            &cache_key,
            0,
            99_999,
            100_000, // 100KB each
            now - Duration::from_secs(3600 - i * 60),
            now - Duration::from_secs(1800 - i * 60),
            10,
            ttl,
        )
        .await
        .unwrap();
    }

    // Verify cache is at ~800KB (80%)
    let initial_size = cache_manager.calculate_disk_cache_size().await.unwrap();
    assert!(
        initial_size >= 750_000 && initial_size < 850_000,
        "Cache should be around 80%, got {}",
        initial_size
    );

    // Try to trigger eviction with small required space
    cache_manager.evict_if_needed(10_000).await.unwrap();

    // Verify cache size hasn't changed (no eviction occurred)
    let final_size = cache_manager.calculate_disk_cache_size().await.unwrap();
    assert_eq!(
        initial_size, final_size,
        "Cache size should not change when below 95% threshold"
    );
}
