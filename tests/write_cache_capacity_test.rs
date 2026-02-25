use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::SharedStorageConfig;
use s3_proxy::Result;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_get_write_cache_capacity_default() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(600),
        Duration::from_secs(1800),
        false,
        SharedStorageConfig::default(),
        10.0,                       // 10% write cache
        true,                       // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );

    // With no total cache size set, should use 1GB default
    // 10% of 1GB (approximately 107MB)
    let capacity = cache_manager.get_write_cache_capacity();
    assert!(capacity > 107_000_000 && capacity < 108_000_000); // ~10% of 1GB

    Ok(())
}

#[tokio::test]
async fn test_get_write_cache_capacity_with_total_size() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        10 * 1024 * 1024 * 1024, // 10GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(600),
        Duration::from_secs(1800),
        false,
        SharedStorageConfig::default(),
        15.0,                       // 15% write cache
        true,                       // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await?;

    // 15% of 10GB = 1.5GB = 1610612736 bytes
    let capacity = cache_manager.get_write_cache_capacity();
    assert_eq!(capacity, 1610612736); // 10GB * 0.15

    Ok(())
}

#[tokio::test]
async fn test_get_write_cache_capacity_different_percentages() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();

    // Test with 5%
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(600),
        Duration::from_secs(1800),
        false,
        SharedStorageConfig::default(),
        5.0,                        // 5% write cache
        true,                       // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );
    cache_manager.update_total_cache_size(1024 * 1024 * 1024); // 1GB

    let capacity = cache_manager.get_write_cache_capacity();
    assert!(capacity > 53_000_000 && capacity < 54_000_000); // ~5% of 1GB

    Ok(())
}

#[tokio::test]
async fn test_get_write_cache_capacity_consistency() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false,
        0,
        5 * 1024 * 1024 * 1024, // 5GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(600),
        Duration::from_secs(1800),
        false,
        SharedStorageConfig::default(),
        20.0,                       // 20% write cache
        true,                       // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95, // eviction_trigger_percent
        80, // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    );
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await?;

    // 20% of 5GB = 1GB
    let capacity = cache_manager.get_write_cache_capacity();
    assert_eq!(capacity, 1073741824); // 5GB * 0.20

    // Call multiple times to ensure consistency
    let capacity2 = cache_manager.get_write_cache_capacity();
    assert_eq!(capacity, capacity2);

    Ok(())
}
