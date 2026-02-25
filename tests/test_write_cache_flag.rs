//! Tests for write cache flag behavior
//!
//! Validates that the write_cache_enabled flag controls directory creation correctly.
//! With unified storage, write_cache/ directory is no longer created.
//! Instead, mpus_in_progress/ is created when write caching is enabled.

use s3_proxy::disk_cache::DiskCacheManager;
use tempfile::TempDir;

#[tokio::test]
async fn test_mpus_in_progress_directory_not_created_when_disabled() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);

    cache_manager.initialize().await.unwrap();

    // Verify mpus_in_progress directory was NOT created when write caching is disabled
    let mpus_path = temp_dir.path().join("mpus_in_progress");
    assert!(
        !mpus_path.exists(),
        "mpus_in_progress directory should not exist when write caching is disabled"
    );

    // Verify write_cache directory was NOT created (legacy directory)
    let write_cache_path = temp_dir.path().join("write_cache");
    assert!(
        !write_cache_path.exists(),
        "write_cache directory should never exist (legacy)"
    );

    // Verify core directories were created
    assert!(temp_dir.path().join("metadata").exists());
    assert!(temp_dir.path().join("ranges").exists());
    assert!(temp_dir.path().join("locks").exists());
}

#[tokio::test]
async fn test_mpus_in_progress_directory_created_when_enabled() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, true);

    cache_manager.initialize().await.unwrap();

    // Verify mpus_in_progress directory WAS created when write caching is enabled
    let mpus_path = temp_dir.path().join("mpus_in_progress");
    assert!(
        mpus_path.exists(),
        "mpus_in_progress directory should exist when write caching is enabled"
    );

    // Verify write_cache directory was NOT created (legacy directory)
    let write_cache_path = temp_dir.path().join("write_cache");
    assert!(
        !write_cache_path.exists(),
        "write_cache directory should never exist (legacy)"
    );

    // Verify core directories were also created
    assert!(temp_dir.path().join("metadata").exists());
    assert!(temp_dir.path().join("ranges").exists());
    assert!(temp_dir.path().join("locks").exists());
}
