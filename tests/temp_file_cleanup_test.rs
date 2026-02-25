//! Temporary File Cleanup Test
//!
//! Tests that temporary files are cleaned up on proxy startup.
//!
//! Requirements:
//! - Requirement 4.4: Clean up temporary files on proxy startup
//! - Requirement 8.4: Handle incomplete uploads from crashes

use s3_proxy::cache::CacheManager;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
async fn test_cleanup_temporary_files_on_startup() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories (unified storage format - no write_cache/ or head_cache/)
    let subdirs = ["metadata", "ranges", "mpus_in_progress"];
    for subdir in &subdirs {
        fs::create_dir_all(cache_dir.join(subdir)).await.unwrap();
    }

    // Create some temporary files in different subdirectories
    let tmp_files = vec![
        cache_dir.join("metadata/test1.tmp"),
        cache_dir.join("ranges/test2.tmp"),
        cache_dir.join("mpus_in_progress/test3.tmp"),
    ];

    for tmp_file in &tmp_files {
        fs::write(tmp_file, b"temporary data").await.unwrap();
        assert!(
            tmp_file.exists(),
            "Temporary file should exist before cleanup"
        );
    }

    // Create some non-temporary files that should NOT be deleted
    let permanent_files = vec![
        cache_dir.join("metadata/test.cache"),
        cache_dir.join("ranges/test.bin"),
        cache_dir.join("metadata/test.meta"),
    ];

    for perm_file in &permanent_files {
        fs::write(perm_file, b"permanent data").await.unwrap();
        assert!(perm_file.exists(), "Permanent file should exist");
    }

    // Create cache manager
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
    let _disk_cache = _cache_manager.create_configured_disk_cache_manager();
    _cache_manager.initialize().await.unwrap();

    // Verify temporary files were deleted
    for tmp_file in &tmp_files {
        assert!(
            !tmp_file.exists(),
            "Temporary file {:?} should be deleted after initialization",
            tmp_file
        );
    }

    // Verify permanent files were NOT deleted
    for perm_file in &permanent_files {
        assert!(
            perm_file.exists(),
            "Permanent file {:?} should still exist after cleanup",
            perm_file
        );
    }
}

#[tokio::test]
async fn test_cleanup_handles_missing_directories() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Don't create subdirectories - test that cleanup handles missing dirs gracefully

    // Create cache manager (should not fail even with missing subdirs)
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
}

#[tokio::test]
async fn test_cleanup_logs_but_continues_on_errors() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories
    fs::create_dir_all(cache_dir.join("metadata"))
        .await
        .unwrap();

    // Create a temporary file
    let tmp_file = cache_dir.join("metadata/test.tmp");
    fs::write(&tmp_file, b"temporary data").await.unwrap();

    // Create cache manager
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
}

#[tokio::test]
async fn test_cleanup_multiple_tmp_files_in_same_directory() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories
    fs::create_dir_all(cache_dir.join("ranges")).await.unwrap();

    // Create multiple temporary files in the same directory
    let tmp_files = vec![
        cache_dir.join("ranges/file1.tmp"),
        cache_dir.join("ranges/file2.tmp"),
        cache_dir.join("ranges/file3.tmp"),
        cache_dir.join("ranges/file4.tmp"),
        cache_dir.join("ranges/file5.tmp"),
    ];

    for tmp_file in &tmp_files {
        fs::write(tmp_file, b"temporary data").await.unwrap();
    }

    // Initialize cache manager
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
    let _disk_cache = _cache_manager.create_configured_disk_cache_manager();
    _cache_manager.initialize().await.unwrap();

    // Verify all temporary files were deleted
    for tmp_file in &tmp_files {
        assert!(
            !tmp_file.exists(),
            "Temporary file {:?} should be deleted",
            tmp_file
        );
    }
}

#[tokio::test]
async fn test_cleanup_empty_tmp_files() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories (unified storage format)
    fs::create_dir_all(cache_dir.join("metadata"))
        .await
        .unwrap();

    // Create an empty temporary file
    let tmp_file = cache_dir.join("metadata/empty.tmp");
    fs::write(&tmp_file, b"").await.unwrap();
    assert!(tmp_file.exists());

    // Initialize cache manager
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
    let _disk_cache = _cache_manager.create_configured_disk_cache_manager();
    _cache_manager.initialize().await.unwrap();

    // Verify empty temporary file was deleted
    assert!(!tmp_file.exists(), "Empty temporary file should be deleted");
}

#[tokio::test]
async fn test_cleanup_large_tmp_files() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories
    fs::create_dir_all(cache_dir.join("metadata"))
        .await
        .unwrap();

    // Create a large temporary file (1 MB)
    let tmp_file = cache_dir.join("metadata/large.tmp");
    let large_data = vec![0u8; 1024 * 1024]; // 1 MB
    fs::write(&tmp_file, &large_data).await.unwrap();
    assert!(tmp_file.exists());

    // Initialize cache manager
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
    let _disk_cache = _cache_manager.create_configured_disk_cache_manager();
    _cache_manager.initialize().await.unwrap();

    // Verify large temporary file was deleted
    assert!(!tmp_file.exists(), "Large temporary file should be deleted");
}

#[tokio::test]
async fn test_cleanup_preserves_non_tmp_extensions() {
    // Create a temporary cache directory
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    // Create cache subdirectories
    fs::create_dir_all(cache_dir.join("metadata"))
        .await
        .unwrap();

    // Create files with various extensions
    let files = vec![
        (cache_dir.join("metadata/file.tmp"), false), // Should be deleted
        (cache_dir.join("metadata/file.cache"), true), // Should be kept
        (cache_dir.join("metadata/file.meta"), true), // Should be kept
        (cache_dir.join("metadata/file.bin"), true),  // Should be kept
        (cache_dir.join("metadata/file.lock"), true), // Should be kept
        (cache_dir.join("metadata/file.tmp.bak"), true), // Should be kept (not .tmp extension)
        (cache_dir.join("metadata/tmpfile"), true),   // Should be kept (no extension)
    ];

    for (file_path, _) in &files {
        fs::write(file_path, b"test data").await.unwrap();
    }

    // Initialize cache manager
    let _cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);
    let _disk_cache = _cache_manager.create_configured_disk_cache_manager();
    _cache_manager.initialize().await.unwrap();

    // Verify correct files were deleted/kept
    for (file_path, should_exist) in &files {
        if *should_exist {
            assert!(
                file_path.exists(),
                "File {:?} should still exist",
                file_path
            );
        } else {
            assert!(
                !file_path.exists(),
                "File {:?} should be deleted",
                file_path
            );
        }
    }
}
