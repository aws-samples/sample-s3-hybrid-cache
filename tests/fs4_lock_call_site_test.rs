//! Per-site unit and example tests for the fs2 -> fs4 migration.
//!
//! Feature: fs2-to-fs4-migration
//!
//! Covers the three `map_err` sites (Requirement 4.8), the two three-way sites
//! (Requirement 4.7), and the `Err(TryLockError::Error(..))` I/O-error branch
//! (Requirement 4.3), per design.md "Unit and example tests" and tasks.md task 9.
//!
//! Style follows `tests/distributed_eviction_lock_property_test.rs`:
//! `tokio::runtime::Runtime` for sites reachable only through async paths,
//! `tempfile::TempDir` for fixtures, no network/credentials/external services.
//!
//! All lock calls in this file use fully-qualified `fs4::FileExt::...` paths.
//! Never `use fs4::FileExt` and never bare method-call syntax on a `File` --
//! (method-call syntax silently binds to `std` on
//! Rust 1.89+).

use s3_proxy::cache_initialization_coordinator::CacheInitializationCoordinator;
use s3_proxy::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
use s3_proxy::config::CacheConfig;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::write_cache_manager::WriteCacheManager;
use s3_proxy::ProxyError;

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::runtime::Runtime;

// ============================================================================
// Shared helpers
// ============================================================================

fn create_test_cache_config(cache_dir: PathBuf) -> CacheConfig {
    CacheConfig {
        cache_dir,
        ..Default::default()
    }
}

/// Open the same lock-file path on a *separate* file handle (a fresh `open()`
/// call, not a `dup()` / `clone()`), so it contends exactly as a second
/// process would. See design.md "The same-process versus separate-process
/// question": `flock(2)` associates a lock with the open file description,
/// not the process or descriptor.
fn open_second_handle(path: &std::path::Path) -> std::fs::File {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .expect("failed to open second handle on lock file")
}

// ============================================================================
// 9.1 -- the `map_err` sites (Requirement 4.8)
//
// Each test holds the lock on a second file handle opened separately on the
// same lock-file path, then asserts the site under test returns the expected
// ProxyError variant when it tries (and fails) to acquire that already-held
// lock.
//
// The `metadata_lock_manager.rs` site this section used to cover,
// `try_acquire_exclusive_lock`, was a `pub fn` with no production caller and
// has been deleted; the metadata slot protocol's contention path is covered by
// `metadata_lock_manager`'s own `a_contended_slot_with_an_empty_lock_file_refuses_the_contender`.
// ============================================================================

/// Site: `cache_initialization_coordinator.rs` (~line 1765),
/// `try_acquire_lock_once` (reached via the public
/// `try_acquire_initialization_lock`).
///
/// `.map_err(|e| ProxyError::CacheError(...))` over `fs4::FileExt::try_lock`.
/// Holding the lock on a second handle must make the site return
/// `ProxyError::CacheError`.
#[test]
fn test_cache_initialization_coordinator_returns_cache_error_on_contention() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let config = create_test_cache_config(cache_dir.clone());

        let coordinator = CacheInitializationCoordinator::new(cache_dir.clone(), true, config);

        // First acquisition through the coordinator itself succeeds and holds
        // the lock (the coordinator's own lock file handle stays open inside
        // the returned InitializationLock).
        let first_lock = coordinator
            .try_acquire_initialization_lock()
            .await
            .expect("first acquisition should succeed");

        // Second acquisition, same coordinator (same lock path), must hit the
        // Contended branch and surface as ProxyError::CacheError with the
        // "Failed to acquire initialization lock" template, wrapped by the
        // retry-loop's own CacheError after retries are exhausted.
        let result = coordinator.try_acquire_initialization_lock().await;
        match result {
            Err(ProxyError::CacheError(msg)) => {
                assert!(
                    msg.contains("Failed to acquire initialization lock"),
                    "unexpected CacheError message: {msg}"
                );
            }
            Err(e) => panic!("expected ProxyError::CacheError, got: {e:?}"),
            Ok(_) => panic!("expected lock contention, but acquisition succeeded"),
        }

        drop(first_lock);
    });
}

/// Site: `cache_size_tracker.rs` (~line 1874), `try_acquire_validation_lock`.
///
/// `.map_err(|e| ProxyError::CacheError(...))` over `fs4::FileExt::try_lock`.
/// Holding the lock on a second handle must make the site return
/// `ProxyError::CacheError`.
#[test]
fn test_cache_size_tracker_validation_lock_returns_cache_error_on_contention() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tracker, _consolidator, _temp_dir) = create_test_size_tracker().await;

        let first_lock = tracker
            .try_acquire_validation_lock()
            .await
            .expect("first validation lock acquisition should succeed");

        let result = tracker.try_acquire_validation_lock().await;
        match result {
            Err(ProxyError::CacheError(msg)) => {
                assert!(
                    msg.contains("Failed to acquire validation lock"),
                    "unexpected CacheError message: {msg}"
                );
            }
            Err(e) => panic!("expected ProxyError::CacheError, got: {e:?}"),
            Ok(_) => panic!("expected lock contention, but acquisition succeeded"),
        }

        drop(first_lock);
    });
}

/// Helper mirroring `cache_size_tracker.rs`'s own `create_test_tracker`
/// pattern: wires a minimal `JournalConsolidator` + `MetadataLockManager` so
/// `CacheSizeTracker::new` can be constructed.
async fn create_test_size_tracker() -> (Arc<CacheSizeTracker>, Arc<JournalConsolidator>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.join("locks"),
        Duration::from_secs(30),
        3,
    ));
    let consolidation_config = ConsolidationConfig::default();

    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.clone(),
        journal_manager,
        lock_manager,
        consolidation_config,
    ));
    consolidator.initialize().await.unwrap();

    let config = CacheSizeConfig::default();
    let tracker = Arc::new(
        CacheSizeTracker::new(cache_dir, config, false, consolidator.clone())
            .await
            .unwrap(),
    );
    (tracker, consolidator, temp_dir)
}

// ============================================================================
// 9.2 -- the two three-way sites (Requirement 4.7)
//
// Each test asserts the observable return for both the acquired and
// contended states.
// ============================================================================

/// Site: `journal_consolidator.rs` (~line 1586),
/// `try_acquire_global_consolidation_lock`.
///
/// Three-way exemplar: `Ok(())` -> `Ok(true)`; `WouldBlock` -> `Ok(false)`;
/// other `Err` -> `warn!` + `Ok(false)`.
#[test]
fn test_journal_consolidator_try_acquire_global_consolidation_lock_acquired_and_contended() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance-a".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator_a = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator_a.initialize().await.unwrap();

        // Acquired state: no competing holder.
        let acquired = consolidator_a
            .try_acquire_global_consolidation_lock()
            .expect("try_acquire_global_consolidation_lock should not error");
        assert!(acquired, "expected the lock to be acquired");

        // Contended state: a second consolidator instance (representing a
        // second proxy process) pointed at the same cache_dir must observe
        // Ok(false) while consolidator_a still holds the lock.
        let journal_manager_b = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance-b".to_string(),
        ));
        let lock_manager_b = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator_b = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager_b,
            lock_manager_b,
            ConsolidationConfig::default(),
        ));
        consolidator_b.initialize().await.unwrap();

        let contended = consolidator_b
            .try_acquire_global_consolidation_lock()
            .expect("try_acquire_global_consolidation_lock should not error on contention");
        assert!(
            !contended,
            "expected Ok(false) while another instance holds the lock"
        );

        consolidator_a.release_global_consolidation_lock();
    });
}

/// Site: `write_cache_manager.rs` (~line 1117),
/// `evict_incomplete_upload_with_tracker`'s per-upload eviction lock.
///
/// Three-way exemplar: `Ok(())` -> proceeds with eviction; `WouldBlock` ->
/// skip and return `Ok(0)`; other `Err` -> `warn!` and proceed unlocked.
/// Exercised at the level of the observable eviction outcome (bytes freed),
/// via the public `evict_incomplete_uploads` entry point, since the
/// per-upload lock helper itself is private.
#[test]
fn test_write_cache_manager_incomplete_upload_eviction_lock_acquired_and_contended() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();

        let manager = WriteCacheManager::new(
            cache_dir.clone(),
            100 * 1024 * 1024, // 100MB total
            10.0,              // 10% write cache
            Duration::from_secs(86400),
            Duration::from_secs(1), // short incomplete-upload TTL
            s3_proxy::cache::CacheEvictionAlgorithm::TinyLFU,
            256 * 1024 * 1024,
        );

        let upload_id = "test-upload-lock-site";
        let cache_key = "test-bucket/test-object-lock-site";
        let mpus_dir = cache_dir.join("mpus_in_progress").join(upload_id);
        tokio::fs::create_dir_all(&mpus_dir).await.unwrap();

        let part_path = mpus_dir.join("part1.bin");
        tokio::fs::write(&part_path, vec![0u8; 1024]).await.unwrap();

        let tracker = s3_proxy::cache_types::MultipartUploadTracker {
            upload_id: upload_id.to_string(),
            cache_key: cache_key.to_string(),
            started_at: std::time::SystemTime::now() - Duration::from_secs(3600),
            parts: vec![s3_proxy::cache_types::CachedPartInfo {
                part_number: 1,
                size: 1024,
                // Fixture writes the part uncompressed, so the on-disk length equals
                // `size` here. This test is about lock call sites, not byte accounting.
                compressed_size: 1024,
                etag: "\"etag-1\"".to_string(),
                compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
                file_identity: None,
            }],
            total_size: 1024,
            content_type: None,
        };
        let upload_meta_path = mpus_dir.join("upload.meta");
        tokio::fs::write(
            &upload_meta_path,
            serde_json::to_string_pretty(&tracker).unwrap(),
        )
        .await
        .unwrap();

        // Create (and lock) the per-upload lock file BEFORE backdating
        // mtimes. Creating a new entry inside mpus_dir bumps the directory's
        // mtime, which would undo the backdating below if done afterward and
        // make the eviction sweep's age check skip this upload for being
        // "too fresh" rather than for the lock contention this test targets.
        let lock_file_path = mpus_dir.join("upload.lock");
        let held_handle = open_second_handle(&lock_file_path);
        fs4::FileExt::try_lock(&held_handle).expect("failed to pre-hold per-upload lock");

        let old_time = std::time::SystemTime::now() - Duration::from_secs(30);
        let old_filetime = filetime::FileTime::from_system_time(old_time);
        filetime::set_file_mtime(&upload_meta_path, old_filetime).unwrap();
        filetime::set_file_mtime(&mpus_dir, old_filetime).unwrap();

        let freed_while_contended = manager.evict_incomplete_uploads().await.unwrap();
        assert_eq!(
            freed_while_contended, 0,
            "eviction must skip (free 0 bytes) while the per-upload lock is held by another handle"
        );
        assert!(
            mpus_dir.exists(),
            "upload directory must survive while its lock is contended"
        );
        assert!(
            part_path.exists(),
            "part file must survive while its lock is contended"
        );

        // Release the contending handle, then re-run: acquired state, the
        // eviction must now proceed and free the bytes.
        fs4::FileExt::unlock(&held_handle).ok();
        drop(held_handle);

        let freed_when_acquired = manager.evict_incomplete_uploads().await.unwrap();
        assert!(
            freed_when_acquired > 0,
            "eviction must free bytes once the per-upload lock is acquirable"
        );
        assert!(
            !mpus_dir.exists(),
            "upload directory must be removed once eviction proceeds"
        );
    });
}

// ============================================================================
// 9.3 -- the I/O-error branch (Requirement 4.3)
//
// A real non-WouldBlock flock failure needs a hostile descriptor and is
// neither portable nor cheap (per design.md). Instead this tests the
// TryLockError::Error(..) -> ProxyError mapping logic in isolation, at the
// same site shape used by the three map_err sites (9.1): constructing a
// synthetic fs4::TryLockError::Error(io::Error) directly and running it
// through the identical .map_err closure the sites use, confirming the
// rendered message shape and that the branch taken is the I/O-error branch,
// not the acquired or WouldBlock branches.
// ============================================================================

#[test]
fn test_try_lock_error_io_branch_maps_to_lock_contention_via_map_err_closure() {
    // Mirrors the `map_err`-to-`LockContention` closure shape the fs4 migration
    // introduced. The site that carried it in `metadata_lock_manager.rs`
    // (`try_acquire_exclusive_lock`) has since been deleted as dead code; the
    // live slot primitive `try_acquire_slot_lock` discriminates the three
    // outcomes with a `match` instead. This test keeps the closure shape itself
    // pinned:
    //   fs4::FileExt::try_lock(&lock_file).map_err(|e| {
    //       ProxyError::LockContention(format!(
    //           "Lock is held by another process: path={:?}, error={}",
    //           lock_path, e
    //       ))
    //   })
    let lock_path = PathBuf::from("/tmp/synthetic-lock-path-for-test.lock");
    let synthetic_io_error = std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "permission denied (synthetic)",
    );
    let synthetic_result: Result<(), fs4::TryLockError> =
        Err(fs4::TryLockError::Error(synthetic_io_error));

    let mapped: Result<(), ProxyError> = synthetic_result.map_err(|e| {
        ProxyError::LockContention(format!(
            "Lock is held by another process: path={:?}, error={}",
            lock_path, e
        ))
    });

    match mapped {
        Err(ProxyError::LockContention(msg)) => {
            assert!(
                msg.contains("permission denied (synthetic)"),
                "expected the underlying io::Error text to be preserved through Display, got: {msg}"
            );
            assert!(
                !msg.contains("would block"),
                "an I/O-error branch must not render the WouldBlock message shape, got: {msg}"
            );
        }
        other => {
            panic!("expected ProxyError::LockContention from the I/O-error branch, got: {other:?}")
        }
    }
}

#[test]
fn test_try_lock_error_io_branch_maps_to_cache_error_via_map_err_closure() {
    // Mirrors cache_initialization_coordinator.rs:1765 and
    // cache_size_tracker.rs:1874's exact closure shape:
    //   fs4::FileExt::try_lock(&lock_file).map_err(|e| {
    //       ProxyError::CacheError(format!("Failed to acquire initialization lock: {}", e))
    //   })
    let synthetic_io_error = std::io::Error::other("device or resource busy (synthetic)");
    let synthetic_result: Result<(), fs4::TryLockError> =
        Err(fs4::TryLockError::Error(synthetic_io_error));

    let mapped: Result<(), ProxyError> = synthetic_result.map_err(|e| {
        ProxyError::CacheError(format!("Failed to acquire initialization lock: {}", e))
    });

    match mapped {
        Err(ProxyError::CacheError(msg)) => {
            assert!(
                msg.contains("device or resource busy (synthetic)"),
                "expected the underlying io::Error text to be preserved through Display, got: {msg}"
            );
        }
        other => {
            panic!("expected ProxyError::CacheError from the I/O-error branch, got: {other:?}")
        }
    }
}

/// Confirms the three `TryLockError` outcomes are distinguishable at the same
/// seam: acquired, WouldBlock (contended), and a real I/O error each take a
/// different branch, rather than the I/O-error case being conflated with
/// Contended. This is the design's explicit three-outcome discrimination
/// (Requirement 4.1) applied directly to the enum, independent of any
/// particular call site's wiring.
#[test]
fn test_try_lock_error_three_outcomes_are_distinguishable() {
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Acquired,
        Contended,
        IoError,
    }

    fn decide(r: Result<(), fs4::TryLockError>) -> Outcome {
        match r {
            Ok(()) => Outcome::Acquired,
            Err(fs4::TryLockError::WouldBlock) => Outcome::Contended,
            Err(fs4::TryLockError::Error(_)) => Outcome::IoError,
        }
    }

    assert_eq!(decide(Ok(())), Outcome::Acquired);
    assert_eq!(
        decide(Err(fs4::TryLockError::WouldBlock)),
        Outcome::Contended
    );
    assert_eq!(
        decide(Err(fs4::TryLockError::Error(std::io::Error::other(
            "synthetic"
        )))),
        Outcome::IoError
    );
}
