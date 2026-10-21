//! Edge Case Handling Tests
//!
//! Tests for Requirement 9: Handle edge cases and error conditions
//!
//! This test suite verifies:
//! - Requirement 9.1: Handle corrupted range metadata with default statistics
//! - Requirement 9.2: Handle insufficient space after eviction (bypass caching)
//! - Requirement 9.3: RETIRED. There is no metadata lock timeout and no 60s break-lock rule;
//!   the protocol `flock`s a permanent shared slot. What is asserted in its place is that a
//!   legacy per-key `{key}.meta.lock` is inert — see `test_stale_metadata_lock_handling`.
//! - Requirement 9.4: Ensure .bin file deletion when range is deleted
//! - Requirement 9.5: Ensure .meta file deletion when last range is deleted

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::SharedStorageConfig;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Test that corrupted metadata is handled gracefully
/// Requirement 9.1: Handle corrupted range metadata with default statistics
#[tokio::test]
async fn test_corrupted_metadata_handling() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_key = "test-bucket/test-object";

    let disk_cache_temp = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
    );
    let metadata_path = disk_cache_temp.get_new_metadata_file_path(cache_key);

    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_path, "{ invalid json }").unwrap();

    let disk_cache = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
    );

    let result = disk_cache.get_metadata(cache_key).await.unwrap();
    assert!(
        result.is_none(),
        "Should return None for corrupted metadata"
    );
}

/// Test that insufficient space after eviction is handled
/// Requirement 9.2: Handle insufficient space after eviction (bypass caching)
///
/// Note: This is tested in eviction_buffer_test.rs where perform_eviction()
/// returns an error when it can't free enough space.
#[tokio::test]
async fn test_insufficient_space_after_eviction() {
    // The actual test for insufficient space is in the eviction logic
    // When perform_eviction() can't free enough space, it returns an error
    // This is already tested in eviction_buffer_test.rs

    println!("Insufficient space handling is verified in perform_eviction() method");
}

/// A pre-existing legacy per-key `{key}.meta.lock` is inert: not honoured, not unlinked.
///
/// **This test's original subject no longer exists.** It was written for "Requirement 9.3:
/// handle metadata lock timeout (break lock after 60s)", against a protocol that inferred a
/// holder's liveness from a lock file's age and broke the lock when it looked stale. That
/// protocol is retired: `metadata_lock_manager` locks a permanent shared slot under
/// `_meta_locks/`, holds a payload-free `flock`, and never infers staleness from elapsed
/// time — so there is no 60-second rule left to assert, and the per-key path this plants is
/// not written by anything.
///
/// The `.meta.lock` name below is therefore deliberate: the point is that a **pre-existing
/// legacy file at the retired path must not affect the slot-locked path**. Do not rename it
/// to the slot path, which would delete the only thing the fixture still says.
///
/// # What the body asserted before, and why it could never fail
///
/// It asserted `update_range_access(..).is_ok()` against metadata whose `ranges` vector was
/// empty. `update_range_access_direct` returns `Ok(())` from its `!range_found` arm for a key
/// whose metadata holds no matching range, so the assertion held whether or not the legacy
/// file existed — and would have held with the legacy file, the metadata file, and the whole
/// protocol behaving any way at all. It was vacuous from the day it was written.
///
/// # What it asserts now
///
/// Two arms, on **observable state** rather than on a return value:
///
/// - **Arm 1, slot free.** The stored range's `access_count` must rise by one in the `.meta`
///   on disk, which is the precondition that the call had something to do — the thing the old
///   body lacked. And the planted legacy file must survive with the same bytes, the same
///   mtime, and (on unix) the same inode. That is not a tautology: the retired protocol's
///   stale-detection branches judged a payload-free lock file stale **by mtime** and then
///   `remove_file`d it, unlinking a live holder's lock, which is the defect six comments in
///   `src/disk_cache.rs` warn against reintroducing.
/// - **Arm 2, slot held by a peer.** With this test holding the key's shared slot, the same
///   call must change nothing. This **observes** which file the operation locks instead of
///   assuming it: a holder of the shared slot would not exclude an operation that locked the
///   per-key path, so an update landing here means the lock namespace is not the slot.
///   `flock` conflicts between two descriptors of one file even inside one process (see
///   `metadata_lock_manager`'s `nested_meta_lock_acquisition_is_refused`), which is what lets
///   a single-process fixture hold the slot against the code under test.
/// - **Arm 3, slot released.** The same call must update again. Without it, arm 2 would pass
///   if the operation had simply stopped working, and both arms together would pass for the
///   wrong reason.
///
/// # Why this is not a duplicate
///
/// `disk_cache`'s `test_delete_cache_entry_basic` asserts only that no per-key `.meta.lock` is
/// **created**; it never plants one, so it says nothing about a pre-existing file being read
/// or unlinked. `cache`'s `meta_lock_unlink_sites_are_pinned` is a **source-text** scan that
/// only counts an unlink when the path derivation is bound with `let` and a `remove_file`
/// naming that binding follows within twelve lines — so an inline
/// `remove_file(path.with_extension("meta.lock"))`, or one further down the function, evades
/// it. `METALOCK-B1`'s tests in `cache` assert slot acquisition on the **deletion** paths, not
/// on `update_range_access`. This is the runtime guard for this operation.
#[tokio::test]
async fn test_stale_metadata_lock_handling() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let mut disk_cache = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
    );

    disk_cache.initialize().await.unwrap();

    let cache_key = "test-bucket/test-object";
    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        1024,
        Some("application/octet-stream".to_string()),
    );

    // A real cached range through the real producer, so the entry the operation below updates
    // is the shape production creates. The old fixture hand-wrote a `.meta` with an empty
    // `ranges` vector, which is what made the call a no-op and the assertion vacuous.
    disk_cache
        .store_range(
            cache_key,
            0,
            1023,
            &vec![7u8; 1024],
            object_metadata,
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("fixture: storing one range must succeed");

    let metadata_path = disk_cache.get_new_metadata_file_path(cache_key);
    // The RETIRED per-key path, planted on purpose. Nothing writes this any more; the
    // protocol locks a permanent shared slot under `_meta_locks/`.
    let legacy_lock_path = metadata_path.with_extension("meta.lock");
    let slot_path = s3_proxy::metadata_lock_manager::slot_lock_path(&cache_dir, cache_key);
    assert_ne!(
        legacy_lock_path, slot_path,
        "fixture: the retired per-key path and the slot path must be different files, or \
         neither arm below distinguishes anything"
    );

    // Read the access count out of the `.meta` on disk. Deliberately not through a cache
    // accessor: the state this test is about is what is on the volume.
    let access_count = || -> u64 {
        let json = std::fs::read_to_string(&metadata_path)
            .expect("fixture: the stored entry's .meta must be readable");
        let meta: s3_proxy::cache_types::NewCacheMetadata =
            serde_json::from_str(&json).expect("fixture: the stored .meta must parse");
        meta.ranges
            .iter()
            .find(|r| r.start == 0 && r.end == 1023)
            .expect(
                "fixture precondition: the stored range must be present in the .meta, or \
                 `update_range_access` takes its `!range_found` arm and this test is the \
                 vacuous one it replaced",
            )
            .access_count
    };

    let initial = access_count();
    assert!(
        initial > 0,
        "fixture precondition: a freshly stored range must carry a non-zero access count, \
         got {initial}"
    );

    // Plant the legacy lock file with a distinctive payload, at the age the retired protocol
    // judged stale. The slot protocol reads neither the path nor the mtime.
    const LEGACY_PAYLOAD: &str = "stale lock";
    std::fs::write(&legacy_lock_path, LEGACY_PAYLOAD).unwrap();
    let stale_time = SystemTime::now() - Duration::from_secs(61);
    let stale_filetime = filetime::FileTime::from_system_time(stale_time);
    // `expect`, not `ok()`: a silently failed mtime set would make this fixture's "61 seconds
    // old" claim false while the test still passed.
    filetime::set_file_mtime(&legacy_lock_path, stale_filetime)
        .expect("fixture: the planted file's mtime must be settable");

    let planted = std::fs::metadata(&legacy_lock_path).unwrap();
    let planted_mtime = planted.modified().unwrap();
    #[cfg(unix)]
    let planted_ino = std::os::unix::fs::MetadataExt::ino(&planted);

    let assert_legacy_untouched = |arm: &str| {
        let now = std::fs::metadata(&legacy_lock_path).unwrap_or_else(|e| {
            panic!(
                "{arm}: the legacy per-key .meta.lock was UNLINKED ({e}). The retired \
                 protocol's stale branches did exactly this to a live holder's lock; nothing \
                 may unlink a file in this namespace."
            )
        });
        assert_eq!(
            std::fs::read_to_string(&legacy_lock_path).unwrap(),
            LEGACY_PAYLOAD,
            "{arm}: the legacy per-key .meta.lock was rewritten or truncated"
        );
        assert_eq!(
            now.modified().unwrap(),
            planted_mtime,
            "{arm}: the legacy per-key .meta.lock's mtime moved, so something wrote or \
             touched it"
        );
        #[cfg(unix)]
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(&now),
            planted_ino,
            "{arm}: the legacy per-key .meta.lock is a different inode, so it was unlinked \
             and recreated"
        );
    };

    // ---- Arm 1: the slot is free, so the update must happen and the legacy file must not
    // ---- be consulted. A legacy file that were honoured as a held lock would skip the
    // ---- update and leave the count where it started.
    disk_cache
        .update_range_access(cache_key, 0, 1023)
        .await
        .expect("the slot is free, so the update must be attempted, not refused");
    assert_eq!(
        access_count(),
        initial + 1,
        "arm 1: the update did not happen with the slot free. A legacy per-key .meta.lock \
         must not be read, and must not be treated as a held lock"
    );
    assert_legacy_untouched("arm 1");

    // ---- Arm 2: a peer holds the key's SHARED SLOT. The operation must be excluded by it,
    // ---- which is how this test observes the lock namespace rather than assuming it.
    let peer = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(&cache_dir, cache_key)
        .expect("fixture: the slot must be free for the peer to take");
    assert_eq!(
        peer.lock_file_path(),
        slot_path.as_path(),
        "fixture: the peer must hold the very path the code under test acquires, or arm 2 \
         proves nothing about contention"
    );

    let held = access_count();
    disk_cache
        .update_range_access(cache_key, 0, 1023)
        .await
        .expect("a contended slot is skipped, not an error");
    assert_eq!(
        access_count(),
        held,
        "arm 2: the update landed while a peer held the SHARED SLOT. The operation is \
         therefore excluding on some other file — the retired per-key path, most likely — so \
         the slot is not the lock namespace"
    );
    assert_legacy_untouched("arm 2");

    // ---- Arm 3: released. The positive control for arm 2 — without it, arm 2 also passes if
    // ---- the operation stopped working entirely.
    drop(peer);
    disk_cache
        .update_range_access(cache_key, 0, 1023)
        .await
        .expect("the slot is free again, so the update must be attempted");
    assert_eq!(
        access_count(),
        held + 1,
        "arm 3: the update did not resume after the peer released the slot, so arm 2's \
         non-update is not attributable to contention"
    );
    assert_legacy_untouched("arm 3");
}

/// Test that .bin files are deleted when ranges are evicted
/// Requirement 9.4: Ensure .bin file deletion when range is deleted
#[tokio::test]
async fn test_bin_file_deletion_on_eviction() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            lock_acquire_wait_budget: Duration::from_secs(60),
            ..Default::default()
        },
        10.0,
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        std::time::Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache,
        std::time::Duration::from_secs(10), // ram_cache_flush_interval (Req 19)
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    );

    // Create a range with .bin file
    let cache_key = "test-bucket/test-object";
    let range_start = 0u64;
    let range_end = 1023u64;

    let mut disk_cache = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
    );

    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        1024,
        Some("application/octet-stream".to_string()),
    );

    let data = vec![0u8; 1024];
    disk_cache
        .store_range(
            cache_key,
            range_start,
            range_end,
            &data,
            object_metadata,
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await
        .unwrap();

    // Verify .bin file exists using new sharded path
    let range_file_path = disk_cache.get_new_range_file_path(cache_key, range_start, range_end);
    assert!(
        range_file_path.exists(),
        ".bin file should exist after storing range"
    );

    // Evict the range
    cache_manager
        .evict_range(cache_key, range_start, range_end)
        .await
        .unwrap();

    // Verify .bin file was deleted
    assert!(
        !range_file_path.exists(),
        ".bin file should be deleted after eviction"
    );

    println!(".bin file deletion on eviction verified");
}

/// Test that .meta files are deleted when the last range is evicted
/// Requirement 9.5: Ensure .meta file deletion when last range is deleted
#[tokio::test]
async fn test_meta_file_deletion_on_last_range_eviction() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            lock_acquire_wait_budget: Duration::from_secs(60),
            ..Default::default()
        },
        10.0,
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        std::time::Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache,
        std::time::Duration::from_secs(10), // ram_cache_flush_interval (Req 19)
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    );

    // Create a range with .bin file
    let cache_key = "test-bucket/test-object";
    let range_start = 0u64;
    let range_end = 1023u64;

    let mut disk_cache = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
    );

    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        1024,
        Some("application/octet-stream".to_string()),
    );

    let data = vec![0u8; 1024];
    disk_cache
        .store_range(
            cache_key,
            range_start,
            range_end,
            &data,
            object_metadata,
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await
        .unwrap();

    // Verify .meta file exists using new sharded path
    let metadata_path = disk_cache.get_new_metadata_file_path(cache_key);
    assert!(
        metadata_path.exists(),
        ".meta file should exist after storing range"
    );

    // Evict the only range
    cache_manager
        .evict_range(cache_key, range_start, range_end)
        .await
        .unwrap();

    // Verify .meta file was deleted (since it was the last range)
    assert!(
        !metadata_path.exists(),
        ".meta file should be deleted when last range is evicted"
    );

    println!(".meta file deletion on last range eviction verified");
}

/// Test that stale eviction locks are broken after 60 seconds
/// Requirement 9.3: Handle stale eviction lock (break if >60s old)
#[tokio::test]
async fn test_stale_eviction_lock_handling() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            lock_acquire_wait_budget: Duration::from_secs(60),
            ..Default::default()
        },
        10.0,
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        std::time::Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache,
        std::time::Duration::from_secs(10), // ram_cache_flush_interval (Req 19)
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    );

    // Create a stale eviction lock
    let lock_dir = cache_dir.join("locks");
    std::fs::create_dir_all(&lock_dir).unwrap();
    let lock_path = lock_dir.join("global_eviction.lock");

    let stale_lock = s3_proxy::cache::EvictionLockPayload {
        uuid: "stale-uuid-12345".to_string(),
        acquired_at_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 61_000, // 61 seconds ago
        hostname: "stale-host".to_string(),
    };

    let lock_json = serde_json::to_string(&stale_lock).unwrap();
    std::fs::write(&lock_path, lock_json).unwrap();

    // Try to acquire lock - should break stale lock and succeed
    let result = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .unwrap();
    assert!(
        result,
        "Should break stale eviction lock and acquire new lock"
    );

    // Verify new lock was created
    assert!(lock_path.exists(), "New eviction lock should exist");

    // Clean up
    cache_manager.release_global_eviction_lock().await.unwrap();

    println!("Stale eviction lock handling verified");
}

/// Test that multiple ranges can be evicted and only the last one deletes .meta
/// Requirement 9.4 and 9.5: Proper file cleanup during eviction
#[tokio::test]
async fn test_multiple_range_eviction_meta_deletion() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = CacheManager::new_with_shared_storage(
        cache_dir.clone(),
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            lock_acquire_wait_budget: Duration::from_secs(60),
            ..Default::default()
        },
        10.0,
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        std::time::Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache,
        std::time::Duration::from_secs(10), // ram_cache_flush_interval (Req 19)
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    );

    // Create multiple ranges
    let cache_key = "test-bucket/test-object";
    let mut disk_cache = s3_proxy::disk_cache::DiskCacheManager::new(
        cache_dir.clone(),
        true,
        1024,
        false,
        1_048_576,
    );

    let object_metadata = s3_proxy::cache_types::ObjectMetadata::new(
        "test-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        2048,
        Some("application/octet-stream".to_string()),
    );

    // Store two ranges
    let data1 = vec![0u8; 1024];
    disk_cache
        .store_range(
            cache_key,
            0,
            1023,
            &data1,
            object_metadata.clone(),
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await
        .unwrap();

    let data2 = vec![1u8; 1024];
    disk_cache
        .store_range(
            cache_key,
            1024,
            2047,
            &data2,
            object_metadata,
            std::time::Duration::from_secs(315360000),
            true,
        )
        .await
        .unwrap();

    // Verify both .bin files and .meta file exist using new sharded paths
    let range1_path = disk_cache.get_new_range_file_path(cache_key, 0, 1023);
    let range2_path = disk_cache.get_new_range_file_path(cache_key, 1024, 2047);
    let metadata_path = disk_cache.get_new_metadata_file_path(cache_key);

    assert!(range1_path.exists(), "Range 1 .bin file should exist");
    assert!(range2_path.exists(), "Range 2 .bin file should exist");
    assert!(metadata_path.exists(), ".meta file should exist");

    // Evict first range
    cache_manager.evict_range(cache_key, 0, 1023).await.unwrap();

    // Verify first .bin deleted, but .meta still exists (second range remains)
    assert!(!range1_path.exists(), "Range 1 .bin file should be deleted");
    assert!(range2_path.exists(), "Range 2 .bin file should still exist");
    assert!(
        metadata_path.exists(),
        ".meta file should still exist (second range remains)"
    );

    // Evict second range
    cache_manager
        .evict_range(cache_key, 1024, 2047)
        .await
        .unwrap();

    // Verify second .bin deleted and .meta deleted (no ranges remain)
    assert!(!range2_path.exists(), "Range 2 .bin file should be deleted");
    assert!(
        !metadata_path.exists(),
        ".meta file should be deleted (no ranges remain)"
    );

    println!("Multiple range eviction and .meta deletion verified");
}
