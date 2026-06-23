//! Unit tests for UUID-fenced eviction lock behavior.
//!
//! This test suite verifies:
//! - Requirement 5.1: Acquisition writes UUID + acquired_at_ms + hostname to lockfile
//! - Requirement 5.2: verify_eviction_fence() returns Ok when UUID matches
//! - Requirement 5.3: verify_eviction_fence() returns Err(EvictionFenceLost) on UUID mismatch
//! - Requirement 5.4: Release only truncates lockfile if UUID still matches
//! - Requirement 5.5: Stale takeover via eviction_lock_timeout
//!
//! **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5**

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager, EvictionLockPayload};
use s3_proxy::config::SharedStorageConfig;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Helper: create a CacheManager with shared storage and a short eviction lock timeout
fn create_cache_manager_with_timeout(
    cache_dir: std::path::PathBuf,
    eviction_lock_timeout: Duration,
) -> CacheManager {
    CacheManager::new_with_shared_storage(
        cache_dir,
        false,
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            lock_timeout: eviction_lock_timeout,
            lock_refresh_interval: Duration::from_secs(30),
            ..Default::default()
        },
        10.0,
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        Duration::from_secs(60),            // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache,
        std::time::Duration::from_secs(10), // ram_cache_flush_interval (Req 19)
        64,                                 // ram_cache_shard_count
    )
}

/// Helper: create locks directory
fn setup_locks_dir(cache_dir: &std::path::Path) {
    std::fs::create_dir_all(cache_dir.join("locks")).expect("Failed to create locks directory");
}

// ============================================================================
// Requirement 5.1: Acquisition writes UUID + acquired_at_ms + hostname
// ============================================================================

/// Verify that lock acquisition writes a valid EvictionLockPayload with all required fields.
#[tokio::test]
async fn test_acquisition_writes_uuid_acquired_at_and_hostname() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired, "Should acquire lock successfully");

    // Read and parse the lockfile
    let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
    let content = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
    let payload: EvictionLockPayload =
        serde_json::from_str(&content).expect("Lock file should contain valid JSON");

    // Verify UUID is a non-empty string (UUID v4 format: 36 chars with dashes)
    assert!(!payload.uuid.is_empty(), "UUID must not be empty");
    assert_eq!(
        payload.uuid.len(),
        36,
        "UUID should be 36 characters (v4 format)"
    );
    assert_eq!(
        payload.uuid.chars().filter(|c| *c == '-').count(),
        4,
        "UUID v4 should have 4 dashes"
    );

    // Verify acquired_at_ms is recent (within last 5 seconds)
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let elapsed = now_ms.saturating_sub(payload.acquired_at_ms);
    assert!(
        elapsed < 5000,
        "acquired_at_ms should be within last 5 seconds, got elapsed={}ms",
        elapsed
    );

    // Verify hostname is non-empty
    assert!(!payload.hostname.is_empty(), "Hostname must not be empty");

    // Clean up
    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");
}

// ============================================================================
// Requirement 5.2: verify_eviction_fence() returns Ok when UUID matches
// ============================================================================

/// After acquiring the lock, verify_eviction_fence() should return Ok because
/// the lockfile UUID matches the holder's UUID.
#[tokio::test]
async fn test_verify_eviction_fence_ok_when_uuid_matches() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired, "Should acquire lock");

    // Fence verification should succeed — UUID in lockfile matches our UUID
    let result = cache_manager.verify_eviction_fence();
    assert!(
        result.is_ok(),
        "verify_eviction_fence should return Ok when UUID matches, got: {:?}",
        result.err()
    );

    // Clean up
    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");
}

/// Multiple consecutive verify_eviction_fence() calls should all succeed
/// while the lock is held and the lockfile is unchanged.
#[tokio::test]
async fn test_verify_eviction_fence_ok_multiple_calls() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired);

    // Multiple verifications should all pass
    for i in 0..5 {
        let result = cache_manager.verify_eviction_fence();
        assert!(
            result.is_ok(),
            "verify_eviction_fence call {} should succeed",
            i
        );
    }

    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");
}

// ============================================================================
// Requirement 5.3: verify_eviction_fence() returns Err on UUID mismatch
// ============================================================================

/// Simulate another instance taking over the lock by overwriting the lockfile
/// with a different UUID. verify_eviction_fence() should return Err(EvictionFenceLost).
#[tokio::test]
async fn test_verify_eviction_fence_err_on_uuid_mismatch() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired, "Should acquire lock");

    // Verify fence is initially valid
    assert!(cache_manager.verify_eviction_fence().is_ok());

    // Simulate another instance taking over: overwrite lockfile with different UUID
    let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
    let intruder_payload = EvictionLockPayload {
        uuid: "intruder-uuid-aaaabbbb-cccc-dddd".to_string(),
        acquired_at_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        hostname: "intruder-host".to_string(),
    };
    let intruder_json =
        serde_json::to_string(&intruder_payload).expect("Failed to serialize intruder payload");
    std::fs::write(&lock_file_path, intruder_json).expect("Failed to overwrite lock file");

    // Now verify_eviction_fence should detect the mismatch and return Err
    let result = cache_manager.verify_eviction_fence();
    assert!(
        result.is_err(),
        "verify_eviction_fence should return Err when UUID doesn't match"
    );

    // Verify the error is specifically EvictionFenceLost
    let err = result.unwrap_err();
    let err_msg = format!("{}", err);
    assert!(
        err_msg.contains("UUID mismatch") || err_msg.contains("fence"),
        "Error should indicate fence lost / UUID mismatch, got: {}",
        err_msg
    );
}

/// verify_eviction_fence() should return Err when the lockfile is deleted mid-pass.
#[tokio::test]
async fn test_verify_eviction_fence_err_when_lockfile_deleted() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired);

    // Delete the lockfile to simulate filesystem disruption
    let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
    std::fs::remove_file(&lock_file_path).expect("Failed to remove lock file");

    // verify_eviction_fence should fail because it can't read the lockfile
    let result = cache_manager.verify_eviction_fence();
    assert!(
        result.is_err(),
        "verify_eviction_fence should return Err when lockfile is missing"
    );
}

/// verify_eviction_fence() should return Err when no UUID is set (lock not held).
#[tokio::test]
async fn test_verify_eviction_fence_err_when_no_uuid_set() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    // Don't acquire the lock — verify_eviction_fence should fail immediately
    let result = cache_manager.verify_eviction_fence();
    assert!(
        result.is_err(),
        "verify_eviction_fence should return Err when lock is not held"
    );

    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("No eviction UUID set") || err_msg.contains("lock not held"),
        "Error should indicate no UUID / lock not held, got: {}",
        err_msg
    );
}

// ============================================================================
// Requirement 5.4: Release only truncates lockfile if UUID still matches
// ============================================================================

/// When the holder releases and its UUID still matches, the lockfile should be truncated.
#[tokio::test]
async fn test_release_truncates_lockfile_when_uuid_matches() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired);

    let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");

    // Verify lockfile has content before release
    let content_before =
        std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
    assert!(
        !content_before.is_empty(),
        "Lock file should have content before release"
    );

    // Release the lock — should truncate because UUID matches
    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");

    // Verify lockfile is truncated (empty)
    let content_after = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
    assert!(
        content_after.is_empty(),
        "Lock file should be truncated (empty) after release when UUID matched"
    );
}

/// When another instance has overwritten the lockfile, release should NOT truncate it.
#[tokio::test]
async fn test_release_does_not_truncate_when_uuid_changed() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Lock acquisition should not error");
    assert!(acquired);

    // Simulate another instance taking over: overwrite lockfile with different UUID
    let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
    let other_payload = EvictionLockPayload {
        uuid: "other-instance-uuid-12345678".to_string(),
        acquired_at_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        hostname: "other-host".to_string(),
    };
    let other_json =
        serde_json::to_string(&other_payload).expect("Failed to serialize other payload");
    std::fs::write(&lock_file_path, &other_json).expect("Failed to overwrite lock file");

    // Release the lock — should NOT truncate because UUID doesn't match
    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock gracefully");

    // Verify lockfile still contains the other instance's payload (not truncated)
    let content_after = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
    assert!(
        !content_after.is_empty(),
        "Lock file should NOT be truncated when UUID doesn't match"
    );

    let preserved_payload: EvictionLockPayload =
        serde_json::from_str(&content_after).expect("Lock file should still be valid JSON");
    assert_eq!(
        preserved_payload.uuid, "other-instance-uuid-12345678",
        "Other instance's UUID should be preserved"
    );
}

// ============================================================================
// Requirement 5.5: Stale takeover via eviction_lock_timeout
// ============================================================================

/// When acquired_at_ms + timeout < now, the lock is considered stale and eligible
/// for takeover by another instance.
#[tokio::test]
async fn test_stale_takeover_when_lock_exceeds_timeout() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    // Use a short timeout (5 seconds) so we can simulate staleness easily
    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(5));
    setup_locks_dir(&cache_dir);

    // Write a lock that is well past the 5-second timeout
    let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
    let stale_time_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 10_000; // 10 seconds ago — exceeds 5s timeout

    let stale_payload = EvictionLockPayload {
        uuid: "stale-holder-uuid-99999".to_string(),
        acquired_at_ms: stale_time_ms,
        hostname: "stale-host".to_string(),
    };
    let stale_json =
        serde_json::to_string(&stale_payload).expect("Failed to serialize stale payload");
    std::fs::write(&lock_file_path, stale_json).expect("Failed to write stale lock");

    // Attempt acquisition — should succeed via stale takeover
    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("Stale takeover should not error");
    assert!(acquired, "Should acquire stale lock via timeout takeover");

    // Verify the lockfile now has a new UUID (not the stale one)
    let content = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
    let new_payload: EvictionLockPayload =
        serde_json::from_str(&content).expect("Lock file should be valid JSON");
    assert_ne!(
        new_payload.uuid, "stale-holder-uuid-99999",
        "UUID should be replaced after stale takeover"
    );
    assert!(!new_payload.uuid.is_empty(), "New UUID should be set");

    // Verify fence is valid after takeover
    assert!(
        cache_manager.verify_eviction_fence().is_ok(),
        "Fence should be valid after stale takeover"
    );

    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");
}

/// When acquired_at_ms + timeout > now, the lock is NOT stale and acquisition should fail.
#[tokio::test]
async fn test_no_takeover_when_lock_is_fresh() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    // Use a 300-second timeout
    let cache_manager =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    setup_locks_dir(&cache_dir);

    // First instance acquires the lock
    let acquired = cache_manager
        .try_acquire_global_eviction_lock()
        .await
        .expect("First acquisition should not error");
    assert!(acquired, "First instance should acquire lock");

    // Second instance tries to acquire — should fail because lock is fresh
    let cache_manager2 =
        create_cache_manager_with_timeout(cache_dir.clone(), Duration::from_secs(300));
    let acquired2 = cache_manager2
        .try_acquire_global_eviction_lock()
        .await
        .expect("Second acquisition should not error");
    assert!(
        !acquired2,
        "Second instance should NOT acquire lock when it's fresh (not stale)"
    );

    // Clean up
    cache_manager
        .release_global_eviction_lock()
        .await
        .expect("Should release lock");
}

// ============================================================================
// EvictionLockPayload serialization/deserialization round-trip
// ============================================================================

/// Verify that EvictionLockPayload serializes and deserializes correctly.
#[test]
fn test_eviction_lock_payload_serde_roundtrip() {
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let payload = EvictionLockPayload {
        uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        acquired_at_ms: now_ms,
        hostname: "test-host-abc".to_string(),
    };

    // Serialize
    let json = serde_json::to_string(&payload).expect("Serialization should succeed");

    // Deserialize
    let deserialized: EvictionLockPayload =
        serde_json::from_str(&json).expect("Deserialization should succeed");

    // Verify all fields round-trip correctly
    assert_eq!(deserialized.uuid, payload.uuid);
    assert_eq!(deserialized.acquired_at_ms, payload.acquired_at_ms);
    assert_eq!(deserialized.hostname, payload.hostname);
}

/// Verify that the JSON format contains the expected field names.
#[test]
fn test_eviction_lock_payload_json_field_names() {
    let payload = EvictionLockPayload {
        uuid: "test-uuid".to_string(),
        acquired_at_ms: 1715180000000,
        hostname: "my-host".to_string(),
    };

    let json = serde_json::to_string(&payload).expect("Serialization should succeed");

    // Verify expected field names are present in the JSON
    assert!(
        json.contains("\"uuid\""),
        "JSON should contain 'uuid' field"
    );
    assert!(
        json.contains("\"acquired_at_ms\""),
        "JSON should contain 'acquired_at_ms' field"
    );
    assert!(
        json.contains("\"hostname\""),
        "JSON should contain 'hostname' field"
    );

    // Verify values
    assert!(json.contains("\"test-uuid\""));
    assert!(json.contains("1715180000000"));
    assert!(json.contains("\"my-host\""));
}

/// Verify deserialization from a known JSON string (compatibility check).
#[test]
fn test_eviction_lock_payload_deserialize_from_known_json() {
    let json = r#"{"uuid":"550e8400-e29b-41d4-a716-446655440000","acquired_at_ms":1715180000000,"hostname":"host-abc"}"#;

    let payload: EvictionLockPayload =
        serde_json::from_str(json).expect("Should deserialize known JSON format");

    assert_eq!(payload.uuid, "550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(payload.acquired_at_ms, 1715180000000);
    assert_eq!(payload.hostname, "host-abc");
}
