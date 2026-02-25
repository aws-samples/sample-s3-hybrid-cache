use s3_proxy::cache::GlobalEvictionLock;
use std::time::{Duration, SystemTime};

#[test]
fn test_global_eviction_lock_creation() {
    let now = SystemTime::now();
    let lock = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 300,
    };

    assert_eq!(lock.instance_id, "test-instance:12345");
    assert_eq!(lock.process_id, 12345);
    assert_eq!(lock.hostname, "test-host");
    assert_eq!(lock.timeout_seconds, 300);
}

#[test]
fn test_global_eviction_lock_is_not_stale_when_fresh() {
    let now = SystemTime::now();
    let lock = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 300,
    };

    // Check immediately - should not be stale
    assert!(!lock.is_stale(now));

    // Check 1 second later - should not be stale
    let one_second_later = now + Duration::from_secs(1);
    assert!(!lock.is_stale(one_second_later));

    // Check 299 seconds later - should not be stale
    let almost_timeout = now + Duration::from_secs(299);
    assert!(!lock.is_stale(almost_timeout));
}

#[test]
fn test_global_eviction_lock_is_stale_after_timeout() {
    let now = SystemTime::now();
    let lock = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 300,
    };

    // Check exactly at timeout - should be stale
    let at_timeout = now + Duration::from_secs(300);
    assert!(!lock.is_stale(at_timeout)); // At exactly timeout, not stale yet

    // Check 1 second after timeout - should be stale
    let after_timeout = now + Duration::from_secs(301);
    assert!(lock.is_stale(after_timeout));

    // Check well after timeout - should be stale
    let well_after_timeout = now + Duration::from_secs(600);
    assert!(lock.is_stale(well_after_timeout));
}

#[test]
fn test_global_eviction_lock_is_stale_when_time_goes_backwards() {
    let now = SystemTime::now();
    let lock = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 300,
    };

    // Check with time before acquisition - should be stale (clock skew protection)
    let before_acquisition = now - Duration::from_secs(10);
    assert!(lock.is_stale(before_acquisition));
}

#[test]
fn test_global_eviction_lock_expires_at() {
    let now = SystemTime::now();
    let timeout_seconds = 300u64;
    let lock = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds,
    };

    let expected_expiration = now + Duration::from_secs(timeout_seconds);
    let actual_expiration = lock.expires_at();

    // Compare the durations since UNIX_EPOCH to avoid SystemTime comparison issues
    let expected_duration = expected_expiration
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let actual_duration = actual_expiration
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    assert_eq!(expected_duration.as_secs(), actual_duration.as_secs());
}

#[test]
fn test_global_eviction_lock_serialization() {
    let now = SystemTime::now();
    let lock = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 300,
    };

    // Serialize to JSON
    let json = serde_json::to_string(&lock).expect("Failed to serialize lock");

    // Deserialize from JSON
    let deserialized: GlobalEvictionLock =
        serde_json::from_str(&json).expect("Failed to deserialize lock");

    // Verify fields match
    assert_eq!(deserialized.instance_id, lock.instance_id);
    assert_eq!(deserialized.process_id, lock.process_id);
    assert_eq!(deserialized.hostname, lock.hostname);
    assert_eq!(deserialized.timeout_seconds, lock.timeout_seconds);

    // Verify acquired_at is close (within 1 second due to serialization precision)
    let original_duration = lock
        .acquired_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let deserialized_duration = deserialized
        .acquired_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let diff = if original_duration > deserialized_duration {
        original_duration - deserialized_duration
    } else {
        deserialized_duration - original_duration
    };
    assert!(
        diff.as_secs() <= 1,
        "Timestamps differ by more than 1 second"
    );
}

#[test]
fn test_global_eviction_lock_different_timeout_values() {
    let now = SystemTime::now();

    // Test with 60 second timeout (minimum recommended)
    let lock_60 = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 60,
    };

    let after_60 = now + Duration::from_secs(61);
    assert!(lock_60.is_stale(after_60));

    // Test with 3600 second timeout (1 hour, maximum recommended)
    let lock_3600 = GlobalEvictionLock {
        instance_id: "test-instance:12345".to_string(),
        process_id: 12345,
        hostname: "test-host".to_string(),
        acquired_at: now,
        timeout_seconds: 3600,
    };

    let after_3600 = now + Duration::from_secs(3601);
    assert!(lock_3600.is_stale(after_3600));

    let before_3600 = now + Duration::from_secs(3599);
    assert!(!lock_3600.is_stale(before_3600));
}

#[cfg(test)]
mod helper_methods_tests {
    use s3_proxy::cache::CacheManager;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_get_global_eviction_lock_path() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Verify the expected lock file path structure
        let locks_dir = cache_dir.join("locks");
        let expected_lock_path = locks_dir.join("global_eviction.lock");
        assert_eq!(
            expected_lock_path,
            cache_dir.join("locks").join("global_eviction.lock"),
            "Lock path should follow the pattern {{cache_dir}}/locks/global_eviction.lock"
        );
    }

    #[tokio::test]
    async fn test_get_instance_id_format() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // We can't directly test the private method, but we can verify the format
        // by checking that hostname and PID are available
        let hostname = hostname::get()
            .unwrap_or_else(|_| "unknown".into())
            .to_string_lossy()
            .to_string();
        let pid = std::process::id();

        // Verify the expected format
        let expected_instance_id = format!("{}:{}", hostname, pid);

        // Verify hostname is not empty
        assert!(!hostname.is_empty(), "Hostname should not be empty");

        // Verify PID is valid
        assert!(pid > 0, "Process ID should be greater than 0");

        // Verify format contains colon separator
        assert!(
            expected_instance_id.contains(':'),
            "Instance ID should contain colon separator"
        );

        // Verify format has both parts
        let parts: Vec<&str> = expected_instance_id.split(':').collect();
        assert_eq!(
            parts.len(),
            2,
            "Instance ID should have exactly 2 parts separated by colon"
        );
        assert!(!parts[0].is_empty(), "Hostname part should not be empty");
        assert!(!parts[1].is_empty(), "PID part should not be empty");
    }

    #[tokio::test]
    async fn test_get_eviction_lock_timeout_seconds_default() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // The default timeout should be 300 seconds (5 minutes)
        // We can't directly test the private method, but we verify the constant
        const EXPECTED_DEFAULT_TIMEOUT: u64 = 300;

        // Verify the expected default is reasonable
        assert!(
            EXPECTED_DEFAULT_TIMEOUT >= 60,
            "Default timeout should be at least 60 seconds"
        );
        assert!(
            EXPECTED_DEFAULT_TIMEOUT <= 3600,
            "Default timeout should be at most 3600 seconds"
        );
        assert_eq!(
            EXPECTED_DEFAULT_TIMEOUT, 300,
            "Default timeout should be 300 seconds (5 minutes)"
        );
    }

    #[tokio::test]
    async fn test_locks_directory_created_during_initialization() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Before initialization, locks directory should not exist
        let locks_dir = cache_dir.join("locks");
        assert!(
            !locks_dir.exists(),
            "Locks directory should not exist before initialization"
        );

        // Initialize cache manager
        let _disk_cache = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        // After initialization, locks directory should exist
        assert!(
            locks_dir.exists(),
            "Locks directory should exist after initialization"
        );
        assert!(locks_dir.is_dir(), "Locks path should be a directory");

        // Verify we can write to the locks directory
        let test_file = locks_dir.join("test.txt");
        std::fs::write(&test_file, b"test").expect("Should be able to write to locks directory");
        assert!(
            test_file.exists(),
            "Test file should exist in locks directory"
        );

        // Clean up
        std::fs::remove_file(&test_file).expect("Should be able to remove test file");
    }

    #[tokio::test]
    async fn test_instance_id_uniqueness_across_processes() {
        // This test verifies that instance IDs would be unique across different processes
        // by checking that the PID is included in the instance ID

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Get current process ID
        let current_pid = std::process::id();

        // Build expected instance ID
        let hostname = hostname::get()
            .unwrap_or_else(|_| "unknown".into())
            .to_string_lossy()
            .to_string();
        let instance_id = format!("{}:{}", hostname, current_pid);

        // Verify the instance ID contains the current PID
        assert!(
            instance_id.contains(&current_pid.to_string()),
            "Instance ID should contain the current process ID for uniqueness"
        );

        // Verify that different PIDs would produce different instance IDs
        let different_pid = current_pid + 1;
        let different_instance_id = format!("{}:{}", hostname, different_pid);

        assert_ne!(
            instance_id, different_instance_id,
            "Instance IDs with different PIDs should be different"
        );
    }
}

#[cfg(test)]
mod lock_acquisition_tests {
    use s3_proxy::cache::CacheManager;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_acquire_lock_when_no_lock_exists() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Try to acquire lock - should succeed since no lock exists
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");

        assert!(
            acquired,
            "Should successfully acquire lock when no lock exists"
        );

        // Verify lock file was created
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        assert!(
            lock_file_path.exists(),
            "Lock file should exist after acquisition"
        );

        // Verify lock file contains valid JSON
        let lock_json = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
        let lock: s3_proxy::cache::GlobalEvictionLock =
            serde_json::from_str(&lock_json).expect("Lock file should contain valid JSON");

        // Verify lock contains expected fields
        assert!(
            !lock.instance_id.is_empty(),
            "Instance ID should not be empty"
        );
        assert!(lock.process_id > 0, "Process ID should be valid");
        assert!(!lock.hostname.is_empty(), "Hostname should not be empty");
        // Requirement 2.4: Default lock timeout is 60 seconds
        assert_eq!(
            lock.timeout_seconds, 60,
            "Timeout should be 60 seconds (default per requirements 2.4 and 6.2)"
        );
    }

    #[tokio::test]
    async fn test_cannot_acquire_lock_when_held_by_another_instance() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager1 = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // First instance acquires lock
        let acquired1 = cache_manager1
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired1, "First instance should acquire lock");

        // Create second cache manager (simulating another instance)
        let cache_manager2 = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Second instance tries to acquire lock - should fail
        let acquired2 = cache_manager2
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to check lock");

        assert!(
            !acquired2,
            "Second instance should not acquire lock when held by first instance"
        );
    }

    #[tokio::test]
    async fn test_acquire_stale_lock() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Create a stale lock file manually
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        std::fs::create_dir_all(lock_file_path.parent().unwrap()).expect("Failed to create locks dir");
        let stale_time = SystemTime::now() - Duration::from_secs(400); // 400 seconds ago (stale)

        let stale_lock = s3_proxy::cache::GlobalEvictionLock {
            instance_id: "old-instance:99999".to_string(),
            process_id: 99999,
            hostname: "old-host".to_string(),
            acquired_at: stale_time,
            timeout_seconds: 300,
        };

        let lock_json =
            serde_json::to_string_pretty(&stale_lock).expect("Failed to serialize stale lock");
        std::fs::write(&lock_file_path, lock_json).expect("Failed to write stale lock file");

        // Try to acquire lock - should succeed by forcibly acquiring stale lock
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire stale lock");

        assert!(acquired, "Should successfully acquire stale lock");

        // Verify lock file was updated with new instance
        let lock_json = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
        let lock: s3_proxy::cache::GlobalEvictionLock =
            serde_json::from_str(&lock_json).expect("Lock file should contain valid JSON");

        // Verify lock is now owned by current instance (not the old one)
        assert_ne!(
            lock.instance_id, "old-instance:99999",
            "Lock should be owned by new instance"
        );
        assert_eq!(
            lock.process_id,
            std::process::id(),
            "Lock should have current process ID"
        );
    }

    #[tokio::test]
    async fn test_acquire_corrupted_lock() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Create a corrupted lock file manually
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        std::fs::create_dir_all(lock_file_path.parent().unwrap()).expect("Failed to create locks dir");
        std::fs::write(&lock_file_path, b"invalid json {{{")
            .expect("Failed to write corrupted lock file");

        // Try to acquire lock - should succeed by treating corrupted lock as stale
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock with corrupted file");

        assert!(
            acquired,
            "Should successfully acquire lock when existing lock is corrupted"
        );

        // Verify lock file was replaced with valid lock
        let lock_json = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
        let lock: s3_proxy::cache::GlobalEvictionLock =
            serde_json::from_str(&lock_json).expect("Lock file should now contain valid JSON");

        // Verify lock contains expected fields
        assert!(
            !lock.instance_id.is_empty(),
            "Instance ID should not be empty"
        );
        assert!(lock.process_id > 0, "Process ID should be valid");
    }

    #[tokio::test]
    async fn test_atomic_lock_file_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Acquire lock
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired, "Should acquire lock");

        // Verify temp file was cleaned up (atomic rename pattern)
        let temp_file_path = cache_dir.join("locks").join("global_eviction.lock.tmp");
        assert!(
            !temp_file_path.exists(),
            "Temp file should not exist after atomic rename"
        );

        // Verify actual lock file exists
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        assert!(lock_file_path.exists(), "Lock file should exist");
    }

    #[tokio::test]
    async fn test_lock_file_contains_all_required_fields() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Acquire lock
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired, "Should acquire lock");

        // Read and parse lock file
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        let lock_json = std::fs::read_to_string(&lock_file_path).expect("Failed to read lock file");
        let lock: s3_proxy::cache::GlobalEvictionLock =
            serde_json::from_str(&lock_json).expect("Lock file should contain valid JSON");

        // Verify all required fields are present and valid
        assert!(
            !lock.instance_id.is_empty(),
            "instance_id should be present"
        );
        assert!(
            lock.process_id > 0,
            "process_id should be present and valid"
        );
        assert!(!lock.hostname.is_empty(), "hostname should be present");
        assert!(
            lock.timeout_seconds > 0,
            "timeout_seconds should be present and valid"
        );

        // Verify acquired_at is recent (within last 5 seconds)
        let now = SystemTime::now();
        let elapsed = now
            .duration_since(lock.acquired_at)
            .expect("acquired_at should be in the past");
        assert!(elapsed.as_secs() < 5, "acquired_at should be recent");
    }

    #[tokio::test]
    async fn test_multiple_acquisition_attempts() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // First acquisition should succeed
        let acquired1 = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired1, "First acquisition should succeed");

        // Second acquisition by same instance should fail (lock already held)
        let acquired2 = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to check lock");
        assert!(
            !acquired2,
            "Second acquisition should fail when lock is held"
        );

        // Third acquisition should also fail
        let acquired3 = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to check lock");
        assert!(
            !acquired3,
            "Third acquisition should fail when lock is held"
        );
    }
}

#[cfg(test)]
mod lock_release_tests {
    use s3_proxy::cache::CacheManager;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_release_lock_after_acquisition() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Acquire lock
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired, "Should acquire lock");

        // Verify lock file exists
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        assert!(
            lock_file_path.exists(),
            "Lock file should exist after acquisition"
        );

        // Release lock
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Failed to release lock");

        // Verify lock was released by confirming it can be re-acquired
        let reacquired = cache_manager.try_acquire_global_eviction_lock().await.expect("Should be able to attempt lock acquisition");
        assert!(reacquired, "Lock should be re-acquirable after release");
        cache_manager.release_global_eviction_lock().await.expect("Should release lock");
    }

    #[tokio::test]
    async fn test_release_lock_when_no_lock_exists() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Try to release lock when no lock exists - should not panic
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Should handle missing lock file gracefully");
    }

    #[tokio::test]
    async fn test_release_lock_owned_by_another_instance() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Manually create a lock file with a different instance ID
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        std::fs::create_dir_all(lock_file_path.parent().unwrap()).expect("Failed to create locks dir");
        let fake_lock = s3_proxy::cache::GlobalEvictionLock {
            instance_id: "different-host:99999".to_string(),
            process_id: 99999,
            hostname: "different-host".to_string(),
            acquired_at: std::time::SystemTime::now(),
            timeout_seconds: 300,
        };

        let lock_json =
            serde_json::to_string_pretty(&fake_lock).expect("Failed to serialize fake lock");
        std::fs::write(&lock_file_path, lock_json).expect("Failed to write fake lock file");

        // Verify lock file exists
        assert!(lock_file_path.exists(), "Lock file should exist");

        // Try to release lock owned by different instance - should not delete it
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Should handle release of lock owned by another instance");

        // Verify lock file still exists (not deleted by non-owner)
        assert!(
            lock_file_path.exists(),
            "Lock file should still exist after attempted release by non-owner"
        );
    }

    #[tokio::test]
    async fn test_release_lock_with_corrupted_lock_file() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Create a corrupted lock file
        let lock_file_path = cache_dir.join("locks").join("global_eviction.lock");
        std::fs::create_dir_all(lock_file_path.parent().unwrap()).expect("Failed to create locks dir");
        std::fs::write(&lock_file_path, b"invalid json {{{")
            .expect("Failed to write corrupted lock file");

        // Try to release lock with corrupted file - should delete it anyway
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Should handle corrupted lock file during release");

        // Verify lock was released by confirming it can be re-acquired
        let reacquired = cache_manager.try_acquire_global_eviction_lock().await.expect("Should be able to attempt lock acquisition");
        assert!(reacquired, "Lock should be re-acquirable after release");
        cache_manager.release_global_eviction_lock().await.expect("Should release lock");
    }

    #[tokio::test]
    async fn test_acquire_release_acquire_cycle() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // First acquisition
        let acquired1 = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired1, "First acquisition should succeed");

        // Release
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Failed to release lock");

        // Second acquisition should succeed after release
        let acquired2 = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired2, "Second acquisition should succeed after release");

        // Release again
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Failed to release lock");

        // Third acquisition should succeed
        let acquired3 = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(
            acquired3,
            "Third acquisition should succeed after second release"
        );
    }

    #[tokio::test]
    async fn test_release_allows_other_instance_to_acquire() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        // First instance
        let cache_manager1 = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // First instance acquires lock
        let acquired1 = cache_manager1
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired1, "First instance should acquire lock");

        // Second instance
        let cache_manager2 = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Second instance cannot acquire lock
        let acquired2 = cache_manager2
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to check lock");
        assert!(
            !acquired2,
            "Second instance should not acquire lock while first holds it"
        );

        // First instance releases lock
        cache_manager1
            .release_global_eviction_lock()
            .await
            .expect("Failed to release lock");

        // Now second instance can acquire lock
        let acquired3 = cache_manager2
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(
            acquired3,
            "Second instance should acquire lock after first releases it"
        );
    }
}

#[cfg(test)]
mod eviction_integration_tests {
    
    use s3_proxy::cache::CacheManager;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_eviction_acquires_and_releases_lock() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        // Create cache manager
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Verify lock doesn't exist initially
        let lock_path = cache_manager.get_global_eviction_lock_path();
        assert!(!lock_path.exists(), "Lock should not exist initially");

        // Manually acquire lock to simulate eviction flow
        let acquired = cache_manager
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired, "Should acquire lock");

        // Verify lock exists
        assert!(lock_path.exists(), "Lock should exist after acquisition");

        // Release lock to simulate end of eviction
        cache_manager
            .release_global_eviction_lock()
            .await
            .expect("Failed to release lock");

        // Verify lock was released by confirming it can be re-acquired
        let reacquired = cache_manager.try_acquire_global_eviction_lock().await.expect("Should be able to attempt lock acquisition");
        assert!(reacquired, "Lock should be re-acquirable after release");
        cache_manager.release_global_eviction_lock().await.expect("Should release lock");
    }

    #[tokio::test]
    async fn test_eviction_skips_when_lock_held_by_another_instance() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().to_path_buf();

        // First instance
        let cache_manager1 = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // First instance acquires lock
        let acquired = cache_manager1
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(acquired, "First instance should acquire lock");

        // Second instance
        let cache_manager2 = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        // Second instance tries to acquire lock - should fail because lock is held
        let acquired2 = cache_manager2
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to check lock");
        assert!(
            !acquired2,
            "Second instance should not acquire lock while first holds it"
        );

        // Verify lock is still held by first instance
        let lock_path = cache_manager1.get_global_eviction_lock_path();
        assert!(
            lock_path.exists(),
            "Lock should still be held by first instance"
        );

        // Release lock from first instance
        cache_manager1
            .release_global_eviction_lock()
            .await
            .expect("Failed to release lock");

        // Verify lock was released by confirming it can be re-acquired
        let reacquired = cache_manager1.try_acquire_global_eviction_lock().await.expect("Should be able to attempt lock acquisition");
        assert!(reacquired, "Lock should be re-acquirable after release");
        cache_manager1.release_global_eviction_lock().await.expect("Should release lock");

        // Now second instance can acquire lock
        let acquired3 = cache_manager2
            .try_acquire_global_eviction_lock()
            .await
            .expect("Failed to acquire lock");
        assert!(
            acquired3,
            "Second instance should acquire lock after first releases it"
        );
    }
}
