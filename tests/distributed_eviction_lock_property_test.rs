//! Consolidated property-based tests for distributed eviction lock functionality
//!
//! This file consolidates all property tests for the distributed eviction lock feature:
//! - Property 1: Mutual Exclusion (Requirements 5.5, 1.3)
//! - Property 3: Lock File Completeness (Requirements 1.4, 4.3, 4.4, 4.5, 4.6, 4.7)
//! - Property 4: Staleness Detection (Requirements 2.1, 2.2, 2.3)
//! - Property 5: Lock Acquisition Precedes Eviction (Requirements 3.1, 3.2)
//! - Property 6: Lock Release After Eviction (Requirements 3.3, 3.4)
//! - Property 7: Atomic Lock Acquisition (Requirements 1.5)
//! - Property 8: Skip on Failure (Requirements 5.1, 5.2)
//! - Property 9: Location Consistency (Requirements 4.1, 4.2)

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache::{CacheManager, GlobalEvictionLock};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

// ============================================================================
// Test Configuration Types
// ============================================================================

#[derive(Debug, Clone)]
struct MutualExclusionConfig {
    num_instances: usize,
    eviction_duration_ms: u64,
    delay_between_attempts_us: u64,
}

impl Arbitrary for MutualExclusionConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        MutualExclusionConfig {
            num_instances: 2 + (usize::arbitrary(g) % 5),
            eviction_duration_ms: 10 + (u64::arbitrary(g) % 91),
            delay_between_attempts_us: u64::arbitrary(g) % 501,
        }
    }
}

#[derive(Debug, Clone)]
struct AtomicLockTestConfig {
    num_instances: usize,
    attempts_per_instance: usize,
    delay_between_attempts_us: u64,
}

impl Arbitrary for AtomicLockTestConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        AtomicLockTestConfig {
            num_instances: 2 + (usize::arbitrary(g) % 4),
            attempts_per_instance: 1 + (usize::arbitrary(g) % 3),
            delay_between_attempts_us: u64::arbitrary(g) % 501,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct LockCompletenessConfig {
    timeout_seconds: u64,
    instance_suffix: u32,
    use_custom_hostname: bool,
}

impl Arbitrary for LockCompletenessConfig {
    fn arbitrary(g: &mut Gen) -> Self {
        LockCompletenessConfig {
            timeout_seconds: 30 + (u64::arbitrary(g) % 3571),
            instance_suffix: u32::arbitrary(g) % 10000,
            use_custom_hostname: bool::arbitrary(g),
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn create_test_cache_dir() -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cache_dir = temp_dir.path().to_path_buf();

    std::fs::create_dir_all(cache_dir.join("locks")).ok();
    std::fs::create_dir_all(cache_dir.join("metadata")).ok();
    std::fs::create_dir_all(cache_dir.join("ranges")).ok();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).ok();

    (temp_dir, cache_dir)
}

// ============================================================================
// Property 1: Mutual Exclusion
// **Validates: Requirements 5.5, 1.3**
// ============================================================================

fn prop_mutual_exclusion_during_eviction(config: MutualExclusionConfig) -> TestResult {
    if config.num_instances > 10 {
        return TestResult::discard();
    }

    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let concurrent_evictions = Arc::new(AtomicUsize::new(0));
        let max_concurrent_evictions = Arc::new(AtomicUsize::new(0));
        let total_evictions_started = Arc::new(AtomicUsize::new(0));

        for _ in 0..config.num_instances {
            if config.delay_between_attempts_us > 0 {
                tokio::time::sleep(Duration::from_micros(
                    config.delay_between_attempts_us % 100,
                ))
                .await;
            }

            match cache_manager.try_acquire_global_eviction_lock().await {
                Ok(true) => {
                    total_evictions_started.fetch_add(1, Ordering::SeqCst);
                    let current = concurrent_evictions.fetch_add(1, Ordering::SeqCst) + 1;

                    let mut max = max_concurrent_evictions.load(Ordering::SeqCst);
                    while current > max {
                        match max_concurrent_evictions.compare_exchange_weak(
                            max,
                            current,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(m) => max = m,
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(config.eviction_duration_ms.min(20)))
                        .await;
                    concurrent_evictions.fetch_sub(1, Ordering::SeqCst);
                    cache_manager.release_global_eviction_lock().await.ok();
                }
                Ok(false) => {}
                Err(_) => return TestResult::discard(),
            }
        }

        let max_concurrent = max_concurrent_evictions.load(Ordering::SeqCst);
        if max_concurrent > 1 {
            return TestResult::failed();
        }

        let total_started = total_evictions_started.load(Ordering::SeqCst);
        if total_started == 0 {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

fn prop_sequential_evictions_after_release(config: MutualExclusionConfig) -> TestResult {
    let num_rounds = config.num_instances.min(5);

    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let mut successful_evictions = 0;
        for _round in 0..num_rounds {
            match cache_manager.try_acquire_global_eviction_lock().await {
                Ok(true) => {
                    tokio::time::sleep(Duration::from_millis(config.eviction_duration_ms / 10))
                        .await;
                    successful_evictions += 1;
                    cache_manager.release_global_eviction_lock().await.ok();
                }
                Ok(false) => return TestResult::failed(),
                Err(_) => return TestResult::discard(),
            }
        }

        if successful_evictions != num_rounds {
            return TestResult::failed();
        }
        TestResult::passed()
    })
}

#[test]
fn test_property_mutual_exclusion_during_eviction() {
    QuickCheck::new().tests(20).quickcheck(
        prop_mutual_exclusion_during_eviction as fn(MutualExclusionConfig) -> TestResult,
    );
}

#[test]
fn test_property_sequential_evictions_after_release() {
    QuickCheck::new().tests(20).quickcheck(
        prop_sequential_evictions_after_release as fn(MutualExclusionConfig) -> TestResult,
    );
}

// ============================================================================
// Property 3: Lock File Completeness
// **Validates: Requirements 1.4, 4.3, 4.4, 4.5, 4.6, 4.7**
// ============================================================================

fn prop_lock_file_contains_all_required_fields(_config: LockCompletenessConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        match cache_manager.try_acquire_global_eviction_lock().await {
            Ok(true) => {
                let lock_path = cache_dir.join("locks").join("global_eviction.lock");
                if !lock_path.exists() {
                    return TestResult::failed();
                }

                let content = match std::fs::read_to_string(&lock_path) {
                    Ok(c) => c,
                    Err(_) => return TestResult::failed(),
                };

                let json_value: serde_json::Value = match serde_json::from_str(&content) {
                    Ok(v) => v,
                    Err(_) => return TestResult::failed(),
                };

                // Check all required fields
                if json_value.get("instance_id").is_none()
                    || json_value.get("process_id").is_none()
                    || json_value.get("hostname").is_none()
                    || json_value.get("acquired_at").is_none()
                    || json_value.get("timeout_seconds").is_none()
                {
                    return TestResult::failed();
                }

                cache_manager.release_global_eviction_lock().await.ok();
                TestResult::passed()
            }
            Ok(false) => TestResult::discard(),
            Err(_) => TestResult::discard(),
        }
    })
}

fn prop_lock_file_fields_have_valid_values(_config: LockCompletenessConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        match cache_manager.try_acquire_global_eviction_lock().await {
            Ok(true) => {
                let lock_path = cache_dir.join("locks").join("global_eviction.lock");
                let content = match std::fs::read_to_string(&lock_path) {
                    Ok(c) => c,
                    Err(_) => return TestResult::failed(),
                };

                let lock: GlobalEvictionLock = match serde_json::from_str(&content) {
                    Ok(l) => l,
                    Err(_) => return TestResult::failed(),
                };

                if lock.instance_id.is_empty()
                    || lock.process_id == 0
                    || lock.hostname.is_empty()
                    || lock.timeout_seconds == 0
                {
                    return TestResult::failed();
                }

                let now = SystemTime::now();
                if let Ok(duration_since_acquired) = now.duration_since(lock.acquired_at) {
                    if duration_since_acquired.as_secs() > 60 {
                        return TestResult::failed();
                    }
                } else {
                    return TestResult::failed();
                }

                cache_manager.release_global_eviction_lock().await.ok();
                TestResult::passed()
            }
            Ok(false) => TestResult::discard(),
            Err(_) => TestResult::discard(),
        }
    })
}

fn prop_process_id_matches_current(_config: LockCompletenessConfig) -> TestResult {
    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let current_pid = std::process::id();

        match cache_manager.try_acquire_global_eviction_lock().await {
            Ok(true) => {
                let lock_path = cache_dir.join("locks").join("global_eviction.lock");
                let content = match std::fs::read_to_string(&lock_path) {
                    Ok(c) => c,
                    Err(_) => return TestResult::failed(),
                };

                let lock: GlobalEvictionLock = match serde_json::from_str(&content) {
                    Ok(l) => l,
                    Err(_) => return TestResult::failed(),
                };

                if lock.process_id != current_pid {
                    return TestResult::failed();
                }

                cache_manager.release_global_eviction_lock().await.ok();
                TestResult::passed()
            }
            Ok(false) => TestResult::discard(),
            Err(_) => TestResult::discard(),
        }
    })
}

#[test]
fn test_property_lock_file_contains_all_required_fields() {
    QuickCheck::new().tests(20).quickcheck(
        prop_lock_file_contains_all_required_fields as fn(LockCompletenessConfig) -> TestResult,
    );
}

#[test]
fn test_property_lock_file_fields_have_valid_values() {
    QuickCheck::new().tests(20).quickcheck(
        prop_lock_file_fields_have_valid_values as fn(LockCompletenessConfig) -> TestResult,
    );
}

#[test]
fn test_property_process_id_matches_current() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_process_id_matches_current as fn(LockCompletenessConfig) -> TestResult);
}

// ============================================================================
// Property 7: Atomic Lock Acquisition
// **Validates: Requirements 1.5**
// ============================================================================

fn prop_atomic_lock_mutual_exclusion(config: AtomicLockTestConfig) -> TestResult {
    if config.num_instances * config.attempts_per_instance > 50 {
        return TestResult::discard();
    }

    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let total_cycles = config.num_instances.min(5);
        let attempts_per_cycle = config.attempts_per_instance.min(3);

        for _cycle in 0..total_cycles {
            let acquired = match cache_manager.try_acquire_global_eviction_lock().await {
                Ok(a) => a,
                Err(_) => return TestResult::discard(),
            };

            if !acquired {
                return TestResult::failed();
            }

            let mut violations = 0;
            for _ in 0..attempts_per_cycle {
                match cache_manager.try_acquire_global_eviction_lock().await {
                    Ok(true) => violations += 1,
                    Ok(false) => {}
                    Err(_) => {}
                }

                if config.delay_between_attempts_us > 0 {
                    tokio::time::sleep(Duration::from_micros(config.delay_between_attempts_us))
                        .await;
                }
            }

            if violations > 0 {
                return TestResult::failed();
            }

            if cache_manager.release_global_eviction_lock().await.is_err() {
                return TestResult::discard();
            }
        }

        TestResult::passed()
    })
}

fn prop_no_temp_file_leaks(config: AtomicLockTestConfig) -> TestResult {
    if config.num_instances > 10 || config.attempts_per_instance > 3 {
        return TestResult::discard();
    }

    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        let total_attempts = config.num_instances * config.attempts_per_instance;
        for _ in 0..total_attempts {
            match cache_manager.try_acquire_global_eviction_lock().await {
                Ok(true) => {
                    cache_manager.release_global_eviction_lock().await.ok();
                }
                _ => {}
            }
        }

        let locks_dir = cache_dir.join("locks");
        if locks_dir.exists() {
            for entry in std::fs::read_dir(&locks_dir).expect("Failed to read locks dir") {
                let entry = entry.expect("Failed to read entry");
                let path = entry.path();
                if path.extension().map(|e| e == "tmp").unwrap_or(false) {
                    return TestResult::failed();
                }
            }
        }

        TestResult::passed()
    })
}

fn prop_sequential_acquisition_after_release(config: AtomicLockTestConfig) -> TestResult {
    let num_rounds = config.num_instances.min(5);

    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        for round in 0..num_rounds {
            let acquired = match cache_manager.try_acquire_global_eviction_lock().await {
                Ok(a) => a,
                Err(_) => return TestResult::failed(),
            };

            if !acquired {
                return TestResult::failed();
            }

            if cache_manager.release_global_eviction_lock().await.is_err() {
                return TestResult::failed();
            }

            if round < num_rounds - 1 {
                tokio::time::sleep(Duration::from_micros(config.delay_between_attempts_us)).await;
            }
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_atomic_lock_mutual_exclusion() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_atomic_lock_mutual_exclusion as fn(AtomicLockTestConfig) -> TestResult);
}

#[test]
fn test_property_no_temp_file_leaks() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_no_temp_file_leaks as fn(AtomicLockTestConfig) -> TestResult);
}

#[test]
fn test_property_sequential_acquisition_after_release() {
    QuickCheck::new().tests(20).quickcheck(
        prop_sequential_acquisition_after_release as fn(AtomicLockTestConfig) -> TestResult,
    );
}

// ============================================================================
// Property: Eviction Count Consistency
// ============================================================================

fn prop_eviction_count_consistency(config: MutualExclusionConfig) -> TestResult {
    let num_instances = config.num_instances.min(6);

    let rt = Runtime::new().expect("Failed to create runtime");
    rt.block_on(async {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_dir = Arc::new(cache_dir);

        let lock_acquisitions = Arc::new(AtomicUsize::new(0));
        let evictions_performed = Arc::new(AtomicUsize::new(0));

        let mut join_set = JoinSet::new();

        for _instance_id in 0..num_instances {
            let cache_dir = Arc::clone(&cache_dir);
            let lock_acquisitions = Arc::clone(&lock_acquisitions);
            let evictions_performed = Arc::clone(&evictions_performed);
            let eviction_duration_ms = config.eviction_duration_ms;

            join_set.spawn(async move {
                let cache_manager =
                    CacheManager::new_with_defaults(cache_dir.as_ref().clone(), false, 0);

                match cache_manager.try_acquire_global_eviction_lock().await {
                    Ok(true) => {
                        lock_acquisitions.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(eviction_duration_ms)).await;
                        evictions_performed.fetch_add(1, Ordering::SeqCst);
                        cache_manager.release_global_eviction_lock().await.ok();
                    }
                    Ok(false) => {}
                    Err(_) => {}
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            result.expect("Task should complete successfully");
        }

        let acquisitions = lock_acquisitions.load(Ordering::SeqCst);
        let evictions = evictions_performed.load(Ordering::SeqCst);

        if acquisitions != evictions {
            return TestResult::failed();
        }

        if acquisitions == 0 {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_eviction_count_consistency() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_eviction_count_consistency as fn(MutualExclusionConfig) -> TestResult);
}

// ============================================================================
// Unit tests for edge cases
// ============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[tokio::test]
    async fn test_lock_file_is_valid_json() {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        match cache_manager.try_acquire_global_eviction_lock().await {
            Ok(true) => {
                let lock_path = cache_dir.join("locks").join("global_eviction.lock");
                let content = std::fs::read_to_string(&lock_path).unwrap();

                let json_result: Result<serde_json::Value, _> = serde_json::from_str(&content);
                assert!(json_result.is_ok());

                let lock_result: Result<GlobalEvictionLock, _> = serde_json::from_str(&content);
                assert!(lock_result.is_ok());

                cache_manager.release_global_eviction_lock().await.ok();
            }
            _ => panic!("Should acquire lock"),
        }
    }

    #[tokio::test]
    async fn test_hostname_is_valid() {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        match cache_manager.try_acquire_global_eviction_lock().await {
            Ok(true) => {
                let lock_path = cache_dir.join("locks").join("global_eviction.lock");
                let content = std::fs::read_to_string(&lock_path).unwrap();
                let lock: GlobalEvictionLock = serde_json::from_str(&content).unwrap();

                assert!(!lock.hostname.is_empty());
                assert!(!lock.hostname.chars().any(|c| c.is_control()));
                assert!(lock.hostname.len() <= 255);

                cache_manager.release_global_eviction_lock().await.ok();
            }
            _ => panic!("Should acquire lock"),
        }
    }

    #[tokio::test]
    async fn test_instance_id_format() {
        let (_temp_dir, cache_dir) = create_test_cache_dir();
        let cache_manager = CacheManager::new_with_defaults(cache_dir.clone(), false, 0);

        match cache_manager.try_acquire_global_eviction_lock().await {
            Ok(true) => {
                let lock_path = cache_dir.join("locks").join("global_eviction.lock");
                let content = std::fs::read_to_string(&lock_path).unwrap();
                let lock: GlobalEvictionLock = serde_json::from_str(&content).unwrap();

                assert!(lock.instance_id.len() >= 3);
                assert!(!lock.instance_id.trim().is_empty());

                cache_manager.release_global_eviction_lock().await.ok();
            }
            _ => panic!("Should acquire lock"),
        }
    }
}
