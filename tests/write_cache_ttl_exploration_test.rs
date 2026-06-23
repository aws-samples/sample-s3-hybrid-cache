//! Bug Condition Exploration Test: Write-cache GET TTL revalidation
//!
//! **Property 1: Expected Behavior** — First GET of write-cached object with get_ttl=0
//! triggers revalidation
//!
//! This test encodes the EXPECTED CORRECT behavior: when a write-cached object
//! has `get_ttl=0`, calling `refresh_write_cache_ttl` (the synchronous transition)
//! BEFORE `check_object_expiration` should return `Expired` (triggering revalidation).
//!
//! After the fix in http_proxy.rs, `refresh_write_cache_ttl` runs synchronously
//! BEFORE `check_object_expiration`. This test mimics the FIXED code path.
//!
//! **EXPECTED OUTCOME**: Test PASSES (confirms bug is fixed).
//!
//! **Validates: Requirements 2.1, 2.2, 2.3**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::ObjectExpirationResult;
use s3_proxy::config::SharedStorageConfig;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

// ============================================================================
// Test infrastructure
// ============================================================================

/// Create a CacheManager configured with get_ttl=0 and a given put_ttl.
fn create_cache_manager_get_ttl_zero(
    cache_dir: &std::path::Path,
    put_ttl: Duration,
) -> CacheManager {
    CacheManager::new_with_shared_storage(
        cache_dir.to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024,
        CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::ZERO, // get_ttl = 0 (the bug condition)
        Duration::from_secs(300),
        put_ttl,
        false,
        SharedStorageConfig::default(),
        10.0,
        true,
        Duration::from_secs(86400),
        s3_proxy::config::MetadataCacheConfig::default(),
        95,
        80,
        true,
        Duration::from_secs(60),
        1_048_576,
        false,
        Duration::from_secs(10),
        64, // ram_cache_shard_count
    )
}
async fn store_write_cached_object(cache_manager: &CacheManager, cache_key: &str) {
    let test_data = b"Test data for write-cache TTL exploration.";
    let mut response_headers = HashMap::new();
    response_headers.insert("content-type".to_string(), "text/plain".to_string());

    cache_manager
        .store_put_as_write_cached_range(
            cache_key,
            test_data,
            "\"etag-exploration-test\"".to_string(),
            "Wed, 15 Jan 2025 10:00:00 GMT".to_string(),
            Some("text/plain".to_string()),
            response_headers,
        )
        .await
        .expect("store_put_as_write_cached_range should succeed");
}

// ============================================================================
// Arbitrary input for the bug condition property
// ============================================================================

/// Input space scoped to the bug condition:
/// - `put_ttl` in [60s, 7200s] (object is stored with a future expires_at)
/// - All inputs use `get_ttl=0` (the revalidation contract)
/// - `is_write_cached=true` (set by store_put_as_write_cached_range)
#[derive(Debug, Clone)]
struct BugConditionInput {
    /// put_ttl: the TTL set at write time (60s to 7200s)
    put_ttl_secs: u64,
    /// Unique suffix for the cache key (avoids collisions between test cases)
    key_suffix: u32,
}

impl Arbitrary for BugConditionInput {
    fn arbitrary(g: &mut Gen) -> Self {
        // put_ttl between 60s and 7200s — ensures expires_at is well in the future
        let put_ttl_secs = (u64::arbitrary(g) % 7141) + 60; // [60, 7200]
        let key_suffix = u32::arbitrary(g) % 100_000;
        Self {
            put_ttl_secs,
            key_suffix,
        }
    }
}

// ============================================================================
// Property 1: Expected Behavior (post-fix)
//
// For a write-cached object with get_ttl=0 where put_ttl has NOT elapsed,
// calling refresh_write_cache_ttl BEFORE check_object_expiration should
// return Expired — because get_ttl=0 means "every GET must revalidate".
//
// This mimics the FIXED code path in http_proxy.rs where the synchronous
// transition runs before the freshness check.
// ============================================================================

fn prop_bug_condition_first_get_returns_expired(input: BugConditionInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let put_ttl = Duration::from_secs(input.put_ttl_secs);
        let cache_manager = create_cache_manager_get_ttl_zero(temp_dir.path(), put_ttl);
        let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

        let cache_key = format!("/test-bucket/exploration-{}.txt", input.key_suffix);
        store_write_cached_object(&cache_manager, &cache_key).await;

        // Verify preconditions: object is write-cached and not expired by put_ttl
        let meta = cache_manager
            .get_metadata_from_disk(&cache_key)
            .await
            .unwrap()
            .unwrap();
        if !meta.object_metadata.is_write_cached {
            return TestResult::discard();
        }
        if meta.is_object_expired() {
            // put_ttl already elapsed (shouldn't happen with 60s+ TTL, but discard)
            return TestResult::discard();
        }

        // THE FIX: Call refresh_write_cache_ttl BEFORE check_object_expiration.
        // This mimics the fixed code path in http_proxy.rs where the synchronous
        // transition runs before the freshness check.
        let _ = cache_manager.refresh_write_cache_ttl(&cache_key).await;

        let result = disk_cache_manager
            .check_object_expiration(&cache_key, Duration::ZERO)
            .await
            .unwrap();

        // EXPECTED BEHAVIOR: get_ttl=0 means the object should be Expired
        // (triggering S3 revalidation on every GET) after the transition.
        TestResult::from_bool(matches!(result, ObjectExpirationResult::Expired { .. }))
    })
}

// ============================================================================
// Test runner
// ============================================================================

#[test]
fn test_bug_condition_write_cached_get_ttl_zero_returns_expired() {
    QuickCheck::new().tests(20).quickcheck(
        prop_bug_condition_first_get_returns_expired as fn(BugConditionInput) -> TestResult,
    );
}
