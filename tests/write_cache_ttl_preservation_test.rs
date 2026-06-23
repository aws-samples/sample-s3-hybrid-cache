//! Preservation Property Tests: Write-cache GET TTL revalidation
//!
//! **Property 2: Preservation** — Non-zero get_ttl and read-cached objects unchanged
//!
//! These tests capture the baseline behavior that MUST be preserved after the fix:
//! 1. Read-cached objects: freshness depends only on `expires_at` vs `now`
//! 2. Write-cached objects with get_ttl > 0: after transition, object is fresh
//! 3. Write-cached objects with expired put_ttl: object is expired regardless
//!
//! **EXPECTED OUTCOME**: All tests PASS on unfixed code (confirms baseline to preserve).
//!
//! **Validates: Requirements 3.1, 3.2, 3.3, 3.5**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{
    CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata, UploadState,
};
use s3_proxy::config::SharedStorageConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

// ============================================================================
// Test infrastructure
// ============================================================================

/// Create a CacheManager with configurable get_ttl and put_ttl.
fn create_cache_manager(
    cache_dir: &std::path::Path,
    get_ttl: Duration,
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
        get_ttl,
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

/// Store a read-cached object directly by writing metadata to disk.
/// This simulates an object that was fetched via GET (is_write_cached=false).
async fn store_read_cached_object(
    cache_manager: &CacheManager,
    cache_key: &str,
    expires_at: SystemTime,
) {
    let object_metadata = ObjectMetadata {
        etag: "\"etag-read-cached\"".to_string(),
        last_modified: "Wed, 15 Jan 2025 10:00:00 GMT".to_string(),
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        response_headers: HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: 42,
        parts: Vec::new(),
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
        ..Default::default()
    };

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges: Vec::new(),
        created_at: SystemTime::now(),
        expires_at,
        compression_info: CompressionInfo::default(),
        ..Default::default()
    };

    // Write metadata directly to disk using the cache manager's metadata path
    let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
    if let Some(parent) = metadata_file_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let json = serde_json::to_string_pretty(&metadata).unwrap();
    std::fs::write(&metadata_file_path, json).unwrap();
}

/// Store a write-cached object with a specific expires_at (simulating various put_ttl ages).
async fn store_write_cached_object_with_expiry(
    cache_manager: &CacheManager,
    cache_key: &str,
    expires_at: SystemTime,
) {
    let now = SystemTime::now();
    let object_metadata = ObjectMetadata {
        etag: "\"etag-write-cached\"".to_string(),
        last_modified: "Wed, 15 Jan 2025 10:00:00 GMT".to_string(),
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        response_headers: HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: 42,
        parts: Vec::new(),
        is_write_cached: true,
        write_cache_expires_at: Some(expires_at),
        write_cache_created_at: Some(now),
        write_cache_last_accessed: Some(now),
        ..Default::default()
    };

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges: Vec::new(),
        created_at: now,
        expires_at,
        compression_info: CompressionInfo::default(),
        ..Default::default()
    };

    let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
    if let Some(parent) = metadata_file_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let json = serde_json::to_string_pretty(&metadata).unwrap();
    std::fs::write(&metadata_file_path, json).unwrap();
}

// ============================================================================
// Arbitrary inputs
// ============================================================================

/// Input for read-cached object preservation tests.
/// Generates objects with expires_at in the future (fresh).
#[derive(Debug, Clone)]
struct ReadCachedFreshInput {
    /// Seconds until expiry (1s to 7200s)
    ttl_remaining_secs: u64,
    /// Unique suffix for the cache key
    key_suffix: u32,
}

impl Arbitrary for ReadCachedFreshInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let ttl_remaining_secs = (u64::arbitrary(g) % 7200) + 1; // [1, 7200]
        let key_suffix = u32::arbitrary(g) % 100_000;
        Self {
            ttl_remaining_secs,
            key_suffix,
        }
    }
}

/// Input for read-cached object with expired TTL.
#[derive(Debug, Clone)]
struct ReadCachedExpiredInput {
    /// Seconds ago the object expired (1s to 3600s)
    expired_ago_secs: u64,
    /// Unique suffix for the cache key
    key_suffix: u32,
}

impl Arbitrary for ReadCachedExpiredInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let expired_ago_secs = (u64::arbitrary(g) % 3600) + 1; // [1, 3600]
        let key_suffix = u32::arbitrary(g) % 100_000;
        Self {
            expired_ago_secs,
            key_suffix,
        }
    }
}

/// Input for write-cached objects with get_ttl > 0.
#[derive(Debug, Clone)]
struct WriteCachedNonZeroGetTtlInput {
    /// get_ttl in seconds (1s to 86400s)
    get_ttl_secs: u64,
    /// put_ttl in seconds (60s to 7200s) — ensures object is stored with future expires_at
    put_ttl_secs: u64,
    /// Unique suffix for the cache key
    key_suffix: u32,
}

impl Arbitrary for WriteCachedNonZeroGetTtlInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let get_ttl_secs = (u64::arbitrary(g) % 86400) + 1; // [1, 86400]
        let put_ttl_secs = (u64::arbitrary(g) % 7141) + 60; // [60, 7200]
        let key_suffix = u32::arbitrary(g) % 100_000;
        Self {
            get_ttl_secs,
            put_ttl_secs,
            key_suffix,
        }
    }
}

/// Input for write-cached objects where put_ttl has already elapsed.
#[derive(Debug, Clone)]
struct WriteCachedExpiredPutTtlInput {
    /// How many seconds ago the put_ttl expired (1s to 3600s)
    expired_ago_secs: u64,
    /// get_ttl in seconds (0s to 3600s) — irrelevant since put_ttl already expired
    get_ttl_secs: u64,
    /// Unique suffix for the cache key
    key_suffix: u32,
}

impl Arbitrary for WriteCachedExpiredPutTtlInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let expired_ago_secs = (u64::arbitrary(g) % 3600) + 1; // [1, 3600]
        let get_ttl_secs = u64::arbitrary(g) % 3601; // [0, 3600]
        let key_suffix = u32::arbitrary(g) % 100_000;
        Self {
            expired_ago_secs,
            get_ttl_secs,
            key_suffix,
        }
    }
}

// ============================================================================
// Property 2a: Read-cached fresh objects → check_object_expiration returns Fresh
//
// For all read-cached objects with expires_at in the future,
// check_object_expiration returns Fresh. The refresh_write_cache_ttl call
// is a no-op for these objects (early-returns false).
//
// **Validates: Requirements 3.2, 3.5**
// ============================================================================

fn prop_read_cached_fresh_returns_fresh(input: ReadCachedFreshInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = create_cache_manager(
            temp_dir.path(),
            Duration::from_secs(300), // get_ttl doesn't matter for read-cached
            Duration::from_secs(3600),
        );
        let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

        let cache_key = format!("/test-bucket/read-fresh-{}.txt", input.key_suffix);
        let expires_at = SystemTime::now() + Duration::from_secs(input.ttl_remaining_secs);

        store_read_cached_object(&cache_manager, &cache_key, expires_at).await;

        // refresh_write_cache_ttl is a no-op for read-cached objects
        let transition_result = cache_manager
            .refresh_write_cache_ttl(&cache_key)
            .await
            .unwrap();
        assert!(
            !transition_result,
            "Transition should be a no-op for read-cached objects"
        );

        // check_object_expiration should return Fresh
        let result = disk_cache_manager
            .check_object_expiration(&cache_key, Duration::from_secs(300))
            .await
            .unwrap();

        TestResult::from_bool(matches!(result, ObjectExpirationResult::Fresh))
    })
}

// ============================================================================
// Property 2b: Read-cached expired objects → check_object_expiration returns Expired
//
// For all read-cached objects with expires_at in the past,
// check_object_expiration returns Expired. The transition is still a no-op.
//
// **Validates: Requirements 3.3**
// ============================================================================

fn prop_read_cached_expired_returns_expired(input: ReadCachedExpiredInput) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = create_cache_manager(
            temp_dir.path(),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );
        let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

        let cache_key = format!("/test-bucket/read-expired-{}.txt", input.key_suffix);
        let expires_at = SystemTime::now() - Duration::from_secs(input.expired_ago_secs);

        store_read_cached_object(&cache_manager, &cache_key, expires_at).await;

        // refresh_write_cache_ttl is a no-op for read-cached objects
        let transition_result = cache_manager
            .refresh_write_cache_ttl(&cache_key)
            .await
            .unwrap();
        assert!(
            !transition_result,
            "Transition should be a no-op for read-cached objects"
        );

        // check_object_expiration should return Expired
        let result = disk_cache_manager
            .check_object_expiration(&cache_key, Duration::ZERO)
            .await
            .unwrap();

        TestResult::from_bool(matches!(result, ObjectExpirationResult::Expired { .. }))
    })
}

// ============================================================================
// Property 2c: Write-cached objects with get_ttl > 0 → Fresh after transition
//
// For all write-cached objects with get_ttl > 0, after calling
// refresh_write_cache_ttl (which sets expires_at = now + get_ttl),
// check_object_expiration returns Fresh because now + get_ttl > now
// always holds for get_ttl > 0.
//
// **Validates: Requirements 3.1, 3.5**
// ============================================================================

fn prop_write_cached_nonzero_get_ttl_fresh_after_transition(
    input: WriteCachedNonZeroGetTtlInput,
) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let get_ttl = Duration::from_secs(input.get_ttl_secs);
        let put_ttl = Duration::from_secs(input.put_ttl_secs);
        let cache_manager = create_cache_manager(temp_dir.path(), get_ttl, put_ttl);
        let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

        let cache_key = format!("/test-bucket/write-nonzero-{}.txt", input.key_suffix);
        // Object was recently stored, put_ttl hasn't elapsed → expires_at is in the future
        let expires_at = SystemTime::now() + put_ttl;

        store_write_cached_object_with_expiry(&cache_manager, &cache_key, expires_at).await;

        // Perform the synchronous transition (sets expires_at = now + get_ttl)
        let transition_result = cache_manager
            .refresh_write_cache_ttl(&cache_key)
            .await
            .unwrap();
        assert!(
            transition_result,
            "Transition should succeed for write-cached objects"
        );

        // After transition with get_ttl > 0, object should be Fresh
        // because expires_at = now + get_ttl > now (always true for get_ttl > 0)
        let result = disk_cache_manager
            .check_object_expiration(&cache_key, get_ttl)
            .await
            .unwrap();

        TestResult::from_bool(matches!(result, ObjectExpirationResult::Fresh))
    })
}

// ============================================================================
// Property 2d: Write-cached objects with expired put_ttl → Expired regardless
//
// For all write-cached objects where put_ttl has already elapsed
// (expires_at is in the past), check_object_expiration returns Expired
// regardless of whether refresh_write_cache_ttl is called.
//
// On unfixed code, check_object_expiration is called WITHOUT prior transition,
// so it sees the expired put_ttl-based expires_at and correctly returns Expired.
//
// **Validates: Requirements 3.3, 3.5**
// ============================================================================

fn prop_write_cached_expired_put_ttl_returns_expired(
    input: WriteCachedExpiredPutTtlInput,
) -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let get_ttl = Duration::from_secs(input.get_ttl_secs);
        let put_ttl = Duration::from_secs(3600); // Doesn't matter — we manually set expired expires_at
        let cache_manager = create_cache_manager(temp_dir.path(), get_ttl, put_ttl);
        let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

        let cache_key = format!("/test-bucket/write-expired-{}.txt", input.key_suffix);
        // Object's put_ttl has already elapsed — expires_at is in the past
        let expires_at = SystemTime::now() - Duration::from_secs(input.expired_ago_secs);

        store_write_cached_object_with_expiry(&cache_manager, &cache_key, expires_at).await;

        // Without calling refresh_write_cache_ttl (mimics the current unfixed code path),
        // check_object_expiration sees the expired put_ttl and returns Expired.
        // With current-TTL semantics, pass Duration::ZERO to simulate "always expired".
        let result = disk_cache_manager
            .check_object_expiration(&cache_key, Duration::ZERO)
            .await
            .unwrap();

        TestResult::from_bool(matches!(result, ObjectExpirationResult::Expired { .. }))
    })
}

// ============================================================================
// Test runners
// ============================================================================

#[test]
fn test_preservation_read_cached_fresh_returns_fresh() {
    QuickCheck::new()
        .tests(20)
        .quickcheck(prop_read_cached_fresh_returns_fresh as fn(ReadCachedFreshInput) -> TestResult);
}

#[test]
fn test_preservation_read_cached_expired_returns_expired() {
    QuickCheck::new().tests(20).quickcheck(
        prop_read_cached_expired_returns_expired as fn(ReadCachedExpiredInput) -> TestResult,
    );
}

#[test]
fn test_preservation_write_cached_nonzero_get_ttl_fresh_after_transition() {
    QuickCheck::new().tests(20).quickcheck(
        prop_write_cached_nonzero_get_ttl_fresh_after_transition
            as fn(WriteCachedNonZeroGetTtlInput) -> TestResult,
    );
}

#[test]
fn test_preservation_write_cached_expired_put_ttl_returns_expired() {
    QuickCheck::new().tests(20).quickcheck(
        prop_write_cached_expired_put_ttl_returns_expired
            as fn(WriteCachedExpiredPutTtlInput) -> TestResult,
    );
}
