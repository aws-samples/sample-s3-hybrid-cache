//! Write-cache GET TTL revalidation bugfix test.
//!
//! Validates that the first GET of a write-through-cached object correctly
//! respects `get_ttl=0` by triggering revalidation against S3, rather than
//! serving stale from cache using the `put_ttl`-based `expires_at`.
//!
//! Also validates the preservation property: non-zero `get_ttl` objects remain
//! fresh after the write-to-read TTL transition (no unnecessary revalidation).

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::ObjectExpirationResult;
use s3_proxy::config::SharedStorageConfig;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

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

async fn store_write_cached_object(cache_manager: &CacheManager, cache_key: &str) {
    let test_data = b"Hello, World! Test data for write-cache revalidation.";
    let mut response_headers = HashMap::new();
    response_headers.insert("content-type".to_string(), "text/plain".to_string());

    cache_manager
        .store_put_as_write_cached_range(
            cache_key,
            test_data,
            "\"abc123def456\"".to_string(),
            "Mon, 01 Jan 2024 12:00:00 GMT".to_string(),
            Some("text/plain".to_string()),
            response_headers,
        )
        .await
        .expect("store_put_as_write_cached_range should succeed");
}

// =============================================================================
// Property 1: Bug Condition — get_ttl=0 triggers revalidation on first GET
// =============================================================================

/// After the synchronous `refresh_write_cache_ttl` (the fix), with `get_ttl=0`,
/// `check_object_expiration` returns `Expired` — triggering S3 revalidation.
#[tokio::test]
async fn get_ttl_zero_first_get_triggers_revalidation() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        create_cache_manager(temp_dir.path(), Duration::ZERO, Duration::from_secs(3600));
    let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

    let cache_key = "/test-bucket/revalidation-test.txt";
    store_write_cached_object(&cache_manager, cache_key).await;

    // Precondition: write-cached, not yet expired by put_ttl
    let meta = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .unwrap()
        .unwrap();
    assert!(meta.object_metadata.is_write_cached);
    assert!(!meta.is_object_expired());

    // THE FIX: synchronous transition before freshness check
    let transitioned = cache_manager
        .refresh_write_cache_ttl(cache_key)
        .await
        .unwrap();
    assert!(transitioned);

    // Now freshness check sees get_ttl=0 → expired → revalidation
    let result = disk_cache_manager
        .check_object_expiration(cache_key, Duration::ZERO)
        .await
        .unwrap();
    match result {
        ObjectExpirationResult::Expired {
            last_modified,
            etag,
        } => {
            assert_eq!(
                last_modified,
                Some("Mon, 01 Jan 2024 12:00:00 GMT".to_string())
            );
            assert_eq!(etag, Some("\"abc123def456\"".to_string()));
        }
        ObjectExpirationResult::Fresh => {
            panic!("BUG: check_object_expiration returned Fresh with get_ttl=0 — the fix is not working");
        }
    }
}

/// Second call to refresh_write_cache_ttl is a no-op (already transitioned).
#[tokio::test]
async fn second_transition_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        create_cache_manager(temp_dir.path(), Duration::ZERO, Duration::from_secs(3600));

    let cache_key = "/test-bucket/noop-test.txt";
    store_write_cached_object(&cache_manager, cache_key).await;

    assert!(cache_manager
        .refresh_write_cache_ttl(cache_key)
        .await
        .unwrap());
    assert!(!cache_manager
        .refresh_write_cache_ttl(cache_key)
        .await
        .unwrap());
}

// =============================================================================
// Property 2: Preservation — non-zero get_ttl objects remain fresh
// =============================================================================

/// With `get_ttl=300s`, after the synchronous transition, the object is still
/// fresh (no unnecessary revalidation for non-zero TTL).
#[tokio::test]
async fn nonzero_get_ttl_remains_fresh_after_transition() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_cache_manager(
        temp_dir.path(),
        Duration::from_secs(300),
        Duration::from_secs(3600),
    );
    let disk_cache_manager = cache_manager.create_configured_disk_cache_manager();

    let cache_key = "/test-bucket/preservation-test.txt";
    store_write_cached_object(&cache_manager, cache_key).await;

    let transitioned = cache_manager
        .refresh_write_cache_ttl(cache_key)
        .await
        .unwrap();
    assert!(transitioned);

    // get_ttl=300s → expires_at = now+300s → Fresh
    let result = disk_cache_manager
        .check_object_expiration(cache_key, Duration::from_secs(300))
        .await
        .unwrap();
    assert!(
        matches!(result, ObjectExpirationResult::Fresh),
        "Non-zero get_ttl object should remain fresh after transition"
    );
}

/// Read-cached objects (is_write_cached=false) are unaffected — transition is no-op.
#[tokio::test]
async fn read_cached_object_unaffected() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        create_cache_manager(temp_dir.path(), Duration::ZERO, Duration::from_secs(3600));

    let cache_key = "/test-bucket/read-cached.txt";
    store_write_cached_object(&cache_manager, cache_key).await;

    // Transition once (now read-cached)
    cache_manager
        .refresh_write_cache_ttl(cache_key)
        .await
        .unwrap();

    // Second call is no-op
    let result = cache_manager
        .refresh_write_cache_ttl(cache_key)
        .await
        .unwrap();
    assert!(!result, "Already-transitioned object should be a no-op");
}
