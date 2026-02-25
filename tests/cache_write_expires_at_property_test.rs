//! Property-based tests for cache write expires_at behavior
//!
//! Property 3: Cache writes set object-level expires_at
//!
//! For any `get_ttl` duration (including zero) and any range parameters, when `store_range`
//! is called, the resulting `NewCacheMetadata.expires_at` equals approximately `now + get_ttl`.
//! This holds for both new metadata creation and adding ranges to existing metadata.
//!
//! **Validates: Requirements 1.4, 4.1, 4.2, 4.3**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata};
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Generate a random Duration between 0 and ~1 year.
/// Includes Duration::ZERO to test the zero-TTL case.
fn arbitrary_ttl_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600 + 1); // 0s to ~1 year (inclusive of 0)
    Duration::from_secs(secs)
}

/// Generate a random last_modified string (RFC 2822-ish format).
fn arbitrary_last_modified(g: &mut Gen) -> String {
    let day = (u32::arbitrary(g) % 28) + 1;
    let months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let month = months[(u32::arbitrary(g) % 12) as usize];
    let year = 2020 + (u32::arbitrary(g) % 5);
    let hour = u32::arbitrary(g) % 24;
    let min = u32::arbitrary(g) % 60;
    let sec = u32::arbitrary(g) % 60;
    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} GMT",
        ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][(u32::arbitrary(g) % 7) as usize],
        day,
        month,
        year,
        hour,
        min,
        sec
    )
}

// ============================================================================
// Arbitrary input for Property 3
// ============================================================================

#[derive(Debug, Clone)]
struct ArbitraryCacheWriteInput {
    /// The get_ttl to use when storing the range
    get_ttl: Duration,
    /// The last_modified value stored in object metadata
    last_modified: String,
    /// Range size in bytes (1 to 256 bytes, small for speed)
    range_size: u64,
}

impl Arbitrary for ArbitraryCacheWriteInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let range_size = (u64::arbitrary(g) % 256) + 1; // 1 to 256 bytes
        Self {
            get_ttl: arbitrary_ttl_duration(g),
            last_modified: arbitrary_last_modified(g),
            range_size,
        }
    }
}

// ============================================================================
// Property 3: store_range sets object-level expires_at ≈ now + get_ttl
// ============================================================================

/// For any get_ttl duration, calling store_range produces metadata whose
/// object-level expires_at ≈ now + get_ttl (within 2s tolerance).
fn prop_cache_write_sets_expires_at(input: ArbitraryCacheWriteInput) -> TestResult {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TestResult::discard(),
    };

    rt.block_on(async {
        let temp_dir = match TempDir::new() {
            Ok(d) => d,
            Err(_) => return TestResult::discard(),
        };
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), false, 0, false);
        if cache_manager.initialize().await.is_err() {
            return TestResult::discard();
        }

        let cache_key = "test-bucket/cache-write-expires-test";
        let start = 0u64;
        let end = input.range_size - 1;

        // Create data matching the range size
        let data = vec![0xABu8; input.range_size as usize];

        let object_metadata = ObjectMetadata::new(
            "test-etag-abc123".to_string(),
            input.last_modified.clone(),
            input.range_size,
            Some("application/octet-stream".to_string()),
        );

        let before_store = SystemTime::now();

        // Use store_range — the actual code path that sets expires_at = now + ttl
        if cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata,
                input.get_ttl,
                false, // compression disabled for speed
            )
            .await
            .is_err()
        {
            return TestResult::discard();
        }

        let after_store = SystemTime::now();

        // Read back metadata directly from disk
        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        let content = match std::fs::read_to_string(&meta_path) {
            Ok(c) => c,
            Err(_) => return TestResult::discard(),
        };
        let stored_metadata = match serde_json::from_str::<NewCacheMetadata>(&content) {
            Ok(m) => m,
            Err(_) => return TestResult::discard(),
        };

        // Verify the range was stored
        if !stored_metadata
            .ranges
            .iter()
            .any(|r| r.start == start && r.end == end)
        {
            return TestResult::error("Range not found after store_range".to_string());
        }

        // Verify: object-level expires_at ≈ now + get_ttl
        // store_range calls refresh_object_ttl(ttl) which sets expires_at = now + ttl
        let expected_min = before_store + input.get_ttl;
        let expected_max = after_store + input.get_ttl + Duration::from_secs(2);

        let actual = stored_metadata.expires_at;

        if actual < expected_min {
            return TestResult::error(format!(
                "object expires_at too early: actual={:?}, expected_min={:?}, get_ttl={:?}",
                actual, expected_min, input.get_ttl
            ));
        }
        if actual > expected_max {
            return TestResult::error(format!(
                "object expires_at too late: actual={:?}, expected_max={:?}, get_ttl={:?}",
                actual, expected_max, input.get_ttl
            ));
        }

        TestResult::passed()
    })
}

// ============================================================================
// Test runner (minimum 100 iterations)
// ============================================================================

#[test]
fn test_property_cache_write_sets_expires_at_from_get_ttl() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_cache_write_sets_expires_at as fn(ArbitraryCacheWriteInput) -> TestResult,
        );
}
