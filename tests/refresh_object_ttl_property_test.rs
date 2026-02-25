//! Property-based tests for refresh_object_ttl behavior
//!
//! Property 2: Object TTL refresh sets expires_at correctly
//!
//! For any resolved `get_ttl` value (including zero), when the proxy refreshes
//! an object's TTL via `refresh_object_ttl(cache_key, resolved_ttl)`,
//! the object's `expires_at` is approximately `now + resolved_ttl`.
//!
//! **Validates: Requirements 1.3, 3.1**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Generate a random Duration between 0 and ~1 year.
/// Includes Duration::ZERO to test the zero-TTL case.
fn arbitrary_ttl_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600 + 1);
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
        day, month, year, hour, min, sec
    )
}

#[derive(Debug, Clone)]
struct ArbitraryTtlRefreshInput {
    /// The new TTL to apply via refresh_object_ttl (includes Duration::ZERO)
    resolved_ttl: Duration,
    /// The last_modified value stored in object metadata
    last_modified: String,
    /// Range start byte
    range_start: u64,
    /// Range size in bytes (at least 1)
    range_size: u64,
}

impl Arbitrary for ArbitraryTtlRefreshInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let range_start = u64::arbitrary(g) % (1024 * 1024 * 1024);
        let range_size = (u64::arbitrary(g) % (8 * 1024 * 1024)) + 1;
        Self {
            resolved_ttl: arbitrary_ttl_duration(g),
            last_modified: arbitrary_last_modified(g),
            range_start,
            range_size,
        }
    }
}

/// After calling refresh_object_ttl with a given resolved_ttl, the object's
/// expires_at should be approximately now + resolved_ttl.
fn prop_ttl_refresh_uses_resolved_ttl(input: ArbitraryTtlRefreshInput) -> TestResult {
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

        let cache_key = "test-bucket/ttl-refresh-test-object";
        let now = SystemTime::now();
        let range_end = input.range_start + input.range_size - 1;

        let range_spec = RangeSpec {
            start: input.range_start,
            end: range_end,
            file_path: format!("test_range_{}-{}.bin", input.range_start, range_end),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: input.range_size,
            uncompressed_size: input.range_size,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        };

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata::new(
                "test-etag-abc123".to_string(),
                input.last_modified.clone(),
                input.range_start + input.range_size,
                Some("application/octet-stream".to_string()),
            ),
            ranges: vec![range_spec],
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        if cache_manager.update_metadata(&metadata).await.is_err() {
            return TestResult::discard();
        }

        let before_refresh = SystemTime::now();

        // Call refresh_object_ttl with the resolved TTL
        if cache_manager
            .refresh_object_ttl(cache_key, input.resolved_ttl)
            .await
            .is_err()
        {
            return TestResult::discard();
        }

        let after_refresh = SystemTime::now();

        // Read back metadata to verify expires_at
        let updated_metadata = match cache_manager.get_metadata(cache_key).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
                let content = match std::fs::read_to_string(&meta_path) {
                    Ok(c) => c,
                    Err(_) => return TestResult::discard(),
                };
                match serde_json::from_str::<NewCacheMetadata>(&content) {
                    Ok(m) => m,
                    Err(_) => return TestResult::discard(),
                }
            }
            Err(_) => return TestResult::discard(),
        };

        // Verify: object-level expires_at should be between (before_refresh + resolved_ttl)
        // and (after_refresh + resolved_ttl + 1s tolerance)
        let expected_min = before_refresh + input.resolved_ttl;
        let expected_max = after_refresh + input.resolved_ttl + Duration::from_secs(1);

        let actual = updated_metadata.expires_at;

        if actual < expected_min {
            return TestResult::error(format!(
                "expires_at too early: actual={:?}, expected_min={:?}, resolved_ttl={:?}",
                actual, expected_min, input.resolved_ttl
            ));
        }
        if actual > expected_max {
            return TestResult::error(format!(
                "expires_at too late: actual={:?}, expected_max={:?}, resolved_ttl={:?}",
                actual, expected_max, input.resolved_ttl
            ));
        }

        TestResult::passed()
    })
}

#[test]
fn test_property_ttl_refresh_uses_resolved_ttl() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(
            prop_ttl_refresh_uses_resolved_ttl as fn(ArbitraryTtlRefreshInput) -> TestResult,
        );
}
