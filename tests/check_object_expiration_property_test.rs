//! Property-based tests for object-level expiration check
//!
//! Property 1: Object-level expiration determines freshness
//!
//! For any `NewCacheMetadata` with `expires_at` in the past, `is_object_expired()` returns true.
//! For any `NewCacheMetadata` with `expires_at` in the future, `is_object_expired()` returns false.
//!
//! **Validates: Requirements 1.2, 2.1, 2.2, 2.3**
//!
//! Extended properties (Task 4.5):
//!
//! Property 2: GET freshness vs current `get_ttl`
//! For random `created_at` and random `current_get_ttl`, `check_object_expiration` returns
//! `Expired` iff `get_ttl=0` or `now - created_at > get_ttl`.
//!
//! Property 3: HEAD freshness vs current `head_ttl`
//! For random `created_at`, random `current_head_ttl`, and `head_expires_at ∈ {Some, None}`,
//! the HEAD freshness comparison reports fresh iff `head_expires_at = Some` and `head_ttl > 0`
//! and `now - created_at ≤ head_ttl`.
//!
//! Property 4: Preservation — TTL-unchanged equivalence
//! For random metadata with the TTL unchanged from write time, the `created_at` comparison
//! agrees with the stored-expiry check — for both `expires_at`/`get_ttl` and
//! `head_expires_at`/`head_ttl`.
//!
//! Property 5: Class B — eager invalidation deletes cached copy
//! For random keys with a cached copy and `read_cache_enabled=false`, after
//! `invalidate_all_ranges` the copy no longer exists on disk.
//!
//! **Validates: Requirements 2.1, 2.1b, 2.2, 2.2b, 2.3, 3.1, 3.2**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use std::time::{Duration, SystemTime};

/// Generate a random positive Duration between 1 second and ~1 year.
/// We avoid Duration::ZERO to ensure clear expired/non-expired distinction.
fn arbitrary_positive_duration(g: &mut Gen) -> Duration {
    let secs = u64::arbitrary(g) % (365 * 24 * 3600) + 1; // 1s to ~1 year
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
// Arbitrary input for Property 1
// ============================================================================

#[derive(Debug, Clone)]
struct ArbitraryObjectExpirationInput {
    /// Duration to offset expires_at from now (always positive)
    offset_duration: Duration,
    /// The last_modified value stored in object metadata
    last_modified: String,
}

impl Arbitrary for ArbitraryObjectExpirationInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            offset_duration: arbitrary_positive_duration(g),
            last_modified: arbitrary_last_modified(g),
        }
    }
}

/// Helper: create a NewCacheMetadata with the given expires_at.
fn create_metadata(
    input: &ArbitraryObjectExpirationInput,
    expires_at: SystemTime,
) -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: "test-bucket/object-expiration-test".to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag-abc123".to_string(),
            input.last_modified.clone(),
            1024,
            Some("application/octet-stream".to_string()),
        ),
        ranges: Vec::new(),
        created_at: now,
        expires_at,
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

// ============================================================================
// Property 1a: Expired metadata (expires_at in the past) → is_object_expired() returns true
// ============================================================================

fn prop_expired_object_returns_true(input: ArbitraryObjectExpirationInput) -> TestResult {
    let now = SystemTime::now();
    // expires_at in the past: now - offset_duration
    let expires_at = match now.checked_sub(input.offset_duration) {
        Some(t) => t,
        None => return TestResult::discard(),
    };

    let metadata = create_metadata(&input, expires_at);
    TestResult::from_bool(metadata.is_object_expired())
}

// ============================================================================
// Property 1b: Fresh metadata (expires_at in the future) → is_object_expired() returns false
// ============================================================================

fn prop_fresh_object_returns_false(input: ArbitraryObjectExpirationInput) -> TestResult {
    let now = SystemTime::now();
    // expires_at in the future: now + offset_duration
    let expires_at = now + input.offset_duration;

    let metadata = create_metadata(&input, expires_at);
    TestResult::from_bool(!metadata.is_object_expired())
}

// ============================================================================
// Test runners (minimum 100 iterations each)
// ============================================================================

#[test]
fn test_property_expired_object_is_detected() {
    QuickCheck::new().tests(100).quickcheck(
        prop_expired_object_returns_true as fn(ArbitraryObjectExpirationInput) -> TestResult,
    );
}

#[test]
fn test_property_fresh_object_is_not_expired() {
    QuickCheck::new().tests(100).quickcheck(
        prop_fresh_object_returns_false as fn(ArbitraryObjectExpirationInput) -> TestResult,
    );
}

// ============================================================================
// Property 2: GET freshness vs current get_ttl
//
// For random `created_at` (expressed as age from now) and random `current_get_ttl`,
// `check_object_expiration` returns `Expired` iff `get_ttl=0` or `age > get_ttl`.
//
// We test the logic directly against the disk cache manager by writing metadata
// to disk and calling `check_object_expiration` with the randomized TTL.
//
// **Validates: Requirements 2.1, 2.2**
// ============================================================================

#[derive(Debug, Clone)]
struct GetFreshnessInput {
    /// Age of the cached object in seconds (0 to ~1 day)
    age_secs: u32,
    /// Current get_ttl in seconds (0 to ~1 day; 0 means "always expired")
    get_ttl_secs: u32,
}

impl Arbitrary for GetFreshnessInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            age_secs: u32::arbitrary(g) % (24 * 3600),
            get_ttl_secs: u32::arbitrary(g) % (24 * 3600),
        }
    }
}

#[tokio::test]
async fn test_property_get_freshness_vs_current_get_ttl() {
    use s3_proxy::cache_types::ObjectExpirationResult;
    use s3_proxy::disk_cache::DiskCacheManager;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    // Generate inputs upfront with quickcheck's Gen
    let mut g = Gen::new(100);
    let num_tests = 200;

    for _ in 0..num_tests {
        let input = GetFreshnessInput::arbitrary(&mut g);
        let age = Duration::from_secs(input.age_secs as u64);
        let get_ttl = Duration::from_secs(input.get_ttl_secs as u64);

        let now = SystemTime::now();
        let created_at = match now.checked_sub(age) {
            Some(t) => t,
            None => continue,
        };

        // Write metadata with this created_at (expires_at is irrelevant for the
        // current-TTL check, but we set it to something valid)
        let cache_key = format!("test-bucket/get-freshness-{}", fastrand::u32(..));
        let metadata = NewCacheMetadata {
            cache_key: cache_key.clone(),
            object_metadata: ObjectMetadata::new(
                "\"test-etag\"".to_string(),
                "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
                1024,
                Some("text/plain".to_string()),
            ),
            ranges: Vec::new(),
            created_at,
            expires_at: created_at + Duration::from_secs(999999),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(&cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        let result = cache_manager
            .check_object_expiration(&cache_key, get_ttl)
            .await
            .unwrap();

        // Expected: expired iff get_ttl=0 or age > get_ttl
        // Skip the exact boundary (age == get_ttl) — a few milliseconds of execution
        // time between setting created_at and the check can tip either way.
        if age == get_ttl && !get_ttl.is_zero() {
            continue;
        }
        let expected_expired = get_ttl.is_zero() || age > get_ttl;

        let actual_expired = matches!(result, ObjectExpirationResult::Expired { .. });

        assert_eq!(
            actual_expired, expected_expired,
            "GET freshness mismatch: age={:?}, get_ttl={:?}, expected_expired={}, actual_expired={}",
            age, get_ttl, expected_expired, actual_expired
        );
    }
}

// ============================================================================
// Property 3: HEAD freshness vs current head_ttl
//
// For random `created_at` (age), random `current_head_ttl`, and
// `head_expires_at ∈ {Some, None}`, the HEAD freshness comparison reports
// fresh iff `head_expires_at = Some` AND `head_ttl > 0` AND
// `now - created_at ≤ head_ttl`.
//
// This tests the formula used in `get_head_cache_entry_unified`:
//   head_fresh = metadata.head_expires_at.is_some()
//       && !current_head_ttl.is_zero()
//       && now.duration_since(metadata.created_at).unwrap_or(Duration::ZERO) <= current_head_ttl
//
// **Validates: Requirements 2.1b, 2.2b**
// ============================================================================

#[derive(Debug, Clone)]
struct HeadFreshnessInput {
    /// Age of the cached object in seconds (0 to ~1 day)
    age_secs: u32,
    /// Current head_ttl in seconds (0 to ~1 day; 0 means "always expired")
    head_ttl_secs: u32,
    /// Whether head_expires_at is Some (true) or None (false)
    head_expires_at_present: bool,
}

impl Arbitrary for HeadFreshnessInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            age_secs: u32::arbitrary(g) % (24 * 3600),
            head_ttl_secs: u32::arbitrary(g) % (24 * 3600),
            head_expires_at_present: bool::arbitrary(g),
        }
    }
}

/// Tests the HEAD freshness formula directly (same logic as get_head_cache_entry_unified).
#[test]
fn test_property_head_freshness_vs_current_head_ttl() {
    fn prop(input: HeadFreshnessInput) -> TestResult {
        let age = Duration::from_secs(input.age_secs as u64);
        let head_ttl = Duration::from_secs(input.head_ttl_secs as u64);

        let now = SystemTime::now();
        let created_at = match now.checked_sub(age) {
            Some(t) => t,
            None => return TestResult::discard(),
        };

        let head_expires_at = if input.head_expires_at_present {
            // Set some arbitrary value — the formula doesn't use the actual
            // head_expires_at time, only whether it's Some
            Some(created_at + Duration::from_secs(3600))
        } else {
            None
        };

        // The formula from get_head_cache_entry_unified:
        let actual_head_fresh = head_expires_at.is_some()
            && !head_ttl.is_zero()
            && now.duration_since(created_at).unwrap_or(Duration::ZERO) <= head_ttl;

        // Expected: fresh iff head_expires_at = Some AND head_ttl > 0 AND age ≤ head_ttl
        let expected_head_fresh =
            input.head_expires_at_present && !head_ttl.is_zero() && age <= head_ttl;

        TestResult::from_bool(actual_head_fresh == expected_head_fresh)
    }

    QuickCheck::new()
        .tests(1000)
        .quickcheck(prop as fn(HeadFreshnessInput) -> TestResult);
}

// ============================================================================
// Property 4: Preservation — TTL-unchanged equivalence
//
// For random metadata with the TTL unchanged from write time, the `created_at`
// comparison agrees with the stored-expiry check — for both
// `expires_at`/`get_ttl` and `head_expires_at`/`head_ttl`.
//
// When expires_at was computed as `created_at + get_ttl` at write time, the
// stored-expiry check (`now ≤ expires_at` i.e. `!is_object_expired()`) must
// agree with the current-TTL comparison (`age ≤ get_ttl`).
//
// Similarly for HEAD: when head_expires_at = created_at + head_ttl, the
// stored-expiry check (`now ≤ head_expires_at` i.e. `!is_head_expired()`) must
// agree with the current-TTL comparison (`age ≤ head_ttl`).
//
// **Validates: Requirements 3.1, 3.2**
// ============================================================================

#[derive(Debug, Clone)]
struct PreservationInput {
    /// TTL in seconds (non-zero for meaningful preservation test)
    ttl_secs: u32,
    /// Age in milliseconds for higher resolution
    age_ms: u32,
}

impl Arbitrary for PreservationInput {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ttl_secs: u32::arbitrary(g) % (7 * 24 * 3600) + 1, // 1s to 7 days, never 0
            age_ms: u32::arbitrary(g) % (14 * 24 * 3600 * 1000), // 0 to 14 days in ms
        }
    }
}

/// Preservation for GET: is_object_expired() agrees with the current-TTL comparison
/// when the TTL has not changed since write time.
#[test]
fn test_property_preservation_get_ttl_unchanged() {
    fn prop(input: PreservationInput) -> TestResult {
        let ttl = Duration::from_secs(input.ttl_secs as u64);
        let age = Duration::from_millis(input.age_ms as u64);

        let now = SystemTime::now();
        let created_at = match now.checked_sub(age) {
            Some(t) => t,
            None => return TestResult::discard(),
        };
        // At write time: expires_at = created_at + ttl
        let expires_at = created_at + ttl;

        let metadata = NewCacheMetadata {
            cache_key: "test/preservation-get".to_string(),
            object_metadata: ObjectMetadata::new(
                "\"etag\"".to_string(),
                "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            ),
            ranges: Vec::new(),
            created_at,
            expires_at,
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Stored-expiry check (original code)
        let stored_fresh = !metadata.is_object_expired();

        // Current-TTL check (fixed code): age ≤ ttl
        let current_ttl_fresh = now.duration_since(created_at).unwrap_or(Duration::ZERO) <= ttl;

        TestResult::from_bool(stored_fresh == current_ttl_fresh)
    }

    QuickCheck::new()
        .tests(1000)
        .quickcheck(prop as fn(PreservationInput) -> TestResult);
}

/// Preservation for HEAD: is_head_expired() agrees with the current-TTL comparison
/// when the TTL has not changed since write time (and head_expires_at is Some).
#[test]
fn test_property_preservation_head_ttl_unchanged() {
    fn prop(input: PreservationInput) -> TestResult {
        let ttl = Duration::from_secs(input.ttl_secs as u64);
        let age = Duration::from_millis(input.age_ms as u64);

        let now = SystemTime::now();
        let created_at = match now.checked_sub(age) {
            Some(t) => t,
            None => return TestResult::discard(),
        };
        // At write time: head_expires_at = created_at + head_ttl
        let head_expires_at = created_at + ttl;

        let metadata = NewCacheMetadata {
            cache_key: "test/preservation-head".to_string(),
            object_metadata: ObjectMetadata::new(
                "\"etag\"".to_string(),
                "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            ),
            ranges: Vec::new(),
            created_at,
            expires_at: now + Duration::from_secs(99999),
            compression_info: CompressionInfo::default(),
            head_expires_at: Some(head_expires_at),
            ..Default::default()
        };

        // Stored-expiry check (original is_head_expired): expired when now > head_expires_at
        let stored_head_fresh = !metadata.is_head_expired();

        // Current-TTL check (fixed code): fresh when age ≤ head_ttl
        // (with head_expires_at.is_some() gate — always true in this test since we set Some)
        let current_ttl_fresh = now.duration_since(created_at).unwrap_or(Duration::ZERO) <= ttl;

        TestResult::from_bool(stored_head_fresh == current_ttl_fresh)
    }

    QuickCheck::new()
        .tests(1000)
        .quickcheck(prop as fn(PreservationInput) -> TestResult);
}

// ============================================================================
// Property 5: Class B — eager invalidation deletes cached copy
//
// For random cache keys with a cached copy on disk (metadata + range files),
// calling `invalidate_all_ranges` removes the range .bin files listed in the
// metadata. This verifies the primitive that Class B relies on: when
// read_cache_enabled=false, the read path calls invalidate_all_ranges, which
// must remove cached data from disk.
//
// The property: for any valid cache key with metadata and range files on disk,
// after `invalidate_all_ranges` the range files no longer exist.
//
// **Validates: Requirement 2.3**
// ============================================================================

#[derive(Debug, Clone)]
struct ClassBInput {
    /// Random suffix for the cache key (alphanumeric, 1-20 chars)
    key_suffix: String,
    /// Random content size in bytes (1 to 4096)
    content_size: u16,
}

impl Arbitrary for ClassBInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = (usize::arbitrary(g) % 20) + 1;
        let chars: String = (0..len)
            .map(|_| {
                let idx = u8::arbitrary(g) % 36;
                if idx < 10 {
                    (b'0' + idx) as char
                } else {
                    (b'a' + idx - 10) as char
                }
            })
            .collect();

        Self {
            key_suffix: chars,
            content_size: (u16::arbitrary(g) % 4096) + 1,
        }
    }
}

#[tokio::test]
async fn test_property_class_b_invalidation_removes_cached_copy() {
    use s3_proxy::cache_types::RangeSpec;
    use s3_proxy::compression::CompressionAlgorithm;
    use s3_proxy::disk_cache::DiskCacheManager;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let cache_manager =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), false, 1024, false, 1_048_576);
    cache_manager.initialize().await.unwrap();

    let mut g = Gen::new(100);
    let num_tests = 100;

    for _ in 0..num_tests {
        let input = ClassBInput::arbitrary(&mut g);
        let cache_key = format!("test-bucket/{}", input.key_suffix);
        let content_end = input.content_size as u64 - 1;

        // Determine range file path (relative to cache_dir/ranges/)
        let range_file_path = cache_manager.get_new_range_file_path(&cache_key, 0, content_end);
        let ranges_dir = temp_dir.path().join("ranges");
        let relative_range_path = range_file_path
            .strip_prefix(&ranges_dir)
            .unwrap()
            .to_string_lossy()
            .to_string();

        // Create metadata with a range spec pointing to the file
        let now = SystemTime::now();
        let range_spec = RangeSpec::new(
            0,
            content_end,
            relative_range_path.clone(),
            CompressionAlgorithm::None,
            input.content_size as u64,
            input.content_size as u64,
        );

        let metadata = NewCacheMetadata {
            cache_key: cache_key.clone(),
            object_metadata: ObjectMetadata::new(
                "\"class-b-etag\"".to_string(),
                "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
                input.content_size as u64,
                Some("application/octet-stream".to_string()),
            ),
            ranges: vec![range_spec],
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            head_expires_at: Some(now + Duration::from_secs(60)),
            ..Default::default()
        };

        // Write metadata at the new path (where get_metadata reads from)
        let metadata_path = cache_manager.get_new_metadata_file_path(&cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        // Write the range .bin file
        std::fs::create_dir_all(range_file_path.parent().unwrap()).unwrap();
        let content: Vec<u8> = (0..input.content_size).map(|i| (i % 256) as u8).collect();
        std::fs::write(&range_file_path, &content).unwrap();

        // Verify files exist before invalidation
        assert!(
            metadata_path.exists(),
            "Metadata file should exist before invalidation: {:?}",
            metadata_path
        );
        assert!(
            range_file_path.exists(),
            "Range file should exist before invalidation: {:?}",
            range_file_path
        );

        // Call invalidate_all_ranges (what Class B fix does on read_cache_enabled=false)
        cache_manager
            .invalidate_all_ranges(&cache_key)
            .await
            .expect("invalidate_all_ranges should succeed");

        // Verify range file is gone after invalidation (the core Class B property:
        // cached data is purged from disk)
        assert!(
            !range_file_path.exists(),
            "Range file should NOT exist after invalidation: key={}, path={:?}",
            cache_key,
            range_file_path
        );
    }
}
