//! Integration tests for metadata heal-on-corrupt (cache-metadata-resilience Req 3, Task 6).
//!
//! Tests:
//! 1. Corrupt .meta → `get_metadata_classified` returns `corrupt == true`
//! 2. After store_range (simulating S3 refetch), .meta is replaced with valid metadata
//! 3. Second `get_metadata_classified` returns clean (no corruption)
//! 4. HEAD-only path: corrupt .meta → remove_corrupt_metadata removes the file

use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::disk_cache::{
    read_and_parse_metadata_blocking, CorruptReason, DiskCacheManager, MetadataReadOutcome,
};
use std::path::Path;
use tempfile::TempDir;

/// Helper: create a DiskCacheManager with metadata_io configured
fn make_disk_cache(dir: &Path) -> DiskCacheManager {
    let mut dcm = DiskCacheManager::new(dir.to_path_buf(), true, 1024, false, 1_048_576);
    // Use 4 MiB cap (default)
    dcm.set_metadata_config(4 * 1024 * 1024, 32);
    dcm
}

/// Helper: write garbage into the .meta file for a cache key
fn write_corrupt_meta(dcm: &DiskCacheManager, cache_key: &str, content: &str) {
    let path = dcm.get_new_metadata_file_path(cache_key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&path, content).unwrap();
}

// ---------------------------------------------------------------------------
// Test 1: Blocking helper classifies corrupt file as Corrupt{ParseFailure}
// ---------------------------------------------------------------------------

#[test]
fn test_blocking_helper_classifies_corrupt_stable_parse_failure() {
    let temp_dir = TempDir::new().unwrap();
    let meta_path = temp_dir.path().join("test.meta");

    // Write invalid JSON that is stable (won't change between retries)
    std::fs::write(&meta_path, r#"{"not_valid": true}"#).unwrap();

    let outcome = read_and_parse_metadata_blocking(&meta_path, 4 * 1024 * 1024);
    match outcome {
        MetadataReadOutcome::Corrupt { reason } => {
            assert_eq!(reason, CorruptReason::ParseFailure);
        }
        other => panic!("Expected Corrupt{{ParseFailure}}, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 2: Blocking helper classifies oversized file as Corrupt{Oversize}
// ---------------------------------------------------------------------------

#[test]
fn test_blocking_helper_classifies_oversize() {
    let temp_dir = TempDir::new().unwrap();
    let meta_path = temp_dir.path().join("test.meta");

    // Write a file exceeding the 100-byte cap we'll set
    std::fs::write(&meta_path, "x".repeat(200)).unwrap();

    let outcome = read_and_parse_metadata_blocking(&meta_path, 100);
    match outcome {
        MetadataReadOutcome::Corrupt { reason } => {
            assert_eq!(reason, CorruptReason::Oversize);
        }
        other => panic!("Expected Corrupt{{Oversize}}, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 3: get_metadata_classified returns corrupt flag for bad .meta
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_metadata_classified_returns_corrupt_on_bad_meta() {
    let temp_dir = TempDir::new().unwrap();
    let dcm = make_disk_cache(temp_dir.path());
    dcm.initialize().await.unwrap();

    let cache_key = "test-bucket/corrupt-object.bin";
    write_corrupt_meta(&dcm, cache_key, r#"{"garbage": true, "not_metadata": 1}"#);

    let lookup = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(lookup.corrupt, "Expected corrupt == true");
    assert!(lookup.meta.is_none(), "Expected meta == None");
    assert_eq!(
        lookup.corrupt_reason,
        Some(CorruptReason::ParseFailure),
        "Expected ParseFailure reason"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Full heal cycle — corrupt .meta → store_range → valid .meta → clean read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_heal_cycle_corrupt_then_store_replaces_meta() {
    let temp_dir = TempDir::new().unwrap();
    let mut dcm = make_disk_cache(temp_dir.path());
    dcm.initialize().await.unwrap();

    let cache_key = "heal-bucket/heal-object.txt";
    let test_data = b"Hello, healed world!";
    let object_metadata = ObjectMetadata::new(
        "\"etag-heal-123\"".to_string(),
        "Wed, 21 Oct 2025 07:28:00 GMT".to_string(),
        test_data.len() as u64,
        Some("text/plain".to_string()),
    );

    // Step 1: Write corrupt .meta
    write_corrupt_meta(&dcm, cache_key, "THIS IS CORRUPT GARBAGE NOT JSON AT ALL");

    // Step 2: Confirm classified as corrupt
    let lookup = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(lookup.corrupt, "Pre-heal: expected corrupt == true");

    // Step 3: Simulate what the GET miss path does — store_range writes fresh .meta
    dcm.store_range(
        cache_key,
        0,
        test_data.len() as u64 - 1,
        test_data,
        object_metadata.clone(),
        std::time::Duration::from_secs(3600),
        true,
    )
    .await
    .unwrap();

    // Step 4: Confirm the .meta is now valid
    let lookup_after = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(
        !lookup_after.corrupt,
        "Post-heal: expected corrupt == false"
    );
    assert!(
        lookup_after.meta.is_some(),
        "Post-heal: expected valid metadata"
    );
    let meta = lookup_after.meta.unwrap();
    assert_eq!(meta.cache_key, cache_key);
    assert_eq!(meta.object_metadata.etag, "\"etag-heal-123\"");
    assert_eq!(meta.ranges.len(), 1);

    // Step 5: get_metadata (thin wrapper) also returns clean data
    let thin_result = dcm.get_metadata(cache_key).await.unwrap();
    assert!(
        thin_result.is_some(),
        "get_metadata should return Some after heal"
    );
}

// ---------------------------------------------------------------------------
// Test 5: get_metadata (thin wrapper) returns None for corrupt .meta (backward compat)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_metadata_returns_none_for_corrupt() {
    let temp_dir = TempDir::new().unwrap();
    let dcm = make_disk_cache(temp_dir.path());
    dcm.initialize().await.unwrap();

    let cache_key = "test-bucket/backward-compat.bin";
    write_corrupt_meta(&dcm, cache_key, "not valid json");

    // get_metadata is the thin wrapper — should return None (backward compatible)
    let result = dcm.get_metadata(cache_key).await.unwrap();
    assert!(
        result.is_none(),
        "get_metadata should return None for corrupt file"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Second GET after heal is clean (no corruption log)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_second_get_after_heal_is_clean() {
    let temp_dir = TempDir::new().unwrap();
    let mut dcm = make_disk_cache(temp_dir.path());
    dcm.initialize().await.unwrap();

    let cache_key = "clean-bucket/second-get.dat";
    let test_data = b"data for second get test";
    let object_metadata = ObjectMetadata::new(
        "\"etag-second-get\"".to_string(),
        "Thu, 22 Oct 2025 08:00:00 GMT".to_string(),
        test_data.len() as u64,
        Some("application/octet-stream".to_string()),
    );

    // Write corrupt, then store (heal)
    write_corrupt_meta(&dcm, cache_key, r#"{"invalid": "metadata"}"#);
    dcm.store_range(
        cache_key,
        0,
        test_data.len() as u64 - 1,
        test_data,
        object_metadata,
        std::time::Duration::from_secs(3600),
        true,
    )
    .await
    .unwrap();

    // Second read should be clean — no corruption
    let lookup = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(!lookup.corrupt, "Second read should not be corrupt");
    assert!(lookup.meta.is_some(), "Second read should return metadata");
    assert_eq!(lookup.corrupt_reason, None);
}

// ---------------------------------------------------------------------------
// Test 7: Oversized .meta → store_range heals it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_oversized_meta_healed_by_store_range() {
    let temp_dir = TempDir::new().unwrap();
    let mut dcm =
        DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
    // Set a low cap (500 bytes) to trigger oversize classification
    dcm.set_metadata_config(500, 32);
    dcm.initialize().await.unwrap();

    let cache_key = "oversize-bucket/big-meta.bin";
    let test_data = b"small actual data";
    let object_metadata = ObjectMetadata::new(
        "\"etag-oversize\"".to_string(),
        "Fri, 23 Oct 2025 09:00:00 GMT".to_string(),
        test_data.len() as u64,
        Some("application/octet-stream".to_string()),
    );

    // Write an oversized .meta (exceeds 500 byte cap)
    write_corrupt_meta(&dcm, cache_key, &"x".repeat(1000));

    // Classified as corrupt/oversize
    let lookup = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(lookup.corrupt);
    assert_eq!(lookup.corrupt_reason, Some(CorruptReason::Oversize));

    // Heal via store_range
    dcm.store_range(
        cache_key,
        0,
        test_data.len() as u64 - 1,
        test_data,
        object_metadata,
        std::time::Duration::from_secs(3600),
        true,
    )
    .await
    .unwrap();

    // After heal — valid .meta is small enough to pass the cap
    // (store_range writes a normal ~2KB .meta which is > 500 bytes for this test,
    // so bump the cap to 4 MiB for the verification read)
    dcm.set_metadata_config(4 * 1024 * 1024, 32);
    let lookup_after = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(!lookup_after.corrupt);
    assert!(lookup_after.meta.is_some());
}

// ---------------------------------------------------------------------------
// Test 8: Legacy CacheEntry .meta → detected as Corrupt{Legacy} (Task 7)
// ---------------------------------------------------------------------------

#[test]
fn test_blocking_helper_classifies_legacy_cache_entry() {
    let temp_dir = TempDir::new().unwrap();
    let meta_path = temp_dir.path().join("legacy.meta");

    // Write a legacy CacheEntry-format .meta with inline body
    let legacy_content = r#"{
        "cache_key": "bucket/object.bin",
        "headers": {"content-type": "application/octet-stream"},
        "body": [72, 101, 108, 108, 111],
        "ranges": {},
        "is_put_cached": true
    }"#;
    std::fs::write(&meta_path, legacy_content).unwrap();

    let outcome = read_and_parse_metadata_blocking(&meta_path, 4 * 1024 * 1024);
    match outcome {
        MetadataReadOutcome::Corrupt { reason } => {
            assert_eq!(reason, CorruptReason::Legacy);
        }
        other => panic!("Expected Corrupt{{Legacy}}, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 9: Legacy detection does NOT fire for non-legacy invalid JSON
// ---------------------------------------------------------------------------

#[test]
fn test_blocking_helper_does_not_detect_legacy_for_non_legacy_json() {
    let temp_dir = TempDir::new().unwrap();
    let meta_path = temp_dir.path().join("not_legacy.meta");

    // Valid JSON object but NOT legacy (no "body", no "is_put_cached")
    let content = r#"{"some_field": "value", "another": 42}"#;
    std::fs::write(&meta_path, content).unwrap();

    let outcome = read_and_parse_metadata_blocking(&meta_path, 4 * 1024 * 1024);
    match outcome {
        MetadataReadOutcome::Corrupt { reason } => {
            assert_eq!(
                reason,
                CorruptReason::ParseFailure,
                "Non-legacy invalid JSON should be ParseFailure, not Legacy"
            );
        }
        other => panic!("Expected Corrupt{{ParseFailure}}, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 10: Legacy with object_metadata present → NOT legacy (it's a different issue)
// ---------------------------------------------------------------------------

#[test]
fn test_blocking_helper_does_not_detect_legacy_when_object_metadata_present() {
    let temp_dir = TempDir::new().unwrap();
    let meta_path = temp_dir.path().join("has_object_metadata.meta");

    // Has body + is_put_cached BUT also has object_metadata → not legacy
    let content = r#"{
        "cache_key": "bucket/obj.bin",
        "body": [1, 2, 3],
        "is_put_cached": true,
        "object_metadata": {"etag": "\"abc\""}
    }"#;
    std::fs::write(&meta_path, content).unwrap();

    let outcome = read_and_parse_metadata_blocking(&meta_path, 4 * 1024 * 1024);
    match outcome {
        MetadataReadOutcome::Corrupt { reason } => {
            // Has object_metadata, so the legacy check doesn't match;
            // it falls through to ParseFailure (fails NewCacheMetadata parse)
            assert_eq!(
                reason,
                CorruptReason::ParseFailure,
                "File with object_metadata should be ParseFailure, not Legacy"
            );
        }
        other => panic!("Expected Corrupt{{ParseFailure}}, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 11: Full heal cycle for legacy .meta → store_range heals → second GET clean
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_legacy_meta_healed_by_store_range() {
    let temp_dir = TempDir::new().unwrap();
    let mut dcm = make_disk_cache(temp_dir.path());
    dcm.initialize().await.unwrap();

    let cache_key = "legacy-bucket/legacy-object.txt";
    let test_data = b"Fresh data after legacy heal";
    let object_metadata = ObjectMetadata::new(
        "\"etag-legacy-heal\"".to_string(),
        "Mon, 16 Jun 2025 12:00:00 GMT".to_string(),
        test_data.len() as u64,
        Some("text/plain".to_string()),
    );

    // Step 1: Write a legacy CacheEntry .meta
    let legacy_content = r#"{
        "cache_key": "legacy-bucket/legacy-object.txt",
        "headers": {"content-type": "text/plain", "content-length": "28"},
        "body": [70, 114, 101, 115, 104, 32, 100, 97, 116, 97, 32, 97, 102, 116, 101, 114, 32, 108, 101, 103, 97, 99, 121, 32, 104, 101, 97, 108],
        "ranges": {},
        "is_put_cached": true
    }"#;
    write_corrupt_meta(&dcm, cache_key, legacy_content);

    // Step 2: Classified as corrupt/legacy
    let lookup = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(
        lookup.corrupt,
        "Legacy .meta should be classified as corrupt"
    );
    assert_eq!(
        lookup.corrupt_reason,
        Some(CorruptReason::Legacy),
        "Should be classified as Legacy"
    );

    // Step 3: Heal via store_range (simulating what GET miss path does)
    dcm.store_range(
        cache_key,
        0,
        test_data.len() as u64 - 1,
        test_data,
        object_metadata.clone(),
        std::time::Duration::from_secs(3600),
        true,
    )
    .await
    .unwrap();

    // Step 4: Second GET is clean
    let lookup_after = dcm.get_metadata_classified(cache_key).await.unwrap();
    assert!(!lookup_after.corrupt, "After heal, should not be corrupt");
    assert!(
        lookup_after.meta.is_some(),
        "After heal, should have valid metadata"
    );
    let meta = lookup_after.meta.unwrap();
    assert_eq!(meta.cache_key, cache_key);
    assert_eq!(meta.object_metadata.etag, "\"etag-legacy-heal\"");
    assert_eq!(meta.object_metadata.content_length, test_data.len() as u64);
    assert_eq!(lookup_after.corrupt_reason, None);
}
