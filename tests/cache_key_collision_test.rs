//! Cache key sanitization and collision prevention tests
//!
//! Validates Requirements 3.1, 3.4, 2.5 (cache key uniqueness and sanitization)
//! Tests that special characters are properly encoded to prevent collisions

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::CacheMetadata;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create test metadata
fn create_metadata(etag: &str, content_length: u64) -> CacheMetadata {
    CacheMetadata {
        etag: etag.to_string(),
        last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
        content_length,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    }
}

/// Helper to create and initialize a test cache manager with JournalConsolidator
async fn create_test_cache_manager(cache_dir: std::path::PathBuf) -> Arc<CacheManager> {
    let cache_manager = Arc::new(CacheManager::new_with_defaults(cache_dir, false, 0));

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();

    // Now initialize the cache manager (which requires the consolidator to exist)
    cache_manager.initialize().await.unwrap();

    cache_manager
}

/// Test that cache key sanitization prevents collisions between similar keys
/// Old sanitization: "file:name" and "file_colon_name" would collide
/// New sanitization: "file%3Aname" vs "file_colon_name" (no collision)
#[tokio::test]
async fn test_cache_key_sanitization_prevents_collisions() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    let key1 = "file:name.txt";
    let key2 = "file_colon_name.txt";
    let data1 = b"Data for key1 with colon";
    let data2 = b"Data for key2 with literal _colon_";

    // Store both entries
    cache_manager
        .store_response(key1, data1, create_metadata("etag1", data1.len() as u64))
        .await
        .unwrap();
    cache_manager
        .store_response(key2, data2, create_metadata("etag2", data2.len() as u64))
        .await
        .unwrap();

    // Retrieve and verify no collision
    let entry1 = cache_manager
        .get_cached_response(key1)
        .await
        .unwrap()
        .unwrap();
    let entry2 = cache_manager
        .get_cached_response(key2)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(entry1.body.as_ref().unwrap(), data1);
    assert_eq!(entry2.body.as_ref().unwrap(), data2);
    assert_ne!(
        entry1.body, entry2.body,
        "Entries should have different data - no collision!"
    );
    assert_eq!(entry1.metadata.etag, "etag1");
    assert_eq!(entry2.metadata.etag, "etag2");
}

/// Test round-trip storage and retrieval with various special characters
#[tokio::test]
async fn test_cache_key_sanitization_round_trip() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Test various special characters that need sanitization
    let test_keys = vec![
        "bucket/path:with:colons",
        "bucket/path?with?questions",
        "bucket/path with spaces",
        "bucket/path*with*stars",
        "bucket/path<with>brackets",
    ];

    for (i, key) in test_keys.iter().enumerate() {
        let data = format!("Data {}", i).into_bytes();
        let metadata = create_metadata(&format!("etag{}", i), data.len() as u64);

        cache_manager
            .store_response(key, &data, metadata)
            .await
            .unwrap();

        let entry = cache_manager
            .get_cached_response(key)
            .await
            .unwrap()
            .expect(&format!("Key '{}' should be cached", key));
        assert_eq!(entry.body.as_ref().unwrap(), &data);
        assert_eq!(entry.metadata.etag, format!("etag{}", i));
    }
}

/// Test that similar keys with special vs literal characters don't collide
#[tokio::test]
async fn test_cache_key_sanitization_uniqueness() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(temp_dir.path().to_path_buf()).await;

    // Test pairs of similar keys that should not collide
    let similar_keys = vec![
        ("file:name", "file_colon_name"),
        ("file?name", "file_question_name"),
        ("file name", "file_space_name"),
    ];

    for (key_with_special, key_with_literal) in similar_keys {
        let data_special = format!("Data for {}", key_with_special).into_bytes();
        let data_literal = format!("Data for {}", key_with_literal).into_bytes();

        cache_manager
            .store_response(
                key_with_special,
                &data_special,
                create_metadata("etag_special", data_special.len() as u64),
            )
            .await
            .unwrap();
        cache_manager
            .store_response(
                key_with_literal,
                &data_literal,
                create_metadata("etag_literal", data_literal.len() as u64),
            )
            .await
            .unwrap();

        let entry_special = cache_manager
            .get_cached_response(key_with_special)
            .await
            .unwrap()
            .unwrap();
        let entry_literal = cache_manager
            .get_cached_response(key_with_literal)
            .await
            .unwrap()
            .unwrap();

        assert_ne!(
            entry_special.body, entry_literal.body,
            "Keys '{}' and '{}' should not collide",
            key_with_special, key_with_literal
        );
        assert_eq!(entry_special.metadata.etag, "etag_special");
        assert_eq!(entry_literal.metadata.etag, "etag_literal");
    }
}
