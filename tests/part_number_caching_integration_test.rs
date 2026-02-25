//! Integration test for part number caching end-to-end workflow
//!
//! Tests Requirements: All from part-number-caching spec
//!
//! This test validates:
//! - Request part 1 (cache miss)
//! - Verify part stored and metadata populated
//! - Request part 1 again (cache hit)
//! - Verify response headers match

use s3_proxy::{cache::CacheManager, config::SharedStorageConfig};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

/// Helper function to create test cache manager
fn create_test_cache_manager(temp_dir: &TempDir) -> CacheManager {
    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false, // RAM cache disabled for these tests
        0,
        1024 * 1024 * 1024, // 1GB max cache size
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,
        true,
        Duration::from_secs(3600), // GET_TTL: 1 hour
        Duration::from_secs(3600), // HEAD_TTL: 1 hour
        Duration::from_secs(3600), // PUT_TTL: 1 hour
        false,
        SharedStorageConfig::default(),
        10.0,
        false,                      // write_cache_enabled: false - not testing write cache
        Duration::from_secs(86400), // 1 day incomplete_upload_ttl
        s3_proxy::config::MetadataCacheConfig::default(),
        95,                                    // eviction_trigger_percent
        80,                                    // eviction_target_percent
        true,                                          // read_cache_enabled
        std::time::Duration::from_secs(60),            // bucket_settings_staleness_threshold
    )
}

/// Helper function to create S3 response headers for a part
fn create_part_response_headers(
    _part_number: u32,
    start: u64,
    end: u64,
    total_size: u64,
    parts_count: u32,
    etag: &str,
) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "content-range".to_string(),
        format!("bytes {}-{}/{}", start, end, total_size),
    );
    headers.insert("content-length".to_string(), (end - start + 1).to_string());
    headers.insert("etag".to_string(), etag.to_string());
    headers.insert(
        "last-modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );
    headers.insert("accept-ranges".to_string(), "bytes".to_string());
    headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    headers.insert("x-amz-mp-parts-count".to_string(), parts_count.to_string());
    headers.insert("checksum-crc32c".to_string(), "AAAAAA==".to_string());
    headers.insert("checksum-type".to_string(), "COMPOSITE".to_string());
    headers.insert("server-side-encryption".to_string(), "AES256".to_string());
    headers
}
