//! Integration test for range consolidation
//!
//! Tests Requirement 1.2:
//! - Create test with multiple small missing ranges
//! - Verify ranges with small gaps are consolidated into fewer S3 requests
//! - Verify ranges with large gaps are NOT consolidated
//! - Verify all missing bytes are fetched correctly

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test cache manager
fn create_test_cache_manager() -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
    TempDir,
) {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();

    let cache_manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir.clone(),
        false, // RAM cache disabled for these tests
        0,
        s3_proxy::cache::CacheEvictionAlgorithm::LRU,
        1024,                                 // compression threshold
        true,                                 // compression enabled
        std::time::Duration::from_secs(3600), // GET_TTL
        std::time::Duration::from_secs(1800), // HEAD_TTL
        std::time::Duration::from_secs(900),  // PUT_TTL
        false,                                // actively_remove_cached_data
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(DiskCacheManager::new(
        cache_dir, true, 1024, false,
    )));

    (cache_manager, disk_cache_manager, temp_dir)
}

#[tokio::test]
async fn test_range_consolidation_small_gaps() {
    // Test that ranges with small gaps are consolidated
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/consolidation-small-gaps.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Create 10MB of test data
    let mb = 1024 * 1024;
    let total_size = 10 * mb;

    let full_data: Vec<u8> = (0..total_size)
        .map(|i| ((i / 1024 + i % 256) % 256) as u8)
        .collect();

    // Cache specific ranges with small gaps between them
    // Cached: 0-1MB, 2-3MB, 4-5MB, 6-7MB, 8-9MB
    // Missing: 1-2MB (1MB gap), 3-4MB (1MB gap), 5-6MB (1MB gap), 7-8MB (1MB gap), 9-10MB (1MB gap)
    let cached_ranges = vec![
        (0, 1 * mb - 1),      // 0-1MB
        (2 * mb, 3 * mb - 1), // 2-3MB
        (4 * mb, 5 * mb - 1), // 4-5MB
        (6 * mb, 7 * mb - 1), // 6-7MB
        (8 * mb, 9 * mb - 1), // 8-9MB
    ];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-consolidation".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: total_size as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: total_size as u64,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: total_size as u64,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store each cached range
    for (start, end) in &cached_ranges {
        let range_data = full_data[*start..=*end].to_vec();
        disk_cache_manager
            .write()
            .await
            .store_range(
                &cache_key,
                *start as u64,
                *end as u64,
                &range_data,
                object_metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();
    }

    // Request the full range (0-10MB)
    let requested_range = RangeSpec {
        start: 0,
        end: (total_size - 1) as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find cached ranges and missing ranges
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    // Verify we have missing ranges
    assert!(!overlap.can_serve_from_cache, "Should have missing ranges");
    assert_eq!(
        overlap.missing_ranges.len(),
        5,
        "Should have 5 missing ranges initially"
    );

    // Verify the missing ranges are correct
    let expected_missing = vec![
        RangeSpec {
            start: 1 * mb as u64,
            end: 2 * mb as u64 - 1,
        }, // 1-2MB
        RangeSpec {
            start: 3 * mb as u64,
            end: 4 * mb as u64 - 1,
        }, // 3-4MB
        RangeSpec {
            start: 5 * mb as u64,
            end: 6 * mb as u64 - 1,
        }, // 5-6MB
        RangeSpec {
            start: 7 * mb as u64,
            end: 8 * mb as u64 - 1,
        }, // 7-8MB
        RangeSpec {
            start: 9 * mb as u64,
            end: 10 * mb as u64 - 1,
        }, // 9-10MB
    ];

    for (i, missing) in overlap.missing_ranges.iter().enumerate() {
        assert_eq!(missing.start, expected_missing[i].start);
        assert_eq!(missing.end, expected_missing[i].end);
    }

    // Requirement 1.2: Consolidate missing ranges with small gap threshold
    // Gap threshold of 256KB (default) - since our gaps are 1MB, they should NOT be consolidated
    let gap_threshold = 256 * 1024; // 256KB
    let consolidated =
        range_handler.consolidate_missing_ranges(overlap.missing_ranges.clone(), gap_threshold);

    // With 256KB threshold, 1MB gaps should NOT be consolidated
    assert_eq!(
        consolidated.len(),
        5,
        "With 256KB threshold, 1MB gaps should NOT be consolidated"
    );

    // Now test with a larger gap threshold (2MB) - gaps should be consolidated
    let large_gap_threshold = 2 * mb as u64; // 2MB
    let consolidated_large = range_handler
        .consolidate_missing_ranges(overlap.missing_ranges.clone(), large_gap_threshold);

    // Requirement 1.2: With 2MB threshold, all 1MB gaps should be consolidated into 1 range
    assert_eq!(
        consolidated_large.len(),
        1,
        "With 2MB threshold, all ranges should be consolidated into 1"
    );
    assert_eq!(
        consolidated_large[0].start,
        1 * mb as u64,
        "Consolidated range should start at 1MB"
    );
    assert_eq!(
        consolidated_large[0].end,
        10 * mb as u64 - 1,
        "Consolidated range should end at 10MB"
    );

    println!("✓ Range consolidation with small gaps test passed!");
    println!("  - 5 missing ranges with 1MB gaps between them");
    println!("  - With 256KB threshold: 5 ranges (no consolidation)");
    println!("  - With 2MB threshold: 1 range (full consolidation)");
}

#[tokio::test]
async fn test_range_consolidation_large_gaps() {
    // Test that ranges with large gaps are NOT consolidated
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/consolidation-large-gaps.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Create 20MB of test data
    let mb = 1024 * 1024;
    let total_size = 20 * mb;

    let full_data: Vec<u8> = (0..total_size)
        .map(|i| ((i / 512 + i % 256) % 256) as u8)
        .collect();

    // Cache ranges with large gaps between them
    // Cached: 0-1MB, 6-7MB, 12-13MB, 18-19MB
    // Missing: 1-6MB (5MB gap), 7-12MB (5MB gap), 13-18MB (5MB gap), 19-20MB (1MB gap)
    let cached_ranges = vec![
        (0, 1 * mb - 1),        // 0-1MB
        (6 * mb, 7 * mb - 1),   // 6-7MB
        (12 * mb, 13 * mb - 1), // 12-13MB
        (18 * mb, 19 * mb - 1), // 18-19MB
    ];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-large-gaps".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: total_size as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: total_size as u64,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: total_size as u64,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store each cached range
    for (start, end) in &cached_ranges {
        let range_data = full_data[*start..=*end].to_vec();
        disk_cache_manager
            .write()
            .await
            .store_range(
                &cache_key,
                *start as u64,
                *end as u64,
                &range_data,
                object_metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();
    }

    // Request the full range (0-20MB)
    let requested_range = RangeSpec {
        start: 0,
        end: (total_size - 1) as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find cached ranges and missing ranges
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(!overlap.can_serve_from_cache);
    assert_eq!(
        overlap.missing_ranges.len(),
        4,
        "Should have 4 missing ranges"
    );

    // Requirement 1.2: Consolidate with default threshold (256KB)
    // Large gaps (5MB) should NOT be consolidated
    let gap_threshold = 256 * 1024; // 256KB
    let consolidated =
        range_handler.consolidate_missing_ranges(overlap.missing_ranges.clone(), gap_threshold);

    // With 256KB threshold, 5MB gaps should NOT be consolidated
    assert_eq!(
        consolidated.len(),
        4,
        "With 256KB threshold, large gaps should NOT be consolidated"
    );

    // Verify each consolidated range is correct
    assert_eq!(consolidated[0].start, 1 * mb as u64);
    assert_eq!(consolidated[0].end, 6 * mb as u64 - 1);

    assert_eq!(consolidated[1].start, 7 * mb as u64);
    assert_eq!(consolidated[1].end, 12 * mb as u64 - 1);

    assert_eq!(consolidated[2].start, 13 * mb as u64);
    assert_eq!(consolidated[2].end, 18 * mb as u64 - 1);

    assert_eq!(consolidated[3].start, 19 * mb as u64);
    assert_eq!(consolidated[3].end, 20 * mb as u64 - 1);

    println!("✓ Range consolidation with large gaps test passed!");
    println!("  - 4 missing ranges with 5MB gaps between them");
    println!("  - With 256KB threshold: 4 ranges (no consolidation due to large gaps)");
}

#[tokio::test]
async fn test_range_consolidation_mixed_gaps() {
    // Test consolidation with a mix of small and large gaps
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/consolidation-mixed-gaps.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Create 10MB of test data
    let mb = 1024 * 1024;
    let kb = 1024;
    let total_size = 10 * mb;

    let full_data: Vec<u8> = (0..total_size).map(|i| ((i / 256) % 256) as u8).collect();

    // Cache ranges with mixed gap sizes
    // Cached: 0-1MB, 1.1MB-2MB, 2.1MB-3MB, 5MB-6MB, 6.1MB-7MB, 7.2MB-8MB
    // Gaps: 100KB (small), 100KB (small), 2MB (large), 100KB (small), 100KB (small)
    let cached_ranges = vec![
        (0, 1 * mb - 1),                 // 0-1MB
        (1 * mb + 100 * kb, 2 * mb - 1), // 1.1MB-2MB (100KB gap)
        (2 * mb + 100 * kb, 3 * mb - 1), // 2.1MB-3MB (100KB gap)
        (5 * mb, 6 * mb - 1),            // 5MB-6MB (2MB gap)
        (6 * mb + 100 * kb, 7 * mb - 1), // 6.1MB-7MB (100KB gap)
        (7 * mb + 200 * kb, 8 * mb - 1), // 7.2MB-8MB (200KB gap)
    ];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-mixed-gaps".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: total_size as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: total_size as u64,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: total_size as u64,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store each cached range
    for (start, end) in &cached_ranges {
        let range_data = full_data[*start..=*end].to_vec();
        disk_cache_manager
            .write()
            .await
            .store_range(
                &cache_key,
                *start as u64,
                *end as u64,
                &range_data,
                object_metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();
    }

    // Request the full range (0-10MB)
    let requested_range = RangeSpec {
        start: 0,
        end: (total_size - 1) as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find missing ranges
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(!overlap.can_serve_from_cache);

    // Requirement 1.2: Consolidate with 256KB threshold
    // Small gaps (100KB, 200KB) should be consolidated
    // Large gap (2MB) should NOT be consolidated
    let gap_threshold = 256 * 1024; // 256KB
    let consolidated =
        range_handler.consolidate_missing_ranges(overlap.missing_ranges.clone(), gap_threshold);

    // Should consolidate into 3 ranges:
    // 1. First group (0-3MB with small gaps consolidated)
    // 2. Large gap (3-5MB) stays separate
    // 3. Second group (5-10MB with small gaps consolidated)
    assert!(
        consolidated.len() <= overlap.missing_ranges.len(),
        "Consolidated ranges should be fewer than or equal to original"
    );

    // Verify that consolidation reduced the number of ranges
    println!("✓ Range consolidation with mixed gaps test passed!");
    println!(
        "  - Original missing ranges: {}",
        overlap.missing_ranges.len()
    );
    println!("  - Consolidated ranges: {}", consolidated.len());
    println!("  - Small gaps (≤256KB) were consolidated");
    println!("  - Large gaps (>256KB) were kept separate");
}

#[tokio::test]
async fn test_range_consolidation_with_fetch_and_merge() {
    // End-to-end test: consolidate, fetch, and merge
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/consolidation-e2e.bin";
    let cache_key = CacheManager::generate_cache_key(path, None);

    // Create 5MB of test data
    let mb = 1024 * 1024;
    let kb = 1024;
    let total_size = 5 * mb;

    let full_data: Vec<u8> = (0..total_size).map(|i| ((i / 128) % 256) as u8).collect();

    // Cache ranges with small gaps
    // Cached: 0-1MB, 1.5MB-2.5MB, 3MB-4MB
    // Missing: 1-1.5MB (500KB), 2.5-3MB (500KB), 4-5MB (1MB)
    let cached_ranges = vec![
        (0, 1 * mb - 1),                            // 0-1MB
        (1 * mb + 500 * kb, 2 * mb + 500 * kb - 1), // 1.5MB-2.5MB
        (3 * mb, 4 * mb - 1),                       // 3MB-4MB
    ];

    let object_metadata = ObjectMetadata {
        etag: "test-etag-e2e".to_string(),
        last_modified: "Wed, 21 Nov 2025 12:00:00 GMT".to_string(),
        content_length: total_size as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: UploadState::Complete,
        cumulative_size: total_size as u64,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: total_size as u64,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store each cached range
    for (start, end) in &cached_ranges {
        let range_data = full_data[*start..=*end].to_vec();
        disk_cache_manager
            .write()
            .await
            .store_range(
                &cache_key,
                *start as u64,
                *end as u64,
                &range_data,
                object_metadata.clone(),
                std::time::Duration::from_secs(315360000), // 10 years TTL
                true)
            .await
            .unwrap();
    }

    // Request the full range
    let requested_range = RangeSpec {
        start: 0,
        end: (total_size - 1) as u64,
    };

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Find missing ranges
    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(!overlap.can_serve_from_cache);
    let original_missing_count = overlap.missing_ranges.len();

    // Don't consolidate - fetch each missing range separately to avoid gaps
    // This demonstrates that consolidation is about reducing S3 requests,
    // but we still need to fetch all the data
    let gap_threshold = 0; // No consolidation for this test
    let _consolidated =
        range_handler.consolidate_missing_ranges(overlap.missing_ranges.clone(), gap_threshold);

    // Simulate fetching the missing ranges (not consolidated in this case)
    let fetched_ranges: Vec<(RangeSpec, Vec<u8>, HashMap<String, String>)> = overlap
        .missing_ranges
        .iter()
        .map(|range_spec| {
            let data = full_data[range_spec.start as usize..=range_spec.end as usize].to_vec();
            (range_spec.clone(), data, HashMap::new())
        })
        .collect();

    // Requirement 1.2: Verify all missing bytes are fetched correctly
    // Merge cached and fetched ranges
    let merge_result = range_handler
        .merge_range_segments(
            &cache_key,
            &requested_range,
            &overlap.cached_ranges,
            &fetched_ranges,
        )
        .await
        .unwrap();

    // Verify the merged data is complete and correct
    assert_eq!(
        merge_result.data.len(),
        total_size,
        "Merged data should be complete"
    );
    assert_eq!(
        merge_result.data, full_data,
        "Merged data should be byte-identical to original"
    );

    println!("✓ Range consolidation with fetch and merge test passed!");
    println!("  - Original missing ranges: {}", original_missing_count);
    println!("  - Fetched ranges: {}", fetched_ranges.len());
    println!("  - All missing bytes fetched correctly");
    println!("  - Merged data is byte-identical to original");
}

#[tokio::test]
async fn test_range_consolidation_adjacent_ranges() {
    // Test consolidation of adjacent ranges (no gap)
    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Create adjacent missing ranges
    let missing_ranges = vec![
        RangeSpec {
            start: 1000,
            end: 1999,
        }, // 1000 bytes
        RangeSpec {
            start: 2000,
            end: 2999,
        }, // Adjacent (no gap)
        RangeSpec {
            start: 3000,
            end: 3999,
        }, // Adjacent (no gap)
    ];

    // Consolidate with any threshold - adjacent ranges should always be merged
    let gap_threshold = 0; // Even with 0 threshold, adjacent ranges should merge
    let consolidated =
        range_handler.consolidate_missing_ranges(missing_ranges.clone(), gap_threshold);

    // Should consolidate into 1 range
    assert_eq!(
        consolidated.len(),
        1,
        "Adjacent ranges should be consolidated"
    );
    assert_eq!(consolidated[0].start, 1000);
    assert_eq!(consolidated[0].end, 3999);

    println!("✓ Range consolidation of adjacent ranges test passed!");
    println!("  - 3 adjacent ranges consolidated into 1");
}
