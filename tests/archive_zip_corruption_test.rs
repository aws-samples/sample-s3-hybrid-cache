//! Integration test for Archive.zip corruption bug
//!
//! This test reproduces and validates the fix for the bug where cached range data
//! is not properly sliced to match the exact client-requested byte range, causing
//! file corruption and AWS SDK errors like "AWS_ERROR_DEST_COPY_TOO_SMALL".
//!
//! Test scenario:
//! 1. Upload a 50MB+ file via multipart upload (simulating Archive.zip)
//! 2. Download the file through the proxy using range requests
//! 3. Verify downloaded file matches original (checksum comparison)
//! 4. Verify no "DEST_COPY_TOO_SMALL" errors occur
//!
//! Requirements tested:
//! - Requirement 4.1: AWS SDK requests return exactly the requested range
//! - Requirement 4.2: Content-Range end byte matches the request
//! - Requirement 4.3: Content-Length validation passes
//! - Requirement 4.4: Multiple range requests return correct byte counts

use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::UploadState;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use sha2::{Digest, Sha256};
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

    // Must call create_configured_disk_cache_manager() to set up the JournalConsolidator
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));

    (cache_manager, disk_cache_manager, temp_dir)
}

/// Generate deterministic test data for a given part number
fn generate_part_data(part_number: u32, size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| ((part_number * 17 + (i / 1024) as u32) % 256) as u8)
        .collect()
}

/// Calculate SHA256 checksum of data
fn calculate_checksum(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[tokio::test]
async fn test_archive_zip_corruption_bug_multipart_upload_and_download() {
    // This test reproduces the exact scenario that caused the Archive.zip corruption:
    // 1. Upload a 50MB file via multipart (7 parts of ~8MB each)
    // 2. Download using range requests (simulating AWS SDK behavior)
    // 3. Verify no corruption occurs

    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/Archive.zip";

    // Create 50MB file with 7 parts (simulating typical multipart upload)
    // Standard part size is 8 MiB (8,388,608 bytes)
    let standard_part_size = 8 * 1024 * 1024; // 8 MiB
    let num_parts = 7;
    let total_size = standard_part_size * num_parts;

    println!("Creating 50MB test file with {} parts", num_parts);

    // Generate full data for checksum verification
    let mut full_data = Vec::with_capacity(total_size);
    let mut parts = Vec::new();

    for part_num in 1..=num_parts {
        let part_data = generate_part_data(part_num as u32, standard_part_size);
        full_data.extend_from_slice(&part_data);
        parts.push((part_num, part_data));
    }

    assert_eq!(full_data.len(), total_size);
    let original_checksum = calculate_checksum(&full_data);
    println!("Original file checksum: {}", original_checksum);

    // Step 1: Simulate multipart upload
    println!("Step 1: Uploading file via multipart upload");
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    for (part_num, part_data) in &parts {
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, *part_num as u32, part_data, etag)
            .await
            .unwrap();
        println!("  Uploaded part {}: {} bytes", part_num, part_data.len());
    }

    cache_manager.complete_multipart_upload(path).await.unwrap();
    println!("  Multipart upload completed");

    // Verify upload is complete
    let cache_key = CacheManager::generate_cache_key(path, None);
    let metadata = cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(metadata.object_metadata.upload_state, UploadState::Complete);
    assert_eq!(metadata.object_metadata.content_length, total_size as u64);
    println!(
        "  Verified metadata: {} ranges cached",
        metadata.ranges.len()
    );

    // Step 2: Download file using range requests (simulating AWS SDK behavior)
    println!("\nStep 2: Downloading file using range requests");

    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Download in chunks (simulating AWS SDK multipart download)
    // AWS SDK typically requests exact part boundaries
    let mut downloaded_data = Vec::new();
    let chunk_size = standard_part_size;

    for chunk_num in 0..num_parts {
        let start = (chunk_num * chunk_size) as u64;
        let end = ((chunk_num + 1) * chunk_size - 1) as u64;

        let requested_range = RangeSpec { start, end };

        println!(
            "  Requesting range: bytes={}-{} ({} bytes)",
            start,
            end,
            end - start + 1
        );

        // Find cached ranges
        let overlap = range_handler
            .find_cached_ranges(&cache_key, &requested_range, None, None)
            .await
            .unwrap();

        // Requirement 4.1: Verify we can serve from cache
        assert!(
            overlap.can_serve_from_cache,
            "Should be able to serve range {}-{} from cache",
            start, end
        );
        assert!(
            overlap.missing_ranges.is_empty(),
            "Should have no missing ranges for {}-{}",
            start,
            end
        );

        // Merge the cached ranges
        let merge_result = range_handler
            .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
            .await
            .unwrap();

        // Requirement 4.3: Verify Content-Length matches requested range
        let expected_size = (end - start + 1) as usize;
        assert_eq!(
            merge_result.data.len(),
            expected_size,
            "Content-Length mismatch for range {}-{}: expected {} bytes, got {} bytes",
            start,
            end,
            expected_size,
            merge_result.data.len()
        );

        // Requirement 4.4: Verify correct byte count returned
        assert_eq!(
            merge_result.bytes_from_cache, expected_size as u64,
            "Bytes from cache mismatch for range {}-{}",
            start, end
        );

        println!(
            "    Downloaded {} bytes (cache efficiency: {:.1}%)",
            merge_result.data.len(),
            merge_result.cache_efficiency
        );

        downloaded_data.extend_from_slice(&merge_result.data);
    }

    // Step 3: Verify downloaded file matches original (checksum comparison)
    println!("\nStep 3: Verifying file integrity");

    assert_eq!(
        downloaded_data.len(),
        full_data.len(),
        "Downloaded file size mismatch"
    );

    let downloaded_checksum = calculate_checksum(&downloaded_data);
    println!("Downloaded file checksum: {}", downloaded_checksum);

    // Requirement 4.1, 4.2, 4.3, 4.4: Verify no corruption occurred
    assert_eq!(
        downloaded_checksum, original_checksum,
        "File corruption detected! Checksums do not match"
    );

    // Verify byte-by-byte equality
    assert_eq!(
        downloaded_data, full_data,
        "Downloaded data does not match original data"
    );

    println!("✓ File integrity verified - no corruption detected");
    println!("✓ All range requests returned correct byte counts");
    println!("✓ No DEST_COPY_TOO_SMALL errors would occur");
}

#[tokio::test]
async fn test_non_aligned_range_requests_after_multipart_upload() {
    // Test downloading with non-aligned range requests (not at part boundaries)
    // This tests the range slicing logic more thoroughly

    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/non-aligned-test.bin";

    // Create 30MB file with 4 parts
    let part_size = 8 * 1024 * 1024; // 8 MiB
    let num_parts = 4;
    let total_size = part_size * num_parts;

    let mut full_data = Vec::with_capacity(total_size);

    // Upload via multipart
    cache_manager.initiate_multipart_upload(path).await.unwrap();

    for part_num in 1..=num_parts {
        let part_data = generate_part_data(part_num as u32, part_size);
        full_data.extend_from_slice(&part_data);
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, part_num as u32, &part_data, etag)
            .await
            .unwrap();
    }

    cache_manager.complete_multipart_upload(path).await.unwrap();

    let cache_key = CacheManager::generate_cache_key(path, None);
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Test various non-aligned range requests
    let test_ranges = vec![
        // Start in middle of first part
        (500 * 1024, 1024 * 1024 - 1),
        // Span multiple parts
        (7 * 1024 * 1024, 10 * 1024 * 1024 - 1),
        // End in middle of last part
        (28 * 1024 * 1024, 30 * 1024 * 1024 - 1),
        // Small range in middle
        (15 * 1024 * 1024, 15 * 1024 * 1024 + 1024 - 1),
    ];

    for (start, end) in test_ranges {
        let requested_range = RangeSpec { start, end };

        let overlap = range_handler
            .find_cached_ranges(&cache_key, &requested_range, None, None)
            .await
            .unwrap();

        assert!(
            overlap.can_serve_from_cache,
            "Should serve range {}-{} from cache",
            start, end
        );

        let merge_result = range_handler
            .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
            .await
            .unwrap();

        let expected_size = (end - start + 1) as usize;
        assert_eq!(
            merge_result.data.len(),
            expected_size,
            "Size mismatch for range {}-{}",
            start,
            end
        );

        // Verify data matches original
        let expected_data = &full_data[start as usize..=end as usize];
        assert_eq!(
            merge_result.data, expected_data,
            "Data mismatch for range {}-{}",
            start, end
        );

        println!(
            "✓ Non-aligned range {}-{} served correctly ({} bytes)",
            start, end, expected_size
        );
    }
}

#[tokio::test]
async fn test_single_byte_range_requests() {
    // Test edge case: single byte range requests
    // This is important for validating precise slicing logic

    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/single-byte-test.bin";

    // Create small file with 2 parts
    let part_size = 1024 * 1024; // 1 MiB
    let num_parts = 2;

    let mut full_data = Vec::with_capacity(part_size * num_parts);

    cache_manager.initiate_multipart_upload(path).await.unwrap();

    for part_num in 1..=num_parts {
        let part_data = generate_part_data(part_num as u32, part_size);
        full_data.extend_from_slice(&part_data);
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, part_num as u32, &part_data, etag)
            .await
            .unwrap();
    }

    cache_manager.complete_multipart_upload(path).await.unwrap();

    let cache_key = CacheManager::generate_cache_key(path, None);
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Test single byte requests at various positions
    let test_positions = vec![
        0,                          // First byte
        1024,                       // Middle of first part
        part_size as u64 - 1,       // Last byte of first part
        part_size as u64,           // First byte of second part
        (2 * part_size - 1) as u64, // Last byte of file
    ];

    for pos in test_positions {
        let requested_range = RangeSpec {
            start: pos,
            end: pos,
        };

        let overlap = range_handler
            .find_cached_ranges(&cache_key, &requested_range, None, None)
            .await
            .unwrap();

        assert!(
            overlap.can_serve_from_cache,
            "Should serve single byte at position {} from cache",
            pos
        );

        let merge_result = range_handler
            .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
            .await
            .unwrap();

        assert_eq!(
            merge_result.data.len(),
            1,
            "Should return exactly 1 byte for position {}",
            pos
        );

        // Verify byte matches original
        assert_eq!(
            merge_result.data[0], full_data[pos as usize],
            "Byte mismatch at position {}",
            pos
        );

        println!("✓ Single byte at position {} served correctly", pos);
    }
}

#[tokio::test]
async fn test_last_part_smaller_than_standard() {
    // Test scenario where the last part is smaller than standard part size
    // This is common in real multipart uploads

    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/uneven-parts.bin";

    // Create file with 3 full parts + 1 smaller last part
    let standard_part_size = 8 * 1024 * 1024; // 8 MiB
    let last_part_size = 3 * 1024 * 1024; // 3 MiB
    let total_size = standard_part_size * 3 + last_part_size;

    let mut full_data = Vec::with_capacity(total_size);

    cache_manager.initiate_multipart_upload(path).await.unwrap();

    // Upload 3 standard parts
    for part_num in 1..=3 {
        let part_data = generate_part_data(part_num, standard_part_size);
        full_data.extend_from_slice(&part_data);
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, part_num, &part_data, etag)
            .await
            .unwrap();
    }

    // Upload smaller last part
    let part_data = generate_part_data(4, last_part_size);
    full_data.extend_from_slice(&part_data);
    cache_manager
        .store_multipart_part(path, 4, &part_data, "etag-part-4".to_string())
        .await
        .unwrap();

    cache_manager.complete_multipart_upload(path).await.unwrap();

    let cache_key = CacheManager::generate_cache_key(path, None);
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // Request the entire file
    let requested_range = RangeSpec {
        start: 0,
        end: (total_size - 1) as u64,
    };

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &requested_range, None, None)
        .await
        .unwrap();

    assert!(overlap.can_serve_from_cache);

    let merge_result = range_handler
        .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    assert_eq!(merge_result.data.len(), total_size);
    assert_eq!(merge_result.data, full_data);

    // Specifically test requesting just the last part
    let last_part_start = (standard_part_size * 3) as u64;
    let last_part_end = (total_size - 1) as u64;
    let last_part_range = RangeSpec {
        start: last_part_start,
        end: last_part_end,
    };

    let overlap = range_handler
        .find_cached_ranges(&cache_key, &last_part_range, None, None)
        .await
        .unwrap();

    let merge_result = range_handler
        .merge_range_segments(&cache_key, &last_part_range, &overlap.cached_ranges, &[])
        .await
        .unwrap();

    assert_eq!(
        merge_result.data.len(),
        last_part_size,
        "Last part size mismatch"
    );

    let expected_last_part = &full_data[last_part_start as usize..];
    assert_eq!(
        merge_result.data, expected_last_part,
        "Last part data mismatch"
    );

    println!("✓ File with smaller last part handled correctly");
    println!("  Total size: {} bytes", total_size);
    println!("  Last part size: {} bytes", last_part_size);
}

#[tokio::test]
async fn test_aws_sdk_sync_simulation() {
    // Simulate AWS SDK sync operation which makes multiple concurrent range requests
    // This is the exact scenario that triggered the DEST_COPY_TOO_SMALL error

    let (cache_manager, disk_cache_manager, _temp_dir) = create_test_cache_manager();
    disk_cache_manager.write().await.initialize().await.unwrap();

    let path = "/test-bucket/aws-sdk-sync-test.bin";

    // Create 64MB file (8 parts of 8MB each)
    let part_size = 8 * 1024 * 1024;
    let num_parts = 8;
    let total_size = part_size * num_parts;

    let mut full_data = Vec::with_capacity(total_size);

    cache_manager.initiate_multipart_upload(path).await.unwrap();

    for part_num in 1..=num_parts {
        let part_data = generate_part_data(part_num as u32, part_size);
        full_data.extend_from_slice(&part_data);
        let etag = format!("etag-part-{}", part_num);
        cache_manager
            .store_multipart_part(path, part_num as u32, &part_data, etag)
            .await
            .unwrap();
    }

    cache_manager.complete_multipart_upload(path).await.unwrap();

    let cache_key = CacheManager::generate_cache_key(path, None);
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    // AWS SDK typically downloads in parallel chunks
    // Simulate concurrent range requests
    let mut handles = vec![];

    for chunk_num in 0..num_parts {
        let start = (chunk_num * part_size) as u64;
        let end = ((chunk_num + 1) * part_size - 1) as u64;

        let range_handler = range_handler.clone();
        let cache_key = cache_key.clone();
        let expected_data = full_data[start as usize..=end as usize].to_vec();

        let handle = tokio::spawn(async move {
            let requested_range = RangeSpec { start, end };

            let overlap = range_handler
                .find_cached_ranges(&cache_key, &requested_range, None, None)
                .await
                .unwrap();

            assert!(
                overlap.can_serve_from_cache,
                "Chunk {} should be servable from cache",
                chunk_num
            );

            let merge_result = range_handler
                .merge_range_segments(&cache_key, &requested_range, &overlap.cached_ranges, &[])
                .await
                .unwrap();

            let expected_size = (end - start + 1) as usize;
            assert_eq!(
                merge_result.data.len(),
                expected_size,
                "Chunk {} size mismatch",
                chunk_num
            );

            assert_eq!(
                merge_result.data, expected_data,
                "Chunk {} data mismatch",
                chunk_num
            );

            (chunk_num, merge_result.data)
        });

        handles.push(handle);
    }

    // Wait for all concurrent downloads to complete
    let mut results = vec![];
    for handle in handles {
        let (chunk_num, data) = handle.await.unwrap();
        results.push((chunk_num, data));
    }

    // Sort by chunk number and reassemble
    results.sort_by_key(|(chunk_num, _)| *chunk_num);
    let mut reassembled = Vec::new();
    for (_, data) in results {
        reassembled.extend_from_slice(&data);
    }

    // Verify reassembled file matches original
    assert_eq!(reassembled.len(), full_data.len());
    assert_eq!(reassembled, full_data);

    let original_checksum = calculate_checksum(&full_data);
    let reassembled_checksum = calculate_checksum(&reassembled);
    assert_eq!(original_checksum, reassembled_checksum);

    println!("✓ AWS SDK sync simulation completed successfully");
    println!("  Downloaded {} parts concurrently", num_parts);
    println!("  Total size: {} bytes", total_size);
    println!("  Checksum verified: {}", reassembled_checksum);
    println!("✓ No DEST_COPY_TOO_SMALL errors occurred");
}
