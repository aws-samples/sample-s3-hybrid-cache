use futures::StreamExt;
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use tempfile::TempDir;

/// Integration test: store an 8 MiB range on disk, stream it back in 512 KiB chunks,
/// verify all bytes match (Requirement 5.1, 5.4).
#[tokio::test]
async fn test_stream_range_data_8mib_uncompressed() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/large-object.bin";
    let range_size: usize = 8 * 1024 * 1024; // 8 MiB

    // Generate deterministic test data (repeating pattern)
    let test_data: Vec<u8> = (0..range_size).map(|i| (i % 251) as u8).collect();

    let metadata = ObjectMetadata {
        etag: "etag-large-object".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: s3_proxy::cache_types::UploadState::Complete,
        cumulative_size: test_data.len() as u64,
        parts: Vec::new(),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store the 8 MiB range on disk
    cache_manager
        .store_range(
            cache_key,
            0,
            range_size as u64 - 1,
            &test_data,
            metadata,
            std::time::Duration::from_secs(3600), true)
        .await
        .unwrap();

    // Get metadata to find the RangeSpec
    let cached_metadata = cache_manager.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(cached_metadata.ranges.len(), 1);
    let range_spec = &cached_metadata.ranges[0];
    assert_eq!(range_spec.start, 0);
    assert_eq!(range_spec.end, range_size as u64 - 1);

    // Stream the range data in 512 KiB chunks (default chunk size)
    let chunk_size: usize = 524_288; // 512 KiB
    let stream = cache_manager
        .stream_range_data(range_spec, chunk_size)
        .await
        .unwrap();

    // Collect all streamed chunks
    let mut collected_data = Vec::new();
    let mut chunk_count = 0;
    let mut stream = std::pin::pin!(stream);

    while let Some(result) = stream.next().await {
        let bytes = result.unwrap();
        assert!(
            bytes.len() <= chunk_size,
            "Chunk {} exceeds chunk_size: {} > {}",
            chunk_count,
            bytes.len(),
            chunk_size
        );
        collected_data.extend_from_slice(&bytes);
        chunk_count += 1;
    }

    // Verify total data matches
    assert_eq!(
        collected_data.len(),
        test_data.len(),
        "Streamed data length mismatch: got {} expected {}",
        collected_data.len(),
        test_data.len()
    );
    assert_eq!(
        collected_data, test_data,
        "Streamed data content does not match original"
    );

    // Verify chunk count: 8 MiB / 512 KiB = 16 chunks
    let expected_chunks = (range_size + chunk_size - 1) / chunk_size;
    assert_eq!(
        chunk_count, expected_chunks,
        "Expected {} chunks, got {}",
        expected_chunks, chunk_count
    );
}

/// Integration test: store a compressed range, stream it, verify decompressed bytes match.
#[tokio::test]
async fn test_stream_range_data_compressed() {
    let temp_dir = TempDir::new().unwrap();
    let mut cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, true); // compression enabled
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/compressed-object.txt";
    let range_size: usize = 2 * 1024 * 1024; // 2 MiB

    // Generate compressible test data (repeated text pattern)
    let pattern = b"Hello, this is a test pattern for LZ4 compression. ";
    let test_data: Vec<u8> = pattern.iter().cycle().take(range_size).copied().collect();

    let metadata = ObjectMetadata {
        etag: "etag-compressed".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        content_type: Some("text/plain".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: s3_proxy::cache_types::UploadState::Complete,
        cumulative_size: test_data.len() as u64,
        parts: Vec::new(),
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
    };

    // Store the range (compression enabled, so it should be compressed on disk)
    cache_manager
        .store_range(
            cache_key,
            0,
            range_size as u64 - 1,
            &test_data,
            metadata,
            std::time::Duration::from_secs(3600), true)
        .await
        .unwrap();

    // Get metadata to find the RangeSpec
    let cached_metadata = cache_manager.get_metadata(cache_key).await.unwrap().unwrap();
    assert_eq!(cached_metadata.ranges.len(), 1);
    let range_spec = &cached_metadata.ranges[0];

    // Stream the range data (should decompress automatically)
    let chunk_size: usize = 524_288; // 512 KiB
    let stream = cache_manager
        .stream_range_data(range_spec, chunk_size)
        .await
        .unwrap();

    // Collect all streamed chunks
    let mut collected_data = Vec::new();
    let mut stream = std::pin::pin!(stream);

    while let Some(result) = stream.next().await {
        let bytes = result.unwrap();
        collected_data.extend_from_slice(&bytes);
    }

    // Verify decompressed data matches original
    assert_eq!(
        collected_data.len(),
        test_data.len(),
        "Decompressed streamed data length mismatch"
    );
    assert_eq!(
        collected_data, test_data,
        "Decompressed streamed data content does not match original"
    );
}

/// Integration test: verify streaming handles missing range file gracefully.
#[tokio::test]
async fn test_stream_range_data_missing_file() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
    cache_manager.initialize().await.unwrap();

    // Create a fake RangeSpec pointing to a non-existent file
    let range_spec = s3_proxy::cache_types::RangeSpec::new(
        0,
        1023,
        "nonexistent/file.bin".to_string(),
        CompressionAlgorithm::Lz4,
        1024,
        1024,
    );

    // stream_range_data should return an error for missing files
    let result = cache_manager.stream_range_data(&range_spec, 524_288).await;
    assert!(result.is_err(), "Expected error for missing range file");
}
