// Feature: throughput-optimization, Property 1: Streaming decompression produces byte-identical output
//
// For any valid LZ4-compressed range file and for any chunk size, concatenating all chunks
// yielded by `stream_range_data` must produce a byte sequence identical to the output of
// `decompress_with_algorithm` applied to the same compressed file.
//
// **Validates: Requirements 1.1, 1.2, 3.2**

use futures::StreamExt;
use quickcheck::{quickcheck, TestResult};
use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::{CompressionAlgorithm, CompressionHandler};
use s3_proxy::disk_cache::DiskCacheManager;

/// Property 1: Streaming decompression produces byte-identical output
///
/// Generate random byte vectors (1 byte to 10 MiB), compress with compress_with_algorithm,
/// write compressed data to a temp file in the cache ranges directory, create a RangeSpec,
/// then compare concatenated stream_range_data output against decompress_with_algorithm output.
/// Chunk sizes vary from 1 KiB to 2 MiB.
#[test]
fn prop_streaming_decompression_byte_identity() {
    fn property(data_seed: Vec<u8>, chunk_seed: u16) -> TestResult {
        // Discard empty inputs — we need at least 1 byte
        if data_seed.is_empty() {
            return TestResult::discard();
        }

        // Scale data: use data_seed directly but cap at 10 MiB.
        // quickcheck generates Vec<u8> up to ~100 elements by default,
        // so we repeat the seed to get meaningful sizes up to 10 MiB.
        let target_size = if data_seed.len() <= 64 {
            // Small seed: use as-is for small data tests (1 byte to ~64 bytes)
            data_seed.len()
        } else {
            // Larger seed: scale up by repeating, capped at 10 MiB
            let scale = (data_seed.len() * 256).min(10 * 1024 * 1024);
            scale
        };

        let original_data: Vec<u8> = data_seed
            .iter()
            .cycle()
            .take(target_size)
            .copied()
            .collect();

        // Vary chunk_size from 1 KiB to 2 MiB based on chunk_seed
        let chunk_size = 1024 + ((chunk_seed as usize) % (2 * 1024 * 1024 - 1024 + 1));

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let cache_manager =
                DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false);
            cache_manager.initialize().await.unwrap();

            // Compress the data with LZ4
            let mut compression_handler = CompressionHandler::new(0, true);
            let compression_result = compression_handler
                .compress_with_algorithm(&original_data, CompressionAlgorithm::Lz4)
                .unwrap();
            let compressed_data = &compression_result.data;

            // Write compressed data to the cache ranges directory
            let file_path = "test-bucket/prop-test-object_0-end.bin";
            let range_file_path = temp_dir.path().join("ranges").join(file_path);
            if let Some(parent) = range_file_path.parent() {
                tokio::fs::create_dir_all(parent).await.unwrap();
            }
            tokio::fs::write(&range_file_path, compressed_data)
                .await
                .unwrap();

            // Create a RangeSpec pointing to the file
            let range_spec = RangeSpec::new(
                0,
                original_data.len() as u64 - 1,
                file_path.to_string(),
                CompressionAlgorithm::Lz4,
                compressed_data.len() as u64,
                original_data.len() as u64,
            );

            // Get the reference output via decompress_with_algorithm
            let reference_output = compression_handler
                .decompress_with_algorithm(compressed_data, CompressionAlgorithm::Lz4)
                .unwrap();

            // Get the streaming output via stream_range_data
            let stream = cache_manager
                .stream_range_data(&range_spec, chunk_size)
                .await
                .unwrap();

            let mut streamed_data = Vec::new();
            let mut stream = std::pin::pin!(stream);

            while let Some(result) = stream.next().await {
                match result {
                    Ok(bytes) => streamed_data.extend_from_slice(&bytes),
                    Err(e) => {
                        return TestResult::error(format!(
                            "stream_range_data yielded error: {}",
                            e
                        ));
                    }
                }
            }

            // Property: streaming output must be byte-identical to decompress_with_algorithm output
            if streamed_data != reference_output {
                return TestResult::error(format!(
                    "Byte mismatch: streamed {} bytes vs reference {} bytes (original {} bytes, chunk_size {})",
                    streamed_data.len(),
                    reference_output.len(),
                    original_data.len(),
                    chunk_size
                ));
            }

            // Also verify both match the original data
            if streamed_data != original_data {
                return TestResult::error(format!(
                    "Streamed data does not match original: streamed {} bytes vs original {} bytes",
                    streamed_data.len(),
                    original_data.len()
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(Vec<u8>, u16) -> TestResult);
}
