// Feature: dependency-security-upgrades, Task 2.1: LZ4 round-trip preservation
//
// Establishes a baseline green run on the unchanged workspace before Task 3 (lz4_flex bump).
// After the Cluster A fix lands, this test must continue to pass — any regression in the
// FrameDecoder / FrameEncoder surface will surface here before it reaches production.
//
// **Validates: Cluster A preservation (lz4_flex round-trip correctness)**

use quickcheck::{quickcheck, TestResult};
use s3_proxy::compression::CompressionHandler;

// ---------------------------------------------------------------------------
// Helper: split `data` into `n` roughly-equal chunks (at least 1 byte each).
// Used by the concatenated-frame property to simulate IncrementalRangeWriter.
// ---------------------------------------------------------------------------
fn split_into_n_chunks(data: &[u8], n: usize) -> Vec<Vec<u8>> {
    if data.is_empty() || n == 0 {
        return vec![];
    }
    let n = n.min(data.len()); // can't have more chunks than bytes
    let base = data.len() / n;
    let remainder = data.len() % n;
    let mut chunks = Vec::with_capacity(n);
    let mut offset = 0;
    for i in 0..n {
        let extra = if i < remainder { 1 } else { 0 };
        let end = offset + base + extra;
        chunks.push(data[offset..end].to_vec());
        offset = end;
    }
    chunks
}

// ---------------------------------------------------------------------------
// Property 1: decompress(compress(X)) == X for arbitrary Vec<u8> X
//
// Uses CompressionHandler::new(0, true) so the threshold is 0 — every input
// is compressed regardless of size.  Routes through compress_data /
// decompress_data, the same entry points the disk cache uses.
// ---------------------------------------------------------------------------
#[test]
fn prop_lz4_roundtrip_arbitrary_bytes() {
    fn property(data: Vec<u8>) -> TestResult {
        // Discard empty inputs — we need at least 1 byte
        if data.is_empty() {
            return TestResult::discard();
        }

        let mut handler = CompressionHandler::new(0, true);

        let compressed = match handler.compress_data(&data) {
            Ok(c) => c,
            Err(e) => return TestResult::error(format!("compress_data failed: {}", e)),
        };

        let decompressed = match handler.decompress_data(&compressed) {
            Ok(d) => d,
            Err(e) => return TestResult::error(format!("decompress_data failed: {}", e)),
        };

        if decompressed != data {
            return TestResult::error(format!(
                "Round-trip mismatch: original {} bytes, decompressed {} bytes",
                data.len(),
                decompressed.len()
            ));
        }

        TestResult::passed()
    }

    quickcheck(property as fn(Vec<u8>) -> TestResult);
}

// ---------------------------------------------------------------------------
// Property 2: Concatenated-frame round-trip (IncrementalRangeWriter framing)
//
// Simulates the path documented in compression-internals steering:
//   - Each write_range_chunk produces one complete LZ4 frame via compress_always
//   - Frames are concatenated into a single buffer
//   - decompress_data loops through frames until EOF
//
// For arbitrary Vec<u8> X and chunk count seed n_seed:
//   concat(compress_always(chunk_i) for chunk_i in split(X)) decompresses to X
// ---------------------------------------------------------------------------
#[test]
fn prop_lz4_concatenated_frames_roundtrip() {
    fn property(data: Vec<u8>, n_seed: u8) -> TestResult {
        if data.is_empty() {
            return TestResult::discard();
        }

        // Derive chunk count: 1..=8 chunks
        let n_chunks = 1 + (n_seed as usize % 8);
        let chunks = split_into_n_chunks(&data, n_chunks);

        // Compress each chunk independently (one LZ4 frame per chunk)
        let mut handler = CompressionHandler::new(0, true);
        let mut concatenated = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            match handler.compress_always(chunk) {
                Ok(frame) => concatenated.extend_from_slice(&frame),
                Err(e) => {
                    return TestResult::error(format!(
                        "compress_always failed on chunk {}: {}",
                        i, e
                    ))
                }
            }
        }

        // Decompress the concatenated buffer — must loop through all frames
        let decompressed = match handler.decompress_data(&concatenated) {
            Ok(d) => d,
            Err(e) => {
                return TestResult::error(format!(
                    "decompress_data failed on concatenated frames: {}",
                    e
                ))
            }
        };

        if decompressed != data {
            return TestResult::error(format!(
                "Concatenated-frame round-trip mismatch: original {} bytes, \
                 decompressed {} bytes, {} chunks",
                data.len(),
                decompressed.len(),
                n_chunks
            ));
        }

        TestResult::passed()
    }

    quickcheck(property as fn(Vec<u8>, u8) -> TestResult);
}

// ---------------------------------------------------------------------------
// Fixed fixtures: exercise specific size boundaries
//
// Sizes chosen to cover:
//   - 1 byte  : smallest possible input
//   - 63 bytes: just below the 64-byte threshold in compress_always (wrap_in_frame path)
//   - 64 bytes: exactly at the threshold boundary
//   - 1024 bytes: medium, well above threshold
//   - 1 MiB + 1 byte: crosses the default compression_batch_size boundary
// ---------------------------------------------------------------------------
fn roundtrip_fixture(size: usize, label: &str) {
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let mut handler = CompressionHandler::new(0, true);

    let compressed = handler
        .compress_data(&data)
        .unwrap_or_else(|e| panic!("compress_data failed for {}: {}", label, e));

    let decompressed = handler
        .decompress_data(&compressed)
        .unwrap_or_else(|e| panic!("decompress_data failed for {}: {}", label, e));

    assert_eq!(
        decompressed, data,
        "Round-trip mismatch for fixture '{}' ({} bytes)",
        label, size
    );
}

fn concatenated_frames_fixture(size: usize, n_chunks: usize, label: &str) {
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let chunks = split_into_n_chunks(&data, n_chunks);

    let mut handler = CompressionHandler::new(0, true);
    let mut concatenated = Vec::new();
    for chunk in &chunks {
        let frame = handler
            .compress_always(chunk)
            .unwrap_or_else(|e| panic!("compress_always failed for {}: {}", label, e));
        concatenated.extend_from_slice(&frame);
    }

    let decompressed = handler.decompress_data(&concatenated).unwrap_or_else(|e| {
        panic!(
            "decompress_data failed on concatenated frames for {}: {}",
            label, e
        )
    });

    assert_eq!(
        decompressed, data,
        "Concatenated-frame mismatch for fixture '{}' ({} bytes, {} chunks)",
        label, size, n_chunks
    );
}

#[test]
fn fixture_1_byte() {
    roundtrip_fixture(1, "1-byte");
}

#[test]
fn fixture_63_bytes_below_threshold() {
    roundtrip_fixture(63, "63-bytes-below-threshold");
}

#[test]
fn fixture_64_bytes_at_threshold() {
    roundtrip_fixture(64, "64-bytes-at-threshold");
}

#[test]
fn fixture_1024_bytes_medium() {
    roundtrip_fixture(1024, "1024-bytes-medium");
}

#[test]
fn fixture_1mib_plus_1_large() {
    roundtrip_fixture(1024 * 1024 + 1, "1MiB+1-large");
}

#[test]
fn fixture_concatenated_1_byte_single_chunk() {
    concatenated_frames_fixture(1, 1, "1-byte-1-chunk");
}

#[test]
fn fixture_concatenated_63_bytes_3_chunks() {
    // 63 bytes split into 3 chunks of 21 bytes each — all below the 64-byte threshold
    concatenated_frames_fixture(63, 3, "63-bytes-3-chunks");
}

#[test]
fn fixture_concatenated_1024_bytes_4_chunks() {
    concatenated_frames_fixture(1024, 4, "1024-bytes-4-chunks");
}

#[test]
fn fixture_concatenated_1mib_plus_1_8_chunks() {
    // Crosses the default compression_batch_size (1 MiB) boundary across multiple frames
    concatenated_frames_fixture(1024 * 1024 + 1, 8, "1MiB+1-8-chunks");
}
