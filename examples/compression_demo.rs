//! Compression Demo
//!
//! Demonstrates the built-in extension denylist (the *default* layer of the
//! compression decision) and the store-mode frame mechanism that keeps
//! denylisted/skipped writes checksummed even though the LZ4 block
//! compressor never runs for them.
//!
//! Reworked for the compression-content-aware-fix change: the compression
//! decision is made by the caller (in production,
//! `CacheManager::effective_compression`, which layers per-key
//! `cache_rules.json` overrides on top of this denylist) and passed into
//! `CompressionHandler::compress_with_metadata`. `should_compress_content`,
//! `compress_data_content_aware_with_fallback`, and friends were removed as
//! dead code — they had no production callers.

use s3_proxy::compression::CompressionHandler;

fn main() {
    const THRESHOLD: usize = 100; // byte threshold used for the demo decision
    let mut handler = CompressionHandler::new(THRESHOLD, true); // compression enabled

    // Sample data that compresses well
    let sample_data = "This is some sample text data that should compress well with LZ4 because it has repeating patterns and is longer than our threshold. ".repeat(5);
    let data_bytes = sample_data.as_bytes();

    println!("Content-Aware Compression Demo");
    println!("==============================");
    println!("Sample data size: {} bytes", data_bytes.len());
    println!();

    // Test different file types
    let test_files = vec![
        ("config.json", "JSON configuration file - should compress"),
        ("style.css", "CSS stylesheet - should compress"),
        ("script.js", "JavaScript file - should compress"),
        ("data.xml", "XML data file - should compress"),
        ("readme.txt", "Text file - should compress"),
        (
            "photo.jpg",
            "JPEG image - should NOT compress (built-in denylist)",
        ),
        (
            "video.mp4",
            "MP4 video - should NOT compress (built-in denylist)",
        ),
        (
            "archive.zip",
            "ZIP archive - should NOT compress (built-in denylist)",
        ),
        (
            "document.pdf",
            "PDF document - should NOT compress (built-in denylist)",
        ),
        (
            "music.mp3",
            "MP3 audio - should NOT compress (built-in denylist)",
        ),
    ];

    for (filename, description) in test_files {
        // This mirrors the default layer of CacheManager::effective_compression:
        // enabled + threshold, then the built-in denylist when no cache rule
        // explicitly overrides compression_enabled for this key.
        let is_denylisted = CompressionHandler::is_denylisted_extension(filename);
        let should_compress =
            handler.is_compression_enabled() && data_bytes.len() >= THRESHOLD && !is_denylisted;
        let result = handler.compress_with_metadata(data_bytes, filename, should_compress);

        let compression_ratio = if result.was_compressed {
            result.compressed_size as f32 / result.original_size as f32
        } else {
            1.0
        };

        println!("File: {}", filename);
        println!("  Description: {}", description);
        println!("  Denylisted extension: {}", is_denylisted);
        println!("  Should compress: {}", should_compress);
        println!(
            "  Was compressed (LZ4 block compressor ran): {}",
            result.was_compressed
        );
        println!(
            "  Size: {} -> {} bytes",
            result.original_size, result.compressed_size
        );
        println!("  Compression ratio: {:.2}", compression_ratio);
        println!(
            "  Stored algorithm tag: {:?} (store-mode frames are tagged Lz4 too — see docs/COMPRESSION.md)",
            result.algorithm
        );
        println!();
    }

    // Show statistics (live across handler clones — see compression-content-aware-fix spec)
    let stats = handler.get_stats();
    println!("Compression Statistics:");
    println!("======================");
    println!("Objects compressed: {}", stats.total_objects_compressed);
    println!(
        "Objects uncompressed (store-mode): {}",
        stats.total_objects_uncompressed
    );
    println!("Total bytes before: {}", stats.total_bytes_before);
    println!("Total bytes after: {}", stats.total_bytes_after);
    println!(
        "Average compression ratio: {:.2}",
        stats.average_compression_ratio
    );
    println!("Compression failures: {}", stats.compression_failures);
    println!("Decompression failures: {}", stats.decompression_failures);
    println!();
    println!(
        "Denylisted extensions are detected via CompressionHandler::is_denylisted_extension \
         (see the per-file output above). Override with a cache_rules.json rule setting \
         compression_enabled explicitly."
    );
}
