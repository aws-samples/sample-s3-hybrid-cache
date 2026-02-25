//! Compression Demo
//!
//! Demonstrates the content-aware compression functionality

use s3_proxy::compression::CompressionHandler;

fn main() {
    let mut handler = CompressionHandler::new(100, true); // 100 byte threshold, compression enabled

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
            "JPEG image - should NOT compress (already compressed)",
        ),
        (
            "video.mp4",
            "MP4 video - should NOT compress (already compressed)",
        ),
        (
            "archive.zip",
            "ZIP archive - should NOT compress (already compressed)",
        ),
        (
            "document.pdf",
            "PDF document - should NOT compress (already compressed)",
        ),
        (
            "music.mp3",
            "MP3 audio - should NOT compress (already compressed)",
        ),
    ];

    for (filename, description) in test_files {
        let should_compress = handler.should_compress_content(filename, data_bytes.len());
        let (compressed_data, was_compressed) =
            handler.compress_data_content_aware_with_fallback(data_bytes, filename);

        let compression_ratio = if was_compressed {
            compressed_data.len() as f32 / data_bytes.len() as f32
        } else {
            1.0
        };

        println!("File: {}", filename);
        println!("  Description: {}", description);
        println!("  Should compress: {}", should_compress);
        println!("  Was compressed: {}", was_compressed);
        println!(
            "  Size: {} -> {} bytes",
            data_bytes.len(),
            compressed_data.len()
        );
        println!("  Compression ratio: {:.2}", compression_ratio);
        println!();
    }

    // Show statistics
    let stats = handler.get_stats();
    println!("Compression Statistics:");
    println!("======================");
    println!("Objects compressed: {}", stats.total_objects_compressed);
    println!("Objects uncompressed: {}", stats.total_objects_uncompressed);
    println!("Total bytes before: {}", stats.total_bytes_before);
    println!("Total bytes after: {}", stats.total_bytes_after);
    println!(
        "Average compression ratio: {:.2}",
        stats.average_compression_ratio
    );
    println!("Compression failures: {}", stats.compression_failures);

    // Show list of skipped extensions
    println!();
    println!("File extensions that skip compression:");
    println!("=====================================");
    let skipped_extensions = CompressionHandler::get_skipped_extensions();
    for (i, ext) in skipped_extensions.iter().enumerate() {
        print!("{}", ext);
        if i < skipped_extensions.len() - 1 {
            print!(", ");
        }
        if (i + 1) % 10 == 0 {
            println!();
        }
    }
    println!();
}
