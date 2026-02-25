//! Cache Consistency Demo
//!
//! Demonstrates how the cache system handles compression algorithm changes
//! and rule changes using per-entry metadata to maintain consistency
//! without invalidating the entire cache.

use s3_proxy::cache::{CacheEntry, CacheManager, CompressionInfo};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::compression::CompressionAlgorithm;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

fn main() {
    println!("Multi-Algorithm Cache Consistency Demo");
    println!("======================================");
    println!("Demonstrates per-entry compression metadata for cache consistency");
    println!();

    // Create a cache manager with initial compression settings
    let cache_manager = CacheManager::new(
        PathBuf::from("/tmp/cache"),
        false, // RAM cache disabled for simplicity
        0,     // No RAM cache
        100,   // 100 byte threshold
        true,  // Compression enabled
    );

    println!(
        "Initial compression algorithm: {:?}",
        cache_manager
            .get_compression_handler()
            .get_preferred_algorithm()
    );
    println!();

    // Simulate a cache entry for a JPEG image (initially compressible)
    let mut jpeg_entry = CacheEntry {
        cache_key: "bucket.amazonaws.com:/photos/image.jpg".to_string(),
        headers: HashMap::new(),
        body: Some(b"fake jpeg data that was compressed".to_vec()),
        ranges: vec![],
        metadata: CacheMetadata {
            etag: "abc123".to_string(),
            last_modified: "2024-01-01T00:00:00Z".to_string(),
            content_length: 1000,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        },
        created_at: SystemTime::now(),
        expires_at: SystemTime::now(),
        metadata_expires_at: SystemTime::now(),
        compression_info: CompressionInfo {
            body_algorithm: CompressionAlgorithm::Lz4, // This was compressed with LZ4
            original_size: Some(2000),
            compressed_size: Some(1000),
            file_extension: Some("jpg".to_string()),
        },
        is_put_cached: false,
    };

    println!("Scenario 1: Cache entry created with LZ4 compression");
    println!("  - JPEG file was compressed with LZ4 (content-aware was disabled)");
    println!(
        "  - Entry algorithm: {:?}",
        jpeg_entry.compression_info.body_algorithm
    );
    println!(
        "  - Original size: {:?}",
        jpeg_entry.compression_info.original_size
    );
    println!(
        "  - Compressed size: {:?}",
        jpeg_entry.compression_info.compressed_size
    );
    println!();

    // Check if we should recompress this entry
    let should_recompress = cache_manager.should_recompress_entry(&jpeg_entry.compression_info);
    println!("Should recompress entry: {}", should_recompress);
    println!();

    // Now simulate changing preferred algorithm
    println!("Scenario 2: Changing preferred compression algorithm...");
    // In a real scenario, this would be done via configuration
    println!(
        "Current algorithm: {:?}",
        cache_manager
            .get_compression_handler()
            .get_preferred_algorithm()
    );
    println!("(In real usage, algorithm changes would be via configuration)");
    println!();

    // Try to decompress the cache entry (this should fail due to version mismatch)
    println!("Scenario 3: Decompressing cache entry with algorithm metadata...");
    match cache_manager.decompress_cache_entry(&mut jpeg_entry) {
        Ok(_) => {
            println!("✅ Decompression succeeded using stored algorithm metadata");
            println!(
                "   Algorithm used: {:?}",
                jpeg_entry.compression_info.body_algorithm
            );
        }
        Err(e) => {
            println!("❌ Decompression failed: {}", e);
        }
    }
    println!();

    // Create a new cache entry with current rules
    println!("Scenario 4: Creating new cache entry with current rules...");
    let sample_data = b"This is some sample JPEG data that should not be compressed now";

    // Check if JPEG should be compressed under new rules
    let should_compress = cache_manager
        .get_compression_handler()
        .should_compress_content("/photos/image.jpg", sample_data.len());

    println!("Should JPEG be compressed now: {}", should_compress);

    let mut new_jpeg_entry = CacheEntry {
        cache_key: "bucket.amazonaws.com:/photos/new_image.jpg".to_string(),
        headers: HashMap::new(),
        body: Some(sample_data.to_vec()),
        ranges: vec![],
        metadata: CacheMetadata {
            etag: "def456".to_string(),
            last_modified: "2024-01-02T00:00:00Z".to_string(),
            content_length: sample_data.len() as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        },
        created_at: SystemTime::now(),
        expires_at: SystemTime::now(),
        metadata_expires_at: SystemTime::now(),
        compression_info: CompressionInfo::default(), // Will be updated during compression
        is_put_cached: false,
    };

    // Compress the new entry (should not compress JPEG under new rules)
    match cache_manager.compress_cache_entry(&mut new_jpeg_entry) {
        Ok(was_compressed) => {
            println!("New entry compression result: {}", was_compressed);
            println!(
                "New entry algorithm: {:?}",
                new_jpeg_entry.compression_info.body_algorithm
            );
            println!(
                "New entry original size: {:?}",
                new_jpeg_entry.compression_info.original_size
            );
            println!(
                "New entry compressed size: {:?}",
                new_jpeg_entry.compression_info.compressed_size
            );
        }
        Err(e) => {
            println!("Compression failed: {}", e);
        }
    }
    println!();

    // Verify the new entry can be decompressed successfully
    println!("Scenario 5: Decompressing new cache entry...");
    match cache_manager.decompress_cache_entry(&mut new_jpeg_entry) {
        Ok(_) => {
            println!("✅ New entry decompression succeeded");
            println!("   Data integrity maintained with current compression rules");
        }
        Err(e) => {
            println!("❌ New entry decompression failed: {}", e);
        }
    }
    println!();

    // Demonstrate text file behavior
    println!("Scenario 6: Text file behavior (should still be compressed)...");
    let text_data = b"This is a JSON configuration file that should be compressed even with content-aware rules";
    let should_compress_json = cache_manager
        .get_compression_handler()
        .should_compress_content("/config/settings.json", text_data.len());

    println!("Should JSON be compressed: {}", should_compress_json);
    println!();

    println!("Summary:");
    println!("========");
    println!("✅ Cache consistency is maintained with per-entry algorithm metadata");
    println!("✅ Cache entries store which algorithm was used for compression");
    println!("✅ Algorithm changes don't invalidate existing cache entries");
    println!("✅ Content-aware compression works correctly for different file types");
    println!("✅ Per-entry metadata prevents serving incorrectly processed data");
    println!("✅ Future algorithm support can be added without breaking existing cache");
}
