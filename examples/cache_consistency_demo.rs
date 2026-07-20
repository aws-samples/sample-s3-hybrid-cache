//! Cache Consistency Demo
//!
//! Demonstrates how the cache system stores per-entry compression metadata
//! (`CompressionInfo::body_algorithm`) so that reads never depend on the
//! *current* compression configuration — only on what was recorded when the
//! entry was written. This is what lets denylist / threshold / cache-rule
//! changes take effect for new writes without invalidating or needing to
//! migrate existing cache entries.
//!
//! Reworked for the compression-content-aware-fix change: the compression
//! decision is now made by the caller (`CacheManager::effective_compression`,
//! combining per-key cache-rule overrides, the size threshold, and the
//! built-in extension denylist) and passed into
//! `CompressionHandler::compress_with_metadata`. There is no longer a
//! `should_recompress_entry` / `compress_cache_entry` / `decompress_cache_entry`
//! API on `CacheManager` — those were dead code with zero production callers.
//! This demo now drives `CompressionHandler` directly, which is what those
//! removed helpers did internally anyway.

use s3_proxy::compression::CompressionHandler;

fn main() {
    println!("Multi-Algorithm Cache Consistency Demo");
    println!("======================================");
    println!("Demonstrates per-entry compression metadata for cache consistency");
    println!();

    let mut handler = CompressionHandler::new(100, true); // 100-byte threshold, compression enabled

    println!(
        "Preferred algorithm: {:?}",
        handler.get_preferred_algorithm()
    );
    println!();

    // Scenario 1: a JPEG key under the built-in default denylist.
    println!("Scenario 1: JPEG key, no cache-rule override (built-in denylist applies)");
    let jpeg_data = b"fake jpeg data that will be store-mode framed, not compressed";
    // .jpg is in the built-in denylist, so the default-layer decision skips compression.
    let jpeg_should_compress = !CompressionHandler::is_denylisted_extension("photos/image.jpg");
    let jpeg_result =
        handler.compress_with_metadata(jpeg_data, "photos/image.jpg", jpeg_should_compress);
    println!(
        "  is_denylisted_extension(\"photos/image.jpg\") = {}",
        CompressionHandler::is_denylisted_extension("photos/image.jpg")
    );
    println!("  was_compressed: {}", jpeg_result.was_compressed);
    println!("  stored algorithm tag: {:?}", jpeg_result.algorithm);
    println!(
        "  original_size={}, compressed_size={}",
        jpeg_result.original_size, jpeg_result.compressed_size
    );
    println!(
        "  (Even when was_compressed=false, the stored bytes are a checksummed LZ4 frame with"
    );
    println!("   stored/uncompressed blocks — never raw, unprotected bytes.)");
    println!();

    // Scenario 2: reading it back only needs the stored algorithm tag, not the
    // current handler config — this is the consistency property.
    println!("Scenario 2: Decompressing using only the stored algorithm metadata");
    match handler.decompress_with_algorithm(&jpeg_result.data, jpeg_result.algorithm.clone()) {
        Ok(decompressed) => {
            println!("  Decompression succeeded using stored algorithm metadata");
            println!("  Bytes match original: {}", decompressed == jpeg_data);
        }
        Err(e) => println!("  Decompression failed: {}", e),
    }
    println!();

    // Scenario 3: a text/JSON key — not in the built-in denylist, so it
    // compresses under the default layer.
    println!("Scenario 3: JSON key, no cache-rule override (compresses under default layer)");
    let json_data =
        b"{\"this\": \"is some sample JSON data that should compress well under LZ4 because it repeats\"}".repeat(4);
    let json_denylisted = CompressionHandler::is_denylisted_extension("config/settings.json");
    println!(
        "  is_denylisted_extension(\"config/settings.json\") = {}",
        json_denylisted
    );
    let json_result =
        handler.compress_with_metadata(&json_data, "config/settings.json", !json_denylisted);
    println!("  was_compressed: {}", json_result.was_compressed);
    println!(
        "  original_size={}, compressed_size={}",
        json_result.original_size, json_result.compressed_size
    );
    println!();

    // Scenario 4: a cache-rule explicitly forcing compression of a normally
    // denylisted extension ("rules win" — see CacheManager::effective_compression).
    println!("Scenario 4: A cache_rules.json rule can override the denylist either way");
    println!("  e.g. {{\"pattern\": \"**/*.jpg\", \"compression_enabled\": true}}");
    println!("  forces compression of .jpg keys despite the built-in denylist.");
    let jpeg_forced_result = handler.compress_with_metadata(jpeg_data, "photos/image.jpg", true);
    println!(
        "  was_compressed (rule-forced): {}",
        jpeg_forced_result.was_compressed
    );
    println!();

    println!("Summary:");
    println!("========");
    println!("- Cache consistency is maintained with per-entry algorithm metadata");
    println!("- Every write is a checksummed LZ4 frame (compressed OR store-mode blocks)");
    println!("- Reads depend only on the stored algorithm tag, never current config");
    println!("- The built-in denylist is the default layer; cache_rules.json wins when set");
}
