# Compression

The S3 proxy includes intelligent compression with per-entry algorithm metadata that automatically determines which files should be compressed based on their file extensions. The system is optimized to eliminate unnecessary compression cycles between cache tiers.

## Files That Are Compressed (LZ4)

- **Text files**: `.txt`, `.json`, `.xml`, `.html`, `.css`, `.js`, `.csv`, `.log`, `.md`
- **Configuration files**: `.yaml`, `.yml`, `.ini`, `.conf`, `.cfg`
- **Source code**: `.py`, `.java`, `.cpp`, `.h`, `.rs`, `.go`, `.php`
- **Data files**: `.sql`, `.tsv`, `.ndjson`

## Files That Skip Compression (Frame-Wrapped, Uncompressed Blocks)

- **Images**: `.jpg`, `.png`, `.gif`, `.webp`, `.avif`, `.heic`
- **Videos**: `.mp4`, `.avi`, `.mkv`, `.mov`, `.webm`
- **Audio**: `.mp3`, `.aac`, `.ogg`, `.flac`, `.opus`
- **Archives**: `.zip`, `.rar`, `.7z`, `.gz`, `.bz2`, `.xz`
- **Documents**: `.pdf`, `.docx`, `.xlsx`, `.pptx`
- **Applications**: `.apk`, `.jar`, `.exe`, `.dmg`

## Algorithm Support & Cache Consistency

- **Current**: LZ4 compression (fast, good compression ratio)
- **Future**: Zstd, Brotli, LZ4HC (easily extensible)
- **Per-Entry Metadata**: Each cache entry stores which algorithm was used
- **Seamless Upgrades**: Changing compression algorithms doesn't invalidate existing cache
- **Gradual Migration**: Cache entries can be optionally migrated to new algorithms on access

## RAM Cache Compression Optimization

When promoting data from disk cache to RAM cache, the proxy passes compressed data directly without decompressing and recompressing. Size checks use the compressed size, so large compressible files are accepted into RAM cache based on their compressed footprint rather than their uncompressed size.

**Algorithm handling**:

| Disk Cache | RAM Cache | Behavior | CPU Cost |
|------------|-----------|----------|----------|
| LZ4 | LZ4 | Pass compressed data directly | None |
| Different algorithm | LZ4 | Decompress then recompress | 1× decompress + 1× compress |

**Example**: A 500MB text file compressed to 1MB on disk is stored in RAM cache as 1MB. The size check compares 1MB against the RAM cache limit, not 500MB.

No configuration is required. The behavior is automatic based on compression metadata stored with each cache entry.

**Debug log indicators**:
- `"Using pre-compressed data for RAM cache entry"` — direct pass-through path
- `"Compressed data for RAM cache entry"` — first-time compression path

## Multipart Upload Compression

Multipart uploads use the same content-aware compression as single-part uploads:

- Each part is individually compressed based on the object's file extension
- The compression algorithm used is stored per-part in the tracking metadata
- On `CompleteMultipartUpload`, each part's compression algorithm is preserved in the final range metadata
- This ensures correct decompression when serving cached multipart uploads

**Example**: Uploading `data.zip` via multipart:
- Each part is wrapped in LZ4 frame format with uncompressed blocks (`.zip` is already compressed)
- Metadata records `compression_algorithm: Lz4` for each part
- GET requests decode the frame wrapper before serving

**Example**: Uploading `logs.json` via multipart:
- Each part is compressed with LZ4 frame format (`.json` is compressible)
- Metadata records `compression_algorithm: Lz4` for each part
- GET requests decompress each part via frame decoder before serving

## Benefits

This intelligent approach provides multiple layers of optimization:

1. **Content-Aware Compression**: Saves CPU cycles and storage space by avoiding redundant compression of already-compressed formats
2. **Algorithm Consistency**: Maintains cache consistency across compression rule changes and algorithm upgrades  
3. **Multi-Tier Optimization**: Eliminates unnecessary compression cycles between disk and RAM cache tiers
4. **Capacity Optimization**: Allows large compressible files to be cached in RAM by checking compressed size limits
5. **Multipart Support**: Correct compression handling for multipart uploads with per-part algorithm tracking
