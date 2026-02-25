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

The proxy implements an advanced optimization that eliminates unnecessary decompress/recompress cycles when promoting data from disk cache to RAM cache.

### The Problem (Before Optimization)

Traditional multi-tier caching systems suffer from compression inefficiency:

```
Disk Cache: File (LZ4 compressed, 1MB)
     ↓ Load for RAM promotion
RAM Cache: File (decompressed to 500MB for size check)
     ↓ Size check: 500MB > 256MB limit → REJECTED
Result: Large compressible files cannot be cached in RAM
```

Or if the file did fit:

```
Disk Cache: File (LZ4 compressed, 1MB) 
     ↓ Decompress for RAM cache
RAM Cache: File (uncompressed, 500MB)
     ↓ Recompress with LZ4
RAM Cache: File (LZ4 compressed, 1MB)
Result: Wasted CPU cycles (decompress + recompress same algorithm)
```

### The Solution (After Optimization)

The proxy now preserves compression state during cache tier promotion:

```
Disk Cache: File (LZ4 compressed, 1MB)
     ↓ Pass compressed data directly
RAM Cache: File (LZ4 compressed, 1MB) → Size check: 1MB < 256MB → ACCEPTED
Result: Large compressible files cached efficiently in RAM
```

### Algorithm Compatibility Matrix

| Disk Cache | RAM Cache Config | Behavior | CPU Cost |
|------------|------------------|----------|----------|
| LZ4 | LZ4 | Pass compressed data directly | **0** (optimized) |
| Zstd | LZ4 | Decompress Zstd, recompress LZ4 | 1x decompress + 1x compress |

### Performance Benefits

**CPU Efficiency**:
- **Before**: 2x compression work (decompress + recompress)
- **After**: 0x compression work (direct pass-through)
- **Savings**: 100% CPU reduction for same-algorithm promotion

**Memory Efficiency**:
- **Before**: Temporary 500MB buffer during decompression
- **After**: No temporary buffers (1MB stays compressed)
- **Savings**: 99.8% memory reduction during promotion

**Cache Capacity**:
- **Before**: Size check on 500MB uncompressed data → rejected
- **After**: Size check on 1MB compressed data → accepted
- **Result**: 500x improvement in effective cache capacity for compressible data

### Real-World Example

**Scenario**: Application logs (highly compressible JSON)
- **File size**: 500MB uncompressed
- **LZ4 compression**: 500MB → 1MB (99.8% ratio)
- **RAM cache limit**: 256MB

**Before Optimization**:
```
2025-12-12T17:56:31.099287+00:00 WARN Entry /bucket/app.log too large for RAM cache 
(524288000 bytes > 268435456 bytes max)
```

**After Optimization**:
```
2025-12-12T17:56:31.099287+00:00 DEBUG Using pre-compressed data for RAM cache entry: 
/bucket/app.log (algorithm: Lz4)
2025-12-12T17:56:31.099287+00:00 DEBUG Stored entry in RAM cache: /bucket/app.log 
(1048576 bytes, compressed: true, algorithm: Lz4)
```

**Result**: 500MB log file successfully cached in RAM using only 1MB of space.

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
