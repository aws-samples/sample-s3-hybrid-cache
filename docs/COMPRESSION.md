# Compression

The S3 proxy compresses cacheable data with LZ4, using per-entry algorithm
metadata so that reads never depend on the current compression
configuration. The compression *decision* — whether to run the LZ4 block
compressor for a given write — is a denylist with compress-by-default
fallback, not an allowlist: any file extension not explicitly recognized as
"already compressed" is compressed by default.

## The Compression Decision

For every cache write, the proxy computes:

```
enabled AND size >= threshold AND (rule_explicitly_set_it OR NOT denylisted(extension))
```

- **`enabled`** — the global `compression.enabled` setting.
- **`threshold`** — the global `compression.threshold` (bytes). A size floor
  guarding against compressing tiny payloads; it applies regardless of the
  extension or any rule override.
- **The built-in denylist** — a hardcoded set of extensions
  (`CompressionHandler::is_denylisted_extension`, see below) that are
  already compressed and gain little to nothing from LZ4. This is the
  *default* layer, consulted only when no `cache_rules.json` rule explicitly
  sets `compression_enabled` for the key.
- **Cache rules win.** If a `cache_rules.json` glob rule (see
  `docs/CONFIGURATION.md`) explicitly sets `compression_enabled: true` or
  `false` for a matching key, that value is honored verbatim — including
  forcing compression of a normally-denylisted extension (e.g. `.jpg`), or
  skipping compression of a normally-compressible one. This is the intended
  operator-facing override mechanism; there is no separate extension-list
  configuration field.

## Built-In Denylist

Extensions in this list skip the LZ4 block compressor under the *default*
layer (no cache-rule override):

- **Images**: `jpg`, `jpeg`, `png`, `gif`, `webp`, `avif`, `heic`, `heif`
- **Video**: `mp4`, `avi`, `mkv`, `mov`, `wmv`, `flv`, `webm`, `m4v`
- **Audio**: `mp3`, `aac`, `ogg`, `flac`, `m4a`, `wma`, `opus`
- **Archives**: `zip`, `rar`, `7z`, `gz`, `bz2`, `xz`, `lz4`, `zst`, `tgz`
- **Documents**: `pdf`, `docx`, `xlsx`, `pptx`, `odt`, `ods`, `odp`
- **Applications**: `apk`, `ipa`, `jar`, `war`, `ear`
- **Fonts**: `woff`, `woff2`
- **Database**: `sqlite`, `db`
- **Executables**: `exe`, `msi`, `dmg`, `pkg`

Everything else (`.txt`, `.json`, `.xml`, `.html`, `.css`, `.js`, `.log`,
`.md`, `.yaml`, `.py`, `.rs`, `.sql`, and any extension not listed above)
compresses by default, subject to the size threshold.

**Extension matching caveat**: extraction takes only the final dot-suffix of
the object key's last path segment, lowercased. `archive.tar.gz` matches via
the `gz` arm, not a distinguished `tar.gz` entry — `tar.gz` is not, and
cannot be, a member of this list. Operators needing exact multi-part-suffix
precision (distinguishing `.tar.gz` from `.gz`) should use a
`cache_rules.json` glob rule instead, e.g. `{"pattern": "**/*.tar.gz", "compression_enabled": false}`
— glob rules match the full cache key and have no extraction step.

## Integrity: Every Write Is a Checksummed LZ4 Frame

Every write — compressed or not — produces a valid LZ4 frame carrying an
**xxhash32 content checksum** (`FrameInfo::content_checksum = true`). When
the compression decision above is `false`, the proxy does not fall back to
writing raw, unprotected bytes: it encodes a **store-mode frame** — a
standard LZ4 frame whose data blocks are stored uncompressed (the LZ4 frame
format's native mechanism for incompressible data), still carrying the same
content checksum. Store-mode encoding never invokes the LZ4 block
compressor, so skipping compression for a denylisted or rule-disabled
extension still avoids the CPU cost — it just does so via a stored block
instead of raw bytes.

On every read, `lz4_flex::frame::FrameDecoder` verifies the checksum as it
decodes, transparently handling compressed and store-mode frames identically
(both are standard LZ4 frames — `FrameDecoder` requires no special-casing).
Any disk bit-flip, truncation, or silent corruption of a range file surfaces
as a decode error, which callers treat as a cache miss and refetch from S3.
No separate hash-at-rest or read-time re-hashing is required.

Both paths are implemented in `CompressionHandler`: `compress_with_algorithm`
for the compressed path, `encode_store_mode_frame` for the store-mode path.
`compress_with_metadata` picks between them based on the caller's
already-computed decision. Tests `test_store_mode_round_trip`,
`test_store_mode_corruption_detected`, `test_corrupted_frame_data_returns_error`,
and `test_corrupt_cache_entry_decompression_error` cover the round-trip and
corruption paths.

**Legacy entries**: cache entries written by proxy versions prior to 2.3.0 may be tagged `CompressionAlgorithm::None` — raw bytes with no
frame or checksum. These remain readable (the read path dispatches on the
stored per-entry algorithm tag, never on current configuration), but carry
no integrity protection. No write path in this version produces `None`
entries; they age out naturally via TTL/eviction and get re-cached as
checksummed frames on next fetch.

## Algorithm Support & Cache Consistency

- **Current**: LZ4 (compressed or store-mode blocks — both tagged `Lz4`)
- **Legacy**: `None` (raw bytes, no checksum) — read-only, no longer written
- **Future**: Zstd, Brotli, LZ4HC (easily extensible; `preferred_algorithm`
  in config is parsed but not yet wired to a second algorithm)
- **Per-Entry Metadata**: Each cache entry stores which algorithm was used,
  so reads never depend on the current compression configuration —
  changing the denylist, threshold, or cache rules only affects new writes

## RAM Cache Compression Optimization

When promoting data from disk cache to RAM cache, the proxy stores the
on-disk frame verbatim — whichever form it's in (LZ4-compressed blocks or a
store-mode/uncompressed-but-framed block) is exactly what's cached in RAM.
There is no separate "always compress for RAM" step and no decompress-then-
recompress round trip. Size checks use the on-disk (framed) size, so large
compressible files are accepted into RAM cache based on their compressed
footprint rather than their uncompressed size.

This holds across every promotion path — full-object, write-cache, and
range promotion all pass the disk frame through unchanged
(`compressed: true` + the entry's real `compression_algorithm`). Range
promotion previously decompressed the frame and stored the raw bytes with
`compressed: false`, which meant range-promoted entries consumed their full
uncompressed size in RAM instead of the compact on-disk footprint. That was
a bug, not intended behavior, and has since been fixed.

**Algorithm handling**:

| Disk Cache | RAM Cache | Behavior | CPU Cost |
|------------|-----------|----------|----------|
| LZ4 (compressed or store-mode) | LZ4 | Pass data directly | None |

**Example**: A 500MB text file compressed to 1MB on disk is stored in RAM
cache as 1MB. The size check compares 1MB against the RAM cache limit, not
500MB.

No configuration is required. The behavior is automatic based on compression
metadata stored with each cache entry.

**Debug log indicators**:
- `"Using pre-compressed data for RAM cache entry"` — direct pass-through path
- `"Compressed data for RAM cache entry"` — first-time compression path

## Multipart Upload Compression

Multipart uploads use the same compression decision as single-part uploads:

- Each part's effective compression decision is computed the same way
  (enabled + threshold + rules-win-over-denylist)
- The resulting algorithm tag (always `Lz4` — compressed or store-mode) is
  stored per-part in the tracking metadata
- On `CompleteMultipartUpload`, each part's compression algorithm is
  preserved in the final range metadata
- This ensures correct decompression when serving cached multipart uploads

**Example**: Uploading `data.zip` via multipart:
- Each part is written as a store-mode LZ4 frame (`.zip` is in the built-in
  denylist; no `cache_rules.json` override present)
- Metadata records `compression_algorithm: Lz4` for each part
- GET requests decode the frame via `FrameDecoder` before serving —
  identically whether the frame is compressed or store-mode

**Example**: Uploading `logs.json` via multipart:
- Each part is compressed with LZ4 (`.json` is not in the built-in denylist)
- Metadata records `compression_algorithm: Lz4` for each part
- GET requests decompress each part via the same frame decoder

## Configuration

```yaml
compression:
  enabled: true      # master on/off switch
  threshold: 1024    # size floor in bytes; smaller writes always skip compression
  preferred_algorithm: "lz4"  # only "lz4" is implemented; parsed but otherwise inert
```

Field types, defaults, and the removed `content_aware` alias are documented in
[CONFIGURATION.md — Compression Configuration](CONFIGURATION.md#compression-configuration).

There is no extension-list configuration field. The built-in denylist above is the
default, and per-key overrides go through the `compression_enabled` field on a
`cache_rules.json` glob rule (see
[CONFIGURATION.md — Cache Rules](CONFIGURATION.md#cache-rules)), which overrides the
denylist in either direction.

## Benefits

1. **Content-Aware Compression**: Saves CPU cycles and storage space by
   avoiding redundant compression of already-compressed formats
2. **Operator Override**: `cache_rules.json` lets any key's compression be
   forced on or off, overriding the built-in denylist in either direction
3. **Algorithm Consistency**: Per-entry metadata means denylist, threshold,
   or rule changes only affect new writes — existing cache entries decode
   unchanged
4. **Universal Integrity**: Every entry, compressed or not, is a
   checksummed LZ4 frame — corruption is detected on read regardless of
   whether the LZ4 compressor ran
5. **Multi-Tier Optimization**: Eliminates unnecessary compression cycles
   between disk and RAM cache tiers
6. **Multipart Support**: Correct compression handling for multipart
   uploads with per-part algorithm tracking
