# Compression

Hybrid Cache for Amazon S3 stores every cached range as an LZ4 frame with a checksum, and
compresses the payload inside the frame unless the content is already compressed. Compression is
on by default and needs no configuration. Reads decode each entry by the algorithm recorded with
it, so a changed setting affects new writes only.

## Turn it on or off

These are the defaults:

```yaml
compression:
  enabled: true      # false stores every payload uncompressed, still framed and checksummed
  threshold: 1024    # Payloads smaller than this many bytes are stored uncompressed
  preferred_algorithm: "lz4"  # LZ4 is the only algorithm; the field selects nothing
```

[Compression configuration](CONFIGURATION.md#compression-configuration) lists the field types and
defaults. To change the decision for some keys, set `compression_enabled` in a
[cache rule](CONFIGURATION.md#cache-rules). There is no field for the extension list.

## How it works

### The compression decision

For each cache write, the proxy decides whether to run the LZ4 block compressor:

1. A payload smaller than `compression.threshold` (default `1024` bytes) is stored uncompressed,
   whatever any rule says.
2. If a cache rule sets `compression_enabled` for the key, the rule decides: `true` compresses the
   payload whatever its extension, and `false` stores it uncompressed. The rule's value replaces
   `compression.enabled` for the keys it matches.
3. Otherwise the payload is compressed when `compression.enabled` is `true` and the key's extension
   is not on the built-in denylist.

An uncompressed payload is still written as a checksummed frame, described under
[Integrity](#integrity-every-write-is-a-checksummed-lz4-frame).

### Built-in denylist

Extensions on this list skip the block compressor unless a cache rule sets `compression_enabled`
for the key:

- **Images**: `jpg`, `jpeg`, `png`, `gif`, `webp`, `avif`, `heic`, `heif`
- **Video**: `mp4`, `avi`, `mkv`, `mov`, `wmv`, `flv`, `webm`, `m4v`
- **Audio**: `mp3`, `aac`, `ogg`, `flac`, `m4a`, `wma`, `opus`
- **Archives**: `zip`, `rar`, `7z`, `gz`, `bz2`, `xz`, `lz4`, `zst`, `tgz`
- **Documents**: `pdf`, `docx`, `xlsx`, `pptx`, `odt`, `ods`, `odp`
- **Applications**: `apk`, `ipa`, `jar`, `war`, `ear`
- **Fonts**: `woff`, `woff2`
- **Database**: `sqlite`, `db`
- **Executables**: `exe`, `msi`, `dmg`, `pkg`

Every other extension, including `.txt`, `.json`, `.csv`, `.log` and `.parquet`, and a key with no
extension, is compressed by default, subject to the threshold.

The extension is the text after the last dot in the key's last path segment, lowercased. So
`archive.tar.gz` matches `gz`, and the list cannot hold a multi-dot suffix such as `tar.gz`. To
treat `.tar.gz` differently from `.gz`, use a cache rule, which matches the whole cache key:
`{"pattern": "**/*.tar.gz", "compression_enabled": false}`.

### Integrity: every write is a checksummed LZ4 frame

Every write, compressed or not, is a standard LZ4 frame with an xxHash32 content checksum. When the
decision above is not to compress, the proxy writes a store-mode frame: the LZ4 frame format's
stored-block form, with the same checksum. Writing a store-mode frame never runs the block
compressor, so a denylisted or rule-disabled key costs no compression CPU.

Every read verifies the checksum as it decodes, and decodes compressed and store-mode frames the
same way. A bit flip, a truncation or other damage to a range file shows up as a decode error:

- A read the proxy assembles in memory treats the range as a cache miss and fetches the bytes from
  S3.
- A read streamed from the cache has already sent part of its response, so it ends early with a
  connection error. The proxy marks the file, and later reads of the range on that proxy are
  fetched from S3 until the range is removed or cached again, or two minutes pass with no read turned away by the mark.
  [When the proxy can return data that differs from S3](CACHE_FRESHNESS.md#when-the-proxy-can-return-data-that-differs-from-s3)
  gives the full bound.

A checksum shows that a file is intact, not that it is the file its metadata describes. The proxy
records each range file's size and modification time when it writes the range. Where a range's
metadata carries that record, a read that opens a file with another size or time treats the range
as not cached.

A cache entry whose metadata names the algorithm `None` holds raw bytes with no frame and no
checksum. The proxy still reads it, because reads follow each entry's recorded algorithm, but
nothing checks its integrity. No write produces such an entry. Once eviction, invalidation or
expiry removes it, the next fetch caches the object as a checksummed frame.

### Block size

Every frame the proxy writes, compressed or store-mode, declares 64 KiB blocks (block-size code 4
in the frame descriptor). The decoder sizes its buffers from the block size in the frame header,
not from the amount of data: one compressed-block buffer and one decompressed-block buffer per
stream. With 64 KiB blocks, each disk read or streamed serve of a range holds about 128 KiB of
decoder state; with 4 MiB blocks it would hold about 8 MiB, whatever the range length.

The proxy reads a range file through a buffer as large as the file, up to 256 KiB, so each read
call covers several blocks. In all, a read of a file with 64 KiB blocks holds about 384 KiB. A
range file with 4 MiB blocks, which the proxy reads but does not write, holds about 8.25 MiB per
read. These figures are per concurrent stream, so the total grows with
`server.max_concurrent_requests`.

Smaller blocks compress slightly worse, because an LZ4 match cannot cross a block boundary. Highly
repetitive content can take up to twice the disk space it would take with 4 MiB blocks, which is
still a small fraction of its uncompressed size. Content that does not compress grows by a few
bytes per block.

The reader takes the block size from each frame's header, on both the buffered and the streamed
read paths. It therefore decodes frames of either block size, and one range file can hold both.

### RAM cache compression optimization

When the proxy promotes a range from the disk cache to the RAM cache, it stores the disk frame
unchanged, compressed or store-mode, and never decompresses and recompresses it. Every promotion
path does this: full objects, write-cached objects and ranges. The RAM cache's size checks use the
frame's size, so a compressible object takes its compressed size in RAM. For example, a 100 MiB log
file that compresses to 25 MiB on disk counts as 25 MiB against `cache.max_ram_cache_size`.

### Multipart uploads

Each part of a multipart upload takes the same decision as a single PUT. Each part's metadata
records its algorithm (`Lz4`, for compressed and store-mode frames alike), and
CompleteMultipartUpload carries those records into the completed object's ranges, so a GET decodes
every part correctly. For example, the parts of `data.zip` are written as store-mode frames
because `zip` is on the denylist, and the parts of `logs.json` are compressed.

## Limits

- LZ4 is the only algorithm. `preferred_algorithm` is parsed and selects nothing.
- The extension denylist is built in. A cache rule's `compression_enabled` is the only per-key
  change.
- A changed setting or rule applies to new writes only. Entries already cached keep the algorithm
  recorded with them.

## Observe it

- [`range_file_corruption_detected`](METRICS_REFERENCE.md#atomic_metadata) on `/metrics` counts
  damaged range files that reads found.
- `cache.total_cache_size` reports the bytes the cache holds on disk, after compression.

## See also

- [Caching](CACHING.md): what the proxy caches and how cache rules apply
- [Compression configuration](CONFIGURATION.md#compression-configuration): the field reference
- [Multipart uploads](MULTIPART_UPLOAD.md): how parts are staged and completed
