# Developer documentation

This page is for developers changing Hybrid Cache for Amazon S3: the trade-offs behind its main
design decisions, the implementation detail a maintainer needs, the known limitations, and how to
test and debug. [Architecture](ARCHITECTURE.md) is the reference for the core principles
(transparent forwarder, streaming architecture, unified range storage), the module inventory, the
system diagram, and the streaming and sharding designs; this page does not repeat them. Code is
cited by symbol name; read the symbol in `src/` rather than trusting a paraphrase of it.

## Contents

- [Module organization](#module-organization)
- [Key design decisions](#key-design-decisions)
- [Implementation details](#implementation-details)
- [Optimizations](#optimizations)
- [Known limitations](#known-limitations)
- [Testing strategy](#testing-strategy)
- [Debugging tips](#debugging-tips)
- [Code style guidelines](#code-style-guidelines)
- [Contributing guidelines](#contributing-guidelines)
- [References](#references)

## Module organization

[Module organization](ARCHITECTURE.md#module-organization) groups the modules by subsystem (cache
layer, journal and size accounting, recovery, HTTP proxy, listeners, networking, observability,
configuration). To list the current modules:

```bash
ls src/*.rs
```

The two largest modules are `cache.rs` and `http_proxy.rs`. Both contain several similar-looking
range read paths whose cache behavior is not visible at the call site. Read the
[Range read-path map](ARCHITECTURE.md#3a-range-read-path-map) before changing any of them.

## Key design decisions

### Streaming responses

The design and a sketch of `TeeStream` are in
[Streaming response architecture](ARCHITECTURE.md#1-streaming-response-architecture); the
implementation is `src/tee_stream.rs`. When the cache channel is full, `TeeStream` switches to
backpressure and waits for the cache writer rather than dropping bytes, and counts each switch in
`BACKPRESSURE_EVENTS_TOTAL`.

Trade-offs:

- A background task per streamed response.
- A slow cache writer slows the client response through backpressure.
- A partial cache hit is still buffered, because it is merged.

### Bucket-first hash-based sharding

The layout, the reason for BLAKE3 and the capacity figures are in
[Bucket-first hash-based sharding](ARCHITECTURE.md#2-bucket-first-hash-based-sharding); the
implementation is `get_sharded_path` in `src/disk_cache.rs`.

- Lookup is O(1) whatever the cache size, and files spread evenly across directories.
- A bucket's cache can be managed as a directory (`rm -rf cache_dir/metadata/{bucket}/`), and
  file names do not repeat the bucket name.
- The cost is up to 1,048,576 leaf directories per bucket, each an inode on the cache volume, and
  one BLAKE3 hash per lookup.

### Unified range storage

All cached data is stored as ranges, whatever wrote it:

- A PUT is stored as range 0-N at once (the write cache is enabled by default), through
  `CacheManager::store_put_as_write_cached_range`.
- A GET stores a full object or a partial range.
- A multipart upload's parts become ranges on completion, in
  `SignedPutHandler::finalize_multipart_upload`.

So a range request can be served from any cached data, and a TTL transition rewrites one `.meta`
and no range file. The cost is larger metadata files (upload state and the parts list) and more
complex multipart handling (part position calculation).

### Frequency-recency eviction

Pure LRU evicts frequently used entries during a scan, and pure LFU lets one-time reads pollute
the cache. The proxy uses a simplified frequency-recency hybrid, labelled `tinylfu` in the
configuration:

- Victims are scored by `decayed_frequency(access_count, idle_secs)` in `src/cache.rs`: the
  access count halves for every `TINYLFU_HALF_LIFE_SECS` of idle time.
- The RAM cache picks its victim with `shard_find_tinylfu_victim` in `src/ram_cache.rs`, reading
  each entry's `access_count` and `last_accessed` atomics at eviction time.
- The disk cache sorts candidates with `sort_range_candidates_for_tinylfu`, after expiry and
  compressed size.
- Counting uses per-entry counters, not a probabilistic sketch.

[Eviction algorithms](EVICTION.md#eviction-algorithms) gives the full ordering for each cache.

This is not the full TinyLFU algorithm from the research paper: it has no Count-Min Sketch, no
doorkeeper Bloom filter for admission, and no window and main segments (W-TinyLFU). A hot entry
that is idle keeps a higher score than a one-hit read, so a one-time scan does not flush the
cache, and the bookkeeping is two atomics per entry. In the RAM cache the score ignores entry
size, and victim selection scans the shard, O(entries in the shard) where LRU is O(1). GDSF
(Greedy Dual Size Frequency) was considered for its size-aware byte hit rate and rejected as
harder to implement and tune.

### Range request merging

On a partial cache hit the proxy fetches only the missing parts from S3:

- It identifies the missing ranges and merges those separated by small gaps
  (`range_merge_gap_threshold`, default 1 MiB), in `RangeHandler::consolidate_missing_ranges`.
- It fetches the merged ranges with `RangeHandler::fetch_missing_ranges` and combines cached and
  fetched bytes with `RangeHandler::merge_range_segments` (`src/range_handler.rs`).

This fetches fewer bytes from S3 than refetching the whole range. The cost is more code, a merge
that has to buffer rather than stream, and a gap threshold that may need tuning per workload. See
[Range request gap consolidation](#range-request-gap-consolidation).

### Connection keepalive

A new TCP and TLS connection per request would add a handshake to every request. The proxy reuses
connections through hyper's connection pool, with per-IP pools. `src/s3_client.rs` builds the
client with `pool_idle_timeout` from `connection_pool.idle_timeout` (default 55s). The per-host
and per-IP idle limits and their defaults are in [Connection pooling](CONNECTION_POOLING.md). The
cost is handling stale connections, rotating connections when DNS changes, and memory for idle
connections.

### Part number request caching

Parts are stored as ranges in the existing range storage:

- `partNumber` in the query string identifies a GetObject part request.
- The multipart metadata comes from the S3 response headers (`x-amz-mp-parts-count`,
  `Content-Range`), and each part is stored as a range at its `Content-Range` offsets.
- Each part's byte range is recorded in `ObjectMetadata::part_ranges` (a
  `HashMap<u32, (u64, u64)>`) and looked up by `CacheManager::lookup_part`.
- Cached parts are served with the same response headers S3 returned.

Repeated requests for a part are served without contacting S3. The costs are more request
parsing and the multipart fields in the metadata. [Part number requests](#part-number-requests)
lists when a part request is and is not cached.

### RAM metadata cache

`src/metadata_cache.rs` keeps `NewCacheMetadata` objects in RAM for both HEAD and GET. Each
`MetadataCacheEntry` records `loaded_at`, `disk_mtime` and `last_accessed` beside the metadata.

- **One `.meta` for HEAD and GET, with independent TTLs.** HEAD freshness uses
  `head_expires_at` and GET freshness the object-level `expires_at`. A HEAD expiry does not delete
  the file, because the ranges may still be valid, and a GET expiry does not affect HEAD validity.
- **LRU, not TinyLFU.** Metadata entries are all about the same size (about 1 to 2 KB), so
  frequency scoring adds nothing; eviction pops the front of `lru_order`.
- **No per-key lock on the request path.** `CacheManager::get_metadata_cached` checks the RAM
  cache and, on a miss, reads the `.meta` without a lock, so concurrent misses for one key each
  read the file. `MetadataCache::get_or_load`, which does lock per key, has no caller on the
  request path.
- **Stale file handle recovery for NFS.** The `.meta` reader retries `ESTALE` up to 3 times
  (after 50, 100 and 150 ms). A read still stale after that is a cache miss, and the `.meta` is
  not deleted.

A hot entry's metadata is served from memory with no disk read. The costs are the memory for the
metadata cache (see [Memory use](CACHE_INTERNALS.md#memory-use)) and up to one refresh interval
of staleness.

### RAM-disk cache coherency

RAM cache hits do not update disk metadata by themselves, which would skew eviction, and direct
metadata writes from several proxies on shared NFS storage would race. The proxy batches the
updates through a journal:

- Cache-hit updates (TTL refresh, access count) are buffered in RAM by `CacheHitUpdateBuffer`
  (`src/cache_hit_update_buffer.rs`), recorded through `DiskCacheManager::record_range_access` for disk hits. RAM hits are queued for
  one background task that records them into the buffer with `CacheHitUpdateBuffer::record_access_update`.
- The buffer is flushed every 5 seconds to a per-proxy journal file under `metadata/_journals/`.
- `JournalConsolidator` applies journal entries to the metadata files under each key's slot
  lock; the entry types are `JournalOperation` in `src/journal_manager.rs`. It tries each key's
  lock once and skips a contended key until the next cycle.

[RAM-disk cache coherency](CACHE_INTERNALS.md#ram-disk-cache-coherency) describes the mechanism
in full. The costs are a delay before statistics reach the `.meta` (the 5-second buffer plus the
consolidation interval), memory for pending updates up to the 10,000-entry buffer ceiling, and
background I/O for consolidation.

### Coordinated cache initialization

`src/cache_initialization_coordinator.rs` runs startup in sequential, logged phases with shared
validation utilities. A starting proxy does not walk the cache: it reads the saved size
state and, where that is missing or stale, asks the validation scheduler for a scan. Startup time
therefore does not grow with the cache. The cost is coordination logic between subsystems.

## Implementation details

### Cache key format

Keys are path-only (no hostname) with no leading slash; `normalize_cache_key` strips it.

```text
{bucket}/{object_key}
```

- Full object: `my-bucket/path/to/object.txt` (`CacheManager::generate_cache_key`)
- Range, disk form: `my-bucket/path/to/object.txt:range:0-8388607` (`generate_range_cache_key`)
- Range, RAM form: `my-bucket/path/to/object.txt:range:0:8388607` (`generate_ram_range_key`)
- Part: `my-bucket/path/to/object.txt:part:1` (`generate_part_cache_key`)

For a virtual-hosted or access-point request, `generate_cache_key` prefixes the bucket or
access-point name taken from the `Host` header, so path-style and virtual-hosted requests for the
same object share one entry. The hostname is left out because S3 routes on the bucket, which
also keeps keys and file names short.

### Metadata structure

A `.meta` file holds one `NewCacheMetadata` (`src/cache_types.rs`): the `cache_key`, an
`ObjectMetadata`, the cached `ranges` (each a `RangeSpec` with its offsets, `.bin` path,
compression algorithm and sizes, and access statistics), the object-level `created_at` and
`expires_at`, and the HEAD fields (`head_expires_at`, `head_access_count` and related).

`ObjectMetadata` carries the S3 object facts (`etag`, `last_modified`, `content_length`,
`content_type`, `response_headers`), the upload state (`upload_state`: Complete, InProgress or
Bypassed; `cumulative_size`; `parts`), the multipart part map (`parts_count`, `part_ranges`,
`upload_id`), and the write cache fields (`is_write_cached` and its timestamps). A `.meta` is
typically under 100 KB, even with hundreds of ranges.

### Compression algorithm selection

`CacheManager::effective_compression` decides whether a write is compressed. A write below
`compression.threshold` (default 1 KiB) is never compressed. Above it, a per-key cache rule that
sets `compression_enabled` wins; otherwise the built-in extension denylist,
`CompressionHandler::is_denylisted_extension`, decides. [Compression](COMPRESSION.md) has the
denylist and the decision order.

Each `RangeSpec` records the algorithm used and its compressed and uncompressed sizes, so
changing the algorithm does not invalidate the existing cache. Already-compressed data is not
compressed again, and compressible content such as text and JSON takes less space.

### Error handling strategy

A cache error does not fail the request: the proxy serves from S3 instead, and a missing range
file is a cache miss. [Error handling](ERROR_HANDLING.md) says how each error category is
detected, which ones delete or repair files, and which leave them for a retry. The counters are
`corruption_metadata_total`, `corruption_missing_range_total` and `disk_full_events_total`.

### Multipart upload handling

While an upload is in progress, its parts are cached under `mpus_in_progress/{hash(uploadId)}/`
(`SignedPutHandler`, `src/signed_put_handler.rs`). On `CompleteMultipartUpload`,
`finalize_multipart_upload` keeps the requested parts, checks their ETags, computes cumulative
byte offsets, and writes one `RangeSpec` per part with that part's own compression algorithm.
[Multipart upload caching](MULTIPART_UPLOAD.md) lists the gates, and which failures clean up the
staging directory and which leave it for the TTL sweep.

Each upload has its own directory, and a failed gate skips caching without failing the S3
operation; see [Multipart upload processing](ARCHITECTURE.md#multipart-upload-processing). A
PutObject invalidates the key's cached object once S3 confirms it. A CompleteMultipartUpload
replaces the cached object, and the old one keeps serving until then; see
[Write-through ordering](CACHING.md#write-through-ordering).

### DNS resolution

`src/connection_pool.rs` builds a hickory resolver from `connection_pool.dns_servers`, or from
the `GOOGLE` and `CLOUDFLARE` name-server sets when that list is empty, instead of the system
resolver. It ignores `/etc/hosts`, which may point S3's hostnames at the proxy, so it resolves
the real S3 addresses, avoids routing loops and lets the pool spread requests across S3's IPs.

### Graceful shutdown

`ShutdownCoordinator` (`src/shutdown.rs`) listens for SIGINT and SIGTERM in
`listen_for_shutdown`, broadcasts a shutdown signal to subscribed components, and runs
`perform_shutdown` within the configured shutdown timeout:

1. Flush the access log buffer.
2. Run a final journal consolidation and save the size state.
3. Release cache locks.
4. Close the connection pools.
5. Final cleanup.

## Optimizations

### Range request gap consolidation

Missing ranges separated by a gap no larger than `cache.range_merge_gap_threshold` (default
1 MiB) are merged into one S3 request by `RangeHandler::consolidate_missing_ranges`, which sorts
by start offset and extends the current range across each small gap. A sparse access pattern
then makes fewer S3 requests, at the cost of fetching the bytes inside the gaps and of buffering
the merge.

### Disk cache admission window

A range accessed within the last 60 seconds (`ADMISSION_WINDOW` in `src/cache.rs`) is not an
eviction candidate unless its object has passed its stored expiry. New ranges start with zero
frequency, so without the window a large download's own ranges would be evicted as they arrive;
the window gives them time to gather access statistics. See
[Admission window protection](EVICTION.md#admission-window-protection).

### Eviction target below the limit

Once over capacity, eviction frees space down to a target below the limit, so it runs in batches
rather than on every write at the boundary. The target is `eviction_target_percent`, default 80:

```rust
let target_size = (max_size as f64 * (self.eviction_target_percent as f64 / 100.0)) as u64;
let bytes_to_free = current_size.saturating_sub(target_size);
```

`cache.eviction_buffer_percent` has no effect; `eviction_target_percent` sets the target.

### Conditional requests answered from cache

`evaluate_conditions_from_cache`, default `true` and settable per key in `cache_rules.json`,
decides who answers a conditional request:

- With local evaluation on, a GET, range GET or HEAD whose only conditional header is
  `If-Match` is served from cache when `get_ttl` is non-zero and the cached entry is unexpired
  with a matching ETag; a HEAD must also be within `head_ttl`. A request whose only conditional
  header is `If-Range`, with a validator that strong-matches the cached ETag, is served from
  cache the same way.
- Any other conditional (`If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` or a mix of
  headers), a mismatch or a miss is forwarded, and S3 decides. So is a request with a client
  bypass header (`Cache-Control: no-cache` or `no-store`, `Pragma: no-cache`).

The proxy never answers `If-None-Match` with a local 304 from cached metadata. That would answer
the client without S3 seeing the request, which skips S3-side authorization for it and can serve
a 304 against an ETag the caller is no longer entitled to. Do not add it.

Requests answered locally do not revalidate credentials with S3. That is the documented trade-off
of `evaluate_conditions_from_cache`, which is why it can be set per key and why the dashboard
shows which rules enable it. Comma-separated ETag lists are parsed per RFC 7232, so
`If-Match: "a", "b", W/"c"` matches on any entry.
[Conditional headers handling](CACHE_FRESHNESS.md#conditional-headers-handling) has the full set
of requests answered locally.

### Lazy directory creation

Directories are created when first needed, not at startup, so startup creates no shard
directories:

```rust
if let Some(parent) = path.parent() {
    tokio::fs::create_dir_all(parent).await?;
}
```

### Atomic file writes

Metadata and range files are written to a temporary file and then renamed, so a crash or power
failure leaves either the old file or the new one, never a partial write:

```rust
tokio::fs::write(&tmp_path, data).await?;
tokio::fs::rename(&tmp_path, &path).await?;
```

### Parallel range fetching

`RangeHandler::fetch_missing_ranges` issues the merged missing ranges concurrently and joins them
with `futures::future::join_all`, over the reused connections described in
[Connection keepalive](#connection-keepalive). A multi-range fill then waits for its slowest
range rather than for the sum of all of them.

## Known limitations

### Write cache

The write cache is enabled by default (`cache.write_cache_enabled`). An object PUT through the
proxy is cached on its way to S3, so it can be read from cache as soon as the upload succeeds.

- **Capacity accounting is atomic.** A reservation goes through `try_reserve_write_cache()`,
  which returns an RAII `WriteReservation` that is released on drop, so a cancelled or panicking
  upload cannot leak capacity. Multipart uploads reserve against the declared content length on
  the same path.
- **The transition from write cache to read cache** runs before the freshness check, so the first
  GET of a write-cached object honors `get_ttl`, including `get_ttl: 0`, which revalidates
  against S3.
- **Conflicts are checked at completion.** On any per-part ETag mismatch at
  `CompleteMultipartUpload`, caching is skipped and the in-progress directory is removed, so the
  cache never keeps an object it cannot show matches S3; see
  [Multipart upload caching](MULTIPART_UPLOAD.md).

### Versioned requests

A request with a `versionId` query parameter bypasses the cache and always goes to S3.

### Part number requests

A GET with `partNumber` (for example `GET /bucket/object?partNumber=5`) is cached when S3's
response carries a `Content-Range` header; the part is stored as a range. An UploadPart (a PUT
with `uploadId` and `partNumber`) is cached during the upload when the write cache is enabled and
has capacity, and the parts become the object's ranges on `CompleteMultipartUpload`, readable at
once.

Part requests that are not cached:

- Upload verification requests (`partNumber` with `uploadId`).
- UploadPart requests beyond the write cache's capacity.
- Responses without the S3 headers above.

HEAD responses carry no multipart metadata (`x-amz-mp-parts-count`). For an object that was not
write-cached, the first part request therefore goes to S3, which supplies the metadata, and later
part requests can be served from cache:

```bash
aws s3api get-object --bucket my-bucket --key large-file.bin --part-number 1 part1.bin  # Miss: forwarded to S3, metadata cached
aws s3api get-object --bucket my-bucket --key large-file.bin --part-number 1 part1-copy.bin  # Hit: served from the cached range
```

### TTL expiry in the RAM cache

RAM range entries carry no expiry of their own: `shard_find_tinylfu_victim` scores entries by
decayed frequency and recency only, so a live entry can be evicted before one whose object has
expired. The disk cache does sort TTL-expired ranges first; see
[Range-level granularity](EVICTION.md#range-level-granularity).

### RAM cache eviction scan

RAM victim selection scans every entry in the shard (O(entries in the shard)) on each eviction,
so a very large number of entries per shard makes RAM eviction slower. Raising
`ram_cache_shard_count` shortens each scan. Disk eviction reads a bounded prefix of a per-shard
index instead; see [Eviction flow](EVICTION.md#eviction-flow).

## Testing strategy

### Running the suite

```bash
cargo test --release  # everything; always use --release
cargo test --release --lib  # in-crate unit tests only
cargo test --release --test group_readpath -- integration_test:: --skip s3_integration_test:: --skip download_coordination_integration_test::  # one test file inside a group
cargo test --release -- --nocapture  # with test output
```

The integration tests build as a few dozen binaries, not one per file. `Cargo.toml` sets
`autotests = false`, so a new `tests/*.rs` file is not built or run until it is registered, in
one of two ways:

- Add a `#[path = "<file>.rs"]` line and a `mod <stem>;` line to the `tests/group_*.rs` file for
  its subject. Its tests then run inside that group binary as `<stem>::<test>`.
- Give it a `[[test]]` entry in `Cargo.toml`, with a comment giving the reason, when its verdict
  depends on state shared by the whole process (a `#[global_allocator]`, a global `tracing`
  subscriber, a process-global counter) or on a timing budget.

To run one file inside a group, filter on its stem:
`cargo test --release --test group_<name> -- <stem>::`. A filter is a substring match, so a stem
that ends another file's stem also selects that file's tests; add `--skip <other stem>::` for
each.

Use `--release`: debug builds of this crate are slow and use enough memory that the full suite
becomes impractical.

- **CI runs no tests.** The runner cannot link the test binaries within its memory, so the
  pipeline type-checks the library, the binary and every integration test target (`cargo check --lib --bins --test '*'`) and runs
  nothing. Unit, integration and property tests are run locally. A green pipeline does not mean
  any test passed.
- **Run one `cargo` command at a time against a `target/` directory.** Parallel `--release` links
  add up their peak memory and can exhaust it, which shows as a hang while linking rather than as
  an error.

### Test scenarios

The integration suite covers:

| Area | Scenarios |
|---|---|
| Basic operations | GET (small, medium and large), HEAD metadata, PUT through the proxy |
| Conditional requests | `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`, each with matching and non-matching validators |
| Range requests | Single ranges, mid-file ranges, byte-exact returned content |
| Cache behavior | Repeat requests for hit and miss timing, invalidation when the object changes |
| AWS CLI integration | Standard CLI operations end to end |
| Error handling | 404 responses, invalid conditional headers, recovery from network errors |

### Asserting on conditional requests

Which side answers a conditional request depends on `evaluate_conditions_from_cache`, default
`true`. A test must assert against the mode it configures:

- **Local evaluation (the default):** a GET, range GET or HEAD whose only conditional header is
  `If-Match` or `If-Range` is served from cache when the cached entry matches (see
  [Conditional requests answered from cache](#conditional-requests-answered-from-cache)), with
  no S3 round trip and so no S3-side credential revalidation.
- **Forwarded to S3** (`evaluate_conditions_from_cache: false`, or any request the local path
  declines): S3 decides the condition.

When the request reaches S3, the cache action follows S3's response:

| S3 response | Cache action |
|---|---|
| `200 OK` | Invalidate the old entry, cache the new data |
| `304 Not Modified` | Refresh the TTL, keep the entry |
| `412 Precondition Failed` | No change |

See [Conditional headers handling](CACHE_FRESHNESS.md#conditional-headers-handling) for the full
dispatch matrix.

### Unit tests

Most `src/` modules carry inline `#[cfg(test)]` modules, and `tests/` holds the integration and
property-test files. To count them in the current tree:

```bash
cargo test --release 2>&1 | grep "test result"  # per-binary pass and fail totals
ls tests/*.rs | grep -vc '/group_'  # integration and property-test files
```

The unit tests cover cache operations (get, put, evict), range handling, compression and
decompression, TTL expiry, multipart uploads, error handling, SNI parsing for the HTTPS
passthrough (`tcp_proxy`), shutdown signalling (`shutdown`) and health status aggregation
(`health`). The property-based tests (quickcheck) cover cache consistency, range merging and the
compression round trip.

### Coverage

Coverage is measured locally with [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov)
(LLVM source-based instrumentation):

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
cargo llvm-cov --release --workspace  # text summary in the terminal
cargo llvm-cov --release --workspace --html  # browsable report under target/llvm-cov/html
```

Coverage uses its own instrumented build, so it does not reuse the `target/` artifacts of a
normal build or test run.

### Integration tests

- `integration_test.rs`: end-to-end scenarios
- `cache_statistics_test.rs`: metrics validation
- `multi_instance_integration_test.rs`: coordination between proxies sharing a cache
- `s3_integration_test.rs`: S3 API compatibility

### Benchmarks

`tests/connection_keepalive_benchmark.rs` holds benchmark-style tests of connection pooling:

```bash
cargo test --release --test connection_keepalive_benchmark -- --nocapture
```

## Debugging tips

### Enable debug logging

```bash
RUST_LOG=debug cargo run --release -- -c config.yaml
```

### Monitor cache operations

```bash
tail -f ./tmp/logs/app/*/s3-proxy.log.* | grep "cache HIT\|cache MISS\|Part cache"  # cache hits and misses, part cache included
watch -n 1 'curl -s http://localhost:9090/metrics | jq .cache'  # the cache section of /metrics
```

### Inspect cache files

```bash
find ./tmp/cache/metadata -name "*.meta" | head -10  # list metadata files
cat ./tmp/cache/metadata/my-bucket/a7/3f2/object.meta | jq .  # view one
ls -lh ./tmp/cache/ranges/my-bucket/a7/3f2/  # its range files
```

### Profiling

```bash
cargo flamegraph --bin s3-proxy -- -c config.yaml  # CPU
valgrind --tool=massif ./target/release/s3-proxy -c config.yaml  # memory
```

### Test specific scenarios

These assume the proxy runs in proxy-only mode on port 3128 on the same host (the local setup in
[Proxy-only mode](GETTING_STARTED.md#proxy-only-mode-no-sudo-required)). Adjust the port if you
run the HTTP listener on port 80.

```bash
export HTTP_PROXY=http://127.0.0.1:3128
export AWS_ENDPOINT_URL_S3=http://s3.us-east-1.amazonaws.com  # the bucket's Region
aws s3 cp s3://bucket/key ./test1.txt  # first read: a cache miss
aws s3 cp s3://bucket/key ./test2.txt  # second read: a cache hit
aws s3api get-object --bucket bucket --key key --range bytes=0-1023 ./range.bin  # range request
aws s3api get-object --bucket bucket --key large-file.bin --part-number 1 ./part1.bin  # part number request
aws s3 cp large-file.zip s3://bucket/key  # multipart upload
```

## Code style guidelines

### Rust conventions

- Format with `rustfmt` and lint with `clippy`.
- Follow the Rust API guidelines.
- Return `Result<T>` rather than panicking, and propagate errors with `?`.

### Error handling

```rust
// Good
match operation().await {
    Ok(result) => { /* handle success */ }
    Err(e) => {
        error!("Operation failed: {}", e);
        return Err(e);
    }
}

// Bad
let result = operation().await.unwrap();
```

### Logging

```rust
// Use structured logging
info!(
    "Cache hit: cache_key={}, range={}-{}, size={}",
    cache_key, start, end, size
);

// Include context in errors
error!(
    "Failed to store range {}-{} for key {}: {}",
    start, end, cache_key, e
);
```

### Async patterns

```rust
// Use tokio::spawn for concurrent tasks
let handle = tokio::spawn(async move {
    // background work
});

// Use tokio::select! for concurrent operations
tokio::select! {
    result = operation1() => { /* handle result1 */ }
    result = operation2() => { /* handle result2 */ }
}
```

## Contributing guidelines

### Before submitting

1. Run all tests: `cargo test --release`
2. Run clippy: `cargo clippy --all-targets --all-features -- -D warnings`
3. Format the code: `cargo fmt`
4. Update the documentation
5. Add tests for new features
6. Update `CHANGELOG.md`

### Pull request checklist

- [ ] Tests pass
- [ ] Documentation updated
- [ ] Code formatted
- [ ] Clippy warnings addressed
- [ ] Performance impact assessed
- [ ] Breaking changes documented

## References

### External documentation

- [Hyper 1.0 guide](https://hyper.rs/guides/1/)
- [Tokio tutorial](https://tokio.rs/tokio/tutorial)
- [Rust async book](https://rust-lang.github.io/async-book/)
- [Amazon S3 API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/)

### Related documentation

- [Caching](CACHING.md): what is cached and how to control it
- [Compression](COMPRESSION.md): the compression decision and format
- [Connection pooling](CONNECTION_POOLING.md): upstream connections and IP distribution
- [Error handling](ERROR_HANDLING.md): errors and recovery
- [Configuration reference](CONFIGURATION.md): every configuration field

### Research papers

- TinyLFU: "TinyLFU: A Highly Efficient Cache Admission Policy" (Gil Einziger, Roy Friedman, 2017)
- GDSF: "GreedyDual-Size: A Cost-Aware WWW Proxy Caching Algorithm" (Pei Cao, Sandy Irani, 1997)
- BLAKE3: "BLAKE3: One Function, Fast Everywhere" (Jack O'Connor et al., 2020)
