# Cache internals

This page describes how Hybrid Cache for Amazon S3 lays out its cache on disk and in memory: the two tiers, the sharding
scheme, the file formats, and the background work that maintains them. It is the implementation
view, for developers and for operators debugging a cache. For the operator view of the same
machinery on a fleet, see [Running a fleet on one cache volume](SHARED_STORAGE.md). For which
reads hit which tier, see [Cache read paths](CACHE_READ_PATHS.md). For what is cached at all,
start at [Caching](CACHING.md).

## Two-tier cache system

1. **RAM cache** (optional, first tier)
   - In-memory cache for hot objects, HEAD metadata and range data
   - Size limit `max_ram_cache_size`, default 512 MiB
   - Eviction: TinyLFU, decayed-frequency scoring (see [Eviction algorithms](EVICTION.md#eviction-algorithms))
   - Promotion from disk keeps the stored compression, so no decompress and recompress cycle runs
   - Size limits apply to compressed data, so a large compressible file can still be cached
   - **RAM metadata cache**: holds `NewCacheMetadata` objects so HEAD and GET requests read fewer `.meta` files from disk
   - **Range data**: both the streaming and the buffered paths check the RAM cache before disk, and promote disk hits to it (key format: `{cache_key}:range:{start}:{end}`)
   - Objects cached by PUT are stored on disk only, not in the RAM cache
   - **Sharded for concurrency**: the RAM cache is split into `ram_cache_shard_count` independent shards (default 8), each guarded by its own `tokio::sync::RwLock`. A key maps to a shard by `blake3(cache_key) % shard_count`. Reads for keys in different shards run in parallel without contention, and concurrent reads of the same key share a read lock. Per-shard capacity is `max_ram_cache_size / effective_shard_count`, where the effective count may be clamped below the configured value to honor the 64 MiB admission ceiling. See [RAM cache concurrency model](#ram-cache-concurrency-model) and [`ram_cache_shard_count`](CONFIGURATION.md) for tuning.

2. **Disk cache** (second tier)
   - File-based persistent cache, which a fleet can share on one cache volume
   - Eviction: TinyLFU, decayed-frequency scoring, per range rather than per object (see [Eviction algorithms](EVICTION.md#eviction-algorithms))
   - LZ4 compression (see [Compression](COMPRESSION.md))
   - Range storage: `.meta` (metadata) plus `.bin` (range data)

## RAM cache concurrency model

The RAM cache's read path scales with concurrent connections instead of serializing all reads
through one lock.

**Sharding.** The cache is split into `ram_cache_shard_count` shards (default 8). Each shard owns a disjoint subset of keys and has its own `tokio::sync::RwLock`, eviction state and capacity budget (`max_ram_cache_size / effective_shard_count`, after the admission-ceiling clamp). A key is routed to its shard with `blake3(cache_key) % shard_count`, the same hash function used for disk-path sharding. GETs and PUTs lock only the target shard, so operations on keys in different shards never contend.

**Zero-copy reads.** Cached bytes are stored as `Arc<Bytes>`. A `get()` returns a reference-count increment (O(1)), not a copy of the object data. The shard lock is released before any LZ4 decompression or response-body construction, so the lock is held only for the pointer clone. Peak per-request memory for a cache hit is one copy of the decompressed object.

**Async-aware locking.** Each shard uses `tokio::sync::RwLock`, so a read under contention yields to the Tokio runtime instead of blocking the OS worker thread. Reads take the lock in shared mode; PUTs and eviction take it in exclusive mode. Unlike `std::sync::Mutex`, this lock does not poison on a panic in a request handler.

**Read-only access tracking.** Per-entry `last_accessed` and `access_count`, plus the cache-wide hit and miss counters, are `AtomicU64` fields updated through the shared read lock. The read path never takes a write lock. There is no ordering structure to maintain: when a `put()` needs room, it scans its shard and evicts the entry with the lowest TinyLFU score, computed from those two atomics.

**Shard skew.** Each shard evicts against its own capacity slice, so an uneven key distribution or a few very large objects can fill one shard while others stay underused, and the RAM cache then holds less than its configured maximum. BLAKE3 distributes keys uniformly, so skew is small across many keys. Workloads dominated by a few very large hot objects should size `max_ram_cache_size` with headroom or lower `ram_cache_shard_count`. The 64 MiB ceiling guarantees that a large entry is admitted, not that it stays: keeping `N` such entries resident at once needs `max_ram_cache_size >= N * 64 MiB`. See [`ram_cache_shard_count`](CONFIGURATION.md) for tuning.

## RAM-disk cache coherency

When both the RAM and disk caches are enabled, RAM cache hits are batched and written to disk
metadata, so eviction decisions see accurate access statistics.

**RAM range hits are verified against the shared `.meta`, lazily and per key
(`ram_cache_verification_interval`).** A RAM range hit within the configured interval (default
1s) is served straight from the RAM entry with no read of the cache volume. Past the interval,
the proxy reads the shared `.meta` once (concurrent hits on the same key share at most one read
per interval), compares its ETag with the RAM entry's own, and:

- on a match, refreshes the RAM entry's verification timestamp and serves it;
- on a mismatch, invalidates the key's RAM ranges (`CacheManager::invalidate_ram_ranges`) and
  falls through to disk;
- if the shared `.meta` is gone, treats it as a miss (invalidates and falls through);
- on a transient read failure, serves the RAM entry unverified rather than invalidating it,
  since a failed read is not evidence of staleness.

What bounds RAM staleness overall:

- Staleness relative to the shared `.meta`'s own ETag is bounded by
  `cache.ram_cache_verification_interval` (default 1s) for RAM range data.
- Staleness relative to the fleet's own knowledge is bounded by
  `cache.metadata_cache.refresh_interval` (default 5s) for a verdict derived from a copy in the
  RAM metadata cache, a different cache from the one above.
- Staleness relative to S3 is bounded by the per-key `get_ttl`, nested inside both of the above.
  `get_ttl: 0` opts a key out of RAM range caching and only that: it forces
  `ram_cache_eligible` to `false`, and each of the three RAM range serve entry points skips the
  lookup, including for bytes promoted before the rule was tightened. It does not reach the RAM
  metadata cache, whose consumers carry their own bounds: a HEAD is bounded by `head_ttl`, and a
  Mode B `If-Match` verdict by the live `get_ttl` and the entry's stored expiry. `get_ttl: 0` is
  an authorization boundary as much as a freshness one, so a prefix that needs every read to
  reach S3 must set `head_ttl: "0s"` as well. See
  [RAM cache coherency contract](CACHE_FRESHNESS.md#ram-tier-coherency-contract).
- An ETag stored bare and the same ETag seen quoted are one ETag. Every comparison on this path
  removes one balanced pair of quotes before comparing, so an entry whose stored form differs
  from the wire form only in quoting is not treated as superseded. An empty or quote-only value
  on either side still compares as a mismatch.
- `CacheManager::invalidate_ram_ranges` also clears the RAM ranges at every other stale-object
  invalidation site (an ETag mismatch detected elsewhere, an explicit DELETE). Verification is
  one more caller of the same mechanism.

See [Cache freshness](CACHE_FRESHNESS.md) for the operator-facing contract.

**Error handling.** If disk cache storage fails because of lock contention or an I/O error, the
operation returns an error rather than succeeding silently, so the RAM and disk caches do not
diverge.

### Access statistics propagation

RAM cache hits update access statistics, which reach disk metadata through the journal system:

```text
RAM hit → the stored range's bounds are queued for one background task that records them into CacheHitUpdateBuffer.record_access_update()
        (a page-widened hit's ranges come from the RAM metadata cache, else the .meta, else this proxy's fill-visibility tracker before the fold)
        (a disk hit reaches the same call through DiskCacheManager.record_range_access())
        → CacheHitUpdateBuffer buffers the update in RAM
        → periodic flush (every 5s, fixed) writes to the proxy's own journal file
        → JournalConsolidator applies journal entries to metadata files
```

The flush cadence is a fixed 5 seconds, set where the background task is spawned in `main.rs`,
and no configuration field changes it. In particular `cache.ram_cache_flush_interval` (default
`10s`) does not; see [RAM-disk cache coherency](CONFIGURATION.md#ram-disk-cache-coherency) for
what that field governs.

Disk eviction needs these statistics. Without them, entries read often from RAM would look cold
on disk, so hot data could be evicted from disk while RAM still served it, and the read after
RAM evicted the entry would miss.

### Journal-based access tracking

The journal system records accesses on a cache volume without races:

- **RAM buffering**: updates collect in `CacheHitUpdateBuffer`, so most accesses cost no disk I/O
- **Per-proxy journals**: each proxy writes to its own journal file, so writers never contend
- **Background consolidation**: `JournalConsolidator` applies the updates under the key's lock
- **Atomic updates**: lock acquisition with retry keeps updates consistent on NFS

### Configuration options

Journal-based metadata writes are always on; there is no `enabled` field.
[Running a fleet on one cache volume](SHARED_STORAGE.md) is the operator guide to coordination:
the mount requirements, journals, distributed eviction, size tracking, the validation scan and
the failure modes. For field types, defaults and valid ranges, see the
[configuration reference](CONFIGURATION.md#multi-instance-coordination).

### Cost of verification and flushes

Verification:

- Reads only the metadata file, not range data.
- Runs at most once per key per interval.
- Is neither bounded nor timed. On a volume slow enough to make the default interval a bad
  trade, the read's latency lands on whichever request triggered the check and is counted in
  that request's access-log `total_time`; no metric moves and no log line names it. Raising
  `cache.ram_cache_verification_interval` reduces how often a check happens, not how long one
  takes.

Batch flush:

- Processes all pending updates in one pass.
- Appends them as one write to the proxy's journal, so there is no per-key step and no file
  lock. Applying them to the `.meta` files is the consolidator's work, and it takes the key's
  metadata slot lock there; neither duration is measured or bounded.
- Logs its entry count, duration and error count at `debug` level, so a deployment running at
  `info` has no record of a flush.

### Monitoring metrics

Neither verification nor the batch flush is counted. `/metrics` builds its `cache` section from
a fixed field list, and no verification or flush counter is on it; the dashboard API does not
carry one either. What is available:

- `consolidation.consolidation_count` and `consolidation.last_consolidation_timestamp` observe
  the last step of the chain above, the consolidator applying journal entries to `.meta` files.
  Both are read from the shared `size_state.json`, so they describe the fleet rather than one
  proxy. A `last_consolidation_timestamp` that stops advancing means updates are accumulating in
  journals unapplied. Neither says anything about the RAM buffer or the flush that feeds the
  journal.
- The flush's own entry count, duration and error count exist only in the application log, at
  `debug` level.
- A verification mismatch logs at `debug` level and increments nothing. The RAM lookup before it
  has already counted a hit by then, so in `cache.ram_cache_hits` an entry found and then
  rejected on ETag looks the same as one that was served.

See the [metrics reference](METRICS_REFERENCE.md) for the published field list.

### Troubleshooting

`RAM entry etag mismatch` in the application log (`debug` level) is the only signal that
verification is rejecting entries:

- The disk cache may be too small, so entries are evicted from disk while still in RAM.
  Consider a larger disk cache.
- Check for external changes to cache files.
- Lower `cache.ram_cache_verification_interval` to shorten how long a superseded RAM entry can
  be served before a check catches it, at the cost of one more cache volume read per key per
  interval.

Batch flush errors:

- Check free disk space.
- Check the permissions on `cache_dir`.
- Check for file locking problems (NFS configuration).
- Read the application log at `debug` level; no counter records these errors.

## RAM metadata cache

The RAM metadata cache keeps `NewCacheMetadata` objects in memory so HEAD and GET requests read
fewer `.meta` files from disk, while staying consistent with the files on disk.

### Architecture

- **RAM metadata cache**: an in-memory LRU cache of `NewCacheMetadata` objects. It evicts the
  least recently used entry when it reaches `max_entries`; see
  [Eviction algorithms](EVICTION.md#eviction-algorithms).
- **Disk metadata**: persistent `.meta` files in the `metadata/` directory.
- **Shared model**: HEAD and GET share the same metadata file, with independent TTLs.

```rust
MetadataCacheEntry {
    metadata: NewCacheMetadata,  // Full metadata including HEAD and GET fields
    loaded_at: Instant,          // When loaded from disk
    disk_mtime: Option<SystemTime>, // Disk file modification time
    last_accessed: Instant,      // For LRU eviction
}
```

Design rules:

- HEAD and GET metadata are stored in the same `.meta` file.
- HEAD expiry is the `head_expires_at` field. GET expiry is the object-level `expires_at`
  field, which covers every cached range of the object.
- HEAD expiry does not delete the file, because ranges may still be valid.
- GET expiry does not affect HEAD validity.

### Request flows

HEAD request:

```text
HEAD Request → Check RAM MetadataCache
            → If HIT and not stale (loaded_at < refresh_interval):
                → Check head_expires_at in cached metadata
                → If not expired: return headers (RAM HIT)
                → If expired: fetch from S3, update cache
            → If MISS or stale:
                → Read .meta from disk (metadata/ directory)
                → Update RAM cache
                → Check head_expires_at
                → If not expired: return headers (DISK HIT)
                → If expired: fetch from S3, update both caches
```

GET or range request:

```text
GET Request → Check RAM MetadataCache for NewCacheMetadata
           → If HIT: check if requested range exists and the object is not expired
               → If valid range: serve from range file
               → If no range: re-read the shared .meta first (a RAM entry is not
                 authoritative for the ABSENCE of ranges, since another proxy
                 may have added them), then
                   → If the shared .meta covers the request: serve from range files
                   → Otherwise: fetch from S3, cache as new range
           → If MISS: read .meta from disk or fetch from S3
           → Update range access_count, last_accessed
           → Serve response
```

### TTL independence

| Cache layer | TTL field | Default | Purpose |
|-------------|-----------|---------|---------|
| RAM metadata cache | `refresh_interval` | 5s | How often to re-read from disk |
| HEAD (disk) | `head_expires_at` | 1 minute | When to re-fetch HEAD from S3 |
| GET (disk) | object-level `expires_at` | 1 day | When to revalidate the object's ranges with S3 |

HEAD expiry does not delete the `.meta` file; it means the HEAD needs revalidation from S3.
Ranges can still be valid and served from the cache.

### Concurrent misses

Lookups take no per-key lock. Concurrent requests that miss the RAM metadata cache for the same
key each read the `.meta` from disk, and each stores the result; once one has, later requests
are RAM hits until `refresh_interval` passes.

### Stale file handle recovery

The cache retries stale file handle errors, which are common on NFS:

```text
Read attempt → ESTALE error → Retry with backoff (up to 3 times)
            → If persists: treat as a cache miss and fetch from S3 (the .meta is not deleted)
```

The retry count and backoff (50 ms, 100 ms, 150 ms) are fixed.

### Metadata file size cap and self-heal

The proxy enforces a maximum `.meta` file size (`max_metadata_file_bytes`, default 4 MiB) on
range lookups in the disk cache, validation against HEAD, range invalidation, and the RAM-hit recorder's extent read. A file over the cap is rejected after a `stat`, without being read, so a very large metadata file,
such as one that carries the object body inline, is not loaded there. Every other `.meta` read ignores the cap, including the lookup that serves a GET or HEAD when an object's metadata is not in the RAM metadata cache, the HEAD-entry lookup, the RAM-hit ETag check, TTL refresh, the write cache, eviction and journal consolidation.

When a `.meta` file is classified as confidently corrupt (oversize, legacy schema, or a stable
parse failure after bounded retries), the proxy heals it: the S3 refetch writes a fresh valid
`.meta` through an atomic temporary file and rename, overwriting the corrupt file. A transient
or partial read (a file mid-write on NFS) does not delete the file. See
[Error handling](ERROR_HANDLING.md) for the full classification and how healing works.

Those capped reads run inside `spawn_blocking` under a concurrency limit
(`metadata_io_concurrency`, default 32), so one slow or large `.meta` file does not starve the
async runtime there. Each of the proxy's disk cache handles holds its own limit, so the total for
one proxy can exceed 32. Every other `.meta` read takes no permit.

### Configuration

```yaml
cache:
  metadata_cache:
    enabled: true
    refresh_interval: "5s"      # How often the RAM metadata cache re-reads from disk
    max_entries: 100000         # Maximum entries in the RAM metadata cache

  # HEAD-specific TTL (independent of GET TTL)
  head_ttl: "60s"               # How long a HEAD is valid before an S3 re-fetch
```

Notes:

- The RAM metadata cache is separate from the RAM cache, which stores range bytes.
- The HEAD TTL is independent of the GET TTL.
- The refresh interval controls disk re-reads, not S3 re-fetches.

### Cache statistics

RAM metadata cache operations are included in the cache statistics:

```json
{
  "metadata_cache": {
    "entries": 5000,
    "hits": 125000,
    "misses": 8500,
    "stale_refreshes": 2100,
    "evictions": 450,
    "stale_handle_errors": 3
  }
}
```

- `entries`: current number of entries in the RAM metadata cache
- `hits`: hits served from RAM
- `misses`: misses that needed a disk read
- `stale_refreshes`: entries refreshed because they were stale
- `evictions`: entries evicted for capacity
- `stale_handle_errors`: stale file handles (`ESTALE`) seen while reading `.meta` files on this
  proxy, one per retry; the same figure as `cache.metadata_read_stale_handles_total` on
  `/metrics`. Investigate a value above zero.

### Monitoring and logging

Cache operations (`debug` level):

```text
DEBUG Metadata cache hit (RAM) for key: bucket/object.txt
DEBUG MetadataCache entry is stale cache_key=bucket/object.txt age_ms=5012
DEBUG MetadataCache evicted entry (LRU) evicted_key=bucket/old-object.txt
```

Stale handle recovery:

```text
WARN [METADATA_BLOCKING] Stale file handle, retrying: path=..., attempt=1/3, error=...
WARN [METADATA_BLOCKING] Failed to read metadata file after retries: path=..., error=...
```

### Memory use

About 15 to 25 MiB for 10,000 entries, so roughly 150 to 250 MiB at the default `max_entries` of
100,000.

### Use cases

Load balancer health checks:

```text
Scenario: a load balancer checks object existence every 5 seconds
Without the RAM metadata cache: each check reads from disk
With the RAM metadata cache: the first check loads the entry into RAM; later checks are served
from RAM, with one disk re-read per refresh_interval
```

A fleet on an NFS cache volume:

```text
Scenario: several proxies share an NFS cache volume
Challenge: stale file handles when other proxies modify files
Solution: automatic retry with backoff; a read that still fails is treated as a cache miss
```

## Streaming response architecture

The proxy streams S3 responses to the client rather than collecting them in memory first. The
one exception is a partial cache hit that is merged (below): its fetched gaps are held in memory
with the cached bytes, and a merge is attempted only when the requested range and every cached
extent it overlaps are each at most 8 MiB (uncompressed). A larger, partly cached range is
refetched from S3 through the streaming path instead.

`cache.disk_streaming_threshold` (default 1 MiB) is a separate setting for the disk read path:
it decides whether a cached range is read fully into memory or streamed from disk in chunks.
See [Disk streaming threshold](CONFIGURATION.md#disk-streaming-threshold).

How it works:

1. **TeeStream for simultaneous streaming and caching.** S3 responses are wrapped in a
   TeeStream. Data streams to the client as it arrives and is sent at the same time to a
   background task for caching, so the whole response is never buffered in memory.
2. **Effect.** Memory per transfer is proportional to the chunk size, not the object size, and
   the first byte reaches the client as soon as it arrives from S3.

Complete cache miss (streaming):

```text
S3 Response (Incoming)
        │
        ▼
   TeeStream
   ┌───┴───┐
   │       │
   ▼       ▼
Client  Background Task
        (accumulates & caches)
```

Partial cache hit (buffered):

- Some ranges are cached and some need fetching.
- Only the missing ranges are fetched from S3, then merged with the cached bytes in one buffer
  before sending.
- This happens only when the requested range and each overlapped cached extent are at most
  8 MiB. Otherwise a GET refetches the whole requested range through the streaming path and
  caches it as one range, and any other request is forwarded to S3 uncached.

Cache hit (range requests):

```text
Range Request → Check RAM cache (key: {cache_key}:range:{start}:{end})
             → RAM HIT: Serve as buffered 206 response (no disk I/O)
             → RAM MISS + range < streaming threshold: Buffered path
                  → Load from disk, serve, promote to RAM cache
             → RAM MISS + range >= streaming threshold: Streaming path
                  → Stream from disk in 512 KiB chunks
                  → Collect chunks during streaming
                  → Promote to RAM cache after completion
                  → Skip promotion if range exceeds max_ram_cache_size
```

- The RAM cache is checked before the streaming or buffered decision for every range request.
- Both paths promote disk hits to the RAM cache for later requests.
- Dashboard statistics count RAM cache hits and misses from both paths.

Configuration:

- Disk streaming threshold: `cache.disk_streaming_threshold`, default 1 MiB, based on the
  Content-Length header. It governs the disk read path only; the S3 response path has no
  threshold and streams whatever the size.
- Chunk size for disk streaming: 512 KiB (as in the flow above).

### Copies and allocation

- **No copy on cache writes without compression.** When compression is disabled, cache writes
  use the incoming buffer directly; there is no `data.to_vec()` copy.
- **Journal-only metadata on cache misses.** Range metadata written on a cache miss goes to the
  journal only, so proxies caching the same object do not contend on its metadata lock. The
  consolidator merges the entries later without blocking the stream.
- **Reference-counted bytes.** TeeStream uses `Bytes::clone()`, which increments a reference
  count rather than copying, so the client stream and the cache writer share one copy of the
  data.
- **Asynchronous cache writer.** Cache writes happen in a background task, so they do not block
  the client stream. Chunks go to the writer over a bounded channel, tried without waiting
  first. If the channel is full, the client stream waits for space (backpressure), so the
  cached copy is not truncated; if the writer has gone away, the client stream continues
  without caching.

## Range storage architecture

All cached data uses the same storage format, whatever its source:

- **PUT**: stored as range 0-N at once
- **GET**: full objects and partial ranges stored the same way
- **Multipart uploads**: parts assembled into ranges on completion

As a result:

1. PUT-cached and multipart-cached objects serve range requests at once, without an S3 fetch.
2. A TTL transition rewrites the `.meta` only; range files are never copied.
3. One storage format serves every cache type.

### Partial range prefix salvage

A streamed range cache write can end before the full requested range arrives: the client
canceled the transfer, or the mid-stream idle watchdog (`connection_pool.upstream_idle_timeout`)
aborted a stalled upstream. With `cache.partial_range_commit_ratio` (default `0.5`), the proxy
commits the received prefix as a smaller valid range `[start, start + received - 1]` when at
least that fraction of the requested bytes arrived in order.

Clients that open many parallel range connections, such as the AWS CRT transfer client, cut
ranges short often: the idle watchdog or the client's own part reassignment can end any of them.
The salvaged range is recorded with its true byte bounds, so it is never served as the full
requested range. A later request for the missing tail fetches it from S3 and merges it with the
cached prefix (see [Range merging](CACHE_READ_PATHS.md#intelligent-range-merging)). `1.0` keeps
only exact ranges (any short range is discarded); `0.0` commits any non-empty prefix. The write
cache is unaffected: a truncated upload is never cached. See
[Partial range commit ratio](CONFIGURATION.md#partial-range-commit-ratio) for tuning.

### Range lookup purpose and stored freshness

`DiskCacheManager::find_cached_ranges` takes a required `RangeLookupPurpose` and returns a
`RangeLookupResult`: the coverage plus a `StoredFreshness` marker.

The stored-expiry check (`SystemTime::now() > metadata.expires_at`) sits at one point, after both
metadata arms converge, so the policy is the same whether the metadata was read from disk or
supplied preloaded by a caller. Past that point:

- `FreshServe` returns empty coverage.
- `RevalidationCandidate` continues the overlap calculation and marks the result
  `StoredFreshness::Expired`.

The two purposes share the overlap calculation and differ only in the expiry decision, so they
cannot disagree about what is cached.

The journal fallback (`find_pending_journal_ranges`, used when metadata is absent or has no
overlapping extent) is reported `Fresh`, because a journal record carries no `expires_at` that
could have passed.

The marker travels with the coverage, rather than being recomputed by the caller, because the
caller may hold a different copy of the metadata (its own preloaded one, or a second disk read).
Deciding freshness from that copy would decide it from a value other than the one the lookup
branched on.

[Cache read paths](CACHE_READ_PATHS.md) has the per-call-site assignment and the serve-authority
table.

### Range file identity

Each range the proxy caches records the size and modification time of its file
(`file_identity` in the range's entry). The writer takes them from its own file descriptor after
the data is synced and before the file is renamed into `ranges/`. A read serves the range only
from a file whose size equals the recorded size and whose modification time is within 1 ms of
the recorded time, checked on the descriptor it reads from when it opens the file. A mismatch is
a miss: the read is answered from S3, the range is removed and its file deleted, and the next
read caches it again. A frame's checksum shows that the file is intact, not that it is the file
its metadata describes, so this check covers what the checksum cannot.

The identity is optional. A range without one, such as a range cached or last rewritten by an earlier release, is served
unchecked, except in the two shapes in
which an unchecked range's bytes can be wrong:

- A range that starts at the object's first byte and ends before its last is not served, because
  its bytes could belong to another offset.
- A range of at most 64 KiB whose bytes are an S3 error document is not served. The proxy treats
  bytes as an error document when they start with `<?xml` or `<Error>` and hold both `<Error>`
  and `<Code>` in their first 512 bytes.

In both cases the read is answered from S3, the range is removed and its file deleted, and the
refill is cached with an identity.

The identity also settles two proxies caching the same range at once. When two `Add` or `Update`
lines in one consolidation pass name the same range with different ETags, the consolidator
removes the range from the metadata and deletes its file, unless the object's metadata already
existed and both lines record a file identity. When the lines agree on the ETag but record
different identities, it lists the one whose identity matches the file now at the path, and
removes the range when neither does.

## Cache entry structure

Each cached object has:

- **Metadata** (a `.meta` file in `metadata/`):
  - Object metadata: ETag, Last-Modified, Content-Length, Content-Type
  - HTTP headers
  - Range index: the cached ranges, with start, end and file path
  - Upload state: Complete, InProgress or Bypassed
  - Cumulative size: running total for multipart uploads
  - Parts list: temporary storage for multipart parts, cleared on completion
  - Expiration timestamps
  - Typically under 100 KiB, even with hundreds of cached ranges
- **Range data** (`.bin` files in `ranges/`):
  - Compressed binary data for each cached range
  - Full objects stored as range 0-N (from PUT or a completed multipart upload)
  - Partial ranges stored separately (from GET with a Range header)
  - Multipart parts stored as ranges after completion

## Cache directory structure

The disk cache uses bucket-first, hash-based sharding with range storage, which keeps the number
of entries in any one directory bounded (see [Hash-based sharding](#hash-based-sharding)).

### Directory hierarchy

```text
cache_dir/
├── metadata/                   # Metadata files (HEAD and GET share one .meta)
│   ├── {bucket}/              # S3 bucket name (first level)
│   │   ├── {XX}/              # First 2 hex digits of BLAKE3(object_key)
│   │   │   ├── {YYY}/         # Next 3 hex digits of BLAKE3(object_key)
│   │   │   │   ├── {object_key}.meta      # JSON metadata and range index (<100 KiB)
│   │   │   │   └── {object_key}.wtok      # Write-order marker: the latest write or delete's token (tens of bytes)
│   │   │   └── ...
│   │   └── ...
│   ├── _meta_locks/           # Fixed set of metadata lock slot files, never deleted
│   │   └── {NNNN}.lock        # Slot chosen by hash of the cache key
│   ├── _journals/             # Per-proxy journals of metadata updates
│   │   └── {instance_id}.journal
│   ├── _write_ledger/         # Write-cache records (uploaded, not yet read); see EVICTION.md
│   │   ├── {bucket}/{XX}/{instance_id}.ledger  # One file per cache shard per proxy
│   │   └── {instance_id}.ledger.mark           # Per-proxy record-sequence floor
│   └── _shard_state/          # Write-cache size accounting and eviction coordination
│       ├── checkpoints/{bucket}/{XX}.json       # Per-shard write-cache figure and its cut
│       ├── staged_authority_epoch.json          # Which producer owns the figure on this volume
│       ├── staged_pending/{instance_id}.pending # Credits that landed after their shard's cut
│       ├── in_progress_parts/{instance_id}.inprog # Multipart part bytes each proxy has written
│       ├── eviction_worklist/, eviction_claims/, eviction_generation.json  # Shared eviction work list
│       ├── eviction_reclaims/{instance_id}.json # What each proxy's count-driven passes reclaimed
│       └── claims/, ready/                      # Shard leases, readiness records
├── ranges/                     # All cached data, stored as binary ranges
│   ├── {bucket}/              # S3 bucket name (first level)
│   │   ├── {XX}/              # First 2 hex digits of BLAKE3(object_key)
│   │   │   ├── {YYY}/         # Next 3 hex digits of BLAKE3(object_key)
│   │   │   │   ├── {object_key}_0-8388607.bin       # Range data
│   │   │   │   ├── {object_key}_8388608-16777215.bin
│   │   │   │   └── {object_key}_16777216-25165823.bin
│   │   │   └── ...
│   │   └── ...
│   └── ...
├── mpus_in_progress/           # Multipart uploads in progress (when the write cache is enabled)
├── locks/                      # Coordination locks
│   ├── global_eviction.lock   # Eviction lock
│   └── initialization.lock    # Cache-initialization lock
└── size_tracking/              # Cache size tracking files
    ├── size_state.json        # Authoritative size state (updated by the consolidator)
    ├── delta_{instance_id}_{seq}.json  # One new file per accumulator flush
    ├── validation.json        # Last validation scan's results (size scanned, drift, duration)
    ├── schedule.json          # Validation scan schedule state (mode, cursor, scan rate)
    └── validation.lock        # Validation lock file
```

Every successful PUT, CompleteMultipartUpload and DELETE of an object writes its
`{object_key}.wtok` file, so a background cache store can tell whether a newer write of the same
object was acknowledged after its own, and decline to publish. When the cache volume refuses the
key's metadata lock, the write still succeeds and the proxy retries the file in the background;
[Write-through ordering](CACHING.md#write-through-ordering) says what holds until the retry lands
and when a write is not retried. A GET or HEAD miss also creates the file when the key has none,
and removes it again when S3 answers 404 or 403 or the request fails, unless another miss on the
same proxy read it meanwhile and stored its response. The write's own store removes the file when
it publishes; otherwise the ledger-repair rotation removes it on its next visit to the shard once
the file is an hour old. Reads and cache scans ignore `.wtok` files: an object with only a
`.wtok` has nothing cached.

### Bucket-first organization

Cache key format: `{bucket}/{object_key}`.

The cache key always starts with a `{bucket}/` prefix. For path-style requests
(`s3.{region}.amazonaws.com/{bucket}/{key}`) the bucket is taken from the first path segment; for
the virtual-hosted styles (regional, regional dualstack, accelerate, accelerate dualstack, and
legacy global) it is extracted from the leading label of the Host header. Every style produces
the same cache key for the same bucket and object, so cache entries are shared across
addressing styles (see Regular bucket requests below).

Examples:

- `my-bucket/path/to/object.txt`
- `my-bucket/file.jpg`
- `my.bucket.name/deeply/nested/path/data.bin`

### Object key spelling

The `{object_key}` part of a cache key is the object key S3 reads from the request path, written in one spelling: unreserved characters (`A-Z a-z 0-9 - . _ ~`) and `/` as they are, every other byte as `%XX` with uppercase hex. This is the spelling AWS SDKs and the AWS CLI send. The proxy reads a path the way S3 does, so every spelling of one object gives one cache key:

- `%XX` in either hex case is that byte, and `%2F` is `/`.
- A literal `+` is a space. The object `a+b` is spelled `a%2Bb`, and `a+b` in a path names the object `a b`.
- A raw byte above `0x7F` is `?`, which is how S3 stores an object written through such a path.
- Nothing else changes: `x/../a` and `y//z` are object keys of their own.

A write through any spelling therefore removes the copy a read cached through any other. A path S3 refuses as malformed (a `%` not followed by two hex digits) keeps its literal spelling, which no other request's key can equal. Cache rules (`cache_rules.json` patterns) match this spelling.

### Access point and MRAP cache key prefixing

S3 Access Point and Multi-Region Access Point (MRAP) requests use a different URL structure from regular bucket requests. The endpoint identity appears in the Host header (virtual-hosted style) or the first path segment (path style with an alias), not in the bucket position. The proxy detects these patterns and builds cache key folders with AWS reserved suffixes, so they cannot collide with S3 bucket names.

The AWS CLI uses SigV4A (`AWS4-ECDSA-P256-SHA256`) by default for MRAP requests. The proxy recognizes both SigV4 and SigV4A signatures: signed headers, range signatures, referer injection guards and `aws-chunked` streaming payload detection all work the same for both algorithms.

Per the [S3 bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html), the suffixes `-s3alias` and `.mrap` are reserved by AWS for access point alias names and cannot appear in general purpose bucket names. The proxy appends these suffixes to every AP and MRAP cache key folder, so they never collide with bucket-derived cache keys.

Scope: the proxy caches S3 general purpose buckets only. S3 Tables buckets, S3 directory buckets and S3 Vectors buckets are not supported.

**Virtual-hosted style (AP or MRAP identity in the Host header)**

Regional AP hosts match `{name}-{account_id}.s3-accesspoint.{region}.amazonaws.com`. The proxy extracts `{name}-{account_id}` and appends `-s3alias` to form the cache key folder.

MRAP hosts match `{mrap_alias}.accesspoint.s3-global.amazonaws.com`. The proxy extracts `{mrap_alias}` and appends `.mrap` to form the cache key folder.

```text
Regional AP (virtual-hosted):
  Host: my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com
  Path: /data/file.txt
  Cache key: my-ap-123456789012-s3alias/data/file.txt

MRAP (virtual-hosted):
  Host: mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com
  Path: /data/file.txt
  Cache key: mfzwi23gnjvgw.mrap/data/file.txt
```

The reserved suffixes also prevent collisions between the two types. A regional AP identifier and an MRAP alias that happen to be the same string produce distinct cache key folders:

```text
Regional AP: abc123-s3alias/data/file.txt
MRAP:        abc123.mrap/data/file.txt
```

**Path-style forwarding (AP or MRAP alias in the URL path)**

When the proxy receives a request with a base AP or MRAP domain as the Host and an alias in the first path segment, it detects the alias for logging but forwards the request to S3 as received, plus one `Via` entry. The proxy does not rewrite the host or path: S3 handles path-style AP and MRAP routing itself, and SigV4 signatures stay valid only if the original request is preserved.

The alias appears as the first path segment in the cache key, which separates the namespaces:

```text
AP alias (path-style, forwarded as-is):
  Host: s3-accesspoint.us-east-1.amazonaws.com
  Path: /myname-abcdef123456-s3alias/data/file.txt
  Cache key: myname-abcdef123456-s3alias/data/file.txt
  (alias in first path segment provides namespace separation)

MRAP alias (path-style, forwarded as-is):
  Host: accesspoint.s3-global.amazonaws.com
  Path: /mrymoq6iot5o4.mrap/data/file.txt
  Cache key: mrymoq6iot5o4.mrap/data/file.txt
```

Path-style AP alias requests require the base AP domain to resolve to S3 IP addresses. The proxy uses external DNS, which cannot resolve `s3-accesspoint.{region}.amazonaws.com`, so this pattern does not work in practice. Use the regular S3 endpoint with AP aliases instead (see [Access point alias usage](GETTING_STARTED.md#access-point-alias-usage)).

**Known limitation: ARN-based and alias-based cache keys differ**

The same access point reached through ARN-based virtual-hosted style and through alias-based path style produces different cache key folders. ARN-based access uses the `{name}-{account_id}` identifier from the Host header (with `-s3alias` appended by the proxy), while alias-based access uses the alias string directly (which already contains `-s3alias` but with different metadata characters). The proxy has no AWS credentials and cannot map between these identifiers.

```text
ARN-based (virtual-hosted): my-ap-123456789012-s3alias/data/file.txt
Alias-based (path-style):   my-ap-abcdef123456-s3alias/data/file.txt
```

Both cache key folders end with `-s3alias`, so neither collides with bucket names. The difference caches the same object twice, under two keys; it does not affect correctness.

**Regular bucket requests (cache entries shared across addressing styles)**

Regular bucket requests produce a cache key of the form `{bucket}/{object_key}`. The proxy extracts the bucket from whichever part of the request carries it for the addressing style in use:

- **Path style** (`s3.{region}.amazonaws.com/{bucket}/{key}`, `s3.amazonaws.com/{bucket}/{key}`, `s3.dualstack.{region}.amazonaws.com/{bucket}/{key}`): the bucket is the first path segment.
- **Regional virtual-hosted** (`{bucket}.s3.{region}.amazonaws.com/{key}`, `{bucket}.s3.dualstack.{region}.amazonaws.com/{key}`): the bucket is the leading label of the Host header. Dots are allowed in bucket names under the general purpose naming rules (`my.company.logs.s3.us-east-1.amazonaws.com` parses to bucket `my.company.logs`).
- **S3 Transfer Acceleration** (`{bucket}.s3-accelerate.amazonaws.com/{key}`, `{bucket}.s3-accelerate.dualstack.amazonaws.com/{key}`): the bucket is the leading label of the Host header. Acceleration requires DNS-compliant bucket names, so bucket labels containing dots are rejected as malformed.
- **Legacy global** (`{bucket}.s3.amazonaws.com/{key}`): the bucket is the leading label of the Host header.

Every style produces the same cache key for the same bucket and object. A request for `my-bucket/file.txt` through path-style, regional virtual-hosted, accelerate and legacy global endpoints hits the same cache entry, so a client that uses acceleration for writes and regional virtual-hosted requests for reads (or the reverse) shares one cached copy.

For non-AWS S3-compatible hostnames (MinIO, R2 and others), and any Host the proxy does not recognize as an AWS S3 endpoint, the cache key falls back to the bare normalized path, so the host contributes nothing to it. A path-style request to such a host therefore keys on its first path segment as the bucket, exactly as it does against S3, while a virtual-hosted request to such a host leaves its bucket in the Host header, where nothing reads it into the key.

Two different S3-compatible origins serving the same path therefore resolve to the same cache key, so a distinct hostname does not give a store a cache namespace of its own. The per-bucket origin marker keeps them apart instead. Each bucket's directory in the cache records the origin its entries came from, and a request for that bucket name arriving from a different origin is forwarded to its own origin and served without being cached. To give two stores a cached copy each, address them under distinct bucket names. See [One bucket name at two origins](CACHING.md#one-bucket-name-at-two-origins).

The path-style access point and Multi-Region Access Point keys above are host-free in the same way: the alias in the first path segment is the bucket. Only a virtual-hosted access point or MRAP hostname contributes a prefix (`{name}-{account}-s3alias`, `{alias}.mrap`) from the Host header.

Path resolution:

1. Split the cache key at the first `/` to get the bucket and the object key.
2. Hash the object key (not the bucket) with BLAKE3.
3. Take the first 2 hex digits for the level 1 directory (XX).
4. Take the next 3 hex digits for the level 2 directory (YYY).
5. Build the path: `{type}/{bucket}/{XX}/{YYY}/{filename}`.

Example:

```text
Cache key: my-bucket/photos/vacation.jpg
Bucket: my-bucket
Object key: photos/vacation.jpg
BLAKE3(photos/vacation.jpg) = a7f3c2...
Level 1: a7
Level 2: f3c
Metadata path: metadata/my-bucket/a7/f3c/photos%2Fvacation.jpg.meta
Range path: ranges/my-bucket/a7/f3c/photos%2Fvacation.jpg_0-1048575.bin
```

### Hash-based sharding

BLAKE3 distributes keys uniformly across directories.

Sharding levels:

- Level 1 (XX): 256 directories (00-ff)
- Level 2 (YYY): 4,096 subdirectories per level 1 directory (000-fff)
- Total: 1,048,576 leaf directories per bucket

Capacity:

- Maximum: 10.5 billion files per bucket (at 10,000 files per directory)
- Comfortable: 2.6 billion files per bucket (25% of that, about 2,500 files per directory)
- Example: 100 million objects × 10 ranges = 1.0 billion files, about 954 files per directory

Distribution:

- The same object key in different buckets lands in the same hash directories under different
  bucket directories.
- Different object keys are spread uniformly across hash directories.
- The same cache key always resolves to the same path.

### Directory purpose

- `metadata/{bucket}/{XX}/{YYY}/`: metadata files (`.meta`) holding object metadata, the range index, HEAD TTL fields, upload state and part caching fields, with no embedded binary data. HEAD and GET share the same `.meta` file with independent TTLs.
- `ranges/{bucket}/{XX}/{YYY}/`: all cached data as binary files (`.bin`), including full objects from PUT (stored as range 0-N), partial ranges from GET, and assembled multipart uploads.
- `locks/`: coordination locks for eviction and for cache initialization. Per-key write coordination uses the fixed slot set under `metadata/_meta_locks/` instead.

### Layout properties

1. **Bounded directories**: 1,048,576 leaf directories per bucket keep per-directory entry counts bounded (see Capacity above).
2. **Bucket isolation**: a bucket's cache can be cleared on its own with `rm -rf cache_dir/{type}/{bucket}/`.
3. **Uniform distribution**: BLAKE3 hashing spreads files evenly across directories.
4. **Small metadata**: a `.meta` holds no range bytes, so it stays small (typically under 100 KiB even with hundreds of ranges).
5. **One storage model**: full objects, partial ranges and multipart uploads are all stored as ranges.
6. **Concurrent access**: separate files allow concurrent reads without lock contention.
7. **Lazy directory creation**: hash directories are created on demand, not at startup.
8. **Direct lookups**: a path is computed from the key's hash, so a lookup reads no directory listing.

### Filename conventions

Metadata files: `{sanitized_object_key}.meta`

- The object key is percent-encoded for file system safety.
- Special characters (/, \, :, *, ?, ", <, >, |, %) are encoded.
- Long keys (over 200 characters after encoding) use the BLAKE3 hash as the file name.

Range files: `{sanitized_object_key}_{start}-{end}.bin`

- The same sanitization as metadata files.
- The byte range is appended to the file name.

```text
Object key: path/to/file.txt
Metadata: path%2Fto%2Ffile.txt.meta
Range: path%2Fto%2Ffile.txt_0-8388607.bin

Object key: very/long/path/that/exceeds/200/characters/after/encoding...
Metadata: a7f3c2d1e5b8...9f4a.meta (64 hex chars = BLAKE3 hash)
Range: a7f3c2d1e5b8...9f4a_0-8388607.bin
```

Part number requests: GET requests with a `partNumber` query parameter are cached as ranges.
S3's GetObjectPart responses include a `Content-Range` header (for example
"bytes 0-8388607/5368709120") that gives the exact byte offsets for storing a part as a range.
Each part's byte range is stored in `ObjectMetadata.part_ranges` as a `HashMap<u32, (u64, u64)>`
mapping part number to `(start, end)`. Parts may differ in size. For example, a 23 MiB object
with 3 parts:

- Part 1: (0, 10485759), 10 MiB
- Part 2: (10485760, 15728639), 5 MiB
- Part 3: (15728640, 24117247), 8 MiB

All subdirectories are created lazily on first write, with the right permissions.

## Access tracking

### Overview

The proxy records per-range access counts and timestamps without blocking concurrent reads.
TinyLFU eviction uses them to decide which individual ranges to evict, so hot ranges are retained
even when other ranges of the same object are cold.

### Architecture

Accesses are buffered in RAM, flushed to each proxy's own journal file, and applied to the
`.meta` files by the consolidator. That chain is described above under
[Access statistics propagation](#access-statistics-propagation) and
[Journal-based access tracking](#journal-based-access-tracking); the operator-facing view is in
[Journal-based metadata writes](SHARED_STORAGE.md#journal-based-metadata-writes). There is no
separate access-tracking directory, no time-bucketed log file, and no per-bucket lock.

### How it works

#### Metadata updates

Consolidated access data is written to range metadata:

```json
{
  "ranges": [
    {
      "start": 0,
      "end": 8388607,
      "access_count": 42,
      "last_accessed": "2024-01-15T10:30:00Z",
      "staged": true,
      ...
    }
  ],
  "head_access_count": 15,
  "head_last_accessed": "2024-01-15T10:30:00Z"
}
```

`staged` records whether the range counts toward the write cache, decided once when the range
was written and never re-derived. It is optional: a range without it is classified from the
object's `is_write_cached` flag, not read as `false`.

Membership is per range because credits and debits are per range. A PUT stages one range; a
later GET for a different range of the same object caches that range to the read cache, while
the object stays flagged until it graduates. Only the first range's bytes belong to
`write_cache_size`.

Metadata updates are atomic: a temporary file is written, then renamed. Metadata is updated only
during consolidation, not on every access, so an access costs no `.meta` write of its own.

### Integration with eviction

Eviction scores each range on its own from these statistics (see
[Eviction algorithms](EVICTION.md#eviction-algorithms)). Eviction runs from the consolidation
cycle, so the accesses that cycle applied are in the metadata it reads.

### Coordination across proxies

Coordination is the journal system's: each proxy appends to its own journal file, and the
consolidator applies entries to the `.meta` files under the key's metadata slot lock. See
[Journal-based access tracking](#journal-based-access-tracking) above and
[Journal internals](#journal-internals) below. Consolidation runs on a fixed
`consolidation_interval` on every proxy, with no jitter.

## Journal internals

The operator-facing description of journal-based metadata writes (why they exist, what the lag
means, and how to spot a stalled consolidator) is in
[Journal-based metadata writes](SHARED_STORAGE.md#journal-based-metadata-writes). This section
covers the buffer thresholds, entry formats and apply order.

### Components

`CacheHitUpdateBuffer` (RAM layer):

- Buffers TTL refreshes and access-count updates in RAM
- Flushes to the proxy's journal file every 5 seconds
- Coalesces repeated access updates to one stored range inside a flush window into one entry (increments summed, latest timestamp kept), and repeated TTL refreshes of one object into one entry (latest refresh kept)
- Flushes early when the buffer holds 10,000 distinct entries
- Can be force-flushed at shutdown

Per-proxy journal files:

- Location: `metadata/_journals/{instance_id}.journal`
- Format: newline-delimited JSON, one entry per line
- Append-only writes, with no read-modify-write
- Each proxy writes only to its own journal

Journal entry types:

- `Add`, `Update`, `Remove`: add, change or remove a range in the key's `.meta`
- `TtlRefresh`: updates the object's `expires_at`
- `AccessUpdate`: increments a range's `access_count` and updates its `last_accessed`
- `Graduation`: records that an object left the write cache on its first read, for the size
  accounting

`JournalConsolidator` (background task):

- Runs every 5 seconds by default (`shared_storage.consolidation_interval`, range 1 to 60 seconds)
- Applies every proxy's journal entries to the `.meta` files; see [Consolidation process](#consolidation-process)

### Lock acquisition

The consolidator makes one attempt at each key's metadata slot lock. If another process holds
it, the consolidator skips that key and its journal entries wait for the next cycle.

Request-path writers that take the same lock retry with exponential backoff instead: 100 ms
doubling per attempt, capped at 5 seconds, with ±25% jitter, for up to
`cache.shared_storage.lock_max_retries` retries (default 5).

### Journal file format

```json
{"timestamp":{"secs_since_epoch":1704067200},"instance_id":"proxy-1","cache_key":"bucket/object.txt","range_spec":{"start":0,"end":8388607},"operation":"TtlRefresh","new_ttl_secs":3600}
{"timestamp":{"secs_since_epoch":1704067201},"instance_id":"proxy-1","cache_key":"bucket/object.txt","range_spec":{"start":0,"end":8388607},"operation":"AccessUpdate","access_increment":5}
```

### Consolidation process

1. **Discover pending cache keys**: scan all `*.journal` files for unique cache keys.
2. **For each cache key**:
   - Collect all entries from every proxy's journal
   - Sort entries by timestamp, oldest first
   - Take the key's metadata slot lock
   - Load the metadata from disk
   - Apply each entry in order (see the entry types above)
   - Write the updated metadata to disk
   - Release the lock
3. **Clean up**: delete a journal file whose entries were all applied; rewrite a file that still
   holds unapplied entries with only those entries.

### Error handling

- **Lock contention**: a key whose lock is held is skipped and retried next cycle.
- **Metadata file missing**: journal entries for metadata that does not exist are skipped and
  logged as a warning (the entry may have been evicted).
- **Journal parse errors**: invalid JSON lines are logged and skipped; valid entries in the same
  file are still processed.

### Monitoring

The buffer flush logs at `debug` level, and only when it wrote or failed something. A
consolidation cycle logs at `info` level when it did work:

```text
DEBUG Cache hit update buffer flushed: entries=150, lines=40, duration=5ms, errors=0
INFO  Consolidation cycle complete: keys=45, entries=89, accumulator_delta=+1048576, duration=23ms, total_cache_size=52428800, eviction_triggered=false, write_cache_delta=+0
```

In the flush line, `entries` counts recorded updates and `lines` the coalesced journal lines written.

### Configuration

Access tracking is automatic and needs no configuration. Its parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Flush interval | 5 seconds | How often the RAM buffer is flushed to the journal |
| Max buffer entries | 10,000 | Buffer size that forces an early flush |
| Consolidation interval | 5 seconds | How often journal entries are applied to metadata (1 to 60 seconds) |

## Eviction lock internals

The eviction lock's purpose and tuning are in
[Distributed eviction](SHARED_STORAGE.md#distributed-eviction). This section covers what the
lock file holds.

The lock is a non-blocking exclusive `flock` on `cache_dir/locks/global_eviction.lock`. The
kernel releases it when the holding process exits, and the NFS server releases it when the
holder's lease expires, so a held lock always belongs to a live process and is never taken over.

Once a proxy holds the lock, it writes an `EvictionLockPayload` into the file as a fence token:
a `uuid` generated afresh on every acquisition, `acquired_at_ms` and `hostname`. The holder
re-reads the payload before each batch of file system changes and aborts the pass if the UUID
no longer matches, logging:

```text
Eviction fence lost: expected uuid=<x>, found uuid=<y> (holder=<host>). Aborting eviction pass.
```

A proxy that finds the lock held reads `acquired_at_ms`. When the holder acquired the lock more
than `eviction_lock_timeout` ago, the proxy tries once more before skipping the pass, and that
attempt succeeds only if the holder has released the lock in between.

## Size tracking internals

The operator view, including the concurrent-write over-counting this design is subject to, is
in [Size tracking](SHARED_STORAGE.md#size-tracking).

### Architecture

Cache size tracking uses an in-memory `AtomicI64` accumulator in each proxy:

```text
store_range() success → accumulator.add(compressed_size)
eviction              → accumulator.subtract(compressed_size)
                              ↓
              Flush to delta_{instance_id}_{seq}.json (every 5s)
                              ↓
                    JournalConsolidator (under global lock)
                              ↓
                    Sum all delta files → Update size_state.json
                              ↓
                    Delete consolidated delta files
                              ↓
                    Trigger Eviction (if over capacity)
```

Properties:

- **No per-write file I/O**: a write or eviction updates an in-memory atomic; the cache volume
  is touched once per flush.
- **No timing gaps**: the size is recorded when data is written, not during consolidation.
- **One source of truth**: the consolidator sums all delta files under the global lock.
- **Crash recovery**: at most one flush interval of deltas is lost, and the daily validation
  scan corrects the drift.

### How it works

#### In-memory accumulator

Each proxy keeps an `AtomicI64` accumulator of the net size change since the last flush:

```rust
// On successful range write
accumulator.add(compressed_size);

// On range eviction
accumulator.subtract(compressed_size);
```

A separate `write_cache_delta` accumulator tracks write-cached ranges (PUT requests and
multipart uploads).

A third accumulator, `cached_objects_delta`, counts objects: it takes +1 when a request publishes
a `.meta` where none was counted (a new PUT key, a HEAD-only entry) and -1 when a request or
eviction removes a counted `.meta`. A re-PUT of a counted key changes nothing. The consolidation
cycle applies the sum, so `cached_objects` moves once per cycle rather than once per request.

#### Delta file flush

Every consolidation cycle (5 seconds by default), each proxy flushes its accumulated delta to a
file of its own:

```text
Delta File (size_tracking/delta_{instance_id}_{seq}.json):
{
  "delta": 1048576,
  "write_cache_delta": 0,
  "cached_objects_delta": 1,
  "instance_id": "proxy1.example.com:12345",
  "timestamp": "2026-01-26T15:30:00.000Z"
}
```

The flush swaps the accumulators to zero atomically; if the file write fails, the swapped values
are restored.

#### Consolidator integration

The consolidator, under the global lock, reads all delta files, sums them, and updates the size
state:

```text
Consolidation Cycle:
1. Flush own accumulator to delta file (before lock)
2. Acquire global consolidation lock
3. Read all delta_*.json files from size_tracking/
4. Sum delta, write_cache_delta and cached_objects_delta values
5. Add sums to size_state.json (clamping to 0)
6. Delete each consolidated delta file
7. Process journal entries for metadata updates only
8. Trigger eviction if over capacity
9. Release lock
```

The delta file name is `delta_{instance_id}_{seq}.json`: a new, sequence-numbered file per
flush, not one file per proxy. The consolidator deletes each file after reading it rather than
zeroing it, because zeroing would lose any delta a proxy flushed between the read and the reset.

### Size state persistence

The authoritative size state is stored in `size_state.json`:

```text
Size State File (size_tracking/size_state.json):
{
  "total_size": 5368709120,
  "write_cache_size": 268435456,
  "last_consolidation": 1706282400,
  "consolidation_count": 12345,
  "last_updated_by": "proxy1.example.com:12345"
}
```

Fields:

- `total_size`: total cache size in bytes (read cache plus write cache).
- `write_cache_size`: write-cache bytes, a subset of `total_size` rather than an addition to it,
  so read-cache bytes are `total_size - write_cache_size`. It counts the compressed bytes of
  objects written through the cache that have not yet been read. An object leaves this figure
  when it is read for the first time (it becomes read-cached, and the bytes stay on disk) or
  when it is removed. Only the consolidator maintains it: adjusted incrementally as objects are
  cached, read and removed, and re-grounded absolutely by a full validation scan. On a volume
  whose staged-authority epoch has been activated, the staged fold produces the published
  write-cache figure instead, and the credits and debits below do not govern it; see
  [Size accounting](#size-accounting) for which mechanism is live and how to tell.

  Every credit and debit is by `compressed_size` and is classified through one shared predicate,
  so a range cannot be added under one rule and subtracted under another. The credit is applied
  when the cache entry is written, by whichever path wrote it. The two single-object upload paths
  write their entry directly rather than through the journal (so an immediate read of a
  just-uploaded object is a cache hit), so they credit the accounting themselves; multipart
  completion credits through its journal entries. A range that already existed on the cache
  volume is not credited again, because the proxy that published it credited it then.

  **A credit is durable only if it lands on the consolidator the background consolidation task
  holds.** Deltas accumulate in memory on the consolidator instance and are written to a delta
  file only by `run_consolidation_cycle`, which runs on the single `Arc` captured at startup.
  There is therefore exactly one `JournalConsolidator` per process, created on the first
  `create_configured_disk_cache_manager()` call and reused by every later one. The same applies
  to the `HybridMetadataWriter` and `CacheHitUpdateBuffer` created alongside it, which are
  likewise drained only by their own startup tasks. Constructing a second instance of any of the
  three does not duplicate a component; it creates a sink whose contents are discarded, because
  nothing that drains it holds a reference. The cycle's idle short-circuit makes the failure
  total rather than partial: `has_pending_delta()` is false on the task's own accumulator, so it
  returns before flushing anything.

  A code path that needs one of these must take it from `create_configured_disk_cache_manager()`
  or the `get_*` accessors, never construct one. `journal_components_identity_tests` in
  `src/cache.rs` pins this.

  **Every deletion of a range file must debit, and must release the range's dedup entry.** A path
  that removes a `.bin` and does not debit leaves the figure holding bytes the disk no longer
  has. The two write paths that replace an object's content
  (`store_put_as_write_cached_range_with_ttl` on a re-PUT, and `store_full_object_as_range_new`
  when it supersedes partial ranges) both route through `remove_range_files` and
  `debit_removed_ranges` for this reason.

  Two rules are easy to get half-right:

  - **Debit only files that existed and deleted cleanly.** Iterating `metadata.ranges` directly
    charges for files that were already gone, and that debit stands until the next full scan
    because nothing re-credits it. `remove_range_files` returns a filtered list for this reason;
    do not widen it.
  - **Use `SizeAccumulator::subtract_range`, not `subtract`, wherever the range's identity is
    known.** `add_range` credits only when it can insert into the dedup set, and `subtract`
    leaves that entry behind, so a delete-then-rewrite debits once and credits nothing, leaving
    the figure short. `subtract` remains for callers with no range identity to offer. The dedup
    set is cleared only by a validation scan, never by a flush, so an entry left behind
    suppresses credits for up to a full validation interval.

  Classification is always `cache_types::is_staged_range_spec`, which reads the range's own
  recorded membership (`RangeSpec.staged`), and falls back to the `is_write_cached` flag of the
  `.meta` the deleting call actually read only for a range that has no `staged` field.
  Membership is per range, not per object: a range credited to the read cache and then attached
  to a still-flagged object must not be debited from the write cache. Do not consult
  `graduation_accounted`: it is the consolidator's exactly-once token, and no debit site reads
  it. Reading the flag at delete time is what keeps a concurrent graduation from being debited
  twice, and that relies on `refresh_write_cache_ttl` writing the flag-cleared `.meta` before it
  appends its `Graduation` entry.
- `last_consolidation`: Unix timestamp of the last consolidation cycle.
- `consolidation_count`: number of consolidation cycles completed.
- `last_updated_by`: identity of the proxy that last updated the state.

### Startup recovery

On startup, the consolidator loads the existing size state:

```text
Recovery Flow:
1. Try to load size_state.json
2. If found → Initialize with persisted values
3. If not found → Start at 0, validation will correct
```

Recovery is a single file read, whatever the cache size. The in-memory accumulator starts at
zero.

## Size accounting

This section covers one figure: how many bytes the write cache holds, meaning objects written
through the cache and not yet read. Read-cache bytes are `total_size - write_cache_size`, and
`total_size` is the consolidator's, as described above.

### Two producers, and a volume commits to one

A cache volume carries an activation epoch in
`metadata/_shard_state/staged_authority_epoch.json`, which decides which of two mechanisms
produces the write-cache figure.

Before activation, the figure is the event-sourced one described under
[Size state persistence](#size-state-persistence): every write credits, every removal debits,
the consolidator applies the deltas, and a full validation scan re-grounds it. After
activation, the staged fold recomputes the figure from the `.meta` files themselves, and the
credit-and-debit bookkeeping is not the authority. `Graduation` journal entries are parsed and
counted, but applying one is a no-op.

`write_cache.staged_authority_epoch` on `/metrics` reports the volume's epoch. Activation is
one-way and evidence-based. The epoch cannot advance while any proxy that has ever appended a
staged credit has not also published a readiness record under `_shard_state/ready/`, unless that
proxy's write-ledger files have gone unmodified for a day. Once the epoch has advanced, a
process latches it for its lifetime.

**A binary that does not understand the fold must not be started against an activated volume.**
Such a binary keeps its own write-cache accounting in `size_state.json`, which 3.0.0 no longer
maintains that way, so its write-cache admission works from a wrong figure, and its daily
validation scan discards size updates other proxies have not yet applied.
Activation adds a schema field to `size_state.json`, but 2.8 releases ignore unknown fields, so
the field neither stops them reading the document nor tells 3.0.0 that they wrote it. 3.0.0
refuses no such binary and almost never reports one, so the rollback procedure is the
protection. Returning
a volume to the earlier mechanism takes a fleet-wide stop, a manual reset of the epoch document,
and a deletion of `size_state.json`; it cannot be done as a rolling operation.

### What a fold pass does

The write cache is partitioned into `(bucket, L1)` shards: the bucket name and the first two hex
digits of the key hash, the same split the `metadata/` and `ranges/` trees use. Each shard has a
checkpoint at `metadata/_shard_state/checkpoints/{bucket}/{XX}.json` holding that shard's byte
figure and the ledger sequence it was cut at. The published figure is the sum of every shard's
checkpoint, plus credits that landed after their shard's cut (from `_shard_state/staged_pending/`),
plus bytes staged in `mpus_in_progress/`.

A pass runs every 300 seconds and does not visit every shard. It computes a per-pass budget from
the shard count and visits that many, stalest first, taking a lease on each so two proxies do not
fold the same shard at once. The budget is sized so that 288 consecutive passes cover the
population, which holds every shard's figure inside the fold's 24-hour freshness bound. A full
sweep of the population is therefore a rotation, not a pass.

#### Where the in-progress multipart term comes from

The `mpus_in_progress/` part of that sum is not walked per reader. Each proxy knows exactly how
many part bytes it has written, because it wrote them, and publishes that figure once per
`cache.shared_storage.consolidation_interval` to
`metadata/_shard_state/in_progress_parts/{instance_id}.inprog`. A reader takes the background
collection's walk of `mpus_in_progress/` and adds each proxy's growth since that walk began, so
in the steady state it reads one small document per proxy and walks nothing. The exception is the
first seconds after a proxy restarts, described below, when deciding readers walk the volume
themselves.

The walk stays the reconciliation. Once per `metrics.collection_interval` the collection compares
its walk against the figure the previous walk's composition supports and publishes the
difference as `cache.in_progress_part_bytes_drift_bytes`. The composed figure can only read high,
because a part removed since the base walk is still counted by it, so a figure reading below the
walk means a proxy's counter has missed an update: readers on that proxy then walk the volume
themselves until the next collection. A proxy that publishes nothing (one that does not write
this document), whose document is older than three of its own publication intervals (a stopped
proxy), or whose document cannot be parsed has the same effect. The bytes of a stopped proxy stay
in the figure throughout, because the walk counts them whoever wrote them.

A restarted proxy publishes under a new identity and leaves its predecessor's document behind.
Two rules take that document out of the sum, and a restart meets the first. The replacement
proxy removes it within a few of its `consolidation_interval`s of starting, about half a minute
at the 5-second default, on the evidence that the predecessor's process id is dead on that
machine and that its document did not move while the replacement watched it. That evidence rests
on each proxy having its own hostname
([Each proxy needs its own hostname](SHARED_STORAGE.md#each-proxy-needs-its-own-hostname)) and is
not used once a shared hostname has been detected. Failing that, a document older than an hour
whose proxy has also written no journal in that hour belongs to a process that has gone, so it
is left out of the sum rather than read as a proxy that stopped publishing, and the next
publication by any proxy removes it. Until one of the two rules fires, the document is read as a
proxy whose publishing has stalled, and the deciding readers on every proxy walk the volume
themselves. The part bytes that process wrote stay in the figure, from the walk, until a
`CompleteMultipartUpload`, an abort or the incomplete-upload sweep removes the records
themselves.

A proxy brings its own figure back down when a part record goes: by its own removal, and
otherwise at its next publication, which drops the records whose upload directory is no longer
on the volume. That covers a `CompleteMultipartUpload` or an abort landing on a different proxy
from the one that uploaded the parts.

### Why the figure lags a write by up to a rotation

**The fold never subtracts.** A shard's figure changes only when a visit recomputes it from that
shard's `.meta` files. So a DELETE, a first read that graduates an object out of the write cache,
or a re-PUT that supersedes ranges does not move `cache.write_cache_size` when it happens. The
bytes stay in the figure until that shard's next visit.

The proxy that removed the bytes records the shard it touched and brings it forward, which
usually gets it into the next pass: it takes the pass budget's spare slots first, and up to 128
extra visits a pass beyond them. A shard a peer holds a lease on waits for the visit after that,
and if more than 128 shards are waiting, the rest wait for later passes. The bound is one
rotation, 288 passes of 300 seconds: 24 hours.

**This lag is an overstatement, and that direction is chosen.** A figure that is too high fires
write cache reclamation early; a figure that is too low fires it late or not at all. So a
`cache.write_cache_size` that has not dropped after a delete means the accounting has not reached
that shard yet, not that the delete failed. Confirm it against the shard-age figures below before
treating it as a fault.

Count-driven write cache reclamation corrects for the same lag in its own figure; see
[Coordination across proxies](EVICTION.md#coordination-across-proxies).

### Reading the fold's own figures

`write_cache.staged_fold_bytes` is the fold's byte total, and it is `null`, never a number,
whenever the fold cannot stand behind it: an unreadable input, or a pass that kept an old figure
rather than installing a new one.

Its freshness is the stalest shard's, not the current time.
`write_cache.staged_shard_authority_age_secs` is the age the bound is judged on, and the one to
alarm on. `write_cache.staged_authority_age_secs` is the freshest shard's age, and only answers
whether the fold is getting anything done. `write_cache.staged_fold_shards_beyond_max_age` counts
shards past the bound and should read zero.

## HEAD cache cleanup

The daily validation scan deletes a `.meta` that holds no ranges and whose `head_expires_at`
passed more than one day ago (an entry with no `head_expires_at` is judged on the object's
`expires_at`, which covers a cached zero-length object). The predicate is re-evaluated on a fresh
read under the key's metadata slot lock before the file is removed, and an entry whose slot a
peer holds is left for the next day's scan. A `.meta` that still holds ranges is never deleted
for HEAD expiry.

A read of an expired HEAD entry refreshes it from S3 and deletes nothing; see
[Lazy deletion on read](CACHE_FRESHNESS.md#lazy-deletion-on-read).

## See also

- [Caching](CACHING.md): what is cached, and what bypasses the cache
- [Cache read paths](CACHE_READ_PATHS.md): how a read is served from these structures
- [Cache freshness](CACHE_FRESHNESS.md): TTLs, revalidation and conditional requests
- [Cache eviction](EVICTION.md): how space is reclaimed from them
- [Running a fleet on one cache volume](SHARED_STORAGE.md): the operator view for a fleet
- [Configuration reference](CONFIGURATION.md): every field
