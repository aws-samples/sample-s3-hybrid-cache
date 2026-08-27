# Cache Internals

How the cache is laid out on disk and in memory: the two tiers, the sharding scheme,
the file formats, and the background subsystems that maintain them.

This is the implementation view. For the operator view of the same machinery on a shared
volume, see [SHARED_STORAGE.md](SHARED_STORAGE.md). For which reads hit which tier, see
[CACHE_READ_PATHS.md](CACHE_READ_PATHS.md). For what gets cached at all, start at
[CACHING.md](CACHING.md).

## Two-Tier Cache System

1. **RAM Cache** (Optional, First Tier)
   - In-memory cache for hot objects, HEAD metadata, and range data
   - Configurable size limit (`max_ram_cache_size`, default: 512 MiB)
   - Eviction algorithms: LRU or TinyLFU (decayed-frequency scoring, see [Range-Based Disk Cache Eviction](EVICTION.md#eviction-algorithms))
   - Fastest access path
   - Compression optimization: Eliminates decompress/recompress cycles during disk-to-RAM promotion
   - Size limits enforced on compressed data (allows large compressible files to be cached)
   - **RAM Metadata Cache**: Caches `NewCacheMetadata` objects to reduce disk I/O for both HEAD and GET requests
   - **Range data caching**: Both streaming and buffered paths check RAM cache before disk and promote disk hits to RAM cache (key format: `{cache_key}:range:{start}:{end}`)
   - Note: PUT-cached objects are NOT stored in RAM cache (disk only)
   - **Sharded for concurrency**: The RAM cache is partitioned into `ram_cache_shard_count` independent shards (default 8), each guarded by its own `tokio::sync::RwLock`. A key maps to a shard via `blake3(cache_key) % shard_count`. Reads for keys in different shards proceed in parallel with no contention, and concurrent reads of the same key share a read lock. Per-shard capacity is `max_ram_cache_size / effective_shard_count`, where the effective count may be clamped below the configured value to honour the 64 MiB admission ceiling (see below). See [Concurrency Model](#ram-cache-concurrency-model) below and [`ram_cache_shard_count`](CONFIGURATION.md) for tuning.

2. **Disk Cache** (Second Tier)
   - File-based persistent cache
   - Supports shared volumes for multi-instance deployments
   - Eviction algorithms: LRU or TinyLFU (decayed-frequency scoring, see [Range-Based Disk Cache Eviction](EVICTION.md#eviction-algorithms))
   - Range-level eviction granularity for optimal cache utilization
   - LZ4 compression for space efficiency
   - Range storage architecture: `.meta` (lightweight metadata) + `.bin` (range data)

## RAM Cache Concurrency Model

The RAM cache read path is designed to scale with concurrent connections rather than serialize all reads through one lock.

**Sharding**: The cache is split into `ram_cache_shard_count` shards (default 8). Each shard owns a disjoint subset of keys and its own `tokio::sync::RwLock`, eviction state, and capacity budget (`max_ram_cache_size / effective_shard_count`, after the admission-ceiling clamp). A key is routed to its shard with `blake3(cache_key) % shard_count` — reusing the BLAKE3 hash already computed for disk-path sharding, so routing adds no extra hashing cost. GETs and PUTs lock only the target shard; operations on keys in different shards never contend.

**Zero-copy reads**: Cached bytes are stored as `Arc<Bytes>`. A `get()` returns a reference-count increment (O(1)), not a copy of the object data. The shard lock is released before any LZ4 decompression or HTTP response-body construction, so the lock is held only for the pointer clone. Peak per-request memory for a cache hit is bounded by one copy of the decompressed object, not two.

**Async-aware locking**: Each shard uses `tokio::sync::RwLock`, so a read yields to the Tokio runtime instead of blocking the OS worker thread under contention. Reads acquire the lock in shared mode; PUTs and eviction acquire it in exclusive mode. Unlike `std::sync::Mutex`, this lock does not poison on a panic in a request handler.

**Read-only access tracking**: Per-entry `last_accessed` and `access_count`, plus the cache-wide hit/miss counters, are `AtomicU64` fields updated through the shared read lock. The read path never needs a write lock to record an access. LRU/TinyLFU ordering is updated approximately: reads push to a sampled `pending_accesses` buffer that the next `put()` drains under the write lock. Exact ordering on every read is not maintained — eviction only needs to favor cold entries, and a one-`put()`-delayed reorder is sufficient for steady-state hot-set workloads.

**Shard skew caveat**: Because each shard evicts against its own capacity slice, an uneven key distribution or a few very large objects can fill one shard while others stay underused, lowering effective cache utilization below the configured maximum. BLAKE3 distributes keys uniformly, so skew is small across many keys; workloads dominated by a handful of very large hot objects should size `max_ram_cache_size` with headroom or lower `ram_cache_shard_count`. Admission is not retention: the 64 MiB ceiling guarantees a large entry is *accepted*, but keeping `N` such entries resident concurrently needs `max_ram_cache_size >= N * 64 MiB`. See [`ram_cache_shard_count`](CONFIGURATION.md) for tuning guidance.

## RAM-Disk Cache Coherency

When both RAM and disk caches are enabled, the proxy maintains coherency between them through two mechanisms:

1. **Periodic Verification**: RAM cache entries are verified against disk metadata to detect stale data
2. **Access Statistics Propagation**: RAM cache hits are batched and written to disk metadata for eviction decisions

### How Verification Works

When a RAM cache hit occurs, the proxy periodically verifies the entry against disk metadata:

```
RAM Cache Hit → Check verification interval elapsed?
             → Yes: Read disk metadata, compare etag/size
                   → Match: Serve from RAM, record verification
                   → Mismatch: Invalidate RAM entry, return cache miss
                   → Disk missing: Invalidate RAM entry, return cache miss
             → No: Serve from RAM (skip verification)
```

**Error Handling**: If disk cache storage fails due to lock contention or I/O errors, the operation returns an error rather than silently succeeding, preventing RAM-disk inconsistencies that could cause verification failures.

**Verification Fields Compared:**
- ETag (content hash)
- Size (byte count)
- Compression status

**Verification Throttling:**
- Verification is throttled per cache key to avoid excessive disk I/O
- Default interval: 1 second (configurable via `ram_cache_verification_interval`)
- Entries accessed multiple times within the interval skip verification

### Access Statistics Propagation

RAM cache hits update access statistics that are propagated to disk metadata via the journal system:

```
RAM Hit → DiskCacheManager.record_range_access() called
       → CacheHitUpdateBuffer buffers the update in RAM
       → Periodic flush (every 5s) writes to per-instance journal file
       → JournalConsolidator applies journal entries to metadata files
```

**Why This Matters:**
- Disk eviction algorithms (LRU/TinyLFU, decayed-frequency scoring) need accurate access statistics
- Without propagation, frequently-accessed RAM entries would appear "cold" on disk
- Hot data could be evicted from disk while still being served from RAM
- When RAM evicts the entry, disk would have already evicted it → cache miss

### Journal-Based Access Tracking

The journal system provides efficient, race-condition-free access tracking on shared storage:

- **RAM buffering**: Updates collected in `CacheHitUpdateBuffer` (reduces disk I/O)
- **Per-instance journals**: Each proxy instance writes to its own journal file (no contention)
- **Background consolidation**: `JournalConsolidator` applies updates with proper locking
- **Atomic updates**: Lock acquisition with retry ensures consistency on NFS

### Configuration Options

Journal-based metadata writes are always enabled; there is no `enabled` flag.

**[SHARED_STORAGE.md](SHARED_STORAGE.md) is the guide** to multi-instance coordination:
the mount requirements, journals, distributed eviction, size tracking, the validation
scan, and the failure modes. For field types, defaults, and validated ranges see
[Configuration — Multi-Instance Coordination](CONFIGURATION.md#multi-instance-coordination).

### Performance Characteristics

**Verification:**
- Target: <10ms per verification
- Reads only metadata file (not range data)
- Throttled to avoid excessive I/O

**Batch Flush:**
- Processes all pending updates in single pass
- File lock held <100ms per key
- Logs duration and error counts

### Monitoring Metrics

The following metrics track RAM-disk coherency operations:

```json
{
  "batch_flush": {
    "pending_disk_updates": 45,
    "batch_flush_count": 12,
    "batch_flush_keys_updated": 156,
    "batch_flush_ranges_updated": 423,
    "batch_flush_avg_duration_ms": 23.5,
    "batch_flush_errors": 0
  },
  "ram_verification": {
    "ram_verification_checks": 1250,
    "ram_verification_invalidations": 3,
    "ram_verification_disk_missing": 1,
    "ram_verification_errors": 0,
    "ram_verification_avg_duration_ms": 2.1
  }
}
```

**Key Metrics to Monitor:**

- **ram_verification_invalidations**: High values indicate RAM-disk inconsistency
- **ram_verification_disk_missing**: Disk entries evicted while in RAM
- **batch_flush_errors**: Failed metadata updates (check disk space/permissions)
- **pending_disk_updates**: Should stay below flush_threshold

### Troubleshooting

**High verification invalidations:**
- Disk cache may be too small (entries evicted before RAM)
- Consider increasing disk cache size
- Check for external modifications to cache files

**Batch flush errors:**
- Check disk space availability
- Verify cache directory permissions
- Check for file locking issues (NFS configuration)

**Slow verification (>10ms):**
- Disk I/O bottleneck
- Consider SSD storage for cache directory
- Check for high disk utilization

## RAM Metadata Cache

The proxy implements a RAM Metadata Cache that stores `NewCacheMetadata` objects in memory to reduce disk I/O for both HEAD and GET requests. This provides sub-millisecond metadata lookups while maintaining consistency with disk-based storage.

### Architecture

**Unified Storage System:**
- **RAM MetadataCache**: In-memory LRU cache for `NewCacheMetadata` objects
- **Disk Metadata**: Persistent `.meta` files in `metadata/` directory
- **Unified Model**: HEAD and GET share the same metadata file with independent TTLs

**Data Structure:**
```rust
MetadataCacheEntry {
    metadata: NewCacheMetadata,  // Full metadata including HEAD and GET fields
    loaded_at: Instant,          // When loaded from disk
    disk_mtime: Option<SystemTime>, // Disk file modification time
    last_accessed: Instant,      // For LRU eviction
}
```

**Key Design Principles:**
- HEAD and GET metadata stored in same `.meta` file
- HEAD has `head_expires_at` field, GET ranges have `expires_at` field
- HEAD expiry doesn't delete the file (ranges may still be valid)
- Range expiry doesn't affect HEAD validity

### Eviction with LRU

The RAM Metadata Cache uses simple LRU eviction (not TinyLFU/decayed-frequency):

**Why LRU (not TinyLFU):**
- All metadata entries are similar size (~1-2KB)
- TinyLFU complexity not needed for uniform-size entries
- Simple LRU provides optimal performance for metadata caching

**Eviction Behavior:**
- When cache reaches `max_entries`, least recently accessed entries are evicted
- Eviction only removes from RAM cache, not from disk
- Evicted entries are reloaded from disk on next access

### Request Flows

**HEAD Request Flow:**
```
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

**GET/Range Request Flow:**
```
GET Request → Check RAM MetadataCache for NewCacheMetadata
           → If HIT: check if requested range exists and not expired
               → If valid range: serve from range file
               → If no range: fetch from S3, cache as new range
           → If MISS: read .meta from disk or fetch from S3
           → Update range access_count, last_accessed
           → Serve response
```

### TTL Independence

| Cache Layer | TTL Field | Default | Purpose |
|-------------|-----------|---------|---------|
| RAM MetadataCache | `refresh_interval` | 5s | How often to re-read from disk |
| HEAD (disk) | `head_expires_at` | 1 minute | When to re-fetch HEAD from S3 |
| GET range (disk) | `RangeSpec.expires_at` | ~10 years | When to re-fetch range from S3 |

**Key insight**: HEAD expiry doesn't delete the `.meta` file - it just means HEAD needs revalidation from S3. Ranges can still be valid and served from cache.

### Per-Key Locking

The MetadataCache implements per-key locking to prevent concurrent disk reads:

```
Multiple concurrent requests for same cache_key:
  Request 1 → Acquires key lock → Reads from disk → Updates cache → Releases lock
  Request 2 → Waits for key lock → Gets cached result (no disk read)
  Request 3 → Waits for key lock → Gets cached result (no disk read)

Result: Only one disk read regardless of concurrent request count
```

**Benefits:**
- Prevents thundering herd on cache miss
- Reduces disk I/O under high concurrency
- Improves response times for concurrent requests

### Stale File Handle Recovery

The cache implements retry logic for stale file handle errors (common with NFS):

```
Read attempt → ESTALE error → Retry with backoff (up to 3 times)
            → If persists: invalidate cache entry, return error
```

**Configuration:**
- `stale_handle_max_retries`: Maximum retry attempts (default: 3)
- Exponential backoff between retries (10ms, 20ms, 30ms)

### Metadata File Size Cap and Self-Heal

The proxy enforces a maximum `.meta` file size (`max_metadata_file_bytes`, default 4 MiB) to prevent pathologically large metadata files — such as legacy inline-body entries from pre-upgrade proxy versions — from blocking the runtime or consuming unbounded memory. Files exceeding the cap are rejected in O(stat) without being read.

When a `.meta` file is classified as confidently corrupt (oversize, legacy schema, or stable parse failure after bounded retries), the proxy heals it automatically: the S3 refetch writes a fresh valid `.meta` via atomic tmp+rename, overwriting the corrupt file. Transient/partial reads (mid-write on NFS) remain on the existing "do not delete" tolerance path. See `docs/ERROR_HANDLING.md` for the full classification and heal mechanism.

All metadata reads run inside `spawn_blocking` with a concurrency semaphore (`metadata_io_concurrency`, default 32), so one slow or large `.meta` file cannot starve the async runtime.

### Configuration

```yaml
cache:
  metadata_cache:
    enabled: true
    refresh_interval: "5s"      # How often RAM cache re-reads from disk
    max_entries: 100000         # Maximum entries in RAM cache
    stale_handle_max_retries: 3 # Retry count for stale file handles
  
  # HEAD-specific TTL (independent of GET TTL)
  head_ttl: "60s"               # How long HEAD is valid before S3 re-fetch
```

**Important Notes:**
- MetadataCache is separate from RAM data cache (which stores range bytes)
- HEAD TTL is independent of GET TTL
- Refresh interval controls disk re-reads, not S3 re-fetches

### Cache Statistics

RAM Metadata Cache operations are included in cache statistics:

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

**Metrics Breakdown:**
- **entries**: Current number of entries in RAM cache
- **hits**: Cache hits (served from RAM)
- **misses**: Cache misses (required disk read)
- **stale_refreshes**: Entries refreshed due to staleness
- **evictions**: Entries evicted due to capacity
- **stale_handle_errors**: ESTALE errors encountered

### Monitoring and Logging

**Cache Operations:**
```
INFO Metadata cache HIT: cache_key=bucket/object.txt
INFO Metadata cache MISS: cache_key=bucket/object.txt loaded_from_disk=true
INFO Metadata cache STALE: cache_key=bucket/object.txt refreshing_from_disk=true
```

**Eviction Events:**
```
INFO Metadata cache eviction: evicted_key=/bucket/old-object.txt reason=capacity_limit
```

**Stale Handle Recovery:**
```
WARN Stale file handle error: cache_key=bucket/object.txt attempt=1 retrying=true
ERROR Stale file handle persisted: cache_key=bucket/object.txt attempts=3 invalidating=true
```

### Benefits

**Performance:**
- Sub-millisecond metadata lookups for hot entries
- Reduced disk I/O for frequently accessed objects
- Per-key locking prevents thundering herd

**Efficiency:**
- Unified storage reduces disk space (no separate HEAD cache)
- LRU eviction optimal for uniform-size metadata entries
- Memory usage: ~15-25MB for 10,000 entries, so roughly 150-250MB at the default `max_entries` of 100,000

**Consistency:**
- Independent HEAD/GET TTLs in same file
- Stale file handle recovery for NFS reliability
- Refresh interval ensures disk consistency

### Use Cases

**API Gateway / Load Balancer Health Checks:**
```
Scenario: Load balancer checks object existence every 5 seconds
Without RAM Metadata Cache: Each check reads from disk
With RAM Metadata Cache: First check loads to RAM, subsequent served from RAM
Result: 99%+ reduction in disk I/O, <1ms response time (based on internal testing with synthetic workloads)
```

**High-Concurrency Workloads:**
```
Scenario: 100 concurrent requests for same object metadata
Without per-key locking: 100 disk reads
With per-key locking: 1 disk read, 99 served from RAM
Result: 99% reduction in disk I/O under concurrency
```

**NFS Shared Cache:**
```
Scenario: Multiple proxy instances sharing NFS cache volume
Challenge: Stale file handles when other instances modify files
Solution: Automatic retry with backoff, graceful degradation
Result: Reliable operation despite NFS limitations
```

## Streaming Response Architecture

The proxy streams **all** S3 responses to eliminate buffering and reduce memory usage. The 1 MiB size threshold that used to select between buffered and streaming mode was removed in 1.7.6; there is no size at which an S3 response is collected in memory before being returned.

Do not confuse this with `cache.disk_streaming_threshold` (default 1 MiB), which is a separate knob governing the **disk read** path — whether a cached range is read fully into memory or streamed from disk in chunks. See [Cache Hit Performance Tuning](CONFIGURATION.md#disk-streaming-threshold).

**How It Works:**

1. **TeeStream for Simultaneous Streaming and Caching**:
   - S3 responses are wrapped in a TeeStream
   - Data streams to client immediately (first byte latency < 100ms)
   - Data is simultaneously sent to a background task for caching
   - No buffering of entire response in memory

2. **Benefits**:
   - **Eliminates AWS SDK throughput timeouts**: Large files (500MB+) download without timeout
   - **Constant memory usage**: Memory usage is proportional to chunk size (64KB), not file size
   - **Low first byte latency**: Client receives data immediately as it arrives from S3
   - **No cache performance regression**: Cache hits remain fast (disk reads are already efficient)

**Complete Cache Miss Flow (Streaming):**
```
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

**Partial Cache Hit Flow (Buffered):**
- When some ranges are cached and some need fetching
- Data must be merged before sending to client
- Requires buffering to combine cached + fetched data
- Still efficient: only missing ranges fetched from S3

**Cache Hit Flow (Range Requests):**
```
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
- RAM cache is checked before the streaming/buffered decision for all range requests
- Both paths promote disk hits to RAM cache for subsequent requests
- Dashboard statistics reflect RAM cache hits/misses from both paths

**Configuration:**
- Disk streaming threshold: `cache.disk_streaming_threshold`, default 1 MiB, based on
  the Content-Length header. This governs the disk-read path only; it is not the
  1 MiB S3-response streaming threshold that was removed in 1.7.6, when all S3
  responses began streaming regardless of size.
- Chunk size for disk streaming: 512 KiB (as shown in the flow above)

### Performance Optimizations

The streaming and caching path includes several optimizations for maximum throughput:

**Zero-Copy Cache Writes:**
- When compression is disabled, cache writes use the original data slice directly
- No `data.to_vec()` copy operation — writes directly from the incoming buffer
- Reduces memory allocations and CPU usage during cache-miss streaming
- Particularly beneficial for large file transfers (8MB+ ranges)

**Journal-Only Metadata Mode:**
- Cache-miss range metadata writes use journal-only mode on shared storage
- Eliminates lock contention when multiple instances cache the same object
- Journal consolidator merges entries asynchronously without blocking streaming

**Bytes Reference Counting:**
- TeeStream uses `Bytes::clone()` which is reference-counted (not a deep copy)
- Data is shared between client stream and cache writer without duplication
- Memory efficient even for large chunks

**Async Cache Writer:**
- Cache writes happen in a background task, not blocking the client stream
- Uses mpsc channel with `try_send` to avoid backpressure blocking
- If cache writer falls behind, chunks are dropped (client stream continues)

## Range Storage Architecture

All cached data uses the same storage format, regardless of source:

- **PUT operations**: Stored as range 0-N immediately
- **GET operations**: Full objects or partial ranges stored uniformly
- **Multipart uploads**: Parts assembled into ranges on completion

**Key Benefits:**

1. **Range Request Support Everywhere**: PUT-cached and multipart-cached objects support range requests immediately without S3 fetch
2. **No Data Copying**: TTL transitions only update metadata, never copy range files
3. **Simplified Code**: Single storage format for all cache types
4. **Efficient Metadata**: Metadata files remain <100KB even with hundreds of ranges
5. **Fast Transitions**: TTL transitions complete in <10ms (metadata-only update)

### Partial range prefix salvage

A streamed range cache write can end before the full requested range arrives — the client cancelled the transfer, or the mid-stream idle watchdog (`connection_pool.upstream_idle_timeout`) aborted a stalled upstream. By default such a range was discarded entirely. With `cache.partial_range_commit_ratio` (default `0.5`), the proxy instead commits the received prefix as a smaller valid range `[start, start + received - 1]` when at least that fraction of the requested bytes arrived **in order**.

This matters for high-throughput clients like the AWS CLI CRT transfer client, which opens many parallel range connections — any of which can be cut short by the proxy's mid-stream idle watchdog or by CRT's adaptive part reassignment. Without salvage, the received bytes from those interrupted ranges are discarded entirely. The salvaged range is recorded with its true byte bounds, so it is never served as if it were the full requested range: a later request for the missing tail fetches it from S3 and merges with the cached prefix (see [Range Merging](CACHE_READ_PATHS.md#intelligent-range-merging)). `1.0` keeps the exact-only behavior (any short range discarded); `0.0` commits any non-empty prefix. The write-through PUT path is unaffected — a truncated upload is never cached. See [Partial Range Commit Ratio](CONFIGURATION.md#partial-range-commit-ratio) for tuning.

## Cache Entry Structure

Each cached object has:
- **Lightweight metadata** (`.meta` file in `metadata/` directory):
  - Object metadata: ETag, Last-Modified, Content-Length, Content-Type
  - HTTP headers
  - Range index: List of cached ranges with start, end, and file path
  - Upload state: Complete, InProgress, or Bypassed
  - Cumulative size: Running total for multipart uploads
  - Parts list: Temporary storage for multipart parts (cleared on completion)
  - Expiration timestamps
  - Typically <100KB even with hundreds of cached ranges
- **Range data** (`.bin` files in `ranges/` directory):
  - Compressed binary data for each cached range
  - Full objects stored as range 0-N (from PUT or completed multipart)
  - Partial ranges stored separately (from GET with Range header)
  - Multipart parts stored as ranges after completion

## Cache Directory Structure

The disk cache uses a **bucket-first hash-based sharding** architecture with range storage. This structure scales to billions of cached objects per bucket without filesystem performance degradation.

### Directory Hierarchy

```
cache_dir/
├── metadata/                   # Unified metadata files (HEAD + GET share same .meta files)
│   ├── {bucket}/              # S3 bucket name (first level)
│   │   ├── {XX}/              # First 2 hex digits of BLAKE3(object_key)
│   │   │   ├── {YYY}/         # Next 3 hex digits of BLAKE3(object_key)
│   │   │   │   ├── {object_key}.meta      # JSON metadata + range index (<100KB)
│   │   │   │   └── {object_key}.meta.lock # Lock file for metadata updates
│   │   │   └── ...
│   │   └── ...
│   └── _journals/             # Access tracking journals for distributed eviction
│       └── {instance_id}.journal   # Per-instance journal files
│   └── _write_ledger/         # Staged (uploaded, not yet read) object records
│       └── {instance_id}.ledger    # Per-instance; see EVICTION.md
├── ranges/                     # All cached data stored as binary ranges
│   ├── {bucket}/              # S3 bucket name (first level)
│   │   ├── {XX}/              # First 2 hex digits of BLAKE3(object_key)
│   │   │   ├── {YYY}/         # Next 3 hex digits of BLAKE3(object_key)
│   │   │   │   ├── {object_key}_0-8388607.bin       # Range data
│   │   │   │   ├── {object_key}_8388608-16777215.bin
│   │   │   │   └── {object_key}_16777216-25165823.bin
│   │   │   └── ...
│   │   └── ...
│   └── ...
├── mpus_in_progress/           # Multipart uploads in progress (when write caching enabled)
├── locks/                      # Coordination locks for shared cache
│   ├── {key}.lock             # Lock file for write coordination
│   └── global_eviction.lock   # Global eviction coordinator lock
└── size_tracking/              # Cache size tracking files
    ├── size_state.json        # Authoritative size state (updated by consolidator)
    ├── delta_{instance_id}_{seq}.json  # One new file per accumulator flush
    ├── validation.json        # Validation scan state (mode, cursor, scan rate — see CONFIGURATION.md#validation-scan)
    └── validation.lock        # Validation lock file
```

### Bucket-First Organization

**Cache Key Format**: `{bucket}/{object_key}`

The cache key always starts with a `{bucket}/` prefix. For path-style requests (`s3.{region}.amazonaws.com/{bucket}/{key}`) the bucket is taken from the first path segment; for virtual-hosted styles — regional, regional dualstack, accelerate, accelerate dualstack, and legacy global — it is extracted from the leading label of the Host header. All four styles produce the same cache key for the same bucket and object, so cache entries are shared across addressing styles (see *Regular Bucket Requests* below).

Examples:
- `my-bucket/path/to/object.txt`
- `my-bucket/file.jpg`
- `my.bucket.name/deeply/nested/path/data.bin`

### Access Point and MRAP Cache Key Prefixing

S3 Access Point and Multi-Region Access Point (MRAP) requests use a different URL structure than regular bucket requests. The endpoint identity appears in the Host header (virtual-hosted style) or the first path segment (path-style with alias), not in the bucket position. The proxy detects these patterns and generates cache key folders with AWS reserved suffixes to prevent namespace collisions with S3 bucket names.

The AWS CLI uses SigV4A (`AWS4-ECDSA-P256-SHA256`) by default for MRAP requests. The proxy recognizes both SigV4 and SigV4A signatures — signed headers, range signatures, referer injection guards, and `aws-chunked` streaming payload detection all work identically for both algorithms.

Per the [S3 bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html), the suffixes `-s3alias` and `.mrap` are reserved by AWS for access point alias names and cannot appear in general purpose bucket names. The proxy appends these suffixes to all AP/MRAP cache key folders, guaranteeing no collision with bucket-derived cache keys.

**Scope**: This caching solution supports S3 general purpose buckets only. S3 Tables buckets, S3 directory buckets, and S3 Vectors buckets are not supported.

**Virtual-Hosted Style (AP/MRAP identity in Host header)**

Regional AP hosts match `{name}-{account_id}.s3-accesspoint.{region}.amazonaws.com`. The proxy extracts `{name}-{account_id}` and appends `-s3alias` to form the cache key folder.

MRAP hosts match `{mrap_alias}.accesspoint.s3-global.amazonaws.com`. The proxy extracts `{mrap_alias}` and appends `.mrap` to form the cache key folder.

```
Regional AP (virtual-hosted):
  Host: my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com
  Path: /data/file.txt
  Cache key: my-ap-123456789012-s3alias/data/file.txt

MRAP (virtual-hosted):
  Host: mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com
  Path: /data/file.txt
  Cache key: mfzwi23gnjvgw.mrap/data/file.txt
```

The reserved suffixes also prevent cross-type collisions. A regional AP identifier and an MRAP alias that happen to be the same string produce distinct cache key folders:
```
Regional AP: abc123-s3alias/data/file.txt
MRAP:        abc123.mrap/data/file.txt
```

**Path-Style Forwarding (AP/MRAP alias in URL path)**

When the proxy receives a request with a base AP or MRAP domain as the Host and an alias in the first path segment, it detects the alias for logging purposes but forwards the request to S3 unchanged. The proxy does not rewrite the host or path — S3 handles path-style AP/MRAP routing natively, and preserving the original request is required for SigV4 signature validity.

The alias naturally appears as the first path segment in the cache key, providing correct namespace separation:

```
AP alias (path-style, forwarded as-is):
  Host: s3-accesspoint.us-east-1.amazonaws.com
  Path: /myname-abcdef123456-s3alias/data/file.txt
  Cache key: s3-accesspoint.us-east-1.amazonaws.com:/myname-abcdef123456-s3alias/data/file.txt
  (alias in first path segment provides namespace separation)

MRAP alias (path-style, forwarded as-is):
  Host: accesspoint.s3-global.amazonaws.com
  Path: /mrymoq6iot5o4.mrap/data/file.txt
  Cache key: accesspoint.s3-global.amazonaws.com:/mrymoq6iot5o4.mrap/data/file.txt
```

Note: Path-style AP alias requests require the base AP domain to resolve to S3 IPs. Since the proxy uses external DNS (which cannot resolve `s3-accesspoint.{region}.amazonaws.com`), this pattern does not work in practice. Use the regular S3 endpoint with AP aliases instead (see [Getting Started - Access Point Alias Usage](GETTING_STARTED.md#access-point-alias-usage)).

**Known Limitation: ARN-Based vs Alias-Based Cache Key Divergence**

The same Access Point accessed via ARN-based virtual-hosted style and via alias-based path style produces different cache key folders. ARN-based access uses the `{name}-{account_id}` identifier from the Host header (with `-s3alias` appended by the proxy), while alias-based access uses the alias string directly (which already contains `-s3alias` but with different metadata characters). The proxy has no AWS credentials and cannot map between these identifiers.

```
ARN-based (virtual-hosted): my-ap-123456789012-s3alias/data/file.txt
Alias-based (path-style):   my-ap-abcdef123456-s3alias/data/file.txt
```

Both cache key folders end with `-s3alias`, so neither collides with bucket names. The divergence causes a minor cache efficiency loss (same object cached twice under different keys) but is not a correctness issue.

**Regular Bucket Requests (Shared Cache Entries Across Addressing Styles)**

Regular bucket requests produce a cache key of the form `{bucket}/{object_key}`. The proxy extracts the bucket from whichever part of the request carries it for the addressing style in use:

- **Path-style** (`s3.{region}.amazonaws.com/{bucket}/{key}`, `s3.amazonaws.com/{bucket}/{key}`, `s3.dualstack.{region}.amazonaws.com/{bucket}/{key}`) — bucket is the first path segment.
- **Regional virtual-hosted** (`{bucket}.s3.{region}.amazonaws.com/{key}`, `{bucket}.s3.dualstack.{region}.amazonaws.com/{key}`) — bucket is the leading label of the Host header. Dots are allowed in bucket names per general-purpose naming rules (`my.company.logs.s3.us-east-1.amazonaws.com` parses to bucket `my.company.logs`).
- **S3 Transfer Acceleration** (`{bucket}.s3-accelerate.amazonaws.com/{key}`, `{bucket}.s3-accelerate.dualstack.amazonaws.com/{key}`) — bucket is the leading label of the Host header. Accelerate requires DNS-compliant bucket names, so bucket labels containing dots are rejected as malformed.
- **Legacy global** (`{bucket}.s3.amazonaws.com/{key}`) — bucket is the leading label of the Host header.

All four styles produce the identical cache key for the same bucket and object. A request for `my-bucket/file.txt` through path-style, regional virtual-hosted, accelerate, and legacy global endpoints hits the same cache slot, so cache entries are shared across addressing styles. This means a client that uses accelerate for writes and regional virtual-hosted for reads (or vice versa) benefits from one cached copy rather than four.

For non-AWS S3-compatible hostnames (MinIO, R2, etc.) and any Host the proxy does not recognise as an AWS S3 endpoint, the cache key falls back to the bare normalised path. This keeps the proxy functional against S3-compatible storage but without shared cache entries across addressing styles.

**Path Resolution**:
1. Parse cache key on first `/` after bucket to extract bucket and object key
2. Hash object key (not bucket) using BLAKE3
3. Extract first 2 hex digits for level 1 directory (XX)
4. Extract next 3 hex digits for level 2 directory (YYY)
5. Construct path: `{type}/{bucket}/{XX}/{YYY}/{filename}`

**Example**:
```
Cache key: /my-bucket/photos/vacation.jpg
Bucket: my-bucket
Object key: photos/vacation.jpg
BLAKE3(photos/vacation.jpg) = a7f3c2...
Level 1: a7
Level 2: f3c
Metadata path: metadata/my-bucket/a7/f3c/photos%2Fvacation.jpg.meta
Range path: ranges/my-bucket/a7/f3c/photos%2Fvacation.jpg_0-1048575.bin
```

### Hash-Based Sharding

**Why BLAKE3?**
- Cryptographically secure
- Excellent distribution properties
- Uniform file distribution across directories

**Sharding Levels**:
- **Level 1 (XX)**: 256 directories (00-ff)
- **Level 2 (YYY)**: 4,096 subdirectories per L1 (000-fff)
- **Total**: 1,048,576 leaf directories per bucket

**Capacity**:
- Maximum: 10.5 billion files per bucket (10,000 files/directory limit)
- Optimal: 2.6 billion files per bucket (40% safety margin)
- Example: 100M objects × 10 ranges = 1.1B files → ~1,049 files/directory

**Distribution**:
- Same object key in different buckets → same hash directories, different bucket directories
- Different object keys → uniformly distributed across hash directories
- Deterministic: same cache key always resolves to same path

### Directory Purpose

- **metadata/{bucket}/{XX}/{YYY}/**: Unified metadata files (`.meta`) containing object metadata, range index, HEAD TTL fields, upload state, and part caching fields - NO embedded binary data, typically <100KB even with hundreds of ranges. HEAD and GET share the same `.meta` file with independent TTLs.
- **ranges/{bucket}/{XX}/{YYY}/**: ALL cached data stored as binary files (`.bin`) - includes full objects from PUT (stored as range 0-N), partial ranges from GET, and assembled multipart uploads
- **locks/**: Lock files for coordinating writes in shared cache deployments and distributed eviction coordination

### Architecture Benefits

1. **Scalability**: Scales to billions of objects per bucket without filesystem performance degradation (1M+ leaf directories)
2. **Bucket Isolation**: Per-bucket cache management (clear cache by bucket with `rm -rf cache_dir/{type}/{bucket}/`)
3. **Uniform Distribution**: BLAKE3 hashing ensures even file distribution across directories
4. **Metadata Efficiency**: Metadata files remain small (<100KB) even with hundreds of cached ranges, enabling sub-10ms parsing
5. **Range Storage**: Full objects, partial ranges, and multipart uploads all use the same storage model (everything is a range)
6. **Concurrent Access**: Separate files enable efficient concurrent reads without lock contention
7. **Lazy Directory Creation**: Hash directories created on-demand, not at startup
8. **Fast Lookups**: O(1) hash computation + O(log n) directory traversal = sub-millisecond lookups

### Filename Conventions

**Metadata Files**: `{sanitized_object_key}.meta`
- Object key is percent-encoded for filesystem safety
- Special characters (/, \, :, *, ?, ", <, >, |, %) are encoded
- Long keys (>200 chars after encoding) use BLAKE3 hash as filename

**Range Files**: `{sanitized_object_key}_{start}-{end}.bin`
- Same sanitization as metadata files
- Byte range appended to filename

**Examples**:
```
Object key: path/to/file.txt
Metadata: path%2Fto%2Ffile.txt.meta
Range: path%2Fto%2Ffile.txt_0-8388607.bin

Object key: very/long/path/that/exceeds/200/characters/after/encoding...
Metadata: a7f3c2d1e5b8...9f4a.meta (64 hex chars = BLAKE3 hash)
Range: a7f3c2d1e5b8...9f4a_0-8388607.bin
```

**Part Number Request Handling**: GET requests with `partNumber` query parameters are cached as ranges:

**Content-Range Parsing**: S3 GetObjectPart responses include `Content-Range` headers (e.g., "bytes 0-8388607/5368709120") that provide exact byte offsets for storing parts as ranges.

**Part Range Storage**: Each part's byte range is stored in `ObjectMetadata.part_ranges` as a `HashMap<u32, (u64, u64)>` mapping part number to `(start, end)`. This supports variable-sized parts — no uniform size assumption.

**Example**: 5GB object with 3 variable-sized parts:
- Part 1: (0, 10485759) — 10MB
- Part 2: (10485760, 15728639) — 5MB
- Part 3: (15728640, 24117247) — 8MB

All subdirectories are created lazily on first write with proper permissions.

## Access Tracking

## Overview

The proxy implements a distributed access tracking system that records **per-range** access counts and timestamps without blocking concurrent reads. This data is used by eviction algorithms (LRU, TinyLFU) to make intelligent decisions about which individual ranges to evict. Access statistics are tracked at the range level, not the object level, enabling fine-grained eviction where hot ranges are retained even if other ranges of the same object are cold.

## Architecture

The access tracking system uses **RAM buffering** with periodic flush to per-instance time-bucketed log files, followed by consolidation:

```
cache/
├── metadata/xx/yyy/
│   └── key.meta                    # Contains access_count, last_accessed per range
└── access_tracking/
    ├── 10-30-00/                   # Time bucket (HH-MM-00)
    │   ├── instance-a.log          # Per-instance access log
    │   ├── instance-b.log
    │   └── .lock                   # Per-bucket consolidation lock
    └── 10-31-00/                   # Current bucket (not processed)
        └── instance-a.log
```

**Key Features:**
- RAM buffer for access entries (reduces disk I/O dramatically)
- Periodic flush every 5 seconds (configurable)
- Per-instance log files (no cross-instance contention on shared storage)
- Time-bucketed consolidation for eviction decisions

## How It Works

### 1. Recording Accesses (RAM Buffered)

When a range or HEAD is accessed, the tracker:

1. Adds entry to in-memory buffer (no disk I/O)
2. Increments access counter
3. Returns immediately (<1ms)

```
Range Read → Add to RAM buffer: "bucket/key:0-8388607"
          → Return immediately (< 1ms, no disk I/O)

HEAD Read → Add to RAM buffer: "bucket/key:HEAD"
         → Return immediately (< 1ms, no disk I/O)
```

**Performance**: Recording completes in <1ms with zero disk I/O.

### 2. Periodic Buffer Flush

Every 5 seconds (or when buffer reaches 10,000 entries), the buffer is flushed to disk:

1. Takes all entries from RAM buffer
2. Groups entries by time bucket
3. Appends to per-instance log file in each bucket
4. Single disk write per bucket (batched)

```
Buffer Flush Flow:
1. Take all entries from RAM buffer (atomic swap)
2. Group by time bucket (HH-MM-00)
3. For each bucket:
   - Append all entries to {instance_id}.log
   - Single write operation per bucket
4. Update flush timestamp
```

**Log Format** (one line per access):
```
bucket/key:0-8388607     # Range access (GET)
bucket/key:HEAD          # HEAD access
```

### 3. Periodic Consolidation

Every ~60 seconds (with jitter), a background task consolidates access data:

1. Finds safe-to-process time buckets (ended at least 15 seconds ago)
2. Acquires per-bucket lock (non-blocking, skips if locked)
3. Reads and aggregates log files from all instances
4. Updates metadata files with consolidated access counts and timestamps
5. Deletes processed log files
6. Removes empty bucket directories

```
Consolidation Flow:
1. Find old time buckets (not current minute, ended 15+ seconds ago)
2. For each bucket:
   - Try to acquire .lock (skip if held by another instance)
   - Read all {instance_id}.log files
   - Aggregate access counts per (cache_key, range/HEAD)
   - Update corresponding .meta files with aggregated stats
   - Delete processed log files
   - Remove bucket directory if empty
3. Log consolidation results
```

### 4. Metadata Updates

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

**`staged`** records whether the range counts toward the write (staging) tier, decided
once when the range was written and never re-derived. It is optional: a `.meta` written
before this field existed omits it, and such a range is classified from the object's
`is_write_cached` flag instead. That fallback is why the field is nullable rather than a
plain boolean — reading a missing field as `false` would report the whole staging tier as
empty on the first scan after an upgrade.

Membership is per range because credits and debits are per range. A write-through PUT
stages one range; a later GET for a different range of the same object caches that one to
the read tier, while the object stays flagged until it graduates. Only the first range's
bytes belong to `write_cache_size`.

**Atomic Updates**: Metadata updates use temp file + rename pattern for atomicity.

**Note**: Metadata is only updated during consolidation, not on every access. This dramatically reduces disk I/O while maintaining accurate access statistics for eviction decisions.

## Integration with Eviction

The eviction system uses per-range access tracking data to make decisions:

- **LRU (Least Recently Used)**: Sorts ranges by `last_accessed` (oldest first)
- **TinyLFU**: Decayed-frequency scoring — victim is the range minimizing `(decayed_frequency(access_count, idle_secs), last_accessed)`. `access_count` halves once per hour of idle time, so a frequently-accessed range stays shielded from a single large one-hit read; recency only breaks ties among equally-decayed ranges.

Each range is evaluated independently, allowing hot ranges to be retained even if other ranges of the same object are cold. See [Range-Based Disk Cache Eviction](EVICTION.md#eviction-algorithms) for details.

**Before Eviction**: The system triggers consolidation to ensure recent accesses are reflected in metadata before making eviction decisions.

## Multi-Instance Coordination

The access tracking system is designed for multi-instance deployments:

- **Per-Instance Log Files**: Each instance writes to its own `{instance_id}.log` files
- **No Write Conflicts**: Separate files per instance eliminate write conflicts
- **Per-Bucket Locking**: Each time bucket has its own lock for consolidation
- **Cross-Instance Aggregation**: Consolidation reads log files from all instances
- **Jittered Timing**: Each instance has a unique consolidation offset to spread load

## Journal Internals

The operator-facing description of journal-based metadata writes — why they exist, what
lag means, and how to spot a stalled consolidator — is in
[SHARED_STORAGE.md](SHARED_STORAGE.md#journal-based-metadata-writes). This section covers
the buffer thresholds, entry formats, and apply ordering.

## Architecture

**CacheHitUpdateBuffer (RAM Layer)**:
- Buffers TTL refresh and access count updates in RAM
- Flushes to per-instance journal file every 5 seconds
- Auto-flushes when buffer reaches 10,000 entries
- Force flush available for shutdown scenarios

**Per-Instance Journal Files**:
- Location: `metadata/_journals/{instance_id}.journal`
- Format: Newline-delimited JSON (one entry per line)
- Append-only writes (no read-modify-write)
- Each instance writes only to its own journal

**Journal Entry Types**:
- `TtlRefresh`: Updates `expires_at` for a cached range
- `AccessUpdate`: Increments `access_count` and updates `last_accessed`

**JournalConsolidator (Background Task)**:
- Runs every 5 seconds by default (`shared_storage.consolidation_interval`, range 1-60s)
- Reads entries from all instance journals
- Groups entries by cache key
- Acquires exclusive lock on metadata file
- Applies entries in timestamp order
- Truncates processed journal files

## Lock Acquisition with Retry

The consolidator uses exponential backoff with jitter for lock acquisition:

```
Attempt 1: Try lock → Contention → Wait 100ms + jitter
Attempt 2: Try lock → Contention → Wait 200ms + jitter
Attempt 3: Try lock → Success → Apply updates
```

**Configuration**:
- Max retries: 5 (default)
- Initial backoff: 100ms
- Max backoff: 5 seconds
- Jitter factor: 0.3 (±30% randomization)

## Journal File Format

```json
{"timestamp":{"secs_since_epoch":1704067200},"instance_id":"proxy-1","cache_key":"bucket/object.txt","range_spec":{"start":0,"end":8388607},"operation":"TtlRefresh","new_ttl_secs":3600}
{"timestamp":{"secs_since_epoch":1704067201},"instance_id":"proxy-1","cache_key":"bucket/object.txt","range_spec":{"start":0,"end":8388607},"operation":"AccessUpdate","access_increment":5}
```

## Consolidation Process

1. **Discover pending cache keys**: Scan all `*.journal` files for unique cache keys
2. **For each cache key**:
   - Collect all entries from all instance journals
   - Sort entries by timestamp (oldest first)
   - Acquire exclusive lock on metadata file
   - Load metadata from disk
   - Apply each entry in order:
     - `TtlRefresh`: Update `expires_at` for matching range
     - `AccessUpdate`: Increment `access_count`, update `last_accessed`
   - Write updated metadata to disk
   - Release lock
3. **Cleanup**: Truncate all processed journal files

## Error Handling

**Lock Contention**:
- Exponential backoff with jitter prevents thundering herd
- After max retries, consolidation skips the key (retried next cycle)

**Metadata File Missing**:
- Journal entries for non-existent metadata are skipped
- Logged as warning (may indicate evicted cache entry)

**Journal Parse Errors**:
- Invalid JSON lines are logged and skipped
- Valid entries in same file are still processed

## Monitoring

Journal operations are logged at DEBUG/INFO level:

```
DEBUG Cache hit update buffered: cache_key=bucket/object.txt, type=TtlRefresh
INFO  Cache hit buffer flushed: entries=150, duration=5ms
INFO  Journal consolidation completed: cache_key=bucket/object.txt, entries=5, duration=12ms
```

# Configuration

Access tracking is automatic and requires no configuration. Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Flush interval | 5 seconds | How often RAM buffer is flushed to disk |
| Max buffer entries | 10,000 | Buffer size before forced flush |
| Consolidation interval | 5 seconds | How often journal entries are consolidated to metadata (1-60s) |
| Bucket safe age | 15 seconds | Minimum age before a bucket can be processed |
| Consolidation jitter | 0-10 seconds | Per-instance offset to spread consolidation load |

# Monitoring

Buffer flush results are logged at DEBUG level:

```
DEBUG Access buffer flushed: entries=150, buckets=1, duration=5ms, errors=0
```

Consolidation results are logged at INFO level:

```
INFO Disk cache access tracking flush completed: trigger=periodic, logs=3, keys=45, ranges=89, duration=23.50ms, errors=0
```

**Log Fields**:
- **logs**: Number of log files processed
- **keys**: Number of unique objects updated
- **ranges**: Total access records consolidated
- **duration**: Time spent consolidating

## Eviction Lock Internals

The eviction lock's purpose and tuning live in
[SHARED_STORAGE.md](SHARED_STORAGE.md#distributed-eviction). Two structures are written
to `cache_dir/locks/global_eviction.lock`, and it is worth knowing they are distinct:

# Lock Mechanism

The coordination uses a filesystem-based lock stored at:

```
cache_dir/
└── locks/
    └── global_eviction.lock    # Global eviction coordinator lock
```

**Lock File Format** (JSON):

```json
{
  "instance_id": "proxy-host-1:12345",
  "process_id": 12345,
  "hostname": "proxy-host-1",
  "acquired_at": "2024-01-15T10:30:00Z",
  "timeout_seconds": 60
}
```

`EvictionLockPayload` is the fence token, and carries different fields from
`GlobalEvictionLock` above: a `uuid` regenerated on every acquisition, `acquired_at_ms`,
and `hostname`. The holder re-reads it before each batch of filesystem mutations and
aborts the pass if the UUID no longer matches, logging:

```
Eviction fence lost: expected uuid=<x>, found uuid=<y> (holder=<host>). Aborting eviction pass.
```

# Lock Lifecycle

## 1. Lock Acquisition

When an instance needs to evict:

1. Check if lock file exists
2. If exists, read lock metadata and check timestamp
3. If timestamp is older than timeout → lock is **stale** → forcibly acquire
4. If timestamp is fresh → lock is **held** → skip eviction
5. If no lock exists → create lock file atomically

**Atomic Operations**: Lock creation uses temp file + rename pattern to prevent race conditions.

## 2. Lock Hold

While holding the lock:

- Instance performs eviction using configured algorithm (LRU/TinyLFU)
- Other instances skip eviction and log the reason
- Lock timeout prevents indefinite holding if instance crashes

## 3. Lock Release

After eviction completes (success or failure):

1. Verify ownership by reading lock file
2. Delete lock file if owned by current instance
3. Log warning if lock is missing or owned by another instance

### Measured lock performance

**Measured Performance** (from the performance test suite):

| Operation | Average | P95 | Design Target |
|-----------|---------|-----|---------------|
| Lock Acquisition | 410µs | 565µs | 1-5ms |
| Lock Release | 466µs | 584µs | 1-5ms |
| Stale Lock Check | 1.6ms | 3.7ms | ~1ms |
| Full Cycle | 875µs | 1.2ms | 2-10ms |

**Key Findings**:
- Lock operations are **4-10x faster** than design targets
- Average overhead per eviction coordination: **~4.3ms**
- Mutual exclusion properly maintained under load (81.54% lock utilization)
- No significant performance regression introduced

**Impact on Eviction**:
- Lock coordination adds minimal overhead (~4ms per eviction)
- Typical eviction operations take hundreds of milliseconds to complete
- Lock overhead is <1% of total eviction time
- Performance impact is negligible compared to benefits of preventing over-eviction

**Under Load** (3 instances, 5 seconds):
- All instances successfully acquired lock multiple times (62-64 acquisitions each)
- Lock utilization: 81.54% (efficient without over-contention)
- Failed acquisitions return immediately (no blocking)
- System remains responsive even under contention

## Size Tracking Internals

The operator view, including the concurrent-write over-counting this design is subject to,
is in [SHARED_STORAGE.md](SHARED_STORAGE.md#size-tracking).

# Architecture

Cache size tracking uses an in-memory `AtomicI64` accumulator per proxy instance:

```
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

**Key Benefits:**
- **Zero NFS Overhead**: Size tracked at write/eviction time using atomic operations
- **No Timing Gaps**: Size recorded immediately when data is written, not during consolidation
- **Single Source of Truth**: Consolidator sums all delta files under global lock
- **Crash Recovery**: At most 5 seconds of deltas lost; daily validation corrects drift

# How It Works

## In-Memory Accumulator

Each proxy instance maintains an `AtomicI64` accumulator that tracks the net size delta since the last flush:

```rust
// On successful range write
accumulator.add(compressed_size);

// On range eviction
accumulator.subtract(compressed_size);
```

**Write Cache Tracking**: A separate `write_cache_delta` accumulator tracks write-cached ranges (PUT operations and multipart uploads).

## Delta File Flush

Every consolidation cycle (5 seconds by default), each instance flushes its accumulated delta to a per-instance file:

```
Delta File (size_tracking/delta_{instance_id}_{seq}.json):
{
  "delta": 1048576,
  "write_cache_delta": 0,
  "instance_id": "proxy1.example.com:12345",
  "timestamp": "2026-01-26T15:30:00.000Z"
}
```

The flush uses atomic swap-to-zero: if the file write fails, the swapped values are restored to the accumulator.

## Consolidator Integration

The consolidator (under global lock) reads all delta files, sums them, and updates the size state:

```
Consolidation Cycle:
1. Flush own accumulator to delta file (before lock)
2. Acquire global consolidation lock
3. Read all delta_*.json files from size_tracking/
4. Sum delta and write_cache_delta values
5. Add sums to size_state.json (clamping to 0)
6. Delete each consolidated delta file
7. Process journal entries for metadata updates only
8. Trigger eviction if over capacity
9. Release lock
```

Note the delta filename is `delta_{instance_id}_{seq}.json` — a **new,
sequence-numbered file per flush**, not one file per instance. The consolidator deletes
each file after reading it rather than zeroing it, deliberately: zeroing would lose any
delta an instance flushed between the read and the reset.

## Size State Persistence

The authoritative size state is stored in `size_state.json`:

```
Size State File (size_tracking/size_state.json):
{
  "total_size": 5368709120,
  "write_cache_size": 268435456,
  "last_consolidation": 1706282400,
  "consolidation_count": 12345,
  "last_updated_by": "proxy1.example.com:12345"
}
```

**Fields:**
- **total_size**: Total cache size in bytes (read cache + write cache)
- **write_cache_size**: Write cache bytes — a **subset** of `total_size`, not an addition to
  it, so read-cache bytes are `total_size - write_cache_size`. Counts the compressed bytes
  of objects written through the cache that have **not yet been read**. An object leaves
  this figure by being read for the first time (it becomes read-cached, and the bytes stay
  on disk) or by being removed. Maintained only by the consolidator: adjusted incrementally
  as objects are cached, read, and removed, and re-grounded absolutely by a full validation
  scan.

  Every credit and debit is by `compressed_size` and is classified through one shared
  predicate, so a range cannot be added under one rule and subtracted under another. The
  credit is applied when the cache entry is written, by whichever path wrote it — the two
  single-object upload paths write their entry directly rather than through the journal (so
  that an immediate read of a just-uploaded object is a cache hit), which means they credit
  the accounting themselves; multipart completion credits via its journal entries. A range
  that already existed on shared storage is not credited again, because the instance that
  published it credited it then.

  **A credit is only durable if it lands on the consolidator the background consolidation
  task holds.** Deltas accumulate in memory on the consolidator instance and are written to
  a delta file only by `run_consolidation_cycle`, which runs on the single `Arc` captured at
  startup. There is therefore exactly one `JournalConsolidator` per process, created on the
  first `create_configured_disk_cache_manager()` call and reused by every later one — the
  same applies to the `HybridMetadataWriter` and `CacheHitUpdateBuffer` created alongside it,
  which are likewise drained only by their own startup tasks. Constructing a second instance
  of any of the three does not duplicate a component; it creates a sink whose contents are
  discarded, because nothing that drains it holds a reference. The cycle's idle
  short-circuit makes the failure total rather than partial: `has_pending_delta()` is false
  on the task's own accumulator, so it returns before flushing anything.

  If you add a code path that needs one of these, take it from
  `create_configured_disk_cache_manager()` or the `get_*` accessors. Do not construct one.
  `journal_components_identity_tests` in `src/cache.rs` pins this.

  **Every deletion of a range file must debit, and must release the range's dedup entry.**
  A path that removes a `.bin` and does not debit leaves the figure holding bytes the disk
  no longer has; the two write paths that replace an object's content
  (`store_put_as_write_cached_range_with_ttl` on a re-PUT and
  `store_full_object_as_range_new` when it supersedes partial ranges) did exactly that, so
  each overwrite added a phantom copy. Both now route through `remove_range_files` +
  `debit_removed_ranges`.

  Two rules that are easy to get half-right:

  - **Debit only files that existed and deleted cleanly.** Iterating `metadata.ranges`
    directly charges for files that were already gone — a phantom debit, permanent until the
    next full scan because nothing re-credits it. `remove_range_files` returns a filtered
    list for this reason; do not widen it.
  - **Use `SizeAccumulator::subtract_range`, not `subtract`, wherever the range's identity
    is known.** `add_range` credits only when it can insert into the dedup set, and
    `subtract` leaves that entry behind — so a delete-then-rewrite debits once and credits
    nothing, leaving the figure *short*. Adding the re-PUT debit with a plain `subtract`
    took the total to zero for an object still on disk. `subtract` remains for callers with
    no range identity to offer; the dedup set is cleared only by a validation scan, never by
    a flush, so an entry left behind suppresses credits for up to a full validation interval.

  Classification is always `cache_types::is_staged_range_spec`, which reads the range's
  own recorded membership (`RangeSpec.staged`) and falls back to the `is_write_cached` of
  the `.meta` the deleting call actually read only for a range written before that field
  existed. Membership is per range, not per object: a range credited to the read tier and
  then attached to a still-flagged object must not be debited from the write tier. Do not consult
  `graduation_accounted` — it is the consolidator's exactly-once token and no debit site
  reads it. Reading the flag at delete time is what keeps a concurrent graduation from being
  debited twice, and that in turn relies on `refresh_write_cache_ttl` writing the
  flag-cleared `.meta` *before* appending its `Graduation` entry.
- **last_consolidation**: Unix timestamp of last consolidation cycle
- **consolidation_count**: Number of consolidation cycles completed
- **last_updated_by**: Instance ID that last updated the state

## Startup Recovery

On startup, the consolidator loads the existing size state:

```
Recovery Flow:
1. Try to load size_state.json
2. If found → Initialize with persisted values
3. If not found → Start at 0, validation will correct
```

Recovery is instant (single file read) regardless of cache size. The in-memory accumulator starts at zero.

## HEAD Cache Cleanup

The validation scan includes automatic cleanup of expired HEAD cache entries:

## Periodic Cleanup (During Validation)

When validation runs, the system scans HEAD cache files and deletes expired entries:

```
Validation Scan:
1. Scan .meta files (for size validation and TTL checking)
2. For each .meta file:
   - Check if HEAD TTL has expired (head_expires_at field)
   - Check if all ranges have expired
   - If both HEAD and ranges expired: delete .meta file
   - Track validation results
3. Report validation results in metadata
```

**Note**: HEAD expiry alone does not delete the `.meta` file - ranges may still be valid. The file is only deleted when both HEAD and all ranges have expired.

Expired HEAD entries are also cleaned on read; see
[CACHE_FRESHNESS.md](CACHE_FRESHNESS.md#lazy-deletion-on-read).

## See Also

- [CACHING.md](CACHING.md) — what gets cached, and what bypasses the cache
- [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md) — how a read is satisfied from these structures
- [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md) — TTL, revalidation, and conditional requests
- [EVICTION.md](EVICTION.md) — how space is reclaimed from them
- [SHARED_STORAGE.md](SHARED_STORAGE.md) — the multi-instance operator view
- [CONFIGURATION.md](CONFIGURATION.md) — field reference
