# Caching Guide

## Table of Contents

- [Overview](#overview)
- [Cache Architecture](#cache-architecture)
  - [Two-Tier Cache System](#two-tier-cache-system)
  - [RAM-Disk Cache Coherency](#ram-disk-cache-coherency)
  - [RAM Metadata Cache](#ram-metadata-cache)
  - [Streaming Response Architecture](#streaming-response-architecture)
  - [Range Storage Architecture](#range-storage-architecture)
  - [Cache Entry Structure](#cache-entry-structure)
  - [Cache Directory Structure](#cache-directory-structure)
    - [Access Point and MRAP Cache Key Prefixing](#access-point-and-mrap-cache-key-prefixing)
- [Time-To-Live (TTL) Configuration](#time-to-live-ttl-configuration)
  - [GET TTL](#get-ttl-get_ttl)
  - [PUT TTL](#put-ttl-put_ttl)
  - [HEAD TTL](#head-ttl-head_ttl)
  - [Cache Expiration Modes](#cache-expiration-modes-actively_remove_cached_data)
- [Cache Validation Flow](#cache-validation-flow)
- [Write Cache Configuration](#write-cache-configuration)
- [Bucket-Level Cache Settings](#bucket-level-cache-settings)
- [RAM-Disk Cache Coherency](#ram-disk-cache-coherency-1)
- [Range Request Optimization](#range-request-optimization)
- [Part Caching](#part-caching)
- [Download Coordination](#download-coordination)
- [Eviction Configuration](#eviction-configuration)
- [Multi-Instance Coordination](#multi-instance-coordination)
- [Journal-Based Metadata Updates (Shared Storage)](#journal-based-metadata-updates-shared-storage)
- [Range-Based Disk Cache Eviction](#range-based-disk-cache-eviction)
- [Cache Access Tracking](#cache-access-tracking)
- [Distributed Eviction Coordination](#distributed-eviction-coordination)
- [Cache Bypass for Non-Cacheable Operations](#cache-bypass-for-non-cacheable-operations)
- [Versioned Request Handling](#versioned-request-handling)

---

## Overview

The S3 Proxy provides intelligent caching to accelerate S3 access while maintaining data consistency. The proxy is a **transparent forwarder** - it only responds to client requests and cannot initiate requests to S3 (as it has no AWS credentials and cannot sign requests).

## Cache Architecture

### Two-Tier Cache System

1. **RAM Cache** (Optional, First Tier)
   - In-memory cache for hot objects, HEAD metadata, and range data
   - Configurable size limit (default: 256MB)
   - Eviction algorithms: LRU or TinyLFU (simplified frequency-recency hybrid)
   - Fastest access path
   - Compression optimization: Eliminates decompress/recompress cycles during disk-to-RAM promotion
   - Size limits enforced on compressed data (allows large compressible files to be cached)
   - **RAM Metadata Cache**: Caches `NewCacheMetadata` objects to reduce disk I/O for both HEAD and GET requests
   - **Range data caching**: Both streaming and buffered paths check RAM cache before disk and promote disk hits to RAM cache (key format: `{cache_key}:range:{start}:{end}`)
   - Note: PUT-cached objects are NOT stored in RAM cache (disk only)

2. **Disk Cache** (Second Tier)
   - File-based persistent cache
   - Supports shared volumes for multi-instance deployments
   - Eviction algorithms: LRU or TinyLFU (simplified frequency-recency hybrid)
   - Range-level eviction granularity for optimal cache utilization
   - LZ4 compression for space efficiency
   - Range storage architecture: `.meta` (lightweight metadata) + `.bin` (range data)

### RAM-Disk Cache Coherency

When both RAM and disk caches are enabled, the proxy maintains coherency between them through two mechanisms:

1. **Periodic Verification**: RAM cache entries are verified against disk metadata to detect stale data
2. **Access Statistics Propagation**: RAM cache hits are batched and written to disk metadata for eviction decisions

#### How Verification Works

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

#### Access Statistics Propagation

RAM cache hits update access statistics that are propagated to disk metadata via the journal system:

```
RAM Hit → DiskCacheManager.record_range_access() called
       → CacheHitUpdateBuffer buffers the update in RAM
       → Periodic flush (every 5s) writes to per-instance journal file
       → JournalConsolidator applies journal entries to metadata files
```

**Why This Matters:**
- Disk eviction algorithms (LRU/TinyLFU) need accurate access statistics
- Without propagation, frequently-accessed RAM entries would appear "cold" on disk
- Hot data could be evicted from disk while still being served from RAM
- When RAM evicts the entry, disk would have already evicted it → cache miss

#### Journal-Based Access Tracking

The journal system provides efficient, race-condition-free access tracking on shared storage:

- **RAM buffering**: Updates collected in `CacheHitUpdateBuffer` (reduces disk I/O)
- **Per-instance journals**: Each proxy instance writes to its own journal file (no contention)
- **Background consolidation**: `JournalConsolidator` applies updates with proper locking
- **Atomic updates**: Lock acquisition with retry ensures consistency on NFS

#### Configuration Options

```yaml
cache:
  shared_storage:
    enabled: true                        # Enable journal-based updates for shared storage
    consolidation_interval: "5s"         # Time between consolidation runs (1-60s, default: 5s)
    consolidation_size_threshold: 10485760  # Journal size threshold for consolidation (10MB)
    lock_timeout: "5s"                   # Lock acquisition timeout
    lock_max_retries: 3                  # Max retries for lock acquisition
```

**Configuration Guidance:**

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| flush_interval | More disk I/O, fresher stats | Less I/O, staler stats | 60s for most workloads |
| flush_threshold | More frequent flushes | Larger batches | 100 for balanced performance |
| verification_interval | More disk reads, fresher data | Less I/O, risk of stale data | 1s for consistency |

#### Performance Characteristics

**Verification:**
- Target: <10ms per verification
- Reads only metadata file (not range data)
- Throttled to avoid excessive I/O

**Batch Flush:**
- Processes all pending updates in single pass
- File lock held <100ms per key
- Logs duration and error counts

#### Monitoring Metrics

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

#### Troubleshooting

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

### RAM Metadata Cache

The proxy implements a RAM Metadata Cache that stores `NewCacheMetadata` objects in memory to reduce disk I/O for both HEAD and GET requests. This provides sub-millisecond metadata lookups while maintaining consistency with disk-based storage.

#### Architecture

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

#### Eviction with LRU

The RAM Metadata Cache uses simple LRU eviction (not TinyLFU):

**Why LRU (not TinyLFU):**
- All metadata entries are similar size (~1-2KB)
- TinyLFU complexity not needed for uniform-size entries
- Simple LRU provides optimal performance for metadata caching

**Eviction Behavior:**
- When cache reaches `max_entries`, least recently accessed entries are evicted
- Eviction only removes from RAM cache, not from disk
- Evicted entries are reloaded from disk on next access

#### Request Flows

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

#### TTL Independence

| Cache Layer | TTL Field | Default | Purpose |
|-------------|-----------|---------|---------|
| RAM MetadataCache | `refresh_interval` | 5s | How often to re-read from disk |
| HEAD (disk) | `head_expires_at` | 1 minute | When to re-fetch HEAD from S3 |
| GET range (disk) | `RangeSpec.expires_at` | ~10 years | When to re-fetch range from S3 |

**Key insight**: HEAD expiry doesn't delete the `.meta` file - it just means HEAD needs revalidation from S3. Ranges can still be valid and served from cache.

#### Per-Key Locking

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

#### Stale File Handle Recovery

The cache implements retry logic for stale file handle errors (common with NFS):

```
Read attempt → ESTALE error → Retry with backoff (up to 3 times)
            → If persists: invalidate cache entry, return error
```

**Configuration:**
- `stale_handle_max_retries`: Maximum retry attempts (default: 3)
- Exponential backoff between retries (10ms, 20ms, 30ms)

#### Configuration

```yaml
cache:
  metadata_cache:
    enabled: true
    refresh_interval: "5s"      # How often RAM cache re-reads from disk
    max_entries: 10000          # Maximum entries in RAM cache
    stale_handle_max_retries: 3 # Retry count for stale file handles
  
  # HEAD-specific TTL (independent of GET TTL)
  head_ttl: "60s"               # How long HEAD is valid before S3 re-fetch
```

**Important Notes:**
- MetadataCache is separate from RAM data cache (which stores range bytes)
- HEAD TTL is independent of GET TTL
- Refresh interval controls disk re-reads, not S3 re-fetches

#### Cache Statistics

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

#### Monitoring and Logging

**Cache Operations:**
```
INFO Metadata cache HIT: cache_key=/bucket/object.txt
INFO Metadata cache MISS: cache_key=/bucket/object.txt loaded_from_disk=true
INFO Metadata cache STALE: cache_key=/bucket/object.txt refreshing_from_disk=true
```

**Eviction Events:**
```
INFO Metadata cache eviction: evicted_key=/bucket/old-object.txt reason=capacity_limit
```

**Stale Handle Recovery:**
```
WARN Stale file handle error: cache_key=/bucket/object.txt attempt=1 retrying=true
ERROR Stale file handle persisted: cache_key=/bucket/object.txt attempts=3 invalidating=true
```

#### Benefits

**Performance:**
- Sub-millisecond metadata lookups for hot entries
- Reduced disk I/O for frequently accessed objects
- Per-key locking prevents thundering herd

**Efficiency:**
- Unified storage reduces disk space (no separate HEAD cache)
- LRU eviction optimal for uniform-size metadata entries
- Memory usage: ~15-25MB for 10,000 entries

**Consistency:**
- Independent HEAD/GET TTLs in same file
- Stale file handle recovery for NFS reliability
- Refresh interval ensures disk consistency

#### Use Cases

**API Gateway / Load Balancer Health Checks:**
```
Scenario: Load balancer checks object existence every 5 seconds
Without RAM Metadata Cache: Each check reads from disk
With RAM Metadata Cache: First check loads to RAM, subsequent served from RAM
Result: 99%+ reduction in disk I/O, <1ms response time
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

### Streaming Response Architecture

The proxy uses a streaming architecture for large responses (> 1MB) to eliminate buffering and reduce memory usage:

**How It Works:**

1. **S3 Response Classification**:
   - Responses < 1MB: Buffered mode (collected in memory, then cached and returned)
   - Responses > 1MB: Streaming mode (data flows directly to client while being cached in background)

2. **TeeStream for Simultaneous Streaming and Caching**:
   - Large responses are wrapped in a TeeStream
   - Data streams to client immediately (first byte latency < 100ms)
   - Data is simultaneously sent to a background task for caching
   - No buffering of entire response in memory

3. **Benefits**:
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
- Streaming threshold: 1MB (hardcoded, based on Content-Length header)
- Chunk size for disk streaming: 64KB
- No configuration changes required

#### Performance Optimizations

The streaming and caching path includes several optimizations for maximum throughput:

**Zero-Copy Cache Writes (v0.7.4+):**
- When compression is disabled, cache writes use the original data slice directly
- No `data.to_vec()` copy operation - writes directly from the incoming buffer
- Reduces memory allocations and CPU usage during cache-miss streaming
- Particularly beneficial for large file transfers (8MB+ ranges)

**JournalOnly Metadata Mode (v0.7.3+):**
- Cache-miss range metadata writes use journal-only mode on shared storage
- Eliminates lock contention when multiple instances cache the same object
- Journal consolidator merges entries asynchronously without blocking streaming
- Prevents the 4x performance degradation seen with lock contention on NFS

**Bytes Reference Counting:**
- TeeStream uses `Bytes::clone()` which is reference-counted (not a deep copy)
- Data is shared between client stream and cache writer without duplication
- Memory efficient even for large chunks

**Async Cache Writer:**
- Cache writes happen in a background task, not blocking the client stream
- Uses mpsc channel with `try_send` to avoid backpressure blocking
- If cache writer falls behind, chunks are dropped (client stream continues)

### Range Storage Architecture

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

### Cache Entry Structure

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

### Cache Directory Structure

The disk cache uses a **bucket-first hash-based sharding** architecture with range storage. This structure scales to billions of cached objects per bucket without filesystem performance degradation.

#### Directory Hierarchy

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
    ├── delta_{instance_id}.json  # Per-instance delta files (flushed every 5s)
    ├── validation.json        # Validation metadata
    └── validation.lock        # Validation lock file
```

#### Bucket-First Organization

**Cache Key Format**: `/bucket/object_key`

For regular path-style and virtual-hosted-style requests, the cache key is the S3 request path (e.g., `/my-bucket/path/to/object.txt`).

Examples:
- `/my-bucket/path/to/object.txt`
- `/my-bucket/file.jpg`
- `/my.bucket.name/deeply/nested/path/data.bin`

#### Access Point and MRAP Cache Key Prefixing

S3 Access Point and Multi-Region Access Point (MRAP) requests use a different URL structure than regular bucket requests. The endpoint identity appears in the Host header (virtual-hosted style) or the first path segment (path-style with alias), not in the bucket position. The proxy detects these patterns and generates cache key folders with AWS reserved suffixes to prevent namespace collisions with S3 bucket names.

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

**Regular Bucket Requests**

Regular path-style requests (`s3.{region}.amazonaws.com`) and virtual-hosted-style requests (`{bucket}.s3.{region}.amazonaws.com`) are unaffected — their cache keys remain path-only with no prefix or suffix appended.

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

#### Hash-Based Sharding

**Why BLAKE3?**
- 10x faster than SHA-256
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

#### Directory Purpose

- **metadata/{bucket}/{XX}/{YYY}/**: Unified metadata files (`.meta`) containing object metadata, range index, HEAD TTL fields, upload state, and part caching fields - NO embedded binary data, typically <100KB even with hundreds of ranges. HEAD and GET share the same `.meta` file with independent TTLs.
- **ranges/{bucket}/{XX}/{YYY}/**: ALL cached data stored as binary files (`.bin`) - includes full objects from PUT (stored as range 0-N), partial ranges from GET, and assembled multipart uploads
- **locks/**: Lock files for coordinating writes in shared cache deployments and distributed eviction coordination

#### Architecture Benefits

1. **Scalability**: Scales to billions of objects per bucket without filesystem performance degradation (1M+ leaf directories)
2. **Bucket Isolation**: Per-bucket cache management (clear cache by bucket with `rm -rf cache_dir/{type}/{bucket}/`)
3. **Uniform Distribution**: BLAKE3 hashing ensures even file distribution across directories
4. **Metadata Efficiency**: Metadata files remain small (<100KB) even with hundreds of cached ranges, enabling sub-10ms parsing
5. **Range Storage**: Full objects, partial ranges, and multipart uploads all use the same storage model (everything is a range)
6. **Concurrent Access**: Separate files enable efficient concurrent reads without lock contention
7. **Lazy Directory Creation**: Hash directories created on-demand, not at startup
8. **Fast Lookups**: O(1) hash computation + O(log n) directory traversal = sub-millisecond lookups

#### Filename Conventions

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

## Time-To-Live (TTL) Configuration

### GET TTL (`get_ttl`)

**Default: Infinite (cache forever unless explicit headers)**

Controls how long cached **data** remains valid for GET requests.

```yaml
cache:
  get_ttl: "315360000s"  # ~10 years (infinite caching)
  actively_remove_cached_data: false  # Lazy expiration (default)
```

**Default Behavior:**
- By default, cached data never expires unless S3 provides explicit cache headers (Cache-Control, Expires)
- This matches S3's immutable object model - objects don't change unless explicitly overwritten
- Expiration is checked lazily on access (not actively removed)

**When GET_TTL expires:**
- **Lazy mode** (actively_remove_cached_data: false, default): Expired entries remain on disk until accessed
  - Intended for fixed-capacity deployments where eviction manages space
  - On next request, proxy revalidates with S3 using conditional requests (see below)
- **Active mode** (actively_remove_cached_data: true): Background process actively removes expired entries
  - Intended for elastic shared storage where immediate space reclamation is valuable
  - Not compatible with TTL=0 — active mode deletes expired entries, and TTL=0 entries expire immediately after caching

**Cache Revalidation:**
When an expired entry is accessed in lazy mode, the proxy uses HTTP conditional requests to validate freshness:
1. Sends `If-Modified-Since` header with cached object's `Last-Modified` timestamp
2. If S3 returns `304 Not Modified`: Object unchanged, TTL refreshed, cached data served (no data transfer)
3. If S3 returns `200 OK`: Object changed, old cached data actively removed, fresh data fetched and cached

This approach minimizes bandwidth usage while ensuring cache freshness and consistency.

**Client Conditional Requests:**
When clients provide conditional headers (If-None-Match, If-Modified-Since, If-Match, If-Unmodified-Since), the proxy uses an always-forward approach to ensure perfect HTTP compliance:

- **ALL conditional requests are forwarded to S3** with the client's original headers
- **The proxy never makes conditional decisions** based on cached metadata
- **S3 makes the authoritative decision** about whether conditions are met
- **Cache management is based solely on S3's response:**
  - **S3 returns 200 OK** → Return data to client, invalidate old cache, cache new data
  - **S3 returns 304 Not Modified** → Return 304 to client, refresh cache TTL
  - **S3 returns 412 Precondition Failed** → Return 412 to client, do NOT invalidate cache

This approach eliminates edge cases and ensures the proxy never provides incorrect responses based on potentially stale cached metadata. The proxy acts as a transparent forwarder for all conditional requests, letting S3 handle the complex HTTP conditional logic.

**Important**: GET_TTL only affects GET requests. HEAD requests use HEAD_TTL independently.

### HEAD TTL (`head_ttl`)

**Default: 1 minute**

Controls how long cached **HEAD metadata** (ETag, Last-Modified, headers) remains valid for HEAD requests.

```yaml
cache:
  head_ttl: "60s"  # 1 minute
```

**HEAD_TTL Behavior:**
- HEAD_TTL only affects HEAD requests
- HEAD_TTL expiration does NOT trigger validation on GET requests
- HEAD metadata is always actively removed when HEAD_TTL expires
- GET and HEAD operations use separate TTLs and cache entries

**When HEAD_TTL expires:**
- HEAD metadata is actively removed from cache
- Next HEAD request fetches fresh metadata from S3

**Important**: HEAD_TTL and GET_TTL are completely independent. A GET request with valid GET_TTL will serve cached data regardless of HEAD_TTL status.

### Minimum TTL Values

There is no enforced minimum for TTL values. Both `head_ttl` and `get_ttl` accept any valid duration, including `"0s"`.

**TTL=0 Behavior (Always-Revalidate Mode)**:

Setting TTL to 0 creates an "always-revalidate" mode where every request triggers S3 validation:

```yaml
cache:
  get_ttl: "0s"
  head_ttl: "0s"
  actively_remove_cached_data: false  # Required for TTL=0 to work
```

- Cache still stores data (enables range merging, bandwidth savings)
- Every request triggers conditional revalidation with S3 using client's credentials
- S3 validates IAM authorization on every request
- S3 returns 304 → cached data served, bandwidth saved
- S3 returns 200 → fresh data fetched and cached
- S3 returns 403 → unauthorized, error returned

**Critical**: TTL=0 requires lazy expiration (`actively_remove_cached_data: false`). With active expiration, data would be deleted immediately after caching, making the cache ineffective.

See [Always-Revalidate Mode](ARCHITECTURE.md#shared-cache-access-model) in the Architecture documentation for security considerations and use cases.

### PUT TTL (`put_ttl`)

**Default: 1 hour**

Controls how long write-through cached objects remain valid after a PUT operation.

```yaml
cache:
  put_ttl: "3600s"  # 1 hour
```

**PUT TTL Behavior:**
- Objects cached during PUT operations start with PUT_TTL
- PUT_TTL is shorter because objects may be **written but never read** (wasted cache space)
- **If a GET request accesses the object within PUT_TTL**, the cache entry transitions to using GET_TTL (metadata-only update, no data copying)
- This optimizes for "upload once, download many times" patterns while avoiding cache pollution
- PUT-cached objects are stored using the range storage format, enabling immediate range request support

**Example Flow:**
1. Client PUTs object → Cached as range 0-N with PUT_TTL (1 hour)
2. Client GETs object 30 minutes later → Served from cache, TTL transitions to GET_TTL (metadata-only update)
3. Object now cached with GET_TTL (infinite by default)
4. Client requests byte range → Served directly from cache without S3 fetch
5. If never read within 1 hour → Expires and is removed (lazy or active depending on config)

### Cache Expiration Modes (`actively_remove_cached_data`)

The proxy supports two cache expiration modes optimized for different deployment scenarios:

**Default: false (lazy expiration)**

```yaml
cache:
  actively_remove_cached_data: false  # Lazy expiration (default)
```

#### Lazy Expiration (false) - Fixed Capacity Deployments

**Intended for**: Fixed-capacity deployments where cache size is managed by eviction algorithms rather than TTL expiration.

**How it works**:
- Expired cache entries remain on disk until accessed
- On GET request, if GET_TTL expired, fetch fresh data and update cache
- No background cleanup processes
- Cache space is managed by eviction when capacity limits are reached

**Benefits**:
- Saves CPU/IO from background cleanup processes
- Optimal for deployments with fixed disk allocations
- Eviction algorithms handle space management efficiently
- No benefit from active removal since eviction manages capacity

**Use cases**:
- Local SSD deployments with fixed cache sizes
- Container deployments with persistent volume claims
- Single-instance deployments with dedicated cache storage

#### Active Expiration (true) - Elastic Shared Storage

**Intended for**: Elastic shared storage deployments where immediate space reclamation is valuable.

**How it works**:
- Background process periodically scans and removes expired cache entries
- Frees disk space immediately when GET_TTL expires
- Proactive cleanup before eviction is needed
- Reduces pressure on eviction algorithms

**Benefits**:
- Immediate disk space reclamation
- Reduces storage costs in elastic environments
- Prevents cache from growing beyond necessary size
- Useful when storage capacity can be dynamically adjusted

**Use cases**:
- Elastic shared filesystems (e.g., AWS EFS)
- Multi-instance deployments with shared cache volumes
- Cloud deployments where storage costs scale with usage
- Environments where disk space is constrained or expensive

**Trade-offs**:
- Adds background CPU/IO overhead for scanning and cleanup
- May remove data that could still be useful if accessed soon

#### HEAD Metadata Expiration

**Important**: HEAD metadata is ALWAYS actively removed when HEAD_TTL expires, regardless of the `actively_remove_cached_data` setting. This ensures HEAD requests always get fresh metadata for object existence and properties.

### Presigned URL Support

The proxy transparently supports AWS SigV4 presigned URLs, which embed authentication credentials in query parameters for time-limited access to S3 objects.

**Expiration Enforcement:**
The proxy detects expired presigned URLs by parsing `X-Amz-Date` and `X-Amz-Expires` query parameters and rejects them immediately with 403 Forbidden, preventing access to cached data with expired credentials.

**Cache Key Generation:**
The proxy generates cache keys from the **path only**, excluding query parameters:
- Request: `/bucket/object?X-Amz-Signature=abc123`
- Cache key: `bucket/object`
- Multiple presigned URLs for the same object share the same cache entry

**TTL Strategies:**

1. **Long TTL (Default - Performance Focused)**
   ```yaml
   cache:
     get_ttl: "24h"
   ```
   - Maximum performance (no S3 calls on cache hit with valid signature)
   - Expired presigned URLs rejected before cache lookup (403 Forbidden)
   - Use when presigned URLs should continue to work until their intended expiry time, regardless of whether an underlying STS credential has expired

2. **Zero TTL (Security Focused)**
   ```yaml
   cache:
     get_ttl: "0s"
   ```
   - Every request triggers conditional revalidation (If-Modified-Since)
   - S3 responds with 304 Not Modified if data unchanged (bandwidth savings)
   - Expired presigned URLs rejected before cache lookup (403 Forbidden)
   - Use when S3 should authenticate every request

**Security Consideration:**
With long cache TTLs and valid presigned URLs, cached data remains accessible for the cache TTL duration. Expired presigned URLs are always rejected regardless of cache state. Choose TTL strategy based on your security requirements.

## Cache Validation Flow

### GET Request Scenarios

#### Scenario 1: Fresh Cache (GET_TTL Valid)

```
Client GET → Proxy checks cache → GET_TTL valid → Serve from cache
```

No S3 request needed. HEAD_TTL status is irrelevant for GET requests.

#### Scenario 2: GET_TTL Expired

```
Client GET → Proxy checks cache → GET_TTL expired
          → Forward GET to S3
          → S3 returns 200 OK with new data
          → Update cache with new GET_TTL
          → Serve fresh data
```

Full cache refresh. All parts and ranges for this object are expired together.

#### Scenario 3: Client Provides Conditional Headers (Always Forward)

```
Client GET with If-Match: "old-etag"
          → Proxy detects conditional header
          → Forward entire request to S3 with ALL client headers
          → S3 evaluates condition and responds:
             - 200 OK: Object changed, return new data, invalidate old cache, cache new data
             - 304 Not Modified: Object unchanged, return 304, refresh cache TTL
             - 412 Precondition Failed: Condition not met, return 412, keep cache unchanged
          → Proxy returns S3's response to client
```

The proxy never makes conditional decisions - S3 always makes the authoritative determination. This ensures perfect HTTP compliance and eliminates edge cases where the proxy might make incorrect decisions based on cached metadata.

**Cache Invalidation Behavior:** When cache invalidation occurs due to ETag or Last-Modified mismatch, all cached ranges for that object are immediately removed (metadata file + all range binary files). The proxy does not attempt selective range invalidation — any version mismatch triggers a complete cache purge for that object.

#### Scenario 3a: Partial Cache Hit with Cache Validation

When the proxy has partial cache coverage and needs to fetch from S3, it adds an `If-Match` header with the cached ETag to ensure the object hasn't changed since caching:

```
Client GET for bytes 0-41943039 (40MB)
          → Proxy checks cache
          → Found cached ranges: 0-8388607, 16777216-25165823, 33554432-41943039
          → Missing ranges: 8388608-16777215, 25165824-33554431
          → Proxy adds If-Match header with cached ETag (if client didn't specify one)
          → Forward original request to S3 unchanged
          → S3 validates If-Match:
             - 200 OK: Object unchanged, cache new data, merge and serve
             - 412 Precondition Failed: Object changed, invalidate cache, retry without If-Match
          → Return complete response
```

The `If-Match` validation ensures cached data is consistent with S3. If the object changed (412 response), all cached ranges are invalidated and the request is retried fresh.

#### Scenario 3b: Fully Cached Non-Contiguous Ranges

```
Client GET for bytes 0-104857599 (100MB)
          → Proxy checks cache
          → Found cached ranges: 0-8388607, 8388608-16777215, ..., 96468992-104857599 (13 ranges)
          → Missing ranges: empty
          → Load and merge 13 cached ranges
          → Return complete 100MB response
          → No S3 fetch needed
          → Log: cache_efficiency=100.00%, bytes_from_cache=104857600, bytes_from_s3=0
```

All bytes cached in non-contiguous ranges (typical after multipart upload). Served entirely from cache with ~50ms merge time.

#### Scenario 3c: TTL-Expired Cache Entry (Revalidation)

When a cached entry's TTL expires, the proxy uses `If-Modified-Since` to revalidate without re-downloading unchanged data:

```
Client GET → Proxy checks cache → GET_TTL expired
          → Proxy adds If-Modified-Since header with cached Last-Modified timestamp
          → Forward request to S3
          → S3 validates If-Modified-Since:
             - 304 Not Modified: Object unchanged, refresh TTL, serve from cache
             - 200 OK: Object changed, invalidate old cache, cache new data, serve fresh
          → Return response
```

This minimizes bandwidth usage while ensuring cache freshness.

### HEAD Request Scenarios

#### Scenario 4: Fresh HEAD Cache (HEAD_TTL Valid)

```
Client HEAD → Proxy checks HEAD cache → HEAD_TTL valid → Serve cached headers
```

No S3 request needed.

#### Scenario 5: HEAD_TTL Expired

```
Client HEAD → Proxy checks HEAD cache → HEAD_TTL expired (actively removed)
           → Forward HEAD to S3
           → S3 returns headers
           → Cache headers with new HEAD_TTL
           → Serve headers
```

HEAD metadata is actively removed when HEAD_TTL expires.

### PUT Request Scenarios

#### Scenario 6: Write-Through Caching

```
Client PUT → Forward to S3 → S3 returns 200 OK
          → Store as range 0-N with PUT_TTL
          → Mark upload_state = Complete
          → Return success to client
```

Object is immediately available for range requests.

#### Scenario 7: PUT-Cached Object Accessed via GET

```
Client GET → Proxy checks cache → Found with PUT_TTL
          → Serve from cache
          → Transition TTL from PUT_TTL to GET_TTL (metadata-only update)
```

Optimizes for "upload once, download many" pattern. No data copying during TTL transition.

#### Scenario 8: Range Request from PUT-Cached Object

```
Client GET with Range: bytes=0-1023 → Proxy checks cache → Found with PUT_TTL
                                    → Serve range from cache (no S3 fetch)
                                    → Transition TTL to GET_TTL
                                    → Return 206 Partial Content
```

PUT-cached objects support range requests immediately without fetching from S3.

### Multipart Upload Scenarios

#### Scenario 9: Multipart Upload Within Capacity

```
Client CreateMultipartUpload → Create metadata with upload_state = InProgress
Client UploadPart (part 1)   → Store part info, cumulative_size = 5MB
Client UploadPart (part 2)   → Store part info, cumulative_size = 10MB
Client CompleteMultipartUpload → Sort parts, calculate positions
                               → Store as ranges: 0-5MB, 5MB-10MB
                               → Mark upload_state = Complete
                               → Set PUT_TTL expiration
```

Completed multipart upload behaves like PUT-cached object.

#### Scenario 10: Multipart Upload Exceeding Capacity

```
Client CreateMultipartUpload → Create metadata with upload_state = InProgress
Client UploadPart (part 1)   → Store part info, cumulative_size = 500MB
Client UploadPart (part 2)   → cumulative_size would exceed capacity
                             → Mark upload_state = Bypassed
                             → Invalidate part 1
                             → Skip caching part 2
Client UploadPart (part 3)   → Skip caching (upload is Bypassed)
Client CompleteMultipartUpload → Skip caching (upload is Bypassed)
```

Upload continues to S3 normally, but caching is bypassed to respect capacity limits.

#### Scenario 11: Incomplete Upload Cleanup

```
Client CreateMultipartUpload → Create metadata with upload_state = InProgress
Client UploadPart (part 1)   → Store part info
[Client disconnects, never completes]
[1 hour passes]
Background cleanup task → Detects InProgress upload older than 1 hour
                       → Delete metadata and cached part data
                       → Free cache space
```

Prevents abandoned uploads from consuming cache space indefinitely.

#### Scenario 12: Part Number Request (Cache Miss)

```
Client GET /bucket/5GB?partNumber=1 → Proxy detects GetObjectPart request
                                   → Check cache for /bucket/5GB with part 1
                                   → No ObjectMetadata or no multipart info → Forward to S3
S3 Response → Content-Range: bytes 0-8388607/5368709120
            → x-amz-mp-parts-count: 640
            → Content-Length: 8388608
Proxy → Parse Content-Range → start=0, end=8388607, total=5368709120
      → Store as range 0-8388607 in ranges/ directory
      → Update ObjectMetadata: parts_count=640, part_ranges[1]=(0, 8388607)
      → Stream response to client with original headers
```

First part request populates part range metadata for subsequent requests.

#### Scenario 13: Part Number Request (Cache Hit)

```
Client GET /bucket/5GB?partNumber=2 → Proxy detects GetObjectPart request
                                   → Load ObjectMetadata for /bucket/5GB
                                   → Lookup part_ranges[2] → (8388608, 16777215)
                                   → Check if range 8388608-16777215 is cached
                                   → Range found in cache
                                   → Read and decompress range data
                                   → Construct response:
                                     - Status: 206 Partial Content
                                     - Content-Range: bytes 8388608-16777215/5368709120
                                     - Content-Length: 8388608
                                     - x-amz-mp-parts-count: 640
                                     - ETag: (from ObjectMetadata)
                                   → Stream cached data to client
```

Subsequent part requests are served entirely from cache using stored byte ranges.

## S3 Response Header Handling

### Overview

The proxy stores and returns S3 response headers to ensure cached responses are indistinguishable from direct S3 responses. This is critical for AWS SDK compatibility, as clients expect specific headers for features like checksums, encryption status, and versioning.

### Headers Stored in Cache

All S3 response headers are stored in the cache metadata's `response_headers` field, except for connection-specific headers that should not be cached:

**Excluded from cache** (request/connection-specific):
- `x-amz-request-id` - Unique per request
- `x-amz-id-2` - Unique per request  
- `Date` - Response timestamp
- `Server` - Server identifier
- `Connection` - Connection management
- `Transfer-Encoding` - Transport encoding

**All other headers are stored**, including:

| Category | Headers |
|----------|---------|
| Core | `ETag`, `Last-Modified`, `Content-Length`, `Content-Type`, `Content-Encoding`, `Content-Language`, `Content-Disposition`, `Cache-Control` |
| Checksums | `x-amz-checksum-crc32`, `x-amz-checksum-crc32c`, `x-amz-checksum-sha1`, `x-amz-checksum-sha256`, `x-amz-checksum-crc64nvme`, `x-amz-checksum-type` |
| Encryption | `x-amz-server-side-encryption`, `x-amz-server-side-encryption-aws-kms-key-id`, `x-amz-server-side-encryption-bucket-key-enabled` |
| Versioning | `x-amz-version-id`, `x-amz-delete-marker` |
| Object Lock | `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, `x-amz-object-lock-legal-hold` |
| Storage | `x-amz-storage-class`, `x-amz-restore`, `x-amz-expiration` |
| Multipart | `x-amz-mp-parts-count` |
| Replication | `x-amz-replication-status` |
| Other | `x-amz-website-redirect-location`, `x-amz-tagging-count`, `x-amz-missing-meta`, `x-amz-meta-*` (custom metadata) |

### Header Restoration on Cache Hits

When serving responses from cache, the proxy restores all stored headers with these exceptions:

**Always recalculated**:
- `Content-Length` - Calculated from actual response size
- `Content-Range` - Calculated for range requests
- `Accept-Ranges` - Always set to "bytes"

**Filtered for partial ranges**:
- Checksum headers (`x-amz-checksum-*`) are excluded from partial range responses since checksums apply to the complete object, not byte ranges. They are included when serving the full object (range 0 to content_length-1).

### Implementation Details

Headers are extracted in `s3_client.rs`:
```rust
// Store all response headers for complete response reconstruction
for (key, value) in headers {
    let key_lower = key.to_lowercase();
    if !matches!(key_lower.as_str(),
        "connection" | "transfer-encoding" | "date" | "server" 
        | "x-amz-request-id" | "x-amz-id-2"
    ) {
        response_headers.insert(key.clone(), value.clone());
    }
}
```

This design ensures any new headers S3 adds will automatically be preserved without code changes.

### Verifying Header Consistency

To verify cached responses match direct S3 responses:

```bash
# First request (cache miss - fetches from S3)
aws s3api get-object --bucket my-bucket --key test.txt \
  --endpoint-url http://s3.eu-west-1.amazonaws.com /tmp/test1.txt

# Second request (cache hit - served from cache)  
aws s3api get-object --bucket my-bucket --key test.txt \
  --endpoint-url http://s3.eu-west-1.amazonaws.com /tmp/test2.txt

# Compare - headers should be identical except Date and x-amz-request-id
```

## Conditional Headers Handling

### Always-Forward Approach

The proxy implements an always-forward approach for client conditional headers to ensure perfect HTTP compliance and eliminate edge cases. This approach was implemented to fix incorrect behavior where the proxy would make conditional decisions based on potentially stale cached metadata.

### Supported Conditional Headers

The proxy detects and forwards all standard HTTP conditional headers:

- **If-Match**: Forwards to S3, lets S3 validate ETag matches
- **If-None-Match**: Forwards to S3, lets S3 validate ETag mismatches  
- **If-Modified-Since**: Forwards to S3, lets S3 validate modification time
- **If-Unmodified-Since**: Forwards to S3, lets S3 validate modification time

### Behavior Changes

**Previous Behavior (Incorrect)**:
- Proxy would compare client conditional headers against cached metadata
- Proxy would return 412 Precondition Failed immediately for If-Match mismatches
- Proxy would invalidate cache based on client header values
- Could lead to incorrect responses when cached metadata was stale

**New Behavior (Correct)**:
- ALL conditional requests are forwarded to S3 with original client headers
- S3 makes the authoritative decision about condition evaluation
- Cache is managed based solely on S3's response, not client headers
- Ensures perfect HTTP semantics and eliminates edge cases

### Cache Management Based on S3 Response

| S3 Response | Action | Cache Invalidation |
|-------------|--------|-------------------|
| 200 OK | Return data to client, cache new data | Yes - old data invalidated |
| 304 Not Modified | Return 304 to client, refresh TTL | No - cache remains valid |
| 412 Precondition Failed | Return 412 to client | No - cache unchanged |

### Benefits

1. **Perfect HTTP Compliance**: S3 handles all conditional logic according to HTTP standards
2. **Eliminates Edge Cases**: No risk of proxy making incorrect decisions
3. **Transparent Operation**: Clients receive the same responses as direct S3 access
4. **Cache Consistency**: Cache is only invalidated when S3 confirms data is stale
5. **Simplified Logic**: Proxy acts as transparent forwarder for conditional requests

### Performance Impact

- **Minimal Overhead**: Conditional header detection adds <1ms per request
- **Optimized for Common Cases**: Non-conditional requests use existing fast paths
- **S3 Request Efficiency**: Only forwards when client provides conditional headers
- **Cache Efficiency**: Preserves cache when S3 returns 304 or 412 responses

### Logging

Conditional request handling is logged at INFO level for observability:

```
INFO Conditional request forwarded to S3: method=GET path=/bucket/object.txt 
     headers="If-Match: \"abc123\"" cache_key=/bucket/object.txt

INFO S3 conditional response: method=GET path=/bucket/object.txt status=304 
     cache_action="TTL refreshed" cache_key=/bucket/object.txt

INFO S3 conditional response: method=GET path=/bucket/object.txt status=200 
     cache_action="invalidated and updated" cache_key=/bucket/object.txt

INFO S3 conditional response: method=GET path=/bucket/object.txt status=412 
     cache_action="no change" cache_key=/bucket/object.txt
```

**Log Fields:**
- **method**: HTTP method (GET, HEAD)
- **path**: Request path
- **headers**: Conditional headers detected (sanitized for logging)
- **status**: S3 response status code
- **cache_action**: Action taken on cache based on S3 response
- **cache_key**: Cache key for the object

## Cache Types

### Full Object Cache

Caches complete objects from GET requests.

**Cache Key**: `/{bucket}/{object_key}`

### Range Cache

Caches byte ranges independently across both RAM and disk tiers.

**Disk Cache Key**: `/{bucket}/{object_key}:range:{start}-{end}`
**RAM Cache Key**: `{cache_key}:range:{start}:{end}`

Benefits:
- Large files: cache only accessed ranges
- Partial downloads: resume without re-downloading
- Efficient for video streaming, large datasets

#### RAM Cache Integration for Ranges

Both the streaming path (ranges >= 1 MiB) and the buffered path (ranges < 1 MiB) use RAM cache:

1. **RAM cache lookup**: Before deciding between streaming and buffered paths, the proxy checks RAM cache using the range-specific key
2. **RAM hit**: Serves data directly from memory as a buffered 206 response, avoiding all disk I/O
3. **RAM miss → disk hit**: After serving from disk, the proxy promotes the range data to RAM cache so subsequent requests are served from memory
4. **Streaming path promotion**: During disk streaming, chunks are collected into a buffer and promoted to RAM cache after the stream completes
5. **Size guard**: Ranges exceeding `max_ram_cache_size` skip RAM cache promotion (no buffer allocated)

**Important**: All ranges for the same object share the same expiration time. When GET_TTL expires for an object, ALL cached ranges for that object expire together, even though they're stored separately.

### Intelligent Range Merging

The proxy implements intelligent range merging to optimize partial cache hits. When a GET request requires bytes that are partially cached (some bytes in cache, some missing), the system serves cached portions and only fetches missing bytes from S3, then merges them into a complete response.

**Range Merge Optimization**: Simple cache hits (where the requested range exactly matches or is fully contained within a single cached range) bypass merge operations entirely, eliminating unnecessary processing overhead.

#### How Range Merging Works

**Scenario: Partial Cache Hit**

```
Cached ranges: 0-8MB, 16-24MB, 32-40MB
Client requests: 0-40MB

Traditional behavior: Fetch entire 0-40MB from S3 (wasteful)
Range merging behavior:
  1. Identify missing ranges: 8-16MB, 24-32MB
  2. Consolidate missing ranges (if gaps are small)
  3. Fetch only 8-16MB and 24-32MB from S3 (16MB total)
  4. Merge cached + fetched ranges in correct order
  5. Return complete 0-40MB response
  6. Cache the newly fetched ranges for future requests

Result: 60% cache efficiency (24MB from cache, 16MB from S3)
```

#### Range Consolidation

When multiple missing ranges exist, the proxy consolidates them to minimize S3 requests:

**Gap Threshold**: 1MiB (default, configurable)

```
Missing ranges: 10-11MB, 11.1-12MB, 20-21MB

Without consolidation: 3 separate S3 requests
With consolidation:
  - 10-11MB and 11.1-12MB have 100KB gap → Merge into 10-12MB (1 request)
  - 20-21MB has large gap → Keep separate (1 request)
  
Result: 2 S3 requests instead of 3
```

**Rationale**: If the gap between ranges is smaller than the threshold, fetching extra bytes is faster than making another S3 request (typical S3 request overhead: 50-100ms).

#### Configuration

```yaml
cache:
  range_merge_gap_threshold: 1048576  # 1MiB (default)
```

**Tuning Recommendations**:

- **Low latency to S3** (< 10ms): Use smaller threshold (128KB)
  - Multiple requests are cheap
  - Minimize unnecessary data transfer
  
- **High latency to S3** (> 50ms): Use larger threshold (512KB-1MB)
  - Request overhead is expensive
  - Fetching extra bytes is cheaper than extra requests
  
- **Cost-optimized**: Use larger threshold (512KB-1MB)
  - Minimize S3 request count (each request has a cost)
  - Bandwidth is typically cheaper than request count

#### Cache Efficiency Metrics

Range merging operations log detailed efficiency metrics:

```
INFO Range merge completed: cache_key=example.bin, requested=0-41943039, 
     segments=5, cache_efficiency=60.00%, bytes_from_cache=25165824, 
     bytes_from_s3=16777216, duration=45.23ms
```

**Metrics Explained**:

- **segments**: Number of range segments merged (cached + fetched)
- **cache_efficiency**: Percentage of bytes served from cache
- **bytes_from_cache**: Total bytes served from cached ranges
- **bytes_from_s3**: Total bytes fetched from S3
- **duration**: Time spent merging ranges (typically < 100ms)

**Good cache efficiency**: 50-90%
- Significant bandwidth savings
- Faster response times than full S3 fetch

**Low cache efficiency**: < 30%
- May indicate poor cache alignment with access patterns
- Consider adjusting range boundaries or cache size

#### Fully Cached Non-Contiguous Ranges

When all requested bytes are cached but in non-contiguous ranges, the proxy serves the response entirely from cache without contacting S3:

```
Cached ranges: 0-8MB, 8-16MB, 16-24MB (non-contiguous storage)
Client requests: 0-24MB

Behavior:
  1. Detect all bytes are cached (missing_ranges is empty)
  2. Load and merge the 3 cached ranges
  3. Return complete response
  4. No S3 request needed

Result: 100% cache efficiency, 0 bytes from S3
```

This is particularly efficient for:
- **Multipart uploads**: Parts are cached as separate ranges, subsequent GET serves entirely from cache
- **Sequential range requests**: Previous range requests populate cache, later full object request merges them
- **Large files with partial access**: Only accessed portions are cached, but can be merged on demand

#### Range Extraction

The proxy correctly extracts bytes from cached ranges that overlap with requested ranges:

**Full Containment**:
```
Cached range: 0-10MB
Requested: 2-5MB

Extraction: Read bytes 2097152-5242880 from cached file
Result: 3MB extracted, no S3 fetch needed
```

**Partial Overlap**:
```
Cached range: 0-8MB
Requested: 6-10MB

Extraction: Read bytes 6291456-8388608 from cached file (2MB)
Missing: 8-10MB (fetch from S3)
Result: Merge 2MB cached + 2MB fetched = 4MB response
```

**Boundary Alignment**:
- Cached ranges are typically aligned to 8MB boundaries (from multipart uploads)
- Requests can cross boundaries (e.g., 1MB-10MB)
- Proxy correctly extracts partial bytes from each cached range
- No unnecessary S3 fetches for boundary-crossing requests

#### Error Handling and Fallback

Range merging includes comprehensive error handling:

**Validation Failures**:
```
Scenario: Merged data size doesn't match requested range size
Action: Log error, fall back to complete S3 fetch
Result: Client receives correct data, cache is updated
```

**Cached File Missing**:
```
Scenario: Metadata indicates range is cached, but file is missing
Action: Mark range as missing, fetch from S3, recache
Result: Transparent recovery, no client impact
```

**Decompression Failures**:
```
Scenario: Cached range file is corrupted
Action: Invalidate corrupted cache entry, fetch from S3
Result: Fresh data from S3, corrupted entry removed
```

**Partial Merge Failures**:
```
Scenario: Some segments merge successfully, others fail
Action: Fall back to fetching complete range from S3
Result: Guaranteed correct response, cache is refreshed
```

All error scenarios fall back to fetching the complete range from S3, ensuring clients always receive correct data even if cache operations fail.

#### Performance Characteristics

**Parallel S3 Fetches**:
- Multiple missing ranges are fetched from S3 in parallel
- Reduces total fetch time compared to sequential requests
- Example: 3 missing ranges fetched in ~100ms instead of ~300ms

**Memory-Efficient Merging**:
- Ranges are loaded and merged incrementally
- Avoids loading entire object into memory at once
- Suitable for large objects (multi-GB files)

**Cache Warming**:
- Fetched ranges are cached immediately after merge
- Future requests benefit from newly cached ranges
- Gradually improves cache coverage over time

**Typical Performance**:
- Range merge operation: 10-100ms (depending on number of segments)
- S3 fetch for missing ranges: 50-200ms (depending on size and latency)
- Total overhead vs full S3 fetch: Often 2-5x faster for partial cache hits

#### Use Cases

**Video Streaming**:
```
Scenario: Client seeks to different positions in a video file
Cached: Ranges from previous seeks (0-10MB, 50-60MB, 100-110MB)
New request: 0-120MB (full video)

Benefit: Serve 30MB from cache, fetch 90MB from S3
Result: 25% cache efficiency, faster startup than full fetch
```

**Parquet File Queries**:
```
Scenario: Analytics query reads specific columns from a 50MB Parquet file
First query: Footer metadata (49.99-50MB, 8KB) + Column A chunks (5-7MB, 15-17MB)
Cached: Footer (49.99-50MB) + Column A data (5-7MB, 15-17MB) = 4.008MB

Second query: Same file, different column (Column B at 25-27MB, 35-37MB)
Benefit: Serve footer from cache (8KB), fetch only Column B chunks (4MB)
Result: Footer always cached, only new column data fetched from S3
```

**Multipart Upload Followed by GET**:
```
Scenario: Client uploads 100MB file via multipart (13 parts), then immediately GETs it
Cached: All 13 parts as separate ranges (0-8MB, 8-16MB, ..., 96-100MB)
GET request: 0-100MB (full file)

Benefit: Serve entire file from cache by merging 13 ranges
Result: 100% cache efficiency, 0 bytes from S3, ~50ms merge time
```

**Multiple Clients Accessing Same File**:
```
Scenario: Build system downloads 100MB artifact, multiple workers need same file
First client: Downloads 0-100MB (cache miss, fetches from S3)
Subsequent clients: Request 0-100MB (cache hit, served from proxy)

Benefit: First client caches file, all other clients served from cache
Result: 1 S3 request instead of N requests, significant cost savings
```

**Part Number Requests**:
```
Scenario: Client downloads specific parts of a large multipart object
First request: GET /bucket/5GB?partNumber=1 (cache miss)
S3 response: Content-Range: bytes 0-8388607/5368709120, x-amz-mp-parts-count: 640
Proxy: Store part as range 0-8388607, update metadata with parts_count=640, part_ranges[1]=(0, 8388607)

Second request: GET /bucket/5GB?partNumber=2 (cache hit)
Proxy: Lookup part_ranges[2] → (8388608, 16777215), serve from cache
Result: Part served from cache without S3 request
```

### Part Number Cache

Caches S3 GetObject requests with `partNumber` query parameters, treating parts as ranges within the existing range storage architecture.

**How It Works:**

1. **Part Request Detection**: GET requests with `partNumber` parameter are identified as GetObjectPart operations
2. **Part Range Extraction**: S3 response headers (`x-amz-mp-parts-count`, `Content-Range`) provide exact byte ranges
3. **Range Storage**: Parts are stored as ranges using the existing range storage mechanism
4. **Direct Lookup**: Future part requests look up stored byte ranges from `part_ranges` map
5. **Cache Lookup**: Parts are served from cache when the stored range is available

**Cache Key Strategy**: Parts use the same cache key as the full object (`/{bucket}/{object_key}`) but are stored as ranges with specific byte offsets.

**Example Flow:**
```
Client: GET /bucket/5GB?partNumber=1
Proxy: Parse partNumber=1, check cache for /bucket/5GB
S3: Returns Content-Range: bytes 0-8388607/5368709120, x-amz-mp-parts-count: 640
Proxy: Store as range 0-8388607, update part_ranges[1]=(0, 8388607)
Client: GET /bucket/5GB?partNumber=2 (later request)
Proxy: Lookup part_ranges[2], serve from cache if available
```

**Benefits:**
- Eliminates repeated S3 requests for the same parts
- Leverages existing range storage and compression
- Supports variable-sized parts (no uniform size assumption)
- Maintains response header consistency

**Limitations:**
- Upload verification requests (with both `partNumber` and `uploadId`) bypass cache
- Invalid part numbers are passed through to S3
- Parts exceeding the object's part count are forwarded to S3

#### Per-Instance Part Request Deduplication

#### Per-Instance Part Request Deduplication

Concurrent part requests for the same object are handled by the InFlightTracker (download coordination). When multiple requests arrive for the same part, only one fetches from S3 while others wait. See [Download Coordination](#download-coordination) for details.

### Write-Through Cache

Caches objects during PUT operations using the range storage format, enabling subsequent GET requests to be served from cache immediately without fetching from S3.

**Use Case**: Upload once, download many times immediately after

#### How Write Caching Works

**Full PUT Operations**:
1. Client sends PutObject request
2. Proxy forwards to S3 and streams body to temp file
3. S3 returns 200 OK with ETag
4. Proxy commits temp file as range 0-N
5. Metadata created with `is_write_cached=true` and TTL
6. S3 response returned to client unchanged

**Storage Format**: PUT-cached objects are stored as range 0-N in the `ranges/` directory, enabling immediate range request support without fetching from S3.

**Header Behavior for Write-Cached Objects**:
- **ETag**: Available immediately from S3 PUT response
- **Last-Modified**: S3 PUT responses don't include Last-Modified headers. The timestamp is populated only after a subsequent HEAD request or cache-miss GET operation. Cache hits for PUT-cached objects won't include Last-Modified headers until this timestamp is learned.
- **Content-Type**: If provided in the PUT request (single-part) or CreateMultipartUpload request (multipart), it is cached and used. If not provided, learned on first HEAD or cache-miss GET. Note: S3's CompleteMultipartUpload response has `content-type: application/xml` which is the XML response type, not the object's content-type - this is filtered out.

#### TTL Refresh on Read

Write-cached objects have a separate TTL (default: 1 day) that is refreshed when accessed:

- Initial PUT: TTL set to `put_ttl` (default: 1 day)
- GET access: TTL refreshed to current time + `put_ttl`
- No access within TTL: Object expires and is removed

This keeps frequently accessed objects in cache while allowing rarely-read uploads to expire.

#### Capacity Management

Write cache is limited to a percentage of total disk cache:

```yaml
cache:
  write_cache_percent: 10.0  # Default: 10% of max_cache_size
```

**Eviction behavior**:
- When write cache is full, oldest write-cached objects are evicted first
- Uses the same eviction algorithm as read cache (LRU or TinyLFU)
- If eviction cannot free enough space, the PUT bypasses caching
- S3 operation always succeeds regardless of caching outcome

#### Disk-Only Storage

Write-cached objects are stored only on disk, not in RAM cache:
- Preserves RAM cache for hot read data
- Write-cached objects can be promoted to RAM cache on subsequent GET access
- Capacity tracking uses compressed size (actual disk usage)

#### Limitations

- Single PUT size limit: 256MB per object (configurable via `write_cache_max_object_size`)
- Space limit: 10% of total cache for write operations (configurable via `write_cache_percent`)
- Objects larger than limit bypass caching automatically

### Multipart Upload Cache

Caches multipart uploads with intelligent capacity management and shared cache coordination.

#### How Multipart Caching Works

**1. Initiation** (`CreateMultipartUpload`):
- Forward to S3, get uploadId from response XML
- Create tracking metadata in `mpus_in_progress/{uploadId}/upload.meta`
- Record start time and cache key
- Return S3 response unchanged

**2. Part Storage** (`UploadPart`):
- Stream body to S3 and temp file simultaneously
- S3 returns ETag for the part
- Apply content-aware compression (see below)
- Store part as range file: `{key}_part{N}_0-{size-1}.bin`
- Update tracking metadata with part info and compression algorithm (acquire lock first)
- Return S3 response unchanged

**Content-Aware Compression for Parts**:
- Each part is compressed using the same content-aware rules as single-part uploads
- Already-compressed file types (`.zip`, `.jpg`, `.mp4`, etc.) are stored uncompressed
- Compressible file types (`.txt`, `.json`, `.log`, etc.) are compressed with LZ4
- The actual compression algorithm used is stored per-part in the tracking metadata
- On completion, each part's compression algorithm is preserved in the final range metadata

**3. Completion** (`CompleteMultipartUpload`):
- Forward to S3, get final ETag from response XML
- Acquire lock on tracking metadata
- Read all parts, sort by part number
- Calculate final byte offsets for each part
- Rename part files with final offsets
- Create object metadata with `is_write_cached=true`, ETag, and Content-Type (if provided in CreateMultipartUpload)
- Note: Last-Modified is NOT available from CompleteMultipartUpload response; learned on first HEAD or cache-miss GET
- Delete tracking directory
- Return S3 response unchanged

**4. Abort** (`AbortMultipartUpload`):
- Forward to S3
- Delete all cached parts for uploadId
- Delete tracking directory
- Return S3 response unchanged

#### Byte Offset Calculation

Parts are assembled in part number order (not upload order):

```
Part 1: 5MB → bytes 0-5242879
Part 2: 5MB → bytes 5242880-10485759
Part 3: 3MB → bytes 10485760-13631487

Final ranges:
  {key}_0-5242879.bin
  {key}_5242880-10485759.bin
  {key}_10485760-13631487.bin
```

#### Incomplete Upload Cleanup

Multipart uploads that are never completed are automatically cleaned up:

```yaml
cache:
  incomplete_upload_ttl: "1d"  # Default: 1 day
```

**Cleanup behavior**:
- Runs at startup and periodically during operation
- Uses file modification time (not creation time) to detect recent activity
- Uploads with recent UploadPart activity are not evicted
- Uses distributed eviction lock for shared cache coordination
- Acquires per-upload lock before deletion

**Why file mtime**: The tracking file is updated after each UploadPart, so mtime reflects the most recent activity. An upload with recent activity should not be evicted even if it started long ago.

#### Capacity-Aware Bypass

If cumulative parts exceed write cache capacity:
- Upload is marked as "Bypassed"
- Already-cached parts are invalidated to free space
- Subsequent parts are not cached
- Upload continues to S3 normally (caching is transparent)

#### Shared Cache Considerations

When multiple proxy instances share a cache volume:

**Part data storage (no lock needed)**:
- Each part has a unique filename
- Different part numbers → different files → no conflict
- Write uses temp file + atomic rename → no partial reads

**Tracking metadata updates (lock needed)**:
- Lock acquired when updating `upload.meta`
- Lock scope: only the specific uploadId, not global
- Concurrent uploads to different uploadIds → no contention

**CompleteMultipartUpload coordination**:
- Acquire lock on tracking metadata
- Read all parts from tracking file (regardless of which instance cached them)
- Calculate offsets, rename files, create object metadata
- Delete tracking directory
- Release lock

**Incomplete upload scanner coordination**:
- Uses distributed eviction lock (same as read cache eviction)
- Only one instance scans at a time
- Prevents duplicate cleanup work

#### Edge Cases

**Instance crashes during CompleteMultipartUpload**:
- Lock times out after configured timeout
- Next instance can retry completion
- Parts remain intact until completion or TTL expiration

**GET request during incomplete multipart upload**:
- Proxy checks existing read cache first
- If cache miss, forward to S3 (S3 is source of truth)
- In-progress upload parts are NOT served
- Parts only become accessible after CompleteMultipartUpload

**Object exists, then new multipart upload started**:
- Existing cached object remains valid until new upload completes
- GET requests continue to serve existing cached version
- On CompleteMultipartUpload: new object metadata replaces old

**CompleteMultipartUpload with missing parts (Multi-Instance)**:
- Proxy validates all parts exist locally before finalization
- If any parts missing (uploaded to other instances): skips caching entirely
- Cleans up partial cache data to prevent corruption
- Returns S3 success response unchanged to client
- Prevents incomplete cache entries that could serve corrupted data

**Benefits**:
- Large multipart uploads can be cached if they fit within capacity
- Completed multipart uploads support range requests immediately
- Capacity limits are respected incrementally as parts arrive
- Abandoned uploads don't consume cache space indefinitely
- Any instance can handle any part of the upload
- Multi-instance deployments maintain data integrity

**Cache Key**: `/{bucket}/{object_key}`

**Important**: Multipart uploads use the same range storage as PUT and GET operations. Once completed, they behave identically to PUT-cached objects.

### Write Cache Error Handling

The write cache implements robust error handling to ensure that S3 operation failures (including authentication/authorization errors like 403 Forbidden) do not result in corrupted or invalid cache entries.

#### Core Principle: Only Cache on S3 Success

**Requirement 9.1**: The write cache follows the fundamental principle of "only cache on S3 success." This ensures that:
- Authentication failures (403 Forbidden) don't create cache entries
- Authorization failures don't cache data the client shouldn't access
- Network errors don't result in partial cache data
- S3 service errors don't corrupt the cache

#### Single PUT Request Error Handling

When a PUT request receives an error response from S3 (including 403 Forbidden):

**Process Flow:**
1. **Request Processing**: Proxy reads the request body and spawns a background cache task
2. **S3 Forward**: Request is forwarded to S3 immediately (doesn't wait for cache)
3. **Error Detection**: Background cache task receives the error status via oneshot channel
4. **Cache Prevention**: Cache task checks `if status.is_success()` and skips caching entirely

**Code Behavior:**
```rust
if status.is_success() {
    // S3 success - store as single range with write cache metadata
    // ... caching logic here ...
} else {
    // S3 returned error - don't cache (Requirement 9.1)
    debug!(
        "S3 error response, not caching PUT: cache_key={}, status={}",
        cache_key, status
    );
}
```

**Result for 403 Errors:**
- No cache entry is created
- No disk space is consumed
- No partial or corrupted data is stored
- 403 error is returned to client unchanged
- No cleanup is needed (no cache data was created)

#### Multipart Upload Error Handling

**UploadPart 403 Errors:**
- Individual part uploads that receive 403 are not cached
- No partial cache data is stored for that failed part
- Other successful parts remain cached (if any)
- Upload can continue with remaining parts

**CompleteMultipartUpload 403 Errors:**
When CompleteMultipartUpload receives a 403 error:

```rust
if status.is_success() {
    // ... finalize cache metadata linking all parts ...
} else {
    // Mark upload as incomplete but don't delete parts (Requirement 5.5, 8.2, 9.3)
    error!(
        "CompleteMultipartUpload S3 error: bucket={}, key={}, status={}",
        bucket, key, status.as_u16()
    );
}
```

**Result for 403 Errors:**
- Previously cached parts remain in cache (not deleted)
- No final metadata linking parts together is created
- Upload remains "incomplete" in cache terms
- Parts will be cleaned up by incomplete upload TTL (default: 1 day)
- 403 error is returned to client unchanged

**AbortMultipartUpload:**
This operation always cleans up cached parts regardless of S3 response status, so a 403 here would still result in proper cache cleanup.

#### Error Types and Handling

**Authentication/Authorization Errors (403, 401):**
- No cache entries created
- Existing cache entries remain unchanged
- Client receives exact S3 error response

**Network/Connection Errors:**
- Background cache task receives error via channel
- No cache entries created
- S3 error propagated to client

**S3 Service Errors (5xx):**
- Treated same as authentication errors
- No caching occurs on any S3 error status
- Ensures cache consistency

#### Resource Management

**No Orphaned Cache Data:**
- 403 errors prevent new cache entries but don't leave partial/corrupted data
- Background tasks properly handle channel closure
- Temporary files are cleaned up automatically

**Memory Management:**
- Request body is read once and shared between S3 forward and cache task
- Failed operations don't consume additional memory
- Background tasks exit cleanly on S3 errors

**Disk Space Conservation:**
- Failed operations don't consume cache space
- No temporary files left behind
- Incomplete multipart uploads cleaned up by TTL

#### Transparency to Clients

**Error Passthrough:**
- All S3 errors (including 403) are passed through to client unchanged
- No modification of error responses
- Client sees exact same error as direct S3 access

**No Cache Pollution:**
- Failed operations don't create invalid cache entries
- Subsequent requests don't serve stale/invalid data
- Cache remains consistent with S3 state

#### Monitoring and Logging

**Error Logging:**
```
DEBUG S3 error response, not caching PUT: cache_key=/bucket/object.txt, status=403
ERROR CompleteMultipartUpload S3 error: bucket=my-bucket, key=large-file.bin, status=403
```

**Metrics:**
- Failed PUT operations don't increment cache metrics
- Error rates tracked separately from cache hit/miss rates
- No false positive cache statistics

#### Benefits

1. **Data Integrity**: Only valid, authorized data is cached
2. **Security**: Authentication failures don't bypass security controls
3. **Consistency**: Cache state always reflects successful S3 operations
4. **Resource Efficiency**: Failed operations don't waste cache space
5. **Transparency**: Clients see identical behavior to direct S3 access
6. **Reliability**: No partial or corrupted cache entries

This error handling ensures that the write cache enhances performance without compromising security, consistency, or reliability.

## Bucket-Level Cache Settings

Per-bucket and per-prefix cache configuration via JSON files at `cache_dir/metadata/{bucket}/_settings.json`. Changes take effect without proxy restart.

See [CONFIGURATION.md](CONFIGURATION.md#bucket-level-cache-settings) for file format, schema reference, and global config fields.

### Per-Bucket Cache Control

#### read_cache_enabled

Controls whether GET responses are cached on disk for a bucket or prefix.

When `false`:
- GET response data is not written to disk cache
- Range data is not promoted to RAM cache
- Metadata is not stored in the RAM metadata cache
- Requests are forwarded to S3 and streamed directly to the client
- Write caching for PUT operations is independent — `write_cache_enabled` controls that separately

When `true` (default): Normal caching behavior.

Global default: `cache.read_cache_enabled` in YAML config (default: `true`). Set globally to `false` for an allowlist pattern where only buckets with `_settings.json` containing `"read_cache_enabled": true` are cached.

#### write_cache_enabled

Controls whether PUT operations are cached for a bucket or prefix.

When `false`: PUT requests are forwarded to S3 but not cached locally.
When `true` (default from global config): Normal write-through caching.

Independent of `read_cache_enabled` — disabling read cache does not affect write caching.

#### ram_cache_eligible

Controls whether range data from a bucket or prefix can be stored in RAM cache.

When `false`: Data is served from disk cache only (no RAM cache promotion).
When `true` (default): Normal RAM cache behavior.

Automatically forced to `false` when:
- `get_ttl` is `"0s"` (RAM cache bypasses revalidation, which would serve stale data)
- `read_cache_enabled` is `false`

### Zero TTL Revalidation

Zero-TTL requests (`get_ttl: "0s"` or `head_ttl: "0s"`) go through the normal cache flow — there is no separate bypass path. The proxy stores data with `expires_at = now`, making it immediately expired. Every subsequent request finds expired data and triggers conditional revalidation with S3.

**GET flow with `get_ttl: "0s"`:**

```
Client GET → Cache lookup (normal path)
           → Cold cache: fetch from S3, store with expires_at = now
           → Warm cache (data exists but expired):
               → Send If-Modified-Since to S3
               → 304 Not Modified: serve cached data, refresh expires_at to now (expired again)
               → 200 OK: replace cached data, serve new data
               → Error: forward error to client
```

Both range requests and full-object GETs follow this flow. The proxy calls `check_object_expiration()` with the cache key to determine freshness — expiration is checked at the object level, so all cached ranges of the same object share the same freshness state. If the object-level `expires_at` is in the past, the proxy proceeds with conditional revalidation. After a 304 Not Modified, `refresh_object_ttl()` updates the object-level `expires_at` for all ranges at once.

If the proxy cannot read or deserialize the metadata file during an expiration check, it treats the cached data as expired and forwards the request to S3. This fail-safe behavior prevents serving stale or unauthorized data when freshness cannot be verified.

**HEAD flow with `head_ttl: "0s"`:**

HEAD responses are cached with `head_expires_at = now`. On every request, `is_head_expired()` returns true, and the proxy forwards the HEAD to S3 and updates the cache.

**RAM cache exclusion:** Range data is excluded from RAM cache when `get_ttl` is zero. RAM cache serves data without revalidation, so storing zero-TTL data there would serve stale content. The metadata cache continues to store entries (it has its own refresh interval independent of data TTL).

Zero TTL revalidation applies to both GET-cached and write-cached data. The `is_write_cached` flag does not override zero-TTL revalidation semantics.

### Settings Apply at Cache-Write Time

Bucket and prefix cache settings (TTL values, `read_cache_enabled`, `ram_cache_eligible`) take effect when data is written to cache, not retroactively. The proxy stamps each cache entry with the resolved settings at write time.

Consequences:
- Changing `get_ttl` from `"3600s"` to `"0s"` does not expire already-cached objects. They retain their original `expires_at` until they expire naturally or are evicted.
- Changing `get_ttl` from `"0s"` to `"3600s"` does not extend the TTL of existing zero-TTL entries. New requests that trigger a cache write store data with the new TTL.
- Changing `read_cache_enabled` from `true` to `false` does not delete existing cached data. It stops new data from being cached, and existing data remains on disk until TTL expiry or eviction.

### Read Cache Disabled Behavior

When `read_cache_enabled` is `false` (at bucket or prefix level), the proxy acts as a pure pass-through for GET requests:

```
Client GET → Proxy → S3 → Stream response directly to client (no caching)
```

- No disk cache reads or writes
- No RAM cache promotion or metadata storage
- PUT write caching is unaffected (controlled by `write_cache_enabled`)
- Existing cached data for the bucket/prefix remains on disk but is not read; it expires via normal TTL and eviction

## Cache Bypass for Non-Cacheable Operations

### Overview

The proxy intelligently bypasses cache for S3 operations that return dynamic or frequently-changing data. Only GetObject and HeadObject operations are cached, as they retrieve immutable object data and metadata. All other S3 operations bypass the cache to ensure clients always receive fresh data.

### Operations That Bypass Cache

**LIST Operations** (always bypass):
- **ListBuckets** (GET or HEAD to root path `/`)
- **ListObjects** (query parameters: `list-type`, `delimiter`)
- **ListObjectVersions** (query parameter: `versions`)
- **ListMultipartUploads** (query parameter: `uploads`)

**Metadata Operations** (always bypass):
- **GetObjectAcl** (query parameter: `acl`)
- **GetObjectAttributes** (query parameter: `attributes`)
- **GetObjectLegalHold** (query parameter: `legal-hold`)
- **GetObjectLockConfiguration** (query parameter: `object-lock`)
- **GetObjectRetention** (query parameter: `retention`)
- **GetObjectTagging** (query parameter: `tagging`)
- **GetObjectTorrent** (query parameter: `torrent`)

**Part Operations** (cached):
- **GetObjectPart** (query parameter: `partNumber`) - Cached as ranges using existing range storage architecture

### Cached Operations

**GetObject** (cached):
- GET requests without non-cacheable query parameters
- GET requests with `Range` header (range requests are cached)

**HeadObject** (cached):
- HEAD requests to object paths (not root path)
- HEAD metadata cached separately with HEAD_TTL

### Detection Logic

The proxy examines each request to determine if it should bypass cache:

```
Request Flow:
1. Parse HTTP method and path
2. Check for root path "/" → ListBuckets/HeadBucket (bypass)
3. Parse query parameters
4. Check for non-cacheable parameters → Bypass cache
5. Otherwise → Use cache (GetObject or HeadObject)
```

**Examples:**

```
GET /bucket/object.txt                    → Cached (GetObject)
GET /bucket/object.txt Range: bytes=0-100 → Cached (range request)
GET /bucket/?list-type=2                  → Bypassed (ListObjects)
GET /bucket/object.txt?acl                → Bypassed (GetObjectAcl)
GET /bucket/object.txt?tagging            → Bypassed (GetObjectTagging)
GET /bucket/object.txt?partNumber=1       → Cached (GetObjectPart)
HEAD /                                    → Bypassed (HeadBucket/ListBuckets)
HEAD /bucket/object.txt                   → Cached (HeadObject)
```

### Logging

Cache bypass operations are logged at INFO level with detailed information:

```
INFO Bypassing cache: operation=ListObjects method=GET path=/my-bucket/ 
     query="list-type=2&prefix=photos/" reason="list operation - always fetch fresh data"

INFO Bypassing cache: operation=GetObjectAcl method=GET path=/my-bucket/photo.jpg 
     query="acl" reason="metadata operation - always fetch fresh data"

INFO Caching part request: operation=GetObjectPart method=GET path=/my-bucket/large-file.bin 
     query="partNumber=5" part_number=5

INFO Bypassing cache: operation=ListBuckets method=HEAD path=/ 
     query="" reason="list operation - always fetch fresh data"
```

**Log Fields:**
- **operation**: The specific S3 operation detected (ListObjects, GetObjectAcl, ListBuckets, etc.)
- **method**: HTTP method (GET, HEAD)
- **path**: Request path
- **query**: Query string parameters
- **reason**: Explanation for why cache was bypassed

### Performance Characteristics

**Bypass Operations:**
- No cache lookup overhead
- Direct forwarding to S3
- Minimal latency added by proxy (<1ms)
- Error responses passed through unchanged

**Cached Operations:**
- Cache lookup: 1-10ms (disk) or <1ms (RAM)
- Cache hit: Serve from cache (no S3 request)
- Cache miss: Forward to S3, cache response

### Configuration

Cache bypass behavior is automatic and cannot be disabled. No configuration is required.

**Rationale:**
- LIST operations return dynamic data that changes frequently
- Metadata operations (ACL, tags, attributes) are mutable and must be fresh
- Caching these operations would provide stale data and violate S3 semantics

## Cache Bypass Headers

### Overview

The proxy supports standard HTTP cache control headers that allow clients to explicitly bypass the cache for GET and HEAD requests. This is useful for debugging, testing, and scenarios where clients need guaranteed fresh data from S3.

### Supported Headers

**Cache-Control Header:**
- `Cache-Control: no-cache` - Bypass cache lookup, but cache the response for future requests
- `Cache-Control: no-store` - Bypass cache lookup and do not cache the response

**Pragma Header:**
- `Pragma: no-cache` - Bypass cache lookup, cache the response (HTTP/1.0 compatibility)

### Header Behavior

| Header | Cache Lookup | Cache Response | Use Case |
|--------|--------------|----------------|----------|
| `Cache-Control: no-cache` | Bypassed | Yes | Get fresh data, benefit future requests |
| `Cache-Control: no-store` | Bypassed | No | Get fresh data, no caching at all |
| `Pragma: no-cache` | Bypassed | Yes | HTTP/1.0 compatibility |

### Precedence Rules

1. **Cache-Control takes precedence over Pragma**: When both headers are present, `Cache-Control` is used
2. **no-store takes precedence over no-cache**: When both directives are in `Cache-Control`, `no-store` wins (more restrictive)
3. **Case-insensitive parsing**: Headers and directives are parsed case-insensitively

### Examples

**Force fresh data, allow caching for others:**
```bash
# Using AWS CLI with custom header
aws s3api get-object --bucket my-bucket --key my-file.txt \
  --custom-headers "Cache-Control=no-cache" ./output.txt
```

**Force fresh data, no caching:**
```bash
# Using AWS CLI with custom header
aws s3api get-object --bucket my-bucket --key my-file.txt \
  --custom-headers "Cache-Control=no-store" ./output.txt
```

**HTTP/1.0 compatibility:**
```bash
# Using AWS CLI with custom header
aws s3api get-object --bucket my-bucket --key my-file.txt \
  --custom-headers "Pragma=no-cache" ./output.txt
```

### Non-Cacheable Operations

Cache bypass headers only affect cache lookup behavior. Operations that are normally non-cacheable (LIST, metadata operations) remain non-cacheable regardless of bypass headers:

| Operation | Normal | With `no-cache` | With `no-store` |
|-----------|--------|-----------------|-----------------|
| GetObject | Cached | Bypass + Cache | Bypass + No Cache |
| HeadObject | Cached | Bypass + Cache | Bypass + No Cache |
| ListObjects | Not Cached | Not Cached | Not Cached |
| GetObjectAcl | Not Cached | Not Cached | Not Cached |

### Header Stripping

Cache control headers are stripped before forwarding requests to S3:
- `Cache-Control` header is removed
- `Pragma` header is removed
- All other headers are preserved

This ensures S3 receives clean requests without proxy-specific cache directives.

### Configuration

Cache bypass header support is enabled by default and can be disabled:

```yaml
cache:
  cache_bypass_headers_enabled: true  # Default: true
```

**When disabled:**
- `Cache-Control` and `Pragma` headers are ignored for bypass decisions
- Requests are processed through normal cache logic
- Headers are still stripped before forwarding to S3

### Logging

Cache bypass operations are logged at INFO level:

```
INFO Cache bypass via header: method=GET path=/bucket/object.txt reason="no-cache directive"
INFO Cache bypass via header: method=HEAD path=/bucket/object.txt reason="no-store directive"
INFO Cache bypass via header: method=GET path=/bucket/object.txt reason="pragma no-cache"
```

**Log Fields:**
- **method**: HTTP method (GET, HEAD)
- **path**: Request path
- **reason**: Bypass reason (no-cache directive, no-store directive, pragma no-cache)

### Metrics

Cache bypass operations are tracked via metrics:

```json
{
  "cache_bypasses": {
    "no-cache directive": 42,
    "no-store directive": 15,
    "pragma no-cache": 8
  }
}
```

### Use Cases

**Debugging Cache Issues:**
```bash
# Force fresh data to verify S3 content
aws s3api get-object --bucket my-bucket --key config.json \
  --custom-headers "Cache-Control=no-cache" ./config.json
```

**Testing Cache Behavior:**
```bash
# First request: bypass cache, populate cache
aws s3api get-object --bucket my-bucket --key test.txt \
  --custom-headers "Cache-Control=no-cache" ./test1.txt

# Second request: should hit cache
aws s3api get-object --bucket my-bucket --key test.txt ./test2.txt
```

**Sensitive Data (No Caching):**
```bash
# Ensure response is never cached
aws s3api get-object --bucket my-bucket --key credentials.json \
  --custom-headers "Cache-Control=no-store" ./credentials.json
```

**Refresh After Upload:**
```bash
# Upload new version
aws s3 cp ./updated-file.txt s3://my-bucket/file.txt

# Immediately get fresh version (bypass any stale cache)
aws s3api get-object --bucket my-bucket --key file.txt \
  --custom-headers "Cache-Control=no-cache" ./downloaded.txt
```

### Monitoring

Monitor cache bypass operations via logs:

```bash
# Count bypass operations by type
grep "Bypassing cache" /logs/app/*/app.log | \
  awk -F'operation_type=' '{print $2}' | \
  awk '{print $1}' | sort | uniq -c

# Example output:
#  142 ListObjects
#   38 GetObjectAcl
#   12 GetObjectTagging
#    5 ListBuckets
```

**Metrics:**
- Cache bypass operations do not affect cache hit/miss metrics
- They are logged separately for monitoring
- High bypass rates are normal for workloads with frequent LIST operations

## Versioned Request Handling

### Overview

S3 supports object versioning, allowing multiple versions of an object to exist in a bucket. The proxy bypasses cache entirely for any request containing a `?versionId=` query parameter.

### Behavior

When a GET or HEAD request includes `?versionId=`:

1. **Cache bypass**: No cache read, no cache write
2. **Forward to S3**: Request forwarded transparently to S3
3. **Metric recorded**: Cache bypass with reason `versioned_request`

Requests without `versionId` are unaffected — normal caching applies.

```
Versioned Request Flow:

GET /bucket/object?versionId=abc123
        │
        ▼
   Cache BYPASS (versioned request)
        │
        ▼
   Forward to S3
        │
        ▼
   Return S3 response (not cached)
```

### Rationale

- Versioned requests are typically infrequent (auditing, rollback, compliance)
- Caching specific versions adds complexity with minimal benefit
- Full cache bypass guarantees correct data from S3 for every versioned request
- Unversioned requests (the common case) benefit from caching as before

### Cache Key Design

The cache key does NOT include `versionId`:
- Cache key: `/bucket/object` (path only)
- The cached object represents the "current" version at time of caching
- Versioned requests bypass cache entirely, so no version comparison is needed

### Logging

Versioned request bypass is logged at DEBUG level:

```
DEBUG Versioned request detected, bypassing cache: cache_key=/bucket/object
```

### Metrics

Versioned request bypasses are tracked:

```json
{
  "cache_bypasses": {
    "versioned_request": 23
  }
}
```

### Use Cases

**Accessing historical versions:**
```bash
aws s3api get-object --bucket my-bucket --key document.pdf \
  --version-id "abc123" ./document-v1.pdf --no-cli-pager  # Bypasses cache, fetches from S3

```

**Current version access (normal caching):**
```bash
aws s3api get-object --bucket my-bucket --key document.pdf ./document.pdf --no-cli-pager  # Normal cache behavior
```

**Rollback scenario:**
```bash
aws s3api list-object-versions --bucket my-bucket --prefix document.pdf --no-cli-pager  # List versions

aws s3api copy-object --bucket my-bucket --key document.pdf \
  --copy-source "my-bucket/document.pdf?versionId=abc123" --no-cli-pager  # Copy old version to make it current

# GET without versionId returns the restored version, cache updated on next request
```

### Configuration

Versioned request bypass is automatic and cannot be disabled.

### Best Practices

1. **Use versioning for compliance/audit**: Versioned requests always get authoritative data from S3
2. **Access current versions when possible**: Requests without `versionId` benefit from caching
3. **Monitor bypass metrics**: Track `versioned_request` bypass count for workload visibility

## Configuration Examples

### High-Change Environment

Objects change frequently, need fresh data quickly:

```yaml
cache:
  get_ttl: "3600s"      # 1 hour data TTL
  head_ttl: "300s"      # 5 minutes metadata TTL
  max_cache_size: 10737418240  # 10GB
```

### Static Content / CDN Use Case

Objects rarely change, maximize cache hits:

```yaml
cache:
  get_ttl: "604800s"    # 7 days data TTL
  head_ttl: "86400s"    # 24 hours metadata TTL
  max_cache_size: 107374182400  # 100GB
```

### Cost-Optimized

Minimize S3 requests, balance freshness:

```yaml
cache:
  get_ttl: "86400s"     # 24 hours
  head_ttl: "3600s"     # 1 hour
  ram_cache_enabled: true
  max_ram_cache_size: 1073741824  # 1GB RAM cache
```

### Write-Heavy Workload

Optimize for upload-then-download patterns with multipart support:

```yaml
cache:
  write_cache_enabled: true
  put_ttl: "7200s"          # 2 hours
  write_cache_percent: 20.0  # 20% of cache for writes
  write_cache_max_object_size: 536870912  # 512MB max per PUT
  
  # Multipart uploads use same capacity limits
  # Incomplete uploads cleaned up after 1 hour (hardcoded)
```

**Multipart Behavior:**
- Multipart uploads share the same `write_cache_percent` capacity
- If cumulative parts exceed capacity, upload is bypassed automatically
- Incomplete uploads are cleaned up after 1 hour
- Completed multipart uploads support range requests immediately

### Cache Efficiency Optimization

Optimize for maximum cache efficiency with range merging and part caching:

```yaml
cache:
  max_cache_size: 107374182400  # 100GB
  get_ttl: "604800s"            # 7 days
  
  # Range merging optimization
  range_merge_gap_threshold: 524288  # 512KB (aggressive consolidation)
  
  # Enable write caching for multipart uploads
  write_cache_enabled: true
  write_cache_percent: 15.0     # 15% for uploads
  
  # RAM cache for hot ranges and parts
  ram_cache_enabled: true
  max_ram_cache_size: 2147483648  # 2GB
```

**Optimization Strategy:**
- **Larger gap threshold** (512KB): Minimize S3 request count, maximize cache reuse
- **Write caching enabled**: Multipart uploads immediately available for range requests
- **RAM cache**: Hot ranges and parts served from memory (sub-millisecond latency)
- **Long GET TTL**: Maximize cache retention for frequently accessed ranges and parts
- **Part caching**: GetObjectPart requests cached automatically as ranges

**Expected Results:**
- 70-90% cache efficiency for partial cache hits
- 100% cache efficiency for multipart upload followed by GET
- 100% cache efficiency for repeated GetObjectPart requests
- Reduced S3 bandwidth costs by 50-80%
- Faster response times (2-5x) compared to full S3 fetches
- Eliminated S3 requests for cached parts

**Monitoring:**
```bash
# Check cache efficiency in logs
grep "Range merge completed" /logs/app/*/app.log | grep "cache_efficiency"

# Expected output:
# cache_efficiency=75.50%, bytes_from_cache=31457280, bytes_from_s3=10485760
# cache_efficiency=100.00%, bytes_from_cache=104857600, bytes_from_s3=0

# Check part caching operations
grep "Part cache" /logs/app/*/app.log

# Expected output:
# Part cache HIT - serving from cache: cache_key=/bucket/large-file.bin part_number=1
# Part cache MISS - fetching from S3: cache_key=/bucket/large-file.bin part_number=2
# Part cached - stored as range: cache_key=/bucket/large-file.bin part_number=2
```

## Cache Invalidation

### Automatic Invalidation

The proxy automatically invalidates cache when:

1. **Metadata Mismatch**: ETag or Last-Modified changes
2. **PUT to Same Key**: New upload invalidates old cache (conflict handling)
3. **CreateMultipartUpload to Same Key**: New multipart upload invalidates old cache (conflict handling)
4. **DELETE Request**: Removes cache entry
5. **S3 Returns New Data**: When S3 returns 200 OK for conditional requests, indicating cached data is stale
6. **Incomplete Upload Timeout**: Uploads in-progress for >1 hour are removed

### Conflict Handling

When a new upload starts for an existing cache key:

**PUT Conflict:**
```
Existing cache: object.bin (Complete, 10MB)
Client PUT → Invalidate existing cache (delete metadata + ranges)
          → Store new object as range 0-N
          → Mark upload_state = Complete
```

**Multipart Conflict:**
```
Existing cache: object.bin (Complete, 10MB)
Client CreateMultipartUpload → Invalidate existing cache
                             → Create new metadata with upload_state = InProgress
Client UploadPart → Store parts incrementally
```

This ensures cache consistency with S3 - new uploads always replace old cached data.

### Manual Invalidation

Not supported - the proxy has no management API. To clear cache:

```bash
# Stop proxy
sudo systemctl stop s3-proxy

# Clear cache directory
rm -rf /cache/*

# Restart proxy
sudo systemctl start s3-proxy
```

## Compression

All cached data is compressed with LZ4 by default for both disk and RAM cache tiers.

**Benefits**:
- 2-3x space savings for text, JSON, XML
- Fast compression/decompression (minimal CPU)
- Automatic for all cache types
- Optimized disk-to-RAM cache promotion (no decompress/recompress cycles)

**Content-Aware**:
- Skips already-compressed formats (images, video, archives)
- Configurable threshold (default: 1KB minimum)

```yaml
compression:
  enabled: true
  threshold: 1024           # 1KB minimum
  preferred_algorithm: "lz4"
  content_aware: true       # Skip .jpg, .mp4, .zip, etc.
```

### RAM Cache Compression Optimization

The proxy implements an intelligent compression optimization that eliminates unnecessary decompress/recompress cycles when promoting data from disk cache to RAM cache.

#### How It Works

**Before Optimization (Inefficient)**:
```
Disk Cache: Data (LZ4 compressed) 
    ↓ decompress LZ4
RAM Cache: Data (uncompressed) → size check (FAILS for large files)
    ↓ recompress LZ4  
RAM Cache: Data (LZ4 compressed)
```

**After Optimization (Efficient)**:
```
Disk Cache: Data (LZ4 compressed)
    ↓ pass compressed data directly
RAM Cache: Data (LZ4 compressed) → size check (PASSES on compressed size)
```

#### Key Benefits

1. **Fixes Large File Caching**: Large compressible files (e.g., 500MB of text) can now be stored in RAM cache
   - Size check happens **after** compression, not before
   - Files that compress well (high compression ratio) fit within RAM cache limits
   - Eliminates "too large for RAM cache" warnings for compressible data

2. **Eliminates CPU Waste**: No more decompress → recompress cycles
   - When disk and RAM use the same compression algorithm (LZ4), data passes through unchanged
   - Saves CPU cycles and reduces memory pressure during cache promotion
   - Faster disk-to-RAM promotion (no compression work needed)

3. **Algorithm Compatibility**: Handles mixed compression scenarios correctly
   - **LZ4 → LZ4**: Pass compressed data directly (optimized path)
   - **None → LZ4**: Compress for first time (normal path)  
   - **LZ4 → None**: Decompress and store uncompressed (normal path)
   - **Mixed algorithms**: Handled correctly with fallback to decompress/recompress

#### Performance Impact

**Disk-to-RAM Promotion**:
- **Before**: Decompress (CPU) + Recompress (CPU) + Size check on uncompressed data
- **After**: Size check on compressed data (no CPU work)
- **Result**: 2-5x faster promotion for large compressible files

**Memory Usage**:
- **Before**: Temporary uncompressed buffer during promotion (high memory pressure)
- **After**: No temporary buffers (compressed data passed directly)
- **Result**: Lower memory usage during cache operations

**Cache Capacity**:
- **Before**: Large compressible files rejected based on uncompressed size
- **After**: Large compressible files accepted based on compressed size
- **Result**: Better cache utilization for compressible workloads

#### Example Scenario

```
File: 500MB of highly compressible text data (logs, JSON, etc.)
LZ4 Compression: 500MB → 1MB (99.8% compression ratio)
RAM Cache Limit: 256MB

Before Optimization:
- Size check: 500MB > 256MB → REJECTED
- Warning: "Entry too large for RAM cache (524288000 bytes > 268435456 bytes max)"
- File never cached in RAM

After Optimization:
- Disk cache stores: 1MB compressed data
- RAM cache receives: 1MB compressed data (no decompression)
- Size check: 1MB < 256MB → ACCEPTED
- Result: File successfully cached in RAM for fast access
```

#### Configuration

No configuration changes are required. The optimization is automatic and maintains backward compatibility:

- **Same algorithm**: Compressed data passed directly (optimized)
- **Different algorithms**: Falls back to decompress/recompress (compatible)
- **Algorithm detection**: Based on compression metadata from disk cache

#### Monitoring

The optimization can be observed in debug logs:

```
DEBUG Using pre-compressed data for RAM cache entry: /bucket/large-file.txt 
      (algorithm: Lz4)

DEBUG Stored entry in RAM cache: /bucket/large-file.txt 
      (1048576 bytes, compressed: true, algorithm: Lz4)
```

**Log Indicators**:
- **"Using pre-compressed data"**: Optimization applied (no decompress/recompress)
- **"Compressed data for RAM cache entry"**: Normal compression path (first-time compression)
- **Size in logs**: Shows final compressed size used for RAM cache storage

## Multi-Instance Caching

### Shared Cache Volume

Multiple proxy instances can share a cache volume (NFS):

```yaml
cache:
  cache_dir: "/mnt/shared-cache"
```

**Coordination**:
- File locking prevents write conflicts
- Concurrent reads are safe (no locks needed)
- Write locks are held briefly during cache updates

**Best Practices**:
- Use high-performance shared NFS storage
- Enable RAM cache on each instance for hot objects
- Monitor lock contention in metrics

## Cache Access Tracking

### Overview

The proxy implements a distributed access tracking system that records **per-range** access counts and timestamps without blocking concurrent reads. This data is used by eviction algorithms (LRU, TinyLFU) to make intelligent decisions about which individual ranges to evict. Access statistics are tracked at the range level, not the object level, enabling fine-grained eviction where hot ranges are retained even if other ranges of the same object are cold.

### Architecture

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

### How It Works

#### 1. Recording Accesses (RAM Buffered)

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

#### 2. Periodic Buffer Flush

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

#### 3. Periodic Consolidation

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

#### 4. Metadata Updates

Consolidated access data is written to range metadata:

```json
{
  "ranges": [
    {
      "start": 0,
      "end": 8388607,
      "access_count": 42,
      "last_accessed": "2024-01-15T10:30:00Z",
      ...
    }
  ],
  "head_access_count": 15,
  "head_last_accessed": "2024-01-15T10:30:00Z"
}
```

**Atomic Updates**: Metadata updates use temp file + rename pattern for atomicity.

**Note**: Metadata is only updated during consolidation, not on every access. This dramatically reduces disk I/O while maintaining accurate access statistics for eviction decisions.

### Integration with Eviction

The eviction system uses per-range access tracking data to make decisions:

- **LRU (Least Recently Used)**: Sorts ranges by `last_accessed` (oldest first)
- **TinyLFU**: Combines frequency (`access_count`) and recency (`last_accessed`) for optimal eviction

Each range is evaluated independently, allowing hot ranges to be retained even if other ranges of the same object are cold. See [Range-Based Disk Cache Eviction](#range-based-disk-cache-eviction) for details.

**Before Eviction**: The system triggers consolidation to ensure recent accesses are reflected in metadata before making eviction decisions.

### Multi-Instance Coordination

The access tracking system is designed for multi-instance deployments:

- **Per-Instance Log Files**: Each instance writes to its own `{instance_id}.log` files
- **No Write Conflicts**: Separate files per instance eliminate write conflicts
- **Per-Bucket Locking**: Each time bucket has its own lock for consolidation
- **Cross-Instance Aggregation**: Consolidation reads log files from all instances
- **Jittered Timing**: Each instance has a unique consolidation offset to spread load

### Journal-Based Metadata Updates (Shared Storage)

When running multiple proxy instances with shared NFS storage, cache-hit metadata updates (TTL refresh, access count increment) use a journal-based system to eliminate race conditions.

#### Problem: Race Conditions on Shared Storage

Without journaling, concurrent metadata updates from multiple instances can corrupt data:

```
Instance A: Read metadata → Modify → Write
Instance B: Read metadata → Modify → Write (overwrites A's changes)
```

#### Solution: Per-Instance Journals with Consolidation

Each instance writes cache-hit updates to its own journal file, then a background consolidator applies them atomically:

```
Instance A: Record update → Buffer in RAM → Flush to instance-a.journal
Instance B: Record update → Buffer in RAM → Flush to instance-b.journal
Consolidator: Read all journals → Acquire lock → Apply to metadata → Truncate journals
```

#### Architecture

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
- Runs every 30 seconds (configurable)
- Reads entries from all instance journals
- Groups entries by cache key
- Acquires exclusive lock on metadata file
- Applies entries in timestamp order
- Truncates processed journal files

#### Lock Acquisition with Retry

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

#### Single-Instance Mode Optimization

When `shared_storage.enabled: false`, cache-hit updates bypass the journal system and write directly to metadata files. This provides better performance for single-instance deployments where race conditions are not a concern.

#### Data Flow

**Cache Hit (Shared Storage Mode)**:
```
GET Request → Cache HIT → Record TTL refresh in CacheHitUpdateBuffer
                       → Buffer accumulates entries
                       → Every 5s: Flush to instance journal
                       → Every 30s: Consolidator applies to metadata
```

**Cache Hit (Single-Instance Mode)**:
```
GET Request → Cache HIT → Direct metadata update (no journal)
```

#### Journal File Format

```json
{"timestamp":{"secs_since_epoch":1704067200},"instance_id":"proxy-1","cache_key":"bucket/object.txt","range_spec":{"start":0,"end":8388607},"operation":"TtlRefresh","new_ttl_secs":3600}
{"timestamp":{"secs_since_epoch":1704067201},"instance_id":"proxy-1","cache_key":"bucket/object.txt","range_spec":{"start":0,"end":8388607},"operation":"AccessUpdate","access_increment":5}
```

#### Consolidation Process

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

#### Error Handling

**Lock Contention**:
- Exponential backoff with jitter prevents thundering herd
- After max retries, consolidation skips the key (retried next cycle)

**Metadata File Missing**:
- Journal entries for non-existent metadata are skipped
- Logged as warning (may indicate evicted cache entry)

**Journal Parse Errors**:
- Invalid JSON lines are logged and skipped
- Valid entries in same file are still processed

#### Monitoring

Journal operations are logged at DEBUG/INFO level:

```
DEBUG Cache hit update buffered: cache_key=bucket/object.txt, type=TtlRefresh
INFO  Cache hit buffer flushed: entries=150, duration=5ms
INFO  Journal consolidation completed: cache_key=bucket/object.txt, entries=5, duration=12ms
```

#### Benefits

- **No Race Conditions**: Each instance writes to its own journal
- **Atomic Updates**: Consolidator holds lock for entire read-modify-write cycle
- **Reduced Disk I/O**: RAM buffering batches updates
- **Crash Recovery**: Unprocessed journal entries are applied on next consolidation
- **Scalability**: Works with any number of proxy instances

### Configuration

Access tracking is automatic and requires no configuration. Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Flush interval | 5 seconds | How often RAM buffer is flushed to disk |
| Max buffer entries | 10,000 | Buffer size before forced flush |
| Consolidation interval | 5 seconds | How often journal entries are consolidated to metadata (1-60s) |
| Bucket safe age | 15 seconds | Minimum age before a bucket can be processed |
| Consolidation jitter | 0-10 seconds | Per-instance offset to spread consolidation load |

### Monitoring

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

### Performance Characteristics

| Operation | Target | Actual |
|-----------|--------|--------|
| Record access (RAM buffer) | < 1ms | ~0.1ms |
| Buffer flush (100 entries) | < 10ms | ~5ms |
| Consolidation (1000 records) | < 500ms | ~200ms |
| Metadata update | < 10ms | ~5ms |

**Disk I/O Reduction**: The RAM buffering approach reduces disk writes by up to 99% compared to immediate writes:
- Old approach: 1 disk write per access
- New approach: 1 disk write per flush (batches hundreds of accesses)

### Troubleshooting

#### High Consolidation Duration

**Symptoms**:
- Consolidation taking > 1 second
- Log messages showing slow consolidation

**Causes**:
1. Large number of log files (many instances, high traffic)
2. Slow disk I/O (network storage latency)
3. Lock contention during metadata updates

**Solutions**:
1. Use faster storage (SSD instead of HDD)
2. Monitor disk I/O during consolidation
3. Check for lock contention in logs

#### Missing Access Data

**Symptoms**:
- `access_count` not increasing for frequently accessed ranges
- Eviction removing hot data

**Causes**:
1. Consolidation not running (check logs for errors)
2. Buffer not flushing (check flush logs)
3. Metadata updates failing (permission errors)

**Solutions**:
1. Check logs for consolidation and flush errors
2. Verify cache directory permissions
3. Ensure sufficient disk space for log files

#### Buffer Growing Too Large

**Symptoms**:
- Memory usage increasing over time
- Flush taking longer than expected

**Causes**:
1. Flush failing repeatedly (disk errors)
2. Very high access rate exceeding flush capacity

**Solutions**:
1. Check logs for flush errors
2. Monitor disk I/O and latency
3. Consider faster storage for access_tracking directory

## Distributed Eviction Coordination

### Overview

When multiple proxy instances share a cache volume, each instance independently monitors disk usage and triggers eviction when limits are exceeded. Without coordination, this causes race conditions where multiple instances simultaneously evict entries, leading to:

- **Over-eviction**: Multiple instances evict at once, removing more data than necessary
- **Cache thrashing**: Excessive eviction cycles reduce cache efficiency
- **Under-utilization**: Cache drops below optimal capacity

The distributed eviction lock solves this by ensuring only one instance performs eviction at a time.

### How It Works

```
Instance 1: Detects 11GB/10GB → Acquires lock → Evicts 1GB → Releases lock
Instance 2: Detects 11GB/10GB → Lock held → Skips eviction
Instance 3: Detects 11GB/10GB → Lock held → Skips eviction

Result: Only Instance 1 evicts, cache drops to 10GB (optimal)
```

### Lock Mechanism

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

### Configuration

```yaml
cache:
  max_cache_size: 10737418240  # 10GB
  
  # Distributed eviction coordination
  distributed_eviction:
    enabled: true                    # Enable distributed lock coordination
    lock_timeout_seconds: 60         # Lock timeout (30-3600 valid range)
```

**Configuration Options**:

- **enabled** (default: `false`): Enable distributed eviction coordination
  - Set to `true` for multi-instance deployments with shared cache
  - Set to `false` for single-instance deployments (no coordination overhead)
  
- **lock_timeout_seconds** (default: `60`): Maximum time a lock can be held
  - Valid range: 30-3600 seconds
  - If a lock holder crashes, other instances can forcibly acquire after timeout
  - Recommended: 60 seconds for most deployments

**Backward Compatibility**: If `distributed_eviction` is not configured, it defaults to disabled for backward compatibility with existing single-instance deployments.

### Lock Lifecycle

#### 1. Lock Acquisition

When an instance needs to evict:

1. Check if lock file exists
2. If exists, read lock metadata and check timestamp
3. If timestamp is older than timeout → lock is **stale** → forcibly acquire
4. If timestamp is fresh → lock is **held** → skip eviction
5. If no lock exists → create lock file atomically

**Atomic Operations**: Lock creation uses temp file + rename pattern to prevent race conditions.

#### 2. Lock Hold

While holding the lock:

- Instance performs eviction using configured algorithm (LRU/TinyLFU)
- Other instances skip eviction and log the reason
- Lock timeout prevents indefinite holding if instance crashes

#### 3. Lock Release

After eviction completes (success or failure):

1. Verify ownership by reading lock file
2. Delete lock file if owned by current instance
3. Log warning if lock is missing or owned by another instance

### Eviction Triggers

Eviction is triggered by the JournalConsolidator at the end of each consolidation cycle:

#### Consolidation-Based Triggering

Every 5 seconds (default) during consolidation:
- Consolidator collects per-instance delta files from SizeAccumulator flushes and sums them into size_state.json
- Updates size state with new total
- If total_size > max_cache_size, triggers eviction
- **Only triggers when size changes**: Avoids unnecessary lock acquisition during read-only workloads

**Example scenario:**
```
22:00 - Cache at 1.2 GB (limit: 1.0 GB) due to writes
22:00:05 - Consolidation cycle detects over limit → triggers eviction
22:00:06 - Cache back to 800 MB (80% target)
```

**Read-only workloads**: If no cache writes occur, no journal entries are created, so size doesn't change and eviction isn't triggered. This is expected behavior - the cache is stable.

### Behavior in Multi-Instance Scenarios

**Scenario: Multiple instances writing simultaneously**

```
Instance 1: writes 100MB → journal entry created
Instance 2: writes 100MB → journal entry created
Instance 3: consolidates → processes both entries → size=1.2GB → triggers eviction

5 seconds later (next consolidation cycle):
All instances: read size_state.json → see 800MB
```

Only one instance consolidates and triggers eviction. All instances see the updated size from the shared state file.

**Guaranteed Release**: Lock release happens even during errors using proper cleanup handlers.

### Stale Lock Recovery

If a lock holder crashes or hangs, other instances can recover:

1. Instance attempts to acquire lock
2. Reads existing lock file
3. Calculates: `current_time - acquired_at > timeout_seconds`
4. If stale → forcibly overwrites lock file
5. Logs warning for monitoring: `"Forcibly acquiring stale eviction lock held by instance X"`

**Recovery Time**: Maximum recovery time = `eviction_lock_timeout` (default 60 seconds)

### Troubleshooting

#### High Lock Contention

**Symptoms**:
- Frequent "lock held by another instance" log messages
- Multiple instances attempting eviction simultaneously
- `evictions_skipped_lock_held` metric increasing rapidly

**Solutions**:
1. Increase `max_cache_size` to reduce eviction frequency
2. Increase lock timeout if eviction takes longer than expected
3. Review eviction algorithm efficiency (LRU vs TinyLFU)
4. Consider adding more cache capacity

#### Stale Lock Warnings

**Symptoms**:
- Log messages: `"Forcibly acquiring stale eviction lock held by instance X"`
- `stale_locks_recovered` metric increasing

**Causes**:
1. Instance crashed during eviction
2. Instance hung or became unresponsive
3. Eviction taking longer than timeout
4. Network issues with shared storage

**Solutions**:
1. Investigate why instances are crashing (check logs)
2. Increase `lock_timeout_seconds` if eviction legitimately takes longer
3. Monitor instance health and restart unhealthy instances
4. Check shared storage performance (NFS latency)

#### Lock File Corruption

**Symptoms**:
- Log messages: `"Corrupted eviction lock file, treating as stale"`
- Eviction continues normally after warning

**Handling**:
- Corrupted lock files are automatically treated as stale
- Lock is forcibly acquired and overwritten
- No manual intervention needed
- Monitor for recurring corruption (may indicate storage issues)

#### Permission Errors

**Symptoms**:
- Log messages: `"Failed to acquire eviction lock: Permission denied"`
- Eviction is skipped

**Solutions**:
1. Verify cache directory permissions (typically 755 or 775)
2. Ensure all instances run with same user/group
3. Check shared storage mount permissions
4. Verify `locks/` subdirectory is writable

### Metrics

The following metrics are exposed via the `/metrics` endpoint:

```json
{
  "eviction_coordination": {
    "lock_acquisitions_successful": 42,
    "lock_acquisitions_failed": 15,
    "stale_locks_recovered": 2,
    "total_lock_hold_time_ms": 125000,
    "evictions_coordinated": 42,
    "evictions_skipped_lock_held": 15
  }
}
```

**Metric Descriptions**:

- **lock_acquisitions_successful**: Number of times this instance successfully acquired the lock
- **lock_acquisitions_failed**: Number of times lock was held by another instance (eviction skipped)
- **stale_locks_recovered**: Number of times this instance forcibly acquired a stale lock
- **total_lock_hold_time_ms**: Total milliseconds spent holding the eviction lock
- **evictions_coordinated**: Number of eviction operations performed while holding the lock
- **evictions_skipped_lock_held**: Number of times eviction was skipped due to lock contention

**Monitoring Recommendations**:

1. **Alert on high `stale_locks_recovered`**: May indicate instance crashes or hangs
2. **Monitor `lock_acquisitions_failed` rate**: High rate indicates lock contention
3. **Track `total_lock_hold_time_ms`**: Unusually long times may indicate slow eviction
4. **Compare `evictions_coordinated` across instances**: Should be roughly balanced

### Best Practices

#### Single-Instance Deployments

```yaml
cache:
  distributed_eviction:
    enabled: false  # No coordination overhead
```

Disable distributed eviction for single-instance deployments to avoid unnecessary filesystem operations.

#### Multi-Instance Deployments

```yaml
cache:
  distributed_eviction:
    enabled: true
    lock_timeout_seconds: 60  # Adjust based on eviction duration
```

**Recommendations**:
- Enable distributed eviction for all multi-instance deployments
- Set timeout to 2-3x typical eviction duration
- Monitor metrics to tune timeout value
- Use high-performance shared NFS storage

#### Lock Timeout Tuning

**Too Short** (< typical eviction time):
- Frequent stale lock warnings
- Multiple instances may evict simultaneously
- Reduces coordination effectiveness

**Too Long** (> 5 minutes):
- Slow recovery from crashed instances
- Cache may exceed limits for extended periods
- Delayed eviction after failures

**Recommended Starting Point**: 60 seconds
- Adjust based on `total_lock_hold_time_ms` metric
- Set to 2-3x average eviction duration

#### Shared Storage Considerations

- **NFS**: Ensure file locking is enabled (`nolock` mount option disabled)
- **Local Storage**: Not suitable for multi-instance coordination
- **Network Latency**: Higher latency may require longer timeouts

### Performance Characteristics

Comprehensive performance testing has validated the distributed eviction lock implementation. See `EVICTION_LOCK_PERFORMANCE_REPORT.md` for detailed results.

**Measured Performance** (from performance test suite):

| Operation | Average | P95 | Design Target | Status |
|-----------|---------|-----|---------------|--------|
| Lock Acquisition | 410µs | 565µs | 1-5ms | ✅ Exceeds |
| Lock Release | 466µs | 584µs | 1-5ms | ✅ Exceeds |
| Stale Lock Check | 1.6ms | 3.7ms | ~1ms | ✅ Meets |
| Full Cycle | 875µs | 1.2ms | 2-10ms | ✅ Exceeds |

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

**Recommendation**: Enable distributed eviction coordination for all multi-instance deployments. The minimal performance overhead (<5ms per eviction) is far outweighed by the benefits of preventing over-eviction and cache thrashing.

## Range-Based Disk Cache Eviction

### Overview

The disk cache uses **range-level eviction** where each cached range is an independent eviction candidate. This provides fine-grained cache management that retains hot ranges while evicting cold ones, even within the same object.

### Eviction Algorithms

**LRU (Least Recently Used)**:
- Evicts ranges based on last access time
- Oldest accessed ranges are evicted first
- Simple and predictable behavior

**TinyLFU (default)**:
- Combines access frequency and recency for optimal cache hit rates
- Uses probabilistic frequency sketches for memory efficiency
- Better performance than LRU for most workloads

### Range-Level Granularity

All eviction operates at the range level:
- Every cached range is an independent eviction candidate
- Ranges are sorted by the selected eviction algorithm
- Hot ranges are retained even if other ranges of the same object are cold
- Metadata file is deleted only when all ranges are evicted

### Eviction Flow

```
1. Collect    → Scan all .meta files, create candidate for each range
2. Sort       → Order by eviction algorithm (LRU: oldest first, TinyLFU: lowest score first)
3. Group      → Batch ranges by object for efficient metadata updates
4. Evict      → Delete .bin files, update metadata atomically (one write per object)
5. Cleanup    → Delete .meta when empty, remove empty directories
```

### Per-Range Access Tracking

Each range maintains independent access statistics in the metadata file:

```json
{
  "ranges": [
    {
      "start": 0,
      "end": 8388607,
      "last_accessed": "2024-01-15T10:30:00Z",
      "access_count": 42
    },
    {
      "start": 8388608,
      "end": 16777215,
      "last_accessed": "2024-01-15T08:15:00Z",
      "access_count": 3
    }
  ]
}
```

- `last_accessed`: Timestamp of last access (used by LRU algorithm)
- `access_count`: Number of accesses (used by TinyLFU algorithm)

### Metadata Cleanup

The `.meta` file is deleted only when:
1. All ranges have been evicted (ranges list is empty)
2. Associated lock files are also deleted
3. Empty parent directories are cleaned up recursively

This ensures partial cache entries remain usable for subsequent requests.

### Benefits

| Aspect | Range-Level Eviction |
|--------|---------------------|
| Eviction granularity | Always range-level |
| Hot range retention | Hot ranges always retained |
| Cache efficiency | Optimal utilization |
| Metadata updates | Batched per-object |
| Directory cleanup | Automatic recursive cleanup |

### Example Scenario

**Object A** (100MB, 10 ranges of 10MB each):
- Range 0-10MB: accessed 5 min ago, count=50 → **Keep**
- Range 10-20MB: accessed 2 hours ago, count=2 → **Evict candidate**
- Range 20-30MB: accessed 1 min ago, count=100 → **Keep**
- ...

**Result**: Only cold ranges evicted, hot ranges retained. Metadata updated once with all evicted ranges removed.

### Admission Window Protection

Newly cached ranges are protected from immediate eviction for 60 seconds. This prevents cache thrashing during large file downloads where new ranges would otherwise be evicted immediately due to having zero access history in TinyLFU.

**How It Works:**

1. When collecting eviction candidates, each range's `last_accessed` timestamp is checked
2. Ranges cached within the last 60 seconds are skipped as eviction candidates
3. This gives new ranges time to accumulate access statistics before competing for cache space

**Example Scenario:**
```
Large file download in progress:
- Range 0-8MB: cached 5 seconds ago → Protected (within admission window)
- Range 8-16MB: cached 2 seconds ago → Protected (within admission window)
- Range 16-24MB: cached 3 minutes ago, access_count=1 → Eviction candidate
- Range 24-32MB: cached 1 hour ago, access_count=50 → Eviction candidate (but high score)
```

**Benefits:**
- Prevents evicting ranges that were just downloaded
- Allows new ranges to build access history before eviction decisions
- Reduces cache thrashing during streaming downloads

### Critical Capacity Bypass

When cache usage exceeds 110% of the configured limit, the admission window protection is bypassed to aggressively reclaim space.

**Trigger Condition:**
```
current_cache_size > max_cache_size * 1.10
```

**Behavior:**
- Normal eviction (≤110%): Respects 60-second admission window
- Critical eviction (>110%): Bypasses admission window, all ranges are eviction candidates

**Why This Exists:**

In extreme scenarios (rapid writes, burst traffic), the cache can exceed its limit faster than normal eviction can reclaim space. The critical bypass ensures:
- Cache size is brought under control quickly
- Disk space exhaustion is prevented
- System stability is maintained even under heavy load

**Logging:**
```
INFO [DISK_CACHE_EVICTION] Critical capacity exceeded (11.5 GiB / 10 GiB = 115%), bypassing admission window
INFO [DISK_CACHE_EVICTION] Eviction completed: ranges_evicted=150, freed=2.0 GiB, new_usage=9.5 GiB / 10 GiB (95.0%)
```

### Logging

Eviction operations are logged at INFO level:

```
INFO [DISK_CACHE_EVICTION] Eviction completed: keys_evicted=3, ranges_evicted=47, freed=1.5 GiB, new_usage=8.5 GiB / 10 GiB (85.0%)
INFO [BATCH_EVICTION] Metadata deleted: cache_key=/bucket/object.txt, reason=all_ranges_evicted
DEBUG [BATCH_EVICTION] Directory cleanup: removed /cache/ranges/bucket/a7/f3c (empty)
```

**Log Fields**:
- **keys_evicted**: Number of objects that had ranges evicted
- **ranges_evicted**: Total number of individual ranges evicted
- **freed**: Human-readable bytes freed (KiB/MiB/GiB)
- **new_usage**: Current cache size after eviction vs max size with percentage

### Configuration

```yaml
cache:
  max_cache_size: 10737418240  # 10GB - eviction triggers at 95% (9.5GB)
  eviction_algorithm: "tinylfu"  # or "lru"
```

**Eviction Algorithms**:
- **LRU (Least Recently Used)**: Evicts ranges with oldest `last_accessed` timestamp first
- **TinyLFU**: Combines frequency (`access_count`) and recency (`last_accessed`) for optimal eviction decisions

## Monitoring

### Key Metrics

- `cache.hits`: Requests served from cache
- `cache.misses`: Requests forwarded to S3
- `cache.hit_rate`: Percentage of cache hits
- `cache.size_bytes`: Current cache size
- `cache.evictions`: Entries removed due to size limits
- `cache.invalidations`: Entries removed due to staleness
- `cache.write_cache_stores`: Objects cached during PUT operations
- `cache.multipart_uploads_initiated`: Multipart uploads started
- `cache.multipart_uploads_completed`: Multipart uploads successfully cached
- `cache.multipart_uploads_bypassed`: Multipart uploads exceeding capacity
- `cache.incomplete_uploads_cleaned`: Abandoned uploads removed by cleanup
- `cache.part_hits`: GetObjectPart requests served from cache
- `cache.part_misses`: GetObjectPart requests forwarded to S3
- `cache.part_stores`: Parts cached as ranges
- `cache.part_evictions`: Parts evicted from cache
- `cache.part_errors`: Part caching operation errors
- `range_merge.operations_total`: Total range merge operations performed
- `range_merge.cache_efficiency_avg`: Average cache efficiency percentage
- `range_merge.segments_merged_avg`: Average segments per merge operation
- `range_merge.bytes_from_cache_total`: Total bytes served from cache via merging
- `range_merge.bytes_from_s3_total`: Total bytes fetched from S3 for merging
- `range_merge.s3_requests_saved`: S3 requests avoided via range consolidation
- `eviction_coordination.lock_acquisitions_successful`: Successful lock acquisitions
- `eviction_coordination.lock_acquisitions_failed`: Lock contention events
- `eviction_coordination.stale_locks_recovered`: Stale lock recovery events

### Cache Efficiency

**Good cache hit rate**: 70-90%
- Indicates effective caching
- Significant bandwidth savings

**Low cache hit rate**: <50%
- May need larger cache size
- May need longer TTLs
- Check if workload is cache-friendly

**Range Merge Efficiency**:

With intelligent range merging, even partial cache hits provide significant benefits:

- **100% efficiency**: All bytes served from cache (no S3 fetch)
  - Common after multipart uploads
  - Indicates excellent cache coverage
  
- **50-90% efficiency**: Majority of bytes from cache
  - Significant bandwidth savings
  - Faster than full S3 fetch
  - Indicates good cache alignment
  
- **30-50% efficiency**: Moderate cache benefit
  - Still reduces S3 bandwidth
  - May indicate fragmented cache coverage
  - Consider increasing cache size or adjusting access patterns
  
- **<30% efficiency**: Limited cache benefit
  - Most bytes fetched from S3
  - May indicate poor cache alignment with access patterns
  - Consider adjusting `range_merge_gap_threshold`

**Monitoring Range Merge Efficiency**:

```bash
# View range merge operations in logs
grep "Range merge completed" /logs/app/*/app.log

# Calculate average cache efficiency
grep "cache_efficiency" /logs/app/*/app.log | \
  awk -F'cache_efficiency=' '{print $2}' | \
  awk -F'%' '{sum+=$1; count++} END {print sum/count "%"}'

# Count S3 requests saved by consolidation
grep "Consolidated.*missing ranges" /logs/app/*/app.log | \
  awk '{saved+=$2-$4} END {print "S3 requests saved:", saved}'
```

**Optimization Tips**:

1. **Increase gap threshold** if you see many small S3 fetches
2. **Enable write caching** to populate cache with multipart uploads
3. **Increase cache size** to retain more ranges
4. **Monitor access patterns** to understand range request distribution

## Cache Coherency

### Overview

The proxy implements comprehensive cache coherency mechanisms to ensure clients always receive consistent, up-to-date data. These mechanisms prevent serving stale data when full objects are cached alongside partial ranges, and when range requests don't validate ETags against current object versions.

### Full Object Range Replacement

When a full object is successfully cached, the proxy automatically invalidates all existing partial ranges for that object to prevent serving stale data.

**How It Works:**

1. **Range Invalidation**: When caching a full object, all existing range files for that cache key are removed
2. **Metadata Cleanup**: Object metadata is updated to reflect only the full object, removing references to deleted ranges
3. **Atomic Operations**: Range cleanup and metadata updates use proper locking to ensure consistency
4. **Logging**: All range invalidation events are logged with details of affected ranges

**Example Scenario:**
```
Existing cache: object.bin ranges 0-8MB, 16-24MB, 32-40MB (partial coverage)
Client uploads new version via PUT → Full object cached as range 0-50MB
Action: Invalidate all existing ranges (0-8MB, 16-24MB, 32-40MB)
Result: Only full object 0-50MB remains in cache
```

**Benefits:**
- Prevents serving stale partial ranges from previous object versions
- Ensures range requests are served from current full object data
- Maintains cache consistency across concurrent operations

### ETag Validation for Range Requests

Range requests validate cached range ETags against cached object metadata ETags before serving from cache.

**How It Works:**

1. **ETag Comparison**: Before serving cached ranges, compare range ETag with object metadata ETag
2. **Mismatch Handling**: If ETags don't match, invalidate all cached ranges and forward request to S3
3. **Orphaned Range Cleanup**: If no object metadata exists for ranges, invalidate orphaned ranges
4. **Logging**: All ETag validation results and actions are logged for monitoring

**Validation Flow:**
```
Range Request → Check cached ranges exist
             → Get object metadata ETag
             → Compare range ETag with object ETag
             → Match: Serve from cache
             → Mismatch: Invalidate ranges, forward to S3
             → No metadata: Cleanup orphaned ranges, forward to S3
```

**Example Scenarios:**

**ETag Match (Serve from Cache):**
```
Cached range: 0-8MB, ETag="abc123"
Object metadata: ETag="abc123"
Action: Serve range from cache (ETags match)
```

**ETag Mismatch (Invalidate and Forward):**
```
Cached range: 0-8MB, ETag="abc123" (from old version)
Object metadata: ETag="def456" (from new version)
Action: Invalidate cached range, forward request to S3
Log: "ETag validation failed: range_etag=abc123, object_etag=def456, range is stale"
```

**Orphaned Ranges (Cleanup):**
```
Cached range: 0-8MB, ETag="abc123"
Object metadata: Not found
Action: Remove orphaned range, forward request to S3
Log: "Cleaning up orphaned ranges for cache_key: /bucket/object.txt"
```

### Metadata Consistency

The proxy maintains consistency between range files and object metadata through atomic operations and proper locking.

**Consistency Mechanisms:**

1. **Atomic Updates**: Metadata updates use temporary files and atomic renames
2. **File Locking**: Exclusive locks prevent concurrent modifications during updates
3. **Cleanup Coordination**: Range file removal and metadata updates are coordinated
4. **Error Recovery**: Failed operations are logged and retried in background

**Metadata Operations:**
- **Range Invalidation**: Update metadata to remove references to deleted ranges
- **ETag Updates**: Update object metadata when new versions are cached
- **Cleanup Tracking**: Track which ranges have been removed for consistency

### Concurrent Operation Safety

The proxy uses file locking and atomic operations to ensure cache coherency during concurrent access.

**Safety Mechanisms:**

1. **Metadata Locks**: Exclusive locks with timeouts prevent corruption during updates
2. **Lock Cleanup**: Stale locks are automatically recovered after timeout
3. **Atomic Operations**: File operations use atomic patterns (temp file + rename)
4. **Error Handling**: Lock acquisition failures are handled gracefully with fallback

**Lock Timeout Configuration:**
```yaml
cache:
  metadata_lock_timeout_seconds: 60  # Default: 60 seconds
```

**Lock Recovery:**
- If a process crashes while holding a lock, other processes can recover after timeout
- Stale lock detection prevents indefinite blocking
- Lock recovery events are logged for monitoring

### Error Handling and Fallback

Cache coherency operations include comprehensive error handling to ensure clients always receive correct data.

**Error Scenarios and Responses:**

1. **Metadata Corruption**: Detect corrupted metadata files and invalidate affected entries
2. **Range File Missing**: Handle cases where metadata references non-existent range files
3. **Lock Timeout**: Gracefully handle lock acquisition failures with fallback behavior
4. **Cleanup Failures**: Log cleanup errors and continue serving requests

**Fallback Behavior:**
- When cache coherency operations fail, requests are forwarded directly to S3
- Clients always receive correct data even if cache operations fail
- Failed operations are retried in background where possible

### Monitoring Cache Coherency

The proxy exposes metrics and logging for monitoring cache coherency operations:

**Metrics:**
```json
{
  "cache_coherency": {
    "etag_validations_total": 1250,
    "etag_mismatches_total": 15,
    "range_invalidations_total": 8,
    "orphaned_ranges_cleaned_total": 3,
    "metadata_corruption_total": 1,
    "cleanup_failures_total": 0,
    "lock_timeouts_total": 0
  }
}
```

**Key Metrics:**
- **etag_validations_total**: Total ETag validations performed
- **etag_mismatches_total**: ETags that didn't match (stale ranges detected)
- **range_invalidations_total**: Range invalidations due to full object caching
- **orphaned_ranges_cleaned_total**: Orphaned ranges cleaned up
- **metadata_corruption_total**: Corrupted metadata files detected
- **cleanup_failures_total**: Failed cleanup operations
- **lock_timeouts_total**: Lock acquisition timeouts

**Logging Examples:**
```
INFO Range invalidation for full object: cache_key=/bucket/object.txt, 
     ranges_removed=3, size_freed=25165824

INFO ETag validation failed: cache_key=/bucket/object.txt, 
     range_etag=abc123, object_etag=def456, range is stale

INFO Cleaning up orphaned ranges for cache_key: /bucket/object.txt, 
     ranges_removed=2, size_freed=16777216

WARN Metadata corruption detected: cache_key=/bucket/object.txt, 
     error="Invalid JSON", action="invalidated entry"
```

### Performance Impact

Cache coherency operations are designed to have minimal performance impact:

**Operation Performance:**
- ETag validation: < 1ms (metadata-only comparison)
- Range invalidation: 10-50ms (depending on number of ranges)
- Metadata updates: < 10ms (atomic file operations)
- Lock operations: < 5ms (filesystem-based locks)

**Optimization Features:**
- ETag validation uses only cached metadata (no S3 requests)
- Range cleanup is batched for efficiency
- Background cleanup doesn't block request serving
- Lock timeouts prevent indefinite blocking

## Signed Range Request Handling

### Overview

When AWS CLI or SDKs sign GET requests with AWS Signature Version 4 (SigV4), they may include the Range header in the SignedHeaders list. This creates a challenge for the proxy: modifying the Range header to fetch only missing cache portions would invalidate the signature, causing S3 to return 403 Forbidden errors.

### How It Works

The proxy detects signed range requests and handles them specially:

1. **Detection**: When a range request has cache gaps, the proxy checks if the Range header is included in the AWS SigV4 signature's SignedHeaders list
2. **Forwarding**: If the range is signed, the entire original request is forwarded to S3 unchanged (preserving signature validity)
3. **Caching**: The response is cached while streaming to the client
4. **Subsequent Requests**: Future requests for the same range are served from cache

### Request Flow

```
Client Request (Signed Range)
    ↓
Check Cache Coverage
    ↓
┌─────────────────────────────┐
│ Fully Cached?               │
│ Yes → Serve from cache      │──→ Response to Client
│ No  → Continue              │
└─────────────────────────────┘
    ↓
Parse Authorization Header
    ↓
┌─────────────────────────────┐
│ Range in SignedHeaders?     │
│ No  → Standard logic        │──→ Fetch missing ranges only
│ Yes → Signed range handling │
└─────────────────────────────┘
    ↓
Forward Entire Range to S3
    ↓
Stream Response to Client
    ↓
Cache Response for Future Requests
```

### Key Behaviors

- **Signature Preservation**: All request headers are forwarded exactly as received
- **Streaming**: Responses are streamed to clients immediately (no buffering)
- **Background Caching**: Cache writes happen asynchronously without blocking the client
- **Error Resilience**: Cache failures don't affect client responses

### When This Applies

Signed range requests are detected when:
- The request has an `Authorization` header with `AWS4-HMAC-SHA256`
- The `SignedHeaders` parameter includes `range`
- There are cache gaps (partial or complete cache miss)

### Performance Considerations

- **First Request**: Full range is fetched from S3 (same as unsigned request with cache miss)
- **Subsequent Requests**: Served entirely from cache (no S3 request)
- **Overhead**: Minimal - signature detection only happens when cache gaps exist

## Limitations

### What the Proxy Cannot Do

1. **Cannot initiate requests to S3**
   - No AWS credentials
   - Cannot sign requests
   - Only forwards client requests

2. **Cannot proactively validate cache**
   - Waits for client requests
   - Validation happens on-demand

3. **Cannot cache without client requests**
   - No pre-warming
   - No background refresh

### Capacity Limits

**Write Cache Capacity:**
- Configurable percentage of total cache (default: 10%)
- Applies to both PUT operations and multipart uploads
- Multipart uploads exceeding capacity are automatically bypassed
- Bypass is transparent to clients (upload continues to S3 normally)

**Single Object Limits:**
- PUT operations: Configurable max size (default: 256MB)
- Multipart uploads: Limited by cumulative write cache capacity
- Objects exceeding limits are not cached but still proxied to S3

### Workarounds

**Pre-warming cache**: Have clients request objects after deployment

**Background validation**: Not possible - use shorter TTLs instead

**Proactive invalidation**: Not supported - rely on TTL expiration

**Large multipart uploads**: Increase `write_cache_percent` or `max_cache_size` to cache larger uploads

## Cache Size Tracking

### Overview

The proxy provides scalable cache size tracking for multi-instance deployments with shared disk cache. Size tracking uses an in-memory accumulator approach that records size changes at write/eviction time with zero NFS overhead, then periodically flushes deltas to per-instance files that the consolidator sums into the authoritative size state.

### Architecture

Cache size tracking uses an in-memory `AtomicI64` accumulator per proxy instance:

```
store_range() success → accumulator.add(compressed_size)
eviction              → accumulator.subtract(compressed_size)
                              ↓
                    Flush to delta_{instance_id}.json (every 5s)
                              ↓
                    JournalConsolidator (under global lock)
                              ↓
                    Sum all delta files → Update size_state.json
                              ↓
                    Reset delta files to zero
                              ↓
                    Trigger Eviction (if over capacity)
```

**Key Benefits:**
- **Zero NFS Overhead**: Size tracked at write/eviction time using atomic operations
- **No Timing Gaps**: Size recorded immediately when data is written, not during consolidation
- **Single Source of Truth**: Consolidator sums all delta files under global lock
- **Crash Recovery**: At most 5 seconds of deltas lost; daily validation corrects drift

### How It Works

#### In-Memory Accumulator

Each proxy instance maintains an `AtomicI64` accumulator that tracks the net size delta since the last flush:

```rust
// On successful range write
accumulator.add(compressed_size);

// On range eviction
accumulator.subtract(compressed_size);
```

**Write Cache Tracking**: A separate `write_cache_delta` accumulator tracks write-cached ranges (PUT operations and multipart uploads).

#### Delta File Flush

Every consolidation cycle (5 seconds by default), each instance flushes its accumulated delta to a per-instance file:

```
Delta File (size_tracking/delta_{instance_id}.json):
{
  "delta": 1048576,
  "write_cache_delta": 0,
  "instance_id": "proxy1.example.com:12345",
  "timestamp": "2026-01-26T15:30:00.000Z"
}
```

The flush uses atomic swap-to-zero: if the file write fails, the swapped values are restored to the accumulator.

#### Consolidator Integration

The consolidator (under global lock) reads all delta files, sums them, and updates the size state:

```
Consolidation Cycle:
1. Flush own accumulator to delta file (before lock)
2. Acquire global consolidation lock
3. Read all delta_*.json files from size_tracking/
4. Sum delta and write_cache_delta values
5. Add sums to size_state.json (clamping to 0)
6. Reset each delta file to zero
7. Process journal entries for metadata updates only
8. Trigger eviction if over capacity
9. Release lock
```

#### Size State Persistence

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
- **write_cache_size**: Write cache size in bytes (subset of total_size)
- **last_consolidation**: Unix timestamp of last consolidation cycle
- **consolidation_count**: Number of consolidation cycles completed
- **last_updated_by**: Instance ID that last updated the state

#### Startup Recovery

On startup, the consolidator loads the existing size state:

```
Recovery Flow:
1. Try to load size_state.json
2. If found → Initialize with persisted values
3. If not found → Start at 0, validation will correct
```

Recovery is instant (single file read) regardless of cache size. The in-memory accumulator starts at zero.

#### Eviction Triggering

The consolidator triggers eviction automatically when cache exceeds capacity:

```
Consolidation Cycle:
1. Flush accumulator to delta file
2. Acquire global lock
3. Collect and apply deltas from all instances
4. If total_size > max_cache_size:
   - Trigger eviction via CacheManager
   - Eviction decrements accumulator for each evicted range
5. Persist size state
6. Release lock
```

**Eviction is only triggered when size changes**, avoiding unnecessary lock acquisition during read-only workloads.

#### Daily Validation

Once per day (default: midnight local time with 1-hour jitter), the system performs a full metadata scan to verify tracked size accuracy:

```
Validation Flow:
1. Acquire global validation lock (multi-instance coordination)
2. Scan all .meta files in parallel across CPU cores
3. Sum total cached size from metadata
4. Compare to tracked size → calculate drift
5. If drift > threshold (1GB default) → reconcile to scanned size
6. Reset all delta files to zero (prevent stale deltas)
7. Reset in-memory accumulator to zero
8. Release lock
```

**Scheduling**: Validation runs at a configured time of day (default: midnight local time) with fixed 1-hour jitter to prevent thundering herd when multiple instances start simultaneously.

**Multi-Instance Coordination**: Only one instance performs validation at a time using a global lock, preventing duplicate work.

### HEAD Cache Cleanup

The validation scan includes automatic cleanup of expired HEAD cache entries:

#### Periodic Cleanup (During Validation)

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

### Known Limitation: Size Over-Counting Under Concurrent Multi-Instance Writes

When multiple proxy instances simultaneously cache the same range (e.g., during a burst of concurrent cache misses), each instance adds the range size to its in-memory accumulator independently. On shared NFS storage, only one physical file survives (last writer wins), but the tracked size increases by N× (once per instance that wrote the range).

**Magnitude**: In testing with 100 concurrent clients across 3 proxy instances, a cold-cache stampede resulted in tracked size of ~3× actual disk usage (bounded by the number of proxy instances). Each instance independently counts the bytes it writes to the shared filesystem, even though only one physical file survives.

**Why it can't be fully fixed at write time**: Each proxy instance writes the range file and counts the bytes in its in-memory accumulator. On shared storage, all instances write the same file (last writer wins), but each counts independently. An `exists()` check before writing doesn't help during a stampede because all instances check simultaneously before any have written. The over-count is bounded by the number of proxy instances (N instances = up to N× over-count).

**Mitigation**: The daily validation scan corrects drift by scanning actual metadata and reconciling the tracked size. For deployments where stampede over-counting causes premature eviction, reduce `validation_frequency` (e.g., from `24h` to `1h` or `15m`). The validation scan reads metadata files only (not range data), so it scales with object count, not data volume.

**Safety**: Over-counting is the safe direction — it triggers eviction early rather than late, preventing disk from filling. Eviction correctly decrements the accumulator for each deleted range, so the tracked size converges toward accuracy as eviction runs.

#### Lazy Deletion (On Read)

In addition to periodic validation, cache entries are checked for expiration on every read:

```
Client HEAD Request:
1. Check MetadataCache for entry
2. If found, check head_expires_at timestamp
3. If expired:
   - Fetch fresh from S3
   - Update head_expires_at in .meta file
4. If not expired:
   - Return cached headers
```

**Benefits**:
- Frequently-accessed entries are refreshed immediately without waiting for validation
- Reduces stale metadata in cache
- Minimal overhead (expiration check is fast)

#### Lazy vs Periodic Cleanup

**Lazy Deletion** (on read):
- **When**: Every time a HEAD cache entry is accessed
- **Scope**: Only the specific entry being accessed
- **Overhead**: Minimal (< 1ms per read)
- **Benefit**: Immediate cleanup of frequently-accessed expired entries

**Periodic Cleanup** (during validation):
- **When**: Once per day during validation scan
- **Scope**: All HEAD cache entries
- **Overhead**: Included in validation scan (no additional cost)
- **Benefit**: Cleans up rarely-accessed expired entries that wouldn't be caught by lazy deletion

**Combined Approach**: The dual approach ensures expired HEAD cache entries are removed efficiently:
- Hot entries: Cleaned up immediately on next access (lazy)
- Cold entries: Cleaned up during daily validation (periodic)
- Result: Minimal stale metadata accumulation

### GET Cache Expiration (Optional)

The validation scan can optionally perform active expiration cleanup for GET cache entries (actual cached data: metadata + ranges):

#### Configuration

```yaml
cache:
  actively_remove_cached_data: false  # Default: lazy expiration only
```

**Lazy Expiration** (default, `actively_remove_cached_data: false`):
- Expired GET cache entries remain on disk until accessed
- On GET request, if expired, fetch fresh data and update cache
- Saves CPU/IO from background cleanup processes
- Recommended for most use cases

**Active Expiration** (`actively_remove_cached_data: true`):
- Validation scan actively removes expired GET cache entries
- Frees disk space immediately when GET_TTL expires
- Not compatible with TTL=0 (entries expire immediately and are deleted before they can be revalidated)
- Useful when disk space is constrained
- Adds background CPU/IO overhead during validation

#### Multi-Instance Safety

When active GET cache expiration is enabled, the system checks if entries are actively being used before deletion:

```
Active Expiration Flow:
1. Find expired GET cache entry
2. Check if entry is actively being used by another instance
3. If active → Skip deletion (entry is in use)
4. If not active → Delete metadata + ranges
5. Track skipped entries in validation results
```

**Important**: HEAD cache cleanup does NOT need active-use checks because HEAD entries are metadata-only and safe to remove.

### Configuration

Size tracking is configured through the `shared_storage` section:

```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"     # How often to consolidate and update size (default: 5s)
  
  # Cache expiration control
  actively_remove_cached_data: false   # false = lazy only, true = lazy + daily scan
```

**Configuration Options**:

- **consolidation_interval**: How often to run consolidation and update size (default: 5 seconds)
  - Shorter intervals: More frequent size updates, slightly more I/O
  - Longer intervals: Less frequent updates, larger batches
  - Valid range: 1-60 seconds

- **validation_time_of_day**: Time of day for daily validation in 24-hour format "HH:MM" (default: "00:00" = midnight local time)
  - Examples: "00:00" (midnight), "03:30" (3:30 AM), "14:00" (2:00 PM)
  - Fixed 1-hour jitter is automatically applied to prevent thundering herd
  - Validation runs once per 24-hour period globally (across all instances)

- **validation_threshold_warn**: Drift percentage that triggers a warning (default: 5.0%)
- **validation_threshold_error**: Drift percentage that triggers an error (default: 20.0%)

- **actively_remove_cached_data**: Control cache expiration behavior (default: false)
  - false: Lazy expiration only (expired entries removed on access)
  - true: Lazy + active expiration (validation scan removes expired entries)

### Metrics

Cache size tracking exposes detailed metrics via the `/metrics` endpoint and dashboard:

```json
{
  "cache_size": {
    "current_bytes": 5368709120,
    "write_cache_bytes": 268435456,
    "last_consolidation": "2026-01-26T15:30:00.000Z",
    "consolidation_count": 12345,
    "last_validation": "2026-01-26T00:00:00.000Z",
    "last_validation_drift_percent": 0.5
  }
}
```

**Metric Descriptions**:

- **current_bytes**: Current total cache size in bytes
- **write_cache_bytes**: Current write cache size in bytes (subset of current_bytes)
- **last_consolidation**: Timestamp of last consolidation cycle
- **consolidation_count**: Total number of consolidation cycles completed
- **last_validation**: Timestamp of last validation scan
- **last_validation_drift_percent**: Drift detected during last validation as percentage

### Monitoring

#### Key Metrics to Monitor

**Consolidation Cycles**:
```bash
# Check consolidation activity
grep "Consolidation cycle" /logs/app/*/app.log

# Expected output:
# Consolidation cycle: entries=15, size_delta=+10485760, evicted=0, current_size=5368709120
```

**Eviction Triggering**:
```bash
# Check eviction activity
grep "eviction" /logs/app/*/app.log

# Expected output:
# Eviction triggered: current_size=10737418240, max_size=10737418240, bytes_freed=1073741824
```

**Drift Detection**:
```bash
# Check validation drift
grep "Validation complete" /logs/app/*/app.log | grep "drift="

# Expected output:
# drift=0.5% (under threshold, normal)
# drift=15.0% (over warn threshold, logged as warning)
```

#### Validation Scheduling

```bash
# Check next validation time
grep "Next validation scheduled" /logs/app/*/app.log

# Expected output:
# Next validation scheduled for 2026-01-27T00:37:42Z (in 43862 seconds)
```

#### Performance Monitoring

```bash
# Check consolidation cycle duration
grep "Consolidation cycle" /logs/app/*/app.log | grep "duration="

# Expected output:
# duration=45ms (typical for <1000 entries)
```

### Performance Characteristics

**Consolidation Cycle**:
- Frequency: Every 5 seconds (default)
- Duration: < 1 second for typical workloads (< 1000 entries)
- Size state read is non-blocking (for dashboard/metrics)
- No additional disk I/O beyond existing journal operations

**Eviction Triggering**:
- Only triggered when size changes (not during read-only workloads)
- Uses distributed locking for multi-instance coordination
- Size state updated immediately after eviction

**Validation Scan**:
- Frequency: Once per day (configurable time)
- Duration: ~26 minutes for 100M objects (parallel scanning)
- CPU usage: Limited to 50% (parallel processing across cores)
- Does not impact cache operation latency

**Recovery**:
- Duration: Instant (single file read)
- Happens only on startup
- No replay of delta logs needed

### Troubleshooting

#### High Drift Detected

**Symptoms**:
- Validation logs show drift > 5% (warning) or > 20% (error)
- Log messages: "Cache size drift detected: X% (threshold: Y%)"

**Causes**:
1. Orphaned range files (range file exists but not tracked in metadata)
2. External modifications to cache files
3. Disk errors causing incomplete writes

**Solutions**:
1. Validation will automatically correct drift
2. Verify cache directory permissions (no external modifications)
3. Check disk health (SMART status, filesystem errors)

#### Eviction Not Triggering

**Symptoms**:
- Cache exceeds `max_cache_size` but no eviction occurs
- Dashboard shows cache over capacity

**Causes**:
1. No cache writes occurring (read-only workload)
2. Consolidation not running
3. Eviction lock held by another instance

**Solutions**:
1. Eviction only triggers when size changes - this is expected for read-only workloads
2. Check consolidation is running: `grep "Consolidation cycle" /logs/app/*/app.log`
3. Check for stuck eviction locks: `ls -la {cache_dir}/locks/`

#### Consolidation Errors

**Symptoms**:
- Log messages: "Consolidation cycle failed"
- Size state not updating

**Causes**:
1. Permission errors on cache directory
2. Disk full
3. NFS connectivity issues

**Solutions**:
1. Verify cache directory permissions (755 or 775)
2. Check disk space availability
3. Check NFS mount status and connectivity

### Best Practices

#### Single-Instance Deployments

```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"
  
  actively_remove_cached_data: false  # Lazy expiration sufficient
```

**Rationale**:
- Default consolidation interval provides near-realtime size tracking
- Lazy expiration minimizes overhead
- Validation provides drift detection and HEAD cache cleanup

#### Multi-Instance Deployments

```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"
    validation_frequency: "23h"
  
  actively_remove_cached_data: false  # Lazy expiration recommended
```

**Rationale**:
- All instances share the same size_state.json file
- Consolidation lock ensures only one instance updates at a time
- Lazy expiration reduces coordination overhead
- HEAD cache cleanup runs automatically during validation

#### Disk-Constrained Environments

```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"
    validation_threshold_warn: 2.0    # More aggressive warning
    validation_threshold_error: 10.0  # More aggressive error
  
  actively_remove_cached_data: true  # Active expiration to free space
```

**Rationale**:
- Active expiration frees disk space immediately
- Tighter drift thresholds for better control
- Eviction triggers automatically when over capacity

#### Large-Scale Deployments (100M+ Objects)

```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"
    validation_threshold_warn: 10.0   # Tolerate more drift
    validation_threshold_error: 30.0
  
  actively_remove_cached_data: false  # Lazy expiration (less overhead)
```

**Rationale**:
- Larger drift thresholds tolerate natural variance at scale
- Lazy expiration minimizes validation overhead
- Validation still provides drift detection and HEAD cache cleanup

### Limitations

**Consolidation Interval**:
- Size updates occur every 5 seconds (default)
- Dashboard/metrics may show slightly stale values between cycles
- Eviction only triggers at end of consolidation cycle

**Drift Sources**:
- Orphaned range files (range file exists but never journaled due to crash during write)
- External modifications to cache files (not recommended)
- Disk errors causing incomplete writes

**Multi-Instance Coordination**:
- All instances share the same size_state.json file
- Consolidation lock ensures only one instance updates at a time
- Instances that don't consolidate read size from shared state file

**HEAD Cache Cleanup**:
- Errors during cleanup are logged but do not stop validation
- Corrupted HEAD cache files are skipped
- Manual cleanup may be needed for persistent errors

**GET Cache Expiration**:
- Active expiration adds overhead to validation scan
- Multi-instance safety checks may skip entries in active use
- Consider lazy expiration for most use cases

## Part Caching

### Overview

The proxy caches multipart object parts with exact byte range tracking. Each part's byte range is stored in `ObjectMetadata.part_ranges`, enabling accurate cache lookups for objects with variable-sized parts.

### Part Range Storage

When a GET request with `partNumber` parameter returns from S3, the proxy:

1. Parses the `Content-Range` header (e.g., `bytes 0-8388607/5368709120`)
2. Stores the exact `(start, end)` byte range in `part_ranges` for that part number
3. Updates `parts_count` from the `x-amz-mp-parts-count` header if present
4. Saves the updated metadata to disk

Subsequent requests for the same part number use the stored range for cache lookup.

### CompleteMultipartUpload Handling

During multipart upload completion:

1. The proxy parses the `CompleteMultipartUpload` XML request body
2. Extracts the list of requested parts with their ETags
3. Validates cached part ETags against request ETags (normalized, quotes removed)
4. Builds `part_ranges` from filtered parts with cumulative byte offsets
5. Deletes unreferenced cached parts (parts not in the completion request)

ETag mismatches cause cache finalization to be skipped (request still forwarded to S3).

### Configuration

Part caching is automatic and requires no configuration. Part ranges are stored in the standard metadata format.

## Download Coordination

### Overview

Download coordination (coalescing) prevents redundant S3 fetches when multiple concurrent requests arrive for the same uncached resource. Only one request fetches from S3 while others wait, then all serve from cache.

### How It Works

```
Request 1 (cache miss) → Registers as Fetcher → Fetches from S3 → Caches → Completes
Request 2 (cache miss) → Registers as Waiter → Waits for Fetcher → Serves from cache
Request 3 (cache miss) → Registers as Waiter → Waits for Fetcher → Serves from cache
```

The `InFlightTracker` uses a `DashMap` to track in-flight fetches by key. The first request for a key becomes the Fetcher; subsequent requests become Waiters.

### Scope

Coalescing covers three request types, each with independent flight keys:

| Request Type | Flight Key Format | Example |
|---|---|---|
| Full object GET | `"{cache_key}"` | `"my-bucket/path/to/file.txt"` |
| Range request | `"{cache_key}:{start}-{end}"` | `"my-bucket/path/to/file.txt:0-8388607"` |
| Part number request | `"{cache_key}:part{N}"` | `"my-bucket/path/to/file.txt:part2"` |

Different request types for the same object proceed independently — a full-object GET does not block a range request for the same object.

### Waiter Behavior

Waiters wait up to `wait_timeout_secs` for the Fetcher to complete:

- **Fetcher succeeds**: Waiters try the cache first (the fetcher should have cached the data). If the cache hit succeeds, the response is served directly from disk or RAM — no S3 request. If the cache lookup misses (e.g., metadata not yet consolidated), the waiter falls back to its own S3 fetch.
- **Fetcher fails**: Waiters fall back to their own S3 fetch
- **Timeout**: Waiters fall back to their own S3 fetch

In testing with 100 concurrent clients, the cache-first waiter path reduced S3 data transfer by 88% compared to the naive approach of always re-fetching from S3 (6.3 GB from S3 vs 53.8 GB without the optimization, serving 137 GB to clients).

### Configuration

```yaml
cache:
  download_coordination:
    enabled: true           # Enable/disable coalescing (default: true)
    wait_timeout_secs: 30   # Waiter timeout in seconds (default: 30, range: 5-120)
```

### Metrics

Download coordination exposes metrics via `/metrics`:

| Metric | Description |
|--------|-------------|
| `waits_total` | Total waiter registrations |
| `cache_hits_after_wait_total` | Waiters that served from cache |
| `timeouts_total` | Waiters that timed out |
| `s3_fetches_saved_total` | S3 fetches avoided by coalescing |
| `average_wait_duration_ms` | Average waiter wait time |

### When to Disable

Disable download coordination if:
- Single-instance deployment with no concurrent duplicate requests
- Workload has unique requests (no duplicates)
- Debugging cache behavior

## Cache Eviction

### Overview

The proxy automatically evicts cached ranges when disk usage approaches the configured `max_cache_size`. Eviction uses a range-based approach where each cached range is an independent eviction candidate, allowing fine-grained space reclamation.

### Eviction Triggers

Eviction is triggered in three scenarios:

1. **Before caching new data**: When storing a new range, the proxy checks if adding the data would exceed 95% capacity
2. **On startup**: If the cache exceeds `max_cache_size` during initialization, eviction runs immediately
3. **Background monitoring**: Periodic capacity checks trigger eviction when needed

### Thresholds

- **Trigger threshold**: 95% of `max_cache_size`
- **Target threshold**: 80% of `max_cache_size`

When eviction triggers, it continues until cache usage drops to 80% or below, freeing at least 15% of capacity.

### Eviction Algorithm

The proxy supports three eviction algorithms configured via `cache.eviction_algorithm`:

- **LRU** (Least Recently Used): Evicts ranges with oldest access time
- **LFU** (Least Frequently Used): Evicts ranges with lowest access count
- **TinyLFU** (default): Combines recency and frequency for optimal hit rate

### Range-Based Eviction

Unlike object-level eviction, the proxy evicts individual ranges:

- Each range (e.g., bytes 0-8MB of a 100MB file) is an independent candidate
- Partial objects remain cached after eviction (other ranges preserved)
- Ranges are grouped by object for efficient batch deletion
- When all ranges of an object are evicted, the metadata file is also deleted

### Multi-Instance Coordination

In shared storage deployments (`shared_storage.enabled: true`), eviction uses distributed locking:

- Global eviction lock prevents concurrent eviction across instances
- Only one instance performs eviction at a time
- Other instances skip eviction if lock is held
- Lock is released after eviction completes

### Configuration

```yaml
cache:
  max_cache_size: 10737418240        # 10GB - eviction trigger based on this
  eviction_algorithm: "TinyLFU"      # LRU, LFU, or TinyLFU
```

### Logging

Eviction logs at INFO level for summary information:

```
[DISK_CACHE_EVICTION] Starting eviction: usage=142.5 MiB / 150.0 MiB (95.0%), target=120.0 MiB (80%), to_free=22.5 MiB
[DISK_CACHE_EVICTION] Eviction completed: keys_evicted=3, ranges_evicted=47, freed=24.0 MiB, new_usage=118.5 MiB / 150.0 MiB (79.0%)
```

Per-range and per-object deletion details are logged at DEBUG level.

## See Also

- [Connection Pooling](CONNECTION_POOLING.md) - S3 connection optimization
- [Docker Deployment](README-Docker.md) - Container deployment guide
- [OTLP Metrics](OTLP_METRICS.md) - Observability and monitoring
