# Configuration Reference

Complete configuration guide for S3 Proxy including cache behavior, TTL management, and performance tuning.

## Table of Contents

- [Configuration Methods](#configuration-methods)
- [Server Configuration](#server-configuration)
- [Cache Configuration](#cache-configuration)
- [Time-To-Live (TTL) Configuration](#time-to-live-ttl-configuration)
- [Write Cache Configuration](#write-cache-configuration)
- [Bucket-Level Cache Settings](#bucket-level-cache-settings)
- [Cache Expiration Scenarios](#cache-expiration-scenarios)
- [RAM-Disk Cache Coherency](#ram-disk-cache-coherency)
- [Range Request Optimization](#range-request-optimization)
- [Eviction Configuration](#eviction-configuration)
- [Multi-Instance Coordination](#multi-instance-coordination)
- [Cache Size Tracking Configuration](#cache-size-tracking-configuration)
- [Compression Configuration](#compression-configuration)
- [Connection Pooling](#connection-pooling)
- [Logging Configuration](#logging-configuration)
- [Metrics Configuration](#metrics-configuration)
- [Dashboard Configuration](#dashboard-configuration)
- [Health Check Configuration](#health-check-configuration)
- [HTTPS Passthrough](#https-passthrough)
- [Duration Format](#duration-format)
- [Path Expansion](#path-expansion)
- [Environment Variable Reference](#environment-variable-reference)
- [Example Configurations](#example-configurations)
- [Troubleshooting](#troubleshooting)
- [See Also](#see-also)

---

## Configuration Methods

S3 Proxy supports three-layer configuration with precedence:

1. **YAML file** (base configuration)
2. **Environment variables** (override YAML)
3. **Command-line arguments** (highest priority)

### Loading Configuration

```bash
# Use YAML file
./s3-proxy -c config.yaml

# Override with environment variables
HTTP_PORT=8081 ./s3-proxy -c config.yaml

# Override with CLI arguments
./s3-proxy -c config.yaml --http-port 8081
```

## Server Configuration

### Ports and Protocol

```yaml
server:
  http_port: 80              # HTTP proxy port (caching enabled)
  https_port: 443            # HTTPS proxy port
  https_mode: "passthrough"  # Only option: "passthrough" (TCP passthrough)
  max_concurrent_requests: 200
  request_timeout: "30s"
```

**HTTP Port (80)**
- Full caching enabled (GET/HEAD requests)
- Range request optimization
- Write-through caching (enabled by default)
- Requires sudo for port < 1024

**HTTPS Port (443)**
- **Passthrough mode**: TCP tunneling, no caching (only mode)

**Max Concurrent Requests**
- Small deployments (< 50 users): 50-100
- Medium deployments (50-500 users): 100-300
- Large deployments (500+ users): 300-1000+
- High-throughput scenarios: 1000+

### Proxy Identification

```yaml
server:
  add_referer_header: true   # Add Referer header to forwarded requests (default: true)
```

When enabled, the proxy adds a `Referer` header to requests forwarded to S3 in the format `s3-hybrid-cache/{version} ({hostname})`. This header appears in S3 Server Access Logs, enabling usage tracking and per-instance debugging.

The header is only added when:
- The request does not already contain a `Referer` header
- The `Referer` header is not included in the SigV4 `SignedHeaders` (to preserve signature validity)

**Querying S3 Server Access Logs**:

The `Referer` header in S3 Server Access Logs enables:

1. **Identify which proxy instance served a request**: Each instance includes its hostname in the header (e.g., `s3-hybrid-cache/0.8.0 (proxy-instance-1)`)
2. **Determine if requests went through the cache**: Requests with the proxy's `Referer` header were served via HTTP (port 80) where caching occurs. Requests without this header were served via HTTPS passthrough (port 443) with no caching.
3. **Track proxy version distribution**: Identify which proxy versions are active across your fleet
4. **Debug routing issues**: Verify traffic is flowing through the expected proxy instances

Example S3 Server Access Log entry showing the `Referer` field:
```
bucket-name [01/Jan/2024:12:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT file.txt "GET /file.txt HTTP/1.1" 200 - 1024 1024 10 9 "s3-hybrid-cache/0.8.0 (proxy-instance-1)" "aws-cli/2.x" -
```

Query examples using AWS Athena:
```sql
-- Count requests by proxy instance
SELECT referer, COUNT(*) as request_count
FROM s3_access_logs
WHERE referer LIKE 's3-hybrid-cache/%'
GROUP BY referer
ORDER BY request_count DESC;

-- Identify requests that bypassed the cache (HTTPS passthrough)
SELECT *
FROM s3_access_logs
WHERE referer IS NULL OR referer NOT LIKE 's3-hybrid-cache/%';
```

### Environment Variables

- `HTTP_PORT` - Override HTTP port
- `HTTPS_PORT` - Override HTTPS port
- `HTTPS_MODE` - Override HTTPS mode
- `MAX_CONCURRENT_REQUESTS` - Override request limit

## Cache Configuration

### Basic Settings

```yaml
cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 10737418240     # 10GB in bytes
  ram_cache_enabled: true
  max_ram_cache_size: 268435456   # 256MB in bytes
  eviction_algorithm: "tinylfu"   # Options: lru, tinylfu
```

**Cache Directory**: See [CACHING.md](CACHING.md) for detailed directory structure

**Eviction Algorithms**

- **LRU** (Least Recently Used): Evicts oldest accessed entries
- **TinyLFU**: Frequency-recency hybrid (simplified implementation, not full TinyLFU algorithm)

### Environment Variables

- `CACHE_DIR` - Override cache directory
- `MAX_CACHE_SIZE` - Override max cache size
- `RAM_CACHE_ENABLED` - Enable/disable RAM cache
- `MAX_RAM_CACHE_SIZE` - Override RAM cache size

## Time-To-Live (TTL) Configuration

### TTL Types

S3 Proxy uses three independent TTL values:

```yaml
cache:
  get_ttl: "315360000s"           # ~10 years (cache forever)
  put_ttl: "3600s"                # 1 hour
  head_ttl: "60s"                 # 1 minute
  actively_remove_cached_data: false
```

### GET TTL

**Purpose**: Controls how long cached **data** remains valid for GET requests

**Default**: `315360000s` (~10 years, effectively infinite)

**Behavior**:
- Matches S3's immutable object model
- Objects don't change unless explicitly overwritten
- Expiration checked lazily on access (unless `actively_remove_cached_data: true`)

**When GET_TTL expires**:
- **Lazy mode** (default): Entry remains on disk until next GET, then fresh data fetched
- **Active mode**: Background process removes expired entries

**Example configurations**:
```yaml
# Cache forever (default, recommended for immutable data)
get_ttl: "315360000s"

# Cache for 24 hours (frequently changing data)
get_ttl: "86400s"

# Cache for 1 hour (very dynamic data)
get_ttl: "3600s"
```

**Why long GET_TTL is relatively safe**: The AWS CLI and SDKs using the Common Runtime (CRT) perform a HeadObject request before downloading an object (to determine content length for parallel ranged GETs). The proxy's HEAD_TTL (default: 60s) ensures this HEAD request revalidates against S3 frequently. If the object has changed, the HEAD response returns updated metadata, and the subsequent GET fetches fresh data. This means even with a very long GET_TTL, objects are effectively revalidated on the HEAD_TTL schedule for clients using CRT-based transfers.

### PUT TTL

**Purpose**: Controls how long write-through cached objects remain valid after PUT

**Default**: `3600s` (1 hour)

**Behavior**:
- Objects cached during PUT operations start with PUT_TTL
- Shorter than GET_TTL because objects may be written but never read
- **If GET request accesses object within PUT_TTL**, TTL is refreshed (metadata-only update)
- Optimizes for "upload once, download many times" patterns
- Write-cached objects are stored as ranges on disk only (not in RAM cache)

**Example flow**:
1. Client PUTs object → Cached as range 0-N with PUT_TTL (1 hour)
2. Client GETs object 30 min later → Served from cache, TTL refreshed
3. Object remains cached with refreshed TTL
4. Client requests byte range → Served directly from cache without S3 fetch
5. If never read within TTL → Expires and removed

**Example configurations**:
```yaml
# Short TTL for rarely-read uploads
put_ttl: "1800s"  # 30 minutes

# Longer TTL for frequently-read uploads
put_ttl: "7200s"  # 2 hours
```

### Incomplete Upload TTL

**Purpose**: Controls how long incomplete multipart uploads remain before automatic cleanup

**Default**: `1d` (1 day)

**Behavior**:
- Multipart uploads that are not completed within this TTL are automatically evicted
- Prevents abandoned uploads from consuming cache space indefinitely
- Cleanup runs at startup and periodically during operation
- Uses file modification time (not creation time) to detect recent activity

**Valid range**: 1 hour (`1h`) to 7 days (`7d`)

**Example configurations**:
```yaml
# Short TTL for fast cleanup of abandoned uploads
incomplete_upload_ttl: "1h"  # 1 hour

# Longer TTL for uploads that may take time to complete
incomplete_upload_ttl: "3d"  # 3 days
```

### HEAD TTL

**Purpose**: Controls how long cached **HEAD metadata** remains valid

**Default**: `60s` (1 minute)

**Behavior**:
- HEAD_TTL only affects HEAD requests
- HEAD_TTL expiration does NOT trigger validation on GET requests
- HEAD metadata is always actively removed when HEAD_TTL expires
- GET and HEAD operations use separate TTLs and cache entries

**Important**: HEAD_TTL and GET_TTL are completely independent. A GET request with valid GET_TTL will serve cached data regardless of HEAD_TTL status.

**Example configurations**:
```yaml
# Frequent metadata validation
head_ttl: "30s"

# Less frequent validation
head_ttl: "300s"  # 5 minutes
```

### Active vs Lazy Expiration

```yaml
cache:
  actively_remove_cached_data: false  # Default: lazy expiration
```

**Lazy Expiration (false, recommended)**:
- Expired entries remain on disk until accessed
- On GET request, if GET_TTL expired, fetch fresh data
- Saves CPU/IO from background cleanup
- Recommended for most use cases

**Active Expiration (true)**:
- Background process periodically removes expired entries
- Frees disk space immediately when GET_TTL expires
- Useful when disk capacity is elastic and not fixed
- Adds background CPU/IO overhead

**HEAD Metadata**: Always actively removed regardless of this setting

## Write Cache Configuration

Write-through caching stores PUT operations and multipart uploads in the cache so subsequent GET requests can be served immediately without fetching from S3.

### Basic Settings

```yaml
cache:
  write_cache_enabled: true          # Enable write-through caching (enabled by default)
  write_cache_percent: 10.0           # Percentage of disk cache for writes (1-50%)
  write_cache_max_object_size: 268435456  # 256MB max object size
  put_ttl: "1d"                       # Write cache TTL (refreshed on read)
  incomplete_upload_ttl: "1d"         # Incomplete multipart upload TTL
```

### How Write Caching Works

**Full PUT Operations**:
- Object data stored as a single range (0 to content-length-1)
- Write cache TTL set (default: 1 day)
- S3 response returned to client unchanged

**Multipart Uploads**:
- CreateMultipartUpload: Tracking metadata created with uploadId
- UploadPart: Each part stored as a range file with part number
- CompleteMultipartUpload: Parts assembled with final byte offsets, object metadata created
- AbortMultipartUpload: All cached parts and tracking metadata deleted

**TTL Refresh on Read**:
- When a write-cached object is accessed via GET, the TTL is refreshed
- This keeps frequently accessed objects in cache longer
- Objects never read expire after the initial TTL

### Capacity Management

Write cache is limited to a percentage of total disk cache to prevent starving read cache:

```yaml
cache:
  write_cache_percent: 10.0  # Default: 10% of max_cache_size
```

**Eviction behavior**:
- When write cache is full, oldest write-cached objects are evicted first
- Uses the same eviction algorithm as read cache (LRU or TinyLFU)
- If eviction cannot free enough space, the PUT bypasses caching

### Incomplete Upload Cleanup

Multipart uploads that are never completed are automatically cleaned up:

```yaml
cache:
  incomplete_upload_ttl: "1d"  # Default: 1 day
```

**Cleanup behavior**:
- Runs at startup and periodically during operation
- Uses file modification time to detect recent activity
- Uploads with recent UploadPart activity are not evicted
- AbortMultipartUpload immediately removes all cached parts

### Storage Location

**Benefits**:
- Range requests work immediately on write-cached objects
- No data copying when TTL is refreshed
- Unified eviction across read and write cache

### Shared Cache Considerations

When multiple proxy instances share a cache volume:

- Any instance can handle CreateMultipartUpload, UploadPart, or CompleteMultipartUpload
- File locks prevent concurrent modifications to tracking metadata
- Incomplete upload scanner uses distributed eviction lock
- File modification time (from shared filesystem) used for TTL checks

### Example Configuration

```yaml
cache:
  # Enable write caching
  write_cache_enabled: true
  
  # Reserve 20% of cache for writes
  write_cache_percent: 20.0
  
  # Cache objects up to 512MB
  write_cache_max_object_size: 536870912
  
  # Keep write-cached objects for 1 day (refreshed on read)
  put_ttl: "1d"
  
  # Clean up incomplete uploads after 6 hours
  incomplete_upload_ttl: "6h"
```

## Bucket-Level Cache Settings

Per-bucket and per-prefix cache configuration via JSON files stored at `cache_dir/metadata/{bucket}/_settings.json`. Overrides global cache settings without modifying the YAML config or restarting the proxy.

### Global Config Fields

```yaml
cache:
  # Enable/disable read caching for GET responses globally (default: true)
  # When false, no buckets are read-cached unless explicitly enabled via bucket _settings.json.
  # This allows an "allowlist" pattern where only specific buckets have caching enabled.
  read_cache_enabled: true

  # How long cached bucket settings are considered fresh before re-reading from disk (default: 60s)
  # Controls lazy reload — settings changes take effect after this threshold expires.
  bucket_settings_staleness_threshold: "60s"
```

### Settings File Format

Place a `_settings.json` file at `cache_dir/metadata/{bucket}/_settings.json`. All fields are optional — omitted fields fall back to global config.

```json
{
  "$schema": "../docs/bucket-settings-schema.json",
  "get_ttl": "5m",
  "head_ttl": "30s",
  "put_ttl": "1h",
  "read_cache_enabled": true,
  "write_cache_enabled": true,
  "compression_enabled": false,
  "ram_cache_eligible": true,
  "prefix_overrides": [
    {
      "prefix": "/temp/",
      "get_ttl": "0s",
      "ram_cache_eligible": false
    },
    {
      "prefix": "/static/assets/",
      "get_ttl": "7d",
      "compression_enabled": true
    }
  ]
}
```

**Supported fields**: `get_ttl`, `head_ttl`, `put_ttl`, `read_cache_enabled`, `write_cache_enabled`, `compression_enabled`, `ram_cache_eligible`, `prefix_overrides`

**Duration format**: Same as global config — `"0s"`, `"30s"`, `"5m"`, `"1h"`, `"7d"`

**Schema validation**: Include `$schema` for IDE autocompletion. Full schema at `docs/bucket-settings-schema.json`.

### Settings Cascade

Settings resolve with this precedence (highest to lowest):

1. **Prefix override** — longest matching prefix in `prefix_overrides`
2. **Bucket settings** — fields in `_settings.json`
3. **Global config** — YAML configuration values

Each field resolves independently. A prefix override for `get_ttl` does not affect `compression_enabled` resolution — that field still falls through to bucket or global level.

### Lazy Reload

Bucket settings are cached in RAM and re-read from disk when the staleness threshold expires (default: 60 seconds). Modify `_settings.json` on disk and changes take effect on the next request after the threshold. No proxy restart required.

If a settings file becomes invalid after a reload, the proxy logs an error and continues using the previously valid settings.

### Settings Apply at Cache-Write Time

Bucket and prefix settings take effect when data is written to cache, not retroactively. Objects cached before a settings change retain their original TTL until they expire naturally or are evicted. For example, changing `get_ttl` from `1h` to `5m` does not shorten the TTL of already-cached objects — only newly cached objects use the updated value.

### Validation

- Negative TTL values are rejected
- Empty or invalid prefixes in `prefix_overrides` are rejected
- On validation failure, the proxy uses the previously valid settings (or global defaults if no previous valid settings exist)

See [CACHING.md](CACHING.md#bucket-level-cache-settings) for detailed behavior of each setting.

## Cache Expiration Scenarios

### Scenario 1: Fresh Cache (GET_TTL Valid)

```
Client GET → Proxy checks cache → GET_TTL valid → Serve from cache
```

No S3 request needed. HEAD_TTL status is irrelevant.

### Scenario 2: GET_TTL Expired

```
Client GET → Proxy checks cache → GET_TTL expired
          → Forward GET to S3
          → S3 returns 200 OK with new data
          → Update cache with new GET_TTL
          → Serve fresh data
```

Full cache refresh. All ranges for this object expire together.

### Scenario 3: PUT-Cached Object Accessed via GET

**Note**: This scenario applies when write-through caching is enabled (enabled by default).

```
Client GET → Proxy checks cache → Found with PUT_TTL
          → Serve from cache
          → Transition TTL from PUT_TTL to GET_TTL (metadata-only)
```

Optimizes for "upload once, download many" pattern.

### Scenario 4: HEAD Request

```
Client HEAD → Proxy checks HEAD cache → HEAD_TTL valid → Serve cached headers
Client HEAD → Proxy checks HEAD cache → HEAD_TTL expired
           → Forward HEAD to S3
           → Cache headers with new HEAD_TTL
           → Serve headers
```

HEAD metadata is actively removed when HEAD_TTL expires.

## RAM-Disk Cache Coherency

When both RAM and disk caches are enabled, the proxy maintains coherency:

```yaml
cache:
  # Batch flush settings
  ram_cache_flush_interval: "60s"      # Time between flushes
  ram_cache_flush_threshold: 100       # Pending updates before flush
  ram_cache_flush_on_eviction: false   # Flush on RAM eviction
  
  # Verification settings
  ram_cache_verification_interval: "1s"  # Min time between verifications
```

### How It Works

**Verification**: RAM cache entries periodically verified against disk metadata
- Compares ETag, size, compression status
- Throttled to avoid excessive disk I/O (default: 1 second interval)
- Invalidates RAM entry if mismatch or disk missing

**Access Statistics Propagation**: RAM cache hits batched and written to disk
- Disk eviction algorithms need accurate access statistics
- Without propagation, hot RAM data appears "cold" on disk
- Batch processing minimizes disk I/O overhead

### Configuration Guidance

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| flush_interval | More disk I/O, fresher stats | Less I/O, staler stats | 60s for most workloads |
| flush_threshold | More frequent flushes | Larger batches | 100 for balanced performance |
| verification_interval | More disk reads, fresher data | Less I/O, risk of stale data | 1s for consistency |

### Monitoring Metrics

```json
{
  "batch_flush": {
    "pending_disk_updates": 45,
    "batch_flush_count": 12,
    "batch_flush_avg_duration_ms": 23.5
  },
  "ram_verification": {
    "ram_verification_checks": 1250,
    "ram_verification_invalidations": 3,
    "ram_verification_avg_duration_ms": 2.1
  }
}
```

## Cache Hit Performance Tuning

### Full-Object Check Threshold

```yaml
cache:
  full_object_check_threshold: 67108864  # 64 MiB (default)
```

Range requests check if the full object is cached before falling back to range-specific lookup. For large files (many cached ranges), this scan is expensive. When `content_length` exceeds this threshold, the full-object check is skipped and the proxy proceeds directly to range-specific lookup.

Set lower for workloads with many large files cached as individual ranges. Set higher (or to 0 to disable) if full-object caching of large files is common.

### Disk Streaming Threshold

```yaml
cache:
  disk_streaming_threshold: 1048576  # 1 MiB (default)
```

Cached ranges at or above this size are streamed from disk in 512 KiB chunks instead of loaded fully into memory. Reduces memory usage under high concurrency. RAM cache hits are always served from memory regardless of this setting.

Set lower for memory-constrained environments with many concurrent large-range requests. Set higher if memory is abundant and you prefer simpler response handling.

### Consolidation Cycle Timeout

```yaml
cache:
  shared_storage:
    consolidation_cycle_timeout: "30s"  # Default: 30 seconds
```

Maximum duration for the per-key processing phase of a consolidation cycle. Prevents one slow NFS operation from blocking the next cycle. Keys not processed due to timeout are retried in the next cycle. Size tracking (accumulator delta collection) runs before per-key processing and is unaffected by this timeout.

## Range Request Optimization

### Range Merging

```yaml
cache:
  range_merge_gap_threshold: 262144  # 256KB
```

**Purpose**: Consolidate missing ranges to minimize S3 requests

**How it works**:
- Cached ranges: 0-8MB, 16-24MB, 32-40MB
- Client requests: 0-40MB
- Missing ranges: 8-16MB, 24-32MB
- Gap between missing ranges: 0 bytes (contiguous)
- **Action**: Fetch 8-16MB and 24-32MB separately (2 requests)
- **Result**: 60% cache efficiency (24MB from cache, 16MB from S3)

**Tuning guide**:
- **Smaller threshold** (64KB): More granular fetching, less wasted bandwidth
- **Larger threshold** (1MB): Fewer S3 requests, more wasted bandwidth on gaps
- **Default 256KB**: Good balance for most workloads

**Considerations**:
- Network latency vs bandwidth: Higher latency favors larger threshold
- S3 request costs: Each request has overhead (~50-100ms)
- Bandwidth costs: Larger threshold may fetch already-cached data

## Eviction Configuration

### Eviction Buffer

```yaml
cache:
  eviction_buffer_percent: 5  # Default: 5%
```

**Purpose**: Create buffer to minimize eviction frequency

**How it works**:
- Eviction triggers at 95% capacity (100% - 5%)
- Eviction target is 90% capacity (100% - 5% - 5%)
- Frees 5% buffer before next eviction needed

**Example** (10GB cache, 5% buffer):
- Eviction triggers at 9.5GB (95%)
- Eviction target is 9GB (90%)
- Frees 500MB buffer

**Tuning guide**:
- **Smaller buffer** (2-3%): Less wasted space, more frequent evictions
- **Larger buffer** (10-15%): More wasted space, fewer evictions
- **Default 5%**: Good balance for most workloads

### Metadata Lock Timeout

```yaml
cache:
  metadata_lock_timeout_seconds: 60  # Default: 60 seconds
```

**Purpose**: Prevent deadlocks from crashed processes holding locks

**Tuning guide**:
- **Shorter timeout** (30s): Faster crash recovery, risk of breaking active locks
- **Longer timeout** (120s): More protection against false breaks, slower recovery
- **Default 60s**: Good balance for most workloads

**Considerations**:
- Normal metadata operations complete in milliseconds
- Only crashes or hung processes should trigger timeout
- Monitor `metadata_lock_timeouts` metric to tune

## Multi-Instance Coordination

For scale-out deployments with shared cache storage:

```yaml
cache:
  shared_storage:
    lock_timeout: "60s"
    lock_refresh_interval: "30s"
    consolidation_interval: "5s"
    validation_frequency: "24h"
    validation_threshold_warn: 5.0
    validation_threshold_error: 20.0
    eviction_lock_timeout: "60s"
    lock_max_retries: 3
    recovery_max_concurrent: 10
```

**Note**: Journal-based metadata writes and distributed eviction locking are always enabled for consistency across all deployment modes. There is no `enabled` flag - these features are always active.

### NFS Mount Requirements

**CRITICAL**: For reliable multi-instance cache coordination, NFS volumes MUST be mounted with `lookupcache=pos`.

**Why this is required**: NFS clients cache directory entry lookups by default. Without `lookupcache=pos`, instances cache "file not found" results and don't see files created by other instances. This causes 40%+ cache miss rate on repeat downloads because one instance caches data but other instances can't find it.

**Example /etc/fstab entry**:
```
nfs-server.example.com:/export/cache /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,_netdev 0 0
```

**Mount options explained**:
| Option | Purpose |
|--------|---------|
| `lookupcache=pos` | **Required** - Caches positive lookups (file exists) but not negative lookups (file not found), so new files from other instances are visible immediately |
| `nfsvers=4.1` | Use NFSv4.1 for better locking support |
| `hard` | Retry NFS requests indefinitely (recommended for data integrity) |
| `_netdev` | Wait for network before mounting |

**Key features**:
- **Atomic metadata writes**: Journal-based updates prevent corruption
- **Distributed eviction**: Only one instance evicts at a time
- **File locking**: Prevents concurrent access conflicts
- **Cache validation**: Cross-instance consistency checks
- **Orphaned range recovery**: Cleanup of incomplete operations

**Configuration guidelines**:

| Setting | Purpose | Recommendations |
|---------|---------|-----------------|
| `lock_timeout` | Max wait for file locks | Small cache: 60s, Large cache: 300-600s |
| `consolidation_interval` | Journal flush frequency | 5s (default), reduce for faster consistency |
| `validation_frequency` | Consistency check interval | 24h (default), increase for large deployments |
| `eviction_lock_timeout` | Distributed eviction timeout | Match lock_timeout for consistency |

## Cache Size Tracking Configuration

### Download Coordination

Controls request coalescing for concurrent cache misses:

```yaml
cache:
  download_coordination:
    enabled: true           # Enable/disable coalescing (default: true)
    wait_timeout_secs: 30   # Waiter timeout in seconds (default: 30, range: 5-120)
```

When multiple requests arrive for the same uncached resource (full object, byte range, or part number), only one request fetches from S3 while others wait. Waiters serve from cache after the fetcher completes, or fall back to their own S3 fetch on timeout or error.

Disable for single-instance deployments with no concurrent duplicate requests, or when debugging cache behavior.

See [CACHING.md - Download Coordination](CACHING.md#download-coordination) for details.

## Cache Size Tracking

Cache size tracking uses an accumulator-based approach. Each proxy instance maintains an in-memory SizeAccumulator (AtomicI64) that tracks size changes at write and eviction time, then flushes to per-instance delta files.

**How It Works**:
- Each instance tracks size changes in an in-memory AtomicI64 accumulator updated at write/eviction time
- The accumulator periodically flushes to per-instance delta files in the shared cache directory
- The consolidator collects per-instance delta files every 5 seconds (configurable via `shared_storage.consolidation_interval`) and sums them into `size_tracking/size_state.json`
- Eviction is triggered automatically when cache exceeds capacity

**Configuration**:
```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"     # How often to consolidate and update size
```

**Deprecated Options** (removed):
- `size_tracking_flush_interval` - replaced by `shared_storage.consolidation_interval`
- `size_tracking_buffer_size` - no longer needed, accumulator tracks size changes in memory

**Size State File**:
- Location: `{cache_dir}/size_tracking/size_state.json`
- Contains: total_size, write_cache_size, last_consolidation timestamp
- Updated atomically after each consolidation cycle
- Shared across all proxy instances

## Compression Configuration

```yaml
compression:
  enabled: true
  threshold: 4096                    # 4KB minimum
  preferred_algorithm: "lz4"         # Options: lz4
  content_aware: true                # Skip compressed formats
```

### Content-Aware Compression

When `content_aware: true`, these formats are automatically skipped:

**Images**: jpg, png, gif, webp, avif, heic, bmp, ico, svg
**Videos**: mp4, avi, mkv, mov, webm, flv, wmv, m4v
**Audio**: mp3, aac, ogg, flac, opus, m4a, wma, wav
**Archives**: zip, rar, 7z, gz, bz2, xz, tar, tgz
**Documents**: pdf, docx, xlsx, pptx

**Text files** (json, html, css, js, txt, etc.) are still compressed.

### Algorithm Metadata

Each cache entry stores which algorithm was used:
- Changing `preferred_algorithm` doesn't invalidate existing cache
- Old entries continue working with their original algorithm
- New entries use the preferred algorithm
- Optional gradual migration on cache access

## Connection Pooling

```yaml
connection_pool:
  max_connections_per_ip: 50
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "60s"
  
  # HTTP Connection Keepalive
  keepalive_enabled: true
  max_idle_per_host: 10
  max_lifetime: "300s"
  pool_check_interval: "10s"
  
  # Custom DNS servers (optional)
  # dns_servers: ["8.8.8.8", "1.1.1.1"]
```

### DNS Server Configuration

**Purpose**: Configure DNS servers for S3 endpoint resolution

The proxy bypasses `/etc/hosts` and uses external DNS servers to resolve S3 endpoints. This is critical because clients point S3 domains to the proxy via hosts file or local DNS zone, but the proxy must resolve S3 to real AWS IPs.

**Default**: Google DNS (8.8.8.8, 8.8.4.4) + Cloudflare DNS (1.1.1.1, 1.0.0.1)

**Custom DNS servers**:
```yaml
connection_pool:
  dns_servers: ["10.0.0.2", "10.0.0.3"]  # Corporate DNS
```

**Use cases**:
- Corporate environments with internal DNS
- S3 PrivateLink (interface VPC endpoints) — see below

### S3 PrivateLink (Interface VPC Endpoints)

When using S3 interface VPC endpoints (PrivateLink), the proxy must resolve S3 endpoints to the PrivateLink ENI IPs instead of public S3 IPs. The default external DNS servers (Google, Cloudflare) return public IPs, bypassing PrivateLink entirely.

Configure `dns_servers` to point at Route 53 Resolver inbound endpoints in the VPC. These return PrivateLink IPs when private DNS is enabled on the interface endpoint, and are reachable from on-premises over VPN or Direct Connect.

```yaml
connection_pool:
  dns_servers: ["10.0.1.50", "10.0.2.50"]  # Route 53 Resolver inbound endpoint IPs
```

The proxy cannot use your on-prem DNS server for this — on-prem DNS resolves S3 endpoints to the proxy's own IP, which would create a loop. Route 53 Resolver inbound endpoints provide a separate resolution path.

**Alternative: Static IP overrides**: If Route 53 Resolver inbound endpoints are not available, use `endpoint_overrides` to map S3 hostnames directly to PrivateLink ENI IPs, bypassing DNS entirely:

```yaml
connection_pool:
  endpoint_overrides:
    "s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]
```

The proxy load-balances across the listed IPs. This also works for the HTTPS passthrough handler (port 443), which otherwise uses its own hardcoded DNS.

See [Getting Started - S3 PrivateLink](GETTING_STARTED.md#s3-privatelink-interface-vpc-endpoints) for setup details and verification steps.

### Connection Keepalive

**Purpose**: Reuse TCP/TLS connections to eliminate handshake overhead

**Benefits**:
- 150-200ms latency reduction per request
- 50-100% throughput increase
- First request: Full handshake (~250-350ms)
- Subsequent requests: Reuse connection (~100-150ms)

**Tuning guide**:

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| max_idle_per_host | Less memory/FDs | More concurrent reuse | 10 (default), reduce for memory-constrained environments |
| max_lifetime | More frequent rotation | Less overhead | 300s (5 min) for stable endpoints |
| pool_check_interval | More responsive cleanup | Less CPU overhead | 10s for balanced performance |

**Traffic-based tuning**:
- Low traffic: 1-2 connections per IP
- Medium traffic: 2-5 connections per IP
- High traffic: 5-10 connections per IP

**Endpoint stability tuning**:
- Frequent DNS changes: 60-120s lifetime
- Stable endpoints: 300-600s lifetime
- Very stable endpoints: 600-3600s lifetime

### Disabling Keepalive

```yaml
connection_pool:
  keepalive_enabled: false
```

Creates new connections for every request. Useful for:
- Debugging connection issues
- Testing without connection reuse
- Specific deployment scenarios

## Logging Configuration

```yaml
logging:
  access_log_dir: "./tmp/logs/access"
  app_log_dir: "./tmp/logs/app"
  access_log_enabled: true
  access_log_mode: "all"             # Options: all, cached_only
  log_level: "info"                  # Options: error, warn, info, debug, trace
  
  # Buffered access log settings (reduces disk I/O on shared storage)
  access_log_flush_interval: "5s"    # How often to flush buffered entries
  access_log_buffer_size: 1000       # Max entries before forced flush
```

**Access Log Modes**:
- **all**: Log all requests (cache hits and misses)
- **cached_only**: Log only cache hits

**Log Levels**:
- **error**: Only errors
- **warn**: Warnings and errors
- **info**: General information (recommended)
- **debug**: Detailed debugging information
- **trace**: Very verbose tracing

**Log Locations**:
- Access logs: `{access_log_dir}/access.log`
- Application logs: `{app_log_dir}/{hostname}/s3-proxy.log.{date}`

### Buffered Access Logging

Access logs are buffered in RAM and flushed periodically to reduce disk I/O, especially important for shared NFS storage.

```yaml
logging:
  access_log_flush_interval: "5s"    # Default: 5 seconds
  access_log_buffer_size: 1000       # Default: 1000 entries
```

**Flush Triggers**:
- Time-based: Flush when `access_log_flush_interval` elapses since last flush
- Size-based: Flush when buffer reaches `access_log_buffer_size` entries
- Shutdown: Force flush during graceful shutdown

**Tuning Guide**:

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| `access_log_flush_interval` | More frequent writes, fresher logs | Less I/O, potential data loss on crash | 5s for most workloads |
| `access_log_buffer_size` | More frequent flushes | Larger memory usage | 1000 for balanced performance |

**Considerations**:
- On crash, unflushed entries are lost (up to buffer_size entries or flush_interval worth)
- Shared NFS storage benefits significantly from buffered writes
- Local SSD deployments can use smaller intervals (1-2s) for fresher logs

## Metrics Configuration

```yaml
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  collection_interval: "60s"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
```

### OTLP Export

```yaml
metrics:
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "60s"
    timeout: "10s"
    compression: "none"              # Options: none, gzip
    headers: {}
```

**Common endpoints**:
- CloudWatch Agent: `http://127.0.0.1:4318`
- Prometheus OTLP: `http://prometheus:9090/api/v1/otlp`
- OpenTelemetry Collector: `http://otel-collector:4318`

**Custom headers** (for authentication):
```yaml
otlp:
  headers:
    Authorization: "Bearer your-token"
    X-Custom-Header: "value"
```

## Dashboard Configuration

```yaml
dashboard:
  enabled: true                        # Enable dashboard server (default: true)
  port: 8081                          # Dashboard server port (default: 8081)
  bind_address: "0.0.0.0"             # Bind address (default: 0.0.0.0 for all interfaces)
  cache_stats_refresh_interval: "5s"   # Auto-refresh interval for cache statistics (default: 5s)
  logs_refresh_interval: "10s"         # Auto-refresh interval for application logs (default: 10s)
  max_log_entries: 100                 # Maximum number of log entries to display (default: 100)
```

### Dashboard Features

The dashboard provides a web-based interface for monitoring proxy status:

**Real-time Cache Statistics**:
- RAM and disk cache hit rates, miss rates, and current sizes
- Cache eviction counts and effectiveness percentages
- Human-readable size formatting (KB, MB, GB)
- Auto-refresh every 5 seconds (configurable)

**Application Log Viewer**:
- Recent log entries with timestamp, level, and message content
- Structured data formatting for key-value pairs
- Log level filtering (ERROR, WARN, INFO, DEBUG)
- Auto-refresh every 10 seconds (configurable)
- Adjustable entry limit (default: 100 entries)

**System Information**:
- Proxy instance hostname and version
- Uptime and basic system status
- Navigation between Cache Stats and Application Logs sections

### Configuration Options

**Basic Settings**:
```yaml
dashboard:
  enabled: true          # Set to false to disable dashboard completely
  port: 8081            # Change if port conflicts with other services
  bind_address: "0.0.0.0"  # Set to "127.0.0.1" for localhost-only access
```

**Refresh Intervals**:
```yaml
dashboard:
  cache_stats_refresh_interval: "5s"   # Valid range: 1-300 seconds
  logs_refresh_interval: "10s"         # Valid range: 1-300 seconds
```

**Log Display**:
```yaml
dashboard:
  max_log_entries: 100    # Valid range: 10-10000 entries
```

### Access and Security

**No Authentication Required**: The dashboard is designed as an internal monitoring tool and does not require authentication.

**Network Access Control**:
- Default: Accessible on all network interfaces (`0.0.0.0`)
- Localhost only: Set `bind_address: "127.0.0.1"`
- Firewall rules recommended for production deployments

**Access URL**: `http://your-proxy-host:8081`

### Performance Impact

**Resource Usage**:
- Minimal overhead when no users are connected
- Uses <10MB additional memory
- Supports up to 10 concurrent dashboard users
- Does not impact main proxy performance

**Optimization**:
- API responses generated on-demand without caching
- Static files embedded in binary (no external dependencies)
- Efficient log reading with streaming to avoid loading entire files

### Integration with Existing Components

**Cache Statistics**: Integrates with existing `MetricsManager` to provide:
- Real-time cache hit/miss statistics
- Cache size and eviction metrics
- Overall proxy effectiveness calculations

**Application Logs**: Reads from the same log files as the main proxy:
- Uses existing tracing framework configuration
- Parses structured log entries with key-value pairs
- Respects log level filtering and rotation

**Graceful Shutdown**: Integrates with existing shutdown coordinator:
- Closes dashboard connections gracefully during proxy shutdown
- No hanging connections or resource leaks

### Environment Variables

Dashboard configuration can be overridden via environment variables:

| Variable | Configuration Path | Example |
|----------|-------------------|---------|
| `DASHBOARD_ENABLED` | `dashboard.enabled` | `DASHBOARD_ENABLED=true` |
| `DASHBOARD_PORT` | `dashboard.port` | `DASHBOARD_PORT=8081` |
| `DASHBOARD_BIND_ADDRESS` | `dashboard.bind_address` | `DASHBOARD_BIND_ADDRESS=127.0.0.1` |

### Example Configurations

**Development** (localhost only):
```yaml
dashboard:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"
  cache_stats_refresh_interval: "2s"
  logs_refresh_interval: "5s"
  max_log_entries: 200
```

**Production** (all interfaces):
```yaml
dashboard:
  enabled: true
  port: 8081
  bind_address: "0.0.0.0"
  cache_stats_refresh_interval: "10s"
  logs_refresh_interval: "15s"
  max_log_entries: 100
```

**Disabled**:
```yaml
dashboard:
  enabled: false
```

### Troubleshooting

**Port Conflicts**:
- Change `port` if 8081 is already in use
- Check for conflicts with health check or metrics ports

**Access Issues**:
- Verify `bind_address` allows connections from your network
- Check firewall rules for the configured port
- Ensure proxy is running and dashboard is enabled

**Performance Issues**:
- Increase refresh intervals to reduce update frequency
- Reduce `max_log_entries` for faster log loading
- Monitor dashboard connection count (max 10 concurrent users)

## Health Check Configuration

```yaml
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "30s"
```

**Endpoints**:
- Health check: `http://localhost:8080/health`
- Metrics: `http://localhost:9090/metrics`
- Dashboard: `http://localhost:8081`

## HTTPS Passthrough

HTTPS requests are handled via TCP passthrough - no TLS termination or configuration required.

**How it works**:
- HTTPS traffic (port 443) is tunneled directly to S3
- No caching occurs for HTTPS requests
- No certificate management needed
- Transparent to clients

**Configuration**: HTTPS passthrough is automatically enabled when `https_port` is configured:

```yaml
server:
  https_port: 443  # Enable HTTPS passthrough on port 443
```

**Note**: Only HTTP requests (port 80) are cached. To ensure caching benefits:
- Set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` for AWS CLI (works for buckets in that region)
- Set `S3_ENDPOINT_URL=http://s3.<region>.amazonaws.com` for s5cmd
- For cross-region requests, use `--endpoint-url` with the bucket's region

## Duration Format

All duration values support human-readable strings:

```yaml
# Seconds
get_ttl: "30s"
get_ttl: "60sec"
get_ttl: "120seconds"

# Minutes
get_ttl: "5m"
get_ttl: "10min"
get_ttl: "30minutes"

# Hours
get_ttl: "1h"
get_ttl: "2hr"
get_ttl: "24hours"

# Days
put_ttl: "1d"
put_ttl: "7day"
put_ttl: "30days"

# Milliseconds
timeout: "500ms"
timeout: "1000millis"
```

## Path Expansion

Configuration paths support tilde expansion:

```yaml
cache:
  cache_dir: "~/cache"              # Expands to /home/user/cache
  
logging:
  access_log_dir: "~/logs/access"   # Expands to /home/user/logs/access
```

## Environment Variable Reference

All configuration options can be overridden via environment variables:

| Variable | Configuration Path | Example |
|----------|-------------------|---------|
| `HTTP_PORT` | `server.http_port` | `HTTP_PORT=8081` |
| `HTTPS_PORT` | `server.https_port` | `HTTPS_PORT=8443` |
| `HTTPS_MODE` | `server.https_mode` | `HTTPS_MODE=passthrough` |
| `CACHE_DIR` | `cache.cache_dir` | `CACHE_DIR=/var/cache/s3-proxy` |
| `MAX_CACHE_SIZE` | `cache.max_cache_size` | `MAX_CACHE_SIZE=10737418240` |
| `RAM_CACHE_ENABLED` | `cache.ram_cache_enabled` | `RAM_CACHE_ENABLED=true` |
| `CONSOLIDATION_INTERVAL` | `cache.shared_storage.consolidation_interval` | `CONSOLIDATION_INTERVAL=5s` |
| `ACCESS_LOG_DIR` | `logging.access_log_dir` | `ACCESS_LOG_DIR=/var/log/s3-proxy` |
| `APP_LOG_DIR` | `logging.app_log_dir` | `APP_LOG_DIR=/var/log/s3-proxy` |
| `ACCESS_LOG_FLUSH_INTERVAL` | `logging.access_log_flush_interval` | `ACCESS_LOG_FLUSH_INTERVAL=5s` |
| `ACCESS_LOG_BUFFER_SIZE` | `logging.access_log_buffer_size` | `ACCESS_LOG_BUFFER_SIZE=1000` |
| `DASHBOARD_ENABLED` | `dashboard.enabled` | `DASHBOARD_ENABLED=true` |
| `DASHBOARD_PORT` | `dashboard.port` | `DASHBOARD_PORT=8081` |
| `DASHBOARD_BIND_ADDRESS` | `dashboard.bind_address` | `DASHBOARD_BIND_ADDRESS=127.0.0.1` |
| `OTLP_ENDPOINT` | `metrics.otlp.endpoint` | `OTLP_ENDPOINT=http://localhost:4318` |
| `METRICS_ENABLED` | `metrics.enabled` | `METRICS_ENABLED=true` |

## Example Configurations

### Development

```yaml
server:
  http_port: 8081
  https_port: 8443

cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 1073741824        # 1GB
  ram_cache_enabled: true
  max_ram_cache_size: 134217728     # 128MB

logging:
  access_log_dir: "./tmp/logs/access"
  app_log_dir: "./tmp/logs/app"
  log_level: "debug"

dashboard:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"         # Localhost only for development
```

### Production

```yaml
server:
  http_port: 80
  https_port: 443
  max_concurrent_requests: 500

cache:
  cache_dir: "/var/cache/s3-proxy"
  max_cache_size: 107374182400      # 100GB
  ram_cache_enabled: true
  max_ram_cache_size: 1073741824    # 1GB
  eviction_algorithm: "tinylfu"
  
  shared_storage:
    eviction_lock_timeout: "300s"

logging:
  access_log_dir: "/var/log/s3-proxy/access"
  app_log_dir: "/var/log/s3-proxy/app"
  log_level: "info"

dashboard:
  enabled: true
  port: 8081
  bind_address: "0.0.0.0"           # All interfaces for production monitoring

metrics:
  otlp:
    enabled: true
    endpoint: "http://localhost:4318"
```

### High-Performance

```yaml
server:
  max_concurrent_requests: 1000

cache:
  max_cache_size: 1099511627776     # 1TB
  ram_cache_enabled: true
  max_ram_cache_size: 10737418240   # 10GB
  eviction_algorithm: "tinylfu"
  eviction_buffer_percent: 10

connection_pool:
  keepalive_enabled: true
  max_idle_per_host: 10
  max_lifetime: "600s"

compression:
  enabled: true
  threshold: 1024
  content_aware: true

dashboard:
  enabled: true
  port: 8081
  cache_stats_refresh_interval: "10s"  # Slower refresh for high-performance
  logs_refresh_interval: "15s"
```

## Troubleshooting

### High Memory Usage

- Reduce `max_ram_cache_size`
- Reduce `max_concurrent_requests`
- Reduce `max_idle_per_host`
- Disable dashboard if not needed: `dashboard.enabled: false`

### High Disk Usage

- Reduce `max_cache_size`
- Enable `actively_remove_cached_data: true`
- Reduce `get_ttl` for frequently changing data

### Poor Cache Hit Rate

- Increase `get_ttl`
- Check bucket settings overrides are correct
- Monitor `cache_hit_rate` metric
- Review access patterns

### Slow Performance

- Enable `keepalive_enabled: true`
- Increase `max_idle_per_host`
- Enable `ram_cache_enabled: true`
- Increase `max_ram_cache_size`
- Check `range_merge_gap_threshold`

### Connection Issues

- Increase `connection_timeout`
- Increase `max_connections_per_ip`
- Check `dns_refresh_interval`
- Monitor connection pool metrics

### Dashboard Issues

**Dashboard Not Accessible**:
- Verify `dashboard.enabled: true`
- Check `dashboard.port` for conflicts
- Verify `dashboard.bind_address` allows connections from your network
- Check firewall rules for the configured port

**Dashboard Performance Issues**:
- Increase `cache_stats_refresh_interval` and `logs_refresh_interval`
- Reduce `max_log_entries` for faster log loading
- Limit concurrent dashboard users (max 10 supported)

**Dashboard Port Conflicts**:
- Change `dashboard.port` if 8081 conflicts with health check or other services
- Ensure dashboard, health check, and metrics use different ports

## See Also

- [CACHING.md](CACHING.md) - Cache architecture details
- [COMPRESSION.md](COMPRESSION.md) - Compression algorithms
- [CONNECTION_POOLING.md](CONNECTION_POOLING.md) - Connection management
- [OTLP_METRICS.md](OTLP_METRICS.md) - Metrics and observability
- [config/config.example.yaml](../config/config.example.yaml) - Complete configuration with all options documented
