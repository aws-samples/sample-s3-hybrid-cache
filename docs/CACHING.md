# Caching Guide

What the proxy caches, what it does not, and how to control that per key. This is the
entry point; the depth lives in five companion documents.

| Document | Covers |
|---|---|
| [CACHE_INTERNALS.md](CACHE_INTERNALS.md) | On-disk and in-memory layout, sharding, cache keys, access tracking, journal and size-tracking internals |
| [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md) | TTL, revalidation, conditional requests |
| [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md) | How a read is satisfied: range merging, page widening, write-through, multipart, coherency checks, download coordination |
| [EVICTION.md](EVICTION.md) | Reclaiming space: algorithms, admission window |
| [SHARED_STORAGE.md](SHARED_STORAGE.md) | Running several proxies against one cache volume |

Field types, defaults, and valid ranges for every option named here are in
[CONFIGURATION.md](CONFIGURATION.md).

## Table of Contents

- [Overview](#overview)
- [Cache Rules](#cache-rules)
- [Cache Bypass for Non-Cacheable Operations](#cache-bypass-for-non-cacheable-operations)
- [Cache Bypass Headers](#cache-bypass-headers)
- [Versioned Request Handling](#versioned-request-handling)
- [Tuning Profiles](#tuning-profiles)
- [Cache Invalidation](#cache-invalidation)
- [Compression](#compression)
- [Cache Hit Rate](#cache-hit-rate)
- [Limitations](#limitations)
- [See Also](#see-also)

---

## Overview

Hybrid Cache for Amazon S3 provides intelligent caching to accelerate S3 access while maintaining data consistency. The proxy is a **transparent forwarder** - it only responds to client requests and cannot initiate requests to S3 (as it has no AWS credentials and cannot sign requests).

## Cache Rules

Cache rules override global cache settings for keys matching a glob pattern, configured in a single hot-reloadable file at `cache_dir/cache_rules.json`. Changes take effect without a proxy restart.

See [CONFIGURATION.md](CONFIGURATION.md#cache-rules) for the file format, glob syntax, first-match-per-field precedence, reload behavior, cache-key forms, and rule-count guidance. This section describes the runtime behavior of each setting a rule can apply.

> **Breaking change (v2.0.0).** Cache rules replace the per-bucket `cache_dir/metadata/{bucket}/_settings.json` mechanism. `_settings.json` files are no longer read, and there is no automatic migration. See [CHANGELOG.md](../CHANGELOG.md) for a before/after translation.

### Per-Key Cache Control

#### read_cache_enabled

Controls whether GET responses are cached on disk for keys a rule matches.

When `false`:
- GET response data is not written to disk cache
- Range data is not promoted to RAM cache
- Metadata is not stored in the RAM metadata cache
- Requests are forwarded to S3 and streamed directly to the client
- Write caching for PUT operations is independent — `write_cache_enabled` controls that separately

When `true` (default): Normal caching behavior.

Global default: `cache.read_cache_enabled` in YAML config (default: `true`). Set globally to `false` for an allowlist pattern where only keys matched by a rule with `"read_cache_enabled": true` are cached.

#### write_cache_enabled

Controls whether PUT operations are cached for keys a rule matches.

When `false`: PUT requests are forwarded to S3 but not cached locally.
When `true` (default from global config): Normal write-through caching.

Independent of `read_cache_enabled` — disabling read cache does not affect write caching.

#### ram_cache_eligible

Controls whether range data for keys a rule matches can be stored in RAM cache.

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

### Settings Are Re-Evaluated on Each Read

Resolved cache settings (TTL values, `read_cache_enabled`, `ram_cache_eligible`) are re-evaluated against the current rules on each read request. Freshness is determined by comparing `now - created_at` against the current resolved TTL (`get_ttl` for GET, `head_ttl` for HEAD), not by the stored `expires_at` / `head_expires_at` baked at write time. A rule change therefore applies to already-cached objects on the next GET/HEAD after the staleness window, without a restart or manual cache wipe.

Consequences:
- Changing `get_ttl` from `"3600s"` to `"0s"` takes effect on the next GET: the object is treated as expired and revalidated against S3 (conditional request; served from cache only on 304).
- Changing `get_ttl` from `"0s"` to `"3600s"` extends the freshness window for already-cached objects if `now - created_at` is within the new TTL.
- Changing `read_cache_enabled` from `true` to `false` eagerly deletes the cached entry (range files + metadata) on the first GET/HEAD after the rule takes effect, then forwards to S3. This is self-cleaning: no manual cache wipe needed.

### Read Cache Disabled Behavior

When `read_cache_enabled` is `false` (resolved from a matching rule or the global default), the proxy acts as a pure pass-through for GET and HEAD requests:

```
Client GET/HEAD → Proxy → S3 → Stream response directly to client (no caching)
```

- No disk cache reads or writes for new requests
- No RAM cache promotion or metadata storage
- PUT write caching is unaffected (controlled by `write_cache_enabled`)
- Existing cached data for the affected keys is eagerly deleted on the first GET/HEAD after the rule takes effect (range `.bin` files + `.meta` metadata, including HEAD `head_expires_at`)
- Bounded cost: at most one delete per key per instance; subsequent requests find nothing to delete
- In shared-storage mode, eager deletion writes one journal `Remove` entry (same as DELETE)
- Conditional requests (`evaluate_conditions_from_cache = true`) also honour `read_cache_enabled=false`: they do not answer from cache and trigger the same eager invalidation

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

**Operations with Customer-Provided Encryption Keys** (always bypass):
- Any GET, HEAD, or PUT carrying any of the SSE-C request headers:
  - `x-amz-server-side-encryption-customer-algorithm`
  - `x-amz-server-side-encryption-customer-key`
  - `x-amz-server-side-encryption-customer-key-md5`

  The proxy has no way to decrypt SSE-C data, and the cache key is path-only — caching plaintext obtained under one key and serving it under a different (or missing) key would leak data. SSE-C requests are forwarded to S3 verbatim, and S3 enforces key matching end-to-end. Existing non-SSE-C cache entries for a path remain; an SSE-C request to that same path goes straight to S3 and S3's response determines what the client sees.

**Part-Scoped HEAD** (always bypasses):
- **HeadObject with `partNumber`** — a `HEAD` naming a single part is forwarded to S3 with no cache lookup and no cache write, under the bypass reason `part-scoped-head`.

  S3 answers such a request with that PART's `Content-Length` plus a `Content-Range`, but the cache key is path-only and does not carry the query string. Treating it as an ordinary `HeadObject` therefore filed a partial response under the whole-object key, after which the object's cached length was one part's length — so a client that sizes an object from `HEAD` before reading (as the AWS CLI's CRT transfer client does) read only that many bytes and treated the transfer as complete, with HTTP 200 and no error. Fixed in 2.6.0; present from v0.5.0 to 2.5.0 inclusive.

  Caching it under a part-scoped key was considered and rejected: a part-scoped `HEAD` is rare and cheap to serve from S3, and a part-key HEAD grammar would add TTL and invalidation surface for the same class of bug. Bypassing also fixes the converse case, where a part-scoped `HEAD` issued after a plain one was answered from the whole-object entry and returned the object's length with no part count.

**Part Operations** (cached):
- **GetObjectPart** (query parameter: `partNumber`) - Cached as ranges using existing range storage architecture. Unaffected by the above: a part `GET` writes a real range and takes the object's length from `Content-Range`'s total, so it neither truncates a later read nor mis-reports the object's size.

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
HEAD /bucket/object.txt?partNumber=1      → Bypassed (part-scoped-head)
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
- Cache key: `bucket/object` (path only)
- The cached object represents the "current" version at time of caching
- Versioned requests bypass cache entirely, so no version comparison is needed

### Logging

Versioned request bypass is logged at DEBUG level:

```
DEBUG Versioned request detected, bypassing cache: cache_key=bucket/object
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

## Tuning Profiles

Cache-behaviour recipes, keyed on TTL, cache size, and the RAM tier. For whole-deployment
configurations (ports, TLS, logging, dashboard) see
[`docs/examples/`](examples/) instead.

## High-Change Environment

Objects change frequently, need fresh data quickly:

```yaml
cache:
  get_ttl: "3600s"      # 1 hour data TTL
  head_ttl: "300s"      # 5 minutes metadata TTL
  max_cache_size: 10737418240  # 10GB
```

## Static Content / CDN Use Case

Objects rarely change, maximize cache hits:

```yaml
cache:
  get_ttl: "604800s"    # 7 days data TTL
  head_ttl: "86400s"    # 24 hours metadata TTL
  max_cache_size: 107374182400  # 100GB
```

## Cost-Optimized

Minimize S3 requests, balance freshness:

```yaml
cache:
  get_ttl: "86400s"     # 24 hours
  head_ttl: "3600s"     # 1 hour
  ram_cache_enabled: true
  max_ram_cache_size: 1073741824  # 1GB RAM cache
```

## Write-Heavy Workload

Optimize for upload-then-download patterns with multipart support:

```yaml
cache:
  write_cache_enabled: true
  put_ttl: "7200s"          # 2 hours
  write_cache_percent: 20.0  # 20% of cache for writes
  write_cache_max_object_size: 536870912  # 512MB max per PUT
  
  # Multipart uploads use same capacity limits
  # Incomplete uploads cleaned up after incomplete_upload_ttl (default 1 day, range 1h-7d)
```

**Multipart Behavior:**
- Multipart uploads share the same `write_cache_percent` capacity
- If cumulative parts exceed capacity, upload is bypassed automatically
- Incomplete uploads are cleaned up after `incomplete_upload_ttl` (default 1 day)
- Completed multipart uploads support range requests immediately

## Cache Efficiency Optimization

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
- Reduced S3 bandwidth costs by 50-80% (based on internal testing with synthetic workloads)
- Faster response times (2-5x) compared to full S3 fetches (based on internal testing with synthetic workloads)
- Eliminated S3 requests for cached parts

## Cache Invalidation

### Automatic Invalidation

The proxy automatically invalidates cache when:

1. **Metadata Mismatch**: ETag or Last-Modified changes
2. **PUT to Same Key**: New upload invalidates old cache (conflict handling)
3. **CreateMultipartUpload to Same Key**: New multipart upload invalidates old cache (conflict handling)
4. **DELETE Request**: Removes cache entry
5. **S3 Returns New Data**: When S3 returns 200 OK for conditional requests, indicating cached data is stale
6. **Incomplete Upload Timeout**: Uploads in-progress for longer than
   `incomplete_upload_ttl` (default 1 day) are removed

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

Every cache write, on both the disk and RAM tiers, is wrapped in an LZ4 frame. Whether
the payload inside that frame is actually compressed depends on the content: a
denylisted extension or a rule setting `compression_enabled: false` produces a
**store-mode** frame, which is still framed and checksummed but never passed to the
block compressor. See [COMPRESSION.md](COMPRESSION.md) for the denylist and the
store-mode contract.

**Benefits**:
- Fast compression/decompression (minimal CPU)
- Integrity checking on every cached range, including store-mode writes
- Optimized disk-to-RAM cache promotion (no decompress/recompress cycles)

**Content-Aware**:
- Skips already-compressed formats (images, video, archives) by writing store-mode frames
- Configurable threshold (default: 1KB minimum)

```yaml
compression:
  enabled: true
  threshold: 1024           # 1KB minimum
  preferred_algorithm: "lz4"
```

The built-in extension denylist (skip .jpg, .mp4, .zip, etc.) is always
active by default; override it per key with a `cache_rules.json` rule's
`compression_enabled` field. See [COMPRESSION.md](COMPRESSION.md).

### RAM Cache Compression Optimization

When promoting data from disk cache to RAM cache, compressed data is passed directly without decompressing and recompressing. Size checks use the compressed size. See [COMPRESSION.md - RAM Cache Compression Optimization](COMPRESSION.md#ram-cache-compression-optimization) for details.

## Cache Hit Rate

**Good hit rate**: 70-90%. Indicates effective caching and significant bandwidth savings.

**Low hit rate**: below 50%. Consider a larger cache, longer TTLs, or check whether the
workload is cache-friendly at all — a workload of unique one-time reads has no hit rate to
improve.

The formula and the fields behind it are in
[METRICS_REFERENCE.md](METRICS_REFERENCE.md#cache). Note that the denominator counts every
GET/HEAD through the proxy, including list-object GETs and non-cacheable responses, so the
figure is the fraction of all GET/HEAD traffic served from cache rather than of cacheable
object reads alone.

Partial cache hits are still valuable — see
[CACHE_READ_PATHS.md — Observing Range Merge Efficiency](CACHE_READ_PATHS.md#observing-range-merge-efficiency).

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

## See Also

- [CACHE_INTERNALS.md](CACHE_INTERNALS.md), [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md), [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md), [EVICTION.md](EVICTION.md), [SHARED_STORAGE.md](SHARED_STORAGE.md) — the five companion documents above
- [CONFIGURATION.md](CONFIGURATION.md) — field reference for every cache option
- [COMPRESSION.md](COMPRESSION.md) — the compression decision and the denylist
- [MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md) — multipart cache internals
- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) — the `cache` and `page_cache` counters
- [CONNECTION_POOLING.md](CONNECTION_POOLING.md) — S3 connection optimization
