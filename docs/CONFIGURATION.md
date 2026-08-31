# Configuration Reference

Complete configuration guide for Hybrid Cache for Amazon S3 including cache behavior, TTL management, and performance tuning.

## Table of Contents
- [Configuration Methods](#configuration-methods)
- [Server Configuration](#server-configuration)
- [Cache Configuration](#cache-configuration)
- [Time-To-Live (TTL) Configuration](#time-to-live-ttl-configuration)
- [Write Cache Configuration](#write-cache-configuration)
- [Cache Rules](#cache-rules)
- [Cache Expiration Scenarios](#cache-expiration-scenarios)
- [RAM-Disk Cache Coherency](#ram-disk-cache-coherency)
- [Cache Hit Performance Tuning](#cache-hit-performance-tuning)
- [Range Request Optimization](#range-request-optimization)
- [Eviction Configuration](#eviction-configuration)
- [Multi-Instance Coordination](#multi-instance-coordination)
- [Download Coordination](#download-coordination)
- [Cache Size Tracking](#cache-size-tracking)
- [Compression Configuration](#compression-configuration)
- [Connection Pooling](#connection-pooling)
- [DNS Server Configuration](#dns-server-configuration)
- [S3 PrivateLink (Interface VPC Endpoints)](#s3-privatelink-interface-vpc-endpoints)
- [Upstream Transport Overrides](#upstream-transport-overrides)
- [IP Distribution](#ip-distribution)
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
- [Download Bandwidth QoS Configuration](#download-bandwidth-qos-configuration)
- [See Also](#see-also)
---

## Configuration Methods

Hybrid Cache for Amazon S3 supports three-layer configuration with precedence:

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
  https_port: 443            # HTTPS proxy port (TCP passthrough, no caching)
  max_concurrent_requests: 1000   # default
  request_timeout: "30s"          # NOT ENFORCED — see below
```

**HTTP Port (80)**
- Full caching enabled (GET/HEAD requests)
- Range request optimization
- Write-through caching (enabled by default)
- Requires sudo for port < 1024

**HTTPS Port (443)**
- **Passthrough mode**: TCP tunneling, no caching (only mode)

#### `request_timeout` is not enforced

`request_timeout` (`Duration`, default `"30s"`) is accepted and validated, but **no code
path reads it**. Setting it has no effect on request handling. It is retained so existing
config files keep parsing.

The 30s upstream timeout that does apply is a separate hardcoded value in `S3Client`,
which coincidentally matches this field's default. Changing this field does not change
that timeout. For timeouts you can actually control, see
[Upstream Timeout](#upstream-timeout-stalled-response-fast-fail).

#### Max Concurrent Requests

Default `1000`.

- Small deployments (< 50 users): 50-100
- Medium deployments (50-500 users): 100-300
- Large deployments (500+ users): 300-1000+
- High-throughput scenarios: 1000+

A permit is held for the whole transfer, not just request setup, so these bands
admit fewer simultaneous requests than the raw numbers suggest. Treat them as a
starting point and confirm against `/metrics` →
`request_metrics.permits_held_peak` under your own load. For reference, a
100-client test of 8 MiB ranged reads peaked at 71 permits held.

##### What this limit actually covers

`max_concurrent_requests` bounds concurrency for the **whole request**, including
its response-body transfer, not just the setup phase. The concurrency permit is
acquired as an owned permit (`try_acquire_owned`) and attached to the response
body itself; it releases only when the body has fully streamed to the client (or
the connection is dropped), and a share of it is held by any background
cache-commit task until that commit completes. This changed with the
transfer-concurrency-admission work (2.5.0): earlier releases released the permit
at response-head construction, before any body byte reached the client, so a
slow transfer held its permit for only the milliseconds of its setup. That
description is no longer accurate — this field now bounds concurrent *transfers*,
not just concurrent request setups.

Exceeding the limit sheds the request with HTTP 503 `SlowDown` and `Retry-After`
rather than queueing it; AWS SDKs retry 503 with backoff automatically. Because a
permit is now held for the duration of a transfer, a workload of many large,
slow concurrent transfers reaches this limit sooner than the old setup-phase-only
behaviour would have — size this value from measured `permits_held_peak`
(`/metrics` → `request_metrics.permits_held_peak`) under representative load, not
from request rate alone. The default was raised from 200 to 1000 for this reason
(derived from a 100-client fleet measurement of peak concurrent transfers; see
`CHANGELOG.md`'s 2.5.0 entry).

#### Memory Impact

Per-connection streaming memory is roughly 5 MiB and is independent of object size:
- Cache hit path: 1 MiB decompression chunk buffer + up to 4 MiB channel backpressure (4 × 1 MiB chunks)
- Cache miss path: 1 MiB TeeStream receive buffer + up to 4 MiB incremental cache write buffer

Because a permit now spans the full transfer, worst-case fleet streaming memory
**is** bounded by `max_concurrent_requests × ~5 MiB`, for the streaming paths
only (signed writes and cache-miss GET). This is the formula an earlier revision
of this document removed as unsound when permits covered only the setup phase; it
is sound again now that permits span the transfer. It does not, however, cover
the paths that still buffer a whole body or range in memory (see below) — those
are a second, independent budget.

Three budgets to size together, none of which the others cover:

1. **Streaming per-connection memory**: `max_concurrent_requests × ~5 MiB`, per
   above.
2. **In-flight buffered-byte ledger** (`server.max_inflight_buffer_bytes`,
   default `0` = disabled): bounds the sum of concurrent buffered request and
   response bodies. Since 2.5.0 this is almost entirely a response-side budget:
   range merge, page widening, buffered range serving, and the
   recovery/tee-accumulation fallbacks. On the request side, uploads stream —
   signed and unsigned PUT and UploadPart, the signed-PUT bypass arms, and POST
   object upload all forward frame by frame — leaving only small non-GET/PUT
   bodies such as a `DeleteObjects` request, which an internal 1 MiB bound covers.
   See [In-Flight Memory Ceiling](#in-flight-memory-ceiling) below for sizing
   guidance. Left at its default (disabled), this budget is unbounded — the same
   as every release before it existed.
3. **RAM cache** (`cache.max_ram_cache_size`, default 512 MiB): accounted
   separately from both of the above, and grows independently of in-flight
   request traffic.

Size instances from **measured** peak resident memory under representative load
(the `/metrics` fields named above for budgets 1 and 2, plus `cache.max_ram_cache_size`
for budget 3), not from a single combined formula — the three budgets are
independent and additive, but nothing enforces a combined ceiling across all
three at once.

### In-Flight Memory Ceiling

```yaml
server:
  # max_inflight_buffer_bytes: 0  # 0 = disabled (default)
```

**`max_inflight_buffer_bytes`** (`u64`, default `0` = Ledger_Disabled)

Maximum total bytes held simultaneously across all buffered request and response
bodies — the paths named in [Memory Impact](#memory-impact) above that still
buffer a whole body or range rather than streaming it. `0` (the default)
disables the accounting entirely: an existing deployment gains no new rejection
behaviour on upgrade without an explicit configuration change.

Since 2.5.0 this ceiling bounds **response-side** buffering almost exclusively:
range merge, page widening, buffered range serving, and the recovery fallbacks.
Every upload path streams, so a request body reaches the ledger only on the small
non-GET/PUT verbs (a `DeleteObjects` POST and similar), which an internal 1 MiB
bound covers.

This does **not** apply to the streaming paths (signed and unsigned writes,
cache-miss GET), which are already bounded per-connection independently of object
size by budget 1 in [Memory Impact](#memory-impact) — configuring this field
changes nothing about their behaviour.

A request whose buffered body would push the running total over this ceiling is
rejected with the same HTTP 503 `SlowDown` + `Retry-After` response used by
`max_concurrent_requests`, before any upstream connection is opened — never HTTP
413, since this is a transient capacity condition, not a statement that the
request itself is too large. AWS SDKs retry 503 with backoff automatically.
Startup validation rejects a non-zero `max_inflight_buffer_bytes` below **1 MiB**,
the internal bound on a single buffered request body — below that floor every
maximal buffered body would be rejected regardless of load. `0` is exempt. Any
ceiling at or above 1 MiB is legal with no other configuration change.

A single request counts once against the ceiling for a given set of bytes, even when
serving it involves an internal re-fetch of the same range (for example repairing a
range whose cached data no longer covers the request). Those bytes are one allocation,
so they are accounted as one claim, sized to the larger of the two.

Sizing: on an instance with `M` bytes of RAM, start at roughly
`M / 4 - cache.max_ram_cache_size`, then confirm against
`/metrics` → `inflight_memory.peak_reserved_bytes` under real traffic and adjust.
On the recommended `c6in.large` (4 GiB) with the default 512 MiB RAM cache that
gives 1024 MiB − 512 MiB = **512 MiB**:

```yaml
server:
  max_inflight_buffer_bytes: 536870912  # 512 MiB on a 4 GiB c6in.large
```

`/metrics` → `inflight_memory` also reports `reserved_bytes` (current total),
`ceiling_bytes` (this field's value, reported even while disabled),
`rejected_total` (Admission_Check rejections), and
`aborted_accumulations_total` (unknown-length bodies aborted mid-accumulation).

### Proxy Identification

```yaml
server:
  add_referer_header: true   # Add Referer header to forwarded requests (default: true)
```

When enabled, the proxy adds a `Referer` header to requests forwarded to S3 in the format `Hybrid Cache for Amazon S3/{version} ({hostname})`. This header appears in S3 Server Access Logs, enabling usage tracking and per-instance debugging.

The header is only added when:
- The request does not already contain a `Referer` header
- The `Referer` header is not included in the SigV4 `SignedHeaders` (to preserve signature validity)

**Querying S3 Server Access Logs**:

The `Referer` header in S3 Server Access Logs enables:

1. **Identify which proxy instance served a request**: Each instance includes its hostname in the header (e.g., `Hybrid Cache for Amazon S3/2.0.0 (proxy-instance-1)`)
2. **Determine if requests went through the cache**: Requests with the proxy's `Referer` header were served via HTTP (port 80) where caching occurs. Requests without this header were served via HTTPS passthrough (port 443) with no caching.
3. **Track proxy version distribution**: Identify which proxy versions are active across your fleet
4. **Debug routing issues**: Verify traffic is flowing through the expected proxy instances

Example S3 Server Access Log entry showing the `Referer` field:
```
bucket-name [01/Jan/2024:12:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT file.txt "GET /file.txt HTTP/1.1" 200 - 1024 1024 10 9 "Hybrid Cache for Amazon S3/2.0.0 (proxy-instance-1)" "aws-cli/2.x" -
```

Query examples using AWS Athena:
```sql
-- Count requests by proxy instance
SELECT referer, COUNT(*) as request_count
FROM s3_access_logs
WHERE referer LIKE 'Hybrid Cache for Amazon S3/%'
GROUP BY referer
ORDER BY request_count DESC;

-- Identify requests that bypassed the cache (HTTPS passthrough)
SELECT *
FROM s3_access_logs
WHERE referer IS NULL OR referer NOT LIKE 'Hybrid Cache for Amazon S3/%';
```

### Request Body Size and Streaming Writes

```yaml
server:
  # write_cache_tee_channel_depth: 5             # frames (default)
```

Upload size is governed by S3's own limits, not by proxy configuration. A body above
S3's 5 GiB single-part PUT and UploadPart maximum is rejected with HTTP 413
`EntityTooLarge` before any upstream connection opens; anything S3 accepts, the proxy
forwards. Every upload path streams the body to the upstream frame by frame, so proxy
memory during an upload is independent of object size.

> **Deprecated: `server.max_buffered_request_body_bytes`.** An existing config file
> setting this field still parses and starts; the value has no effect from 2.5.0
> onward. A value other than the old 5 GiB default logs a startup warning naming the
> field. An operator who had lowered it to reject large uploads no longer gets that
> rejection — S3's limits apply instead. The field will be removed in a future release.

**`write_cache_tee_channel_depth`** (`usize`, default `5`)

Bounded depth, in frames, of the streaming write-cache tee channel. On a streamed PUT or UploadPart the request body is forwarded to the upstream verbatim (preserving the SigV4 signature) while each frame is cloned onto a bounded channel feeding the write-through cache writer. At most this many frames are queued for the cache writer before backpressure stalls the next client read.

Effect on per-connection memory: the per-connection streaming-cache budget is one in-flight frame plus this many queued frames. A frame is a single forwarded request-body frame, bounded by the HTTP read buffer, so the default of 5 keeps per-connection streaming memory on par with the GET path and independent of object size.

This is a **per-connection** bound, and it *does* combine with `max_concurrent_requests`
into the streaming-path fleet-wide ceiling described in [Memory Impact](#memory-impact)
above (`max_concurrent_requests × write_cache_tee_channel_depth × frame`), now that
permits span the whole transfer rather than only the setup phase — see
[What this limit actually covers](#what-this-limit-actually-covers). Raising the depth
can improve cache-write throughput under bursty writes at the cost of more
per-connection memory; it does not duplicate `compression_batch_size`, which the
cache writer reuses for LZ4 batching. Omitting the field applies the default, so
existing config files keep working unchanged on upgrade.

### Environment Variables

- `HTTP_PORT` - Override HTTP port
- `HTTPS_PORT` - Override HTTPS port
- `MAX_CONCURRENT_REQUESTS` - Override request limit

### TLS Proxy Configuration

The TLS proxy listener terminates TLS on a configurable port and processes decrypted HTTP through the caching pipeline. Clients use `HTTP_PROXY=https://proxy:3129` to select the proxy connection and `--endpoint-url http://s3.<region>.amazonaws.com` to select a cacheable HTTP S3 endpoint. For repeated S3 commands against buckets in one Region, set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` instead of repeating `--endpoint-url`.

```yaml
server:
  tls:
    enabled: true                    # Enable TLS proxy listener (default: false)
    tls_proxy_port: 3129             # TLS proxy port (default: 3129)
    cert_path: "/mnt/nfs/config/tls/cert.pem"   # Path to PEM certificate
    key_path: "/mnt/nfs/config/tls/key.pem"     # Path to PEM private key
```

**Configuration fields**:

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable the TLS proxy listener |
| `tls_proxy_port` | `3129` | Port for TLS-terminated caching connections |
| `cert_path` | (empty) | Path to PEM certificate file (required when enabled) |
| `key_path` | (empty) | Path to PEM private key file (required when enabled) |

**Validation rules**:
- When `enabled: true`, `cert_path` and `key_path` must be non-empty
- `tls_proxy_port` must not conflict with `http_port`, `https_port`, `health.port`, `metrics.port`, or `dashboard.port`
- `tls_proxy_port` must not be 0
- If TLS listener fails to start (cert error, bind error), HTTP and HTTPS listeners continue normally

**TLS proxy port vs HTTPS port**: The HTTPS port (443) does TCP passthrough without caching. The TLS proxy port (3129) terminates TLS using the proxy's own certificate and processes decrypted HTTP through the caching pipeline with full range merging, compression, and write-through support.

**Certificate storage**: For multi-instance deployments with shared storage, store the certificate and key on the shared volume alongside the configuration so all instances use the same certificate. See [Architecture - Network Security](ARCHITECTURE.md#network-security-requirements) for details.

**Certificate generation**: See [Getting Started - Generating a Self-Signed Certificate](GETTING_STARTED.md#generating-a-self-signed-certificate) for openssl commands and SAN guidance.

## Cache Configuration

### Basic Settings

```yaml
cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 10737418240     # 10GB in bytes
  ram_cache_enabled: true
  max_ram_cache_size: 536870912   # 512MB in bytes (default; raised from 256MB — see "RAM Sizing and the Admission Ceiling" below)
  ram_cache_shard_count: 8        # Number of independent RAM cache shards (default: 8, range: 1–256)
  eviction_algorithm: "tinylfu"   # Options: lru, tinylfu
```

**Sizing `max_cache_size`**: Set `max_cache_size` to no more than 90% of available storage capacity. The cache is designed to temporarily exceed the configured limit during high load — writes are non-blocking and eviction runs asynchronously, so burst traffic can push usage above the limit before eviction reclaims space. A 10% headroom buffer prevents disk exhaustion during these transient spikes. For example, on a 100 GB volume, set `max_cache_size` to 90 GB or less.

**Cache Directory**: See [CACHING.md](CACHING.md) for detailed directory structure

**`ram_cache_shard_count`** (`usize`, default `8`, range `1–256`)

Divides the RAM cache into this many independent shards, each with its own lock. Concurrent requests targeting different shards proceed in parallel without contention. Per-shard capacity is `max_ram_cache_size / effective_shard_count` (see the admission-ceiling clamp below) — for example, at the default 512 MB / 8 shards = 64 MB per shard.

Entries larger than the per-shard capacity are not admitted to the RAM cache; they are served from disk instead. The admission-ceiling clamp below keeps per-shard capacity at 64 MiB or above, and the proxy logs a warning at startup when that clamp reduces the effective shard count below the configured `ram_cache_shard_count`.

Tuning:
- Higher values (e.g. 32, 64) — lower contention under extreme concurrency (hundreds of parallel requests hitting distinct keys). Most deployments see no additional gain beyond 8–16 shards.
- Lower values (e.g. 1, 2) — larger per-shard capacity per bucket; useful for very small caches or workloads dominated by a few large objects.

Skew caveat: each shard evicts independently. When a shard reaches capacity it evicts regardless of free space in other shards, so effective RAM cache utilization may be lower than the configured maximum for workloads with uneven key distributions or very large objects.

#### RAM Sizing and the Admission Ceiling

The proxy unconditionally guarantees that any single RAM cache entry up to a hardcoded **64 MiB admission ceiling** (`RAM_CACHE_ADMISSION_CEILING = 67108864` bytes — a compile-time constant, not a config field) is admitted to the RAM cache rather than silently dropped. This applies regardless of whether [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching) is enabled for any key — it also covers plain large range reads.

It works by clamping the **effective** shard count so per-shard capacity never falls below the ceiling:

```
effective_shard_count = min(ram_cache_shard_count, max(1, max_ram_cache_size / 67108864))
```

When this clamps the effective shard count below the configured `ram_cache_shard_count`, the proxy logs a warning at startup naming the reduced concurrency and suggesting a larger `max_ram_cache_size`.

**Default `max_ram_cache_size` is 512 MiB** (`536870912` bytes), which at the default `ram_cache_shard_count: 8` yields exactly 8 effective shards (512 MiB / 8 = 64 MiB per shard — no clamp). Deployments pinning a smaller value keep it and accept the clamp's reduced shard count with a warning — e.g. 256 MiB / 8 configured shards clamps to 4 effective shards (256 MiB / 64 MiB).

Admission is not the same as retention: a shard sized at the 64 MiB ceiling holds few large entries, so shard skew can evict a hot entry even while other shards are idle. To keep `N` concurrent hot large (up to 64 MiB) entries resident, size `max_ram_cache_size >= N * 67108864`. This is documented guidance, not an enforced invariant.

**Eviction Algorithms**

- **LRU** (Least Recently Used): Evicts oldest accessed entries
- **TinyLFU**: Frequency-recency hybrid (simplified implementation, not full TinyLFU algorithm)

### Metadata Cache

A RAM cache for `NewCacheMetadata` objects, separate from the RAM *data* cache above.
It holds parsed `.meta` contents so HEAD requests and GET freshness checks avoid a disk
read. See [CACHING.md — RAM Metadata Cache](CACHE_INTERNALS.md#ram-metadata-cache) for the
mechanism and its interaction with shared storage.

```yaml
cache:
  metadata_cache:
    enabled: true                  # Default: true
    refresh_interval: "5s"         # Default: 5s. Valid range: 1-300s
    max_entries: 100000            # Default: 100000. Valid range: 100-1000000
    stale_handle_max_retries: 3    # Default: 3. Valid range: 1-10
```

**`enabled`** (`bool`, default `true`) — turn the metadata cache off to force every
metadata read to hit disk.

**`refresh_interval`** (`Duration`, default `"5s"`, range 1-300s) — staleness threshold.
An entry older than this is re-read from disk on next access. This governs **disk**
re-reads, not S3 re-fetches; S3 freshness is governed by the TTLs.

**`max_entries`** (`usize`, default `100000`, range 100-1000000) — entry cap. Each entry
is roughly 1-2 KB, so the default is about 150-250 MB of RAM. Size this alongside
`max_ram_cache_size`; the two are independent budgets.

**`stale_handle_max_retries`** (`u32`, default `3`, range 1-10) — retries on an NFS/EFS
stale file handle, which happens when another instance replaces a `.meta` file mid-read.

### Cache Bypass Headers

**`cache.cache_bypass_headers_enabled`** (`bool`, default `true`)

When `true`, a client can bypass the cache for a single request by sending
`Cache-Control: no-cache`, `Cache-Control: no-store`, or `Pragma: no-cache`. Set it to
`false` to ignore those headers, so no client can force a cache bypass. See
[CACHING.md — Cache Bypass](CACHING.md#cache-bypass-headers).

### Cache Environment Variables

- `CACHE_DIR` - Override cache directory
- `RAM_CACHE_ENABLED` - Enable/disable RAM cache
- `WRITE_CACHE_ENABLED` - Enable/disable write-through caching

There is **no** environment override for `max_cache_size` or `max_ram_cache_size`; set
those in the config file. See [Environment Variable Reference](#environment-variable-reference)
for the complete list.

## Time-To-Live (TTL) Configuration

TTL (Time-To-Live) controls how long cached data is served to clients without revalidating against S3. While a cached object's TTL has not expired, the proxy serves it directly from cache — S3 is not contacted, and the requesting client's IAM credentials are not checked by S3 for that request. When TTL expires, the next request triggers [revalidation](CACHE_FRESHNESS.md#time-to-live-ttl-configuration): the proxy sends a conditional request to S3 using the client's credentials, and S3 performs its normal authentication and authorization checks. Setting TTL to zero forces revalidation on every request, ensuring S3 checks every client's credentials while still saving bandwidth via 304 Not Modified responses. See [Security Considerations](ARCHITECTURE.md#security-considerations) for the access control implications of TTL settings.

### TTL Types

Hybrid Cache for Amazon S3 uses three independent TTL values:

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
- The daily validation scan removes expired entries as it goes
- Frees disk space immediately when GET_TTL expires
- Useful when disk capacity is elastic and not fixed
- Adds CPU/IO overhead to the validation scan

> **Not compatible with `get_ttl: "0s"`.** A zero TTL means an entry expires the moment it
> is cached, so active expiration deletes it before anything can revalidate against it,
> and the cache never serves a hit. Zero-TTL deployments must keep
> `actively_remove_cached_data: false` — see
> [CACHE_FRESHNESS.md — Minimum TTL Values](CACHE_FRESHNESS.md#minimum-ttl-values).

On shared storage, active expiration checks whether an entry is in active use by another
instance before deleting it, and skips it if so. HEAD entries need no such check because
they are metadata-only.

**HEAD Metadata**: Always actively removed regardless of this setting

## Write Cache Configuration

Write-through caching stores PUT operations and multipart uploads in the cache so subsequent GET requests can be served immediately without fetching from S3.

**Not covered: POST object upload** (the browser form upload, `POST /bucket` with a
`multipart/form-data` body). It streams to S3 and succeeds, but is never write-through
cached — see [Caching → POST object upload is not write-through cached](CACHE_READ_PATHS.md#post-object-upload-is-not-write-through-cached).

### Write Cache Basic Settings

```yaml
cache:
  write_cache_enabled: true          # Enable write-through caching (enabled by default)
  write_cache_percent: 10.0           # Percentage of disk cache for writes (1-50%)
  write_cache_max_object_size: 268435456  # 256MB max object size
  put_ttl: "3600s"                    # Write cache TTL (default 1 hour)
  incomplete_upload_ttl: "1d"         # Incomplete multipart upload TTL
```

### How Write Caching Works

**Full PUT Operations**:
- Object data stored as a single range (0 to content-length-1)
- Write cache TTL set (default: 1 hour)
- S3 response returned to client unchanged

**Multipart Uploads**:
- CreateMultipartUpload: Tracking metadata created with uploadId
- UploadPart: Each part stored as a range file with part number
- CompleteMultipartUpload: Parts assembled with final byte offsets, object metadata created
- AbortMultipartUpload: All cached parts and tracking metadata deleted

**`max_complete_body_bytes`** (`usize`, default `10485760` = 10 MiB)

Maximum size of the `CompleteMultipartUpload` request body the proxy will buffer before forwarding. Bodies exceeding this limit are rejected with HTTP 413. The Complete XML normally lists at most 10,000 parts and is well under 1 MiB in practice; the cap prevents unbounded memory consumption from oversized or malicious payloads.

```yaml
cache:
  # max_complete_body_bytes: 10485760  # 10 MiB (default)
```

**TTL Transition on First Read**:
- When a write-cached object is first accessed via GET, the proxy performs a
  one-time transition: it clears the `is_write_cached` flag and resets `expires_at`
  from `now + put_ttl` to `now + get_ttl`. This is not a repeating refresh —
  subsequent reads do not extend the TTL.
- After the transition the object follows normal read-cache expiration rules
  (expires after one `get_ttl` window from the first GET, regardless of how
  frequently it is accessed).
- Objects never read expire after the original `put_ttl` and are evicted normally.
- The transition also **releases the object's bytes from the write-cache allocation**.
  Until the first read the object counts against `write_cache_percent`; afterwards it
  counts only against the total cache size. The bytes are not moved or rewritten — only
  which allocation they are charged to changes, so `cache.write_cache_size` falls while
  `cache.size` does not. `write_cache.graduations_total` counts these transitions; see
  [METRICS_REFERENCE.md](METRICS_REFERENCE.md#write_cache).
- The release is applied once per object across the whole fleet, so several proxies
  reading the same object at the same moment cannot release its bytes more than once.

### Capacity Management

The write cache is allocated a percentage of the total disk cache, so that uploads cannot
crowd out read-cached data:

```yaml
cache:
  write_cache_percent: 10.0  # Default: 10% of max_cache_size
```

**What `write_cache_percent` bounds.** It bounds **un-graduated staging** —
objects that have been written through the cache but not yet read — not all write-cached
data. Once an object is read for the first time it graduates: it stops counting against
this allocation and counts only against the total cache size instead, exactly as described
under [TTL Transition on First Read](#how-write-caching-works) above. A deployment whose
workload reads back what it writes therefore keeps very little resident against this
percentage at any given moment, regardless of how much has been written through overall.

The bound applies to bytes resident on the **shared cache volume**, fleet-wide, not to a
private per-instance figure. Every proxy sharing the volume reads and enforces against the
same underlying size, so the allocation is not multiplied by the number of instances.

Enforcement is continuous, against the staged bytes recorded for the shared volume. Going
over the allocation triggers background reclamation and refuses nothing — see Eviction
behavior below. If a larger unread working set is intended, raise `write_cache_percent`
rather than letting reclamation churn against it.

**Eviction behavior**:
- **The allocation is a target, not a limit.** An upload is cached even when the allocation
  is already full; the excess is reclaimed in the background rather than by refusing the
  upload. Reclamation starts at `eviction_trigger_percent` of the allocation and runs until
  it reaches `eviction_target_percent` — the same two knobs the read cache uses, so
  hysteresis tuned for one tier applies to both.
- **Reclamation order is oldest-staged-first, with expired entries first.** An object that
  has never been read cannot change its own eviction rank — its last-access time *is* its
  write time — so there is no frequency or recency score to compute. Objects whose
  `put_ttl` has elapsed without ever being read are reclaimed ahead of those still within
  it, and both are candidates.
- **Reclamation never runs on the request path.** It is driven from the background
  maintenance cycle, so no upload waits on it, and a deployment that stops uploading still
  drains its staging tier.
- Staged bytes also leave the allocation without any reclamation, by being read for the
  first time (graduation, described above) — which is the normal case for a
  read-after-write workload.
- **Only available space declines to cache.** A write-through upload is skipped when
  caching it would take the cache past `max_cache_size`, or when the cache volume has less
  than 1 GiB free beyond the object's own size. This is reported as `disk_safety` in
  `signed_put.skipped_puts_total`, and `/health` reports the cache component as `Degraded`
  while it is happening. The upload itself always proceeds to S3 regardless.
- See
  [EVICTION.md — Recovery from an inflated write-cache figure](EVICTION.md#recovery-from-an-inflated-write-cache-figure)
  if the reported write-cache size looks too high for what is actually staged.

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

### CompleteMultipartUpload Body Limit

The CompleteMultipartUpload XML body (listing parts and ETags) is bounded to prevent
unbounded memory consumption from oversized or malicious payloads:

```yaml
cache:
  max_complete_body_bytes: 10485760  # Default: 10 MiB
```

Bodies exceeding this limit are rejected with HTTP 413 before forwarding to S3. The
S3 maximum of 10,000 parts results in a body well under 1 MiB in practice, so the
10 MiB default is conservative. Operators on memory-constrained instances may lower
this value.

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
  
  # Keep write-cached objects for 1 day before their first read
  put_ttl: "1d"
  
  # Clean up incomplete uploads after 6 hours
  incomplete_upload_ttl: "6h"
```

## Cache Rules

Cache rules override global cache settings for keys that match a glob pattern, without modifying the YAML config or restarting the proxy. Rules live in a single hot-reloadable file at `cache_dir/cache_rules.json`. The file is optional: when it is absent or its `rules` array is empty, every setting resolves to the global YAML `cache:` defaults, so a deployment that has never used the file behaves exactly as before.

> **Breaking change (v2.0.0).** Cache rules replace the per-bucket `cache_dir/metadata/{bucket}/_settings.json` mechanism. `_settings.json` files are no longer read, and there is no automatic migration. See [CHANGELOG.md](../CHANGELOG.md) for a before/after translation.

### Global Config Fields

These YAML `cache:` fields supply the defaults that rules fall through to. They are unchanged; only the rules mechanism that overrides them has changed.

```yaml
cache:
  # Default read caching for GET responses (default: true).
  # When false, nothing is read-cached unless a matching rule sets read_cache_enabled: true —
  # an "allowlist" pattern where only keys matched by an enabling rule are cached.
  read_cache_enabled: true

  # Default true: If-Match requests are served from cache when the cached ETag matches and
  # data is fully cached (no S3 round trip). All other conditionals still forward to S3.
  # Set false to forward every conditional to S3 for strict S3-authoritative evaluation.
  # See CACHING.md "Conditional Headers Handling" for full semantics.
  evaluate_conditions_from_cache: true

  # How long a loaded rule set is considered fresh before cache_rules.json is re-read from disk
  # (default: 60s). Controls lazy reload — rule edits take effect on the next request after this
  # threshold expires. Field name retained for config compatibility (conceptually "rules staleness").
  bucket_settings_staleness_threshold: "60s"
```

### Rules File Format

The file holds an optional `$schema` reference plus an ordered `rules` array. Each rule has a required glob `pattern` and an optional subset of settings fields. Omitted fields are unset for that rule and fall through (see [Precedence](#first-match-per-field-precedence)).

```json
{
  "$schema": "../docs/cache-rules-schema.json",
  "rules": [
    { "pattern": "**/credit-cards/**", "read_cache_enabled": false },
    { "pattern": "**/logs/**",         "get_ttl": "0s" },
    { "pattern": "prod-*/static/**",   "get_ttl": "7d", "compression_enabled": true },
    { "pattern": "my-bucket/temp/**",  "get_ttl": "0s" },
    { "pattern": "**",                 "get_ttl": "5m" }
  ]
}
```

**Optional per-rule fields**: `get_ttl`, `head_ttl`, `put_ttl`, `read_cache_enabled`, `write_cache_enabled`, `compression_enabled`, `ram_cache_eligible`, `evaluate_conditions_from_cache`, `page_widening`, `page_size`, `hedging_enabled`, `hedge_trigger_after`, `hedge_max_per_request`

**`page_widening` / `page_size` (page-aligned range caching / range read widening).** `page_widening` (bool, default `false`) enables widening a small ranged GET for matching keys into a fixed-size, page-aligned fetch; `page_size` (bytes, default `16777216` = 16 MiB when `page_widening` is enabled without specifying it) sets the page size `P`. `page_size` must be `> 0` and `<= 67108864` (64 MiB) for any rule enabling `page_widening` — validated at startup and on hot reload; an out-of-range value invalidates the rule set (see [Validation](#validation)). Off by default and never enabled globally — only via an explicit rule, since amplification is workload-dependent.

**Check eligibility before enabling.** Widening applies only when `range` is absent from the request's SigV4 `SignedHeaders`. The AWS CLI and every official AWS SDK sign `Range`, so enabling this for a CLI/SDK workload has no effect and produces no error: requests fall through to the ordinary range path and increment `page_cache.skipped_signed_range`. Read [CACHING.md — Eligibility: which clients can use this](CACHE_READ_PATHS.md#eligibility-which-clients-can-use-this) first, then [Page-Aligned Range Caching](CACHE_READ_PATHS.md#page-aligned-range-caching) for the mechanism.

Field syntax, for a key pattern whose reads cluster within pages:

```json
{ "pattern": "**/*.parquet", "page_widening": true, "page_size": 16777216 }
```

**`hedging_enabled` / `hedge_trigger_after` / `hedge_max_per_request` (hedged upstream requests).** `hedging_enabled` (bool, default `false`) enables hedged upstream requests for matching keys: when the original upstream fetch has not returned its first byte within `hedge_trigger_after`, the proxy issues a second identical fetch and serves whichever responds first, cancelling the loser. `hedge_trigger_after` (duration, default `250ms` when `hedging_enabled` is true without specifying it) sets the TTFB threshold; must be `> 0` and strictly less than `connection_pool.upstream_first_byte_timeout` (default `5s`) — validated on every rules load. `hedge_max_per_request` (integer, default `1`) caps the number of hedges per client request; for range GETs fanning out into N parallel sub-fetches this budget is shared across all N. Off by default and never enabled globally — only via an explicit rule, since hedging trades bounded S3 request cost for lower tail latency and the right threshold depends on the workload's TTFB distribution. See [CONNECTION_POOLING.md](CONNECTION_POOLING.md) for the mechanism:

```json
{ "pattern": "analytics-bucket/hot-prefix/**", "hedging_enabled": true, "hedge_trigger_after": "250ms", "hedge_max_per_request": 1 }
```

**`compression_enabled` is rules-win.** The proxy has a built-in default
denylist of already-compressed extensions (images, video, audio, archives,
documents, executables — see `docs/COMPRESSION.md`) that skip LZ4
compression when no rule matches. If a rule explicitly sets
`compression_enabled` for a key, that value is honored verbatim, overriding
the denylist in either direction — e.g. `{"pattern": "**/*.jpg", "compression_enabled": true}`
forces compression of `.jpg` keys despite the built-in denylist; the reverse
lets you disable compression for an extension the denylist would otherwise
compress. The global `compression.threshold` size floor still applies
regardless of any rule.

**Duration format**: Same as global config — `"0s"`, `"30s"`, `"5m"`, `"1h"`, `"7d"`

**Schema and example**: Include `$schema` for IDE autocompletion. Full schema at [`docs/cache-rules-schema.json`](cache-rules-schema.json); a complete example at [`config/cache_rules.example.json`](../config/cache_rules.example.json).

### Glob Syntax

Patterns are globs, not regex. A rule matches against the **full cache key** (`{bucket}/{object_key}`, no leading slash). The metacharacters are:

| Glob | Matches |
|------|---------|
| `*`  | Any run of characters **except** `/` — one path segment |
| `**` | Any run of characters **including** `/` — crosses segments |
| `?`  | Exactly one character **except** `/` |
| any other char | A literal. Regex metacharacters are escaped, so a bucket named `my.logs` matches `.` literally, not as "any character" |

Two properties matter for writing correct rules:

- **Case-sensitive.** Patterns match the cache key exactly as cased. This mirrors S3: object keys are case-sensitive and bucket names are lowercase.
- **Anchored whole-string, not prefix.** A pattern matches only if it matches the *entire* key. `my-bucket/temp` matches only the exact key `my-bucket/temp` — not `my-bucket/temp/file.txt`.

> **Migration gotcha.** The old `prefix_overrides` used `starts_with`, so `temp/` matched everything under `temp/`. Globs are anchored. To match "everything under `temp/` in `my-bucket`", write `my-bucket/temp/**` — **not** `my-bucket/temp`, which matches only that one exact key.

Because the bucket is part of the match surface, one syntax expresses every scope:

| Pattern | Meaning |
|---------|---------|
| `my-bucket/temp/**` | The `temp/` prefix in one bucket (the old per-bucket prefix rule) |
| `**/logs/**` | A `logs/` segment anywhere, in every bucket |
| `**/credit-cards/**` | A `credit-cards` segment anywhere, in every bucket |
| `prod-*/static/**` | The `static/` prefix in every bucket whose name starts `prod-` |

### First-Match-Per-Field Precedence

Rules are an ordered list. For **each field independently**, the value comes from the earliest rule in list order that both matches the key and sets that field. A field left unset by a matching rule falls through to the next matching rule, and finally to the global YAML default. Put specific rules above broad ones.

Worked example — fields resolve independently:

```json
{
  "rules": [
    { "pattern": "my-bucket/reports/**", "get_ttl": "1h" },
    { "pattern": "**",                   "compression_enabled": true }
  ]
}
```

For the key `my-bucket/reports/q3.csv`:

- `get_ttl` → `1h` (from rule 1, the first rule that sets it)
- `compression_enabled` → `true` (rule 1 does not set it; falls through to rule 2)
- every other field → its global YAML default

The first rule supplies `get_ttl` and the second supplies `compression_enabled`; the result combines both.

Two invariants are applied after resolution (unchanged from prior behavior):

- `get_ttl == 0s` forces `ram_cache_eligible = false` (RAM cache bypasses revalidation, which would serve stale data).
- `read_cache_enabled == false` forces `ram_cache_eligible = false`.

### Hot Reload and Error Resilience

The proxy polls `cache_rules.json` on a staleness threshold (`bucket_settings_staleness_threshold`, default 60s). When the file has changed and the threshold has elapsed, the next request recompiles the rule set — no restart. Within the threshold the in-memory rule set is reused without touching disk. The pattern set is compiled once per successful load, never per request.

A bad edit never takes the cache down:

- Invalid JSON, failed validation (empty pattern, a pattern that fails to compile, an unparseable duration), or a rule count over the cap → the proxy keeps the last-known-good rule set and logs a warning.
- Invalid file at first startup → the proxy starts with an empty rule set (global defaults for every field) and logs a warning.

### Cache-Key Forms

Patterns match the cache key as computed by [cache-key normalization](CACHE_INTERNALS.md#cache-directory-structure), which differs by request kind. Match the leading segment accordingly (matching is case-sensitive):

| Request kind | Cache-key leading segment | Example pattern |
|--------------|---------------------------|-----------------|
| Regular bucket (any addressing style) | `{bucket}` | `my-bucket/temp/**` |
| Regional access point | `{name}-{account}-s3alias` | `myap-123456789012-s3alias/**` |
| Multi-Region Access Point (MRAP) | `{alias}.mrap` | `mfzwi23gnjvgw.mrap/data/**` |
| Non-AWS S3-compatible / unrecognized host | `{host}:/{path}` (bare normalized path) | `minio.local:/bucket/**` |

See [CACHING.md — Access Point and MRAP Cache Key Prefixing](CACHE_INTERNALS.md#access-point-and-mrap-cache-key-prefixing) for how each form is derived.

### Performance

Resolution cost depends on how many rules match a single key, not the total rule
count. 1024 highly-specific rules where only 1–2 match any given key are
effectively free; many broad `**` rules that all match the same key cost more.
Prefer specific patterns over broad catch-alls when rule counts are high.

The default cap of 1024 rules is a safety guardrail. In practice, resolution
adds negligible latency at any realistic rule count.

### Rules Are Re-Evaluated on Each Read

Resolved settings are re-evaluated against the current rules on each read request, so a rule change applies to already-cached objects on the next GET/HEAD after the staleness window — not only to newly written objects. Freshness is determined by comparing `now - created_at` against the current resolved TTL (`get_ttl` for GET, `head_ttl` for HEAD), not by the `expires_at` / `head_expires_at` baked at write time. For example, tightening a key's resolved `get_ttl` from `1h` to `5m` expires already-cached objects once `now - created_at` exceeds 5m, and `get_ttl: "0s"` forces revalidation against S3 on the next GET. Setting `read_cache_enabled: false` stops serving the key from cache and eagerly deletes the cached entry (range files + metadata) on the first GET/HEAD after the rule takes effect. No restart or manual cache wipe is required.

### Validation

- Empty `pattern` → rule set rejected
- A glob that cannot be translated to a valid regex → rule set rejected
- More than the maximum number of rules (default 1024) → rule set rejected
- An unparseable duration → rule set rejected
- A rule with `page_widening: true` and a `page_size` of `0` or greater than `67108864` (64 MiB) → rule set rejected
- A rule with `hedging_enabled: true` and a `hedge_trigger_after` of `0` or greater than or equal to `connection_pool.upstream_first_byte_timeout` (default `5s`) → rule set rejected

On any validation failure the proxy applies the reload error behavior above: it keeps the last-known-good rule set (or starts with an empty rule set if the file was invalid at first startup) and logs a warning.

See [CACHING.md](CACHING.md#cache-rules) for the runtime behavior of each setting.

## Cache Expiration Scenarios

Request-flow walkthroughs for each TTL state — fresh cache, expired GET TTL, a
PUT-cached object read for the first time, HEAD revalidation, conditional requests,
multipart, and incomplete-upload cleanup — live in
[CACHE_FRESHNESS.md — Cache Validation Flow](CACHE_FRESHNESS.md#cache-validation-flow),
which covers thirteen scenarios.

For the fields that drive them, see [Time-To-Live (TTL) Configuration](#time-to-live-ttl-configuration).

## RAM-Disk Cache Coherency

When both RAM and disk caches are enabled, the proxy maintains coherency:

```yaml
cache:
  # Batch flush settings
  ram_cache_flush_interval: "10s"      # Time between flushes (default: 10s)
  ram_cache_flush_threshold: 100       # Pending updates before flush (default: 100)
  ram_cache_flush_on_eviction: false   # Flush on RAM eviction (default: false)
```

> **Deprecated: `cache.ram_cache_verification_interval`.** An existing config file
> setting this field still parses and starts; the value has never had any effect. There
> is no RAM-versus-disk verification loop — earlier revisions of this document described
> one that was never implemented. RAM staleness is bounded by
> `cache.metadata_cache.refresh_interval` and by the per-key `get_ttl`; see
> [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md). A value other than the old 1s default logs a
> startup warning naming the field. The field will be removed in a future release.

### How It Works

**Access Statistics Propagation**: RAM cache hits batched and written to disk
- Disk eviction algorithms need accurate access statistics
- Without propagation, hot RAM data appears "cold" on disk
- Batch processing minimizes disk I/O overhead

### Configuration Guidance

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| flush_interval | More disk I/O, fresher stats | Less I/O, staler stats | 60s for most workloads |
| flush_threshold | More frequent flushes | Larger batches | 100 for balanced performance |

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

### Compression Batch Size

```yaml
cache:
  compression_batch_size: 1048576  # 1 MiB (default)
```

Size of compression batches during cache writes. Incoming S3 bytes are accumulated in RAM up to this size and then compressed as a single LZ4 frame before being appended to the cache file. Larger batches produce a better compression ratio and lower per-frame overhead; smaller batches reduce per-request peak memory.

**Valid range**: `65536` (64 KiB) to `16777216` (16 MiB) inclusive. Values outside this range are rejected at startup.

Set lower for memory-constrained environments with many concurrent writers. Set higher if memory is abundant and you want to maximize compression ratio on highly compressible content.

### Metadata File Size Cap

```yaml
cache:
  # max_metadata_file_bytes: 4194304  # 4 MiB (default)
```

Maximum size of a `.meta` file the proxy will read from disk. Files exceeding this cap are classified as corrupt (oversize) without being read, preventing pathologically large metadata files — such as legacy inline-body entries — from consuming unbounded memory or blocking the runtime.

A normal range-based `.meta` file is ~2 KB. The 4 MiB default leaves ample headroom for legitimate metadata while catching the legacy inline-body files that caused the production stall (5+ MB). Operators should not need to change this unless they have unusual object-key structures that produce very large metadata.

### Metadata I/O Concurrency

```yaml
cache:
  # metadata_io_concurrency: 32  # (default)
```

Maximum number of concurrent blocking metadata I/O operations (`spawn_blocking` tasks) allowed process-wide. A semaphore with this capacity is acquired before each blocking metadata read; excess callers wait asynchronously rather than spawning unbounded blocking tasks.

This prevents the Tokio blocking thread pool and the underlying filesystem (especially NFS) from being overwhelmed during high-concurrency bursts or consolidation cycles. The default of 32 provides good throughput on most deployments without saturating NFS.

Tuning:
- Lower (e.g. 16) on constrained NFS mounts or instances with limited I/O bandwidth
- Higher (e.g. 64) on local SSD-backed caches with high core counts

### Partial Range Commit Ratio

```yaml
cache:
  # partial_range_commit_ratio: 0.5  # (default)
```

Minimum received fraction (`0.0`–`1.0`) of an incomplete range that the proxy will salvage as a smaller valid range on the read/GET cache-write path. Default `0.5`.

When a streamed range cache write ends with fewer bytes than the requested range — because the client cancelled the transfer or the mid-stream idle watchdog (`connection_pool.upstream_idle_timeout`) aborted the stream — but at least this fraction of the expected bytes were received **in order**, the proxy commits the received prefix as a clamped range `[start, start + received - 1]` instead of discarding the whole range. This lets a single high-throughput download (for example the AWS CLI CRT client, which opens many parallel range connections and closes them as soon as it has the bytes it needs) populate the cache even when some part requests are cut short.

Behavior at the bounds:
- `1.0` — only a fully-received range is committed (legacy behavior; any short range is discarded).
- `0.0` — any non-empty received prefix is committed.
- Below the configured ratio, nothing is committed and the partial bytes are dropped.

A salvaged prefix is recorded with its true byte bounds, so it is never served as if it were the full requested range — a later request for the missing tail fetches it from S3 and merges. The write-through **PUT** cache path is unaffected: it always requires the exact byte count, so a truncated upload is never cached.

**Valid range**: `0.0` to `1.0` inclusive (`NaN` is rejected at startup).

### Consolidation Cycle Timeout

```yaml
cache:
  shared_storage:
    consolidation_cycle_timeout: "30s"  # Default: 30 seconds
```

Maximum duration for the consolidation cycle (discovery + per-key processing + cleanup). When the deadline fires, completed keys are preserved and cleaned up; unprocessed keys are retried next cycle. Discovery is capped at 5000 keys per cycle to limit NFS I/O. Size tracking (accumulator delta collection) runs before the deadline starts and is unaffected.

## Range Request Optimization

### Range Merging

```yaml
cache:
  range_merge_gap_threshold: 1048576  # 1MiB (default)
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
- **Larger threshold** (4MB): Fewer S3 requests, more wasted bandwidth on gaps
- **Default 1MiB**: Good balance for most workloads

**Considerations**:
- Network latency vs bandwidth: Higher latency favors larger threshold
- S3 request costs: Each request has overhead (~50-100ms)
- Bandwidth costs: Larger threshold may fetch already-cached data

## Eviction Configuration

### Eviction Thresholds

```yaml
cache:
  eviction_trigger_percent: 95  # Default: 95% — eviction starts when cache exceeds this
  eviction_target_percent: 80   # Default: 80% — eviction reduces cache to this level
```

**Purpose**: Control when eviction starts and how much space it reclaims.

**How it works**:
- Eviction triggers when cache size exceeds `eviction_trigger_percent` of `max_cache_size`
- Eviction removes entries until cache size drops to `eviction_target_percent` of `max_cache_size`
- The gap between trigger and target creates a buffer to minimize eviction frequency

**Example** (10GB cache, defaults):
- Eviction triggers at 9.5GB (95%)
- Eviction target is 8GB (80%)
- Frees 1.5GB buffer before next eviction needed

**Tuning guide**:
- **Higher target** (85-90%): Less wasted space, more frequent evictions
- **Lower target** (70-75%): More wasted space, fewer evictions
- **Default 80%**: Good balance for most workloads

> **Note**: The `eviction_buffer_percent` field is deprecated and has no effect. Use
> `eviction_trigger_percent` and `eviction_target_percent` instead.

## Multi-Instance Coordination

For scale-out deployments with shared cache storage:

```yaml
cache:
  shared_storage:
    lock_timeout: "60s"                     # Default: 60s. Valid range: 10-300s
    lock_refresh_interval: "30s"            # Default: 30s. Range 5-120s, must be < lock_timeout
    consolidation_interval: "5s"            # Default: 5s. Valid range: 1-60s
    consolidation_size_threshold: 1048576   # Default: 1 MiB. Valid range: 100 KB - 10 MB
    validation_frequency: "23h"             # Default: 23h. Validated 1-168h, but INERT (see below)
    validation_max_duration: "4h"           # Default: 4h. Valid range: 10m-23h
    validation_threshold_warn: 5.0          # Default: 5.0 (percent drift → warn! log)
    validation_threshold_error: 20.0        # Default: 20.0 (percent drift → error! log)
    eviction_lock_timeout: "60s"            # Default: 60s. Valid range: 30-3600s
    lock_max_retries: 5                     # Default: 5
    recovery_max_concurrent: 10             # Default: 10
```

`validation_threshold_warn` and `validation_threshold_error` select a **log level only**,
and only on rolling scans. Neither gates the size correction, which the validation scan
applies unconditionally.

**Note**: Journal-based metadata writes and distributed eviction locking are always enabled for consistency across all deployment modes. There is no `enabled` flag - these features are always active.

### NFS Mount Requirements

Two mount properties are **correctness requirements** for multi-instance coordination:
`lookupcache=pos` so peers' new files are visible, and working cross-host `flock`
(pin `nfsvers=4.1`, never `nolock` or `local_lock=*`). Neither produces an error when
absent — the deployment looks healthy while losing most of its hit rate or corrupting
cache state.

Mount lines for generic NFS, FSx for OpenZFS, and EFS, the differences between the two
managed file systems, why `nconnect` matters on FSx, and how to verify both properties
end to end are in
**[SHARED_STORAGE.md — Mount requirements](SHARED_STORAGE.md#mount-requirements)**.

**Key features**:
- **Atomic metadata writes**: Journal-based updates prevent corruption
- **Distributed eviction**: Only one instance evicts at a time
- **File locking**: Prevents concurrent access conflicts
- **Cache validation**: Cross-instance consistency checks
- **Orphaned range recovery**: Cleanup of incomplete operations

See [SHARED_STORAGE.md](SHARED_STORAGE.md) for how each works and its failure modes.

**Configuration guidelines**:

| Setting | Purpose | Recommendations |
|---------|---------|-----------------|
| `lock_timeout` | Max wait for file locks | Small cache: 60s, Large cache: up to 300s (the validated maximum) |
| `consolidation_interval` | Journal flush frequency | 5s (default), reduce for faster consistency |
| `validation_frequency` | Accepted and validated, but **not read by the scheduler** | Leave at the default; see [Validation Scan](#validation-scan) |
| `validation_max_duration` | Selects the next cycle's scan mode; does **not** bound the scan in progress | 4h (default). Controls automatic full↔rolling mode switching |
| `eviction_lock_timeout` | Distributed eviction timeout | Match lock_timeout for consistency |

### Validation Scan

A daily scan reads every cached `.meta` file and reconciles tracked cache size against the
sizes those files record. It does not stat the range (`.bin`) files.

**Cadence is not configurable.** It fires once per day at midnight local time, plus up to
one hour of random jitter to avoid a thundering herd across instances.
`validation_frequency` (default `23h`, validated 1–168h) is parsed, range-checked, and
logged at startup, but no code path reads it when scheduling the scan — treat it as inert,
like `server.request_timeout`.

```yaml
cache:
  shared_storage:
    validation_max_duration: "4h"  # Default: 4 hours. Valid range: 10m – 23h
```

**`validation_max_duration`** is the single knob. The scan self-tunes between a **full**
mode (all 256 L1 shard directories in parallel) and a **rolling** mode (a subset per
cycle, resuming from a persistent cursor) depending on whether the previous scan fit
inside this budget. No manual mode switching is needed.

It selects the next cycle's mode and does not bound the current one — nothing aborts a scan
in progress. A full scan runs to completion, then warns that it overran, and the following
cycle switches to rolling.

**`validation_threshold_warn`** (default `5.0`) and **`validation_threshold_error`**
(default `20.0`) are drift percentages that select a **log level only**, and they apply to
**rolling scans only**. Neither gates the size correction, which is applied unconditionally
in both modes.

The mode-selection rules, the rolling scan's adaptive batch sizing and cursor persistence,
and what to monitor are in
[SHARED_STORAGE.md — The validation scan](SHARED_STORAGE.md#the-validation-scan).

## Download Coordination

Controls request coalescing for concurrent cache misses:

```yaml
cache:
  download_coordination:
    enabled: true           # Enable/disable coalescing (default: true)
    wait_timeout_secs: 30   # Waiter timeout in seconds (default: 30, range: 5-120)
```

When multiple requests arrive for the same uncached resource (full object, byte range, or part number), only one request fetches from S3 while others wait. Waiters serve from cache after the fetcher completes.

On waiter timeout the proxy does **not** launch an independent duplicate fetch, which would defeat the point of coalescing under load. A timed-out waiter is re-subscribed to the still-in-flight fetch, up to `download_coordination.max_waiter_resubscriptions` (default 3); once that budget is exhausted the waiter receives HTTP 504.

Disable for single-instance deployments with no concurrent duplicate requests, or when debugging cache behavior.

See [CACHING.md - Download Coordination](CACHE_READ_PATHS.md#download-coordination) for details.

## Cache Size Tracking

Each instance keeps an in-memory `AtomicI64` accumulator of size changes at write and
eviction time, flushes it to a per-instance delta file, and a consolidator sums the deltas
into `size_tracking/size_state.json`. Eviction triggers off that consolidated figure.

The mechanism, and the concurrent-write over-counting it is subject to on shared storage,
are in [SHARED_STORAGE.md — Size tracking](SHARED_STORAGE.md#size-tracking).

**Configuration**:
```yaml
cache:
  shared_storage:
    consolidation_interval: "5s"     # Default: 5s. Valid range: 1-60s
```

**Other `shared_storage` fields**

**`metadata_lock_timeout_ms`** (`u64`, default `30000` = 30 seconds)

How long a metadata lock held by another instance is honoured before it is treated as
stale. Only wall-clock time can decide staleness for a lock owned by a different host —
a local lock is checked by testing whether the owning PID still exists — so this value
governs cross-host takeover only.

**`orphan_recovery_enabled`** (`bool`, default `true`)

Background sweep that finds range (`.bin`) files with no referencing metadata — left by
crashed writers or consolidation lag — and either reconciles them into metadata or
removes them. Scans one shard per cycle to spread I/O.

**`orphan_recovery_interval`** (`Duration`, default `"300s"`, range 60-3600s)

Interval between orphan recovery scans.

**`orphan_scan_timeout`** (`Duration`, default `"30s"`, range 5-300s)

Maximum time spent scanning per cycle, so a long scan cannot block other operations.

**`orphan_max_per_cycle`** (`usize`, default `100`)

Maximum orphaned range files handled per cycle, bounding the I/O one sweep can generate.

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
  threshold: 1024                    # Size floor in bytes; smaller writes always skip compression
  preferred_algorithm: "lz4"         # Options: lz4 (parsed but otherwise inert)
```

**`threshold`** (`usize`, default `1024`) — size floor in bytes. Writes smaller than
this always skip compression, regardless of extension or any `cache_rules.json`
override.

**`preferred_algorithm`** (`String`, default `"lz4"`) — parsed and stored per entry, but
otherwise inert; LZ4 is the only implemented algorithm. Changing it does not invalidate
existing cache entries, which continue to be read with the algorithm recorded in their
own metadata.

**Removed**: `compression.content_aware` never had any effect. It still parses via a
deprecation alias and is ignored, with a startup warning when present.

### Content-Aware Compression

Content-aware filtering is always active. When no `cache_rules.json` rule sets
`compression_enabled` for a key, already-compressed formats (images, video, audio,
archives, office documents, app bundles, fonts, embedded databases, installers) skip the
LZ4 block compressor and are written as checksummed store-mode frames instead. Text-like
content is compressed.

**[COMPRESSION.md](COMPRESSION.md#built-in-denylist) owns the authoritative
extension list**, the store-mode contract, and the extension-matching rules that make
`archive.tar.gz` match through the `gz` arm. A `cache_rules.json` rule setting
`compression_enabled` overrides the denylist in either direction for matching keys (see
[Cache Rules](#cache-rules)).

## Connection Pooling

```yaml
connection_pool:
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "55s"        # Just under S3's ~60s server-side timeout

  # HTTP Connection Keepalive
  keepalive_enabled: true
  max_idle_per_host: 100
  max_lifetime: "300s"
  pool_check_interval: "10s"

  # TCP-level keepalive socket options
  keepalive_idle_secs: 15     # TCP_KEEPIDLE
  keepalive_interval_secs: 5  # TCP_KEEPINTVL
  keepalive_retries: 3        # TCP_KEEPCNT

  # TCP receive buffer (SO_RCVBUF). null = kernel auto-tuning. See warning below.
  # tcp_recv_buffer_size: null

  # Endpoint registration cap
  # max_registered_endpoints: 10000

  # Per-IP health exclusion and recovery probing.
  # An IP is dropped from round-robin after ip_failure_threshold consecutive
  # failures and returns only when a recovery probe (TCP connect + TLS handshake)
  # succeeds — a DNS refresh does not restore it. Probes run on the
  # pool_check_interval tick once the cooldown has elapsed; each failed probe
  # doubles that IP's cooldown up to the maximum.
  # ip_failure_threshold: 3
  # health_probe_initial_cooldown: "5s"
  # health_probe_max_cooldown: "300s"

  # Upstream timeout (stalled-response fast-fail)
  # upstream_first_byte_timeout: "5s"
  # upstream_idle_timeout: "5s"
  # upstream_idle_retries: 2
  
  # Custom DNS servers (optional)
  # dns_servers: ["8.8.8.8", "1.1.1.1"]
```

**`max_registered_endpoints`** (`usize`, default `10000`)

Maximum number of DNS-resolved endpoints the connection pool will track. When the cap is reached, new endpoint registrations are rejected with a warning log; existing endpoints continue operating normally. This prevents unbounded memory growth from excessive unique hostnames in forward-proxy mode. Normal S3 usage (a small set of regional endpoints) is well under the default cap.

### TCP Socket Options

These map directly onto socket options on each upstream connection. They are distinct
from `keepalive_enabled` and `max_idle_per_host`, which govern HTTP-level connection
reuse. See [CONNECTION_POOLING.md](CONNECTION_POOLING.md) for how the two layers interact.

**`keepalive_idle_secs`** (`u64`, default `15`) — `TCP_KEEPIDLE`. Idle seconds before the
kernel starts sending keepalive probes.

**`keepalive_interval_secs`** (`u64`, default `5`) — `TCP_KEEPINTVL`. Seconds between
probes.

**`keepalive_retries`** (`u32`, default `3`) — `TCP_KEEPCNT`. Unanswered probes before the
connection is declared dead. At the defaults a dead peer is detected in roughly
15 + (5 × 3) = 30 seconds.

**`tcp_recv_buffer_size`** (`Option<usize>`, default `null`) — `SO_RCVBUF` hint in bytes.

> **Leave this unset unless you have a specific reason.** `null` lets the kernel
> auto-tune the receive window (DRS). Pinning an explicit value **disables**
> auto-tuning, which caps single-stream throughput on high-bandwidth-delay-product
> paths — the buffer becomes the ceiling on in-flight bytes, so throughput is bounded
> by `buffer / RTT` no matter how much bandwidth is available. A value chosen for a
> low-RTT path will throttle a high-RTT one.

### Upstream Timeout (Stalled-Response Fast-Fail)

These settings control how quickly the proxy detects and recovers from a stalled upstream S3 connection. They apply only after download-coordinator acquisition — coordination waits (coalesced requests waiting for another request's fetch) are unaffected.

```yaml
connection_pool:
  # upstream_first_byte_timeout: "5s"
  # upstream_idle_timeout: "5s"
  # upstream_idle_retries: 2
```

**`upstream_first_byte_timeout`** (`Duration`, default `"5s"`)

How long the proxy waits after connecting before receiving the first response byte from upstream. On timeout (before any bytes are sent to the client), the fetch is retried up to `upstream_idle_retries` times using connection-pool/IP failover. After exhausting retries, an error is returned. Set above cross-region TTFB tails (~3s) but well below the 60s client read-timeout so the proxy recovers before the client gives up.

**`upstream_idle_timeout`** (`Duration`, default `"5s"`)

Mid-stream idle watchdog: if no bytes are received from upstream for this duration after data has started flowing, the proxy terminates the client connection so the client's own retry logic engages. A slow-but-steady transfer (bytes arriving within the timeout window) is never aborted — the criterion is "no bytes for T", not "transfer took longer than T". The timer is paused under client backpressure (when the proxy is not reading from upstream because the client write half is slow).

**`upstream_idle_retries`** (`usize`, default `2`)

Pre-stream retry budget. Number of times to retry the upstream fetch when `upstream_first_byte_timeout` fires before any bytes have been sent to the client. Uses the existing connection-pool IP failover for each retry. After exhausting retries, the proxy returns an error response rather than hanging.

### Hedged Requests Governor

Fleet-wide cost cap for hedged upstream requests. Hedging itself is enabled per key pattern in `cache_rules.json` via `hedging_enabled`, `hedge_trigger_after`, and `hedge_max_per_request` — there is no fleet-wide on/off toggle here. This section only limits the per-instance ratio of in-flight hedges to in-flight fetches, bounding cost amplification across all hedging-enabled keys.

```yaml
connection_pool:
  hedged_requests:
    max_inflight_fraction: 0.1
```

**`max_inflight_fraction`** (`f64`, default `0.1`)

Maximum fraction of in-flight upstream fetches that may be hedges. Valid range `[0.0, 1.0]`. When `(in_flight_hedges + 1) / max(in_flight_fetches, 1)` exceeds this value, new hedges are suppressed and the original fetch is served alone.

**The first hedge is always admitted** when no other hedge is in flight ("first-is-free"), so a low-concurrency workload still hedges. Consequently **`0.0` does not disable hedging** — removing `hedging_enabled` from the rules is the only complete off switch.

The cap is per-instance; each proxy enforces it independently with no cross-instance coordination.

See [HEDGING.md](HEDGING.md) for the mechanism, the rule fields, and how to tell whether hedging is earning its cost.

### Connection Keepalive

Reuses TCP/TLS connections so repeat requests skip the handshake. The latency and
throughput effect, and how the HTTP-level pool interacts with the TCP socket options
above, are covered in [CONNECTION_POOLING.md](CONNECTION_POOLING.md).

**Tuning guide**:

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| max_idle_per_host | Less memory/FDs | More concurrent reuse | 100 (default), reduce for memory-constrained environments |
| max_lifetime | More frequent rotation | Less overhead | 300s (5 min) for stable endpoints. 60-120s if DNS changes often; up to 3600s for very stable endpoints |
| pool_check_interval | More responsive cleanup, and excluded IPs probed sooner | Less CPU overhead | 10s for balanced performance |
| health_probe_initial_cooldown | Faster recovery from a transient failure | Fewer probes against a genuinely dead IP | 5s (default); raise it if brief S3 blips cause probe churn |
| health_probe_max_cooldown | An unreachable IP is retried more often | Less wasted work on a dead IP | 300s (default); the doubling backoff reaches it after ~6 failed probes from 5s |

Set `keepalive_enabled: false` to disable connection reuse (useful for debugging
connection issues).

## DNS Server Configuration

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
- S3 PrivateLink (interface VPC endpoints), for on-premises proxies only — see below. Proxies running in the VPC that holds the endpoint should use `endpoint_overrides` instead, because any resolver in that VPC answers from the hosted zone that routes clients to the proxy.

Whichever resolver you configure, it must not be one that resolves S3 hostnames to the proxy itself. That is the same override clients rely on, and pointing the proxy at it creates a loop.

## S3 PrivateLink (Interface VPC Endpoints)

With S3 interface VPC endpoints, the proxy must resolve S3 to the endpoint's ENI IPs
rather than public S3 IPs. The default external resolvers (Google, Cloudflare) return
public IPs and bypass PrivateLink entirely.

**Which mechanism to use depends on where the proxy runs**, and the two cases need
opposite settings for private DNS on the endpoint — one requires it disabled, the other
requires it enabled. That decision, the DNS-collision and resolution-loop reasoning
behind it, and the verification steps are in
[GETTING_STARTED.md — S3 PrivateLink](GETTING_STARTED.md#s3-privatelink-interface-vpc-endpoints).
Read that first; this section is the field reference.

### `endpoint_overrides`

Maps S3 hostnames directly to PrivateLink ENI IPs, taking DNS out of the path.

```yaml
connection_pool:
  endpoint_overrides:
    # Exact match — single hostname
    "s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]

    # Suffix (wildcard) match — all virtual-hosted bucket hostnames in a region
    "*.s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]

    # MRAP global endpoint (requires the com.amazonaws.s3-global.accesspoint VPCE)
    "*.accesspoint.s3-global.amazonaws.com": ["10.0.3.100"]

    # Regional access points
    "*.s3-accesspoint.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]
```

**Matching precedence**: keys starting with `*.` are suffix patterns matching any hostname
ending with that suffix. Exact matches take precedence; among suffix matches the longest
wins. The proxy load-balances across the listed IPs.

**TLS version**: when any `endpoint_overrides` are configured, outbound TLS is locked to
1.2, because interface endpoints do not support 1.3. Regular S3 endpoints support 1.2, so
there is no functional regression.

**Scope**: overrides apply to both the HTTP caching path and the HTTPS passthrough handler
on port 443. `dns_servers` does not cover the passthrough handler.

**No DNS refresh**: an ENI IP excluded by health tracking is not restored until restart,
because static overrides have no refresh cycle. See
[PrivateLink (endpoint_overrides) Interaction](#privatelink-endpoint_overrides-interaction).

### `dns_servers` with Route 53 Resolver

```yaml
connection_pool:
  dns_servers: ["10.0.1.50", "10.0.2.50"]  # Route 53 Resolver inbound endpoint IPs
```

For the on-premises topology only, and it requires private DNS **enabled** on the
endpoint. Do not point this at the VPC resolver when the proxy runs inside the endpoint's
VPC, and never at your on-prem resolver: both return the proxy's own address and form a
loop. See the GETTING_STARTED section linked above.

## Upstream Transport Overrides

Front an S3-compatible store reached on a non-standard transport — plaintext HTTP, or TLS on a port other than 443 — without weakening egress to every other destination. `connection_pool.upstream_overrides` maps a specific upstream `host:port` to the transport the proxy uses to connect.

The field is empty by default. With no overrides, every caching-egress connection uses verified TLS on port 443.

```yaml
connection_pool:
  upstream_overrides:
    # Plaintext local store (e.g. MinIO/RustFS on :9000) — cleartext, dev only
    "127.0.0.1:9000": { scheme: http }

    # Customer store with a publicly-trusted cert on a non-443 port — validated HTTPS
    "store.example.com:9000": { scheme: https, validate_tls: true }

    # Self-signed store in a trusted network — HTTPS, no cert verification
    "store.local:9000": { scheme: https, validate_tls: false }
```

### Key and value shape

Each key is `"host:port"`. The port is part of the key, so the same host on different ports can resolve to different transports (e.g. `host:8080` plaintext and `host:9000` validated TLS). Each value is `{ scheme: http|https, validate_tls: <bool> }`.

### Three transport modes

| `scheme` | `validate_tls` | Transport | Waives a protection? |
|----------|----------------|-----------|----------------------|
| `http`   | (ignored)      | Plaintext HTTP, no TLS handshake | yes — cleartext |
| `https`  | `true` (default) | HTTPS verified against the system trust store | no |
| `https`  | `false`        | HTTPS with no certificate verification (no chain or hostname check) | yes — no MITM protection |

`validate_tls` defaults to `true` when omitted — secure by default; skipping validation is an explicit opt-in. There is no custom-CA mode: validation is either full (system trust store) or none.

### Host matching: DNS names cover subdomains

- An **IP literal** matches that IP exactly (no subdomain semantics; IP endpoints use path-style only).
- A **DNS name** matches itself and any subdomain. `store.local:9000` covers `store.local:9000`, `bucket.store.local:9000`, and `my.bucket.store.local:9000`, so one entry serves both path-style and virtual-hosted addressing of a store.
- The port must match. Matching is case-insensitive.
- Where both an exact and a DNS-suffix entry match, the exact match wins; among suffix matches the longest (most specific) wins.
- A bare `*` matcher is rejected (it would match link-local and internal hosts); the entry is skipped with a warning and does not abort startup.

Hostnames resolve via the proxy's own configured `dns_servers`, not the host's default system resolver — the same resolver the rest of the egress uses. A publicly-resolvable name such as `s3.<region>.amazonaws.com` therefore resolves to its real address — you can override real S3 to plaintext on port 80 the same way you override a local store.

**Blast radius when testing this feature**: because a DNS-name override matches every subdomain at that port, one override entry (e.g. `s3.us-west-2.amazonaws.com:80`) shadows both path-style and virtual-hosted requests to that entire Region on that port. That is wider than it looks when testing: once such an entry exists, no request to that Region on that port can reach the default (no-override) transport path, because every one of them matches the override. So a test intended to exercise the default path — verified TLS on 443 with a distributed IP — has to target a host the override does not cover, for example a bucket in a different Region. Otherwise the test silently exercises the override arm and reports success for the wrong reason.

### Security implications

The plaintext and unvalidated-TLS modes **waive a security protection**:

- `scheme: http` exposes the proxy→origin traffic in cleartext.
- `validate_tls: false` removes MITM protection on the proxy→origin leg — the proxy accepts any certificate.

Use these modes for **local development or trusted networks only**. Do not enable them across untrusted networks. The proxy logs a startup warning naming each endpoint and the specific protection waived. Validated HTTPS (`scheme: https` with `validate_tls: true`) waives nothing and is not warned.

When a `validate_tls: true` upstream presents a certificate that fails verification, the proxy returns a non-retryable HTTP 400 with an `UpstreamTLSValidationFailed` error naming the `host:port`. It never falls back to plaintext or unvalidated TLS.

### Standard mode and non-standard upstream ports

The upstream port comes from the request the proxy receives, and the two modes obtain it differently:

- **Forward-proxy mode** receives an absolute-form request whose authority carries the upstream `host:port`, so the proxy dials whatever port the client targets — including a non-standard one.
- **Standard (hosts-file) mode** receives an origin-form request and takes the upstream port from the `Host` header, defaulting to 80 (the caching-egress origin port) when the header carries none. It accepts client connections only on its configured `http_port` (default 80) and `https_port` (default 443, TCP passthrough). Hosts-file interception rewrites only the IP, not the port, so a client aimed at a non-standard port connects to that port directly and is not intercepted by the proxy's standard-mode listeners.

The transport itself — plaintext, validated TLS, or unvalidated TLS — is selectable by configuration on whatever port the request targets, in both modes.

Standard mode *can* front a non-standard port — but only the single port it is bound to. Set `http_port` to that port and the proxy intercepts it: the client connects there, the `Host` header carries it, and an override keyed on that `host:port` matches. What standard mode cannot do is intercept more than one upstream port at a time, or any port other than its configured `http_port`/`https_port` (the 443 listener is TCP passthrough and does not cache). To front several ports, or to leave the listener on 80/443 while reaching a store on another port, use forward-proxy mode, where each request's authority carries the upstream `host:port`.

In either mode the proxy resolves the upstream hostname with its own configured `dns_servers`, not the host's default system resolver. That separation is what avoids a loop: the routing that sends traffic to the proxy (a client `/etc/hosts` entry, a local DNS zone, or `HTTP_PROXY`) points the name at the proxy, while the proxy's own resolver must return the real origin. If the proxy's `dns_servers` resolve the name back to the proxy, egress loops.

### Relationship to `endpoint_overrides`

`upstream_overrides` (how to connect — scheme and validation) is independent of `endpoint_overrides` (what IP a hostname resolves to, plus its TLS-1.2 lock). Reach a store on a non-default transport with `upstream_overrides` alone — using an IP-literal key keeps the TLS-1.2 lock off, since that lock applies only to hosts listed in `endpoint_overrides`. The two combine only when you must address a store by a **hostname** the proxy's own DNS can't resolve (for example a private name needed for TLS SNI or certificate matching): `endpoint_overrides` pins the IP and `upstream_overrides` sets the port and transport. Note that any host in `endpoint_overrides` is TLS-1.2-locked, so a validated-TLS upstream override on such a host negotiates TLS 1.2.

### Cache-key note

The cache key is built from the host (with the port stripped) and the object path — it ignores both the port and the transport mode. So two *different* stores on the same host but different ports (for example `host:9000` and `host:9001`) write into the **same** cache namespace: an object at the same path on both collides, and whichever was fetched first is served until it expires or is invalidated. To front two distinct stores, give them distinct hostnames (or distinct key paths) — don't rely on the port to tell them apart.

## IP Distribution

Distributes outgoing S3 connections across all resolved IP addresses for an endpoint. By default, hyper pools connections by hostname, so all requests share one pool regardless of how many IPs DNS returns. IP distribution rewrites each request's URI authority to a specific IP, causing hyper to create separate per-IP connection pools.

```yaml
connection_pool:
  ip_distribution_enabled: true   # Per-IP connection pools (default: true)
  max_idle_per_ip: 10             # Idle connections per IP pool (default: 10, range: 1-100)
```

### When to Enable

Enable for high-throughput, highly concurrent workloads. A single HTTP/1.1 connection to S3 tops out at ~90 MB/s, so aggregate throughput comes from running many connections in parallel. Distributing those connections across S3's frontend IPs spreads them over multiple frontend servers instead of concentrating them on one, which avoids per-IP connection limits and throttling. It does not change the ~90 MB/s per-connection cap — it lets more connections run at that speed at once.

### IP Distribution: How It Works

1. The `ConnectionPoolManager` resolves S3 endpoint IPs via DNS (or uses `endpoint_overrides`)
2. The `IpDistributor` selects a target IP using round-robin for each outgoing request
3. The request URI authority is rewritten from hostname to IP (e.g., `s3.eu-west-1.amazonaws.com` → `52.92.17.224`)
4. Hyper creates a separate connection pool for each distinct IP
5. TLS SNI and the Host header retain the original hostname, preserving AWS SigV4 signature validity

### Connection Capacity

When IP distribution is enabled, `max_idle_per_ip` replaces `max_idle_per_host` for the hyper client. Total idle connections across all IPs:

```
total_idle = number_of_IPs × max_idle_per_ip
```

S3 typically returns ~8 IPs per endpoint, so the default of 10 yields ~80 total idle connections. Adjust `max_idle_per_ip` based on your throughput needs:

| IPs | max_idle_per_ip | Total Idle |
|-----|-----------------|------------|
| 4   | 10              | 40         |
| 8   | 10              | 80         |
| 8   | 25              | 200        |
| 16  | 5               | 80         |

### Graceful Degradation

- If no IPs are available (DNS not yet resolved, all IPs unhealthy), the proxy falls back to hostname-based routing matching the default behavior
- If URI rewriting fails for a request, the proxy forwards using the original hostname and logs a warning
- During startup before the first DNS resolution completes, requests use hostname-based routing

### Compatibility

- Works with both DNS-resolved IPs and static `endpoint_overrides` (PrivateLink)
- Preserves TLS SNI (original hostname) for successful TLS handshakes
- Preserves Host header for AWS SigV4 signature validity
- IP set updates automatically on DNS refresh; stale IPs are removed within one refresh cycle

### PrivateLink (endpoint_overrides) Interaction

When `endpoint_overrides` provides the IP set, IP distribution round-robins across the listed ENI IPs identically to DNS-resolved IPs. Two differences from DNS-based distribution:

1. **Eager initialization**: Exact-match overrides seed the distributor at startup. Suffix (`*.`) overrides create distributors lazily on first matching request.
2. **Health exclusion is permanent until restart**: DNS-refreshed endpoints automatically restore excluded IPs every `dns_refresh_interval`. Static overrides have no DNS refresh, so an excluded ENI IP stays excluded until the proxy restarts. If you have only 1–2 ENI IPs, a transient network issue could exclude all of them — the proxy falls back to hostname-based routing (which still works, but loses per-IP pool separation). Monitor the health check endpoint for excluded IPs in PrivateLink deployments.

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

**Access Log Format**:

The proxy writes access logs in [S3 Server Access Log format](https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html), so existing log analysis tools, Athena queries, and scripts that parse S3 Server Access Logs work without modification.

**Access Log Modes**:
- **all**: Log all requests (cache hits and misses). Provides a complete audit trail of every request the proxy handles.
- **cached_only**: Log only requests served from cache. These requests never reach S3, so they don't appear in S3 Server Access Logs. This mode captures the requests that would otherwise have no audit trail, and is useful when you already have S3 Server Access Logging enabled and want to avoid duplicating entries for cache misses (which S3 logs directly).

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

### Log Retention and Rotation

```yaml
logging:
  access_log_retention_days: 30              # Default: 30, range: 1-365
  app_log_retention_days: 30                 # Default: 30, range: 1-365
  log_cleanup_interval: "24h"                # Default: 24h, range: 1h-7d
  access_log_file_rotation_interval: "5m"    # Default: 5m, range: 1m-60m
```

**Retention**: Each proxy instance independently deletes log files older than the configured retention period. No inter-instance coordination is needed on shared storage — each instance manages its own log files.

**Cleanup interval**: How often the background cleanup task scans for and removes expired log files. The default of 24h is sufficient for most deployments; reduce for tighter disk usage control.

**File rotation**: Access log files rotate on this interval — a new file is created every `access_log_file_rotation_interval`, allowing retention cleanup to operate at file granularity. Shorter intervals produce more files but enable finer-grained cleanup; longer intervals reduce file count but may retain more data than the retention period strictly requires.

**Tuning Guide**:

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| `access_log_retention_days` | Less disk usage | Longer audit trail | 30 days for most deployments |
| `app_log_retention_days` | Less disk usage | Longer debug history | 30 days for most deployments |
| `log_cleanup_interval` | More frequent cleanup, more I/O | Less I/O, slower reclaim | 24h for most workloads |
| `access_log_file_rotation_interval` | More files, finer cleanup granularity | Fewer files, coarser cleanup | 5m default; increase on high-request-rate deployments |

## Metrics Configuration

```yaml
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  bind_address: "0.0.0.0"   # Default: all interfaces. Use "127.0.0.1" for loopback-only.
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

### Per-Bucket Traffic Metrics

The proxy tracks cumulative per-bucket bandwidth and request counters across all serving paths. In-memory accounting and local observability (`/metrics` JSON endpoint and dashboard) are always active. OTLP export is opt-in.

```yaml
metrics:
  # Export gate for per-bucket OTLP observable counters (default: false).
  # In-memory accounting and /metrics + dashboard views are always active.
  otlp:
    per_bucket_enabled: false

  # Always-on accounting knobs.
  per_bucket:
    max_series: 100               # Max distinct bucket[+prefix] series (default: 100).
                                  # Beyond this, traffic folds into an "__other__" overflow entry.
    bucket_prefixes:              # Optional per-bucket prefix lists for prefix-level attribution.
      my-bucket:                  # Key: bucket name. Value: list of prefix strings.
        - "logs/"
        - "data/raw/"
```

**`metrics.otlp.per_bucket_enabled`** (bool, default `false`)

Enable per-bucket OTLP cumulative counter export. When true, the proxy emits four `ObservableCounter<u64>` instruments per collection cycle — `s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, and `s3proxy.put_requests` — each with a `bucket` attribute and, when prefix attribution is active, a `prefix` attribute. Metric names mirror S3's `BytesDownloaded`/`BytesUploaded`/`GetRequests`/`PutRequests` for direct CloudWatch comparison. Only object reads (GET) and object/part writes (PUT/UploadPart) are tracked.

**`metrics.per_bucket.max_series`** (usize, default `100`)

Maximum number of distinct bucket+prefix series tracked in memory. Once the cap is reached, traffic from new buckets or prefixes is folded into a synthetic `__other__` series (visible in `/metrics` and the dashboard) rather than creating a new entry. Total traffic is conserved; only the series key changes. A `warn!` is logged once on first overflow.

**`metrics.per_bucket.bucket_prefixes`** (map, default `{}`)

Optional per-bucket prefix lists for prefix-level attribution. When configured, requests are attributed to the longest matching prefix. Requests with no matching prefix — or when the bucket has no configured prefixes — are attributed at the bucket level. Empty-string prefixes are rejected with a warning at config load.

## Dashboard Configuration

A read-only web interface for cache statistics, a log viewer, and system info.
**[DASHBOARD.md](DASHBOARD.md) is the guide** — features, API endpoints, deployment
patterns, security posture, and troubleshooting. This section is the field reference only.

```yaml
dashboard:
  enabled: true                        # Default: true
  port: 8081                           # Default: 8081
  bind_address: "127.0.0.1"            # Default: 127.0.0.1 (loopback only)
  cache_stats_refresh_interval: "5s"   # Default: 5s. Valid range: 1-300s
  logs_refresh_interval: "10s"         # Default: 10s. Valid range: 1-300s
  max_log_entries: 100                 # DEPRECATED: parsed and validated, but inert
```

**`enabled`** (`bool`, default `true`) — set `false` to not start the dashboard server.

**`port`** (`u16`, default `8081`) — chosen to avoid the health server's 8080. Must differ
from `health.port`, `metrics.port`, and `tls.tls_proxy_port`.

**`bind_address`** (`String`, default `"127.0.0.1"`) — **loopback only by default**, unlike
the health and metrics servers which default to `0.0.0.0`. The dashboard is
unauthenticated, so binding it more widely makes cache statistics and recent application
log lines readable by anyone who can reach the port. See
[DASHBOARD.md — Security Considerations](DASHBOARD.md#security-considerations) for the
SSH-tunnel pattern and what the interface exposes.

**`cache_stats_refresh_interval`** (`Duration`, default `"5s"`, range 1-300s) — browser
poll interval for the statistics cards.

**`logs_refresh_interval`** (`Duration`, default `"10s"`, range 1-300s) — browser poll
interval for the log viewer.

**`max_log_entries`** (`usize`, default `100`, range 10-10000) — **deprecated and inert**.
Parsed, range-validated, and logged at startup, but the log viewer does not use it to cap
output. It will be removed in a future release.

Environment overrides exist for every field above except `max_log_entries`' effect; see
[Environment Variable Reference](#environment-variable-reference) for the `DASHBOARD_*`
entries.

**Resource cost**: under 10 MB of additional memory, 50 concurrent connections accepted,
and no measurable impact on proxy request handling. Details in
[DASHBOARD.md — Performance Impact](DASHBOARD.md#performance-impact).

## Health Check Configuration

```yaml
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  bind_address: "0.0.0.0"   # Default: all interfaces. Use "127.0.0.1" for loopback-only.
  check_interval: "30s"
```

**Endpoints**:
- Health check: `http://localhost:8080/health`
- Metrics: `http://localhost:9090/metrics`

Each dedicated listener accepts only `GET` on its configured `endpoint`. A different path
returns `404 Not Found`; another method on the configured path returns `405 Method Not
Allowed` with `Allow: GET`. Change `endpoint` if another path is required.
- Dashboard: `http://localhost:8081`
- TLS proxy: `https://localhost:3129` (when enabled)

## HTTPS Passthrough

HTTPS requests on port 443 are handled via TCP passthrough — the proxy tunnels the encrypted connection directly to S3 without terminating TLS or caching.

**How it works**:
- HTTPS traffic (port 443) is tunneled directly to S3
- No caching occurs for HTTPS requests
- No certificate management needed for this port
- Transparent to clients

**Note**: For encrypted client-to-proxy connections with caching, use the [TLS Proxy](#tls-proxy-configuration) on port 3129 instead.

**Configuration**: HTTPS passthrough is automatically enabled when `https_port` is configured:

```yaml
server:
  https_port: 443  # Enable HTTPS passthrough on port 443
```

**Note**: Only HTTP (port 80) and TLS proxy (port 3129) requests are cached. HTTPS passthrough (port 443) bypasses the cache entirely. To ensure caching benefits:
- Set `HTTP_PROXY=https://proxy:3129` for encrypted caching via the TLS proxy listener (recommended)
- Set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` for AWS CLI with DNS routing (works for buckets in that region)
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

Environment variables override the YAML file for a **subset** of options. This table is
the complete set — 33 variables. Anything not listed here has no environment override
and must be set in the config file.

Two things to know before using these:

- **Durations take a bare integer, not a duration string.** `LOCK_TIMEOUT=60` is 60
  seconds; `LOCK_TIMEOUT=60s` does not parse. Units are seconds everywhere except
  `VALIDATION_FREQUENCY`, which is in hours.
- **A value that fails to parse is silently ignored**, leaving the YAML or default value
  in place. There is no warning. Check the startup config log line if an override does
  not appear to take effect.

| Variable | Configuration Path | Example |
|----------|-------------------|---------|
| `HTTP_PORT` | `server.http_port` | `HTTP_PORT=80` |
| `HTTPS_PORT` | `server.https_port` | `HTTPS_PORT=443` |
| `MAX_CONCURRENT_REQUESTS` | `server.max_concurrent_requests` | `MAX_CONCURRENT_REQUESTS=1000` |
| `CACHE_DIR` | `cache.cache_dir` | `CACHE_DIR=/var/cache/s3-proxy` |
| `RAM_CACHE_ENABLED` | `cache.ram_cache_enabled` | `RAM_CACHE_ENABLED=true` |
| `WRITE_CACHE_ENABLED` | `cache.write_cache_enabled` | `WRITE_CACHE_ENABLED=true` |
| `PARALLEL_SCAN` | `cache.initialization.parallel_scan` | `PARALLEL_SCAN=true` |
| `PROGRESS_LOGGING` | `cache.initialization.progress_logging` | `PROGRESS_LOGGING=true` |
| `SCAN_TIMEOUT` | `cache.initialization.scan_timeout` | `SCAN_TIMEOUT=300` (seconds) |
| `LOCK_TIMEOUT` | `cache.shared_storage.lock_timeout` | `LOCK_TIMEOUT=60` (seconds) |
| `LOCK_REFRESH_INTERVAL` | `cache.shared_storage.lock_refresh_interval` | `LOCK_REFRESH_INTERVAL=30` (seconds) |
| `LOCK_MAX_RETRIES` | `cache.shared_storage.lock_max_retries` | `LOCK_MAX_RETRIES=5` |
| `CONSOLIDATION_INTERVAL` | `cache.shared_storage.consolidation_interval` | `CONSOLIDATION_INTERVAL=5` (seconds) |
| `CONSOLIDATION_SIZE_THRESHOLD` | `cache.shared_storage.consolidation_size_threshold` | `CONSOLIDATION_SIZE_THRESHOLD=1048576` (bytes) |
| `EVICTION_LOCK_TIMEOUT` | `cache.shared_storage.eviction_lock_timeout` | `EVICTION_LOCK_TIMEOUT=60` (seconds) |
| `RECOVERY_MAX_CONCURRENT` | `cache.shared_storage.recovery_max_concurrent` | `RECOVERY_MAX_CONCURRENT=10` |
| `VALIDATION_FREQUENCY` | `cache.shared_storage.validation_frequency` | `VALIDATION_FREQUENCY=23` (**hours**; the field is inert, see [Validation Scan](#validation-scan)) |
| `VALIDATION_THRESHOLD_WARN` | `cache.shared_storage.validation_threshold_warn` | `VALIDATION_THRESHOLD_WARN=5.0` |
| `VALIDATION_THRESHOLD_ERROR` | `cache.shared_storage.validation_threshold_error` | `VALIDATION_THRESHOLD_ERROR=20.0` |
| `COMPRESSION_ENABLED` | `compression.enabled` | `COMPRESSION_ENABLED=true` |
| `ACCESS_LOG_DIR` | `logging.access_log_dir` | `ACCESS_LOG_DIR=/var/log/s3-proxy/access` |
| `APP_LOG_DIR` | `logging.app_log_dir` | `APP_LOG_DIR=/var/log/s3-proxy/app` |
| `LOG_LEVEL` | `logging.log_level` | `LOG_LEVEL=info` |
| `HEALTH_ENABLED` | `health.enabled` | `HEALTH_ENABLED=true` |
| `METRICS_ENABLED` | `metrics.enabled` | `METRICS_ENABLED=true` |
| `OTLP_ENDPOINT` | `metrics.otlp.endpoint` (also sets `otlp.enabled=true`) | `OTLP_ENDPOINT=http://localhost:4318` |
| `OTLP_EXPORT_INTERVAL` | `metrics.otlp.export_interval` | `OTLP_EXPORT_INTERVAL=60` (seconds) |
| `DASHBOARD_ENABLED` | `dashboard.enabled` | `DASHBOARD_ENABLED=true` |
| `DASHBOARD_PORT` | `dashboard.port` | `DASHBOARD_PORT=8081` |
| `DASHBOARD_BIND_ADDRESS` | `dashboard.bind_address` | `DASHBOARD_BIND_ADDRESS=127.0.0.1` |
| `DASHBOARD_CACHE_STATS_REFRESH_INTERVAL` | `dashboard.cache_stats_refresh_interval` | `DASHBOARD_CACHE_STATS_REFRESH_INTERVAL=10` (seconds) |
| `DASHBOARD_LOGS_REFRESH_INTERVAL` | `dashboard.logs_refresh_interval` | `DASHBOARD_LOGS_REFRESH_INTERVAL=30` (seconds) |
| `DASHBOARD_MAX_LOG_ENTRIES` | `dashboard.max_log_entries` | `DASHBOARD_MAX_LOG_ENTRIES=100` (field is deprecated and inert) |

**No environment override exists** for `cache.max_cache_size`, any `logging.*` field
other than the two directories and the level, any `cache.metadata_cache.*` field, any
TTL, or any `connection_pool.*` field. Earlier revisions of this table listed
`MAX_CACHE_SIZE`, `ACCESS_LOG_FLUSH_INTERVAL`, `ACCESS_LOG_BUFFER_SIZE`,
`ACCESS_LOG_RETENTION_DAYS`, `APP_LOG_RETENTION_DAYS`, `LOG_CLEANUP_INTERVAL`, and
`ACCESS_LOG_FILE_ROTATION_INTERVAL`; none of those are read by the proxy. Set those
fields in the config file.

## Example Configurations

Three complete profiles live in [`docs/examples/`](examples/) as loadable YAML rather
than as prose in this document, so they can be started directly and are load-tested
against the binary:

| File | Profile |
|---|---|
| [`config-development.yaml`](examples/config-development.yaml) | Local cache, debug logging, unprivileged ports, loopback dashboard |
| [`config-production.yaml`](examples/config-production.yaml) | Standard ports, TLS-terminating listener, shared cache volume, OTLP export |
| [`config-high-performance.yaml`](examples/config-high-performance.yaml) | 1 TiB disk / 10 GiB RAM tiers, long-lived connections, slower dashboard polling |

```bash
s3-proxy --config docs/examples/config-development.yaml
```

For an annotated walk through every available option, see
[`config/config.example.yaml`](../config/config.example.yaml).

### Partial sections fail to parse

Four sections require **all** of their fields once the section is present at all:
`compression`, `health`, `metrics`, and `metrics.otlp`. Omitting a section entirely is
fine and accepts every default, but writing it partially fails startup with a
`missing field` error naming a field you did not write:

```
Error: ConfigError("Failed to parse config file config.yaml:
compression: missing field `preferred_algorithm` at line 26 column 3")
```

Either omit the section or write it in full. `config-production.yaml` and
`config-high-performance.yaml` show the complete form for `metrics`/`metrics.otlp` and
`compression` respectively.

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
- Check that `cache_rules.json` rules match the keys you expect (see [Cache Rules](#cache-rules))
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
- Increase `max_idle_per_host` (or `max_idle_per_ip` when IP distribution is enabled)
- Check `dns_refresh_interval`
- Monitor connection pool metrics

### Dashboard Issues

See [DASHBOARD.md — Troubleshooting](DASHBOARD.md#troubleshooting) for the dashboard not
being reachable, port conflicts, and connection limits.

## Download Bandwidth QoS Configuration

Controls the aggregate rate limit on cache-miss origin downloads and per-caller /
per-bucket fairness scheduling.  All fields are optional and default to disabled.

See [BANDWIDTH_QOS.md](BANDWIDTH_QOS.md) for the full guide.

**`download_bandwidth.max_bytes_per_sec`** (u64, default `0`)
- Aggregate cache-miss download ceiling in bytes/s.  `0` = unlimited (feature disabled).

**`download_bandwidth.caller_id.enabled`** (bool, default `false`)
- Use the User-Agent `app/<value>` token as the fairness key when present and valid.

**`download_bandwidth.caller_id.validation_regex`** (string, default `null`)
- Optional regex to validate the caller value; non-matching values fall back to bucket.
- Compiled once at startup — an invalid regex is a fatal startup error.

**`download_bandwidth.caller_id.max_len`** (usize, default `64`)
- Maximum character length for the caller value; longer values fall back to bucket.

**`download_bandwidth.max_tracked_classes`** (usize, default `1024`)
- Top-K cardinality cap for per-class metric series.

**`download_bandwidth.fleet.fallback_instance_count`** (u32, default `1`)
- Instance count used when the live count cannot be determined from shared storage.
- **Fleet operators must set this to the fleet size** to ensure safe throttling under
  coordination loss.

**`download_bandwidth.fleet.instance_staleness`** (duration, default `"30s"`)
- Heartbeat staleness window: instances with files older than this are excluded from
  the live-instance count `N`.

**`download_bandwidth.fleet.refresh_interval`** (duration, default `"30s"`)
- Cold-path cadence for the heartbeat touch and `qos/heartbeats/` readdir.  Floor: 10 s.

## See Also

This document is the field reference. Each topic doc below owns the mechanism and the
interpretation guidance for its own subject, and is authoritative there:

- [CACHING.md](CACHING.md) - Cache architecture, TTL and revalidation flow, range merging, eviction, multi-instance coordination
- [COMPRESSION.md](COMPRESSION.md) - The compression decision, the authoritative denylist, store-mode frames
- [CONNECTION_POOLING.md](CONNECTION_POOLING.md) - Pooling, IP distribution, health tracking, TCP socket options
- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) - Every field in the `/metrics` payload
- [METRICS.md](METRICS.md) - Per-bucket traffic, cache-savings inference, reading the permit and in-flight-memory counters
- [OTLP_METRICS.md](OTLP_METRICS.md) - The OpenTelemetry export surface and backend integration
- [DASHBOARD.md](DASHBOARD.md) - The web interface, its API endpoints, and its security posture
- [BANDWIDTH_QOS.md](BANDWIDTH_QOS.md) - Download ceiling mechanics and fair-share scheduling
- [HEDGING.md](HEDGING.md) - Hedged upstream requests and the cost governor
- [SHARED_STORAGE.md](SHARED_STORAGE.md) - Multi-instance mount requirements, journals, eviction coordination, size tracking
- [ACCESS_LOG_FORMAT.md](ACCESS_LOG_FORMAT.md) - The 25-field access log record
- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Failure classes, recovery paths, and the upstream idle watchdog
- [GETTING_STARTED.md](GETTING_STARTED.md) - Deployment modes, client routing, TLS patterns, PrivateLink topology
- [docs/examples/](examples/) - Three loadable config profiles and worked `cache_rules.json` files
- [config/config.example.yaml](../config/config.example.yaml) - Annotated example covering every option
