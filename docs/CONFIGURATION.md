# Configuration reference

This page lists the configuration fields of Hybrid Cache for Amazon S3, with their defaults, valid ranges and effects. Each section links to the page that explains the behavior behind its fields.

## Table of contents
- [Configuration methods](#configuration-methods)
- [Server configuration](#server-configuration)
- [Cache configuration](#cache-configuration)
- [Time-to-live (TTL) configuration](#time-to-live-ttl-configuration)
- [Write cache configuration](#write-cache-configuration)
- [Cache rules](#cache-rules)
- [Cache expiration scenarios](#cache-expiration-scenarios)
- [RAM-disk cache coherency](#ram-disk-cache-coherency)
- [Cache hit performance tuning](#cache-hit-performance-tuning)
- [Range request optimization](#range-request-optimization)
- [Eviction configuration](#eviction-configuration)
- [Multi-instance coordination](#multi-instance-coordination)
- [Download coordination](#download-coordination)
- [Cache size tracking](#cache-size-tracking)
- [Compression configuration](#compression-configuration)
- [Connection pooling](#connection-pooling)
- [DNS server configuration](#dns-server-configuration)
- [S3 PrivateLink (interface VPC endpoints)](#s3-privatelink-interface-vpc-endpoints)
- [Upstream transport overrides](#upstream-transport-overrides)
- [IP distribution](#ip-distribution)
- [Download bandwidth QoS configuration](#download-bandwidth-qos-configuration)
- [Logging configuration](#logging-configuration)
- [Metrics configuration](#metrics-configuration)
- [Dashboard configuration](#dashboard-configuration)
- [Health check configuration](#health-check-configuration)
- [HTTPS passthrough](#https-passthrough)
- [Duration format](#duration-format)
- [Path expansion](#path-expansion)
- [Environment variable reference](#environment-variable-reference)
- [Example configurations](#example-configurations)
- [Troubleshooting](#troubleshooting)
- [See also](#see-also)
---

## Configuration methods

The proxy reads configuration from three layers, each overriding the one before:

1. The YAML configuration file
2. Environment variables
3. Command-line arguments

Some fields are accepted and have no effect. A config that sets one still loads, and the proxy logs a startup warning naming the field:

- `cache.eviction_buffer_percent`, `connection_pool.max_connections_per_ip`, `dashboard.max_log_entries` and `server.max_buffered_request_body_bytes`, when set to a value other than the default.
- `metrics.include_cache_stats`, `metrics.include_compression_stats` and `metrics.include_connection_stats`, when set to `false`.
- `health.check_interval` and `cache.initialization.parallel_scan`, `scan_timeout` and `progress_logging` (environment variables `PARALLEL_SCAN`, `SCAN_TIMEOUT` and `PROGRESS_LOGGING`; flags `--parallel-scan`, `--no-parallel-scan`, `--scan-timeout`, `--progress-logging` and `--no-progress-logging`), which share one warning logged when any is set to a value other than the default.
- `cache.ram_cache_flush_threshold`, `cache.ram_cache_flush_on_eviction`, `cache.metadata_cache.stale_handle_max_retries`, `cache.shared_storage.consolidation_size_threshold`, `cache.shared_storage.max_cached_objects`, `cache.shared_storage.lock_refresh_interval`, `cache.shared_storage.metadata_lock_timeout_ms`, `server.https_mode` and `compression.content_aware`, when present.
- `cache.shared_storage.recovery_max_concurrent`, when present, and `cache.shared_storage.orphan_recovery_enabled`, `orphan_recovery_interval`, `orphan_scan_timeout` and `orphan_max_per_cycle`, which share one warning logged when any is set to a non-default value. Public 2.8.4 honoured these five ([Configuration fields that are now ignored](UPGRADING.md#configuration-fields-that-are-now-ignored)).

`cache.shared_storage.validation_frequency` is validated and has no effect, without a warning ([Validation scan](#validation-scan)). `cache.shared_storage.consolidation_size_threshold` (100 KiB to 10 MiB) and `dashboard.max_log_entries` (10 to 10,000) have no effect but are still range-checked, and a value outside the range fails startup.

### Loading configuration

```bash
./s3-proxy -c config.yaml  # The configuration file alone
HTTP_PORT=8000 ./s3-proxy -c config.yaml  # An environment variable overrides the file
./s3-proxy -c config.yaml --http-port 8000  # A command-line argument overrides both
```

## Server configuration

### Listeners and ports

| Listener | Port field | Default | What it does |
|---|---|---|---|
| HTTP listener | `server.http_port` | `80` | Caches. Opened in `standard` mode |
| HTTPS passthrough | `server.https_port` | `443` | Forwards the encrypted connection to S3 without caching. Opened in `standard` mode. See [HTTPS passthrough](#https-passthrough) |
| Forward proxy | `server.proxy_port` | `3128` | Caches, for clients that set `HTTP_PROXY`. Opened in `proxy_only` mode |
| TLS proxy listener | `server.tls.tls_proxy_port` | `3129` | Terminates TLS and caches. Opened in either mode when enabled. See [TLS proxy listener configuration](#tls-proxy-listener-configuration) |
| Health | `health.port` | `8080` | Monitoring listener. See [Health check configuration](#health-check-configuration) |
| Dashboard | `dashboard.port` | `8081` | Monitoring listener. See [Dashboard configuration](#dashboard-configuration) |
| Metrics | `metrics.port` | `9090` | Monitoring listener. See [Metrics configuration](#metrics-configuration) |

```yaml
server:
  mode: "standard"                # default; or "proxy_only"
  http_port: 80                   # standard mode
  https_port: 443                 # standard mode
  proxy_port: 3128                # proxy_only mode
  max_concurrent_requests: 1000   # default
```

**`mode`** (default `standard`) selects the client-facing listeners. `standard` opens the HTTP
listener and HTTPS passthrough and ignores `proxy_port`. `proxy_only` opens only the forward
proxy. The TLS proxy listener is available in both modes. The client-facing listeners bind
all interfaces, so restrict their ports with a firewall or security group. Binding a port
below 1024 needs root or the `CAP_NET_BIND_SERVICE` capability. To choose how clients reach
the proxy, see [Configure client routing](GETTING_STARTED.md#3-configure-client-routing).

The health, dashboard and metrics listeners are the monitoring listeners. They are read-only
and bind to `127.0.0.1` unless their `bind_address` field says otherwise. OTLP export, when
enabled, sends to `http://localhost:4318` by default.

#### `request_timeout` is not enforced

`server.request_timeout` (`Duration`, default `30s`) is accepted and validated but has no
effect on request handling. The proxy waits at most 30 seconds for each upstream response
whatever this field says. The upstream timeouts you can set are described under
[Upstream timeout](#upstream-timeout-stalled-response-fast-fail).

#### Max concurrent requests

`server.max_concurrent_requests` (`usize`, default `1000`) caps the requests one proxy
handles at once. Size it from `request_metrics.permits_held_peak` on `/metrics` under
representative load.

##### What this limit actually covers

A request holds its permit until its response body has finished streaming to the client or
the connection drops, and a background cache commit holds a share of it until the commit
completes. The field therefore bounds concurrent transfers, and many large, slow transfers
reach it sooner than their request rate suggests.

A request over the limit is rejected at once with HTTP 503 `SlowDown` and a `Retry-After`
header; it is never queued. AWS SDKs retry 503 responses with backoff.

#### Memory impact

A streamed transfer uses about 5 MiB of memory whatever the object size:

- Cache hit: a 1 MiB decompression buffer plus up to four queued 1 MiB chunks.
- Cache miss: a 1 MiB receive buffer plus up to 4 MiB of pending cache writes.

A range file written with 4 MiB LZ4 blocks holds about 8.25 MiB of decoder state per hit, which these figures omit. The proxy writes 64 KiB blocks and reads the larger ones; see [Compression](COMPRESSION.md).

Size three memory budgets together. They add up, none covers another, and nothing enforces a
combined ceiling:

1. Streamed transfers: at most `max_concurrent_requests` × about 5 MiB per proxy.
2. Buffered bodies, on the paths that hold a whole body or range in memory: bounded by
   [`server.max_inflight_buffer_bytes`](#in-flight-memory-ceiling). At its default of `0`
   this budget has no bound.
3. The RAM cache: `cache.max_ram_cache_size`, default 512 MiB (`536870912`).

Size the instance from its measured peak resident memory under representative load.

### In-flight memory ceiling

```yaml
server:
  # max_inflight_buffer_bytes: 0  # 0 = disabled (default)
```

**`max_inflight_buffer_bytes`** (`u64`, default `0`, which disables it) caps the bytes held
at once by the request and response bodies that the proxy buffers whole instead of
streaming. Almost all of them are responses: range merges, page widening, buffered range
serving, and the recovery and tee fallbacks. Every upload streams, so a request body counts
only on the small verbs other than GET and PUT, such as a `DeleteObjects` POST, which a
fixed 1 MiB bound covers. Streamed transfers do not count; [Memory impact](#memory-impact)
bounds them.

A request whose buffered body would take the total over the ceiling is rejected with HTTP
503 `SlowDown` and `Retry-After`, as for `max_concurrent_requests`, before any upstream
connection opens. A request that re-fetches the same range internally counts once, at the
larger of the two sizes. Startup validation rejects a non-zero value below 1 MiB
(`1048576`), the bound on a single buffered request body.

To size it on an instance with `M` bytes of RAM, start at `M / 4 - cache.max_ram_cache_size`
and adjust against `inflight_memory.peak_reserved_bytes` under real traffic. With 4 GiB of
RAM and the default 512 MiB RAM cache, that gives 512 MiB:

```yaml
server:
  max_inflight_buffer_bytes: 536870912  # 512 MiB on an instance with 4 GiB of RAM
```

The `inflight_memory` block on `/metrics` also reports `reserved_bytes`, `ceiling_bytes`
(reported while the ceiling is disabled too), `rejected_total` and
`aborted_accumulations_total`.

### Proxy identification

```yaml
server:
  add_referer_header: true   # Add Referer header to forwarded requests (default: true)
```

When `add_referer_header` is `true`, the proxy adds `Referer: Hybrid Cache for Amazon S3/{version} ({hostname})` to each request it forwards to S3, so S3 server access logs show which proxy sent it. It skips a request that already carries a `Referer` or signs one in `SignedHeaders`, since adding it would break the signature.

Separately from `add_referer_header`, the proxy appends a `Via: 1.1 s3-proxy-<token>` entry to every request it forwards, and answers `508 Loop Detected` to a request that already carries its own entry. There is no setting for it. It is skipped only for a request that signs `via`, in `SignedHeaders` or in a presigned URL's `X-Amz-SignedHeaders`. See [Request loops](ERROR_HANDLING.md#9-request-loops-via-and-508-loop-detected).

**Querying S3 server access logs.** The `Referer` field shows which proxy, at which version,
forwarded each request. A request without it came through HTTPS passthrough, carried its own
`Referer`, or did not pass through the proxy. An access log entry with the field:

```text
bucket-name [01/Jan/2024:12:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT file.txt "GET /file.txt HTTP/1.1" 200 - 1024 1024 10 9 "Hybrid Cache for Amazon S3/3.0.0 (proxy-1)" "aws-cli/2.x" -
```

Athena queries:

```sql
-- Count requests by proxy
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

### Request body size and streaming writes

```yaml
server:
  # write_cache_tee_channel_depth: 5             # frames (default)
```

Upload size is governed by S3's own limits, not by proxy configuration. A body above
S3's 5 GiB single-part PUT and UploadPart maximum is rejected with HTTP 413
`EntityTooLarge` before any upstream connection opens; anything S3 accepts, the proxy
forwards. Every upload path streams the body to the upstream frame by frame, so proxy
memory during an upload is independent of object size.

`server.max_buffered_request_body_bytes` is accepted and has no effect. A value other than
`5368709120` logs a startup warning; delete the field.

**`write_cache_tee_channel_depth`** (`usize`, default `5`)

The number of request-body frames queued for the write cache on a streamed PUT or
UploadPart. The proxy forwards the body to S3 unchanged, signature included, and copies
each frame onto this queue. When the queue is full, the proxy stops reading from the client
until the cache writer catches up. If the cache writer accepts no frame for 5 seconds, or
less once the write's 1-second cache budget is spent, the proxy stops caching that object,
discards the partial entry and keeps forwarding the body to S3, so a slow cache volume
cannot fail an upload. Each occurrence counts in `cache.tee_abandoned_total_put`.

Each upload holds one frame in flight plus up to this many queued frames, and a frame is
bounded by the HTTP read buffer. Upload memory per proxy is therefore at most
`max_concurrent_requests` × (`write_cache_tee_channel_depth` + 1) frames, since a permit
spans the whole transfer ([What this limit actually covers](#what-this-limit-actually-covers)).
A deeper queue absorbs bursts of slow cache writes at the cost of more memory per upload.

### TLS proxy listener configuration

The TLS proxy listener (port 3129) terminates TLS from clients and serves the decrypted requests through the caching pipeline, so the client hop is encrypted and responses are still cached. It is the recommended way for clients to reach the proxy: set `HTTP_PROXY=https://<proxy-host>:3129` and `--endpoint-url http://s3.<region>.amazonaws.com`, or set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` in place of `--endpoint-url`. [Configure client routing](GETTING_STARTED.md#3-configure-client-routing) compares this with the other routing options.

```yaml
server:
  tls:
    enabled: true                    # default false
    tls_proxy_port: 3129             # default
    cert_path: "/mnt/nfs/config/tls/cert.pem"   # Path to PEM certificate
    key_path: "/mnt/nfs/config/tls/key.pem"     # Path to PEM private key
```

**Configuration fields**:

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable the TLS proxy listener |
| `tls_proxy_port` | `3129` | Port of the TLS proxy listener |
| `cert_path` | (empty) | Path to PEM certificate file (required when enabled) |
| `key_path` | (empty) | Path to PEM private key file (required when enabled) |

**Validation rules**:
- With `enabled: true`, `cert_path` and `key_path` must be set.
- `tls_proxy_port` must not be `0` and must differ from `http_port`, `https_port`, `proxy_port` and the health, metrics and dashboard ports.
- If the TLS proxy listener fails to start, for example on a certificate or bind error, the other listeners keep running.
- The TLS proxy listener binds its port at startup and accepts connections once cache initialization completes, like the HTTP listener. Earlier connections wait in the listen queue. If cache initialization fails, it serves nothing.

**Certificate storage.** In a fleet, keep the certificate and key on the file system that holds the shared configuration file, so every proxy presents the same certificate. See [Network security requirements](ARCHITECTURE.md#network-security-requirements).

**Certificate generation.** See [Generating a self-signed certificate](GETTING_STARTED.md#generating-a-self-signed-certificate) for the `openssl` commands and the subject alternative names to include.

**Certificate verification.** Only the AWS CRT transfer client verifies the proxy's certificate; botocore-based clients (`aws s3api`, `aws s3` with the classic transfer client, boto3) encrypt the hop without verifying it. See [Configuring clients to trust the certificate](GETTING_STARTED.md#configuring-clients-to-trust-the-certificate).

## Cache configuration

### Basic settings

```yaml
cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 10737418240     # 10 GiB (default)
  ram_cache_enabled: true         # default false
  max_ram_cache_size: 536870912   # 512 MiB (default)
  ram_cache_shard_count: 8        # default 8, range 1-256
  eviction_algorithm: "tinylfu"   # the only algorithm
```

**Sizing `max_cache_size`.** Set it to at most 90% of the cache volume's capacity. Writes do not wait for eviction, which runs in the background, so a burst can push usage above the limit until eviction catches up; the 10% margin absorbs that. On a 100 GiB volume, set 90 GiB or less. [Eviction](EVICTION.md) describes how the limit is enforced.

**`cache_dir`** is the root of the cache on disk, normally the cache volume's mount point. Its layout is described in [Cache directory structure](CACHE_INTERNALS.md#cache-directory-structure).

**`ram_cache_shard_count`** (`usize`, default `8`, range `1–256`)

Splits the RAM cache into this many shards, each with its own lock, so requests for keys in different shards do not contend. Each shard holds `max_ram_cache_size / effective_shard_count`, which is 64 MiB at the defaults. An entry larger than one shard is served from disk instead of the RAM cache.

More shards lower lock contention when hundreds of requests hit distinct keys at once. Fewer shards give each shard more room, which suits workloads dominated by a few large objects. Each shard evicts on its own, so with an uneven key distribution the RAM cache can hold less than `max_ram_cache_size` in total.

#### RAM sizing and the admission ceiling

The RAM cache admits any single entry up to 64 MiB (`67108864` bytes). This admission ceiling is fixed, not a configuration field, and applies to every entry, whether or not [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching) is enabled. To keep every shard at least that large, the proxy clamps the effective shard count:

```text
effective_shard_count = min(ram_cache_shard_count, max(1, max_ram_cache_size / 67108864))
```

When the clamp lowers the shard count below `ram_cache_shard_count`, the proxy logs a startup warning that suggests a larger `max_ram_cache_size`. The default 512 MiB with 8 shards needs no clamp; a 256 MiB cache with 8 configured shards runs 4.

An admitted entry can still be evicted: a 64 MiB shard holds few large entries, so it can evict a hot one while other shards sit idle. To keep `N` hot entries of up to 64 MiB resident, set `max_ram_cache_size` to at least `N × 67108864`.

**`eviction_algorithm`** takes `"tinylfu"`, the default and only algorithm, a simplified frequency-recency hybrid. `"lru"` is accepted as another name for it and logs a startup warning; set `"tinylfu"` or remove the field.

### Metadata cache

A RAM cache of parsed `.meta` files, separate from the RAM cache of object data, so HEAD
requests and GET freshness checks can skip a disk read. See
[RAM metadata cache](CACHE_INTERNALS.md#ram-metadata-cache) for how it stays consistent
across a fleet.

```yaml
cache:
  metadata_cache:
    enabled: true                  # default true
    refresh_interval: "5s"         # default 5s, range 1-300s
    max_entries: 100000            # default 100000, range 100-1000000
```

**`enabled`** (`bool`, default `true`): turn the metadata cache off to force every
metadata read to hit disk.

**`refresh_interval`** (`Duration`, default `"5s"`, range 1-300s): staleness threshold.
An entry older than this is re-read from disk on next access. This governs disk
re-reads, not S3 re-fetches; S3 freshness is governed by the TTLs.

**`max_entries`** (`usize`, default `100000`, range 100-1000000): entry cap. Each entry
holds one object's parsed metadata, roughly 1-2 KiB, so the default cap uses on the order of
100-200 MiB. It is a separate budget from `max_ram_cache_size`.

A value outside either range is used as configured and logs a startup warning.

### Cache bypass headers

**`cache.cache_bypass_headers_enabled`** (`bool`, default `true`)

When `true`, a client can bypass the cache for a single request by sending
`Cache-Control: no-cache`, `Cache-Control: no-store`, or `Pragma: no-cache`. Set it to
`false` to ignore those headers, so no client can force a cache bypass. See
[Cache bypass headers](CACHING.md#cache-bypass-headers).

## Time-to-live (TTL) configuration

A TTL sets how long the proxy serves a cached entry without asking S3. With a TTL above zero, cache hits are served without contacting S3, so any client that can reach the proxy can read any cached object; see [Shared cache access model](ARCHITECTURE.md#shared-cache-access-model). After the TTL expires, the next request [revalidates](CACHE_FRESHNESS.md#time-to-live-ttl-configuration) the entry with a conditional request signed by that client, so S3 checks the client's credentials. A TTL of `0s` revalidates every request and still saves the transfer whenever S3 answers `304 Not Modified`.

### TTL types

| Field | Default | Applies to |
|---|---|---|
| `get_ttl` | `1d` | Object data served to GET requests |
| `head_ttl` | `60s` | HEAD responses |
| `put_ttl` | `1h` | Objects in the write cache, until their first GET |
| `incomplete_upload_ttl` | `1d` | Cached parts of a multipart upload with no new activity |

```yaml
cache:
  get_ttl: "1d"
  head_ttl: "60s"
  put_ttl: "1h"
  incomplete_upload_ttl: "1d"
  actively_remove_cached_data: false
```

### GET TTL

`get_ttl` (default `1d`, range `0s` to `315360000s`, which is 10 years) bounds how long the proxy keeps serving an object that was overwritten or deleted without passing through it. An overwrite or delete through the proxy invalidates the cached copy at once. When `get_ttl` expires, the entry stays on disk and the next GET sends S3 a conditional request, serving the cached bytes on `304`. With `actively_remove_cached_data: true`, the validation scan also deletes expired entries; see [Active vs lazy expiration](#active-vs-lazy-expiration).

```yaml
cache:
  get_ttl: "315360000s"  # The maximum, for objects that never change. Use 1h or less for objects that change often
```

A client that sends a HeadObject before each download, as the AWS CRT transfer client does, sees a changed object within `head_ttl` even under a long `get_ttl`; see [HEAD TTL](CACHE_FRESHNESS.md#head-ttl-head_ttl).

### PUT TTL

`put_ttl` (default `1h`) is how long an object in the write cache stays valid before its first GET. It is shorter than `get_ttl` by default because an uploaded object may never be read. The first GET within `put_ttl` makes one conditional request to S3 to learn the object's `Last-Modified`, serves the cached bytes and moves the entry onto `get_ttl`; see [TTL transition on first read](#how-write-caching-works). Later reads do not extend it, and an object not read within `put_ttl` expires. The write cache stores objects on disk only, not in the RAM cache.

### Incomplete upload TTL

`incomplete_upload_ttl` (default `1d`, range `1h` to `7d`) is how long the proxy keeps the cached parts of a multipart upload that has no new activity. Removing them does not touch the upload in S3. Activity is the newest modification time of the upload's directory, its `upload.meta` and its part records, so an upload still receiving parts is kept, and so is one whose `CompleteMultipartUpload` is in progress. Each proxy sweeps at startup and then every `incomplete_upload_ttl / 4` or every hour, whichever is sooner.

### HEAD TTL

`head_ttl` (default `60s`) is how long the proxy answers HEAD requests from cached metadata. It affects HEAD requests only: a GET within `get_ttl` is served from the cache whatever the state of `head_ttl`, and an expired `head_ttl` does not make a GET revalidate.

### Active vs lazy expiration

```yaml
cache:
  actively_remove_cached_data: false  # Default: lazy expiration
```

With `false`, the default and the recommended setting, an expired entry stays on disk until
a request revalidates it or eviction removes it. With `true`, the
[validation scan](#validation-scan) also deletes expired entries as it passes them, which
frees their space before eviction would, at the cost of more I/O in each scan.

Keep `actively_remove_cached_data: false` with `get_ttl: "0s"`. A zero TTL expires an entry
as soon as it is cached, so active expiration would delete it before any request could
revalidate it, and the cache would never serve a hit. See
[Minimum TTL values](CACHE_FRESHNESS.md#minimum-ttl-values).

In a fleet, active expiration skips an entry that another proxy is using. HEAD metadata is
removed when `head_ttl` expires, whatever this setting.

## Write cache configuration

The write cache stores the bodies of PUT requests and multipart uploads as they pass through, so later GETs are served without fetching the body from S3. The first GET makes one conditional request to learn the object's `Last-Modified`, which S3's write responses omit.

POST object uploads (browser form uploads, `POST /bucket` with a `multipart/form-data` body) reach S3 but are not cached; see [POST object upload is not write-through cached](CACHE_READ_PATHS.md#post-object-upload-is-not-write-through-cached).

### Write cache basic settings

```yaml
cache:
  write_cache_enabled: true               # default true
  write_cache_percent: 10.0               # default 10.0: share of max_cache_size for objects not yet read
  write_cache_max_object_size: 268435456  # 256 MiB (default); larger uploads are not cached
  put_ttl: "1h"                           # default
  incomplete_upload_ttl: "1d"             # default
```

### How write caching works

- **PUT**: the body is stored as one range covering the whole object, valid for `put_ttl`. The client receives S3's response unchanged.
- **Multipart upload**: CreateMultipartUpload records the upload, each UploadPart is stored as a range file, CompleteMultipartUpload gives the parts their final byte offsets and writes the object's metadata, and AbortMultipartUpload deletes the cached parts.

The `CompleteMultipartUpload` body is bounded by `max_complete_body_bytes`; see
[CompleteMultipartUpload body limit](#completemultipartupload-body-limit).

**TTL transition on first read.** The first GET of an object in the write cache clears its
`is_write_cached` flag and sets its expiry to `get_ttl` from that moment. This happens once:
later reads do not extend it, and the object then follows the read cache's expiry rules
([Rules are re-evaluated on each read](#rules-are-re-evaluated-on-each-read)). An object
never read expires after `put_ttl`.

The same transition moves the object's bytes out of the write cache's allocation. Before its
first read the object counts against `write_cache_percent`; afterwards it counts only against
`max_cache_size`. The bytes stay where they are, so `cache.write_cache_size` falls while
`cache.total_cache_size` does not, and `write_cache.graduations_total` counts the transition
([write cache metrics](METRICS_REFERENCE.md#write_cache)). The fleet applies it once per
object, even when several proxies read the object at the same moment.

### Capacity management

The write cache gets a share of `max_cache_size`, so unread uploads cannot crowd out the read
cache:

```yaml
cache:
  write_cache_percent: 10.0  # default: 10% of max_cache_size
```

`write_cache_percent` bounds the staged objects: those written through the cache and not yet
read. An object leaves the bound at its first read
([TTL transition on first read](#how-write-caching-works)), so a workload that reads back
what it writes keeps little against it however much it uploads. The bound covers every
staged byte on the cache volume, across the fleet, so it does not grow with the number of
proxies. Going over it starts reclamation and refuses nothing. To keep a larger unread
working set, raise `write_cache_percent`. The proxy uses the value as set, with no clamp or range check, so `80` gives a bound of 80% of `max_cache_size`.

Reclamation, described in [Eviction](EVICTION.md), uses these rules:

- It starts when staged bytes reach `eviction_trigger_percent` of the allocation and stops
  at `eviction_target_percent`, the same two fields the read cache uses.
- It removes the oldest staged objects first, with those past `put_ttl` ahead of the rest.
- It runs in the background, never on the request path, so no upload waits for it.

An upload is not cached when caching it would take the cache past `max_cache_size`, or would
leave less than `disk_safety_free_space_floor` free on the cache volume. The upload still
goes to S3. The skip counts as `disk_safety` in `signed_put.skipped_puts_total`, and
`/health` reports the cache component as `Degraded` meanwhile. If `cache.write_cache_size`
looks too high for what is staged, see
[Recovery from an inflated write-cache figure](EVICTION.md#recovery-from-an-inflated-write-cache-figure).

### Free-space floor and the danger band

```yaml
cache:
  disk_safety_free_space_floor: 1073741824  # 1 GiB (default)
```

`disk_safety_free_space_floor` (`u64`, default 1 GiB) is the free space the proxy keeps on
the cache volume beyond what an incoming object needs. It applies to every cache write, from
the write cache and from GET-miss fills. When the volume has less free space than the floor
plus the object's size, the proxy does not cache the object; the client's request goes ahead
regardless.

- Outside the danger band, a fill checks one free-space reading per process, refreshed by
  the first fill that finds it more than 5 seconds old. An idle proxy makes no checks.
- Inside the danger band, where free space is at most the object's size plus four times the
  floor, every fill takes a fresh `statvfs` reading and a reservation for its bytes that
  every proxy on the cache volume sees, held until the write commits or aborts. A fresh
  reading alone is not enough, because several proxies could each admit a fill that fits on
  its own and together overrun the volume.

Size the floor for every writer on the cache volume, not for one proxy. Outside the band
each writer may admit fills for up to 5 seconds against its last reading, so:

```text
floor >= (5 s × total fill rate of every writer on the volume + largest cached object) / 4
```

Every writer includes anything else writing to the same file system. At a 2 GiB/s fleet
cache-miss rate with a 256 MiB `write_cache_max_object_size`, that is about 2.6 GiB, so the
1 GiB default is too small for a busy fleet. Raising the floor widens the band with it; there
is no separate band field. `0` disables both and leaves only the handling of `ENOSPC` errors,
which is not recommended for a fleet.

Declines on the write path count as `disk_safety` in `signed_put.skipped_puts_total`.
Declined GET-miss fills and full-object stores count in `cache.capacity_guard.declines_total`,
beside `cache.capacity_guard.last_free_bytes` and `cache.capacity_guard.floor_bytes`
([capacity guard metrics](METRICS_REFERENCE.md#free-space-floor-guard-cachecapacity_guard)).
The proxy logs each decline with the free-space reading and the fleet's reserved total.

### CompleteMultipartUpload body limit

`max_complete_body_bytes` caps the CompleteMultipartUpload XML body, which lists the parts
and their ETags, so an oversized or malicious body cannot exhaust memory:

```yaml
cache:
  max_complete_body_bytes: 10485760  # Default: 10 MiB
```

A larger body is rejected with HTTP 413 before it reaches S3. A list of 10,000 parts, the S3
maximum, is on the order of 1 MiB, so the default leaves room to lower it on an instance
short of memory.

### Write cache in a fleet

In a fleet, any proxy can take any request of a multipart upload. File locks on the cache
volume serialize changes to an upload's tracking metadata, and the incomplete-upload sweep
takes the fleet's eviction lock. TTL checks use file modification times from the cache
volume.

### Example configuration

```yaml
cache:
  write_cache_enabled: true
  write_cache_percent: 20.0               # 20% of max_cache_size for objects not yet read
  write_cache_max_object_size: 536870912  # cache uploads up to 512 MiB
  put_ttl: "1d"                           # keep unread uploads for a day
  incomplete_upload_ttl: "6h"             # drop the parts of abandoned uploads after 6 hours
```

## Cache rules

Cache rules override global cache settings for keys that match a glob pattern, without editing the configuration file or restarting the proxy. Rules live in a single hot-reloadable file at `cache_dir/cache_rules.json`. The file is optional: when it is absent or its `rules` array is empty, every setting resolves to the global YAML `cache:` defaults.

The proxy does not read per-bucket `cache_dir/metadata/{bucket}/_settings.json` files, and logs a warning once if it finds any. To translate them into rules, see [Upgrading to 2.0.0](UPGRADING.md#upgrading-to-200-per-bucket-_settingsjson-removed).

### Global config fields

These YAML `cache:` fields supply the defaults that rules fall through to.

```yaml
cache:
  # Default read caching for GET responses (default: true).
  # When false, nothing is read-cached unless a matching rule sets read_cache_enabled: true;
  # an "allowlist" pattern where only keys matched by an enabling rule are cached.
  read_cache_enabled: true

  # Default true: a GET, range GET or HEAD whose only conditional is an If-Match or If-Range
  # matching the cached ETag is answered from cache when the data is cached. Other conditional
  # requests go to S3. Set false to send every conditional request to S3.
  # See CACHE_FRESHNESS.md, "Conditional headers handling".
  evaluate_conditions_from_cache: true

  # How long a loaded rule set is used before cache_rules.json is read again (default 60s).
  # The first request after this interval starts the re-read. A rule edit takes effect
  # once that read completes, which is a later request if the read is slow.
  bucket_settings_staleness_threshold: "60s"
```

### Rules file format

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

**`get_ttl` and `head_ttl` are independent per rule.** Neither is derived from or clamped to the other. A rule setting `get_ttl: "0s"` without `head_ttl` leaves HEAD requests for matching keys answerable from cache for the resolved `head_ttl` (default 60s). A prefix that must reach S3 on every read, so that S3 evaluates IAM for each request, needs both `get_ttl` and `head_ttl` at `"0s"`, because each covers a different request class. The example also sets `evaluate_conditions_from_cache: false`, which a zero `get_ttl` already implies for a lone `If-Match` or `If-Range` GET:

```json
{ "pattern": "**/restricted/**", "get_ttl": "0s", "head_ttl": "0s", "evaluate_conditions_from_cache": false }
```

See [Zero TTL revalidation](CACHING.md#zero-ttl-revalidation) for what each field closes.

**`page_widening` / `page_size` (page-aligned range caching / range read widening).** `page_widening` (bool, default `false`) enables widening a small ranged GET for matching keys into a fixed-size, page-aligned fetch; `page_size` (bytes, default `16777216` = 16 MiB when `page_widening` is enabled without specifying it) sets the page size `P`. `page_size` must be `> 0` and `<= 67108864` (64 MiB) for any rule enabling `page_widening`, validated at startup and on hot reload; an out-of-range value invalidates the rule set (see [Validation](#validation)). Off by default and never enabled globally: only an explicit rule enables it, since amplification is workload-dependent.

**Check eligibility before enabling.** Widening applies only when `range` is absent from the request's SigV4 `SignedHeaders`. The AWS CLI and every official AWS SDK sign `Range`, so enabling this for a CLI/SDK workload has no effect and produces no error: requests fall through to the ordinary range path and increment `page_cache.skipped_signed_range`. Read [Eligibility: which clients can use this](CACHE_READ_PATHS.md#eligibility-which-clients-can-use-this) first, then [Page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching) for the mechanism.

Field syntax, for a key pattern whose reads fall mostly within the same pages:

```json
{ "pattern": "**/*.parquet", "page_widening": true, "page_size": 16777216 }
```

**`hedging_enabled` / `hedge_trigger_after` / `hedge_max_per_request` (hedged upstream requests).** `hedging_enabled` (bool, default `false`) turns on hedged upstream requests for matching keys. When the original fetch has not returned its first byte within `hedge_trigger_after`, the proxy sends a second identical fetch, serves whichever responds first and cancels the other. `hedge_trigger_after` (duration, default `250ms`) sets that first-byte threshold, and every rules load checks that it is above zero and below `connection_pool.upstream_first_byte_timeout` (default `5s`). `hedge_max_per_request` (integer, default `1`) caps the hedges per client request, shared across all the parallel fetches of a range GET. Only a rule can turn hedging on, because each hedge is an extra S3 request and the right threshold depends on the workload's first-byte times. See [Hedged upstream requests](HEDGING.md) for the mechanism:

```json
{ "pattern": "analytics-bucket/hot-prefix/**", "hedging_enabled": true, "hedge_trigger_after": "250ms", "hedge_max_per_request": 1 }
```

**`compression_enabled` is rules-win.** The proxy has a built-in default
denylist of already-compressed extensions (images, video, audio, archives,
documents, executables; see [Compression](COMPRESSION.md)) that skip LZ4
compression when no rule matches. If a rule explicitly sets
`compression_enabled` for a key, that value is honored verbatim, overriding
the denylist in either direction, e.g. `{"pattern": "**/*.jpg", "compression_enabled": true}`
forces compression of `.jpg` keys despite the built-in denylist; the reverse
lets you disable compression for an extension the denylist would otherwise
compress. The global `compression.threshold` size floor still applies
regardless of any rule.

**Duration format.** The same as in the configuration file, for example `"0s"`, `"5m"` or `"7d"`; see [Duration format](#duration-format).

**Schema and example.** Include `$schema` to get autocompletion from the [rules schema](cache-rules-schema.json) in an editor. The [example rules file](../config/cache_rules.example.json) is a complete starting point.

### Glob syntax

Patterns are globs, not regex. A rule matches against the full cache key (`{bucket}/{object_key}`, no leading slash). The metacharacters are:

| Glob | Matches |
|------|---------|
| `*`  | Any run of characters except `/`: one path segment |
| `**` | Any run of characters including `/`: crosses segments |
| `?`  | Exactly one character except `/` |
| any other char | A literal. Regex metacharacters are escaped, so a bucket named `my.logs` matches `.` literally, not as "any character" |

Matching is:

- **Case-sensitive.** Patterns match the cache key exactly as cased. This mirrors S3: object keys are case-sensitive and bucket names are lowercase.
- **Anchored to the whole key.** A pattern matches only if it matches the entire key. `my-bucket/temp` matches only the exact key `my-bucket/temp`, not `my-bucket/temp/file.txt`. To match everything under `temp/` in `my-bucket`, write `my-bucket/temp/**`.

Because the bucket is part of the match surface, one syntax expresses every scope:

| Pattern | Meaning |
|---------|---------|
| `my-bucket/temp/**` | The `temp/` prefix in one bucket |
| `**/logs/**` | A `logs/` segment anywhere, in every bucket |
| `**/credit-cards/**` | A `credit-cards` segment anywhere, in every bucket |
| `prod-*/static/**` | The `static/` prefix in every bucket whose name starts `prod-` |

### First-match-per-field precedence

Rules are an ordered list. For each field independently, the value comes from the earliest rule in list order that both matches the key and sets that field. A field left unset by a matching rule falls through to the next matching rule, and finally to the global YAML default. Put specific rules above broad ones.

Worked example, with fields resolving independently:

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

Two invariants are applied after resolution:

- `get_ttl == 0s` forces `ram_cache_eligible = false` (RAM cache bypasses revalidation, which would serve stale data).
- `read_cache_enabled == false` forces `ram_cache_eligible = false`.

### Hot reload and error resilience

The proxy re-reads `cache_rules.json` on a staleness threshold (`bucket_settings_staleness_threshold`, default 60s), whether or not the file changed. Once the threshold has elapsed, the next request re-reads the file and recompiles the rule set, with no restart. A request waits at most 100 ms for that read, so a slow read takes effect on a later request. An edit that lands while a read is already in flight is picked up by the following read, at most about twice the threshold later. Within the threshold the in-memory rule set is reused without touching disk. The pattern set is compiled once per successful load, never per request.

A bad edit never takes the cache down:

- Invalid JSON, failed validation (empty pattern, a pattern that fails to compile, an unparseable duration), or a rule count over the cap → the proxy keeps the last-known-good rule set and logs a warning.
- Invalid file at first startup → the proxy starts with an empty rule set (global defaults for every field) and logs a warning.

### Cache-key forms

Patterns match the cache key as computed by [cache-key normalization](CACHE_INTERNALS.md#cache-directory-structure), which differs by request kind. Match the leading segment accordingly (matching is case-sensitive):

| Request kind | Cache-key leading segment | Example pattern |
|--------------|---------------------------|-----------------|
| Regular bucket (any addressing style) | `{bucket}` | `my-bucket/temp/**` |
| Regional access point | `{name}-{account}-s3alias` | `myap-123456789012-s3alias/**` |
| Multi-Region Access Point (MRAP) | `{alias}.mrap` | `mfzwi23gnjvgw.mrap/data/**` |
| Non-AWS S3-compatible / unrecognized host, path-style | `{bucket}` (the first path segment; the host is not part of the key) | `my-bucket/data/**` |

See [Access point and MRAP cache key prefixing](CACHE_INTERNALS.md#access-point-and-mrap-cache-key-prefixing) for how each form is derived.

### Rule count and resolution cost

A rule set holds at most 1024 rules. Resolving a key costs more for each rule that matches
it, not for each rule in the set, so with many rules prefer specific patterns to broad `**`
patterns that match every key.

### Rules are re-evaluated on each read

Resolved settings are re-evaluated against the current rules on each read request, so a rule change applies to already-cached objects once the proxy has re-read the rules (see the staleness threshold and 100 ms read bound above), not only to newly written objects. Freshness is judged against the current resolved TTL: for GET, the time since the object was cached or last confirmed by a `304` from S3, against `get_ttl`; for HEAD, the time since the last HEAD refresh against `head_ttl`. A tightened TTL therefore applies to entries already cached. The stored `expires_at` still applies as well: an entry past it is revalidated before it is served. For example, tightening a key's resolved `get_ttl` from `1h` to `5m` expires cached objects once 5 minutes have passed since they were cached or last revalidated. `get_ttl: "0s"` makes a later GET revalidate against S3 once the rules are re-read. After a `304`, the next revalidation of that object happens one `get_ttl` later, not on every read. Setting `read_cache_enabled: false` stops serving the key from cache and eagerly deletes the cached entry (range files + metadata) on the first GET/HEAD after the rule takes effect. A GET that was already fetching from S3 when the rule took effect can still store its response; that entry is not served while the rule is `false`, and a later GET/HEAD for the key deletes it. No restart or manual cache wipe is required.

### Validation

- Empty `pattern` → rule set rejected
- A glob that cannot be translated to a valid regex → rule set rejected
- More than the maximum number of rules (default 1024) → rule set rejected
- An unparseable duration → rule set rejected
- A rule with `page_widening: true` and a `page_size` of `0` or greater than `67108864` (64 MiB) → rule set rejected
- A rule with `hedging_enabled: true` and a `hedge_trigger_after` of `0` or greater than or equal to `connection_pool.upstream_first_byte_timeout` (default `5s`) → rule set rejected

On any validation failure the proxy keeps the last-known-good rule set, or starts with an empty one if the file was invalid at first startup, and logs a warning.

See [Cache rules](CACHING.md#cache-rules) in the caching guide for when to use each setting.

## Cache expiration scenarios

Request-flow walkthroughs for each TTL state (fresh cache, expired GET TTL, a
PUT-cached object read for the first time, HEAD revalidation, conditional requests,
multipart, and incomplete-upload cleanup) live in
[Cache validation flow](CACHE_FRESHNESS.md#cache-validation-flow).

For the fields that drive them, see [Time-to-live (TTL) configuration](#time-to-live-ttl-configuration).

## RAM-disk cache coherency

These fields bound how long the RAM cache serves bytes without checking them against the cache volume. [RAM-disk cache coherency](CACHE_INTERNALS.md#ram-disk-cache-coherency) describes the mechanism.

```yaml
cache:
  ram_cache_verification_interval: "1s"   # default 1s, range 0-60s
  ram_cache_flush_interval: "10s"         # default 10s, range 10-600s
```

| Field | Default | Range | Meaning |
|---|---|---|---|
| `ram_cache_verification_interval` | `1s` | `0s` to `60s` | How long a RAM cache hit is served without comparing its ETag with the object's `.meta` file. `0s` checks every hit |
| `ram_cache_flush_interval` | `10s` | `10s` to `600s` | Eviction keeps an entry that a peer accessed within twice this interval. It does not set how often RAM cache hits are written to disk |

Past `ram_cache_verification_interval`, the next hit reads the `.meta` file once. On a mismatch, or if the object is gone, the proxy drops the key's RAM ranges and serves from disk; if the read fails, it serves the RAM copy. The check confirms only that the RAM copy matches the cache volume, so `get_ttl` still governs revalidation with S3, and it is separate from `metadata_cache.refresh_interval`, which bounds the RAM copy of metadata. The access statistics behind eviction are written from RAM to disk every 5 seconds, in batches of up to 10,000 entries; neither figure is configurable, and no `/metrics` field counts these writes or the verification reads.

## Cache hit performance tuning

### Full-object check threshold

```yaml
cache:
  full_object_check_threshold: 67108864  # 64 MiB (default)
```

A range request first checks whether the whole object is cached, which means scanning its cached ranges. For an object larger than `full_object_check_threshold`, the proxy skips that check and looks up the requested range directly. Lower it when many large objects are cached as separate ranges; raise it when large objects are usually cached whole.

### Disk streaming threshold

```yaml
cache:
  disk_streaming_threshold: 1048576  # 1 MiB (default)
```

A cached range at or above this size is streamed from disk in chunks instead of being read into memory whole. RAM cache hits are served from memory whatever this setting. Lower it when memory is short and many large ranges are served at once.

### Compression batch size

```yaml
cache:
  compression_batch_size: 1048576  # 1 MiB (default)
```

The proxy collects up to this many bytes from S3 in memory, compresses them as one LZ4 frame and appends the frame to the cache file. Larger batches compress better and add less per-frame overhead; smaller ones use less memory per request. The range is `65536` (64 KiB) to `16777216` (16 MiB), and startup rejects a value outside it.

### Metadata file size cap

```yaml
cache:
  # max_metadata_file_bytes: 4194304  # 4 MiB (default)
```

The largest `.meta` file the proxy reads. A larger file is treated as corrupt without being read, so an oversized file is not loaded. The cap applies to range lookups in the disk cache, validation against HEAD, range invalidation, and the RAM-hit recorder's extent read. Every other `.meta` read ignores the cap, including the lookup that serves a GET or HEAD when an object's metadata is not in the RAM metadata cache, the HEAD-entry lookup, the RAM-hit ETag check, TTL refresh, the write cache, eviction and journal consolidation. A typical `.meta` file is about 2 KiB. Leave this field unset; the default is 4 MiB. A value below 64 KiB (65536) is raised to 64 KiB with a startup warning, because a cap below a real `.meta` size turns every read of that object into a miss and a rewrite.

### Metadata I/O concurrency

```yaml
cache:
  # metadata_io_concurrency: 32  # (default)
```

The most blocking metadata reads run at once by range lookups in the disk cache, validation against HEAD, range invalidation, and the RAM-hit recorder's extent read. Further such reads wait their turn. Each of the proxy's disk cache handles holds its own limit, so the total for one proxy can exceed it. Every other `.meta` read takes no part in the limit, including the lookup that serves a GET or HEAD on a RAM metadata-cache miss. Lower it, for example to 16, on a constrained NFS mount; raise it, for example to 64, on a local SSD cache with many cores. A value of 0 is raised to 1 with a startup warning.

### Partial range commit ratio

```yaml
cache:
  # partial_range_commit_ratio: 0.5  # (default)
```

The smallest fraction (`0.0` to `1.0`, default `0.5`) of a range that a GET fill must receive, in order, for the proxy to keep what it received when the stream ends early. A stream ends early when the client cancels it or `connection_pool.upstream_idle_timeout` aborts it. The AWS CRT transfer client does this routinely: it opens many range connections and closes each once it has the bytes it needs.

At or above the ratio, the proxy caches the received prefix as a shorter range with its true bounds, and a later request fetches the rest from S3. Below it, the bytes are dropped. `1.0` keeps only complete ranges, `0.0` keeps any non-empty prefix, and startup rejects `NaN`. The write cache always needs the exact byte count, so it never caches a truncated upload.

### Consolidation cycle timeout

```yaml
cache:
  shared_storage:
    consolidation_cycle_timeout: "30s"  # Default: 30 seconds
```

The longest one consolidation cycle may run, covering discovery, per-key processing and cleanup. At the deadline, the keys already processed stay applied and the rest wait for the next cycle. A cycle discovers at most 5,000 keys. Collecting size changes runs before the deadline starts, so the timeout does not cut it short.

## Range request optimization

### Range merging

```yaml
cache:
  range_merge_gap_threshold: 1048576  # 1 MiB (default)
```

When a range request needs several missing pieces, the proxy fetches pieces separated by less than `range_merge_gap_threshold` of cached data as one S3 request, re-fetching the cached gap between them. Pieces further apart are fetched separately.

For example, with 0-8 MiB, 16-24 MiB and 32-40 MiB cached, a request for 0-40 MiB is missing 8-16 MiB and 24-32 MiB. The cached 8 MiB between the two pieces is above the 1 MiB threshold, so the proxy makes two S3 requests and serves 24 MiB of the 40 MiB from the cache.

A smaller threshold re-fetches fewer cached bytes. A larger one makes fewer S3 requests, which suits a high-latency link to the origin, and re-fetches more cached bytes.

## Eviction configuration

```yaml
cache:
  eviction_trigger_percent: 95  # default
  eviction_target_percent: 80   # default
```

Eviction starts when the cache exceeds `eviction_trigger_percent` of `max_cache_size` and removes entries until it is down to `eviction_target_percent`. With a 10 GiB cache and the defaults, eviction starts at 9.5 GiB and stops at 8 GiB. A target closer to the trigger keeps more of the cache in use and evicts more often. The write cache applies the same two percentages to its own allocation. [Eviction](EVICTION.md) describes how entries are chosen and how a fleet shares the work.

`eviction_buffer_percent` is accepted and has no effect.

## Multi-instance coordination

Proxies in a fleet coordinate through files on the cache volume: journals for metadata updates, `flock` locks, and shared size and eviction state. Coordination is always on and has no `enabled` field. [Shared storage](SHARED_STORAGE.md) describes each mechanism and its failure modes.

```yaml
cache:
  shared_storage:
    lock_acquire_wait_budget: "5s"    # default 5s, range 1-300s
    lock_max_retries: 5               # default 5, range 1-20
    consolidation_interval: "5s"      # default 5s, range 1-60s
    validation_max_duration: "4h"     # default 4h, range 10m-23h
    validation_threshold_warn: 5.0    # default
    validation_threshold_error: 20.0  # default
    shard_fold_interval: "1h"         # default 1h, range 5m-7d
    shard_fold_max_duration: "30m"    # default 30m, range 1m-24h
    tail_floor_bytes: 1048576         # default 1 MiB, range 4 KiB-1 GiB
    tail_max_entries: 4096            # default 4096, range 1-1048576
    tail_delta_max_entries: 4096      # default 4096, range 1-1048576
    eviction_lock_timeout: "60s"      # default 60s, range 30-3600s
    eviction_worklist_lease: "900s"   # default 900s, range 760s-24h
```

| Field | Meaning |
|---|---|
| `lock_acquire_wait_budget` | How long a metadata lock acquisition waits before the request fails; see [Metadata lock timing](#metadata-lock-timing) |
| `lock_max_retries` | How many attempts a retrying metadata lock acquisition makes |
| `consolidation_interval` | How often each proxy applies the journals to the `.meta` files. Each cycle applies whatever the journals hold |
| `validation_max_duration`, `validation_threshold_warn`, `validation_threshold_error` | The validation scan's time budget and logging thresholds; see [Validation scan](#validation-scan) |
| `shard_fold_interval`, `shard_fold_max_duration` | The shortest interval between size folds, and the time budget of one pass; see [Size fold cadence](#size-fold-cadence) |
| `tail_floor_bytes` | The smallest compressed range that eviction's per-shard index of large ranges holds. Lower it when most objects are under 1 MiB, where the default leaves the index empty |
| `tail_max_entries` | How many of the largest ranges the index keeps per cache shard. Raise it if eviction reports freeing less than it was asked to while the cache is full |
| `tail_delta_max_entries` | How many recently cached large ranges each proxy remembers between index rebuilds. Keep it at or above `tail_max_entries` |
| `eviction_lock_timeout` | Whether a proxy that finds the fleet's eviction lock held tries once more before skipping the pass; see [Distributed eviction](SHARED_STORAGE.md#distributed-eviction) |
| `eviction_worklist_lease` | How long a proxy holds a chunk of the eviction work list before another proxy may take it over; see [Which proxy deletes](EVICTION.md#which-proxy-deletes) |

A proxy starts each batch of up to 64 deletions only while at least half its work-list lease remains, and the lease is not extended, so a long chunk takes several claims. The 760-second minimum lets one batch fit in half the lease when each deletion waits up to `lock_acquire_wait_budget` for its lock. With `lock_acquire_wait_budget` above `5s`, proxies hold each lease for at least 2 × (64 × the budget + 60 s) and log a startup warning.

### NFS mount requirements

Two mount properties are correctness requirements in a fleet: `lookupcache=pos`, so each proxy sees files its peers create, and working cross-host `flock`. Use `nfsvers=4.1`, or NFSv3 with working NLM, and never `nolock`, `local_lock=flock` or `local_lock=all`. The proxy logs one warning at startup, naming the mount, when `lookupcache=pos` is missing and still starts; a missing lock property is not reported. In both cases the fleet looks healthy while it loses most of its hit rate or corrupts cache state. [Mount requirements](SHARED_STORAGE.md#mount-requirements) gives the mount lines for NFS, FSx for OpenZFS and EFS, and how to verify both properties.

### Size fold cadence

The size fold lists and stats every range file to re-ground each cache shard's byte total. It reads no `.meta` file, so it costs less per entry than the [validation scan](#validation-scan), and it runs on its own schedule.

- `shard_fold_interval` is a floor, not a schedule. The fold does not start a pass until twice the previous pass's duration has elapsed, so on a volume where a pass takes more than half the interval, the cadence lengthens by itself. Each declined tick counts in `cache.st_size_folds_declined_overlap_total`, the first figure to read when the total looks stale ([Size fold figures](METRICS_REFERENCE.md#size-fold-figures)).
- A value above that floor is used as set. The fold never runs more often than asked, and a pass never overlaps another: a tick that arrives during a pass is declined, and the next tick retries.
- The interval belongs to the cache volume, not to one proxy. When a fold falls due, the proxy reads how long ago the stalest shard was folded; if that is within the interval, it schedules its next pass from that age instead. A restart, or a rolling restart of the fleet, inside the interval folds nothing.

Each pass costs one directory listing per cache leaf and one stat per range file on the cache volume, the main reason to lengthen the interval on a large cache. `shard_fold_max_duration` bounds one pass and is checked at shard boundaries. A pass that runs out leaves the shards it did not reach with their previous totals, and each shard records when it was last folded.

### Metadata lock timing

Two fields bound a metadata lock acquisition, in different ways:

| Field | Bounds | Default |
|---|---|---|
| `lock_acquire_wait_budget` | How long a budgeted acquisition waits | `5s` (range `1s` to `300s`) |
| `lock_max_retries` | How many attempts a retrying acquisition makes, with exponential backoff | `5` (range 1 to 20) |

The `.meta` write, eviction and cleanup paths use the budget. Both kinds of acquisition poll with non-blocking attempts, so no thread waits in the kernel while a peer holds the lock. When the budget runs out, the request fails with a retryable contention error and nothing on disk has changed; a client retry normally succeeds. A sustained non-zero `cache.metadata_lock_wait_budget_expired_total` means requests are failing rather than waiting, and the budget is short for how long locks are held on your volume.

The proxy never takes a lock by deciding that its holder is dead, and no field enables that. A `flock` cannot go stale: the kernel releases it when the holding process exits, and the NFS server releases it when the holder's lease expires, whatever the budget. Neither EFS nor FSx states its lease duration, so tune the budget against the hold time you can measure, `metadata_lock_hold_time_ms_total` divided by `metadata_lock_acquisitions_successful`. A lock released at lease expiry can also lose one metadata update; [Journal-based metadata writes](SHARED_STORAGE.md#journal-based-metadata-writes) explains when.

`lock_timeout`, the `LOCK_TIMEOUT` environment variable and the `--lock-timeout` flag are accepted as other names for `lock_acquire_wait_budget`. `eviction_lock_timeout` is a separate field for the fleet's eviction lock, which is also a `flock` and is never broken.

### Validation scan

Once a day, a scan reads every `.meta` file and corrects the tracked cache size. It starts at midnight local time plus up to an hour of random delay, and its schedule is not configurable. [The validation scan](SHARED_STORAGE.md#the-validation-scan) describes its full and rolling modes, how it corrects the size and what to monitor.

| Field | Default | Meaning |
|---|---|---|
| `validation_max_duration` | `4h` (range `10m` to `23h`) | The scan's time budget. It selects whether the next scan is full or rolling and stops a rolling scan at a shard boundary; a full scan runs to completion |
| `validation_threshold_warn` | `5.0` | Drift, in percent, above which a rolling scan logs at WARN |
| `validation_threshold_error` | `20.0` | Drift, in percent, above which a rolling scan logs at ERROR |
| `validation_frequency` | `23h` | Accepted and logged at startup, but it does not change the schedule. A value outside 1 to 168 hours stops the proxy at startup. Leave it at the default |

### Limits with no configuration field

These limits are fixed and published on `/metrics` beside the figure each one bounds:

| Limit | Value | Published as | Compare with |
|---|---|---|---|
| Staged ranges in the fleet | 32,800,000 | `write_cache.staged_range_budget` | `write_cache.staged_range_count` |
| Cache shards one proxy's staged fold can revisit in 24 hours | 147,456 | `write_cache.staged_fold_shards_reachable_per_instance` | `write_cache.staged_fold_shards` |
| Cache shards one proxy's ledger repair can cover in 24 hours | 589,824 | `write_cache.ledger_repair_shards_reachable_per_instance` | `write_cache.ledger_repair_observed_shards` |

At the staged range limit, the write cache stops caching new uploads, which still succeed. A cache shard is a bucket paired with the first two hex digits of a key hash, so a bucket has at most 256. The shard limits are per proxy, and proxies take shards from each other, so adding proxies raises the fleet's reach. The [`write_cache` metrics](METRICS_REFERENCE.md#write_cache) show whether the fleet keeps up.

## Download coordination

```yaml
cache:
  download_coordination:
    enabled: true          # default true
    wait_timeout_secs: 30  # default 30
```
`wait_timeout_secs` outside 5 to 120 is clamped to that range, with a startup warning.

When several requests miss the cache for the same object, range or part at once, one fetches from S3 and the others wait for it and are served from the cache. A waiter that times out rejoins the fetch still in flight, up to `download_coordination.max_waiter_resubscriptions` times (default `3`), and then receives HTTP 504; it never starts a duplicate fetch. See [Download coordination](CACHE_READ_PATHS.md#download-coordination).

## Cache size tracking

Each proxy records the size changes it makes in a delta file of its own, and consolidation sums the delta files into `size_tracking/size_state.json` under `cache_dir` each `consolidation_interval`. Eviction acts on that total. [Size tracking](SHARED_STORAGE.md#size-tracking) describes the mechanism and how concurrent writes can over-count.

Orphan reclamation and the eviction work list have no fields. Both start once every proxy active on the cache volume, meaning one that wrote a journal entry in the last hour, has recorded that it is ready for them. Until then, and whenever no proxy has written a journal entry for an hour, orphan reclamation waits and only the eviction lock holder deletes; both resume with the next journal entry. [Orphan recovery](SHARED_STORAGE.md#orphan-recovery) describes how orphan range files are found and removed.

## Compression configuration

```yaml
compression:
  enabled: true
  threshold: 1024                    # Size floor in bytes; smaller writes always skip compression
  preferred_algorithm: "lz4"         # Options: lz4 (parsed but otherwise inert)
```

**`threshold`** (`usize`, default `1024`): size floor in bytes. Writes smaller than
this always skip compression, regardless of extension or any `cache_rules.json`
override.

**`preferred_algorithm`** (`String`, default `"lz4"`): parsed and stored per entry, but
otherwise inert; LZ4 is the only implemented algorithm. Changing it does not invalidate
existing cache entries, which continue to be read with the algorithm recorded in their
own metadata.

`compression.content_aware` is ignored with a startup warning; delete it.

### Content-aware compression

When no rule sets `compression_enabled` for a key, the proxy stores already-compressed formats, such as images, video, archives and office documents, without LZ4 compression. A rule that sets `compression_enabled` overrides this either way ([Cache rules](#cache-rules)). The [built-in denylist](COMPRESSION.md#built-in-denylist) lists the extensions and how they match.

## Connection pooling

```yaml
connection_pool:
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "55s"        # 1-300s. S3 can close an idle connection sooner; see CONNECTION_POOLING.md

  # HTTP Connection Keepalive
  keepalive_enabled: true
  max_idle_per_host: 100
  max_lifetime: "300s"       # Max age of a pooled connection, 60-3600s (see below)
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
  # succeeds; a DNS refresh does not restore it. Probes run on the
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

The most hostnames the connection pool tracks, each with its own IP distributor, so that many distinct hostnames cannot grow memory without bound. Each DNS-resolved hostname takes a slot, and so does each hostname that a suffix (`*.`) `endpoint_overrides` entry matches, including every virtual-hosted bucket hostname under it. Exact override entries take none. Past the cap, the proxy logs a warning and registers no new hostname. An unregistered hostname connects to the first address its override entry lists, or the first one DNS returns; registered hostnames carry on. Path-style traffic uses a few regional hostnames, far below the default. Raise it if a suffix override serves more bucket hostnames than the cap.

### TCP socket options

These map directly onto socket options on each upstream connection. They are distinct
from `keepalive_enabled` and `max_idle_per_host`, which govern HTTP-level connection
reuse. See [Connection pooling](CONNECTION_POOLING.md) for how the two layers interact.

**`keepalive_idle_secs`** (`u64`, default `15`): `TCP_KEEPIDLE`. Idle seconds before the
kernel starts sending keepalive probes.

**`keepalive_interval_secs`** (`u64`, default `5`): `TCP_KEEPINTVL`. Seconds between
probes.

**`keepalive_retries`** (`u32`, default `3`): `TCP_KEEPCNT`. Unanswered probes before the
connection is declared dead. At the defaults a dead peer is detected in roughly
15 + (5 × 3) = 30 seconds.

**`tcp_recv_buffer_size`** (`Option<usize>`, default `null`): `SO_RCVBUF` hint in bytes.

Leave it unset. `null` lets the kernel tune the receive window. An explicit value turns that
tuning off and caps each connection at `buffer / RTT` whatever the bandwidth, so a value
chosen for a low-latency path throttles a high-latency one.

### Upstream timeout (stalled-response fast-fail)

These fields set how soon the proxy gives up on a stalled connection to the origin. They apply to the request that fetches from S3, not to requests waiting on that fetch through [download coordination](#download-coordination).

```yaml
connection_pool:
  # upstream_first_byte_timeout: "5s"
  # upstream_idle_timeout: "5s"
  # upstream_idle_retries: 2
```

**`upstream_first_byte_timeout`** (`Duration`, default `"5s"`)

How long the proxy waits, after connecting, for the first byte of the response. On timeout, before anything has been sent to the client, the proxy retries up to `upstream_idle_retries` times, failing over between the origin's addresses, and then returns an error. Set it above the slowest first-byte times you see from the origin and well below the client's 60-second read timeout, so the proxy retries before the client gives up.

**`upstream_idle_timeout`** (`Duration`, default `"5s"`)

Mid-stream idle watchdog: if no bytes are received from upstream for this duration after data has started flowing, the proxy terminates the client connection so the client's own retry logic engages. A slow-but-steady transfer (bytes arriving within the timeout window) is never aborted: the criterion is "no bytes for T", not "transfer took longer than T". The timer is paused under client backpressure (when the proxy is not reading from upstream because the client write half is slow).

**`upstream_idle_retries`** (`usize`, default `2`)

How many times the proxy retries a fetch whose first byte timed out before anything was sent to the client. Each retry fails over between addresses; when the retries run out, the proxy returns an error.

### Hedged requests governor

A per-proxy cap on hedged upstream requests. Cache rules turn hedging on per key pattern with `hedging_enabled`, `hedge_trigger_after` and `hedge_max_per_request`; this field only limits the share of a proxy's in-flight fetches that may be hedges, which bounds the extra S3 requests across every key with hedging on.

```yaml
connection_pool:
  hedged_requests:
    max_inflight_fraction: 0.1
```

**`max_inflight_fraction`** (`f64`, default `0.1`)

Maximum fraction of in-flight upstream fetches that may be hedges. Valid range `[0.0, 1.0]`. When `(in_flight_hedges + 1) / max(in_flight_fetches, 1)` exceeds this value, new hedges are suppressed and the original fetch is served alone.

The first hedge is always admitted when no other hedge is in flight, so a lightly loaded proxy still hedges and `0.0` does not turn hedging off. To turn it off, remove `hedging_enabled` from the rules. Each proxy enforces the cap on its own.

See [Hedged upstream requests](HEDGING.md) for the mechanism, the rule fields, and how to tell whether hedging is earning its cost.

### Connection keepalive

The pool reuses TCP and TLS connections so repeat requests skip the handshake. [Connection pooling](CONNECTION_POOLING.md) explains how the HTTP-level pool and the TCP socket options above interact.

`max_lifetime` (default `300s`, range `60s` to `3600s`) caps how long a pooled upstream
connection is used after it opens. The pool retires a connection that has reached the cap the
next time a request checks it out, and that request opens a fresh one. A request already
running on the connection completes, and a response still streaming from it streams to the
end. Retirements count in `max_lifetime_closures` on `/metrics`, separately from idle closures.

`idle_timeout` retires a connection nothing is using; `max_lifetime` also retires one in
constant use, such as a connection to an S3 address that has since left DNS. Lowering it costs
one TCP and TLS handshake per connection per interval.

**Tuning guide**:

| Setting | Low Value | High Value | Recommendation |
|---------|-----------|------------|----------------|
| max_idle_per_host | Less memory/FDs | More concurrent reuse | 100 (default), reduce for memory-constrained environments |
| max_lifetime | Connections recycled sooner, so a rotated-out S3 IP is dropped sooner, at the cost of more TCP/TLS handshakes | Fewer handshakes, but the proxy keeps sending traffic down a connection to the same S3 IP for longer | 300s (default); raise towards 3600s only if handshake cost is measurable, and keep it above `idle_timeout` |
| dns_refresh_interval | S3 IP rotation is picked up sooner, at the cost of more DNS queries | Fewer DNS queries, but the distributor keeps rotated-out IPs longer | 60s (default). The refresh runs on the `pool_check_interval` tick and re-resolves each endpoint whose `dns_refresh_interval` has elapsed, so values below `pool_check_interval` behave as `pool_check_interval` |
| pool_check_interval | Idle connections closed and excluded IPs probed sooner | Less CPU | 10s (default) |
| health_probe_initial_cooldown | Quicker recovery from a transient failure | Fewer probes against a genuinely dead IP | 5s (default); raise it if brief S3 blips cause probe churn |
| health_probe_max_cooldown | An unreachable IP is retried more often | Less wasted work on a dead IP | 300s (default); the doubling backoff reaches it after ~6 failed probes from 5s |

Set `keepalive_enabled: false` to disable connection reuse (useful for debugging
connection issues).

## DNS server configuration

By default the proxy resolves origin hostnames through the Google and Cloudflare public resolvers (`8.8.8.8`, `8.8.4.4`, `1.1.1.1` and `1.0.0.1`) and never reads `/etc/hosts`; `connection_pool.dns_servers` replaces them for upstream requests through the connection pool. The HTTPS passthrough on port 443, CONNECT tunnels through the TLS proxy listener, and destination-policy lookups on the HTTP forwarding path always use the public resolvers.

The proxy ignores `/etc/hosts` because a hosts file or DNS zone may point S3's hostnames at the proxy itself. For the same reason, `dns_servers` must not name a resolver that answers S3's hostnames with the proxy's address, which would create a loop.

```yaml
connection_pool:
  dns_servers: ["10.0.0.2", "10.0.0.3"]  # Internal resolvers
```

Set `dns_servers` to use internal resolvers, or for S3 PrivateLink from on premises (below).

## S3 PrivateLink (interface VPC endpoints)

With S3 interface VPC endpoints, the proxy must reach S3 through the endpoint's network interfaces instead of S3's public addresses. The default public resolvers return public addresses, so traffic would bypass the endpoint. How clients reach the proxy does not change; only the proxy's connection to S3 does.

Choose the mechanism by where the proxy runs:

| Proxy location | Use | Private DNS on the endpoint |
|---|---|---|
| In AWS, in the VPC that holds the endpoint | [`endpoint_overrides`](#endpoint_overrides) | Disabled, if a DNS zone routes clients to the proxy |
| On premises, reaching S3 over AWS Direct Connect or VPN | [`dns_servers`](#dns_servers-with-route-53-resolver) pointing at Route 53 Resolver inbound endpoints in that VPC | Enabled |

In the endpoint's VPC, private DNS creates a private hosted zone for `s3.<region>.amazonaws.com`. If clients reach the proxy through a DNS zone for the same names, the two collide, and AWS rejects the endpoint with a conflicting-domain error where such a zone already exists. Any resolver in that VPC also answers from the zone that routes clients to the proxy, so `dns_servers` cannot help there.

On premises, the Resolver inbound endpoints sit on the VPC's subnets, reachable over Direct Connect or VPN, and answer from the VPC's resolver, which returns the interface endpoint's addresses. Do not point `dns_servers` at the on-premises resolver that sends clients to the proxy: it resolves S3 to the proxy and creates a loop. `endpoint_overrides` is the on-premises fallback when inbound endpoints are not available.

The endpoint-specific DNS names (`*.vpce-<id>.s3.<region>.vpce.amazonaws.com`) cannot replace either mechanism. The proxy takes the upstream hostname from the client's signed `Host` header and has no field to replace it. To use an endpoint without private DNS, resolve those names yourself and list the addresses in `endpoint_overrides`.

### `endpoint_overrides`

Maps S3 hostnames directly to PrivateLink ENI IPs, taking DNS out of the path.

```yaml
connection_pool:
  endpoint_overrides:
    # Exact match: single hostname
    "s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]

    # Suffix (wildcard) match: all virtual-hosted bucket hostnames in a region
    "*.s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]

    # MRAP global endpoint (requires the com.amazonaws.s3-global.accesspoint VPCE)
    "*.accesspoint.s3-global.amazonaws.com": ["10.0.3.100"]

    # Regional access points
    "*.s3-accesspoint.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]
```

**Matching precedence**: keys starting with `*.` are suffix patterns matching any hostname
ending with that suffix. Exact matches take precedence; among suffix matches the longest
wins. The proxy load-balances across the listed IPs for both kinds. An exact entry gets its
distributor at startup. A hostname matched by a suffix entry gets its own distributor, holding
that entry's IPs, on its first request, so the first connection to each such hostname goes to
the entry's first listed IP and later ones round-robin.

**Registration cap**: each hostname a suffix entry matches takes one
[`max_registered_endpoints`](#connection-pooling) slot, from the same pool as
DNS-resolved hostnames. Past the cap, a new hostname gets no distributor and every one of its
connections goes to the entry's first listed IP; hostnames already registered are unaffected.
The proxy logs `Using endpoint override` at INFO once, when a hostname is registered, and at
debug on each `dns_refresh_interval` after that.

**IPs listed in more than one entry**: an IP listed in both an exact and a suffix entry is
attributed to the exact entry's hostname; an IP shared only by suffix-matched hostnames is
attributed to the lexicographically smallest of them. That attribution names the hostname in
connection-pool metrics and the hostname the recovery probe uses when it retests an excluded IP.
It does not decide the TLS server name or which distributor a failure removes the IP from: both
follow the hostname the request itself targeted. Failure counts and the excluded state belong to the
address, so every hostname sharing it sees an exclusion, and the recovery probe restores it.

**TLS version**: a host matched by `endpoint_overrides` negotiates TLS 1.2 only, because
interface endpoints do not support 1.3. Every other host negotiates TLS 1.2 or 1.3.

**Scope**: overrides apply to both the HTTP caching path and the HTTPS passthrough handler
on port 443. `dns_servers` does not cover the passthrough handler.

**Health exclusion**: an ENI IP that health tracking excludes, from an exact or a suffix
entry, returns to round-robin only when a recovery probe to it succeeds. See
[PrivateLink (endpoint_overrides) interaction](#privatelink-endpoint_overrides-interaction).

### `dns_servers` with Route 53 Resolver

```yaml
connection_pool:
  dns_servers: ["10.0.1.50", "10.0.2.50"]  # Route 53 Resolver inbound endpoint IPs
```

Use this for proxies on premises only, with private DNS enabled on the interface endpoint. It does not apply to HTTPS passthrough, which always uses the public resolvers.

### Verifying the route

For `dns_servers`, confirm from the proxy host that the inbound endpoint returns the interface endpoint's private addresses, not public S3 addresses:

```bash
dig +short s3.us-west-2.amazonaws.com @10.0.1.50  # Expect private addresses
```

Public addresses mean private DNS is disabled on the interface endpoint: enable it, or use `endpoint_overrides`.

For `endpoint_overrides`, `dig` proves nothing, because overrides bypass DNS. Request an object through the proxy, then confirm that the proxy's upstream connections go to the endpoint's addresses:

```bash
ss -tn state established '( dport = :443 )'  # Remote addresses should be the endpoint's
```

## Upstream transport overrides

Front an S3-compatible store reached on a non-standard transport (plaintext HTTP, or TLS on a port other than 443) without weakening egress to every other destination. `connection_pool.upstream_overrides` maps a specific upstream `host:port` to the transport the proxy uses to connect.

The field is empty by default. With no overrides, every caching-egress connection uses verified TLS on port 443.

```yaml
connection_pool:
  upstream_overrides:
    # Plaintext local store (e.g. MinIO/RustFS on :9000): cleartext, dev only
    "127.0.0.1:9000": { scheme: http }

    # Customer store with a publicly-trusted cert on a non-443 port: validated HTTPS
    "store.example.com:9000": { scheme: https, validate_tls: true }

    # Self-signed store on a secure network segment: HTTPS, no certificate verification
    "store.local:9000": { scheme: https, validate_tls: false }
```

### Key and value shape

Each key is `"host:port"`. The port is part of the key, so the same host on different ports can resolve to different transports (e.g. `host:8080` plaintext and `host:9000` validated TLS). Each value is `{ scheme: http|https, validate_tls: <bool> }`.

### Three transport modes

| `scheme` | `validate_tls` | Transport | Waives a protection? |
|----------|----------------|-----------|----------------------|
| `http`   | (ignored)      | Plaintext HTTP, no TLS handshake | yes: cleartext |
| `https`  | `true` (default) | HTTPS verified against the system trust store | no |
| `https`  | `false`        | HTTPS with no certificate verification (no chain or hostname check) | yes: no MITM protection |

`validate_tls` defaults to `true` when omitted (secure by default); skipping validation is an explicit opt-in. There is no custom-CA mode: validation is either full (system trust store) or none.

### Host matching: DNS names cover subdomains

- An IP literal matches that IP exactly (no subdomain semantics; IP endpoints use path-style only).
- A DNS name matches itself and any subdomain. `store.local:9000` covers `store.local:9000`, `bucket.store.local:9000`, and `my.bucket.store.local:9000`, so one entry serves both path-style and virtual-hosted addressing of a store.
- The port must match. Matching is case-insensitive.
- Where both an exact and a DNS-suffix entry match, the exact match wins; among suffix matches the longest (most specific) wins.
- A bare `*` matcher is rejected (it would match link-local and internal hosts); the entry is skipped with a warning and does not abort startup.

Hostnames resolve via the proxy's own configured `dns_servers`, not the host's default system resolver: the same resolver the rest of the egress uses. A publicly-resolvable name such as `s3.<region>.amazonaws.com` therefore resolves to its real address, so you can override real S3 to plaintext on port 80 the same way you override a local store.

A DNS-name override matches every subdomain at its port, so one entry such as `s3.us-west-2.amazonaws.com:80` covers path-style and virtual-hosted requests to the whole Region on that port. None of them reaches the default transport.

### Security implications

The plaintext and unvalidated-TLS modes waive a security protection:

- `scheme: http` exposes the proxy→origin traffic in cleartext.
- `validate_tls: false` removes MITM protection on the proxy→origin leg: the proxy accepts any certificate.

Use them only for local development or on a secure network segment. The proxy logs a startup warning naming each endpoint and the specific protection waived. Validated HTTPS (`scheme: https` with `validate_tls: true`) waives nothing and is not warned.

When a `validate_tls: true` upstream presents a certificate that fails verification, the proxy returns a non-retryable HTTP 400 with an `UpstreamTLSValidationFailed` error naming the `host:port`. It never falls back to plaintext or unvalidated TLS.

### Standard mode and non-standard upstream ports

The upstream port comes from the request the proxy receives, and the two modes obtain it differently:

- **Forward-proxy mode** receives an absolute-form request whose authority carries the upstream `host:port`, and whose `Host` header must name the same host and port (an omitted port and `:80` count as the same). The `upstream_overrides` lookup keys on that port, including a non-standard one, and a matching entry is dialed on it. With `server.tls.connect_allowlist` set, the destination policy admits an explicit port other than 80 only when it is `server.http_port` (see [Destination Policy](ARCHITECTURE.md#destination-policy-ssrf-protection)).
- **Standard (hosts-file) mode** receives an origin-form request and takes the upstream port from the `Host` header, defaulting to 80 (the caching-egress origin port) when the header carries none. It accepts client connections only on its configured `http_port` (default 80) and `https_port` (default 443, HTTPS passthrough). Hosts-file interception rewrites only the IP, not the port, so a client aimed at a non-standard port connects to that port directly and is not intercepted by the proxy's standard-mode listeners.

The transport itself (plaintext, validated TLS, or unvalidated TLS) is selectable by configuration per `host:port`, in both modes. A request whose port matches no `upstream_overrides` entry uses the default, verified TLS on 443.

Standard mode can front a non-standard port, but only the single port it is bound to. Set `http_port` to that port and the proxy intercepts it: the client connects there, the `Host` header carries it, and an override keyed on that `host:port` matches. What standard mode cannot do is intercept more than one upstream port at a time, or any port other than its configured `http_port` and `https_port` (HTTPS passthrough on 443 does not cache). To front several ports, or to leave the listener on 80/443 while reaching a store on another port, use forward-proxy mode, where each request's authority carries the upstream `host:port`.

In either mode the proxy resolves the upstream hostname with its own configured `dns_servers`, not the host's default system resolver. That separation avoids a loop. The routing that sends traffic to the proxy, whether a client `/etc/hosts` entry, a local DNS zone or `HTTP_PROXY`, points the name at the proxy, while the proxy's own resolver must return the real origin. If the proxy's `dns_servers` resolve the name back to the proxy, egress loops.

### Relationship to `endpoint_overrides`

`upstream_overrides` (how to connect: scheme and validation) is independent of `endpoint_overrides` (what IP a hostname resolves to, plus its TLS-1.2 lock). Reach a store on a non-default transport with `upstream_overrides` alone. Using an IP-literal key keeps the TLS-1.2 lock off, since that lock applies only to hosts listed in `endpoint_overrides`. The two combine only when you must address a store by a hostname the proxy's own DNS cannot resolve, such as a private name needed for TLS SNI or certificate matching. Then `endpoint_overrides` pins the IP and `upstream_overrides` sets the port and transport. Note that any host in `endpoint_overrides` is TLS-1.2-locked, so a validated-TLS upstream override on such a host negotiates TLS 1.2.

### Chaining to another proxy

Together the two can point one proxy's S3 egress at a second proxy, giving a two-tier cache: `endpoint_overrides` pins the S3 hostname to the second proxy's addresses, and `upstream_overrides` selects the transport for that hop. Signed requests survive the extra hop, because every hop forwards the `Host` header verbatim and SigV4 covers that header rather than the TCP peer.

Like the rest of this project, this is sample configuration for you to evaluate, not a tested topology. Three things to get right:

- **Port.** The upstream port comes from the request the first proxy received (see [Standard mode and non-standard upstream ports](#standard-mode-and-non-standard-upstream-ports)), so the second proxy must listen on the port the first one dials. For a TLS hop that means setting its `tls_proxy_port` to that port and moving `http_port` elsewhere, since the two cannot match.
- **Trust.** SNI stays the S3 hostname or, for a bucket whose name contains a dot, the S3 endpoint name without the bucket. So `validate_tls: true` on that hop needs a certificate valid for that name, from a CA the first proxy's host already trusts.
- **Freshness and loops.** Where you rely on `get_ttl: 0`, set it at every tier, because a tier answering from its own cache does not reach S3. And leave the override off the second proxy for the hostname it forwards, so egress cannot loop back.

### Cache-key note

A cache key is a bucket name and an object path. It carries neither the origin host, the port, nor the transport mode; under virtual-hosted addressing the host is read only to recover the bucket name from it. So two different stores sharing a bucket name write into the same cache namespace, whether they differ by port (`host:9000` and `host:9001`) or by hostname (`store-a.example.com` and `store-b.example.com`).

- Two hostnames are two origins, and a bucket name binds to the first origin that caches it. Requests from the other origin are forwarded and served uncached rather than answered from the first origin's copy, at the cost of caching for that bucket name at the second store. Entries cached before the bucket had a recorded origin are attributed to the first origin to claim it, so the binding does not separate those. See [One Bucket Name at Two Origins](CACHING.md#one-bucket-name-at-two-origins).
- Two ports on one hostname are the same origin, so that binding does not separate them. An object at the same path on both collides, and whichever was fetched first is served until it expires or is invalidated.

To front two distinct stores and cache for both, give them distinct bucket names (or distinct key paths). A distinct port does not separate them, and a distinct hostname prevents the collision rather than resolving it.

## IP distribution

IP distribution spreads upstream connections across every address an endpoint resolves to. Without it, the HTTP client pools connections by hostname, so every request shares one pool however many addresses DNS returns.

```yaml
connection_pool:
  ip_distribution_enabled: true   # default true
  max_idle_per_ip: 10             # default 10, range 1-100
```

### When it helps

One HTTP/1.1 connection to S3 carries one request at a time, so throughput comes from many connections in parallel, as Amazon S3's [performance guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html) recommend. Spreading them across S3's front-end addresses avoids per-address connection limits and throttling. It does not raise the per-connection rate.

### How IP distribution works

1. The connection pool resolves the endpoint's addresses through DNS, or takes them from `endpoint_overrides`.
2. Each outgoing request takes the next address in round-robin order.
3. The proxy rewrites the request's URI authority from the hostname to that address, for example from `s3.eu-west-1.amazonaws.com` to `52.92.17.224`.
4. The HTTP client keeps a separate connection pool for each address.
5. TLS SNI and the `Host` header keep the original hostname, so the SigV4 signature stays valid. For a bucket whose name contains a dot, SNI is the S3 endpoint name without the bucket; see [Per-IP pool isolation](CONNECTION_POOLING.md#5-per-ip-pool-isolation).

### Connection capacity

When IP distribution is enabled, `max_idle_per_ip` replaces `max_idle_per_host` for the hyper client. Total idle connections across all IPs:

```text
total_idle = number_of_IPs × max_idle_per_ip
```

S3 typically returns ~8 IPs per endpoint, so the default of 10 yields ~80 total idle connections. Adjust `max_idle_per_ip` based on your throughput needs:

| IPs | max_idle_per_ip | Total Idle |
|-----|-----------------|------------|
| 4   | 10              | 40         |
| 8   | 10              | 80         |
| 8   | 25              | 200        |
| 16  | 5               | 80         |

### Graceful degradation

- If no IPs are available (DNS not yet resolved, all IPs unhealthy), the proxy falls back to hostname-based routing matching the default behavior: it resolves the hostname and connects to the first address. For a hostname covered by `endpoint_overrides` that is the entry's first listed IP, even if it is excluded
- If URI rewriting fails for a request, the proxy forwards using the original hostname and logs a warning
- During startup before the first DNS resolution completes, requests use hostname-based routing

### Compatibility

- Works with both DNS-resolved IPs and static `endpoint_overrides` (PrivateLink)
- Preserves TLS SNI (original hostname, or the S3 endpoint name for a bucket whose name contains a dot) for successful TLS handshakes
- Preserves Host header for AWS SigV4 signature validity
- IP set updates automatically on DNS refresh; stale IPs are removed within one refresh cycle

### PrivateLink (endpoint_overrides) interaction

When an `endpoint_overrides` entry provides the IP set, IP distribution round-robins across the listed ENI IPs identically to DNS-resolved IPs. Two differences from DNS-based distribution:

1. **When the distributor appears**: an exact entry seeds its distributor at startup. A hostname matched only by a suffix (`*.`) entry gets a distributor of its own on its first request, holding the suffix entry's IPs; that first request connects to the entry's first listed IP. Each such hostname counts toward `max_registered_endpoints`; past the cap, a new hostname connects to the entry's first listed IP every time.
2. **Health exclusion**: an excluded IP returns to round-robin only when a recovery probe succeeds, for DNS-resolved and override IPs alike; a DNS refresh does not restore it. If you have only 1–2 ENI IPs, a transient network issue could exclude all of them. The proxy then falls back to hostname-based routing, which for an overridden hostname means the entry's first listed IP, excluded or not, never a public S3 address. Monitor the health check endpoint for excluded IPs in PrivateLink deployments.

## Download bandwidth QoS configuration

These fields cap the total rate of cache-miss downloads from the origin and share it fairly between callers or buckets. Every field is optional, and the feature is off by default. [Download bandwidth QoS](BANDWIDTH_QOS.md) explains the mechanism.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `download_bandwidth.max_bytes_per_sec` | `u64` | `0` | Ceiling on cache-miss downloads, in bytes per second. `0` turns the feature off |
| `download_bandwidth.caller_id.enabled` | `bool` | `false` | Use the User-Agent `app/<value>` token, when present and valid, as the fairness key |
| `download_bandwidth.caller_id.validation_regex` | `string` | `null` | A regex the caller value must match, or the request falls back to its bucket. An invalid regex stops startup |
| `download_bandwidth.caller_id.max_len` | `usize` | `64` | The longest caller value, in bytes of UTF-8. A longer value is not truncated: the request falls back to its bucket |
| `download_bandwidth.max_tracked_classes` | `usize` | `1024` | Cap on per-class metric series |
| `download_bandwidth.fleet.fallback_instance_count` | `u32` | `1` | Floor on the proxy count `N` that divides the ceiling: `N` is the number of proxies counted or this value, whichever is larger |
| `download_bandwidth.fleet.instance_staleness` | duration | `90s` | A proxy whose heartbeat file is older than this is not counted in `N` |
| `download_bandwidth.fleet.refresh_interval` | duration | `30s` | How often each proxy touches its heartbeat and reads the others'. At least `10s`, or startup fails |

In a fleet, set `fallback_instance_count` to the number of proxies, so the fleet stays within the ceiling when proxies cannot count each other. It is required where each proxy has its own `cache.cache_dir`: there the count is always 1, and this field alone divides the ceiling ([Set the QoS instance count](LOCAL_NVME_CACHE.md#set-the-qos-instance-count)). An `instance_staleness` below twice `refresh_interval` is raised to three times `refresh_interval`, with a startup warning; [Fleet sharing](BANDWIDTH_QOS.md#fleet-sharing-cap--n) explains why.

## Logging configuration

```yaml
logging:
  access_log_dir: "./tmp/logs/access"
  app_log_dir: "./tmp/logs/app"
  local_state_dir: "/var/lib/s3-proxy"  # default; keep it on the instance's own disk
  access_log_enabled: true
  access_log_mode: "all"                # all or cached_only
  log_level: "info"                     # error, warn, info, debug or trace
  access_log_flush_interval: "5s"       # default
  access_log_buffer_size: 1000          # default
```

The proxy writes access logs in the [S3 server access log format](https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html), so tools and Athena queries written for S3's own logs read them unchanged; [Access log format](ACCESS_LOG_FORMAT.md) describes each field. `access_log_mode: all` logs every request. `cached_only` logs only the requests served from the cache, which never reach S3 and so are missing from S3's own access logs; use it when S3 server access logging is on, to avoid logging cache misses twice. `log_level` sets the detail of the application log, from `error` to `trace`; `info` is the recommended level.

Access logs go to `{access_log_dir}/YYYY/MM/DD/{YYYY-MM-DD-HH-MM-SS}-{hostname}`, a new file every `access_log_file_rotation_interval`. Application logs go to `{app_log_dir}/{hostname}/s3-proxy.log.{date}`.

**Local state.** `local_state_dir` (default `/var/lib/s3-proxy`) holds `pending-stamps.log`. When the cache volume refuses the metadata lock for a write, the proxy records the key in this file before it responds, and retries the invalidation until it lands, including after a restart ([Write-through ordering](CACHING.md#write-through-ordering)). Put the directory on the instance's own disk, not on the cache volume or a shared log volume: the file exists for the time the cache volume is unavailable, and one proxy process locks it. It holds about one line per pending key, a few megabytes even during a long outage.

The systemd unit in `config/s3-proxy.service` and the example user-data scripts create the default directory with `StateDirectory=s3-proxy`. A unit with `ProtectSystem=strict` and neither that line nor the directory in `ReadWritePaths` leaves it read-only. In a container, mount a volume for it ([Cache persistence](DOCKER.md#cache-persistence)).

The proxy refuses a directory inside `cache_dir`, including through a symlink, and one on a file system it does not know to be local. It accepts ext2, ext3, ext4, XFS, btrfs, tmpfs, overlayfs, ZFS and f2fs. It refuses NFS (including EFS, and FSx for OpenZFS or NetApp ONTAP over NFS), SMB or CIFS, Lustre (including FSx for Lustre), GPFS, any FUSE mount, and any other type or one it cannot identify. A cache on local disk, on the root volume beside the default directory, keeps the log. When the proxy refuses the directory, or cannot create or write it, it still starts and logs an ERROR naming the reason. It then reports `cache.write_stamp_log_memory_only` as `1` and holds the retries in memory only, so a restart drops them. A log it can still read is replayed from memory.

### Buffered access logging

The proxy buffers access log entries in memory and writes them when `access_log_flush_interval` has passed since the last write, when the buffer holds `access_log_buffer_size` entries, or at a graceful shutdown. A crash loses the entries not yet written. A shorter interval gives fresher logs at the cost of more writes, which adds up on an NFS log volume; on local SSD, 1 or 2 seconds is reasonable.

### Log retention and rotation

```yaml
logging:
  access_log_retention_days: 30            # default 30, range 1-365
  app_log_retention_days: 30               # default 30, range 1-365
  log_cleanup_interval: "24h"              # default 24h, range 1h-7d
  access_log_file_rotation_interval: "5m"  # default 5m, range 1m-60m
```

Each proxy deletes log files older than their retention period on its own, every `log_cleanup_interval`. The access log sweep covers every correctly named file in `access_log_dir`, including files other proxies wrote, which get one extra day of grace; see [Retention applies to the directory](ACCESS_LOG_FORMAT.md#retention-applies-to-the-directory-not-to-the-writer). Retention deletes whole files, so a shorter `access_log_file_rotation_interval` makes more, smaller files and deletes closer to the retention period. On a proxy with a high request rate, a longer interval keeps the file count down.

## Metrics configuration

```yaml
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  bind_address: "127.0.0.1" # Default: loopback only. Set "0.0.0.0" to allow remote scraping.
  collection_interval: "60s"
```
`collection_interval` (default `60s`) is clamped to 10s through 86,400s, with a startup warning.

**`bind_address`** (`String`, default `"127.0.0.1"`): an IP address, as for `health.bind_address`.

`include_cache_stats`, `include_compression_stats` and `include_connection_stats` are
accepted and have no effect: every figure is always included. Setting one to `false` logs a
startup warning; delete them.

### OTLP export

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
- Prometheus OTLP: `http://prometheus:9090/api/v1/otlp/v1/metrics`
- OpenTelemetry Collector: `http://otel-collector:4318`

An endpoint with no path exports to `/v1/metrics`; any other path is used exactly as written.

**Custom headers** (for authentication):
```yaml
otlp:
  headers:
    Authorization: "Bearer your-token"
    X-Custom-Header: "value"
```

### Per-bucket traffic metrics

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

Enable per-bucket OTLP cumulative counter export. When true, the proxy emits four `ObservableCounter<u64>` instruments per collection cycle (`s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, and `s3proxy.put_requests`), each with a `bucket` attribute and, when prefix attribution is active, a `prefix` attribute. Metric names mirror S3's `BytesDownloaded`/`BytesUploaded`/`GetRequests`/`PutRequests` for direct CloudWatch comparison. Only object reads (GET) and object/part writes (PUT/UploadPart) are tracked.

**`metrics.per_bucket.max_series`** (usize, default `100`)

Maximum number of distinct bucket+prefix series tracked in memory. Once the cap is reached, traffic from new buckets or prefixes is folded into a synthetic `__other__` series (visible in `/metrics` and the dashboard) rather than creating a new entry. Total traffic is conserved; only the series key changes. The proxy logs a warning on the first overflow.

**`metrics.per_bucket.bucket_prefixes`** (map, default `{}`)

Optional per-bucket prefix lists for prefix-level attribution. When configured, requests are attributed to the longest matching prefix. Requests with no matching prefix, or for a bucket with no configured prefixes, are attributed at the bucket level. Empty-string prefixes are rejected with a warning at config load.

## Dashboard configuration

The dashboard is a read-only web interface for cache statistics, recent application log lines and system information. [Dashboard](DASHBOARD.md) describes its pages, API, deployment and security; this section lists its fields.

```yaml
dashboard:
  enabled: true                        # default true
  port: 8081                           # default 8081
  bind_address: "127.0.0.1"            # default 127.0.0.1, loopback only
  cache_stats_refresh_interval: "5s"   # default 5s, range 1-300s
  logs_refresh_interval: "10s"         # default 10s, range 1-300s
```

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Set `false` to not start the dashboard |
| `port` | `8081` | Must differ from `health.port`, `metrics.port` and `server.tls.tls_proxy_port` |
| `bind_address` | `127.0.0.1` | The dashboard has no authentication, so a wider bind lets anyone who can reach the port read cache statistics and recent log lines; see [Security considerations](DASHBOARD.md#security-considerations) |
| `cache_stats_refresh_interval` | `5s` | How often the browser refreshes the statistics |
| `logs_refresh_interval` | `10s` | How often the browser refreshes the log viewer |

`max_log_entries` (default `100`, range 10-10000) is validated and logged at startup but has no effect. Each field has a `DASHBOARD_*` environment variable; see [Environment variable reference](#environment-variable-reference).

## Health check configuration

```yaml
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  bind_address: "127.0.0.1" # Default: loopback only. Set "0.0.0.0" for a load balancer check.
```

`health.check_interval` is accepted and has no effect; a value other than `30s` logs a startup warning.

**`bind_address`** (`String`, default `"127.0.0.1"`): an IP address, IPv4 or IPv6 (`127.0.0.1`,
`0.0.0.0`, `::1`, `::`). A hostname such as `localhost` is not resolved. The proxy refuses to
start and names the field, so the listener is never published more widely than configured.

With the defaults, the monitoring listeners answer on the proxy host at `http://localhost:8080/health`, `http://localhost:9090/metrics` and `http://localhost:8081`.

The health and metrics listeners accept only `GET` on their configured `endpoint`. A
different path returns `404 Not Found`; another method on the configured path returns `405
Method Not Allowed` with `Allow: GET`. Change `endpoint` if another path is required.

## HTTPS passthrough

HTTPS passthrough (port 443) forwards each encrypted connection to S3 without terminating TLS, so nothing on it is cached and it needs no certificate. It is opened in `standard` mode on `server.https_port`:

```yaml
server:
  https_port: 443  # default
```

The HTTP listener, the forward proxy and the TLS proxy listener cache; HTTPS passthrough does not. For an encrypted client hop that is still cached, use the [TLS proxy listener](#tls-proxy-listener-configuration). [Configure client routing](GETTING_STARTED.md#3-configure-client-routing) shows how to point each client at a caching listener.

## Duration format

Duration fields take a number followed by a unit, as a quoted string:

| Unit | Suffixes | Example |
|---|---|---|
| Milliseconds | `ms`, `millis`, `millisecond`, `milliseconds` | `"500ms"` |
| Seconds | `s`, `sec`, `secs`, `second`, `seconds` | `"30s"` |
| Minutes | `m`, `min`, `mins`, `minute`, `minutes` | `"5m"` |
| Hours | `h`, `hr`, `hrs`, `hour`, `hours` | `"1h"` |
| Days | `d`, `day`, `days` | `"1d"` |

Environment variables take a bare number instead; see [Environment variable reference](#environment-variable-reference).

## Path expansion

Configuration paths support tilde expansion:

```yaml
cache:
  cache_dir: "~/cache"              # Expands to /home/user/cache
  
logging:
  access_log_dir: "~/logs/access"   # Expands to /home/user/logs/access
```

## Environment variable reference

Environment variables override the configuration file for the fields in this table.
`CONSOLIDATION_SIZE_THRESHOLD`, `LOCK_REFRESH_INTERVAL` and `RECOVERY_MAX_CONCURRENT` are
also accepted, have no effect and log a startup warning, as do the `--lock-refresh-interval` and `--recovery-max-concurrent` command-line flags. Every other field can be set only in
the configuration file.

- **Durations take a bare integer, not a duration string.** `LOCK_ACQUIRE_WAIT_BUDGET=5`
  is 5 seconds; `LOCK_ACQUIRE_WAIT_BUDGET=5s` does not parse. Units are seconds everywhere
  except `VALIDATION_FREQUENCY`, which is in hours.
- **A value that fails to parse is silently ignored**, leaving the YAML or default value
  in place. There is no warning. Check the startup config log line if an override does
  not appear to take effect.

| Variable | Field | Example |
|----------|-------------------|---------|
| `HTTP_PORT` | `server.http_port` | `HTTP_PORT=80` |
| `HTTPS_PORT` | `server.https_port` | `HTTPS_PORT=443` |
| `MAX_CONCURRENT_REQUESTS` | `server.max_concurrent_requests` | `MAX_CONCURRENT_REQUESTS=1000` |
| `CACHE_DIR` | `cache.cache_dir` | `CACHE_DIR=/var/cache/s3-proxy` |
| `RAM_CACHE_ENABLED` | `cache.ram_cache_enabled` | `RAM_CACHE_ENABLED=true` |
| `WRITE_CACHE_ENABLED` | `cache.write_cache_enabled` | `WRITE_CACHE_ENABLED=true` |
| `PARALLEL_SCAN` | `cache.initialization.parallel_scan` (no effect) | `PARALLEL_SCAN=true` |
| `PROGRESS_LOGGING` | `cache.initialization.progress_logging` (no effect) | `PROGRESS_LOGGING=true` |
| `SCAN_TIMEOUT` | `cache.initialization.scan_timeout` (no effect) | `SCAN_TIMEOUT=300` (seconds) |
| `LOCK_ACQUIRE_WAIT_BUDGET` | `cache.shared_storage.lock_acquire_wait_budget` | `LOCK_ACQUIRE_WAIT_BUDGET=5` (seconds) |
| `LOCK_TIMEOUT` | `cache.shared_storage.lock_acquire_wait_budget`, as another name for `LOCK_ACQUIRE_WAIT_BUDGET`; logs a startup warning | `LOCK_TIMEOUT=60` (seconds) |
| `LOCK_MAX_RETRIES` | `cache.shared_storage.lock_max_retries` | `LOCK_MAX_RETRIES=5` |
| `CONSOLIDATION_INTERVAL` | `cache.shared_storage.consolidation_interval` | `CONSOLIDATION_INTERVAL=5` (seconds) |
| `EVICTION_LOCK_TIMEOUT` | `cache.shared_storage.eviction_lock_timeout` | `EVICTION_LOCK_TIMEOUT=60` (seconds) |
| `VALIDATION_FREQUENCY` | `cache.shared_storage.validation_frequency` | `VALIDATION_FREQUENCY=23` (hours; outside 1 to 168 stops the proxy at startup, otherwise no effect, see [Validation scan](#validation-scan)) |
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
| `DASHBOARD_MAX_LOG_ENTRIES` | `dashboard.max_log_entries` | `DASHBOARD_MAX_LOG_ENTRIES=100` (has no effect) |

No environment variable sets `cache.max_cache_size`, any `logging.*` field
other than the two directories and the level, any `cache.metadata_cache.*` field, any
TTL, or any `connection_pool.*` field.

## Example configurations

The [example configurations](examples/) are complete YAML files you can start the proxy with:

| Profile | What it sets up |
|---|---|
| [Development](examples/config-development.yaml) | Local cache, debug logging, unprivileged ports, dashboard on loopback |
| [Production](examples/config-production.yaml) | Standard ports, the TLS proxy listener, a cache volume shared by a fleet, OTLP export |
| [Large cache](examples/config-high-performance.yaml) | 1 TiB disk cache and 10 GiB RAM cache, long-lived connections, slower dashboard polling |

```bash
s3-proxy --config docs/examples/config-development.yaml  # Start with the development profile
```

The [annotated example configuration](../config/config.example.yaml) shows every field with a comment.

## Troubleshooting

### High memory usage

- Lower `cache.max_ram_cache_size` or `server.max_concurrent_requests`.
- Set `server.max_inflight_buffer_bytes` to bound buffered bodies ([In-flight memory ceiling](#in-flight-memory-ceiling)).
- Lower `connection_pool.max_idle_per_ip`, or `max_idle_per_host` with IP distribution off.
- Turn the dashboard off if you do not use it: `dashboard.enabled: false`.

### High disk usage

- Lower `cache.max_cache_size`.
- Set `actively_remove_cached_data: true`, so the validation scan deletes expired entries.
- Lower `get_ttl` for data that changes often.

### Poor cache hit rate

- Raise `get_ttl`.
- Check that cache rules match the keys you expect ([Cache rules](#cache-rules)).
- Watch `cache.cache_hit_rate_percent` on `/metrics`.

### Slow performance

- Keep `keepalive_enabled` at its default, `true`, and set `ram_cache_enabled: true` (default `false`) if memory allows.
- Raise `max_idle_per_ip`, and `max_ram_cache_size` if memory allows.
- Check `range_merge_gap_threshold` against the latency to the origin ([Range merging](#range-merging)).

### Connection issues

- Raise `connection_timeout`.
- Raise `max_idle_per_ip`, or `max_idle_per_host` with IP distribution off.
- Check `dns_refresh_interval` and the connection pool figures on `/metrics`.

### Dashboard issues

See [Troubleshooting the dashboard](DASHBOARD.md#troubleshooting) for an unreachable dashboard, port conflicts and connection limits.

## See also

- [Getting started](GETTING_STARTED.md): installation, client routing and certificates
- [Upgrading](UPGRADING.md): upgrade steps and the fields that are now ignored
- [Caching](CACHING.md): what is cached, bypass and invalidation, and when to use cache rules
- [Cache freshness](CACHE_FRESHNESS.md): TTLs, revalidation and conditional requests
- [Eviction](EVICTION.md): capacity limits and how eviction works
- [Shared storage](SHARED_STORAGE.md): mount requirements, journals, locks and size tracking in a fleet
- [Compression](COMPRESSION.md): the compression decision and the built-in denylist
- [Connection pooling](CONNECTION_POOLING.md): pooling, IP distribution, health tracking and TCP socket options
- [Hedged upstream requests](HEDGING.md): hedging and its cost cap
- [Download bandwidth QoS](BANDWIDTH_QOS.md): the download ceiling and fair sharing
- [Metrics reference](METRICS_REFERENCE.md): every `/metrics` field
- [Metrics](METRICS.md): per-bucket traffic, and reading the permit and in-flight memory counters
- [OTLP metrics](OTLP_METRICS.md): the OpenTelemetry export
- [Dashboard](DASHBOARD.md): the web interface, its API and its security
- [Access log format](ACCESS_LOG_FORMAT.md): the access log record
- [Error handling](ERROR_HANDLING.md): failure classes, recovery and the upstream idle watchdog
- [Example configurations](examples/): three complete profiles and example cache rules files
- [Annotated example configuration](../config/config.example.yaml): every field with a comment
