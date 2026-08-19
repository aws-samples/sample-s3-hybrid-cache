# Per-Bucket Traffic Metrics

This document covers per-bucket traffic accounting and cache-savings inference, plus
interpretation guidance for the request-completion and in-flight-memory counters.

For a field-by-field reference of the whole `/metrics` payload, see
[METRICS_REFERENCE.md](METRICS_REFERENCE.md).

For the subset of metrics exported over OpenTelemetry, and how to publish to CloudWatch,
Prometheus, or an OTel Collector, see [OTLP Metrics](OTLP_METRICS.md). Note that OTLP
exports only five `request_metrics.*` gauges; the permit counters, `inflight_memory.*`,
the cache hit/miss counters, and `cache.incomplete_range_fallbacks` are `/metrics`-only.

## What is counted

The proxy tracks cumulative per-bucket bandwidth and request counters for **object reads (GET) and object/part writes (PUT, including UploadPart)**. Counters mirror S3's named request metrics — `GetRequests`, `PutRequests`, `BytesDownloaded`, `BytesUploaded` — so the proxy's per-bucket data is directly comparable to S3 CloudWatch request metrics for cache savings inference.

Other operations (HEAD, DELETE, the multipart lifecycle POSTs, and LIST) are intentionally **not** counted — the feature is scoped to GET/PUT bandwidth and request volume.

### Accessing the data

**`/metrics` JSON** (always active):
```json
{
  "bucket_traffic": {
    "my-bucket": {
      "bytes_served": 1073741824,
      "bytes_saved": 805306368,
      "bytes_uploaded": 104857600,
      "get_requests": 5000,
      "put_requests": 200
    }
  }
}
```

- `bytes_served` — total GET bytes delivered to clients (cache hits + S3 fetches). Mirrors S3's `BytesDownloaded`.
- `bytes_saved` — GET bytes served from cache, i.e. S3 transfer cost the proxy avoided. A cache hit adds the response `content-length` here; a miss adds zero.
- `bytes_uploaded` — PUT/UploadPart bytes received from clients. Mirrors S3's `BytesUploaded`.
- `get_requests` / `put_requests` — cumulative object GET / PUT+UploadPart request counts.

**Dashboard** (`/api/bucket-traffic`): the operational dashboard shows a "Per-Bucket Traffic" table alongside the existing cache hit/miss table.

**OTLP** (opt-in): set `metrics.otlp.per_bucket_enabled: true` to emit `s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, and `s3proxy.put_requests` as cumulative counters in CloudWatch (via CloudWatch Agent or ADOT). See [OTLP Metrics — Per-bucket counters](OTLP_METRICS.md#per-bucket-counters-opt-in) for the export configuration.

### Request completion metrics

`request_metrics` is a fixed-cardinality, cumulative view of every response the HTTP
proxy records since startup. It contains `total_requests`, `successful_requests`,
`failed_requests`, `client_error_requests`, `server_error_requests`, and
`rejected_requests`. `rejected_requests` is the top-level counter for every
`503 SlowDown` Shed_Response, for either of two independent causes: the
`server.max_concurrent_requests` permit was already occupied, or (if configured)
the in-flight buffered-byte ledger (`server.max_inflight_buffer_bytes`) could not
admit a reservation. Both cases are included in `failed_requests` and
`server_error_requests` as well.

`permits_total`, `permits_held`, `permits_available`, and `permits_held_peak`
give direct visibility into the first cause: `permits_held` is derived as
`permits_total - permits_available` from the live concurrency semaphore, and
`permits_held_peak` is a process-lifetime high-water mark. A permit now spans
the whole request including its response-body transfer, not just the setup
phase (see [Configuration → Max Concurrent Requests](CONFIGURATION.md#max-concurrent-requests)),
so `permits_held_peak` reflects genuine concurrent transfer load.

`inflight_memory.rejected_total` gives the same visibility into the second
cause — it is the subset of `rejected_requests` specifically attributable to
the ledger, always `<= rejected_requests`, with the difference attributable to
concurrency-limit sheds. `inflight_memory.ceiling_bytes` is reported even when
the ledger is disabled (`0`), so `/metrics` alone confirms whether the feature
is active. See [Configuration → In-Flight Memory Ceiling](CONFIGURATION.md#in-flight-memory-ceiling).

### What `reserved_bytes` covers, and what it does not

`inflight_memory.reserved_bytes` counts bytes claimed by paths that hold a whole
range or object in memory rather than streaming it. From 2.5.0 those are
**response-side** paths almost exclusively — range merge, page widening, buffered
range serving, and the recovery fallbacks. Every upload path streams, so a request
body appears here only on the small non-GET/PUT verbs such as a `DeleteObjects`
POST, which an internal 1 MiB bound covers. A ceiling sized from this figure is in
practice a read-path ceiling; a write-heavy workload will show it near zero.

Three properties matter when sizing a ceiling from it.

**It spans delivery to the client.** A cached range or object stays resident until
its response body has been transmitted or the client has disconnected, and the
claim is held for that whole window — a slow reader's transfer included, not just
the time spent loading from cache. Size a ceiling from `peak_reserved_bytes`
observed under representative load, including your slowest clients and largest
objects.

**It does not cover the streaming paths, by design.** A range at or above
`cache.disk_streaming_threshold` streams from disk through a small bounded frame
channel, so its memory is a fixed per-request cost rather than the range size, and
it reserves nothing. The same applies to a RAM-cache hit, whose bytes are already
resident and accounted against `cache.max_ram_cache_size` — reported alongside as
`ram_cache_max_bytes` so the two budgets can be read together. A workload served
predominantly from RAM or by disk streaming can therefore show a low
`reserved_bytes` while still using substantial memory: the ledger bounds the
buffered paths, not total process memory.
`docs/ARCHITECTURE.md` → Range Read-Path Map records which read path reserves.

**It is not a resident-memory figure.** A reservation is the declared size of the
body or range being buffered, not what the allocator ends up holding. Process RSS
also carries the base footprint, the RAM cache, per-connection streaming buffers,
and allocator slack, and none of those are reserved against the ledger. Read
`reserved_bytes` and `peak_reserved_bytes` as the buffered-path budget they bound;
size an instance from measured RSS under representative load.

One caution when reading `active_requests` against these: it counts **active
TCP connections**, not requests in flight. With HTTP/1 keep-alive a single
connection serves many sequential requests, so this value does not track the
quantity `max_concurrent_requests` limits. Use `permits_held` for that. The
dashboard's `Requests: N / M` tile pairs `permits_held` with the configured
limit and shows the connection count separately alongside it (see
[DASHBOARD.md — Cache Statistics](DASHBOARD.md#cache-statistics)).

For range-cache recovery, `cache.incomplete_range_fallbacks` counts safe responses that
refetched from S3 because the cached extents did not cover the requested interval.
A value above zero means the proxy avoided serving an incomplete range; it is not a
cache hit.

For completed GET and HEAD requests, `cache_hit_requests` and `cache_miss_requests`
record whether the response carried `X-Cache: HIT`. The proxy does not label these
metrics by URL, bucket, object key, client address, or host, so the values remain
bounded under a high-cardinality workload. `requests_per_second` remains a
lifetime-average rate since process start. `active_requests` is the active TCP
connection count, which is different from in-flight HTTP requests when clients reuse
HTTP/1 connections.

Only object reads and object/part writes are counted:

| S3 operation | HTTP | Proxy metric |
|---|---|---|
| GetObject (including range and presigned GETs) | GET | `get_requests` + `bytes_served` (+ `bytes_saved` when served from cache) |
| PutObject, UploadPart | PUT | `put_requests` + `bytes_uploaded` |

A GET is counted only when it carries an object key. Bucket-level GETs with no key (list-objects) are not counted. HEAD, DELETE, CreateMultipartUpload/CompleteMultipartUpload, and list operations do not increment any counter.

### Recording sites (exactly-once)

Each request is counted exactly once:
- **GET** is recorded once at the HTTP/TLS request-completion site (covers cache hit, cache miss, coalesced fetch, and range requests).
- **PUT / UploadPart** is recorded once in the signed write-through handler, where the request-body byte count (`bytes_uploaded`) is available — for `aws-chunked` bodies this is the decoded length, otherwise `Content-Length`.

### Direct cache savings measurement

`bytes_saved` is the proxy's direct measurement of S3 transfer cost avoided — GET bytes served from cache, not fetched from S3. It is available in `/metrics` JSON (always) and `/api/bucket-traffic` (dashboard).

It is **not** exported over OTLP. The per-bucket OTLP surface is four counters only (`s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, `s3proxy.put_requests`), so a backend-side savings figure has to be derived as `bytes_downloaded` minus S3's own `BytesDownloaded`, per the cross-validation below.

```
cache_hit_rate(bucket) ≈ bytes_saved / bytes_served
s3_bytes_actually_fetched(bucket) = bytes_served - bytes_saved
```

`bytes_saved` is a cumulative counter since proxy start; reset on restart.

### `X-Cache` response header

Every response served from the proxy cache carries an `X-Cache: HIT` response header, visible to any HTTP client (curl, AWS SDK, CDN). It is set on full-object 200 responses, range 206 responses (RAM, streaming, and buffered paths), and HEAD responses served from cached metadata. Responses fetched from S3 do not carry the header.

This is the same signal the proxy uses internally to drive the global hit/miss counters (`/api/cache-stats`) and the per-bucket `bytes_saved` counter, so a client can read it to confirm a given response was a cache hit:

```
$ curl -sI http://<proxy>/my-bucket/key | grep -i x-cache
x-cache: HIT
```

### Cross-validating with S3 CloudWatch

For an independent cross-check, subtract S3's own `BytesDownloaded` metric from the proxy's `bytes_served`:

```
savings_cross_check = proxy.bytes_served(bucket) - s3.BytesDownloaded(bucket)
```

This should closely match `proxy.bytes_saved(bucket)`. Differences indicate requests that bypassed the proxy (went direct to S3) or accounting skew between reset boundaries.

**Requirements for this cross-check to be accurate:**
1. **S3 request metrics must be enabled** on the bucket (`put-bucket-metrics-configuration`).
2. **Granularity alignment**: both metrics must cover the same time window.
3. **Multi-instance deployments**: aggregate `proxy.bytes_served` across all proxy instances before subtracting S3's total.
