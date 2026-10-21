# Per-bucket traffic metrics

This page explains how to read the figures that need more than a field definition: per-bucket
traffic and the cache savings it lets you infer, request completion and concurrency, in-flight
memory, the RAM cache's range figures, and the cost of a scrape. The
[Metrics reference](METRICS_REFERENCE.md) defines every `/metrics` field, and
[OTLP metrics](OTLP_METRICS.md) lists what is exported over OpenTelemetry and how to publish it
to CloudWatch, Prometheus or an OpenTelemetry Collector.

OTLP exports six `request_metrics.*` gauges. The permit counters, `cache_hit_requests`,
`cache_miss_requests` and `cache.incomplete_range_fallbacks` are on `/metrics` only. The
`inflight_memory` section is exported field for field, buffered-serve counters included.

## Per-bucket traffic

### What is counted

The proxy keeps cumulative per-bucket bandwidth and request counters for object reads (GET) and
object and part writes (PUT, including UploadPart). The counters mirror S3's named request
metrics (`GetRequests`, `PutRequests`, `BytesDownloaded`, `BytesUploaded`), so the proxy's
per-bucket figures compare directly with S3's CloudWatch request metrics for inferring cache
savings.

| S3 operation | HTTP | Proxy metric |
|---|---|---|
| GetObject (including range and presigned GETs) | GET | `get_requests` + `bytes_served` (+ `bytes_saved` when served from cache) |
| PutObject, UploadPart | PUT | `put_requests` + `bytes_uploaded` |

A GET is counted only when it carries an object key, so bucket-level GETs (list requests) are not
counted. HEAD, DELETE, CreateMultipartUpload, CompleteMultipartUpload and list operations
increment no counter.

### Reading the data

**`/metrics` JSON**, served whenever the endpoint is enabled. `bucket_traffic` is read live on
every scrape; [The endpoint](METRICS_REFERENCE.md#the-endpoint) lists the figures that come from
the background collection instead:

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

- `bytes_served`: total GET bytes delivered to clients, from cache hits and S3 fetches. Mirrors
  S3's `BytesDownloaded`.
- `bytes_saved`: GET bytes served from cache, that is, S3 transfer the proxy avoided. A cache hit
  adds the response `content-length`; a miss adds zero.
- `bytes_uploaded`: PUT and UploadPart bytes received from clients. Mirrors S3's `BytesUploaded`.
- `get_requests` and `put_requests`: cumulative object GET counts and PUT plus UploadPart counts.

**Dashboard** (`/api/bucket-traffic`): the dashboard shows a per-bucket traffic table beside the
cache hit and miss table.

**OTLP** (opt-in): set `metrics.otlp.per_bucket_enabled: true` to export
`s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests` and
`s3proxy.put_requests` as cumulative counters, for example to CloudWatch through the CloudWatch
agent or ADOT. See [Per-bucket counters](OTLP_METRICS.md#per-bucket-counters-opt-in).

### Recording sites

Each request is counted once:

- A GET is recorded once, where the request completes, which covers cache hits, cache misses,
  coalesced fetches and range requests.
- A PUT or UploadPart is recorded in the signed write handler, where the request-body byte count
  (`bytes_uploaded`) is known: the decoded length for `aws-chunked` bodies, otherwise
  `Content-Length`.

### Direct cache savings measurement

`bytes_saved` is the proxy's direct measurement of avoided S3 transfer: GET bytes served from
cache instead of fetched from S3. It is in the `/metrics` JSON (always) and in
`/api/bucket-traffic` (dashboard). It is a cumulative counter since the proxy started and resets
on restart.

It is not exported over OTLP. The per-bucket OTLP instruments are the four counters above, so a
savings figure in a metrics backend has to be derived as `bytes_downloaded` minus S3's own
`BytesDownloaded`, as in the cross-check below.

```text
cache_hit_rate(bucket) ≈ bytes_saved / bytes_served
s3_bytes_actually_fetched(bucket) = bytes_served - bytes_saved
```

### `X-Cache` response header

Every response served from the proxy's cache carries an `X-Cache: HIT` header, visible to any
HTTP client (curl, an AWS SDK, a CDN). It is set on full-object 200 responses, on range 206
responses (RAM, streaming and buffered paths), and on HEAD responses served from cached
metadata. Responses fetched from S3 do not carry it.

The proxy uses the same signal for the global hit and miss counters (`/api/cache-stats`) and for
the per-bucket `bytes_saved` counter, so a client can read the header to confirm that a given
response was a cache hit:

```text
$ curl -sI http://<proxy-host>/my-bucket/key | grep -i x-cache
x-cache: HIT
```

### Cross-validating with S3 CloudWatch

For an independent check, subtract S3's own `BytesDownloaded` metric from the proxy's
`bytes_served`:

```text
savings_cross_check = proxy.bytes_served(bucket) - s3.BytesDownloaded(bucket)
```

The result should be close to `proxy.bytes_saved(bucket)`. A difference means requests bypassed
the proxy and went to S3 directly, or the two counters reset at different times.

The check is accurate only when:

1. S3 request metrics are enabled on the bucket (`put-bucket-metrics-configuration`).
2. Both metrics cover the same time window.
3. In a fleet, `proxy.bytes_served` is summed across every proxy before S3's total is subtracted.

## Request completion metrics

`request_metrics` is a fixed-cardinality, cumulative view of every response the proxy has
recorded since it started. It contains `total_requests`, `successful_requests`,
`failed_requests`, `client_error_requests`, `server_error_requests` and `rejected_requests`.
`rejected_requests` counts every `503 SlowDown` the proxy returns to shed load, for either of two
independent causes: every `server.max_concurrent_requests` permit was taken, or the in-flight
buffered-byte ledger (`server.max_inflight_buffer_bytes`, when set) could not admit a
reservation. Both causes are also counted in `failed_requests` and `server_error_requests`.

`permits_total`, `permits_held`, `permits_available` and `permits_held_peak` show the first
cause directly. `permits_held` is `permits_total - permits_available` from the live concurrency
semaphore, and `permits_held_peak` is a high-water mark for the life of the process. A permit
spans the whole request, including its response-body transfer (see
[Max concurrent requests](CONFIGURATION.md#max-concurrent-requests)), so `permits_held_peak`
reflects real concurrent transfer load. Read `permits_held`, not `active_requests`, for requests
in flight: `active_requests` counts TCP connections (see
[Fields with misleading names](METRICS_REFERENCE.md#fields-with-misleading-names)). The
dashboard's `Requests: N / M` tile pairs `permits_held` with the configured limit (see
[Cache statistics](DASHBOARD.md#cache-statistics)).

`inflight_memory.rejected_total` shows the second cause. It is the part of `rejected_requests`
caused by the ledger, always `<= rejected_requests`, and the difference is the concurrency-limit
sheds. `inflight_memory.ceiling_bytes` is reported even when the ledger is disabled (`0`), so
`/metrics` alone shows whether the ledger is active. See
[In-flight memory ceiling](CONFIGURATION.md#in-flight-memory-ceiling).

For completed GET and HEAD requests, `cache_hit_requests` and `cache_miss_requests` record
whether the response carried `X-Cache: HIT`. `cache.incomplete_range_fallbacks` counts range
responses that were fetched again from S3 because the cached extents did not cover the requested
interval. A value above zero means the proxy declined to serve an incomplete range; those
responses are not cache hits.

## What `reserved_bytes` covers, and what it does not

`inflight_memory.reserved_bytes` counts bytes claimed by paths that hold a whole range or object
in memory rather than streaming it. Those are almost all response paths: range merge, page
widening, buffered range serving and the recovery fallbacks. Every upload path streams, so a
request body appears here only for the small non-GET, non-PUT requests such as a
`DeleteObjects` POST, which an internal 1 MiB bound covers. A ceiling sized from this figure is
in practice a read-path ceiling; a write-heavy workload shows it near zero.

Three properties matter when sizing a ceiling from it.

**It spans delivery to the client.** A cached range or object stays reserved until its response
body has been sent or the client has disconnected, a slow reader's transfer included. Size a
ceiling from the `peak_reserved_bytes` you observe under representative load, including your
slowest clients and largest objects.

**It does not cover the streaming paths.** A range at or above `cache.disk_streaming_threshold`
streams from disk through a small bounded frame channel, so its memory is a fixed cost per
request rather than the range size, and it reserves nothing. The same holds for a RAM cache hit,
whose bytes are already resident and counted against `cache.max_ram_cache_size` (reported beside
it as `ram_cache_max_bytes`, so the two budgets can be read together). A workload served mostly
from RAM or by disk streaming can therefore show a low `reserved_bytes` while using a lot of
memory: the ledger bounds the buffered paths, not total process memory.
[Range read-path map](ARCHITECTURE.md#3a-range-read-path-map) records which read paths reserve.

**It is not a resident-memory figure.** A reservation is the declared size of the body or range
being buffered, not what the allocator ends up holding. Process RSS also includes the base
footprint, the RAM cache, per-connection streaming buffers and allocator slack, and none of
those are reserved against the ledger. Read `reserved_bytes` and `peak_reserved_bytes` as the
budget for the buffered paths, and size a proxy host from its measured RSS under representative
load.

## Reading the RAM cache's range figures

**Size the RAM cache by the cached extent.** A small ranged read of a large cached extent
promotes the whole extent into RAM. A handful of 4 KiB reads at an interior offset of a 32 MiB
object leave `ram_cache_size` holding that object's whole extent, thousands of times the bytes
any client asked for. So a `max_ram_cache_size` of 2 GiB holds only about sixty such extents
however small the reads are, and many small reads spread over many large objects fill the RAM
cache far sooner than the read sizes suggest. Divide `ram_cache_size` by
`cache.ram_cache_entries` for the mean entry size.

**A sub-range access pattern cannot push `cache.ram_cache_hit_rate_percent` much above 50%.** A
ranged GET performs two RAM lookups under different keys, and only the extent-keyed one can hit
when the request is a sub-range of the extent, so each request records one miss however warm
the RAM cache is. Such a workload settles near `(N-1)/(2N)`, around 47% in practice, where an
exact-extent read pattern on the same object reaches roughly 87%. This holds for clients that
sign `Range` (the AWS CLI and SDKs) and for clients that do not (presigned URLs) alike.

Three further cautions apply when reading these fields:

- `cache.ram_cache_hit_rate_percent` is cumulative since the process started, so it cannot be
  attributed to any one access pattern. A window containing two patterns reports a value between
  them that matches neither, and the field alone cannot tell that window from a single-pattern
  one. Derive the rate from `cache.ram_cache_hits` and `cache.ram_cache_misses` over the
  interval you care about, and quote the denominator beside it.
- `ram_cache_hit_rate_percent: 0.0` is ambiguous: a RAM cache with traffic and no hits reads
  `0.0`, and so does one with no traffic. Read the hit and miss counters to tell them apart; if
  both are `0`, there was no traffic.
- `cache.ram_cache_evictions` does not move when entries are dropped by cache invalidation. A
  DELETE through the proxy can take `ram_cache_size` to zero with the eviction counter unchanged.
  `cache.ram_cache_entries` is the level that shows the drop.

**`X-Cache: HIT` does not say whether the RAM cache or the disk cache served the response.** The
disk cache sets the same header, and a response can carry it while the RAM counters record only
misses at the same moment. The header reports that the response came from cache and nothing
finer.

## Scrape cost and the scrape deadline

A scrape does not read the cache volume. A background task collects the volume-derived figures
every `metrics.collection_interval` (default 60 s), and a scrape serves them from the latest
collection beside the live in-process counters. That collection reads per-shard state from the
cache volume, so its duration follows the shard population rather than the request rate.
`write_cache.ledger_repair_observed_shards` is the figure that drives the cost. On an EFS-backed
cache each shard document costs on the order of a millisecond and a hot re-read does not help,
so a cache with a few thousand shards spends several seconds per collection.

A slow collection shows as stale figures, not as a slow scrape: `volume_snapshot.age_secs` grows
past `volume_snapshot.interval_secs`. The collector skips a missed tick rather than overlapping
runs, so a run longer than the interval leaves the published figures two intervals old. Expect
longer collections while `write_cache.ledger_repair_rotation_complete` is `false`, since the
repair rotation is reading the same volume at the same time.

The payload is one document serialized whole, so a scrape that exceeds the scraper's timeout
loses every series at once. There is no partial body and no section set to `null`. To the
consumer that looks the same as the endpoint being gone or a feature being absent, so when a
section or field seems to be missing, check the scrape before drawing a conclusion about the
feature:

1. **Re-scrape with a generous timeout and time it:**
   `curl -s -o /dev/null -w '%{time_total}\n' --max-time 120 http://<proxy-host>:9090/metrics`.
   A full body means the deadline or the fetch was the problem, not the payload.
2. **Compare with OTLP if it is enabled.** The OTLP export runs on the background collection's
   schedule, so OTLP series present while `/metrics` returns nothing point at the scrape path
   rather than at the feature.

The `metrics_collection` section reports the collection's cost: `duration_ms_last`,
`duration_ms_max`, `slow_collections_total`, `collections_total` and `slow_threshold_ms`. Read
`duration_ms_max` for the worst case since start. `slow_threshold_ms` is half of
`metrics.collection_interval`, so a rising `slow_collections_total` means collections are within
one doubling of overrunning their slot. The remedies are fewer shards, a volume with lower latency per request, or a
longer `metrics.collection_interval` if staler figures are acceptable.

## See also

- [Metrics reference](METRICS_REFERENCE.md): every `/metrics` field
- [OTLP metrics](OTLP_METRICS.md): the exported instruments and how to publish them
- [Dashboard](DASHBOARD.md): the cards and the per-bucket traffic table
