# Metrics and Observability

## Per-Bucket Traffic Metrics

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

**OTLP** (opt-in): set `metrics.otlp.per_bucket_enabled: true` to emit `s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, and `s3proxy.put_requests` as cumulative counters in CloudWatch (via CloudWatch Agent or ADOT).

### Operation-to-metric mapping

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

`bytes_saved` is the proxy's direct measurement of S3 transfer cost avoided — GET bytes served from cache, not fetched from S3. It is available in `/metrics` JSON (always), `/api/bucket-traffic` (dashboard), and optionally in OTLP as `s3proxy.bytes_saved`.

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
