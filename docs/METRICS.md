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
      "bytes_uploaded": 104857600,
      "get_requests": 5000,
      "put_requests": 200
    }
  }
}
```

**Dashboard** (`/api/bucket-traffic`): the operational dashboard shows a "Per-Bucket Traffic" table alongside the existing cache hit/miss table.

**OTLP** (opt-in): set `metrics.otlp.per_bucket_enabled: true` to emit `s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, and `s3proxy.put_requests` as cumulative counters in CloudWatch (via CloudWatch Agent or ADOT).

### Operation-to-metric mapping

Only object reads and object/part writes are counted:

| S3 operation | HTTP | Proxy metric |
|---|---|---|
| GetObject (including range and presigned GETs) | GET | `get_requests` + `bytes_served` |
| PutObject, UploadPart | PUT | `put_requests` + `bytes_uploaded` |

A GET is counted only when it carries an object key. Bucket-level GETs with no key (list-objects) are not counted. HEAD, DELETE, CreateMultipartUpload/CompleteMultipartUpload, and list operations do not increment any counter.

### Recording sites (exactly-once)

Each request is counted exactly once:
- **GET** is recorded once at the HTTP/TLS request-completion site (covers cache hit, cache miss, coalesced fetch, and range requests).
- **PUT / UploadPart** is recorded once in the signed write-through handler, where the request-body byte count (`bytes_uploaded`) is available — for `aws-chunked` bodies this is the decoded length, otherwise `Content-Length`.

### Inferring cache savings

The proxy does **not** emit a cached-bytes-saved metric directly. Infer it by subtracting the proxy's `BytesDownloaded` from S3's own `BytesDownloaded` for the same bucket:

```
proxy.BytesDownloaded(bucket)  — bytes the proxy served to clients
s3.BytesDownloaded(bucket)     — bytes S3 sent to the proxy (cache misses only)
──────────────────────────────────────────────────────────────────
cache_savings = proxy.BytesDownloaded(bucket) - s3.BytesDownloaded(bucket)
```

**Requirements for this formula to be accurate:**
1. **S3 request metrics must be enabled** on the bucket. The proxy's metric is independent (always on), but S3's `BytesDownloaded` requires S3-side request metrics. Enable via the S3 console or CLI (`put-bucket-metrics-configuration`).
2. **Granularity alignment**: both metrics must cover the same time window. OTLP counters are cumulative since proxy start; use CloudWatch's `Sum` statistic over a consistent period.
3. **Signed PUT traffic**: `s3.BytesUploaded` includes all uploads; the proxy's `bytes_uploaded` covers signed writes through the proxy. Uploads that go directly to S3 (not through the proxy) appear in S3's counter but not the proxy's.
4. **Multi-instance deployments**: aggregate `proxy.BytesDownloaded` across all proxy instances before subtracting S3's total.
