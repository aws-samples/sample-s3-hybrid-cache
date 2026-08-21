# Cache Freshness

TTL, revalidation, and conditional requests: how the proxy decides whether cached data may
be served, and what it asks S3 when it cannot.

Field types, defaults, and valid ranges for every TTL are in
[CONFIGURATION.md — Time-To-Live (TTL) Configuration](CONFIGURATION.md#time-to-live-ttl-configuration).

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
1. Sends `If-None-Match` with the cached ETag and `If-Modified-Since` with the cached `Last-Modified` timestamp
2. If S3 returns `304 Not Modified`: Object unchanged, TTL refreshed, cached data served (no data transfer)
3. If S3 returns `200 OK`: Object changed, old cached data actively removed, fresh data fetched and cached
4. If S3 returns `403 Forbidden` or `401 Unauthorized`: Error returned to client, cached data preserved (a credentials failure is not a data change — cached data remains valid for other authorized callers)

This approach minimizes bandwidth usage while ensuring cache freshness and consistency.

The proxy handles conditional headers in three semantically distinct ways, configurable via `cache.evaluate_conditions_from_cache` (default `true`).

### Opt-out: always forward to S3 for condition evaluation (`evaluate_conditions_from_cache = false`)

Requests on the GET/HEAD path that carry `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, or `If-Range` are forwarded to S3 with the client's headers intact, regardless of whether they also carry a `Range` header. The SigV4 signature is preserved, so the conditional reaches S3 exactly as the client signed it. S3 is the sole judge (forward-and-cache). The proxy takes cache action based only on S3's response:

- **S3 returns 200 OK / 206 Partial Content** → return the data to the client and cache the fresh response (full object or range). This is the path the AWS CLI CRT client takes — every CRT ranged GET of a multi-part download carries `If-Match` — so CRT downloads of large objects now populate the cache. If the object changed, the stale cached ranges/entry are dropped by ETag-mismatch invalidation during the cache lookup, not by a blanket purge.
- **S3 returns 304 Not Modified** → return 304 to the client, cache unchanged. TTL is refreshed only if S3's response ETag equals the cached ETag; if the ETags differ, the cached copy is stale and is invalidated on the next non-conditional miss (a `304` has no body to replace it with now).
- **S3 returns 412 Precondition Failed** → return 412 to client, cache unchanged
- **S3 returns 403 / 401** → return error to client, cache unchanged (credentials issue is not a data change)

> Before v2.2.0 a conditional request was forwarded on a non-caching path, so it was answered from S3 but never written to the cache — which is why CRT large-object downloads never cached. The forward-and-cache path above is the fix.

Strict RFC 7232 compliance and strongest consistency — the proxy cannot serve stale data based on a stale cached ETag. Costs one S3 round trip per conditional request. If you prefer strict S3-authoritative condition evaluation for every request, set `evaluate_conditions_from_cache: false`.

### Default: serve `If-Match` hits from cache (`evaluate_conditions_from_cache = true`)

When `If-Match` is the only conditional header on a GET or range request, and the proxy holds the requested data in cache with a matching ETag, the proxy serves the response directly from cache without contacting S3. The client's `If-Match` value is the freshness assertion — if the proxy has that exact ETag cached, it is the correct version by definition. TTL is refreshed on serve.

A lone `If-Range` is treated the same way, for the same reason: when its validator strong-matches the cached ETag, the client has named the exact version the cache holds, so the range is sliced from cache with no S3 call. Unlike `If-Match`, a matching `If-Range` does **not** refresh the TTL or bypass expiry — `If-Range` asserts which version a `Range` applies to, not that the representation is fresh (RFC 7233 §3.2), so an expired entry still revalidates normally.

An `If-Range` **mismatch** always forwards, and this is the one asymmetry with `If-Match`: a failed `If-Match` is a bodyless `412` the proxy could answer locally, whereas a failed `If-Range` requires the `Range` to be ignored and the **full current representation** returned with `200` — a body the proxy may not hold. The same applies when the validator is an HTTP-date or a weak ETag (not locally comparable against a cached ETag), or when nothing is cached.

For `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` — in any combination, or combined with `If-Match` or `If-Range` — the proxy always forwards the full conditional request to S3. These headers are caller-supplied negative-match or timestamp assertions that only S3 can answer authoritatively; a local answer could be based on a stale cached validator. `If-Match` combined with `If-Range` also forwards: the two validators can disagree, and only S3 can resolve "precondition passes but the range validator is stale".

| Conditional | Has full cache hit with matching ETag | Action |
|---|---|---|
| `If-Match` only | Yes | Serve from cache, refresh TTL — no S3 call |
| `If-Match` only | No (miss / ETag mismatch / partial cache) | Forward to S3 → `412` or `200`/`206` (cached) |
| `If-None-Match` | Any | Always forward to S3 |
| `If-Modified-Since` | Any | Always forward to S3 |
| `If-Unmodified-Since` | Any | Always forward to S3 |
| `If-Range` only | Yes | Serve the range from cache — no S3 call (TTL not refreshed) |
| `If-Range` only | No (miss / mismatch / weak or date validator) | Forward to S3 → `206` or `200`-full |
| Mixed (`If-Match` + any other) | Any | Forward to S3 |

This is the recommended setting for the AWS CLI CRT client: CRT stamps `If-Match` on every ranged GET to pin parts to the same object version, so with the default `true` a warm CRT download is served from cache with no S3 round trips. With `evaluate_conditions_from_cache: false`, every CRT ranged GET re-contacts S3.

> **v2.2.0 change:** the previous `true` behavior evaluated all four conditional headers locally — local `304` for `If-None-Match` match, local `412` for `If-Unmodified-Since` violations, etc. That full local evaluation was removed because it could serve verdicts based on a stale cached ETag/Last-Modified. The new behavior is strictly safer: only `If-Match` (the client's positive ETag assertion) is answered locally; the ambiguous temporal and negative-match headers always go to S3.

### Proxy-internal conditional injection (independent of the above)

Regardless of the `evaluate_conditions_from_cache` setting, the proxy injects conditional headers on its own S3 requests in two narrow cases:

1. **TTL-expired revalidation**: when cached data is accessed past its TTL under lazy expiration, the proxy sends `If-None-Match: <cached-etag>` + `If-Modified-Since: <cached-last-modified>` to get a 304 on match (RFC 7232 §3.2).
2. **Partial-cache merge**: when partial cached ranges exist for a request and the proxy must fetch the missing bytes from S3, it injects `If-Match: <cached-etag>` so S3 refuses (412) rather than return data from a newer object version — which would corrupt the merged response. On 412, the proxy invalidates the stale cache and retries once without `If-Match`. The client never sees the 412.

   A Mode B `If-Range` cache serve uses this same pin: the client's `If-Range` is replaced with the proxy-injected `If-Match` on the ETag it just matched, so it never travels on to a gap fetch. This is a cost decision as well as a consistency one — a stale `If-Range` on a gap fetch makes S3 ignore `Range` and return the **full object** with `200`, and the gap-fetch path buffers a response body in memory before inspecting its status, so that full object is read in full and then discarded for being a non-`206`. The `If-Match` form fails with a bodyless `412` instead. The swap is skipped when `If-Range` appears in `SignedHeaders`, since removing a signed header would invalidate the client's signature.

Client-supplied conditional headers are always preserved exactly; proxy-injected headers are internal and never visible to the client.

**Important**: `get_ttl` only affects GET requests. HEAD requests use `head_ttl` independently.

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

**Common HEAD-heavy clients:**
- **AWS Common Runtime (CRT)** — used by AWS CLI v2 and modern SDKs, issues a HeadObject before every GetObject to retrieve object metadata. HEAD caching eliminates the S3 round-trip for these preflight requests.
- **Mountpoint for Amazon S3** — issues frequent HeadObject requests to check object existence and properties.

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
- Elastic shared filesystems (NFS)
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

The proxy forwards the client's conditional request to S3 verbatim; S3 evaluates the condition. The proxy takes cache action based solely on S3's response. A client-supplied `If-Match` that produces 412 is passed through to the client unchanged; the cache is not modified.

**Cache Invalidation Behavior:** When the proxy decides to invalidate cache (on ETag or Last-Modified mismatch detected via response comparison, or after a proxy-injected `If-Match` produces 412 on the partial-cache-merge path), all cached ranges for that object are removed (metadata file + all range binary files). The proxy does not attempt selective range invalidation — any version mismatch triggers a complete cache purge for that object.

#### Scenario 3a: Partial Cache Hit with Cache Validation

When the proxy has partial cache coverage and needs to fetch missing bytes from S3, it injects an `If-Match: <cached-etag>` header on the S3 sub-fetch (unless the client already supplied one of their own) to prevent a newer object version's bytes from being merged with the older cached bytes:

```
Client GET for bytes 0-41943039 (40MB)
          → Proxy checks cache
          → Found cached ranges: 0-8388607, 16777216-25165823, 33554432-41943039
          → Missing ranges: 8388608-16777215, 25165824-33554431
          → Proxy injects If-Match: <cached-etag> on the S3 sub-fetch (client had no If-Match)
          → S3 validates If-Match:
             - 200 / 206: Object unchanged, merge cached + fetched bytes, serve
             - 412: Object changed on S3 between cache population and now
                   → Proxy invalidates all cached ranges for the object
                   → Proxy retries the S3 fetch once WITHOUT the injected If-Match
                   → Serves fresh data to the client, caches fresh ranges
                   → Client never sees 412 (they did not send a conditional header)
          → Return complete response
```

An injected `If-Match` that fails guarantees the client receives fresh data, never a corrupt merge or a surprising 412. A client-supplied `If-Match` is always preserved and its 412 is passed through.

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

When a cached entry's TTL expires, the proxy revalidates using conditional headers to avoid re-downloading unchanged data:

```
Client GET → Proxy checks cache → GET_TTL expired
          → Proxy adds conditional headers from cached metadata:
             - If-Modified-Since: cached Last-Modified timestamp (1-second granularity)
             - If-None-Match: cached ETag (content hash, when available)
          → Forward request to S3
          → S3 validates conditions:
             - 304 Not Modified: Object unchanged, refresh TTL, serve from cache
             - 200 OK: Object changed, invalidate old cache, cache new data, serve fresh
          → Return response
```

`If-None-Match` (ETag) is the primary revalidation signal. `Last-Modified` has one-second granularity — two writes to the same key within one second produce identical timestamps, which can cause false 304 responses. ETag is a content hash that changes on every write regardless of timing.

Both headers are sent when available. If only one is present (e.g., ETag absent for objects PUT through the proxy before v1.8.3), the proxy sends whichever is available. If neither is present (metadata unreadable), the proxy falls back to an unconditional GET.

If S3 returns 403 or 401 during revalidation (expired credentials, revoked access), the proxy returns the error to the client without invalidating the cache. A credentials failure is not a data change — the cached data remains valid for other authorized callers.

#### Scenario 3d: HEAD Detects Object Change (Range Invalidation)

When a HEAD response returns a different ETag or content-length than what is cached, the proxy invalidates all cached ranges for that key:

```
Client HEAD → Proxy forwards to S3 → S3 returns new ETag or content-length
           → Proxy compares against cached metadata
           → Mismatch detected: clear all cached ranges, expire object immediately
           → Update metadata with new values from HEAD
           → Next GET fetches fresh data from S3
```

This prevents serving stale range data when the object has been overwritten between GET accesses.

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

Object is immediately available for range requests. This covers signed PUT, presigned
PUT, and multipart upload. A POST object upload takes a different path and is not
cached — see [POST object upload is not write-through cached](CACHE_READ_PATHS.md#post-object-upload-is-not-write-through-cached).

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
[incomplete_upload_ttl elapses — default 1 day]
Background cleanup task → Detects InProgress upload older than incomplete_upload_ttl
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

To verify cached responses match direct S3 responses. These commands use the proxy's
HTTP interception endpoint — see
[What a Cleartext Hop Exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes) for the
transport implications and the encrypted alternative:

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

### Three Distinct Uses of Conditional Headers

The proxy handles conditional headers in three semantically distinct ways. The first is pure pass-through; the other two are narrow proxy-internal optimizations that never leak conditional behavior back to the client.

#### 1. Client-supplied conditional headers (forward-and-cache)

Conditional headers (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `If-Range`) are dispatched by header class and by the `evaluate_conditions_from_cache` setting:

**`evaluate_conditions_from_cache = false`:** Every conditional request is forwarded to S3 with all headers intact (SigV4 signature preserved). S3 evaluates the precondition. `200`/`206` success is cached via the normal caching pipeline. `304`/`412` are forwarded to the client without caching. No conditional is served from a cache hit with this setting.

**`evaluate_conditions_from_cache = true` (default):** An `If-Match` request where the cached ETag matches the `If-Match` value AND the data is fully cached is served from cache (TTL refreshed, no S3 call). A lone `If-Range` whose validator strong-matches the cached ETag is likewise served from cache, but without refreshing TTL. `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` always forward to S3, exactly as the `false` setting.

**TTL/version reconciliation:** When a forwarded conditional returns `200`/`206`, the body is cached under S3's response ETag; if that ETag differs from a stale cached entry, the cache lookup invalidates all of the old ranges and **replaces** the object with the fresh content (ETag-mismatch invalidation, `range_handler.rs`). When it returns `304` (no body to cache), the cached entry's TTL is refreshed only if the `304`'s ETag equals the cached ETag; if they differ, the TTL is not refreshed and the stale entry is invalidated on the next non-conditional miss.

The proxy takes cache action based only on S3's response when forwarding:

| S3 response | Action | Cache |
|---|---|---|
| 200 OK | Stream data to client, cache new data | Old data invalidated |
| 304 Not Modified | Return 304 to client; refresh TTL only if S3 response ETag = cached ETag | Valid only if ETag unchanged |
| 412 Precondition Failed | Return 412 to client | Unchanged |
| 403 / 401 | Return error to client | Unchanged (credentials ≠ data change) |

Range requests with client conditional headers are handled by the range-merge path, which preserves the client's conditional headers exactly on every outbound S3 fetch. With `evaluate_conditions_from_cache = true`, an `If-Match` range request whose ETag and full range are cached is served from cache instead of forwarded.

#### 2. TTL-expired revalidation (proxy-injected `If-None-Match` + `If-Modified-Since`)

When cached data is accessed past its TTL under lazy expiration, the proxy issues a conditional GET with the cached ETag and Last-Modified to save bandwidth on unchanged objects:

```
Expired cache entry → Proxy sends If-None-Match: <etag> + If-Modified-Since: <lm>
  → 304: refresh TTL, serve cached data
  → 200: invalidate old cache, fetch fresh, cache new
  → 403/401: surface error, keep cache (credentials issue)
```

This matches RFC 7232 §3.2 and saves egress when objects are immutable. The client never sees the injected headers and never sees a 304 caused by revalidation (they see fresh data or cached data equivalently).

#### 3. Partial-cache merge (proxy-injected `If-Match`, with 412-retry)

When only some bytes of a requested range are cached and the rest must be fetched from S3, the proxy protects the merge against mid-flight object changes by injecting `If-Match: <cached-etag>` on the S3 sub-fetch (unless the client already supplied `If-Match`):

```
Partial cache hit → Fetch missing bytes with injected If-Match
  → 200/206: merge cached + fresh bytes, serve, cache missing
  → 412: object changed on S3 → invalidate all cached ranges for the key,
         retry the fetch once WITHOUT the injected If-Match, serve fresh data
```

The client never sees the 412 caused by the proxy-injected header. A client-supplied `If-Match` is always preserved verbatim and its 412 is passed through.

### Supported Conditional Headers

All four standard headers from RFC 7232 are detected: `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, plus `If-Range` (RFC 7233 §3.2). S3 evaluates precedence per RFC 7232 §6; the proxy does not re-evaluate. The two exceptions where a matching validator is answered from cache instead of forwarded are described above.

### Performance Impact

- Conditional-header detection adds <1 ms per request.
- Non-conditional requests use the existing cache fast paths.
- 304 responses from revalidation save the full response body transfer.
- 412 responses on a proxy-injected `If-Match` incur one extra S3 round-trip (the retry), but only on the rare case that an object changed on S3 mid-partial-merge.

### Logging

```
INFO Forwarding conditional request to S3: method=GET path=/bucket/obj conditional_headers=[if-match="abc"]
INFO S3 conditional response: method=GET path=/bucket/obj status=304 cache_action="TTL refreshed"
INFO S3 conditional response: method=GET path=/bucket/obj status=200 cache_action="invalidated and updated"
INFO S3 conditional response: method=GET path=/bucket/obj status=412 cache_action="no change"
WARN S3 returned 412 on proxy-injected If-Match; invalidating cache and retrying without If-Match: cache_key=bucket/obj
```

## Lazy Deletion (On Read)

## Lazy Deletion (On Read)

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

## Lazy vs Periodic Cleanup

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

## See Also

- [CACHING.md](CACHING.md) — what gets cached, and what bypasses the cache
- [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md) — how a read is satisfied
- [CACHE_INTERNALS.md](CACHE_INTERNALS.md) — on-disk layout, and the HEAD cleanup that runs during validation
- [CONFIGURATION.md](CONFIGURATION.md) — TTL field reference
- [ARCHITECTURE.md](ARCHITECTURE.md#security-considerations) — the access-control implications of TTL
