# Cache Read Paths

How a read is satisfied: which tier answers it, how partial coverage is merged, and the
correctness checks applied before cached bytes reach a client.

`docs/ARCHITECTURE.md` → Range Read-Path Map is the quick answer for a single entry point
(RAM consulted? RAM promoted? under which key?). This document is the mechanism.

## Lookup purpose, and what authorises a cached serve

Every cache-coverage lookup states **why** it is asking, because the answer changes what an
expired entry looks like. `RangeLookupPurpose` is a required argument on both
`DiskCacheManager::find_cached_ranges` and `RangeHandler::find_cached_ranges`; there is no
default, so a new call site has to choose.

| Purpose | Past stored expiry | For callers that |
|---|---|---|
| `FreshServe` | returns **no** coverage | may serve what they get with no further freshness check |
| `RevalidationCandidate` | returns coverage, marked `StoredFreshness::Expired` | will evaluate freshness themselves before serving |

The question to ask at a call site is: **between this lookup and bytes reaching a client,
what bounds the staleness?** A live-TTL verdict, a client validator, or a successful `304`
means `RevalidationCandidate`. Nothing but the lookup itself means `FreshServe`. If the
answer is "nothing at all", the call site is a defect whichever purpose it names.

`RevalidationCandidate` grants **discovery, not permission**. `RangeOverlap` enforces that
in its accessors rather than by convention:

- `is_serveable_unvalidated()` — complete coverage **and** stored-fresh. Safe on its own.
- `has_complete_coverage()` — coverage only. Requires the caller to hold a separate
  authority and say which one.

So serving expired bytes always involves reaching for the second accessor and writing down
why it is allowed.

### Which authority each cached serve holds

| Read path | Purpose | What authorises the serve |
|---|---|---|
| Full-object mainline GET | `RevalidationCandidate` | Mode B matching `If-Match`, or a live-TTL `Fresh` verdict, or a `304` |
| Range path, early full-object shortcut | `FreshServe` | Live-TTL `Fresh` verdict; when expired it falls through rather than serving |
| Range path, range-specific lookup | `RevalidationCandidate` | Live-TTL `Fresh` verdict, or a `304` |
| Page widening (`find_page_overlap`) | `FreshServe` | Stored expiry only — see the caveat below |
| Part-scoped lookup (`?partNumber=N`) | `FreshServe` | Stored expiry only — see the caveat below |
| Post-`304` validated-serve helpers | `RevalidationCandidate` | The `304` itself; coverage is re-checked because a `304` proves the version, not the coverage |
| Degraded coalescing fallbacks | `FreshServe` | Stored expiry only; these run when S3 errored, so there is no validation to lean on |

**Caveat on the two `FreshServe`-only rows.** Page widening and the part-scoped lookup are
bounded by *stored* expiry rather than by the currently resolved `get_ttl`, so lowering
`get_ttl` does not take effect on already-cached data on those paths until the old
`expires_at` elapses. The widened path additionally consults the RAM tier before this
lookup. Both are pre-existing, both are tracked separately, and neither is a consequence
of candidate revalidation. The mainline GET paths are bounded by the resolved `get_ttl` and
honour a `get_ttl` change without a restart or a cache wipe.

If you need `get_ttl` to take effect immediately on a key, keep it off page widening
(`page_widening` defaults to off) and read whole objects or ordinary ranges rather than
part-scoped (`?partNumber=N`) requests.

## Cache Types

### Full Object Cache

Caches complete objects from GET requests.

**Cache Key**: `{bucket}/{object_key}`

### Range Cache

Caches byte ranges independently across both RAM and disk tiers.

**Disk Cache Key**: `{bucket}/{object_key}:range:{start}-{end}`
**RAM Cache Key**: `{cache_key}:range:{start}:{end}`

Benefits:
- Large files: cache only accessed ranges
- Partial downloads: resume without re-downloading
- Efficient for video streaming, large datasets

#### RAM Cache Integration for Ranges

Both the streaming path (ranges at or above `cache.disk_streaming_threshold`) and the buffered path (ranges below it) use RAM cache:

1. **RAM cache lookup**: Before deciding between streaming and buffered paths, the proxy checks RAM cache using the range-specific key
2. **RAM hit**: Serves data directly from memory as a buffered 206 response, avoiding all disk I/O
3. **RAM miss → disk hit**: After serving from disk, the proxy promotes the range data to RAM cache so subsequent requests are served from memory
4. **Streaming path promotion**: During disk streaming, chunks are collected into a buffer and promoted to RAM cache after the stream completes
5. **Size guard**: promotion is bounded by **per-shard** capacity (`max_ram_cache_size / effective_shard_count`), not by `max_ram_cache_size` as a whole. At defaults that is 64 MiB, not 512 MiB. Ranges above it skip RAM cache promotion with no buffer allocated. See [RAM Sizing and the Admission Ceiling](CONFIGURATION.md#ram-sizing-and-the-admission-ceiling)

**Important**: All ranges for the same object share the same expiration time. When GET_TTL expires for an object, ALL cached ranges for that object expire together, even though they're stored separately.

### Intelligent Range Merging

The proxy implements intelligent range merging to optimize partial cache hits. When a GET request requires bytes that are partially cached (some bytes in cache, some missing), the system serves cached portions and only fetches missing bytes from S3, then merges them into a complete response.

**Range Merge Optimization**: Simple cache hits (where the requested range exactly matches or is fully contained within a single cached range) bypass merge operations entirely, eliminating unnecessary processing overhead.

#### Full-Object GETs with Partial Cache Coverage

A GET without a `Range` header whose cache has partial coverage can also benefit from the merge path. The proxy synthesizes `Range: bytes=0-{total_size-1}` and routes through the same merge machinery, subject to three hard-coded gates:

1. **Signature preservation**: `range` must not appear in the request's SigV4 SignedHeaders. Since this path synthesizes a Range header on a request that had no Range, and AWS SDKs only sign headers present at signing time, this gate passes trivially for the intended use case (full-object GETs with partial cache coverage).
2. **Cached fraction ≥ 10 %**: sum of cached range bytes must be at least 10 % of `total_size`.
3. **Object size ≤ 128 MiB**: the merge path buffers the reconstructed response; a 128 MiB cap avoids memory pressure. Larger objects fall through to an unconditional S3 fetch as before.

If any gate fails, the request falls through to an unconditional S3 fetch (pre-1.14.0 behavior). These thresholds are fixed in code, not exposed in configuration.

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
- Total overhead vs full S3 fetch: Often 2-5x faster for partial cache hits (based on internal testing with synthetic workloads)

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

**Cache Key Strategy**: Parts use the same cache key as the full object (`{bucket}/{object_key}`) but are stored as ranges with specific byte offsets.

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

#### TTL Transition on First Read

Write-cached objects start on their own TTL and move to the read TTL the first time
they are read. This is a one-time transition, not a refresh on every access:

- Initial PUT: TTL set to `put_ttl` (default 1 hour)
- First GET access: the entry transitions from `put_ttl` to `get_ttl`. This is a
  one-time transition, not a repeating refresh, and it runs before the freshness check
  so `get_ttl: 0` revalidates against S3 on that first GET
- No access within TTL: Object expires and is removed

This keeps frequently accessed objects in cache while allowing rarely-read uploads to expire.

#### Capacity Management

The write cache is allocated a percentage of the total disk cache:

```yaml
cache:
  write_cache_percent: 10.0  # Default: 10% of max_cache_size
```

**Eviction behavior**:
- The allocation is a reclamation target, not an admission limit — an upload is cached even
  when it is already full, and the excess is reclaimed in the background
- Reclamation order is oldest-staged-first, with entries whose `put_ttl` elapsed unread
  taken first. An object never read has no recency or frequency score to compute, so the
  read cache's LRU/TinyLFU choice does not apply here
- Reclamation runs from the background maintenance cycle, never on the request path
- Staged bytes also leave the allocation without any reclamation, by being read for the
  first time — the normal case for a read-after-write workload
- S3 operation always succeeds regardless of caching outcome

#### Disk-Only Storage

Write-cached objects are stored only on disk, not in RAM cache:
- Preserves RAM cache for hot read data
- Write-cached objects can be promoted to RAM cache on subsequent GET access
- Capacity tracking uses compressed size (actual disk usage)

#### Limitations

- Single PUT size limit: 256MB per object (configurable via `write_cache_max_object_size`)
- Objects larger than that limit bypass caching automatically
- Caching is also declined when it would take the cache past `max_cache_size`, or when the
  cache volume has under 1 GiB free beyond the object's own size — reported as
  `disk_safety` in `signed_put.skipped_puts_total`. This is the only capacity-shaped
  refusal; `write_cache_percent` does not decline anything (see above)
- POST object upload is not write-through cached — see below

#### POST object upload is not write-through cached

A browser form upload (`POST /bucket` with a `multipart/form-data` body, the signature
carried in form fields rather than an `Authorization` header) reaches S3 correctly and
returns S3's own response, but the object is **not** placed in the cache.

The body is a MIME envelope, not the object bytes: the payload is one part (`file`)
alongside form fields such as `key`, `policy`, and `x-amz-signature`. Caching the body
as received would store the envelope and a later GET would serve corrupt data.
Extracting the object part means parsing the envelope, which means buffering the whole
upload in memory — the cost the streaming write path exists to remove. So the proxy
streams the upload and skips caching.

The practical effect is one extra S3 round-trip: a POST-uploaded object is absent from
the cache until something reads it back through the proxy, at which point the read path
caches it as a normal cache miss. Every later read is a cache hit. Presigned PUT,
signed PUT, and multipart upload are all write-through cached as described above.

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
- Store part under the in-progress upload directory: `mpus_in_progress/{uploadId}/part{N}.bin` (see [MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md))
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

**Cache Key**: `{bucket}/{object_key}`

**Important**: Multipart uploads use the same range storage as PUT and GET operations. Once completed, they behave identically to PUT-cached objects.

### Write Cache Error Handling

The write cache implements robust error handling to ensure that S3 operation failures (including authentication/authorization errors like 403 Forbidden) do not result in corrupted or invalid cache entries.

#### Core Principle: Only Cache on S3 Success

The write cache follows the fundamental principle of "only cache on S3 success." This ensures that:
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
    // S3 returned error - don't cache
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
    // Mark upload as incomplete but don't delete parts
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
DEBUG S3 error response, not caching PUT: cache_key=bucket/object.txt, status=403
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

## Page-Aligned Range Caching

Page-aligned range caching (also called *range read widening*) widens an eligible small ranged GET into a fixed-size, page-aligned fetch, caches the whole page (disk and RAM), and serves the client exactly the bytes it requested. Later small reads that fall in the same page — from the same reader, a re-run, or a different reader sharing the cache — are served with no S3 round trip, and concurrent small reads in the same page coalesce onto a single fetch.

The access shape it targets is a trailing footer read followed by clustered reads at scattered offsets, as produced by columnar readers (Parquet, ORC). Footer/tail caching is simply the special case where the read lands in the object's last page. The proxy never parses object content to detect any of this; it works purely from request byte offsets.

### Eligibility: which clients can use this

**Read this before enabling.** Widening requires that `range` is **absent** from the request's SigV4 `SignedHeaders`, because widening works by rewriting the upstream `Range` and rewriting a signed header invalidates the client's signature. The proxy has no credentials and cannot re-sign, so this is a hard constraint, not a tuning choice.

The AWS CLI and every official AWS SDK (botocore/boto3, Java, JS v3, Rust, Go v2) sign `Range` — see [Which clients sign Range?](#which-clients-sign-range) for the full breakdown. **A workload made up exclusively of those clients gets no widening at all**, and gets no error saying so: every request falls through to the ordinary un-widened range path and increments `page_cache.skipped_signed_range`.

Widening is therefore useful for:

- **Presigned-URL access patterns** — video streaming with browser range-seeking, PDF viewers, columnar readers fetching via presigned URLs issued by a backend. A presigned URL signs via query-string parameters, so no `SignedHeaders` list containing `range` exists.
- **rclone** — uses its own SigV4 implementation signing a minimal header set.
- **Custom HTTP clients** that add `Range` after signing, or sign only the minimum required headers.

Note what this excludes: Spark, Hadoop, Hive, Trino, Flink, `aws s3 cp`, `aws s3api get-object --range`, s5cmd, and Mountpoint. Columnar data read through any of those does not qualify, despite being the access shape the mechanism was designed around.

### Off by default, per-key opt-in

Widening is **never enabled globally** and **off unless a `cache_rules.json` rule explicitly turns it on** for matching keys, via the `page_widening` and `page_size` rule fields (see [Cache Rules](CACHING.md#cache-rules) and [CONFIGURATION.md](CONFIGURATION.md#cache-rules)):

```json
{ "pattern": "**/*.parquet", "page_widening": true, "page_size": 16777216 }
```

- `page_widening` (bool, default `false`): enables widening for keys the rule matches.
- `page_size` (bytes, default `16777216` = 16 MiB when the rule enables widening without specifying it): the fixed page size `P` for the grid, in bytes. Must be `> 0` and `<= 67108864` (64 MiB) — the RAM admission ceiling described below.

### Operator warning: amplification is workload-dependent

Widening every small read to a full page is a large win when reads **cluster** within pages (columnar scans, footers) and a real **cost** when reads are **scattered** — a 4 KiB random read against a key with a 16 MiB page size becomes a 16 MiB fetch. Only enable `page_widening` for key patterns whose access pattern clusters reads within pages.

### Mechanism

A request is eligible for widening only when: the rule's `page_widening` is `true` for the key, the method is `GET` with a byte `Range` header, the requested length is smaller than `P`, the `Range` is not part of the request's SigV4 signed headers (a Signed_Range is forwarded unmodified — its bytes cannot be safely rewritten), and the request does not carry a `partNumber` query parameter (served by the separate part-caching path, unaffected by widening).

For an eligible request:

1. The requested byte range is mapped to the **Page(s)** it overlaps on a fixed grid anchored at offset 0: `page_index = floor(start / P)`, `page = [page_index * P, page_index * P + P - 1]`, clamped to the object's last byte on the final page. A request landing near a page boundary overlaps two Pages.
2. A `Range: bytes=-N` suffix request is converted to its absolute range once the object size is known (from cached metadata) and then mapped to overlapping Page(s) the same way — never widened to a single fixed "last page", since that could be smaller than the requested suffix. When the size is not yet known, the proxy issues `bytes=-P` instead (a superset of the requested bytes, end-anchored but not grid-aligned); once the size is learned, later suffix reads take the grid-aligned path.
3. Conditional range requests (`If-Range`, or `If-Match`/`If-None-Match` accompanying a `Range`) are widened the same way. A `206` response is sliced to the client's original sub-range and the Page is cached; a `304`, `412`, or `200`-full response (condition not met) is passed through to the client unchanged — it is never sliced or treated as a cacheable Page.
4. **Only genuinely missing, not-in-flight bytes are fetched** — the proxy never re-requests bytes it already holds or bytes another in-flight fetch is already retrieving for the same Page (coalesced via the same in-flight tracker used for other cache misses). When a request overlaps two Pages, each Page is fetched independently and **concurrently**, never sequentially — parallelizing across S3 connections rather than merging into one larger request.
5. If the widened/Page fetch fails (error status or network error), the proxy retries the client's original, un-widened range, serves it normally, and skips caching for that request — widening never turns a request that would otherwise succeed into a failure.
6. The client always receives exactly the bytes it requested: a `206 Partial Content` response with `Content-Range`/`Content-Length` computed against its original request, sliced from the widened Page data.

### Disk footprint

A page is stored through the same range-store path as any other cached range — there is no new on-disk format, and it composes with the existing range-merge/consolidation logic. However, enabling `page_widening` for a key means a single small read caches an entire page rather than only the requested bytes, **increasing per-object disk footprint** for that key relative to unwidened range caching. Size the disk cache (`max_cache_size`) with this in mind for keys where widening is enabled.

### RAM cache uses the Page as its unit

When widening is enabled for a key, the RAM cache stores and looks up data by the containing Page's bounds, not the client's requested sub-range — a sub-page read is served by looking up the deterministic containing Page and slicing the requested bytes from it. Promotion to RAM (whether from a disk hit or, for the Page path, only on a subsequent disk-hit read rather than the initial cold fetch) promotes the whole Page. RAM cache heat (access recency/frequency) is recorded against the whole Page, so any sub-page hit keeps the entire Page resident. When widening is **not** enabled for a key, RAM caching remains per-range exactly as before.

**64 MiB RAM admission ceiling.** The proxy unconditionally guarantees that any single RAM cache entry up to a hardcoded 64 MiB (`RAM_CACHE_ADMISSION_CEILING = 67108864` bytes, a compile-time constant, not a config field) is admitted rather than silently dropped — regardless of whether page-aligned range caching is used for any key. It does this by clamping the effective number of RAM cache shards so `max_ram_cache_size / effective_shard_count >= 64 MiB`, logging a warning when the clamp reduces concurrency below the configured `ram_cache_shard_count`. See [CONFIGURATION.md — RAM Sizing and the Admission Ceiling](CONFIGURATION.md#ram-sizing-and-the-admission-ceiling) for the sizing guidance and shard-clamp formula, and [`config/config.example.yaml`](../config/config.example.yaml) for a worked example.

### Coalescing and coherency

Concurrent small reads that overlap the same Page on the same instance are coalesced onto a single upstream fetch — the in-flight fetch is keyed on the Page's bounds, not the client's requested sub-range — and served from the resulting cached Page. Cached Pages use the object's normal, object-level `get_ttl`; there is no separate per-Page TTL, so all Pages of an object expire together and an ETag/version mismatch purges the whole object, cached Pages included, exactly like any other cached range.

### Metrics

Widening exposes the following counters (see [Monitoring](#observing-range-merge-efficiency) and the dashboard):

| Metric | Meaning |
|---|---|
| `page_cache.widened_requests` | Small reads widened to a Page fetch |
| `page_cache.bytes_prefetched` | Bytes fetched beyond what the client requested (plus a derived amplification ratio) |
| `page_cache.page_hits` | Small reads served from an already-cached Page with no S3 fetch |
| `page_cache.skipped_signed_range` | Requests that would otherwise be eligible but were left unmodified because the Range was signed (expected for AWS CLI/SDK traffic; see [Eligibility](#eligibility-which-clients-can-use-this)) |
| `page_cache.fallbacks` | Widened/Page fetches that failed and fell back to the client's original range |
| `page_cache.ram_page_promotions` | Pages successfully promoted to the RAM cache |
| `page_cache.ram_page_promotion_skipped` | Pages not promoted to RAM (e.g. exceeded the applicable RAM budget) |

A widened request is also logged at `DEBUG` with the cache key, the original requested range, and the widened Page range.

### Non-goals

The proxy never parses object content (no Parquet/ORC footer, Thrift metadata, or magic-byte parsing) — every decision is based on request metadata only (the key's glob match and byte offsets/lengths). There is no chunk prediction, no widening of reads already `>= P`, and no cross-instance coordination of Page fetches.

## Cache Coherency on the Read Path

Two checks run before cached bytes are served, both of which can invalidate cache rather
than serve stale data.

## Full Object Range Replacement

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

## ETag Validation for Range Requests

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

## Metadata Consistency

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

## Error Handling and Fallback

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

### Coherency log lines

```
INFO Range invalidation for full object: cache_key=bucket/object.txt, 
     ranges_removed=3, size_freed=25165824

INFO ETag validation failed: cache_key=bucket/object.txt, 
     range_etag=abc123, object_etag=def456, range is stale

INFO Cleaning up orphaned ranges for cache_key: /bucket/object.txt, 
     ranges_removed=2, size_freed=16777216

WARN Metadata corruption detected: cache_key=bucket/object.txt, 
     error="Invalid JSON", action="invalidated entry"
```

### Coherency performance

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

The counters for these operations are `cache.cache_etag_validations_total`,
`cache.cache_etag_mismatches_total`, `cache.cache_range_invalidations_total`, and
`cache.cache_orphaned_ranges_cleaned_total` — see
[METRICS_REFERENCE.md](METRICS_REFERENCE.md#cache).

## Signed Range Request Handling

### Overview

When clients sign GET requests with AWS Signature Version 4 (SigV4), the Range header is included in the SignedHeaders list if it was present at signing time. This creates a challenge for the proxy: modifying the Range header to fetch only missing cache portions would invalidate the signature, causing S3 to return 403 Forbidden errors.

#### Which clients sign Range?

The AWS CLI and all official AWS SDKs (botocore/boto3, Java SDK, JS SDK v3, Rust SDK, Go SDK v2) sign every header not on botocore's internal blocklist. Range is not on that blocklist, so **any request made through an official AWS SDK or CLI with a Range header will have Range signed**. This includes:

- `aws s3api get-object --range`
- `aws s3 cp` (CRT parallel downloads sign Range + If-Match on each ranged part)
- s5cmd (uses Go AWS SDK v2)
- Mountpoint for Amazon S3 (uses CRT `auto_ranged_get`, which signs Range internally)

Clients that typically do **not** sign Range:

- **Presigned URLs with Range added at request time** — the signature was computed before the Range header was added, so Range is not in SignedHeaders
- **curl / wget / custom HTTP clients** using presigned URLs
- **rclone** — uses its own SigV4 implementation that signs a minimal header set (host + x-amz-* headers)
- **Older MinIO Go client** — similar minimal SignedHeaders set

The proxy detects which case applies on every request by parsing the `SignedHeaders` field in the Authorization header, so it handles both signed and unsigned Range transparently.

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

## Observing Range Merge Efficiency

**Range-merge efficiency is log-only.** There is no `range_merge.*` metric namespace in
`/metrics`; the figures below come from application log lines.

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
# Part cache HIT - serving from cache: cache_key=bucket/large-file.bin part_number=1
# Part cache MISS - fetching from S3: cache_key=bucket/large-file.bin part_number=2
# Part cached - stored as range: cache_key=bucket/large-file.bin part_number=2
```

## See Also

- [CACHING.md](CACHING.md) — what gets cached, and what bypasses the cache
- [CACHE_INTERNALS.md](CACHE_INTERNALS.md) — the on-disk and in-memory structures these paths read
- [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md) — TTL and conditional-request handling
- [HEDGING.md](HEDGING.md) — racing a duplicate upstream fetch to cut tail latency
- [MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md) — multipart cache internals
- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) — the `cache` and `page_cache` counters
