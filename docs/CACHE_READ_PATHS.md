# Cache read paths

This page explains how Hybrid Cache for Amazon S3 answers a read from its cache: which cache serves it and how partly cached ranges are merged or refetched. It also covers signed ranges, part numbers and page widening, and the checks that run before cached bytes reach a client. It is for operators tuning range-heavy workloads and for developers changing a read path. The [range read-path map](ARCHITECTURE.md#3a-range-read-path-map) gives the short answer for each entry point; this page describes the mechanism.

## Cache types

### Full object cache

Caches complete objects from GET requests, under the cache key `{bucket}/{object_key}`.

### Range cache

Caches byte ranges independently, in both the RAM cache and the disk cache. The disk cache key is `{bucket}/{object_key}:range:{start}-{end}` and the RAM cache key is `{cache_key}:range:{start}:{end}`.

Caching ranges lets the proxy keep only the parts of a large file that clients read, and lets an interrupted download resume from cached bytes. Video seeking and columnar reads over large datasets are the typical workloads.

All ranges of one object share one expiration time. When `get_ttl` expires for an object, all of its cached ranges expire together, even though they are stored separately.

#### RAM cache integration for ranges

Both the streaming path (ranges at or above `cache.disk_streaming_threshold`, default 1 MiB) and the buffered path (ranges below it) use the RAM cache:

1. **Lookup**: before choosing between the streaming and buffered paths, the proxy checks the RAM cache under the range's key.
2. **RAM hit**: the proxy serves the bytes from memory as a buffered `206` response, with no disk I/O.
3. **RAM miss, disk hit**: after serving from disk, the proxy promotes the range to the RAM cache so later requests are served from memory. A range is promoted only when the object's ETag is known, so a later hit can check the RAM entry against the current version.
4. **Streaming promotion**: while streaming from disk, the proxy collects the chunks and promotes them to the RAM cache when the stream completes.
5. **Size guard**: promotion is bounded by the capacity of one RAM cache shard (`max_ram_cache_size / effective_shard_count`), not by `max_ram_cache_size` as a whole. At the defaults that is 64 MiB, not 512 MiB. A larger range skips promotion and no buffer is allocated. See [RAM sizing and the admission ceiling](CONFIGURATION.md#ram-sizing-and-the-admission-ceiling).

### Intelligent range merging

When a GET needs bytes that are partly cached, the proxy serves the cached bytes, fetches only the missing bytes from S3, and merges them into one response. A merge is attempted only when the requested range and every cached extent it overlaps are each at most 8 MiB uncompressed. A larger, partly cached range is refetched from S3 instead (see [How much memory a cached serve may use](#how-much-memory-a-cached-serve-may-use)).

A simple cache hit, where one cached range holds the whole requested range, skips the merge.

#### Full-object GETs with partial cache coverage

A GET without a `Range` header can also use the merge when its object is partly cached. The proxy builds `Range: bytes=0-{total_size-1}` and routes the request through the same merge, subject to four fixed gates:

1. **Signature preservation**: `range` must not appear in the request's SigV4 `SignedHeaders`. The AWS SDKs sign only headers present at signing time, and this request had no `Range`, so the gate passes for the intended case.
2. **Cached fraction at least 10%**: the cached bytes must add up to at least 10% of `total_size`.
3. **Object size at most 128 MiB**: a larger object goes to an unconditional S3 fetch. Passing this gate only routes the request to the merge path. Because the built range is the whole object, the 8 MiB merge bound then decides. An object of at most 8 MiB, with every overlapped extent at most 8 MiB, is merged. A larger one is refetched whole from S3 through the streamed cache-miss path and stored as one range.
4. **Object not empty**: a zero-length object has nothing to merge and no valid range.

If any gate fails, the request goes to an unconditional S3 fetch. These thresholds are fixed in code, not configuration fields.

This is the only place the proxy creates a `Range` header for a request that carried none, so internally a no-`Range` GET behaves like a ranged one. The response does not: a GET sent without a `Range` header is answered `200` with no `content-range`, whatever the object's size and however much of it is cached. Both routes keep to that, the one whose merge fits inside the memory bound and the one refetched from S3 because it does not. A client that receives `206` for a request with no `Range` is seeing a defect, not a documented mode. The bytes and `content-length` are still correct in that case; only the status and the `content-range` header are wrong.

The built range is carried to the refetch in an internal request header that is removed before the request leaves the proxy, so it never appears on the wire.

#### How range merging works

```text
Cached ranges:   0-1 MiB, 2-3 MiB, 4-5 MiB
Client requests: 0-5 MiB (inside the 8 MiB merge bound)

1. Find the missing ranges: 1-2 MiB, 3-4 MiB
2. Consolidate missing ranges separated by small gaps
3. Fetch only the missing bytes from S3 (2 MiB)
4. Merge cached and fetched bytes in order
5. Return the complete 0-5 MiB response
6. Cache the newly fetched ranges

Result: 3 MiB from the cache and 2 MiB from S3, a cache efficiency of 60%
```

The same layout under a request larger than 8 MiB is not merged: the whole requested range is refetched from S3 and stored as one range.

#### Range consolidation

When several ranges are missing, the proxy joins those separated by less than `cache.range_merge_gap_threshold` (default 1 MiB) into one S3 request:

```text
Missing ranges: 10-11 MiB, 11.1-12 MiB, 20-21 MiB

Without consolidation: 3 S3 requests
With consolidation:
  10-11 MiB and 11.1-12 MiB are 0.1 MiB apart, so they become one request for 10-12 MiB
  20-21 MiB is far from both, so it stays a separate request

Result: 2 S3 requests instead of 3
```

When the gap is below the threshold, fetching the extra bytes costs less than another S3 request, each of which pays a round trip, and round trips are longest across Regions.

```yaml
cache:
  range_merge_gap_threshold: 1048576  # 1 MiB (default)
```

How to tune it:

- **Origin in the same Region**: a smaller threshold, such as 128 KiB, keeps unnecessary transfer down while extra requests stay cheap.
- **Origin across Regions or outside AWS**: keep the 1 MiB default or raise it, since each extra request pays a long round trip.
- **Request cost matters most**: keep the default or raise it. S3 charges per request, and bandwidth usually costs less than request count.

#### Cache efficiency metrics

A successful merge logs its efficiency at `debug` level:

```text
DEBUG Range merge succeeded: cache_key=example-bucket/example.bin, size=5242880 bytes, cache_efficiency=60.00%
```

`size` is the number of bytes in the merged response, and `cache_efficiency` is the percentage of them served from the cache. A figure that stays low (under about 30%) on a busy prefix means the cached ranges line up poorly with the reads: consider the range boundaries clients use, or a larger cache.

#### Fully cached across several ranges

When every requested byte is cached but split across several range files, the proxy serves the response from the cache alone:

```text
Cached ranges:   0-8 MiB, 8-16 MiB, 16-24 MiB (three files)
Client requests: 0-24 MiB

1. Every byte is cached, so nothing is missing
2. Serve the three ranges in order; above 8 MiB each extent is streamed in turn
   rather than merged into one buffer
3. Send no S3 request

Result: 100% cache efficiency, 0 bytes from S3
```

This layout usually comes from a multipart upload, where each part is cached as its own range. It also comes from range requests that a later full-object request reads together, or from a large file whose parts were read on demand.

#### Range extraction

The proxy reads the overlapping bytes out of each cached range that a request touches:

```text
Full containment
  Cached range: 0-10 MiB
  Requested:    2-5 MiB
  Read bytes 2097152-5242879 of the cached file: 3 MiB, no S3 fetch

Partial overlap
  Cached range: 0-8 MiB
  Requested:    6-10 MiB
  Read bytes 6291456-8388607 of the cached file (2 MiB) and fetch 8-10 MiB from S3
  Merge 2 MiB cached and 2 MiB fetched into a 4 MiB response
```

Cached ranges from AWS CLI uploads usually sit on 8 MiB boundaries, the CLI's default part size. A request can cross those boundaries (for example 1-10 MiB); the proxy reads the partial bytes from each cached range it overlaps and fetches nothing it already holds.

#### Error handling and fallback

| Scenario | What the proxy does | Result |
|---|---|---|
| Merged data size does not match the requested range | Logs an error and falls back to a complete S3 fetch | The client receives correct data, and the cache is updated |
| Metadata lists a range whose file is missing or unreadable, unsigned `Range` | Marks the range missing, fetches it from S3 and caches it again | No client impact |
| The same, signed `Range` | Invalidates the whole key and forwards the client's own request to S3 | No client impact |
| A cached range file holds fewer bytes than its metadata records | Invalidates the whole key and forwards the client's own request to S3 | The client gets S3's response instead of a `500`, and that response is cached again |
| A cached range file fails decompression | A read assembled in memory is answered from S3. A read streamed from the cache has already started its response, so it stops partway with a connection error | The damaged range is removed so the next read caches it again; until then, later reads of it on that proxy are fetched from S3 |
| Some segments merge and others fail | Falls back to fetching the complete range from S3 | The response is S3's bytes, and the cache is refreshed |

Every scenario above except a streamed read that finds a damaged range file falls back to S3, so the client receives correct data even when the cache operation fails. A streamed read that finds a damaged file ends its response early with a connection error, and the bytes it sent before the check failed can be wrong. See [When the proxy can return data that differs from S3](CACHE_FRESHNESS.md#when-the-proxy-can-return-data-that-differs-from-s3).

#### Signed `Range` requests

A client whose SigV4 `SignedHeaders` lists `range` must reach S3 with that header byte for byte. That covers every AWS SDK ranged download, including the final open-ended chunk of an `aws s3 cp` download such as `bytes=16777216-`. Any rewrite is a different canonical request, and S3 answers `403 SignatureDoesNotMatch`. The proxy has no credentials and cannot sign again, so on every path that would otherwise send S3 a different `Range`, it forwards the client's own header instead:

| Situation | Unsigned `Range` | Signed `Range` |
|---|---|---|
| Partial coverage (cached bytes plus holes) | Fetch only the holes and merge them with the cached bytes | Stream the client's whole range from S3 and cache it; no hole fetches |
| Cached range file missing or unreadable | Refetch that extent with a rewritten `Range` | Invalidate the whole key and forward the client's request |
| A merge or fallback refetch would need a different `Range` | Fetch the needed extent | Forward the client's request unchanged; the cached entry is kept |
| Complete-range fetch after a merge failure | Rewritten to the requested extent | The client's header is forwarded as sent, suffix and open-ended forms included |

Invalidating the whole key drops its RAM ranges, its range files and its metadata through the same accounted path a version change takes, so the cache size stays correct. Every one of these cases answers the client from S3 rather than with a `500` or `502`. The hole fetcher itself refuses a signed `Range`, so a caller that missed one of the routes above still cannot send a rewritten header.

The proxy checks range data from an upstream response before storing it. A body shorter than the requested range is stored as a shorter range when S3's `Content-Range`, or a `200` carrying the whole object from byte 0, shows that the object ends there. A streamed body cut short under a matching `Content-Range` is still stored as a shorter range when at least `cache.partial_range_commit_ratio` (default `0.5`) of the requested bytes arrived in order; see [Cache safety](CONNECTION_POOLING.md#cache-safety). Any other short body is not stored, and a range without an `ETag` is never stored over metadata that has one.

#### Fetching and memory

- The proxy fetches several missing ranges from S3 in parallel.
- The merged response is assembled in one buffer, so a merge is limited to 8 MiB uncompressed. A larger, partly cached range is refetched from S3 through the streamed path instead.
- Fetched ranges are cached as soon as the merge completes, so later requests can use them.

#### Use cases

```text
Video seeking
  Cached:      ranges from earlier seeks (0-2 MiB, 50-52 MiB, 100-102 MiB)
  New request: 50-54 MiB
  Result:      2 MiB from the cache and 2 MiB from S3 (50% cache efficiency).
               A request larger than 8 MiB over the same layout is refetched whole

Parquet queries
  First query:  the footer (the last 8 KiB of a 50 MiB file) and column A (5-7 MiB, 15-17 MiB)
  Second query: the same file, column B (25-27 MiB, 35-37 MiB)
  Result:       the footer is served from the cache; only column B's 4 MiB comes from S3

Multipart upload followed by GET
  Upload:  a 100 MiB file in 13 parts, cached as ranges 0-8 MiB, 8-16 MiB, ..., 96-100 MiB
  GET:     0-100 MiB
  Result:  the 13 ranges are streamed in order, 0 bytes from S3

Many clients reading one file
  First client:  downloads a 100 MiB artifact (cache miss, fetched from S3)
  Other clients: the same request, served from the cache
  Result:        1 S3 request instead of one per client

Part number requests
  GET /bucket/big.bin?partNumber=1 (cache miss)
  S3 answers Content-Range: bytes 0-8388607/5368709120 and x-amz-mp-parts-count: 640
  The proxy stores the part as range 0-8388607 and records parts_count=640, part_ranges[1]=(0, 8388607)
  A later GET /bucket/big.bin?partNumber=1 is served from the cache with no S3 request
```

### Part number cache

The proxy caches GetObject requests that carry a `partNumber` query parameter, storing each part as a range of the object.

1. **Detection**: a GET with `partNumber` is a GetObjectPart request.
2. **Part range**: S3's `Content-Range` and `x-amz-mp-parts-count` headers give the part's exact byte range.
3. **Storage**: the part is stored as a range, with the same storage and compression as any other range.
4. **Lookup**: a later request for the same part number finds its byte range in the object's `part_ranges` map and is served from the cache when that range is cached.

Parts use the full object's cache key (`{bucket}/{object_key}`) and are stored as ranges at their byte offsets. Parts of different sizes are supported, and the cached response keeps S3's response headers. [Part caching](#part-caching) describes how part ranges are recorded.

Limits:

- An upload verification request, carrying both `partNumber` and `uploadId`, bypasses the cache.
- An invalid part number, or one beyond the object's part count, is passed through to S3.

#### Per-proxy part request deduplication

Concurrent requests for the same part of the same object share one S3 fetch through download coordination: one request fetches while the others wait. See [Download coordination](#download-coordination).

### Write-through cache

The write cache stores an object's bytes as a PUT passes through, in the same range format the read path uses. A GET straight after the upload is then served from the cache without fetching from S3.

#### How the write cache works

For a single PUT:

1. The client sends `PutObject`.
2. The proxy forwards it to S3 and streams the body to a temporary file.
3. S3 returns `200 OK` with an ETag.
4. The proxy commits the temporary file as the range `0-N`.
5. It writes metadata with `is_write_cached=true` and the `put_ttl`.
6. It returns S3's response to the client unchanged.

Because the object is stored as the range `0-N` under `ranges/`, range requests are served from it at once.

Headers for write-cached objects:

- **ETag**: available at once from S3's PUT response.
- **Last-Modified**: S3's `PutObject` and `CompleteMultipartUpload` responses carry no `Last-Modified`, so a write-cache entry starts without one. The first GET of such an entry sends a conditional request with `If-None-Match` alone, since there is no cached `Last-Modified` to send as `If-Modified-Since`. It takes the header from the `304` response and serves the learned value on that same request. The conditional is needed because a GET that resolves to a write-cache entry is otherwise a cache hit that never contacts S3, and the proxy has no AWS credentials to fetch the metadata separately.
- **Last-Modified on HEAD**: a HEAD is never served from the cache while the field is unknown. It is forwarded to S3, and S3's response rewrites the entry. Two independent mechanisms cause that. A write-cache entry has no HEAD TTL set, so the HEAD lookup misses on it. The lookup also refuses any entry without the field, which covers an entry whose HEAD TTL was set by a previous revalidation that did not learn it.
- **Content-Type**: cached when the PUT request (single part) or the CreateMultipartUpload request (multipart) provides it, and otherwise learned on the first HEAD or cache-miss GET. S3's CompleteMultipartUpload response carries `content-type: application/xml`, the type of its XML body rather than the object's, and the proxy filters it out.

#### TTL transition on first read

A write-cached object starts on its own TTL and moves to the read TTL the first time it is read. The move happens once:

- On the PUT, the TTL is `put_ttl` (default 1 hour).
- On the first GET, the entry moves from `put_ttl` to `get_ttl`. The move runs before the freshness check, so with `get_ttl: 0` that first GET revalidates against S3.
- The move needs a known `Last-Modified`. An entry whose `Last-Modified` is not yet known stays on `put_ttl`, in the write cache, until a revalidation learns it (see the `Last-Modified` header above), and then moves as part of that same operation. This keeps the revalidation trigger from clearing the flag it depends on before it fires.
- An object not read within its TTL expires and is removed.

Frequently read uploads therefore stay cached, and uploads nobody reads expire.

#### Capacity management

The write cache is given a share of the disk cache:

```yaml
cache:
  write_cache_percent: 10.0  # Default: 10% of max_cache_size
```

- The share is a reclamation target, not an admission limit: an upload is cached even when the share is already full, and the excess is reclaimed in the background.
- Reclamation takes the oldest staged objects first, starting with those whose `put_ttl` elapsed unread. An object never read has no recency or frequency score, so the read cache's TinyLFU scoring does not apply.
- Reclamation runs in the background maintenance cycle, never on the request path.
- Staged bytes also leave the share without any reclamation when they are read for the first time, the normal case for a read-after-write workload.
- The S3 operation succeeds whatever happens to the cached copy.

#### Disk-only storage

Write-cached objects are stored on disk only, which keeps the RAM cache for read traffic. A later GET can promote a write-cached object to the RAM cache. Capacity tracking counts the compressed size, which is what the object takes on disk.

#### Limitations

- A single PUT is cached only up to `write_cache_max_object_size` (default 256 MiB); a larger object bypasses the cache.
- Caching is also declined when it would take the cache past `max_cache_size`, or when the cache volume has under 1 GiB free beyond the object's own size. Both are reported as `disk_safety` in `signed_put.skipped_puts_total`. This is the only capacity-shaped refusal; `write_cache_percent` declines nothing (see above).
- A POST object upload is not cached; see the next section.

#### POST object upload is not write-through cached

A browser form upload (`POST /bucket` with a `multipart/form-data` body, its signature carried in form fields rather than an `Authorization` header) reaches S3 correctly and returns S3's own response, but the object is not cached.

The body is a MIME envelope, not the object's bytes: the payload is one part (`file`) beside form fields such as `key`, `policy` and `x-amz-signature`. Caching the body as received would store the envelope, and a later GET would serve corrupt data. Extracting the object part means parsing the envelope, which means buffering the whole upload in memory, so the proxy streams the upload and skips caching.

The effect is one extra S3 round trip: a POST-uploaded object is not in the cache until something reads it through the proxy, and that read caches it as a normal cache miss. Every later read is a cache hit. Presigned PUT, signed PUT and multipart upload are all cached on write as described above.

### Multipart upload cache

#### How multipart caching works

Parts are staged under `mpus_in_progress/{hash(uploadId)}/` as they arrive, each as a `part{N}.bin` with its own `part{N}.json` record, and `CompleteMultipartUpload` renames the requested parts into `ranges/` at their final byte offsets. Each part is compressed by the same content-aware rules as a single PUT, and its compression algorithm carries through to the final range metadata. The completed object is a write-cached entry stored without `Last-Modified`, which the first read learns by revalidating (see the write-cached headers above). `AbortMultipartUpload` deletes the staging directory whatever S3 answers.

[Multipart upload caching](MULTIPART_UPLOAD.md) is the reference for the on-disk layout, the locks, the checks made at completion, fleet behavior and the incomplete-upload sweep.

#### Byte offset calculation

Parts are assembled in part-number order, not upload order. Every part except the last is at least 5 MiB:

```text
Part 1: 5 MiB  -> bytes 0-5242879
Part 2: 5 MiB  -> bytes 5242880-10485759
Part 3: 3 MiB  -> bytes 10485760-13631487

Final ranges:
  {key}_0-5242879.bin
  {key}_5242880-10485759.bin
  {key}_10485760-13631487.bin
```

#### Capacity-aware bypass

Each `UploadPart` makes its own capacity decision, the same one a single PUT makes:

- A part that does not fit is forwarded to S3 without being cached.
- Parts already cached stay in place.
- At completion, a part that was never cached counts as missing, so the object is not cached.
- The upload continues to S3 normally either way.

#### Edge cases

- **A GET during an incomplete multipart upload**: the proxy checks the read cache first and forwards a miss to S3, the source of truth. In-progress parts are not served; they become readable after `CompleteMultipartUpload`.
- **A new multipart upload over an existing object**: the cached object stays valid, and GETs keep serving it, until the new upload completes. On `CompleteMultipartUpload`, the new object's metadata replaces the old.
- **`CompleteMultipartUpload` with missing parts**: a part is missing when it was not cached, because it was bypassed for capacity or went through a proxy that does not share this cache volume. If any part is missing, the object is not cached and S3's success response is returned unchanged.

Once completed, a multipart object uses the cache key `{bucket}/{object_key}` and behaves like a PUT-cached object.

### Write cache error handling

The write cache commits data only after S3 returns success. An authentication or authorization failure (`403`, `401`), a `5xx` or a transport error never produces a cache entry, and S3's response reaches the client unchanged.

- **PUT**: the body streams to S3 and, through a tee, to a staging file. The background cache task learns S3's status when the response arrives; on anything other than success it discards the staging file and commits nothing.
- **UploadPart**: a failed part is not staged. Parts that succeeded stay staged, and the upload can continue.
- **CompleteMultipartUpload**: on an S3 error no object metadata is written and the staged parts are kept, so a client retry can still succeed. The incomplete-upload sweep removes them if the upload is abandoned (`incomplete_upload_ttl`, default 1 day).
- **AbortMultipartUpload**: removes the staged parts whatever S3 answers.

Failed operations do not move the cache hit or miss metrics. Two log lines record them:

```text
DEBUG Streaming cache: S3 non-success status, not caching: cache_key=bucket/object.txt, status=403 Forbidden
ERROR CompleteMultipartUpload S3 error: bucket=my-bucket, key=large-file.bin, status=403
```

## How much memory a cached serve may use

A cached serve may assemble at most 8 MiB of uncompressed bytes in one buffer. The limit is part of how the read paths are built, not a setting. No configuration field controls it, it applies whether or not the in-flight memory ledger is enabled, and it is the same on every deployment. The memory a cached serve takes is bounded by this limit, not by the size of the object or of the extent its bytes are stored in.

Above the limit, a cached serve takes one of three forms, chosen after the lookup has established coverage and freshness:

| Coverage of the requested bytes | How it is served |
|---|---|
| Inside one cached extent, or a whole object held as one extent | The requested slice is streamed out of that extent: `206` for a ranged request, `200` for a request with no `Range` |
| Spanned by several adjacent cached extents with no gap | Each extent is streamed in turn, chained into one response body |
| A gap, partial caching, or cached data that cannot be read | Not a cached serve. The whole requested interval is refetched from S3 through the streamed cache-miss path and stored as one new range, so the next identical request is a single-extent cache hit |

A HEAD never reaches any of this: it reads no range bytes, and its headers come from the cached metadata.

Two consequences to expect:

- **The third row costs hit ratio once per range.** A partly cached range above the limit is fetched from S3 in full rather than merged with what is cached, so the cached part is fetched again. It heals: the refetch is stored, and the repeat is a hit. A degrade counter that keeps rising for the same range means the refetch is not being stored, which needs investigating, not tuning.
- **Latency on a compressed extent grows with the offset.** A range deep inside an LZ4-compressed extent is decoded from the extent's start, discarding the bytes before it. Memory does not grow with the offset; time does.

Each row, and each reason within the third, has its own counter under `inflight_memory` on `/metrics` and in the OTLP export: `buffered_serve_sliced_streams_total`, `buffered_serve_degraded_chained_stream_total`, and a `buffered_serve_degraded_*` counter per forward or heal reason. Serves that stayed inside the limit are counted by materialized size in `buffered_serve_size_{le_64kib,le_1mib,le_8mib,gt_8mib}_total`, with the largest single buffer in `buffered_serve_peak_materialised_bytes` and the no-`Range` whole-object share of those buckets in `buffered_serve_full_object_total`. A `buffered_serve_size_gt_8mib_total` above zero, or a peak above 8,388,608, means something materialized past the limit every serve is checked against; report it as a defect. See [`inflight_memory`](METRICS_REFERENCE.md#inflight_memory).

Page widening is the one cached read this limit does not govern, because it holds a page rather than an extent. [Disk footprint](#disk-footprint) and [RAM cache uses the page as its unit](#ram-cache-uses-the-page-as-its-unit) give the figure that bounds it instead.

## Page-aligned range caching

Page-aligned range caching, also called range read widening, turns an eligible small ranged GET into a fetch of the fixed-size, aligned page that contains it. The proxy caches the whole page, on disk and in RAM, and serves the client exactly the bytes it asked for. Later small reads in the same page are served with no S3 round trip, whether they come from the same reader, a re-run or another reader of the same cache. Concurrent small reads in one page share one fetch.

The access pattern it targets is a read of the file's trailing footer followed by clustered reads at scattered offsets, as columnar readers (Parquet, ORC) produce. Caching the footer is the special case where the read lands in the object's last page. The proxy never parses object content to detect this; it works only from the request's byte offsets.

### Eligibility: which clients can use this

Widening requires that `range` is absent from the request's SigV4 `SignedHeaders`. Widening works by rewriting the upstream `Range`, and rewriting a signed header breaks the client's signature. The proxy has no credentials and cannot sign again, so this is a hard constraint, not a tuning choice.

The AWS CLI and every official AWS SDK (botocore and boto3, Java, JavaScript v3, Rust, Go v2) sign `Range`; [Which clients sign Range?](#which-clients-sign-range) has the full list. A workload made up only of those clients gets no widening at all, and no error says so: every request goes through the ordinary unwidened range path and increments `page_cache.skipped_signed_range`.

Widening is useful for:

- **Presigned-URL access**: video streaming with browser range-seeking, PDF viewers, and columnar readers fetching through presigned URLs issued by an application server. A presigned URL that lists `range` in `X-Amz-SignedHeaders` is not widened, and one that does not list it is.
- **rclone**, which uses its own SigV4 implementation that signs a minimal header set.
- **Custom HTTP clients** that add `Range` after signing, or sign only the required headers.

It excludes Spark, Hadoop, Hive, Trino, Flink, `aws s3 cp`, `aws s3api get-object --range`, s5cmd and Mountpoint. Columnar data read through any of those does not qualify, although it is the access pattern the mechanism was designed around.

### Off by default, per-key opt-in

Widening is never enabled globally. It is off unless a `cache_rules.json` rule turns it on for matching keys with the `page_widening` and `page_size` rule fields (see [Cache rules](CACHING.md#cache-rules) and the [rules file format](CONFIGURATION.md#cache-rules)):

```json
{ "pattern": "**/*.parquet", "page_widening": true, "page_size": 16777216 }
```

- `page_widening` (bool, default `false`): turns widening on for the keys the rule matches.
- `page_size` (bytes, default `16777216`, which is 16 MiB, when the rule enables widening without setting it): the page size `P` of the grid. It must be greater than 0 and at most `67108864` (64 MiB), the RAM admission ceiling described below. That check binds a rule that enables widening. A rule that sets only `page_size` is not rejected, so a widened key that matches both rules can inherit an out-of-range value. Such a value is clamped to 64 MiB, and an inherited `page_size: 0` resolves to the 16 MiB default. The rules file still loads, and each clamp logs a warning naming the rule's index; lower the value to silence it.

### Operator warning: amplification is workload-dependent

Widening every small read to a full page pays off when reads cluster within pages, as in columnar scans and footer reads. It costs real transfer when reads are scattered: a 4 KiB random read against a key with a 16 MiB page size becomes a 16 MiB fetch. Enable `page_widening` only for key patterns whose reads cluster within pages.

### Mechanism

A request is widened only when all of these hold:

- the matching rule sets `page_widening: true` for the key;
- the method is `GET` with a byte `Range` header, and the requested length is less than `P`;
- `range` is not signed, either in the `Authorization` header's `SignedHeaders` or in a presigned URL's `X-Amz-SignedHeaders`. A signed `Range` is forwarded unmodified, because its bytes cannot be rewritten safely;
- the request carries no `partNumber` query parameter. Part requests take the separate part path, which widening does not affect.

For an eligible request:

1. The requested range is mapped to the page or pages it overlaps on a fixed grid anchored at offset 0. The page index is `floor(start / P)`, and the page is `[page_index * P, page_index * P + P - 1]`, clamped to the object's last byte on the final page. A request near a page boundary overlaps two pages.
2. A suffix request (`Range: bytes=-N`) is converted to its absolute range once the object size is known from cached metadata, then mapped to the pages it overlaps the same way. It is never widened to a single fixed "last page", which could be smaller than the requested suffix. When the size is not yet known, the proxy requests `bytes=-P` instead, which covers the requested bytes and ends at the object's end but is not aligned to the grid. Once the size is known, later suffix reads take the aligned path.
3. A range request with a conditional that S3 must evaluate is not widened. By default that is every conditional; with `evaluate_conditions_from_cache`, it is anything other than an `If-Match` or `If-Range` naming the cached ETag. Such a request is forwarded with the client's own range, and S3's response is returned unchanged. In that mode, an `If-Match` or `If-Range` that matches the cached ETag is widened like an unconditional request. A `206` response is sliced to the client's original range and the page is cached. A `200` with the full object is passed through unchanged, and is never sliced or cached as a page.
4. The proxy fetches only bytes it does not hold and that no in-flight fetch for the same page is already retrieving; such fetches share the in-flight tracker used for other cache misses. When a request overlaps two pages, each page is a separate fetch. The pages are filled one at a time, each released before the next is filled, so at most one page is held in memory. A page's RAM promotion finishes before the next page is loaded, and only the last page's promotion runs after the response is returned. A cold read that straddles two pages therefore pays two upstream round trips in sequence, where one merged request would pay one.
5. If the page fetch fails, with an error status or a network error, the proxy retries the client's original range, serves it normally and skips caching for that request. Widening never turns a request that would otherwise succeed into a failure.
6. The client always receives exactly the bytes it requested: a `206 Partial Content` response whose `Content-Range` and `Content-Length` match its original request, sliced from the page.

### Disk footprint

A page is stored through the same range-store path as any other cached range, with no separate on-disk format, and it works with the existing range merging and consolidation. A single small read on a widened key caches a whole page, though, not only the requested bytes, so widening increases the disk space each object takes. Size `max_cache_size` with that in mind for the keys where widening is on.

### RAM cache uses the page as its unit

When widening is on for a key, the RAM cache stores and looks up data by the containing page's bounds, not by the client's requested range. A read inside a page looks up that page and slices the requested bytes from it. Promotion to the RAM cache promotes the whole page. On the page path, a page is promoted on a later disk hit, not on the cold fetch that first cached it. Each page is written to the RAM cache once per read, including a page stored as one extent, and only when its ETag is known. A page assembled from several cached extents is held in RAM as the page alone; the extents it was read from are not promoted under their own keys. RAM cache recency and frequency are recorded against the whole page, so any hit inside the page keeps the entire page resident. When widening is off for a key, the RAM cache works per range.

The RAM cache admits any single entry up to 64 MiB (67,108,864 bytes, a compile-time constant rather than a configuration field), whether or not any key uses page widening. It does so by clamping the effective number of RAM cache shards so that `max_ram_cache_size / effective_shard_count` is at least 64 MiB, and logs a warning when the clamp reduces concurrency below the configured `ram_cache_shard_count`. See [RAM sizing and the admission ceiling](CONFIGURATION.md#ram-sizing-and-the-admission-ceiling) for the sizing guidance and the clamp formula, and the [example configuration](../config/config.example.yaml) for a worked example.

### Coalescing and coherency

Concurrent small reads that overlap the same page on one proxy share one upstream fetch, keyed on the page's bounds rather than the client's requested range, and are served from the resulting cached page. Cached pages use the object's normal `get_ttl`; there is no separate TTL per page. All the pages of an object therefore expire together, and an ETag or version mismatch removes the whole object, cached pages included, like any other cached range.

### Page cache metrics

These counters are under `page_cache` in `/metrics` and on the [dashboard](DASHBOARD.md); [`page_cache`](METRICS_REFERENCE.md#page_cache) is the field reference.

| Metric | Meaning |
|---|---|
| `page_cache.widened_requests` | Small reads widened to a page fetch |
| `page_cache.bytes_prefetched` | Bytes fetched beyond what clients requested, with a derived amplification ratio |
| `page_cache.page_hits` | Small reads served from an already cached page, with no S3 fetch |
| `page_cache.skipped_signed_range` | Requests that were otherwise eligible but left unmodified because their `Range` was signed. Expected for AWS CLI and SDK traffic; see [Eligibility](#eligibility-which-clients-can-use-this) |
| `page_cache.fallbacks` | Widened requests served with the client's original range from S3 instead: the page fetch failed or did not cover the requested bytes, the pages could not be shown to belong to one version of the object, or the object's length was not yet known |
| `page_cache.ram_page_promotions` | Pages promoted to the RAM cache |
| `page_cache.ram_page_promotion_skipped` | Pages not promoted to the RAM cache, for example because they exceeded the RAM budget |

A widened request is also logged at `DEBUG` with the cache key, the requested range and the page range.

### Non-goals

The proxy never parses object content: no Parquet or ORC footer, Thrift metadata or magic-byte parsing. Every decision uses request metadata only, meaning the key's pattern match and the byte offsets and lengths. There is no prediction of which chunks come next, no widening of reads already at least `P` long, and no coordination of page fetches between proxies.

## Cache coherency on the read path

Two checks run before cached bytes are served. Either can invalidate the cache rather than serve stale data.

### Full object range replacement

When the proxy caches a full object, it removes every existing partial range of that object, so a range from a previous version cannot be served beside the new object:

1. It removes all existing range files for the cache key.
2. It updates the object's metadata to list only the full object.
3. It does both under the key's metadata lock.

```text
Existing cache: object.bin ranges 0-8 MiB, 16-24 MiB, 32-40 MiB
A PUT of a new version caches the full object as range 0-50 MiB
The proxy removes ranges 0-8 MiB, 16-24 MiB and 32-40 MiB
Only the full object, 0-50 MiB, remains
```

### ETag validation for range requests

Ranges carry no ETag of their own: the object's ETag in its `.meta` covers every cached range. When the caller of the range lookup already knows the object's current ETag, from S3 or from a fresher metadata read, the lookup compares it with the cached ETag before returning coverage:

1. **Comparison**: the current ETag is compared with the cached object ETag, ignoring one balanced pair of quotes on either side.
2. **Mismatch**: every cached range for the key is invalidated, on disk and in RAM, and the range is reported missing so it is fetched from S3.
3. **No current ETag**: when the caller has none, no comparison is made, and the TTL checks decide freshness.
4. **No metadata**: there is nothing to compare, and the range is treated as missing.

```text
Range request -> read the cached .meta (or use the caller's preloaded copy)
              -> current ETag known?
                   match:    continue with the cached coverage
                   mismatch: invalidate all ranges, fetch from S3
              -> no .meta:   the range is missing, fetch from S3
```

A mismatch logs, for example:

```text
INFO ETag validation failed: invalidated 3 stale ranges for cache_key=bucket/object.txt, cached_etag="abc123", current_etag="def456"
```

### Metadata consistency

The proxy keeps range files and object metadata consistent:

- Metadata updates write a temporary file and rename it into place.
- An exclusive lock on the key prevents concurrent changes during an update.
- Removing range files and updating the metadata happen together: invalidating ranges removes them from the metadata, a new version updates the object's metadata, and removed ranges are tracked so the two stay consistent.
- Failed operations are logged and retried in the background.

### Coherency error handling

- **Corrupt metadata**: detected, and the affected entries are invalidated.
- **Missing range file**: handled as described in [Error handling and fallback](#error-handling-and-fallback).
- **Lock contention**: a contended metadata lock is retried a bounded number of times, and the request then falls back instead of waiting indefinitely. There is no elapsed-time limit on the retries.
- **Cleanup failures**: logged, and requests keep being served.

When a coherency operation fails, the request is forwarded to S3, so the client still receives correct data, and the failed operation is retried in the background where possible.

### Coherency log lines

```text
WARN ETag mismatch detected for cache_key: bucket/object.txt, cached_etag: "abc123",
     current_etag: "def456", action: invalidating stale ranges
INFO ETag validation failed: invalidated 3 stale ranges for cache_key=bucket/object.txt,
     cached_etag="abc123", current_etag="def456"
```

### Coherency cost and metrics

ETag validation sends no S3 request of its own. Range cleanup is batched and runs in the background without blocking requests.

Two counters observe these operations: `cache.cache_etag_mismatches_total` and `cache.cache_range_invalidations_total` (see [`cache`](METRICS_REFERENCE.md#cache)). `cache.cache_etag_validations_total` and `cache.cache_orphaned_ranges_cleaned_total` are published but always zero: nothing outside the metrics module's own tests writes them, so neither says anything about this path.

## Signed range request handling

When a client signs a GET with AWS Signature Version 4 (SigV4), the `Range` header is in the `SignedHeaders` list if it was present at signing time. The proxy then cannot change it: rewriting `Range` to fetch only the missing bytes would invalidate the signature, and S3 would return `403 Forbidden`.

### Which clients sign Range?

The AWS CLI and all official AWS SDKs (botocore and boto3, Java, JavaScript v3, Rust, Go v2) sign every header that is not on botocore's internal blocklist. `Range` is not on it, so any request made through an official AWS SDK or the CLI with a `Range` header has `Range` signed. That includes:

- `aws s3api get-object --range`;
- `aws s3 cp`, whose CRT parallel downloads sign `Range` and `If-Match` on each ranged part;
- s5cmd, which uses the AWS SDK for Go v2;
- Mountpoint for Amazon S3, which uses the CRT's `auto_ranged_get` and signs `Range` internally.

Clients that usually do not sign `Range`:

- **Presigned URLs with `Range` added at request time**: the signature was computed before the header existed, so `range` is not in `X-Amz-SignedHeaders`. A presigned URL generated with a `Range` parameter, as aws-sdk-js v2's `getSignedUrl` can, does sign it.
- **curl, wget and other HTTP clients** using presigned URLs.
- **rclone**, which uses its own SigV4 implementation and signs only `host` and the `x-amz-*` headers.
- **Older MinIO Go clients**, with a similarly minimal `SignedHeaders` set.

The proxy checks every request, parsing the `SignedHeaders` field of the `Authorization` header or the `X-Amz-SignedHeaders` query parameter of a presigned URL, so it handles signed and unsigned `Range` without configuration.

### How signed ranges are served

A request is treated as a signed range request when:

- it has an `Authorization` header using `AWS4-HMAC-SHA256` (SigV4) or `AWS4-ECDSA-P256-SHA256` (SigV4A), or is a presigned URL;
- its `SignedHeaders` (the `Authorization` header's, or a presigned URL's `X-Amz-SignedHeaders`) includes `range`;
- the requested range has cache gaps: it is partly cached or not cached at all.

A fully cached range is served from the cache whether or not `Range` is signed. Otherwise:

1. The proxy checks whether `range` is among the signed headers. The check runs only when the range has cache gaps.
2. If it is, the proxy forwards the original request to S3 as received, plus one `Via` entry. Every signed header is forwarded exactly as received; the proxy adds only unsigned headers (`Via`, and `Referer` when enabled).
3. The response streams to the client at once, with no buffering, and is cached in the background without delaying the client. A cache failure does not affect the response.
4. Later requests for the same range are served from the cache with no S3 request.

The first request fetches the full range from S3, the same as an unsigned cache miss.

## Part caching

The request-level behavior is described under [Part number cache](#part-number-cache). This section covers how part ranges are recorded.

### Part range storage

When a GET with a `partNumber` parameter returns from S3, the proxy:

1. parses the `Content-Range` header (for example `bytes 0-8388607/5368709120`);
2. stores the exact `(start, end)` byte range in `part_ranges` for that part number;
3. updates `parts_count` from the `x-amz-mp-parts-count` header when present;
4. saves the updated metadata to disk.

Later requests for the same part number use the stored range to look it up in the cache.

### CompleteMultipartUpload handling

At multipart completion, the proxy:

1. parses the `CompleteMultipartUpload` XML request body;
2. reads the list of requested parts and their ETags;
3. checks each cached part's ETag against the request's, with quotes removed from both;
4. builds `part_ranges` from the requested parts, with cumulative byte offsets;
5. deletes cached parts that the completion request does not list.

An ETag mismatch skips cache finalization; the request is still forwarded to S3. Part caching is automatic and needs no configuration.

## Download coordination

Download coordination, or coalescing, prevents redundant S3 fetches when several concurrent requests arrive for the same uncached object. One request fetches from S3 while the others wait, and then they are all served from the cache.

```text
Request 1 (cache miss) -> registers as the fetcher -> fetches from S3 -> caches -> completes
Request 2 (cache miss) -> registers as a waiter   -> waits for the fetcher -> served from the cache
Request 3 (cache miss) -> registers as a waiter   -> waits for the fetcher -> served from the cache
```

The proxy tracks in-flight fetches by key: the first request for a key becomes the fetcher, and later requests become waiters. Coordination is per proxy.

### Scope

Coalescing covers three request types, each with its own key:

| Request type | Key format | Example |
|---|---|---|
| Full object GET | `"{cache_key}"` | `"my-bucket/path/to/file.txt"` |
| Range request | `"{cache_key}:{start}-{end}"` | `"my-bucket/path/to/file.txt:0-8388607"` |
| Part number request | `"{cache_key}:part{N}"` | `"my-bucket/path/to/file.txt:part2"` |

Different request types for the same object proceed independently: a full-object GET does not block a range request for the same object.

### Waiter behavior

A waiter waits up to `wait_timeout_secs` for the fetcher to finish:

- **The fetcher succeeds**: the waiter tries the cache first, since the fetcher should have cached the data. On a hit, it serves the response from disk or RAM with no S3 request. If the lookup misses, for example because the metadata is not yet consolidated, it makes its own S3 fetch.
- **The fetcher fails**: the waiter makes its own S3 fetch.
- **The wait times out**: the waiter makes its own S3 fetch.

```yaml
cache:
  download_coordination:
    enabled: true           # Turn coalescing on or off (default: true)
    wait_timeout_secs: 30   # Waiter timeout in seconds (default: 30, clamped to 5-120)
```

### Coalescing metrics

`/metrics` reports download coordination under `coalescing` (see [`coalescing`](METRICS_REFERENCE.md#coalescing)):

| Metric | Description |
|--------|-------------|
| `waits_total` | Waiter registrations |
| `cache_hits_after_wait_total` | Waiters served from the cache |
| `timeouts_total` | Waiters that timed out |
| `s3_fetches_saved_total` | S3 fetches avoided by coalescing |
| `average_wait_duration_ms` | Average time a waiter waited |

### When to disable it

Consider turning download coordination off for a single proxy whose clients never send concurrent duplicate requests, for a workload with no repeated requests, or while debugging cache behavior.

## Observing range merge efficiency

Range-merge efficiency is reported only in the logs: there is no `range_merge.*` section in `/metrics`, and the figures come from `debug`-level application log lines. See [Cache efficiency metrics](#cache-efficiency-metrics) for how to read the percentage.

```bash
grep -h "Range merge succeeded" /logs/app/*/s3-proxy.log*  # Range merges and their efficiency (debug level)
grep -hE "Part cache|Part cached|cache_type=\"?part_cache" /logs/app/*/s3-proxy.log*  # Part caching operations
```

A merge line reads, for example, `Range merge succeeded: cache_key=bucket/large-file.bin, size=5242880 bytes, cache_efficiency=75.50%`. A part-cache miss logs `Part cache MISS: cache_key=…, part_number=…` and `Part cache MISS - fetching from S3`, both at `debug`. Storing a part logs `Part cached - stored as range` at `info`, and a hit logs `Cache operation completed` with `cache_result=HIT` and `cache_type=part_cache` at `debug`.

## Internals: lookup purpose and what authorizes a cached serve

This section is for developers changing a read path.

Every cache-coverage lookup states why it is asking, because the answer changes what an expired entry looks like. `RangeLookupPurpose` is a required argument of both `DiskCacheManager::find_cached_ranges` and `RangeHandler::find_cached_ranges`. It has no default, so a new call site has to choose.

| Purpose | Past stored expiry | For callers that |
|---|---|---|
| `FreshServe` | returns no coverage | may serve what they get with no further freshness check |
| `RevalidationCandidate` | returns coverage, marked `StoredFreshness::Expired` | evaluate freshness themselves before serving |

At a call site, ask what bounds the staleness between this lookup and bytes reaching a client. A live-TTL verdict, a client validator or a successful `304` means `RevalidationCandidate`. Nothing but the lookup itself means `FreshServe`. If the answer is "nothing at all", the call site is a defect whichever purpose it names.

`RevalidationCandidate` grants discovery, not permission, and `RangeOverlap` enforces that in its accessors rather than by convention:

- `is_serveable_unvalidated()`: complete coverage and stored-fresh. Safe on its own.
- `has_complete_coverage()`: coverage only. The caller must hold a separate authority and say which one.

Serving expired bytes therefore always means reaching for the second accessor and writing down why it is allowed.

### Which authority each cached serve holds

| Read path | Purpose | What authorizes the serve |
|---|---|---|
| Full-object mainline GET | `RevalidationCandidate` | Mode B matching `If-Match`, a live-TTL `Fresh` verdict, or a `304` |
| Range path, early full-object shortcut | `FreshServe` | A live-TTL `Fresh` verdict; when expired it falls through rather than serving |
| Range path, range-specific lookup | `RevalidationCandidate` | A live-TTL `Fresh` verdict, or a `304` |
| Page widening (`find_page_overlap`) | `FreshServe` | Stored expiry only, plus a live-TTL check once per request ahead of it; see below |
| Part-scoped lookup (`?partNumber=N`) | `FreshServe` | A live-TTL `Fresh` verdict and stored expiry; an expired part revalidates with one conditional request (`304`) |
| Post-`304` validated-serve helpers | `RevalidationCandidate` | The `304` itself; coverage is checked again, because a `304` proves the version, not the coverage |
| Degraded coalescing fallbacks | `FreshServe` | Stored expiry and the live `get_ttl` verdict; never for `get_ttl: "0s"`. These run when S3 returned an error, so there is no validation to rely on |

For page widening, `find_page_overlap` passes `FreshServe` and is bounded only by stored expiry. Its caller, `try_widened_range_request`, checks the currently resolved `get_ttl` against the object's live age (since it was cached or last confirmed by a `304`) once per request, before either widening branch runs. Checking once per page instead would multiply cache-volume reads by the page count and undo the point of widening. On expiry it returns `None` and hands the request to the unwidened path's own revalidation, rather than running a second conditional-request flow. An already cached widened key therefore honors a tightened or zeroed `get_ttl` on its next request, the same guarantee the mainline GET paths give.

The RAM cache check in `fill_page`, made before `find_page_overlap`, is gated on `resolved.ram_cache_eligible`. When the caller supplies a known `current_etag`, it is compared with the RAM entry's own ETag before serving, and a mismatch invalidates the entry and falls through to disk. When `current_etag` is unknown, the common case for a cold request, the RAM entry is trusted for `cache.ram_cache_verification_interval` (default `1s`) and then checked again against the shared `.meta`. A key whose `get_ttl` is `"0s"` is never widened.

## See also

- [Caching](CACHING.md): what is cached, and what bypasses the cache
- [Cache internals](CACHE_INTERNALS.md): the on-disk and in-memory structures these paths read
- [Cache freshness](CACHE_FRESHNESS.md): TTLs and conditional requests
- [Hedged upstream requests](HEDGING.md): racing a second upstream fetch to cut tail latency
- [Multipart upload caching](MULTIPART_UPLOAD.md): the multipart write path
- [`/metrics` reference](METRICS_REFERENCE.md): the `cache`, `page_cache`, `inflight_memory` and `coalescing` counters
