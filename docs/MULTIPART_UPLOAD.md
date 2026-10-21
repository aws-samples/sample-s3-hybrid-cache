# Multipart upload caching

Hybrid Cache for Amazon S3 caches a multipart upload as its parts pass through, so the completed object can be read from the cache as soon as `CompleteMultipartUpload` succeeds. S3 stays the source of truth: the proxy keeps the object only when S3 accepted the upload and the cached parts match what S3 assembled. This page covers `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload` and `AbortMultipartUpload`; reads of one part (`GetObject?partNumber=N`) are covered in [Part number cache](CACHE_READ_PATHS.md#part-number-cache).

## Turn it on

Multipart caching is part of the write cache, which is on by default:

```yaml
cache:
  write_cache_enabled: true   # The default; false stops caching PUT and multipart uploads
```

A cache rule can also turn the write cache off for matching keys. [Write cache configuration](CONFIGURATION.md#write-cache-configuration) lists the write cache fields, and [Caching](CACHING.md) describes the write cache as a whole.

## How it works

S3 requires every part except the last to be at least 5 MiB. The proxy stages each part as it arrives, and on a successful `CompleteMultipartUpload` turns the parts the request names into one cached range per part, at the part's byte offsets in the final object.

### The four operations

All four operations carry AWS SigV4 signatures. The proxy holds no credentials and cannot sign, so it forwards the signed bytes and headers to S3 unchanged, adding only an unsigned `Via` header unless the request signs `via`. The caching runs beside the forwarding and never replaces it.

| Operation | HTTP | Query |
| --- | --- | --- |
| `CreateMultipartUpload` | `POST` | `?uploads` |
| `UploadPart` | `PUT` | `?uploadId=X&partNumber=N` |
| `CompleteMultipartUpload` | `POST` | `?uploadId=X` (no `partNumber`) |
| `AbortMultipartUpload` | `DELETE` | `?uploadId=X` |

### Wire framing

`UploadPart` bodies are often aws-chunked: an inner SigV4 streaming-signature framing inside the HTTP body. The proxy passes that inner layer to S3 untouched, with the same chunk data and the same per-chunk signatures.

The outer HTTP framing is separate. A client sends the body either with `Content-Length` (the AWS CLI's CRT client) or with `Transfer-Encoding: chunked` (pyarrow and some SDK clients). The proxy's HTTP server removes whichever outer framing the client used before the proxy sees the body. For a chunked request, the proxy therefore wraps each forwarded frame in HTTP chunk framing again. Writing the unwrapped bytes straight to S3 would be neither valid HTTP chunking nor a valid `Content-Length` body, and S3 would reject it with `IncompleteBody`.

### On-disk layout

While an upload is in progress, its state lives under `{cache_dir}/mpus_in_progress/{hash(uploadId)}/`, where `hash(uploadId)` is the 64-character lowercase hex BLAKE3 digest of the upload ID. The upload ID itself is recorded in `upload.meta`:

```text
mpus_in_progress/{hash(uploadId)}/
├── upload.meta       # Upload-level facts, written once at CreateMultipartUpload
├── upload.lock       # Exclusive lock file for the whole upload
├── part1.bin         # Part 1's bytes in an LZ4 frame (compressed or stored)
├── part1.json        # Part 1's record: size, ETag, compression algorithm
├── part1.lock        # Exclusive lock file for part 1
├── part2.bin
├── part2.json
├── part2.lock
└── ...
```

There are two lock namespaces. `part{N}.lock` guards the publishing and recording of one part and is taken on every `UploadPart`. `upload.lock` covers the whole upload and is taken only by `CompleteMultipartUpload`; the incomplete-upload sweep also respects it (see [What reaps abandoned staging](#what-reaps-abandoned-staging)). Neither holder takes the other lock, so a part never waits on a completion and a completion never waits on one part's lock. Complete observes the parts by polling instead, as described in [Complete waits for the parts it names](#complete-waits-for-the-parts-it-names).

Each part owns its own files: no shared document grows with every `UploadPart`. `part{N}.json` is written once, atomically (a `.tmp` file, fsync, then rename), by whichever proxy cached part N. The cost of a part therefore does not depend on how many parts came before it, and two parts never contend.

`upload.meta` holds only upload-level facts:

```json
{
  "upload_id": "...",
  "cache_key": "bucket/key",
  "started_at": { ... },
  "content_type": "image/jpeg",
  "representation_headers": { ... }
}
```

At completion the proxy assembles the upload from `upload.meta` plus the part records on disk. A `parts` array or `total_size` in `upload.meta` is ignored; the parts always come from `part{N}.json`. A part record that fails to parse is skipped with a warning, so that part counts as not cached and the missing-parts gate declines to finalize, as if the part had never arrived.

Each proxy publishes the bytes of the parts it has staged to `metadata/_shard_state/in_progress_parts/{instance_id}.inprog` once per consolidation cycle (every `cache.shared_storage.consolidation_interval` on a healthy volume). Readers that need the fleet-wide figure combine those documents with the background collection's walk of `mpus_in_progress/` instead of walking it themselves. The walk remains the check: `cache.in_progress_part_bytes_drift_bytes` reports how far the published figures stood from it. A proxy's readers go back to walking the volume when a published figure reads below the walk, when a proxy publishes nothing, or when a document is older than three of its publication intervals. The limit of 5,000 in-progress entries is counted from a walk either way. See [Where the in-progress multipart term comes from](CACHE_INTERNALS.md#where-the-in-progress-multipart-term-comes-from).

### Starting an upload

The proxy forwards `CreateMultipartUpload` unchanged. When S3 succeeds, the proxy records the upload and the object's representation headers in `upload.meta`. It answers the client when S3 answers, and waits at most 500 ms for the disk-safety check and the `upload.meta` write. If either is still running then, the upload is not cached, and a tracker written later is removed.

No `upload.meta` is written, so the upload is not cached, when:

- the write cache's backlog is full;
- the cache volume already holds 5,000 in-progress multipart entries: one per tracked upload plus one per recorded part, across every proxy sharing the volume. This is counted as `multipart_tracking_full` in `skipped_puts_total`, or `multipart_tracking_unknown` when the count cannot be read.

These cases also invalidate the key's cached copy when S3 succeeds:

- an SSE-C `CreateMultipartUpload`, counted as an `sse-c` bypass;
- a key whose cache rules turn the write cache off, counted as `write_cache_disabled` in `skipped_puts_total`;
- a Create forwarded uncached because the bucket's origin marker or the first cache-size read did not finish within 250 ms, counted as `origin_marker_unresolved` or `capacity_unknown`.

Past the same 5,000-entry limit, an `UploadPart` is forwarded without being staged and its upload stops being cached: the upload's directory and any parts it had recorded are removed. A part whose upload directory does not exist yet counts as two entries, because staging it creates the directory too. When the count cannot be read, the part is forwarded unstaged and the parts already recorded are kept.

### Uploading parts

```text
CreateMultipartUpload
  └─ writes upload.meta (upload-level facts only)

UploadPart (any order, any number of times)
  └─ writes part{N}.bin and part{N}.json, one pair per part

CompleteMultipartUpload                      AbortMultipartUpload
  ├─ waits up to 10 s for the named parts       └─ deletes mpus_in_progress/{hash(uploadId)}/
  ├─ renames the parts into ranges/
  ├─ writes the object's metadata
  └─ removes the upload directory
```

Uploading the same part number again rewrites `part{N}.json` (new size, new ETag) and overwrites `part{N}.bin`. This matches S3, where a new `UploadPart` with the same part number replaces the previous one.

### Complete waits for the parts it names

The proxy answers `CompleteMultipartUpload` when S3 answers. Cache finalization runs in the background, and the response waits for it for at most 250 ms. If S3 answers `200` with an `<Error>` document, or the result carries no ETag, the upload is not cached, its staged parts are removed, and the body is passed to the client unchanged. Finalization takes one of the write cache's store slots, which also bound PUT stores. With none free, the completed object is not cached (`skipped_puts_total{backlog_full}`) and its staged parts are left for the incomplete-upload sweep; the client's response is unaffected.

Finalization first polls for the `part{N}.json` records of the parts its own request body lists, for up to 10 seconds (a fixed internal limit) at 100 ms intervals. The wait exists because each part's cache task finishes after the client's response for that part. The proxy answers the client first, then drains the part's body into the cache, waits for S3's result and finalizes the part. Clients send Complete as soon as S3 acknowledges the last part, often before the last record lands, so without the wait Complete would find only some of the parts and decline to cache. On a working cache, waiting is the normal path.

Two properties follow:

- Complete latency includes at most 250 ms of local cache work. The wait for part records runs in the background finalization, so it delays caching, not the client. A GET sent right after a slow Complete is served from S3 until the finalization publishes the object.
- Running out of time is not an error. S3 has already accepted the upload. The wait returns, the missing-parts gate reports it, and the object is not cached.

The poll reads records without taking `upload.lock`, because taking the exclusive lock every 100 ms would contend with the part tasks it is waiting for; the authoritative read happens under the lock afterwards. Polling the cache volume, rather than tracking tasks in process memory, is what makes this correct on a fleet: one upload's parts may pass through several proxies and its Complete through another.

### Publishing the completed object

On a successful completion, each `part{N}.bin` is renamed from the upload directory into the sharded ranges tree at `ranges/{bucket}/{XX}/{YYY}/{key}_{start}-{end}.bin`, where `start` and `end` are the part's byte offsets in the final object. The upload directory is then removed. On the read path, the range files are indistinguishable from any other cached range.

When the completion replaces an object that is still cached, it removes that object's range files and `.meta` under the key's metadata lock before renaming any part. A new range with the same byte offsets as an old one has the same file path, so removing the old files first keeps the new object's bytes at that path.

The completed object is published as a staged object in the write cache. Each of its ranges gets a staging record in the write ledger (see [Eviction](EVICTION.md)) before its `.meta` is renamed into place. A proxy that stops between the two steps leaves records for an object that was never published; staging reclamation reads those as absent and discards them once they are older than 60 seconds. It never leaves a published staged object that staging reclamation cannot find. If a record cannot be written, the completion is not cached.

CompleteMultipartUpload caches an upload only when its `upload.meta` records the representation headers from CreateMultipartUpload. An upload without that record is not cached, and its staged parts are removed at Complete. That happens when the Create was answered while the backlog was full or its cache setup took longer than 500 ms. It also happens when the disk-safety check or the in-progress multipart limit refused it, or when no proxy in the fleet saw the Create.

## Correctness gates

The cache keeps a completed multipart object only when all of the following hold. If any fails, nothing is cached for that object; S3 still returns its success response, and the proxy only loses a cache hit. Most failures also remove the upload directory; [Failure paths](#failure-paths) lists the exceptions.

1. S3 returned 2xx for the `CompleteMultipartUpload`. S3 is the source of truth, so if it rejected the Complete (missing parts, a bad ETag) there is nothing to cache. The staged parts are kept so that a client retry can still succeed.
2. The `CompleteMultipartUpload` request body parses as a valid `<CompleteMultipartUpload>` XML document. A malformed or empty body skips finalization. The requested part list is the only statement of what the completed object contains, and it is the set the wait waits for. Without it, gates 4 and 5 would have nothing to check, and a partly recorded upload could finalize as a shorter object.
3. The assembled part set is not empty.
4. Every part listed in the request body is present locally, as both a `part{N}.json` record and a `part{N}.bin` file. A requested part can be missing because its cache task has not finished, which the 10-second wait covers. It can also be missing because it went to a proxy that does not share this cache volume, because it was uploaded directly to S3, or because the proxy restarted mid-upload.
5. Each part's ETag in the request equals its recorded ETag, after removing surrounding quotes from both. Any mismatch skips finalization.
6. The assembled part count agrees with S3's. A multipart ETag has the form `"<md5-of-md5s>-<part-count>"`, and the proxy compares that suffix with the number of parts it assembled. On disagreement nothing is cached. The comparison runs after any object this upload replaces has been removed from the cache, and before any part moves into `ranges/`. A disagreement therefore leaves the key uncached and removes the upload directory with its parts.

Gate 6 is the only one anchored to S3 rather than to the proxy's own records. Byte offsets and `content_length` come from adding up the recorded part sizes and are otherwise never compared with S3. A part set holding a subset of the parts would yield consistent metadata for a shorter object, and a later GET would serve the wrong length and the wrong bytes. Gates 1 to 5 make that unreachable, but they check the proxy against itself. The Complete response carries no object length, so the part-count suffix is the only size-related fact available without an extra HEAD on the client's path, and a missing part is exactly the truncation it catches.

Parts cached locally but not listed in the Complete request are deleted before the object's metadata is written.

## Concurrency

Publishing a part's bytes and writing its record form one critical section, guarded by an exclusive lock on `part{N}.lock`. The lock is per part because the rule it protects is per part: this part's bytes must match this part's recorded ETag. Two writers of the same part number serialize; writers of different part numbers never contend.

The part file is renamed inside the lock. Renaming it outside would let two concurrent `UploadPart` calls for the same part number leave upload A's bytes on disk with upload B's recorded ETag. That entry would pass the ETag check at completion and serve the wrong bytes.

At most four part finalizations run at once in each proxy, so slow finalizations (a degraded cache volume, an unusually large part) hold at most four blocking threads. Four is below the AWS CLI's default upload concurrency of 10.

The part lock is an OS file lock that works across processes and across hosts on NFS and EFS, so the rule holds for a fleet sharing one cache volume.

## Multi-instance deployments

Three deployment shapes matter for multipart uploads:

- **One proxy**: the same process handles every operation for an upload, and the per-part lock is the only coordination needed.
- **A fleet sharing one cache volume** (EFS or NFS): different proxies may handle different `UploadPart` calls for the same upload, and any proxy may handle the Complete. Because `mpus_in_progress/` is on the cache volume, each part's record is written by whichever proxy cached the part and read by whichever proxy completes the upload. Only writers of the same part number need to exclude each other, and `part{N}.lock` does that, with the usual caveats about NFS lock recovery. Complete polls the cache volume for the records it needs, so nothing depends on which proxy holds what in memory.
- **A fleet where each proxy has its own cache**: an upload is cached only when all of its operations reach one proxy. Consistent-hash affinity on the request path does that with no other coordination. All four operations carry the object in the path and none carries a `Range` header, so hashing on the path routes them together (see [Request-aware routing](REQUEST_AWARE_ROUTING.md)). Leave `hash-balance-factor` unset, since every part of an upload shares one routing key and bounded load would spill parts to other proxies. Without affinity, the proxy that handles the Complete waits in the background for the full 10 seconds for records that never arrive, declines at the missing-parts gate, and leaves the staged parts for the sweep. S3 still completes the upload and the client's Complete waits at most 250 ms; the cost is a lower hit rate. [Local NVMe cache](LOCAL_NVME_CACHE.md) covers this deployment.

In all three shapes the correctness gates keep the same rule: the cache holds exactly the bytes S3 holds for an object, or nothing.

## aws-chunked bodies

The AWS CLI and the SDKs wrap `UploadPart` bodies in aws-chunked encoding. The proxy forwards the chunked body to S3 unmodified, so the SigV4 signature over the original bytes stays valid, and decodes it separately for the cache. Single PUTs and multipart parts use the same decoder. It detects aws-chunked from the `content-encoding` and `x-amz-content-sha256` headers, never from the body bytes, and reads the expected decoded length from `x-amz-decoded-content-length` when present.

If decoding fails, or the decoded length differs from the header, the proxy skips caching that part. S3 still receives the original chunked body.

## Failure paths

| Failure | What the proxy does | What the client sees |
| --- | --- | --- |
| S3 rejects `UploadPart` | Does not cache the part, and logs the error | S3's error response |
| S3 rejects `CompleteMultipartUpload` | Does not finalize; the staged parts stay so a retry can succeed | S3's error response |
| Request body is malformed XML | Does not finalize, and leaves the staged parts for the sweep | S3's response, most likely an error |
| A requested part is still missing after the wait | Does not finalize, and leaves the staged parts for the sweep | S3's success response; later reads miss the cache |
| A part's ETag differs from its record | Does not finalize, and removes the upload directory | S3's success response |
| The part count disagrees with S3's ETag | Does not finalize. The replaced object is still removed; the upload directory is removed and no part reaches `ranges/` | S3's success response |
| A staging record for a part cannot be written | Does not finalize: no `.meta` is published and the renamed range files are removed. Counted in `write_cache.mpu_completion_ledger_declines_total` | S3's success response |
| A part record is unreadable | That part counts as not cached, which trips the missing-parts gate | S3's success response |
| aws-chunked decoding fails | Skips caching that part only; the upload continues | S3's success response |
| `AbortMultipartUpload` | Always removes the upload directory, whatever S3 answers | S3's response |
| The proxy restarts mid-upload | `mpus_in_progress/` persists, so later parts sent through the same cache volume resume normally. A proxy with its own cache starts without the earlier parts and does not cache the object | No change |

### Why the missing-parts path does not clean up

Deleting the upload directory when parts appear to be missing is unsafe. The parts declared missing may still be mid-write, on this proxy or another. Removing the directory pulls its lock files and any `.tmp` file out from under those writers, which then fail with `ENOENT` on rename or `ESTALE` on the lock. Those errors name missing files and stale handles, so they read like a fault on the cache volume when there is none.

The paths that do clean up are the ones where the staged state itself is unusable rather than incomplete. They are an upload that fails to load, an empty part set, a recorded part whose `.bin` is gone, an ETag mismatch, an `AbortMultipartUpload`, and a successful finalization.

### What reaps abandoned staging

The incomplete-upload sweep removes any upload directory idle for longer than `cache.incomplete_upload_ttl` (default 1 day, valid range 1 hour to 7 days). Idle time runs from the upload's last activity: the newest of the directory's modification time, `upload.meta`'s, and every `part{N}.json` record's. An upload that keeps receiving parts is never reaped, however long it runs. Abandoned uploads count against the in-progress multipart limit until the sweep removes them, so many abandoned uploads leave room for fewer new ones.

The sweep runs at startup and then on every proxy as a background pass every `min(incomplete_upload_ttl / 4, 1h)`. With the default TTL, abandoned parts are reclaimed at most an hour after their TTL passes. The sweep does not depend on the daily validation scan.

An upload being completed is never reaped. `CompleteMultipartUpload` holds the upload's `upload.lock` while it checks and moves the parts. The sweep skips any upload whose lock is held or cannot be taken, checks the idle time again under the lock, and renames the directory aside before removing it. If a part file disappears during finalization anyway, the proxy declines to cache the object, removes any part it had already moved, and publishes no metadata. S3 still completes the upload. Staged parts are not counted in the cache size until Complete, so reaping an upload does not change the cache size figure.

## Compression and integrity

Each `part{N}.bin` goes through the standard compression path, which decides per part whether to compress. Parts of compressible content types are LZ4-compressed, and parts of incompressible content are stored in an uncompressed LZ4 frame. Either way the frame carries an xxHash32 content checksum, so a flipped bit on disk produces a decode error, which a read handles as a cache miss. See [Integrity: every write is a checksummed LZ4 frame](COMPRESSION.md#integrity-every-write-is-a-checksummed-lz4-frame).

Each part's compression algorithm is recorded in its `part{N}.json` and carried into the final range metadata, so a read uses the right decoder.

## Threat model

[Trust and integrity model](ARCHITECTURE.md#trust-and-integrity-model) gives the full framing. For multipart uploads:

- **Upload ID path traversal (mitigated)**: the `uploadId` query parameter comes from the client. A path built from it could escape the cache through path separators (`/`, `\`) or traversal sequences (`..`), letting the proxy create or delete directories elsewhere. The upload ID is never a path component: the upload directory is named by its BLAKE3 hex digest, which contains only `[0-9a-f]`, so no upload ID can name anything outside `mpus_in_progress/`. The proxy also refuses to cache an upload ID that is empty or contains anything other than printable ASCII, which keeps control characters out of the logs. Such a request is forwarded to S3 unmodified with no local cache work.
- **In scope**: every correctness gate above, enforced unconditionally; LZ4 frame integrity on disk; concurrent writes of the same part number of one upload, serialized by `part{N}.lock`.
- **Cache behavior, outside the security boundary**: a client with valid S3 credentials uploading parts directly to S3. The proxy's record of that upload is incomplete and fails the missing-parts gate, so nothing is cached and there is no risk of corruption.
- **Residual gap**: an attacker who intercepts a client's multipart upload and injects a part directly into S3, while producing an MD5 collision that matches the ETag the client will send in `CompleteMultipartUpload`. On SSE-S3, where a part's ETag is its MD5, that collision is feasible. The mitigations sit with the bucket and the client, not the proxy:
  - Specify `--checksum-algorithm SHA256` at `CreateMultipartUpload`. Per-part SHA-256 checksums then travel in the Complete request body and S3 verifies them end to end.
  - Use SSE-KMS, whose ETags are opaque and cannot be forged by changing the content.

## Limits

These bounds are fixed in the proxy, with no configuration field:

- 5,000 in-progress multipart entries per cache volume, counting one per tracked upload and one per recorded part.
- 500 ms for the disk-safety check and the `upload.meta` write on `CreateMultipartUpload`.
- 250 ms for the bucket's origin marker or the first cache-size read on a Create.
- 250 ms of cache finalization added to a `CompleteMultipartUpload` response.
- 10 seconds, polled every 100 ms, for the part records a Complete names.
- Four part finalizations at once per proxy.

`cache.incomplete_upload_ttl` (default 1 day, 1 hour to 7 days) bounds how long abandoned parts stay on the cache volume.

## Observe it

- `signed_put.skipped_puts_total`, by reason: `backlog_full`, `multipart_tracking_full`, `multipart_tracking_unknown`, `write_cache_disabled`, `origin_marker_unresolved`, `capacity_unknown` and `disk_safety`. See [`signed_put`](METRICS_REFERENCE.md#signed_put).
- `write_cache.mpu_completion_ledger_declines_total`: completions not cached because a staging record could not be written. See [`write_cache`](METRICS_REFERENCE.md#write_cache).
- `cache.in_progress_part_bytes_drift_bytes`: how far the published in-progress part bytes stood from the last walk. See [`cache`](METRICS_REFERENCE.md#cache).
- The application log records each S3 error on Complete as `CompleteMultipartUpload S3 error: bucket=..., key=..., status=...`.

## Common gotchas

- The completed object's `Content-Type` and other representation headers come from `upload.meta`, captured at `CreateMultipartUpload`, not from the `CompleteMultipartUpload` response. The Complete response's `Content-Type: application/xml` describes its XML body, not the object, and an upload without the record is not cached.
- The `Content-Length` of the Complete response is the size of the XML response, not of the object, and is removed from the cached response headers.
- An HTTP 200 on `CompleteMultipartUpload` can still carry an error in its XML body. S3 sends whitespace to keep the connection alive during a long assembly, then writes the final status into the body. The proxy reads the ETag from the body and caches nothing when that fails.
- A part file's size on disk is its compressed size, while the `size` in `part{N}.json` is the original size. Byte offsets at completion use the recorded `size`.
- Part numbers start at 1, in the S3 API and in the part records.

## See also

- [Caching](CACHING.md): the write cache and what is cached
- [Part number cache](CACHE_READ_PATHS.md#part-number-cache): serving `?partNumber=N` reads from the cache
- [Eviction](EVICTION.md): how staged objects are reclaimed
- [Running a fleet on one cache volume](SHARED_STORAGE.md): mount and lock requirements
