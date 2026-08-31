# Multipart Upload Caching

Scope: this document covers the proxy's **write-through cache for S3 multipart uploads** — `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`. It does not cover multipart *reads* (`GetObject?partNumber=N`), which are a separate concern on the read path in `http_proxy.rs`.

Related docs: [CACHING.md](CACHING.md) covers the write-through cache policy at a higher level. [ARCHITECTURE.md#trust-and-integrity-model](ARCHITECTURE.md#trust-and-integrity-model) frames what the cache does and does not verify. The compressor's integrity guarantees are in [COMPRESSION.md](COMPRESSION.md#integrity-every-write-is-a-checksummed-lz4-frame).

Primary source file: `src/signed_put_handler.rs`.

## Table of Contents

- [Why multipart caching matters](#why-multipart-caching-matters)
- [The four operations](#the-four-operations)
- [On-disk layout](#on-disk-layout)
- [State machine](#state-machine)
- [Correctness gates](#correctness-gates)
- [Concurrency](#concurrency)
- [Multi-instance deployments](#multi-instance-deployments)
- [aws-chunked bodies](#aws-chunked-bodies)
- [Failure paths](#failure-paths)
- [Compression and integrity](#compression-and-integrity)
- [Threat model](#threat-model)
- [Tests to consult](#tests-to-consult)
- [Common gotchas](#common-gotchas)

## Why multipart caching matters

S3 multipart uploads let clients split large objects into parts uploaded in parallel, then assemble them into a single object. Without caching, every subsequent read of that object has to be fetched from S3 over the network. The write-through cache assembles the proxy-local copy as parts arrive, so the object is cache-hot from the moment `CompleteMultipartUpload` succeeds.

## The four operations

All four multipart operations carry AWS SigV4 signatures. The proxy holds no credentials and cannot sign, so every request is forwarded to S3 unmodified. The cache work runs *in addition to* forwarding, never *instead of* it.

| Operation | HTTP | Query | Handler |
| --- | --- | --- | --- |
| `CreateMultipartUpload` | `POST` | `?uploads` | `handle_create_multipart_upload` |
| `UploadPart` | `PUT` | `?uploadId=X&partNumber=N` | `handle_upload_part` |
| `CompleteMultipartUpload` | `POST` | `?uploadId=X` (no partNumber) | `handle_complete_multipart_upload` |
| `AbortMultipartUpload` | `DELETE` | `?uploadId=X` | `handle_abort_multipart_upload` |

Routing lives in `handle_signed_put`; detection helpers are `is_create_multipart_upload`, `parse_upload_part_query`, `is_complete_multipart_upload`, `is_abort_multipart_upload`.

## On-disk layout

While an upload is in progress, all its state lives under `{cache_dir}/mpus_in_progress/{uploadId}/`:

```
mpus_in_progress/{uploadId}/
├── upload.meta       # Upload-level facts, written once at CreateMultipartUpload
├── upload.lock       # fs2 exclusive lock file (held by finalize only)
├── part1.bin         # LZ4-framed part bytes (compressed or wrapped)
├── part1.json        # Part 1's CachedPartInfo: size, ETag, compression algorithm
├── part1.lock        # fs2 exclusive lock file, scoped to part 1
├── part2.bin
├── part2.json
├── part2.lock
└── ...
```

**A part owns its own files.** There is no shared document that each `UploadPart` appends to. `part{N}.json` is written once, atomically (`.tmp` + fsync + rename), by whichever instance cached part N — see `record_part_blocking`. Per-part cost is therefore independent of how many parts precede it, and two parts never contend.

`upload.meta` carries only upload-level facts:

```json
{
  "upload_id": "...",
  "cache_key": "bucket/key",
  "started_at": { ... },
  "content_type": "image/jpeg"
}
```

`load_tracker_blocking` assembles a `cache_types::MultipartUploadTracker` at finalize time from `upload.meta` plus the per-part records on disk. Two consequences worth knowing:

- A `parts` array or `total_size` in an older-format `upload.meta` is **ignored** — the loader clears both and rebuilds from `part{N}.json`.
- A missing or unparseable `upload.meta` is deliberately non-fatal. Upload-level fields are synthesised from the request and the parts still come from disk, so an upload whose `CreateMultipartUpload` this fleet never saw can still cache. Only `content_type` is lost.

A part record that fails to parse is skipped with a warning rather than failing the load. That part then counts as not cached, and the missing-parts gate declines to finalise — the same outcome as if it had never landed.

On successful `CompleteMultipartUpload`, each `part{N}.bin` is renamed from the upload directory into the sharded ranges tree at `ranges/{bucket}/{XX}/{YYY}/{key}_{start}-{end}.bin`, where `start` and `end` are the part's byte offsets in the final object. The `mpus_in_progress/{uploadId}/` directory is then removed. The range files become first-class cache entries indistinguishable (on the read path) from any other cached range.

## State machine

```
                     CreateMultipartUpload
                            │
                            ▼
                   ┌────────────────────┐
                   │  upload.meta       │
                   │  created (upload-  │
                   │  level facts only) │
                   └────────┬───────────┘
                            │
             UploadPart ────┤───── UploadPart
                            │      (any order, any count)
                            ▼
                   ┌────────────────────┐
                   │  partN.bin  +      │
                   │  partN.json,       │
                   │  one pair per part │
                   └────┬───────────┬───┘
                        │           │
                        │           │
        CompleteMultipart│      Abort│MultipartUpload
        Upload           │           │
                        ▼           ▼
      ┌────────────────────────────┐  ┌───────────────┐
      │ Wait for the named parts'  │  │ Delete entire │
      │ records to land (≤10s)     │  │ mpus_in_progress/{uploadId}/ │
      │ Rename parts into ranges/  │  └───────────────┘
      │ Write object metadata      │
      │ Clean up dir               │
      └────────────────────────────┘
```

The happy path on `UploadPart` is idempotent: re-uploading the same part number rewrites `part{N}.json` (new size, new ETag) and overwrites `part{N}.bin`. This matches S3's own behaviour — a new `UploadPart` with the same part number overwrites the previous one.

### Complete waits for the parts it names

`CompleteMultipartUpload` does not evaluate the cache immediately. It first polls for the `part{N}.json` records naming the parts **its own request body lists**, for up to `MULTIPART_COMPLETE_CACHE_WAIT` (10 seconds, a fixed internal constant) at a 100 ms interval. See `await_tracker_parts`.

This exists because the per-part cache task is fire-and-forget and deliberately lags the client's response: the forward path answers the client, and only then does the cache task drain the tee, await S3's result and finalise. Clients send Complete as soon as the last part is acknowledged — 147 ms after the first part was recorded, in the measured case — so without the wait, Complete reads a set of records covering a fraction of the parts and declines to cache. The wait is the normal path on a working cache, not an exception.

Two properties follow:

- **Complete latency now includes local cache work.** It previously returned as soon as S3 did. The cost is bounded by the constant above, is only paid on multipart uploads, and buys the object being cached at all. Uploads with very large part counts sit at the slower end of that range.
- **Exceeding the bound is not an error.** S3 has already accepted the upload. The wait returns, the missing-parts gate reports it, and the object simply is not cached.

The poll reads records **without** taking `upload.lock` — taking the exclusive lock every 100 ms would contend with the very part tasks being waited for. The authoritative read happens under the lock afterwards. Polling shared-volume state, rather than tracking spawned tasks in process memory, is what makes this correct on a fleet: parts of one upload may be served by several proxies and Complete by a fourth.

## Correctness gates

The cache retains a completed multipart object only when **all** of the following hold. If any fails, no object is cached — S3 still returns its success response to the client; the proxy just loses a cache hit. Most failures also clean up the upload directory; the exceptions are listed in [Failure paths](#failure-paths).

1. **S3 returned 2xx for the `CompleteMultipartUpload`**. S3 is the source of truth; if S3 rejected the Complete (missing parts, bad ETag, etc.), there is nothing to cache. Staging is left in place so a client retry can still succeed.
2. **The `CompleteMultipartUpload` request body parses as a valid XML `<CompleteMultipartUpload>` document**. Malformed or empty bodies skip finalization. See `parse_complete_mpu_request`. There is no safe way to finalise without the requested part list: it is the only statement of what the completed object contains, and it is also the set the wait above waits for. Treating a parse failure as "use whatever is cached" made gates 4 and 5 vacuous and let a partially recorded upload finalise as a shorter object.
3. **The assembled part set is non-empty.**
4. **Every part listed in the request body is present locally** — both a `part{N}.json` record and the on-disk `part{N}.bin` file. A requested part can be missing because its cache task has not finished (which the 10-second wait exists for), because it went to a different proxy instance, because it was uploaded directly to S3 bypassing the proxy, or because the proxy restarted mid-upload.
5. **ETag equality between the request and the part records**, normalized by stripping surrounding quotes. For each part the request lists, `normalize_etag(request_etag) == normalize_etag(recorded_etag)` must hold. Any mismatch skips cache finalization.
6. **The assembled part count agrees with S3's own.** A multipart ETag has the form `"<md5-of-md5s>-<part-count>"`, and that suffix is compared against the number of parts assembled. On disagreement, nothing is cached.

   This is the only gate anchored to S3 rather than to the proxy's own bookkeeping. Byte offsets and `content_length` are derived purely by summing recorded part sizes and are otherwise never compared with S3, so a part set holding a subset yields self-consistent metadata describing a **shorter** object, and a later GET then serves the wrong length and the wrong bytes. The gates above make that unreachable, but they check the proxy against itself. The Complete response carries no object length, so the ETag's part-count suffix is the only size-related fact available without an extra HEAD on a client-visible path — and a missing part is exactly the truncation signature it catches.

**Unreferenced parts** (cached locally but not listed in the Complete request) are deleted from disk before the object metadata is written. See `unreferenced_parts` in `finalize_multipart_upload`.

## Concurrency

Publishing a part's bytes and writing its record are one critical section, guarded by an exclusive `fs2` lock on `part{N}.lock`. In production this is `finalize_and_record_cached_part`, reached from the streaming part sink (`open_multipart_part_sink` + `MultipartPartSink::finalize`); `cache_upload_part` is a `#[cfg(test)]` buffered equivalent retained for the multipart test suite and never ships in the release binary.

**The lock is per part, not per upload.** The invariant is per part — this part's bytes must agree with this part's recorded ETag — so nothing here ever needed to exclude a different part number. Two writers racing the same part number still serialise; two writers on different part numbers do not contend at all.

Why the scope matters: a single per-upload `upload.lock` around a whole-tracker read-modify-write serialised every part of every concurrent upload on one cross-instance lock on a network filesystem, at O(n²) bytes over an upload. Each waiter occupied a `spawn_blocking` thread for up to its 30-second timeout, and the forward path needs that same pool. Measured at 2,000 parts on a three-proxy fleet: 1,214 lock timeouts, 1,220 part-record failures, and the client's upload failed with `Connection reset by peer`. At ten concurrent parts, the same queueing left records unlanded past the Complete wait, so the object was not cached at all.

The ordering invariant itself is older and still holds. Before 1.11.0 the part file was renamed *outside* the lock, so two concurrent `UploadPart` calls for the same part number could leave the bytes on disk from upload A and the recorded ETag from upload B — an entry that passed the finalize ETag check but served the wrong bytes, reproducible across two proxy processes sharing an EFS volume. `test_cache_upload_part_concurrent_same_part_keeps_file_and_tracker_consistent` drives the race.

A process-wide semaphore (`PART_FINALIZE_SLOTS`, 4) caps how many finalizations may occupy a `spawn_blocking` thread at once. Since per-part locking removed the contention it was introduced for, it is a backstop against any one finalization being slow — a degraded shared volume, an unusually large part — rather than a fix for a live defect. Four sits below the AWS CLI's default upload concurrency of 10.

The `fs2` file lock works across processes and across hosts on shared NFS/EFS, so the invariant holds for multi-proxy fleets on a shared cache volume.

## Multi-instance deployments

The proxy is designed to scale horizontally. For multipart uploads, there are three relevant deployment shapes:

**Single proxy, single upload**: Simplest. The same process handles every operation for a given `uploadId`. Staging is exclusive to this process. No coordination needed beyond the per-part file lock.

**Multi-proxy, shared cache volume** (EFS/NFS): Different proxy instances may handle different `UploadPart` calls for the same `uploadId`, and any instance may handle the `Complete`. Because `mpus_in_progress/{uploadId}/` is on shared storage, each part's record is written by whichever instance cached it and read by whichever instance completes. Cross-instance exclusion is needed only between writers of the *same* part number, which `part{N}.lock` provides — an OS file lock that NFS respects, with the usual caveats about NFS lock recovery. Complete's correctness comes from polling the shared volume for the records it needs, so nothing depends on which instance holds what in memory. No additional coordination layer needed.

**Multi-proxy, independent cache volumes**: Each proxy has its own local `mpus_in_progress/` directory, so an upload is cached only when all of its operations reach one instance. Consistent-hash affinity achieves that with no extra coordination: all four operations carry the object in the request path and none carries a `Range` header, so hashing on the path routes them together (see [Request-Aware Routing](REQUEST_AWARE_ROUTING.md)). Leave `hash-balance-factor` out, since every part of an upload shares one routing key and bounded load would spill parts to other instances. Without affinity, the instance that handles `Complete` waits the full 10 seconds for records that will never appear, declines at the missing-parts gate, and leaves staging for the TTL sweep. S3 still completes the upload; the proxy just doesn't retain the object. This is a degraded cache hit rate and a slower Complete, not a correctness problem. [LOCAL_NVME_CACHE.md](LOCAL_NVME_CACHE.md) covers this deployment in full.

In all three shapes the correctness gates guarantee the same invariant: **the cache either holds the exact bytes S3 holds, or holds nothing for that object.**

## aws-chunked bodies

AWS CLI and SDKs wrap `UploadPart` bodies in aws-chunked transfer encoding. The proxy forwards the chunked body **unmodified** to S3 (so the SigV4 signature over the original bytes stays valid) and **separately decodes** for caching.

Both the multipart path (`handle_upload_part`) and the non-multipart PUT path (`handle_with_caching`) use the shared `crate::aws_chunked_decoder` module:

- `is_aws_chunked(&headers)` — detects aws-chunked via `content-encoding` / `x-amz-content-sha256`. Does not sniff body bytes.
- `decode_aws_chunked(&bytes)` — returns decoded bytes or an error.
- `get_decoded_content_length(&headers)` — reads `x-amz-decoded-content-length` if present.

If the header says the decoded body should be N bytes and the decoder produces M ≠ N, the proxy skips caching that part and records `record_cache_bypass("aws_chunked_decode_error")`. S3 still gets the original chunked body; the cache just refuses to cache potentially-wrong bytes.

**Do not reinvent chunk parsing.** An earlier version of `handle_upload_part` had its own byte-sniffing stripper, which was replaced in 1.11.0 with a call into the shared decoder.

## Failure paths

| Failure | Proxy behaviour | Client sees |
| --- | --- | --- |
| S3 rejects `UploadPart` | Don't cache the part. Log error. | S3's error response. |
| S3 rejects `CompleteMultipartUpload` | Don't finalize cache. `mpus_in_progress/` untouched so a retry can still succeed. | S3's error response. |
| Request body malformed XML | Skip finalize. No cleanup — staging is left for the TTL sweep. | S3's response (whatever it was — likely an error). |
| Requested part still missing after the wait | Skip finalize. **No cleanup** — staging is left for the TTL sweep. | S3's success response (but no cache hit for future reads). |
| Part ETag mismatch vs. record | Skip finalize, clean up upload dir. | S3's success response. |
| Assembled part count disagrees with S3's ETag | Skip finalize. No cleanup — staging is left for the TTL sweep. | S3's success response. |
| A part record is unreadable | That part counts as not cached, which then trips the missing-parts gate. | S3's success response. |
| aws-chunked decode fails | Skip caching that part only; bypass metric. Upload continues. | S3's success response. |
| `AbortMultipartUpload` | Always clean up `mpus_in_progress/{uploadId}/` regardless of S3's response code. | S3's response. |
| Proxy restarts mid-upload | `mpus_in_progress/` persists. Subsequent UploadParts on the same instance resume correctly. Another proxy instance without shared storage starts fresh (degraded cache). | Transparent (no client-visible change). |

### Why the missing-parts path does not clean up

Deleting the staging directory when parts appear to be missing is unsafe, and was the source of a misleading error cascade. The parts being declared missing may still be mid-write, on this instance or another; `remove_dir_all` then pulls the directory — including the lock files and any `.tmp` — out from under those writers, which fail `ENOENT` on rename or `ESTALE` on the lock. Those errors name missing files and stale handles, so they read like a fault on the cache volume and are not.

The paths that still clean up are the ones where the staging state itself is unusable rather than merely incomplete: a tracker that fails to load, an empty part set, a recorded part whose `.bin` is gone, an ETag mismatch, an `AbortMultipartUpload`, and a successful finalize.

### What reaps abandoned staging

`cleanup_incomplete_uploads_on_startup` removes any `mpus_in_progress/{uploadId}/` older than `cache.incomplete_upload_ttl` (default 1 day, valid range 1 hour to 7 days), judged by the mtime of `upload.meta` or, if absent, of the directory itself.

**It runs at startup only** — there is no periodic timer driving it. So staging left behind by a declined finalize is reclaimed at the next proxy restart, not within the TTL. On a long-running instance with many declined completions, that space stays occupied until then. Sizing note: staging holds the part bytes, so the worst case is bounded by the object sizes of uploads that failed to finalise since the last restart.

## Compression and integrity

Each `part{N}.bin` goes through the standard compression path, with the
compress-or-store decision made per part by `CacheManager::effective_compression`. Parts of compressible content types are LZ4-compressed; parts of incompressible content are wrapped in an uncompressed LZ4 frame. Either way, the frame carries an xxhash32 content checksum — disk bit-flips produce decode errors, handled as cache misses on read. See [COMPRESSION.md](COMPRESSION.md#integrity-every-write-is-a-checksummed-lz4-frame).

Each part's `compression_algorithm` is recorded in its `part{N}.json` and carried through into the final range metadata, so the correct decoder is used at read time.

## Threat model

See [ARCHITECTURE.md#trust-and-integrity-model](ARCHITECTURE.md#trust-and-integrity-model) for the full framing. Specific to multipart:

- **uploadId path traversal (mitigated)**: The `uploadId` query parameter arrives from the client and is used to construct the `mpus_in_progress/{uploadId}/` directory path. A malicious `uploadId` containing path separators (`/`, `\`) or traversal sequences (`..`) could escape the cache directory, potentially allowing arbitrary directory creation (`create_dir_all`) or deletion (`remove_dir_all`). **Mitigation**: All three multipart handlers (`handle_upload_part`, `handle_complete_multipart_upload`, `handle_abort_multipart_upload`) validate the `uploadId` with `is_safe_path_component` at entry, before any filesystem path construction. On reject, the request is forwarded to S3 unmodified (preserving the SigV4 response) and all local cache work is skipped. The validation rejects empty strings, path separators, `..` substrings, NUL bytes, and control characters.
- **In scope**: All the correctness gates above, enforced unconditionally. LZ4 frame integrity on disk. Concurrent-same-part-number writes on the same `uploadId` — serialised by `part{N}.lock`; writes to *different* part numbers are not serialised and do not need to be.
- **Not a security boundary, just cache behaviour**: A client who holds valid S3 credentials uploading parts *directly* to S3 bypassing the proxy. The proxy's cache for that upload will be incomplete and will fail the missing-parts gate. Nothing is cached, the client gets S3's response, there is no corruption risk.
- **Residual gap**: A sophisticated attacker who intercepts a client's multipart upload and injects their own part via direct-to-S3 UploadPart, while also producing an MD5 collision to match the ETag the original client will send in `CompleteMultipartUpload`. On SSE-S3 single-part ETags (= MD5) this is mathematically feasible. Mitigations are at the bucket/client layer, not the proxy:
  - Specify `--checksum-algorithm SHA256` at `CreateMultipartUpload` time. Per-part SHA256 checksums then flow through the Complete request body and S3 verifies end-to-end.
  - Use SSE-KMS; ETags become opaque and non-forgeable via content manipulation.

## Tests to consult

Integration-level invariants:
- `test_cache_upload_part` — baseline single-part path.
- `test_cache_multiple_upload_parts` — part records accumulate correctly.
- `test_finalize_multipart_upload_etag_mismatch` — ETag gate rejects.
- `test_finalize_multipart_upload_with_missing_parts` / `_missing_directory` — bail paths.
- `test_finalize_multipart_upload_deletes_unreferenced_parts` — unreferenced cleanup.
- `test_cache_upload_part_concurrent_same_part_keeps_file_and_tracker_consistent` — concurrency invariant (added in 1.11.0, now at per-part scope).
- `complete_arriving_before_part_tasks_finish_still_caches_every_part` — the Complete wait.
- `tracker_holding_a_subset_does_not_finalize_a_short_object` — truncation gates.
- `unparseable_complete_body_does_not_finalize_the_cache` — gate 2.
- `test_finalize_multipart_upload_streamed_parts_cleanup_on_missing_part` — asserts staging **survives** the missing-parts path.

When reading these, note that a test cannot inspect `upload.meta` for part facts. Use the `tracker_from_disk` helper, which goes through the same loader production uses, so a test cannot pass against a layout the product does not read.

Property-based tests (`quickcheck`):
- `prop_multipart_part_storage` — round-trip.
- `prop_multipart_completion_creates_linked_metadata` — finalize produces consistent metadata.
- `prop_abort_upload_cleanup` — abort always cleans up.
- `prop_part_filtering_preserves_only_requested_parts` — unreferenced filtering.
- `prop_part_ranges_build_correctly_from_sizes` — byte-offset arithmetic.
- `prop_etag_validation_rejects_mismatches` — ETag gate correctness.

## Common gotchas

- **Content-Type** for the completed object comes from `upload.meta` (captured at `CreateMultipartUpload` time), not from the `CompleteMultipartUpload` response. The Complete response carries `Content-Type: application/xml` which is the XML body's type, not the object's.
- **Content-Length** in the Complete response is the XML response size, not the object size. It's filtered out of cached response headers.
- **An HTTP 200 on `CompleteMultipartUpload` can still contain an embedded error in the XML body.** S3 sends whitespace keep-alives during long assemblies, then writes the final status into the body. `extract_etag_from_xml` handles this; on failure the cache is not written.
- **Part file size on disk** is the *compressed* size; the `size` field in `part{N}.json` is the *original uncompressed* size. Don't conflate them when computing byte offsets at finalize — use the recorded `size`.
- **Part numbers are 1-based** in the S3 API and in the tracker.
