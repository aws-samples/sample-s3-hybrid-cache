# Multipart Upload Caching

Scope: this document covers the proxy's **write-through cache for S3 multipart uploads** — `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`. It does not cover multipart *reads* (`GetObject?partNumber=N`), which are a separate concern on the read path in `http_proxy.rs`.

Related docs: [CACHING.md](CACHING.md) covers the write-through cache policy at a higher level. [ARCHITECTURE.md#trust-and-integrity-model](ARCHITECTURE.md#trust-and-integrity-model) frames what the cache does and does not verify. The compressor's integrity guarantees are in [COMPRESSION.md](COMPRESSION.md#integrity-lz4-frame-content-checksums).

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
├── upload.meta       # MultipartUploadTracker serialized as JSON
├── upload.lock       # fs2 exclusive lock file (coordinates updates)
├── part1.bin         # LZ4-framed part bytes (compressed or wrapped)
├── part2.bin
└── ...
```

`upload.meta` is the `cache_types::MultipartUploadTracker` serialized as JSON:

```json
{
  "upload_id": "...",
  "cache_key": "bucket/key",
  "started_at": { ... },
  "parts": [
    {
      "part_number": 1,
      "size": 5242880,
      "etag": "\"abc...\"",
      "compression_algorithm": "Lz4"
    }
  ],
  "total_size": 5242880,
  "content_type": "image/jpeg"
}
```

On successful `CompleteMultipartUpload`, each `part{N}.bin` is renamed from the upload directory into the sharded ranges tree at `ranges/{bucket}/{XX}/{YYY}/{key}_{start}-{end}.bin`, where `start` and `end` are the part's byte offsets in the final object. The `mpus_in_progress/{uploadId}/` directory is then removed. The range files become first-class cache entries indistinguishable (on the read path) from any other cached range.

## State machine

```
                     CreateMultipartUpload
                            │
                            ▼
                   ┌────────────────────┐
                   │  upload.meta       │
                   │  created with      │
                   │  parts=[]          │
                   └────────┬───────────┘
                            │
             UploadPart ────┤───── UploadPart
                            │      (any order, any count)
                            ▼
                   ┌────────────────────┐
                   │  upload.meta       │
                   │  parts=[1, 2, …]   │
                   │  partN.bin on disk │
                   └────┬───────────┬───┘
                        │           │
                        │           │
        CompleteMultipart│      Abort│MultipartUpload
        Upload           │           │
                        ▼           ▼
               ┌──────────────┐  ┌───────────────┐
               │ Rename parts │  │ Delete entire │
               │ into ranges/ │  │ mpus_in_progress/{uploadId}/ │
               │ Write object │  └───────────────┘
               │ metadata     │
               │ Clean up dir │
               └──────────────┘
```

The happy path on `UploadPart` is idempotent: re-uploading the same part number replaces the tracker entry (new size, new ETag) and overwrites `part{N}.bin`. This matches S3's own behaviour — a new `UploadPart` with the same part number overwrites the previous one.

## Correctness gates

The cache retains a completed multipart object only when **all** of the following hold. If any fails, the upload directory is cleaned up and no object is cached — S3 still returns its success response to the client; the proxy just loses a cache hit.

1. **S3 returned 2xx for the `CompleteMultipartUpload`**. S3 is the source of truth; if S3 rejected the Complete (missing parts, bad ETag, etc.), the cache is meaningless and cleanup runs.
2. **The `CompleteMultipartUpload` request body parses as a valid XML `<CompleteMultipartUpload>` document**. Malformed or empty bodies skip finalization. See `parse_complete_mpu_request`.
3. **Every part listed in the request body is present locally** — both a tracker entry and the on-disk `part{N}.bin` file. If a requested part is missing (because it went to a different proxy instance, was uploaded directly to S3 bypassing the proxy, or the proxy restarted mid-upload), `cleanup_incomplete_multipart_cache` runs.
4. **ETag equality between the request and the tracker**, normalized by stripping surrounding quotes. For each part the request lists, `normalize_etag(request_etag) == normalize_etag(tracker_etag)` must hold. Any mismatch skips cache finalization.

**Unreferenced parts** (cached locally but not listed in the Complete request) are deleted from disk before the object metadata is written. See `unreferenced_parts` in `finalize_multipart_upload`.

## Concurrency

`cache_upload_part` holds an exclusive file lock on `upload.lock` (via `fs2::FileExt::lock_exclusive`) across **both** the part file rename and the tracker read-modify-write. This ordering matters:

- **Before version 1.11.0**, the part file was renamed *outside* the lock. Two concurrent UploadPart calls for the same part number could interleave such that the bytes on disk came from upload A while the tracker's recorded ETag was from upload B — a cache entry that passed the finalize ETag check but served the wrong bytes. The race was reproducible across two proxy processes sharing an EFS volume.
- **Since 1.11.0**, the file rename and the tracker update are one critical section. Test `test_cache_upload_part_concurrent_same_part_keeps_file_and_tracker_consistent` drives the race and verifies the invariant.

The `fs2` file lock works across processes and across hosts on shared NFS/EFS, so the invariant holds for multi-proxy fleets on a shared cache volume.

## Multi-instance deployments

The proxy is designed to scale horizontally. For multipart uploads, there are three relevant deployment shapes:

**Single proxy, single upload**: Simplest. The same process handles every operation for a given `uploadId`. Tracker is exclusive to this process. No coordination needed beyond the in-process file lock.

**Multi-proxy, shared cache volume** (EFS/NFS): Different proxy instances may handle different `UploadPart` calls for the same `uploadId`, and any instance may handle the `Complete`. Because `mpus_in_progress/{uploadId}/` is on shared storage and `upload.lock` is an OS file lock that NFS respects (with the usual caveats about NFS lock recovery), all instances coordinate on the same tracker. No additional coordination layer needed.

**Multi-proxy, independent cache volumes**: Each proxy has its own local `mpus_in_progress/` directory. If parts land on different proxies, the instance that handles `Complete` will find parts missing from its local tracker, hit correctness gate #3, and skip cache finalization. S3 still completes the upload; the proxy just doesn't retain the object. This is a degraded cache hit rate, not a correctness problem.

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
| Request body malformed XML | Skip finalize, clean up upload dir. | S3's response (whatever it was — likely an error). |
| Requested part missing locally | Skip finalize, clean up upload dir. | S3's success response (but no cache hit for future reads). |
| Part ETag mismatch vs. tracker | Skip finalize, clean up upload dir. | S3's success response. |
| aws-chunked decode fails | Skip caching that part only; bypass metric. Upload continues. | S3's success response. |
| `AbortMultipartUpload` | Always clean up `mpus_in_progress/{uploadId}/` regardless of S3's response code. | S3's response. |
| Proxy restarts mid-upload | `mpus_in_progress/` persists. Subsequent UploadParts on the same instance resume correctly. Another proxy instance without shared storage starts fresh (degraded cache). | Transparent (no client-visible change). |

## Compression and integrity

Each `part{N}.bin` goes through the standard `CompressionHandler::compress_content_aware_with_metadata` path. Parts of compressible content types are LZ4-compressed; parts of incompressible content are wrapped in an uncompressed LZ4 frame. Either way, the frame carries an xxhash32 content checksum — disk bit-flips produce decode errors, handled as cache misses on read. See [COMPRESSION.md](COMPRESSION.md#integrity-lz4-frame-content-checksums).

Each part's `compression_algorithm` is recorded in the tracker and carried through into the final range metadata, so the correct decoder is used at read time.

## Threat model

See [ARCHITECTURE.md#trust-and-integrity-model](ARCHITECTURE.md#trust-and-integrity-model) for the full framing. Specific to multipart:

- **uploadId path traversal (mitigated)**: The `uploadId` query parameter arrives from the client and is used to construct the `mpus_in_progress/{uploadId}/` directory path. A malicious `uploadId` containing path separators (`/`, `\`) or traversal sequences (`..`) could escape the cache directory, potentially allowing arbitrary directory creation (`create_dir_all`) or deletion (`remove_dir_all`). **Mitigation**: All three multipart handlers (`handle_upload_part`, `handle_complete_multipart_upload`, `handle_abort_multipart_upload`) validate the `uploadId` with `is_safe_path_component` at entry, before any filesystem path construction. On reject, the request is forwarded to S3 unmodified (preserving the SigV4 response) and all local cache work is skipped. The validation rejects empty strings, path separators, `..` substrings, NUL bytes, and control characters.
- **In scope**: All four correctness gates above, enforced unconditionally. LZ4 frame integrity on disk. Concurrent-same-part-number writes on the same `uploadId`.
- **Not a security boundary, just cache behaviour**: A client who holds valid S3 credentials uploading parts *directly* to S3 bypassing the proxy. The proxy's cache for that upload will be incomplete and will fail gate #3. Cleanup runs, the client gets S3's response, there is no corruption risk.
- **Residual gap**: A sophisticated attacker who intercepts a client's multipart upload and injects their own part via direct-to-S3 UploadPart, while also producing an MD5 collision to match the ETag the original client will send in `CompleteMultipartUpload`. On SSE-S3 single-part ETags (= MD5) this is mathematically feasible. Mitigations are at the bucket/client layer, not the proxy:
  - Specify `--checksum-algorithm SHA256` at `CreateMultipartUpload` time. Per-part SHA256 checksums then flow through the Complete request body and S3 verifies end-to-end.
  - Use SSE-KMS; ETags become opaque and non-forgeable via content manipulation.

## Tests to consult

Integration-level invariants:
- `test_cache_upload_part` — baseline single-part path.
- `test_cache_multiple_upload_parts` — tracker accumulates correctly.
- `test_finalize_multipart_upload_etag_mismatch` — ETag gate rejects.
- `test_finalize_multipart_upload_with_missing_parts` / `_missing_directory` — bail-and-cleanup paths.
- `test_finalize_multipart_upload_deletes_unreferenced_parts` — unreferenced cleanup.
- `test_cache_upload_part_concurrent_same_part_keeps_file_and_tracker_consistent` — concurrency invariant (added in 1.11.0).

Property-based tests (`quickcheck`):
- `prop_multipart_part_storage` — round-trip.
- `prop_multipart_completion_creates_linked_metadata` — finalize produces consistent metadata.
- `prop_abort_upload_cleanup` — abort always cleans up.
- `prop_part_filtering_preserves_only_requested_parts` — unreferenced filtering.
- `prop_part_ranges_build_correctly_from_sizes` — byte-offset arithmetic.
- `prop_etag_validation_rejects_mismatches` — ETag gate correctness.

## Common gotchas

- **Content-Type** for the completed object comes from the tracker (captured at `CreateMultipartUpload` time), not from the `CompleteMultipartUpload` response. The Complete response carries `Content-Type: application/xml` which is the XML body's type, not the object's.
- **Content-Length** in the Complete response is the XML response size, not the object size. It's filtered out of cached response headers.
- **An HTTP 200 on `CompleteMultipartUpload` can still contain an embedded error in the XML body.** S3 sends whitespace keep-alives during long assemblies, then writes the final status into the body. `extract_etag_from_xml` handles this; on failure the cache is not written.
- **Part file size on disk** is the *compressed* size; the tracker's `size` field is the *original uncompressed* size. Don't conflate them when computing byte offsets at finalize — use the tracker's `size`.
- **Part numbers are 1-based** in the S3 API and in the tracker.
