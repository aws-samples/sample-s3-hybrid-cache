# Error handling and recovery

This page describes what Hybrid Cache for Amazon S3 does when something on the cache path fails: a damaged or missing cache file, a full or slow cache volume, a stalled upstream response, or a request loop. It is for operators deciding what to alert on and what to do when a counter moves. Each section names the detection, what the proxy does, and the metrics and log lines that show it.

## Overview

Cache corruption, missing files and disk space errors do not stop the proxy. The proxy logs the error, records a metric where one exists, and serves the request from S3. Two cases reach the client:

- A stall mid-stream closes the client connection; see [Stalled upstream responses](#7-stalled-upstream-responses-mid-stream-idle-watchdog).
- A read streamed from the cache that finds a damaged range file ends early with a connection error; see [Damaged range files](#damaged-range-files).

Damaged state is cleared without operator action. Confidently corrupt metadata is overwritten by the next full-object fetch (see [Self-healing via overwrite-in-place](#self-healing-via-overwrite-in-place)), and range files that no metadata lists are deleted by the orphan reclamation rotation (see [Orphaned files](#6-orphaned-files)). A single damaged cache entry never stops the process.

## Failure classes

### 1. Corrupted metadata files

**Detection**: a `.meta` file fails to parse when it is read.

**What the proxy does**: it logs the error with the file path, treats the read as a cache miss and leaves the file in place, and counts it in `corruption_metadata_total`. The request is served from S3. A confidently corrupt file is then replaced as described below, with no manual step.

#### Self-healing via overwrite-in-place

When a `.meta` file is confidently corrupt, rather than caught mid-write, the proxy heals it. Three signals classify a file as confidently corrupt, checked in this order:

1. **Oversize**: the file is larger than `max_metadata_file_bytes` (default 4 MiB). It is rejected without reading the body. This check runs on the capped metadata reads; the lookup that serves a GET or HEAD on a RAM metadata-cache miss has no size check.
2. **Legacy**: the document uses the old layout, with an inline `body` and no `object_metadata`.
3. **Parse failure**: the file still fails to parse after bounded retries, and its size did not change during the retry window, so it is not being written.

A file that meets none of these stays on the "treat as a miss, do not delete" path. That keeps reads tolerant of a document another proxy is writing at that moment on the cache volume (NFS or EFS).

The heal happens on a later fetch:

- **GET**: the normal cache-miss handling fetches the object from S3 and writes a fresh `.meta` at the same sharded path, over the corrupt file. A full-object fill does this during the request. It publishes the range file first. It then holds the key's metadata lock while it reads whatever `.meta` is there, releases the ranges the new copy supersedes, and renames the new document into place. A document that cannot be parsed is not read, and the fresh one replaces it whole. A range fill instead records the range in this proxy's journal for consolidation to fold in, and folding reads the existing document, so a range fill does not replace an unparseable one. A corrupt `.meta` is therefore cleared by the next full-object fetch of that key, and until then every request for the key is served from S3 as a miss.
- **HEAD**: a HEAD response does not write a fresh `.meta`, since it caches only in the HEAD cache, and it does not remove the corrupt file either. The file stays until a GET's fill replaces it.

The overwrite is always backed by a real S3 fetch: the proxy never builds metadata from internal state alone.

| Metric | Description |
|--------|-------------|
| `corruption_metadata_total` | Corrupt metadata detections, all reasons |
| `metadata_heal_overwrite_total` | Unparseable `.meta` documents replaced by a fresh one, from a full-object fill, a zero-length fill or a range store. Counted once the replacement is renamed into place, per proxy, and exported over OTLP. A rising `corruption_metadata_total` with a flat heal count means the damaged keys are not being fetched as full objects, so they stay unhealed and keep going to S3 |

A heal cannot interleave with a consolidation. Every writer of a `.meta`, the fill paths and the consolidator alike, holds the key's metadata lock across the whole read-modify-write and releases it only after the rename. The lock is never taken from its holder. A fill retries a contended lock a bounded number of times, then abandons the write and leaves the corrupt file for a later request to heal. Consolidation skips a contended key and retries on its next cycle. A file caught mid-write is never healed; only confident corruption triggers the overwrite.

### 2. Missing range binary files

**Detection**: a range file that the metadata lists is not found when the proxy loads it.

**What the proxy does**: it logs the error with the range's start, end and cache key, treats the range as a cache miss, fetches it from S3 and caches it again. It removes the missing range from the metadata and counts it in `corruption_missing_range_total`. Other ranges of the same object keep serving, and the client sees no error.

Common causes are manual deletion of cache files, disk corruption or I/O errors, an unclean shutdown during a write, and file system faults. Leave the cache files alone, and watch disk health and I/O errors.

#### Damaged range files

The proxy handles three kinds of range-file damage as a miss:

- a frame that fails its checksum;
- a file whose size or modification time differs from what was recorded when the range was cached;
- a file that cannot be checked when it is opened.

The read is answered from S3, and the proxy removes the range so the next read caches it again. A read already streaming from the cache cannot switch to S3 once its response has started. It ends early with a connection error, and the bytes it sent before the check failed can be wrong. Until the range is removed, later reads of it on that proxy are fetched from S3. The counters are `range_file_corruption_detected` and `range_file_identity_mismatch_detected`; see [`atomic_metadata`](METRICS_REFERENCE.md#atomic_metadata) and [When the proxy can return data that differs from S3](CACHE_FRESHNESS.md#when-the-proxy-can-return-data-that-differs-from-s3).

### 3. Disk space exhaustion

**Detection**: a cache write fails with `ENOSPC`. These writes detect and count it:

- range data files, whether written whole or streamed from a GET response, including multipart parts assembled into the cache;
- `.meta` files written on the request path: range stores, TTL refreshes, HEAD metadata and range removals;
- journal appends.

Not counted: `.meta` writes made by the journal consolidator, cache-hit journal flushes, and the accounting state files under `metadata/_shard_state/` and `metadata/_write_ledger/`.

**What the proxy does**: it logs a WARN naming the write that failed, with the running total, removes the write's temporary file where it has one, and counts the event in `disk_full_events_total`. The request is still served from S3, and no range file is published for the failed write. Caching resumes once eviction or the operator frees space.

Nothing on the write path triggers eviction: eviction runs on the consolidation cycle's own schedule (see [Triggers](EVICTION.md#triggers)).

To prevent it:

- set `cache.max_cache_size` to 80 to 90% of the cache volume's capacity;
- alert on low free space on the cache volume and on increases in `disk_full_events_total`;
- put the cache and the logs on separate disks or partitions.

### 4. Inconsistent metadata

**Detection**: metadata lists range files that do not exist.

The proxy does not scan for this. No pass walks the cache checking each `.meta` against its range files. A request that reads a missing range file handles it as described in [Missing range binary files](#2-missing-range-binary-files): the affected bytes are fetched from S3 and the other cached ranges keep serving.

Typical causes are a crash during a range deletion, manual changes to cache files, a failed partial write, and disk corruption affecting specific files.

### 5. Partial write failures

**Detection**: `.tmp` files exist with no matching final file.

A cache writer removes its own temporary file when its write or rename fails. No automatic cleanup runs for a temporary file left behind by a crash: it stays until it is removed by hand, and [Metadata cleanup](EVICTION.md#metadata-cleanup) lists the three shapes and when removing them is safe. No `/metrics` field counts these files or their removal.

Partial writes come from a crash, a full disk, an I/O error or the process being killed during a write. A temporary file is never served; only a renamed, complete range file is, and valid cache entries are unaffected.

### 6. Orphaned files

**Detection**: range files exist with no metadata listing them.

**What the proxy does**: the orphan reclamation rotation deletes the file. The rotation runs on every proxy. It deletes a file only when, under the object's metadata lock, no `.meta` lists it and no proxy's journal holds a pending write for it. Orphans are never added back to metadata. A pass is skipped, and counted in `orphan_reclaim.passes_gated`, while the rotation waits for proxies on an older release to leave the fleet or for traffic; see [During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade) and [Orphan recovery](SHARED_STORAGE.md#orphan-recovery).

The rotation finds orphans on its own schedule, not on the next read of the object. Their bytes were never in `cache.total_cache_size`, so deleting one does not change it, and valid cache entries are unaffected.

Typical causes are a crash during a metadata deletion, manual deletion of metadata files, and disk corruption that hit metadata but not range files. An invalidation or overwrite that removed metadata and left the range files leaves orphans too.

To monitor it:

- the `orphan_reclaim` section of `/metrics`: `found`, `reclaimed`, `bytes_reclaimed`, `skipped_unconfirmed` and the rotation-progress gauges (see [`orphan_reclaim`](METRICS_REFERENCE.md#orphan_reclaim));
- while passes are skipped, disk usage against `cache.total_cache_size` (see [Error metrics](#error-metrics)): orphan bytes show as the gap between the two.

### 7. Stalled upstream responses (mid-stream idle watchdog)

**Detection**: no bytes arrive from the upstream within `upstream_idle_timeout` (default `5s`) while the proxy is trying to read. Backpressure from a slow client does not count as an idle upstream.

**What the proxy does**:

- **Before the first byte**: it retries up to `upstream_idle_retries` times (default 2), failing over through the connection pool. When the retries run out, it answers `504 Gateway Timeout`.
- **Mid-stream**: it closes the client connection, ending the body early, and the client's own retry logic takes over. The requested range is never cached as if it had arrived whole.

The idle timer measures a gap with no bytes, not the total transfer time, so a slow transfer that keeps making progress is never aborted. While the cache writer cannot keep up, the proxy stops asking the upstream for data and the timer pauses. An aborted fetch never publishes a truncated `.bin` under the requested bounds. If at least `cache.partial_range_commit_ratio` (default `0.5`) of the requested bytes arrived in order, the proxy caches them as a smaller valid range; otherwise it deletes the temporary file. See [Cache safety](CONNECTION_POOLING.md#cache-safety). The watchdog does not apply while a request waits for another request's fetch of the same object, which has its own coordination timeout.

The counters, per proxy since the process started, on `/metrics` and over OTLP:

- `cache.upstream_idle_abort_total_mid_stream`: responses cut off mid-body by the watchdog;
- `cache.upstream_idle_abort_total_pre_stream`: requests answered `504` after every first-byte attempt timed out;
- `cache.upstream_idle_retry_total`: first-byte timeouts that were retried.

Each abort also logs a WARN: `[UPSTREAM_IDLE_ABORT]` mid-stream and `[UPSTREAM_FIRST_BYTE]` before the first byte.

```yaml
connection_pool:
  upstream_first_byte_timeout: "5s"
  upstream_idle_timeout: "5s"
  upstream_idle_retries: 2
```

### 8. A slow cache volume on the write path (bounded waits)

Every write-path step that touches the cache volume is bounded, and every bound expires the same way: the upload continues to S3, uncached. A slow volume costs hit rate, not uploads, and none of these bounds returns an error to the client.

The bounds are internal constants with no configuration field:

| Step | Bound | What happens when it expires |
|---|---|---|
| Cache setup, which runs beside the upload: sink open, capacity reservation, `.tmp` creation, TTL lookup | 5s | Upload forwarded uncached |
| Cache setup, when a streaming PUT's buffer fills while setup is still running | 500ms | Upload forwarded uncached |
| Each body frame handed to the cache writer, after setup | 5s, and at most what remains of the write's 1s in total | Caching abandoned mid-upload; bytes already written are discarded, not published |
| Origin-marker read or claim for the bucket, for a PUT, UploadPart, multipart request, DELETE, DeleteObjects or form POST | 250ms | A PUT, UploadPart or multipart request is forwarded uncached and the key's cached copy is invalidated on success; a DELETE, DeleteObjects or form POST still invalidates the cached copy of each key it deletes or writes |
| Origin-marker read or claim for the bucket, for a read such as GET or HEAD | 5s | That request forwarded uncached |
| The first cache-size read a PUT's capacity check needs | 250ms | Write forwarded uncached; the key's cached copy is invalidated on success |
| The first `cache_rules.json` load, for a write | 250ms | Write forwarded uncached; the key's cached copy is invalidated on success |
| Cache-rule reload, for the request that triggers it | 100ms | That request uses the rules it already has; the reload completes in the background |
| `CreateMultipartUpload`: the disk-safety check and the `upload.meta` write | 500ms | The upload is not cached, and a tracker written later is removed |
| `CompleteMultipartUpload`: the client's wait for cache finalization | 250ms | The response returns; finalization continues in the background |

#### One budget per write

Cache work on a write has one budget of 1,000 ms, which every wait in the table draws on except the 5 s cache setup, since that runs beside the upload and delays nothing. The budget covers the per-frame waits during the upload, the 500 ms wait after the buffer fills, and the origin-marker and cache-size reads. It also covers the cache-rule reload, the `CreateMultipartUpload` checks, the `CompleteMultipartUpload` wait, and the invalidation after a PUT, DELETE or form POST. Each wait takes the smaller of its own bound and what remains, so the 5 s per-frame figure is a ceiling the budget cuts short. Cache work therefore delays a write by at most 1,000 ms in total, however many steps it takes. When the budget is spent, a cache read that has not finished forwards the write uncached, caching an upload is abandoned, and an invalidation finishes in the background.

DeleteObjects is the exception. Its origin-marker read (at most 250 ms) and the invalidation of its whole batch share one flat 3 s budget that starts when S3 answers. A key still being invalidated at the deadline, or not yet started, is handed with the other unfinished keys to the background stamp retry. Until the retry lands, this proxy forwards that key's GET and HEAD to S3. That holds even when every retry worker is busy and the record on the proxy's local disk cannot be written; [Write ordering](CACHING.md#write-through-ordering) gives the memory limit and what happens past it. Handing keys over writes records to the proxy's local disk and syncs them after the deadline, and so does a key whose lock retries ran out just before it. The response can therefore run past 3 s by a few local disk writes.

Serial stalled DeleteObjects still open the breaker described next. Each counts once, when its batch ends. The breaker therefore trips when the time each request spends beyond its 3 s is under about 2 s, made up of the client's turnaround, S3's own DeleteObjects time and the local disk writes. While the breaker is open, a DeleteObjects hands every key over at once with no cache delay. One that started while the breaker was closed keeps its 3 s even if the breaker opens during it.

#### The volume stall breaker

If three writes each have a bounded cache wait time out within 10 seconds, and those writes did not all start waiting on the cache together, the proxy treats the cache volume as stalled. Any bounded cache wait counts: the 250 ms origin-marker, cache-size and cache-rules reads, the 1 s budget, and a DeleteObjects' 3 s. The breaker stays open until three probes of the volume succeed in a row, spanning at least one second. Until then, writes skip the cache with no added delay, their invalidations go to the background retry, and GET and HEAD are forwarded to S3 uncached. The exception is a plain HEAD whose metadata this proxy loaded into memory within `metadata_cache.refresh_interval`, which is answered from memory.

Each proxy decides for itself. `cache.volume_stall_breaker_open` is `1` while the breaker is open, and `cache.volume_stall_breaker_trips_total` counts the times it opened. The `volume_stalled` reason in `skipped_puts_total` and `cache_bypasses_by_reason` counts what was skipped.

An `UploadPart` response is also held until its part sink has opened, for at most 500 ms, so that a `CompleteMultipartUpload` sent right after a small part finds the upload's staging directory.

#### What it looks like

Each bound logs a rate-limited WARN that names the step that gave up and says the upload was unaffected:

```text
Streaming write cache: cache setup did not finish within 5s, forwarding uncached (upload unaffected; ...)
Streaming write cache: cache writer did not accept a frame within <bound>, abandoning caching for this object (upload unaffected): cache_key=...
Streaming write cache: cache setup had not finished within 500ms of the tee's buffer filling, forwarding uncached (upload unaffected; ...)
Origin marker: read/claim for bucket B did not finish within 5s; forwarding uncached ...
Origin marker: read/claim for bucket B did not finish within 250ms; forwarding the write uncached ...
PUT capacity check: the first cache-size read did not finish within 250ms; forwarding writes uncached until it does ...
```

Two counters per proxy on `/metrics` separate the two places caching can give up:

- `write_cache.setup_abandoned_total_put`: cache setups, which run beside the upload, that did not finish;
- `cache.tee_abandoned_total_put`: caching abandoned once the body was already streaming.

A steady non-zero reading on either means the write cache is being skipped, so PUT-heavy traffic pays S3 latency on the reads that follow. Read them against the volume's own latency: there is no proxy setting to tune, and the fix is the volume's metadata-operation latency. On FSx, check `nconnect` first; see [Mount requirements](SHARED_STORAGE.md#mount-requirements).

#### How stale the capacity figure can be

The capacity figure a PUT checks is refreshed by a background task every 5 seconds, not read per request. It can be up to one refresh interval stale on a healthy volume, and as stale as the stall on an unhealthy one. The `max_cache_size` check each upload passes adds what this proxy has written since a consolidation cycle last folded its own additions into the shared total. One proxy's burst is therefore measured against a figure that includes it. GET-miss fills take the same soft stop and count the same way.

What a proxy adds is what it has admitted, not the net of what it has also evicted since the total was folded. A cache sitting at `max_cache_size` with eviction keeping pace can therefore decline some uploads and fills it has room for, until the next consolidation cycle. A record the proxy renamed aside as corrupt, or retired at its 256-record cap, can keep that over-decline going for longer. An upload's check reads this proxy's additions before the shared total, and does not pair the two reads the way a fill's check does. Around a consolidation cycle it can therefore decline an upload that a fill would have passed.

A fleet stays within a bound made of three terms. They are `max_cache_size`, the uploads and fills in flight on each proxy, and what each other proxy admits at its own offered (attempted) rate over the age of the figure it reads. That age is the 5-second refresh plus the time one consolidation cycle takes: close to `cache.shared_storage.consolidation_interval` on a healthy volume, and several times longer when admission alone saturates a small file system. Eviction reclaims the overshoot.

### 9. Request loops (Via and 508 Loop Detected)

**Detection**: a request arrives carrying a `Via` entry that names this proxy process.

The proxy connects to the `Host` a client names. If that name resolves back to the fleet, for example to a load balancer in front of the proxies, the request would visit proxies in a cycle. Each visit holds a `max_concurrent_requests` permit, until the fleet sheds traffic.

Every request the proxy forwards over the HTTP listener or the TLS proxy listener carries a `Via: 1.1 s3-proxy-<token>` entry. The token is 16 random hex digits chosen when the process starts, so each proxy process has its own and a restart changes it. A request that arrives with this process's own entry is answered `508 Loop Detected` with an S3-style `LoopDetected` XML error and is not forwarded. The proxy logs a WARN at most once per 60 seconds, with the number of refusals in that window.

**What to do**: fix the DNS record or load balancer so the `Host` names S3, not the proxy. The proxy does not retry a looped request.

- A `Via` entry from another proxy, as in [chaining to another proxy](CONFIGURATION.md), is kept, and this proxy's entry is appended after it.
- A request that signs `via`, in its `Authorization` header's `SignedHeaders` or in a presigned URL's `X-Amz-SignedHeaders`, is forwarded with its `Via` unchanged. It carries no entry from this proxy, so a loop through it is not detected. This is the only exception: every other forwarded request carries the entry, including cache revalidation and refetch requests.
- The entry is added whatever `add_referer_header` is set to; no setting turns it off.
- The HTTPS passthrough (port 443) never reads request headers, so it neither adds the entry nor detects loops.

## Error metrics

The error counters sit in the `cache` section of `/metrics` (`curl http://localhost:9090/metrics`); [`cache`](METRICS_REFERENCE.md#cache) is the field reference.

| Field | Meaning |
|---|---|
| `corruption_metadata_total` | Corrupt `.meta` files detected, all reasons |
| `metadata_heal_overwrite_total` | Corrupt `.meta` files replaced by a fresh document |
| `corruption_missing_range_total` | Range files the metadata listed that were missing |
| `disk_full_events_total` | Counted cache writes that failed with `ENOSPC` |

For orphan range files, read the `orphan_reclaim` section, and compare disk usage with `cache.total_cache_size` while the rotation is waiting (`orphan_reclaim.passes_gated` rising).

## Scheduled cleanup

The proxy has no whole-cache cleanup pass that removes temporary files, deletes orphaned range files or repairs metadata, and nothing outside the process can start one. Two mechanisms run on their own:

- the [orphan reclamation rotation](SHARED_STORAGE.md#orphan-recovery), which deletes range files no metadata lists. Each proxy runs one bounded pass every 300 seconds while passes are not skipped; the interval and the per-pass budget are not configurable;
- the daily validation scan, which deletes unparseable `.meta` files only.

## Monitoring and alerting

Recommended alerts:

1. **High corruption rate**: `corruption_metadata_total` increasing quickly. It can mean disk faults or a software defect.
2. **Disk full events**: any increase in `disk_full_events_total`. The cache size limit or the disk may need changing.
3. **Orphaned files accumulating**: free space on the cache volume, and the gap between disk usage and `cache.total_cache_size`. Also alert on a sustained rise in `peer_unavailable` or `indeterminate` in `orphan_reclaim.journal_evidence_total`, or in `orphan_reclaim.passes_gated` while the fleet serves traffic and no proxy on an older release remains. `orphan_reclaim.reclaimed` and `orphan_reclaim.bytes_reclaimed` count the orphaned range files the reclaim rotation has removed.

Example Prometheus queries over the OTLP export, where `cache.corruption_metadata_total` and `cache.disk_full_events_total` arrive with dots converted to underscores (see [OTLP metrics](OTLP_METRICS.md)):

```promql
# Corruption rate (per minute)
rate(cache_corruption_metadata_total[1m])

# Disk full events (last hour)
increase(cache_disk_full_events_total[1h])
```

No query is useful for orphaned files, inconsistencies or partial-write cleanup: OTLP exports none of them and the `/metrics` fields never move. Alert on the cache volume's free space instead.

## Troubleshooting

### High corruption rate

**Symptoms**: `corruption_metadata_total` increasing quickly.

**Possible causes**: disk errors or failing hardware, unclean shutdowns or crashes, or a serialization defect.

**What to do**:

1. Check disk health, for example `smartctl -a /dev/sdX` on a local disk.
2. Look in the system logs for crashes.
3. Consider clearing the cache and starting fresh.

### Disk full events

**Symptoms**: `disk_full_events_total` increasing, and cache writes failing.

**Possible causes**: a cache size limit too high for the disk, other processes using the disk, or eviction not keeping up.

**What to do**:

1. Check disk usage with `df -h`.
2. Lower `cache.max_cache_size`.
3. Check the `[DISK_CACHE_EVICTION]` log lines and `cache.eviction_unlink_failures_total` (see [When a range file cannot be deleted](EVICTION.md#when-a-range-file-cannot-be-deleted)).
4. Remove old cache entries by hand if needed.

### Orphaned files accumulating

**Symptoms**: disk usage on the cache volume exceeds `cache.total_cache_size` and the gap grows, while `orphan_reclaim.found` rises.

**Possible causes**: crashes during writes, or invalidations that removed metadata and left range files. Candidates are also kept when a journal could not be read (`peer_unavailable` or `indeterminate` in `orphan_reclaim.journal_evidence_total`) or the cache key could not be recovered from the file name.

**What to do**:

1. `orphan_reclaim.skipped_unconfirmed` rises normally on a busy cache, because new files and uploads in progress are kept by design. If `peer_unavailable` or `indeterminate` keeps rising in `journal_evidence_total`, check that every proxy's journal under `metadata/_journals/` is readable (see [Orphan recovery](SHARED_STORAGE.md#orphan-recovery)).
2. Size the cache volume with headroom above `cache.max_cache_size` for orphans not yet reclaimed.
3. Check for crashes during cache operations.

## Prevention

Let the proxy shut down gracefully, so no write is cut off mid-file:

```bash
kill -TERM <proxy_pid>  # Graceful shutdown
systemctl stop s3-proxy  # The same, for the systemd service
```

Avoid `kill -9` (SIGKILL) unless nothing else works.

On a local disk, watch disk health and I/O errors:

```bash
smartctl -a /dev/sdX  # Disk health
iostat -x 5  # Disk I/O
fsck /dev/sdX  # File system check, on an unmounted file system only
```

Size `cache.max_cache_size` against the volume as described under [Disk space exhaustion](#3-disk-space-exhaustion), and watch free space with `df -h`. Orphan recovery and the validation scan run inside the proxy with nothing to schedule; see [Scheduled cleanup](#scheduled-cleanup).

## Testing

The error-handling paths are covered by `tests/error_handling_test.rs`, which builds inside the `group_other` test target:

```bash
cargo test --release --test group_other -- error_handling_test::  # Run the error-handling tests
```

## See also

- [Connection pooling](CONNECTION_POOLING.md): retries, IP health tracking and the upstream idle watchdog
- [Eviction](EVICTION.md): capacity limits and what frees space
- [Orphan recovery](SHARED_STORAGE.md#orphan-recovery): how orphaned range files are reclaimed on a cache volume
- [`/metrics` reference](METRICS_REFERENCE.md): every counter named on this page
