# `/metrics` Reference

Complete field reference for the `/metrics` HTTP endpoint. This is the authoritative list
of what the proxy exposes.

Related docs, so you land in the right place:

- **[METRICS.md](METRICS.md)** — how to *interpret* per-bucket traffic, cache savings, and the memory/permit counters
- **[OTLP_METRICS.md](OTLP_METRICS.md)** — the much smaller subset exported over OpenTelemetry
- **[DASHBOARD.md](DASHBOARD.md)** — the browser view and its own `/api/*` endpoints
- **[CONFIGURATION.md — Metrics Configuration](CONFIGURATION.md#metrics-configuration)** — the config fields

## The endpoint

```bash
curl -s http://<proxy>:9090/metrics
```

| Behaviour | Detail |
|---|---|
| Success | `200`, `Content-Type: application/json` |
| Wrong path | `404`, **empty body, no `Content-Type`**. Only the exact configured `metrics.endpoint` is served |
| Non-GET on the right path | `405` with `Allow: GET`, empty body |
| Auth | **None.** Bind address defaults to `0.0.0.0` |

Three things that surprise people:

**The JSON is pretty-printed.** The wire format is `"reserved_bytes": 0`, with a space
after the colon. Grep patterns like `'"reserved_bytes":[0-9]*'` match nothing, silently
yield empty, and typically get defaulted to `0` by the caller — which reads as a
confident, wrong measurement. Parse with a real JSON parser:

```bash
curl -s http://<proxy>:9090/metrics | python3 -c \
  'import sys,json; print(json.load(sys.stdin)["inflight_memory"]["reserved_bytes"])'
```

**Every scrape collects fresh.** Values are never served from the background cache, so a
scrape is not free — it walks the cache manager, connection pool, and consolidator.

**A scrape also triggers an OTLP export** when OTLP is enabled. Scrape frequency therefore
drives OTLP push frequency in addition to `metrics.otlp.export_interval`.

## Reading this reference

| Kind | Meaning |
|---|---|
| **counter** | Cumulative since process start. Resets to zero on restart. No reset API |
| **gauge** | Point-in-time |
| **derived** | Computed at serialization time. Formula given |
| **windowed** | Mean over a rolling buffer of the **last 1000 samples**. Moves down as conditions improve. The buffer is 1000 samples, not a time window, so the interval it covers varies with load |
| **config** | Echo of a configured value, not a measurement |

**Nullable sections.** `cache`, `compression`, `connection_pool`, `cache_size`,
`consolidation`, and `cache_rules` serialize as `null` when their source component was not
wired in at startup. Handle null rather than assuming the key is an object.

## Top-level shape

Keys appear in this order.

```
timestamp, uptime_seconds, cache, compression, connection_pool,
eviction_coordination, signed_put, cache_size, atomic_metadata,
consolidation, coalescing, cache_rules, page_cache, request_metrics,
bucket_traffic, download_bandwidth, hedged_requests, inflight_memory
```

**`timestamp`** is **not** a number and not RFC3339. It serializes as an object:

```json
"timestamp": { "secs_since_epoch": 1755608499, "nanos_since_epoch": 358795000 }
```

Note the inconsistency: `cache_size.last_checkpoint`, `cache_size.last_validation`,
`consolidation.last_consolidation_timestamp`, and `cache_rules.last_load_unix` are all
plain unix-second integers. Only the top-level `timestamp` uses the object form.

**`uptime_seconds`** (gauge) — seconds since process start.

---

## Fields with misleading names

Read this before building any dashboard. Each of these has burned someone.

### `request_metrics.active_requests` counts TCP connections

Not requests in flight. With HTTP/1 keep-alive one connection serves many sequential
requests, so this does not track the quantity `max_concurrent_requests` bounds.

**Use `request_metrics.permits_held`** for in-flight requests.

### `cache.ram_cache_hit_rate_percent` changed scale in 2.5.0

Up to 2.4.3 the field was a `0.0`–`1.0` fraction despite the `_percent` suffix, and it was
also written from two different code paths — one of which computed it from **overall** cache
hits rather than RAM-tier hits, so it could reflect the wrong quantity.

From 2.5.0 it is a true `0`–`100` percentage sourced only from RAM-tier hit and miss counts,
matching its sibling `cache.cache_hit_rate_percent`. A dashboard that multiplied this field
by 100 to work around the old scale now reads 100x high and must drop the multiplication.

To derive the same rate independently:

```
ram_hit_rate = cache.ram_cache_hits / (cache.ram_cache_hits + cache.ram_cache_misses)
```

### Three `connection_pool` fields are hardcoded placeholders

`failed_connections` is always `0`, `average_latency_ms` always `0`, and
`success_rate_percent` always `1.0`. Nothing measures them. Do not alert on them —
`success_rate_percent: 1.0` looks like a catastrophic 1% on a percent-scaled panel while
actually meaning "unimplemented".

### `cache.total_requests` counts cache lookups

It is `cache_hits + cache_misses`, not HTTP requests, and will not reconcile with
`request_metrics.total_requests`.

### `request_metrics.failed_requests` includes 3xx

It counts every non-2xx, so a `304 Not Modified` from a successful revalidation lands in
"failed". For genuine errors use `client_error_requests` + `server_error_requests`.

### `permits_total` and `max_concurrent_requests` are the same value

Both echo the configured `server.max_concurrent_requests`. Neither is a measurement.
The observed maximum is `permits_held_peak`.

### No percentiles exist anywhere

Every `average_*` and `*_duration_ms` field is a scalar mean. There are no histograms and
no percentiles in this payload, despite some source comments calling them histograms.

---

## `cache`

Nullable. Sizes, hit counters, and error counters for the disk and RAM tiers.

### Sizes and rates

| Field | Kind | Meaning |
|---|---|---|
| `total_cache_size` | gauge | Total disk bytes (read + write) |
| `read_cache_size` | gauge | Read-cache bytes |
| `write_cache_size` | gauge | Write-cache bytes |
| `ram_cache_size` | gauge | Current RAM range-cache bytes |
| `cache_hit_rate_percent` | derived | `cache_hits / (cache_hits + cache_misses) * 100`. `0.0` when no lookups |
| `ram_cache_hit_rate_percent` | derived | RAM-tier hit rate, `0`–`100`. `0.0` when the RAM cache is disabled. Was a `0.0`–`1.0` fraction before 2.5.0 — see above |
| `total_requests` | derived | `cache_hits + cache_misses`. Lookups, not HTTP requests |
| `cache_hits` | counter | Cache lookups served from cache |
| `cache_misses` | counter | Cache lookups that had to fetch |
| `bytes_served_from_cache` | counter | Bytes served from cache, i.e. S3 transfer avoided |
| `evictions` | counter | Entries evicted |

### Range and metadata behaviour

| Field | Kind | Meaning |
|---|---|---|
| `incomplete_range_fallbacks` | counter | Range responses that refetched from S3 because cached extents did not cover the request. Above zero means the proxy correctly avoided serving an incomplete range; it is not a hit |
| `range_file_count` | gauge | Range files present |
| `range_load_duration_ms` | windowed | Mean range-load time |
| `metadata_parse_duration_ms` | windowed | Mean `.meta` parse time |
| `metadata_file_size_bytes` | gauge | Last observed `.meta` size |
| `cache_operation_duration_ms` | windowed | Mean cache-operation time |
| `ttl_revalidations_total` | counter | Conditional GET/HEAD sent to S3 after TTL expiry |
| `read_cache_disabled_invalidations_total` | counter | Eager purges from a `read_cache_enabled: false` rule |
| `old_cache_key_encounters` | counter | Legacy-format cache keys seen |

### Error and integrity counters

All counters. `corruption_metadata_total`, `corruption_missing_range_total`,
`inconsistency_fixed_total`, `partial_write_cleanup_total`, `disk_full_events_total`,
`orphaned_files_cleaned_total`, `lock_timeout_total`, `cache_write_failures_total`,
`cache_cleanup_failures_total`, `cache_etag_validations_total`,
`cache_etag_mismatches_total`, `cache_range_invalidations_total`,
`cache_orphaned_ranges_cleaned_total`.

`cache_bypasses_by_reason` is a map of reason string to count. Reasons include
`sse-c`, `no-cache directive`, `no-store directive`, `read_cache_disabled`, and
`part-scoped-head` (a `HEAD` naming a single part, which is always forwarded — see
[CACHING.md](CACHING.md#operations-that-bypass-cache)). See
[ERROR_HANDLING.md](ERROR_HANDLING.md) for what each failure class means.

### Part cache (multipart)

Counters: `cache_part_hits`, `cache_part_misses`, `cache_part_stores`,
`cache_part_evictions`, `cache_part_errors`. See
[MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md).

### Write cache

`write_cache_hits` (counter), `incomplete_uploads_evicted` (counter).

### RAM tier

`ram_cache_hits`, `ram_cache_misses`, `ram_cache_evictions` (counters);
`ram_cache_max_size` (config).

### Metadata cache tier

`metadata_cache_hits`, `metadata_cache_misses`, `metadata_cache_evictions`,
`metadata_cache_stale_refreshes` (counters); `metadata_cache_entries` (gauge);
`metadata_cache_max_entries` (config).

---

## `compression`

Nullable. `total_objects_compressed`, `total_objects_uncompressed`, `total_bytes_before`,
`total_bytes_after`, `compression_failures`, `decompression_failures` (counters), and
`average_compression_ratio` (derived).

`total_objects_uncompressed` counts store-mode writes — framed and checksummed but not
block-compressed, which is what a denylisted extension produces. See
[COMPRESSION.md](COMPRESSION.md).

---

## `connection_pool`

Nullable. **`failed_connections`, `average_latency_ms`, and `success_rate_percent` are
hardcoded placeholders** (see above).

| Field | Kind | Meaning |
|---|---|---|
| `dns_refresh_count` | counter | DNS refresh cycles |
| `ip_addresses` | gauge | Flat list of every resolved upstream IP across all endpoints. Endpoint attribution is lost and duplicates are possible |
| `connections_created` | counter map | New upstream connections, keyed by host |
| `connections_reused` | counter map | Keep-alive reuses, keyed by host |
| `idle_timeout_closures` | counter | Closed on idle timeout |
| `max_lifetime_closures` | counter | Closed on max lifetime |
| `error_closures` | counter | Closed on error |

Reuse rate is the useful derived figure:

```
reuse_rate(host) = connections_reused[host] / (connections_created[host] + connections_reused[host])
```

A low reuse rate with keepalive enabled points at `max_lifetime` or `idle_timeout` being
too short. See [CONNECTION_POOLING.md](CONNECTION_POOLING.md).

---

## `request_metrics`

Always present. Fixed cardinality: the proxy does not label these by URL, bucket, key,
client address, or host, so the payload stays bounded under a high-cardinality workload.

| Field | Kind | Meaning |
|---|---|---|
| `total_requests` | counter | Completions, **including** sheds |
| `successful_requests` | counter | 2xx |
| `failed_requests` | counter | Every non-2xx, **including 3xx** |
| `client_error_requests` | counter | 4xx |
| `server_error_requests` | counter | 5xx |
| `rejected_requests` | counter | `503 SlowDown` sheds, **all causes** |
| `cache_hit_requests` | counter | GET/HEAD that carried `X-Cache: HIT` |
| `cache_miss_requests` | counter | GET/HEAD that did not |
| `average_response_time_ms` | derived | `total_response_time_ms / total_requests`, integer division |
| `requests_per_second` | derived | **Lifetime average since process start, not a rolling window.** Flattens over long uptimes and will badly under-report a recent burst |
| `active_requests` | gauge | **TCP connections — see above** |
| `max_concurrent_requests` | config | Configured limit |
| `permits_total` | config | Same value, `permits_*`-named |
| `permits_held` | derived gauge | `permits_total - permits_available`. **The real in-flight-request gauge** |
| `permits_available` | gauge | Free permits |
| `permits_held_peak` | high-water | Process-lifetime maximum of `permits_held` |

### Attributing a shed

`rejected_requests` counts every shed cause together. There is no dedicated counter for
concurrency sheds, so derive it:

```
concurrency_sheds = request_metrics.rejected_requests - inflight_memory.rejected_total
```

Size `max_concurrent_requests` from measured `permits_held_peak` under representative
load, not from request rate. See
[CONFIGURATION.md — Max Concurrent Requests](CONFIGURATION.md#max-concurrent-requests).

---

## `inflight_memory`

Always present, including when disabled — `ceiling_bytes: 0` means the ledger is off, so
`/metrics` alone confirms whether the feature is active.

| Field | Kind | Meaning |
|---|---|---|
| `reserved_bytes` | gauge | Bytes currently claimed by buffering paths |
| `ceiling_bytes` | config | `server.max_inflight_buffer_bytes`. **`0` = disabled** |
| `peak_reserved_bytes` | high-water | Maximum `reserved_bytes` since start |
| `rejected_total` | counter | Sheds attributable to **this ledger only**. Always `<= request_metrics.rejected_requests` |
| `aborted_accumulations_total` | counter | Unknown-size accumulations aborted mid-stream at the ceiling |
| `ram_cache_max_bytes` | config | `cache.max_ram_cache_size`, echoed so the two memory budgets read side by side |

`ceiling_bytes: 0` cannot distinguish "configured off" from "not wired in" — both produce
zero.

**`reserved_bytes` is not a resident-memory figure**, and it does not cover the streaming
or RAM-hit paths. [METRICS.md](METRICS.md#what-reserved_bytes-covers-and-what-it-does-not)
has the sizing guidance; read it before setting a ceiling from this number.

---

## `page_cache`

Always present. Page-aligned range caching, off unless a rule enables `page_widening`.

| Field | Kind | Meaning |
|---|---|---|
| `widened_requests` | counter | Requests widened to a page-aligned fetch |
| `bytes_prefetched` | counter | Bytes fetched beyond what clients asked for |
| `amplification_ratio` | derived | `(bytes_requested + bytes_prefetched) / bytes_requested`. **`1.0` when nothing has been requested yet, not `0.0`.** `bytes_requested` is internal and not in the payload, so this cannot be recomputed |
| `page_hits` | counter | Small reads served from an already-cached page with no S3 fetch |
| `skipped_signed_range` | counter | Widening declined because the `Range` header was signed |
| `fallbacks` | counter | Widened fetches that failed and fell back to the client's range |
| `ram_page_promotions` | counter | Pages promoted to RAM |
| `ram_page_promotion_skipped` | counter | Promotions skipped for exceeding the RAM budget |

`skipped_signed_range` climbing while `widened_requests` stays flat is the expected
picture for AWS CLI and SDK traffic: those clients sign the `Range` header, and widening
excludes signed ranges by design. Widening applies to unsigned ranges, which come from
presigned URLs and raw HTTP clients. Not exported over OTLP. See
[CACHING.md](CACHE_READ_PATHS.md#page-aligned-range-caching).

---

## `hedged_requests`

Always present. `issued`, `won`, `suppressed` (counters). `issued` is your duplicate-S3
bill. See [HEDGING.md](HEDGING.md).

---

## `bucket_traffic`

A map, always present, `{}` when no traffic. Keys are `"bucket"`, or `"bucket/prefix"`
when a configured prefix matched, or the reserved `"__other__"` once
`metrics.per_bucket.max_series` is exceeded.

Per-key counters: `bytes_served`, `bytes_saved`, `bytes_uploaded`, `get_requests`,
`put_requests`.

Two gotchas. The `"bucket/prefix"` join is **lossy** — you cannot split a key back apart
without knowing the configured prefix list. And only object GET and PUT/UploadPart are
counted: HEAD, DELETE, LIST, and the multipart lifecycle POSTs are deliberately excluded,
so `get_requests + put_requests` will never reconcile with
`request_metrics.total_requests`.

[METRICS.md](METRICS.md) is the interpretation guide, including S3 CloudWatch
cross-validation.

---

## `download_bandwidth`

Always present, reflecting current state whether enabled or not.

| Field | Kind | Meaning |
|---|---|---|
| `enabled` | gauge | `false` when `max_bytes_per_sec` is `0` |
| `instance_ceiling_bps` | gauge | Current per-instance ceiling, bytes/sec |
| `failopen_total` | counter | Fail-open events (limiter fault or DRR timeout) |
| `class_bytes` | counter map | Per-class cumulative bytes, capped at `max_tracked_classes` |
| `residual_bytes` | counter | Bytes for classes evicted from the top-K |

**`class_bytes` values can over-count.** The tracker is Space-Saving, so a retained class
inherits the evicted minimum when it displaces another class. Treat the values as
heavy-hitter estimates, not exact totals. Every byte is counted somewhere, either in a
class or in `residual_bytes`. Note the OTLP export folds the residual into
`download_bandwidth.class_bytes` with `class="residual"` rather than a separate metric.

See [BANDWIDTH_QOS.md](BANDWIDTH_QOS.md).

---

## `coalescing`

Nullable in type, always present in practice. Download coordination.

| Field | Kind | Meaning |
|---|---|---|
| `waits_total` | counter | Requests that waited on an in-flight fetch |
| `cache_hits_after_wait_total` | counter | Waiters served from cache after the wait |
| `timeouts_total` | counter | Waiters that timed out |
| `s3_fetches_saved_total` | counter | S3 fetches avoided by coalescing |
| `average_wait_duration_ms` | derived | `sum / count` over **all** samples. A lifetime mean that never decays — unlike the windowed means elsewhere |
| `fetcher_completions_success` / `_error` | counter | Fetch outcomes |
| `waiter_conditional_304` | counter | Waiter revalidation returned 304 (IAM-validated cache serve) |
| `waiter_conditional_200` | counter | Object changed mid-flight; waiter served S3's fresh body |
| `waiter_conditional_4xx` | counter | Auth or client error; returned to client, cache preserved |
| `waiter_conditional_error` | counter | 5xx or transport error; degraded cache-serve fallback |

`s3_fetches_saved_total` is the direct benefit figure. See
[CACHING.md](CACHE_READ_PATHS.md#download-coordination).

---

## `cache_rules`

Nullable. Health of `cache_rules.json` hot-reload.

| Field | Kind | Meaning |
|---|---|---|
| `reloads_total` | counter | Successful loads |
| `reload_failures_total` | counter | Parse, validation, or compile failures |
| `on_fallback` | **bool gauge** | `true` when running a stale ruleset because the last load failed |
| `rules_loaded` | gauge | Rules in the active set |
| `last_load_unix` | gauge | Unix seconds of the last successful load |

**`on_fallback` is the field to alert on.** A rules file that fails to parse does not stop
the proxy — it keeps the last known-good set and carries on, which is the right behaviour
but means a broken edit is otherwise silent.

---

## `cache_size`

Nullable. The size-tracking accumulator and validation scan.

| Field | Kind | Meaning |
|---|---|---|
| `current_size` | gauge | Tracked total bytes |
| `write_cache_size` | gauge | Tracked write-cache bytes |
| `last_checkpoint` | **deprecated stub** | Always the current time — set to `now()` on every read. Not a checkpoint timestamp |
| `last_validation` | gauge | Unix seconds, or `null` if no scan has run |
| `last_validation_drift` | gauge | Signed byte drift the last scan found, or `null` |
| `checkpoint_count` | **deprecated stub** | Always `0` |
| `delta_log_size` | **deprecated stub** | Always `0` |

**Three of these are deprecated stubs**, in the same category as the `connection_pool`
placeholders above: `last_checkpoint`, `checkpoint_count`, and `delta_log_size` are
hardcoded rather than measured. Checkpointing was replaced by the journal consolidator,
which persists `size_state.json` directly, and the delta files it uses are transient. Use
`consolidation.last_consolidation_timestamp` and `consolidation.consolidation_count`
instead — those are real.

`last_validation_drift` is the accuracy signal for tracked size. Persistent large drift on
a multi-instance deployment is the expected symptom of concurrent-write over-counting;
see [SHARED_STORAGE.md](SHARED_STORAGE.md).

---

## `consolidation`

Nullable — `null` when no journal consolidator is attached.

`cache_size_bytes`, `cache_write_size_bytes` (gauges), `consolidation_count` (counter),
`last_consolidation_timestamp` (derived unix seconds, `0` on error).

A `last_consolidation_timestamp` that stops advancing means the consolidator has stalled,
which on shared storage means metadata updates are accumulating in journals unapplied.

---

## `atomic_metadata`

Nullable in type, always present in practice. Journal and lock internals — 25 fields,
all counters except two windowed means. Mostly useful when diagnosing shared-storage
coordination; see [SHARED_STORAGE.md](SHARED_STORAGE.md).

- **Locks**: `lock_acquisitions_successful`, `lock_acquisitions_failed`,
  `lock_timeouts_total`, `stale_locks_detected`, `stale_locks_broken`,
  `average_lock_hold_time_ms` (windowed)
- **Corruption**: `metadata_corruption_detected`, `journal_corruption_detected`,
  `range_file_corruption_detected`
- **Recovery**: `orphaned_ranges_detected`, `orphaned_ranges_recovered`,
  `orphaned_ranges_cleaned`, `metadata_recovery_attempts`, `metadata_recovery_successes`,
  `journal_consolidation_attempts`, `journal_consolidation_successes`,
  `average_recovery_duration_ms` (windowed)
- **Journal**: `journal_entries_written`, `journal_entries_consolidated`,
  `journal_cleanup_operations`, `journal_validation_failures`
- **Write mode**: `immediate_writes`, `journal_only_writes`, `hybrid_writes`,
  `write_mode_fallbacks`

---

## `eviction_coordination`

Nullable in type, always present in practice. All counters:
`lock_acquisitions_successful`, `lock_acquisitions_failed`, `stale_locks_recovered`,
`evictions_coordinated`, `evictions_skipped_lock_held`, and `total_lock_hold_time_ms`
— which is a cumulative **sum**, not a mean.

---

## `signed_put`

Nullable in type, always present in practice.

`cached_puts_total`, `bypassed_puts_total`, `cache_failures_total` (counters);
`average_cached_bytes`, `average_streaming_duration_ms` (windowed, integer means).

A climbing `bypassed_puts_total` means write-through caching is declining objects, usually
on `write_cache_max_object_size` or capacity. See
[CONFIGURATION.md — Write Cache Configuration](CONFIGURATION.md#write-cache-configuration).

---

## Config fields that do not affect this payload

`metrics.include_cache_stats`, `metrics.include_compression_stats`, and
`metrics.include_connection_stats` are **deprecated and inert**. They are parsed and a
startup warning is logged when set to `false`, but nothing reads them — every section is
always included. What actually determines whether a section appears is whether its source
component was wired in at startup.

`metrics.collection_interval` drives the background collection loop only. An HTTP scrape
always collects fresh regardless of it.
