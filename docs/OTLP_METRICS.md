# OTLP metrics export

Hybrid Cache for Amazon S3 can export its metrics over the OpenTelemetry Protocol (OTLP). This
page shows how to turn the export on and publish it to CloudWatch, Prometheus or an
OpenTelemetry Collector, and lists the exported instruments by family with each one's type. What
each figure means is in the [Metrics reference](METRICS_REFERENCE.md), which covers every
`/metrics` field, exported or not.

## Enabling OTLP metrics

Configure via file:

```yaml
metrics:
  enabled: true
  otlp:
    enabled: true
    endpoint: "http://localhost:4318"
    export_interval: "60s"
    timeout: "10s"
    compression: "none"  # Options: "none", "gzip"
    headers: {}          # Optional custom headers
```

Or via CLI:

```bash
s3-proxy --otlp-endpoint http://localhost:4318 --otlp-export-interval 60
```

An endpoint with no path, like the ones above, exports to `/v1/metrics`. Any other path is used
exactly as written, so a receiver on a custom path needs its full URL.

### Per-bucket counters (opt-in)

Set `metrics.otlp.per_bucket_enabled: true` to emit four additional `ObservableCounter<u64>` instruments (`s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, `s3proxy.put_requests`) with a `bucket` attribute and an optional `prefix` attribute. These mirror S3's named CloudWatch request metrics for direct comparison.

In-memory accounting is always active regardless of this flag; the flag only controls OTLP export. See [Per-bucket traffic metrics](METRICS.md) for the full per-bucket reference including `bytes_saved` (direct cache savings measurement, `/metrics` and dashboard only, not exported here) and cross-validation with S3 CloudWatch.

## Publishing metrics to observability backends

### CloudWatch

Use CloudWatch Agent v1.300048.0 or later, the first release with the `metrics_collected.otlp` receiver:

```json
{
  "metrics": {
    "metrics_collected": {
      "otlp": {
        "http_endpoint": "127.0.0.1:4318"
      }
    }
  }
}
```

### Prometheus

Use Prometheus v3.0 or later with OTLP receiver support:

```bash
prometheus \
  --config.file=prometheus.yml \
  --web.enable-otlp-receiver \
  --enable-feature=native-histograms,otlp-deltatocumulative
```

### OpenTelemetry Collector

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318

exporters:
  otlphttp:
    endpoint: http://prometheus:9090/api/v1/otlp

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [otlphttp]
```

## Available metrics

The tables below list the exported instruments by family. For what each figure means, and for
the `/metrics` fields that are not exported, see the [Metrics reference](METRICS_REFERENCE.md).
Most instruments carry the name of the `/metrics` field they export, so the name is also the
field's path on `/metrics`. The exceptions are figures the exporter renames or derives, such as
`write_cache.staged_bytes` and `cache_size.accounting_drift.*`.

Every metric carries the resource attributes `host.name`, `service.name`, `service.version`, and
`service.instance.id`. `host.name` and `service.instance.id` both carry the hostname; the latter is what
Prometheus's OTLP receiver uses to derive the `instance` label, so without it every proxy's
series collapse into one. Backends that key on `host.name` instead are unaffected either way.

Most metrics are gauges with no per-metric dimensions. Two groups are exceptions:

- The four per-bucket counters (`s3proxy.bytes_downloaded`, `s3proxy.bytes_uploaded`, `s3proxy.get_requests`, `s3proxy.put_requests`) are cumulative `ObservableCounter`s carrying a `bucket` attribute and an optional `prefix` attribute. Cardinality is capped by `metrics.per_bucket.max_series`, with overflow folded into a synthetic `__other__` bucket.
- `download_bandwidth.class_bytes` is a gauge carrying a `class` attribute. Classes over the tracking cap are reported under `class="residual"`.

### Cache / sizes

| Metric | Type | Description |
|--------|------|-------------|
| `cache.total_cache_size` | Gauge (bytes) | Bytes on the cache volume. Equal to `read + write_cache`, except while the write-cache figure is briefly above it, when `read` is held at its last consistent value; see [`total_cache_size`](METRICS_REFERENCE.md#sizes-and-rates). Fleet-wide |
| `cache.max_cache_size_limit` | Gauge (bytes) | Configured `cache.max_cache_size`. Divide `total_cache_size` by this for utilization. `0` means unlimited |
| `cache.read_cache_size` | Gauge (bytes) | Non-staged bytes: read at least once. Fleet-wide |
| `cache.read_cache_size_inverted_secs` | Gauge (seconds) | Seconds the staged figure has exceeded `cache.total_cache_size` without a break; `0` when it has not, and also `0` on a read where the fold's figure is unreadable. Non-zero means `cache.read_cache_size` is held at its last consistent value, or past about 87,300 s reported as `0`; alarm at 1,800 s (six passes of the 300 s staged fold). Restarts at `0` with the proxy; see [`read_cache_size_inverted_secs`](METRICS_REFERENCE.md#sizes-and-rates). Per-proxy |
| `cache.write_cache_size` | Gauge (bytes) | Staged bytes: written through, not yet read. Fleet-wide |
| `cache.ram_cache_size` | Gauge (bytes) | RAM cache size, counting copies of bytes also on disk. Per-proxy |

The two disk gauges are disjoint and partition the volume, so
`cache.total_cache_size = cache.read_cache_size + cache.write_cache_size` holds, except briefly after a same-key re-PUT or staged DELETE, when `cache.read_cache_size` is held at its last consistent value ([metrics reference](METRICS_REFERENCE.md)).
`cache.ram_cache_size` is not part of that sum: it counts copies of bytes already on disk, and
it is per-proxy where the other three are fleet-wide. Sum it across proxies for the fleet's RAM
residency. Read the other three from one proxy rather than summing them: they are already
fleet-wide, so a sum multiplies them by the proxy count.

That rule assumes the fleet shares one cache volume, which is what makes every proxy report the
same figure. When each proxy has its own local `cache_dir`, the three disk gauges are
per-proxy too, so the fleet total is their sum, and a dashboard built on this section reads low
by roughly the proxy count. See [What to alarm on](LOCAL_NVME_CACHE.md#what-to-alarm-on).

### Cache hits, misses and evictions

| Metric | Type | Description |
|--------|------|-------------|
| `cache.cache_hits` | Gauge | Total disk cache hits (GET) |
| `cache.cache_misses` | Gauge | Total disk cache misses (GET, forwarded to S3) |
| `cache.evictions` | Gauge | Disk cache evictions |
| `cache.s3_requests_saved` | Gauge | S3 requests avoided (disk hits + metadata hits) |
| `cache.bytes_served_from_cache` | Gauge (bytes) | S3 transfer bytes saved |

### RAM cache

| Metric | Type | Description |
|--------|------|-------------|
| `cache.ram_cache_hits` | Gauge | RAM range cache lookups that found the key, not bytes served. The etag comparison that can reject the entry as a stale version runs afterwards, so a rejected entry counts here too. See [RAM cache fields](METRICS_REFERENCE.md#ram-tier) |
| `cache.ram_cache_misses` | Gauge | RAM range cache misses |
| `cache.ram_cache_evictions` | Gauge | RAM range cache evictions |
| `cache.ram_cache_entries` | Gauge | Entries resident in the RAM cache (range plus whole-object), a level. Per-proxy |
| `cache.ram_cache_eligibility_skips_total` | Gauge | RAM admissions declined because the key's `ram_cache_eligible` resolved `false`. Per-proxy. See [RAM cache fields](METRICS_REFERENCE.md#ram-tier) |

### Metadata cache

| Metric | Type | Description |
|--------|------|-------------|
| `cache.metadata_cache_hits` | Gauge | Metadata RAM lookups that returned a copy to the caller, from any request type: the HEAD path, the mainline GET handler and the range paths all read this tier. The caller can still reject the entry and forward to S3, so this is a lookup count rather than a serve count. The HEAD path's own figures are exported separately, here and on `/metrics`, as `cache.metadata_cache_head_hits`, `cache.metadata_cache_head_misses` and `cache.metadata_cache_head_disk_hits` |
| `cache.metadata_cache_misses` | Gauge | Metadata RAM misses |
| `cache.metadata_cache_entries` | Gauge | Metadata entries currently in RAM |
| `cache.metadata_cache_evictions` | Gauge | Metadata entries evicted from RAM |
| `cache.metadata_cache_stale_refreshes` | Gauge | Expired RAM entries that required disk re-read |
| `cache.metadata_cache_disk_hits` | Gauge | Metadata documents read from the cache volume after a RAM miss, outside the unconditional HEAD path. Each cost a read from the cache volume. Per-proxy |

### Cache health and error signals

| Metric | Type | Description |
|--------|------|-------------|
| `cache.corruption_metadata_total` | Gauge | Corrupted metadata files detected |
| `cache.corruption_missing_range_total` | Gauge | Missing range files detected |
| `cache.disk_full_events_total` | Gauge | Cache writes refused with `ENOSPC`: range data files (whole or streamed), request-path `.meta` writes, and journal appends. Consolidator and orphan-sweep `.meta` writes, cache-hit journal flushes and accounting state files are not counted; see [ERROR_HANDLING.md: Disk Space Exhaustion](ERROR_HANDLING.md#3-disk-space-exhaustion) |
| `cache.metadata_lock_retry_exhausted_total` | Gauge | Callers that exhausted `lock_max_retries` waiting for a metadata slot lock and gave up. The retry count bounds the wait; the metadata lock protocol has no timeout |
| `cache.etag_mismatches_total` | Gauge | ETag mismatches (cache coherency violations) |
| `cache.range_invalidations_total` | Gauge | Range invalidations triggered by ETag mismatch |
| `cache.incomplete_uploads_evicted` | Gauge | Incomplete multipart uploads evicted from the write cache |
| `cache.consumed_bytes` | Gauge (bytes) | Real `statvfs`-derived bytes consumed on the cache volume's file system. Observability only, not a capacity authority. Per-proxy file system, `0` on a read failure. Compare against `cache.total_cache_size` yourself only with the scope mismatch in mind; see [Metrics reference](METRICS_REFERENCE.md) |
| `cache.capacity_guard.declines_total` | Gauge (count) | GET-miss fills and full-object stores declined because the cache volume was near `disk_safety_free_space_floor`. The client request is served either way. Per-proxy |
| `cache.capacity_guard.last_free_bytes` | Gauge (bytes) | Free space the capacity guard last read on the cache volume, taken only when a fill arrives. Not exported until the first fill after start. Per-proxy |
| `cache.capacity_guard.floor_bytes` | Gauge (bytes) | `disk_safety_free_space_floor` as configured; `0` means the guard is disabled |
| `cache.hostname_collisions_total` | Gauge (count) | Proxies found sharing this proxy's hostname on the cache volume, counted once each. Above zero is a deployment error: proxies sharing one volume must have distinct hostnames. Requests are served normally; cleanup of a stopped proxy's files falls back to age alone, and a proxy whose whole identity is shared also stops write-caching. Per-proxy |
| `cache.read_fills_declined_max_cache_size_total` | Gauge (count) | GET-miss fills served from S3 without being stored because the cache had reached `max_cache_size`. The client request is served either way. Per-proxy |
| `cache.eviction_oracle_refreshes_total` | Gauge (count) | Times a rebuilt cross-instance activity set replaced the one an eviction pass started with. Per-proxy |
| `cache.eviction_oracle_stale_verdicts_total` | Gauge (count) | Eviction candidates kept because the activity set was older than `ram_cache_flush_interval` while its rebuild was still running. Should stay `0`. Per-proxy |
| `cache.eviction_oracle_last_walk_ms` | Gauge (ms) | Duration of the most recent journal walk behind the activity set. Compare against half `ram_cache_flush_interval`. Not exported until a pass has built a set. Per-proxy |
| `cache.read_disabled_invalidations_total` | Gauge | Eager cache purges triggered by `read_cache_enabled=false` rules |
| `cache.ttl_revalidations_total` | Gauge | TTL-driven conditional revalidations against S3 |
| `cache.metadata_read_stale_handles_total` | Gauge | Stale file handle errors (`ESTALE`) observed while reading a `.meta`, including retried ones. Should read `0`. Per-proxy |
| `cache.metadata_read_stale_handle_misses_total` | Gauge | `.meta` reads a stale handle turned into an S3 fetch after retries. Should read `0`. Per-proxy |
| `cache.tee_abandoned_total_put` | Gauge | Streamed PUT and UploadPart bodies whose cache writer stalled for up to 5 s, less when the write's 1-second cache budget is spent; the upload went to S3 uncached. Per-proxy |
| `cache.volume_stall_breaker_open` | Gauge | `1` while this proxy treats its cache volume as stalled (see [Metrics reference](METRICS_REFERENCE.md)). Per-proxy |
| `cache.volume_stall_breaker_trips_total` | Gauge | Times this proxy has started treating its cache volume as stalled. Per-proxy |
| `cache.metadata_heal_overwrite_total` | Gauge | Unparseable `.meta` documents replaced by a fresh one fetched from S3. Read against `cache.corruption_metadata_total`. Per-proxy |
| `cache.upstream_idle_abort_total_mid_stream` | Gauge | GET-miss responses cut off because S3 sent no bytes for `upstream_idle_timeout` mid-body; each is a truncated client response. Per-proxy |
| `cache.upstream_idle_abort_total_pre_stream` | Gauge | GET and HEAD misses answered `504` after every attempt missed `upstream_first_byte_timeout`. Per-proxy |
| `cache.upstream_idle_retry_total` | Gauge | First-byte timeouts that were retried. Per-proxy |


### Cache rules reload health

| Metric | Type | Description |
|--------|------|-------------|
| `cache_rules.reloads_total` | Gauge | Successful `cache_rules.json` loads since startup |
| `cache_rules.reload_failures_total` | Gauge | Failed loads (parse, validation, compile errors) |
| `cache_rules.on_fallback` | Gauge (0/1) | 1 = running stale fallback rules (last load failed) |
| `cache_rules.rules_loaded` | Gauge | Number of rules currently active |

### Compression

| Metric | Type | Description |
|--------|------|-------------|
| `compression.average_compression_ratio` | Gauge | Average compression ratio |
| `compression.failures_total` | Gauge | Compression + decompression failures |

### Connection pool

No `connection_pool` figure is exported over OTLP. The pool's measured fields
(`dns_refresh_count`, `connections_created`, `connections_reused`, `max_lifetime_closures`,
`error_closures`) are on `/metrics` only, alongside the hardcoded placeholder `success_rate_percent` and the
always-zero `idle_timeout_closures`; see
[METRICS_REFERENCE.md](METRICS_REFERENCE.md#connection_pool). For request latency use
`request_metrics.average_response_time_ms` below, which is measured.

### Coalescing

| Metric | Type | Description |
|--------|------|-------------|
| `coalescing.waits_total` | Gauge | Requests that waited for an in-flight fetch |
| `coalescing.cache_hits_after_wait_total` | Gauge | Requests served from cache after waiting |
| `coalescing.timeouts_total` | Gauge | Coalescing wait timeouts |
| `coalescing.s3_fetches_saved_total` | Gauge | S3 fetches avoided by coalescing |
| `coalescing.average_wait_duration_ms` | Gauge (ms) | Average coalescing wait time |

### Request

| Metric | Type | Description |
|--------|------|-------------|
| `request_metrics.total_requests` | Gauge | Total HTTP requests processed |
| `request_metrics.successful_requests` | Gauge | Successful requests |
| `request_metrics.failed_requests` | Gauge | Failed requests |
| `request_metrics.access_log_records_dropped` | Gauge | Cumulative access-log records dropped since process start; see [Metrics reference](METRICS_REFERENCE.md) |
| `request_metrics.average_response_time_ms` | Gauge (ms) | Cumulative average response time |
| `request_metrics.active_requests` | Gauge | Active TCP connections, not requests in flight: with HTTP/1 keep-alive one connection serves many sequential requests. See [METRICS.md](METRICS.md#request-completion-metrics) |

### Process

| Metric | Type | Description |
|--------|------|-------------|
| `uptime_seconds` | Gauge (s) | Proxy uptime |
| `process.memory_usage_bytes` | Gauge (bytes) | RSS memory usage |

### Download bandwidth QoS

| Metric | Type | Description |
|--------|------|-------------|
| `download_bandwidth.instance_ceiling_bps` | Gauge (bytes/s) | This proxy's share of the configured origin download ceiling |
| `download_bandwidth.failopen_total` | Gauge (count) | Times the scheduler failed open, admitting traffic without shaping |
| `download_bandwidth.class_bytes` | Gauge (bytes), `class` attribute | Bytes attributed to the scheduling class. Classes beyond `max_tracked_classes` are folded into `class="residual"` |

See [Bandwidth QoS](BANDWIDTH_QOS.md) for what these mean and how the ceiling is
shared across a fleet.

### Staged write tier

| Metric | Type | Description |
|---|---|---|
| `write_cache.staged_range_count` | Gauge (count) | Write-cached ranges currently staged. Fleet-wide |
| `write_cache.staged_range_budget` | Gauge (count) | The fixed limit the count is compared against. Exported so a dashboard's denominator is data rather than a hardcoded number; there is no configuration field for it |
| `write_cache.staged_range_budget_percent` | Gauge (percent) | The count as a percentage of the limit. The write cache declines new objects at 100%, so this is a capacity signal |
| `write_cache.staged_range_budget_declines_total` | Gauge (count) | Admissions declined because the limit was reached. Per-proxy |
| `write_cache.staged_bytes` | Gauge (bytes) | Compressed on-disk bytes of staged objects. Fleet-wide |
| `cache.in_progress_part_bytes` | Gauge (bytes) | Compressed on-disk bytes held by multipart uploads not yet completed, from the collection's one walk of `mpus_in_progress/`. Fleet-wide |
| `cache.in_progress_part_bytes_age_secs` | Gauge (seconds) | Seconds since that walk began, at the collection that exported it. Not recorded when the figure is unknown. Per-proxy |
| `cache.in_progress_part_bytes_drift_bytes` | Gauge (bytes, signed) | The walk reconciled against what each proxy publishes about the parts it wrote: the composed figure at the instant the walk began, minus the walk. Negative means a proxy's counter missed an update and this proxy's readers walk the volume until the next collection. Per-proxy |
| `write_cache.staged_shard_authority_age_secs` | Gauge (seconds) | Age of the stalest shard contributing to the figure: the figure's real age |
| `write_cache.staged_authority_max_age_secs` | Gauge (seconds) | The target age the row above is judged against |
| `write_cache.staged_fold_shards_beyond_max_age` | Gauge (count) | Shards past that target |
| `write_cache.staged_fold_incomplete_total` | Gauge (count) | Recomputation passes that retained the previous figure because an input could not be read, or a multipart completion was mid-publish. Per-proxy |
| `write_cache.staged_fold_shard_budget_capped_total` | Gauge (count) | Passes whose per-pass shard budget was clipped. Non-zero means this proxy alone cannot revisit every shard inside the target age, because the shard count exceeds one proxy's reach. It depends on the shard count alone, so adding proxies does not stop it; whether the fleet keeps up is `write_cache.staged_fold_shards_beyond_max_age`. Per-proxy |
| `write_cache.staged_fold_dirty_shards` | Gauge (count) | Shards this proxy removed staged bytes from (DELETE, first read of a written-through object, overwrite, staging or capacity eviction) and has not yet re-folded. The next pass visits them: inside its shard budget as far as the target age allows, and beyond it for the rest, up to one budget's worth more per pass (fewer near the per-pass shard ceiling). Process memory, 0 after a restart. Per-proxy |
| `write_cache.staged_fold_shard_duration_count` | Gauge (count) | Shard visits recorded |
| `write_cache.staged_fold_shard_duration_sum_ms` | Gauge (milliseconds) | Summed visit duration, in milliseconds so sub-second visits do not sum to zero |
| `write_cache.staged_fold_shard_duration_bucket` | Gauge (count), `le` attribute | Visits per fixed duration class; `le="+Inf"` is the overflow class |
| `write_cache.setup_abandoned_total_put` | Gauge (count) | Streamed PUT and UploadPart write-cache setups abandoned, either because the setup did not finish within 5 s or because the request-body buffer filled and the setup still had not finished up to 500 ms later; the upload went to S3 uncached. Per-proxy |
| `write_cache.mpu_completion_ledger_declines_total` | Gauge (count) | Completed multipart uploads left uncached because a write-ledger record could not be written; the upload itself succeeded. Per-proxy |
| `write_cache.pending_lock_waits_total` | Gauge (count) | Pending-credit lock acquisitions that had to wait, counted per batch rather than per credit: a credit append takes the lock once per batch, so divide by `write_ledger.pending_batches_total` to read it as a fraction of holds. Per-proxy |
| `write_cache.pending_lock_wait_ms_total` | Gauge (milliseconds) | Summed wait for those locks, timeouts included. Per-proxy |
| `write_cache.pending_lock_timeouts_total` | Gauge (count) | Waits abandoned at their bound. On a read, each makes the staged range count unknown for that read, and the budget admits rather than declines. On a credit append, each loses up to 1,024 credit records whose ledger entries are durable, so the staged figure under-states those bytes until a staged-fold pass visits the shards the stage marked dirty. Per-proxy |
| `write_cache.staged_authority_epoch` | Gauge (0 or 1) | Which derivation the staged figure uses. Read it before comparing figures across proxies or across time |
| `write_cache.staged_authority_snapshot_hits_total` | Gauge (count) | Staged-figure reads served from the short-lived cache. Per-proxy |
| `write_cache.staged_authority_snapshot_recomputes_total` | Gauge (count) | Staged-figure reads that re-read shared-volume state. Per-proxy |
| `write_cache.ledger_repair_rotation_reachable` | Gauge (1 or 0) | Whether one proxy can complete the repair rotation at this shard population. `0` persists for that proxy; adding proxies spreads the rotation across them |
| `write_cache.ledger_repair_projected_rotation_secs` | Gauge (seconds) | Projected time for one full rotation; compare against 86,400 |
| `write_cache.ledger_repair_shard_budget_capped_total` | Gauge (count) | Repair passes whose shard budget was clipped. Per-proxy |
| `write_cache.ledger_repair_ranges_reappended_total` | Gauge (count) | Non-zero is a finding: staged ranges found on disk with no staging record. Per-proxy |
| `write_cache.scanner_concurrency_k_fold`, `write_cache.scanner_concurrency_k_repair` | Gauge (scanners) | Concurrency the next fold or repair pass respects. Per-proxy |
| `write_cache.scanner_concurrency_observed_fold`, `write_cache.scanner_concurrency_observed_repair` | Gauge (scanners) | Other proxies scanning at the gate's last check. Per-proxy |
| `write_cache.scanner_samples_discarded_total_fold`, `write_cache.scanner_samples_discarded_total_repair` | Gauge (count) | Visits left out of the rate estimate. Per-proxy |
| `write_cache.scan_deferred_total_fold`, `write_cache.scan_deferred_total_repair` | Gauge (count) | Deferred consolidation cycles, not passes. Per-proxy |
| `write_cache.scanner_last_adjust_reason_fold`, `write_cache.scanner_last_adjust_reason_repair` | Gauge (code) | `0` floor, `1` insufficient_samples, `2` hold, `3` probe, `4` argmax. Per-proxy |
| `write_cache.scanner_rate_by_level_fold`, `write_cache.scanner_rate_by_level_repair` | Gauge (files/s), `level` attribute | Smoothed per-visit read rate by level, at most 64 series. Per-proxy |
| `write_cache.scanner_samples_by_level_fold`, `write_cache.scanner_samples_by_level_repair` | Gauge (count), `level` attribute | Kept samples per level, at most 64 series. Per-proxy |
| `write_cache.scanner_aggregate_estimate_by_level_fold`, `write_cache.scanner_aggregate_estimate_by_level_repair` | Gauge (files/s), `level` attribute | Estimated fleet-wide read rate at each level: level times the per-visit rate. Only levels with at least 3 kept samples. Per-proxy |

`write_cache.ledger_repair_rotation_remedy` is a text field and is published on `/metrics` only.

A figure that is unknown is not recorded at all, rather than recorded as `0`. OTLP
gauges have no null, and zero is also the healthy value for most of these, so a zero would
publish the healthiest possible reading for the least healthy state. The series is absent only
while the figure has not been known since the proxy started. Under the exporter's default
cumulative temporality the OpenTelemetry SDK re-sends a gauge's last recorded value on every
export, so a figure that was known and later becomes unknown keeps reporting that last value, and
an alarm on absence never fires for it. Read the always-exported
`write_cache.staged_fold_incomplete_total` and `write_cache.staged_fold_shards_beyond_max_age`
beside these series, or set `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta`, under
which a gauge not recorded in an interval is left out of that export, at the cost of also
turning the four per-bucket counters into deltas, which not every backend accepts.
`ledger_repair_rotation_reachable` is the deliberate exception and is sent as `1`/`0`, because an
alarm on a permanent condition needs a value to compare.

See [Write cache fields](METRICS_REFERENCE.md#write_cache) for what each figure means and
[Staged write tier](DASHBOARD.md#staged-write-tier) for the dashboard card that shows them.

### Write-cache count eviction pass

The last count-driven write-cache eviction pass, recorded only once one has completed on the
volume. Fleet-wide, read from a shared document any proxy may write, so aggregate with last-value
or max, never sum. `passes_total`, `ranges_freed_total` and `admission_declines_total` are the
writing proxy's own counters and jump when a different proxy wins the eviction lock. `schema` and
`instance_id` are not exported. See
[The last count-driven write cache eviction pass](METRICS_REFERENCE.md#write_cachecount_pass_last_result--the-last-count-driven-write-cache-eviction-pass).

| Metric | Type | Description |
|---|---|---|
| `write_cache.count_pass_last_result.completed_at` | Gauge (unix seconds) | When the pass finished |
| `write_cache.count_pass_last_result.count_budget` | Gauge (ranges) | The staged range budget at pass time |
| `write_cache.count_pass_last_result.count_trigger` | Gauge (ranges) | The count the pass fires above |
| `write_cache.count_pass_last_result.count_target` | Gauge (ranges) | The count the pass tries to reach |
| `write_cache.count_pass_last_result.count_at_start` | Gauge (ranges) | Count the trigger fired on, net of this proxy's not-yet-folded reclaims |
| `write_cache.count_pass_last_result.observed_count_at_start` | Gauge (ranges) | Count before that adjustment |
| `write_cache.count_pass_last_result.ranges_requested` | Gauge (ranges) | What the pass set out to reclaim |
| `write_cache.count_pass_last_result.ranges_freed` | Gauge (ranges) | What it reclaimed |
| `write_cache.count_pass_last_result.shortfall_ranges` | Gauge (ranges) | `ranges_requested − ranges_freed`. `0` means the target was reached |
| `write_cache.count_pass_last_result.unevictable_unreadable_ranges` | Gauge (ranges) | Part of the shortfall: candidates whose `.meta` could not be read |
| `write_cache.count_pass_last_result.unexplained_shortfall_ranges` | Gauge (ranges) | The rest of the shortfall |
| `write_cache.count_pass_last_result.reached_target` | Gauge (1 or 0) | Whether the target was reached |
| `write_cache.count_pass_last_result.objects_evicted` | Gauge (count) | Objects evicted |
| `write_cache.count_pass_last_result.candidates_considered` | Gauge (count) | Candidates walked |
| `write_cache.count_pass_last_result.reclaimable_candidates` | Gauge (count) | Candidates that could be evicted. `0` means nothing was reclaimable |
| `write_cache.count_pass_last_result.expired_candidates` | Gauge (count) | Candidates older than `put_ttl` |
| `write_cache.count_pass_last_result.passes_total` | Gauge (count) | Count-driven passes the writing proxy has completed |
| `write_cache.count_pass_last_result.ranges_freed_total` | Gauge (ranges) | Ranges those passes reclaimed |
| `write_cache.count_pass_last_result.admission_declines_total` | Gauge (count) | The writing proxy's budget declines at pass time |
| `write_cache.count_pass_last_result.shortfall_reason` | Gauge (constant `1`), `reason` attribute | Why the pass ended where it did. Read the attribute, not the value |

### Cache size accounting drift

Recorded only once a size validation has installed a sample. Fleet-wide: every proxy exports the
same sample, so aggregate with last-value or max, never sum. The five per-population series carry
a `figure` attribute, one of `total_size`, `write_cache_size` or `cached_objects`, and each is
recorded only when its value is known. `schema` and `instance_id` are not exported. See
[How far the running size counters had drifted](METRICS_REFERENCE.md#cache_sizeaccounting_drift--how-far-the-running-size-counters-had-drifted).

| Metric | Type | Description |
|---|---|---|
| `cache_size.accounting_drift.sampled_at` | Gauge (unix seconds) | When the sample was taken. Alarm on its age |
| `cache_size.accounting_drift.sample_kind` | Gauge (constant `1`), `kind` attribute | Which validation produced the sample. Read the attribute, not the value |
| `cache_size.accounting_drift.persisted` | Gauge (bytes or objects), `figure` attribute | The running counter the validation compared against. For a full scan, the figure recorded just before the scan started |
| `cache_size.accounting_drift.reference` | Gauge (bytes or objects), `figure` attribute | The figure the validation computed. For a full scan, a census over the scan's duration |
| `cache_size.accounting_drift.drift` | Gauge (signed), `figure` attribute | `reference − persisted`. Negative means the counter was over-stating |
| `cache_size.accounting_drift.drift_ratio` | Gauge (ratio), `figure` attribute | `abs(drift) ÷ reference` |
| `cache_size.accounting_drift.unevaluable_reason` | Gauge (constant `1`), `figure` and `reason` attributes | Present when a drift could not be computed, naming why. Absent `drift` with this series present means "could not be compared", not "no drift" |

### Metrics collection

The cost of the background collection that records every instrument on this page. Per-proxy.

| Metric | Type | Description |
|---|---|---|
| `metrics_collection.collections_total` | Gauge (count) | Background collections completed since process start |
| `metrics_collection.duration_ms_last` | Gauge (milliseconds) | Duration of the most recent collection |
| `metrics_collection.duration_ms_max` | Gauge (milliseconds) | Longest collection since process start |
| `metrics_collection.slow_collections_total` | Gauge (count) | Collections longer than `slow_threshold_ms`. Non-zero means a collection used more than half its interval |
| `metrics_collection.slow_threshold_ms` | Gauge (milliseconds) | Half of `metrics.collection_interval` |

### Fill visibility

All gauges, per-proxy, recorded only when a cache manager is wired in. See
[Fill visibility fields](METRICS_REFERENCE.md#fill_visibility) for what each drop reason means.

| Metric | Type | Description |
|---|---|---|
| `fill_visibility.entries` | Gauge (count) | Streamed GET-miss fills held in memory until their `.meta` is folded |
| `fill_visibility.bytes` | Gauge (bytes) | Estimated memory those entries occupy, against a 16 MiB cap |
| `fill_visibility.recorded_total` | Gauge (count) | Fills recorded since process start |
| `fill_visibility.hits_total` | Gauge (count) | Responses authorized from a held record since process start |
| `fill_visibility.dropped_folded_total` | Gauge (count) | Entries dropped because this proxy folded the key's `.meta` |
| `fill_visibility.dropped_disk_meta_total` | Gauge (count) | Entries dropped because a `.meta` for the key was found |
| `fill_visibility.dropped_invalidated_total` | Gauge (count) | Entries dropped because the key was invalidated, on this proxy or by a write or delete acknowledged on a peer |
| `fill_visibility.dropped_cap_total` | Gauge (count) | Entries dropped because the 8,192-entry or 16 MiB cap was reached |
| `fill_visibility.dropped_age_total` | Gauge (count) | Entries dropped because they outlived `max(4 × consolidation_interval, 30 s)` |
| `fill_visibility.dropped_range_file_changed_total` | Gauge (count) | Entries dropped because the range file they name was missing or rewritten |

### Orphan reclamation

All gauges, per-proxy, always recorded. See
[Orphan reclamation fields](METRICS_REFERENCE.md#orphan_reclaim) for what each figure means.

| Metric | Type | Description |
|---|---|---|
| `orphan_reclaim.enabled` | Gauge (1 or 0) | Whether this proxy has started the orphan rotation: 1 once its cache is initialized |
| `orphan_reclaim.found` | Gauge (count) | Range files no `.meta` listed when scanned, since process start |
| `orphan_reclaim.reclaimed` | Gauge (count) | Candidates unlinked after `confirmed_absent` evidence |
| `orphan_reclaim.bytes_reclaimed` | Gauge (bytes) | On-disk size of the unlinked candidates. Not a reduction of the accounted cache size |
| `orphan_reclaim.skipped_unconfirmed` | Gauge (count) | Candidates kept on unconfirmed evidence or an error, because the file is less than 10 minutes old, because its object has a multipart upload in progress or the uploads in progress could not be read in full, because a different file replaced it after it was judged, or because a proxy that is not yet ready joined the fleet during the pass |
| `orphan_reclaim.kept_listed` | Gauge (count) | Candidates kept because the `.meta` listed them under the key's lock |
| `orphan_reclaim.journal_evidence_total` | Gauge (count), attribute `result` | One series per outcome: `confirmed_absent`, `journal_present`, `peer_unavailable`, `indeterminate`. All four are recorded every export |
| `orphan_reclaim.passes` | Gauge (count) | Rotation passes this proxy has run |
| `orphan_reclaim.passes_gated` | Gauge (count) | Passes skipped or stopped early because an active proxy is older than 3.0.0, no proxy has written a journal entry in the last hour, or the list of active proxies could not be read |
| `orphan_reclaim.leases_unreadable` | Gauge (count) | Reads of an orphan or eviction work-list lease file that did not parse |
| `orphan_reclaim.leases_unreadable_expired` | Gauge (count) | Of those, files older than 30 minutes, read as expired and removed |
| `orphan_reclaim.units_visited` | Gauge (count) | Rotation units this proxy claimed and visited |
| `orphan_reclaim.stalest_checkpoint_age_secs` | Gauge (seconds) | Age of the oldest completed unit visit. Not recorded before the first pass or while no unit has completed |
| `orphan_reclaim.units_never_scanned` | Gauge (count) | Units no rotation has completed yet. Not recorded before the first pass |

### Eviction work list

All gauges, per-proxy, always recorded. The three totals with an attribute record every
series every export, zero included. See
[Whether the eviction work list is running](METRICS_REFERENCE.md#eviction_worklist-whether-the-eviction-work-list-is-running)
for what each figure means and which to alarm on.

| Metric | Type | Description |
|---|---|---|
| `eviction_worklist.generation` | Gauge (count) | Newest work-list generation this proxy has seen |
| `eviction_worklist.chunks_authored_total` | Gauge (count) | Chunks this proxy wrote as eviction-lock holder |
| `eviction_worklist.chunks_claimed_total` | Gauge (count), attribute `outcome` | One series per outcome: `claimed`, `busy` |
| `eviction_worklist.chunks_done_total` | Gauge (count) | Chunks this proxy finished, counted at the done-marker |
| `eviction_worklist.chunks_unusable_total` | Gauge (count) | Chunks that read damaged or absent and were skipped |
| `eviction_worklist.items_executed_total` | Gauge (count) | Listed ranges this proxy ran the identity-checked delete for |
| `eviction_worklist.items_refused_total` | Gauge (count), attribute `reason` | One series per reason: `identity_changed`, `stale_generation` |
| `eviction_worklist.lease_takeovers_total` | Gauge (count) | Claims that replaced a lapsed lease |
| `eviction_worklist.author_fallback_total` | Gauge (count), attribute `reason` | Eviction triggers that ran the single-proxy pass instead of writing a work list. One series per reason: `not_capable`, `unknown`. Alarm on its fleet sum increasing |
| `eviction_worklist.in_flight_bytes` | Gauge (bytes) | Listed bytes not yet deleted, as this proxy last read them when authoring |

### Write ledger append stages

| Metric | Type | Description |
|---|---|---|
| `write_ledger.append_batches_total` | Gauge (count) | Batches the ledger-append stage drained from its queue, failed batches included. Per-proxy |
| `write_ledger.append_batched_entries_total` | Gauge (count) | Entries in those batches. Divide by `write_ledger.append_batches_total` for the mean batch size, which is 1 on an idle proxy and rises with the submission rate. Per-proxy |
| `write_ledger.pending_batches_total` | Gauge (count) | Batches the paired-credit stage drained, each one hold of the per-proxy pending-credit lock. Per-proxy |
| `write_ledger.pending_batch_failures_total` | Gauge (count) | Non-zero is a finding: credit batches whose write failed, each losing up to 1,024 records whose ledger entries are durable. The staged figure under-states those bytes until a staged-fold pass visits the shards the stage marked dirty. Per-proxy |

### Page cache

The page-aligned range caching figures are exported as eight gauges:
`page_cache.widened_requests`, `page_cache.bytes_prefetched`,
`page_cache.amplification_ratio`, `page_cache.page_hits`, `page_cache.skipped_signed_range`,
`page_cache.fallbacks`, `page_cache.ram_page_promotions` and
`page_cache.ram_page_promotion_skipped`. `page_cache.amplification_ratio` is recorded only once
a request has been widened, so the series has a gap until then rather than reading `1.0`. See
[Page cache fields](METRICS_REFERENCE.md#page_cache) and
[Page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching).

### Not exported via OTLP

Most of `/metrics` is exported. What is not divides into two cases: whole fields with no
instrument, and sections that are only partly exported.

**Whole fields with no instrument.** Scrape `/metrics` directly if you need these in a metrics
backend:

| Field | Where to read it instead |
|---|---|
| `write_cache.resident_bytes`, `staging_bound_bytes`, `inflight_bytes`, `over_bound`, `staged_entries`, `staging_evictions_total`, `staging_eviction_bytes_total` | [`write_cache`](METRICS_REFERENCE.md#write_cache) |
| `signed_put.skipped_puts_total` | [`skipped_puts_total`](METRICS_REFERENCE.md#skipped_puts_total) |
| `write_cache.ledger_entries`, `ledger_entries_age_secs`, `journal_unparseable_lines_retained` | [`write_cache`](METRICS_REFERENCE.md#write_cache) |
| `cache.origin_marker_mismatch_bypasses_total`, `origin_marker_unparseable_bypasses_total`, `origin_marker_unresolved_bypasses_total`, `uncacheable_aws_host_bypasses_total` | [Origin marker bypasses](METRICS_REFERENCE.md#origin-marker-bypasses) |
| `coalescing.fetcher_completions_success`, `fetcher_completions_error` | [`coalescing`](METRICS_REFERENCE.md#coalescing) |
| `cache.ram_cache_hit_rate_percent` | Derive it from `cache.ram_cache_hits` and `cache.ram_cache_misses`, both of which *are* exported |

**Some sections are partially exported.** `write_cache.ledger_repair_*` has instruments for the
rotation's scheduling and outcome signals (reachability, projected rotation time, budget
capping, re-appended ranges) but not for `ledger_repair_observed_shards` or
`ledger_repair_stalest_age_secs`. So a rotation-progress panel built on the exported subset can
show that the rotation is reachable without being able to show how far through it is, or how
stale the worst shard's sweep has become. Both of those come from `/metrics` only. Of `atomic_metadata`, the four
`metadata_lock_*` figures, `atomic_metadata.range_file_corruption_detected`,
`atomic_metadata.range_file_corruption_healed`,
`atomic_metadata.range_file_corruption_heal_suppressed`,
`atomic_metadata.range_file_identity_mismatch_detected` and
`atomic_metadata.journal_validation_failures` are exported, each a per-proxy gauge carrying a
count since process start; its other 8 fields are always zero and have no instrument. The
orphan reclamation rotation exports under `orphan_reclaim.*`.

`page_cache`, `inflight_memory`, `fill_visibility`, `consolidation`, `metrics_collection`,
`cache_hit_buffer` and `hedged_requests` are
exported field for field, and a test fails the build if a field is added to one of them without
an instrument. If a series from one of those is missing, the export is not the cause; check that
`metrics.otlp.enabled` is set and that the collector is receiving from this proxy.
`consolidation.consolidation_lock_held_secs` is per-proxy, unlike the other `consolidation.*`
series, and is recorded even before the first consolidation completes. Alert on it per proxy:
a value climbing past `consolidation_cycle_timeout` is a cycle stuck holding the global
consolidation lock. See [Consolidation fields](METRICS_REFERENCE.md#consolidation).

## Configuration options

The `metrics.otlp` fields besides `enabled` and `endpoint`, with their defaults. The
[Metrics configuration](CONFIGURATION.md#metrics-configuration) reference has every field.

| Field | Default | Meaning |
|---|---|---|
| `export_interval` | `60s` | How often metrics are exported |
| `timeout` | `10s` | Timeout for each export request |
| `compression` | `"none"` | `"none"` or `"gzip"` |
| `headers` | `{}` | HTTP headers sent with every export, for example `Authorization: "Bearer <token>"` |
| `per_bucket_enabled` | `false` | Export the four per-bucket counters; see [Per-bucket counters](#per-bucket-counters-opt-in) |

## Environment variables

- `OTLP_ENDPOINT`: the OTLP endpoint URL; setting it also enables the export
- `OTLP_EXPORT_INTERVAL`: Export interval in seconds
- `METRICS_ENABLED`: Enable/disable metrics collection

## Troubleshooting

Check logs for OTLP export errors:

```bash
grep "OTLP" /logs/app/$(hostname)/s3-proxy.log.$(date +%Y-%m-%d)
```

Verify CloudWatch Agent is configured for OTLP:

```bash
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
```
