# `/metrics` reference

This page lists every field that the `/metrics` endpoint of Hybrid Cache for Amazon S3
returns. Every field on the wire is named here or in [OTLP metrics](OTLP_METRICS.md), apart from
the `schema` format-version markers on persisted records. Where this page and the payload
disagree, the payload is authoritative. Behavior is explained on the pages each section links to.

Related pages:

- [Interpreting the metrics](METRICS.md): per-bucket traffic, cache savings, and the memory and permit counters
- [OTLP metrics](OTLP_METRICS.md): the instruments exported over OpenTelemetry
- [Dashboard](DASHBOARD.md): the browser view and its own `/api/*` endpoints
- [Metrics configuration](CONFIGURATION.md#metrics-configuration): the configuration fields

## The endpoint

```bash
curl -s http://<proxy>:9090/metrics
```

| Behavior | Detail |
|---|---|
| Success | `200`, `Content-Type: application/json` |
| Wrong path | `404`, empty body, no `Content-Type`. Only the exact configured `metrics.endpoint` is served |
| Non-GET on the right path | `405` with `Allow: GET`, empty body |
| Auth | **None.** Bind address defaults to `127.0.0.1`; set `metrics.bind_address: "0.0.0.0"` to scrape from another host |

Endpoint behavior:

**The JSON is pretty-printed.** The wire format is `"reserved_bytes": 0`, with a space
after the colon. Grep patterns like `'"reserved_bytes":[0-9]*'` match nothing, silently
yield empty, and typically get defaulted to `0` by the caller, which reads as a
confident, wrong measurement. Parse with a real JSON parser:

```bash
curl -s http://<proxy>:9090/metrics | python3 -c \
  'import sys,json; print(json.load(sys.stdin)["inflight_memory"]["reserved_bytes"])'
```

**Figures read from the cache volume come from a background collection.** A background task
collects every `metrics.collection_interval` (default 60 s). A scrape reads every in-process
counter and gauge live, but it does not traverse the cache volume: the figures whose only
source is such a traversal are served from the latest background collection. Those are the
`write_cache.staged_fold_*` / `staged_*` figures derived from the per-shard checkpoints, the
six volume-derived `write_cache.ledger_repair_*` figures (`rotation_complete`,
`stalest_age_secs`, `observed_shards`, `swept_shards`, `rotation_reachable`,
`projected_rotation_secs`), `write_cache.ledger_entries` with its age,
`cache.st_size_fold_oldest_shard_scan_age_secs`, and `cache.in_progress_part_bytes` with
`cache.in_progress_part_bytes_age_secs`. `volume_snapshot.collected_at_unix_secs` and
`volume_snapshot.age_secs` say when that collection ran. Both are `null` until the first
collection after startup completes, and until then those figures read `null` rather than
zero.

**The payload is all-or-nothing.** The body is one document serialized whole, so a response
that outruns the consumer's timeout delivers no body at all: not a partial body, and not
a section set to `null`. A scrape does not wait on the cache volume, so this needs a stalled
process rather than a slow volume. A slow volume shows instead as stale figures:
`volume_snapshot.age_secs` grows past `volume_snapshot.interval_secs`, and the age fields among
the background-derived figures keep counting up. `metrics_collection.duration_ms_last` and
`duration_ms_max` describe the background collection, and `slow_collections_total` counts runs
that took more than half the interval.

**OTLP exports on the background schedule.** The collection records the OTLP instruments, and
`metrics.otlp.export_interval` sends them, whether or not anything scrapes `/metrics`.

## Reading this reference

| Kind | Meaning |
|---|---|
| **counter** | Cumulative since process start. Resets to zero on restart. No reset API |
| **gauge** | Point-in-time |
| **derived** | Computed at serialization time. Formula given |
| **windowed** | Mean over a rolling buffer of the last 1000 samples. Moves down as conditions improve. The buffer is 1000 samples, not a time window, so the interval it covers varies with load |
| **config** | Echo of a configured value, not a measurement |

**Nullable sections.** Twelve sections serialize as `null` when their source component was not
wired in at startup: `cache`, `compression`, `connection_pool`, `eviction_coordination`,
`signed_put`, `cache_size`, `atomic_metadata`, `consolidation`, `cache_hit_buffer`,
`fill_visibility`, `coalescing`, and `cache_rules`. Handle null rather than assuming the key is an object. Every
other top-level key is always an object or a scalar.

**"Always present" describes the document, not the endpoint's availability.** A section
described as always present appears in every *delivered* body, but a scrape that outran its
timeout delivers no body at all (see "The payload is all-or-nothing" above). That loses every
section at once, which looks from the consumer's side like a feature that is not there. A
consumer that defaults a failed fetch to `{}` reports a missing feature when it saw a failed
scrape.

**Nullable objects one level down.** Three nested keys are also `null` until the document
behind them has been written, and they need the same guard:

- `cache.eviction_last_result`: until an eviction pass has completed on this volume.
- `write_cache.count_pass_last_result`: until a count-driven write-cache eviction pass has completed.
- `cache_size.accounting_drift`: until a validation has installed a sample.

Individual nullable *fields* are marked `gauge (nullable)` in the tables below.

> If a dashboard or alarm of yours treats `cache_size` as permanently absent, see
> [Upgrading to 3.0.0](UPGRADING.md#upgrading-to-300).

## Top-level shape

Keys appear in this order.

```text
timestamp, uptime_seconds, cache, compression, connection_pool,
eviction_coordination, signed_put, cache_size, atomic_metadata,
consolidation, cache_hit_buffer, fill_visibility, orphan_reclaim, eviction_worklist, coalescing, cache_rules,
page_cache, request_metrics, bucket_traffic, download_bandwidth, hedged_requests,
inflight_memory, write_cache, write_ledger, metrics_collection, volume_snapshot
```

**`timestamp`** is not a number and not RFC3339. It serializes as an object:

```json
"timestamp": { "secs_since_epoch": 1755608499, "nanos_since_epoch": 358795000 }
```

Note the inconsistency: `cache_size.last_checkpoint`, `cache_size.last_validation`,
`consolidation.last_consolidation_timestamp`, and `cache_rules.last_load_unix` are all
plain unix-second integers. Only the top-level `timestamp` uses the object form.

**`uptime_seconds`** (gauge): seconds since process start.

---

## Fields with misleading names

### `request_metrics.active_requests` counts TCP connections

Not requests in flight. With HTTP/1 keep-alive one connection serves many sequential
requests, so this does not track the quantity `max_concurrent_requests` bounds.

**Use `request_metrics.permits_held`** for in-flight requests.

### `cache.ram_cache_hit_rate_percent` is a percentage from `0` to `100`

It is computed only from RAM cache hit and miss counts, matching its sibling
`cache.cache_hit_rate_percent`. A dashboard that multiplies it by 100 reads 100 times too high;
if yours was built against a fractional value, see [Upgrading to 3.0.0](UPGRADING.md#upgrading-to-300).

To derive the same rate independently:

```text
ram_hit_rate = cache.ram_cache_hits / (cache.ram_cache_hits + cache.ram_cache_misses)
```

A lookup that finds the key counts as a hit even when the caller then rejects the entry as a
stale version, so this is a consultation-success rate rather than a serve rate. See
[RAM cache](#ram-tier).

### `connection_pool.success_rate_percent` is a hardcoded placeholder

`success_rate_percent` always reads `1.0`. Nothing measures it. Do not alert on it:
`success_rate_percent: 1.0` looks like a 1% success rate on a percent-scaled panel, and means
"not implemented".

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

Every `average_*` and `*_duration_ms` field is a scalar mean. The one histogram in the payload
is `write_cache.staged_fold_shard_duration_secs`, which publishes fixed class counts and a sum,
not percentiles. Some source comments call other fields histograms; they are means.

### No HEAD-scoped RAM hit rate exists

`cache.ram_cache_hit_rate_percent` covers every request shape together, and there is no
HEAD-specific equivalent. For the metadata path, which is what a HEAD reads, use
`cache.metadata_cache_head_hits` and `cache.metadata_cache_head_misses`: they count HEADs only.
`cache.metadata_cache_hits` counts metadata lookups from every request type.

### Two counters cover RAM admissions a cache rule declines

A RAM admission declined because `ram_cache_eligible` is `false` for the key is counted in
`cache.ram_cache_eligibility_skips_total`, except on the page-widening path. There it is counted
in `page_cache.ram_page_promotion_skipped`, which also counts pages over the RAM budget, pages of an object with no known ETag or length, and every page when `cache.ram_cache_enabled` is `false`. Read both
before concluding that a rule caused a low RAM hit rate on a prefix; see [RAM cache](#ram-tier).

---

## `cache`

Nullable. Sizes, hit counters, and error counters for the disk cache and the RAM cache.

### Sizes and rates

| Field | Kind | Meaning | Scope |
|---|---|---|---|
| `total_cache_size` | gauge | Bytes on the cache volume, over both populations the cache occupies: published ranges plus in-progress multipart parts. Equal to `read_cache_size + write_cache_size`, except while `write_cache_size` is briefly above it (see the note below the table), when `read_cache_size` is held at its last consistent value | fleet-wide |
| `max_cache_size_limit` | gauge | Configured `cache.max_cache_size`, the figure `total_cache_size` is a fraction of. `0` means no limit is configured | this proxy's configuration |
| `read_cache_size` | gauge | Non-staged bytes among the published ranges: objects that have been read at least once | fleet-wide |
| `read_cache_size_inverted_secs` | gauge | Seconds the staged figure has exceeded `total_cache_size` without a break; `0` when it has not, `1` in an excess's first second. While it is non-zero and at most about 87,300 s, `read_cache_size` is held at its last consistent value (see the note below the table); past that the proxy logs a WARN and reports `read_cache_size` as `0`, and this keeps counting. The excess after a same-key re-PUT or staged DELETE usually ends at the next fold pass over the written shard, so an alarm on this gauge with a threshold of 1,800 s (six passes of the 300 s staged fold) catches a real accounting inversion the proxy is still holding. It can also fire on a designed excess in a shard the fold has not revisited yet. Kept in the proxy's memory, so it restarts at `0` when the proxy restarts. Always `0` at `write_cache.staged_authority_epoch` `0`, where an inversion logs a WARN at once. Also reads `0` on any read where the fold's figure is unreadable and the proxy falls back to its running counter; the next readable fold figure resumes the full age, so an alarm that needs consecutive datapoints restarts its count at that dip | **this proxy only** |
| `write_cache_size` | gauge | Staged bytes: written through the cache and not yet read, plus the bytes of multipart parts currently in flight | fleet-wide |
| `ram_cache_size` | gauge | Current RAM range-cache bytes, counting promoted copies of bytes that are also on disk | **this proxy only** |
| `in_progress_part_bytes` | gauge (nullable) | **`null` means UNREADABLE**: the `mpus_in_progress/` traversal failed, or an individual `part{N}.json` record could not be read or parsed. Neither is the same observation as "no upload is in flight", and neither may be read as `0`, because under-reporting this figure delays reclaiming space. A directory that does not exist is a genuine `0`. It is also `null` until the first background collection after startup completes. Otherwise: **compressed on-disk bytes** occupied by multipart-upload parts staged under `mpus_in_progress/` and not yet completed. Summed from each `part{N}.json` record's `compressed_size`, which is the length of the part file as it sits on the volume, not the uncompressed object bytes the part contributes. Background-derived: the collector walks `mpus_in_progress/` once per `metrics.collection_interval` and a request serves that walk's figure, so read it with `in_progress_part_bytes_age_secs` | fleet-wide |
| `in_progress_part_bytes_age_secs` | gauge (nullable) | Seconds since the walk of `mpus_in_progress/` that produced `in_progress_part_bytes` began, at the moment of the response. In the steady state it stays below one `metrics.collection_interval` plus one collection's duration; a larger figure means the collector has stalled. `null` exactly when `in_progress_part_bytes` is | **this proxy only** |
| `in_progress_part_bytes_drift_bytes` | gauge (nullable, signed) | How far the per-proxy published in-progress figures stood from the truth at the last collection: the figure a reader would have composed at the instant this collection's walk began, minus that walk's own figure. Zero or positive in health, because a composed figure counts a removal its base walk has not seen yet. **Negative means some proxy's counter has missed an update**, and every reader on this proxy falls back to walking `mpus_in_progress/` until the next collection replaces the verdict, which costs volume I/O rather than accuracy. `null` while either term is unreadable, and until this proxy has made two walks | **this proxy only** |

> **`in_progress_part_bytes` is a term in `total_cache_size` and in `write_cache_size` while
> `write_cache.staged_authority_epoch` reads `1`.** It counts a different population,
> `mpus_in_progress/` rather than `ranges/`, so it is disjoint from `read_cache_size` and
> adding it does not double-count. Do not add it to `total_cache_size` yourself; at epoch `1`
> it is already included.
>
> At epoch `1` the `in_progress_part_bytes` field and the term inside the two composites are
> produced differently. The field is the background collection's walk. The term is that walk
> **composed** with what each proxy publishes about the parts it has written, so it also counts
> parts recorded since the walk began, and it accepts a walk up to three
> `metrics.collection_interval`s old. Subtracting the field from a composite therefore leaves
> the parts recorded between the walk and the read, and the term is the larger of the two
> whenever an upload is in flight. Read `in_progress_part_bytes_drift_bytes` beside them: it is
> how far the published figures stood from the walk at the last collection.
>
> The unit is compressed bytes, measured by `stat` on the part file when the part is
> finalized. An operator comparing it against the object sizes a client is uploading will
> see a smaller figure whenever compression applies, and that is correct: the volume holds
> the compressed bytes.
>
> It falls to zero for an upload when that upload completes or aborts. A figure that stays
> elevated after a completion means a crash left orphaned part records behind; those are
> reclaimed on `cache.incomplete_upload_ttl`, so the recovery bound is that configured
> TTL.

### Size fold figures

The size fold is the maintenance pass that re-grounds the per-shard byte aggregate behind
`total_cache_size` by listing and stat-ing every range file. See
[Size fold cadence](CONFIGURATION.md#size-fold-cadence) for what it does and what it costs.

| Field | Kind | Meaning | Scope |
|---|---|---|---|
| `st_size_fold_last_duration_secs` | gauge | Seconds the last completed pass this proxy ran took. `0` before one completes, and also `0` for a pass that finished inside a second: whole seconds only, so a zero is not evidence that nothing ran. A proxy whose peers always fold first never runs a pass of its own and reads `0` indefinitely; read `st_size_fold_oldest_shard_scan_age_secs` for the volume | **this proxy only** |
| `st_size_folds_declined_overlap_total` | counter | Scheduling ticks declined because a pass was already running. **Non-zero is not an error**: it is the never-overlap rule working, and it says the interval is shorter than this volume can sustain | **this proxy only** |
| `st_size_fold_oldest_shard_scan_age_secs` | gauge (nullable) | `max` over shards of `now − ` the shard's own scan timestamp, seconds: the age of the stalest shard contributing to the aggregate. **`null` means UNREADABLE or INCOMPLETE**, never zero: a shard's record could not be read, no shard has been folded yet, or at least one shard has no scan timestamp. A maximum over a subset of shards is indistinguishable from a fresher cache | fleet-wide |

> **Read the decline counter first when the aggregate looks stale.** This pass's
> characteristic failure is silent: a tick that finds a pass already running is a normal
> outcome rather than an error, it does not consume the interval, and it logs only at debug
> level. So a deployment where the pass never once completes looks identical on every other
> signal to one where it completes on schedule. The figures on disk are older, and
> nothing else says why. A decline count that keeps climbing while the scan age also climbs is
> that state.
>
> The age is a maximum, not a mean, for the same reason the write cache's own age gauge is:
> an aggregate is only as fresh as its stalest component, and a mean hides exactly the shard
> that is not being visited.
>
> The two counters are per-proxy, so a zero on one proxy says nothing about the fleet. The
> age comes from the cache volume and reads the same on every proxy that can read it.

### The last fleet-wide eviction pass (`cache.eviction_last_result`)

`null` until a pass has completed on this volume. Read from
`metadata/_shard_state/eviction_last_result.json`, so it is fleet-wide: any proxy may
win the eviction lock, and this reports whichever one last ran a pass rather than only this
process's activity.

| Field | Type | Meaning |
|---|---|---|
| `instance_id` | string | The proxy that ran the pass (it held the fleet-wide eviction lock) |
| `completed_at` | unix seconds | When the pass finished |
| `bytes_requested` | gauge | `current_size − target_size`: what the pass set out to free |
| `bytes_freed` | gauge | In `legacy` mode, what the pass actually freed. In `authored` mode, what it listed for executors toward `bytes_requested`; those bytes leave the disk as executors run, so the figure is not yet freed |
| `shortfall_bytes` | gauge | `bytes_requested − bytes_freed`, saturating. In `legacy` mode, `0` means the target was reached. In `authored` mode it is the part of `bytes_requested` the pass could not list for executors, and `0` means the work list covers the target, not that the bytes have left the disk |
| `unevictable_in_progress_bytes` | gauge (nullable) | **A named component of `shortfall_bytes`**: the part of it attributable to multipart parts staged under `mpus_in_progress/`, which eviction cannot free at any candidate count because its candidates are published ranges. `min(P, shortfall_bytes)`, so it is genuinely part of the total; the raw unclamped figure is `cache.in_progress_part_bytes`. **`null` means UNREADABLE**, inherited from that gauge |
| `unexplained_shortfall_bytes` | gauge (nullable) | The rest of the shortfall: in `legacy` mode, bytes eviction could in principle have freed and did not; in `authored` mode, bytes the selection could in principle have listed and did not. `null` when the component above is unreadable, because a residual computed against an unknown term is not a residual |
| `reached_target` | bool | In `legacy` mode, whether the pass met its target. In `authored` mode, whether the work list covers the target; the listed bytes leave the disk as executors run, and the next pass's `bytes_requested` shows what is still resident |
| `ranges_evicted` | gauge | Ranges removed, for context on whether the pass did any work at all. In `authored` mode, ranges listed for executors |
| `mode` | string | Which pass wrote the document. `legacy`: the eviction-lock holder selected and deleted. `authored`: the lock holder wrote a work list for every proxy's executors and deleted nothing itself. That happens whenever at least one proxy has written a journal entry in the last hour and every such proxy supports the work list. See [Which proxy deletes](EVICTION.md#which-proxy-deletes). A document written by a build without the field reads as `legacy` |
| `duration_ms` | gauge (nullable), milliseconds | How long the pass was actively running: from the point it had a non-zero byte target through to this document being built. **`null` means NOT MEASURED, not instant**: a document written by a build without the field reads this way. A rate consumer must skip such a sample rather than divide by a denominator it never observed. **Includes pre-eviction journal consolidation** when that was not skipped, because that consolidation is work the pass does before it can select candidates: this is the pass's cost, not the executor's inner loop in isolation. **Not `eviction_coordination.total_lock_hold_time_ms`**, which is cumulative across passes, includes lock acquisition, and keeps counting for a pass that found nothing to free |

> **Why the decomposition exists.** In-progress part bytes are inside the figure the
> eviction trigger is measured against, which means a trigger can fire on bytes no evictor
> can reclaim. That is a deliberate trade, and it is only safe because this decomposition
> makes it diagnosable:
>
> ```
> shortfall_bytes == unevictable_in_progress_bytes + unexplained_shortfall_bytes
> ```
>
> A pass reporting `bytes_freed: 0` with the whole shortfall in the in-progress component
> did everything it could: wait for the uploads to complete, or lower
> `cache.incomplete_upload_ttl`. The same `bytes_freed: 0` with the whole shortfall
> unexplained means the candidate set was exhausted or eviction is not finding what it
> should, which is a different problem with a different fix. The total alone cannot tell
> them apart.

> **How these three figures are derived depends on `write_cache.staged_authority_epoch`.**
> Names, units and scopes are the same at either epoch:
>
> | Figure | Epoch `0` (the default) | Epoch `1` |
> |---|---|---|
> | `total_cache_size` | the persisted byte total | that total + `in_progress_part_bytes` |
> | `read_cache_size` | total − the staged counter | total − the fold's staged range bytes |
> | `write_cache_size` | the event-sourced staged counter | **recomputed** from `.meta` ground truth: staged range bytes + un-folded credits + `in_progress_part_bytes` |
>

> **Read the epoch before comparing these figures across proxies or across time.** At
> activation `write_cache_size` steps once to the fold's figure. The step is usually down,
> sometimes by a large factor, because the event-sourced counter's known error is
> over-statement; it can also be up. Either way nothing was evicted or lost: the figure is
> recomputed from `.meta` ground truth.
>
> The identity `total_cache_size = read_cache_size + write_cache_size` holds in
> both columns for every value of
> `in_progress_part_bytes`. It is arithmetic rather than a coincidence of the current
> state: `(total − staged_ranges) + (staged_ranges + parts) = total + parts`. A dashboard
> that relies on it needs no in-flight-upload caveat. It has one exception: after a same-key
> re-PUT or a staged DELETE, the staged figure can exceed `total_cache_size` until the fold next
> visits that shard (at most about 87,300 s). While it does, `read_cache_size` is held at its last
> consistent value, the excess is logged at `debug!`, `read_cache_size_inverted_secs` reports how long it has lasted, and the identity does not hold. An object
> moves from `write_cache_size` to `read_cache_size` when it is first read, with no change
> to the total.
>
> `ram_cache_size` is outside that sum. It counts promoted copies of bytes that are also on
> disk and is per-proxy: sum it across proxies for fleet RAM residency, but never add
> it to the disk figures.
>
> For "how full is the cache", compare `total_cache_size` against the configured
> `cache.max_cache_size`. That is the pair every eviction and capacity decision uses,
> and what the `/health` cache component reports as a percentage.

### `eviction_worklist`: whether the eviction work list is running

A top-level section, always present. Every figure is per-proxy and counts since process
start, except `generation` and `in_flight_bytes`, which are gauges. The author figures move only
on the proxy that held the eviction lock when eviction triggered, so sum them across the fleet;
the executor figures move on every proxy. See
[Which proxy deletes](EVICTION.md#which-proxy-deletes) for the mechanism.

| Field | Type | Meaning |
|---|---|---|
| `eviction_worklist.generation` | gauge | The newest work-list generation this proxy has seen. `0` before any |
| `eviction_worklist.chunks_authored_total` | counter | Chunks this proxy wrote as eviction-lock holder, counted once the generation naming them is written |
| `eviction_worklist.chunks_claimed_total.claimed` | counter | Chunk leases this proxy claimed, fresh or taken over from a lapsed lease |
| `eviction_worklist.chunks_claimed_total.busy` | counter | Claims refused because a live peer holds the lease or the lease file could not be read or written |
| `eviction_worklist.chunks_done_total` | counter | Chunks this proxy finished: counted when the chunk's done-marker is in place, never at the claim |
| `eviction_worklist.chunks_unusable_total` | counter | Chunks that read damaged or absent and were skipped, deleting nothing. A chunk with an item missing its object identity is one of these |
| `eviction_worklist.items_executed_total` | counter | Listed ranges this proxy ran the identity-checked delete for. It counts attempts: a chunk the proxy restarts from its first item, after a failed flush, a lost lease or a takeover, counts its items again, so a fleet sum can exceed the listed items. An item whose `.meta` was removed since the list was written counts as executed |
| `eviction_worklist.items_refused_total.identity_changed` | counter | Listed ranges not deleted because the object was rewritten after the list was written, or a listed range became staged. Nothing of them was deleted or subtracted from the cache size |
| `eviction_worklist.items_refused_total.stale_generation` | counter | Listed ranges left because a newer generation was published before this proxy reached them |
| `eviction_worklist.lease_takeovers_total` | counter | Claims that replaced a lapsed lease: a peer stopped or ran out of lease before finishing its chunk. A takeover of an expired lease file that cannot be parsed is not counted |
| `eviction_worklist.author_fallback_total.not_capable` | counter | Eviction triggers on which this proxy held the lock and ran the single-proxy pass because some proxy that wrote a journal in the last hour has not published work-list readiness |
| `eviction_worklist.author_fallback_total.unknown` | counter | The same, because the list of active proxies or a readiness record could not be read, or because no proxy has written a journal entry in the last hour. A sustained rise is worth an alert only while the fleet serves traffic and every proxy supports the work list; see [During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade) |
| `eviction_worklist.in_flight_bytes` | gauge, bytes | Listed bytes not yet deleted, as this proxy last read them when authoring. Only the lock holder updates it |

**Alarm on the fleet sum of `author_fallback_total` (both reasons) increasing.** It is the only
figure that moves on the branch where the work list is skipped. An eviction trigger on a lock
holder that supports the work list, and skips it, moves the figure once, even when the
single-proxy pass then defers to chunks still in flight. A lock holder without work-list support
moves nothing. A trigger on which the author itself fails runs neither the work list nor the
single-proxy pass and moves no figure here; for that, alarm on `eviction_backlog_projected_bytes`
and `eviction_keeping_pace` below. `cache.eviction_last_result.mode` shows only the
last pass. For a stalled work list, compare `chunks_claimed_total.claimed` with
`chunks_done_total` across the fleet: claims rising while done stays flat, with
`lease_takeovers_total` rising, means chunks are being claimed and not finished.

### Eviction rate, admission rate, and whether eviction is keeping pace

Five cumulative counters, four rates derived from them, one verdict, and two backlog figures.
Every field here is per-proxy except the two backlog fields; see their rows.

| Field | Kind | Meaning | Scope |
|---|---|---|---|
| `eviction_shed_bytes_total` | counter | Accounted-payload bytes freed by the read-cache eviction passes this process completed. The figure `eviction_last_result.bytes_freed` reports for the last pass, accumulated | **this proxy only** |
| `eviction_shed_ranges_total` | counter | Ranges removed by those passes. **Ranges, not entries**: one entry is a `.meta` plus N ranges | **this proxy only** |
| `eviction_active_micros_total` | counter, microseconds | Summed active duration of those passes, and the shed rate's denominator. Idle time between passes is excluded deliberately. See the note below | **this proxy only** |
| `eviction_shed_bytes_per_sec` | derived (nullable) | `eviction_shed_bytes_total ÷ eviction_active_micros_total`. **`null` means no pass has completed in this process**, which is not zero: zero is a real reading, a pass that ran and freed nothing | **this proxy only** |
| `eviction_shed_ranges_per_sec` | derived (nullable) | The same quotient in ranges per second. `null` on the same condition | **this proxy only** |
| `admission_bytes_total` | counter | Accounted-payload bytes admitted to the cache by this process: one range's `compressed_size` for each range that entered the accounted total. It counts every path that credits the total: a read-through store, an incremental range commit, a PUT stored in the write cache, and multipart completion. It excludes deduplicated writes (another writer already holds those bytes on this volume), writes of a range that already existed on the cache volume, and RAM cache promotions. It also excludes multipart parts staged under `mpus_in_progress/`, which are `in_progress_part_bytes` | **this proxy only** |
| `admission_ranges_total` | counter | Ranges admitted, same population. Ranges, not entries | **this proxy only** |
| `admission_bytes_per_sec` | derived (nullable) | `admission_bytes_total ÷ uptime_seconds`. Wall-clock, unlike the shed rate. See the note below. `null` only inside the first second of process life | **this proxy only** |
| `admission_ranges_per_sec` | derived (nullable) | The same quotient in ranges per second | **this proxy only** |
| `eviction_keeping_pace` | derived (nullable), bool | Whether the shed rate is at least the admission rate in both units. Both, because a cache can keep pace in bytes while falling behind in ranges, and the two have different fixes (mean object size versus the range count). **`null` when any of the four rates is unknown.** This is the field to alarm on, so `false` means "measured, and falling behind" and never "not measured yet" | **this proxy only** |
| `eviction_backlog_bytes` | derived (nullable), signed | `total_cache_size − (max_cache_size × eviction_trigger_percent ÷ 100)`. The resident figure: negative means the bytes on the volume are under the trigger by that much, positive means they are over it. Negative here does not mean no pass is due, because the trigger compares the projected figure in the next row. `null` when `max_cache_size` is `0`, i.e. eviction is disabled. Not derivable without this field, because `eviction_trigger_percent` is published nowhere else | **mixed**: the size is fleet-wide, the threshold is this proxy's configuration |
| `eviction_backlog_projected_bytes` | derived (nullable), signed | The same subtraction against the figure the trigger compares: the recorded size plus every size change the fleet has recorded and no consolidation cycle has applied, minus the trigger threshold. **This is the field to alarm on**: positive means a pass is due or running. `null` when `max_cache_size` is `0`, and also before this proxy has evaluated the trigger even once, which is a third state and not a backlog of zero. As fresh as this proxy's last evaluation: one `cache.shared_storage.consolidation_interval` on the proxy holding the consolidation lock, and the last eviction pass on a proxy that holds neither. Read it on the proxy that is evicting, as `eviction_keeping_pace` also asks | **mixed**: the size is fleet-wide, the threshold is this proxy's configuration |

> **The two denominators differ on purpose.** The shed rate divides by summed active pass
> time. The admission rate divides by wall-clock uptime.
>
> Eviction is a discrete trigger-then-idle process, and its idle time is the absence of demand
> rather than a lower rate: a pass that reaches its target and stops is working correctly. A
> shed rate over wall-clock would read that idle time as falling behind and set
> `eviction_keeping_pace` to `false` on a cache that is keeping pace comfortably. Admission has
> no such distinction, because the proxy is always available to admit, so a quiet minute
> genuinely is a lower ingest rate.
>
> So `eviction_keeping_pace` compares eviction's capability against the average load it
> has to absorb, which is the property that decides whether the cache is bounded. If eviction
> drains faster than bytes arrive whenever it runs, a rising duty cycle absorbs the rest.
>
> **Why not wall-clock on both sides.** The verdict would reduce to "has the cache grown since
> this process started", which is `false` for hours on a healthy cold deployment filling toward
> its trigger. That is a worse false alarm, because it fires on every restart.

> The counters are what make a fleet reading possible. Every rate here is per-proxy, and
> any proxy may win the eviction lock. A proxy that never wins it reports
> `eviction_shed_bytes_per_sec: null` and `eviction_keeping_pace: null`, which is honest: it
> cannot answer the question. A proxy that evicts but takes few writes can report
> `eviction_keeping_pace: true` while the fleet as a whole falls behind.
>
> For the fleet verdict, sum `eviction_shed_bytes_total`, `eviction_active_micros_total`,
> `admission_bytes_total` and the two range counters across every proxy's `/metrics`, then
> divide. Differencing the counters across two scrapes also gives a true windowed rate, which
> the since-start quotients cannot: they never decay, so a regression an hour ago is diluted by
> every healthy hour before it.

> These rates measure this cache, on this storage type, at this workload. Eviction throughput
> differs by more than an order of magnitude between storage types, so no published figure
> predicts this one.

### What an eviction pass deleted

These fields describe the executor's work and the cross-proxy activity check it runs before
each delete. All of them are per-proxy and reset to zero on restart.

| Field | Kind | Meaning |
|---|---|---|
| `eviction_passes_total` | counter | Read-cache eviction passes this process completed. **Read this one first.** Every counter below is meaningless until it is non-zero: on a proxy that has run no pass, a zero says nothing ran rather than nothing was found |
| `eviction_pass_path_whole_object_total` | counter | Objects removed with a single `.meta` unlink and no rewrite. The cheap path |
| `eviction_pass_path_partial_total` | counter | Objects left partially resident, each costing one `.meta` rewrite. The expensive path |
| `eviction_whole_object_downgraded_total` | counter | Whole-object removals that became partial because a `.bin` survived the unlink. Sustained non-zero means the cheap path is being paid for at the expensive path's price |
| `eviction_unlink_failures_total` | counter | `.bin` unlinks that returned an error with the file still on the volume. **Non-zero is a finding**: those bytes are still consuming the volume while no longer being accounted for, so tracked size drifts below real consumption |
| `eviction_meta_rewrites_total` | counter | `.meta` documents the executor rewrote. The mechanism figure behind the partial path's cost |
| `eviction_active_key_journal_reads_total` | counter | Journal files the cross-proxy activity check read. Read it as a ratio against `eviction_passes_total`, never on its own, because the absolute value scales with how many passes ran |
| `eviction_oracle_refreshes_total` | counter | Times a rebuilt cross-proxy activity set replaced the one a pass started with. The set is rebuilt every half `ram_cache_flush_interval` while a pass runs. Counts byte eviction and entry-budget reclaim passes alike |
| `eviction_oracle_stale_verdicts_total` | counter | Candidates treated as active, and so not evicted, because the activity set in use was older than `ram_cache_flush_interval` while its rebuild was still walking the journals. Should stay `0`. A rising figure means the journal walk takes longer than half the flush interval, and eviction frees nothing until each rebuild lands |
| `eviction_oracle_last_walk_ms` | gauge (nullable), milliseconds | How long the most recent journal walk behind the activity set took. Compare it against half `ram_cache_flush_interval` (5000 ms at the 10 s default). `null` until a pass has built a set in this process |

The ratio of `eviction_pass_path_partial_total` to
`eviction_pass_path_whole_object_total` predicts a pass's duration. A fleet whose
evictions are mostly partial is doing a rewrite per object, and the fix is the workload's range
shape rather than any eviction setting.

### How the pass chose what to delete

Selection reads a bounded prefix of each partition rather than the whole population, so these
fields describe whether that prefix was large enough to find the target.

| Field | Kind | Meaning |
|---|---|---|
| `tail_floor_bytes` | gauge (bytes) | The size below which a range is not a selection candidate |
| `tail_max_entries` | gauge (count) | The per-partition cap on how far into a partition selection reads |
| `tail_rebuilds_total` | counter | Partition rebuilds this process performed |
| `tail_entries_last_rebuild` | gauge (count) | Entries the last rebuild wrote |
| `tail_effective_min_bytes` | gauge (bytes, nullable) | The smallest compressed size in the partition the last rebuild wrote: that shard's real threshold for being a selection candidate. It equals `tail_floor_bytes` or sits just above it while the cap has room, and rises above it when `tail_max_entries` is binding. One shard's figure, from this process's most recent rebuild, not a minimum across shards. `null` before a rebuild, or when the last partition written had no members |
| `tail_delta_records` | gauge (count) | Records currently held in the tail delta |
| `tail_delta_cap_rewrites_total` | counter | Whole-file delta rewrites forced by the delta's own cap |
| `tail_coverage_ratio` | gauge (**nullable**) | Bytes held by the capped prefix, over twice the maximum single-pass target. At or above `1.0` the prefix covers twice what one pass can need. See the caveat below |
| `tail_coverage_short_total` | counter | Times the proxy logged that the prefix was short of that target |
| `eviction_fallback_tail_empty_total` | counter | Passes that fell back because no partition holds a candidate above the floor |
| `eviction_fallback_cap_exhausted_total` | counter | Passes that fell back because the prefix hit the cap with the target still unmet. The signal to raise `tail_max_entries` or lower `tail_floor_bytes` |
| `eviction_fallback_unvisited_total` | counter | Passes that fell back because no partition file exists yet |
| `eviction_oversized_single_candidate_total` | counter | Passes whose selected set overshot the target by one candidate |
| `eviction_tail_verdict_total` | map, reason → counter | Why candidates were accepted or rejected, tallied by reason |

> **`tail_coverage_ratio` is as current as the last selection pass, not as now.** It is computed
> only where a cross-partition figure already exists, which is inside a pass. So it is `null`
> until one has run (`null` here means not yet measured, and a genuine ratio of zero is a real
> and serious reading that must stay distinguishable from it), and on a cache under no pressure
> it reports whatever the last pass saw, however long ago. That is the reading least likely to be
> current exactly when an operator would most like advance warning. Between passes, the
> per-partition entry counts give an estimate.

### Volume consumption versus accounted payload

| Field | Kind | Meaning | Scope |
|---|---|---|---|
| `consumed_bytes` | gauge | Real bytes consumed on the cache volume's file system, read with `statvfs` (total space minus available space). `0` on a read failure (fail-open; in the JSON alone this is indistinguishable from an empty volume) | this proxy's view of the file system |

> **`consumed_bytes` is observability only, not the capacity authority.** `max_cache_size`,
> `eviction_trigger_percent`, and every admission/eviction decision are derived exclusively from
> `total_cache_size` (`Σ compressed_size`, above), never from `consumed_bytes`. On a file system that
> deduplicates, snapshots or shares blocks (VAST always, ZFS and FSx for OpenZFS with snapshots, NetApp
> with deduplication), the `statvfs` figure does not fall reliably when the proxy deletes files, so a
> control loop built on it can livelock, deleting without the figure moving.
>
> `consumed_bytes` also covers the whole file system, not one directory: on a cache volume that also
> holds logs, journals and the write ledger, it attributes those bytes to the cache.
> [Running a fleet on one cache volume](SHARED_STORAGE.md) recommends a volume dedicated to the cache.
>

> **Comparing this against `total_cache_size` yourself mixes scopes and inputs of different
> reliability.** `consumed_bytes` is per-proxy and covers the whole file system; `total_cache_size` is
> fleet-wide and cache-scoped, and is itself maintained by a delta path re-grounded only by the
> periodic validation scan. A divergence between the two therefore has at least six causes
> (allocation granularity, per-object metadata/directory overhead, orphaned `.bin` files,
> staged multipart parts under `mpus_in_progress/`, a cache volume shared with
> non-cache data, and drift in `total_cache_size` itself), and no single number can attribute it to
> any one of them. Prefer a reading taken shortly after a completed full scan
> (`cache_size.last_validation`) if you do compute the difference.

### Free-space floor guard (`cache.capacity_guard`)

The guard that declines cache fills near a full volume (see
[Free-space floor and the danger band](CONFIGURATION.md#free-space-floor-and-the-danger-band)).
A nested object, per-proxy, reset on restart.

| Field | Kind | Meaning |
|---|---|---|
| `cache.capacity_guard.declines_total` | counter | GET-miss fills and full-object stores this process declined to cache. Either the volume was inside the danger band and the fill would have taken free space below `disk_safety_free_space_floor`, or the fleet's outstanding reservations could not be read. The client request is served either way. Write-cache PUT refusals are counted separately, in `signed_put.skipped_puts_total` under `disk_safety` |
| `cache.capacity_guard.last_free_bytes` | gauge (nullable), bytes | The free space the guard most recently read on the cache volume. The guard reads only when a fill arrives, so this can be old on an idle proxy. `null` until the first fill after start, not zero |
| `cache.capacity_guard.floor_bytes` | gauge, bytes | `disk_safety_free_space_floor` as configured. `0` means the guard is disabled. The danger band starts at four times this figure plus the incoming fill's size |

### The `max_cache_size` soft stop on fills

| Field | Kind | Meaning |
|---|---|---|
| `cache.hostname_collisions_total` | counter | Times this proxy has detected another proxy sharing its hostname on this cache volume. Proxies sharing one volume must have distinct hostnames: every instance id on the volume is `hostname:pid`, and each proxy decides whether another process of its own host has stopped by testing that process id locally. Anything above zero is a deployment error, not a transient: see [Each proxy needs its own hostname](SHARED_STORAGE.md#each-proxy-needs-its-own-hostname). Requests are served normally. When the other proxy has a different process id, no cached data or figure is wrong. The cleanup shortcuts that rest on the process-id test turn off for the life of the process, so files left by a stopped proxy are removed on their age alone. When the other proxy holds this one's whole identity (the same hostname and the same process id), this proxy also stops caching writes, counted as `hostname_not_unique` below. Ranges it staged before the detection can then keep duplicate sequence numbers and understate the staged figure until the hostnames are made distinct. It counts one per colliding proxy, not one per check, so it stops rising once each has been seen. The first detection also logs one ERROR naming both identities. Per-proxy, reset on restart |
| `cache.read_fills_declined_max_cache_size_total` | counter | GET-miss fills this process served from S3 without storing, because the cache had already reached `max_cache_size`. Per-proxy, reset on restart. The client's response is unaffected. A rising figure means eviction is not keeping pace with miss ingest, not that anything failed; it stops rising on its own once the total falls back below the limit. The figure it reads is the fleet's cache total, refreshed every 5 seconds, plus what this proxy has admitted since a consolidation cycle last folded its own additions into that total. A proxy therefore decides against a figure that accounts for its own fills. Those admissions are counted without netting the evictions the same proxy made in the same window. So a cache sitting at `max_cache_size`, with eviction keeping pace, can decline some fills it has room for until the next consolidation cycle folds both into the total. A record the proxy renamed aside as corrupt, or retired at its 256-record cap, can keep the over-decline going for longer. The cache stays within `max_cache_size` plus the fills in flight on each proxy plus what each other proxy has admitted, at its own offered (attempted) rate, over the age of the figure it reads. That age is the 5-second refresh plus the elapsed time of one consolidation cycle: close to `cache.shared_storage.consolidation_interval` on healthy storage, and several times longer when admission alone saturates a small file system. Eviction reclaims the overshoot. Write-cache declines against the same limit are counted in `signed_put.skipped_puts_total` under `disk_safety`; free-space declines are `cache.capacity_guard.declines_total` |

### Hit rates and counts

| Field | Kind | Meaning |
|---|---|---|
| `cache_hit_rate_percent` | derived | `cache_hits / (cache_hits + cache_misses) * 100`. `0.0` when no lookups |
| `ram_cache_hit_rate_percent` | derived | RAM cache hit rate, `0` to `100`. `0.0` when the RAM cache is disabled. See the note above on its scale |
| `total_requests` | derived | `cache_hits + cache_misses`. Lookups, not HTTP requests |
| `cache_hits` | counter | Cache lookups served from cache |
| `cache_misses` | counter | Cache lookups that had to fetch |
| `bytes_served_from_cache` | counter | Bytes served from cache, i.e. S3 transfer avoided |
| `evictions` | counter | Entries evicted |

### Range and metadata behavior

| Field | Kind | Meaning |
|---|---|---|
| `incomplete_range_fallbacks` | counter | Range responses that refetched from S3 because cached extents did not cover the request. Above zero means the proxy correctly avoided serving an incomplete range; it is not a hit |
| `range_file_count` | gauge | Range files present |
| `range_load_duration_ms` | windowed | Mean range-load time |
| `metadata_parse_duration_ms` | windowed | Mean `.meta` parse time |
| `metadata_file_size_bytes` | gauge | Last observed `.meta` size |
| `ttl_revalidations_total` | counter | Conditional GET/HEAD sent to S3 before serving cached bytes. Two causes, not one: TTL expiry, and a write-cached entry whose `Last-Modified` is not yet known. The second revalidates while the entry is still fresh by TTL, so this counter moves on the first read of every written object even under a generous `get_ttl`; see [Cache freshness](CACHE_FRESHNESS.md) |
| `read_cache_disabled_invalidations_total` | counter | Cached entries invalidated eagerly because a rule sets `read_cache_enabled: false` |

### Error and integrity counters

All counters. `corruption_metadata_total`, `corruption_missing_range_total`,
`disk_full_events_total`, `metadata_lock_retry_exhausted_total`,
`cache_cleanup_failures_total`, `cache_etag_validations_total`,
`cache_etag_mismatches_total`, `cache_range_invalidations_total`,
`cache_orphaned_ranges_cleaned_total`.

Three of these are always zero, and are documented here because the fields are published
rather than because they carry a signal:
- `cache_cleanup_failures_total`: nothing writes it.
- `cache_orphaned_ranges_cleaned_total`: nothing writes it.
- `cache_etag_validations_total`: nothing writes it. Note `cache_etag_mismatches_total` and
  `cache_range_invalidations_total` **are** written on the range-serve path, so a mismatch is
  observable even though the validation count is not.

`disk_full_events_total` is a process-wide counter of cache writes refused with `ENOSPC`. It
counts range data files, request-path `.meta` writes and journal appends, and not every write
to the cache volume; [Disk space exhaustion](ERROR_HANDLING.md#3-disk-space-exhaustion)
lists both sets. Alert on free space as well as on this counter.

### Metadata heals and upstream stalls

All counters, per-proxy, exported over OTLP as `cache.*` series.

| Field | Meaning |
|---|---|
| `metadata_heal_overwrite_total` | Unparseable `.meta` documents replaced by a fresh one fetched from S3: a full-object fill, a zero-length fill or a range store over a document that did not parse. Read it against `corruption_metadata_total`: detections that are never followed by a fill of the same key stay unhealed, and every request for such a key is served from S3 until one arrives |
| `upstream_idle_abort_total_mid_stream` | GET-miss responses cut off because S3 sent no bytes for `connection_pool.upstream_idle_timeout` part-way through the body. Each one is a truncated client response the client has to retry |
| `upstream_idle_abort_total_pre_stream` | GET and HEAD misses answered `504` because every attempt missed `connection_pool.upstream_first_byte_timeout` |
| `upstream_idle_retry_total` | First-byte timeouts that were retried, at most `connection_pool.upstream_idle_retries` per request. Retries with few pre-stream aborts mean the retry is absorbing the stalls |

### Cache-tee backpressure

`tee_backpressure_events_total_get` / `tee_backpressure_wait_ms_total_get` (GET-miss
path) and `tee_backpressure_events_total_put` / `tee_backpressure_wait_ms_total_put`
(signed PUT and UploadPart streaming path). Process-wide counters. On a GET miss, or a
signed PUT or UploadPart, the body streams to the client or to S3. It is copied to the
cache writer at the same time, through a small bounded channel. When that
channel is full, the stream applies backpressure. On the GET path this slows the client's
download to the disk write rate rather than dropping the frame or the cache write.

A non-zero `*_events_total` means the cache writer, not S3, was the bottleneck for at least
one request. On a fleet that shares a cache volume, it is the signal that the volume is too
slow for the miss and write rate and is holding back client transfers.
[Running a fleet on one cache volume](SHARED_STORAGE.md) covers the diagnosis. On a
[local NVMe cache](LOCAL_NVME_CACHE.md) the cache write goes to a local disk over PCIe
rather than across the network.

`tee_abandoned_total_put` (counter, per-proxy) is where the PUT-path backpressure ends.
It counts streamed PUT and UploadPart bodies whose cache writer did not accept a frame within
up to 5 s, less when the write's 1-second cache budget is spent. The proxy then stops caching that object, discards the partial cache entry, and keeps
streaming the body to S3, so the upload succeeds uncached. Each abandonment also logs a WARN
naming the cache key. Read it beside `tee_backpressure_wait_ms_total_put`: rising waits with no
abandonments mean the volume is slow but keeping up, and abandonments mean it stalled. Its
counterpart for a cache setup that did not finish is `write_cache.setup_abandoned_total_put`.

`cache_bypasses_by_reason` is a map of reason string to count. The reasons include `sse-c`,
`no-cache directive`, `no-store directive` and `read_cache_disabled`, the reasons for a HEAD on a
metadata or list operation, and these:

- `part-scoped-head`: a `HEAD` naming a single part, which is always forwarded.
- `response_header_override`: a GET or HEAD with a `response-*` query override, always forwarded.
- `multi_range`: a `GET` whose `Range` names more than one range, forwarded to S3 uncached.
- `unrecognised_query`: a GET or HEAD forwarded uncached because a query parameter is outside the
  object-read allowlist.
- `invalid_part_number`: a `partNumber` that is not an integer from 1 to 10,000, forwarded so S3
  returns its own error.
- `volume_stalled`: a GET or HEAD forwarded uncached while this proxy treated the cache volume as
  stalled. A `Cache-Control: no-cache` request is counted under its no-cache reason instead.
- `write_stamp_pending`: a GET or HEAD forwarded uncached because this proxy acknowledged a write
  of the key and the write's invalidation has not landed yet. Once this proxy can no longer track
  such keys one by one, every key of a cache directory counts here.

A `CreateMultipartUpload` that uses SSE-C counts under `sse-c`; see
[Operations that bypass the cache](CACHING.md#operations-that-bypass-cache).
[Error handling](ERROR_HANDLING.md) explains each failure class.

### Directory-entry type resolution

`dirent_type_resolution_failures_total` (counter, entries) counts directory entries inside
a cache shard whose type the cache-scanning and ledger-repair traversals could not
determine at all. **Per-proxy.**

**Non-zero is a finding, not a routine reading**, and it should read zero on a healthy
volume. These traversals classify each entry from the type the directory read already
returned, rather than paying a separate file system lookup per entry. This counts the
entries where that failed *and* the fallback lookup failed too, so the traversal admitted
the entry rather than risk skipping a real directory. A rising count points at the volume
or the mount rather than at the proxy.

It is not a measure of how often the fallback lookup was needed. That figure is not
observable through the standard library, which performs the fallback internally and does
not report whether it did, so a fallback that succeeded is indistinguishable from an
entry classified for free.

### Stale file handles on metadata reads

Two counters, both per-proxy, both covering `.meta` reads only: not range-file reads and
not metadata writes.

| Field | Kind | Meaning |
|---|---|---|
| `metadata_read_stale_handles_total` | counter | Stale file handle errors (`ESTALE`) observed while reading a `.meta`, once per observation. A read whose first two attempts fail this way and whose third succeeds adds `2` |
| `metadata_read_stale_handle_misses_total` | counter | `.meta` reads abandoned because of a stale file handle and treated as a cache miss, so the object was fetched from S3. Once per read: a `stat` that fails this way, or a read still failing after three retries |

**Both should read zero on a healthy volume.** A stale handle means the NFS client held a handle
to a `.meta` the server no longer recognizes, typically because another proxy replaced the file
between this proxy's open and its read. Occasional ones are absorbed by retrying, at up to
300 ms of blocking time each. The misses are the subset that retrying did not absorb, and each is
an S3 request the cache would otherwise have served. A sustained rate points at the cache volume
or its mount options rather than at the proxy.

### Origin marker bypasses

The first request for a bucket records the S3 origin it came from in that bucket's `_origin.json`
marker on the cache volume. A later request whose origin differs is forwarded to S3 without
caching, so a second origin that uses a bound bucket's name is not served the first origin's
entries. The binding does not cover every case; see
[One bucket name at two origins](CACHING.md#one-bucket-name-at-two-origins). Four counters, all per-proxy, each counting forwarded requests:

| Field | Kind | Meaning |
|---|---|---|
| `origin_marker_mismatch_bypasses_total` | counter | The request's origin differs from the one recorded in the marker. A single-origin deployment reads `0` forever. Non-zero means one bucket name is in use at two origins, and requests from the origin that did not claim the bucket are uncached. That is correct behavior and still a misconfiguration worth fixing, since it otherwise shows only as a lower hit rate |
| `origin_marker_unparseable_bypasses_total` | counter | A marker is present and could not be read. Neither reading of such a file is safe, so the proxy forwards uncached. Inspect the bucket's `_origin.json`; if it is corrupt, stop every proxy that shares the volume, remove the bucket's cached data as described in [Removing one bucket's cached data](CACHING.md#removing-one-buckets-cached-data), and start them again |
| `origin_marker_unresolved_bypasses_total` | counter | Reading or claiming the marker did not finish within 250 ms for a PUT, UploadPart or multipart request, or within 5 s for any other request. The cache volume was slow to answer a bucket's first request in this process, and those requests went to S3 uncached rather than waiting. A write counted here still invalidates the key's cached copy. Not a second origin |
| `uncacheable_aws_host_bypasses_total` | counter | The request's host is under an AWS partition domain but is not an S3 endpoint the proxy caches. Examples are an API Gateway or load balancer hostname, or a virtual-hosted form the cache key does not read the bucket from. Such requests are forwarded uncached and record no origin. Non-zero with no such traffic expected means a client is addressing S3 through one of these forms; [What counts as one origin](CACHING.md#what-counts-as-one-origin) lists them and the cacheable alternative |

### Write-order stamp retries

Every acknowledged PUT, CompleteMultipartUpload and DELETE writes the key's write-order file under
the key's metadata lock before it responds, so that an older write's background store cannot
publish over it. That write fails when the cache volume refuses the lock, when removing the key
from this proxy's RAM cache fails, or when the write does not finish within the write's cache
budget. The proxy then records the key in its local pending-stamp log (`logging.local_state_dir`)
before it responds, and retries the write in the background until it lands, including after a
restart. Until it lands, the object the write replaced or deleted can
still be served, and this proxy declines to store the key's older writes and GET or HEAD responses.
Eleven figures, all per-proxy:

| Field | Kind | Meaning |
|---|---|---|
| `cache.write_stamp_retried_total` | counter | Acknowledged writes whose write-order file could not be written and were handed to the background retry. Non-zero means the cache volume refused or timed out lock requests, or removing a key from this proxy's RAM cache failed |
| `cache.write_stamp_retry_landed_total` | counter | Keys whose write-order file the retry has since written. It moves once per key, and several failed writes of one key can share one retry, so it can stay below `write_stamp_retried_total` with nothing pending. Keys still pending are `write_stamp_retry_active_keys` plus `write_stamp_retry_overflow_keys` |
| `cache.write_stamp_retry_overflow_keys` | gauge | Keys waiting now in memory for one of the 4,096 retry workers. The queue holds up to 128 MiB of keys, about 500,000 keys of 100 bytes; keys past it wait in the local log only and are reloaded as the queue drains |
| `cache.write_stamp_retry_active_keys` | gauge | Keys a retry worker holds now, one per worker. Keys still pending a retry are this plus `write_stamp_retry_overflow_keys`. A worker that fails four times on one key while others are queued puts it back at the end of the queue and takes the next |
| `cache.write_stamp_retry_abandoned_total` | counter | Times an acknowledged write's stamp retry was abandoned: no worker, no queue room and no local record, or a retry worker that ended early with no record. Each early end counts, so one write can count more than once. **Non-zero is a finding**: the proxy that took each write forwards the key's GET and HEAD to S3 (bypass reason `write_stamp_pending`) until the retry queue has drained and its stamp lands. Until then other proxies can serve the object the write replaced or deleted, and after a restart of that proxy any proxy can serve it until its TTL |
| `cache.write_stamp_log_pending_keys` | gauge | Keys with a pending retry recorded in the local log now. Rises during a cache-volume outage and returns to `0` as the retries land |
| `cache.write_stamp_log_replayed_total` | counter | Pending retries read back from the local log at startup and resumed |
| `cache.write_stamp_log_torn_records_total` | counter | Unreadable lines skipped while reading the local log at startup, such as a final line cut short when the proxy stopped mid-write. A skipped line's retry is lost |
| `cache.write_stamp_log_write_failures_total` | counter | Failed writes whose record could not be written to the local log (its volume full or read-only, or the directory unusable at startup). **Non-zero is a finding**: those retries are held in memory only, and each failure logs an ERROR at most once a minute |
| `cache.write_stamp_log_memory_only` | gauge | `1` when this proxy's local log runs in memory only. That happens when `logging.local_state_dir` is inside `cache_dir`, is on a file system other than ext2/3/4, XFS, btrfs, tmpfs, overlayfs, ZFS or f2fs, or could not be created, locked or written at startup. The startup ERROR names the reason. **Non-zero is a finding**: pending retries are held in memory only, so a restart drops them |
| `cache.write_stamp_pending_declined_total` | counter | Background stores and GET or HEAD responses this proxy did not cache because it held a pending retry for their key |
| `cache.volume_stall_breaker_open` | gauge | `1` while this proxy treats its cache volume as stalled: three write responses had a bounded cache wait time out within 10 seconds, with those writes' cache waits not all starting together. Until three probes of the volume in a row succeed, writes skip the cache with no added delay, and GET and HEAD are forwarded to S3 uncached. The exception is a HEAD answered from metadata this proxy loaded into memory within `metadata_cache.refresh_interval`. Per-proxy |
| `cache.volume_stall_breaker_trips_total` | counter | Times this proxy has started treating its cache volume as stalled. Each start logs a WARN and each recovery an INFO, at most one of each a minute. Per-proxy |

### Part cache (multipart)

Counters: `cache_part_hits`, `cache_part_misses`, `cache_part_stores`,
`cache_part_evictions`, `cache_part_errors`. See
[Multipart upload caching](MULTIPART_UPLOAD.md).

### Write cache

`incomplete_uploads_evicted` (counter).

### RAM tier

`ram_cache_hits`, `ram_cache_misses`, `ram_cache_evictions` (counters);
`ram_cache_max_size` (config).
`ram_cache_eligibility_skips_total` (counter) counts RAM admissions this proxy declined because
the key's resolved `ram_cache_eligible` was `false`: a disk-hit promotion, a full-object store, a
write-cache store, or a streamed disk-serve promotion. Page-widening promotions are counted
separately, by `page_cache.ram_page_promotion_skipped`. The RAM lookup itself is not gated on
eligibility, so a closed gate still records misses and produces a low `ram_cache_hit_rate_percent`.
A rising skip count tells that case apart from a RAM cache that admits entries and still misses.
`ram_cache_entries` (gauge) is the number of entries resident in this proxy's RAM cache now,
summed across shards: range entries (one per cached extent) plus whole-object entries. Divide
`ram_cache_size` by it for the mean entry size. Invalidation removes entries without moving
`ram_cache_evictions`; this gauge falls when it does.

`ram_cache_hits` counts lookups that found the key in a shard, not bytes served. The etag
comparison that can reject the entry as a stale version runs in the caller, after this counter
has already moved, so a rejected entry is indistinguishable here from one that was served.
Read the derived `ram_cache_hit_rate_percent` as a consultation-success rate on that basis.

### Metadata cache tier

`metadata_cache_hits`, `metadata_cache_misses`, `metadata_cache_evictions`,
`metadata_cache_stale_refreshes` (counters); `metadata_cache_entries` (gauge);
`metadata_cache_max_entries` (config).

`metadata_cache_head_hits`, `metadata_cache_head_misses` and `metadata_cache_head_disk_hits`
(counters, per-proxy) count the same tier for HEAD requests only. A head disk hit is a RAM
miss found on the cache volume. They are raw counters: divide them yourself over a window.

`metadata_cache_disk_hits` (counter, per-proxy) is the non-HEAD counterpart: metadata documents
read from the cache volume after a RAM miss, on the GET, range and part-scoped serve paths. The
conditional-request check that runs before a request is split into GET or HEAD also counts here,
so a conditional HEAD can move it; an unconditional HEAD never does. Each one cost a cache-volume
read where a RAM hit cost none. Read it beside `metadata_cache_misses`, which counts RAM misses
from every request type.

---

## `compression`

Nullable. `total_objects_compressed`, `total_objects_uncompressed`, `total_bytes_before`,
`total_bytes_after`, `compression_failures`, `decompression_failures` (counters), and
`average_compression_ratio` (derived).

`total_objects_uncompressed` counts store-mode writes: framed and checksummed but not
block-compressed, which is what a denylisted extension produces. See
[Compression](COMPRESSION.md).

---

## `connection_pool`

Nullable. **`success_rate_percent` is a hardcoded placeholder** (see above).

| Field | Kind | Meaning |
|---|---|---|
| `dns_refresh_count` | counter | DNS refresh cycles |
| `ip_addresses` | gauge | Flat list of every resolved upstream IP across all endpoints. Endpoint attribution is lost and duplicates are possible |
| `connections_created` | counter map | New upstream connections, keyed by host |
| `connections_reused` | counter map | Keep-alive reuses, keyed by host |
| `idle_timeout_closures` | counter | **Always zero.** Idle reaping happens inside hyper's connection pool, which tells the proxy nothing when it drops an idle connection, so there is no point at which this could be counted |
| `max_lifetime_closures` | counter | Connections retired for reaching `connection_pool.max_lifetime`. Retirement takes the connection out of the pool at checkout; a request already running on it is unaffected |
| `error_closures` | counter | Closed on error |

Reuse rate is the useful derived figure:

```text
reuse_rate(host) = connections_reused[host] / (connections_created[host] + connections_reused[host])
```

A low reuse rate with keepalive enabled points at `max_lifetime` or `idle_timeout` being
too short. Which of them it is cannot be read off these counters. Take the rates of
`max_lifetime_closures` and `error_closures` from two scrapes minutes apart, and compare each
against the number of connections created in the same window. Either one climbing at about one
closure per created connection is retiring connections as fast as the pool can replace them. `idle_timeout_closures` cannot contribute to that comparison, so
idle-timeout churn cannot be separated from age retirement or from error churn.
A short `idle_timeout` shows up only indirectly, as connections created faster than
`max_lifetime_closures` and `error_closures` together account for.

Before acting on a per-host reuse rate, check that the two maps carry the same key set. They
are written by different code paths, so one endpoint's connections can be filed under two
keys and leave the rate computed over part of its traffic.
[Connection pooling](CONNECTION_POOLING.md) gives the signature to look for.

---

## `request_metrics`

Always present. Fixed cardinality: the proxy does not label these by URL, bucket, key,
client address, or host, so the payload stays bounded under a high-cardinality workload.

| Field | Kind | Meaning |
|---|---|---|
| `total_requests` | counter | Completions, including sheds |
| `successful_requests` | counter | 2xx |
| `failed_requests` | counter | Every non-2xx, including 3xx |
| `client_error_requests` | counter | 4xx |
| `server_error_requests` | counter | 5xx |
| `rejected_requests` | counter | `503 SlowDown` sheds, all causes |
| `cache_hit_requests` | counter | GET/HEAD that carried `X-Cache: HIT` |
| `cache_miss_requests` | counter | GET/HEAD that did not |
| `average_response_time_ms` | derived | `total_response_time_ms / total_requests`, integer division |
| `requests_per_second` | derived | **Lifetime average since process start, not a rolling window.** Flattens over long uptimes and will badly under-report a recent burst |
| `active_requests` | gauge | **TCP connections; see above** |
| `max_concurrent_requests` | config | Configured limit |
| `permits_total` | config | Same value, `permits_*`-named |
| `permits_held` | derived gauge | `permits_total - permits_available`. **The real in-flight-request gauge** |
| `permits_available` | gauge | Free permits |
| `permits_held_peak` | high-water | Process-lifetime maximum of `permits_held` |
| `access_log_records_dropped` | counter | Access-log records not written because the logger stayed busy for 30 s, or 10,000 records were already waiting for it. Access logs are best-effort; above zero means records were lost |

### Attributing a shed

`rejected_requests` counts every shed cause together. There is no dedicated counter for
concurrency sheds, so derive it:

```text
concurrency_sheds = request_metrics.rejected_requests - inflight_memory.rejected_total
```

Size `max_concurrent_requests` from measured `permits_held_peak` under representative
load, not from request rate. See
[Max concurrent requests](CONFIGURATION.md#max-concurrent-requests).

---

## `inflight_memory`

Always present, including when disabled: `ceiling_bytes: 0` means the ledger is off, so
`/metrics` alone confirms whether the feature is active.

| Field | Kind | Meaning |
|---|---|---|
| `reserved_bytes` | gauge | Bytes currently claimed by buffering paths |
| `ceiling_bytes` | config | `server.max_inflight_buffer_bytes`. **`0` = disabled** |
| `peak_reserved_bytes` | high-water | Maximum `reserved_bytes` since start |
| `rejected_total` | counter | Sheds attributable to this ledger only. Always `<= request_metrics.rejected_requests` |
| `aborted_accumulations_total` | counter | Unknown-size accumulations aborted mid-stream at the ceiling |
| `granted_total` | counter | Grants since start: each admitted reservation and each successful growth of one. Take a delta of this, not a sample of `reserved_bytes`, to prove a path reserved nothing: a reservation shorter than the sampling interval still moves it. `0` while disabled |
| `granted_bytes_total` | counter | Bytes granted by those grants |
| `ram_cache_max_bytes` | config | `cache.max_ram_cache_size`, echoed so the two memory budgets read side by side |

`ceiling_bytes: 0` cannot distinguish "configured off" from "not wired in": both produce
zero.

**`reserved_bytes` is not a resident-memory figure**, and it does not cover the streaming
or RAM-hit paths. [What `reserved_bytes` covers](METRICS.md#what-reserved_bytes-covers-and-what-it-does-not)
has the sizing guidance; read it before setting a ceiling from this number.

### Buffered serves and the 8 MiB materialization bound

A cached read that assembles bytes into one buffer before answering is a buffered serve.
One such buffer may hold at most 8 MiB of uncompressed bytes. That is a fixed
per-request limit with no configuration field: the same on every deployment, and in force
whether or not the ledger is enabled. With `ceiling_bytes: 0` it is the only thing bounding
a buffered serve. The fourteen fields below describe what the proxy did against that limit.
All fourteen are per-proxy counters or gauges since process start, all are present even
when the ledger is disabled, and all are exported over OTLP under their `inflight_memory.`
names.

**Two serve paths reach these counters, and every field counts both** unless it says
otherwise: a ranged cache hit materialized into one buffer, and a GET carrying no `Range`
header answered whole from cache. Both materialize into a single buffer, both take their own
ledger reservation, and neither the size buckets nor the peak distinguishes them.
`buffered_serve_full_object_total` is the discriminator: of the serves counted in the
buckets, it says how many came from the no-`Range` whole-object path, so the bucket total
minus that figure is the ranged ones.

| Field | Kind | Meaning |
|---|---|---|
| `buffered_serve_sliced_streams_total` | counter | Requests answered by streaming a slice of one contained cached extent instead of loading the whole extent. The route taken when the extent is over the bound but contains the request |
| `buffered_serve_full_object_total` | counter | Of the serves counted in the buckets and the peak, how many came from the no-`Range` whole-object path |
| `buffered_serve_size_le_64kib_total` | counter | Buffered serves materializing `<= 64 KiB` |
| `buffered_serve_size_le_1mib_total` | counter | Buffered serves materializing `> 64 KiB` and `<= 1 MiB` |
| `buffered_serve_size_le_8mib_total` | counter | Buffered serves materializing `> 1 MiB` and `<= 8 MiB` |
| `buffered_serve_size_gt_8mib_total` | counter | Buffered serves materializing more than 8 MiB. An invariant alarm, not a workload signal; see below |
| `buffered_serve_peak_materialised_bytes` | high-water | Largest single buffered-serve materialization seen by this process, in bytes. Never reset; see below |

The four size buckets are disjoint and every buffered serve lands in exactly one, so their
sum is the number of buffered serves this process has performed on both paths together. The
size they are bucketed on is what was loaded, not what the client asked for. A request
for a few bytes of a cached extent loads that extent's whole uncompressed length, so a small
`Range` can land in a large bucket.

**Seven `buffered_serve_degraded_*` counters record the opposite decision**: the bytes were
*not* materialized, because the request or the extents it would have loaded were over the
bound. Each names the route taken instead. They are disjoint, and safe to add: a request that
degrades is counted once, under the one reason that decided it, and never falls through to be
counted again under a second. The seven:

| Field | Kind | What happened instead |
|---|---|---|
| `buffered_serve_degraded_chained_stream_total` | counter | Served by chaining slice streams over two or more contiguous cached extents. **The cache still answered the request** |
| `buffered_serve_degraded_forward_gap_total` | counter | Forwarded to S3 uncached: a gap between the cached extents |
| `buffered_serve_degraded_forward_partial_total` | counter | Forwarded uncached: one extent, and it does not contain the request |
| `buffered_serve_degraded_forward_unresolved_total` | counter | Forwarded uncached: an extent's on-disk range could not be resolved |
| `buffered_serve_degraded_forward_stream_failed_total` | counter | Forwarded uncached: an extent's stream could not be opened |
| `buffered_serve_degraded_forward_partial_cache_merge_total` | counter | Forwarded uncached, before any segment was fetched: merging the cached and fetched segments would have been over the bound |
| `buffered_serve_degraded_merge_backstop_total` | counter | The range merge itself refused an over-bound merge. This is the backstop behind the checks above, reached by a request none of them stopped, so it normally reads 0. It does not re-count the requests `forward_partial_cache_merge` already counted, because that one forwards before the merge is attempted. A rising value means a shape the earlier checks let through |

**Their sum is not every over-bound decision, and one term is missing from it.** A request over
the bound whose range sits inside a single cached extent is served by streaming a slice of that
extent, and that is counted in `buffered_serve_sliced_streams_total` rather than under any of
the seven. The same is true of a no-`Range` whole-object read whose object sits in one extent.
So the seven account for every over-bound decision *except* the single-extent slice stream, and
the total this process has taken is their sum plus `buffered_serve_sliced_streams_total`. Add
that term before comparing over-bound decisions against `buffered_serve_*` bucket volume, or
the slice streams (the cheapest and most common of the degrades) go missing.

A serve forwarded because its cached bytes and the head it would send name different versions is
within the bound, and no counter records it. Each is logged at `warn` as
`Cached bytes of version ... cannot be served under this response's head`.

Only the first of those seven is a cache hit. The other six are requests the cache held data
for and forwarded anyway, so they are the ones to watch when hit rate looks lower than the
cache's contents suggest. A rising `forward_unresolved` or `forward_stream_failed` means
something else: those two name an extent the proxy expected to read and could not. Treat them
as a storage or metadata signal rather than as a sizing one.

#### `buffered_serve_size_gt_8mib_total` above zero is an invariant alarm

Every buffered serve on both paths is gated against the 8 MiB bound before anything is
loaded, so a serve materializing more than that cannot happen. The field exists to catch the
case where it did. **Any non-zero value means something materialized past the bound**, which
is a defect in the proxy rather than a property of the workload. Do not read it as "large
reads are happening" and do not tune anything in response. Alarm on `> 0`, and treat it as
you would any other invariant breach.

The same reading applies to `buffered_serve_peak_materialised_bytes`: a value above 8 MiB
(8388608) is the same breach reported as a size rather than as a count, and the two should
move together.

#### The peak is a high-water mark, so it is not a current figure

`buffered_serve_peak_materialised_bytes` only ever rises. It is the worst case this process
has seen since it started, and it says nothing about what the proxy is holding now; for
that, read `reserved_bytes`. One whole-object serve of a large cached object pins the peak
for the life of the process, and there is no reset API, so a restart is the only thing that
lowers it. Use it to answer "did this proxy ever buffer more than I sized for", never "is it
buffering that much".

---

## `write_cache`

Always present, including when the write cache or the cache manager is not wired in. In
that case every field is zeroed rather than the section going `null`, the same shape
`inflight_memory` uses when disabled.

This section reports on staged objects: objects in the write cache that no client has read
yet. To see whether the write cache is evicting, read this section rather than
`cache.evictions`, which counts read-cache eviction only; a `0` there says nothing about the
write cache.

| Field | Kind | Meaning |
|---|---|---|
| `resident_bytes` | gauge | Staged bytes on the cache volume: the same fleet-wide figure `cache.write_cache_size` reports, and the figure the write-cache bound (`staging_bound_bytes`) is enforced against. Its derivation depends on `staged_authority_epoch`: at `0` it is the event-sourced size counter, and at `1` it is recomputed from the `.meta` files. See the epoch's own row below, and the derivation table under `cache.total_cache_size` |
| `staged_authority_epoch` | gauge | `0` while the event-sourced counter is the write cache's accounting authority (the default on first start), and `1` once the fold is. **Read this before comparing `resident_bytes` or `cache.write_cache_size` across proxies or across time**: the two epochs are two different computations of the same quantity. The transition includes a one-time step to the fold's figure, up or down, as the old counter's accumulated error corrects. Fleet-wide: it is read from a document on the cache volume, so every proxy agrees once each has observed the activation. Activation is automatic once the evaluating proxy's own read of the volume shows the fold rotation and the ledger-completeness repair rotation complete. It is evaluated only on consolidation cycles with something to record (an upload, overwrite or delete that changes the cache's size, a miss that caches data, a GET served from the cache, or a revalidation with S3), so an idle volume stays at `0`, and so can one whose only traffic is same-size overwrites; [Rolling back from 3.0.0](UPGRADING.md#rolling-back-from-300) gives the timing. It also needs at least one readiness record under 24 h old, every such record reporting a fold-capable binary with no incomplete fold, and every instance id with staging-record files vouched for. A readiness record over 24 h old stops counting. An id with staging-record files and no current record stops counting once all its ledger files have been unmodified for at least a day (the day restarts when 3.0.0 clears one of its entries), or once `s3-proxy --config <path> retire-instance <id>` retires it. `staged_authority_activation_blocker`, `staged_authority_activation_blocking_instance` and `staged_authority_activation_blocking_instance_count` below say what is holding it. Once it reads `1`, rolling back to a release without the fold is not supported; see [Rolling back from 3.0.0](UPGRADING.md#rolling-back-from-300) |
| `graduation_lock_unavailable_total` | counter | Graduations refused because another writer held the key's metadata lock. **Per-proxy.** Read as a ratio against `graduations_total`: a small ratio is the expected steady state, since two concurrent reads of one staged object race by design and the loser graduates on a later read. A ratio approaching or exceeding `1` means the lock namespace is contended or wedged and the write cache has stopped draining. Otherwise that shows only as `resident_bytes` sitting flat against a rising `staged_entries`, two gauges away from the cause. Counts only lock contention: a metadata file that vanished, and an object a peer had already graduated, are different outcomes and are deliberately not folded in |
| `graduation_noop_entries_total` | counter | Graduation journal entries parsed, applied as no-ops, and retired. **Per-proxy.** Zero while `staged_authority_epoch` is `0`. After activation it rises while the old accounting entries drain, then stops. If it keeps rising, a proxy without fold support is still writing those entries. Read it with `staged_authority_foreign_write_total` |
| `staged_authority_foreign_write_total` | counter | Rewrites of the shared accounting state by a proxy without fold support that this proxy caught after activation. **Per-proxy.** It rarely moves even while such a proxy runs: a 2.8 proxy rewrites the state under the same lock 3.0.0 uses, and 3.0.0 restores its schema field without counting the rewrite. Zero does not show that no 2.8 proxy writes to the volume, so stop any 2.8 proxy on an activated volume. The activation gate waits while a proxy's staging-record files have been modified within the last day. A proxy idle longer than that which writes again after activation is neither refused nor reported. Non-zero is an incident: stop that proxy; see [Rolling back from 3.0.0](UPGRADING.md#rolling-back-from-300) |
| `staged_authority_activation_blocker` | label (nullable) | Why `staged_authority_epoch` has not flipped to `1`: the gate's blocker label, such as `ledger_without_ready_record`, `no_fresh_ready_records`, `rotation_incomplete`, `repair_rotation_incomplete`, `fold_incomplete_observed` or `not_fold_capable`. **Per-proxy**: the last verdict this process evaluated, which it does only on non-idle consolidation cycles where it wins the global consolidation lock, so a proxy that has not won the lock since start reads `null`. `null` also when the last verdict permitted activation, and once the epoch is `1`. Read it with the next two rows |
| `staged_authority_activation_blocking_instance` | label (nullable) | The instance id (`{hostname}:{pid}`) the blocker names. `null` when there is no blocker, and for the volume-wide blockers `no_ready_records`, `no_fresh_ready_records` and `ready_records_unreadable`, which name no instance. This is the id to pass to `retire-instance` |
| `staged_authority_activation_blocking_instance_count` | gauge (nullable) | How many instance ids are holding activation back. It counts each id with staging-record files that no current readiness record vouches for, whose ledger files were modified within the last day and that no `retire-instance` marker covers. It also counts each readiness record under 24 h old that fails a precondition. `staged_authority_activation_blocking_instance` is the first of them. **Per-proxy**, like the blocker. `null` in the blocker's `null` cases and for `ready_records_unreadable` and `no_ready_records`; `0` for `no_fresh_ready_records` when no id holds the gate |
| `staging_bound_bytes` | config-derived | The configured write-cache bound: `max_cache_size * write_cache_percent / 100`. See [Write cache configuration](CONFIGURATION.md#write-cache-configuration) |
| `inflight_bytes` | gauge | This proxy's current in-flight write-reservation total. **Per-proxy, not shared**: do not sum across the fleet expecting a global figure |
| `over_bound` | derived (bool) | `resident_bytes > staging_bound_bytes`, exposed explicitly so the condition needs no arithmetic to detect |
| `staged_entries` | gauge | Live count of staged entries: written to the write cache and not yet graduated. **Approximate and best-effort**: a live gauge maintained by each proxy, not a scan result. Incremented on every write-cache commit. Decremented when an entry leaves the write cache by any of four routes: graduation (first read, once the object's `Last-Modified` is known), a re-PUT superseding the previous entry, eviction, or invalidation (expiry or explicit) |
| `graduations_total` | counter | Entries that left the write cache by being read, which also requires the object's `Last-Modified` to be known: an entry read while it is still unknown stays staged until a revalidation learns it. **Per-proxy.** Read it alongside `staged_entries`: a flat gauge on its own cannot distinguish an idle proxy from a broken graduation path, and graduation is how the write cache is meant to drain. Together with `staging_evictions_total` it accounts for the two ways an entry leaves: graduation keeps the bytes on disk (they move to the read cache, so `resident_bytes` falls while `cache.total_cache_size` does not), and eviction reclaims them. Not the accounting authority: the authoritative decrement is applied to the size counter under the consolidation lock, so two proxies reading the same key at once can both count here while only one decrement lands |
| `staging_evictions_total` | counter | Objects evicted from the write cache. Separate from `cache.evictions`, which counts read-cache eviction only |
| `staging_eviction_bytes_total` | counter | Compressed bytes freed by write-cache eviction |
| `ledger_entries` | gauge (nullable) | Write-ledger length: records across every ledger file under `metadata/_write_ledger/`, which holds one file per cache shard per proxy (`{bucket}/{XX}/{instance}.ledger`). **`null` means UNREADABLE, not zero**: a directory listing or file read that failed. `0` is the healthy drained value, so alarm on `null` separately from `0`. Counts the same population as `staged_entries` by a different route: that one is a per-proxy in-memory gauge, and this is a count of records on the cache volume. A **persistent** gap between them is the signal that appends are being lost or compaction is not keeping up. A transient gap is normal. Compaction visits the ledger a shard at a time and starts a full pass every 5 minutes. On a ledger too large for one pass (10 seconds by default), it resumes once a minute until the pass completes. That cadence is fleet-wide: the proxies share one rotation, so the fleet runs one pass per interval, not one per proxy. Not the accounting authority; `resident_bytes` is, and a ledger figure must never be reported as a cache size |
| `ledger_entries_age_secs` | gauge (nullable), seconds | How long ago `ledger_entries` was observed. The count is taken by staging eviction and by the background collection rather than on each scrape, and is kept no older than 300 s in the steady state. `null` exactly when `ledger_entries` is. It keeps counting up at scrape time, so a figure far past 300 s means the count is not being refreshed. Two scrapes showing the same `ledger_entries` with a growing age are the same observation reported twice, not a stable count |
| `setup_abandoned_total_put` | counter | Streamed PUT and UploadPart write-cache setups abandoned because the setup did not finish within 5 s, or had not finished up to 500 ms after the request-body buffer filled. Setup covers opening the cache writer, reserving capacity, creating the temporary file and looking up the TTL. **Per-proxy.** Each is an upload forwarded to S3 uncached because the cache setup, which runs beside the upload, did not finish; the upload itself succeeds. A WARN is logged at most once a minute. Its counterpart once the body is streaming is `cache.tee_abandoned_total_put`. Read it beside the three `pending_lock_*` rows: a rise in both at once points at contention on the per-proxy pending-credit lock, a rise here alone points elsewhere, such as a slow cache volume |
| `mpu_completion_ledger_declines_total` | counter | Completed multipart uploads left uncached because a write-ledger record could not be written before the object was published. **Per-proxy.** The upload itself succeeds on S3; a WARN names each one. A steady rise means the ledger directory (`metadata/_write_ledger/`) is failing and the write cache is effectively off for multipart uploads, while single-part PUTs keep caching |
| `pending_lock_waits_total` | counter | Acquisitions of a per-proxy pending-credit lock (`{instance}.pending.lock`) that did not succeed on the first try. **Per-proxy.** This is the only cross-proxy lock on the write-cache admission path for PUTs; its holders include every peer's own credit appends, the fold's cleanup after a pass, and the startup reaper. **This is not a per-credit figure.** A credit append takes the lock once per batch of up to 1,024 credits, so a busy proxy publishes far fewer waits here than it commits writes, and the rate falls as batches grow. Divide by `write_ledger.pending_batches_total` to read it as a fraction of lock holds that waited; a rate read per PUT understates contention by the mean batch size |
| `pending_lock_wait_ms_total` | counter, milliseconds | Time spent waiting for those locks, timeouts included. Divide by `pending_lock_waits_total` for a mean wait. **Per-proxy** |
| `pending_lock_timeouts_total` | counter | Waits abandoned at their bound, at most 100 ms, each logged at WARN. **Per-proxy.** Two consequences, and the second is the larger one. On a read, the pending-credit file is treated as unreadable, which makes `staged_range_count` unknown for that read, and the range budget admits rather than declines. On a credit append, the batch that timed out loses up to 1,024 credit records at once. Their ledger entries are durable, so the staged figure under-states those bytes across every shard the batch touched until a staged-fold pass visits them; the stage schedules that pass by marking each shard dirty. Either way the direction is an under-stated staged figure and an admitted write, never a refused one. Read it beside `write_ledger.pending_batch_failures_total`, which counts the append-path events on their own |
| `journal_unparseable_lines_retained` | gauge, lines | Journal lines the most recent journal cleanup pass that removed anything could not parse and left in place. **Per-proxy**, re-set by each such pass, with a WARN at most once per 5 minutes while non-zero. Non-zero is expected while proxies on different releases share the cache volume, since a release cannot parse entries that a newer one writes, and it returns to `0` once every proxy runs the same release. Non-zero on a fleet that runs one release means torn or corrupt journal lines; check the volume's health and free space |

### The fold-derived staged figure (`write_cache.staged_fold_*`)

**These are the components of the staged figure, published separately so a reader can check
the total.** Once `staged_authority_epoch` reads `1` they are what
`resident_bytes` and `cache.write_cache_size` are computed from, so the two are not
independent readings and a divergence between them means one was sampled mid-fold rather than
that they disagree.

While the epoch is `0` they are observability only and `resident_bytes` is the
event-sourced counter, unchanged. `staged_fold_gap_bytes` is the figure that quantifies the
difference during that window, and it is the evidence for the step at activation: a positive
gap means the figure will step up, a negative one down.

**The range count is a decision input in both epochs**: `staged_range_count` is enforced
against `staged_range_budget`, and reaching it declines write-cache admission. See
[The staged range budget](#the-staged-range-budget) below.

Every byte figure is nullable, and `null` means UNREADABLE rather than zero: a staged
figure reporting `0` because a file could not be read is a failure wearing the healthiest
value available.

| Field | Kind | Meaning |
|---|---|---|
| `staged_fold_bytes` | gauge (nullable) | The fold's staged figure in compressed on-disk bytes: `staged_fold_byte_sum` + `staged_fold_pending_bytes` + `staged_fold_in_progress_part_bytes`. `null` if any component is unknown, or if no fold has completed on this volume yet. **`null` is how this figure says Unknown, not a field that failed to arrive**, so never substitute a zero for it. **Not a current total**; see below |
| `staged_fold_byte_sum` | gauge (nullable) | The `.meta` fold alone: summed staged bytes over the verified metadata the write ledger indexed, as installed by the last complete pass |
| `staged_fold_pending_bytes` | gauge (nullable) | Credits recorded after the last fold's cut, summed. Bytes that are staged but not yet in `staged_fold_byte_sum` |
| `staged_fold_in_progress_part_bytes` | gauge (nullable) | The same figure as `cache.in_progress_part_bytes`, restated here so all three components of the total are visible beside it |
| `staged_fold_gap_bytes` | gauge (nullable, signed) | `staged_fold_bytes` minus the event-sourced size counter. `null` once `staged_authority_epoch` is `1`. See the sign convention below |
| `staged_authority_age_secs` | gauge (nullable), seconds | Seconds since the fold last installed a figure for any shard: the age of the *freshest* shard, i.e. "is the fold getting anything done". **Not the authority's age** (see the next row), and not a field to alarm on alone: read it with `staged_mutations_awaiting_fold` |
| `staged_shard_authority_age_secs` | gauge (nullable), seconds | The age of the stalest shard's staged figure: the maximum, over shards, of now minus that shard's fold time. This is the authority's real age and the figure to alarm on, because a sum of shard figures is only as fresh as its stalest component. A fold spinning on one shard and starving the rest keeps `staged_authority_age_secs` small while this one climbs. An elevated value can be confined to a single shard: an unreadable write-cache record file holds back only the shard it belongs to. Read it beside `staged_fold_shards_beyond_max_age`, which says how much of the figure is stale rather than how stale the worst part is |
| `staged_fold_shards` | gauge, shards | Cache shards with an installed staged figure. A shard is a `(bucket, first-two-hex-of-key-hash)` pair, so a single-bucket cache has up to 256 |
| `staged_fold_shards_beyond_max_age` | gauge, shards | Shards whose figure is older than `staged_authority_max_age_secs`. Read beside `staged_shard_authority_age_secs`: that says how stale the stalest shard is, this says how much of the figure is stale. One shard past the bound is a scheduling hiccup; most of them is a rotation that cannot keep up, and `staged_fold_shard_budget_capped_total` then says why |
| `staged_authority_max_age_secs` | gauge, seconds | The target age the two age gauges are judged against. Published for the same reason as `staged_range_budget`: it is fixed in the binary with no configuration field, so a consumer that could not read it here would have to hardcode it |
| `staged_fold_completed_age_secs` | gauge (nullable), seconds | Time since the last recomputation of the staged figure completed. `null` means none has completed on this volume, or the shared state file was last rewritten by a proxy that does not record the field. Absence is unknown, never failure. This answers "how long has the recomputation been failing", which is a different question from `staged_shard_authority_age_secs`, "how stale is the figure on display". The two diverge exactly when the figure is a retained one. **Fleet-scoped** |
| `staged_fold_last_attempt_age_secs` | gauge (nullable), seconds | Time since the last recomputation attempt, complete or not. The gap between this and `staged_fold_completed_age_secs` is how long the figure has been retained. **Fleet-scoped** |
| `staged_fold_last_attempt_complete` | gauge (nullable), bool | Whether the last attempt completed. `false` is what puts the dashboard's figure-state badge into its retained state even while a complete older figure is still on display and inside its age bound, the one state no other field distinguishes. `null` when no attempt is recorded. **Fleet-scoped** |
| `staged_fold_shard_budget_capped_total` | counter, passes | Fold passes that wanted to visit more shards than one pass is permitted. **Non-zero means this proxy alone cannot revisit every shard inside the staged figure's target age**, because the population exceeds `staged_fold_shards_reachable_per_instance`. Whether it rises depends on `staged_fold_shards` alone: above that figure every pass on every proxy is clipped however many proxies share the volume, so adding proxies does not stop it. Proxies claim shards from each other, so more proxies share the work; whether the fleet keeps up is `staged_fold_shards_beyond_max_age`. **Per-proxy** |
| `staged_fold_shards_reachable_per_instance` | gauge, shards | The most shards one proxy's fold can revisit inside `staged_authority_max_age_secs`: its per-pass ceiling times the passes in that window. Fixed in the binary. Compare against `staged_fold_shards`; above it, this proxy's budget is capped and the WARN `Staged fold: this volume has more cache shards than one instance can revisit` fires. More proxies share the rotation, but each one's budget stays capped, so the counter and the WARN continue on every proxy after proxies are added; judge the remedy by `staged_fold_shards_beyond_max_age` |
| `staged_fold_dirty_shards` | gauge, shards | Shards this proxy removed staged bytes from and has not yet re-folded. A DELETE, the first read of a written-through object, an overwrite, a staging eviction or a capacity eviction marks the shard; a visit of it that this proxy begins after the mark clears it. The next fold pass visits dirty shards ahead of stalest-first order. It visits them inside its shard budget as far as `staged_authority_max_age_secs` allows, and beyond the budget for the rest, up to one budget's worth more per pass. That extra allowance shrinks once the budget is past half the per-pass shard ceiling, and is zero at the ceiling. A pass that visits any beyond its budget logs an INFO line, `Staged fold: visiting N dirty shard(s) beyond this pass's budget of B`. Process memory, `0` after a restart. **Per-proxy** |
| `staged_range_count` | gauge (nullable), ranges | Staged ranges: summed over shard figures, plus credits recorded after each shard's cut. **This is the figure the budget is enforced against** |
| `staged_range_budget` | gauge, ranges | The fixed limit `staged_range_count` is compared against. Published so a dashboard can draw one against the other without hardcoding it; there is no configuration field to read |
| `staged_range_budget_declines_total` | counter, objects | Write-cache admissions declined because the budget was reached, since process start. **Per-proxy** |
| `staged_fold_shard_duration_secs` | histogram, seconds (`sum_ms` in milliseconds) | Per-shard fold visit durations, in fixed classes with upper edges 1, 5, 15, 60, 300, 900 and 3600 s plus `+Inf`. `classes` is an array of `{le_secs, count}`: `le_secs` is the class's inclusive upper edge in seconds, `null` for the `+Inf` class, and `count` is the visits in that class alone, not cumulative. `count` at the top level is total visits; `sum_ms` is summed duration in milliseconds, so a fleet of sub-second visits does not sum to zero. **Per-proxy** |
| `staged_mutations_awaiting_fold` | gauge (nullable), credits | Count of credits recorded after the cut, i.e. staged mutations the installed figure does not yet account for |
| `staged_fold_incomplete_total` | counter, passes | Fold passes since process start that retained the previous figure, because an input could not be read or because a multipart completion was between its ledger append and its metadata publish. The second case is `candidate_publishing`, and that shard is revisited on the next pass. A pass that installed nothing because peers held every candidate shard is *not* counted here; that is the claim mechanism working. **Per-proxy** |
| `staged_fold_candidate_read_inflight_peak` | gauge, reads | Peak metadata reads the write cache recomputation has held outstanding at once, since process start, out of a maximum of 64. **`1` on a fleet with more than one staged object in a shard is a finding**: the recomputation is reading serially. Read width decides whether the recomputation can cover the whole staged set inside its freshness window. A value below 64 is not itself a fault: it is bounded by the largest number of staged objects any one shard has held. **Per-proxy** |
| `staged_authority_snapshot_age_secs` | gauge (nullable), seconds | Age of the short-lived cache in front of the staged figure. `null` means there is no live entry, so the next read re-derives from the cache volume. **Not alarmable on its own**: an age of 3 s is consistent both with one entry serving for 3 s and with a figure re-derived on every call since; the two counters below separate them. The age is measured from the time of the walk that built the figure, so it can read `null` while scrapes are served a figure derived a moment earlier |
| `staged_authority_snapshot_hits_total` | counter, reads | Staged-figure reads served from that cache, since process start. **Per-proxy** |
| `staged_authority_snapshot_recomputes_total` | counter, reads | Staged-figure reads that re-derived from the cache volume: a checkpoint read, a locked read of every staging credit file, and the in-progress multipart figure. That figure is one small document per proxy, on top of the background collection's traversal of `mpus_in_progress/`, and it traverses the directory again only when those documents are not a usable set. Read as a hit rate against the row above: near zero means the cache-volume reads the cache exists to remove are still being paid on every dashboard refresh, every scrape and every write. A refresh made for a scrape counts here whether or not its result is kept, and starts no `mpus_in_progress/` walk |
| `ledger_repair_rotation_complete` | gauge, bool | Whether the ledger-completeness repair rotation has swept every observed shard inside its 24 h window. Read from the cache volume, so any proxy's sweep counts. This is an activation precondition: while it is `false`, `staged_authority_epoch` cannot flip to `1` |
| `ledger_repair_stalest_age_secs` | gauge (nullable), seconds | Age of the stalest *completed* shard sweep. `null` means either the metadata tree could not be listed or some shard has never been swept, not a healthy zero. Watch this to see the precondition converge rather than only being told activation is blocked |
| `ledger_repair_observed_shards` | gauge (nullable), shards | Shards found in the metadata tree on disk. `null` if the tree could not be listed. Read from disk rather than from the fold's shard set, because a shard whose staged records were all lost appears in none of the fold's sources |
| `ledger_repair_swept_shards` | gauge, shards | Shards with at least one completed sweep. Compare against `ledger_repair_observed_shards` for rotation progress |
| `ledger_repair_ranges_reappended_total` | counter, ranges | **Non-zero is a finding, not a routine reading.** Staged ranges found on disk that no staging record named, and restored. Each one was excluded from the staged figure, which biases the eviction trigger *late*. Expect `resident_bytes` to step up by the repaired bytes at the next fold. **Per-proxy** |
| `ledger_repair_incomplete_total` | counter, shards and passes | Reads that could not be completed, so the work they would have fed was skipped rather than done from a partial view. Usually one per shard: a shard whose own staging records cannot be listed or read completely is skipped, and its siblings in the same pass are unaffected. Only that shard records no completed sweep, so this can increment while the same pass finishes another shard's sweep. Increments once for the whole pass instead when what failed is not confined to one shard: listing the shard population, or reading or writing a shard's stored sweep progress. A pass that found every shard held by a peer is *not* counted. Read beside `ledger_repair_swept_shards` and `ledger_repair_stalest_age_secs`, because a shard skipped this way holds the activation precondition back until a later pass reads it successfully. **Per-proxy** |
| `ledger_repair_sweeps_completed_total` | counter, sweeps | Shard sweeps that reached the end of their shard. A sweep stopped by the per-visit cap stores a cursor and does not count until it finishes. **Per-proxy** |
| `ledger_repair_rotation_reachable` | gauge (nullable), bool | **Whether ONE proxy can complete the rotation inside its 24 h window at this shard population**, i.e. `ledger_repair_observed_shards` ≤ `ledger_repair_shards_reachable_per_instance`. `false` is permanent for that proxy: its sweep budget is clamped by the per-pass ceiling. On a fleet of one proxy the rotation then never completes, the activation precondition never passes, and `staged_authority_epoch` never flips. Proxies claim shards from each other, so adding a proxy is the remedy; the field judges one proxy and stays `false` after it, so watch `ledger_repair_rotation_complete` and `ledger_repair_stalest_age_secs` to see the fleet complete the rotation. This is the only field that distinguishes "impossible" from "not yet"; every other repair gauge reads identically in both cases. `null` if the metadata tree could not be listed. A **scheduling** statement: it compares shard count against visits per pass and cannot see a visit overrunning its time budget on a dense shard |
| `ledger_repair_projected_rotation_secs` | gauge (nullable), seconds | Projected time for one proxy, sweeping alone, to complete one full rotation at the observed population and the budget in force. Proxies share the sweep, so a fleet of N proxies takes about this divided by N. **Compare against 86,400.** Larger means unreachable, and by how much, the magnitude the boolean above cannot carry. `null` if the population could not be listed |
| `ledger_repair_rotation_remedy` | string | What to change when the rotation is not completing, chosen by which limit is binding. When `ledger_repair_rotation_reachable` is `false`, the shard population is too large for the window and the text lists adding proxies first. When it is `true` but `ledger_repair_stalest_age_secs` exceeds twice `ledger_repair_projected_rotation_secs` (at least two repair intervals), the scanner concurrency limit is binding. The text then gives `scanner_concurrency_k_repair` and `scan_deferred_total_repair`, points at `scanner_aggregate_estimate_by_level_repair`, and does not suggest more proxies, which would add scanners the limit defers. `""` when neither applies or the population could not be listed. Uses the repair limiter of the proxy that served the request |
| `ledger_repair_shards_reachable_per_instance` | gauge, shards | The most shards one proxy's repair sweep can cover inside its 24 h window: its per-pass ceiling times the passes in that window. Fixed in the binary. `ledger_repair_rotation_reachable` is `ledger_repair_observed_shards` compared against this |
| `ledger_repair_shard_budget_capped_total` | counter, passes | Passes whose per-pass shard budget was clipped by the ceiling. Non-zero implies `ledger_repair_rotation_reachable` is `false`. A rate-limited WARN names the observed shard count, the required budget and the ceiling. **Per-proxy** |
| `ledger_repair_metas_per_l2_dir` | gauge (nullable), files per directory | Metadata files per second-level metadata directory, smoothed over completed shard sweeps. Decides which of two regimes the sweep is in: below the crossover (this figure divided into `ledger_repair_l2_readdir_ms / ledger_repair_meta_read_ms`) the sweep is limited by listing directories, above it by reading files. Measured continuously rather than configured, because the crossover differs by a factor of nearly five across storage types, and a volume can cross it by growing. `null` until one sweep has completed. **Per-proxy** |
| `ledger_repair_l2_readdir_ms` | gauge (nullable), milliseconds | Mean time to list one second-level metadata directory, smoothed over completed sweeps. An in-flight duration at the width the sweep was running at, not a solo latency. Read it beside the row below: their ratio is this volume's own crossover point. **Per-proxy** |
| `ledger_repair_meta_read_ms` | gauge (nullable), milliseconds | Mean time to read and parse one metadata file, smoothed the same way. **Per-proxy** |
| `ledger_repair_directory_concurrency` | gauge, operations | Outstanding directory listings the next sweep will keep in flight, out of a fixed total of 64 shared with file reads. Derived from the three rows above and clamped to 1 to 63, so neither stage can be starved. `32` means no measurement exists yet. Near 64 is directory-limited, near 1 file-limited. **Per-proxy** |
| `scanner_concurrency_k_fold`, `scanner_concurrency_k_repair` | gauge, scanners | The concurrency the next staged fold or repair pass respects: the pass defers while at least this many other proxies are scanning. Starts at the number of proxies seen scanning and moves only on measured evidence. `0` when no consolidator is running. See [Scanner concurrency](SHARED_STORAGE.md#scanner-concurrency). **Per-proxy** |
| `scanner_concurrency_observed_fold`, `scanner_concurrency_observed_repair` | gauge, scanners | How many other proxies were scanning the last time this proxy's fold or repair gate checked. `0` until the gate has run. **Per-proxy** |
| `scanner_samples_discarded_total_fold`, `scanner_samples_discarded_total_repair` | counter, visits | Shard visits left out of the rate estimate because the number of other scanners changed during the visit. A high share against the per-level sample counts means the fleet's scanning population is churning faster than one visit. **Per-proxy** |
| `scan_deferred_total_fold`, `scan_deferred_total_repair` | counter, consolidation cycles | Consolidation cycles in which the pass was due and did not run because enough other proxies were scanning. **Counts cycles, not passes:** one pass that stays deferred for five cycles adds five. **Per-proxy** |
| `scanner_last_adjust_reason_fold`, `scanner_last_adjust_reason_repair` | string | Why the last completed pass left the concurrency where it is. `argmax` moved it to the level with the best measured fleet rate, `probe` tried one more scanner, and `hold` made no change. `floor` held it at 1 because no scanning proxy was seen, and `insufficient_samples` means too few visits at the current level to judge. `""` when no consolidator is running. **Per-proxy** |
| `scanner_rate_by_level_fold`, `scanner_rate_by_level_repair` | object, metadata files per second | Smoothed per-visit read rate keyed by level, where level is the number of proxies scanning including this one. Only levels with a kept sample appear, at most the first 64. **Per-proxy** |
| `scanner_samples_by_level_fold`, `scanner_samples_by_level_repair` | object, visits | Kept samples behind each rate above, same keys. A level needs 3 before it can move the concurrency. **Per-proxy** |
| `scanner_aggregate_estimate_by_level_fold`, `scanner_aggregate_estimate_by_level_repair` | object, metadata files per second | Estimated fleet-wide read rate at each level: the level times its per-visit rate. Only levels with at least 3 kept samples appear, so its keys are a subset of the rate object's. The concurrency moves to the level with the highest value here, so this is the measured curve behind `scanner_concurrency_k_*`. **Per-proxy** |
| `ledger_torn_lines_quarantined_total` | counter, lines | **Non-zero is a finding, not a routine reading.** Staging-record lines found unreadable (the signature of a write that landed only partly) and moved to a `.torn` file beside the record file instead of being discarded. Each one is a staged range whose record was destroyed, so its bytes are missing from the staged figure until the repair sweep restores them. The sweep re-appends the range from the `.meta` on disk and removes the write-cache credit that the lost line left without a record to pair with. The range is then counted once, rather than by both the credit and the restored record. A peer whose identity is not shared can restore it. A pass refused on a shared identity keeps its claim on the shard, though, so the bytes can stay missing until the hostnames are made distinct. A credit is only ever removed in the pass that writes its replacement, and only when that pass could read the shard's records whole; otherwise the figure stays over-stated until a later pass can. Read it beside `ledger_repair_ranges_reappended_total`: this counter says a record was lost, that one says the sweep found the range and restored it. A rising count here with a flat count there means the loss is recorded and not yet repaired. Non-zero also points at host or volume durability rather than at the proxy. **Per-proxy** |
| `unledgered_staged_credits_refused_total` | counter, credits | **Should read zero forever.** A write-cache credit was reached on a code path that writes no staging record, and was refused. The object is still cached correctly; only the write-cache accounting declines to count it, which keeps the credit and its record paired. Non-zero means a caller reached one of two internal paths with staged metadata and is a defect to report rather than a condition to tune. **Per-proxy** |

> **`staged_fold_bytes` is not a live total.** It is a sum of per-shard figures installed at different times, plus the
> credits recorded since each shard's own cut. So its freshness is the stalest shard's, which
> `staged_shard_authority_age_secs` reports and this figure does not carry with it, and a staged
> object written moments ago may be in it, or in the pending term, or in neither yet. Two
> consequences for an alarm. `null` means Unknown: a component could not be read, or no fold has
> completed on this volume. And a plain number is not evidence the fold is working: an older
> complete figure stays installed and stays derivable while every subsequent attempt fails, so
> read `staged_fold_last_attempt_complete` and `staged_fold_incomplete_total` beside it. Where a
> figure must be exact rather than fresh (the staged range budget's enforcement), the proxy
> re-derives rather than reading this one.

> **`ledger_repair_rotation_reachable: false` means one proxy cannot complete the rotation, and it is
> not a transient.** On a fleet of one proxy, activation then never happens. The sweep reads one `.meta` per object (the fold reads
> one per staged candidate), so the rotation's capacity is
> `objects ≤ 0.167 × proxies × 86,400 s × 2,900 obj/s` = **41.8 M × proxies** on a densely
> populated tree. On a maximally sparse tree (every directory holding one object, so the sweep pays a
> directory listing per object), it is 5.60 M × proxies, and that is the figure to plan against
> if you do not know which you have. Adding proxies is the remedy, and the capacity scales linearly with them.
> **Shortening the repair interval is not**: it multiplies the per-pass ledger read while leaving
> total `.meta` work unchanged. Staying on the event-sourced counter is safe: it over-states, so
> eviction triggers early rather than late. It only wastes capacity.
>
> The 2,900 objects/s rate is for an EFS volume with the sweep keeping 64 reads outstanding. Other
> storage types sustain different rates, so read `ledger_repair_projected_rotation_secs` rather than
> relying on this arithmetic.


> **`ledger_repair_*` is a different rotation from `staged_fold_*`, on the same 24 h window.**
> The fold recomputes the staged figure from the staging records every ~24 h per shard; this
> sweep checks the *records themselves* against the `.meta` files on disk every ~24 h per
> shard. The fold cannot detect what this finds: its candidate set comes only from the
> records, so a staged object no record names is absent from every fold figure while the fold
> reports a flawless rotation. A sweep costs one read per object in the shard against the
> fold's one read per *staged* object.


> **`staged_fold_gap_bytes` sign convention, and its comparand.** It is the fold **minus the
> event-sourced size counter**, not minus `resident_bytes`, which at
> `staged_authority_epoch: 1` *is* the fold's own figure. **It reads `null` once the epoch is
> `1`**, because there is then no second producer to compare against; a number there would
> invite reading a small value as agreement between two producers when one has been retired.
> **Positive** means the fold sees *more* staged bytes than the event-sourced counter does.
> **Negative** means that counter *over-states* relative to the fold, and the magnitude
> is the over-statement. Negative is the expected sign on a fleet carrying real drift,
> because that counter's known failure mode is lost debit events, which only ever inflate
> it. A gap that is large and *positive* is the surprising direction and worth
> investigating.

> **Age alone cannot tell an idle fleet from a stalled fold, which is why there are two
> gauges.** A large `staged_authority_age_secs` with `staged_mutations_awaiting_fold` at
> `0` means nothing has been staged since the last complete fold: the installed figure
> still accounts for everything on the volume, and it is healthy at any age. The same age
> with a non-zero count means mutations are waiting and the figure is behind. Alarm on the
> pair, never on the age by itself.

> **`staged_fold_incomplete_total` rising means an input could not be read completely, or a multipart completion was mid-publish.**
> A mid-publish retain clears on the next visit of that shard; a counter that keeps rising with no completions in flight points at an unreadable input.
> Rather than install a figure derived from a partial view, the fold keeps the previous
> figure and its cut together. **Most causes cost one shard rather than the volume**: an
> unreadable staging-record file, a discarded or torn record line, a truncated read and an
> unreadable metadata file each confine the retain to the shard they belong to, and that
> shard's siblings install new figures in the same pass. A smaller set has no shard to
> confine it to and holds the whole pass back, including an unreadable post-cut credit file
> or `mpus_in_progress/`, unreadable stored progress, and a record file that has not been
> converted to the one-file-per-shard layout. The reason is named in a WARN log line and
> is the first retain in visit order, so a pass that retained one shard and installed
> others still increments this. A rising counter alongside a rising
> `staged_shard_authority_age_secs` is the signal to look at the cache volume; read it
> beside `staged_fold_shards_beyond_max_age` to tell one held-back shard from all of them.

> **`staged_fold_shard_budget_capped_total` rising means one proxy cannot keep the staged figure
> fresh on its own, and no amount of waiting fixes it.** The fold revisits shards on a rotation and
> sizes each pass so the whole population is covered inside the figure's target age. Past
> **147,456 shards (576 buckets)**, published as `staged_fold_shards_reachable_per_instance`, there
> are more shards than one proxy's per-pass budget can revisit in time, the per-pass budget is
> clipped, and this counter rises. On a single proxy `staged_shard_authority_age_secs` then climbs
> past its target. The figure stays correct (it is a sum of complete per-shard figures); it is
> older than intended, so anything differencing it against another cache figure gets a number
> mixing two ages.
>
> **The remedy is more proxies.** Each proxy visits up to 512 shards per pass and skips
> shards a peer has claimed, so every added proxy adds up to 512 shard visits per pass to the
> fleet's rotation. Adding proxies does not stop this counter or its WARN: both depend on the
> shard population alone and keep rising on every proxy while it stays above 147,456. Judge the
> remedy by `staged_fold_shards_beyond_max_age` returning to 0 and `staged_shard_authority_age_secs`
> staying under `staged_authority_max_age_secs`. No configuration field raises the per-pass ceiling,
> the fold interval or the target age. 147,456 is a scheduling ceiling: on a slow storage type the
> rotation can fall behind below it, because each visit's cost depends on per-object metadata
> parse time. `staged_fold_shard_duration_secs` shows what a shard visit costs on your storage
> type.

#### The staged range budget

The proxy holds a fixed maximum number of staged ranges, published as `staged_range_budget`. Once
`staged_range_count` reaches it, the write cache declines new writes: the object goes to S3 and the
upload succeeds without a cached copy. Nothing about the client-visible upload changes.

The limit is fixed and has no configuration field. Raising it would require either a faster
metadata scan or accepting a staler staged figure, and both are decisions with arithmetic behind
them rather than a value to tune.

**It is a count limit, not a byte limit, and the two bind at very different points.**
`write_cache_percent` bounds staged *bytes*: on a 1 PiB volume at 20% that is about 205 TiB.
The range limit covers about 250 TiB at an 8 MiB mean range, and about 3.9 TiB at 128 KiB.
A workload staging many small objects therefore reaches this limit with the byte bound nearly
empty, and `resident_bytes` far below
`staging_bound_bytes` is not evidence that the budget is not the cause.

The limit is derived from how fast a proxy can read the metadata it re-reads to keep the staged
figure fresh. Read `staged_range_budget` from the running proxy rather than carrying a figure.

**How to recognize it.** Three signals, and the first two are enough:

1. `write_cache.staged_range_budget_declines_total` rising on `/metrics`. It is
   per-proxy, so check each proxy.
2. `staged_range_count` sitting at or above `staged_range_budget`.
3. A WARN log line naming `staged_range_count` and `staged_range_budget` together, emitted at
   most once per 60 seconds while the limit is in effect. Without the rate limit it would be one
   line per upload.

The symptom without those signals is a workload whose write-cache hit rate falls to zero
while uploads keep succeeding.

**Recovering headroom.** A staged object stops being staged when it is first read
(graduation), so a workload that reads what it writes drains on its own. Otherwise lower `put_ttl`
for the affected prefix so staged entries expire sooner. Note the count includes credits recorded
since the last fold, so it responds to new writes immediately rather than at fold cadence.

**An unreadable figure admits rather than declines.** If the shard checkpoints or the pending
files cannot be read, the count is unknown and caching proceeds. Silently turning off the write
cache on an unreadable file would be the worse failure, so `staged_range_count` reading `null`
means the limit is not being enforced, not that it is.

**When `over_bound` is `true`, expect a rate-limited WARN log line** (not a metric) naming
both `resident_bytes` and `staging_bound_bytes`, at most once per 60 seconds. The condition
is then visible in logs even without polling `/metrics`.

**`resident_bytes` and `inflight_bytes` are different quantities and are allowed to
diverge.** `resident_bytes` is fleet-wide and read from the cache volume;
`inflight_bytes` is this proxy's in-memory reservation total. Write admission gates on
`inflight_bytes`, so that is the figure to check when PUTs are being refused: a healthy
`resident_bytes` beside a refusing proxy means this proxy's own counter is the problem.
They are normally close because one is seeded from the other at start, which is exactly why
seeing them equal to the byte is not corroboration that either is right.

**Two log lines make write-cache drift visible without diffing state files:** a full
validation scan logs the write-cache drift it corrected (at WARN when non-zero), and the
consolidator logs `old_write_cache_size`/`new_write_cache_size`/`write_cache_drift` on every
validation update. Recurring non-zero drift means bytes are being credited or debited
somewhere that is not paired.

**Exported over OTLP.** These write-cache signals are exported under `write_cache.*`:

- Occupancy: `staged_range_count`, `staged_range_budget`, `staged_range_budget_percent` and
  `staged_range_budget_declines_total`.
- The figure and its age: `staged_bytes`, `staged_shard_authority_age_secs`,
  `staged_authority_max_age_secs`, `staged_fold_shards_beyond_max_age` and
  `staged_fold_incomplete_total`.
- The shard-visit histogram: `staged_fold_shard_duration_count`, `..._sum_ms`, and `..._bucket`
  with an `le` attribute per class.
- The snapshot counters, the epoch, and the four repair-rotation fields.

`cache.in_progress_part_bytes` is exported under its own name.

**A field that reads as unknown is not recorded for the OTLP export rather than sent as `0`.**
OTLP gauges have no null, and a zero would publish the healthiest possible value for the least
healthy state, so an alarm on "occupancy is low" would fire as reassurance. The series is
**absent** only while the field has not been known since the proxy started. Under the default
cumulative temporality the exporter re-sends a gauge's last recorded value, so a field that
becomes unknown later keeps its last value on OTLP. See
[what to alarm on instead](OTLP_METRICS.md#staged-write-tier).
`ledger_repair_rotation_reachable` is the exception and is sent as `1`/`0`,
because an alarm on a permanent condition needs a value to compare.

The rest of this section's fields are not exported; see
[Not exported via OTLP](OTLP_METRICS.md#not-exported-via-otlp).

### `write_cache.count_pass_last_result` — the last count-driven write-cache eviction pass

`null` until a count-driven pass has completed on this volume. Write-cache eviction runs when either
the staged bytes or the staged range count crosses its trigger; this document is written only
when the count arm fired, from `metadata/_shard_state/write_cache_count_eviction_last_result.json`.
The trigger and target are `cache.eviction_trigger_percent` and `cache.eviction_target_percent`
of `staged_range_budget`.

**The document is fleet-wide, and three of its fields are not.** Whichever proxy held the
eviction lock wrote it, and every proxy publishes the same copy. `passes_total`,
`ranges_freed_total` and `admission_declines_total` are that writer's own counters since its
start, so never sum them across proxies, and expect them to jump when a different proxy wins the
lock.

| Field | Kind | Meaning |
|---|---|---|
| `instance_id` | string | The proxy (`{hostname}:{pid}`) that ran the pass |
| `completed_at` | unix seconds | When the pass finished |
| `count_budget` | gauge, ranges | `staged_range_budget` at pass time |
| `count_trigger` | gauge, ranges | The count the pass fires above |
| `count_target` | gauge, ranges | The count the pass tries to reach |
| `count_at_start` | gauge, ranges | The staged range count the trigger fired on, after subtracting the ranges this proxy's own earlier passes reclaimed and the fold has not yet reflected |
| `observed_count_at_start` | gauge, ranges | The count before that subtraction. Equal to `count_at_start` on the first pass after a fold installs |
| `ranges_requested` | gauge, ranges | `count_at_start − count_target`: what the pass set out to reclaim |
| `ranges_freed` | gauge, ranges | What it reclaimed |
| `shortfall_ranges` | gauge, ranges | `ranges_requested − ranges_freed`, saturating. `0` means the target was reached |
| `unevictable_unreadable_ranges` | gauge, ranges | **A named component of `shortfall_ranges`**: ranges belonging to candidates whose `.meta` could not be read. The pass refuses to evict on a failed read, so these cannot be reclaimed however far it walks |
| `unexplained_shortfall_ranges` | gauge, ranges | The rest of the shortfall. `shortfall_ranges == unevictable_unreadable_ranges + unexplained_shortfall_ranges` always |
| `reached_target` | bool | Whether `shortfall_ranges` is `0` |
| `objects_evicted` | gauge | Objects the pass evicted |
| `candidates_considered` | gauge | Staged candidates the pass walked |
| `reclaimable_candidates` | gauge | Of those, how many could be evicted |
| `expired_candidates` | gauge | Of those, how many were older than `put_ttl`, i.e. written and never read inside their TTL |
| `passes_total` | counter | Count-driven passes the writing proxy has completed since its start |
| `ranges_freed_total` | counter, ranges | Ranges those passes reclaimed |
| `admission_declines_total` | counter | The writing proxy's `staged_range_budget_declines_total` at pass time |
| `shortfall_reason` | string | `target_reached`, `no_reclaimable_candidates`, `reclaimed_and_refilled`, `candidates_exhausted` or `unreadable_candidate_metadata`. `reclaimed_and_refilled` is checked first and can accompany a met target |

Three states to tell apart when the write cache is declining writes on the range budget:

- **The count trigger never fired**: this document is `null`, or `passes_total` is flat across
  reads while `admission_declines_total` or the per-proxy `staged_range_budget_declines_total`
  climbs. That is a defect in the trigger's wiring or inputs.
- **Nothing was reclaimable**: `shortfall_reason` is `no_reclaimable_candidates`,
  `reclaimable_candidates` is `0`. Staged entries leave by being read or by expiring, so lower
  `put_ttl` for the affected prefix, or read what is written.
- **Reclaimed and re-filled**: `shortfall_reason` is `reclaimed_and_refilled`. Passes keep
  reclaiming and the count keeps returning to the trigger, so the gap between
  `eviction_trigger_percent` and `eviction_target_percent` is too narrow for this write rate.

A pass whose `shortfall_ranges` is non-zero logs a WARN with the same figures.

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
| `fallbacks` | counter | Widened requests served by fetching the client's own range from S3 instead. That happens when the widened fetch fails or returns too little, when the pages cannot be shown to be one version of the object, or when the object's length is not known yet |
| `ram_page_promotions` | counter | Pages promoted to RAM |
| `ram_page_promotion_skipped` | counter | Pages not promoted to RAM. Causes: over the RAM budget, no known ETag, no known object length, `cache.ram_cache_enabled` is `false`, or `ram_cache_eligible` is `false` under a cache rule |

`skipped_signed_range` climbing while `widened_requests` stays flat is the expected
picture for AWS CLI and SDK traffic: those clients sign the `Range` header, and widening
excludes signed ranges by design. Widening applies to unsigned ranges, which come from
presigned URLs and raw HTTP clients. Exported over OTLP as `page_cache.*`, one series per
field; `amplification_ratio` is recorded only once a request has been widened. See
[Page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching).

---

## `hedged_requests`

Always present. `issued`, `won`, `suppressed` (counters). `issued` is your duplicate-S3
bill. See [Hedged upstream requests](HEDGING.md).

---

## `bucket_traffic`

A map, always present, `{}` when no traffic. Keys are `"bucket"`, or `"bucket/prefix"`
when a configured prefix matched, or the reserved `"__other__"` once
`metrics.per_bucket.max_series` is exceeded.

Per-key counters: `bytes_served`, `bytes_saved`, `bytes_uploaded`, `get_requests`,
`put_requests`.

Two gotchas. The `"bucket/prefix"` join is lossy: you cannot split a key back apart
without knowing the configured prefix list. And only object GET and PUT/UploadPart are
counted: HEAD, DELETE, LIST, and the multipart lifecycle POSTs are deliberately excluded,
so `get_requests + put_requests` will never reconcile with
`request_metrics.total_requests`.

With `metrics.otlp.per_bucket_enabled` set, `bytes_served`, `bytes_uploaded`, `get_requests` and
`put_requests` are exported as the per-bucket OTLP counters `s3proxy.bytes_downloaded`,
`s3proxy.bytes_uploaded`, `s3proxy.get_requests` and `s3proxy.put_requests`. `bytes_saved` is
`/metrics`-only.

[Interpreting the metrics](METRICS.md) covers per-bucket traffic in depth, including
cross-checks against Amazon S3 CloudWatch metrics.

---

## `download_bandwidth`

Always present in a delivered body, reflecting current state whether enabled or not. If the
whole section is absent, read the all-or-nothing caveat under [The endpoint](#the-endpoint)
before concluding the feature is gone.

| Field | Kind | Meaning |
|---|---|---|
| `enabled` | gauge | `false` when `max_bytes_per_sec` is `0` |
| `instance_ceiling_bps` | gauge | Current per-proxy ceiling, in bytes per second |
| `failopen_total` | counter | Fail-open events (limiter fault or DRR timeout) |
| `class_bytes` | counter map | Per-class cumulative bytes, capped at `max_tracked_classes` |
| `residual_bytes` | counter | Bytes for classes evicted from the top-K |

**`class_bytes` values can over-count.** The tracker is Space-Saving, so a retained class
inherits the evicted minimum when it displaces another class. Treat the values as
heavy-hitter estimates, not exact totals. Every byte is counted somewhere, either in a
class or in `residual_bytes`. Note the OTLP export folds the residual into
`download_bandwidth.class_bytes` with `class="residual"` rather than a separate metric.

See [Download bandwidth limits](BANDWIDTH_QOS.md).

---

## `coalescing`

Nullable in type, always present in practice. Download coordination.

| Field | Kind | Meaning |
|---|---|---|
| `waits_total` | counter | Requests that waited on an in-flight fetch |
| `cache_hits_after_wait_total` | counter | Waiters served from cache after the wait |
| `timeouts_total` | counter | Waiters that timed out |
| `s3_fetches_saved_total` | counter | S3 fetches avoided by coalescing |
| `average_wait_duration_ms` | derived | `sum / count` over all samples. A lifetime mean that never decays, unlike the windowed means elsewhere |
| `fetcher_completions_success` | counter | Fetches by the request that other requests were waiting on, which completed and notified its waiters of success |
| `fetcher_completions_error` | counter | Such fetches that notified their waiters of a failure instead. Recorded when the fetcher's conditional revalidation of a cached entry got `401`/`403`, an unexpected status, or a transport error. The application log names which, at debug level. A `401`/`403` here is a credentials problem, and the cached entry is kept |
| `waiter_conditional_304` | counter | Waiter revalidation returned 304: the cached copy was served, or, when the client sent its own `If-None-Match` or `If-Modified-Since`, S3's `304` was returned to it |
| `waiter_conditional_200` | counter | Object changed mid-flight; waiter served S3's fresh body |
| `waiter_conditional_4xx` | counter | Auth or client error; returned to client, cache preserved |
| `waiter_conditional_error` | counter | 5xx or transport error; degraded cache-serve fallback |

`s3_fetches_saved_total` is the direct benefit figure. See
[Download coordination](CACHE_READ_PATHS.md#download-coordination).

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
the proxy: it keeps the last known-good set and carries on, so a broken edit is otherwise
silent.

---

## `cache_size`

Nullable: `null` until the cache manager has finished initializing, which is when the size
tracker is created. In practice that means a brief window at startup rather than a permanent
state.

`current_size` and `write_cache_size` duplicate `cache.total_cache_size` and
`cache.write_cache_size`, which come from a different accessor. `last_validation`,
`last_validation_drift` and `accounting_drift` are published only here.

`last_validation` reports when the last scan finished, in either mode. A rolling scan
extrapolates the object count in `size_state.json` from the first-level shard directories it visited
rather than counting the population. Treat the object count as an estimate unless the last scan was
a full one.
Installing any scan's figures, full or rolling, replaces the count with the scan's own and
discards the object additions and removals that proxies have recorded but the consolidation cycle
has not yet applied. An object published after the scan passed its part of the cache can therefore
be missing from the count, and one removed after that point can still be counted. A later full
scan corrects both.

Between scans, that object count moves once per consolidation cycle. Each proxy adds the objects
its requests publish and subtracts the ones they remove in memory, flushes the sum with its
byte deltas, and the consolidation cycle applies it to `size_state.json`. The published count
therefore lags writes by up to one consolidation cycle's elapsed time, normally about
`consolidation_interval`, and no request waits on the
size-state lock to update it.


| Field | Kind | Meaning |
|---|---|---|
| `current_size` | gauge | Tracked total bytes |
| `write_cache_size` | gauge | Tracked write-cache bytes |
| `last_checkpoint` | **placeholder** | Always the current time, set to `now()` on every read. Not a checkpoint timestamp |
| `last_validation` | gauge | Unix seconds, or `null` if no scan has run |
| `last_validation_drift` | gauge | Signed byte drift the last scan found, or `null` if none has run |
| `checkpoint_count` | **placeholder** | Always `0` |
| `delta_log_size` | **placeholder** | Always `0` |

**Three of these are placeholders**, like the `connection_pool`
placeholders above: `last_checkpoint`, `checkpoint_count`, and `delta_log_size` are
hardcoded rather than measured. The journal consolidator persists `size_state.json`
directly, and the delta files it uses are transient. Use
`consolidation.last_consolidation_timestamp` and `consolidation.consolidation_count`
instead; those are real.

The accuracy signal for tracked size is
[`cache_size.accounting_drift`](#cache_sizeaccounting_drift--how-far-the-running-size-counters-had-drifted).
`last_validation_drift` holds a different figure depending on which scan wrote it; see
[The correction is unconditional](SHARED_STORAGE.md#the-correction-is-unconditional).

### `cache_size.accounting_drift` — how far the running size counters had drifted

`null` until a size validation has installed a sample on this volume. Each validation, whichever
proxy runs it, compares the running counters in `size_state.json` against the figures it computed,
corrects the counters, and writes this comparison to
`metadata/_shard_state/accounting_drift_sample.json`. It is therefore fleet-wide, and describes
the last validation on the volume rather than anything this process did.

| Field | Kind | Meaning |
|---|---|---|
| `instance_id` | string | The proxy (`{hostname}:{pid}`) that ran the validation |
| `sampled_at` | unix seconds | When the sample was taken. Alarm on `now − sampled_at`: a low drift from a sample days old says nothing about now |
| `sample_kind` | string | Which validation produced the reference figures: `full_scan`, `rolling_extrapolation` or `unspecified`. **Read this first.** Only `full_scan` can produce a drift; the other two set `unevaluable_reason` on every population |

Three populations follow, `total_size` (bytes), `write_cache_size` (bytes) and `cached_objects`
(cache metadata `.meta` entries), each with the same five fields:

| Field | Kind | Meaning |
|---|---|---|
| `persisted` | gauge (nullable) | The running counter the validation compared against, e.g. `cache_size.accounting_drift.total_size.persisted`. For a full scan it is the figure recorded just before the scan started, including pending size deltas. `null` when `size_state.json` could not be read |
| `reference` | gauge (nullable) | The figure the validation computed. For a full scan it is a census taken over the scan's duration. The counters are corrected by `reference − persisted` rather than set to it, so writes and deletes made during the scan are kept. `null` when the validation supplied none for this population |
| `drift` | gauge (nullable, signed) | `reference − persisted`. **Negative means the running counter was over-stating**, which makes the cache evict before it needs to. `null` whenever `unevaluable_reason` names a cause other than `reference_zero` |
| `drift_ratio` | gauge (nullable) | `abs(drift) ÷ reference`. Compare this rather than `drift` across caches of different sizes. `null` when `drift` is, and when `reference` is `0` |
| `unevaluable_reason` | string (nullable) | Why `drift` or `drift_ratio` is `null`: `persisted_state_unreadable`, `figure_not_supplied`, `reference_is_extrapolation` (a rolling cycle extrapolates from the counter it would be compared against), `sample_kind_unspecified`, or `reference_zero` (the ratio alone is missing; `drift` is still published). `null` when both are published |

The published reading on a large cache is often `rolling_extrapolation` with every drift `null`,
because a full scan does not fit there. That is the absence of a measurement, not a zero drift. A
`full_scan` sample with a `drift_ratio` that stays large across validations means size bytes are
being credited or debited somewhere unpaired; see `last_validation_drift` above and
[Running a fleet on one cache volume](SHARED_STORAGE.md). Over OTLP each field is one series with a
`figure` attribute naming the population; see
[Cache size accounting drift](OTLP_METRICS.md#cache-size-accounting-drift).

---

## `consolidation`

Nullable: `null` when no journal consolidator is attached, and also when `size_state.json`
exists but cannot be read or parsed. A missing `size_state.json` is not an error: the section
is present and reads zero, which is the reading of an empty cache.

This section, `cache.total_cache_size` and its components, and the `/health` cache component
all report the consolidator's last in-memory reading of `size_state.json`, which every
consolidation cycle refreshes. A scrape reads nothing from the cache volume for them, so it does not wait on the
volume under heavy write load, and the figures lag the file by up to one
`consolidation_interval`. `null` means the consolidator's most recent read of the file failed.

`cache_size_bytes`, `cache_write_size_bytes` (gauges), `consolidation_count` (counter),
`last_consolidation_timestamp` (derived unix seconds, `0` until a consolidation has been
recorded, including when `size_state.json` does not exist yet).

A `last_consolidation_timestamp` that stops advancing means the consolidator has stalled,
and metadata updates are accumulating in the journals unapplied.

`consolidation_lock_held_secs` (gauge, per-proxy) is how long this proxy has held the global
consolidation lock, `0` when it does not hold it. The other four fields come from the shared
`size_state.json`; this one is local to the proxy that serves the request. A healthy cycle holds
the lock for seconds. A value that keeps climbing past `consolidation_cycle_timeout` means a
cycle on this proxy is stuck while holding the lock, and every proxy has stopped collecting size
deltas and evicting. Once the hold reaches `max(10 × consolidation_cycle_timeout, 5 min)`, the
proxy releases the lock so another proxy can take it, logs an error, and the stuck cycle writes
nothing further that depends on the lock.
`consolidation_lock_takeovers` (counter, per-proxy) counts those releases since the process
started. Any non-zero value means a consolidation cycle on this proxy stalled while holding the lock.

---

## `cache_hit_buffer`

The write-behind that carries cache-hit metadata updates to this proxy's journal. Every
cache hit that refreshes an object's TTL or bumps a range's access count goes into a RAM
buffer, which a 5 s background task appends to `metadata/_journals/{instance}.journal`. The
consolidator then folds those lines into the shared `.meta`, so a hit's effect on the metadata
is asynchronous.

Nullable: `null` when no cache manager is attached, which is a different statement from five
zeroes. A wired but idle buffer legitimately reports zeroes, so the `null` is what lets you
tell a dead write-behind from a quiet one.

| Field | Type | Meaning |
|---|---|---|
| `pending_updates` | gauge | Distinct entries in the RAM buffer at collection time, each written as one journal line at the next flush. Repeated access updates to one stored range count once, and so do repeated TTL refreshes of one object. A value near 10,000 (the buffer cap) means it is filling between ticks and flushing inline on the request path |
| `updates_recorded_total` | counter | Cache-hit updates accepted into the buffer since process start. Counts every update, including those coalesced into an existing entry |
| `entries_flushed_total` | counter | Cache-hit updates successfully written to the journal since process start. A coalesced journal line counts every update folded into it, so this compares directly with `updates_recorded_total` |
| `flush_errors_total` | counter | Cache-hit updates a flush failed to write since process start; a failed coalesced line counts every update folded into it |
| `ram_records_dropped_total` | counter | RAM-cache hits whose access record was dropped since process start because the 4,096-record queue that feeds this buffer was full, or because the task that drains the queue had ended (`try_send` returned `Closed`). Each drop loses one access count for the stored ranges that hit read, so eviction treats them as slightly colder. Normally 0; climbing means RAM hits are arriving faster than the proxy records them, or the drain task has stopped |

Read `entries_flushed_total` against `updates_recorded_total`: the difference is what is
buffered or lost. Both count updates, while `pending_updates` counts distinct entries, so
`pending_updates` can be lower than that difference. The counter advancing as `updates_recorded_total` climbs is the healthy
state. `updates_recorded_total` climbing while `entries_flushed_total` is flat means the flush
task is no longer running, and the buffer will reach its cap of 10,000 distinct entries and start flushing on
the request path.

`flush_errors_total` is the one to alert on. The lost updates carry the TTL refreshes and
access counts that eviction ranks candidates by, so losing them skews eviction toward
objects that are in fact hot. The flush returns success to its caller, so nothing else
reports it. Non-zero means checking the cache volume's free space and permissions. Individual
failures are also logged at `error` level with the journal path.

**Exported over OTLP.** All five have `cache_hit_buffer.*` instruments, so an OTLP-only
deployment can alert on `flush_errors_total`. The section is optional, and the exporter records
nothing when it is absent. With no cache manager wired into the metrics manager, the series
carry a gap rather than five zeroes; five zeroes mean wired and idle.

---

## `fill_visibility`

Nullable: `null` when no cache manager is wired in, the same rule as `cache_hit_buffer`. With a
cache manager the tracker always exists, so zeroes mean wired and idle.

A streamed GET miss publishes its range file at once but writes its metadata as a journal line,
so the `.meta` appears only when the consolidator next folds that line. Until then this proxy
holds, in process memory, what the `.meta` will say, and serves re-reads of the object it has just
filled from that record instead of going to S3 again. Nothing is written to the cache volume.
Every field is per-proxy: a peer proxy cannot see this proxy's fills, so a re-read routed
to another proxy still waits for consolidation.

| Field | Kind | Meaning |
|---|---|---|
| `entries` | gauge | Fills held now. Entries past the age limit are swept before the count is taken |
| `bytes` | gauge, bytes | Estimated memory the held entries occupy, against the 16 MiB cap |
| `recorded_total` | counter | Fills recorded: a streamed GET miss whose range file and journal line have both been written |
| `hits_total` | counter | Responses authorized from a held record: a live-TTL `Fresh` verdict issued from it, or a Mode B `If-Match` or post-`304` serve of it. Counted at the authorizing step, so a serve that then fails and falls back to S3 still counts |
| `dropped_folded_total` | counter | Entries dropped because this proxy's consolidator wrote the key's `.meta`. The normal way an entry leaves |
| `dropped_disk_meta_total` | counter | Entries dropped because a lookup found a `.meta` for the key, usually one a peer folded. The disk copy wins. A HEAD-only `.meta` describing the same object version does not drop the entry |
| `dropped_invalidated_total` | counter | Entries dropped because the key was invalidated. Either this proxy handled a PUT, a DELETE or an object found to have changed, or a lookup found a write or delete of the key acknowledged on any proxy since the fill |
| `dropped_cap_total` | counter | Entries dropped oldest-first because the 8,192-entry or 16 MiB cap was reached |
| `dropped_age_total` | counter | Entries dropped because they outlived the age limit, `max(4 × consolidation_interval, 30 s)` |
| `dropped_range_file_changed_total` | counter | Entries dropped because the range file they name was missing or had been rewritten when a lookup re-checked its length and modification time |

Every entry leaves by exactly one reason, so `recorded_total` minus the six `dropped_*` counters
minus `entries` is zero. On a healthy proxy nearly every entry leaves as `dropped_folded_total` or
`dropped_disk_meta_total` within one consolidation interval.

A rising `dropped_age_total` means consolidation is taking longer than the age limit; read
`consolidation.last_consolidation_timestamp`. A rising `dropped_cap_total` means more fills per
interval than the tracker holds. Neither affects correctness: a dropped entry is a cache miss until
the `.meta` appears, which is the behavior without the tracker. `hits_total` flat while
`recorded_total` climbs is expected when re-reads land on other proxies, and when a client's
requests are all first reads.

---

## `orphan_reclaim`

Always present. The orphan rotation removes range files that no `.meta` lists and no journal
holds an `Add` for. It runs while every proxy active on the cache volume supports it (see
[Orphan recovery](SHARED_STORAGE.md#orphan-recovery)).
Every field is per-proxy and counted since process start. The counters are live on every
request. The two rotation gauges are read from the shared rotation checkpoints at the end of this
proxy's last pass, so they describe every proxy's progress as this proxy last saw it.
Orphan bytes were never in the accounted cache size, so `bytes_reclaimed` does not reduce
`cache.total_cache_size`. It counts space returned to the volume outside the accounted total.

| Field | Kind | Meaning |
|---|---|---|
| `enabled` | boolean | Whether this proxy has started the rotation: `true` once its cache is initialized |
| `found` | counter | Candidates: range files the key's `.meta` did not list when their directory was scanned |
| `reclaimed` | counter | Candidates unlinked. Only a `confirmed_absent` evidence read permits an unlink |
| `bytes_reclaimed` | counter, bytes | On-disk size of the unlinked candidates |
| `skipped_unconfirmed` | counter | Candidates kept because the evidence was `peer_unavailable` or `indeterminate`, the cache key could not be recovered from the file name, the `.meta` was unreadable, or a lock, stat or unlink failed. A range file is also kept when it is less than 10 minutes old, or when its object has a multipart upload in progress. It is kept, too, when the list of uploads in progress could not be read in full. It is also kept when a different file replaced it after it was judged, or when a proxy that is not yet ready joined the fleet while the pass ran |
| `kept_listed` | counter | Candidates kept because the `.meta` listed them by the time the key's lock was held |
| `journal_evidence_total` | object of four counters | One count per journal-evidence read, taken under the key's lock, keyed by outcome (below) |
| `passes` | counter | Rotation passes this proxy has run |
| `passes_gated` | counter | Passes skipped, or stopped before their next directory. A pass waits when an active proxy does not support the rotation, when no proxy has written a journal entry in the last hour, or when the list of active proxies could not be read. An active proxy is one that wrote a journal entry in the last hour. Nothing is lost while passes wait; orphaned files stay until the next pass. A sustained rise is worth an alert only while the fleet serves traffic and every proxy supports the rotation; see [During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade). A candidate whose check fails under its lock is counted in `skipped_unconfirmed` instead |
| `leases_unreadable` | counter | Reads of a lease file that did not parse, in the orphan rotation's claims or the eviction work list's chunk leases. A crash between creating a lease file and writing it leaves one |
| `leases_unreadable_expired` | counter | Of those, files at least 30 minutes old: read as expired and removed, so the directory or chunk they blocked is claimable again |
| `units_visited` | counter | Rotation units this proxy claimed and visited. A unit is one `(bucket, first shard level)` directory, or the flat files at the `ranges/` root |
| `stalest_checkpoint_age_secs` | gauge, seconds, nullable | Age of the oldest completed visit among units that have one, advanced to request time. `null` before this proxy's first pass, or while no unit has completed a visit |
| `units_never_scanned` | gauge, nullable | Units no proxy's rotation has completed yet. `null` before this proxy's first pass |

`journal_evidence_total` keys:

| Key | Meaning |
|---|---|
| `confirmed_absent` | Every journal was read in full and none holds an `Add` for the range. The file is unlinked |
| `journal_present` | A journal holds an `Add` for the range: a fill not yet consolidated. The file is kept |
| `peer_unavailable` | A journal file could not be opened or read. The file is kept |
| `indeterminate` | The journal directory could not be listed, a line was torn, or a journal rewrite was in progress. The file is kept |

Read `reclaimed` against `found`. A rising `found` with a flat `reclaimed` and a rising
`skipped_unconfirmed` is not a fault by itself. On a busy cache it rises normally, because new
files and parts of uploads in progress are kept by design. A journal that cannot be read shows as
`peer_unavailable` or `indeterminate` in `journal_evidence_total`. `journal_present` rising and falling between passes is normal on a busy cache. A
`stalest_checkpoint_age_secs` far beyond the time one rotation should take, or a
`units_never_scanned` that stops falling, means the rotation is not reaching part of the volume.

---

## `atomic_metadata`

Nullable in type, always present in practice. Journal and lock internals: 17 fields,
all counters, per-proxy and monotone since process start. Mostly useful when diagnosing
coordination on the cache volume; see
[Running a fleet on one cache volume](SHARED_STORAGE.md). The four
`metadata_lock_*` figures, the three `range_file_corruption_*` counters,
`range_file_identity_mismatch_detected` and `journal_validation_failures` are exported over OTLP, which is every field in the section.

- **Metadata slot locks**: `metadata_lock_acquisitions_successful`,
  `metadata_lock_acquisitions_failed`, `metadata_lock_hold_time_ms_total`,
  `metadata_lock_wait_budget_expired_total`

  The first four cover the per-key metadata slot lock, which serializes `.meta`
  read-modify-write. They carry the `metadata_lock_` prefix to keep them distinct from
  `eviction_coordination`'s `lock_acquisitions_successful` and `lock_acquisitions_failed`,
  which count the global eviction lock, a different lock.

  `metadata_lock_acquisitions_failed` counts refused attempts, so a contended
  acquisition that succeeds after retrying adds to both it and the success count;
  `cache.metadata_lock_retry_exhausted_total` counts the callers that gave up.
  `metadata_lock_hold_time_ms_total` is a cumulative sum; divide it by
  `metadata_lock_acquisitions_successful` for a mean hold time.

  `metadata_lock_wait_budget_expired_total` counts acquisitions that gave up because
  `cache.shared_storage.lock_acquire_wait_budget` elapsed. It and
  `cache.metadata_lock_retry_exhausted_total` are the same event on the two acquisition
  paths (one bounded by elapsed time, the other by attempt count), and a deployment
  normally produces both. Neither is a lock expiry: giving up never takes a lock a peer
  still holds. A sustained non-zero reading here alongside a large hold time per
  acquisition means the budget is short relative to how long critical sections take on
  this volume.

  All four cover every acquisition of a slot lock, on the request, metadata-write,
  eviction and cache-maintenance paths alike. They are a total, not a lower bound.

  A metadata lock is never taken from its holder, so the section has no stale-lock figure.
- **Corruption**: `range_file_corruption_detected`
  Cached range files whose LZ4 data failed to decode while being read for a client, on both
  the buffered and the streamed read path. Each one also logs `Cached range file is corrupt`
  at `warn` with the file name. The request falls back to S3. A rising count with a flat
  `cache.corruption_missing_range_total` means the volume is returning damaged bytes rather
  than losing files: check the volume's integrity. A range file that decodes cleanly to a
  length other than its metadata records is counted too. Every writer names a range file by
  the bounds it holds, so an overwrite of the object cannot produce one. Read errors are not
  counted. Each damaged file is normally counted once per proxy, by the read that finds it: later
  reads of it on that proxy do not read it (see the heals below). Reads already in flight when
  the damage is found each count it, and so does a read after the proxy's mark for the file has
  lapsed or been dropped, or after a restart. Corrupt `.meta` files are
  counted in
  `cache.corruption_metadata_total`; unparseable journal lines in
  `write_cache.journal_unparseable_lines_retained`
- **Corruption heals**: `range_file_corruption_healed`, `range_file_corruption_heal_suppressed`
  `range_file_corruption_healed` counts damaged range files this proxy removed, with their entry
  in the object's metadata, when a read found them. Each one logs `Damaged range file
  invalidated` at `warn`. Only the damaged range is removed; the object's other cached ranges
  keep serving. The read that finds a damaged file also marks it on that proxy, so the next
  read of that range there fetches it from S3 without reading the file. That holds whether or not
  the file could be removed. A range that only a pending journal entry lists is removed once a
  consolidation pass lists it, and a file past the heal limit stays in place. Each read a mark
  turns away retries the removal. A mark lasts until its file is removed or replaced, or until
  two minutes after the last read it turned away; each proxy holds at most 4,096 and drops the
  oldest beyond that. Each proxy removes at most 16 a minute. Past that, a damaged file is left
  in place, and the read that found it and each later read the mark turns away add to
  `range_file_corruption_heal_suppressed` and log `heal limit reached` at `warn`. A request
  refused on the streamed path that then falls back to the buffered path adds two and logs two
  such lines. A non-zero
  `range_file_corruption_heal_suppressed` means the volume is returning damaged bytes faster
  than the proxy will refetch them: check the volume's integrity
- **File identity mismatches**: `range_file_identity_mismatch_detected`
  Cached range files a read refused because the file it opened is not the one the cached
  metadata describes. Its length differs, or its modification time differs by 1 ms or more,
  from the ones recorded when the range was cached. Usually another version of the object
  written to the same file, or a cache volume copied with a tool that does not keep file
  modification times, so it is counted here and not in `range_file_corruption_detected`. Checked when the file is opened,
  for a read for a client and before copying a range to the RAM cache. Each one logs `is not
  the file its listing describes` at `warn` with the file name and both identities. The read
  sends nothing from that file: a read that has not sent its response is answered from S3, and
  a response already streaming earlier ranges from the cache ends early with a connection
  error. The range is then removed and counted in `range_file_corruption_healed`, outside the
  heal limit, so the next read caches it again. A range whose metadata records no file identity
  is not checked
- **Journal**: `journal_validation_failures`
  Range journal entries the consolidator discarded because their range file had still not
  appeared five minutes after the entry was written, so the
  size credit the entry carried was never applied. An entry dropped because its range was
  just evicted is not counted. A few are expected when an object is overwritten or
  invalidated before its entries are consolidated; a count that keeps climbing on a steady
  workload means range writes are not landing on the volume. Use
  `consolidation.consolidation_count` and `consolidation.last_consolidation_timestamp` for
  whether consolidation is running

---

## `eviction_coordination`

Nullable in type, always present in practice. All seven counters:
`lock_acquisitions_successful`, `lock_acquisitions_failed`, `stale_locks_recovered`,
`evictions_coordinated`, `evictions_skipped_lock_held`,
`staging_evictions_skipped_lock_held`, and `total_lock_hold_time_ms`,
which is a cumulative sum, not a mean.

`staging_evictions_skipped_lock_held` is the write cache's counterpart of
`evictions_skipped_lock_held`: write-cache eviction passes skipped because another proxy held
the global eviction lock. The read cache and the write cache share the lock but not the
trigger, so they are counted separately. Folded together, they could not show whether
read-cache eviction is starving the write cache of the lock. Both are per-proxy.

---

## `signed_put`

Nullable in type, always present in practice.

`cached_puts_total`, `bypassed_puts_total`, `cache_failures_total` (counters);
`average_cached_bytes`, `average_streaming_duration_ms` (windowed, integer means).

A climbing `bypassed_puts_total` means the write cache is declining objects, usually
on `write_cache_max_object_size` or capacity. See
[Write cache configuration](CONFIGURATION.md#write-cache-configuration).

### `skipped_puts_total`

A map, always present, `{}` when nothing has been skipped. Keys are reason strings and values
are counts, the same map shape as `bucket_traffic` and
`cache.cache_bypasses_by_reason` above. The keys below are a minimum, not a closed set, so a
consumer should accept a key it does not recognize.

Reason keys:

| Key | Meaning |
|---|---|
| `write_cache_disabled` | Caching disabled by rule for this key. Also counts a `CreateMultipartUpload` on such a key |
| `unknown_length` | Decoded content length could not be determined (e.g. chunked-encoded signed PUTs) |
| `disk_safety` | The write cache skipped the object because caching it would take the cache past `max_cache_size`, or would leave less free space on the cache volume than `cache.disk_safety_free_space_floor`. That floor defaults to 1 GiB, and `0` disables this check. The only capacity-shaped refusal, and the remedy is more disk or a lower `max_cache_size` rather than a different `write_cache_percent`. While this is being recorded, `/health` reports the cache component `Degraded`, a recency signal that clears 300 s after the last refusal, so it reflects "happening now" rather than "happened once". The upload itself always succeeds and is never affected |
| `object_too_large` | Upload exceeds `write_cache_max_object_size` |
| `commit_failed` | The write-cache commit failed after the object was already streamed to S3 |
| `capacity_unknown` | No cache-size figure had been read since the proxy started and the first read did not finish within 250 ms, so the write was forwarded uncached. The key's previous cached version is still invalidated |
| `origin_marker_unresolved` | The bucket's origin marker could not be read or claimed within 250 ms, so the write was forwarded uncached. The key's previous cached version is still invalidated. Also counted in `cache.origin_marker_unresolved_bypasses_total` |
| `rules_unknown` | The cache rules could not be resolved within the write's wait: 250 ms, or what remained of the write's 1,000 ms cache-wait total if less. This happens when the first load of the rules since the proxy started has not finished. It also happens when a stale reload this write started has not finished and less than 100 ms of the write's cache-wait total remained. The write was forwarded uncached, and the key's previous cached version is still invalidated |
| `volume_stalled` | This proxy treated the cache volume as stalled, so the write was forwarded uncached. The key's previous cached version is still invalidated, in the background |
| `non_object_put` | A PUT that writes a subresource (`?tagging`, `?acl`, `?retention`, `?legal-hold`, `?renameObject`) or copies, renames or appends to an object. Its body is not the object, so it is forwarded uncached and the key's cached copy is invalidated |
| `no_etag` | S3's successful response to a single-part PUT carried no `ETag`, so the store declined. The key's previous cached version is still invalidated |
| `backlog_full` | 1,024 PUTs or multipart parts on this proxy were already waiting for their background cache store, so this one was forwarded uncached. Also counts a `CreateMultipartUpload` whose tracker was not written, and a `CompleteMultipartUpload` not finalized, because the write-cache backlog was full. Sustained growth means PUTs are arriving faster than the cache volume commits them; the cached fraction is roughly the commit rate divided by the PUT rate. The key's previous cached version is still invalidated |
| `hostname_not_unique` | Another running proxy holds this one's whole identity, the same hostname and the same process id, so the two would write the same per-proxy accounting files. This proxy stops caching writes for the life of the process: the write is forwarded to S3 and served normally, and the key's previous cached version is still invalidated. Reads are unaffected. A deployment error, not a transient: see [Each proxy needs its own hostname](SHARED_STORAGE.md#each-proxy-needs-its-own-hostname) and `cache.hostname_collisions_total` |
| `superseded` | Another PUT, CompleteMultipartUpload or DELETE of the same key was acknowledged, on any proxy sharing the cache volume, after this write but before its cache store published, so the store did not publish. The newer write's own store, or the next GET, caches the current object. Occasional counts are expected when one key is overwritten in quick succession |
| `multipart_tracking_full` | The cache volume already held 5,000 in-progress multipart entries, counting each upload the write cache is tracking and each of its uploaded parts, across every proxy sharing the volume. A `CreateMultipartUpload` counted here writes no tracker, so the upload is not cached. An `UploadPart` counted here is not staged, its upload stops being cached, and the parts the upload had staged are removed. The upload itself completes at S3 as usual, and its CompleteMultipartUpload still invalidates the key's previous cached version. An `UploadPart` whose upload has no staging directory yet counts as two entries, because staging it creates the directory too. The cap keeps the walk of in-progress uploads short: the metrics collector makes it once per collection interval, and the readers that decide on its figure up to once every 5 seconds. Sustained counts mean more multipart uploads are running at once than the write cache can track |
| `multipart_tracking_unknown` | The count of in-progress multipart entries could not be read, because a listing of `mpus_in_progress/` failed, so the `CreateMultipartUpload` or `UploadPart` was forwarded uncached. Unlike `multipart_tracking_full`, the parts the upload had already staged are kept, though the upload is then not cached because a part is missing. The application log names the listing error. Sustained counts mean the cache volume cannot list `mpus_in_progress/` |

A PUT counted here still succeeds against S3; these are write-cache skips, not
upload failures. A non-zero total here alongside flat cache-population figures is the
signal that a fleet is taking PUT traffic and caching none of it.

---

## `write_ledger`

Always present; zeroes before this proxy's first write-cache commit. Every field is
**per-proxy** and counted since process start.

A staged write's accounting is appended in two stages: a ledger entry under
`metadata/_write_ledger/{bucket}/{XX}/{instance}.ledger`, and, once that line is durable, a
paired credit record in `{instance}.pending`. Each stage drains its queue in batches and writes a
whole batch under one lock hold, so these four counters are how an operator sees batch sizes and
whether the second stage is losing records.

| Field | Kind | Meaning |
|---|---|---|
| `append_batches_total` | counter | Batches the ledger-append stage drained from its queue, failed batches included. The denominator of the mean batch size |
| `append_batched_entries_total` | counter | Entries in those batches. The numerator of the mean batch size, and the count of staged-write records this proxy has submitted to the stage |
| `pending_batches_total` | counter | Batches the paired-credit stage drained, each one hold of `{instance}.pending.lock` |
| `pending_batch_failures_total` | counter | Those credit batches whose write failed, at a lock timeout, a write error or a sync error. **Non-zero is a finding, not a routine reading** |

**`append_batched_entries_total / append_batches_total` is the mean batch size, and it is the
figure to read first.** One hold of a shard ledger file's lock carries a whole batch's lines for
that shard. The ratio therefore separates a commit rate set by batching from one set by the
per-hold latency of the cache volume. At 1, each hold carries one entry; above 1, each hold is amortized
over that many. A batch is whatever arrived while the previous one was being written, so the
ratio is 1 on an idle or lightly loaded proxy and rises with the submission rate. A ratio of 1
under load means the volume is answering faster than writes arrive, not that batching is off.

**`pending_batch_failures_total` is the field to alert on.** Each failure lost up to 1,024 credit
records whose ledger entries are durable, so the staged figure (`write_cache.resident_bytes` once
`write_cache.staged_authority_epoch` is `1`) under-states those bytes until a staged-fold pass
visits the shards involved. The stage marks each of those shards dirty, so the next pass visits
them ahead of its stalest-first order and the cut it installs covers the entries. Expect the gap
to close within one pass, and read `write_cache.staged_fold_dirty_shards` to see the work
outstanding. Each failure also logs one WARN carrying the record count.

---

## `metrics_collection`

Always present. What the background collection costs: the task that runs every
`metrics.collection_interval`, produces the cache-volume figures a scrape serves, and records the
OTLP instruments. A `/metrics` request or dashboard refresh performs no collection of its own, so
none of these move on a scrape. All per-proxy, since process start.

| Field | Kind | Meaning |
|---|---|---|
| `collections_total` | counter | Background collections completed. Collections, not HTTP scrapes. The denominator for the two duration figures |
| `duration_ms_last` | gauge, milliseconds | Wall-clock time the most recent collection took |
| `duration_ms_max` | gauge, milliseconds | The longest collection since process start. One slow run stays visible here after `duration_ms_last` has moved on |
| `slow_collections_total` | counter | Collections that took longer than `slow_threshold_ms`. Each also logs a WARN, at most once per 60 s |
| `slow_threshold_ms` | config-derived, milliseconds | Half of `metrics.collection_interval` after clamping. Published so the threshold in force is readable from outside the process |

`slow_collections_total` above zero means some collection used more than half its slot, one
doubling of its cost away from overrunning the interval. A collection that overruns its slot
skips a tick, and the cache-volume figures on display are then two intervals old; read
`volume_snapshot.age_secs` beside it. A rising count usually points at a slow cache volume. Raising
`metrics.collection_interval` buys headroom at the cost of older figures.

---

## Configuration fields that do not affect this payload

`metrics.include_cache_stats`, `metrics.include_compression_stats` and
`metrics.include_connection_stats` have no effect. They are parsed, and a startup warning is
logged when one is set to `false`, but nothing reads them: every section is always included. Delete them.
Whether a section appears depends on whether its source component was wired in at startup.

`metrics.collection_interval` does affect the payload. It is the period of the background
collection that produces the cache-volume figures a scrape serves and the OTLP samples, clamped
to between 10 s and 86,400 s. A scrape does not collect those figures itself.
