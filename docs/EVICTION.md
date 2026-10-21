# Cache eviction

This page explains how Hybrid Cache for Amazon S3 frees space on the cache volume: what triggers eviction, how a
pass chooses what to remove, how the write cache is reclaimed, and how to recover when a size
figure is wrong. Eviction works on individual ranges, so a hot range survives even when the rest
of its object is cold.

Thresholds and their valid ranges are in the
[eviction configuration](CONFIGURATION.md#eviction-configuration). How a fleet shares the
eviction lock is in [Distributed eviction](SHARED_STORAGE.md#distributed-eviction).

## Triggers

Eviction runs from two places:

1. **At startup**, the proxy checks whether the cache is already over `max_cache_size` and
   evicts if it is.
2. **During and after every consolidation cycle**, the journal consolidator compares the cache's
   size against `cache.eviction_trigger_percent` and starts a pass if the size is over it. The
   comparison runs every `cache.shared_storage.consolidation_interval` while the cycle processes
   keys, and once more when it finishes, so a cycle that spends its whole budget on a journal
   backlog still starts a pass within one interval. Only the proxy holding the consolidation
   lock starts a pass, and each proxy runs one pass at a time: while a pass is in flight, the
   comparison is skipped. A cycle with no journal entries or size changes to apply still
   checks. It reads the stored size at most once a minute while the cache is under the trigger,
   and takes the lock only when the cache is over it, so an idle fleet that is over its trigger
   evicts without waiting for new traffic.

   **Both capacity limits use that cadence**, each on its own figure: the byte limit above and
   the write cache's bounds (see [Write cache reclamation](#write-cache-reclamation)). A
   comparison made while the cycle is working costs the cycle the time it takes to read the size
   figures, so the cycle consolidates slightly fewer journal keys. Consolidation is not on the
   request path, so no read or write waits for it.

   After a pass that reclaimed nothing, the next comparison waits instead of repeating at once.
   A pass reclaims nothing when another proxy holds the eviction lock, when the shortfall came
   out at zero, or when the previous round of removals has not finished. In each case, finding
   that out costs cache volume reads that are not worth repeating every interval. The byte
   comparison waits until this proxy's own eviction work moves on: a new round of removals is
   listed, this proxy runs a pass alone because the work list is not in use, or this proxy
   finishes a removal of the previous round. It never waits longer than two consolidation
   intervals, the bound that applies when the rest of the fleet finishes a round on its own,
   since nothing then moves on this proxy. The wait carries across consolidation cycles, so a
   long round of removals is not re-read every few seconds. A comparison that comes out under
   the trigger ends the wait, so a cache that drops below its trigger and crosses it again is
   acted on at once. The write cache and object-count comparisons each wait two intervals.

   The figure compared is the shared size state plus every size change the fleet has recorded
   and no cycle has applied yet. A cache that crosses its trigger while a cycle is running is
   therefore seen on that cycle's next comparison rather than on the next cycle. That figure is
   published as `cache.eviction_backlog_projected_bytes` on `/metrics`. The resident figure
   beside it, `cache.eviction_backlog_bytes`, can read under the trigger while passes are firing.
   Where the figures come from and how they are corrected is described in
   [Re-grounding the size figures the triggers read](#re-grounding-the-size-figures-the-triggers-read).

Both places compare bytes against `max_cache_size`, which counts compressed payload bytes. Every
cached object also uses a metadata file and directory space on the volume that the limit does
not count, so size the volume with headroom for small-object workloads.

Storing new data does not check capacity itself: a write is never blocked or delayed waiting on
an eviction decision. The consolidation cycle runs on its own schedule, independent of request
traffic, so capacity is checked regularly rather than only when a write happens to trigger it.

## Thresholds

Eviction starts at `cache.eviction_trigger_percent` of `max_cache_size` (default `95`), and a
pass selects enough ranges to bring the recorded size down to `cache.eviction_target_percent`
(default `80`). The target sets the byte budget of one pass. Usage need not reach it: new data
arrives while the pass deletes, so under sustained fills usage can stay above the target. Both
values are configurable; see the [eviction configuration](CONFIGURATION.md#eviction-configuration)
for their valid ranges.

## Eviction algorithms

Cached data has one eviction algorithm, TinyLFU. `eviction_algorithm: "lru"` is accepted,
behaves as `"tinylfu"`, and logs a deprecation warning at startup; see the
[eviction configuration](CONFIGURATION.md#eviction-configuration).

**TinyLFU (decayed frequency).** The RAM cache and the disk cache score victims with the same
function:

```text
decayed_frequency(access_count, idle_secs) = access_count >> min(idle_secs / 3600, 63)
```

- A range's access count halves for each hour it sits idle. Ranges are ordered by
  `(decayed_frequency, last_accessed)`: lowest decayed frequency first, with the oldest
  `last_accessed` breaking ties. In the RAM cache this score alone picks the victim. On disk it
  is the third sort key, after expiry and compressed size (see
  [Range-level granularity](#range-level-granularity)).
- A range that was read often but is now idle is shielded from a single one-hit read or scan:
  its decayed frequency stays above that of a range read once, so the one-hit range scores
  lower.
- Decay only lowers a range's score toward 0 while it sits idle. It never resets a TTL or forces
  expiry. TTL expiry and revalidation remain the limit on how long stale data can stay when
  there is no capacity pressure; decay matters only once eviction needs to reclaim space.

**LRU for the RAM metadata cache.** The RAM metadata cache, which holds `.meta` documents rather
than cached data, evicts the least recently used entry instead. Its entries are all about the
same size (roughly 1 to 2 KiB), so recency alone is enough to pick a victim. When the cache
reaches `cache.metadata_cache.max_entries` (default `100000`), the least recently used entries
leave RAM only: the `.meta` file on disk is untouched, and an evicted entry is reloaded from
disk on its next access. See [RAM metadata cache](CACHE_INTERNALS.md#ram-metadata-cache).

## Range-level granularity

All eviction works at the range level:

- Every cached range is an independent eviction candidate.
- Ranges are sorted expired first, then largest first, then by TinyLFU score.
- A range can be retained while other ranges of the same object are evicted.
- An object's metadata file is deleted only when all its ranges are evicted.

A request that spans retained ranges with an evicted gap validates the object and fetches the
missing bytes from S3. The response stays byte-exact for the requested range, and counts as a
partial cache miss rather than a hit.

### Example

Object A is 100 MiB, cached as 10 ranges of 10 MiB each, none expired:

- Range 0-10 MiB: read 5 minutes ago, count 50. Kept.
- Range 10-20 MiB: read 2 hours ago, count 2. Eviction candidate.
- Range 20-30 MiB: read 1 minute ago, count 100. Kept.
- ...

With equal sizes and no expiry, the TinyLFU score decides, so the cold ranges go first. The
object's metadata is updated once, with all evicted ranges removed.

## Eviction flow

```text
1. Collect  → Read a bounded prefix of each cache shard's large-range index, plus each
              proxy's record of ranges cached since that index was last rebuilt. Fall back to
              scanning all .meta files when no index exists yet, when no indexed range clears
              `tail_floor_bytes`, or when the index cannot meet the target. The fallback reads
              up to 64 .meta files at a time as it walks the cache volume, and considers them
              in the same order either way. It holds that many documents at once rather than
              one per cached object, but the candidate list it returns has one entry per
              resident range, so its memory grows with the number of cached ranges.
2. Sort     → Expired first, then largest compressed size first, then by TinyLFU score
              (lowest score first)
3. Group    → Batch ranges by object, so each object's metadata is updated once
4. Evict    → Under the object's metadata lock, re-read its .meta. If the object has been
              rewritten since it was selected (a different ETag, Last-Modified or creation
              time, or a selected range now staged), skip it: nothing is deleted and nothing is
              subtracted. Otherwise delete the .bin files. If every range of the object was
              evicted, unlink its .meta (one unlink, with no rewrite first). Otherwise rewrite
              the .meta atomically, keeping the ranges that survive: one write per object, not
              one per range.
5. Account  → Subtract the bytes of the files that were actually deleted, as recorded in the
              .meta read under the lock
6. Clean up → Remove empty directories
```

### Which proxy deletes

Only the proxy holding the fleet-wide eviction lock selects (steps 1 to 3). When every active
proxy (one that wrote a journal entry in the last hour) supports the work list, the lock holder
deletes nothing itself. It writes the selection to a shared work list under
`metadata/_shard_state/eviction_worklist/`, one chunk per cache shard, and names the list in
`metadata/_shard_state/eviction_generation.json`. Every proxy then claims chunks under a lease in
`metadata/_shard_state/eviction_claims/` (`shared_storage.eviction_worklist_lease`, default 900
seconds) and runs steps 4 to 6 for them, 64 objects per batch. The lock holder starts claiming as
soon as it has written the list; the other proxies start on their next consolidation cycle. A
proxy keeps claiming chunks until none is left or a minute has passed, and starts again on its
next consolidation cycle. A proxy starts a batch only while at least half its lease remains, so
a long chunk is finished over several claims.

A proxy works up to four chunks at once and deletes up to 32 objects at a time across all of
them, so a list of small chunks still uses the volume's delete concurrency. The 32 is a total
across the chunks of one pass, not per chunk: on a volume whose IOPS are shared with the read
path, it bounds one proxy's work-list load however many chunks that proxy holds. A proxy that is
also running the single-proxy pass, as the lock holder does when the work list is not in use,
can have up to 32 more objects in flight. Each chunk keeps its own lease, batch fences,
accounting and completion marker, so a chunk that loses its lease, or a work list that is
abandoned, stops exactly as it does when chunks run one at a time.

A proxy shows that it supports the work list with a record under `metadata/_shard_state/ready/`.
It writes the record at startup, refreshes it on every consolidation cycle that has work
(whether or not it holds the consolidation lock), and refreshes it on any journal write once the
record is 15 minutes old.

The lock holder writes no new list until every chunk of the current one is done, or until the
list has been abandoned and no proxy still holds a live lease on any of its chunks. A list is
abandoned when a proxy that does not support the work list joins the fleet, or when a chunk's
completion marker stays unreadable; each proxy then stops at its next batch boundary. While any
such proxy is active, or while no proxy has written a journal entry in the last hour, the lock
holder runs every step itself until the next journal entry. `cache.eviction_last_result.mode`
reports which happened on each pass. Each consolidation cycle removes work-list generations
other than the two newest and the current one.

### When a range file cannot be deleted

Step 5 subtracts only range files that were present and removed without error. A file the proxy
could not delete (a read-only or full cache volume, restrictive directory permissions, or an
underlying I/O error) keeps its bytes counted, because they still occupy the volume.
Discounting them would make the cache look smaller than it is and admit new data against space
that was never freed, and the error would compound with each failed delete until the cache
exceeded `cache.max_cache_size`. The `/metrics` field `cache.eviction_unlink_failures_total`
counts these failures.

The range also keeps its entry in the object's metadata, and the object keeps its metadata file
even when every other range has been evicted. Removing the entry would leave a range file that
no metadata refers to, which nothing on the eviction or accounting paths would then reach. The
validation scan reads the metadata tree and never lists range directories, so it cannot see
such a file either. [Orphan recovery](SHARED_STORAGE.md#orphan-recovery) would delete it on its
own schedule, with its bytes already out of the accounted total. Keeping the entry keeps the
file accounted for, reachable, and evictable on the next pass.

A deployment in this state therefore does not drift over its limit unnoticed:
`cache.total_cache_size` stays accurate, so the eviction trigger keeps firing and eviction keeps
retrying the same files. Because the entry is still in the metadata, the next pass sees the range
as a candidate again and retries the delete, so a transient error resolves itself and a
persistent one keeps reporting until its cause is fixed. Each pass logs a warning naming the
affected object:

```text
[EVICTION_ACCOUNTING] Skipped debit for range files that did not leave the disk: cache_key=..., candidates=3, skipped=1
[RANGE_EVICTION] Failed to delete range file: cache_key=..., range_start=..., range_end=..., error=..., action=retaining_metadata_entry_for_retry
[RANGE_EVICTION] Range files could not be deleted and remain on disk: cache_key=..., retained=1, extents=[(0, 4095)], action=metadata_entries_kept_bytes_still_accounted, retry=next_eviction_pass
```

`ranges_evicted` in the `[DISK_CACHE_EVICTION]` summary counts the same set that was accounted
for, so it reports ranges actually removed rather than ranges selected.

A range file that was already absent is handled the other way round. Its bytes are not
subtracted, because there are no bytes to reclaim and the proxy cannot tell whether they were
already accounted for when the file went; reconciling that is the validation scan's job. Its
metadata entry is removed, and the object's metadata file is deleted if that was the last entry,
because the entry points at a file that does not exist. Keeping it would leave the object
permanently unevictable: every later pass would select the same range and fail on the same
missing file.

### Admission window protection

Newly cached ranges are protected from eviction for 60 seconds. Without this, a large download
could have its new ranges evicted at once, because they have no access history for TinyLFU to
score.

When a pass collects candidates, it checks each range's `last_accessed` timestamp and skips
ranges cached within the last 60 seconds, unless the object has passed its expiry. New ranges
get time to gather access statistics before they compete for space.

```text
Large file download in progress:
- Range 0-8 MiB: cached 5 seconds ago → protected (within the admission window)
- Range 8-16 MiB: cached 2 seconds ago → protected (within the admission window)
- Range 16-24 MiB: cached 3 minutes ago, access_count=1 → eviction candidate
- Range 24-32 MiB: cached 1 hour ago, access_count=50 → eviction candidate (high score)
```

**The window has one exception and no bypass.** A range younger than 60 seconds is not an
eviction candidate, however far over its limit the cache is, unless its object has passed its
expiry. The window exists to stop thrashing on freshly cached data, and an expired range is not
that. Without the exception, a range cached, expired and read inside the same minute would keep
dead data resident in preference to live data, which a short `get_ttl` makes reachable.

## Eviction at scale

### What a pass reads

A pass picks its candidates from a bounded prefix of each cache shard's large-range index
(`tail_max_entries` entries per shard, default `4096`), so the metadata it reads to choose what
to delete is set by configuration rather than by how many objects the volume holds. It reads
every `.meta` file only when the index cannot serve the pass, as described in
[Eviction flow](#eviction-flow), and that walk's duration then depends on the volume's metadata
read rate (see [Sizing the metadata rate](SHARED_STORAGE.md#sizing-the-metadata-rate)).
Measured with one proxy reading `.meta` files from an idle volume, that rate is 2,900-3,800 obj/s on EFS,
29,000-31,000 obj/s on FSx for OpenZFS, and 28,500 obj/s on FSx for NetApp ONTAP at 32 reads outstanding,
rising to 45,000 at 64. On ONTAP the fleet aggregate kept rising up to 24 proxies, the largest fleet
measured. These are the single-proxy and fleet figures of the table in
[Sizing the metadata rate](AWS_DEPLOYMENT.md#sizing-the-metadata-rate), which owns them and their
caveats.

### How the fleet shares the work

When the work list is in use, one proxy selects and every proxy deletes, each working up to four
chunks and 32 objects at a time. Adding proxies adds delete capacity without adding selection
work. See [Which proxy deletes](#which-proxy-deletes).

### Staying within `max_cache_size`

A GET-miss fill that would take the cache past `max_cache_size` is served without being stored and is
counted on `cache.read_fills_declined_max_cache_size_total`; the client's response is the same
as if it had been cached. Uploads take the same stop and are forwarded to S3 without being
cached. The cache can exceed the limit only by the fills and uploads in flight plus what other proxies admit
during the age of the shared size figure; [Error handling](ERROR_HANDLING.md) states that bound
in full.

In tests with two processes filling a cache of 1 MiB objects faster than eviction could clear it
(trigger 95%, target 80%, 5-second consolidation interval), the cache peaked at most 0.6% above
`max_cache_size` on FSx for OpenZFS at 160 MB/s. Between 4% and 21% of the offered fill was
served without caching.

### Slow or full volumes

On a volume whose throughput the fill alone saturates, the shared size figure updates less often
and the overshoot grows with its age. A range file that cannot be deleted stays counted and is
retried on the next pass (see
[When a range file cannot be deleted](#when-a-range-file-cannot-be-deleted)), and fills are
declined before the volume itself runs out of space (see
[Free-space floor and the danger band](CONFIGURATION.md#free-space-floor-and-the-danger-band)).

### Request serving

Each proxy has at most 32 work-list deletes in flight. Each object's deletes and metadata commit
run on a blocking thread, not on the threads that serve requests.

## Per-range access tracking

Each range keeps its own access statistics in the metadata file:

```json
{
  "ranges": [
    {
      "start": 0,
      "end": 8388607,
      "last_accessed": "2024-01-15T10:30:00Z",
      "access_count": 42
    },
    {
      "start": 8388608,
      "end": 16777215,
      "last_accessed": "2024-01-15T08:15:00Z",
      "access_count": 3
    }
  ]
}
```

- `last_accessed`: time of the last access (the TinyLFU score's tiebreak)
- `access_count`: number of accesses (the TinyLFU score's input)

## Metadata cleanup

The `.meta` file is deleted only when all its ranges have been evicted and no range file was
left behind by a failed unlink. Empty parent directories are then removed recursively.

Metadata lock files are not deleted with the object. Each is a shared slot in a fixed set, so
removing one would remove a file that holders of other keys are locking. Because `flock` binds
to the open file description rather than to the path, the removal would release nothing while
handing the next opener an independent lock on a different inode.

Three temporary-file shapes have no automatic cleanup: a proxy's own in-flight metadata write
(`{key}.meta.tmp.{host}.{pid}`), a fixed-name metadata temporary file used by locked writers
(`{key}.meta.tmp`), and a range file mid-write (`{key}_{start}-{end}.bin.tmp`). The write that
created each one normally removes it when it completes; one left behind by a crash stays until
it is removed by hand. Metadata temporary files are small, but a range temporary file can be as
large as the range being written. Do not remove these files while any proxy sharing the volume
may still be writing. Stop the writers first, because the fixed-name and range forms carry no
liveness marker.

## Logging

Eviction logs a start line and a completion line at INFO:

```text
[DISK_CACHE_EVICTION] Starting eviction: usage=142.5 MiB / 150.0 MiB (95.0%), target=120.0 MiB (80%), to_free=22.5 MiB
[DISK_CACHE_EVICTION] Eviction completed: keys_evicted=3, ranges_evicted=47, freed=24.0 MiB, new_usage=118.5 MiB / 150.0 MiB (79.0%)
```

Per-range and per-object deletion detail is logged at DEBUG. The completion line's fields:

- `keys_evicted`: objects that had at least one range evicted
- `ranges_evicted`: individual ranges evicted
- `freed`: bytes freed, human-readable
- `new_usage`: cache size after eviction, against the maximum, with a percentage

Metadata deletion and directory removal log separately:

```text
[BATCH_EVICTION] Metadata deleted: cache_key=bucket/object.txt, reason=all_ranges_evicted
[BATCH_EVICTION] Directory cleanup: removed /cache/ranges/bucket/a7/f3c (empty)
```

## Metrics

`cache.evictions` counts evicted entries, and `cache.cache_part_evictions` counts multipart
parts. Coordination counters live under `eviction_coordination`.
`cache.eviction_oracle_stale_verdicts_total` counts candidates a pass kept because its
cross-proxy activity set had gone stale while the journal walk rebuilding it was still running;
`cache.eviction_oracle_last_walk_ms` is that walk's latest duration. Fills that the free-space
floor's danger band declines near a full volume (see
[Free-space floor and the danger band](CONFIGURATION.md#free-space-floor-and-the-danger-band))
are counted in `cache.capacity_guard.declines_total`. See the
[cache metrics](METRICS_REFERENCE.md#cache).

A request spanning retained ranges with an evicted gap revalidates and refetches the missing
bytes, and is counted in `cache.incomplete_range_fallbacks` rather than as a hit.

## Re-grounding the size figures the triggers read

Eviction triggers compare a running counter against a bound. The counter is maintained
incrementally as objects are cached and removed, so it drifts: a lost update, a crash between
writing a file and recording it, or two proxies both crediting the same cache fill all leave it
slightly wrong. Two mechanisms re-ground it, with different costs and cadences:

| | Size fold | Metadata validation scan |
|---|---|---|
| Reads | directory listings plus one `stat` per range file | every metadata file's contents |
| Interval | `shard_fold_interval`, default `1h`, scaling with the volume | daily (see [The validation scan](SHARED_STORAGE.md#the-validation-scan)); `validation_max_duration` selects full or rolling |
| Produces | per-shard byte totals, range counts, object counts, a size histogram | the write-cache figure and metadata consistency |
| Sees | every range file on the volume, including ones no metadata names | only what metadata records |

The fold is the cheaper of the two per entry, which is why it can run far more often. It works
shard by shard and writes each shard's figures as soon as that shard is done, so a pass that is
interrupted or runs out of its budget leaves a mixed-age but well-defined total rather than an
incomplete one: shards it reached carry fresh figures, shards it did not carry their previous
ones, and each shard records its own scan age.

The interval belongs to the volume, not to a proxy. Before starting a pass, a proxy reads the
scan age of the stalest shard. If that is inside the interval, it skips the pass and schedules
the next one for when that shard ages past it. A proxy restart, a rolling restart of the fleet,
or a pass another proxy has just completed therefore does not trigger a new pass. A volume that
cannot show every shard was scanned inside the interval is scanned: one no pass has reached yet,
one with a shard that has never been scanned, and one whose shard records cannot be read.

The fold leaves out two things on purpose:

- **In-progress multipart uploads.** Those bytes are on the volume but eviction cannot reclaim
  them while the upload is live, so including them in a figure that drives eviction would give a
  trigger that fires and frees nothing.
- **Subtracting range files that no metadata names.** Its total covers every range file present,
  and the gap between that and the metadata-derived total is the measure of how many such files
  exist. Subtracting them would remove the only signal that they are accumulating.

See [Size fold cadence](CONFIGURATION.md#size-fold-cadence) for how the interval scales and what
it costs on the cache volume, and [Size fold figures](METRICS_REFERENCE.md#size-fold-figures) for
the three `/metrics` fields that say whether it is completing. A pass that is declined on every
tick leaves the figures older without reporting an error, and one of those fields is the only
signal for it.

## Write cache reclamation

The write cache holds objects uploaded through the proxy and not yet read. It is reclaimed
differently from the read cache, and the differences follow from one property: an object that
has never been read cannot change its own eviction rank. Its last-access time is its write time,
so there is no frequency or recency score to compute, and TinyLFU scoring reduces to "oldest
first" over a population where nothing has been read.

| | Read cache | Write cache |
|---|---|---|
| Trigger | `eviction_trigger_percent` of `max_cache_size` | `eviction_trigger_percent` of the write cache allocation and of the staged range limit |
| Target | `eviction_target_percent` of `max_cache_size` | `eviction_target_percent` of the allocation and of the limit |
| Order | TinyLFU score, lowest first | oldest staged first; objects past `put_ttl` and still unread go first |
| Candidate discovery | cache index | append-only record of staged objects |
| Runs on the request path | no | no |

### Two bounds, two triggers

The write cache has a byte bound, the allocation that `write_cache_percent` sets (see
[Capacity management](CONFIGURATION.md#capacity-management)), and a count bound, the staged range
limit (`write_cache.staged_range_budget` on `/metrics`). Either one being over its trigger starts
a pass, and both use the same `eviction_trigger_percent` and `eviction_target_percent`
hysteresis.

A pass starts from the consolidation cycle, both while the cycle works through journal keys and
once after it, so a write burst that fills the write cache during a long cycle is reclaimed
within one `cache.shared_storage.consolidation_interval` rather than at the end of the cycle.
The byte figure the in-cycle comparison reads is the recorded staging size plus every change the
fleet has recorded and no cycle has applied. It reads slightly high while a cycle's own
graduations are pending, so the effect is a pass that starts and finds little to reclaim, never a
pass that is missed.

The count trigger matters because different workloads reach the two bounds. A workload of small
uploads reaches the count limit with the byte allocation nearly empty, and once the limit is
reached new uploads are no longer cached. Reclaiming on count lets that recover on its own.
Because reclamation takes expired, unread objects first, the entries it removes are the
read-after-write bets that did not pay off.

Each count-driven pass writes
`metadata/_shard_state/write_cache_count_eviction_last_result.json`. It names how many staged
ranges the pass set out to reclaim, how many it reclaimed, and `shortfall_reason`, one of:

| `shortfall_reason` | Reading |
|---|---|
| `target_reached` | the pass met its target |
| `no_reclaimable_candidates` | nothing was reclaimable, because nothing had expired. Informational: uploads are outpacing `put_ttl`, and declining to cache a new upload is cheaper than evicting a live read-after-write bet |
| `reclaimed_and_refilled` | reclamation works and the count came straight back. Actionable: the hysteresis gap or the limit is too low for this workload |
| `candidates_exhausted` | the record set ran out before the target was met |
| `unreadable_candidate_metadata` | the whole shortfall is objects whose metadata could not be read, which are never deleted on a failed read |

An absent document while `write_cache.staged_range_budget_declines_total` is climbing is a
different signal: the count trigger is not running at all.

### How candidates are found

The proxy keeps a compact append-only record of the objects it has staged, under
`metadata/_write_ledger/`, held as one file per cache shard per proxy
(`{bucket}/{XX}/{instance}.ledger`). Reclamation reads those records instead of scanning the
cache, so its cost follows what is staged and not yet reclaimed rather than everything the cache
holds. That set is smaller, because an object that has been read graduates out of the write
cache and its record is dropped on a later maintenance pass. Splitting the records by shard also
confines a lost or unreadable file: only that shard's figure stops refreshing.

Every record is a hint. Before anything is deleted, the candidate is re-checked against the
object's authoritative metadata, and skipped if the object has since been read (graduated out
of the write cache), replaced by a later upload, or removed. An object whose metadata cannot be
read is also skipped, rather than deleted on the strength of a failed read.

One pass examines a bounded number of records, so a long record set is worked through over
several passes. A pass removes only the records for objects it reclaimed; the rest are left for
the next pass, including any another proxy appended while it ran. Removing them reads every
record file under that file's own lock and writes back only the files a record is being dropped
from, so the write cost follows what was reclaimed while the read cost follows the size of the
record set. A pass that reclaims nothing therefore still reads every record file and writes
none.

Because a record is only a hint, losing one does not affect correctness: the object stays cached
and counted in the cache size, and it is invisible to write cache reclamation until the
ledger-repair rotation or a full validation scan re-adds it. Losing the record for one range of
an object whose other records survive has no effect unless it was the object's most recently
written range. Then the whole object reads as replaced, and both reclamation and the write-cache
figure computed from these records leave it out until the ledger-repair rotation re-adds the
missing record. A full validation scan does not re-add it, because the object still has records.
The rotation sweeps every shard at least once a day while
`write_cache.ledger_repair_rotation_complete` reads `true`.

When writing a record fails, the proxy does not count that range toward the write-cache size,
so the figure never includes bytes reclamation cannot find. A completed multipart upload writes
its records before it publishes its `.meta`, and is not cached at all if a record cannot be
written. A replaced object is removed from the cache as soon as it is replaced. A record is
discarded only once it is older than 60 seconds, whether it no longer matches a cached object or
its object was just reclaimed; this 60-second rule applies to records, not to cached data. A
pass that reads the cache between a completion's records and its publish therefore reclaims the
replaced object at once but keeps the new object's records. A re-added entry is timestamped when
it is re-added, so it is treated as recently written and reclaimed later than its true age
suggests.

The same mechanism covers a cache holding staged objects with no records at all, which is the
state every record set starts in: the first full validation scan populates the records from the
cache, with no operator action. For how the write-cache figure behaves while a fleet is being
upgraded, see [During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade).

`write_cache.ledger_entries` on `/metrics` reports the record's length. Read it alongside
`write_cache.staged_entries`: the two count the same population by different routes, so a
persistent gap between them means entries are being lost or reclamation is not keeping up.

### Coordination across proxies

Write cache reclamation takes the same eviction lock as read cache eviction, so only one proxy
reclaims at a time, and it re-reads the staged total after taking the lock in case another
proxy has already brought the write cache under its trigger.
`eviction_coordination.staging_evictions_skipped_lock_held` counts passes skipped because the
lock was held. The decision to reclaim is taken from shared state because the deletes land on
the cache volume; a proxy acting on a private view of usage could delete data another proxy had
just written. See [Distributed eviction](SHARED_STORAGE.md#distributed-eviction).

Reclaiming one object reads its `.meta`, deletes the range files it names, and removes the
`.meta`, all under that object's metadata lock. An upload replacing the same object takes the
same lock to remove the old object and publish the new one, so reclamation never deletes a new
object's files by the old object's names. Reclamation decides from a `.meta` read taken before
it holds the lock, so under the lock it compares the object's ETag and staged-write time with
what it decided on. If an upload published a different object in between, reclamation leaves it
in place, reports it as `replaced_since_assessment` in the pass's `skips`, and considers the new
object on a later pass. If the lock is not granted within
`cache.shared_storage.lock_acquire_wait_budget`, the object is skipped and considered again on
the next pass.

The staged range count comes from per-shard figures that move only when the staged fold next
re-reads a shard (see [Size accounting](CACHE_INTERNALS.md#size-accounting)), so the count a
pass reads lags the deletes. A count-driven pass therefore records what it has reclaimed since
its proxy's last staged fold in `metadata/_shard_state/eviction_reclaims/{instance}.json`,
rewritten atomically after each pass. Each proxy subtracts its own reclaims and every other
proxy's from the count before deciding whether to reclaim, so ranges one proxy has freed are not
asked for again by another. A proxy removes its document at its next staged fold. A document
older than 24 hours 15 minutes (the fold's 24-hour freshness bound plus its 15-minute shard
lease), left by a proxy that stopped first, is no longer subtracted, and it is removed once it is
twice that age. Admission's staged range budget does not subtract reclaims; it reads the
checkpoint figure.

## Recovery from an inflated write-cache figure

This section applies where `write_cache.staged_authority_epoch` on `/metrics` reads `0`, the
event-maintained computation described below. Where it reads `1`, the figure is recomputed from
the cache's own `.meta` files and there is no drift to recover from; see
[Which computation is in force](#which-computation-is-in-force).

An inflated `cache.write_cache_size` (reported as `write_cache.resident_bytes`) can sit above the
[`write_cache_percent`](CONFIGURATION.md#capacity-management) allocation while little is staged.
PUT requests are still cached; the effect is that write cache reclamation runs when it does not
need to, removing staged objects early. Recovery removes no cached object and discards nothing;
only the figure is wrong.

### Automatic recovery

Recovery happens on its own, in two stages that work together:

1. **Graduation releases the bytes it credited.** Every object read for the first time releases
   its bytes from the write cache allocation as it graduates (see
   [How write caching works](CONFIGURATION.md#how-write-caching-works)). This alone corrects the
   figure for any object a client reads.
2. **A full validation scan re-grounds the figure from what is on disk.** The daily
   [validation scan](SHARED_STORAGE.md#the-validation-scan) recomputes `cache.write_cache_size`
   from the cache's own `.meta` files, the same way it re-grounds the total cache size. Once a
   full scan completes, the figure drops to its true value whatever the read traffic. A proxy
   that starts more than 23 hours after the last validation asks for the scan to run promptly
   under the fleet's validation lock, rather than counting the cache itself.

   **In rolling mode the correction is partial.** When a full scan takes longer than
   `validation_max_duration`, the scan switches to rolling mode and each pass visits only some
   of the 256 top-level directories. A rolling pass corrects the figure by the discrepancy it
   finds in the directories it visited, scaled against the tracked figure, so an error
   concentrated in directories not yet visited stays until the rotation reaches them, which can
   take several days on a large cache.

An operator who can wait for the next full validation scan needs to do nothing. On a cache large
enough to validate in rolling mode, use the manual recovery below for an immediate correction.

### Which computation is in force

The two stages above repair a figure that is maintained by events: credited when an object is
staged, debited when it graduates. At epoch 0 both stages are needed because a debit can be
lost: the write is best-effort on the cache volume, and a lost debit stands until a full scan
runs.

At epoch 1, `cache.write_cache_size` is recomputed from the cache's own `.meta` files on a
rolling per-shard schedule. There is no debit to lose, so there is no drift to converge, and the
daily scan is not the figure's only recovery.

Two consequences for an operator:

- **`write_cache.staged_authority_epoch` on `/metrics` says which computation is in force.**
  `0` is the event-maintained figure described above; `1` is the recomputed one. The transition
  is automatic; [Rolling back from 3.0.0](UPGRADING.md#rolling-back-from-300) says when it
  happens. While it has not happened, `write_cache.staged_authority_activation_blocker`
  names what is holding it; see the [metrics reference](METRICS_REFERENCE.md).
- **Expect a one-time step in `cache.write_cache_size` at the transition**, to the recomputed
  figure, possibly by a large factor. The step can go up or down: its size and sign are the old
  counter's accumulated error. Nothing is evicted and nothing is lost; the recomputed figure is
  the correct one. On a volume only 3.0.0 proxies write, the figure between recomputations can only
  overstate, so the eviction trigger fires early at worst and never late.

**Rolling back to a release without the recomputed figure is not supported once the epoch reads
`1`.** To roll back after that point, move the fleet to a fresh cache volume as
[Rolling back from 3.0.0](UPGRADING.md#rolling-back-from-300) describes.

### Manual recovery, for an operator who cannot wait

Deleting `size_tracking/validation.json` and `size_tracking/schedule.json` on the cache volume
and restarting forces an immediate full validation scan instead of waiting for the next
scheduled one. Both files are needed: the scheduler checks the first for a previous scan, and
the second selects the mode and holds the rolling cursor. This runs the same mechanism the
automatic path uses, on demand, and it does not touch `ranges/` or `metadata/`.

The scan's duration grows roughly linearly with the number of cached objects.

This manual path has one requirement the automatic path does not: restart every proxy sharing
the cache volume. Correcting the shared `size_tracking/size_state.json` file does not reach a
running proxy's in-memory write-cache counter; a proxy picks up the corrected figure when it
restarts. A proxy left running against the old, inflated figure keeps triggering write cache
reclamation early after the others have recovered, until it is restarted too. A staggered
restart across the fleet is safe, and takes longer to take full effect than restarting every
proxy together.

## See also

- [Eviction configuration](CONFIGURATION.md#eviction-configuration): thresholds and algorithm
- [Capacity management](CONFIGURATION.md#capacity-management): `write_cache_percent` and what it bounds
- [Distributed eviction](SHARED_STORAGE.md#distributed-eviction): the fleet-wide eviction lock
- [Access tracking](CACHE_INTERNALS.md#access-tracking): the statistics eviction sorts on
- [Cache read paths](CACHE_READ_PATHS.md): what a partly evicted object does to a range read
