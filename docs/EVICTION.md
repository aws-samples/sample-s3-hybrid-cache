# Cache Eviction

How the proxy reclaims disk space. Eviction operates on **individual ranges**, not whole
objects, so a hot range survives even when the rest of its object is cold.

Thresholds and the algorithm choice are configured in
[CONFIGURATION.md — Eviction Configuration](CONFIGURATION.md#eviction-configuration).
Multi-instance coordination of eviction is in
[SHARED_STORAGE.md — Distributed eviction](SHARED_STORAGE.md#distributed-eviction).

## Triggers

Eviction runs from four places:

1. **Before caching new data** — storing a new range checks whether it would push usage
   past the trigger threshold
2. **At startup** — if the cache is already over `max_cache_size` during initialization
3. **At the end of a consolidation cycle** — the journal consolidator checks the freshly
   summed size and triggers if over capacity
4. **Background capacity checks**

The consolidator path **only triggers when the size changed**, which is why a read-only
workload legitimately never evicts even while sitting over capacity. That is expected
behaviour, not a defect — see
[SHARED_STORAGE.md — Failure modes](SHARED_STORAGE.md#failure-modes).

## Thresholds

Eviction starts at `cache.eviction_trigger_percent` of `max_cache_size` (default 95) and
runs until usage falls to `cache.eviction_target_percent` (default 80). Both are
configurable; see
[CONFIGURATION.md — Eviction Configuration](CONFIGURATION.md#eviction-configuration) for
the validated ranges and the deprecated `eviction_buffer_percent`.

The disk cache uses **range-level eviction** where each cached range is an independent eviction candidate. This provides fine-grained cache management that retains hot ranges while evicting cold ones, even within the same object.

## Eviction Algorithms

**LRU (Least Recently Used, default)**:
- Evicts ranges based on last access time
- Oldest accessed ranges are evicted first
- Simple and predictable behavior

**TinyLFU (decayed-frequency)**:
- Both cache tiers — RAM (`shard_find_tinylfu_victim`) and disk (`RangeSpec::tinylfu_score` /
  `sort_range_candidates_for_tinylfu`) — score victims with the same shared helper,
  `decayed_frequency(access_count, idle_secs) = access_count >> min(idle_secs / 3600, 63)`.
- Access count halves once per hour (the half-life) of idle time. The victim is the range
  minimizing `(decayed_frequency, last_accessed)` — lowest decayed frequency first, oldest
  `last_accessed` as tiebreak.
- A frequently-accessed-but-idle range is shielded from a single large one-hit read or scan:
  its decayed frequency stays higher than a range accessed only once, so the one-hit range is
  evicted first. (Prior to this fix the score divided frequency by recency, which inverted
  this — a genuinely hot-but-idle range could be evicted before a fresh one-hit read.)
- Decay only reduces a range's score toward 0 as it sits idle; it never resets a TTL or
  forces expiry on its own. TTL expiry/revalidation remains the ceiling for how long stale
  data can persist when there is no capacity pressure — decay only matters once eviction
  needs to reclaim space.

## Range-Level Granularity

All eviction operates at the range level:
- Every cached range is an independent eviction candidate
- Ranges are sorted by the selected eviction algorithm
- Hot ranges are retained even if other ranges of the same object are cold
- Metadata file is deleted only when all ranges are evicted

A request that spans retained ranges with an evicted gap validates the object and
fetches the missing bytes from S3. The response stays byte-exact for the requested
range; it is a partial cache miss rather than a cache hit.

## Eviction Flow

```
1. Collect    → Scan all .meta files, create candidate for each range
2. Sort       → Order by eviction algorithm (LRU: oldest first, TinyLFU: lowest score first)
3. Group      → Batch ranges by object for efficient metadata updates
4. Evict      → Delete .bin files, update metadata atomically (one write per object)
5. Cleanup    → Delete .meta when empty, remove empty directories
```

## Per-Range Access Tracking

Each range maintains independent access statistics in the metadata file:

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

- `last_accessed`: Timestamp of last access (used by LRU algorithm)
- `access_count`: Number of accesses (used by TinyLFU algorithm)

## Metadata Cleanup

The `.meta` file is deleted only when:
1. All ranges have been evicted (ranges list is empty)
2. Associated lock files are also deleted
3. Empty parent directories are cleaned up recursively

This ensures partial cache entries remain usable for subsequent requests.

## Benefits

| Aspect | Range-Level Eviction |
|--------|---------------------|
| Eviction granularity | Always range-level |
| Hot range retention | Hot ranges always retained |
| Cache efficiency | Optimal utilization |
| Metadata updates | Batched per-object |
| Directory cleanup | Automatic recursive cleanup |

## Example Scenario

**Object A** (100MB, 10 ranges of 10MB each):
- Range 0-10MB: accessed 5 min ago, count=50 → **Keep**
- Range 10-20MB: accessed 2 hours ago, count=2 → **Evict candidate**
- Range 20-30MB: accessed 1 min ago, count=100 → **Keep**
- ...

**Result**: Only cold ranges evicted, hot ranges retained. Metadata updated once with all evicted ranges removed.

### Admission Window Protection

Newly cached ranges are protected from immediate eviction for 60 seconds. This prevents cache thrashing during large file downloads where new ranges would otherwise be evicted immediately due to having zero access history in TinyLFU.

**How It Works:**

1. When collecting eviction candidates, each range's `last_accessed` timestamp is checked
2. Ranges cached within the last 60 seconds are skipped as eviction candidates
3. This gives new ranges time to accumulate access statistics before competing for cache space

**Example Scenario:**
```
Large file download in progress:
- Range 0-8MB: cached 5 seconds ago → Protected (within admission window)
- Range 8-16MB: cached 2 seconds ago → Protected (within admission window)
- Range 16-24MB: cached 3 minutes ago, access_count=1 → Eviction candidate
- Range 24-32MB: cached 1 hour ago, access_count=50 → Eviction candidate (but high score)
```

**Benefits:**
- Prevents evicting ranges that were just downloaded
- Allows new ranges to build access history before eviction decisions
- Reduces cache thrashing during streaming downloads

### Critical Capacity Bypass

When cache usage exceeds 110% of the configured limit, the admission window protection is bypassed to aggressively reclaim space.

**Trigger Condition:**
```
current_cache_size > max_cache_size * 1.10
```

**Behavior:**
- Normal eviction (≤110%): Respects 60-second admission window
- Critical eviction (>110%): Bypasses admission window, all ranges are eviction candidates

**Why This Exists:**

In extreme scenarios (rapid writes, burst traffic), the cache can exceed its limit faster than normal eviction can reclaim space. The critical bypass ensures:
- Cache size is brought under control quickly
- Disk space exhaustion is prevented
- System stability is maintained even under heavy load

**Logging:**
```
INFO [DISK_CACHE_EVICTION] Critical capacity exceeded (11.5 GiB / 10 GiB = 115%), bypassing admission window
INFO [DISK_CACHE_EVICTION] Eviction completed: ranges_evicted=150, freed=2.0 GiB, new_usage=9.5 GiB / 10 GiB (95.0%)
```

## Logging

Eviction logs a start line and a completion line at INFO:

```
[DISK_CACHE_EVICTION] Starting eviction: usage=142.5 MiB / 150.0 MiB (95.0%), target=120.0 MiB (80%), to_free=22.5 MiB
[DISK_CACHE_EVICTION] Eviction completed: keys_evicted=3, ranges_evicted=47, freed=24.0 MiB, new_usage=118.5 MiB / 150.0 MiB (79.0%)
```

Per-range and per-object deletion detail is logged at DEBUG.

**Log fields**:
- **keys_evicted**: objects that had at least one range evicted
- **ranges_evicted**: individual ranges evicted
- **freed**: bytes freed, human-readable
- **new_usage**: cache size after eviction, against the maximum, with a percentage

Metadata deletion and directory cleanup log separately:

```
[BATCH_EVICTION] Metadata deleted: cache_key=bucket/object.txt, reason=all_ranges_evicted
[BATCH_EVICTION] Directory cleanup: removed /cache/ranges/bucket/a7/f3c (empty)
```

## Metrics

`cache.evictions` counts evicted entries and `cache.cache_part_evictions` covers multipart
parts. Coordination counters live under `eviction_coordination`. See
[METRICS_REFERENCE.md](METRICS_REFERENCE.md#cache).

Note the interaction with range reads: a request spanning retained ranges with an evicted
gap revalidates and refetches the missing bytes, and is counted in
`cache.incomplete_range_fallbacks` rather than as a hit.

## Write cache (staging tier) reclamation

The write cache holds objects that have been uploaded through the proxy but not yet read.
It is reclaimed differently from the read cache, and the differences follow from one
property: **an object that has never been read cannot change its own eviction rank.** Its
last-access time is its write time, so there is no frequency or recency score to compute —
LRU and TinyLFU both degenerate to "oldest first" over a population where nothing has been
accessed.

| | Read cache | Write cache (staging) |
|---|---|---|
| Trigger | `eviction_trigger_percent` of `max_cache_size` | `eviction_trigger_percent` of the write cache allocation |
| Target | `eviction_target_percent` of `max_cache_size` | `eviction_target_percent` of the allocation |
| Order | LRU or TinyLFU per `eviction_algorithm` | oldest-staged-first; objects past `put_ttl` and still unread go first |
| Candidate discovery | cache index | append-only record of staged objects |
| Runs on the request path | no | no |

### How candidates are found

The proxy keeps a compact append-only record of the objects it has staged, one file per
instance under `metadata/_write_ledger/`. Reclamation reads that instead of scanning the
cache, so its cost is proportional to what it removes rather than to how much is cached.

Every record is a **hint**. Before anything is deleted the candidate is re-checked against
the object's authoritative metadata, and skipped if the object has since been read
(graduated out of the staging tier), been replaced by a later upload, or already gone. An
object whose metadata cannot be read is also skipped, rather than deleted on the strength
of a failed read.

One pass examines a bounded number of records, so a very long record set is worked through
over several passes rather than in one. A pass removes only the records for objects it
actually reclaimed; the rest are left for the next pass, including any appended by another
instance while it was running.

Because the record is only a hint, losing an entry is not a correctness problem: the object
stays cached and stays correctly accounted, it is simply invisible to staging reclamation
until the next full validation scan re-adds it. Note that a re-added entry is timestamped
when it is re-added, so it is treated as recently written and reclaimed later than its true
age would suggest. That same mechanism means **no migration step is needed when upgrading** —
a deployment with existing staged objects starts with an empty record and the first
validation scan populates it from the cache.

`write_cache.ledger_entries` on `/metrics` reports the record's length. Read alongside
`write_cache.staged_entries`: the two count the same population by different routes, so a
persistent gap between them indicates entries being lost or reclamation not keeping up.

### Coordination across instances

Staging reclamation takes the same global eviction lock as read-cache eviction, so only one
instance reclaims at a time, and it re-reads the staged total after acquiring the lock in
case another instance has already brought the tier under its trigger.
`eviction_coordination.staging_evictions_skipped_lock_held` counts passes skipped because
the lock was held. This matters more than it might appear: the decision to reclaim is taken
from shared state and the deletion lands on shared storage, so an instance acting on a
private view of usage could delete data another instance had just written. See
[SHARED_STORAGE.md](SHARED_STORAGE.md#distributed-eviction).

## Recovery from an inflated write-cache figure

Before 2.7.0, `cache.write_cache_size` (reported as `write_cache.resident_bytes`) could only
grow: it was credited when an object was written through the cache but never reduced when
that object was later read, overwritten, or removed. On a long-running deployment this
figure could climb past the configured [`write_cache_percent`](CONFIGURATION.md#capacity-management)
allocation even though almost nothing was genuinely staged, and once it did the proxy
refused new write-through PUTs — reporting a full write cache while little or nothing was
actually resident.

**This is not a cache-wipe scenario.** No cached object needs to be removed to recover, and
the fix does not discard anything. What is wrong is a number, not the data it was supposed
to describe.

### Automatic recovery

Recovery happens on its own, with no operator action, in two stages that layer together:

1. **Graduation now decrements the figure it should always have decremented.** Every object
   read for the first time after upgrading releases its bytes from the write-cache
   allocation as it graduates, exactly as [described above](CONFIGURATION.md#how-write-caching-works).
   This alone starts correcting the figure for any object a client reads.
2. **The first full validation scan re-grounds the figure from what is actually on disk.**
   The periodic validation scan (see
   [CONFIGURATION.md — Validation scan](CONFIGURATION.md#validation-scan)) now recomputes
   `cache.write_cache_size` from the cache's own `.meta` files rather than leaving it
   uncorrected, the same way it has always re-grounded the total cache size. Once this scan
   completes, the figure drops to its true value regardless of read traffic.

An operator who can wait for the next scheduled validation scan needs to do nothing at all.

### Manual recovery, for an operator who cannot wait

Deleting `size_tracking/validation.json` on the shared cache volume and restarting forces an
immediate full validation scan instead of waiting for the next scheduled one. This is the
same mechanism the automatic path uses, run on demand — it is not a special "repair" code
path, and it does not touch `ranges/` or `metadata/`.

Measured cost: **8.2 seconds for 1,780 cached objects.** Scale roughly linearly with object
count for a rough estimate on a larger cache.

One thing this manual path requires that the automatic path does not, established by running
it on a live fleet:

- **Every instance sharing the cache volume must be restarted, not just one.** Correcting
  the shared `size_tracking/size_state.json` file does not reach a running instance's
  in-memory write-cache counter — an instance restarts to pick up the corrected figure, it
  does not observe it live. A proxy left running against the old, inflated figure continues
  refusing new write-through PUTs after its siblings have recovered, until it is restarted
  too. As of 2.7.0 this is an availability gap on the stale instance only, not a data-loss
  risk: refusing a write-through PUT for lack of capacity no longer scans or deletes
  anything on the shared volume (see [CHANGELOG.md](../CHANGELOG.md) 2.7.0), so a
  staggered restart across the fleet is safe, just slower to fully take effect than
  restarting every instance together.

## See Also

- [CONFIGURATION.md — Eviction Configuration](CONFIGURATION.md#eviction-configuration) — thresholds and algorithm
- [CONFIGURATION.md — Capacity Management](CONFIGURATION.md#capacity-management) — `write_cache_percent` and what it bounds
- [SHARED_STORAGE.md](SHARED_STORAGE.md#distributed-eviction) — the distributed eviction lock
- [CACHE_INTERNALS.md](CACHE_INTERNALS.md) — access tracking, which supplies the statistics eviction sorts on
- [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md) — what a partially-evicted object does to a range read
