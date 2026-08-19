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

## See Also

- [CONFIGURATION.md — Eviction Configuration](CONFIGURATION.md#eviction-configuration) — thresholds and algorithm
- [SHARED_STORAGE.md](SHARED_STORAGE.md#distributed-eviction) — the distributed eviction lock
- [CACHE_INTERNALS.md](CACHE_INTERNALS.md) — access tracking, which supplies the statistics eviction sorts on
- [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md) — what a partially-evicted object does to a range read
