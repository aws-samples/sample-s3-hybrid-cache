# Shared Cache Storage

Running more than one proxy against a single cache volume, so any instance can serve what
any other instance cached. This doc covers the mount requirements, the coordination
mechanisms, and the failure modes.

**Read the mount requirements first.** Both are correctness requirements, neither produces
an error when absent, and getting either wrong yields a deployment that looks healthy while
silently corrupting cache state or throwing away most of its hit rate.

Field types, defaults, and validated ranges for every `cache.shared_storage` option are in
[CONFIGURATION.md — Multi-Instance Coordination](CONFIGURATION.md#multi-instance-coordination).

## Coordination is always on

There is no `enabled` flag. Journal-based metadata writes, distributed eviction locking,
and accumulator-based size tracking are active in every deployment, including
single-instance ones. A single-instance deployment simply never contends.

This matters for two reasons: a single-instance deployment pays a small coordination cost
it cannot switch off, and a deployment that grows from one instance to three needs no
configuration change to become correct — only the mount options below.

## Mount requirements

### 1. New files from other instances must be visible immediately

Mount with **`lookupcache=pos`**.

NFS clients cache directory-entry lookups by default, including negative ones. Without
this option an instance caches "file not found" and never sees a file its peer created.
The symptom is a 40%+ miss rate on repeat downloads: one instance caches the data, the
others cannot find it, and every instance fetches from S3 independently.

### 2. Advisory file locks must be enforced across hosts

Pin **`nfsvers=4.1`**, and never use `nolock`, `local_lock=flock`, or `local_lock=all`.

The proxy uses `flock(2)` for every cross-instance critical section: global eviction,
journal consolidation, cache-size accounting, multipart part publishing, and range commit.
On NFS the Linux client emulates `flock` as whole-file POSIX byte-range locks (see
`nfs(5)`), which works across hosts only when the mount has working lock support. NFSv4.x
carries locking in the protocol; on NFSv3 it depends on NLM (`rpc.statd`, `lockd`) being
reachable, and otherwise fails or falls back to host-local behaviour.

**Each of `nolock`, `local_lock=flock`, and `local_lock=all` makes locks host-local.**
Every instance then acquires the "same" lock simultaneously and believes it holds
exclusive access, producing concurrent eviction, racing consolidation, and interleaved
multipart publishes. Nothing logs an error and the health endpoint stays green.

Do not add `noac` or a short `actimeo`. Coordination does not depend on attribute
freshness, so these only slow every operation on the volume. Rename-based lock
coordination was removed in 1.0.2 for exactly that reason: it depended on attribute
caching, produced 75+ lock errors per minute, and allowed simultaneous eviction. `flock`
replaced it and reads no attributes.

### Mount lines

Generic NFS server:

```
nfs-server.example.com:/export/cache /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,_netdev 0 0
```

FSx for OpenZFS:

```
fs-0123456789abcdef0.fsx.us-east-1.amazonaws.com:/fsx /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,lookupcache=pos,nconnect=16,_netdev 0 0
```

EFS:

```
fs-0123456789abcdef0.efs.us-east-1.amazonaws.com:/ /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,noresvport,_netdev 0 0
```

**Neither AWS vendor's recommended option set includes `lookupcache=pos` or
`nfsvers=4.1`.** Carry both into whichever line you start from, or the mount fails
silently in the two ways above.

The two managed file systems differ:

| Option | FSx for OpenZFS | EFS |
|--------|-----------------|-----|
| `noresvport` | **Rejected** — FSx requires reserved source ports and fails the mount with `Operation not permitted` | **Recommended** by AWS so clients reconnect on a new source port after a network event |
| `nconnect=16` | **Recommended** by AWS, up to 16, to exceed single-flow limits (below) | **Neither supported nor needed** — the EFS client handles connection parallelism. Use Elastic throughput with `amazon-efs-utils` 2.0+ |
| `retrans=2` | Not in AWS's FSx recommendations; harmless, 2 is already the Linux default | **Recommended** by AWS |
| NFS version | Supports v3, v4.0, v4.1, v4.2. Pin `nfsvers=4.1` for locking | Only v4.0/v4.1, so locking is satisfied by construction |

| Option | Purpose |
|--------|---------|
| `lookupcache=pos` | **Required.** Caches positive lookups but not negative ones, so peers' new files are visible immediately |
| `nfsvers=4.1` | **Required for cross-host locking.** Pin explicitly rather than relying on negotiation |
| `hard` | Retry NFS requests indefinitely, for data integrity |
| `rsize`/`wsize=1048576` | Largest supported sizes; both AWS file systems recommend the maximum. FSx file systems provisioned at only 64 or 128 MB/s cap these lower (262144 and 524288) because of reduced file-server memory |
| `_netdev` | Wait for network before mounting |

**Why `nconnect` matters on FSx.** Without it a mount uses one TCP connection, and EC2
limits [single-flow traffic to 5 Gbps](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html)
— about 625 MB/s — outside a cluster placement group, which is unavailable for a managed
endpoint. So the mount, not the provisioned throughput tier, becomes the limit on
disk-cache reads at roughly 625 MB/s per proxy. RAM cache hits never touch the mount and
are unaffected.

### Verifying the mount

Check what landed, not what was requested. A silently dropped or overridden option
degrades coordination with no error.

```bash
mount | grep -E 'nfs|efs|fsx'  # Confirm lookupcache=pos and nfsvers=4.1 present, nolock/local_lock absent
```

Then confirm both properties end to end. For visibility, request an object through one
proxy and the same object pinned to a different proxy, and expect a cache hit. For
locking:

```bash
# On instance A — hold an exclusive lock for 30 seconds
flock -x /mnt/cache/.locktest -c 'sleep 30'

# On instance B, while A still holds it — must fail immediately
flock -n -x /mnt/cache/.locktest -c true && echo "BROKEN: locks are host-local" || echo "OK: lock is enforced across hosts"
```

If B acquires the lock, coordination is unsafe. Fix the mount before running more than one
instance against the volume.

## Journal-based metadata writes

Writing `.meta` files in place from several hosts would corrupt them. Instead each
instance appends metadata updates to its **own** journal file, and a background
consolidator folds journals into the sharded `.meta` files under a lock.

```
cache_dir/metadata/_journals/{instance_id}.journal
```

Per-instance journals mean the hot path never contends: an instance writes only its own
file. Contention is confined to the consolidator, which runs on
`consolidation_interval` (default 5s) or when a journal exceeds
`consolidation_size_threshold` (default 1 MiB), whichever comes first.

Consequences worth knowing:

- **A metadata update is not immediately visible to peers.** It becomes visible after the
  writing instance's next journal flush plus the next consolidation cycle. At defaults
  that is single-digit seconds. Cached *data* is visible as soon as the range file lands;
  it is the metadata index that lags.
- **A stalled consolidator accumulates journals.** Watch
  `consolidation.last_consolidation_timestamp` in `/metrics`; if it stops advancing,
  metadata updates are piling up unapplied.
- **Cache-hit TTL refreshes are buffered before they even reach the journal**, on
  `ram_cache_flush_interval` (default 10s), so a hit recorded on one instance affects
  another instance's eviction decisions only after both intervals.

Lock acquisition retries `lock_max_retries` times (default 5) with `lock_timeout` (default
60s, validated 10-300s) as the ceiling. `metadata_lock_timeout_ms` (default 30000)
governs how long a lock held by a **different host** is honoured before being treated as
stale — a local lock is judged by testing whether the owning PID still exists, which is
impossible across hosts, so wall-clock is the only available test there.

## Distributed eviction

Only one instance evicts at a time, coordinated through
`cache_dir/locks/global_eviction.lock`. Without this, several instances would
independently decide the cache is over capacity and evict concurrently, each unaware of
what the others freed, overshooting the target badly.

`eviction_lock_timeout` (default 60s, validated 30-3600s) is how long a held eviction lock
is honoured before being forcibly acquired. Set it to two or three times your typical
eviction duration. Too short and a second instance starts evicting while the first still
is; too long and a crashed instance blocks eviction until the timeout expires.

Relevant `/metrics` counters under `eviction_coordination`:
`evictions_skipped_lock_held` climbing is normal and healthy — it means the lock is doing
its job. `stale_locks_recovered` climbing means instances are dying mid-eviction or the
timeout is too short.

## Size tracking

Each instance keeps an in-memory `AtomicI64` accumulator of size changes at write and
eviction time, and flushes to a per-instance delta file. The consolidator sums the deltas
into `cache_dir/size_tracking/size_state.json`. Eviction triggers off that consolidated
figure.

### Concurrent-write over-counting

When several instances cache the **same** range at the same time — a cold-cache stampede
— each adds the range size to its own accumulator. On shared storage only one physical
file survives, but the tracked size grows by N×, once per instance that wrote it.

Measured: 100 concurrent clients across 3 proxy instances produced a tracked size about
3× actual disk usage. The over-count is bounded by the instance count.

This cannot be fixed at write time. An `exists()` check before writing does not help
during a stampede, because every instance checks before any has written.

**Over-counting is the safe direction.** It triggers eviction early rather than late, so
the disk does not fill. Eviction decrements the accumulator correctly for each deleted
range, so tracked size converges toward accuracy as eviction runs, and the daily
validation scan reconciles the remainder.

If premature eviction is a problem, the levers are raising `max_cache_size` headroom or
reducing how many instances can cold-miss the same key concurrently. The scan cadence is
**not** tunable (see below).

## The validation scan

A daily scan reads every cached `.meta` file and reconciles tracked size against the sizes
those files record. It does not stat the range (`.bin`) files, so a `.meta` that disagrees
with what is on disk is not detected here — that is [orphan recovery](#orphan-recovery)'s
job.

**Cadence is not configurable.** It fires once per day at midnight local time plus up to
one hour of random jitter, to avoid a thundering herd across instances.
`validation_frequency` is parsed, range-validated, and logged at startup, but no code path
reads it when scheduling the scan. Treat it as inert.

The scan holds a global validation lock for its whole duration, so only one instance scans
per cycle.

**It deletes `.meta` files it cannot parse**, logging `Removed invalid metadata file during
validation` for each one. An unparseable entry is unrecoverable, and leaving it would fail
every later scan the same way, so this is deliberate self-heal rather than data loss.

### Full and rolling modes

The scan self-tunes between two modes based on how long the previous one took:

**Full mode** scans all 256 L1 shard directories in parallel. Used on the first scan and
whenever the previous scan finished inside the time budget.

**Rolling mode** scans a subset of L1 directories per cycle, resuming from a persistent
cursor, reaching full coverage over several cycles. Activated automatically when a full
scan exceeds `validation_max_duration` (default 4h, validated 10m-23h).

Mode selection:

| Previous scan | Next mode |
|---|---|
| None (first ever) | Full |
| Full, within budget | Full |
| Full, exceeded budget | Rolling |
| Rolling, extrapolated full time > budget | Rolling |
| Rolling, extrapolated full time ≤ budget | Full |

`validation_max_duration` is the only knob, and it selects the **next** cycle's mode rather
than bounding the current one. Nothing aborts a scan in progress: a full scan runs to
completion, then warns that it overran (`Full validation scan exceeded time budget`), and
the following cycle switches to rolling. Crossing the threshold therefore costs one scan
that runs for as long as it needs to, however far past the budget that is.

### Rolling scan internals

- **Adaptive batch sizing.** Uses the previous cycle's rate (seconds per L1 directory) to
  estimate how many directories fit in the budget. The first cycle defaults to 64.
- **Proportional correction.** Adjusts tracked size by the drift observed in the scanned
  subset rather than replacing the total, so a partial scan cannot cause a large swing.
- **Cursor persistence.** Position, scan rate, and rotation count live in
  `cache_dir/size_tracking/validation.json`. A missing or corrupt file resets the cursor
  to 0.
- **Rotation tracking.** When the cursor wraps past all 256 directories, a rotation
  completion is logged with elapsed time.
- **Multi-bucket caches.** The cursor addresses 256 L1 index slots, and one slot covers
  that index in *every* bucket. On a cache holding several buckets a cycle's recorded rate
  therefore understates the work per slot, which makes batch sizing and the extrapolated
  full-scan time optimistic.

### The correction is unconditional

The size correction is always applied. There is no minimum drift below which reconciliation
is skipped.

`validation_threshold_warn` (default 5.0) and `validation_threshold_error` (default 20.0)
are drift percentages that select a **log level only** — `warn!` above the first,
`error!` above the second — and they apply to **rolling scans only**. A full scan logs the
drift it corrected unconditionally and consults neither field.

Read `cache_size.last_validation_drift` in `/metrics` for the signed byte drift the last
full scan found; it reports `null` after a rolling cycle. Persistent large drift on a
multi-instance deployment is the expected symptom of the over-counting above, not a defect.

### Monitoring

The proxy logs the selected mode, the reason, the time budget, directories scanned,
objects validated, scan rate, and cursor position at INFO:

```bash
journalctl -u s3-proxy | grep -i validation
```

In rolling mode the log is the only source of scan progress, since the `/metrics` fields
above report `null`.

## Orphan recovery

A background sweep finds range (`.bin`) files with no referencing metadata — left by
crashed writers or consolidation lag — and either reconciles them into metadata or removes
them. It scans one shard per cycle to spread I/O.

`orphan_recovery_enabled` (default `true`), `orphan_recovery_interval` (default 300s,
range 60-3600s), `orphan_scan_timeout` (default 30s, range 5-300s), and
`orphan_max_per_cycle` (default 100) bound it.

**This runs automatically; operators do not schedule cleanup.** See
[ERROR_HANDLING.md](ERROR_HANDLING.md) for the failure classes it handles.

## Failure modes

| Symptom | Likely cause |
|---|---|
| High miss rate on repeat downloads, each instance fetching independently | `lookupcache=pos` missing |
| Concurrent eviction, corrupt `.meta`, interleaved multipart publishes, nothing in the logs | Locks are host-local: `nolock`, `local_lock=*`, or NFSv3 without working NLM |
| `consolidation.last_consolidation_timestamp` stops advancing | Consolidator stalled; metadata updates accumulating in journals |
| `atomic_metadata.lock_timeouts_total` climbing | Lock contention; consider a longer `lock_timeout` or a longer `consolidation_interval` |
| `eviction_coordination.stale_locks_recovered` climbing | Instances dying mid-eviction, or `eviction_lock_timeout` too short |
| Tracked size several × actual disk usage | Cold-cache stampede over-counting. Expected, bounded by instance count, corrected by the daily scan |
| Disk-cache reads capped near 625 MB/s per proxy on FSx | `nconnect` missing; single TCP flow hitting the EC2 5 Gbps single-flow limit |
| `cache_rules.on_fallback: true` | A `cache_rules.json` edit failed to parse; the proxy kept the last good ruleset |

## See also

- [CONFIGURATION.md — Multi-Instance Coordination](CONFIGURATION.md#multi-instance-coordination) — field reference for every `cache.shared_storage` option
- [CACHING.md](CACHING.md) — cache layout, eviction policy, and what gets cached
- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) — the `atomic_metadata`, `eviction_coordination`, `consolidation`, and `cache_size` sections
- [AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md) — FSx for OpenZFS sizing and the reference architecture
- [ERROR_HANDLING.md](ERROR_HANDLING.md) — corruption, orphaned files, and recovery
- [ARCHITECTURE.md — Shared Cache Access Model](ARCHITECTURE.md#security-considerations) — the trust model for a shared volume
