# Running a fleet on one cache volume

This page is for operators running a fleet: several Hybrid Cache for Amazon S3 proxies that share one cache volume, so any
proxy can serve what another proxy cached. It covers the mount requirements, how the proxies
coordinate on the volume, and the failure modes.

**Read the mount requirements first.** Neither one produces an error when it is missing, and a
fleet that gets either wrong looks healthy while it corrupts cache state or loses most of its
hit rate.

Field types, defaults and valid ranges for every `cache.shared_storage` field are in the
[configuration reference](CONFIGURATION.md#multi-instance-coordination).

## Coordination is always on

There is no `enabled` field. Journal-based metadata writes, the eviction lock and
accumulator-based size tracking run in every deployment, including a single proxy. A single
proxy never contends, but it still makes the journal writes and lock calls, which cannot be
switched off. In return, growing from one proxy to three needs no configuration change, only
the mount options below.

## Each proxy needs its own hostname

Each proxy writes its coordination state on the cache volume under an identity of the form
`hostname:pid`. To decide whether another process on its own host has stopped, a proxy tests
that process id on its own machine. Two proxies that share a hostname break that test: one
proxy's live process id can read as stopped on the other's machine, and two proxies with the
same hostname and the same process id write the same files.

Give each proxy a distinct hostname. Docker's default per-container hostname does this. A fixed
`--hostname` repeated across replicas does not, and neither does `network_mode: host` on
machines cloned from one image with one hostname. See
[Each container needs its own hostname](DOCKER.md#each-container-needs-its-own-hostname).

Each proxy checks for a collision before it takes any decision that rests on the process-id
test. When it finds one, it logs one ERROR naming both identities and raises
`cache.hostname_collisions_total` on `/metrics` and OTLP, once per colliding proxy. Any value
above zero is a deployment error. Requests are still served. The other effects depend on what
the two proxies share:

- **The same hostname, different process ids.** Cleanup of files left by a stopped proxy slows
  down: the shortcuts that rest on the process-id test stay off for the life of the process, so
  those files are removed on their age alone. No cached data or capacity figure is wrong.
- **The same hostname and the same process id**, for example two containers that both run as
  process 1. The two proxies write the same per-proxy files, including the one that issues the
  sequence numbers that order the write-cache accounting, so the proxy stops caching writes for
  the life of the process. `PUT` requests and multipart parts are forwarded to S3 and served
  normally without caching, each counted as `hostname_not_unique` in
  [`signed_put.skipped_puts_total`](METRICS_REFERENCE.md#skipped_puts_total). Reads and the read
  cache are unaffected. Ranges the proxy staged before it detected the collision can keep
  duplicate sequence numbers, which understates the write cache's figure for staged objects. A
  peer with a distinct identity can re-index the ranges this proxy leaves unindexed. A pass
  refused on a shared identity keeps its claim on the shard, though, so those entries can stay
  unindexed until the hostnames are fixed.

Fix the hostnames and restart.

## Mount requirements

### 1. New files from other proxies must be visible immediately

Mount with `lookupcache=pos`.

NFS clients cache directory-entry lookups by default, including negative ones. Without this
option a proxy caches "file not found" and does not see a file a peer created until the parent
directory's cached attributes expire. The symptom is a miss rate above 40% on repeat downloads:
one proxy caches the data, the others cannot find it, and each fetches from S3 on its own.
When the cache directory is on NFS without `lookupcache=pos` or `lookupcache=none`, the proxy logs one warning at startup that names the mount, its file system type and its `vers` and `lookupcache` options (never the server or client address), and still starts.

### 2. Advisory file locks must be enforced across hosts

**The requirement is `flock(2)` enforced across hosts.** `nfsvers=4.1` is the recommended way to
get it, and NFSv3 with working NLM also works; see
[Choosing the NFS version](#choosing-the-nfs-version). Never use `nolock`, `local_lock=flock` or
`local_lock=all`.

The proxy uses `flock(2)` for every critical section that proxies share: eviction, journal
consolidation, cache-size accounting, multipart part publishing and range commit. On NFS the
Linux client emulates `flock` as whole-file POSIX byte-range locks (see `nfs(5)`), which works
across hosts only when the mount has working lock support.

`flock` excludes per open file, so two requests in one proxy exclude each other just as two
proxies do. Each proxy therefore queues its own updates to the cache-size accounting file in
memory, and at most one of its threads waits on that file's lock at a time. No request takes
that lock: byte and object-count changes accumulate in memory, the consolidation cycle applies
them, and `/health` and `/metrics` report the consolidator's last reading of the file. A peer
holding the lock delays the accounting update without tying up the threads that serve requests,
`/health` and `/metrics`. The holder reads and rewrites the accounting file on the thread that
took the lock, so it never waits for another I/O thread while other proxies wait for it.

The other locks follow the same rule. Reserving and releasing danger-band bytes queue on the
reservation lock, so at most one thread per proxy waits on it. A multipart completion polls its
upload's lock instead of blocking a request thread on it. A request waiting for a metadata slot
lock polls it until `lock_acquire_wait_budget` expires, so it holds no I/O thread between
attempts.

**Each of `nolock`, `local_lock=flock` and `local_lock=all` makes locks host-local.** Every proxy
then takes the "same" lock at once and believes it holds it alone, which produces concurrent
eviction, racing consolidation and interleaved multipart publishes. Nothing logs an error and
`/health` keeps reporting healthy.

`local_lock=posix` is safe. Per `nfs(5)`, under `posix` the client still sends `flock` locks to
the server through the NLM sideband protocol, so the proxy's locking is unaffected. Only the
`flock` and `all` values break it.

Do not add `noac` or a short `actimeo`. Coordination does not depend on attribute freshness
(`flock` reads no attributes), so these options only add attribute requests to operations on
the volume.

Do not use a vendor option that disables close-to-open consistency, such as VAST's `nocto`
(`nfs(5)` documents the generic form). It replaces close-to-open cache revalidation with a
heuristic, so one proxy's write is not reliably visible to another. Vendors recommend it for
single-host workloads such as scratch space, and a fleet sharing a cache volume is the opposite
case.

### Choosing the NFS version

Both NFSv4.1 and NFSv3 can satisfy the requirement above. The trade-off is operational.

| | NFSv4.1 | NFSv3 |
|---|---|---|
| How locking works | Carried in the protocol. No sideband daemon | NLM sideband protocol, requiring `rpc.statd` and `lockd` on the client and NLM on the server |
| Failure mode if locking is unavailable | Does not arise: `nolock` and `local_lock` are NFSv3-only options and are not accepted here | **Loud, at mount time.** `mount.nfs` refuses with a message naming `rpc.statd` and telling you to start it or pass `-o nolock` |
| Default lock scope | Remote | **Remote.** Per `nfs(5)`, when `local_lock` is unspecified "the client assumes that the locks are not local" |
| Lock recovery after a client or server restart | A protocol property | Depends on NSM/`statd` notification. A v3-specific fragility, and worth weighing on a fleet whose instances are replaced |
| Firewall surface | One port, 2049 | portmapper, mountd and statd on separate and partly ephemeral ports |

**Prefer `nfsvers=4.1`** when the mount crosses a security boundary you do not control, or when
you want lock recovery to be a property of the protocol rather than of a daemon. Pin it
explicitly rather than relying on negotiation.

**NFSv3 is a legitimate choice** where NLM is reachable and verified. The common reason to choose
it is that some vendors' highest-throughput NFS features are NFSv3-only. VAST, for instance,
restricts `localports` (multiple client NICs), `spread_reads` / `spread_writes`, and `mdconnect`
(dedicated metadata channels) to NFSv3, while offering `remoteports` multipath and `nconnect` on
both. `mdconnect` is relevant because the proxy is metadata-heavy, and its background passes
compete with miss-fill writes for the same volume throughput (see
[Sizing the network](AWS_DEPLOYMENT.md#sizing-the-network) and the `tee_backpressure_*` counters
in the [metrics reference](METRICS_REFERENCE.md)). Check your vendor's matrix rather than
assuming this shape.

Choosing NFSv3 carries three obligations:

1. Confirm `rpc.statd` and `lockd` are running and that NLM reaches the server. A mount that
   succeeds without `nolock` has already shown this.
2. Never work around a statd problem with `-o nolock`. That turns a loud failure into the silent
   corruption described above.
3. Run the [two-host verification](#verifying-the-mount) either way.

### Mount lines

Generic NFS server:

```text
nfs-server.example.com:/export/cache /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,_netdev 0 0
```

FSx for OpenZFS:

```text
fs-0123456789abcdef0.fsx.us-east-1.amazonaws.com:/fsx /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,lookupcache=pos,nconnect=16,_netdev 0 0
```

EFS:

```text
fs-0123456789abcdef0.efs.us-east-1.amazonaws.com:/ /mnt/cache nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,noresvport,_netdev 0 0
```

AWS's recommended mount options for EFS and FSx do not include `lookupcache=pos`. Add it, and pin
`nfsvers=4.1`, whichever line you start from. Without `lookupcache=pos` the proxy logs one warning
at startup and still starts; without working cross-host locking it reports nothing, so that is the
failure that stays silent.

FSx for OpenZFS and EFS differ on these options. For FSx for NetApp ONTAP, the [lock checks](#cross-host-lock-verification-status) row gives the options its mounts used.

| Option | FSx for OpenZFS | EFS |
|--------|-----------------|-----|
| `noresvport` | **Rejected**: FSx requires reserved source ports and fails the mount with `Operation not permitted` | **Recommended** by AWS so clients reconnect on a new source port after a network event |
| `nconnect=16` | **Recommended** by AWS, up to 16, to exceed single-flow limits (below) | **Neither supported nor needed**: the EFS client handles connection parallelism. Use Elastic throughput with `amazon-efs-utils` 2.0+ |
| `retrans=2` | Not in AWS's FSx recommendations; harmless, 2 is already the Linux default | **Recommended** by AWS |
| NFS version | Supports v3, v4.0, v4.1, v4.2. Pin `nfsvers=4.1` for locking | Only v4.0/v4.1, so locking is satisfied by construction |

| Option | Purpose |
|--------|---------|
| `lookupcache=pos` | **Required.** Caches positive lookups but not negative ones, so peers' new files are visible immediately. The proxy logs a warning at startup when it is missing |
| `nfsvers=4.1` | **Recommended** for cross-host locking; pin it explicitly rather than relying on negotiation. NFSv3 with working NLM also satisfies the requirement; see [Choosing the NFS version](#choosing-the-nfs-version) |
| `hard` | Retry NFS requests indefinitely, for data integrity |
| `rsize`/`wsize=1048576` | Largest supported sizes; FSx for OpenZFS and EFS recommend the maximum. FSx file systems provisioned at only 64 or 128 MB/s cap these lower (262144 and 524288) because of reduced file-server memory, and FSx for NetApp ONTAP caps them at 65536 whatever the mount requests |
| `_netdev` | Wait for network before mounting |

**Why `nconnect` matters on FSx.** Without it a mount uses one TCP connection, and EC2 limits
[single-flow traffic to 5 Gbps](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html)
(about 625 MB/s) outside a cluster placement group, which is unavailable for a managed endpoint.
The mount then limits disk-cache reads to roughly 625 MB/s per proxy, whatever throughput tier
the file system has. RAM cache hits never touch the mount and are unaffected.

Encryption at rest comes from the cache volume: enable EFS or FSx encryption at rest, EBS volume
encryption, or block-device encryption such as dm-crypt. See
[Data confidentiality](ARCHITECTURE.md#data-confidentiality).

### Cross-host lock verification status

The table separates two claims: whether the storage documents cross-host lock enforcement, and
whether this proxy has been run against it. A vendor's documentation answers the first and says
nothing about the second.

Results are recorded per storage type and per NFS version. NFSv4.1 carries locks in the protocol
and NFSv3 uses the NLM sideband, so a result on one version is not a result on the other. Mount
options are part of each result: `local_lock=none` is what sends a lock to the server, so a
mount that resolved it to another value would fail through the mount's fault rather than the
storage's.

Where a row records enforcement, the storage passed the two-host `flock` recipe in
[Verifying the mount](#verifying-the-mount) and three further checks the proxy depends on:

- A peer is refused with `EAGAIN` while a lock is held, and succeeds on the same file once the
  lock is released.
- The lock survives the holding process's own open, read and close of the lock file, because
  the eviction fence reads a lock file it holds.
- A peer can take a different lock file while the first is held, so exclusion is per file
  rather than per volume.

| Storage | Cross-host lock enforcement |
|---|---|
| Generic NFSv4.1 | Not verified, and "generic" is not a testable target. Run the recipe against your own server before relying on it. |
| FSx for OpenZFS | **Enforces cross-host locks on NFSv4.1 and, separately, on NFSv3.** Every check behaved the same on both versions. The NFSv4.1 mount used the FSx line above; its relevant options as mounted were `vers=4.1,rsize=1048576,wsize=1048576,nconnect=16,lookupcache=pos,local_lock=none`. The NFSv3 mount was identical but for `nfsvers=3`, and with `local_lock` left unnamed it resolved to `local_lock=none`. Re-run the recipe after an upgrade rather than assuming the result carries over. |
| FSx for NetApp ONTAP | **Enforces cross-host locks on NFSv4.1 and, separately, on NFSv3**, with the same result on every check. ONTAP is a supported cache volume; the mount-option rows above were measured on FSx for OpenZFS and EFS. The mounts used `vers=4.1` and `vers=3` with `nconnect=16,lookupcache=pos,local_lock=none`. The server capped `rsize` and `wsize` at 65536 on both versions, whatever the mount requested. Re-run the recipe after an upgrade rather than assuming the result carries over. |
| Amazon EFS | **Enforces cross-host locks on NFSv4.1.** The per-file check was not run on EFS. The mount used `vers=4.1,lookupcache=pos,local_lock=none` with no `nconnect`. Re-run the recipe after an upgrade rather than assuming the result carries over. |
| VAST Data | **Documented by the vendor; this proxy has never been run against it.** VAST documents byte-range locking on both NFSv3 (NLMv4) and NFSv4.1, and states that lock information is persisted in the file system metadata tree so every node has immediate access to a file's current lock state, which is the cross-host property this requirement needs. VAST also notes that NLM locking is advisory, which suits this use: `flock(2)` is advisory too, and the proxy is the only writer and always takes the lock. No mount line is given, because none has been tested. Run the recipe. |

VAST's own recommended multipath mount options include `lookupcache=pos`, for the same reason
(one client must see the files another client creates), so requirement 1 fits the vendor's
guidance.

A passing recipe shows that the storage enforces locks across hosts. The recipe does not run the
proxy; that the proxy makes the same call is a property of its source. Its locking library,
`fs4`, calls `flock(2)` with a non-blocking exclusive lock on Unix.

### Verifying the mount

Check the options the mount reports, because a dropped or overridden option degrades
coordination. Only a missing `lookupcache=pos` is logged, once at startup; a missing lock
property is not reported.

```bash
mount | grep -E 'nfs|efs|fsx'  # Expect lookupcache=pos and vers=4.1; nolock, local_lock=flock and local_lock=all must be absent
```

Then confirm both properties end to end. For visibility, request an object through one proxy and
the same object pinned to a different proxy, and expect a cache hit. For locking:

```bash
# On instance A: hold an exclusive lock for 30 seconds
flock -x /mnt/cache/.locktest -c 'sleep 30'

# On instance B, while A still holds it: must fail immediately
flock -n -x /mnt/cache/.locktest -c true && echo "BROKEN: locks are host-local" || echo "OK: lock is enforced across hosts"
```

If B acquires the lock, coordination is unsafe. Fix the mount before running more than one proxy
on the volume.

## Cache volume dedication and storage-layer dedup/compression

Two recommendations about the storage under the cache. The capacity budget never depends on what
the volume reports, so neither one is needed for `max_cache_size` to be enforced.

**Give the cache its own storage unit (recommended).** Make `cache_dir` the root of a file
system, dataset, volume or quota-scoped share that nothing else writes to. The capacity budget
(`max_cache_size`, `eviction_trigger_percent`, `eviction_target_percent`) works the same either
way. On a shared unit the disk-exhaustion guard can be more conservative than necessary, because
the file system's free space also reflects bytes the proxy did not write, and the
`cache.consumed_bytes` gauge charges a neighbor's growth to the cache. Two of the shipped example
configurations, `docs/examples/config-production.yaml` and
`docs/examples/config-high-performance.yaml`, point `cache_dir` at a mounted volume for this
reason; see the comment on `cache_dir` in each.

**If the storage deduplicates or compresses, set `compression.enabled: false`.** LZ4 output has
no fixed alignment, so proxy-side compression defeats storage-level deduplication: the storage
sees different bytes for logically identical content and cannot deduplicate them.

On storage that deduplicates or takes snapshots, deleting cache data may not free physical
capacity at once. Snapshots pin freed blocks until they expire, and global deduplication may
free nothing for a range that shared blocks with data still referenced elsewhere. The storage's
own consumed-bytes figure then understates what eviction freed; it does not contradict the
eviction log. Snapshots have the same effect at the file system level without deduplication: a
range the proxy evicted stays charged against the volume's consumed bytes until the snapshot
that references it expires.

## Journal-based metadata writes

Writing `.meta` files in place from several hosts would corrupt them. Instead, each proxy
appends metadata updates to its own journal file, and a background consolidator folds the
journals into the sharded `.meta` files under a lock.

```text
cache_dir/metadata/_journals/{instance_id}.journal
```

A proxy writes only its own journal, so request handling never contends on it. Contention is
confined to the consolidator, which runs every `consolidation_interval` (default `5s`).

Consequences:

- **Peers see a metadata update after a delay.** The update becomes visible after the writing
  proxy's next journal flush and the next consolidation cycle, single-digit seconds at the
  defaults. Cached data is visible as soon as its range file lands; only the metadata index lags.
- **A stalled consolidator accumulates journals.** Watch
  `consolidation.last_consolidation_timestamp` on `/metrics`. If it stops advancing, metadata
  updates are piling up unapplied.
- **Cache-hit updates are buffered before they reach the journal.** The buffer flushes every 5
  seconds, or sooner when it holds 10,000 distinct entries, and neither value is configurable. A hit on
  one proxy therefore reaches another proxy's eviction decisions only after that flush and the
  next consolidation cycle. Despite its name, `ram_cache_flush_interval` does not drive this
  flush. It sets the width of the cross-proxy activity window that eviction consults, which is
  twice its value.
- **A proxy that stops without its final consolidation leaves its last few seconds of updates in
  its journal.** After a kill, a crash or an out-of-memory stop, the next consolidation cycle on
  any proxy applies them, usually a peer's on a fleet. A proxy that restarts and finds an
  unapplied journal from a stopped process on its own host, last written more than 5 minutes
  before the start, removes it and logs a warning naming the file. Those objects are fetched
  from S3 again.

Lock acquisition retries up to `lock_max_retries` times (default `5`), with exponential backoff
between attempts. The retry count is the whole bound; there is no time limit.
`cache.metadata_lock_retry_exhausted_total` on `/metrics` counts the callers that used up their
retries and gave up. A request waiting for a metadata slot lock is bounded by
`lock_acquire_wait_budget` instead, as described under
[advisory file locks](#2-advisory-file-locks-must-be-enforced-across-hosts).

**Metadata locks are never taken from their holder.** There is no staleness timeout and no
takeover: a contended lock is retried, and nothing more. The kernel releases a `flock` when the
holding process dies, and the NFS server releases it when a client's lease expires, so a lock
that is still held belongs to something live. Breaking it would remove mutual exclusion instead
of recovering it. Deleting the lock file releases nothing either: `flock` binds to the open file
description rather than the path, so the holder keeps locking an unlinked inode while the next
opener locks a new one, and neither can detect the other.

**A partitioned proxy can lose its lock without knowing.** The server's release at lease expiry
is a safety boundary, and the lock does not fence the publish: the lock is held on a slot file,
while the update it protects publishes a new `{key}.meta`, and nothing ties that publish to the
lock still being held.

Reaching that state needs a network partition longer than the cache volume's NFS lease and a
peer update to the same key during it. The server releases the partitioned proxy's lock, a peer
takes the slot and publishes, and then the partitioned proxy reconnects and publishes the
document it read before the partition, discarding the peer's update. Neither side sees an error
and no counter records it, so the cost is one lost metadata update on a volume that reports
healthy.

Most of the effect corrects itself. A later read of the affected object refetches it from S3 and
rewrites the entry, including an entry the lost update left unreadable.
[Orphan recovery](#orphan-recovery) deletes range files that no metadata entry names, on its own
rotation. One case is corrected by neither: a lost invalidation restores the entry it deleted,
so a HEAD answered from that entry returns the previous object's `ETag` and `Content-Length`
until `head_ttl` expires (default `60s`). Ranged reads of the object miss and refetch, so no
stale object data is served. For a prefix that cannot accept that window, set a
`head_ttl: "0s"` rule, which revalidates every HEAD against S3. There is nothing else to
configure and nothing to recover by hand.

Metadata locks are a fixed set of slot files under `metadata/_meta_locks/`, created on first use
and never deleted. A cache key maps to a slot by hash, so two keys can share a slot and then
serialize against each other. That costs the duration of one `.meta` read-modify-write and does
not affect correctness. The slot files are empty: the operating system enforces ownership, and
nothing in a file records it.

## Distributed eviction

One proxy at a time runs an eviction pass, coordinated through
`cache_dir/locks/global_eviction.lock`. The lock holder decides what to delete, and the deletes
can be shared across the fleet (see [Which proxy deletes](EVICTION.md#which-proxy-deletes)).
Without the lock, several proxies would each decide the cache is over capacity and evict at
once, each unaware of what the others freed, and overshoot the target.

The lock is a `flock`, so like the metadata locks it is held only while its holder is alive: the
kernel releases it when the holding process exits, and the NFS server releases it when a
client's lease expires. A held eviction lock is never broken. `eviction_lock_timeout` (default
`60s`, valid from 30 to 3,600 seconds) decides only whether a proxy that finds the lock held
tries once more before skipping the pass. It tries again when the acquisition time the holder
recorded in the lock file is older than the timeout. No value lets two proxies run a pass at
once.

Two counters under `eviction_coordination` on `/metrics` report the lock.
`evictions_skipped_lock_held` rises in normal operation, each time another proxy held the lock.
`stale_locks_recovered` counts second attempts that succeeded because the holder released the
lock between the two tries, after holding it longer than `eviction_lock_timeout`. It does not
mean a live holder was displaced, and it counts the eviction lock only: the metadata slot-lock
figures under `atomic_metadata` cannot record a takeover, because that lock is never taken from
its holder.

The lock serializes passes. A pass triggered by the count of staged ranges also needs to know
what earlier passes on other proxies freed, because the count it reads lags the deletes, so each
proxy publishes what its passes reclaimed under `metadata/_shard_state/eviction_reclaims/`. See
[Coordination across proxies](EVICTION.md#coordination-across-proxies).

## Size tracking

Each proxy keeps an in-memory accumulator of the size changes its writes and evictions make, and
flushes it to its own delta file in `size_tracking/`. The consolidator sums the delta files into
`cache_dir/size_tracking/size_state.json` under that file's lock. Eviction triggers on that
consolidated figure.

### Concurrent-write over-counting

When a proxy publishes a range file, it credits the range's size only if no file already existed
at that path, so a range another proxy cached first is not counted twice. The check and the
rename are separate steps, and NFS gives no cross-host atomicity between them. When several
proxies publish the same range at the same moment (a cold-cache stampede), more than one can see
no file and credit it, though only one file survives. The over-count is bounded by the number of
proxies that published the range.

**Over-counting is the safe direction.** It triggers eviction early rather than late, so the
disk does not fill. Eviction decrements the accumulator for each deleted range, so the tracked
size converges as eviction runs, and the
[re-grounding passes](EVICTION.md#re-grounding-the-size-figures-the-triggers-read) reconcile the
remainder.

If early eviction is a problem, raise `max_cache_size` or reduce how many proxies can miss on the
same key at once. The [validation scan's](#the-validation-scan) schedule cannot be changed.

## The validation scan

A daily scan reads every cached `.meta` file and reconciles tracked size against the sizes those
files record. It does not stat the range (`.bin`) files, so it does not detect a `.bin` that no
`.meta` entry names; [orphan recovery](#orphan-recovery) handles those.

**The schedule is fixed.** The scan fires once a day at midnight local time plus up to one hour
of random jitter, so the proxies do not all try at once. `validation_frequency` is parsed,
range-checked and logged at startup, but the scheduler never reads it.

The scan holds a validation lock on the cache volume for its whole duration, and a proxy that
finds the lock held does not scan, so one proxy scans the volume per cycle.

**A starting proxy does not count the cache.** If `size_state.json` is missing on a cache that
holds data, is unreadable, or has no object count, the proxy logs the size as unvalidated and
requests a full validation scan. A proxy that starts more than 23 hours after the last
validation requests a scan too. Either request runs under the same validation lock. Until the
scan finishes, a proxy whose size figure was missing, unreadable or without an object count
under-counts, so size-based eviction fires late. A proxy that started only overdue keeps its
saved figure.

**The scan deletes `.meta` files it cannot parse**, logging
`Removed invalid metadata file during validation` for each one. An unparseable entry cannot be
recovered, and leaving it would fail every later scan the same way.

### Full and rolling modes

The scan picks one of two modes from how long the previous scan took.

**Full mode** scans all 256 L1 shard directories in parallel. It runs on the first scan and
whenever the previous scan finished inside the time budget.

**Rolling mode** scans a subset of L1 directories per cycle, resuming from a persistent cursor,
and reaches full coverage over several cycles. It starts automatically when a full scan exceeds
`validation_max_duration` (default `4h`, valid from 10 minutes to 23 hours).

Mode selection:

| Previous scan | Next mode |
|---|---|
| None (first ever) | Full |
| Full, within budget | Full |
| Full, exceeded budget | Rolling |
| Rolling, extrapolated full time > budget | Rolling |
| Rolling, extrapolated full time ≤ budget | Full |

`validation_max_duration` is the only setting, and it does two things. It selects the next
cycle's mode, per the table above. In rolling mode it is also a real budget: the elapsed time is
checked before every shard visit, so a cycle that reaches the budget part-way through a rotation
stops cleanly and leaves a durable cursor for the next cycle. A shard is visited whole or not at
all, so a part-scanned shard never contributes a size figure. A rolling cycle therefore overruns
its budget by at most one shard's scan, however large the cache is.

A full scan still runs to completion and then warns that it overran
(`Full validation scan exceeded time budget`), and the following cycle switches to rolling. So
the first time a scan crosses the threshold, it runs for as long as it needs to, however far
past the budget that is.

### Rolling scan internals

- **Adaptive batch sizing.** The scan uses the previous cycle's rate (seconds per L1 directory)
  to estimate how many directories fit in the budget. The first cycle defaults to 64.
- **Proportional correction.** The scan adjusts tracked size by the drift observed in the
  scanned subset instead of replacing the total, so a partial scan cannot cause a large swing.
- **Cursor persistence.** Position, mode, scan rate and rotation count live in
  `cache_dir/size_tracking/schedule.json`, and the cursor is made durable after every shard
  visit rather than once per cycle. If that file is missing or corrupt, the scan adopts the same
  fields from `validation.json`; if neither is readable the cursor resets to 0.
- **Two state files.** `schedule.json` holds the scheduler's state. `validation.json` holds the
  last scan's results, which `/metrics` exposes as `cache_size.last_validation` and
  `cache_size.last_validation_drift`.
- **Rotation tracking.** When the cursor wraps past all 256 directories, the scan logs a
  rotation completion with the elapsed time.
- **Caches holding several buckets.** The cursor addresses 256 L1 index slots, and one slot
  covers that index in every bucket. On a cache holding several buckets, a cycle's recorded rate
  therefore understates the work per slot, which makes batch sizing and the extrapolated
  full-scan time optimistic.

### The correction is unconditional

The scan always applies its size correction. There is no minimum drift below which it skips the
adjustment.

`validation_threshold_warn` (default `5.0`) and `validation_threshold_error` (default `20.0`)
are drift percentages that select only the log level, WARN above the first and ERROR above the
second, and only for rolling scans. A full scan always logs the drift it corrected and reads
neither field.

Read `cache_size.accounting_drift` on `/metrics` for the drift. It reports all three accounted
figures, carries the sample's timestamp, and names a cause when a gauge cannot be evaluated. One
such case is a rolling cycle, whose reference figure is computed from the counter it would be
compared against, so no drift can be read from that pair.

Do not read `cache_size.last_validation_drift` as the drift. Three producers write that single
number: a full scan writes a real whole-cache drift, a rolling cycle writes the bytes of the
subset it visited minus the whole-cache counter, and a proxy that starts on an empty cache whose
validation is also overdue writes a literal zero. On a fleet, persistent large drift can come
from [concurrent-write over-counting](#concurrent-write-over-counting).

### Monitoring

The proxy logs the selected mode, the reason, the time budget, directories scanned, objects
validated, scan rate and cursor position at INFO:

```bash
journalctl -u s3-proxy | grep -i validation
```

## Sizing the metadata rate

Every request that misses a proxy's RAM metadata cache reads a `.meta` file from the cache volume, and a cache miss writes one as well. Whether the volume can serve that read rate depends on its storage type (EFS, FSx, an on-premises NFS server) and on how many proxies share it. Measure the volume before sizing a fleet on it.

**The storage type is an input with no default.** Rates do not carry across storage types, and on some a growing fleet lowers the aggregate rate. A figure from one storage type applied to another is wrong with nothing to signal it. A figure for one throughput tier or HA configuration does not carry to another of the same type either. [AWS deployment](AWS_DEPLOYMENT.md#sizing-the-metadata-rate) lists measured rates for EFS and FSx.

**Measure it with `s3-proxy bench-metadata`.** The subcommand reads and parses `.meta` files under `cache_dir` at a sweep of concurrency levels and reports objects per second at each. It is read-only: it lists directories and reads files under `<cache_dir>/metadata`, and never writes, renames, deletes, or locks anything, so it can run against a volume that live proxies are using. It does not need a running proxy.

```bash
s3-proxy bench-metadata --config /etc/s3-proxy/config.yaml  # Text report, default sweep 1,4,...,256
s3-proxy bench-metadata --cache-dir /mnt/cache --concurrency 32,64 --json  # JSON report
```

| Flag | Default | Meaning |
|---|---|---|
| `--config <FILE>` or `--cache-dir <DIR>` | one is required | Where the cache is; `--config` reads only `cache.cache_dir` |
| `--concurrency <LIST>` | `1,4,8,16,32,64,128,256` | Reads outstanding at each level |
| `--files-per-level <N>` | `2000` | Files each level reads, from its own set, so no level re-reads a file another level pulled into the page cache |
| `--max-level-secs <SECS>` | no limit | Stop each level after this long |
| `--max-l1-shards <N>`, `--max-l2-per-shard <N>` | `64`, `512` | Bound enumeration, so the command never walks the whole tree |
| `--skip-l1-shards <N>` | `0` | Start further into the tree, so a repeat run reads files the last run did not |
| `--start-sweep-at <UNIX_SECS>` | none | Wait until this time before reading, so several hosts measure the same window |
| `--json` | off | Print JSON instead of text |

The sweep needs `levels × files-per-level` metadata files; on a smaller cache each level reads fewer files, and the report says so. One host gives the one-proxy rate. For the fleet aggregate `A(I)`, run it on `I` hosts with the same `--start-sweep-at` and a different `--skip-l1-shards` on each, so the hosts read different files, then add the hosts' rates at the same level. Read the rate at the concurrency your proxies reach.

**A figure measured with no serving traffic on the volume is a ceiling.** A busy volume, or a metadata working set larger than the file server's memory, delivers less. A run against a volume live proxies are using measures what is left over at that moment.

**The rule.** The RAM metadata cache must absorb the share of metadata reads the volume cannot serve:

```text
h ≥ 1 − A(I) / R
```

where `R` is the fleet's metadata demand in operations per second and `h` is the fraction of it served from RAM. Count `R` as at least the request rate that reads metadata (GET and HEAD, hit or miss); misses also write metadata, so the true demand is higher, and so is the required `h`.

**Check `h` on the running fleet**, per proxy over a window, as `metadata_cache_hits / (metadata_cache_hits + metadata_cache_misses)` from `/metrics` ([Metadata cache tier](METRICS_REFERENCE.md#metadata-cache-tier)). The metadata cache is per process, so each proxy has its own `h`. Two settings raise it ([Metadata cache](CONFIGURATION.md#metadata-cache)). `cache.metadata_cache.max_entries` (default 100,000) holds more objects. `cache.metadata_cache.refresh_interval` (default `5s`) sets how often a cached entry is re-read from the volume, so a hot object costs one volume read per proxy per interval. `h` depends on how often your workload re-reads the same objects, so measure it rather than predicting it.

## Scanner concurrency

Two maintenance passes walk the cache volume from every proxy: the staged fold, which recomputes
the write-cache figure, and the ledger-repair rotation. On some storage types the fleet's
combined scan rate falls once too many proxies scan at once (see
[Sizing the metadata rate](#sizing-the-metadata-rate)), so each proxy limits how
many proxies may run each pass.

Before a pass, the proxy counts the other proxies currently scanning. If that count has reached
the pass's concurrency `k`, the pass defers to the next consolidation cycle. Each proxy sets its
own `k` from its own measurements: after every pass it compares the read rate it measured at
each level of concurrency, moves `k` to the level with the best estimated fleet rate, and
occasionally tries one level higher. `k` starts at the number of proxies seen scanning, so
nothing defers until there is evidence.

The limiter is on by default and has no configuration field. The validation scan publishes that
it is running, so the fold and repair passes count it, but the validation scan itself never
defers.

A deferral cannot starve a rotation. When a pass's stalest shard is older than three quarters of
its 24-hour window (18 hours), or its age cannot be read, the pass runs regardless of `k`.

The `write_cache.scanner_*` and `write_cache.scan_deferred_total_*` fields on `/metrics` report
the limiter per proxy; see the [write_cache metrics](METRICS_REFERENCE.md#write_cache).
`scan_deferred_total_*` counts consolidation cycles that deferred, rather than passes.

## Orphan recovery

An orphan is a range (`.bin`) file that no `.meta` lists, left by a crashed writer or by an
invalidation that removed the metadata. Eviction never selects it and its bytes are not in
`cache.total_cache_size`, so disk usage on the cache volume can exceed what
`cache.total_cache_size` reports by the bytes orphans hold.

The orphan reclamation rotation removes them. It is safe only while every proxy writing to the
cache volume holds the object's metadata lock from publishing a range file until it has
journaled it. Each proxy that does so publishes a readiness record saying so, and a proxy runs a
pass only while every active proxy has one. A proxy is active for an hour after it last wrote a
journal entry. Proxies write journal entries when they fill the cache from S3, record cache
hits, evict, or complete multipart uploads; a PUT stored in the write cache writes none. Each
proxy checks readiness before every pass, before each directory and again for each candidate.
It skips the pass when it cannot read the list of active proxies, and when the list is empty
because no proxy has written a journal entry for an hour; passes resume with the next journal
entry. For the rotation during an upgrade, see
[During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade).

The rotation never removes a range file that any proxy may still be writing: it keeps every
range file younger than 10 minutes and every part of a multipart upload that is still being
completed. Nothing is lost while a pass waits; orphaned files stay until a later pass.
`orphan_reclaim.passes_gated` on `/metrics` counts the passes skipped.

Each proxy runs one pass every 300 seconds, capped at 30 seconds or 4,096 directories, and
resumes where its previous pass stopped. Proxies claim one `(bucket, first shard level)`
directory at a time under a lease, so two proxies never scan the same directory at once, and
each proxy added to the fleet adds its own pass per interval. Files at the `ranges/` root,
outside any shard directory, are covered once per rotation.

A candidate is deleted only when, with the object's metadata lock held, its `.meta` does not
list it and every proxy's journal was read in full and holds no pending write for it. If any
journal cannot be read, or the cache key cannot be recovered from the file name, the file is
kept. Orphans are never added back to metadata. Deleted bytes are not subtracted from
`cache.total_cache_size`, because they were never counted in it.

The `orphan_reclaim` section of `/metrics` reports the rotation's work and progress; see the
[orphan_reclaim metrics](METRICS_REFERENCE.md#orphan_reclaim). The rotation's log lines contain
`Orphan reclaim:`:

```bash
journalctl -u s3-proxy | grep 'Orphan reclaim:'
```

`orphan_recovery_enabled`, `orphan_recovery_interval`, `orphan_scan_timeout` and
`orphan_max_per_cycle` are ignored; see
[Configuration fields that are now ignored](UPGRADING.md#configuration-fields-that-are-now-ignored).

## Failure modes

| Symptom | Likely cause |
|---|---|
| High miss rate on repeat downloads, each proxy fetching from S3 on its own | `lookupcache=pos` missing |
| Concurrent eviction, corrupt `.meta`, interleaved multipart publishes, nothing in the logs | Locks are host-local: `nolock`, `local_lock=flock`, `local_lock=all`, or NFSv3 without working NLM |
| `consolidation.last_consolidation_timestamp` stops advancing | Consolidator stalled; metadata updates accumulating in journals |
| WARN `Discarded unapplied journal of a stopped process on this host` at startup | A proxy on this host stopped without its final consolidation and was restarted more than 5 minutes later, and no peer applied its journal in between. On a fleet the file usually holds only lines no proxy could apply. On a single proxy its last few seconds of cache fills were not recorded, so those objects are fetched from S3 again and their range files are left for [orphan recovery](#orphan-recovery) |
| `cache.metadata_lock_retry_exhausted_total` climbing | Metadata slot-lock contention: callers are using up `lock_max_retries` and giving up. Raise `lock_max_retries`, or lengthen `consolidation_interval` so the consolidator competes for the same slots less often. `atomic_metadata.metadata_lock_hold_time_ms_total` divided by `metadata_lock_acquisitions_successful` shows whether the holds themselves are slow, which points at volume latency instead |
| `atomic_metadata.metadata_lock_wait_budget_expired_total` climbing | Metadata slot-lock contention on the budgeted path: requests give up after `lock_acquire_wait_budget` instead of waiting. The client sees a retryable error and nothing was written. Raise the budget to favor waiting over failing, or reduce how long holds last; the same hold-time division as the row above separates contention from a slow volume |
| `eviction_coordination.stale_locks_recovered` climbing | Eviction passes holding the lock longer than `eviction_lock_timeout`, with a peer retrying as one ends. No two proxies ran a pass at once |
| Tracked size above actual disk usage | [Concurrent-write over-counting](#concurrent-write-over-counting) during a cold-cache stampede. Bounded by the number of proxies and corrected by the re-grounding passes |
| Disk-cache reads capped near 625 MB/s per proxy on FSx | `nconnect` missing; one TCP flow hitting the EC2 5 Gbps single-flow limit |
| Slow downloads on cache misses while hits are unaffected, `tee_backpressure_wait_ms_total_get` rising | Cache volume write throughput is too low for the miss rate; see [write-side sizing](AWS_DEPLOYMENT.md#writes-bind-before-reads-do-and-a-slow-volume-degrades-differently-than-a-failed-one). Nothing is written on a hit, so the slowdown follows the miss rate instead of affecting every request |
| `cache_rules.on_fallback: true` | A `cache_rules.json` edit failed to parse; the proxy kept the last good rule set |

## See also

- [Configuration reference](CONFIGURATION.md#multi-instance-coordination): every `cache.shared_storage` field
- [Eviction](EVICTION.md): capacity limits, triggers and how the fleet shares eviction
- [Caching](CACHING.md): what is cached and how to control it
- [Metrics reference](METRICS_REFERENCE.md): the `atomic_metadata`, `eviction_coordination`, `consolidation` and `cache_size` sections
- [AWS deployment](AWS_DEPLOYMENT.md): FSx for OpenZFS sizing and the reference architecture
- [Error handling](ERROR_HANDLING.md): corruption, orphaned files and recovery
- [Shared cache access model](ARCHITECTURE.md#shared-cache-access-model): who can read what the cache holds
