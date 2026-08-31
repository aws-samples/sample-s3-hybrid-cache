# Local NVMe Cache Fleets

An alternative to the [shared cache volume](AWS_DEPLOYMENT.md#shared-cache-volume). Give each
proxy its own local NVMe instance store, and front the fleet with an
[affinity router](REQUEST_AWARE_ROUTING.md), an HAProxy tier that hashes each request to one proxy
by object key and page index. The private disks then behave as one cache. On AWS pricing a
capacity-matched fleet costs about 39% less, delivers the same bytes to clients on a disk cache
hit, and drops the shared volume's provisioned read ceiling. In exchange the cache is lost whenever
an instance is replaced, and every client must reach the fleet through such a router.

The shared volume remains the default and suits most deployments.
[Choosing between the two](#choosing-between-the-two) has the full comparison and the criteria for
picking one.

The cost case here is an AWS one, where the shared volume is a metered service. On-premises, where
NFS is already in place and probably already paid for, the reason to choose this is per-instance
disk throughput rather than price.

Router placement differs the same way. The sidecar in the diagram below is an AWS accommodation: a
central router tier there needs a load balancer in front, whose per-GB charge is part of what this
pattern sets out to avoid. Nothing meters that hop on-premises, so a central tier is the
recommended placement there and a router on every client host is unnecessary. It still costs a
component to operate, since a central tier means two HAProxy instances behind a floating address
managed by keepalived or an equivalent, and if you already run a load-balancing tier this can
replace it. See [Where the router runs](REQUEST_AWARE_ROUTING.md#where-the-router-runs).

```
        Region (compute)                              Origin
 ┌───────────────────────────────────────┐
 │  client host A         client host B  │
 │  app → haproxy         app → haproxy  │   sidecar: the cleartext hop
 │        └──────────┬──────────┘        │   never leaves the host
 │       ┌───────────┼───────────┐       │
 │    proxy A     proxy B     proxy C    │
 │       │           │           │       │
 │    ┌──┴───┐    ┌──┴───┐    ┌──┴───┐   │
 │    │ NVMe │    │ NVMe │    │ NVMe │   │
 │    │pages │    │pages │    │pages │   │
 │    │ 0,3  │    │ 1,4  │    │ 2,5  │   │
 │    └──────┘    └──────┘    └──────┘   │            ┌─────────────────┐
 │       └───────────┼───────────┘       │            │ S3 (other       │
 │                   └── misses only ────┼───────────▶│ region) or      │
 │                         high RTT      │            │ external store  │
 └───────────────────────────────────────┘            └─────────────────┘
```

Each proxy owns a disjoint slice of the cache, and the router is what makes that true. Compare the
[shared-volume topology](AWS_DEPLOYMENT.md#when-this-guide-applies), where one file system sits
below all three proxies and any of them can serve any object. Drawn with sidecar routers for the
reason above; a central tier works the same way for ownership.

Like the rest of this project, everything here is sample configuration for you to evaluate,
not a supported deployment.

**About the figures.** Prices are AWS list, on-demand, Linux, us-west-2, 
retrieved 2026-08, with FSx for OpenZFS (FSxZ) `SINGLE_AZ_HA_2` as the shared-volume
comparison.

- [Which requests share an owner](#which-requests-share-an-owner)
- [The prerequisite](#the-prerequisite)
- [What you gain](#what-you-gain)
- [What you give up](#what-you-give-up)
- [Cost](#cost)
- [Choosing an instance family](#choosing-an-instance-family)
- [Operating it](#operating-it)
- [Choosing between the two](#choosing-between-the-two)
- [Configuration](#configuration)
- [What to measure on a trial fleet](#what-to-measure-on-a-trial-fleet)
- [See Also](#see-also)

## Which requests share an owner

Without affinity, N proxies with private disks each hold roughly the same working set, because any
proxy may be asked for any object. Aggregate capacity is one instance's disk and the other N-1
copies are waste, which is why the shared volume has always been the requirement for a fleet.
Affinity routing gives each `(object, page)` exactly one owner, so each proxy caches a disjoint
slice and aggregate capacity becomes the sum of the disks. Three instances with 3.75 TB each hold
11.25 TB of distinct data rather than 3.75 TB held three times.

The routing key is the object path plus a page index, and a request carrying no `Range` header
gets the index `full`. Every whole-object `GET` of a key therefore converges on one instance,
along with its `HEAD`, `PUT`, `DELETE` and every multipart operation. Query strings do not split
that grouping: the router hashes HAProxy's `path`, which stops at the question mark, so
`?partNumber=2&uploadId=…` hashes the same as the bare key.

A whole-object workload is therefore the cleanest fit, with nothing duplicated. Artefact
distribution, media serving and restore traffic all look like this.

Mixing whole-object and ranged reads of the same keys stores them more than once. `full` is a
separate bucket from `0`, `1`, `2`, and a `bytes=-N` suffix read is a third, `tail`. An object read
whole, by page and by footer is cached on up to three instances where a shared volume would hold
one entry. Your read mix decides what that costs.

```
GET /bucket/key                    → hash(/bucket/key + full) → proxy B
GET /bucket/key  Range: bytes=0-…  → hash(/bucket/key + 0)     → proxy A
GET /bucket/key  Range: bytes=-N   → hash(/bucket/key + tail)  → proxy C
```

Write-through follows the same rule. A `PUT` lands on the `full` owner, so a later whole-object
read of that key hits while a later ranged read goes to a page owner and misses.

Multipart uploads land entirely on one instance. `CreateMultipartUpload`, every `UploadPart`,
`CompleteMultipartUpload` and `AbortMultipartUpload` carry the object in `path` and none carries a
`Range` header, so all four hash to the `full` owner. Staging lives on disk under
`mpus_in_progress/{uploadId}/`, so Complete reads the records its own parts wrote and finalises
normally. Keep the parts together by leaving `hash-balance-factor` out (see
[Router settings](#router-settings)). If they split across instances, S3 still completes the
upload, Complete waits 10 seconds for the missing records, and the object is not cached
([MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md#multi-instance-deployments) covers that case).

## The prerequisite

Ownership is what the pattern rests on, so **every client must reach the fleet through an
[affinity router](REQUEST_AWARE_ROUTING.md)**, at HAProxy 2.9 or later, configured as
Request-Aware Routing describes. Either placement works: a sidecar on each client host, or a
central router tier. They differ on cost rather than on affinity, and
[Where the router runs](REQUEST_AWARE_ROUTING.md#where-the-router-runs) compares them. If neither
is available to you, use a shared volume.

A client arriving over [multi-value DNS](GETTING_STARTED.md#3-configure-client-routing) or
through a load balancer lands on whichever instance its connection reached, so each proxy caches
the same working set independently. On a shared volume that costs a little RAM locality; on
private disks it costs the entire capacity gain, and health checks stay green while it happens.
A partly-covered fleet gets a proportionally smaller benefit and the same cold-start exposure.

## What you gain

**Cache capacity costs about a quarter as much per GB**, $0.028 against $0.120, and the `im4gn`
rate holds at every size in the family. [Cost](#cost) has the comparison.

**A disk cache hit costs one network crossing instead of two.** The
[NIC crossing table](AWS_DEPLOYMENT.md#sizing-the-network) puts a shared-volume cache hit at
two crossings, so roughly half of an instance's baseline bandwidth reaches the client. Local
NVMe reads across PCIe rather than the network, so a local disk hit behaves like a RAM hit and
delivers close to the full baseline. That doubles each instance's client-facing throughput on a
disk hit, which is what lets the recommended family carry half the baseline bandwidth of its
shared-volume equivalent and still serve clients as fast. Traffic that does not hit local disk,
including misses and uploads, gets the smaller NIC with nothing to offset it.

**No shared throughput ceiling.** A shared volume's read throughput is provisioned and fleet-wide:
the 640 MB/s tier in [Cost](#cost) is about 213 MB/s per proxy across three, and raising it is a
line item. Local NVMe gives each proxy its own device, so adding a proxy adds read throughput as
well as capacity rather than drawing from a fixed pool.

**Eviction runs in parallel.** A shared volume serialises eviction fleet-wide through a
distributed lock, so its throughput is one ceiling for the whole fleet however many proxies you
add. Each proxy now evicts its own cache independently, on local NVMe rather than over NFS, so
eviction capacity grows with the fleet. This matters most for a cache that churns many small
ranges, where per-operation cost rather than bandwidth is the constraint.

**Two mount requirements go away.** No `lookupcache=pos` and no `nfsvers=4.1` to pin, neither of
which [reports an error when absent](SHARED_STORAGE.md#mount-requirements).

**No shared dependency for fleet availability.** The volume's availability is currently the
fleet's availability, which is why the guide recommends an HA deployment type. Local disks
remove that coupling. A failed instance takes its own slice out and the ring reassigns it.

## What you give up

**The cache does not survive instance replacement.** Instance store is
[erased on stop, hibernate and terminate, and survives only a reboot](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-store-lifetime.html).
Restarting the proxy process keeps the cache. Replacing the AMI, stopping a development fleet,
a Spot interruption or an instance retirement does not. [Operating it](#operating-it) costs this
out and covers how to manage the frequency.

**Ownership churn costs an origin fetch rather than RAM locality.** When fleet membership changes,
the hash ring reassigns some pages to new owners. With a shared volume the new owner reads those
bytes off the volume. With local disks it fetches them from the origin again, so a change of
membership costs transfer as well as latency. The trade is churn cost against storage cost, and
churn happens at a frequency you control; [Operating it](#operating-it) puts numbers on both sides.

**Invalidation stops being fleet-visible.** A `PUT` or `DELETE` carries no `Range` header, so it
reaches the key's `full` owner and only that instance
(see [Which requests share an owner](#which-requests-share-an-owner)). The instances holding the
same object's pages never see it. On a shared volume the metadata is shared, so the change reaches
every instance; on local disks the page owners keep serving their cached ranges until `get_ttl`
elapses.

This stays inside the documented [freshness contract](CACHE_FRESHNESS.md): staleness remains
bounded by `get_ttl`, and `get_ttl: 0` still forces revalidation. What changes is the effective
window on instances that did not receive the write. If you rely on overwrite-then-read or
delete-then-read behaviour for ranged reads, set `get_ttl` accordingly for those keys or use a
shared volume.

**Capacity is coupled to instance count.** You cannot add cache without adding instances, or
without moving to a storage-dense instance family and accepting its network and CPU profile. A
shared volume lets you size cache and compute independently.

**Fleet-wide configuration loses its single edit.** `cache_rules.json` lives in the cache
directory, so it follows the cache onto local disk and has to reach every host. Hot reload
survives the move. `config.yaml` and the TLS certificate are separate paths already and can stay
where they are, including on a small shared volume kept only for them. See
[Distribute `cache_rules.json`](#distribute-cache_rulesjson-to-every-host).

**A spilled request becomes an origin fetch**, which puts a per-GB cost on the router's
`hash-balance-factor`. See [Router settings](#router-settings).

## Cost

One comparison, matched on both capacity and compute. `m8gn.2xlarge` is the shared-volume side:
Graviton and network-optimized like `im4gn`, same 8 vCPU and 32 GiB, so the two fleets differ only
in where the cache lives.

| Three proxies, 11,250 GB of cache | Shared volume | Local NVMe |
|---|---|---:|
| Instances | 3× `m8gn.2xlarge` | 3× `im4gn.2xlarge` |
| Instance cost | $1,274.58 | $1,593.40 |
| Cache storage | $1,345.30 (FSxZ `SINGLE_AZ_HA_2` @ 640 MB/s) | included |
| **Total per month** | **$2,619.88** | **$1,593.40** |
| Cache cost per GB-month | $0.120 | $0.028 |
| Aggregate baseline bandwidth | 75 Gbps | 37.5 Gbps |
| Client-facing throughput on a disk hit | 37.5 Gbps | 37.5 Gbps |

**$1,026/month cheaper, a 39% saving, at the same client-facing throughput on a disk hit.** A
shared-volume cache hit crosses the proxy NIC twice, once to read the volume and once to write the
client, where a local disk hit crosses it once. Half the raw baseline delivers the same bytes to
clients. On traffic that does not hit local disk the smaller NIC is simply smaller, so size the
fleet against your expected hit ratio rather than against the disk-hit row alone.

**Capacity scales by instance size, at a flat rate.** The `im4gn` ladder doubles NVMe at each step,
937 GB on `large` through 30,000 GB on `16xlarge`, and the disk's share of the price is
$0.0283/GB-month at every one of them. So a larger cache means a larger size rather than more
instances, and the cost per GB does not move.

**FSxZ costs more per GB at every size.** Its figure falls as capacity grows, since the fixed
throughput charge spreads further, but it bottoms out at the $0.09/GB-month SSD rate, three times
`im4gn`'s $0.028. What a shared volume wins is independence: capacity you can grow without changing
the fleet. If you need a large cache behind a small amount of compute, that is the case for it, and
it is the coupling under [What you give up](#what-you-give-up).

## Choosing an instance family

Use **`im4gn.2xlarge`**: the lowest cost per GB of cache here, and tied with `m6idn.2xlarge` for
the highest bandwidth of any row carrying NVMe. Its 32 GiB of RAM matches `m8gn.2xlarge`, so RAM
cache and in-flight budgets carry over unchanged.

The delta columns price the disk alone, against `m8gn.2xlarge` as the equivalent Graviton
network-optimized instance with no instance store.

| Type | Arch | NVMe | $/hr | $/mo delta | $/GB-mo | Baseline bandwidth |
|---|---|---:|---:|---:|---:|---:|
| `m8gn.2xlarge` | arm64 | none | $0.5820 | reference | n/a | 25.0 Gbps |
| **`im4gn.2xlarge`** | **arm64** | **3,750 GB** | **$0.7276** | **$106.27** | **$0.028** | **12.5 Gbps** |
| `m6idn.2xlarge` | x86_64 | 474 GB | $0.6365 | $39.77 | $0.084 | 12.5 Gbps |
| `is4gen.2xlarge` | arm64 | 7,500 GB | $1.1526 | $421.20 | $0.056 | 12.5 Gbps |
| `i7ie.2xlarge` | x86_64 | 5,000 GB | $1.0396 | $334.05 | $0.067 | 8.333 Gbps |
| `i8g.2xlarge` | arm64 | 1,875 GB | $0.6864 | $76.21 | $0.041 | 4.688 Gbps |
| `i4i.2xlarge` | x86_64 | 1,875 GB | $0.6860 | $75.92 | $0.041 | 4.687 Gbps |

`us-west-2`, on-demand, Linux, 730-hour month. Re-check rates for your Region. Every NVMe row sits
below `m8gn`'s 25 Gbps, which costs less than it appears, because a local disk hit crosses the NIC
once where a shared-volume hit crosses it twice.

`m6idn.2xlarge` is the one worthwhile alternative. $39.77/month is the cheapest uplift on the
table if 474 GB per instance is enough, and its small device also cuts the cost of losing a cache:
a replacement refetches 948 GB rather than 7,500 GB. See
[Replacement cost](#replacement-cost-has-a-fixed-ceiling).

**For more cache, add `im4gn` instances rather than a denser part.** Four `im4gn.2xlarge` hold
15,000 GB for $2.91/hour at 50 Gbps aggregate; three `i7ie.2xlarge` hold the same 15,000 GB for
$3.12/hour at 25 Gbps. `is4gen.2xlarge` makes the same case at 12.5 Gbps baseline (`im4gn`'s
network profile exactly, so no bandwidth penalty): two `im4gn.2xlarge` at $1.4552/hour hold the
same 7,500 GB as one `is4gen.2xlarge` at $1.1526/hour, for more money but twice the NICs and
twice the independent eviction rings. Scaling out costs less per GB, doubles the bandwidth, and
raises eviction throughput as well, since each instance evicts its own cache.

`i8g.2xlarge` and `i4i.2xlarge` hold *less* than `im4gn` as well as being slower, so neither has a
case. `i7ie`, `i8g`, `i4i` and `is4gen` each carry more RAM than `im4gn`'s 32 GiB, which does not
pay here: a RAM hit and a local disk hit both cross the NIC once, so a larger RAM tier buys
latency rather than throughput. `i8g` is Graviton but not network-optimized; there is no `i8gn`.

Build for aarch64 on a Graviton host and do not mix architectures within one fleet; nothing in the
proxy is architecture-specific. See
[Binary Portability](GETTING_STARTED.md#binary-portability).

## Operating it

Every transfer figure here is priced at **$0.02/GB**, the rate for S3 data transfer out to another
AWS Region when the origin is in the US, Canada or Europe. The charge falls on the origin's Region,
and from Asia Pacific, South America, Africa and most of the Middle East it is four to seven times
that, up to $0.147/GB from Cape Town. Check your origin's rate and scale these figures by it. None
of them include the latency a miss pays, which on a distant origin is the reason the cache exists.

A same-region origin carries no transfer charge, so churn costs latency alone. The exception is
objects in a storage class that bills per-GB retrieval, which is charged wherever they are read from
and adds to any transfer rate (see [Storage class](AWS_DEPLOYMENT.md#storage-class)). Refetching
those is never free, so read the figures below as transfer plus retrieval.

### The cost of a cold slice

Warming the full 11,250 GB fleet cache from a cross-region origin costs about **$225** in
transfer at $0.02/GB. GET requests add $0.0004 per 1,000, which at an 8 MiB mean range is another
$0.54 and can be ignored. Against the $1,026/month saving, break-even at that rate is roughly one
full-fleet cold start every seven days. Routine patching sits inside that. Autoscaling churn,
frequent Spot interruption, or stopping the fleet nightly does not, and Auto Scaling in particular
is worth sizing before you commit (see
[Auto Scaling](#auto-scaling-makes-churn-something-you-do-not-control)).

**A high-egress origin moves that break-even a long way.** At $0.09/GB the same cold start costs
about $1,012, so one full-fleet cold start a month consumes the whole saving, and at $0.147/GB it
exceeds it. For an origin in one of those Regions the pattern needs either genuinely low churn or
the live-fraction argument below to hold, and it is worth measuring rather than assuming.

### Replacement cost has a fixed ceiling

A replacement is a leave and a join: the departing instance's slice is redistributed to survivors
that do not hold it, and the replacement arrives with a new IP address, so it takes a fresh ring
position rather than reclaiming what left. Each half puts about one instance's capacity beyond the
reach of whichever instance now owns it.

Because aggregate capacity scales with instance count while the disturbed fraction falls as `1/N`,
those cancel: **the ceiling is about twice one instance's local capacity, whatever the fleet size.**
On `im4gn.2xlarge` that is 7,500 GB, or **$150**. A scale-out is a join and a scale-in is a leave,
so either costs half of that.

**The ceiling assumes every disturbed page is requested again.** Nothing is refetched until a client
asks for it, so the real figure tracks how much of the cache is live rather than how large it is, and
that is not the same as hit rate: a cache can serve a high hit rate from a small hot set while
holding several times that volume in entries nothing reads again. The third column scales the ceiling
to a cache that is a fifth live, to show how fast it falls off; substitute your own fraction. The
table bounds the exposure rather than forecasting it. The last column gives each row's cost as a
share of the $1,026 monthly saving, at the ceiling and at a fifth live.

| Event, three `im4gn.2xlarge` | Ceiling | At a fifth live | Share of the saving |
|---|---:|---:|---:|
| One instance replaced | $150 | $30 | 15% → 3% |
| Scale out or in by one | $75 | $15 | 7% → 2% |
| Monthly AMI update, rolling | $450 | $90 | 44% → 9% |
| Monthly AMI update, all at once | $225 | $45 | 22% → 4% |

Absolute cost scales with device size, so a 474 GB `m6idn.2xlarge` fleet's ceiling is about an
eighth of these figures, against a proportionally smaller saving.

### Not every outage is a replacement

The ring is keyed on the server's IP address, because that is what lets independent routers agree
(see [`hash-key addr`](REQUEST_AWARE_ROUTING.md#more-than-one-router-requires-hash-key-addr)). So
what an event costs depends on whether the address and the disk survive it:

| Event | Ring position | Instance store | Ceiling |
|---|---|---|---|
| Proxy process restarts | unchanged | intact | none |
| Instance reboots | unchanged | intact | misses during the window |
| Instance stops and starts | unchanged | erased | one instance's capacity |
| Instance is replaced | new | erased | twice that |

A VPC instance keeps its primary private address across a stop and start, so only a genuine
replacement moves ring position, and only the last row pays the rebalance half.

**That is also the fix.** Bring the replacement up on the departed instance's address and it
inherits the same ring position, which leaves only the erased cache to refetch. A fixed-size fleet
can do this by moving a secondary ENI onto the new instance; an Auto Scaling group will not do it
for you. Keying the ring on `hash-key id` would achieve the same thing through the server's slot
rather than its address, but that is exactly what stops routers agreeing when DNS returns members
in different orders, so it is not an option for a sidecar fleet.

While a member is unhealthy the router drains it and its pages go to the next instance on the ring,
which caches them. When it returns they route back, and the neighbour's copies become orphans that
eviction reclaims.

### Auto Scaling makes churn something you do not control

Under an Auto Scaling group, replacements arrive on their own schedule: a failed health check, a
Spot interruption, a scheduled retirement or an AZ rebalance each cost a full replacement, and
scaling costs a join or a leave in either direction. A group that tracks load through the day can
easily churn more than the pattern saves, which is why this suits a fixed-size group, or one that
scales rarely.

Two settings are worth choosing deliberately. Health-check grace period should exceed the time the
proxy needs to start serving, or a slow start is read as a failure and replaced, buying a
replacement's refetch for nothing. Instance refresh is the mechanism for an AMI update, and its `MinHealthyPercentage` picks
your column above: 100 replaces one instance at a time, lower values overlap.

### Replacing everything at once is cheaper and is an outage

Rolling reassigns ownership at every step, so rolling through all N instances disturbs about twice
the whole fleet's cache. Replacing everything at once leaves the fleet cold exactly once, for half
the transfer.

The catch is that it is downtime, not just a cold cache. With every proxy gone the router has no
healthy member and requests fail rather than slow down, and a sidecar has no other fleet to fall
back to. Take that option only if you can absorb the window or point clients straight at S3 for it.
Rolling keeps the fleet serving throughout and pays double the transfer for it.

### Warm deliberately after a replacement

The proxy holds no credentials and cannot prefetch, so warming means requesting the data through
it. Where the working set is predictable, reading it through the fleet after a replacement turns
an unpredictable latency regression into a scheduled transfer cost.

### What to alarm on

Cache hit ratio per instance, and origin fetch volume. Both come from one pair of OTLP-exported
counters, `cache.cache_hits` and `cache.cache_misses`, since a miss is by definition a GET forwarded
to S3. Both move if affinity degrades, whether because a client bypassed the router, two routers
disagree, or `hash-key addr` is missing. A hit ratio that falls with no change in workload points at
the routing layer. Dimension per instance rather than averaging: one member going cold is the
signal, and a fleet average hides it.

**One aggregation rule inverts here.** [OTLP_METRICS.md](OTLP_METRICS.md#cache--sizes) and
[METRICS_REFERENCE.md](METRICS_REFERENCE.md#cache) both describe `total_cache_size`,
`read_cache_size` and `write_cache_size` as fleet-wide and say not to aggregate them, which is
correct on a shared volume where every instance reports the same figure. With a cache directory per
instance each reports only its own slice, so the fleet figure is their sum, and a dashboard carried
over unchanged reads low by roughly the instance count.

Instance store also changes what CloudWatch sees. `DiskReadBytes` and `DiskWriteBytes` in the
`AWS/EC2` namespace measure instance store rather than EBS, so local cache throughput arrives with
no setup. There is no capacity metric though, and the FSxZ file-system metrics a shared volume
publishes are gone, so cache fill has to come from the proxy's own `max_cache_size_limit` and
`disk_safety` or from the CloudWatch agent. [Monitoring](AWS_DEPLOYMENT.md#monitoring) covers the
export path.

## Choosing between the two

| | Shared volume | Local NVMe |
|---|---|---|
| Every client behind an [affinity router](REQUEST_AWARE_ROUTING.md) | Not required | **Required** |
| Cache survives instance replacement | Yes | No |
| Capacity independent of instance count | Yes | No |
| Cache cost per GB-month, capacity-matched | $0.120, floor $0.09 | $0.028, flat across sizes |
| Client-facing throughput on a disk hit | ~50% of baseline | ~100% of baseline |
| Disk read throughput | Provisioned, shared fleet-wide | Local device, per instance |
| Eviction throughput | Serialised fleet-wide | Per instance, parallel |
| Invalidation visibility | Fleet-wide | Bounded by `get_ttl` on other instances |
| Fleet-wide config from one edit | Yes | Needs distribution |
| Mount options that fail silently | Two | None |
| Fleet availability depends on the volume | Yes | No |

**Where you run changes the balance, mostly through the router prerequisite.** On AWS a central tier
needs a load balancer in front, which reintroduces the per-GB charge this pattern avoids, so sidecars
are the practical option. On-premises a central tier is metered by nothing, so it is the recommended
placement, at the cost of running two HAProxy instances behind a keepalived floating address. That
address is also what lets a replacement keep its ring position. See
[Where the router runs](REQUEST_AWARE_ROUTING.md#where-the-router-runs).

Choose the **shared volume** when clients cannot all reach an affinity router, when the cache is
large and cool, when capacity has to scale independently of compute, when instances are replaced
often, including any fleet under an Auto Scaling group that tracks load, or when the origin sits in
a Region whose egress rate makes refetching a lost slice expensive (see
[Operating it](#operating-it)).

Choose **local NVMe** when every client routes through an affinity router, the fleet is a fixed
size and stable between patch cycles, and either cache cost per GB or per-instance disk throughput
is the binding constraint.

## Configuration

Two steps are required: size `cache.max_cache_size` for the device, and set the router's
`hash-balance-factor` deliberately. The rest is instance preparation and optional retuning.

### There is no shared-storage switch to turn off

**The coordination machinery has no `enabled` flag, and a local disk does not disable it.**
Journal-based metadata writes, per-key `flock` metadata locking, the global consolidation lock,
distributed eviction locking and the orphan-recovery sweep all stay active. As
[SHARED_STORAGE.md](SHARED_STORAGE.md#coordination-is-always-on) puts it, a single-instance
deployment "simply never contends", and a per-instance cache directory is that case repeated
once per proxy.

It costs little. One process owns the directory, so every lock is uncontended, `flock(2)` is
native on ext4 and xfs rather than emulated over NFS, and the journal and consolidation cycle run
against local NVMe.

**Change nothing under `cache.shared_storage`.** Its lock timeouts and retry counts all bound
waiting for a lock another instance holds, and there is no other instance.
`metadata_cache.stale_handle_max_retries` retries `ESTALE`, which a local filesystem does not
produce. `cache.metadata_io_concurrency` bounds concurrent blocking metadata reads to protect the
async runtime rather than to accommodate storage throughput, so the device does not change what it
is for either.

**Every `config.yaml` field is read once at startup**, with no reload signal and no file watcher,
so any change above needs a restart. [`cache_rules.json`](CONFIGURATION.md#cache-rules) is the
only file the proxy re-reads while running.

### Preparing the instance store

Instance-store devices arrive unformatted, so this belongs in userdata rather than `fstab`.

**Match devices by model, not by path.** On every family listed above, both the instance store
and the EBS root volume present as NVMe, so `/dev/nvme*n1` includes the root disk. Striping
everything that matches that glob will destroy your operating system. Naming is not stable
either: the root volume and the instance store can swap names across a reboot, and on
multi-device types the root volume can sit between the instance-store devices. Filter on the model
string instead:

```bash
lsblk -dno NAME,MODEL | grep 'Amazon EC2 NVMe Instance Storage' | awk '{print "/dev/"$1}'
```

**Some sizes present more than one device**, and a single cache directory needs them striped
into one filesystem. RAID 0 is the right level: the data is already ephemeral, so redundancy
buys nothing that surviving a device failure would not also require surviving a stop.

The count varies by type, so drive the array from the number you detect rather than one you assume,
and do not infer it from instance size: `i7ie.2xlarge` has two devices while the larger
`i7ie.3xlarge` has one. Ask the API for the type you plan to run:

```bash
aws ec2 describe-instance-types --instance-types im4gn.2xlarge \
  --query 'InstanceTypes[].InstanceStorageInfo.[TotalSizeInGB,Disks[0].Count]' --output text
```

```bash
#!/bin/bash
exec > /tmp/userdata.log 2>&1
set -euxo pipefail

CACHE_MNT=/mnt/nvme

mapfile -t DEVS < <(lsblk -dno NAME,MODEL \
  | grep 'Amazon EC2 NVMe Instance Storage' \
  | awk '{print "/dev/"$1}')

if [ "${#DEVS[@]}" -eq 0 ]; then
  echo "No instance-store NVMe present. Wrong instance type." >&2
  exit 1
fi

if [ "${#DEVS[@]}" -gt 1 ]; then
  # mdadm is not in the stock Amazon Linux 2023 AMI. Install it before use, or
  # the array steps below fail with "command not found".
  command -v mdadm >/dev/null || dnf install -y mdadm

  # Re-assemble before creating: instance store survives a reboot, so an array
  # built on an earlier boot may still hold a warm cache. Resolve it by NAME:
  # the kernel renumbers an auto-assembled array (md127 is common), so testing
  # for /dev/md0 would miss it and the create below would fail on busy devices.
  mdadm --assemble --scan || true
  TARGET=""
  for cand in /dev/md/cache*; do
    if [ -e "$cand" ]; then TARGET=$(readlink -f "$cand"); break; fi
  done
  if [ -z "$TARGET" ]; then
    mdadm --create --verbose /dev/md0 --name=cache --level=0 \
      --raid-devices="${#DEVS[@]}" "${DEVS[@]}"
    TARGET=/dev/md0
  fi
else
  TARGET="${DEVS[0]}"
fi

# Format only when there is no filesystem, for the same reason: a reboot
# preserves instance store, and an unconditional mkfs would discard a warm cache.
blkid "$TARGET" >/dev/null 2>&1 || mkfs.xfs "$TARGET"

mkdir -p "$CACHE_MNT"
mount -o noatime "$TARGET" "$CACHE_MNT"
mkdir -p "$CACHE_MNT/cache"

df -B1 "$CACHE_MNT"   # size max_cache_size against this, not the spec sheet
```

xfs and ext4 both work and both support the `flock(2)` the proxy relies on.

**Order the service after the mount.** Add `RequiresMountsFor=/mnt/nvme` to the unit so
systemd cannot start the proxy against a missing cache directory, add the cache path to
`ReadWritePaths` (`ProtectSystem=strict` otherwise makes it read-only), and start the service
at the end of userdata rather than relying on enable-at-boot.

Do not place the binary on the instance store. Bake it into the AMI or fetch it in userdata as
the existing [examples](AWS_DEPLOYMENT.md#bootstrap) do.

### Sizing the cache

**`cache.max_cache_size` defaults to 10 GiB and nothing raises it for you.** Left alone on a
3,750 GB device it uses 0.3% of the disk. Nothing validates the value against the device, so set
it deliberately.

Set it from the `Available` column of `df`, not from the instance specification, because two
reductions sit between them. AWS quotes instance-store capacity in decimal GB while a filesystem
reports GiB, about 7% smaller, and a fresh xfs then takes roughly another 0.7% for metadata. Both
rows below are measured on a formatted, mounted device:

| Type | Quoted | `df` size | `df` available | A reasonable `max_cache_size` |
|---|---:|---:|---:|---:|
| `m6idn.2xlarge` | 474 GB | 441.2 GiB | 438.1 GiB | `429496729600` (400 GiB) |
| `im4gn.2xlarge` | 3,750 GB | 3,490.8 GiB | 3,466.4 GiB | `3543348019200` (3,300 GiB) |

Above the real capacity, eviction never triggers, because its thresholds are percentages of
`max_cache_size`. The disk-safety check takes over instead: once free space drops below the
incoming object plus a 1 GiB reserve, the proxy declines to write-through cache, counts it as
`skipped_puts_total{reason="disk_safety"}`, and reports the `/health` cache component `Degraded`
for 300 seconds after each refusal. Uploads keep working throughout, since the body still streams
to S3 and S3's response is returned unchanged, but the cache stops taking new data. Keep
`max_cache_size` below the real capacity so eviction governs.

Three more settings to choose rather than inherit:

- **`cache.eviction_trigger_percent` (95) and `cache.eviction_target_percent` (80)** are a
  percentage band, so the absolute work per pass scales with the cache. On a 3,300 GiB cache
  that band frees about 500 GiB in one pass. Narrowing it to `95`/`90` frees about 170 GiB
  instead and runs more often, which is a good trade on NVMe where eviction is cheap and no
  longer serialised across the fleet. Target is clamped below trigger, so they cannot cross.
- **`cache.write_cache_percent` (10.0)** is a percentage of `max_cache_size`, so it scales with
  the figure above and generally needs no change. `cache.write_cache_max_object_size` (256 MiB)
  bounds single-part `PUT` bodies only: a multipart upload's parts are not checked against it, so a
  multipart object of any size is write-cached and any excess is reclaimed afterwards by eviction.
  Since clients switch to multipart well below 256 MiB, size the device against your real upload
  sizes rather than against that figure.
- **`cache.ram_cache_enabled` defaults to `false`**, and the RAM tier is where the second network
  crossing is saved. Enable it and set `cache.max_ram_cache_size` (512 MiB by default) against the
  instance's memory. It is force-disabled when `get_ttl` is `0`, with a log line saying so.

If your fleet mixes instance families, each family needs its own `max_cache_size`, so a
homogeneous fleet is meaningfully simpler to operate.

### Router settings

**Leave `hash-balance-factor` out.**
[Bounded load](REQUEST_AWARE_ROUTING.md#bounded-load-so-a-hot-page-does-not-pin-one-member) caps
an instance's share of in-flight requests and spills the excess to the next instance on the ring.
On a shared volume a spill is nearly free, because the spill target reads the same bytes off the
same volume. On local disks it fetches from the origin and caches a second copy.

Omitting it also keeps multipart uploads on one instance, since every part of an upload shares one
routing key. Ten parts in flight across three instances average 3.3 each, so the recommended factor
of 150 caps the owner at 5 and spills the rest. One large file through the AWS CLI can reach ten,
since `max_concurrent_requests` defaults to 10.

The cost is that a hot page concentrates on one instance's network interface. If you need that
spread, set the factor high enough that spill is rare, then check origin fetch volume and multipart
cache hits at the value you pick.

**`hash-key addr` on every server line.** Routers otherwise build their rings from server
identity, so two of them can disagree about which instance owns a page. Affinity then holds only
among the clients sharing one router, and the capacity gain goes with it. This applies to a
sidecar fleet and to a central HA pair alike.

**Page size.** The router's `div()` converter must match the cache's page size. Both default to
16 MiB; if a `cache_rules.json` rule sets a different `page_size`, use that value in the router.

### Distribute `cache_rules.json` to every host

The proxy reads it from `cache_dir/cache_rules.json`, so it now lives on each instance's local
disk. Push it with your configuration management or fetch it from S3 in userdata and on a
schedule.

Hot reload works the same way it does on a shared volume. The proxy re-reads the file once its
cached copy is older than `cache.bucket_settings_staleness_threshold` (60s by default), so an
updated file takes effect within that window with no restart. A file that fails to parse leaves
the last valid ruleset in place and reports the failure through `/metrics`. Write it before
starting the proxy so the first requests resolve against the rules you intend.

### A worked configuration

Only the fields this pattern changes. Everything else follows
[CONFIGURATION.md](CONFIGURATION.md).

```yaml
cache:
  cache_dir: /mnt/nvme/cache
  max_cache_size: 3543348019200        # 3,300 GiB, taken from df
  eviction_trigger_percent: 95
  eviction_target_percent: 90          # narrowed from 80; ~170 GiB per pass
  ram_cache_enabled: true              # defaults to false
  max_ram_cache_size: 2147483648       # 2 GiB, against 32 GiB of instance memory
  write_cache_percent: 10.0
  # cache.shared_storage is deliberately absent: every default is correct here.
```

### Verifying a deployment

```bash
# One filesystem, the capacity you expect, and the cache directory on it
df -B1 /mnt/nvme && ls -ld /mnt/nvme/cache

# The proxy agrees about its limit, and the cache is filling
curl -s localhost:9090/metrics | python3 -c \
  'import sys,json; m=json.load(sys.stdin)["cache"]; print(m["max_cache_size_limit"], m["total_cache_size"])'

# Nothing is being refused for want of space (0 when healthy; the key only
# appears once a refusal has happened, so a missing key is not an error)
curl -s localhost:9090/metrics | python3 -c \
  'import sys,json; print(json.load(sys.stdin).get("signed_put",{}).get("skipped_puts_total",{}).get("disk_safety",0))'

# All components healthy
curl -s localhost:8080/health
```

`max_cache_size_limit` reading `0` means no limit is configured. A non-zero and rising
`disk_safety` count means `max_cache_size` is above what the device can hold, so lower it. Parse
`/metrics` with a JSON reader rather than `grep`: the endpoint is pretty-printed, so a pattern like
`'"disk_safety":[0-9]*'` cannot match the space after the colon and reports nothing on a proxy that
is refusing writes.

Read all three cache-size gauges as one instance's slice rather than the fleet's total, which is
what they mean on a shared volume. [What to alarm on](#what-to-alarm-on) covers the dashboard
consequence.

## What to measure on a trial fleet

The cost model above is arithmetic on published rates, and the behavioural claims follow from the
routing and caching designs. These six sharpen both against a real workload, and each names the
signal to read it from. Run them on a trial fleet before committing production traffic; the first
three are the ones that decide whether the economics hold for your read mix.

1. **Deduplication.** Drive your real mix of whole-object and ranged reads, then compare the sum of
   `cache.read_cache_size` across instances against the distinct bytes you requested. The capacity
   case rests on this, and the separate `full`, `tail` and page buckets described in
   [Which requests share an owner](#which-requests-share-an-owner) mean some duplication survives.
2. **Spill multiplier.** Drive one hot page at real concurrency and take the `cache.cache_misses`
   delta per instance, at your chosen `hash-balance-factor` and with the directive absent. Confirm
   in the same pass that a concurrent multipart upload still produces a cached object.
3. **Replacement cost.** Replace one instance and compare the `cache.cache_misses` and
   `cache.bytes_served_from_cache` deltas against the ceiling above. The bucket's own CloudWatch
   `BytesDownloaded` is the independent check, since it counts what S3 actually served.
4. **Invalidation window.** Overwrite through the whole-object owner, read a page from another
   owner, and confirm staleness clears within `get_ttl`.
5. **Multipart affinity.** Upload a large multipart object at your client's default concurrency and
   confirm a `.meta` appears for the key, which is what proves every part and the Complete reached
   one instance.
6. **Local read throughput.** Read a warm cache through the proxy and watch `DiskReadBytes` in the
   `AWS/EC2` namespace, which needs no agent. That puts a figure on the throughput gain above.

## See Also

- [REQUEST_AWARE_ROUTING.md](REQUEST_AWARE_ROUTING.md) — the router this pattern requires
- [AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md) — the shared-volume deployment and the default
- [SHARED_STORAGE.md](SHARED_STORAGE.md) — mount requirements and coordination on a shared volume
- [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md) — the staleness contract that bounds invalidation
- [EVICTION.md](EVICTION.md) — thresholds and algorithm, unchanged by this pattern
