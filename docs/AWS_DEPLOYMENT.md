# Deploying on AWS for distant origins

This guide covers running Hybrid Cache for Amazon S3 on AWS when the origin is distant: an S3 bucket in another Region, or an S3-compatible store outside AWS. It explains how to choose and size the cache volume and the EC2 fleet, route clients, configure the origin, and verify the deployment. For installation see [Getting started](GETTING_STARTED.md), for every field see the [configuration reference](CONFIGURATION.md), and for measured throughput see [Performance](../README.md#performance).

This guide puts the fleet on one cache volume. Where every client reaches the proxies through an [affinity router](REQUEST_AWARE_ROUTING.md), a [local NVMe cache fleet](LOCAL_NVME_CACHE.md) is the alternative. Each proxy caches to its own instance store, which costs about 39% less for the same capacity, but the cache does not survive instance replacement. Read that comparison before provisioning a file system, because moving between the two designs later means re-warming the cache.

- [When this guide applies](#when-this-guide-applies)
- [Shared cache volume](#shared-cache-volume)
- [EC2 fleet](#ec2-fleet)
- [Client routing](#client-routing)
- [Origin configuration](#origin-configuration)
- [Mount and configuration](#mount-and-configuration)
- [Verification](#verification)
- [Monitoring](#monitoring)

## When this guide applies

The origin is in a different Region from your compute, or outside AWS. Every origin fetch pays a long round trip and either an inter-Region transfer charge per GB or a share of a fixed-capacity link. A cache hit pays neither, so the hit ratio and the read rate from the cache volume decide the result. Size for re-reads: a miss is slower than the same request made directly to the origin, because it adds the client's hop to the proxy, compression, and the write to the cache volume.

```text
        Region (compute)                             Origin
 ┌──────────────────────────────────┐
 │  clients (AZ-a)    clients (AZ-b)│
 │       │                  │       │
 │  proxy (AZ-a)      proxy (AZ-b)  │
 │       │                  │       │
 │       └────────┬─────────┘       │            ┌─────────────────┐
 │                │  misses only   ─┼───────────▶│ S3 (other       │
 │   ┌────────────┴──────────────┐  │ high RTT   │ region) or      │
 │   │ FSx for OpenZFS           │  │            │ external store  │
 │   │ Multi-AZ or EFS Regional  │  │            └─────────────────┘
 │   └───────────────────────────┘  │
 └──────────────────────────────────┘
```

## Shared cache volume

Use FSx for OpenZFS with an HA deployment type: `SINGLE_AZ_HA_2` if clients are concentrated in one AZ, `MULTI_AZ_1` if they span AZs. Place proxies in the same AZ as the clients they serve. [EFS](#the-efs-alternative) is an alternative at a low duty cycle, and a [local NVMe cache fleet](LOCAL_NVME_CACHE.md) replaces the cache volume where an affinity router fronts the fleet.

Every proxy in the fleet depends on the volume, so the volume's availability is the fleet's availability. HA pairs [fail over in under 60 seconds](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/availability-durability.html), whereas a non-HA file system takes roughly 30 minutes to recover. On an HA file system, a throughput change or a maintenance window is a failover rather than an outage.

Both HA types are priced per provisioned unit ([FSx for OpenZFS pricing](https://aws.amazon.com/fsx/openzfs/pricing/), us-east-1, as of September 2026):

| Per month | `SINGLE_AZ_HA_2` | `MULTI_AZ_1` |
|---|---|---|
| Throughput | $0.52 /MBps | $0.87 /MBps (1.7×) |
| SSD storage | $0.09 /GB | $0.18 /GB (2×) |
| Provisioned SSD IOPS | $0.012 | $0.024 (2×) |

Multi-AZ synchronously replicates storage across AZs, which is why its SSD storage price is double.

> **Note**: `SINGLE_AZ_HA_2` is not offered in every Region; check [Availability by AWS Region](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/available-aws-regions.html). Where it is absent, prefer `MULTI_AZ_1` over `SINGLE_AZ_HA_1`, which caps at 4,096 MB/s.

**Multi-AZ clients**: `MULTI_AZ_1` runs a preferred and a standby file server in two AZs. One is active at a time, behind a floating IP address that follows it across a failover. There is [no data transfer charge from any AZ in the Region](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/access-within-aws.html), but only proxies in the active server's AZ read locally. The others cross AZs on every disk cache read, which adds latency, and a failover changes which AZ is local. AWS recommends placing clients in the preferred file server's AZ.

Place a group of proxies in each AZ alongside the clients it serves; all proxies share one cache namespace. With a Single-AZ file system, proxies outside its AZ also pay a per-GB transfer charge on every request that is not a RAM cache hit.

**Single-AZ clients**: `SINGLE_AZ_HA_2` costs less and has lower latency when all compute is in one AZ. Place the file system in the same AZ as the proxies.

Encryption at rest comes from the cache volume: enable EFS or FSx encryption at rest, EBS volume encryption, or file-level encryption such as dm-crypt.

### Sizing the file system

Throughput capacity sets the file server's memory and read throughput, and drives almost all of the cost:

| Provisioned throughput | Memory | Cached-read (baseline / burst) | Throughput $/month, `SINGLE_AZ_HA_2` |
|---|---|---|---|
| 160 MB/s | 8 GB | 375 / 3,125 MB/s | $83 |
| 640 MB/s | 32 GB | 1,550 / 5,000 MB/s | $333 |
| 1,280 MB/s | 64 GB | 3,125 / 6,250 MB/s | $666 |
| 2,560 MB/s | 128 GB | 6,250 MB/s | $1,331 |
| 3,840 MB/s | 192 GB | 9,375 MB/s | $1,997 |
| 10,240 MB/s | 512 GB | 21,000 MB/s | $5,325 |

Storage adds $0.09 per GB-month on a 64 GiB minimum. Rates are [us-east-1 list pricing](https://aws.amazon.com/fsx/openzfs/pricing/) as of September 2026. See [Compute cost](#compute-cost) for the instances and the all-in minimum.

Size the read side against the cached-read column. Reads served from the file server's memory run at roughly 2.4× the provisioned rate (2.05× at the top tier), which is the cached-read baseline divided by the provisioned throughput in the table. The provisioned figure is the disk ceiling, while cached reads are bound only by the network path. A cache is mostly re-reads, so this column applies, but only while the working set fits in the memory shown. Beyond that, reads fall back to the disk rate.

Throughput capacity also provisions the file server's CPU, which bounds metadata operation rates as well as bandwidth. At the 160 MB/s floor tier, parallel unlink and file creation level off well below what the cached-read column implies. If the cache churns many small ranges, size a tier above what read throughput alone would justify. [Sizing the metadata rate](#sizing-the-metadata-rate) covers the fleet-wide metadata ceiling, which on FSx for OpenZFS falls as proxies are added.

Pick the smallest tier whose baseline cached-read throughput clears the fleet's aggregate cache-hit read throughput with headroom. Count the bytes the proxy reads off the volume, which are compressed: client-delivered throughput divided by your compression ratio. Assume a ratio of 1 unless you know the content compresses; cached media, Parquet files, and archives largely do not. The low tiers burst above baseline, which suits a spiky cache, but burst credits accrue only while you are under baseline, so do not size sustained load against them. Start small, because throughput capacity can be changed later.

Set storage capacity above `cache.max_cache_size`, leaving room for journals, locks, size-tracking state, and in-flight `.tmp` writes. SSD capacity can be increased but not reduced.

Leave IOPS at the included 3 IOPS per GiB unless the volume is small. The proxy's demand is bounded by its design: large-block range I/O, one journal consolidation per cycle, and `cache.metadata_io_concurrency`, which limits the capped metadata reads to 32 concurrent operations per disk cache handle; the lookup on a RAM metadata-cache miss is outside that limit. On a multi-TB volume the included IOPS are ample. Below roughly 1 TB, 3 IOPS per GiB falls short, so provision IOPS explicitly.

Leave compression to the proxy, which LZ4-compresses range data before writing it. Compressing before the data crosses the network shrinks both the cache and the NFS traffic, which file-system compression cannot do. LZ4 on the volume is harmless, but leave it out of sizing.

#### Writes bind before reads do, and a slow volume degrades differently than a failed one

The cached-read column sizes the hit path. Miss fills are volume writes, which run at the provisioned throughput rate; the ~2.4× cached-read multiplier applies only to reads served from the file server's memory. At a typical hit rate the write share of traffic is smaller than the read share, but its ceiling is also lower, so writes bind first as the hit rate falls. On the 3× `c8gn.large` starting fleet from [Sizing the network](#sizing-the-network), at a 50% hit rate the fleet delivers 894 MiB/s to clients, with 469 MB/s of reads and 469 MB/s of writes reaching the volume. The reads sit well inside the 640 MB/s tier's 1,550 MB/s cached-read baseline, but the writes leave only 1.36× headroom against the tier's 640 MB/s provisioned rate. All-miss traffic on this fleet would write 781 MB/s and exceed the tier. Size the write side against the provisioned rate, at the lowest hit rate you expect.

An undersized volume slows requests rather than rejecting them, and a slow volume behaves in the opposite way to a failed one. A hard write failure (ENOSPC, EIO, a panicking writer) drops the cache-write channel: the proxy passes the remaining response frames through to the client, so the request completes correctly and is not cached. A slow volume fails nothing. The cache-write channel applies backpressure through the response stream instead of dropping frames. The client's download slows for as long as the volume stays behind, with no error and no sign of a cache miss in the response. `connection_pool.upstream_idle_timeout` does not cut this short, because it excludes time spent waiting on cache-write backpressure ([Upstream timeout](CONFIGURATION.md#upstream-timeout-stalled-response-fast-fail)).

The `tee_backpressure_events_total_get` and `tee_backpressure_wait_ms_total_get` counters on `/metrics` ([Cache-tee backpressure](METRICS_REFERENCE.md#cache-tee-backpressure)) show it. A rising `tee_backpressure_wait_ms_total_get` with slow downloads and a normal hit rate points at the volume's write throughput rather than the client, the network, or S3. The `_put` pair is the same signal for slow uploads, and [Failure modes](SHARED_STORAGE.md#failure-modes) has the troubleshooting entry.

### The EFS alternative

EFS can hold the cache volume, but FSx answers each operation faster. EFS scales bulk operations wider, costs less at a low duty cycle, grows capacity elastically, and puts a mount target in every AZ.

| | Read latency | Write latency |
|---|---|---|
| FSx for OpenZFS (cached data) | a few hundred µs | a few hundred µs (`sync` export, the default and recommended) |
| FSx for OpenZFS Multi-AZ (cached data) | a few hundred µs | 1–2 ms |
| EFS Regional, Elastic Throughput | ~1 ms | ~2.7 ms |
| EFS One Zone, Elastic Throughput | ~1 ms | ~1.6 ms |

Per-operation latency matters more here than headline throughput. Each cache hit reads `.meta`, may take a lock, then reads range data, so per-operation cost lands on the critical path before any bytes flow. The gap is widest for small objects and shrinks on large sequential reads.

For a fleet that spans AZs, EFS Regional has [a mount target in each AZ](https://docs.aws.amazon.com/efs/latest/ug/how-it-works.html) and each proxy mounts its own, so every proxy reads locally with no cross-AZ charge. FSx Multi-AZ keeps only one file server active, so here EFS has the advantage. EFS One Zone has a single mount target and charges for data access from other AZs.

On EFS, use Elastic Throughput and mount with the EFS client (`amazon-efs-utils` 2.0 or later, or the EFS CSI driver). Both are required for the [1,500 MiB/s per-client cap](https://docs.aws.amazon.com/efs/latest/ug/performance.html); any other combination is limited to 500 MiB/s, and each proxy counts as one client. EFS mounts take no `nconnect` option, because the EFS client opens its own parallel connections; [Mount lines](SHARED_STORAGE.md#mount-lines) gives the options for each storage type. `amazon-efs-utils` 2.0 replaced `stunnel` with `efs-proxy`, so re-mount any volume mounted with a 1.x release of the package.

Choose the storage type from the workload:

- Choose FSx when cache-hit latency, lock-heavy maintenance, or a predictable bill matters most, or when the cache churns many small ranges.
- Choose EFS Regional when proxies span AZs, demand is elastic, or the workload stays under the break-even in [Cost comparison](#cost-comparison).
- Size either storage type from the traffic the proxy sends to the cache volume, after accounting for compression and the two NIC crossings on a cache-volume hit.

#### Cost comparison

FSx charges a fixed rate for provisioned capacity. EFS charges per GB transferred: [$0.03 per GB read and $0.06 per GB written](https://aws.amazon.com/efs/pricing/) with Elastic Throughput in us-east-1 as of September 2026. EFS storage costs $0.30 per GB-month for Regional or $0.16 per GB-month for One Zone. A cache miss writes to the volume, so the effective EFS transfer rate depends on the hit rate.

**Illustrative starting fleet**: three `c8gn.large` proxies, which the [network model](#sizing-the-network) projects at roughly 1,118 MiB/s (1,172 MB/s) of sustained cache-volume hits. The 640 MB/s tier covers that with its 1,550 MB/s cached-read baseline, leaving 1.3× headroom. AWS documents the same performance levels for [Multi-AZ and Single-AZ 2](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/performance.html).

Size against baseline rather than burst. EC2 network burst and FSx burst are both credit-based, so matching one against the other sizes for a rate neither side can hold.

EFS Elastic Throughput has no matching provisioned tier. On sustained load, one proxy's share is ~373 MiB/s, well inside the [1,500 MiB/s per-client cap](https://docs.aws.amazon.com/efs/latest/ug/performance.html). At full network burst that share reaches ~1,788 MiB/s, so on this fleet the EFS cap sets the burst ceiling. On 3× `c6in.large`, the NIC binds first, at ~1,490 MiB/s per proxy. The break-even tables below compare cost only.

The storage model assumes 500 GB of compressed payload as the proxy accounts it, a 0.5 MB average range, and one `.meta` per range: 1,000,000 pairs. On EFS, a 500,000-byte `.bin` rounds up to 503,808 bytes and carries a 2 KiB inode; a small `.meta` carries 2 KiB of metadata plus a 4 KiB allocation. Each pair meters at 512,000 bytes, so the cached payload occupies 512 GB of EFS metered storage before directories and lock files. Recalculate for your range size and ranges-per-object ratio.

**Multi-AZ**: FSx Multi-AZ against EFS Regional. FSx: $90 storage + $557 throughput = $647/month. EFS: $153.60/month storage, plus transfer.

| Cache hit rate | EFS transfer per GB served | Break-even |
|---|---:|---:|
| 100% | $0.030 | ~16.4 TB/month served |
| 50% | $0.045 | ~11.0 TB/month |
| 0% | $0.060 | ~8.2 TB/month |

**Single-AZ**: FSx Single-AZ HA against EFS One Zone. FSx: $45 storage + $333 throughput = $378/month. EFS: $81.92/month storage, plus transfer.

| Cache hit rate | EFS transfer per GB served | Break-even |
|---|---:|---:|
| 100% | $0.030 | ~9.9 TB/month served |
| 50% | $0.045 | ~6.6 TB/month |
| 0% | $0.060 | ~4.9 TB/month |

EFS is least competitive when the cache is cold or churning. The 512 GB storage model adds 12 GB (2.4%) for the `.bin` and `.meta` objects at a 0.5 MB average range. It excludes directory and lock-file allocation, so treat it as a floor. Elastic Throughput also meters a minimum of 32 KiB per data operation and 4 KiB per metadata operation. These are transfer charges rather than stored capacity: at 0.5 MB per range the data-operation minimum does not change the per-GB transfer figures above, but metadata operations still add cost.

Choose EFS for spiky or low-duty-cycle workloads below the break-even, or for elastic capacity with no tier to size. FSx bills a flat rate for provisioned capacity, so its monthly cost does not depend on traffic, while the EFS bill tracks bytes served. One option is to keep the cache on FSx and the configuration and logs on EFS.

## EC2 fleet

### Instance sizing

Add proxies to raise aggregate throughput. A larger instance mainly adds concurrent connections, because the bottleneck is per-connection processing rather than total CPU. Choose the proxy count from network bandwidth, then check it against the cache volume's [metadata rate](#sizing-the-metadata-rate) and the [staged range limit](#the-staged-range-limit). Adding proxies does not raise the limit, and on some storage types it lowers the metadata rate.

**Recommended starting point**: 3× `c8gn.large` (2 vCPU, 4 GiB, 6.25 Gbps sustained, 30 Gbps burst), a network-optimized Graviton instance.

Use a network-optimized Graviton instance. The `n` in `c8gn` and `m8gn` marks the network-optimized variant; the general-purpose `c8g` and `m8g` sustain far less bandwidth (see below). Each recommended instance has the same vCPU and memory as the x86 network-optimized instance it replaces, twice the sustained bandwidth, and costs about 4.5% more:

| Use | vCPU / RAM | Sustained | $/hour | Replaces (x86) |
|---|---|---|---|---|
| `c8gn.large` | 2 / 4 GiB | 6.25 Gbps | $0.1185 | `c6in.large`: 3.125 Gbps, $0.1134 |
| `m8gn.large` | 2 / 8 GiB | 6.25 Gbps | $0.1455 | `m6in.large`: 3.125 Gbps, $0.1392 |

Prefer `c8gn` over `c7gn`: the specification is the same at every size, and `c8gn` costs roughly 5% less. The proxy has no architecture-specific code, so Graviton needs no other change: build for aarch64 on a Graviton host, and run one architecture across the fleet ([Binary portability](GETTING_STARTED.md#binary-portability)).

`m8g` without the `n` is general-purpose: `m8g.large` sustains 0.937 Gbps against `m8gn.large`'s 6.25 Gbps. Take the figure from [`describe-instance-types`](https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instance-types.html) (`NetworkInfo.NetworkCards[].BaselineBandwidthInGbps`) rather than from the family name or the generation number.

#### Sizing the network

`c8gn.large` and `m8gn.large` sustain 6.25 Gbps (≈745 MiB/s) indefinitely; the older `c6in.large` and `m6in.large` sustain 3.125 Gbps (≈373 MiB/s). The "up to 30 Gbps" in the specification is credit-based burst: credits accrue below baseline and drain above it.

Burst suits a cache, whose traffic is usually spiky: a working set is pulled in and read hard for a few minutes, then the fleet idles and rebuilds credit. Sizing every proxy for its own peak means paying continuously for capacity you use in bursts. If your peaks are short relative to the gaps between them, count the burst figure in your capacity plan.

Size against baseline in two cases:

- **Sustained load.** A run above baseline that lasts long enough to exhaust credits drops to baseline mid-flight, which looks like an unexplained slowdown. Bulk ingest, long analytics scans, and overnight migrations all qualify.
- **Benchmarking.** A benchmark is sustained by design, so one run on burst credit measures capacity the instance cannot hold. Its rate starts high, drops as credits drain, and depends on how idle the instance was beforehand, so the result is not reproducible.

[Sizing the file system](#sizing-the-file-system) makes the same point about burst credits for FSx throughput tiers.

Whichever figure you size against, the proxy's NIC carries each byte more than once:

| Operation | NIC crossings per byte | Client-facing share of baseline |
|---|---|---|
| Cache hit from the cache volume | 2 (read the volume, write the client) | ~50% |
| Cache hit from the RAM cache | 1 (write the client) | ~100% |
| Cache miss | 3 (fetch from the origin, write the client, write the volume) | ~33% |
| Write (PUT or `UploadPart`) | ~3 (ingest, forward to the origin, write the body to the volume) | ~33% |

The volume write counts because EFS and FSx are reached over the network. With a [local NVMe](LOCAL_NVME_CACHE.md) disk cache, that write goes over PCIe instead, so a miss costs 2 crossings (~50%) and a disk cache hit costs 1 (~100%).

A 3× `c8gn.large` fleet has ~2,235 MiB/s of aggregate baseline (6.25 Gbps ÷ 8 = 781 MB/s = 745 MiB/s each). That is about 1,118 MiB/s of client-facing reads on cache-volume hits, or 745 MiB/s of sustained writes. Divide your required client-facing throughput by the relevant share above rather than comparing it with the raw baseline. Writes are the expensive direction. On 3× `c6in.large`, halve both figures, since its baseline is half.

The multiplier applies to burst too. At 30 Gbps peak each, the same fleet has ~10,700 MiB/s of aggregate burst, so roughly 5,360 MiB/s of client-facing read headroom while credits last. A RAM cache hit crosses the NIC once, so raising `cache.max_ram_cache_size` adds network capacity as well as cutting latency.

#### Sizing the metadata rate

After setting the proxy count from the network, check the cache volume's metadata rate at that count. The volume's aggregate rate for `.meta` reads depends on the storage type and on how many proxies share it, and on some storage types adding proxies lowers it. [Sizing the metadata rate](SHARED_STORAGE.md#sizing-the-metadata-rate) explains why it matters and how to measure it on any shared volume.

Measured `.meta` read rates, on an idle volume with ~2.4 KB metadata files:

| Storage type | One proxy | Fleet aggregate |
|---|---|---|
| EFS, Elastic Throughput, EFS client | 2,900-3,800 obj/s | not measured above 3 proxies |
| FSx for OpenZFS `SINGLE_AZ_HA_2`, 10,240 MB/s, `nconnect=16` | 29,000-31,000 obj/s | peaks at ~84,700 obj/s with 3 proxies, then falls with every proxy added: 73,900 at 7, 73,800 at 12, 70,500 at 24, 68,800 at 36 |
| FSx for NetApp ONTAP, 6 HA pairs | 28,500 obj/s at 32 reads outstanding, up to 45,000 at 64 | rises with every proxy added, up to 24 proxies, the largest fleet measured |

On FSx for OpenZFS between 7 and 36 proxies, the aggregate `A(I)` for `I` proxies follows `A(I) ≈ 85,252 − 4,596 × ln(I)` obj/s. Do not extend it past 36. The ONTAP figures come from a fleet whose proxies were spread unevenly across the file system's network interfaces, so use them for the trend only and measure before planning against them.

**The storage type is an input with no default.** At 24 proxies the two FSx storage types differ by more than 2×, and they move in opposite directions as the fleet grows. Measure a storage type that is not in the table, or a different throughput tier or HA configuration of one that is, before relying on it. [Sizing the metadata rate](SHARED_STORAGE.md#sizing-the-metadata-rate) gives the method: the `s3-proxy bench-metadata` command, how to measure a fleet aggregate, and the rule `h ≥ 1 − A(I) / R`.

**Every figure is a ceiling.** The measurements ran with no serving traffic on the volume, and on FSx for OpenZFS more than 99.9% of the reads came from the file server's memory. A busy volume, or a metadata working set larger than the file server's memory, delivers less.

**Worked example.** Applying the [rule](SHARED_STORAGE.md#sizing-the-metadata-rate) to a fleet serving 100,000 GET and HEAD requests per second on FSx for OpenZFS:

| Proxies | `A(I)` | Required `h` |
|---|---|---|
| 16 | 72,509 obj/s | ≥ 0.27 |
| 32 | 69,324 obj/s | ≥ 0.31 |

At least 27-31% of metadata reads must come from RAM, and doubling the fleet raises the requirement. On FSx for OpenZFS, adding proxies cannot relieve a metadata shortfall. A higher RAM metadata cache hit ratio, a higher throughput tier, or a storage type that scales out, such as FSx for NetApp ONTAP, can.

Check `h` on the running fleet as [Sizing the metadata rate](SHARED_STORAGE.md#sizing-the-metadata-rate) describes.

#### The staged range limit

Uploads the write cache holds that no client has read yet are staged objects; the first read of one moves it to the read cache. Each staged object is stored as one or more ranges, and the whole fleet holds at most 32,800,000 of these staged ranges. The limit is fixed in the binary, has no configuration field, and does not grow with the proxy count. `/metrics` publishes it as `write_cache.staged_range_budget`; compare it with `write_cache.staged_range_count`. At the limit, uploads still go to S3 and succeed, but they are not cached until the count falls.

The limit is a count, so the bytes it covers depend on your mean staged range size: about 3.9 TiB at 128 KiB per range, and about 250 TiB at 8 MiB. A workload of small uploads can reach it while the `cache.write_cache_percent` byte bound is far from full. If your write-cache bytes divided by your mean range size exceed 32,800,000, expect the write cache to stop caching new uploads at that point.

The limit lets the proxies re-read every staged range's metadata within 24 hours, the target age of the write-cache size figure, which `/metrics` publishes as `write_cache.staged_authority_max_age_secs`. Each proxy can revisit at most 147,456 cache shards (576 buckets) in that window, so more buckets than that per proxy needs more proxies. [Limits with no configuration field](CONFIGURATION.md#limits-with-no-configuration-field) lists the fixed limits, and [The staged range budget](METRICS_REFERENCE.md#the-staged-range-budget) describes the signals.

#### Sizing memory

A proxy's memory has four parts:

- **Streaming paths.** `server.max_concurrent_requests` bounds each request for its whole life, transfer included, so `~200 MiB + max_concurrent_requests × ~5 MiB` is a per-proxy ceiling for the streaming paths ([Memory impact](CONFIGURATION.md#memory-impact)). Upload paths and cache-miss GETs stream rather than buffering whole objects, so this ceiling does not grow with object size.
- **Write-cache backlog.** A PUT's response returns before its background cache store finishes, and up to 1,024 stores can be pending per proxy. Each holds up to min(object size, `cache.compression_batch_size`) of buffer and one open file. The backlog is therefore at most 1,024 × min(object size, `cache.compression_batch_size`) of buffer and 1,024 file descriptors ([Write-through ordering](CACHING.md#write-through-ordering)). That is 1 GiB of buffer at the 1 MiB default, and 16 GiB at the 16 MiB maximum.
- **Buffered read paths.** Range merge, page widening, buffered range serving, and the recovery fallbacks buffer a whole range. `server.max_inflight_buffer_bytes` bounds them in aggregate and is off by default ([In-flight memory ceiling](CONFIGURATION.md#in-flight-memory-ceiling)). On a 4 GiB `c8gn.large` with the default 512 MiB RAM cache, 512 MiB (`536870912`) is a reasonable starting value.
- **RAM cache.** `cache.max_ram_cache_size` (default 512 MiB) is a separate budget on top of the others.

Lower `server.max_concurrent_requests` on a small instance. Its default of 1000 puts the streaming paths at ~5.1 GiB, which exceeds a 4 GiB instance before the RAM cache and the in-flight ceiling are counted. On `c8gn.large` with both at 512 MiB, the remaining streaming budget is ~2.8 GiB, so 500 is a sound starting value; scale it with memory. Under representative load, confirm these budgets against measured peak resident memory and the `/metrics` fields `request_metrics.permits_held_peak` and `inflight_memory.peak_reserved_bytes`.

#### Compute cost

`c8gn.large` is [$0.1185/hour on-demand](https://aws.amazon.com/ec2/pricing/on-demand/) (identical in us-east-1 and us-west-2 as of September 2026), about $87/month per instance. A single proxy with FSx at 160 MB/s (64 GiB) costs roughly $175/month on-demand. For redundancy, start with three instances: $348/month all-in on the 160 MB/s tier, or about $598/month on the 640 MB/s tier that carries their sustained hit throughput ([Cost comparison](#cost-comparison)). [Compute Savings Plans](https://aws.amazon.com/savingsplans/compute-pricing/) reduce the `c8gn.large` rate by about 27% on a 1-year, no-upfront term ($0.0859/hour) and about 58% on a 3-year, no-upfront term ($0.0496/hour), us-east-1 as of September 2026. Savings Plans do not cover FSx throughput and storage charges.

### Bootstrap

Get the binary once, by downloading a release asset or building it on CI or a build host, and distribute it; keep the Rust toolchain off the proxies. Compiling on each instance puts a compiler and crates.io egress on production hosts, lengthens boot, and lets proxies drift onto binaries built from different source states. Observe the architecture and glibc constraints in [Binary portability](GETTING_STARTED.md#binary-portability), then choose a distribution route:

| Approach | Trade-off |
|---|---|
| Download the release asset | Skips the build. Take `s3-proxy-x86_64-linux` or `s3-proxy-aarch64-linux` and `SHA256SUMS` from the [GitHub release](https://github.com/aws-samples/sample-s3-hybrid-cache/releases/latest), check them with `sha256sum -c --ignore-missing SHA256SUMS`, then use any route below. Needs glibc 2.34 or newer, so Amazon Linux 2023 and not Amazon Linux 2. |
| Bake into an AMI (EC2 Image Builder) | The binary is in place at boot, the image is immutable, and no fetch permissions are needed. New AMI per upgrade. |
| Publish to S3, fetch in user data | Simple to automate. Needs read access to that prefix in the instance profile. |
| Place on the cache volume | No extra bucket or permissions; the volume is already mounted. |

If the binary lives on the volume, copy it to local disk at boot instead of executing it from the mount, so that process start does not depend on the volume's availability.

User data mounts the volume, places the binary, writes the systemd unit, and enables the service. Write the unit in the same step as the binary: if it is missing, `systemctl enable` fails while the rest of the script succeeds, leaving an instance that looks bootstrapped but runs no proxy. Redirect user data output to its own log (`exec > /tmp/userdata.log 2>&1`) so a failed step is easy to find.

Example scripts: [user data for FSx for OpenZFS](examples/userdata-fsxz.sh) and [user data for EFS with Elastic Throughput](examples/userdata-efs.sh). Both fetch the binary from S3, install the sandboxed systemd unit, and check health on boot.

### Service configuration

Start from the sandboxed [systemd unit](../config/s3-proxy.service) and add your cache and log directories to `ReadWritePaths`; otherwise `ProtectSystem=strict` makes the file system read-only and cache writes fail. Keep its `StateDirectory=s3-proxy` line, which makes `/var/lib/s3-proxy` (the default `logging.local_state_dir`) writable on the instance's own volume. The unit runs as `root` to bind ports 80 and 443; [proxy-only mode](GETTING_STARTED.md#proxy-only-mode) lets it run as an unprivileged user.

Keep `config.yaml` on a volume every proxy mounts, such as the cache volume, and point each proxy at it with `-c`, so one edit applies fleet-wide and proxies cannot drift. `cache_rules.json` lives in `cache_dir` and reloads without a restart.

### Credentials and network access

The proxy holds no AWS credentials: it forwards requests the client has already signed and cannot call S3 on its own behalf, so the instance profile grants nothing for the data path. The instance profile needs `AmazonSSMManagedInstanceCore` for management, `CloudWatchAgentServerPolicy` if you publish metrics to CloudWatch, and read access to the artifact prefix if user data fetches the binary from S3. Manage instances over SSM so no inbound SSH is required.

Open only the ports your client routing uses. With the recommended TLS proxy listener, only port 3129 needs to be reachable from clients, whether they reach it directly or through one of the [encrypted load balancer patterns](#load-balancer-encrypting-the-client-hop). Leaving port 80 open as well adds a cleartext path into the fleet.

| Port | Source |
|---|---|
| 3129 (TLS proxy listener), and 80, 443 or 3128 if used | client CIDRs, or the load balancer's security group |
| 2049 (NFS) | the file system; a self-referencing security group rule is simplest |
| 8080 health, 8081 dashboard, 9090 metrics | operators, monitoring, and load balancer health checks, once the listener is bound off loopback (below) |

The monitoring listeners (health on 8080, metrics on 9090, dashboard on 8081) are unauthenticated and bind `127.0.0.1` by default, so a security group rule alone does not expose them. For a load balancer health check, an application status check, or remote scraping, set `bind_address: "0.0.0.0"` in the `health`, `metrics`, or `dashboard` section, as `config/config.example.yaml` does. Then restrict the port in the security group to the checkers' and operators' sources. Leave the dashboard on loopback and reach it over an SSH or SSM tunnel unless you need it remotely.

## Client routing

Point clients at the TLS proxy listener: set `HTTP_PROXY=https://<proxy-host>:3129` and `--endpoint-url http://s3.<region>.amazonaws.com`. The client hop is encrypted and responses are still cached, on-premises and across Regions. Where the network segment between clients and proxies is secure and cleartext on it is acceptable, a DNS zone that resolves S3's hostnames to the proxies is a simpler alternative. Clients need no proxy setting and connect on port 80.

On AWS, `<proxy-host>` is a name in a Route 53 private hosted zone that resolves to every proxy, or a Network Load Balancer. With an NLB, `HTTP_PROXY` is `https://<nlb-hostname>` on port 443, and the NLB forwards to port 3129. The DNS alternative uses a private hosted zone for S3's own hostnames. [Configure client routing](GETTING_STARTED.md#3-configure-client-routing) covers every mechanism and the client settings. In a multi-AZ deployment, route each client to a proxy in its own AZ to avoid cross-AZ latency on the client hop.

### DNS: Route 53 private hosted zones

A private hosted zone associated with the client VPCs can resolve a proxy name you own, for the recommended pattern, or S3's own hostnames, for the DNS alternative. In either zone, add one multi-value answer `A` record per proxy, each with a distinct set identifier and a short TTL.

- **A proxy name** needs one zone and one record set, which cover every Region and bucket, and does not conflict with an interface endpoint's private DNS. Clients connect to the proxies by that name, so one certificate with the name as a SAN serves every proxy ([Fleet behind one proxy name](GETTING_STARTED.md#fleet-behind-one-proxy-name)).
- **S3's hostnames** need a zone for each hostname the fleet serves: `s3.<region>.amazonaws.com` for each Region whose buckets clients access, plus any access-point hostnames.

The proxy resolves origin hostnames through public resolvers by default, or through `connection_pool.dns_servers` when it is set, and never reads `/etc/hosts`, so neither zone creates a resolution loop ([DNS server configuration](CONFIGURATION.md#dns-server-configuration)). Keep `dns_servers` off any resolver that answers from a zone for S3's hostnames, such as the VPC resolver. Otherwise the proxy sends requests to the fleet instead of S3, and each request passes between proxies until it reaches one it has already visited, which answers `508 Loop Detected` ([Request loops](ERROR_HANDLING.md#9-request-loops-via-and-508-loop-detected)).

**AZ-local preference**: Route 53 multi-value answers do not prefer same-AZ proxies, and a private hosted zone is associated with whole VPCs rather than with subnets or AZs. For AZ affinity with a proxy name, give each AZ its own name that resolves only to that AZ's proxies, and point each AZ's clients at their own name.

A hosts-file entry on each client host is the minimal form of either shape, for a single proxy in a proof of concept. It holds one address, so it gives no rotation, does not adapt to proxy failures, and must be edited by hand on every fleet change. Use a hosted zone or an NLB beyond a proof of concept.

**Resolver rotation spreads the load.** The VPC resolver returns the addresses of a cached record set in rotation, advancing one position per query, so consecutive lookups start on different proxies. `getaddrinfo` on the client inherits that order. A client that reads only the first address therefore spreads across the fleet without needing the CRT. Rotation is a property of the resolver rather than of DNS, so where clients resolve through an on-premises forwarder instead, confirm it there first with the check in [Fleet behind one proxy name](GETTING_STARTED.md#fleet-behind-one-proxy-name).

Rotation does not depend on the routing policy, which the resolver cannot see: a simple-routing record holding several values and a set of multi-value answer records rotate the same way. The routing policy matters for health checking, covered below.

**Use a 10-second TTL.** A short TTL keeps rotation working, because the resolver refills the record and carries on rotating from the new copy, at the cost of refreshing from Route 53 more often. A TTL of 0 removes the cache and the rotation with it. The VPC resolver honors a zero TTL rather than clamping it, so distribution becomes uneven and consecutive lookups can land on the same address. Zero also adds a resolver round trip to many lookups, paid at connection setup.

**A simultaneous start spreads evenly.** Rotation holds under concurrency, because every client's lookup advances the same cached position, so many clients starting at once divide evenly across the proxies rather than converging on one. At TTL 0 they do not, because there is no cached position to advance.

**Budget the resolver quota for a simultaneous start.** Each network interface has a [hard limit of 1,024 packets per second to the Amazon-provided DNS server](https://docs.aws.amazon.com/vpc/latest/userguide/amazon-vpc-limits.html), which cannot be raised. The resolver rejects traffic beyond it. The limit covers all link-local services, so a client's IMDS credential fetch competes with its own DNS lookup. Size for many clients per host starting together; one client per instance has its own interface and its own budget. The TTL does not affect this, since every client lookup sends a packet regardless.

**Failure detection sets how quickly traffic stops reaching a failed proxy.** The total is detection plus TTL plus the client re-resolving. A private fleet cannot use Route 53's own [10 or 30 second endpoint checks](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/dns-failover-determining-health-of-endpoints.html), whose checkers sit outside the VPC. A `CLOUDWATCH_METRIC` check has no interval of its own: it tracks an alarm, so detection is whatever feeds the alarm.

| Detection mechanism | Detection | + TTL 10 |
|---|---|---|
| Application status check (60 s interval, threshold 2) plus alarm | ~2 min | ~2 min |
| EventBridge-scheduled Lambda polling `/health` (1 min minimum) | ~1-2 min | ~1-2 min |
| Route 53 endpoint check, 10 s interval, public endpoints only | ~30 s | ~40 s |

A private fleet therefore takes one to two minutes whichever route it uses, and the TTL is a small share of that.

**Most requests still succeed during that window.** With one dead proxy of three, a re-resolving client has roughly a two-in-three chance of a healthy address, and SDK retries cover much of the rest. The symptom is elevated errors and latency on a fraction of attempts. A hung proxy is worse than one that refuses connections, because clients wait for a timeout instead of failing at once. That is why the health check should read `/health` rather than test a bare TCP connection. A client holding an open connection does not re-resolve at all.

**Withdrawing a failed proxy needs a health-checkable record.** Route 53 does not health check the values in a [simple routing](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-simple.html) record, so use multi-value answer or weighted records. That costs nothing in distribution, even where clients read only the first address. Two constraints follow:

- Route 53's own health checkers run outside the VPC and cannot reach a private address, so a Route 53 HTTP check against `:8080/health` is not available for a private fleet. Drive a `CLOUDWATCH_METRIC` health check from an alarm instead, fed by an [EC2 application status check](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/application-status-checks.html) (below). Where you need an interval shorter than 60 seconds, extend the EventBridge Lambda to poll `/health` and delete records itself.
- Instance and system status checks do not read the proxy's health endpoint, so they stay green while the proxy runs with its cache mount missing, which the [verification](#verification) step catches. An application status check reads the endpoint. You give it a port, path, and expected status code; it reaches instances from a managed interface inside your VPC and publishes `StatusCheckFailed_Application` for an alarm to read. Auto Scaling replaces instances reporting `impaired` with no extra configuration, so a failing health endpoint leads to replacement without a load balancer.

Application status checks work against the proxy's health endpoint unchanged. They are documented as using HTTP/2 but send an HTTP/1.1 request offering `Upgrade: h2c`; the endpoint ignores the offer and answers HTTP/1.1, and the check passes with `ResponseCodeMatched` and an application status of `ok`. The endpoint would reject HTTP/2 with prior knowledge, which the checks do not use. Three things to get right:

- The check reaches the instance from another network interface, so the health listener must be bound off loopback with `health.bind_address: "0.0.0.0"`; with the default `127.0.0.1` every check fails. The instance security group must also allow the check's source security group inbound on the check port, the same rule the NLB target group needs for 8080.
- A restarting proxy fails the check, so a rolling deploy under Auto Scaling can become a replacement storm. Set `Aggregation` to `excluded` while trying out a check, and suppress or disassociate it around a deploy.
- Defaults are a 60-second interval, a failure threshold of 2, and a 300-second startup grace period, so detection takes around two minutes. Billing is $0.01 per managed network interface per hour, per Availability Zone.

**Auto Scaling with DNS**: use EventBridge to react to EC2 instance state changes. Create a rule matching `EC2 Instance State-change Notification` for the states `running` and `terminated`, filtered to the Auto Scaling group's instances by tag. Target it at a Lambda function that looks up the instance's private IP address and AZ, then upserts (on `running`) or deletes (on `terminated`) its multi-value `A` record. With a name per AZ, the record goes in that AZ's record set. No lifecycle hook is needed: the proxy must mount the file system and pass health checks before serving traffic anyway, and the short TTL keeps the propagation window after `running` brief.

**Static fleet**: update the records by hand on every fleet change, keeping each per-AZ name to that AZ's proxies.

### Load balancer: encrypting the client hop

[Which pattern to choose](GETTING_STARTED.md#which-pattern-to-choose) compares three TLS termination patterns for a load balancer. Two of them encrypt the NLB-to-proxy hop, and this section gives the AWS configuration for both. In both, clients set `HTTP_PROXY` to the NLB and keep the S3 endpoint on `http://`, as shown under Pattern 2.

Pattern 1, a `TLS` listener with a `TCP` target group, is not covered here. It decrypts at the NLB and forwards cleartext to the proxy, so choose it only where the segment between the NLB and the proxies is secure and cleartext on it is acceptable. SigV4 authenticates each request but does not encrypt it; see [What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes). Pattern 3 encrypts that segment for one extra target-group setting.

#### Pattern 3: NLB re-encrypts to the proxy (recommended)

Set the target group protocol to `TLS` instead of `TCP`:

```text
Client (HTTPS) → NLB (TLS :443, ACM cert) → Proxy (TLS :3129) → S3 (HTTPS)
```

The NLB terminates the client-facing session with an ACM certificate and opens a second TLS session to the TLS proxy listener. Because [an NLB TLS target group does not validate the target's certificate](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups), the proxy can present a self-signed, long-lived certificate with any SAN, and its rotation is not tied to the client-facing name.

Enable the [TLS proxy listener](GETTING_STARTED.md#tls-proxy-listener-configuration) on each proxy, then configure the NLB:

| Setting | Value |
|---|---|
| Listener | `TLS` on 443, ACM certificate attached |
| Target group | Protocol `TLS`, port 3129 (`tls_proxy_port`) |
| Health check | Protocol `HTTP`, override port `8080`, path `/health`; requires `health.bind_address: "0.0.0.0"` |
| Cross-zone load balancing | Off, which keeps traffic in the client's AZ |
| AZ DNS affinity | 100% (Availability Zone affinity): clients resolve the NLB IP address in their own AZ |
| Idle timeout | 350 s, fixed for a `TLS` listener; clients that idle longer mid-session must reconnect |
| Deregistration delay | Above 300 s if single transfers run longer |

Cross-zone load balancing and AZ DNS affinity together keep traffic in the client's AZ. AZ DNS affinity gives clients that use Route 53 Resolver the NLB address in their own AZ. With cross-zone load balancing off, the NLB does not forward to targets in other AZs, so traffic runs from the client to the same-AZ NLB node and on to a same-AZ proxy. If every target in an AZ becomes unhealthy, the NLB's DNS fails open and routes to a healthy AZ.

Store a self-signed certificate and key on the cache volume so every proxy presents the same one, and `chmod 600` the key. The NLB does not validate it, so any CN or SAN and any expiry works. The NLB health check connects from the load balancer's nodes, so set `health.bind_address: "0.0.0.0"` (with the default `127.0.0.1` every target fails) and allow health-check traffic to port 8080 in the instance security group.

#### Pattern 2: NLB passes TCP through, proxy terminates the client's TLS

Choose this over Pattern 3 when the client must validate the proxy's certificate itself, or when you cannot attach a certificate to the NLB. Pattern 3 protects the internal hop against passive observation but not against an active attacker impersonating a proxy on it, because the NLB does not validate the target certificate. Pattern 2 closes that gap for clients that verify the proxy's certificate, at the cost of real certificate management. Only the AWS CRT transfer client verifies the proxy's certificate; botocore-based clients (`aws s3api`, `aws s3` with the classic transfer client, boto3) encrypt the hop without verifying it.

The proxy reads its certificate from disk, so it needs one you can export. ACM public certificates [can be exported for use on EC2](https://docs.aws.amazon.com/acm/latest/userguide/acm-exportable-certificates.html) if they were requested with export enabled, which excludes any issued before June 17, 2025. An exported certificate gives clients a publicly trusted chain with no private CA to run and no trust to distribute. AWS Private CA is the alternative when a private trust chain is required.

```text
Client (HTTPS) → NLB (TCP :443, passthrough) → Proxy (TLS :3129, terminates) → S3 (HTTPS)
```

The differences from the Pattern 3 configuration above:

| Setting | Value |
|---|---|
| Listener | `TCP` on 443, with no certificate on the NLB |
| Target group | Protocol `TCP`, port 3129 (`tls_proxy_port`) |

Everything else (health check, cross-zone off, AZ DNS affinity, deregistration delay) is unchanged. The idle timeout differs: a `TCP` listener defaults to 350 s and can be raised for clients that idle mid-session. If you raise it, raise the targets' ENI `TcpEstablishedTimeout` to at least the same value.

Certificate management is the cost of Pattern 2, and the reason Pattern 3 is recommended. The client validates the certificate the proxy presents, so its SANs must match the NLB's hostname, which is what clients connect to. Every proxy must present a certificate covering those SANs, and rotation is tied to the client-facing name. Put the certificate and key on the cache volume so every proxy serves the same one, `chmod 600` the key, and plan the rotation.

Clients point `HTTP_PROXY` at the NLB and keep the S3 endpoint on `http://`, so the signed `Host` stays the S3 hostname. Passing the NLB as `--endpoint-url` would make the NLB name the signed `Host`, and the proxy would treat it as the origin. Install the proxy's certificate in the client's system trust store; `AWS_CA_BUNDLE` does not apply to the `HTTP_PROXY` hop. [Configuring clients to trust the certificate](GETTING_STARTED.md#configuring-clients-to-trust-the-certificate) covers each client.

```bash
export HTTP_PROXY=https://<nlb-hostname>
export NO_PROXY=169.254.169.254  # On EC2, keep IMDS off the proxy path
aws s3 cp s3://<bucket>/<key> ./local \
  --endpoint-url http://s3.<region>.amazonaws.com \
  --region <region>
```

The NLB's TCP listener on 443 forwards to target port 3129, so clients use the default HTTPS port.

#### Both encrypted patterns

**Auto Scaling with NLB**: attach the Auto Scaling group directly to the target group. Registration and deregistration are automatic, with no lifecycle hook or Lambda function needed. The deregistration delay drains in-flight connections before the instance is terminated.

**Use an NLB rather than an ALB.** The proxy terminates and interprets HTTP itself, so an ALB repeats that work on every request and inserts its own HTTP behavior between client and proxy. An NLB forwards bytes at Layer 4. For a Layer 7 router that picks a proxy from the request path, see [Request-aware routing](REQUEST_AWARE_ROUTING.md).

### Choosing between DNS and NLB

DNS comes in two shapes, which differ in whether the client hop is encrypted. Clients that set `HTTP_PROXY=https://<proxy-host>:3129` to a [proxy name](GETTING_STARTED.md#fleet-behind-one-proxy-name) reach the TLS proxy listener, so the hop is encrypted and cached. A [DNS zone for S3's hostnames](GETTING_STARTED.md#option-c-dns-zone-for-s3-hostnames-secure-network-segments) needs no client setting, and clients reach the proxy in cleartext on port 80.

| | DNS + proxy name | NLB | DNS on S3 hostnames |
|---|---|---|---|
| Client requirement | Must set `HTTP_PROXY` | Must set `HTTP_PROXY` to the NLB for an encrypted hop | Any client; the CRT also retries a second address |
| Client → proxy encryption | Encrypted and cached, via the TLS proxy listener on 3129 | Encrypted and cached: Pattern 3 or Pattern 2 | Cleartext on 80; HTTPS on 443 passes through uncached |
| Distribution | Per lookup, by resolver rotation | Per connection | Per lookup, by resolver rotation |
| Health checking | Application status check → CloudWatch alarm → Route 53 | Target group, built in | As for a proxy name |
| Failover | Detection + TTL + client re-resolve | Detection alone | As for a proxy name |
| AZ affinity | A name per AZ (manual) | Built in, with cross-zone off | None within a VPC |
| Auto Scaling | EventBridge + Lambda for records; Auto Scaling replaces on application status | Attach the group to a target group | As for a proxy name |
| Cost | < $1/month, plus $0.01/hr per managed network interface | ~$450/month at 100 GB/hr; ~$2,200/month at 500 GB/hr: $0.0225/hour plus $0.006 per LCU-hour, one LCU per GB/hour processed ([pricing](https://aws.amazon.com/elasticloadbalancing/pricing/), us-east-1, as of September 2026) | As for a proxy name |

Use a proxy name where clients can set `HTTP_PROXY`. It gives an encrypted and cached client hop, even distribution, and server-side health checking at DNS prices, two to three orders of magnitude below the NLB's per-GB charge.

Choose the NLB for per-connection balance, failover that does not wait on a TTL, built-in AZ affinity, or one stable endpoint.

A DNS zone for S3's hostnames is the simpler alternative where the network segment between clients and proxies is secure and cleartext on it is acceptable: clients need no proxy setting. SigV4 authenticates each request but does not encrypt it; see [What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes). For clients inside a VPC, [VPC Encryption Controls](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-encryption-controls.html) in monitor or enforce mode encrypt traffic between instances in Nitro hardware, even when the application layer is cleartext. They do not cover clients outside the VPC, such as on-premises clients.

**A self-managed option.** HAProxy in front of the fleet encrypts the client hop while caching, and adds routing by object key and byte range, so concurrent readers of the same bytes converge on one proxy. Run on a client host, it also keeps a load balancer out of the data path and avoids the per-GB charge above, but you operate it yourself. [Request-aware routing](REQUEST_AWARE_ROUTING.md) describes it, including why putting it behind an NLB on AWS brings the charge back.

## Origin configuration

### Cross-Region S3

**Network path to the origin Region.** An S3 gateway endpoint reaches only its own Region's S3, so it cannot carry misses to a bucket in another Region, and you have to choose the egress route. Behind a cache, only misses leave the Region, so per-GB charges scale with the miss rate while hourly charges are paid in full. At a high hit ratio the fixed costs dominate and the per-GB differences between patterns shrink.

**Recommended: cross-Region PrivateLink.** Create an S3 interface endpoint in the proxies' VPC targeting the bucket's Region, so misses leave through PrivateLink with no VPC peering, Transit Gateway attachment, or internet gateway in the path. The endpoint reaches S3 and nothing else, an endpoint policy can restrict the buckets, and adding another origin Region means adding an endpoint rather than reworking routing. Three requirements fall on the proxy side:

- **Give the proxy the endpoint's ENI IP addresses in `connection_pool.endpoint_overrides`.** By default the proxy resolves origin hostnames through the Google and Cloudflare public resolvers, which return public S3 addresses and bypass the endpoint. Map the S3 hostnames to the endpoint's ENI IP addresses instead, which takes DNS out of the path ([S3 PrivateLink](CONFIGURATION.md#s3-privatelink-interface-vpc-endpoints)). The proxy rewrites only the URI authority and keeps the original hostname in TLS SNI and the `Host` header, so certificate validation and the client's SigV4 signature are unaffected. Outbound TLS to an overridden hostname is pinned to 1.2, which interface endpoints require; other hosts keep TLS 1.2 or 1.3. ENI IP addresses are stable for the endpoint's lifetime. Adding a subnet later means editing `config.yaml` and restarting, because the proxy reads it at startup.

- **Do not enable private DNS on the endpoint.** Private DNS works by creating a private hosted zone for `s3.<region>.amazonaws.com`. That zone collides with a zone for S3's hostnames if you use the DNS alternative for [client routing](#client-routing). Where such a zone already exists, AWS rejects the endpoint with a conflicting-domain error. The proxy does not need private DNS, because `endpoint_overrides` takes DNS out of its path. With a zone for S3's hostnames, pointing `dns_servers` at the VPC resolver or a Route 53 Resolver inbound endpoint does not work either. Both answer from that zone and hand the proxy back its own address.

- **Monitor for excluded IP addresses.** An ENI IP address that health tracking excludes, from an exact or a suffix (`*.`) entry, returns only when a recovery probe succeeds after its cooldown. The cooldown starts at `connection_pool.health_probe_initial_cooldown` and backs off to `connection_pool.health_probe_max_cooldown`. Each virtual-hosted bucket hostname matched by a suffix entry gets its own set of the entry's IP addresses on its first request, so it round-robins and fails over like an exact entry. With only two or three ENIs, a transient fault can exclude all of them. The proxy then connects to the entry's first listed IP address whether or not it is excluded, and never falls back to public S3 addresses, so watch the health endpoint for excluded IP addresses.

**Alternatives**, worth taking when the routing already exists. [Cost effective methods for accessing S3 buckets cross-region](https://repost.aws/articles/ARjzluyMS8RbeOOK4MGXRG6Q/cost-effective-methods-for-accessing-s3-buckets-cross-region) compares seven patterns with worked costs:

- **Interface endpoints reached over VPC peering or Transit Gateway** carry the same three proxy-side requirements as above. Peering is subject to per-VPC connection quotas, and Transit Gateway adds per-attachment hourly and per-GB processing charges.
- **NAT gateway** is the simplest and already present in many VPCs, and the most expensive per GB: data processing on top of inter-Region transfer. It is also where the cache saves the most, since only misses are processed.
- **Egress-only internet gateway with IPv6** is the cheapest pattern in that comparison but is not a supported configuration here. The proxy's resolver asks for AAAA and A records and orders AAAA first, so a dualstack hostname puts IPv6 addresses into the IP distributor. On an instance without an IPv6 route, each IPv6 address fails to connect until the per-IP failure threshold excludes it. Every new IPv6 address that DNS returns repeats that, so the result is recurring bursts of errors rather than one clean failure. Dualstack hostnames also change what the client signs.
- **S3 Multi-Region Access Points** require clients to sign for the MRAP hostname with SigV4a and address a global endpoint (`accesspoint.s3-global.amazonaws.com`). The proxy handles both; reach the MRAP through a `com.amazonaws.s3-global.accesspoint` interface endpoint and list its ENI IP addresses in `endpoint_overrides`.

**Tuning**:

- **Address the bucket by its home-Region hostname.** A wrong-Region endpoint makes S3 redirect rather than fail outright, so the symptom is lower throughput rather than an error. Check this first.
- **Leave `connection_pool.tcp_recv_buffer_size` unset** so the kernel auto-tunes the TCP receive window; pinning `SO_RCVBUF` caps throughput at the bandwidth-delay product on a high-RTT path.
- **Lengthen `get_ttl` for prefixes whose objects never change.** The default is `1d`, after which the next read of an object sends S3 a conditional GET. Such a prefix can use up to `315360000s` (10 years) through [cache rules](CONFIGURATION.md#cache-rules).

### Storage class

Storage class is a property of each object. A bucket has no storage class and no default one: the `x-amz-storage-class` header on each `PUT` chooses the class, S3 uses `STANDARD` when the header is absent, and a lifecycle transition can change it afterwards. One prefix can therefore hold a mix of classes, and an object's class can change while the proxy holds a cached copy of it.

The class decides whether reading an object pays a per-GB retrieval charge on top of everything else. Standard and Intelligent-Tiering carry none. Standard-IA and One Zone-IA are $0.010/GB, and Glacier Instant Retrieval is $0.030/GB (us-west-2, as of September 2026; check your Region).

Retrieval is charged on every byte a miss reads, wherever it is read from, and adds to [data transfer out to another AWS Region](#cross-region-s3). Reading an object in Glacier Instant Retrieval from another Region costs $0.050/GB against $0.020/GB for one in Standard; reading it from inside its own Region costs $0.030/GB where Standard costs nothing.

Where a prefix holds these classes, the hit ratio is worth more than the transfer arithmetic suggests, because each hit avoids retrieval as well as transfer. A long `get_ttl` is worth more too, because a revalidation that re-fetches the object pays retrieval again.

### S3-compatible store outside AWS

The proxy works with any S3-compatible store that meets the requirements below. The Direct Connect or VPN link to the store has fixed capacity shared with everything else crossing it, so each hit frees link bandwidth for other workloads as well as saving time.

- **Declare the origin's transport** with `connection_pool.upstream_overrides`, preferring validated HTTPS on a non-standard port, which keeps every protection ([Upstream transport overrides](CONFIGURATION.md#upstream-transport-overrides)).
- **Point `connection_pool.dns_servers` at a resolver that can see the origin.** The default public resolvers (Google and Cloudflare) do not resolve an internal name. [DNS server configuration](CONFIGURATION.md#dns-server-configuration) lists the paths that always use the public resolvers.
- **Private-IP origins are reached normally on the caching paths until you set an allowlist.** The [destination policy](ARCHITECTURE.md#destination-policy-ssrf-protection) prohibits `0.0.0.0/8`, `127.0.0.0/8`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `169.254.0.0/16` (which includes the instance metadata address). In IPv6 it also prohibits `::`, `::1`, `fe80::/10`, `fc00::/7` and IPv4-mapped forms of the IPv4 ranges.
  - It always gates the two tunneled paths, the `CONNECT` passthrough and the HTTPS passthrough (port 443). Those admit port 443 only, and only a destination with at least one resolved IP address that is outside the prohibited ranges or listed in `endpoint_overrides`.
  - It gates the HTTP forwarding paths (the HTTP listener on port 80 and the TLS proxy listener's non-`CONNECT` requests) only once `server.tls.connect_allowlist` is set. By default they reach a private-IP origin with no check.
  - With an allowlist set, an HTTP forward must target `server.http_port`, so a forward-proxy request to another port, such as `:9000`, is refused with 403. Its hostname must match the allowlist, and it needs at least one resolved IP address that is not prohibited or is listed in `endpoint_overrides`. The policy reads the port from the `Host` header, falling back to the request-target port when there is no `Host`, and treats a portless `Host` or `:80` as `server.http_port`.
  - With or without an allowlist, a forward-proxy request whose `Host` names a different host or port from its request target is refused with 400.
  - `endpoint_overrides` is the only carve-out: an allowlist entry or an `upstream_overrides` entry does not admit a private IP address. A hostname that resolves to both a public and a private address passes the check, and the HTTP paths resolve again when they connect, so the connection can still go to the private address.
- **The origin must return an `ETag` on GET and HEAD responses**, as S3 does. The proxy tells object versions apart by ETag.
- **Give each store distinct bucket names.** A cache key is a bucket name and an object path, with neither the host nor the port. So two stores sharing a bucket name map to one cache key. A bucket name belongs to the first origin that caches it, so neither store is served the other's entries, and the origin that did not claim the name is served uncached. Only distinct bucket names give both stores a cached copy ([One bucket name at two origins](CACHING.md#one-bucket-name-at-two-origins)).
- **Warm the cache before a workload runs.** Misses run at link speed, and the proxy holds no credentials and cannot prefetch, so warming means requesting the dataset through it once.

## Mount and configuration

Two mount properties are correctness requirements. Without `lookupcache=pos`, proxies cache negative lookups and cannot see files their peers wrote (a 40%+ miss rate on repeat downloads); the proxy logs one warning at startup naming the mount and still starts. Without working cross-host file locking, proxies hold the same coordination lock at the same time and evict or consolidate against each other, and the proxy reports nothing.

Neither appears in AWS's recommended options for EFS or for FSx, so add both. The rest of the option list differs by file system, so take the line for the file system you run. For example, `noresvport` is recommended on EFS and rejected by FSx, and `nconnect` is recommended on FSx and neither supported nor needed on EFS. [Mount requirements](SHARED_STORAGE.md#mount-requirements) gives both lines, the per-option comparison, and the verification steps.

A mount that fails silently leaves `cache_dir` on the root file system, so cache writes go to local disk; check with `df -h`.

Settings that differ from the defaults:

```yaml
cache:
  cache_dir: "/mnt/fsx/cache"
  max_cache_size: 53687091200        # 50 GiB; must fit the file system with headroom
  max_ram_cache_size: 1073741824     # 1 GiB per proxy

logging:
  access_log_dir: "/mnt/efs/logs/access"
  app_log_dir: "/mnt/efs/logs/app"
```

The example keeps the logs on EFS and the cache on FSx, two separate file systems. Shared log directories let one `cat` read every proxy's access log for a date ([aggregating logs across proxies](ACCESS_LOG_FORMAT.md#multi-instance-aggregation)) and let retention reap files a departed proxy left behind ([Retention applies to the directory](ACCESS_LOG_FORMAT.md#retention-applies-to-the-directory-not-to-the-writer)). Keep them off the cache's file system. The proxy measures consumed bytes across the whole file system rather than by walking `cache_dir`, so log bytes there count against the cache and the disk-exhaustion guard turns conservative. A sibling directory on the same file system is not enough ([Cache volume dedication](SHARED_STORAGE.md#cache-volume-dedication-and-storage-layer-dedupcompression)).

Coordination between proxies on the cache volume is always on and needs no setting. For analytics-style small reads inside large objects, consider [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching).

## Verification

Run the `curl` checks on each proxy against `127.0.0.1`, or from another host once `health.bind_address` and `metrics.bind_address` are `0.0.0.0` ([Credentials and network access](#credentials-and-network-access)).

```bash
curl -s http://<proxy-host>:8080/health                          # each proxy healthy
mount | grep -E 'fsx|efs'                                        # confirm lookupcache=pos and nfsvers=4.1, and that nolock/local_lock are absent
curl -s http://<proxy-host>:9090/metrics | grep cache_hit_rate_percent   # hit rate rising
```

Confirm the cache is shared, which a health check cannot tell you: request an object through one proxy, then the same object pinned to a different proxy, and expect a hit. A miss points at a mount missing `lookupcache=pos`.

Confirm locking separately, since a mount can pass the shared-cache check and still not enforce locks between hosts:

```bash
flock -x /mnt/fsx/.locktest -c 'sleep 30'                              # on one instance, hold the lock
flock -n -x /mnt/fsx/.locktest -c true && echo BROKEN || echo OK       # on a second instance, must print OK
```

`BROKEN` means locks are host-local and coordination between proxies is unsafe; fix the mount options before running more than one proxy against the volume.

## Monitoring

To send the proxy's metrics to CloudWatch, enable OTLP export to the CloudWatch agent's OTLP receiver (agent v1.300048.0 or later); [CloudWatch](OTLP_METRICS.md#cloudwatch) gives the agent configuration. The agent needs `CloudWatchAgentServerPolicy` on the instance profile. Cache hit rate and per-bucket traffic are also on the [dashboard](DASHBOARD.md) and `/metrics` without an agent ([Metrics](METRICS.md)).

Alarm on the infrastructure metrics that confirm the sizing decisions above:

| Metric | Namespace | Use |
|---|---|---|
| `FileServerCacheHitRatio` | `AWS/FSx` | Share of reads served by the file server's cache. A falling ratio means more reads are reaching disk and losing the cached-read multiple. |
| `NetworkThroughputUtilization` | `AWS/FSx` | Percentage of the provisioned network limit. Consistently low means you can drop a throughput tier. |
| `FileServerDiskIopsUtilization` | `AWS/FSx` | Confirms the included 3 IOPS per GiB is enough before you provision more. |
| `FileServerDiskThroughputBalance`, `FileServerDiskIopsBalance` | `AWS/FSx` | Burst credit balance. Watch these on the low tiers; a balance trending to zero means the workload is sustained rather than spiky. |
| `MeteredIOBytes` vs `PermittedThroughput` | `AWS/EFS` | Throughput utilization on EFS. `MeteredIOBytes` is also the Elastic Throughput billing signal, so the break-even above is denominated in it. |
| `PercentIOLimit` | `AWS/EFS` | Approach to the General Purpose I/O limit. |
| `UnHealthyHostCount`, `TCP_Target_Reset_Count` | `AWS/NetworkELB` | Proxies failing health checks, and connections reset; the latter rises if the idle timeouts are mismatched. |

Also watch for validation scan warnings in `journalctl -u s3-proxy`, and for origin egress on the bill or the link graphs. [Error handling](ERROR_HANDLING.md) covers recovery after cache corruption.
