# Deploying on AWS for Distant Origins

Deployment recommendations for two cases with a high-latency origin: a **cross-region** S3 bucket, and an **S3-compatible store outside AWS**. For installation see the [Quick Start Guide](GETTING_STARTED.md); for the full option list see the [Configuration Reference](CONFIGURATION.md); for measured throughput see [Performance](../README.md#performance).

- [When this guide applies](#when-this-guide-applies)
- [Shared cache volume](#shared-cache-volume)
  - [Sizing the file system](#sizing-the-file-system)
  - [The EFS alternative](#the-efs-alternative)
- [EC2 fleet](#ec2-fleet)
  - [Instance sizing](#instance-sizing)
  - [Bootstrap](#bootstrap)
  - [Service configuration](#service-configuration)
  - [Credentials and network access](#credentials-and-network-access)
- [Client routing](#client-routing)
  - [DNS: Route 53 private hosted zones](#dns-route-53-private-hosted-zones)
  - [Load balancer: encrypting the client hop](#load-balancer-encrypting-the-client-hop)
  - [Choosing between DNS and NLB](#choosing-between-dns-and-nlb)
- [Origin configuration](#origin-configuration)
  - [Cross-region S3](#cross-region-s3)
  - [S3-compatible store outside AWS](#s3-compatible-store-outside-aws)
- [Mount and configuration](#mount-and-configuration)
- [Verification](#verification)
- [Monitoring](#monitoring)

## When this guide applies

The origin is in a different Region to your compute, or outside AWS entirely, so every fetch pays a high round-trip time and either an inter-region per-GB charge or consumption of a fixed-capacity link. In both cases a cache hit is far cheaper than an origin fetch, so hit ratio and shared-cache read speed determine the result. Size for re-reads: an individual miss is slower than the equivalent direct request, since it pays the origin fetch plus TLS, compression, and the cache write.

```
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

Use **FSx for OpenZFS** in an HA deployment type: `SINGLE_AZ_HA_2` if clients are concentrated in one AZ, `MULTI_AZ_1` if they span AZs. Place proxies in the same AZ as the clients they serve. [EFS is a viable alternative](#the-efs-alternative) at low duty cycle.

The volume is a shared dependency of the whole fleet, so its availability is the fleet's availability. HA pairs [fail over in under 60 seconds](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/availability-durability.html) where non-HA file systems self-heal in roughly 30 minutes, and throughput changes and maintenance become failovers rather than outages.

Both HA types are priced per provisioned unit ([FSx for OpenZFS pricing](https://aws.amazon.com/fsx/openzfs/pricing/), us-east-1):

| Per month | `SINGLE_AZ_HA_2` | `MULTI_AZ_1` |
|---|---|---|
| Throughput | $0.52 /MBps | $0.87 /MBps (1.7×) |
| SSD storage | $0.09 /GB | $0.18 /GB (2×) |
| Provisioned SSD IOPS | $0.012 | $0.024 (2×) |

Multi-AZ synchronously replicates storage across AZs, which is what doubles the SSD storage price.

> **Note**: `SINGLE_AZ_HA_2` is not offered in every Region — check [Availability by AWS Region](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/available-aws-regions.html). Where it is absent, prefer `MULTI_AZ_1` over `SINGLE_AZ_HA_1`, which caps at 4,096 MB/s.

**Multi-AZ clients**: `MULTI_AZ_1` is a preferred and a standby file server in two AZs, one active at a time behind a floating IP that follows the active server across failover. There is [no data transfer charge from any AZ in the Region](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/access-within-aws.html), but only proxies in the active server's AZ read locally. The rest traverse an inter-AZ network path on every disk-cache read, adding latency; a failover moves which AZ is local. AWS recommends placing clients in the preferred file server's AZ.

Place a proxy group in each AZ alongside the clients it serves. All proxies share one cache namespace. A Single-AZ file system adds per-GB transfer on top of that latency for proxies outside its AZ, on every request that is not a RAM hit.

**Single-AZ clients**: `SINGLE_AZ_HA_2` is the lower-cost, lower-latency choice when all compute is in one AZ. Place the file system in the same AZ as the proxies.

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

Storage adds $0.09 per GB-month on a 64 GiB minimum. Rates are [us-east-1 list pricing](https://aws.amazon.com/fsx/openzfs/pricing/) at time of writing. See [Instance sizing](#instance-sizing) for the compute cost and the all-in minimum.

Size against the cached-read column, not the headline throughput figure. Reads served from the file server's memory run at roughly **2.4× the provisioned rate** (2.05× at the top tier) — these ratios are derived from the table above (cached-read baseline ÷ provisioned throughput). The provisioned number is the disk ceiling while cached reads are bound only by the network path. A cache is re-read heavy by nature, so this is the column that applies — but the benefit holds only while the working set fits the memory shown; beyond that, reads fall back to the disk rate.

Throughput capacity also provisions the file server's CPU, which bounds metadata operation rates as well as bandwidth. At the 160 MB/s floor tier, parallel unlink and file creation flatten well below what the cached-read column implies. If the cache churns many small ranges, size a tier above what read throughput alone would justify.

Pick the smallest tier whose **baseline** cached-read throughput clears your fleet's aggregate cache-hit read throughput with headroom. Size against the bytes the proxy reads off the volume — the compressed size, so client-delivered throughput divided by your compression ratio. Assume a ratio of 1 unless you know the content compresses; cached media, Parquet, and archives largely do not. The low tiers burst above baseline, which suits a spiky cache, but burst credits accrue only while you are under baseline, so do not size sustained load against them. Start modest: capacity is adjustable later, and on an HA file system adjusting it is a failover rather than an outage.

Set storage capacity above `cache.max_cache_size`, leaving room for journals, locks, size-tracking state, and in-flight `.tmp` writes. SSD capacity can be expanded but not reduced.

Leave IOPS at the included 3 IOPS/GiB unless the volume is small. The proxy's demand is bounded by its own design — large-block range I/O, per-cycle journal consolidation, and `metadata_io_concurrency` capping metadata I/O at 32 concurrent operations per proxy — so the included IOPS on a multi-TB volume is ample. Provision explicitly below roughly 1 TB, where 3 IOPS/GiB is starved.

Leave compression to the proxy, which LZ4-compresses range data before writing. Compressing before the data crosses the network shrinks both the cache and the NFS traffic, which file-system compression cannot do. Enabling LZ4 on the volume is harmless but should not feature in sizing.

### The EFS alternative

EFS is viable but not performance-equivalent. FSx answers each operation faster; EFS scales bulk operations wider, costs less at low duty cycle, has elastic capacity, and puts a mount target in every AZ.

| | Read latency | Write latency |
|---|---|---|
| FSx OpenZFS (cached data) | a few hundred µs | a few hundred µs (`sync` export — the default and recommended) |
| FSx OpenZFS Multi-AZ (cached data) | a few hundred µs | 1–2 ms |
| EFS Regional, Elastic Throughput | ~1 ms | ~2.7 ms |
| EFS One Zone, Elastic Throughput | ~1 ms | ~1.6 ms |

Latency matters more here than headline throughput: each cache hit reads `.meta`, may take a lock, then reads range data, so per-operation cost lands on the critical path before any bytes flow. The gap is widest for small objects and amortizes away on large sequential reads.

For an AZ-spanning fleet, EFS Regional has [a mount target in each AZ](https://docs.aws.amazon.com/efs/latest/ug/how-it-works.html) and each proxy mounts its own, so every proxy reads locally with no cross-AZ charge. This is the one place EFS beats FSx Multi-AZ, which keeps only one file server active. EFS One Zone has a single mount target and charges data access from other AZs.

On EFS, **use Elastic Throughput and mount with the EFS client** (`amazon-efs-utils` 2.0 or later, or the EFS CSI driver). Both are required for the [1,500 MiBps per-client cap](https://docs.aws.amazon.com/efs/latest/ug/performance.html); any other combination is limited to 500 MiBps, and each proxy counts as one client. This is also why EFS has no use for `nconnect` — the client provides that connection parallelism itself, so the mount option that FSx needs has no equivalent here. Note that 2.0 replaced `stunnel` with `efs-proxy`, and existing mounts must be re-mounted to pick up its behaviour.

#### Storage backend behavior

FSx for OpenZFS and EFS solve different problems.

**FSx for OpenZFS** suits cache workloads that are sensitive to per-operation latency. Cache
hits commonly read metadata, take a lock, and read range data, so several filesystem operations
sit on the request path. Size its provisioned throughput for both cached-read capacity and the
file server memory that holds frequently accessed data.

**EFS Regional** suits fleets that need an AZ-local mount target for every proxy, elastic
capacity, or a lower fixed cost at low duty cycle. It can scale bulk operations across its
distributed service, but has higher metadata and locking latency than FSx. Use Elastic
Throughput with the EFS mount helper so each proxy can reach the documented per-client limit.

A Multi-AZ FSx file system has one active file server at a time. Clients in that server's AZ
read locally; clients in other AZs traverse the regional network path. The file system fails
over automatically, so which AZ is local can change. EFS Regional instead gives each AZ its own
mount target.

Choose the backend from the workload:

- Choose **FSx** when cache-hit latency, lock-heavy maintenance, or predictable storage cost
  matters most.
- Choose **EFS Regional** when proxies span AZs, demand is elastic, or the workload stays below
  the cost crossover below.
- Size either backend from the traffic the proxy actually sends to the shared volume, after
  accounting for compression and the two NIC crossings on a shared-volume cache hit.

#### Cost comparison

FSx charges a fixed rate for provisioned capacity; EFS charges per GB transferred
([$0.03/GB read, $0.06/GB written](https://aws.amazon.com/efs/pricing/) in us-east-1
Elastic Throughput). A cache miss writes to the volume, so the effective EFS transfer rate
depends on hit rate.

**Illustrative starting fleet:** three `c6in.large` proxies. The [network model below](#sizing-the-network)
projects roughly 560 MiB/s of sustained shared-cache hits and 3,750 MiB/s while EC2 burst
credits last. The 160 MB/s FSx tier cannot cover that burst. The 640 MB/s tier is the first
that can: it supplies 1,550 MB/s cached-read baseline and roughly 5,000 MB/s burst from the
sizing table. AWS documents the same performance levels for [Multi-AZ and Single-AZ 2](https://docs.aws.amazon.com/fsx/latest/OpenZFSGuide/performance.html).

EFS Elastic Throughput has no matching provisioned tier. Its 1,500 MiB/s per-client limit
exceeds one proxy's share of this fleet's burst, so the proxy NICs bind before EFS does. This
is a cost crossover for the documented starting fleet, not a performance-equivalence claim.
For sustained load, size FSx against its cached-read **baseline**, as [Sizing the file system](#sizing-the-file-system) requires.

The storage model assumes **500 GB of proxy-accounted compressed payload**, a **0.5 MB average
range**, and one `.meta` per range: 1,000,000 pairs. On EFS, a 500,000-byte `.bin` rounds to
503,808 bytes and carries a 2 KiB inode; a small `.meta` carries 2 KiB metadata plus a 4 KiB
allocation. The pair meters at 512,000 bytes, so the payload cache occupies **512 GB** of EFS
metered storage before directories and lock files. Recalculate for your range size and
ranges-per-object ratio.

**Multi-AZ** — FSx Multi-AZ vs EFS Regional:

FSx: `$90 storage + $557 throughput` = $647/month. EFS: **$153.60/month storage** + transfer.

| Cache hit rate | EFS transfer per GB served | Break-even |
|---|---:|---:|
| 100% | $0.030 | ~16.4 TB/month served |
| 50% | $0.045 | ~11.0 TB/month |
| 0% | $0.060 | ~8.2 TB/month |

**Single-AZ** — FSx Single-AZ HA vs EFS One Zone:

FSx: `$45 storage + $333 throughput` = $378/month. EFS: **$81.92/month storage** + transfer.

| Cache hit rate | EFS transfer per GB served | Break-even |
|---|---:|---:|
| 100% | $0.030 | ~9.9 TB/month served |
| 50% | $0.045 | ~6.6 TB/month |
| 0% | $0.060 | ~4.9 TB/month |

EFS is least competitive when the cache is cold or churning. The 512 GB storage model above
adds 12 GB (2.4%) for the `.bin` and `.meta` objects at a 0.5 MB average range; it excludes
directory and lock-file allocation, so treat it as a floor. Elastic Throughput separately
meters a minimum 32 KiB per data operation and 4 KiB per metadata operation. Those are transfer
charges, not stored capacity: at 0.5 MB per range the data-operation minimum does not change
the per-GB transfer figures above, but metadata operations still add cost.

**Choose EFS** for spiky or low-duty-cycle workloads below the break-even, or for elastic capacity with no tier to size, provided your mean cached range clears the eviction figure above. **Choose FSx** when performance or cost predictability is the priority, or when the cache churns small ranges: FSx bills a flat rate for provisioned capacity, so the monthly cost is fixed regardless of traffic, while the EFS bill tracks bytes served. A reasonable hybrid is cache on FSx, configuration and logs on EFS.

## EC2 fleet

### Instance sizing

Scale out, not up: adding proxies raises aggregate throughput, while enlarging a single one mainly buys more concurrent connections, because the bottleneck is per-connection processing rather than total CPU.

**Recommended starting point**: 3× `c6in.large` (2 vCPU, 4 GiB, **3.125 Gbps sustained**, 25 Gbps burst) — the network-optimized family, sized for the proxy's actual resource use. Every upload path and cache-miss GET streams rather than buffering whole objects, so memory on them is independent of object size. What still buffers is read-side: range merge, page widening, buffered range serving, and the recovery fallbacks. Those are bounded in aggregate by `server.max_inflight_buffer_bytes` (opt-in, disabled by default — see [Configuration → In-Flight Memory Ceiling](CONFIGURATION.md#in-flight-memory-ceiling)).

#### Sizing the network

`c6in.large` and `m6in.large` sustain 3.125 Gbps (≈373 MiB/s) indefinitely. The "up
to 25 Gbps" in their spec is credit-based burst: credits accrue below baseline and
drain above it.

**Burst suits a cache.** Cache traffic is usually spiky — a working set is pulled in,
read hard for a few minutes, then the fleet idles and rebuilds credit. Sizing every
proxy for its own peak means paying continuously for capacity you use in bursts. If
your peaks are short relative to the gaps between them, count the burst figure in
your capacity plan.

Size against baseline in two cases:

- **Sustained load.** Run above baseline long enough to exhaust credits and
  throughput drops to baseline mid-flight, which looks like an unexplained slowdown.
  Bulk ingest, long analytics scans, and overnight migrations all qualify.
- **Benchmarking.** A benchmark is sustained by design, so one run on burst credit
  measures capacity the instance cannot hold: high at the start, dropping as credits
  drain, and dependent on how idle the instance was beforehand. The result is not
  reproducible.

[Sizing the file system](#sizing-the-file-system) makes the same point about burst
credits for FSx throughput tiers.

Whichever figure you size against, the proxy's NIC carries each byte more than once:

| Operation | NIC crossings per byte | Client-facing share of baseline |
|---|---|---|
| Cache hit from the shared volume | 2 (read the volume, write the client) | ~50% |
| Cache hit from the RAM cache | 1 (write the client) | ~100% |
| Cache miss | 2 (fetch from S3, write the client) plus the cache write | ~50% or less |
| Write (PUT or `UploadPart`) | ~3 (ingest, forward to S3, write the part to the volume) | ~33% |

A 3× `c6in.large` fleet has ~1,118 MiB/s of aggregate baseline, so about **560 MiB/s
of client-facing reads on shared-volume cache hits and 370 MiB/s of sustained
writes**. Divide your required client-facing throughput by the relevant share above;
do not compare it to the raw baseline. Writes are the expensive direction.

The multiplier applies to burst too, so the same fleet has roughly 3,750 MiB/s of
client-facing read headroom while credits last. A RAM hit crosses the NIC once, so
raising `cache.max_ram_cache_size` buys network capacity as well as latency, often
more cheaply than adding instances.

Sizing memory: `server.max_concurrent_requests` now bounds the whole request including its transfer, not just setup, so the streaming-path memory formula `~200 MiB + max_concurrent_requests × ~5 MiB` (see [Configuration → Memory Impact](CONFIGURATION.md#server-configuration)) is a sound fleet-wide ceiling for the streaming paths. It does not cover the read paths that still buffer a whole range — those are bounded in aggregate across concurrent requests by the opt-in `max_inflight_buffer_bytes` ceiling. On a 4 GiB `c6in.large` with the default 512 MiB RAM cache, 512 MiB (`536870912`) is a reasonable starting ceiling. `cache.max_ram_cache_size` (default 512 MiB) is a third, independent budget on top of both. Confirm all three against measured peak resident memory and the corresponding `/metrics` fields (`request_metrics.permits_held_peak`, `inflight_memory.peak_reserved_bytes`) under representative load.

Place each proxy in the same AZ as the clients it serves, subject to the file-system constraints in [Shared cache volume](#shared-cache-volume).

**Cost**: `c6in.large` is [$0.1134/hour on-demand in us-east-1](https://aws.amazon.com/ec2/pricing/on-demand/), or about **$83/month** per instance. A single proxy + FSx at 160 MB/s (64 GiB) costs roughly **$171/month** on-demand. For redundancy, start with 3 instances (**$337/month** all-in). [Compute Savings Plans](https://aws.amazon.com/savingsplans/compute-pricing/) (1-year, no upfront) reduce EC2 costs by approximately 30–40%; FSx throughput and storage charges are not covered by Savings Plans.

### Bootstrap

Build once, then distribute the binary. Do not install a Rust toolchain on the proxies — compiling per instance puts a compiler and crates.io egress on production hosts, lengthens boot, and lets instances drift onto binaries built from different source states. Build on CI or a build host, observing the architecture and glibc constraints in [Binary Portability](GETTING_STARTED.md#binary-portability), then choose a distribution route:

| Approach | Trade-off |
|---|---|
| Bake into an AMI (EC2 Image Builder) | Fastest boot, immutable, no fetch permissions. New AMI per upgrade. |
| Publish to S3, fetch in userdata | Simple to automate. Needs read on that prefix in the instance profile. |
| Place on the shared volume | No extra bucket or permissions; the volume is already mounted for config. |

From the shared volume, copy the binary to local disk at boot rather than executing from the mount, which would couple process start to volume availability.

Userdata mounts the volume, places the binary, writes the systemd unit, and enables the service. Write the unit in the same step as the binary: if it is missing, `systemctl enable` fails while the rest of the script succeeds, leaving an instance that looks bootstrapped but runs no proxy. Redirect userdata output (`exec > /tmp/userdata.log 2>&1`), which is otherwise discarded.

Example scripts: [`userdata-fsxz.sh`](examples/userdata-fsxz.sh) (FSx for OpenZFS) and [`userdata-efs.sh`](examples/userdata-efs.sh) (EFS with Elastic Throughput). Both fetch the binary from S3, install the sandboxed systemd unit, and verify health on boot.

### Service configuration

Start from [`config/s3-proxy.service`](../config/s3-proxy.service), which is sandboxed, and add your cache and log directories to `ReadWritePaths` — `ProtectSystem=strict` otherwise makes the filesystem read-only and cache writes fail. The unit runs as `root` to bind ports 80 and 443; [proxy-only mode](GETTING_STARTED.md#proxy-only-mode) avoids that if you prefer an unprivileged user.

Keep `config.yaml` on the shared volume and point every instance at it with `-c`, so one edit applies fleet-wide and instances cannot drift. `cache_rules.json` lives there too and hot-reloads without a restart.

### Credentials and network access

The proxy holds no AWS credentials — it forwards requests the client has already signed and cannot call S3 on its own behalf — so the instance profile grants nothing for the data path. It needs `AmazonSSMManagedInstanceCore` for management, `CloudWatchAgentServerPolicy` if you publish metrics to CloudWatch, and read on the artifact prefix if userdata fetches the binary from S3. Manage instances over SSM so no inbound SSH is required.

Open only the ports the chosen client-routing mechanism actually uses. Behind a load balancer that reaches the TLS listener (either encrypted configuration under [Load balancer: encrypting the client hop](#load-balancer-encrypting-the-client-hop)), only 3129 needs to be reachable — leaving 80 open there is an unnecessary cleartext path into the fleet.

| Port | Source |
|---|---|
| 80, 443 (and 3128/3129 if used) | client CIDRs, or the load balancer's security group |
| 2049 (NFS) | the file system — a self-referencing SG rule is simplest |
| 8080 health, 8081 dashboard, 9090 metrics | operators, monitoring, and load balancer health checks |

## Client routing

Clients must resolve S3 hostnames to the proxy fleet. [Getting Started](GETTING_STARTED.md#3-configure-client-routing) covers every mechanism; on AWS, use a Route 53 private hosted zone or a Network Load Balancer. In a multi-AZ deployment, prefer same-AZ routing to avoid cross-AZ latency on client → proxy.

### DNS: Route 53 private hosted zones

Create a private hosted zone associated with the client VPC for **each S3 hostname the fleet serves** — `s3.<region>.amazonaws.com` per region whose buckets are accessed, plus any access-point hostnames. In each zone, add one multi-value answer `A` record per proxy, each with a distinct set identifier and a short TTL.

This creates no resolution loop for the proxy, which resolves upstream S3 through `connection_pool.dns_servers` and ignores `/etc/hosts`.

**AZ-local preference**: Route 53 multi-value routing does not natively prefer same-AZ answers. Use **per-AZ hosted zones**: create a separate hosted zone per AZ (e.g. `s3.eu-west-1.amazonaws.com` associated with AZ-a subnets only, containing only AZ-a proxy IPs). Each AZ's clients resolve only their local proxies. If an AZ loses all proxies, clients fall back to a shared "all AZ" zone associated at a lower priority — implemented as a Route 53 Resolver rule forwarding to the shared zone.

For a single-proxy static fleet, `/etc/hosts` on each compute host is the minimal alternative — but it resolves to one address (no rotation or load-balancing), does not adapt to proxy failures, and must be maintained manually across fleet changes. Prefer DNS or an NLB for anything beyond a proof-of-concept.

**Auto Scaling with DNS**: use EventBridge to react to EC2 instance state changes. Create a rule matching `EC2 Instance State-change Notification` for states `running` and `terminated`, filtered to the ASG's instances by tag. Target it at a Lambda that looks up the instance's private IP and AZ, then upserts (on `running`) or deletes (on `terminated`) the multi-value A record in the zone for that AZ. No lifecycle hook is needed — the proxy must mount the file system and pass health checks before serving traffic anyway, and DNS TTL is short enough that the brief propagation window after `running` is not a gap in practice.

**Static fleet**: update every zone on every fleet change manually. Do not mix proxy IPs across AZs in a single zone if you want AZ affinity.

### Load balancer: encrypting the client hop

[GETTING_STARTED](GETTING_STARTED.md#which-pattern-to-choose) names three TLS termination patterns and compares them. Two of the three encrypt the NLB→proxy hop, and this section gives the AWS configuration for both.

**Pattern 1 (`TLS` listener with a `TCP` target group) is not covered here.** It decrypts at the NLB and forwards cleartext to the proxy — see [what that exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes). Choose it only where you have consciously accepted the NLB→proxy segment as trusted; Pattern 3 below encrypts that segment for one extra target-group setting.

#### Pattern 3: NLB re-encrypts to the proxy (recommended)

Change the target group protocol from `TCP` to **`TLS`**:

```
Client (HTTPS) → NLB (TLS :443, ACM cert) → Proxy (TLS :3129) → S3 (HTTPS)
```

The NLB terminates the client-facing session with an ACM certificate and opens a second TLS session to the proxy's TLS listener. Because [an NLB TLS target group does not validate the target's certificate](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups), the proxy can present a self-signed, long-lived cert with any SAN — no rotation tied to the client-facing name.

Enable the [TLS proxy listener](GETTING_STARTED.md#tls-proxy-listener-configuration) on each instance, then configure the NLB:

| Setting | Value |
|---|---|
| Listener | `TLS` on 443, ACM certificate attached |
| Target group | Protocol `TLS`, port 3129 (`tls_proxy_port`) |
| Health check | Protocol `HTTP`, override port `8080`, path `/health` |
| Cross-zone load balancing | **Off** — keeps traffic in the client's AZ |
| AZ DNS affinity | **100% (Availability Zone affinity)** — clients resolve the NLB IP in their own AZ |
| TCP idle timeout | 350 s default; raise for clients that idle mid-session |
| Deregistration delay | Above 300 s if single transfers run longer |

**AZ-local preference** is built in when these two settings are combined. AZ DNS affinity causes clients using Route 53 Resolver to receive the NLB IP in their own AZ, and disabled cross-zone load balancing keeps the NLB from forwarding to targets in other AZs. Together, traffic stays client → same-AZ NLB node → same-AZ proxy. If all targets in an AZ become unhealthy, the NLB DNS falls open and routes to a healthy AZ.

Store a self-signed cert and key on the shared volume so every instance presents the same one (`chmod 600` the key) — the NLB does not validate it, so any CN/SAN and any expiry works. Allow health-check traffic to port 8080 in the instance security group. If you raise the TCP idle timeout above 350 s, raise the targets' ENI `TcpEstablishedTimeout` to match.

#### Pattern 2: NLB passes TCP through, proxy terminates the client's TLS

Choose this over Pattern 3 when the **client** must validate the proxy's certificate itself, or when you cannot attach a certificate to the NLB. Pattern 3 protects the internal hop against passive observation but not against an active attacker impersonating a proxy instance on it, because the NLB does not validate the target certificate. Pattern 2 closes that gap — at the cost of real certificate management.

```
Client (HTTPS) → NLB (TCP :443, passthrough) → Proxy (TLS :3129, terminates) → S3 (HTTPS)
```

The differences from the Pattern 3 configuration above:

| Setting | Value |
|---|---|
| Listener | `TCP` on 443, **no** certificate on the NLB |
| Target group | Protocol `TCP`, port 3129 (`tls_proxy_port`) |

Everything else — health check, cross-zone off, AZ DNS affinity, idle and deregistration timeouts — is unchanged.

The certificate requirements are the real cost, and they are the reason Pattern 3 is the default. The client validates the certificate the proxy presents, so its SANs must match the **NLB's** hostname (that is what clients connect to), every instance must present a cert covering those SANs, and rotation is now tied to the client-facing name rather than being a one-off self-signed file. Put the cert and key on the shared volume so all instances serve the same one, `chmod 600` the key, and plan the rotation.

Clients must trust that certificate and address the NLB on the TLS port:

```bash
export AWS_CA_BUNDLE=/path/to/proxy-cert.pem
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url https://<nlb-hostname>:3129 \
  --region us-east-1
```

To let clients use the default port instead, point the NLB's TCP listener on 443 at target port 3129 and drop `:3129` from the endpoint URL.

#### Both encrypted patterns

**Auto Scaling with NLB**: attach the ASG directly to the target group. Registration and deregistration are automatic — no lifecycle hook or Lambda needed. The deregistration delay drains in-flight connections before the instance is terminated.

**Use an NLB, not an ALB.** The proxy already terminates and interprets HTTP itself, so an ALB redoes that work on every request for no gain and inserts its own HTTP behaviour between client and proxy. An NLB forwards bytes at Layer 4 and stays out of the way. If you specifically want a Layer 7 router that picks an instance from the request path, that is a different design — see [Request-Aware Routing](REQUEST_AWARE_ROUTING.md).

### Choosing between DNS and NLB

| | DNS (Route 53 multi-value) | NLB |
|---|---|---|
| AZ affinity | Per-AZ hosted zones (manual) | Built-in via AZ DNS affinity + cross-zone off |
| Auto Scaling | EventBridge rule + Lambda | Attach ASG to target group; automatic |
| Client requirement | CRT or multi-value-aware resolver | Any client |
| Failover speed | DNS TTL | Health check interval |
| Client → proxy encryption | Cleartext (HTTP on port 80 for caching); HTTPS on 443 passes through uncached | Encrypted and cached via the TLS listener on 3129 — Pattern 3 (re-encrypt at NLB) or Pattern 2 (TCP passthrough) |
| Cost | < $1/month (query charges) | ~$450/month at 100 GB/hr; ~$2,200/month at 500 GB/hr ([pricing](https://aws.amazon.com/elasticloadbalancing/pricing/)) |

NLB is the simpler path for multi-AZ fleets with Auto Scaling: AZ affinity, health checking, and scaling are all managed for you, and of the two mechanisms compared here it is the only one that encrypts the client→proxy hop while still caching. DNS is cheaper at any throughput and simpler for static fleets and CRT clients, but the caching path is cleartext — see [What a Cleartext Hop Exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes). [VPC Encryption Controls](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-encryption-controls.html) can mitigate this: in monitor or enforce mode, Nitro hardware encrypts traffic between instances even when the application layer is cleartext, so the HTTP hop is protected from network-level observers without changing the proxy configuration. Choose DNS when cost matters more than managed health checking and automatic registration, and either VPC Encryption Controls is active or the cleartext exposure is acceptable for your network.

**A third option, self-managed.** Running HAProxy in front of the fleet also encrypts the client hop while caching, and adds routing by object key and byte range so concurrent readers of the same bytes converge on one instance. On a client host it removes the load balancer from the data path entirely, which is the only way to avoid the per-GB charge above while keeping an encrypted hop. The cost is that you operate it. See [Request-Aware Routing](REQUEST_AWARE_ROUTING.md), which also explains why putting it *behind* an NLB on AWS keeps the charge it was meant to avoid.

## Origin configuration

### Cross-region S3

**Network path to the origin Region.** An S3 gateway endpoint reaches only its own Region's S3, so it cannot carry misses to a bucket in another Region — the free default path does not apply and the egress route has to be chosen deliberately. Cost ordering also works differently behind a cache than it does for direct clients: only misses leave the Region, so per-GB charges scale with the miss rate while hourly charges are paid in full. At a high hit ratio the fixed costs dominate and the per-GB differences between patterns compress.

**Recommended: cross-region PrivateLink.** Create an S3 interface endpoint in the proxies' VPC targeting the bucket's Region, so misses leave through PrivateLink with no VPC peering, Transit Gateway attachment, or internet gateway in the path. The endpoint reaches S3 and nothing else, an endpoint policy can restrict which buckets, and adding another origin Region means adding an endpoint rather than reworking routing. Three proxy-side requirements:

- **Give the proxy the ENI IPs directly, in `endpoint_overrides`.** The proxy resolves upstream S3 through `connection_pool.dns_servers`, which defaults to Google and Cloudflare — those return public S3 addresses and bypass the endpoint entirely. Map the S3 hostnames to the endpoint's ENI IPs instead, which takes DNS out of the path completely; see [S3 PrivateLink](CONFIGURATION.md#s3-privatelink-interface-vpc-endpoints). The proxy rewrites only the URI authority, keeping TLS SNI and the `Host` header as the original hostname, so certificate validation and the client's SigV4 signature are both unaffected. Setting any override also pins outbound TLS to 1.2 fleet-wide, which interface endpoints require in any case. ENI IPs are stable for the endpoint's lifetime; adding a subnet later is a config edit plus a restart, since `config.yaml` is read at startup.

- **Do not enable private DNS on the endpoint.** It resolves only inside the VPC holding the endpoint, and it works by creating a private hosted zone for `s3.<region>.amazonaws.com` — the same name clients must resolve to the proxy fleet ([Client routing](#client-routing)). The two collide, and where a zone for that name already exists AWS rejects the endpoint with a conflicting-domain error. This is also why pointing `dns_servers` at the VPC resolver or a Route 53 Resolver inbound endpoint does not work here: both answer from the associated hosted zone and hand the proxy back its own address.

- **Monitor for excluded IPs.** Static overrides have no DNS refresh, so an ENI IP excluded by health tracking stays excluded until the proxy restarts. With only two or three ENIs, a transient fault can exclude all of them, at which point the proxy falls back to hostname-based routing — public S3 addresses, which are not routable in a VPC whose only egress is the endpoint. Watch the health endpoint for excluded IPs rather than assuming the fallback covers you.

**Alternatives**, worth taking when the routing already exists — [Cost effective methods for accessing S3 buckets cross-region](https://repost.aws/articles/ARjzluyMS8RbeOOK4MGXRG6Q/cost-effective-methods-for-accessing-s3-buckets-cross-region) compares seven patterns with worked costs:

- **Interface endpoints reached over VPC peering or Transit Gateway** carry the same three proxy-side requirements as above. Peering is subject to per-VPC connection quotas; Transit Gateway adds per-attachment hourly and per-GB processing charges on top.
- **NAT gateway** is the simplest and already present in many VPCs, and the most expensive per GB — data processing on top of inter-Region transfer. It is also where the cache pays back hardest, since only misses are processed.
- **Egress-only internet gateway with IPv6** is the cheapest pattern in that comparison but is not a supported configuration here. The proxy's resolver prefers IPv4 when a hostname publishes both record types, so on an IPv6-only egress path it would select an A record it cannot route to. Dualstack hostnames also change what the client signs and occupy a separate cache-key namespace from the non-dualstack name.
- **S3 Multi-Region Access Points** require clients to sign for the MRAP hostname with SigV4a and address a global endpoint (`accesspoint.s3-global.amazonaws.com`). The proxy handles both; configure the MRAP endpoint with a `com.amazonaws.s3-global.accesspoint` interface endpoint and list its ENI IPs in `endpoint_overrides`.

**Tuning:**

- **Address the bucket by its home-region hostname.** A wrong-region endpoint makes S3 redirect rather than fail outright, so the symptom is degraded throughput rather than an error. Check this first.
- **Leave `connection_pool.tcp_recv_buffer_size` unset** so the kernel auto-tunes the TCP receive window; pinning `SO_RCVBUF` caps throughput at the bandwidth-delay product on a high-RTT path.
- **Keep `get_ttl` long** (the default is effectively infinite), shortening only for prefixes that mutate via [`cache_rules.json`](CONFIGURATION.md#cache-rules).

### S3-compatible store outside AWS

Nothing in the proxy is specific to Amazon S3. Because the Direct Connect or VPN link is fixed capacity shared with everything else crossing it, each hit returns bandwidth to other workloads as well as saving time.

- **Declare the origin's transport** with `connection_pool.upstream_overrides`, preferring validated HTTPS on a non-standard port, which waives no protection. See [Upstream Transport Overrides](CONFIGURATION.md#upstream-transport-overrides).
- **Point `dns_servers` at a resolver that can see the origin.** The defaults (Google and Cloudflare) will not resolve an internal name.
- **Private-IP origins need no special configuration** on the caching path. The [destination policy](ARCHITECTURE.md#destination-policy-ssrf-protection) that rejects private ranges applies to the `CONNECT` passthrough and TLS-listener paths; if clients use those, add the IPs to `endpoint_overrides` or `server.tls.connect_allowlist`.
- **Give each store a distinct hostname.** Cache keys are port-stripped, so two stores on one host share a key namespace.
- **Warm the cache before a workload runs.** Misses run at link speed, and the proxy holds no credentials and cannot prefetch, so warming means requesting the dataset through it once.

## Mount and configuration

Two mount properties are correctness requirements, and neither reports an error when
absent: `lookupcache=pos`, without which instances cache negative lookups and cannot see
files written by their peers (a 40%+ miss rate on repeat downloads), and working
cross-host file locking, without which instances hold the same coordination lock
simultaneously and evict or consolidate against each other.

Neither appears in AWS's recommended options for EFS or for FSx, so add both. The rest of
the option list differs by file system — `noresvport` is required on EFS and rejected by
FSx, `nconnect` is recommended on FSx and neither supported nor needed on EFS — so take
the line for the file system you actually run. Both lines, the per-option comparison, and
the verification steps are in
[NFS Mount Requirements](CONFIGURATION.md#nfs-mount-requirements).

A silently-failed mount falls through to the root filesystem and cache writes go to local
disk; verify with `df -h`.

Settings that differ from defaults:

```yaml
cache:
  cache_dir: "/mnt/fsx/cache"
  max_cache_size: 53687091200        # must fit the file system with headroom
  max_ram_cache_size: 1073741824     # 1 GB per instance

logging:
  access_log_dir: "/mnt/efs/logs/access"
  app_log_dir: "/mnt/efs/logs/app"
```

Shared-storage coordination is always active and needs no enabling. For analytics-style small reads inside large objects, consider [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching).

## Verification

```bash
curl -s http://<proxy-ip>:8080/health                          # each proxy healthy
mount | grep -E 'fsx|efs'                                      # confirm lookupcache=pos and nfsvers=4.1, and that nolock/local_lock are absent
curl -s http://<proxy-ip>:9090/metrics | grep cache_hit_rate_percent   # hit rate rising
```

Confirm the cache is genuinely shared, which a health check cannot tell you: request an object through one proxy, then the same object pinned to a different proxy, and expect a hit. A miss points at a mount missing `lookupcache=pos`.

Confirm locking separately, since a mount can pass the shared-cache check above and still not enforce locks between hosts:

```bash
flock -x /mnt/fsx/.locktest -c 'sleep 30'                              # on one instance, hold the lock
flock -n -x /mnt/fsx/.locktest -c true && echo BROKEN || echo OK       # on a second instance, must print OK
```

`BROKEN` means locks are host-local and multi-instance coordination is unsafe; fix the mount options before running more than one instance against the volume.

## Monitoring

Send the proxy's own metrics to CloudWatch by enabling OTLP export against the CloudWatch agent's OTLP receiver (agent v1.300060.0 or later) — see [OTLP Metrics — CloudWatch](OTLP_METRICS.md#cloudwatch) for the agent config. The agent needs `CloudWatchAgentServerPolicy` on the instance profile. Cache hit rate and per-bucket traffic are also on the [dashboard](DASHBOARD.md) and `/metrics` without any of this; see [Metrics](METRICS.md).

Alarm on the infrastructure metrics that confirm the sizing decisions above:

| Metric | Namespace | Use |
|---|---|---|
| `FileServerCacheHitRatio` | `AWS/FSx` | Share of reads served by the file server's cache. A falling ratio means more reads are reaching disk and losing the cached-read multiple. |
| `NetworkThroughputUtilization` | `AWS/FSx` | Percentage of the provisioned network limit. Consistently low means you can drop a throughput tier. |
| `FileServerDiskIopsUtilization` | `AWS/FSx` | Confirms the included 3 IOPS/GiB is sufficient before provisioning more. |
| `FileServerDiskThroughputBalance`, `FileServerDiskIopsBalance` | `AWS/FSx` | Burst credit balance. Watch these on the low tiers; a balance trending to zero means the workload is sustained, not spiky. |
| `MeteredIOBytes` vs `PermittedThroughput` | `AWS/EFS` | Throughput utilization on EFS. `MeteredIOBytes` is also the Elastic Throughput billing signal, so it is what the break-even above is denominated in. |
| `PercentIOLimit` | `AWS/EFS` | Approach to the General Purpose I/O limit. |
| `UnHealthyHostCount`, `TCP_Target_Reset_Count` | `AWS/NetworkELB` | Instances failing health checks, and connections reset — the latter rises if the idle timeouts are mismatched. |

Also watch validation scan warnings in `journalctl -u s3-proxy`, and origin egress on the bill or link graphs. For recovery after cache corruption, see [Error Handling](ERROR_HANDLING.md).
