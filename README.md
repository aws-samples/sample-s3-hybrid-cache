# Hybrid Cache for Amazon S3

A transparent S3 caching proxy with RAM and shared-disk cache tiers, streamed responses, S3-style access logs, and OTLP metrics export.

Read the introductory blog post: [Introducing Hybrid Cache for Amazon S3: An Intelligent Caching Proxy](https://repost.aws/articles/ARaWiUYarLQSanDgHfMAfbsA)

>Important: Hybrid Cache for Amazon S3 is sample code provided for demonstration and educational purposes. It is not intended for production use without thorough testing, security review, and validation for your specific use case and environment. You are responsible for evaluating whether this solution meets your requirements before deploying it.

## The problem

Users with hybrid or on-premises workloads find Amazon S3 slow or costly to use because of latency, bandwidth constraints, and data transfer out (DTO) costs, especially for repeat-read and read-after-write workflows.

This is particularly acute in healthcare, life sciences, quantitative trading, and autonomous driving, where large data volumes require frequent access with low latency and high throughput. Existing caching solutions don't support transparent authentication or shared storage, forcing users to either:
- Accept repeated DTO costs when multiple users access the same data
- Configure caches with their own credentials, breaking IAM policies and S3 access controls
- Run multiple independent caches that each pull the same data from S3

## The solution

Hybrid Cache for Amazon S3 provides a caching layer that serves repeat reads locally and cuts data transfer costs. Users keep S3's full capabilities and supply only the on-premises compute and storage they choose.

**Key Differentiators**

- **Transparent Authentication**: No cache credentials. The proxy reuses client request signatures, so S3 policies and IAM remain the sole authentication mechanism. By adding conditional headers, the proxy can authenticate every request against S3 without data transfer, or serve cache hits immediately. You define the Time To Live (TTL) for cached HEAD and GET requests.

- **Shared File Storage**: Multiple cache servers share cached data via existing on-premises file storage (e.g., NFS), eliminating redundant S3 pulls. Users allocate capacity from gigabytes to petabytes using existing hardware.

- **Stateless Proxies**: Any proxy can serve any request, so high availability comes from client routing, with no coordination between proxies. Clients point `HTTP_PROXY` at the TLS proxy listener, on [a name of your own](docs/GETTING_STARTED.md#fleet-behind-one-proxy-name) that resolves to every proxy or on a [Layer 4 load balancer](docs/GETTING_STARTED.md#option-b-layer-4-load-balancer). The client hop is encrypted and responses are still cached. Where the network segment between clients and proxies is secure and cleartext on it is acceptable, a DNS zone that resolves S3's own hostnames to the proxies is a simpler alternative. It mirrors S3's [multi-value answer DNS resolution](https://docs.aws.amazon.com/general/latest/gr/s3.html#multivalue-answer-dns-routing). [Choosing between DNS and NLB](docs/AWS_DEPLOYMENT.md#choosing-between-dns-and-nlb) compares the options on distribution, health checking, failover and cost.

## Benefits

**Accelerate Repeated Requests**
- Cache frequently accessed objects to eliminate redundant S3 round-trips
- Read-after-write consistency through the write cache: objects are available from cache as soon as the upload completes
- RAM caching serves hot data and metadata, including HEAD responses, from memory
- Cached HEAD responses answer metadata lookups from tools such as Mountpoint for Amazon S3 without an S3 round trip
- Hedged upstream requests: opt in per key pattern to race a second fetch against a slow original on cache misses, and serve whichever returns first. It suits workloads sensitive to upstream tail latency such as analytics column-chunk reads. Off by default
- Download bandwidth QoS: cap aggregate cache-miss download bandwidth and distribute it fairly among callers (by User-Agent app ID) or buckets with deficit round-robin scheduling. Disabled by default
- Large files stream through without being buffered whole in memory
- HTTPS passthrough for clients that cannot be configured for HTTP (only HTTP requests use the cache)
- Optional TLS proxy listener for encrypted client-to-proxy traffic with full caching via `HTTP_PROXY`

**Reduce Data Transfer Costs**
- Serve cached content locally instead of fetching from S3 repeatedly
- Download coordination coalesces concurrent requests for the same uncached resource: one request fetches from S3 while the others wait, then all serve from cache
- Content-aware LZ4 compression stores compressible content compressed and skips formats that are already compressed
- TinyLFU-like eviction keeps data that is read often ahead of data read once
- Separate TTLs for HEAD and GET requests. Expired cached objects are automatically revalidated with S3 using If-None-Match, avoiding re-download when unchanged
- Choose lazy expiration for fixed-capacity storage or active expiration for elastic storage

**Unified Range Storage**
- All cached data is stored in one format, so full objects, byte ranges, and multipart parts are interchangeable
- Upload via multipart, then download as a full object or byte ranges, all served from cache
- Request part 5 of a multipart object, then an overlapping byte range, and the cache serves both
- Partial cache hits fetch only missing bytes from S3, merging with cached data
- Resumable downloads: if a transfer is interrupted, the client can resume and the proxy serves already-cached ranges locally while fetching only the remainder from S3

**Configure Cache Behavior Per Key Pattern**
- Set TTLs, read/write caching, compression, RAM-cache eligibility, page-aligned range caching, hedged upstream requests, and local conditional-request evaluation for keys matching a glob pattern
- Update `cache_rules.json` without restarting the proxy

**Designed for On-Premises Deployments**
- Run a fleet of proxies on one cache volume, such as an NFS export, for high availability
- Scale out by adding proxies; they coordinate through the cache volume
- No single point of failure: any proxy can serve cached content
- Stateless proxies: proxies never talk to each other, and all coordination goes through the cache volume, so any proxy can be replaced
- Journal-based metadata updates: each proxy writes cache updates to its own journal file, and a background consolidator merges them atomically, so proxies do not race on the cache volume
- Flexible cache storage: a single proxy with local disk may suit a hypervisor platform that provides high availability. A fleet uses any NFS-compatible file system: a dedicated NAS appliance, a file server VM, or file services built into a hypervisor platform

**Also Runs on AWS**

The same proxy caches a cross-Region Amazon S3 bucket or an S3-compatible store outside AWS. Deploy a fleet of EC2 instances sharing an FSx for OpenZFS, FSx for NetApp ONTAP or EFS cache volume, point clients at the TLS proxy listener directly or through an NLB. Cache hits then avoid the cross-Region round-trip and per-GB transfer charge entirely. See the **[AWS Deployment Guide](docs/AWS_DEPLOYMENT.md)** for sizing, bootstrap scripts, client routing patterns, and cost modeling.

Where every client host can run a sidecar [affinity router](docs/REQUEST_AWARE_ROUTING.md), a cheaper option opens up: give each proxy its own local NVMe instance store. The router hashes each request to one proxy by object key and page index, so the private disks behave as one cache, at about 39% lower cost than a capacity-matched cache volume. The cache is erased when an instance is replaced. See **[Local NVMe Cache Fleets](docs/LOCAL_NVME_CACHE.md)**.

## Documentation

The [documentation index](docs/README.md) lists every guide and reference page. Start with:

- [Getting started](docs/GETTING_STARTED.md): install the proxy, run it, and route clients to it
- [Configuration reference](docs/CONFIGURATION.md): every configuration field, with its default
- [Security](SECURITY.md): the security model, and the risks it leaves with you

## Quick start

The quickest way to try the proxy is to run it on localhost in `proxy_only` mode and point clients at it with `HTTP_PROXY`. No sudo, no DNS changes, no hosts file edits, no TLS: the client-to-proxy hop is loopback.

```bash
# Option 1: download a prebuilt binary (Linux, x86_64 or aarch64, glibc 2.34 or newer).
# From https://github.com/aws-samples/sample-s3-hybrid-cache/releases/latest
# download s3-proxy-x86_64-linux or s3-proxy-aarch64-linux, plus SHA256SUMS, into an empty directory.
sha256sum -c --ignore-missing SHA256SUMS
mv s3-proxy-x86_64-linux s3-proxy   # or s3-proxy-aarch64-linux
chmod +x s3-proxy
mkdir -p config
# Option 2: clone and build from source
git clone https://github.com/aws-samples/sample-s3-hybrid-cache
cd sample-s3-hybrid-cache
cargo build --release

# Write a minimal proxy_only config
cat > ./config/quickstart.yaml <<'EOF'
server:
  mode: "proxy_only"
  proxy_port: 3128
cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 524288000  # 500 MiB
logging:
  access_log_dir: "./tmp/logs/access"
  app_log_dir: "./tmp/logs/app"
EOF

# Start the proxy on port 3128 (no sudo required). It listens on all interfaces,
# so block 3128 at the host firewall if other hosts can reach this machine.
# Option 1: ./s3-proxy -c ./config/quickstart.yaml
# Option 2:
cargo run --release -- -c ./config/quickstart.yaml

# In another shell, route the AWS CLI through the proxy
export HTTP_PROXY=http://127.0.0.1:3128
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

`HTTP_PROXY` selects the proxy connection. `--endpoint-url http://s3.<region>.amazonaws.com` selects a cacheable HTTP S3 endpoint; otherwise, the AWS CLI defaults to HTTPS and sends a CONNECT tunnel that the proxy cannot cache. The S3 hostname remains the request's signed `Host` value, so do not use the proxy hostname as the endpoint URL. SigV4 authenticates the HTTP request, but does not encrypt the client-to-proxy hop. That is acceptable here because this quick start runs the proxy on `127.0.0.1`, where traffic never leaves the host. Once the proxy is on a different host, use the TLS proxy listener (`HTTP_PROXY=https://<proxy-host>:3129`). See [What a cleartext hop exposes](docs/ARCHITECTURE.md#what-a-cleartext-hop-exposes).

**Tip**: Set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` to avoid passing `--endpoint-url` on every S3 command for buckets in that Region.

**On EC2 (or any host with IPs you don't want proxied)**, also set `NO_PROXY=169.254.169.254` so IMDS credential retrieval bypasses the proxy. Add other hosts to `NO_PROXY` as needed.

**Beyond one host**: for clients on other hosts, enable the TLS proxy listener and set `HTTP_PROXY=https://<proxy-host>:3129`, so the client hop is encrypted and responses are still cached. Where the network segment between clients and proxies is secure and cleartext on it is acceptable, a DNS zone that resolves S3's hostnames to the proxies is a simpler alternative. [Configure client routing](docs/GETTING_STARTED.md#3-configure-client-routing) covers every option.

**Deployment**: the prebuilt release binary, or `target/release/s3-proxy` from a build, is a single portable binary for the same architecture and Linux, with glibc 2.34 or newer for the release binary or at least the build host's for your own build ([Binary portability](docs/GETTING_STARTED.md#binary-portability)). Upgrading replaces the binary and restarts the proxy; existing configuration keeps working. Before upgrading a fleet to 3.0.0, read [Upgrading to 3.0.0](docs/UPGRADING.md#upgrading-to-300). A fleet on a release older than 2.8 first upgrades to a 2.8 release.

**Next steps**: [Getting started](docs/GETTING_STARTED.md) covers installation and configuration in full.

## Architecture

```text
 ┌──────────────────────┐  ┌───────────────────────┐  ┌─────────────────────┐
 │  S3 Client (HTTP)    │  │ S3 Client (HTTP_PROXY)│  │  S3 Client (HTTPS)  │
 │  - DNS/hosts routing │  │ - HTTP_PROXY=https:// │  │  - Default HTTPS    │
 │  - AWS CLI / SDK     │  │ - AWS CLI / SDK       │  │  - AWS CLI / SDK    │
 └──────────┬───────────┘  └──────────┬────────────┘  └──────────┬──────────┘
            │                         │                          │
            ▼                         ▼                          ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                    Hybrid Cache for Amazon S3 (1..N)                       │
│                                                                            │
│  ┌────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐  │
│  │ HTTP (Port 80)     │ │ TLS Proxy (Port 3129)│ │ HTTPS (Port 443)     │  │
│  │ - Caching          │ │ - TLS Termination    │ │ - TCP Passthrough    │  │
│  │ - Range Merging    │ │ - Caching            │ │ - No Caching         │  │
│  │ - Streaming        │ │ - Range Merging      │ │ - Direct to S3       │  │
│  └────────┬───────────┘ └──────────┬───────────┘ └──────────┬───────────┘  │
│           │                        │                        │              │
│           └────────────┬───────────┘                        │              │
│                        ▼                                    │              │
│           ┌───────────────────────────┐                     │              │
│           │        RAM Cache          │                     │              │
│           │  - Metadata + ranges      │                     │              │
│           │  - Compression            │                     │              │
│           │  - Eviction               │                     │              │
│           └─────────────┬─────────────┘                     │              │
└─────────────────────────┼───────────────────────────────────┼──────────────┘
                          │                                   │
                          ▼                                   │
           ┌───────────────────────────────┐                  │
           │   Shared Disk Cache (NFS)     │                  │
           │  - Metadata + ranges          │                  │
           │  - Compression                │                  │
           │  - TinyLFU-like eviction      │                  │
           │  - Fixed or elastic size      │                  │
           │  - Journaled writes           │                  │
           └───────────────┬───────────────┘                  │
                           │                                  │
                           └────────────────┬─────────────────┘
                                            │
                                            ▼
                              ┌─────────────────────────┐
                              │    Amazon S3 (HTTPS)    │
                              └─────────────────────────┘
```

Routing methods:
- **HTTP_PROXY (recommended)**: set `HTTP_PROXY=https://<proxy-host>:3129` to reach the TLS proxy listener, so traffic is encrypted and still cached. Only the AWS CRT transfer client verifies the proxy's certificate; botocore-based clients encrypt the hop without verifying it ([which clients verify](docs/GETTING_STARTED.md#configuring-clients-to-trust-the-certificate)). S3's DNS names stay unchanged. For a fleet, point `HTTP_PROXY` at [a name of your own with one address record per proxy](docs/GETTING_STARTED.md#fleet-behind-one-proxy-name), or at a [Layer 4 load balancer](docs/GETTING_STARTED.md#option-b-layer-4-load-balancer).
- **DNS routing**: resolve S3 hostnames to the proxies with a DNS zone or `/etc/hosts`, with multi-value DNS for a fleet. Clients need no proxy setting. Requests to port 80 are cleartext, so use DNS routing where the network segment between clients and proxies is secure and cleartext on it is acceptable.

Endpoints:
- **HTTP listener (port 80)**: full caching
- **HTTPS passthrough (port 443)**: no caching
- **TLS proxy listener (port 3129)**: TLS termination with full caching, for `HTTP_PROXY` clients
- **Health**: `localhost:8080/health`
- **Metrics**: `localhost:9090/metrics`
- **Dashboard**: `localhost:8081`

## Security

> **Your Responsibility**: You are responsible for restricting network access to the proxy to only clients authorized to access all objects that may be cached, and for securing file system access to the cache volume. This is the same security model as any shared cache. The proxy does not weaken S3's security, but depending on [TTL configuration](docs/CONFIGURATION.md#time-to-live-ttl-configuration), cached data may be accessible without S3 authorization checks.

**Traffic in transit**: SigV4 authenticates every request but provides no confidentiality, so any hop carrying plain HTTP exposes the object key, the caller's access key ID, and the object payload to anything that can observe it. A captured request can be replayed against S3. The affected hops are the HTTP listener (`:80`) and forward proxy (`:3128`) when reached across a network, a load balancer that terminates TLS and forwards cleartext to the proxy, and a `scheme: http` [upstream override](docs/CONFIGURATION.md#upstream-transport-overrides). Each has an encrypted alternative that keeps full caching. On the TLS proxy listener only the AWS CRT transfer client verifies the proxy's certificate, so botocore-based clients are protected against passive observation but not impersonation ([which clients verify](docs/GETTING_STARTED.md#configuring-clients-to-trust-the-certificate)). See [What a cleartext hop exposes](docs/ARCHITECTURE.md#what-a-cleartext-hop-exposes) for what is captured, what is not, and the replay windows.

**Network access**: With a [TTL](docs/CACHE_FRESHNESS.md#time-to-live-ttl-configuration) above zero, cache hits are served without contacting S3, so any client that can reach the proxy can read any cached object. Restrict proxy access using security groups, firewalls, or network segmentation. [Listening surface](SECURITY.md#listening-surface) lists every port and when it is open. The monitoring listeners (8080, 8081, 9090) are unauthenticated and bind to `127.0.0.1` by default. `config/config.example.yaml` sets all three to `0.0.0.0` so a load balancer or remote scraper can reach them. Keep each `bind_address` at `127.0.0.1` unless another host needs the port, and restrict any monitoring port reachable beyond localhost with the same firewall, security group, or reverse proxy rules as the data ports.

**Outbound destination restrictions**: The proxy restricts CONNECT tunnels (TLS proxy listener) and SNI passthrough (HTTPS listener) to port 443 only. Destinations whose resolved addresses all fall in unspecified (`0.0.0.0/8`), link-local (`169.254.0.0/16`), loopback (`127.0.0.0/8`), or private (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`) ranges are rejected, as are their IPv6 equivalents (`::`, `::1`, `fe80::/10`, `fc00::/7`). This prevents clients from using the proxy as a relay to IMDS, internal services, or arbitrary network destinations. IPs listed in `endpoint_overrides` are exempt from the IP-range restriction, allowing PrivateLink ENIs, on-premises object stores, and other explicitly configured endpoints to function normally. An optional `server.tls.connect_allowlist` can further restrict allowed hostnames via glob patterns (e.g., `*.amazonaws.com`); when configured, only hostnames matching a pattern are permitted.

**Cache storage access**: Restrict file system access to the cache volume to the proxies. The proxy stores cached data unencrypted (LZ4 compressed), so encryption at rest comes from the cache volume: enable EFS or FSx encryption at rest, EBS volume encryption, or block-device encryption such as dm-crypt.

**Per-request authorization (TTL=0)**: For environments requiring per-request IAM authorization, [set TTL to zero](docs/ARCHITECTURE.md#shared-cache-access-model). Every request revalidates with S3 via conditional headers, saving bandwidth on 304 responses, with full IAM enforcement on every access.

**HTTPS**: HTTPS passthrough (port 443) relays encrypted bytes to S3 without caching ([architecture](#architecture)). For encrypted client-to-proxy traffic with caching, use the TLS proxy listener (port 3129) with `HTTP_PROXY`. AWS CLI and SDKs use HTTPS by default, so all requests are authenticated by S3 unless clients explicitly opt into HTTP endpoints or `HTTP_PROXY` routing for caching. By default all proxy-to-S3 communication uses verified TLS regardless of client connection protocol. This secure default can be changed per destination via [`upstream_overrides`](docs/CONFIGURATION.md#upstream-transport-overrides), whose protection-waiving modes (plaintext HTTP, or HTTPS with certificate validation disabled) are intended for local development or secure network segments only. Outbound TLS to hosts matching an entry in `endpoint_overrides` is locked to TLS 1.2 (for compatibility with endpoints that do not support TLS 1.3). Outbound TLS to all other hosts negotiates TLS 1.2 or TLS 1.3, using the highest version mutually supported by the proxy and the remote server.

[Security](SECURITY.md) states the security model and its accepted risks, and [Security considerations](docs/ARCHITECTURE.md#security-considerations) covers the shared cache access model, deployment guidelines and appropriate use cases.

## Performance

### Single-client throughput testing

Single client downloading large files (up to 5 GiB) using AWS CLI with [CRT](https://aws.amazon.com/blogs/storage/improving-amazon-s3-throughput-for-the-aws-cli-and-boto3-with-the-aws-common-runtime/), 8 proxies (m6in.2xlarge), with FSx for OpenZFS (10 GiB/s) as the cache volume. All tests are cross-Region (S3 bucket in eu-west-1, proxies and clients in us-west-2). For more on this subject see [Cost effective methods for accessing S3 buckets cross-region](https://repost.aws/articles/ARjzluyMS8RbeOOK4MGXRG6Q/cost-effective-methods-for-accessing-s3-buckets-cross-region).

**m6in.4xlarge client (up to 50 Gbps)**

| Scenario | Throughput |
|----------|------------|
| Direct S3 (no proxy) | ~2.9 GiB/s |
| Cache miss (proxy → S3) | ~1.7 GiB/s |
| Cache hit (proxy → FSx) | ~3.4 GiB/s |

**m6in.16xlarge client (100 Gbps)**

| Scenario | Throughput |
|----------|------------|
| Direct S3 (no proxy) | ~5.0 GiB/s |
| Cache miss (proxy → S3) | ~2.0 GiB/s |
| Cache hit (proxy → FSx) | ~5.5 GiB/s |

Cache hit throughput exceeded direct S3 throughput. Cache miss throughput is proxy-limited (S3 fetch + TLS + LZ4 compression + cache write). Compared to a 3-proxy configuration (which achieved ~1.1 GiB/s cache miss and ~2.4 GiB/s cache hit), scaling to 8 proxies approximately doubled throughput.

See [this article](https://repost.aws/articles/ARRnpZ4QYrS9CtnYyTleS5bg/storage-benchmarking-downloading-deepseek-v3-0324-from-amazon-s3-to-local-storage-at-8-1-gib-s) for guidance on configuring the AWS CLI CRT transfer client for high throughput.

### Multi-client request time and efficiency testing

100 clients (c7gn.large, 25 Gbps) each sequentially downloading the same 100 files (0.1–100 MB), i.e. 'thundering herd'. 3 proxies (m6in.2xlarge) sharing an EFS cache volume. Cross-Region: eu-west-1 bucket, us-west-2 clients and proxies (~116 ms RTT to S3). "Request time" is the **total per-file download time** (request start to last byte received), not time-to-first-byte.

| Scenario | p50 | p95 | p99 | Aggregate throughput | Errors |
|----------|-----|-----|-----|----------------------|--------|
| Direct to S3 (no proxy) | 1,736 ms | 4,247 ms | 4,929 ms | 3.8 Gbps | 0 |
| Via proxy (cache starts cold) | 570 ms | 1,373 ms | 2,433 ms | 17.1 Gbps | 0 |

Against a cross-Region bucket the proxy is 3× faster at p50 and delivers 4.5× the aggregate throughput of direct S3 access, with zero errors across 10,000 requests per run. The download coordinator collapses the 100 concurrent first-touch requests for each file into a single S3 fetch, and the other 99 wait and serve from the freshly cached copy. Every subsequent read is served from the sharded RAM/disk cache without the ~116 ms cross-Region round-trip. Even though the cache starts empty, only ~1% of requests (one per file) actually reach S3; the rest are cache hits.

### Single-proxy bandwidth ceiling

One proxy and one client (both `c6in.16xlarge`, 100 Gbps NIC), same Availability Zone, intra-region bucket, 64 MiB objects over a boto3 connection sweep (1–200 connections). This isolates the per-proxy ceiling for each cache tier.

| Scenario | Best avg throughput |
|----------|---------------------|
| Direct S3 (no proxy) | 4.6 GiB/s |
| Cache miss (proxy → S3 → client + disk write) | 3.6 GiB/s |
| Cache hit, disk (io2, 100K IOPS) | 4.2 GiB/s |
| Cache hit, RAM | 7.1 GiB/s |

RAM cache-hit throughput scales approximately linearly with connection count (0.31 GiB/s at 1 connection, 2.5 GiB/s at 10, and 7.1 GiB/s at 50). It then tapers as the single client's NIC saturates while the proxy still has headroom. RAM hits use a sharded cache, zero-copy `Arc<Bytes>` reads, and shared read locks, so concurrent hits do not serialize.

Cache-miss throughput is proxy-limited (S3 fetch + TLS + LZ4 compression + cache write). Disk cache hits scale well with concurrency and are not the bottleneck at this scale.

## FAQ

**Q: Why HTTP instead of HTTPS for caching?**

A: The HTTP listener (port 80) uses plaintext HTTP so the proxy can read and cache request/response content. HTTPS passthrough (port 443) relays encrypted bytes because the proxy cannot present a trusted certificate for the S3 endpoint; it can only relay encrypted bytes. For encrypted client-to-proxy traffic with caching, the optional TLS proxy listener (port 3129) terminates TLS using the proxy's own certificate, then processes decrypted HTTP through the caching pipeline. `HTTP_PROXY=https://proxy:3129` selects that proxy connection; `--endpoint-url http://s3.<region>.amazonaws.com` selects the cacheable HTTP S3 endpoint. Without the HTTP endpoint, the client defaults to HTTPS and sends a CONNECT tunnel that bypasses the cache. The S3 hostname remains the request's signed `Host` value. By default all proxy-to-S3 communication uses verified TLS (HTTPS) regardless of client connection protocol, except where a destination is configured with a protection-waiving [`upstream_overrides`](docs/CONFIGURATION.md#upstream-transport-overrides) mode (plaintext HTTP, or HTTPS with certificate validation disabled). These modes are intended for local development or secure network segments only.

**Q: How does load balancing and failover work?**

A: Clients point `HTTP_PROXY` at the TLS proxy listener through [a name of your own](docs/GETTING_STARTED.md#fleet-behind-one-proxy-name) with one address record per proxy, or through a [Layer 4 load balancer](docs/GETTING_STARTED.md#option-b-layer-4-load-balancer). With a name, the resolver spreads clients across the proxies and a failed proxy's record is withdrawn; a load balancer health-checks the proxies and balances per connection. Where the network segment between clients and proxies is secure and cleartext on it is acceptable, a DNS zone can resolve S3's own hostnames to the proxies with multi-value answer routing. This matches how [S3 itself resolves DNS queries](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-s3-multivalue-answer-response-dns-queries/). The AWS Common Runtime (CRT) transfer client, in AWS CLI v2 and current SDKs, then spreads requests across every resolved address and retries another when a connection fails. [Comparing the fleet routing options](docs/GETTING_STARTED.md#comparing-the-fleet-routing-options) sets the three side by side.

**Q: Can the cache preload data?**

A: As the cache is transparent and has no credentials of its own, it cannot request data from S3 without a client request. Users can warm the cache by requesting datasets upfront.

**Q: Will this work with S3-compatible storage?**

A: Nothing about this solution is specific to Amazon S3. Any origin compatible with the S3 API should work.

**Q: Does S3 Transfer Acceleration work with the proxy?**

A: The proxy caches accelerate requests correctly once they reach it. Send accelerate traffic to the proxy with the options in [Configure client routing](docs/GETTING_STARTED.md#3-configure-client-routing). Route 53 does not host a private zone for `s3-accelerate.amazonaws.com`, so the DNS zone option needs a VPC-level resolver (CoreDNS, Unbound) for that name specifically. For most deployments, targeting regional virtual-hosted endpoints (`<bucket>.s3.<region>.amazonaws.com`) is the easier path: they reuse the existing regional DNS routing and share cache entries with accelerate access for the same object. See [S3 Transfer Acceleration](docs/GETTING_STARTED.md#s3-transfer-acceleration).

## Author

Ed Gummett, Storage Specialist Solutions Architect, AWS. [Connect on LinkedIn](https://www.linkedin.com/in/egummett/)

## Contributing

See the [developer guide](docs/DEVELOPER.md) for implementation details and development setup. We welcome feedback, bug reports, and feature requests via [issues](https://github.com/aws-samples/sample-s3-hybrid-cache/issues) and [pull requests](https://github.com/aws-samples/sample-s3-hybrid-cache/pulls).

## License

See [LICENSE](LICENSE) for licensing information.
