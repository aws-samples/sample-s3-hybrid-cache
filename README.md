# S3 Hybrid Cache - Intelligent, Transparent Caching for Amazon S3

A high-performance, transparent S3 caching proxy with intelligent multi-tier caching, streaming architecture, and comprehensive observability. 

📖 Read the introductory blog post: [Introducing S3 Hybrid Cache: An Intelligent Caching Proxy for Amazon S3](https://repost.aws/articles/ARaWiUYarLQSanDgHfMAfbsA)

## The Problem

Users with hybrid or on-premises workloads face challenges leveraging Amazon S3 due to latency, bandwidth constraints, and data transfer out (DTO) costs—especially for repeat-read and read-after-write workflows.

This is particularly acute in healthcare, life sciences, quantitative trading, and autonomous driving, where large data volumes require frequent access with low latency and high throughput. Existing caching solutions don't support transparent authentication or shared storage, forcing users to either:
- Accept repeated DTO costs when multiple users access the same data
- Configure caches with their own credentials, breaking IAM policies and S3 access controls
- Run multiple independent caches that each pull the same data from S3

## The Solution

S3 Hybrid Cache provides an intelligent caching layer that accelerates performance while minimizing data transfer costs. It allows users to retain S3's full capabilities while leveraging minimal, user-defined on-premises resources.

**Key Differentiators**

- **Transparent Authentication**: No cache credentials—reuses client request signatures, ensuring S3 policies and IAM remain the sole authentication mechanism. By intelligently injecting conditional headers, the proxy can authenticate every request against S3 without data transfer, or serve cache hits immediately - you define the Time To Live (TTL) for cached HEAD and GET requests.

- **Shared File Storage**: Multiple cache servers share cached data via existing on-premises file storage (e.g., NFS), eliminating redundant S3 pulls. Users allocate capacity from gigabytes to petabytes using existing hardware.

- **Stateless Proxies**: Simplifies high availability by mimicking [S3's multi-value answer DNS resolution](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-s3-multivalue-answer-response-dns-queries/). S3 clients using the AWS Common Runtime (CRT)—available as a transfer client in AWS CLI v2 and modern SDKs—automatically distribute requests across all resolved IPs and retry against alternate IPs on connection failure. No load balancer or client-side configuration required.

## Benefits

**Accelerate Repeated Requests**
- Cache frequently accessed objects to eliminate redundant S3 round-trips
- Read-after-write consistency through write-through caching—objects are immediately available from cache after upload completes
- RAM caching accelerates repeated access to hot data and metadata (including HEAD responses)
- HEAD response caching reduces metadata lookup latency for tools like Mountpoint for Amazon S3
- Streaming architecture for large files—no buffering or throughput degradation
- HTTPS passthrough for clients that cannot be configured for HTTP (only HTTP requests use the cache)
- Optional TLS proxy listener for encrypted client-to-proxy traffic with full caching via `HTTP_PROXY`

**Reduce Data Transfer Costs**
- Serve cached content locally instead of fetching from S3 repeatedly
- Download coordination coalesces concurrent requests for the same uncached resource — only one request fetches from S3 while others wait, then all serve from cache
- TinyLFU-like eviction algorithm optimizes cache retention for frequently accessed data
- Separate TTLs for HEAD and GET requests—expired cached objects are automatically revalidated with S3 using If-None-Match, avoiding re-download when unchanged

**Unified Range Storage**
- All cached data stored in a common format—full objects, byte ranges, and multipart parts are interchangeable
- Upload via multipart, download as full object or byte ranges—all served from cache
- Request part 5 of a multipart object, then request overlapping byte range—cache serves both
- Partial cache hits fetch only missing bytes from S3, merging with cached data
- Resumable downloads—if a transfer is interrupted, the client can resume and the proxy serves already-cached ranges locally while fetching only the remainder from S3

**Designed for On-Premises Deployments**
- Deploy multiple cache instances behind shared file storage (e.g., NFS) for high availability
- Horizontal scaling with coordinated cache access across instances
- No single point of failure—any instance can serve cached content
- Stateless instances—no direct communication between nodes; all coordination via shared storage makes instances ephemeral and replaceable
- Journal-based metadata updates—each instance writes cache updates to its own journal file, and a background consolidator merges them atomically, eliminating race conditions on shared storage without inter-node communication
- Content-aware LZ4 compression—2-10x space savings, automatically skips already-compressed formats
- Per-bucket and per-prefix cache settings—configure TTLs, read/write caching, compression, and RAM cache eligibility per bucket via JSON files, with hot reload
- Flexible expiration modes—lazy (fixed capacity) or active (elastic storage)
- Cache storage is flexible—a single proxy with local disk may be suitable on a hypervisor platform that provides high availability. Multi-proxy deployments use any NFS-compatible shared storage: a dedicated NAS appliance, a file server VM within the cluster, or file services built into a hypervisor platform

## Documentation

### Core Documentation
- **[Getting Started](docs/GETTING_STARTED.md)** - Installation, configuration, and first run
- **[Configuration](docs/CONFIGURATION.md)** - Complete configuration reference
- **[Architecture](docs/ARCHITECTURE.md)** - Technical architecture and design principles
- **[Security Considerations](docs/ARCHITECTURE.md#security-considerations)** - Network security and shared cache access model
- **[Testing](docs/TESTING.md)** - Test suite and validation procedures
- **[Developer Guide](docs/DEVELOPER.md)** - Implementation details and development notes

### Feature Documentation
- **[Caching](docs/CACHING.md)** - Cache behavior and TTL management
- **[Compression](docs/COMPRESSION.md)** - LZ4 compression and content detection
- **[Connection Pooling](docs/CONNECTION_POOLING.md)** - Connection management and load balancing
- **[Dashboard](docs/DASHBOARD.md)** - Web-based monitoring interface
- **[Error Handling](docs/ERROR_HANDLING.md)** - Error handling patterns
- **[OTLP Metrics](docs/OTLP_METRICS.md)** - OpenTelemetry metrics export

## Quick Start

```bash
# Clone and build
git clone <repository>
cd s3-proxy
cargo build --release

# Add hosts file entry to route S3 traffic to localhost
echo "127.0.0.1 s3.<region>.amazonaws.com" | sudo tee -a /etc/hosts

# Start proxy (requires sudo for ports 80/443)
sudo cargo run --release -- -c config/config.example.yaml
```

**Tip**: Set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` to automatically route AWS CLI S3 traffic through the proxy for buckets in that region. DNS zones are preferable to hosts file entries - see the [limitations and details](docs/GETTING_STARTED.md#3-configure-dns-routing).

**Alternative: HTTP_PROXY routing** — Instead of DNS routing, you can point clients at the proxy using the `HTTP_PROXY` environment variable. This avoids DNS or hosts file changes entirely and works well for single-instance deployments. With the optional TLS proxy listener enabled, use `HTTP_PROXY=https://proxy-host:3129` for encrypted client-to-proxy traffic with full caching. See the [Getting Started Guide](docs/GETTING_STARTED.md) for configuration details.

**Next Steps**: See [Getting Started Guide](docs/GETTING_STARTED.md) for detailed installation and configuration.

## Architecture

```
 ┌──────────────────────┐  ┌───────────────────────┐  ┌─────────────────────┐
 │  S3 Client (HTTP)    │  │ S3 Client (HTTP_PROXY)│  │  S3 Client (HTTPS)  │
 │  - DNS/hosts routing │  │ - HTTP_PROXY=https:// │  │  - Default HTTPS    │
 │  - AWS CLI / SDK     │  │ - AWS CLI / SDK       │  │  - AWS CLI / SDK    │
 └──────────┬───────────┘  └──────────┬────────────┘  └──────────┬──────────┘
            │                         │                          │
            ▼                         ▼                          ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                        S3 Hybrid Cache (1..N)                              │
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
           │  - LRU/TinyLFU-like eviction  │                  │
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
- **DNS routing**: Configure DNS or `/etc/hosts` to resolve S3 hostnames to the proxy. Supports multi-instance HA with multi-value DNS.
- **HTTP_PROXY**: Set `HTTP_PROXY=https://proxy:3129` (recommended) to route traffic via the proxy with TLS encryption and full caching. Also works unencrypted with `HTTP_PROXY=http://proxy:80` on private networks. No DNS changes required. HA is possible by placing a load balancer in front of multiple proxy instances (as HTTP_PROXY does not support multi-value DNS), but this is outside the scope of this solution.

Endpoints:
- **HTTP (Port 80)**: Full caching with range optimization
- **HTTPS (Port 443)**: TCP passthrough (no caching)
- **TLS Proxy (Port 3129)**: TLS termination with full caching (for `HTTP_PROXY` clients)
- **Health**: `localhost:8080/health`
- **Metrics**: `localhost:9090/metrics`
- **Dashboard**: `localhost:8081`

## Security

> **Your Responsibility**: You are responsible for restricting network access to the proxy to only clients authorized to access all objects that may be cached, and for securing file system access to the shared cache volume. This is the same security model as any shared cache — the proxy does not weaken S3's security, but depending on [TTL configuration](docs/CONFIGURATION.md#time-to-live-ttl-configuration), cached data may be accessible without S3 authorization checks.

**Network access**: With [TTL](docs/CACHING.md#time-to-live-ttl-configuration) > 0, cache hits bypass S3 entirely — any client that can reach the proxy over the network can read any cached object without IAM authorization checks. Restrict proxy access using security groups, firewalls, or network segmentation.

**Cache storage access**: Cached data is stored unencrypted (LZ4 compressed) on the shared volume. Restrict file system access to authorized proxy instances only. Encryption at rest can be provided by the storage layer if required.

**Per-request authorization (TTL=0)**: For environments requiring per-request IAM authorization, [set TTL to zero](docs/ARCHITECTURE.md#shared-cache-access-model). Every request revalidates with S3 via conditional headers — bandwidth savings from 304 responses, with full IAM enforcement on every access.

**HTTPS**: The HTTPS listener (port 443) provides [passthrough](#architecture) only (TCP tunneling to S3, no caching). For encrypted client-to-proxy traffic with caching, use the TLS proxy listener (port 3129) with `HTTP_PROXY`. AWS CLI and SDKs use HTTPS by default, so all requests are authenticated by S3 unless clients explicitly opt into HTTP endpoints or `HTTP_PROXY` routing for caching. All proxy-to-S3 communication uses HTTPS regardless of client connection protocol.

See [Security Considerations](docs/ARCHITECTURE.md#security-considerations) for detailed guidance on the shared cache access model, deployment guidelines, and appropriate use cases.

## Performance

### Single-Client Throughput Testing

Single client downloading large files (up to 5 GiB) using AWS CLI with [CRT](https://aws.amazon.com/blogs/storage/improving-amazon-s3-throughput-for-the-aws-cli-and-boto3-with-the-aws-common-runtime/), 8 proxies (m6in.2xlarge), with FSx for OpenZFS (10 GiB/s) as shared cache. All tests are cross-region (S3 bucket in eu-west-1, proxies and clients in us-west-2). For more on this subject see [Cost effective methods for accessing S3 buckets cross-region](https://repost.aws/articles/ARjzluyMS8RbeOOK4MGXRG6Q/cost-effective-methods-for-accessing-s3-buckets-cross-region).

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

### Multi-Client Request Time And Efficiency Testing

100 clients (c7gn.large) each sequentially downloading the same 100 (0.1–100 MB) files, i.e. 'thundering herd'. 3 proxies (m6in.2xlarge) with shared EFS cache. Cross-region (eu-west-1 bucket, us-west-2 proxies):

| Scenario | p50 | p95 | p99 | Throughput |
|----------|-----|-----|-----|------------|
| Direct to S3 (no proxy) | 659 ms | 1,407 ms | 1,620 ms | 1.5 GiB/s |
| Proxy, cold cache | 712 ms | 2,551 ms | 4,005 ms | 1.2 GiB/s |
| Proxy, warm cache | 578 ms | 839 ms | 1,247 ms | 1.9 GiB/s |

Warm cache delivered 22% higher throughput and 2.4× lower p95 latency than direct S3 access. Download coordination coalesces concurrent cache-miss requests — only one client fetches from S3 while others wait for the cached result.

## FAQ

**Q: Why HTTP instead of HTTPS for caching?**

A: The HTTP listener (port 80) uses plaintext HTTP so the proxy can read and cache request/response content. The HTTPS listener (port 443) uses TCP passthrough because the proxy cannot present a trusted certificate for the S3 endpoint — it can only relay encrypted bytes. For encrypted client-to-proxy traffic with caching, the optional TLS proxy listener (port 3129) terminates TLS using the proxy's own certificate, then processes the decrypted HTTP through the caching pipeline. Clients use `HTTP_PROXY=https://proxy:3129` with `--endpoint-url http://s3.region.amazonaws.com` — the SDK signs against the real S3 hostname at the HTTP level, and the proxy decrypts, caches, and forwards to S3 over HTTPS. All proxy-to-S3 communication uses HTTPS regardless of client connection protocol.

**Q: How does load balancing and failover work?**

A: The proxy instances are configured as a DNS zone with multi-value answer routing, matching how [S3 itself resolves DNS queries](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-s3-multivalue-answer-response-dns-queries/). The AWS Common Runtime (CRT), available as a transfer client in AWS CLI v2 and modern SDKs, handles load balancing and failure recovery automatically: it resolves all IPs from DNS, distributes requests across them, and retries against alternate IPs when a connection fails. No external load balancer is needed.

**Q: Can the cache preload data?**

A: As the cache is transparent and has no credentials of its own, it cannot request data from S3 without a client request. Users can warm the cache by requesting datasets upfront.

**Q: Will this work with S3-compatible storage?**

A: Nothing about this solution is specific to Amazon S3. Any origin compatible with the S3 API should work.

## Status

**Beta Status**: This is sample code demonstrating S3 caching concepts. Not recommended for production use without thorough testing and validation for your specific use case.

## Author

Ed Gummett, Storage Specialist Solutions Architect, AWS — [Connect on LinkedIn](https://www.linkedin.com/in/egummett/)

## Contributing

See [Developer Guide](docs/DEVELOPER.md) for implementation details and development setup. We welcome feedback, bug reports, and feature requests via [issues](https://github.com/aws-samples/sample-s3-hybrid-cache/issues) and [pull requests](https://github.com/aws-samples/sample-s3-hybrid-cache/pulls).

## License

See [LICENSE](LICENSE) for licensing information.
