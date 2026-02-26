# S3 Hybrid Cache - Intelligent, Transparent Caching for Amazon S3

A high-performance, transparent S3 caching proxy with intelligent multi-tier caching, streaming architecture, and comprehensive observability.

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

**Tip**: Set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` to automatically route AWS CLI S3 traffic through the proxy for buckets in that region. DNS zones are preferable to hosts file entries - see the limitations and details at docs/GETTING_STARTED.md#3-configure-dns-routing
**Tip**: Set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` to automatically route AWS CLI S3 traffic through the proxy for buckets in that region. DNS zones are preferable to hosts file entries - see the [limitations and details](docs/GETTING_STARTED.md#3-configure-dns-routing).

**Next Steps**: See [Getting Started Guide](docs/GETTING_STARTED.md) for detailed installation and configuration.

## Architecture

```
      ┌─────────────────────┐       ┌─────────────────────┐
      │  S3 Client (HTTP)   │       │  S3 Client (HTTPS)  │
      │  - AWS CLI          │       │  - AWS CLI          │
      │  - S3 SDK           │       │  - S3 SDK           │
      └──────────┬──────────┘       └──────────┬──────────┘
                 │                             │
                 ▼                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        S3 Hybrid Cache (1..N)                   │
│  ┌───────────────────────────┐  ┌───────────────────────────┐   │
│  │  HTTP Handler (Port 80)   │  │ HTTPS Handler (Port 443)  │   │
│  │  - Caching                │  │ - TCP Passthrough         │   │
│  │  - Range Merging          │  │ - No Caching              │   │
│  │  - Streaming              │  │ - Direct to S3            │   │
│  └─────────────┬─────────────┘  └────────────┬──────────────┘   │
│                │                             │                  │
│                ▼                             │                  │
│  ┌───────────────────────────┐               │                  │
│  │        RAM Cache          │               │                  │
│  │  - Metadata + ranges      │               │                  │
│  │  - Compression            │               │                  │
│  │  - Eviction               │               │                  │
│  └─────────────┬─────────────┘               │                  │
└────────────────┼─────────────────────────────┼──────────────────┘
                 │                             │
                 ▼                             │
┌───────────────────────────────┐              │
│   Shared Disk Cache (NFS)     │              │
│  - Metadata + ranges          │              │
│  - Compression                │              │
│  - LRU/TinyLFU-like eviction  │              │
│  - Fixed or elastic size      │              │
│  - Journaled writes           │              │
└───────────────┬───────────────┘              │
                │                              │
                └────────────────┬─────────────┘
                                 │
                                 ▼
                   ┌─────────────────────────┐
                   │    Amazon S3 (HTTPS)    │
                   └─────────────────────────┘
```

Endpoints:
- **HTTP (Port 80)**: Full caching with range optimization
- **HTTPS (Port 443)**: TCP passthrough (no caching)
- **Health**: `localhost:8080/health`
- **Metrics**: `localhost:9090/metrics`
- **Dashboard**: `localhost:8081`

## Security

> **Your Responsibility**: You are responsible for restricting network access to the proxy to only clients authorized to access all objects that may be cached, and for securing file system access to the shared cache volume. This is the same security model as any shared cache — the proxy does not weaken S3's security, but depending on [TTL configuration](docs/CONFIGURATION.md#time-to-live-ttl-configuration), cached data may be accessible without S3 authorization checks.

**Network access**: With [TTL](docs/CACHING.md#time-to-live-ttl-configuration) > 0, cache hits bypass S3 entirely — any client that can reach the proxy over the network can read any cached object without IAM authorization checks. Restrict proxy access using security groups, firewalls, or network segmentation.

**Cache storage access**: Cached data is stored unencrypted (LZ4 compressed) on the shared volume. Restrict file system access to authorized proxy instances only. Encryption at rest can be provided by the storage layer if required.

**Per-request authorization (TTL=0)**: For environments requiring per-request IAM authorization, [set TTL to zero](docs/ARCHITECTURE.md#shared-cache-access-model). Every request revalidates with S3 via conditional headers — bandwidth savings from 304 responses, with full IAM enforcement on every access.

**HTTPS**: Supported only for [passthrough](#architecture) (TCP tunneling to S3). AWS CLI and SDKs use HTTPS by default, so all requests are authenticated by S3 unless clients explicitly opt into HTTP endpoints for caching. All proxy-to-S3 communication uses HTTPS regardless of client connection protocol. See the [FAQ](#faq) below for why caching requires HTTP.

See [Security Considerations](docs/ARCHITECTURE.md#security-considerations) for detailed guidance on the shared cache access model, deployment guidelines, and appropriate use cases.

## Performance

Tested with 100 concurrent clients (c7gn.large) downloading 100 files (0.1–100 MB each, ~1.4 GB total per client) through 3 proxy instances (m6in.2xlarge) with shared EFS cache.

| Scenario | p50 | p95 | p99 | Throughput |
|----------|-----|-----|-----|------------|
| Direct to S3 (no proxy) | 634 ms | 1,988 ms | 2,540 ms | 13.3 Gbps |
| Proxy, cold cache | 627 ms | 4,469 ms | 6,409 ms | 9.8 Gbps |
| Proxy, warm cache | 578 ms | 839 ms | 1,247 ms | 16.2 Gbps |

Warm cache delivers 22% higher throughput and 2.4× lower p95 latency than direct S3 access. Download coordination coalesced 94% of concurrent cache-miss requests during the cold-cache run (4,323 of ~4,600 requests served from cache after waiting, only 272 actual S3 fetches). CloudWatch confirmed 95% S3 data transfer savings: 6.3 GB pulled from S3 to serve 137 GB to 100 clients (22× amplification). Zero errors across 30,000+ requests.

## Status

**Beta Status**: This is sample code demonstrating S3 caching concepts. Not recommended for production use without thorough testing and validation for your specific use case.

**Key Limitations**:
- Requires hosts file or DNS modification for transparent operation
- HTTPS mode provides passthrough only (no caching)
- Cannot initiate requests to S3 (no AWS credentials, transparent forwarder only)

## FAQ

**Q: Why HTTP instead of HTTPS for caching?**

A: The proxy cannot present a trusted certificate for the S3 endpoint, so it cannot terminate TLS or inspect the traffic — only relay encrypted bytes. Caching would require TLS interception with a custom CA trusted by all clients. The proxies run inside the user's secure network, where encryption-in-transit between client and proxy may be unnecessary. All proxy-to-S3 communication uses HTTPS encryption. `HTTPS_PROXY` support was considered, but this would not help as it uses the HTTP CONNECT method to create a tunnel, and the client performs a TLS handshake with the destination (S3) through that tunnel. Additionally, `HTTPS_PROXY` routes through a single proxy address without multi-value DNS support, preventing load balancing and failover.

**Q: How does load balancing and failover work?**

A: The proxy instances are configured as a DNS zone with multi-value answer routing, matching how [S3 itself resolves DNS queries](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-s3-multivalue-answer-response-dns-queries/). The AWS Common Runtime (CRT), available as a transfer client in AWS CLI v2 and modern SDKs, handles load balancing and failure recovery automatically: it resolves all IPs from DNS, distributes requests across them, and retries against alternate IPs when a connection fails. No external load balancer is needed.

**Q: Can the cache preload data?**

A: As the cache is transparent and has no credentials of its own, it cannot request data from S3 without a client request. Users can warm the cache by requesting datasets upfront.

**Q: Will this work with S3-compatible storage?**

A: Nothing about this solution is specific to Amazon S3. Any origin compatible with the S3 API should work.

## Author

Ed Gummett, Storage Specialist Solutions Architect, AWS — [Connect on LinkedIn](https://www.linkedin.com/in/egummett/)

## Contributing

See [Developer Guide](docs/DEVELOPER.md) for implementation details and development setup. We welcome feedback, bug reports, and feature requests via [issues](https://github.com/aws-samples/sample-s3-hybrid-cache/issues) and [pull requests](https://github.com/aws-samples/sample-s3-hybrid-cache/pulls).

## License

See [LICENSE](LICENSE) for licensing information.
