# Hybrid Cache for Amazon S3 documentation

Reference documentation for Hybrid Cache for Amazon S3. For an overview, measured performance
figures and a quick start, see the [project README](../README.md).

## Start here

Two decisions come first, and [Getting started](GETTING_STARTED.md) covers both:

1. **How the proxy runs.** Standard mode on ports 80 and 443, `proxy_only` mode on an
   unprivileged port with no `sudo`, or [in a container](DOCKER.md). The TLS proxy listener
   (port 3129) is enabled separately, in the `server.tls` block.
2. **How client traffic reaches it.** The recommended pattern points clients at the TLS proxy
   listener with `HTTP_PROXY=https://<proxy-host>:3129` and an `http://` S3 endpoint URL, so
   the client hop is encrypted and responses are still cached. Where the network segment
   between clients and proxies is secure and cleartext on it is acceptable, a DNS zone that
   resolves S3's hostnames to the proxies is a simpler alternative. A load balancer and a
   hosts file are the other options. See
   [Configure client routing](GETTING_STARTED.md#3-configure-client-routing).

After that, the [configuration reference](CONFIGURATION.md) lists every field, and the
[example configurations](examples/) include three loadable profiles.

## I want to…

| Goal | Go to |
|---|---|
| Install and run it for the first time | [Getting started](GETTING_STARTED.md) |
| Run it in Docker or Kubernetes | [Docker deployment](DOCKER.md) |
| Deploy a fleet on AWS, and size it | [AWS deployment](AWS_DEPLOYMENT.md) |
| Look up what a configuration field does | [Configuration reference](CONFIGURATION.md) |
| Upgrade an existing deployment | [Upgrading](UPGRADING.md) |
| See what changed in a release | [Changelog](../CHANGELOG.md) |
| Encrypt the client-to-proxy hop | [HTTP_PROXY with the TLS proxy listener](GETTING_STARTED.md#option-a-http_proxy-with-the-tls-proxy-listener-recommended) |
| Put a load balancer in front of a fleet | [Layer 4 load balancer](GETTING_STARTED.md#option-b-layer-4-load-balancer) |
| Review the security model before deploying | [Security](../SECURITY.md) |
| Cache a MinIO, RustFS or other S3-compatible store | [Upstream transport overrides](CONFIGURATION.md#upstream-transport-overrides) |
| Use S3 interface VPC endpoints (PrivateLink) | [S3 PrivateLink](GETTING_STARTED.md#s3-privatelink-interface-vpc-endpoints) |
| Change caching behavior for some keys only | [Cache rules](CONFIGURATION.md#cache-rules) and the [worked examples](examples/) |
| Know when the proxy can return data that differs from S3 | [Cache freshness](CACHE_FRESHNESS.md#when-the-proxy-can-return-data-that-differs-from-s3) |
| Understand why a request missed the cache | [Caching](CACHING.md), then [cache freshness](CACHE_FRESHNESS.md) |
| Look up a `/metrics` field | [Metrics reference](METRICS_REFERENCE.md) |
| Interpret a metric, or infer cache savings | [Per-bucket metrics](METRICS.md) |
| Export metrics to CloudWatch or Prometheus | [OTLP metrics](OTLP_METRICS.md) |
| Watch it running in a browser | [Dashboard](DASHBOARD.md) |
| Diagnose an error or recover a corrupt cache | [Error handling](ERROR_HANDLING.md) |
| Tune throughput or latency | [Cache hit performance tuning](CONFIGURATION.md#cache-hit-performance-tuning) and [connection pooling](CONNECTION_POOLING.md) |
| Cap origin download bandwidth or share it fairly | [Bandwidth QoS](BANDWIDTH_QOS.md) |
| Cut upstream tail latency | [Hedged requests](HEDGING.md) |
| Run a fleet of proxies on one cache volume | [Shared storage](SHARED_STORAGE.md) |
| Route concurrent readers of the same bytes to one proxy | [Request-aware routing](REQUEST_AWARE_ROUTING.md) |
| Cache on each proxy's local NVMe instead of a cache volume | [Local NVMe cache fleets](LOCAL_NVME_CACHE.md) |
| Parse the access log | [Access log format](ACCESS_LOG_FORMAT.md) |
| Build, test, or contribute | [Developer guide](DEVELOPER.md) |

## Deployment and operations

- [Getting started](GETTING_STARTED.md): installation, deployment modes, client routing, the
  TLS proxy listener and its certificate, load-balancer patterns, PrivateLink, access points,
  Multi-Region Access Points, and fronting a plaintext or non-standard origin.
- [Docker deployment](DOCKER.md): the container image, compose, the privilege model, and the
  container-specific traps: bind-mount ownership, loopback binds, capabilities and distroless
  health checks.
- [AWS deployment](AWS_DEPLOYMENT.md): the reference architecture, instance and cache volume
  sizing, and NLB configuration for cross-Region and non-AWS origins.
- [Upgrading](UPGRADING.md): the upgrade contract, per-release manual steps and default
  changes, and rolling back from 3.0.0.
- [Changelog](../CHANGELOG.md): what changed in each release from 3.0.0. Earlier releases are
  in the [pre-3.0.0 changelog](../changelog-pre-v3.md).
- [Shared storage](SHARED_STORAGE.md): running a fleet on one cache volume: the mount options
  that correctness depends on, journal-based metadata writes, distributed eviction, size
  tracking, the validation scan, and the failure modes.
- [Error handling](ERROR_HANDLING.md): cache corruption, missing range files, disk exhaustion,
  orphaned files, and the upstream idle watchdog.
- [Request-aware routing](REQUEST_AWARE_ROUTING.md): an optional HAProxy router that picks a
  proxy by object key and byte range, so concurrent readers of the same bytes reach one proxy.
  It covers when the router is worth running, bounded load, DNS discovery, and a health-check
  trap that hides a broken fleet certificate.
- [Local NVMe cache fleets](LOCAL_NVME_CACHE.md): the alternative to a cache volume, where each
  proxy caches on its own NVMe instance store and an affinity router makes the private disks
  behave as one cache. It covers the routing prerequisite, instance-family choice, the cost
  comparison, preparing and sizing the instance store, and what fleet churn costs.

## Configuration

- [Configuration reference](CONFIGURATION.md): every field, with its type, default, valid
  range and validation.
- [Annotated example configuration](../config/config.example.yaml): a `config.yaml` covering
  every option.
- [Example configurations and cache rules](examples/): three loadable configuration profiles
  and seven worked `cache_rules.json` files.
- [Cache rules schema](cache-rules-schema.json): the JSON Schema for `cache_rules.json`.

## How it works

- [Architecture](ARCHITECTURE.md): design principles, module layout, the streaming
  architecture, the range read-path map, and the
  [security considerations](ARCHITECTURE.md#security-considerations): the shared cache access
  model, what a cleartext hop exposes, and the trust and integrity model.
- [Security](../SECURITY.md): the security model the proxy assumes, the risks it leaves with
  you, and the checks you can run yourself.
- [Caching](CACHING.md): start here for the cache. It covers what is cached, what bypasses the
  cache, and per-key rules. Four pages go deeper:
  - [Cache internals](CACHE_INTERNALS.md): on-disk and in-memory layout, sharding, cache-key
    derivation, access tracking, and the journal and size-tracking internals
  - [Cache freshness](CACHE_FRESHNESS.md): TTLs, revalidation and conditional requests
  - [Cache read paths](CACHE_READ_PATHS.md): how a read is served:
    [range merging](CACHE_READ_PATHS.md#intelligent-range-merging),
    [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching),
    [the write cache](CACHE_READ_PATHS.md#write-through-cache), multipart uploads and
    coherency checks
  - [Eviction](EVICTION.md): how the proxy reclaims space, the eviction algorithm and the
    admission window
- [Compression](COMPRESSION.md): the compression decision, the built-in denylist, the
  store-mode frame contract, and
  [compression in the RAM cache](COMPRESSION.md#ram-cache-compression-optimization).
- [Connection pooling](CONNECTION_POOLING.md): pooling, IP distribution, health tracking and
  recovery probing, and TCP socket options.
- [Multipart uploads](MULTIPART_UPLOAD.md): how multipart uploads are cached, and the
  correctness model.
- [Bandwidth QoS](BANDWIDTH_QOS.md): the origin download ceiling, fair-share scheduling and
  fleet coordination.
- [Hedged requests](HEDGING.md): when a duplicate upstream fetch cuts tail latency, and the
  governor that bounds its cost.

## Observability

- [Metrics reference](METRICS_REFERENCE.md): every field in the `/metrics` payload, and the
  [fields with misleading names](METRICS_REFERENCE.md#fields-with-misleading-names).
- [Per-bucket metrics](METRICS.md): per-bucket traffic accounting, cache-savings inference,
  and how to read the request-completion and in-flight-memory counters.
- [OTLP metrics](OTLP_METRICS.md): the OpenTelemetry export, and publishing to CloudWatch,
  Prometheus or an OpenTelemetry Collector.
- [Dashboard](DASHBOARD.md): the web interface, its JSON API endpoints and its security
  posture.
- [Access log format](ACCESS_LOG_FORMAT.md): the 25-field S3-style record and what each field
  carries.

## Development

- [Developer guide](DEVELOPER.md): key design decisions and their trade-offs, implementation
  detail, known limitations, and the build, test and coverage workflow.
