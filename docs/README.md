# Hybrid Cache for Amazon S3 Documentation

Reference documentation for Hybrid Cache for Amazon S3. For an overview, benchmark
figures, and a five-minute quick start, see the [project README](../README.md).

## Start here

Two decisions come before anything else, and both live in
[GETTING_STARTED.md](GETTING_STARTED.md):

1. **How does the proxy run?** Standard mode on ports 80/443, `proxy_only` mode on an
   unprivileged port with no `sudo`, a TLS-terminating listener, or
   [in a container](DOCKER.md).
2. **How does client traffic reach it?** `HTTP_PROXY` plus an `http://` endpoint URL, a
   hosts-file override, a Route 53 private hosted zone, or a load balancer. This is the
   most consequential choice in a deployment and the one most often got wrong.

Then: [Configuration Reference](CONFIGURATION.md) for every field, and
[`docs/examples/`](examples/) for three loadable config profiles.

## I want to…

| Goal | Go to |
|---|---|
| Install and run it for the first time | [GETTING_STARTED.md](GETTING_STARTED.md) |
| Run it in Docker or Kubernetes | [DOCKER.md](DOCKER.md) |
| Deploy a fleet on AWS, and size it | [AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md) |
| Look up what a config field does | [CONFIGURATION.md](CONFIGURATION.md) |
| Upgrade an existing deployment | [UPGRADING.md](UPGRADING.md) |
| Encrypt the client-to-proxy hop | [GETTING_STARTED.md — TLS patterns](GETTING_STARTED.md#option-d-layer-4-load-balancer-multi-instance) |
| Cache a MinIO, RustFS, or other S3-compatible store | [CONFIGURATION.md — Upstream Transport Overrides](CONFIGURATION.md#upstream-transport-overrides) |
| Use S3 interface VPC endpoints (PrivateLink) | [GETTING_STARTED.md — S3 PrivateLink](GETTING_STARTED.md#s3-privatelink-interface-vpc-endpoints) |
| Change caching behaviour for some keys only | [CONFIGURATION.md — Cache Rules](CONFIGURATION.md#cache-rules) + [examples](examples/) |
| Understand why a request missed cache | [CACHING.md](CACHING.md), then [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md) |
| Look up a `/metrics` field | [METRICS_REFERENCE.md](METRICS_REFERENCE.md) |
| Interpret a metric, or infer cache savings | [METRICS.md](METRICS.md) |
| Export metrics to CloudWatch or Prometheus | [OTLP_METRICS.md](OTLP_METRICS.md) |
| Watch it running in a browser | [DASHBOARD.md](DASHBOARD.md) |
| Diagnose an error or recover a corrupt cache | [ERROR_HANDLING.md](ERROR_HANDLING.md) |
| Tune throughput or latency | [CONFIGURATION.md — Cache Hit Performance Tuning](CONFIGURATION.md#cache-hit-performance-tuning), [CONNECTION_POOLING.md](CONNECTION_POOLING.md) |
| Cap origin download bandwidth or share it fairly | [BANDWIDTH_QOS.md](BANDWIDTH_QOS.md) |
| Cut upstream tail latency | [HEDGING.md](HEDGING.md) |
| Run several proxies against one cache volume | [SHARED_STORAGE.md](SHARED_STORAGE.md) |
| Route concurrent readers of the same bytes to one instance | [REQUEST_AWARE_ROUTING.md](REQUEST_AWARE_ROUTING.md) |
| Parse the access log | [ACCESS_LOG_FORMAT.md](ACCESS_LOG_FORMAT.md) |
| Build, test, or contribute | [DEVELOPER.md](DEVELOPER.md) |

## Deployment and operations

- [GETTING_STARTED.md](GETTING_STARTED.md) — installation, deployment modes, client
  routing (four options), TLS listener and certificate generation, load-balancer
  patterns, PrivateLink, access points, MRAP, and fronting a non-standard upstream
- [DOCKER.md](DOCKER.md) — container image, compose, the privilege model, and four
  container-specific traps (bind-mount ownership, loopback binds, capabilities,
  distroless healthchecks)
- [AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md) — reference architecture, instance and FSx for
  OpenZFS sizing, and NLB configuration for cross-region and non-AWS origins
- [UPGRADING.md](UPGRADING.md) — the upgrade contract, plus per-release manual steps and
  default changes
- [SHARED_STORAGE.md](SHARED_STORAGE.md) — running several proxies against one cache
  volume: the two mount requirements that are correctness requirements, journal-based
  metadata writes, distributed eviction, size tracking, the validation scan, and the
  failure modes
- [ERROR_HANDLING.md](ERROR_HANDLING.md) — cache corruption, missing range files, disk
  exhaustion, orphaned files, and the upstream idle watchdog
- [REQUEST_AWARE_ROUTING.md](REQUEST_AWARE_ROUTING.md) — optional HAProxy pattern that
  selects a fleet member by object key and byte range, so concurrent readers of the same
  bytes converge on one instance. Covers where it is worth running, bounded load, DNS
  discovery, and a health-check trap that hides a broken fleet certificate

## Configuration

- [CONFIGURATION.md](CONFIGURATION.md) — every field: type, default, valid range, and
  validation behaviour
- [`config/config.example.yaml`](../config/config.example.yaml) — annotated example
  covering every option
- [`docs/examples/`](examples/) — three loadable config profiles and six worked
  `cache_rules.json` files
- [`cache-rules-schema.json`](cache-rules-schema.json) — JSON Schema for `cache_rules.json`

## How it works

- [ARCHITECTURE.md](ARCHITECTURE.md) — design principles, module layout, streaming
  architecture, the range read-path map, and the
  [security model](ARCHITECTURE.md#security-considerations) (shared cache access, what a
  cleartext hop exposes, trust and integrity)
- [CACHING.md](CACHING.md) — **start here for the cache**: what gets cached, what
  bypasses it, and per-key rules. Four companions carry the depth:
  - [CACHE_INTERNALS.md](CACHE_INTERNALS.md) — on-disk and in-memory layout, sharding,
    cache-key derivation, access tracking, journal and size-tracking internals
  - [CACHE_FRESHNESS.md](CACHE_FRESHNESS.md) — TTL, revalidation, conditional requests
  - [CACHE_READ_PATHS.md](CACHE_READ_PATHS.md) — how a read is satisfied:
    [range merging](CACHE_READ_PATHS.md#intelligent-range-merging),
    [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching),
    [write-through](CACHE_READ_PATHS.md#write-through-cache), multipart, coherency checks
  - [EVICTION.md](EVICTION.md) — reclaiming space: algorithms, admission window,
    critical capacity bypass
- [COMPRESSION.md](COMPRESSION.md) — the compression decision, the built-in denylist, the
  store-mode frame contract, and
  [RAM-tier optimization](COMPRESSION.md#ram-cache-compression-optimization)
- [CONNECTION_POOLING.md](CONNECTION_POOLING.md) — pooling, IP distribution, health
  tracking and recovery probing, TCP socket options
- [MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md) — multipart cache internals and correctness
  model
- [BANDWIDTH_QOS.md](BANDWIDTH_QOS.md) — origin download ceiling, fair-share scheduling,
  and fleet coordination
- [HEDGING.md](HEDGING.md) — hedged upstream requests: when a second duplicate fetch cuts
  tail latency, and the governor that bounds its cost

## Observability

- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) — every field in the `/metrics` payload,
  and the seven misleadingly-named ones to avoid
- [METRICS.md](METRICS.md) — per-bucket traffic accounting, cache-savings inference, and
  how to read the request-completion and in-flight-memory counters
- [OTLP_METRICS.md](OTLP_METRICS.md) — the OpenTelemetry export surface and publishing to
  CloudWatch, Prometheus, or an OTel Collector
- [DASHBOARD.md](DASHBOARD.md) — the web interface, its six JSON API endpoints, and its
  security posture
- [ACCESS_LOG_FORMAT.md](ACCESS_LOG_FORMAT.md) — the 25-field S3-style record, what each
  field carries, and the seven that are always empty

## Development

- [DEVELOPER.md](DEVELOPER.md) — key design decisions and their trade-offs,
  implementation detail, known limitations, and the build/test/coverage workflow
  (the former `TESTING.md` was merged into its Testing Strategy section)
