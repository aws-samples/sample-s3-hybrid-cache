# OTLP Metrics Export

S3 Hybrid Cache supports exporting metrics using OpenTelemetry Protocol (OTLP), providing visibility into proxy requests, S3 API calls, cache performance, and system health.

## Enabling OTLP Metrics

Configure via file:

```yaml
metrics:
  enabled: true
  otlp:
    enabled: true
    endpoint: "http://localhost:4318"
    export_interval: "60s"
    timeout: "10s"
    compression: "none"  # Options: "none", "gzip"
    headers: {}          # Optional custom headers
```

Or via CLI:

```bash
s3-proxy --otlp-endpoint http://localhost:4318 --otlp-export-interval 60
```

## Publishing Metrics to Observability Backends

### CloudWatch

Use CloudWatch Agent v1.300060.0 or later:

```json
{
  "metrics": {
    "metrics_collected": {
      "otlp": {
        "http_endpoint": "127.0.0.1:4318"
      }
    }
  }
}
```

### Prometheus

Use Prometheus v3.0 or later with OTLP receiver support:

```bash
prometheus \
  --config.file=prometheus.yml \
  --web.enable-otlp-receiver \
  --enable-feature=native-histograms,otlp-deltatocumulative
```

### OpenTelemetry Collector

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318

exporters:
  otlphttp:
    endpoint: http://prometheus:9090/api/v1/otlp

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [otlphttp]
```

## Available Metrics

All metrics are gauges with resource attributes `host.name`, `service.name`, and `service.version`. No per-metric dimensions.

### Cache — Sizes

| Metric | Type | Description |
|--------|------|-------------|
| `cache.total_cache_size` | Gauge (bytes) | Total cache size (read + write + RAM) |
| `cache.read_cache_size` | Gauge (bytes) | Disk read cache size |
| `cache.write_cache_size` | Gauge (bytes) | Disk write cache size |
| `cache.ram_cache_size` | Gauge (bytes) | RAM cache size |

### Cache — Hits / Misses / Evictions

| Metric | Type | Description |
|--------|------|-------------|
| `cache.cache_hits` | Gauge | Total disk cache hits (GET) |
| `cache.cache_misses` | Gauge | Total disk cache misses (GET, forwarded to S3) |
| `cache.evictions` | Gauge | Disk cache evictions |
| `cache.write_cache_hits` | Gauge | Write cache hits (PUT objects served on GET) |
| `cache.s3_requests_saved` | Gauge | S3 requests avoided (disk hits + metadata hits) |
| `cache.bytes_served_from_cache` | Gauge (bytes) | S3 transfer bytes saved |

### Cache — RAM Tier

| Metric | Type | Description |
|--------|------|-------------|
| `cache.ram_cache_hits` | Gauge | RAM range cache hits |
| `cache.ram_cache_misses` | Gauge | RAM range cache misses |
| `cache.ram_cache_evictions` | Gauge | RAM range cache evictions |

### Cache — Metadata Tier (HEAD)

| Metric | Type | Description |
|--------|------|-------------|
| `cache.metadata_cache_hits` | Gauge | Metadata RAM hits (HEAD requests) |
| `cache.metadata_cache_misses` | Gauge | Metadata RAM misses |
| `cache.metadata_cache_entries` | Gauge | Metadata entries currently in RAM |
| `cache.metadata_cache_evictions` | Gauge | Metadata entries evicted from RAM |
| `cache.metadata_cache_stale_refreshes` | Gauge | Expired RAM entries that required disk re-read |

### Cache — Health / Error Signals

| Metric | Type | Description |
|--------|------|-------------|
| `cache.corruption_metadata_total` | Gauge | Corrupted metadata files detected |
| `cache.corruption_missing_range_total` | Gauge | Missing range files detected |
| `cache.disk_full_events_total` | Gauge | Disk-full events during cache writes |
| `cache.lock_timeout_total` | Gauge | Cache lock acquisition timeouts |
| `cache.write_failures_total` | Gauge | Cache write failures |
| `cache.etag_mismatches_total` | Gauge | ETag mismatches (cache coherency violations) |
| `cache.range_invalidations_total` | Gauge | Range invalidations triggered by ETag mismatch |
| `cache.incomplete_uploads_evicted` | Gauge | Incomplete MPU uploads evicted from write cache |

### Compression

| Metric | Type | Description |
|--------|------|-------------|
| `compression.average_compression_ratio` | Gauge | Average compression ratio |
| `compression.failures_total` | Gauge | Compression + decompression failures |

### Connection Pool

| Metric | Type | Description |
|--------|------|-------------|
| `connection_pool.failed_connections` | Gauge | Failed connection attempts |
| `connection_pool.average_latency_ms` | Gauge (ms) | Average connection latency |
| `connection_pool.success_rate_percent` | Gauge (%) | Connection success rate |

### Coalescing

| Metric | Type | Description |
|--------|------|-------------|
| `coalescing.waits_total` | Gauge | Requests that waited for an in-flight fetch |
| `coalescing.cache_hits_after_wait_total` | Gauge | Requests served from cache after waiting |
| `coalescing.timeouts_total` | Gauge | Coalescing wait timeouts |
| `coalescing.s3_fetches_saved_total` | Gauge | S3 fetches avoided by coalescing |
| `coalescing.average_wait_duration_ms` | Gauge (ms) | Average coalescing wait time |

### Request

| Metric | Type | Description |
|--------|------|-------------|
| `request_metrics.total_requests` | Gauge | Total HTTP requests processed |
| `request_metrics.successful_requests` | Gauge | Successful requests |
| `request_metrics.failed_requests` | Gauge | Failed requests |
| `request_metrics.average_response_time_ms` | Gauge (ms) | Cumulative average response time |
| `request_metrics.active_requests` | Gauge | Requests currently in flight |

### Process

| Metric | Type | Description |
|--------|------|-------------|
| `uptime_seconds` | Gauge (s) | Proxy uptime |
| `process.memory_usage_bytes` | Gauge (bytes) | RSS memory usage |

## Configuration Options

### Export Interval

How frequently metrics are exported (default: 60 seconds):

```yaml
otlp:
  export_interval: "60s"
```

### Timeout

Request timeout for OTLP exports (default: 10 seconds):

```yaml
otlp:
  timeout: "10s"
```

### Compression

```yaml
otlp:
  compression: "gzip"  # Options: "none", "gzip"
```

### Custom Headers

```yaml
otlp:
  headers:
    Authorization: "Bearer your-token"
```

## Environment Variables

- `OTLP_ENDPOINT`: OTLP endpoint URL
- `OTLP_EXPORT_INTERVAL`: Export interval in seconds
- `METRICS_ENABLED`: Enable/disable metrics collection

## Troubleshooting

Check logs for OTLP export errors:

```bash
grep "OTLP" /logs/app/$(hostname)/proxy.log
```

Verify CloudWatch Agent is configured for OTLP:

```bash
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
```
