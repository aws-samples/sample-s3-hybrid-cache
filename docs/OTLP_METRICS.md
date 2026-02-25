# OTLP Metrics Export

The S3 Proxy supports exporting metrics using OpenTelemetry Protocol (OTLP), similar to AWS Mountpoint for Amazon S3. This provides visibility into proxy requests, S3 API calls, cache performance, and system metrics.

## Enabling OTLP Metrics

To export S3 Proxy metrics using OTLP, configure the proxy with an OTLP endpoint:

```bash
s3-proxy --otlp-endpoint http://localhost:4318 --otlp-export-interval 60
```

Or using environment variables:

```bash
export OTLP_ENDPOINT=http://localhost:4318
export OTLP_EXPORT_INTERVAL=60
s3-proxy
```

Or using a configuration file:

```yaml
metrics:
  enabled: true
  otlp:
    enabled: true
    endpoint: "http://localhost:4318"
    export_interval: "60s"
    timeout: "10s"
    compression: "none"  # Options: "none", "gzip"
    headers: {}  # Optional custom headers
```

## Publishing Metrics to Observability Backends

The S3 Proxy exports metrics using OTLP protocol in HTTP protobuf format. It uses the OpenTelemetry SDK with a periodic reader for automatic export.

### CloudWatch

Use CloudWatch Agent v1.300060.0 or later to export S3 Proxy metrics to CloudWatch:

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
  --web.listen-address=:9090 \
  --web.enable-otlp-receiver \
  --enable-feature=native-histograms,otlp-deltatocumulative
```

### OpenTelemetry Collector

Route metrics through an OpenTelemetry Collector:

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

OTLP metric names mirror the `/metrics` JSON endpoint field paths. All metrics are gauges with resource attributes `host.name`, `service.name`, and `service.version`. No per-metric dimensions.

### Cache

| Metric | Type | Description |
|--------|------|-------------|
| `cache.total_cache_size` | Gauge (bytes) | Total cache size (read + write + RAM) |
| `cache.read_cache_size` | Gauge (bytes) | Disk read cache size |
| `cache.write_cache_size` | Gauge (bytes) | Disk write cache size |
| `cache.ram_cache_size` | Gauge (bytes) | RAM cache size |
| `cache.cache_hit_rate_percent` | Gauge (%) | Overall cache hit rate |
| `cache.ram_cache_hit_rate_percent` | Gauge (%) | RAM cache hit rate |
| `cache.total_requests` | Gauge | Total cache lookups |
| `cache.cache_hits` | Gauge | Total cache hits |
| `cache.cache_misses` | Gauge | Total cache misses |
| `cache.evictions` | Gauge | Disk cache evictions |
| `cache.write_cache_hits` | Gauge | Write cache hits |
| `cache.bytes_served_from_cache` | Gauge (bytes) | S3 transfer saved |
| `cache.ram_cache_hits` | Gauge | RAM cache hits |
| `cache.ram_cache_misses` | Gauge | RAM cache misses |
| `cache.ram_cache_evictions` | Gauge | RAM cache evictions |
| `cache.ram_cache_max_size` | Gauge (bytes) | RAM cache max size |
| `cache.metadata_cache_hits` | Gauge | Metadata cache hits |
| `cache.metadata_cache_misses` | Gauge | Metadata cache misses |
| `cache.metadata_cache_entries` | Gauge | Metadata cache entries |
| `cache.metadata_cache_max_entries` | Gauge | Metadata cache max entries |
| `cache.metadata_cache_evictions` | Gauge | Metadata cache evictions |
| `cache.metadata_cache_stale_refreshes` | Gauge | Metadata stale refreshes |

### Compression

| Metric | Type | Description |
|--------|------|-------------|
| `compression.average_compression_ratio` | Gauge | Average compression ratio |
| `compression.failures_total` | Gauge | Compression + decompression failures |

### Connection Pool

| Metric | Type | Description |
|--------|------|-------------|
| `connection_pool.failed_connections` | Gauge | Failed connections |
| `connection_pool.average_latency_ms` | Gauge (ms) | Average connection latency |
| `connection_pool.success_rate_percent` | Gauge (%) | Connection success rate |

### Coalescing

| Metric | Type | Description |
|--------|------|-------------|
| `coalescing.waits_total` | Gauge | Requests that waited for in-flight fetch |
| `coalescing.cache_hits_after_wait_total` | Gauge | Requests served from cache after waiting |
| `coalescing.timeouts_total` | Gauge | Coalescing timeouts |
| `coalescing.s3_fetches_saved_total` | Gauge | S3 fetches avoided by coalescing |
| `coalescing.average_wait_duration_ms` | Gauge (ms) | Average coalescing wait time |

### Request

| Metric | Type | Description |
|--------|------|-------------|
| `request_metrics.total_requests` | Gauge | Total HTTP requests |
| `request_metrics.successful_requests` | Gauge | Successful requests |
| `request_metrics.failed_requests` | Gauge | Failed requests |
| `request_metrics.average_response_time_ms` | Gauge (ms) | Average response time |
| `request_metrics.requests_per_second` | Gauge | Request rate |

### Process

| Metric | Type | Description |
|--------|------|-------------|
| `uptime_seconds` | Gauge (s) | Proxy uptime |
| `process.memory_usage_bytes` | Gauge (bytes) | RSS memory usage |

## Configuration Options

### OTLP Endpoint

The OTLP endpoint URL where metrics will be exported:

```yaml
otlp:
  endpoint: "http://localhost:4318"
```

Common endpoints:
- CloudWatch Agent: `http://127.0.0.1:4318`
- Prometheus OTLP receiver: `http://prometheus:9090/api/v1/otlp`
- OpenTelemetry Collector: `http://otel-collector:4318`

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

Compression for OTLP requests:

```yaml
otlp:
  compression: "gzip"  # Options: "none", "gzip"
```

### Custom Headers

Optional headers for OTLP requests (useful for authentication):

```yaml
otlp:
  headers:
    Authorization: "Bearer your-token"
    X-Custom-Header: "value"
```

## Environment Variables

All OTLP settings can be configured via environment variables:

- `OTLP_ENDPOINT`: OTLP endpoint URL
- `OTLP_EXPORT_INTERVAL`: Export interval in seconds
- `METRICS_ENABLED`: Enable/disable metrics collection

## Command Line Arguments

Key OTLP settings can be set via command line:

- `--otlp-endpoint URL`: Set OTLP endpoint
- `--otlp-export-interval SECONDS`: Set export interval
- `--metrics-enabled`: Enable metrics collection

## Troubleshooting

### OTLP Export Failures

Check the logs for OTLP export errors:

```bash
grep "OTLP" /logs/app/$(hostname)/proxy.log
```

### Metric Validation

Verify metrics are being collected:

```bash
curl http://localhost:9090/metrics
```

### CloudWatch Agent Issues

Ensure CloudWatch Agent is configured for OTLP:

```bash
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
```

### Prometheus Configuration

Verify Prometheus OTLP receiver is enabled:

```bash
curl http://prometheus:9090/api/v1/label/__name__/values | grep proxy
```

## Performance Considerations

- OTLP export adds minimal overhead (< 1% CPU)
- Metrics are exported asynchronously
- Failed exports are logged but don't affect proxy performance
- Consider adjusting export interval based on metric volume
- Use compression for high-volume deployments

## Security

- OTLP endpoints should use HTTPS in production
- Use authentication headers for secure endpoints
- Consider network policies for OTLP traffic
- Monitor OTLP export logs for security events