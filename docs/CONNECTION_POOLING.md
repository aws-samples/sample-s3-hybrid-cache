# Connection Pooling

## Overview

The proxy uses Hyper 1.0's built-in connection pooling to reuse TCP/TLS connections to S3. The `ConnectionPoolManager` handles DNS resolution and IP distribution; hyper manages actual TCP connection lifecycle.

### Architecture
- **Hyper Client**: Manages connection pooling, idle timeout, and reuse automatically
- **CustomHttpsConnector**: Establishes TCP+TLS connections, applies socket options (keepalive, receive buffer) via socket2, records failures in IpHealthTracker
- **ConnectionPoolManager**: DNS resolution, endpoint overrides, per-endpoint IpDistributor (round-robin), hostname lookup for TLS SNI
- **IpHealthTracker**: Lock-free per-IP failure tracking via DashMap. Excludes IPs after consecutive failures; DNS refresh restores them
- **RwLock**: Hot path (get_distributed_ip, get_hostname_for_ip) uses read locks. Write locks only for DNS refresh and IP exclusion

### Configuration

```yaml
connection_pool:
  keepalive_enabled: true        # Enable HTTP connection keepalive (default: true)
  idle_timeout: "55s"            # Just under S3's ~60s server-side timeout (default: 55s)
  max_idle_per_host: 100         # Max idle connections per host when IP distribution disabled (default: 100)
  max_idle_per_ip: 10            # Max idle connections per IP when IP distribution enabled (default: 10)
  dns_refresh_interval: "60s"    # DNS re-resolution interval (default: 60s)
  connection_timeout: "10s"      # TCP connect timeout (default: 10s)
  ip_distribution_enabled: true  # Per-IP connection pools via URI authority rewriting (default: true)
  keepalive_idle_secs: 15        # TCP_KEEPIDLE — seconds idle before first probe (default: 15)
  keepalive_interval_secs: 5     # TCP_KEEPINTVL — seconds between probes (default: 5)
  keepalive_retries: 3           # TCP_KEEPCNT — failed probes before dead (default: 3)
  tcp_recv_buffer_size: null     # SO_RCVBUF hint in bytes; null = kernel auto-tune (default: null)
  ip_failure_threshold: 3        # Consecutive failures before IP exclusion (default: 3)
```

### Key Features

#### 1. Connection Reuse
- First request to an endpoint: full TCP + TLS handshake (~150-300ms)
- Subsequent requests: reuse pooled connection (~0ms overhead)
- Hyper automatically manages pool keyed by (scheme, authority, port)

#### 2. TCP Socket Options (socket2)
Applied after TCP connect, before TLS handshake on every new connection:
- **SO_KEEPALIVE**: Detects dead connections at the TCP layer before hyper tries to reuse them. Configured via `keepalive_idle_secs`, `keepalive_interval_secs`, `keepalive_retries`.
- **SO_RCVBUF**: Left unset by default (`tcp_recv_buffer_size: null`) so the Linux kernel auto-tunes the TCP receive window (up to `net.core.rmem_max`). Auto-tuning is required for full single-stream throughput on high-RTT cross-region paths: with a fixed window the bandwidth-delay product caps throughput (e.g. a 256KB window over a 116ms RTT limits a single stream to ~3.9 Mbps, vs ~132 Mbps auto-tuned). Setting an explicit `tcp_recv_buffer_size` pins `SO_RCVBUF`, which **disables** the kernel's dynamic receive-window auto-tuning (DRS) — only override if you have a specific reason.
- Failures log a warning and continue — socket options are best-effort.

#### 3. IP Health Tracking
- `IpHealthTracker` uses `DashMap<IpAddr, u32>` for lock-free concurrent access
- TCP connect failures, TLS handshake failures, and hyper connection errors increment the counter
- Successful responses reset the counter to zero
- When counter reaches `ip_failure_threshold` (default: 3), the IP is removed from the IpDistributor
- DNS refresh (every `dns_refresh_interval`) rebuilds the distributor with all resolved IPs, restoring excluded ones
- If all IPs are excluded, `get_distributed_ip` returns None and the request falls back to hostname-based routing

#### 4. Idle Timeout
- Default: 55 seconds (aligned with S3's ~60s server-side timeout)
- 5-second safety margin avoids reusing connections S3 is about to close
- Connections idle beyond this are automatically closed by Hyper

#### 5. Per-IP Pool Isolation
- When `ip_distribution_enabled: true`, the proxy rewrites URI authority from hostname to IP
- Hyper creates separate connection pools per IP address
- `max_idle_per_ip` controls idle connections per pool (default: 10)
- TLS SNI and Host headers use the original hostname for AWS SigV4 compatibility

#### 6. Error Recovery
- Connection errors automatically remove failed connections from hyper's pool
- Requests are retried with new connections (up to 3 retries for GET/HEAD)
- Connection errors don't count against retry limit
- IpHealthTracker excludes persistently failing IPs from round-robin

### Metrics
Connection metrics via `/metrics`:

```json
{
  "connection_pool": {
    "connections_created": { "s3.us-west-2.amazonaws.com": 15 },
    "connections_reused": { "s3.us-west-2.amazonaws.com": 142 },
    "idle_timeout_closures": 3,
    "error_closures": 1,
    "dns_refresh_count": 12,
    "ip_addresses": ["52.92.17.224", "52.92.17.225"]
  }
}
```

IP distribution stats via `/health`:

```json
{
  "ip_distribution": {
    "endpoints": [{
      "endpoint": "s3.us-west-2.amazonaws.com",
      "total_distributor_ips": 8,
      "ips": [{"ip": "52.92.17.224", "active_connections": 0, "idle_connections": 0}]
    }]
  }
}
```

### Troubleshooting

#### High Connection Creation Rate
If `connections_created` is high relative to `connections_reused`:
- Check if `idle_timeout` is too short (should be 55s for S3)
- Verify `keepalive_enabled` is true
- Check TCP keepalive settings — dead connections detected late cause pool misses

#### IPs Being Excluded
If IPs are being excluded from the distributor:
- Check application logs for "IP failure threshold reached" messages
- Verify network connectivity to S3 IPs
- Increase `ip_failure_threshold` if transient errors are common
- Reduce `dns_refresh_interval` for faster recovery of excluded IPs

#### Connection Errors
If `error_closures` is increasing:
- Check network stability and firewall/NAT timeout settings
- Verify S3 endpoint is healthy
- TCP keepalive should detect dead connections — check `keepalive_idle_secs` is not too high

### Disabling Connection Keepalive

```yaml
connection_pool:
  keepalive_enabled: false
```

This sets `pool_idle_timeout` and `pool_max_idle_per_host` to 0 in hyper, creating new connections for every request.

### Integration with Other Features
- **Load Balancing**: IpDistributor round-robin preserved across connection reuse
- **DNS Refresh**: Rebuilds distributor with all resolved IPs, restoring excluded ones
- **IP Health Tracking**: Failing IPs excluded from round-robin, auto-recovered on DNS refresh
- **Streaming**: Connections stay active during streaming, returned to pool after
- **Retry Logic**: Connection errors trigger retries without counting against limit

### Upstream Idle Watchdog

The proxy detects stalled upstream connections and terminates them rather than waiting for the client's read-timeout (typically 60s). This prevents a single slow or dead upstream from blocking the client indefinitely.

#### Two-phase detection

1. **Pre-first-byte** (`upstream_first_byte_timeout`, default 5s): After the proxy sends a request to S3 (or another upstream), if no response byte arrives within the timeout, the attempt is aborted and retried up to `upstream_idle_retries` times. After exhausting retries, the proxy returns a 504 Gateway Timeout.

2. **Mid-stream** (`upstream_idle_timeout`, default 5s): Once the upstream has started delivering bytes, if no new chunk arrives within the timeout, the proxy terminates the client connection (the body stream ends prematurely). The client's own retry logic engages. A partial response is never committed to cache.

#### Backpressure exclusion

The mid-stream watchdog measures idle time at the upstream-read boundary. When the proxy is not reading from the upstream because the client write half is applying backpressure (slow client → cache channel full), the idle timer is paused. This prevents false aborts on transfers that are making progress on the client side but where the proxy hasn't asked the upstream for more data yet.

#### Configuration

```yaml
connection_pool:
  upstream_first_byte_timeout: "5s"  # Connect→first-byte (default: 5s)
  upstream_idle_timeout: "5s"        # Mid-stream no-bytes watchdog (default: 5s)
  upstream_idle_retries: 2           # Pre-stream retry budget (default: 2)
```

#### Coordination wait exclusion

The watchdog timers are armed only after the proxy owns the upstream byte stream. Requests waiting on the download coordinator for another request's in-flight fetch are governed by the coordination wait timeout (~30s), not the idle watchdog.

#### Cache safety

An aborted mid-stream fetch never commits a *truncated full range* to cache — partial bytes are never served as if they satisfied a larger request. As of v2.2.0, however, the proxy does commit the received bytes as a **smaller valid sub-range** when at least `cache.partial_range_commit_ratio` (default `0.5`) of the requested bytes arrived in order before the abort. The incremental writer detects the size shortfall: if the received fraction meets the threshold it renames the temp file with clamped bounds and journals it as a valid sub-range `[start, start + received - 1]`; if it falls below the threshold the temp file is deleted and nothing is cached. See `cache.partial_range_commit_ratio` in `CONFIGURATION.md`.
