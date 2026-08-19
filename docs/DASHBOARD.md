# Web Dashboard

Real-time web-based monitoring interface for S3 proxy cache performance and application logs.

## Overview

The dashboard provides a lightweight, browser-based interface for monitoring proxy status without requiring authentication. It serves static HTML/CSS/JavaScript and provides JSON APIs for real-time data updates.

## Features

### Cache Statistics
- **Real-time metrics**: RAM and disk cache hit rates, miss rates, current sizes
- **Auto-refresh**: Updates every 5 seconds (configurable)
- **Human-readable formatting**: Displays sizes in KB, MB, GB units
- **Eviction tracking**: Shows eviction counts and recently evicted items
- **Requests tile**: `Requests: N / M` shows concurrency permits held against the configured `server.max_concurrent_requests`, with the TCP connection count displayed alongside. Before 2.5.0 the numerator was the connection count, which does not track what the limit bounds — with HTTP/1 keep-alive one connection serves many sequential requests. See [Metrics](METRICS.md) for the underlying `permits_held` and `permits_held_peak` fields.
- **Effectiveness metrics**: Overall Statistics card shows total requests served (GET + HEAD + PUT), GET hits, GET misses, and cache hit rate. The hit-rate denominator counts every GET/HEAD that flows through the proxy — including list-object GETs, conditional requests, and non-cacheable or error responses (e.g. 404/403) — so the displayed rate is the fraction of all GET/HEAD traffic served from cache, not of cacheable object reads alone. Per-bucket traffic table shows GET/PUT request counts, bytes downloaded to clients, bytes saved (S3 transfers avoided via cache), and bytes uploaded.

### Cache Rules
- **Rules-only table**: Lists the active cache rules from `cache_rules.json` in evaluation order, each with its configured settings and HEAD/GET hit rate. Only appears when at least one rule is configured.
- **Rule rows**: One row per rule, shown in first-match-per-field order. The "Rule / Pattern" column holds the glob pattern; HEAD and GET columns show per-rule hit summaries (e.g. `72.3% of 4 102`)
- **Pattern filter**: Client-side text filter that matches rule patterns
- **Top 20 display**: Shows the top 20 rows by default with a "Show all" toggle
- **Expandable settings**: Click "Settings" on a rule row to view the fields that rule sets (GET/HEAD/PUT TTL, read/write/compression/RAM, "Local conditions" for `evaluate_conditions_from_cache`, and page widening + page size for [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching)). Only fields the rule actually sets are shown; unset fields fall through to the global defaults and are omitted. "Local conditions: On" means the proxy answers conditional requests for matching keys from cached metadata rather than forwarding them to S3 — worth noting when auditing which prefixes skip S3-side credential revalidation.

### Page-Aligned Range Caching Statistics

The `page_cache` counters (widened requests, bytes prefetched, page hits, signed-range
skips, fallbacks, and RAM page promotions) are returned in the `/api/cache-stats` JSON
payload. There is no dedicated dashboard card for them yet — read them from that
endpoint or from `/metrics`. Per-rule page settings *are* visible in the Cache Rules
table via the Settings expander, described above.

### Application Log Viewer
- **Recent entries**: Shows the most recent 100 log entries by default
- **Auto-refresh**: Updates every 10 seconds (`logs_refresh_interval`)
- **Structured display**: Timestamp, log level, and message content
- **Log level filtering**: Filter by ERROR, WARN, INFO, DEBUG levels
- **Adjustable limit**: An in-page dropdown selects 50, 100, 200, or 500 entries and is passed to `/api/logs?limit=`. This is a **browser-side control**, which is why the `dashboard.max_log_entries` config field is inert — the viewer's cap comes from the dropdown, not from configuration.
- **Structured data formatting**: Key-value pairs displayed in readable format

### System Information
- **Instance details**: Hostname, version, uptime
- **Navigation**: Menu with Cache Stats and Application Logs sections
- **Error handling**: User-friendly error messages with retry options
- **Responsive design**: Works on desktop and mobile browsers

## Configuration

### Basic Settings

```yaml
dashboard:
  enabled: true                        # Enable dashboard server
  port: 8081                          # Dashboard server port
  bind_address: "127.0.0.1"           # Default: loopback only
  cache_stats_refresh_interval: "5s"   # Cache stats refresh rate
  logs_refresh_interval: "10s"         # Log refresh rate
  max_log_entries: 100                 # DEPRECATED: parsed but has no effect
```

### Access Control

**No Authentication**: The dashboard is unauthenticated and read-only — it exposes cache statistics and log excerpts, not credentials or control operations. Access control is network-level.

**Network Access**:
- `bind_address: "127.0.0.1"` - Localhost only (code default; safest)
- `bind_address: "0.0.0.0"` - All interfaces (requires network-layer restriction)
- Use firewall rules or security groups to restrict access when non-loopback
- SSH tunnel provides secure remote access without exposing the port

### Performance Settings

**Refresh Intervals**:
- `cache_stats_refresh_interval`: 1-300 seconds (default: 5s)
- `logs_refresh_interval`: 1-300 seconds (default: 10s)
- Lower values = more frequent updates, higher CPU usage

**Log Display**:
- `max_log_entries` is **deprecated**. It is parsed and range-validated (10-10000) and logged at startup, but the log viewer does not use it to cap output. It will be removed in a future release.

## Architecture

### Server Component
- **Dedicated HTTP server**: Runs on separate port from proxy traffic
- **Static file handler**: Serves HTML, CSS, JavaScript assets
- **JSON API handler**: Provides real-time data endpoints
- **Connection management**: Handles up to 50 concurrent users

### Integration Points
- **CacheManager**: Real-time cache statistics
- **LoggerManager**: Application log access
- **MetricsManager**: System metrics and counters
- **ShutdownSignal**: Graceful shutdown coordination

### API Endpoints

**Static Assets**:
- `GET /` - Dashboard HTML interface
- `GET /style.css` - Stylesheet
- `GET /script.js` - JavaScript

**JSON APIs** (six endpoints):
- `GET /api/cache-stats` - Cache statistics, including the global hit/miss counters and the `page_cache` counters. Returns `overall` with `total_requests` (GET + HEAD + PUT), `get_hits`, `get_misses`, `get_total`, `head_hits`, `head_misses`, `head_total`, `put_total`, `cache_hit_rate`, `s3_requests_saved`, and `bytes_served_from_cache`, plus the per-tier size and eviction figures the statistics cards render.
- `GET /api/bucket-stats` - Cache rules and per-rule hit/miss stats. Returns `rules` (the ordered rule list, each with the fields it sets plus HEAD/GET hit and miss counts). Only populated when `cache_rules.json` has at least one rule.
- `GET /api/bucket-traffic` - Per-bucket traffic counters. Returns `bucket_traffic` map keyed by `bucket` or `bucket/prefix`, each entry with `get_requests`, `put_requests`, `bytes_served` (all GET bytes to clients), `bytes_saved` (GET bytes served from cache, i.e. S3 transfers avoided), and `bytes_uploaded` (PUT/UploadPart bytes from clients). See [METRICS.md](METRICS.md).
- `GET /api/bandwidth` - Download bandwidth QoS state. Returns `instance_ceiling_bps`, `class_bytes`, `residual_bytes`, and `failopen_total`. See [BANDWIDTH_QOS.md](BANDWIDTH_QOS.md).
- `GET /api/logs` - Recent application log entries. Accepts `?limit=` (50/100/200/500) and `?level=` (ERROR/WARN/INFO/DEBUG).
- `GET /api/system-info` - Hostname, version, and uptime

## Performance Impact

### Resource Usage
- **Memory overhead**: <10MB additional memory
- **CPU impact**: Minimal when no users connected
- **Network**: Efficient polling, no persistent connections
- **Disk I/O**: Read-only access to logs and cache metadata

### Scalability
- **Concurrent users**: Up to 50 simultaneous connections
- **Auto-refresh**: Staggered updates to prevent thundering herd
- **Graceful degradation**: Continues serving under proxy load
- **Non-blocking**: Does not impact main proxy operations

## Deployment

### Development
```yaml
dashboard:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"  # Localhost only
  cache_stats_refresh_interval: "2s"   # Faster updates
  logs_refresh_interval: "5s"
```

### Production
```yaml
dashboard:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"  # Loopback; reach it over an SSH tunnel
  cache_stats_refresh_interval: "10s"  # Reduced frequency
  logs_refresh_interval: "30s"
```

The dashboard is unauthenticated and exposes cache statistics and application log
content, so the recommended production posture is to leave it on loopback and reach
it through an SSH tunnel (see [Security Considerations](#security-considerations)).
Binding `0.0.0.0` is supported, but then the port needs a firewall or a reverse proxy
in front of it.

## Troubleshooting

### Dashboard Not Accessible
1. Verify `dashboard.enabled: true` in configuration
2. Check port conflicts with `dashboard.port`
3. Verify `bind_address` allows connections from your network
4. Check firewall rules for configured port
5. Ensure proxy is running and dashboard started successfully

### Performance Issues
1. Increase refresh intervals to reduce update frequency
3. Monitor concurrent connection count (50 concurrent connections are accepted; beyond
   that, new connections are rejected and the rejection is logged)
4. Check proxy logs for dashboard-related errors

### Connection Limits
- Dashboard supports maximum 50 concurrent connections
- Additional connections are rejected once the limit is reached, and each rejection is logged at WARN with the peer address
- Use browser refresh if connection limit reached
- Consider increasing refresh intervals to reduce connection frequency

## Security Considerations

### Unauthenticated, Read-Only Interface

The dashboard is unauthenticated and read-only. It provides cache statistics, application log excerpts, and system information (hostname, version, uptime). It exposes no write operations, no credentials, and no control-plane actions. Presigned URL parameters are masked before they reach the log viewer, so signatures, credentials, and session tokens are redacted rather than displayed.

The security posture depends entirely on who can reach the port:

| `bind_address` | Who can reach it | When to use |
|---|---|---|
| `127.0.0.1` (code default) | Only the local host | Production — access via SSH tunnel |
| `0.0.0.0` | Any host with network access | Only when security groups / firewall rules restrict the port to administrators |

**Production recommendation**: bind to `127.0.0.1` and use an SSH tunnel for remote access:

```bash
ssh -L 8081:127.0.0.1:8081 proxy-host
```

Then open `http://localhost:8081` in your browser.

If you bind to `0.0.0.0`, you **must** restrict access at the network layer (security groups, NACLs, firewall rules) to prevent unauthorized monitoring.

### Internal Use Only
- Dashboard provides full access to cache statistics and logs
- No authentication or authorization mechanisms
- Designed for internal monitoring by administrators
- Should not be exposed to public networks

### Network Security
- Use `bind_address: "127.0.0.1"` for localhost-only access (code default)
- Configure firewall rules to restrict network access when using `0.0.0.0`
- Consider VPN or SSH tunneling for remote access
- Monitor access logs for unexpected connections

### Data Exposure
- Cache statistics may reveal usage patterns
- Application logs may contain sensitive information (presigned params are masked)
- Log filtering helps reduce information exposure
- Consider log sanitization for sensitive environments
