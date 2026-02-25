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
- **Effectiveness metrics**: Total requests served and cache effectiveness percentage

### Per-Bucket Cache Statistics
- **Bucket stats table**: Hit count, miss count, hit rate, GET TTL, read/write/RAM cache status per bucket
- **Sortable columns**: Click column headers to sort (default: hit count descending)
- **Top 20 display**: Shows top 20 buckets by default with "Show all" toggle
- **Bucket name filter**: Client-side text filter for searching by bucket name
- **Expandable rows**: Click a row to view full resolved settings and prefix overrides
- **Conditional rendering**: Table only appears when at least one bucket has a `_settings.json` file

### Application Log Viewer
- **Recent entries**: Shows most recent 100 log entries by default (configurable)
- **Auto-refresh**: Updates every 10 seconds (configurable)
- **Structured display**: Timestamp, log level, and message content
- **Log level filtering**: Filter by ERROR, WARN, INFO, DEBUG levels
- **Adjustable limits**: Configure display count (50, 100, 200, 500 entries)
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
  bind_address: "0.0.0.0"             # Bind to all interfaces
  cache_stats_refresh_interval: "5s"   # Cache stats refresh rate
  logs_refresh_interval: "10s"         # Log refresh rate
  max_log_entries: 100                 # Maximum log entries to display
```

### Access Control

**No Authentication**: Dashboard is designed as an internal monitoring tool.

**Network Access**:
- `bind_address: "0.0.0.0"` - All interfaces (default)
- `bind_address: "127.0.0.1"` - Localhost only
- Use firewall rules for additional access control

### Performance Settings

**Refresh Intervals**:
- `cache_stats_refresh_interval`: 1-300 seconds (default: 5s)
- `logs_refresh_interval`: 1-300 seconds (default: 10s)
- Lower values = more frequent updates, higher CPU usage

**Log Display**:
- `max_log_entries`: 10-10000 entries (default: 100)
- Higher values = more memory usage, slower loading

## Architecture

### Server Component
- **Dedicated HTTP server**: Runs on separate port from proxy traffic
- **Static file handler**: Serves HTML, CSS, JavaScript assets
- **JSON API handler**: Provides real-time data endpoints
- **Connection management**: Handles up to 10 concurrent users

### Integration Points
- **CacheManager**: Real-time cache statistics
- **LoggerManager**: Application log access
- **MetricsManager**: System metrics and counters
- **ShutdownSignal**: Graceful shutdown coordination

### API Endpoints

**Static Assets**:
- `GET /` - Dashboard HTML interface
- `GET /static/*` - CSS, JavaScript, images

**JSON APIs**:
- `GET /api/cache-stats` - Current cache statistics
- `GET /api/bucket-stats` - Per-bucket cache statistics (hit/miss counts, resolved settings, prefix overrides). Only includes buckets with `_settings.json` files.
- `GET /api/logs` - Recent application log entries
- `GET /api/system-info` - System information

## Performance Impact

### Resource Usage
- **Memory overhead**: <10MB additional memory
- **CPU impact**: Minimal when no users connected
- **Network**: Efficient polling, no persistent connections
- **Disk I/O**: Read-only access to logs and cache metadata

### Scalability
- **Concurrent users**: Up to 10 simultaneous connections
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
  bind_address: "0.0.0.0"    # All interfaces
  cache_stats_refresh_interval: "10s"  # Reduced frequency
  logs_refresh_interval: "30s"
  max_log_entries: 50        # Reduced memory usage
```

### Docker
```yaml
services:
  s3-proxy:
    ports:
      - "80:80"
      - "443:443"
      - "8081:8081"  # Dashboard port
```

## Troubleshooting

### Dashboard Not Accessible
1. Verify `dashboard.enabled: true` in configuration
2. Check port conflicts with `dashboard.port`
3. Verify `bind_address` allows connections from your network
4. Check firewall rules for configured port
5. Ensure proxy is running and dashboard started successfully

### Performance Issues
1. Increase refresh intervals to reduce update frequency
2. Reduce `max_log_entries` for faster log loading
3. Monitor concurrent user count (max 10 supported)
4. Check proxy logs for dashboard-related errors

### Connection Limits
- Dashboard supports maximum 10 concurrent connections
- Additional connections are rejected with HTTP 503
- Use browser refresh if connection limit reached
- Consider increasing refresh intervals to reduce connection frequency

## Security Considerations

### Internal Use Only
- Dashboard provides full access to cache statistics and logs
- No authentication or authorization mechanisms
- Designed for internal monitoring by administrators
- Should not be exposed to public networks

### Network Security
- Use `bind_address: "127.0.0.1"` for localhost-only access
- Configure firewall rules to restrict network access
- Consider VPN or SSH tunneling for remote access
- Monitor access logs for unexpected connections

### Data Exposure
- Cache statistics may reveal usage patterns
- Application logs may contain sensitive information
- Log filtering helps reduce information exposure
- Consider log sanitization for sensitive environments