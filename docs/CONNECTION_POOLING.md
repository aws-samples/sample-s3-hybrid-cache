# Connection Pooling

## Connection Keepalive Implementation

### Overview
The S3 proxy now implements true HTTP connection keepalive (persistent connections) using Hyper 1.0's built-in connection pooling. This eliminates the TCP and TLS handshake overhead for subsequent requests to the same S3 endpoint.

### Architecture
- **Hyper Client**: Manages connection pooling automatically
- **CustomHttpsConnector**: Integrates with ConnectionPoolManager for IP selection and load balancing
- **Per-IP Pooling**: Separate connection pools maintained for each resolved IP address
- **Automatic Lifecycle**: Hyper handles connection reuse, idle timeout, and cleanup

### Configuration
Connection keepalive is configured in the `connection_pool` section of the configuration file:

```yaml
connection_pool:
  # Enable HTTP connection keepalive (default: true)
  keepalive_enabled: true
  
  # Idle timeout for unused connections (default: 30s)
  idle_timeout: "30s"
  
  # Maximum idle connections per host/IP (default: 1)
  max_idle_per_host: 1
  
  # Maximum connection lifetime (default: 300s)
  max_lifetime: "300s"
  
  # Connection pool maintenance check interval (default: 10s)
  pool_check_interval: "10s"
```

### Key Features

#### 1. Connection Reuse
- First request to an endpoint: Full TCP + TLS handshake (~150-300ms overhead)
- Subsequent requests: Reuse existing connection (~0ms overhead)
- Expected latency reduction: 60-70% for subsequent requests

#### 2. Idle Timeout
- Connections idle beyond `idle_timeout` are automatically closed by Hyper
- Prevents resource waste from unused connections
- Default: 30 seconds

#### 3. Max Lifetime
- Background task checks for connections exceeding `max_lifetime`
- Ensures connections are rotated periodically
- Handles DNS changes and connection staleness
- Default: 300 seconds (5 minutes)

#### 4. Per-IP Pool Isolation
- Each resolved IP address has its own connection pool
- Preserves load balancing across multiple S3 IPs
- DNS refresh adds new pools for new IPs and removes old ones

#### 5. Error Recovery
- Connection errors automatically remove failed connections
- Requests are retried with new connections
- Connection errors don't count against retry limit
- Metrics track error closures for monitoring

### Metrics
Connection keepalive exposes the following metrics via `/metrics`:

```json
{
  "connection_pool": {
    "connections_created": {
      "s3.amazonaws.com": 15,
      "s3.us-west-2.amazonaws.com": 8
    },
    "connections_reused": {
      "s3.amazonaws.com": 142,
      "s3.us-west-2.amazonaws.com": 67
    },
    "idle_timeout_closures": 3,
    "max_lifetime_closures": 2,
    "error_closures": 1
  }
}
```

### Performance Expectations

#### Latency Improvements
- **First request**: 250-350ms (includes TCP + TLS handshake)
- **Subsequent requests**: 100-150ms (connection reused)
- **Improvement**: 150-200ms reduction (60-70% faster)

#### Throughput Improvements
- **Without keepalive**: ~50 requests/second per connection
- **With keepalive**: ~75-100 requests/second per connection
- **Improvement**: 50-100% throughput increase

#### Resource Usage
- **Memory**: ~50KB per idle connection
- **File descriptors**: 1 per connection
- **With default settings (1 idle per IP)**: ~50KB memory, 1 FD per IP
- **Minimal overhead** for significant performance gains

### Troubleshooting

#### High Connection Creation Rate
If `connections_created` is high relative to `connections_reused`:
- Check if `idle_timeout` is too short
- Verify `max_idle_per_host` is not set to 0
- Ensure `keepalive_enabled` is true

#### Connection Errors
If `error_closures` is increasing:
- Check network stability
- Verify S3 endpoint is healthy
- Review connection timeout settings
- Check for firewall/NAT timeout issues

#### Memory Usage
If memory usage is high:
- Reduce `max_idle_per_host`
- Decrease `idle_timeout` to close connections sooner
- Monitor `idle_connections` metric

### Disabling Connection Keepalive
To disable connection keepalive and revert to creating new connections per request:

```yaml
connection_pool:
  keepalive_enabled: false
```

This will:
- Set `pool_idle_timeout` to 0 in Hyper client
- Set `pool_max_idle_per_host` to 0
- Create new connections for every request
- Useful for debugging or specific deployment scenarios

### Integration with Existing Features
Connection keepalive integrates seamlessly with:
- **Load Balancing**: IP selection via ConnectionPoolManager preserved
- **DNS Refresh**: New IPs get new pools, removed IPs have connections closed
- **Health Monitoring**: Connection metrics tracked per endpoint
- **Streaming**: Connections stay active during streaming, returned to pool after
- **Retry Logic**: Connection errors trigger retries without counting against limit
