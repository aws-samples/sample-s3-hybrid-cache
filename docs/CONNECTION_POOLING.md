# Connection pooling

Hybrid Cache for Amazon S3 keeps its connections to S3 open and reuses them, spreads requests across the IP addresses that S3's DNS returns, and takes an address out of rotation when connections to it keep failing. This page explains how pooling, IP distribution, health tracking and the upstream timeouts behave, and how to read their metrics. [Connection pooling configuration](CONFIGURATION.md#connection-pooling) is the field reference.

## How it fits together

- The HTTP client keeps a pool of open connections per endpoint, reuses them and closes idle ones.
- The connector opens each new TCP and TLS connection, sets the socket options, and records connection failures against the IP address it dialed.
- The pool manager resolves endpoint hostnames, applies `endpoint_overrides`, and keeps a round-robin list of addresses (the distributor) for each endpoint.
- The health tracker counts consecutive failures per IP address, takes an address out of rotation at a threshold, and returns it after a successful recovery probe.

## Configuration

The pool's fields and their defaults. [Connection pooling configuration](CONFIGURATION.md#connection-pooling) gives each field's type and valid range.

```yaml
connection_pool:
  keepalive_enabled: true        # Reuse connections (default: true)
  idle_timeout: "55s"            # How long an idle connection stays pooled, 1-300s (default: 55s)
  max_lifetime: "300s"           # Age at which a pooled connection is retired (default: 300s)
  max_idle_per_host: 100         # Idle connections per host when IP distribution is off (default: 100)
  max_idle_per_ip: 10            # Idle connections per IP address when IP distribution is on (default: 10)
  dns_refresh_interval: "60s"    # DNS re-resolution interval (default: 60s)
  connection_timeout: "10s"      # TCP connect timeout (default: 10s)
  ip_distribution_enabled: true  # One pool per IP address, by rewriting the URI authority (default: true)
  keepalive_idle_secs: 15        # TCP_KEEPIDLE: seconds idle before the first probe (default: 15)
  keepalive_interval_secs: 5     # TCP_KEEPINTVL: seconds between probes (default: 5)
  keepalive_retries: 3           # TCP_KEEPCNT: failed probes before the connection is dead (default: 3)
  tcp_recv_buffer_size: null     # SO_RCVBUF in bytes; null leaves it to kernel auto-tuning (default: null)
  ip_failure_threshold: 3        # Consecutive failures before an address is excluded (default: 3)
  pool_check_interval: "10s"     # DNS refresh and recovery probe tick (default: 10s)
  health_probe_initial_cooldown: "5s"    # Wait before an excluded address is first probed (default: 5s)
  health_probe_max_cooldown: "300s"      # Ceiling for the doubling probe backoff (default: 300s)
```

## Pool behavior

### 1. Connection reuse

- The first request to an endpoint pays a full TCP and TLS handshake, several round trips to S3.
- Later requests reuse a pooled connection with no handshake.
- The client keys its pools by scheme, host and port. A connection stays checked out while a response streams, and returns to the pool when the body ends.

### 2. TCP socket options

The connector sets these on every new connection, after the TCP connect and before the TLS handshake:

- **SO_KEEPALIVE** detects dead connections at the TCP layer before the client tries to reuse them. `keepalive_idle_secs`, `keepalive_interval_secs` and `keepalive_retries` configure it.
- **SO_RCVBUF** is left unset by default (`tcp_recv_buffer_size: null`), so the Linux kernel auto-tunes the TCP receive window, up to `net.core.rmem_max`. Auto-tuning is what lets one stream reach full throughput on a high-latency cross-Region path. A fixed window caps a single stream at window divided by round-trip time: a 256 KiB window over a 116 ms round trip is about 18 Mbit/s. Setting `tcp_recv_buffer_size` pins `SO_RCVBUF` and turns off the kernel's dynamic receive-window auto-tuning (DRS), so set it only for a specific reason.
- Setting a socket option is best effort: a failure logs a warning and the connection continues.

### 3. IP health tracking

- TCP connect failures, TLS handshake failures and HTTP connection errors increment the address's failure count. A successful response resets it to zero.
- When the count reaches `ip_failure_threshold` (default 3), the address is removed from the distributor of the hostname whose request failed.
- Health state belongs to the address. Once an address is excluded, each hostname's next DNS refresh leaves it out of that hostname's distributor until a recovery probe confirms it. A client whose `Host` the upstream certificate does not cover can therefore briefly take a shared address out of rotation for other hostnames. Their requests keep succeeding, on the remaining addresses or through hostname-based routing.
- A DNS refresh rebuilds the distributor from the resolved addresses minus any address still excluded, so a refresh never undoes an exclusion.
- If every address is excluded, requests fall back to hostname-based routing: the connector resolves the hostname and dials the first address. For a hostname covered by `endpoint_overrides`, that is the entry's first listed IP, whether or not it is excluded.

### 3a. Recovery probing

An excluded address returns to rotation only when a probe confirms it is reachable.

- After `health_probe_initial_cooldown` (default `5s`), an excluded address becomes a probe candidate.
- The proxy probes candidates once per `pool_check_interval` (default `10s`), right after the DNS refresh, so this interval only bounds how soon an elapsed cooldown is noticed.
- A probe performs a full TCP connect and TLS handshake to the port and transport that live traffic would use, honoring any `upstream_overrides` entry for the destination. A handshake failure is one of the errors that excludes an address, so a connect-only check would readmit a host that accepts TCP and then fails to negotiate TLS.
- A probe is bounded by `connection_timeout` (default `10s`). It opens no pooled connection, records no connection metrics and does not touch the failure count.
- When a probe succeeds, the exclusion clears and the address goes straight back into its distributor.
- When a probe fails, the cooldown doubles, up to `health_probe_max_cooldown` (default `300s`), so an unreachable address is retried less and less often.
- An excluded address that disappears from DNS is dropped from tracking rather than probed.

### 4. Idle timeout

- Default 55 seconds; valid range 1 to 300 seconds. The client closes connections that stay idle longer.
- S3 can close an idle connection before `idle_timeout` expires, and a request sent on a connection S3 has just closed fails before any response arrives. A GET or HEAD without a body that meets one is retried once on a new connection (see [Error recovery](#6-error-recovery)).
- A lower `idle_timeout` makes reusing a connection S3 has closed less likely, at the cost of more new connections.

### 4a. Maximum lifetime

- Default 300 seconds, measured from when the connection was established.
- A connection that has reached `max_lifetime` is retired from the pool, so no further request gets it; the next request to that endpoint opens a new connection.
- Retirement happens at checkout, never mid-request. The request already running on the connection completes normally, and a response body still streaming from it streams to the end, however long the transfer runs.
- The limit is per connection: one connection reaching its lifetime says nothing about the others in the same pool.
- `max_lifetime_closures` in `/metrics` counts retirements.
- Unlike `idle_timeout`, this reclaims a connection that is in continuous use. It bounds how long the proxy keeps sending traffic down one connection to one S3 address, so a connection cannot outlive the DNS answer that produced it by more than the limit.

### 5. Per-IP pool isolation

- With `ip_distribution_enabled: true`, the proxy rewrites the request's URI authority from the hostname to an IP address, and keeps the hostname alongside it so the connection keeps the identity the request asked for.
- The client keeps a separate pool for each pair of hostname and IP address, so two bucket hostnames that share an address keep their own pools. Round-robin across the addresses carries on with pooled connections, because each address has its own pool.
- `max_idle_per_ip` sets the idle connections per pool (default 10).
- TLS SNI, certificate validation and the `Host` header all use the hostname the request targeted. That keeps the AWS SigV4 signature valid, and a certificate covering that hostname is all a connection needs, except for a bucket whose name contains a dot (next item).
- For a bucket whose name contains a dot, addressed virtual-hosted style (`my.bucket.s3.us-west-2.amazonaws.com`), TLS SNI and certificate validation use the S3 endpoint name without the bucket (`s3.us-west-2.amazonaws.com`), because S3's certificate does not cover such hostnames. The `Host` header and the signature stay as the client sent them, and S3 routes the request by its `Host` header.

### 6. Error recovery

- A connection error removes the failed connection from the pool.
- Connection errors, timeouts and S3 5xx and 429 responses are retried on a new connection, up to 3 retries for GET and HEAD. Every retry counts against that limit.
- A GET or HEAD without a body whose established connection is lost after the request went out is first retried once, immediately, on a new connection that bypasses the pool. The usual cause is a pooled connection that S3 had already closed while it was idle. That first loss does not count toward `ip_failure_threshold`, since S3 closes idle connections on every address alike; a loss on the retry does. Requests with a body are never retried this way.
- The health tracker excludes persistently failing addresses from the round-robin, as described in [IP health tracking](#3-ip-health-tracking).

## Disabling connection keepalive

```yaml
connection_pool:
  keepalive_enabled: false
```

This sets the client's pool idle timeout and its idle connections per host to 0, so every request opens a new connection.

## Upstream idle watchdog

The proxy detects a stalled upstream connection and ends it, rather than leaving the client to wait for its own read timeout (60 seconds by default in the AWS CLI and botocore). One slow or dead upstream connection therefore cannot hold a client indefinitely.

### Two-phase detection

1. **Before the first byte** (`upstream_first_byte_timeout`, default `5s`): if no response byte arrives within the timeout after the proxy sends a request upstream, the attempt is aborted and retried, up to `upstream_idle_retries` times. When the retries run out, the proxy answers `504 Gateway Timeout`.
2. **Mid-stream** (`upstream_idle_timeout`, default `5s`): once the upstream has started sending, if no new chunk arrives within the timeout, the proxy closes the client connection and the body ends early. The client's own retry logic takes over. [Cache safety](#cache-safety) describes what is cached.

```yaml
connection_pool:
  upstream_first_byte_timeout: "5s"  # Connect to first byte (default: 5s)
  upstream_idle_timeout: "5s"        # Longest gap between body chunks (default: 5s)
  upstream_idle_retries: 2           # Retries before the first byte (default: 2)
```

### Backpressure exclusion

The mid-stream timer measures idle time where the proxy reads from the upstream. When a slow client fills the cache channel, the proxy stops reading from the upstream and the timer pauses. A transfer that is still progressing on the client side is therefore never aborted while the proxy has not asked the upstream for more data.

### Coordination wait exclusion

The watchdog's timers start only once the proxy owns the upstream byte stream. A request waiting for another request's in-flight fetch of the same object is bounded by the download coordination wait timeout (`cache.download_coordination.wait_timeout_secs`, default 30 seconds), not by the watchdog.

### Cache safety

An aborted mid-stream fetch never caches a truncated range under the bounds that were requested, and partial bytes are never served as if they covered a larger request. The proxy keeps the received bytes as a smaller valid range when at least `cache.partial_range_commit_ratio` (default `0.5`) of the requested bytes arrived in order before the abort. It then caches them as the range `[start, start + received - 1]`; below the ratio it deletes the temporary file and caches nothing. See [Partial range commit ratio](CONFIGURATION.md#partial-range-commit-ratio).

## Hedged upstream requests

When a cache rule enables hedging for a key, the proxy can send a second, identical request when the first is slow to return its first byte, and serve whichever answers first. The hedge prefers a different IP address from the original, chosen through the same distributor, which is what makes it effective against one slow connection or one degraded address. If only one healthy address is available, both attempts use it on separate connections; if the destination matches an `upstream_overrides` entry, neither attempt is pinned to an address.

[Hedged upstream requests](HEDGING.md) is the guide: when hedging helps, its rule fields and cost governor, and [how it relates to the upstream timeouts](HEDGING.md#relationship-to-the-upstream-timeouts). Each upstream request also has a fixed 30-second timeout from connect to response. No timer covers a whole client request: `server.request_timeout` has no effect, as described in [`request_timeout` is not enforced](CONFIGURATION.md#request_timeout-is-not-enforced).

## Metrics

`/metrics` reports the pool under `connection_pool`; [`connection_pool`](METRICS_REFERENCE.md#connection_pool) is the field reference.

```json
{
  "connection_pool": {
    "connections_created": { "s3.us-west-2.amazonaws.com": 15 },
    "connections_reused": { "s3.us-west-2.amazonaws.com": 142 },
    "idle_timeout_closures": 0,
    "max_lifetime_closures": 2,
    "error_closures": 1,
    "dns_refresh_count": 12,
    "ip_addresses": ["52.92.17.224", "52.92.17.225"]
  }
}
```

Both per-endpoint maps are keyed by the hostname the proxy dialed and count since the process started. `connections_created` counts requests that had to open a new upstream connection, and `connections_reused` counts requests sent over a connection already in the pool. Together they account for the requests forwarded to that endpoint: `15` and `142` above describe 157 forwarded requests, 15 of which needed a new connection. Neither map resets, so every scrape includes the startup burst, when there is no connection to reuse yet.

`idle_timeout_closures` always reads zero: nothing in the proxy attributes a pool closure to the idle timeout. Read `max_lifetime_closures` and `error_closures`, which are counted, and do not conclude from the zero that connections outlive `idle_timeout`.

`/health` reports the IP distribution:

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

## Troubleshooting

### High connection creation rate

Act on the share of forwarded requests that needed a new connection: `connections_created` over `connections_created + connections_reused`, for one endpoint key. Compute it from the differences between two scrapes taken minutes apart, not from the totals. A share taken from one scrape is dominated by startup, when the pool is empty and every connection is new, so it looks alarming on a pool that works. Watch the rate at which new connections appear once the process is warm. The size of `connections_created` alone says nothing, because it grows with traffic on a healthy pool too.

The share is only as good as the keys it is computed over, and the two maps are written by different code. Creations are keyed by the name used for the TLS handshake, and reuses by the host the request was signed for. Where those differ, one endpoint occupies two keys and each key sees part of its traffic. Two signs that a key's share is not worth acting on:

- `connections_reused` holds a key that `connections_created` lacks, or one whose `connections_created` entry stays flat while reuse climbs. That reads as a pool at 100% reuse; on a busy endpoint it more likely means the creations are filed under another key.
- `connections_created` holds an IP address key (`52.92.17.224`) that does not appear in `connections_reused`. That is the other half of the same split, and it happens when the proxy cannot map a distributed address back to its hostname.

The cheapest fleet-wide check for the second case is to search the application log (`{app_log_dir}/{hostname}/s3-proxy.log.{date}`) for `No hostname mapping found for IP`. When the key sets match and both maps grow, the share means what it says.

In steady state the share should be small and roughly flat. If it stays high on a warm process:

- check whether `idle_timeout` is too short (the default is `55s`);
- confirm `keepalive_enabled` is `true`;
- check the TCP keepalive settings, since dead connections detected late cause pool misses;
- check whether `max_lifetime_closures` or `error_closures` climbs at a similar rate. Those say why connections left the pool, and one new connection per request means something retires them at that pace;
- with `ip_distribution_enabled`, each address has its own pool sized by `max_idle_per_ip` rather than `max_idle_per_host`, so a distributor spread over many addresses needs more idle slots in total to keep the same reuse rate.

### IP addresses being excluded

If addresses are being excluded from the distributor:

- look in the application log for `IP failure threshold reached`;
- check network connectivity to the S3 addresses;
- raise `ip_failure_threshold` if transient errors are common;
- to bring an excluded address back sooner, lower `health_probe_initial_cooldown`. Lowering `dns_refresh_interval` does not help, because a DNS refresh never restores an excluded address; only a successful probe does;
- look for `Recovery probe failed` (the cooldown was extended) and `Recovery probe succeeded` to see whether an address is being retried and why it is still out.

### Connection errors

`Retried a request on a fresh connection after its connection was lost` at INFO (`outcome="recovered"`) means a GET or HEAD met a connection S3 had closed and succeeded on a new one. At WARN (`outcome="failed"`) the retry failed too. A steady rate of the INFO line suggests lowering `idle_timeout`.

If `error_closures` keeps increasing:

- check network stability and firewall or NAT timeout settings;
- check that the S3 endpoint is healthy;
- check that `keepalive_idle_secs` is not so high that TCP keepalive finds dead connections too late.

## See also

- [Connection pooling configuration](CONFIGURATION.md#connection-pooling): every field, default and valid range
- [Upstream timeout](CONFIGURATION.md#upstream-timeout-stalled-response-fast-fail): the first-byte and idle timeout fields
- [Hedged upstream requests](HEDGING.md): second attempts against slow first bytes
- [Error handling](ERROR_HANDLING.md#7-stalled-upstream-responses-mid-stream-idle-watchdog): what a stalled upstream looks like in metrics and logs
