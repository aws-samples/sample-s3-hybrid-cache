# Dashboard

The dashboard is a read-only web page that shows a proxy's cache statistics, per-bucket traffic,
active cache rules and recent application log entries. It is one of the three monitoring
listeners, with health and metrics. It requires no authentication, so it binds to `127.0.0.1` by
default; read [Security considerations](#security-considerations) before binding it to any other
address.

## Turn it on

```yaml
dashboard:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"            # the default: loopback only
  cache_stats_refresh_interval: "5s"   # how often an open page polls cache statistics
  logs_refresh_interval: "10s"         # how often an open page polls the log viewer
```

Both refresh intervals accept 1 to 300 seconds; the defaults are `5s` and `10s`. An open page
polls the proxy at these intervals, so longer intervals send it fewer requests. `max_log_entries`
is accepted and validated (10 to 10000) but has no effect, and a value other than `100` logs a startup warning to delete it: the selector on the page sets how many
log entries the viewer shows. Every dashboard field is in
[Dashboard configuration](CONFIGURATION.md#dashboard-configuration).

Open `http://localhost:8081` on the proxy host, or through an SSH tunnel (see
[Security considerations](#security-considerations)).

## What it shows

### Cache statistics

- **Hit and miss rates and sizes** for the RAM cache and the disk cache, refreshed every
  `cache_stats_refresh_interval` and shown in human-readable units.
- **Eviction counts** for the RAM range cache, the metadata cache and the disk cache.
- **Write Cache:** the space used by multipart uploads in progress and PUT objects not yet read,
  against its limit. The limit is `cache.write_cache_percent` of `cache.max_cache_size`, the
  bound the proxy enforces. See
  [Write cache configuration](CONFIGURATION.md#write-cache-configuration).
- **Requests tile:** `Requests: N / M` shows concurrency permits held against
  `server.max_concurrent_requests`, with the TCP connection count beside it. The numerator is
  permits because with HTTP/1 keep-alive one connection serves many sequential requests, so a
  connection count does not track what the limit bounds. See
  [Request completion metrics](METRICS.md#request-completion-metrics) for `permits_held` and
  `permits_held_peak`.
- **Overall statistics card:** total requests served (GET, HEAD and PUT), GET hits, GET misses
  and the cache hit rate. The hit-rate denominator counts every GET and HEAD through the proxy,
  including list requests, conditional requests and error responses such as 404 and 403, so the
  rate is the share of all GET and HEAD traffic served from cache, not of cacheable object reads
  alone.
- **Per-bucket traffic table:** GET and PUT request counts, bytes downloaded by clients, bytes
  saved (S3 transfers the cache avoided) and bytes uploaded. See
  [Per-bucket traffic metrics](METRICS.md).

### Staged write tier

A card on the Cache Statistics tab, shown whenever the metrics manager is available. It reports
on staged objects, the write-cache objects not yet read, and every figure on it is fleet-wide
because it comes from the cache volume.

- **Staged Ranges:** write-cached ranges against the staged range limit, with the percentage.
  The write cache declines new objects once the limit is reached, so this row is a capacity signal; it
  turns to a warning at 75%. The limit is read from the running proxy (`staged_tier.range_budget`
  in the API payload), so the card always shows the limit in force.
- **Figure State:** `CURRENT`, `STALE` or `UNKNOWN`. `STALE` is a complete figure for an older
  state, shown with the count of shards past the age bound. `UNKNOWN` means a staged input could
  not be read, so the occupancy above is the last complete figure, labelled as retained.
- **Authority Age:** the age of the stalest shard behind the figure, against its bound, measured
  from the last successful install, never from a pass start or a failed attempt.

The card also shows staged bytes, in-progress multipart bytes, budget declines, fold shard counts
and mean visit duration, whether the per-pass shard budget is being clipped, the staged-authority
epoch in the card heading, and the state of the repair rotation. `Repair Rotation: UNREACHABLE`
is the one permanent condition on the card: every other signal resolves by waiting, while this
one means the staged-authority epoch cannot advance at this shard population.

The last row, **Authority Memo**, shows read cost rather than correctness. The staged figure is
cached for a few seconds, so a dashboard refresh, a `/metrics` scrape and a write do not each
re-read the same state from the cache volume. A low hit count against a high recompute count
means those reads are still being paid for.

Every field is in the `/api/cache-stats` payload under `staged_tier`, and the same figures are on
`/metrics` under `write_cache`; see the [Metrics reference](METRICS_REFERENCE.md#write_cache).

### Cache rules

- **Rules table:** the active rules from `cache_rules.json` in evaluation order, each with its
  settings and its HEAD and GET hit rates (for example `72.3% of 4 102`). The table appears only
  when at least one rule is configured.
- **Pattern filter:** a text filter on rule patterns, applied in the browser.
- **Top 20:** the first 20 rows are shown, with a "Show all" toggle.
- **Settings:** expands a rule to the fields it sets: GET, HEAD and PUT TTLs; read, write,
  compression and RAM settings; "Local conditions" for `evaluate_conditions_from_cache`; and page
  widening and page size for
  [page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching). Fields the rule
  does not set take the global defaults and are not shown. "Local conditions: On" means the proxy
  answers conditional requests for matching keys from cached metadata instead of forwarding them
  to S3. Check this setting when auditing which prefixes skip S3-side credential revalidation.

### Page-aligned range caching statistics

The `page_cache` counters (widened requests, bytes prefetched, page hits, signed-range skips,
fallbacks and RAM page promotions) are in the `/api/cache-stats` payload and on `/metrics`; the
dashboard has no card for them. Per-rule page settings are in the cache rules table's Settings
expander.

### Application log viewer

- **Recent entries:** the most recent 100 entries by default, refreshed every
  `logs_refresh_interval`.
- **Entry limit:** a selector on the page chooses 50, 100, 200 or 500 entries and passes the
  choice to `/api/logs?limit=`.
- **Level filter:** ERROR, WARN, INFO or DEBUG.
- **Layout:** timestamp, level and message, with structured key-value fields formatted for
  reading.

### System information

The hostname, version and uptime, and a menu for the Cache Stats and Application Logs sections.
A failed API call shows an error in place of its card, and the next refresh retries it. The page
works in desktop and mobile browsers.

## API endpoints

The dashboard runs its own HTTP server on `dashboard.port`, separate from proxy traffic. It serves
three static assets (`GET /` for the page, `GET /style.css` and `GET /script.js`) and six JSON
endpoints:

| Endpoint | Returns |
|---|---|
| `GET /api/cache-stats` | Cache statistics: the global hit and miss counters, the `page_cache` counters, and `overall` with `total_requests` (GET, HEAD and PUT), `get_hits`, `get_misses`, `get_total`, `head_hits`, `head_misses`, `head_total`, `put_total`, `cache_hit_rate`, `s3_requests_saved` and `bytes_served_from_cache`, plus the per-tier size and eviction figures the cards render |
| `GET /api/bucket-stats` | `rules`: the ordered rule list, each rule with the fields it sets and its HEAD and GET hit and miss counts. Populated only when `cache_rules.json` has at least one rule |
| `GET /api/bucket-traffic` | `bucket_traffic`, keyed by `bucket` or `bucket/prefix`, each entry with `get_requests`, `put_requests`, `bytes_served` (all GET bytes to clients), `bytes_saved` (GET bytes served from cache) and `bytes_uploaded` (PUT and UploadPart bytes from clients). See [Per-bucket traffic metrics](METRICS.md) |
| `GET /api/bandwidth` | Download bandwidth QoS state: `instance_ceiling_bps`, `class_bytes`, `residual_bytes` and `failopen_total`. See [Bandwidth QoS](BANDWIDTH_QOS.md) |
| `GET /api/logs` | Recent application log entries. Accepts `?limit=` (50, 100, 200 or 500) and `?level=` (ERROR, WARN, INFO or DEBUG) |
| `GET /api/system-info` | Hostname, version and uptime |

## Limits

The server accepts at most 50 concurrent connections. It rejects further connections and logs
each rejection at WARN with the peer address.

## Performance impact

An open page polls the proxy at the two refresh intervals, and each poll reads cache statistics
or recent log entries. The dashboard's disk access is read-only: application log files and cache
metadata. It writes nothing.

## Troubleshooting

### Dashboard not accessible

1. Check that `dashboard.enabled` is `true`.
2. Check that no other process uses `dashboard.port`.
3. Check that `bind_address` accepts connections from your network.
4. Check the firewall rules for the port.
5. Check the application log for the dashboard starting.

### Slow or rejected connections

1. Raise the refresh intervals so open pages poll less often.
2. Check the application log for connection-limit rejections (WARN, with the peer address). A
   browser refresh retries a rejected connection.
3. Check the application log for other dashboard errors.

## Security considerations

### Unauthenticated, read-only interface

The dashboard requires no authentication and is read-only. It shows cache statistics,
application log excerpts and system information (hostname, version and uptime). It has no write
operations, no credentials and no control actions. Presigned URL parameters are masked before
they reach the log viewer, so signatures, credentials and session tokens are redacted.

Access control is at the network level, so who can reach the port decides who can read the page:

| `bind_address` | Who can reach it | When to use |
|---|---|---|
| `127.0.0.1` (default) | The local host only | Production: reach it through an SSH tunnel |
| `0.0.0.0` | Any host with network access | Only when security groups or firewall rules restrict the port to administrators |

In production, bind to `127.0.0.1` and reach the dashboard through an SSH tunnel:

```bash
ssh -L 8081:127.0.0.1:8081 <proxy-host>
```

Then open `http://localhost:8081` in your browser.

If you bind to `0.0.0.0`, restrict access at the network layer (security groups, network ACLs,
firewall rules) so that only administrators can reach the port.

### Data exposure

- Cache statistics can reveal usage patterns.
- Application logs can contain sensitive information; presigned URL parameters are masked.
- The log level filter narrows what a viewer sees.
- Consider sanitizing logs in sensitive environments.

## See also

- [Metrics reference](METRICS_REFERENCE.md): every `/metrics` field, including those the cards
  show
- [Per-bucket traffic metrics](METRICS.md): the traffic table's counters and how to read them
- [Dashboard configuration](CONFIGURATION.md#dashboard-configuration): every `dashboard.*` field
