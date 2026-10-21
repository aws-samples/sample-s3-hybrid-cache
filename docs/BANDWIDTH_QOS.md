# Download bandwidth QoS

Download bandwidth QoS (quality of service) caps the bandwidth Hybrid Cache for Amazon S3 uses to fetch cache misses from S3 and shares that ceiling fairly between callers or buckets. Use it to keep cache-miss traffic inside an origin egress budget, or to stop one workload taking all of it from the others. Cache hits are never counted against the ceiling or slowed by it.

## Turn it on

The feature is off by default (`max_bytes_per_sec: 0`). Set the ceiling in bytes per second for the whole fleet, and on a fleet set `fallback_instance_count` to the number of proxies:

```yaml
download_bandwidth:
  max_bytes_per_sec: 1073741824   # 1 GiB/s across the fleet; 0 disables the feature
  fleet:
    fallback_instance_count: 4    # The number of proxies in the fleet
```

Every field has a default, and a configuration file without a `download_bandwidth` section leaves the feature off. The proxy refuses to start when `fleet.refresh_interval` is below `10s` or `fleet.fallback_instance_count` is `0`. [Download bandwidth QoS configuration](CONFIGURATION.md#download-bandwidth-qos-configuration) lists every field with its default and valid range.

## How it works

The proxy throttles a cache-miss download by holding back reads from the S3 response until the byte budget allows them, so TCP flow control slows the S3 connection. The bytes counted are the bytes fetched from S3 on cache misses.

### Fairness classes: caller or bucket

Each cache-miss download belongs to exactly one fairness class:

1. **Caller**: when `caller_id.enabled` is `true` and the request's User-Agent carries a valid `app/<value>` token, the value is the class. A valid value is at most `caller_id.max_len` bytes long (default 64) and matches `caller_id.validation_regex` when one is set. All requests from one caller share one budget, whichever buckets they read.
2. **Bucket**: when caller keying is off, or the token is missing or invalid, the bucket name is the class. All requests to one bucket share one budget. This is the default.

Caller classes and bucket classes are kept apart, so a caller value equal to a bucket name is still a separate class.

### Setting the application ID on clients

The AWS CLI v2 and the AWS SDKs append an `app/<value>` token to their User-Agent when an application ID is configured, and keep their own identification in front of it:

```bash
export AWS_SDK_UA_APP_ID=my-app-uid-1001  # AWS CLI v2, boto3 and botocore; the User-Agent ends in app/my-app-uid-1001
```

```ini
# ~/.aws/config, read by the AWS CLI and the SDKs
[default]
sdk_ua_app_id = my-app-uid-1001
```

In boto3 code, pass `botocore.config.Config(user_agent_appid="my-app-uid-1001")` when you create the client.

The proxy treats the value as opaque. Choose whatever convention suits the deployment, such as a Linux UID, an application name or a team name.

### Scheduling: deficit round-robin

When demand exceeds the ceiling, the proxy shares it between the active classes by deficit round-robin. The scheduler visits the active classes in turn. Each visit adds 1 MiB to the class's deficit (the bytes it is owed) and serves the class's waiting reads while the deficit and the token bucket cover them. The remaining deficit carries forward to the class's next turn. A class with nothing waiting leaves the rotation and its deficit is dropped, so an idle class builds up no credit. When only one class is active it receives the whole ceiling.

The token bucket refills every 100 ms and holds at most 100 ms of the ceiling, so a burst can exceed the ceiling by at most 100 ms of traffic.

### Fleet sharing: cap / N

On a fleet, each proxy enforces an equal share of the ceiling:

```text
per-proxy ceiling = max_bytes_per_sec / N
N                 = max(live proxies counted, fleet.fallback_instance_count)
```

Each proxy counts the live proxies from heartbeat files in `cache_dir/qos/heartbeats/`. It touches its own `{instance_id}.qos` file every `fleet.refresh_interval` (default `30s`) and counts the files modified within `fleet.instance_staleness` (default `90s`). The same scan deletes heartbeats older than ten staleness windows, with a minimum of 10 minutes (15 minutes at the defaults). The directory sits outside `cache_dir/metadata/`, so cache consolidation, eviction and journal cleanup never touch it, and resetting the cache leaves it in place.

The default staleness window is three refresh intervals, so a live proxy drops out of `N` only after it misses two heartbeats in a row. A shorter window would count a slightly late peer as stopped and raise every other proxy's ceiling. A peer's newest heartbeat can already be one interval old when another proxy reads the directory, and a slow write or clock skew between hosts adds to that. An `instance_staleness` below twice `refresh_interval` is therefore raised to three times it, with a startup warning that names the value used. After a proxy stops, the others keep dividing by the old `N` for up to `instance_staleness` plus one refresh interval, so for that time they throttle harder than necessary, never less.

Set `fleet.fallback_instance_count` to the fleet size on every fleet. It is a floor on `N`, not only a startup default, so each proxy's ceiling stays at `max_bytes_per_sec` divided by the fleet size while the cache volume is unreachable.

Where each proxy has its own `cache_dir`, as on a [local NVMe fleet](LOCAL_NVME_CACHE.md), the floor is the only thing that divides the ceiling. Each proxy sees only its own heartbeat, counts one proxy and takes the whole ceiling. Nothing increments `failopen_total`, no warning is logged and `/health` still reports healthy; the only sign is `instance_ceiling_bps` showing the full ceiling on every proxy. See [Set the QoS instance count](LOCAL_NVME_CACHE.md#set-the-qos-instance-count).

## Limits

- The ceiling applies to cache-miss downloads only. Uploads, cache hits and the HTTPS passthrough (port 443) are not throttled.
- Each proxy's share is a fixed `max_bytes_per_sec / N` that does not follow demand. When traffic is uneven across the fleet, the fleet can run below the ceiling while its busiest proxies are throttled; see [Detecting uneven load](#detecting-uneven-load).
- The fleet-wide ceiling is approximate: each proxy can exceed its share by up to 100 ms of traffic.

## Observe it

`/metrics` reports the feature under `download_bandwidth`. The [`download_bandwidth` field reference](METRICS_REFERENCE.md#download_bandwidth) describes each field.

```json
{
  "download_bandwidth": {
    "enabled": true,
    "instance_ceiling_bps": 268435456,
    "failopen_total": 0,
    "class_bytes": {
      "bkt:my-bucket": 1073741824,
      "ua:myapp": 536870912
    },
    "residual_bytes": 0
  }
}
```

- `enabled`: `false` when `max_bytes_per_sec` is `0`.
- `instance_ceiling_bps`: this proxy's ceiling in bytes per second, `max_bytes_per_sec / N`.
- `failopen_total`: fail-open events since the process started, from a scheduler timeout or a scheduler task fault.
- `class_bytes`: bytes per class since the process started, keyed `ua:<value>` for a caller and `bkt:<name>` for a bucket. It holds at most `max_tracked_classes` classes (default 1024).
- `residual_bytes`: bytes for classes outside the tracked set.

The dashboard's `/api/bandwidth` returns the same fields at the top level, with no enclosing `download_bandwidth` key: read `.instance_ceiling_bps` there, not `.download_bandwidth.instance_ceiling_bps`.

With `metrics.otlp.enabled: true`, the proxy exports three gauges: `download_bandwidth.instance_ceiling_bps`, `download_bandwidth.failopen_total` and `download_bandwidth.class_bytes`. `class_bytes` carries a `class` attribute, and the residual bytes are reported as `class="residual"`.

### Detecting uneven load

`instance_ceiling_bps` is the same on every proxy and does not move with load. Uneven load shows as:

- fleet-wide cache-miss bytes per second below `max_bytes_per_sec`;
- the busiest proxies' cache-miss throughput held at their `instance_ceiling_bps` while the others run well below it;
- `class_bytes` for the busiest classes growing on only some proxies.

The ceiling is static per proxy: nothing moves share toward the busy ones.

## See also

- [Download bandwidth QoS configuration](CONFIGURATION.md#download-bandwidth-qos-configuration): every field, default and valid range
- [Hedged upstream requests](HEDGING.md#interaction-with-other-features): a losing hedge is not charged against the ceiling
- [Local NVMe cache](LOCAL_NVME_CACHE.md#set-the-qos-instance-count): setting the instance count when each proxy has its own cache
