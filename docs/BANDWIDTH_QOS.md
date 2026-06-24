# Download Bandwidth QoS

Operators running the proxy as a managed service on a shared host or fleet often
need to cap the **aggregate cache-miss download bandwidth** the proxy pulls from S3
and distribute it fairly across competing callers or buckets.

## Overview

- The constrained resource is **origin egress on cache misses** — bytes fetched from
  S3 when the cache does not hold the object.
- **Cache hits serve from RAM/disk and are never counted against or limited by the
  ceiling.**
- Enforcement is via streaming backpressure: the proxy withholds stream readiness
  until byte-budget is available, causing TCP flow control to slow the S3 connection.
- The feature is **disabled by default** (`max_bytes_per_sec = 0`).  A binary-only
  upgrade never changes behaviour until you opt in.

## Fairness model: caller XOR bucket

Each cache-miss download is assigned to exactly one **fairness class**:

1. **Caller identity** (optional, opt-in): when `caller_id.enabled: true` and the
   request carries a valid `app/<value>` token in the User-Agent, that value is the
   fairness key. All requests from that caller share a single budget regardless of
   how many buckets they touch.
2. **Bucket** (default fallback): when caller-identity keying is disabled, absent,
   or invalid, the S3 bucket name is the fairness key. All requests to a bucket share
   one budget.

Caller-derived and bucket-derived keys are in separate namespaces, so a caller
value equal to a bucket name is never confused with a bucket fairness class.

### Setting the app-id on clients

The AWS CLI / SDKs append an `app/<value>` token when the app-id is configured:

```bash
# AWS CLI (v2)
export AWS_SDK_UA_APP_ID=my-app-uid-1001
# The User-Agent becomes: ... app/my-app-uid-1001

# boto3 / botocore
export AWS_SDK_UA_APP_ID=my-app-uid-1001

# Programmatic (boto3)
import boto3
session = boto3.Session()
session.events.register('before-send', ...)  # or use sdk_ua_app_id in ~/.aws/config
```

The `app/<value>` token is always **appended** to the existing User-Agent string, not
replacing it, so normal SDK identification is preserved.

The proxy treats the value as **opaque** — it does not interpret it as a UID,
hostname, or any other semantic. The convention (e.g. Linux UID, application name,
team identifier) is the operator's choice.

## Scheduling: deficit round-robin (DRR)

When aggregate demand exceeds the ceiling, the proxy schedules delivery using
**deficit round-robin**:

- Each fairness class maintains a deficit counter (bytes the class is owed).
- On each refill tick (`BURST_WINDOW` = 100 ms), every active class's deficit grows
  by `LEASE_QUANTUM` (1 MiB).
- A class whose deficit covers the current frame is served; the surplus carries
  forward to the next frame.
- **Work-conserving**: when only one class is active, it receives the full ceiling.
  Idle classes do not accumulate deficit.

The token bucket capacity is `ceiling_bps × 100 ms`, bounding burst overshoot to
100 ms worth of tokens.

## Fleet sharing: cap / N

On a multi-instance fleet behind a load balancer:

```
per_instance_ceiling = max_bytes_per_sec / live_instance_count
```

`live_instance_count` is derived from heartbeat files in
`cache_dir/qos/heartbeats/`.  Each proxy instance touches its
`{instance_id}.qos` heartbeat on a configurable cadence (`fleet.refresh_interval`,
default 30 s); instances older than `fleet.instance_staleness` are excluded, and
heartbeats from long-dead instances (past a 10-minute grace) are reaped during
the same scan.  The directory is deliberately outside `cache_dir/metadata/` so it
is never touched by the cache-metadata consolidation, eviction, or journal-cleanup
sweeps, and a cache reset does not disturb fleet liveness state.

**This is an approximation.**  Under skew (traffic not evenly distributed across
proxy instances), the fleet may run below the aggregate cap while individual
instances are throttled.  Reading the per-instance metrics (see Observability) to
detect this pattern is the trigger for enabling Phase-B demand-weighted
reconciliation (currently deferred; see Non-goals below).

**Important for fleet deployments:** set `fleet.fallback_instance_count` to the
fleet size.  This ensures that if shared storage becomes temporarily unavailable,
the per-instance ceiling is `cap / fleet_size` rather than the full aggregate cap.

## Configuration reference

```yaml
download_bandwidth:
  max_bytes_per_sec: 500000000   # 500 MB/s aggregate ceiling; 0 = disabled
  caller_id:
    enabled: false               # use bucket fairness by default
    validation_regex: null       # optional; bad regex = startup error
    max_len: 64                  # max caller-value chars
  max_tracked_classes: 1024      # top-K cardinality cap
  fleet:
    fallback_instance_count: 1   # SET TO FLEET SIZE on a fleet!
    instance_staleness: "30s"
    refresh_interval: "30s"
```

All fields are `#[serde(default)]` — omitting the section leaves the feature
disabled.

## Observability

The proxy exposes bandwidth metrics in three places:

### `/metrics` JSON

```json
{
  "download_bandwidth": {
    "enabled": true,
    "instance_ceiling_bps": 125000000,
    "failopen_total": 0,
    "class_bytes": {
      "bkt:my-bucket": 1073741824,
      "ua:myapp": 536870912
    },
    "residual_bytes": 0
  }
}
```

- `enabled` — `false` when `max_bytes_per_sec = 0`.
- `instance_ceiling_bps` — current per-instance ceiling (aggregate / N).
- `failopen_total` — cumulative fail-open events (DRR timeout or task fault).
- `class_bytes` — per-class cumulative bytes, key format `ua:<value>` (caller) or
  `bkt:<name>` (bucket).  Bounded by `max_tracked_classes`.
- `residual_bytes` — bytes attributed to classes outside the top-K.

### Dashboard (`/api/bandwidth`)

Returns the same JSON as above.

### OTLP

When `metrics.otlp.enabled: true`:
- `download_bandwidth.instance_ceiling_bps` (gauge)
- `download_bandwidth.failopen_total` (gauge)
- `download_bandwidth.class_bytes` (gauge, `class` attribute)

### Detecting skew-induced under-utilisation (Phase-B trigger)

If instances that are serving heavy traffic report low `instance_ceiling_bps` while
clients complain of throttling, and other instances are barely loaded, you are
experiencing skew.  The signal is:
- Fleet-aggregate bytes/s < configured aggregate ceiling
- Individual proxy `instance_ceiling_bps` lower than per-instance ideal
- `class_bytes` for hot classes clustered on a subset of instances

This pattern is the trigger for Phase-B demand-weighted reconciliation (not yet
implemented).

## Non-goals

- **Demand-weighted cross-instance reconciliation (Phase B)**: deferred; only
  static `cap / N` is implemented.
- **Hard instantaneous fleet-wide cap**: the ceiling is approximate; a small
  overshoot is bounded by the 100 ms burst window.
- **Upload / PUT bandwidth shaping**: only cache-miss download is throttled.
- **Cache-hit throttling**: cache hits are unaffected.
- **TCP-passthrough (HTTPS :443) shaping**: no cache-miss concept there.
