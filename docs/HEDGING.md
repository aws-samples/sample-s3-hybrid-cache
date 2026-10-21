# Hedged upstream requests

A hedge is a second, identical upstream request that Hybrid Cache for Amazon S3 sends when the first is slow to return its first byte. The proxy serves whichever responds first and drops the other, trading a bounded amount of duplicate S3 cost for lower tail latency. Hedging is off by default and only a cache rule turns it on, because the right trigger depends on the workload and every hedge is a second billed S3 request.

## When hedging helps

Hedging targets the long tail of upstream first-byte latency. It helps when a small fraction of otherwise identical requests are much slower than the rest, and an immediate second attempt on another connection is likely to answer sooner than the first.

It costs money without helping when:

- upstream latency is high for every request, so the hedge is as slow as the original and you pay twice;
- the bottleneck is bandwidth rather than time to first byte, so two transfers of the same bytes compete for the same link;
- the workload is mostly cache hits, since hedging applies only to upstream fetches.

Measure before widening it. If `hedged_requests.issued` climbs while `hedged_requests.won` stays near zero, hedging is spending money without changing latency: raise the trigger or turn it off for that prefix.

## Turn it on

Three per-rule fields in `cache_rules.json` turn hedging on for the keys a rule matches:

```json
{
  "rules": [
    {
      "pattern": "latency-sensitive-bucket/hot/**",
      "hedging_enabled": true,
      "hedge_trigger_after": "250ms",
      "hedge_max_per_request": 1
    }
  ]
}
```

- **`hedging_enabled`** (`bool`, default `false`): the switch. Nothing on this page applies to a key unless a matching rule sets it to `true`.
- **`hedge_trigger_after`** (duration, default `250ms` when hedging is enabled): how long to wait for the original request's first byte before sending the hedge. It must be greater than zero and less than `connection_pool.upstream_first_byte_timeout` (default `5s`), or the first-byte timeout would fire before a hedge could be sent. The proxy checks this each time it loads the rules file and rejects a file that breaks it; see [Hot reload and error resilience](CONFIGURATION.md#hot-reload-and-error-resilience).
- **`hedge_max_per_request`** (integer, default `1` when hedging is enabled): the hedge budget for one client request. A range GET that fans out into several parallel sub-fetches shares one budget across all of them, so a budget of 1 on a request with four sub-fetches allows one hedge in total.

Rule matching and first-match-per-field precedence work as for every other rule field; see [Cache rules](CONFIGURATION.md#cache-rules). The rule fields are also described in the [cache rules schema](cache-rules-schema.json).

## The cost governor

One setting per proxy bounds how much duplicate traffic hedging can generate, however many rules enable it:

```yaml
connection_pool:
  hedged_requests:
    max_inflight_fraction: 0.1   # Default 0.1. Valid range 0.0 to 1.0
```

`max_inflight_fraction` is the largest share of in-flight upstream fetches that may be hedges. [Hedged requests governor](CONFIGURATION.md#hedged-requests-governor) is its field reference.

The first hedge is always admitted while no other hedge is in flight, whatever the setting. Without that rule a quiet proxy would suppress every hedge: with one fetch in flight, `(0 + 1) / 1 = 1.0` exceeds any useful fraction. The cap exists to bound amplification under load, and a single request has nothing to amplify.

For every later hedge the governor evaluates:

```text
(in_flight_hedges + 1) / max(in_flight_fetches, 1) > max_inflight_fraction
```

When that holds, the hedge is suppressed and the original is served alone. The request still succeeds without a second attempt, and `hedged_requests.suppressed` increments.

At the default `0.1`, hedges stay near 10% of concurrent upstream fetches, so duplicate-request cost stays near 10% above the unhedged level however the rules are set.

Setting `0.0` does not turn hedging off. The first-hedge rule still admits one hedge at a time, so a quiet workload keeps hedging at `0.0`. Removing `hedging_enabled` from the rules is the only way to stop hedging completely.

Each proxy runs its own governor against its own in-flight count. Three proxies each at `0.1` keep the fleet near 10% because each bounds itself, not because they coordinate.

## How it works

The two attempts race on the first byte, not on completion. The first to return a response status wins, whatever the status code, including an error. The proxy cancels the loser as soon as the winner's headers arrive, before reading any of the loser's body, so each request produces at most one cache write and one client response.

When both attempts time out, the retry loop moves on and uses up a retry. A hedge still in flight when the original times out is kept and can still win: aborting the original does not cancel the fetch.

Only GET and HEAD requests are hedged. PUT, POST, DELETE and the multipart operations never are.

| Outcome | Result |
|---|---|
| Original returns first | Original served. Only `issued` moves |
| Hedge returns first | Hedge served, and `won` increments |
| Governor declines the hedge | Original served alone, and `suppressed` increments |
| Per-request budget used up | Original served alone; no hedge sent |
| Original fails, hedge succeeds | Hedge served, so hedging also covers a transport error on one attempt |
| Both fail | The request fails as it would have without hedging |

### Connection diversity

The hedge prefers a different upstream IP address from the original, so it is a second attempt against another endpoint. That covers one slow connection or one degraded endpoint; see [Connection pooling](CONNECTION_POOLING.md) for how addresses are distributed and health-tracked.

If only one healthy address is available, both attempts use it on separate connections; the hedge is not suppressed for lack of a second address. If the destination matches an `upstream_overrides` entry, neither attempt is pinned to an address and both take the override's path.

### Which fetch paths hedge

All of them: full-object GET, HEAD, range GET and part-number GET (`?partNumber=N`), which hedges through the full-object path.

Signed and unsigned ranges are both covered. The AWS CLI and the AWS SDKs include `range` in `SignedHeaders` on a ranged GET, so a signed range is the common case for those clients. An unsigned range comes from a presigned URL or a plain HTTP client.

Range GETs take one of two paths, and both hedge:

- **Complete range miss**: none of the requested bytes are cached, so the range streams straight from the origin. This is the cold path for a workload of small range reads, and the one where hedging matters most. The winner's body streams to the client and the cache, so proxy memory stays bounded whatever the range size.
- **Partial range hit**: some bytes are cached. The proxy merges the gaps and fetches them as parallel sub-fetches, each racing its own original against a hedge. The winner is buffered so it can be merged with the cached bytes.

On both paths the loser is dropped when the winner's headers arrive, so no partial data from it is kept. Three details apply to ranges:

- **No first-byte timeout.** Neither range path sets a first-byte timeout, and hedging does not add one. The proxy's fixed 30-second upstream request timeout remains each attempt's ceiling.
- **One shared budget.** One client range GET gets one `hedge_max_per_request` budget across all its sub-fetches. Only sub-fetches still waiting for a first byte at `hedge_trigger_after` try to claim it, so the budget goes to a slow one. Raise it on the prefix's rule for workloads that routinely fetch several ranges per request.
- **Independent address pairs.** Each sub-fetch picks its own two addresses.

## Relationship to the upstream timeouts

Hedging runs beneath the upstream timers and replaces none of them:

| Timer | Scope | How it interacts with hedging |
|-------|-------|-------------------------------|
| `hedge_trigger_after` (per rule) | Time to first byte of the original | Sends the hedge; the original keeps running |
| `upstream_first_byte_timeout` (`config.yaml`) | Connect to first byte, per attempt | Full-object path only, where each attempt has its own timer. The range paths set none |
| `upstream_idle_timeout` (`config.yaml`) | Gap between body chunks | Applies only to the winner's body |
| Fixed 30-second upstream request timeout | Connect to response, per upstream request | Applies to each attempt separately |

No timer covers a whole client request: `server.request_timeout` has no effect, as described in [`request_timeout` is not enforced](CONFIGURATION.md#request_timeout-is-not-enforced).

## Interaction with other features

**Download coordination** runs first. Concurrent requests for the same uncached key share one upstream fetch, and hedging applies to that one fetch, so it does not multiply across the waiting requests.

**Download bandwidth QoS** throttles what the proxy fetches from S3 on cache misses. The losing attempt is dropped before its body is read, so it is not charged against the [download bandwidth ceiling](BANDWIDTH_QOS.md), and the ceiling does not bound hedge cost. Bytes the origin had already sent for the loser still cross the network.

**Page-aligned range caching** works with hedging. With [`page_widening`](CACHE_READ_PATHS.md#page-aligned-range-caching) on the same prefix, the widened page fill hedges too, and the hedge sends the widened, page-aligned range rather than the client's original one. Enabling both on one prefix is the expected combination, since both target small range reads over large objects.

A client range GET that straddles a page boundary fills several pages at once, and all those page fills and every sub-fetch beneath them share the one `hedge_max_per_request` budget of that client request. Page coordination is unchanged: concurrent readers of the same page still share one fetch, and only the winner's bytes are committed.

## Observe it

Three counters under `hedged_requests` in `/metrics`, each cumulative since the process started:

| Field | Meaning |
|---|---|
| `issued` | Hedges sent: your duplicate S3 requests |
| `won` | Requests where the hedge, not the original, was served |
| `suppressed` | Hedges the governor declined on cost grounds |

The OTLP export carries them as `hedged_requests.issued`, `hedged_requests.won` and `hedged_requests.suppressed`. [`hedged_requests`](METRICS_REFERENCE.md#hedged_requests) is the field reference.

Two ratios computed from these counters show whether hedging is paying off:

```text
win rate         = won / issued
suppression rate = suppressed / (issued + suppressed)
```

A win rate above about 0.2 means the tail you targeted is real and hedging is cutting it. A win rate near zero means the hedges cost money and change nothing. A high suppression rate means `max_inflight_fraction` throttles hedges before they can help: either the rules are too broad, or the fraction is too low for your concurrency.

## See also

- [Hedged requests governor](CONFIGURATION.md#hedged-requests-governor): the `max_inflight_fraction` field reference
- [Connection pooling](CONNECTION_POOLING.md): how IP distribution and health tracking choose the addresses the two attempts use
- [`/metrics` reference](METRICS_REFERENCE.md): the full payload
- [Cache rules schema](cache-rules-schema.json): the schema for the three rule fields
