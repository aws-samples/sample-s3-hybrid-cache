# Hedged upstream requests

A hedge is a second, identical upstream request that Hybrid Cache for Amazon S3 sends when the first is slow to answer. The proxy serves whichever answers first and drops the other, trading a bounded amount of duplicate S3 cost for lower tail latency. Hedging is off by default and only a cache rule turns it on, because the right trigger depends on the workload and every hedge is a second billed S3 request.

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
- **`hedge_trigger_after`** (duration, default `250ms` when hedging is enabled): how long the original request may go unanswered before the proxy sends the hedge. It must be greater than zero and less than `connection_pool.upstream_first_byte_timeout` (default `5s`), or the first-byte timeout would fire before a hedge could be sent. The proxy checks an explicit value on a rule that sets `hedging_enabled: true` each time it loads the rules file, and rejects a file that breaks it; see [Hot reload and error resilience](CONFIGURATION.md#hot-reload-and-error-resilience).
- **`hedge_max_per_request`** (integer, default `1` when hedging is enabled): the hedge budget for one client request. A range GET that fans out into several parallel sub-fetches shares one budget across all of them, so a budget of 1 on a request with four sub-fetches allows one hedge in total. A budget of `0` sends no hedge, yet the original request is still tied to an address as described in [Connection diversity](#connection-diversity). It therefore skips the connect and timeout retries an unhedged request gets, and every fetch still unanswered at `hedge_trigger_after` increments `suppressed`. To turn hedging off for a prefix, set `hedging_enabled: false` instead.

Rule matching and first-match-per-field precedence work as for every other rule field; see [Cache rules](CONFIGURATION.md#cache-rules). The rule fields are also described in the [cache rules schema](cache-rules-schema.json).

## The cost governor

One setting per proxy bounds how much duplicate traffic hedging can generate, however many rules enable it:

```yaml
connection_pool:
  hedged_requests:
    max_inflight_fraction: 0.1   # Default 0.1. Valid range 0.0 to 1.0
```

`max_inflight_fraction` is the largest share of in-flight upstream fetches that may be hedges, counting only fetches for keys with hedging on. [Hedged requests governor](CONFIGURATION.md#hedged-requests-governor) is its field reference.

The first hedge is always admitted while no other hedge is in flight, whatever the setting. Without that rule a quiet proxy would suppress every hedge: with one fetch in flight, `(0 + 1) / 1 = 1.0` exceeds any useful fraction. The cap exists to bound amplification under load, and a single request has nothing to amplify.

For every later hedge the governor evaluates:

```text
(in_flight_hedges + 1) / max(in_flight_fetches, 1) > max_inflight_fraction
```

When that holds, the hedge is suppressed, the original runs alone, and `hedged_requests.suppressed` increments.

At the default `0.1` and under load, hedges stay near 10% of the concurrent fetches for keys with hedging on, so their duplicate-request cost stays near 10% above the unhedged level.

Setting `0.0` does not turn hedging off. The first-hedge rule still admits one hedge at a time, so a quiet workload keeps hedging at `0.0`. To stop hedging completely, remove `hedging_enabled` from the rules.

Each proxy runs its own governor against its own in-flight count. Three proxies each at `0.1` keep the fleet near 10% because each bounds itself, not because they coordinate.

## How it works

The two attempts race to answer. An attempt answers when its response headers arrive, whatever the status code, including an error, and the first to answer wins. On a sub-fetch of a partial range hit or a page fill, each attempt buffers its body and answers only when the whole body has arrived. The proxy then cancels the loser and keeps none of its data, so the loser never produces a cache write or a client response.

On the full-object path, a hedge still in flight when the original reaches `upstream_first_byte_timeout` is kept and can still win.

Only GET and HEAD requests are hedged. PUT, POST, DELETE and the multipart operations never are.

| Outcome | Result |
|---|---|
| Original answers first | Original served. `issued` counts the hedge if one had been sent |
| Hedge answers first | Hedge served, and `won` increments |
| Governor declines the hedge | Original runs alone, and `suppressed` increments |
| Per-request budget used up | Original runs alone, and `suppressed` increments |
| Original fails, hedge succeeds | Hedge served, so hedging also covers a transport error on one attempt |
| Both fail | Depends on the fetch path; see [Connection diversity](#connection-diversity) |

### Connection diversity

The hedge prefers a different upstream IP address from the original, so it is a second attempt against another endpoint. That covers one slow connection or one degraded endpoint; see [Connection pooling](CONNECTION_POOLING.md) for how addresses are distributed and health-tracked.

Both attempts are tied to addresses whenever the proxy holds a healthy address for the host. With two or more, they get different ones. With one, both use it on separate connections, and the hedge is not suppressed for lack of a second address. Neither attempt is tied to an address while the proxy holds none for the host, or when the destination matches an `upstream_overrides` entry, in which case both take the override's path.

With IP distribution on, the proxy resolves a host when it first sees it and keeps its addresses current. With it off, the proxy holds addresses only for a host named by an exact `endpoint_overrides` entry, and for a host it has forwarded a PUT, a POST or a signed DELETE to. It does not refresh those addresses or probe excluded ones. A host it has written to keeps the addresses its first successful lookup returned, and an address excluded after repeated failures stays out of rotation until the proxy restarts.

A failed connect on a tied attempt is not retried; the other attempt is the recovery. An untied attempt takes the connect retries described in [Error recovery](CONNECTION_POOLING.md#6-error-recovery). When both attempts fail, or the original fails and no hedge is sent, what happens next depends on the fetch path:

| Fetch path | When both attempts fail |
|---|---|
| Full-object GET, HEAD and part-number GET | The request fails. If every attempt sent reaches `upstream_first_byte_timeout` without answering, the race first runs again on the same addresses, up to `upstream_idle_retries` times |
| Complete range miss | The request fails |
| Signed range | The proxy tries up to twice more, after waits of 100 ms and 200 ms, choosing the addresses again each time. A failed upstream TLS certificate check fails the request at once |
| Partial range hit or page fill | The proxy fetches the client's range once more without hedging, which takes the connect retries, and the request fails only if that fetch fails too |

A request that fails gets the status an unhedged request gets on the same path: `502` for most failures and `504` for a timeout. Retries share the request's hedge budget, so at the default budget of 1 a retry runs without a hedge once a hedge has been sent.

### Which fetch paths hedge

All of them: full-object GET, HEAD, range GET and part-number GET (`?partNumber=N`), which hedges through the full-object path.

Signed and unsigned ranges are both covered. The AWS CLI and the AWS SDKs include `range` in `SignedHeaders` on a ranged GET, so a signed range is the common case for those clients. An unsigned range comes from a presigned URL or a plain HTTP client.

Range GETs take one of three paths, and all three hedge. An unsigned range takes one of the first two, and a signed range usually takes the third:

- **Complete range miss**: none of the requested bytes are cached, so the range streams straight from the origin. This is the cold path for a workload of small range reads, and the one where hedging matters most. The winner's body streams to the client and the cache, so proxy memory stays bounded whatever the range size.
- **Partial range hit**: some bytes are cached. The proxy merges the gaps and fetches them as parallel sub-fetches, each racing its own original against a hedge. Each attempt buffers its whole body for the merge with the cached bytes, so a sub-fetch answers at its last byte.
- **Signed range**: the client signed its `Range` header, so the proxy cannot split the range and fetches exactly the client's range in one request, even when part of it is cached. The winner's body streams to the client. After revalidating a partly cached signed range, the proxy can fetch the whole range as a complete range miss instead.

On every path the loser is dropped as soon as the winner answers, and none of its data is kept. Three details apply to ranges:

- **No first-byte timeout.** No range path sets a first-byte timeout, and hedging does not add one. The fixed 30-second upstream request timeout still bounds each upstream request an attempt sends, from connect to its response headers. Nothing times the body of a buffered sub-fetch after that.
- **One shared budget.** One client range GET gets one `hedge_max_per_request` budget across all its sub-fetches. Only sub-fetches that have not answered at `hedge_trigger_after` try to claim it, so the budget goes to a slow one. Raise it on the prefix's rule for workloads that routinely fetch several ranges per request.
- **Independent address pairs.** Each sub-fetch picks its own two addresses.

## Relationship to the upstream timeouts

Hedging runs beneath the upstream timers and replaces none of them:

| Timer | Scope | How it interacts with hedging |
|-------|-------|-------------------------------|
| `hedge_trigger_after` (per rule) | Time for the original to answer | Sends the hedge; the original keeps running |
| `upstream_first_byte_timeout` (`config.yaml`) | Connect to first byte, per attempt | Full-object path only, where each attempt has its own timer. The range paths set none |
| `upstream_idle_timeout` (`config.yaml`) | Gap between body chunks | Applies only to the winner's body, on the paths that stream it |
| Fixed 30-second upstream request timeout | Connect to response headers, per upstream request | Applies to each attempt separately |

No timer covers a whole client request: `server.request_timeout` has no effect, as described in [`request_timeout` is not enforced](CONFIGURATION.md#request_timeout-is-not-enforced).

## Interaction with other features

**Download coordination** runs first. Concurrent requests for the same uncached key share one upstream fetch, and hedging applies to that one fetch, so it does not multiply across the waiting requests.

**Download bandwidth QoS** throttles the cache-miss fetches the proxy streams: full-object GETs, part-number GET misses, complete range misses and signed ranges. The sub-fetches of a partial range hit and page fills are not throttled. The losing attempt's body never passes through the throttle, so it is not charged against the [download bandwidth ceiling](BANDWIDTH_QOS.md), and the ceiling does not bound hedge cost. Bytes the origin had already sent for the loser still cross the network.

**Page-aligned range caching** works with hedging. With [`page_widening`](CACHE_READ_PATHS.md#page-aligned-range-caching) on the same prefix, the widened page fill hedges too, and the hedge sends the widened, page-aligned range rather than the client's original one. Enabling both on one prefix is the expected combination, since both target small range reads over large objects.

A client range GET that straddles a page boundary fills several pages at once, and all those page fills and every sub-fetch beneath them share the one `hedge_max_per_request` budget of that client request. Page coordination is unchanged: concurrent readers of the same page still share one fetch, and only the winner's bytes are committed.

## Observe it

Three counters under `hedged_requests` in `/metrics`, each cumulative since the process started:

| Field | Meaning |
|---|---|
| `issued` | Hedges sent: your duplicate S3 requests |
| `won` | Fetches the hedge won by answering before the original |
| `suppressed` | Hedges not sent because the governor or the per-request budget declined them |

The OTLP export carries them as `hedged_requests.issued`, `hedged_requests.won` and `hedged_requests.suppressed`. [`hedged_requests`](METRICS_REFERENCE.md#hedged_requests) is the field reference.

Two ratios computed from these counters show whether hedging is paying off:

```text
win rate         = won / issued
suppression rate = suppressed / (issued + suppressed)
```

A win rate above about 0.2 means the tail you targeted is real and hedging is cutting it. A win rate near zero means the hedges cost money and change nothing. A high suppression rate means hedges are declined before they can help. The rules may be too broad, `max_inflight_fraction` too low for your concurrency, or `hedge_max_per_request` too low for requests with several slow fetches.

## See also

- [Hedged requests governor](CONFIGURATION.md#hedged-requests-governor): the `max_inflight_fraction` field reference
- [Connection pooling](CONNECTION_POOLING.md): how IP distribution and health tracking choose the addresses the two attempts use
- [`/metrics` reference](METRICS_REFERENCE.md): the full payload
- [Cache rules schema](cache-rules-schema.json): the schema for the three rule fields
