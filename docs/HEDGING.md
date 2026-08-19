# Hedged Upstream Requests

A hedge is a second, identical upstream fetch issued when the first one is slow to
return its first byte. Whichever responds first wins; the loser is dropped. This trades
a bounded amount of duplicate S3 cost for lower tail latency.

Hedging is **off by default and is never enabled globally**. It only turns on for keys
matched by a `cache_rules.json` rule, because the right trigger threshold depends on the
workload and the cost is real: every hedge is a second billed S3 request.

## When it helps, and when it does not

Hedging targets the long tail of upstream first-byte latency, not the median. It helps
when a small fraction of otherwise-identical requests are much slower than the rest, and
retrying immediately against a different connection is likely to be faster than waiting.

It does not help, and costs money, when:

- Upstream latency is uniformly high. A hedge issued against a slow-for-everyone origin
  is just as slow, so you pay twice for the same latency.
- The bottleneck is bandwidth rather than TTFB. Two concurrent transfers of the same
  bytes compete for the same link.
- The workload is already mostly cache hits. Hedging only applies to upstream fetches.

Start by measuring. If `hedged_requests.issued` climbs while `hedged_requests.won` stays
near zero, hedging is spending money without changing latency; raise the trigger
threshold or turn it off for that prefix.

## Enabling it

Three per-rule fields in `cache_rules.json`, all optional:

```json
{
  "$schema": "../cache-rules-schema.json",
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

**`hedging_enabled`** (`bool`, default `false`) — the gate. Nothing else in this document
applies to a key unless a matching rule sets this `true`.

**`hedge_trigger_after`** (duration, default `250ms` when hedging is enabled) — how long
to wait for the original fetch's first byte before issuing the hedge. Must be greater
than zero and **strictly less than `connection_pool.upstream_first_byte_timeout`**
(default `5s`), otherwise the first-byte timeout would fire before a hedge could ever be
issued. This is validated on every rules load, so a bad value is rejected at reload time
rather than silently ignored.

**`hedge_max_per_request`** (integer, default `1` when hedging is enabled) — the
per-request hedge budget. Note the scope: for a range GET that fans out into several
parallel missing-range sub-fetches, this budget is **shared across all of them**, not
applied per sub-fetch. A budget of 1 on a request that needs four sub-fetches means one
hedge total, not four.

Per-key rule matching and first-match-per-field precedence work the same as every other
rule field; see [CONFIGURATION.md — Cache Rules](CONFIGURATION.md#cache-rules).

## The fleet-wide cost governor

One process-global knob bounds how much duplicate traffic hedging can generate,
regardless of how many rules enable it:

```yaml
connection_pool:
  hedged_requests:
    max_inflight_fraction: 0.1   # Default: 0.1. Valid range: 0.0-1.0
```

**`max_inflight_fraction`** (`f64`, default `0.1`) — the maximum fraction of in-flight
upstream fetches that may be hedges.

**The first hedge is always admitted**, regardless of this cap, whenever no other hedge is
in flight. Without that rule a low-traffic deployment would have every hedge suppressed
forever: with one fetch in flight, `(0 + 1) / 1 = 1.0` exceeds any sane fraction. The cap
is there to bound amplification under load, and there is nothing to amplify when a single
request is in flight.

For every subsequent hedge the governor evaluates:

```
(in_flight_hedges + 1) / max(in_flight_fetches, 1) > max_inflight_fraction
```

If that holds, the hedge is **suppressed** and the original is served alone. The request
still succeeds; it just does not get a second chance. Suppressions increment
`hedged_requests.suppressed`.

At the default `0.1`, hedges settle at roughly 10% of concurrent upstream fetches, so
worst-case duplicate-request cost is bounded near 10% above baseline no matter how
aggressively individual rules are configured.

**Setting `0.0` does not disable hedging.** The first-is-free rule still admits one hedge
at a time, so a low-concurrency workload keeps hedging at `0.0`. To actually stop hedging,
remove `hedging_enabled` from the rules — that is the only complete off switch.

The governor is per-instance. Three proxies each at `0.1` bound the fleet near 10% because
each computes the ratio against its own in-flight count, not because they coordinate.

## What happens on each outcome

The two arms race on **first byte**, not on completion. The first arm to return a response
status — **any** status code, including an error — wins, and the loser is cancelled at
header time with no body bytes read. So at most one cache write and one client response
happen per request.

If both arms time out, the retry loop advances and consumes a retry. A hedge still in
flight when the original times out is kept alive and may still win; aborting the original
does not cancel the logical fetch.

Only idempotent **GET and HEAD** are hedged. PUT, POST, DELETE, and multipart mutations
never are.

| Outcome | Result |
|---|---|
| Original returns first | Original served. No metric movement beyond `issued`. |
| Hedge returns first | Hedge served, `won` increments. |
| Governor declines the hedge | Original served alone, `suppressed` increments. |
| Per-request budget exhausted | Original served alone; no hedge issued. |
| Original fails, hedge succeeds | Hedge served. This is a secondary benefit: hedging also covers a transport error on one arm. |
| Both fail | The request fails as it would have without hedging. |

### Connection diversity

The hedge prefers a **different upstream IP** than the original, so it is a retry against a
different endpoint rather than the same one. That is what makes it effective against a
single slow connection or a single degraded endpoint.

If only one healthy IP is available, both arms use it on separate connections; the hedge is
not suppressed for lack of a second IP. If the destination matches an `upstream_overrides`
entry, no IP pin is applied and both arms take the normal override path.

## Relationship to the upstream timeouts

Hedging sits **beneath** the existing upstream timers and replaces none of them:

| Timer | Scope | Hedging interaction |
|-------|-------|---------------------|
| `hedge_trigger_after` (per rule) | TTFB of the original fetch | Issues the hedge; the original keeps running |
| `upstream_first_byte_timeout` (startup) | Connect to first byte, per arm | Each arm gets its own independent timer |
| `upstream_idle_timeout` (startup) | Mid-stream gap | Applied only to the winner's body stream |
| `S3Client` upstream timeout (hardcoded 30s) | Connect to response, per upstream request | Applies independently to each arm |

`hedge_trigger_after` must be strictly less than `upstream_first_byte_timeout`, validated
on every rules load.

> `server.request_timeout` is **not** in this table because it is not enforced — no code
> path reads it. There is currently no whole-request wall covering a request end to end.
> See [CONFIGURATION.md](CONFIGURATION.md#request_timeout-is-not-enforced).

## Which fetch paths hedge

All of them: full-object GET, HEAD, range GET, and part-number GET.

### Signed ranges are covered

The AWS CLI and the AWS SDKs include `range` in `SignedHeaders` on a ranged GET, so a
**signed** range is the common case for those clients; an unsigned range comes from a
presigned URL or a raw HTTP client. Before 2.4.3 the signed-range path never called into
the hedging coordinator, so hedging silently did nothing for the majority of range traffic
even when a rule enabled it. Both are covered now.

### Two range paths, both hedged

- **Complete range miss** — none of the requested bytes are cached, so the range streams
  straight from the origin and never reaches the missing-range fan-out. This is the cold
  path for a small-range-read workload, and the one hedging matters most for. The winner's
  body streams to client and cache, so proxy memory stays bounded regardless of range size.
- **Partial range hit** — some bytes are cached. The gaps are consolidated and fetched as
  N parallel sub-fetches, each independently racing an original against a hedge. The winner
  is buffered for merge with the cached data.

Either way the loser is dropped at header time, so no partial merge data is retained.

Two details specific to ranges:

- **No first-byte timeout.** Both range paths pass no first-byte timeout, so hedging does
  not introduce a timeout regime where none existed. The hardcoded 30s `S3Client` timeout
  remains each arm's ceiling.
- **The budget is shared.** One client range GET gets one `hedge_max_per_request` budget
  across all N sub-fetches. Only sub-fetches still lacking a first byte at
  `hedge_trigger_after` attempt a claim, so the budget lands on a genuinely slow one. Raise
  it on the prefix's rule for routinely multi-range workloads.
- **Independent IP pairs.** Each sub-fetch selects its own two-IP pair.

Part-number GETs (`?partNumber=N`) hedge through the full-object fetcher.

## Observability

Three counters under `hedged_requests` in `/metrics`, all cumulative since process start:

| Field | Meaning |
|---|---|
| `issued` | Hedges launched. **This is your duplicate-S3-request bill.** |
| `won` | Requests where the hedge, not the original, was served |
| `suppressed` | Hedges the governor declined on cost grounds |

They are not exported over OTLP. See
[METRICS_REFERENCE.md](METRICS_REFERENCE.md#hedged_requests) for the field reference.

Two ratios worth watching:

```
win_rate     = won / issued           # Is hedging actually changing outcomes?
suppression  = suppressed / (issued + suppressed)   # Is the governor the binding constraint?
```

A high `win_rate` (say above 0.2) means the tail you targeted is real and hedging is
cutting it. A `win_rate` near zero means you are paying for nothing. A high suppression
ratio means `max_inflight_fraction` is throttling hedges before they can help, so either
the rules are too broad or the fraction is too low for your concurrency.

## Interaction with other features

**Download coordination** runs first. Concurrent requests for the same uncached key
coalesce onto a single upstream fetch, and hedging applies to that one fetch. Hedging
does not multiply across coalesced waiters.

**Bandwidth QoS** counts hedge bytes. A hedge that loses the race still consumed some
upstream bandwidth before being dropped, and those bytes are charged against the
download ceiling. On a bandwidth-constrained deployment, hedging and a tight
`download_bandwidth.max_bytes_per_sec` work against each other.

**Page-aligned range caching** composes rather than conflicting. With `page_widening` on
the same prefix, the widened page fill hedges too, and the hedge re-sends the **widened**,
page-aligned Range rather than the client's original sub-range. Enabling both on one prefix
is the expected combination, since both target small range reads over large objects.

A client range GET straddling a page boundary fills several pages concurrently, and all
those page fills plus every missing-range sub-fetch beneath them share the single
`hedge_max_per_request` budget for that one client request. Page coordination is
unaffected: concurrent readers of the same page still coalesce onto one logical fetch, and
only the winning arm's bytes are committed.

## See also

- [CONFIGURATION.md — Hedged Requests Governor](CONFIGURATION.md#hedged-requests-governor) — the field reference
- [CONNECTION_POOLING.md](CONNECTION_POOLING.md) — IP distribution and health tracking, which determine what the two arms get pinned to
- [METRICS_REFERENCE.md](METRICS_REFERENCE.md) — the full `/metrics` payload
- [`docs/cache-rules-schema.json`](cache-rules-schema.json) — schema for the three rule fields
