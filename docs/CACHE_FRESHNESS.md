# Cache freshness

This page explains how Hybrid Cache for Amazon S3 decides whether cached data may be served, and
what it asks S3 when it cannot: TTLs, revalidation and conditional requests. It is for operators
who set TTLs or need to know how stale a response can be.
[Time-to-live (TTL) configuration](CONFIGURATION.md#time-to-live-ttl-configuration) gives the
type, default and valid range of every TTL.

## When the proxy can return data that differs from S3

Here "differs from S3" means any answer other than the one S3 would give this caller now: an
older object, a deleted object, an object S3 would refuse this caller, bytes from another store,
or damaged bytes.

What always holds:

- A proxy that acknowledges a PUT, CompleteMultipartUpload, DELETE, DeleteObjects or form POST stops serving the object it replaced before it responds, or sends that key's GET and HEAD to S3 until the invalidation lands, for as long as it keeps running.
- One response's bytes, ETag and length come from one object version. When S3 refuses the `If-Match` pin on a merge of cached and fetched bytes, the proxy invalidates the cached copy and serves the request from S3.
- A request for anything other than the current object never reads or fills the cache ([Operations that bypass cache](CACHING.md#operations-that-bypass-cache)).

| Situation | What a client can receive | For how long | Mitigation | Details |
|---|---|---|---|---|
| The object is overwritten or deleted without passing through a proxy that shares this cache volume: a client writing to S3 directly, S3 Lifecycle, replication, or a proxy with its own cache volume | The older object, or one S3 has deleted. A lone `If-Match` naming the old ETag gets the old object where S3 would answer `412`. A download sized from a cached HEAD can be planned against the older `Content-Length` | GET: until `get_ttl` has passed since the entry was cached or last confirmed by S3 (default `1d`). HEAD: until `head_ttl` has passed (default `60s`). The next read after that revalidates by ETag. A client that sends a HEAD before its GET, as `aws s3 cp` does, is covered by `head_ttl`. The first HEAD after it passes revalidates with S3, and a changed ETag or length removes the cached ranges, so the following GET fetches the new object ([Scenario 3d](#scenario-3d-head-detects-object-change-range-invalidation)). A client that sends no HEAD gets no such check | Send writes through the proxy. Lower `get_ttl` and `head_ttl` for the prefix in `cache_rules.json`; `"0s"` revalidates every read. A lowered TTL reaches entries already cached, as [Settings are re-evaluated on each read](CACHING.md#settings-are-re-evaluated-on-each-read) describes | [GET TTL](#get-ttl-get_ttl), [HEAD TTL](#head-ttl-head_ttl) |
| A caller loses access after the object was cached: an IAM or bucket policy change, revoked credentials, or a request carrying a non-matching `x-amz-expected-bucket-owner` | The cached object, where S3 would answer `403` | As the row above: `get_ttl` for GET, `head_ttl` for HEAD | For the prefix, set `get_ttl: "0s"` and `head_ttl: "0s"`, so every read reaches S3, including a `?partNumber=` GET, a HEAD that carries a `Range` header, and a request whose only conditional header is `If-Match` or `If-Range`. Restrict network access to the proxy. The proxy refuses an expired presigned URL whatever the cache holds | [Shared cache access model](ARCHITECTURE.md#shared-cache-access-model), [Zero TTL revalidation](CACHING.md#zero-ttl-revalidation), [Request validation limitations](ARCHITECTURE.md#request-validation-limitations) |
| An object is written through one hostname and read through another: an access point or Multi-Region Access Point and the bucket's own hostname, or two access points | The older object, or a deleted one | As the first row: `get_ttl` for GET, `head_ttl` for HEAD, including the HEAD-before-GET check | Write and read each object through the same hostname, or give the access point's key prefix a short or `"0s"` TTL | [Access points and Multi-Region Access Points](CACHING.md#access-points-and-multi-region-access-points) |
| Another proxy sharing the cache volume acknowledges an overwrite or delete while this proxy holds the object in RAM | The older object, or a deleted one, from this proxy | A RAM range hit: up to `cache.ram_cache_verification_interval` (default `1s`). An answer from the RAM metadata cache, such as a HEAD: up to `cache.metadata_cache.refresh_interval` (default `5s`). A key with `page_widening: true`: a widened read re-checks a page held in this proxy's RAM against the shared metadata after `cache.ram_cache_verification_interval`, like a RAM range hit | Read after write through the proxy that took the write. Lower both intervals on every proxy, since each bounds only its own RAM. For keys where even one second of RAM staleness matters, set `ram_cache_eligible: false` | [RAM cache coherency](#ram-tier-coherency-contract), [Write-through ordering](CACHING.md#write-through-ordering) |
| A key has `page_widening: true`, and a PUT, CompleteMultipartUpload or DELETE is acknowledged within one consolidation cycle of a page of that key being cached | On a widened read, the earlier object, or a deleted one, from any proxy including the one that took the write | About one consolidation cycle after the acknowledgement: `cache.shared_storage.consolidation_interval`, default `5s` | Leave `page_widening` off for keys that are read back right after they are written or deleted | [Write-through ordering](CACHING.md#write-through-ordering) |
| The cache volume is slow or unavailable when a proxy acknowledges a write or delete, so the invalidation does not finish within the write's cache budget (1 second, or 3 seconds for a DeleteObjects) | From other proxies sharing the volume: the object the write replaced or deleted. The proxy that took the write sends the key to S3 | Until that proxy's background retry lands after the volume recovers, plus the RAM bounds in the row above on the other proxies. The other proxies wait on that retry only while the proxy that took the write keeps running, or has the key in `logging.local_state_dir`, and still tracks the key by name. That ends early if the proxy restarts before the retry lands and the key had no record in `logging.local_state_dir`. It also ends if the proxy ran out of room to track keys by name and sent every read to S3 instead. The other proxies can then serve the replaced or deleted object until `get_ttl` (GET, default `1d`) or `head_ttl` (HEAD, default `60s`) passes, or until the key's next write | Keep `logging.local_state_dir` (default `/var/lib/s3-proxy`) writable on a local file system. Alert on `cache.write_stamp_log_memory_only` and `cache.volume_stall_breaker_open`. Fix the volume's metadata-operation latency | [Write-through ordering](CACHING.md#write-through-ordering), [A slow cache volume on the write path](ERROR_HANDLING.md#8-a-slow-cache-volume-on-the-write-path-bounded-waits) |
| One bucket name is in use at two S3-compatible stores and the proxy cannot tell them apart: the stores share a hostname (two ports, or two transports), or the bucket's cached entries were written before the proxy recorded which store the bucket belongs to | An object, or HEAD headers, from the other store | GET: until `get_ttl` has passed since the copy was cached or last confirmed (default `1d`). HEAD: until `head_ttl` has passed (default `60s`). With a shared hostname this recurs for as long as both stores are in use | Give each store its own bucket names: that is the only way both stores are cached. Otherwise give each store its own hostname, so the bucket name binds to one of them and the other's requests are forwarded uncached. Or set `get_ttl: "0s"`, `head_ttl: "0s"` and `evaluate_conditions_from_cache: false` on the bucket, so every read is checked against the client's own store | [Cache-key note](CONFIGURATION.md#cache-key-note), [One bucket name at two origins](CACHING.md#one-bucket-name-at-two-origins), [Removing one bucket's cached data](CACHING.md#removing-one-buckets-cached-data) |
| A proxy fronts Amazon S3 in both the commercial and GovCloud (US) partitions on one cache volume, and one bucket name exists in both | An object, or HEAD headers, from the other partition | As the row above | Use a separate cache volume for each partition, or set `get_ttl: "0s"`, `head_ttl: "0s"` and `evaluate_conditions_from_cache: false` for the shared bucket names | [What counts as one origin](CACHING.md#what-counts-as-one-origin) |
| S3 fails with a transport error or a retryable `5xx` while the proxy revalidates a cached entry for several coalesced requests | The cached copy, which can be older than S3's current object | One failed revalidation: only the requests already queued behind it are answered from the cached copy, and the next request revalidates again. The copy is at most `get_ttl` old, counted from when the object was cached or last confirmed by S3, and is never served for a key with `get_ttl: "0s"` | Set `get_ttl: "0s"` for keys where an error must not be answered from cache | [GET TTL](#get-ttl-get_ttl) (availability exception) |
| A cached range file is damaged on disk | For a read the proxy assembles in memory (one cached extent smaller than `cache.disk_streaming_threshold`, 1 MiB by default, or a slice or merge of up to 8 MiB): nothing wrong, the proxy fetches from S3. For a read streamed from the cache (one extent at or above that threshold, or a larger read over several extents): the response stops partway with a connection error, and the bytes sent before the check failed can be wrong | On each proxy, the streamed read that finds the damage and any already reading the file. That read marks the file, and later reads of it on that proxy are fetched from S3 until the range is removed or cached again. A mark lapses two minutes after the last read it turned away, so a damaged file no one reads for that long can break one more streamed read | Validate checksums in the client: a full-object GET carries the stored `x-amz-checksum-*` headers. Watch `range_file_corruption_detected` on `/metrics` | [Integrity](COMPRESSION.md#integrity-every-write-is-a-checksummed-lz4-frame) |
| Something other than the proxy writes to the cache volume | Whatever bytes and headers were written, served as the object | Until the entry is replaced, invalidated, expires or is evicted | Restrict write access to the cache volume to the proxies. The frame checksum detects accidental damage, not deliberate edits | [Trust and integrity model](ARCHITECTURE.md#trust-and-integrity-model) |

Each proxy measures an entry's age on its own clock against the time recorded by the proxy that
cached it. Clock skew between proxies therefore lengthens or shortens every TTL bound above by the
skew. Keep the proxies' clocks synchronized.

## Time-to-live (TTL) configuration

### GET TTL (`get_ttl`)

Default: `1d` (1 day). `get_ttl` controls how long cached data stays valid for GET requests.

```yaml
cache:
  get_ttl: "1d"  # Default
  actively_remove_cached_data: false  # Lazy expiration (default)
```

- An overwrite or delete through a proxy sharing the cache volume invalidates the cached copy. A
  change made any other way is seen once `get_ttl` has passed since the entry was cached or last
  confirmed by S3. At the default that is within a day, at the cost of about one conditional GET
  per cached object per day when the object is read.
- For data that never changes after it is written, set a longer value, up to `315360000s`
  (10 years), globally or for a prefix in `cache_rules.json`.
- Freshness is judged at each access against the currently resolved `get_ttl`.

When `get_ttl` passes, what happens depends on the expiration mode:

- **Lazy expiration** (`actively_remove_cached_data: false`, the default): expired entries stay on
  disk until they are read or evicted, and the next request revalidates with S3 as described
  below. This suits fixed-capacity deployments, where eviction manages space.
- **Active expiration** (`actively_remove_cached_data: true`): the daily validation scan removes
  expired entries. This suits storage that bills by usage. It does not work with a zero TTL: a
  zero-TTL entry is expired as soon as it is cached, so active expiration removes the data that
  revalidation works from.

**Revalidation.** When a request reads an expired entry under lazy expiration, the proxy sends S3
a conditional request:

1. It sends `If-None-Match` with the cached ETag and `If-Modified-Since` with the cached
   `Last-Modified`.
2. If S3 returns `304 Not Modified`, the object is unchanged and the proxy serves the cached data
   with no body transfer. The `304` starts a new `get_ttl` window: GETs within the next `get_ttl`
   are served from cache without contacting S3, and the next revalidation comes once that window
   has passed. A local `If-Match` serve under `evaluate_conditions_from_cache` (below) does not
   start a new window, because S3 was not asked. When the `304` answers a coalesced waiter, it
   also starts a new `head_ttl` window for HEAD.
3. If S3 returns `200 OK`, or `206 Partial Content` for a range request, the object changed. The
   proxy invalidates the key's old cached coverage, then fetches and caches the new data.
   Invalidation reaches every range that the object's metadata and pending journal records name.
   Cache invalidation behavior under [Scenario 3](#scenario-3-client-conditional-headers-forwarded-to-s3)
   describes the one case it does not reach.
4. If S3 returns `403 Forbidden` or `401 Unauthorized`, the proxy returns the error to the client
   and keeps the cached data. A credentials failure is not a data change, and the cached data
   remains valid for other authorized callers. The proxy does not serve the expired data in this
   case.

This applies to full-object GETs and byte-range GETs alike. A range revalidation carries the
client's original `Range` header unchanged, including suffix (`bytes=-512`) and open-ended
(`bytes=1024-`) forms, so a client that signs `Range` in its SigV4 `SignedHeaders` is unaffected.

If the client sent its own `If-None-Match` or `If-Modified-Since`, the proxy does not add its own
validators. It forwards the client's precondition, S3 evaluates it as the client intended, and the
proxy returns S3's answer. A `304` stays a `304`, including for a request coalesced with another
request's fetch. An expired range entry is not revalidated with the client's validator: the
request is forwarded.

**Write-cached entries with no known `Last-Modified` are revalidated whatever their TTL.** S3's
PutObject and CompleteMultipartUpload responses never carry `Last-Modified`, so a write-cached
entry starts without one. The first GET is normally a cache hit that never reaches S3, so nothing
would otherwise learn it. A write-cached entry with no known `Last-Modified` therefore revalidates
on its next GET, even while its TTL says it is fresh. The proxy sends `If-None-Match` alone,
because it has no `Last-Modified` to send as `If-Modified-Since`. A `304` that carries
`Last-Modified` stores it and serves the cached bytes with it. A `304` without one stores nothing,
and the entry gets the same check on its next read; an S3-compatible origin need not send it, per
RFC 7232 §4.1. This check does not depend on `evaluate_conditions_from_cache`. It applies only to
entries still marked write-cached with the field unknown, not to entries cached on read.

**Expired entries stay available for revalidation.** Under lazy expiration, an entry past
`get_ttl` keeps its metadata, ETag and range files, and the proxy revalidates it instead of
fetching it again. An unchanged object then costs one conditional round trip and no body transfer,
however large it is. Two limits apply:

- **The entry is available only while its files exist.** Active expiration and capacity eviction
  both delete expired entries on their own schedules. Once either has, the next request is an
  ordinary miss that transfers the body, so the saving depends on lazy expiration and spare
  capacity.
- **Incomplete coverage is still a miss.** If the metadata lists a range the proxy cannot read
  (the `.bin` file is gone, or the cached extents do not cover the whole request), the proxy
  fetches instead of serving bytes it cannot prove it has. A `304` proves the version is current;
  it does not prove the cache still covers the request.

**Availability exception: coordinated stale-if-error.** Several concurrent requests for one key can
be coalesced behind one revalidation. When that revalidation fails with a transport error or a
retryable upstream `5xx`, the waiting requests may be served the cached data instead of an error.
It is the one case in which the proxy serves expired data without a successful validation. It does
not extend to `403`, `401`, signature failures or any other client error; those return the error
to the client.

`cache.evaluate_conditions_from_cache` (default `true`) sets how the proxy treats a client's
conditional headers, described in the next two sections. The proxy also adds conditional headers of
its own in the cases under
[Proxy-internal conditional injection](#proxy-internal-conditional-injection-independent-of-the-above).

### Opt-out: always forward to S3 for condition evaluation (`evaluate_conditions_from_cache = false`)

GET and HEAD requests that carry `If-Match`, `If-None-Match`, `If-Modified-Since`,
`If-Unmodified-Since` or `If-Range` are forwarded to S3 with the client's headers intact, with or
without a `Range` header. The SigV4 signature is preserved, so the conditional reaches S3 exactly as
the client signed it. S3 judges every precondition, and the proxy acts on S3's response:

- **`200 OK` or `206 Partial Content`**: return the data to the client and cache it (full object
  or range). The AWS CLI's CRT client takes this path, because every ranged GET of its split
  downloads carries `If-Match`, so CRT downloads of large objects fill the cache. If the object
  changed, the cache lookup drops the stale entry when the ETags differ.
- **`304 Not Modified`**: return `304` to the client and keep the cached bytes. The proxy refreshes
  the TTL when S3's response ETag equals the cached ETag. When they differ, the cached copy is
  stale and is invalidated on the next miss without a conditional, because a `304` carries no body
  to replace it with.
- **`412 Precondition Failed`**: return `412` to the client; the cache is unchanged.
- **`403` or `401`**: return the error; the cache is unchanged, since a credentials failure is not
  a data change.

With this setting the proxy never answers a precondition from a stale cached ETag, and each
conditional request costs one S3 round trip.

### Default: serve `If-Match` hits from cache (`evaluate_conditions_from_cache = true`)

When `If-Match` is the only conditional header on a GET, range GET or HEAD, and the cache holds the
requested data with a matching ETag, the proxy serves the response from cache without contacting
S3. The client's `If-Match` names the version it wants, so a cached copy with that ETag is that
version. The entry is served from cache only while it is within `get_ttl` of being cached or last
confirmed by a `304`, and only when `get_ttl` is not zero. A HEAD is answered from the cached HEAD
entry, which must also be within `head_ttl`. Serving from cache does not refresh either TTL.

A lone `If-Range` is treated the same way, for the same reason. When its validator strong-matches
the cached ETag, the client has named the version the cache holds, and the range is served from
cache with no S3 call. As with `If-Match`, the serve does not refresh the TTL, and an entry past
its TTL revalidates normally. `If-Range` says which version a `Range` applies to, not that the
representation is fresh (RFC 7233 §3.2).

An `If-Range` mismatch always forwards, and here `If-Range` differs from `If-Match`. A failed
`If-Match` is a bodyless `412` the proxy could answer locally, but a failed `If-Range` requires the
full current representation with `200`, a body the proxy may not hold. The request also forwards
when the validator is an HTTP date or a weak ETag, which cannot be compared with a cached ETag, or
when nothing is cached.

For `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`, alone, together, or with
`If-Match` or `If-Range`, the proxy always forwards the full conditional request to S3. These are
negative-match or timestamp assertions that only S3 can answer reliably, and a local answer could
rest on a stale cached validator. `If-Match` together with `If-Range` also forwards: the two
validators can disagree, and only S3 can resolve a precondition that passes while the range
validator is stale.

| Conditional | Cache holds the data with a matching ETag | Action |
|---|---|---|
| `If-Match` only | Yes, within `get_ttl` (a HEAD also within `head_ttl`) | Serve from cache; no S3 call; TTL not refreshed |
| `If-Match` only | Yes, but past `get_ttl` | Revalidate with S3 first |
| `If-Match` only | No (miss, ETag mismatch, or partly cached) | Forward to S3 → `412`, or `200`/`206` (cached) |
| `If-None-Match` | Any | Always forward to S3 |
| `If-Modified-Since` | Any | Always forward to S3 |
| `If-Unmodified-Since` | Any | Always forward to S3 |
| `If-Range` only | Yes, within `get_ttl` | Serve the range from cache; no S3 call; TTL not refreshed |
| `If-Range` only | No (miss, mismatch, or a weak or date validator) | Forward to S3 → `206`, or `200` with the full object |
| Mixed (`If-Match` and any other) | Any | Forward to S3 |

This is the recommended setting for the AWS CLI's CRT client. CRT puts `If-Match` on every ranged
GET to pin the parts to one object version, so with the default `true` a warm CRT download is
served from cache with no S3 round trips. With `evaluate_conditions_from_cache: false`, every CRT
ranged GET contacts S3.

**An entry past `get_ttl` does not qualify for the local `If-Match` serve.** Expired entries are
kept so that they can be revalidated instead of downloaded again, but that does not make them safe
to serve. The local serve compares `If-Match` with the cache's own stored ETag, which says nothing
about the origin's current object, so a client replaying an old ETag would otherwise get stale
bytes. Entries within `get_ttl` still serve locally, so warm CRT downloads are unaffected. The cost
is one upstream request for an `If-Match` read of an expired key. With `get_ttl: "0s"` every entry
is past `get_ttl`, so a GET with a lone `If-Match` always reaches S3.

### Proxy-internal conditional injection (independent of the above)

Whatever `evaluate_conditions_from_cache` says, the proxy adds conditional headers to its own S3
requests in three cases:

1. **Revalidating an expired entry.** Under lazy expiration, a read of an entry past its TTL sends
   `If-None-Match: <cached-etag>` and `If-Modified-Since: <cached-last-modified>`, so S3 answers
   `304` when nothing changed (RFC 7232 §3.2).
2. **Merging partly cached data.** When cached ranges cover part of a request and the proxy must
   fetch the missing bytes, it adds `If-Match: <cached-etag>` and
   `If-Unmodified-Since: <cached-last-modified>`. S3 then refuses with `412` instead of returning
   bytes from a newer object version, which would corrupt the merged response. On a `412`, the
   proxy invalidates the stale entry and retries once without the added headers. The client never
   sees the `412`.

   A local `If-Range` serve uses the same pin: the proxy replaces the client's `If-Range` with an
   `If-Match` on the ETag it just matched, so the `If-Range` never reaches a gap fetch. A stale
   `If-Range` on a gap fetch would make S3 ignore `Range` and return the full object with `200`.
   The gap-fetch path buffers a response body in memory before checking its status, so it would
   read that whole object and then discard it as a non-`206`. The `If-Match` form fails with a
   bodyless `412` instead. The proxy skips the swap when `If-Range` is in `SignedHeaders`, since
   removing a signed header would invalidate the client's signature.
3. **Replacing a missing range file.** When the cache lists a range whose file is gone, the proxy
   fetches that range again from S3 with `If-Match: <cached-etag>`, unless the client sent its own
   `If-Match`. It serves the fetched bytes only when S3's response carries the cached ETag. On a
   `412` or a different ETag, it invalidates the cached object and forwards the client's original
   request to S3 as received, plus one `Via` entry, so the response's bytes, ETag and length all
   come from one version. A presigned URL that lists `if-match` in `X-Amz-SignedHeaders` gets the
   ETag comparison without the added header.

The proxy always preserves client-supplied conditional headers exactly. Headers it adds are
internal, and the client never sees them.

#### A revalidation response's ETag decides currency, not its status

The proxy forwards a client's own conditional unchanged, so the response to a revalidation may
answer the client's question instead of the proxy's. Its status therefore says nothing reliable
about the cached copy. Two examples, both measured against S3:

- `If-None-Match: *` returns `304` for any object that exists, whatever the cached ETag is.
- `If-None-Match: "<value that cannot match>"` with a `Range` returns `206` even when the object
  has not changed.

So the proxy compares the ETag the response carries with the ETag it has stored:

| Response | Verdict |
|---|---|
| ETag equal to the cached one | The cached copy is current: refresh the TTL and serve it |
| ETag different from the cached one | The cached copy is out of date: invalidate its coverage and fetch again. Cache invalidation behavior under [Scenario 3](#scenario-3-client-conditional-headers-forwarded-to-s3) describes the one case not reached |
| No ETag (for example `412`) | Undetermined: change nothing in the cache and pass the response through |

`304`, `200` and `206` all carry the origin's current ETag, so the common cases are decidable.
Deciding from the status alone would let a crafted client conditional get out-of-date bytes served
or an unchanged entry discarded, so the proxy does not consult the status.

`get_ttl` affects GET requests only. HEAD requests follow `head_ttl`.

### HEAD TTL (`head_ttl`)

Default: `60s` (1 minute). `head_ttl` controls how long cached HEAD metadata (ETag, Last-Modified
and headers) stays valid for HEAD requests.

```yaml
cache:
  head_ttl: "60s"  # Default
```

- `head_ttl` affects HEAD requests only. An expired HEAD entry does not make a GET revalidate.
- GET and HEAD share one `.meta` file per object, with a separate expiry for each.
- When `head_ttl` passes, the `.meta` file stays and its cached ranges still serve GETs. The next
  HEAD is forwarded to S3, and its response refreshes the HEAD fields in the `.meta`.
- A GET within `get_ttl` serves cached data whatever the state of the HEAD entry.

**Clients that send many HEADs.** The AWS Common Runtime (CRT), used by AWS CLI v2 and recent SDKs,
sends a HeadObject before every GetObject to read the object's metadata. A cached HEAD answers it
without an S3 round trip. Mountpoint for Amazon S3 sends frequent HeadObject requests to check that
objects exist and to read their properties.

### Minimum TTL values

No minimum is enforced. `get_ttl` and `head_ttl` accept any valid duration, including `"0s"`.

**TTL=0 (always revalidate).** A zero TTL makes every request revalidate with S3:

```yaml
cache:
  get_ttl: "0s"
  head_ttl: "0s"
  actively_remove_cached_data: false  # Required with a zero TTL
```

- The cache still stores the data, so range merging and bandwidth savings still work.
- Every request sends S3 a conditional request with the client's credentials, so S3 checks IAM
  authorization on every request.
- S3 returns `304`: the cached data is served and the body transfer is saved.
- S3 returns `200`: the new data is fetched and cached.
- S3 returns `403`: the client is not authorized and gets the error.

**Both TTLs are required.** `get_ttl` covers GET, including a `?partNumber=` GET and a GET whose
only conditional header is `If-Match` or `If-Range`. `head_ttl` covers HEAD, including a HEAD that
carries a `Range` header or a lone `If-Match`. Neither implies the other. The YAML above sets both
globally. The same applies per prefix in `cache_rules.json`, where a rule naming only `get_ttl`
leaves HEAD requests answerable from cache for `head_ttl` (default `60s`).
`evaluate_conditions_from_cache: false` adds nothing at `get_ttl: "0s"`, where a GET with a lone
`If-Match` or `If-Range` is forwarded whatever it says.
[Zero TTL revalidation](CACHING.md#zero-ttl-revalidation) shows the per-rule form.

If S3 must evaluate IAM on every read, and not only check freshness, both settings are needed: a
request answered from cache is a request S3 never authorizes.

**Lazy expiration is required.** A zero TTL needs `actively_remove_cached_data: false`. Active
expiration deletes expired entries, and a zero-TTL entry is expired as soon as it is cached, so
active expiration removes the data that revalidation works from.

[Shared cache access model](ARCHITECTURE.md#shared-cache-access-model) covers the security side of
a zero TTL and when to use one.

### PUT TTL (`put_ttl`)

Default: `1h` (1 hour). `put_ttl` controls how long a write-cached object stays valid after its PUT.

```yaml
cache:
  put_ttl: "1h"  # Default
```

- An object cached by a PUT starts with `put_ttl`.
- `put_ttl` is shorter than `get_ttl` because an uploaded object may never be read, and an unread
  object only takes space.
- A GET within `put_ttl` moves the entry to `get_ttl`, with a metadata-only update and no data
  copy.
- A write-cached object is stored in the range format, so it serves range requests at once.

For example:

1. A client PUTs an object. It is cached as range 0-N with `put_ttl` (1 hour).
2. The client GETs it 30 minutes later. The GET is served from cache, and the entry moves to
   `get_ttl` (1 day by default).
3. A later range request is served from cache with no S3 fetch.
4. An object not read within the hour expires, and the daily validation scan removes it in either
   expiration mode (see [Lazy vs periodic cleanup](#lazy-vs-periodic-cleanup)).

### Cache expiration modes (`actively_remove_cached_data`)

The default is `false`, lazy expiration:

```yaml
cache:
  actively_remove_cached_data: false  # Lazy expiration (default)
```

#### Lazy expiration (`false`)

For fixed-capacity deployments, where eviction manages the cache size:

- Expired read-cache entries stay on disk until they are read or evicted.
- A GET of an entry past `get_ttl` revalidates with S3 (see
  [Scenario 3c](#scenario-3c-ttl-expired-cache-entry-revalidation)).
- Background passes still remove three kinds of entry that no TTL decision can revive: write-cached
  entries whose `put_ttl` passed unread, HEAD-only entries (no cached ranges) whose HEAD expiry
  passed more than a day ago, and abandoned multipart uploads (see
  [Lazy vs periodic cleanup](#lazy-vs-periodic-cleanup)).
- No background pass spends I/O removing expired read-cache entries, and an expired entry stays
  available for a bodyless `304` revalidation.

Typical deployments are a local SSD with a fixed cache size, a container with a persistent volume
claim, or a single proxy with dedicated cache storage.

#### Active expiration (`true`)

For storage that bills by usage, such as an elastic NFS file system. The daily validation scan
removes expired entries, so their space is freed at the first scan after `get_ttl` passes instead
of when eviction needs it, which lowers the bill on usage-billed storage. The scan's deletions add
I/O, and they remove data that a read soon afterwards could have revalidated with a bodyless `304`.

#### HEAD metadata expiration

HEAD expiry works the same way in both modes: an expired HEAD is forwarded to S3, and the response
refreshes the `.meta`, whose cached ranges are unaffected. See [HEAD TTL](#head-ttl-head_ttl).

### Presigned URL support

The proxy serves AWS SigV4 presigned URLs, which carry their credentials in query parameters for
time-limited access to an object.

**Expiry check.** The proxy reads `X-Amz-Date` and `X-Amz-Expires` from a SigV4 presigned URL and
answers an expired one with `403 Forbidden`, before any cache lookup. The check is a courtesy, not
an access check: the proxy never verifies a signature. It does not check expiry on a URL that
carries `X-Amz-Algorithm` without both `X-Amz-Date` and `X-Amz-Expires`, or on a SigV2 URL
(`Expires=<epoch>`). It answers `400 Bad Request` to a URL whose `X-Amz-Algorithm` is not
`AWS4-HMAC-SHA256`, including SigV4A.

**Cache key.** The cache key is the path alone, without query parameters.
`/bucket/object?X-Amz-Signature=abc123` is cached as `bucket/object`, so every presigned URL for
one object shares one cache entry.

**Choosing a TTL**:

1. **Long TTL** (`get_ttl: "315360000s"`, 10 years, the maximum): a cache hit makes no S3 request.
   Expired SigV4 presigned URLs are still refused before the cache lookup. Use it when presigned
   URLs should work until their own expiry, even after an STS credential behind them has expired.
2. **Zero TTL** (`get_ttl: "0s"`): every request revalidates with S3, sending `If-None-Match` with
   the cached ETag, and `If-Modified-Since` where a `Last-Modified` is cached. S3 answers
   `304 Not Modified` when the data is unchanged, so no body is transferred. Also set
   `head_ttl: "0s"` if S3 must see every request (see [Minimum TTL values](#minimum-ttl-values)).
   Use it when S3 should authenticate every request.

**Security.** With a long TTL, cached data stays readable through a valid presigned URL for the
whole TTL. A SigV4 presigned URL past its expiry is refused whatever the cache holds. The expiry
checks above are the only checks, so a URL the proxy cannot read as SigV4 with an expiry is served
whenever the entry is fresh. Choose the TTL to match your security requirements.

## Cache validation flow

### GET request scenarios

#### Scenario 1: Fresh cache (`get_ttl` valid)

```text
Client GET → proxy checks the cache → get_ttl valid → serve from cache
```

No S3 request is made. The state of the HEAD entry does not matter for a GET.

#### Scenario 1a: Re-read on the same proxy before consolidation

```text
Client GET → cache miss → S3 streams 200 → proxy stores the object and journals its metadata
          → Client GET for the same key on the SAME proxy, before consolidation
          → No .meta on disk yet, or only one a HEAD wrote for the same version
          → Proxy uses its in-memory record of the fill it just made
          → get_ttl valid, age measured from the fill or the HEAD → serve from cache
```

A streamed cache miss writes the object's range file at once, but the range appears in the `.meta`
only when the journal consolidator next runs. For that interval, the proxy that did the fill keeps
an in-memory record of what the `.meta` will contain, so a repeat read on that proxy is a cache hit
instead of a second S3 fetch. Only a fill of the whole object with a non-zero `get_ttl` and an ETag
or Last-Modified is recorded. The proxy checks the record against the range file on every use, and
discards it if the file is gone or has changed. The records take at most about 16 MiB of memory
per proxy.

A client that sends HEAD before GET, as `aws s3 cp` does, leaves a `.meta` with no ranges on disk
before the fill. The record is still used over that `.meta` when both describe the same object
version: the same ETag (the record must have one), the same content length, and the same
Last-Modified where both carry one. The age is then measured from whichever of the HEAD and the
fill came first, and the expiry is the earlier of the two.

The proxy never answers from the record without asking S3:

- When the key has a `.meta` with cached ranges, a `.meta` written by a HEAD alone that describes a
  different version (a different ETag, content length or Last-Modified), a `.meta` marked as
  write-cached by an upload, or a `.meta` that cannot be read or parsed. The record is discarded,
  and the read proceeds as if it did not exist.
- For a key whose current `get_ttl` is `0s`. Every read of such a key goes to S3, as it would with
  a `.meta`.
- For a key whose current rules turn the read cache off.
- Once `get_ttl` has passed, measured from the fill, or from a HEAD for the same version that came
  first. The age is taken from the fill or the HEAD, not from consolidation, so the record is never
  judged fresher than the `.meta` would be.
- Once the record is older than `max(4 × consolidation_interval, 30 s)`: 30 seconds at the default
  5-second interval. After that, the key is an ordinary cache miss until consolidation writes the
  fill into the `.meta`.
- After any proxy acknowledges a PUT or DELETE of the key, or this proxy detects that the object
  changed. Every use of the record reads the key's write-order marker again, and each acknowledged
  write or delete replaces that marker.

The record lives in one proxy's memory. A re-read that reaches a different proxy is an ordinary
cache miss until consolidation writes the fill into the `.meta`, and follows Scenario 1 after that.
A write or delete acknowledged by a different proxy ends the record the next time this proxy uses
it.

#### Scenario 2: `get_ttl` expired

```text
Client GET → proxy checks the cache → get_ttl expired
          → conditional GET to S3 with the cached validators
          → S3 returns 304 → refresh the TTL, serve the cached data
          → S3 returns 200 → replace the cached object, serve the new data
```

All parts and ranges of the object expire together.
[Scenario 3c](#scenario-3c-ttl-expired-cache-entry-revalidation) gives the full flow, including the
`403` case and byte-range requests. Under active expiration the entry may already have been
removed, and the GET is then an ordinary miss.

#### Scenario 3: Client conditional headers forwarded to S3

```text
Client GET with If-None-Match: "old-etag"
          → proxy detects the conditional header
          → forwards the whole request to S3 with ALL client headers
          → S3 evaluates the condition and responds:
             - 200 OK: object changed; return the new data, invalidate the old entry, cache the new data
             - 304 Not Modified: return 304; refresh the TTL if the 304's ETag equals the cached one
             - 412 Precondition Failed (If-Match / If-Unmodified-Since): return 412; cache unchanged
          → proxy returns S3's response to the client
```

This is the path for `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` and mixed
conditionals, and for every conditional when `evaluate_conditions_from_cache` is `false`. Under the
default, a GET or HEAD whose only conditional header is `If-Match` or `If-Range`, and that matches a
cached entry, is answered from cache instead; see
[Default: serve `If-Match` hits from cache](#default-serve-if-match-hits-from-cache-evaluate_conditions_from_cache--true).
The proxy forwards the client's conditional request to S3 unchanged and acts on S3's response
alone. A client-supplied `If-Match` that produces a `412` is passed through to the client
unchanged, and the cache is not modified.

**Cache invalidation behavior.** When the proxy invalidates a cached object, it removes the
object's metadata file and every range file that the metadata and the cache's pending journal
records name between them. It does so on an ETag or Last-Modified mismatch found by comparing a
response, and after a proxy-added `If-Match` produces a `412` on the partial-merge path. One case is
not reached. A cached range's file becomes visible on disk before its journal record is written. A
range cached between those two steps, or whose journal record then fails to write, is named by no
record, and an invalidation that runs meanwhile leaves that range file in place. The proxy does not
invalidate ranges selectively: any version mismatch removes the whole object.

#### Scenario 3a: Partial cache hit with cache validation

When cached ranges cover part of a request and the proxy must fetch the missing bytes, it adds
`If-Match: <cached-etag>` to the S3 sub-fetch, and `If-Unmodified-Since` from the cached
`Last-Modified`, each only where the client did not send its own. That stops a newer object
version's bytes from being merged with older cached bytes. The proxy attempts a merge only when the
requested range and every cached extent it overlaps are within 8 MiB. A larger, partly cached range
is fetched from S3 whole (see
[Scenario 3e](#scenario-3e-cached-data-too-large-to-assemble-in-memory)).

```text
Client GET for bytes 0-6291455 (6 MiB)
          → proxy checks the cache
          → cached ranges found: 0-2097151, 4194304-6291455
          → missing range: 2097152-4194303
          → proxy adds If-Match: <cached-etag> to the S3 sub-fetch (the client sent no If-Match)
          → S3 validates If-Match:
             - 200 / 206: object unchanged; merge cached and fetched bytes, serve
             - 412: the object changed on S3 since it was cached
                   → proxy invalidates the object's cached ranges (see Cache invalidation
                     behavior above for the one case not reached)
                   → proxy retries the S3 fetch once WITHOUT the added If-Match
                   → serves fresh data to the client and caches the fresh ranges
                   → the client never sees the 412 (it sent no conditional header)
          → return the complete response
```

When an added precondition fails, the client receives fresh data, never a corrupt merge or a `412`
it did not ask for. A client-supplied `If-Match` is always preserved, and its `412` is passed
through.

#### Scenario 3b: Fully cached across several ranges

```text
Client GET for bytes 0-104857599 (100 MiB)
          → proxy checks the cache
          → cached ranges found: 0-8388607, 8388608-16777215, ..., 96468992-104857599 (13 ranges)
          → missing ranges: none
          → the request is over the 8 MiB bound: stream the 13 cached extents in order
          → return the complete 100 MiB response
          → no S3 fetch
```

All bytes are cached across several ranges, as is typical after a multipart upload, and the
request is served from cache. A request within 8 MiB whose extents are each under 8 MiB is merged
into one buffer instead. Either way, no bytes come from S3.

#### Scenario 3c: TTL-expired cache entry (revalidation)

When a cached entry's TTL has passed, the proxy revalidates it with conditional headers instead of
downloading unchanged data again:

```text
Client GET → proxy checks the cache → get_ttl expired
          → proxy adds conditional headers from the cached metadata:
             - If-Modified-Since: the cached Last-Modified (1-second granularity)
             - If-None-Match: the cached ETag (when available)
          → forward the request to S3
          → S3 validates the conditions:
             - 304 Not Modified: object unchanged; refresh the TTL, serve from cache
             - 200 OK: object changed; invalidate the old entry, cache the new data, serve it
          → return the response
```

`If-None-Match` (the ETag) is the main revalidation signal. `Last-Modified` has one-second
granularity, so two writes to the same key within one second carry the same timestamp, which can
produce a false `304`. The ETag changes on every write, whatever the timing.

The proxy sends both headers when it has both, and whichever one it has otherwise. If it has
neither (the metadata is unreadable, was just removed by another request, or carries neither), it
sends no revalidation and forwards the client's request to S3 once, as a cache miss.

If S3 returns `403` or `401` during revalidation (expired credentials, revoked access), the proxy
returns the error to the client without invalidating the cache. A credentials failure is not a data
change, and the cached data remains valid for other authorized callers. The expired data is not
served either.

**Byte-range requests take the same path.** A range GET of an expired entry whose cached data fully
covers the requested interval revalidates exactly as above:

```text
Client GET bytes=1024-2047 → proxy checks the cache → complete coverage, get_ttl expired
          → conditional request carrying BOTH:
             - the client's original Range header, byte for byte
             - If-None-Match / If-Modified-Since from the cached metadata
          → 304 Not Modified: TTL refreshed; the requested bytes served from cache as 206
          → 206/200 with a new ETag: old coverage invalidated (see Cache invalidation
            behavior under Scenario 3 for the one case not reached); fresh bytes fetched
```

The `Range` header is forwarded unmodified, not rebuilt from parsed offsets, which keeps suffix and
open-ended forms intact and keeps a signed `Range` valid.

If coverage is only partial, the request is not treated as a conditional cache hit; the missing
ranges are fetched as usual.

#### Scenario 3d: HEAD detects object change (range invalidation)

When a HEAD response carries a different ETag or content length from the cached one, the proxy
removes every range file that the key's metadata and pending journal records name, drops them from
the metadata, and expires the entry. It then clears this proxy's RAM copies of the key's ranges and
metadata. This reaches the same ranges as the invalidation under Cache invalidation behavior in
[Scenario 3](#scenario-3-client-conditional-headers-forwarded-to-s3), and misses the same one case:
a range whose journal record has not been written yet.

```text
Client HEAD → proxy forwards to S3 → S3 returns a new ETag or content length
           → proxy compares it with the cached metadata
           → mismatch: remove the range files named by the metadata and the pending
             journal records, clear the ranges from the metadata, expire the object
           → update the metadata with the values from the HEAD
           → clear this proxy's RAM ranges and RAM metadata for the key
           → the next GET fetches fresh data from S3
```

This stops the proxy serving stale range data when the object was overwritten between GETs.

**A RAM copy on another proxy can outlast this.** The invalidation runs on the proxy that detected
the change. The `.meta` it clears is shared, but nothing sends the event to another proxy's RAM
cache. [RAM cache coherency](#ram-tier-coherency-contract) gives the bound that applies
instead.

#### Scenario 3e: Cached data too large to assemble in memory

A cached serve that would have to hold more than 8 MiB of uncompressed bytes in one buffer is
delivered differently, with the same freshness verdict. The proxy decides on the size after the
lookup has established coverage and run the live `get_ttl` check, so every case below is bounded
by `get_ttl` exactly as Scenarios 1 and 3c are. Only the delivery changes.

```text
Client GET bytes=1024-5119 of a key whose one cached extent is 5 GiB
          → proxy checks the cache → complete coverage, get_ttl valid
          → the requested bytes are inside one extent, and the extent is over the 8 MiB bound
          → stream the requested slice out of that extent; 206, x-cache: HIT

Client GET (no Range header) of a cached 5 GiB object
          → complete coverage, get_ttl valid
          → stream the cached extents in order; 200, no content-range

Client GET bytes=0-104857599 with a gap in coverage, or an extent that cannot be read
          → NOT a cached serve: the whole requested interval takes the streamed cache-miss
            path, so the bytes come from S3 and are as fresh as S3
          → the refetched interval is stored as one new range, so the next identical
            request is a single-extent cache hit

Client HEAD bytes=1024-5119
          → answered from the cached metadata alone; no range bytes are read from disk or RAM
          → bounded by head_ttl, and, where that metadata comes from the RAM metadata
            cache, also by cache.metadata_cache.refresh_interval
```

In the third case, a partly cached range over the bound is served from S3 instead of from a merge,
so its bytes cannot mix two object versions; the merge that would have needed an added `If-Match`
(Scenario 3a) does not happen. The cost is hit ratio: the cached part of the range is fetched again.

### RAM tier coherency contract

Each proxy keeps two RAM caches of its own: a metadata cache, which answers HEAD and GET metadata
lookups, and a range cache, which holds cached object bytes. A request to one proxy cannot evict,
update or invalidate anything in another proxy's RAM. The `.meta` file on the cache volume is the
only thing proxies share.

A response from RAM can therefore be stale, within these bounds:

- **RAM metadata answers** (a GET or HEAD answered from the RAM metadata cache instead of a disk
  read) are bounded by `cache.metadata_cache.refresh_interval` (default `5s`). An entry expires
  from the metadata cache after this interval, and the next lookup reads the shared `.meta` again.
- **RAM range hits** are bounded by `cache.ram_cache_verification_interval` (default `1s`). Within
  the interval, a RAM range hit reads nothing from the cache volume. The bytes and the response
  headers (`etag`, `last-modified`, `content-length`) all come from the RAM entry, which keeps the
  response to one version. Past the interval, the proxy reads the shared `.meta` once, coalesced
  per key so that concurrent hits share one read per interval. It compares the `.meta` ETag with the
  RAM entry's ETag before trusting the entry further.
- **A HEAD never takes the range bound, because it reads no range bytes.** A HEAD carrying a
  `Range` header is answered from the cached metadata, never from the RAM range cache, so the
  metadata bound applies, and the HEAD does not count as a RAM range hit. A dashboard that reads
  the RAM range hit rate as a guide to HEAD traffic reads low for that reason.
- **Both bounds nest inside `get_ttl`.** An answer from RAM never replaces the live TTL check
  against S3. It says only that the RAM copy agrees with the fleet's shared metadata, which is
  itself subject to `get_ttl`.
- **`get_ttl: "0s"` takes a key out of the RAM range cache, and only that cache.** A key whose
  `get_ttl` resolves to `"0s"` has `ram_cache_eligible` forced to `false`. Every RAM range serve
  path (full-object serves, range serves and page widening) skips the RAM lookup for such a key,
  including for bytes promoted before the rule was tightened. On the main GET and ranged-GET
  paths, the live `get_ttl` check runs before any cached bytes are considered, so a `"0s"` key
  revalidates with S3 first.

The RAM metadata cache follows a different rule: each consumer applies its own TTL, not `get_ttl`
alone.

| Consumer | Bounded by |
|---|---|
| HEAD response | `head_ttl`, independent of `get_ttl` (default `60s`) |
| Local `If-Match` answer (`evaluate_conditions_from_cache: true`) | The currently resolved `get_ttl` and the entry's stored expiry |
| Main GET and ranged-GET metadata | The live `get_ttl` check, which always revalidates at `"0s"` |

So `get_ttl: "0s"` alone does not make every read reach S3. A prefix that needs that, because S3's
IAM evaluation is the point and not only freshness, must also set `head_ttl: "0s"`.
[Zero TTL revalidation](CACHING.md#zero-ttl-revalidation) gives the per-field rule and why each
field is needed.

A client cannot tell "the proxy did not know about a change yet" from "the proxy learned of it a
moment ago", provided the response is self-consistent, which both bounds guarantee. They do not
guarantee immediacy across the fleet. Tightening either interval on one proxy shrinks only that
proxy's window, because nothing about RAM state passes between proxies. Do not read either bound as
a promise about how quickly a change made on one proxy becomes visible on another, beyond what the
shared `.meta` and these intervals provide.

### HEAD request scenarios

#### Scenario 4: Fresh HEAD cache (`head_ttl` valid)

```text
Client HEAD → proxy checks the HEAD entry → head_ttl valid → serve the cached headers
```

No S3 request is made.

#### Scenario 5: `head_ttl` expired

```text
Client HEAD → proxy checks the HEAD entry → head_ttl expired
           → forward the HEAD to S3
           → S3 returns headers
           → update the HEAD fields in the .meta with a new head_ttl
           → serve the headers
```

The `.meta` is not removed when `head_ttl` passes; its cached ranges keep serving GETs.

### PUT request scenarios

#### Scenario 6: Write-cached PUT

```text
Client PUT → forward to S3 → S3 returns 200 OK
          → store as range 0-N with put_ttl
          → mark upload_state = Complete
          → return success to the client
```

The object serves range requests at once. This covers signed PUTs, presigned PUTs and multipart
uploads. A POST object upload takes a different path and is not cached; see
[POST object uploads](CACHE_READ_PATHS.md#post-object-upload-is-not-write-through-cached).

#### Scenario 7: PUT-cached object read by a GET

```text
Client GET → proxy checks the cache → found with put_ttl
          → serve from cache
          → move the TTL from put_ttl to get_ttl (metadata-only update)
```

This suits objects uploaded once and read many times. The TTL change copies no data.

#### Scenario 8: Range request from a PUT-cached object

```text
Client GET with Range: bytes=0-1023 → proxy checks the cache → found with put_ttl
                                    → serve the range from cache (no S3 fetch)
                                    → move the TTL to get_ttl
                                    → return 206 Partial Content
```

A PUT-cached object serves range requests at once, with no S3 fetch.

### Multipart upload scenarios

#### Scenario 9: Multipart upload within capacity

```text
Client CreateMultipartUpload   → create mpus_in_progress/{hash(uploadId)}/upload.meta
Client UploadPart (part 1)     → stage part1.bin and its record (5 MiB)
Client UploadPart (part 2)     → stage part2.bin and its record (5 MiB)
Client CompleteMultipartUpload → order the requested parts, calculate their positions
                               → store as ranges: 0 to 5 MiB, 5 MiB to 10 MiB
                               → mark upload_state = Complete
                               → set the put_ttl expiry
```

A completed multipart upload behaves like a PUT-cached object.

#### Scenario 10: Multipart upload exceeding capacity

```text
Client CreateMultipartUpload   → create mpus_in_progress/{hash(uploadId)}/upload.meta
Client UploadPart (part 1)     → fits in the remaining cache capacity → staged
Client UploadPart (part 2)     → would take the cache past max_cache_size
                               → forwarded to S3 without being staged
Client CompleteMultipartUpload → part 2 is not staged → object not cached;
                                 the staging is left for the incomplete-upload sweep
```

Each part is checked against the remaining capacity as it arrives. The upload continues to S3
normally; only the caching is skipped.

#### Scenario 11: Incomplete upload removal

```text
Client CreateMultipartUpload → create mpus_in_progress/{hash(uploadId)}/upload.meta
Client UploadPart (part 1)   → stage part1.bin and its record
[The client disconnects and never completes]
[incomplete_upload_ttl passes with no activity, default 1 day]
Incomplete-upload sweep → finds the upload idle for longer than incomplete_upload_ttl
                        → deletes the upload's staging directory
                        → frees the cache space
```

Abandoned uploads therefore cannot hold cache space indefinitely.

#### Scenario 12: Part number request (cache miss)

```text
Client GET /bucket/5GB?partNumber=1 → proxy detects a GetObjectPart request
                                   → checks the cache for part 1 of /bucket/5GB
                                   → no metadata, or no part information → forward to S3
S3 response → Content-Range: bytes 0-8388607/5368709120
            → x-amz-mp-parts-count: 640
            → Content-Length: 8388608
Proxy → parse Content-Range → start=0, end=8388607, total=5368709120
      → store as range 0-8388607
      → record in the object's metadata: 640 parts, part 1 = bytes 0-8388607
      → stream the response to the client with the original headers
```

The first request for a part records its byte range for later requests.

#### Scenario 13: Part number request (cache hit)

```text
Client GET /bucket/5GB?partNumber=2 → proxy detects a GetObjectPart request
                                   → loads the metadata for /bucket/5GB
                                   → looks up part 2 → bytes 8388608-16777215
                                   → checks that range 8388608-16777215 is cached
                                   → range found in the cache
                                   → reads and decompresses the range data
                                   → builds the response:
                                     - Status: 206 Partial Content
                                     - Content-Range: bytes 8388608-16777215/5368709120
                                     - Content-Length: 8388608
                                     - x-amz-mp-parts-count: 640
                                     - ETag: (from the metadata)
                                   → streams the cached data to the client
```

Later part requests are served entirely from cache, using the stored byte ranges.

## S3 response header handling

The proxy stores S3's response headers and returns them on cache hits, so a cached response matches
what S3 would send. AWS SDKs read specific headers for checksums, encryption status and versioning.

### Headers stored in cache

The proxy stores every S3 response header in the cache metadata except six that belong to one
response or connection: `x-amz-request-id`, `x-amz-id-2`, `Date`, `Server`, `Connection` and
`Transfer-Encoding`. Because it stores everything else, a header S3 adds later is kept with no
change to the proxy. The stored headers include:

| Category | Headers |
|----------|---------|
| Core | `ETag`, `Last-Modified`, `Content-Length`, `Content-Type`, `Content-Encoding`, `Content-Language`, `Content-Disposition`, `Cache-Control` |
| Checksums | `x-amz-checksum-crc32`, `x-amz-checksum-crc32c`, `x-amz-checksum-sha1`, `x-amz-checksum-sha256`, `x-amz-checksum-crc64nvme`, `x-amz-checksum-type` |
| Encryption | `x-amz-server-side-encryption`, `x-amz-server-side-encryption-aws-kms-key-id`, `x-amz-server-side-encryption-bucket-key-enabled` |
| Versioning | `x-amz-version-id`, `x-amz-delete-marker` |
| Object Lock | `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`, `x-amz-object-lock-legal-hold` |
| Storage | `x-amz-storage-class`, `x-amz-restore`, `x-amz-expiration` |
| Multipart | `x-amz-mp-parts-count` |
| Replication | `x-amz-replication-status` |
| Other | `x-amz-website-redirect-location`, `x-amz-tagging-count`, `x-amz-missing-meta`, `x-amz-meta-*` (custom metadata) |

### Header restoration on cache hits

When the proxy serves from cache, it restores every stored header, with these exceptions:

- It calculates `Content-Length` from the response it sends, and `Content-Range` for a range
  request, and always sets `Accept-Ranges` to `bytes`.
- It leaves the checksum headers (`x-amz-checksum-*`) out of partial-range responses, because a
  checksum covers the whole object, not a byte range. It includes them when it serves the full
  object (range 0 to the object's length minus 1).

### Verifying header consistency

To check that cached responses match S3's, fetch an object twice through the proxy and compare the
headers. These commands use the endpoint URL your clients use. With the TLS proxy listener, set
`HTTP_PROXY=https://<proxy-host>:3129` first; with a DNS zone for S3 hostnames, no setting is
needed, but the request crosses the network in cleartext on port 80. SigV4 authenticates each
request but does not encrypt it; see
[What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes).

```bash
aws s3api get-object --bucket my-bucket --key test.txt --endpoint-url http://s3.eu-west-1.amazonaws.com /tmp/test1.txt  # First request: a cache miss, fetched from S3
aws s3api get-object --bucket my-bucket --key test.txt --endpoint-url http://s3.eu-west-1.amazonaws.com /tmp/test2.txt  # Second request: served from cache
```

The two responses' headers should be identical except `Date` and `x-amz-request-id`.

## Conditional headers handling

### Three uses of conditional headers

The proxy handles conditional headers in three ways, each described above:

1. **Client-supplied conditionals** are forwarded to S3, or answered from cache for a GET or HEAD
   whose only conditional header is a matching `If-Match` or `If-Range`, as
   `evaluate_conditions_from_cache` sets. See
   [Opt-out: always forward](#opt-out-always-forward-to-s3-for-condition-evaluation-evaluate_conditions_from_cache--false),
   [Default: serve `If-Match` hits from cache](#default-serve-if-match-hits-from-cache-evaluate_conditions_from_cache--true)
   and [Scenario 3](#scenario-3-client-conditional-headers-forwarded-to-s3). On a range request,
   the client's conditional headers are preserved exactly on every S3 fetch.
2. **Revalidating an expired entry** adds `If-None-Match` and `If-Modified-Since` from the cached
   entry. See [Scenario 3c](#scenario-3c-ttl-expired-cache-entry-revalidation).
3. **Merging partly cached data** adds `If-Match` and `If-Unmodified-Since` from the cached entry,
   and on a `412` invalidates the key and retries once without them. See
   [Scenario 3a](#scenario-3a-partial-cache-hit-with-cache-validation).

The client never sees a header the proxy added, or a `304` or `412` that such a header caused.

### Supported conditional headers

The proxy detects all four RFC 7232 headers (`If-Match`, `If-None-Match`, `If-Modified-Since` and
`If-Unmodified-Since`) and `If-Range` (RFC 7233 §3.2). S3 evaluates their precedence per RFC 7232
§6, and the proxy does not evaluate it again. The two cases where a matching validator is answered
from cache instead of forwarded are described above.

### Cost

- A `304` from a revalidation saves the full response body transfer.
- A `412` on a proxy-added precondition costs one extra S3 round trip (the retry), and only when the
  object changed on S3 during a partial merge.

### Logging

The retry after a proxy-added precondition fails logs at WARN:

```text
WARN S3 returned 412 on proxy-injected precondition (injected_if_match=true, injected_if_unmodified_since=true); invalidating cache and retrying: cache_key=bucket/obj
```

## Lazy deletion (on read)

An expired HEAD entry is refreshed on the next read, not deleted:

```text
Client HEAD request:
1. Check the RAM metadata cache, then the .meta file, for the entry
2. If found, check the HEAD expiry against the current head_ttl
3. If expired:
   - forward the HEAD to S3
   - update the HEAD expiry in the .meta file
4. If not expired:
   - return the cached headers
```

The `.meta` file and its cached ranges stay in place throughout.

## Lazy vs periodic cleanup

**On read**: an expired HEAD entry is refreshed from S3 when it is next requested, as above.
Nothing is deleted.

**Periodic** removal happens in two background passes, in either expiration mode:

- **The daily validation scan** (once a day, around midnight local time) deletes write-cached
  entries whose `put_ttl` passed unread. With `actively_remove_cached_data: true`, it also deletes
  read-cache entries whose `get_ttl` has passed. It also deletes `.meta` files that hold no cached
  ranges and whose HEAD expiry passed more than a day ago.
- **The incomplete-upload sweep** (at startup, then every `min(incomplete_upload_ttl / 4, 1h)`)
  removes multipart uploads idle for longer than `incomplete_upload_ttl`. See
  [What reaps abandoned staging](MULTIPART_UPLOAD.md#what-reaps-abandoned-staging).

A `.meta` that still holds ranges is never deleted for HEAD expiry alone.

## See also

- [Caching guide](CACHING.md): what the proxy caches, and what bypasses the cache
- [Cache read paths](CACHE_READ_PATHS.md): how each read is served
- [Cache internals](CACHE_INTERNALS.md): the on-disk layout, and the HEAD cleanup the daily validation scan runs
- [Configuration reference](CONFIGURATION.md): the TTL fields
- [Security considerations](ARCHITECTURE.md#security-considerations): the access-control side of TTLs
