# Caching guide

This page explains what Hybrid Cache for Amazon S3 caches, for operators deciding how to configure
it. It covers which requests the proxy caches and which it forwards uncached, how cache rules
change that per key, how writes invalidate cached copies, and the limits that apply. The proxy holds no AWS credentials and never sends S3 a
request of its own. It forwards client requests and caches the responses, on disk and in RAM.

| Page | Covers |
|---|---|
| [Cache freshness](CACHE_FRESHNESS.md) | TTLs, revalidation and conditional requests |
| [Cache read paths](CACHE_READ_PATHS.md) | How a read is served: range merging, page widening, multipart reads, coherency checks, download coordination |
| [Cache internals](CACHE_INTERNALS.md) | On-disk and in-memory layout, sharding, cache keys, access tracking, journals and size tracking |
| [Eviction](EVICTION.md) | How the proxy frees space: algorithms and the admission window |
| [Fleets on one cache volume](SHARED_STORAGE.md) | Running several proxies against one cache volume |
| [Compression](COMPRESSION.md) | LZ4 compression and the integrity check on every cached range |

The [configuration reference](CONFIGURATION.md) gives the type, default and valid range of every
field this page names.

## Contents

- [Cache rules](#cache-rules)
- [Requests that bypass the cache](#requests-that-bypass-the-cache)
- [Cache bypass headers](#cache-bypass-headers)
- [Versioned request handling](#versioned-request-handling)
- [One bucket name at two origins](#one-bucket-name-at-two-origins)
- [Tuning profiles](#tuning-profiles)
- [Cache invalidation](#cache-invalidation)
- [Compression](#compression)
- [Cache hit rate](#cache-hit-rate)
- [Limitations](#limitations)
- [See also](#see-also)

## Cache rules

Cache rules override the global cache settings for keys that match a glob pattern. They live in one
file, `cache_dir/cache_rules.json`, which the proxy reloads without a restart.
[Cache rules](CONFIGURATION.md#cache-rules) in the configuration reference covers the file format,
glob syntax, first-match-per-field precedence, reload behavior, cache-key forms and how many rules
to use. This section describes what each setting does at run time.

The proxy does not read per-bucket `cache_dir/metadata/{bucket}/_settings.json` files, and logs a
warning at startup when it finds any.
[Upgrading to 2.0.0](UPGRADING.md#upgrading-to-200-per-bucket-_settingsjson-removed) shows how to
translate them into `cache_rules.json`.

### Per-key cache control

#### `read_cache_enabled`

Controls whether GET responses for the keys a rule matches are cached. When `false`:

- GET response data is not written to the disk cache.
- Range data is not promoted to the RAM cache.
- Metadata is not stored in the RAM metadata cache.
- Requests are forwarded to S3 and streamed to the client.
- PUTs are still write-cached unless `write_cache_enabled` is `false`.

When `true`, the default, caching works normally. The global default is `cache.read_cache_enabled`
(default `true`). Set it to `false` globally for an allowlist, where only keys matched by a rule
with `"read_cache_enabled": true` are cached.

#### `write_cache_enabled`

Controls whether PUTs for the keys a rule matches are write-cached. When `false`, a PUT is
forwarded to S3 and not cached. The default comes from `cache.write_cache_enabled` (default
`true`). It is independent of `read_cache_enabled`.

#### `ram_cache_eligible`

Controls whether range data for the keys a rule matches can be held in the RAM cache. When
`false`, the data is served from the disk cache only. The default is the global
`cache.ram_cache_enabled` (default `false`). The proxy forces it to `false` when the key's `get_ttl` is `"0s"`,
because the RAM cache serves data without revalidating it, and when `read_cache_enabled` is
`false`.

### Zero TTL revalidation

A zero TTL (`get_ttl: "0s"` or `head_ttl: "0s"`) uses the normal cache path; there is no separate
bypass. The proxy stores the response already expired, so every later request finds expired data
and revalidates it with S3.

The GET flow with `get_ttl: "0s"`:

```text
Client GET → cache lookup (normal path)
           → Cold cache: fetch from S3, store it already expired
           → Warm cache (data present but expired):
               → Send If-None-Match (cached ETag) and If-Modified-Since (cached Last-Modified) to S3
               → 304 Not Modified: serve the cached data, which stays expired
               → 200 OK: replace the cached data, serve the new data
               → Error: forward the error to the client
```

Range requests and full-object GETs follow this flow. Freshness is checked per object, so all
cached ranges of an object share one state. When the object's age exceeds the current `get_ttl`,
the proxy revalidates. The age runs from the later of when the object was cached and its last
`304`. A `304` moves the expiry and that age anchor to now for every range at once, so with a
non-zero `get_ttl` the next revalidation comes one `get_ttl` later.

If the proxy cannot read or parse the metadata file during the expiry check, it treats the cached
data as expired and forwards the request to S3. It never serves data whose freshness it could not
check.

With `head_ttl: "0s"`, the cached HEAD entry is always expired, so the proxy forwards every HEAD to
S3 and updates the cache.

The RAM cache never holds range data for a key whose `get_ttl` is zero, because it serves data
without revalidation. The RAM metadata cache still holds entries for such a key. It holds
metadata, never object bytes, has its own refresh interval, and each of its consumers applies its
own TTL: `get_ttl` on the GET path and `head_ttl` on the HEAD path.

Zero-TTL revalidation applies to write-cached entries as well as to entries cached on read.

#### `get_ttl` and `head_ttl` are independent: set both

`get_ttl: "0s"` makes GET requests revalidate. HEAD requests follow `head_ttl` alone, which
defaults to `60s`. A rule that sets `get_ttl: "0s"` and leaves `head_ttl` at its default still
answers HEAD requests from cache for up to a minute, with the cached ETag, `Content-Length` and
stored response headers, and no S3 request.

If every read must reach S3 so that S3 evaluates IAM authorization, a HEAD served from cache is a
read S3 never sees. For such a prefix, set both fields. The example also sets
`evaluate_conditions_from_cache: false`, so that a client's `If-Match` is forwarded instead of
compared with the cached ETag, as a zero `get_ttl` already does:

```json
{
  "pattern": "**/restricted/**",
  "get_ttl": "0s",
  "head_ttl": "0s",
  "evaluate_conditions_from_cache": false
}
```

Each field governs a different set of requests:

| Field | Requests it governs |
|---|---|
| `get_ttl: "0s"` | GET, ranged GET, `?partNumber=` GET, and a GET whose only conditional header is `If-Match` or `If-Range`: every request reaches S3 |
| `head_ttl: "0s"` | HEAD, including a HEAD that carries a `Range` header or a lone `If-Match`: every request is forwarded to S3 |
| `evaluate_conditions_from_cache: false` | A GET or HEAD whose only conditional header is `If-Match` or `If-Range`. With the default `true`, the proxy answers it from cache without contacting S3 while the entry is within `get_ttl`, and for a HEAD also within `head_ttl`. At `get_ttl: "0s"` those requests are forwarded whatever this field says |

The proxy does not derive one field from the other: `head_ttl` is not clamped to `get_ttl`, and a
rule that sets only one of them is accepted as written. A long `get_ttl` with a short `head_ttl`
suits immutable objects whose existence clients poll often. A prefix where revoked access must take
effect at once has to name both fields.

**Sizing with a non-zero `head_ttl`.** Some clients send a HEAD and then split the download into
ranged GETs sized from the HEAD's `Content-Length`, as the AWS CRT and the transfer managers do.
Such a client can take the length from a cached HEAD while the GETs revalidate onto a newer object
version. Each GET is correct, because S3 validates it, but the transfer was planned against the
older length. Any non-zero `head_ttl` allows this, whatever `get_ttl` is; `head_ttl: "0s"`
removes it.

### Settings are re-evaluated on each read

The proxy resolves cache settings (TTLs, `read_cache_enabled`, `ram_cache_eligible`) against the
current rules on every read. It judges freshness by comparing an age with the current TTL, not by
an expiry stored when the entry was written. For GET, the age runs from when the object was cached
or last confirmed by a `304`, whichever is later, and is compared with `get_ttl`. For HEAD, it runs
from the last HEAD refresh and is compared with `head_ttl`. A rule change therefore reaches entries
already cached once the proxy has re-read the rules, with no restart and no cache wipe. The
proxy re-reads on a staleness window, and a slow read takes effect on a later request; see
[Hot reload and error resilience](CONFIGURATION.md#hot-reload-and-error-resilience). This includes
`?partNumber=N` GETs (see [Cache read paths](CACHE_READ_PATHS.md)) and the local `If-Match` serve
under `evaluate_conditions_from_cache: true`, which serve only while the entry is within the
current `get_ttl`.

For example:

- Changing `get_ttl` from `1h` to `0s` takes effect once the proxy has re-read the rules. The object
  then counts as expired on a later GET and revalidates with S3, and is served from cache only on a `304`.
- Changing `get_ttl` from `0s` to `1h` lets an entry already cached serve from cache while its GET
  age is within the new TTL.
- Changing `read_cache_enabled` from `true` to `false` deletes the key's cached entry (range files
  and metadata) at the first GET or HEAD after the rule takes effect, then forwards the request to
  S3. No manual cache wipe is needed. A GET that was already fetching from S3 when the rule took
  effect can still store its response after that delete. That entry is never served while the rule
  is `false`, and a later GET or HEAD for the key deletes it.

### When the read cache is off

When `read_cache_enabled` resolves to `false`, from a matching rule or the global default, the proxy
passes GET and HEAD requests through to S3 and streams the response to the client without caching
it:

```text
Client GET/HEAD → Proxy → S3 → response streamed to the client, not cached
```

- New requests read and write nothing in the disk cache, promote nothing to the RAM cache and store
  no metadata.
- PUTs are still write-cached unless `write_cache_enabled` is `false`.
- The first GET or HEAD after the rule takes effect deletes the key's cached data: its range `.bin`
  files and `.meta` metadata, including the HEAD entry. Later GETs and HEADs delete whatever is
  still cached for the key, such as the response of a GET that was already fetching from S3 when
  the rule took effect and finished storing it after the first delete.
- A conditional request that `evaluate_conditions_from_cache: true` would answer from cache is
  forwarded too, and triggers the same deletion.

## Requests that bypass the cache

The proxy caches GetObject and HeadObject responses only. Every other S3 operation, such as a LIST
or a read of an object's ACL or tags, returns data that can change at any time. The proxy forwards
those requests to S3 without reading or writing the cache. The bypass is automatic and cannot be
turned off.

### Operations that bypass cache

**LIST operations**:

- ListBuckets (GET or HEAD to the root path `/`)
- ListObjects (query parameter `list-type` or `delimiter`)
- ListObjectVersions (`versions`)
- ListMultipartUploads (`uploads`)
- ListParts (`uploadId`), on GET and HEAD

**Metadata operations**, on GET and HEAD:

- GetObjectAcl (`acl`)
- GetObjectAttributes (`attributes`)
- GetObjectLegalHold (`legal-hold`)
- GetObjectLockConfiguration (`object-lock`)
- GetObjectRetention (`retention`)
- GetObjectTagging (`tagging`)
- GetObjectTorrent (`torrent`)

**Requests with customer-provided encryption keys (SSE-C)**: any GET, HEAD or PUT that carries
`x-amz-server-side-encryption-customer-algorithm`, `x-amz-server-side-encryption-customer-key` or
`x-amz-server-side-encryption-customer-key-md5`. The proxy cannot decrypt SSE-C data, and the cache
key is the path alone. Caching plaintext fetched under one key and serving it under another key, or
none, would leak data. The proxy forwards SSE-C requests to S3 unchanged, and S3 enforces the key
match. Existing non-SSE-C entries for the same path stay cached. An SSE-C request to that path goes
to S3, and S3's response decides what the client sees.

**Part-scoped HEAD**: a HEAD with `partNumber` is forwarded to S3 with no cache lookup and no cache
write, under the bypass reason `part-scoped-head`. S3 answers such a request with that part's
`Content-Length` and a `Content-Range`, but the cache key carries no query string. Caching the
response under the whole-object key would record one part's length as the object's length, so a
client that sizes an object from HEAD would read only that many bytes. Answering a part-scoped HEAD
from a whole-object entry would return the object's length with no part count, so the proxy never
looks one up.

**Response-header overrides**: a GET or HEAD carrying `response-cache-control`,
`response-content-disposition`, `response-content-encoding`, `response-content-language`,
`response-content-type` or `response-expires` is forwarded with no cache lookup and no cache write,
under the bypass reason `response_header_override`. S3 rewrites the response headers to match the
query, and the cache key is the path alone, so a cached answer would hand those headers to every
later reader of the object.

**Multi-range GETs** (`Range: bytes=0-1,5-6`): forwarded to S3 uncached, under the bypass reason
`multi_range`. S3 ignores a multi-range `Range` and returns `200` with the whole object, and the
proxy returns the same.

**Unrecognized query parameters**, on GET and HEAD: a request reads or fills the cache only when
every query parameter is `versionId`, `partNumber`, `x-id` or a presigned URL authentication
parameter (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
`X-Amz-SignedHeaders`, `X-Amz-Signature`, `X-Amz-Security-Token`, `X-Amz-Region-Set`,
`AWSAccessKeyId`, `Signature`, `Expires`). Any other parameter sends the request to S3 uncached,
under the bypass reason `unrecognised_query`, so an object subresource or operation the proxy does
not know is never cached as the object. A request with `versionId` still bypasses the cache, as
[Versioned request handling](#versioned-request-handling) describes.

**Part reads are cached.** A GET with `partNumber` (GetObjectPart) is cached as a range in the
normal range storage. It writes a real range and takes the object's length from the total in
`Content-Range`, so it neither truncates a later read nor misreports the object's size. A
`partNumber` that is not an integer from 1 to 10,000 is forwarded uncached under the bypass reason
`invalid_part_number`, so S3 returns its own error.

### Cached operations

- **GetObject**: a GET, with or without a `Range` header, whose query parameters are all on the
  object-read list above.
- **HeadObject**: a HEAD to an object path (not the root path) whose query parameters are all on
  that list. The HEAD entry is stored in the object's `.meta` file beside the GET data, with its
  own TTL, `head_ttl`.

### Examples

```text
GET /bucket/object.txt                                 → Cached (GetObject)
GET /bucket/object.txt Range: bytes=0-100              → Cached (range request)
GET /bucket/?list-type=2                               → Bypassed (ListObjects)
GET /bucket/object.txt?acl                             → Bypassed (GetObjectAcl)
GET /bucket/object.txt?tagging                         → Bypassed (GetObjectTagging)
GET /bucket/object.txt?partNumber=1                    → Cached (GetObjectPart)
GET /bucket/object.txt?partNumber=0                    → Bypassed (invalid_part_number)
GET /bucket/object.txt?response-content-type=text/html → Bypassed (response_header_override)
GET /bucket/object.txt?uploadId=abc                    → Bypassed (ListParts)
GET /bucket/object.txt Range: bytes=0-1,5-6            → Bypassed (multi_range)
HEAD /                                                 → Bypassed (HeadBucket/ListBuckets)
HEAD /bucket/object.txt                                → Cached (HeadObject)
HEAD /bucket/object.txt?partNumber=1                   → Bypassed (part-scoped-head)
HEAD /bucket/object.txt?tagging                        → Bypassed (GetObjectTagging)
```

### Logging

The proxy logs each bypassed operation at DEBUG:

```text
DEBUG Bypassing cache: operation=ListObjects method=GET query="list-type=2&prefix=photos/"
      reason=list operation - always fetch fresh data path=/my-bucket/

DEBUG Bypassing cache: operation=GetObjectAcl method=GET query="acl"
      reason=metadata operation - always fetch fresh data path=/my-bucket/photo.jpg

DEBUG Bypassing cache: operation=ListBuckets method=HEAD query=""
      reason=list operation - always fetch fresh data path=/
```

The fields are `operation` (the S3 operation detected, such as ListObjects, GetObjectAcl or
ListBuckets), `method` (GET or HEAD), `path`, `query` and `reason`. A cached part is logged at
DEBUG as `Caching part 5 for my-bucket/large-file.bin as range <start>-<end>`.

With the application log at debug level, this counts bypasses by operation:

```bash
grep -h "Bypassing cache" /logs/app/*/s3-proxy.log* | awk -F'operation=' '{print $2}' | awk '{print $1}' | sort | uniq -c  # Bypasses per operation
```

```text
 142 ListObjects
  38 GetObjectAcl
  12 GetObjectTagging
   5 ListBuckets
```

A high count is expected on a workload that lists often.

## Cache bypass headers

A client can skip the cache lookup for a GET or HEAD with a standard HTTP cache-control header, to
debug, to test, or to read data that must come from S3:

| Header | Cache lookup | Response cached | Use |
|--------|--------------|-----------------|-----|
| `Cache-Control: no-cache` | Skipped | Yes | Fresh data, and later requests benefit |
| `Cache-Control: no-store` | Skipped | No | Fresh data, nothing cached |
| `Pragma: no-cache` | Skipped | Yes | The HTTP/1.0 form of `no-cache` |

When both `Cache-Control` and `Pragma` are present, `Cache-Control` decides. When `Cache-Control`
carries both directives, `no-store` wins. Header names and directives are matched
case-insensitively.

### Examples

The AWS CLI cannot add a request header, so these examples use `curl` with a presigned URL. They
reach the proxy through the TLS proxy listener (port 3129). With a DNS zone that resolves S3
hostnames to the proxies, used where the network segment is secure and cleartext on it is
acceptable, leave out `--proxy`. curl verifies the proxy's certificate, so add
`--proxy-cacert <ca-file>` when a private CA signed it.

```bash
url=$(aws s3 presign s3://my-bucket/my-file.txt --endpoint-url http://s3.eu-west-1.amazonaws.com)
curl -s --proxy "https://<proxy-host>:3129" -H "Cache-Control: no-cache" -o output.txt "$url"  # Fresh data, response cached for others
curl -s --proxy "https://<proxy-host>:3129" -H "Cache-Control: no-store" -o output.txt "$url"  # Fresh data, response not cached
curl -s --proxy "https://<proxy-host>:3129" -H "Pragma: no-cache" -o output.txt "$url"  # HTTP/1.0 form of no-cache
```

### Header stripping

When the proxy honors a bypass header, it removes `Cache-Control` and `Pragma` before forwarding
the request to S3. A header the client signed, in `Authorization` or in a presigned URL's
`X-Amz-SignedHeaders`, is forwarded as sent. All other headers are kept. A request with no bypass
directive, or any request while `cache_bypass_headers_enabled` is `false`, is forwarded with its
headers unchanged. The one addition is the `Via` entry the proxy appends to every forwarded request
unless the request signs `via`.

### Configuration

Bypass headers are honored by default:

```yaml
cache:
  cache_bypass_headers_enabled: true  # Default: true
```

When it is `false`, the proxy ignores `Cache-Control` and `Pragma` for bypass decisions, sends the
request through the normal cache path, and forwards the headers to S3 as received, plus one `Via`
entry unless the request signs `via`.

### Logging and metrics

Each honored bypass is logged at DEBUG:

```text
DEBUG Cache bypass via header: method=GET path=/bucket/object.txt reason=no-cache directive
DEBUG Cache bypass via header: method=HEAD path=/bucket/object.txt reason=no-store directive
```

The `reason` is `no-cache directive` or `no-store directive`; `Pragma: no-cache` is logged as
`no-cache directive`. `/metrics` counts honored bypasses in `cache.cache_bypasses_by_reason`:

```json
{
  "cache_bypasses_by_reason": {
    "no-cache directive": 42,
    "no-store directive": 15
  }
}
```

### Requests the proxy never caches

Bypass headers change only the cache lookup. Requests the proxy never caches stay uncached:

| Operation | Normal | With `no-cache` | With `no-store` |
|-----------|--------|-----------------|-----------------|
| GetObject | Cached | Lookup skipped, response cached | Lookup skipped, not cached |
| HeadObject | Cached | Lookup skipped, response cached | Lookup skipped, not cached |
| ListObjects | Not cached | Not cached | Not cached |
| GetObjectAcl | Not cached | Not cached | Not cached |

## Versioned request handling

The proxy bypasses the cache for any GET or HEAD that carries a `versionId` query parameter. It
reads nothing from the cache, writes nothing to it, forwards the request to S3 and returns S3's
response. Each bypass is counted in `cache.cache_bypasses_by_reason` under `versioned_request`, and
logged at DEBUG as `Versioned request detected, bypassing cache: path=/bucket/object`. The bypass
is automatic and cannot be turned off. Requests without `versionId` are cached normally.

```bash
aws s3api get-object --bucket my-bucket --key document.pdf --version-id "abc123" ./document-v1.pdf --no-cli-pager  # Bypasses the cache
```

The cache key is the bucket and object path, with no version. A cached entry holds the version that
was current when it was cached, and a versioned request never consults it. Restoring an old version
with CopyObject through the proxy invalidates the key's cached copy like any PUT (see
[PUTs that do not carry the object](#puts-that-do-not-carry-the-object)), so the next GET without
`versionId` fetches the restored version.

## One bucket name at two origins

A bucket name belongs to the first origin that caches it. The proxy records, per bucket, the origin
its cached entries came from. A request for that bucket name from a different origin is forwarded
to its own origin and served without being cached. No request fails: that origin's clients get
correct data, fetched from their origin on every read.

A bucket name in use at only one origin is unaffected, however many origins the proxy fronts, and
two origins with no bucket names in common do not interact. Only a name in use at two origins is
constrained, and only the origin that did not claim it loses caching for it.

The binding exists because a cache key is a bucket name and an object path, with no origin. Without
it, two origins holding different objects under one bucket name would share one cache namespace,
and a request to one origin could be answered from the other origin's cached copy.

### What counts as one origin

Every Amazon S3 endpoint in a partition is one origin. Path-style, virtual-hosted, dualstack,
path-style FIPS (`s3-fips.<region>`), Accelerate, legacy global, access-point and Multi-Region Access
Point hostnames under `amazonaws.com` all name the same origin. Reaching one bucket through several
of these addressing forms therefore matches and caches normally. Each other AWS partition domain
(`amazonaws.com.cn`, `amazonaws.eu`, `c2s.ic.gov`, `sc2s.sgov.gov`, `cloud.adc-e.uk`,
`csp.hci.ic.gov`) is a separate origin. It is reached through its endpoints that carry no bucket in
the hostname: `s3.<region>.<domain>`, `s3.dualstack.<region>.<domain>`, `s3-fips.<region>.<domain>`
and `s3-fips.dualstack.<region>.<domain>`, the legacy `s3-<region>.<domain>`, bare `s3.<domain>`,
and the Accelerate and access-point base domains.

A request whose host is under an AWS partition domain but is not one of those S3 forms is forwarded
and never cached. It reads no cached entry, writes none, and records no origin for the bucket. That
covers:

- other AWS services' hostnames, such as API Gateway and load balancers
- S3 website endpoints, S3 Express and S3 Object Lambda
- PrivateLink `vpce` hostnames
- the legacy `<bucket>.s3-<region>` form and the virtual-hosted FIPS form `<bucket>.s3-fips.<region>`
- a virtual-hosted hostname containing uppercase letters
- virtual-hosted hostnames outside `amazonaws.com`

Anyone can create a hostname under an AWS partition domain that serves arbitrary bytes. Caching
such a response under a bucket's key would let it replace that bucket's objects for every client.
To cache that traffic, send it to a path-style S3 endpoint instead.
`cache.uncacheable_aws_host_bypasses_total` on `/metrics` counts requests through these hosts.

In forward-proxy mode the proxy keys a request on its request-target host, while S3 answers from
the bucket the `Host` header names. The proxy therefore answers `400 InvalidRequest` to a
forward-proxy request whose `Host` header names a different host or port from its request target,
and to any request carrying more than one `Host` header. A port written as `:80` and an omitted port
count as the same. A client that builds both from one URL, as the AWS CLI does, is unaffected.

Any other host is an origin of its own, identified by its hostname alone: `store-a.example.com` and
`store-b.example.com` are two origins. The port and the transport are not part of the identity, so
one host reached on two ports is one origin. The [cache-key note](CONFIGURATION.md#cache-key-note)
covers the matching consequence for cache keys.

To make two stores on one host separate origins, give one of them a second DNS name that the
proxy's `dns_servers` resolve to the same address, key its `upstream_overrides` entry on that name,
and point its clients at it. The store must accept requests addressed to that name, and for
validated TLS its certificate must name it.

Amazon S3 in the GovCloud (US) Regions is under `amazonaws.com` and counts as the same origin as the
commercial partition, although its bucket names are a separate namespace.

### What it looks like when it fires

There is no error and no failed request. The symptom is a hit rate that stays low for one bucket
whose traffic looks cacheable. On `/metrics`, `cache.origin_marker_mismatch_bypasses_total`
counts these forwards on each proxy, and reads zero on a deployment fronting a single origin. A
separate counter, `cache.origin_marker_unparseable_bypasses_total`, counts requests for a bucket
whose recorded origin cannot be read. A non-zero value there points at the cache volume, not at
addressing.

To give both origins a cached copy, address them under different bucket names.

The binding does not keep two stores apart in the cases below. In each, a proxy can serve an
object, or HEAD headers, from the other store for up to `get_ttl` (GET) or `head_ttl` (HEAD):

- The two stores share a hostname: two ports, or two transports, including a front end that
  translates ports, since standard mode takes the egress port from the `Host` header. This repeats
  for as long as both stores are in use.
- Amazon S3 is fronted in both the commercial and the GovCloud (US) partitions.
- The bucket's entries were cached before it had a recorded origin.
- The cache volume refuses the write of the record.
- The record is removed or edited while proxies run.
- A proxy that does not check origin records shares the volume, as during a rolling upgrade (see
  [During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade)).

### A bucket that holds entries but no recorded origin

The proxy records a bucket's origin the first time it handles a cacheable request for the bucket,
so a bucket directory can hold cached entries with no recorded origin yet. Removing a bucket's
`_origin.json` file is part of removing its cached data.
[Upgrading to 3.0.0](UPGRADING.md#upgrading-to-300) covers the upgrade that leaves buckets in this
state.

From that state, the next cacheable request claims the bucket, and the entries already on the
volume are attributed to the origin that request came from. They are then served to that origin's
clients as its own, as ordinary cache hits that return whatever is on the volume. If the bucket
name is in use at two origins and those entries came from the other one, the claiming origin's
clients are served bytes their own origin never returned. Requests from the origin that did not
claim the bucket are forwarded uncached from then on. The entries already on the volume, including
any written from that origin, are not deleted: they stay, count towards the cache size, and leave
through ordinary eviction.

To stop a shared bucket name serving entries the other origin may have written, let them
revalidate or remove them:

- **Revalidate.** Add a `cache_rules.json` rule for the bucket with `get_ttl: "0s"`,
  `head_ttl: "0s"` and `evaluate_conditions_from_cache: false`. Keep it for at least the bucket's
  usual `get_ttl` (default `1d`). Every GET and HEAD then checks the cached ETag against the
  requesting client's own origin, and a copy that origin does not confirm is replaced. No proxy
  needs to stop. Once you remove the rule, any entry not read in the meantime is older than
  `get_ttl` and revalidates on its next read.
- **Remove.** Follow [Removing one bucket's cached data](#removing-one-buckets-cached-data), which
  also covers choosing the origin the bucket binds to.

**Any client that can reach the proxy can claim an unclaimed bucket name.** The first cacheable
request for a bucket records the origin it came from, whatever host the request names. A client
can ask for your bucket name with a `Host` header of its choosing before your own origin has made a
request. The bucket then binds to that host and fills with what that host returns. Your clients are
forwarded uncached from then on, and removing only the record would hand the squatter's entries to
your origin. To prevent this:

- Record the origin yourself before first use, as
  [Removing one bucket's cached data](#removing-one-buckets-cached-data) describes.
- Set `server.tls.connect_allowlist` so the proxy forwards only to the hostnames you list (see
  [Destination policy](ARCHITECTURE.md#destination-policy-ssrf-protection)).
- Restrict network access to the proxy.

Neither bypass counter moves while this happens. No record was present, so nothing was a mismatch
and nothing was unreadable. A zero on `cache.origin_marker_mismatch_bypasses_total` or
`cache.origin_marker_unparseable_bypasses_total` is therefore no evidence that a bucket's entries
from before the binding were not attributed this way.

The signal is a log line. When the proxy records an origin for a bucket whose directory on the
cache volume already held entries, it logs a warning once per bucket, naming the bucket. The
warning says those entries cannot be attributed to an origin, not that anything has gone wrong. On a
deployment that serves each bucket name from one origin, it is the expected result of entries
cached before the binding and needs no action.

The recorded origin lasts as long as the bucket's directory on the cache volume, across restarts.
The only reset is to remove the bucket's cached data, as
[Removing one bucket's cached data](#removing-one-buckets-cached-data) describes. The bucket then
binds to whichever origin requests it next, unless you record the origin first. Remove or write the
record only while every proxy sharing the volume is stopped. A running proxy keeps the origin it
read until it restarts, so proxies started before and after the change can bind the bucket to
different origins and serve each other's entries.

### Access points and Multi-Region Access Points

A request through an access point or Multi-Region Access Point hostname is cached under that access
point, not under the bucket behind it ([cache-key forms](CONFIGURATION.md#cache-key-forms)). The
proxy holds no AWS credentials, so it cannot look up which bucket an access point fronts. A write
through one hostname therefore does not remove copies cached through another. After a PUT or DELETE
through the bucket hostname, a copy cached through an access point is served until its TTL passes.
The same holds in reverse and between two access points.

To keep reads current, do one of these:

- Write and read each object through the same hostname.
- Give objects read through more than one hostname a short `get_ttl` and `head_ttl`, or `0s` to
  revalidate every read with S3, with a cache rule on the access point's key prefix. For example:
  `{ "pattern": "myap-123456789012-s3alias/**", "get_ttl": "0s", "head_ttl": "0s" }`.

## Tuning profiles

Cache settings for common workloads, keyed on TTL, cache size and the RAM cache. For
whole-deployment configurations (ports, TLS, logging, dashboard), see the
[example configurations](examples/README.md).

### Objects that change often

Clients see changes made outside the proxy within an hour:

```yaml
cache:
  get_ttl: "1h"
  head_ttl: "5m"
  max_cache_size: 10737418240  # 10 GiB
```

### Objects that rarely change

The cache keeps objects for a week between revalidations:

```yaml
cache:
  get_ttl: "7d"
  head_ttl: "1d"
  max_cache_size: 107374182400  # 100 GiB
```

### Fewer S3 requests

Fewer conditional requests to S3, at the cost of up to a day before a change made outside the proxy
is seen:

```yaml
cache:
  get_ttl: "1d"
  head_ttl: "1h"
  ram_cache_enabled: true
  max_ram_cache_size: 1073741824  # 1 GiB
```

### Write-heavy workload

Objects uploaded through the proxy and then read back, including multipart uploads:

```yaml
cache:
  write_cache_enabled: true
  put_ttl: "2h"
  write_cache_percent: 20.0               # The write cache's reclamation target, as a share of the cache
  write_cache_max_object_size: 536870912  # 512 MiB: the largest single PUT that is cached
  # incomplete_upload_ttl (default 1d, valid 1h to 7d) sets when abandoned uploads are removed
```

- Multipart uploads count towards the same `write_cache_percent`, which is a reclamation target,
  not an admission limit.
- Each part is checked against the remaining cache capacity as it arrives. A part that does not fit
  is forwarded unstaged, and the completed object is then not cached.
- At most 5,000 in-progress multipart entries (uploads plus uploaded parts, across every proxy
  sharing the volume) are write-cached at once. That is about 38 concurrent 1 GiB uploads at the
  AWS CLI's 8 MiB part size. Further uploads go to S3 uncached and are counted in
  `signed_put.skipped_puts_total` under `multipart_tracking_full`.
- Incomplete uploads are removed after `incomplete_upload_ttl` (default `1d`).
- A completed multipart upload serves range requests at once.

### Range reads of uploaded objects

Large objects uploaded through the proxy, then read in ranges or parts:

```yaml
cache:
  max_cache_size: 107374182400        # 100 GiB
  get_ttl: "7d"
  range_merge_gap_threshold: 2097152  # 2 MiB (default 1 MiB)
  write_cache_enabled: true
  write_cache_percent: 15.0
  ram_cache_enabled: true
  max_ram_cache_size: 2147483648      # 2 GiB
```

- A larger `range_merge_gap_threshold` makes fewer, larger S3 fetches when the proxy fills gaps in
  a partly cached range.
- With the write cache on, a GET after a multipart upload through the proxy is usually a cache hit;
  see [Write-through ordering](#write-through-ordering).
- The RAM cache serves hot ranges and parts from memory.
- GetObjectPart requests are cached as ranges, so a repeated part read makes no S3 request.

## Cache invalidation

### Automatic invalidation

The proxy invalidates a key's cached entry when:

1. S3 reports a different ETag or Last-Modified for it.
2. A PUT to the key arrives: the new upload replaces the old entry (see
   [Conflict handling](#conflict-handling)).
3. A CompleteMultipartUpload to the key completes: the completed object replaces the old entry.
4. A DELETE request for the key arrives.
5. A DeleteObjects request or a browser form POST upload names the key (see
   [Writes that name keys in the body](#writes-that-name-keys-in-the-body)).
6. S3 answers a conditional request with `200 OK`, which shows the cached data is stale.

Multipart uploads that stay idle for longer than `incomplete_upload_ttl` (default `1d`) are also
removed, with their staged parts.

**ETags are compared without their quotes.** An ETag stored bare and the same ETag on the wire in
quotes match, so the entry is not invalidated on the next read. ETags that genuinely differ still
fail the comparison, including when one side is empty, is only quotes, or has an unbalanced quote.
A multipart upload whose part ETags cannot be verified this way is left out of the cache, with the
WARN `ETag mismatch for part N ... skipping cache finalization`. The upload itself is unaffected,
since S3 has already completed it, and the next GET fetches the object from S3.

### Writes that name keys in the body

DeleteObjects (`POST /{bucket}?delete`) and a browser form POST upload (`POST /{bucket}` with
`multipart/form-data`) name their objects in the request body. The proxy reads the keys from the
body and invalidates them as it does for a DELETE or PUT. The invalidation finishes before the
response when it fits within the write's cache budget (see
[Write-through ordering](#write-through-ordering)), and in the background otherwise.

- **DeleteObjects.** The proxy reads the `<Key>` list from the request body before forwarding it.
  A body it cannot read exactly (a DOCTYPE, a CDATA section, an unknown entity, an encoding other
  than UTF-8, or malformed XML) is answered `400 MalformedXML` without being forwarded, and the
  proxy logs a warning naming the cause. AWS SDKs and the AWS CLI never send those forms. After a
  `2xx`, every requested key is invalidated except those S3 reported an `<Error>` for. When the
  result document is larger than 4 MiB or cannot be read, or S3 answers `5xx` or not at all, every
  requested key is invalidated. A `4xx` invalidates nothing.
- **Form POST.** The proxy forwards the body as it streams and reads the `key` field on the way
  past; it does not keep the file content. A `2xx`, a `3xx` redirect, a `5xx` or a failed forward
  invalidates the key, and a `4xx` does not. When the `key` field cannot be read, the proxy
  invalidates the object named in S3's `Location` header instead. Form POST uploads are not
  write-cached.

### PUTs that do not carry the object

A PUT that does not carry the object's bytes is forwarded without being write-cached, and
invalidates the key on success like any PUT. These are `?tagging`, `?acl`, `?retention`,
`?legal-hold` and `?renameObject`, CopyObject and UploadPartCopy (`x-amz-copy-source`), and appends
(`x-amz-write-offset-bytes`). A multipart upload that includes an UploadPartCopy part is not
cached. Neither is a single-part PUT whose S3 response carries no `ETag`. These are counted as
`non_object_put` and `no_etag` in
[`signed_put.skipped_puts_total`](METRICS_REFERENCE.md#skipped_puts_total).

### Headers on a write-cached object

A write-cached object is served with the headers S3 stores from the write: `Content-Type`,
`Content-Encoding`, `Content-Disposition`, `Content-Language`, `Cache-Control`, `Expires`,
`x-amz-website-redirect-location`, `x-amz-storage-class` (except `STANDARD`), every `x-amz-meta-*`,
and `x-amz-tagging-count`. A multipart upload takes them from its CreateMultipartUpload. SSE,
version-id and checksum headers come from S3's response to the write.

An object written without `Content-Type` is served from the cache as `binary/octet-stream`, as S3
serves it. A GET of a cached object that was written with tags carries `x-amz-tagging-count`, as
S3 returns it. A GET of a zero-length object is forwarded to S3 and answered with S3's headers; the
cached entry records the PUT's representation headers but is not served from the cache. Cached
reads never return `Content-MD5`: S3 does not send it on GET or HEAD, and the proxy keeps a PUT's
value only as a checksum fallback.

### Conflict handling

When a new upload starts for a key that is already cached:

**PUT**:

```text
Existing cache: object.bin (complete, 10 MiB)
Client PUT → invalidate the cached entry (metadata and ranges)
           → store the new object as range 0-N
           → mark upload_state = Complete
```

**Multipart upload**:

```text
Existing cache: object.bin (complete, 10 MiB)
Client CreateMultipartUpload → the cached object stays valid and keeps serving GETs
                               (except for an SSE-C Create, or one on a key whose rules
                               turn the write cache off, which invalidates the key like a PUT)
Client UploadPart → stage the parts as they arrive
Client CompleteMultipartUpload → the new object replaces the old cached entry
```

A new upload always replaces the old cached data.

### Write-through ordering

A write-cached PUT or CompleteMultipartUpload works like this:

1. The PUT returns when S3 confirms it. The proxy does not wait for the new object to be stored in
   the cache; that store finishes in the background.
2. Before the response returns, the proxy invalidates the key's previously cached version (for a
   DELETE, DeleteObjects or form POST, the key's cached copy). This proxy and the cache volume
   therefore stop serving the old object before the client has its `200`. That holds even when the
   new object is not cached at all: a capacity bypass, a skip, or a restart before the background
   store finished. The invalidation finishes before the response only within the write's cache
   budget, which is 3 seconds for a DeleteObjects and 1 second for every other write. Past the
   budget it finishes in the background, and until it does, this proxy fetches the key from S3
   instead of serving it from the cache. Other proxies sharing the volume can serve the old object
   until then.
3. A GET right after a PUT is served from S3 until the background store publishes the new object,
   and is a cache hit from then on. A CompleteMultipartUpload waits at most 250 ms for its cache
   store before responding. A GET after it is therefore usually a cache hit when the upload's parts
   were cached, and is served from S3 until the store publishes when the store takes longer.

**Other proxies' RAM caches.** Another proxy sharing the volume can serve the old object from its
own RAM for a short time. A RAM range hit can be up to `cache.ram_cache_verification_interval`
(default `1s`) old, and an answer from the RAM metadata cache, such as a HEAD, up to
`cache.metadata_cache.refresh_interval` (default `5s`). For keys under a cache rule with `page_widening: true`, a
widened read re-checks a page held in RAM against the shared metadata once
`cache.ram_cache_verification_interval` has passed.
[RAM cache coherency](CACHE_FRESHNESS.md#ram-tier-coherency-contract) has the details.

**Keys with `page_widening: true` can serve the older object just after an acknowledged write.**
A widened read can serve a page that was cached just before a PUT, CompleteMultipartUpload or
DELETE was acknowledged. It can do so on any proxy, including the one that took the write, until
the next consolidation cycle records the write (`cache.shared_storage.consolidation_interval`,
default `5s`). Step 2 above and the paragraph on GET and HEAD misses below describe keys without
`page_widening`.

**Which version a cached range is served as.** A cached range is served under the headers of the
version that the cache metadata on the volume lists it for. When this proxy's in-memory copy of
that metadata is older than an overwrite, the request is served from the newer metadata.

**Write order.** An older write's background store does not replace a newer write in the cache.
When a second PUT, CompleteMultipartUpload or DELETE of a key is acknowledged before the first
PUT's background store has run, on this proxy or any other sharing the volume, the first store does
not publish. It is counted as `superseded` in
[`signed_put.skipped_puts_total`](METRICS_REFERENCE.md#skipped_puts_total). The order is the order
in which the proxies receive S3's acknowledgements. For two PUTs of one key in flight at the same
time, S3 decides which is current and the proxy cannot observe that order. Do not overwrite a key
from two clients at once and rely on the cache to hold the version S3 kept.

GET and HEAD misses are ordered the same way, including `?partNumber=` GETs. A GET or HEAD can miss
the cache and send its request to S3 before a PUT, CompleteMultipartUpload or DELETE of the key is
acknowledged, on this proxy or any other sharing the volume. Such a miss does not cache what S3
returned to it, and the next request misses and fetches the current object.

This ordering rests on a small `{object}.wtok` file per key. Each acknowledged write rewrites it
before the proxy responds. Before each GET or HEAD miss goes to S3, the proxy reads the file, or
creates it when the key has none, and stores the response only if the file is unchanged when the
store runs. A miss that S3 answers with `404` or `403`, or that fails before its response reaches
the cache store, removes the file and any directories it created for it. It retries the removal for
under a second while the key's metadata lock is busy, and leaves the file in place when another
miss on the same proxy read it in the meantime and stored its response. A file left behind (the
lock stayed busy, the cache declined to store the response, or the proxy restarted) is removed
later by the ledger-repair rotation.
[During a rolling upgrade](UPGRADING.md#during-a-rolling-upgrade) covers which of these guarantees
hold before every proxy sharing the volume is upgraded.

**When the cache volume is unavailable.** The cache volume can refuse the key's metadata lock, as it
does for 20 to 60 seconds during a file system failover. The proxy then still returns S3's
response. Before it responds, it appends a record of the key to a small log on the instance's own
disk (`logging.local_state_dir`, default `/var/lib/s3-proxy`) and flushes it. It then retries the
`{object}.wtok` file in the background until it lands. A restarted proxy reads the log before it
accepts requests and resumes those retries. While that directory is writable, neither a volume
outage nor a restart during one drops a pending invalidation.

The shipped systemd unit creates the directory with `StateDirectory=s3-proxy`. Under
`ProtectSystem=strict`, a unit without that line, or without the directory in `ReadWritePaths`,
leaves it read-only. The proxy refuses a directory inside `cache_dir`. It also refuses one on any
file system other than ext2/3/4, XFS, btrfs, tmpfs, overlayfs, ZFS or f2fs: NFS, SMB, Lustre, GPFS,
FUSE, any other network or unrecognized type, and a type the proxy cannot read. The log can share
the root volume with a local-disk cache, as long as it sits outside `cache_dir`. When the directory
is refused or read-only, the proxy logs an ERROR at startup naming why, reports
`cache.write_stamp_log_memory_only` as `1`, and holds its retries in memory only.

Each proxy holds up to 128 MiB of pending keys in memory (about 500,000 keys) and the rest only in
the log, from which it reloads them as the memory queue drains. While a key's retry is pending, the
proxy that took the write does not itself add to the cache that key's older background stores,
CompleteMultipartUpload publishes, or GET and HEAD responses. Until the retry lands, the object the
write replaced or deleted can still be served. Three cases fall outside that:

- After the volume returns and before the retry lands, another proxy sharing the volume can add an
  older copy of the key to the cache. The copy can be an older write of the key, a GET or HEAD
  response the other proxy fetched before the write, or a GET response the writing proxy saved to
  the volume before the write. Proxies share the work of adding each other's saved GET responses to the cache, and the
  other proxy has no record of the pending retry. The copy is removed from the volume when the
  retry lands. A proxy that already holds the copy in its RAM cache serves GETs of it from RAM
  until `cache.ram_cache_verification_interval` (default `1s`) has passed since it last checked the
  copy against the volume. Its next GET after that, if it can read the cache volume, finds the copy
  gone and drops it from RAM. RAM pages of keys with `page_widening: true` follow the page-widening
  bound above.
- A proxy accepts writes while the volume is down, and its instance is then permanently terminated
  before its retries land. The log is lost with the instance's disk, and the object can be served
  until its TTL passes or the key's next write.
- The proxy runs in memory only, or the instance's own disk is full or read-only when the record is
  written. The proxy then logs an ERROR and retries that key from memory only, so a restart drops
  the retry and the object can be served until its TTL passes or the key's next write. If the
  memory queue is also full, the proxy that took the write keeps the key in a separate in-memory
  list of up to 16 MiB (about 50,000 keys). It forwards that key's GET and HEAD to S3 without
  caching, and retries the key once the retry queue has drained. Past that list's limit, it
  forwards every GET and HEAD to S3, and logs an ERROR, until it restarts. Other proxies have no
  record of these keys and can serve the object until the retry lands or its TTL passes. A restart
  drops the list, after which any proxy can serve the object until its TTL passes or the key's next
  write.

[Write-order stamp retries](METRICS_REFERENCE.md#write-order-stamp-retries) has the counters.

If the invalidation fails, the proxy logs a warning and still returns S3's response, because the
upload succeeded. It retries the invalidation in the background, as described above under When the
cache volume is unavailable.

At most 1,024 PUTs per proxy can wait for their background cache store at once. When the
cache volume cannot keep up with sustained PUT load, further PUTs are forwarded to S3 and succeed
as normal but are not cached. They are counted as `backlog_full` in
[`signed_put.skipped_puts_total`](METRICS_REFERENCE.md#skipped_puts_total). A GET of such an object
is served from S3 and cached on the way through. The same limit covers multipart part stores,
CreateMultipartUpload tracker writes and CompleteMultipartUpload finalizations. A completed upload
with a part that was not cached is not cached itself, and neither is one whose finalization found
no free slot.

### Manual invalidation

The proxy has no management API. To clear the cache, stop every proxy that shares the cache volume,
empty the volume, then start them again:

```bash
sudo systemctl stop s3-proxy  # On every proxy that shares the cache volume
rm -rf /cache/*  # The cache_dir from the proxy configuration
sudo systemctl start s3-proxy  # On every proxy
```

### Removing one bucket's cached data

To remove one bucket's entries without emptying the volume, stop every proxy that shares the cache
volume. Then run this with `CACHE_DIR` set to the `cache_dir` from the proxy configuration:

```bash
CACHE_DIR=/cache  # The cache_dir from the proxy configuration
BUCKET=my-bucket
cd "${CACHE_DIR:?}" && ls -d metadata/_journals  # Must list the directory; stop if it does not
grep -lF -e "\"cache_key\":\"${BUCKET:?}/" -e "\"bucket\":\"$BUCKET\"" \
  metadata/_journals/* metadata/_shard_state/staged_pending/*  # Must print no file name
rm -rf "metadata/${BUCKET:?}" "ranges/$BUCKET" "metadata/_write_ledger/$BUCKET"
rm -rf metadata/_shard_state/*/"${BUCKET:?}"
```

The `ls` confirms that the current directory is `cache_dir`. From any other directory, the `grep`
matches nothing and the `rm` lines run against the wrong place. The `grep` confirms that no
metadata update for the bucket is still waiting to be applied, since one would recreate an entry
for data you have removed. It reports an error for a pattern that matches no file, which is
harmless; a printed file name is not. If it lists a file, start the proxies, wait 10 minutes, stop
them and run it again.

The size figures still count the removed bytes, so eviction can start early by up to that amount
until the daily validation scan. To recount at startup instead, also delete
`size_tracking/validation.json` and `size_tracking/schedule.json` before starting the proxies, as
[Manual recovery](EVICTION.md#manual-recovery-for-an-operator-who-cannot-wait) describes.

The bucket now has no recorded origin, so the first cacheable request after the restart claims it.
To choose the origin instead, write the record before starting the proxies:

```bash
mkdir -p "metadata/${BUCKET:?}"
printf '{"schema":1,"origin":"%s"}' store-a.example.com > "metadata/$BUCKET/_origin.json.tmp"
ln "metadata/$BUCKET/_origin.json.tmp" "metadata/$BUCKET/_origin.json" && rm "metadata/$BUCKET/_origin.json.tmp"
```

The origin is the hostname clients use for that store, in lowercase and without a port. For Amazon
S3 it is the partition domain, `amazonaws.com`. `ln` refuses to replace a record that already
exists. A record the proxy cannot read makes every request for the bucket bypass the cache, counted
in `cache.origin_marker_unparseable_bypasses_total`.

## Compression

The proxy stores every cached range as an LZ4 frame with a checksum, on disk and in RAM. It
compresses the payload while `compression.enabled` is `true`, unless the payload is smaller than
`compression.threshold` or the key's extension marks the content as already compressed. A cache
rule's `compression_enabled` replaces `compression.enabled`, and a rule set to `true` also
compresses a denylisted extension. [Compression](COMPRESSION.md) covers the decision, the built-in
denylist, the integrity check and the settings.

## Cache hit rate

The formula and the fields behind it are in the [metrics reference](METRICS_REFERENCE.md#cache).
The denominator counts every GET and HEAD through the proxy, including list-object GETs and
responses the proxy cannot cache. The figure is therefore the share of all GET and HEAD traffic
served from cache, not the share of cacheable object reads alone.

A workload of unique, one-time reads has no hit rate to improve. Otherwise, a larger cache or longer
TTLs raise it. Partial cache hits still save S3 transfer; see
[Observing range merge efficiency](CACHE_READ_PATHS.md#observing-range-merge-efficiency).

### The RAM cache hit rate has a ceiling below 100%

`cache.ram_cache_hit_rate_percent` is not comparable to the disk figure above. For one common
client shape it cannot approach it, however warm the RAM cache is.

**The RAM cache is keyed by the cached extent, so a read of part of an extent misses once on every
request.** A ranged GET consults the RAM cache twice, under two keys: once for the byte range the
client asked for, and once for the cached extent the merge path resolves that range to. Promotion
stores only the extent. A client reading a slice of a larger extent therefore takes one miss and
one hit on every request, including the thousandth request for bytes already in RAM. The rate for N
such reads is `(N-1)/(2N)`, which converges on 50%: 15 identical 4 KiB reads inside one extent give
46.67%. Reads at exactly the extent boundaries give `(N-1)/N`, which approaches 100%. This holds
for clients that sign `Range` (the AWS CLI and the SDKs) and for clients that do not (presigned URLs
that do not sign `range`).

A reading near 47% on a workload of sub-range reads is therefore the ceiling, and a larger cache
will not move it. The client's request shape moves it.

**Size the RAM cache by the extent, not by the read.** A small read promotes the whole extent, so a
4 KiB read of a 32 MiB extent adds about 32 MiB to `cache.ram_cache_size`: one entry, about 8,000
times the bytes requested. A 2 GiB RAM cache holds about 64 such extents whatever the read sizes
are, and many small reads spread over many large objects fill it with far more bytes than they
request. With page widening on for a key, the RAM cache holds the page instead of the extents it
was assembled from.

**The figure is cumulative for the life of the process**, so it cannot show a recent change and
cannot be attributed to one access pattern. A window that holds both shapes above reads somewhere
between their two rates: a number that belongs to neither and looks the same as that figure on a
single-shape workload. To attribute a rate to a pattern, read `cache.ram_cache_hits` and
`cache.ram_cache_misses` before and after it, and compute the rate from the differences.

**`0.0` is ambiguous.** A RAM cache with traffic and no hits and one with no lookups at all both
publish `0.0`. Read the two counters: both zero means there was no traffic to judge. A third cause
is a `ram_cache_eligible: false` rule on the prefix, which has no counter of its own; the rule file
is the only place to check.

A lookup that finds the key counts as a hit even when the caller then rejects the entry as a stale
version, so the rate counts successful lookups, not serves. An `x-cache: HIT` header on a response
does not say which cache served it, because the disk cache sets the same header. The
[RAM cache fields](METRICS_REFERENCE.md#ram-tier) in the metrics reference give the detail for each
of these.

There is no HEAD-scoped RAM hit rate; `cache.ram_cache_hit_rate_percent` covers every request shape
together. For HEADs, compute the rate over a window from the per-proxy counters
`cache.metadata_cache_head_hits` and `cache.metadata_cache_head_misses`.
`cache.metadata_cache_head_disk_hits` says how many of those misses were found on the cache volume
instead of costing an S3 request. See
[No HEAD-scoped RAM hit rate exists](METRICS_REFERENCE.md#no-head-scoped-ram-hit-rate-exists).

## Limitations

### What the proxy does not do

The proxy holds no AWS credentials and cannot sign requests, so it never sends S3 a request of its
own. It does not pre-warm the cache, refresh entries in the background, or revalidate an entry
before a client asks for it. A change made outside the proxy is seen when the entry's TTL passes.
To warm the cache, have clients read the objects. For fresher data, use shorter TTLs.

### Memory a cached read may use

A cached read assembles at most 8 MiB of uncompressed bytes in memory at once. No configuration
field changes it, so the limit is the same on every deployment. It bounds the memory a cached serve
costs by the limit, not by the size of the object being read.

Above it, a ranged read of bytes inside one cached extent, and a whole object requested with no
`Range` header, are streamed from their cached extents instead of loaded. A read spanning several
adjacent extents is streamed from each in turn. A read that is only partly cached, or whose cached
data cannot be read, is fetched again from S3 in full and stored as one range. That costs hit ratio
once and then heals: the repeat is a cache hit. Counters under `inflight_memory`, such as
`inflight_memory.buffered_serve_degraded_forward_partial_total`, say which reason applied, and each
forward or heal also logs a warning naming the range. A counter that keeps climbing for the same
range means the refetched range is not being stored.

A GET with no `Range` header is answered `200` with no `content-range`, whatever the object's size
and however much of it is cached.

[How much memory a cached serve may use](CACHE_READ_PATHS.md#how-much-memory-a-cached-serve-may-use)
covers the mechanism, the full set of counters, and the one cached read the limit does not govern
(page widening, which holds a page instead).

### Capacity limits

**Write cache**:

- The write cache is `write_cache_percent` (default 10%) of the total cache, shared by PUTs and
  multipart uploads. It is a reclamation target, not an admission limit: staged bytes above it are
  reclaimed in the background.
- A PUT or upload part that would take the cache past `max_cache_size` is not cached; the upload
  continues to S3 normally.

**Read cache**:

- A GET that misses and would take the cache past `max_cache_size` is served from S3 and not
  stored. The response is byte-for-byte what it would have been, with the same status and headers;
  only the copy is skipped.
- This is the same soft stop as the write cache's, against the same figure. The proxy decides once,
  at the start of each fill, so a fill already streaming completes and is cached.
- Caching resumes on its own once eviction brings the total back below the limit.
  [`cache.read_fills_declined_max_cache_size_total`](METRICS_REFERENCE.md#the-max_cache_size-soft-stop-on-fills)
  counts the declined fills, and one log line per minute names the figures while it is happening.
- The [free-space floor](CONFIGURATION.md#free-space-floor-and-the-danger-band) governs free space
  separately.

**Single objects**:

- A PUT larger than `write_cache_max_object_size` (default 256 MiB, `268435456`) is not cached.
- A multipart upload is cached only when every part was staged. To cache larger multipart uploads,
  raise `max_cache_size`.
- An object that is not cached is still forwarded to S3.

## See also

- [Cache freshness](CACHE_FRESHNESS.md): TTLs, revalidation and conditional requests
- [Cache read paths](CACHE_READ_PATHS.md): how each read is served
- [Cache internals](CACHE_INTERNALS.md): the on-disk and in-memory layout
- [Eviction](EVICTION.md): how the proxy frees space
- [Fleets on one cache volume](SHARED_STORAGE.md): running several proxies on one volume
- [Configuration reference](CONFIGURATION.md): every cache field
- [Compression](COMPRESSION.md): the compression decision and the denylist
- [Multipart uploads](MULTIPART_UPLOAD.md): how multipart uploads are cached
- [Metrics reference](METRICS_REFERENCE.md): the `cache` and `page_cache` counters
- [Connection pooling](CONNECTION_POOLING.md): upstream connections to S3
