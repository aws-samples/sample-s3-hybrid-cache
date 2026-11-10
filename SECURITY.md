# Security

This document is written for someone reviewing Hybrid Cache for Amazon S3 before deploying it. It states the
security model the software assumes, the risks that model leaves with you, and the checks you can run
yourself against this repository.


**Applies to release 3.0.4. Last reviewed 2026-11-10.**

Confirm the release you are reading with `grep '^version' Cargo.toml`. If it does not match the release
named above, this document was written against a different release than the code in front of you.

If you are looking for behavior detail rather than posture,
[Security considerations](docs/ARCHITECTURE.md#security-considerations) is the fuller treatment and this document links into it rather than
restating it.

## Reporting a vulnerability

If you discover a potential security issue in this project, please notify AWS Security through the
[AWS vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/). Please
do **not** create a public GitHub issue for a suspected vulnerability.

For anything that is not a vulnerability, a GitHub issue is the right place.

---

## The security model

Four positions govern everything else. If any of them does not hold in your environment, do not
deploy this software there; no configuration change makes it hold.

**1. The proxy does not authenticate clients or authorize requests.** It holds no AWS credentials and
cannot validate them. Amazon S3 remains the authorization boundary, but only for requests that reach
S3, and a cache hit does not. Network controls are your access boundary, and they also decide who can
use the proxy's capacity. Beyond two limits, `server.max_concurrent_requests` and
`server.max_inflight_buffer_bytes`, the proxy is not hardened against a hostile client that can reach
its listeners. Both limits apply per proxy and are shared by every client, and the second has no
effect until you set it ([In-flight memory ceiling](docs/CONFIGURATION.md#in-flight-memory-ceiling)).
Allow only clients you trust to reach the data-plane listeners.

**2. The cache volume is your boundary.** Anyone who can read the cache volume can read cached object
data directly. Anyone who can write to it can influence what the proxy serves. The proxy cannot tell
clients apart, and cache hits are served without consulting S3, so a cache shared by clients that
must not see each other's data is not a supported configuration. Run a separate proxy and cache
volume per trust domain.

**3. The proxy caches the requests it can read.** Clients send `http://` requests to the proxy,
and the proxy opens verified TLS to S3 on the upstream side. Point clients at the TLS proxy listener: set
`HTTP_PROXY=https://<proxy-host>:3129` and `--endpoint-url http://s3.<region>.amazonaws.com`. The
client hop is encrypted and responses are still cached, on-premises and across Regions. Where the
network segment between clients and proxies is secure and cleartext on it is acceptable, a DNS zone
that resolves S3's hostnames to the proxies is a simpler alternative: clients need no proxy setting
and connect on port 80 (see accepted risk 5).

**4. Plaintext and unvalidated-TLS upstream transports are opt-in development features.** The default
upstream is verified TLS on port 443, with the certificate checked against the host the request
named; for a bucket whose name contains a dot, addressed virtual-hosted, it is checked against the S3
endpoint name without the bucket (see [connection pooling](docs/CONNECTION_POOLING.md)). The overrides that waive that exist for local development
and testing on a secure network segment, and the proxy logs a warning at startup naming each one.

### Who the proxy trusts

The proxy trusts four parties, each for what its paragraph names.

**Clients that can reach a listener.** The proxy serves any client that can connect to a data-plane
listener. That client can read any cached object (risk 1), can have the proxy connect to any
destination the destination policy allows (risk 8), and can use up the shared limits in position 1.
The proxy checks a request only as far as it needs to route and cache it.

**Configuration.** The proxy acts on `config.yaml`, its environment variables and command-line flags,
`cache_rules.json`, and the TLS proxy listener's certificate and key as written. They decide which
destinations clients can reach, which upstream transports the proxy uses, and what it caches and for
how long. Anyone who can change them controls the proxy, so give write access only to the people who
run it. A finding that needs an attacker to write one of them is outside this model. Each entry in
`connection_pool.endpoint_overrides`, `connection_pool.upstream_overrides` and
`server.tls.connect_allowlist` widens what clients can reach, so list only hosts and addresses you
control. `cache_rules.json` sits in `cache_dir`, so position 2 covers it too.

**The cache volume and every proxy that mounts it.** Position 2 and risk 2 cover the volume. Every
proxy that mounts it is trusted equally: each acts on what the others wrote without authenticating
it, so mount the volume only on proxies you run. Run proxies of different releases on one volume only
as [Upgrading](docs/UPGRADING.md) describes for each release.

**The origin.** The proxy trusts the origin's responses: the bytes, headers and ETag of a GET or HEAD,
and the status of a write. On the default transport, the certificate check in position 4 is what
identifies the origin. The write cache keeps an uploaded object only after the origin answers the
upload with a success status, so an upload the origin refuses is never cached.
[Trust and integrity model](docs/ARCHITECTURE.md#trust-and-integrity-model) lists what the proxy
verifies itself.

---

## Accepted risks

These are known, unaddressed in code, and left to you to mitigate at deployment time.

### 1. Any client that can reach the proxy can read any cached object

With a TTL above zero, cache hits are served without contacting S3, so a client with no credentials
and a client whose S3 access was revoked both receive cached data until the entry expires. Different
users share the same cached responses; there is no per-user isolation. Conditional requests follow
the same rule: `cache.evaluate_conditions_from_cache` defaults to `true`, so within the TTL a request
whose `If-Match` names the cached ETag is answered from cache.

In 3.0.1, and possibly earlier releases, an entry stayed fresh for `get_ttl` from when consolidation
recorded it rather than from when it was fetched. That is up to one `consolidation_interval` (`5s` by
default) longer, and up to 5 minutes longer after a consolidation backlog or a restart. With page
widening on, a page that consolidation had not yet recorded could also be served past `get_ttl`.
3.0.2 counts `get_ttl` from the fetch on both paths.

This is the model of any shared cache, including a CDN edge cache or a shared copy of downloaded
files on an NFS export. The proxy does not weaken S3's security. It requires you to control access to
the cache.

**Mitigations.** Restrict network access with security groups, firewalls, or segmentation. For
prefixes where S3 must evaluate IAM on every read, set `get_ttl: "0s"`, which forces revalidation
against S3 on each request and returns S3's own 403 when the caller has lost access. See
[Cache freshness](docs/CACHE_FRESHNESS.md).

### 2. Write access to the cache volume can influence what the proxy serves

An attacker who can write to the cache volume could tamper with cached data or metadata. The
proxy validates paths, rejects symlinks, and creates files exclusively, which raises the cost of
several specific attacks, but the boundary remains the file system permissions you set.

**Mitigations.** Restrict the cache volume to the proxies. Do not share it with general-purpose
workloads or across tenants.

### 3. Encryption at rest comes from the cache's storage

The proxy stores object data as the client received it, compressed where the content compresses and
byte-for-byte where it does not, and adds no encryption of its own. Object keys, sizes, and access patterns are visible in cache metadata.

**Mitigations.** Encrypt the storage the cache runs on, at the layer that storage provides:
file-system encryption at rest for a shared file system (Amazon FSx, Amazon EFS), volume encryption
for a block volume (Amazon EBS), or host-level disk encryption for a local disk (dm-crypt, usually
with LUKS), which encrypts the block device under the file system. The proxy caches data the
requesting client was already authorized to read.

### 4. A presigned URL's cached object outlives the URL's expiry

The proxy refuses an expired SigV4 presigned URL with `403` before any cache lookup. It never
verifies a signature, and it does not check the expiry of a SigV2 presigned URL. Within the TTL, the
object a presigned URL fetched is served to any request for that object, as risk 1 describes, so
access can extend beyond what the signer intended.
[Presigned URL support](docs/CACHE_FRESHNESS.md#presigned-url-support) lists every check the proxy
makes.

**Mitigations.** Set `get_ttl: "0s"` and `head_ttl: "0s"` for prefixes accessed by presigned URL,
which sends every GET and HEAD to S3 for revalidation
([Zero TTL revalidation](docs/CACHING.md#zero-ttl-revalidation)).

### 5. A client hop on port 80 is cleartext

Requests to the HTTP listener and the forward proxy are not encrypted. From a client on the proxy's
own host they stay on loopback. Across a network, SigV4 authenticates a request but provides no
confidentiality, so an observer on that network segment captures the bucket and object
key, the `Authorization` header including the caller's access key ID, and object payloads in both
directions. The secret access key is never transmitted and cannot be recovered from a captured
request, but a captured request can be replayed against S3 until its timestamp ages out, and a
captured presigned URL can be replayed for its remaining validity.

The documented examples use `--endpoint-url http://s3.<region>.amazonaws.com` because the proxy can
cache only what it can read. Changing the scheme to `https://` makes the SDK open a `CONNECT` tunnel
that the proxy cannot decrypt, so nothing is cached. If a scanner recommends switching the scheme,
that recommendation is wrong for this software; use one of the encrypted alternatives below instead.

**Mitigations.** Point clients on other hosts at the TLS proxy listener with
`HTTP_PROXY=https://<proxy-host>:3129`, or terminate TLS at a load balancer that re-encrypts to that
listener; both preserve caching. A client on the proxy's own host can use the HTTP listener, because
the hop stays on loopback. Port 80 across a network, including DNS routing of S3's hostnames to the
proxies, suits only a network segment that is secure and where cleartext is acceptable.
[What a cleartext hop exposes](docs/ARCHITECTURE.md#what-a-cleartext-hop-exposes) has a per-hop table showing which hops are cleartext under each deployment pattern.

The TLS proxy listener protects against passive observation for every client, but only the AWS CRT
transfer client verifies the proxy's certificate. botocore-based clients (`aws s3api`, `aws s3` with
the classic transfer client, boto3) do not, so an attacker who can impersonate the proxy is not
stopped by TLS on that hop. See the
[per-client table](docs/GETTING_STARTED.md#configuring-clients-to-trust-the-certificate).

### 6. The example configuration publishes the monitoring listeners on all interfaces

`config/config.example.yaml` sets `bind_address` to `0.0.0.0` in the `health`, `metrics` and
`dashboard` sections, so copying it verbatim publishes all three unauthenticated listeners on every
interface. The code default for each is `127.0.0.1`; the example differs deliberately, so that a load
balancer health check, a remote metrics scraper and the dashboard work without further edits.

The health endpoint reports status. The metrics endpoint and the dashboard expose cache statistics,
bucket names and system information, and the dashboard adds a log viewer. None of them offers write
operations or credentials. Each answers a request whatever its `Host` header names, so a web page
open in a browser that can reach the listener, on the proxy host or through an SSH tunnel's local
port, can read it by DNS rebinding, including when the listener is on loopback.

**Mitigations.** Set `bind_address: "127.0.0.1"` on any listener no other host needs, and use an SSH
tunnel for occasional remote access. Restrict any port left on `0.0.0.0` to its known sources with a
security group or firewall. Keep a tunnel open only while you use it, and do not browse the web from
the proxy host. See [Dashboard](docs/DASHBOARD.md) § Security Considerations.

### 7. Two upstream transports for the same host share cache entries

Cache entries are bound to the origin that populated them, identified by hostname. The identity does
not include the port. If you configure two transports for the same host on different ports, for
example TLS with certificate validation on one port and without it on another, both are treated as
one origin and share cached data. Data fetched over the connection without certificate validation can
then be served to a request that used the validated port.

Relatedly, AWS GovCloud endpoints share the `amazonaws.com` suffix with the standard partition while
having a separate bucket namespace, so a bucket name present in both partitions would be treated as
one origin.

Only Amazon S3 endpoints share an AWS partition's identity. A request whose `Host` is under an AWS
partition domain but is not an S3 endpoint whose cache key names the bucket it serves, such as an API
Gateway or load balancer hostname, is forwarded uncached and never claims a bucket, so it cannot read
or write another bucket's cached entries. In forward-proxy mode the cache key comes from the request
target while S3 routes on the `Host` header, so a request whose `Host` header names a different host
from its request target, or that carries more than one `Host` header, is refused with `400` rather
than forwarded.

**Mitigations.** Both cases require you to configure the proxy against two origins that share a
hostname or suffix. Use a separate proxy per origin where the distinction matters. Note that
protection-waiving upstream transports are development features and the proxy warns about each one at
startup.

### 8. The HTTP forwarding paths check destinations only when you set an allowlist

The destination policy refuses the address ranges that
[Destination policy](docs/ARCHITECTURE.md#destination-policy-ssrf-protection) lists, and no others.
They include loopback, private and link-local addresses and the instance metadata service address.
It always applies to `CONNECT` tunnels and to the port 443 passthrough. On the HTTP listener, the
forward proxy and the TLS proxy listener's non-`CONNECT` requests, it applies only when
`server.tls.connect_allowlist` is set, and those paths then connect only to addresses the policy
accepts. Without an allowlist, a client can have the proxy forward a request to a private or
link-local address. Without an allowlist the proxy cannot tell an intended
origin on a private address, such as an S3-compatible store on your network, from an unintended one.

The default upstream transport still limits what such a request reaches. It is verified TLS to
port 443, whatever port the request names (position 4), so only a service that presents a
certificate valid for the requested name can answer it. The instance metadata service serves plain
HTTP, so it cannot. A `connection_pool.upstream_overrides` entry that waives a protection lifts this
limit for the requests it matches, and an entry for a DNS name matches every subdomain of that name.

**Mitigations.** Set `server.tls.connect_allowlist` to the hosts the proxy serves; it turns the check
on for the HTTP paths as well. List only names and addresses you control in `upstream_overrides`.
Restrict which clients can reach the proxy, as for risk 1. See
[Architecture](docs/ARCHITECTURE.md#destination-policy-ssrf-protection).

---

## Cache integrity

A cache that serves the wrong bytes gives a client data that does not belong to the object version
or byte range its response headers name. This section describes how that could happen, what guards
against it, what the guards do not cover, which releases were affected, and what to do.

### How a client could receive the wrong bytes

The proxy stores each cached range as a file on the cache volume, at a path that names the object
and the range but not the object version. The object's ETag and headers are recorded in its cached
metadata. When an object is overwritten in place by a client that does not go through the proxies,
two versions of one key exist, and a range file of one version can end up listed under the other
version's ETag. A read then returns one version's bytes under the other version's headers.

Every known case needs an object overwritten in place outside the proxy, with two exceptions
(see [Affected releases](#affected-releases)). One is an error reply that a 2.x release cached as
object data. The other is a multipart upload that sent one part number twice at once with different
bodies. Most
cases also need concurrent fills of one range, a rolling upgrade, or a cache volume that a 2.x
release wrote to. A deployment whose objects are each written once is exposed to none of the others.

### What guards against it

- **File identity.** When the proxy caches a range, it records the range file's length and
  modification time, and every read compares them with the file it opens before sending anything
  from it. A mismatch is refused, counted in
  `atomic_metadata.range_file_identity_mismatch_detected`, and the range is fetched from S3 and
  cached again. Two files of equal length whose recorded modification times are within 1 ms of
  each other have the same identity.
- **No identity, no serve.** A range whose cached metadata or pending journal entry records no file
  identity is refused and fetched from S3 again, counted in
  `atomic_metadata.unverified_ranges_refused`. Every range a 2.x release cached is such a range.
  Consolidation discards journal entries that list a range but record no file identity, instead of
  listing them. It deletes
  the range file the entry wrote if the file is already 10 minutes old when the entry is discarded
  and no cached metadata or other pending entry names it, counted in
  `atomic_metadata.retired_fill_files_unlinked`. A younger file is left to
  [orphan recovery](docs/SHARED_STORAGE.md#orphan-recovery). A fill whose file identity cannot be
  read is not cached.
- **One temporary file per fill.** Each fill writes its own temporary file, created only if no file
  of that name exists. Under the key's metadata lock, the fill publishes that file only if it is
  still the file the fill wrote.
- **Versions compared.** A fill whose ETag differs from the cached metadata's is dropped instead of
  published, and consolidation never lists a fill of another version. A HEAD that finds the object
  changed removes the old version's ranges and part layout. Consolidation keeps a listing over a
  range file only when the file is the one the listing names, so a revalidation that S3 answers
  `304` renews only a listing that matches its file.
- **Objects with no cached metadata.** A fill for an object with no cached metadata leaves a range
  file already at its path in place if that file was modified in the last 10 minutes, and discards
  its own copy. The client is still answered from S3. A file that recent may come from another
  fill, of the same version or another, that consolidation has not yet listed, and replacing it
  could leave this fill's bytes listed under that fill's ETag. The fill replaces a file that is at
  least 10 minutes old, or that was last modified before the object's write-order file (`.wtok`)
  was last written ([Directory hierarchy](docs/CACHE_INTERNALS.md#directory-hierarchy) says what
  writes it). The age compares the file's
  modification time, set by the file server, with the proxy's clock.
- **The RAM cache.** A range copied into the RAM cache is served only while the cache volume's
  metadata still lists a range covering it, under the same ETag.
- **Part layouts.** A part's layout is recorded only when the part's ETag matches the cached
  object's, under the key's metadata lock, and is marked as checked. A layout without the mark,
  which every release before 3.0.1 wrote, is discarded on its first read and each of its parts is
  fetched from S3 once. Each discard logs one `info` line, `Cleared a part layout recorded before
  3.0.1`, and no counter records it.
- **Staged parts.** CompleteMultipartUpload checks that each part file it moves into place is the
  file the part's record was written for, and caches nothing for the upload when one is not. S3
  still completes the upload. A part staged by a release before 3.0.2 records nothing to check
  against and is not checked.
- **Error replies.** A range with no file identity whose bytes are an S3 XML error reply is refused
  whatever `cache.trust_unverified_ranges` is set to. Cached metadata with neither an ETag nor a
  `Last-Modified`, such as metadata built from an error reply, is never served. When it lists a
  range with no file identity, the first read removes it, so the object is cached again.

### What the guards do not cover

- **`cache.trust_unverified_ranges: true` where objects are overwritten in place outside the
  proxy.** Ranges and pending journal entries with no file identity are then served and consolidated
  unchecked, as 3.0.0 did, including another version's bytes and non-XML error bodies that a 2.x
  release cached. Unmarked part layouts are kept. No time limit ends such a serve: while the cached
  metadata names the version S3 holds now, every revalidation gets `304` and renews the entry.
- **A 2.x proxy on the same cache volume.** A 2.x proxy compares no versions when it fills,
  consolidates or reads, so it keeps creating and serving listings of another version's bytes. It
  also locks different files from 3.x proxies, so their metadata writes are not ordered against each
  other. 3.0.1 proxies refuse the ranges it caches, so the fleet keeps fetching them from S3 until
  it stops.
- **A 3.0.0 proxy on the same cache volume.** A 3.0.0 proxy keeps the 3.0.0 behavior listed under
  [Affected releases](#affected-releases). In particular, for an object with no cached metadata it
  can replace a range file that a 3.0.1 proxy has just written, so that case stays open until every
  proxy runs 3.0.1.
- **Origins that send no ETag.** The version checks compare ETags, so they cannot tell apart two
  versions of an object that an S3-compatible origin served without an ETag. Amazon S3 always sends
  one.
- **Proxy clocks ahead of the file server.** A proxy whose clock runs 10 minutes or more ahead of
  the cache volume's file server sees every range file as at least 10 minutes old. Its fills for
  objects with no cached metadata then replace any range file at their path, and nothing reports
  this.

### Damaged range files

Damage to the storage under the cache volume, such as a flipped bit, is outside the version guards,
and keeping that storage undamaged is your responsibility. Range files are LZ4 frames with a content
checksum, which the proxy reads after a frame's last block.

- A buffered response decodes the whole file before sending anything, so it never sends damaged
  bytes.
- A streamed response that runs to the end of a cached range holds its last chunk until the checksum
  has been read, and ends short if the check fails, so the client receives an incomplete response.
- A streamed response that ends before the end of its cached range can complete with damaged bytes,
  because the checksum comes after its last byte. A small range read from a large cached range is
  the common case.

The next read that reaches the checksum detects the damage, counts it in
`atomic_metadata.range_file_corruption_detected`, and removes the range so it is cached again. A
count that keeps rising means the cache volume is returning damaged bytes; check the storage.

### Affected releases

The guards above close each case in 3.0.1, or in 3.0.2 where the case says so, except as described
under [What the guards do not cover](#what-the-guards-do-not-cover) and
[Damaged range files](#damaged-range-files).

- **Concurrent fills sharing one temporary file**: 3.0.0, between fills in one proxy, for example
  concurrent `?partNumber=` GETs of one part. Public 1.6.7 to 2.8.4 shared one temporary file
  between every proxy on the cache volume.
- **A fill replacing a range file that another proxy had just written, for an object with no cached
  metadata**: 3.0.0.
- **Part layouts recorded across versions**: public 1.6.7 to 3.0.0, including public 2.8.1 and
  public 2.8.2. Before 3.0.0 the other version's part bytes could be served as well. From public
  2.2.0 to 2.8.4, a conditional `?partNumber=` GET was also answered from the cache without S3
  evaluating its condition.
- **One part number uploaded twice at once with different bodies**: public 3.0.0 and 3.0.1; earlier
  releases were not examined. When the two bodies had the same compressed length and their part
  files recorded modification times within 1 ms of each other, CompleteMultipartUpload could
  publish one body's bytes under the other body's record. On ext4 with 10 ms timestamp ticks,
  writes up to one tick apart can record identical times. Where S3 kept the recorded body, every later revalidation passed.
  No known transfer manager sends one part number twice at once. 3.0.2 closes it.
- **Journal entries with no file identity consolidated over another version's file**: 3.0.0.
  Every 2.x release writes such entries, so the case could arise during a rolling upgrade from 2.x
  or until the entries 2.x left on the cache volume were consolidated. 3.0.0 also writes one when
  it cannot read the identity of a range file it has just written.
- **Ranges and pending journal entries with no file identity served unchecked**: 3.0.0. Public 1.6.7
  to 2.8.4 compared no versions when filling, consolidating or reading, so after an overwrite
  outside the proxy a single proxy could cache another version's bytes under the cached ETag and
  serve them.
- **RAM copies served after the cache volume's copy was corrected**: 3.0.0, for up to
  `cache.metadata_cache.refresh_interval` or, on page-widened reads, until `get_ttl` passed. Public
  1.6.7 to 2.8.4 compared no versions in the RAM cache at all, which 3.0.0 fixed.
- **A whole-object RAM copy answering another object's range**: public 1.6.7 to 3.0.0, with the
  RAM cache enabled. A client that uploaded an LZ4 frame as `{key}:range:{start}:{end}` and read it
  back through one proxy made that proxy answer those bytes of object `{key}` with the frame's
  decoded content. Public 1.6.7 to 2.8.4 kept the entry until RAM eviction. 3.0.0 checked it against
  the object's ETag within `cache.ram_cache_verification_interval`, so only an upload of the exact
  bytes of an LZ4 `{key}` lasted until eviction. From 3.0.1 the `s3-proxy` binary keeps only
  ranges in the RAM cache. Code that called the `s3_proxy` library crate's whole-object methods
  could still store a whole object there through 3.0.1, and 3.0.2 removes those methods.
- **A `304` renewing a wrong entry**: every public release from 1.6.7 to 3.0.0. Such an entry lasted
  until eviction, a write through the proxy or another overwrite. Before 3.0.0 the default `get_ttl`
  was about 10 years, so a wrong entry was rarely revalidated.
- **Error replies cached as object data**: public 1.9.7 to 2.8.4 could cache an error reply to a
  refetch, such as an S3 `403` after a range file was evicted, as range data and serve it as object
  bytes. Where eviction had removed the object's whole cached metadata, a client that sent GET
  without a HEAD first could receive the reply as the entire object, with status `200` and type
  `application/xml`. 3.0.0 refused S3 XML error replies left on the cache volume and served other
  error bodies, such as an intermediary's HTML error page.
- **Damaged range files streamed as complete responses**: public 1.8.2 to 3.0.0.

### What to do

- Upgrade a fleet from 2.x in one pass: stop every proxy on the cache volume, replace the binary on
  each, then start them. Upgrade from 3.0.0 the same way where objects are overwritten in place. See
  [Upgrading to 3.0.1](docs/UPGRADING.md#upgrading-to-301).
- Leave `cache.trust_unverified_ranges` at `false` unless every object is written once and never
  overwritten in place outside the proxy.
- After upgrading a cache volume that a 2.x release wrote to, watch
  `atomic_metadata.unverified_ranges_refused`. It rises while the ranges 2.x cached are fetched
  again, then stays flat. A count that keeps rising most likely means a 2.x proxy is still writing
  to the cache volume.
- Keep each proxy's clock within 10 minutes of the cache volume's file server, for example with NTP.
- Watch `atomic_metadata.range_file_corruption_detected` for damaged storage.

---

## What you can verify yourself

Every check here runs against this repository with public tooling. Figures that change between
releases are given as commands rather than numbers, so what you read is what your checkout contains.

**Advisories against the pinned dependency tree.** `Cargo.lock` is committed, so this checks the exact
versions a release builds with.

```bash
cargo install cargo-audit --locked --version 0.22.2   # if not already installed
cargo audit
```

The pin is the release this review ran. When you update it, move it to a newer release you have tested.

Advisories that are deferred rather than fixed are listed in `.cargo/audit.toml`, each with its
reasoning inline. That file is the authoritative list, so read it as part of your review rather than
taking the entries below as the current set; the policy is that every entry names why it is deferred
and what would resolve it, and that entries are removed once they no longer apply rather than left as
permanent waivers. One entry is present as this document was last reviewed:

- `RUSTSEC-2026-0097` (`rand` 0.8): reached only through a development dependency used by
  property-test macros, never linked into the release binary.

It is an `unsound` advisory rather than a vulnerability, so `cargo audit` exits zero whether or not
it is ignored; removing the entry changes what the command prints but not whether it passes. At this
release the command reports no vulnerabilities and three warnings, which likewise do not fail it.
They are `unsafe-libyaml` (RUSTSEC-2024-0449, unmaintained), reached through the YAML configuration
parser, `pin-utils` (RUSTSEC-2026-0441, unmaintained), reached through `hyper` and `futures-util`,
and a yanked `chacha20` release, reached through the DNS resolver. None is a vulnerability
advisory.

**Lints, promoted to errors.** This is a release gate, so it should be clean.

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

**The `unsafe` inventory.** Small. Every block is an operating-system call (privilege and
process-liveness checks, a socket option, the file-creation mask, file-system and process-memory
statistics) or the standard uninitialized-read-buffer idiom. None takes untrusted input.

```bash
grep -rn 'unsafe {'    src/ --include='*.rs'   # count and read each
grep -rn 'unsafe impl' src/ --include='*.rs'   # expected: none
grep -rn 'unsafe fn'   src/ --include='*.rs'   # expected: none
```

**Suppressed dead-code warnings.** Each carries a comment explaining why the item is kept.

```bash
grep -rn 'allow(dead_code)' src/
```

**The toolchain.** `rust-toolchain.toml` pins the build toolchain so a build here matches the build
that was linted and tested. `Cargo.toml` declares the minimum supported Rust version separately.

```bash
grep channel rust-toolchain.toml
grep rust-version Cargo.toml
```

---

## Dependencies and supply chain

- `Cargo.lock` is committed and shipped, so release builds are reproducible.
- `rust-toolchain.toml` pins the build toolchain. It is bumped deliberately, with the full check
  suite re-run under the new version.
- `Cargo.toml` declares the minimum supported Rust version, which is lower than the build pin.
- `THIRD_PARTY_LICENSES` ships with the source and lists every dependency's license.
- Dependency and toolchain bumps are recorded in `CHANGELOG.md`, security-relevant ones with their
  advisory identifier.

## Reviewing scanner findings

Automated reviewers raise the same classes of finding on every run against this repository. They
include AI code reviewers, AI content-review rubrics, rule-based tools such as Semgrep and CodeQL,
secret scanners such as gitleaks, and `cargo audit`. Most describe behavior this document already
sets out. The tables below map each class to that behavior.

**How to use the tables.** Match a finding to a row by the behavior it describes, not by its title
or identifier, which differ between tools and between runs. The verdict is this software's position
on that behavior. The source column names where `src/` decides it, so you can check the verdict
against the code. The setting column links to where each field is documented, and the
[Configuration reference](docs/CONFIGURATION.md) owns the defaults. The last column is what stays
with you once the setting is chosen. For a finding that matches no row, see
[Findings this document does not cover](#findings-this-document-does-not-cover).

**Findings from AI code reviewers and AI content-review rubrics.** These tools return different
titles, severities and identifiers each time they read the same code. Match a finding by the
behavior it describes and the CWE it cites, not by its title or identifier. They often rate
design-level behavior as high severity: no client authentication, one cache for all callers, trust
in the origin or the cache volume, permissive defaults. [The security model](#the-security-model)
and the accepted risks answer these. Read the row, then check the finding's description of the
mechanism against `src/` before you accept it, because a reviewer can describe a mechanism that is
partly wrong. A scan of a diff can report a defect the diff removes, at the earlier line numbers, so
scan from the commit you run and treat a finding at deleted lines as already fixed. A finding
whose location holds none of the code it describes was raised against another commit: look the
function up by name, and if it is gone the finding is stale. A reviewer reads
this document and the comments in `src/`, and can be steered by both, so a sentence here that states
a protection is a claim for you to check. Record each disposition against the behavior, the release
you reviewed and the row it matched, so that the next run's findings can be matched to it. A tool's
silence on a file is not evidence that the file was reviewed.

### Findings about the code

| What scanners report | Verdict for this software | Where `src/` decides it | Setting that changes it | What is left to you |
|---|---|---|---|---|
| No client authentication or authorization; cache hits skip IAM and `x-amz-expected-bucket-owner`; no per-user isolation; the access log names the client address, not the caller, and has no cache-hit field (CWE-306, CWE-862, CWE-639, CWE-778) | By design: position 1, risk 1 and [Request validation limitations](docs/ARCHITECTURE.md#request-validation-limitations). With `get_ttl: "0s"`, every GET, including one that waited on another request's fetch, sends its own signed request to S3 before it is served | `HttpProxy::handle_request` in `src/http_proxy.rs` serves a hit without contacting S3; `CacheManager::generate_cache_key` in `src/cache.rs` keys an entry by bucket and object key, not by caller | [`cache.get_ttl` and `cache.head_ttl`](docs/CONFIGURATION.md#time-to-live-ttl-configuration), globally or per prefix in [cache rules](docs/CONFIGURATION.md#cache-rules) | Restrict who can reach the proxy. Set both TTLs to `"0s"` where S3 must authorize every read |
| The health, metrics and dashboard listeners have no authentication; the example configuration binds them to all interfaces; they answer any `Host`; OTLP export sends no authentication header (CWE-306, CWE-346, CWE-350) | Accepted: risk 6, [Listening surface](#listening-surface) and OTLP export under [Deployment hardening](#deployment-hardening) | `src/main.rs` (health and metrics) and `src/dashboard.rs`; `validate_listener_ip` in `src/config.rs` refuses a hostname as the health or metrics bind address | [`health.bind_address`](docs/CONFIGURATION.md#health-check-configuration), [`metrics.bind_address`](docs/CONFIGURATION.md#metrics-configuration), [`dashboard.bind_address`](docs/CONFIGURATION.md#dashboard-configuration) | Keep each on loopback unless another host needs it, and firewall the rest. Export OTLP to a collector you trust |
| A dynamic scanner run against the dashboard reports missing security response headers, such as `Content-Security-Policy` or `X-Frame-Options` (CWE-693, CWE-1021) | Accepted: the dashboard is a read-only status page that sets no cookies and offers no write or control actions ([Dashboard](docs/DASHBOARD.md)) | `src/dashboard.rs` | [`dashboard.bind_address`](docs/CONFIGURATION.md#dashboard-configuration) | Keep the dashboard on loopback, or restrict who can reach it |
| The proxy connects to whatever host a client names, including private and link-local addresses on the HTTP paths (server-side request forgery, CWE-918) | By design for a forwarding proxy: risk 8. The policy refuses only the ranges [Destination policy](docs/ARCHITECTURE.md#destination-policy-ssrf-protection) lists. With an allowlist set, the HTTP paths connect only to addresses the policy accepts; without one they apply no policy | `DestinationPolicy::check` in `src/destination_policy.rs`, called on each path the policy gates; `NameLookup::resolve` in `src/connection_pool.rs` drops refused addresses before a connection is made | `server.tls.connect_allowlist` ([Destination policy](docs/ARCHITECTURE.md#destination-policy-ssrf-protection)) | Set the allowlist to the hosts the proxy serves. Restrict who can reach the proxy |
| The destination check and the connection resolve the name separately, so a name with an allowed and a refused address could be dialed at the refused one (time-of-check to time-of-use, DNS rebinding; CWE-367, CWE-918) | Mitigated when an allowlist is set: the connection pool keeps only the addresses the policy accepts, and dials none when it refuses them all. Without an allowlist the HTTP paths apply no policy (risk 8) | `NameLookup::resolve` in `src/connection_pool.rs` | `server.tls.connect_allowlist` | Set the allowlist. Addresses in `endpoint_overrides` are dialed as listed, so list only addresses you control |
| A client can reach the instance metadata service through the proxy (CWE-918) | Refused wherever the destination policy applies. Elsewhere the default upstream transport is verified TLS to port 443, which the metadata service does not serve (risk 8) | `connect_port` in `src/https_connector.rs`; `HttpProxy::resolve_signed_upstream_transport` in `src/http_proxy.rs` | [`connection_pool.upstream_overrides`](docs/CONFIGURATION.md#upstream-transport-overrides) | An entry that waives a protection lifts this limit for every request it matches, including every subdomain of a DNS name. List only names whose DNS you control |
| A finding that needs an attacker to edit `config.yaml`, the environment, command-line arguments, `cache_rules.json` or the TLS key, or that notes an `endpoint_overrides` or `upstream_overrides` entry exempts a host from a destination check (CWE-15, CWE-807) | Outside the model: configuration is trusted input ([Who the proxy trusts](#who-the-proxy-trusts)), and each entry is the operator's own widening | `src/config.rs` loads and validates the configuration; `src/bucket_settings.rs` loads cache rules | None | Give write access to configuration only to the people who run the proxy. Each override and allowlist entry widens what clients can reach |
| Plaintext or unvalidated TLS between the proxy and the origin (CWE-295, CWE-319) | Opt-in development feature: position 4. The default is verified TLS on port 443 | `UpstreamOverrides::resolve` in `src/upstream_overrides.rs`; the startup warning names each entry that waives a protection | [`connection_pool.upstream_overrides`](docs/CONFIGURATION.md#upstream-transport-overrides) | Use these entries only on a secure network segment |
| The TLS proxy listener's private key is loaded without a check of its file permissions (CWE-732) | Accepted: the proxy reads the key as configured and does not check its mode or owner, and reads the key and certificate once, at startup | Loaded as configured, with no permission check | [`server.tls.key_path`](docs/CONFIGURATION.md#tls-proxy-listener-configuration) | Make the key readable by the proxy's user only (`chmod 600`), in a directory other users cannot list. To rotate the certificate or key, replace the files and restart the proxy |
| One bucket name's entries shared by two transports to one host, or across AWS partitions (CWE-345) | Accepted: risk 7. A bucket's entries bind to the first origin that caches it | `origin_identity` and `read_or_claim` in `src/origin_identity.rs` | None | Run a separate proxy per origin where the distinction matters ([One bucket name at two origins](docs/CACHING.md#one-bucket-name-at-two-origins)) |
| Cached data is stored unencrypted, and cache files carry no authentication code (CWE-311, CWE-345) | Accepted: risks 2 and 3. Frame checksums on cached data detect corruption, not tampering | `src/disk_cache.rs` and `src/compression.rs` | None in the proxy | Encrypt the storage the cache runs on. Restrict the cache volume to the proxies |
| An attacker who can write to the cache volume: tampered data, metadata or stored response headers, file paths named in cached metadata, or oversized files (CWE-20, CWE-22, CWE-73, CWE-400) | Outside the model: position 2 and risk 2. The proxy acts on what the cache volume holds, including the file paths and sizes its metadata records, without authenticating it | The proxy does not authenticate what the volume holds, including the file paths and sizes its metadata records. Exclusive file creation in `src/disk_cache.rs` covers only the files the proxy creates | None | Restrict the cache volume to the proxies. The proxy acts on the file paths cached metadata names, with its own file permissions. Give it write access to only the cache, log and state directories, as the systemd sandbox under [Deployment hardening](#deployment-hardening) does |
| Path traversal through object keys, bucket names or upload IDs (CWE-22) | Mitigated: every cache path built from a request stays under `cache_dir`. A path read from cached metadata falls under the row above | `parse_cache_key` and `get_sharded_path` in `src/disk_cache.rs`; `mpu_upload_dir` hashes an upload ID before it names a directory | None | Nothing beyond risk 2 |
| A presigned URL stays usable after it expires, and signatures are not verified (CWE-613, CWE-347) | Accepted: risk 4. An expired SigV4 URL is refused before any cache lookup | `parse_presigned_url` and `PresignedUrlInfo::is_expired` in `src/presigned_url.rs`, called from `HttpProxy::handle_get_head_request` | [`cache.get_ttl` and `cache.head_ttl`](docs/CONFIGURATION.md#time-to-live-ttl-configuration) | Set both to `"0s"` for prefixes read through presigned URLs |
| Upstream DNS goes to public resolvers, `/etc/hosts` is ignored, and `connection_pool.dns_servers` is not used on every path (CWE-319) | Disclosed: Upstream DNS under [Deployment hardening](#deployment-hardening) | `src/connection_pool.rs` builds the resolver that uses `dns_servers`; the paths Upstream DNS lists build their own | [`connection_pool.dns_servers`](docs/CONFIGURATION.md#dns-server-configuration) | Point it at resolvers you control that do not resolve S3's hostnames to the proxy |
| Resource exhaustion by a client that can reach a listener (CWE-400, CWE-770) | Accepted under position 1: the proxy is not hardened against a hostile client that can reach its listeners. It enforces no connection cap, and some per-client state that a listener holds is not bounded. It does bound each wait on the origin during an upload, and S3's buffered answer to a write | The request limit in `HttpProxy::handle_request`; the buffered-byte ledger in `src/inflight_ledger.rs` | [`server.max_concurrent_requests`](docs/CONFIGURATION.md#max-concurrent-requests) and [`server.max_inflight_buffer_bytes`](docs/CONFIGURATION.md#in-flight-memory-ceiling) | Restrict who can reach the listeners. Limit connections at a load balancer, a firewall or the host ([Listening surface](#listening-surface)). `server.request_timeout` ([`request_timeout` is not enforced](docs/CONFIGURATION.md#request_timeout-is-not-enforced)) and `connection_pool.max_connections_per_ip` ([Configuration methods](docs/CONFIGURATION.md#configuration-methods)) are accepted and have no effect, so neither bounds a client |
| Uploads to the origin have no overall deadline (CWE-400) | Mitigated: each wait on the origin is bounded, and an upload that keeps moving is never cut off | The upstream write path in `src/signed_request_proxy.rs` | [`connection_pool.connection_timeout` and `connection_pool.upstream_write_idle_timeout`](docs/CONFIGURATION.md#upstream-write-timeout) | Nothing beyond the settings |
| Insecure defaults: no destination policy until an allowlist is set, cached objects served without authorization within the TTL, data-plane listeners on all interfaces (CWE-1188) | By design, with each default stated: the HTTP forwarding paths are unchecked without an allowlist (risk 8), hits within `cache.get_ttl` skip S3 (risk 1), and the data-plane listeners accept on all interfaces ([Listening surface](#listening-surface)) | `src/config.rs` holds the defaults ([Configuration reference](docs/CONFIGURATION.md)) | `server.tls.connect_allowlist`, [`cache.get_ttl`](docs/CONFIGURATION.md#time-to-live-ttl-configuration) | Set the allowlist, set TTLs to `"0s"` where S3 must authorize every read, and restrict who can reach the ports |
| A request that loops back through the proxy on the HTTP paths (CWE-400) | Detected, with the exceptions in [Request loops](docs/ERROR_HANDLING.md#9-request-loops-via-and-508-loop-detected) | `HttpProxy::loop_rejection` in `src/http_proxy.rs` and `via_loop_detected` in `src/signed_request_proxy.rs` | None | Make sure the names clients send resolve to the origin, not to the proxies |
| The HTTPS passthrough (port 443) has no loop detection, and connects wherever a client's TLS handshake names (CWE-406, CWE-674) | Accepted under position 1: the passthrough relays bytes without reading them, so it cannot detect a loop, and a client that can reach port 443 can use up the proxy's connections | The passthrough does not read requests | `server.tls.connect_allowlist` ([Destination policy](docs/ARCHITECTURE.md#destination-policy-ssrf-protection)) | Set the allowlist to the hosts the proxy serves, restrict who can reach port 443, and limit connections at a load balancer or firewall ([Listening surface](#listening-surface)) |
| Log injection through request text written to a log (CWE-117) | Mitigated: the access log escapes every field that holds request text, and the proxy answers `400` to an invalid `Host` ([Escaping and redaction](docs/ACCESS_LOG_FORMAT.md#escaping-and-redaction)) | `format_entry` and `sanitize_log_field` in `src/logging.rs`; `parse_host_header` in `src/http_proxy.rs` | None | Application log lines carry object keys and other request values as sent; only presigned URL credentials are masked. Restrict the log directories, and treat log content and error response bodies, which repeat request text, as untrusted when you parse or display them |
| Credentials in logs (CWE-532) | Mitigated: presigned URL credentials are masked at the application log writer, in every line at every level, and the dashboard's log viewer masks them again | `mask_presigned_params` and the masking log writer in `src/logging.rs`; `read_log_file` in `src/dashboard.rs` | [`logging.log_level`](docs/CONFIGURATION.md#logging-configuration) | Run at `info`: masking covers presigned URL parameters, and `debug` and `trace` lines carry object keys and other request detail |
| The log retention sweep follows symbolic links (CWE-59) | Accepted: anyone who can create entries in the log directories is already trusted | Not checked: the log directories are trusted | [Logging configuration](docs/CONFIGURATION.md#logging-configuration) | Give write access to the log directories only to the people who run the proxy, and keep them off the cache volume |
| The bandwidth fairness class comes from the client's `User-Agent` (CWE-807) | Accepted: QoS divides bandwidth fairly and is not access control ([Fairness classes](docs/BANDWIDTH_QOS.md#fairness-classes-caller-or-bucket)) | `resolve_fairness_key` in `src/bandwidth_limiter.rs` | [`download_bandwidth.caller_id`](docs/CONFIGURATION.md#download-bandwidth-qos-configuration) | Leave caller keying off, its default, where clients cannot be trusted to name themselves |
| Proxies of different releases on one cache volume (no single CWE) | Supported only as [Upgrading](docs/UPGRADING.md) describes for each release; [Cache integrity](#cache-integrity) lists the affected releases and what to do | See [Who the proxy trusts](#who-the-proxy-trusts) | None | Follow the upgrade and rollback steps for your release |
| A race between fills, listings or a publish can serve one object version's bytes under another's headers (CWE-367, CWE-345) | Guarded: [Cache integrity](#cache-integrity) lists the guards, what they do not cover and the affected releases | The guards that section names | [`cache.trust_unverified_ranges`](docs/CONFIGURATION.md) | Leave `cache.trust_unverified_ranges` at `false` unless objects are never overwritten in place outside the proxy, and follow [What to do](#what-to-do) |
| Uploads are cached without the proxy checking everything S3 checks, and multipart part ETags are MD5 (CWE-20, CWE-328) | By design: the origin's success response decides whether an upload is cached ([Trust and integrity model](docs/ARCHITECTURE.md#trust-and-integrity-model)) | `SignedPutHandler` commits a cached upload only after a success response | [`cache.write_cache_enabled`](docs/CONFIGURATION.md#write-cache-basic-settings) | Request a SHA-256 checksum on multipart uploads where you need protection against a writer who controls the content |
| `unsafe` blocks (Semgrep `unsafe` rules) | Accepted: the `unsafe` inventory under [What you can verify yourself](#what-you-can-verify-yourself) | The `grep` commands in that section list each block | None | Read each block |
| Findings in test code: a credential-like string, an assertion that prints a fixture credential (CodeQL's cleartext logging), a generic key rule matching a test string, or a temporary directory (CWE-312, CWE-359, CWE-377, CWE-532, CWE-798) | Not shipped: compiled only when tests or examples are built, never into the release binary | `#[cfg(test)]` modules, `tests/` and `examples/`. A finding in a `src/` file is test code when its line lies inside a `#[cfg(test)]` module; `grep -n 'cfg(test)'` on the file locates each one. Every file under `src/cache/moved_fill_tests/` is test code, because `src/cache.rs` declares that module `#[cfg(test)]` | None | Nothing |
| S3 Select (SelectObjectContent) responses are buffered without bound, cut short or cached (CWE-770, CWE-20) | Not applicable: the proxy answers a SelectObjectContent request with `501 NotImplemented` before any upstream connection or cache work. A request through the HTTPS passthrough (port 443) or a `CONNECT` tunnel reaches S3 unread: S3 authorizes it, and nothing is cached | `HttpProxy::is_select_object_content`, checked in `HttpProxy::handle_request` in `src/http_proxy.rs` | None | Send S3 Select requests to S3 directly |
| `cargo audit`, or another dependency scanner, reports an unmaintained, yanked or unsound crate (no single CWE) | Not a vulnerability in this software: none of these fails `cargo audit` ([What you can verify yourself](#what-you-can-verify-yourself)) | `.cargo/audit.toml` lists each ignored advisory with its reason | None | Re-run `cargo audit` at the release you build |

### Findings about documentation and examples

| What scanners report | Verdict for this software | Where it is decided | Setting that changes it | What is left to you |
|---|---|---|---|---|
| Endpoint examples use `http://` URLs (CWE-319) | Required for caching: risk 5. Changing them to `https://` makes the client open a tunnel the proxy cannot cache | [Configure client routing](docs/GETTING_STARTED.md#3-configure-client-routing) | `server.tls`, for the [TLS proxy listener](docs/CONFIGURATION.md#tls-proxy-listener-configuration) | Use the TLS proxy listener for clients on other hosts |
| Container base images named by tag (CWE-494) | Accepted: the Dockerfile is an example | The notes under the [Dockerfile](docs/DOCKER.md#dockerfile) | None | Pin each `FROM` to a digest, as those notes describe |
| `AKIAIOSFODNN7EXAMPLE` matches an AWS access key pattern (CWE-798) | False positive | [The example access key](#the-example-access-key) | None | Confirm the match is that string, then dismiss it |
| A documentation sentence gives a figure with no source, uses wording such as "production-ready", or omits a compliance note for regulated use (no CWE) | Not a defect in the software. Measured figures hold for the hardware the page states, and compliance obligations rest with the operator | The page the finding names | None | Treat a figure as a starting point and measure on your own hardware. Report a figure presented as a measurement of the software that cites no source as a documentation issue |

A content-review rubric that flags an `http://` endpoint, a figure without a source or a missing
compliance note is checking the documentation, not the software. Match its finding to the rows
above, and dismiss it only after you confirm the page says what the row says.

### The example access key

Semgrep's secret rules derived from gitleaks flag `AKIAIOSFODNN7EXAMPLE` as an AWS access key,
and other secret scanners may too. It is the example access key ID published in AWS's own
documentation, and it appears in test fixtures, signature-parsing tests, and examples. It is not a
credential and grants nothing.

gitleaks itself does not flag that string, but its generic key rule matches test strings that name a
key, such as a cache key with a file suffix. Those matches are in test code, which the test-code row
under [Findings about the code](#findings-about-the-code) covers.

Most occurrences carry inline suppression annotations, but not every scanner honors them, so the
finding recurs on every run regardless. Confirm the matched literal is that string, then dismiss it.

Apart from `.cargo/audit.toml`, the published repository ships no scanner configuration or ignore
files, so a scanner reports every match its rules find.

### Findings this document does not cover

For a finding that matches no row and no accepted risk, report it privately as a potential
vulnerability if it would let any of these happen:

- A client receives bytes or headers that differ from what S3 holds for that request, beyond the
  cases in
  [When the proxy can return data that differs from S3](docs/CACHE_FRESHNESS.md#when-the-proxy-can-return-data-that-differs-from-s3).
- One client makes another client's correctly signed request fail by changing what the cache holds.
- A party reads cached data, or reaches a destination the destination policy refuses, in a way the
  security model does not already allow.
- A credential or signature reaches a log, metric or page that the documentation says masks it.
- The proxy writes outside `cache_dir`, its log directories and its state directory, or runs code
  that a request supplies. Write access to the cache volume that plants a path in cached metadata is
  position 2, not a new finding.

Otherwise, open a GitHub issue. That covers hardening, resource use by a client that can reach the
proxy (position 1), and documentation that is unclear or inaccurate. While you wait for an answer,
apply the model's mitigations. Restrict who can reach the proxy, set `get_ttl: "0s"` and
`head_ttl: "0s"` for prefixes where S3 must see every read, and keep the monitoring listeners on
loopback. If you are unsure which applies, report it privately as
[Reporting a vulnerability](#reporting-a-vulnerability) describes.

## Deployment hardening

`config/s3-proxy.service` ships with a systemd sandbox applied:

`NoNewPrivileges=true`, `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp=true`,
`PrivateDevices=true`, `ProtectKernelTunables=true`, `ProtectKernelModules=true`,
`ProtectControlGroups=true`, `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX`, a
`ReadWritePaths` allowlist covering only the cache and log directories, and
`StateDirectory=s3-proxy`, which gives the proxy a writable `/var/lib/s3-proxy` for its node-local
state (`logging.local_state_dir`).

The unit runs as root by default, because binding ports 80 and 443 requires it. If your deployment
uses the forward-proxy port or a load balancer that owns 80 and 443, the unit file documents the
change to an unprivileged user with `AmbientCapabilities=CAP_NET_BIND_SERVICE`. Adjust
`ReadWritePaths` to match your configured cache and log directories, and keep `StateDirectory` if
you leave `logging.local_state_dir` at its default; `ProtectSystem=strict` makes
everything else read-only, and a missing path fails at write time rather than at startup.

**Upstream DNS.** By default the proxy resolves origin hostnames through the Google and Cloudflare
public resolvers over unencrypted DNS and never reads `/etc/hosts`; `connection_pool.dns_servers`
replaces them for upstream requests through the connection pool. The HTTPS passthrough on port 443,
CONNECT tunnels through the TLS proxy listener, and destination-policy lookups on the HTTP forwarding
path always use the public resolvers. The proxy never reads `/etc/hosts` because an on-host
deployment uses it to send S3 traffic to the proxy itself. Set `connection_pool.dns_servers` to
resolvers you control that do not resolve S3 hostnames to the proxy. See [Configuration reference](docs/CONFIGURATION.md).

**OTLP export.** The proxy does not send `metrics.otlp.headers`, so metrics export carries no
authentication header. Export to a collector on the proxy host or on a network you trust.

**Logs.** The access log escapes the fields that carry request text: `bucket_owner`, `bucket`,
`request_uri`, `referer`, `user_agent` and `host_header`
([Escaping and redaction](docs/ACCESS_LOG_FORMAT.md#escaping-and-redaction)). The proxy answers
`400` to a `Host` header that is not a host name or address with an optional port. Presigned URL
credentials are masked in every application log line at every level, and the dashboard's log viewer
masks them again. Every log still holds values that clients sent, such as object keys, so treat log
files as sensitive and their contents as untrusted input when you parse or display them. Run
application logging at `info` in production
([`logging.log_level`](docs/CONFIGURATION.md#logging-configuration)), because `debug` and `trace`
lines carry object keys and other request detail. `RUST_LOG`, when set, overrides the configured
level. Restrict the log directories to the people who operate the proxy, because the retention
sweep acts on whatever they hold. The dashboard's log viewer shows the application log to anyone who
can reach the dashboard, so restrict it the same way.

**Cache size.** Cache capacity is limited by bytes only. A `cache.shared_storage.max_cached_objects`
key in a configuration file is accepted and ignored, with a warning at startup. On a file system that
limits its number of files, size that limit for the number of objects you expect to cache.

**Upgrades.** Upgrading replaces the binary and restarts the proxy; existing configuration keeps
working. Before upgrading a fleet, read [Upgrading to 3.0.2](docs/UPGRADING.md#upgrading-to-302) and
the section for each release you skip, including
[Upgrading to 3.0.0](docs/UPGRADING.md#upgrading-to-300) when upgrading from 2.x.

## Listening surface

None of these ports authenticate. All of them are yours to restrict.

`server.mode` selects between two mutually exclusive front doors, so the first three rows are never
all open at once. `standard`, the default, opens the HTTP and HTTPS listeners and ignores the
forward-proxy port; `proxy_only` opens the forward proxy and starts neither of the others. Every port
number below is the default and is configurable.

| Port | Purpose | When it is open |
|---|---|---|
| 80 | HTTP listener, with caching | `standard` mode, the default |
| 443 | HTTPS passthrough, no caching | `standard` mode, the default |
| 3128 | HTTP forward proxy | `proxy_only` mode only |
| 3129 | TLS proxy listener, terminates TLS and caches | only when `server.tls` is enabled, which it is not by default |
| 8080 | Health check | enabled by default; loopback by code default, all interfaces in the example config |
| 9090 | Metrics | enabled by default; loopback by code default, all interfaces in the example config |
| 8081 | Dashboard, read-only | enabled by default; loopback by code default, all interfaces in the example config |

The HTTP listener, HTTPS passthrough, forward proxy and TLS proxy listener take no bind address and listen on all interfaces;
restricting them is a network control. The monitoring listeners (health, metrics and dashboard) each take a
`bind_address`, which defaults to `127.0.0.1`; open one only when another host needs it. For health
and metrics it must be a literal IP address such as `127.0.0.1` or `::1`, and the proxy refuses to
start with anything else. See [Configuration reference](docs/CONFIGURATION.md).

The 9090 metrics default applies whenever a configuration file is given. With no configuration file
the metrics endpoint defaults to 8080, the health check's port, and only one of the two starts, so
run the proxy with a configuration file.

The data-plane listeners do not limit the number of open connections, per client or in total. Limit
connections at a load balancer, a firewall, or the host.

## Where verification happens

**This repository ships no continuous-integration configuration.** There is no workflow file, no
pipeline definition, and no status badge, so nothing here builds, lints, or tests the code on its own.

Tests are run before each release, in two places. The full unit and integration suite runs under
`cargo test --release`, which you can run yourself and which is the same suite that gates a release.
The maintainers also run an end-to-end suite, which is not included in this repository, against a
live fleet and real S3, covering the caching, conditional-request, multipart,
compression, range, and write-through paths.

Apart from that end-to-end suite, `cargo test --release` and the checks in "What you can verify
yourself" above are the whole release gate.
