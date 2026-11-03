# Security

This document is written for someone reviewing Hybrid Cache for Amazon S3 before deploying it. It states the
security model the software assumes, the risks that model leaves with you, and the checks you can run
yourself against this repository.


**Applies to release 3.0.1. Last reviewed 2026-11-03.**

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
S3, and a cache hit does not. Network controls are your access boundary.

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

---

## Accepted risks

These are known, unaddressed in code, and left to you to mitigate at deployment time.

### 1. Any client that can reach the proxy can read any cached object

With a TTL above zero, cache hits are served without contacting S3, so a client with no credentials
and a client whose S3 access was revoked both receive cached data until the entry expires. Different
users share the same cached responses; there is no per-user isolation. Conditional requests follow
the same rule: `cache.evaluate_conditions_from_cache` defaults to `true`, so within the TTL a request
whose `If-Match` names the cached ETag is answered from cache.

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

A presigned URL with a five-minute expiry may be served from cache for the full cache TTL, so access
can extend beyond what the signer intended. This is inherent to caching a presigned request's result.

**Mitigations.** Set `get_ttl: "0s"` for prefixes accessed by presigned URL, which forces
revalidation on every request.

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

The destination policy refuses loopback, private and link-local addresses, including the instance
metadata service address. It always applies to `CONNECT` tunnels and to the port 443 passthrough. On
the HTTP listener, the forward proxy and the TLS proxy listener's non-`CONNECT` requests it applies
only when `server.tls.connect_allowlist` is set, so without an allowlist a client can have the proxy
forward a request to a private or link-local address. Without an allowlist the proxy cannot tell an
intended origin on a private address, such as an S3-compatible store on your network, from an
unintended one.

**Mitigations.** Set `server.tls.connect_allowlist` to the hosts the proxy serves; it turns the check
on for the HTTP paths as well. Restrict which clients can reach the proxy, as for risk 1. See
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

Every known case needs an object overwritten in place outside the proxy, except an error reply that
a 2.x release cached as object data, described under [Affected releases](#affected-releases). Most
cases also need concurrent fills of one range, a rolling upgrade, or a cache volume that a 2.x
release wrote to. A deployment whose objects are each written once is exposed to none of the others.

### What guards against it

- **File identity.** When the proxy caches a range, it records the range file's length and
  modification time, and every read compares them with the file it opens before sending anything
  from it. A mismatch is refused, counted in
  `atomic_metadata.range_file_identity_mismatch_detected`, and the range is fetched from S3 and
  cached again. Two files of equal length modified less than 1 ms apart have the same identity.
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

The guards above close each case in 3.0.1, except as described under [What the guards do not
cover](#what-the-guards-do-not-cover) and [Damaged range files](#damaged-range-files).

- **Concurrent fills sharing one temporary file**: 3.0.0, between fills in one proxy, for example
  concurrent `?partNumber=` GETs of one part. Public 1.6.7 to 2.8.4 shared one temporary file
  between every proxy on the cache volume.
- **A fill replacing a range file that another proxy had just written, for an object with no cached
  metadata**: 3.0.0.
- **Part layouts recorded across versions**: public 1.6.7 to 3.0.0, including public 2.8.1 and
  public 2.8.2. Before 3.0.0 the other version's part bytes could be served as well. From public
  2.2.0 to 2.8.4, a conditional `?partNumber=` GET was also answered from the cache without S3
  evaluating its condition.
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
  bytes of an LZ4 `{key}` lasted until eviction. The RAM cache now holds ranges only.
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
permanent waivers. Two entries are present as this document was last reviewed:

- `RUSTSEC-2026-0190` (`anyhow`): reached only through build-time code generation for the optional
  OpenTelemetry export, not through a runtime dependency of the shipped binary.
- `RUSTSEC-2026-0097` (`rand` 0.8): reached only through a development dependency used by
  property-test macros, never linked into the release binary.

Both are `unsound` advisories rather than vulnerabilities, so `cargo audit` exits zero whether or not
they are ignored; removing the entries changes what the command prints but not whether it passes. A run
may also report two warnings, which likewise do not fail the command: `unsafe-libyaml` (RUSTSEC-2024-0449,
unmaintained), reached through the YAML configuration parser, and a yanked `chacha20` release, reached
through the DNS resolver. Neither is a vulnerability advisory.

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

## If you run your own scanners

One finding recurs and is a false positive.

Secret scanners such as gitleaks, and Semgrep rules derived from them, flag
`AKIAIOSFODNN7EXAMPLE` as an AWS access key. It is the example access key ID published in AWS's own
documentation, and it appears in test fixtures, signature-parsing tests, and examples. It is not a
credential and grants nothing.

Most occurrences carry inline suppression annotations, but not every scanner honors them, so the
finding recurs on every run regardless. Confirm the matched literal is that string, then dismiss it.

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

**Cache size.** Cache capacity is limited by bytes only. A `cache.shared_storage.max_cached_objects`
key in a configuration file is accepted and ignored, with a warning at startup. On a file system that
limits its number of files, size that limit for the number of objects you expect to cache.

**Upgrades.** Upgrading replaces the binary and restarts the proxy; existing configuration keeps
working. Before upgrading a fleet, read [Upgrading to 3.0.1](docs/UPGRADING.md#upgrading-to-301),
and [Upgrading to 3.0.0](docs/UPGRADING.md#upgrading-to-300) as well when upgrading from 2.x.

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
