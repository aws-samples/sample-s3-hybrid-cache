# Request-Aware Routing with HAProxy

A multi-instance fleet needs something in front of it to select a member and health-check
the rest. [HAProxy](https://www.haproxy.org/), a third-party open-source load balancer not
affiliated with or endorsed by AWS, can do that job and two others at the same time. This
page gives a tested configuration and says where each of the three pays off.

**Encrypt the client hop.** DNS multi-value routing is the cheap way to spread clients
across a fleet, but its cacheable path is plain HTTP straight to the chosen instance, so
the request and its payload cross the network in the clear. HAProxy terminates client TLS
and re-encrypts to each instance's own TLS listener, closing that without a managed load
balancer. See [What a Cleartext Hop Exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes).

**Drop the per-GB charge on AWS.** A Network Load Balancer bills for every GB it
processes, cache hits included, even though those bytes came off local disk and never
reached S3. Caching reduces your S3 transfer cost but not the balancer's, so the balancer
takes a growing share of the spend the cache was bought to reduce. A router on each client
host removes it from the data path.

**Route by object and byte range.** A Layer 4 balancer selects by connection, so the same
hot bytes are served by whichever instance a connection landed on. They then sit in several
instances' RAM tiers at once, and simultaneous misses for them are fetched from S3 more than
once. Hashing on the object and page gives each page a single owner, so the fleet's RAM
holds more distinct data and concurrent readers of one range share a fetch.

Like the rest of this project, everything here is sample code for you to evaluate, not a
supported configuration. [GETTING_STARTED.md](GETTING_STARTED.md) covers the simpler
client-routing options and is the better starting point. Deploying and operating HAProxy
itself is your responsibility; consult its own
[documentation](https://www.haproxy.org/#docs) for anything beyond the configuration given
here.

## What affinity adds

An HAProxy tier configured this way is an **affinity router**: it hashes each request to one proxy
by object key and page index, so every read of a given page reaches the same instance. Other
documents in this set use that term for it, [Local NVMe Cache Fleets](LOCAL_NVME_CACHE.md) in
particular, where the router is a prerequisite rather than an optimisation.

Routing by object and page buys two distinct things.

**RAM cache efficiency, on every repeat read.** Each instance keeps its own RAM tier.
Without affinity a hot range is served by whichever instance the connection happened to
land on, so the same bytes go resident in several instances' RAM at once and the fleet's
aggregate RAM holds a fraction of the distinct data it could. Send every read of a page to
one owner and each page occupies RAM once, so the fleet keeps roughly N times more distinct
hot data resident. This needs no concurrency and no page-aligned range caching.

**Concurrent-miss coalescing, when reads overlap in time.** In-flight miss deduplication
happens inside a single proxy process, so it applies when several readers want the same
bytes at the same time. Reuse spread over minutes is already served from the shared cache
volume. If this is your main reason for deploying, measure duplicate upstream fetches under
the real workload.

One client note, and it bounds only the widening behaviour:
[page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching) is opt-in per
key pattern and is skipped when the client signs the `Range` header, which the AWS CLI and
the official SDKs do. Those clients still get one owner per page, the RAM efficiency above,
and coalescing of byte-identical concurrent ranges. What they do not get is adjacent ranges
merged into a single upstream fetch.

## Where the router runs

The routing configuration is the same wherever it runs. Only the `bind` line and what
fronts it change.

| | On-premises | AWS |
|---|---|---|
| **Central** | Recommended. Nothing meters the hop, so affinity costs a component to operate rather than a per-GB charge: two HAProxy instances behind a floating address ([High availability](#high-availability)), or the load-balancing tier you already operate if you have one. Encrypts the client hop. | Not recommended. Needs a load balancer in front, which keeps the per-GB charge it was meant to avoid. |
| **Sidecar on each client host** | Works, and needs no client TLS at all because the cleartext hop never leaves the host. Costs a process, a config, and the fleet CA on every host. | The only way to get affinity without paying for a load balancer in front. |

### The router's certificate

Central placement terminates client TLS, so give the router a certificate clients already
trust: one from your own PKI, or a publicly trusted one from ACM or Let's Encrypt. Then no
client needs changing. Certificate loading, rotation, and ACME integration are HAProxy's
own concern, and its [documentation](https://www.haproxy.org/#docs) covers them.

A self-signed certificate is the fallback, and it costs you trust distribution to every
client host. If you go that way, install it in the system trust store rather than pointing
`AWS_CA_BUNDLE` at it, because that variable applies to the S3 endpoint and not to the
proxy connection.

Whichever you use, the certificate needs a SAN covering the address or name clients dial.

A sidecar needs none of this: it binds to loopback, so there is no client TLS to terminate.

### Why a central router needs a virtual address

The proxy fleet's usual high-availability story relies on
[multi-value DNS](GETTING_STARTED.md#3-configure-client-routing): clients resolve the
*S3 hostname* to several proxy addresses, and modern S3 clients built on the AWS Common
Runtime (the AWS CLI v2 and current SDKs) distribute requests across the returned
addresses and retry a different one on connection failure. That is genuine client-side
load balancing and failover, done by the client, for free. See
[Load Balancer vs DNS Multi-Value Routing](GETTING_STARTED.md#load-balancer-vs-dns-multi-value-routing)
for how that compares to a Layer 4 load balancer.

**`HTTP_PROXY` does not get any of that.** The proxy variable takes one URL. The client
resolves that host once per connection and does not distribute across multiple A records
or retry a sibling address on failure the way it does for the S3 hostname. Publishing
several addresses for a central router therefore buys nothing: the CRT's multi-value
handling is specific to resolving the destination it is signing for, not to whatever
`HTTP_PROXY` happens to point at.

### High availability

The two placements fail differently, and the sidecar has the smaller blast radius.

**Central** needs at least two HAProxy instances sharing one address, and open-source
HAProxy does not provide that itself. The common route is a floating address managed by a
separate tool such as [keepalived](https://www.keepalived.org/), which implements VRRP;
HAProxy's commercial editions ship
[their own VRRP module](https://www.haproxy.com/documentation/haproxy-enterprise/administration/high-availability/active-standby/)
for the same job. Pick whichever suits your environment, and treat it as infrastructure
outside this configuration.

What matters here is that moving traffic between the two instances does not move ownership,
provided both carry [`hash-key addr`](#more-than-one-router-requires-hash-key-addr). A
failover therefore keeps affinity intact.

**Sidecar** needs no network-layer HA. Each host has its own router, so a failure affects
that host alone rather than every client. Availability comes from process supervision:

```ini
[Service]
Restart=on-failure
RestartSec=2
```

The trade is that a dead sidecar takes its host out of service completely, with no
failover, so supervision has to be reliable. Weigh that against a central tier whose
failure is shared by everyone.

## Client configuration

```bash
# Sidecar: plaintext to loopback, HAProxy re-encrypts to the fleet.
export HTTP_PROXY=http://127.0.0.1:8080

# Central: TLS to the router.
#   export HTTP_PROXY=https://router.internal:8443

export AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com
export NO_PROXY=169.254.169.254
```

- **The `http://` S3 endpoint is required.** It makes the client send a readable,
  absolute-form request that HAProxy can inspect. An `https://` endpoint produces a
  `CONNECT` tunnel that cannot be cached or routed.
- **`NO_PROXY` is required**, at minimum for the instance metadata service. See
  [below](#this-is-not-a-general-purpose-proxy) for why the list matters more than it looks.
- **No certificate for an AWS-owned hostname is involved.** The client's TLS session, when
  there is one, terminates on the router's own name. SigV4 signs the `Host` header, not the
  scheme, so the signature stays valid through to S3.
- **A multi-value DNS name behind `HTTP_PROXY` does not give you failover.** Modern S3
  clients only spread across addresses and retry siblings for the *S3 hostname* they are
  signing for; that behaviour does not extend to the destination in `HTTP_PROXY`.
- **Central placement needs a client that can speak TLS to a proxy**, or the request fails
  outright. For the AWS CLI that means a current version from the bundled installer, which
  ships its own HTTP stack; a build linked against an old system `urllib3` cannot do it.
  `aws --version` reports `exe/…` for a bundled install and `source/…` for one using
  system libraries.

### This is not a general-purpose proxy

The configuration below routes hosts ending in `.amazonaws.com` to the cache fleet and
**refuses everything else with a 502**. That is deliberate: a router whose backend is the
cache fleet has nowhere sensible to send unrelated traffic, and quietly forwarding it to
an S3 address would be worse than refusing it.

The consequence matters on a client host, where `HTTP_PROXY` is often set globally and
every HTTP request on the box would arrive here. Either scope the variable to the S3
client rather than exporting it shell-wide, or list everything else in `NO_PROXY`. If you
need a real forward proxy for other traffic, run one separately.

## Configuration

Exercised with HAProxy 3.0.23 on Amazon Linux 2023 in front of a three-instance fleet, in
both placements.

```haproxy
global
    log stdout format raw local0
    maxconn 2000
    stats socket /var/lib/haproxy/admin.sock mode 660 level admin

defaults
    log     global
    mode    http
    option  httplog
    timeout connect 5s
    timeout client  300s
    timeout server  300s
    retries 2
    option  redispatch

frontend s3_router
    # Sidecar placement: loopback only, plaintext never leaves the host.
    bind 127.0.0.1:8080
    # Central placement: terminate client TLS instead. Clients must trust this
    # cert; see "The router's certificate".
    #   bind :8443 ssl crt /etc/haproxy/tls/router.pem
    mode http
    option http-use-proxy-header

    acl is_fleet        req.hdr(host) -m end .amazonaws.com
    acl has_range       req.hdr(Range) -m found
    acl is_suffix_range req.hdr(Range) -m reg ^bytes=-

    # Page index = range_start / 16 MiB, matching the cache's page grid.
    http-request set-var(txn.pg) str(full) if is_fleet !has_range
    http-request set-var(txn.pg) str(tail) if is_fleet has_range is_suffix_range
    http-request set-var(txn.pg) req.hdr(Range),regsub(^bytes=,,),field(1,-),div(16777216) if is_fleet has_range !is_suffix_range

    # Routing key = object path + page index.
    http-request set-var(txn.rk) path,concat(,txn.pg) if is_fleet

    use_backend cache_fleet if is_fleet
    default_backend refuse_other

backend cache_fleet
    balance hash var(txn.rk)
    hash-type consistent
    hash-balance-factor 150

    option httpchk GET /health
    http-check expect status 200

    # Overrides the defaults block: a redispatched retry goes to a DIFFERENT member,
    # which is the one thing an affinity backend must not do. See below.
    no option redispatch

    # hash-key addr is REQUIRED whenever more than one router exists.
    server-template m 1-16 cache-fleet.internal:3129 check port 8080 resolvers fleetdns init-addr none ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr

# Anything not addressed to the fleet. See "This is not a general-purpose proxy".
backend refuse_other
    http-request deny deny_status 502

resolvers fleetdns
    nameserver ns1 10.0.0.2:53
    resolve_retries 3
    timeout resolve 1s
    timeout retry   1s
    hold valid      10s
    hold other      10s
    hold refused    10s
    hold nx         10s
    hold timeout    10s

listen stats
    bind 127.0.0.1:9000
    mode http
    stats enable
    stats uri /
```

Substitute `cache-fleet.internal` for a name resolving to your fleet's addresses and
`10.0.0.2:53` for a resolver that can answer it.

**No DNS name for the fleet?** Replace the template with static lines. Keep `hash-key
addr` on every one, and keep the addresses identical across all routers:

```haproxy
    server m1 10.0.1.11:3129 check port 8080 ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
    server m2 10.0.1.12:3129 check port 8080 ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
    server m3 10.0.1.13:3129 check port 8080 ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
```

Validate before reloading, and check the exit status rather than piping to `tail`, or a
failure will be masked:

```bash
haproxy -c -f /etc/haproxy/haproxy.cfg && systemctl reload haproxy
```

### Why the backend turns off `redispatch`

`option redispatch` in the `defaults` block sends a retried request to a **different** server.
Every other backend wants that. An affinity backend does not: the whole point is that one member
owns a page, and redispatching hands the request to a member that does not, which fetches the same
bytes from the origin and caches a second copy. So the backend overrides it with
`no option redispatch`, and the retry goes back to the owner.

`retries 2` is left alone, because HAProxy's default `retry-on` is `conn-failure` — a retry fires
only when the connection could not be established, before any request body has been forwarded. That
is worth keeping: it rides out a transient connect failure without the router having to replay
anything.

**If you widen `retry-on`, exclude writes.** Adding response-shaped conditions
(`empty-response`, `response-timeout`, `503`, and so on) lets a retry fire after the body has
started moving, and HAProxy cannot replay a request body it has already streamed. On a large `PUT`
or `UploadPart` that produces a failed upload rather than a retried one.

### Deploying a sidecar

A sidecar handles two hops, and only the first is plaintext. It accepts HTTP on loopback,
where nothing crosses the network, then originates TLS outbound to each instance's listener
on 3129. That outbound leg is why the server lines carry `ssl verify required`, and it is
what keeps the fleet hop encrypted.

Install HAProxy from your distribution, drop the config in place, and supervise it. The
loopback bind needs no privileged port, so the service does not need to run as root for the
listener's sake.

Verifying the fleet's certificates is the part that scales with host count. If those
certificates come from your own PKI or a public CA, point `ca-file` at the system trust
store and there is nothing to distribute:

```haproxy
    ... ssl verify required ca-file @system-ca hash-key addr
```

A self-signed fleet certificate has to be copied to every host instead, and rotated there,
which is the real operational cost of this placement. Keeping the config identical
everywhere is the other; the routing itself is the easy part.

## How the routing key works

`(object path, floor(range_start / 16 MiB))`.

Requests for one page converge on one member. Different pages of the same object still
spread across the fleet, so a parallel multi-range download keeps its fan-out.

**Include the path in the key.** Hashing on `Host` alone puts every object in a Region
under one key, which gives co-location on a single member rather than page routing, and
looks superficially similar in a short test.

**Whole-object and suffix requests get their own buckets.** A `GET` with no `Range` hashes
to `full`, and a `bytes=-N` suffix request to `tail`, because a suffix range has no
computable page index without knowing the object size. Sending all of an object's footer
reads to one member is the useful behaviour for columnar formats.

**Match the page size to the cache.** 16 MiB is the cache's default. If you have set a
different `page_size` on a `cache_rules.json` rule, use the same value in the `div()`
converter. A mismatch costs alignment quality, not correctness: a router page smaller than
the cache page lets two members widen the same page and fetch it twice.

## More than one router requires `hash-key addr`

Set it on every server line. Without it, routers do not agree on which member owns a page,
and affinity only works among clients that happen to share a router: for sidecars, that
means within a single host. This applies to every sidecar deployment and to any highly
available central pair.

The reason is that the hash ring is built from server identity by default, so slot `m1` on
one router and `m1` on another point at different addresses whenever DNS returned them in a
different order. The disagreement is then total rather than marginal. `hash-key addr` keys
the ring on the server's address instead, making the mapping independent of ordering.

It requires HAProxy 2.9 or later; check your distribution's version before planning a
rollout.

## Bounded load, so a hot page does not pin one member

Plain consistent hashing sends every request for a hot page to one member with no relief.
`hash-balance-factor` caps a member's share of in-flight requests at `factor/100` times the
average and spills the excess to the next member in the ring.

The factor is the dial between concentration and spread. With 24 concurrent requests for a
single page against three members, the average is 8, so:

| `hash-balance-factor` | Distribution | Members used |
|---|---|---|
| absent | 24 | 1 |
| 150 (cap 12) | 12 / 12 | 2 |
| 110 (cap 9) | 9 / 9 / 6 | 3 |

Requests that spill still succeed, and what a spill costs depends on where the cache lives.

**On a shared cache volume it is nearly free.** The spill target reads the same bytes off the same
volume, so a spill costs some cache locality and not correctness. 150 is a reasonable starting
point. Do not tune it by watching a single burst; concentration only appears under genuine
concurrency, and a sequential test will show one member regardless of the setting.

**On [local NVMe](LOCAL_NVME_CACHE.md) it is billable, and the recommendation inverts: leave the
directive out.** The spill target has none of those bytes, so it fetches from the origin and keeps
its own copy. Measured on a four-instance fleet: duplicate copies are exactly linear in the members
the factor admits, 1.00 / 3.00 / 4.00 copies at absent / 150 / 110. That is per-GB origin traffic
plus lost cache capacity, arriving precisely when load is highest.

**Either way, be deliberate about multipart uploads.** Every operation of one upload —
`CreateMultipartUpload`, every `UploadPart`, and Complete — carries the object in `path` and no
`Range` header, so they all share the `full` routing key and one member owns the whole upload.
Bounded load will spill parts off that owner: ten parts in flight across three members average 3.3,
so a factor of 150 caps the owner at 5 and spills the rest, and one large file through the AWS CLI
reaches ten at the default `max_concurrent_requests`. Split parts and the object is not cached, even
though S3 still completes the upload — see
[MULTIPART_UPLOAD.md](MULTIPART_UPLOAD.md#multi-instance-deployments).

Omitting the directive keeps an upload together and caps a single object's write throughput at one
member's capacity, since nothing else can absorb it. That trade is not avoidable by tuning; it is
what one-owner-per-key means for a write.

## Discovery

`server-template` creates a fixed pool of slots filled from DNS, so members can be added
and removed without editing the configuration. With `init-addr none` HAProxy starts even
when the name does not resolve yet.

Size the template above your expected fleet. Unused slots sit idle and report
`MAINT (resolution)`, which is normal and not a fault. Removing an address returns that
slot to the same state, and HAProxy picks the change up within the `hold valid` window with
no restart or reload.

SRV records also work and carry the port, which is useful if members do not share one.
Plain A records are simpler when they do.

**Locality is settled here, not at routing time.** To keep traffic within a site or
Availability Zone, point the router at a discovery name that resolves only to local
members. The candidate set is then local by construction, and the routing key never has to
know where anything is.

## Health checking

Discovery finds members; health checks decide which of them get traffic.
`option httpchk GET /health` with `http-check expect status 200` polls each instance's
[health endpoint](CONFIGURATION.md#health-check-configuration), and `check port 8080` sends
that poll to the health port rather than the data port. An instance answers 200 only while
it considers its own cache, connection pool, and compression subsystems healthy, so this is
a real readiness signal rather than a bare TCP probe.

A member that fails its checks is taken out of rotation and the ring hands its pages to the
next member. That reassignment is deterministic, so every router picks the same replacement
and affinity holds through the failure. When the member passes again, its pages return to
it.

Planned maintenance uses the same path: an instance that stops answering 200 is drained by
every router within a check interval, so taking one out for an upgrade needs no router-side
change. If you would rather drive it from the router, HAProxy's runtime API can set a server
to `drain` or `maint` directly.

This is the same job a load balancer's health check does, and it is the reason a central
router can replace an existing balancer tier rather than sit behind one.

## Certificate validation on the fleet hop

The router is the TLS client on this hop, so `ssl verify required ca-file …` is what
validates the certificate each cache instance presents. Do not ship `verify none`.

Point `ca-file` at whatever signed those certificates. Certificates from your own PKI or a
public CA need no file of their own: use `@system-ca` and HAProxy verifies against the
system trust store. A self-signed fleet certificate is its own CA file, and then every
router needs a copy.

Either way, give every instance the same certificate, and make sure its SANs cover the addresses or
names HAProxy connects to. On a shared cache volume the simplest place to keep it is that volume. A
[local NVMe fleet](LOCAL_NVME_CACHE.md) has no shared volume, so distribute it the same way you
distribute `cache_rules.json` — configuration management, or a fetch from S3 in userdata. Do not put
it on the instance store, which is erased whenever an instance stops.

With a certificate that does not validate, requests fail closed: HAProxy returns 503 and
logs the server-side connection error, and no data is returned. That is the correct
behaviour, but see the trap below.

### A broken fleet certificate can leave health checks green

`check port 8080` health-checks the proxy's health endpoint in **cleartext**, on a
different port from the data path. It therefore does not exercise the TLS connection at
all. If the fleet certificate is rotated and `ca-file` goes stale, every client request
fails with 503 while the stats page still shows every member `UP / L7OK`.

Two ways to handle it:

- **Keep the cleartext liveness check and verify the data path separately.** Simplest, and
  the health check keeps reporting on the endpoint that actually reports proxy health.
  After any certificate change, make a real request through the router rather than trusting
  the stats page.
- **Make the check TLS-aware**, so a certificate problem takes the member out of rotation:

  ```haproxy
  http-check expect status 502
  server-template m 1-16 cache-fleet.internal:3129 check port 3129 check-ssl ca-file /etc/haproxy/tls/fleet-ca.pem resolvers fleetdns init-addr none ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
  ```

  With a bad CA this correctly marks members `DOWN / L6RSP`. The cost is that it expects a
  502 from the forward-proxy listener for a non-proxied request, which is a less stable
  contract than a 200 from `/health`.

Whichever you choose, alarm on the client-visible 503 rate. It is the signal that does not
depend on the health check being right.

## What this does not do

- **It does not add a new encryption capability.** Each instance already terminates TLS on
  its own listener, so a client pointed at a single instance, or at a load balancer that
  re-encrypts to one, gets an encrypted hop with full caching and no router. What a router
  adds is that encryption *plus* member selection for a fleet, without a managed load
  balancer.
- **It does not coordinate across routers at runtime.** Routers agree because they hash the
  same key over the same member addresses, not because they talk to each other. Brief
  disagreement during membership change costs a duplicate fetch rather than correctness. On a
  shared cache volume that fetch is a read off the volume; on [local NVMe](LOCAL_NVME_CACHE.md)
  it is an origin fetch and a second cached copy, so the window costs transfer as well as
  latency.
- **It reads the health endpoint as a signal, not as data.** [Health
  checking](#health-checking) covers liveness well, and taking an instance out of service is
  just a matter of it failing its own check. What `option httpchk` cannot do is read *values*
  out of the response. Anything needing a number or a field is therefore out of reach:
  weighting members by reported load, or detecting that two routers disagree about
  membership.
  Convergence is bounded by DNS and check-interval timing rather than being immediate.

## Verifying a deployment

```bash
# Members discovered and healthy
curl -s 'http://127.0.0.1:9000/;csv' | grep '^cache_fleet,m' | cut -d, -f2,18,37

# A signed request survives the hop, and which member served it
HTTP_PROXY=http://127.0.0.1:8080 NO_PROXY=169.254.169.254 \
  aws s3api get-object --bucket <bucket> --key <key> \
  --endpoint-url http://s3.<region>.amazonaws.com --region <region> /tmp/out
journalctl -u haproxy -n1 --no-pager
```

Check that signed `PUT`, `GET`, and ranged `GET` all return byte-exact data, that repeated
requests for one range reach the same member, and that taking a member out reroutes its
pages without a client-visible error.

**With more than one router, check they agree.** Send the same ranged request through two
of them and confirm both reach the same member. If they do not, `hash-key addr` is missing
or the routers disagree about the member list.
