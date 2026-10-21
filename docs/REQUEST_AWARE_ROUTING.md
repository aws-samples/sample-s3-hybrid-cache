# Request-aware routing with HAProxy

A fleet of Hybrid Cache for Amazon S3 proxies needs something in front of it that picks a
proxy for each request and stops sending to one that fails. [HAProxy](https://www.haproxy.org/),
a third-party open-source load balancer not affiliated with or endorsed by AWS, can do that
and also route each request by object and byte range. This page gives a configuration and
says when each of the router's three uses applies.

**An encrypted client hop across a fleet.** The recommended client setting points
`HTTP_PROXY` at a proxy's TLS proxy listener (port 3129), and a client neither spreads
requests across several addresses behind that name nor fails over between them. A router
gives those clients one address, terminates their TLS, and re-encrypts to each proxy's TLS
proxy listener, without a managed load balancer. The DNS alternative spreads clients across
the fleet but sends each request in cleartext; see
[What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes).

**No per-GB balancer charge on AWS.** A Network Load Balancer bills for the bytes it
processes, cache hits included, although those bytes never came from S3. Caching reduces the
S3 transfer bill but not the balancer's, so the balancer's share of the spend grows with the
hit rate. A router on each client host takes the balancer out of the data path.

**Routing by object and byte range.** A Layer 4 balancer picks a proxy per connection, so the
same hot bytes end up in several proxies' RAM caches, and simultaneous misses for them reach
S3 more than once. Hashing on the object and page gives each page one owner, so the fleet's
RAM holds more distinct data and concurrent readers of one range share a fetch.

Everything on this page is sample code for you to evaluate, not a supported configuration.
[Configure client routing](GETTING_STARTED.md#3-configure-client-routing) covers the simpler
options and is the better starting point. Deploying and operating HAProxy is your
responsibility; its [documentation](https://www.haproxy.org/#docs) covers anything beyond the
configuration given here.

## What affinity adds

An HAProxy tier configured this way is an **affinity router**: it hashes each request to one
proxy by object key and page index, so every read of a given page reaches the same proxy.
[Local NVMe cache fleets](LOCAL_NVME_CACHE.md) use the same term, and there the router is a
prerequisite rather than an optimization.

Routing by object and page buys two things.

**RAM cache efficiency, on every repeat read.** Each proxy keeps its own RAM cache. Without
affinity, a hot range is served by whichever proxy the connection landed on, so the same
bytes are resident in several proxies' RAM at once. With every read of a page sent to one
owner, each page occupies RAM once, so a fleet of N proxies holds up to N times as much
distinct hot data in RAM. This needs no concurrency and no page-aligned range caching.

**Concurrent-miss coalescing, when reads overlap in time.** A proxy deduplicates in-flight
misses within its own process, so coalescing applies only when several readers want the same
bytes at the same time. Reuse spread over minutes is already served from the cache volume. If
coalescing is your main reason to deploy a router, measure duplicate upstream fetches under
the real workload.

[Page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching) is opt-in per key
pattern, and the proxy skips it when the client signs the `Range` header, as the AWS CLI and
the official SDKs do. Those clients still get one owner per page, the RAM saving above, and
coalescing of byte-identical concurrent ranges. They do not get adjacent ranges merged into
one upstream fetch.

## Where the router runs

The routing configuration is the same wherever the router runs. Only the `bind` line and what
fronts it change.

| | On-premises | AWS |
|---|---|---|
| **Central** | Recommended. Nothing meters the hop, so affinity costs a component to operate rather than a per-GB charge: two HAProxy instances behind a floating address ([High availability](#high-availability)), or the load-balancing tier you already operate. Encrypts the client hop. | Not recommended. It needs a load balancer in front, which brings back the per-GB charge the router was meant to avoid. |
| **Sidecar on each client host** | Works, and needs no client TLS because the cleartext hop never leaves the host. Costs a process and a configuration file on every host, plus the fleet's CA certificate if it is self-signed. | The only way to get affinity without paying for a load balancer in front. |

### The router's certificate

A central router terminates client TLS, so give it a certificate that the verifying clients
already trust: one from your own PKI, or a publicly trusted one from ACM or Let's Encrypt.
Those clients then need no change. Certificate loading, rotation and ACME integration are
HAProxy's concern, and its [documentation](https://www.haproxy.org/#docs) covers them.

Only the AWS CRT transfer client verifies the router's certificate; botocore-based clients
(`aws s3api`, `aws s3` with the classic transfer client, boto3) encrypt the hop without
verifying it. For them the hop is not protected against an active attacker who can
impersonate the router, whatever certificate you install. See the
[per-client table](GETTING_STARTED.md#configuring-clients-to-trust-the-certificate).

A self-signed certificate is the fallback, and every client host that verifies then needs a
copy. Install it in the system trust store. `AWS_CA_BUNDLE` applies to the S3 endpoint, not
to the proxy connection.

Whichever you use, the certificate needs a SAN that covers the address or name clients dial.
A client that verifies rejects a certificate without one.

A sidecar needs none of this: it binds to loopback, so there is no client TLS to terminate.

### Why a central router needs a virtual address

With the DNS alternative, clients resolve the S3 hostname to several proxy addresses, and S3
clients built on the AWS Common Runtime (the AWS CLI v2 and current SDKs) spread requests
across the returned addresses and retry a different one when a connection fails.
[Comparing the fleet routing options](GETTING_STARTED.md#comparing-the-fleet-routing-options)
sets this against a Layer 4 load balancer.

**`HTTP_PROXY` gets none of that.** The variable takes one URL. The client resolves its host
once per connection, and neither spreads across several A records nor retries a sibling
address on failure. Publishing several addresses for a central router therefore adds no
failover: the CRT's multi-value handling applies to the destination it signs for, not to the
`HTTP_PROXY` host. A central router needs one address that survives the loss of an HAProxy
instance.

### High availability

The two placements fail differently, and the sidecar has the smaller blast radius.

**Central** needs at least two HAProxy instances sharing one address, and open-source HAProxy
leaves that to a separate tool. The common choice is a floating address managed by
[keepalived](https://www.keepalived.org/), which implements VRRP; HAProxy's commercial editions
ship
[their own VRRP module](https://www.haproxy.com/documentation/haproxy-enterprise/administration/high-availability/active-standby/)
for the same job. Pick whichever suits your environment, and treat it as infrastructure
outside this configuration.

A failover between the two keeps affinity, provided both carry
[`hash-key addr`](#more-than-one-router-requires-hash-key-addr).

**Sidecar** needs no network-layer HA. Each host has its own router, so a failure affects
that host alone. Availability comes from process supervision:

```ini
[Service]
Restart=on-failure
RestartSec=2
```

A dead sidecar takes its host out of service completely, with no failover, so the
supervision has to be reliable. Weigh that against a central tier whose failure every client
shares.

## Client configuration

Without a router, point clients at the TLS proxy listener: set
`HTTP_PROXY=https://<proxy-host>:3129` and `--endpoint-url http://s3.<region>.amazonaws.com`.
The client hop is encrypted and responses are still cached, on-premises and across Regions.
Where the network segment between clients and proxies is secure and cleartext on it is
acceptable, a DNS zone that resolves S3's hostnames to the proxies is a simpler alternative:
clients need no proxy setting and connect on port 80.
[Configure client routing](GETTING_STARTED.md#3-configure-client-routing) covers both.

With a router, clients keep the `http://` S3 endpoint and point `HTTP_PROXY` at the router
instead of a proxy. For a sidecar:

```bash
export HTTP_PROXY=http://127.0.0.1:8000  # Plaintext to loopback; HAProxy re-encrypts to the fleet
export AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com
export NO_PROXY=169.254.169.254  # The instance metadata service
```

For a central router, set `HTTP_PROXY=https://<router-host>:8443` instead.

- **The `http://` S3 endpoint is required.** It makes the client send an absolute-form
  request that HAProxy can read. An `https://` endpoint produces a `CONNECT` tunnel, which
  the router cannot route by object and the proxies cannot cache.
- **`NO_PROXY` is required**, at least for the instance metadata service.
  [Scope `HTTP_PROXY` to S3 clients](#scope-http_proxy-to-s3-clients) covers what else
  belongs on it.
- **No certificate for an AWS-owned hostname is involved.** When the client uses TLS, the
  session terminates on the router's own name. SigV4 signs the `Host` header, not the
  scheme, so the signature stays valid through to S3.
- **Central placement needs a client that can speak TLS to a proxy**, or the request fails.
  For the AWS CLI that means a current version from the bundled installer, which ships its
  own HTTP stack; a build linked against an old system `urllib3` cannot. `aws --version`
  reports `exe/…` for a bundled install and `source/…` for one that uses system libraries.

### Scope `HTTP_PROXY` to S3 clients

The configuration below sends hosts ending in `.amazonaws.com` to the fleet and refuses
everything else with a 502. A router whose targets are the proxies has nowhere sensible to
send other traffic, and forwarding it to an S3 address would be worse than refusing it.

`HTTP_PROXY` is often set for the whole host, so every HTTP request on the host would arrive
at the router. Set the variable for the S3 client only, or list everything else in
`NO_PROXY`. Run a separate forward proxy for other traffic.

## HAProxy configuration

Requires HAProxy 2.9 or later, for `hash-key addr`.

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
    bind 127.0.0.1:8000
    # Central placement: terminate client TLS instead. Clients that verify must
    # trust this certificate; see "The router's certificate".
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

    # Overrides the defaults block: a redispatched retry goes to a different member,
    # which an affinity backend must not do. See below.
    no option redispatch

    # hash-key addr is required whenever more than one router exists.
    server-template m 1-16 cache-fleet.internal:3129 check port 8080 resolvers fleetdns init-addr none ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr

# Anything not addressed to the fleet. See "Scope HTTP_PROXY to S3 clients".
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

Replace `cache-fleet.internal` with a name that resolves to your proxies' addresses, and
`10.0.0.2:53` with a resolver that can answer for it.

Without a DNS name for the fleet, replace the template with static server lines. Keep
`hash-key addr` on every line, and use the same addresses on every router:

```haproxy
    server m1 10.0.1.11:3129 check port 8080 ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
    server m2 10.0.1.12:3129 check port 8080 ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
    server m3 10.0.1.13:3129 check port 8080 ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
```

Validate the configuration before reloading, and check the exit status; piping the output to
`tail` hides a failure:

```bash
haproxy -c -f /etc/haproxy/haproxy.cfg && systemctl reload haproxy
```

### Why the backend turns off `redispatch`

`option redispatch` in the `defaults` block sends a retried request to a different server,
which suits most backends. In an affinity backend one member owns each page, so a
redispatched retry reaches a member that does not own it. With the RAM cache enabled, that member keeps a second copy of
the page in RAM, and on [local NVMe](LOCAL_NVME_CACHE.md) it also fetches the bytes from the
origin. The `cache_fleet` backend therefore sets `no option redispatch`, and a retry goes back
to the owner.

`retries 2` stays, because HAProxy's default `retry-on` is `conn-failure`: a retry fires only
when the connection could not be established, before any of the request body has been
forwarded. It rides out a transient connection failure without replaying anything.

**If you widen `retry-on`, exclude writes.** Response-based conditions (`empty-response`,
`response-timeout`, `503` and so on) let a retry fire after the body has started moving, and
HAProxy cannot replay a request body it has already streamed. A large `PUT` or `UploadPart`
then fails instead of being retried.

### Deploying a sidecar

A sidecar handles two hops, and only the first is plaintext. It accepts HTTP on loopback,
where nothing crosses the network, then opens TLS to each proxy's TLS proxy listener on port
3129. That outbound leg is why the server lines carry `ssl verify required`, and it keeps the
fleet hop encrypted.

Install HAProxy from your distribution, install the configuration, and run HAProxy under a
supervisor. Port 8000 is not a privileged port, so HAProxy does not need root to bind it.

Verifying the fleet's certificates is the part that grows with the number of hosts. If those
certificates come from your own PKI or a public CA, point `ca-file` at the system trust store
and there is nothing to distribute:

```haproxy
    ... ssl verify required ca-file @system-ca hash-key addr
```

A self-signed fleet certificate must instead be copied to every host and rotated there. That
and keeping the configuration identical on every host are the operational costs of this
placement.

## How the routing key works

The routing key is `(object path, floor(range_start / 16 MiB))`.

Requests for one page converge on one member. Different pages of the same object still
spread across the fleet, so a parallel multi-range download keeps its fan-out.

**Include the path in the key.** Hashing on `Host` alone puts every object in a Region under
one key, so one member serves them all. A short test of that setup can look like page
routing.

**Whole-object and suffix requests get their own keys.** A `GET` with no `Range` hashes to
`full`, and a `bytes=-N` suffix request to `tail`, because a suffix range has no page index
until the object size is known. Sending all of an object's footer reads to one member suits
columnar formats.

**Match the page size to the cache.** The cache's default page size is 16 MiB (`16777216`).
If a [cache rule](CONFIGURATION.md#cache-rules) sets a different `page_size`, use the same
value in the `div()` converter. A mismatch costs alignment, not correctness: a router page
smaller than the cache page lets two members widen the same page and fetch it twice.

## More than one router requires `hash-key addr`

Set it on every server line. Without it, routers disagree about which member owns a page, and
affinity holds only among clients that share a router; for sidecars, that means within one
host. This applies to every sidecar deployment and to a highly available central pair.

By default HAProxy builds the hash ring from server identity, so slot `m1` on one router and
`m1` on another point at different addresses whenever DNS returned them in a different order.
The routers then disagree about almost every page. `hash-key addr` builds the ring from each
server's address instead, so the mapping does not depend on that order.

## Bounded load, so a hot page does not pin one member

Plain consistent hashing sends every request for a hot page to one member.
`hash-balance-factor` caps a member's share of in-flight requests at `factor/100` times the
average and sends the excess to the next member in the ring.

The factor sets the balance between concentration and spread. With 24 concurrent requests
for one page against three members, the average is 8:

| `hash-balance-factor` | Distribution | Members used |
|---|---|---|
| absent | 24 | 1 |
| 150 (cap 12) | 12 / 12 | 2 |
| 110 (cap 9) | 9 / 9 / 6 | 3 |

A request that spills still succeeds. What a spill costs depends on where the cache lives.

**When the fleet shares a cache volume, a spill costs little.** The spill target reads the
same bytes off the same volume, so a spill costs some RAM locality, not correctness. 150 is a
reasonable starting point. Tune it under real concurrency: concentration appears only when
requests overlap, so a sequential test shows one member whatever the setting.

**On [local NVMe](LOCAL_NVME_CACHE.md), a spill costs origin transfer, so leave the directive
out.** The spill target holds none of the page, so it fetches the page from the origin and
keeps its own copy. Each extra member the factor admits for a page holds another copy. That
is per-GB origin traffic and lost cache capacity, arriving when load is highest.

**On local NVMe, leaving the directive out also keeps a multipart upload together.** Every
request of one upload (`CreateMultipartUpload`, each `UploadPart`, and
`CompleteMultipartUpload`) carries the object key in its path and no `Range` header, so all of
them share the `full` routing key and one member owns the upload. Bounded load spills parts
off that owner. Ten parts in flight across three members average 3.3, so a factor of 150 caps
the owner at 5 and spills the rest. One large file through the AWS CLI reaches ten parts in
flight at its default `max_concurrent_requests` of 10.

With a separate cache on each proxy, split parts mean the object is not cached, although S3
still completes the upload. When the fleet shares a cache volume the parts still assemble,
because every member writes its part records to that volume. See
[Multipart uploads on a fleet](MULTIPART_UPLOAD.md#multi-instance-deployments).

Keeping an upload on one member caps a single object's write throughput at that member's
capacity.

## Discovery

`server-template` creates a fixed pool of slots filled from DNS, so members can be added and
removed without editing the configuration. With `init-addr none`, HAProxy starts even when the
name does not resolve yet.

Size the template above your expected fleet. Unused slots sit idle and report
`MAINT (resolution)`, which is normal. Removing an address returns that slot to the same
state, and HAProxy picks up the change within the `hold valid` window, with no restart or
reload.

SRV records also work and carry the port, which helps if members do not share one. Plain A
records are simpler when they do.

**Keep traffic local through discovery.** To keep traffic within a site or Availability Zone,
point the router at a discovery name that resolves only to local members. The candidate set
is then local, and the routing key needs no location.

## Health checking

Discovery finds members; health checks decide which of them get traffic.
`option httpchk GET /health` with `http-check expect status 200` polls each proxy's
[health endpoint](CONFIGURATION.md#health-check-configuration), and `check port 8080` sends
that poll to the health port (default `8080`) rather than the data port. The proxy answers
503 when its cache, connection pool or compression check reports unhealthy, and 200
otherwise, including while it reports itself degraded. The check is therefore a readiness
signal, not a bare TCP probe. The health listener binds `127.0.0.1` unless `health.bind_address` is set, so set it to `0.0.0.0` or the proxy's address, or every member fails the check.

A member that fails its checks leaves rotation, and the ring hands its pages to the next
member. Every router picks the same replacement, so affinity holds through the failure. When
the member passes again, its pages return to it.

Planned maintenance uses the same path. Every router drains a proxy that stops answering 200
after three failed checks at HAProxy's defaults, so taking a proxy out for an upgrade needs no
router-side change. To drive it from the router instead, HAProxy's runtime API can set a
server to `drain` or `maint`.

A load balancer's health check does the same job, which is why a central router can replace a
balancer tier rather than sit behind one.

## Certificate validation on the fleet hop

The router is the TLS client on this hop, so `ssl verify required ca-file …` validates the
certificate each proxy presents. Keep `verify required`; `verify none` accepts any
certificate.

Point `ca-file` at whatever signed those certificates. For certificates from your own PKI or a
public CA, use `@system-ca`, and HAProxy verifies against the system trust store. A
self-signed fleet certificate is its own CA file, and every router needs a copy.

Give every proxy the same certificate, with SANs that cover the addresses or names HAProxy
connects to. When the fleet shares a cache volume, the simplest place to keep the certificate
is that volume. A [local NVMe fleet](LOCAL_NVME_CACHE.md) has no volume that every proxy reads,
so distribute the certificate the same way as `cache_rules.json`: with configuration management, or a fetch
from S3 in user data. Keep it off the instance store, which is erased whenever an instance
stops.

If the certificate does not validate, requests fail closed: HAProxy returns 503, logs the
server-side connection error, and returns no data. That is correct, but see the trap below.

### A broken fleet certificate can leave health checks green

`check port 8080` sends the health check to the proxy's health endpoint in cleartext, on a
different port from the data path, so the check never exercises the TLS connection. If the
fleet certificate is rotated and `ca-file` goes stale, every client request fails with 503
while the stats page still shows every member `UP / L7OK`.

Two ways to handle it:

- **Keep the cleartext check and test the data path separately.** This is simpler, and the
  check keeps reading the endpoint that reports proxy health. After any certificate change,
  make a real request through the router rather than trusting the stats page.
- **Make the check TLS-aware**, so a certificate problem takes the member out of rotation:

  ```haproxy
  http-check expect status 400
  server-template m 1-16 cache-fleet.internal:3129 check port 3129 check-ssl ca-file /etc/haproxy/tls/fleet-ca.pem resolvers fleetdns init-addr none ssl verify required ca-file /etc/haproxy/tls/fleet-ca.pem hash-key addr
  ```

  With a bad CA this marks members `DOWN / L6RSP`. The check sends `GET /health` with no
  `Host` header to the TLS proxy listener, which answers `400 MissingHostHeader`, so the check
  passes on a 400. That is a less stable contract than a 200 from `/health`.

Whichever you choose, alarm on the client-visible 503 rate. That signal does not depend on the
health check being right.

## Limits

- **A router adds no new encryption.** Each proxy already terminates TLS on its TLS proxy
  listener, so a client pointed at one proxy, or at a load balancer that re-encrypts to one,
  gets an encrypted hop with full caching and no router. A router adds member selection for a
  fleet behind that encrypted hop, without a managed load balancer.
- **Routers do not coordinate at runtime.** They agree because they hash the same key over
  the same member addresses. During a membership change they can briefly disagree, which
  costs a duplicate fetch, not correctness. When the fleet shares a cache volume, that fetch
  is a read off the volume; on [local NVMe](LOCAL_NVME_CACHE.md) it is an origin fetch and a
  second cached copy. Convergence takes as long as DNS and the check interval allow.
- **The health check reads a status code, not values.** [Health checking](#health-checking)
  covers liveness, and taking a proxy out of service only needs it to fail its own check.
  `option httpchk` cannot read values out of the response, so a router cannot weight members
  by reported load or detect that two routers disagree about membership.

## Verifying a deployment

```bash
curl -s 'http://127.0.0.1:9000/;csv' | grep '^cache_fleet,m' | cut -d, -f2,18,37  # Members discovered, and their status
HTTP_PROXY=http://127.0.0.1:8000 NO_PROXY=169.254.169.254 \
  aws s3api get-object --bucket <bucket> --key <key> \
  --endpoint-url http://s3.<region>.amazonaws.com --region <region> /tmp/out  # A signed request survives the hop
journalctl -u haproxy -n1 --no-pager  # Which member served it
```

Check that signed `PUT`, `GET` and ranged `GET` requests all return byte-exact data, that
repeated requests for one range reach the same member, and that taking a member out reroutes
its pages without a client-visible error.

**With more than one router, check that they agree.** Send the same ranged request through two
of them and confirm both reach the same member. If they do not, `hash-key addr` is missing or
the routers disagree about the member list.

## See also

- [Local NVMe cache fleets](LOCAL_NVME_CACHE.md), where the router is a prerequisite
- [Configure client routing](GETTING_STARTED.md#3-configure-client-routing)
- [What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes)
- [Page-aligned range caching](CACHE_READ_PATHS.md#page-aligned-range-caching)
