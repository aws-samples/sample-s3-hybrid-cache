# Getting started with Hybrid Cache for Amazon S3

Quick start guide for installing, configuring, and running Hybrid Cache for Amazon S3.

## Table of contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [1. Clone and build](#1-clone-and-build)
  - [Binary portability](#binary-portability)
  - [Upgrading](#upgrading)
  - [2. Start the proxy](#2-start-the-proxy)
  - [3. Configure client routing](#3-configure-client-routing)
    - [Option A: HTTP_PROXY with the TLS proxy listener (recommended)](#option-a-http_proxy-with-the-tls-proxy-listener-recommended)
    - [Option B: Layer 4 load balancer](#option-b-layer-4-load-balancer)
    - [Option C: DNS zone for S3 hostnames (secure network segments)](#option-c-dns-zone-for-s3-hostnames-secure-network-segments)
    - [Option D: hosts file (testing and development)](#option-d-hosts-file-testing-and-development)
    - [Comparing the fleet routing options](#comparing-the-fleet-routing-options)
    - [S3 PrivateLink (interface VPC endpoints)](#s3-privatelink-interface-vpc-endpoints)
    - [Access point and MRAP endpoint requirements](#access-point-and-mrap-endpoint-requirements)
    - [Access point alias usage](#access-point-alias-usage)
    - [MRAP alias usage](#mrap-alias-usage)
    - [Access point endpoint summary](#access-point-endpoint-summary)
- [Basic usage](#basic-usage)
  - [Upload and download files](#upload-and-download-files)
- [Simplifying client configuration](#simplifying-client-configuration)
  - [AWS CLI environment variable](#aws-cli-environment-variable)
  - [s5cmd configuration](#s5cmd-configuration)
  - [MinIO Go client (Forgejo, Gitea, etc.)](#minio-go-client-forgejo-gitea-etc)
  - [boto3 per-client configuration](#boto3-per-client-configuration)
  - [Mountpoint for Amazon S3](#mountpoint-for-amazon-s3)
  - [Shell profile configuration](#shell-profile-configuration)
  - [Check cache performance](#check-cache-performance)
- [Port configuration options](#port-configuration-options)
  - [Standard ports (80/443)](#standard-ports-80443)
  - [Custom ports](#custom-ports)
- [Proxy-only mode](#proxy-only-mode)
  - [Configuration](#configuration)
  - [Client configuration](#client-configuration)
  - [High availability: shared NFS cache pattern](#high-availability-shared-nfs-cache-pattern)
- [Fronting a plaintext or non-standard store (upstream_overrides)](#fronting-a-plaintext-or-non-standard-store-upstream_overrides)
  - [Local-dev recipe: plaintext MinIO on :9000](#local-dev-recipe-plaintext-minio-on-9000)
  - [Fronting real S3 over plaintext HTTP](#fronting-real-s3-over-plaintext-http)
  - [Cache-key caveat: the origin is not part of the key](#cache-key-caveat-the-origin-is-not-part-of-the-key)
  - [Security: local development and secure network segments only](#security-local-development-and-secure-network-segments-only)
- [Configuration files](#configuration-files)
- [Verification](#verification)
  - [Test basic functionality](#test-basic-functionality)
  - [Monitor logs](#monitor-logs)
- [Troubleshooting](#troubleshooting)
  - [Common issues](#common-issues)
  - [Debug mode](#debug-mode)
- [Next steps](#next-steps)
- [System requirements and sizing](#system-requirements-and-sizing)

## Prerequisites

- **Rust**: the toolchain version pinned in `rust-toolchain.toml`, which `rustup` reads. Only needed to build from source; the [prebuilt release binaries](#binary-portability) need none
- **OS**: Linux or macOS for building and running; production deployments typically run on Linux (systemd service, binary portability via glibc)
- **sudo access**: required to bind ports 80/443; not needed for proxy-only mode on port 3128
- **git**: to clone the repository

## Installation

### 1. Clone and build

To skip the build, download the prebuilt binary for your architecture from the [latest release](https://github.com/aws-samples/sample-s3-hybrid-cache/releases/latest), check it as [Binary portability](#binary-portability) describes, and use it wherever the steps below name `target/release/s3-proxy`.

```bash
# Clone repository
git clone https://github.com/aws-samples/sample-s3-hybrid-cache
cd sample-s3-hybrid-cache

# Build release binary
cargo build --release
```

### Binary portability

`cargo build --release` produces a single self-contained executable at `target/release/s3-proxy`. It statically links the Rust standard library and all crates, and dynamically links only against glibc and a small set of standard Linux libraries.

You can build on one host and copy the binary to others, provided:

- Same CPU architecture (x86_64 or aarch64; don't mix)
- Same operating system family (Linux)
- Target host's glibc version is the same or newer than the build host's

Each GitHub release from 3.0.0 publishes two prebuilt binaries, `s3-proxy-x86_64-linux` and `s3-proxy-aarch64-linux`, for Linux on x86_64 and aarch64. They are dynamically linked against glibc and need glibc 2.34 or newer, so they run on Amazon Linux 2023 and newer and do not run on Amazon Linux 2 (glibc 2.26). There are no musl, macOS or Windows builds. Check a download against the release's `SHA256SUMS` with `sha256sum -c --ignore-missing SHA256SUMS`, and check where it was built with `gh attestation verify <file> --repo aws-samples/sample-s3-hybrid-cache`. The builds are not bit-for-bit reproducible; the attestation records the repository and commit a binary was built from.

The binary has no bundled assets. At runtime it needs a configuration file, a `cache_dir`, log directories and, when the TLS proxy listener is enabled, a certificate and key. The configuration file supplies every path.

### Upgrading

Upgrading replaces the binary and restarts the proxy; existing configuration keeps working. Before upgrading a fleet to 3.0.0, read [Upgrading to 3.0.0](UPGRADING.md#upgrading-to-300). A release can also change a default, so [Upgrading](UPGRADING.md) lists every release that needs a manual step or changes a default; read the entries between your version and your target.

```bash
git pull                                                 # Sync latest source
cargo build --release                                    # Rebuild release binary
sudo cp target/release/s3-proxy /usr/local/bin/s3-proxy  # Replace binary
sudo systemctl restart s3-proxy                          # Restart service
/usr/local/bin/s3-proxy --version                        # Verify new version
```

If you build on a separate host, copy the binary to each target (see [Binary portability](#binary-portability)) and run only the last three commands. The proxy hosts need no Rust toolchain. To run the proxy in a container instead, see [Docker deployment](DOCKER.md).

The proxy logs its version and build timestamp at startup (`Starting Hybrid Cache for Amazon S3 server v<version> (built: <timestamp>)`), so `journalctl -u s3-proxy | grep Starting` confirms which binary is running.

### 2. Start the proxy

Run the compiled binary directly; no Rust toolchain is needed on the target host:

```bash
# Start on standard ports 80/443 (requires sudo)
sudo /usr/local/bin/s3-proxy -c /etc/s3-proxy/config.yaml
```

For development (requires Rust toolchain):

```bash
sudo cargo run --release -- -c config/config.example.yaml
```

The proxy starts on:
- **HTTP**: `<proxy-host>:80` (caching enabled)
- **HTTPS passthrough**: `<proxy-host>:443` (no caching)
- **TLS proxy listener**: `<proxy-host>:3129` (when enabled; decrypts and caches)
- **Health**: `<proxy-host>:8080/health`
- **Dashboard**: `<proxy-host>:8081` (real-time statistics)
- **Metrics**: `<proxy-host>:9090/metrics`

The monitoring listeners (health, dashboard and metrics) listen on `<proxy-host>` because `config/config.example.yaml` sets their `bind_address` to `0.0.0.0`. A config that omits `bind_address` binds them to `127.0.0.1`, reachable only from the proxy host.

#### Running as a systemd service (recommended for production)

Copy the provided unit file (or create `/etc/systemd/system/s3-proxy.service`):

```bash
sudo cp config/s3-proxy.service /etc/systemd/system/s3-proxy.service
```

The unit file includes systemd sandbox directives that restrict the proxy's file system and kernel access. Key hardening features:

- `NoNewPrivileges=true`: prevents privilege escalation via setuid/setgid binaries
- `ProtectSystem=strict`: mounts the entire file system read-only except explicit `ReadWritePaths`
- `ProtectHome=true`: hides `/home`, `/root`, `/run/user`
- `PrivateTmp=true`: isolates `/tmp` and `/var/tmp`
- `PrivateDevices=true`: removes access to physical devices
- `ProtectKernelTunables=true`: read-only `/proc/sys`, `/sys`
- `ProtectKernelModules=true`: blocks kernel module loading
- `ProtectControlGroups=true`: read-only cgroup file system
- `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX`: only TCP/UDP and Unix sockets
- `ReadWritePaths=/mnt/cache /mnt/nfs`: explicit allowlist for writable directories
- `StateDirectory=s3-proxy`: creates `/var/lib/s3-proxy` on the host's own disk and keeps it writable, for the default `logging.local_state_dir`

##### Directory prerequisites

The shipped unit's paths match [`docs/examples/config-production.yaml`](examples/config-production.yaml): `cache_dir` on `/mnt/cache`, logs under `/mnt/nfs`. The unit's `RequiresMountsFor=` line names the same two paths, so the service waits for those mounts. Before starting the service, create the directories if they are not already mounted volumes:

```bash
sudo mkdir -p /mnt/cache /mnt/nfs
```

If your `config.yaml` uses different paths for `cache_dir` or log directories, change both the `ReadWritePaths=` and `RequiresMountsFor=` lines in the unit file to match. Keep the `StateDirectory=s3-proxy` line; if you set `logging.local_state_dir` elsewhere, add that directory to `ReadWritePaths=` and keep it on the host's own disk. The proxy cannot write outside these paths when `ProtectSystem=strict` is active, and a missing path fails at write time rather than at startup.

##### Privilege tradeoff (User=root)

The shipped unit runs as `User=root` because binding ports 80 and 443 requires either root or `CAP_NET_BIND_SERVICE`. If your deployment uses proxy-only mode (port 3128) or places a load balancer in front that owns 80/443, you can drop to a dedicated user for stronger isolation:

```ini
User=s3proxy
Group=s3proxy
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
```

When switching to a non-root user, transfer ownership of the cache and log directories:

```bash
sudo useradd -r -s /usr/sbin/nologin s3proxy
sudo chown -R s3proxy:s3proxy /mnt/cache /mnt/nfs
```

##### Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable s3-proxy
sudo systemctl start s3-proxy
```

##### Verify

```bash
sudo systemctl status s3-proxy
journalctl -u s3-proxy --no-pager | tail -20
curl -s http://localhost:8080/health
```

The service restarts automatically on failure. Logs go to the systemd journal and are accessible via `journalctl -u s3-proxy`.

#### Proxy-only mode (no sudo required)

If you don't need ports 80/443, run in proxy-only mode on port 3128. No root privileges, no DNS changes, no hosts file edits:

```bash
/usr/local/bin/s3-proxy -c /etc/s3-proxy/config.yaml
```

With a config that sets:

```yaml
server:
  mode: "proxy_only"
  # proxy_port: 3128  (default)
```

Clients route traffic by setting an environment variable:

```bash
export HTTP_PROXY=http://127.0.0.1:3128  # A proxy on this host
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

This is the simplest setup, for development and for a proxy on the client's own host. See [Proxy-Only Mode](#proxy-only-mode) below for full details including TLS and shared-cache patterns.

**Security note**: The proxy connects to Amazon S3 over HTTPS whichever way clients connect, unless an [`upstream_overrides`](#fronting-a-plaintext-or-non-standard-store-upstream_overrides) entry declares a plaintext origin. Port 3128 carries cleartext, so give clients on other hosts the TLS proxy listener ([Option A](#option-a-http_proxy-with-the-tls-proxy-listener-recommended)), and restrict port 3128 with a firewall or security group.

### 3. Configure client routing

Clients reach the proxy in one of four ways. Point clients at the TLS proxy listener: set `HTTP_PROXY=https://<proxy-host>:3129` and `--endpoint-url http://s3.<region>.amazonaws.com`. The client hop is encrypted and responses are still cached, on-premises and across Regions. Where the network segment between clients and proxies is secure and cleartext on it is acceptable, a DNS zone that resolves S3's hostnames to the proxies is a simpler alternative. Clients need no proxy setting and connect on port 80.

- **Option A: HTTP_PROXY with the TLS proxy listener (recommended).** Two client settings and no DNS change. Point `HTTP_PROXY` at one proxy, or at [a name of your own](#fleet-behind-one-proxy-name) with one address record per proxy in a fleet. A proxy on the client's own host can use `HTTP_PROXY=http://127.0.0.1:3128`, because that hop stays on loopback.
- **Option B: Layer 4 load balancer.** One stable endpoint for a fleet, with health checks and per-connection balancing at the load balancer. Clients use Option A's settings, pointed at the load balancer.
- **Option C: DNS zone for S3 hostnames (secure network segments).** S3's own hostnames resolve to the proxies, so clients need no proxy setting. Requests to port 80 cross the network in cleartext, so use this option only where that segment is secure and cleartext on it is acceptable.
- **Option D: Hosts file (testing and development).** Edit `/etc/hosts` on one client. Use it only when the client cannot set `HTTP_PROXY`.

Options A to C spread clients across a fleet, and [Comparing the fleet routing options](#comparing-the-fleet-routing-options) sets them side by side. To send every request for one object and byte range to the same proxy, you need a Layer 7 router that reads each request; see [Request-aware routing](REQUEST_AWARE_ROUTING.md) and [Layer 4 or Layer 7?](#layer-4-or-layer-7).

#### Option A: HTTP_PROXY with the TLS proxy listener (recommended)

Route S3 traffic through the proxy with the `HTTP_PROXY` environment variable. The client sends requests with absolute URIs (`GET http://s3.amazonaws.com/bucket/key`), and the proxy takes the target host from the URI and sends the request through the caching pipeline.

A proxy on another host is reached through the TLS proxy listener: [Remote proxy](#remote-proxy) sets it up, and [Fleet behind one proxy name](#fleet-behind-one-proxy-name) spreads clients across a fleet. A proxy on the client's own host needs no TLS ([Localhost](#localhost-simplest)). For one address with server-side health checks, put a load balancer in front of the fleet ([Option B](#option-b-layer-4-load-balancer)).

##### Localhost (simplest)

When the proxy runs on the same host as the client, TLS is unnecessary: loopback traffic never leaves the machine. Run the proxy in [Proxy-Only Mode](#proxy-only-mode) on port 3128 (no sudo, no port 80/443 binding), then:

```bash
export HTTP_PROXY=http://127.0.0.1:3128
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

`HTTP_PROXY` selects the proxy connection. `--endpoint-url http://s3.<region>.amazonaws.com` selects a cacheable HTTP S3 endpoint; otherwise, the AWS CLI defaults to HTTPS and sends a CONNECT tunnel that the proxy cannot cache. The S3 hostname remains the request's signed `Host` value, so do not use the proxy hostname as the endpoint URL. For repeated S3 commands against buckets in one Region, set `AWS_ENDPOINT_URL_S3=http://s3.<region>.amazonaws.com` instead of passing `--endpoint-url` each time.

On EC2, or any host where you need to bypass IMDS or other IPs, set `NO_PROXY`:

```bash
export NO_PROXY=169.254.169.254  # Exclude IMDS from proxying
```

Without this on EC2, credential retrieval from the instance metadata service will fail because IMDS requests get forwarded through the proxy instead of going directly to `169.254.169.254`. Add other IPs or hostnames that should bypass the proxy to the same list.

##### Remote proxy

When the proxy is on another host, point `HTTP_PROXY` at the TLS proxy listener (port 3129), enabled as shown in [TLS proxy listener configuration](#tls-proxy-listener-configuration):

```bash
export HTTP_PROXY=https://<proxy-host>:3129
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

Where the network segment between client and proxy is secure and cleartext on it is acceptable, the HTTP listener (port 80) also works. SigV4 authenticates each request but does not encrypt it; see [What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes).

```bash
export HTTP_PROXY=http://<proxy-host>:80
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

On EC2, add `export NO_PROXY=169.254.169.254` to keep IMDS off the proxy path.

**Why `HTTP_PROXY`, not `HTTPS_PROXY`?** When `HTTPS_PROXY` is set, the SDK sends a `CONNECT` request asking the proxy to establish a TCP tunnel to S3. The traffic inside the tunnel is end-to-end encrypted between the client and S3: the proxy cannot decrypt, inspect, or cache it. The TLS proxy listener handles `CONNECT` as a passthrough tunnel (same as port 443), so the request succeeds but bypasses the cache entirely. Use `HTTP_PROXY` instead. The `HTTP_PROXY` variable controls where the SDK sends HTTP requests, regardless of whether the proxy URL uses `http://` or `https://`. With `HTTP_PROXY=https://proxy:3129`, the client opens a TLS connection to the proxy, then sends plaintext HTTP inside that tunnel. The proxy decrypts the TLS layer and sees the HTTP request for caching.

##### TLS proxy listener configuration

To use `HTTP_PROXY=https://...` against a remote proxy, enable the TLS proxy listener in your config:

```yaml
server:
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/proxy/tls/cert.pem"
    key_path: "/etc/proxy/tls/key.pem"
```

`tls_proxy_port` is separate from `https_port`. HTTPS passthrough (port 443) relays encrypted bytes without caching, while the TLS proxy listener terminates TLS with the proxy's own certificate and sends the decrypted HTTP through the caching pipeline.

##### Generating a self-signed certificate

For development or testing, generate a self-signed certificate with `openssl`:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=proxy-host" \
  -addext "subjectAltName=DNS:proxy-host,IP:10.0.1.101"
```

The `-addext "subjectAltName=..."` entries must match how clients connect to the proxy:
- `IP:10.0.1.101`: clients connecting by IP address (e.g., `HTTP_PROXY=https://10.0.1.101:3129`). Use this for direct connections to proxies.
- `DNS:proxy-host`: clients connecting by hostname (e.g., `HTTP_PROXY=https://proxy-host:3129`). Use this when clients connect through a DNS name or load balancer.

Add a SAN entry for every address or name clients use to reach the proxies. For example, for a fleet where clients connect by IP:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=s3-proxy" \
  -addext "subjectAltName=IP:10.0.1.101,IP:10.0.1.102,IP:10.0.1.103"
```

Or when clients connect through a load balancer or DNS name:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=s3-proxy.internal" \
  -addext "subjectAltName=DNS:s3-proxy.internal,DNS:*.s3-proxy.internal"
```

Without a matching SAN, clients that verify the proxy certificate reject it with a hostname verification error (see the next section for which clients verify).

##### Configuring clients to trust the certificate

The certificate belongs to the proxy, on the `HTTP_PROXY` hop, not to S3. Only the AWS CRT transfer client verifies the proxy's certificate; botocore-based clients (`aws s3api`, `aws s3` with the classic transfer client, boto3) encrypt the hop without verifying it:

| Client | Verifies the proxy certificate? | What makes it trust the certificate |
|---|---|---|
| AWS CLI `aws s3` commands with the AWS CRT transfer client | Yes. A certificate without a SAN matching the proxy name fails with `AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE`, and `--no-verify-ssl` does not change that | The system trust store |
| botocore-based clients: `aws s3api`, `aws s3` with the classic transfer client, boto3 | **No.** The connection to the proxy is encrypted but not authenticated, and urllib3 logs `InsecureRequestWarning: Unverified HTTPS request` for it | Nothing. No setting turns verification on for an `http://` endpoint |

botocore sets `CERT_NONE` on any connection whose target URL is `http://` and does not exempt the TLS session to an `https://` proxy, which in forwarding mode uses that same connection. `AWS_CA_BUNDLE`, `--no-verify-ssl` and `verify=` only matter for an `https://` endpoint URL. In botocore, the `proxies_config` `proxy_ca_bundle` setting applies only to a `CONNECT` tunnel, which an `http://` endpoint never opens. The AWS CLI does not read it from `~/.aws/config`. So for botocore-based clients, TLS on this hop protects against passive observation only, not against an active attacker who can impersonate the proxy.

The AWS CLI chooses its transfer client through `preferred_transfer_client` in the `s3` section of `~/.aws/config`; set it to `crt` for `aws s3` commands to verify the proxy. For clients that verify, install the certificate explicitly:

```bash
# Amazon Linux, RHEL, Fedora
sudo cp cert.pem /etc/pki/ca-trust/source/anchors/s3-proxy.crt && sudo update-ca-trust

# Debian, Ubuntu
sudo cp cert.pem /usr/local/share/ca-certificates/s3-proxy.crt && sudo update-ca-certificates
```

Then point the client at the TLS proxy listener, keeping `--endpoint-url` on `http://`:

```bash
export HTTP_PROXY=https://<proxy-host>:3129  # TLS to the proxy, not to S3
export NO_PROXY=169.254.169.254              # On EC2, keep IMDS off the proxy path
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

An `https://` endpoint URL would make the SDK open a `CONNECT` tunnel that bypasses the cache; see "Why `HTTP_PROXY`, not `HTTPS_PROXY`" above.

For production, prefer a certificate signed by your organization's internal CA, which needs no per-client configuration on hosts that already trust that CA.

##### Fleet behind one proxy name

`HTTP_PROXY` accepts a hostname, so a fleet can sit behind a single name with no load balancer. Point the variable at a name you own, and give that name one address record per proxy:

```bash
export HTTP_PROXY=https://s3-proxy.internal.example.com:3129
export NO_PROXY=169.254.169.254  # On EC2, keep IMDS off the proxy path
```

Give the record a short TTL, around 10 seconds, rather than zero. Zero stops resolvers caching the record, and caching is what produces the even distribution described below. Do not expect a low TTL to make failover quick, though: detecting the failure usually takes longer than clearing the address from caches, and a client holding an open connection does not re-resolve at all. On AWS both terms are quantified; see [DNS: Route 53 private hosted zones](AWS_DEPLOYMENT.md#dns-route-53-private-hosted-zones).

This uses DNS for a different purpose than [Option C](#option-c-dns-zone-for-s3-hostnames-secure-network-segments) does. Option C overrides S3's own hostnames, which means one zone per S3 hostname the fleet serves, apex `A` records repeated in each zone, and a wildcard `CNAME` so virtual-hosted bucket names resolve too. Here the client already knows to send everything to the proxy, so S3's hostnames stay untouched and one record set is the whole configuration. It also keeps the S3 hostname free, which avoids the collision with an interface endpoint's private DNS described under [S3 PrivateLink](#s3-privatelink-interface-vpc-endpoints).

One shared name also keeps the certificate simple: it needs a single `DNS:` SAN for the proxy name rather than an `IP:` SAN per proxy, so every proxy can present the same certificate. On a secure network segment where cleartext is acceptable, the same name works without TLS, for example `HTTP_PROXY=http://s3-proxy.internal.example.com:3128` with the proxies in `proxy_only` mode.

**Distribution depends on the resolver, so check yours.** A client that uses only the first address still spreads across the fleet when the resolver in front of it rotates the cached addresses. Many recursive resolvers do this; some return a fixed order, and the difference decides whether you get load balancing or only failover. Query the same name repeatedly and watch the TTL:

```bash
for i in $(seq 1 6); do
  dig @<your-resolver> s3-proxy.internal.example.com A +noall +answer \
    | awk '$4=="A"{printf "%s/%s ", $2, $5}'; echo; sleep 1
done
```

A descending TTL proves the answer came from cache rather than a re-fetch, so any change of order across those responses is the resolver rotating its cached copy. Watch the TTL rather than just the order: a DNS provider that randomizes its authoritative answers makes the first query look balanced whether the resolver rotates or not. A fixed order alongside a descending TTL means no rotation, so every client behind that resolver reaches the same proxy until the record expires: failover, but no balancing.

Where a resolver does rotate, clients starting at the same moment spread across the fleet, because each lookup advances a shared position. On AWS this holds under a concurrent start; see [DNS: Route 53 private hosted zones](AWS_DEPLOYMENT.md#dns-route-53-private-hosted-zones), which also covers the resolver query quota a simultaneous start must fit inside.

Two properties this does not have:

- **Balance is per lookup, not per request.** A client that resolves once at startup and holds its connections open stays on one proxy for as long as those connections live. The distribution is across client processes, not requests, so a few long-lived heavy clients can sit unevenly however well the resolver rotates.
- **No client-side retry against a second address.** The AWS CRT spreads requests across every address it resolves and retries another on a connection failure. It does that for the S3 endpoint, not for the proxy address, which the HTTP client treats as one destination. Withdrawing a failed proxy is therefore DNS's job, and requests fail until the record is withdrawn and the client's cached answer expires. Check whether your DNS service can withdraw a record on a failed health check; on AWS, see [DNS: Route 53 private hosted zones](AWS_DEPLOYMENT.md#dns-route-53-private-hosted-zones).

Where either of those matters more than the cost of the extra tier, a Layer 4 load balancer ([Option B](#option-b-layer-4-load-balancer)) health-checks the proxies itself and distributes per connection.

#### Option B: Layer 4 load balancer

Front a fleet with a Layer 4 (TCP) load balancer to give clients one stable endpoint, with health checking and failover at the load balancer. The proxy is protocol-agnostic behind the load balancer: it sees whatever bytes the load balancer forwards. Any L4 load balancer works: a cloud-managed TCP load balancer (for example AWS Network Load Balancer), HAProxy in TCP mode, nginx's `stream` module, F5 LTM, or similar.

There are three deployment patterns, depending on where TLS terminates. The choice decides whether the hop from the load balancer to the proxy crosses your network in cleartext. Pattern 3 suits most deployments, and [Which pattern to choose](#which-pattern-to-choose) compares the three.

##### Pattern 1: load balancer terminates TLS → proxy serves HTTP

```text
Client (HTTPS) → Load Balancer (TLS, cert on LB) → Proxy (HTTP :80 or :3128) → S3
```

The load balancer handles TLS using a certificate attached to the LB itself. Targets receive plain HTTP, so the proxy needs no TLS configuration; it runs in `standard` mode on port 80 or `proxy_only` mode on port 3128.

Typical load balancer configuration:
- TLS listener on port 443 with a server certificate attached
- Target pool forwarding TCP to the proxy's HTTP port (80 or 3128)
- TCP health check against the proxy's health port (`:8080/health`) or the HTTP port itself

Client configuration: point `HTTP_PROXY` at the load balancer and keep the S3 endpoint on `http://`, as in [Option A](#option-a-http_proxy-with-the-tls-proxy-listener-recommended):

```bash
export HTTP_PROXY=https://<load-balancer-hostname>
export NO_PROXY=169.254.169.254  # On EC2, keep IMDS off the proxy path
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

The request's `Host` stays the S3 hostname (e.g., `s3.us-east-1.amazonaws.com`), which SigV4 signs and the proxy forwards to; the load balancer hostname only determines where the TCP connection goes. Do not pass the load balancer hostname as `--endpoint-url`: the SDK would sign it as the `Host`, and the proxy takes the upstream from that header. Clients must trust the LB's certificate through the system trust store, since `AWS_CA_BUNDLE` does not apply to the `HTTP_PROXY` hop (see [Configuring clients to trust the certificate](#configuring-clients-to-trust-the-certificate)). A certificate from a public CA or an internal CA the hosts already trust needs no client change.

This pattern is the simplest: one certificate managed on the load balancer, no cert distribution to instances, no TLS config in the proxy.

> **Security: the LB→proxy hop is cleartext.** The client connection is encrypted only as far as the load balancer. From there to the proxy, the request travels in plain HTTP across your network, including the object key, the `Authorization` header with the caller's access key ID, and the object payload in both directions. Anything that can observe that segment can read all S3 traffic on it and replay captured requests against S3. See [What a Cleartext Hop Exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes) for the full exposure and replay windows. Choose this pattern only where that segment is secure and cleartext on it is acceptable; otherwise use Pattern 3 (the same certificate convenience, with an encrypted hop) or Pattern 2.

##### Pattern 2: load balancer TCP passthrough → proxy terminates TLS

```text
Client (HTTPS) → Load Balancer (TCP :443 passthrough) → Proxy (TLS :3129, terminates) → S3
```

The load balancer forwards encrypted TCP bytes without decrypting. Each proxy terminates TLS itself using a certificate on the instance. Use this pattern when:
- You cannot or prefer not to attach certificates to the load balancer
- You need the client-to-proxy connection to remain encrypted end-to-end across the VPC/internal network
- You want the proxy's access logs to reflect the full TLS session (the LB in passthrough mode contributes no TLS metadata)

Typical load balancer configuration:
- TCP listener on port 443 forwarding to the target pool
- Target pool forwarding TCP to the proxy's `tls_proxy_port` (default 3129)
- TCP health check against `:8080/health` or the TLS port

Enable the TLS proxy listener in each proxy's config (see [TLS Proxy Listener Configuration](#tls-proxy-listener-configuration) for full details and cert generation):

```yaml
server:
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/proxy/tls/cert.pem"
    key_path: "/etc/proxy/tls/key.pem"
```

The certificate's subject alternative names must match the load balancer hostname (clients connect to the LB, not to individual proxies, so the LB hostname is what gets verified during the TLS handshake). A certificate that names only the proxies' IP addresses does not cover it: the AWS CLI's CRT transfer client then fails TLS negotiation with `AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE`. Add a `DNS:` entry for the load balancer name:

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes \
  -subj "/CN=<load-balancer-hostname>" \
  -addext "subjectAltName=DNS:<load-balancer-hostname>"
```

Every proxy needs the same certificate, or certificates covering the same SANs.

Client configuration: install the proxy certificate in the client's system trust store (see [Configuring clients to trust the certificate](#configuring-clients-to-trust-the-certificate)), then:

```bash
export HTTP_PROXY=https://<load-balancer-hostname>:3129
export NO_PROXY=169.254.169.254  # On EC2, keep IMDS off the proxy path
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

The proxy listens on `tls_proxy_port` (3129 by default). Either point the LB's TCP listener at port 3129 on the targets and advertise the LB on port 443 (so clients use the default port), or expose the LB on port 3129 directly.

##### Pattern 3: load balancer terminates and re-encrypts → proxy terminates TLS

```text
Client (HTTPS) → Load Balancer (TLS, cert on LB) → [re-encrypted] → Proxy (TLS :3129, terminates) → S3
```

The load balancer terminates the client's TLS using its own certificate, then opens a second TLS connection to the proxy. Two independent TLS sessions, no cleartext on either. This combines Pattern 1's certificate convenience with Pattern 2's encrypted internal hop, and is the best default for most deployments.

It costs little because the load balancer does not validate the proxy's certificate. On an AWS NLB, [a target group configured with the TLS protocol connects to targets using the certificates installed on them, without validating those certificates](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups). The proxy's certificate can be self-signed and long-lived, and needs no SAN matching the client-facing name. Most software load balancers offer the same (HAProxy `ssl verify none`, nginx `proxy_ssl_verify off`).

Typical load balancer configuration:
- TLS listener on port 443 with a server certificate attached (ACM, internal CA, Let's Encrypt)
- Target pool with protocol **TLS**, forwarding to the proxy's `tls_proxy_port` (default 3129)
- Health check against `:8080/health`

Proxy config is the same as Pattern 2 (`tls.enabled: true` with `cert_path`/`key_path`). The certificate requirements are looser: generate one self-signed cert, reuse it on every instance, and set the SAN to anything (the instance hostname is conventional):

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
  -days 3650 -nodes -subj "/CN=s3-proxy-backend"  # not validated by the LB; no SAN matching needed
```

Client configuration is identical to Pattern 1. Clients see only the LB's certificate:

```bash
export HTTP_PROXY=https://<load-balancer-hostname>
export NO_PROXY=169.254.169.254  # On EC2, keep IMDS off the proxy path
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

Because the LB does not validate the proxy's certificate, this pattern protects against **passive** observation of the internal segment, not against an active attacker who can impersonate a proxy on it. Pattern 2 (TCP passthrough) is stronger on that point only for clients that verify the proxy's certificate themselves, such as the AWS CRT transfer client. Clients built on botocore do not verify it, so for them Pattern 2 also protects against passive observation only (see [Configuring clients to trust the certificate](#configuring-clients-to-trust-the-certificate)).

##### Which pattern to choose

| | Pattern 1 (LB terminates TLS) | Pattern 2 (TCP passthrough) | Pattern 3 (LB re-encrypts) |
|---|---|---|---|
| LB→proxy hop | cleartext | encrypted (client's own TLS) | encrypted (second TLS session) |
| Certificate management | One certificate on the LB | A certificate on every proxy | One certificate on the LB, plus any self-signed certificate on the proxies |
| Certificate rotation | Update once at the LB | Update on every proxy (automate it) | Update once at the LB; the proxies' certificate can be long-lived |
| Client trust config | Trusts the LB cert (usually already in place) | Proxy cert in the client's system trust store | Trusts the LB cert (usually already in place) |
| Proxy config | No TLS needed | `tls.enabled: true` + cert/key paths | `tls.enabled: true` + cert/key paths |
| Proxy certificate verified? | n/a | By the AWS CRT transfer client, not by botocore-based clients (`aws s3api`, classic `aws s3`, boto3) | No, the LB accepts any certificate |
| Where TLS runs | At the LB | On each proxy | At the LB and on each proxy |
| Best for | Only where the LB→proxy segment is secure and cleartext on it is acceptable | End-to-end encryption; no certificate on the LB | Most deployments |

**Recommendation**: use Pattern 3 unless you have a reason not to. It encrypts the internal hop at roughly Pattern 1's operational cost. Use Pattern 2 when your clients use the AWS CRT transfer client and must verify the proxy's certificate themselves, or when you cannot attach a certificate to the load balancer. Use Pattern 1 only where the segment between the load balancer and the proxies is secure and cleartext on it is acceptable; see [What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes).

##### Layer 4 or Layer 7?

Default to L4. The proxy already terminates and interprets HTTP itself (caching decisions, range handling, conditional requests). An L7 load balancer therefore redoes that work on every request for no gain, and inserts its own HTTP behavior between client and proxy. An L4 load balancer forwards bytes and stays out of the way.

Choose L7 only if you want the router to decide *per request*, sending every read of one object and byte range to the same instance, which L4 cannot do because it never sees the request. See [Request-Aware Routing](REQUEST_AWARE_ROUTING.md).

#### Option C: DNS zone for S3 hostnames (secure network segments)

Resolve S3's own hostnames to the proxies, so clients need no proxy setting. Clients send `http://` requests to port 80, which the proxy caches, and HTTPS requests to port 443, which pass through uncached. Requests to port 80 cross the network in cleartext, so use this option only where the network segment between clients and proxies is secure and cleartext on it is acceptable. SigV4 authenticates each request but does not encrypt it; see [What a cleartext hop exposes](ARCHITECTURE.md#what-a-cleartext-hop-exposes).

The example below uses Amazon Route 53 private hosted zones; the same approach works with any DNS service.

**Architecture**: Designate one S3 regional zone as the "primary": it holds the multi-value A records for your proxies. All other zones (additional regions, access points, MRAPs) use wildcard CNAMEs that point to the primary zone's apex. This keeps proxy IPs defined in as few places as possible.

> **Why apex A records are duplicated**: DNS does not allow CNAME records at a zone apex. Each hosted zone must have its own apex A records. Wildcard records (e.g., `*.s3.us-west-2.amazonaws.com`) can be CNAMEs, so those chain back to the primary zone. When adding or removing proxies, update the A records in each zone's apex.

**1. Create the primary S3 regional zone:**

```bash
aws route53 create-hosted-zone \
  --name s3.us-west-2.amazonaws.com \
  --vpc VPCRegion=us-west-2,VPCId=vpc-xxxxxxxx \
  --caller-reference "s3-proxy-$(date +%s)" \
  --hosted-zone-config PrivateZone=true \
  --no-cli-pager
```

**2. Add A records and wildcard CNAME to the primary zone:**

Create `dns-primary.json`:

```json
{
    "Changes": [
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy1",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.101"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy2",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.102"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "*.s3.us-west-2.amazonaws.com",
                "Type": "CNAME",
                "TTL": 60,
                "ResourceRecords": [{"Value": "s3.us-west-2.amazonaws.com"}]
            }
        }
    ]
}
```

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id Z_PRIMARY_ZONE_ID \
  --change-batch file://dns-primary.json \
  --no-cli-pager
```

**3. Create zones for access points, MRAPs, and additional regions:**

Each zone needs apex A records (same IPs as the primary) and a wildcard CNAME that chains to the primary zone's apex. The VPC resolver walks the CNAME chain across private hosted zones, so `*.s3-accesspoint.us-west-2.amazonaws.com` → `s3.us-west-2.amazonaws.com` → proxy IPs.

```bash
aws route53 create-hosted-zone \
  --name s3-accesspoint.us-west-2.amazonaws.com \
  --vpc VPCRegion=us-west-2,VPCId=vpc-xxxxxxxx \
  --caller-reference "s3-proxy-ap-$(date +%s)" \
  --hosted-zone-config PrivateZone=true \
  --no-cli-pager

aws route53 create-hosted-zone \
  --name accesspoint.s3-global.amazonaws.com \
  --vpc VPCRegion=us-west-2,VPCId=vpc-xxxxxxxx \
  --caller-reference "s3-proxy-mrap-$(date +%s)" \
  --hosted-zone-config PrivateZone=true \
  --no-cli-pager
```

For each zone, create a change batch with apex A records and a wildcard CNAME to the primary. Example for the access point zone (`dns-accesspoint.json`):

```json
{
    "Changes": [
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3-accesspoint.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy1",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.101"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "s3-accesspoint.us-west-2.amazonaws.com",
                "Type": "A",
                "SetIdentifier": "proxy2",
                "MultiValueAnswer": true,
                "TTL": 60,
                "ResourceRecords": [{"Value": "10.0.1.102"}]
            }
        },
        {
            "Action": "UPSERT",
            "ResourceRecordSet": {
                "Name": "*.s3-accesspoint.us-west-2.amazonaws.com",
                "Type": "CNAME",
                "TTL": 60,
                "ResourceRecords": [{"Value": "s3.us-west-2.amazonaws.com"}]
            }
        }
    ]
}
```

The MRAP zone (`dns-mrap.json`) follows the same pattern: apex A records plus wildcard CNAME to `s3.us-west-2.amazonaws.com`. Additional regional zones (e.g., `s3.eu-west-1.amazonaws.com`) use the same template: apex A records with the proxy IPs, wildcard CNAME → `s3.us-west-2.amazonaws.com`.

**DNS zone summary:**

| Zone | Apex Records | Wildcard CNAME Target |
|------|-------------|----------------------|
| `s3.us-west-2.amazonaws.com` (primary) | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` (self) |
| `s3-accesspoint.us-west-2.amazonaws.com` | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` |
| `accesspoint.s3-global.amazonaws.com` | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` |
| `s3.{other-region}.amazonaws.com` | Multi-value A → proxy IPs | `s3.us-west-2.amazonaws.com` |

**Adding or removing a proxy**: Update the apex A records in each zone. The wildcard CNAMEs require no changes; they resolve through the chain automatically.

**Key DNS configuration concepts:**

| Concept | Purpose |
|---------|---------|
| Multi-value A records | Load balancing across proxies |
| Wildcard CNAME chain | Routes all subdomains (buckets, access points) to proxy IPs via the primary zone |
| Short TTL (60s) | Bounds how long resolvers keep a withdrawn proxy address; failure detection usually takes longer (see [Fleet Behind One Proxy Name](#fleet-behind-one-proxy-name)) |

This approach mirrors how S3 itself uses multi-value answer (MVA) routing for DNS queries ([launch announcement](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-s3-multivalue-answer-response-dns-queries/); see also [Multivalue answer DNS routing](https://docs.aws.amazon.com/general/latest/gr/s3.html#multivalue-answer-dns-routing) in the AWS General Reference). The AWS Common Runtime (CRT), available as a transfer client in AWS CLI v2 and modern SDKs, resolves all IPs from DNS, distributes requests across them, and retries against alternate IPs on connection failure. That gives load balancing and failover without an external load balancer. Clients without MVA support fall back to using the first IP in the response.

**A fleet on one NFS cache volume**: mount the volume with `lookupcache=pos`, and with advisory file locks enforced between hosts (never `nolock` or `local_lock`). Cache coordination depends on both. The proxy logs a startup warning for a missing `lookupcache=pos` and reports nothing for missing locks. See [NFS Mount Requirements](CONFIGURATION.md#nfs-mount-requirements) for the full option list and how to verify a live mount.

#### Option D: hosts file (testing and development)

> Consider [Option A (HTTP_PROXY)](#option-a-http_proxy-with-the-tls-proxy-listener-recommended) first. It avoids the need for sudo, root-owned files, and the per-bucket/per-access-point entries required here. Use hosts file routing only when the client can't set `HTTP_PROXY` or you specifically want to exercise DNS-style routing.

For local testing or single-machine setups, edit `/etc/hosts`:

```bash
sudo nano /etc/hosts

# Add entries for your region's S3 endpoints
127.0.0.1 s3.amazonaws.com
127.0.0.1 s3.us-east-1.amazonaws.com
127.0.0.1 s3.eu-west-1.amazonaws.com
# Add other regions as needed

# For bucket-style URLs, add each bucket explicitly
127.0.0.1 your-bucket.s3.us-east-1.amazonaws.com
127.0.0.1 another-bucket.s3.us-east-1.amazonaws.com

# For S3 Access Points, add each access point explicitly
# Regional access points: {name}-{account_id}.s3-accesspoint.{region}.amazonaws.com
127.0.0.1 my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com
127.0.0.1 other-ap-123456789012.s3-accesspoint.eu-west-1.amazonaws.com

# Multi-Region Access Points (MRAPs): {mrap_alias}.accesspoint.s3-global.amazonaws.com
127.0.0.1 mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com
```

**Limitations**: 
- Hosts files don't support wildcards, so you must add each bucket and access point individually
- A hosts file gives no multi-value answers, so it cannot spread load across a fleet
- No high availability: if the proxy fails, all S3 traffic fails
- For a fleet, use Option A with a [proxy name](#fleet-behind-one-proxy-name), Option B or Option C

See [AWS S3 endpoints documentation](https://docs.aws.amazon.com/general/latest/gr/s3.html) for complete list of regional endpoints.

#### Comparing the fleet routing options

Three of the options spread a fleet's traffic: Option A pointed at a [proxy name](#fleet-behind-one-proxy-name), Option B (a Layer 4 load balancer) and Option C (a DNS zone for S3's hostnames).

| | Option A with a proxy name | Option B: Layer 4 load balancer | Option C: DNS zone for S3 hostnames |
|---|---|---|---|
| Client → proxy hop | Encrypted and cached, through the TLS proxy listener (port 3129) | Encrypted and cached (Pattern 2 or 3) | Cleartext on port 80, so only on a secure network segment; HTTPS on port 443 passes through uncached |
| Infrastructure | One record set for a name you own | A load balancer, managed or self-hosted | A private DNS zone per S3 hostname, with address records |
| Distribution | The resolver rotating its cached record set | Per connection, by the load balancer | The client spreading requests over every resolved address |
| Failover | The record withdrawn from DNS | The load balancer removing unhealthy targets | The client retrying another address from the DNS answer |
| Health checking | Whatever the DNS service offers | Server-side: the load balancer probes the proxies | Client-side: connection attempts |
| Client requirement | Any client that reads `HTTP_PROXY` | Any client that reads `HTTP_PROXY` | The AWS CRT transfer client, for automatic failover |
| Single endpoint | Yes, one name | Yes, the load balancer's name or address | No, clients resolve several addresses |

Use Option A with a proxy name when clients can set `HTTP_PROXY`: it encrypts the client hop and keeps caching without a load balancer. It also needs one record set rather than a zone per S3 hostname. Use Option B when you need per-connection balance, or failover that does not wait on a DNS TTL. Use Option C where the network segment between clients and proxies is secure and cleartext on it is acceptable, and clients should need no proxy setting; automatic failover then needs the AWS CRT transfer client.

#### S3 PrivateLink (interface VPC endpoints)

If your environment uses S3 interface VPC endpoints (PrivateLink), the proxy needs additional configuration to route traffic to S3 through the private endpoint rather than over the public internet.

Client routing is unchanged. PrivateLink affects only how the proxy resolves and connects to S3 on the upstream side.

**The problem**: By default, the proxy uses external DNS servers (Google DNS, Cloudflare DNS) to resolve S3 endpoints. These public resolvers return public S3 IPs, bypassing your PrivateLink endpoints entirely. Traffic would leave your private network instead of using the private path.

**The fix depends on where the proxy runs**, and the two cases need opposite settings for private DNS on the endpoint.

**Proxies in AWS, in the VPC holding the endpoint**: use `endpoint_overrides` (below) and leave private DNS **disabled** on the endpoint. Private DNS creates a private hosted zone for `s3.<region>.amazonaws.com` in that VPC, the same name Option C points at the proxies, so the two collide. AWS rejects the endpoint with a conflicting-domain error where such a zone already exists. Any resolver in that VPC, including a Route 53 Resolver inbound endpoint, answers from the hosted zone and returns the proxy's own address, forming a loop.

**Proxies on-premises, reaching S3 over Direct Connect or VPN**: configure `dns_servers` to point at Route 53 Resolver inbound endpoints in the VPC, and **enable** private DNS on the interface endpoint. The inbound endpoint ENIs sit on your private subnets, reachable from on-premises over the VPN or Direct Connect link, and query the VPC's internal resolver, which returns PrivateLink IPs. Nothing hijacks the S3 name inside the VPC in this topology, so its view is clean.

```yaml
connection_pool:
  dns_servers: ["10.0.1.50", "10.0.2.50"]  # Route 53 Resolver inbound endpoint IPs
```

The proxy cannot use your on-prem DNS server for this, because that is where the override sending clients to the proxy lives: it resolves S3 endpoints to the proxy's own IP. Pointing `dns_servers` at it creates a loop. The Route 53 Resolver inbound endpoint provides a separate resolution path that returns the PrivateLink IPs.

**Static IP overrides**: required for proxies in the endpoint's VPC, and the fallback on-premises when Route 53 Resolver inbound endpoints are unavailable. Map S3 hostnames directly to PrivateLink ENI IPs:

```yaml
connection_pool:
  endpoint_overrides:
    # Suffix patterns cover all bucket/AP hostnames in a region
    "*.s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]
    # MRAP global endpoint (separate VPCE: com.amazonaws.s3-global.accesspoint)
    "*.accesspoint.s3-global.amazonaws.com": ["10.0.3.100"]
```

Keys starting with `*.` are suffix patterns: they match any hostname ending with that suffix. Exact-match keys (without `*.`) are also supported and take precedence. See [Configuration Guide - endpoint_overrides](CONFIGURATION.md#s3-privatelink-interface-vpc-endpoints) for the full syntax and precedence rules.

Outbound TLS to a hostname matched by `endpoint_overrides` is pinned to version 1.2 for VPC interface endpoint compatibility; other hosts negotiate TLS 1.2 or 1.3.

**Verifying resolution (`dns_servers` route)**: from the proxy host, confirm the inbound endpoint returns private IPs for S3 endpoints:

```bash
dig +short s3.us-west-2.amazonaws.com @10.0.1.50
```

This should return private IPs (e.g., `10.0.x.x` or `172.x.x.x`) from the interface endpoint ENIs, not public S3 IPs. If it returns public IPs, private DNS is disabled on the interface endpoint; enable it, or switch to `endpoint_overrides`.

Note that the endpoint-specific DNS names (`*.vpce-0abc123def456.s3.us-west-2.vpce.amazonaws.com`) are not usable as a substitute here. The proxy takes the upstream hostname from the client's signed `Host` header and has no setting to replace it. Reaching an endpoint without private DNS means resolving those names yourself and listing the resulting IPs in `endpoint_overrides`.

**Verifying resolution (`endpoint_overrides` route)**: a `dig` test proves nothing, since overrides bypass DNS. Request an object through the proxy, then confirm on the proxy host that the established upstream connections land on the ENI IPs rather than public S3 addresses:

```bash
ss -tn state established '( dport = :443 )'
```

**HTTPS passthrough**: `endpoint_overrides` apply to the HTTPS passthrough handler (port 443) as well as the HTTP caching path, so the overrides route covers both. `dns_servers` does not: the passthrough handler uses its own resolver fixed to Google/Cloudflare DNS. On the `dns_servers` route, either ensure the proxies can reach the PrivateLink ENIs by network routing, or use HTTP endpoints for all cached traffic.

#### Access point and MRAP endpoint requirements

S3 Access Points and Multi-Region Access Points (MRAPs) require different client configuration than regular bucket requests.

**Why `AWS_ENDPOINT_URL_S3` doesn't work for access points**: The AWS SDK resolves access point ARNs to their own hostnames (e.g., `my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com`), bypassing `AWS_ENDPOINT_URL_S3`. Each access point request must specify `--endpoint-url` explicitly.

**Simplified endpoint approach**: The `--endpoint-url` does not need the full access-point-specific hostname. The base regional endpoint works for all access points in a region:

```bash
--endpoint-url http://s3-accesspoint.{region}.amazonaws.com
```

The SDK resolves the ARN from the S3 URI, constructs the correct Host header (e.g., `my-ap-123456789012.s3-accesspoint.us-east-1.amazonaws.com`), and uses the `--endpoint-url` only as the connection target.

**Example: download via regional access point:**

```bash
aws s3 cp s3://arn:aws:s3:eu-west-1:123456789012:accesspoint/my-ap/bigfiles/5GB ~/tmp/ \
  --region eu-west-1 \
  --endpoint-url http://s3-accesspoint.eu-west-1.amazonaws.com
```

The ARN in the S3 URI provides the SDK with all information needed to build the correct Host header and SigV4 signature. The `--endpoint-url` only controls where the HTTP connection goes (i.e., the proxy).

**MRAP requests** follow the same pattern but use the global endpoint:

```bash
aws s3 cp s3://arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw/bigfiles/5GB ~/tmp/ \
  --endpoint-url http://accesspoint.s3-global.amazonaws.com
```

The AWS CLI automatically uses SigV4A (`AWS4-ECDSA-P256-SHA256`) for MRAP requests. The proxy recognizes SigV4A signatures and handles them identically to classic SigV4: signed headers are preserved, range signatures are detected, and referer injection respects the `SignedHeaders` list.

#### Access point alias usage

S3 Access Points can be accessed by alias (e.g., `my-ap-abcdef123456-s3alias`) instead of ARN. The simplest way to cache AP alias requests through the proxy is to use the regular S3 regional endpoint:

```bash
# AP alias via regular S3 endpoint (recommended for caching)
aws s3 cp s3://my-ap-abcdef123456-s3alias/data/file.txt ~/tmp/ \
  --region us-east-1 \
  --endpoint-url http://s3.us-east-1.amazonaws.com
```

The AWS CLI resolves the alias to a virtual-hosted Host header (`my-ap-abcdef123456-s3alias.s3.us-east-1.amazonaws.com`), which the Route 53 wildcard `*.s3.{region}.amazonaws.com` routes to the proxy. S3 accepts AP alias bucket names on the regular S3 endpoint.

**Why not `--endpoint-url http://s3-accesspoint.{region}.amazonaws.com`?**

The base domain `s3-accesspoint.{region}.amazonaws.com` does not resolve to S3 IPs via public DNS; it only exists as a base for virtual-hosted subdomains. The proxy uses external DNS (Google/Cloudflare) to resolve upstream endpoints, so it cannot connect to this hostname. Use the regular S3 endpoint instead.

**ARN-based access point requests** work with the AP-specific endpoint because the CLI constructs the full virtual-hosted hostname from the ARN:

```bash
# ARN-based AP access (works with AP endpoint)
aws s3 cp s3://arn:aws:s3:eu-west-1:123456789012:accesspoint/my-ap/data/file.txt ~/tmp/ \
  --region eu-west-1 \
  --endpoint-url http://s3-accesspoint.eu-west-1.amazonaws.com
```

#### MRAP alias usage

Multi-Region Access Point aliases work similarly. Use the MRAP-specific endpoint with the full alias:

```bash
# MRAP alias via MRAP endpoint
aws s3 cp s3://mfzwi23gnjvgw.mrap/data/file.txt ~/tmp/ \
  --endpoint-url http://accesspoint.s3-global.amazonaws.com
```

The CLI resolves the alias to `mfzwi23gnjvgw.accesspoint.s3-global.amazonaws.com`, which the Route 53 wildcard routes to the proxy.

#### Access point endpoint summary

| Access Pattern | Endpoint URL | Cached? | Notes |
|---|---|---|---|
| AP alias | `http://s3.{region}.amazonaws.com` | Yes | Alias resolves under `*.s3.{region}` wildcard. Proxy can resolve this upstream. |
| AP ARN | `http://s3.{region}.amazonaws.com` | Yes | CLI builds Host from ARN. Either endpoint works for ARN-based requests. |
| AP ARN | `http://s3-accesspoint.{region}.amazonaws.com` | Yes | CLI builds Host from ARN. |
| AP alias | `http://s3-accesspoint.{region}.amazonaws.com` | No | Bare `s3-accesspoint` domain doesn't resolve via public DNS; proxy can't connect upstream. |
| MRAP alias | `http://accesspoint.s3-global.amazonaws.com` | Yes | CLI builds correct Host from alias. |
| MRAP ARN | `http://accesspoint.s3-global.amazonaws.com` | Yes | CLI builds correct Host from ARN. |
| Any AP/MRAP | *(none, default HTTPS)* | No | HTTPS uses HTTPS passthrough, bypassing the cache. |

All addressing styles (path-style, `s3.{region}.amazonaws.com/{bucket}/{key}`, regional virtual-hosted (`{bucket}.s3.{region}.amazonaws.com/{key}`), accelerate (`{bucket}.s3-accelerate.amazonaws.com/{key}`), and legacy global, `{bucket}.s3.amazonaws.com/{key}`) share cache entries for the same bucket and key. Accessing the same object through any combination of these styles hits the same cache slot.

#### S3 Transfer Acceleration

The proxy extracts the bucket from accelerate hostnames (`<bucket>.s3-accelerate.amazonaws.com` and `<bucket>.s3-accelerate.dualstack.amazonaws.com`) and caches accelerate requests against the same entries as path-style and regional virtual-hosted requests for the same bucket and key. Getting accelerate traffic to the proxy uses the same routing options as other S3 traffic ([Option A](#option-a-http_proxy-with-the-tls-proxy-listener-recommended), [Option C](#option-c-dns-zone-for-s3-hostnames-secure-network-segments) or [Option D](#option-d-hosts-file-testing-and-development)), with one exception: Route 53 refuses private hosted zones for `s3-accelerate.amazonaws.com`, because its canonical target (CloudFront) is outside AWS Region routing. For Option C on accelerate, use a VPC-level resolver (CoreDNS, Unbound) instead of Route 53. Options A and D work unchanged.

Enable acceleration at the SDK/CLI level (`use_accelerate_endpoint = true`) and pass `--endpoint-url http://s3-accelerate.amazonaws.com` to force HTTP.

**Recommendation**: for most deployments, target Regional virtual-hosted endpoints (`<bucket>.s3.<region>.amazonaws.com`) instead. They reuse the existing Regional routing, cache correctly, and share cache entries with path-style and accelerate access for the same bucket and key. Through a proxy, accelerate's upstream leg goes to a CloudFront edge location rather than the bucket's home Region, so any latency difference depends on where the proxy runs.

Clients that prefer path-style over virtual-hosted addressing can still use `--endpoint-url http://s3.<region>.amazonaws.com`. This works and caches correctly.

## Basic usage

### Upload and download files

```bash
# Upload a file (stored in S3 and in the write cache)
aws s3 cp file.txt s3://your-bucket/key \
  --endpoint-url "http://s3.us-east-1.amazonaws.com" \
  --region us-east-1

# Download the same file (cache hit - served from proxy)
aws s3 cp s3://your-bucket/key ./downloaded.txt \
  --endpoint-url "http://s3.us-east-1.amazonaws.com" \
  --region us-east-1
```

## Simplifying client configuration

Instead of adding `--endpoint-url` to every AWS CLI command, you can set environment variables to automatically route S3 traffic through the proxy.

### AWS CLI environment variable

Set `AWS_ENDPOINT_URL_S3` to use HTTP for all S3 operations (example for us-east-1):

```bash
export AWS_ENDPOINT_URL_S3=http://s3.us-east-1.amazonaws.com
```

Now AWS CLI S3 commands use the proxy without the `--endpoint-url` flag:

```bash
# Works for buckets in us-east-1
aws s3 cp file.txt s3://your-bucket/key
aws s3 ls s3://your-bucket/
```

**Region Limitation**: The environment variable only works for buckets in the endpoint's region. For buckets in other regions, you must specify both `--region` and `--endpoint-url`:

```bash
# Bucket in eu-west-1 (different from environment variable region)
aws s3 cp file.txt s3://eu-bucket/key \
  --region eu-west-1 \
  --endpoint-url http://s3.eu-west-1.amazonaws.com
```

### s5cmd configuration

[s5cmd](https://github.com/peak/s5cmd) is a parallel S3 command-line client. Use the `S3_ENDPOINT_URL` environment variable:

```bash
export S3_ENDPOINT_URL=http://s3.us-east-1.amazonaws.com
```

Then use s5cmd normally:

```bash
s5cmd cp file.txt s3://your-bucket/key
s5cmd ls s3://your-bucket/
```

The same region limitation applies - the endpoint URL must match the bucket's region.

### MinIO Go client (Forgejo, Gitea, etc.)

Applications using the [MinIO Go client](https://github.com/minio/minio-go) (including Forgejo and Gitea) default to HTTPS, which bypasses the cache. Set `MINIO_USE_SSL = false` to use HTTP:

```ini
MINIO_USE_SSL = false
```

For Go applications using minio-go directly, set `Secure: false` in `minio.Options`.

`HTTP_PROXY` does not help here. Go's `net/http` sends a `CONNECT` tunnel for HTTPS targets, which bypasses caching. `MINIO_USE_SSL = false` is required.

### boto3 per-client configuration

boto3 clients accept `endpoint_url` at creation time, so a single application can route some S3 traffic through the proxy while keeping other traffic direct:

```python
import boto3

# Cached: reads go through proxy
s3_cached = boto3.client('s3',
    endpoint_url='http://s3.us-east-1.amazonaws.com',
    region_name='us-east-1'
)

# Direct: writes bypass proxy entirely
s3_direct = boto3.client('s3', region_name='us-east-1')
```

This avoids the all-or-nothing behavior of environment variables like `AWS_ENDPOINT_URL_S3`.

#### Per-client proxy configuration

boto3 does not accept `http_proxy` as a keyword argument on `boto3.client()`. Proxy settings are configured through a `botocore.config.Config` object passed via `config=`, which is the per-client equivalent of `HTTP_PROXY` / `HTTPS_PROXY` environment variables:

```python
import boto3
from botocore.config import Config

cfg = Config(
    proxies={
        'http':  'http://<proxy-host>:80',
        'https': 'http://<proxy-host>:80',
    },
)

s3 = boto3.client('s3',
    endpoint_url='http://s3.us-east-1.amazonaws.com',
    region_name='us-east-1',
    config=cfg,
)
```

Notes:

- The `https` proxy URL typically uses the `http://` scheme, which is the scheme of the proxy connection, not the target.
- For encrypted client-to-proxy transport, point the proxy URL at the TLS proxy listener (e.g., `'https://<proxy-host>:3129'`). With an `http://` endpoint, boto3 encrypts that hop but does not verify the proxy's certificate. Neither `verify=`, `AWS_CA_BUNDLE` nor `proxies_config={'proxy_ca_bundle': ...}` changes that (see [Configuring clients to trust the certificate](#configuring-clients-to-trust-the-certificate)).
- Without `config=`, boto3 picks up `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` from the environment. The per-client `Config` only overrides proxy settings for that specific client, leaving other clients in the same process unaffected.
- Set `NO_PROXY=169.254.169.254` on EC2 to keep IMDS credential retrieval off the proxy path. Not needed off EC2 unless you have other IPs to bypass.
- `proxies_config={'proxy_use_forwarding_for_https': ...}` governs only `https://` targets, so it does not affect the `http://` endpoint above. At its default (`False`), an `https://` target is tunnelled via `CONNECT`, which the proxy cannot cache. Use an `http://` endpoint for cached traffic.

### Mountpoint for Amazon S3

[Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3) is a file client that mounts S3 buckets as local file systems. To enable caching through the proxy, use the `--endpoint-url` flag with HTTP:

```bash
mount-s3 --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1 \
  your-bucket /mnt/s3
```

**Important**: Mountpoint uses HTTPS by default, which bypasses the cache regardless of routing mode: HTTPS connections use HTTPS passthrough (port 443) under DNS routing, or a `CONNECT` tunnel under `HTTPS_PROXY`. The `--endpoint-url` flag with HTTP is required for caching with both DNS routing and proxy mode. For encrypted client-to-proxy transport, combine `--endpoint-url http://...` with `HTTP_PROXY=https://proxy:3129` and the TLS proxy listener.

**DNS routing**: If using Route 53 private hosted zones or hosts file DNS routing, the endpoint URL should match your configured DNS zone. The proxy caches HEAD responses (metadata) and range requests from Mountpoint, so repeated reads are served from the cache (see the performance figures below).

**Addressing**: with `--endpoint-url` set and no other flags, Mountpoint connects virtual-hosted, sending `your-bucket.s3.us-east-1.amazonaws.com` rather than `s3.us-east-1.amazonaws.com/your-bucket`. Both styles share cache entries for the same bucket and key (see [Access Point Endpoint Summary](#access-point-endpoint-summary)), so this is a DNS question, not a caching one.

Virtual-hosted needs the bucket subdomain to resolve to the proxy. [Option C](#option-c-dns-zone-for-s3-hostnames-secure-network-segments) covers that with its `*.s3.{region}.amazonaws.com` wildcard CNAME. A hosts file cannot express a wildcard, so under [Option D](#option-d-hosts-file-testing-and-development) either add a line per bucket or pass `--force-path-style` and let the apex entry serve every bucket:

```bash
mount-s3 --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1 --force-path-style \
  your-bucket /mnt/s3
```

Option A (`HTTP_PROXY`) needs neither: the proxy is addressed directly and the bucket name never has to resolve.

**Mountpoint caching**: When using Mountpoint with the proxy, configure Mountpoint's [metadata TTL](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#metadata-cache) but disable Mountpoint's [data cache](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#data-cache). The proxy provides data caching, so Mountpoint's data cache would be redundant. Use `--metadata-ttl` to control how long Mountpoint caches file metadata.

**Performance**: Measured with upstream Mountpoint's own `fs_bench.sh` fio script (single proxy, FSx for OpenZFS cache volume, m5dn.24xlarge client, 10-iteration average). A single-threaded sequential read through the proxy reached 1,491 MiB/s against 1,097 MiB/s direct to S3 from the same client, 1.36x, because the shared cache served the read instead of S3. A single-threaded read at direct I/O landed close to parity (1.03x). The proxy's four-thread and write figures on this run were bound by a single proxy's own capacity rather than by Mountpoint or the caching path; scale proxies to raise those. Instance type, storage type and working-set size all move these numbers, so measure your own shape.

### Shell profile configuration

For persistent configuration, add to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.):

```bash
# Route S3 traffic through proxy (HTTP for caching)
# Set to your primary Region - cross-Region requests need explicit --endpoint-url
export AWS_ENDPOINT_URL_S3=http://s3.us-east-1.amazonaws.com
export S3_ENDPOINT_URL=http://s3.us-east-1.amazonaws.com
```

### Check cache performance

```bash
# View real-time dashboard (open in browser)
open http://<proxy-host>:8081

# View cache metrics (JSON format)
curl http://<proxy-host>:9090/metrics | grep cache_hit_rate

# Check proxy health
curl http://<proxy-host>:8080/health
```

## Port configuration options

### Standard ports (80/443)

```yaml
# config/config.example.yaml
server:
  http_port: 80
  https_port: 443
```

- **Pros**: Works with all S3 clients, HTTPS passthrough on standard port
- **Cons**: Requires `sudo` for privileged ports

### Custom ports

Custom port numbers are possible but not recommended. HTTP clients can use `--endpoint-url` with a custom port, but HTTPS clients with non-configurable endpoints will fail. For development or environments where `sudo` is not available, use [Proxy-Only Mode](#proxy-only-mode) instead.

## Proxy-only mode

Proxy-only mode starts only the HTTP forward proxy listener (default port 3128) without binding to ports 80 or 443. No `sudo`, DNS changes, or `/etc/hosts` modifications needed: clients set `HTTP_PROXY` and traffic flows through the proxy.

Use proxy-only mode for:
- **Localhost deployments**: run the proxy on the same machine as your clients
- **Development and testing**: quick setup with no system-level changes
- **Environments where DNS changes are impractical**: no infrastructure dependencies

### Configuration

Set `server.mode` to `"proxy_only"` in your configuration file:

```yaml
server:
  mode: "proxy_only"
  # proxy_port: 3128  (default)

cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 524288000  # 500 MiB
```

Start the proxy (no `sudo` required):

```bash
cargo run --release -- -c config.yaml
```

The proxy listens on port 3128, on all interfaces, for unencrypted HTTP forward proxy traffic. On a
host other machines can reach, block or restrict 3128 with the host firewall or a security group.

To add encrypted client-to-proxy connections, enable the TLS proxy listener:

```yaml
server:
  mode: "proxy_only"
  proxy_port: 3128
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/proxy/tls/cert.pem"
    key_path: "/etc/proxy/tls/key.pem"
```

This starts both listeners: port 3128 (cleartext) and the TLS proxy listener on port 3129, which terminates TLS with the proxy's own certificate and sends the decrypted HTTP through the caching pipeline. See [Option A: HTTP_PROXY](#option-a-http_proxy-with-the-tls-proxy-listener-recommended) for certificate generation and client trust configuration.

### Client configuration

Clients set `HTTP_PROXY` and an `http://` S3 endpoint URL, as described under [Localhost](#localhost-simplest).

**Unencrypted (loopback, or a secure network segment):**

```bash
export HTTP_PROXY=http://127.0.0.1:3128
aws s3 cp s3://bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

**Encrypted, through the TLS proxy listener (clients on other hosts):**

```bash
export HTTP_PROXY=https://<proxy-host>:3129
export NO_PROXY=169.254.169.254
aws s3 cp s3://bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

The proxy's certificate must be in the client host's system trust store. `AWS_CA_BUNDLE` does not apply to the `HTTP_PROXY` hop; see [Configuring clients to trust the certificate](#configuring-clients-to-trust-the-certificate).

**On EC2, add `export NO_PROXY=169.254.169.254`** so IMDS credential retrieval bypasses the proxy (otherwise instance-profile credentials fail to load). Add other IPs or hostnames to `NO_PROXY` as needed.

See [Option A: HTTP_PROXY](#option-a-http_proxy-with-the-tls-proxy-listener-recommended) for why `HTTP_PROXY` is required instead of `HTTPS_PROXY`, and for TLS certificate generation, self-signed certificate SANs, and client trust configuration.

### High availability: shared NFS cache pattern

Instead of a central fleet that clients reach over the network, each compute host runs its own proxy on `127.0.0.1`, and all of them share one cache volume (NFS or EFS). High availability comes from each host's process supervisor and the cache volume the proxies share.

```text
Host A:  proxy (127.0.0.1:3128) ──┐
Host B:  proxy (127.0.0.1:3128) ──┼──► /mnt/shared-cache (NFS/EFS) ──► S3
Host C:  proxy (127.0.0.1:3128) ──┘
```

Each host's configuration points at the shared mount:

```yaml
server:
  mode: "proxy_only"

cache:
  cache_dir: "/mnt/shared-cache"
  max_cache_size: 107374182400  # 100 GiB
```

How it works:
- Each client talks to `127.0.0.1`, with no load balancer or DNS setup
- An object cached by any host's proxy is available to every host through the cache volume
- If a host's proxy crashes, its process supervisor (systemd, for example) restarts it; other hosts are unaffected
- NFS mount requirements: `lookupcache=pos`, and cross-host advisory locking (never `nolock` or `local_lock`); see [NFS Mount Requirements](CONFIGURATION.md#nfs-mount-requirements)

**Comparison with a central fleet:**

| | Central fleet (Options A to C) | A proxy on each host |
|---|---|---|
| **Architecture** | Proxies on their own hosts, reached over the network | A proxy on each compute host, all sharing one cache volume |
| **HA mechanism** | DNS or load-balancer failover across proxies | The process supervisor restarts the local proxy |
| **Load balancing** | DNS or a load balancer spreads clients across proxies | Each client uses its own local proxy |
| **Infrastructure** | A DNS record set, a DNS zone or a load balancer | An NFS or EFS mount and a process supervisor |
| **Network hops** | Client → network → proxy → S3 | Client → loopback → proxy → S3 |
| **Best for** | A fleet serving many clients | Compute hosts that each run their own workload |

A central fleet suits a few proxies serving many clients. A proxy on each host suits compute hosts that each generate their own S3 traffic, with no DNS or load-balancer setup.

## Fronting a plaintext or non-standard store (upstream_overrides)

By default every caching connection the proxy makes to an upstream uses verified TLS on port 443, the secure default. `connection_pool.upstream_overrides` lets you declare, per destination, that a specific `host:port` speaks a different transport: plaintext HTTP, validated HTTPS on a non-443 port, or HTTPS with certificate validation disabled. The map is empty by default; with no overrides configured, every destination uses verified TLS on 443.

The client always speaks `http://` to the proxy regardless of the upstream transport, exactly as when fronting real S3 (the client uses `--endpoint-url http://s3...` and the proxy still connects to S3 over TLS). The client scheme is the client→proxy leg and never signals how the proxy connects to the store, so the upstream scheme comes from configuration. The *port*, by contrast, travels in the request authority. It is the override lookup key, and the port the proxy dials when an entry matches.

This works because the upstream `host:port` is carried in the request the proxy receives. In forward-proxy mode the absolute-form request authority carries it, so the port the client targets is the port the request carries. The proxy connects to that port only when an `upstream_overrides` entry matches it; any other port is dialed with verified TLS on 443. In standard (hosts-file) mode the port comes from the `Host` header (defaulting to 80). The proxy intercepts only the single `http_port` it is bound to, plus HTTPS passthrough on 443, which does not cache. Pointing `http_port` at a non-standard port intercepts that one port. Reaching several ports, or leaving the listener on 80/443 while fronting a store elsewhere, needs forward-proxy mode, which the recipe below uses. The transport (plaintext vs TLS, and TLS validation) is selectable by configuration, through an `upstream_overrides` entry, on the port the request targets in either mode. Either way the proxy resolves the upstream hostname via its configured `dns_servers` (never `/etc/hosts`), so that name must resolve to the real origin, not back to the proxy.

### Local-dev recipe: plaintext MinIO on :9000

This recipe fronts a plaintext S3-compatible store (MinIO shown; RustFS or any S3-compatible server works the same) listening on `127.0.0.1:9000`, with the proxy in [Proxy-Only Mode](#proxy-only-mode). Adjust only the host and port for your store.

**1. Start the store.** MinIO listens on `:9000` in plaintext by default:

```bash
minio server /tmp/minio-data --address :9000  # plaintext S3-compatible store on 127.0.0.1:9000
```

MinIO's default root credentials are `minioadmin` / `minioadmin`. Leave it running in its own terminal.

**2. Configure the proxy override.** Map the store's `host:port`, exactly as the client targets it, to `scheme: http` (Plaintext mode):

```yaml
server:
  mode: "proxy_only"
  # proxy_port: 3128  (default)

cache:
  cache_dir: "./tmp/cache"
  max_cache_size: 524288000  # 500 MiB

connection_pool:
  upstream_overrides:
    "127.0.0.1:9000":   # the store's host:port as the client addresses it (IP literal → exact match, path-style only)
      scheme: http      # Plaintext mode: the proxy connects to the store over plaintext TCP, no TLS handshake
```

Start the proxy:

```bash
cargo run --release -- -c config.yaml  # no sudo needed in proxy-only mode
```

On startup the proxy logs a warning naming the endpoint and the protection being waived (cleartext); this is expected for a plaintext override. Validated-TLS overrides produce no warning because nothing is waived.

**3. Configure the client.** The client signs against the store's `Host` (`127.0.0.1:9000`) and routes through the proxy's forward-proxy port. IP endpoints require path-style addressing:

```bash
export AWS_ACCESS_KEY_ID=minioadmin               # MinIO root user
export AWS_SECRET_ACCESS_KEY=minioadmin           # MinIO root password
export AWS_DEFAULT_REGION=us-east-1               # arbitrary: the store ignores it, but SigV4 needs a region
export HTTP_PROXY=http://127.0.0.1:3128           # route the client's request through the proxy's forward-proxy port
aws configure set default.s3.addressing_style path  # IP endpoints must use path-style (no virtual-hosted form)
```

The client always uses `--endpoint-url http://127.0.0.1:9000`, `http://` because the client speaks plaintext HTTP to the proxy. The proxy reads the target authority (`127.0.0.1:9000`), matches the override, and connects to the store over plaintext on port 9000.

`NO_PROXY` caveat: make sure `127.0.0.1` and `localhost` are **not** in `NO_PROXY`, or the AWS CLI will bypass `HTTP_PROXY` and talk to the store directly, skipping the proxy and the cache entirely. (On EC2, still keep `export NO_PROXY=169.254.169.254` so IMDS credential retrieval bypasses the proxy; just don't add `127.0.0.1`.)

**4. Exercise it (PUT, GET miss, GET hit).** This is the verification that the recipe works:

```bash
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://test-bucket                          # create a bucket on the store
echo "hello plaintext upstream" > /tmp/obj.txt
aws --endpoint-url http://127.0.0.1:9000 s3 cp /tmp/obj.txt s3://test-bucket/obj.txt      # PUT: cached on write
time aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://test-bucket/obj.txt /tmp/get1.txt  # first GET: cache MISS, fetched from the store
time aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://test-bucket/obj.txt /tmp/get2.txt  # second GET: cache HIT, served by the proxy
```

Expected outcome: both GETs return identical bytes (`diff /tmp/get1.txt /tmp/get2.txt` is empty), and the second GET is served from cache. Confirm the hit on the proxy side:

```bash
curl -s http://127.0.0.1:9090/metrics | grep cache_hit_rate  # hit rate rises after the second GET
```

The hit is also visible on the dashboard (`http://127.0.0.1:8081`) and in the access log (the second GET is logged as served from cache).

### Fronting real S3 over plaintext HTTP

A publicly-resolvable endpoint reached over HTTP can be overridden the same way. To front real S3 over plaintext HTTP on port 80, override the regional endpoint on `:80`:

```yaml
connection_pool:
  upstream_overrides:
    "s3.us-east-1.amazonaws.com:80":   # a DNS name covers itself and all subdomains (e.g. bucket.s3.us-east-1.amazonaws.com)
      scheme: http
```

```bash
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1                                   # http:// → port 80 → matches the :80 override
```

The proxy resolves `s3.us-east-1.amazonaws.com` via its configured external DNS (`connection_pool.dns_servers`, default Google/Cloudflare), never `/etc/hosts`. It reaches the real S3 origin even in a hosts-file deployment where the name would otherwise loop back to the proxy. The signed `Host` is forwarded verbatim and SigV4 validates at the origin, because the signature covers the `Host` and path, not the scheme. Because the key is a DNS name, the single entry also covers virtual-hosted bucket addressing (`bucket.s3.us-east-1.amazonaws.com`).

### Cache-key caveat: the origin is not part of the key

A cache key is a bucket name and an object path. It carries neither the host, the port, nor the transport mode, so two upstreams serving the same bucket name resolve to one cache key. Two upstreams on the **same host but different ports** (for example `127.0.0.1:9000` and `127.0.0.1:9001`) are the same origin. An object at the same key path on both stores collides in the cache, and whichever is fetched first is served until it is evicted or invalidated. The same object served over different transports of one host is likewise not duplicated.

Two distinct hostnames are two distinct origins, and a bucket name belongs to the first origin that caches it. A request for that name from the other origin is forwarded and served uncached, so once the name is bound neither store is served the other's entries. To cache for both stores, address them under distinct bucket names (or distinct key paths). A distinct hostname prevents the collision rather than resolving it. See [Configuration Guide: Cache-key note](CONFIGURATION.md#cache-key-note) for the canonical statement of this rule and [One Bucket Name at Two Origins](CACHING.md#one-bucket-name-at-two-origins) for the origin binding.

### Security: local development and secure network segments only

Plaintext (`scheme: http`) and unvalidated TLS (`scheme: https` with `validate_tls: false`) each waive a security protection on the proxy→origin leg: cleartext exposure, or loss of MITM protection. They are for local development, or for a secure network segment where cleartext is acceptable. Do not enable them across any other network. Validated HTTPS on a non-443 port (`scheme: https`, with `validate_tls` defaulting to `true`) waives nothing and is safe to use anywhere the default 443 path is.

## Configuration files

- `config/config.example.yaml` - Complete configuration with all options documented

## Verification

### Test basic functionality

```bash
# Test proxy is running
curl -I http://<proxy-host>:8080/health

# Test S3 access through proxy
aws s3 ls s3://your-bucket \
  --endpoint-url "http://s3.us-east-1.amazonaws.com"
```

### Monitor logs

```bash
# Watch application logs (replace <app-log-dir> with your configured app_log_dir)
tail -f <app-log-dir>/$(hostname)/s3-proxy.log*

# Watch access logs (replace <access-log-dir> with your configured access_log_dir)
tail -f <access-log-dir>/$(date +%Y/%m/%d)/*
```

## Troubleshooting

### Common issues

1. **Permission denied on ports 80/443**:
   ```bash
   # Use sudo for privileged ports
   sudo cargo run --release -- -c config/config.example.yaml
   ```

   Or avoid sudo entirely by switching to [Proxy-Only Mode](#proxy-only-mode) (`server.mode: proxy_only` on port 3128). Clients then route via `HTTP_PROXY=http://<proxy-host>:3128` instead of DNS or hosts file.

2. **AWS credentials not configured on the client**:
   The proxy does not use AWS credentials; it forwards signed requests from clients. Ensure credentials are configured on the *client* machine:
   ```bash
   aws configure
   # or set environment variables
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

3. **Connection refused**:
   - Check proxy is running: `curl http://<proxy-host>:8080/health`
   - Check client routing: `HTTP_PROXY`, the DNS zone or the hosts file
   - Check firewall settings

4. **Cache not working**:
   - Use an `http://` endpoint URL, directly or through the TLS proxy listener; HTTPS passthrough on port 443 does not cache
   - Check cache directory permissions: `ls -la <cache-dir>/`
   - Monitor cache metrics: `curl http://<proxy-host>:9090/metrics`

### Debug mode

Enable debug logging by setting environment variable:

```bash
sudo RUST_LOG=debug cargo run --release -- -c config/config.example.yaml  # sudo resets the environment, so set RUST_LOG after it
```

## Next steps

- **A fleet on AWS**: [AWS deployment](AWS_DEPLOYMENT.md)
- **Security model**: [Security](../SECURITY.md)
- **Configuration**: [Configuration reference](CONFIGURATION.md)
- **Dashboard**: [Dashboard](DASHBOARD.md)
- **Testing**: [Developer guide: testing strategy](DEVELOPER.md#testing-strategy)
- **Everything else**: [Documentation index](README.md)

## System requirements and sizing

In the measurement below, network bandwidth limited the proxy before CPU or disk did, so size for network bandwidth first.

### Observed resource usage

Tested on EC2 (m6in.2xlarge) with 100 concurrent clients downloading 1.4 GB each (140 GB total) through 3 proxies sharing an NFS cache volume:

| Resource | Peak Usage | Notes |
|----------|-----------|-------|
| CPU | 10% of 8 vCPU | ~1 core used |
| Network out (to clients) | 4.5 Gbps per proxy | 10.7 Gbps aggregate |
| Network in (from S3) | 0.18 Gbps per proxy | 95% served from cache |
| RAM cache | 1 GB configured | Holds hot data for repeated reads |

### Sizing guidance

| Deployment | Clients | Server Spec | Notes |
|------------|---------|-------------|-------|
| Development | 1-5 | 2 vCPU, 1 Gbps network | Any modern server |
| Small | 5-50 | 2-4 vCPU, 10+ Gbps network | Network is the constraint |
| Medium | 50-200 | 4 vCPU, 25 Gbps network | More RAM for larger cache |
| Large | 200+ | A fleet on a shared NFS cache volume | Scale out, not up |

**Key considerations**:
- Network bandwidth is the first constraint; choose servers or instances with high network throughput
- CPU use was about one core at 100 clients in the measurement above
- The RAM cache is off by default (`ram_cache_enabled`) and holds 512 MiB when enabled (`max_ram_cache_size`); raise it for a hot working set
- Size the disk cache for the working set; on NFS, one cache volume serves the whole fleet
- Download coordination collapses concurrent requests for the same object into one S3 fetch
- Scale out by adding proxies rather than enlarging one
