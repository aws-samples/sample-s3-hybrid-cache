# Docker deployment

This guide builds Hybrid Cache for Amazon S3 into a container image and runs it. It is an
alternative to the [systemd service](GETTING_STARTED.md#running-as-a-systemd-service-recommended-for-production)
in [Getting started](GETTING_STARTED.md); use whichever fits your orchestration (Kubernetes,
Amazon ECS, Nomad or plain `docker compose`).

Commands below use `docker`. Any Docker-API-compatible CLI works the same way, including
[Finch](https://runfinch.com/) (`finch build`, `finch run`, `finch compose`).

## Contents

- [Why the binary fits a container](#why-the-binary-fits-a-container)
- [Dockerfile](#dockerfile)
  - [Base image choice](#base-image-choice)
  - [Health check trade-off](#health-check-trade-off)
- [Building](#building)
- [Configuration in a container](#configuration-in-a-container)
  - [Configuration file or environment variables](#configuration-file-or-environment-variables)
- [Cache persistence](#cache-persistence)
- [Privilege model: ports 80 and 443 or proxy_only](#privilege-model-ports-80-and-443-or-proxy_only)
- [TLS proxy listener in a container](#tls-proxy-listener-in-a-container)
- [docker compose example](#docker-compose-example)
- [Bind addresses: loopback defaults in a container](#bind-addresses-loopback-defaults-in-a-container)
- [Fleets sharing a cache volume](#fleets-sharing-a-cache-volume)
  - [Each container needs its own hostname](#each-container-needs-its-own-hostname)
- [Upgrading](#upgrading)
- [Kubernetes notes](#kubernetes-notes)

## Why the binary fits a container

The release binary is a single executable. It statically links the Rust standard library and
every crate, and dynamically links only glibc and a few standard Linux libraries; see
[Binary portability](GETTING_STARTED.md#binary-portability). TLS uses `rustls` rather than
OpenSSL, so the build needs no `libssl-dev` or `pkg-config`. A minimal runtime image with no
shell is enough: it needs glibc and little else.

## Dockerfile

Save this as `Dockerfile` at the root of a clone of this repository and build from that
checkout. `COPY src ./src` builds the tree you have checked out, so there is no commit hash
to bump per release and no fetch from GitHub during the build.

```dockerfile
# syntax=docker/dockerfile:1

# Use the Rust version pinned in rust-toolchain.toml.
FROM rust:1.98-slim-bookworm AS builder

WORKDIR /build

# --- Dependency layer ---
# Copy only the manifest and build script first, so this layer (and its cargo
# registry download) is cached across builds and rebuilds only when
# Cargo.toml or Cargo.lock change, not on every source edit.
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "// dummy" > src/lib.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --release

# --- Source layer ---
# Bring in the real source and rebuild. The `touch` stops cargo's mtime-based
# fingerprinting from treating the dummy main.rs build above as current.
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    touch src/main.rs && \
    cargo build --release && \
    cp target/release/s3-proxy /build/s3-proxy && \
    strip /build/s3-proxy

# --- Runtime ---
# distroless/cc holds glibc and libgcc, with no shell and no package manager, in
# line with the systemd unit's hardening (NoNewPrivileges, ProtectSystem=strict;
# see config/s3-proxy.service). The Debian release must be the builder's
# (bookworm, 12) or newer, per the glibc rule in Binary Portability; debian12
# matches it exactly.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /build/s3-proxy /usr/bin/s3-proxy

# Every port the proxy can bind; trim the list to the ones your config.yaml
# enables. Standard mode uses 80 and 443, proxy_only mode uses proxy_port
# (default 3128) instead, and the TLS proxy listener uses 3129 in either mode.
# EXPOSE only documents ports; publishing them is -p at run time.
EXPOSE 80 443 3128 3129 8080 8081 9090

ENTRYPOINT ["/usr/bin/s3-proxy"]
CMD ["-c", "/etc/s3-proxy/config.yaml"]
```

How this Dockerfile differs from a typical one:

- **`COPY src ./src`, not a GitHub tarball fetch.** The image has the same provenance as any
  other artifact you build from the checkout, in CI or in a release script.
- **No `apt-get install libssl-dev pkg-config`.** Dockerfiles for OpenSSL-linked Rust crates
  often carry these packages; this build does not use them (see
  [Why the binary fits a container](#why-the-binary-fits-a-container)).
- **The `nonroot` distroless variant.** It runs as a non-root UID, which cannot bind ports 80
  and 443 without extra steps; see
  [Privilege model](#privilege-model-ports-80-and-443-or-proxy_only).
- **No `--bin s3-proxy` flag.** `cargo build --release` without `--examples` builds the library
  and the one binary, not the files under `examples/`.
- **Base images named by tag.** A tag such as `nonroot` or `bookworm-slim` moves as its
  publisher ships updates, so two builds can get different layers. For a reproducible build,
  pin each `FROM` to a digest (`FROM gcr.io/distroless/cc-debian12:nonroot@sha256:<digest>`)
  and update the digest deliberately.

### Base image choice

| | distroless/cc (recommended) | debian:bookworm-slim | scratch |
|---|---|---|---|
| Shell and package manager | None | Full (apt, bash, coreutils) | None |
| Attack surface if the proxy is compromised | Minimal: no tools to pivot with | The normal Debian userland | Minimal, but no CA certificates and no glibc |
| `docker exec sh` for debugging | No | Yes | No |
| Built-in Docker `HEALTHCHECK` | No (no executable to run) | Yes, with `curl` or `wget` installed | No |
| CA certificates for upstream TLS | Bundled | Needs `ca-certificates` installed | Copy them in yourself |

`scratch` needs the CA bundle and glibc handled by hand, or a static musl build, which this
project does not produce. That is more setup than distroless/cc for no security gain, since
distroless already leaves out the shell and package manager. Choose `debian:bookworm-slim` only
when you want an in-container `HEALTHCHECK` or interactive debugging and accept the larger
attack surface for it.

### Health check trade-off

A distroless runtime has no shell and no `curl` or `wget`, so a Docker
`HEALTHCHECK CMD curl ...` instruction has nothing to execute. Decide which approach you want
before copying a Dockerfile that includes one.

- **Recommended:** leave `HEALTHCHECK` out of the image and let your orchestrator probe
  `:8080/health` over the network: a Kubernetes `livenessProbe` or `readinessProbe` with
  `httpGet`, an Amazon ECS or load balancer target group health check, or an external monitor.
  None of these needs an executable inside the container. Each makes an HTTP request to the
  published port, which needs `health.bind_address: "0.0.0.0"` (see
  [Bind addresses](#bind-addresses-loopback-defaults-in-a-container)).
- **For the `healthcheck:` key in `docker compose`**, which runs only in-container commands,
  switch the runtime stage to `debian:bookworm-slim` and install `curl`:

  ```dockerfile
  FROM debian:bookworm-slim
  RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
      && rm -rf /var/lib/apt/lists/*
  COPY --from=builder /build/s3-proxy /usr/bin/s3-proxy
  HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
      CMD curl -f http://localhost:8080/health || exit 1
  ENTRYPOINT ["/usr/bin/s3-proxy"]
  CMD ["-c", "/etc/s3-proxy/config.yaml"]
  ```

  This gives up the shell-free image for an in-container check, which suits plain
  `docker compose` with no external health checking.

## Building

```bash
docker build -t s3-hybrid-cache:<version> .  # <version> from Cargo.toml
```

Tag with the version from `Cargo.toml` rather than `latest`, so each running container maps to
one `s3-proxy --version` output.

## Configuration in a container

### Configuration file or environment variables

The proxy reads its configuration in three layers: the YAML file, then environment variable
overrides, then command-line flags; see the [Configuration reference](CONFIGURATION.md). A
container can use both of the first two.

**Mount a configuration file** (recommended for anything beyond the simplest setup):

```bash
docker run -v /host/path/config.yaml:/etc/s3-proxy/config.yaml:ro ...
```

With the configuration outside the image, changing `cache_dir` or a TTL takes a container
restart, not a rebuild. A `cache_rules.json` in `cache_dir` needs neither: the proxy re-reads it
on its own (see [Cache rules](CONFIGURATION.md#cache-rules)). Existing configuration keeps
working across upgrades ([Upgrading](UPGRADING.md)), so the image can change while the mounted
file stays the same.

**Or override specific fields with environment variables.** The ones most useful in a container
are below. For the complete set, the units each one expects and the fields with no override,
see [Environment variable reference](CONFIGURATION.md#environment-variable-reference).

| Variable | Overrides |
|---|---|
| `CACHE_DIR` | `cache.cache_dir` |
| `ACCESS_LOG_DIR` | `logging.access_log_dir` |
| `APP_LOG_DIR` | `logging.app_log_dir` |
| `LOG_LEVEL` | `logging.log_level` |
| `HTTP_PORT` / `HTTPS_PORT` | `server.http_port` / `server.https_port` |
| `MAX_CONCURRENT_REQUESTS` | `server.max_concurrent_requests` |
| `RAM_CACHE_ENABLED` / `WRITE_CACHE_ENABLED` / `COMPRESSION_ENABLED` | The matching booleans |
| `DASHBOARD_ENABLED` / `DASHBOARD_PORT` / `DASHBOARD_BIND_ADDRESS` | Dashboard settings |
| `OTLP_ENDPOINT` / `OTLP_EXPORT_INTERVAL` | OTLP metrics export (setting the endpoint also enables it) |

Duration variables take a whole number of seconds, not a duration string, and a value that
fails to parse is ignored without a message.

The variable names have no prefix. `RUST_LOG`, when set, overrides `logging.log_level` (and
`LOG_LEVEL`) for the application log.

Environment variables suit per-container overrides from the orchestrator, for example a
Kubernetes `env` block that sets `CACHE_DIR` per pod, without templating several YAML files.
When more than a couple of fields change, mount a full configuration file, which is easier to
diff and version.

## Cache persistence

A `cache_dir` in the container's writable layer is lost on every `docker rm` or recreate,
and most orchestrators recreate containers routinely (rolling updates, node drains, restarts
after a crash). Mount `cache_dir` as a volume or a bind mount:

```bash
docker run \
  -v s3-proxy-cache:/var/lib/s3-proxy/cache \
  -v s3-proxy-logs:/var/lib/s3-proxy/logs \
  -v s3-proxy-state:/var/lib/s3-proxy/state \
  ...
```

The third volume holds the proxy's host-local state, a small pending-stamp log. Set
`logging.local_state_dir` to its path (`/var/lib/s3-proxy/state`), as the compose example's
`config.yaml` does (see [Logging configuration](CONFIGURATION.md#logging-configuration)). The
field's default, `/var/lib/s3-proxy`, is not writable by the image's user, so without the
volume the proxy logs an ERROR at startup, reports `cache.write_stamp_log_memory_only` as `1`
and holds write-order retries in memory only. Put this volume on the host's own disk, never on
the cache volume.

The `nonroot` distroless image runs as the fixed non-root UID 65532 (`nonroot`). The cache, log
and state storage must be writable by that UID. How to arrange that depends on whether you use
a named volume or a host bind mount, which behave differently on VM-backed runtimes.

**Named volumes (recommended):** change the ownership from inside a throwaway container, then
reuse the volume by name. The `chown` runs as root inside that container, so it needs no host
privileges, and the proxy container sees the new ownership:

```bash
for v in s3-proxy-cache s3-proxy-logs s3-proxy-state; do
  docker volume create "$v"
  docker run --rm -v "$v":/mnt alpine chown -R 65532:65532 /mnt
done
```

**Host bind mounts:** on a Linux host with a native container runtime, a host-side
`sudo chown -R 65532:65532 /host/cache` takes effect inside the container. VM-backed runtimes
on macOS (Finch with Lima, Docker Desktop) do not pass it through reliably: the directory can
appear owned by `root` inside the container whatever its owner on the host, because the
file-sharing layer between host and VM maps UIDs itself. The proxy then fails to start with a
`Failed to create cache subdirectory` error reporting `Permission denied`. If you bind-mount,
for example to inspect cache contents from the host, check the ownership the container sees:

```bash
docker run --rm -v /host/cache:/mnt/cache alpine ls -la /mnt/cache
```

The proxy sets `umask(0o077)` at startup, so the files it creates are readable only by the UID
that created them. If you run the container as root or as another UID, the volume ownership
must match that UID, or the proxy cannot write its cache and log directories and fails with the
same error.

## Privilege model: ports 80 and 443 or proxy_only

Binding a port below 1024 needs root or `CAP_NET_BIND_SERVICE` in the process that binds it,
which is the proxy inside the container. Publishing a port (`-p 80:3128`) does not change that:
the host-side forwarding and the container-side `bind()` are separate operations, and the
kernel checks the privilege on the `bind()`. This is the container form of the systemd unit's
[privilege trade-off](GETTING_STARTED.md#privilege-tradeoff-userroot). The options, recommended
first:

- **`proxy_only` mode (recommended for containers).** Its listeners, the forward proxy on 3128
  and the TLS proxy listener on 3129, use unprivileged ports, so the `nonroot` image works
  unchanged:

  ```yaml
  server:
    mode: "proxy_only"
    proxy_port: 3128
  ```

  [Proxy-only mode](GETTING_STARTED.md#proxy-only-mode-no-sudo-required) describes the same
  mode for hosts without root.
- **Standard mode on unprivileged container ports.** Set `server.http_port` and
  `server.https_port` to unprivileged ports (for example `8000` and `8443`) and publish them on
  the host's privileged ports with `-p 80:8000 -p 443:8443`. Only the host-side publish uses
  ports 80 and 443, and the container runtime's own privileged daemon performs it, so no
  capability is needed. Use this when clients must reach the proxy on port 80, as in the DNS
  alternative in [TLS proxy listener in a container](#tls-proxy-listener-in-a-container).
- **Standard mode on ports 80 and 443 inside the container.** Run the container as root (drop
  the `nonroot` tag or add `USER root`, which undoes part of the hardening in
  [Base image choice](#base-image-choice)), or keep the non-root user and grant the capability:

  ```bash
  docker run --cap-add=NET_BIND_SERVICE --user=65532:65532 ...
  ```

  `--cap-add` puts the capability in the container's permitted set, and a non-root process does
  not get it in its effective set automatically. The binary carries no file capabilities, so on
  many runtimes this combination still fails to bind. Test it in your environment, and use one
  of the options above if it fails.

## TLS proxy listener in a container

Point clients at the TLS proxy listener: set `HTTP_PROXY=https://<proxy-host>:3129` and
`--endpoint-url http://s3.<region>.amazonaws.com`. The client hop is encrypted and responses
are still cached, on-premises and across Regions. Where the network segment between clients and
proxies is secure and cleartext on it is acceptable, a DNS zone that resolves S3's hostnames to
the proxies is a simpler alternative: clients need no proxy setting and connect on port 80.
[Configure client routing](GETTING_STARTED.md#3-configure-client-routing) compares the options.

The listener is the `server.tls` block. Its port, 3129 by default, is unprivileged, so the
listener runs in the `nonroot` image in either server mode. Mount the certificate and key
read-only beside the configuration file, and point `cert_path` and `key_path` at them:

```bash
docker run \
  -v /host/tls/cert.pem:/etc/s3-proxy/tls/cert.pem:ro \
  -v /host/tls/key.pem:/etc/s3-proxy/tls/key.pem:ro \
  -p 3129:3129 \
  ...
```

```yaml
server:
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/s3-proxy/tls/cert.pem"
    key_path: "/etc/s3-proxy/tls/key.pem"
```

The key must be readable by UID 65532. Mount the certificate and key at run time rather than
copying them into the image: a `COPY` puts the private key in every layer of every tag you push,
including in any registry.

Only the AWS CRT transfer client verifies the proxy's certificate; botocore-based clients
(`aws s3api`, `aws s3` with the classic transfer client, boto3) encrypt the hop without
verifying it. See [Configuring clients to trust the certificate](GETTING_STARTED.md#configuring-clients-to-trust-the-certificate),
and [TLS proxy listener configuration](GETTING_STARTED.md#tls-proxy-listener-configuration) for
creating the certificate.

## docker compose example

`proxy_only` mode with the TLS proxy listener, a mounted configuration file and persistent
volumes:

```yaml
services:
  s3-proxy:
    image: s3-hybrid-cache:<version>
    build: .
    restart: unless-stopped
    ports:
      - "3129:3129"             # TLS proxy listener: the client path
      - "8080:8080"             # health (config.yaml below opens it to 0.0.0.0)
      - "9090:9090"             # metrics (config.yaml below opens it to 0.0.0.0)
      - "127.0.0.1:8081:8081"   # dashboard: host loopback only; see Bind addresses
    volumes:
      - ./config.yaml:/etc/s3-proxy/config.yaml:ro
      - ./tls:/etc/s3-proxy/tls:ro
      - s3-proxy-cache:/var/lib/s3-proxy/cache
      - s3-proxy-logs:/var/lib/s3-proxy/logs
      - s3-proxy-state:/var/lib/s3-proxy/state
    environment:
      - LOG_LEVEL=info
      # The dashboard defaults to 127.0.0.1, which in a container is reachable only
      # from inside it. The loopback-only host publish above keeps it off other hosts.
      - DASHBOARD_BIND_ADDRESS=0.0.0.0

volumes:
  s3-proxy-cache:
  s3-proxy-logs:
  s3-proxy-state:
```

Change the ownership of all three volumes to UID 65532 first, as in
[Cache persistence](#cache-persistence).

The matching `config.yaml`:

```yaml
server:
  mode: "proxy_only"
  proxy_port: 3128
  tls:
    enabled: true
    tls_proxy_port: 3129
    cert_path: "/etc/s3-proxy/tls/cert.pem"
    key_path: "/etc/s3-proxy/tls/key.pem"

cache:
  cache_dir: "/var/lib/s3-proxy/cache"
  max_cache_size: 10737418240  # 10 GiB

logging:
  access_log_dir: "/var/lib/s3-proxy/logs/access"
  app_log_dir: "/var/lib/s3-proxy/logs/app"
  local_state_dir: "/var/lib/s3-proxy/state"  # the host-local state volume, not the cache

# Both default to 127.0.0.1, which inside a container is unreachable through -p.
health:
  bind_address: "0.0.0.0"
metrics:
  bind_address: "0.0.0.0"
```

On the client:

```bash
export HTTP_PROXY=https://<proxy-host>:3129
aws s3 cp s3://<bucket>/<key> ./local \
  --endpoint-url http://s3.<region>.amazonaws.com \
  --region <region>
```

The forward proxy on 3128 is the cleartext form of the same client setting
(`HTTP_PROXY=http://<proxy-host>:3128`). Publish `3128:3128` for it only where the network
segment condition above holds.

## Bind addresses: loopback defaults in a container

A listener bound to `127.0.0.1` inside a container is reachable only from inside that container,
which has its own loopback interface. `-p 8081:8081` then publishes a port that nothing listens
on from the host's side. On a host, a loopback bind means reaching the listener through an SSH
tunnel; in a container, the listener looks broken.

The three monitoring listeners (health, metrics and dashboard) default to loopback, because none
of them requires authentication:

| Listener | Port | Default `bind_address` | Reachable through `-p` without a change |
|---|---|---|---|
| Health | 8080 | `127.0.0.1` | No |
| Metrics | 9090 | `127.0.0.1` | No |
| Dashboard | 8081 | `127.0.0.1` | No |

A Kubernetes `httpGet` probe, an Amazon ECS or load balancer health check, and a Prometheus
scrape all connect to the container's own address, so each fails against a loopback bind. To
reach a listener from outside the container, set its address:

```yaml
health:
  bind_address: "0.0.0.0"
metrics:
  bind_address: "0.0.0.0"
dashboard:
  bind_address: "0.0.0.0"
```

The dashboard also has an environment variable, often the cleaner choice in a compose file.
Health and metrics have none, so set them in `config.yaml`:

```yaml
environment:
  - DASHBOARD_BIND_ADDRESS=0.0.0.0
```

With its address changed, the dashboard is reachable without authentication from anywhere the
published port is reachable. Publish it only on an interface you control: bind the host side to
loopback (`-p 127.0.0.1:8081:8081`) and tunnel in, or restrict the port with security groups or
firewall rules. Keep it off the internet.

`config/config.example.yaml` sets `bind_address: "0.0.0.0"` in all three sections, so a
configuration copied from it already has them. A minimal configuration that omits them gets the
`127.0.0.1` default.

## Fleets sharing a cache volume

A fleet of proxy containers on one cache volume follows the same rules as a fleet on hosts;
[Running a fleet on one cache volume](SHARED_STORAGE.md) explains them. Two of them depend on how the container gets
its mount: the cache volume must be mounted with `lookupcache=pos`, and it must enforce advisory
file locks between hosts. Both are required for the proxies to coordinate correctly; see
[NFS mount requirements](CONFIGURATION.md#nfs-mount-requirements).

There are two ways to meet them:

- **Mount on the host and bind-mount into the container.** The host owns the NFS mount, with
  `lookupcache=pos` in `/etc/fstab` or on the `mount` command, and each container bind-mounts
  the mounted path. This is the easiest to verify, since `mount | grep nfs` on the host shows the
  options in effect.
- **Pass the options to the volume driver.** Docker's built-in `local` driver accepts NFS
  options, so each volume mounts itself:

  ```bash
  docker volume create --driver local \
    --opt type=nfs \
    --opt o=addr=<nfs-server>,nfsvers=4.1,lookupcache=pos \
    --opt device=:/export/s3-proxy-cache \
    s3-proxy-cache
  ```

  Confirm the options took effect (`docker volume inspect`, then check the mount inside a
  running container). A dropped `lookupcache=pos` is logged once at startup, naming the mount, and the proxy still
  starts while the proxies no longer coordinate correctly. Leave `nolock` and `local_lock` out of the option string: either makes
  advisory locks local to one host, so every container can hold the same coordination lock at
  once.

Locking is the requirement containers most often get wrong, because a container that shares the
host's network and mount namespace still depends on the host having a working NFS lock path.
Test it from two containers on different hosts. Two containers on the same host can appear to
lock correctly even when the mount does not enforce locks between hosts:

```bash
docker exec <container-a> flock -x /cache/.locktest -c 'sleep 30'  # On host A: hold an exclusive lock for 30 seconds
docker exec <container-b> flock -n -x /cache/.locktest -c true && echo BROKEN || echo OK  # On host B, while A holds it: must print OK
```

`BROKEN` means the volume is unsafe for a fleet. The check needs `flock(1)` in the image. The
distroless image has none, so run the same check from the hosts against the mounted path.

### Each container needs its own hostname

Proxies sharing a cache volume must have distinct hostnames; see
[Each proxy needs its own hostname](SHARED_STORAGE.md#each-proxy-needs-its-own-hostname) for
why. Docker gives each container its own hostname by default, so the common case needs nothing.
Two deployment choices break it:

- **A fixed `--hostname` (or `hostname:` in compose) repeated across replicas.** Set it per
  replica, or leave it unset and let Docker assign one.
- **`network_mode: host` on machines cloned from one image.** The container takes the host's
  hostname, and cloned hosts often share one. Set distinct host names before deploying.

Check it from the running containers:

```bash
docker exec <container-a> cat /etc/hostname
docker exec <container-b> cat /etc/hostname
```

Two identical values on different proxies are the problem. Each proxy also reports it:
`cache.hostname_collisions_total` on `/metrics` rises above zero, and one ERROR names both
identities. When two containers share a hostname and both run the proxy as process 1, they hold
one whole identity, and each also stops write-caching, counted as `hostname_not_unique` in
`signed_put.skipped_puts_total`; see
[Each proxy needs its own hostname](SHARED_STORAGE.md#each-proxy-needs-its-own-hostname).

## Upgrading

Upgrading replaces the binary and restarts the proxy; existing configuration keeps working.
Before upgrading a fleet to 3.0.0, read [Upgrading to 3.0.0](UPGRADING.md#upgrading-to-300).

In a container, rebuild the image from the new source and recreate the container. The cache
volume is not part of the container's writable layer, so its contents survive the recreate:

```bash
docker build -t s3-hybrid-cache:<new-version> .
docker compose up -d  # recreates the container, keeps the named volumes
```

Confirm which binary you got. Both commands work against a distroless image, because they run
the proxy binary directly rather than through a shell:

```bash
docker run --rm --entrypoint /usr/bin/s3-proxy s3-hybrid-cache:<new-version> --version
docker logs <container> | grep Starting  # the proxy logs its version on startup
```

The startup log line is the more useful of the two: it reports the version of the binary in the
running container rather than of an image tag.

## Kubernetes notes

The container patterns above carry over to Kubernetes, with these points:

- **`livenessProbe` and `readinessProbe`** should use `httpGet` against `:8080/health`, not an
  `exec` probe, because the distroless runtime has no shell for an `exec` probe (see
  [Health check trade-off](#health-check-trade-off)). The kubelet probes the pod IP, so set
  `health.bind_address: "0.0.0.0"`; with the loopback default every probe fails.
- **`securityContext.runAsNonRoot: true`** works with the `nonroot` distroless tag. Pair it with
  `proxy_only` mode (see [Privilege model](#privilege-model-ports-80-and-443-or-proxy_only)) so
  the pod needs no `NET_BIND_SERVICE`.
- **Cache volume:** for a fleet sharing one cache, use a `PersistentVolumeClaim` on a file system
  that gives the consistency `lookupcache=pos` gives and enforces advisory locks across hosts; see
  [Fleets sharing a cache volume](#fleets-sharing-a-cache-volume). A `ReadWriteMany` claim on
  Amazon EFS or on an NFS-backed storage class works. Check the mount options the storage class
  applies, since `mountOptions` on a `StorageClass` is where `nolock` or `local_lock` would
  appear, and run the two-host lock check between replicas on different nodes.
- **A ConfigMap for `config.yaml`**, mounted as a volume, takes the place of the bind mount in
  the [docker compose example](#docker-compose-example). Changing the ConfigMap and rolling the
  pods is the Kubernetes form of editing the configuration file and restarting the proxy.
