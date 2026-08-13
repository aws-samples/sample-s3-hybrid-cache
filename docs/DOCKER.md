# Docker Deployment

Building and running Hybrid Cache for Amazon S3 in a container. This is an alternative
to the [systemd service](GETTING_STARTED.md#running-as-a-systemd-service-recommended-for-production)
path in [Getting Started](GETTING_STARTED.md) — pick whichever fits your orchestration
(Kubernetes, ECS, Nomad, plain `docker compose`).

Commands below use `docker`; any Docker-API-compatible CLI works the same way,
including [Finch](https://runfinch.com/) (`finch build`, `finch run`, `finch
compose`) — this guide's Dockerfile and compose file were verified end-to-end with
Finch (see [Verified](#verified) below).

## Table of Contents

- [Why the Binary Fits a Container](#why-the-binary-fits-a-container)
- [Dockerfile](#dockerfile)
  - [Base Image Choice](#base-image-choice)
  - [Healthcheck Tradeoff](#healthcheck-tradeoff)
- [Building](#building)
- [Configuration in a Container](#configuration-in-a-container)
  - [Config File vs Environment Variables](#config-file-vs-environment-variables)
- [Cache Persistence](#cache-persistence)
- [Privilege Model: Ports 80/443 vs proxy_only](#privilege-model-ports-80443-vs-proxy_only)
- [docker compose Example](#docker-compose-example)
- [Bind Addresses: Loopback Defaults Are a Container Trap](#bind-addresses-loopback-defaults-are-a-container-trap)
- [Multi-Instance / Shared Cache](#multi-instance--shared-cache)
- [TLS Proxy Listener in a Container](#tls-proxy-listener-in-a-container)
- [Upgrading](#upgrading)
- [Kubernetes Notes](#kubernetes-notes)
- [Verified](#verified)

## Why the Binary Fits a Container

The release binary is a single ~20 MB executable that statically links the Rust
standard library and all crates, and dynamically links only glibc and a small set of
standard Linux libraries — see
[Binary Portability](GETTING_STARTED.md#binary-portability). No OpenSSL: TLS is
`rustls`, so the build needs no `libssl-dev`/`pkg-config`. That makes a minimal,
non-shell runtime image a natural fit rather than a stretch — it needs a glibc and
essentially nothing else.

## Dockerfile

Build from the source already in this repository, not by re-fetching a tarball from
GitHub by commit hash inside the Dockerfile. Pinning a commit hash in an `ARG` is a
legitimate pattern for building a standalone image *outside* a clone of this repo —
it does give a reproducible, inspectable record of what's running. It's the wrong
choice here specifically because the Dockerfile already lives inside the checkout
it would be re-fetching: the pinned hash has to be bumped by hand for every release
and can silently drift from the actual `HEAD` you're building at, and the fetch adds
a build-time dependency on GitHub being reachable for no benefit over `COPY src
./src`, which can't drift because it *is* the checkout.

```dockerfile
# syntax=docker/dockerfile:1

# Keep this pinned to the same version as rust-toolchain.toml and the CI image tag
# in .gitlab-ci.yml. All three must move together — see rust-toolchain.toml's comment.
FROM rust:1.96-slim-bookworm AS builder

WORKDIR /build

# --- Dependency layer ---
# Copy only the manifest and build script first, so this layer (and its cargo
# registry download) is cached across builds and only invalidates when
# Cargo.toml/Cargo.lock actually change — not on every source edit.
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "// dummy" > src/lib.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --release

# --- Source layer ---
# Now bring in the real source and rebuild. The `touch` guards against cargo's
# mtime-based fingerprinting deciding the dummy main.rs build above is still current.
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    touch src/main.rs && \
    cargo build --release && \
    cp target/release/s3-proxy /build/s3-proxy && \
    strip /build/s3-proxy

# --- Runtime ---
# distroless/cc: glibc + libgcc, no shell, no package manager. Matches the systemd
# unit's hardening posture (NoNewPrivileges, ProtectSystem=strict, etc. — see
# config/s3-proxy.service) by removing everything an attacker could use if they
# gained code execution inside the container. Debian release must be >= the
# builder's (bookworm/12) per the Binary Portability glibc rule; debian12 pins an
# exact match so there's no ambiguity about which glibc the binary links against.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /build/s3-proxy /usr/bin/s3-proxy

# The full set of ports the proxy can bind; trim this list to the ones your
# config.yaml actually enables. See server.mode in config.example.yaml — standard
# mode uses 80/443, proxy_only uses proxy_port (default 3128) instead. EXPOSE is
# documentation only; it does not publish anything (that's -p at run time).
EXPOSE 80 443 3128 3129 8080 8081 9090

ENTRYPOINT ["/usr/bin/s3-proxy"]
CMD ["-c", "/etc/s3-proxy/config.yaml"]
```

Notable differences from a naive Dockerfile:

- **`COPY src ./src`, not a GitHub tarball fetch.** The image is built from exactly
  the checked-out commit. `docker build` in CI or a release script has the same
  provenance as any other artifact you ship.
- **No `apt-get install libssl-dev pkg-config`.** This crate doesn't link OpenSSL
  (see [Why the Binary Fits a Container](#why-the-binary-fits-a-container)). Adding
  those packages is dead weight that also doesn't help — a common copy-paste from
  Dockerfiles for openssl-linked Rust crates that doesn't apply here.
- **`nonroot` distroless variant.** Runs as a non-root UID by default. This matters
  for ports 80/443 — see [Privilege Model](#privilege-model-ports-80443-vs-proxy_only).
- **`--bin s3-proxy` is implicit.** `cargo build --release` with no `--examples`
  flag builds the lib + the one binary, not the six files under `examples/`. No
  need to exclude them explicitly.

### Base Image Choice

| | distroless/cc (recommended) | debian:bookworm-slim | scratch |
|---|---|---|---|
| Shell/package manager | None | Full (apt, bash, coreutils) | None |
| Attack surface if the proxy is compromised | Minimal — no tools to pivot with | Normal Debian userland available | Minimal, but no CA certs, no glibc |
| `docker exec sh` for debugging | No | Yes | No |
| Built-in Docker `HEALTHCHECK` | No (no executable to run) | Yes, if `curl`/`wget` installed | No |
| CA certificates for upstream TLS | Bundled | Needs `ca-certificates` installed | Needs certs copied in manually |

`scratch` needs the CA bundle and any glibc dependency handled manually (or requires
a static musl build, which this project doesn't produce) — more setup for no
security gain over distroless/cc, since distroless already strips the shell and
package manager. Use `debian:bookworm-slim` only if you specifically want an
in-container `HEALTHCHECK` or interactive debugging and are willing to trade the
smaller attack surface for it.

### Healthcheck Tradeoff

A distroless runtime has no shell and no `curl`/`wget`, so a Docker `HEALTHCHECK
CMD curl ...` instruction cannot run — there's no binary to execute. This is a real
tradeoff, not an oversight: decide before copying a Dockerfile that includes one.

- **Recommended**: skip the in-image `HEALTHCHECK` and let your orchestrator probe
  `:8080/health` over the network instead — a Kubernetes `livenessProbe`/
  `readinessProbe` with `httpGet`, an ECS/ALB target group health check, or an
  external monitor. None of these need an executable inside the container; they
  just make an HTTP request to the exposed port.
- **If you need `docker compose`'s built-in `healthcheck:`** (which only supports
  in-container commands), switch the runtime stage to `debian:bookworm-slim` and
  install `curl`:
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
  This gives up the shell-free hardening for operability. Reasonable if you're
  running plain `docker compose` without an external health-checking layer.

## Building

```bash
docker build -t s3-hybrid-cache:2.4.2 .
```

Tag with the version from `Cargo.toml` rather than `latest`, so a running container
maps back to a specific `s3-proxy --version` output — the same discipline
[Upgrading](GETTING_STARTED.md#upgrading) recommends for bare-binary deploys.

## Configuration in a Container

### Config File vs Environment Variables

The proxy loads config in three layers — YAML file, environment variable overrides,
then CLI flags — see `config.rs` and [Configuration Reference](CONFIGURATION.md).
Two ways to configure a container, and they compose:

**Mount a config file** (recommended for anything beyond the simplest setup):

```bash
docker run -v /host/path/config.yaml:/etc/s3-proxy/config.yaml:ro ...
```

Keeping config outside the image means changing `cache_dir`, TTLs, or adding
`cache_rules.json` never requires a rebuild — only a container restart. This extends
the project's [upgrade contract](UPGRADING.md) (config stays backward-compatible at
the field level, so no config edits are needed across versions) to the container
boundary: the image is the binary, the config is data.

**Or override specific fields via environment variables** — the real supported set,
from `apply_env_overrides()` in `config.rs`:

| Variable | Overrides |
|---|---|
| `CACHE_DIR` | `cache.cache_dir` |
| `ACCESS_LOG_DIR` | `logging.access_log_dir` |
| `APP_LOG_DIR` | `logging.app_log_dir` |
| `LOG_LEVEL` | `logging.log_level` |
| `HTTP_PORT` / `HTTPS_PORT` | `server.http_port` / `server.https_port` |
| `MAX_CONCURRENT_REQUESTS` | `server.max_concurrent_requests` |
| `RAM_CACHE_ENABLED` / `WRITE_CACHE_ENABLED` / `COMPRESSION_ENABLED` | corresponding booleans |
| `DASHBOARD_ENABLED` / `DASHBOARD_PORT` / `DASHBOARD_BIND_ADDRESS` | dashboard settings |
| `OTLP_ENDPOINT` / `OTLP_EXPORT_INTERVAL` | OTLP metrics export (setting the endpoint also enables it) |

There is no `S3_PROXY_`-prefixed family of variables — every name above is used
as-is. `RUST_LOG` is also honored, but separately: it overrides `logging.log_level`
at the `tracing` subscriber level (see `logging.rs`), independent of the config
system's own `LOG_LEVEL` override.

Environment variables are useful for orchestration-level per-instance overrides
(e.g. a Kubernetes `env` block setting `CACHE_DIR` differently per pod) without
templating multiple YAML files. For anything with more than a couple of fields to
change, mount a full config file — it's easier to diff and version.

## Cache Persistence

The proxy's entire value is a persistent cache. A cache directory that lives inside
the container's writable layer is destroyed on every `docker rm`/recreate, and most
orchestrators recreate containers routinely (rolling updates, node drains, restarts
on crash) — so this isn't an edge case, it's the normal lifecycle. **Always mount
`cache_dir` as a volume or bind mount**, never leave it as unmounted container
storage:

```bash
docker run \
  -v s3-proxy-cache:/var/lib/s3-proxy/cache \
  -v s3-proxy-logs:/var/lib/s3-proxy/logs \
  ...
```

The `nonroot` distroless image runs as a fixed non-root UID (65532, aka `nonroot`).
The cache and log storage must be writable by that UID, and how you achieve that
depends on whether you're using a named volume or a host bind mount — these behave
differently in practice, verified against finch's Lima-based VM:

**Named volumes (recommended)**: chown from inside a throwaway container, then reuse
the volume by name. The chown runs as root inside that container, so it needs no host
privileges, and the resulting ownership is visible to the proxy container:

```bash
docker volume create s3-proxy-cache
docker run --rm -v s3-proxy-cache:/mnt alpine chown -R 65532:65532 /mnt
```

**Host bind mounts**: on a Linux host with a native container runtime, a host-side
`sudo chown -R 65532:65532 /host/cache` maps straight through as you'd expect. On
**VM-backed runtimes on macOS** (Finch/Lima, Docker Desktop) it does not reliably do
so — the directory can appear owned by `root` inside the container regardless of the
host-side ownership, because the file sharing layer between host and VM does its own
UID mapping. The proxy then fails to start with
`ConfigError("Failed to create cache subdirectory 'metadata': Permission denied")`.
If you bind-mount (e.g. to inspect cache contents from the host), check the ownership
the *container* sees rather than trusting the host-side chown:

```bash
docker run --rm -v /host/cache:/mnt/cache alpine ls -la /mnt/cache
```

The proxy also sets `umask(0o077)` on startup (owner-only file permissions — see
`main.rs`), so files it creates inside the cache directory are readable only by the
UID that created them. If you switch the container to run as root or a different
UID, the volume ownership must match whichever UID the process actually runs as, or
the proxy will fail to write to its own cache and log directories — the exact
`ConfigError("Failed to create cache subdirectory ...: Permission denied")` message
you'll see if it doesn't.

## Privilege Model: Ports 80/443 vs proxy_only

Binding a port below 1024 requires `CAP_NET_BIND_SERVICE` or root **for the process
doing the bind**, which is the proxy inside the container. Publishing a port
(`-p 80:3128`) does not change that, because the host-side forwarding and the
container-side `bind()` are separate operations — the kernel checks the privilege on
the latter. This is the containerised form of the systemd unit's
[privilege tradeoff](GETTING_STARTED.md#privilege-tradeoff-userroot):

- **Standard mode (ports 80/443)**: run the container as root (drop the `nonroot`
  distroless tag or add `USER root` — this undoes part of the hardening in
  [Base Image Choice](#base-image-choice)), or keep the non-root user and grant the
  capability:
  ```bash
  docker run --cap-add=NET_BIND_SERVICE --user=65532:65532 ...
  ```
  Be aware that `--cap-add` puts the capability in the container's *permitted* set,
  which a non-root process does not automatically get in its *effective* set — the
  binary has no file capabilities set on it, so on many runtimes this combination
  still fails to bind. Verify it in your environment instead of assuming, and use
  one of the two options below if it doesn't work.
- **Recommended for containers: `proxy_only` mode on port 3128.** No capability
  juggling — 3128 is unprivileged, so the `nonroot` image works unmodified:
  ```yaml
  server:
    mode: "proxy_only"
    proxy_port: 3128
  ```
  This is also the path [Getting Started](GETTING_STARTED.md#proxy-only-mode-no-sudo-required)
  recommends as the simplest deployment overall, independent of containers — it
  composes cleanly here.
- **Or publish an unprivileged container port on a privileged host port**: run the
  proxy on 3128 inside the container and map it with `-p 80:3128`. The container-side
  `bind()` is unprivileged; only the host-side publish uses port 80, and that's done
  by the container runtime's own (already privileged) daemon rather than by the proxy.
  This works with any server mode without touching capabilities — the simplest option
  when you specifically need the *client-facing* port to be 80 or 443.

## docker compose Example

Using `proxy_only` mode (recommended default above) with a mounted config and
persistent volumes:

```yaml
services:
  s3-proxy:
    image: s3-hybrid-cache:2.4.2
    build: .
    restart: unless-stopped
    ports:
      - "3128:3128"             # proxy_port — the S3 traffic path
      - "8080:8080"             # health (binds 0.0.0.0 by default)
      - "9090:9090"             # metrics (binds 0.0.0.0 by default)
      - "127.0.0.1:8081:8081"   # dashboard — host loopback only; see note below
    volumes:
      - ./config.yaml:/etc/s3-proxy/config.yaml:ro
      - s3-proxy-cache:/var/lib/s3-proxy/cache
      - s3-proxy-logs:/var/lib/s3-proxy/logs
    environment:
      - LOG_LEVEL=info
      # Required for the dashboard to be reachable outside the container at all —
      # its code default is 127.0.0.1, which in a container means container-only.
      # Paired with the loopback-only host publish above so it is not world-reachable.
      # See "Bind Addresses" below.
      - DASHBOARD_BIND_ADDRESS=0.0.0.0

volumes:
  s3-proxy-cache:
  s3-proxy-logs:
```

Matching `config.yaml`:

```yaml
server:
  mode: "proxy_only"
  proxy_port: 3128

cache:
  cache_dir: "/var/lib/s3-proxy/cache"
  max_cache_size: 10737418240  # 10 GiB

logging:
  access_log_dir: "/var/lib/s3-proxy/logs/access"
  app_log_dir: "/var/lib/s3-proxy/logs/app"
```

Client side, same as any `proxy_only` deployment — see
[Option A: HTTP_PROXY](GETTING_STARTED.md#option-a-http_proxy-single-instance--no-dns-changes):

```bash
export HTTP_PROXY=http://<docker-host>:3128
aws s3 cp s3://your-bucket/key ./local \
  --endpoint-url http://s3.us-east-1.amazonaws.com \
  --region us-east-1
```

For standard mode (80/443) instead of `proxy_only`, see
[Privilege Model](#privilege-model-ports-80443-vs-proxy_only). For why the dashboard
needs `DASHBOARD_BIND_ADDRESS` when nothing else does, see
[Bind Addresses](#bind-addresses-loopback-defaults-are-a-container-trap).

## Bind Addresses: Loopback Defaults Are a Container Trap

A service bound to `127.0.0.1` inside a container is reachable only from *within that
container* — the container has its own loopback interface, so `-p 8081:8081` publishes
a port that nothing is listening on from the host's perspective. On a bare host a
loopback bind is a mild inconvenience (SSH-tunnel to reach it); in a container it
looks like the feature is broken.

The defaults differ per service, so check before publishing:

| Service | Port | Default `bind_address` | Reachable via `-p` out of the box? |
|---|---|---|---|
| Health | 8080 | `0.0.0.0` | Yes |
| Metrics | 9090 | `0.0.0.0` | Yes |
| Dashboard | 8081 | **`127.0.0.1`** | **No** — must be overridden |

The dashboard's loopback default is deliberate (it is unauthenticated and read-only —
see the `dashboard` section of
[`config/config.example.yaml`](../config/config.example.yaml)). To reach it from
outside the container, set it explicitly:

```yaml
dashboard:
  bind_address: "0.0.0.0"
```

or via the environment override, which is often cleaner in a compose file:

```yaml
environment:
  - DASHBOARD_BIND_ADDRESS=0.0.0.0
```

**Overriding this makes an unauthenticated dashboard reachable from anywhere the
published port is reachable.** Publish it only on an interface you control — bind the
host side to loopback (`-p 127.0.0.1:8081:8081`) and tunnel in, or restrict it with
security groups or firewall rules. Do not expose it to the internet.

Note that `config/config.example.yaml` ships `dashboard.bind_address: "0.0.0.0"`
explicitly, so a config copied from that file already has this set; a minimal
hand-written config that omits the `dashboard` section falls back to the `127.0.0.1`
code default.

## Multi-Instance / Shared Cache

Running more than one proxy container against a shared cache follows the same rules
as bare-metal multi-instance deployments — see
[High Availability: Shared NFS Cache Pattern](GETTING_STARTED.md#high-availability-shared-nfs-cache-pattern).
The container adds one constraint worth getting right: the shared volume must be
mounted with `lookupcache=pos`. That is a correctness requirement for cache
coordination, not a performance tweak — see
[Configuration Guide - Multi-Instance Coordination](CONFIGURATION.md#multi-instance-coordination).

Two ways to satisfy it:

- **Mount on the host, bind-mount into the container.** The host owns the NFS mount
  (with `lookupcache=pos` in `/etc/fstab` or the `mount` command), and each container
  bind-mounts the already-mounted path. Simplest to verify, since `mount | grep nfs`
  on the host shows exactly what options are in effect.
- **Pass the options to the volume driver.** Docker's built-in `local` driver accepts
  NFS options directly, so the mount happens per-volume rather than on the host:
  ```bash
  docker volume create --driver local \
    --opt type=nfs \
    --opt o=addr=<nfs-server>,nfsvers=4.1,lookupcache=pos \
    --opt device=:/export/s3-proxy-cache \
    s3-proxy-cache
  ```
  Confirm the option actually landed (`docker volume inspect`, then check the mount
  inside a running container) rather than assuming — a silently-dropped
  `lookupcache=pos` degrades coordination correctness without any error.

## TLS Proxy Listener in a Container

If you terminate TLS at the proxy itself (see
[TLS Proxy Listener Configuration](GETTING_STARTED.md#tls-proxy-listener-configuration)),
mount the certificate and key read-only alongside the config:

```bash
docker run \
  -v /host/tls/cert.pem:/etc/proxy/tls/cert.pem:ro \
  -v /host/tls/key.pem:/etc/proxy/tls/key.pem:ro \
  -p 3129:3129 \
  ...
```

Do not `COPY` certificates into the image — that bakes a private key into every
layer of every tag you push, including to any registry. Mount them at runtime.

## Upgrading

Same flow as [Upgrading](GETTING_STARTED.md#upgrading), translated to containers:
rebuild the image from the new source, then recreate the container. The cache
volume is preserved across the recreate (it's not part of the container's
writable layer), so there's no cold-start cost:

```bash
docker build -t s3-hybrid-cache:<new-version> .
docker compose up -d  # recreates the container, keeps the named volumes
```

Confirm which binary you actually got. Both of these work against a distroless image
because they exec the proxy binary directly rather than going through a shell:

```bash
docker run --rm --entrypoint /usr/bin/s3-proxy s3-hybrid-cache:<new-version> --version
docker logs <container> | grep Starting  # the proxy logs its version on startup
```

The startup log line is the more useful of the two, because it reports the version of
the binary in the *running* container rather than of an image tag.

Config compatibility rules are unchanged: an existing mounted `config.yaml` keeps
parsing across versions (see [Upgrading](GETTING_STARTED.md#upgrading) and
[UPGRADING.md](UPGRADING.md) for release-specific defaults). Check
[UPGRADING.md](UPGRADING.md) for any manual step or default change between your
running version and the target before recreating.

## Kubernetes Notes

The container patterns above translate directly, with a few Kubernetes-specific
points:

- **`livenessProbe`/`readinessProbe`** should use `httpGet` against `:8080/health`
  rather than an `exec` probe — the distroless runtime has no shell for `exec`
  probes to run in. This is the same reasoning as
  [Healthcheck Tradeoff](#healthcheck-tradeoff).
- **`securityContext.runAsNonRoot: true`** works out of the box with the
  `nonroot` distroless tag. Pair with `proxy_only` mode (see
  [Privilege Model](#privilege-model-ports-80443-vs-proxy_only)) to avoid needing
  `NET_BIND_SERVICE` at all.
- **Cache volume**: use a `PersistentVolumeClaim` backed by a filesystem that
  supports the `lookupcache=pos`-equivalent consistency guarantee for multi-replica
  deployments sharing one cache — see
  [Multi-Instance / Shared Cache](#multi-instance--shared-cache). A `ReadWriteMany`
  PVC on EFS or an NFS-backed storage class both work; verify the mount options the
  storage class actually applies.
- **ConfigMap for `config.yaml`**, mounted as a volume — equivalent to the bind
  mount shown in [docker compose Example](#docker-compose-example). Changing the
  ConfigMap and rolling the pods is the container-native version of "edit config,
  restart" from [Upgrading](GETTING_STARTED.md#upgrading).

## Verified

The [Dockerfile](#dockerfile) above was built and run end-to-end against this
repository's source at 2.4.2, using Finch v1.17.2 on macOS/arm64.

What was confirmed:

- The two-stage build completes, with the dependency layer and source layer caching
  as described.
- The resulting image reports `s3-proxy 2.4.2`, matching `Cargo.toml`.
- It runs as UID 65532, from the `nonroot` distroless base.
- A container in `proxy_only` mode with named-volume cache and log storage starts
  cleanly, initialises the cache, and answers `GET /health` with `200` and
  `"status": "Healthy"`.
- Image size ~58 MB (arm64).

The [bind-mount ownership caveat](#cache-persistence) was found during this run, not
reasoned about in advance: the first attempt used host bind mounts and failed with
`ConfigError("Failed to create cache subdirectory 'metadata': Permission denied")`
despite a host-side `chown` to 65532. Named volumes fixed it.

Not yet verified, so treat with more caution than the above: the
`debian:bookworm-slim` runtime variant with `HEALTHCHECK`, the `--cap-add` route for
binding 80/443 as a non-root user, the NFS volume-driver options under
[Multi-Instance / Shared Cache](#multi-instance--shared-cache), and the Kubernetes
manifests, none of which were exercised. The bind-address behaviour in
[Bind Addresses](#bind-addresses-loopback-defaults-are-a-container-trap) is read from
the defaults in `src/config.rs`; only the health endpoint (`0.0.0.0`) was confirmed
reachable in a live container.
