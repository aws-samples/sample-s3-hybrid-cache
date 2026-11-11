# Upgrading

The upgrade flow is: download the new release binary (or rebuild, or copy the binary from a build host), replace, restart.
Configuration is backward-compatible at the field level: new options always have
defaults, so an existing `config.yaml` keeps parsing and running across versions. In
the normal case no config edits are required.

A release can keep your config parsing while changing a **default**, so a deployment that pins nothing can
see different memory use, connection counts, network exposure, or on-disk layout after a
binary swap. The tables below list the releases that do one of those things, so you can
read only the ones between your current version and your target.

For 3.0.0 to 3.0.5, each release's section below, from [Upgrading to 3.0.5](#upgrading-to-305) to [Upgrading to 3.0.0](#upgrading-to-300), and the guides it links carry the detail, and
the [changelog](../CHANGELOG.md) summarizes each release. For every release up to 2.8.4,
the detail is in the [pre-3.0.0 changelog](../changelog-pre-v3.md).

3.0.0 upgrades in place from any 2.8 release, and 3.0.1 to 3.0.5 from 3.0.0 or any 2.8 release; a fleet on an older release first upgrades to a 2.8 release. Where objects are overwritten in place, read [During a rolling upgrade](#during-a-rolling-upgrade) and [Upgrading a fleet to 3.0.1](#upgrading-a-fleet-to-301) before upgrading a fleet one proxy at a time.
After 3.0.0, restart a fleet one proxy at a time. Clients that fail over, such as CRT-based ones, retry on another proxy while one restarts, and the cache volume survives restarts.
Releases from before 1.0.0 are not covered. Upgrading from a pre-1.0.0 build is not a
supported path; start from a fresh cache directory and a config derived from the
current [`config/config.example.yaml`](../config/config.example.yaml).

## Releases requiring a manual step

Do these before or while upgrading. Skipping one can mean a failed start or a cache that
cannot be read.

| Version | Action |
|---|---|
| 3.0.5 | Deployments whose DNS64 resolver uses the local-use NAT64 range `64:ff9b:1::/48`, clients that reach a proxy's own addresses through the HTTPS passthrough or `CONNECT`, and fleets that set `connection_pool.ip_failure_threshold` against the old count have a step to take; see [Upgrading to 3.0.5](#upgrading-to-305). |
| 3.0.2 | Operators with an allowlist and S3 on private addresses, S3 Select clients, or alarms on removed `/metrics` fields, and code built on the `s3_proxy` library, have a step to take; see [Upgrading to 3.0.2](#upgrading-to-302). |
| 3.0.1 | Fleets where objects are overwritten in place, and proxies whose clocks may differ from the cache volume's file server by 10 minutes or more, have a step to take; see [Upgrading to 3.0.1](#upgrading-to-301). |
| 3.0.0 | Operators with their own hardened systemd unit, Docker, health or metrics checks from another host, `cache_rules.json` patterns with special characters, several proxies on one cache volume, or an S3-compatible origin that omits the `Range` total have a step to take; see [Upgrading to 3.0.0](#upgrading-to-300). |
| 2.6.0 | To keep log files older than the configured retention, copy them off or raise `logging.access_log_retention_days` / `logging.app_log_retention_days` before upgrading; the first cleanup pass now enforces retention across the whole of `access_log_dir` and `app_log_dir`, including on a deployment running with `access_log_enabled: false`, where neither sweep previously ran. |
| 2.5.0 | If you set `server.max_concurrent_requests` explicitly, review the value; a permit is now held for the whole transfer, not just request setup, so the same number admits far fewer concurrent large transfers than before. Remove `server.max_buffered_request_body_bytes` if you set it; it is deprecated, has no effect, and logs a startup warning; if you had lowered it to reject large uploads, S3's own limits apply instead. On FSx for OpenZFS, add `nconnect=16` to the mount and remount, or disk-cache reads stay capped near 625 MB/s; EFS needs no change. |
| 2.2.0 | Copy the hardened `config/s3-proxy.service` unit if you installed the old one; it adds systemd sandbox directives and a `ReadWritePaths` allowlist. |
| 2.0.0 | Hand-translate any per-bucket `_settings.json` into `cache_dir/cache_rules.json`. No auto-migration; see [below](#upgrading-to-200-per-bucket-_settingsjson-removed). |
| 1.16.0 | If any TTL in your config exceeds 10 years, reduce it; startup now rejects it by design, naming the field. |
| 1.5.0 | `ttl_overrides` removed from YAML. Migrate those settings (to `_settings.json` then, or straight to `cache_rules.json` if you are also passing 2.0.0). |
| 1.3.0 | **Flush the cache** with `rm -rf cache_dir/*`. Cached data moved to the LZ4 frame format; old block-format `.bin` files are unreadable by the new decoder. |
| 1.1.0 | **Clear the cache directory**, and remove `shared_storage.enabled`, `size_tracking_flush_interval`, and `size_tracking_buffer_size` if present. |
| 1.0.13 | Multi-instance NFS deployments must mount the shared volume with `lookupcache=pos`. |

## Releases changing a default or observable behaviour

No action is needed to keep running, but the effect lands whether or not you pin
anything. Cache-key changes orphan previously-cached entries; they are re-fetched, not
corrupted.

| Version | Change on a binary-only upgrade |
|---|---|
| 3.0.4 | Deployments that cache multipart uploads keep an unread completed one for `put_ttl` (default `1h`) instead of a day, and some write cache metrics change meaning; see [Upgrading to 3.0.4](#upgrading-to-304). |
| 3.0.3 | Write cache reclamation removes nothing by size while the recomputed staged figure cannot be read, and `write_cache.inflight_bytes` counts only uploads in flight; see [Upgrading to 3.0.3](#upgrading-to-303). |
| 3.0.1 | Ranges a 2.x release cached, and part layouts recorded before 3.0.1, are fetched from S3 once, on first read; see [Upgrading to 3.0.1](#upgrading-to-301). |
| public 2.8.2 | The first GET of an object cached by a PUT or multipart upload now makes one extra round trip to S3 (a conditional request) before serving, where previously it was served straight from cache with no S3 contact. Subsequent reads of the same object are unaffected. |
| 2.8.0 | Expired GETs, including byte-range reads, revalidate with a conditional S3 request and serve cached bytes on `304`, so body transfers fall and `ttl_revalidations_total` rises; a lowered `get_ttl` applies to ranged reads of cached objects; orphan range files are no longer swept at startup; four log lines change fields. |
| 2.7.0 | `write_cache.resident_bytes`, `cache.write_cache_size`, `cache.total_cache_size` and `cache.read_cache_size` on `/metrics` change meaning (`total = read + write_cache`), so rebaseline thresholds and panels on all four; `cache.write_cache_percent` becomes a background reclamation target rather than an admission limit; write-through caching is declined on a cache near `cache.max_cache_size`; `/health` can report the cache `Degraded` above 95% usage. No manual step; an inflated write-cache figure recovers on its own ([Eviction](EVICTION.md#recovery-from-an-inflated-write-cache-figure)). |
| 2.6.1 | With `metrics.otlp.enabled: true`, OTLP metrics now reach the collector where previously none did, and each instance reports its own `service.instance.id`; a Prometheus OTLP receiver that folded every instance into one `instance` label now produces one series per instance. |
| 2.5.0 | `cache.ram_cache_hit_rate_percent` becomes 0–100 instead of 0.0–1.0; `server.max_concurrent_requests` default 200 → 1000, now covering the whole transfer (see the manual-step table); health and metrics listeners return 404 for other paths and 405 for non-GET methods; an excluded upstream IP stays out until a recovery probe succeeds, so `health_probe_initial_cooldown` and `health_probe_max_cooldown` take effect; HEAD stays cacheable past `head_ttl`; upload memory no longer scales with body size. |
| 2.4.0 | `max_ram_cache_size` default 256 → 512 MiB: **+256 MiB RAM per instance**. Pin `268435456` to stay at 256 MiB (runs 4 effective shards). `config.example.yaml` now ships the built-in `put_ttl` (1 hour) and `compression.threshold` (1024) instead of `1d` and `4096`; an existing config keeps its own explicit values. A client-supplied `If-Range` whose validator does not match the cached ETag is now forwarded to S3, so it costs one round trip and returns `200`-full instead of a `206` sliced from cache. |
| 2.3.0 | Compression extension denylist now enforced on writes; `compression.threshold` now applies (was hardcoded 1024); `compression.content_aware` removed (aliased, ignored, warns). |
| 2.2.3 | All cache-hit responses carry `X-Cache: HIT`, visible to clients and CDNs. |
| 2.2.0 | Health/metrics servers bind `0.0.0.0` by default (set `127.0.0.1` to restrict); RAM cache sharded 8 ways, entries above per-shard capacity dropped; `evaluate_conditions_from_cache` defaults `true`; `tcp_recv_buffer_size` unset so the kernel auto-tunes; CompleteMultipartUpload bodies over 10 MiB rejected with 413. |
| 1.16.4 | Build toolchain pinned to Rust 1.96 (`rust-toolchain.toml` and CI image). |
| 1.16.0 | Dashboard binds `127.0.0.1` (was `0.0.0.0`); set it explicitly for remote access; `umask(0o077)` makes proxy-created files owner-only; `ram_cache_flush_interval` 60s → 10s raises journal write rate; signed-write bodies capped at 5 GiB with 413. |
| 1.14.1 | Virtual-hosted and accelerate cache keys change shape; previously-written flat keys are orphaned. |
| 1.14.0 | MSRV raised to Rust 1.89; SSE-C requests bypass the cache entirely (GET/HEAD/PUT). |
| 1.13.2 | If you copied the example config, `ip_distribution_enabled` was wrongly `false` there; the real default is `true`. |
| 1.13.1 | `compression.enabled: false` now disables compression, changing disk footprint and CPU. |
| 1.11.3 | Outbound TLS is per-destination: 1.3 for S3, 1.2 only for `endpoint_overrides` hosts. (1.11.1 had locked *all* outbound TLS to 1.2 whenever `endpoint_overrides` was non-empty.) |
| 1.9.9 | Seven OTLP metrics removed; dashboards and alarms referencing them break. |
| 1.9.0 | Connection `idle_timeout` 30s → 55s; `SO_RCVBUF` pinned to 256 KB per connection. |
| 1.7.8 | `ip_distribution_enabled` defaults `true`: per-IP connection pools multiply outbound connection count. |
| 1.7.6 | `allow_streaming` defaults `true`; the 1 MiB streaming threshold is gone, so all responses stream. |
| 1.7.1 | `max_idle_per_host` 10 → 100 (range 1–500), ~10x idle connections; streaming chunk 512 KiB → 1 MiB. |
| 1.7.0 | Logs are auto-deleted after 30 days by a background cleanup task. |
| 1.6.8 | Access log records gain a 25th field (`source_region`); update log parsers. |
| 1.5.2 | On-disk metadata schema change: expiry moves from per-range to object level. |
| 1.4.5 | Metric names gain a `_percent` suffix in `/metrics` and OTLP (dashboards break); RAM cache compresses unconditionally, ignoring `compression.enabled`. |
| 1.4.2 | All OTLP gauge names change, and `cache.size` loses its `cache_type` dimension. |
| 1.3.0 | `--compression-enabled` CLI flag removed; use `COMPRESSION_ENABLED` or `compression.enabled`. |
| 1.2.7 | A `Referer` header is injected into every forwarded S3 request by default (`server.add_referer_header`). Renamed in 2.0.0; update any access-log Referer queries. |
| 1.2.5 | RAM data cache is silently disabled whenever `get_ttl` is `0s`. |
| 1.2.2 | Request coalescing on by default: concurrent misses wait rather than each fetching. |
| 1.1.0 | Consolidation interval 30s → 5s, raising shared-storage I/O. |

Releases not listed above require no action and change no defaults: internal fixes,
performance work, additive opt-in features that stay disabled until configured, and
dependency bumps.

## Upgrading to 3.0.5

3.0.5 upgrades in place from 3.0.4 and needs no configuration change: its two new fields, `connection_pool.dns_ip_strategy` and `server.tls.tunnel_refused_addresses`, have defaults. When upgrading from 3.0.3 or earlier, take those releases' steps as well. It lets the proxy connect to an origin's IPv6 addresses on every path, and tightens which destinations the HTTPS passthrough (port 443) and `CONNECT` tunnels accept.

### Before upgrading to 3.0.5

- If your DNS64 resolver synthesizes addresses in the local-use NAT64 range `64:ff9b:1::/48`, list the addresses the proxy needs in `connection_pool.endpoint_overrides` before upgrading. 3.0.5 refuses that range on the HTTPS passthrough and `CONNECT` tunnels, and on the HTTP forwarding paths once `server.tls.connect_allowlist` is set ([Destination policy](ARCHITECTURE.md#destination-policy-ssrf-protection)).
- The well-known prefix `64:ff9b::/96`, which DNS64 in an Amazon VPC uses, needs no change. An address in it is judged by the IPv4 address it embeds.

### What you will see after upgrading to 3.0.5

- A GET or HEAD that IP distribution sends to an origin's IPv6 address connects to it. On a host with no IPv6 route, set `connection_pool.dns_ip_strategy: ipv4_only`, because the default tries IPv6 addresses first ([Address families](CONFIGURATION.md#address-families)).
- The HTTPS passthrough and `CONNECT` tunnels refuse a destination with an address on the proxy host's interfaces, a loopback or unspecified address, or one in `server.tls.tunnel_refused_addresses`, even when `endpoint_overrides` lists it. The passthrough closes the connection, and `CONNECT` answers `403` with the body `Forbidden: the destination leads back to a proxy`.
- To refuse the addresses that reach a proxy without being on its interfaces, such as each proxy's EC2 public IPv4 or Elastic IP address, list them in `server.tls.tunnel_refused_addresses` ([Credentials and network access](AWS_DEPLOYMENT.md#credentials-and-network-access)).
- The HTTPS passthrough and `CONNECT` tunnels try each allowed address in turn, each attempt for up to 10 seconds and the whole connect for up to 30. A `CONNECT` connect attempt now gives up after 10 seconds and moves to the next address.
- A GET or HEAD whose upstream connect fails is retried up to three times before the proxy answers `502`, which adds up to 700 ms of waiting. A signed range GET can make up to 12 attempts and wait up to 2.4 s. With IP distribution on, each retry goes to the next address.
- A key whose cache rule turns hedging on follows different connect-retry rules ([Hedging](HEDGING.md#connection-diversity)).
- A failed connect counts once against its address, so an address that refuses connections leaves rotation after `ip_failure_threshold` failed connects (3 by default) instead of 2. Review the threshold if you set it against the old count.
- Uploads and other writes count failed connects against the address too, so an address that keeps failing leaves rotation for reads and writes alike.
- A part or range request that waited on another request's fetch through all its rejoins fetches from S3 itself instead of getting `504`.
- A dashboard log request reads at most the eight newest application log files.

### During a rolling upgrade to 3.0.5

3.0.5 uses the cache volume as 3.0.4 does, so 3.0.4 and 3.0.5 proxies can share it while the fleet is upgraded one proxy at a time.

- A 3.0.4 proxy ignores `dns_ip_strategy` and `tunnel_refused_addresses` in a shared `config.yaml`, so it keeps trying IPv6 addresses first and does not refuse the listed addresses.
- Until it is upgraded, a 3.0.4 proxy still fails to connect to an origin's IPv6 addresses and still relays a passthrough or `CONNECT` connection back to itself. It also still answers a part or range waiter `504`.

## Upgrading to 3.0.4

3.0.4 upgrades in place from 3.0.3 and needs no configuration change and no manual step. When upgrading from 3.0.2 or earlier, take those releases' steps as well. It stops write cache reclamation from removing more unread uploads than it needs to, recovers on its own from a torn `.pending` line or damaged shard checkpoint, and keeps completed multipart uploads for `put_ttl`.

### What you will see after upgrading to 3.0.4

- With the default `put_ttl` of `1h`, an unread completed multipart upload leaves the write cache after an hour instead of a day, as a single-part upload does. Entries already cached keep their expiry, and a cache rule that sets `put_ttl` applies to multipart uploads too.
- `write_cache.staged_removal_debit_bytes` shows the staged bytes subtracted for removals that their shards' figures do not yet include, and `write_cache.staged_fold_bytes` is net of them ([Coordination across proxies](EVICTION.md#coordination-across-proxies)).
- A blank or non-JSON line in a `.pending` file is moved to `{instance}.pending.torn` and counted by `write_cache.pending_lines_quarantined_total`, and the staged figure recovers without an operator. A shard checkpoint that does not parse is kept in `{XX}.json.torn` and replaced by recomputing that shard ([When the recomputed figure cannot be read](EVICTION.md#when-the-recomputed-figure-cannot-be-read)).
- `write_cache.staged_authority_snapshot_recomputes_total` counts recomputations of the staged figure, not reads. A reader that waits for a recomputation already running counts in neither it nor `staged_authority_snapshot_hits_total`, so the two no longer give a per-read hit rate.
- `write_cache.staged_authority_snapshot_age_secs` is the age of the figure's inputs, and can exceed 5 s by the recomputation's read time.
- When a metrics collection has not returned after twice `metrics.collection_interval`, or 60 s if that is longer, the proxy logs an error and records OTLP once from the figures `/metrics` reads. The line begins `Background metrics collection has not returned`, so an alert on error lines fires on it.
- At `write_cache.staged_authority_epoch` `0`, `write_cache.staging_figure_unknown_seconds` and its error line now also report a staged figure the proxy cannot recompute. Reclamation then still sizes itself from the event-maintained counter.
- At `write_cache.staged_authority_epoch` `1`, a full validation scan no longer reports graduated bytes as write cache drift. `cache_size.accounting_drift` marks the write cache figure not evaluable with reason `staged_authority_epoch_1`, and the scan's log line names the two figures it compared instead of warning about drift.
- At `write_cache.staged_authority_epoch` `1`, the warning that `write_cache.resident_bytes` is over `staging_bound_bytes` is not logged before the proxy has recomputed its first staged figure.

### During a rolling upgrade to 3.0.4

These differences last until the last 3.0.3 proxy is upgraded, so upgrade the fleet in one pass.

- 3.0.3 proxies size their passes as 3.0.3 does, and a 3.0.4 proxy cannot subtract the bytes a 3.0.3 proxy's passes remove. Either can then remove more unread uploads than it needs to.
- 3.0.4 proxies keep writing the `eviction_reclaims/` documents 3.0.3 proxies read, and subtract the ones 3.0.3 proxies write. A 3.0.4 proxy can subtract a 3.0.3 document after it has already recomputed the shard, so the count limit can act late.
- A torn line in a running 3.0.3 proxy's own `.pending` file still stops the staged figure on every proxy until that proxy is upgraded. The upgraded proxy then clears it after 15 minutes, or another proxy after 24 hours where several proxies share a hostname.
- Multipart uploads a 3.0.3 proxy completes still stay in the write cache for a day.

## Upgrading to 3.0.3

3.0.3 upgrades in place from 3.0.2 and needs no configuration change and no manual step. When upgrading from 3.0.1 or earlier, take the 3.0.2 (and 3.0.1) steps as well. It changes how write cache reclamation sizes itself, what the write cache figures on `/metrics` show while the staged figure cannot be recomputed, and what `write_cache.inflight_bytes` reports.

### What you will see after upgrading to 3.0.3

- While a pass cannot recompute the staged figure, write cache reclamation removes nothing by size, and the staged range count limit, which reads the same files, can stop removing ranges too. Staged bytes can then grow until the proxy stops caching new uploads at `max_cache_size`, displacing read caching ([When the recomputed figure cannot be read](EVICTION.md#when-the-recomputed-figure-cannot-be-read)). Reclamation by size resumes on the first pass that can recompute the figure.
- The new `write_cache.staging_figure_unknown_seconds` shows how long the proxy has been unable to recompute the figure, and the new `write_cache.staging_figure_unknown_passes_total` counts the passes that removed nothing by size. When a recomputation fails after the figure has been unknown for 60 s, the proxy logs an error, at most every 10 minutes.
- The gauge also reads above `0` while a recomputation has run for 60 s or more, which on a cache volume with many buckets can happen in normal operation ([What a pass reads](EVICTION.md#what-a-pass-reads)). An alert on `> 0` then fires on a healthy proxy, so set its threshold above the values the gauge shows on your volume.
- At `write_cache.staged_authority_epoch` `1`, while the staged figure cannot be recomputed, `cache.write_cache_size`, `cache.read_cache_size` and `write_cache.resident_bytes` come from the last figure the proxy recomputed, whatever its age. Until the proxy has recomputed one, `cache.write_cache_size` and `write_cache.resident_bytes` show the event-maintained counter, whose error at that epoch can run in either direction.
- During a rolling upgrade, a 3.0.2 proxy that cannot read the recomputed figure sizes a pass from its event-maintained counter at once. That counter can read high at epoch 1, so a 3.0.2 proxy can remove unread uploads a 3.0.3 proxy would keep. The difference ends when the last proxy is upgraded.
- `write_cache.inflight_bytes` counts only uploads in flight, as its [reference entry](METRICS_REFERENCE.md#write_cache) describes. A dashboard that saw it read `0` after a reclamation pass now sees a figure that falls to `0` only when no upload is in flight.
## Upgrading to 3.0.2
3.0.2 upgrades in place from 3.0.1 and needs no configuration change: its one new field, `connection_pool.upstream_write_idle_timeout`, has a default. When upgrading from 3.0.0 or a 2.8 release, take the [3.0.1 steps](#upgrading-to-301) as well. Four kinds of deployment have a step to take before upgrading.

### Before upgrading to 3.0.2

- **An allowlist with S3 on private addresses**: if you set `server.tls.connect_allowlist` and `connection_pool.dns_servers` resolves S3's hostnames to the private addresses of an interface endpoint, list those addresses in `connection_pool.endpoint_overrides` before upgrading ([S3 PrivateLink](CONFIGURATION.md#s3-privatelink-interface-vpc-endpoints)). With an allowlist set, the HTTP forwarding paths connect only to addresses the destination policy accepts, and the policy refuses a private address that `endpoint_overrides` does not list. A host listed in `endpoint_overrides` negotiates TLS 1.2.
- **S3 Select**: send SelectObjectContent requests to S3 directly. The proxy answers them `501 NotImplemented`. Select requests through a `CONNECT` tunnel or the HTTPS passthrough (port 443) are not inspected, so they reach S3 unchanged.
- **Dashboards and alarms on removed `/metrics` fields**: remove `cache.cache_etag_mismatches_total`, `cache.cache_range_invalidations_total`, `cache.cache_etag_validations_total`, `cache.cache_orphaned_ranges_cleaned_total` and `cache.cache_cleanup_failures_total`, and the OTLP series `cache.etag_mismatches_total` and `cache.range_invalidations_total`. Each read `0` on every proxy, and 3.0.2 removes them ([Metrics reference](METRICS_REFERENCE.md#cache)).
- **Code built on the `s3_proxy` library crate**: the crate exports only what the `s3-proxy` binary uses. Types and functions the binary never called are removed, among them the `cache_validator` module, `CacheEntry`, `WriteCacheEntry` and the `CacheManager` methods `get_cached_response`, `get_write_cache_entry`, `check_cache_consistency` and `is_write_cache_entry_expired`. Most other items are now private to the crate. Code that uses a removed item needs changing before it builds against 3.0.2.

### What you will see after upgrading to 3.0.2

- With an allowlist set, a request whose name `connection_pool.dns_servers` resolves only to addresses the policy refuses gets no connection and is answered `502 Bad Gateway`, and the proxy logs a warning naming the host. A name that public DNS already resolves only to refused addresses gets `403 AccessDenied` instead ([Destination policy](ARCHITECTURE.md#destination-policy-ssrf-protection)).
- A SelectObjectContent request gets `501 NotImplemented`, and the proxy logs at most one warning a minute about such refusals.
- Cached data is fresh for `get_ttl` from when it was fetched. 3.0.1, and possibly earlier releases, counted from the consolidation that recorded it, up to one `consolidation_interval` later, or longer after a consolidation backlog or a restart. Entries now expire that much sooner, so `ttl_revalidations_total` can rise. An entry cached by an earlier release keeps its old expiry until it expires or is revalidated.
- With `get_ttl: "0s"`, a GET whose lone `If-Match` names the cached ETag is revalidated with S3 and, when S3 answers `304`, served from cache with `X-Cache: HIT`, so `ttl_revalidations_total` rises ([Zero TTL revalidation](CACHING.md#zero-ttl-revalidation)).

Rolling back to 3.0.1 needs no step: 3.0.1 ignores `connection_pool.upstream_write_idle_timeout`.

## Upgrading to 3.0.1

3.0.1 upgrades in place from 3.0.0 or any 2.8 release, and needs no configuration change. It refuses cached ranges that record no file identity, and every range a 2.x release cached is one, so a cache volume that a 2.x release populated re-warms from S3 after the upgrade. When upgrading from 2.8, read [Upgrading to 3.0.0](#upgrading-to-300) as well. [Cache integrity](../SECURITY.md#cache-integrity) describes what these changes guard against.

### Before upgrading to 3.0.1

- **Clocks**: keep each proxy's clock within 10 minutes (600 seconds) of the cache volume's file server, for example with NTP. A proxy whose clock runs 10 minutes or more ahead can replace a range file that another proxy has just written for an object with no cached metadata, and nothing reports it. Where objects are overwritten in place outside the proxy, that can let a read return one version's bytes under another version's ETag ([What the guards do not cover](../SECURITY.md#what-the-guards-do-not-cover)).
- **Ranges a 2.x release cached**: by default each is fetched from S3 again on its first read. Where every object is written once and never overwritten in place outside the proxy, you can keep them by setting `cache.trust_unverified_ranges: true` ([Ranges with no file identity](CONFIGURATION.md#ranges-with-no-file-identity)).

### Upgrading a fleet to 3.0.1

- From a 2.x release, upgrade every proxy on a cache volume in one pass, as [During a rolling upgrade](#during-a-rolling-upgrade) describes. A 2.x proxy that is still running keeps caching ranges with no file identity, which 3.0.1 proxies refuse and fetch from S3 again.
- A 3.0.1 proxy discards the journal entries that 2.x proxies write for their fills, so those ranges are fetched from S3 again. It deletes the range file such an entry wrote if the file is already 10 minutes old when the entry is discarded and no cached metadata or other pending entry names it. A running 2.x proxy's entries are discarded at the next consolidation (every 5 seconds by default), so their files are left to [orphan recovery](SHARED_STORAGE.md#orphan-recovery), which starts about an hour after the last 2.x proxy stops, or after the first 3.0.1 proxy starts if that is later.
- From 3.0.0, where objects are overwritten in place, upgrade every proxy in one pass too. Until every proxy runs 3.0.1, a 3.0.0 proxy can replace a range file that a 3.0.1 proxy has just written for an object with no cached metadata, and keeps its other 3.0.0 behavior ([Cache integrity](../SECURITY.md#cache-integrity)). Where each object is written once, one proxy at a time is safe.

### What you will see after upgrading to 3.0.1

- On a cache volume that a 2.x release wrote to, the first read of each range 2.x cached goes to S3, whatever the range's size, and the range is cached again. These reads count as misses, and `atomic_metadata.unverified_ranges_refused` rises while they happen, then stays flat ([Metrics reference](METRICS_REFERENCE.md#atomic_metadata)).
- Journal entries that a 2.x release left on the cache volume are discarded instead of consolidated, and each range file they wrote that is already 10 minutes old is deleted, counted in `atomic_metadata.retired_fill_files_unlinked`.
- A part layout recorded before 3.0.1 is discarded on its first read, and each of its parts is fetched from S3 once and recorded again. No counter records this; each discard logs one `info` line, `Cleared a part layout recorded before 3.0.1`.
- A fill whose file identity cannot be read is not cached, and logs one warning naming the key and the file.
- A streamed cache hit whose client reads slowly holds about 3.9 MiB ([Memory impact](CONFIGURATION.md#memory-impact)).

## Rolling back from 3.0.1

Rolling back to 3.0.0 needs no step. 3.0.1 adds one field to an object's cached metadata, which 3.0.0 ignores, so 3.0.0 reads the cache volume as 3.0.1 left it. The 3.0.0 behavior listed in [Cache integrity](../SECURITY.md#cache-integrity) returns. A part layout that 3.0.0 rewrites loses the mark 3.0.1 adds, so after upgrading again each of its parts is fetched from S3 once. To roll back to a 2.8 release, follow [Rolling back from 3.0.0](#rolling-back-from-300).

## Upgrading to 3.0.0

3.0.0 upgrades in place from any 2.8 release, and most deployments need no configuration change. Where objects are overwritten in place, upgrade every proxy on a cache volume in one pass. Until every proxy runs 3.0.0, a proxy still on 2.8 can keep serving the previous version of an overwritten object, whichever proxy took the write. Where each object is written once, you can upgrade one proxy at a time. A few deployment types have a step to take first, some defaults change, and some metric names, log lines and configuration keys change. At least a day after the last 2.8 proxy stops writing, provided the volume still serves GETs from the cache or takes uploads, misses or deletes that change the cache's size, the volume switches to 3.0.0's write-cache accounting, and from then it cannot go back to an earlier release in place; see [Rolling back from 3.0.0](#rolling-back-from-300).

### Before you upgrade

- **Your own hardened systemd unit** (`ProtectSystem=strict`): install the updated `config/s3-proxy.service`, or add `StateDirectory=s3-proxy` (or `/var/lib/s3-proxy` in `ReadWritePaths`). The proxy keeps its record of pending invalidations there; without it the record is held in memory only and `cache.write_stamp_log_memory_only` reads `1`.
- **Docker**: mount a volume on the host's own disk, owned by UID 65532, and set `logging.local_state_dir` to its path, as [Docker deployment](DOCKER.md#cache-persistence) shows with `/var/lib/s3-proxy/state`.
- **Health or metrics checked from another host** (a load balancer, a Kubernetes probe, a published container port): if that section omits `bind_address`, set `bind_address: "0.0.0.0"`, because both listeners now bind `127.0.0.1` by default. A `health.bind_address` or `metrics.bind_address` must be an IP address (`127.0.0.1`, `0.0.0.0`, `::1`); the proxy names the field and does not start if it is a hostname such as `localhost`.
- **`cache_rules.json` patterns with special characters**: write any literal character outside `A-Z a-z 0-9 - . _ ~ /` in percent-encoded form, so `my-bucket/report 2026/**` becomes `my-bucket/report%202026/**`. Rules match the object key in one spelling ([Cache internals](CACHE_INTERNALS.md#object-key-spelling)).
- **Several proxies on one cache volume**: give each proxy its own hostname. Proxies sharing one, such as replicas given a single fixed container hostname, log an error and count the collision in `cache.hostname_collisions_total`. Two that also share a process ID forward uploads to S3 uncached ([Shared storage](SHARED_STORAGE.md#each-proxy-needs-its-own-hostname)).
- **An S3-compatible origin that answers `Range` with an unknown total** (`Content-Range: bytes N-M/*`): clear the cache volume (`rm -rf cache_dir/*`) with every proxy stopped. This is needed because an earlier release could store such a response as the whole object. AWS S3 always sends the total, so S3 deployments skip this step.

### During a rolling upgrade

- Where objects are overwritten in place, upgrade every proxy on a cache volume in one pass: stop every proxy on the volume, replace the binary on each, then start them. This way no 2.8 proxy serves requests once a 3.0.0 proxy is running. Requests fail while every proxy is stopped. Until every proxy runs 3.0.0, a proxy still on 2.8 can keep serving the previous version of an overwritten or deleted object, whichever proxy took the write. Until every proxy runs 3.0.0, a proxy on 3.0.0 can return part of an object that was overwritten in place with one version's bytes under another version's ETag and headers. It can continue afterwards for ranges cached from updates the 2.8 proxies left on the cache volume, so a one-pass upgrade shortens this window without closing it. 3.0.1 closes it ([Upgrading to 3.0.1](#upgrading-to-301)). Where each object is written once, one proxy at a time is safe. Until every proxy runs 3.0.0, the eviction-lock holder evicts alone, orphan cleanup waits, and an older proxy can serve a cached range that 3.0.0 would fetch from S3. Fleet-wide eviction and orphan cleanup start about an hour after the last older proxy stops. The write-order record (the `.wtok` file) and a bucket's binding to its origin take effect only once every proxy runs 3.0.0.
- A multipart upload in progress at the upgrade completes on S3 and is not cached.
- At least a day after the last 2.8 proxy stops writing, provided the volume still serves GETs from the cache or takes uploads, misses or deletes that change the cache's size, the cache volume switches to a write-cache size figure recomputed from the cache's metadata: `write_cache.staged_authority_epoch` reads `1`, and `cache.write_cache_size` steps once, up or down. Until then the fleet keeps 2.8's write-cache accounting, which the daily validation scan corrects, and a proxy can be rolled back in place. After the switch, rolling back needs a fresh cache volume ([Rolling back from 3.0.0](#rolling-back-from-300)). A 2.8 proxy that is still running does not reliably hold the switch back, so upgrade every proxy on the volume within a day; a 2.8 proxy still running after the switch miscounts its own write cache.

### Changed defaults

- `cache.get_ttl` defaults to 1 day instead of about 10 years. A config that omits it sends S3 one conditional GET per cached object per day when the object is read, with no body transfer when the object is unchanged, so `ttl_revalidations_total` rises. Set `cache.get_ttl: "315360000s"` to keep the old behavior ([Cache freshness](CACHE_FRESHNESS.md#get-ttl-get_ttl)).
- `tinylfu` is the only eviction algorithm. A config that omits `cache.eviction_algorithm`, or sets `"lru"`, now runs `tinylfu`; `"lru"` still loads with one startup warning ([Eviction](EVICTION.md#eviction-algorithms)).
- `cache.shared_storage.lock_timeout` is renamed `lock_acquire_wait_budget`, defaults to `5s` and is honored, so a request waiting on a stalled peer fails with a retryable error after that budget. A value set under the old name becomes the budget ([Configuration reference](CONFIGURATION.md#metadata-lock-timing)).
- `connection_pool.max_lifetime` (default `300s`) is enforced, so pooled upstream connections are recycled about every five minutes; and a suffix `endpoint_overrides` entry round-robins and fails over across its IPs ([Connection pooling](CONNECTION_POOLING.md#4a-maximum-lifetime)).
- `download_bandwidth.fleet.instance_staleness` defaults to `90s` instead of `30s` ([Bandwidth QoS](BANDWIDTH_QOS.md#fleet-sharing-cap--n)).
- With `connection_pool.ip_distribution_enabled` on (the default) and virtual-hosted addressing, `connection_pool.max_idle_per_ip` (default 10) applies to each bucket hostname, so idle upstream connections and file descriptors rise with the number of bucket hostnames in use. Lower `max_idle_per_ip`, or raise the file-descriptor limit, if a fleet serves many bucket hostnames.
- A re-read on the proxy that has just fetched an object is a cache hit at once, using up to 16 MiB more memory per proxy.

- `cache.max_metadata_file_bytes` and `cache.metadata_io_concurrency` now take effect. Public 2.8.4 parsed both and always used 4 MiB and 32, so check any value your config sets. A `max_metadata_file_bytes` below 64 KiB is raised to 64 KiB, and a `metadata_io_concurrency` of 0 to 1, each with a startup warning. `max_metadata_file_bytes` is deprecated: remove it from your config unless you need a different cap. Both apply to range lookups in the disk cache, validation against HEAD, range invalidation, and the RAM-hit recorder's extent read; every other `.meta` read, including the lookup that serves a GET or HEAD on a RAM metadata-cache miss, uses neither.
### Cache contents and background work

- Cache keys hold the object key in the spelling AWS SDKs send, so entries cached under another spelling (`+` for a space, lower-case hex, `%2F`) become misses and age out.
- A range cached or last rewritten by an earlier release records no file identity ([Cache internals](CACHE_INTERNALS.md#range-file-identity)). 3.0.0 served such a range unchecked, apart from the ranges in the next item. From 3.0.1 it is fetched from S3 once, on its first read, and cached again, unless `cache.trust_unverified_ranges` is `true` ([Upgrading to 3.0.1](#upgrading-to-301)).
- A few ranges cached by an earlier release are fetched from S3 once, on their first read, and cached again. These are a range that starts at an object's first byte and ends before its last, and a range stored from an S3 error reply. Those reads count as misses in the access log and on `/metrics`.
- The reported cache size can fall after the upgrade. Orphan cleanup removes range files no metadata references, startup removes stale temporary files and the journals of hosts gone for 24 hours, and the validation scan removes expired write-cache entries ([Shared storage](SHARED_STORAGE.md#orphan-recovery)).
- New range files use 64 KiB LZ4 blocks, so highly compressible content takes more disk space and `cache.total_cache_size` rises for the same data ([Compression](COMPRESSION.md#block-size)).
- Background work on the cache volume changes. An hourly pass lists and stats every range file to keep the size figures accurate (`shard_fold_interval`, `shard_fold_max_duration`). The daily validation scan runs once a day for the whole fleet, and every proxy takes part in eviction ([Shared storage](SHARED_STORAGE.md#scanner-concurrency), [Eviction](EVICTION.md#which-proxy-deletes)). To force a validation scan, delete both `size_tracking/validation.json` and `size_tracking/schedule.json`.
- The proxy creates its own coordination files under `metadata/` and `size_tracking/`, a per-bucket `_origin.json`, and a small `.wtok` file per recently written key; none needs action. The old `*.meta.lock` and `locks/{key}.lock` files are no longer used and can be deleted once every proxy runs 3.0.0.
- A bucket name binds to the first origin that caches it. On a proxy fronting two S3-compatible origins that share a bucket name, requests from the other origin are served uncached, counted in `cache.origin_marker_mismatch_bypasses_total` ([Caching](CACHING.md#one-bucket-name-at-two-origins)).

### Requests

- A successful PUT, CompleteMultipartUpload, DELETE, DeleteObjects or form POST removes the previous cached version before the proxy responds, and cache work adds at most 1 second to a write's response. A DeleteObjects body the proxy cannot read exactly is answered `400 MalformedXML` ([Caching](CACHING.md#write-through-ordering)).
- Write-through caching declines, and the upload still reaches S3, when a proxy already holds 1,024 pending stores (`backlog_full`, which now also counts CreateMultipartUpload and CompleteMultipartUpload). It also declines when the volume holds 5,000 in-progress multipart entries (`multipart_tracking_full`), or past the staged range limit (`write_cache.staged_range_budget`). Each reason is counted in `signed_put.skipped_puts_total`.
- Once the cache reaches `cache.max_cache_size`, a GET that misses is served from S3 and left uncached until eviction brings the total back below the limit, counted in `cache.read_fills_declined_max_cache_size_total`. The free-space floor (`cache.disk_safety_free_space_floor`) also guards GET-miss fills and CreateMultipartUpload.
- These requests are forwarded to S3 uncached: a GET or HEAD with a `response-*` override (`response_header_override` in `cache_bypasses_by_reason`) or another query parameter outside the object-read list, and a HEAD naming an object subresource. So are a PUT that sets tags, an ACL, retention or a legal hold, copies or appends, and a PUT answered without an `ETag`. So are bucket-root requests, requests to AWS hostnames that are not S3 endpoints, and a `partNumber` outside 1 to 10,000.
- A GET with several ranges is forwarded to S3 and answered as S3 answers it, where the proxy answered `501`. A GET without `Range` is always answered `200`, and cache hits no longer return `Content-MD5`.
- A forward-proxy request whose `Host` disagrees with its target, or that carries more than one `Host`, is answered `400 InvalidRequest`. Forwarded requests carry a `Via` header, and a request that already carries it is answered `508 Loop Detected` ([Error handling](ERROR_HANDLING.md#9-request-loops-via-and-508-loop-detected)).
- A key whose `get_ttl` is `"0s"` reaches S3 on every read path, and page-widened reads of it are no longer widened. A client that sends `If-Match` on every request makes one S3 request per object per `get_ttl`.

### Metrics, OTLP and logs

- Metrics are collected in the background every `metrics.collection_interval` (default 60s), and size and object-count figures follow writes within about one `consolidation_interval`. A figure that cannot be read reports `null` or is omitted instead of `0` ([Metrics reference](METRICS_REFERENCE.md#metrics_collection)).
- Renamed and removed names: the metadata-lock fields gain a `metadata_lock_` prefix, `cache.lock_timeout_total` becomes `cache.metadata_lock_retry_exhausted_total`, the always-zero `atomic_metadata` fields and `cache.old_cache_key_encounters` are removed.
- These `/metrics` fields read `0` on every proxy and are removed, with the OTLP series `cache.write_cache_hits`: `cache.inconsistency_fixed_total`, `partial_write_cleanup_total`, `orphaned_files_cleaned_total`, `cache_write_failures_total`, `cache_operation_duration_ms` and `write_cache_hits`, and `connection_pool.failed_connections` and `average_latency_ms`. The OTLP series `connection_pool.failed_connections`, `average_latency_ms`, `success_rate_percent` and `cache.write_failures_total` are no longer exported ([Metrics reference](METRICS_REFERENCE.md)).
- Some figures now report real values, so alarms on them can fire for the first time: `cache_size`, `cache.disk_full_events_total`, `connection_pool.connections_reused`, the dashboard's stale-handle figure and the metadata-lock figures. A deployment with OTLP enabled receives many new series ([OTLP metrics](OTLP_METRICS.md)).
- Alarm on `cache.eviction_backlog_projected_bytes`, the headroom figure eviction acts on; it reads `null` until the proxy has evaluated the eviction trigger once ([Metrics reference](METRICS_REFERENCE.md#eviction-rate-admission-rate-and-whether-eviction-is-keeping-pace)).
- An OTLP endpoint with no path, such as the default `http://localhost:4318`, exports to `/v1/metrics`, so the CloudWatch agent and OpenTelemetry Collector examples receive metrics.
- Log lines that parsers may match change shape. `Consolidation cycle complete` and `Consolidation cycle:` gain `write_cache_delta=`. `Range invalidation completed`, `Bulk range removal completed` and `Write-ledger compaction` gain fields. `Updated size state from validation:` reports `baseline_*` and `census_*` fields in place of `old_write_cache_size` and `new_write_cache_size`. Accept failures log `Accept failed; the listener keeps running`. Each bucket cached before the upgrade logs one warning as its origin is recorded.

### Configuration fields that are now ignored
- `health.check_interval` and `cache.initialization.parallel_scan`, `scan_timeout` and `progress_logging` (environment variables `PARALLEL_SCAN`, `SCAN_TIMEOUT` and `PROGRESS_LOGGING`; flags `--parallel-scan`, `--no-parallel-scan`, `--scan-timeout`, `--progress-logging` and `--no-progress-logging`) still parse and have no effect. Public 2.8.4 did not read them either. 3.0.0 logs one startup warning naming each one set to a value other than its default, and no longer refuses to start over an out-of-range `scan_timeout`. Delete them from your config.
- Public 2.8.4 ran an orphan re-adoption sweep, which orphan cleanup replaces. `orphan_recovery_enabled`, `orphan_recovery_interval`, `orphan_scan_timeout`, `orphan_max_per_cycle` and `shared_storage.recovery_max_concurrent` still load and are ignored, so the sweep no longer runs. The `RECOVERY_MAX_CONCURRENT` variable and the `--recovery-max-concurrent` flag are ignored too. The four orphan fields share one startup warning, logged when any is set to a non-default value; each of the others logs its own warning when set.
## Rolling back from 3.0.0

A proxy can be rolled back to an earlier release in place until the cache volume switches to the write-cache size figure that 3.0.0 recomputes from the cache's metadata. The switch is checked only on a consolidation cycle that has something to record: an upload, overwrite or delete that changes the cache's size, a miss that caches data, a GET served from the cache, or a revalidation with S3 that refreshes an entry. An overwrite that leaves an object's stored size unchanged usually records nothing. The volume switches on the first such cycle once all of these hold:

- Every proxy that has written to the volume runs 3.0.0 and has published that it is ready, or has not changed its files on the volume for one day. After an upgrade, this means at least a day after the last 2.8 proxy stopped writing. The day restarts whenever 3.0.0 clears an entry of that proxy's, which happens on the next compaction or eviction pass after one of its staged objects is read, overwritten, deleted or evicted.
- The proxies, between them, have refreshed every cache shard's write-cache figure and checked every shard's write-cache records within the last day. Each proxy checks at least one shard every 5 minutes, and the proxies share the shards. `ledger_repair_projected_rotation_secs` is the time one proxy would take alone: `ledger_repair_observed_shards` × 300 s below 288 shards, about 86,400 s above. With several proxies, divide it by their number. New shards, created as new objects are cached, add to it.
- The volume has cached at least one upload.

The one-day wait and the shard checks run at the same time, so after an upgrade the switch comes at least a day after the last 2.8 proxy stopped writing. A new volume that no 2.8 proxy wrote to has no one-day wait and switches once the shard checks finish. A volume with no traffic stays at `0`, and so can one whose only traffic is same-size overwrites. `write_cache.staged_authority_epoch` on `/metrics` reads `0` before the switch and `1` after it ([Eviction](EVICTION.md#which-computation-is-in-force)).

While the epoch reads `0`, roll back by installing the earlier release and restarting, one proxy at a time.

Once the epoch reads `1`, a release before 3.0.0 miscounts the write cache on that volume, so move the fleet to a fresh, empty cache volume to roll back. 3.0.0 does not report a 2.8 proxy running against a switched volume, so make sure none is:

1. Stop every proxy that mounts the cache volume.
2. Point `cache_dir` on every proxy at a fresh, empty volume.
3. If you use `cache_rules.json`, copy it from the old `cache_dir` to the new one. It is the
   only file under `cache_dir` that you write yourself. Use the `config.yaml` and
   `cache_rules.json` that the earlier release ran with, not ones edited for 3.0.0.
4. Start the earlier release on every proxy.

Keep the old volume off every proxy running the earlier release. The new volume starts
cold: the cache holds only copies of S3 data, so each object is fetched from S3 again on its
first read. Log directories are separate settings from `cache_dir` and need no change.

### If the epoch stays at 0

`write_cache.staged_authority_activation_blocker` names what the switch is waiting for ([Metrics reference](METRICS_REFERENCE.md) lists the labels). The usual reason after an upgrade is a proxy you removed or replaced, named by `write_cache.staged_authority_activation_blocking_instance`. The switch stops waiting for it once its files on the volume have been unchanged for one day, and the day restarts whenever 3.0.0 clears an entry of that proxy's. `rotation_incomplete` means the write-cache figure has not been refreshed for every shard: read `staged_shard_authority_age_secs` against 86,400 and `staged_fold_shards_beyond_max_age`. `staged_fold_shards` at `0` means no upload is cached yet. `repair_rotation_incomplete` means the write-cache record check has not finished: compare `ledger_repair_swept_shards` with `ledger_repair_observed_shards`.

## Upgrading to 2.0.0 (per-bucket `_settings.json` removed)

2.0.0 replaced the per-bucket `cache_dir/metadata/{bucket}/_settings.json` mechanism
(including `prefix_overrides`) with a single optional `cache_dir/cache_rules.json`
holding an ordered list of glob rules matched against the full `{bucket}/{object_key}`
cache key.

There is no auto-migration. After upgrading, any `_settings.json` files are ignored, and the proxy logs a one-time warning if it
finds them under `cache_dir/metadata/`, so their settings stop taking effect until you
translate them by hand.

Watch the matching semantics while translating: the old `prefix_overrides` were prefix
matches, whereas rules are anchored globs. "Everything under `temp/` in `mybucket`"
becomes the pattern `mybucket/temp/**`, not `mybucket/temp`.

If you never used `_settings.json`, no action is needed. See
[Cache Rules](CONFIGURATION.md#cache-rules) and the 2.0.0 entry in
the [pre-3.0.0 changelog](../changelog-pre-v3.md) for the rule syntax and a before/after example.
