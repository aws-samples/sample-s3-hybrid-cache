# Upgrading

The upgrade flow is: rebuild (or copy the binary from a build host), replace, restart.
Configuration is backward-compatible at the field level — new options always have
defaults, so an existing `config.yaml` keeps parsing and running across versions. In
the normal case no config edits are required.

Field-level compatibility is not the same as identical behaviour. A release can keep
your config parsing while changing a **default**, so a deployment that pins nothing can
see different memory use, connection counts, network exposure, or on-disk layout after a
binary swap. The tables below list the releases that do one of those things, so you can
read only the ones between your current version and your target.

These tables are an index. The authoritative detail for each entry is in
[`CHANGELOG.md`](../CHANGELOG.md).

Releases from before 1.0.0 are not covered. Upgrading from a pre-1.0.0 build is not a
supported path — start from a fresh cache directory and a config derived from the
current [`config/config.example.yaml`](../config/config.example.yaml).

## Releases requiring a manual step

Do these before or while upgrading. Skipping one can mean a failed start or a cache that
cannot be read.

| Version | Action |
|---|---|
| 2.6.0 | To keep log files older than the configured retention, copy them off or raise `logging.access_log_retention_days` / `logging.app_log_retention_days` before upgrading — the first cleanup pass now enforces retention across the whole of `access_log_dir` and `app_log_dir`, including on a deployment running with `access_log_enabled: false`, where neither sweep previously ran. |
| 2.5.0 | If you set `server.max_concurrent_requests` explicitly, review the value — a permit is now held for the whole transfer, not just request setup, so the same number admits far fewer concurrent large transfers than before. Remove `server.max_buffered_request_body_bytes` if you set it — it is deprecated, has no effect, and logs a startup warning; if you had lowered it to reject large uploads, S3's own limits apply instead. On FSx for OpenZFS, add `nconnect=16` to the mount and remount, or disk-cache reads stay capped near 625 MB/s; EFS needs no change. |
| 2.2.0 | Copy the hardened `config/s3-proxy.service` unit if you installed the old one — it adds systemd sandbox directives and a `ReadWritePaths` allowlist. |
| 2.0.0 | Hand-translate any per-bucket `_settings.json` into `cache_dir/cache_rules.json`. No auto-migration; see [below](#upgrading-to-200-per-bucket-_settingsjson-removed). |
| 1.16.0 | If any TTL in your config exceeds 10 years, reduce it — startup now rejects it by design, naming the field. |
| 1.5.0 | `ttl_overrides` removed from YAML. Migrate those settings (to `_settings.json` then, or straight to `cache_rules.json` if you are also passing 2.0.0). |
| 1.3.0 | **Flush the cache** — `rm -rf cache_dir/*`. Cached data moved to the LZ4 frame format; old block-format `.bin` files are unreadable by the new decoder. |
| 1.1.0 | **Clear the cache directory**, and remove `shared_storage.enabled`, `size_tracking_flush_interval`, and `size_tracking_buffer_size` if present. |
| 1.0.13 | Multi-instance NFS deployments must mount the shared volume with `lookupcache=pos`. |

## Releases changing a default or observable behaviour

No action is needed to keep running, but the effect lands whether or not you pin
anything. Cache-key changes orphan previously-cached entries — they are re-fetched, not
corrupted.

| Version | Change on a binary-only upgrade |
|---|---|
| 2.7.0 | Four cache size figures on `/metrics` change. `write_cache.resident_bytes` and `cache.write_cache_size` drop, often substantially, once a full validation scan runs, then move with uploads, overwrites and removals instead of only ever growing. `cache.total_cache_size` becomes the bytes on the shared cache volume, identical on every instance sharing it, and `cache.read_cache_size` becomes the non-staged remainder of it, so `total = read + write_cache` holds exactly with `cache.ram_cache_size` outside that sum. Rebaseline absolute thresholds on all four, and correct any panel that stacked them or added read to write_cache — that was double-counting. `cache.write_cache_percent` becomes a reclamation target rather than an admission limit, enforced continuously against staged bytes across the shared volume, so a deployment whose staged working set has drifted above it sees background reclamation begin where none happened before; nothing is refused, but raise the percentage if that larger working set is intended. On a cache at or near `cache.max_cache_size`, write-through caching is now declined (`disk_safety` in `signed_put.skipped_puts_total`; `capacity_refused` retired), so a read of a just-uploaded object may miss where it previously hit — uploads themselves are never affected. `/health` can now report the cache component `Degraded` above 95% usage; HTTP status codes are unchanged. Cached range metadata gains one optional field that older releases ignore. New `/metrics` fields `cache.max_cache_size_limit`, `write_cache.graduations_total`, `write_cache.ledger_entries`, `eviction_coordination.staging_evictions_skipped_lock_held`. No manual step and no cache wipe; a write-cache figure inflated by an earlier release recovers on its own, and [EVICTION.md](EVICTION.md#recovery-from-an-inflated-write-cache-figure) has the timeline and a manual path. |
| 2.6.1 | With `metrics.otlp.enabled: true`, OTLP metrics now reach the collector where previously none did, and each instance reports its own `service.instance.id` — a Prometheus OTLP receiver that folded every instance into one `instance` label now produces one series per instance. |
| 2.5.0 | `cache.ram_cache_hit_rate_percent` on `/metrics` is now a 0–100 percentage instead of a 0.0–1.0 fraction; a dashboard that multiplied it by 100 now reads 100x high. `server.max_concurrent_requests` default 200 → 1000, and its meaning widens to cover the whole transfer, not just setup (see the manual-step table above). HEAD requests now stay cacheable past `head_ttl` instead of missing indefinitely, changing HEAD latency and hit-rate metrics. Health and metrics listeners now reject non-configured paths (404) and non-GET methods (405); update any probe or scraper that used another route. An upstream IP excluded after repeated failures now stays out of rotation until a recovery probe succeeds rather than returning at the next DNS refresh, so `health_probe_initial_cooldown` and `health_probe_max_cooldown` take effect for the first time. Presigned PUT uploads now succeed instead of returning 403, and proxy memory during an upload no longer scales with body size on any upload path, including browser form (POST) object uploads. `server.max_inflight_buffer_bytes` accepts a practical ceiling with no other configuration change; on a default config startup previously rejected any value below 5 GiB. |
| 2.4.0 | `max_ram_cache_size` default 256 → 512 MiB: **+256 MiB RAM per instance**. Pin `268435456` to stay at 256 MiB (runs 4 effective shards). `config.example.yaml` now ships the built-in `put_ttl` (1 hour) and `compression.threshold` (1024) instead of `1d` and `4096`; an existing config keeps its own explicit values. A client-supplied `If-Range` whose validator does not match the cached ETag is now forwarded to S3, so it costs one round trip and returns `200`-full instead of a `206` sliced from cache. |
| 2.3.0 | Compression extension denylist now enforced on writes; `compression.threshold` now applies (was hardcoded 1024); `compression.content_aware` removed (aliased, ignored, warns). |
| 2.2.3 | All cache-hit responses carry `X-Cache: HIT`, visible to clients and CDNs. |
| 2.2.0 | Health/metrics servers bind `0.0.0.0` by default (set `127.0.0.1` to restrict); RAM cache sharded 8 ways, entries above per-shard capacity dropped; `evaluate_conditions_from_cache` defaults `true`; `tcp_recv_buffer_size` unset so the kernel auto-tunes; CompleteMultipartUpload bodies over 10 MiB rejected with 413. |
| 1.16.4 | Build toolchain pinned to Rust 1.96 (`rust-toolchain.toml` and CI image). |
| 1.16.0 | Dashboard binds `127.0.0.1` (was `0.0.0.0`) — set it explicitly for remote access; `umask(0o077)` makes proxy-created files owner-only; `ram_cache_flush_interval` 60s → 10s raises journal write rate; signed-write bodies capped at 5 GiB with 413. |
| 1.14.1 | Virtual-hosted and accelerate cache keys change shape; previously-written flat keys are orphaned. |
| 1.14.0 | MSRV raised to Rust 1.89; SSE-C requests bypass the cache entirely (GET/HEAD/PUT). |
| 1.13.2 | If you copied the example config, `ip_distribution_enabled` was wrongly `false` there — the real default is `true`. |
| 1.13.1 | `compression.enabled: false` now genuinely disables compression, changing disk footprint and CPU. |
| 1.11.3 | Outbound TLS is per-destination: 1.3 for S3, 1.2 only for `endpoint_overrides` hosts. (1.11.1 had locked *all* outbound TLS to 1.2 whenever `endpoint_overrides` was non-empty.) |
| 1.9.9 | Seven OTLP metrics removed — dashboards and alarms referencing them break. |
| 1.9.0 | Connection `idle_timeout` 30s → 55s; `SO_RCVBUF` pinned to 256 KB per connection. |
| 1.7.8 | `ip_distribution_enabled` defaults `true`: per-IP connection pools multiply outbound connection count. |
| 1.7.6 | `allow_streaming` defaults `true`; the 1 MiB streaming threshold is gone, so all responses stream. |
| 1.7.1 | `max_idle_per_host` 10 → 100 (range 1–500), ~10x idle connections; streaming chunk 512 KiB → 1 MiB. |
| 1.7.0 | Logs are auto-deleted after 30 days by a background cleanup task. |
| 1.6.8 | Access log records gain a 25th field (`source_region`) — update log parsers. |
| 1.5.2 | On-disk metadata schema change: expiry moves from per-range to object level. |
| 1.4.5 | Metric names gain a `_percent` suffix in `/metrics` and OTLP (dashboards break); RAM cache compresses unconditionally, ignoring `compression.enabled`. |
| 1.4.2 | All OTLP gauge names change, and `cache.size` loses its `cache_type` dimension. |
| 1.3.0 | `--compression-enabled` CLI flag removed; use `COMPRESSION_ENABLED` or `compression.enabled`. |
| 1.2.7 | A `Referer` header is injected into every forwarded S3 request by default (`server.add_referer_header`). Renamed in 2.0.0 — update any access-log Referer queries. |
| 1.2.5 | RAM data cache is silently disabled whenever `get_ttl` is `0s`. |
| 1.2.2 | Request coalescing on by default: concurrent misses wait rather than each fetching. |
| 1.1.0 | Consolidation interval 30s → 5s, raising shared-storage I/O. |

Releases not listed above require no action and change no defaults: internal fixes,
performance work, additive opt-in features that stay disabled until configured, and
dependency bumps.

## Upgrading to 2.0.0 (per-bucket `_settings.json` removed)

2.0.0 replaced the per-bucket `cache_dir/metadata/{bucket}/_settings.json` mechanism
(including `prefix_overrides`) with a single optional `cache_dir/cache_rules.json`
holding an ordered list of glob rules matched against the full `{bucket}/{object_key}`
cache key.

There is no auto-migration. After upgrading, any `_settings.json` files are ignored —
the proxy logs a one-time warning if it finds them under `cache_dir/metadata/` — so
their settings stop taking effect until you translate them by hand.

Watch the matching semantics while translating: the old `prefix_overrides` were prefix
matches, whereas rules are anchored globs. "Everything under `temp/` in `mybucket`"
becomes the pattern `mybucket/temp/**`, not `mybucket/temp`.

If you never used `_settings.json`, no action is needed. See
[Cache Rules](CONFIGURATION.md#cache-rules) and the 2.0.0 entry in
[`CHANGELOG.md`](../CHANGELOG.md) for the rule syntax and a before/after example.
