# Examples

This directory holds two kinds of example file: complete proxy configurations
(`config-*.yaml`) and cache rules files (`*-rules.json`).

## Configuration examples

Each `config-*.yaml` file is a complete configuration you can start the proxy with:

| File | Profile |
|---|---|
| `config-development.yaml` | Local cache, debug logging, unprivileged ports, loopback dashboard |
| `config-production.yaml` | Standard ports, the TLS proxy listener (port 3129), a cache volume shared by the fleet, shared logs, OTLP export |
| `config-high-performance.yaml` | A 1 TiB disk cache and a 10 GiB RAM cache, with less frequent dashboard refreshes |

```bash
s3-proxy --config docs/examples/config-development.yaml
```

The production and large-cache profiles assume two shared mounts: `/mnt/cache` for the cache
volume, and `/mnt/nfs` for configuration, TLS material and logs. They also use ports 80 and 443,
which need root or `CAP_NET_BIND_SERVICE`. The two mounts are separate because the
consumed-bytes reading covers the whole file system, so it describes the cache only when the
cache volume is its own file system. That is recommended, not required; see the `cache_dir`
comment in `config-production.yaml` and [Running a fleet on one cache volume](../SHARED_STORAGE.md). For a local
trial, adjust the paths and ports, or start from the development profile, which uses relative
paths and unprivileged ports.

`config/s3-proxy.service` ships with `ReadWritePaths` and `RequiresMountsFor` set to these
paths. On AWS the volumes are named after the services behind them; see
[AWS deployment](../AWS_DEPLOYMENT.md) and the `userdata-*.sh` scripts here.

## Cache rules examples

Each `*-rules.json` file is a `cache_rules.json` that overrides the global cache settings for
keys matching a glob pattern, without editing the YAML configuration or restarting the proxy.
To use one, copy it to `cache_dir/cache_rules.json`:

```bash
cp static-assets-rules.json /path/to/cache_dir/cache_rules.json
```

The proxy re-reads the file every `bucket_settings_staleness_threshold` (default 60s). The file
is optional: when it is absent or its `rules` array is empty, every setting comes from the
global `cache:` section of the YAML configuration.

[Cache rules](../CONFIGURATION.md#cache-rules) is the reference for the file format, the glob
syntax, the per-rule fields and what happens when a reload fails.
[Duration format](../CONFIGURATION.md#duration-format) lists the TTL values accepted. The JSON
schema is [`docs/cache-rules-schema.json`](../cache-rules-schema.json), and
[`config/cache_rules.example.json`](../../config/cache_rules.example.json) is a fuller worked
example.

| File | Use case |
|---|---|
| `static-assets-rules.json` | A static assets bucket with a long TTL (7d) and compression enabled |
| `frequently-updated-rules.json` | Frequently updated data with a short TTL (30s) and the RAM cache disabled |
| `zero-ttl-auth-rules.json` | An authentication bucket where every GET and HEAD revalidates with S3 (`get_ttl: "0s"`) |
| `no-cache-prefix-rules.json` | A bucket whose `volatile/` prefix bypasses the read cache, with everything else cached for 1h |
| `allowlist-pattern-rules.json` | A rule that enables caching for one bucket while the global `read_cache_enabled` is `false` |
| `path-prefix-rules.json` | Several path prefixes in one bucket, each with its own settings |
| `page-aligned-parquet-rules.json` | Page-aligned range caching for a Parquet prefix, so footer and column-chunk reads that cluster within a page hit the cache |

Two rules of the format shape every example. Each field takes its value from the earliest rule
that matches the key and sets that field, falling through to the global YAML default, so put
specific rules above broad ones. Patterns are anchored to the whole cache key,
`{bucket}/{object_key}`, so `my-bucket/temp` matches only that exact key and
`my-bucket/temp/**` matches everything under the prefix.

Two settings make a key ineligible for the RAM cache whatever `ram_cache_eligible` says:
`get_ttl: "0s"`, because a RAM cache hit skips revalidation, and `read_cache_enabled: false`.

### Allowlist pattern

To cache only specific keys, set `read_cache_enabled: false` in the global YAML configuration,
then add a rule with `"read_cache_enabled": true` for each pattern to cache.
`allowlist-pattern-rules.json` does this for one bucket.

### Atomic updates

On a cache volume shared over NFS, write the file atomically so that no proxy reads a partial
file. Stage the copy in the same directory, so the rename stays on one file system:

```bash
cp new-rules.json /path/to/cache_dir/cache_rules.json.tmp
mv /path/to/cache_dir/cache_rules.json.tmp /path/to/cache_dir/cache_rules.json
```
