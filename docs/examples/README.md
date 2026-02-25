# Bucket Settings Examples

Example `_settings.json` files for per-bucket cache configuration. Place a `_settings.json` file at `cache_dir/metadata/{bucket}/_settings.json` to override global settings for that bucket.

All fields are optional — omitted fields fall back to global config values.

## Settings Cascade

Settings resolve in this precedence order (highest to lowest):

1. **Prefix override** — longest matching prefix in `prefix_overrides`
2. **Bucket settings** — top-level fields in `_settings.json`
3. **Global config** — values from the YAML configuration file

## Allowlist Pattern

To cache only specific buckets, set `read_cache_enabled: false` in the global YAML config, then create a `_settings.json` with `read_cache_enabled: true` for each bucket you want cached. See `allowlist-pattern-settings.json`.

## Examples

| File | Use Case |
|---|---|
| `static-assets-settings.json` | Static assets bucket with long TTL (7d) and compression enabled |
| `frequently-updated-settings.json` | Frequently-updated data with short TTL (30s) and RAM cache disabled |
| `zero-ttl-auth-settings.json` | Authentication bucket where every GET/HEAD revalidates with S3 (`get_ttl=0s`) |
| `no-cache-prefix-settings.json` | Bucket with a `/volatile/` prefix that bypasses read caching entirely |
| `allowlist-pattern-settings.json` | Bucket override enabling caching when global `read_cache_enabled` is `false` |
| `prefix-overrides-settings.json` | Bucket with multiple prefix overrides for different key paths |

## Automatic Behaviors

- `get_ttl: "0s"` forces `ram_cache_eligible` to `false` (RAM cache bypasses revalidation)
- `read_cache_enabled: false` forces `ram_cache_eligible` to `false`
- Settings reload automatically when the file changes on disk (default staleness threshold: 60s)
- Invalid JSON or validation errors fall back to previously valid settings

## Duration Format

TTL values accept human-readable durations: `"0s"`, `"30s"`, `"5m"`, `"1h"`, `"7d"`, `"500ms"`.

## Atomic Updates

On shared NFS storage, write settings atomically to avoid partial reads:

```bash
echo '{"get_ttl": "5m"}' > _settings.json.tmp
mv _settings.json.tmp _settings.json
```
