# Cache Rules Examples

Example `cache_rules.json` files. Each overrides global cache settings for keys matching a glob pattern, without editing the YAML config or restarting the proxy.

To use one, copy it to `cache_dir/cache_rules.json`:

```bash
cp static-assets-rules.json /path/to/cache_dir/cache_rules.json
```

The proxy reloads the file automatically when it changes (default staleness threshold: 60s). The file is optional — when absent or when its `rules` array is empty, every setting resolves to the global YAML `cache:` defaults.

For the authoritative reference, see [CONFIGURATION.md — Cache Rules](../CONFIGURATION.md#cache-rules). The JSON schema is [`docs/cache-rules-schema.json`](../cache-rules-schema.json) and a fuller worked example is [`config/cache_rules.example.json`](../../config/cache_rules.example.json).

## File Format

Each file is a `{ "rules": [...] }` document with an optional `$schema` reference and an ordered `rules` array. Each rule has a required glob `pattern` plus an optional subset of settings fields. Omitted fields fall through (see Precedence below).

```json
{
  "$schema": "../cache-rules-schema.json",
  "rules": [
    { "pattern": "my-bucket/static/**", "get_ttl": "7d", "compression_enabled": true },
    { "pattern": "**", "get_ttl": "5m" }
  ]
}
```

Optional per-rule fields: `get_ttl`, `head_ttl`, `put_ttl`, `read_cache_enabled`, `write_cache_enabled`, `compression_enabled`, `ram_cache_eligible`, `evaluate_conditions_from_cache`.

## Glob Syntax

A pattern matches against the full cache key, `{bucket}/{object_key}` (no leading slash). It is anchored to the whole key and is case-sensitive.

| Glob | Matches |
|------|---------|
| `*`  | Any run of characters except `/` — one path segment |
| `**` | Any run of characters including `/` — crosses segments |
| `?`  | Exactly one character except `/` |
| any other char | A literal |

Because the bucket is part of the match surface, one syntax expresses every scope:

| Pattern | Meaning |
|---------|---------|
| `my-bucket/temp/**` | The `temp/` prefix in one bucket |
| `**/logs/**` | A `logs/` segment anywhere, in every bucket |
| `prod-*/static/**` | The `static/` prefix in every bucket whose name starts `prod-` |

Patterns are anchored, so `my-bucket/temp` matches only that exact key. To match everything under a prefix, use `my-bucket/temp/**`.

## First-Match-Per-Field Precedence

Rules are an ordered list. For each field independently, the value comes from the earliest rule that both matches the key and sets that field, falling through to the global YAML default. Put specific rules above broad ones.

## Examples

| File | Use Case |
|---|---|
| `static-assets-rules.json` | Static assets bucket with long TTL (7d) and compression enabled |
| `frequently-updated-rules.json` | Frequently-updated data with short TTL (30s) and RAM cache disabled |
| `zero-ttl-auth-rules.json` | Authentication bucket where every GET/HEAD revalidates with S3 (`get_ttl: "0s"`) |
| `no-cache-prefix-rules.json` | Bucket with a `volatile/` prefix that bypasses read caching, everything else cached 1h |
| `allowlist-pattern-rules.json` | Rule enabling caching for one bucket when global `read_cache_enabled` is `false` |
| `path-prefix-rules.json` | Matching several path prefixes within a bucket with distinct settings each |

## Allowlist Pattern

To cache only specific keys, set `read_cache_enabled: false` in the global YAML config, then add a rule with `"read_cache_enabled": true` for each pattern you want cached. See `allowlist-pattern-rules.json`.

## Automatic Behaviors

- `get_ttl: "0s"` forces `ram_cache_eligible` to `false` (RAM cache bypasses revalidation)
- `read_cache_enabled: false` forces `ram_cache_eligible` to `false`
- The rule set reloads automatically when the file changes on disk (default staleness threshold: 60s)
- Invalid JSON or validation errors keep the last-known-good rule set

## Duration Format

TTL values accept human-readable durations: `"0s"`, `"30s"`, `"5m"`, `"1h"`, `"7d"`, `"500ms"`.

## Atomic Updates

On shared NFS storage, write the file atomically to avoid partial reads:

```bash
cp new-rules.json cache_rules.json.tmp
mv cache_rules.json.tmp /path/to/cache_dir/cache_rules.json
```
