//! Cache Match Rules
//!
//! Glob-based cache rules loaded from a single hot-reloadable file at
//! `cache_dir/cache_rules.json`. Each rule is a glob `pattern` plus an optional
//! subset of cache settings. Patterns are matched against the full cache key
//! (`{bucket}/{object_key}`, or the access-point / MRAP / S3-compatible cache-key
//! form). Resolution is first-match-per-field over the ordered rule list, falling
//! through to the global config scalar defaults.
//!
//! Glob syntax (user-facing):
//! - `*`  matches any run of characters except `/` (one path segment)
//! - `**` matches any run of characters including `/` (crosses segments)
//! - `?`  matches exactly one character except `/`
//! - every other character is a literal (regex metacharacters are escaped)
//!
//! Internally each glob is translated to an anchored regex and all rules are
//! compiled into one `regex::RegexSet` at load time (never per request).
//!
//! This replaces the former per-bucket `_settings.json` mechanism. That is a
//! breaking change with no automatic migration.

use regex::RegexSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::config::duration_serde;

/// Default maximum number of rules. A generous safety guardrail; the real cost
/// driver is match fan-out per key, not total rule count. Confirmed by benchmark.
pub const DEFAULT_MAX_RULES: usize = 1024;

/// Default page size for page-aligned range caching (widening): 16 MiB.
/// Used when a rule enables `page_widening` without specifying `page_size`.
///
/// Spec: page-aligned-range-cache Requirement 1.4.
pub const DEFAULT_PAGE_SIZE: u64 = 16 * 1024 * 1024;

/// Default TTFB threshold after which a hedge is issued: 250ms.
/// Used when a rule enables `hedging_enabled` without specifying
/// `hedge_trigger_after`. Sits well below the 5s `upstream_first_byte_timeout`
/// default so both the constant and the ceiling are valid out of the box.
///
/// Spec: hedged-upstream-requests Requirement 3.1.
pub const DEFAULT_HEDGE_TRIGGER_AFTER: Duration = Duration::from_millis(250);

/// Default per-request hedge budget: 1.
/// Used when a rule enables `hedging_enabled` without specifying
/// `hedge_max_per_request`. For a range GET fanning out into N parallel
/// sub-fetches this budget is shared across all N — at most one hedge fires.
///
/// Spec: hedged-upstream-requests Requirement 6.1.
pub const DEFAULT_HEDGE_MAX_PER_REQUEST: usize = 1;

/// Custom deserializer for optional Duration fields in rule JSON.
/// Handles both string values ("30s", "5m") and null/missing fields.
fn deserialize_optional_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => duration_serde::parse_duration(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

/// Custom serializer for optional Duration fields.
/// Converts Duration to a human-readable string that round-trips through `parse_duration`.
fn serialize_optional_duration<S>(
    duration: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match duration {
        Some(d) => serializer.serialize_some(&format_duration(*d)),
        None => serializer.serialize_none(),
    }
}

/// Format a Duration as a human-readable string compatible with `parse_duration`.
/// Uses the largest whole unit that represents the duration exactly, falling back to seconds.
pub fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let nanos = d.subsec_nanos();

    if total_secs == 0 && nanos == 0 {
        return "0s".to_string();
    }

    // If there are sub-second components, use milliseconds
    if nanos > 0 {
        let total_ms = total_secs * 1000 + nanos as u64 / 1_000_000;
        // Check if nanos are an exact number of milliseconds
        if nanos.is_multiple_of(1_000_000) {
            return format!("{}ms", total_ms);
        }
        // Fall back to seconds with fractional part
        return format!("{}s", d.as_secs_f64());
    }

    // Use the largest whole unit
    if total_secs.is_multiple_of(86400) {
        format!("{}d", total_secs / 86400)
    } else if total_secs.is_multiple_of(3600) {
        format!("{}h", total_secs / 3600)
    } else if total_secs.is_multiple_of(60) {
        format!("{}m", total_secs / 60)
    } else {
        format!("{}s", total_secs)
    }
}

/// Translate a user glob pattern into an anchored regex string.
///
/// Returns an error for empty/whitespace-only patterns. The output is always
/// anchored (`^…$`) so a pattern matches only the entire cache key. Matching is
/// case-sensitive (the regex default), mirroring S3 key semantics.
pub fn glob_to_regex(pattern: &str) -> Result<String, String> {
    if pattern.trim().is_empty() {
        return Err("pattern is empty".to_string());
    }

    let mut out = String::with_capacity(pattern.len() * 2 + 2);
    out.push('^');

    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '*' => {
                // `**` crosses `/`; `*` stays within a path segment.
                if i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    out.push_str(".*");
                    i += 2;
                } else {
                    out.push_str("[^/]*");
                    i += 1;
                }
            }
            '?' => {
                out.push_str("[^/]");
                i += 1;
            }
            other => {
                // Escape any regex metacharacter; pass through normal chars.
                // `regex::escape` handles multi-byte chars correctly via the &str slice.
                let ch_str = &pattern[i..i + other.len_utf8()];
                out.push_str(&regex::escape(ch_str));
                i += other.len_utf8();
            }
        }
    }

    out.push('$');
    Ok(out)
}

/// The optional settings fields a rule (or resolution layer) may set.
/// All optional; omitted fields fall through to the next layer.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub struct CacheRule {
    /// Glob pattern matched against the full cache key.
    pub pattern: String,

    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub get_ttl: Option<Duration>,

    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub head_ttl: Option<Duration>,

    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub put_ttl: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_cache_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_cache_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_cache_eligible: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluate_conditions_from_cache: Option<bool>,

    /// Enables page-aligned range caching (widening) for keys matching this
    /// rule. Off by default — never enabled globally, only per-key via an
    /// explicit rule. Spec: page-aligned-range-cache Requirement 1.1, 1.2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_widening: Option<bool>,

    /// Per-key page size (in bytes) for page-aligned range caching. When a
    /// rule enables `page_widening` without specifying this, resolution falls
    /// back to [`DEFAULT_PAGE_SIZE`] (16 MiB). Must be `> 0` and
    /// `<= 64 MiB` for any rule enabling widening (validated at startup).
    /// Spec: page-aligned-range-cache Requirement 1.4, 7.9.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u64>,

    /// Enables hedged upstream requests for keys matching this rule. Off by
    /// default — never enabled globally, only per-key via an explicit rule.
    /// Spec: hedged-upstream-requests Requirement 1.1, 1.2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hedging_enabled: Option<bool>,

    /// Per-key TTFB threshold after which a Hedge is issued. Falls back to
    /// [`DEFAULT_HEDGE_TRIGGER_AFTER`] (250ms). Must be > 0 and strictly less
    /// than `connection_pool.upstream_first_byte_timeout` for any rule enabling
    /// hedging (validated on every rules load).
    /// Spec: hedged-upstream-requests Requirement 3.1, 9.2.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub hedge_trigger_after: Option<Duration>,

    /// Per-key cap on Hedges for one client request, shared across the parallel
    /// missing-range sub-fetches of a range GET. Falls back to
    /// [`DEFAULT_HEDGE_MAX_PER_REQUEST`] (1).
    /// Spec: hedged-upstream-requests Requirement 6.1, 6.5.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hedge_max_per_request: Option<usize>,
}

/// The on-disk `cache_rules.json` shape.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CacheRules {
    /// Optional JSON schema reference for IDE validation.
    #[serde(rename = "$schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Ordered list of rules. Order is significant (first-match-per-field).
    #[serde(default)]
    pub rules: Vec<CacheRule>,
}

impl CacheRules {
    /// Validate the rule set. Returns a list of human-readable errors; empty = valid.
    /// Checks: non-empty patterns, compilable globs, the rule-count cap, (for
    /// any rule enabling `page_widening`) that `page_size` is `0 < page_size <= 64 MiB`
    /// (Spec: page-aligned-range-cache Requirement 7.9), and (for any rule
    /// enabling `hedging_enabled`) that `hedge_trigger_after` is > 0 and <
    /// `upstream_first_byte_timeout` (Spec: hedged-upstream-requests Requirement 9.2).
    pub fn validate(&self, max_rules: usize, upstream_first_byte_timeout: Duration) -> Vec<String> {
        let mut errors = Vec::new();

        if self.rules.len() > max_rules {
            errors.push(format!(
                "rule count {} exceeds maximum {}",
                self.rules.len(),
                max_rules
            ));
        }

        for (i, rule) in self.rules.iter().enumerate() {
            match glob_to_regex(&rule.pattern) {
                Ok(re) => {
                    if regex::Regex::new(&re).is_err() {
                        errors.push(format!("rules[{}]: pattern failed to compile", i));
                    }
                }
                Err(e) => errors.push(format!("rules[{}]: {}", i, e)),
            }
        }

        // A rule that enables page_widening must carry a page_size within
        // (0, RAM_CACHE_ADMISSION_CEILING] — a Page can never exceed the fixed
        // 64 MiB RAM admission ceiling (Requirement 7.9). A rule that leaves
        // page_size unset falls back to DEFAULT_PAGE_SIZE at resolution time
        // and is always valid; only an explicit out-of-range value is rejected
        // here.
        for (i, rule) in self.rules.iter().enumerate() {
            if rule.page_widening == Some(true) {
                if let Some(page_size) = rule.page_size {
                    if page_size == 0 {
                        errors.push(format!(
                            "rules[{}]: page_size must be greater than 0 when page_widening is enabled",
                            i
                        ));
                    } else if page_size > crate::ram_cache::RAM_CACHE_ADMISSION_CEILING as u64 {
                        errors.push(format!(
                            "rules[{}]: page_size {} exceeds the maximum of {} bytes (64 MiB)",
                            i,
                            page_size,
                            crate::ram_cache::RAM_CACHE_ADMISSION_CEILING
                        ));
                    }
                }
            }
        }

        // Hedging validation (Req 9.2): for any rule enabling hedging, an
        // explicit hedge_trigger_after of 0 or >= upstream_first_byte_timeout
        // is rejected. A rule that leaves hedge_trigger_after unset resolves to
        // the DEFAULT_HEDGE_TRIGGER_AFTER constant and is always valid against
        // the 5s default — only explicit out-of-range values are rejected,
        // exactly the page_size pattern.
        for (i, rule) in self.rules.iter().enumerate() {
            if rule.hedging_enabled == Some(true) {
                if let Some(trigger) = rule.hedge_trigger_after {
                    if trigger.is_zero() {
                        errors.push(format!(
                            "rules[{}]: hedge_trigger_after must be greater than 0 when hedging is enabled",
                            i
                        ));
                    } else if trigger >= upstream_first_byte_timeout {
                        errors.push(format!(
                            "rules[{}]: hedge_trigger_after ({}) must be strictly less than upstream_first_byte_timeout ({})",
                            i,
                            format_duration(trigger),
                            format_duration(upstream_first_byte_timeout),
                        ));
                    }
                }
            }
        }

        errors
    }
}

/// Fully resolved settings for a specific cache key.
/// Every field has a concrete value (no Options) after resolution.
#[derive(Debug, Clone)]
pub struct ResolvedSettings {
    pub get_ttl: Duration,
    pub head_ttl: Duration,
    pub put_ttl: Duration,
    pub read_cache_enabled: bool,
    pub write_cache_enabled: bool,
    pub compression_enabled: bool,
    /// True iff a matched rule explicitly set `compression_enabled` for this
    /// key, as opposed to `compression_enabled` falling through to the
    /// global default. Drives rules-win semantics for the built-in
    /// extension denylist in `CacheManager::effective_compression`: an
    /// explicit rule value is honored verbatim (bypassing the denylist in
    /// either direction); a fallthrough value is combined with the denylist.
    pub compression_from_rule: bool,
    pub ram_cache_eligible: bool,
    pub evaluate_conditions_from_cache: bool,
    /// Whether page-aligned range caching (widening) is enabled for this key.
    /// Off unless an explicit rule sets `page_widening: true` (first-match-per-field).
    /// Spec: page-aligned-range-cache Requirement 1.1, 1.2, 1.3.
    pub page_widening: bool,
    /// Per-key page size (bytes) used when `page_widening` is enabled. Falls
    /// back to [`DEFAULT_PAGE_SIZE`] (16 MiB) when no rule sets it explicitly.
    /// Spec: page-aligned-range-cache Requirement 1.4.
    pub page_size: u64,
    /// Whether hedged upstream requests are enabled for this key. Off unless
    /// an explicit rule sets `hedging_enabled: true` (first-match-per-field).
    /// Spec: hedged-upstream-requests Requirement 1.1, 1.2, 1.3.
    pub hedging_enabled: bool,
    /// Per-key TTFB threshold after which a Hedge is issued. Falls back to
    /// [`DEFAULT_HEDGE_TRIGGER_AFTER`] (250ms) when no rule sets it explicitly.
    /// Spec: hedged-upstream-requests Requirement 3.1.
    pub hedge_trigger_after: Duration,
    /// Per-key cap on Hedges for one client request. Falls back to
    /// [`DEFAULT_HEDGE_MAX_PER_REQUEST`] (1) when no rule sets it explicitly.
    /// Spec: hedged-upstream-requests Requirement 6.1.
    pub hedge_max_per_request: usize,
    /// Tracks which layer provided the dominant settings.
    pub source: SettingsSource,
}

/// Indicates which layer of resolution provided the resolved values.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingsSource {
    /// No rule matched; all fields from global config defaults.
    Global,
    /// At least one rule matched; carries the index and pattern of the first match.
    Rule(usize, String),
}

impl Default for ResolvedSettings {
    /// Permissive defaults used as a neutral fallback (and by tests that drive
    /// the request-path entry points directly). Mirrors a no-rules resolution
    /// against typical global defaults: caching enabled, compression off, RAM
    /// eligible, 1h TTLs, `source = Global`. Production resolves settings once
    /// per request via [`crate::cache::CacheManager::resolve_settings`] and
    /// threads that value in; it never relies on this default.
    fn default() -> Self {
        Self {
            get_ttl: Duration::from_secs(3600),
            head_ttl: Duration::from_secs(3600),
            put_ttl: Duration::from_secs(3600),
            read_cache_enabled: true,
            write_cache_enabled: true,
            compression_enabled: false,
            compression_from_rule: false,
            ram_cache_eligible: true,
            evaluate_conditions_from_cache: true,
            page_widening: false,
            page_size: DEFAULT_PAGE_SIZE,
            hedging_enabled: false,
            hedge_trigger_after: DEFAULT_HEDGE_TRIGGER_AFTER,
            hedge_max_per_request: DEFAULT_HEDGE_MAX_PER_REQUEST,
            source: SettingsSource::Global,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GlobalDefaults {
    pub get_ttl: Duration,
    pub head_ttl: Duration,
    pub put_ttl: Duration,
    pub read_cache_enabled: bool,
    pub write_cache_enabled: bool,
    pub compression_enabled: bool,
    pub ram_cache_enabled: bool,
    pub evaluate_conditions_from_cache: bool,
    /// The `connection_pool.upstream_first_byte_timeout` value, threaded here
    /// so `CacheRules::validate` can enforce Req 9.2 (hedge_trigger_after <
    /// first-byte timeout) without a separate parameter channel.
    pub upstream_first_byte_timeout: Duration,
}

/// A rule plus its compiled regex index. The regex itself lives in the shared
/// `RegexSet`; this keeps the original pattern and values for resolution/display.
#[derive(Debug, Clone)]
struct CompiledRule {
    rule: CacheRule,
}

/// Parsed, validated, compiled rule set held in memory.
#[derive(Debug, Clone)]
struct RuleSet {
    rules: Vec<CompiledRule>,
    /// One automaton over all anchored pattern translations.
    /// `regex_set` index i corresponds to `rules[i]`.
    regex_set: RegexSet,
}

impl RuleSet {
    /// Build a compiled rule set from parsed rules. Caller must have validated first.
    fn build(rules: Vec<CacheRule>) -> Result<Self, String> {
        let patterns: Result<Vec<String>, String> =
            rules.iter().map(|r| glob_to_regex(&r.pattern)).collect();
        let patterns = patterns?;
        let regex_set = RegexSet::new(&patterns).map_err(|e| e.to_string())?;
        let compiled = rules
            .into_iter()
            .map(|rule| CompiledRule { rule })
            .collect();
        Ok(Self {
            rules: compiled,
            regex_set,
        })
    }

    /// An empty rule set: matches nothing, so resolution always yields globals.
    fn empty() -> Self {
        Self {
            rules: Vec::new(),
            // RegexSet::empty() never matches.
            regex_set: RegexSet::empty(),
        }
    }

    /// Resolve a full cache key to concrete settings using first-match-per-field.
    fn resolve(&self, full_key: &str, g: &GlobalDefaults) -> ResolvedSettings {
        // Matched indices in ascending (list) order.
        let matched: Vec<usize> = self.regex_set.matches(full_key).into_iter().collect();

        // Record the source as the first matching rule (if any).
        let source = match matched.first() {
            Some(&idx) => SettingsSource::Rule(idx, self.rules[idx].rule.pattern.clone()),
            None => SettingsSource::Global,
        };

        // For each field, the first matched rule that sets it wins; else global.
        let first = |pick: &dyn Fn(&CacheRule) -> Option<bool>| -> Option<bool> {
            matched.iter().find_map(|&i| pick(&self.rules[i].rule))
        };
        let first_dur = |pick: &dyn Fn(&CacheRule) -> Option<Duration>| -> Option<Duration> {
            matched.iter().find_map(|&i| pick(&self.rules[i].rule))
        };
        let first_u64 = |pick: &dyn Fn(&CacheRule) -> Option<u64>| -> Option<u64> {
            matched.iter().find_map(|&i| pick(&self.rules[i].rule))
        };

        let get_ttl = first_dur(&|r| r.get_ttl).unwrap_or(g.get_ttl);
        let head_ttl = first_dur(&|r| r.head_ttl).unwrap_or(g.head_ttl);
        let put_ttl = first_dur(&|r| r.put_ttl).unwrap_or(g.put_ttl);
        let read_cache_enabled = first(&|r| r.read_cache_enabled).unwrap_or(g.read_cache_enabled);
        let write_cache_enabled =
            first(&|r| r.write_cache_enabled).unwrap_or(g.write_cache_enabled);
        let compression_enabled_from_rule = first(&|r| r.compression_enabled);
        let compression_from_rule = compression_enabled_from_rule.is_some();
        let compression_enabled = compression_enabled_from_rule.unwrap_or(g.compression_enabled);
        let mut ram_cache_eligible =
            first(&|r| r.ram_cache_eligible).unwrap_or(g.ram_cache_enabled);
        let evaluate_conditions_from_cache = first(&|r| r.evaluate_conditions_from_cache)
            .unwrap_or(g.evaluate_conditions_from_cache);
        // page_widening is off by default — never enabled globally, only via
        // an explicit rule (Requirement 1.2). There is no global fallback.
        let page_widening = first(&|r| r.page_widening).unwrap_or(false);
        let page_size = first_u64(&|r| r.page_size).unwrap_or(DEFAULT_PAGE_SIZE);

        // Hedging is off by default — never enabled globally, only per-key
        // via an explicit rule (hedged-upstream-requests Requirement 1.2).
        let hedging_enabled = first(&|r| r.hedging_enabled).unwrap_or(false);
        let hedge_trigger_after =
            first_dur(&|r| r.hedge_trigger_after).unwrap_or(DEFAULT_HEDGE_TRIGGER_AFTER);
        let first_usize = |pick: &dyn Fn(&CacheRule) -> Option<usize>| -> Option<usize> {
            matched.iter().find_map(|&i| pick(&self.rules[i].rule))
        };
        let hedge_max_per_request =
            first_usize(&|r| r.hedge_max_per_request).unwrap_or(DEFAULT_HEDGE_MAX_PER_REQUEST);

        // Post-resolution invariants (unchanged from prior behaviour):
        // - Zero get_ttl → RAM range cache ineligible (RAM cache bypasses revalidation)
        // - Read cache disabled → RAM range cache ineligible
        if get_ttl == Duration::ZERO {
            ram_cache_eligible = false;
        }
        if !read_cache_enabled {
            ram_cache_eligible = false;
        }

        ResolvedSettings {
            get_ttl,
            head_ttl,
            put_ttl,
            read_cache_enabled,
            write_cache_enabled,
            compression_enabled,
            compression_from_rule,
            ram_cache_eligible,
            evaluate_conditions_from_cache,
            page_widening,
            page_size,
            hedging_enabled,
            hedge_trigger_after,
            hedge_max_per_request,
            source,
        }
    }
}

/// Cached rule set with load timestamp and last-known-good fallback.
struct CachedRules {
    ruleset: RuleSet,
    loaded_at: Instant,
    /// The rules as parsed (for the dashboard).
    rules: Vec<CacheRule>,
    /// Previous valid rule set, kept as fallback if a reload produces invalid content.
    previous_valid: Option<(RuleSet, Vec<CacheRule>)>,
    /// Whether a `cache_rules.json` file exists on disk.
    has_rules_file: bool,
}

/// Thread-safe manager for loading, caching, and resolving cache rules.
/// Lazily loads the rules file on first access, then caches with a staleness threshold.
pub struct BucketSettingsManager {
    cache_dir: PathBuf,
    /// Cached rule set (single global file).
    cache: RwLock<Option<CachedRules>>,
    /// Staleness threshold — a loaded rule set older than this triggers a re-read.
    staleness_threshold: Duration,
    /// Global config scalar defaults used as the lowest-precedence fallback.
    global_config: GlobalDefaults,
    /// Maximum number of rules permitted.
    max_rules: usize,
    /// Single-flight coordination: only one task reloads the rules file at a time.
    pending_load: Mutex<()>,
    /// Count of calls to [`resolve`](Self::resolve), incremented once per call.
    /// Used by the resolve-once regression test (Requirement 8.2) to assert that
    /// a multi-range request resolves settings exactly once rather than once per
    /// spawned per-range cache-write task. A single relaxed atomic increment is
    /// off the latency-sensitive matching work and adds no measurable cost.
    resolve_calls: AtomicUsize,
    // -- Reload health counters (item 3) --
    /// Total successful rule-file loads since startup.
    reloads_total: AtomicU64,
    /// Total rule-file load failures (parse, validation, compile) since startup.
    reload_failures_total: AtomicU64,
    /// Whether the running ruleset is a stale fallback (last load failed).
    on_fallback: AtomicBool,
    /// Number of rules currently loaded.
    rules_loaded: AtomicUsize,
    /// Unix timestamp (seconds) of the last successful load.
    last_load_unix: AtomicU64,
}

impl BucketSettingsManager {
    /// Create a new manager with global defaults and staleness threshold.
    pub fn new(
        cache_dir: PathBuf,
        global_config: GlobalDefaults,
        staleness_threshold: Duration,
    ) -> Self {
        Self {
            cache_dir,
            cache: RwLock::new(None),
            staleness_threshold,
            global_config,
            max_rules: DEFAULT_MAX_RULES,
            pending_load: Mutex::new(()),
            resolve_calls: AtomicUsize::new(0),
            reloads_total: AtomicU64::new(0),
            reload_failures_total: AtomicU64::new(0),
            on_fallback: AtomicBool::new(false),
            rules_loaded: AtomicUsize::new(0),
            last_load_unix: AtomicU64::new(0),
        }
    }

    /// Extract bucket name from a cache key or request path.
    /// Cache keys have the form "/{bucket}/{key}" or "{bucket}/{key}".
    /// Returns `None` for empty paths or paths that are just "/".
    /// Retained for per-bucket metrics attribution.
    pub fn extract_bucket(path: &str) -> Option<&str> {
        let trimmed = path.strip_prefix('/').unwrap_or(path);
        let bucket = match trimmed.find('/') {
            Some(pos) => &trimmed[..pos],
            None => trimmed,
        };
        if bucket.is_empty() {
            None
        } else {
            Some(bucket)
        }
    }

    /// Path to the rules file: `{cache_dir}/cache_rules.json`.
    fn rules_path(&self) -> PathBuf {
        self.cache_dir.join("cache_rules.json")
    }

    /// Resolve settings for a full cache key. Handles lazy load/reload.
    pub async fn resolve(&self, full_key: &str) -> ResolvedSettings {
        // Count every resolution call. The resolve-once-per-request optimization
        // (Requirement 8.2) threads a single `ResolvedSettings` through all
        // per-range cache-write tasks instead of re-resolving per range; this
        // counter lets the regression test assert that a multi-range request
        // resolves exactly once. Relaxed ordering is sufficient — the test reads
        // the count after all spawned work it cares about has completed.
        self.resolve_calls.fetch_add(1, Ordering::Relaxed);

        let needs_reload = {
            let cache = self.cache.read().await;
            match cache.as_ref() {
                Some(cached) => cached.loaded_at.elapsed() >= self.staleness_threshold,
                None => true,
            }
        };

        if needs_reload {
            self.load_rules().await;
        }

        let cache = self.cache.read().await;
        match cache.as_ref() {
            Some(cached) => cached.ruleset.resolve(full_key, &self.global_config),
            None => RuleSet::empty().resolve(full_key, &self.global_config),
        }
    }

    /// Number of times [`resolve`](Self::resolve) has been called since
    /// construction. Public test-observability hook for the resolve-once
    /// regression test (Requirement 8.2): it asserts that a multi-range request
    /// resolves settings exactly once (reusing one `ResolvedSettings` across all
    /// spawned per-range cache-write tasks) rather than once per range. Reached
    /// in the test through `CacheManager::get_bucket_settings_manager`.
    pub fn resolve_call_count(&self) -> usize {
        self.resolve_calls.load(Ordering::Relaxed)
    }

    /// Return the parsed rules (for the dashboard). Triggers a lazy load if needed.
    pub async fn rules(&self) -> Vec<CacheRule> {
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref() {
                if cached.loaded_at.elapsed() < self.staleness_threshold {
                    return cached.rules.clone();
                }
            }
        }
        self.load_rules().await;
        let cache = self.cache.read().await;
        cache.as_ref().map_or_else(Vec::new, |c| c.rules.clone())
    }

    /// Whether a `cache_rules.json` file exists on disk (triggers a lazy load).
    pub async fn has_rules_file(&self) -> bool {
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref() {
                if cached.loaded_at.elapsed() < self.staleness_threshold {
                    return cached.has_rules_file;
                }
            }
        }
        self.load_rules().await;
        let cache = self.cache.read().await;
        cache.as_ref().is_some_and(|c| c.has_rules_file)
    }

    /// Return a snapshot of cache-rules reload health counters.
    /// Cheap (all atomics, no async lock).
    pub fn rules_health(&self) -> crate::metrics::CacheRulesMetrics {
        crate::metrics::CacheRulesMetrics {
            reloads_total: self.reloads_total.load(Ordering::Relaxed),
            reload_failures_total: self.reload_failures_total.load(Ordering::Relaxed),
            on_fallback: self.on_fallback.load(Ordering::Relaxed),
            rules_loaded: self.rules_loaded.load(Ordering::Relaxed) as u64,
            last_load_unix: self.last_load_unix.load(Ordering::Relaxed),
        }
    }

    /// Single-flight load of the rules file from disk.
    async fn load_rules(&self) {
        let _guard = self.pending_load.lock().await;

        // Re-check — another task may have loaded while we waited.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref() {
                if cached.loaded_at.elapsed() < self.staleness_threshold {
                    return;
                }
            }
        }

        let path = self.rules_path();
        match tokio::fs::read_to_string(&path).await {
            Ok(contents) => match serde_json::from_str::<CacheRules>(&contents) {
                Ok(parsed) => {
                    let errors = parsed.validate(
                        self.max_rules,
                        self.global_config.upstream_first_byte_timeout,
                    );
                    if errors.is_empty() {
                        match RuleSet::build(parsed.rules.clone()) {
                            Ok(ruleset) => {
                                info!(
                                    rule_count = parsed.rules.len(),
                                    "Cache rules loaded from disk"
                                );
                                self.store_valid(ruleset, parsed.rules, true).await;
                            }
                            Err(e) => {
                                warn!(error = %e, "Cache rules failed to compile, using fallback");
                                self.use_fallback(true).await;
                            }
                        }
                    } else {
                        warn!(errors = ?errors, "Cache rules validation failed, using fallback");
                        self.use_fallback(true).await;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to parse cache_rules.json, using fallback");
                    self.use_fallback(true).await;
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No rules file → empty rule set (global defaults for every key).
                self.store_valid(RuleSet::empty(), Vec::new(), false).await;
            }
            Err(e) => {
                warn!(error = %e, "Failed to read cache_rules.json, using fallback");
                self.use_fallback(false).await;
            }
        }
    }

    /// Store a freshly loaded valid rule set, preserving the prior one as fallback.
    async fn store_valid(&self, ruleset: RuleSet, rules: Vec<CacheRule>, has_file: bool) {
        let rule_count = rules.len();
        let mut cache = self.cache.write().await;
        let previous_valid = cache.as_ref().and_then(|c| {
            if !c.rules.is_empty() {
                Some((c.ruleset.clone(), c.rules.clone()))
            } else {
                c.previous_valid.clone()
            }
        });
        *cache = Some(CachedRules {
            ruleset,
            loaded_at: Instant::now(),
            rules,
            previous_valid,
            has_rules_file: has_file,
        });
        // Update health counters.
        self.reloads_total.fetch_add(1, Ordering::Relaxed);
        self.on_fallback.store(false, Ordering::Relaxed);
        self.rules_loaded.store(rule_count, Ordering::Relaxed);
        self.last_load_unix.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    /// On error, keep the last-known-good rule set (or empty if none), refreshing
    /// the timestamp so we don't hot-loop on a broken file.
    async fn use_fallback(&self, has_file: bool) {
        self.reload_failures_total.fetch_add(1, Ordering::Relaxed);
        self.on_fallback.store(true, Ordering::Relaxed);
        let mut cache = self.cache.write().await;
        let (ruleset, rules, previous_valid) = match cache.as_ref() {
            Some(c) if !c.rules.is_empty() => {
                (c.ruleset.clone(), c.rules.clone(), c.previous_valid.clone())
            }
            Some(c) => match &c.previous_valid {
                Some((rs, rules)) => (rs.clone(), rules.clone(), None),
                None => (RuleSet::empty(), Vec::new(), None),
            },
            None => (RuleSet::empty(), Vec::new(), None),
        };
        *cache = Some(CachedRules {
            ruleset,
            loaded_at: Instant::now(),
            rules,
            previous_valid,
            has_rules_file: has_file,
        });
    }
}

/// Scan `{cache_dir}/metadata/` for legacy per-bucket `_settings.json` files.
///
/// These historically lived at `cache_dir/metadata/{bucket}/_settings.json`. The
/// per-bucket settings mechanism has been removed (Requirement 7.1): such files
/// are NO LONGER read or honoured. This scan is purely informational — it never
/// reads, parses, or loads the files, and a scan failure is logged at debug and
/// treated as "none found" so it can never crash or block startup.
///
/// The scan is intentionally bounded: it checks only the immediate child
/// directories of `metadata/` (the exact historical layout), so its cost scales
/// with the number of buckets, not the full sharded metadata tree.
fn find_legacy_settings_files(cache_dir: &Path) -> Vec<PathBuf> {
    let metadata_dir = cache_dir.join("metadata");
    let entries = match std::fs::read_dir(&metadata_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // No metadata directory → nothing to warn about (common case).
            return Vec::new();
        }
        Err(e) => {
            debug!(
                path = %metadata_dir.display(),
                error = %e,
                "Could not scan metadata directory for legacy _settings.json files"
            );
            return Vec::new();
        }
    };

    let mut found = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let candidate = path.join("_settings.json");
            if candidate.is_file() {
                found.push(candidate);
            }
        }
    }
    found
}

/// Log a single aggregate warning at startup if any legacy per-bucket
/// `_settings.json` files are present under `cache_dir/metadata/`.
///
/// Call this exactly once during startup. The files are NOT read or honoured;
/// the warning only points operators at the migration note. Requirements 7.1, 7.4.
pub fn warn_if_legacy_settings_present(cache_dir: &Path) {
    let found = find_legacy_settings_files(cache_dir);
    if let Some(example) = found.first() {
        warn!(
            count = found.len(),
            example = %example.display(),
            "Detected {} legacy per-bucket _settings.json file(s) under {}/metadata/. \
             These are NO LONGER read or honoured — the per-bucket settings mechanism has been \
             replaced by a single cache_rules.json rules file. Migrate your settings to \
             cache_dir/cache_rules.json (see the BREAKING CHANGE entry in CHANGELOG.md and \
             docs/CACHING.md). These files can be deleted once migrated.",
            found.len(),
            cache_dir.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_global_defaults() -> GlobalDefaults {
        GlobalDefaults {
            get_ttl: Duration::from_secs(300),
            head_ttl: Duration::from_secs(60),
            put_ttl: Duration::from_secs(3600),
            read_cache_enabled: true,
            write_cache_enabled: true,
            compression_enabled: true,
            ram_cache_enabled: true,
            evaluate_conditions_from_cache: true,
            upstream_first_byte_timeout: Duration::from_secs(5),
        }
    }

    fn test_manager(cache_dir: &std::path::Path) -> BucketSettingsManager {
        BucketSettingsManager::new(
            cache_dir.to_path_buf(),
            test_global_defaults(),
            Duration::from_secs(60),
        )
    }

    fn test_manager_always_reload(cache_dir: &std::path::Path) -> BucketSettingsManager {
        BucketSettingsManager::new(
            cache_dir.to_path_buf(),
            test_global_defaults(),
            Duration::ZERO,
        )
    }

    // ---- glob_to_regex translator (Property 3, Property 4) ----

    fn matches(pattern: &str, key: &str) -> bool {
        let re = regex::Regex::new(&glob_to_regex(pattern).unwrap()).unwrap();
        re.is_match(key)
    }

    #[test]
    fn glob_single_star_stays_within_segment() {
        assert!(matches("a/*/b", "a/x/b"));
        assert!(!matches("a/*/b", "a/x/y/b")); // * does not cross /
        assert!(matches("*/b", "a/b"));
        assert!(!matches("*/b", "a/x/b"));
    }

    #[test]
    fn glob_double_star_crosses_segments() {
        assert!(matches("**/credit-cards/**", "cust1/credit-cards/card.cc"));
        assert!(matches(
            "**/credit-cards/**",
            "cust1/sub/credit-cards/deep/card.cc"
        ));
        assert!(matches("a/**", "a/b/c/d"));
        assert!(!matches("a/**/z", "a/b/c")); // must end in z
        assert!(matches("a/**/z", "a/b/c/z"));
    }

    #[test]
    fn glob_question_matches_one_non_slash() {
        assert!(matches("a/?", "a/x"));
        assert!(!matches("a/?", "a/")); // needs exactly one char
        assert!(!matches("a/?", "a/xy"));
        assert!(!matches("a?b", "a/b")); // ? does not match /
    }

    #[test]
    fn glob_anchored_whole_string() {
        assert!(matches("bucket/temp", "bucket/temp"));
        assert!(!matches("bucket/temp", "bucket/temp/x")); // anchored, not prefix
        assert!(matches("bucket/temp/**", "bucket/temp/x"));
    }

    #[test]
    fn glob_escapes_metacharacters() {
        // A dot in a bucket name must match a literal dot only.
        assert!(matches("my.logs/**", "my.logs/x"));
        assert!(!matches("my.logs/**", "myXlogs/x"));
        // Other metacharacters are literals.
        assert!(matches("a+b/(c)/**", "a+b/(c)/file"));
        assert!(!matches("a+b/(c)/**", "aab/c/file"));
    }

    #[test]
    fn glob_case_sensitive() {
        assert!(matches("Bucket/**", "Bucket/x"));
        assert!(!matches("Bucket/**", "bucket/x"));
    }

    #[test]
    fn glob_empty_pattern_rejected() {
        assert!(glob_to_regex("").is_err());
        assert!(glob_to_regex("   ").is_err());
    }

    // ---- validation (Requirement 5) ----

    #[test]
    fn validate_empty_pattern_is_error() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "".to_string(),
                ..Default::default()
            }],
        };
        assert!(!rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    #[test]
    fn validate_rule_cap_enforced() {
        let rules = CacheRules {
            schema: None,
            rules: vec![
                CacheRule {
                    pattern: "**".to_string(),
                    ..Default::default()
                };
                5
            ],
        };
        assert!(rules
            .validate(4, Duration::from_secs(5))
            .iter()
            .any(|e| e.contains("exceeds maximum")));
        assert!(rules.validate(5, Duration::from_secs(5)).is_empty());
    }

    // ---- resolution (Property 1, 2, 5) ----

    #[tokio::test]
    async fn resolve_no_file_returns_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("my-bucket/some/key").await;
        assert_eq!(r.get_ttl, Duration::from_secs(300));
        assert_eq!(r.head_ttl, Duration::from_secs(60));
        assert_eq!(r.put_ttl, Duration::from_secs(3600));
        assert!(r.read_cache_enabled);
        assert!(r.write_cache_enabled);
        assert!(r.compression_enabled);
        assert!(r.ram_cache_eligible);
        assert!(matches!(r.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn resolve_empty_rules_array_is_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("cache_rules.json"), r#"{"rules": []}"#).unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("my-bucket/key").await;
        assert_eq!(r.get_ttl, Duration::from_secs(300));
        assert!(matches!(r.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn resolve_literal_bucket_prefix_rule() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "my-bucket/temp/**", "get_ttl": "0s"}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        let r = mgr.resolve("my-bucket/temp/file.txt").await;
        assert_eq!(r.get_ttl, Duration::ZERO);
        assert!(matches!(r.source, SettingsSource::Rule(0, _)));

        // Different bucket → no match → global.
        let r = mgr.resolve("other-bucket/temp/file.txt").await;
        assert_eq!(r.get_ttl, Duration::from_secs(300));
        assert!(matches!(r.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn resolve_global_middle_segment_rule() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**/credit-cards/**", "read_cache_enabled": false}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        // Matches across any bucket and any depth.
        let r = mgr.resolve("cust1/credit-cards/card.cc").await;
        assert!(!r.read_cache_enabled);
        let r = mgr.resolve("cust2/sub/credit-cards/deep/card.cc").await;
        assert!(!r.read_cache_enabled);
        // No middle segment → global.
        let r = mgr.resolve("cust1/other/file").await;
        assert!(r.read_cache_enabled);
    }

    #[tokio::test]
    async fn resolve_first_match_per_field() {
        let tmp = tempfile::tempdir().unwrap();
        // Rule 0 (specific) sets only get_ttl; rule 1 (broad) sets compression + get_ttl.
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [
                {"pattern": "b/special/**", "get_ttl": "1s"},
                {"pattern": "**", "get_ttl": "9s", "compression_enabled": false}
            ]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        let r = mgr.resolve("b/special/x").await;
        // get_ttl from rule 0 (earlier wins), compression from rule 1 (rule 0 didn't set it).
        assert_eq!(r.get_ttl, Duration::from_secs(1));
        assert!(!r.compression_enabled);
        assert!(r.compression_from_rule); // rule 1 explicitly set it
                                          // source is the first matching rule.
        assert!(matches!(r.source, SettingsSource::Rule(0, _)));

        // A key only matching the broad rule.
        let r = mgr.resolve("b/other/y").await;
        assert_eq!(r.get_ttl, Duration::from_secs(9));
        assert!(!r.compression_enabled);
        assert!(r.compression_from_rule);
        assert!(matches!(r.source, SettingsSource::Rule(1, _)));
    }

    #[tokio::test]
    async fn resolve_compression_provenance_fallthrough_is_global() {
        let tmp = tempfile::tempdir().unwrap();
        // A rule matches the key but does not set compression_enabled, so
        // resolution falls through to the global default — provenance must
        // reflect "not from a rule" even though a rule did match.
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**", "get_ttl": "9s"}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        let r = mgr.resolve("b/key").await;
        assert!(matches!(r.source, SettingsSource::Rule(0, _)));
        assert!(!r.compression_from_rule);
        // Falls through to the test global default (compression_enabled: true).
        assert!(r.compression_enabled);
    }

    #[tokio::test]
    async fn resolve_compression_provenance_explicit_true_overrides_denylist_intent() {
        let tmp = tempfile::tempdir().unwrap();
        // An explicit true is provenance-tracked even when it matches what
        // the global default already was, so callers can distinguish
        // "operator explicitly forced this on" from "just the default".
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**", "compression_enabled": true}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        let r = mgr.resolve("b/key").await;
        assert!(r.compression_enabled);
        assert!(r.compression_from_rule);
    }

    #[tokio::test]
    async fn resolve_zero_ttl_forces_ram_ineligible() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**", "get_ttl": "0s", "ram_cache_eligible": true}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("b/key").await;
        assert_eq!(r.get_ttl, Duration::ZERO);
        assert!(!r.ram_cache_eligible);
    }

    #[tokio::test]
    async fn resolve_read_disabled_forces_ram_ineligible() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**", "read_cache_enabled": false, "ram_cache_eligible": true}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("b/key").await;
        assert!(!r.read_cache_enabled);
        assert!(!r.ram_cache_eligible);
    }

    // ---- reload / resilience (Requirement 4) ----

    #[tokio::test]
    async fn reload_invalid_json_keeps_last_known_good() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        std::fs::write(&path, r#"{"rules": [{"pattern": "**", "get_ttl": "10s"}]}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let r = mgr.resolve("b/key").await;
        assert_eq!(r.get_ttl, Duration::from_secs(10));

        std::fs::write(&path, r#"{"rules": BROKEN"#).unwrap();
        let r = mgr.resolve("b/key").await;
        assert_eq!(r.get_ttl, Duration::from_secs(10)); // kept
    }

    #[tokio::test]
    async fn reload_cap_exceeded_keeps_last_known_good() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        std::fs::write(&path, r#"{"rules": [{"pattern": "**", "get_ttl": "10s"}]}"#).unwrap();
        let mut mgr = test_manager_always_reload(tmp.path());
        mgr.max_rules = 1;
        let r = mgr.resolve("b/key").await;
        assert_eq!(r.get_ttl, Duration::from_secs(10));

        std::fs::write(
            &path,
            r#"{"rules": [{"pattern": "a/**"}, {"pattern": "b/**"}]}"#,
        )
        .unwrap();
        let r = mgr.resolve("b/key").await;
        // Over cap → keep previous valid (get_ttl 10s).
        assert_eq!(r.get_ttl, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn reload_invalid_at_first_start_uses_global() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("cache_rules.json"), r#"NOT JSON"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let r = mgr.resolve("b/key").await;
        assert_eq!(r.get_ttl, Duration::from_secs(300));
        assert!(matches!(r.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn reload_valid_after_invalid_picks_up_new() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        std::fs::write(&path, r#"{"rules": [{"pattern": "**", "get_ttl": "10s"}]}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        assert_eq!(mgr.resolve("b/k").await.get_ttl, Duration::from_secs(10));

        std::fs::write(&path, r#"BROKEN"#).unwrap();
        assert_eq!(mgr.resolve("b/k").await.get_ttl, Duration::from_secs(10));

        std::fs::write(&path, r#"{"rules": [{"pattern": "**", "get_ttl": "99s"}]}"#).unwrap();
        assert_eq!(mgr.resolve("b/k").await.get_ttl, Duration::from_secs(99));
    }

    #[tokio::test]
    async fn extract_bucket_variants() {
        assert_eq!(
            BucketSettingsManager::extract_bucket("/my-bucket/some/key"),
            Some("my-bucket")
        );
        assert_eq!(
            BucketSettingsManager::extract_bucket("my-bucket/key"),
            Some("my-bucket")
        );
        assert_eq!(
            BucketSettingsManager::extract_bucket("my-bucket"),
            Some("my-bucket")
        );
        assert_eq!(BucketSettingsManager::extract_bucket("/"), None);
        assert_eq!(BucketSettingsManager::extract_bucket(""), None);
    }

    #[tokio::test]
    async fn rules_accessor_returns_parsed_rules() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "a/**", "get_ttl": "5s"}, {"pattern": "**"}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let rules = mgr.rules().await;
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].pattern, "a/**");
        assert!(mgr.has_rules_file().await);
    }

    // ---- legacy _settings.json startup-warning detection (Requirement 7.4) ----

    #[test]
    fn legacy_scan_detects_planted_settings_file() {
        let tmp = tempfile::tempdir().unwrap();
        // Plant cache_dir/metadata/{bucket}/_settings.json for two buckets.
        let meta = tmp.path().join("metadata");
        for bucket in ["bucket-a", "bucket-b"] {
            let bucket_dir = meta.join(bucket);
            std::fs::create_dir_all(&bucket_dir).unwrap();
            std::fs::write(bucket_dir.join("_settings.json"), r#"{"get_ttl":"0s"}"#).unwrap();
        }
        let found = find_legacy_settings_files(tmp.path());
        assert_eq!(found.len(), 2);
        assert!(found
            .iter()
            .all(|p| p.file_name().unwrap() == "_settings.json"));
    }

    #[test]
    fn legacy_scan_quiet_when_none_present() {
        let tmp = tempfile::tempdir().unwrap();
        // metadata/ exists with a bucket dir, but no _settings.json inside it.
        let bucket_dir = tmp.path().join("metadata").join("bucket-a");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(bucket_dir.join("something.meta"), "{}").unwrap();
        assert!(find_legacy_settings_files(tmp.path()).is_empty());
    }

    #[test]
    fn legacy_scan_quiet_when_metadata_dir_absent() {
        let tmp = tempfile::tempdir().unwrap();
        // No metadata/ directory at all → no files, no error.
        assert!(find_legacy_settings_files(tmp.path()).is_empty());
        // The public warning entry point must not panic in this common case.
        warn_if_legacy_settings_present(tmp.path());
    }

    // ---- page-aligned range caching rule fields (page_widening / page_size) ----
    // Spec: page-aligned-range-cache Requirements 1.1-1.5, 7.9

    #[test]
    fn rule_parses_without_page_fields() {
        let json = r#"{"pattern": "**"}"#;
        let rule: CacheRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.page_widening, None);
        assert_eq!(rule.page_size, None);
    }

    #[test]
    fn rule_parses_with_page_fields() {
        let json = r#"{"pattern": "*.parquet", "page_widening": true, "page_size": 33554432}"#;
        let rule: CacheRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.page_widening, Some(true));
        assert_eq!(rule.page_size, Some(33554432));
    }

    #[tokio::test]
    async fn resolve_no_rule_page_widening_defaults_off() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("my-bucket/some/key").await;
        assert!(!r.page_widening);
        assert_eq!(r.page_size, DEFAULT_PAGE_SIZE);
    }

    #[tokio::test]
    async fn resolve_rule_enables_page_widening_with_default_size() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**/*.parquet", "page_widening": true}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("bucket/file.parquet").await;
        assert!(r.page_widening);
        assert_eq!(r.page_size, DEFAULT_PAGE_SIZE);
    }

    #[tokio::test]
    async fn resolve_rule_enables_page_widening_with_explicit_size() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**/*.orc", "page_widening": true, "page_size": 8388608}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("bucket/file.orc").await;
        assert!(r.page_widening);
        assert_eq!(r.page_size, 8388608);
    }

    #[tokio::test]
    async fn resolve_page_widening_first_match_per_field_precedence() {
        let tmp = tempfile::tempdir().unwrap();
        // Rule 0 (specific) sets only page_widening; rule 1 (broad) sets page_size.
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [
                {"pattern": "b/special/**", "page_widening": true},
                {"pattern": "**", "page_size": 4194304}
            ]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        let r = mgr.resolve("b/special/x").await;
        // page_widening from rule 0 (earlier wins); page_size from rule 1
        // (rule 0 didn't set it, falls through to next matched rule).
        assert!(r.page_widening);
        assert_eq!(r.page_size, 4194304);

        // A key only matching the broad rule: page_widening stays off (no
        // rule enabled it), page_size still comes from rule 1.
        let r = mgr.resolve("b/other/y").await;
        assert!(!r.page_widening);
        assert_eq!(r.page_size, 4194304);
    }

    #[tokio::test]
    async fn resolve_page_widening_hot_reload_picks_up_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        std::fs::write(
            &path,
            r#"{"rules": [{"pattern": "**", "page_widening": false}]}"#,
        )
        .unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        assert!(!mgr.resolve("b/k").await.page_widening);

        std::fs::write(
            &path,
            r#"{"rules": [{"pattern": "**", "page_widening": true, "page_size": 1048576}]}"#,
        )
        .unwrap();
        let r = mgr.resolve("b/k").await;
        assert!(r.page_widening);
        assert_eq!(r.page_size, 1048576);
    }

    #[test]
    fn validate_rejects_page_size_zero_when_widening_enabled() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "*.parquet".to_string(),
                page_widening: Some(true),
                page_size: Some(0),
                ..Default::default()
            }],
        };
        let errors = rules.validate(DEFAULT_MAX_RULES, Duration::from_secs(5));
        assert!(errors
            .iter()
            .any(|e| e.contains("page_size must be greater than 0")));
    }

    #[test]
    fn validate_rejects_page_size_over_64mib_when_widening_enabled() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "*.parquet".to_string(),
                page_widening: Some(true),
                page_size: Some(64 * 1024 * 1024 + 1),
                ..Default::default()
            }],
        };
        let errors = rules.validate(DEFAULT_MAX_RULES, Duration::from_secs(5));
        assert!(errors.iter().any(|e| e.contains("exceeds the maximum")));
    }

    #[test]
    fn validate_accepts_page_size_at_64mib_boundary() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "*.parquet".to_string(),
                page_widening: Some(true),
                page_size: Some(64 * 1024 * 1024),
                ..Default::default()
            }],
        };
        assert!(rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    #[test]
    fn validate_accepts_widening_enabled_without_explicit_page_size() {
        // No page_size set → falls back to DEFAULT_PAGE_SIZE at resolution
        // time, which is always valid; validation must not reject this.
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "*.parquet".to_string(),
                page_widening: Some(true),
                page_size: None,
                ..Default::default()
            }],
        };
        assert!(rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    #[test]
    fn validate_ignores_page_size_when_widening_not_enabled() {
        // page_widening false/absent: an out-of-range page_size is harmless
        // (never used), so validation should not reject it.
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "*.parquet".to_string(),
                page_widening: Some(false),
                page_size: Some(0),
                ..Default::default()
            }],
        };
        assert!(rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    // ---- hedged upstream request rule fields (hedging_enabled / hedge_trigger_after / hedge_max_per_request) ----
    // Spec: hedged-upstream-requests Requirements 1.1-1.5, 3.1, 6.1, 9.2

    #[test]
    fn rule_parses_without_hedging_fields() {
        let json = r#"{"pattern": "**"}"#;
        let rule: CacheRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.hedging_enabled, None);
        assert_eq!(rule.hedge_trigger_after, None);
        assert_eq!(rule.hedge_max_per_request, None);
    }

    #[test]
    fn rule_parses_with_hedging_fields() {
        let json = r#"{"pattern": "a/**", "hedging_enabled": true, "hedge_trigger_after": "250ms", "hedge_max_per_request": 2}"#;
        let rule: CacheRule = serde_json::from_str(json).unwrap();
        assert_eq!(rule.hedging_enabled, Some(true));
        assert_eq!(rule.hedge_trigger_after, Some(Duration::from_millis(250)));
        assert_eq!(rule.hedge_max_per_request, Some(2));
    }

    #[tokio::test]
    async fn resolve_hedging_enabled_true_for_matching_key() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "hot/**", "hedging_enabled": true}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("hot/key.parquet").await;
        assert!(r.hedging_enabled);
        assert_eq!(r.hedge_trigger_after, DEFAULT_HEDGE_TRIGGER_AFTER);
        assert_eq!(r.hedge_max_per_request, DEFAULT_HEDGE_MAX_PER_REQUEST);
    }

    #[tokio::test]
    async fn resolve_hedging_enabled_false_for_non_matching_key() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "hot/**", "hedging_enabled": true}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("cold/key.txt").await;
        assert!(!r.hedging_enabled);
    }

    #[tokio::test]
    async fn resolve_no_hedging_fields_defaults_to_false_250ms_1() {
        // Backward compatibility: omitting all three fields resolves to (false, 250ms, 1).
        let tmp = tempfile::tempdir().unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("any-bucket/any-key").await;
        assert!(!r.hedging_enabled);
        assert_eq!(r.hedge_trigger_after, Duration::from_millis(250));
        assert_eq!(r.hedge_max_per_request, 1);
    }

    #[tokio::test]
    async fn resolve_hedging_explicit_overrides_win_over_constants() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "**", "hedging_enabled": true, "hedge_trigger_after": "500ms", "hedge_max_per_request": 3}]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());
        let r = mgr.resolve("b/k").await;
        assert!(r.hedging_enabled);
        assert_eq!(r.hedge_trigger_after, Duration::from_millis(500));
        assert_eq!(r.hedge_max_per_request, 3);
    }

    #[tokio::test]
    async fn resolve_hedging_first_match_per_field_ordering() {
        let tmp = tempfile::tempdir().unwrap();
        // Rule 0 sets hedging_enabled only; rule 1 sets hedge_trigger_after.
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [
                {"pattern": "b/special/**", "hedging_enabled": true},
                {"pattern": "**", "hedge_trigger_after": "1s", "hedge_max_per_request": 5}
            ]}"#,
        )
        .unwrap();
        let mgr = test_manager(tmp.path());

        let r = mgr.resolve("b/special/x").await;
        // hedging_enabled from rule 0; trigger + budget from rule 1 (rule 0 didn't set them).
        assert!(r.hedging_enabled);
        assert_eq!(r.hedge_trigger_after, Duration::from_secs(1));
        assert_eq!(r.hedge_max_per_request, 5);
    }

    #[test]
    fn validate_rejects_hedge_trigger_zero_when_hedging_enabled() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "**".to_string(),
                hedging_enabled: Some(true),
                hedge_trigger_after: Some(Duration::ZERO),
                ..Default::default()
            }],
        };
        let errors = rules.validate(DEFAULT_MAX_RULES, Duration::from_secs(5));
        assert!(errors
            .iter()
            .any(|e| e.contains("hedge_trigger_after must be greater than 0")));
    }

    #[test]
    fn validate_rejects_hedge_trigger_at_first_byte_timeout() {
        // 6s trigger against 5s first-byte timeout.
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "**".to_string(),
                hedging_enabled: Some(true),
                hedge_trigger_after: Some(Duration::from_secs(6)),
                ..Default::default()
            }],
        };
        let errors = rules.validate(DEFAULT_MAX_RULES, Duration::from_secs(5));
        assert!(errors
            .iter()
            .any(|e| e.contains("must be strictly less than upstream_first_byte_timeout")));
    }

    #[test]
    fn validate_rejects_hedge_trigger_equal_to_first_byte_timeout() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "**".to_string(),
                hedging_enabled: Some(true),
                hedge_trigger_after: Some(Duration::from_secs(5)),
                ..Default::default()
            }],
        };
        let errors = rules.validate(DEFAULT_MAX_RULES, Duration::from_secs(5));
        assert!(!errors.is_empty());
    }

    #[test]
    fn validate_accepts_hedge_trigger_250ms() {
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "**".to_string(),
                hedging_enabled: Some(true),
                hedge_trigger_after: Some(Duration::from_millis(250)),
                ..Default::default()
            }],
        };
        assert!(rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    #[test]
    fn validate_ignores_out_of_range_trigger_when_hedging_not_enabled() {
        // hedging_enabled is false/absent: an out-of-range hedge_trigger_after
        // is harmless (never used), so validation should not reject it.
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "**".to_string(),
                hedging_enabled: Some(false),
                hedge_trigger_after: Some(Duration::from_secs(6)),
                ..Default::default()
            }],
        };
        assert!(rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    #[test]
    fn validate_accepts_hedging_enabled_without_explicit_trigger() {
        // No hedge_trigger_after set → falls back to DEFAULT_HEDGE_TRIGGER_AFTER
        // at resolution time, which is always valid; validation must not reject.
        let rules = CacheRules {
            schema: None,
            rules: vec![CacheRule {
                pattern: "**".to_string(),
                hedging_enabled: Some(true),
                hedge_trigger_after: None,
                ..Default::default()
            }],
        };
        assert!(rules
            .validate(DEFAULT_MAX_RULES, Duration::from_secs(5))
            .is_empty());
    }

    #[tokio::test]
    async fn reload_invalid_hedging_rule_keeps_previous_valid() {
        // The T21 pattern: an invalid live edit is rejected with the previous
        // rules retained.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        std::fs::write(
            &path,
            r#"{"rules": [{"pattern": "**", "hedging_enabled": true, "hedge_trigger_after": "250ms"}]}"#,
        )
        .unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let r = mgr.resolve("b/k").await;
        assert!(r.hedging_enabled);
        assert_eq!(r.hedge_trigger_after, Duration::from_millis(250));

        // Write an invalid rule (trigger >= first-byte timeout).
        std::fs::write(
            &path,
            r#"{"rules": [{"pattern": "**", "hedging_enabled": true, "hedge_trigger_after": "6s"}]}"#,
        )
        .unwrap();
        let r = mgr.resolve("b/k").await;
        // Previous valid retained.
        assert!(r.hedging_enabled);
        assert_eq!(r.hedge_trigger_after, Duration::from_millis(250));
    }

    // ---- rules_health() reload counters ----

    #[tokio::test]
    async fn rules_health_counters_after_successful_load() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("cache_rules.json"),
            r#"{"rules": [{"pattern": "a/**", "get_ttl": "5s"}, {"pattern": "**"}]}"#,
        )
        .unwrap();
        let mgr = test_manager_always_reload(tmp.path());

        // Trigger a load.
        let _ = mgr.resolve("a/key").await;

        let health = mgr.rules_health();
        assert_eq!(health.reloads_total, 1);
        assert_eq!(health.reload_failures_total, 0);
        assert!(!health.on_fallback);
        assert_eq!(health.rules_loaded, 2);
        assert!(health.last_load_unix > 0);
    }

    #[tokio::test]
    async fn rules_health_counters_after_failed_load() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        // First load succeeds.
        std::fs::write(&path, r#"{"rules": [{"pattern": "**", "get_ttl": "10s"}]}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let _ = mgr.resolve("b/k").await;

        let health = mgr.rules_health();
        assert_eq!(health.reloads_total, 1);
        assert_eq!(health.reload_failures_total, 0);
        assert!(!health.on_fallback);

        // Break the file — next load fails, falls back.
        std::fs::write(&path, r#"NOT JSON"#).unwrap();
        let _ = mgr.resolve("b/k").await;

        let health = mgr.rules_health();
        assert_eq!(health.reloads_total, 1); // no new success
        assert_eq!(health.reload_failures_total, 1);
        assert!(health.on_fallback);
        // Still serving the 1 rule from last-known-good.
        assert_eq!(health.rules_loaded, 1);
    }

    #[tokio::test]
    async fn rules_health_recovery_clears_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache_rules.json");
        std::fs::write(&path, r#"{"rules": [{"pattern": "**", "get_ttl": "10s"}]}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let _ = mgr.resolve("b/k").await;

        // Break it.
        std::fs::write(&path, r#"BROKEN"#).unwrap();
        let _ = mgr.resolve("b/k").await;
        assert!(mgr.rules_health().on_fallback);

        // Fix it.
        std::fs::write(
            &path,
            r#"{"rules": [{"pattern": "x/**"}, {"pattern": "**"}]}"#,
        )
        .unwrap();
        let _ = mgr.resolve("b/k").await;

        let health = mgr.rules_health();
        assert_eq!(health.reloads_total, 2);
        assert_eq!(health.reload_failures_total, 1);
        assert!(!health.on_fallback);
        assert_eq!(health.rules_loaded, 2);
    }
}
