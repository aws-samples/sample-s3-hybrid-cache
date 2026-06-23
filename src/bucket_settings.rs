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
    /// Checks: non-empty patterns, compilable globs, and the rule-count cap.
    pub fn validate(&self, max_rules: usize) -> Vec<String> {
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
    pub ram_cache_eligible: bool,
    pub evaluate_conditions_from_cache: bool,
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
            ram_cache_eligible: true,
            evaluate_conditions_from_cache: true,
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

        let get_ttl = first_dur(&|r| r.get_ttl).unwrap_or(g.get_ttl);
        let head_ttl = first_dur(&|r| r.head_ttl).unwrap_or(g.head_ttl);
        let put_ttl = first_dur(&|r| r.put_ttl).unwrap_or(g.put_ttl);
        let read_cache_enabled = first(&|r| r.read_cache_enabled).unwrap_or(g.read_cache_enabled);
        let write_cache_enabled =
            first(&|r| r.write_cache_enabled).unwrap_or(g.write_cache_enabled);
        let compression_enabled =
            first(&|r| r.compression_enabled).unwrap_or(g.compression_enabled);
        let mut ram_cache_eligible =
            first(&|r| r.ram_cache_eligible).unwrap_or(g.ram_cache_enabled);
        let evaluate_conditions_from_cache = first(&|r| r.evaluate_conditions_from_cache)
            .unwrap_or(g.evaluate_conditions_from_cache);

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
            ram_cache_eligible,
            evaluate_conditions_from_cache,
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
                    let errors = parsed.validate(self.max_rules);
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
        assert!(!rules.validate(DEFAULT_MAX_RULES).is_empty());
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
            .validate(4)
            .iter()
            .any(|e| e.contains("exceeds maximum")));
        assert!(rules.validate(5).is_empty());
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
        // source is the first matching rule.
        assert!(matches!(r.source, SettingsSource::Rule(0, _)));

        // A key only matching the broad rule.
        let r = mgr.resolve("b/other/y").await;
        assert_eq!(r.get_ttl, Duration::from_secs(9));
        assert!(!r.compression_enabled);
        assert!(matches!(r.source, SettingsSource::Rule(1, _)));
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
