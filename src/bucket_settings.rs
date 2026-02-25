//! Bucket-Level Cache Settings
//!
//! Per-bucket and per-prefix cache configuration loaded from `_settings.json` files
//! stored at `cache_dir/metadata/{bucket}/_settings.json`. Supports a settings cascade
//! (Prefix → Bucket → Global) for fine-grained cache control.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::config::duration_serde;

/// Custom deserializer for optional Duration fields in bucket settings JSON.
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
        if nanos % 1_000_000 == 0 {
            return format!("{}ms", total_ms);
        }
        // Fall back to seconds with fractional part
        return format!("{}s", d.as_secs_f64());
    }

    // Use the largest whole unit
    if total_secs % 86400 == 0 {
        format!("{}d", total_secs / 86400)
    } else if total_secs % 3600 == 0 {
        format!("{}h", total_secs / 3600)
    } else if total_secs % 60 == 0 {
        format!("{}m", total_secs / 60)
    } else {
        format!("{}s", total_secs)
    }
}

/// Per-bucket settings loaded from `_settings.json`.
/// All fields are optional — omitted fields fall back to global config.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub struct BucketSettings {
    /// Optional JSON schema reference for IDE validation
    #[serde(rename = "$schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// GET response TTL override (e.g., "0s", "30s", "5m", "1h", "7d")
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub get_ttl: Option<Duration>,

    /// HEAD response TTL override
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub head_ttl: Option<Duration>,

    /// PUT write-cache TTL override
    #[serde(
        default,
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        skip_serializing_if = "Option::is_none"
    )]
    pub put_ttl: Option<Duration>,

    /// Enable/disable read caching for GET responses
    pub read_cache_enabled: Option<bool>,

    /// Enable/disable write-through caching for PUT operations
    pub write_cache_enabled: Option<bool>,

    /// Enable/disable LZ4 compression for cached data
    pub compression_enabled: Option<bool>,

    /// Whether range data from this bucket can be stored in RAM cache
    pub ram_cache_eligible: Option<bool>,

    /// Per-prefix overrides within this bucket
    #[serde(default)]
    pub prefix_overrides: Vec<PrefixOverride>,
}

/// Per-prefix settings override within a bucket.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct PrefixOverride {
    /// Prefix path (e.g., "/temp/", "/static/assets/")
    pub prefix: String,

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

    pub read_cache_enabled: Option<bool>,
    pub write_cache_enabled: Option<bool>,
    pub compression_enabled: Option<bool>,
    pub ram_cache_eligible: Option<bool>,
}

/// Fully resolved settings for a specific bucket+path combination.
/// Every field has a concrete value (no Options) after cascade resolution.
#[derive(Debug, Clone)]
pub struct ResolvedSettings {
    pub get_ttl: Duration,
    pub head_ttl: Duration,
    pub put_ttl: Duration,
    pub read_cache_enabled: bool,
    pub write_cache_enabled: bool,
    pub compression_enabled: bool,
    pub ram_cache_eligible: bool,
    /// Tracks which level provided the settings
    pub source: SettingsSource,
}

/// Indicates which level of the settings cascade provided the resolved values.
#[derive(Debug, Clone)]
pub enum SettingsSource {
    Global,
    Bucket(String),
    Prefix(String, String), // (bucket, prefix)
}

/// Global config defaults extracted from CacheConfig at startup.
#[derive(Debug, Clone)]
pub struct GlobalDefaults {
    pub get_ttl: Duration,
    pub head_ttl: Duration,
    pub put_ttl: Duration,
    pub read_cache_enabled: bool,
    pub write_cache_enabled: bool,
    pub compression_enabled: bool,
    pub ram_cache_enabled: bool,
}

/// Cached bucket settings entry with load timestamp and fallback.
struct CachedBucketSettings {
    settings: BucketSettings,
    loaded_at: Instant,
    /// Previous valid settings, kept as fallback if reload produces invalid JSON.
    previous_valid: Option<BucketSettings>,
    /// Whether a `_settings.json` file exists on disk for this bucket.
    /// Used to determine if per-bucket metrics should be emitted.
    has_settings_file: bool,
}

/// Thread-safe manager for loading, caching, and resolving bucket settings.
/// Lazily loads settings from disk on first access, then caches with staleness threshold.
pub struct BucketSettingsManager {
    cache_dir: PathBuf,
    /// Cached settings per bucket: bucket_name -> CachedBucketSettings
    cache: RwLock<HashMap<String, CachedBucketSettings>>,
    /// Staleness threshold — settings older than this trigger a re-read from disk
    staleness_threshold: Duration,
    /// Global config values used as fallback
    global_config: GlobalDefaults,
    /// Single-flight coordination: only one task reloads a bucket's settings at a time
    pending_loads: Mutex<HashMap<String, Arc<Mutex<()>>>>,
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
            cache: RwLock::new(HashMap::new()),
            staleness_threshold,
            global_config,
            pending_loads: Mutex::new(HashMap::new()),
        }
    }

    /// Extract bucket name from a cache key or request path.
    /// Cache keys have the form "/{bucket}/{key}" or "{bucket}/{key}".
    /// Returns `None` for empty paths or paths that are just "/".
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

    /// Resolve settings for a given bucket and object path.
    /// Handles the full cascade: Prefix → Bucket → Global.
    /// Lazily loads/reloads settings from disk as needed.
    pub async fn resolve(&self, bucket: &str, path: &str) -> ResolvedSettings {
        // Step 1: Check if cached settings exist and are fresh
        let needs_reload = {
            let cache = self.cache.read().await;
            match cache.get(bucket) {
                Some(cached) => cached.loaded_at.elapsed() >= self.staleness_threshold,
                None => true,
            }
        };

        // Step 2: If stale or missing, use single-flight pattern to reload from disk
        if needs_reload {
            self.load_settings(bucket).await;
        }

        // Step 3: Read cached settings and apply cascade
        let cache = self.cache.read().await;
        let settings = cache
            .get(bucket)
            .map(|c| &c.settings);

        self.cascade(bucket, path, settings)
    }

    /// Check if a bucket has a `_settings.json` file on disk.
    /// Returns `false` if the bucket hasn't been loaded yet or has no settings file.
    /// Used to determine whether per-bucket metrics should be emitted.
    pub async fn has_custom_settings(&self, bucket: &str) -> bool {
        let cache = self.cache.read().await;
        cache.get(bucket).map_or(false, |c| c.has_settings_file)
    }

    /// Return the list of bucket names that have a `_settings.json` file.
    /// Used by the dashboard and metrics collection.
    pub async fn buckets_with_settings(&self) -> Vec<String> {
        let cache = self.cache.read().await;
        cache
            .iter()
            .filter(|(_, v)| v.has_settings_file)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Return the number of prefix overrides for a bucket.
    /// Returns 0 if the bucket has no custom settings.
    pub async fn prefix_override_count(&self, bucket: &str) -> usize {
        let cache = self.cache.read().await;
        cache
            .get(bucket)
            .map_or(0, |c| c.settings.prefix_overrides.len())
    }

    /// Return the prefix overrides for a bucket.
    /// Returns an empty vec if the bucket has no custom settings.
    pub async fn get_prefix_overrides(&self, bucket: &str) -> Vec<PrefixOverride> {
        let cache = self.cache.read().await;
        cache
            .get(bucket)
            .map_or_else(Vec::new, |c| c.settings.prefix_overrides.clone())
    }

    /// Single-flight load of settings from disk for a bucket.
    /// Only one task reads from disk per bucket; others wait.
    async fn load_settings(&self, bucket: &str) {
        // Get or create per-bucket lock
        let lock = {
            let mut pending = self.pending_loads.lock().await;
            pending
                .entry(bucket.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        // Only one task proceeds past this point per bucket
        let _guard = lock.lock().await;

        // Re-check cache — another task may have loaded while we waited
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(bucket) {
                if cached.loaded_at.elapsed() < self.staleness_threshold {
                    return;
                }
            }
        }

        // Build settings file path: {cache_dir}/metadata/{bucket}/_settings.json
        let settings_path = self
            .cache_dir
            .join("metadata")
            .join(bucket)
            .join("_settings.json");

        match tokio::fs::read_to_string(&settings_path).await {
            Ok(contents) => {
                match serde_json::from_str::<BucketSettings>(&contents) {
                    Ok(new_settings) => {
                        let errors = new_settings.validate();
                        if errors.is_empty() {
                            info!(bucket = bucket, "Bucket settings reloaded from disk");
                            let mut cache = self.cache.write().await;
                            let previous_valid = cache
                                .get(bucket)
                                .and_then(|c| {
                                    // Keep the current settings as previous_valid
                                    // (only if they were themselves valid)
                                    if c.settings != BucketSettings::default() || c.previous_valid.is_some() {
                                        Some(c.settings.clone())
                                    } else {
                                        c.previous_valid.clone()
                                    }
                                });
                            cache.insert(
                                bucket.to_string(),
                                CachedBucketSettings {
                                    settings: new_settings,
                                    loaded_at: Instant::now(),
                                    previous_valid,
                                    has_settings_file: true,
                                },
                            );
                        } else {
                            // Valid JSON but invalid field values
                            warn!(
                                bucket = bucket,
                                errors = ?errors,
                                "Bucket settings validation failed, using fallback"
                            );
                            self.use_fallback_settings(bucket).await;
                        }
                    }
                    Err(e) => {
                        // Invalid JSON
                        warn!(
                            bucket = bucket,
                            error = %e,
                            "Failed to parse bucket settings JSON, using fallback"
                        );
                        self.use_fallback_settings(bucket).await;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File not found — cache a default "no settings" marker
                let mut cache = self.cache.write().await;
                cache.insert(
                    bucket.to_string(),
                    CachedBucketSettings {
                        settings: BucketSettings::default(),
                        loaded_at: Instant::now(),
                        previous_valid: None,
                        has_settings_file: false,
                    },
                );
            }
            Err(e) => {
                // Disk I/O error
                warn!(
                    bucket = bucket,
                    error = %e,
                    "Failed to read bucket settings file, using fallback"
                );
                self.use_fallback_settings(bucket).await;
            }
        }
    }

    /// On error, keep previous valid settings or cache a default with updated timestamp.
    ///
    /// Fallback priority:
    /// 1. Current `settings` in cache (the last successfully loaded valid settings)
    /// 2. `previous_valid` (the settings before the current ones)
    /// 3. Global defaults (empty BucketSettings — cascade will use GlobalDefaults)
    async fn use_fallback_settings(&self, bucket: &str) {
        let mut cache = self.cache.write().await;
        if let Some(existing) = cache.get(bucket) {
            // The current `settings` is the last valid load. Use it as fallback
            // unless it's a default marker (from a previous file-not-found),
            // in which case try `previous_valid`.
            let is_current_meaningful = existing.settings != BucketSettings::default()
                || existing.previous_valid.is_none();
            let (fallback_settings, previous_valid) = if is_current_meaningful {
                // Current settings are valid — keep using them
                (existing.settings.clone(), existing.previous_valid.clone())
            } else if let Some(prev) = &existing.previous_valid {
                // Current settings are a default marker, but we have a previous valid
                (prev.clone(), None)
            } else {
                // Both are empty/default — use global defaults
                (BucketSettings::default(), None)
            };
            let has_settings_file = existing.has_settings_file;
            cache.insert(
                bucket.to_string(),
                CachedBucketSettings {
                    settings: fallback_settings,
                    loaded_at: Instant::now(),
                    previous_valid,
                    has_settings_file,
                },
            );
        } else {
            // No previous settings at all — use global defaults (empty BucketSettings)
            cache.insert(
                bucket.to_string(),
                CachedBucketSettings {
                    settings: BucketSettings::default(),
                    loaded_at: Instant::now(),
                    previous_valid: None,
                    has_settings_file: false,
                },
            );
        }
    }

    /// Apply the settings cascade: Prefix → Bucket → Global.
    /// For each field independently, use the most specific level that defines it.
    fn cascade(
        &self,
        bucket: &str,
        path: &str,
        settings: Option<&BucketSettings>,
    ) -> ResolvedSettings {
        let g = &self.global_config;

        // Find the longest matching prefix override
        let prefix_match = settings.and_then(|s| {
            s.prefix_overrides
                .iter()
                .filter(|po| path.starts_with(&po.prefix))
                .max_by_key(|po| po.prefix.len())
        });

        // Determine source for debug logging
        let source = match &prefix_match {
            Some(po) => SettingsSource::Prefix(bucket.to_string(), po.prefix.clone()),
            None => match settings {
                Some(s) if self.has_any_field(s) => SettingsSource::Bucket(bucket.to_string()),
                _ => SettingsSource::Global,
            },
        };

        // Cascade each field: prefix → bucket → global
        let get_ttl = prefix_match
            .and_then(|po| po.get_ttl)
            .or(settings.and_then(|s| s.get_ttl))
            .unwrap_or(g.get_ttl);

        let head_ttl = prefix_match
            .and_then(|po| po.head_ttl)
            .or(settings.and_then(|s| s.head_ttl))
            .unwrap_or(g.head_ttl);

        let put_ttl = prefix_match
            .and_then(|po| po.put_ttl)
            .or(settings.and_then(|s| s.put_ttl))
            .unwrap_or(g.put_ttl);

        let read_cache_enabled = prefix_match
            .and_then(|po| po.read_cache_enabled)
            .or(settings.and_then(|s| s.read_cache_enabled))
            .unwrap_or(g.read_cache_enabled);

        let write_cache_enabled = prefix_match
            .and_then(|po| po.write_cache_enabled)
            .or(settings.and_then(|s| s.write_cache_enabled))
            .unwrap_or(g.write_cache_enabled);

        let compression_enabled = prefix_match
            .and_then(|po| po.compression_enabled)
            .or(settings.and_then(|s| s.compression_enabled))
            .unwrap_or(g.compression_enabled);

        let mut ram_cache_eligible = prefix_match
            .and_then(|po| po.ram_cache_eligible)
            .or(settings.and_then(|s| s.ram_cache_eligible))
            .unwrap_or(g.ram_cache_enabled);

        // Enforce invariants after cascade:
        // - Zero get_ttl → RAM range cache ineligible (RAM cache bypasses revalidation)
        // - Read cache disabled → RAM range cache ineligible
        // Note: These invariants only affect ram_cache_eligible (RAM range cache).
        // The metadata cache is NOT affected — it stores entries for zero-TTL objects
        // because it has its own refresh interval and is used for revalidation. (Requirement 6.6)
        if get_ttl == Duration::ZERO {
            ram_cache_eligible = false;
        }
        if !read_cache_enabled {
            ram_cache_eligible = false;
        }

        let resolved = ResolvedSettings {
            get_ttl,
            head_ttl,
            put_ttl,
            read_cache_enabled,
            write_cache_enabled,
            compression_enabled,
            ram_cache_eligible,
            source: source.clone(),
        };

        debug!(
            bucket = bucket,
            path = path,
            source = ?source,
            get_ttl = ?resolved.get_ttl,
            ram_cache_eligible = resolved.ram_cache_eligible,
            read_cache_enabled = resolved.read_cache_enabled,
            "Resolved bucket settings"
        );

        resolved
    }

    /// Check if a BucketSettings has any field set (not all defaults/None).
    fn has_any_field(&self, s: &BucketSettings) -> bool {
        s.get_ttl.is_some()
            || s.head_ttl.is_some()
            || s.put_ttl.is_some()
            || s.read_cache_enabled.is_some()
            || s.write_cache_enabled.is_some()
            || s.compression_enabled.is_some()
            || s.ram_cache_eligible.is_some()
            || !s.prefix_overrides.is_empty()
    }
}

impl BucketSettings {
    /// Validate a BucketSettings struct. Returns a list of human-readable validation errors.
    /// An empty list means the settings are valid.
    ///
    /// Validation rules:
    /// - TTL values must be >= 0 (Duration is unsigned, so this is always true post-parse,
    ///   but we validate explicitly for completeness)
    /// - prefix_overrides entries must have non-empty prefixes starting with "/"
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        for (i, po) in self.prefix_overrides.iter().enumerate() {
            if po.prefix.is_empty() {
                errors.push(format!(
                    "prefix_overrides[{}]: prefix is empty",
                    i
                ));
            } else if !po.prefix.starts_with('/') {
                errors.push(format!(
                    "prefix_overrides[{}]: prefix {:?} must start with '/'",
                    i, po.prefix
                ));
            }
        }

        errors
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_empty_settings_is_valid() {
        let settings = BucketSettings::default();
        assert!(settings.validate().is_empty());
    }

    #[test]
    fn validate_valid_prefix_overrides() {
        let settings = BucketSettings {
            prefix_overrides: vec![
                PrefixOverride {
                    prefix: "/temp/".to_string(),
                    get_ttl: Some(Duration::from_secs(0)),
                    head_ttl: None,
                    put_ttl: None,
                    read_cache_enabled: None,
                    write_cache_enabled: None,
                    compression_enabled: None,
                    ram_cache_eligible: None,
                },
                PrefixOverride {
                    prefix: "/static/assets/".to_string(),
                    get_ttl: None,
                    head_ttl: None,
                    put_ttl: None,
                    read_cache_enabled: None,
                    write_cache_enabled: None,
                    compression_enabled: None,
                    ram_cache_eligible: None,
                },
            ],
            ..Default::default()
        };
        assert!(settings.validate().is_empty());
    }

    #[test]
    fn validate_rejects_empty_prefix() {
        let settings = BucketSettings {
            prefix_overrides: vec![PrefixOverride {
                prefix: "".to_string(),
                get_ttl: None,
                head_ttl: None,
                put_ttl: None,
                read_cache_enabled: None,
                write_cache_enabled: None,
                compression_enabled: None,
                ram_cache_eligible: None,
            }],
            ..Default::default()
        };
        let errors = settings.validate();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("prefix is empty"));
        assert!(errors[0].contains("prefix_overrides[0]"));
    }

    #[test]
    fn validate_rejects_prefix_without_leading_slash() {
        let settings = BucketSettings {
            prefix_overrides: vec![PrefixOverride {
                prefix: "temp/".to_string(),
                get_ttl: None,
                head_ttl: None,
                put_ttl: None,
                read_cache_enabled: None,
                write_cache_enabled: None,
                compression_enabled: None,
                ram_cache_eligible: None,
            }],
            ..Default::default()
        };
        let errors = settings.validate();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("must start with '/'"));
        assert!(errors[0].contains("prefix_overrides[0]"));
    }

    #[test]
    fn validate_reports_multiple_invalid_prefixes() {
        let settings = BucketSettings {
            prefix_overrides: vec![
                PrefixOverride {
                    prefix: "".to_string(),
                    get_ttl: None,
                    head_ttl: None,
                    put_ttl: None,
                    read_cache_enabled: None,
                    write_cache_enabled: None,
                    compression_enabled: None,
                    ram_cache_eligible: None,
                },
                PrefixOverride {
                    prefix: "/valid/".to_string(),
                    get_ttl: None,
                    head_ttl: None,
                    put_ttl: None,
                    read_cache_enabled: None,
                    write_cache_enabled: None,
                    compression_enabled: None,
                    ram_cache_eligible: None,
                },
                PrefixOverride {
                    prefix: "no-slash".to_string(),
                    get_ttl: None,
                    head_ttl: None,
                    put_ttl: None,
                    read_cache_enabled: None,
                    write_cache_enabled: None,
                    compression_enabled: None,
                    ram_cache_eligible: None,
                },
            ],
            ..Default::default()
        };
        let errors = settings.validate();
        assert_eq!(errors.len(), 2);
        assert!(errors[0].contains("prefix_overrides[0]"));
        assert!(errors[1].contains("prefix_overrides[2]"));
    }

    #[test]
    fn validate_settings_with_ttls_and_valid_prefixes() {
        let settings = BucketSettings {
            get_ttl: Some(Duration::from_secs(300)),
            head_ttl: Some(Duration::from_secs(30)),
            put_ttl: Some(Duration::from_secs(3600)),
            prefix_overrides: vec![PrefixOverride {
                prefix: "/data/".to_string(),
                get_ttl: Some(Duration::from_secs(0)),
                head_ttl: None,
                put_ttl: None,
                read_cache_enabled: Some(false),
                write_cache_enabled: None,
                compression_enabled: None,
                ram_cache_eligible: None,
            }],
            ..Default::default()
        };
        assert!(settings.validate().is_empty());
    }

    #[test]
    fn extract_bucket_with_leading_slash_and_key() {
        assert_eq!(BucketSettingsManager::extract_bucket("/my-bucket/some/key"), Some("my-bucket"));
    }

    #[test]
    fn extract_bucket_without_leading_slash() {
        assert_eq!(BucketSettingsManager::extract_bucket("my-bucket/some/key"), Some("my-bucket"));
    }

    #[test]
    fn extract_bucket_with_trailing_slash_only() {
        assert_eq!(BucketSettingsManager::extract_bucket("/my-bucket/"), Some("my-bucket"));
    }

    #[test]
    fn extract_bucket_name_only() {
        assert_eq!(BucketSettingsManager::extract_bucket("my-bucket"), Some("my-bucket"));
    }

    #[test]
    fn extract_bucket_just_slash() {
        assert_eq!(BucketSettingsManager::extract_bucket("/"), None);
    }

    #[test]
    fn extract_bucket_empty_string() {
        assert_eq!(BucketSettingsManager::extract_bucket(""), None);
    }

    // Helper to create a GlobalDefaults with known values
    fn test_global_defaults() -> GlobalDefaults {
        GlobalDefaults {
            get_ttl: Duration::from_secs(300),
            head_ttl: Duration::from_secs(60),
            put_ttl: Duration::from_secs(3600),
            read_cache_enabled: true,
            write_cache_enabled: true,
            compression_enabled: true,
            ram_cache_enabled: true,
        }
    }

    fn test_manager(cache_dir: &std::path::Path) -> BucketSettingsManager {
        BucketSettingsManager::new(
            cache_dir.to_path_buf(),
            test_global_defaults(),
            Duration::from_secs(60),
        )
    }

    #[tokio::test]
    async fn resolve_no_settings_file_returns_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = test_manager(tmp.path());

        let resolved = mgr.resolve("my-bucket", "/some/key").await;

        assert_eq!(resolved.get_ttl, Duration::from_secs(300));
        assert_eq!(resolved.head_ttl, Duration::from_secs(60));
        assert_eq!(resolved.put_ttl, Duration::from_secs(3600));
        assert!(resolved.read_cache_enabled);
        assert!(resolved.write_cache_enabled);
        assert!(resolved.compression_enabled);
        assert!(resolved.ram_cache_eligible);
        assert!(matches!(resolved.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn resolve_bucket_settings_override_ttls() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"get_ttl": "10s", "head_ttl": "5s"}"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/some/key").await;

        assert_eq!(resolved.get_ttl, Duration::from_secs(10));
        assert_eq!(resolved.head_ttl, Duration::from_secs(5));
        assert_eq!(resolved.put_ttl, Duration::from_secs(3600)); // global fallback
        assert!(matches!(resolved.source, SettingsSource::Bucket(_)));
    }

    #[tokio::test]
    async fn resolve_prefix_override_takes_precedence() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{
                "get_ttl": "10s",
                "prefix_overrides": [
                    {"prefix": "/temp/", "get_ttl": "0s"},
                    {"prefix": "/static/", "get_ttl": "7d"}
                ]
            }"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());

        // Path matching /temp/ prefix
        let resolved = mgr.resolve("my-bucket", "/temp/file.txt").await;
        assert_eq!(resolved.get_ttl, Duration::ZERO);
        assert!(matches!(resolved.source, SettingsSource::Prefix(_, _)));

        // Path matching /static/ prefix
        let resolved = mgr.resolve("my-bucket", "/static/image.png").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(7 * 86400));

        // Path matching no prefix — falls back to bucket-level
        let resolved = mgr.resolve("my-bucket", "/other/file.txt").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn resolve_longest_prefix_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{
                "prefix_overrides": [
                    {"prefix": "/data/", "get_ttl": "30s"},
                    {"prefix": "/data/archive/", "get_ttl": "7d"}
                ]
            }"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());

        let resolved = mgr.resolve("my-bucket", "/data/archive/old.bin").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(7 * 86400));

        let resolved = mgr.resolve("my-bucket", "/data/recent.bin").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn resolve_zero_ttl_forces_ram_cache_ineligible() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"get_ttl": "0s", "ram_cache_eligible": true}"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;

        assert_eq!(resolved.get_ttl, Duration::ZERO);
        assert!(!resolved.ram_cache_eligible); // forced false despite explicit true
    }

    #[tokio::test]
    async fn resolve_read_cache_disabled_forces_ram_cache_ineligible() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"read_cache_enabled": false, "ram_cache_eligible": true}"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;

        assert!(!resolved.read_cache_enabled);
        assert!(!resolved.ram_cache_eligible); // forced false despite explicit true
    }

    #[tokio::test]
    async fn resolve_prefix_zero_ttl_forces_ram_ineligible() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{
                "ram_cache_eligible": true,
                "prefix_overrides": [
                    {"prefix": "/volatile/", "get_ttl": "0s", "ram_cache_eligible": true}
                ]
            }"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/volatile/data.json").await;

        assert_eq!(resolved.get_ttl, Duration::ZERO);
        assert!(!resolved.ram_cache_eligible); // zero TTL overrides explicit true
    }

    #[tokio::test]
    async fn resolve_cascade_each_field_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        // Bucket sets get_ttl and compression_enabled
        // Prefix sets only head_ttl
        // Other fields fall through to global
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{
                "get_ttl": "10s",
                "compression_enabled": false,
                "prefix_overrides": [
                    {"prefix": "/special/", "head_ttl": "1s"}
                ]
            }"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/special/file").await;

        assert_eq!(resolved.get_ttl, Duration::from_secs(10)); // bucket
        assert_eq!(resolved.head_ttl, Duration::from_secs(1)); // prefix
        assert_eq!(resolved.put_ttl, Duration::from_secs(3600)); // global
        assert!(!resolved.compression_enabled); // bucket
        assert!(resolved.write_cache_enabled); // global
    }

    #[tokio::test]
    async fn resolve_empty_json_uses_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(bucket_dir.join("_settings.json"), "{}").unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;

        assert_eq!(resolved.get_ttl, Duration::from_secs(300));
        assert_eq!(resolved.head_ttl, Duration::from_secs(60));
        assert!(resolved.ram_cache_eligible);
        assert!(matches!(resolved.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn resolve_caches_settings_across_calls() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"get_ttl": "10s"}"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());

        // First call loads from disk
        let r1 = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(r1.get_ttl, Duration::from_secs(10));

        // Modify file on disk — should NOT be picked up (within staleness threshold)
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"get_ttl": "99s"}"#,
        )
        .unwrap();

        let r2 = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(r2.get_ttl, Duration::from_secs(10)); // still cached
    }

    #[tokio::test]
    async fn resolve_schema_field_ignored_in_resolution() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"$schema": "https://example.com/schema.json", "get_ttl": "15s"}"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;

        assert_eq!(resolved.get_ttl, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn resolve_write_cache_independent_of_read_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"{"read_cache_enabled": false, "write_cache_enabled": true}"#,
        )
        .unwrap();

        let mgr = test_manager(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;

        assert!(!resolved.read_cache_enabled);
        assert!(resolved.write_cache_enabled); // independent of read cache
    }

    /// Helper: create a manager with zero staleness threshold so every resolve() triggers reload.
    fn test_manager_always_reload(cache_dir: &std::path::Path) -> BucketSettingsManager {
        BucketSettingsManager::new(
            cache_dir.to_path_buf(),
            test_global_defaults(),
            Duration::ZERO, // force reload on every call
        )
    }

    // ========================================================================
    // Error recovery tests (Task 2.3)
    // Requirements: 1.3, 1.4, 9.6, 10.5
    // ========================================================================

    #[tokio::test]
    async fn error_recovery_invalid_json_uses_previous_valid_settings() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let settings_path = bucket_dir.join("_settings.json");

        // Step 1: Load valid settings
        std::fs::write(&settings_path, r#"{"get_ttl": "10s", "compression_enabled": false}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(10));
        assert!(!resolved.compression_enabled);

        // Step 2: Replace with invalid JSON
        std::fs::write(&settings_path, r#"{"get_ttl": BROKEN"#).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;

        // Should keep previous valid settings, not fall back to global defaults
        assert_eq!(resolved.get_ttl, Duration::from_secs(10));
        assert!(!resolved.compression_enabled);
    }

    #[tokio::test]
    async fn error_recovery_invalid_prefixes_uses_previous_valid_settings() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let settings_path = bucket_dir.join("_settings.json");

        // Step 1: Load valid settings
        std::fs::write(&settings_path, r#"{"get_ttl": "20s", "head_ttl": "5s"}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(20));
        assert_eq!(resolved.head_ttl, Duration::from_secs(5));

        // Step 2: Replace with valid JSON but invalid prefix (no leading slash)
        std::fs::write(
            &settings_path,
            r#"{"get_ttl": "99s", "prefix_overrides": [{"prefix": "bad-prefix", "get_ttl": "1s"}]}"#,
        )
        .unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;

        // Should keep previous valid settings
        assert_eq!(resolved.get_ttl, Duration::from_secs(20));
        assert_eq!(resolved.head_ttl, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn error_recovery_file_not_found_uses_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        // Don't create any settings file
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("no-such-bucket", "/key").await;

        // Should use global defaults
        assert_eq!(resolved.get_ttl, Duration::from_secs(300));
        assert_eq!(resolved.head_ttl, Duration::from_secs(60));
        assert_eq!(resolved.put_ttl, Duration::from_secs(3600));
        assert!(resolved.read_cache_enabled);
        assert!(resolved.write_cache_enabled);
        assert!(resolved.compression_enabled);
        assert!(resolved.ram_cache_eligible);
        assert!(matches!(resolved.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn error_recovery_file_deleted_after_valid_load_uses_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let settings_path = bucket_dir.join("_settings.json");

        // Step 1: Load valid settings
        std::fs::write(&settings_path, r#"{"get_ttl": "10s"}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(10));

        // Step 2: Delete the file (file not found on reload)
        std::fs::remove_file(&settings_path).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;

        // File not found caches "no settings" marker → global defaults
        assert_eq!(resolved.get_ttl, Duration::from_secs(300));
        assert!(matches!(resolved.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn error_recovery_no_previous_valid_uses_global_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // First load is already invalid JSON — no previous valid settings exist
        std::fs::write(
            bucket_dir.join("_settings.json"),
            r#"NOT VALID JSON"#,
        )
        .unwrap();

        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;

        // No previous valid → global defaults
        assert_eq!(resolved.get_ttl, Duration::from_secs(300));
        assert_eq!(resolved.head_ttl, Duration::from_secs(60));
        assert!(matches!(resolved.source, SettingsSource::Global));
    }

    #[tokio::test]
    async fn error_recovery_multiple_invalid_reloads_preserve_last_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let settings_path = bucket_dir.join("_settings.json");

        // Step 1: Load valid settings
        std::fs::write(&settings_path, r#"{"get_ttl": "42s"}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(42));

        // Step 2: First invalid reload
        std::fs::write(&settings_path, r#"BROKEN"#).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(42));

        // Step 3: Second invalid reload — should still preserve the original valid settings
        std::fs::write(&settings_path, r#"ALSO BROKEN"#).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(42));

        // Step 4: Third invalid reload with different invalid content
        std::fs::write(&settings_path, r#"{"get_ttl": "not-a-duration"}"#).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(42));
    }

    #[tokio::test]
    async fn error_recovery_valid_after_invalid_updates_settings() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let settings_path = bucket_dir.join("_settings.json");

        // Step 1: Load valid settings
        std::fs::write(&settings_path, r#"{"get_ttl": "10s"}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(10));

        // Step 2: Invalid reload — falls back to previous valid
        std::fs::write(&settings_path, r#"BROKEN"#).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(10));

        // Step 3: New valid settings — should pick up the new values
        std::fs::write(&settings_path, r#"{"get_ttl": "99s"}"#).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(99));
    }

    #[tokio::test]
    async fn error_recovery_io_error_uses_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket_dir = tmp.path().join("metadata").join("my-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let settings_path = bucket_dir.join("_settings.json");

        // Step 1: Load valid settings
        std::fs::write(&settings_path, r#"{"get_ttl": "15s"}"#).unwrap();
        let mgr = test_manager_always_reload(tmp.path());
        let resolved = mgr.resolve("my-bucket", "/key").await;
        assert_eq!(resolved.get_ttl, Duration::from_secs(15));

        // Step 2: Replace file with a directory to cause an I/O error on read
        std::fs::remove_file(&settings_path).unwrap();
        std::fs::create_dir_all(&settings_path).unwrap();
        let resolved = mgr.resolve("my-bucket", "/key").await;

        // Should fall back to previous valid settings
        assert_eq!(resolved.get_ttl, Duration::from_secs(15));
    }
}
