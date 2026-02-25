//! Configuration Module
//!
//! Handles configuration loading from files, environment variables, and command-line arguments.
//! Supports YAML configuration files and runtime configuration management.

use crate::{ProxyError, Result};
use clap::{Arg, Command};
use serde::{Deserialize, Serialize};

use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Custom deserializer for PathBuf that expands ~ to home directory
mod pathbuf_serde {
    use serde::{Deserialize, Deserializer};
    use std::path::PathBuf;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(expand_tilde(&s))
    }

    fn expand_tilde(path: &str) -> PathBuf {
        if path.starts_with("~/") {
            if let Some(home) = std::env::var_os("HOME") {
                let mut result = PathBuf::from(home);
                result.push(&path[2..]);
                return result;
            }
        }
        PathBuf::from(path)
    }
}

/// Custom deserializer for Duration from string format like "30s", "5m", "1h"
pub(crate) mod duration_serde {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    pub(crate) fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("Empty duration string".to_string());
        }

        // Find where the number ends and the unit begins
        let mut num_end = 0;
        for (i, c) in s.chars().enumerate() {
            if c.is_ascii_digit() || c == '.' {
                num_end = i + 1;
            } else {
                break;
            }
        }

        if num_end == 0 {
            return Err(format!("No number found in duration string: {}", s));
        }

        let num_str = &s[..num_end];
        let unit = s[num_end..].trim();

        let value: f64 = num_str
            .parse()
            .map_err(|e| format!("Failed to parse number '{}': {}", num_str, e))?;

        let duration = match unit {
            "s" | "sec" | "secs" | "second" | "seconds" => Duration::from_secs_f64(value),
            "m" | "min" | "mins" | "minute" | "minutes" => Duration::from_secs_f64(value * 60.0),
            "h" | "hr" | "hrs" | "hour" | "hours" => Duration::from_secs_f64(value * 3600.0),
            "d" | "day" | "days" => Duration::from_secs_f64(value * 86400.0),
            "ms" | "millis" | "millisecond" | "milliseconds" => {
                Duration::from_secs_f64(value / 1000.0)
            }
            "" => Duration::from_secs_f64(value), // Default to seconds if no unit
            _ => return Err(format!("Unknown duration unit: {}", unit)),
        };

        Ok(duration)
    }
}

/// Dashboard configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardConfig {
    pub enabled: bool,
    pub port: u16,
    pub bind_address: String,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub cache_stats_refresh_interval: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub logs_refresh_interval: Duration,
    pub max_log_entries: usize,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Enabled by default per requirements
            port: 8081,    // Changed from 8080 to avoid conflict with health server
            bind_address: "0.0.0.0".to_string(),
            cache_stats_refresh_interval: Duration::from_secs(5),
            logs_refresh_interval: Duration::from_secs(10),
            max_log_entries: 100,
        }
    }
}

impl DashboardConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate port (1024-65535 for non-privileged ports)
        if self.port < 1024 {
            return Err(format!(
                "Dashboard port must be 1024 or higher for non-privileged ports, got {}",
                self.port
            ));
        }

        // Validate bind address format (basic check)
        if self.bind_address.is_empty() {
            return Err("Dashboard bind address cannot be empty".to_string());
        }

        // Validate refresh intervals (1-300 seconds)
        let cache_stats_secs = self.cache_stats_refresh_interval.as_secs();
        if cache_stats_secs < 1 || cache_stats_secs > 300 {
            return Err(format!(
                "Cache stats refresh interval must be between 1 and 300 seconds, got {}",
                cache_stats_secs
            ));
        }

        let logs_secs = self.logs_refresh_interval.as_secs();
        if logs_secs < 1 || logs_secs > 300 {
            return Err(format!(
                "Logs refresh interval must be between 1 and 300 seconds, got {}",
                logs_secs
            ));
        }

        // Validate max log entries (10-10000)
        if self.max_log_entries < 10 || self.max_log_entries > 10000 {
            return Err(format!(
                "Max log entries must be between 10 and 10000, got {}",
                self.max_log_entries
            ));
        }

        Ok(())
    }
}

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub connection_pool: ConnectionPoolConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub dashboard: DashboardConfig,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_https_port")]
    pub https_port: u16,
    /// Maximum number of concurrent requests to handle
    /// Default: 200 (suitable for most deployments)
    /// Small deployments: 50-100
    /// Medium deployments: 100-300  
    /// Large deployments: 300-1000+
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(
        deserialize_with = "duration_serde::deserialize",
        default = "default_request_timeout"
    )]
    pub request_timeout: Duration,
    /// Enable adding Referer header for proxy identification in S3 Server Access Logs
    #[serde(default = "default_add_referer_header")]
    pub add_referer_header: bool,
}

fn default_http_port() -> u16 {
    80
}

fn default_https_port() -> u16 {
    443
}

fn default_max_concurrent_requests() -> usize {
    200
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_add_referer_header() -> bool {
    true
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_port: default_http_port(),
            https_port: default_https_port(),
            max_concurrent_requests: default_max_concurrent_requests(),
            request_timeout: default_request_timeout(),
            add_referer_header: default_add_referer_header(),
        }
    }
}

/// HTTPS mode configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HttpsMode {
    #[serde(rename = "passthrough")]
    Passthrough, // TCP passthrough (only mode)
}

impl Default for HttpsMode {
    fn default() -> Self {
        HttpsMode::Passthrough
    }
}

/// Download coordination configuration for request coalescing
///
/// When multiple requests arrive for the same uncached resource, the InFlightTracker
/// coordinates them so only one request fetches from S3 while others wait. This reduces
/// redundant S3 fetches and improves cache efficiency under concurrent load.
///
/// Requirements: 18.1, 18.2, 18.3, 18.4, 18.5
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadCoordinationConfig {
    /// Enable download coordination/coalescing (default: true)
    /// When enabled, concurrent requests for the same uncached resource are coalesced.
    /// When disabled, each cache miss independently fetches from S3.
    /// Requirement: 18.4
    #[serde(default = "default_download_coordination_enabled")]
    pub enabled: bool,

    /// Maximum time a waiter will wait for the fetcher to complete (default: 30s)
    /// If the fetcher takes longer than this, the waiter falls back to its own S3 fetch.
    /// Valid range: 5-120 seconds (values outside this range are clamped with a warning)
    /// Requirement: 18.2, 18.3, 18.5
    #[serde(default = "default_download_coordination_wait_timeout_secs")]
    pub wait_timeout_secs: u64,
}

fn default_download_coordination_enabled() -> bool {
    true
}

fn default_download_coordination_wait_timeout_secs() -> u64 {
    30
}

impl Default for DownloadCoordinationConfig {
    fn default() -> Self {
        Self {
            enabled: default_download_coordination_enabled(),
            wait_timeout_secs: default_download_coordination_wait_timeout_secs(),
        }
    }
}

impl DownloadCoordinationConfig {
    /// Validate and clamp configuration values
    /// Returns the clamped wait_timeout_secs value
    /// Requirement: 18.5
    pub fn validate_and_clamp(&mut self) {
        const MIN_TIMEOUT: u64 = 5;
        const MAX_TIMEOUT: u64 = 120;

        if self.wait_timeout_secs < MIN_TIMEOUT {
            warn!(
                "download_coordination.wait_timeout_secs ({}) is below minimum ({}), clamping to {}",
                self.wait_timeout_secs, MIN_TIMEOUT, MIN_TIMEOUT
            );
            self.wait_timeout_secs = MIN_TIMEOUT;
        } else if self.wait_timeout_secs > MAX_TIMEOUT {
            warn!(
                "download_coordination.wait_timeout_secs ({}) exceeds maximum ({}), clamping to {}",
                self.wait_timeout_secs, MAX_TIMEOUT, MAX_TIMEOUT
            );
            self.wait_timeout_secs = MAX_TIMEOUT;
        }
    }

    /// Get the wait timeout as a Duration
    pub fn wait_timeout(&self) -> Duration {
        Duration::from_secs(self.wait_timeout_secs)
    }
}

/// Shared storage configuration for multi-instance deployments
///
/// Activates all coordination features needed for multiple proxy instances
/// sharing a cache volume (EFS, NFS, shared PersistentVolume, etc.):
/// - Atomic metadata writes with journaling
/// - Distributed eviction coordination
/// - Cross-instance validation
/// - File locking for metadata operations
/// - Orphaned range recovery coordination
///
/// Note: Shared storage coordination is always enabled. The journal-based metadata
/// writes and distributed eviction locking are used unconditionally for consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedStorageConfig {
    /// Maximum time to wait when acquiring a file lock before giving up (default: 60s)
    /// Longer values handle slow storage; shorter values fail faster on contention.
    /// Valid range: 10-300 seconds
    #[serde(
        default = "default_shared_storage_lock_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub lock_timeout: Duration,

    /// How often to refresh held locks to prevent expiration (default: 30s)
    /// Must be less than lock_timeout to maintain lock ownership.
    /// Valid range: 5-120 seconds
    #[serde(
        default = "default_shared_storage_lock_refresh_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub lock_refresh_interval: Duration,

    /// Cache inconsistency percentage that triggers a warning log (default: 5.0)
    /// Helps identify emerging issues before they become critical.
    /// Valid range: 0.0-100.0, must be less than validation_threshold_error
    #[serde(default = "default_validation_threshold_warn")]
    pub validation_threshold_warn: f64,

    /// Cache inconsistency percentage that triggers an error log (default: 20.0)
    /// Indicates significant coordination problems requiring attention.
    /// Valid range: 0.0-100.0, must be greater than validation_threshold_warn
    #[serde(default = "default_validation_threshold_error")]
    pub validation_threshold_error: f64,

    /// Maximum number of orphaned ranges to recover simultaneously (default: 4)
    /// Higher values speed up recovery but increase I/O load.
    /// Valid range: 1-16
    #[serde(default = "default_recovery_max_concurrent")]
    pub recovery_max_concurrent: usize,

    /// Journal consolidation interval (default: 5s)
    /// How often to consolidate journal entries into final metadata files.
    /// Shorter intervals improve cross-instance cache visibility but increase I/O.
    /// Valid range: 1-60 seconds
    #[serde(
        default = "default_consolidation_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub consolidation_interval: Duration,

    /// Journal size threshold for immediate consolidation (default: 1MB)
    /// When journal file exceeds this size, trigger immediate consolidation.
    /// Valid range: 100KB-10MB
    #[serde(default = "default_consolidation_size_threshold")]
    pub consolidation_size_threshold: u64,

    /// Maximum duration for the per-key processing phase of a consolidation cycle (default: 30s)
    /// When the timeout is reached, remaining keys are skipped and retried next cycle.
    /// Prevents one slow NFS operation from blocking the next cycle or holding locks indefinitely.
    /// Requirement: 4.4
    #[serde(
        default = "default_consolidation_cycle_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub consolidation_cycle_timeout: Duration,

    /// Maximum lock acquisition retries (default: 5)
    /// Number of times to retry lock acquisition before giving up.
    /// Valid range: 1-20
    #[serde(default = "default_lock_max_retries")]
    pub lock_max_retries: u32,

    /// Metadata validation frequency (default: 23h)
    /// How often to run background validation of metadata consistency.
    /// Valid range: 1-168 hours
    #[serde(
        default = "default_validation_frequency",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub validation_frequency: Duration,

    /// Distributed eviction lock timeout (default: 60s)
    /// Maximum time an instance can hold the global eviction lock before it's considered stale.
    /// Other instances can forcibly acquire stale locks to prevent deadlocks.
    /// Valid range: 30-3600 seconds (30 seconds to 1 hour)
    /// Requirement: 6.1, 6.2, 6.4
    #[serde(
        default = "default_eviction_lock_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub eviction_lock_timeout: Duration,

    /// Enable background orphan recovery (default: true when shared_storage is enabled)
    /// Scans for orphaned .bin files that aren't tracked in metadata and recovers them.
    /// Essential for shared storage where journal consolidation may lag behind writes.
    #[serde(default = "default_orphan_recovery_enabled")]
    pub orphan_recovery_enabled: bool,

    /// Interval between orphan recovery scans (default: 300s = 5 minutes)
    /// Each scan processes one shard to spread I/O load over time.
    /// Valid range: 60-3600 seconds
    #[serde(
        default = "default_orphan_recovery_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub orphan_recovery_interval: Duration,

    /// Maximum time to spend scanning per cycle (default: 30s)
    /// Prevents long scans from blocking other operations.
    /// Valid range: 5-300 seconds
    #[serde(
        default = "default_orphan_scan_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub orphan_scan_timeout: Duration,

    /// Maximum orphans to process per recovery cycle (default: 100)
    /// Limits I/O impact per cycle. Remaining orphans processed in subsequent cycles.
    /// Valid range: 10-1000
    #[serde(default = "default_orphan_max_per_cycle")]
    pub orphan_max_per_cycle: usize,
}

fn default_shared_storage_lock_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_shared_storage_lock_refresh_interval() -> Duration {
    Duration::from_secs(30)
}

/// Default eviction lock timeout: 60 seconds
/// Requirement: 6.2 - THE system SHALL use a default lock timeout of 60 seconds if not configured
fn default_eviction_lock_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_orphan_recovery_enabled() -> bool {
    true
}

fn default_orphan_recovery_interval() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_orphan_scan_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_orphan_max_per_cycle() -> usize {
    100
}

impl Default for SharedStorageConfig {
    fn default() -> Self {
        Self {
            lock_timeout: default_shared_storage_lock_timeout(),
            lock_refresh_interval: default_shared_storage_lock_refresh_interval(),
            validation_threshold_warn: default_validation_threshold_warn(),
            validation_threshold_error: default_validation_threshold_error(),
            recovery_max_concurrent: default_recovery_max_concurrent(),
            consolidation_interval: default_consolidation_interval(),
            consolidation_size_threshold: default_consolidation_size_threshold(),
            consolidation_cycle_timeout: default_consolidation_cycle_timeout(),
            lock_max_retries: default_lock_max_retries(),
            validation_frequency: default_validation_frequency(),
            eviction_lock_timeout: default_eviction_lock_timeout(),
            orphan_recovery_enabled: default_orphan_recovery_enabled(),
            orphan_recovery_interval: default_orphan_recovery_interval(),
            orphan_scan_timeout: default_orphan_scan_timeout(),
            orphan_max_per_cycle: default_orphan_max_per_cycle(),
        }
    }
}

impl SharedStorageConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate lock_timeout (10-300 seconds)
        let lock_timeout_secs = self.lock_timeout.as_secs();
        if lock_timeout_secs < 10 || lock_timeout_secs > 300 {
            return Err(format!(
                "lock_timeout must be between 10 and 300 seconds, got {}",
                lock_timeout_secs
            ));
        }

        // Validate lock_refresh_interval (5-120 seconds, must be less than lock_timeout)
        let lock_refresh_secs = self.lock_refresh_interval.as_secs();
        if lock_refresh_secs < 5 || lock_refresh_secs > 120 {
            return Err(format!(
                "lock_refresh_interval must be between 5 and 120 seconds, got {}",
                lock_refresh_secs
            ));
        }
        if lock_refresh_secs >= lock_timeout_secs {
            return Err(format!(
                "lock_refresh_interval ({}) must be less than lock_timeout ({})",
                lock_refresh_secs, lock_timeout_secs
            ));
        }

        // Validate threshold values
        if self.validation_threshold_warn < 0.0 || self.validation_threshold_warn > 100.0 {
            return Err(format!(
                "validation_threshold_warn must be between 0.0 and 100.0, got {}",
                self.validation_threshold_warn
            ));
        }
        if self.validation_threshold_error < 0.0 || self.validation_threshold_error > 100.0 {
            return Err(format!(
                "validation_threshold_error must be between 0.0 and 100.0, got {}",
                self.validation_threshold_error
            ));
        }
        if self.validation_threshold_warn >= self.validation_threshold_error {
            return Err(format!(
                "validation_threshold_warn ({}) must be less than validation_threshold_error ({})",
                self.validation_threshold_warn, self.validation_threshold_error
            ));
        }

        // Validate recovery_max_concurrent (1-16)
        if self.recovery_max_concurrent < 1 || self.recovery_max_concurrent > 16 {
            return Err(format!(
                "recovery_max_concurrent must be between 1 and 16, got {}",
                self.recovery_max_concurrent
            ));
        }

        // Validate consolidation_interval (1-60 seconds)
        let consolidation_secs = self.consolidation_interval.as_secs();
        if consolidation_secs < 1 || consolidation_secs > 60 {
            return Err(format!(
                "consolidation_interval must be between 1 and 60 seconds, got {}",
                consolidation_secs
            ));
        }

        // Validate consolidation_size_threshold (100KB-10MB)
        if self.consolidation_size_threshold < 100 * 1024
            || self.consolidation_size_threshold > 10 * 1024 * 1024
        {
            return Err(format!(
                "consolidation_size_threshold must be between 100KB and 10MB, got {}",
                self.consolidation_size_threshold
            ));
        }

        // Validate lock_max_retries (1-20)
        if self.lock_max_retries < 1 || self.lock_max_retries > 20 {
            return Err(format!(
                "lock_max_retries must be between 1 and 20, got {}",
                self.lock_max_retries
            ));
        }

        // Validate validation_frequency (1h-168h)
        let validation_hours = self.validation_frequency.as_secs() / 3600;
        if validation_hours < 1 || validation_hours > 168 {
            return Err(format!(
                "validation_frequency must be between 1 and 168 hours, got {}h",
                validation_hours
            ));
        }

        // Validate eviction_lock_timeout (30-3600 seconds)
        // Requirement: 6.4 - THE system SHALL validate that lock timeout is between 30 and 3600 seconds
        let eviction_lock_timeout_secs = self.eviction_lock_timeout.as_secs();
        if eviction_lock_timeout_secs < 30 || eviction_lock_timeout_secs > 3600 {
            return Err(format!(
                "eviction_lock_timeout must be between 30 and 3600 seconds, got {}",
                eviction_lock_timeout_secs
            ));
        }

        Ok(())
    }
}

fn default_range_merge_gap_threshold() -> u64 {
    1024 * 1024 // 1MiB
}

fn default_eviction_buffer_percent() -> u8 {
    5
}
fn default_full_object_check_threshold() -> u64 {
    67_108_864 // 64 MiB
}

fn default_disk_streaming_threshold() -> u64 {
    1_048_576 // 1 MiB
}

/// Default eviction trigger percentage: 95%
/// Eviction triggers when cache size exceeds this percentage of max_cache_size
/// Requirement: 3.1
fn default_eviction_trigger_percent() -> u8 {
    95
}

/// Default eviction target percentage: 80%
/// After eviction, cache size should be reduced to this percentage of max_cache_size
/// Requirement: 3.2
fn default_eviction_target_percent() -> u8 {
    80
}

fn default_ram_cache_flush_interval() -> Duration {
    Duration::from_secs(60) // 60 seconds
}

fn default_ram_cache_flush_threshold() -> usize {
    100 // 100 pending updates
}

fn default_ram_cache_flush_on_eviction() -> bool {
    false // Do not flush on eviction by default
}

fn default_ram_cache_verification_interval() -> Duration {
    Duration::from_secs(1) // 1 second
}

fn default_incomplete_upload_ttl() -> Duration {
    Duration::from_secs(86400) // 1 day (default)
}

fn default_parallel_scan() -> bool {
    true
}

// Shared storage / atomic metadata writes configuration defaults
fn default_consolidation_interval() -> Duration {
    Duration::from_secs(5) // 5 seconds - reduced from 30s for faster cross-instance cache visibility
}

fn default_consolidation_size_threshold() -> u64 {
    1024 * 1024 // 1MB
}

fn default_lock_max_retries() -> u32 {
    5
}

fn default_validation_frequency() -> Duration {
    Duration::from_secs(23 * 3600) // 23 hours
}

fn default_recovery_max_concurrent() -> usize {
    4
}

fn default_validation_threshold_warn() -> f64 {
    5.0
}

fn default_validation_threshold_error() -> f64 {
    20.0
}

fn default_scan_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_consolidation_cycle_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_progress_logging() -> bool {
    true
}

/// Cache initialization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializationConfig {
    /// Enable parallel metadata processing during initialization (default: true)
    #[serde(default = "default_parallel_scan")]
    pub parallel_scan: bool,
    /// Timeout for directory scanning operations (default: 30s)
    #[serde(
        default = "default_scan_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub scan_timeout: Duration,
    /// Enable detailed progress logging during initialization (default: true)
    #[serde(default = "default_progress_logging")]
    pub progress_logging: bool,
}

impl Default for InitializationConfig {
    fn default() -> Self {
        Self {
            parallel_scan: default_parallel_scan(),
            scan_timeout: default_scan_timeout(),
            progress_logging: default_progress_logging(),
        }
    }
}

impl InitializationConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate scan timeout (minimum 5 seconds, maximum 300 seconds)
        let scan_timeout_secs = self.scan_timeout.as_secs();
        if scan_timeout_secs < 5 || scan_timeout_secs > 300 {
            return Err(format!(
                "scan_timeout must be between 5 and 300 seconds, got {}",
                scan_timeout_secs
            ));
        }

        Ok(())
    }
}

// Default functions for MetadataCacheConfig
fn default_metadata_cache_enabled() -> bool {
    true
}

fn default_metadata_cache_refresh_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_metadata_cache_max_entries() -> usize {
    10000
}

fn default_metadata_cache_stale_handle_max_retries() -> u32 {
    3
}

/// RAM Metadata Cache configuration
///
/// Controls the in-memory cache for NewCacheMetadata objects, reducing disk I/O
/// for both HEAD and GET requests. Uses simple LRU eviction since all metadata
/// entries are similar size (~1-2KB).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataCacheConfig {
    /// Whether the metadata cache is enabled (default: true)
    #[serde(default = "default_metadata_cache_enabled")]
    pub enabled: bool,
    /// How often to re-read from disk to check for changes (default: 5s)
    /// This is the staleness threshold - entries older than this are refreshed from disk.
    /// Valid range: 1-300 seconds
    #[serde(
        default = "default_metadata_cache_refresh_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub refresh_interval: Duration,
    /// Maximum number of entries in the cache (default: 10000)
    /// Each entry is ~1-2KB, so 10000 entries uses ~15-25MB of RAM.
    /// Valid range: 100-1000000
    #[serde(default = "default_metadata_cache_max_entries")]
    pub max_entries: usize,
    /// Maximum retries for stale file handle errors (default: 3)
    /// On shared storage (EFS/NFS), stale file handles can occur when files
    /// are modified by other instances. This controls retry behavior.
    /// Valid range: 1-10
    #[serde(default = "default_metadata_cache_stale_handle_max_retries")]
    pub stale_handle_max_retries: u32,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_metadata_cache_enabled(),
            refresh_interval: default_metadata_cache_refresh_interval(),
            max_entries: default_metadata_cache_max_entries(),
            stale_handle_max_retries: default_metadata_cache_stale_handle_max_retries(),
        }
    }
}

impl MetadataCacheConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate refresh_interval (1-300 seconds)
        let refresh_secs = self.refresh_interval.as_secs();
        if refresh_secs < 1 || refresh_secs > 300 {
            return Err(format!(
                "metadata_cache.refresh_interval must be between 1 and 300 seconds, got {}",
                refresh_secs
            ));
        }

        // Validate max_entries (100-1000000)
        if self.max_entries < 100 || self.max_entries > 1_000_000 {
            return Err(format!(
                "metadata_cache.max_entries must be between 100 and 1000000, got {}",
                self.max_entries
            ));
        }

        // Validate stale_handle_max_retries (1-10)
        if self.stale_handle_max_retries < 1 || self.stale_handle_max_retries > 10 {
            return Err(format!(
                "metadata_cache.stale_handle_max_retries must be between 1 and 10, got {}",
                self.stale_handle_max_retries
            ));
        }

        Ok(())
    }

    /// Convert to the MetadataCacheConfig used by the metadata_cache module
    pub fn to_metadata_cache_config(&self) -> crate::metadata_cache::MetadataCacheConfig {
        crate::metadata_cache::MetadataCacheConfig {
            enabled: self.enabled,
            refresh_interval: self.refresh_interval,
            max_entries: self.max_entries,
            stale_handle_max_retries: self.stale_handle_max_retries,
        }
    }
}

impl ConnectionPoolConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate idle_timeout (10-300 seconds)
        let idle_timeout_secs = self.idle_timeout.as_secs();
        if idle_timeout_secs < 10 || idle_timeout_secs > 300 {
            return Err(format!(
                "Idle timeout must be between 10 and 300 seconds, got {}",
                idle_timeout_secs
            ));
        }

        // Validate max_idle_per_host (1-50)
        if self.max_idle_per_host < 1 || self.max_idle_per_host > 50 {
            return Err(format!(
                "Max idle connections per host must be between 1 and 50, got {}",
                self.max_idle_per_host
            ));
        }

        // Validate max_lifetime (60-3600 seconds)
        let max_lifetime_secs = self.max_lifetime.as_secs();
        if max_lifetime_secs < 60 || max_lifetime_secs > 3600 {
            return Err(format!(
                "Max connection lifetime must be between 60 and 3600 seconds, got {}",
                max_lifetime_secs
            ));
        }

        // Validate pool_check_interval (1-60 seconds)
        let pool_check_interval_secs = self.pool_check_interval.as_secs();
        if pool_check_interval_secs < 1 || pool_check_interval_secs > 60 {
            return Err(format!(
                "Pool check interval must be between 1 and 60 seconds, got {}",
                pool_check_interval_secs
            ));
        }

        Ok(())
    }
}

fn default_write_cache_enabled() -> bool {
    true // Enabled by default - write caching is complete
}

fn default_cache_bypass_headers_enabled() -> bool {
    true // Enabled by default - allows clients to bypass cache via Cache-Control/Pragma headers
}

fn default_read_cache_enabled() -> bool {
    true // Enabled by default - read caching for GET responses
}

fn default_bucket_settings_staleness_threshold() -> Duration {
    Duration::from_secs(60) // 60 seconds
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(deserialize_with = "pathbuf_serde::deserialize")]
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub ram_cache_enabled: bool,
    pub max_ram_cache_size: u64,
    /// Eviction algorithm used for both RAM and disk caches (unified)
    pub eviction_algorithm: EvictionAlgorithm,
    /// Enable write-through caching for PUT operations (default: true)
    #[serde(default = "default_write_cache_enabled")]
    pub write_cache_enabled: bool,
    pub write_cache_percent: f32,
    pub write_cache_max_object_size: u64,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub put_ttl: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub get_ttl: Duration,
    /// TTL for metadata validation (HEAD requests) - typically shorter than data TTL
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub head_ttl: Duration,
    /// Whether to actively remove cached data when get_ttl expires (default: false for lazy expiration)
    #[serde(default)]
    pub actively_remove_cached_data: bool,

    /// Shared storage configuration for multi-instance deployments
    #[serde(default)]
    pub shared_storage: SharedStorageConfig,
    /// Download coordination configuration for request coalescing
    /// When enabled, concurrent requests for the same uncached resource are coalesced
    /// so only one request fetches from S3 while others wait.
    #[serde(default)]
    pub download_coordination: DownloadCoordinationConfig,
    /// Gap threshold for range merging consolidation (default: 1MiB)
    /// When fetching missing ranges, ranges separated by gaps smaller than this threshold
    /// will be consolidated into a single S3 request to reduce request overhead
    ///
    /// Note: Range modification only applies to unsigned requests. For signed requests
    /// (AWS SigV4), the proxy forwards the exact Range header to preserve signature validity.
    /// This dual-mode design allows range consolidation for unsigned requests while
    /// maintaining compatibility with signed requests.
    #[serde(default = "default_range_merge_gap_threshold")]
    pub range_merge_gap_threshold: u64,
    /// Eviction buffer percentage (default: 5)
    /// When eviction is triggered at 95% capacity, evict to (100 - eviction_buffer_percent)%
    /// to create a buffer and minimize scan frequency
    #[serde(default = "default_eviction_buffer_percent")]
    pub eviction_buffer_percent: u8,
    /// Interval between periodic flushes of RAM cache access statistics to disk
    /// Default: 60 seconds, Range: 10-600 seconds
    #[serde(
        default = "default_ram_cache_flush_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub ram_cache_flush_interval: Duration,
    /// Number of pending updates that triggers an immediate flush
    /// Default: 100, Range: 10-10000
    #[serde(default = "default_ram_cache_flush_threshold")]
    pub ram_cache_flush_threshold: usize,
    /// Whether to flush pending updates when RAM cache entries are evicted
    /// Default: false (do not flush on eviction to avoid blocking)
    #[serde(default = "default_ram_cache_flush_on_eviction")]
    pub ram_cache_flush_on_eviction: bool,
    /// Interval between RAM cache coherency verification checks
    /// Default: 1 second, Range: 1-60 seconds
    #[serde(
        default = "default_ram_cache_verification_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub ram_cache_verification_interval: Duration,
    /// TTL for incomplete multipart uploads before eviction
    /// Default: 1 day, Range: 1 hour to 7 days
    /// Incomplete uploads that exceed this TTL are automatically cleaned up
    #[serde(
        default = "default_incomplete_upload_ttl",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub incomplete_upload_ttl: Duration,
    /// Cache initialization configuration
    #[serde(default)]
    pub initialization: InitializationConfig,
    /// Enable cache bypass header support (Cache-Control: no-cache, no-store, Pragma: no-cache)
    /// When enabled, clients can bypass the cache by including these headers in requests.
    /// Default: true
    #[serde(default = "default_cache_bypass_headers_enabled")]
    pub cache_bypass_headers_enabled: bool,
    /// RAM Metadata Cache configuration
    /// Caches NewCacheMetadata objects in RAM to reduce disk I/O for HEAD and GET requests.
    #[serde(default)]
    pub metadata_cache: MetadataCacheConfig,
    /// Percentage of max_cache_size at which eviction triggers (default: 95)
    /// Valid range: 50-100
    /// Requirement: 3.1
    #[serde(default = "default_eviction_trigger_percent")]
    pub eviction_trigger_percent: u8,
    /// Percentage of max_cache_size to reduce to after eviction (default: 80)
    /// Valid range: 50-99, must be less than eviction_trigger_percent
    /// Requirement: 3.2
    #[serde(default = "default_eviction_target_percent")]
    pub eviction_target_percent: u8,
    /// Content-length threshold above which the full-object cache check is skipped
    /// for range requests (default: 64 MiB). Large files are unlikely to be cached
    /// as a single full-object range, so skipping the check avoids scanning hundreds
    /// of cached ranges unnecessarily.
    /// Requirement: 2.2
    #[serde(default = "default_full_object_check_threshold")]
    pub full_object_check_threshold: u64,
    /// Disk streaming threshold (default: 1 MiB)
    /// Cached ranges at or above this size are streamed from disk in chunks instead of
    /// loaded fully into memory. Ranges below this threshold are loaded into memory as before.
    /// Requirement: 5.5
    #[serde(default = "default_disk_streaming_threshold")]
    pub disk_streaming_threshold: u64,
    /// Enable read caching for GET responses (default: true)
    /// When false, no buckets are read-cached unless explicitly enabled via bucket settings.
    /// This allows an "allowlist" pattern where only specific buckets have caching enabled.
    #[serde(default = "default_read_cache_enabled")]
    pub read_cache_enabled: bool,
    /// How long cached bucket settings are considered fresh before re-reading from disk (default: 60s)
    #[serde(
        default = "default_bucket_settings_staleness_threshold",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub bucket_settings_staleness_threshold: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/cache"),
            max_cache_size: 10 * 1024 * 1024 * 1024, // 10GB
            ram_cache_enabled: false,
            max_ram_cache_size: 256 * 1024 * 1024, // 256MB
            eviction_algorithm: EvictionAlgorithm::default(),
            write_cache_enabled: true,
            write_cache_percent: 10.0,
            write_cache_max_object_size: 256 * 1024 * 1024, // 256MB
            put_ttl: Duration::from_secs(3600),             // 1 hour
            get_ttl: Duration::from_secs(315360000),        // ~10 years
            head_ttl: Duration::from_secs(60),              // 1 minute
            actively_remove_cached_data: false,
            shared_storage: SharedStorageConfig::default(),
            download_coordination: DownloadCoordinationConfig::default(),
            range_merge_gap_threshold: default_range_merge_gap_threshold(),
            eviction_buffer_percent: default_eviction_buffer_percent(),
            ram_cache_flush_interval: default_ram_cache_flush_interval(),
            ram_cache_flush_threshold: default_ram_cache_flush_threshold(),
            ram_cache_flush_on_eviction: default_ram_cache_flush_on_eviction(),
            ram_cache_verification_interval: default_ram_cache_verification_interval(),
            incomplete_upload_ttl: default_incomplete_upload_ttl(),
            initialization: InitializationConfig::default(),
            cache_bypass_headers_enabled: default_cache_bypass_headers_enabled(),
            metadata_cache: MetadataCacheConfig::default(),
            eviction_trigger_percent: default_eviction_trigger_percent(),
            eviction_target_percent: default_eviction_target_percent(),
            full_object_check_threshold: default_full_object_check_threshold(),
            disk_streaming_threshold: default_disk_streaming_threshold(),
            read_cache_enabled: default_read_cache_enabled(),
            bucket_settings_staleness_threshold: default_bucket_settings_staleness_threshold(),
        }
    }
}

/// Cache eviction algorithms
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EvictionAlgorithm {
    #[serde(rename = "lru")]
    LRU, // Least Recently Used (default)
    #[serde(rename = "tinylfu")]
    TinyLFU, // Window-based TinyLFU
}

impl Default for EvictionAlgorithm {
    fn default() -> Self {
        EvictionAlgorithm::LRU
    }
}

impl From<EvictionAlgorithm> for crate::cache::CacheEvictionAlgorithm {
    fn from(algo: EvictionAlgorithm) -> Self {
        match algo {
            EvictionAlgorithm::LRU => crate::cache::CacheEvictionAlgorithm::LRU,
            EvictionAlgorithm::TinyLFU => crate::cache::CacheEvictionAlgorithm::TinyLFU,
        }
    }
}

impl From<crate::cache::CacheEvictionAlgorithm> for EvictionAlgorithm {
    fn from(algo: crate::cache::CacheEvictionAlgorithm) -> Self {
        match algo {
            crate::cache::CacheEvictionAlgorithm::LRU => EvictionAlgorithm::LRU,
            crate::cache::CacheEvictionAlgorithm::TinyLFU => EvictionAlgorithm::TinyLFU,
        }
    }
}

/// Default flush interval for access log buffer (5 seconds)
fn default_access_log_flush_interval() -> Duration {
    Duration::from_secs(5)
}

/// Default maximum entries in access log buffer before forced flush (1000)
fn default_access_log_buffer_size() -> usize {
    1000
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(deserialize_with = "pathbuf_serde::deserialize")]
    pub access_log_dir: PathBuf,
    #[serde(deserialize_with = "pathbuf_serde::deserialize")]
    pub app_log_dir: PathBuf,
    pub access_log_enabled: bool,
    pub access_log_mode: AccessLogMode,
    pub log_level: String,
    /// Flush interval for access log buffer (default: 5s)
    /// Access log entries are buffered in RAM and flushed to disk at this interval
    /// or when the buffer reaches access_log_buffer_size entries, whichever comes first.
    /// Valid range: 1-60 seconds
    #[serde(
        default = "default_access_log_flush_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub access_log_flush_interval: Duration,
    /// Maximum entries in access log buffer before forced flush (default: 1000)
    /// When the buffer reaches this size, it will be flushed immediately regardless
    /// of the flush interval. Valid range: 100-100000
    #[serde(default = "default_access_log_buffer_size")]
    pub access_log_buffer_size: usize,
}

/// Access logging mode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccessLogMode {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "cached_only")]
    CachedOnly,
}

impl Default for AccessLogMode {
    fn default() -> Self {
        AccessLogMode::All
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            access_log_dir: PathBuf::from("/logs/access"),
            app_log_dir: PathBuf::from("/logs/app"),
            access_log_enabled: true,
            access_log_mode: AccessLogMode::default(),
            log_level: "info".to_string(),
            access_log_flush_interval: default_access_log_flush_interval(),
            access_log_buffer_size: default_access_log_buffer_size(),
        }
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolConfig {
    pub max_connections_per_ip: usize,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub dns_refresh_interval: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub connection_timeout: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub idle_timeout: Duration,
    /// Enable HTTP connection keepalive (persistent connections) (default: true)
    #[serde(default = "default_keepalive_enabled")]
    pub keepalive_enabled: bool,
    /// Maximum idle connections per host (IP address) (default: 10)
    #[serde(default = "default_max_idle_per_host")]
    pub max_idle_per_host: usize,
    /// Maximum connection lifetime (default: 300s)
    #[serde(
        default = "default_max_lifetime",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub max_lifetime: Duration,
    /// Connection pool maintenance check interval (default: 10s)
    #[serde(
        default = "default_pool_check_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub pool_check_interval: Duration,
    /// Custom DNS servers for S3 endpoint resolution (default: Google DNS + Cloudflare DNS)
    /// Format: list of IP addresses, e.g., ["8.8.8.8", "1.1.1.1"]
    /// The proxy bypasses /etc/hosts and uses these servers to resolve S3 endpoints
    #[serde(default = "default_dns_servers")]
    pub dns_servers: Vec<String>,
    /// Static hostname-to-IP mappings that bypass DNS resolution.
    /// Useful for S3 PrivateLink where the proxy cannot use DNS to resolve S3 endpoints
    /// to PrivateLink ENI IPs (e.g., on-prem deployments without Route 53 Resolver).
    /// Format: map of hostname to list of IP addresses.
    /// Example: {"s3.us-west-2.amazonaws.com": ["10.0.1.100", "10.0.2.100"]}
    #[serde(default)]
    pub endpoint_overrides: std::collections::HashMap<String, Vec<String>>,
}

fn default_dns_servers() -> Vec<String> {
    Vec::new() // Empty means use default (Google DNS + Cloudflare DNS)
}

fn default_keepalive_enabled() -> bool {
    true
}

fn default_max_idle_per_host() -> usize {
    10
}

fn default_max_lifetime() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_pool_check_interval() -> Duration {
    Duration::from_secs(10) // 10 seconds
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_ip: 10,
            dns_refresh_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            keepalive_enabled: default_keepalive_enabled(),
            max_idle_per_host: default_max_idle_per_host(),
            max_lifetime: default_max_lifetime(),
            pool_check_interval: default_pool_check_interval(),
            dns_servers: default_dns_servers(),
            endpoint_overrides: std::collections::HashMap::new(),
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub threshold: usize,
    pub preferred_algorithm: CompressionAlgorithm,
    pub content_aware: bool,
}

/// Compression algorithms
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "lz4")]
    Lz4,
    // Future algorithms can be added here
    // #[serde(rename = "zstd")]
    // Zstd,
    // #[serde(rename = "brotli")]
    // Brotli,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        CompressionAlgorithm::Lz4
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: 1024, // 1KB
            preferred_algorithm: CompressionAlgorithm::default(),
            content_aware: true,
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub port: u16,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub check_interval: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/health".to_string(),
            port: 8080,
            check_interval: Duration::from_secs(30),
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub port: u16,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub collection_interval: Duration,
    pub include_cache_stats: bool,
    pub include_compression_stats: bool,
    pub include_connection_stats: bool,
    pub otlp: OtlpConfig,
}

/// OTLP (OpenTelemetry Protocol) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpConfig {
    pub enabled: bool,
    pub endpoint: String,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub export_interval: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub timeout: Duration,
    pub headers: std::collections::HashMap<String, String>,
    pub compression: OtlpCompression,
}

/// OTLP compression options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OtlpCompression {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "gzip")]
    Gzip,
}

impl Default for OtlpCompression {
    fn default() -> Self {
        OtlpCompression::None
    }
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4318".to_string(),
            export_interval: Duration::from_secs(60),
            timeout: Duration::from_secs(10),
            headers: std::collections::HashMap::new(),
            compression: OtlpCompression::default(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".to_string(),
            port: 9090,
            collection_interval: Duration::from_secs(60),
            include_cache_stats: true,
            include_compression_stats: true,
            include_connection_stats: true,
            otlp: OtlpConfig::default(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                http_port: 80,
                https_port: 443,
                max_concurrent_requests: 200,
                request_timeout: Duration::from_secs(30),
                add_referer_header: true,
            },
            cache: CacheConfig {
                cache_dir: PathBuf::from("/cache"),
                max_cache_size: 10 * 1024 * 1024 * 1024, // 10GB
                ram_cache_enabled: false,
                max_ram_cache_size: 256 * 1024 * 1024, // 256MB
                eviction_algorithm: EvictionAlgorithm::default(), // LRU for both RAM and disk
                write_cache_enabled: true, // Enabled by default - write caching is complete
                write_cache_percent: 10.0,
                write_cache_max_object_size: 256 * 1024 * 1024, // 256MB
                put_ttl: Duration::from_secs(3600),             // 1 hour
                get_ttl: Duration::from_secs(315360000),        // ~10 years (infinite caching)
                head_ttl: Duration::from_secs(60),              // 1 minute
                actively_remove_cached_data: false,             // Lazy expiration by default
                shared_storage: SharedStorageConfig::default(), // Disabled by default (single-instance mode)
                download_coordination: DownloadCoordinationConfig::default(), // Enabled by default
                range_merge_gap_threshold: 1024 * 1024,         // 1MiB
                eviction_buffer_percent: 5,                     // 5% buffer
                ram_cache_flush_interval: Duration::from_secs(60), // 60 seconds
                ram_cache_flush_threshold: 100,                 // 100 pending updates
                ram_cache_flush_on_eviction: false,             // Do not flush on eviction
                ram_cache_verification_interval: Duration::from_secs(1), // 1 second
                incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
                initialization: InitializationConfig::default(), // Default initialization config
                cache_bypass_headers_enabled: true,             // Enabled by default
                metadata_cache: MetadataCacheConfig::default(), // RAM metadata cache config
                eviction_trigger_percent: 95,                   // Trigger eviction at 95% capacity
                eviction_target_percent: 80,                    // Reduce to 80% after eviction
                full_object_check_threshold: 67_108_864,        // 64 MiB
                disk_streaming_threshold: 1_048_576,            // 1 MiB
                read_cache_enabled: true,                       // Read caching enabled by default
                bucket_settings_staleness_threshold: Duration::from_secs(60), // 60 seconds
            },
            logging: LoggingConfig {
                access_log_dir: PathBuf::from("/logs/access"),
                app_log_dir: PathBuf::from("/logs/app"),
                access_log_enabled: true,
                access_log_mode: AccessLogMode::default(),
                log_level: "info".to_string(),
                access_log_flush_interval: default_access_log_flush_interval(),
                access_log_buffer_size: default_access_log_buffer_size(),
            },
            connection_pool: ConnectionPoolConfig {
                max_connections_per_ip: 10,
                dns_refresh_interval: Duration::from_secs(60), // 1 minute
                connection_timeout: Duration::from_secs(10),
                idle_timeout: Duration::from_secs(60),
                keepalive_enabled: true,
                max_idle_per_host: 1,
                max_lifetime: Duration::from_secs(300), // 5 minutes
                pool_check_interval: Duration::from_secs(10), // 10 seconds
                dns_servers: Vec::new(),                // Default: Google DNS + Cloudflare DNS
                endpoint_overrides: std::collections::HashMap::new(),
            },
            compression: CompressionConfig {
                enabled: true,
                threshold: 1024, // 1KB
                preferred_algorithm: CompressionAlgorithm::default(),
                content_aware: true,
            },
            health: HealthConfig {
                enabled: true,
                endpoint: "/health".to_string(),
                port: 8080,
                check_interval: Duration::from_secs(30),
            },
            metrics: MetricsConfig {
                enabled: true,
                endpoint: "/metrics".to_string(),
                port: 8080,
                collection_interval: Duration::from_secs(60),
                include_cache_stats: true,
                include_compression_stats: true,
                include_connection_stats: true,
                otlp: OtlpConfig {
                    enabled: false,
                    endpoint: "http://localhost:4318".to_string(),
                    export_interval: Duration::from_secs(60),
                    timeout: Duration::from_secs(10),
                    headers: std::collections::HashMap::new(),
                    compression: OtlpCompression::default(),
                },
            },
            dashboard: DashboardConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from file, environment, and command line
    pub fn load() -> Result<Self> {
        let matches = Self::build_cli().get_matches();

        let mut config = Self::default();

        // Load from config file if specified
        if let Some(config_path) = matches.get_one::<String>("config") {
            config = Self::load_from_file(config_path)?;
        }

        // Override with environment variables
        config.apply_env_overrides();

        // Override with command line arguments
        config.apply_cli_overrides(&matches);

        // Validate shared storage configuration
        if let Err(e) = config.cache.shared_storage.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid shared storage configuration: {}",
                e
            )));
        }

        // Validate connection pool configuration
        if let Err(e) = config.connection_pool.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid connection pool configuration: {}",
                e
            )));
        }

        // Validate initialization configuration
        if let Err(e) = config.cache.initialization.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid initialization configuration: {}",
                e
            )));
        }

        // Validate dashboard configuration
        if let Err(e) = config.dashboard.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid dashboard configuration: {}",
                e
            )));
        }

        // Validate and clamp download coordination configuration
        config.cache.download_coordination.validate_and_clamp();

        // Disable RAM cache when get_ttl is zero (every request must validate against S3)
        // RAM cache has no TTL check on the hit path, so it would serve stale data.
        // MetadataCache (for NewCacheMetadata objects) remains active regardless.
        if config.cache.get_ttl.is_zero() && config.cache.ram_cache_enabled {
            info!(
                "RAM cache auto-disabled: get_ttl=0 requires every request to validate against S3"
            );
            config.cache.ram_cache_enabled = false;
        }

        // Validate RAM cache flush configuration
        config.validate_ram_cache_flush_config();

        // Validate eviction threshold configuration
        // Requirements: 3.5, 3.6, 3.7, 3.8, 3.9
        config.validate_eviction_thresholds();

        // Log shared storage configuration (always enabled)
        info!(
            "Shared storage mode: lock_timeout={}s, lock_refresh={}s, recovery_max_concurrent={}",
            config.cache.shared_storage.lock_timeout.as_secs(),
            config.cache.shared_storage.lock_refresh_interval.as_secs(),
            config.cache.shared_storage.recovery_max_concurrent
        );
        info!(
            "Shared storage coordination: consolidation_interval={}s, validation_frequency={}h, lock_max_retries={}",
            config.cache.shared_storage.consolidation_interval.as_secs(),
            config.cache.shared_storage.validation_frequency.as_secs() / 3600,
            config.cache.shared_storage.lock_max_retries
        );
        info!(
            "Shared storage thresholds: validation_warn={}%, validation_error={}%",
            config.cache.shared_storage.validation_threshold_warn,
            config.cache.shared_storage.validation_threshold_error
        );
        // Requirement: 6.5 - THE system SHALL log the configured lock timeout at startup
        info!(
            "Distributed eviction lock: timeout={}s (stale locks will be forcibly acquired after this duration)",
            config.cache.shared_storage.eviction_lock_timeout.as_secs()
        );

        // Log connection pool configuration
        if config.connection_pool.keepalive_enabled {
            info!(
                "Connection keepalive enabled (idle_timeout: {}s, max_idle_per_host: {}, max_lifetime: {}s)",
                config.connection_pool.idle_timeout.as_secs(),
                config.connection_pool.max_idle_per_host,
                config.connection_pool.max_lifetime.as_secs()
            );
        } else {
            info!("Connection keepalive disabled");
        }

        // Log initialization configuration
        info!(
            "Cache initialization config: parallel_scan={}, scan_timeout={}s, progress_logging={}",
            config.cache.initialization.parallel_scan,
            config.cache.initialization.scan_timeout.as_secs(),
            config.cache.initialization.progress_logging
        );

        // Log dashboard configuration
        if config.dashboard.enabled {
            info!(
                "Dashboard enabled on {}:{} (cache_stats_refresh: {}s, logs_refresh: {}s, max_log_entries: {})",
                config.dashboard.bind_address,
                config.dashboard.port,
                config.dashboard.cache_stats_refresh_interval.as_secs(),
                config.dashboard.logs_refresh_interval.as_secs(),
                config.dashboard.max_log_entries
            );
        } else {
            info!("Dashboard disabled");
        }

        // Log download coordination configuration
        if config.cache.download_coordination.enabled {
            info!(
                "Download coordination enabled (wait_timeout: {}s)",
                config.cache.download_coordination.wait_timeout_secs
            );
        } else {
            info!("Download coordination disabled");
        }

        info!("Configuration loaded successfully");
        debug!("Configuration: {:?}", config);

        Ok(config)
    }

    /// Build CLI argument parser
    fn build_cli() -> Command {
        Command::new("s3-proxy")
            .version(env!("CARGO_PKG_VERSION"))
            .about("High-performance S3 proxy with intelligent caching")
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_name("FILE")
                    .help("Configuration file path"),
            )
            .arg(
                Arg::new("http-port")
                    .long("http-port")
                    .value_name("PORT")
                    .help("HTTP port (default: 80)"),
            )
            .arg(
                Arg::new("https-port")
                    .long("https-port")
                    .value_name("PORT")
                    .help("HTTPS port (default: 443)"),
            )
            .arg(
                Arg::new("https-mode")
                    .long("https-mode")
                    .value_name("MODE")
                    .help("HTTPS mode: passthrough (default and only option)"),
            )
            .arg(
                Arg::new("cache-dir")
                    .long("cache-dir")
                    .value_name("DIR")
                    .help("Cache directory path"),
            )
            .arg(
                Arg::new("max-concurrent-requests")
                    .long("max-concurrent-requests")
                    .value_name("COUNT")
                    .help("Maximum number of concurrent requests"),
            )
            .arg(
                Arg::new("ram-cache-enabled")
                    .long("ram-cache-enabled")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable RAM cache"),
            )
            .arg(
                Arg::new("health-enabled")
                    .long("health-enabled")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable health checks"),
            )
            .arg(
                Arg::new("metrics-enabled")
                    .long("metrics-enabled")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable metrics collection"),
            )
            .arg(
                Arg::new("otlp-endpoint")
                    .long("otlp-endpoint")
                    .value_name("URL")
                    .help("OTLP endpoint for metrics export (e.g., http://localhost:4318)"),
            )
            .arg(
                Arg::new("otlp-export-interval")
                    .long("otlp-export-interval")
                    .value_name("SECONDS")
                    .help("OTLP export interval in seconds (default: 60)"),
            )
            .arg(
                Arg::new("write-cache-enabled")
                    .long("write-cache-enabled")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable write-through caching"),
            )
            .arg(
                Arg::new("parallel-scan")
                    .long("parallel-scan")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable parallel metadata processing during initialization"),
            )
            .arg(
                Arg::new("no-parallel-scan")
                    .long("no-parallel-scan")
                    .action(clap::ArgAction::SetTrue)
                    .help("Disable parallel metadata processing during initialization"),
            )
            .arg(
                Arg::new("scan-timeout")
                    .long("scan-timeout")
                    .value_name("SECONDS")
                    .help("Timeout for directory scanning operations (default: 30)"),
            )
            .arg(
                Arg::new("progress-logging")
                    .long("progress-logging")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable detailed progress logging during initialization"),
            )
            .arg(
                Arg::new("no-progress-logging")
                    .long("no-progress-logging")
                    .action(clap::ArgAction::SetTrue)
                    .help("Disable detailed progress logging during initialization"),
            )
            .arg(
                Arg::new("dashboard-enabled")
                    .long("dashboard-enabled")
                    .action(clap::ArgAction::SetTrue)
                    .help("Enable dashboard server"),
            )
            .arg(
                Arg::new("dashboard-disabled")
                    .long("dashboard-disabled")
                    .action(clap::ArgAction::SetTrue)
                    .help("Disable dashboard server"),
            )
            .arg(
                Arg::new("dashboard-port")
                    .long("dashboard-port")
                    .value_name("PORT")
                    .help("Dashboard server port (default: 8080)"),
            )
            .arg(
                Arg::new("dashboard-bind-address")
                    .long("dashboard-bind-address")
                    .value_name("ADDRESS")
                    .help("Dashboard server bind address (default: 0.0.0.0)"),
            )
            // Shared storage configuration CLI arguments
            .arg(
                Arg::new("lock-timeout")
                    .long("lock-timeout")
                    .value_name("SECONDS")
                    .help("Lock timeout in seconds (default: 60)"),
            )
            .arg(
                Arg::new("lock-refresh-interval")
                    .long("lock-refresh-interval")
                    .value_name("SECONDS")
                    .help("Lock refresh interval in seconds (default: 30)"),
            )
            .arg(
                Arg::new("validation-threshold-warn")
                    .long("validation-threshold-warn")
                    .value_name("PERCENT")
                    .help("Warning threshold for validation discrepancies (default: 5.0)"),
            )
            .arg(
                Arg::new("validation-threshold-error")
                    .long("validation-threshold-error")
                    .value_name("PERCENT")
                    .help("Error threshold for validation discrepancies (default: 20.0)"),
            )
            .arg(
                Arg::new("recovery-max-concurrent")
                    .long("recovery-max-concurrent")
                    .value_name("COUNT")
                    .help("Maximum concurrent recovery operations (default: 4)"),
            )
            .arg(
                Arg::new("consolidation-interval")
                    .long("consolidation-interval")
                    .value_name("SECONDS")
                    .help("Journal consolidation interval in seconds (default: 30)"),
            )
            .arg(
                Arg::new("consolidation-size-threshold")
                    .long("consolidation-size-threshold")
                    .value_name("BYTES")
                    .help("Journal size threshold for immediate consolidation in bytes (default: 1048576)"),
            )
            .arg(
                Arg::new("lock-max-retries")
                    .long("lock-max-retries")
                    .value_name("COUNT")
                    .help("Maximum lock acquisition retries (default: 5)"),
            )
            .arg(
                Arg::new("validation-frequency")
                    .long("validation-frequency")
                    .value_name("HOURS")
                    .help("Metadata validation frequency in hours (default: 23)"),
            )
            .arg(
                Arg::new("eviction-lock-timeout")
                    .long("eviction-lock-timeout")
                    .value_name("SECONDS")
                    .help("Distributed eviction lock timeout in seconds (default: 60, range: 30-3600)"),
            )
    }

    /// Load configuration from YAML file
    fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            ProxyError::ConfigError(format!("Failed to read config file {}: {}", path, e))
        })?;

        let config: Self = serde_yaml::from_str(&content).map_err(|e| {
            ProxyError::ConfigError(format!("Failed to parse config file {}: {}", path, e))
        })?;

        info!("Configuration loaded from file: {}", path);
        Ok(config)
    }

    /// Apply environment variable overrides
    pub fn apply_env_overrides(&mut self) {
        if let Ok(port) = std::env::var("HTTP_PORT") {
            if let Ok(port) = port.parse() {
                self.server.http_port = port;
            }
        }

        if let Ok(port) = std::env::var("HTTPS_PORT") {
            if let Ok(port) = port.parse() {
                self.server.https_port = port;
            }
        }

        // HTTPS mode is always passthrough - no configuration needed

        if let Ok(cache_dir) = std::env::var("CACHE_DIR") {
            self.cache.cache_dir = PathBuf::from(cache_dir);
        }

        if let Ok(access_log_dir) = std::env::var("ACCESS_LOG_DIR") {
            self.logging.access_log_dir = PathBuf::from(access_log_dir);
        }

        if let Ok(app_log_dir) = std::env::var("APP_LOG_DIR") {
            self.logging.app_log_dir = PathBuf::from(app_log_dir);
        }

        if let Ok(max_requests) = std::env::var("MAX_CONCURRENT_REQUESTS") {
            if let Ok(max_requests) = max_requests.parse() {
                self.server.max_concurrent_requests = max_requests;
            }
        }

        if let Ok(ram_cache) = std::env::var("RAM_CACHE_ENABLED") {
            self.cache.ram_cache_enabled = ram_cache.to_lowercase() == "true";
        }

        if let Ok(compression) = std::env::var("COMPRESSION_ENABLED") {
            self.compression.enabled = compression.to_lowercase() == "true";
        }

        if let Ok(health) = std::env::var("HEALTH_ENABLED") {
            self.health.enabled = health.to_lowercase() == "true";
        }

        if let Ok(metrics) = std::env::var("METRICS_ENABLED") {
            self.metrics.enabled = metrics.to_lowercase() == "true";
        }

        if let Ok(otlp_endpoint) = std::env::var("OTLP_ENDPOINT") {
            self.metrics.otlp.endpoint = otlp_endpoint;
            self.metrics.otlp.enabled = true;
        }

        if let Ok(otlp_interval) = std::env::var("OTLP_EXPORT_INTERVAL") {
            if let Ok(interval) = otlp_interval.parse::<u64>() {
                self.metrics.otlp.export_interval = Duration::from_secs(interval);
            }
        }

        if let Ok(log_level) = std::env::var("LOG_LEVEL") {
            self.logging.log_level = log_level;
        }

        if let Ok(write_cache) = std::env::var("WRITE_CACHE_ENABLED") {
            self.cache.write_cache_enabled = write_cache.to_lowercase() == "true";
        }

        // Initialization configuration environment overrides
        if let Ok(parallel_scan) = std::env::var("PARALLEL_SCAN") {
            self.cache.initialization.parallel_scan = parallel_scan.to_lowercase() == "true";
        }

        if let Ok(scan_timeout) = std::env::var("SCAN_TIMEOUT") {
            // Parse duration manually since parse_duration is private
            if let Ok(timeout_secs) = scan_timeout.parse::<u64>() {
                self.cache.initialization.scan_timeout = Duration::from_secs(timeout_secs);
            }
        }

        if let Ok(progress_logging) = std::env::var("PROGRESS_LOGGING") {
            self.cache.initialization.progress_logging = progress_logging.to_lowercase() == "true";
        }

        // Dashboard configuration environment overrides
        if let Ok(dashboard_enabled) = std::env::var("DASHBOARD_ENABLED") {
            self.dashboard.enabled = dashboard_enabled.to_lowercase() == "true";
        }

        if let Ok(dashboard_port) = std::env::var("DASHBOARD_PORT") {
            if let Ok(port) = dashboard_port.parse::<u16>() {
                self.dashboard.port = port;
            }
        }

        if let Ok(dashboard_bind_address) = std::env::var("DASHBOARD_BIND_ADDRESS") {
            self.dashboard.bind_address = dashboard_bind_address;
        }

        if let Ok(cache_stats_interval) = std::env::var("DASHBOARD_CACHE_STATS_REFRESH_INTERVAL") {
            if let Ok(interval_secs) = cache_stats_interval.parse::<u64>() {
                self.dashboard.cache_stats_refresh_interval = Duration::from_secs(interval_secs);
            }
        }

        if let Ok(logs_interval) = std::env::var("DASHBOARD_LOGS_REFRESH_INTERVAL") {
            if let Ok(interval_secs) = logs_interval.parse::<u64>() {
                self.dashboard.logs_refresh_interval = Duration::from_secs(interval_secs);
            }
        }

        if let Ok(max_log_entries) = std::env::var("DASHBOARD_MAX_LOG_ENTRIES") {
            if let Ok(entries) = max_log_entries.parse::<usize>() {
                self.dashboard.max_log_entries = entries;
            }
        }

        // Shared storage configuration environment overrides
        if let Ok(timeout) = std::env::var("LOCK_TIMEOUT") {
            if let Ok(timeout_secs) = timeout.parse::<u64>() {
                self.cache.shared_storage.lock_timeout = Duration::from_secs(timeout_secs);
            }
        }

        if let Ok(interval) = std::env::var("LOCK_REFRESH_INTERVAL") {
            if let Ok(interval_secs) = interval.parse::<u64>() {
                self.cache.shared_storage.lock_refresh_interval =
                    Duration::from_secs(interval_secs);
            }
        }

        if let Ok(threshold_warn) = std::env::var("VALIDATION_THRESHOLD_WARN") {
            if let Ok(threshold) = threshold_warn.parse::<f64>() {
                self.cache.shared_storage.validation_threshold_warn = threshold;
            }
        }

        if let Ok(threshold_error) = std::env::var("VALIDATION_THRESHOLD_ERROR") {
            if let Ok(threshold) = threshold_error.parse::<f64>() {
                self.cache.shared_storage.validation_threshold_error = threshold;
            }
        }

        if let Ok(concurrent) = std::env::var("RECOVERY_MAX_CONCURRENT") {
            if let Ok(concurrent_count) = concurrent.parse::<usize>() {
                self.cache.shared_storage.recovery_max_concurrent = concurrent_count;
            }
        }

        if let Ok(interval) = std::env::var("CONSOLIDATION_INTERVAL") {
            if let Ok(interval_secs) = interval.parse::<u64>() {
                self.cache.shared_storage.consolidation_interval =
                    Duration::from_secs(interval_secs);
            }
        }

        if let Ok(threshold) = std::env::var("CONSOLIDATION_SIZE_THRESHOLD") {
            if let Ok(threshold_bytes) = threshold.parse::<u64>() {
                self.cache.shared_storage.consolidation_size_threshold = threshold_bytes;
            }
        }

        if let Ok(retries) = std::env::var("LOCK_MAX_RETRIES") {
            if let Ok(retries_count) = retries.parse::<u32>() {
                self.cache.shared_storage.lock_max_retries = retries_count;
            }
        }

        if let Ok(frequency) = std::env::var("VALIDATION_FREQUENCY") {
            if let Ok(frequency_hours) = frequency.parse::<u64>() {
                self.cache.shared_storage.validation_frequency =
                    Duration::from_secs(frequency_hours * 3600);
            }
        }

        // Eviction lock timeout environment override
        if let Ok(timeout) = std::env::var("EVICTION_LOCK_TIMEOUT") {
            if let Ok(timeout_secs) = timeout.parse::<u64>() {
                self.cache.shared_storage.eviction_lock_timeout = Duration::from_secs(timeout_secs);
            }
        }
    }

    /// Apply command line argument overrides
    fn apply_cli_overrides(&mut self, matches: &clap::ArgMatches) {
        if let Some(port) = matches.get_one::<String>("http-port") {
            if let Ok(port) = port.parse() {
                self.server.http_port = port;
            }
        }

        if let Some(port) = matches.get_one::<String>("https-port") {
            if let Ok(port) = port.parse() {
                self.server.https_port = port;
            }
        }

        // HTTPS mode is always passthrough - no CLI argument needed

        if let Some(cache_dir) = matches.get_one::<String>("cache-dir") {
            self.cache.cache_dir = PathBuf::from(cache_dir);
        }

        if let Some(max_requests) = matches.get_one::<String>("max-concurrent-requests") {
            if let Ok(max_requests) = max_requests.parse() {
                self.server.max_concurrent_requests = max_requests;
            }
        }

        if matches.get_flag("ram-cache-enabled") {
            self.cache.ram_cache_enabled = true;
        }

        if matches.get_flag("health-enabled") {
            self.health.enabled = true;
        }

        if matches.get_flag("metrics-enabled") {
            self.metrics.enabled = true;
        }

        if let Some(otlp_endpoint) = matches.get_one::<String>("otlp-endpoint") {
            self.metrics.otlp.endpoint = otlp_endpoint.clone();
            self.metrics.otlp.enabled = true;
        }

        if let Some(otlp_interval) = matches.get_one::<String>("otlp-export-interval") {
            if let Ok(interval) = otlp_interval.parse::<u64>() {
                self.metrics.otlp.export_interval = Duration::from_secs(interval);
            }
        }

        if matches.get_flag("write-cache-enabled") {
            self.cache.write_cache_enabled = true;
        }

        // Initialization configuration CLI overrides
        if matches.get_flag("parallel-scan") {
            self.cache.initialization.parallel_scan = true;
        }
        if matches.get_flag("no-parallel-scan") {
            self.cache.initialization.parallel_scan = false;
        }

        if let Some(scan_timeout) = matches.get_one::<String>("scan-timeout") {
            if let Ok(timeout_secs) = scan_timeout.parse::<u64>() {
                self.cache.initialization.scan_timeout = Duration::from_secs(timeout_secs);
            }
        }

        if matches.get_flag("progress-logging") {
            self.cache.initialization.progress_logging = true;
        }
        if matches.get_flag("no-progress-logging") {
            self.cache.initialization.progress_logging = false;
        }

        // Dashboard configuration CLI overrides
        if matches.get_flag("dashboard-enabled") {
            self.dashboard.enabled = true;
        }
        if matches.get_flag("dashboard-disabled") {
            self.dashboard.enabled = false;
        }

        if let Some(dashboard_port) = matches.get_one::<String>("dashboard-port") {
            if let Ok(port) = dashboard_port.parse::<u16>() {
                self.dashboard.port = port;
            }
        }

        if let Some(dashboard_bind_address) = matches.get_one::<String>("dashboard-bind-address") {
            self.dashboard.bind_address = dashboard_bind_address.clone();
        }

        // Shared storage configuration CLI overrides
        if let Some(timeout) = matches.get_one::<String>("lock-timeout") {
            if let Ok(timeout_secs) = timeout.parse::<u64>() {
                self.cache.shared_storage.lock_timeout = Duration::from_secs(timeout_secs);
            }
        }

        if let Some(interval) = matches.get_one::<String>("lock-refresh-interval") {
            if let Ok(interval_secs) = interval.parse::<u64>() {
                self.cache.shared_storage.lock_refresh_interval =
                    Duration::from_secs(interval_secs);
            }
        }

        if let Some(threshold_warn) = matches.get_one::<String>("validation-threshold-warn") {
            if let Ok(threshold) = threshold_warn.parse::<f64>() {
                self.cache.shared_storage.validation_threshold_warn = threshold;
            }
        }

        if let Some(threshold_error) = matches.get_one::<String>("validation-threshold-error") {
            if let Ok(threshold) = threshold_error.parse::<f64>() {
                self.cache.shared_storage.validation_threshold_error = threshold;
            }
        }

        if let Some(concurrent) = matches.get_one::<String>("recovery-max-concurrent") {
            if let Ok(concurrent_count) = concurrent.parse::<usize>() {
                self.cache.shared_storage.recovery_max_concurrent = concurrent_count;
            }
        }

        if let Some(interval) = matches.get_one::<String>("consolidation-interval") {
            if let Ok(interval_secs) = interval.parse::<u64>() {
                self.cache.shared_storage.consolidation_interval =
                    Duration::from_secs(interval_secs);
            }
        }

        if let Some(threshold) = matches.get_one::<String>("consolidation-size-threshold") {
            if let Ok(threshold_bytes) = threshold.parse::<u64>() {
                self.cache.shared_storage.consolidation_size_threshold = threshold_bytes;
            }
        }

        if let Some(retries) = matches.get_one::<String>("lock-max-retries") {
            if let Ok(retries_count) = retries.parse::<u32>() {
                self.cache.shared_storage.lock_max_retries = retries_count;
            }
        }

        if let Some(frequency) = matches.get_one::<String>("validation-frequency") {
            if let Ok(frequency_hours) = frequency.parse::<u64>() {
                self.cache.shared_storage.validation_frequency =
                    Duration::from_secs(frequency_hours * 3600);
            }
        }

        if let Some(timeout) = matches.get_one::<String>("eviction-lock-timeout") {
            if let Ok(timeout_secs) = timeout.parse::<u64>() {
                self.cache.shared_storage.eviction_lock_timeout = Duration::from_secs(timeout_secs);
            }
        }
    }

    /// Validate and correct RAM cache flush configuration
    fn validate_ram_cache_flush_config(&mut self) {
        // Validate flush_interval (10-600 seconds)
        let flush_interval_secs = self.cache.ram_cache_flush_interval.as_secs();
        if flush_interval_secs < 10 || flush_interval_secs > 600 {
            warn!(
                "Invalid ram_cache_flush_interval: {}s (must be 10-600s), using default 60s",
                flush_interval_secs
            );
            self.cache.ram_cache_flush_interval = Duration::from_secs(60);
        }

        // Validate flush_threshold (10-10000)
        if self.cache.ram_cache_flush_threshold < 10 || self.cache.ram_cache_flush_threshold > 10000
        {
            warn!(
                "Invalid ram_cache_flush_threshold: {} (must be 10-10000), using default 100",
                self.cache.ram_cache_flush_threshold
            );
            self.cache.ram_cache_flush_threshold = 100;
        }

        // Validate verification_interval (1-60 seconds)
        let verification_interval_secs = self.cache.ram_cache_verification_interval.as_secs();
        if verification_interval_secs < 1 || verification_interval_secs > 60 {
            warn!(
                "Invalid ram_cache_verification_interval: {}s (must be 1-60s), using default 1s",
                verification_interval_secs
            );
            self.cache.ram_cache_verification_interval = Duration::from_secs(1);
        }

        // Log configuration
        if self.cache.ram_cache_enabled {
            info!(
                "RAM cache flush config: interval={}s, threshold={}, flush_on_eviction={}, verification_interval={}s",
                self.cache.ram_cache_flush_interval.as_secs(),
                self.cache.ram_cache_flush_threshold,
                self.cache.ram_cache_flush_on_eviction,
                self.cache.ram_cache_verification_interval.as_secs()
            );
        }

        // Validate incomplete_upload_ttl (1 hour to 7 days)
        let incomplete_upload_ttl_secs = self.cache.incomplete_upload_ttl.as_secs();
        let one_hour = 3600u64;
        let seven_days = 7 * 24 * 3600u64;
        if incomplete_upload_ttl_secs < one_hour || incomplete_upload_ttl_secs > seven_days {
            warn!(
                "Invalid incomplete_upload_ttl: {}s (must be 1h-7d), using default 1d",
                incomplete_upload_ttl_secs
            );
            self.cache.incomplete_upload_ttl = Duration::from_secs(86400); // 1 day default
        }

        // Log write cache configuration if enabled
        if self.cache.write_cache_enabled {
            info!(
                "Write cache config: percent={}%, max_object_size={}MB, put_ttl={:?}, incomplete_upload_ttl={:?}",
                self.cache.write_cache_percent,
                self.cache.write_cache_max_object_size / 1024 / 1024,
                self.cache.put_ttl,
                self.cache.incomplete_upload_ttl
            );
        }
    }

    /// Validate and correct eviction threshold configuration
    ///
    /// Requirements:
    /// - 3.5: eviction_trigger_percent must be in range 50-100
    /// - 3.6: eviction_target_percent must be in range 50-99
    /// - 3.7: If trigger is outside 50-100, log warning and clamp
    /// - 3.8: If target is outside 50-99, log warning and clamp
    /// - 3.9: If target >= trigger, log warning and set target = trigger - 10
    fn validate_eviction_thresholds(&mut self) {
        let original_trigger = self.cache.eviction_trigger_percent;
        let original_target = self.cache.eviction_target_percent;

        // Validate eviction_trigger_percent (50-100)
        // Requirement: 3.5, 3.7
        if self.cache.eviction_trigger_percent < 50 {
            warn!(
                "eviction_trigger_percent {} is below minimum 50, clamping to 50",
                original_trigger
            );
            self.cache.eviction_trigger_percent = 50;
        } else if self.cache.eviction_trigger_percent > 100 {
            warn!(
                "eviction_trigger_percent {} is above maximum 100, clamping to 100",
                original_trigger
            );
            self.cache.eviction_trigger_percent = 100;
        }

        // Validate eviction_target_percent (50-99)
        // Requirement: 3.6, 3.8
        if self.cache.eviction_target_percent < 50 {
            warn!(
                "eviction_target_percent {} is below minimum 50, clamping to 50",
                original_target
            );
            self.cache.eviction_target_percent = 50;
        } else if self.cache.eviction_target_percent > 99 {
            warn!(
                "eviction_target_percent {} is above maximum 99, clamping to 99",
                original_target
            );
            self.cache.eviction_target_percent = 99;
        }

        // Validate target < trigger
        // Requirement: 3.9
        if self.cache.eviction_target_percent >= self.cache.eviction_trigger_percent {
            let new_target = self.cache.eviction_trigger_percent.saturating_sub(10).max(50);
            warn!(
                "eviction_target_percent ({}) must be less than eviction_trigger_percent ({}), setting target to {}",
                self.cache.eviction_target_percent,
                self.cache.eviction_trigger_percent,
                new_target
            );
            self.cache.eviction_target_percent = new_target;
        }

        // Log final eviction threshold configuration
        info!(
            "Eviction thresholds: trigger={}%, target={}%",
            self.cache.eviction_trigger_percent, self.cache.eviction_target_percent
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn test_duration_parsing_seconds() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "3600s"
  get_ttl: "315360000s"
  head_ttl: "3600s"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "60s"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "30s"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "60s"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "60s"
    timeout: "10s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");

        assert_eq!(config.server.request_timeout, Duration::from_secs(30));
        assert_eq!(config.cache.put_ttl, Duration::from_secs(3600));
        assert_eq!(config.cache.get_ttl, Duration::from_secs(315360000)); // ~10 years
        assert_eq!(
            config.connection_pool.dns_refresh_interval,
            Duration::from_secs(60)
        );
        assert_eq!(
            config.connection_pool.connection_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(config.connection_pool.idle_timeout, Duration::from_secs(60));
        assert_eq!(config.health.check_interval, Duration::from_secs(30));
        assert_eq!(config.metrics.collection_interval, Duration::from_secs(60));
        assert_eq!(config.metrics.otlp.export_interval, Duration::from_secs(60));
        assert_eq!(config.metrics.otlp.timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_duration_parsing_minutes() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "5m"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");

        assert_eq!(config.server.request_timeout, Duration::from_secs(300)); // 5 minutes
        assert_eq!(config.cache.put_ttl, Duration::from_secs(3600)); // 1 hour
        assert_eq!(config.cache.get_ttl, Duration::from_secs(315360000)); // ~10 years
        assert_eq!(
            config.connection_pool.dns_refresh_interval,
            Duration::from_secs(60)
        ); // 1 minute
        assert_eq!(
            config.connection_pool.idle_timeout,
            Duration::from_secs(120)
        ); // 2 minutes
        assert_eq!(config.health.check_interval, Duration::from_secs(60)); // 1 minute
        assert_eq!(config.metrics.collection_interval, Duration::from_secs(300));
        // 5 minutes
    }

    #[test]
    fn test_duration_parsing_milliseconds() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "500ms"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1000ms"
  get_ttl: "2000ms"
  head_ttl: "100ms"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "100ms"
  connection_timeout: "200ms"
  idle_timeout: "300ms"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "400ms"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "500ms"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "600ms"
    timeout: "700ms"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");

        assert_eq!(config.server.request_timeout, Duration::from_millis(500));
        assert_eq!(config.cache.put_ttl, Duration::from_millis(1000));
        assert_eq!(config.cache.get_ttl, Duration::from_millis(2000));
        assert_eq!(
            config.connection_pool.dns_refresh_interval,
            Duration::from_millis(100)
        );
        assert_eq!(
            config.connection_pool.connection_timeout,
            Duration::from_millis(200)
        );
        assert_eq!(
            config.connection_pool.idle_timeout,
            Duration::from_millis(300)
        );
        assert_eq!(config.health.check_interval, Duration::from_millis(400));
        assert_eq!(
            config.metrics.collection_interval,
            Duration::from_millis(500)
        );
        assert_eq!(
            config.metrics.otlp.export_interval,
            Duration::from_millis(600)
        );
        assert_eq!(config.metrics.otlp.timeout, Duration::from_millis(700));
    }

    #[test]
    fn test_duration_parsing_invalid() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "invalid"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let result: std::result::Result<Config, serde_yaml::Error> = serde_yaml::from_str(yaml);
        assert!(
            result.is_err(),
            "Should fail to parse invalid duration string"
        );
    }

    #[test]
    fn test_duration_parsing_days() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1d"
  incomplete_upload_ttl: "7days"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");

        // 1 day = 86400 seconds
        assert_eq!(config.cache.put_ttl, Duration::from_secs(86400));
        // 7 days = 604800 seconds
        assert_eq!(
            config.cache.incomplete_upload_ttl,
            Duration::from_secs(604800)
        );
    }

    #[test]
    fn test_range_merge_gap_threshold_config() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
  range_merge_gap_threshold: 524288
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.cache.range_merge_gap_threshold, 524288); // 512KB
    }

    #[test]
    fn test_range_merge_gap_threshold_default() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        // Should use default value of 1MiB when not specified
        assert_eq!(config.cache.range_merge_gap_threshold, 1024 * 1024);
    }

    #[test]
    fn test_eviction_buffer_and_metadata_lock_defaults() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        // Should use default values when not specified
        assert_eq!(config.cache.eviction_buffer_percent, 5);
        // shared_storage.lock_timeout defaults to 60s
        assert_eq!(config.cache.shared_storage.lock_timeout.as_secs(), 60);
    }

    #[test]
    fn test_eviction_buffer_custom() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
  eviction_buffer_percent: 10
  shared_storage:
    enabled: true
    lock_timeout: "120s"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        // Should use custom values when specified
        assert_eq!(config.cache.eviction_buffer_percent, 10);
        assert_eq!(config.cache.shared_storage.lock_timeout.as_secs(), 120);
    }

    #[test]
    fn test_write_cache_enabled_default_true() {
        // Test that the default configuration has write_cache_enabled = true
        let config = Config::default();
        assert_eq!(
            config.cache.write_cache_enabled, true,
            "write_cache_enabled should default to true (write caching is complete)"
        );
    }

    #[test]
    fn test_write_cache_enabled_missing_field_defaults_to_true() {
        // Test that when write_cache_enabled is missing from YAML, it defaults to true
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(
            config.cache.write_cache_enabled, true,
            "write_cache_enabled should default to true when missing (write caching is complete)"
        );
    }

    #[test]
    fn test_write_cache_enabled_explicit_true() {
        // Test that write_cache_enabled can be explicitly set to true
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(
            config.cache.write_cache_enabled, true,
            "write_cache_enabled should be true when explicitly set"
        );
    }

    #[test]
    fn test_write_cache_enabled_explicit_false() {
        // Test that write_cache_enabled can be explicitly set to false
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: false
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(
            config.cache.write_cache_enabled, false,
            "write_cache_enabled should be false when explicitly set"
        );
    }

    #[test]
    fn test_write_cache_enabled_env_override() {
        // Test that WRITE_CACHE_ENABLED environment variable overrides file config
        std::env::set_var("WRITE_CACHE_ENABLED", "true");

        let mut config = Config::default();
        config.cache.write_cache_enabled = false; // Start with false
        config.apply_env_overrides();

        assert_eq!(
            config.cache.write_cache_enabled, true,
            "WRITE_CACHE_ENABLED=true should override to true"
        );

        std::env::set_var("WRITE_CACHE_ENABLED", "false");
        config.cache.write_cache_enabled = true; // Start with true
        config.apply_env_overrides();

        assert_eq!(
            config.cache.write_cache_enabled, false,
            "WRITE_CACHE_ENABLED=false should override to false"
        );

        // Clean up
        std::env::remove_var("WRITE_CACHE_ENABLED");
    }

    #[test]
    fn test_connection_pool_config_defaults() {
        // Test default values for ConnectionPoolConfig
        let config = ConnectionPoolConfig::default();

        assert_eq!(config.max_connections_per_ip, 10);
        assert_eq!(config.dns_refresh_interval, Duration::from_secs(60));
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.idle_timeout, Duration::from_secs(30));
        assert_eq!(config.keepalive_enabled, true);
        assert_eq!(config.max_idle_per_host, 10);
        assert_eq!(config.max_lifetime, Duration::from_secs(300));
        assert_eq!(config.pool_check_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_connection_pool_config_parsing() {
        // Test parsing ConnectionPoolConfig from YAML
        let yaml = r#"
max_connections_per_ip: 20
dns_refresh_interval: "120s"
connection_timeout: "15s"
idle_timeout: "45s"
keepalive_enabled: false
max_idle_per_host: 5
max_lifetime: "600s"
pool_check_interval: "20s"
"#;

        let config: ConnectionPoolConfig =
            serde_yaml::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert_eq!(config.max_connections_per_ip, 20);
        assert_eq!(config.dns_refresh_interval, Duration::from_secs(120));
        assert_eq!(config.connection_timeout, Duration::from_secs(15));
        assert_eq!(config.idle_timeout, Duration::from_secs(45));
        assert_eq!(config.keepalive_enabled, false);
        assert_eq!(config.max_idle_per_host, 5);
        assert_eq!(config.max_lifetime, Duration::from_secs(600));
        assert_eq!(config.pool_check_interval, Duration::from_secs(20));
    }

    #[test]
    fn test_connection_pool_config_validation() {
        // Test that configuration values are within expected ranges
        let config = ConnectionPoolConfig {
            max_connections_per_ip: 50,
            dns_refresh_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(10),
            keepalive_enabled: true,
            max_idle_per_host: 10,
            max_lifetime: Duration::from_secs(60),
            pool_check_interval: Duration::from_secs(5),
            dns_servers: Vec::new(),
            endpoint_overrides: std::collections::HashMap::new(),
        };

        // Verify values are within reasonable ranges
        assert!(config.max_connections_per_ip > 0 && config.max_connections_per_ip <= 100);
        assert!(config.dns_refresh_interval >= Duration::from_secs(10));
        assert!(config.connection_timeout >= Duration::from_secs(1));
        assert!(config.idle_timeout >= Duration::from_secs(10));
        assert!(config.max_idle_per_host > 0 && config.max_idle_per_host <= 50);
        assert!(config.max_lifetime >= Duration::from_secs(60));
        assert!(config.pool_check_interval >= Duration::from_secs(1));
    }

    #[test]
    fn test_initialization_config_defaults() {
        let config = InitializationConfig::default();

        assert_eq!(config.parallel_scan, true);
        assert_eq!(config.scan_timeout, Duration::from_secs(30));
        assert_eq!(config.progress_logging, true);
    }

    #[test]
    fn test_initialization_config_validation_valid() {
        let config = InitializationConfig {
            parallel_scan: true,
            scan_timeout: Duration::from_secs(30),
            progress_logging: true,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_initialization_config_validation_invalid_timeout() {
        // Test timeout too short
        let config = InitializationConfig {
            scan_timeout: Duration::from_secs(4),
            ..InitializationConfig::default()
        };
        assert!(config.validate().is_err());

        // Test timeout too long
        let config = InitializationConfig {
            scan_timeout: Duration::from_secs(301),
            ..InitializationConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_shared_storage_config_defaults() {
        let config = SharedStorageConfig::default();

        // Note: enabled field was removed - shared storage coordination is always enabled
        assert_eq!(config.lock_timeout, Duration::from_secs(60));
        assert_eq!(config.lock_refresh_interval, Duration::from_secs(30));
        assert_eq!(config.validation_threshold_warn, 5.0);
        assert_eq!(config.validation_threshold_error, 20.0);
    }

    #[test]
    fn test_shared_storage_config_validation_valid() {
        let config = SharedStorageConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_shared_storage_config_validation_invalid_thresholds() {
        // Test invalid warning threshold (negative)
        let config = SharedStorageConfig {
            validation_threshold_warn: -1.0,
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        // Test invalid warning threshold (> 100)
        let config = SharedStorageConfig {
            validation_threshold_warn: 101.0,
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        // Test invalid error threshold (negative)
        let config = SharedStorageConfig {
            validation_threshold_error: -1.0,
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        // Test invalid error threshold (> 100)
        let config = SharedStorageConfig {
            validation_threshold_error: 101.0,
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        // Test warning >= error threshold
        let config = SharedStorageConfig {
            validation_threshold_warn: 20.0,
            validation_threshold_error: 20.0,
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        let config = SharedStorageConfig {
            validation_threshold_warn: 25.0,
            validation_threshold_error: 20.0,
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_initialization_config_parsing() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: false
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
  initialization:
    parallel_scan: false
    scan_timeout: "60s"
    progress_logging: false
  shared_storage:
    enabled: true
    lock_timeout: "60s"
    validation_threshold_warn: 10.0
    validation_threshold_error: 30.0
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");

        assert_eq!(config.cache.initialization.parallel_scan, false);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(60)
        );
        assert_eq!(config.cache.initialization.progress_logging, false);
        assert_eq!(config.cache.shared_storage.validation_threshold_warn, 10.0);
        assert_eq!(config.cache.shared_storage.validation_threshold_error, 30.0);
    }

    #[test]
    fn test_config_loads_with_example_file() {
        // Test that the example config file loads successfully with initialization defaults
        let yaml = std::fs::read_to_string("config/config.example.yaml")
            .expect("Failed to read config/config.example.yaml");

        let config: Config =
            serde_yaml::from_str(&yaml).expect("Failed to parse config/config.example.yaml");

        // Verify initialization config has expected values from the example file
        assert_eq!(config.cache.initialization.parallel_scan, true);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(config.cache.initialization.progress_logging, true);
        // Validation thresholds are now in shared_storage
        assert_eq!(config.cache.shared_storage.validation_threshold_warn, 5.0);
        assert_eq!(config.cache.shared_storage.validation_threshold_error, 20.0);
    }

    #[test]
    fn test_initialization_config_defaults_when_missing() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: false
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");

        // Should use default values when initialization section is missing
        assert_eq!(config.cache.initialization.parallel_scan, true);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(config.cache.initialization.progress_logging, true);
        // Validation thresholds are now in shared_storage with defaults
        assert_eq!(config.cache.shared_storage.validation_threshold_warn, 5.0);
        assert_eq!(config.cache.shared_storage.validation_threshold_error, 20.0);
    }

    #[test]
    fn test_config_validation_with_initialization() {
        // Test that Config::load() validates initialization config correctly
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: false
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
  initialization:
    parallel_scan: true
    scan_timeout: "30s"
    progress_logging: true
  shared_storage:
    enabled: true
    validation_threshold_warn: 5.0
    validation_threshold_error: 20.0
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        // Write to a temporary file and test Config::load_from_file
        use std::io::Write;
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        temp_file.write_all(yaml.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config::load_from_file(temp_file.path().to_str().unwrap())
            .expect("Should load valid config successfully");

        // Verify initialization config was loaded correctly
        assert_eq!(config.cache.initialization.parallel_scan, true);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(config.cache.initialization.progress_logging, true);
        // Validation thresholds are now in shared_storage
        assert_eq!(config.cache.shared_storage.validation_threshold_warn, 5.0);
        assert_eq!(config.cache.shared_storage.validation_threshold_error, 20.0);
    }

    #[test]
    fn test_config_validation_rejects_invalid_shared_storage() {
        // Test that shared_storage config validation rejects invalid values
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: false
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
  shared_storage:
    enabled: true
    validation_threshold_warn: 25.0
    validation_threshold_error: 20.0
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
dashboard:
  enabled: true
  port: 8080
  bind_address: "0.0.0.0"
  cache_stats_refresh_interval: "5s"
  logs_refresh_interval: "10s"
  max_log_entries: 100
"#;

        // Parse the config (this should succeed)
        let config: Config = serde_yaml::from_str(yaml).expect("Should parse YAML successfully");

        // But validation should fail because validation_threshold_warn (25.0) >= validation_threshold_error (20.0)
        let result = config.cache.shared_storage.validate();
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("validation_threshold_warn"));
        assert!(error_msg.contains("must be less than"));
    }

    #[test]
    fn test_dashboard_config_defaults() {
        let config = DashboardConfig::default();

        assert_eq!(config.enabled, true);
        assert_eq!(config.port, 8081);
        assert_eq!(config.bind_address, "0.0.0.0");
        assert_eq!(config.cache_stats_refresh_interval, Duration::from_secs(5));
        assert_eq!(config.logs_refresh_interval, Duration::from_secs(10));
        assert_eq!(config.max_log_entries, 100);
    }

    #[test]
    fn test_dashboard_config_validation_valid() {
        let config = DashboardConfig {
            enabled: true,
            port: 8080,
            bind_address: "0.0.0.0".to_string(),
            cache_stats_refresh_interval: Duration::from_secs(5),
            logs_refresh_interval: Duration::from_secs(10),
            max_log_entries: 100,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_dashboard_config_validation_invalid_port() {
        // Test port too low
        let config = DashboardConfig {
            port: 1023,
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_dashboard_config_validation_invalid_bind_address() {
        // Test empty bind address
        let config = DashboardConfig {
            bind_address: "".to_string(),
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_dashboard_config_validation_invalid_intervals() {
        // Test cache stats interval too short
        let config = DashboardConfig {
            cache_stats_refresh_interval: Duration::from_secs(0),
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());

        // Test cache stats interval too long
        let config = DashboardConfig {
            cache_stats_refresh_interval: Duration::from_secs(301),
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());

        // Test logs interval too short
        let config = DashboardConfig {
            logs_refresh_interval: Duration::from_secs(0),
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());

        // Test logs interval too long
        let config = DashboardConfig {
            logs_refresh_interval: Duration::from_secs(301),
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_dashboard_config_validation_invalid_max_log_entries() {
        // Test max log entries too low
        let config = DashboardConfig {
            max_log_entries: 9,
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());

        // Test max log entries too high
        let config = DashboardConfig {
            max_log_entries: 10001,
            ..DashboardConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_dashboard_config_parsing() {
        let yaml = r#"
enabled: false
port: 9090
bind_address: "127.0.0.1"
cache_stats_refresh_interval: "3s"
logs_refresh_interval: "15s"
max_log_entries: 200
"#;

        let config: DashboardConfig =
            serde_yaml::from_str(yaml).expect("Failed to parse DashboardConfig");

        assert_eq!(config.enabled, false);
        assert_eq!(config.port, 9090);
        assert_eq!(config.bind_address, "127.0.0.1");
        assert_eq!(config.cache_stats_refresh_interval, Duration::from_secs(3));
        assert_eq!(config.logs_refresh_interval, Duration::from_secs(15));
        assert_eq!(config.max_log_entries, 200);
    }

    #[test]
    fn test_config_includes_dashboard() {
        let config = Config::default();

        // Verify dashboard is included with default values
        assert_eq!(config.dashboard.enabled, true);
        assert_eq!(config.dashboard.port, 8081);
        assert_eq!(config.dashboard.bind_address, "0.0.0.0");
    }

    #[test]
    fn test_eviction_threshold_defaults() {
        let config = CacheConfig::default();
        assert_eq!(config.eviction_trigger_percent, 95);
        assert_eq!(config.eviction_target_percent, 80);
    }

    #[test]
    fn test_eviction_threshold_validation_valid_values() {
        // Test that valid values are not modified
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 90;
        config.cache.eviction_target_percent = 70;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 90);
        assert_eq!(config.cache.eviction_target_percent, 70);
    }

    #[test]
    fn test_eviction_threshold_validation_trigger_below_min() {
        // Requirement 3.7: If trigger is below 50, clamp to 50
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 30;
        config.cache.eviction_target_percent = 20;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 50);
        // Target should also be clamped to 50 (min), then adjusted since target >= trigger
        // After clamping target to 50, target (50) >= trigger (50), so target = 50 - 10 = 40
        // But 40 < 50 (min), so it stays at 50... wait, let me re-check the logic
        // Actually target 20 gets clamped to 50 first, then 50 >= 50, so target = 50 - 10 = 40
        // But 40 is below min 50, so we use max(40, 50) = 50
        assert_eq!(config.cache.eviction_target_percent, 50);
    }

    #[test]
    fn test_eviction_threshold_validation_trigger_above_max() {
        // Requirement 3.7: If trigger is above 100, clamp to 100
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 150;
        config.cache.eviction_target_percent = 80;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 100);
        assert_eq!(config.cache.eviction_target_percent, 80);
    }

    #[test]
    fn test_eviction_threshold_validation_target_below_min() {
        // Requirement 3.8: If target is below 50, clamp to 50
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 95;
        config.cache.eviction_target_percent = 30;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 95);
        assert_eq!(config.cache.eviction_target_percent, 50);
    }

    #[test]
    fn test_eviction_threshold_validation_target_above_max() {
        // Requirement 3.8: If target is above 99, clamp to 99
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 100;
        config.cache.eviction_target_percent = 120;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 100);
        // Target clamped to 99, but 99 >= 100 is false, so no further adjustment
        // Wait, 99 < 100, so it should stay at 99
        assert_eq!(config.cache.eviction_target_percent, 99);
    }

    #[test]
    fn test_eviction_threshold_validation_target_equals_trigger() {
        // Requirement 3.9: If target >= trigger, set target = trigger - 10
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 80;
        config.cache.eviction_target_percent = 80;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 80);
        assert_eq!(config.cache.eviction_target_percent, 70); // 80 - 10 = 70
    }

    #[test]
    fn test_eviction_threshold_validation_target_greater_than_trigger() {
        // Requirement 3.9: If target >= trigger, set target = trigger - 10
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 70;
        config.cache.eviction_target_percent = 85;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 70);
        assert_eq!(config.cache.eviction_target_percent, 60); // 70 - 10 = 60
    }

    #[test]
    fn test_eviction_threshold_validation_target_adjustment_respects_min() {
        // When trigger - 10 would be below 50, use 50
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 55;
        config.cache.eviction_target_percent = 55;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 55);
        // 55 - 10 = 45, but min is 50, so target = max(45, 50) = 50
        assert_eq!(config.cache.eviction_target_percent, 50);
    }

    #[test]
    fn test_eviction_threshold_validation_boundary_values() {
        // Test boundary values: trigger=50, target=50 (both at minimum)
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 50;
        config.cache.eviction_target_percent = 50;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 50);
        // target >= trigger, so target = max(50 - 10, 50) = max(40, 50) = 50
        assert_eq!(config.cache.eviction_target_percent, 50);
    }

    #[test]
    fn test_eviction_threshold_validation_max_boundary() {
        // Test boundary values: trigger=100, target=99 (both at maximum)
        let mut config = Config::default();
        config.cache.eviction_trigger_percent = 100;
        config.cache.eviction_target_percent = 99;

        config.validate_eviction_thresholds();

        assert_eq!(config.cache.eviction_trigger_percent, 100);
        assert_eq!(config.cache.eviction_target_percent, 99);
    }

    // =========================================================================
    // Download Coordination Configuration Tests
    // =========================================================================

    #[test]
    fn test_download_coordination_default_values() {
        let config = DownloadCoordinationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.wait_timeout_secs, 30);
    }

    #[test]
    fn test_download_coordination_wait_timeout_method() {
        let config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 45,
        };
        assert_eq!(config.wait_timeout(), Duration::from_secs(45));
    }

    #[test]
    fn test_download_coordination_clamp_below_minimum() {
        // Requirement 18.5: Clamp wait_timeout_secs to 5-120 range
        let mut config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 2, // Below minimum of 5
        };
        config.validate_and_clamp();
        assert_eq!(config.wait_timeout_secs, 5);
    }

    #[test]
    fn test_download_coordination_clamp_above_maximum() {
        // Requirement 18.5: Clamp wait_timeout_secs to 5-120 range
        let mut config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 200, // Above maximum of 120
        };
        config.validate_and_clamp();
        assert_eq!(config.wait_timeout_secs, 120);
    }

    #[test]
    fn test_download_coordination_valid_values_unchanged() {
        // Values within range should not be modified
        let mut config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 60,
        };
        config.validate_and_clamp();
        assert_eq!(config.wait_timeout_secs, 60);
    }

    #[test]
    fn test_download_coordination_boundary_minimum() {
        // Exactly at minimum should not be clamped
        let mut config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 5,
        };
        config.validate_and_clamp();
        assert_eq!(config.wait_timeout_secs, 5);
    }

    #[test]
    fn test_download_coordination_boundary_maximum() {
        // Exactly at maximum should not be clamped
        let mut config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 120,
        };
        config.validate_and_clamp();
        assert_eq!(config.wait_timeout_secs, 120);
    }

    #[test]
    fn test_download_coordination_yaml_parsing() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "3600s"
  get_ttl: "315360000s"
  head_ttl: "3600s"
  download_coordination:
    enabled: false
    wait_timeout_secs: 45
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert!(!config.cache.download_coordination.enabled);
        assert_eq!(config.cache.download_coordination.wait_timeout_secs, 45);
    }

    #[test]
    fn test_download_coordination_yaml_defaults() {
        // When download_coordination is not specified, defaults should apply
        let yaml = r#"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_enabled: true
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "3600s"
  get_ttl: "315360000s"
  head_ttl: "3600s"
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert!(config.cache.download_coordination.enabled);
        assert_eq!(config.cache.download_coordination.wait_timeout_secs, 30);
    }

    #[test]
    fn test_add_referer_header_default_true() {
        let config = Config::default();
        assert_eq!(config.server.add_referer_header, true);
    }

    #[test]
    fn test_add_referer_header_missing_defaults_to_true() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.server.add_referer_header, true);
    }

    #[test]
    fn test_add_referer_header_explicit_true() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
  add_referer_header: true
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.server.add_referer_header, true);
    }

    #[test]
    fn test_add_referer_header_explicit_false() {
        let yaml = r#"
server:
  http_port: 8000
  https_port: 8443
  https_mode: "passthrough"
  max_concurrent_requests: 200
  request_timeout: "30s"
  add_referer_header: false
cache:
  cache_dir: "./cache"
  max_cache_size: 1073741824
  ram_cache_enabled: false
  max_ram_cache_size: 268435456
  eviction_algorithm: "lru"
  write_cache_percent: 10.0
  write_cache_max_object_size: 268435456
  put_ttl: "1h"
  get_ttl: "315360000s"
  head_ttl: "1h"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
  access_log_enabled: true
  access_log_mode: "all"
  log_level: "info"
connection_pool:
  max_connections_per_ip: 10
  dns_refresh_interval: "1m"
  connection_timeout: "30s"
  idle_timeout: "2m"
compression:
  enabled: true
  threshold: 1024
  preferred_algorithm: "lz4"
  content_aware: true
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "1m"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 8080
  collection_interval: "5m"
  include_cache_stats: true
  include_compression_stats: true
  include_connection_stats: true
  otlp:
    enabled: false
    endpoint: "http://localhost:4318"
    export_interval: "1m"
    timeout: "30s"
    compression: "none"
    headers: {}
"#;

        let config: Config = serde_yaml::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.server.add_referer_header, false);
    }
}

#[cfg(test)]
mod dashboard_property_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: web-dashboard, Property 12: Configuration validation and defaults**
    /// *For any* dashboard configuration with valid parameters, validation should succeed,
    /// and for any configuration with invalid parameters, validation should fail appropriately.
    /// **Validates: Requirements 5.2, 5.4**
    #[quickcheck]
    fn prop_dashboard_config_validation_and_defaults(
        enabled: bool,
        port: u16,
        bind_address: String,
        cache_stats_secs: u16,
        logs_secs: u16,
        max_entries: u16,
    ) -> TestResult {
        // Filter out completely invalid inputs
        if bind_address.is_empty() {
            return TestResult::discard();
        }

        let config = DashboardConfig {
            enabled,
            port,
            bind_address: bind_address.clone(),
            cache_stats_refresh_interval: Duration::from_secs(cache_stats_secs as u64),
            logs_refresh_interval: Duration::from_secs(logs_secs as u64),
            max_log_entries: max_entries as usize,
        };

        let validation_result = config.validate();

        // Determine if the configuration should be valid
        let should_be_valid = port >= 1024
            && !bind_address.is_empty()
            && cache_stats_secs >= 1
            && cache_stats_secs <= 300
            && logs_secs >= 1
            && logs_secs <= 300
            && max_entries >= 10
            && max_entries <= 10000;

        if should_be_valid {
            TestResult::from_bool(validation_result.is_ok())
        } else {
            TestResult::from_bool(validation_result.is_err())
        }
    }

    /// **Feature: web-dashboard, Property 12: Configuration validation and defaults**
    /// *For any* missing dashboard configuration in YAML, the system should use default values
    /// and the defaults should always pass validation.
    /// **Validates: Requirements 5.2, 5.4**
    #[quickcheck]
    fn prop_dashboard_defaults_always_valid(_seed: u8) -> TestResult {
        let default_config = DashboardConfig::default();

        // Default configuration should always be valid
        let validation_result = default_config.validate();

        // Verify specific default values per requirements
        let defaults_correct = default_config.enabled == true  // Enabled by default per requirements
            && default_config.port == 8081
            && default_config.bind_address == "0.0.0.0"
            && default_config.cache_stats_refresh_interval == Duration::from_secs(5)
            && default_config.logs_refresh_interval == Duration::from_secs(10)
            && default_config.max_log_entries == 100;

        TestResult::from_bool(validation_result.is_ok() && defaults_correct)
    }
}
