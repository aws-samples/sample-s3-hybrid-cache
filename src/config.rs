//! Configuration Module
//!
//! Handles configuration loading from files, environment variables, and command-line arguments.
//! Supports YAML configuration files and runtime configuration management.

use crate::{ProxyError, Result};
use clap::{Arg, Command};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
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
        if let Some(stripped) = path.strip_prefix("~/") {
            if let Some(home) = std::env::var_os("HOME") {
                let mut result = PathBuf::from(home);
                result.push(stripped);
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
#[serde(default)]
pub struct DashboardConfig {
    pub enabled: bool,
    pub port: u16,
    pub bind_address: String,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub cache_stats_refresh_interval: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub logs_refresh_interval: Duration,
    /// DEPRECATED: This field is not read by any logic. Log entries displayed on the dashboard
    /// are managed internally. Retained for one release cycle for backward-compatible config
    /// parsing. Will be removed in a future release.
    #[serde(default = "default_max_log_entries")]
    pub max_log_entries: usize,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,                        // Enabled by default per requirements
            port: 8081, // Changed from 8080 to avoid conflict with health server
            bind_address: default_bind_address(), // Loopback only by default
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
        if !(1..=300).contains(&cache_stats_secs) {
            return Err(format!(
                "Cache stats refresh interval must be between 1 and 300 seconds, got {}",
                cache_stats_secs
            ));
        }

        let logs_secs = self.logs_refresh_interval.as_secs();
        if !(1..=300).contains(&logs_secs) {
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
    /// Download bandwidth QoS (fairness). Disabled by default (`max_bytes_per_sec = 0`).
    #[serde(default)]
    pub download_bandwidth: DownloadBandwidthConfig,
}

/// Server operating mode
///
/// Controls which listeners are started at startup.
/// - `Standard`: HTTP (port 80) + HTTPS (port 443) + optional TLS proxy
/// - `ProxyOnly`: HTTP forward proxy (port 3128) + optional TLS proxy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum ServerMode {
    #[serde(rename = "standard")]
    #[default]
    Standard,
    #[serde(rename = "proxy_only")]
    ProxyOnly,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server operating mode (default: standard)
    #[serde(default)]
    pub mode: ServerMode,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_https_port")]
    pub https_port: u16,
    /// HTTP forward proxy port for proxy-only mode (default: 3128)
    #[serde(default = "default_proxy_port")]
    pub proxy_port: u16,
    /// Maximum number of concurrent requests, bounding the whole request
    /// including its response-body transfer.
    ///
    /// Scope: the permit is acquired as an `OwnedSemaphorePermit`
    /// (`try_acquire_owned`) at the top of `handle_request`, wrapped in `Arc`,
    /// and attached to the response body itself (`PermitBody`) — it releases
    /// only when the body has fully streamed to the client (or the connection
    /// is dropped), and a share of it is held by any background cache-commit
    /// task until that commit completes. This bounds concurrent transfers, not
    /// just concurrent request setups. (Prior to the transfer-concurrency-
    /// admission change, the permit was a borrowed `SemaphorePermit` released
    /// when `handle_request` returned — before any response body byte reached
    /// the client for a streaming response — so it bounded only the setup
    /// phase; that behaviour no longer applies.)
    ///
    /// Exceeding this limit sheds the request with HTTP 503 `SlowDown` and
    /// `Retry-After` rather than queueing it, on the basis that AWS SDKs retry 503
    /// with backoff.
    ///
    /// Default: 1000, derived from a G3 fleet measurement of
    /// Peak_Concurrent_Transfers = 71 (100-client fleet, 60s of concurrent
    /// 8 MiB range reads) with ~14x headroom — see
    /// `default_max_concurrent_requests` and
    /// the combined 2.5.0 feature, G3. Because a permit now spans
    /// the whole transfer, size this from measured `permits_held_peak`
    /// (`/metrics` → `request_metrics.permits_held_peak`) under representative
    /// load, not from request rate alone — see `docs/CONFIGURATION.md` →
    /// "What this limit actually covers".
    /// Small deployments: 50-100
    /// Medium deployments: 100-300  
    /// Large deployments: 300-1000+
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// NOT ENFORCED. Accepted and validated for backward compatibility, but no code
    /// path reads this value, so setting it has no effect on request handling.
    ///
    /// The 30s upstream request timeout that is actually applied is a separate
    /// hard-coded value in `S3Client` (`src/s3_client.rs`), which coincidentally
    /// matches this field's default. Changing this field does not change that timeout.
    ///
    /// Retained rather than removed so existing config files keep parsing (see
    /// the config compatibility rule). Do not reference it as the
    /// whole-request wall in new code or docs.
    #[serde(
        deserialize_with = "duration_serde::deserialize",
        default = "default_request_timeout"
    )]
    pub request_timeout: Duration,
    /// Enable adding Referer header for proxy identification in S3 Server Access Logs
    #[serde(default = "default_add_referer_header")]
    pub add_referer_header: bool,
    /// Deprecated compatibility field. Existing configuration files continue to
    /// parse unchanged, but the value does not control request handling.
    ///
    /// S3's own 5 GiB single-part PUT and UploadPart maximum now governs streamed
    /// uploads. This field will be removed in a future release.
    #[serde(default = "default_max_buffered_request_body_bytes")]
    pub max_buffered_request_body_bytes: u64,
    /// Cache-tee channel depth for the streaming signed-write path (default: 5 frames).
    ///
    /// On a streamed signed write (single-part PUT / UploadPart), the request body is
    /// forwarded to the upstream verbatim while a clone of each frame is tee'd to the
    /// write-through cache over a bounded `mpsc` channel of this many frames. The depth
    /// bounds the per-connection streaming buffer: at most this many frames are queued
    /// for the cache writer before backpressure stalls the next client read, so proxy
    /// memory is `O(per-connection buffer)` rather than `O(object size)`.
    ///
    /// A frame is a single forwarded request-body frame, bounded by the HTTP read
    /// buffer, so a depth of 5 keeps per-connection streaming memory on par with the GET
    /// path and independent of object size.
    ///
    /// Note on the fleet-wide bound: this depth bounds memory **per connection**,
    /// and it now *does* multiply into a sound fleet-wide ceiling for the
    /// streaming paths — `max_concurrent_requests × depth × frame` — because the
    /// concurrency permit is an owned permit attached to the response body
    /// (`PermitBody`) and held for the whole Transfer_Phase, not released at
    /// head construction (see `max_concurrent_requests`'s own doc comment).
    /// This does not cover the paths that still buffer a whole body or range in
    /// memory. Request bodies on those paths use the internal
    /// `BUFFERED_BODY_BOUND`; response bodies use their requested range size. The
    /// opt-in `max_inflight_buffer_bytes` ledger accounts for both as a separate,
    /// independent budget. Confirm the streaming-path formula against measured
    /// peak memory rather than trusting it blindly.
    /// Requirement: 9.1, 9.2, 9.3
    #[serde(default = "default_write_cache_tee_channel_depth")]
    pub write_cache_tee_channel_depth: usize,
    /// Maximum total bytes held simultaneously across all buffered request and
    /// response bodies (the Inflight_Ceiling). `0` (default) disables the
    /// accounting entirely (Ledger_Disabled) — an existing deployment gains no
    /// new rejection behaviour on upgrade without an explicit configuration
    /// change.
    ///
    /// This bounds the paths that must buffer a whole body or range (request
    /// bodies read into memory, and response bodies buffered because streaming
    /// isn't possible). It does NOT apply to the streaming paths (cache-miss
    /// GET, signed PUT/UploadPart with caching), which are already bounded per-
    /// connection independently of object size via
    /// `write_cache_tee_channel_depth`.
    ///
    /// Size this alongside `cache.max_ram_cache_size` and instance memory: the
    /// two budgets are independent and both count against RSS. A request
    /// rejected by this ceiling receives HTTP 503 `SlowDown` with
    /// `Retry-After`, which AWS SDKs retry automatically.
    ///
    /// Validated at startup: a non-zero value below the internal one-MiB
    /// buffered-request bound is rejected, since such a config guarantees every
    /// maximal buffered request body is rejected regardless of load — a
    /// misconfiguration rather than a policy.
    /// Requirement: IMA 7.1-7.5, 7.8
    #[serde(default = "default_max_inflight_buffer_bytes")]
    pub max_inflight_buffer_bytes: u64,
    /// Optional TLS proxy listener configuration
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

fn default_http_port() -> u16 {
    80
}

fn default_https_port() -> u16 {
    443
}

fn default_proxy_port() -> u16 {
    3128
}

fn default_max_concurrent_requests() -> usize {
    // Raised from 200 to 1000 for TCA (transfer-concurrency-admission): the permit
    // now spans the full response-body Transfer_Phase (and Commit_Phase), not just
    // Setup_Phase, so a permit is held far longer than before. 1000 is derived from
    // a G3 fleet measurement of Peak_Concurrent_Transfers = 71 (100-client fleet,
    // 60s of concurrent 8 MiB range reads) with ~14x headroom — see
    // the combined 2.5.0 feature, G3. Lands on this doc's own
    // "large deployment: 300-1000+" figure rather than an arbitrary multiplier.
    1000
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_add_referer_header() -> bool {
    true
}

/// Helper for deprecated boolean fields that default to true
fn default_true() -> bool {
    true
}

/// Default bind address for the health, metrics and dashboard servers: loopback only.
/// These listeners are unauthenticated, so reaching them from another host is an explicit
/// opt-in (`bind_address: "0.0.0.0"`).
fn default_bind_address() -> String {
    "127.0.0.1".to_string()
}

/// Require `value` to be an IP address (IPv4 or IPv6, as `std::net::IpAddr` parses it).
/// A hostname is not resolved: the listener binds an address, not a name.
fn validate_listener_ip(field: &str, value: &str) -> Result<()> {
    value.parse::<std::net::IpAddr>().map(|_| ()).map_err(|_| {
        ProxyError::ConfigError(format!(
            "{field} must be an IP address such as 127.0.0.1, 0.0.0.0 or ::1, got {value:?}"
        ))
    })
}

/// Default for deprecated max_log_entries field
fn default_max_log_entries() -> usize {
    100
}

/// Default for deprecated max_connections_per_ip field
fn default_max_connections_per_ip() -> usize {
    10
}

/// Default retained for the deprecated request-body compatibility field.
///
/// Existing configurations may keep `max_buffered_request_body_bytes` through
/// this release. The value is not used to select the streamed request-body cap.
fn default_max_buffered_request_body_bytes() -> u64 {
    5 * 1024 * 1024 * 1024
}

/// Default cache-tee channel depth: 5 frames.
///
/// On the streaming signed-write path, the request body is forwarded to the upstream
/// verbatim while being tee'd to the write-through cache over a bounded `mpsc` channel
/// of this many frames. The depth bounds the per-connection streaming buffer: at most
/// this many frames are queued for the cache writer before backpressure stalls the next
/// client read.
///
/// A frame is a single forwarded request-body frame, bounded by the HTTP read buffer,
/// so a depth of 5 keeps per-connection streaming memory on par with the GET path and
/// independent of object size. This is a per-connection bound only — see the field
/// doc on `write_cache_tee_channel_depth` for why `max_concurrent_requests` does not
/// multiply it into a fleet-wide ceiling.
fn default_write_cache_tee_channel_depth() -> usize {
    5
}

fn default_tls_proxy_port() -> u16 {
    3129
}

/// Default in-flight buffered-byte ceiling: `0`, meaning Ledger_Disabled.
///
/// Deferring to an operator-set value rather than deriving one from detected
/// instance memory keeps the proxy's rejection behaviour independent of the
/// host it lands on — reproducible across instance types and fleets — at the
/// cost of protecting nobody until configured. See `docs/CONFIGURATION.md` for
/// the worked sizing example.
/// Requirement: IMA 7.4
fn default_max_inflight_buffer_bytes() -> u64 {
    0
}

impl ServerConfig {
    fn max_buffered_request_body_bytes_deprecation_warning(&self) -> Option<&'static str> {
        (self.max_buffered_request_body_bytes != default_max_buffered_request_body_bytes()).then_some(
            "Configuration field 'server.max_buffered_request_body_bytes' is deprecated and ignored: \
             S3's own single-part PUT and UploadPart limits apply. \
             Delete the field to silence this warning.",
        )
    }

    /// Validate the configuration.
    ///
    /// Rejects a non-zero `max_inflight_buffer_bytes` below
    /// `BUFFERED_BODY_BOUND`: such a config guarantees every single maximal
    /// buffered request body is rejected by the Admission_Check regardless of
    /// load. `0` (Ledger_Disabled) is exempt, since it performs no
    /// Admission_Check.
    ///
    /// `max_buffered_request_body_bytes` is retained only for compatibility and
    /// has no role in validation.
    /// Requirement: 4.5
    pub fn validate(&self) -> std::result::Result<(), String> {
        let buffered_body_bound = crate::signed_request_proxy::BUFFERED_BODY_BOUND;
        if self.max_inflight_buffer_bytes > 0
            && self.max_inflight_buffer_bytes < buffered_body_bound
        {
            return Err(format!(
                "server.max_inflight_buffer_bytes ({}) is non-zero but smaller than \
                 BUFFERED_BODY_BOUND ({}); every maximal buffered request body would be \
                 rejected regardless of load. Set max_inflight_buffer_bytes to 0 to disable \
                 the ceiling, or to a value >= BUFFERED_BODY_BOUND.",
                self.max_inflight_buffer_bytes, buffered_body_bound
            ));
        }
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: ServerMode::Standard,
            http_port: default_http_port(),
            https_port: default_https_port(),
            proxy_port: default_proxy_port(),
            max_concurrent_requests: default_max_concurrent_requests(),
            request_timeout: default_request_timeout(),
            add_referer_header: default_add_referer_header(),
            max_buffered_request_body_bytes: default_max_buffered_request_body_bytes(),
            write_cache_tee_channel_depth: default_write_cache_tee_channel_depth(),
            max_inflight_buffer_bytes: default_max_inflight_buffer_bytes(),
            tls: None,
        }
    }
}

/// TLS proxy listener configuration
///
/// Configures a TLS-terminating listener that decrypts client connections
/// and feeds plaintext HTTP into the caching pipeline. Uses `tls_proxy_port`
/// to distinguish from the existing `https_port` (TCP passthrough).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TlsConfig {
    /// Whether the TLS proxy listener is active
    #[serde(default)]
    pub enabled: bool,
    /// Port for the TLS proxy listener (default: 3129)
    #[serde(default = "default_tls_proxy_port")]
    pub tls_proxy_port: u16,
    /// Path to PEM-encoded certificate chain file
    #[serde(default)]
    pub cert_path: String,
    /// Path to PEM-encoded private key file
    #[serde(default)]
    pub key_path: String,
    /// Optional hostname allowlist for CONNECT and SNI passthrough destinations.
    /// When present, only hostnames matching a pattern in this list are permitted.
    /// When absent (None), no hostname filtering is applied — only IP-range checks apply.
    /// No hardcoded default patterns are shipped; operators add their own patterns.
    #[serde(default)]
    pub connect_allowlist: Option<Vec<String>>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tls_proxy_port: default_tls_proxy_port(),
            cert_path: String::new(),
            key_path: String::new(),
            connect_allowlist: None,
        }
    }
}

impl TlsConfig {
    /// Validate the TLS configuration.
    /// When enabled, cert_path and key_path must be non-empty, and tls_proxy_port must not be 0.
    /// Requirements: 6.2, 6.3
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.enabled {
            if self.cert_path.is_empty() {
                return Err("tls.cert_path must be non-empty when tls is enabled".to_string());
            }
            if self.key_path.is_empty() {
                return Err("tls.key_path must be non-empty when tls is enabled".to_string());
            }
        }
        if self.tls_proxy_port == 0 {
            return Err("tls.tls_proxy_port must not be 0".to_string());
        }
        Ok(())
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
    /// If the fetcher takes longer than this, the waiter re-subscribes to the in-flight fetch
    /// (or becomes the fetcher if that fetch has gone), and returns 504 once
    /// `max_waiter_resubscriptions` is exceeded.
    /// Valid range: 5-120 seconds (values outside this range are clamped with a warning)
    /// Requirement: 18.2, 18.3, 18.5
    #[serde(default = "default_download_coordination_wait_timeout_secs")]
    pub wait_timeout_secs: u64,

    /// Maximum number of times a waiter will re-subscribe after timeout before
    /// returning a gateway timeout error (default: 3).
    /// When a waiter's timeout fires and the FetchGuard is still present, the waiter
    /// re-subscribes to the in-flight fetch rather than launching a duplicate S3 request.
    /// After this many re-subscriptions without receiving a result, the waiter fails
    /// with HTTP 504 Gateway Timeout.
    /// Requirement: 7.5
    #[serde(default = "default_max_waiter_resubscriptions")]
    pub max_waiter_resubscriptions: u32,
}

fn default_download_coordination_enabled() -> bool {
    true
}

fn default_download_coordination_wait_timeout_secs() -> u64 {
    30
}

fn default_max_waiter_resubscriptions() -> u32 {
    3
}

impl Default for DownloadCoordinationConfig {
    fn default() -> Self {
        Self {
            enabled: default_download_coordination_enabled(),
            wait_timeout_secs: default_download_coordination_wait_timeout_secs(),
            max_waiter_resubscriptions: default_max_waiter_resubscriptions(),
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
    /// How long an acquisition of a key's metadata slot lock waits before giving up
    /// (default: 5s). Valid range: 1-300 seconds.
    ///
    /// A **wait budget**, deliberately not a timeout. Expiry means the caller stops waiting
    /// and returns having touched nothing; it never means the holder is presumed dead and its
    /// lock taken. The protocol does not infer liveness from elapsed time, so a lock a peer
    /// still holds is never stolen — stealing one would remove exclusion rather than restore
    /// it, which is the defect the retired per-key sentinel had.
    ///
    /// The budget bounds the *blocking* acquisition path. `MetadataLockManager::acquire_lock`
    /// retries a non-blocking attempt instead, and its bound is `lock_max_retries`.
    ///
    /// Accepts `lock_timeout` as a deserialization alias so an existing configuration file
    /// keeps parsing. That field was accepted and validated but read by nothing, so honouring
    /// the value now is the first time it has had an effect — see `docs/UPGRADING.md`.
    #[serde(
        alias = "lock_timeout",
        default = "default_slot_acquire_wait_budget",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub lock_acquire_wait_budget: Duration,

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

    /// Journal consolidation interval (default: 5s)
    /// How often to consolidate journal entries into final metadata files.
    /// Shorter intervals improve cross-instance cache visibility but increase I/O.
    /// Valid range: 1-60 seconds
    #[serde(
        default = "default_consolidation_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub consolidation_interval: Duration,

    /// DEPRECATED, no effect. Accepted and range-validated (100KB-10MB) so existing configs
    /// keep loading; a config that sets it, or the `CONSOLIDATION_SIZE_THRESHOLD` environment
    /// variable, logs one WARN at load. No release ever triggered an early consolidation from
    /// it: journal consolidation runs on `consolidation_interval` only.
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

    /// Ignored. Accepted so an existing config file still loads; the orphan re-adoption sweep it
    /// controlled no longer exists. Orphan range files are reclaimed by `crate::orphan_reclaim`,
    /// which always runs. A non-default value logs one startup warning
    /// ([`SharedStorageConfig::warn_retired_orphan_fields`]).
    #[serde(default = "default_orphan_recovery_enabled")]
    pub orphan_recovery_enabled: bool,

    /// Ignored; see [`SharedStorageConfig::orphan_recovery_enabled`].
    #[serde(
        default = "default_orphan_recovery_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub orphan_recovery_interval: Duration,

    /// Ignored; see [`SharedStorageConfig::orphan_recovery_enabled`].
    #[serde(
        default = "default_orphan_scan_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub orphan_scan_timeout: Duration,

    /// Ignored; see [`SharedStorageConfig::orphan_recovery_enabled`].
    #[serde(default = "default_orphan_max_per_cycle")]
    pub orphan_max_per_cycle: usize,

    /// Lease on one eviction work-list chunk while a proxy executes it (default: 900s).
    ///
    /// The proxy holding the eviction lock writes each pass's selection to a work list under
    /// `metadata/_shard_state/eviction_worklist/`, and every proxy executes chunks of it under
    /// this lease. A chunk whose lease has lapsed is taken over by another proxy. The lease is
    /// not extended on progress. Valid range: 60 seconds - 24 hours.
    ///
    /// There is no switch for the work list, or for orphan range-file reclamation: both always
    /// run. `orphan_reclaim_enabled` and `eviction_worklist_enabled`, which internal builds
    /// accepted, are removed keys ([`REMOVED_SWITCH_KEYS`]).
    #[serde(
        default = "default_eviction_worklist_lease",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub eviction_worklist_lease: Duration,

    /// Maximum duration for a single validation scan cycle (default: 4h)
    /// This is the single knob for self-tuning mode selection:
    /// - Full scan that exceeds this → switch to rolling next cycle
    /// - Rolling scan that extrapolates full time ≤ this → switch back to full
    ///
    /// Valid range: 10 minutes - 23 hours
    #[serde(
        default = "default_validation_max_duration",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub validation_max_duration: Duration,
    /// Interval between `st_size` fold passes over the cache volume (default: 1h).
    ///
    /// The fold `readdir`s and `stat`s every range file to re-ground the per-shard byte
    /// aggregate. It parses nothing, which is why it can run on its own cadence rather than
    /// riding the metadata validation rotation.
    ///
    /// **The configured value is a floor, not the schedule.** The fold never runs more often
    /// than twice its own last measured duration, so on a volume large enough that a pass takes
    /// longer than half this interval the effective interval lengthens automatically — see
    /// [`crate::st_size_fold::effective_interval_secs`]. A fold is never scheduled to overlap
    /// itself.
    ///
    /// Valid range: 5 minutes - 7 days. The floor keeps a whole-volume traversal from being
    /// configured as if it were a metric refresh; the ceiling is a sanity bound, past which the
    /// aggregate is stale enough that nothing should be reading it.
    #[serde(
        default = "default_shard_fold_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub shard_fold_interval: Duration,
    /// Maximum wall-clock time one `st_size` fold pass may spend (default: 30m).
    ///
    /// The shard is the unit of abortability: the budget is checked before each shard is
    /// visited, never inside one, so a pass that runs out leaves every shard it reached with a
    /// fresh figure and every shard it did not with its previous one. That is a mixed-age but
    /// well-defined sum, and the per-shard scan ages are what make the mix visible.
    ///
    /// Valid range: 1 minute - 24 hours.
    #[serde(
        default = "default_shard_fold_max_duration",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub shard_fold_max_duration: Duration,
    /// A range is a Tail_Partition member only if its **compressed** size is at least this
    /// (default: 1 MiB).
    ///
    /// The floor exists to keep tiny entries out of a file eviction reads a bounded prefix of;
    /// it is not the membership boundary. Membership is a per-shard **rank** cap
    /// ([`Self::tail_max_entries`]), which is what makes partitions from different shards
    /// mergeable even when their smallest member differs by orders of magnitude.
    ///
    /// **Compressed size, not the range's logical extent.** They differ for compressible data,
    /// and the extent is not the number of bytes an eviction frees — testing the floor against
    /// it admits ranges whose on-disk size is below the floor.
    ///
    /// Valid range: 4 KiB - 1 GiB. Rejected rather than clamped, per
    /// [`Self::shard_fold_interval`]'s precedent.
    #[serde(default = "default_tail_floor_bytes")]
    pub tail_floor_bytes: u64,
    /// Per shard, how many of the **largest** above-floor ranges the Tail_Partition keeps
    /// (default: 4096).
    ///
    /// A per-shard rank cap rather than a fraction of shards, which is what makes it sound: any
    /// range in the global top-K by size is a fortiori in the top-K of its own shard, so for
    /// `K <= tail_max_entries` a merge over every shard's prefix finds it.
    ///
    /// Also what bounds this mechanism's cost independently of cache size — a partition holds at
    /// most this many records however large the shard is.
    ///
    /// Valid range: 1 - 1048576.
    #[serde(default = "default_tail_max_entries")]
    pub tail_max_entries: usize,
    /// Per instance, how many of the **largest** recent above-floor admissions the tail-delta
    /// keeps (default: 4096).
    ///
    /// The tail-delta is the between-visits record of newly admitted large ranges. Without a cap
    /// its length is `(above-floor admission rate) × (shard revisit interval)`, and the revisit
    /// interval grows with cache size — so selection's read of it would be proportional to the
    /// resident population, which is exactly what a bounded prefix read of the partitions exists
    /// to avoid.
    ///
    /// Capping it is sound for the same reason the per-shard rank cap is: an instance appends
    /// only its own admissions, so per-instance sets are disjoint and each keeping its own
    /// largest N means any range in the global top-K is still present for `K <=` this value.
    /// **Do not set it below `tail_max_entries`** for that reason.
    ///
    /// The file is rewritten down to this many records when it reaches twice it, so one rewrite
    /// is amortised over this many appends.
    ///
    /// Valid range: 1 - 1048576.
    #[serde(default = "default_tail_delta_max_entries")]
    pub tail_delta_max_entries: usize,
}

/// Default metadata slot-lock acquisition wait budget: 5 seconds.
///
/// Five, not the 60 the superseded `lock_timeout` defaulted to, because three acquisition
/// call sites already bounded their wait at a hardcoded five seconds. Making the budget
/// configurable at any other default would have silently changed those three, so 5s is what
/// makes this a refactor for them rather than a behaviour change.
fn default_slot_acquire_wait_budget() -> Duration {
    Duration::from_secs(5)
}

/// Default eviction lock timeout: 60 seconds
/// Requirement: 6.2 - THE system SHALL use a default lock timeout of 60 seconds if not configured
fn default_eviction_lock_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_orphan_recovery_enabled() -> bool {
    true
}

fn default_eviction_worklist_lease() -> Duration {
    Duration::from_secs(900)
}

/// Unlink, `.meta`, accounting and flush time allowed per work-list batch on top of its lock
/// waits, in [`eviction_worklist_lease_floor_secs`].
const EVICTION_BATCH_IO_MARGIN_SECS: u64 = 60;

/// The shortest chunk lease under which one work-list batch fits in half the lease (fix 6,
/// For a given `lock_acquire_wait_budget` in seconds.
///
/// An executor starts a batch only while half its lease remains
/// (`eviction_worklist::batch_lease_margin_secs`), so a batch must finish inside half the lease
/// or a peer may take the chunk over mid-batch. A batch is
/// `eviction_worklist::EXECUTOR_BATCH_OBJECTS` (64) objects. Objects in one cache leaf run
/// serially (`run_leaf_groups`), and each waits up to `lock_acquire_wait_budget` for its slot
/// lock, so a batch whose objects share one leaf can spend 64 x the budget waiting; add
/// [`EVICTION_BATCH_IO_MARGIN_SECS`] and double it. At the default 5 s budget that is
/// 2 x (64 x 5 + 60) = 760 s, which is [`EVICTION_WORKLIST_LEASE_MIN_SECS`].
pub(crate) const fn eviction_worklist_lease_floor_secs(lock_acquire_wait_budget_secs: u64) -> u64 {
    2 * (crate::eviction_worklist::EXECUTOR_BATCH_OBJECTS as u64 * lock_acquire_wait_budget_secs
        + EVICTION_BATCH_IO_MARGIN_SECS)
}

/// The shortest `eviction_worklist_lease` accepted: the floor at the default 5 s lock wait
/// budget. A larger configured budget raises the lease executors use instead
/// (`eviction_worklist::executor_budget`), so a config that sets only the budget still loads.
pub(crate) const EVICTION_WORKLIST_LEASE_MIN_SECS: u64 = eviction_worklist_lease_floor_secs(5);
fn default_orphan_recovery_interval() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_orphan_scan_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_orphan_max_per_cycle() -> usize {
    100
}

fn default_validation_max_duration() -> Duration {
    Duration::from_secs(4 * 3600) // 4 hours
}

/// Default `st_size` fold interval: one hour.
///
/// The constant lives beside the fold it schedules
/// ([`crate::st_size_fold::DEFAULT_SHARD_FOLD_INTERVAL_SECS`]) rather than being written twice,
/// and its doc comment there records why the hour is a planning figure with ±2x headroom rather
/// than a measurement of a production cache.
fn default_shard_fold_interval() -> Duration {
    Duration::from_secs(crate::st_size_fold::DEFAULT_SHARD_FOLD_INTERVAL_SECS)
}

/// Default per-pass budget for the `st_size` fold: 30 minutes.
///
/// Half the default interval, so a volume that cannot fold inside the budget still spends at
/// most half its time folding, and the interval scaling then takes over from the next pass.
fn default_shard_fold_max_duration() -> Duration {
    Duration::from_secs(1800)
}

/// Default Tail_Partition floor: 1 MiB.
fn default_tail_floor_bytes() -> u64 {
    1_048_576
}

/// Default Tail_Partition per-shard rank cap: 4096.
fn default_tail_max_entries() -> usize {
    4096
}

/// Default per-instance tail-delta cap: 4096, the same as the per-shard rank cap.
///
/// The same value deliberately, because the soundness condition is `K <= cap` on both files and
/// there is no reason for one to admit a global top-K the other would miss.
fn default_tail_delta_max_entries() -> usize {
    4096
}

impl Default for SharedStorageConfig {
    fn default() -> Self {
        Self {
            lock_acquire_wait_budget: default_slot_acquire_wait_budget(),
            validation_threshold_warn: default_validation_threshold_warn(),
            validation_threshold_error: default_validation_threshold_error(),
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
            validation_max_duration: default_validation_max_duration(),
            shard_fold_interval: default_shard_fold_interval(),
            shard_fold_max_duration: default_shard_fold_max_duration(),
            tail_floor_bytes: default_tail_floor_bytes(),
            tail_max_entries: default_tail_max_entries(),
            tail_delta_max_entries: default_tail_delta_max_entries(),
            eviction_worklist_lease: default_eviction_worklist_lease(),
        }
    }
}

impl SharedStorageConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate lock_acquire_wait_budget (1-300 seconds).
        //
        // The floor is 1, not the 10 the superseded `lock_timeout` enforced. Lowering a floor
        // is the one direction that cannot reject a configuration that used to be accepted,
        // and it has to move because the default is now 5.
        let wait_budget_secs = self.lock_acquire_wait_budget.as_secs();
        if !(1..=300).contains(&wait_budget_secs) {
            return Err(format!(
                "lock_acquire_wait_budget must be between 1 and 300 seconds, got {}",
                wait_budget_secs
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

        // Validate consolidation_interval (1-60 seconds)
        let consolidation_secs = self.consolidation_interval.as_secs();
        if !(1..=60).contains(&consolidation_secs) {
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
        if !(1..=168).contains(&validation_hours) {
            return Err(format!(
                "validation_frequency must be between 1 and 168 hours, got {}h",
                validation_hours
            ));
        }

        // Validate eviction_lock_timeout (30-3600 seconds)
        // Requirement: 6.4 - THE system SHALL validate that lock timeout is between 30 and 3600 seconds
        let eviction_lock_timeout_secs = self.eviction_lock_timeout.as_secs();
        if !(30..=3600).contains(&eviction_lock_timeout_secs) {
            return Err(format!(
                "eviction_lock_timeout must be between 30 and 3600 seconds, got {}",
                eviction_lock_timeout_secs
            ));
        }

        // Validate validation_max_duration (10 minutes - 23 hours)
        let max_duration_secs = self.validation_max_duration.as_secs();
        if max_duration_secs < 10 * 60 {
            return Err(format!(
                "validation_max_duration must be at least 10 minutes (600 seconds), got {}s",
                max_duration_secs
            ));
        }
        if max_duration_secs > 23 * 3600 {
            return Err(format!(
                "validation_max_duration must be at most 23 hours (82800 seconds), got {}s",
                max_duration_secs
            ));
        }
        // Validate shard_fold_interval (5 minutes - 7 days). Rejected rather than clamped
        // because the field is new: no existing configuration file can carry a value outside
        // the range, so rejecting cannot refuse a file that started the proxy yesterday.
        let fold_interval_secs = self.shard_fold_interval.as_secs();
        if !(300..=7 * 86_400).contains(&fold_interval_secs) {
            return Err(format!(
                "shard_fold_interval must be between 5 minutes (300 seconds) and 7 days \
                 (604800 seconds), got {}s",
                fold_interval_secs
            ));
        }
        // Validate shard_fold_max_duration (1 minute - 24 hours).
        let fold_budget_secs = self.shard_fold_max_duration.as_secs();
        if !(60..=86_400).contains(&fold_budget_secs) {
            return Err(format!(
                "shard_fold_max_duration must be between 1 minute (60 seconds) and 24 hours \
                 (86400 seconds), got {}s",
                fold_budget_secs
            ));
        }
        // Validate eviction_worklist_lease (760 s - 24 hours). Below the floor one 64-object batch
        // may not fit in half the lease at the default lock wait budget, so a chunk could be taken
        // over while a batch is still executing (`eviction_worklist_lease_floor_secs`).
        let lease_secs = self.eviction_worklist_lease.as_secs();
        if !(EVICTION_WORKLIST_LEASE_MIN_SECS..=86_400).contains(&lease_secs) {
            return Err(format!(
                "eviction_worklist_lease must be between {}s (two 64-object eviction batches at \
                 the default 5s lock_acquire_wait_budget, plus margin) and 24 hours (86400 \
                 seconds), got {}s",
                EVICTION_WORKLIST_LEASE_MIN_SECS, lease_secs
            ));
        }
        let floor = eviction_worklist_lease_floor_secs(wait_budget_secs);
        if lease_secs < floor {
            warn!(
                "eviction_worklist_lease {}s is shorter than the {}s two eviction batches need at \
                 lock_acquire_wait_budget {}s; executors hold chunk leases for {}s",
                lease_secs, floor, wait_budget_secs, floor
            );
        }
        // Validate tail_floor_bytes (4 KiB - 1 GiB) and tail_max_entries (1 - 1048576).
        // Rejected rather than clamped for the same reason as the two fold fields above: both
        // are new, so no existing configuration file can carry an out-of-range value and a
        // rejection cannot refuse a file that started the proxy yesterday.
        if !(4_096..=1_073_741_824).contains(&self.tail_floor_bytes) {
            return Err(format!(
                "tail_floor_bytes must be between 4 KiB (4096) and 1 GiB (1073741824), got {}",
                self.tail_floor_bytes
            ));
        }
        if !(1..=1_048_576).contains(&self.tail_max_entries) {
            return Err(format!(
                "tail_max_entries must be between 1 and 1048576, got {}",
                self.tail_max_entries
            ));
        }
        if !(1..=1_048_576).contains(&self.tail_delta_max_entries) {
            return Err(format!(
                "tail_delta_max_entries must be between 1 and 1048576, got {}",
                self.tail_delta_max_entries
            ));
        }
        // Cross-field: the delta's cap must admit at least as deep a global top-K as the
        // partitions do, or a range in the global top-K could be present in a partition and
        // discarded from the delta. Warned rather than rejected — it is a tuning mistake that
        // costs a missed recent admission, not a startup-blocking error, and rejecting would
        // refuse a file that lowered only one of the two.
        if self.tail_delta_max_entries < self.tail_max_entries {
            warn!(
                "tail_delta_max_entries ({}) is below tail_max_entries ({}). The per-instance \
                 tail-delta then admits a shallower global top-K than the shard partitions do, \
                 so a newly admitted large range can be discarded from the delta while a range \
                 of the same size would be kept in a partition. Raise it to at least \
                 tail_max_entries.",
                self.tail_delta_max_entries, self.tail_max_entries
            );
        }

        Ok(())
    }

    /// The retired orphan-sweep fields this config sets to a non-default value, in field order.
    pub fn retired_orphan_fields_set(&self) -> Vec<&'static str> {
        let mut set = Vec::new();
        if self.orphan_recovery_enabled != default_orphan_recovery_enabled() {
            set.push("orphan_recovery_enabled");
        }
        if self.orphan_recovery_interval != default_orphan_recovery_interval() {
            set.push("orphan_recovery_interval");
        }
        if self.orphan_scan_timeout != default_orphan_scan_timeout() {
            set.push("orphan_scan_timeout");
        }
        if self.orphan_max_per_cycle != default_orphan_max_per_cycle() {
            set.push("orphan_max_per_cycle");
        }
        set
    }

    /// Log one warning when any retired orphan-sweep field carries a non-default value.
    ///
    /// The four fields still parse, so a config that loaded before keeps loading
    /// (the config compatibility rule), but nothing reads them. One warning for all
    /// four, naming each one set and the replacement, so an operator who tuned the old sweep
    /// learns it is gone. Called once, from `Config::load`.
    pub fn warn_retired_orphan_fields(&self) {
        let set = self.retired_orphan_fields_set();
        if !set.is_empty() {
            warn!(
                "cache.shared_storage.{} is ignored: the orphan re-adoption sweep was removed. \
                 Orphan range files are reclaimed automatically. \
                 Remove these fields from your config.",
                set.join(", cache.shared_storage.")
            );
        }
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

/// Default compression batch size: 1 MiB
/// Bytes are accumulated in RAM before being compressed and flushed as a single
/// LZ4 frame. Larger batches improve compression ratio and throughput at the
/// cost of higher transient memory use per in-flight writer.
fn default_compression_batch_size() -> usize {
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
    Duration::from_secs(10) // 10 seconds (shortened for cross-instance access visibility, Req 19.1)
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

/// Cache initialization configuration. Every field is parsed for compatibility and has no
/// effect; `Config::unread_fields_warning` names any field set to a non-default value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializationConfig {
    /// Ignored (default: true)
    #[serde(default = "default_parallel_scan")]
    pub parallel_scan: bool,
    /// Ignored (default: 30s)
    #[serde(
        default = "default_scan_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub scan_timeout: Duration,
    /// Ignored (default: true)
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

// Default functions for MetadataCacheConfig
fn default_metadata_cache_enabled() -> bool {
    true
}

fn default_metadata_cache_refresh_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_metadata_cache_max_entries() -> usize {
    100000
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
    /// Maximum number of entries in the cache (default: 100000)
    /// Each entry is ~1-2KB, so 100000 entries uses ~150-250MB of RAM.
    /// Valid range: 100-1000000
    #[serde(default = "default_metadata_cache_max_entries")]
    pub max_entries: usize,
    // REMOVED: `stale_handle_max_retries`.
    //
    // Validated at 1-10, logged at `MetadataCache` creation, and read by nothing. The live
    // stale-file-handle retry is `disk_cache::read_and_parse_metadata_blocking`, which uses its
    // own `MAX_RETRIES` constant of 3. The only function that ever read this field,
    // `MetadataCache::load_with_stale_handle_retry`, never had a caller and was deleted in
    // v1.6.9. Its 1-10 validation was never enforced either: `MetadataCacheConfig::validate`
    // has no caller on the startup path.
    //
    // A config still setting it loads (unknown keys are ignored, whatever the value) and
    // warns, via `warn_on_removed_cache_keys`.
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_metadata_cache_enabled(),
            refresh_interval: default_metadata_cache_refresh_interval(),
            max_entries: default_metadata_cache_max_entries(),
        }
    }
}

impl MetadataCacheConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate refresh_interval (1-300 seconds)
        let refresh_secs = self.refresh_interval.as_secs();
        if !(1..=300).contains(&refresh_secs) {
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

        Ok(())
    }

    /// Warn about a value outside the documented range, and USE IT AS CONFIGURED.
    ///
    /// Called from `Config::load`. [`MetadataCacheConfig::validate`] was never on the startup
    /// path, so every value in an existing file booted; rejecting one now would turn an upgrade
    /// into a startup failure, which the config compatibility rule forbids.
    ///
    /// **Why this warns rather than clamps**, unlike `MetricsConfig::validate_and_clamp`:
    /// raising `refresh_interval: 0` to 1s would WIDEN a freshness bound. At zero,
    /// `MetadataCacheEntry::is_stale` (`elapsed() >= refresh_interval`) is true for every entry,
    /// so the RAM metadata tier never answers and each verdict is read from shared storage; a
    /// clamp would silently give that operator up to a second of cross-instance staleness they
    /// had configured away. And lowering a `max_entries` above 1,000,000 would shrink a cache
    /// someone sized deliberately. So the value stands, and the warning says what it does.
    pub(crate) fn warn_if_outside_documented_range(&self) {
        let refresh_secs = self.refresh_interval.as_secs();
        if self.refresh_interval.is_zero() {
            warn!(
                "cache.metadata_cache.refresh_interval is 0, outside the documented 1-300s range; \
                 used as configured: every RAM metadata entry is treated as stale, so the RAM \
                 metadata tier never serves and every lookup reads shared storage. Set \
                 cache.metadata_cache.enabled: false to turn the tier off explicitly."
            );
        } else if !(1..=300).contains(&refresh_secs) {
            warn!(
                "cache.metadata_cache.refresh_interval ({:?}) is outside the documented 1-300s \
                 range; used as configured. It bounds how long this proxy's RAM copy of a \
                 .meta can lag a change made by another proxy.",
                self.refresh_interval
            );
        }
        if self.max_entries == 0 {
            warn!(
                "cache.metadata_cache.max_entries is 0, outside the documented 100-1000000 \
                 range; used as configured, which holds at most one entry (each insert evicts \
                 the one before it)."
            );
        } else if !(100..=1_000_000).contains(&self.max_entries) {
            warn!(
                "cache.metadata_cache.max_entries ({}) is outside the documented 100-1000000 \
                 range; used as configured. Each entry is roughly 1-2 KB of RAM.",
                self.max_entries
            );
        }
    }

    /// Convert to the MetadataCacheConfig used by the metadata_cache module
    pub(crate) fn to_metadata_cache_config(&self) -> crate::metadata_cache::MetadataCacheConfig {
        crate::metadata_cache::MetadataCacheConfig {
            enabled: self.enabled,
            refresh_interval: self.refresh_interval,
            max_entries: self.max_entries,
        }
    }
}

impl ConnectionPoolConfig {
    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate idle_timeout (1-300 seconds). The floor is low enough to sit
        // under S3's own idle close, which was about 5 to 6 s when measured
        // against us-east-1 and varies.
        let idle_timeout_secs = self.idle_timeout.as_secs();
        if !(1..=300).contains(&idle_timeout_secs) {
            return Err(format!(
                "Idle timeout must be between 1 and 300 seconds, got {}",
                idle_timeout_secs
            ));
        }

        // Validate max_idle_per_host (1-500)
        if self.max_idle_per_host < 1 || self.max_idle_per_host > 500 {
            return Err(format!(
                "Max idle connections per host must be between 1 and 500, got {}",
                self.max_idle_per_host
            ));
        }

        // Validate max_lifetime (60-3600 seconds)
        let max_lifetime_secs = self.max_lifetime.as_secs();
        if !(60..=3600).contains(&max_lifetime_secs) {
            return Err(format!(
                "Max connection lifetime must be between 60 and 3600 seconds, got {}",
                max_lifetime_secs
            ));
        }

        // Validate pool_check_interval (1-60 seconds)
        let pool_check_interval_secs = self.pool_check_interval.as_secs();
        if !(1..=60).contains(&pool_check_interval_secs) {
            return Err(format!(
                "Pool check interval must be between 1 and 60 seconds, got {}",
                pool_check_interval_secs
            ));
        }

        // Validate max_idle_per_ip (1-100)
        if self.max_idle_per_ip < 1 || self.max_idle_per_ip > 100 {
            return Err(format!(
                "Max idle connections per IP must be between 1 and 100, got {}",
                self.max_idle_per_ip
            ));
        }

        // Validate hedged_requests.max_inflight_fraction [0.0, 1.0]
        if self.hedged_requests.max_inflight_fraction < 0.0
            || self.hedged_requests.max_inflight_fraction > 1.0
        {
            return Err(format!(
                "hedged_requests.max_inflight_fraction must be between 0.0 and 1.0, got {}",
                self.hedged_requests.max_inflight_fraction
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

fn default_evaluate_conditions_from_cache() -> bool {
    // Enabled by default: a GET, range GET or HEAD whose only conditional is an If-Match
    // or an If-Range matching the cached ETag is answered from cache (the client names
    // the version it wants). If-None-Match,
    // If-Modified-Since, If-Unmodified-Since and mixed conditionals always forward to S3.
    // Set false to forward all conditionals to S3.
    true
}

fn default_bucket_settings_staleness_threshold() -> Duration {
    Duration::from_secs(60) // 60 seconds
}

fn default_ram_cache_shard_count() -> usize {
    8
}

/// Default maximum .meta file size before classifying as corrupt/oversized: 4 MiB.
///
/// A normal range-based .meta is ~2 KB; 4 MiB leaves ample headroom while catching
/// pathological legacy inline-body files (which can be 5+ MB).
fn default_max_metadata_file_bytes() -> u64 {
    4 * 1024 * 1024 // 4 MiB
}

/// Default concurrent blocking metadata I/O operations: 32.
///
/// Metadata reads run in spawn_blocking to avoid starving the async runtime; this
/// semaphore caps how many are in-flight at once so the blocking thread pool and
/// NFS are not overwhelmed under high concurrency or during consolidation.
fn default_metadata_io_concurrency() -> usize {
    32
}

/// Default free-space floor on the cache volume: 1 GiB.
///
/// Today's hardcoded `cache::DISK_SAFETY_FREE_SPACE_FLOOR_BYTES`, promoted to
/// configuration by `cache-eviction-at-scale` task 36 because it is the one genuinely
/// deployment-dependent value in the capacity guard — 1 GiB is generous on a 64 GiB volume
/// and nothing on a petabyte filer. The const's own comment already argued bytes over
/// percent correctly; it never argued for being non-configurable.
///
/// **The default is sized for moderate single-instance traffic on a small volume, and
/// deliberately not for a fleet.** See
/// `capacity_guard::DANGER_BAND_FLOOR_MULTIPLIER` for the aggregate-writer arithmetic and
/// the sizing rule.
fn default_disk_safety_free_space_floor() -> u64 {
    1024 * 1024 * 1024 // 1 GiB
}

/// Default minimum received fraction of an incomplete range to salvage as a
/// clamped sub-range: 0.5 (50%).
///
/// When a range cache write on the read/GET path ends with fewer bytes than the
/// requested range (client cancel / mid-stream idle abort) but at least this
/// fraction of the expected bytes were received in order, the received prefix is
/// committed as a smaller valid sub-range instead of being discarded. 1.0 = only
/// commit a fully-received range (legacy behaviour); 0.0 = commit any non-empty
/// prefix. The write-through PUT path is unaffected (always requires exact bytes).
fn default_partial_range_commit_ratio() -> f64 {
    0.5
}

/// Default maximum CompleteMultipartUpload body size: 10 MiB.
///
/// The Complete XML lists part numbers and ETags — a few MiB is ample for the
/// S3 maximum of 10,000 parts. This prevents unbounded memory consumption from
/// an oversized or malicious body.
fn default_max_complete_body_bytes() -> u64 {
    10 * 1024 * 1024 // 10 MiB
}

/// Removed `cache.*` keys, paired with what the shipped code does instead.
///
/// Each key is a PATH of mapping keys below `cache:`, so a key removed from a nested section is
/// one more segment rather than a second table: `["ram_cache_flush_threshold"]` is
/// `cache.ram_cache_flush_threshold`, and `["metadata_cache", "stale_handle_max_retries"]` is
/// `cache.metadata_cache.stale_handle_max_retries`. A one-segment path walks exactly as the
/// flat table did.
///
/// Kept beside the struct the keys were removed from so the two cannot drift apart.
const REMOVED_CACHE_KEYS: [(&[&str], &str); 3] = [
    (
        &["ram_cache_flush_threshold"],
        "Buffered cache-hit updates flush at a fixed 10000-entry ceiling.",
    ),
    (
        &["ram_cache_flush_on_eviction"],
        "RAM cache eviction never flushed buffered updates.",
    ),
    (
        &["metadata_cache", "stale_handle_max_retries"],
        "A stale file handle on a metadata read is retried a fixed 3 times, 50 ms x attempt \
         apart, and a read that still fails is treated as a cache miss.",
    ),
];

/// Removed `cache.*` switches for behaviour that now always runs, with what runs in their place.
///
/// Both were accepted by internal builds only, so no deprecation cycle is owed (Ed,
/// 2026-10-08: new v3 behaviours get no off switch). A config naming either, at any value,
/// still loads and runs the default behaviour, and [`warn_on_removed_cache_keys`] logs one
/// warning per key present. Separate from [`REMOVED_CACHE_KEYS`] because that table's message
/// says the key never had an effect, which is not true of these.
pub(crate) const REMOVED_SWITCH_KEYS: [(&[&str], &str); 2] = [
    (
        &["shared_storage", "orphan_reclaim_enabled"],
        "Orphan range files are always reclaimed.",
    ),
    (
        &["shared_storage", "eviction_worklist_enabled"],
        "Eviction always runs through the shared work list once every proxy on the cache \
         volume supports it, and falls back to the single-proxy pass until then.",
    ),
];

/// Removed `cache.shared_storage.*` fields, with the reason. Some were dead settings; at least
/// `metadata_lock_timeout_ms` drove cross-host lock staleness in an earlier release
/// (`changelog-pre-v3.md`). A config naming one still loads and the value is ignored;
/// [`warn_on_removed_cache_keys`] logs one warning per key present.
const REMOVED_SHARED_STORAGE_KEYS: [(&[&str], &str); 4] = [
    (
        &["shared_storage", "max_cached_objects"],
        "There is no resident-object limit; `max_cache_size` bounds the cache.",
    ),
    (
        &["shared_storage", "recovery_max_concurrent"],
        "Orphan range files are reclaimed by a rotation with no settings.",
    ),
    (
        &["shared_storage", "lock_refresh_interval"],
        "Metadata locks never expire, so there is nothing to refresh.",
    ),
    (
        &["shared_storage", "metadata_lock_timeout_ms"],
        "Metadata lock acquisition waits for `lock_acquire_wait_budget`.",
    ),
];

/// Removed top-level `server.*` fields, with what happens instead.
const REMOVED_SERVER_KEYS: [(&str, &str); 1] = [(
    "https_mode",
    "The HTTPS listener always passes TCP through to S3 uncached.",
)];

/// Warn for each removed `cache.*` key a loaded config document still sets.
///
/// Serde ignores unknown keys, and that is what keeps an existing config file loading across
/// an upgrade — the contract in the config compatibility rule. The cost is silence:
/// the operator who tuned the key gets no signal that it does nothing. This restores the
/// signal without restoring the field, the same trade the retired
/// `cache.shared_storage.lock_refresh_interval` makes for its CLI flag and env var.
///
/// Keyed off the key's PRESENCE, not a deviation from its former default. Both keys shipped in
/// `config/config.example.yaml` at their defaults, so a `!= default` check would stay quiet for
/// exactly the operators who copied the example.
fn warn_on_removed_cache_keys(document: &serde_yaml_ng::Value) {
    let Some(cache) = document.get("cache") else {
        return;
    };
    for (path, detail) in REMOVED_CACHE_KEYS {
        // Descend one mapping per segment. `get` on a non-mapping (a scalar, or a null
        // `metadata_cache:` with nothing under it) yields `None`, so a section that is present
        // but empty is quiet rather than a false warning.
        let mut node = Some(cache);
        for segment in path {
            node = node.and_then(|n| n.get(*segment));
        }
        if node.is_some() {
            warn!(
                "Configuration field 'cache.{}' has been removed and had no effect in any \
                 release that accepted it. {} Delete the field to silence this warning.",
                path.join("."),
                detail
            );
        }
    }
    for (path, detail) in REMOVED_SWITCH_KEYS
        .iter()
        .chain(REMOVED_SHARED_STORAGE_KEYS.iter())
    {
        let mut node = Some(cache);
        for segment in path.iter() {
            node = node.and_then(|n| n.get(*segment));
        }
        if node.is_some() {
            warn!(
                "Configuration field 'cache.{}' has been removed and is ignored. {} Delete the \
                 field to silence this warning.",
                path.join("."),
                detail
            );
        }
    }
}

/// Warn for each removed `server.*` key a loaded config document still sets.
fn warn_on_removed_server_keys(document: &serde_yaml_ng::Value) {
    let Some(server) = document.get("server") else {
        return;
    };
    for (key, detail) in REMOVED_SERVER_KEYS {
        if server.get(key).is_some() {
            warn!(
                "Configuration field 'server.{}' has been removed and is ignored. {} Delete the \
                 field to silence this warning.",
                key, detail
            );
        }
    }
}

/// WARN once per config load for each accepted-but-inert key present in the loaded document.
///
/// Keyed off PRESENCE, for the same reason as [`warn_on_removed_cache_keys`]: the example
/// config ships these keys at their defaults, so a `!= default` check would stay quiet for the
/// operators who copied it.
fn warn_on_inert_cache_keys(document: &serde_yaml_ng::Value) {
    let present = document
        .get("cache")
        .and_then(|c| c.get("shared_storage"))
        .and_then(|s| s.get("consolidation_size_threshold"))
        .is_some();
    if present {
        warn!("{}", CONSOLIDATION_SIZE_THRESHOLD_DEPRECATION);
    }
}

/// The deprecation notice for `cache.shared_storage.consolidation_size_threshold`.
const CONSOLIDATION_SIZE_THRESHOLD_DEPRECATION: &str =
    "Configuration field 'cache.shared_storage.consolidation_size_threshold' is deprecated and \
     has no effect: journal consolidation runs on 'cache.shared_storage.consolidation_interval' \
     only. The value is ignored. Delete the field to silence this warning.";

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
    /// How long a cached GET entry is served without contacting S3 (default: 1 day).
    ///
    /// After it expires, the next read sends S3 a conditional request; an unchanged
    /// object costs a 304 and no body transfer. The value therefore bounds how long an
    /// object overwritten or deleted without passing through this proxy can still be
    /// served from the cache. Immutable data can use up to 315,360,000 seconds
    /// (10 years), globally or for a prefix in `cache_rules.json`.
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
    /// DEPRECATED: This field is not read by any logic. Eviction behavior is controlled by
    /// `eviction_trigger_percent` (default 95%) and `eviction_target_percent` (default 80%).
    /// Retained for one release cycle for backward-compatible config parsing.
    /// Will be removed in a future release.
    #[serde(default = "default_eviction_buffer_percent")]
    pub eviction_buffer_percent: u8,
    /// Interval between periodic flushes of RAM cache access statistics to disk
    /// Default: 10 seconds, Range: 10-600 seconds
    /// Shorter intervals improve cross-instance access visibility for eviction decisions (Req 19).
    #[serde(
        default = "default_ram_cache_flush_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub ram_cache_flush_interval: Duration,
    // REMOVED: `ram_cache_flush_threshold` and `ram_cache_flush_on_eviction`.
    //
    // Both described flushing buffered cache-hit updates — at a pending-update count, and on
    // RAM eviction — and no shipped code ever read either. `CacheHitUpdateBuffer::new` passes
    // the literal `MAX_BUFFER_ENTRIES` (10000), and the parameterised `with_config` is only
    // ever called with literals. They arrived with the never-implemented archived
    // `ram-disk-cache-coherency` spec and are the unimplemented remainder of it; the sibling
    // that was properly re-specified and built is `ram_cache_verification_interval`.
    //
    // `ram_cache_flush_threshold` was not wired up instead: the config default was `100`
    // against that hardcoded cap of `10000`, so honouring it would flush a hundred times more
    // often on every default deployment — a behaviour change, not a cleanup.
    //
    // A config still setting either key loads (unknown keys are ignored) and warns, via
    // `warn_on_removed_cache_keys`. `ram_cache_flush_interval` above is NOT removed: it
    // governs no flush, but it has two real consumers.
    /// How often a RAM range hit is re-verified against the shared `.meta`
    /// before being served without a Shared_Storage read. `0` verifies on
    /// every hit (opt-out). Default: 1s, Range: 0-60s.
    ///
    /// Un-deprecated in ram-tier-effectiveness Requirement 4 (v2.8.6). This
    /// field shipped in an earlier release with no consumer, was deprecated
    /// in v2.8.0, and now has a real one: `CacheManager::ram_verify` uses it
    /// to bound RAM-served response headers/bytes to one object version
    /// without paying a disk read on every hit. See `docs/CACHE_FRESHNESS.md`.
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
    /// Compression batch size in bytes (default: 1 MiB)
    /// Bytes are accumulated in RAM up to this size, then compressed as a single
    /// LZ4 frame and appended to the cache file. Larger batches improve compression
    /// ratio and throughput; smaller batches reduce transient memory per in-flight writer.
    /// Valid range: 64 KiB to 16 MiB inclusive.
    /// Requirement: 5.1
    #[serde(default = "default_compression_batch_size")]
    pub compression_batch_size: usize,
    /// Enable read caching for GET responses (default: true)
    /// When false, no buckets are read-cached unless explicitly enabled via bucket settings.
    /// This allows an "allowlist" pattern where only specific buckets have caching enabled.
    #[serde(default = "default_read_cache_enabled")]
    pub read_cache_enabled: bool,
    /// Answer a client's lone `If-Match` or lone `If-Range` from cached metadata instead
    /// of forwarding it to S3 (default: true).
    ///
    /// When true (default): a GET, range GET or HEAD whose only conditional header is
    /// `If-Match`, with a non-zero `get_ttl` and an unexpired cached entry whose ETag
    /// matches, is served from cache without contacting S3. A HEAD is answered from the
    /// cached HEAD entry, which must also be within `head_ttl`. A GET, range GET or HEAD
    /// whose only conditional header is `If-Range`, with a validator that strong-matches
    /// the cached ETag, is served from cache the same way. Any
    /// other conditional (`If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, or
    /// a mix of headers), a mismatch, or a miss is forwarded to S3. Client bypass headers
    /// (Cache-Control: no-cache/no-store, Pragma: no-cache) also force forward.
    ///
    /// When false: every conditional GET/HEAD is forwarded to S3 with the client's
    /// headers intact, so S3 decides every precondition.
    #[serde(default = "default_evaluate_conditions_from_cache")]
    pub evaluate_conditions_from_cache: bool,
    /// How long cached bucket settings are considered fresh before re-reading from disk (default: 60s)
    #[serde(
        default = "default_bucket_settings_staleness_threshold",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub bucket_settings_staleness_threshold: Duration,
    /// Number of independent shards for the RAM cache (default: 64)
    /// Each shard is protected by its own `tokio::sync::RwLock`, allowing concurrent
    /// reads for keys in different shards to proceed without contention.
    /// Higher values reduce lock contention under concurrent load.
    /// Lower values reduce per-shard metadata overhead for small caches.
    /// Per-shard capacity = max_ram_cache_size / ram_cache_shard_count.
    /// Valid range: 1–256.
    /// Requirements: 1.2, 5.1, 5.2, 5.3
    #[serde(default = "default_ram_cache_shard_count")]
    pub ram_cache_shard_count: usize,
    /// Maximum size in bytes for CompleteMultipartUpload request bodies (default: 10 MiB).
    /// The Complete XML lists part numbers and ETags — a few MiB is ample for even the
    /// largest uploads (10,000 parts). Bodies exceeding this limit are rejected with
    /// HTTP 413 before forwarding to S3, preventing unbounded memory consumption.
    #[serde(default = "default_max_complete_body_bytes")]
    pub max_complete_body_bytes: u64,

    /// Maximum size in bytes of a .meta file the proxy will read and parse (default: 4 MiB).
    /// Files exceeding this cap are classified as corrupt/oversized without a full read,
    /// then healed via cache-miss-and-refetch. Protects against legacy inline-body .meta
    /// files (which can be MBs) and any other pathological metadata.
    /// A normal range-based .meta is ~2 KB; default 4 MiB leaves ample headroom.
    /// Spec: cache-metadata-resilience Req 3/4
    #[serde(default = "default_max_metadata_file_bytes")]
    pub max_metadata_file_bytes: u64,

    /// Maximum number of concurrent blocking metadata I/O operations (default: 32).
    /// Metadata reads run in spawn_blocking to avoid starving the async runtime; this
    /// semaphore caps how many are in-flight at once so the blocking thread pool and
    /// NFS are not overwhelmed under high concurrency or during consolidation.
    /// Spec: cache-metadata-resilience Req 1
    #[serde(default = "default_metadata_io_concurrency")]
    pub metadata_io_concurrency: usize,

    /// Minimum received fraction (0.0–1.0) of an incomplete range to salvage as a
    /// clamped sub-range on the read/GET cache-write path (default: 0.5).
    ///
    /// When a streamed range cache write ends with fewer bytes than the requested
    /// range — because the client cancelled or the mid-stream idle watchdog aborted
    /// the stream — but at least this fraction of the expected bytes were received
    /// (in order), the received prefix is committed as a smaller valid range
    /// `[start, start + received - 1]` instead of being discarded. This lets a
    /// single high-throughput (e.g. CRT) download populate the cache even when some
    /// part requests are cut short. Below the threshold, nothing is committed.
    /// `1.0` requires a fully-received range (legacy behaviour); `0.0` commits any
    /// non-empty prefix. The write-through PUT cache path always requires the exact
    /// byte count and is unaffected (a truncated upload is never cached).
    /// Spec: crt-conditional-range-caching Req 2
    #[serde(default = "default_partial_range_commit_ratio")]
    pub partial_range_commit_ratio: f64,

    /// Free space kept in reserve on the cache volume, in bytes, above whatever an
    /// incoming object needs (default: 1 GiB = 1073741824).
    ///
    /// Applies to **every** admission point, write-through and GET-miss fill alike: the
    /// pre-existing write-through `disk_safety_refusal`, and the danger-band publication
    /// guard added by `cache-eviction-at-scale` task 36. Caching is declined when the
    /// volume has less than this free beyond the object's own size; the client request
    /// always proceeds regardless.
    ///
    /// **Sizing rule, and it is aggregate across writers rather than per-instance.** The
    /// danger band is `4 ×` this floor, and outside the band every writer sharing the
    /// volume may admit for up to 5 s against its own cached free-space reading. So:
    ///
    /// ```text
    ///   floor  ≥  ( 5 s × Σ fill rate of every writer on the volume  +  largest admitted
    ///               body )  /  4
    /// ```
    ///
    /// "Every writer" includes non-proxy neighbours on the same filer. At this project's
    /// recorded ~2.0 GiB/s aggregate fleet cache-miss ingest and a 256 MiB largest body,
    /// that is ~2.6 GiB — so the 1 GiB default does **not** absorb a fleet running at that
    /// rate for five seconds. Raising the floor widens the band with it.
    ///
    /// `0` disables both the floor and the danger band, leaving only reactive `ENOSPC`
    /// handling. Not recommended on a shared volume.
    ///
    /// Spec: cache-eviction-at-scale task 36 (A1).
    #[serde(default = "default_disk_safety_free_space_floor")]
    pub disk_safety_free_space_floor: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/cache"),
            max_cache_size: 10 * 1024 * 1024 * 1024, // 10GB
            ram_cache_enabled: false,
            // 512MB: yields 8 effective shards (512MiB / 64MiB admission ceiling) out
            // of the box, preserving the pre-clamp default shard concurrency.
            // Spec: page-aligned-range-cache Resolved Question 4.
            max_ram_cache_size: 512 * 1024 * 1024, // 512MB
            eviction_algorithm: EvictionAlgorithm::default(),
            write_cache_enabled: true,
            write_cache_percent: 10.0,
            write_cache_max_object_size: 256 * 1024 * 1024, // 256MB
            put_ttl: Duration::from_secs(3600),             // 1 hour
            get_ttl: Duration::from_secs(86400),            // 1 day
            head_ttl: Duration::from_secs(60),              // 1 minute
            actively_remove_cached_data: false,
            shared_storage: SharedStorageConfig::default(),
            download_coordination: DownloadCoordinationConfig::default(),
            range_merge_gap_threshold: default_range_merge_gap_threshold(),
            eviction_buffer_percent: default_eviction_buffer_percent(),
            ram_cache_flush_interval: default_ram_cache_flush_interval(),
            ram_cache_verification_interval: default_ram_cache_verification_interval(),
            incomplete_upload_ttl: default_incomplete_upload_ttl(),
            initialization: InitializationConfig::default(),
            cache_bypass_headers_enabled: default_cache_bypass_headers_enabled(),
            metadata_cache: MetadataCacheConfig::default(),
            eviction_trigger_percent: default_eviction_trigger_percent(),
            eviction_target_percent: default_eviction_target_percent(),
            full_object_check_threshold: default_full_object_check_threshold(),
            disk_streaming_threshold: default_disk_streaming_threshold(),
            compression_batch_size: default_compression_batch_size(),
            read_cache_enabled: default_read_cache_enabled(),
            evaluate_conditions_from_cache: default_evaluate_conditions_from_cache(),
            bucket_settings_staleness_threshold: default_bucket_settings_staleness_threshold(),
            ram_cache_shard_count: default_ram_cache_shard_count(),
            max_complete_body_bytes: default_max_complete_body_bytes(),
            max_metadata_file_bytes: default_max_metadata_file_bytes(),
            metadata_io_concurrency: default_metadata_io_concurrency(),
            partial_range_commit_ratio: default_partial_range_commit_ratio(),
            disk_safety_free_space_floor: default_disk_safety_free_space_floor(),
        }
    }
}

impl CacheConfig {
    /// Maximum allowed TTL value: 315,360,000 seconds (10 years).
    /// TTL fields exceeding this are rejected at startup.
    /// Requirement: 8.5
    const MAX_TTL_SECONDS: u64 = 315_360_000;

    /// The deprecation notice for `cache.eviction_algorithm: "lru"`, or `None` when the
    /// configured value is not a retired spelling.
    ///
    /// **The behaviour this describes changed in 3.0.0 and the wording tracks it.** While
    /// LRU was still selectable this notice said `"lru"` "still takes effect: eviction
    /// continues to order by recency". R10.10 removed LRU as a choice, so that sentence is
    /// now false and saying it would send an operator looking for recency-ordered eviction
    /// that is no longer running. The notice now states the three things an operator needs:
    /// the value is still accepted (so the upgrade did not fail), it resolves to `tinylfu`,
    /// and the ordering they get is frequency-plus-recency rather than recency.
    ///
    /// **This is still not a "has no effect" deprecation.** The value is honoured to the
    /// extent that it starts the proxy; what it no longer does is select an algorithm.
    ///
    /// Returned as a value rather than logged here so the text is assertable without a
    /// tracing subscriber; `log_deprecated_fields` does the logging. The pure function and
    /// the wiring are tested separately, because a helper that returns the right string
    /// and is never called produces exactly the silence this exists to end.
    ///
    /// Requirement: cache-eviction-at-scale R10.5, R10.9, R10.10
    pub(crate) fn eviction_algorithm_deprecation_warning(&self) -> Option<&'static str> {
        matches!(self.eviction_algorithm, EvictionAlgorithm::DeprecatedLru).then_some(
            "Configuration field 'cache.eviction_algorithm' is set to 'lru', which has been \
             removed as a choice and is no longer the default. The value is still accepted \
             so that this upgrade does not require a config edit, and it now behaves as \
             'tinylfu': eviction orders by decayed access frequency and recency together, \
             not by recency alone. No cached data is invalidated and no manual step is \
             required. Set 'cache.eviction_algorithm: \"tinylfu\"' or remove the field to \
             silence this warning.",
        )
    }

    /// Validate cache configuration values.
    ///
    /// Currently validates:
    /// - `compression_batch_size` must be between 64 KiB and 16 MiB inclusive
    ///   (Requirements: 5.1, 5.2, 5.3, 5.4)
    /// - TTL fields must not exceed 315,360,000 seconds (10 years)
    ///   (Requirement: 8.5)
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate compression_batch_size (64 KiB to 16 MiB inclusive)
        const MIN_COMPRESSION_BATCH_SIZE: usize = 64 * 1024; // 64 KiB
        const MAX_COMPRESSION_BATCH_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
        if self.compression_batch_size < MIN_COMPRESSION_BATCH_SIZE
            || self.compression_batch_size > MAX_COMPRESSION_BATCH_SIZE
        {
            return Err(format!(
                "cache.compression_batch_size must be between {} and {} bytes, got {}",
                MIN_COMPRESSION_BATCH_SIZE, MAX_COMPRESSION_BATCH_SIZE, self.compression_batch_size
            ));
        }

        // Validate TTL fields do not exceed maximum (Requirement: 8.5)
        self.validate_ttl_field("cache.get_ttl", &self.get_ttl)?;
        self.validate_ttl_field("cache.put_ttl", &self.put_ttl)?;
        self.validate_ttl_field("cache.head_ttl", &self.head_ttl)?;
        self.validate_ttl_field("cache.incomplete_upload_ttl", &self.incomplete_upload_ttl)?;

        // Validate ram_cache_shard_count (1–256 inclusive)
        // Requirements: 5.2, 5.3
        if self.ram_cache_shard_count < 1 || self.ram_cache_shard_count > 256 {
            return Err("cache.ram_cache_shard_count must be between 1 and 256".into());
        }

        // Validate partial_range_commit_ratio (0.0–1.0 inclusive)
        // Spec: crt-conditional-range-caching Req 2.6
        if !(0.0..=1.0).contains(&self.partial_range_commit_ratio)
            || self.partial_range_commit_ratio.is_nan()
        {
            return Err(format!(
                "cache.partial_range_commit_ratio must be between 0.0 and 1.0, got {}",
                self.partial_range_commit_ratio
            ));
        }

        Ok(())
    }

    /// Validate a single TTL field does not exceed the maximum.
    /// Requirement: 8.5
    fn validate_ttl_field(
        &self,
        field_name: &str,
        value: &Duration,
    ) -> std::result::Result<(), String> {
        let secs = value.as_secs();
        if secs > Self::MAX_TTL_SECONDS {
            return Err(format!(
                "configuration field '{}' value {} exceeds maximum {} seconds (10 years)",
                field_name,
                secs,
                Self::MAX_TTL_SECONDS
            ));
        }
        Ok(())
    }
}

/// Cache eviction algorithms.
///
/// **There is exactly one selectable algorithm as of 3.0.0.** `TinyLFU` is it. LRU was
/// removed as a *choice* (cache-eviction-at-scale R10.5, R10.10) — no value of
/// `cache.eviction_algorithm` selects recency-ordered eviction any more.
///
/// [`Self::DeprecatedLru`] is **not a second algorithm**. It is the legacy spelling
/// `"lru"`, retained so the upgrade contract holds, and it resolves to `TinyLFU`
/// behaviour. See its own doc comment before adding a variant or reading one as a
/// behavioural choice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum EvictionAlgorithm {
    /// Decayed-frequency eviction, and the only selectable algorithm as of 3.0.0 (R10.5).
    /// R4 makes the entry dimension a continuously-running reclaimer rather than an
    /// occasional pressure response, so ordering quality now applies to every small entry
    /// on every rotation — which is exactly where decayed frequency beats pure recency.
    #[serde(rename = "tinylfu")]
    #[default]
    TinyLFU,
    /// **The legacy spelling `"lru"`, not an algorithm.** A config carrying
    /// `eviction_algorithm: "lru"` parses into this variant, starts, emits a deprecation
    /// `WARN`, and evicts as `TinyLFU` — the conversion in
    /// `From<EvictionAlgorithm> for CacheEvictionAlgorithm` below is the single place that
    /// mapping lives.
    ///
    /// **Why a variant and not `#[serde(alias = "lru")]` on `TinyLFU`.** An alias is the
    /// obvious mechanism and it cannot satisfy this requirement: it collapses `"lru"` into
    /// `TinyLFU` during deserialization, so by the time any code can inspect the config the
    /// fact that the operator wrote `"lru"` is gone, and the warning R10.9 mandates becomes
    /// unimplementable. the config compatibility rule requires a retired value to
    /// be accepted *and* to log a warning when used; only a distinguishable variant does
    /// both. It also makes `Serialize` round-trip `"lru"` → `"lru"` rather than silently
    /// respelling an operator's file.
    ///
    /// Do not use this variant in a test fixture to mean "run LRU" — it does not, and no
    /// config value does. Retire it (and this variant) one release after 3.0.0 per R10.9.
    ///
    /// Requirement: cache-eviction-at-scale R10.9
    #[serde(rename = "lru")]
    DeprecatedLru,
}

impl From<EvictionAlgorithm> for crate::cache::CacheEvictionAlgorithm {
    fn from(algo: EvictionAlgorithm) -> Self {
        match algo {
            EvictionAlgorithm::TinyLFU => crate::cache::CacheEvictionAlgorithm::TinyLFU,
            // R10.9: the retired spelling behaves as the replacement, and this is the only
            // site that decides so. `http_proxy.rs`'s startup wiring used to hand-roll a
            // second copy of this match; it now calls this impl, so the two cannot diverge.
            EvictionAlgorithm::DeprecatedLru => crate::cache::CacheEvictionAlgorithm::TinyLFU,
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

/// Default retention period for access log files (30 days)
fn default_access_log_retention_days() -> u32 {
    30
}

/// Default node-local state directory: `/var/lib/s3-proxy`, which the shipped systemd unit's
/// `StateDirectory=s3-proxy` creates writable under `ProtectSystem=strict`.
fn default_local_state_dir() -> PathBuf {
    PathBuf::from("/var/lib/s3-proxy")
}

/// Default retention period for application log files (30 days)
fn default_app_log_retention_days() -> u32 {
    30
}

/// Default interval between log cleanup cycles (24 hours)
fn default_log_cleanup_interval() -> Duration {
    Duration::from_secs(24 * 3600)
}

/// Default rotation interval for access log files (5 minutes)
fn default_access_log_file_rotation_interval() -> Duration {
    Duration::from_secs(5 * 60)
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    #[serde(deserialize_with = "pathbuf_serde::deserialize")]
    pub access_log_dir: PathBuf,
    #[serde(deserialize_with = "pathbuf_serde::deserialize")]
    pub app_log_dir: PathBuf,
    /// Node-local directory for this proxy's own durable state: the pending write-order stamp
    /// log (`crate::stamp_backlog`). Must be on the instance's own volume, never the shared
    /// cache volume or shared log storage: a directory inside `cache_dir`, or on a file system that
    /// is not a known local type (ext2/3/4, XFS, btrfs, tmpfs, overlayfs, ZFS or f2fs), is
    /// refused at startup and the proxy runs in memory only. Default `/var/lib/s3-proxy`.
    #[serde(
        default = "default_local_state_dir",
        deserialize_with = "pathbuf_serde::deserialize"
    )]
    pub local_state_dir: PathBuf,
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
    /// Days to retain access log files (default: 30, range: 1-365)
    #[serde(default = "default_access_log_retention_days")]
    pub access_log_retention_days: u32,
    /// Days to retain application log files (default: 30, range: 1-365)
    #[serde(default = "default_app_log_retention_days")]
    pub app_log_retention_days: u32,
    /// Interval between cleanup cycles (default: 24h, range: 1h-7d)
    #[serde(
        default = "default_log_cleanup_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub log_cleanup_interval: Duration,
    /// Time window for appending to same access log file (default: 5m, range: 1m-60m)
    #[serde(
        default = "default_access_log_file_rotation_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub access_log_file_rotation_interval: Duration,
}

/// Access logging mode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum AccessLogMode {
    #[serde(rename = "all")]
    #[default]
    All,
    #[serde(rename = "cached_only")]
    CachedOnly,
}

impl LoggingConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate access_log_retention_days (1-365)
        if self.access_log_retention_days < 1 || self.access_log_retention_days > 365 {
            return Err(format!(
                "access_log_retention_days must be between 1 and 365, got {}",
                self.access_log_retention_days
            ));
        }

        // Validate app_log_retention_days (1-365)
        if self.app_log_retention_days < 1 || self.app_log_retention_days > 365 {
            return Err(format!(
                "app_log_retention_days must be between 1 and 365, got {}",
                self.app_log_retention_days
            ));
        }

        // Validate log_cleanup_interval (1h-7d)
        let cleanup_secs = self.log_cleanup_interval.as_secs();
        if !(3600..=7 * 24 * 3600).contains(&cleanup_secs) {
            return Err(format!(
                "log_cleanup_interval must be between 1 hour and 7 days, got {}s",
                cleanup_secs
            ));
        }

        // Validate access_log_file_rotation_interval (1m-60m)
        let rotation_secs = self.access_log_file_rotation_interval.as_secs();
        if !(60..=3600).contains(&rotation_secs) {
            return Err(format!(
                "access_log_file_rotation_interval must be between 1 and 60 minutes, got {}s",
                rotation_secs
            ));
        }

        Ok(())
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            access_log_dir: PathBuf::from("/logs/access"),
            app_log_dir: PathBuf::from("/logs/app"),
            local_state_dir: default_local_state_dir(),
            access_log_enabled: true,
            access_log_mode: AccessLogMode::default(),
            log_level: "info".to_string(),
            access_log_flush_interval: default_access_log_flush_interval(),
            access_log_buffer_size: default_access_log_buffer_size(),
            access_log_retention_days: default_access_log_retention_days(),
            app_log_retention_days: default_app_log_retention_days(),
            log_cleanup_interval: default_log_cleanup_interval(),
            access_log_file_rotation_interval: default_access_log_file_rotation_interval(),
        }
    }
}

/// Per-instance hedged-request governor configuration.
///
/// Lives at `connection_pool.hedged_requests` in `config.yaml`. Hedging itself is
/// enabled per key pattern in `cache_rules.json` (`hedging_enabled`, `hedge_trigger_after`,
/// `hedge_max_per_request`); this struct holds only the single process-global knob that
/// governs the in-flight hedge ratio across all keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HedgedRequestsConfig {
    /// Maximum fraction of in-flight fetches that may be hedges (default: 0.1).
    /// When `(in_flight_hedges + 1) / max(in_flight_fetches, 1)` exceeds this
    /// value, new hedges are suppressed and the original fetch is served alone.
    /// Valid range: [0.0, 1.0].
    #[serde(default = "default_max_inflight_fraction")]
    pub max_inflight_fraction: f64,
}

fn default_max_inflight_fraction() -> f64 {
    0.1
}

impl Default for HedgedRequestsConfig {
    fn default() -> Self {
        Self {
            max_inflight_fraction: default_max_inflight_fraction(),
        }
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionPoolConfig {
    /// DEPRECATED: This field is not read by any logic. Connection pooling is managed by
    /// `max_idle_per_host` and `max_idle_per_ip` instead.
    /// Retained for one release cycle for backward-compatible config parsing.
    /// Will be removed in a future release.
    #[serde(default = "default_max_connections_per_ip")]
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
    /// Maximum idle connections per host (IP address) (default: 100)
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
    /// Per-destination upstream transport overrides. Empty (default) means the feature
    /// is off and every caching-egress destination gets verified TLS on port 443, exactly
    /// as a build without this feature. Each entry maps a host-matcher-and-port key
    /// (e.g. "127.0.0.1:9000", "store.local:9000") to the upstream transport declaration
    /// (`scheme`, and for HTTPS `validate_tls`). Independent of `endpoint_overrides`
    /// (IP resolution) and its TLS-1.2 lock. See docs for the three transport modes.
    #[serde(default)]
    pub upstream_overrides: std::collections::HashMap<String, UpstreamOverrideConfig>,
    /// Enable per-IP connection pool distribution (default: true)
    /// When enabled, the proxy rewrites request URI authorities to individual S3 IP addresses,
    /// causing hyper to create separate connection pools per IP for better load distribution.
    #[serde(default = "default_ip_distribution_enabled")]
    pub ip_distribution_enabled: bool,
    /// Maximum idle connections per IP when IP distribution is enabled (default: 10)
    /// With 8 S3 IPs, this results in ~80 total idle connections.
    /// Valid range: 1-100
    #[serde(default = "default_max_idle_per_ip")]
    pub max_idle_per_ip: usize,
    /// TCP keepalive idle time in seconds before probing begins (default: 15)
    #[serde(default = "default_keepalive_idle_secs")]
    pub keepalive_idle_secs: u64,
    /// TCP keepalive probe interval in seconds (default: 5)
    #[serde(default = "default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
    /// TCP keepalive probe retry count (default: 3)
    #[serde(default = "default_keepalive_retries")]
    pub keepalive_retries: u32,
    /// TCP receive buffer size hint (`SO_RCVBUF`) in bytes. Default: `None`.
    ///
    /// `None` (the default) leaves the socket buffer unset so the Linux kernel
    /// auto-tunes the TCP receive window dynamically (up to `net.core.rmem_max`).
    /// Auto-tuning is required to reach full single-stream throughput on
    /// high-RTT (cross-region) paths, where a small fixed window caps the
    /// bandwidth-delay product.
    ///
    /// Setting an explicit `Some(bytes)` value pins `SO_RCVBUF`, which disables
    /// the kernel's dynamic receive-window auto-tuning (DRS). Only set this if
    /// you have a specific reason to override the kernel.
    #[serde(default = "default_tcp_recv_buffer_size")]
    pub tcp_recv_buffer_size: Option<usize>,
    /// Consecutive failures before excluding an IP from round-robin (default: 3)
    #[serde(default = "default_ip_failure_threshold")]
    pub ip_failure_threshold: u32,
    /// Initial cooldown before an unhealthy IP becomes a probe candidate (default: 5s)
    #[serde(
        default = "default_health_probe_initial_cooldown",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub health_probe_initial_cooldown: Duration,
    /// Maximum cooldown for unhealthy IP recovery probing (default: 300s / 5 min)
    #[serde(
        default = "default_health_probe_max_cooldown",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub health_probe_max_cooldown: Duration,
    /// Maximum number of endpoints that can be registered for DNS-based IP distribution
    /// (default: 10000). Prevents unbounded growth from excessive unique hostnames.
    /// Normal S3 usage is well under this cap.
    #[serde(default = "default_max_registered_endpoints")]
    pub max_registered_endpoints: usize,
    /// Connect-to-first-byte timeout for the proxy's own S3 fetch (default: 5s).
    /// Bounds how long the proxy waits after connecting before receiving the first
    /// response byte from upstream. Above cross-region TTFB tails, well below the
    /// 60s client read-timeout.
    #[serde(
        default = "default_upstream_first_byte_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub upstream_first_byte_timeout: Duration,
    /// Mid-stream idle timeout: no bytes received from upstream for this duration
    /// triggers connection termination (default: 5s). Only applies once the upstream
    /// is actively delivering bytes; a multi-second gap after data has started is
    /// abnormal.
    #[serde(
        default = "default_upstream_idle_timeout",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub upstream_idle_timeout: Duration,
    /// Pre-stream retry budget (default: 2). Number of times to retry an upstream
    /// fetch when first-byte timeout fires before any bytes are sent to the client.
    #[serde(default = "default_upstream_idle_retries")]
    pub upstream_idle_retries: usize,
    /// Per-instance hedged-request governor. Hedging is enabled per key pattern in
    /// `cache_rules.json` — see `hedging_enabled`, `hedge_trigger_after`, and
    /// `hedge_max_per_request` in the rules schema. This section holds only the
    /// process-global in-flight fraction cap.
    #[serde(default)]
    pub hedged_requests: HedgedRequestsConfig,
}

fn default_dns_servers() -> Vec<String> {
    Vec::new() // Empty means use default (Google DNS + Cloudflare DNS)
}

fn default_keepalive_enabled() -> bool {
    true
}

fn default_max_idle_per_host() -> usize {
    100
}

fn default_max_lifetime() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_pool_check_interval() -> Duration {
    Duration::from_secs(10) // 10 seconds
}

fn default_max_idle_per_ip() -> usize {
    10
}

fn default_ip_distribution_enabled() -> bool {
    true
}

fn default_keepalive_idle_secs() -> u64 {
    15
}

fn default_keepalive_interval_secs() -> u64 {
    5
}

fn default_keepalive_retries() -> u32 {
    3
}

fn default_tcp_recv_buffer_size() -> Option<usize> {
    // None lets the Linux kernel auto-tune the TCP receive window. Setting an
    // explicit SO_RCVBUF disables auto-tuning (DRS), which caps single-stream
    // throughput on high-RTT cross-region paths.
    None
}

fn default_ip_failure_threshold() -> u32 {
    3
}

fn default_health_probe_initial_cooldown() -> Duration {
    Duration::from_secs(5)
}

fn default_health_probe_max_cooldown() -> Duration {
    Duration::from_secs(300)
}

fn default_max_registered_endpoints() -> usize {
    10_000
}

fn default_upstream_first_byte_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_upstream_idle_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_upstream_idle_retries() -> usize {
    2
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_ip: 10,
            dns_refresh_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(55),
            keepalive_enabled: default_keepalive_enabled(),
            max_idle_per_host: default_max_idle_per_host(),
            max_lifetime: default_max_lifetime(),
            pool_check_interval: default_pool_check_interval(),
            dns_servers: default_dns_servers(),
            endpoint_overrides: std::collections::HashMap::new(),
            upstream_overrides: std::collections::HashMap::new(),
            ip_distribution_enabled: true,
            max_idle_per_ip: default_max_idle_per_ip(),
            keepalive_idle_secs: default_keepalive_idle_secs(),
            keepalive_interval_secs: default_keepalive_interval_secs(),
            keepalive_retries: default_keepalive_retries(),
            tcp_recv_buffer_size: default_tcp_recv_buffer_size(),
            ip_failure_threshold: default_ip_failure_threshold(),
            health_probe_initial_cooldown: default_health_probe_initial_cooldown(),
            health_probe_max_cooldown: default_health_probe_max_cooldown(),
            max_registered_endpoints: default_max_registered_endpoints(),
            upstream_first_byte_timeout: default_upstream_first_byte_timeout(),
            upstream_idle_timeout: default_upstream_idle_timeout(),
            upstream_idle_retries: default_upstream_idle_retries(),
            hedged_requests: HedgedRequestsConfig::default(),
        }
    }
}

/// Upstream transport scheme for an `upstream_overrides` entry.
///
/// Deserializes from the lowercase strings `"http"` and `"https"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UpstreamScheme {
    /// Plaintext HTTP — no TLS handshake.
    Http,
    /// HTTPS — TLS handshake (validated or unvalidated per `validate_tls`).
    Https,
}

/// Per-destination upstream transport declaration (the value side of an
/// `upstream_overrides` entry).
///
/// `validate_tls` is only meaningful for `scheme: https` and defaults to `true`
/// (secure by default — skipping certificate validation is an explicit opt-in).
/// For `scheme: http` it is ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamOverrideConfig {
    /// Upstream transport scheme: `http` (plaintext) or `https` (TLS).
    pub scheme: UpstreamScheme,
    /// For HTTPS, whether to verify the server certificate against the system trust
    /// store. Defaults to `true`. Ignored for `scheme: http`.
    #[serde(default = "default_true")]
    pub validate_tls: bool,
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub threshold: usize,
    /// Preferred compression algorithm. FUTURE USE — intentionally retained,
    /// not dead code. Production currently always uses Lz4 (handlers are built
    /// via `CompressionHandler::new`); this field is the forward-looking config
    /// surface for planned multi-algorithm support (e.g. zstd), consumed via
    /// `CompressionHandler::new_with_algorithm`/`get_preferred_algorithm`. Do
    /// not remove in dead-code sweeps.
    /// Spec: compression-followup-fixes Requirement 5.
    pub preferred_algorithm: CompressionAlgorithm,
    /// Deprecated, ignored. Content-aware filtering (the built-in extension
    /// denylist) is always applied by default when no `cache_rules.json`
    /// rule explicitly sets `compression_enabled` for a key — there is no
    /// configuration path to disable it. This field has no effect (verified:
    /// no production code path reads it). Accepted via serde alias so
    /// existing config files with `content_aware: true/false` keep parsing;
    /// a startup warning is logged when present. See
    /// compression-content-aware-fix spec, Requirement 4.
    #[serde(default, alias = "content_aware", skip_serializing)]
    pub content_aware_deprecated: Option<bool>,
}

/// Compression algorithms
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum CompressionAlgorithm {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "lz4")]
    #[default]
    Lz4,
    // Future algorithms can be added here
    // #[serde(rename = "zstd")]
    // Zstd,
    // #[serde(rename = "brotli")]
    // Brotli,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: 1024, // 1KB
            preferred_algorithm: CompressionAlgorithm::default(),
            content_aware_deprecated: None,
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub port: u16,
    /// Address to bind the health check server to.
    /// Default "127.0.0.1" listens on loopback only; set "0.0.0.0" (or "::") to accept
    /// connections from other hosts, e.g. a load balancer health check.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub check_interval: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/health".to_string(),
            port: 8080,
            bind_address: default_bind_address(),
            check_interval: Duration::from_secs(30),
        }
    }
}

/// Per-bucket metrics accounting knobs. Nested on MetricsConfig as `metrics.per_bucket`.
/// These govern always-on in-memory accounting that feeds /metrics and the dashboard.
/// The OTLP export gate is `metrics.otlp.per_bucket_enabled` (see OtlpConfig).
/// Design §6b.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerBucketMetricsConfig {
    /// Maximum number of distinct bucket+prefix series tracked in memory (default: 100).
    /// Beyond this, traffic folds into an "__other__" overflow series.
    pub max_series: usize,
    /// Optional per-bucket prefix lists for prefix-level attribution.
    /// Key: bucket name. Value: list of prefix strings.
    pub bucket_prefixes: HashMap<String, Vec<String>>,
}

impl Default for PerBucketMetricsConfig {
    fn default() -> Self {
        Self {
            max_series: 100,
            bucket_prefixes: HashMap::new(),
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub port: u16,
    /// Address to bind the metrics server to.
    /// Default "127.0.0.1" listens on loopback only; set "0.0.0.0" (or "::") to allow
    /// scraping from other hosts.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub collection_interval: Duration,
    /// DEPRECATED: This field is not read by any logic. All cache stats are always included.
    /// Retained for one release cycle for backward-compatible config parsing.
    /// Will be removed in a future release.
    #[serde(default = "default_true")]
    pub include_cache_stats: bool,
    /// DEPRECATED: This field is not read by any logic. All compression stats are always included.
    /// Retained for one release cycle for backward-compatible config parsing.
    /// Will be removed in a future release.
    #[serde(default = "default_true")]
    pub include_compression_stats: bool,
    /// DEPRECATED: This field is not read by any logic. All connection stats are always included.
    /// Retained for one release cycle for backward-compatible config parsing.
    /// Will be removed in a future release.
    #[serde(default = "default_true")]
    pub include_connection_stats: bool,
    #[serde(default)]
    pub otlp: OtlpConfig,
    /// Always-on per-bucket accounting knobs (cardinality cap and prefix attribution).
    /// Governs in-memory accounting fed to /metrics and the dashboard.
    /// The OTLP export gate is metrics.otlp.per_bucket_enabled.
    #[serde(default)]
    pub per_bucket: PerBucketMetricsConfig,
}

/// OTLP (OpenTelemetry Protocol) configuration
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtlpConfig {
    pub enabled: bool,
    pub endpoint: String,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub export_interval: Duration,
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub timeout: Duration,
    pub headers: std::collections::HashMap<String, String>,
    pub compression: OtlpCompression,
    /// Enable per-bucket OTLP export (default: false). In-memory accounting and the
    /// /metrics + dashboard views are always active regardless of this flag.
    /// Setting this to true causes the proxy to emit four per-bucket cumulative counters
    /// (bytes_downloaded, bytes_uploaded, get_requests, put_requests)
    /// via OTLP on every collection cycle, each with a `bucket` attribute.
    /// Design §6a.
    #[serde(default)]
    pub per_bucket_enabled: bool,
}

/// Manual Debug impl that redacts header values (may contain auth tokens/API keys).
impl std::fmt::Debug for OtlpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpConfig")
            .field("enabled", &self.enabled)
            .field("endpoint", &self.endpoint)
            .field("export_interval", &self.export_interval)
            .field("timeout", &self.timeout)
            .field(
                "headers",
                &format_args!("<{} redacted>", self.headers.len()),
            )
            .field("compression", &self.compression)
            .field("per_bucket_enabled", &self.per_bucket_enabled)
            .finish()
    }
}

/// OTLP compression options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum OtlpCompression {
    #[serde(rename = "none")]
    #[default]
    None,
    #[serde(rename = "gzip")]
    Gzip,
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
            per_bucket_enabled: false,
        }
    }
}

impl MetricsConfig {
    /// The shortest `collection_interval` the proxy will run; a smaller value is clamped up to it
    /// by [`MetricsConfig::validate_and_clamp`].
    ///
    /// # Why 10 seconds
    ///
    /// The field became live when the background collector was added; before that it was read by
    /// nothing, so no deployment has ever run at any value of it. A collection's cost is dominated
    /// by shared-volume document reads and was measured on the fleet at 6.5-10 s before the
    /// repair-tree reads were de-duplicated. The collector never overlaps runs and skips missed
    /// ticks, so an interval shorter than one run does not run collections faster than they
    /// complete — it runs them back to back, permanently, which is a constant shared-volume load
    /// and a blocking-pool thread held forever for a figure nobody reads more often than a
    /// scraper polls. Ten seconds is the measured worst run, rounded, so a clamped deployment
    /// still leaves the volume idle between runs on the fleet this was measured on.
    pub const MIN_COLLECTION_INTERVAL_SECS: u64 = 10;
    /// The longest `collection_interval` the proxy will run; a larger value is shortened to it by
    /// [`MetricsConfig::validate_and_clamp`].
    ///
    /// One day. Nothing needs a ceiling for correctness, but an unbounded interval is
    /// config-reachable all the way to `Instant + Duration` overflow inside `tokio::time::interval`,
    /// which panics in a spawned task where the panic is swallowed and the collector silently does
    /// not exist. Past a day the shared-volume figures would be too old to act on anyway.
    pub const MAX_COLLECTION_INTERVAL_SECS: u64 = 86_400;

    /// Bring `collection_interval` inside its range, warning rather than rejecting.
    ///
    /// **Why this clamps rather than rejects.** The field was accepted and ignored for its whole
    /// life, so any value an existing file carries started the proxy yesterday. Rejecting one now
    /// would turn a routine upgrade into a startup failure, which
    /// the config compatibility rule forbids. The same reasoning, and the same warning
    /// dialect, as `DownloadCoordinationConfig::validate_and_clamp`.
    pub fn validate_and_clamp(&mut self) {
        let secs = self.collection_interval.as_secs();
        if secs < Self::MIN_COLLECTION_INTERVAL_SECS {
            warn!(
                "metrics.collection_interval ({:?}) is below minimum ({}s), clamping to {}s",
                self.collection_interval,
                Self::MIN_COLLECTION_INTERVAL_SECS,
                Self::MIN_COLLECTION_INTERVAL_SECS
            );
            self.collection_interval = Duration::from_secs(Self::MIN_COLLECTION_INTERVAL_SECS);
        } else if secs > Self::MAX_COLLECTION_INTERVAL_SECS {
            warn!(
                "metrics.collection_interval ({}s) exceeds maximum ({}s), shortening to {}s",
                secs,
                Self::MAX_COLLECTION_INTERVAL_SECS,
                Self::MAX_COLLECTION_INTERVAL_SECS
            );
            self.collection_interval = Duration::from_secs(Self::MAX_COLLECTION_INTERVAL_SECS);
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".to_string(),
            port: 9090,
            bind_address: default_bind_address(),
            collection_interval: Duration::from_secs(60),
            include_cache_stats: true,
            include_compression_stats: true,
            include_connection_stats: true,
            otlp: OtlpConfig::default(),
            per_bucket: PerBucketMetricsConfig::default(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Download Bandwidth QoS configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Caller-identity fairness key configuration.
///
/// When `enabled = true`, the proxy extracts the `app/<value>` token from the
/// User-Agent header and uses it as the per-request fairness key.  Non-matching
/// or absent tokens fall back to the S3 bucket key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CallerIdConfig {
    /// Use the User-Agent app-id (`app/<value>`) as the fairness key when present
    /// and valid.  Default: `false` (every request keys on bucket).
    pub enabled: bool,
    /// Optional regex to validate the caller value extracted from the User-Agent.
    /// A value that does not match falls back to the bucket key.
    /// Compiled once at startup; an invalid regex is a startup error.
    pub validation_regex: Option<String>,
    /// Maximum character length for the caller value (default: 64).
    /// Values longer than this are treated as absent (bucket fallback).
    pub max_len: usize,
}

impl Default for CallerIdConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            validation_regex: None,
            max_len: 64,
        }
    }
}

/// Fleet sharing configuration for the `cap / N` fairness model.
///
/// The proxy derives the live instance count `N` from heartbeat files it writes
/// to `cache_dir/qos/heartbeats/` — a dedicated subtree, never under `metadata/`,
/// so liveness state is isolated from the cache-metadata sweeps.  Each instance
/// periodically refreshes `N` on a cold path; the hot request path only reads a
/// single atomic.
///
/// Because the path is derived from `cache.cache_dir`, a deployment giving each
/// instance its own cache directory counts one instance everywhere and every
/// proxy takes the whole aggregate ceiling.  `fallback_instance_count` is the
/// floor that corrects this — see its own doc comment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FleetSharingConfig {
    /// **Floor** on the live instance count, not merely a fallback: the effective
    /// `N` is `max(counted, fallback_instance_count)`, so it applies continuously
    /// rather than only when the count cannot be read.  For a fleet deployment,
    /// set this to the fleet size so the per-instance ceiling is
    /// `cap / fallback_instance_count` under coordination loss (safe: throttles
    /// more heavily, never unlimited).  Default: `1` (correct for single-instance;
    /// fleet operators must override).
    ///
    /// Required on a deployment where `cache.cache_dir` is per-instance, since the
    /// counted value is then always 1 and this is the only thing dividing the
    /// aggregate ceiling.  See `docs/LOCAL_NVME_CACHE.md`.
    pub fallback_instance_count: u32,
    /// Heartbeat staleness window: `.qos` heartbeat files older than this are
    /// excluded when counting live instances `N`.  Default: 90 s (three refresh
    /// intervals).  A value below twice the refresh interval is raised to three
    /// times it, with a startup warning; see `effective_instance_staleness`.
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub instance_staleness: Duration,
    /// Cold-path cadence for the heartbeat touch + `qos/heartbeats/` readdir.
    /// Floor: 10 s.  Default: 30 s.
    #[serde(deserialize_with = "duration_serde::deserialize")]
    pub refresh_interval: Duration,
}

impl Default for FleetSharingConfig {
    fn default() -> Self {
        Self {
            fallback_instance_count: 1,
            instance_staleness: Duration::from_secs(90),
            refresh_interval: Duration::from_secs(30),
        }
    }
}

/// Smallest staleness window, in refresh intervals, that the fleet task honours as
/// configured. Below it a heartbeat that lands less than one interval late drops a
/// live peer out of `N`.
const MIN_STALENESS_REFRESH_MULTIPLE: u32 = 2;

/// Staleness window, in refresh intervals, used as the default and as the value a
/// too-small setting is raised to: a peer drops out of `N` only after missing two
/// consecutive heartbeats.
const STALENESS_REFRESH_MULTIPLE: u32 = 3;

impl FleetSharingConfig {
    /// The refresh interval the fleet task actually runs at (floor 10 s).
    fn effective_refresh_interval(&self) -> Duration {
        std::cmp::max(self.refresh_interval, Duration::from_secs(10))
    }

    /// The staleness window the fleet task counts live instances with.
    ///
    /// A value below `2 x` the effective refresh interval is raised to `3 x`,
    /// because a peer's newest heartbeat is up to one full interval old whenever
    /// this instance reads the directory, before the write, the shared file
    /// system's mtime and clock skew between hosts add anything. Counting a live
    /// peer as stale raises every other instance's ceiling.
    pub(crate) fn effective_instance_staleness(&self) -> Duration {
        let refresh = self.effective_refresh_interval();
        if self.instance_staleness < refresh.saturating_mul(MIN_STALENESS_REFRESH_MULTIPLE) {
            refresh.saturating_mul(STALENESS_REFRESH_MULTIPLE)
        } else {
            self.instance_staleness
        }
    }

    /// The startup warning for a staleness window that was raised, if it was.
    pub(crate) fn instance_staleness_warning(&self) -> Option<String> {
        let effective = self.effective_instance_staleness();
        (effective != self.instance_staleness).then(|| {
            format!(
                "download_bandwidth.fleet.instance_staleness ({}s) is less than twice \
                 refresh_interval ({}s), so a live instance whose heartbeat is slightly late \
                 would be counted as stopped and the others would raise their ceilings. \
                 Using {}s instead. Set instance_staleness to at least {}s to silence this warning.",
                self.instance_staleness.as_secs_f64(),
                self.effective_refresh_interval().as_secs_f64(),
                effective.as_secs_f64(),
                self.effective_refresh_interval()
                    .saturating_mul(MIN_STALENESS_REFRESH_MULTIPLE)
                    .as_secs_f64(),
            )
        })
    }
}

/// Download bandwidth QoS (fairness) configuration.
///
/// Controls the aggregate rate limit on cache-miss origin downloads and the
/// fairness scheduling across caller / bucket classes.
///
/// All fields have serde defaults: omitting the entire section leaves the feature
/// **disabled** (`max_bytes_per_sec = 0`), so a binary-only upgrade never changes
/// behaviour until the operator opts in.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadBandwidthConfig {
    /// Aggregate cache-miss download ceiling in bytes per second.
    ///
    /// `0` (default) = unlimited / feature disabled.  When disabled the proxy
    /// streaming path is byte-for-byte and latency-for-latency unchanged; the
    /// only cost is a single relaxed atomic load per stream.
    pub max_bytes_per_sec: u64,
    /// Caller-identity fairness key configuration.
    pub caller_id: CallerIdConfig,
    /// Maximum number of retained per-class metric series (top-K cap).
    /// Classes outside the top-K are folded into a single `residual` series;
    /// they are still throttled and counted against the ceiling.  Default: 1024.
    pub max_tracked_classes: usize,
    /// Fleet sharing (`cap / N`) configuration.
    pub fleet: FleetSharingConfig,
}

impl Default for DownloadBandwidthConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_sec: 0,
            caller_id: CallerIdConfig::default(),
            max_tracked_classes: 1024,
            fleet: FleetSharingConfig::default(),
        }
    }
}

impl DownloadBandwidthConfig {
    /// Validate the configuration.
    ///
    /// Compiles `caller_id.validation_regex` so that an invalid regex is caught
    /// at startup rather than silently falling back to no validation at runtime.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if let Some(ref pattern) = self.caller_id.validation_regex {
            regex::Regex::new(pattern).map_err(|e| {
                format!(
                    "download_bandwidth.caller_id.validation_regex is invalid: {}",
                    e
                )
            })?;
        }
        if self.caller_id.max_len == 0 {
            return Err("download_bandwidth.caller_id.max_len must be greater than 0".to_string());
        }
        if self.fleet.fallback_instance_count == 0 {
            return Err(
                "download_bandwidth.fleet.fallback_instance_count must be at least 1".to_string(),
            );
        }
        let refresh_secs = self.fleet.refresh_interval.as_secs();
        if refresh_secs < 10 {
            return Err(format!(
                "download_bandwidth.fleet.refresh_interval must be at least 10s, got {}s",
                refresh_secs
            ));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                mode: ServerMode::Standard,
                http_port: 80,
                https_port: 443,
                proxy_port: 3128,
                max_concurrent_requests: default_max_concurrent_requests(),
                request_timeout: Duration::from_secs(30),
                add_referer_header: true,
                max_buffered_request_body_bytes: default_max_buffered_request_body_bytes(),
                write_cache_tee_channel_depth: default_write_cache_tee_channel_depth(),
                max_inflight_buffer_bytes: default_max_inflight_buffer_bytes(),
                tls: None,
            },
            cache: CacheConfig {
                cache_dir: PathBuf::from("/cache"),
                max_cache_size: 10 * 1024 * 1024 * 1024, // 10GB
                ram_cache_enabled: false,
                // 512MB: yields 8 effective shards (512MiB / 64MiB admission ceiling)
                // out of the box, preserving the pre-clamp default shard concurrency.
                // Spec: page-aligned-range-cache Resolved Question 4.
                max_ram_cache_size: 512 * 1024 * 1024, // 512MB
                eviction_algorithm: EvictionAlgorithm::default(), // TinyLFU for both RAM and disk (R10.5)
                write_cache_enabled: true, // Enabled by default - write caching is complete
                write_cache_percent: 10.0,
                write_cache_max_object_size: 256 * 1024 * 1024, // 256MB
                put_ttl: Duration::from_secs(3600),             // 1 hour
                get_ttl: Duration::from_secs(86400),            // 1 day
                head_ttl: Duration::from_secs(60),              // 1 minute
                actively_remove_cached_data: false,             // Lazy expiration by default
                // Coordination is unconditional; this configures its parameters, including lock timeouts.
                shared_storage: SharedStorageConfig::default(),
                download_coordination: DownloadCoordinationConfig::default(), // Enabled by default
                range_merge_gap_threshold: 1024 * 1024,                       // 1MiB
                eviction_buffer_percent: 5,                                   // 5% buffer
                ram_cache_flush_interval: Duration::from_secs(10),            // 10 seconds (Req 19)
                ram_cache_verification_interval: Duration::from_secs(1),      // 1 second
                incomplete_upload_ttl: Duration::from_secs(86400),            // 1 day
                initialization: InitializationConfig::default(), // Default initialization config
                cache_bypass_headers_enabled: true,              // Enabled by default
                metadata_cache: MetadataCacheConfig::default(),  // RAM metadata cache config
                eviction_trigger_percent: 95,                    // Trigger eviction at 95% capacity
                eviction_target_percent: 80,                     // Reduce to 80% after eviction
                full_object_check_threshold: 67_108_864,         // 64 MiB
                disk_streaming_threshold: 1_048_576,             // 1 MiB
                compression_batch_size: 1_048_576,               // 1 MiB
                read_cache_enabled: true,                        // Read caching enabled by default
                bucket_settings_staleness_threshold: Duration::from_secs(60), // 60 seconds
                evaluate_conditions_from_cache: true, // Default: serve If-Match from cache; If-None-Match/etc still forward
                ram_cache_shard_count: default_ram_cache_shard_count(),
                max_complete_body_bytes: default_max_complete_body_bytes(),
                max_metadata_file_bytes: default_max_metadata_file_bytes(),
                metadata_io_concurrency: default_metadata_io_concurrency(),
                partial_range_commit_ratio: default_partial_range_commit_ratio(),
                disk_safety_free_space_floor: default_disk_safety_free_space_floor(),
            },
            logging: LoggingConfig {
                access_log_dir: PathBuf::from("/logs/access"),
                app_log_dir: PathBuf::from("/logs/app"),
                local_state_dir: default_local_state_dir(),
                access_log_enabled: true,
                access_log_mode: AccessLogMode::default(),
                log_level: "info".to_string(),
                access_log_flush_interval: default_access_log_flush_interval(),
                access_log_buffer_size: default_access_log_buffer_size(),
                access_log_retention_days: default_access_log_retention_days(),
                app_log_retention_days: default_app_log_retention_days(),
                log_cleanup_interval: default_log_cleanup_interval(),
                access_log_file_rotation_interval: default_access_log_file_rotation_interval(),
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
                upstream_overrides: std::collections::HashMap::new(),
                ip_distribution_enabled: true,
                max_idle_per_ip: default_max_idle_per_ip(),
                keepalive_idle_secs: default_keepalive_idle_secs(),
                keepalive_interval_secs: default_keepalive_interval_secs(),
                keepalive_retries: default_keepalive_retries(),
                tcp_recv_buffer_size: default_tcp_recv_buffer_size(),
                ip_failure_threshold: default_ip_failure_threshold(),
                health_probe_initial_cooldown: default_health_probe_initial_cooldown(),
                health_probe_max_cooldown: default_health_probe_max_cooldown(),
                max_registered_endpoints: default_max_registered_endpoints(),
                upstream_first_byte_timeout: default_upstream_first_byte_timeout(),
                upstream_idle_timeout: default_upstream_idle_timeout(),
                upstream_idle_retries: default_upstream_idle_retries(),
                hedged_requests: HedgedRequestsConfig::default(),
            },
            compression: CompressionConfig {
                enabled: true,
                threshold: 1024, // 1KB
                preferred_algorithm: CompressionAlgorithm::default(),
                content_aware_deprecated: None,
            },
            health: HealthConfig {
                enabled: true,
                endpoint: "/health".to_string(),
                port: 8080,
                bind_address: default_bind_address(),
                check_interval: Duration::from_secs(30),
            },
            metrics: MetricsConfig {
                enabled: true,
                endpoint: "/metrics".to_string(),
                port: 8080,
                bind_address: default_bind_address(),
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
                    per_bucket_enabled: false,
                },
                per_bucket: PerBucketMetricsConfig::default(),
            },
            dashboard: DashboardConfig::default(),
            download_bandwidth: DownloadBandwidthConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from file, environment, and command line
    pub fn load() -> Result<Self> {
        let matches = Self::build_cli().get_matches();
        Self::load_from_matches(&matches)
    }

    /// `load` for an already-parsed command line, so a test can drive the whole startup
    /// path (file, environment, flags, validation) without the process arguments.
    pub(crate) fn load_from_matches(matches: &clap::ArgMatches) -> Result<Self> {
        let mut config = Self::default();

        // Load from config file if specified
        if let Some(config_path) = matches.get_one::<String>("config") {
            config = Self::load_from_file(config_path)?;
        }

        // Override with environment variables
        config.apply_env_overrides();

        // Override with command line arguments
        config.apply_cli_overrides(matches);

        // Deprecation warning: `compression.content_aware` never had any
        // effect (verified: no production code path ever read it) and has
        // been removed as a live field. It still parses via a serde alias
        // for backward compatibility, but a value here is now silently
        // ignored. See compression-content-aware-fix spec, Requirement 4.
        if config.compression.content_aware_deprecated.is_some() {
            tracing::warn!(
                "compression.content_aware is deprecated and has no effect (it never did); \
                 the built-in extension denylist is now always applied by default when no \
                 cache_rules.json rule explicitly sets compression_enabled for a key. \
                 Remove this field from your config."
            );
        }

        // The retired orphan-sweep fields parse and are ignored; one warning if any is set.
        config.cache.shared_storage.warn_retired_orphan_fields();

        // Validate shared storage configuration
        if let Err(e) = config.cache.shared_storage.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid shared storage configuration: {}",
                e
            )));
        }
        // Publish the metadata slot-lock wait budget to the acquisition primitives.
        //
        // They are synchronous free functions, some of them running inside blocking-pool
        // closures, with no handle to reach a `Config` through — so the value is carried in a
        // process-wide atomic and seeded here, AFTER validation, which is the one point every
        // startup passes through exactly once and before any cache manager is constructed.
        crate::metadata_lock_manager::set_slot_acquire_wait_budget(
            config.cache.shared_storage.lock_acquire_wait_budget,
        );

        // Validate cache configuration (compression_batch_size, TTL bounds)
        // Requirement: 8.5
        if let Err(e) = config.cache.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid cache configuration: {}",
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

        // Validate dashboard configuration
        if let Err(e) = config.dashboard.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid dashboard configuration: {}",
                e
            )));
        }

        // Health and metrics bind addresses must be IP addresses; a value that does not
        // parse is refused here rather than falling back to every interface at bind time.
        config.validate_listener_addresses()?;
        // Validate download bandwidth QoS configuration (compiles regex at startup)
        if let Err(e) = config.download_bandwidth.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid download_bandwidth configuration: {}",
                e
            )));
        }

        // Validate logging configuration
        if let Err(e) = config.logging.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid logging configuration: {}",
                e
            )));
        }

        // Validate server configuration against the internal buffered-body bound.
        // Requirement: 4.5
        if let Err(e) = config.server.validate() {
            return Err(ProxyError::ConfigError(format!(
                "Invalid server configuration: {}",
                e
            )));
        }

        // Validate TLS configuration
        // Requirements: 6.2, 6.3, 6.5
        config.validate_tls_config()?;

        // Validate proxy-only mode configuration
        // Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
        config.validate_proxy_only_mode()?;

        // Validate and clamp download coordination configuration
        config.cache.download_coordination.validate_and_clamp();
        // Clamp the metrics collection interval, which became live with the background collector
        // and so must not reject a value that booted while it was ignored.
        config.metrics.validate_and_clamp();
        // The metadata cache's documented ranges were never enforced. Warn, never reject and
        // never clamp: see `warn_if_outside_documented_range` for why a clamp is unsafe here.
        config
            .cache
            .metadata_cache
            .warn_if_outside_documented_range();

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
            "Shared storage mode: lock_acquire_wait_budget={}s",
            config
                .cache
                .shared_storage
                .lock_acquire_wait_budget
                .as_secs()
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
                "Connection keepalive enabled (idle_timeout: {}s, max_idle_per_host: {})",
                config.connection_pool.idle_timeout.as_secs(),
                config.connection_pool.max_idle_per_host
            );
        } else {
            info!("Connection keepalive disabled");
        }

        // Log dashboard configuration
        if config.dashboard.enabled {
            info!(
                "Dashboard enabled on {}:{} (cache_stats_refresh: {}s, logs_refresh: {}s)",
                config.dashboard.bind_address,
                config.dashboard.port,
                config.dashboard.cache_stats_refresh_interval.as_secs(),
                config.dashboard.logs_refresh_interval.as_secs(),
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

        // Log server mode and relevant ports
        match config.server.mode {
            ServerMode::Standard => {
                info!("Server mode: standard");
                info!(
                    "HTTP port: {}, HTTPS port: {}",
                    config.server.http_port, config.server.https_port
                );
            }
            ServerMode::ProxyOnly => {
                info!("Server mode: proxy_only");
                info!("HTTP forward proxy port: {}", config.server.proxy_port);
                info!("HTTP/HTTPS direct listeners will not be started");
            }
        }

        info!("Configuration loaded successfully");
        debug!("Configuration: {:?}", config);

        // Validate per-bucket prefix lists: reject empty-string prefixes with a warning.
        // An empty string matches every key (zero-length prefix), which is equivalent to
        // bucket-level attribution — the caller should simply omit the prefix list entry.
        // Design Error Handling table.
        for (bucket, prefixes) in config.metrics.per_bucket.bucket_prefixes.iter_mut() {
            let before = prefixes.len();
            prefixes.retain(|p| {
                if p.is_empty() {
                    warn!(
                        "Empty string in metrics.per_bucket.bucket_prefixes[\"{}\"] removed \
                         (an empty prefix matches all keys and is equivalent to bucket-level \
                         attribution — omit the entry instead)",
                        bucket
                    );
                    false
                } else {
                    true
                }
            });
            let removed = before - prefixes.len();
            if removed > 0 {
                debug!(
                    "Removed {} empty prefix(es) from bucket_prefixes[\"{}\"]",
                    removed, bucket
                );
            }
        }

        // Log deprecation warnings for dead config fields (Req 20.5)
        config.log_deprecated_fields();

        Ok(config)
    }

    /// The warning for `health.check_interval` and the three `cache.initialization` fields,
    /// or `None` when every one of them holds its default. They parse, so a config that
    /// loaded before keeps loading, but nothing reads them. One warning names each one set.
    fn unread_fields_warning(&self) -> Option<String> {
        let mut set = Vec::new();
        if self.health.check_interval != HealthConfig::default().check_interval {
            set.push("health.check_interval");
        }
        let init = &self.cache.initialization;
        if init.parallel_scan != default_parallel_scan() {
            set.push("cache.initialization.parallel_scan");
        }
        if init.scan_timeout != default_scan_timeout() {
            set.push("cache.initialization.scan_timeout");
        }
        if init.progress_logging != default_progress_logging() {
            set.push("cache.initialization.progress_logging");
        }
        if set.is_empty() {
            return None;
        }
        Some(format!(
            "Configuration field(s) '{}' are ignored: nothing in the proxy reads them \
             (the PARALLEL_SCAN, SCAN_TIMEOUT and PROGRESS_LOGGING environment variables and the \
             --parallel-scan, --no-parallel-scan, --scan-timeout, --progress-logging and \
             --no-progress-logging flags set the cache.initialization fields). Delete them to \
             silence this warning.",
            set.join("', '")
        ))
    }

    /// Log deprecation warnings for config fields that are no longer read by any logic.
    /// These fields are retained for one release cycle for backward-compatible config parsing.
    /// Requirement: 20.5
    fn log_deprecated_fields(&self) {
        if self.cache.eviction_buffer_percent != default_eviction_buffer_percent() {
            warn!(
                "Configuration field 'cache.eviction_buffer_percent' is deprecated and ignored: \
                 eviction is controlled by 'cache.eviction_trigger_percent' (default 95%) \
                 and 'cache.eviction_target_percent' (default 80%). Delete the field to silence this warning."
            );
        }
        if !self.metrics.include_cache_stats {
            warn!(
                "Configuration field 'metrics.include_cache_stats' is deprecated and ignored: \
                 all cache stats are always included. Delete the field to silence this warning."
            );
        }
        if !self.metrics.include_compression_stats {
            warn!(
                "Configuration field 'metrics.include_compression_stats' is deprecated and ignored: \
                 all compression stats are always included. Delete the field to silence this warning."
            );
        }
        if !self.metrics.include_connection_stats {
            warn!(
                "Configuration field 'metrics.include_connection_stats' is deprecated and ignored: \
                 all connection stats are always included. Delete the field to silence this warning."
            );
        }
        if self.connection_pool.max_connections_per_ip != 10 {
            warn!(
                "Configuration field 'connection_pool.max_connections_per_ip' is deprecated and ignored: \
                 connection pooling is managed by 'connection_pool.max_idle_per_host' and \
                 'connection_pool.max_idle_per_ip'. Delete the field to silence this warning."
            );
        }
        if self.dashboard.max_log_entries != 100 {
            warn!(
                "Configuration field 'dashboard.max_log_entries' is deprecated and ignored by the \
                 log viewer (the value is still range-checked, 10-10000). \
                 Delete the field to silence this warning."
            );
        }
        if let Some(message) = self
            .server
            .max_buffered_request_body_bytes_deprecation_warning()
        {
            warn!("{}", message);
        }
        // Unlike every other arm in this function, this one is not about a dead field:
        // `"lru"` is still accepted and still starts the proxy. What it no longer does is
        // select an algorithm — 3.0.0 moved the default away from LRU (R10.5) and then
        // removed it as a choice (R10.10), so the value now resolves to TinyLFU. Without
        // this arm that remapping happens in total silence. See the helper's doc comment.
        if let Some(message) = self.cache.eviction_algorithm_deprecation_warning() {
            warn!("{}", message);
        }
        if let Some(message) = self.unread_fields_warning() {
            warn!("{}", message);
        }
        // `cache.ram_cache_flush_threshold` and `cache.ram_cache_flush_on_eviction` are
        // REMOVED rather than merely dead, so there is no field left to compare against a
        // default. They are warned about from the loaded document instead, by
        // `warn_on_removed_cache_keys`.
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
                    .help("Ignored: has no effect (sets cache.initialization.parallel_scan)"),
            )
            .arg(
                Arg::new("no-parallel-scan")
                    .long("no-parallel-scan")
                    .action(clap::ArgAction::SetTrue)
                    .help("Ignored: has no effect (sets cache.initialization.parallel_scan)"),
            )
            .arg(
                Arg::new("scan-timeout")
                    .long("scan-timeout")
                    .value_name("SECONDS")
                    .help("Ignored: has no effect (sets cache.initialization.scan_timeout)"),
            )
            .arg(
                Arg::new("progress-logging")
                    .long("progress-logging")
                    .action(clap::ArgAction::SetTrue)
                    .help("Ignored: has no effect (sets cache.initialization.progress_logging)"),
            )
            .arg(
                Arg::new("no-progress-logging")
                    .long("no-progress-logging")
                    .action(clap::ArgAction::SetTrue)
                    .help("Ignored: has no effect (sets cache.initialization.progress_logging)"),
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
                    .help("Dashboard server port (default: 8081)"),
            )
            .arg(
                Arg::new("dashboard-bind-address")
                    .long("dashboard-bind-address")
                    .value_name("ADDRESS")
                    .help("Dashboard server bind address (default: 127.0.0.1)"),
            )
            // Shared storage configuration CLI arguments
            .arg(
                Arg::new("lock-acquire-wait-budget")
                    .long("lock-acquire-wait-budget")
                    .value_name("SECONDS")
                    .help(
                        "How long a metadata slot-lock acquisition waits before giving up \
                         (default: 5)",
                    ),
            )
            .arg(
                // Renamed to `--lock-acquire-wait-budget`. Kept registered and hidden so an
                // existing service unit that passes it still starts, and it still SETS the
                // budget rather than being ignored. `apply_cli_overrides` warns when supplied.
                Arg::new("lock-timeout")
                    .long("lock-timeout")
                    .value_name("SECONDS")
                    .hide(true)
                    .help("Deprecated alias for --lock-acquire-wait-budget"),
            )
            .arg(
                // Removed and ignored. Kept registered and hidden so an existing service unit
                // or wrapper script that passes it still starts — clap errors on an unknown
                // argument, so deleting it outright would turn a removed field into a failed
                // boot. `apply_cli_overrides` warns when it is supplied.
                Arg::new("lock-refresh-interval")
                    .long("lock-refresh-interval")
                    .value_name("SECONDS")
                    .hide(true)
                    .help("Removed and ignored; accepted for backward compatibility"),
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
                    .hide(true)
                    .help("Removed and ignored; accepted for backward compatibility"),
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

        let config: Self = serde_yaml_ng::from_str(&content).map_err(|e| {
            ProxyError::ConfigError(format!("Failed to parse config file {}: {}", path, e))
        })?;

        // Removed keys are silently ignored by serde, which is what keeps an existing config
        // loading across an upgrade — but silence means an operator who set one never learns
        // it does nothing. Re-parse as a document to warn by key name.
        if let Ok(document) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&content) {
            warn_on_removed_cache_keys(&document);
            warn_on_removed_server_keys(&document);
            warn_on_inert_cache_keys(&document);
        }

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
        if let Ok(budget) = std::env::var("LOCK_ACQUIRE_WAIT_BUDGET") {
            if let Ok(budget_secs) = budget.parse::<u64>() {
                self.cache.shared_storage.lock_acquire_wait_budget =
                    Duration::from_secs(budget_secs);
            }
        }
        // `LOCK_TIMEOUT` set `cache.shared_storage.lock_timeout`, which is now
        // `lock_acquire_wait_budget`. It still SETS the budget rather than being ignored, so an
        // existing environment keeps the value the operator chose instead of silently reverting
        // to the default — and it is warned about so the rename is visible. `LOCK_TIMEOUT` is
        // read after `LOCK_ACQUIRE_WAIT_BUDGET`, so setting both leaves the deprecated one
        // winning; that is stated rather than left to be discovered.
        if let Ok(timeout) = std::env::var("LOCK_TIMEOUT") {
            warn!(
                "Environment variable 'LOCK_TIMEOUT' is deprecated: it now sets \
                 'cache.shared_storage.lock_acquire_wait_budget'. Rename it to \
                 'LOCK_ACQUIRE_WAIT_BUDGET'. The default is 5s, so a value carried over from \
                 the old 60s default will make a contended metadata lock wait twelve times \
                 longer before giving up."
            );
            if let Ok(timeout_secs) = timeout.parse::<u64>() {
                self.cache.shared_storage.lock_acquire_wait_budget =
                    Duration::from_secs(timeout_secs);
            }
        }

        // `LOCK_REFRESH_INTERVAL` set the removed `cache.shared_storage.lock_refresh_interval`.
        // It is still accepted so an existing environment does not change the exit status of a
        // start, and warned about so the operator gets a signal rather than silence.
        if std::env::var("LOCK_REFRESH_INTERVAL").is_ok() {
            warn!(
                "Environment variable 'LOCK_REFRESH_INTERVAL' is ignored: \
                 'cache.shared_storage.lock_refresh_interval' has been removed. Metadata locks \
                 are never refreshed because they never expire, so there was nothing to refresh."
            );
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

        if std::env::var("RECOVERY_MAX_CONCURRENT").is_ok() {
            warn!(
                "Environment variable 'RECOVERY_MAX_CONCURRENT' is ignored: \
                 'cache.shared_storage.recovery_max_concurrent' has been removed."
            );
        }

        if let Ok(interval) = std::env::var("CONSOLIDATION_INTERVAL") {
            if let Ok(interval_secs) = interval.parse::<u64>() {
                self.cache.shared_storage.consolidation_interval =
                    Duration::from_secs(interval_secs);
            }
        }

        if let Ok(threshold) = std::env::var("CONSOLIDATION_SIZE_THRESHOLD") {
            warn!("{}", CONSOLIDATION_SIZE_THRESHOLD_DEPRECATION);
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
        if let Some(budget) = matches.get_one::<String>("lock-acquire-wait-budget") {
            if let Ok(budget_secs) = budget.parse::<u64>() {
                self.cache.shared_storage.lock_acquire_wait_budget =
                    Duration::from_secs(budget_secs);
            }
        }
        // `--lock-timeout` set `cache.shared_storage.lock_timeout`, which is now
        // `lock_acquire_wait_budget`. The argument stays registered because clap rejects an
        // unknown argument, which would turn a rename into a failed boot for any service unit
        // still passing it — the `--lock-refresh-interval` lesson. Unlike that one it still
        // SETS the value, so an existing unit file keeps its operator's choice.
        if let Some(timeout) = matches.get_one::<String>("lock-timeout") {
            warn!(
                "'--lock-timeout' is deprecated: it now sets \
                 'cache.shared_storage.lock_acquire_wait_budget'. Rename it to \
                 '--lock-acquire-wait-budget'. The default is 5s, so a value carried over from \
                 the old 60s default will make a contended metadata lock wait twelve times \
                 longer before giving up."
            );
            if let Ok(timeout_secs) = timeout.parse::<u64>() {
                self.cache.shared_storage.lock_acquire_wait_budget =
                    Duration::from_secs(timeout_secs);
            }
        }

        // `--lock-refresh-interval` set the removed `cache.shared_storage.lock_refresh_interval`.
        // The argument is still registered (hidden) so a service unit passing it still starts;
        // clap rejects an unknown argument, which would turn a removed field into a failed boot.
        if matches.get_one::<String>("lock-refresh-interval").is_some() {
            warn!(
                "'--lock-refresh-interval' is ignored: \
                 'cache.shared_storage.lock_refresh_interval' has been removed. Metadata locks \
                 are never refreshed because they never expire, so there was nothing to refresh."
            );
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

        if matches
            .get_one::<String>("recovery-max-concurrent")
            .is_some()
        {
            warn!(
                "'--recovery-max-concurrent' is ignored: \
                 'cache.shared_storage.recovery_max_concurrent' has been removed."
            );
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

    /// Validate TLS proxy listener configuration.
    /// Checks TlsConfig fields and ensures tls_proxy_port does not conflict with other ports.
    /// Requirements: 6.2, 6.3, 6.5
    /// Reject a `health.bind_address` or `metrics.bind_address` that is not an IP address.
    ///
    /// Only an enabled listener is checked: a disabled one never binds, so an unparseable
    /// value there has never had an effect and must not stop an existing deployment.
    fn validate_listener_addresses(&self) -> Result<()> {
        if self.health.enabled {
            validate_listener_ip("health.bind_address", &self.health.bind_address)?;
        }
        if self.metrics.enabled {
            validate_listener_ip("metrics.bind_address", &self.metrics.bind_address)?;
        }
        Ok(())
    }

    fn validate_tls_config(&self) -> Result<()> {
        if let Some(tls) = &self.server.tls {
            if let Err(e) = tls.validate() {
                return Err(ProxyError::ConfigError(format!(
                    "Invalid TLS configuration: {}",
                    e
                )));
            }

            // Check port conflicts only when TLS is enabled
            // Requirement: 6.5
            if tls.enabled {
                let port = tls.tls_proxy_port;
                let conflicts: &[(&str, u16)] = &[
                    ("http_port", self.server.http_port),
                    ("https_port", self.server.https_port),
                    ("health.port", self.health.port),
                    ("metrics.port", self.metrics.port),
                    ("dashboard.port", self.dashboard.port),
                ];
                for (name, other_port) in conflicts {
                    if port == *other_port {
                        return Err(ProxyError::ConfigError(format!(
                            "tls.tls_proxy_port ({}) conflicts with {} ({})",
                            port, name, other_port
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate proxy-only mode configuration.
    /// Returns Ok(()) immediately when mode is Standard.
    /// In ProxyOnly mode, validates proxy_port is non-zero, does not conflict
    /// with infrastructure ports, and warns on privileged ports.
    /// Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
    fn validate_proxy_only_mode(&self) -> Result<()> {
        if self.server.mode != ServerMode::ProxyOnly {
            return Ok(());
        }

        // Validate proxy_port is not 0
        if self.server.proxy_port == 0 {
            return Err(ProxyError::ConfigError(
                "server.proxy_port must not be 0 in proxy_only mode".into(),
            ));
        }

        // Warn if proxy_port < 1024 (defeats purpose of proxy-only mode)
        if self.server.proxy_port < 1024 {
            warn!(
                "server.proxy_port={} requires elevated privileges; \
                 proxy_only mode is designed for non-privileged ports (>= 1024)",
                self.server.proxy_port
            );
        }

        // Check proxy_port doesn't conflict with health/metrics/dashboard ports
        let reserved: &[(u16, &str)] = &[
            (self.health.port, "health"),
            (self.metrics.port, "metrics"),
            (self.dashboard.port, "dashboard"),
        ];
        for &(port, name) in reserved {
            if self.server.proxy_port == port {
                return Err(ProxyError::ConfigError(format!(
                    "server.proxy_port={} conflicts with {} port",
                    self.server.proxy_port, name
                )));
            }
        }

        // If TLS enabled, check tls_proxy_port doesn't conflict with proxy_port
        if let Some(tls) = &self.server.tls {
            if tls.enabled && tls.tls_proxy_port == self.server.proxy_port {
                return Err(ProxyError::ConfigError(format!(
                    "tls.tls_proxy_port={} conflicts with server.proxy_port={}",
                    tls.tls_proxy_port, self.server.proxy_port
                )));
            }
        }

        Ok(())
    }

    /// Validate and correct RAM cache flush configuration
    fn validate_ram_cache_flush_config(&mut self) {
        // Validate flush_interval (10-600 seconds)
        let flush_interval_secs = self.cache.ram_cache_flush_interval.as_secs();
        if !(10..=600).contains(&flush_interval_secs) {
            warn!(
                "Invalid ram_cache_flush_interval: {}s (must be 10-600s), using default 10s",
                flush_interval_secs
            );
            self.cache.ram_cache_flush_interval = Duration::from_secs(10);
        }

        // `ram_cache_flush_threshold`'s 10-10000 range check went with the field. Validation
        // only ever got looser here, so no config that loaded before this release fails now.

        // Validate ram_cache_verification_interval (0-60s). 0 is a valid opt-out
        // (verify on every RAM hit), per ram-tier-effectiveness Requirement 4
        // criterion 6 — this is why the range is 0-60s rather than the archived
        // ram-disk-cache-coherency design's 1-60s.
        if self.cache.ram_cache_verification_interval > Duration::from_secs(60) {
            warn!(
                "Invalid cache.ram_cache_verification_interval: {}s (must be 0-60s), using default 1s",
                self.cache.ram_cache_verification_interval.as_secs()
            );
            self.cache.ram_cache_verification_interval = Duration::from_secs(1);
        }

        // Log configuration
        if self.cache.ram_cache_enabled {
            info!(
                "RAM cache access-activity window: {}s (2 x cache.ram_cache_flush_interval={}s, \
                 which governs no flush)",
                self.cache.ram_cache_flush_interval.as_secs() * 2,
                self.cache.ram_cache_flush_interval.as_secs()
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
            let new_target = self
                .cache
                .eviction_trigger_percent
                .saturating_sub(10)
                .max(50);
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

    /// a config that omits `logging.local_state_dir` gets the root-volume default.
    #[test]
    fn local_state_dir_defaults_when_omitted() {
        let logging: LoggingConfig =
            serde_yaml_ng::from_str("app_log_dir: /x/app\naccess_log_dir: /x/access\n").unwrap();
        assert_eq!(logging.local_state_dir, PathBuf::from("/var/lib/s3-proxy"));
        let set: LoggingConfig = serde_yaml_ng::from_str("local_state_dir: /var/lib/p\n").unwrap();
        assert_eq!(set.local_state_dir, PathBuf::from("/var/lib/p"));
    }

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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");

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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");

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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");

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

        let result: std::result::Result<Config, serde_yaml_ng::Error> =
            serde_yaml_ng::from_str(yaml);
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");

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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        // Should use default values when not specified
        assert_eq!(config.cache.eviction_buffer_percent, 5);
        // shared_storage.lock_acquire_wait_budget defaults to 5s
        assert_eq!(
            config
                .cache
                .shared_storage
                .lock_acquire_wait_budget
                .as_secs(),
            5
        );
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        // Should use custom values when specified
        assert_eq!(config.cache.eviction_buffer_percent, 10);
        // Written as `lock_timeout` in the YAML above, read through the serde alias — so this
        // assertion is also the whole-`Config` proof that an existing file keeps its value.
        assert_eq!(
            config
                .cache
                .shared_storage
                .lock_acquire_wait_budget
                .as_secs(),
            120
        );
    }

    #[test]
    fn test_write_cache_enabled_default_true() {
        // Test that the default configuration has write_cache_enabled = true
        let config = Config::default();
        assert!(
            config.cache.write_cache_enabled,
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(
            config.cache.write_cache_enabled,
            "write_cache_enabled should default to true when missing (write caching is complete)"
        );
    }

    /// A config that omits `cache.get_ttl` gets the 1-day default, from the YAML path
    /// and from both `Default` impls (config-compatibility: no field the operator did
    /// not write may change how the file parses).
    #[test]
    fn minimal_config_defaults_get_ttl_to_one_day() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");
        assert_eq!(
            config.cache.get_ttl,
            Duration::from_secs(86_400),
            "a config without cache.get_ttl must default to 1 day"
        );
        assert_eq!(CacheConfig::default().get_ttl, Duration::from_secs(86_400));
        assert_eq!(Config::default().cache.get_ttl, Duration::from_secs(86_400));
        assert!(config.cache.validate().is_ok());
    }

    /// The old default stays a valid explicit value: an operator keeping 10-year
    /// caching for immutable data sets `get_ttl: "315360000s"`, and it parses and
    /// validates unchanged.
    #[test]
    fn explicit_ten_year_get_ttl_is_still_accepted() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
  get_ttl: "315360000s"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("config must parse");
        assert_eq!(config.cache.get_ttl, Duration::from_secs(315_360_000));
        assert!(
            config.cache.validate().is_ok(),
            "get_ttl of 315360000s (the validated maximum) must be accepted"
        );
    }

    #[test]
    fn test_minimal_config_fills_struct_defaults() {
        // Regression test: a config that only specifies the essential path fields
        // should deserialize successfully, with every other field falling back to
        // its struct-level Default impl (enabled by #[serde(default)] on the struct).
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");

        // Spot-check fields that previously had no field-level serde default.
        assert_eq!(config.cache.write_cache_percent, 10.0);
        assert_eq!(config.cache.write_cache_max_object_size, 256 * 1024 * 1024);
        assert_eq!(config.cache.max_cache_size, 10 * 1024 * 1024 * 1024);
        assert!(!config.cache.ram_cache_enabled);
        assert!(config.logging.access_log_enabled);
        assert_eq!(config.logging.log_level, "info");
        assert_eq!(config.connection_pool.max_connections_per_ip, 10);
        // Admin listeners default to loopback only.
        assert_eq!(config.health.bind_address, "127.0.0.1");
        assert_eq!(config.metrics.bind_address, "127.0.0.1");
        assert_eq!(config.dashboard.bind_address, "127.0.0.1");
        // Metadata resilience defaults (cache-metadata-resilience spec).
        assert_eq!(config.cache.max_metadata_file_bytes, 4 * 1024 * 1024);
        assert_eq!(config.cache.metadata_io_concurrency, 32);
        // Upstream timeout defaults (cache-metadata-resilience spec, Req 5).
        assert_eq!(
            config.connection_pool.upstream_first_byte_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            config.connection_pool.upstream_idle_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(config.connection_pool.upstream_idle_retries, 2);
        // Download bandwidth QoS defaults (disabled by default).
        assert_eq!(
            config.download_bandwidth.max_bytes_per_sec, 0,
            "download_bandwidth must default to disabled (max_bytes_per_sec=0)"
        );
        assert!(
            !config.download_bandwidth.caller_id.enabled,
            "caller_id must default to disabled"
        );
        assert!(
            config
                .download_bandwidth
                .caller_id
                .validation_regex
                .is_none(),
            "validation_regex must default to None"
        );
        assert_eq!(config.download_bandwidth.caller_id.max_len, 64);
        assert_eq!(config.download_bandwidth.max_tracked_classes, 1024);
        assert_eq!(config.download_bandwidth.fleet.fallback_instance_count, 1);
        assert_eq!(
            config.download_bandwidth.fleet.instance_staleness,
            Duration::from_secs(90),
            "an omitted instance_staleness must default to three refresh intervals"
        );
        assert_eq!(
            config.download_bandwidth.fleet.refresh_interval,
            Duration::from_secs(30)
        );
        // Hedged requests governor default (hedged-upstream-requests spec, Req 6.2).
        assert_eq!(
            config.connection_pool.hedged_requests.max_inflight_fraction, 0.1,
            "hedged_requests.max_inflight_fraction must default to 0.1"
        );
    }

    #[test]
    fn test_ram_cache_verification_interval_parses_and_round_trips() {
        // Un-deprecated in ram-tier-effectiveness Requirement 4 (v2.8.6): the field
        // now has a real consumer (`CacheManager::ram_verify`). A config file
        // setting it to an in-range value must parse and round-trip unchanged.
        let yaml = r#"
cache:
  cache_dir: "./cache"
  ram_cache_verification_interval: "30s"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("a config setting the field must parse");
        assert_eq!(
            config.cache.ram_cache_verification_interval,
            Duration::from_secs(30),
            "an in-range value must round-trip"
        );
    }

    #[test]
    fn test_ram_cache_verification_interval_out_of_range_clamps_to_default() {
        // Requirement 4 criterion 6: 0 is a valid opt-out (verify on every RAM
        // hit), so the range is 0-60s. A value above 60s is out of range and
        // gets clamped to the 1s default by `validate_ram_cache_flush_config()`.
        let mut config = Config::default();
        config.cache.ram_cache_verification_interval = Duration::from_secs(600);
        config.validate_ram_cache_flush_config();
        assert_eq!(
            config.cache.ram_cache_verification_interval,
            Duration::from_secs(1),
            "an out-of-range value must be clamped to the 1s default"
        );
    }

    #[test]
    fn test_ram_cache_verification_interval_zero_is_a_valid_opt_out() {
        // Requirement 4 criterion 6: 0 means "verify every RAM hit" and must
        // NOT be clamped away by range validation.
        let mut config = Config::default();
        config.cache.ram_cache_verification_interval = Duration::ZERO;
        config.validate_ram_cache_flush_config();
        assert_eq!(
            config.cache.ram_cache_verification_interval,
            Duration::ZERO,
            "zero must remain a valid opt-out, not be clamped to the default"
        );
    }

    #[test]
    fn test_ram_cache_verification_interval_defaults_when_omitted() {
        // A config file that omits the field must parse and apply the 1s default.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("a config omitting the field must parse");
        assert_eq!(
            config.cache.ram_cache_verification_interval,
            default_ram_cache_verification_interval(),
            "omitting the field must apply the default"
        );
        assert_eq!(
            config.cache.ram_cache_verification_interval,
            Duration::from_secs(1),
            "the default is 1 second"
        );
    }

    #[test]
    fn test_admin_bind_addresses_default_to_loopback_when_sections_omitted() {
        // A config with no health, metrics or dashboard section at all must bind
        // all three unauthenticated admin listeners to loopback.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("config must parse");
        assert_eq!(
            config.health.bind_address, "127.0.0.1",
            "health bind_address must default to 127.0.0.1 when the section is omitted"
        );
        assert_eq!(
            config.metrics.bind_address, "127.0.0.1",
            "metrics bind_address must default to 127.0.0.1 when the section is omitted"
        );
        assert_eq!(
            config.dashboard.bind_address, "127.0.0.1",
            "dashboard bind_address must default to 127.0.0.1 when the section is omitted"
        );
    }

    #[test]
    fn test_admin_bind_addresses_explicit_all_interfaces_is_kept() {
        // An operator who opens the listeners explicitly keeps that choice.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
health:
  bind_address: "0.0.0.0"
metrics:
  bind_address: "0.0.0.0"
dashboard:
  bind_address: "0.0.0.0"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("config must parse");
        assert_eq!(config.health.bind_address, "0.0.0.0");
        assert_eq!(config.metrics.bind_address, "0.0.0.0");
        assert_eq!(config.dashboard.bind_address, "0.0.0.0");
    }

    #[test]
    fn test_bind_address_defaults_when_omitted() {
        // Config-compat regression: a health/metrics section that omits
        // bind_address must still parse and default to loopback.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  check_interval: "30s"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  collection_interval: "60s"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("config must parse");
        assert_eq!(
            config.health.bind_address, "127.0.0.1",
            "health bind_address must default to 127.0.0.1 when omitted"
        );
        assert_eq!(
            config.metrics.bind_address, "127.0.0.1",
            "metrics bind_address must default to 127.0.0.1 when omitted"
        );
    }

    #[test]
    fn test_bind_address_explicit_loopback() {
        // Verify explicit bind_address values are honored.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
health:
  enabled: true
  endpoint: "/health"
  port: 8080
  bind_address: "127.0.0.1"
  check_interval: "30s"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  bind_address: "127.0.0.1"
  collection_interval: "60s"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("config must parse");
        assert_eq!(config.health.bind_address, "127.0.0.1");
        assert_eq!(config.metrics.bind_address, "127.0.0.1");
    }

    #[test]
    fn test_upstream_overrides_defaults_empty_when_omitted() {
        // Config-compatibility regression (Requirements 1.2, 10.1, 10.2):
        // a config file that omits `connection_pool.upstream_overrides` must
        // still parse, fall back to an empty map (the feature is off /
        // Secure_Default_Behaviour holds), and pass connection-pool validation.
        // This guards the restart-free upgrade contract for the new field.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");

        assert!(
            config.connection_pool.upstream_overrides.is_empty(),
            "omitted upstream_overrides must default to an empty map (feature off)"
        );

        // Secure_Default_Behaviour holds: the matcher built from the default
        // (empty) map applies no override to any destination.
        let overrides = crate::upstream_overrides::UpstreamOverrides::from_config(
            &config.connection_pool.upstream_overrides,
        );
        assert!(
            overrides.is_empty(),
            "an empty upstream_overrides map must produce an empty matcher"
        );

        // The config must validate successfully with the field absent.
        assert!(
            config.connection_pool.validate().is_ok(),
            "connection pool config must validate when upstream_overrides is omitted"
        );
    }

    #[test]
    fn test_hedged_requests_max_inflight_fraction_out_of_range_rejected() {
        // Validation must reject max_inflight_fraction outside [0.0, 1.0].
        let mut pool_config = ConnectionPoolConfig::default();

        // Above 1.0
        pool_config.hedged_requests.max_inflight_fraction = 1.5;
        let result = pool_config.validate();
        assert!(result.is_err(), "fraction > 1.0 must be rejected");
        assert!(
            result.unwrap_err().contains("max_inflight_fraction"),
            "error message must mention the field"
        );

        // Below 0.0
        pool_config.hedged_requests.max_inflight_fraction = -0.1;
        let result = pool_config.validate();
        assert!(result.is_err(), "fraction < 0.0 must be rejected");

        // Boundary: 0.0 is valid
        pool_config.hedged_requests.max_inflight_fraction = 0.0;
        assert!(
            pool_config.validate().is_ok(),
            "fraction 0.0 must be accepted"
        );

        // Boundary: 1.0 is valid
        pool_config.hedged_requests.max_inflight_fraction = 1.0;
        assert!(
            pool_config.validate().is_ok(),
            "fraction 1.0 must be accepted"
        );

        // Default 0.1 is valid
        pool_config.hedged_requests.max_inflight_fraction = 0.1;
        assert!(
            pool_config.validate().is_ok(),
            "default 0.1 must be accepted"
        );
    }

    #[test]
    fn test_bucket_settings_staleness_threshold_defaults_when_omitted() {
        // Config-compatibility regression: a config file that omits
        // `bucket_settings_staleness_threshold` (the rules-file staleness window)
        // must still parse and fall back to the 60s default, so upgrading the
        // binary never requires editing an existing config file.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");

        assert_eq!(
            config.cache.bucket_settings_staleness_threshold,
            Duration::from_secs(60),
            "omitted bucket_settings_staleness_threshold must default to 60s"
        );
    }

    #[test]
    fn test_write_cache_tee_channel_depth_defaults_when_omitted() {
        // Config-compatibility regression (Requirement 9.4): a config file that
        // omits `server.write_cache_tee_channel_depth` (the streaming signed-write
        // cache-tee channel depth) must still parse and fall back to the documented
        // default of 5 frames, so upgrading the binary never requires editing an
        // existing config file. Guards the restart-free upgrade contract.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");

        assert_eq!(
            config.server.write_cache_tee_channel_depth, 5,
            "omitted write_cache_tee_channel_depth must default to 5 frames"
        );
    }

    #[test]
    fn test_deprecated_max_buffered_request_body_bytes_parses_and_warns() {
        // Requirement 4.1: retain the old field for restart-free upgrades, but make
        // its ignored status visible at startup.
        let yaml = r#"
server:
  max_buffered_request_body_bytes: 1048576
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml)
            .expect("config with deprecated max_buffered_request_body_bytes must parse");
        assert!(
            config.server.validate().is_ok(),
            "a compatible config with the deprecated field must still start"
        );
        assert_eq!(
            config.server.max_buffered_request_body_bytes_deprecation_warning(),
            Some(
                "Configuration field 'server.max_buffered_request_body_bytes' is deprecated and ignored: \
                 S3's own single-part PUT and UploadPart limits apply. \
                 Delete the field to silence this warning."
            ),
            "a non-default deprecated value must produce the startup warning"
        );
    }

    #[test]
    fn test_deprecated_max_buffered_request_body_bytes_defaults_without_warning() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml)
            .expect("config omitting deprecated max_buffered_request_body_bytes must parse");
        assert_eq!(
            config.server.max_buffered_request_body_bytes,
            5 * 1024 * 1024 * 1024,
            "omitting the field must preserve the 5 GiB compatibility default"
        );
        assert_eq!(
            config
                .server
                .max_buffered_request_body_bytes_deprecation_warning(),
            None,
            "an omitted field must not produce a deprecation warning"
        );
    }

    #[test]
    fn test_max_complete_body_bytes_defaults_when_omitted() {
        // Config-compatibility regression (Requirement 6): a config file that
        // omits `cache.max_complete_body_bytes` must still parse and fall back to
        // the 10 MiB default. Guards the restart-free upgrade contract.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");

        assert_eq!(
            config.cache.max_complete_body_bytes,
            10 * 1024 * 1024,
            "omitted max_complete_body_bytes must default to 10 MiB"
        );
    }

    #[test]
    fn test_max_complete_body_bytes_explicit_override() {
        // Verify that operators can override max_complete_body_bytes.
        let yaml = r#"
cache:
  cache_dir: "./cache"
  max_complete_body_bytes: 5242880
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("config with explicit field must parse");

        assert_eq!(
            config.cache.max_complete_body_bytes,
            5 * 1024 * 1024,
            "explicit max_complete_body_bytes must be respected"
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(
            config.cache.write_cache_enabled,
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(
            !config.cache.write_cache_enabled,
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

        assert!(
            config.cache.write_cache_enabled,
            "WRITE_CACHE_ENABLED=true should override to true"
        );

        std::env::set_var("WRITE_CACHE_ENABLED", "false");
        config.cache.write_cache_enabled = true; // Start with true
        config.apply_env_overrides();

        assert!(
            !config.cache.write_cache_enabled,
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
        assert_eq!(config.idle_timeout, Duration::from_secs(55));
        assert!(config.keepalive_enabled);
        assert_eq!(config.max_idle_per_host, 100);
        assert_eq!(config.max_lifetime, Duration::from_secs(300));
        assert_eq!(config.pool_check_interval, Duration::from_secs(10));
        assert_eq!(config.upstream_first_byte_timeout, Duration::from_secs(5));
        assert_eq!(config.upstream_idle_timeout, Duration::from_secs(5));
        assert_eq!(config.upstream_idle_retries, 2);
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
            serde_yaml_ng::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert_eq!(config.max_connections_per_ip, 20);
        assert_eq!(config.dns_refresh_interval, Duration::from_secs(120));
        assert_eq!(config.connection_timeout, Duration::from_secs(15));
        assert_eq!(config.idle_timeout, Duration::from_secs(45));
        assert!(!config.keepalive_enabled);
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
            ip_distribution_enabled: false,
            max_idle_per_ip: 10,
            ..Default::default()
        };

        // Verify values are within reasonable ranges
        assert!(config.max_connections_per_ip > 0 && config.max_connections_per_ip <= 100);
        assert!(config.dns_refresh_interval >= Duration::from_secs(10));
        assert!(config.connection_timeout >= Duration::from_secs(1));
        assert!(config.idle_timeout >= Duration::from_secs(10));
        assert!(config.max_idle_per_host > 0 && config.max_idle_per_host <= 500);
        assert!(config.max_lifetime >= Duration::from_secs(60));
        assert!(config.pool_check_interval >= Duration::from_secs(1));
    }

    #[test]
    fn test_connection_pool_yaml_without_max_idle_per_host_uses_default() {
        // Requirement 4.4: When max_idle_per_host is not specified in YAML, default to 100
        let yaml = r#"
max_connections_per_ip: 15
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "30s"
keepalive_enabled: true
max_lifetime: "300s"
pool_check_interval: "10s"
"#;

        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert_eq!(
            config.max_idle_per_host, 100,
            "max_idle_per_host should default to 100 when not specified"
        );
    }

    #[test]
    fn test_ip_distribution_defaults() {
        // Requirement 5.2, 5.3: ip_distribution_enabled defaults to true, max_idle_per_ip defaults to 10
        let config = ConnectionPoolConfig::default();
        assert!(config.ip_distribution_enabled);
        assert_eq!(config.max_idle_per_ip, 10);
    }

    #[test]
    fn test_ip_distribution_yaml_parsing() {
        // Requirement 5.2, 5.3: Parse ip_distribution_enabled and max_idle_per_ip from YAML
        let yaml = r#"
max_connections_per_ip: 10
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "30s"
ip_distribution_enabled: true
max_idle_per_ip: 25
"#;

        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert!(config.ip_distribution_enabled);
        assert_eq!(config.max_idle_per_ip, 25);
    }

    #[test]
    fn test_ip_distribution_yaml_defaults_when_omitted() {
        // Requirement 5.2, 5.3: Fields default correctly when omitted from YAML
        let yaml = r#"
max_connections_per_ip: 10
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "30s"
"#;

        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert!(
            config.ip_distribution_enabled,
            "ip_distribution_enabled should default to true"
        );
        assert_eq!(
            config.max_idle_per_ip, 10,
            "max_idle_per_ip should default to 10"
        );
    }

    #[test]
    fn test_tcp_recv_buffer_size_defaults_to_none_when_omitted() {
        // Bugfix: the default must be None so the kernel auto-tunes the TCP
        // receive window. A fixed SO_RCVBUF disables auto-tuning and caps
        // single-stream throughput on high-RTT cross-region paths.
        let yaml = r#"
max_connections_per_ip: 10
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "30s"
"#;

        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert_eq!(
            config.tcp_recv_buffer_size, None,
            "tcp_recv_buffer_size should default to None (kernel auto-tune) when omitted"
        );
        assert_eq!(
            ConnectionPoolConfig::default().tcp_recv_buffer_size,
            None,
            "ConnectionPoolConfig::default() should leave tcp_recv_buffer_size as None"
        );
    }

    #[test]
    fn test_tcp_recv_buffer_size_explicit_value_parses_to_some() {
        // Operators can still pin an explicit SO_RCVBUF if they have a reason to.
        let yaml = r#"
max_connections_per_ip: 10
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "30s"
tcp_recv_buffer_size: 262144
"#;

        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse ConnectionPoolConfig");

        assert_eq!(
            config.tcp_recv_buffer_size,
            Some(262_144),
            "an explicit tcp_recv_buffer_size value should parse to Some(value)"
        );
    }

    #[test]
    fn test_ip_distribution_validation_rejects_zero_max_idle_per_ip() {
        // Requirement 5.4: max_idle_per_ip must be >= 1
        let config = ConnectionPoolConfig {
            max_idle_per_ip: 0,
            ..ConnectionPoolConfig::default()
        };
        let result = config.validate();
        assert!(
            result.is_err(),
            "Validation should reject max_idle_per_ip of 0"
        );
        assert!(result.unwrap_err().contains("Max idle connections per IP"));
    }

    #[test]
    fn test_ip_distribution_validation_rejects_over_100_max_idle_per_ip() {
        // Requirement 5.4: max_idle_per_ip must be <= 100
        let config = ConnectionPoolConfig {
            max_idle_per_ip: 101,
            ..ConnectionPoolConfig::default()
        };
        let result = config.validate();
        assert!(
            result.is_err(),
            "Validation should reject max_idle_per_ip > 100"
        );
        assert!(result.unwrap_err().contains("Max idle connections per IP"));
    }

    #[test]
    fn test_ip_distribution_validation_accepts_valid_max_idle_per_ip() {
        // Requirement 5.4: max_idle_per_ip of 1 and 100 are valid boundary values
        let config_min = ConnectionPoolConfig {
            max_idle_per_ip: 1,
            ..ConnectionPoolConfig::default()
        };
        assert!(
            config_min.validate().is_ok(),
            "max_idle_per_ip of 1 should be valid"
        );

        let config_max = ConnectionPoolConfig {
            max_idle_per_ip: 100,
            ..ConnectionPoolConfig::default()
        };
        assert!(
            config_max.validate().is_ok(),
            "max_idle_per_ip of 100 should be valid"
        );
    }

    #[test]
    fn test_initialization_config_defaults() {
        let config = InitializationConfig::default();

        assert!(config.parallel_scan);
        assert_eq!(config.scan_timeout, Duration::from_secs(30));
        assert!(config.progress_logging);
    }

    #[test]
    fn test_shared_storage_config_defaults() {
        let config = SharedStorageConfig::default();

        // Note: enabled field was removed - shared storage coordination is always enabled
        assert_eq!(
            config.lock_acquire_wait_budget,
            Duration::from_secs(5),
            "the default must stay 5s: three acquisition sites hardcoded a five-second bound \
             before this field was honoured, and any other default silently changes them"
        );
        assert_eq!(config.validation_threshold_warn, 5.0);
        assert_eq!(config.validation_threshold_error, 20.0);
        assert_eq!(
            config.validation_max_duration,
            Duration::from_secs(4 * 3600)
        );
    }

    /// `metadata_lock_timeout_ms` is REMOVED, and an existing `config.yaml` carrying it must
    /// still parse.
    ///
    /// The upgrade contract is that an existing deployment's config keeps working after a
    /// binary swap with no edits (the config compatibility rule).
    ///
    /// # Why the field could simply be deleted rather than kept as an ignored shim
    ///
    /// Because this struct does NOT carry `deny_unknown_fields`, so the deserializer ignores
    /// keys it does not know — crate-wide, not as a property of this field. That is the actual
    /// mechanism the upgrade contract rests on, so it is what this test pins. A first version
    /// of this change kept the field as a renamed `Option<u64>` shim and asserted that an old
    /// config parsed; that assertion passed identically with the field deleted, so it was
    /// evidence about the deserializer while reading as evidence about the shim.
    #[test]
    fn a_config_carrying_the_removed_metadata_lock_timeout_still_parses() {
        let yaml = r#"
lock_timeout: 60s
metadata_lock_timeout_ms: 45000
validation_threshold_warn: 5.0
"#;
        let parsed: SharedStorageConfig =
            serde_yaml_ng::from_str(yaml).expect("a config carrying the removed field must parse");
        assert_eq!(parsed.lock_acquire_wait_budget, Duration::from_secs(60));
        assert_eq!(
            parsed.validation_threshold_warn, 5.0,
            "fields AFTER the removed key must still be read, so the unknown key is skipped \
             rather than terminating the mapping"
        );
    }

    /// The mechanism above, stated directly: unknown keys are ignored.
    ///
    /// If this ever fails, `deny_unknown_fields` has been added somewhere above and EVERY
    /// removed config field becomes a startup failure on upgrade — including this one, which
    /// would then need reinstating as an ignored shim.
    #[test]
    fn unknown_shared_storage_keys_are_ignored_which_is_what_makes_removal_safe() {
        let yaml = r#"
lock_timeout: 60s
a_key_no_version_of_this_software_has_ever_had: 45
"#;
        let parsed: SharedStorageConfig = serde_yaml_ng::from_str(yaml).expect(
            "unknown keys must be ignored; if they are not, removing any config field breaks \
             the upgrade contract",
        );
        assert_eq!(parsed.lock_acquire_wait_budget, Duration::from_secs(60));
    }

    /// `lock_refresh_interval` is REMOVED, and an existing config carrying it must still load.
    ///
    /// The field described refreshing held locks to prevent an expiration the protocol does not
    /// have, and nothing outside this module ever read it. Removing it also removed a validation
    /// rule (`lock_refresh_interval < lock_timeout`, plus a 5-120s range), so the value used here
    /// is one the old rule REJECTED: it proves both that the key is skipped and that no surviving
    /// rule rejects it. Validation only ever got looser, so no config that loaded before fails now.
    #[test]
    fn a_config_carrying_the_removed_lock_refresh_interval_still_loads() {
        let yaml = r#"
lock_timeout: 60s
lock_refresh_interval: 999s
validation_threshold_warn: 5.0
"#;
        let parsed: SharedStorageConfig = serde_yaml_ng::from_str(yaml)
            .expect("a config carrying the removed lock_refresh_interval must parse");
        assert_eq!(parsed.lock_acquire_wait_budget, Duration::from_secs(60));
        assert_eq!(
            parsed.validation_threshold_warn, 5.0,
            "fields AFTER the removed key must still be read, so the unknown key is skipped \
             rather than terminating the mapping"
        );
        assert!(
            parsed.validate().is_ok(),
            "999s exceeded the retired 5-120s range AND the retired 'must be less than \
             lock_timeout' rule; with both gone it must validate"
        );
    }

    /// `--lock-refresh-interval` is still ACCEPTED by the argument parser.
    ///
    /// Removing a config field is safe for YAML because unknown keys are ignored, but clap
    /// rejects an unknown argument — so deleting the flag would turn a removed field into a
    /// failed boot for any service unit or wrapper script that passes it. The flag is retained
    /// hidden and `apply_cli_overrides` warns when it is supplied.
    #[test]
    fn the_removed_lock_refresh_interval_flag_is_still_accepted_by_the_parser() {
        let matches = Config::build_cli()
            .try_get_matches_from(vec!["s3-proxy", "--lock-refresh-interval", "30"])
            .expect(
                "the retired flag must still parse; clap errors on an unknown argument, which \
                 would make an upgrade fail to start",
            );
        assert_eq!(
            matches
                .get_one::<String>("lock-refresh-interval")
                .map(String::as_str),
            Some("30"),
            "the argument must reach ArgMatches so the ignored-field warning can fire"
        );
    }

    #[test]
    fn test_shared_storage_config_validation_valid() {
        let config = SharedStorageConfig::default();
        assert!(config.validate().is_ok());
    }

    /// A minimal-but-complete config file with `orphan_recovery_interval` set to `secs`.
    ///
    /// `Config` carries no struct-level `#[serde(default)]`, so a partial document does not
    /// deserialize; this is the shape `test_initialization_config_defaults_when_missing`
    /// already establishes as sufficient, plus the one field under test.
    pub(super) fn yaml_with_orphan_recovery_interval(secs: &str) -> String {
        format!(
            r#"
server:
  http_port: 8000
  https_port: 8443
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
    orphan_recovery_interval: "{}"
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
    headers: {{}}
"#,
            secs
        )
    }

    /// Capture `warn!` output emitted inside `f`, so a clamp warning can be asserted as text.
    ///
    /// Thread-local (`with_default`), so this is safe under the parallel test harness and
    /// cannot capture another test's logging.
    pub(super) fn capture_warnings<F: FnOnce()>(f: F) -> String {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct Sink(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Sink {
            type Writer = Sink;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = Sink(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = sink.0.lock().unwrap().clone();
        String::from_utf8(bytes).expect("captured log output must be UTF-8")
    }

    /// `metrics.collection_interval` was accepted and ignored until the background metrics
    /// collector made it live, so every value in an existing file booted. It must still boot,
    /// clamped into `[MIN_COLLECTION_INTERVAL_SECS, MAX_COLLECTION_INTERVAL_SECS]` with a WARN,
    /// and `Config::load` must be the path that clamps it.
    ///
    /// `"0s"` is the value that matters most: an unclamped zero reaches `tokio::time::interval`,
    /// which panics inside the spawned collector, where the panic is swallowed and the collector
    /// silently does not exist.
    #[test]
    fn out_of_range_collection_interval_still_boots_clamped() {
        for (supplied, expected_secs) in [
            (Duration::from_secs(0), 10u64),
            (Duration::from_millis(500), 10),
            (Duration::from_secs(9), 10),
            (Duration::from_secs(10), 10),
            (Duration::from_secs(60), 60),
            (Duration::from_secs(172_800), 86_400),
        ] {
            let mut metrics = MetricsConfig {
                collection_interval: supplied,
                ..MetricsConfig::default()
            };
            let logged = capture_warnings(|| metrics.validate_and_clamp());
            assert_eq!(
                metrics.collection_interval,
                Duration::from_secs(expected_secs),
                "collection_interval={:?} must clamp to {}s",
                supplied,
                expected_secs
            );
            let in_range = supplied.as_secs() >= MetricsConfig::MIN_COLLECTION_INTERVAL_SECS
                && supplied.as_secs() <= MetricsConfig::MAX_COLLECTION_INTERVAL_SECS;
            assert_eq!(
                logged.contains("metrics.collection_interval"),
                !in_range,
                "a clamp must warn, and an in-range value must not: {:?} logged {:?}",
                supplied,
                logged
            );
        }

        // Wiring: `Config::load` calls the clamp. Built with `concat!` so this test's own source
        // does not contain the literal it searches for.
        let needle = concat!("config.metrics.", "validate_and_clamp();");
        let source = include_str!("config.rs");
        let load = source
            .find("pub fn load() -> Result<Self>")
            .expect("Config::load");
        assert!(
            source[load..].contains(needle),
            "Config::load must clamp metrics.collection_interval before the collector reads it"
        );
    }

    /// Part D — `cache.metadata_cache`'s documented ranges (refresh_interval 1-300s,
    /// max_entries 100-1,000,000) were never enforced, because `MetadataCacheConfig::validate`
    /// has no startup caller. `Config::load` now WARNS for a value outside them and keeps it:
    /// every such value booted before, and a clamp of `refresh_interval: 0` would widen a
    /// freshness bound (see `warn_if_outside_documented_range`).
    ///
    /// Two halves. The behaviour: each out-of-range shape warns naming its field, and an
    /// in-range config (the default, and both range edges) warns about nothing, so the warning
    /// cannot become noise on a correct file. The wiring: `Config::load` builds its own CLI
    /// matcher and cannot run under the test harness, so the call is pinned by source the way
    /// `out_of_range_collection_interval_still_boots_clamped` pins its clamp.
    #[test]
    fn out_of_range_metadata_cache_values_warn_on_load_and_are_kept() {
        let cases: [(Duration, usize, &[&str]); 8] = [
            (Duration::from_secs(5), 100_000, &[]),
            (Duration::from_secs(1), 100, &[]),
            (Duration::from_secs(300), 1_000_000, &[]),
            (
                Duration::ZERO,
                100_000,
                &["refresh_interval is 0", "never serves"],
            ),
            (
                Duration::from_secs(301),
                100_000,
                &["refresh_interval (301s)"],
            ),
            (
                Duration::from_secs(5),
                0,
                &["max_entries is 0", "at most one entry"],
            ),
            (Duration::from_secs(5), 99, &["max_entries (99)"]),
            (
                Duration::from_secs(5),
                1_000_001,
                &["max_entries (1000001)"],
            ),
        ];
        for (refresh_interval, max_entries, expected) in cases {
            let cfg = MetadataCacheConfig {
                refresh_interval,
                max_entries,
                ..MetadataCacheConfig::default()
            };
            let logged = capture_warnings(|| cfg.warn_if_outside_documented_range());
            if expected.is_empty() {
                assert!(
                    logged.is_empty(),
                    "an in-range metadata_cache ({refresh_interval:?}, {max_entries}) must not \
                     warn; logged {logged:?}"
                );
            }
            for needle in expected {
                assert!(
                    logged.contains(" WARN ") && logged.contains(needle),
                    "metadata_cache ({refresh_interval:?}, {max_entries}) must WARN containing \
                     {needle:?}; logged {logged:?}"
                );
            }
        }
        // Wiring. Built with `concat!` so this test's own source does not contain the literal.
        // rustfmt breaks the call's receiver chain across lines, so match the method call alone;
        // Config::load has no other receiver for it.
        let needle = concat!(".warn_if_outside", "_documented_range();");
        let source = include_str!("config.rs");
        let load = source
            .find("pub fn load() -> Result<Self>")
            .expect("Config::load");
        // Scoped to Config::load's own body: it ends where the next `fn` in the impl begins.
        let load_end = load
            + source[load..]
                .find("\n    fn ")
                .expect("a private fn follows Config::load in the impl");
        assert!(
            source[load..load_end].contains(needle),
            "Config::load must warn for out-of-range cache.metadata_cache values; nothing else \
             on the startup path checks them"
        );
    }

    /// THE UPGRADE-CONTRACT ASSERTION for the retired orphan-sweep fields. A config carrying all
    /// four, at non-default values including ones the old clamp rewrote (`"0s"`, `"48h"`), must
    /// still load and validate, keep the values it was given (nothing clamps an ignored field),
    /// and produce exactly one warning naming each field and the replacement.
    #[test]
    fn config_carrying_every_retired_orphan_field_loads_and_warns_once() {
        use std::io::Write;

        for interval in ["0s", "30s", "48h"] {
            let base = yaml_with_orphan_recovery_interval(interval);
            let anchor = "  shared_storage:\n";
            assert_eq!(base.matches(anchor).count(), 1, "fixture anchor");
            let yaml = base.replacen(
                anchor,
                "  shared_storage:\n    orphan_recovery_enabled: false\n    \
                 orphan_scan_timeout: \"7s\"\n    orphan_max_per_cycle: 5\n",
                1,
            );
            let mut temp_file = tempfile::NamedTempFile::new().unwrap();
            temp_file.write_all(yaml.as_bytes()).unwrap();
            temp_file.flush().unwrap();

            let config =
                Config::load_from_file(temp_file.path().to_str().unwrap()).unwrap_or_else(|e| {
                    panic!(
                        "a config carrying the retired orphan fields must still load. \
                         orphan_recovery_interval={:?} err={:?}",
                        interval, e
                    )
                });
            let shared = &config.cache.shared_storage;
            assert!(shared.validate().is_ok(), "{:?}", shared.validate());
            assert!(!shared.orphan_recovery_enabled);
            assert_eq!(shared.orphan_scan_timeout, Duration::from_secs(7));
            assert_eq!(shared.orphan_max_per_cycle, 5);
            assert_eq!(
                shared.retired_orphan_fields_set(),
                vec![
                    "orphan_recovery_enabled",
                    "orphan_recovery_interval",
                    "orphan_scan_timeout",
                    "orphan_max_per_cycle"
                ]
            );

            let logged = capture_warnings(|| shared.warn_retired_orphan_fields());
            assert_eq!(
                logged.matches("is ignored").count(),
                1,
                "exactly one warning for all four fields. got={:?}",
                logged
            );
            for needle in [
                "orphan_recovery_enabled",
                "orphan_recovery_interval",
                "orphan_scan_timeout",
                "orphan_max_per_cycle",
                "reclaimed automatically",
            ] {
                assert!(
                    logged.contains(needle),
                    "the warning must name {:?}. got={:?}",
                    needle,
                    logged
                );
            }
        }
    }

    /// Positive control: a config that sets none of the retired fields, or sets them to their
    /// defaults, warns nothing. An over-broad check reds here rather than nagging every
    /// operator on upgrade.
    #[test]
    fn default_retired_orphan_fields_do_not_warn() {
        let explicit_defaults = SharedStorageConfig {
            orphan_recovery_enabled: true,
            orphan_recovery_interval: Duration::from_secs(300),
            orphan_scan_timeout: Duration::from_secs(30),
            orphan_max_per_cycle: 100,
            ..SharedStorageConfig::default()
        };
        for config in [SharedStorageConfig::default(), explicit_defaults] {
            assert!(config.retired_orphan_fields_set().is_empty());
            let logged = capture_warnings(|| config.warn_retired_orphan_fields());
            assert!(
                !logged.contains("is ignored"),
                "default values must not warn. got={:?}",
                logged
            );
        }
    }

    /// Pins the warning's CALL SITE. `Config::load` reads the real process argv, so no test can
    /// call it; the needle is assembled with `concat!` so only the call site matches.
    #[test]
    fn the_retired_orphan_warning_is_wired_into_config_load() {
        let source = include_str!("config.rs");
        let needle = concat!("shared_storage.", "warn_retired_orphan_fields();");
        assert_eq!(
            source.matches(needle).count(),
            1,
            "expected exactly one {:?} call site (in Config::load)",
            needle
        );
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
    fn test_validation_max_duration_defaults_to_4_hours() {
        let config = SharedStorageConfig::default();
        assert_eq!(
            config.validation_max_duration,
            Duration::from_secs(4 * 3600)
        );
    }

    #[test]
    fn test_validation_max_duration_validation_rejects_too_short() {
        // Less than 10 minutes should be rejected
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(5 * 60), // 5 minutes
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        // Exactly 9 minutes 59 seconds should be rejected
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(599),
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_max_duration_validation_rejects_too_long() {
        // More than 23 hours should be rejected
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(24 * 3600), // 24 hours
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());

        // 23 hours + 1 second should be rejected
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(23 * 3600 + 1),
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_max_duration_validation_accepts_valid_values() {
        // Exactly 10 minutes (lower bound)
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(10 * 60),
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_ok());

        // Exactly 23 hours (upper bound)
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(23 * 3600),
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_ok());

        // 4 hours (default)
        let config = SharedStorageConfig::default();
        assert!(config.validate().is_ok());

        // 1 hour
        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(3600),
            ..SharedStorageConfig::default()
        };
        assert!(config.validate().is_ok());
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");

        assert!(!config.cache.initialization.parallel_scan);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(60)
        );
        assert!(!config.cache.initialization.progress_logging);
        assert_eq!(config.cache.shared_storage.validation_threshold_warn, 10.0);
        assert_eq!(config.cache.shared_storage.validation_threshold_error, 30.0);
    }

    #[test]
    fn test_config_loads_with_example_file() {
        // Test that the example config file loads successfully with initialization defaults
        let yaml = std::fs::read_to_string("config/config.example.yaml")
            .expect("Failed to read config/config.example.yaml");

        let config: Config =
            serde_yaml_ng::from_str(&yaml).expect("Failed to parse config/config.example.yaml");

        // Verify initialization config has expected values from the example file
        assert!(config.cache.initialization.parallel_scan);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert!(config.cache.initialization.progress_logging);
        // Validation thresholds are now in shared_storage
        assert_eq!(config.cache.shared_storage.validation_threshold_warn, 5.0);
        assert_eq!(config.cache.shared_storage.validation_threshold_error, 20.0);

        // Verify log lifecycle fields are present with expected values
        assert_eq!(config.logging.access_log_retention_days, 30);
        assert_eq!(config.logging.app_log_retention_days, 30);
        assert_eq!(
            config.logging.log_cleanup_interval,
            Duration::from_secs(86400)
        );
        assert_eq!(
            config.logging.access_log_file_rotation_interval,
            Duration::from_secs(300)
        );
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");

        // Should use default values when initialization section is missing
        assert!(config.cache.initialization.parallel_scan);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert!(config.cache.initialization.progress_logging);
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
        assert!(config.cache.initialization.parallel_scan);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert!(config.cache.initialization.progress_logging);
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
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Should parse YAML successfully");

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

        assert!(config.enabled);
        assert_eq!(config.port, 8081);
        assert_eq!(config.bind_address, "127.0.0.1");
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
            serde_yaml_ng::from_str(yaml).expect("Failed to parse DashboardConfig");

        assert!(!config.enabled);
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
        assert!(config.dashboard.enabled);
        assert_eq!(config.dashboard.port, 8081);
        assert_eq!(config.dashboard.bind_address, "127.0.0.1");
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
            max_waiter_resubscriptions: 3,
        };
        assert_eq!(config.wait_timeout(), Duration::from_secs(45));
    }

    #[test]
    fn test_download_coordination_clamp_below_minimum() {
        // Requirement 18.5: Clamp wait_timeout_secs to 5-120 range
        let mut config = DownloadCoordinationConfig {
            enabled: true,
            wait_timeout_secs: 2, // Below minimum of 5
            max_waiter_resubscriptions: 3,
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
            max_waiter_resubscriptions: 3,
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
            max_waiter_resubscriptions: 3,
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
            max_waiter_resubscriptions: 3,
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
            max_waiter_resubscriptions: 3,
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(config.cache.download_coordination.enabled);
        assert_eq!(config.cache.download_coordination.wait_timeout_secs, 30);
    }

    #[test]
    fn test_add_referer_header_default_true() {
        let config = Config::default();
        assert!(config.server.add_referer_header);
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(config.server.add_referer_header);
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(config.server.add_referer_header);
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert!(!config.server.add_referer_header);
    }

    // --- TLS config validation tests ---

    #[test]
    fn test_tls_config_validate_enabled_missing_cert_path() {
        let tls = TlsConfig {
            enabled: true,
            tls_proxy_port: 8443,
            cert_path: String::new(),
            key_path: "/path/to/key.pem".to_string(),
            connect_allowlist: None,
        };
        let result = tls.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cert_path"));
    }

    #[test]
    fn test_tls_config_validate_enabled_missing_key_path() {
        let tls = TlsConfig {
            enabled: true,
            tls_proxy_port: 8443,
            cert_path: "/path/to/cert.pem".to_string(),
            key_path: String::new(),
            connect_allowlist: None,
        };
        let result = tls.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key_path"));
    }

    #[test]
    fn test_tls_config_validate_enabled_valid() {
        let tls = TlsConfig {
            enabled: true,
            tls_proxy_port: 8443,
            cert_path: "/path/to/cert.pem".to_string(),
            key_path: "/path/to/key.pem".to_string(),
            connect_allowlist: None,
        };
        assert!(tls.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validate_disabled_empty_paths_ok() {
        let tls = TlsConfig {
            enabled: false,
            tls_proxy_port: 8443,
            cert_path: String::new(),
            key_path: String::new(),
            connect_allowlist: None,
        };
        assert!(tls.validate().is_ok());
    }

    #[test]
    fn test_tls_config_validate_port_zero_rejected() {
        let tls = TlsConfig {
            enabled: false,
            tls_proxy_port: 0,
            cert_path: String::new(),
            key_path: String::new(),
            connect_allowlist: None,
        };
        let result = tls.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must not be 0"));
    }

    #[test]
    fn test_tls_config_port_conflict_with_http_port() {
        let mut config = Config::default();
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: config.server.http_port,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        let result = config.validate_tls_config();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("http_port"));
    }

    #[test]
    fn test_tls_config_port_conflict_with_https_port() {
        let mut config = Config::default();
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: config.server.https_port,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        let result = config.validate_tls_config();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("https_port"));
    }

    #[test]
    fn test_tls_config_port_conflict_with_health_port() {
        let mut config = Config::default();
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: config.health.port,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        let result = config.validate_tls_config();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("health.port"));
    }

    #[test]
    fn test_tls_config_port_conflict_with_metrics_port() {
        let mut config = Config::default();
        // Set metrics port to a unique value so it doesn't collide with health port
        config.metrics.port = 9090;
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: 9090,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        let result = config.validate_tls_config();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("metrics.port"));
    }

    #[test]
    fn test_tls_config_port_conflict_with_dashboard_port() {
        let mut config = Config::default();
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: config.dashboard.port,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        let result = config.validate_tls_config();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("dashboard.port"));
    }

    #[test]
    fn test_tls_config_unique_port_passes() {
        let mut config = Config::default();
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: 9999,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn test_tls_config_none_passes() {
        let config = Config::default();
        assert!(config.validate_tls_config().is_ok());
    }

    #[test]
    fn test_tls_config_disabled_skips_port_conflict_check() {
        let mut config = Config::default();
        config.server.tls = Some(TlsConfig {
            enabled: false,
            tls_proxy_port: config.server.http_port, // conflicts, but TLS is disabled
            cert_path: String::new(),
            key_path: String::new(),
            connect_allowlist: None,
        });
        // Port 0 check still applies
        // But http_port (80) is not 0, so this should pass since enabled=false skips conflict check
        assert!(config.validate_tls_config().is_ok());
    }

    // ── Proxy-only mode tests (Task 6) ──────────────────────────────────

    /// 6.1 ServerMode deserialization: "standard" → Standard, "proxy_only" → ProxyOnly, invalid → error
    /// Validates: Requirements 1.1
    #[test]
    fn test_server_mode_deserialize_standard() {
        let mode: ServerMode = serde_yaml_ng::from_str("\"standard\"").unwrap();
        assert_eq!(mode, ServerMode::Standard);
    }

    #[test]
    fn test_server_mode_deserialize_proxy_only() {
        let mode: ServerMode = serde_yaml_ng::from_str("\"proxy_only\"").unwrap();
        assert_eq!(mode, ServerMode::ProxyOnly);
    }

    #[test]
    fn test_server_mode_deserialize_invalid_value() {
        let result: std::result::Result<ServerMode, _> = serde_yaml_ng::from_str("\"invalid\"");
        assert!(result.is_err());
    }

    /// 6.2 ServerConfig defaults: mode == Standard, proxy_port == 3128
    /// Validates: Requirements 1.2, 1.3, 6.2
    #[test]
    fn test_server_config_defaults() {
        let server = ServerConfig::default();
        assert_eq!(server.mode, ServerMode::Standard);
        assert_eq!(server.proxy_port, 3128);
    }

    /// 6.3 validate_proxy_only_mode returns Ok for valid proxy-only config
    /// Validates: Requirements 2.1, 2.2, 2.3, 2.4
    #[test]
    fn test_validate_proxy_only_mode_valid_config() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        config.server.proxy_port = 3128;
        assert!(config.validate_proxy_only_mode().is_ok());
    }

    /// 6.4 validate_proxy_only_mode returns Err when proxy_port == 0
    /// Validates: Requirements 2.1
    #[test]
    fn test_validate_proxy_only_mode_port_zero() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        config.server.proxy_port = 0;
        let result = config.validate_proxy_only_mode();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("proxy_port"));
    }

    /// 6.5 validate_proxy_only_mode returns Err for port conflicts
    /// Validates: Requirements 2.2, 2.3
    #[test]
    fn test_validate_proxy_only_mode_conflict_health_port() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        config.server.proxy_port = config.health.port; // 8080
        let result = config.validate_proxy_only_mode();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("health"));
    }

    #[test]
    fn test_validate_proxy_only_mode_conflict_metrics_port() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        // Set metrics to a unique port so it doesn't collide with health
        config.metrics.port = 9090;
        config.server.proxy_port = 9090;
        let result = config.validate_proxy_only_mode();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("metrics"));
    }

    #[test]
    fn test_validate_proxy_only_mode_conflict_dashboard_port() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        config.server.proxy_port = config.dashboard.port; // 8081
        let result = config.validate_proxy_only_mode();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("dashboard"));
    }

    #[test]
    fn test_validate_proxy_only_mode_conflict_tls_proxy_port() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        config.server.proxy_port = 3129;
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port: 3129,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });
        let result = config.validate_proxy_only_mode();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("tls_proxy_port"));
    }

    /// 6.6 validate_proxy_only_mode returns Ok for privileged port (< 1024) — warning only
    /// Validates: Requirements 2.4
    #[test]
    fn test_validate_proxy_only_mode_privileged_port_ok() {
        let mut config = Config::default();
        config.server.mode = ServerMode::ProxyOnly;
        config.server.proxy_port = 80;
        // Should succeed (warning only, not an error)
        assert!(config.validate_proxy_only_mode().is_ok());
    }

    /// 6.7 validate_proxy_only_mode returns Ok immediately when mode is Standard
    /// Validates: Requirements 2.5
    #[test]
    fn test_validate_proxy_only_mode_standard_mode_skips() {
        let mut config = Config::default();
        config.server.mode = ServerMode::Standard;
        // Even with an invalid proxy_port, standard mode skips validation
        config.server.proxy_port = 0;
        assert!(config.validate_proxy_only_mode().is_ok());
    }

    /// 6.8 Config parsing with mode: "proxy_only" and custom proxy_port
    /// Validates: Requirements 1.1, 1.3, 6.1
    #[test]
    fn test_config_parse_proxy_only_mode_with_custom_port() {
        let yaml = r#"
server:
  mode: "proxy_only"
  proxy_port: 4000
  http_port: 8000
  https_port: 8443
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
        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.server.mode, ServerMode::ProxyOnly);
        assert_eq!(config.server.proxy_port, 4000);
    }

    #[test]
    fn test_compression_batch_size_default() {
        // Requirement 5.1: default compression_batch_size is 1 MiB
        assert_eq!(CacheConfig::default().compression_batch_size, 1_048_576);
        // Default value is valid per the validation range
        assert!(CacheConfig::default().validate().is_ok());
    }

    #[test]
    fn test_compression_batch_size_missing_from_yaml_uses_default() {
        // Requirement 5.2: when the field is not present in YAML, the default
        // (1 MiB) is used via `#[serde(default = "default_compression_batch_size")]`.
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

        let config: Config = serde_yaml_ng::from_str(yaml).expect("Failed to parse config");
        assert_eq!(config.cache.compression_batch_size, 1_048_576);
    }

    #[test]
    fn test_compression_content_aware_alias_still_parses() {
        // compression-content-aware-fix spec, Requirement 4.1/4.3/8.2: a
        // pre-upgrade config setting `compression.content_aware` must still
        // parse (via the serde alias) rather than fail configuration
        // validation. The value lands in `content_aware_deprecated` and has
        // no effect on behavior.
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
  content_aware: false
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
"#;

        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("content_aware must still parse via alias");
        assert_eq!(config.compression.content_aware_deprecated, Some(false));
        // Behavior is unaffected regardless of the (ignored) value.
        assert!(config.compression.enabled);
        assert_eq!(config.compression.threshold, 1024);
    }

    #[test]
    fn test_compression_section_omitted_uses_defaults() {
        // compression-content-aware-fix spec, Requirement 7.1/8.1: a config
        // omitting the `compression:` section entirely must still start,
        // with `enabled: true`, `threshold: 1024`, and no deprecated field set.
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
"#;

        let config: Config = serde_yaml_ng::from_str(yaml)
            .expect("omitting the compression section entirely must still parse");
        assert!(config.compression.enabled);
        assert_eq!(config.compression.threshold, 1024);
        assert_eq!(config.compression.content_aware_deprecated, None);
    }

    #[test]
    fn test_compression_batch_size_below_range_rejected() {
        // Requirement 5.4: values below 64 KiB must be rejected.
        let cfg = CacheConfig {
            compression_batch_size: 32 * 1024, // 32 KiB, below the 64 KiB minimum
            ..CacheConfig::default()
        };
        let err = cfg.validate().expect_err("32 KiB must fail validation");
        assert!(
            err.contains("compression_batch_size"),
            "error should mention the field name, got: {}",
            err
        );
    }

    #[test]
    fn test_compression_batch_size_above_range_rejected() {
        // Requirement 5.4: values above 16 MiB must be rejected.
        let cfg = CacheConfig {
            compression_batch_size: 32 * 1024 * 1024, // 32 MiB, above the 16 MiB maximum
            ..CacheConfig::default()
        };
        let err = cfg.validate().expect_err("32 MiB must fail validation");
        assert!(
            err.contains("compression_batch_size"),
            "error should mention the field name, got: {}",
            err
        );
    }

    #[test]
    fn test_ttl_validation_get_ttl_exceeds_max() {
        // Requirement 8.5: TTL fields exceeding 315_360_000 seconds must be rejected.
        let cfg = CacheConfig {
            get_ttl: Duration::from_secs(999_999_999_999),
            ..CacheConfig::default()
        };
        let err = cfg.validate().expect_err("get_ttl exceeding max must fail");
        assert!(
            err.contains("cache.get_ttl"),
            "error should name the field, got: {}",
            err
        );
        assert!(
            err.contains("999999999999"),
            "error should include the value, got: {}",
            err
        );
        assert!(
            err.contains("315360000"),
            "error should include the maximum, got: {}",
            err
        );
    }

    #[test]
    fn test_ttl_validation_put_ttl_exceeds_max() {
        // Requirement 8.5: TTL fields exceeding 315_360_000 seconds must be rejected.
        let cfg = CacheConfig {
            put_ttl: Duration::from_secs(315_360_001),
            ..CacheConfig::default()
        };
        let err = cfg.validate().expect_err("put_ttl exceeding max must fail");
        assert!(
            err.contains("cache.put_ttl"),
            "error should name the field, got: {}",
            err
        );
    }

    #[test]
    fn test_ttl_validation_head_ttl_exceeds_max() {
        // Requirement 8.5: TTL fields exceeding 315_360_000 seconds must be rejected.
        let cfg = CacheConfig {
            head_ttl: Duration::from_secs(400_000_000),
            ..CacheConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("head_ttl exceeding max must fail");
        assert!(
            err.contains("cache.head_ttl"),
            "error should name the field, got: {}",
            err
        );
    }

    #[test]
    fn test_ttl_validation_incomplete_upload_ttl_exceeds_max() {
        // Requirement 8.5: TTL fields exceeding 315_360_000 seconds must be rejected.
        let cfg = CacheConfig {
            incomplete_upload_ttl: Duration::from_secs(315_360_001),
            ..CacheConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("incomplete_upload_ttl exceeding max must fail");
        assert!(
            err.contains("cache.incomplete_upload_ttl"),
            "error should name the field, got: {}",
            err
        );
    }

    #[test]
    fn test_ttl_validation_at_boundary_accepted() {
        // Requirement 8.5: TTL value exactly at 315_360_000 seconds should be accepted.
        let cfg = CacheConfig {
            get_ttl: Duration::from_secs(315_360_000),
            put_ttl: Duration::from_secs(315_360_000),
            head_ttl: Duration::from_secs(315_360_000),
            incomplete_upload_ttl: Duration::from_secs(315_360_000),
            ..CacheConfig::default()
        };
        assert!(
            cfg.validate().is_ok(),
            "TTL at exactly the maximum should be accepted"
        );
    }

    #[test]
    fn test_ttl_validation_zero_accepted() {
        // TTL=0 is valid (means "do not cache / per-request auth").
        let cfg = CacheConfig {
            get_ttl: Duration::from_secs(0),
            put_ttl: Duration::from_secs(0),
            head_ttl: Duration::from_secs(0),
            incomplete_upload_ttl: Duration::from_secs(0),
            ..CacheConfig::default()
        };
        assert!(cfg.validate().is_ok(), "TTL=0 should be accepted");
    }

    // --- ram_cache_shard_count tests (Requirements 5.2, 5.3) ---

    #[test]
    fn test_ram_cache_shard_count_default() {
        // Default shard count is 8.
        let cfg = CacheConfig::default();
        assert_eq!(cfg.ram_cache_shard_count, 8);
    }

    #[test]
    fn test_ram_cache_shard_count_missing_from_yaml_uses_default() {
        // Config-compatibility: a YAML that omits `ram_cache_shard_count` must
        // fall back to the default of 8 so upgrading the binary never requires
        // editing an existing config file.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");
        assert_eq!(
            config.cache.ram_cache_shard_count, 8,
            "omitted ram_cache_shard_count must default to 8"
        );
    }

    #[test]
    fn test_eviction_algorithm_default_is_tinylfu() {
        // cache-eviction-at-scale R10.5: TinyLFU is the default. R4 makes the entry
        // dimension a continuous reclaimer, so ordering quality applies on every
        // rotation rather than only under pressure.
        assert_eq!(
            CacheConfig::default().eviction_algorithm,
            EvictionAlgorithm::TinyLFU,
            "the default eviction algorithm must be TinyLFU"
        );
    }

    #[test]
    fn test_eviction_algorithm_missing_from_yaml_defaults_to_tinylfu() {
        // R10.5 + R9.3. A YAML that omits `eviction_algorithm` gets TinyLFU, which is
        // also what `config/config.example.yaml` ships explicitly — so the documented
        // starting configuration and the default now agree instead of diverging.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");
        assert_eq!(
            config.cache.eviction_algorithm,
            EvictionAlgorithm::TinyLFU,
            "omitted eviction_algorithm must default to TinyLFU"
        );
    }

    /// **Config-compatibility guard — read before deleting.**
    ///
    /// cache-eviction-at-scale R10.9 requires `"lru"` to keep parsing for at least one
    /// release after it stops being the default, because the documented upgrade
    /// contract in `docs/UPGRADING.md` is `cargo build` → copy binary → restart with
    /// **no config edits**. A config that fails to parse on upgrade is a startup
    /// failure on every host that set the field explicitly.
    ///
    /// This test is the executable form of that contract. When LRU is eventually
    /// retired, this assertion MUST NOT simply be deleted — it must be changed to
    /// assert that `"lru"` still parses and resolves to the replacement, with a
    /// warning. Deleting it is how the contract breaks silently.
    ///
    /// **Retargeted in 3.0.0, following that instruction rather than deleting it.** R10.10
    /// removed LRU as a choice, so the assertion is no longer "`\"lru\"` selects LRU" — it is
    /// now the four things an operator's upgrade depends on: the value parses, it validates
    /// (so the proxy starts), it resolves to `TinyLFU` behaviour, and it warns. The last two
    /// are what make this a *compatibility* test rather than a parsing test.
    ///
    /// Requirement: cache-eviction-at-scale R10.9, R10.10
    #[test]
    fn test_eviction_algorithm_lru_still_parses_after_default_moved() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
  eviction_algorithm: "lru"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("a config selecting \"lru\" must still parse");

        // 1. It parses, into the retired-spelling variant rather than being rejected.
        assert_eq!(
            config.cache.eviction_algorithm,
            EvictionAlgorithm::DeprecatedLru,
            "\"lru\" must still parse; rejecting it breaks the no-config-edit upgrade contract"
        );

        // 2. It validates, so the proxy starts.
        assert!(
            config.cache.validate().is_ok(),
            "a config selecting \"lru\" must still pass validation and start"
        );

        // 3. It BEHAVES as TinyLFU. This is the assertion the removal turns on: parsing a
        //    value that then selects nothing would satisfy the two above and still leave the
        //    operator running an algorithm that no longer exists.
        let effective: crate::cache::CacheEvictionAlgorithm =
            config.cache.eviction_algorithm.clone().into();
        assert_eq!(
            effective,
            crate::cache::CacheEvictionAlgorithm::TinyLFU,
            "R10.9: the retired \"lru\" spelling must behave as the replacement"
        );

        // 4. And it warns, rather than remapping in silence.
        assert!(
            config
                .cache
                .eviction_algorithm_deprecation_warning()
                .is_some(),
            "a config carrying the retired \"lru\" spelling must warn"
        );

        // Control, in the same test: the surviving spelling must NOT warn, so a helper that
        // returned Some unconditionally could not satisfy the assertion above.
        let tinylfu_yaml = yaml.replace("\"lru\"", "\"tinylfu\"");
        let tinylfu: Config =
            serde_yaml_ng::from_str(&tinylfu_yaml).expect("\"tinylfu\" must parse");
        assert_eq!(tinylfu.cache.eviction_algorithm, EvictionAlgorithm::TinyLFU);
        assert!(
            tinylfu
                .cache
                .eviction_algorithm_deprecation_warning()
                .is_none(),
            "the surviving spelling must not warn"
        );
    }

    /// **`Serialize` must not silently respell an operator's config.**
    ///
    /// This is the property a bare `#[serde(alias = "lru")]` on `TinyLFU` would lose: it
    /// would read `"lru"` and write back `"tinylfu"`, so anything that round-trips a config
    /// would rewrite a field the operator set. Asserted rather than assumed, because the
    /// alias is the mechanism a future reader is most likely to "simplify" this to.
    ///
    /// Requirement: cache-eviction-at-scale R10.9
    #[test]
    fn test_retired_lru_spelling_round_trips_without_respelling() {
        let parsed: EvictionAlgorithm =
            serde_yaml_ng::from_str("lru").expect("\"lru\" must deserialize");
        assert_eq!(parsed, EvictionAlgorithm::DeprecatedLru);

        let written = serde_yaml_ng::to_string(&parsed).expect("must serialize");
        assert_eq!(
            written.trim(),
            "lru",
            "the retired spelling must round-trip as written, not be rewritten as \"tinylfu\""
        );

        // An unknown value is still rejected — the alias widens what is accepted by exactly
        // one spelling, not generally.
        assert!(
            serde_yaml_ng::from_str::<EvictionAlgorithm>("lfu").is_err(),
            "an unrecognised algorithm must still be rejected"
        );
    }

    #[test]
    fn test_eviction_algorithm_tinylfu_parses_explicitly() {
        // The value `config/config.example.yaml` ships. Asserted separately from the
        // default so that a future change to either cannot make the other vacuous.
        let yaml = r#"
cache:
  cache_dir: "./cache"
  eviction_algorithm: "tinylfu"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("explicit tinylfu must parse");
        assert_eq!(
            config.cache.eviction_algorithm,
            EvictionAlgorithm::TinyLFU,
            "\"tinylfu\" must parse to TinyLFU"
        );
    }

    /// Two-sided, because a deprecation warning that fires for every value is not a
    /// warning about anything. The TinyLFU and default arms are the control: they are
    /// what makes a positive reading on the LRU arm attributable to the *value* rather
    /// than to the helper returning `Some` unconditionally.
    ///
    /// Requirement: cache-eviction-at-scale R10.5
    #[test]
    fn test_eviction_algorithm_deprecation_warning_fires_only_for_lru() {
        let lru = CacheConfig {
            eviction_algorithm: EvictionAlgorithm::DeprecatedLru,
            ..CacheConfig::default()
        };
        let message = lru
            .eviction_algorithm_deprecation_warning()
            .expect("'lru' must produce a deprecation notice");

        // Assert the parts an operator needs in order to act, not the whole string:
        // the field name to grep their config for, the offending value, the
        // replacement, and the fact that it is going. A whole-string assertion would
        // break on any rewording and teach nothing.
        for needle in [
            "cache.eviction_algorithm",
            "lru",
            "tinylfu",
            "no longer the default",
            "silence this warning",
            // Added in 3.0.0 with the removal. The first is what stops an operator reading
            // the WARN as a startup failure; the second is what tells them which algorithm
            // they are now running. Neither was needed while LRU was still selectable.
            "still accepted",
            "behaves as 'tinylfu'",
        ] {
            assert!(
                message.contains(needle),
                "the deprecation notice must name {needle:?}; got: {message}"
            );
        }

        // **The notice must not promise recency ordering, and this assertion is the reason
        // the wording was rewritten rather than left alone.** Until R10.10 it said "eviction
        // continues to order by recency", which was true then and is false now — LRU is not
        // reachable from config. An operator who believed it would go looking for the cause
        // of recency-ordered eviction that is not running.
        assert!(
            !message.contains("order by recency"),
            "'lru' no longer selects recency ordering; the notice must not say it does: {message}"
        );
        // The notice states current behaviour only. It used to promise that "lru" would be
        // rejected in a release after 3.0.0; no such release is scheduled, and a promise in
        // a WARN line is one an operator plans around.
        assert!(
            !message.contains("scheduled for removal") && !message.contains("will be rejected"),
            "the notice must not promise a future removal: {message}"
        );
        assert!(
            !message.contains("still takes effect"),
            "the pre-removal wording claimed the setting selected an algorithm; it no longer \
             does, and this phrasing must not survive the removal: {message}"
        );

        let tinylfu = CacheConfig {
            eviction_algorithm: EvictionAlgorithm::TinyLFU,
            ..CacheConfig::default()
        };
        assert!(
            tinylfu.eviction_algorithm_deprecation_warning().is_none(),
            "'tinylfu' is the default and must not warn"
        );
        assert!(
            CacheConfig::default()
                .eviction_algorithm_deprecation_warning()
                .is_none(),
            "the default config must not warn"
        );
    }

    /// **The WIRING assertion, and it is the one that matters.** The helper above can
    /// return perfect text and never be called, which produces exactly the silence this
    /// change exists to end — so this drives the real `log_deprecated_fields` path a
    /// config load takes and reads the warning back off a tracing subscriber.
    ///
    /// Its red side is deleting the two-line `if let` in `log_deprecated_fields`: this
    /// test goes red while the pure-function test above stays green, which is what
    /// distinguishes "the text is wrong" from "nothing logs it".
    ///
    /// Requirement: cache-eviction-at-scale R10.5
    #[test]
    fn test_eviction_algorithm_lru_deprecation_warning_reaches_the_log() {
        let lru_config = Config {
            cache: CacheConfig {
                eviction_algorithm: EvictionAlgorithm::DeprecatedLru,
                ..CacheConfig::default()
            },
            ..Config::default()
        };
        let captured = capture_warnings(|| lru_config.log_deprecated_fields());
        assert!(
            captured.contains("cache.eviction_algorithm"),
            "loading a config that selects 'lru' must emit a WARN naming the field; \
             captured output was: {captured:?}"
        );

        // Control, in the same run and through the same subscriber: the default config
        // must stay quiet about this field. Without it, a subscriber that captured
        // anything at all would satisfy the assertion above.
        let default_config = Config::default();
        let quiet = capture_warnings(|| default_config.log_deprecated_fields());
        assert!(
            !quiet.contains("cache.eviction_algorithm"),
            "a default config must not warn about eviction_algorithm; got: {quiet:?}"
        );
    }

    /// `health.check_interval` and `cache.initialization.*` warn when set to a non-default
    /// value and stay quiet at their defaults.
    #[test]
    fn test_unread_health_and_initialization_fields_warn_when_set() {
        assert_eq!(Config::default().unread_fields_warning(), None);
        let mut config = Config::default();
        config.health.check_interval = Duration::from_secs(60);
        config.cache.initialization.parallel_scan = false;
        config.cache.initialization.scan_timeout = Duration::from_secs(300);
        config.cache.initialization.progress_logging = false;
        let message = config
            .unread_fields_warning()
            .expect("non-default values must warn");
        for field in [
            "health.check_interval",
            "cache.initialization.parallel_scan",
            "cache.initialization.scan_timeout",
            "cache.initialization.progress_logging",
        ] {
            assert!(message.contains(field), "{field} missing from: {message}");
        }
        let captured = capture_warnings(|| config.log_deprecated_fields());
        assert!(
            captured.contains("health.check_interval"),
            "the warning must reach the log; captured: {captured:?}"
        );
        let mut one = Config::default();
        one.cache.initialization.scan_timeout = Duration::from_secs(300);
        let message = one
            .unread_fields_warning()
            .expect("one field set must warn");
        assert!(!message.contains("health.check_interval"), "{message}");
    }

    /// A `cache.initialization.scan_timeout` outside 5-300 s used to stop `Config::load`
    /// ("Invalid initialization configuration"). Nothing reads the field, so it now loads and
    /// the unread-fields warning names it and the flags that set it.
    #[test]
    fn out_of_range_scan_timeout_loads_and_warns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "cache:\n  initialization:\n    scan_timeout: \"10m\"\n",
        )
        .unwrap();
        let matches = Config::build_cli()
            .try_get_matches_from(vec!["s3-proxy", "-c", path.to_str().unwrap()])
            .unwrap();
        let mut loaded = None;
        let logged = capture_warnings(|| loaded = Some(Config::load_from_matches(&matches)));
        let config = loaded
            .unwrap()
            .expect("an out-of-range scan_timeout must no longer refuse to load");
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(600)
        );
        assert!(
            logged.contains("cache.initialization.scan_timeout"),
            "warning missing; captured: {logged:?}"
        );
        assert!(
            logged.contains("--scan-timeout"),
            "flags missing; captured: {logged:?}"
        );
    }

    /// `cache.shared_storage.consolidation_size_threshold` is accepted and has no effect.
    ///
    /// A config carrying it (at a non-default value, and at the default the example ships)
    /// must still load with the value parsed, and loading it must emit exactly one WARN naming
    /// the field. A config without it must load with no such WARN.
    #[test]
    fn consolidation_size_threshold_loads_and_warns_once_when_present() {
        let field = "cache.shared_storage.consolidation_size_threshold";
        for (yaml, expected) in [
            (
                "cache:\n  shared_storage:\n    consolidation_size_threshold: 2097152\n",
                2_097_152u64,
            ),
            (
                "cache:\n  shared_storage:\n    consolidation_size_threshold: 1048576\n",
                1_048_576u64,
            ),
        ] {
            let config: Config =
                serde_yaml_ng::from_str(yaml).expect("config with the field loads");
            assert_eq!(
                config.cache.shared_storage.consolidation_size_threshold,
                expected
            );
            assert!(config.cache.shared_storage.validate().is_ok());
            let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml).unwrap();
            let captured = capture_warnings(|| warn_on_inert_cache_keys(&document));
            assert_eq!(
                captured.matches(field).count(),
                1,
                "a config carrying the field must WARN exactly once naming it; captured: {captured:?}"
            );
        }

        let yaml = "cache:\n  shared_storage:\n    consolidation_interval: \"5s\"\n";
        let config: Config = serde_yaml_ng::from_str(yaml).expect("config without the field loads");
        assert_eq!(
            config.cache.shared_storage.consolidation_size_threshold,
            default_consolidation_size_threshold()
        );
        let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml).unwrap();
        let quiet = capture_warnings(|| warn_on_inert_cache_keys(&document));
        assert!(
            !quiet.contains("consolidation_size_threshold"),
            "a config without the field must not warn about it; got: {quiet:?}"
        );
    }

    /// `cache.ram_cache_flush_threshold` is REMOVED, and a config carrying it must still load.
    ///
    /// The upgrade contract in the config compatibility rule is that an existing
    /// config file keeps parsing across an upgrade, so removing a field must not turn a running
    /// deployment into a failed start. The value used here is one the RETIRED 10-10000 range
    /// check rejected — it warned and clamped to 100 — which proves both that the key is still
    /// accepted and that no surviving rule rejects it: validation only ever got looser here, so
    /// no config that loaded before this release fails now.
    #[test]
    fn a_config_carrying_the_removed_ram_cache_flush_threshold_still_loads() {
        let yaml = r#"
ram_cache_flush_threshold: 999999
ram_cache_verification_interval: 5s
"#;
        let parsed: CacheConfig = serde_yaml_ng::from_str(yaml)
            .expect("a config carrying the removed ram_cache_flush_threshold must parse");
        assert_eq!(
            parsed.ram_cache_verification_interval,
            Duration::from_secs(5),
            "a field AFTER the removed key must still be read, so the unknown key is skipped \
             rather than terminating the mapping"
        );
        assert!(
            parsed.validate().is_ok(),
            "999999 exceeded the retired 10-10000 range; with that rule gone it must validate"
        );
    }

    /// `cache.ram_cache_flush_on_eviction` is REMOVED, and a config carrying it must still load.
    ///
    /// Sibling of the `ram_cache_flush_threshold` test above; see it for the contract. `true` is
    /// used because it is the non-default value, so the key is doing something observable in the
    /// document rather than matching what the old default would have produced anyway.
    #[test]
    fn a_config_carrying_the_removed_ram_cache_flush_on_eviction_still_loads() {
        let yaml = r#"
ram_cache_flush_on_eviction: true
ram_cache_verification_interval: 5s
"#;
        let parsed: CacheConfig = serde_yaml_ng::from_str(yaml)
            .expect("a config carrying the removed ram_cache_flush_on_eviction must parse");
        assert_eq!(
            parsed.ram_cache_verification_interval,
            Duration::from_secs(5),
            "a field AFTER the removed key must still be read, so the unknown key is skipped \
             rather than terminating the mapping"
        );
        assert!(parsed.validate().is_ok());
    }

    /// A document setting either removed flush key must produce a WARN naming it.
    ///
    /// Silence is the failure mode this change exists to fix. Before it, the startup log printed
    /// `ram_cache_flush_threshold` back to the operator under a "flush config" label, so tuning a
    /// dead knob looked like it had taken effect. Loading must now say the field is removed and
    /// had no effect.
    ///
    /// The values here are the FORMER DEFAULTS — `100` and `false` — and that is the point: they
    /// are what `config/config.example.yaml` shipped, so a check keyed off deviation from a
    /// default would stay silent for precisely the operators who copied the example. This is
    /// keyed off the key's presence instead, and these values are what demonstrate the
    /// difference.
    #[test]
    fn the_removed_ram_cache_flush_keys_warn_when_a_document_sets_them() {
        let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
            r#"
cache:
  ram_cache_flush_threshold: 100
  ram_cache_flush_on_eviction: false
"#,
        )
        .expect("fixture must parse");
        let captured = capture_warnings(|| warn_on_removed_cache_keys(&document));
        assert!(
            captured.contains("cache.ram_cache_flush_threshold"),
            "a document setting the removed threshold must emit a WARN naming it, even at its \
             former default of 100; captured output was: {captured:?}"
        );
        assert!(
            captured.contains("cache.ram_cache_flush_on_eviction"),
            "a document setting the removed flush-on-eviction key must emit a WARN naming it, \
             even at its former default of false; captured output was: {captured:?}"
        );

        // Control, in the same run and through the same subscriber: a document that does not
        // carry the keys must stay quiet. Without it, a subscriber that captured anything at all
        // would satisfy both assertions above.
        let without: serde_yaml_ng::Value = serde_yaml_ng::from_str(
            r#"
cache:
  ram_cache_flush_interval: 10s
"#,
        )
        .expect("fixture must parse");
        let quiet = capture_warnings(|| warn_on_removed_cache_keys(&without));
        assert!(
            quiet.is_empty(),
            "a document carrying neither removed key must warn about neither — and \
             'ram_cache_flush_interval' is NOT removed, so a check matching on the shared \
             'ram_cache_flush' prefix would wrongly fire here; got: {quiet:?}"
        );
    }

    /// The removed `cache.metadata_cache.stale_handle_max_retries` must warn under its NESTED
    /// path, and only there.
    ///
    /// The table was flat — keyed for keys directly under `cache:` — and this key sits one
    /// section down, so the walker now descends one mapping per path segment. Two controls, in
    /// the same run and through the same subscriber, make the positive assertion attributable to
    /// that descent rather than to anything capturing output at all:
    ///
    /// - a `metadata_cache:` section that does not carry the key must stay quiet, so a walker
    ///   that stopped at the first segment (warning on the section's mere presence) fails; and
    /// - the same key name placed directly under `cache:` must stay quiet, so a walker that
    ///   matched only the LAST segment against the top-level mapping fails. That key is not
    ///   removed at that level; it never existed there.
    #[test]
    fn the_retired_stale_handle_max_retries_warns_under_its_nested_path_only() {
        let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(
            r#"
cache:
  metadata_cache:
    enabled: true
    stale_handle_max_retries: 3
"#,
        )
        .expect("fixture must parse");
        let captured = capture_warnings(|| warn_on_removed_cache_keys(&document));
        assert!(
            captured.contains("cache.metadata_cache.stale_handle_max_retries"),
            "a document setting the removed nested key must emit a WARN naming its full path, \
             even at its former default of 3; captured output was: {captured:?}"
        );
        assert!(
            captured.contains("had no effect"),
            "the WARN must say the field never had an effect, not merely that it is removed; \
             captured output was: {captured:?}"
        );

        let section_without_key: serde_yaml_ng::Value = serde_yaml_ng::from_str(
            r#"
cache:
  metadata_cache:
    enabled: true
    max_entries: 100000
"#,
        )
        .expect("fixture must parse");
        let quiet = capture_warnings(|| warn_on_removed_cache_keys(&section_without_key));
        assert!(
            quiet.is_empty(),
            "a metadata_cache section without the removed key must warn about nothing — a \
             walker that stopped at the first path segment would fire here; got: {quiet:?}"
        );

        let wrong_level: serde_yaml_ng::Value = serde_yaml_ng::from_str(
            r#"
cache:
  stale_handle_max_retries: 3
"#,
        )
        .expect("fixture must parse");
        let quiet = capture_warnings(|| warn_on_removed_cache_keys(&wrong_level));
        assert!(
            quiet.is_empty(),
            "the key name directly under `cache:` is not the removed field — a walker matching \
             only the last path segment at the top level would fire here; got: {quiet:?}"
        );
    }

    /// A config file that still sets `cache.metadata_cache.stale_handle_max_retries` must LOAD
    /// through `Config::load_from_file`, warn, and keep reading the section — AT ANY VALUE.
    ///
    /// The out-of-range and wrongly-typed values are the point. While the field existed as a
    /// `u32`, `-1` and `"not-a-number"` failed to parse and so failed startup; `0` and `11`
    /// parsed, and the 1-10 range check in `MetadataCacheConfig::validate` would have rejected
    /// them had anything on the startup path called it (nothing did). Retiring the field must
    /// not leave any of those behind as a hidden rejection, so each value is loaded here, the
    /// section's validator is run anyway, and a sibling written AFTER the removed key must still
    /// be read — proving the unknown key was skipped rather than ending the mapping.
    ///
    /// Also the nested key's load-path wiring pin: it writes a REAL FILE and never calls
    /// `warn_on_removed_cache_keys` itself, so deleting that call from `load_from_file` reds it.
    #[test]
    fn the_retired_stale_handle_max_retries_still_loads_and_warns_at_any_value() {
        use std::io::Write;

        let base = yaml_with_orphan_recovery_interval("60s");
        let anchor = "  cache_dir: \"./cache\"\n";
        assert_eq!(
            base.matches(anchor).count(),
            1,
            "the fixture inserts the removed key after {anchor:?}, which must appear exactly \
             once inside `cache:`"
        );
        assert!(
            !base.contains("metadata_cache:"),
            "the base fixture must not already carry a metadata_cache section, or the insertion \
             below would produce a duplicate key"
        );

        for value in ["3", "0", "11", "-1", "\"not-a-number\""] {
            let with_removed_key = base.replace(
                anchor,
                &format!(
                    "  cache_dir: \"./cache\"\n  metadata_cache:\n    \
                     stale_handle_max_retries: {value}\n    max_entries: 500\n"
                ),
            );
            let mut temp_file = tempfile::NamedTempFile::new().unwrap();
            temp_file.write_all(with_removed_key.as_bytes()).unwrap();
            temp_file.flush().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string();

            let mut loaded = None;
            let captured = capture_warnings(|| {
                loaded = Some(Config::load_from_file(&path).unwrap_or_else(|e| {
                    panic!(
                        "a config file setting the retired stale_handle_max_retries to {value} \
                         must still LOAD — the upgrade contract requires it; got: {e}"
                    )
                }));
            });
            let config = loaded.expect("load_from_file ran inside the capture");
            assert_eq!(
                config.cache.metadata_cache.max_entries, 500,
                "max_entries follows the removed key in the fixture; if it is not read the \
                 unknown key terminated the mapping instead of being skipped (value {value})"
            );
            assert!(
                config.cache.metadata_cache.validate().is_ok(),
                "the metadata_cache validator must not reject a config for setting the retired \
                 field to {value}"
            );
            assert!(
                config.cache.validate().is_ok(),
                "the cache validator on the startup path must accept a config setting the \
                 retired field to {value}"
            );
            assert!(
                captured.contains("cache.metadata_cache.stale_handle_max_retries"),
                "loading a file that sets the retired nested key to {value} must emit a WARN \
                 naming it. Empty output means the walker is not reached from \
                 Config::load_from_file or does not descend into metadata_cache; captured: \
                 {captured:?}"
            );
        }
    }

    /// Every retired field, set in a YAML document, warns exactly once, naming the key.
    #[test]
    fn every_retired_field_warns_once_when_present() {
        let cases: [(&str, &str, &str); 10] = [
            (
                "cache",
                "ram_cache_flush_threshold: 5",
                "cache.ram_cache_flush_threshold",
            ),
            (
                "cache",
                "ram_cache_flush_on_eviction: true",
                "cache.ram_cache_flush_on_eviction",
            ),
            (
                "cache",
                "metadata_cache:\n    stale_handle_max_retries: 5",
                "cache.metadata_cache.stale_handle_max_retries",
            ),
            (
                "cache",
                "shared_storage:\n    recovery_max_concurrent: 4",
                "cache.shared_storage.recovery_max_concurrent",
            ),
            (
                "cache",
                "shared_storage:\n    lock_refresh_interval: 30s",
                "cache.shared_storage.lock_refresh_interval",
            ),
            (
                "cache",
                "shared_storage:\n    metadata_lock_timeout_ms: 45000",
                "cache.shared_storage.metadata_lock_timeout_ms",
            ),
            (
                "cache",
                "shared_storage:\n    orphan_reclaim_enabled: true",
                "cache.shared_storage.orphan_reclaim_enabled",
            ),
            (
                "cache",
                "shared_storage:\n    eviction_worklist_enabled: true",
                "cache.shared_storage.eviction_worklist_enabled",
            ),
            (
                "cache",
                "shared_storage:\n    consolidation_size_threshold: 1048576",
                "consolidation_size_threshold",
            ),
            ("server", "https_mode: passthrough", "server.https_mode"),
        ];
        for (section, body, needle) in cases {
            let yaml = format!("{section}:\n  {body}\n");
            let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(&yaml).unwrap();
            let captured = capture_warnings(|| {
                warn_on_removed_cache_keys(&document);
                warn_on_removed_server_keys(&document);
                warn_on_inert_cache_keys(&document);
            });
            assert_eq!(
                captured.matches(needle).count(),
                1,
                "{needle} must warn exactly once: {captured:?}"
            );
        }
    }

    /// The removed `recovery_max_concurrent` key still deserialises and validates, and the
    /// cache-key walker warns once for it. This calls the walker directly; it does not go
    /// through `Config::load_from_file` (the wiring test below pins that call site).
    #[test]
    fn a_config_naming_recovery_max_concurrent_loads_and_warns_once() {
        let yaml = "cache:\n  shared_storage:\n    recovery_max_concurrent: 4\n";
        let parsed: SharedStorageConfig =
            serde_yaml_ng::from_str("recovery_max_concurrent: 4").unwrap();
        assert!(parsed.validate().is_ok());
        let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml).unwrap();
        let captured = capture_warnings(|| warn_on_removed_cache_keys(&document));
        assert_eq!(
            captured.matches("recovery_max_concurrent").count(),
            1,
            "{captured:?}"
        );
    }

    /// The removed `max_cached_objects` key still deserialises and validates, and the cache-key
    /// walker warns once for it.
    #[test]
    fn a_config_naming_max_cached_objects_loads_and_warns_once() {
        let yaml = "cache:\n  shared_storage:\n    max_cached_objects: 1000000\n";
        let parsed: SharedStorageConfig =
            serde_yaml_ng::from_str("max_cached_objects: 1000000").unwrap();
        assert!(parsed.validate().is_ok());
        let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml).unwrap();
        let captured = capture_warnings(|| warn_on_removed_cache_keys(&document));
        assert_eq!(
            captured.matches("max_cached_objects").count(),
            1,
            "{captured:?}"
        );
    }
    /// The shipped example config must not set any removed key.
    ///
    /// It set all three, at their defaults, which is why the warnings above are keyed off
    /// presence. An example that makes every deployment copying it log a removed-field warning is
    /// worse than no example. The check walks the whole `REMOVED_CACHE_KEYS` table, so a key
    /// added to it later is covered here without editing this test.
    #[test]
    fn the_example_config_does_not_carry_any_removed_cache_key() {
        let content = std::fs::read_to_string("config/config.example.yaml")
            .expect("config/config.example.yaml must be readable from the crate root");
        let document: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&content).expect("the example config must parse");
        let captured = capture_warnings(|| warn_on_removed_cache_keys(&document));
        assert!(
            captured.is_empty(),
            "config/config.example.yaml must not set a removed cache key; it warned: {captured:?}"
        );
    }

    /// PINS THE CALL SITE, because the three tests above cannot.
    ///
    /// Deleting `warn_on_removed_cache_keys(&document)` from `Config::load_from_file` leaves
    /// every other test in this module green — measured. They establish that the walker works;
    /// none of them establishes that it runs, so the deprecation warning could stop being
    /// emitted in production against a fully green suite.
    ///
    /// Two vacuity traps, and this test exists only if it avoids both. It must NOT call
    /// `warn_on_removed_cache_keys` directly — that is precisely what the tests above do and
    /// precisely why they miss the wiring. And it must write a REAL FILE and load it, because
    /// handing a pre-parsed `serde_yaml_ng::Value` to the walker skips the load path the same
    /// way, one step later.
    ///
    /// The control loads the same fixture WITHOUT the removed key through the same subscriber:
    /// without it, a subscriber that captured anything at all would satisfy the assertion.
    #[test]
    fn the_removed_key_warning_is_wired_into_config_load_from_file() {
        use std::io::Write;

        // A minimal-but-complete document, plus one removed key inserted after a unique
        // anchor line inside `cache:`. The count assertion is what stops the fixture from
        // silently not carrying the key — an insertion that did not apply would leave this
        // test passing for the wrong reason only if the warning were also absent, but it
        // would make a red side unreadable.
        let base = yaml_with_orphan_recovery_interval("60s");
        let anchor = "  cache_dir: \"./cache\"\n";
        assert_eq!(
            base.matches(anchor).count(),
            1,
            "the fixture inserts the removed key after {anchor:?}, which must appear exactly \
             once inside `cache:` or the insertion is not where this test claims it is"
        );
        let with_removed_key = base.replace(
            anchor,
            "  cache_dir: \"./cache\"\n  ram_cache_flush_threshold: 100\n",
        );

        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        temp_file.write_all(with_removed_key.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let captured = capture_warnings(|| {
            Config::load_from_file(&path).expect(
                "a config file carrying a removed key must still LOAD — serde ignores it and \
                 the upgrade contract requires the file to keep working",
            );
        });
        assert!(
            captured.contains("cache.ram_cache_flush_threshold"),
            "loading a file that sets a removed key must emit a WARN naming it. Empty output \
             means the walker is no longer reached from Config::load_from_file, which no \
             direct-call test can detect; captured: {captured:?}"
        );

        let mut clean_file = tempfile::NamedTempFile::new().unwrap();
        clean_file.write_all(base.as_bytes()).unwrap();
        clean_file.flush().unwrap();
        let clean_path = clean_file.path().to_str().unwrap().to_string();
        let quiet = capture_warnings(|| {
            Config::load_from_file(&clean_path).expect("the base fixture must load");
        });
        assert!(
            quiet.is_empty(),
            "the same fixture without the removed key must load silently, or the assertion \
             above is satisfied by unrelated captured output; got: {quiet:?}"
        );
    }

    /// PINS THE CALL SITE of `warn_on_removed_server_keys`, which
    /// `every_retired_field_warns_once_when_present` calls directly and so cannot pin.
    ///
    /// A config FILE naming `server.https_mode` must still load (the field is gone from
    /// `ServerConfig`, serde ignores it), and loading it must emit a WARN naming the key. The
    /// control loads the same fixture without the key and must be silent.
    #[test]
    fn the_removed_https_mode_warning_is_wired_into_config_load_from_file() {
        use std::io::Write;

        let base = yaml_with_orphan_recovery_interval("60s");
        let anchor = "  http_port: 8000\n";
        assert_eq!(
            base.matches(anchor).count(),
            1,
            "fixture anchor must match once"
        );
        let with_key = base.replace(anchor, "  http_port: 8000\n  https_mode: \"passthrough\"\n");
        assert_eq!(with_key.matches("https_mode").count(), 1);

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(with_key.as_bytes()).unwrap();
        file.flush().unwrap();
        let path = file.path().to_str().unwrap().to_string();
        let captured = capture_warnings(|| {
            Config::load_from_file(&path)
                .expect("a config file naming server.https_mode must still load");
        });
        assert_eq!(
            captured.matches("server.https_mode").count(),
            1,
            "loading a file that names server.https_mode must warn once; empty output means \
             warn_on_removed_server_keys is no longer reached from load_from_file: {captured:?}"
        );

        let mut clean = tempfile::NamedTempFile::new().unwrap();
        clean.write_all(base.as_bytes()).unwrap();
        clean.flush().unwrap();
        let clean_path = clean.path().to_str().unwrap().to_string();
        let quiet = capture_warnings(|| {
            Config::load_from_file(&clean_path).expect("the base fixture must load");
        });
        assert!(
            !quiet.contains("server.https_mode"),
            "the fixture without the key must not warn about it: {quiet:?}"
        );
    }

    #[test]
    fn test_ram_cache_shard_count_zero_rejected() {
        // Requirement 5.3: shard count of 0 is outside the valid range (1–256)
        // and must be rejected by validate().
        let cfg = CacheConfig {
            ram_cache_shard_count: 0,
            ..CacheConfig::default()
        };
        let result = cfg.validate();
        assert!(result.is_err(), "shard_count=0 must be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("ram_cache_shard_count"),
            "error message must name the field, got: {}",
            err
        );
    }

    #[test]
    fn test_ram_cache_shard_count_257_rejected() {
        // Requirement 5.3: shard count of 257 is outside the valid range (1–256)
        // and must be rejected by validate().
        let cfg = CacheConfig {
            ram_cache_shard_count: 257,
            ..CacheConfig::default()
        };
        let result = cfg.validate();
        assert!(result.is_err(), "shard_count=257 must be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("ram_cache_shard_count"),
            "error message must name the field, got: {}",
            err
        );
    }

    // --- Per-bucket metrics config tests (per-bucket-metrics spec, Req 6.3, 6.5) ---

    #[test]
    fn test_per_bucket_metrics_defaults_when_all_fields_omitted() {
        // Config-compatibility regression (Req 6.3): a config that omits every
        // per-bucket field must parse, apply correct defaults, and the proxy
        // config must build. A binary-only upgrade never changes behavior.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml)
            .expect("config omitting all per-bucket fields must parse (config-compatibility)");

        // Export gate defaults to disabled (Req 6.1)
        assert!(
            !config.metrics.otlp.per_bucket_enabled,
            "metrics.otlp.per_bucket_enabled must default to false"
        );
        // Cardinality cap defaults to 100 (Req 7.1)
        assert_eq!(
            config.metrics.per_bucket.max_series, 100,
            "metrics.per_bucket.max_series must default to 100"
        );
        // Prefix map defaults to empty (bucket-level only, Req 5.1)
        assert!(
            config.metrics.per_bucket.bucket_prefixes.is_empty(),
            "metrics.per_bucket.bucket_prefixes must default to empty HashMap"
        );
    }

    #[test]
    fn test_per_bucket_metrics_explicit_config_parsed() {
        // Req 6.5: when the export gate and prefix lists are configured, assert
        // the gate is read from metrics.otlp.* and the knobs from
        // metrics.per_bucket.*.
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
metrics:
  enabled: true
  endpoint: "/metrics"
  port: 9090
  collection_interval: "60s"
  otlp:
    enabled: true
    endpoint: "http://localhost:4318"
    export_interval: "60s"
    timeout: "10s"
    compression: "none"
    headers: {}
    per_bucket_enabled: true
  per_bucket:
    max_series: 50
    bucket_prefixes:
      my-bucket:
        - "logs/"
        - "data/raw/"
      other-bucket:
        - "prefix-a/"
"#;

        let config: Config = serde_yaml_ng::from_str(yaml)
            .expect("config with explicit per-bucket fields must parse");

        // Export gate read from metrics.otlp.per_bucket_enabled
        assert!(
            config.metrics.otlp.per_bucket_enabled,
            "metrics.otlp.per_bucket_enabled must be true when explicitly set"
        );
        // Cardinality cap read from metrics.per_bucket.max_series
        assert_eq!(
            config.metrics.per_bucket.max_series, 50,
            "metrics.per_bucket.max_series must be 50 when explicitly set"
        );
        // Prefix map read from metrics.per_bucket.bucket_prefixes
        assert_eq!(
            config.metrics.per_bucket.bucket_prefixes.len(),
            2,
            "bucket_prefixes must contain 2 buckets"
        );
        let my_bucket_prefixes = config
            .metrics
            .per_bucket
            .bucket_prefixes
            .get("my-bucket")
            .expect("my-bucket must be present in bucket_prefixes");
        assert_eq!(
            my_bucket_prefixes,
            &vec!["logs/".to_string(), "data/raw/".to_string()],
            "my-bucket prefixes must match configured values"
        );
        let other_bucket_prefixes = config
            .metrics
            .per_bucket
            .bucket_prefixes
            .get("other-bucket")
            .expect("other-bucket must be present in bucket_prefixes");
        assert_eq!(
            other_bucket_prefixes,
            &vec!["prefix-a/".to_string()],
            "other-bucket prefixes must match configured values"
        );
    }

    // Listener bind addresses: an unparseable value must stop startup, never widen the bind.
    fn listener_config(health: &str, metrics: &str) -> Config {
        let mut config = Config::default();
        config.health.bind_address = health.to_string();
        config.metrics.bind_address = metrics.to_string();
        config
    }

    #[test]
    fn listener_addresses_reject_values_that_are_not_ip_addresses() {
        for bad in [
            "localhost",
            "",
            "  ",
            "127.0.0.1:8080",
            "[::1]",
            "127.0.0.1 ",
            "example.com",
        ] {
            let health = listener_config(bad, "127.0.0.1").validate_listener_addresses();
            let msg = health.expect_err(bad).to_string();
            assert!(msg.contains("health.bind_address"), "{bad:?}: {msg}");
            assert!(msg.contains("IP address"), "{bad:?}: {msg}");
            assert!(msg.contains(&format!("{bad:?}")), "{bad:?}: {msg}");
            let metrics = listener_config("127.0.0.1", bad).validate_listener_addresses();
            let msg = metrics.expect_err(bad).to_string();
            assert!(msg.contains("metrics.bind_address"), "{bad:?}: {msg}");
        }
    }

    #[test]
    fn listener_addresses_accept_ip_addresses() {
        for good in ["127.0.0.1", "0.0.0.0", "::1", "::"] {
            listener_config(good, good)
                .validate_listener_addresses()
                .unwrap_or_else(|e| panic!("{good}: {e}"));
        }
    }

    #[test]
    fn disabled_listener_with_unparseable_address_still_loads() {
        // A disabled listener never binds, so a value there has never had an effect.
        let mut config = listener_config("localhost", "localhost");
        config.health.enabled = false;
        config.metrics.enabled = false;
        config
            .validate_listener_addresses()
            .expect("disabled listeners are not checked");
    }

    #[test]
    fn minimal_config_keeps_loopback_listener_defaults_and_validates() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");
        assert_eq!(config.health.bind_address, "127.0.0.1");
        assert_eq!(config.metrics.bind_address, "127.0.0.1");
        config
            .validate_listener_addresses()
            .expect("defaults are IP addresses");
    }

    #[test]
    fn config_load_checks_listener_addresses_before_any_listener_opens() {
        // `Config::load` builds its own CLI matcher and cannot run here, so the call is
        // pinned by source, and main.rs must hold no wide-bind fallback.
        let source = include_str!("config.rs");
        let load = source
            .find("pub fn load() -> Result<Self>")
            .expect("Config::load");
        let load_end = source[load..]
            .find("fn load_from_file")
            .expect("end of load")
            + load;
        assert!(source[load..load_end].contains("config.validate_listener_addresses()?"));
        let main = include_str!("main.rs");
        assert!(
            !main.contains("Ipv4Addr::UNSPECIFIED"),
            "main.rs must not fall back to 0.0.0.0"
        );
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
            && (1..=300).contains(&cache_stats_secs)
            && (1..=300).contains(&logs_secs)
            && (10..=10000).contains(&max_entries);

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
        let defaults_correct = default_config.enabled  // Enabled by default per requirements
            && default_config.port == 8081
            && default_config.bind_address == "127.0.0.1"
            && default_config.cache_stats_refresh_interval == Duration::from_secs(5)
            && default_config.logs_refresh_interval == Duration::from_secs(10)
            && default_config.max_log_entries == 100;

        TestResult::from_bool(validation_result.is_ok() && defaults_correct)
    }
}

#[cfg(test)]
mod log_lifecycle_property_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    // Feature: log-lifecycle, Property 1: Config defaults are correct
    // **Validates: Requirements 1.1, 1.3, 2.1, 2.3, 3.1, 3.3, 9.1, 9.3**
    //
    // For any YAML configuration string that omits access_log_retention_days,
    // app_log_retention_days, log_cleanup_interval, and access_log_file_rotation_interval,
    // deserializing it into LoggingConfig should produce the correct defaults:
    //   access_log_retention_days = 30
    //   app_log_retention_days = 30
    //   log_cleanup_interval = 24h (86400s)
    //   access_log_file_rotation_interval = 5m (300s)
    #[quickcheck]
    fn prop_config_defaults_are_correct(
        access_dir_suffix: u8,
        app_dir_suffix: u8,
        enabled: bool,
        use_cached_only: bool,
    ) -> TestResult {
        let access_log_mode = if use_cached_only {
            "cached_only"
        } else {
            "all"
        };
        let log_levels = ["info", "debug", "warn", "error", "trace"];
        let log_level = log_levels[(access_dir_suffix as usize) % log_levels.len()];

        // Build YAML with required fields only — omit the four lifecycle fields
        let yaml = format!(
            r#"
access_log_dir: "/tmp/access_logs_{}"
app_log_dir: "/tmp/app_logs_{}"
access_log_enabled: {}
access_log_mode: "{}"
log_level: "{}"
"#,
            access_dir_suffix, app_dir_suffix, enabled, access_log_mode, log_level,
        );

        let config: LoggingConfig = match serde_yaml_ng::from_str(&yaml) {
            Ok(c) => c,
            Err(_) => return TestResult::discard(),
        };

        // Assert all four lifecycle defaults are correct
        let defaults_correct = config.access_log_retention_days == 30
            && config.app_log_retention_days == 30
            && config.log_cleanup_interval == Duration::from_secs(86400)
            && config.access_log_file_rotation_interval == Duration::from_secs(300);

        TestResult::from_bool(defaults_correct)
    }

    // Feature: log-lifecycle, Property 2: Config round-trip preserves values
    // **Validates: Requirements 1.2, 2.2, 3.2, 9.2**
    //
    // For any valid access_log_retention_days in [1, 365], app_log_retention_days in [1, 365],
    // log_cleanup_interval in [1h, 7d], and access_log_file_rotation_interval in [1m, 60m],
    // serializing these values into a YAML string and deserializing back should produce the same values.
    #[quickcheck]
    fn prop_config_round_trip_preserves_values(
        access_ret_raw: u16,
        app_ret_raw: u16,
        cleanup_hours_raw: u8,
        rotation_minutes_raw: u8,
    ) -> TestResult {
        // Map random values into valid ranges
        let access_retention = (access_ret_raw % 365) as u32 + 1; // [1, 365]
        let app_retention = (app_ret_raw % 365) as u32 + 1; // [1, 365]
        let cleanup_hours = (cleanup_hours_raw % 168) as u64 + 1; // [1h, 168h = 7d]
        let rotation_minutes = (rotation_minutes_raw % 60) as u64 + 1; // [1m, 60m]

        let cleanup_secs = cleanup_hours * 3600;
        let rotation_secs = rotation_minutes * 60;

        let yaml = format!(
            r#"
access_log_dir: "/tmp/access"
app_log_dir: "/tmp/app"
access_log_enabled: true
access_log_mode: "all"
log_level: "info"
access_log_retention_days: {}
app_log_retention_days: {}
log_cleanup_interval: "{}s"
access_log_file_rotation_interval: "{}s"
"#,
            access_retention, app_retention, cleanup_secs, rotation_secs,
        );

        let config: LoggingConfig = match serde_yaml_ng::from_str(&yaml) {
            Ok(c) => c,
            Err(_) => return TestResult::discard(),
        };

        let round_trip_correct = config.access_log_retention_days == access_retention
            && config.app_log_retention_days == app_retention
            && config.log_cleanup_interval == Duration::from_secs(cleanup_secs)
            && config.access_log_file_rotation_interval == Duration::from_secs(rotation_secs);

        TestResult::from_bool(round_trip_correct)
    }

    // Feature: log-lifecycle, Property 3: Config validation accepts in-range and rejects out-of-range
    // **Validates: Requirements 1.4, 2.4, 3.5, 9.4**
    //
    // For any u32 value for retention days and any Duration value for intervals,
    // LoggingConfig::validate() should return Ok if and only if all four fields are in range:
    //   access_log_retention_days in [1, 365]
    //   app_log_retention_days in [1, 365]
    //   log_cleanup_interval in [1h (3600s), 7d (604800s)]
    //   access_log_file_rotation_interval in [1m (60s), 60m (3600s)]
    #[quickcheck]
    fn prop_config_validation_accepts_in_range_rejects_out_of_range(
        access_ret: u32,
        app_ret: u32,
        cleanup_secs_raw: u32,
        rotation_secs_raw: u32,
    ) -> TestResult {
        let cleanup_secs = cleanup_secs_raw as u64;
        let rotation_secs = rotation_secs_raw as u64;

        let config = LoggingConfig {
            access_log_retention_days: access_ret,
            app_log_retention_days: app_ret,
            log_cleanup_interval: Duration::from_secs(cleanup_secs),
            access_log_file_rotation_interval: Duration::from_secs(rotation_secs),
            ..LoggingConfig::default()
        };

        let result = config.validate();

        let all_in_range = (1..=365).contains(&access_ret)
            && (1..=365).contains(&app_ret)
            && (3600..=604800).contains(&cleanup_secs)
            && (60..=3600).contains(&rotation_secs);

        TestResult::from_bool(result.is_ok() == all_in_range)
    }
}

#[cfg(test)]
mod tls_config_property_tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;

    /// Generate a random alphanumeric string of length 1..=max_len.
    fn random_nonempty_path(g: &mut Gen) -> String {
        let len = usize::arbitrary(g) % 20 + 1; // 1..=20
        (0..len)
            .map(|_| {
                let idx = usize::arbitrary(g) % 36;
                if idx < 10 {
                    (b'0' + idx as u8) as char
                } else {
                    (b'a' + (idx - 10) as u8) as char
                }
            })
            .collect()
    }

    impl Arbitrary for TlsConfig {
        fn arbitrary(g: &mut Gen) -> Self {
            let enabled = bool::arbitrary(g);
            // Port in 1024..65535
            let tls_proxy_port = (u16::arbitrary(g) % (65535 - 1024)) + 1024;
            let (cert_path, key_path) = if enabled {
                (random_nonempty_path(g), random_nonempty_path(g))
            } else {
                // When disabled, randomly choose empty or non-empty paths
                let cert = if bool::arbitrary(g) {
                    random_nonempty_path(g)
                } else {
                    String::new()
                };
                let key = if bool::arbitrary(g) {
                    random_nonempty_path(g)
                } else {
                    String::new()
                };
                (cert, key)
            };
            TlsConfig {
                enabled,
                tls_proxy_port,
                cert_path,
                key_path,
                connect_allowlist: None,
            }
        }
    }

    /// **Feature: tls-proxy-listener, Property 5: TLS config round-trip serialization**
    ///
    /// *For any* valid `TlsConfig` (with `tls_proxy_port` in 1024..65535, non-empty
    /// `cert_path` and `key_path` when enabled), serializing to YAML and deserializing
    /// back SHALL produce an equivalent `TlsConfig`.
    ///
    /// **Validates: Requirements 6.1, 6.4**
    #[quickcheck]
    fn prop_tls_config_round_trip_serialization(config: TlsConfig) -> TestResult {
        // Serialize to YAML
        let yaml = match serde_yaml_ng::to_string(&config) {
            Ok(y) => y,
            Err(_) => return TestResult::discard(),
        };

        // Deserialize back
        let deserialized: TlsConfig = match serde_yaml_ng::from_str(&yaml) {
            Ok(c) => c,
            Err(_) => return TestResult::failed(),
        };

        TestResult::from_bool(config == deserialized)
    }

    /// **Feature: tls-proxy-listener, Property 6: TLS config validation rejects invalid configurations**
    ///
    /// *For any* `TlsConfig` with `enabled = true`, validation SHALL reject the
    /// configuration if `cert_path` is empty OR `key_path` is empty.
    /// *For any* `TlsConfig` with `enabled = false`, validation SHALL accept the
    /// configuration regardless of `cert_path` and `key_path` values.
    ///
    /// **Validates: Requirements 6.2, 6.3**
    #[quickcheck]
    fn prop_tls_config_validation_rejects_invalid(
        enabled: bool,
        use_empty_cert: bool,
        use_empty_key: bool,
    ) -> TestResult {
        let cert_path = if use_empty_cert {
            String::new()
        } else {
            "/some/cert.pem".to_string()
        };
        let key_path = if use_empty_key {
            String::new()
        } else {
            "/some/key.pem".to_string()
        };
        // Use a valid non-zero port so port validation doesn't interfere
        let config = TlsConfig {
            enabled,
            tls_proxy_port: 8443,
            cert_path,
            key_path,
            connect_allowlist: None,
        };

        let result = config.validate();

        if !enabled {
            // Disabled TLS always passes validation (regardless of paths)
            TestResult::from_bool(result.is_ok())
        } else if use_empty_cert || use_empty_key {
            // Enabled with empty cert or key must be rejected
            TestResult::from_bool(result.is_err())
        } else {
            // Enabled with both paths non-empty and valid port must be accepted
            TestResult::from_bool(result.is_ok())
        }
    }

    /// **Feature: tls-proxy-listener, Property 7: Port conflict detection**
    ///
    /// *For any* server configuration where `tls.tls_proxy_port` equals any of
    /// `http_port`, `https_port`, `health.port`, `metrics.port`, or `dashboard.port`,
    /// config validation SHALL reject the configuration. *For any* configuration where
    /// `tls.tls_proxy_port` differs from all other ports, this check SHALL pass.
    ///
    /// **Validates: Requirements 6.5**
    #[quickcheck]
    #[allow(clippy::too_many_arguments)]
    fn prop_port_conflict_detection(
        tls_port: u16,
        http_port: u16,
        https_port: u16,
        health_port: u16,
        metrics_port: u16,
        dashboard_port: u16,
        force_conflict: bool,
        conflict_index: u8,
    ) -> TestResult {
        // Ensure tls_proxy_port is non-zero (port 0 is rejected by TlsConfig::validate)
        let tls_port = if tls_port == 0 { 1 } else { tls_port };

        let other_ports = [
            http_port,
            https_port,
            health_port,
            metrics_port,
            dashboard_port,
        ];

        // Determine the actual tls_proxy_port to use
        let tls_proxy_port = if force_conflict {
            // Pick one of the other ports to create a guaranteed conflict
            let idx = (conflict_index as usize) % other_ports.len();
            let conflicting = other_ports[idx];
            // If the chosen port is 0, discard (port 0 would fail TlsConfig::validate instead)
            if conflicting == 0 {
                return TestResult::discard();
            }
            conflicting
        } else {
            tls_port
        };

        let has_conflict = other_ports.contains(&tls_proxy_port);

        // Build a Config with these port values and TLS enabled with valid paths
        let mut config = Config::default();
        config.server.http_port = http_port;
        config.server.https_port = https_port;
        config.health.port = health_port;
        config.metrics.port = metrics_port;
        config.dashboard.port = dashboard_port;
        config.server.tls = Some(TlsConfig {
            enabled: true,
            tls_proxy_port,
            cert_path: "/cert.pem".to_string(),
            key_path: "/key.pem".to_string(),
            connect_allowlist: None,
        });

        let result = config.validate_tls_config();

        if has_conflict {
            TestResult::from_bool(result.is_err())
        } else {
            TestResult::from_bool(result.is_ok())
        }
    }
}

/// Property-based tests for rolling validation scan configuration
#[cfg(test)]
mod rolling_validation_config_property_tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: rolling-validation-scan, Property 2: Configuration validation rejects out-of-range values**
    ///
    /// *For any* `validation_max_duration` value less than 10 minutes or greater than 23 hours,
    /// `SharedStorageConfig::validate()` SHALL return an error. *For any* values within the valid
    /// range, validation SHALL succeed (assuming all other config fields are valid).
    ///
    /// **Validates: Requirements 2.2, 2.3**
    #[quickcheck]
    fn prop_validation_max_duration_rejects_out_of_range(
        secs: u64,
        force_invalid: bool,
    ) -> TestResult {
        const MIN_SECS: u64 = 10 * 60; // 10 minutes
        const MAX_SECS: u64 = 23 * 3600; // 23 hours

        let duration_secs = if force_invalid {
            // Generate an invalid value: either below min or above max
            if secs.is_multiple_of(2) {
                // Below minimum: 0 to MIN_SECS-1
                secs % MIN_SECS
            } else {
                // Above maximum: MAX_SECS+1 to MAX_SECS+100000
                MAX_SECS + 1 + (secs % 100_000)
            }
        } else {
            // Generate a valid value: MIN_SECS to MAX_SECS
            MIN_SECS + (secs % (MAX_SECS - MIN_SECS + 1))
        };

        let config = SharedStorageConfig {
            validation_max_duration: Duration::from_secs(duration_secs),
            ..SharedStorageConfig::default()
        };

        let result = config.validate();
        let is_valid = (MIN_SECS..=MAX_SECS).contains(&duration_secs);

        TestResult::from_bool(result.is_ok() == is_valid)
    }

    /// Regression test for serde_yaml → serde_yaml_ng migration (Requirement 30.4).
    /// Parses the full example config and asserts key fields from every major section
    /// to ensure the YAML parser migration didn't break any config parsing behavior.
    #[test]
    fn test_serde_yaml_ng_migration_parses_example_config() {
        let yaml = std::fs::read_to_string("config/config.example.yaml")
            .expect("Failed to read config/config.example.yaml");

        let config: Config =
            serde_yaml_ng::from_str(&yaml).expect("serde_yaml_ng failed to parse example config");

        // --- server section ---
        assert_eq!(config.server.http_port, 80);
        assert_eq!(config.server.https_port, 443);
        assert_eq!(config.server.max_concurrent_requests, 1000);
        assert_eq!(config.server.request_timeout, Duration::from_secs(30));
        assert!(config.server.add_referer_header);
        assert_eq!(config.server.mode, ServerMode::Standard);
        assert_eq!(config.server.proxy_port, 3128);

        // TLS sub-section
        let tls = config
            .server
            .tls
            .as_ref()
            .expect("tls section should be present");
        assert!(!tls.enabled);
        assert_eq!(tls.tls_proxy_port, 3129);
        assert_eq!(tls.cert_path, "/etc/proxy/tls/cert.pem");
        assert_eq!(tls.key_path, "/etc/proxy/tls/key.pem");

        // --- cache section ---
        assert_eq!(config.cache.cache_dir.to_str().unwrap(), "./tmp/cache");
        assert_eq!(config.cache.max_cache_size, 524_288_000);
        assert!(config.cache.ram_cache_enabled);
        assert_eq!(config.cache.max_ram_cache_size, 536_870_912);
        assert!(config.cache.write_cache_enabled);
        assert_eq!(config.cache.write_cache_percent, 10.0);
        assert_eq!(config.cache.write_cache_max_object_size, 268_435_456);
        // The example now carries the built-in default (1 hour) rather than a value
        // that diverges from it — see the 2.4.0 CHANGELOG entry.
        assert_eq!(config.cache.put_ttl, Duration::from_secs(3_600));
        assert_eq!(config.cache.put_ttl, CacheConfig::default().put_ttl);
        // The example carries the built-in default (1 day).
        assert_eq!(config.cache.get_ttl, Duration::from_secs(86_400));
        assert_eq!(config.cache.get_ttl, CacheConfig::default().get_ttl);
        assert_eq!(config.cache.head_ttl, Duration::from_secs(60));
        assert!(!config.cache.actively_remove_cached_data);
        // The example carries the built-in default (10s), as does every other
        // RAM-coherency field below it.
        assert_eq!(
            config.cache.ram_cache_flush_interval,
            Duration::from_secs(10)
        );
        assert_eq!(
            config.cache.ram_cache_flush_interval,
            CacheConfig::default().ram_cache_flush_interval
        );
        // `ram_cache_flush_threshold` and `ram_cache_flush_on_eviction` were removed, so there
        // is nothing left to assert here. That the example no longer SETS them is asserted by
        // `the_example_config_does_not_carry_the_removed_ram_cache_flush_keys`, which reads the
        // document rather than the parsed struct — the only place the keys are still visible.
        // `ram_cache_verification_interval` is not set in the example config, so
        // this asserts the serde default is applied.
        assert_eq!(
            config.cache.ram_cache_verification_interval,
            default_ram_cache_verification_interval()
        );
        assert_eq!(
            config.cache.incomplete_upload_ttl,
            Duration::from_secs(86_400)
        );
        assert_eq!(config.cache.range_merge_gap_threshold, 1_048_576);
        assert_eq!(config.cache.eviction_trigger_percent, 95);
        assert_eq!(config.cache.eviction_target_percent, 80);
        assert!(config.cache.cache_bypass_headers_enabled);
        assert_eq!(config.cache.full_object_check_threshold, 67_108_864);
        assert!(config.cache.read_cache_enabled);
        assert!(config.cache.evaluate_conditions_from_cache);
        assert_eq!(config.cache.disk_streaming_threshold, 1_048_576);
        assert_eq!(config.cache.compression_batch_size, 1_048_576);

        // shared_storage sub-section
        assert_eq!(
            config.cache.shared_storage.lock_acquire_wait_budget,
            Duration::from_secs(5)
        );
        assert_eq!(
            config.cache.shared_storage.eviction_lock_timeout,
            Duration::from_secs(60)
        );
        assert_eq!(
            config.cache.shared_storage.consolidation_interval,
            Duration::from_secs(5)
        );
        assert_eq!(
            config.cache.shared_storage.consolidation_size_threshold,
            1_048_576
        );
        assert_eq!(config.cache.shared_storage.lock_max_retries, 5);
        assert_eq!(
            config.cache.shared_storage.validation_frequency,
            Duration::from_secs(23 * 3600)
        );
        assert!(config.cache.shared_storage.orphan_recovery_enabled);
        assert_eq!(
            config.cache.shared_storage.orphan_recovery_interval,
            Duration::from_secs(300)
        );
        assert_eq!(
            config.cache.shared_storage.orphan_scan_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(config.cache.shared_storage.orphan_max_per_cycle, 100);
        assert_eq!(
            config.cache.shared_storage.validation_max_duration,
            Duration::from_secs(4 * 3600)
        );
        assert_eq!(
            config.cache.shared_storage.consolidation_cycle_timeout,
            Duration::from_secs(30)
        );

        // download_coordination sub-section
        assert!(config.cache.download_coordination.enabled);
        assert_eq!(config.cache.download_coordination.wait_timeout_secs, 30);

        // initialization sub-section
        assert!(config.cache.initialization.parallel_scan);
        assert_eq!(
            config.cache.initialization.scan_timeout,
            Duration::from_secs(30)
        );
        assert!(config.cache.initialization.progress_logging);

        // metadata_cache sub-section
        assert!(config.cache.metadata_cache.enabled);
        assert_eq!(
            config.cache.metadata_cache.refresh_interval,
            Duration::from_secs(5)
        );
        assert_eq!(config.cache.metadata_cache.max_entries, 100_000);

        // --- logging section ---
        assert_eq!(
            config.logging.access_log_dir.to_str().unwrap(),
            "./tmp/logs/access"
        );
        assert_eq!(
            config.logging.app_log_dir.to_str().unwrap(),
            "./tmp/logs/app"
        );
        assert!(config.logging.access_log_enabled);
        assert_eq!(config.logging.log_level, "info");
        assert_eq!(
            config.logging.access_log_flush_interval,
            Duration::from_secs(5)
        );
        assert_eq!(config.logging.access_log_buffer_size, 1000);
        assert_eq!(config.logging.access_log_retention_days, 30);
        assert_eq!(config.logging.app_log_retention_days, 30);

        // --- connection_pool section ---
        assert_eq!(
            config.connection_pool.dns_refresh_interval,
            Duration::from_secs(60)
        );
        assert_eq!(
            config.connection_pool.connection_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(config.connection_pool.idle_timeout, Duration::from_secs(55));
        assert!(config.connection_pool.keepalive_enabled);
        assert_eq!(config.connection_pool.max_idle_per_host, 100);
        assert_eq!(
            config.connection_pool.max_lifetime,
            Duration::from_secs(300)
        );
        assert_eq!(
            config.connection_pool.pool_check_interval,
            Duration::from_secs(10)
        );
        assert!(config.connection_pool.ip_distribution_enabled);
        assert_eq!(config.connection_pool.max_idle_per_ip, 10);
        assert_eq!(config.connection_pool.keepalive_idle_secs, 15);
        assert_eq!(config.connection_pool.keepalive_interval_secs, 5);
        assert_eq!(config.connection_pool.keepalive_retries, 3);
        assert_eq!(config.connection_pool.tcp_recv_buffer_size, None);
        assert_eq!(config.connection_pool.ip_failure_threshold, 3);

        // --- compression section ---
        assert!(config.compression.enabled);
        // The example now carries the built-in default rather than diverging from it.
        assert_eq!(config.compression.threshold, 1024);
        assert_eq!(
            config.compression.threshold,
            CompressionConfig::default().threshold
        );
        assert_eq!(
            config.compression.preferred_algorithm,
            CompressionAlgorithm::Lz4
        );
        // `content_aware` has been removed from config.example.yaml (it never
        // had any effect); the deprecation-alias field is None here since the
        // example no longer sets it. Separate tests cover the alias parsing
        // an old-style config that still sets `content_aware`.
        assert_eq!(config.compression.content_aware_deprecated, None);

        // --- health section ---
        assert!(config.health.enabled);
        assert_eq!(config.health.endpoint, "/health");
        assert_eq!(config.health.port, 8080);
        assert_eq!(config.health.check_interval, Duration::from_secs(30));

        // --- metrics section ---
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.endpoint, "/metrics");
        assert_eq!(config.metrics.port, 9090);
        assert_eq!(config.metrics.collection_interval, Duration::from_secs(60));
        assert!(!config.metrics.otlp.enabled);
        assert_eq!(config.metrics.otlp.endpoint, "http://localhost:4318");
        assert_eq!(config.metrics.otlp.export_interval, Duration::from_secs(60));
        assert_eq!(config.metrics.otlp.timeout, Duration::from_secs(10));

        // --- dashboard section ---
        assert!(config.dashboard.enabled);
        assert_eq!(config.dashboard.port, 8081);
        assert_eq!(config.dashboard.bind_address, "0.0.0.0");
        assert_eq!(
            config.dashboard.cache_stats_refresh_interval,
            Duration::from_secs(5)
        );
        assert_eq!(
            config.dashboard.logs_refresh_interval,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn otlp_config_debug_redacts_header_values() {
        let mut headers = std::collections::HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            "Bearer super-secret-token".to_string(),
        );
        headers.insert("X-Api-Key".to_string(), "ak_1234567890abcdef".to_string());

        let cfg = OtlpConfig {
            enabled: true,
            endpoint: "http://collector:4318".to_string(),
            export_interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            headers,
            compression: OtlpCompression::None,
            per_bucket_enabled: false,
        };

        let debug_output = format!("{:?}", cfg);

        // Must NOT contain actual header values
        assert!(
            !debug_output.contains("super-secret-token"),
            "Debug output must not contain header values, got: {}",
            debug_output
        );
        assert!(
            !debug_output.contains("ak_1234567890abcdef"),
            "Debug output must not contain header values, got: {}",
            debug_output
        );

        // Must contain the redaction placeholder with correct count
        assert!(
            debug_output.contains("<2 redacted>"),
            "Debug output must contain '<2 redacted>' placeholder, got: {}",
            debug_output
        );

        // Other fields should still be visible
        assert!(debug_output.contains("http://collector:4318"));
        assert!(debug_output.contains("enabled: true"));
    }

    #[test]
    fn test_connection_pool_config_without_max_registered_endpoints_uses_default() {
        // Config-compat: minimal config without max_registered_endpoints still parses
        let yaml = r#"
connection_pool:
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "55s"
"#;
        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse connection_pool config");
        assert_eq!(config.max_registered_endpoints, 10_000);
    }

    #[test]
    fn test_connection_pool_config_upstream_timeout_defaults_when_omitted() {
        // Config-compat: upstream timeout fields omitted → defaults apply
        let yaml = r#"
connection_pool:
  dns_refresh_interval: "60s"
  connection_timeout: "10s"
  idle_timeout: "55s"
"#;
        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse connection_pool config");
        assert_eq!(config.upstream_first_byte_timeout, Duration::from_secs(5));
        assert_eq!(config.upstream_idle_timeout, Duration::from_secs(5));
        assert_eq!(config.upstream_idle_retries, 2);
    }

    #[test]
    fn test_connection_pool_config_upstream_timeout_explicit_values() {
        // Explicit override of upstream timeout fields parses correctly
        let yaml = r#"
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "55s"
upstream_first_byte_timeout: "10s"
upstream_idle_timeout: "3s"
upstream_idle_retries: 5
"#;
        let config: ConnectionPoolConfig =
            serde_yaml_ng::from_str(yaml).expect("Failed to parse connection_pool config");
        assert_eq!(config.upstream_first_byte_timeout, Duration::from_secs(10));
        assert_eq!(config.upstream_idle_timeout, Duration::from_secs(3));
        assert_eq!(config.upstream_idle_retries, 5);
    }

    #[test]
    fn server_inflight_ceiling_uses_the_buffered_body_bound() {
        // Validates: Requirements 4.4, 4.5, 4.6, 6.4
        let mut server = ServerConfig {
            max_inflight_buffer_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        assert!(
            server.validate().is_ok(),
            "a 256 MiB ceiling must be legal without changing the deprecated upload-size field"
        );

        server.max_inflight_buffer_bytes = crate::signed_request_proxy::BUFFERED_BODY_BOUND - 1;
        let error = server
            .validate()
            .expect_err("a non-zero ceiling below the buffered-body bound must be rejected");
        assert!(error.contains("max_inflight_buffer_bytes"));
        assert!(error.contains(&crate::signed_request_proxy::BUFFERED_BODY_BOUND.to_string()));
        assert!(error.contains("Set max_inflight_buffer_bytes to 0"));

        server.max_inflight_buffer_bytes = 0;
        assert!(
            server.validate().is_ok(),
            "the disabled ledger remains exempt from the validation floor"
        );
    }

    #[test]
    fn minimal_config_keeps_the_inflight_ledger_disabled() {
        // Validates: Requirements 4.6, 6.4
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).expect("minimal config must parse");

        assert_eq!(config.server.max_inflight_buffer_bytes, 0);
        assert!(
            config.server.validate().is_ok(),
            "an omitted ceiling must retain the disabled default and validate"
        );
    }

    /// THE UPGRADE-CONTRACT ASSERTION for the two removed switches (Ed 2026-10-08). A
    /// config naming `orphan_reclaim_enabled` or `eviction_worklist_enabled`, at `true` or
    /// `false`, alone or together, must load from a real file through `load_from_file`, validate,
    /// keep reading the sibling written after it, and log exactly one warning per key naming it.
    /// The default behaviour is pinned by the struct no longer carrying either field (this test
    /// does not compile against a struct that does) and by
    /// `removed_switches_are_read_by_nothing`.
    #[test]
    fn config_naming_a_removed_switch_loads_runs_the_default_and_warns_once() {
        use super::tests::{capture_warnings, yaml_with_orphan_recovery_interval};
        use std::io::Write;
        let base = yaml_with_orphan_recovery_interval("60s");
        let anchor = "  shared_storage:\n";
        assert_eq!(base.matches(anchor).count(), 1, "fixture anchor");
        let cases: [&[(&str, &str)]; 5] = [
            &[("orphan_reclaim_enabled", "true")],
            &[("orphan_reclaim_enabled", "false")],
            &[("eviction_worklist_enabled", "true")],
            &[("eviction_worklist_enabled", "false")],
            &[
                ("orphan_reclaim_enabled", "false"),
                ("eviction_worklist_enabled", "true"),
            ],
        ];
        for case in cases {
            let mut inserted = String::from(anchor);
            for (key, value) in case {
                inserted.push_str(&format!("    {key}: {value}\n"));
            }
            inserted.push_str("    eviction_worklist_lease: \"30m\"\n");
            let yaml = base.replacen(anchor, &inserted, 1);
            let mut temp_file = tempfile::NamedTempFile::new().unwrap();
            temp_file.write_all(yaml.as_bytes()).unwrap();
            temp_file.flush().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string();
            let mut loaded = None;
            let logged = capture_warnings(|| {
                loaded = Some(Config::load_from_file(&path).unwrap_or_else(|e| {
                    panic!("a config naming a removed switch must still load: {case:?}: {e}")
                }));
            });
            let config = loaded.expect("load_from_file ran inside the capture");
            let shared = &config.cache.shared_storage;
            shared.validate().expect("the config must validate");
            assert_eq!(
                shared.eviction_worklist_lease,
                Duration::from_secs(1800),
                "the sibling after the removed key must still be read: {case:?}"
            );
            for key in ["orphan_reclaim_enabled", "eviction_worklist_enabled"] {
                let named = case.iter().any(|(k, _)| *k == key);
                let needle = format!("'cache.shared_storage.{key}' has been removed");
                assert_eq!(
                    logged.matches(&needle).count(),
                    usize::from(named),
                    "exactly one warning for {key} when named, none otherwise: {case:?} \
                     logged={logged:?}"
                );
            }
        }
    }
    /// The removed switches have no reader: no production module the switches used to gate
    /// names either field, so a config carrying one cannot change behaviour. The needles are
    /// assembled with `concat!` so this file's own text does not match.
    #[test]
    fn removed_switches_are_read_by_nothing() {
        let sources = [
            ("cache.rs", include_str!("cache.rs")),
            (
                "journal_consolidator.rs",
                include_str!("journal_consolidator.rs"),
            ),
            ("eviction_worklist.rs", include_str!("eviction_worklist.rs")),
            ("orphan_reclaim.rs", include_str!("orphan_reclaim.rs")),
            ("main.rs", include_str!("main.rs")),
        ];
        for needle in [
            concat!(".orphan_reclaim", "_enabled"),
            concat!(".eviction_worklist", "_enabled"),
        ] {
            for (name, source) in sources {
                assert!(
                    !source.contains(needle),
                    "{name} reads the removed switch {needle:?}"
                );
            }
        }
    }

    /// Config-compat (DR-EVICT task 49): a config written before `eviction_worklist_lease`
    /// existed parses with a 900 s lease, both with no `shared_storage` section and with one
    /// naming only older fields. An explicit value parses, and a lease outside 60 s - 24 h is
    /// rejected by validation.
    #[test]
    fn config_without_the_eviction_worklist_lease_parses_with_its_default() {
        let minimal = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        let partial = r#"
cache:
  cache_dir: "./cache"
  shared_storage:
    orphan_recovery_enabled: true
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
        for yaml in [minimal, partial] {
            let config: Config = serde_yaml_ng::from_str(yaml).expect("config must parse");
            let shared = &config.cache.shared_storage;
            assert_eq!(shared.eviction_worklist_lease, Duration::from_secs(900));
            shared.validate().expect("the defaults must validate");
        }
        let explicit = partial.replace(
            "orphan_recovery_enabled: true",
            "eviction_worklist_lease: \"30m\"",
        );
        let config: Config = serde_yaml_ng::from_str(&explicit).expect("explicit field parses");
        assert_eq!(
            config.cache.shared_storage.eviction_worklist_lease,
            Duration::from_secs(1800)
        );
        assert_eq!(EVICTION_WORKLIST_LEASE_MIN_SECS, 760, "2 x (64 x 5 + 60)");
        for (secs, ok) in [
            (60, false),
            (759, false),
            (760, true),
            (900, true),
            (86_400, true),
            (86_401, false),
        ] {
            let shared = SharedStorageConfig {
                eviction_worklist_lease: Duration::from_secs(secs),
                ..Default::default()
            };
            assert_eq!(shared.validate().is_ok(), ok, "lease {secs}s");
        }
        let err = SharedStorageConfig {
            eviction_worklist_lease: Duration::from_secs(60),
            ..Default::default()
        }
        .validate()
        .unwrap_err();
        assert!(err.contains("760s"), "the error names the floor: {err}");
        // A config that sets only a larger lock wait budget still loads with the default lease.
        let budget_only = SharedStorageConfig {
            lock_acquire_wait_budget: Duration::from_secs(60),
            ..Default::default()
        };
        assert!(budget_only.validate().is_ok());
        assert_eq!(
            crate::eviction_worklist::executor_budget(&budget_only).lease_secs,
            eviction_worklist_lease_floor_secs(60),
            "executors raise the lease to the floor for the configured budget"
        );
        assert_eq!(
            crate::eviction_worklist::executor_budget(&SharedStorageConfig::default()).lease_secs,
            900
        );
    }
    // Config-compat: a PARTIALLY specified section must parse, with the fields the
    // operator did not write falling back to their documented defaults. `Config`
    // carries field-level `#[serde(default)]` on each section, so omitting a whole
    // section always worked; before `#[serde(default)]` was added to these four
    // structs, writing the section with a single field failed startup naming a field
    // the operator never wrote (e.g. "missing field `preferred_algorithm`").
    // See the config compatibility rule.

    #[test]
    fn partial_compression_section_fills_remaining_defaults() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
compression:
  enabled: true
"#;
        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("partial compression section must parse");

        assert!(config.compression.enabled);
        assert_eq!(config.compression.threshold, 1024);
        assert_eq!(
            config.compression.preferred_algorithm,
            CompressionAlgorithm::Lz4
        );
        assert!(config.compression.content_aware_deprecated.is_none());
    }

    #[test]
    fn partial_health_section_fills_remaining_defaults() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
health:
  port: 18080
"#;
        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("partial health section must parse");

        assert_eq!(config.health.port, 18080);
        assert!(config.health.enabled);
        assert_eq!(config.health.endpoint, "/health");
        assert_eq!(config.health.bind_address, "127.0.0.1");
        assert_eq!(config.health.check_interval, Duration::from_secs(30));
    }

    #[test]
    fn partial_metrics_section_fills_remaining_defaults() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
metrics:
  port: 19090
"#;
        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("partial metrics section must parse");

        assert_eq!(config.metrics.port, 19090);
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.endpoint, "/metrics");
        assert_eq!(config.metrics.bind_address, "127.0.0.1");
        assert_eq!(config.metrics.collection_interval, Duration::from_secs(60));
        assert_eq!(config.metrics.per_bucket.max_series, 100);
        // Nested OTLP section must also fall back wholesale.
        assert!(!config.metrics.otlp.enabled);
        assert_eq!(config.metrics.otlp.endpoint, "http://localhost:4318");
    }

    #[test]
    fn partial_otlp_section_fills_remaining_defaults() {
        let yaml = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
metrics:
  otlp:
    enabled: true
"#;
        let config: Config =
            serde_yaml_ng::from_str(yaml).expect("partial otlp section must parse");

        assert!(config.metrics.otlp.enabled);
        assert_eq!(config.metrics.otlp.endpoint, "http://localhost:4318");
        assert_eq!(config.metrics.otlp.export_interval, Duration::from_secs(60));
        assert_eq!(config.metrics.otlp.timeout, Duration::from_secs(10));
        assert!(config.metrics.otlp.headers.is_empty());
        assert_eq!(config.metrics.otlp.compression, OtlpCompression::None);
        assert!(!config.metrics.otlp.per_bucket_enabled);
        // The enclosing metrics section keeps its own defaults.
        assert_eq!(config.metrics.port, 9090);
    }

    #[test]
    fn metadata_cache_max_entries_default_matches_documented_value() {
        // The doc comment on MetadataCacheConfig::max_entries states 100000; pin it.
        assert_eq!(MetadataCacheConfig::default().max_entries, 100_000);
    }
}
