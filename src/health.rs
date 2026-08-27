//! Health Check Module
//!
//! Provides health check endpoints and system status monitoring.

use crate::cache::CacheManager;
use crate::compression::CompressionHandler;
use crate::connection_pool::ConnectionPoolManager;
use crate::{ProxyError, Result};
use hyper::{Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
// tracing macros used conditionally in debug builds

/// Health check status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Component health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub last_check: SystemTime,
    pub response_time_ms: Option<u64>,
}

/// Overall system health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: HealthStatus,
    pub timestamp: SystemTime,
    pub components: Vec<ComponentHealth>,
    pub uptime_seconds: u64,
    /// Per-IP connection distribution stats (present when IP distribution is enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_distribution: Option<crate::connection_pool::IpDistributionStats>,
}

/// Health check manager
pub struct HealthManager {
    start_time: SystemTime,
    cache_manager: Option<Arc<CacheManager>>,
    connection_pool: Option<Arc<tokio::sync::RwLock<ConnectionPoolManager>>>,
    compression_handler: Option<Arc<CompressionHandler>>,
    last_health_check: Arc<RwLock<Option<SystemHealth>>>,
}

impl Default for HealthManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache usage threshold, as a percentage of the configured limit, above which the
/// cache component reports [`HealthStatus::Degraded`].
const CACHE_USAGE_DEGRADED_PERCENT: f64 = 95.0;

/// Classify on-disk cache usage against the configured cache size limit.
///
/// Split out from [`HealthManager::check_cache_health`] so the arithmetic is testable
/// without constructing a `CacheManager`, and so the two edge cases have named
/// coverage: an unconfigured limit, and the division that used to produce `NaN`.
///
/// `limit_bytes` of 0 means no cache size limit is configured (`max_cache_size_limit`
/// defaults to 0, and `CacheManager` itself treats `max_size > 0` as the precondition
/// for every capacity comparison). There is no capacity to be a percentage of, so this
/// reports Healthy and says so, rather than dividing by zero.
fn evaluate_cache_usage(used_bytes: u64, limit_bytes: u64) -> (HealthStatus, String) {
    if limit_bytes == 0 {
        return (
            HealthStatus::Healthy,
            format!("Cache usage: {} bytes, no limit configured", used_bytes),
        );
    }

    let usage_percent = (used_bytes as f64 / limit_bytes as f64) * 100.0;

    let status = if usage_percent > CACHE_USAGE_DEGRADED_PERCENT {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };

    (status, format!("Cache usage: {:.1}%", usage_percent))
}

impl HealthManager {
    /// Create new health manager
    pub fn new() -> Self {
        Self {
            start_time: SystemTime::now(),
            cache_manager: None,
            connection_pool: None,
            compression_handler: None,
            last_health_check: Arc::new(RwLock::new(None)),
        }
    }
    /// Set cache manager reference
    pub fn set_cache_manager(&mut self, cache_manager: Arc<CacheManager>) {
        self.cache_manager = Some(cache_manager);
    }

    /// Set connection pool reference
    pub fn set_connection_pool(
        &mut self,
        connection_pool: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    ) {
        self.connection_pool = Some(connection_pool);
    }

    /// Set compression handler reference
    pub fn set_compression_handler(&mut self, compression_handler: Arc<CompressionHandler>) {
        self.compression_handler = Some(compression_handler);
    }

    /// Perform comprehensive health check
    pub async fn check_health(&self) -> SystemHealth {
        let start_time = SystemTime::now();
        let mut components = Vec::new();

        // Check cache health
        if let Some(cache_manager) = &self.cache_manager {
            components.push(self.check_cache_health(cache_manager).await);
        }

        // Check connection pool health
        if let Some(connection_pool) = &self.connection_pool {
            components.push(self.check_connection_pool_health(connection_pool).await);
        }

        // Collect IP distribution stats if any distributors are active
        let ip_distribution = if let Some(connection_pool) = &self.connection_pool {
            let pool = connection_pool.read().await;
            let stats = pool.get_ip_distribution_stats();
            if stats.endpoints.is_empty() {
                None
            } else {
                Some(stats)
            }
        } else {
            None
        };

        // Check compression handler health
        if let Some(compression_handler) = &self.compression_handler {
            components.push(self.check_compression_health(compression_handler).await);
        }

        // Determine overall status
        let overall_status = self.determine_overall_status(&components);

        let uptime = self
            .start_time
            .elapsed()
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        let health = SystemHealth {
            status: overall_status,
            timestamp: start_time,
            components,
            uptime_seconds: uptime,
            ip_distribution,
        };

        // Cache the result
        {
            let mut last_check = self.last_health_check.write().await;
            *last_check = Some(health.clone());
        }

        health
    }

    /// Check cache system health
    ///
    /// # Why this reads `get_cache_size_stats` rather than `get_statistics`
    ///
    /// This check was vacuous on every deployment until 2026-08-26. It divided by
    /// `get_statistics().total_cache_size`, and that is the *stored* copy of the
    /// statistics, whose `total_cache_size` was only ever written by
    /// `CacheStatistics::default()` (zero) and by a test-only setter (since removed
    /// along with this fix, having had no production caller). The figure is computed in
    /// `get_cache_size_stats`, on a local copy that is never written back — so in
    /// production the denominator was always 0, the ratio was always `NaN`,
    /// `NaN > 95.0` is false, and the component reported Healthy regardless of how
    /// full the cache was. Observed directly on all three verification-fleet proxies:
    /// `Cache usage: NaN%`.
    ///
    /// The arithmetic was also wrong independently of that. The numerator was
    /// `read_cache_size + write_cache_size` and the denominator the old three-way sum,
    /// so the numerator was a *subset* of the denominator: the ratio could not exceed
    /// 100% and could not express "95% of capacity" at all. Capacity is
    /// `max_cache_size_limit`, which is what every eviction and capacity decision in
    /// `CacheManager` already compares against, so this now uses the same pair —
    /// on-disk bytes over the configured limit.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.3
    async fn check_cache_health(&self, cache_manager: &Arc<CacheManager>) -> ComponentHealth {
        let start_time = SystemTime::now();

        let stats = cache_manager.get_cache_size_stats().await;

        let response_time = start_time
            .elapsed()
            .unwrap_or(Duration::from_millis(0))
            .as_millis() as u64;

        let (status, message) = match stats {
            // Compares the whole-cache figure against the configured limit — the same
            // pair every capacity and eviction decision in `CacheManager` uses. `sizes` is
            // always `Some` from `get_cache_size_stats`; `None` would mean the figures were
            // never computed, which is a distinct condition from an empty cache and is
            // reported as such rather than as 0%.
            Ok(stats) => match stats.sizes {
                Some(sizes) => {
                    let (usage_status, usage_message) =
                        evaluate_cache_usage(sizes.total_cache_size, stats.max_cache_size_limit);
                    // R4.4: a live Disk_Safety_Bound breach degrades regardless of the
                    // utilisation percentage, because the two can disagree. Free space on
                    // the volume is a fact about the filesystem, whereas
                    // `max_cache_size` is a configured intention — a volume smaller than
                    // configured, or shared with something else, runs out of space while
                    // utilisation still reads comfortably low. Reporting only the
                    // percentage would call that Healthy while write-through caching was
                    // being declined on every upload.
                    match crate::cache::disk_safety_recently_breached() {
                        Some(age_secs) => (
                            HealthStatus::Degraded,
                            format!(
                                "{}. Write-through caching is being declined: the disk safety \
                                 bound was breached {}s ago (cache volume out of space, or the \
                                 cache is at its configured maximum). Uploads still succeed; \
                                 they are not being cached.",
                                usage_message, age_secs
                            ),
                        ),
                        None => (usage_status, usage_message),
                    }
                }
                None => (
                    HealthStatus::Degraded,
                    "Cache usage unknown: cache size figures were not computed".to_string(),
                ),
            },
            // Report this rather than defaulting to Healthy. Failing to read the cache
            // size is itself a signal, and silently passing is what made the old
            // version of this check useless.
            Err(e) => (
                HealthStatus::Degraded,
                format!(
                    "Cache usage unknown: failed to read cache size stats: {}",
                    e
                ),
            ),
        };

        ComponentHealth {
            name: "cache".to_string(),
            status,
            message: Some(message),
            last_check: start_time,
            response_time_ms: Some(response_time),
        }
    }
    /// Check connection pool health
    async fn check_connection_pool_health(
        &self,
        connection_pool: &Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    ) -> ComponentHealth {
        let start_time = SystemTime::now();
        let pool = connection_pool.read().await;
        let stats = pool.get_ip_distribution_stats();
        let response_time = start_time
            .elapsed()
            .unwrap_or(Duration::from_millis(0))
            .as_millis() as u64;

        let total_ips: usize = stats
            .endpoints
            .iter()
            .map(|e| e.total_distributor_ips)
            .sum();

        // Healthy if IPs are populated, or if no endpoints have been registered yet
        // (normal at startup before first request). Only Degraded if an endpoint is
        // registered but has zero IPs — meaning DNS resolution failed for a known endpoint.
        let all_registered_empty = !stats.endpoints.is_empty()
            && stats.endpoints.iter().all(|e| e.total_distributor_ips == 0);

        let status = if all_registered_empty {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        ComponentHealth {
            name: "connection_pool".to_string(),
            status,
            message: Some(format!(
                "{} endpoints, {} total IPs",
                stats.endpoints.len(),
                total_ips
            )),
            last_check: start_time,
            response_time_ms: Some(response_time),
        }
    }

    /// Check compression handler health
    async fn check_compression_health(
        &self,
        compression_handler: &Arc<CompressionHandler>,
    ) -> ComponentHealth {
        let start_time = SystemTime::now();

        // Test compression with a small sample
        let _test_data = b"Hello, World! This is a test compression string.";

        // Since compress_with_metadata requires &mut self, we'll just check if compression is enabled
        let response_time = start_time
            .elapsed()
            .unwrap_or(Duration::from_millis(0))
            .as_millis() as u64;

        let status = if compression_handler.is_compression_enabled() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Degraded
        };

        ComponentHealth {
            name: "compression".to_string(),
            status,
            message: Some(format!(
                "Compression enabled: {}",
                compression_handler.is_compression_enabled()
            )),
            last_check: start_time,
            response_time_ms: Some(response_time),
        }
    }

    /// Determine overall system status from component statuses
    fn determine_overall_status(&self, components: &[ComponentHealth]) -> HealthStatus {
        if components.is_empty() {
            return HealthStatus::Healthy;
        }

        let unhealthy_count = components
            .iter()
            .filter(|c| c.status == HealthStatus::Unhealthy)
            .count();

        let degraded_count = components
            .iter()
            .filter(|c| c.status == HealthStatus::Degraded)
            .count();

        if unhealthy_count > 0 {
            HealthStatus::Unhealthy
        } else if degraded_count > 0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }

    /// Handle health check HTTP request
    pub async fn handle_health_request(
        &self,
        _req: Request<hyper::body::Incoming>,
    ) -> Result<Response<String>> {
        let health = self.check_health().await;

        let status_code = match health.status {
            HealthStatus::Healthy => StatusCode::OK,
            HealthStatus::Degraded => StatusCode::OK, // Still return 200 for degraded
            HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
        };

        let body = serde_json::to_string_pretty(&health).map_err(|e| {
            ProxyError::SerializationError(format!("Failed to serialize health status: {}", e))
        })?;

        Response::builder()
            .status(status_code)
            .header("Content-Type", "application/json")
            .body(body)
            .map_err(|e| ProxyError::HttpError(format!("Failed to build health response: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn component(status: HealthStatus) -> ComponentHealth {
        ComponentHealth {
            name: "test".to_string(),
            status,
            message: None,
            last_check: SystemTime::now(),
            response_time_ms: Some(0),
        }
    }

    #[test]
    fn test_determine_overall_status_empty_is_healthy() {
        let mgr = HealthManager::new();
        assert_eq!(mgr.determine_overall_status(&[]), HealthStatus::Healthy);
    }

    /// The regression that made this check useless for the life of the project.
    ///
    /// The old code divided by `get_statistics().total_cache_size`, which is never
    /// written in production and so was always 0. `0.0 / 0.0` is `NaN`, and
    /// `NaN > 95.0` is `false`, so the component reported Healthy no matter how full
    /// the cache was — observed as `Cache usage: NaN%` on all three verification-fleet
    /// proxies. This asserts the zero-limit case is handled explicitly instead.
    #[test]
    fn cache_usage_with_no_configured_limit_does_not_produce_nan() {
        let (status, message) = evaluate_cache_usage(20_767_307_686, 0);

        assert_eq!(status, HealthStatus::Healthy);
        assert!(
            !message.contains("NaN"),
            "message must not report NaN, got: {}",
            message
        );
        assert!(
            message.contains("no limit configured"),
            "message should say the limit is unset, got: {}",
            message
        );
    }

    /// Both figures zero is the genuinely empty case, and must also not be `NaN`.
    #[test]
    fn cache_usage_with_empty_cache_and_no_limit_does_not_produce_nan() {
        let (status, message) = evaluate_cache_usage(0, 0);

        assert_eq!(status, HealthStatus::Healthy);
        assert!(!message.contains("NaN"), "got: {}", message);
    }

    /// The live verification-fleet figures at the time of the fix: 20,767,307,686
    /// bytes on disk against a 100 GiB configured limit is 19.3%, which is the answer
    /// the old code should have given instead of `NaN`.
    #[test]
    fn cache_usage_reports_real_percentage_against_configured_limit() {
        let (status, message) = evaluate_cache_usage(20_767_307_686, 107_374_182_400);

        assert_eq!(status, HealthStatus::Healthy);
        assert_eq!(message, "Cache usage: 19.3%");
    }

    #[test]
    fn cache_usage_above_threshold_is_degraded() {
        // 96 GiB of a 100 GiB limit = 96%, over the 95% threshold.
        let (status, message) =
            evaluate_cache_usage(96 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);

        assert_eq!(status, HealthStatus::Degraded);
        assert_eq!(message, "Cache usage: 96.0%");
    }

    /// The threshold is exclusive, so exactly 95% stays Healthy.
    #[test]
    fn cache_usage_exactly_at_threshold_is_healthy() {
        let (status, message) = evaluate_cache_usage(95, 100);

        assert_eq!(status, HealthStatus::Healthy);
        assert_eq!(message, "Cache usage: 95.0%");
    }

    /// Structural check on the arithmetic, not just the threshold.
    ///
    /// The old numerator (`read + write`) was a subset of the old denominator
    /// (`read + write + ram`), so the ratio was mathematically incapable of exceeding
    /// 100% — it could not express "over capacity" at all, independently of the `NaN`
    /// bug. Usage over the configured limit must now be representable and Degraded.
    #[test]
    fn cache_usage_over_the_limit_exceeds_one_hundred_percent() {
        let (status, message) = evaluate_cache_usage(150, 100);

        assert_eq!(status, HealthStatus::Degraded);
        assert_eq!(message, "Cache usage: 150.0%");
    }

    #[test]
    fn test_determine_overall_status_all_healthy() {
        let mgr = HealthManager::new();
        let components = [
            component(HealthStatus::Healthy),
            component(HealthStatus::Healthy),
        ];
        assert_eq!(
            mgr.determine_overall_status(&components),
            HealthStatus::Healthy
        );
    }

    #[test]
    fn test_determine_overall_status_degraded_takes_precedence_over_healthy() {
        let mgr = HealthManager::new();
        let components = [
            component(HealthStatus::Healthy),
            component(HealthStatus::Degraded),
        ];
        assert_eq!(
            mgr.determine_overall_status(&components),
            HealthStatus::Degraded
        );
    }

    #[test]
    fn test_determine_overall_status_unhealthy_takes_precedence_over_degraded() {
        let mgr = HealthManager::new();
        let components = [
            component(HealthStatus::Degraded),
            component(HealthStatus::Unhealthy),
            component(HealthStatus::Healthy),
        ];
        assert_eq!(
            mgr.determine_overall_status(&components),
            HealthStatus::Unhealthy
        );
    }

    #[tokio::test]
    async fn test_check_health_no_components_is_healthy() {
        // A manager with no subsystems registered should report Healthy with
        // an empty component list (startup / minimal-config case).
        let mgr = HealthManager::new();
        let health = mgr.check_health().await;
        assert_eq!(health.status, HealthStatus::Healthy);
        assert!(health.components.is_empty());
        assert!(health.ip_distribution.is_none());
    }

    #[tokio::test]
    async fn test_check_health_result_is_cached() {
        let mgr = HealthManager::new();
        let _ = mgr.check_health().await;
        let cached = mgr.last_health_check.read().await;
        assert!(cached.is_some());
    }

    #[test]
    fn test_system_health_serializes_to_json() {
        let health = SystemHealth {
            status: HealthStatus::Healthy,
            timestamp: SystemTime::now(),
            components: vec![component(HealthStatus::Healthy)],
            uptime_seconds: 42,
            ip_distribution: None,
        };
        let json = serde_json::to_string(&health).unwrap();
        // ip_distribution is skipped when None
        assert!(!json.contains("ip_distribution"));
        assert!(json.contains("uptime_seconds"));
    }

    #[test]
    fn test_health_status_roundtrip() {
        for status in [
            HealthStatus::Healthy,
            HealthStatus::Degraded,
            HealthStatus::Unhealthy,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let back: HealthStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }
}
