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
}

/// Health check manager
pub struct HealthManager {
    start_time: SystemTime,
    cache_manager: Option<Arc<CacheManager>>,
    connection_pool: Option<Arc<tokio::sync::Mutex<ConnectionPoolManager>>>,
    compression_handler: Option<Arc<CompressionHandler>>,
    last_health_check: Arc<RwLock<Option<SystemHealth>>>,
}

impl Default for HealthManager {
    fn default() -> Self {
        Self::new()
    }
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
        connection_pool: Arc<tokio::sync::Mutex<ConnectionPoolManager>>,
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
        };

        // Cache the result
        {
            let mut last_check = self.last_health_check.write().await;
            *last_check = Some(health.clone());
        }

        health
    }

    /// Check cache system health
    async fn check_cache_health(&self, cache_manager: &Arc<CacheManager>) -> ComponentHealth {
        let start_time = SystemTime::now();

        let stats = cache_manager.get_statistics();
        let response_time = start_time
            .elapsed()
            .unwrap_or(Duration::from_millis(0))
            .as_millis() as u64;

        // Consider cache unhealthy if it's using more than 95% of available space
        let usage_percent = (stats.read_cache_size + stats.write_cache_size) as f64
            / stats.total_cache_size as f64
            * 100.0;

        let status = if usage_percent > 95.0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        ComponentHealth {
            name: "cache".to_string(),
            status,
            message: Some(format!("Cache usage: {:.1}%", usage_percent)),
            last_check: start_time,
            response_time_ms: Some(response_time),
        }
    }
    /// Check connection pool health
    async fn check_connection_pool_health(
        &self,
        connection_pool: &Arc<tokio::sync::Mutex<ConnectionPoolManager>>,
    ) -> ComponentHealth {
        let start_time = SystemTime::now();

        match connection_pool.lock().await.get_health_metrics().await {
            Ok(metrics) => {
                let response_time = start_time
                    .elapsed()
                    .unwrap_or(Duration::from_millis(0))
                    .as_millis() as u64;

                // Consider connection pool degraded if success rate is below 90%
                let overall_success_rate = if metrics.is_empty() {
                    1.0
                } else {
                    metrics.iter().map(|m| m.success_rate).sum::<f32>() / metrics.len() as f32
                };

                let status = if overall_success_rate < 0.9 {
                    HealthStatus::Degraded
                } else if overall_success_rate < 0.5 {
                    HealthStatus::Unhealthy
                } else {
                    HealthStatus::Healthy
                };

                ComponentHealth {
                    name: "connection_pool".to_string(),
                    status,
                    message: Some(format!(
                        "Success rate: {:.1}%",
                        overall_success_rate * 100.0
                    )),
                    last_check: start_time,
                    response_time_ms: Some(response_time),
                }
            }
            Err(e) => ComponentHealth {
                name: "connection_pool".to_string(),
                status: HealthStatus::Unhealthy,
                message: Some(format!("Connection pool error: {}", e)),
                last_check: start_time,
                response_time_ms: None,
            },
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

        // Since compress_content_aware_with_metadata requires &mut self, we'll just check if compression is enabled
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

    /// Get cached health status (non-blocking)
    pub async fn get_cached_health(&self) -> Option<SystemHealth> {
        self.last_health_check.read().await.clone()
    }
}
