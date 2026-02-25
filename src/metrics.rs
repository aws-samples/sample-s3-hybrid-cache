//! Metrics Collection Module
//!
//! Provides comprehensive metrics collection for cache, compression, and connection pools.

use crate::cache::CacheManager;
use crate::cache_size_tracker::{CacheSizeMetrics, CacheSizeTracker};
use crate::compression::CompressionHandler;
use crate::config::OtlpConfig;
use crate::connection_pool::ConnectionPoolManager;
use crate::otlp::OtlpExporter;
use crate::{ProxyError, Result};
use hyper::{Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Cache metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    pub total_cache_size: u64,
    pub read_cache_size: u64,
    pub write_cache_size: u64,
    pub ram_cache_size: u64,
    pub cache_hit_rate_percent: f32,
    pub ram_cache_hit_rate_percent: f32,
    pub total_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub corruption_metadata_total: u64,
    pub corruption_missing_range_total: u64,
    pub inconsistency_fixed_total: u64,
    pub partial_write_cleanup_total: u64,
    pub disk_full_events_total: u64,
    pub orphaned_files_cleaned_total: u64,
    pub lock_timeout_total: u64,
    // New range storage metrics
    pub metadata_parse_duration_ms: f64,
    pub metadata_file_size_bytes: u64,
    pub range_file_count: u64,
    pub range_load_duration_ms: f64,
    // Cache key format metrics
    pub old_cache_key_encounters: u64,
    // Cache write failure metrics
    pub cache_write_failures_total: u64,
    // Cache bypass metrics by reason
    pub cache_bypasses_by_reason: HashMap<String, u64>,
    // Cache coherency metrics
    pub cache_etag_validations_total: u64,
    pub cache_etag_mismatches_total: u64,
    pub cache_range_invalidations_total: u64,
    pub cache_orphaned_ranges_cleaned_total: u64,
    // Cache operation timing histograms
    pub cache_operation_duration_ms: f64,
    pub cache_cleanup_failures_total: u64,
    // Part caching metrics - Requirements 8.1, 8.2, 8.3, 8.4, 8.5
    pub cache_part_hits: u64,
    pub cache_part_misses: u64,
    pub cache_part_stores: u64,
    pub cache_part_evictions: u64,
    pub cache_part_errors: u64,
    // Write cache metrics - Requirement 11.4
    pub write_cache_hits: u64,
    pub incomplete_uploads_evicted: u64,
    // Per-tier RAM cache stats (from ram_cache.get_stats())
    pub ram_cache_hits: u64,
    pub ram_cache_misses: u64,
    pub ram_cache_evictions: u64,
    pub ram_cache_max_size: u64,
    // Per-tier metadata cache stats (from metadata_cache.metrics())
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub metadata_cache_entries: u64,
    pub metadata_cache_max_entries: u64,
    pub metadata_cache_evictions: u64,
    pub metadata_cache_stale_refreshes: u64,
    // Bytes served from cache (for S3 transfer saved)
    pub bytes_served_from_cache: u64,
}

/// Compression metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionMetrics {
    pub total_objects_compressed: u64,
    pub total_objects_uncompressed: u64,
    pub total_bytes_before: u64,
    pub total_bytes_after: u64,
    pub compression_failures: u64,
    pub decompression_failures: u64,
    pub average_compression_ratio: f32,
    pub compression_time_ms: u64,
}

/// Connection pool metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolMetrics {
    pub failed_connections: u64,
    pub average_latency_ms: u64,
    pub success_rate_percent: f32,
    pub dns_refresh_count: u64,
    pub ip_addresses: Vec<String>,
    // Connection keepalive metrics (tracked at Hyper level)
    pub connections_created: HashMap<String, u64>,
    pub connections_reused: HashMap<String, u64>,
    pub idle_timeout_closures: u64,
    pub max_lifetime_closures: u64,
    pub error_closures: u64,
}

/// Eviction coordination metrics
/// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionCoordinationMetrics {
    pub lock_acquisitions_successful: u64,
    pub lock_acquisitions_failed: u64,
    pub stale_locks_recovered: u64,
    pub total_lock_hold_time_ms: u64,
    pub evictions_coordinated: u64,
    pub evictions_skipped_lock_held: u64,
}

/// Signed PUT caching metrics
/// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedPutMetrics {
    pub cached_puts_total: u64,
    pub bypassed_puts_total: u64,
    pub cache_failures_total: u64,
    pub average_cached_bytes: u64,
    pub average_streaming_duration_ms: u64,
}

/// Atomic metadata writes metrics
/// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicMetadataMetrics {
    // Lock contention and timeout metrics (Requirement 9.5)
    pub lock_acquisitions_successful: u64,
    pub lock_acquisitions_failed: u64,
    pub lock_timeouts_total: u64,
    pub stale_locks_detected: u64,
    pub stale_locks_broken: u64,
    pub average_lock_hold_time_ms: f64,

    // Corruption detection metrics (Requirement 9.3)
    pub metadata_corruption_detected: u64,
    pub journal_corruption_detected: u64,
    pub range_file_corruption_detected: u64,

    // Recovery success metrics (Requirement 9.4)
    pub orphaned_ranges_detected: u64,
    pub orphaned_ranges_recovered: u64,
    pub orphaned_ranges_cleaned: u64,
    pub metadata_recovery_attempts: u64,
    pub metadata_recovery_successes: u64,
    pub journal_consolidation_attempts: u64,
    pub journal_consolidation_successes: u64,
    pub average_recovery_duration_ms: f64,

    // Journal operation metrics (Requirement 9.2)
    pub journal_entries_written: u64,
    pub journal_entries_consolidated: u64,
    pub journal_cleanup_operations: u64,
    pub journal_validation_failures: u64,

    // Hybrid writer metrics (Requirement 9.2)
    pub immediate_writes: u64,
    pub journal_only_writes: u64,
    pub hybrid_writes: u64,
    pub write_mode_fallbacks: u64,
}

/// Download coalescing metrics (serializable)
/// Requirements: 19.1, 19.2, 19.3, 19.4, 19.5
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoalescingMetrics {
    /// Total number of requests that waited for an in-flight fetch (Requirement 19.1)
    pub waits_total: u64,
    /// Total number of requests served from cache after waiting (Requirement 19.2)
    pub cache_hits_after_wait_total: u64,
    /// Total number of requests that timed out waiting and fell back to S3 (Requirement 19.3)
    pub timeouts_total: u64,
    /// Total number of S3 fetches saved by coalescing (Requirement 19.4)
    pub s3_fetches_saved_total: u64,
    /// Average wait duration in milliseconds (Requirement 19.5)
    pub average_wait_duration_ms: f64,
    /// Number of fetcher completions (successful)
    pub fetcher_completions_success: u64,
    /// Number of fetcher completions (error)
    pub fetcher_completions_error: u64,
}

/// System-wide metrics
/// Journal consolidation metrics
/// Requirements: 3.3, 3.4, 3.5 - Dashboard and metrics integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidationMetrics {
    /// Total cache size in bytes from consolidator
    pub cache_size_bytes: u64,
    /// Write cache size in bytes from consolidator
    pub cache_write_size_bytes: u64,
    /// Number of consolidation cycles completed
    pub consolidation_count: u64,
    /// Unix timestamp of last consolidation (seconds since epoch)
    pub last_consolidation_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub timestamp: SystemTime,
    pub uptime_seconds: u64,
    pub cache: Option<CacheMetrics>,
    pub compression: Option<CompressionMetrics>,
    pub connection_pool: Option<ConnectionPoolMetrics>,
    pub eviction_coordination: Option<EvictionCoordinationMetrics>,
    pub signed_put: Option<SignedPutMetrics>,
    pub cache_size: Option<CacheSizeMetrics>,
    pub atomic_metadata: Option<AtomicMetadataMetrics>,
    pub consolidation: Option<ConsolidationMetrics>,
    pub coalescing: Option<CoalescingMetrics>,
    pub request_metrics: RequestMetrics,
}

/// Request processing metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub average_response_time_ms: u64,
    pub requests_per_second: f32,
}

/// Internal atomic metadata writes statistics tracking
/// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
#[derive(Debug, Default, Clone)]
pub struct AtomicMetadataStats {
    // Lock contention and timeout metrics (Requirement 9.5)
    lock_acquisitions_successful: u64,
    lock_acquisitions_failed: u64,
    lock_timeouts_total: u64,
    stale_locks_detected: u64,
    stale_locks_broken: u64,
    lock_hold_times_ms: Vec<f64>,

    // Corruption detection metrics (Requirement 9.3)
    metadata_corruption_detected: u64,
    journal_corruption_detected: u64,
    range_file_corruption_detected: u64,

    // Recovery success metrics (Requirement 9.4)
    orphaned_ranges_detected: u64,
    orphaned_ranges_recovered: u64,
    orphaned_ranges_cleaned: u64,
    metadata_recovery_attempts: u64,
    metadata_recovery_successes: u64,
    journal_consolidation_attempts: u64,
    journal_consolidation_successes: u64,
    recovery_durations_ms: Vec<f64>,

    // Journal operation metrics (Requirement 9.2)
    journal_entries_written: u64,
    journal_entries_consolidated: u64,
    journal_cleanup_operations: u64,
    journal_validation_failures: u64,

    // Hybrid writer metrics (Requirement 9.2)
    immediate_writes: u64,
    journal_only_writes: u64,
    hybrid_writes: u64,
    write_mode_fallbacks: u64,
}

/// Per-bucket cache hit/miss counters.
/// Only tracked for buckets that have a `_settings.json` file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketCacheStats {
    pub hit_count: u64,
    pub miss_count: u64,
}

/// Metrics collector and manager
pub struct MetricsManager {
    start_time: SystemTime,
    cache_manager: Option<Arc<CacheManager>>,
    connection_pool: Option<Arc<tokio::sync::Mutex<ConnectionPoolManager>>>,
    compression_handler: Option<Arc<CompressionHandler>>,
    cache_size_tracker: Option<Arc<CacheSizeTracker>>,
    request_stats: Arc<RwLock<RequestStats>>,
    error_stats: Arc<RwLock<ErrorStats>>,
    range_storage_stats: Arc<RwLock<RangeStorageStats>>,
    eviction_coordination_stats: Arc<RwLock<EvictionCoordinationStats>>,
    signed_put_stats: Arc<RwLock<SignedPutStats>>,
    connection_keepalive_stats: Arc<RwLock<ConnectionKeepaliveStats>>,
    atomic_metadata_stats: Arc<RwLock<AtomicMetadataStats>>,
    coalescing_stats: Arc<RwLock<CoalescingStats>>,
    /// Per-bucket cache hit/miss counters (only for buckets with `_settings.json`)
    bucket_cache_stats: Arc<RwLock<HashMap<String, BucketCacheStats>>>,
    last_metrics: Arc<RwLock<Option<SystemMetrics>>>,
    otlp_exporter: Option<Arc<RwLock<OtlpExporter>>>,
}

/// Internal request statistics tracking
#[derive(Debug)]
struct RequestStats {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    total_response_time_ms: u64,
    last_reset: SystemTime,
}

impl Default for RequestStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            total_response_time_ms: 0,
            last_reset: SystemTime::now(),
        }
    }
}

/// Internal error statistics tracking for cache operations
#[derive(Debug, Default, Clone)]
pub struct ErrorStats {
    corruption_metadata_total: u64,
    corruption_missing_range_total: u64,
    inconsistency_fixed_total: u64,
    partial_write_cleanup_total: u64,
    disk_full_events_total: u64,
    orphaned_files_cleaned_total: u64,
    lock_timeout_total: u64,
    old_cache_key_encounters: u64,
    cache_write_failures_total: u64,
    cache_bypasses_by_reason: HashMap<String, u64>,
    // Cache coherency metrics
    cache_etag_validations_total: u64,
    cache_etag_mismatches_total: u64,
    cache_range_invalidations_total: u64,
    cache_orphaned_ranges_cleaned_total: u64,
    // Cache operation timing and error metrics
    cache_operation_durations: Vec<f64>,
    cache_cleanup_failures_total: u64,
    // Part caching metrics - Requirements 8.1, 8.2, 8.3, 8.4, 8.5
    cache_part_hits: u64,
    cache_part_misses: u64,
    cache_part_stores: u64,
    cache_part_evictions: u64,
    cache_part_errors: u64,
}

/// Internal range storage metrics tracking
#[derive(Debug, Clone, Default)]
pub struct RangeStorageStats {
    // Histogram data for metadata parse duration (in milliseconds)
    metadata_parse_durations: Vec<f64>,
    // Histogram data for range load duration (in milliseconds)
    range_load_durations: Vec<f64>,
    // Current metadata file size (updated on each metadata read)
    metadata_file_size_bytes: u64,
    // Current range file count (updated on each metadata read)
    range_file_count: u64,
}

/// Internal eviction coordination statistics tracking
/// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
#[derive(Debug, Default, Clone)]
pub struct EvictionCoordinationStats {
    /// Number of successful lock acquisitions (Requirement 7.1)
    pub lock_acquisitions_successful: u64,
    /// Number of failed lock acquisitions (Requirement 7.2)
    pub lock_acquisitions_failed: u64,
    /// Number of stale locks forcibly acquired (Requirement 7.3)
    pub stale_locks_recovered: u64,
    /// Total time spent holding the eviction lock in milliseconds (Requirement 7.4)
    pub total_lock_hold_time_ms: u64,
    /// Number of evictions performed while holding the lock (Requirement 7.5)
    pub evictions_coordinated: u64,
    /// Number of evictions skipped because another instance holds the lock (Requirement 7.5)
    pub evictions_skipped_lock_held: u64,
}

/// Internal signed PUT caching statistics tracking
/// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
#[derive(Debug, Clone, Default)]
pub struct SignedPutStats {
    cached_puts_total: u64,
    bypassed_puts_total: u64,
    cache_failures_total: u64,
    // Histogram data for cached bytes
    cached_bytes: Vec<u64>,
    // Histogram data for streaming duration (in milliseconds)
    streaming_durations: Vec<u64>,
}

/// Internal connection keepalive statistics tracking
/// Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
#[derive(Debug, Default, Clone)]
pub struct ConnectionKeepaliveStats {
    /// Total connections created per endpoint
    connections_created: HashMap<String, u64>,
    /// Total connection reuses per endpoint (calculated as total_requests - connections_created)
    connections_reused: HashMap<String, u64>,
    /// Total requests made per endpoint (used to calculate reuse)
    total_requests: HashMap<String, u64>,
    /// Connections closed due to idle timeout
    idle_timeout_closures: u64,
    /// Connections closed due to max lifetime
    max_lifetime_closures: u64,
    /// Connections closed due to errors
    error_closures: u64,
}

/// Download coalescing statistics tracking
/// Requirements: 19.1, 19.2, 19.3, 19.4, 19.5
#[derive(Debug, Default, Clone)]
pub struct CoalescingStats {
    /// Total number of requests that waited for an in-flight fetch (Requirement 19.1)
    pub waits_total: u64,
    /// Total number of requests served from cache after waiting (Requirement 19.2)
    pub cache_hits_after_wait_total: u64,
    /// Total number of requests that timed out waiting and fell back to S3 (Requirement 19.3)
    pub timeouts_total: u64,
    /// Total number of S3 fetches saved by coalescing (Requirement 19.4)
    pub s3_fetches_saved_total: u64,
    /// Sum of wait durations in milliseconds (for calculating average)
    pub wait_duration_sum_ms: u64,
    /// Count of wait duration samples
    pub wait_duration_count: u64,
    /// Number of fetcher completions (successful)
    pub fetcher_completions_success: u64,
    /// Number of fetcher completions (error)
    pub fetcher_completions_error: u64,
}

impl Default for MetricsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsManager {
    /// Create new metrics manager
    pub fn new() -> Self {
        Self {
            start_time: SystemTime::now(),
            cache_manager: None,
            connection_pool: None,
            compression_handler: None,
            cache_size_tracker: None,
            request_stats: Arc::new(RwLock::new(RequestStats {
                last_reset: SystemTime::now(),
                ..Default::default()
            })),
            error_stats: Arc::new(RwLock::new(ErrorStats::default())),
            range_storage_stats: Arc::new(RwLock::new(RangeStorageStats::default())),
            eviction_coordination_stats: Arc::new(
                RwLock::new(EvictionCoordinationStats::default()),
            ),
            signed_put_stats: Arc::new(RwLock::new(SignedPutStats::default())),
            connection_keepalive_stats: Arc::new(RwLock::new(ConnectionKeepaliveStats::default())),
            atomic_metadata_stats: Arc::new(RwLock::new(AtomicMetadataStats::default())),
            coalescing_stats: Arc::new(RwLock::new(CoalescingStats::default())),
            bucket_cache_stats: Arc::new(RwLock::new(HashMap::new())),
            last_metrics: Arc::new(RwLock::new(None)),
            otlp_exporter: None,
        }
    }

    /// Initialize OTLP exporter
    pub async fn initialize_otlp(&mut self, config: OtlpConfig) -> Result<()> {
        if config.enabled {
            let mut exporter = OtlpExporter::new(config);
            exporter.initialize().await?;
            self.otlp_exporter = Some(Arc::new(RwLock::new(exporter)));
            info!("OTLP metrics exporter initialized");
        }
        Ok(())
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

    /// Set cache size tracker reference
    pub fn set_cache_size_tracker(&mut self, cache_size_tracker: Arc<CacheSizeTracker>) {
        self.cache_size_tracker = Some(cache_size_tracker);
    }

    /// Record a request completion
    pub async fn record_request(&self, success: bool, response_time: Duration) {
        let mut stats = self.request_stats.write().await;
        stats.total_requests += 1;
        stats.total_response_time_ms += response_time.as_millis() as u64;

        if success {
            stats.successful_requests += 1;
        } else {
            stats.failed_requests += 1;
        }
    }

    /// Collect comprehensive system metrics
    pub async fn collect_metrics(&self) -> SystemMetrics {
        let uptime = self
            .start_time
            .elapsed()
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        // Collect cache metrics
        let cache_metrics = if let Some(cache_manager) = &self.cache_manager {
            self.collect_cache_metrics(cache_manager).await
        } else {
            None
        };

        // Collect compression metrics
        let compression_metrics = if let Some(compression_handler) = &self.compression_handler {
            self.collect_compression_metrics(compression_handler).await
        } else {
            None
        };

        // Collect connection pool metrics
        let connection_pool_metrics = if let Some(connection_pool) = &self.connection_pool {
            self.collect_connection_pool_metrics(connection_pool).await
        } else {
            None
        };

        // Collect eviction coordination metrics
        let eviction_coordination_metrics = self.collect_eviction_coordination_metrics().await;

        // Collect signed PUT metrics
        let signed_put_metrics = self.collect_signed_put_metrics().await;

        // Collect atomic metadata metrics
        let atomic_metadata_metrics = self.collect_atomic_metadata_metrics().await;

        // Collect cache size metrics
        let cache_size_metrics = if let Some(cache_size_tracker) = &self.cache_size_tracker {
            Some(cache_size_tracker.get_metrics().await)
        } else {
            None
        };

        // Collect consolidation metrics from journal consolidator
        // Requirements: 3.3, 3.4, 3.5 - Dashboard and metrics integration
        let consolidation_metrics = if let Some(cache_manager) = &self.cache_manager {
            self.collect_consolidation_metrics(cache_manager).await
        } else {
            None
        };

        // Collect coalescing metrics
        // Requirements: 19.1, 19.2, 19.3, 19.4, 19.5
        let coalescing_metrics = self.collect_coalescing_metrics().await;

        // Collect request metrics
        let request_metrics = self.collect_request_metrics().await;

        let metrics = SystemMetrics {
            timestamp: SystemTime::now(),
            uptime_seconds: uptime,
            cache: cache_metrics,
            compression: compression_metrics,
            connection_pool: connection_pool_metrics,
            eviction_coordination: eviction_coordination_metrics,
            signed_put: signed_put_metrics,
            atomic_metadata: atomic_metadata_metrics,
            cache_size: cache_size_metrics,
            consolidation: consolidation_metrics,
            coalescing: coalescing_metrics,
            request_metrics,
        };

        // Cache the result
        {
            let mut last_metrics = self.last_metrics.write().await;
            *last_metrics = Some(metrics.clone());
        }

        // Export to OTLP if enabled
        if let Some(otlp_exporter) = &self.otlp_exporter {
            if let Err(e) = otlp_exporter.read().await.export_metrics(&metrics).await {
                warn!("Failed to export metrics to OTLP: {}", e);
            }
        }

        metrics
    }

    /// Collect cache-specific metrics
    async fn collect_cache_metrics(
        &self,
        cache_manager: &Arc<CacheManager>,
    ) -> Option<CacheMetrics> {
        let stats = match cache_manager.get_cache_size_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                warn!(
                    "Failed to get cache size stats, falling back to basic stats: {}",
                    e
                );
                cache_manager.get_statistics()
            }
        };
        let total_requests = stats.cache_hits + stats.cache_misses;
        let hit_rate = if total_requests > 0 {
            stats.cache_hits as f32 / total_requests as f32 * 100.0
        } else {
            0.0
        };

        let error_stats = self.error_stats.read().await;
        let range_stats = self.range_storage_stats.read().await;

        // Per-tier stats: RAM cache and metadata cache (same source as dashboard)
        let ram_stats = cache_manager.get_ram_cache_stats();
        let metadata_cache = cache_manager.get_metadata_cache();
        let metadata_snap = metadata_cache.metrics();
        let metadata_entries = metadata_cache.len().await as u64;
        let metadata_max_entries = metadata_cache.config().max_entries as u64;

        // Calculate average metadata parse duration
        let avg_metadata_parse_duration = if !range_stats.metadata_parse_durations.is_empty() {
            range_stats.metadata_parse_durations.iter().sum::<f64>()
                / range_stats.metadata_parse_durations.len() as f64
        } else {
            0.0
        };

        // Calculate average range load duration
        let avg_range_load_duration = if !range_stats.range_load_durations.is_empty() {
            range_stats.range_load_durations.iter().sum::<f64>()
                / range_stats.range_load_durations.len() as f64
        } else {
            0.0
        };

        Some(CacheMetrics {
            total_cache_size: stats.total_cache_size,
            read_cache_size: stats.read_cache_size,
            write_cache_size: stats.write_cache_size,
            ram_cache_size: stats.ram_cache_size,
            cache_hit_rate_percent: hit_rate,
            ram_cache_hit_rate_percent: stats.ram_cache_hit_rate,
            total_requests,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            evictions: stats.evicted_entries,
            corruption_metadata_total: error_stats.corruption_metadata_total,
            corruption_missing_range_total: error_stats.corruption_missing_range_total,
            inconsistency_fixed_total: error_stats.inconsistency_fixed_total,
            partial_write_cleanup_total: error_stats.partial_write_cleanup_total,
            disk_full_events_total: error_stats.disk_full_events_total,
            orphaned_files_cleaned_total: error_stats.orphaned_files_cleaned_total,
            lock_timeout_total: error_stats.lock_timeout_total,
            // New range storage metrics
            metadata_parse_duration_ms: avg_metadata_parse_duration,
            metadata_file_size_bytes: range_stats.metadata_file_size_bytes,
            range_file_count: range_stats.range_file_count,
            range_load_duration_ms: avg_range_load_duration,
            // Cache key format metrics
            old_cache_key_encounters: error_stats.old_cache_key_encounters,
            // Cache write failure metrics
            cache_write_failures_total: error_stats.cache_write_failures_total,
            // Cache bypass metrics by reason
            cache_bypasses_by_reason: error_stats.cache_bypasses_by_reason.clone(),
            // Cache coherency metrics
            cache_etag_validations_total: error_stats.cache_etag_validations_total,
            cache_etag_mismatches_total: error_stats.cache_etag_mismatches_total,
            cache_range_invalidations_total: error_stats.cache_range_invalidations_total,
            cache_orphaned_ranges_cleaned_total: error_stats.cache_orphaned_ranges_cleaned_total,
            // Cache operation timing histogram
            cache_operation_duration_ms: if !error_stats.cache_operation_durations.is_empty() {
                error_stats.cache_operation_durations.iter().sum::<f64>()
                    / error_stats.cache_operation_durations.len() as f64
            } else {
                0.0
            },
            cache_cleanup_failures_total: error_stats.cache_cleanup_failures_total,
            // Part caching metrics - Requirements 8.1, 8.2, 8.3, 8.4, 8.5
            cache_part_hits: error_stats.cache_part_hits,
            cache_part_misses: error_stats.cache_part_misses,
            cache_part_stores: error_stats.cache_part_stores,
            cache_part_evictions: error_stats.cache_part_evictions,
            cache_part_errors: error_stats.cache_part_errors,
            // Write cache metrics - Requirement 11.4
            write_cache_hits: stats.write_cache_hits,
            incomplete_uploads_evicted: stats.incomplete_uploads_evicted,
            // Per-tier RAM cache stats
            ram_cache_hits: ram_stats.as_ref().map_or(0, |r| r.hit_count),
            ram_cache_misses: ram_stats.as_ref().map_or(0, |r| r.miss_count),
            ram_cache_evictions: ram_stats.as_ref().map_or(0, |r| r.eviction_count),
            ram_cache_max_size: ram_stats.as_ref().map_or(0, |r| r.max_size),
            // Per-tier metadata cache stats
            metadata_cache_hits: metadata_snap.hits,
            metadata_cache_misses: metadata_snap.misses,
            metadata_cache_entries: metadata_entries,
            metadata_cache_max_entries: metadata_max_entries,
            metadata_cache_evictions: metadata_snap.evictions,
            metadata_cache_stale_refreshes: metadata_snap.stale_refreshes,
            // Bytes served from cache
            bytes_served_from_cache: stats.bytes_served_from_cache,
        })
    }

    /// Collect compression-specific metrics
    async fn collect_compression_metrics(
        &self,
        compression_handler: &Arc<CompressionHandler>,
    ) -> Option<CompressionMetrics> {
        match compression_handler.get_compression_statistics().await {
            Ok(stats) => Some(CompressionMetrics {
                total_objects_compressed: stats.total_objects_compressed,
                total_objects_uncompressed: stats.total_objects_uncompressed,
                total_bytes_before: stats.total_bytes_before,
                total_bytes_after: stats.total_bytes_after,
                compression_failures: stats.compression_failures,
                decompression_failures: stats.decompression_failures,
                average_compression_ratio: stats.average_compression_ratio,
                compression_time_ms: stats.compression_time_ms,
            }),
            Err(e) => {
                warn!("Failed to collect compression metrics: {}", e);
                None
            }
        }
    }

    /// Collect connection pool metrics
    async fn collect_connection_pool_metrics(
        &self,
        connection_pool: &Arc<tokio::sync::Mutex<ConnectionPoolManager>>,
    ) -> Option<ConnectionPoolMetrics> {
        let pool_manager = connection_pool.lock().await;

        let dns_refresh_count = pool_manager.get_dns_refresh_count();

        // Get keepalive stats (tracked at Hyper level, not ConnectionPoolManager level)
        let keepalive_stats = self.connection_keepalive_stats.read().await;
        let connections_created = keepalive_stats.connections_created.clone();
        let connections_reused = keepalive_stats.connections_reused.clone();
        let idle_timeout_closures = keepalive_stats.idle_timeout_closures;
        let max_lifetime_closures = keepalive_stats.max_lifetime_closures;
        let error_closures = keepalive_stats.error_closures;
        drop(keepalive_stats);

        // Get health metrics for latency and success rate
        match pool_manager.get_health_metrics().await {
            Ok(health_metrics) => {
                let failed_connections: u64 =
                    health_metrics.iter().map(|m| m.failed_requests).sum();

                let average_latency_ms = if !health_metrics.is_empty() {
                    health_metrics
                        .iter()
                        .map(|m| m.average_latency.as_millis() as u64)
                        .sum::<u64>()
                        / health_metrics.len() as u64
                } else {
                    0
                };

                let success_rate = if !health_metrics.is_empty() {
                    health_metrics.iter().map(|m| m.success_rate).sum::<f32>()
                        / health_metrics.len() as f32
                } else {
                    1.0
                };

                let ip_addresses: Vec<String> = health_metrics
                    .iter()
                    .map(|m| m.ip_address.to_string())
                    .collect();

                Some(ConnectionPoolMetrics {
                    failed_connections,
                    average_latency_ms,
                    success_rate_percent: success_rate,
                    dns_refresh_count,
                    ip_addresses,
                    connections_created,
                    connections_reused,
                    idle_timeout_closures,
                    max_lifetime_closures,
                    error_closures,
                })
            }
            Err(e) => {
                warn!("Failed to collect connection pool health metrics: {}", e);
                // Still return keepalive metrics even if health metrics fail
                Some(ConnectionPoolMetrics {
                    failed_connections: 0,
                    average_latency_ms: 0,
                    success_rate_percent: 1.0,
                    dns_refresh_count,
                    ip_addresses: Vec::new(),
                    connections_created,
                    connections_reused,
                    idle_timeout_closures,
                    max_lifetime_closures,
                    error_closures,
                })
            }
        }
    }

    /// Collect request processing metrics
    async fn collect_request_metrics(&self) -> RequestMetrics {
        let stats = self.request_stats.read().await;

        let average_response_time_ms = if stats.total_requests > 0 {
            stats.total_response_time_ms / stats.total_requests
        } else {
            0
        };

        let elapsed_seconds = stats
            .last_reset
            .elapsed()
            .unwrap_or(Duration::from_secs(1))
            .as_secs_f32();

        let requests_per_second = if elapsed_seconds > 0.0 {
            stats.total_requests as f32 / elapsed_seconds
        } else {
            0.0
        };

        RequestMetrics {
            total_requests: stats.total_requests,
            successful_requests: stats.successful_requests,
            failed_requests: stats.failed_requests,
            average_response_time_ms,
            requests_per_second,
        }
    }

    /// Collect consolidation metrics from journal consolidator
    /// Requirements: 3.3, 3.4, 3.5 - Dashboard and metrics integration
    async fn collect_consolidation_metrics(
        &self,
        cache_manager: &Arc<CacheManager>,
    ) -> Option<ConsolidationMetrics> {
        // Get the journal consolidator from cache manager
        let consolidator = cache_manager.get_journal_consolidator().await?;

        // Get size state from consolidator
        let size_state = consolidator.get_size_state().await;

        // Convert last_consolidation SystemTime to Unix timestamp
        let last_consolidation_timestamp = size_state
            .last_consolidation
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Some(ConsolidationMetrics {
            cache_size_bytes: size_state.total_size,
            cache_write_size_bytes: size_state.write_cache_size,
            consolidation_count: size_state.consolidation_count,
            last_consolidation_timestamp,
        })
    }

    /// Collect coalescing metrics
    /// Requirements: 19.1, 19.2, 19.3, 19.4, 19.5
    async fn collect_coalescing_metrics(&self) -> Option<CoalescingMetrics> {
        let stats = self.coalescing_stats.read().await;

        // Calculate average wait duration
        let average_wait_duration_ms = if stats.wait_duration_count > 0 {
            stats.wait_duration_sum_ms as f64 / stats.wait_duration_count as f64
        } else {
            0.0
        };

        Some(CoalescingMetrics {
            waits_total: stats.waits_total,
            cache_hits_after_wait_total: stats.cache_hits_after_wait_total,
            timeouts_total: stats.timeouts_total,
            s3_fetches_saved_total: stats.s3_fetches_saved_total,
            average_wait_duration_ms,
            fetcher_completions_success: stats.fetcher_completions_success,
            fetcher_completions_error: stats.fetcher_completions_error,
        })
    }

    /// Handle metrics HTTP request
    pub async fn handle_metrics_request(
        &self,
        _req: Request<hyper::body::Incoming>,
    ) -> Result<Response<String>> {
        let metrics = self.collect_metrics().await;

        let body = serde_json::to_string_pretty(&metrics).map_err(|e| {
            ProxyError::SerializationError(format!("Failed to serialize metrics: {}", e))
        })?;

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(body)
            .map_err(|e| ProxyError::HttpError(format!("Failed to build metrics response: {}", e)))
    }

    /// Get cached metrics (non-blocking)
    pub async fn get_cached_metrics(&self) -> Option<SystemMetrics> {
        self.last_metrics.read().await.clone()
    }

    /// Reset request statistics
    pub async fn reset_request_stats(&self) {
        let mut stats = self.request_stats.write().await;
        *stats = RequestStats {
            last_reset: SystemTime::now(),
            ..Default::default()
        };
    }

    /// Record a proxy request for OTLP export (similar to Mountpoint S3)
    pub async fn record_proxy_request(
        &self,
        request_type: &str,
        latency_us: u64,
        size_bytes: u64,
        success: bool,
    ) {
        if let Some(otlp_exporter) = &self.otlp_exporter {
            otlp_exporter.read().await.record_proxy_request(
                request_type,
                latency_us,
                size_bytes,
                success,
            );
        }
    }

    /// Record an S3 request for OTLP export (similar to Mountpoint S3)
    pub async fn record_s3_request(
        &self,
        request_type: &str,
        first_byte_latency_us: u64,
        total_latency_us: u64,
        success: bool,
        http_status: Option<u16>,
    ) {
        if let Some(otlp_exporter) = &self.otlp_exporter {
            otlp_exporter.read().await.record_s3_request(
                request_type,
                first_byte_latency_us,
                total_latency_us,
                success,
                http_status,
            );
        }
    }

    /// Shutdown OTLP exporter
    pub async fn shutdown_otlp(&mut self) -> Result<()> {
        if let Some(otlp_exporter) = &mut self.otlp_exporter {
            otlp_exporter.write().await.shutdown().await?;
        }
        Ok(())
    }

    /// Record corrupted metadata file event
    /// Requirement 8.1: Track metadata corruption events
    ///
    /// # Arguments
    /// * `cache_key` - The cache key with corrupted metadata
    /// * `file_path` - The path to the corrupted metadata file
    /// * `error_details` - Details about the corruption error
    pub async fn record_corrupted_metadata(
        &self,
        cache_key: &str,
        file_path: &str,
        error_details: &str,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.corruption_metadata_total += 1;

        warn!(
            cache_key = %cache_key,
            file_path = %file_path,
            error_details = %error_details,
            total_corruptions = stats.corruption_metadata_total,
            "Metadata corruption detected - invalidating cache entry"
        );
    }

    /// Record missing range binary file event
    /// Requirement 8.2: Track missing range file events
    pub async fn record_missing_range_file(&self) {
        let mut stats = self.error_stats.write().await;
        stats.corruption_missing_range_total += 1;
        debug!(
            "Recorded missing range file event (total: {})",
            stats.corruption_missing_range_total
        );
    }

    /// Record inconsistency fixed event
    /// Requirement 8.3: Track inconsistency recovery events
    pub async fn record_inconsistency_fixed(&self) {
        let mut stats = self.error_stats.write().await;
        stats.inconsistency_fixed_total += 1;
        debug!(
            "Recorded inconsistency fixed event (total: {})",
            stats.inconsistency_fixed_total
        );
    }

    /// Record partial write cleanup event
    /// Requirement 8.4: Track partial write cleanup events
    pub async fn record_partial_write_cleanup(&self) {
        let mut stats = self.error_stats.write().await;
        stats.partial_write_cleanup_total += 1;
        debug!(
            "Recorded partial write cleanup event (total: {})",
            stats.partial_write_cleanup_total
        );
    }

    /// Record disk full event
    /// Requirement 8.5: Track disk space exhaustion events
    pub async fn record_disk_full_event(&self) {
        let mut stats = self.error_stats.write().await;
        stats.disk_full_events_total += 1;
        warn!(
            "Recorded disk full event (total: {})",
            stats.disk_full_events_total
        );
    }

    /// Record orphaned files cleaned event
    /// Requirement 7.5: Track orphaned file cleanup events
    pub async fn record_orphaned_files_cleaned(&self, count: u64) {
        let mut stats = self.error_stats.write().await;
        stats.orphaned_files_cleaned_total += count;
        debug!(
            "Recorded {} orphaned files cleaned (total: {})",
            count, stats.orphaned_files_cleaned_total
        );
    }

    /// Record lock acquisition timeout event
    /// Requirement 4.4: Track lock timeout events for concurrent access safety
    pub async fn record_lock_timeout(&self) {
        let mut stats = self.error_stats.write().await;
        stats.lock_timeout_total += 1;
        warn!(
            "Recorded lock timeout event (total: {})",
            stats.lock_timeout_total
        );
    }

    /// Get error statistics
    pub async fn get_error_stats(&self) -> ErrorStats {
        self.error_stats.read().await.clone()
    }

    /// Get old cache key encounters count
    /// Requirement 7.4: Provide access to old cache key encounter metric
    pub async fn get_old_cache_key_encounters(&self) -> u64 {
        self.error_stats.read().await.old_cache_key_encounters
    }

    /// Record metadata parse duration
    /// Requirement 8.5: Track metadata parse performance
    ///
    /// This method records the time taken to parse a metadata file.
    /// The duration is stored in a histogram for later aggregation.
    ///
    /// # Arguments
    /// * `duration_ms` - The duration in milliseconds to parse the metadata
    pub async fn record_metadata_parse_duration(&self, duration_ms: f64) {
        let mut stats = self.range_storage_stats.write().await;
        stats.metadata_parse_durations.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.metadata_parse_durations.len() > 1000 {
            stats.metadata_parse_durations.remove(0);
        }

        debug!("Recorded metadata parse duration: {:.2}ms", duration_ms);
    }

    /// Record metadata file size
    /// Requirement 8.5: Track metadata file sizes
    ///
    /// This method records the size of a metadata file in bytes.
    /// This is a gauge metric that represents the current size.
    ///
    /// # Arguments
    /// * `size_bytes` - The size of the metadata file in bytes
    pub async fn record_metadata_file_size(&self, size_bytes: u64) {
        let mut stats = self.range_storage_stats.write().await;
        stats.metadata_file_size_bytes = size_bytes;
        debug!("Recorded metadata file size: {} bytes", size_bytes);
    }

    /// Record range file count
    /// Requirement 8.5: Track number of range files per object
    ///
    /// This method records the number of range files for a cache entry.
    /// This is a gauge metric that represents the current count.
    ///
    /// # Arguments
    /// * `count` - The number of range files
    pub async fn record_range_file_count(&self, count: u64) {
        let mut stats = self.range_storage_stats.write().await;
        stats.range_file_count = count;
        debug!("Recorded range file count: {}", count);
    }

    /// Record range load duration
    /// Requirement 8.5: Track range data load performance
    ///
    /// This method records the time taken to load range data from a binary file.
    /// The duration is stored in a histogram for later aggregation.
    ///
    /// # Arguments
    /// * `duration_ms` - The duration in milliseconds to load the range data
    pub async fn record_range_load_duration(&self, duration_ms: f64) {
        let mut stats = self.range_storage_stats.write().await;
        stats.range_load_durations.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.range_load_durations.len() > 1000 {
            stats.range_load_durations.remove(0);
        }

        debug!("Recorded range load duration: {:.2}ms", duration_ms);
    }

    /// Get range storage statistics
    pub async fn get_range_storage_stats(&self) -> RangeStorageStats {
        self.range_storage_stats.read().await.clone()
    }

    /// Record old cache key format encounter
    /// Requirement 7.4: Track encounters with old cache key format (host:path) for migration monitoring
    ///
    /// This method records when the system encounters a cache key in the old format
    /// (with hostname prefix like "s3.amazonaws.com:bucket/object.jpg"). The new format
    /// uses only the path ("bucket/object.jpg").
    /// (with hostname prefix). This is useful for monitoring during migration periods.
    ///
    /// # Arguments
    /// * None - increments the counter by 1
    pub async fn record_old_cache_key_encounter(&self) {
        let mut stats = self.error_stats.write().await;
        stats.old_cache_key_encounters += 1;
        debug!(
            "Recorded old cache key encounter (total: {})",
            stats.old_cache_key_encounters
        );
    }

    /// Record cache write failure event
    /// Requirement 1.4: Track cache write failures for monitoring and diagnostics
    ///
    /// This method records when a cache writer creation fails, allowing the system
    /// to continue with proxy-only mode while tracking the failure for monitoring.
    /// This is distinct from PUT-specific cache failures and covers all cache write
    /// operations including multipart uploads and regular writes.
    ///
    /// # Arguments
    /// * None - increments the counter by 1
    pub async fn record_cache_write_failure(&self) {
        let mut stats = self.error_stats.write().await;
        stats.cache_write_failures_total += 1;
        debug!(
            "Recorded cache write failure (total: {})",
            stats.cache_write_failures_total
        );
    }

    /// Collect eviction coordination metrics
    /// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
    async fn collect_eviction_coordination_metrics(&self) -> Option<EvictionCoordinationMetrics> {
        let stats = self.eviction_coordination_stats.read().await;

        Some(EvictionCoordinationMetrics {
            lock_acquisitions_successful: stats.lock_acquisitions_successful,
            lock_acquisitions_failed: stats.lock_acquisitions_failed,
            stale_locks_recovered: stats.stale_locks_recovered,
            total_lock_hold_time_ms: stats.total_lock_hold_time_ms,
            evictions_coordinated: stats.evictions_coordinated,
            evictions_skipped_lock_held: stats.evictions_skipped_lock_held,
        })
    }

    /// Record successful lock acquisition
    /// Requirement 7.1: Track successful lock acquisitions
    pub async fn record_lock_acquisition_successful(&self) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.lock_acquisitions_successful += 1;
        debug!(
            "Recorded successful lock acquisition (total: {})",
            stats.lock_acquisitions_successful
        );
    }

    /// Record failed lock acquisition (lock held by another instance)
    /// Requirement 7.2: Track failed lock acquisitions
    pub async fn record_lock_acquisition_failed(&self) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.lock_acquisitions_failed += 1;
        debug!(
            "Recorded failed lock acquisition (total: {})",
            stats.lock_acquisitions_failed
        );
    }

    /// Record stale lock recovery
    /// Requirement 7.3: Track stale locks forcibly acquired
    pub async fn record_stale_lock_recovered(&self) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.stale_locks_recovered += 1;
        debug!(
            "Recorded stale lock recovery (total: {})",
            stats.stale_locks_recovered
        );
    }

    /// Record lock hold time
    /// Requirement 7.4: Track total time spent holding the eviction lock
    pub async fn record_lock_hold_time(&self, duration_ms: u64) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.total_lock_hold_time_ms += duration_ms;
        debug!(
            "Recorded lock hold time: {}ms (total: {}ms)",
            duration_ms, stats.total_lock_hold_time_ms
        );
    }

    /// Record coordinated eviction
    /// Requirement 7.5: Track evictions performed while holding the lock
    pub async fn record_eviction_coordinated(&self) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.evictions_coordinated += 1;
        debug!(
            "Recorded coordinated eviction (total: {})",
            stats.evictions_coordinated
        );
    }

    /// Record eviction skipped due to lock being held
    /// Requirement 7.5: Track evictions skipped because another instance holds the lock
    pub async fn record_eviction_skipped_lock_held(&self) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.evictions_skipped_lock_held += 1;
        debug!(
            "Recorded eviction skipped (lock held) (total: {})",
            stats.evictions_skipped_lock_held
        );
    }

    /// Get eviction coordination statistics
    pub async fn get_eviction_coordination_stats(&self) -> EvictionCoordinationStats {
        self.eviction_coordination_stats.read().await.clone()
    }

    /// Collect signed PUT caching metrics
    /// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
    async fn collect_signed_put_metrics(&self) -> Option<SignedPutMetrics> {
        let stats = self.signed_put_stats.read().await;

        // Calculate average cached bytes
        let average_cached_bytes = if !stats.cached_bytes.is_empty() {
            stats.cached_bytes.iter().sum::<u64>() / stats.cached_bytes.len() as u64
        } else {
            0
        };

        // Calculate average streaming duration
        let average_streaming_duration_ms = if !stats.streaming_durations.is_empty() {
            stats.streaming_durations.iter().sum::<u64>() / stats.streaming_durations.len() as u64
        } else {
            0
        };

        Some(SignedPutMetrics {
            cached_puts_total: stats.cached_puts_total,
            bypassed_puts_total: stats.bypassed_puts_total,
            cache_failures_total: stats.cache_failures_total,
            average_cached_bytes,
            average_streaming_duration_ms,
        })
    }

    /// Record a cached PUT request
    /// Requirement 9.1: Track cached PUT operations with size and duration
    ///
    /// This method records when a signed PUT request is successfully cached.
    /// It increments the counter and stores the size and duration for histogram analysis.
    ///
    /// # Arguments
    /// * `bytes_cached` - The number of bytes cached
    /// * `duration_ms` - The streaming duration in milliseconds
    pub async fn record_cached_put(&self, bytes_cached: u64, duration_ms: u64) {
        let mut stats = self.signed_put_stats.write().await;
        stats.cached_puts_total += 1;
        stats.cached_bytes.push(bytes_cached);
        stats.streaming_durations.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.cached_bytes.len() > 1000 {
            stats.cached_bytes.remove(0);
        }
        if stats.streaming_durations.len() > 1000 {
            stats.streaming_durations.remove(0);
        }

        debug!(
            "Recorded cached PUT: bytes={}, duration={}ms (total: {})",
            bytes_cached, duration_ms, stats.cached_puts_total
        );
    }

    /// Record a bypassed PUT request
    /// Requirement 9.2: Track PUT requests that bypass cache
    ///
    /// This method records when a signed PUT request bypasses the cache
    /// due to capacity limits or other reasons.
    pub async fn record_bypassed_put(&self) {
        let mut stats = self.signed_put_stats.write().await;
        stats.bypassed_puts_total += 1;
        debug!(
            "Recorded bypassed PUT (total: {})",
            stats.bypassed_puts_total
        );
    }

    /// Record a cache failure for a PUT request
    /// Requirement 9.3: Track cache write failures
    ///
    /// This method records when a signed PUT request fails to cache
    /// due to errors during the caching process.
    pub async fn record_put_cache_failure(&self) {
        let mut stats = self.signed_put_stats.write().await;
        stats.cache_failures_total += 1;
        debug!(
            "Recorded PUT cache failure (total: {})",
            stats.cache_failures_total
        );
    }

    /// Get signed PUT statistics
    pub async fn get_signed_put_stats(&self) -> SignedPutStats {
        self.signed_put_stats.read().await.clone()
    }

    /// Record cache bypass with reason
    /// Requirements: 5.5, 13.4
    ///
    /// This method records when a request bypasses the cache, tracking the reason
    /// for the bypass. This is useful for monitoring and understanding cache behavior.
    ///
    /// # Arguments
    /// * `reason` - The reason for the cache bypass (e.g., "list_operation", "metadata_operation")
    pub async fn record_cache_bypass(&self, reason: &str) {
        let mut stats = self.error_stats.write().await;
        *stats
            .cache_bypasses_by_reason
            .entry(reason.to_string())
            .or_insert(0) += 1;
        debug!(
            "Recorded cache bypass: reason={} (total for this reason: {})",
            reason,
            stats.cache_bypasses_by_reason.get(reason).unwrap_or(&0)
        );
    }

    /// Get cache bypass count for a specific reason
    pub async fn get_cache_bypass_count(&self, reason: &str) -> u64 {
        let stats = self.error_stats.read().await;
        *stats.cache_bypasses_by_reason.get(reason).unwrap_or(&0)
    }

    /// Record ETag validation event
    /// Requirement 2.3: Track ETag validations performed for cache coherency
    ///
    /// This method records when an ETag validation is performed during range request handling.
    /// This helps monitor cache coherency operations and their frequency.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key being validated
    /// * `range_etag` - The ETag from the cached range
    /// * `object_etag` - The ETag from the cached object metadata
    /// * `validation_result` - Whether the validation passed or failed
    pub async fn record_etag_validation(
        &self,
        cache_key: &str,
        range_etag: &str,
        object_etag: &str,
        validation_result: bool,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_etag_validations_total += 1;

        if validation_result {
            info!(
                cache_key = %cache_key,
                range_etag = %range_etag,
                object_etag = %object_etag,
                result = "valid",
                total_validations = stats.cache_etag_validations_total,
                "ETag validation successful - serving from cache"
            );
        } else {
            warn!(
                cache_key = %cache_key,
                range_etag = %range_etag,
                object_etag = %object_etag,
                result = "invalid",
                total_validations = stats.cache_etag_validations_total,
                "ETag validation failed - cache invalidation required"
            );
        }
    }

    /// Record ETag mismatch event
    /// Requirement 2.3: Track ETag mismatches that trigger cache invalidation
    ///
    /// This method records when an ETag mismatch is detected between cached ranges
    /// and cached object metadata, indicating stale data that needs invalidation.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key with the mismatch
    /// * `range_etag` - The ETag from the cached range
    /// * `object_etag` - The ETag from the cached object metadata
    pub async fn record_etag_mismatch(&self, cache_key: &str, range_etag: &str, object_etag: &str) {
        let mut stats = self.error_stats.write().await;
        stats.cache_etag_mismatches_total += 1;

        warn!(
            cache_key = %cache_key,
            range_etag = %range_etag,
            object_etag = %object_etag,
            total_mismatches = stats.cache_etag_mismatches_total,
            "ETag mismatch detected - invalidating cached ranges"
        );
    }

    /// Record range invalidation event
    /// Requirement 2.3: Track range invalidations due to cache coherency issues
    ///
    /// This method records when cached ranges are invalidated due to ETag mismatches
    /// or full object caching operations.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key being invalidated
    /// * `affected_ranges` - List of range specifications that were invalidated
    /// * `reason` - The reason for invalidation (e.g., "etag_mismatch", "full_object_cached")
    pub async fn record_range_invalidation(
        &self,
        cache_key: &str,
        affected_ranges: &[String],
        reason: &str,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_range_invalidations_total += 1;

        warn!(
            cache_key = %cache_key,
            affected_ranges = ?affected_ranges,
            reason = %reason,
            range_count = affected_ranges.len(),
            total_invalidations = stats.cache_range_invalidations_total,
            "Cache ranges invalidated for coherency"
        );
    }

    /// Record orphaned ranges cleanup event
    /// Requirement 2.3: Track cleanup of orphaned range files
    ///
    /// This method records when orphaned range files (ranges without corresponding
    /// object metadata) are cleaned up during cache coherency operations.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for which orphaned ranges were cleaned
    /// * `orphaned_ranges` - List of orphaned range files that were cleaned
    pub async fn record_orphaned_ranges_cleaned(
        &self,
        cache_key: &str,
        orphaned_ranges: &[String],
    ) {
        let mut stats = self.error_stats.write().await;
        let count = orphaned_ranges.len() as u64;
        stats.cache_orphaned_ranges_cleaned_total += count;

        warn!(
            cache_key = %cache_key,
            orphaned_ranges = ?orphaned_ranges,
            count = count,
            total_cleaned = stats.cache_orphaned_ranges_cleaned_total,
            "Orphaned range files cleaned up"
        );
    }

    /// Record cache operation duration
    /// Requirement 4.2: Track timing histograms for cache operations
    ///
    /// This method records the duration of cache operations for performance monitoring.
    /// The duration is stored in a histogram for later aggregation.
    ///
    /// # Arguments
    /// * `operation_type` - The type of cache operation (e.g., "etag_validation", "range_invalidation")
    /// * `cache_key` - The cache key being operated on
    /// * `duration_ms` - The duration in milliseconds for the cache operation
    pub async fn record_cache_operation_duration(
        &self,
        operation_type: &str,
        cache_key: &str,
        duration_ms: f64,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_operation_durations.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.cache_operation_durations.len() > 1000 {
            stats.cache_operation_durations.remove(0);
        }

        if duration_ms > 100.0 {
            warn!(
                operation_type = %operation_type,
                cache_key = %cache_key,
                duration_ms = duration_ms,
                "Slow cache operation detected"
            );
        } else {
            debug!(
                operation_type = %operation_type,
                cache_key = %cache_key,
                duration_ms = duration_ms,
                "Cache operation completed"
            );
        }
    }

    /// Record cache cleanup failure
    /// Requirement 4.2: Track cache cleanup operation failures
    ///
    /// This method records when cache cleanup operations fail, allowing monitoring
    /// of cache maintenance issues.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for which cleanup failed
    /// * `operation_type` - The type of cleanup operation that failed
    /// * `error_details` - Details about the cleanup failure
    pub async fn record_cache_cleanup_failure(
        &self,
        cache_key: &str,
        operation_type: &str,
        error_details: &str,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_cleanup_failures_total += 1;

        warn!(
            cache_key = %cache_key,
            operation_type = %operation_type,
            error_details = %error_details,
            total_failures = stats.cache_cleanup_failures_total,
            "Cache cleanup operation failed"
        );
    }

    /// Backward compatibility method for simple ETag validation recording
    pub async fn record_etag_validation_simple(&self) {
        self.record_etag_validation("unknown", "unknown", "unknown", true)
            .await;
    }

    /// Backward compatibility method for simple ETag mismatch recording
    pub async fn record_etag_mismatch_simple(&self) {
        self.record_etag_mismatch("unknown", "unknown", "unknown")
            .await;
    }

    /// Backward compatibility method for simple range invalidation recording
    pub async fn record_range_invalidation_simple(&self) {
        self.record_range_invalidation("unknown", &[], "unknown")
            .await;
    }

    /// Backward compatibility method for simple orphaned ranges cleanup recording
    pub async fn record_orphaned_ranges_cleaned_simple(&self, count: u64) {
        let ranges: Vec<String> = (0..count).map(|i| format!("range_{}", i)).collect();
        self.record_orphaned_ranges_cleaned("unknown", &ranges)
            .await;
    }

    /// Backward compatibility method for simple corrupted metadata recording
    pub async fn record_corrupted_metadata_simple(&self) {
        self.record_corrupted_metadata("unknown", "unknown", "unknown")
            .await;
    }

    /// Backward compatibility method for simple cache operation duration recording
    pub async fn record_cache_operation_duration_simple(&self, duration_ms: f64) {
        self.record_cache_operation_duration("unknown", "unknown", duration_ms)
            .await;
    }

    /// Backward compatibility method for simple cache cleanup failure recording
    pub async fn record_cache_cleanup_failure_simple(&self) {
        self.record_cache_cleanup_failure("unknown", "unknown", "unknown")
            .await;
    }

    /// Record part cache hit event
    /// Requirement 8.1: Track part cache hits for monitoring
    ///
    /// This method records when a GetObjectPart request is served from cache.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `part_number` - The part number that was served from cache
    /// * `size_bytes` - The size of the cached part in bytes
    pub async fn record_part_cache_hit(&self, cache_key: &str, part_number: u32, size_bytes: u64) {
        let mut stats = self.error_stats.write().await;
        stats.cache_part_hits += 1;

        // Use debug level for part cache hit logging to reduce noise
        debug!(
            operation = "GET",
            cache_result = "HIT",
            cache_type = "part_cache",
            cache_key = %cache_key,
            part_number = part_number,
            size_bytes = size_bytes,
            total_hits = stats.cache_part_hits,
            "Cache operation completed"
        );
    }

    /// Record part cache miss event
    /// Requirement 8.1: Track part cache misses for monitoring
    ///
    /// This method records when a GetObjectPart request is not found in cache
    /// and needs to be fetched from S3.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `part_number` - The part number that was not found in cache
    pub async fn record_part_cache_miss(&self, cache_key: &str, part_number: u32) {
        let mut stats = self.error_stats.write().await;
        stats.cache_part_misses += 1;

        debug!(
            cache_key = %cache_key,
            part_number = part_number,
            total_misses = stats.cache_part_misses,
            "Part cache MISS - fetching from S3"
        );
    }

    /// Record part cache store event
    /// Requirement 8.2: Track part cache stores for monitoring
    ///
    /// This method records when a GetObjectPart response is stored in cache.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `part_number` - The part number that was stored
    /// * `size_bytes` - The size of the stored part in bytes
    pub async fn record_part_cache_store(
        &self,
        cache_key: &str,
        part_number: u32,
        size_bytes: u64,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_part_stores += 1;

        info!(
            cache_key = %cache_key,
            part_number = part_number,
            size_bytes = size_bytes,
            total_stores = stats.cache_part_stores,
            "Part cached - stored as range"
        );
    }

    /// Record part cache eviction event
    /// Requirement 8.4: Track part cache evictions for monitoring
    ///
    /// This method records when cached parts are evicted from cache.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `part_numbers` - The part numbers that were evicted
    /// * `total_size_bytes` - The total size of evicted parts in bytes
    pub async fn record_part_cache_eviction(
        &self,
        cache_key: &str,
        part_numbers: &[u32],
        total_size_bytes: u64,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_part_evictions += part_numbers.len() as u64;

        warn!(
            cache_key = %cache_key,
            part_numbers = ?part_numbers,
            total_size_bytes = total_size_bytes,
            parts_evicted = part_numbers.len(),
            total_evictions = stats.cache_part_evictions,
            "Parts evicted from cache"
        );
    }

    /// Record part cache error event
    /// Requirement 8.5: Track part cache errors for monitoring
    ///
    /// This method records when part caching operations encounter errors.
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `part_number` - The part number that encountered an error
    /// * `operation` - The operation that failed (e.g., "lookup", "store", "parse")
    /// * `error_details` - Details about the error
    pub async fn record_part_cache_error(
        &self,
        cache_key: &str,
        part_number: u32,
        operation: &str,
        error_details: &str,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.cache_part_errors += 1;

        warn!(
            cache_key = %cache_key,
            part_number = part_number,
            operation = %operation,
            error_details = %error_details,
            total_errors = stats.cache_part_errors,
            "Part cache operation error"
        );
    }

    /// Get part caching statistics
    /// Returns the current part caching metrics for monitoring
    pub async fn get_part_cache_stats(&self) -> (u64, u64, u64, u64, u64) {
        let stats = self.error_stats.read().await;
        (
            stats.cache_part_hits,
            stats.cache_part_misses,
            stats.cache_part_stores,
            stats.cache_part_evictions,
            stats.cache_part_errors,
        )
    }

    // =========================================================================
    // Download Coalescing Metrics
    // Requirements: 19.1, 19.2, 19.3, 19.4, 19.5
    // =========================================================================

    /// Record a coalescing wait event
    /// Requirement 19.1: Track requests that waited for an in-flight fetch
    pub async fn record_coalesce_wait(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.waits_total += 1;
        debug!(
            "Recorded coalesce wait (total: {})",
            stats.waits_total
        );
    }

    /// Record a cache hit after waiting for coalescing
    /// Requirement 19.2: Track requests served from cache after waiting
    pub async fn record_coalesce_cache_hit(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.cache_hits_after_wait_total += 1;
        stats.s3_fetches_saved_total += 1;
        debug!(
            "Recorded coalesce cache hit (total hits: {}, fetches saved: {})",
            stats.cache_hits_after_wait_total,
            stats.s3_fetches_saved_total
        );
    }

    /// Record a coalescing timeout event
    /// Requirement 19.3: Track requests that timed out waiting
    pub async fn record_coalesce_timeout(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.timeouts_total += 1;
        debug!(
            "Recorded coalesce timeout (total: {})",
            stats.timeouts_total
        );
    }

    /// Record an S3 fetch saved by coalescing
    /// Requirement 19.4: Track S3 fetches avoided
    pub async fn record_coalesce_s3_fetch_saved(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.s3_fetches_saved_total += 1;
        debug!(
            "Recorded S3 fetch saved (total: {})",
            stats.s3_fetches_saved_total
        );
    }

    /// Record wait duration for coalescing
    /// Requirement 19.5: Track wait duration histogram
    pub async fn record_coalesce_wait_duration(&self, duration: std::time::Duration) {
        let mut stats = self.coalescing_stats.write().await;
        let duration_ms = duration.as_millis() as u64;
        stats.wait_duration_sum_ms += duration_ms;
        stats.wait_duration_count += 1;
        debug!(
            "Recorded coalesce wait duration: {}ms (avg: {}ms)",
            duration_ms,
            if stats.wait_duration_count > 0 {
                stats.wait_duration_sum_ms / stats.wait_duration_count
            } else {
                0
            }
        );
    }

    /// Record fetcher completion (success)
    pub async fn record_coalesce_fetcher_success(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.fetcher_completions_success += 1;
    }

    /// Record fetcher completion (error)
    pub async fn record_coalesce_fetcher_error(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.fetcher_completions_error += 1;
    }

    /// Get coalescing statistics
    /// Returns the current coalescing metrics for monitoring
    pub async fn get_coalescing_stats(&self) -> CoalescingStats {
        let stats = self.coalescing_stats.read().await;
        stats.clone()
    }

    /// Record a per-bucket cache hit.
    /// Only call this for buckets that have a `_settings.json` file.
    /// Requirements: 11.1, 11.2
    pub async fn record_bucket_cache_hit(&self, bucket: &str) {
        let mut stats = self.bucket_cache_stats.write().await;
        let entry = stats.entry(bucket.to_string()).or_default();
        entry.hit_count += 1;
    }

    /// Record a per-bucket cache miss.
    /// Only call this for buckets that have a `_settings.json` file.
    /// Requirements: 11.1, 11.2
    pub async fn record_bucket_cache_miss(&self, bucket: &str) {
        let mut stats = self.bucket_cache_stats.write().await;
        let entry = stats.entry(bucket.to_string()).or_default();
        entry.miss_count += 1;
    }

    /// Get per-bucket cache stats for all tracked buckets.
    /// Returns a snapshot of hit/miss counts keyed by bucket name.
    pub async fn get_bucket_cache_stats(&self) -> HashMap<String, BucketCacheStats> {
        let stats = self.bucket_cache_stats.read().await;
        stats.clone()
    }

    /// Record connection creation
    /// Requirement 6.1: Track connections created per endpoint
    ///
    /// This method records when a new connection is created to an endpoint.
    ///
    /// # Arguments
    /// * `endpoint` - The endpoint for which the connection was created
    pub async fn record_connection_created(&self, endpoint: &str) {
        let mut stats = self.connection_keepalive_stats.write().await;
        *stats
            .connections_created
            .entry(endpoint.to_string())
            .or_insert(0) += 1;
        debug!(
            "Recorded connection created: endpoint={} (total: {})",
            endpoint,
            stats.connections_created.get(endpoint).unwrap_or(&0)
        );
    }

    /// Record a request to an endpoint
    /// Requirement 6.2: Track requests and calculate connection reuse
    ///
    /// This method records when a request is made to an endpoint.
    /// Connection reuse is calculated as: total_requests - connections_created
    ///
    /// # Arguments
    /// * `endpoint` - The endpoint for which the request was made
    pub async fn record_request_to_endpoint(&self, endpoint: &str) {
        let mut stats = self.connection_keepalive_stats.write().await;

        // Increment total requests for this endpoint
        *stats
            .total_requests
            .entry(endpoint.to_string())
            .or_insert(0) += 1;

        // Calculate connection reuse: total_requests - connections_created
        let total_requests = *stats.total_requests.get(endpoint).unwrap_or(&0);
        let connections_created = *stats.connections_created.get(endpoint).unwrap_or(&0);
        let reuse_count = total_requests.saturating_sub(connections_created);

        // Update reuse counter
        stats
            .connections_reused
            .insert(endpoint.to_string(), reuse_count);

        debug!(
            "Recorded request to endpoint: {} (total_requests: {}, connections_created: {}, reused: {})",
            endpoint, total_requests, connections_created, reuse_count
        );
    }

    /// Record connection reuse
    /// Requirement 6.2: Track connection reuses per endpoint
    ///
    /// This method records when an existing connection is reused for a request.
    ///
    /// # Arguments
    /// * `endpoint` - The endpoint for which the connection was reused
    pub async fn record_connection_reused(&self, endpoint: &str) {
        let mut stats = self.connection_keepalive_stats.write().await;
        *stats
            .connections_reused
            .entry(endpoint.to_string())
            .or_insert(0) += 1;
        debug!(
            "Recorded connection reused: endpoint={} (total: {})",
            endpoint,
            stats.connections_reused.get(endpoint).unwrap_or(&0)
        );
    }

    /// Record connection closed due to idle timeout
    /// Requirement 6.4: Track connections closed due to idle timeout
    ///
    /// This method records when a connection is closed because it exceeded the idle timeout.
    pub async fn record_idle_timeout_closure(&self) {
        let mut stats = self.connection_keepalive_stats.write().await;
        stats.idle_timeout_closures += 1;
        debug!(
            "Recorded idle timeout closure (total: {})",
            stats.idle_timeout_closures
        );
    }

    /// Record connection closed due to max lifetime
    /// Requirement 6.5: Track connections closed due to max lifetime
    ///
    /// This method records when a connection is closed because it exceeded the max lifetime.
    pub async fn record_max_lifetime_closure(&self) {
        let mut stats = self.connection_keepalive_stats.write().await;
        stats.max_lifetime_closures += 1;
        debug!(
            "Recorded max lifetime closure (total: {})",
            stats.max_lifetime_closures
        );
    }

    /// Record connection closed due to error
    /// Requirement 6.5: Track connections closed due to errors
    ///
    /// This method records when a connection is closed due to an error.
    pub async fn record_error_closure(&self) {
        let mut stats = self.connection_keepalive_stats.write().await;
        stats.error_closures += 1;
        debug!("Recorded error closure (total: {})", stats.error_closures);
    }

    /// Get connection keepalive statistics
    pub async fn get_connection_keepalive_stats(&self) -> ConnectionKeepaliveStats {
        self.connection_keepalive_stats.read().await.clone()
    }

    /// Collect atomic metadata metrics
    /// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
    async fn collect_atomic_metadata_metrics(&self) -> Option<AtomicMetadataMetrics> {
        let stats = self.atomic_metadata_stats.read().await;

        // Calculate average lock hold time
        let average_lock_hold_time_ms = if !stats.lock_hold_times_ms.is_empty() {
            stats.lock_hold_times_ms.iter().sum::<f64>() / stats.lock_hold_times_ms.len() as f64
        } else {
            0.0
        };

        // Calculate average recovery duration
        let average_recovery_duration_ms = if !stats.recovery_durations_ms.is_empty() {
            stats.recovery_durations_ms.iter().sum::<f64>()
                / stats.recovery_durations_ms.len() as f64
        } else {
            0.0
        };

        Some(AtomicMetadataMetrics {
            // Lock contention and timeout metrics (Requirement 9.5)
            lock_acquisitions_successful: stats.lock_acquisitions_successful,
            lock_acquisitions_failed: stats.lock_acquisitions_failed,
            lock_timeouts_total: stats.lock_timeouts_total,
            stale_locks_detected: stats.stale_locks_detected,
            stale_locks_broken: stats.stale_locks_broken,
            average_lock_hold_time_ms,

            // Corruption detection metrics (Requirement 9.3)
            metadata_corruption_detected: stats.metadata_corruption_detected,
            journal_corruption_detected: stats.journal_corruption_detected,
            range_file_corruption_detected: stats.range_file_corruption_detected,

            // Recovery success metrics (Requirement 9.4)
            orphaned_ranges_detected: stats.orphaned_ranges_detected,
            orphaned_ranges_recovered: stats.orphaned_ranges_recovered,
            orphaned_ranges_cleaned: stats.orphaned_ranges_cleaned,
            metadata_recovery_attempts: stats.metadata_recovery_attempts,
            metadata_recovery_successes: stats.metadata_recovery_successes,
            journal_consolidation_attempts: stats.journal_consolidation_attempts,
            journal_consolidation_successes: stats.journal_consolidation_successes,
            average_recovery_duration_ms,

            // Journal operation metrics (Requirement 9.2)
            journal_entries_written: stats.journal_entries_written,
            journal_entries_consolidated: stats.journal_entries_consolidated,
            journal_cleanup_operations: stats.journal_cleanup_operations,
            journal_validation_failures: stats.journal_validation_failures,

            // Hybrid writer metrics (Requirement 9.2)
            immediate_writes: stats.immediate_writes,
            journal_only_writes: stats.journal_only_writes,
            hybrid_writes: stats.hybrid_writes,
            write_mode_fallbacks: stats.write_mode_fallbacks,
        })
    }

    // ===== ATOMIC METADATA WRITES METRICS RECORDING METHODS =====

    /// Record successful lock acquisition
    /// Requirement 9.5: Track lock contention and timeout frequency
    pub async fn record_atomic_metadata_lock_acquisition_successful(&self, hold_time_ms: f64) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.lock_acquisitions_successful += 1;
        stats.lock_hold_times_ms.push(hold_time_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.lock_hold_times_ms.len() > 1000 {
            stats.lock_hold_times_ms.remove(0);
        }

        debug!(
            "Recorded successful atomic metadata lock acquisition: hold_time={}ms, total={}",
            hold_time_ms, stats.lock_acquisitions_successful
        );
    }

    /// Record failed lock acquisition
    /// Requirement 9.5: Track lock contention and timeout frequency
    pub async fn record_atomic_metadata_lock_acquisition_failed(&self) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.lock_acquisitions_failed += 1;

        debug!(
            "Recorded failed atomic metadata lock acquisition: total={}",
            stats.lock_acquisitions_failed
        );
    }

    /// Record lock timeout
    /// Requirement 9.5: Track lock timeout frequency and duration
    pub async fn record_atomic_metadata_lock_timeout(&self) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.lock_timeouts_total += 1;

        warn!(
            "Recorded atomic metadata lock timeout: total={}",
            stats.lock_timeouts_total
        );
    }

    /// Record stale lock detection
    /// Requirement 9.5: Track stale lock detection for monitoring
    pub async fn record_atomic_metadata_stale_lock_detected(&self) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.stale_locks_detected += 1;

        info!(
            "Recorded stale atomic metadata lock detected: total={}",
            stats.stale_locks_detected
        );
    }

    /// Record stale lock broken
    /// Requirement 9.5: Track stale lock recovery operations
    pub async fn record_atomic_metadata_stale_lock_broken(&self) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.stale_locks_broken += 1;

        info!(
            "Recorded stale atomic metadata lock broken: total={}",
            stats.stale_locks_broken
        );
    }

    /// Record metadata corruption detection
    /// Requirement 9.3: Track corruption detection and log diagnostic information
    pub async fn record_atomic_metadata_corruption_detected(
        &self,
        cache_key: &str,
        corruption_type: &str,
        details: &str,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.metadata_corruption_detected += 1;

        warn!(
            cache_key = %cache_key,
            corruption_type = %corruption_type,
            details = %details,
            total_corruptions = stats.metadata_corruption_detected,
            "Atomic metadata corruption detected"
        );
    }

    /// Record journal corruption detection
    /// Requirement 9.3: Track journal corruption detection
    pub async fn record_atomic_metadata_journal_corruption_detected(
        &self,
        cache_key: &str,
        journal_path: &str,
        details: &str,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_corruption_detected += 1;

        warn!(
            cache_key = %cache_key,
            journal_path = %journal_path,
            details = %details,
            total_corruptions = stats.journal_corruption_detected,
            "Atomic metadata journal corruption detected"
        );
    }

    /// Record range file corruption detection
    /// Requirement 9.3: Track range file corruption detection
    pub async fn record_atomic_metadata_range_file_corruption_detected(
        &self,
        cache_key: &str,
        range_file_path: &str,
        details: &str,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.range_file_corruption_detected += 1;

        warn!(
            cache_key = %cache_key,
            range_file_path = %range_file_path,
            details = %details,
            total_corruptions = stats.range_file_corruption_detected,
            "Atomic metadata range file corruption detected"
        );
    }

    /// Record orphaned range detection
    /// Requirement 9.4: Track recovery success rates
    pub async fn record_atomic_metadata_orphaned_range_detected(
        &self,
        cache_key: &str,
        range_count: u64,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.orphaned_ranges_detected += range_count;

        info!(
            cache_key = %cache_key,
            range_count = range_count,
            total_detected = stats.orphaned_ranges_detected,
            "Atomic metadata orphaned ranges detected"
        );
    }

    /// Record orphaned range recovery
    /// Requirement 9.4: Track recovery success rates and duration
    pub async fn record_atomic_metadata_orphaned_range_recovered(
        &self,
        cache_key: &str,
        range_count: u64,
        duration_ms: f64,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.orphaned_ranges_recovered += range_count;
        stats.recovery_durations_ms.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.recovery_durations_ms.len() > 1000 {
            stats.recovery_durations_ms.remove(0);
        }

        info!(
            cache_key = %cache_key,
            range_count = range_count,
            duration_ms = duration_ms,
            total_recovered = stats.orphaned_ranges_recovered,
            "Atomic metadata orphaned ranges recovered"
        );
    }

    /// Record orphaned range cleanup
    /// Requirement 9.4: Track cleanup operations
    pub async fn record_atomic_metadata_orphaned_range_cleaned(
        &self,
        cache_key: &str,
        range_count: u64,
        bytes_freed: u64,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.orphaned_ranges_cleaned += range_count;

        info!(
            cache_key = %cache_key,
            range_count = range_count,
            bytes_freed = bytes_freed,
            total_cleaned = stats.orphaned_ranges_cleaned,
            "Atomic metadata orphaned ranges cleaned"
        );
    }

    /// Record metadata recovery attempt
    /// Requirement 9.4: Track recovery success rates
    pub async fn record_atomic_metadata_recovery_attempt(&self, cache_key: &str) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.metadata_recovery_attempts += 1;

        debug!(
            cache_key = %cache_key,
            total_attempts = stats.metadata_recovery_attempts,
            "Atomic metadata recovery attempt started"
        );
    }

    /// Record metadata recovery success
    /// Requirement 9.4: Track recovery success rates and duration
    pub async fn record_atomic_metadata_recovery_success(&self, cache_key: &str, duration_ms: f64) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.metadata_recovery_successes += 1;
        stats.recovery_durations_ms.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.recovery_durations_ms.len() > 1000 {
            stats.recovery_durations_ms.remove(0);
        }

        info!(
            cache_key = %cache_key,
            duration_ms = duration_ms,
            total_successes = stats.metadata_recovery_successes,
            success_rate = (stats.metadata_recovery_successes as f64 / stats.metadata_recovery_attempts as f64) * 100.0,
            "Atomic metadata recovery completed successfully"
        );
    }

    /// Record journal consolidation attempt
    /// Requirement 9.4: Track consolidation success rates
    pub async fn record_atomic_metadata_journal_consolidation_attempt(&self, cache_key: &str) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_consolidation_attempts += 1;

        debug!(
            cache_key = %cache_key,
            total_attempts = stats.journal_consolidation_attempts,
            "Atomic metadata journal consolidation attempt started"
        );
    }

    /// Record journal consolidation success
    /// Requirement 9.4: Track consolidation success rates and duration
    pub async fn record_atomic_metadata_journal_consolidation_success(
        &self,
        cache_key: &str,
        entries_consolidated: u64,
        duration_ms: f64,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_consolidation_successes += 1;
        stats.journal_entries_consolidated += entries_consolidated;
        stats.recovery_durations_ms.push(duration_ms);

        // Keep only the last 1000 samples to prevent unbounded growth
        if stats.recovery_durations_ms.len() > 1000 {
            stats.recovery_durations_ms.remove(0);
        }

        info!(
            cache_key = %cache_key,
            entries_consolidated = entries_consolidated,
            duration_ms = duration_ms,
            total_successes = stats.journal_consolidation_successes,
            success_rate = (stats.journal_consolidation_successes as f64 / stats.journal_consolidation_attempts as f64) * 100.0,
            "Atomic metadata journal consolidation completed successfully"
        );
    }

    /// Record journal entry written
    /// Requirement 9.2: Track journal operations
    pub async fn record_atomic_metadata_journal_entry_written(
        &self,
        cache_key: &str,
        operation: &str,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_entries_written += 1;

        debug!(
            cache_key = %cache_key,
            operation = %operation,
            total_written = stats.journal_entries_written,
            "Atomic metadata journal entry written"
        );
    }

    /// Record journal cleanup operation
    /// Requirement 9.2: Track journal cleanup operations
    pub async fn record_atomic_metadata_journal_cleanup(
        &self,
        cache_key: &str,
        entries_removed: u64,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_cleanup_operations += 1;

        info!(
            cache_key = %cache_key,
            entries_removed = entries_removed,
            total_cleanups = stats.journal_cleanup_operations,
            "Atomic metadata journal cleanup completed"
        );
    }

    /// Record journal validation failure
    /// Requirement 9.2: Track journal validation failures
    pub async fn record_atomic_metadata_journal_validation_failure(
        &self,
        cache_key: &str,
        journal_path: &str,
        details: &str,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_validation_failures += 1;

        warn!(
            cache_key = %cache_key,
            journal_path = %journal_path,
            details = %details,
            total_failures = stats.journal_validation_failures,
            "Atomic metadata journal validation failure"
        );
    }

    /// Record immediate write operation
    /// Requirement 9.2: Track hybrid writer operations
    pub async fn record_atomic_metadata_immediate_write(&self, cache_key: &str) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.immediate_writes += 1;

        debug!(
            cache_key = %cache_key,
            total_immediate = stats.immediate_writes,
            "Atomic metadata immediate write completed"
        );
    }

    /// Record journal-only write operation
    /// Requirement 9.2: Track hybrid writer operations
    pub async fn record_atomic_metadata_journal_only_write(&self, cache_key: &str) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.journal_only_writes += 1;

        debug!(
            cache_key = %cache_key,
            total_journal_only = stats.journal_only_writes,
            "Atomic metadata journal-only write completed"
        );
    }

    /// Record hybrid write operation
    /// Requirement 9.2: Track hybrid writer operations
    pub async fn record_atomic_metadata_hybrid_write(&self, cache_key: &str) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.hybrid_writes += 1;

        debug!(
            cache_key = %cache_key,
            total_hybrid = stats.hybrid_writes,
            "Atomic metadata hybrid write completed"
        );
    }

    /// Record write mode fallback
    /// Requirement 9.2: Track write mode fallbacks due to contention
    pub async fn record_atomic_metadata_write_mode_fallback(
        &self,
        cache_key: &str,
        from_mode: &str,
        to_mode: &str,
        reason: &str,
    ) {
        let mut stats = self.atomic_metadata_stats.write().await;
        stats.write_mode_fallbacks += 1;

        warn!(
            cache_key = %cache_key,
            from_mode = %from_mode,
            to_mode = %to_mode,
            reason = %reason,
            total_fallbacks = stats.write_mode_fallbacks,
            "Atomic metadata write mode fallback occurred"
        );
    }

    /// Get atomic metadata statistics
    pub async fn get_atomic_metadata_stats(&self) -> AtomicMetadataStats {
        self.atomic_metadata_stats.read().await.clone()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_signed_put_metrics_recording() {
        let metrics_manager = MetricsManager::new();

        // Record a cached PUT
        metrics_manager.record_cached_put(1024, 100).await;

        // Record a bypassed PUT
        metrics_manager.record_bypassed_put().await;

        // Record a cache failure
        metrics_manager.record_put_cache_failure().await;

        // Collect metrics
        let metrics = metrics_manager.collect_metrics().await;

        // Verify signed PUT metrics
        assert!(metrics.signed_put.is_some());
        let signed_put_metrics = metrics.signed_put.unwrap();

        assert_eq!(signed_put_metrics.cached_puts_total, 1);
        assert_eq!(signed_put_metrics.bypassed_puts_total, 1);
        assert_eq!(signed_put_metrics.cache_failures_total, 1);
        assert_eq!(signed_put_metrics.average_cached_bytes, 1024);
        assert_eq!(signed_put_metrics.average_streaming_duration_ms, 100);
    }

    #[tokio::test]
    async fn test_signed_put_metrics_histogram() {
        let metrics_manager = MetricsManager::new();

        // Record multiple cached PUTs with different sizes and durations
        metrics_manager.record_cached_put(1000, 50).await;
        metrics_manager.record_cached_put(2000, 100).await;
        metrics_manager.record_cached_put(3000, 150).await;

        // Collect metrics
        let metrics = metrics_manager.collect_metrics().await;

        // Verify averages
        let signed_put_metrics = metrics.signed_put.unwrap();
        assert_eq!(signed_put_metrics.cached_puts_total, 3);
        assert_eq!(signed_put_metrics.average_cached_bytes, 2000); // (1000 + 2000 + 3000) / 3
        assert_eq!(signed_put_metrics.average_streaming_duration_ms, 100); // (50 + 100 + 150) / 3
    }

    #[tokio::test]
    async fn test_cache_write_failure_metric() {
        let metrics_manager = MetricsManager::new();

        // Record cache write failures
        metrics_manager.record_cache_write_failure().await;
        metrics_manager.record_cache_write_failure().await;
        metrics_manager.record_cache_write_failure().await;

        // Get error stats directly
        let error_stats = metrics_manager.get_error_stats().await;
        assert_eq!(error_stats.cache_write_failures_total, 3);

        // Verify it's included in collected metrics (when cache manager is not set, cache metrics will be None)
        // So we just verify the error stats are tracked correctly
        assert_eq!(error_stats.cache_write_failures_total, 3);
    }

    #[tokio::test]
    async fn test_cache_coherency_metrics() {
        let metrics_manager = MetricsManager::new();

        // Record cache coherency events
        metrics_manager
            .record_etag_validation("test-key", "etag1", "etag2", false)
            .await;
        metrics_manager
            .record_etag_mismatch("test-key", "etag1", "etag2")
            .await;
        let ranges = vec!["range1".to_string(), "range2".to_string()];
        metrics_manager
            .record_range_invalidation("test-key", &ranges, "etag_mismatch")
            .await;
        let orphaned = vec!["orphan1".to_string(), "orphan2".to_string()];
        metrics_manager
            .record_orphaned_ranges_cleaned("test-key", &orphaned)
            .await;

        // Record error metrics
        metrics_manager
            .record_corrupted_metadata("test-key", "/path/to/meta", "parse error")
            .await;
        metrics_manager
            .record_cache_operation_duration("etag_validation", "test-key", 50.5)
            .await;
        metrics_manager
            .record_cache_cleanup_failure("test-key", "range_cleanup", "disk full")
            .await;

        // Get error stats directly
        let error_stats = metrics_manager.get_error_stats().await;

        // Verify cache coherency metrics
        assert_eq!(error_stats.cache_etag_validations_total, 1);
        assert_eq!(error_stats.cache_etag_mismatches_total, 1);
        assert_eq!(error_stats.cache_range_invalidations_total, 1);
        assert_eq!(error_stats.cache_orphaned_ranges_cleaned_total, 2);

        // Verify error metrics
        assert_eq!(error_stats.corruption_metadata_total, 1);
        assert_eq!(error_stats.cache_cleanup_failures_total, 1);
        assert_eq!(error_stats.cache_operation_durations.len(), 1);
        assert_eq!(error_stats.cache_operation_durations[0], 50.5);
    }

    #[tokio::test]
    async fn test_atomic_metadata_metrics() {
        let metrics_manager = MetricsManager::new();

        // Record lock operations
        metrics_manager
            .record_atomic_metadata_lock_acquisition_successful(150.5)
            .await;
        metrics_manager
            .record_atomic_metadata_lock_acquisition_failed()
            .await;
        metrics_manager.record_atomic_metadata_lock_timeout().await;
        metrics_manager
            .record_atomic_metadata_stale_lock_detected()
            .await;
        metrics_manager
            .record_atomic_metadata_stale_lock_broken()
            .await;

        // Record corruption detection
        metrics_manager
            .record_atomic_metadata_corruption_detected("test-key", "parse_error", "invalid JSON")
            .await;
        metrics_manager
            .record_atomic_metadata_journal_corruption_detected(
                "test-key",
                "/path/to/journal",
                "truncated file",
            )
            .await;
        metrics_manager
            .record_atomic_metadata_range_file_corruption_detected(
                "test-key",
                "/path/to/range",
                "checksum mismatch",
            )
            .await;

        // Record recovery operations
        metrics_manager
            .record_atomic_metadata_orphaned_range_detected("test-key", 3)
            .await;
        metrics_manager
            .record_atomic_metadata_orphaned_range_recovered("test-key", 2, 250.0)
            .await;
        metrics_manager
            .record_atomic_metadata_orphaned_range_cleaned("test-key", 1, 8388608)
            .await;

        metrics_manager
            .record_atomic_metadata_recovery_attempt("test-key")
            .await;
        metrics_manager
            .record_atomic_metadata_recovery_success("test-key", 500.0)
            .await;

        metrics_manager
            .record_atomic_metadata_journal_consolidation_attempt("test-key")
            .await;
        metrics_manager
            .record_atomic_metadata_journal_consolidation_success("test-key", 5, 100.0)
            .await;

        // Record journal operations
        metrics_manager
            .record_atomic_metadata_journal_entry_written("test-key", "add_range")
            .await;
        metrics_manager
            .record_atomic_metadata_journal_cleanup("test-key", 3)
            .await;
        metrics_manager
            .record_atomic_metadata_journal_validation_failure(
                "test-key",
                "/path/to/journal",
                "invalid entry",
            )
            .await;

        // Record hybrid writer operations
        metrics_manager
            .record_atomic_metadata_immediate_write("test-key")
            .await;
        metrics_manager
            .record_atomic_metadata_journal_only_write("test-key")
            .await;
        metrics_manager
            .record_atomic_metadata_hybrid_write("test-key")
            .await;
        metrics_manager
            .record_atomic_metadata_write_mode_fallback(
                "test-key",
                "immediate",
                "journal_only",
                "high_contention",
            )
            .await;

        // Collect metrics
        let metrics = metrics_manager.collect_metrics().await;

        // Verify atomic metadata metrics
        assert!(metrics.atomic_metadata.is_some());
        let atomic_metrics = metrics.atomic_metadata.unwrap();

        // Verify lock metrics
        assert_eq!(atomic_metrics.lock_acquisitions_successful, 1);
        assert_eq!(atomic_metrics.lock_acquisitions_failed, 1);
        assert_eq!(atomic_metrics.lock_timeouts_total, 1);
        assert_eq!(atomic_metrics.stale_locks_detected, 1);
        assert_eq!(atomic_metrics.stale_locks_broken, 1);
        assert_eq!(atomic_metrics.average_lock_hold_time_ms, 150.5);

        // Verify corruption metrics
        assert_eq!(atomic_metrics.metadata_corruption_detected, 1);
        assert_eq!(atomic_metrics.journal_corruption_detected, 1);
        assert_eq!(atomic_metrics.range_file_corruption_detected, 1);

        // Verify recovery metrics
        assert_eq!(atomic_metrics.orphaned_ranges_detected, 3);
        assert_eq!(atomic_metrics.orphaned_ranges_recovered, 2);
        assert_eq!(atomic_metrics.orphaned_ranges_cleaned, 1);
        assert_eq!(atomic_metrics.metadata_recovery_attempts, 1);
        assert_eq!(atomic_metrics.metadata_recovery_successes, 1);
        assert_eq!(atomic_metrics.journal_consolidation_attempts, 1);
        assert_eq!(atomic_metrics.journal_consolidation_successes, 1);
        assert_eq!(
            atomic_metrics.average_recovery_duration_ms,
            (250.0 + 500.0 + 100.0) / 3.0
        );

        // Verify journal metrics
        assert_eq!(atomic_metrics.journal_entries_written, 1);
        assert_eq!(atomic_metrics.journal_entries_consolidated, 5);
        assert_eq!(atomic_metrics.journal_cleanup_operations, 1);
        assert_eq!(atomic_metrics.journal_validation_failures, 1);

        // Verify hybrid writer metrics
        assert_eq!(atomic_metrics.immediate_writes, 1);
        assert_eq!(atomic_metrics.journal_only_writes, 1);
        assert_eq!(atomic_metrics.hybrid_writes, 1);
        assert_eq!(atomic_metrics.write_mode_fallbacks, 1);
    }

    #[tokio::test]
    async fn test_atomic_metadata_metrics_histogram_behavior() {
        let metrics_manager = MetricsManager::new();

        // Record multiple lock hold times
        metrics_manager
            .record_atomic_metadata_lock_acquisition_successful(100.0)
            .await;
        metrics_manager
            .record_atomic_metadata_lock_acquisition_successful(200.0)
            .await;
        metrics_manager
            .record_atomic_metadata_lock_acquisition_successful(300.0)
            .await;

        // Record multiple recovery durations
        metrics_manager
            .record_atomic_metadata_orphaned_range_recovered("test-key", 1, 150.0)
            .await;
        metrics_manager
            .record_atomic_metadata_recovery_success("test-key", 250.0)
            .await;
        metrics_manager
            .record_atomic_metadata_journal_consolidation_success("test-key", 2, 350.0)
            .await;

        // Collect metrics
        let metrics = metrics_manager.collect_metrics().await;
        let atomic_metrics = metrics.atomic_metadata.unwrap();

        // Verify averages are calculated correctly
        assert_eq!(atomic_metrics.average_lock_hold_time_ms, 200.0); // (100 + 200 + 300) / 3
        assert_eq!(atomic_metrics.average_recovery_duration_ms, 250.0); // (150 + 250 + 350) / 3
        assert_eq!(atomic_metrics.lock_acquisitions_successful, 3);
        assert_eq!(atomic_metrics.journal_entries_consolidated, 2);
    }

    #[tokio::test]
    async fn test_connection_keepalive_metrics() {
        let metrics_manager = MetricsManager::new();

        // Record connection creation
        metrics_manager
            .record_connection_created("s3.amazonaws.com")
            .await;
        metrics_manager
            .record_connection_created("s3.amazonaws.com")
            .await;
        metrics_manager
            .record_connection_created("s3.us-west-2.amazonaws.com")
            .await;

        // Record connection reuse
        metrics_manager
            .record_connection_reused("s3.amazonaws.com")
            .await;
        metrics_manager
            .record_connection_reused("s3.amazonaws.com")
            .await;
        metrics_manager
            .record_connection_reused("s3.amazonaws.com")
            .await;

        // Record closures
        metrics_manager.record_idle_timeout_closure().await;
        metrics_manager.record_max_lifetime_closure().await;
        metrics_manager.record_error_closure().await;

        // Get stats
        let stats = metrics_manager.get_connection_keepalive_stats().await;

        // Verify counts
        assert_eq!(
            *stats
                .connections_created
                .get("s3.amazonaws.com")
                .unwrap_or(&0),
            2
        );
        assert_eq!(
            *stats
                .connections_created
                .get("s3.us-west-2.amazonaws.com")
                .unwrap_or(&0),
            1
        );
        assert_eq!(
            *stats
                .connections_reused
                .get("s3.amazonaws.com")
                .unwrap_or(&0),
            3
        );
        assert_eq!(stats.idle_timeout_closures, 1);
        assert_eq!(stats.max_lifetime_closures, 1);
        assert_eq!(stats.error_closures, 1);
    }

    #[tokio::test]
    async fn test_part_cache_metrics() {
        let metrics_manager = MetricsManager::new();

        // Record part cache operations
        metrics_manager
            .record_part_cache_hit("test-bucket/test-object", 1, 8388608)
            .await;
        metrics_manager
            .record_part_cache_miss("test-bucket/test-object", 2)
            .await;
        metrics_manager
            .record_part_cache_store("test-bucket/test-object", 2, 8388608)
            .await;
        metrics_manager
            .record_part_cache_eviction("test-bucket/test-object", &[3, 4], 16777216)
            .await;
        metrics_manager
            .record_part_cache_error("test-bucket/test-object", 5, "lookup", "disk read error")
            .await;

        // Get part cache stats
        let (hits, misses, stores, evictions, errors) =
            metrics_manager.get_part_cache_stats().await;

        // Verify counts
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
        assert_eq!(stores, 1);
        assert_eq!(evictions, 2); // 2 parts evicted
        assert_eq!(errors, 1);

        // Verify metrics are included in collected metrics (when cache manager is not set, cache metrics will be None)
        // So we just verify the error stats are tracked correctly
        let error_stats = metrics_manager.get_error_stats().await;
        assert_eq!(error_stats.cache_part_hits, 1);
        assert_eq!(error_stats.cache_part_misses, 1);
        assert_eq!(error_stats.cache_part_stores, 1);
        assert_eq!(error_stats.cache_part_evictions, 2);
        assert_eq!(error_stats.cache_part_errors, 1);
    }
}
