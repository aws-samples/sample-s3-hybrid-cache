//! OTLP Metrics Export — mirrors the /metrics JSON endpoint.
//!
//! Every numeric field from SystemMetrics is exported as an OTLP gauge using
//! the JSON path as the metric name (e.g. `cache.total_cache_size`,
//! `compression.average_compression_ratio`). This ensures OTLP consumers
//! (CloudWatch, Prometheus) see the same data as the /metrics JSON API.

use crate::config::OtlpConfig;
use crate::metrics::{BucketTrafficKey, BucketTrafficStats, SystemMetrics};
use crate::{ProxyError, Result};
use opentelemetry::metrics::{Gauge, MeterProvider as _, ObservableCounter};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub struct OtlpExporter {
    config: OtlpConfig,
    provider: Option<SdkMeterProvider>,
    instruments: Option<Instruments>,
    /// Per-bucket observable counter instruments (created only when
    /// `metrics.otlp.per_bucket_enabled` is true). Fields are held to keep the
    /// callbacks registered with the SDK — dropping them deregisters the callbacks.
    per_bucket_instruments: Option<PerBucketInstruments>,
    /// Shared handle to the per-bucket traffic stats map. Held alongside the
    /// instruments so the Arc (and thus the callbacks' captured clone) stays live.
    bucket_traffic_stats:
        Option<Arc<tokio::sync::RwLock<HashMap<BucketTrafficKey, BucketTrafficStats>>>>,
}

/// Per-bucket OTLP observable counter instruments.
///
/// Four counters mirroring S3's per-bucket GET/PUT request metrics. Each counter
/// is registered with a collection callback that snapshots the in-memory traffic
/// map and reports absolute cumulative values; the SDK owns the cumulative
/// temporality semantics.
///
/// The struct fields are intentionally never read after construction — they are
/// held only to keep the SDK callbacks alive. Dropping this struct deregisters
/// all callbacks.
///
/// Spec: per-bucket-metrics. Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
#[allow(dead_code)]
struct PerBucketInstruments {
    bytes_downloaded: ObservableCounter<u64>, // mirrors S3 BytesDownloaded
    bytes_uploaded: ObservableCounter<u64>,   // mirrors S3 BytesUploaded
    get_requests: ObservableCounter<u64>,     // mirrors S3 GetRequests
    put_requests: ObservableCounter<u64>,     // mirrors S3 PutRequests
}

/// One gauge per numeric field in the /metrics JSON response.
/// Field names use dot-separated JSON paths: `{section}.{field}`.
struct Instruments {
    // -- cache: sizes --
    cache_total_cache_size: Gauge<u64>,
    cache_read_cache_size: Gauge<u64>,
    cache_write_cache_size: Gauge<u64>,
    cache_ram_cache_size: Gauge<u64>,
    // -- cache: hits/misses/evictions --
    cache_hits: Gauge<u64>,
    cache_misses: Gauge<u64>,
    cache_evictions: Gauge<u64>,
    cache_write_hits: Gauge<u64>,
    cache_s3_requests_saved: Gauge<u64>,
    // -- cache: bytes saved --
    bytes_served_from_cache: Gauge<u64>,
    // -- cache per-tier: RAM --
    ram_cache_hits: Gauge<u64>,
    ram_cache_misses: Gauge<u64>,
    ram_cache_evictions: Gauge<u64>,
    // -- cache per-tier: metadata --
    metadata_cache_hits: Gauge<u64>,
    metadata_cache_misses: Gauge<u64>,
    metadata_cache_entries: Gauge<u64>,
    metadata_cache_evictions: Gauge<u64>,
    metadata_cache_stale_refreshes: Gauge<u64>,
    // -- cache: error/health signals --
    cache_corruption_metadata_total: Gauge<u64>,
    cache_corruption_missing_range_total: Gauge<u64>,
    cache_disk_full_events_total: Gauge<u64>,
    cache_lock_timeout_total: Gauge<u64>,
    cache_write_failures_total: Gauge<u64>,
    cache_etag_mismatches_total: Gauge<u64>,
    cache_range_invalidations_total: Gauge<u64>,
    cache_incomplete_uploads_evicted: Gauge<u64>,
    // -- compression --
    compression_ratio: Gauge<f64>,
    compression_failures: Gauge<u64>,
    // -- connection_pool --
    conn_failed: Gauge<u64>,
    conn_latency_ms: Gauge<u64>,
    conn_success_rate: Gauge<f64>,
    // -- coalescing --
    coalesce_waits: Gauge<u64>,
    coalesce_cache_hits_after_wait: Gauge<u64>,
    coalesce_timeouts: Gauge<u64>,
    coalesce_s3_fetches_saved: Gauge<u64>,
    coalesce_avg_wait_ms: Gauge<f64>,
    coalesce_waiter_conditional_304: Gauge<u64>,
    coalesce_waiter_conditional_200: Gauge<u64>,
    coalesce_waiter_conditional_4xx: Gauge<u64>,
    coalesce_waiter_conditional_error: Gauge<u64>,
    // -- request_metrics --
    req_total: Gauge<u64>,
    req_successful: Gauge<u64>,
    req_failed: Gauge<u64>,
    req_avg_latency_ms: Gauge<u64>,
    req_active: Gauge<u64>,
    // -- cache_rules --
    cache_rules_reloads_total: Gauge<u64>,
    cache_rules_reload_failures_total: Gauge<u64>,
    cache_rules_on_fallback: Gauge<u64>,
    cache_rules_rules_loaded: Gauge<u64>,
    // -- cache: invalidation + revalidation --
    cache_read_disabled_invalidations_total: Gauge<u64>,
    cache_ttl_revalidations_total: Gauge<u64>,
    // -- top-level --
    uptime_seconds: Gauge<u64>,
    // -- process --
    memory_usage_bytes: Gauge<u64>,
    // -- download bandwidth QoS --
    bw_instance_ceiling_bps: Gauge<u64>,
    bw_failopen_total: Gauge<u64>,
    bw_class_bytes: Gauge<u64>,
}

impl OtlpExporter {
    pub fn new(config: OtlpConfig) -> Self {
        Self {
            config,
            provider: None,
            instruments: None,
            per_bucket_instruments: None,
            bucket_traffic_stats: None,
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        if !self.config.enabled {
            info!("OTLP metrics export is disabled");
            return Ok(());
        }
        if self.config.endpoint.is_empty() {
            return Err(ProxyError::ConfigError(
                "OTLP endpoint cannot be empty".into(),
            ));
        }
        info!(
            "Initializing OTLP exporter: endpoint={}, interval={:?}",
            self.config.endpoint, self.config.export_interval
        );

        let exporter = MetricExporter::builder()
            .with_http()
            .with_endpoint(&self.config.endpoint)
            .with_timeout(self.config.timeout)
            .build()
            .map_err(|e| ProxyError::SystemError(format!("OTLP build failed: {}", e)))?;

        let reader = PeriodicReader::builder(exporter)
            .with_interval(self.config.export_interval)
            .build();

        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(
                Resource::builder_empty()
                    .with_attributes(vec![
                        KeyValue::new(
                            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                            "s3-proxy",
                        ),
                        KeyValue::new(
                            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                            env!("CARGO_PKG_VERSION"),
                        ),
                        KeyValue::new("host.name", hostname),
                    ])
                    .build(),
            )
            .build();

        let m = provider.meter("s3-proxy");

        macro_rules! g_u64 {
            ($name:expr) => {
                m.u64_gauge($name).build()
            };
        }
        macro_rules! g_f64 {
            ($name:expr) => {
                m.f64_gauge($name).build()
            };
        }

        let instruments = Instruments {
            // cache: sizes
            cache_total_cache_size: g_u64!("cache.total_cache_size"),
            cache_read_cache_size: g_u64!("cache.read_cache_size"),
            cache_write_cache_size: g_u64!("cache.write_cache_size"),
            cache_ram_cache_size: g_u64!("cache.ram_cache_size"),
            // cache: hits/misses/evictions
            cache_hits: g_u64!("cache.cache_hits"),
            cache_misses: g_u64!("cache.cache_misses"),
            cache_evictions: g_u64!("cache.evictions"),
            cache_write_hits: g_u64!("cache.write_cache_hits"),
            cache_s3_requests_saved: g_u64!("cache.s3_requests_saved"),
            // cache: bytes saved
            bytes_served_from_cache: g_u64!("cache.bytes_served_from_cache"),
            // cache per-tier: RAM
            ram_cache_hits: g_u64!("cache.ram_cache_hits"),
            ram_cache_misses: g_u64!("cache.ram_cache_misses"),
            ram_cache_evictions: g_u64!("cache.ram_cache_evictions"),
            // cache per-tier: metadata
            metadata_cache_hits: g_u64!("cache.metadata_cache_hits"),
            metadata_cache_misses: g_u64!("cache.metadata_cache_misses"),
            metadata_cache_entries: g_u64!("cache.metadata_cache_entries"),
            metadata_cache_evictions: g_u64!("cache.metadata_cache_evictions"),
            metadata_cache_stale_refreshes: g_u64!("cache.metadata_cache_stale_refreshes"),
            // cache: error/health signals
            cache_corruption_metadata_total: g_u64!("cache.corruption_metadata_total"),
            cache_corruption_missing_range_total: g_u64!("cache.corruption_missing_range_total"),
            cache_disk_full_events_total: g_u64!("cache.disk_full_events_total"),
            cache_lock_timeout_total: g_u64!("cache.lock_timeout_total"),
            cache_write_failures_total: g_u64!("cache.write_failures_total"),
            cache_etag_mismatches_total: g_u64!("cache.etag_mismatches_total"),
            cache_range_invalidations_total: g_u64!("cache.range_invalidations_total"),
            cache_incomplete_uploads_evicted: g_u64!("cache.incomplete_uploads_evicted"),
            // compression
            compression_ratio: g_f64!("compression.average_compression_ratio"),
            compression_failures: g_u64!("compression.failures_total"),
            // connection pool
            conn_failed: g_u64!("connection_pool.failed_connections"),
            conn_latency_ms: g_u64!("connection_pool.average_latency_ms"),
            conn_success_rate: g_f64!("connection_pool.success_rate_percent"),
            // coalescing
            coalesce_waits: g_u64!("coalescing.waits_total"),
            coalesce_cache_hits_after_wait: g_u64!("coalescing.cache_hits_after_wait_total"),
            coalesce_timeouts: g_u64!("coalescing.timeouts_total"),
            coalesce_s3_fetches_saved: g_u64!("coalescing.s3_fetches_saved_total"),
            coalesce_avg_wait_ms: g_f64!("coalescing.average_wait_duration_ms"),
            coalesce_waiter_conditional_304: g_u64!("coalescing.waiter_conditional_304"),
            coalesce_waiter_conditional_200: g_u64!("coalescing.waiter_conditional_200"),
            coalesce_waiter_conditional_4xx: g_u64!("coalescing.waiter_conditional_4xx"),
            coalesce_waiter_conditional_error: g_u64!("coalescing.waiter_conditional_error"),
            // request metrics
            req_total: g_u64!("request_metrics.total_requests"),
            req_successful: g_u64!("request_metrics.successful_requests"),
            req_failed: g_u64!("request_metrics.failed_requests"),
            req_avg_latency_ms: g_u64!("request_metrics.average_response_time_ms"),
            req_active: g_u64!("request_metrics.active_requests"),
            // cache_rules
            cache_rules_reloads_total: g_u64!("cache_rules.reloads_total"),
            cache_rules_reload_failures_total: g_u64!("cache_rules.reload_failures_total"),
            cache_rules_on_fallback: g_u64!("cache_rules.on_fallback"),
            cache_rules_rules_loaded: g_u64!("cache_rules.rules_loaded"),
            // cache: invalidation + revalidation
            cache_read_disabled_invalidations_total: g_u64!(
                "cache.read_disabled_invalidations_total"
            ),
            cache_ttl_revalidations_total: g_u64!("cache.ttl_revalidations_total"),
            // top-level
            uptime_seconds: g_u64!("uptime_seconds"),
            memory_usage_bytes: g_u64!("process.memory_usage_bytes"),
            // download bandwidth QoS
            bw_instance_ceiling_bps: g_u64!("download_bandwidth.instance_ceiling_bps"),
            bw_failopen_total: g_u64!("download_bandwidth.failopen_total"),
            bw_class_bytes: g_u64!("download_bandwidth.class_bytes"),
        };

        self.provider = Some(provider);
        self.instruments = Some(instruments);
        info!("OTLP metrics exporter initialized");
        Ok(())
    }

    /// Record SystemMetrics into OTLP instruments — same data as /metrics JSON.
    pub async fn export_metrics(&self, metrics: &SystemMetrics) -> Result<()> {
        let i = match &self.instruments {
            Some(i) => i,
            None => return Ok(()),
        };
        let a = &[]; // no extra attributes — host/service come from Resource

        if let Some(c) = &metrics.cache {
            // sizes
            i.cache_total_cache_size.record(c.total_cache_size, a);
            i.cache_read_cache_size.record(c.read_cache_size, a);
            i.cache_write_cache_size.record(c.write_cache_size, a);
            i.cache_ram_cache_size.record(c.ram_cache_size, a);
            // hits/misses/evictions
            i.cache_hits.record(c.cache_hits, a);
            i.cache_misses.record(c.cache_misses, a);
            i.cache_evictions.record(c.evictions, a);
            i.cache_write_hits.record(c.write_cache_hits, a);
            i.cache_s3_requests_saved
                .record(c.cache_hits + c.metadata_cache_hits, a);
            i.bytes_served_from_cache
                .record(c.bytes_served_from_cache, a);
            // RAM tier
            i.ram_cache_hits.record(c.ram_cache_hits, a);
            i.ram_cache_misses.record(c.ram_cache_misses, a);
            i.ram_cache_evictions.record(c.ram_cache_evictions, a);
            // metadata tier
            i.metadata_cache_hits.record(c.metadata_cache_hits, a);
            i.metadata_cache_misses.record(c.metadata_cache_misses, a);
            i.metadata_cache_entries.record(c.metadata_cache_entries, a);
            i.metadata_cache_evictions
                .record(c.metadata_cache_evictions, a);
            i.metadata_cache_stale_refreshes
                .record(c.metadata_cache_stale_refreshes, a);
            // error/health signals
            i.cache_corruption_metadata_total
                .record(c.corruption_metadata_total, a);
            i.cache_corruption_missing_range_total
                .record(c.corruption_missing_range_total, a);
            i.cache_disk_full_events_total
                .record(c.disk_full_events_total, a);
            i.cache_lock_timeout_total.record(c.lock_timeout_total, a);
            i.cache_write_failures_total
                .record(c.cache_write_failures_total, a);
            i.cache_etag_mismatches_total
                .record(c.cache_etag_mismatches_total, a);
            i.cache_range_invalidations_total
                .record(c.cache_range_invalidations_total, a);
            i.cache_incomplete_uploads_evicted
                .record(c.incomplete_uploads_evicted, a);
        }

        if let Some(comp) = &metrics.compression {
            i.compression_ratio
                .record(comp.average_compression_ratio as f64, a);
            i.compression_failures
                .record(comp.compression_failures + comp.decompression_failures, a);
        }

        if let Some(conn) = &metrics.connection_pool {
            i.conn_failed.record(conn.failed_connections, a);
            i.conn_latency_ms.record(conn.average_latency_ms, a);
            i.conn_success_rate
                .record(conn.success_rate_percent as f64, a);
        }

        if let Some(coal) = &metrics.coalescing {
            i.coalesce_waits.record(coal.waits_total, a);
            i.coalesce_cache_hits_after_wait
                .record(coal.cache_hits_after_wait_total, a);
            i.coalesce_timeouts.record(coal.timeouts_total, a);
            i.coalesce_s3_fetches_saved
                .record(coal.s3_fetches_saved_total, a);
            i.coalesce_avg_wait_ms
                .record(coal.average_wait_duration_ms, a);
            i.coalesce_waiter_conditional_304
                .record(coal.waiter_conditional_304, a);
            i.coalesce_waiter_conditional_200
                .record(coal.waiter_conditional_200, a);
            i.coalesce_waiter_conditional_4xx
                .record(coal.waiter_conditional_4xx, a);
            i.coalesce_waiter_conditional_error
                .record(coal.waiter_conditional_error, a);
        }

        let rm = &metrics.request_metrics;
        i.req_total.record(rm.total_requests, a);
        i.req_successful.record(rm.successful_requests, a);
        i.req_failed.record(rm.failed_requests, a);
        i.req_avg_latency_ms.record(rm.average_response_time_ms, a);
        i.req_active.record(rm.active_requests, a);

        // cache_rules
        if let Some(cr) = &metrics.cache_rules {
            i.cache_rules_reloads_total.record(cr.reloads_total, a);
            i.cache_rules_reload_failures_total
                .record(cr.reload_failures_total, a);
            i.cache_rules_on_fallback
                .record(if cr.on_fallback { 1 } else { 0 }, a);
            i.cache_rules_rules_loaded.record(cr.rules_loaded, a);
        }

        // cache: invalidation + revalidation (from CacheMetrics)
        if let Some(c) = &metrics.cache {
            i.cache_read_disabled_invalidations_total
                .record(c.read_cache_disabled_invalidations_total, a);
            i.cache_ttl_revalidations_total
                .record(c.ttl_revalidations_total, a);
        }

        i.uptime_seconds.record(metrics.uptime_seconds, a);

        if let Ok(mem) = self.get_memory_usage() {
            i.memory_usage_bytes.record(mem, a);
        }

        // download bandwidth QoS
        let bw = &metrics.download_bandwidth;
        i.bw_instance_ceiling_bps.record(bw.instance_ceiling_bps, a);
        i.bw_failopen_total.record(bw.failopen_total, a);
        // Per-class bytes with sanitized label attributes.
        for (label, bytes) in &bw.class_bytes {
            let attrs = &[KeyValue::new("class", label.clone())];
            i.bw_class_bytes.record(*bytes, attrs);
        }
        if bw.residual_bytes > 0 {
            let attrs = &[KeyValue::new("class", "residual")];
            i.bw_class_bytes.record(bw.residual_bytes, attrs);
        }

        debug!("OTLP metrics recorded (same fields as /metrics JSON)");
        Ok(())
    }

    // Per-request recording is a no-op — metrics come from the periodic SystemMetrics snapshot.
    pub fn record_proxy_request(&self, _: &str, _: u64, _: u64, _: bool) {}
    pub fn record_s3_request(&self, _: &str, _: u64, _: u64, _: bool, _: Option<u16>) {}

    fn get_memory_usage(&self) -> Result<u64> {
        #[cfg(target_os = "linux")]
        {
            let status = std::fs::read_to_string("/proc/self/status")
                .map_err(|e| ProxyError::SystemError(format!("read /proc/self/status: {}", e)))?;
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return parts[1]
                            .parse::<u64>()
                            .map(|kb| kb * 1024)
                            .map_err(|e| ProxyError::SystemError(format!("parse VmRSS: {}", e)));
                    }
                }
            }
            Err(ProxyError::SystemError("VmRSS not found".into()))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(0)
        }
    }

    /// Register per-bucket `ObservableCounter<u64>` instruments with collection callbacks.
    ///
    /// Four counters (bytes_downloaded, bytes_uploaded, get_requests, put_requests)
    /// are created on the same meter
    /// that `initialize()` set up. Each counter's callback snapshots the shared
    /// `bucket_traffic_stats` map via `try_read()` (non-blocking; skips the observation
    /// cycle when the write lock is held and picks it up next cycle). Absolute
    /// cumulative values are reported; the SDK owns the delta math and cumulative
    /// temporality.
    ///
    /// This method is a no-op when:
    /// - `metrics.otlp.per_bucket_enabled` is `false` (config guard).
    /// - `initialize()` has not been called (provider is `None`).
    ///
    /// Safe to call multiple times — each call replaces the previous instrument set.
    ///
    /// Spec: per-bucket-metrics. Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
    pub fn initialize_per_bucket(
        &mut self,
        bucket_traffic_stats: Arc<
            tokio::sync::RwLock<HashMap<BucketTrafficKey, BucketTrafficStats>>,
        >,
    ) {
        if !self.config.per_bucket_enabled {
            debug!("initialize_per_bucket: per_bucket_enabled=false, skipping");
            return;
        }

        let provider = match &self.provider {
            Some(p) => p,
            None => {
                warn!("initialize_per_bucket: OTLP provider not initialized, skipping per-bucket counters");
                return;
            }
        };

        let m = provider.meter("s3-proxy");

        // Build one ObservableCounter for each of the nine per-bucket metrics.
        // Each counter captures its own Arc clone so callbacks are independent.
        //
        // The macro syntax `traffic.$field` works because `$field` is an `ident` token;
        // the macro expander substitutes it directly into the field access expression.
        macro_rules! make_counter {
            ($name:expr, $field:ident) => {{
                let stats = bucket_traffic_stats.clone();
                m.u64_observable_counter($name)
                    .with_callback(move |observer| {
                        // Non-blocking read: skip this cycle if the write lock is held.
                        if let Ok(guard) = stats.try_read() {
                            for (key, traffic) in guard.iter() {
                                let mut attrs = vec![KeyValue::new("bucket", key.bucket.clone())];
                                if let Some(p) = &key.prefix {
                                    attrs.push(KeyValue::new("prefix", p.clone()));
                                }
                                observer.observe(traffic.$field, &attrs);
                            }
                        }
                    })
                    .build()
            }};
        }

        let per_bucket = PerBucketInstruments {
            bytes_downloaded: make_counter!("s3proxy.bytes_downloaded", bytes_served),
            bytes_uploaded: make_counter!("s3proxy.bytes_uploaded", bytes_uploaded),
            get_requests: make_counter!("s3proxy.get_requests", get_requests),
            put_requests: make_counter!("s3proxy.put_requests", put_requests),
        };

        self.per_bucket_instruments = Some(per_bucket);
        self.bucket_traffic_stats = Some(bucket_traffic_stats);
        info!("Per-bucket OTLP observable counters initialized (4 instruments)");
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(provider) = self.provider.take() {
            info!("Shutting down OTLP exporter");
            // Drop per-bucket instruments before shutting down the provider so the
            // callbacks are deregistered cleanly before the provider is destroyed.
            self.per_bucket_instruments = None;
            self.bucket_traffic_stats = None;
            provider
                .shutdown()
                .map_err(|e| ProxyError::SystemError(format!("OTLP shutdown: {}", e)))?;
            self.instruments = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Regression guard (telemetry-enhancements Req 9.2): `opentelemetry-otlp`'s
    /// default features pull in `reqwest-blocking-client`, whose blocking client
    /// cannot be constructed inside the Tokio runtime and makes the HTTP exporter
    /// fail at init with "no http client specified" — silently disabling all
    /// telemetry. With default features disabled and the async `reqwest-client`
    /// enabled, `initialize()` must succeed inside a Tokio runtime.
    #[tokio::test]
    async fn otlp_exporter_initializes_without_blocking_client() {
        let cfg = OtlpConfig {
            enabled: true,
            endpoint: "http://localhost:4318/v1/metrics".to_string(),
            export_interval: Duration::from_secs(60),
            timeout: Duration::from_secs(10),
            ..OtlpConfig::default()
        };
        let mut exporter = OtlpExporter::new(cfg);
        let res = exporter.initialize().await;
        assert!(
            res.is_ok(),
            "OTLP exporter init must succeed (no http client regression): {:?}",
            res.err()
        );
        let _ = exporter.shutdown().await;
    }

    /// A disabled exporter must initialize to a no-op without constructing a client.
    #[tokio::test]
    async fn otlp_exporter_disabled_is_noop() {
        let cfg = OtlpConfig {
            enabled: false,
            ..OtlpConfig::default()
        };
        let mut exporter = OtlpExporter::new(cfg);
        assert!(exporter.initialize().await.is_ok());
    }

    /// Integration test: per-bucket OTLP observable counters produce data points
    /// with the correct `bucket` attribute (bucket-only entry) and both `bucket`
    /// and `prefix` attributes (prefix-attributed entry).
    ///
    /// Uses the SDK's InMemoryMetricExporter to capture exported data without a
    /// real OTLP collector. Verifies the callback reads from the shared traffic
    /// map and reports absolute cumulative values with the expected attribute sets.
    ///
    /// **Validates: Requirements 4.6, 4.2** (and the attribute half of Property 7)
    #[tokio::test]
    async fn test_per_bucket_otlp_recording_attributes() {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::data::Sum;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };
        use opentelemetry_sdk::Resource as SdkResource;
        use std::sync::Arc;
        use tokio::sync::RwLock;

        // 1. Build an InMemoryMetricExporter-backed provider (no network).
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_millis(100))
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(SdkResource::builder_empty().build())
            .build();
        let meter = provider.meter("s3-proxy-test");

        // 2. Populate a shared traffic map with two entries:
        //    - bucket-only: "test-bucket" with traffic
        //    - prefix-attributed: "test-bucket" + prefix "logs/" with traffic
        let traffic_map: Arc<RwLock<HashMap<BucketTrafficKey, BucketTrafficStats>>> =
            Arc::new(RwLock::new(HashMap::new()));
        {
            let mut map = traffic_map.write().await;
            map.insert(
                BucketTrafficKey {
                    bucket: "test-bucket".to_string(),
                    prefix: None,
                },
                BucketTrafficStats {
                    bytes_served: 5000,
                    bytes_uploaded: 1000,
                    get_requests: 10,
                    put_requests: 3,
                },
            );
            map.insert(
                BucketTrafficKey {
                    bucket: "test-bucket".to_string(),
                    prefix: Some("logs/".to_string()),
                },
                BucketTrafficStats {
                    bytes_served: 2000,
                    bytes_uploaded: 500,
                    get_requests: 4,
                    put_requests: 2,
                },
            );
        }

        // 3. Register observable counters using the same callback pattern as
        //    initialize_per_bucket. Hold instruments to keep callbacks alive.
        macro_rules! make_test_counter {
            ($name:expr, $field:ident) => {{
                let stats = traffic_map.clone();
                meter
                    .u64_observable_counter($name)
                    .with_callback(move |observer| {
                        if let Ok(guard) = stats.try_read() {
                            for (key, traffic) in guard.iter() {
                                let mut attrs = vec![KeyValue::new("bucket", key.bucket.clone())];
                                if let Some(p) = &key.prefix {
                                    attrs.push(KeyValue::new("prefix", p.clone()));
                                }
                                observer.observe(traffic.$field, &attrs);
                            }
                        }
                    })
                    .build()
            }};
        }

        let _bytes_downloaded = make_test_counter!("s3proxy.bytes_downloaded", bytes_served);
        let _bytes_uploaded = make_test_counter!("s3proxy.bytes_uploaded", bytes_uploaded);
        let _get_requests = make_test_counter!("s3proxy.get_requests", get_requests);
        let _put_requests = make_test_counter!("s3proxy.put_requests", put_requests);

        // 4. Trigger collection via force_flush.
        provider.force_flush().expect("force_flush should succeed");

        // 5. Retrieve exported metrics and assert attributes.
        let finished = exporter
            .get_finished_metrics()
            .expect("should retrieve exported metrics");

        assert!(
            !finished.is_empty(),
            "InMemoryExporter should have captured at least one ResourceMetrics"
        );

        // Find the scope for our test meter.
        let rm = &finished[0];
        let scope = rm
            .scope_metrics
            .iter()
            .find(|s| s.scope.name() == "s3-proxy-test")
            .expect("scope_metrics should contain s3-proxy-test");

        // Assert: s3proxy.bytes_downloaded has correct data points.
        let bytes_dl_metric = scope
            .metrics
            .iter()
            .find(|m| m.name == "s3proxy.bytes_downloaded")
            .expect("should find s3proxy.bytes_downloaded metric");

        let sum_data = bytes_dl_metric
            .data
            .as_any()
            .downcast_ref::<Sum<u64>>()
            .expect("bytes_downloaded should be Sum<u64>");

        assert!(
            sum_data.is_monotonic,
            "Observable counters must be monotonic (cumulative)"
        );

        // Bucket-only data point (test-bucket, no prefix attr).
        let bucket_only_dp = sum_data
            .data_points
            .iter()
            .find(|dp| {
                dp.attributes
                    .contains(&KeyValue::new("bucket", "test-bucket"))
                    && !dp.attributes.iter().any(|kv| kv.key.as_str() == "prefix")
            })
            .expect("should have a data point for test-bucket without prefix");
        assert_eq!(
            bucket_only_dp.value, 5000,
            "bucket-only bytes_downloaded should be 5000"
        );

        // Prefix-attributed data point (test-bucket + prefix "logs/").
        let prefix_dp = sum_data
            .data_points
            .iter()
            .find(|dp| {
                dp.attributes
                    .contains(&KeyValue::new("bucket", "test-bucket"))
                    && dp.attributes.contains(&KeyValue::new("prefix", "logs/"))
            })
            .expect("should have a data point for test-bucket with prefix=logs/");
        assert_eq!(
            prefix_dp.value, 2000,
            "prefix-attributed bytes_downloaded should be 2000"
        );

        // 6. Verify put_requests metric to confirm the request-count counters work.
        let put_req_metric = scope
            .metrics
            .iter()
            .find(|m| m.name == "s3proxy.put_requests")
            .expect("should find s3proxy.put_requests metric");

        let put_req_sum = put_req_metric
            .data
            .as_any()
            .downcast_ref::<Sum<u64>>()
            .expect("put_requests should be Sum<u64>");

        let put_req_bucket_dp = put_req_sum
            .data_points
            .iter()
            .find(|dp| {
                dp.attributes
                    .contains(&KeyValue::new("bucket", "test-bucket"))
                    && !dp.attributes.iter().any(|kv| kv.key.as_str() == "prefix")
            })
            .expect("should have put_requests data point for test-bucket without prefix");
        assert_eq!(
            put_req_bucket_dp.value, 3,
            "bucket-only put_requests should be 3"
        );

        let put_req_prefix_dp = put_req_sum
            .data_points
            .iter()
            .find(|dp| {
                dp.attributes
                    .contains(&KeyValue::new("bucket", "test-bucket"))
                    && dp.attributes.contains(&KeyValue::new("prefix", "logs/"))
            })
            .expect("should have put_requests data point for test-bucket with prefix=logs/");
        assert_eq!(
            put_req_prefix_dp.value, 2,
            "prefix-attributed put_requests should be 2"
        );

        // Cleanup
        let _ = provider.shutdown();
    }
}
