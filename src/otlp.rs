//! OTLP Metrics Export — mirrors the /metrics JSON endpoint.
//!
//! Every numeric field from SystemMetrics is exported as an OTLP gauge using
//! the JSON path as the metric name (e.g. `cache.total_cache_size`,
//! `compression.average_compression_ratio`). This ensures OTLP consumers
//! (CloudWatch, Prometheus) see the same data as the /metrics JSON API.

use crate::config::OtlpConfig;
use crate::metrics::SystemMetrics;
use crate::{ProxyError, Result};
use opentelemetry::metrics::{Gauge, MeterProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use tracing::{debug, info};

pub struct OtlpExporter {
    config: OtlpConfig,
    provider: Option<SdkMeterProvider>,
    instruments: Option<Instruments>,
}

/// One gauge per numeric field in the /metrics JSON response.
/// Field names use dot-separated JSON paths: `{section}.{field}`.
struct Instruments {
    // -- cache --
    cache_total_cache_size: Gauge<u64>,
    cache_read_cache_size: Gauge<u64>,
    cache_write_cache_size: Gauge<u64>,
    cache_ram_cache_size: Gauge<u64>,
    cache_hit_rate: Gauge<f64>,
    cache_ram_hit_rate: Gauge<f64>,
    cache_total_requests: Gauge<u64>,
    cache_hits: Gauge<u64>,
    cache_misses: Gauge<u64>,
    cache_evictions: Gauge<u64>,
    cache_write_hits: Gauge<u64>,
    // -- cache per-tier: RAM --
    ram_cache_hits: Gauge<u64>,
    ram_cache_misses: Gauge<u64>,
    ram_cache_evictions: Gauge<u64>,
    ram_cache_max_size: Gauge<u64>,
    // -- cache per-tier: metadata --
    metadata_cache_hits: Gauge<u64>,
    metadata_cache_misses: Gauge<u64>,
    metadata_cache_entries: Gauge<u64>,
    metadata_cache_max_entries: Gauge<u64>,
    metadata_cache_evictions: Gauge<u64>,
    metadata_cache_stale_refreshes: Gauge<u64>,
    // -- cache: bytes saved --
    bytes_served_from_cache: Gauge<u64>,
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
    // -- request_metrics --
    req_total: Gauge<u64>,
    req_successful: Gauge<u64>,
    req_failed: Gauge<u64>,
    req_avg_latency_ms: Gauge<u64>,
    req_per_second: Gauge<f64>,
    // -- top-level --
    uptime_seconds: Gauge<u64>,
    // -- process --
    memory_usage_bytes: Gauge<u64>,
}

impl OtlpExporter {
    pub fn new(config: OtlpConfig) -> Self {
        Self { config, provider: None, instruments: None }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        if !self.config.enabled {
            info!("OTLP metrics export is disabled");
            return Ok(());
        }
        if self.config.endpoint.is_empty() {
            return Err(ProxyError::ConfigError("OTLP endpoint cannot be empty".into()));
        }
        info!("Initializing OTLP exporter: endpoint={}, interval={:?}",
            self.config.endpoint, self.config.export_interval);

        let exporter = opentelemetry_otlp::new_exporter()
            .http()
            .with_endpoint(&self.config.endpoint)
            .with_timeout(self.config.timeout)
            .build_metrics_exporter(
                Box::new(opentelemetry_sdk::metrics::reader::DefaultAggregationSelector::new()),
                Box::new(opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector::new()),
            ).map_err(|e| ProxyError::SystemError(format!("OTLP build failed: {}", e)))?;

        let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(self.config.export_interval)
            .build();

        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(Resource::new(vec![
                KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_NAME, "s3-proxy"),
                KeyValue::new(opentelemetry_semantic_conventions::resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                KeyValue::new(opentelemetry_semantic_conventions::resource::HOST_NAME, hostname),
            ]))
            .build();

        let m = provider.meter("s3-proxy");

        macro_rules! g_u64 { ($name:expr) => { m.u64_gauge($name).init() } }
        macro_rules! g_f64 { ($name:expr) => { m.f64_gauge($name).init() } }

        let instruments = Instruments {
            cache_total_cache_size:  g_u64!("cache.total_cache_size"),
            cache_read_cache_size:   g_u64!("cache.read_cache_size"),
            cache_write_cache_size:  g_u64!("cache.write_cache_size"),
            cache_ram_cache_size:    g_u64!("cache.ram_cache_size"),
            cache_hit_rate:          g_f64!("cache.cache_hit_rate_percent"),
            cache_ram_hit_rate:      g_f64!("cache.ram_cache_hit_rate_percent"),
            cache_total_requests:    g_u64!("cache.total_requests"),
            cache_hits:              g_u64!("cache.cache_hits"),
            cache_misses:            g_u64!("cache.cache_misses"),
            cache_evictions:         g_u64!("cache.evictions"),
            cache_write_hits:        g_u64!("cache.write_cache_hits"),

            ram_cache_hits:          g_u64!("cache.ram_cache_hits"),
            ram_cache_misses:        g_u64!("cache.ram_cache_misses"),
            ram_cache_evictions:     g_u64!("cache.ram_cache_evictions"),
            ram_cache_max_size:      g_u64!("cache.ram_cache_max_size"),

            metadata_cache_hits:           g_u64!("cache.metadata_cache_hits"),
            metadata_cache_misses:         g_u64!("cache.metadata_cache_misses"),
            metadata_cache_entries:        g_u64!("cache.metadata_cache_entries"),
            metadata_cache_max_entries:    g_u64!("cache.metadata_cache_max_entries"),
            metadata_cache_evictions:      g_u64!("cache.metadata_cache_evictions"),
            metadata_cache_stale_refreshes:g_u64!("cache.metadata_cache_stale_refreshes"),

            bytes_served_from_cache: g_u64!("cache.bytes_served_from_cache"),

            compression_ratio:       g_f64!("compression.average_compression_ratio"),
            compression_failures:    g_u64!("compression.failures_total"),

            conn_failed:             g_u64!("connection_pool.failed_connections"),
            conn_latency_ms:         g_u64!("connection_pool.average_latency_ms"),
            conn_success_rate:       g_f64!("connection_pool.success_rate_percent"),

            coalesce_waits:                g_u64!("coalescing.waits_total"),
            coalesce_cache_hits_after_wait:g_u64!("coalescing.cache_hits_after_wait_total"),
            coalesce_timeouts:             g_u64!("coalescing.timeouts_total"),
            coalesce_s3_fetches_saved:     g_u64!("coalescing.s3_fetches_saved_total"),
            coalesce_avg_wait_ms:          g_f64!("coalescing.average_wait_duration_ms"),

            req_total:          g_u64!("request_metrics.total_requests"),
            req_successful:     g_u64!("request_metrics.successful_requests"),
            req_failed:         g_u64!("request_metrics.failed_requests"),
            req_avg_latency_ms: g_u64!("request_metrics.average_response_time_ms"),
            req_per_second:     g_f64!("request_metrics.requests_per_second"),

            uptime_seconds:      g_u64!("uptime_seconds"),
            memory_usage_bytes:  g_u64!("process.memory_usage_bytes"),
        };

        self.provider = Some(provider);
        self.instruments = Some(instruments);
        info!("OTLP metrics exporter initialized");
        Ok(())
    }

    /// Record SystemMetrics into OTLP instruments — same data as /metrics JSON.
    pub async fn export_metrics(&self, metrics: &SystemMetrics) -> Result<()> {
        let i = match &self.instruments { Some(i) => i, None => return Ok(()) };
        let a = &[];  // no extra attributes — host/service come from Resource

        if let Some(c) = &metrics.cache {
            i.cache_total_cache_size.record(c.total_cache_size, a);
            i.cache_read_cache_size.record(c.read_cache_size, a);
            i.cache_write_cache_size.record(c.write_cache_size, a);
            i.cache_ram_cache_size.record(c.ram_cache_size, a);
            i.cache_hit_rate.record(c.cache_hit_rate_percent as f64, a);
            i.cache_ram_hit_rate.record(c.ram_cache_hit_rate_percent as f64, a);
            i.cache_total_requests.record(c.total_requests, a);
            i.cache_hits.record(c.cache_hits, a);
            i.cache_misses.record(c.cache_misses, a);
            i.cache_evictions.record(c.evictions, a);
            i.cache_write_hits.record(c.write_cache_hits, a);
            i.ram_cache_hits.record(c.ram_cache_hits, a);
            i.ram_cache_misses.record(c.ram_cache_misses, a);
            i.ram_cache_evictions.record(c.ram_cache_evictions, a);
            i.ram_cache_max_size.record(c.ram_cache_max_size, a);
            i.metadata_cache_hits.record(c.metadata_cache_hits, a);
            i.metadata_cache_misses.record(c.metadata_cache_misses, a);
            i.metadata_cache_entries.record(c.metadata_cache_entries, a);
            i.metadata_cache_max_entries.record(c.metadata_cache_max_entries, a);
            i.metadata_cache_evictions.record(c.metadata_cache_evictions, a);
            i.metadata_cache_stale_refreshes.record(c.metadata_cache_stale_refreshes, a);
            i.bytes_served_from_cache.record(c.bytes_served_from_cache, a);
        }

        if let Some(comp) = &metrics.compression {
            i.compression_ratio.record(comp.average_compression_ratio as f64, a);
            i.compression_failures.record(comp.compression_failures + comp.decompression_failures, a);
        }

        if let Some(conn) = &metrics.connection_pool {
            i.conn_failed.record(conn.failed_connections, a);
            i.conn_latency_ms.record(conn.average_latency_ms, a);
            i.conn_success_rate.record(conn.success_rate_percent as f64, a);
        }

        if let Some(coal) = &metrics.coalescing {
            i.coalesce_waits.record(coal.waits_total, a);
            i.coalesce_cache_hits_after_wait.record(coal.cache_hits_after_wait_total, a);
            i.coalesce_timeouts.record(coal.timeouts_total, a);
            i.coalesce_s3_fetches_saved.record(coal.s3_fetches_saved_total, a);
            i.coalesce_avg_wait_ms.record(coal.average_wait_duration_ms, a);
        }

        let rm = &metrics.request_metrics;
        i.req_total.record(rm.total_requests, a);
        i.req_successful.record(rm.successful_requests, a);
        i.req_failed.record(rm.failed_requests, a);
        i.req_avg_latency_ms.record(rm.average_response_time_ms, a);
        i.req_per_second.record(rm.requests_per_second as f64, a);

        i.uptime_seconds.record(metrics.uptime_seconds, a);

        if let Ok(mem) = self.get_memory_usage() {
            i.memory_usage_bytes.record(mem, a);
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
                        return parts[1].parse::<u64>().map(|kb| kb * 1024)
                            .map_err(|e| ProxyError::SystemError(format!("parse VmRSS: {}", e)));
                    }
                }
            }
            Err(ProxyError::SystemError("VmRSS not found".into()))
        }
        #[cfg(not(target_os = "linux"))]
        { Ok(0) }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(provider) = self.provider.take() {
            info!("Shutting down OTLP exporter");
            provider.shutdown().map_err(|e| ProxyError::SystemError(format!("OTLP shutdown: {}", e)))?;
            self.instruments = None;
        }
        Ok(())
    }
}
