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
    cache_max_cache_size_limit: Gauge<u64>,
    cache_read_cache_size: Gauge<u64>,
    cache_read_cache_size_inverted_secs: Gauge<u64>,
    cache_write_cache_size: Gauge<u64>,
    cache_ram_cache_size: Gauge<u64>,
    // -- cache: hits/misses/evictions --
    cache_hits: Gauge<u64>,
    cache_misses: Gauge<u64>,
    cache_evictions: Gauge<u64>,
    cache_s3_requests_saved: Gauge<u64>,
    // -- cache: bytes saved --
    bytes_served_from_cache: Gauge<u64>,
    // -- cache per-tier: RAM --
    ram_cache_hits: Gauge<u64>,
    ram_cache_misses: Gauge<u64>,
    ram_cache_evictions: Gauge<u64>,
    // Admissions declined for `ram_cache_eligible: false`: tells a closed gate from an
    // ineffective tier, which `ram_cache_hits`/`ram_cache_misses` alone cannot.
    ram_cache_eligibility_skips_total: Gauge<u64>,
    // A level: entries resident in the RAM tier, so an OTLP-only deployment can see one-entry-
    // per-extent residency and an invalidation-driven drop that `ram_cache_evictions` misses.
    ram_cache_entries: Gauge<u64>,
    // -- cache per-tier: metadata --
    metadata_cache_hits: Gauge<u64>,
    metadata_cache_misses: Gauge<u64>,
    metadata_cache_entries: Gauge<u64>,
    metadata_cache_max_entries: Gauge<u64>,
    metadata_cache_evictions: Gauge<u64>,
    metadata_cache_stale_refreshes: Gauge<u64>,
    metadata_cache_head_hits: Gauge<u64>,
    metadata_cache_head_misses: Gauge<u64>,
    metadata_cache_head_disk_hits: Gauge<u64>,
    metadata_cache_disk_hits: Gauge<u64>,
    // -- cache: error/health signals --
    cache_corruption_metadata_total: Gauge<u64>,
    cache_corruption_missing_range_total: Gauge<u64>,
    cache_disk_full_events_total: Gauge<u64>,
    cache_metadata_lock_retry_exhausted_total: Gauge<u64>,
    cache_write_stamp_retried_total: Gauge<u64>,
    cache_write_stamp_retry_landed_total: Gauge<u64>,
    cache_write_stamp_retry_overflow_keys: Gauge<u64>,
    cache_write_stamp_retry_active_keys: Gauge<u64>,
    cache_write_stamp_retry_abandoned_total: Gauge<u64>,
    cache_write_stamp_log_pending_keys: Gauge<u64>,
    cache_write_stamp_log_replayed_total: Gauge<u64>,
    cache_write_stamp_log_torn_records_total: Gauge<u64>,
    cache_write_stamp_log_write_failures_total: Gauge<u64>,
    cache_write_stamp_log_memory_only: Gauge<u64>,
    cache_write_stamp_pending_declined_total: Gauge<u64>,
    cache_volume_stall_breaker_open: Gauge<u64>,
    cache_volume_stall_breaker_trips_total: Gauge<u64>,
    cache_tee_abandoned_total_put: Gauge<u64>,
    cache_metadata_heal_overwrite_total: Gauge<u64>,
    cache_upstream_idle_abort_total_mid_stream: Gauge<u64>,
    cache_upstream_idle_abort_total_pre_stream: Gauge<u64>,
    cache_upstream_idle_retry_total: Gauge<u64>,

    cache_metadata_read_stale_handles_total: Gauge<u64>,
    cache_metadata_read_stale_handle_misses_total: Gauge<u64>,
    cache_etag_mismatches_total: Gauge<u64>,
    cache_range_invalidations_total: Gauge<u64>,
    cache_incomplete_uploads_evicted: Gauge<u64>,
    // -- cache: capacity-authority divergence gauges (observability only) --
    cache_consumed_bytes: Gauge<u64>,
    // -- cache: the danger-band capacity guard (per-instance) --
    cache_capacity_guard_declines_total: Gauge<u64>,
    cache_capacity_guard_last_free_bytes: Gauge<u64>,
    cache_capacity_guard_floor_bytes: Gauge<u64>,
    cache_read_fills_declined_max_cache_size_total: Gauge<u64>,
    cache_hostname_collisions_total: Gauge<u64>,
    // -- staged (write) tier: § 16.6.7's operator signals --
    //
    // These were `/metrics`-only. An operator running the proxy as a managed service alarms from
    // the OTLP pipeline, and the budget's decline is a silent capability reduction, so the two
    // fields that say "the cache is about to stop accepting write-through data" had no route to
    // an alarm at all.
    staged_range_count: Gauge<u64>,
    staged_range_budget: Gauge<u64>,
    staged_range_budget_percent: Gauge<f64>,
    staged_range_budget_declines_total: Gauge<u64>,
    staged_bytes: Gauge<u64>,
    cache_in_progress_part_bytes: Gauge<u64>,
    cache_in_progress_part_bytes_age_secs: Gauge<u64>,
    cache_in_progress_part_bytes_drift_bytes: Gauge<i64>,
    staged_shard_authority_age_secs: Gauge<u64>,
    staged_max_authority_age_secs: Gauge<u64>,
    staged_shards_beyond_max_age: Gauge<u64>,
    staged_fold_incomplete_total: Gauge<u64>,
    staged_fold_shard_budget_capped_total: Gauge<u64>,
    staged_fold_dirty_shards: Gauge<u64>,
    staged_fold_shard_duration_count: Gauge<u64>,
    staged_fold_shard_duration_sum_ms: Gauge<u64>,
    staged_fold_shard_duration_bucket: Gauge<u64>,
    staged_authority_epoch: Gauge<u64>,
    write_cache_setup_abandoned_total_put: Gauge<u64>,
    write_cache_mpu_completion_ledger_declines_total: Gauge<u64>,
    pending_lock_waits_total: Gauge<u64>,
    pending_lock_wait_ms_total: Gauge<u64>,
    pending_lock_timeouts_total: Gauge<u64>,
    staged_authority_snapshot_hits_total: Gauge<u64>,
    staged_authority_snapshot_recomputes_total: Gauge<u64>,
    ledger_repair_rotation_reachable: Gauge<u64>,
    ledger_repair_projected_rotation_secs: Gauge<u64>,
    ledger_repair_shard_budget_capped_total: Gauge<u64>,
    ledger_repair_ranges_reappended_total: Gauge<u64>,
    // -- orphan_reclaim: the orphan range-file rotation (R15.11) --
    //
    // Per process. `enabled` is exported as 1/0 so a zero `reclaimed` can be read as "off" rather
    // than "idle". `journal_evidence_total` is one series per `result`, a closed set of four, all
    // recorded every export so none goes missing at zero. The two rotation gauges are recorded
    // only once a pass has read the checkpoint set.
    orphan_reclaim_enabled: Gauge<u64>,
    orphan_reclaim_found: Gauge<u64>,
    orphan_reclaim_reclaimed: Gauge<u64>,
    orphan_reclaim_bytes_reclaimed: Gauge<u64>,
    orphan_reclaim_skipped_unconfirmed: Gauge<u64>,
    orphan_reclaim_kept_listed: Gauge<u64>,
    orphan_reclaim_journal_evidence_total: Gauge<u64>,
    orphan_reclaim_passes: Gauge<u64>,
    orphan_reclaim_units_visited: Gauge<u64>,
    orphan_reclaim_passes_gated: Gauge<u64>,
    orphan_reclaim_leases_unreadable: Gauge<u64>,
    orphan_reclaim_leases_unreadable_expired: Gauge<u64>,
    orphan_reclaim_stalest_checkpoint_age_secs: Gauge<u64>,
    orphan_reclaim_units_never_scanned: Gauge<u64>,
    // -- write_ledger: the append stages' batching counters (design § 16.6.14) --
    write_ledger_append_batches_total: Gauge<u64>,
    write_ledger_append_batched_entries_total: Gauge<u64>,
    write_ledger_pending_batches_total: Gauge<u64>,
    write_ledger_pending_batch_failures_total: Gauge<u64>,
    // -- eviction_worklist: the eviction work list, per process --
    //
    // `chunks_claimed_total`, `items_refused_total` and `author_fallback_total` are one series per
    // `outcome` / `reason`, closed sets recorded every export so none goes missing at zero.
    eviction_worklist_generation: Gauge<u64>,
    eviction_worklist_chunks_authored_total: Gauge<u64>,
    eviction_worklist_chunks_claimed_total: Gauge<u64>,
    eviction_worklist_chunks_done_total: Gauge<u64>,
    eviction_worklist_chunks_unusable_total: Gauge<u64>,
    eviction_worklist_items_executed_total: Gauge<u64>,
    eviction_worklist_items_refused_total: Gauge<u64>,
    eviction_worklist_lease_takeovers_total: Gauge<u64>,
    eviction_worklist_author_fallback_total: Gauge<u64>,
    eviction_worklist_in_flight_bytes: Gauge<u64>,
    // -- scanner-concurrency limiter, `[fold, repair]` --
    scanner_concurrency_k: [Gauge<u64>; 2],
    scanner_concurrency_observed: [Gauge<u64>; 2],
    scanner_samples_discarded_total: [Gauge<u64>; 2],
    scan_deferred_total: [Gauge<u64>; 2],
    scanner_last_adjust_reason: [Gauge<u64>; 2],
    scanner_rate_by_level: [Gauge<f64>; 2],
    scanner_samples_by_level: [Gauge<u64>; 2],
    scanner_aggregate_estimate_by_level: [Gauge<f64>; 2],
    // -- eviction: R12.1's pass figures --
    //
    // Added with the fields they export, deliberately, rather than deferred: this exporter selects
    // per field by hand, so a new `/metrics` key is invisible to an OTLP-only deployment until an
    // instrument is written for it. Four units each deferring that is how a three-field residual
    // became a 22-field one.
    cache_eviction_passes_total: Gauge<u64>,
    cache_eviction_last_result_duration_ms: Gauge<u64>,
    eviction_coordination_staging_evictions_skipped_lock_held: Gauge<u64>,
    // -- eviction: R12.3's rates, the admission rates they race, the verdict, the backlog --
    //
    // Same reasoning as the block above: a field with no instrument here is invisible to an
    // OTLP-only deployment, and R2.3 requires eviction falling behind to be *observable*.
    // Exporting the rates without their numerators and denominator would make the fleet figure
    // uncomputable, since every rate is per-instance and only the counters can be summed.
    cache_eviction_shed_bytes_total: Gauge<u64>,
    cache_eviction_shed_ranges_total: Gauge<u64>,
    cache_eviction_active_micros_total: Gauge<u64>,
    cache_eviction_shed_bytes_per_sec: Gauge<f64>,
    cache_eviction_shed_ranges_per_sec: Gauge<f64>,
    cache_admission_bytes_total: Gauge<u64>,
    cache_admission_ranges_total: Gauge<u64>,
    cache_admission_bytes_per_sec: Gauge<f64>,
    cache_admission_ranges_per_sec: Gauge<f64>,
    cache_eviction_keeping_pace: Gauge<u64>,
    cache_eviction_backlog_bytes: Gauge<i64>,
    cache_eviction_backlog_projected_bytes: Gauge<i64>,
    // -- st_size fold: the ONLY signal for the fold's silent failure --
    //
    // `st_size_folds_declined_overlap_total` is first here because it is the one figure on this
    // exporter that has no substitute. A fold declined on every scheduling tick returns a normal,
    // non-error outcome and logs at `debug`, so an OTLP-only deployment — which sees no logs at
    // all — could not see the pass failing through any other instrument: the duration and the scan
    // age both go stale silently, and staleness is indistinguishable from an idle cache.
    cache_st_size_folds_declined_overlap_total: Gauge<u64>,
    cache_st_size_fold_last_duration_secs: Gauge<u64>,
    cache_st_size_fold_oldest_shard_scan_age_secs: Gauge<u64>,
    // -- eviction: the Tail_Partition mechanism, build side --
    //
    // `tail_floor_bytes` is the one to read first when partitions look empty: a floor above the
    // cache's whole size distribution produces empty partitions, and an empty partition is
    // indistinguishable from an idle cache on every other series here.
    cache_tail_floor_bytes: Gauge<u64>,
    cache_tail_max_entries: Gauge<u64>,
    cache_tail_rebuilds_total: Gauge<u64>,
    cache_tail_entries_last_rebuild: Gauge<u64>,
    cache_tail_effective_min_bytes: Gauge<u64>,
    cache_tail_delta_records: Gauge<u64>,
    // -- eviction: the Tail_Partition mechanism, selection side --
    //
    // The three fallback counters are what say whether the bounded path is being used at all. A
    // deployment whose partitions have never been built falls back on every pass and looks
    // identical to one using the bounded path on every other series;
    // `eviction_fallback_unvisited_total` is the one that distinguishes them.
    cache_tail_coverage_ratio: Gauge<f64>,
    cache_tail_coverage_short_total: Gauge<u64>,
    cache_eviction_fallback_tail_empty_total: Gauge<u64>,
    cache_eviction_fallback_cap_exhausted_total: Gauge<u64>,
    cache_eviction_fallback_unvisited_total: Gauge<u64>,
    cache_eviction_oversized_single_candidate_total: Gauge<u64>,
    cache_tail_delta_cap_rewrites_total: Gauge<u64>,
    // One series per verdict reason, carried as a `reason` attribute — the `class_bytes` pattern.
    // The JSON side is a map precisely because the reason set is data-dependent rather than fixed
    // at compile time, and a map is the one shape that survives to OTLP with its dimension intact.
    cache_eviction_tail_verdict_total: Gauge<u64>,
    // -- eviction: the executor --
    //
    // `eviction_meta_rewrites_total` should equal `eviction_pass_path_partial_total`. A rewrite
    // count above the partial count is the only signal that the whole-object path has stopped
    // taking its shortcut, because a rewrite-then-delete leaves no `.meta` just as a bare delete
    // does — every other series reads identically either way.
    cache_eviction_pass_path_whole_object_total: Gauge<u64>,
    cache_eviction_pass_path_partial_total: Gauge<u64>,
    cache_eviction_whole_object_downgraded_total: Gauge<u64>,
    cache_eviction_unlink_failures_total: Gauge<u64>,
    cache_eviction_meta_rewrites_total: Gauge<u64>,
    cache_eviction_active_key_journal_reads_total: Gauge<u64>,
    cache_eviction_oracle_refreshes_total: Gauge<u64>,
    cache_eviction_oracle_stale_verdicts_total: Gauge<u64>,
    cache_eviction_oracle_last_walk_ms: Gauge<u64>,
    // -- eviction: the read tier's last pass, the shortfall decomposition (R12.1) --
    //
    // The write tier's equivalent document is already exported field-by-field as
    // `write_cache.count_pass_last_result.*`; the read tier's was not, though it is the same shape
    // and `duration_ms` above was already taking a sub-field from it. That asymmetry is what this
    // block closes.
    //
    // **These are FLEET-WIDE**, read from a shared-volume document that any proxy may write, so
    // every instance reports the same values. Aggregate with last-value or max, never sum: a sum
    // across a three-proxy fleet triples a figure describing one pass.
    //
    // `schema` and `instance_id` are deliberately absent. A format version is not a measurement,
    // and an instance identifier is a string — it belongs in an attribute, not in a gauge value.
    cache_eviction_last_result_completed_at: Gauge<u64>,
    cache_eviction_last_result_bytes_requested: Gauge<u64>,
    cache_eviction_last_result_bytes_freed: Gauge<u64>,
    cache_eviction_last_result_shortfall_bytes: Gauge<u64>,
    cache_eviction_last_result_unevictable_in_progress_bytes: Gauge<u64>,
    cache_eviction_last_result_unexplained_shortfall_bytes: Gauge<u64>,
    cache_eviction_last_result_reached_target: Gauge<u64>,
    cache_eviction_last_result_ranges_evicted: Gauge<u64>,
    // -- write_cache: the count-driven pass's last result (R12.1) --
    count_pass_completed_at: Gauge<u64>,
    count_pass_count_budget: Gauge<u64>,
    count_pass_count_trigger: Gauge<u64>,
    count_pass_count_target: Gauge<u64>,
    count_pass_count_at_start: Gauge<u64>,
    count_pass_observed_count_at_start: Gauge<u64>,
    count_pass_ranges_requested: Gauge<u64>,
    count_pass_ranges_freed: Gauge<u64>,
    count_pass_shortfall_ranges: Gauge<u64>,
    count_pass_unevictable_unreadable_ranges: Gauge<u64>,
    count_pass_unexplained_shortfall_ranges: Gauge<u64>,
    count_pass_reached_target: Gauge<u64>,
    count_pass_objects_evicted: Gauge<u64>,
    count_pass_candidates_considered: Gauge<u64>,
    count_pass_reclaimable_candidates: Gauge<u64>,
    count_pass_expired_candidates: Gauge<u64>,
    count_pass_passes_total: Gauge<u64>,
    count_pass_ranges_freed_total: Gauge<u64>,
    count_pass_admission_declines_total: Gauge<u64>,
    /// `shortfall_reason` as a labelled series rather than a flat field per reason: the reason set
    /// is closed but it is a *label* on one figure, and a map-shaped export keeps the dimension
    /// that a flattened field would destroy on both paths permanently.
    count_pass_shortfall_reason: Gauge<u64>,
    // -- cache_size: R12.6-R12.8's accounting-drift gauges --
    //
    // Each carries a `figure` attribute naming the accounted population, for the same reason
    // `download_bandwidth.class_bytes` does: the value set is closed, but it is genuinely a label
    // on one measurement, and an attribute survives to the consumer where three flattened field
    // names would not be recoverable as one series.
    accounting_drift_sampled_at: Gauge<u64>,
    accounting_drift_persisted: Gauge<u64>,
    accounting_drift_reference: Gauge<u64>,
    accounting_drift_drift: Gauge<i64>,
    accounting_drift_ratio: Gauge<f64>,
    accounting_drift_unevaluable: Gauge<u64>,
    accounting_drift_sample_kind: Gauge<u64>,
    // -- compression --
    compression_ratio: Gauge<f64>,
    compression_failures: Gauge<u64>,
    // -- connection_pool: deliberately NO instruments --
    //
    // `failed_connections`, `average_latency_ms` and `success_rate_percent` were exported here
    // until 3.0.0, and all three were literals at their only production construction site
    // (`MetricsManager::collect_connection_pool_metrics`: `0`, `0`, `1.0`). Nothing in the
    // connector or the S3 client counts connection failures cumulatively or times connections —
    // `IpHealthTracker` keeps a per-IP CONSECUTIVE count that a success resets — so there is no
    // source to measure them from. A constant exported as a gauge is worse than a gap:
    // `success_rate_percent: 1.0` reads as 1% on a percent panel and can never change. 3.0.0 also
    // removes the first two from `/metrics`; `success_rate_percent` stays there, unchanged.
    // `connection_pool_placeholders_are_not_exported` fails if one is re-added while still a
    // literal, and fails the other way once one is measured.
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
    req_access_log_records_dropped: Gauge<u64>,
    // -- metrics_collection: what the background collection costs --
    //
    // The collection these measure is the SAME background collection that records these
    // instruments, once per `metrics.collection_interval`. A slow one makes every OTLP sample
    // late and makes the shared-volume figures on `/metrics` older, so an operator on OTLP alone
    // needs these to see the stall at all.
    //
    // `volume_snapshot` deliberately has no instruments: this export runs at the end of the
    // collection it describes, so its age is always 0 and its collection time is the sample's own
    // timestamp. It carries information only on a `/metrics` response served between collections.
    metrics_collections_total: Gauge<u64>,
    metrics_collection_duration_ms_last: Gauge<u64>,
    metrics_collection_duration_ms_max: Gauge<u64>,
    metrics_collection_slow_total: Gauge<u64>,
    metrics_collection_slow_threshold_ms: Gauge<u64>,
    // -- cache_hit_buffer: the journal write-behind queue --
    //
    // The section shipped `/metrics`-only, so an OTLP-only deployment could not see any of the
    // four figures — including `flush_errors_total`, the one the section exists to alarm on: the
    // entries it counts carry the TTL refreshes and access counts eviction ranks candidates by, so
    // losing them silently skews eviction toward objects that are in fact hot.
    //
    // `entries_flushed_total` is the one to read first when `flush_errors_total` is flat: a flush
    // task that has stopped running produces no errors at all, so a flat error count and a flat
    // flushed count together mean the opposite of healthy, and no other series here distinguishes
    // them.
    cache_hit_buffer_pending_updates: Gauge<u64>,
    cache_hit_buffer_updates_recorded_total: Gauge<u64>,
    cache_hit_buffer_entries_flushed_total: Gauge<u64>,
    cache_hit_buffer_flush_errors_total: Gauge<u64>,
    cache_hit_buffer_ram_records_dropped_total: Gauge<u64>,
    // -- fill_visibility: streamed fills servable before consolidation --
    //
    // Per-instance, and `Option`: recorded only when a cache manager is wired in, so an absent
    // section is a gap rather than ten zeroes. `dropped_age_total` and `dropped_cap_total` are the
    // two to alarm on; both mean cache hits lost, never a wrong answer.
    fill_visibility_entries: Gauge<u64>,
    fill_visibility_bytes: Gauge<u64>,
    fill_visibility_recorded_total: Gauge<u64>,
    fill_visibility_hits_total: Gauge<u64>,
    fill_visibility_dropped_disk_meta_total: Gauge<u64>,
    fill_visibility_dropped_folded_total: Gauge<u64>,
    fill_visibility_dropped_invalidated_total: Gauge<u64>,
    fill_visibility_dropped_cap_total: Gauge<u64>,
    fill_visibility_dropped_age_total: Gauge<u64>,
    fill_visibility_dropped_range_file_changed_total: Gauge<u64>,
    // -- inflight_memory: the in-flight buffered-byte ledger --
    //
    // `rejected_total` is the reason this block exists: it is the only count of requests shed
    // because buffered bytes hit `server.max_inflight_buffer_bytes`, and no other series on this
    // exporter carries it — `request_metrics` exports no rejection count at all. Read it with
    // `ceiling_bytes`: `0` there means the ledger is disabled, which is the one state in which
    // every other figure here is a true zero rather than an idle one.
    //
    // Monotone since process start: `rejected_total`, `aborted_accumulations_total`, and
    // `peak_reserved_bytes` (a high-water mark). Levels: `reserved_bytes`. Configuration in force:
    // `ceiling_bytes` and `ram_cache_max_bytes`, exported for the reason `cache.tail_floor_bytes`
    // is — a numerator without its denominator cannot be read as saturation. All six are gauges,
    // like every `_total` on this exporter: the export records a cumulative snapshot, and an OTLP
    // counter takes increments.
    inflight_memory_reserved_bytes: Gauge<u64>,
    inflight_memory_ceiling_bytes: Gauge<u64>,
    inflight_memory_peak_reserved_bytes: Gauge<u64>,
    inflight_memory_rejected_total: Gauge<u64>,
    inflight_memory_aborted_accumulations_total: Gauge<u64>,
    // Monotone grant count and bytes: the figures a fleet assertion can take a delta of, where a
    // sample of `reserved_bytes` can miss a brief reservation.
    inflight_memory_granted_total: Gauge<u64>,
    inflight_memory_granted_bytes_total: Gauge<u64>,
    inflight_memory_ram_cache_max_bytes: Gauge<u64>,
    inflight_memory_buffered_serve_sliced_streams_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_chained_stream_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_forward_gap_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_forward_partial_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_forward_unresolved_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_forward_stream_failed_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_forward_partial_cache_merge_total: Gauge<u64>,
    inflight_memory_buffered_serve_degraded_merge_backstop_total: Gauge<u64>,
    inflight_memory_buffered_serve_size_le_64kib_total: Gauge<u64>,
    inflight_memory_buffered_serve_size_le_1mib_total: Gauge<u64>,
    inflight_memory_buffered_serve_size_le_8mib_total: Gauge<u64>,
    inflight_memory_buffered_serve_size_gt_8mib_total: Gauge<u64>,
    inflight_memory_buffered_serve_peak_materialised_bytes: Gauge<u64>,
    inflight_memory_buffered_serve_full_object_total: Gauge<u64>,
    // -- atomic_metadata: the metadata slot lock, corrupt range files, discarded journal credits --
    //
    // All nine of the section's fields. The four slot-lock figures are
    // read from `metadata_lock_manager`'s own atomics and move on every slot-lock acquisition;
    // `wait_budget_expired_total` and `acquisitions_failed` are the failure signals, and the
    // first has no substitute series (`cache.metadata_lock_retry_exhausted_total` is the
    // retry-bounded path, not the time-bounded one). `range_file_corruption_detected`, the two
    // range-file heal counters, `range_file_identity_mismatch_detected` and
    // `journal_validation_failures` are module atomics in `disk_cache` and
    // `journal_consolidator`. All nine are monotone since process start.
    atomic_metadata_lock_acquisitions_successful: Gauge<u64>,
    atomic_metadata_lock_acquisitions_failed: Gauge<u64>,
    atomic_metadata_lock_hold_time_ms_total: Gauge<u64>,
    atomic_metadata_lock_wait_budget_expired_total: Gauge<u64>,
    atomic_metadata_range_file_corruption_detected: Gauge<u64>,
    atomic_metadata_range_file_corruption_healed: Gauge<u64>,
    atomic_metadata_range_file_corruption_heal_suppressed: Gauge<u64>,
    atomic_metadata_range_file_identity_mismatch_detected: Gauge<u64>,
    atomic_metadata_journal_validation_failures: Gauge<u64>,
    // -- consolidation: the shared size state the consolidator last wrote --
    //
    // FLEET-WIDE: all four are read from `size_tracking/size_state.json` on the shared volume,
    // so every instance reports the same values. Aggregate with last-value or max, never sum.
    //
    // `last_consolidation_timestamp` (Unix seconds) is the one to alarm on: `now` minus it is the
    // consolidator's staleness, and a stalled consolidator is otherwise invisible because the two
    // sizes simply stop moving, which is what an idle cache looks like too.
    // `consolidation_count` is monotone; the two sizes are levels. They are the raw size-state
    // figures, not `cache.total_cache_size`/`cache.write_cache_size`, which resolve through the
    // staged-authority epoch and can legitimately differ from these after activation.
    consolidation_cache_size_bytes: Gauge<u64>,
    consolidation_cache_write_size_bytes: Gauge<u64>,
    consolidation_consolidation_count: Gauge<u64>,
    consolidation_last_consolidation_timestamp: Gauge<u64>,
    // Per-instance, unlike the four above: seconds this proxy has held the global consolidation
    // lock, 0 when it does not. A value climbing past the cycle timeout is a stuck holder.
    consolidation_consolidation_lock_held_secs: Gauge<u64>,
    // Per-instance and monotone since process start: watchdog releases of a consolidation lock
    // held past its threshold.
    consolidation_consolidation_lock_takeovers: Gauge<u64>,
    // -- hedged_requests: duplicate upstream requests --
    //
    // `issued` is the duplicate-S3-request bill, and before these existed an OTLP-only operator
    // could not see what hedging was costing at all. All three are monotone since process start
    // and per-instance; `won / issued` and `suppressed / (issued + suppressed)` are the ratios
    // `docs/HEDGING.md` names, which is why the three are exported together.
    hedged_requests_issued: Gauge<u64>,
    hedged_requests_won: Gauge<u64>,
    hedged_requests_suppressed: Gauge<u64>,
    // -- page_cache: page-aligned range widening --
    //
    // Seven monotone per-instance counters since process start, and the derived
    // `amplification_ratio`. `bytes_prefetched` is the extra S3 transfer widening costs, and
    // `fallbacks` the widened fetches that failed back to the client's own range.
    page_cache_widened_requests: Gauge<u64>,
    page_cache_bytes_prefetched: Gauge<u64>,
    page_cache_amplification_ratio: Gauge<f64>,
    page_cache_page_hits: Gauge<u64>,
    page_cache_skipped_signed_range: Gauge<u64>,
    page_cache_fallbacks: Gauge<u64>,
    page_cache_ram_page_promotions: Gauge<u64>,
    page_cache_ram_page_promotion_skipped: Gauge<u64>,
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

/// The OTLP/HTTP signal path for metrics, from the OTLP exporter specification.
///
/// `opentelemetry-otlp` 0.32 has the same value but not as a public constant: it passes the
/// literal `"/v1/metrics"` to its private `build_client`, and appends it only when the endpoint
/// comes from `OTEL_EXPORTER_OTLP_ENDPOINT` or the built-in default.
const OTLP_HTTP_METRICS_PATH: &str = "/v1/metrics";

/// The endpoint the exporter is actually given for `metrics.otlp.endpoint`.
///
/// `opentelemetry-otlp` uses an endpoint passed to `with_endpoint` **verbatim** — it appends no
/// signal path (`resolve_http_endpoint` in `src/exporter/http/mod.rs`). The shipped default and
/// every documented example are bare `scheme://host:port` URLs, so without this the exporter
/// POSTs to `/`, which neither the CloudWatch agent nor an OpenTelemetry Collector serves, and
/// export fails on every interval.
///
/// The rule: **a URL whose path is empty or exactly `/` gets [`OTLP_HTTP_METRICS_PATH`]; any
/// other path is used as written**, so a full `…/v1/metrics` URL and a custom collector path such
/// as `/api/v1/otlp/v1/metrics` are untouched. A query string is kept after the added path.
/// Anything that does not parse as an absolute URL with a scheme and host is returned unchanged,
/// so the exporter build reports it exactly as it did before this function existed.
///
/// Normalised here, at exporter build, rather than at config load, because the endpoint reaches
/// the config from three layers (YAML, `OTLP_ENDPOINT`, `--otlp-endpoint`) and this is the one
/// place all three pass through; the startup log reports the result, so the effective URL is
/// still visible to an operator.
pub(crate) fn effective_metrics_endpoint(configured: &str) -> String {
    let Ok(uri) = configured.parse::<hyper::Uri>() else {
        return configured.to_string();
    };
    if uri.scheme().is_none() || uri.authority().is_none() || uri.path() != "/" {
        return configured.to_string();
    }
    let path_and_query = match uri.query() {
        Some(q) => format!("{OTLP_HTTP_METRICS_PATH}?{q}"),
        None => OTLP_HTTP_METRICS_PATH.to_string(),
    };
    let mut parts = uri.into_parts();
    let Ok(pq) = path_and_query.parse() else {
        return configured.to_string();
    };
    parts.path_and_query = Some(pq);
    match hyper::Uri::from_parts(parts) {
        Ok(normalised) => normalised.to_string(),
        Err(_) => configured.to_string(),
    }
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
        let endpoint = effective_metrics_endpoint(&self.config.endpoint);
        if endpoint != self.config.endpoint {
            info!(
                "OTLP endpoint had no path; using {} (configured: {})",
                endpoint, self.config.endpoint
            );
        }
        info!(
            "Initializing OTLP exporter: endpoint={}, interval={:?}",
            endpoint, self.config.export_interval
        );

        let exporter = MetricExporter::builder()
            .with_http()
            .with_endpoint(&endpoint)
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
                        // Prometheus's OTLP receiver derives `job` from service.name
                        // and `instance` from service.instance.id only; without the
                        // id every instance's series collide into one.
                        KeyValue::new(
                            opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                            hostname.clone(),
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
        // A signed gauge, for the accounting drift. The sign is the whole reading: a negative
        // drift means the persisted counter was over-stating, which is the direction that makes a
        // cache evict when it need not, and an unsigned gauge would have to throw it away.
        macro_rules! g_i64 {
            ($name:expr) => {
                m.i64_gauge($name).build()
            };
        }

        let instruments = Instruments {
            // cache: sizes
            cache_total_cache_size: g_u64!("cache.total_cache_size"),
            cache_max_cache_size_limit: g_u64!("cache.max_cache_size_limit"),
            cache_read_cache_size: g_u64!("cache.read_cache_size"),
            cache_read_cache_size_inverted_secs: g_u64!("cache.read_cache_size_inverted_secs"),
            cache_write_cache_size: g_u64!("cache.write_cache_size"),
            cache_ram_cache_size: g_u64!("cache.ram_cache_size"),
            // cache: hits/misses/evictions
            cache_hits: g_u64!("cache.cache_hits"),
            cache_misses: g_u64!("cache.cache_misses"),
            cache_evictions: g_u64!("cache.evictions"),
            cache_s3_requests_saved: g_u64!("cache.s3_requests_saved"),
            // cache: bytes saved
            bytes_served_from_cache: g_u64!("cache.bytes_served_from_cache"),
            // cache per-tier: RAM
            ram_cache_hits: g_u64!("cache.ram_cache_hits"),
            ram_cache_misses: g_u64!("cache.ram_cache_misses"),
            ram_cache_evictions: g_u64!("cache.ram_cache_evictions"),
            ram_cache_eligibility_skips_total: g_u64!("cache.ram_cache_eligibility_skips_total"),
            ram_cache_entries: g_u64!("cache.ram_cache_entries"),
            // cache per-tier: metadata
            metadata_cache_hits: g_u64!("cache.metadata_cache_hits"),
            metadata_cache_misses: g_u64!("cache.metadata_cache_misses"),
            metadata_cache_entries: g_u64!("cache.metadata_cache_entries"),
            // The capacity, exported so an OTLP-only deployment can compute the tier's saturation
            // at all — `entries` alone is a numerator with no denominator. It was the only one of
            // the six `metadata_cache_*` fields with no instrument, and exporting it is what lets
            // `every_metadata_cache_field_has_an_otlp_instrument` carry no exclusion list, which
            // matters because an exclusion list is exactly where the next unexported field lands
            // silently. `cache.max_cache_size_limit` is the precedent: a capacity, exported.
            // `cache.ram_cache_max_size` remains unexported and is a separate pre-existing gap.
            metadata_cache_max_entries: g_u64!("cache.metadata_cache_max_entries"),
            metadata_cache_evictions: g_u64!("cache.metadata_cache_evictions"),
            metadata_cache_stale_refreshes: g_u64!("cache.metadata_cache_stale_refreshes"),
            // The HEAD path's own three. Raw counters and deliberately not a rate — see
            // `CacheMetrics::metadata_cache_head_hits` for why a `*_rate_percent` field is the
            // precedent not to follow here.
            metadata_cache_head_hits: g_u64!("cache.metadata_cache_head_hits"),
            metadata_cache_head_misses: g_u64!("cache.metadata_cache_head_misses"),
            metadata_cache_head_disk_hits: g_u64!("cache.metadata_cache_head_disk_hits"),
            // The non-HEAD disk reads after a RAM miss; see `CacheMetrics::metadata_cache_disk_hits`.
            metadata_cache_disk_hits: g_u64!("cache.metadata_cache_disk_hits"),
            // cache: error/health signals
            cache_corruption_metadata_total: g_u64!("cache.corruption_metadata_total"),
            cache_corruption_missing_range_total: g_u64!("cache.corruption_missing_range_total"),
            cache_disk_full_events_total: g_u64!("cache.disk_full_events_total"),
            cache_metadata_lock_retry_exhausted_total: g_u64!(
                "cache.metadata_lock_retry_exhausted_total"
            ),
            cache_write_stamp_retried_total: g_u64!("cache.write_stamp_retried_total"),
            cache_write_stamp_retry_landed_total: g_u64!("cache.write_stamp_retry_landed_total"),
            cache_write_stamp_retry_overflow_keys: g_u64!("cache.write_stamp_retry_overflow_keys"),
            cache_write_stamp_retry_active_keys: g_u64!("cache.write_stamp_retry_active_keys"),
            cache_write_stamp_retry_abandoned_total: g_u64!(
                "cache.write_stamp_retry_abandoned_total"
            ),
            cache_write_stamp_log_pending_keys: g_u64!("cache.write_stamp_log_pending_keys"),
            cache_write_stamp_log_replayed_total: g_u64!("cache.write_stamp_log_replayed_total"),
            cache_write_stamp_log_torn_records_total: g_u64!(
                "cache.write_stamp_log_torn_records_total"
            ),
            cache_write_stamp_log_write_failures_total: g_u64!(
                "cache.write_stamp_log_write_failures_total"
            ),
            cache_write_stamp_log_memory_only: g_u64!("cache.write_stamp_log_memory_only"),
            cache_write_stamp_pending_declined_total: g_u64!(
                "cache.write_stamp_pending_declined_total"
            ),
            cache_volume_stall_breaker_open: g_u64!("cache.volume_stall_breaker_open"),
            cache_volume_stall_breaker_trips_total: g_u64!(
                "cache.volume_stall_breaker_trips_total"
            ),
            cache_tee_abandoned_total_put: g_u64!("cache.tee_abandoned_total_put"),
            cache_metadata_heal_overwrite_total: g_u64!("cache.metadata_heal_overwrite_total"),
            cache_upstream_idle_abort_total_mid_stream: g_u64!(
                "cache.upstream_idle_abort_total_mid_stream"
            ),
            cache_upstream_idle_abort_total_pre_stream: g_u64!(
                "cache.upstream_idle_abort_total_pre_stream"
            ),
            cache_upstream_idle_retry_total: g_u64!("cache.upstream_idle_retry_total"),

            // Stale file handles on `.meta` reads; see `CacheMetrics::metadata_read_stale_handles_total`.
            cache_metadata_read_stale_handles_total: g_u64!(
                "cache.metadata_read_stale_handles_total"
            ),
            cache_metadata_read_stale_handle_misses_total: g_u64!(
                "cache.metadata_read_stale_handle_misses_total"
            ),
            cache_etag_mismatches_total: g_u64!("cache.etag_mismatches_total"),
            cache_range_invalidations_total: g_u64!("cache.range_invalidations_total"),
            cache_incomplete_uploads_evicted: g_u64!("cache.incomplete_uploads_evicted"),
            // cache: capacity-authority divergence gauges
            cache_consumed_bytes: g_u64!("cache.consumed_bytes"),
            // cache: the danger-band capacity guard
            cache_capacity_guard_declines_total: g_u64!("cache.capacity_guard.declines_total"),
            cache_capacity_guard_last_free_bytes: g_u64!("cache.capacity_guard.last_free_bytes"),
            cache_capacity_guard_floor_bytes: g_u64!("cache.capacity_guard.floor_bytes"),
            cache_read_fills_declined_max_cache_size_total: g_u64!(
                "cache.read_fills_declined_max_cache_size_total"
            ),
            cache_hostname_collisions_total: g_u64!("cache.hostname_collisions_total"),
            // staged (write) tier
            staged_range_count: g_u64!("write_cache.staged_range_count"),
            staged_range_budget: g_u64!("write_cache.staged_range_budget"),
            staged_range_budget_percent: g_f64!("write_cache.staged_range_budget_percent"),
            staged_range_budget_declines_total: g_u64!(
                "write_cache.staged_range_budget_declines_total"
            ),
            staged_bytes: g_u64!("write_cache.staged_bytes"),
            cache_in_progress_part_bytes: g_u64!("cache.in_progress_part_bytes"),
            cache_in_progress_part_bytes_age_secs: g_u64!("cache.in_progress_part_bytes_age_secs"),
            cache_in_progress_part_bytes_drift_bytes: g_i64!(
                "cache.in_progress_part_bytes_drift_bytes"
            ),
            staged_shard_authority_age_secs: g_u64!("write_cache.staged_shard_authority_age_secs"),
            staged_max_authority_age_secs: g_u64!("write_cache.staged_authority_max_age_secs"),
            staged_shards_beyond_max_age: g_u64!("write_cache.staged_fold_shards_beyond_max_age"),
            staged_fold_incomplete_total: g_u64!("write_cache.staged_fold_incomplete_total"),
            staged_fold_shard_budget_capped_total: g_u64!(
                "write_cache.staged_fold_shard_budget_capped_total"
            ),
            staged_fold_dirty_shards: g_u64!("write_cache.staged_fold_dirty_shards"),
            staged_fold_shard_duration_count: g_u64!(
                "write_cache.staged_fold_shard_duration_count"
            ),
            staged_fold_shard_duration_sum_ms: g_u64!(
                "write_cache.staged_fold_shard_duration_sum_ms"
            ),
            staged_fold_shard_duration_bucket: g_u64!(
                "write_cache.staged_fold_shard_duration_bucket"
            ),
            staged_authority_epoch: g_u64!("write_cache.staged_authority_epoch"),
            write_cache_setup_abandoned_total_put: g_u64!("write_cache.setup_abandoned_total_put"),
            write_cache_mpu_completion_ledger_declines_total: g_u64!(
                "write_cache.mpu_completion_ledger_declines_total"
            ),
            pending_lock_waits_total: g_u64!("write_cache.pending_lock_waits_total"),
            pending_lock_wait_ms_total: g_u64!("write_cache.pending_lock_wait_ms_total"),
            pending_lock_timeouts_total: g_u64!("write_cache.pending_lock_timeouts_total"),
            staged_authority_snapshot_hits_total: g_u64!(
                "write_cache.staged_authority_snapshot_hits_total"
            ),
            staged_authority_snapshot_recomputes_total: g_u64!(
                "write_cache.staged_authority_snapshot_recomputes_total"
            ),
            ledger_repair_rotation_reachable: g_u64!(
                "write_cache.ledger_repair_rotation_reachable"
            ),
            ledger_repair_projected_rotation_secs: g_u64!(
                "write_cache.ledger_repair_projected_rotation_secs"
            ),
            ledger_repair_shard_budget_capped_total: g_u64!(
                "write_cache.ledger_repair_shard_budget_capped_total"
            ),
            ledger_repair_ranges_reappended_total: g_u64!(
                "write_cache.ledger_repair_ranges_reappended_total"
            ),
            orphan_reclaim_enabled: g_u64!("orphan_reclaim.enabled"),
            orphan_reclaim_found: g_u64!("orphan_reclaim.found"),
            orphan_reclaim_reclaimed: g_u64!("orphan_reclaim.reclaimed"),
            orphan_reclaim_bytes_reclaimed: g_u64!("orphan_reclaim.bytes_reclaimed"),
            orphan_reclaim_skipped_unconfirmed: g_u64!("orphan_reclaim.skipped_unconfirmed"),
            orphan_reclaim_kept_listed: g_u64!("orphan_reclaim.kept_listed"),
            orphan_reclaim_journal_evidence_total: g_u64!("orphan_reclaim.journal_evidence_total"),
            orphan_reclaim_passes: g_u64!("orphan_reclaim.passes"),
            orphan_reclaim_units_visited: g_u64!("orphan_reclaim.units_visited"),
            orphan_reclaim_passes_gated: g_u64!("orphan_reclaim.passes_gated"),
            orphan_reclaim_leases_unreadable: g_u64!("orphan_reclaim.leases_unreadable"),
            orphan_reclaim_leases_unreadable_expired: g_u64!(
                "orphan_reclaim.leases_unreadable_expired"
            ),
            orphan_reclaim_stalest_checkpoint_age_secs: g_u64!(
                "orphan_reclaim.stalest_checkpoint_age_secs"
            ),
            orphan_reclaim_units_never_scanned: g_u64!("orphan_reclaim.units_never_scanned"),
            write_ledger_append_batches_total: g_u64!("write_ledger.append_batches_total"),
            write_ledger_append_batched_entries_total: g_u64!(
                "write_ledger.append_batched_entries_total"
            ),
            write_ledger_pending_batches_total: g_u64!("write_ledger.pending_batches_total"),
            write_ledger_pending_batch_failures_total: g_u64!(
                "write_ledger.pending_batch_failures_total"
            ),
            eviction_worklist_generation: g_u64!("eviction_worklist.generation"),
            eviction_worklist_chunks_authored_total: g_u64!(
                "eviction_worklist.chunks_authored_total"
            ),
            eviction_worklist_chunks_claimed_total: g_u64!(
                "eviction_worklist.chunks_claimed_total"
            ),
            eviction_worklist_chunks_done_total: g_u64!("eviction_worklist.chunks_done_total"),
            eviction_worklist_chunks_unusable_total: g_u64!(
                "eviction_worklist.chunks_unusable_total"
            ),
            eviction_worklist_items_executed_total: g_u64!(
                "eviction_worklist.items_executed_total"
            ),
            eviction_worklist_items_refused_total: g_u64!("eviction_worklist.items_refused_total"),
            eviction_worklist_lease_takeovers_total: g_u64!(
                "eviction_worklist.lease_takeovers_total"
            ),
            eviction_worklist_author_fallback_total: g_u64!(
                "eviction_worklist.author_fallback_total"
            ),
            eviction_worklist_in_flight_bytes: g_u64!("eviction_worklist.in_flight_bytes"),
            scanner_concurrency_k: [
                g_u64!("write_cache.scanner_concurrency_k_fold"),
                g_u64!("write_cache.scanner_concurrency_k_repair"),
            ],
            scanner_concurrency_observed: [
                g_u64!("write_cache.scanner_concurrency_observed_fold"),
                g_u64!("write_cache.scanner_concurrency_observed_repair"),
            ],
            scanner_samples_discarded_total: [
                g_u64!("write_cache.scanner_samples_discarded_total_fold"),
                g_u64!("write_cache.scanner_samples_discarded_total_repair"),
            ],
            scan_deferred_total: [
                g_u64!("write_cache.scan_deferred_total_fold"),
                g_u64!("write_cache.scan_deferred_total_repair"),
            ],
            scanner_last_adjust_reason: [
                g_u64!("write_cache.scanner_last_adjust_reason_fold"),
                g_u64!("write_cache.scanner_last_adjust_reason_repair"),
            ],
            scanner_rate_by_level: [
                g_f64!("write_cache.scanner_rate_by_level_fold"),
                g_f64!("write_cache.scanner_rate_by_level_repair"),
            ],
            scanner_samples_by_level: [
                g_u64!("write_cache.scanner_samples_by_level_fold"),
                g_u64!("write_cache.scanner_samples_by_level_repair"),
            ],
            scanner_aggregate_estimate_by_level: [
                g_f64!("write_cache.scanner_aggregate_estimate_by_level_fold"),
                g_f64!("write_cache.scanner_aggregate_estimate_by_level_repair"),
            ],
            // eviction: R12.1's pass figures
            cache_eviction_passes_total: g_u64!("cache.eviction_passes_total"),
            cache_eviction_last_result_duration_ms: g_u64!(
                "cache.eviction_last_result.duration_ms"
            ),
            eviction_coordination_staging_evictions_skipped_lock_held: g_u64!(
                "eviction_coordination.staging_evictions_skipped_lock_held"
            ),
            // eviction: R12.3's rates
            cache_eviction_shed_bytes_total: g_u64!("cache.eviction_shed_bytes_total"),
            cache_eviction_shed_ranges_total: g_u64!("cache.eviction_shed_ranges_total"),
            cache_eviction_active_micros_total: g_u64!("cache.eviction_active_micros_total"),
            cache_eviction_shed_bytes_per_sec: g_f64!("cache.eviction_shed_bytes_per_sec"),
            cache_eviction_shed_ranges_per_sec: g_f64!("cache.eviction_shed_ranges_per_sec"),
            cache_admission_bytes_total: g_u64!("cache.admission_bytes_total"),
            cache_admission_ranges_total: g_u64!("cache.admission_ranges_total"),
            cache_admission_bytes_per_sec: g_f64!("cache.admission_bytes_per_sec"),
            cache_admission_ranges_per_sec: g_f64!("cache.admission_ranges_per_sec"),
            cache_eviction_keeping_pace: g_u64!("cache.eviction_keeping_pace"),
            cache_eviction_backlog_bytes: g_i64!("cache.eviction_backlog_bytes"),
            cache_eviction_backlog_projected_bytes: g_i64!(
                "cache.eviction_backlog_projected_bytes"
            ),
            cache_st_size_folds_declined_overlap_total: g_u64!(
                "cache.st_size_folds_declined_overlap_total"
            ),
            cache_st_size_fold_last_duration_secs: g_u64!("cache.st_size_fold_last_duration_secs"),
            cache_st_size_fold_oldest_shard_scan_age_secs: g_u64!(
                "cache.st_size_fold_oldest_shard_scan_age_secs"
            ),
            // eviction: Tail_Partition, build side
            cache_tail_floor_bytes: g_u64!("cache.tail_floor_bytes"),
            cache_tail_max_entries: g_u64!("cache.tail_max_entries"),
            cache_tail_rebuilds_total: g_u64!("cache.tail_rebuilds_total"),
            cache_tail_entries_last_rebuild: g_u64!("cache.tail_entries_last_rebuild"),
            cache_tail_effective_min_bytes: g_u64!("cache.tail_effective_min_bytes"),
            cache_tail_delta_records: g_u64!("cache.tail_delta_records"),
            // eviction: Tail_Partition, selection side
            cache_tail_coverage_ratio: g_f64!("cache.tail_coverage_ratio"),
            cache_tail_coverage_short_total: g_u64!("cache.tail_coverage_short_total"),
            cache_eviction_fallback_tail_empty_total: g_u64!(
                "cache.eviction_fallback_tail_empty_total"
            ),
            cache_eviction_fallback_cap_exhausted_total: g_u64!(
                "cache.eviction_fallback_cap_exhausted_total"
            ),
            cache_eviction_fallback_unvisited_total: g_u64!(
                "cache.eviction_fallback_unvisited_total"
            ),
            cache_eviction_oversized_single_candidate_total: g_u64!(
                "cache.eviction_oversized_single_candidate_total"
            ),
            cache_tail_delta_cap_rewrites_total: g_u64!("cache.tail_delta_cap_rewrites_total"),
            cache_eviction_tail_verdict_total: g_u64!("cache.eviction_tail_verdict_total"),
            // eviction: executor
            cache_eviction_pass_path_whole_object_total: g_u64!(
                "cache.eviction_pass_path_whole_object_total"
            ),
            cache_eviction_pass_path_partial_total: g_u64!(
                "cache.eviction_pass_path_partial_total"
            ),
            cache_eviction_whole_object_downgraded_total: g_u64!(
                "cache.eviction_whole_object_downgraded_total"
            ),
            cache_eviction_unlink_failures_total: g_u64!("cache.eviction_unlink_failures_total"),
            cache_eviction_meta_rewrites_total: g_u64!("cache.eviction_meta_rewrites_total"),
            cache_eviction_active_key_journal_reads_total: g_u64!(
                "cache.eviction_active_key_journal_reads_total"
            ),
            cache_eviction_oracle_refreshes_total: g_u64!("cache.eviction_oracle_refreshes_total"),
            cache_eviction_oracle_stale_verdicts_total: g_u64!(
                "cache.eviction_oracle_stale_verdicts_total"
            ),
            cache_eviction_oracle_last_walk_ms: g_u64!("cache.eviction_oracle_last_walk_ms"),
            // eviction: the read tier's last pass (fleet-wide — see the struct comment)
            cache_eviction_last_result_completed_at: g_u64!(
                "cache.eviction_last_result.completed_at"
            ),
            cache_eviction_last_result_bytes_requested: g_u64!(
                "cache.eviction_last_result.bytes_requested"
            ),
            cache_eviction_last_result_bytes_freed: g_u64!(
                "cache.eviction_last_result.bytes_freed"
            ),
            cache_eviction_last_result_shortfall_bytes: g_u64!(
                "cache.eviction_last_result.shortfall_bytes"
            ),
            cache_eviction_last_result_unevictable_in_progress_bytes: g_u64!(
                "cache.eviction_last_result.unevictable_in_progress_bytes"
            ),
            cache_eviction_last_result_unexplained_shortfall_bytes: g_u64!(
                "cache.eviction_last_result.unexplained_shortfall_bytes"
            ),
            cache_eviction_last_result_reached_target: g_u64!(
                "cache.eviction_last_result.reached_target"
            ),
            cache_eviction_last_result_ranges_evicted: g_u64!(
                "cache.eviction_last_result.ranges_evicted"
            ),
            // write_cache: the count-driven pass's last result
            count_pass_completed_at: g_u64!("write_cache.count_pass_last_result.completed_at"),
            count_pass_count_budget: g_u64!("write_cache.count_pass_last_result.count_budget"),
            count_pass_count_trigger: g_u64!("write_cache.count_pass_last_result.count_trigger"),
            count_pass_count_target: g_u64!("write_cache.count_pass_last_result.count_target"),
            count_pass_count_at_start: g_u64!("write_cache.count_pass_last_result.count_at_start"),
            count_pass_observed_count_at_start: g_u64!(
                "write_cache.count_pass_last_result.observed_count_at_start"
            ),
            count_pass_ranges_requested: g_u64!(
                "write_cache.count_pass_last_result.ranges_requested"
            ),
            count_pass_ranges_freed: g_u64!("write_cache.count_pass_last_result.ranges_freed"),
            count_pass_shortfall_ranges: g_u64!(
                "write_cache.count_pass_last_result.shortfall_ranges"
            ),
            count_pass_unevictable_unreadable_ranges: g_u64!(
                "write_cache.count_pass_last_result.unevictable_unreadable_ranges"
            ),
            count_pass_unexplained_shortfall_ranges: g_u64!(
                "write_cache.count_pass_last_result.unexplained_shortfall_ranges"
            ),
            count_pass_reached_target: g_u64!("write_cache.count_pass_last_result.reached_target"),
            count_pass_objects_evicted: g_u64!(
                "write_cache.count_pass_last_result.objects_evicted"
            ),
            count_pass_candidates_considered: g_u64!(
                "write_cache.count_pass_last_result.candidates_considered"
            ),
            count_pass_reclaimable_candidates: g_u64!(
                "write_cache.count_pass_last_result.reclaimable_candidates"
            ),
            count_pass_expired_candidates: g_u64!(
                "write_cache.count_pass_last_result.expired_candidates"
            ),
            count_pass_passes_total: g_u64!("write_cache.count_pass_last_result.passes_total"),
            count_pass_ranges_freed_total: g_u64!(
                "write_cache.count_pass_last_result.ranges_freed_total"
            ),
            count_pass_admission_declines_total: g_u64!(
                "write_cache.count_pass_last_result.admission_declines_total"
            ),
            count_pass_shortfall_reason: g_u64!(
                "write_cache.count_pass_last_result.shortfall_reason"
            ),
            // cache_size: R12.6-R12.8's accounting-drift gauges
            accounting_drift_sampled_at: g_u64!("cache_size.accounting_drift.sampled_at"),
            accounting_drift_persisted: g_u64!("cache_size.accounting_drift.persisted"),
            accounting_drift_reference: g_u64!("cache_size.accounting_drift.reference"),
            accounting_drift_drift: g_i64!("cache_size.accounting_drift.drift"),
            accounting_drift_ratio: g_f64!("cache_size.accounting_drift.drift_ratio"),
            accounting_drift_unevaluable: g_u64!("cache_size.accounting_drift.unevaluable_reason"),
            accounting_drift_sample_kind: g_u64!("cache_size.accounting_drift.sample_kind"),
            // compression
            compression_ratio: g_f64!("compression.average_compression_ratio"),
            compression_failures: g_u64!("compression.failures_total"),
            // connection pool: none — see the struct comment
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
            req_access_log_records_dropped: g_u64!("request_metrics.access_log_records_dropped"),
            // metrics_collection
            metrics_collections_total: g_u64!("metrics_collection.collections_total"),
            metrics_collection_duration_ms_last: g_u64!("metrics_collection.duration_ms_last"),
            metrics_collection_duration_ms_max: g_u64!("metrics_collection.duration_ms_max"),
            metrics_collection_slow_total: g_u64!("metrics_collection.slow_collections_total"),
            metrics_collection_slow_threshold_ms: g_u64!("metrics_collection.slow_threshold_ms"),
            // cache_hit_buffer
            cache_hit_buffer_pending_updates: g_u64!("cache_hit_buffer.pending_updates"),
            cache_hit_buffer_updates_recorded_total: g_u64!(
                "cache_hit_buffer.updates_recorded_total"
            ),
            cache_hit_buffer_entries_flushed_total: g_u64!(
                "cache_hit_buffer.entries_flushed_total"
            ),
            cache_hit_buffer_flush_errors_total: g_u64!("cache_hit_buffer.flush_errors_total"),
            cache_hit_buffer_ram_records_dropped_total: g_u64!(
                "cache_hit_buffer.ram_records_dropped_total"
            ),
            // fill_visibility
            fill_visibility_entries: g_u64!("fill_visibility.entries"),
            fill_visibility_bytes: g_u64!("fill_visibility.bytes"),
            fill_visibility_recorded_total: g_u64!("fill_visibility.recorded_total"),
            fill_visibility_hits_total: g_u64!("fill_visibility.hits_total"),
            fill_visibility_dropped_disk_meta_total: g_u64!(
                "fill_visibility.dropped_disk_meta_total"
            ),
            fill_visibility_dropped_folded_total: g_u64!("fill_visibility.dropped_folded_total"),
            fill_visibility_dropped_invalidated_total: g_u64!(
                "fill_visibility.dropped_invalidated_total"
            ),
            fill_visibility_dropped_cap_total: g_u64!("fill_visibility.dropped_cap_total"),
            fill_visibility_dropped_age_total: g_u64!("fill_visibility.dropped_age_total"),
            fill_visibility_dropped_range_file_changed_total: g_u64!(
                "fill_visibility.dropped_range_file_changed_total"
            ),
            // inflight_memory
            inflight_memory_reserved_bytes: g_u64!("inflight_memory.reserved_bytes"),
            inflight_memory_ceiling_bytes: g_u64!("inflight_memory.ceiling_bytes"),
            inflight_memory_peak_reserved_bytes: g_u64!("inflight_memory.peak_reserved_bytes"),
            inflight_memory_rejected_total: g_u64!("inflight_memory.rejected_total"),
            inflight_memory_aborted_accumulations_total: g_u64!(
                "inflight_memory.aborted_accumulations_total"
            ),
            inflight_memory_granted_total: g_u64!("inflight_memory.granted_total"),
            inflight_memory_granted_bytes_total: g_u64!("inflight_memory.granted_bytes_total"),
            inflight_memory_ram_cache_max_bytes: g_u64!("inflight_memory.ram_cache_max_bytes"),
            inflight_memory_buffered_serve_sliced_streams_total: g_u64!(
                "inflight_memory.buffered_serve_sliced_streams_total"
            ),
            inflight_memory_buffered_serve_degraded_chained_stream_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_chained_stream_total"
            ),
            inflight_memory_buffered_serve_degraded_forward_gap_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_forward_gap_total"
            ),
            inflight_memory_buffered_serve_degraded_forward_partial_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_forward_partial_total"
            ),
            inflight_memory_buffered_serve_degraded_forward_unresolved_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_forward_unresolved_total"
            ),
            inflight_memory_buffered_serve_degraded_forward_stream_failed_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_forward_stream_failed_total"
            ),
            inflight_memory_buffered_serve_degraded_forward_partial_cache_merge_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_forward_partial_cache_merge_total"
            ),
            inflight_memory_buffered_serve_degraded_merge_backstop_total: g_u64!(
                "inflight_memory.buffered_serve_degraded_merge_backstop_total"
            ),
            inflight_memory_buffered_serve_size_le_64kib_total: g_u64!(
                "inflight_memory.buffered_serve_size_le_64kib_total"
            ),
            inflight_memory_buffered_serve_size_le_1mib_total: g_u64!(
                "inflight_memory.buffered_serve_size_le_1mib_total"
            ),
            inflight_memory_buffered_serve_size_le_8mib_total: g_u64!(
                "inflight_memory.buffered_serve_size_le_8mib_total"
            ),
            inflight_memory_buffered_serve_size_gt_8mib_total: g_u64!(
                "inflight_memory.buffered_serve_size_gt_8mib_total"
            ),
            inflight_memory_buffered_serve_peak_materialised_bytes: g_u64!(
                "inflight_memory.buffered_serve_peak_materialised_bytes"
            ),
            inflight_memory_buffered_serve_full_object_total: g_u64!(
                "inflight_memory.buffered_serve_full_object_total"
            ),
            // atomic_metadata (the four live slot-lock figures only; see the struct comment)
            atomic_metadata_lock_acquisitions_successful: g_u64!(
                "atomic_metadata.metadata_lock_acquisitions_successful"
            ),
            atomic_metadata_lock_acquisitions_failed: g_u64!(
                "atomic_metadata.metadata_lock_acquisitions_failed"
            ),
            atomic_metadata_lock_hold_time_ms_total: g_u64!(
                "atomic_metadata.metadata_lock_hold_time_ms_total"
            ),
            atomic_metadata_lock_wait_budget_expired_total: g_u64!(
                "atomic_metadata.metadata_lock_wait_budget_expired_total"
            ),
            atomic_metadata_range_file_corruption_detected: g_u64!(
                "atomic_metadata.range_file_corruption_detected"
            ),
            atomic_metadata_range_file_corruption_healed: g_u64!(
                "atomic_metadata.range_file_corruption_healed"
            ),
            atomic_metadata_range_file_corruption_heal_suppressed: g_u64!(
                "atomic_metadata.range_file_corruption_heal_suppressed"
            ),
            atomic_metadata_range_file_identity_mismatch_detected: g_u64!(
                "atomic_metadata.range_file_identity_mismatch_detected"
            ),
            atomic_metadata_journal_validation_failures: g_u64!(
                "atomic_metadata.journal_validation_failures"
            ),
            // consolidation (fleet-wide — see the struct comment)
            consolidation_cache_size_bytes: g_u64!("consolidation.cache_size_bytes"),
            consolidation_cache_write_size_bytes: g_u64!("consolidation.cache_write_size_bytes"),
            consolidation_consolidation_count: g_u64!("consolidation.consolidation_count"),
            consolidation_last_consolidation_timestamp: g_u64!(
                "consolidation.last_consolidation_timestamp"
            ),
            consolidation_consolidation_lock_held_secs: g_u64!(
                "consolidation.consolidation_lock_held_secs"
            ),
            consolidation_consolidation_lock_takeovers: g_u64!(
                "consolidation.consolidation_lock_takeovers"
            ),
            // hedged_requests
            hedged_requests_issued: g_u64!("hedged_requests.issued"),
            hedged_requests_won: g_u64!("hedged_requests.won"),
            hedged_requests_suppressed: g_u64!("hedged_requests.suppressed"),
            // page_cache
            page_cache_widened_requests: g_u64!("page_cache.widened_requests"),
            page_cache_bytes_prefetched: g_u64!("page_cache.bytes_prefetched"),
            page_cache_amplification_ratio: g_f64!("page_cache.amplification_ratio"),
            page_cache_page_hits: g_u64!("page_cache.page_hits"),
            page_cache_skipped_signed_range: g_u64!("page_cache.skipped_signed_range"),
            page_cache_fallbacks: g_u64!("page_cache.fallbacks"),
            page_cache_ram_page_promotions: g_u64!("page_cache.ram_page_promotions"),
            page_cache_ram_page_promotion_skipped: g_u64!("page_cache.ram_page_promotion_skipped"),
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
            i.cache_max_cache_size_limit
                .record(c.max_cache_size_limit, a);
            i.cache_read_cache_size.record(c.read_cache_size, a);
            i.cache_read_cache_size_inverted_secs
                .record(c.read_cache_size_inverted_secs, a);
            i.cache_write_cache_size.record(c.write_cache_size, a);
            i.cache_ram_cache_size.record(c.ram_cache_size, a);
            // Skipped rather than recorded as 0 when unknown: an age of 0 reads as fresh.
            if let Some(v) = c.in_progress_part_bytes_age_secs {
                i.cache_in_progress_part_bytes_age_secs.record(v, a);
            }
            if let Some(v) = c.in_progress_part_bytes_drift_bytes {
                i.cache_in_progress_part_bytes_drift_bytes.record(v, a);
            }
            // hits/misses/evictions
            i.cache_hits.record(c.cache_hits, a);
            i.cache_misses.record(c.cache_misses, a);
            i.cache_evictions.record(c.evictions, a);
            i.cache_s3_requests_saved
                .record(c.cache_hits + c.metadata_cache_hits, a);
            i.bytes_served_from_cache
                .record(c.bytes_served_from_cache, a);
            // RAM tier
            i.ram_cache_hits.record(c.ram_cache_hits, a);
            i.ram_cache_misses.record(c.ram_cache_misses, a);
            i.ram_cache_evictions.record(c.ram_cache_evictions, a);
            i.ram_cache_eligibility_skips_total
                .record(c.ram_cache_eligibility_skips_total, a);
            i.ram_cache_entries.record(c.ram_cache_entries, a);
            // metadata tier
            i.metadata_cache_hits.record(c.metadata_cache_hits, a);
            i.metadata_cache_misses.record(c.metadata_cache_misses, a);
            i.metadata_cache_entries.record(c.metadata_cache_entries, a);
            i.metadata_cache_max_entries
                .record(c.metadata_cache_max_entries, a);
            i.metadata_cache_evictions
                .record(c.metadata_cache_evictions, a);
            i.metadata_cache_stale_refreshes
                .record(c.metadata_cache_stale_refreshes, a);
            i.metadata_cache_head_hits
                .record(c.metadata_cache_head_hits, a);
            i.metadata_cache_head_misses
                .record(c.metadata_cache_head_misses, a);
            i.metadata_cache_head_disk_hits
                .record(c.metadata_cache_head_disk_hits, a);
            i.metadata_cache_disk_hits
                .record(c.metadata_cache_disk_hits, a);
            // error/health signals
            i.cache_corruption_metadata_total
                .record(c.corruption_metadata_total, a);
            i.cache_corruption_missing_range_total
                .record(c.corruption_missing_range_total, a);
            i.cache_disk_full_events_total
                .record(c.disk_full_events_total, a);
            i.cache_metadata_lock_retry_exhausted_total
                .record(c.metadata_lock_retry_exhausted_total, a);
            i.cache_write_stamp_retried_total
                .record(c.write_stamp_retried_total, a);
            i.cache_write_stamp_retry_landed_total
                .record(c.write_stamp_retry_landed_total, a);
            i.cache_write_stamp_retry_overflow_keys
                .record(c.write_stamp_retry_overflow_keys, a);
            i.cache_write_stamp_retry_active_keys
                .record(c.write_stamp_retry_active_keys, a);
            i.cache_write_stamp_retry_abandoned_total
                .record(c.write_stamp_retry_abandoned_total, a);
            i.cache_write_stamp_log_pending_keys
                .record(c.write_stamp_log_pending_keys, a);
            i.cache_write_stamp_log_replayed_total
                .record(c.write_stamp_log_replayed_total, a);
            i.cache_write_stamp_log_torn_records_total
                .record(c.write_stamp_log_torn_records_total, a);
            i.cache_write_stamp_log_write_failures_total
                .record(c.write_stamp_log_write_failures_total, a);
            i.cache_write_stamp_log_memory_only
                .record(c.write_stamp_log_memory_only, a);
            i.cache_write_stamp_pending_declined_total
                .record(c.write_stamp_pending_declined_total, a);
            i.cache_volume_stall_breaker_open
                .record(c.volume_stall_breaker_open, a);
            i.cache_volume_stall_breaker_trips_total
                .record(c.volume_stall_breaker_trips_total, a);
            i.cache_tee_abandoned_total_put
                .record(c.tee_abandoned_total_put, a);
            i.cache_metadata_heal_overwrite_total
                .record(c.metadata_heal_overwrite_total, a);
            i.cache_upstream_idle_abort_total_mid_stream
                .record(c.upstream_idle_abort_total_mid_stream, a);
            i.cache_upstream_idle_abort_total_pre_stream
                .record(c.upstream_idle_abort_total_pre_stream, a);
            i.cache_upstream_idle_retry_total
                .record(c.upstream_idle_retry_total, a);

            i.cache_metadata_read_stale_handles_total
                .record(c.metadata_read_stale_handles_total, a);
            i.cache_metadata_read_stale_handle_misses_total
                .record(c.metadata_read_stale_handle_misses_total, a);
            i.cache_etag_mismatches_total
                .record(c.cache_etag_mismatches_total, a);
            i.cache_range_invalidations_total
                .record(c.cache_range_invalidations_total, a);
            i.cache_incomplete_uploads_evicted
                .record(c.incomplete_uploads_evicted, a);
            // capacity-authority divergence gauges
            i.cache_consumed_bytes.record(c.consumed_bytes, a);
            // danger-band capacity guard. The reading is skipped, not zeroed, before the guard
            // has taken one: zero free would read as a full volume.
            let cg = &c.capacity_guard;
            i.cache_capacity_guard_declines_total
                .record(cg.declines_total, a);
            if let Some(v) = cg.last_free_bytes {
                i.cache_capacity_guard_last_free_bytes.record(v, a);
            }
            i.cache_capacity_guard_floor_bytes.record(cg.floor_bytes, a);
            // The read tier's `max_cache_size` soft stop. Always recorded: zero is a
            // real reading here, unlike the guard's free-space one.
            i.cache_read_fills_declined_max_cache_size_total
                .record(c.read_fills_declined_max_cache_size_total, a);
            // The unique-hostname prerequisite's detector. Always recorded: zero is the reading
            // an operator needs to see, because it is the one that says the prerequisite holds.
            i.cache_hostname_collisions_total
                .record(c.hostname_collisions_total, a);
        }

        // The staged (write) tier. § 16.6.7's signals, previously `/metrics`-only.
        //
        // **Every `Option` is skipped rather than recorded as 0.** `None` is the UNREADABLE
        // sentinel throughout this section, and OTLP gauges have no null: recording a zero would
        // publish the healthiest possible value for the least healthy state, and an alarm on
        // "occupancy is low" would fire as reassurance. Not recording is the honest export — but
        // it leaves a GAP only until the first recording: under cumulative temporality the SDK's
        // last-value gauge re-sends its last recorded value on every export
        // (`opentelemetry_sdk` 0.32.1 `LastValue::cumulative` uses `collect_readonly`, no reset),
        // so a figure that becomes unknown later keeps its last value on OTLP.
        {
            let wc = &metrics.write_cache;
            let s = wc.staged_fold.summary();
            if let Some(v) = s.range_count {
                i.staged_range_count.record(v, a);
            }
            // The budget is exported so a dashboard's denominator is data rather than a literal —
            // the same reason it is on `/metrics` and in the dashboard payload.
            i.staged_range_budget.record(s.range_budget, a);
            if let Some(v) = s.range_budget_percent {
                i.staged_range_budget_percent.record(v as f64, a);
            }
            i.staged_range_budget_declines_total
                .record(s.range_budget_declines_total, a);
            // `write_cache.staged_bytes` rather than the exact `/metrics` JSON field name for this
            // figure: that name is fenced to the fold and `metrics.rs` by
            // `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`, and this
            // file may not spell it. The one deliberate divergence from the path convention in
            // this exporter.
            if let Some(v) = s.staged_bytes {
                i.staged_bytes.record(v, a);
            }
            // Exported under its own `/metrics` JSON path, `cache.in_progress_part_bytes`, rather
            // than under this section: it is `P` in its own right, disjoint from the read tier's
            // populations, and a consumer looking for it should find it where the JSON puts it.
            if let Some(v) = s.in_progress_part_bytes {
                i.cache_in_progress_part_bytes.record(v, a);
            }
            if let Some(v) = s.shard_authority_age_secs {
                i.staged_shard_authority_age_secs.record(v, a);
            }
            i.staged_max_authority_age_secs
                .record(s.max_authority_age_secs, a);
            i.staged_shards_beyond_max_age
                .record(s.shards_beyond_max_age, a);
            i.staged_fold_incomplete_total
                .record(s.fold_incomplete_total, a);
            i.staged_fold_shard_budget_capped_total
                .record(s.shard_budget_capped_total, a);
            // Per-instance, process memory: shards this proxy removed staged bytes from and has
            // not yet re-folded. Never unreadable, so always recorded. § 16.6.9.
            i.staged_fold_dirty_shards
                .record(wc.staged_fold.staged_fold_dirty_shards, a);
            // The histogram, as `_count`, `_sum` and one series per class carrying its upper edge
            // in an `le` attribute. Fixed classes rather than a quantile, so the export is
            // comparable across instances and releases; `+Inf` is the class with no edge.
            let hist = &wc.staged_fold.staged_fold_shard_duration_secs;
            i.staged_fold_shard_duration_count.record(hist.count, a);
            i.staged_fold_shard_duration_sum_ms.record(hist.sum_ms, a);
            for class in &hist.classes {
                let le = match class.le_secs {
                    Some(edge) => edge.to_string(),
                    None => "+Inf".to_string(),
                };
                i.staged_fold_shard_duration_bucket
                    .record(class.count, &[KeyValue::new("le", le)]);
            }
            i.staged_authority_epoch
                .record(wc.staged_authority_epoch, a);
            i.write_cache_setup_abandoned_total_put
                .record(wc.setup_abandoned_total_put, a);
            i.write_cache_mpu_completion_ledger_declines_total
                .record(wc.mpu_completion_ledger_declines_total, a);
            i.pending_lock_waits_total
                .record(wc.pending_lock_waits_total, a);
            i.pending_lock_wait_ms_total
                .record(wc.pending_lock_wait_ms_total, a);
            i.pending_lock_timeouts_total
                .record(wc.pending_lock_timeouts_total, a);
            i.staged_authority_snapshot_hits_total
                .record(s.authority_snapshot_hits_total, a);
            i.staged_authority_snapshot_recomputes_total
                .record(s.authority_snapshot_recomputes_total, a);
            // `reachable == false` is the only PERMANENT condition in this block, so it is
            // exported as 1/0 rather than skipped: an alarm needs a value to compare.
            if let Some(v) = wc.ledger_repair.ledger_repair_rotation_reachable {
                i.ledger_repair_rotation_reachable.record(u64::from(v), a);
            }
            if let Some(v) = wc.ledger_repair.ledger_repair_projected_rotation_secs {
                i.ledger_repair_projected_rotation_secs.record(v, a);
            }
            i.ledger_repair_shard_budget_capped_total
                .record(wc.ledger_repair.ledger_repair_shard_budget_capped_total, a);
            i.ledger_repair_ranges_reappended_total
                .record(wc.ledger_repair.ledger_repair_ranges_reappended_total, a);
            // Scanner limiter, index 0 = fold, 1 = repair. The adjust reason is sent as a code: a
            // gauge carries no string, and a `reason` attribute would keep re-sending every earlier
            // reason's series under cumulative temporality. The per-level figures carry a `level` attribute, at most
            // `SCANNER_LEVELS_PUBLISHED_MAX` series each, and a level never stops being sampled.
            let sc = &wc.scanner;
            let reason_code = |s: &str| match s {
                "floor" => 0,
                "insufficient_samples" => 1,
                "hold" => 2,
                "probe" => 3,
                "argmax" => 4,
                _ => 1,
            };
            for (idx, (k, observed, discarded, deferred, reason, rate, samples, aggregate)) in [
                (
                    sc.scanner_concurrency_k_fold,
                    sc.scanner_concurrency_observed_fold,
                    sc.scanner_samples_discarded_total_fold,
                    sc.scan_deferred_total_fold,
                    &sc.scanner_last_adjust_reason_fold,
                    &sc.scanner_rate_by_level_fold,
                    &sc.scanner_samples_by_level_fold,
                    &sc.scanner_aggregate_estimate_by_level_fold,
                ),
                (
                    sc.scanner_concurrency_k_repair,
                    sc.scanner_concurrency_observed_repair,
                    sc.scanner_samples_discarded_total_repair,
                    sc.scan_deferred_total_repair,
                    &sc.scanner_last_adjust_reason_repair,
                    &sc.scanner_rate_by_level_repair,
                    &sc.scanner_samples_by_level_repair,
                    &sc.scanner_aggregate_estimate_by_level_repair,
                ),
            ]
            .into_iter()
            .enumerate()
            {
                i.scanner_concurrency_k[idx].record(u64::from(k), a);
                i.scanner_concurrency_observed[idx].record(u64::from(observed), a);
                i.scanner_samples_discarded_total[idx].record(discarded, a);
                i.scan_deferred_total[idx].record(deferred, a);
                if !reason.is_empty() {
                    i.scanner_last_adjust_reason[idx].record(reason_code(reason), a);
                }
                for (level, r) in rate {
                    i.scanner_rate_by_level[idx]
                        .record(*r, &[KeyValue::new("level", i64::from(*level))]);
                }
                for (level, s) in samples {
                    i.scanner_samples_by_level[idx]
                        .record(*s, &[KeyValue::new("level", i64::from(*level))]);
                }
                for (level, est) in aggregate {
                    i.scanner_aggregate_estimate_by_level[idx]
                        .record(*est, &[KeyValue::new("level", i64::from(*level))]);
                }
            }
            // The count-driven pass's last result. Absent until one has completed on the volume,
            // and `absent` is the reading — state (c) of § 16.6.12 point 5 — so nothing is
            // recorded rather than a zero being invented for each field.
            //
            // `schema` and `instance_id` are deliberately NOT exported. `schema` is a document
            // format version, not a measurement. `instance_id` names the instance that ran the
            // pass, which is not the exporting instance and would collide with the resource's own
            // `service.instance.id`; carrying it as an attribute instead would make
            // `passes_total` appear to reset whenever a different proxy won the lock. Both remain
            // on `/metrics`, where the operator reading a fleet-wide document needs them.
            if let Some(cp) = &wc.count_pass_last_result {
                i.count_pass_completed_at.record(cp.completed_at, a);
                i.count_pass_count_budget.record(cp.count_budget, a);
                i.count_pass_count_trigger.record(cp.count_trigger, a);
                i.count_pass_count_target.record(cp.count_target, a);
                i.count_pass_count_at_start.record(cp.count_at_start, a);
                i.count_pass_observed_count_at_start
                    .record(cp.observed_count_at_start, a);
                i.count_pass_ranges_requested.record(cp.ranges_requested, a);
                i.count_pass_ranges_freed.record(cp.ranges_freed, a);
                i.count_pass_shortfall_ranges.record(cp.shortfall_ranges, a);
                i.count_pass_unevictable_unreadable_ranges
                    .record(cp.unevictable_unreadable_ranges, a);
                i.count_pass_unexplained_shortfall_ranges
                    .record(cp.unexplained_shortfall_ranges, a);
                i.count_pass_reached_target
                    .record(u64::from(cp.reached_target), a);
                i.count_pass_objects_evicted.record(cp.objects_evicted, a);
                i.count_pass_candidates_considered
                    .record(cp.candidates_considered, a);
                i.count_pass_reclaimable_candidates
                    .record(cp.reclaimable_candidates, a);
                i.count_pass_expired_candidates
                    .record(cp.expired_candidates, a);
                i.count_pass_passes_total.record(cp.passes_total, a);
                i.count_pass_ranges_freed_total
                    .record(cp.ranges_freed_total, a);
                i.count_pass_admission_declines_total
                    .record(cp.admission_declines_total, a);
                i.count_pass_shortfall_reason
                    .record(1, &[KeyValue::new("reason", cp.shortfall_reason.clone())]);
            }
        }

        if let Some(comp) = &metrics.compression {
            i.compression_ratio
                .record(comp.average_compression_ratio as f64, a);
            i.compression_failures
                .record(comp.compression_failures + comp.decompression_failures, a);
        }

        // connection_pool: nothing recorded. Its three former instruments exported literals; see
        // the `Instruments` comment.

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
        i.req_access_log_records_dropped
            .record(rm.access_log_records_dropped, a);

        // metrics_collection. Note this records the collection currently being exported: the
        // figures are folded in before `export_metrics` is called, so the sample is self-describing
        // rather than one collection behind.
        let mc = &metrics.metrics_collection;
        i.metrics_collections_total.record(mc.collections_total, a);
        i.metrics_collection_duration_ms_last
            .record(mc.duration_ms_last, a);
        i.metrics_collection_duration_ms_max
            .record(mc.duration_ms_max, a);
        i.metrics_collection_slow_total
            .record(mc.slow_collections_total, a);
        i.metrics_collection_slow_threshold_ms
            .record(mc.slow_threshold_ms, a);

        // cache_hit_buffer: recorded only when the section is present, which is the whole point of
        // its `Option`. `None` means no cache manager is wired into the metrics manager; four
        // zeroes are what a wired-but-idle buffer reports, so emitting zeros here would collapse
        // "the flush task is dead" into "the flush task has nothing to do" on the one transport
        // that cannot fall back to reading the log. An absent section leaves a GAP in the series
        // instead — the same convention every other optional section on this exporter already follow.
        if let Some(chb) = &metrics.cache_hit_buffer {
            i.cache_hit_buffer_pending_updates
                .record(chb.pending_updates, a);
            i.cache_hit_buffer_updates_recorded_total
                .record(chb.updates_recorded_total, a);
            i.cache_hit_buffer_entries_flushed_total
                .record(chb.entries_flushed_total, a);
            i.cache_hit_buffer_flush_errors_total
                .record(chb.flush_errors_total, a);
            i.cache_hit_buffer_ram_records_dropped_total
                .record(chb.ram_records_dropped_total, a);
        }
        // fill_visibility: same `Option` convention as `cache_hit_buffer` above — a gap, not
        // zeroes, when no cache manager is wired in.
        if let Some(fv) = &metrics.fill_visibility {
            i.fill_visibility_entries.record(fv.entries, a);
            i.fill_visibility_bytes.record(fv.bytes, a);
            i.fill_visibility_recorded_total
                .record(fv.recorded_total, a);
            i.fill_visibility_hits_total.record(fv.hits_total, a);
            i.fill_visibility_dropped_disk_meta_total
                .record(fv.dropped_disk_meta_total, a);
            i.fill_visibility_dropped_folded_total
                .record(fv.dropped_folded_total, a);
            i.fill_visibility_dropped_invalidated_total
                .record(fv.dropped_invalidated_total, a);
            i.fill_visibility_dropped_cap_total
                .record(fv.dropped_cap_total, a);
            i.fill_visibility_dropped_age_total
                .record(fv.dropped_age_total, a);
            i.fill_visibility_dropped_range_file_changed_total
                .record(fv.dropped_range_file_changed_total, a);
        }
        // orphan_reclaim: always present. The four evidence outcomes are a closed set, so every
        // one is recorded every export, zero included.
        let orc = &metrics.orphan_reclaim;
        i.orphan_reclaim_enabled.record(u64::from(orc.enabled), a);
        i.orphan_reclaim_found.record(orc.found, a);
        i.orphan_reclaim_reclaimed.record(orc.reclaimed, a);
        i.orphan_reclaim_bytes_reclaimed
            .record(orc.bytes_reclaimed, a);
        i.orphan_reclaim_skipped_unconfirmed
            .record(orc.skipped_unconfirmed, a);
        i.orphan_reclaim_kept_listed.record(orc.kept_listed, a);
        let ev = &orc.journal_evidence_total;
        for (result, count) in [
            ("confirmed_absent", ev.confirmed_absent),
            ("journal_present", ev.journal_present),
            ("peer_unavailable", ev.peer_unavailable),
            ("indeterminate", ev.indeterminate),
        ] {
            i.orphan_reclaim_journal_evidence_total
                .record(count, &[KeyValue::new("result", result)]);
        }
        i.orphan_reclaim_passes.record(orc.passes, a);
        i.orphan_reclaim_units_visited.record(orc.units_visited, a);
        i.orphan_reclaim_passes_gated.record(orc.passes_gated, a);
        i.orphan_reclaim_leases_unreadable
            .record(orc.leases_unreadable, a);
        i.orphan_reclaim_leases_unreadable_expired
            .record(orc.leases_unreadable_expired, a);
        if let Some(age) = orc.stalest_checkpoint_age_secs {
            i.orphan_reclaim_stalest_checkpoint_age_secs.record(age, a);
        }
        if let Some(n) = orc.units_never_scanned {
            i.orphan_reclaim_units_never_scanned.record(n, a);
        }
        // write_ledger: always present, process atomics. Recorded every export, zero included,
        // because the mean batch size is a ratio of two of them and a reader needs both.
        let wl = &metrics.write_ledger;
        i.write_ledger_append_batches_total
            .record(wl.append_batches_total, a);
        i.write_ledger_append_batched_entries_total
            .record(wl.append_batched_entries_total, a);
        i.write_ledger_pending_batches_total
            .record(wl.pending_batches_total, a);
        i.write_ledger_pending_batch_failures_total
            .record(wl.pending_batch_failures_total, a);
        // eviction_worklist: always present. The attribute sets are closed, so every series is
        // recorded every export, zero included.
        let ew = &metrics.eviction_worklist;
        i.eviction_worklist_generation.record(ew.generation, a);
        i.eviction_worklist_chunks_authored_total
            .record(ew.chunks_authored_total, a);
        for (outcome, count) in [
            ("claimed", ew.chunks_claimed_total.claimed),
            ("busy", ew.chunks_claimed_total.busy),
        ] {
            i.eviction_worklist_chunks_claimed_total
                .record(count, &[KeyValue::new("outcome", outcome)]);
        }
        i.eviction_worklist_chunks_done_total
            .record(ew.chunks_done_total, a);
        i.eviction_worklist_chunks_unusable_total
            .record(ew.chunks_unusable_total, a);
        i.eviction_worklist_items_executed_total
            .record(ew.items_executed_total, a);
        for (reason, count) in [
            ("identity_changed", ew.items_refused_total.identity_changed),
            ("stale_generation", ew.items_refused_total.stale_generation),
        ] {
            i.eviction_worklist_items_refused_total
                .record(count, &[KeyValue::new("reason", reason)]);
        }
        i.eviction_worklist_lease_takeovers_total
            .record(ew.lease_takeovers_total, a);
        for (reason, count) in [
            ("not_capable", ew.author_fallback_total.not_capable),
            ("unknown", ew.author_fallback_total.unknown),
        ] {
            i.eviction_worklist_author_fallback_total
                .record(count, &[KeyValue::new("reason", reason)]);
        }
        i.eviction_worklist_in_flight_bytes
            .record(ew.in_flight_bytes, a);
        // inflight_memory: not an `Option`, and always recorded. The collector publishes the
        // section whether or not a ledger is wired (`ceiling_bytes: 0` is Ledger_Disabled), so
        // there is no absent-observation state to leave a gap for, and every zero here is a true
        // reading of the ledger it describes.
        let im = &metrics.inflight_memory;
        i.inflight_memory_reserved_bytes
            .record(im.reserved_bytes, a);
        i.inflight_memory_ceiling_bytes.record(im.ceiling_bytes, a);
        i.inflight_memory_peak_reserved_bytes
            .record(im.peak_reserved_bytes, a);
        i.inflight_memory_rejected_total
            .record(im.rejected_total, a);
        i.inflight_memory_aborted_accumulations_total
            .record(im.aborted_accumulations_total, a);
        i.inflight_memory_granted_total.record(im.granted_total, a);
        i.inflight_memory_granted_bytes_total
            .record(im.granted_bytes_total, a);
        i.inflight_memory_ram_cache_max_bytes
            .record(im.ram_cache_max_bytes, a);
        i.inflight_memory_buffered_serve_sliced_streams_total
            .record(im.buffered_serve_sliced_streams_total, a);
        i.inflight_memory_buffered_serve_degraded_chained_stream_total
            .record(im.buffered_serve_degraded_chained_stream_total, a);
        i.inflight_memory_buffered_serve_degraded_forward_gap_total
            .record(im.buffered_serve_degraded_forward_gap_total, a);
        i.inflight_memory_buffered_serve_degraded_forward_partial_total
            .record(im.buffered_serve_degraded_forward_partial_total, a);
        i.inflight_memory_buffered_serve_degraded_forward_unresolved_total
            .record(im.buffered_serve_degraded_forward_unresolved_total, a);
        i.inflight_memory_buffered_serve_degraded_forward_stream_failed_total
            .record(im.buffered_serve_degraded_forward_stream_failed_total, a);
        i.inflight_memory_buffered_serve_degraded_forward_partial_cache_merge_total
            .record(
                im.buffered_serve_degraded_forward_partial_cache_merge_total,
                a,
            );
        i.inflight_memory_buffered_serve_degraded_merge_backstop_total
            .record(im.buffered_serve_degraded_merge_backstop_total, a);
        i.inflight_memory_buffered_serve_size_le_64kib_total
            .record(im.buffered_serve_size_le_64kib_total, a);
        i.inflight_memory_buffered_serve_size_le_1mib_total
            .record(im.buffered_serve_size_le_1mib_total, a);
        i.inflight_memory_buffered_serve_size_le_8mib_total
            .record(im.buffered_serve_size_le_8mib_total, a);
        i.inflight_memory_buffered_serve_size_gt_8mib_total
            .record(im.buffered_serve_size_gt_8mib_total, a);
        i.inflight_memory_buffered_serve_peak_materialised_bytes
            .record(im.buffered_serve_peak_materialised_bytes, a);
        i.inflight_memory_buffered_serve_full_object_total
            .record(im.buffered_serve_full_object_total, a);
        // atomic_metadata: an `Option`, so an absent section leaves a gap rather than four zeroes
        // — the convention every optional section on this exporter follows. Always `Some` in the
        // shipped collector, so in practice the gap does not occur.
        if let Some(am) = &metrics.atomic_metadata {
            i.atomic_metadata_lock_acquisitions_successful
                .record(am.metadata_lock_acquisitions_successful, a);
            i.atomic_metadata_lock_acquisitions_failed
                .record(am.metadata_lock_acquisitions_failed, a);
            i.atomic_metadata_lock_hold_time_ms_total
                .record(am.metadata_lock_hold_time_ms_total, a);
            i.atomic_metadata_lock_wait_budget_expired_total
                .record(am.metadata_lock_wait_budget_expired_total, a);
            i.atomic_metadata_range_file_corruption_detected
                .record(am.range_file_corruption_detected, a);
            i.atomic_metadata_range_file_corruption_healed
                .record(am.range_file_corruption_healed, a);
            i.atomic_metadata_range_file_corruption_heal_suppressed
                .record(am.range_file_corruption_heal_suppressed, a);
            i.atomic_metadata_range_file_identity_mismatch_detected
                .record(am.range_file_identity_mismatch_detected, a);
            i.atomic_metadata_journal_validation_failures
                .record(am.journal_validation_failures, a);
        }
        // consolidation: skipped when the section is absent (no consolidator wired, or a
        // size-state read that FAILED, which `collect_consolidation_metrics` now publishes as
        // `None`), AND when `last_consolidation_timestamp` is 0. That zero is the `SizeState`
        // default's `UNIX_EPOCH`, published for a volume no consolidation has run on yet: the
        // three figures beside it are defaults rather than readings, and exporting them would
        // publish "empty cache, never consolidated" for "not yet consolidated". Not recording
        // leaves a gap before the first good read; after one, the SDK re-sends the last good
        // values, and `last_consolidation_timestamp` going stale is then what says so.
        if let Some(cm) = &metrics.consolidation {
            // Outside the timestamp guard: a first cycle stuck holding the lock is exactly the
            // state in which no consolidation has completed yet.
            i.consolidation_consolidation_lock_held_secs
                .record(cm.consolidation_lock_held_secs, a);
            i.consolidation_consolidation_lock_takeovers
                .record(cm.consolidation_lock_takeovers, a);
            if cm.last_consolidation_timestamp > 0 {
                i.consolidation_cache_size_bytes
                    .record(cm.cache_size_bytes, a);
                i.consolidation_cache_write_size_bytes
                    .record(cm.cache_write_size_bytes, a);
                i.consolidation_consolidation_count
                    .record(cm.consolidation_count, a);
                i.consolidation_last_consolidation_timestamp
                    .record(cm.last_consolidation_timestamp, a);
            }
        }
        // hedged_requests: not an `Option`, always recorded. The shipped proxy wires the hedge
        // counters at startup, so a zero is a true reading of a process that has hedged nothing —
        // including one whose rules enable no hedging, which is the default.
        let hr = &metrics.hedged_requests;
        i.hedged_requests_issued.record(hr.issued, a);
        i.hedged_requests_won.record(hr.won, a);
        i.hedged_requests_suppressed.record(hr.suppressed, a);
        // page_cache: not an `Option`. The seven counters are always recorded: `MetricsManager`
        // owns them and the proxy's widening paths record into the same manager this exporter
        // reads, so a zero is a true reading of a process that has widened nothing, which is the
        // default because widening is opt-in per rule. `amplification_ratio` is the exception:
        // `collect_page_cache_metrics` publishes `1.0` when nothing has been widened, a ratio
        // over zero requests rather than a measurement, so it is recorded only once
        // `widened_requests > 0` and the series carries a gap until then.
        let pc = &metrics.page_cache;
        i.page_cache_widened_requests.record(pc.widened_requests, a);
        i.page_cache_bytes_prefetched.record(pc.bytes_prefetched, a);
        if pc.widened_requests > 0 {
            i.page_cache_amplification_ratio
                .record(pc.amplification_ratio, a);
        }
        i.page_cache_page_hits.record(pc.page_hits, a);
        i.page_cache_skipped_signed_range
            .record(pc.skipped_signed_range, a);
        i.page_cache_fallbacks.record(pc.fallbacks, a);
        i.page_cache_ram_page_promotions
            .record(pc.ram_page_promotions, a);
        i.page_cache_ram_page_promotion_skipped
            .record(pc.ram_page_promotion_skipped, a);
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
            // R12.1's pass count. Read it before the four executor counters beside it on
            // `/metrics`: it is what says whether their zeros are a population or an idle process.
            i.cache_eviction_passes_total
                .record(c.eviction_executor.eviction_passes_total, a);
            // The last pass's ACTIVE duration — R12.3's rate denominator. Skipped rather than
            // zeroed when unmeasured (a document written by a build predating the field), because
            // a rate consumer must not divide by a denominator that was never observed.
            if let Some(d) = c.eviction_last_result.as_ref().and_then(|r| r.duration_ms) {
                i.cache_eviction_last_result_duration_ms.record(d, a);
            }
            // R12.3's rates and the counters they are quotients of. The counters always record:
            // a zero is a true reading of a process that has admitted or freed nothing.
            let r = &c.eviction_rates;
            i.cache_eviction_shed_bytes_total
                .record(r.eviction_shed_bytes_total, a);
            i.cache_eviction_shed_ranges_total
                .record(r.eviction_shed_ranges_total, a);
            i.cache_eviction_active_micros_total
                .record(r.eviction_active_micros_total, a);
            i.cache_admission_bytes_total
                .record(r.admission_bytes_total, a);
            i.cache_admission_ranges_total
                .record(r.admission_ranges_total, a);
            // The four rates and the verdict are SKIPPED rather than zeroed when unknown, and that
            // is the same rule the `duration_ms` export above follows for the same reason. A rate's
            // zero means "shedding nothing", so exporting 0.0 for "no pass has completed here"
            // would publish design § 8's false alarm through the one path where a consumer cannot
            // see the `null` that distinguishes them. Recording `keeping_pace` as 0 for unknown
            // would be worse still: 0 is the alarm condition.
            if let Some(v) = r.eviction_shed_bytes_per_sec {
                i.cache_eviction_shed_bytes_per_sec.record(v, a);
            }
            if let Some(v) = r.eviction_shed_ranges_per_sec {
                i.cache_eviction_shed_ranges_per_sec.record(v, a);
            }
            if let Some(v) = r.admission_bytes_per_sec {
                i.cache_admission_bytes_per_sec.record(v, a);
            }
            if let Some(v) = r.admission_ranges_per_sec {
                i.cache_admission_ranges_per_sec.record(v, a);
            }
            if let Some(v) = r.eviction_keeping_pace {
                i.cache_eviction_keeping_pace
                    .record(if v { 1 } else { 0 }, a);
            }
            if let Some(v) = r.eviction_backlog_bytes {
                i.cache_eviction_backlog_bytes.record(v, a);
            }
            // Skipped rather than zeroed when absent, as every other `Option` here is: no
            // reading yet is a third state, and a 0 would export as "exactly at the trigger".
            if let Some(v) = r.eviction_backlog_projected_bytes {
                i.cache_eviction_backlog_projected_bytes.record(v, a);
            }
            // The `st_size` fold's decline counter. Always recorded, never skipped: a zero is a
            // true reading of a process that has never had a tick collide with a running fold,
            // which is the healthy state. A rising value is the fold never completing — and it is
            // the only machine-readable signal for that, since the declined path logs at `debug`.
            i.cache_st_size_folds_declined_overlap_total
                .record(c.st_size_fold.st_size_folds_declined_overlap_total, a);
            // Whole seconds, and `0` covers both "no pass has completed" and "a pass finished
            // inside a second" — so a zero here is not evidence that nothing ran. Read the decline
            // counter above to tell those apart.
            i.cache_st_size_fold_last_duration_secs
                .record(c.st_size_fold.st_size_fold_last_duration_secs, a);
            // Skipped when unknown, not zeroed: `null` means UNREADABLE or INCOMPLETE, and zero is
            // "every shard was scanned just now" — the healthiest possible reading for a state
            // that is the opposite of healthy.
            if let Some(v) = c.st_size_fold.st_size_fold_oldest_shard_scan_age_secs {
                i.cache_st_size_fold_oldest_shard_scan_age_secs.record(v, a);
            }

            // eviction: Tail_Partition, build side. The floor and the cap are the values in force
            // in THIS process, so they are worth exporting even though they are configuration —
            // a fleet running a stale config is otherwise invisible.
            let tp = &c.tail_partition;
            i.cache_tail_floor_bytes.record(tp.tail_floor_bytes, a);
            i.cache_tail_max_entries.record(tp.tail_max_entries, a);
            i.cache_tail_rebuilds_total
                .record(tp.tail_rebuilds_total, a);
            if let Some(v) = tp.tail_entries_last_rebuild {
                i.cache_tail_entries_last_rebuild.record(v, a);
            }
            // Skipped when unknown: `None` is "no rebuild, or an empty partition", and a zero
            // would read as a threshold of nothing.
            if let Some(v) = tp.tail_effective_min_bytes {
                i.cache_tail_effective_min_bytes.record(v, a);
            }
            if let Some(v) = tp.tail_delta_records {
                i.cache_tail_delta_records.record(v, a);
            }

            // eviction: Tail_Partition, selection side.
            let ts = &c.tail_selection;
            // Skipped when unknown rather than zeroed, and here the distinction is sharper than
            // elsewhere: zero is a REAL reading of this gauge — a genuinely sub-floor cache has
            // coverage zero — so a zeroed unknown is indistinguishable from a measured emptiness.
            if let Some(v) = ts.tail_coverage_ratio {
                i.cache_tail_coverage_ratio.record(v, a);
            }
            i.cache_tail_coverage_short_total
                .record(ts.tail_coverage_short_total, a);
            i.cache_eviction_fallback_tail_empty_total
                .record(ts.eviction_fallback_tail_empty_total, a);
            i.cache_eviction_fallback_cap_exhausted_total
                .record(ts.eviction_fallback_cap_exhausted_total, a);
            i.cache_eviction_fallback_unvisited_total
                .record(ts.eviction_fallback_unvisited_total, a);
            i.cache_eviction_oversized_single_candidate_total
                .record(ts.eviction_oversized_single_candidate_total, a);
            i.cache_tail_delta_cap_rewrites_total
                .record(ts.tail_delta_cap_rewrites_total, a);
            // One series per reason. An absent reason records nothing rather than a zero: the
            // reason set is data-dependent, so there is no closed list of series to zero-fill.
            for (reason, count) in &ts.eviction_tail_verdict_total {
                let attrs = &[KeyValue::new("reason", reason.clone())];
                i.cache_eviction_tail_verdict_total.record(*count, attrs);
            }

            // eviction: executor. All plain counters — a zero is a true reading of a population
            // that has had no such event. Whether a pass has run at all is answered by
            // `cache.eviction_passes_total` above, which is why these need no sentinel.
            let ee = &c.eviction_executor;
            i.cache_eviction_pass_path_whole_object_total
                .record(ee.eviction_pass_path_whole_object_total, a);
            i.cache_eviction_pass_path_partial_total
                .record(ee.eviction_pass_path_partial_total, a);
            i.cache_eviction_whole_object_downgraded_total
                .record(ee.eviction_whole_object_downgraded_total, a);
            i.cache_eviction_unlink_failures_total
                .record(ee.eviction_unlink_failures_total, a);
            i.cache_eviction_meta_rewrites_total
                .record(ee.eviction_meta_rewrites_total, a);
            i.cache_eviction_active_key_journal_reads_total
                .record(ee.eviction_active_key_journal_reads_total, a);
            i.cache_eviction_oracle_refreshes_total
                .record(ee.eviction_oracle_refreshes_total, a);
            i.cache_eviction_oracle_stale_verdicts_total
                .record(ee.eviction_oracle_stale_verdicts_total, a);
            // Skipped, not zeroed, until a walk has completed: a zero would read as instant.
            if let Some(v) = ee.eviction_oracle_last_walk_ms {
                i.cache_eviction_oracle_last_walk_ms.record(v, a);
            }

            // eviction: the read tier's last pass. FLEET-WIDE — aggregate last-value or max across
            // instances, never sum. The whole object is absent until a pass completes on the
            // volume, so every series here is skipped rather than zeroed in that state.
            if let Some(lr) = &c.eviction_last_result {
                i.cache_eviction_last_result_completed_at
                    .record(lr.completed_at, a);
                i.cache_eviction_last_result_bytes_requested
                    .record(lr.bytes_requested, a);
                i.cache_eviction_last_result_bytes_freed
                    .record(lr.bytes_freed, a);
                i.cache_eviction_last_result_shortfall_bytes
                    .record(lr.shortfall_bytes, a);
                // Both components of the shortfall are themselves nullable, and the distinction
                // matters: an unmeasured in-progress figure is not a shortfall that was fully
                // explained. Skipped, so the derived "unexplained" series cannot be read as zero
                // when it was never computed.
                if let Some(v) = lr.unevictable_in_progress_bytes {
                    i.cache_eviction_last_result_unevictable_in_progress_bytes
                        .record(v, a);
                }
                if let Some(v) = lr.unexplained_shortfall_bytes {
                    i.cache_eviction_last_result_unexplained_shortfall_bytes
                        .record(v, a);
                }
                i.cache_eviction_last_result_reached_target
                    .record(if lr.reached_target { 1 } else { 0 }, a);
                i.cache_eviction_last_result_ranges_evicted
                    .record(lr.ranges_evicted, a);
            }
        }

        // eviction_coordination: the write tier's own lock-contention counter, previously
        // incremented and documented but absent from both the JSON and this exporter.
        if let Some(ec) = &metrics.eviction_coordination {
            i.eviction_coordination_staging_evictions_skipped_lock_held
                .record(ec.staging_evictions_skipped_lock_held, a);
        }

        // cache_size: R12.6-R12.8's accounting drift, one series per accounted population.
        //
        // `unevaluable_reason` is exported as 1 with the reason as an attribute, which is what
        // makes R12.8's "by name" survive the export: a numeric gauge cannot carry a cause, and
        // mapping the cause to a number would reintroduce exactly the plausible-looking figure
        // R12.8 forbids. An alarm on drift therefore has a companion series telling it whether a
        // missing drift means "no disagreement" or "could not be compared".
        if let Some(cs) = &metrics.cache_size {
            if let Some(ad) = &cs.accounting_drift {
                i.accounting_drift_sampled_at.record(ad.sampled_at, a);
                i.accounting_drift_sample_kind
                    .record(1, &[KeyValue::new("kind", ad.sample_kind.clone())]);
                for (figure, gauge) in [
                    ("total_size", &ad.total_size),
                    ("write_cache_size", &ad.write_cache_size),
                    ("cached_objects", &ad.cached_objects),
                ] {
                    let attrs = &[KeyValue::new("figure", figure)];
                    if let Some(v) = gauge.persisted {
                        i.accounting_drift_persisted.record(v, attrs);
                    }
                    if let Some(v) = gauge.reference {
                        i.accounting_drift_reference.record(v, attrs);
                    }
                    if let Some(v) = gauge.drift {
                        i.accounting_drift_drift.record(v, attrs);
                    }
                    if let Some(v) = gauge.drift_ratio {
                        i.accounting_drift_ratio.record(v, attrs);
                    }
                    if let Some(reason) = &gauge.unevaluable_reason {
                        i.accounting_drift_unevaluable.record(
                            1,
                            &[
                                KeyValue::new("figure", figure),
                                KeyValue::new("reason", reason.clone()),
                            ],
                        );
                    }
                }
            }
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
    pub(crate) fn initialize_per_bucket(
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

        // Build one ObservableCounter for each of the four per-bucket metrics.
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

    /// `src/otlp.rs` with `//` comment lines dropped and all whitespace removed — the form the
    /// four `every_*_field_has_an_otlp_instrument` guards search.
    ///
    /// **Why this exists.** Those guards used to ask whether the **quoted wire name** appeared
    /// anywhere in this file. That is satisfiable by the name appearing in a comment, in a doc
    /// comment, or in any unrelated string literal — none of which creates an instrument.
    /// Measured: with one real construction renamed and the original name left in a single
    /// comment, all four guards passed. The name must now be the sole argument of a
    /// gauge-construction macro, which is the thing that actually creates the instrument.
    ///
    /// **Both normalisations are load-bearing.**
    ///
    /// - Dropping `//` lines is what makes a comment unable to satisfy a guard even when the
    ///   comment reproduces a construction verbatim. `src/otlp.rs` contains no `/* */` block
    ///   comments (checked), so line-prefixed comments are the whole comment surface here; a
    ///   block comment quoting a construction would still satisfy a guard, which is why nobody
    ///   should introduce one.
    /// - Removing ALL whitespace is what keeps the check format-independent. `cargo fmt` wraps a
    ///   long construction so the literal sits on its own line, and
    ///   `cache_hit_buffer.updates_recorded_total` and `cache_hit_buffer.entries_flushed_total`
    ///   are both wrapped today. Anchoring on the same line would reject them, and **a guard
    ///   that fails on correct code gets deleted by the next person**.
    fn instrument_construction_source() -> String {
        include_str!("otlp.rs")
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .flat_map(|line| line.split_whitespace())
            .collect()
    }

    /// Does `src/otlp.rs` construct a gauge instrument whose name is exactly `wire_name`?
    ///
    /// Accepts, and only accepts, `wire_name` as the sole literal argument of one of the three
    /// numeric gauge macros — `g_u64!`, `g_f64!`, `g_i64!` — in either formatting, since
    /// `g_u64!("x")` and the `cargo fmt`-wrapped `g_u64!(\n    "x"\n)` are the same text once
    /// whitespace is gone.
    ///
    /// It does NOT accept a name assembled by concatenation, nor one constructed through a macro
    /// not listed here. `make_counter!`, which builds the per-bucket observable counters, is
    /// deliberately out of scope: none of the four guarded sections is per-bucket, and those
    /// names carry an `s3proxy.` prefix rather than a JSON path. A future section exported
    /// through a new macro must add it to this list — the guard's failure names the field, so
    /// that is a discoverable one-line fix rather than a mystery.
    fn names_a_gauge_instrument(constructions: &str, wire_name: &str) -> bool {
        ["g_u64!", "g_f64!", "g_i64!"]
            .iter()
            .any(|gauge_macro| constructions.contains(&format!("{gauge_macro}(\"{wire_name}\")")))
    }

    /// Every eviction-mechanism `/metrics` field must have an OTLP instrument naming it.
    ///
    /// This exporter selects per field by hand — the JSON path is an instrument's **name string**,
    /// not a lookup — so a new `/metrics` key is invisible to an OTLP-only deployment until
    /// someone writes an instrument for it. Nothing made that omission fail, which is how a
    /// three-field residual grew to twenty-three across four units that each deferred it.
    ///
    /// The field list is not hardcoded: it is read back out of the gauge structs by serialising
    /// them, so it is the real wire shape including `#[serde(flatten)]`. Add a field to any of
    /// these structs and this test fails until an instrument exists. Rename one and it fails too,
    /// because the old name string no longer matches a live field.
    #[test]
    fn every_eviction_mechanism_field_has_an_otlp_instrument() {
        let constructions = instrument_construction_source();
        let mut missing: Vec<String> = Vec::new();

        // The five flattened gauge groups. Each one's Rust field name IS its `cache.*` wire key —
        // `src/metrics.rs` has no `#[serde(rename)]`, and these are flattened into `CacheMetrics`.
        let groups = [
            serde_json::to_value(crate::st_size_fold::StSizeFoldGauges::default()).unwrap(),
            serde_json::to_value(crate::cache_size_tracker::TailPartitionGauges::default())
                .unwrap(),
            serde_json::to_value(crate::cache::TailSelectionGauges::default()).unwrap(),
            serde_json::to_value(crate::cache::EvictionExecutorGauges::default()).unwrap(),
            serde_json::to_value(crate::metrics::EvictionRateGauges::default()).unwrap(),
        ];
        for group in &groups {
            for key in group
                .as_object()
                .expect("gauge group is a JSON object")
                .keys()
            {
                if !names_a_gauge_instrument(&constructions, &format!("cache.{key}")) {
                    missing.push(format!("cache.{key}"));
                }
            }
        }

        // The nested last-pass document. Written as a full struct literal deliberately: it has no
        // `Default`, so adding a sub-field breaks this test's compilation and forces an explicit
        // decision about whether the new figure is exported.
        let last_result = crate::eviction_result::EvictionPassResult {
            schema: 1,
            instance_id: String::new(),
            completed_at: 0,
            bytes_requested: 0,
            bytes_freed: 0,
            shortfall_bytes: 0,
            unevictable_in_progress_bytes: None,
            unexplained_shortfall_bytes: None,
            reached_target: false,
            ranges_evicted: 0,
            duration_ms: None,
            mode: crate::eviction_result::EvictionMode::Legacy,
        };
        // `schema` is a format version and `instance_id` is a string — neither is a measurement,
        // and a string cannot be a gauge value. Both are excluded on purpose; see the instrument
        // block's comment. `mode` is a string too; DR-EVICT task 50 decides its export. Every
        // other sub-field must be exported.
        let deliberately_unexported = ["schema", "instance_id", "mode"];
        let lr = serde_json::to_value(&last_result).unwrap();
        for key in lr.as_object().expect("last result is a JSON object").keys() {
            if deliberately_unexported.contains(&key.as_str()) {
                continue;
            }
            if !names_a_gauge_instrument(
                &constructions,
                &format!("cache.eviction_last_result.{key}"),
            ) {
                missing.push(format!("cache.eviction_last_result.{key}"));
            }
        }

        assert!(
            missing.is_empty(),
            "{} eviction-mechanism /metrics field(s) have no OTLP instrument, so an OTLP-only \
             deployment cannot see them: {:?}. Add one instrument per field in `Instruments`, \
             construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// Every `metrics_collection.*` field has an OTLP instrument.
    ///
    /// The same rule as the test above, held for the same reason, and derived the same way — the
    /// field list is serialised out of the struct rather than hardcoded, so adding a field without
    /// an instrument fails here instead of shipping invisible.
    ///
    /// **A separate test rather than a sixth entry in that one's `groups` array**, because those
    /// five are all `#[serde(flatten)]`ed into `CacheMetrics` and so share the `cache.` prefix.
    /// These sit in their own top-level section, so they take their own prefix, and folding them in
    /// would mean parameterising that test's one hardcoded prefix — which is the string the whole
    /// check turns on.
    ///
    /// The OTLP-only deployment is the one this protects most: the collection being measured is
    /// the background collection that records these instruments, so a slow one delays every OTLP
    /// sample, and these figures are the only way an OTLP-only operator can see why.
    #[test]
    fn every_metrics_collection_field_has_an_otlp_instrument() {
        let constructions = instrument_construction_source();
        let group =
            serde_json::to_value(crate::metrics::MetricsCollectionGauges::default()).unwrap();
        let mut missing: Vec<String> = Vec::new();
        for key in group
            .as_object()
            .expect("the collection gauges are a JSON object")
            .keys()
        {
            if !names_a_gauge_instrument(&constructions, &format!("metrics_collection.{key}")) {
                missing.push(format!("metrics_collection.{key}"));
            }
        }
        assert!(
            missing.is_empty(),
            "{} metrics_collection field(s) have no OTLP instrument, so an OTLP-only deployment \
             cannot see them: {:?}. Add one instrument per field in `Instruments`, construct it \
             with the JSON path as its name string, and record it in `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// Every `cache_hit_buffer.*` field has an OTLP instrument.
    ///
    /// The same rule as the three tests above, held for the same reason and derived the same way:
    /// the field list is serialised out of the struct rather than hardcoded, so adding a field
    /// without an instrument fails here instead of shipping invisible to an OTLP-only deployment.
    /// A hardcoded list would pass forever on a struct that grows, which is the shape of this
    /// repo's trap 1 — an assertion that cannot fail.
    ///
    /// **A fourth test rather than an entry in the eviction test's `groups` array**, for the reason
    /// the two tests above record: those five are `#[serde(flatten)]`ed into `CacheMetrics` and
    /// share the `cache.` prefix, which is the one hardcoded string that check turns on. This is a
    /// top-level section of its own and takes its own prefix.
    ///
    /// **There is no `deliberately_unexported` list and there should not be one.** All five fields
    /// are measurements; the struct has no `schema` and no `instance_id`, so the eviction test's
    /// two exclusions have no analogue here.
    ///
    /// **This section is an `Option` and that is not an exemption.** An absent section is recorded
    /// as nothing rather than as zeroes, deliberately, but the instrument must still exist — the
    /// gap is what carries the meaning, and a series that was never declared cannot have a gap.
    #[test]
    fn every_cache_hit_buffer_field_has_an_otlp_instrument() {
        let constructions = instrument_construction_source();
        let group = serde_json::to_value(crate::metrics::CacheHitBufferMetrics::default()).unwrap();
        let keys = group
            .as_object()
            .expect("the cache-hit-buffer gauges are a JSON object");
        // A struct that serialised to nothing would pass the loop below vacuously — this repo's
        // trap 1 — so the population is asserted to be non-empty first.
        assert!(
            !keys.is_empty(),
            "the gauges must serialise to at least one field, or the check below proves nothing"
        );
        let mut missing: Vec<String> = Vec::new();
        for key in keys.keys() {
            if !names_a_gauge_instrument(&constructions, &format!("cache_hit_buffer.{key}")) {
                missing.push(format!("cache_hit_buffer.{key}"));
            }
        }
        assert!(
            missing.is_empty(),
            "{} cache_hit_buffer field(s) have no OTLP instrument, so an OTLP-only deployment \
             cannot see the journal write-behind queue: {:?}. Add one instrument per field in \
             `Instruments`, construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// The fields of one top-level `/metrics` section with no gauge instrument named
    /// `{section}.{field}`, the field list serialised out of `value`.
    ///
    /// Shared by the per-section guards below, which are otherwise identical to
    /// `every_cache_hit_buffer_field_has_an_otlp_instrument`. The population is asserted non-empty
    /// here, once, so no caller can forget it — a section that serialised to nothing would pass
    /// the loop vacuously, this repo's trap 1.
    ///
    /// **Flattening.** The field list is the SERIALISED shape, so a `#[serde(flatten)]`ed child
    /// would contribute its own keys rather than one nested key; a nested (non-flattened) child
    /// would appear as a single object-valued key and fail here naming it, which forces a
    /// decision rather than hiding the child. None of the four sections guarded with this helper
    /// has either (checked 2026-09-25): every field is a scalar.
    fn section_fields_without_an_instrument(
        section: &str,
        value: serde_json::Value,
    ) -> Vec<String> {
        let constructions = instrument_construction_source();
        let keys = value
            .as_object()
            .unwrap_or_else(|| panic!("the {section} section serialises to a JSON object"))
            .clone();
        assert!(
            !keys.is_empty(),
            "the {section} section must serialise to at least one field, or the check proves \
             nothing"
        );
        keys.keys()
            .map(|key| format!("{section}.{key}"))
            .filter(|wire_name| !names_a_gauge_instrument(&constructions, wire_name))
            .collect()
    }

    /// Every `inflight_memory.*` field has an OTLP instrument.
    ///
    /// `rejected_total` is the case this guards: it is the only count of memory-pressure sheds on
    /// either transport, and before this guard existed the section was `/metrics`-only, so an
    /// OTLP-only deployment could not see its proxy refusing requests for memory at all.
    ///
    /// # RED SIDE
    ///
    /// With the `inflight_memory.rejected_total` construction deleted, this fails naming
    /// `inflight_memory.rejected_total`.
    /// Every `fill_visibility.*` field has an OTLP instrument.
    ///
    /// # RED SIDE
    ///
    /// With the `fill_visibility.dropped_age_total` construction deleted, this fails naming
    /// `fill_visibility.dropped_age_total`.
    #[test]
    fn every_fill_visibility_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "fill_visibility",
            serde_json::to_value(crate::metrics::FillVisibilityMetrics::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} fill_visibility field(s) have no OTLP instrument: {:?}. Add one instrument per \
             field in `Instruments`, construct it with the JSON path as its name string, and \
             record it in `export_metrics`.",
            missing.len(),
            missing
        );
    }
    /// Every `orphan_reclaim.*` field has an OTLP instrument. `journal_evidence_total` is one
    /// instrument carrying a `result` attribute rather than one per outcome.
    ///
    /// # RED SIDE
    ///
    /// With the `orphan_reclaim.bytes_reclaimed` construction deleted, this fails naming
    /// `orphan_reclaim.bytes_reclaimed`.
    #[test]
    fn every_orphan_reclaim_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "orphan_reclaim",
            serde_json::to_value(crate::metrics::OrphanReclaimMetrics::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} orphan_reclaim field(s) have no OTLP instrument: {:?}",
            missing.len(),
            missing
        );
    }
    /// Every `eviction_worklist.*` field has an OTLP instrument. The three nested totals are one
    /// instrument each, carrying an `outcome` or `reason` attribute.
    ///
    /// # RED SIDE
    ///
    /// With the `eviction_worklist.chunks_done_total` construction deleted, this fails naming
    /// `eviction_worklist.chunks_done_total`.
    #[test]
    fn every_eviction_worklist_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "eviction_worklist",
            serde_json::to_value(crate::metrics::EvictionWorklistMetrics::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} eviction_worklist field(s) have no OTLP instrument: {:?}",
            missing.len(),
            missing
        );
    }
    #[test]
    fn every_inflight_memory_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "inflight_memory",
            serde_json::to_value(crate::metrics::InflightMemoryMetrics::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} inflight_memory field(s) have no OTLP instrument, so an OTLP-only deployment \
             cannot see memory-pressure sheds: {:?}. Add one instrument per field in \
             `Instruments`, construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// Every `atomic_metadata.*` field has an OTLP instrument.
    ///
    /// # RED SIDE
    ///
    /// With the `atomic_metadata.metadata_lock_wait_budget_expired_total` instrument deleted at
    /// every site, this fails naming that field (taken while the section still carried eight
    /// never-recorded fields, which 3.0.0 removes).
    #[test]
    fn every_atomic_metadata_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "atomic_metadata",
            serde_json::to_value(crate::metrics::AtomicMetadataMetrics::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} atomic_metadata field(s) have no OTLP instrument, so an OTLP-only deployment \
             cannot see metadata slot-lock failures: {:?}. Add one instrument per field in \
             `Instruments`, construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// Every `consolidation.*` field has an OTLP instrument.
    ///
    /// `last_consolidation_timestamp` is the case this guards: it is the only figure on either
    /// transport whose staleness says the consolidator has stopped, since a stalled consolidator
    /// otherwise leaves every size figure flat exactly as an idle cache does.
    ///
    /// # RED SIDE
    ///
    /// With the `consolidation.last_consolidation_timestamp` instrument deleted at every site,
    /// this fails naming that field.
    #[test]
    fn every_consolidation_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "consolidation",
            serde_json::to_value(crate::metrics::ConsolidationMetrics::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} consolidation field(s) have no OTLP instrument, so an OTLP-only deployment cannot \
             see whether the consolidator is running: {:?}. Add one instrument per field in \
             `Instruments`, construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// Every `hedged_requests.*` field has an OTLP instrument.
    ///
    /// `issued` is the case this guards: it is the count of duplicate S3 requests hedging bills,
    /// and a cost figure an OTLP-only operator cannot see is one they cannot budget.
    ///
    /// # RED SIDE
    ///
    /// With the `hedged_requests.issued` instrument deleted at every site, this fails naming that
    /// field.
    #[test]
    fn every_hedged_requests_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "hedged_requests",
            serde_json::to_value(crate::metrics::HedgingStats::default()).unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} hedged_requests field(s) have no OTLP instrument, so an OTLP-only deployment \
             cannot see what hedging costs: {:?}. Add one instrument per field in `Instruments`, \
             construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// The `connection_pool.success_rate_percent` placeholder is not exported while it is a
    /// literal, and must be exported once it is not.
    ///
    /// Read from `collect_connection_pool_metrics`' own body, whitespace removed, so the check
    /// follows the only production construction site rather than a copy of it. Two directions:
    ///
    /// - While the collector assigns the literal, no gauge may be constructed under that name — a
    ///   constant on OTLP is the defect this guards (`success_rate_percent: 1.0` reads as 1%).
    /// - Once the literal is gone the figure is presumably measured, and this fails asking for an
    ///   instrument, so a newly real figure cannot stay `/metrics`-only by omission.
    ///
    /// # RED SIDE
    ///
    /// With `conn_success_rate: g_f64!("connection_pool.success_rate_percent")` restored in
    /// `initialize`, this fails naming `connection_pool.success_rate_percent`.
    #[test]
    fn connection_pool_placeholders_are_not_exported() {
        let metrics_src = include_str!("metrics.rs");
        let start = metrics_src
            .find("async fn collect_connection_pool_metrics(")
            .expect("the connection-pool collector exists in src/metrics.rs");
        let body_len = metrics_src[start..]
            .find("\n    }\n")
            .expect("the collector's body ends at a four-space closing brace");
        let body: String = metrics_src[start..start + body_len]
            .split_whitespace()
            .collect();
        assert!(
            body.contains("Some(ConnectionPoolMetrics{"),
            "the extracted collector body does not construct ConnectionPoolMetrics, so the checks \
             below prove nothing: {body}"
        );

        let constructions = instrument_construction_source();
        let (field, literal) = ("success_rate_percent", "1.0");
        let wire = format!("connection_pool.{field}");
        let still_literal = body.contains(&format!("{field}:{literal},"));
        let exported = names_a_gauge_instrument(&constructions, &wire);
        if still_literal {
            assert!(
                !exported,
                "`{wire}` is exported over OTLP but the collector still assigns the literal \
                 `{literal}`, so the series is a constant. Remove the instrument, or measure \
                 the figure first."
            );
        } else {
            assert!(
                exported,
                "`{wire}` is no longer the literal `{literal}` in \
                 collect_connection_pool_metrics, so it is presumably measured now — give it \
                 an OTLP instrument and update this test."
            );
        }
    }

    /// Every `page_cache.*` field has an OTLP instrument.
    ///
    /// `bytes_prefetched` is the case this guards: it is the extra S3 transfer page widening
    /// costs, and before these existed an OTLP-only operator could not see it at all.
    ///
    /// # RED SIDE
    ///
    /// With the `page_cache.bytes_prefetched` construction deleted, this fails naming it.
    #[test]
    fn every_page_cache_field_has_an_otlp_instrument() {
        let missing = section_fields_without_an_instrument(
            "page_cache",
            serde_json::to_value(crate::metrics::PageCacheMetrics {
                widened_requests: 0,
                bytes_prefetched: 0,
                amplification_ratio: 1.0,
                page_hits: 0,
                skipped_signed_range: 0,
                fallbacks: 0,
                ram_page_promotions: 0,
                ram_page_promotion_skipped: 0,
            })
            .unwrap(),
        );
        assert!(
            missing.is_empty(),
            "{} page_cache field(s) have no OTLP instrument, so an OTLP-only deployment cannot \
             see what page widening costs: {:?}. Add one instrument per field in `Instruments`, \
             construct it with the JSON path as its name string, and record it in \
             `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// Every `/metrics` `bucket_traffic` field has a per-bucket OTLP counter behind
    /// `metrics.otlp.per_bucket_enabled`, except `bytes_saved`, which is excluded on purpose.
    ///
    /// The per-bucket counters are `make_counter!` observables in `initialize_per_bucket`, named
    /// after S3's request metrics rather than the JSON path, so the correspondence is written out
    /// here. `bytes_saved` is excluded because `per-bucket-metrics` recorded a customer
    /// requirement that the proxy not emit a cached-bytes-saved metric over OTLP (it is derived in
    /// CloudWatch as `proxy.BytesDownloaded - s3.BytesDownloaded`), and `docs/OTLP_METRICS.md`
    /// says it is not exported. A new `BucketTrafficStats` field fails this test until it is
    /// either given a counter or listed as excluded, and exporting `bytes_saved` fails it until
    /// the exclusion is removed.
    ///
    /// # RED SIDE
    ///
    /// With the `s3proxy.get_requests` counter deleted from `initialize_per_bucket`, this fails
    /// naming `get_requests`.
    #[test]
    fn every_bucket_traffic_field_has_a_per_bucket_counter_or_a_recorded_exclusion() {
        const COUNTERS: [(&str, &str); 4] = [
            ("bytes_served", "s3proxy.bytes_downloaded"),
            ("bytes_uploaded", "s3proxy.bytes_uploaded"),
            ("get_requests", "s3proxy.get_requests"),
            ("put_requests", "s3proxy.put_requests"),
        ];
        const EXCLUDED: [&str; 1] = ["bytes_saved"];

        let constructions = instrument_construction_source();
        let fields: Vec<String> =
            serde_json::to_value(crate::metrics::BucketTrafficStats::default())
                .unwrap()
                .as_object()
                .expect("BucketTrafficStats serialises to a JSON object")
                .keys()
                .cloned()
                .collect();
        assert!(
            !fields.is_empty(),
            "BucketTrafficStats must serialise to at least one field, or this proves nothing"
        );

        let unaccounted: Vec<&String> = fields
            .iter()
            .filter(|f| {
                !COUNTERS.iter().any(|(field, _)| field == f) && !EXCLUDED.contains(&f.as_str())
            })
            .collect();
        assert!(
            unaccounted.is_empty(),
            "bucket_traffic field(s) {:?} have neither a per-bucket OTLP counter nor a recorded \
             exclusion. Add a `make_counter!` in `initialize_per_bucket` behind \
             `per_bucket_enabled` and list it in COUNTERS, or list it in EXCLUDED with the reason.",
            unaccounted
        );

        let unconstructed: Vec<&str> = COUNTERS
            .iter()
            .filter(|(field, name)| {
                !constructions.contains(&format!("make_counter!(\"{name}\",{field})"))
            })
            .map(|(field, _)| *field)
            .collect();
        assert!(
            unconstructed.is_empty(),
            "bucket_traffic field(s) {:?} have lost their per-bucket OTLP counter in \
             `initialize_per_bucket`",
            unconstructed
        );

        for field in EXCLUDED {
            assert!(
                !constructions.contains(&format!("\",{field})")),
                "`{field}` is now exported per bucket; remove it from EXCLUDED and correct \
                 docs/OTLP_METRICS.md, which says it is not exported"
            );
        }
    }

    /// Every `cache.metadata_cache_*` field has an OTLP instrument.
    ///
    /// The same rule as the four tests above. **The derivation is different, and it has to be**:
    /// those four serialise a `Default` instance of a gauge struct to recover the wire shape, and
    /// `CacheMetrics` has **no `Default`** — it is a 100-plus-field struct whose every literal is
    /// written out in full, which is deliberate, since that exhaustiveness is what makes adding a
    /// field a compile error at every construction site. So the population is read out of the
    /// `pub metadata_cache_*` declarations in `src/metrics.rs` instead.
    ///
    /// **Why a source-text population is acceptable here and would not be elsewhere.** The risk of
    /// reading source rather than a serialised value is that the text and the wire shape can
    /// disagree. They cannot for these six: `src/metrics.rs` carries no `#[serde(rename)]` at all,
    /// so a `CacheMetrics` field name **is** its JSON key, and these six are not behind a
    /// `#[serde(flatten)]`ed sub-struct. A field added to `CacheMetrics` named `metadata_cache_*`
    /// is picked up automatically, which is the property that matters — a hardcoded list would pass
    /// forever on a struct that grows, this repo's trap 1.
    ///
    /// **No exclusion list, deliberately.** `cache.metadata_cache_max_entries` was the only one of
    /// the six with no instrument, and it is now exported rather than excluded. An exclusion list is
    /// precisely where the next unexported field lands silently — the eviction guard above records
    /// a three-field residual that grew to twenty-three because nothing failed — and a capacity is
    /// not a reason to exclude in any case, since `cache.max_cache_size_limit` is a capacity and is
    /// exported. `cache.ram_cache_max_size` is still unexported; that is a separate pre-existing
    /// gap in the RAM tier, outside this guard's prefix and not addressed here.
    ///
    /// **The population also covers `pub metadata_read_*`**, the `.meta` reader's own fault
    /// counters (`metadata_read_stale_handles_total`, `metadata_read_stale_handle_misses_total`).
    /// They are not the RAM tier, but they are the same metadata read path's health signal and an
    /// OTLP-only deployment has no other route to a shared-volume stale-handle fault.
    ///
    /// # RED SIDE
    ///
    /// With `metadata_cache_head_hits`' `g_u64!` construction deleted, this fails naming
    /// `cache.metadata_cache_head_hits`.
    #[test]
    fn every_metadata_cache_field_has_an_otlp_instrument() {
        let constructions = instrument_construction_source();

        // The population, from the declarations rather than from a serialised value. `//` lines are
        // dropped first so a doc comment mentioning a field name cannot invent one.
        let fields: Vec<String> = include_str!("metrics.rs")
            .lines()
            .map(|line| line.trim())
            .filter(|line| {
                !line.starts_with("//")
                    && (line.starts_with("pub metadata_cache_")
                        || line.starts_with("pub metadata_read_"))
            })
            .filter_map(|line| line.strip_prefix("pub "))
            .filter_map(|decl| decl.split(':').next())
            .map(|name| name.to_string())
            .collect();

        // Two preconditions, because a population this test derives itself can fail to derive
        // anything and then pass vacuously — trap 1.
        assert!(
            fields.len() >= 6,
            "the extractor found {} `pub metadata_cache_*` declaration(s) in src/metrics.rs, and \
             there are at least six. A parsing change has broken the population, so the loop below \
             proves nothing: {:?}",
            fields.len(),
            fields
        );
        for expected in [
            "metadata_cache_hits",
            "metadata_cache_max_entries",
            "metadata_cache_head_hits",
            "metadata_read_stale_handles_total",
            "metadata_read_stale_handle_misses_total",
        ] {
            assert!(
                fields.iter().any(|f| f == expected),
                "the extractor must find `{expected}`, which is declared on `CacheMetrics`. It \
                 found {fields:?} — if the declaration moved or was reformatted, fix the extractor \
                 rather than this assertion."
            );
        }

        let mut missing: Vec<String> = Vec::new();
        for field in &fields {
            if !names_a_gauge_instrument(&constructions, &format!("cache.{field}")) {
                missing.push(format!("cache.{field}"));
            }
        }
        assert!(
            missing.is_empty(),
            "{} metadata-cache field(s) have no OTLP instrument, so an OTLP-only deployment cannot \
             see the metadata tier: {:?}. Add one instrument per field in `Instruments`, construct \
             it with the JSON path as its name string, and record it in `export_metrics`.",
            missing.len(),
            missing
        );
    }

    /// `initialize()` runs inside the Tokio runtime; `opentelemetry-otlp` builds
    /// its blocking reqwest client on a helper thread so that this works. An init
    /// failure here would silently disable all telemetry.
    #[tokio::test]
    async fn otlp_exporter_initializes_inside_tokio_runtime() {
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

    /// Regression guard for the reader thread: opentelemetry_sdk's PeriodicReader
    /// exports from a plain thread via `futures_executor::block_on`, so the
    /// exporter's HTTP client must not need a Tokio reactor. With the async
    /// `reqwest-client` feature the first export panics ("there is no reactor
    /// running"), the reader thread dies, and the process never pushes a payload —
    /// silently. Drives one real export over HTTP into a local listener and checks
    /// the payload carries the identity Prometheus derives `instance` from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_exporter_exports_over_http() {
        let (addr, rx) = one_shot_otlp_listener().await;
        let cfg = OtlpConfig {
            enabled: true,
            endpoint: format!("http://{}/api/v1/otlp/v1/metrics", addr),
            export_interval: Duration::from_secs(3600), // only the flush below exports
            timeout: Duration::from_secs(10),
            ..OtlpConfig::default()
        };
        let mut exporter = OtlpExporter::new(cfg);
        exporter.initialize().await.expect("init");
        let request = force_one_export(&exporter, rx).await;
        assert_eq!(
            request_line(&request),
            "POST /api/v1/otlp/v1/metrics HTTP/1.1"
        );
        let has = |needle: &[u8]| request.windows(needle.len()).any(|w| w == needle);
        assert!(
            has(b"service.instance.id"),
            "payload lacks service.instance.id"
        );
        assert!(has(b"s3-proxy"), "payload lacks service.name");
        let _ = exporter.shutdown().await;
    }

    /// The shipped default, `http://localhost:4318`, and every documented example are bare
    /// `scheme://host:port` URLs, and `opentelemetry-otlp` sends to a programmatically provided
    /// endpoint verbatim. So this asserts the PATH of the request the real exporter sends, not
    /// the output of the normalising function: a bare endpoint must reach `/v1/metrics`, which
    /// is where the CloudWatch agent and an OpenTelemetry Collector listen.
    ///
    /// Red side: with `effective_metrics_endpoint` bypassed in `initialize`, the request line is
    /// `POST / HTTP/1.1`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_exporter_bare_endpoint_exports_to_v1_metrics() {
        let (addr, rx) = one_shot_otlp_listener().await;
        let cfg = OtlpConfig {
            enabled: true,
            endpoint: format!("http://{}", addr),
            export_interval: Duration::from_secs(3600), // only the flush below exports
            timeout: Duration::from_secs(10),
            ..OtlpConfig::default()
        };
        let mut exporter = OtlpExporter::new(cfg);
        exporter.initialize().await.expect("init");
        let request = force_one_export(&exporter, rx).await;
        assert_eq!(
            request_line(&request),
            "POST /v1/metrics HTTP/1.1",
            "a bare OTLP endpoint must export to the OTLP/HTTP metrics path"
        );
        let _ = exporter.shutdown().await;
    }

    /// The normalisation rule, one case per shape an operator is likely to write. The exporter
    /// test above proves the function's result is what goes on the wire; this pins the rule.
    #[test]
    fn effective_metrics_endpoint_appends_the_path_only_when_there_is_none() {
        let cases = [
            // no path, and a root path: the documented bare forms
            ("http://localhost:4318", "http://localhost:4318/v1/metrics"),
            ("http://127.0.0.1:4318/", "http://127.0.0.1:4318/v1/metrics"),
            (
                "https://collector.example:4318",
                "https://collector.example:4318/v1/metrics",
            ),
            ("http://[::1]:4318", "http://[::1]:4318/v1/metrics"),
            // a query string survives, after the added path
            (
                "http://localhost:4318?tenant=a",
                "http://localhost:4318/v1/metrics?tenant=a",
            ),
            // a full path, byte-identical: the fleet's own config and this module's fixtures
            (
                "http://localhost:4318/v1/metrics",
                "http://localhost:4318/v1/metrics",
            ),
            // a custom collector path, untouched
            (
                "http://127.0.0.1:9090/api/v1/otlp/v1/metrics",
                "http://127.0.0.1:9090/api/v1/otlp/v1/metrics",
            ),
            // any other path is the operator's, including a trailing slash
            ("http://localhost:4318/otlp/", "http://localhost:4318/otlp/"),
            // not an absolute URL: passed through, so the exporter build reports it as before
            ("localhost:4318", "localhost:4318"),
            ("http://bad host:4318", "http://bad host:4318"),
        ];
        for (configured, expected) in cases {
            assert_eq!(
                effective_metrics_endpoint(configured),
                expected,
                "configured endpoint {configured:?}"
            );
        }
    }

    /// An endpoint that does not parse must still fail initialisation, as it did before the
    /// normalisation existed. Passing it through unchanged is what keeps that error.
    #[tokio::test]
    async fn otlp_exporter_unparseable_endpoint_still_fails_init() {
        let cfg = OtlpConfig {
            enabled: true,
            endpoint: "http://bad host:4318".to_string(),
            ..OtlpConfig::default()
        };
        let mut exporter = OtlpExporter::new(cfg);
        let err = exporter
            .initialize()
            .await
            .expect_err("an unparseable endpoint must fail init, not export silently");
        assert!(
            err.to_string().contains("OTLP build failed"),
            "unexpected error: {err}"
        );
    }

    /// Accept ONE HTTP request on a local listener, answer `200`, and hand back its raw bytes.
    async fn one_shot_otlp_listener() -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Receiver<Vec<u8>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let (body_start, content_length) = loop {
                let n = sock.read(&mut chunk).await.unwrap();
                assert!(n > 0, "client hung up before the headers were complete");
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                    let len = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .map(|v| v.trim().parse::<usize>().unwrap())
                        .unwrap_or(0);
                    break (pos + 4, len);
                }
            };
            while buf.len() < body_start + content_length {
                let n = sock.read(&mut chunk).await.unwrap();
                assert!(n > 0, "client hung up mid-body");
                buf.extend_from_slice(&chunk[..n]);
            }
            sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            let _ = tx.send(buf);
        });
        (addr, rx)
    }

    /// Record one value, force the reader to export, and return the request the listener saw.
    async fn force_one_export(
        exporter: &OtlpExporter,
        rx: tokio::sync::oneshot::Receiver<Vec<u8>>,
    ) -> Vec<u8> {
        // The reader skips an empty collection, so record one value first.
        exporter
            .instruments
            .as_ref()
            .unwrap()
            .uptime_seconds
            .record(1, &[]);
        let provider = exporter.provider.clone().unwrap();
        // force_flush blocks on the reader thread's round trip; keep it off the
        // runtime workers so the listener task can answer.
        tokio::task::spawn_blocking(move || provider.force_flush())
            .await
            .unwrap()
            .expect("force_flush: the reader thread must be alive and the export must succeed");

        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("no export reached the listener within 10s")
            .unwrap()
    }

    /// The HTTP request line of a raw request, e.g. `POST /v1/metrics HTTP/1.1`.
    fn request_line(request: &[u8]) -> String {
        String::from_utf8_lossy(request)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    /// Every gauge name constructed in the production half of this file, in source order.
    ///
    /// Read from the same whitespace-free, comment-free text the `every_*` guards use, and cut at
    /// the first `#[cfg(test)]` so nothing in this module can contribute a name. A construction is
    /// `g_u64!("name")`, `g_f64!("name")` or `g_i64!("name")`, which is the only way `initialize`
    /// builds a gauge; `names_a_gauge_instrument` carries the same list.
    fn constructed_gauge_names() -> Vec<String> {
        let source = instrument_construction_source();
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("split always yields a first piece");
        let mut names = Vec::new();
        for gauge_macro in ["g_u64!(\"", "g_f64!(\"", "g_i64!(\""] {
            let mut rest = production;
            while let Some(at) = rest.find(gauge_macro) {
                rest = &rest[at + gauge_macro.len()..];
                let end = rest
                    .find("\")")
                    .expect("a gauge construction's literal is closed by `\")`");
                names.push(rest[..end].to_string());
                rest = &rest[end..];
            }
        }
        names.sort();
        names.dedup();
        names
    }

    /// Does an OTLP protobuf body carry a `Metric` named exactly `name`?
    ///
    /// `Metric.name` is field 1, length-delimited, so on the wire it is `0x0A`, a varint length,
    /// then the bytes. Matching the tag and the length as well as the bytes is what stops
    /// `cache.evictions` being found inside `cache.evictions_total`-style neighbours. Every name
    /// here is under 128 bytes, so the length is one byte; asserted rather than assumed.
    fn body_names_metric(body: &[u8], name: &str) -> bool {
        assert!(
            name.len() < 128,
            "{name:?} needs a multi-byte varint length"
        );
        let mut needle = vec![0x0A, name.len() as u8];
        needle.extend_from_slice(name.as_bytes());
        body.windows(needle.len()).any(|w| w == needle.as_slice())
    }

    /// A `SystemMetrics` in which every branch of `export_metrics` records.
    ///
    /// Every `Option` section is `Some`, every nested `Option` gauge is `Some` except the staged
    /// tier's byte figure (which this file may not spell), every map that
    /// `export_metrics` iterates has an entry, every `> 0` guard is satisfied, and the
    /// accounting-drift sample has one evaluable figure and one unevaluable one (a `None`
    /// reference for `cached_objects`), because `accounting_drift.*` records the values and the
    /// `unevaluable` reason on disjoint branches. A literal rather than a `Default` wherever the
    /// type allows it, so a new field on `CacheMetrics` breaks this build rather than silently
    /// escaping the wire check. `connection_pool` and `signed_put` stay `None`: `export_metrics`
    /// records nothing from either.
    fn every_branch_recording_system_metrics() -> SystemMetrics {
        use crate::cache_size_tracker::{AccountingDriftSample, CacheSizeMetrics};
        use crate::eviction_result::{EvictionPassResult, WriteCacheCountPassResult};
        use crate::metrics::{
            CacheHitBufferMetrics, CacheMetrics, CacheRulesMetrics, CoalescingMetrics,
            CompressionMetrics, ConsolidationMetrics, EvictionCoordinationMetrics,
            PageCacheMetrics, RequestMetrics, WriteCacheMetrics,
        };
        use crate::staged_fold::{ShardDurationClass, ShardDurationHistogram};
        use std::time::SystemTime;

        // The staged tier, set field by field on a default rather than as a literal of its gauge
        // struct: `staged_fold::tests::the_fold_is_the_only_staged_authority_and_it_has_one_
        // decision_surface` forbids this file from naming that struct or its byte figure, so the
        // byte figure is set through a test-only setter in `staged_fold.rs`, which may name it.
        let mut write_cache = WriteCacheMetrics::default();
        write_cache.staged_fold.set_staged_bytes_for_test(128);
        write_cache.staged_fold.staged_fold_in_progress_part_bytes = Some(64);
        write_cache.staged_fold.staged_range_count = Some(3);
        write_cache.staged_fold.staged_range_budget = 100;
        write_cache.staged_fold.staged_shard_authority_age_secs = Some(4);
        write_cache.staged_fold.staged_fold_dirty_shards = 2;
        write_cache.staged_fold.staged_fold_shard_duration_secs = ShardDurationHistogram {
            classes: vec![
                ShardDurationClass {
                    le_secs: Some(1),
                    count: 2,
                },
                ShardDurationClass {
                    le_secs: None,
                    count: 1,
                },
            ],
            count: 3,
            sum_ms: 900,
        };
        write_cache.ledger_repair.ledger_repair_rotation_reachable = Some(true);
        // A non-empty reason and one sampled level per kind, so the scanner reason and per-level
        // branches record.
        write_cache.scanner.scanner_last_adjust_reason_fold = "hold".to_string();
        write_cache.scanner.scanner_last_adjust_reason_repair = "probe".to_string();
        write_cache
            .scanner
            .scanner_rate_by_level_fold
            .insert(1, 12.5);
        write_cache
            .scanner
            .scanner_rate_by_level_repair
            .insert(2, 7.0);
        write_cache
            .scanner
            .scanner_samples_by_level_fold
            .insert(1, 3);
        write_cache
            .scanner
            .scanner_samples_by_level_repair
            .insert(2, 4);
        write_cache
            .scanner
            .scanner_aggregate_estimate_by_level_fold
            .insert(1, 12.5);
        write_cache
            .scanner
            .scanner_aggregate_estimate_by_level_repair
            .insert(2, 14.0);
        write_cache
            .ledger_repair
            .ledger_repair_projected_rotation_secs = Some(84_300);
        write_cache.count_pass_last_result = Some(WriteCacheCountPassResult::new(
            "wire-test-host:1".into(),
            1_000_000,
            950_000,
            800_000,
            960_000,
            962_000,
            160_000,
            120_000,
            7,
            43,
            512,
            300,
            290,
            3,
            500_000,
            11,
        ));

        SystemMetrics {
            timestamp: SystemTime::now(),
            uptime_seconds: 3600,
            cache: Some(CacheMetrics {
                total_cache_size: 4096,
                max_cache_size_limit: 8192,
                consumed_bytes: 4096,
                read_fills_declined_max_cache_size_total: 0,
                hostname_collisions_total: 0,
                capacity_guard: crate::capacity_guard::CapacityGuardGauges {
                    declines_total: 2,
                    last_free_bytes: Some(1_048_576),
                    floor_bytes: 1_073_741_824,
                },
                read_cache_size: 3072,
                read_cache_size_inverted_secs: 1800,
                write_cache_size: 1024,
                ram_cache_size: 512,
                in_progress_part_bytes: Some(64),
                in_progress_part_bytes_age_secs: Some(3),
                in_progress_part_bytes_drift_bytes: Some(-128),
                eviction_last_result: Some(EvictionPassResult::new(
                    "wire-test-host:1".into(),
                    /* bytes_requested */ 2048,
                    /* bytes_freed */ 1024,
                    /* ranges_evicted */ 5,
                    /* in_progress_part_bytes */ Some(64),
                    /* active_duration */ Some(Duration::from_millis(7777)),
                )),
                st_size_fold: crate::st_size_fold::StSizeFoldGauges {
                    st_size_fold_oldest_shard_scan_age_secs: Some(30),
                    ..Default::default()
                },
                tail_partition: crate::cache_size_tracker::TailPartitionGauges {
                    tail_entries_last_rebuild: Some(10),
                    tail_effective_min_bytes: Some(1_048_576),
                    tail_delta_records: Some(3),
                    ..Default::default()
                },
                tail_selection: crate::cache::TailSelectionGauges {
                    tail_coverage_ratio: Some(0.5),
                    eviction_tail_verdict_total: std::collections::BTreeMap::from([(
                        "covered".to_string(),
                        2,
                    )]),
                    ..Default::default()
                },
                eviction_executor: crate::cache::EvictionExecutorGauges {
                    eviction_passes_total: 9,
                    eviction_oracle_last_walk_ms: Some(1_250),
                    ..Default::default()
                },
                eviction_rates: crate::metrics::EvictionRateGauges {
                    eviction_shed_bytes_total: 12_326_478_807,
                    eviction_shed_ranges_total: 11_480,
                    eviction_active_micros_total: 78_800_000,
                    eviction_shed_bytes_per_sec: Some(156_427_396.0),
                    eviction_shed_ranges_per_sec: Some(145.68),
                    admission_bytes_total: 25_165_824_000,
                    admission_ranges_total: 24_000,
                    admission_bytes_per_sec: Some(104_857_600.0),
                    admission_ranges_per_sec: Some(100.0),
                    eviction_keeping_pace: Some(true),
                    eviction_backlog_bytes: Some(-450),
                    eviction_backlog_projected_bytes: Some(150),
                },
                cache_hit_rate_percent: 50.0,
                ram_cache_hit_rate_percent: 25.0,
                total_requests: 10,
                cache_hits: 5,
                cache_misses: 5,
                evictions: 1,
                corruption_metadata_total: 0,
                corruption_missing_range_total: 0,
                disk_full_events_total: 0,
                metadata_lock_retry_exhausted_total: 0,
                tee_backpressure_events_total_get: 0,
                tee_backpressure_wait_ms_total_get: 0,
                tee_backpressure_events_total_put: 0,
                tee_backpressure_wait_ms_total_put: 0,
                tee_abandoned_total_put: 0,
                metadata_heal_overwrite_total: 0,
                upstream_idle_abort_total_mid_stream: 0,
                upstream_idle_abort_total_pre_stream: 0,
                upstream_idle_retry_total: 0,

                dirent_type_resolution_failures_total: 0,
                metadata_read_stale_handles_total: 0,
                metadata_read_stale_handle_misses_total: 0,
                metadata_parse_duration_ms: 0.0,
                metadata_file_size_bytes: 0,
                range_file_count: 0,
                range_load_duration_ms: 0.0,
                incomplete_range_fallbacks: 0,
                cache_bypasses_by_reason: HashMap::new(),
                origin_marker_mismatch_bypasses_total: 0,
                origin_marker_unparseable_bypasses_total: 0,
                origin_marker_unresolved_bypasses_total: 0,
                uncacheable_aws_host_bypasses_total: 0,
                write_stamp_retried_total: 0,
                write_stamp_retry_landed_total: 0,
                write_stamp_retry_overflow_keys: 0,
                write_stamp_retry_active_keys: 0,
                write_stamp_retry_abandoned_total: 0,
                write_stamp_log_pending_keys: 0,
                write_stamp_log_replayed_total: 0,
                write_stamp_log_torn_records_total: 0,
                write_stamp_log_write_failures_total: 0,
                write_stamp_log_memory_only: 0,
                write_stamp_pending_declined_total: 0,
                volume_stall_breaker_open: 0,
                volume_stall_breaker_trips_total: 0,
                cache_etag_validations_total: 0,
                cache_etag_mismatches_total: 0,
                cache_range_invalidations_total: 0,
                cache_orphaned_ranges_cleaned_total: 0,
                cache_cleanup_failures_total: 0,
                cache_part_hits: 0,
                cache_part_misses: 0,
                cache_part_stores: 0,
                cache_part_evictions: 0,
                cache_part_errors: 0,
                incomplete_uploads_evicted: 0,
                ram_cache_hits: 0,
                ram_cache_misses: 0,
                ram_cache_evictions: 0,
                ram_cache_max_size: 0,
                ram_cache_entries: 0,
                ram_cache_eligibility_skips_total: 0,
                metadata_cache_hits: 0,
                metadata_cache_misses: 0,
                metadata_cache_entries: 0,
                metadata_cache_max_entries: 0,
                metadata_cache_evictions: 0,
                metadata_cache_stale_refreshes: 0,
                metadata_cache_head_hits: 0,
                metadata_cache_head_misses: 0,
                metadata_cache_head_disk_hits: 0,
                metadata_cache_disk_hits: 0,
                bytes_served_from_cache: 0,
                read_cache_disabled_invalidations_total: 0,
                ttl_revalidations_total: 0,
            }),
            compression: Some(CompressionMetrics {
                total_objects_compressed: 1,
                total_objects_uncompressed: 1,
                total_bytes_before: 100,
                total_bytes_after: 50,
                compression_failures: 0,
                decompression_failures: 0,
                average_compression_ratio: 0.5,
            }),
            connection_pool: None,
            eviction_coordination: Some(EvictionCoordinationMetrics {
                lock_acquisitions_successful: 4,
                lock_acquisitions_failed: 1,
                stale_locks_recovered: 0,
                total_lock_hold_time_ms: 250,
                evictions_coordinated: 2,
                evictions_skipped_lock_held: 1,
                staging_evictions_skipped_lock_held: 6,
            }),
            signed_put: None,
            cache_size: Some(CacheSizeMetrics {
                current_size: 4096,
                write_cache_size: 1024,
                last_checkpoint: SystemTime::now(),
                last_validation: Some(SystemTime::now()),
                last_validation_drift: Some(-12),
                // `cached_objects` has no reference, so it is the unevaluable figure; the other
                // two carry values. Both recording branches run from one sample.
                accounting_drift: Some(AccountingDriftSample::new(
                    "wire-test-host:1".into(),
                    crate::cache_size_tracker::ValidationSampleKind::FullScan,
                    /* persisted_state_readable */ true,
                    /* persisted */ (4000, 1000, 40),
                    /* reference */ (4096, Some(1024), None),
                )),
                checkpoint_count: 0,
                delta_log_size: 0,
            }),
            atomic_metadata: Some(Default::default()),
            consolidation: Some(ConsolidationMetrics {
                last_consolidation_timestamp: 1_790_000_000,
                ..Default::default()
            }),
            cache_hit_buffer: Some(CacheHitBufferMetrics {
                pending_updates: 45,
                updates_recorded_total: 1_204,
                entries_flushed_total: 1_159,
                flush_errors_total: 3,
                ram_records_dropped_total: 7,
            }),
            fill_visibility: Some(crate::metrics::FillVisibilityMetrics {
                entries: 2,
                bytes: 2_048,
                recorded_total: 9,
                hits_total: 5,
                dropped_disk_meta_total: 1,
                dropped_folded_total: 3,
                dropped_invalidated_total: 1,
                dropped_cap_total: 1,
                dropped_age_total: 1,
                dropped_range_file_changed_total: 0,
            }),
            // Both `Option` rotation gauges set, so the record-coverage test sees them exported.
            orphan_reclaim: crate::metrics::OrphanReclaimMetrics {
                enabled: true,
                found: 6,
                reclaimed: 2,
                bytes_reclaimed: 4_096,
                skipped_unconfirmed: 1,
                kept_listed: 1,
                journal_evidence_total: crate::metrics::OrphanJournalEvidenceTotals {
                    confirmed_absent: 2,
                    journal_present: 2,
                    peer_unavailable: 1,
                    indeterminate: 0,
                },
                passes: 3,
                units_visited: 12,
                passes_gated: 1,
                leases_unreadable: 2,
                leases_unreadable_expired: 1,
                stalest_checkpoint_age_secs: Some(240),
                units_never_scanned: Some(0),
            },
            eviction_worklist: Default::default(),
            coalescing: Some(CoalescingMetrics {
                waits_total: 1,
                cache_hits_after_wait_total: 1,
                timeouts_total: 0,
                s3_fetches_saved_total: 1,
                average_wait_duration_ms: 2.0,
                fetcher_completions_success: 1,
                fetcher_completions_error: 0,
                waiter_conditional_304: 0,
                waiter_conditional_200: 0,
                waiter_conditional_4xx: 0,
                waiter_conditional_error: 0,
            }),
            cache_rules: Some(CacheRulesMetrics {
                reloads_total: 1,
                reload_failures_total: 0,
                on_fallback: false,
                rules_loaded: 2,
                last_load_unix: 1_790_000_000,
            }),
            // `widened_requests > 0`, so the gated `amplification_ratio` is recorded too.
            page_cache: PageCacheMetrics {
                widened_requests: 3,
                bytes_prefetched: 3_000_000,
                amplification_ratio: 250.0,
                page_hits: 2,
                skipped_signed_range: 1,
                fallbacks: 1,
                ram_page_promotions: 2,
                ram_page_promotion_skipped: 1,
            },
            request_metrics: RequestMetrics {
                total_requests: 10,
                successful_requests: 10,
                failed_requests: 0,
                client_error_requests: 0,
                server_error_requests: 0,
                rejected_requests: 0,
                cache_hit_requests: 5,
                cache_miss_requests: 5,
                average_response_time_ms: 1,
                requests_per_second: 1.0,
                active_requests: 0,
                max_concurrent_requests: 10,
                permits_total: 10,
                permits_held: 0,
                permits_available: 10,
                permits_held_peak: 0,
                access_log_records_dropped: 0,
            },
            bucket_traffic: HashMap::new(),
            download_bandwidth: crate::bandwidth_limiter::BandwidthLimiterSnapshot {
                enabled: true,
                instance_ceiling_bps: 1_000_000,
                failopen_total: 0,
                class_bytes: HashMap::from([("interactive".to_string(), 5)]),
                residual_bytes: 7,
            },
            hedged_requests: Default::default(),
            inflight_memory: Default::default(),
            write_cache,
            write_ledger: crate::write_ledger::WriteLedgerMetrics {
                append_batches_total: 4,
                append_batched_entries_total: 37,
                pending_batches_total: 3,
                pending_batch_failures_total: 1,
            },
            metrics_collection: crate::metrics::MetricsCollectionGauges {
                collections_total: 7,
                duration_ms_last: 41,
                duration_ms_max: 2_112,
                slow_collections_total: 1,
                slow_threshold_ms: 30_000,
            },
            volume_snapshot: crate::metrics::VolumeSnapshotGauges {
                collected_at_unix_secs: Some(1_790_000_000),
                age_secs: Some(12),
                interval_secs: 60,
            },
        }
    }

    /// Every gauge `initialize` CONSTRUCTS must be RECORDED by `export_metrics`, observed on the
    /// wire.
    ///
    /// The `every_*_field_has_an_otlp_instrument` guards prove a `/metrics` field has a
    /// constructed instrument naming it. None of them sees the `.record(…)` call, so deleting one
    /// passes all of them while the series never reaches a collector: an unrecorded gauge has no
    /// data point, and the SDK exports no `Metric` for it. This drives one real export of a
    /// `SystemMetrics` in which every recording branch runs, through a local listener, and
    /// requires every constructed name to appear as a `Metric.name` in the protobuf body. The
    /// staged tier's byte figure, which this file may not spell, is set through
    /// `set_staged_bytes_for_test` in `staged_fold.rs`, so `write_cache.staged_bytes` is covered too.
    ///
    /// The flush is done here rather than through `force_one_export`, which records
    /// `process.uptime_seconds` itself and would mask a deleted uptime record.
    ///
    /// Red side: delete `i.cache_hit_buffer_flush_errors_total.record(…)` in `export_metrics`.
    /// `every_cache_hit_buffer_field_has_an_otlp_instrument` still passes; this test fails naming
    /// `cache_hit_buffer.flush_errors_total`. Deleting `i.staged_bytes.record(v, a)` fails it naming
    /// `write_cache.staged_bytes`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_constructed_gauge_is_recorded_on_the_wire() {
        let names = constructed_gauge_names();
        // A parser that found nothing would pass the loop below vacuously.
        assert!(
            names.len() > 100,
            "expected the full instrument set, parsed only {} names: {:?}",
            names.len(),
            names
        );

        eprintln!(
            "constructed OTLP gauges checked on the wire: {}",
            names.len()
        );

        let (addr, rx) = one_shot_otlp_listener().await;
        let cfg = OtlpConfig {
            enabled: true,
            endpoint: format!("http://{}/v1/metrics", addr),
            export_interval: Duration::from_secs(3600), // only the flush below exports
            timeout: Duration::from_secs(10),
            ..OtlpConfig::default()
        };
        let mut exporter = OtlpExporter::new(cfg);
        exporter.initialize().await.expect("init");
        exporter
            .export_metrics(&every_branch_recording_system_metrics())
            .await
            .expect("export_metrics");

        let provider = exporter.provider.clone().unwrap();
        tokio::task::spawn_blocking(move || provider.force_flush())
            .await
            .unwrap()
            .expect("force_flush: the export must succeed");
        let request = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("no export reached the listener within 10s")
            .unwrap();
        let body_start = request
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("request has a header terminator")
            + 4;
        let body = &request[body_start..];

        // Every constructed gauge, `write_cache.staged_bytes` included: until the change it was excluded,
        // because the fixture could not set its input from this file.
        let missing: Vec<&String> = names
            .iter()
            .filter(|n| !body_names_metric(body, n))
            .collect();
        assert!(
            missing.is_empty(),
            "{} of {} constructed OTLP gauge(s) never reached the wire, so an OTLP-only \
             deployment receives no series for them even though every construction guard \
             passes: {:?}. Either `export_metrics` has no `.record(…)` for the instrument, or \
             this test's fixture does not reach its branch — check the fixture first if the \
             instrument sits behind an `Option` or a map.",
            missing.len(),
            names.len(),
            missing
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
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
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
                    bytes_saved: 0,
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
                    bytes_saved: 0,
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
            .scope_metrics()
            .find(|s| s.scope().name() == "s3-proxy-test")
            .expect("scope_metrics should contain s3-proxy-test");

        // Assert: s3proxy.bytes_downloaded has correct data points.
        let bytes_dl_metric = scope
            .metrics()
            .find(|m| m.name() == "s3proxy.bytes_downloaded")
            .expect("should find s3proxy.bytes_downloaded metric");

        let sum_data = match bytes_dl_metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum,
            other => panic!("bytes_downloaded should be Sum<u64>, got {other:?}"),
        };

        assert!(
            sum_data.is_monotonic(),
            "Observable counters must be monotonic (cumulative)"
        );

        // Bucket-only data point (test-bucket, no prefix attr).
        let bucket_only_dp = sum_data
            .data_points()
            .find(|dp| {
                dp.attributes()
                    .any(|kv| kv == &KeyValue::new("bucket", "test-bucket"))
                    && !dp.attributes().any(|kv| kv.key.as_str() == "prefix")
            })
            .expect("should have a data point for test-bucket without prefix");
        assert_eq!(
            bucket_only_dp.value(),
            5000,
            "bucket-only bytes_downloaded should be 5000"
        );

        // Prefix-attributed data point (test-bucket + prefix "logs/").
        let prefix_dp = sum_data
            .data_points()
            .find(|dp| {
                dp.attributes()
                    .any(|kv| kv == &KeyValue::new("bucket", "test-bucket"))
                    && dp
                        .attributes()
                        .any(|kv| kv == &KeyValue::new("prefix", "logs/"))
            })
            .expect("should have a data point for test-bucket with prefix=logs/");
        assert_eq!(
            prefix_dp.value(),
            2000,
            "prefix-attributed bytes_downloaded should be 2000"
        );

        // 6. Verify put_requests metric to confirm the request-count counters work.
        let put_req_metric = scope
            .metrics()
            .find(|m| m.name() == "s3proxy.put_requests")
            .expect("should find s3proxy.put_requests metric");

        let put_req_sum = match put_req_metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum,
            other => panic!("put_requests should be Sum<u64>, got {other:?}"),
        };

        let put_req_bucket_dp = put_req_sum
            .data_points()
            .find(|dp| {
                dp.attributes()
                    .any(|kv| kv == &KeyValue::new("bucket", "test-bucket"))
                    && !dp.attributes().any(|kv| kv.key.as_str() == "prefix")
            })
            .expect("should have put_requests data point for test-bucket without prefix");
        assert_eq!(
            put_req_bucket_dp.value(),
            3,
            "bucket-only put_requests should be 3"
        );

        let put_req_prefix_dp = put_req_sum
            .data_points()
            .find(|dp| {
                dp.attributes()
                    .any(|kv| kv == &KeyValue::new("bucket", "test-bucket"))
                    && dp
                        .attributes()
                        .any(|kv| kv == &KeyValue::new("prefix", "logs/"))
            })
            .expect("should have put_requests data point for test-bucket with prefix=logs/");
        assert_eq!(
            put_req_prefix_dp.value(),
            2,
            "prefix-attributed put_requests should be 2"
        );

        // Cleanup
        let _ = provider.shutdown();
    }
}
