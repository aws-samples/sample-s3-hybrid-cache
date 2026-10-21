//! The `/metrics` WIRE SHAPE, asserted against real JSON rather than inferred from the types.
//!
//! # Why this file exists
//!
//! `/metrics` is one `serde_json::to_string_pretty(&SystemMetrics)` with no `#[serde(rename)]`
//! anywhere, and five of `CacheMetrics`'s fields are `#[serde(flatten)]`ed sub-structs. So the
//! twenty-three eviction-mechanism field names an operator actually sees on `cache.*` are a
//! *consequence* of `serde(flatten)` semantics, and until this file existed **nothing asserted
//! them**. A `METRICS-SURFACE-SCOPE` audit derived all twenty-three from the types plus those
//! semantics and recorded the gap as the one thing it could not close: a scrape needs a running
//! proxy, and a documentation pass was about to write those names into `docs/`.
//!
//! Serialising a populated `SystemMetrics` here closes it without a proxy. The assertions are
//! **literal strings on purpose**. Deriving the expected names from the structs would restate the
//! implementation and pass against any version of it, including one that dropped a `flatten` and
//! nested every name a level deeper — which is the specific regression that breaks a dashboard
//! silently, because a missing JSON key reads as absent rather than as an error.
//!
//! # What each block is for
//!
//! * `every_flattened_cache_field_is_a_top_level_cache_key` — the twenty-three spellings, plus the
//!   pass witness added beside them, plus R12.3's eleven rate and admission names. Thirty-five.
//!   This is the one that reds if a `flatten` is removed or a field renamed.
//! * `the_rate_gauges_carry_their_unknowns_and_their_sign_on_the_wire` — R12.3's three `Option`
//!   fields survive serialisation as themselves: a JSON `null` for unknown rather than a `0` that
//!   would read as "shedding nothing", a JSON `false` distinguishable from that `null`, and a
//!   NEGATIVE backlog, which is the healthy reading and the reason that field is signed.
//! * `a_populated_pass_result_reaches_the_wire_with_its_duration` — both ends for
//!   `cache.eviction_last_result.duration_ms`: computed by the producer, present in the JSON. A
//!   field can be populated and unserialised (this payload had two such figures) or serialised and
//!   unwritten, so asserting one end proves nothing.
//! * `a_pre_duration_document_loads_as_unmeasured_rather_than_zero` — the upgrade path. An
//!   `eviction_last_result.json` written by a build predating `duration_ms` must still load, with
//!   the duration reading `null`, or `cache.eviction_last_result` goes `null` wholesale on upgrade.
//! * `the_count_pass_result_reaches_the_wire` — the figure that was computed, persisted, and never
//!   published.
//! * `the_accounting_drift_gauges_name_their_cause` — R12.8: an unevaluable gauge says which arm it
//!   was, by name, instead of publishing a plausible number.
use s3_proxy::cache_size_tracker::{
    AccountingDriftSample, CacheSizeMetrics, ValidationSampleKind,
    DRIFT_REASON_PERSISTED_STATE_UNREADABLE, DRIFT_REASON_REFERENCE_IS_EXTRAPOLATION,
};
use s3_proxy::eviction_result::{EvictionPassResult, WriteCacheCountPassResult};
use s3_proxy::metrics::{
    CacheHitBufferMetrics, CacheMetrics, EvictionCoordinationMetrics, EvictionWorklistClaimTotals,
    EvictionWorklistFallbackTotals, EvictionWorklistMetrics, EvictionWorklistRefusedTotals,
    OrphanJournalEvidenceTotals, OrphanReclaimMetrics, PageCacheMetrics, RequestMetrics,
    SystemMetrics, WriteCacheMetrics,
};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Every `cache.*` field name this file pins, exactly as an operator reads it off the wire.
///
/// **Thirty-eight.** Twenty-three were already public when this file was written,
/// `eviction_passes_total` was the twenty-fourth, `METRICS-SURFACE-RATES` added the eleven
/// `EvictionRateGauges` names below it, and the three `eviction_oracle_*` names came last. Grouped by the producer that flattens them so a failure
/// points at one struct.
///
/// Note the two `admission_*_total` names carry no `eviction_` prefix and that is deliberate: they
/// count arrivals, not evictions, and the four `admission_*` fields are the figure R12.3's rate is
/// raced against rather than part of it.
const FLATTENED_CACHE_FIELDS: &[&str] = &[
    // StSizeFoldGauges
    "st_size_fold_last_duration_secs",
    "st_size_folds_declined_overlap_total",
    "st_size_fold_oldest_shard_scan_age_secs",
    // TailPartitionGauges
    "tail_floor_bytes",
    "tail_max_entries",
    "tail_rebuilds_total",
    "tail_entries_last_rebuild",
    "tail_effective_min_bytes",
    "tail_delta_records",
    // TailSelectionGauges
    "tail_coverage_ratio",
    "tail_coverage_short_total",
    "eviction_fallback_tail_empty_total",
    "eviction_fallback_cap_exhausted_total",
    "eviction_fallback_unvisited_total",
    "eviction_oversized_single_candidate_total",
    "eviction_tail_verdict_total",
    "tail_delta_cap_rewrites_total",
    // EvictionExecutorGauges
    "eviction_passes_total",
    "eviction_pass_path_whole_object_total",
    "eviction_pass_path_partial_total",
    "eviction_whole_object_downgraded_total",
    "eviction_unlink_failures_total",
    "eviction_meta_rewrites_total",
    // The activity predicate's traversal count. Published because a regression here changes no
    // other figure: the pass evicts the same objects and frees the same bytes, only slower. Read as
    // a ratio against `eviction_passes_total`.
    "eviction_active_key_journal_reads_total",
    // The activity oracle's rebuild and staleness figures, and its last journal-tree walk.
    "eviction_oracle_refreshes_total",
    "eviction_oracle_stale_verdicts_total",
    "eviction_oracle_last_walk_ms",
    // EvictionRateGauges — R12.3's rates, the admission rates they race, the verdict, the backlog.
    "eviction_shed_bytes_total",
    "eviction_shed_ranges_total",
    "eviction_active_micros_total",
    "eviction_shed_bytes_per_sec",
    "eviction_shed_ranges_per_sec",
    "admission_bytes_total",
    "admission_ranges_total",
    "admission_bytes_per_sec",
    "admission_ranges_per_sec",
    "eviction_keeping_pace",
    "eviction_backlog_bytes",
    // Direct CacheMetrics fields, listed here because they are part of the same mechanism's
    // surface and a reader looking for eviction figures must find them in the same place.
    "eviction_last_result",
    "in_progress_part_bytes",
];

fn a_count_pass_result() -> WriteCacheCountPassResult {
    WriteCacheCountPassResult::new(
        "wire-shape-host:1".into(),
        /* count_budget */ 1_000_000,
        /* count_trigger */ 950_000,
        /* count_target */ 800_000,
        /* count_at_start */ 960_000,
        /* observed_count_at_start */ 962_000,
        /* ranges_requested */ 160_000,
        /* ranges_freed */ 120_000,
        /* unreadable_candidate_ranges */ 7,
        /* objects_evicted */ 43,
        /* candidates_considered */ 512,
        /* reclaimable_candidates */ 300,
        /* expired_candidates */ 290,
        /* passes_total */ 3,
        /* ranges_freed_total */ 500_000,
        /* admission_declines_total */ 11,
    )
}

/// A `SystemMetrics` with every section this file asserts on populated.
///
/// Deliberately a literal rather than a `Default`: `CacheMetrics` has no `Default`, and a field
/// added to it without a serde default therefore breaks this build instead of silently escaping
/// the wire-shape assertion.
fn populated_metrics() -> SystemMetrics {
    SystemMetrics {
        timestamp: SystemTime::now(),
        uptime_seconds: 3600,
        cache: Some(CacheMetrics {
            total_cache_size: 4096,
            max_cache_size_limit: 8192,
            consumed_bytes: 4096,
            capacity_guard: s3_proxy::capacity_guard::CapacityGuardGauges::default(),
            read_fills_declined_max_cache_size_total: 7,
            hostname_collisions_total: 8,
            read_cache_size: 3072,
            read_cache_size_inverted_secs: 1800,
            write_cache_size: 1024,
            ram_cache_size: 512,
            in_progress_part_bytes: Some(64),
            in_progress_part_bytes_age_secs: Some(3),
            in_progress_part_bytes_drift_bytes: Some(-128),
            eviction_last_result: Some(EvictionPassResult::new(
                "wire-shape-host:1".into(),
                /* bytes_requested */ 2048,
                /* bytes_freed */ 1024,
                /* ranges_evicted */ 5,
                /* in_progress_part_bytes */ Some(64),
                /* active_duration */ Some(Duration::from_millis(7777)),
            )),
            // Defaults: this file asserts the KEYS these three contribute, not their values.
            st_size_fold: Default::default(),
            tail_partition: Default::default(),
            tail_selection: Default::default(),
            // Seeded, because the pass witness is one of the fields under test and a zero here
            // would not distinguish "serialised" from "serialised as the type's default".
            eviction_executor: s3_proxy::cache::EvictionExecutorGauges {
                eviction_passes_total: 9,
                ..Default::default()
            },
            // Seeded rather than defaulted for the same reason as the line above, and with the
            // three `Option`s deliberately set: a `None` would serialise as `null`, which IS a
            // present key, so a defaulted bundle could not tell "the rate reached the wire" from
            // "the rate is absent and something else nulled the key".
            eviction_rates: s3_proxy::metrics::EvictionRateGauges {
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
        compression: None,
        connection_pool: None,
        eviction_coordination: Some(EvictionCoordinationMetrics {
            lock_acquisitions_successful: 4,
            lock_acquisitions_failed: 1,
            stale_locks_recovered: 0,
            total_lock_hold_time_ms: 250,
            evictions_coordinated: 2,
            evictions_skipped_lock_held: 1,
            // The field that was incremented and documented but never serialised.
            staging_evictions_skipped_lock_held: 6,
        }),
        signed_put: None,
        cache_size: Some(CacheSizeMetrics {
            current_size: 4096,
            write_cache_size: 1024,
            last_checkpoint: SystemTime::now(),
            last_validation: Some(SystemTime::now()),
            last_validation_drift: Some(-12),
            accounting_drift: Some(AccountingDriftSample::new(
                "wire-shape-host:1".into(),
                ValidationSampleKind::FullScan,
                /* persisted_state_readable */ true,
                /* persisted */ (4000, 1000, 40),
                /* reference */ (4096, Some(1024), Some(41)),
            )),
            checkpoint_count: 0,
            delta_log_size: 0,
        }),
        atomic_metadata: None,
        consolidation: None,
        // Populated, and every field set to a DIFFERENT non-zero value: the assertion below
        // reads each key's value, so a defaulted or uniformly-seeded bundle could not tell
        // "the field reached the wire" from "a sibling's value did".
        cache_hit_buffer: Some(CacheHitBufferMetrics {
            pending_updates: 45,
            updates_recorded_total: 1_204,
            entries_flushed_total: 1_159,
            flush_errors_total: 3,
            ram_records_dropped_total: 7,
        }),
        fill_visibility: None,
        // Every figure a DIFFERENT value, so each key's assertion reads its own field.
        orphan_reclaim: OrphanReclaimMetrics {
            enabled: true,
            found: 17,
            reclaimed: 5,
            bytes_reclaimed: 81_920,
            skipped_unconfirmed: 4,
            kept_listed: 2,
            journal_evidence_total: OrphanJournalEvidenceTotals {
                confirmed_absent: 6,
                journal_present: 3,
                peer_unavailable: 8,
                indeterminate: 9,
            },
            passes: 11,
            units_visited: 44,
            passes_gated: 13,
            leases_unreadable: 14,
            leases_unreadable_expired: 15,
            stalest_checkpoint_age_secs: Some(612),
            units_never_scanned: Some(7),
        },
        // Every figure a DIFFERENT value, so each key's assertion reads its own field.
        eviction_worklist: EvictionWorklistMetrics {
            generation: 21,
            chunks_authored_total: 22,
            chunks_claimed_total: EvictionWorklistClaimTotals {
                claimed: 23,
                busy: 24,
            },
            chunks_done_total: 25,
            chunks_unusable_total: 26,
            items_executed_total: 27,
            items_refused_total: EvictionWorklistRefusedTotals {
                identity_changed: 28,
                stale_generation: 29,
            },
            lease_takeovers_total: 30,
            author_fallback_total: EvictionWorklistFallbackTotals {
                not_capable: 31,
                unknown: 32,
            },
            in_flight_bytes: 33,
        },
        coalescing: None,
        cache_rules: None,
        page_cache: PageCacheMetrics {
            widened_requests: 0,
            bytes_prefetched: 0,
            amplification_ratio: 1.0,
            page_hits: 0,
            skipped_signed_range: 0,
            fallbacks: 0,
            ram_page_promotions: 0,
            ram_page_promotion_skipped: 0,
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
        download_bandwidth: s3_proxy::bandwidth_limiter::BandwidthLimiterSnapshot {
            enabled: false,
            instance_ceiling_bps: 0,
            failopen_total: 0,
            class_bytes: HashMap::new(),
            residual_bytes: 0,
        },
        hedged_requests: Default::default(),
        inflight_memory: Default::default(),
        write_cache: WriteCacheMetrics {
            count_pass_last_result: Some(a_count_pass_result()),
            ..Default::default()
        },
        // Non-default for the same reason as `metrics_collection` below. The mean batch size is
        // the ratio of the first two, so both have to reach the wire for either to be readable.
        write_ledger: s3_proxy::write_ledger::WriteLedgerMetrics {
            append_batches_total: 4,
            append_batched_entries_total: 37,
            pending_batches_total: 3,
            pending_batch_failures_total: 1,
        },
        // Populated with non-default values so the wire-shape assertions below distinguish "the
        // section serialises" from "the section is absent" — a defaulted struct of zeros would
        // serialise identically to one that was never wired in.
        metrics_collection: s3_proxy::metrics::MetricsCollectionGauges {
            collections_total: 7,
            duration_ms_last: 41,
            duration_ms_max: 2_112,
            slow_collections_total: 1,
            slow_threshold_ms: 30_000,
        },
        // Non-default for the same reason as `metrics_collection` above.
        volume_snapshot: s3_proxy::metrics::VolumeSnapshotGauges {
            collected_at_unix_secs: Some(1_790_000_000),
            age_secs: Some(12),
            interval_secs: 60,
        },
    }
}

fn wire(metrics: &SystemMetrics) -> serde_json::Value {
    let json = serde_json::to_string_pretty(metrics).expect("SystemMetrics must serialise");
    serde_json::from_str(&json).expect("the serialised payload must parse back")
}

/// `cache.read_cache_size_inverted_secs` serialises to the wire with the fixture's non-zero value.
/// This checks serialisation only: `populated_metrics()` is a literal, so it never runs the copy
/// from `CacheSizes` into `CacheMetrics`. That copy is driven by
/// `inverted_age_metric_tests::the_published_age_is_the_guards_figure` in `src/metrics.rs`.
#[test]
fn read_cache_size_inverted_secs_reaches_the_wire_with_its_value() {
    let value = wire(&populated_metrics());
    let got = value
        .get("cache")
        .and_then(|c| c.get("read_cache_size_inverted_secs"))
        .and_then(|v| v.as_u64());
    assert_eq!(got, Some(1800), "`cache.read_cache_size_inverted_secs`");
}

/// The thirty-eight eviction-mechanism and admission names are TOP-LEVEL keys of `cache`, not nested
/// one level deeper under the sub-struct they come from.
///
/// A failure implicates one of the four `#[serde(flatten)]` attributes on `CacheMetrics`, or a
/// renamed field on the struct the missing name belongs to. The assertion names the missing key,
/// because "the payload changed shape" is not actionable and "`cache.tail_coverage_ratio` is gone"
/// is.
#[test]
fn every_flattened_cache_field_is_a_top_level_cache_key() {
    let value = wire(&populated_metrics());
    let cache = value
        .get("cache")
        .and_then(|c| c.as_object())
        .expect("`cache` must be an object on the wire");

    let mut missing: Vec<&str> = Vec::new();
    for field in FLATTENED_CACHE_FIELDS {
        if !cache.contains_key(*field) {
            missing.push(field);
        }
    }
    assert!(
        missing.is_empty(),
        "these names are not top-level keys of `cache` on the wire: {missing:?}. \
         Either a `#[serde(flatten)]` was removed from CacheMetrics — which nests every name one \
         level deeper and reads to a consumer as the field having disappeared — or the field was \
         renamed. Keys actually present: {:?}",
        {
            let mut keys: Vec<&String> = cache.keys().collect();
            keys.sort();
            keys
        }
    );

    // And the sub-struct names must NOT appear: their presence is the signature of a lost
    // `flatten`, and it is worth naming separately because the check above would then report
    // twenty-two missing fields without saying why.
    for wrapper in [
        "st_size_fold",
        "tail_partition",
        "tail_selection",
        "eviction_executor",
        "eviction_rates",
    ] {
        assert!(
            !cache.contains_key(wrapper),
            "`cache.{wrapper}` is present as a nested object, so its fields are no longer \
             flattened into `cache` — every name it contributes has moved on the wire"
        );
    }
}

/// `cache.capacity_guard` is a nested object carrying its three figures, and a guard that has
/// taken no reading publishes `last_free_bytes: null` rather than a zero that reads as a full
/// volume.
#[test]
fn the_capacity_guard_section_reaches_the_wire_nested_with_a_null_unread_reading() {
    let mut metrics = populated_metrics();
    if let Some(c) = metrics.cache.as_mut() {
        c.capacity_guard = s3_proxy::capacity_guard::CapacityGuardGauges {
            declines_total: 6,
            last_free_bytes: None,
            floor_bytes: 31_183_744,
        };
    }
    let value = wire(&metrics);
    let cg = value
        .pointer("/cache/capacity_guard")
        .and_then(|v| v.as_object())
        .expect("`cache.capacity_guard` must be a nested object on the wire");
    assert_eq!(cg.get("declines_total"), Some(&serde_json::json!(6)));
    assert_eq!(cg.get("floor_bytes"), Some(&serde_json::json!(31_183_744)));
    assert_eq!(
        cg.get("last_free_bytes"),
        Some(&serde_json::Value::Null),
        "an unread guard must publish null, not 0"
    );
    assert_eq!(cg.len(), 3, "unexpected capacity_guard keys: {cg:?}");
}
/// The four `write_ledger.*` counters reach the wire at the paths the metrics reference and the
/// OTLP instruments name.
///
/// A top-level section rather than a nested one under `write_cache`, because the documented paths
/// are `write_ledger.append_batches_total` and so on, and a nested section would publish
/// `write_cache.write_ledger.append_batches_total` instead — the same field under a name nothing
/// reads. The ratio of the first two is the mean batch size, so both have to be present for
/// either to be usable.
#[test]
fn the_write_ledger_section_reaches_the_wire_at_its_documented_paths() {
    let value = wire(&populated_metrics());
    let wl = value
        .pointer("/write_ledger")
        .and_then(|v| v.as_object())
        .expect("`write_ledger` must be a top-level object on the wire");
    assert_eq!(
        wl.get("append_batches_total"),
        Some(&serde_json::json!(4)),
        "write_ledger.append_batches_total"
    );
    assert_eq!(
        wl.get("append_batched_entries_total"),
        Some(&serde_json::json!(37)),
        "write_ledger.append_batched_entries_total, the numerator of the mean batch size"
    );
    assert_eq!(
        wl.get("pending_batches_total"),
        Some(&serde_json::json!(3)),
        "write_ledger.pending_batches_total"
    );
    assert_eq!(
        wl.get("pending_batch_failures_total"),
        Some(&serde_json::json!(1)),
        "write_ledger.pending_batch_failures_total"
    );
    assert_eq!(wl.len(), 4, "unexpected write_ledger keys: {wl:?}");
}

/// `cache.eviction_last_result.duration_ms` is populated by the producer AND reaches the wire.
///
/// Both ends, because this payload has a history of one without the other: two figures were
/// computed and never serialised, and one was serialised and never written. Asserting presence on
/// a hand-built literal would only prove that `serde` works.
#[test]
fn a_populated_pass_result_reaches_the_wire_with_its_duration() {
    // Producer end: the constructor turns an active interval into the published field.
    let built = EvictionPassResult::new(
        "duration-host:1".into(),
        4096,
        4096,
        2,
        Some(0),
        Some(Duration::from_millis(1234)),
    );
    assert_eq!(
        built.duration_ms,
        Some(1234),
        "the constructor must carry the measured active interval into `duration_ms`"
    );

    // Wire end.
    let value = wire(&populated_metrics());
    let duration = value
        .pointer("/cache/eviction_last_result/duration_ms")
        .and_then(|v| v.as_u64());
    assert_eq!(
        duration,
        Some(7777),
        "`cache.eviction_last_result.duration_ms` must carry the pass's active duration. \
         It is R12.3's rate denominator, and a rate over wall-clock-since-start reads a \
         correctly idle cache as falling behind."
    );
}

/// A document written before `duration_ms` existed still loads, reporting the duration as
/// UNMEASURED rather than taking `cache.eviction_last_result` to `null` wholesale.
///
/// This is the upgrade path and it is easy to lose: a required field would make every
/// pre-upgrade document unparseable, so the whole result would read `null` until the next pass —
/// an operator-visible regression in a field that was working.
#[test]
fn a_pre_duration_document_loads_as_unmeasured_rather_than_zero() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = EvictionPassResult::path(tmp.path());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // Exactly the v1 layout, with no `duration_ms` key at all.
    std::fs::write(
        &path,
        r#"{
            "schema": 1,
            "instance_id": "legacy-host:1",
            "completed_at": 1700000000,
            "bytes_requested": 100,
            "bytes_freed": 40,
            "shortfall_bytes": 60,
            "unevictable_in_progress_bytes": 10,
            "unexplained_shortfall_bytes": 50,
            "reached_target": false,
            "ranges_evicted": 3
        }"#,
    )
    .unwrap();

    let loaded = EvictionPassResult::load(tmp.path())
        .expect("a pre-`duration_ms` document must still load, not be rejected as unparseable");
    assert_eq!(
        loaded.duration_ms, None,
        "an absent duration must read as UNMEASURED, so a rate consumer skips the sample \
         rather than dividing by a zero it never observed"
    );
    assert_eq!(
        loaded.bytes_freed, 40,
        "the rest of the document must survive"
    );
}

/// The count-driven pass's result reaches the wire, with the sub-fields R12.1 names.
///
/// It was built and installed to the shared volume on every count-driven pass and read back by
/// nothing, while its read-tier sibling was published — so this asserts the half that was missing,
/// not the half that worked.
#[test]
fn the_count_pass_result_reaches_the_wire() {
    let value = wire(&populated_metrics());
    let cp = value
        .pointer("/write_cache/count_pass_last_result")
        .and_then(|v| v.as_object())
        .expect("`write_cache.count_pass_last_result` must be an object once a pass has run");

    for field in [
        "passes_total",
        "candidates_considered",
        "reclaimable_candidates",
        "expired_candidates",
        "objects_evicted",
        "ranges_freed",
        "ranges_freed_total",
        "shortfall_ranges",
        "unevictable_unreadable_ranges",
        "unexplained_shortfall_ranges",
        "admission_declines_total",
        "shortfall_reason",
        "reached_target",
        "completed_at",
        "instance_id",
    ] {
        assert!(
            cp.contains_key(field),
            "`write_cache.count_pass_last_result.{field}` must reach the wire"
        );
    }
    assert_eq!(
        cp.get("candidates_considered").and_then(|v| v.as_u64()),
        Some(512),
        "the published value must be the producer's, not a default"
    );
    // The shortfall decomposition is an identity, and publishing the total alone cannot
    // distinguish "eviction did all it could" from "the algorithm is not finding what it should".
    let total = cp.get("shortfall_ranges").and_then(|v| v.as_u64()).unwrap();
    let named = cp
        .get("unevictable_unreadable_ranges")
        .and_then(|v| v.as_u64())
        .unwrap();
    let residual = cp
        .get("unexplained_shortfall_ranges")
        .and_then(|v| v.as_u64())
        .unwrap();
    assert_eq!(
        named + residual,
        total,
        "the named component and the residual must sum to the shortfall on the WIRE too, \
         not only in the producer"
    );
    assert!(
        named > 0 && residual > 0,
        "the fixture must exercise a real split ({named} / {residual}): a case where the \
         component is the whole shortfall is satisfied by code that never splits"
    );
}

/// R12.8: a gauge that could not be evaluated says which arm it was, by name, and publishes no
/// number. R12.7: the ratio is a ratio, and the sample carries its timestamp.
#[test]
fn the_accounting_drift_gauges_name_their_cause() {
    let value = wire(&populated_metrics());
    let ad = value
        .pointer("/cache_size/accounting_drift")
        .and_then(|v| v.as_object())
        .expect("`cache_size.accounting_drift` must be an object once a validation has run");

    assert!(
        ad.get("sampled_at").and_then(|v| v.as_u64()).is_some(),
        "R12.7's staleness input must be on the wire: without it a low drift and a stalled \
         validation are the same reading"
    );
    assert_eq!(
        ad.get("sample_kind").and_then(|v| v.as_str()),
        Some("full_scan"),
        "the sample must name the validation that produced it — three of the four kinds \
         cannot produce an evaluable drift at all"
    );

    // The evaluable case: a full scan against a readable counter.
    let total = ad.get("total_size").and_then(|v| v.as_object()).unwrap();
    assert_eq!(
        total.get("drift").and_then(|v| v.as_i64()),
        Some(96),
        "reference 4096 against persisted 4000 is a drift of +96"
    );
    assert!(
        total.get("drift_ratio").and_then(|v| v.as_f64()).unwrap() > 0.0,
        "R12.7 requires the magnitude as a ratio, because a byte threshold that is right at \
         one cache size is wrong at another"
    );
    assert!(
        total.get("unevaluable_reason").unwrap().is_null(),
        "an evaluable gauge must carry no cause"
    );

    // The rolling case: the reference figure is computed FROM the counter it would be compared
    // against, so there is no disagreement between two producers to report.
    let rolling = AccountingDriftSample::new(
        "drift-host:1".into(),
        ValidationSampleKind::RollingExtrapolation,
        true,
        (4000, 1000, 40),
        (4096, Some(1024), Some(41)),
    );
    for gauge in [
        &rolling.total_size,
        &rolling.write_cache_size,
        &rolling.cached_objects,
    ] {
        assert!(
            gauge.drift.is_none() && gauge.drift_ratio.is_none(),
            "a rolling cycle's proportional correction is a function of the persisted counter, \
             so publishing the difference as a drift would alarm on the correction itself"
        );
        assert_eq!(
            gauge.unevaluable_reason.as_deref(),
            Some(DRIFT_REASON_REFERENCE_IS_EXTRAPOLATION),
            "R12.8 requires the cause BY NAME, not an absent number"
        );
        assert!(
            gauge.reference.is_some(),
            "the two terms that could not be compared are still observations and must be \
             published, or an operator cannot diagnose the unevaluable gauge"
        );
    }

    // The unreadable-persisted-state case: the persisted term is a struct default, so the
    // difference would be the whole reference figure — a plausible number from a failed read,
    // which is precisely what R12.8 forbids.
    let unreadable = AccountingDriftSample::new(
        "drift-host:1".into(),
        ValidationSampleKind::FullScan,
        /* persisted_state_readable */ false,
        (0, 0, 0),
        (4096, Some(1024), Some(41)),
    );
    assert_eq!(
        unreadable.total_size.unevaluable_reason.as_deref(),
        Some(DRIFT_REASON_PERSISTED_STATE_UNREADABLE),
    );
    assert!(
        unreadable.total_size.drift.is_none(),
        "the whole reference figure must not be published as a drift because the persisted \
         side could not be read"
    );
    assert!(
        unreadable.total_size.persisted.is_none(),
        "an unreadable persisted term must be absent, not a zero that reads as an empty cache"
    );
}

/// `eviction_coordination.staging_evictions_skipped_lock_held` reaches the wire.
///
/// It was incremented by a production site and listed in the field reference as one of
/// `eviction_coordination`'s counters, while being a field of the internal stats struct only. A
/// consumer following the documentation got `null`.
#[test]
fn the_staging_lock_skip_counter_reaches_the_wire() {
    let value = wire(&populated_metrics());
    assert_eq!(
        value
            .pointer("/eviction_coordination/staging_evictions_skipped_lock_held")
            .and_then(|v| v.as_u64()),
        Some(6),
        "the write tier's own lock-contention counter must be on the wire: folded into \
         `evictions_skipped_lock_held` it would be impossible to tell whether the write tier \
         is being starved of the lock by read-tier eviction"
    );
}

/// R12.3's three `Option` fields survive serialisation as themselves.
///
/// # Why a name check is not enough for these three
///
/// `every_flattened_cache_field_is_a_top_level_cache_key` asserts the key is present, and a `null`
/// is a present key. So for the fields whose whole design is the distinction between "unknown" and
/// "zero", the name assertion is satisfied by exactly the value that would destroy the property:
///
/// * `eviction_shed_bytes_per_sec` reading `0` instead of `null` would state "shedding nothing"
///   about an unpressured cache and drag `eviction_keeping_pace` below any non-zero ingest — design
///   § 8's false alarm, arriving on every cold start.
/// * `eviction_keeping_pace` reading `false` instead of `null` is the alarm condition, so a
///   serialisation that collapsed them would page an operator for a measurement that has not
///   happened.
/// * `eviction_backlog_bytes` must carry its **sign**: negative is the headroom remaining before
///   eviction fires, which is the healthy reading, and an unsigned encoding would have to discard
///   it.
#[test]
fn the_rate_gauges_carry_their_unknowns_and_their_sign_on_the_wire() {
    let populated = wire(&populated_metrics());
    let cache = populated.get("cache").unwrap();

    assert_eq!(
        cache.get("eviction_keeping_pace").and_then(|v| v.as_bool()),
        Some(true),
        "the verdict must serialise as a JSON boolean"
    );
    assert_eq!(
        cache.get("eviction_backlog_bytes").and_then(|v| v.as_i64()),
        Some(-450),
        "the backlog must carry its sign: NEGATIVE is the healthy case — headroom before the \
         trigger — and an unsigned field could not represent it"
    );
    assert!(
        cache
            .get("eviction_shed_bytes_per_sec")
            .and_then(|v| v.as_f64())
            .is_some(),
        "a measured rate must serialise as a number"
    );

    // The unknown side, on the same payload shape, so `null` and `0`/`false` are demonstrably
    // different values on the wire rather than only different in the type.
    let mut unknown = populated_metrics();
    unknown.cache.as_mut().unwrap().eviction_rates = Default::default();
    let unknown = wire(&unknown);
    let cache = unknown.get("cache").unwrap();
    for field in [
        "eviction_shed_bytes_per_sec",
        "eviction_shed_ranges_per_sec",
        "admission_bytes_per_sec",
        "admission_ranges_per_sec",
        "eviction_keeping_pace",
        "eviction_backlog_bytes",
    ] {
        assert!(
            cache.get(field).is_some_and(|v| v.is_null()),
            "`cache.{field}` must be JSON null when unmeasured, never 0 or false. A zero rate \
             means `shedding nothing` and a false verdict means `falling behind`; both are real \
             readings, so unknown needs its own value. Got: {:?}",
            cache.get(field)
        );
    }
    // And the counters beside them are real zeros, because a counter's zero always means "no
    // event yet" and needs no sentinel.
    assert_eq!(
        cache
            .get("eviction_shed_bytes_total")
            .and_then(|v| v.as_u64()),
        Some(0),
        "the cumulative counters are plain counters: zero is their honest reading and they must \
         not become `null`"
    );
}

/// `cache_hit_buffer`'s five names reach the wire as a nested object, each carrying its own value.
///
/// # Why this is asserted here and not left to the integration test
///
/// `tests/cache_hit_buffer_metrics_test.rs` drives a real buffer and asserts the figures MOVE. It
/// reads them through `SystemMetrics` fields, so it would pass identically against a payload whose
/// serialised key names had changed — a dashboard reads JSON keys, not Rust fields, and a missing
/// key reads as absent rather than as an error. This file is the other end.
///
/// The value check is the part that earns its place: `contains_key` alone would pass against a
/// collector that filled every field from the same figure, which is the specific defect this
/// section exists to fix one level down. Each expected value below is distinct.
///
/// The section is deliberately NOT flattened into the top level, so unlike the `cache.*` block
/// above the correct shape here is a nested object. Asserting that explicitly means a later
/// `#[serde(flatten)]` — which would scatter five generically-named counters across the root — reds
/// this test instead of silently changing every name.
#[test]
fn the_cache_hit_buffer_section_reaches_the_wire_with_its_five_figures() {
    let value = wire(&populated_metrics());
    let section = value
        .get("cache_hit_buffer")
        .and_then(|s| s.as_object())
        .expect(
            "`cache_hit_buffer` must be a nested object on the wire. A `null` here means the \
             collector or the fixture stopped populating it; an absent key means the field was \
             renamed or flattened away",
        );
    for (key, expected) in [
        ("pending_updates", 45u64),
        ("updates_recorded_total", 1_204),
        ("entries_flushed_total", 1_159),
        ("flush_errors_total", 3),
        ("ram_records_dropped_total", 7),
    ] {
        let got = section.get(key).and_then(|v| v.as_u64());
        assert_eq!(
            got,
            Some(expected),
            "`cache_hit_buffer.{key}` must be on the wire carrying its own value. Got {got:?}, \
             expected {expected}. Keys actually present: {:?}",
            {
                let mut keys: Vec<&String> = section.keys().collect();
                keys.sort();
                keys
            }
        );
    }
}

/// The absent buffer serialises as `null`, not as five zeroes.
///
/// This is the half that makes the section interpretable at all. A wired-but-idle buffer reports
/// zeroes legitimately — an empty queue and no traffic yet — so if "no buffer at all" also reported
/// zeroes an operator could not tell a dead write-behind from a quiet one. That ambiguity is the
/// defect the six deleted `batch_flush_*` fields had: they read zero because nothing wrote them,
/// and nothing on the wire said so.
#[test]
fn an_absent_cache_hit_buffer_is_null_rather_than_a_zeroed_section() {
    let mut metrics = populated_metrics();
    metrics.cache_hit_buffer = None;
    let value = wire(&metrics);
    assert_eq!(
        value.get("cache_hit_buffer"),
        Some(&serde_json::Value::Null),
        "an absent cache-hit buffer must serialise as an explicit `null`, so \"nothing to report\" \
         is distinguishable from \"a wired buffer reporting zeroes\". Got {:?}",
        value.get("cache_hit_buffer")
    );
}

/// `orphan_reclaim` is a nested top-level section (R15.11), and its journal-evidence outcomes are
/// one nested object keyed by the four `result` names, not four flat fields. Literal names: a
/// rename or a `flatten` reds this rather than silently moving a dashboard's keys.
#[test]
fn the_orphan_reclaim_section_reaches_the_wire_with_every_figure() {
    let value = wire(&populated_metrics());
    let section = value
        .get("orphan_reclaim")
        .and_then(|s| s.as_object())
        .expect("`orphan_reclaim` must be a nested object on the wire");
    let keys = || {
        let mut k: Vec<&String> = section.keys().collect();
        k.sort();
        k
    };
    assert_eq!(
        section.get("enabled"),
        Some(&serde_json::Value::Bool(true)),
        "keys present: {:?}",
        keys()
    );
    for (key, expected) in [
        ("found", 17u64),
        ("reclaimed", 5),
        ("bytes_reclaimed", 81_920),
        ("skipped_unconfirmed", 4),
        ("kept_listed", 2),
        ("passes", 11),
        ("units_visited", 44),
        ("stalest_checkpoint_age_secs", 612),
        ("units_never_scanned", 7),
        ("passes_gated", 13),
        ("leases_unreadable", 14),
        ("leases_unreadable_expired", 15),
    ] {
        let got = section.get(key).and_then(|v| v.as_u64());
        assert_eq!(
            got,
            Some(expected),
            "`orphan_reclaim.{key}`: got {got:?}, expected {expected}. Keys present: {:?}",
            keys()
        );
    }
    let evidence = section
        .get("journal_evidence_total")
        .and_then(|s| s.as_object())
        .expect("`orphan_reclaim.journal_evidence_total` must be a nested object");
    for (result, expected) in [
        ("confirmed_absent", 6u64),
        ("journal_present", 3),
        ("peer_unavailable", 8),
        ("indeterminate", 9),
    ] {
        assert_eq!(
            evidence.get(result).and_then(|v| v.as_u64()),
            Some(expected),
            "`orphan_reclaim.journal_evidence_total.{result}`"
        );
    }
    assert_eq!(section.len(), 14, "keys present: {:?}", keys());
}

/// `eviction_worklist` is a nested top-level section, and its three by-outcome and by-reason
/// totals are nested objects keyed by literal names. A rename or a `flatten` reds this.
#[test]
fn the_eviction_worklist_section_reaches_the_wire_with_every_figure() {
    let value = wire(&populated_metrics());
    let section = value
        .get("eviction_worklist")
        .and_then(|s| s.as_object())
        .expect("`eviction_worklist` must be a nested object on the wire");
    let mut keys: Vec<&String> = section.keys().collect();
    keys.sort();
    for (key, expected) in [
        ("generation", 21u64),
        ("chunks_authored_total", 22),
        ("chunks_done_total", 25),
        ("chunks_unusable_total", 26),
        ("items_executed_total", 27),
        ("lease_takeovers_total", 30),
        ("in_flight_bytes", 33),
    ] {
        let got = section.get(key).and_then(|v| v.as_u64());
        assert_eq!(
            got,
            Some(expected),
            "`eviction_worklist.{key}`: got {got:?}, expected {expected}. Keys present: {keys:?}"
        );
    }
    for (nested, inner, expected) in [
        ("chunks_claimed_total", "claimed", 23u64),
        ("chunks_claimed_total", "busy", 24),
        ("items_refused_total", "identity_changed", 28),
        ("items_refused_total", "stale_generation", 29),
        ("author_fallback_total", "not_capable", 31),
        ("author_fallback_total", "unknown", 32),
    ] {
        let got = section
            .get(nested)
            .and_then(|s| s.as_object())
            .unwrap_or_else(|| panic!("`eviction_worklist.{nested}` must be a nested object"))
            .get(inner)
            .and_then(|v| v.as_u64());
        assert_eq!(got, Some(expected), "`eviction_worklist.{nested}.{inner}`");
    }
    for nested in [
        "chunks_claimed_total",
        "items_refused_total",
        "author_fallback_total",
    ] {
        assert_eq!(
            section[nested].as_object().map(|o| o.len()),
            Some(2),
            "`eviction_worklist.{nested}` has exactly two keys"
        );
    }
    assert_eq!(section.len(), 10, "keys present: {keys:?}");
}
/// Before the first pass the two rotation gauges are `null`, not 0: a 0 age would read as "the
/// whole volume was just scanned".
#[test]
fn the_orphan_rotation_gauges_are_null_before_the_first_pass() {
    let mut metrics = populated_metrics();
    metrics.orphan_reclaim.stalest_checkpoint_age_secs = None;
    metrics.orphan_reclaim.units_never_scanned = None;
    let value = wire(&metrics);
    for key in ["stalest_checkpoint_age_secs", "units_never_scanned"] {
        assert_eq!(
            value["orphan_reclaim"].get(key),
            Some(&serde_json::Value::Null),
            "`orphan_reclaim.{key}` must serialise as an explicit null"
        );
    }
}
