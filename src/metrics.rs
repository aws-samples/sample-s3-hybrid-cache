//! Metrics Collection Module
//!
//! Provides comprehensive metrics collection for cache, compression, and connection pools.

use crate::cache::CacheManager;
use crate::cache_size_tracker::CacheSizeMetrics;
use crate::compression::CompressionHandler;
use crate::config::OtlpConfig;
use crate::connection_pool::ConnectionPoolManager;
use crate::hedged_fetch::HedgeMetrics;
use crate::otlp::OtlpExporter;
use crate::{ProxyError, Result};
use hyper::{Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Cache writes refused with ENOSPC since process start. Process-global rather than a
/// `MetricsManager` field so that write sites with no handle to the manager (the journal, the
/// streaming range writer's associated functions) can count too. Only
/// `disk_cache::note_disk_full` increments it.
static DISK_FULL_EVENTS_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Count one disk-full cache write and return the new total.
pub(crate) fn record_disk_full_event() -> u64 {
    DISK_FULL_EVENTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
}

/// Disk-full cache writes since process start (`cache.disk_full_events_total`).
pub(crate) fn disk_full_events_total() -> u64 {
    DISK_FULL_EVENTS_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Convert the RAM cache hit rate held in `CacheStatistics::ram_cache_hit_rate`
/// (a 0.0-1.0 fraction, sourced from `ShardedRamCache::stats()`) into the
/// percentage published as `cache.ram_cache_hit_rate_percent` on `/metrics`.
fn ram_hit_rate_percent(fraction: f32) -> f32 {
    fraction * 100.0
}

/// Cache metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    pub total_cache_size: u64,
    /// Configured `cache.max_cache_size`, i.e. the figure `total_cache_size` is a
    /// fraction of. 0 means no limit is configured.
    ///
    /// Exposed so utilisation is computable from `/metrics` alone. Without it a consumer
    /// can see how full the cache is but not how full it is *allowed* to get, which also
    /// made the denominator behind the `/health` cache percentage invisible.
    pub max_cache_size_limit: u64,
    pub read_cache_size: u64,
    /// Seconds the staged figure has exceeded `total_size` without a break; `0` when it has
    /// not. While non-zero (up to 87,300 s) `read_cache_size` is held at its last consistent
    /// value; past that it is published as 0 with a WARN, and this keeps counting. Copied from
    /// [`crate::cache::CacheSizes::read_cache_size_inverted_secs`], the same evaluation that
    /// produced `read_cache_size`. Process memory: it restarts at 0 when the proxy restarts.
    #[serde(default)]
    pub read_cache_size_inverted_secs: u64,
    pub write_cache_size: u64,
    pub ram_cache_size: u64,
    /// Bytes occupied by multipart-upload parts staged under `mpus_in_progress/` and
    /// not yet completed — **compressed on-disk bytes**, the same unit as
    /// `total_cache_size`, summed from the `compressed_size` field of each
    /// `part{N}.json` record.
    ///
    /// Its own gauge, and deliberately not a term in `total_cache_size`,
    /// `read_cache_size` or `write_cache_size`: those three are unchanged by its
    /// arrival and their identity `total_cache_size == read_cache_size +
    /// write_cache_size` continues to hold exactly. The population it counts
    /// (`mpus_in_progress/`) is disjoint from the population they count (`ranges/`),
    /// so it is additional real occupancy that the three composites do not yet
    /// report.
    ///
    /// `None` is the UNREADABLE sentinel: a failed `mpus_in_progress/` traversal is not
    /// the same observation as "no upload is in flight", and 0 cannot say which it was.
    /// Serialises as JSON `null`.
    ///
    /// Background-derived: the collector walks `mpus_in_progress/` once per
    /// `metrics.collection_interval`, and a request serves that walk's figure.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.2, 16.6.3.
    pub in_progress_part_bytes: Option<u64>,
    /// Seconds since the walk of `mpus_in_progress/` that produced `in_progress_part_bytes`
    /// began, advanced to the moment of the response, so up to one collection interval plus one
    /// collection's duration in the steady state. `None` exactly when `in_progress_part_bytes`
    /// is.
    #[serde(default)]
    pub in_progress_part_bytes_age_secs: Option<u64>,
    /// How far the per-proxy published in-progress figures stood from the truth at the last
    /// collection: the figure a deciding reader would have composed at the instant this
    /// collection's walk began, minus that walk's own figure.
    ///
    /// Zero or positive in health, because a composition over-counts a removal its base walk has
    /// not seen yet. **Negative means some proxy's counter missed an update**, which is the
    /// under-count direction, and every deciding reader on this proxy takes a walk of
    /// `mpus_in_progress/` instead of composing until the next collection replaces the verdict.
    ///
    /// `null` when either term is unreadable, or until this proxy has made two walks.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.2.
    #[serde(default)]
    pub in_progress_part_bytes_drift_bytes: Option<i64>,
    /// The last **fleet-wide** read-tier eviction pass's result, read from
    /// `metadata/_shard_state/eviction_last_result.json`.
    ///
    /// Shared rather than process-local, because any proxy may win the eviction lock and a
    /// local counter cannot answer a fleet question (§ 16.6.7). Before
    /// this the pass's figures reached one `info!` line and nothing machine-readable.
    ///
    /// Carries `shortfall_bytes` with `unevictable_in_progress_bytes` as a **named
    /// component** of it, which § 16.6.2 requires so that a trigger firing and freeing
    /// nothing is diagnosable. `null` when no pass has completed on this volume.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.2, 16.6.7.
    pub eviction_last_result: Option<crate::eviction_result::EvictionPassResult>,
    /// The `st_size` maintenance pass: how long its last run took here, how many scheduling
    /// ticks it declined because one was already running, and the age of the stalest per-shard
    /// figure on the volume.
    ///
    /// **Read the decline counter first when the figures look stale.** A pass that is declined
    /// on every tick returns a normal, non-error outcome and logs at `debug`, so a deployment
    /// where it never once completes is indistinguishable from a healthy one on every other
    /// signal — see `crate::st_size_fold::StSizeFoldGauges`.
    ///
    /// The age is fleet-wide (read from the shared volume); the other two are per-instance.
    #[serde(flatten)]
    pub st_size_fold: crate::st_size_fold::StSizeFoldGauges,
    /// The Tail_Partition mechanism: the floor and per-shard cap **in force in this process**,
    /// how many shard partitions this process has rebuilt, how many members the last rebuild
    /// wrote, and how many tail-delta records are outstanding across the fleet.
    ///
    /// **Read the floor first when partitions look empty.** A floor set above a cache's whole
    /// size distribution produces empty partitions, and an empty partition is indistinguishable
    /// from an idle cache on every other signal. `tail_delta_records` is the one that shows
    /// retirement keeping up.
    ///
    /// `tail_coverage_ratio` is deliberately absent — see
    /// `crate::cache_size_tracker::TailPartitionGauges`.
    #[serde(flatten)]
    pub tail_partition: crate::cache_size_tracker::TailPartitionGauges,
    /// The **selection** side of the Tail_Partition mechanism: R3.4's coverage ratio and its
    /// `TAIL_COVERAGE_SHORT` emissions, § 4.5's three fallback outcomes, § 5.3's
    /// oversized-single-candidate guard, and § 5.4's verification verdicts by reason.
    ///
    /// **Read the three fallback counters before concluding the mechanism works.** A deployment
    /// whose partitions have never been built falls back on every pass and is indistinguishable
    /// from one using the bounded path on every other signal —
    /// `eviction_fallback_unvisited_total` is the one that says so, and it is deliberately
    /// separate from `..._tail_empty_total`, which is a claim about what the cache holds rather
    /// than about whether anything has looked.
    ///
    /// **`tail_coverage_ratio` is as current as the last selection pass, not as current as now.**
    /// An unpressured cache runs no pass. See `crate::cache::TailSelectionGauges`.
    #[serde(flatten)]
    pub tail_selection: crate::cache::TailSelectionGauges,
    /// The eviction **executor**: R3.12's whole-object/partial split, R15.3's downgrade, R15.4's
    /// unlink failures, and the `.meta` rewrite count.
    ///
    /// **`eviction_meta_rewrites_total` should equal `eviction_pass_path_partial_total`.** One
    /// rewrite per partially-evicted object and none for a whole-object eviction is the whole of
    /// § 5.5's throughput argument, and a rewrite count above the partial count is the one signal
    /// that the whole-object path has stopped taking it — every other figure is identical either
    /// way, because a rewrite-then-delete leaves no `.meta` just as a bare delete does.
    ///
    /// Per-instance, like the request counters and unlike `total_size`.
    #[serde(flatten)]
    pub eviction_executor: crate::cache::EvictionExecutorGauges,
    /// R12.3's eviction rate, the admission rate it is racing, the verdict, and the backlog.
    ///
    /// See [`EvictionRateGauges`] for the two denominators and why they differ.
    #[serde(flatten)]
    pub eviction_rates: EvictionRateGauges,
    pub cache_hit_rate_percent: f32,
    pub ram_cache_hit_rate_percent: f32,
    pub total_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Range responses refetched from S3 because cached extents were incomplete.
    pub incomplete_range_fallbacks: u64,
    pub evictions: u64,
    pub corruption_metadata_total: u64,
    pub corruption_missing_range_total: u64,
    /// Cache writes that failed because the cache volume is full (ENOSPC), process-wide
    /// (`AtomicU64`, incremented through `disk_cache::note_disk_full`). **Per-instance.** Which
    /// writes are counted is listed in `docs/OTLP_METRICS.md`.
    pub disk_full_events_total: u64,
    /// Callers that exhausted `cache.shared_storage.lock_max_retries` waiting for a
    /// metadata slot lock and gave up, process-wide (`AtomicU64` — see
    /// `crate::metadata_lock_manager`).
    ///
    /// Replaces `lock_timeout_total`, which asserted a timeout the metadata lock
    /// protocol does not have: the protocol never infers liveness from elapsed time, so
    /// what bounds a caller's wait is the retry count. The old field had no production
    /// writer and was permanently zero.
    pub metadata_lock_retry_exhausted_total: u64,
    /// Cache-tee backpressure counters, process-wide (`AtomicU64`, incremented
    /// directly at the point of backpressure — see `crate::tee_stream`). Non-
    /// zero means the cache writer, not the upstream, was the bottleneck for
    /// at least one request. `get` is the GET-miss `TeeStream` path; `put` is
    /// the signed-PUT/UploadPart streaming path.
    /// Spec: cache-eviction-at-scale Requirement 18.4.
    pub tee_backpressure_events_total_get: u64,
    pub tee_backpressure_wait_ms_total_get: u64,
    pub tee_backpressure_events_total_put: u64,
    pub tee_backpressure_wait_ms_total_put: u64,
    /// Streamed PUT/`UploadPart` write-cache tees abandoned because the cache writer did
    /// not accept a frame within `WRITE_CACHE_TEE_STALL_BOUND` (5 s), process-wide
    /// (`AtomicU64` in `crate::signed_put_handler`). **Per-instance.** Each one is an
    /// upload that went to S3 uncached because the cache volume stalled.
    pub tee_abandoned_total_put: u64,
    /// Unparseable `.meta` documents replaced by a freshly fetched one, process-wide
    /// (`AtomicU64` in `crate::disk_cache`). **Per-instance.** Read against
    /// `corruption_metadata_total`: a detection is healed only when a later fill of the same
    /// key replaces the document.
    pub metadata_heal_overwrite_total: u64,
    /// GET-miss responses cut off because the upstream sent no bytes for
    /// `connection_pool.upstream_idle_timeout` mid-body, process-wide (`AtomicU64` in
    /// `crate::tee_stream`). **Per-instance.** Each one is a client response truncated.
    pub upstream_idle_abort_total_mid_stream: u64,
    /// GET/HEAD misses answered 504 because every attempt missed
    /// `connection_pool.upstream_first_byte_timeout`. **Per-instance.**
    pub upstream_idle_abort_total_pre_stream: u64,
    /// First-byte timeouts that were retried (at most `upstream_idle_retries` per request).
    /// **Per-instance.**
    pub upstream_idle_retry_total: u64,
    /// Directory entries beneath a shard whose type could not be resolved at all while a
    /// traversal was classifying them, process-wide (`AtomicU64`, incremented at the shared
    /// predicate — see `crate::disk_cache::is_shard_subdirectory`).
    ///
    /// **Non-zero is a finding, not a routine reading.** A strict subset of the `DT_UNKNOWN`
    /// population rather than its rate — see the counter's own doc comment for why the name says
    /// "resolution failures" and not "DT_UNKNOWN".
    /// Spec: cache-eviction-at-scale § 16.6.13 (T2).
    pub dirent_type_resolution_failures_total: u64,
    /// Stale file handles (`ESTALE`) observed while reading a `.meta`, once per observation
    /// including each retried one, process-wide (`AtomicU64`, incremented in
    /// `crate::disk_cache::read_and_parse_metadata_blocking`). **Per-instance.**
    ///
    /// **Non-zero is a finding worth alarming on.** It means the shared volume invalidated a
    /// handle this proxy held to a metadata file, usually because another proxy replaced it
    /// mid-read. Covers `.meta` reads only — not `.bin` reads and not metadata writes.
    pub metadata_read_stale_handles_total: u64,
    /// `.meta` reads that a stale file handle made the proxy abandon and treat as a cache miss
    /// (fetched from S3 instead), process-wide. **Per-instance.** The subset of
    /// `metadata_read_stale_handles_total` that the fixed three retries did not absorb.
    ///
    /// **Non-zero is a finding worth alarming on**: each one is an S3 request the cache would
    /// otherwise have served.
    pub metadata_read_stale_handle_misses_total: u64,
    // New range storage metrics
    pub metadata_parse_duration_ms: f64,
    pub metadata_file_size_bytes: u64,
    pub range_file_count: u64,
    pub range_load_duration_ms: f64,
    // Cache bypass metrics by reason
    pub cache_bypasses_by_reason: HashMap<String, u64>,
    /// Cache bypasses caused by the request's origin differing from the origin recorded in
    /// the bucket's `_origin.json` marker, process-wide (`AtomicU64` — see
    /// `crate::origin_identity`). **Per-instance.**
    ///
    /// **Non-zero is a finding, not a routine reading.** A single-origin deployment reads
    /// zero forever. Non-zero means one bucket name is in use at two S3-compatible origins,
    /// and requests from whichever origin did not claim the bucket are being forwarded
    /// uncached — which is correct, and is also a misconfiguration worth knowing about,
    /// since it presents otherwise as an unexplained drop in hit rate.
    ///
    /// Two flat counters rather than one labelled map, following
    /// `tee_backpressure_*_{get,put}`: the cause set is fixed at compile time.
    /// Spec: public-security-review-readiness R2.5.
    pub origin_marker_mismatch_bypasses_total: u64,
    /// Cache bypasses caused by a marker that is present and could not be read, process-wide.
    /// **Per-instance.**
    ///
    /// **Non-zero is a finding.** Counted apart from an ordinary mismatch because the cause
    /// and the operator's fix differ: this is a damaged or hand-edited `_origin.json`, not a
    /// second origin. Neither reading of such a file is safe — treating it as absent
    /// reintroduces the cross-origin collision, treating it as a mismatch disables caching
    /// for that bucket — so the proxy bypasses, and this counter is what stops that being
    /// silent. The remedy is to inspect the file and, if it is genuinely corrupt, remove it
    /// so the next request re-claims the bucket.
    /// Spec: public-security-review-readiness R2.5.
    pub origin_marker_unparseable_bypasses_total: u64,
    /// Cache bypasses caused by an origin-marker read or claim that did not finish within
    /// `cache::PUT_ROUTER_READ_BOUND` (250 ms) for a write routed through
    /// `HttpProxy::handle_put_request`, or `origin_identity::ORIGIN_CLAIM_BOUND` (5 s) for any
    /// other request, process-wide. **Per-instance.**
    ///
    /// Non-zero means the shared cache volume was slow to answer a bucket's first request in
    /// this process; those requests were forwarded uncached rather than held before their
    /// bodies were read, and such a write invalidated the key's cached copy on success. Not a
    /// second origin.
    #[serde(default)]
    pub origin_marker_unresolved_bypasses_total: u64,
    /// Cache bypasses caused by a host under an AWS partition suffix that is not an S3
    /// endpoint the proxy caches (`OriginVerdict::UncacheableHost`), process-wide.
    /// **Per-instance.** Such a request reads no entry, writes none and claims no marker.
    #[serde(default)]
    pub uncacheable_aws_host_bypasses_total: u64,
    /// Acknowledged writes (PUT, CompleteMultipartUpload, DELETE) whose write-order stamp
    /// failed and was handed to the background retry: started, joined to a pending retry of the
    /// same key, or queued (`crate::cache::write_stamp_retried_total`). **Per-instance.**
    #[serde(default)]
    pub write_stamp_retried_total: u64,
    /// Keys whose write-order stamp landed on a background retry. **Per-instance.**
    #[serde(default)]
    pub write_stamp_retry_landed_total: u64,
    /// Keys waiting now for a free retry task, in the memory-bounded overflow queue.
    /// **Per-instance.**
    #[serde(default)]
    pub write_stamp_retry_overflow_keys: u64,
    /// Keys held now by running retry tasks, one per task. Keys pending a stamp retry are this
    /// plus `write_stamp_retry_overflow_keys`. **Per-instance.**
    #[serde(default)]
    pub write_stamp_retry_active_keys: u64,
    /// Acknowledged writes whose failed stamp is not retried: every retry task was busy, the
    /// overflow queue was full and the node-local pending-stamp log could not be written, or
    /// a retry task ended early with no log record of its key. **Per-instance.** **Non-zero is
    /// a finding**: an older write of each such key can be served until its TTL.
    #[serde(default)]
    pub write_stamp_retry_abandoned_total: u64,
    /// Keys with a pending write-order stamp in this proxy's node-local log now
    /// (`crate::stamp_backlog`). **Per-instance.**
    #[serde(default)]
    pub write_stamp_log_pending_keys: u64,
    /// Pending stamps re-queued from the node-local log at startup. **Per-instance.**
    #[serde(default)]
    pub write_stamp_log_replayed_total: u64,
    /// Unparseable lines (a torn final append) skipped while reading the node-local log at
    /// startup. **Per-instance.**
    #[serde(default)]
    pub write_stamp_log_torn_records_total: u64,
    /// Failed writes whose pending record could not be written to the node-local log.
    /// **Per-instance.** **Non-zero is a finding**: those stamps are retried from memory only.
    #[serde(default)]
    pub write_stamp_log_write_failures_total: u64,
    /// `1` when this proxy's node-local log runs in memory only: `local_state_dir` was
    /// refused (inside `cache_dir`, or on a file system that is not a known local type) or could
    /// not be used at startup.
    /// **Per-instance.** **Non-zero is a finding**: a restart drops pending stamps.
    #[serde(default)]
    pub write_stamp_log_memory_only: u64,
    /// Stores and fills this proxy declined because it held a pending stamp for their key.
    /// **Per-instance.**
    #[serde(default)]
    pub write_stamp_pending_declined_total: u64,
    /// `1` while this proxy's volume-stall breaker is open: writes skip the cache and GET
    /// and HEAD are forwarded to S3 uncached. **Per-instance.**
    #[serde(default)]
    pub volume_stall_breaker_open: u64,
    /// Times this proxy's volume-stall breaker has opened. **Per-instance.**
    #[serde(default)]
    pub volume_stall_breaker_trips_total: u64,
    // Cache coherency metrics
    pub cache_etag_validations_total: u64,
    pub cache_etag_mismatches_total: u64,
    pub cache_range_invalidations_total: u64,
    pub cache_orphaned_ranges_cleaned_total: u64,
    pub cache_cleanup_failures_total: u64,
    // Part caching metrics - Requirements 8.1, 8.2, 8.3, 8.4, 8.5
    pub cache_part_hits: u64,
    pub cache_part_misses: u64,
    pub cache_part_stores: u64,
    pub cache_part_evictions: u64,
    pub cache_part_errors: u64,
    // Write cache metrics - Requirement 11.4
    pub incomplete_uploads_evicted: u64,
    // Per-tier RAM cache stats (from ram_cache.get_stats())
    pub ram_cache_hits: u64,
    pub ram_cache_misses: u64,
    pub ram_cache_evictions: u64,
    pub ram_cache_max_size: u64,
    /// Entries resident in this instance's RAM tier right now: range entries plus whole-object
    /// entries, summed across shards. A level, per-instance. Read with `ram_cache_size` for the
    /// mean entry size; an invalidation drops entries without moving `ram_cache_evictions`, and
    /// this figure shows it.
    #[serde(default)]
    pub ram_cache_entries: u64,
    /// RAM-tier admissions declined because the key's resolved `ram_cache_eligible` was
    /// `false` (a `cache_rules.json` rule). Per-instance, since process start. A rising value
    /// with a low `ram_cache_hit_rate_percent` means the gate is closed for the traffic, not that
    /// the tier is ineffective. Page-widening skips are `page_cache.ram_page_promotion_skipped`.
    #[serde(default)]
    pub ram_cache_eligibility_skips_total: u64,
    // Per-tier metadata cache stats (from metadata_cache.metrics())
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub metadata_cache_entries: u64,
    pub metadata_cache_max_entries: u64,
    pub metadata_cache_evictions: u64,
    pub metadata_cache_stale_refreshes: u64,
    /// Metadata-cache RAM hits serving a **HEAD** specifically.
    ///
    /// # Why the HEAD path needs its own three counters
    ///
    /// `metadata_cache_hits` and `metadata_cache_misses` count lookups from **every** request type,
    /// so they cannot answer "is the metadata tier working for my HEAD traffic?" — which is the
    /// question that matters, because a `HEAD`-heavy workload against objects that are never read
    /// is the access pattern the tier was sized for. Operators substituted the aggregate for want
    /// of anything better, and `docs/METRICS_REFERENCE.md` now warns against exactly that
    /// substitution, so until these three were published an operator had the warning and no
    /// substitute.
    ///
    /// **Raw counters, not a rate.** `cache.ram_cache_hit_rate_percent` is the precedent not to
    /// follow: it is cumulative since process start and a sub-range access pattern caps it at 50%,
    /// and neither qualification fits in a field name. Divide these yourself, over a window.
    ///
    /// **Per-instance**, like every other `metadata_cache_*` figure — the tier is per-process RAM
    /// and nothing propagates between proxies, so read them on the proxy that served the traffic.
    pub metadata_cache_head_hits: u64,
    /// Metadata-cache RAM misses serving a **HEAD**. See [`Self::metadata_cache_head_hits`].
    pub metadata_cache_head_misses: u64,
    /// Metadata-cache **disk** hits serving a `HEAD` — a RAM miss that was found on the cache
    /// volume. See [`Self::metadata_cache_head_hits`].
    ///
    /// Read beside `metadata_cache_head_misses`: a miss that became a disk hit cost a shared-volume
    /// read, and a miss that did not cost an upstream request, so the two have different prices.
    pub metadata_cache_head_disk_hits: u64,
    /// Metadata documents read from the cache volume after a metadata-cache RAM miss, by the
    /// non-`HEAD` lookup `CacheManager::get_metadata_cached`.
    ///
    /// Its one production increment is in that function, after a successful disk read and whether
    /// or not the document was then admitted to RAM. The callers are the mainline `GET`, range and
    /// part-scoped serve paths, plus the conditional-request block at the top of
    /// `handle_get_head_request`, which runs before the `GET`/`HEAD` split — so a conditional
    /// `HEAD` can move this figure too. An unconditional `HEAD` never does; its own figure is
    /// [`Self::metadata_cache_head_disk_hits`].
    ///
    /// Each one cost a shared-volume read, where a RAM hit cost none. Read it beside
    /// `metadata_cache_misses`, which counts the RAM misses from every request type. Per-instance,
    /// and a raw counter since process start rather than a rate.
    pub metadata_cache_disk_hits: u64,
    // Bytes served from cache (for S3 transfer saved)
    pub bytes_served_from_cache: u64,
    // Eager invalidation counter — read_cache_enabled=false purges
    pub read_cache_disabled_invalidations_total: u64,
    // TTL-driven revalidation counter — freshness expired, conditional GET/HEAD to S3
    pub ttl_revalidations_total: u64,
    /// Real bytes consumed on the cache volume's filesystem, read via `statvfs`
    /// (`fs4::total_space(cache_dir) - fs4::available_space(cache_dir)`). Observability
    /// only — does not feed any eviction or admission decision.
    ///
    /// **Not the capacity authority.** `statvfs` consumed is not monotone under the
    /// proxy's own deletions on a deduplicating, snapshotting, or block-sharing backend
    /// (VAST always, ZFS/FSxZ with snapshots, NetApp with dedup), so an eviction trigger
    /// built on this figure can livelock: eviction deletes, consumed does not move, and
    /// the pass never reaches `eviction_target_percent`. `Σ compressed_size`
    /// (`total_cache_size`) remains the budget authority. `consumed_bytes` is retained
    /// for the disk-exhaustion admission guard (`disk_safety_refusal`) and as an
    /// observability-only reading of real volume consumption. `0` if the filesystem
    /// call fails (fail-open, same posture as `disk_safety_refusal_inner`'s free-space
    /// check) — this is indistinguishable from a genuinely empty volume in the JSON
    /// alone, so treat a `0` reading with the same caution as any other unverified
    /// negative claim.
    ///
    /// Per-filesystem, not per-directory: on a shared volume also holding logs,
    /// journals, and the write ledger, this attributes those neighbouring bytes to the
    /// cache. Comparing this against `total_cache_size` mixes a per-instance,
    /// per-filesystem reading with a fleet-wide, cache-scoped one — do the subtraction
    /// yourself only with that in mind. See `docs/SHARED_STORAGE.md`.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 8.2.
    pub consumed_bytes: u64,
    /// The danger-band capacity guard: publications declined near a full volume, the guard's
    /// last free-space reading, and the configured floor. Per-instance. See
    /// [`crate::capacity_guard::CapacityGuardGauges`].
    pub capacity_guard: crate::capacity_guard::CapacityGuardGauges,
    /// GET-miss fills this process declined to store because the cache was already at
    /// `max_cache_size`. Per-instance, monotonic since process start.
    ///
    /// The read tier's half of the soft stop write-through takes: the client's response is
    /// served either way, byte-for-byte and with the same status, and only the copy is
    /// skipped. Write-through's declines against the same bound are counted separately, in
    /// `signed_put.skipped_puts_total` under `disk_safety`; free-space declines are
    /// `cache.capacity_guard.declines_total`. A rising figure here means eviction is not
    /// keeping pace with miss ingest, not that anything failed.
    pub read_fills_declined_max_cache_size_total: u64,
    /// Times this proxy has detected another proxy sharing its hostname on this cache volume.
    ///
    /// Proxies sharing one cache volume must have distinct hostnames: every instance identity on
    /// the volume is `hostname:pid`, and this proxy decides whether another process of its own
    /// host has stopped by testing that pid locally. Anything above zero is a deployment
    /// mistake, not a transient. Requests are served normally and no figure is wrong, but the
    /// shortcuts resting on that test are off for the life of the process, so abandoned files of
    /// a stopped proxy are cleaned up on their age alone and more slowly; and where the other
    /// proxy holds this one's whole identity, this proxy also stops write-caching, counted as
    /// `hostname_not_unique` in `signed_put.skipped_puts_total`. One per colliding proxy, not
    /// one per check. Per-instance, reset on restart; the first detection also logs one ERROR
    /// naming both identities.
    pub hostname_collisions_total: u64,
}

/// R12.3's eviction rate in both units, the admission rate it is racing, the pacing verdict, and
/// the backlog — flattened onto `cache.*`.
///
/// # The two denominators are DIFFERENT, and that asymmetry is the whole design
///
/// Design § 8 specifies an EWMA over completed passes and then refutes itself in the same section:
/// a measured pass ran active for 78.8 of 240 s and sat idle the rest, which is correct behaviour
/// once the pass reaches its target, so a shed rate divided by wall-clock reads that idle time as
/// falling behind and sets the verdict `false` on a cache that is keeping pace comfortably. Its
/// closing sentence says the flawed form "should be revised before implementation". This is the
/// revised form.
///
/// - **Shed rate: summed ACTIVE pass intervals.** Eviction is a discrete trigger-then-idle
///   process, so its idle time is not a lower rate — it is the absence of demand. What the
///   denominator has to measure is *capability*: how fast this deployment frees bytes while it is
///   freeing them.
/// - **Admission rate: WALL-CLOCK since process start.** Admission has no idle/active
///   distinction — the process is always available to admit — so a quiet minute genuinely is a
///   lower ingest rate, and averaging over wall-clock is the honest reading. An active-interval
///   denominator here would be the mirror error.
///
/// Both halves of that asymmetry are load-bearing, and wall-clock on **both** sides is not a safe
/// simplification: the verdict would reduce to "has the cache grown since this process started",
/// which is `false` for hours on a healthy cold deployment filling toward its trigger. That is a
/// worse false alarm than the one § 8 corrects, because it fires on every restart.
///
/// # Why the cumulative counters ship beside the rates
///
/// A since-start quotient cannot show a recent regression, and an EWMA needs tuning and hidden
/// state. Publishing the numerators and the denominator instead lets a scrape-differencing consumer
/// compute a true windowed rate — and, more importantly, lets an operator compute a **fleet** rate.
/// Every rate here is per-instance; on a deployment where one proxy wins most of the eviction locks
/// while another takes most of the writes, neither proxy's own quotient answers R2.3's question and
/// summing the cumulative counters below across the fleet is the only thing that does.
///
/// # Scope, per field, because mixing them silently is a failure this payload has already had
///
/// Every field is **per-instance** except the two backlog fields, whose numerators are read
/// from shared storage and are therefore **fleet-wide**, while their threshold comes from this
/// instance's configured `max_cache_size` and `eviction_trigger_percent`. On a homogeneously
/// configured fleet those agree and they read the same everywhere; on a mixed one they are "the
/// fleet's size against this proxy's bound", which is still the figure that decides whether
/// *this* proxy evicts.
///
/// `eviction_backlog_projected_bytes` is the one a pass fires on, and
/// `eviction_backlog_bytes` is the resident figure. They differ by every size change the fleet
/// has recorded and no consolidation cycle has yet applied, which under a journal backlog is a
/// cycle's worth of writes, so an operator reading only the resident one can see headroom while
/// eviction runs. Alarm on the projected field and read the resident one beside it.
///
/// Spec: cache-eviction-at-scale § 8. Requirements: 2.3, 12.3.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct EvictionRateGauges {
    /// Accounted-payload bytes freed by completed read-tier passes in this process. **Counter**,
    /// per-instance. The shed rate's numerator, published so the rate can be re-derived over any
    /// window and summed across a fleet.
    pub eviction_shed_bytes_total: u64,
    /// Ranges removed by those passes. **Counter**, per-instance. Ranges, not entries — one entry
    /// is a `.meta` plus N ranges, and there is no entry-level figure on either side of R12.3's
    /// comparison.
    pub eviction_shed_ranges_total: u64,
    /// Summed **active** duration of those passes, microseconds. **Counter**, per-instance. The
    /// shed rate's denominator, and the field that makes the rate auditable: a consumer can check
    /// the published quotient rather than trusting it.
    pub eviction_active_micros_total: u64,
    /// `eviction_shed_bytes_total ÷ eviction_active_micros_total`, bytes per second. **Derived**,
    /// per-instance.
    ///
    /// **`null` means no pass has completed in this process**, and that is not zero. Zero is a
    /// real, meaningful reading — a pass that ran and freed nothing — so under the governing rule
    /// (`Option` where the value's domain includes a meaningful zero) unknown needs its own value.
    /// Publishing `0.0` on an unpressured cache would state "shedding nothing" about a deployment
    /// that has had nothing to shed, and would drag the verdict below any non-zero admission rate:
    /// design § 8's own failure mode, arriving on every fresh start.
    pub eviction_shed_bytes_per_sec: Option<f64>,
    /// `eviction_shed_ranges_total ÷ eviction_active_micros_total`, ranges per second.
    /// **Derived**, per-instance. `null` on the same condition as the bytes rate.
    pub eviction_shed_ranges_per_sec: Option<f64>,
    /// Accounted-payload bytes admitted by this process. **Counter**, per-instance. See
    /// [`crate::journal_consolidator::AdmissionAccumulator`] for the population and its
    /// exclusions.
    pub admission_bytes_total: u64,
    /// Ranges admitted by this process. **Counter**, per-instance. Ranges, not entries.
    pub admission_ranges_total: u64,
    /// `admission_bytes_total ÷ uptime_seconds`, bytes per second. **Derived**, per-instance.
    ///
    /// Design § 10 labels this field "(existing)". It never has existed:
    /// `request_metrics.requests_per_second` is a request rate, not a byte-admission rate, and no
    /// admission counter of any kind was on this payload before it.
    ///
    /// `null` only inside the first second of process life, where the denominator is zero.
    pub admission_bytes_per_sec: Option<f64>,
    /// `admission_ranges_total ÷ uptime_seconds`, ranges per second. **Derived**, per-instance.
    pub admission_ranges_per_sec: Option<f64>,
    /// R2.3's verdict: is eviction keeping pace? **Derived**, per-instance.
    ///
    /// `true` when the shed rate is at least the admission rate **in both units**. Both, not bytes
    /// alone — § 8's own reason for requiring two units is that "a cache that keeps pace in bytes
    /// can fall behind in entries and the two failure modes have different fixes", so a bytes-only
    /// verdict would read healthy on a range-starved cache. Read the per-second rates above to see
    /// which dimension a `false` came from.
    ///
    /// **`null` when any input is unknown, and it is `Option<bool>` for exactly that reason.** An
    /// operator alarms on this field, so `false` must mean "measured, and falling behind" and never
    /// "no measurement yet". A plain `bool` has nowhere to put the third state and would publish
    /// `false` before the first pass, alarming on every cold start. A tri-state string would carry
    /// the same information while inventing a second convention on a payload whose unknown is
    /// already `null`.
    ///
    /// **On a fleet, read it on the proxy that is evicting.** A proxy that never wins the eviction
    /// lock has no shed rate and reports `null`, which is honest — it cannot answer the question. A
    /// proxy that evicts but takes few writes can read `true` while the fleet as a whole falls
    /// behind; sum the cumulative counters above across proxies for the fleet verdict.
    pub eviction_keeping_pace: Option<bool>,
    /// `total_cache_size − (max_cache_size × eviction_trigger_percent ÷ 100)`. **Derived**,
    /// **signed**, bytes.
    ///
    /// **Negative means the resident figure is under the trigger**, by that many bytes. It does
    /// **not** mean no pass is due: the trigger is evaluated on the projected figure, which adds
    /// every recorded-but-unapplied size change, so this field can read negative while passes
    /// fire. [`Self::eviction_backlog_projected_bytes`] is the predicate; this one is the
    /// resident arithmetic, kept because it is the figure that moves when bytes land on the
    /// volume rather than when the fleet records them.
    ///
    /// Signed, because an unsigned gauge would have to discard the under-trigger reading.
    ///
    /// `null` when `max_cache_size` is 0, i.e. eviction is disabled: there is no threshold to
    /// measure against, and 0 would read as "exactly at the trigger".
    ///
    /// Not consumer-derivable before this field: `total_cache_size` and `max_cache_size_limit` both
    /// ship, and `eviction_trigger_percent` ships nowhere.
    ///
    /// **Mixed scope, stated rather than flattened away**: the numerator is fleet-wide (shared
    /// storage), the threshold is this instance's configuration.
    pub eviction_backlog_bytes: Option<i64>,
    /// The same subtraction against the figure the trigger actually compares: the projected size
    /// (`size_state.total_size` plus every recorded-but-unapplied size change) minus the trigger
    /// threshold. **Derived**, **signed**, bytes. **This is the predicate** — positive here means
    /// a pass is due or running.
    ///
    /// `null` when `max_cache_size` is 0, and also **before this process has evaluated the
    /// trigger even once**, which is a third state and not a zero: a proxy that has just started,
    /// or one that never wins the consolidation lock and has run no eviction pass, has no reading
    /// to publish. See [`crate::journal_consolidator::last_projected_cache_size`].
    ///
    /// **As fresh as this proxy's last trigger evaluation**, which on the consolidation-lock
    /// winner is one `cache.shared_storage.consolidation_interval` and on a proxy that neither
    /// wins the lock nor evicts is however long ago its last pass ran. Read it on the proxy that
    /// is evicting, as `eviction_keeping_pace` already asks.
    ///
    /// No shared-volume I/O per scrape: the reading is a process-local atomic the trigger path
    /// refreshes.
    pub eviction_backlog_projected_bytes: Option<i64>,
}

impl EvictionRateGauges {
    /// Assemble the gauges from the two accumulators and the three backlog terms.
    ///
    /// `uptime_secs` is the admission denominator and `shed.active_micros_total` the shed one — see
    /// the struct doc for why those are deliberately not the same denominator.
    pub(crate) fn build(
        shed: crate::cache::EvictionShedAccumulator,
        admission: crate::journal_consolidator::AdmissionAccumulator,
        uptime_secs: u64,
        total_cache_size: u64,
        projected_cache_size: Option<u64>,
        max_cache_size: u64,
        eviction_trigger_percent: u8,
    ) -> Self {
        // `None` rather than a rate over zero time. A pass too fast to register a microsecond
        // cannot happen — a pass does filesystem work — but the guard is the honest one either
        // way: no observed interval means no rate, not an infinite one.
        let per_active_sec = |numerator: u64| -> Option<f64> {
            if shed.active_micros_total == 0 {
                None
            } else {
                Some(numerator as f64 / (shed.active_micros_total as f64 / 1_000_000.0))
            }
        };
        let per_uptime_sec = |numerator: u64| -> Option<f64> {
            if uptime_secs == 0 {
                None
            } else {
                Some(numerator as f64 / uptime_secs as f64)
            }
        };

        let eviction_shed_bytes_per_sec = per_active_sec(shed.shed_bytes_total);
        let eviction_shed_ranges_per_sec = per_active_sec(shed.shed_ranges_total);
        let admission_bytes_per_sec = per_uptime_sec(admission.admission_bytes_total);
        let admission_ranges_per_sec = per_uptime_sec(admission.admission_ranges_total);

        // Both units, not bytes alone, and `None` if any of the four is unknown — a verdict an
        // operator alarms on must not read `false` for want of a measurement.
        let eviction_keeping_pace = match (
            eviction_shed_bytes_per_sec,
            admission_bytes_per_sec,
            eviction_shed_ranges_per_sec,
            admission_ranges_per_sec,
        ) {
            (Some(shed_bytes), Some(adm_bytes), Some(shed_ranges), Some(adm_ranges)) => {
                Some(shed_bytes >= adm_bytes && shed_ranges >= adm_ranges)
            }
            _ => None,
        };

        // The threshold arithmetic `enforce_disk_cache_limits` and `maybe_trigger_eviction`
        // both use, applied to two numerators. The threshold is the same in both, deliberately:
        // a backlog computed against a different threshold from the one that fires the pass
        // would be an adjacent figure rather than a reading of the predicate. Which NUMERATOR is
        // the predicate is the part that changed when the trigger moved to the projected figure,
        // so both are published and the projected one is labelled as the predicate.
        let backlog_against = |numerator: u64| -> Option<i64> {
            if max_cache_size == 0 {
                return None;
            }
            let trigger_bytes =
                (max_cache_size as f64 * (eviction_trigger_percent as f64 / 100.0)) as u64;
            let size = i64::try_from(numerator).unwrap_or(i64::MAX);
            let threshold = i64::try_from(trigger_bytes).unwrap_or(i64::MAX);
            Some(size.saturating_sub(threshold))
        };
        let eviction_backlog_bytes = backlog_against(total_cache_size);
        // `None` on two distinct grounds, and both are honest: eviction is disabled, or this
        // process has not evaluated the trigger yet so there is no projected reading to subtract.
        let eviction_backlog_projected_bytes = projected_cache_size.and_then(backlog_against);

        Self {
            eviction_shed_bytes_total: shed.shed_bytes_total,
            eviction_shed_ranges_total: shed.shed_ranges_total,
            eviction_active_micros_total: shed.active_micros_total,
            eviction_shed_bytes_per_sec,
            eviction_shed_ranges_per_sec,
            admission_bytes_total: admission.admission_bytes_total,
            admission_ranges_total: admission.admission_ranges_total,
            admission_bytes_per_sec,
            admission_ranges_per_sec,
            eviction_keeping_pace,
            eviction_backlog_bytes,
            eviction_backlog_projected_bytes,
        }
    }
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
}

/// Connection pool metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolMetrics {
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
    /// **Staging** eviction passes skipped because another instance held the global eviction
    /// lock — the write tier's analogue of `evictions_skipped_lock_held`, counted separately
    /// because the two tiers share the lock but not the trigger, so folding them together makes
    /// it impossible to tell whether the write tier is being starved of the lock by read-tier
    /// eviction.
    ///
    /// Per-instance. It was already incremented by
    /// `MetricsManager::record_staging_eviction_skipped_lock_held` and already documented as one
    /// of `eviction_coordination`'s counters, but it was a field of `EvictionCoordinationStats`
    /// only — the internal struct — and not of this, the serialised one. A consumer following
    /// that documentation got `null`.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 7.4
    pub staging_evictions_skipped_lock_held: u64,
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
    /// Uploads not write-through cached, keyed by reason. A map rather than
    /// fixed fields (mirrors the `bucket_traffic` precedent) because R8.1's
    /// list of reasons is explicitly a minimum, not a closed set fixed at
    /// compile time. Known reason keys as of this writing: `write_cache_disabled`,
    /// `unknown_length`, `disk_safety`, `object_too_large`, `commit_failed`, `backlog_full`,
    /// `superseded`, `capacity_unknown`, `multipart_tracking_full`.
    /// `capacity_refused` was retired in 2.7.0 and is never emitted — see
    /// `docs/METRICS_REFERENCE.md` for the maintained list.
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.1
    pub skipped_puts_total: HashMap<String, u64>,
}

/// Atomic metadata writes metrics
/// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
///
/// `Default` exists for `src/otlp.rs`'s field guard, which serialises a default instance to read
/// the wire shape; no production path constructs one from it.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AtomicMetadataMetrics {
    // Metadata slot-lock contention (Requirement 9.5).
    //
    // Prefixed `metadata_lock_` because `eviction_coordination` carries fields of the
    // unprefixed names for the GLOBAL EVICTION lock. Two different locks, and before
    // the prefix the two sections of one `/metrics` payload held character-identical
    // field names — one of which had always moved and one of which never had.
    /// Slot-lock acquisitions granted.
    pub metadata_lock_acquisitions_successful: u64,
    /// Slot-lock acquisition ATTEMPTS refused. Retries count individually, so this is
    /// greater than or equal to `cache.metadata_lock_retry_exhausted_total`.
    pub metadata_lock_acquisitions_failed: u64,
    /// Cumulative milliseconds slot locks were held. A **sum**, matching the shape of
    /// `eviction_coordination.total_lock_hold_time_ms` so the two are at least
    /// dimensionally comparable; divide by `metadata_lock_acquisitions_successful` for
    /// a mean. Replaces `average_lock_hold_time_ms`, which was a mean over the last
    /// 1000 samples — and permanently zero, so no reader can have depended on it.
    pub metadata_lock_hold_time_ms_total: u64,
    /// Blocking slot-lock acquisitions that gave up because
    /// `cache.shared_storage.lock_acquire_wait_budget` elapsed.
    ///
    /// The failure signal for the time-bounded acquisition path, as
    /// `cache.metadata_lock_retry_exhausted_total` is for the retry-bounded one. Both are
    /// give-ups: neither the budget nor the retry count ever takes a lock a peer still holds.
    ///
    /// Non-zero means at least one request failed rather than waited. A sustained non-zero
    /// reading alongside a large `metadata_lock_hold_time_ms_total` per acquisition means the
    /// budget is short relative to how long critical sections actually take on this volume.
    pub metadata_lock_wait_budget_expired_total: u64,

    /// Cached range files whose LZ4 data failed to decode on a read for a client.
    /// See `disk_cache::range_file_corruption_detected`.
    pub range_file_corruption_detected: u64,
    /// Damaged range files a read invalidated, so the next read refetches the range.
    /// See `disk_cache::RangeHealLimiter`.
    pub range_file_corruption_healed: u64,
    /// Damaged range files left in place because this proxy's heal limit was reached.
    /// See `disk_cache::RANGE_HEAL_LIMIT`.
    pub range_file_corruption_heal_suppressed: u64,
    /// Cached range files a read refused because the file it opened is not the one its listing
    /// describes (inode or length differ), on a read for a client or for RAM promotion. Healed
    /// outside the heal limit. See `disk_cache::range_file_identity_mismatch_detected`.
    pub range_file_identity_mismatch_detected: u64,

    /// Range journal entries discarded because their range file never appeared within
    /// `stale_entry_timeout_secs`. See `journal_consolidator::journal_validation_failures`.
    pub journal_validation_failures: u64,
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
    /// Number of waiter conditional requests that returned `304 Not Modified`
    /// (IAM-validated cache serve). Added by the download-coordination TTL
    /// correctness fix.
    pub waiter_conditional_304: u64,
    /// Number of waiter conditional requests that returned `200 OK`
    /// (object changed mid-flight; waiter served S3's fresh body).
    pub waiter_conditional_200: u64,
    /// Number of waiter conditional requests that returned a `4xx`
    /// status (auth / client error; returned to client, cache preserved).
    pub waiter_conditional_4xx: u64,
    /// Number of waiter conditional requests that failed with a `5xx`
    /// from S3 or a transport error (degraded cache-serve fallback).
    pub waiter_conditional_error: u64,
}

/// Page-aligned range caching (widening) observability metrics.
/// Spec: page-aligned-range-cache. Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageCacheMetrics {
    /// Total requests widened into a page-aligned fetch (Requirement 8.1)
    pub widened_requests: u64,
    /// Bytes fetched beyond what the client requested, summed across all
    /// widened requests (Requirement 8.2)
    pub bytes_prefetched: u64,
    /// Amplification ratio derived from `bytes_prefetched`:
    /// `(bytes_requested + bytes_prefetched) / bytes_requested`, i.e. how many
    /// times larger the widened fetch was than what the client asked for.
    /// `1.0` when no widened bytes have been requested yet (Requirement 8.2)
    pub amplification_ratio: f64,
    /// Small_Reads served from an already-cached Page with no S3 fetch
    /// (Requirement 8.3)
    pub page_hits: u64,
    /// Requests declined widening because the Range was signed
    /// (Requirement 8.4)
    pub skipped_signed_range: u64,
    /// Widened/page fetches that failed and fell back to the client's
    /// original range (Requirement 8.5)
    pub fallbacks: u64,
    /// Pages successfully promoted to the RAM cache (Requirement 8.6)
    pub ram_page_promotions: u64,
    /// Pages not promoted to RAM because they exceeded the applicable RAM
    /// budget (Requirement 8.6, see Requirement 7.7)
    pub ram_page_promotion_skipped: u64,
}

/// System-wide metrics
/// Journal consolidation metrics
/// Requirements: 3.3, 3.4, 3.5 - Dashboard and metrics integration
///
/// `Default` exists for `src/otlp.rs`'s field guard, which serialises a default instance to read
/// the wire shape; no production path constructs one from it.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConsolidationMetrics {
    /// Total cache size in bytes from consolidator
    pub cache_size_bytes: u64,
    /// Write cache size in bytes from consolidator
    pub cache_write_size_bytes: u64,
    /// Number of consolidation cycles completed
    pub consolidation_count: u64,
    /// Unix timestamp of last consolidation (seconds since epoch)
    pub last_consolidation_timestamp: u64,
    /// Seconds this instance has held the global consolidation lock, 0 when it does not hold it.
    /// Per-instance, unlike the size-state figures above.
    pub consolidation_lock_held_secs: u64,
    /// Times this instance's lock watchdog released a consolidation lock held past its threshold,
    /// since process start. Per-instance.
    pub consolidation_lock_takeovers: u64,
}

/// Cache rules reload health metrics.
/// Tracks the lifecycle of `cache_rules.json` hot-reloads so operators can alert
/// on a stuck fallback or silent parse failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheRulesMetrics {
    /// Total successful rule-file loads since startup.
    pub reloads_total: u64,
    /// Total rule-file load failures (parse, validation, compile) since startup.
    pub reload_failures_total: u64,
    /// Whether the running ruleset is a stale fallback (last load failed).
    pub on_fallback: bool,
    /// Number of rules currently loaded.
    pub rules_loaded: u64,
    /// Unix timestamp (seconds) of the last successful load.
    pub last_load_unix: u64,
}

/// Cache-hit metadata update buffer: what the journal write-behind is holding, what it has
/// written, and what it has lost.
///
/// # What the buffer is
///
/// Every cache hit that refreshes an object's TTL or bumps a range's access count goes into
/// `CacheHitUpdateBuffer`, a RAM buffer drained to this instance's journal file by a 5 s
/// background task (`main.rs`). The consolidator then folds those journal lines into the shared
/// `.meta`. The point is I/O reduction — a million hits in one interval collapse to one append —
/// and the cost is that a hit's effect on the metadata is asynchronous.
///
/// # Why these four were unobservable
///
/// The buffer computed all of them and published none. `updates_recorded` and `entries_flushed`
/// were live atomics reachable only through `get_stats()`, which had no production caller;
/// `buffer_len()` likewise; and `FlushResult`'s per-flush `errors` was returned to a `debug!`
/// line and dropped. `docs/CACHE_INTERNALS.md` documented six `batch_flush_*` fields against
/// declaration-only `CacheStatistics` members that no endpoint ever emitted, and that block was
/// deleted in the previous release cycle — which left the capability looking decided against when
/// it was only unwired.
///
/// # Three different shapes, named as such
///
/// `pending_updates` is a **gauge** (a queue depth, sampled at collection time). The three
/// `*_total` fields are **cumulative counters** since process start. Conflating the two is how a
/// metric becomes uninterpretable, so the suffix carries the distinction on the wire.
///
/// # `null` rather than zeroes when there is no buffer
///
/// The section is `None` when no cache manager is wired into the metrics manager, matching
/// `consolidation`'s meaning. A zeroed section would be indistinguishable from a wired, idle
/// buffer — which is the exact failure this section exists to remove, one level up.
///
/// # Exported over OTLP
///
/// All five fields have `cache_hit_buffer.*` instruments in `src/otlp.rs`, held there by the
/// `every_cache_hit_buffer_field_has_an_otlp_instrument` guard, so an OTLP-only deployment can
/// alert on `flush_errors_total`. The exporter records nothing while this section is `None`, so
/// the series carry a **gap** rather than five zeroes when no cache manager is wired in — five
/// zeroes mean wired and idle. Every other top-level section is exported too, except
/// `bucket_traffic`'s `bytes_saved`: the other four per-bucket figures are OTLP counters behind
/// `metrics.otlp.per_bucket_enabled`, and 21 `atomic_metadata` fields that are always zero.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CacheHitBufferMetrics {
    /// Distinct entries sitting in the RAM buffer at collection time, not yet appended to the
    /// journal. Each entry is one journal line at the next flush and stands for one or more
    /// recorded updates, because repeated hits on one stored range inside a flush window fold
    /// into one entry. This figure counts entries; `updates_recorded_total` and
    /// `entries_flushed_total` count updates.
    ///
    /// The `pending_disk_updates` figure the deleted documentation named. Zero is a normal and
    /// meaningful reading — an empty queue — and is distinguishable from "not wired" because the
    /// whole section is `null` in that case. A value that stays near
    /// `MAX_BUFFER_ENTRIES` (10 000) means the buffer is reaching its cap between ticks and
    /// flushing inline on the request path.
    pub pending_updates: u64,
    /// Cache-hit updates accepted into the buffer since process start.
    ///
    /// Moves on any cache-hit traffic: `DiskCacheManager::refresh_object_ttl` (three
    /// `http_proxy` call sites) and `record_range_access`/`update_range_access` (one
    /// `range_handler` and one `http_proxy` call site) are the producers.
    pub updates_recorded_total: u64,
    /// Recorded updates successfully appended to this instance's journal since process start.
    /// A coalesced journal line counts every update folded into it, so this compares directly
    /// with `updates_recorded_total` and not with the number of journal lines written.
    ///
    /// Read against `updates_recorded_total`: the difference is what is buffered or lost. A
    /// counter that stops advancing while `updates_recorded_total` climbs means the flush task
    /// is no longer running.
    pub entries_flushed_total: u64,
    /// Cache-hit updates a flush failed to write since process start (a failed coalesced line
    /// counts every update folded into it): a serialisation failure, or a journal file that
    /// could not be opened or written.
    ///
    /// The one figure the deleted documentation told operators to alert on, and it is genuinely
    /// load-bearing: these entries carry the TTL refreshes and access counts that eviction ranks
    /// candidates by, so losing them silently skews eviction toward objects that are in fact
    /// hot. Non-zero warrants checking the cache volume's free space and permissions.
    pub flush_errors_total: u64,
    /// RAM-cache hits whose access record was dropped since process start because the bounded
    /// queue feeding this buffer was full (`cache::RAM_ACCESS_QUEUE_CAPACITY`, 4096 records).
    ///
    /// Each drop is one access count lost for the stored ranges that hit was served from, so
    /// eviction ranks those ranges as slightly colder than they are. Zero is the normal reading;
    /// a climbing figure means RAM hits are arriving faster than one task can record them.
    pub ram_records_dropped_total: u64,
}

/// The fill-visibility tracker's figures (`src/fill_visibility_tracker.rs`).
///
/// **Per-instance**, like every counter here: each proxy tracks only its own fills, so a fleet
/// reading must be taken from the proxy that did the fill.
///
/// `entries` and `bytes` are **gauges** sampled at collection time (aged entries are swept first,
/// so they are not counted). Every `*_total` is a **cumulative counter** since process start.
///
/// # `null` rather than zeroes
///
/// The section is `None` when no cache manager is wired into the metrics manager, matching
/// `cache_hit_buffer`. With a cache manager the tracker always exists, so zeroes mean wired and
/// idle — for example a proxy that has served only buffered fills, which write their `.meta`
/// directly and never need the tracker.
///
/// # Reading it
///
/// `recorded_total` minus the drop counters minus `entries` is zero; every entry leaves by exactly
/// one reason. On a healthy proxy nearly all leave as `dropped_folded_total` or
/// `dropped_disk_meta_total` within one consolidation interval. A rising `dropped_age_total` means
/// consolidation is slower than the tracker's age limit (`max(4 × consolidation_interval, 30 s)`);
/// a rising `dropped_cap_total` means more fills per interval than the cap (8,192 entries,
/// 16 MiB). Both cost only cache hits, never correctness.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct FillVisibilityMetrics {
    /// Entries held now.
    pub entries: u64,
    /// Estimated bytes held now, against the 16 MiB cap.
    pub bytes: u64,
    /// Fills recorded since process start.
    pub recorded_total: u64,
    /// Responses served from an entry's record since process start, one per response: a live-TTL
    /// `Fresh` verdict issued from it, or a Mode B `If-Match` or post-`304` serve of it. Counted
    /// at the authorising step, so a serve that then fails and falls back to S3 still counts.
    /// A HIT served from a `.meta` another proxy folded is not counted, and is not the tracker's.
    pub hits_total: u64,
    /// Entries dropped because a lookup found a `.meta` for the key (disk wins). A HEAD-only
    /// `.meta` of the same version as the tracked fill does not drop it.
    pub dropped_disk_meta_total: u64,
    /// Entries dropped because this instance's consolidator wrote the key's `.meta`.
    pub dropped_folded_total: u64,
    /// Entries dropped because this instance invalidated the key (PUT, DELETE, stale object).
    pub dropped_invalidated_total: u64,
    /// Entries dropped, oldest first, because the entry or byte cap was reached.
    pub dropped_cap_total: u64,
    /// Entries dropped because they outlived the age limit.
    pub dropped_age_total: u64,
    /// Entries dropped because the committed `.bin` was missing or rewritten at lookup.
    pub dropped_range_file_changed_total: u64,
}

impl From<crate::fill_visibility_tracker::FillVisibilityStats> for FillVisibilityMetrics {
    fn from(s: crate::fill_visibility_tracker::FillVisibilityStats) -> Self {
        Self {
            entries: s.entries,
            bytes: s.bytes,
            recorded_total: s.recorded_total,
            hits_total: s.hits_total,
            dropped_disk_meta_total: s.dropped_disk_meta_total,
            dropped_folded_total: s.dropped_folded_total,
            dropped_invalidated_total: s.dropped_invalidated_total,
            dropped_cap_total: s.dropped_cap_total,
            dropped_age_total: s.dropped_age_total,
            dropped_range_file_changed_total: s.dropped_range_file_changed_total,
        }
    }
}

/// Candidate-specific journal-evidence outcomes of the orphan rotation (R15.11), one count per
/// evidence read taken under a candidate's slot lock. Published on OTLP as one
/// `orphan_reclaim.journal_evidence_total` series per `result`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct OrphanJournalEvidenceTotals {
    /// Every journal was read in full and none holds an `Add` for the extent. The only outcome
    /// that permits an unlink.
    pub confirmed_absent: u64,
    /// Some journal holds an `Add` for the extent; the file is kept.
    pub journal_present: u64,
    /// A listed journal could not be opened or read; the file is kept.
    pub peer_unavailable: u64,
    /// The journal listing failed, a line was torn, or a `.journal.tmp` was present; the file is
    /// kept.
    pub indeterminate: u64,
}

/// The orphan range-file rotation (R15.11). It always runs once cache init has started it.
///
/// Every figure is per process since start and live on every request (module atomics), except
/// the two rotation gauges, which are read from the checkpoint set at the end of this process's
/// last pass. Orphan bytes were never in the accounted total, so `bytes_reclaimed` is not a debit
/// of `cache.total_cache_size`; it is the volume-versus-accounted gap this process has closed.
///
/// # Reading it
///
/// A rising `found` with a flat `reclaimed` and a rising `skipped_unconfirmed` means evidence is
/// failing closed, unless the candidates are too new, have a multipart upload in progress, or were
/// replaced while being checked: read `journal_evidence_total` for which outcome.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct OrphanReclaimMetrics {
    /// Whether this process runs the rotation. `false` (the default) means every other figure
    /// here is a true zero rather than an idle one.
    pub enabled: bool,
    /// Candidates found: range files the key's `.meta` did not list when their leaf was scanned.
    pub found: u64,
    /// Candidates unlinked after `confirmed_absent` evidence.
    pub reclaimed: u64,
    /// On-disk bytes of the unlinked candidates.
    pub bytes_reclaimed: u64,
    /// Candidates kept on unconfirmed evidence (`peer_unavailable`, `indeterminate`), an
    /// unrecoverable cache key, an unreadable `.meta`, or an I/O error. A candidate is also kept
    /// when it is too new, when its object has a multipart upload in progress, or when it was
    /// replaced while being checked.
    pub skipped_unconfirmed: u64,
    /// Candidates kept because the `.meta` listed them by the time the slot lock was held.
    pub kept_listed: u64,
    /// Journal-evidence outcomes, one per evidence read.
    pub journal_evidence_total: OrphanJournalEvidenceTotals,
    /// Passes this process has run.
    pub passes: u64,
    /// Units (a `(bucket, L1)` shard, or the flat `ranges/` root) this process claimed and
    /// visited.
    pub units_visited: u64,
    /// Scheduled passes skipped, or stopped before a unit, because some active proxy on the
    /// volume is older than 3.0.0 or the roster could not be read (fix 1).
    #[serde(default)]
    pub passes_gated: u64,
    /// Reads of a shard lease file (orphan claims, eviction work-list chunk leases) that did
    /// not parse (fix 3).
    #[serde(default)]
    pub leases_unreadable: u64,
    /// Of those, files older than `orphan_reclaim::UNREADABLE_LEASE_EXPIRE_SECS`: read as
    /// expired and removed.
    #[serde(default)]
    pub leases_unreadable_expired: u64,
    /// Seconds since the oldest `scanned_at` among units that have one, as of the end of this
    /// process's last pass and advanced to request time. `null` before the first pass or while
    /// no unit has completed a visit.
    pub stalest_checkpoint_age_secs: Option<u64>,
    /// Units with no `scanned_at` at the end of the last pass: not yet reached by any instance's
    /// rotation. `null` before the first pass.
    pub units_never_scanned: Option<u64>,
}

impl OrphanReclaimMetrics {
    /// The section as of now, from the rotation's per-process counters.
    pub fn current() -> Self {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self::from_counters(
            crate::orphan_reclaim::counters(),
            crate::orphan_reclaim::enabled(),
            now,
        )
    }

    /// Build the section from a counter snapshot, ageing the stalest checkpoint to `now_secs`.
    pub fn from_counters(
        c: crate::orphan_reclaim::OrphanReclaimCounters,
        enabled: bool,
        now_secs: u64,
    ) -> Self {
        Self {
            enabled,
            found: c.found,
            reclaimed: c.reclaimed,
            bytes_reclaimed: c.bytes_reclaimed,
            skipped_unconfirmed: c.skipped_unconfirmed,
            kept_listed: c.kept_listed,
            journal_evidence_total: OrphanJournalEvidenceTotals {
                confirmed_absent: c.evidence_confirmed_absent,
                journal_present: c.evidence_journal_present,
                peer_unavailable: c.evidence_peer_unavailable,
                indeterminate: c.evidence_indeterminate,
            },
            passes: c.passes,
            units_visited: c.units_visited,
            passes_gated: c.passes_gated,
            leases_unreadable: c.leases_unreadable,
            leases_unreadable_expired: c.leases_unreadable_expired,
            stalest_checkpoint_age_secs: c.stalest_scanned_at.map(|at| now_secs.saturating_sub(at)),
            units_never_scanned: c.units_never_scanned,
        }
    }
}

/// How this process's executor's claims of work-list chunk leases ended. Published on OTLP as
/// one `eviction_worklist.chunks_claimed_total` series per `outcome`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct EvictionWorklistClaimTotals {
    /// The lease was claimed, fresh or taken over from a lapsed lease.
    pub claimed: u64,
    /// The claim was refused: a live peer holds the lease, or the lease file could not be read
    /// or written.
    pub busy: u64,
}

/// Work-list items this process's executor did not execute, by reason. Published on OTLP as one
/// `eviction_worklist.items_refused_total` series per `reason`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct EvictionWorklistRefusedTotals {
    /// The object's `.meta` no longer matched the identity the author recorded: it was
    /// rewritten after the list was written, or a listed range became staged. Nothing of it was
    /// deleted or debited. An object whose `.meta` has been REMOVED since authoring is not one
    /// of these — the delete finds nothing to mismatch, so its items count as executed.
    pub identity_changed: u64,
    /// A newer generation was published before the executor reached the item.
    pub stale_generation: u64,
}

/// Author ticks on which this process held the eviction lock and ran the legacy pass instead of
/// authoring, by the capability gate's answer. Published on OTLP as one
/// `eviction_worklist.author_fallback_total` series per `reason`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct EvictionWorklistFallbackTotals {
    /// Some active proxy has no current `.ready` record with the work-list flag (an older binary).
    pub not_capable: u64,
    /// The gate could not establish the roster or a record.
    pub unknown: u64,
}

/// The eviction work list as this process sees it: per process since start, for this process's
/// cache directory, except `generation` and `in_flight_bytes`, which are gauges.
///
/// # Reading it
///
/// On the eviction-lock holder, a rising `author_fallback_total` means eviction is running the
/// legacy pass. On any proxy, `chunks_claimed_total.claimed` rising while `chunks_done_total`
/// stays flat means claimed chunks are not finishing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct EvictionWorklistMetrics {
    /// The newest work-list generation this process has observed; 0 before any.
    pub generation: u64,
    /// Chunks this process authored (counted once their generation document is written).
    pub chunks_authored_total: u64,
    /// This executor's chunk-lease claims, by outcome.
    pub chunks_claimed_total: EvictionWorklistClaimTotals,
    /// Chunks this executor closed with a done-marker.
    pub chunks_done_total: u64,
    /// Chunks that read torn or absent and were treated as absent.
    pub chunks_unusable_total: u64,
    /// Items of objects this executor ran the identity-checked delete for.
    pub items_executed_total: u64,
    /// Items this executor did not execute, by reason.
    pub items_refused_total: EvictionWorklistRefusedTotals,
    /// Claims that replaced a lapsed lease.
    pub lease_takeovers_total: u64,
    /// Author ticks that ran the legacy pass instead, by reason.
    pub author_fallback_total: EvictionWorklistFallbackTotals,
    /// Listed bytes not yet executed, as this process's last author call read them.
    pub in_flight_bytes: u64,
}

impl EvictionWorklistMetrics {
    /// The section as of now for `cache_dir`.
    pub fn current(cache_dir: &std::path::Path) -> Self {
        Self::from_counters(
            crate::eviction_worklist::counters(cache_dir),
            crate::eviction_worklist::generation_latched_for(cache_dir),
        )
    }

    pub(crate) fn from_counters(
        c: crate::eviction_worklist::WorklistCounters,
        generation: u64,
    ) -> Self {
        Self {
            generation,
            chunks_authored_total: c.chunks_authored,
            chunks_claimed_total: EvictionWorklistClaimTotals {
                claimed: c.chunks_claimed,
                busy: c.chunks_claim_busy,
            },
            chunks_done_total: c.chunks_done,
            chunks_unusable_total: c.chunks_unusable,
            items_executed_total: c.items_executed,
            items_refused_total: EvictionWorklistRefusedTotals {
                identity_changed: c.items_refused_identity_changed,
                stale_generation: c.items_refused_stale_generation,
            },
            lease_takeovers_total: c.lease_takeovers,
            author_fallback_total: EvictionWorklistFallbackTotals {
                not_capable: c.author_fallback_not_capable,
                unknown: c.author_fallback_unknown,
            },
            in_flight_bytes: c.in_flight_bytes,
        }
    }
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
    /// `None` (JSON `null`) when no consolidator is wired in, or when `size_state.json` exists
    /// but could not be read or parsed; never a section of zeros standing in for a failed read.
    pub consolidation: Option<ConsolidationMetrics>,
    /// Cache-hit metadata update buffer (journal write-behind) observability.
    /// `None` when no cache manager is wired in; see [`CacheHitBufferMetrics`].
    pub cache_hit_buffer: Option<CacheHitBufferMetrics>,
    /// The fill-visibility tracker: streamed GET-miss fills this instance can serve before the
    /// consolidator writes their `.meta`. `None` (JSON `null`) when no cache manager is wired in;
    /// see [`FillVisibilityMetrics`]. `#[serde(default)]` so a body from a build without this
    /// section still deserialises.
    #[serde(default)]
    pub fill_visibility: Option<FillVisibilityMetrics>,
    /// The orphan range-file rotation. Always present: with the rotation disabled it reports
    /// `enabled: false` and zeroes. `#[serde(default)]` so a body from a build without this
    /// section still deserialises. See [`OrphanReclaimMetrics`].
    #[serde(default)]
    pub orphan_reclaim: OrphanReclaimMetrics,
    /// The eviction work list's per-process counters. Always present: zeroes without a cache
    /// manager or before any work-list event. `#[serde(default)]` so a body from a build without
    /// this section still deserialises. See [`EvictionWorklistMetrics`].
    #[serde(default)]
    pub eviction_worklist: EvictionWorklistMetrics,
    pub coalescing: Option<CoalescingMetrics>,
    pub cache_rules: Option<CacheRulesMetrics>,
    /// Page-aligned range caching (widening) metrics.
    /// Spec: page-aligned-range-cache. Requirements: 8.1-8.6
    pub page_cache: PageCacheMetrics,
    pub request_metrics: RequestMetrics,
    /// Per-bucket (or per-bucket+prefix) traffic counters.
    /// Key: `"bucket"` (no prefix) or `"bucket/prefix"` (prefix attribution active).
    /// Always present; empty map when no traffic has been recorded.
    /// Spec: per-bucket-metrics. Requirements: 3.1, 3.3
    pub bucket_traffic: HashMap<String, BucketTrafficStats>,
    /// Download bandwidth QoS metrics.  Present when the feature is compiled in;
    /// all sub-fields reflect the current state whether enabled or not.
    /// Spec: download-bandwidth-qos. Requirements: 8.1, 8.2, 8.3, 8.4, 8.5
    pub download_bandwidth: crate::bandwidth_limiter::BandwidthLimiterSnapshot,
    /// Hedged upstream request metrics.
    /// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
    pub hedged_requests: HedgingStats,
    /// In-flight buffered-byte ledger metrics. Present regardless of whether the
    /// ceiling is configured (`ceiling_bytes: 0` reports Ledger_Disabled) so an
    /// operator can confirm the feature is off from `/metrics` alone.
    /// Spec: inflight-memory-accounting. Requirements: 8.1-8.7
    pub inflight_memory: InflightMemoryMetrics,
    /// Write-cache (staging tier) observability: Resident_Bytes vs the
    /// Staging_Bound, Inflight_Bytes, and staged-entry count. Always present
    /// (zeroed when write caching or the cache manager is not wired in).
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.2
    pub write_cache: WriteCacheMetrics,
    /// The Write_Ledger append stages' batching counters. Always present: zeroes before the
    /// first append. `#[serde(default)]` so a body from a build without this section still
    /// deserialises. See [`crate::write_ledger::WriteLedgerMetrics`].
    #[serde(default)]
    pub write_ledger: crate::write_ledger::WriteLedgerMetrics,
    /// What the most recent background collection cost. Always present.
    pub metrics_collection: MetricsCollectionGauges,
    /// When the shared-volume figures in this response were collected. Always present; its
    /// fields are `null` until the first background collection has completed.
    ///
    /// `#[serde(default)]` so a body produced by a build without this section still
    /// deserialises.
    #[serde(default)]
    pub volume_snapshot: VolumeSnapshotGauges,
}

/// When the shared-volume-derived figures in a `/metrics` response were collected.
///
/// # Which figures are background-derived, and why only those
///
/// Every section of `/metrics` is assembled when the request arrives **except** the figures whose
/// only source is a traversal of the shared cache volume — a walk or a per-shard document read
/// whose cost scales with the shard or object population. Those are produced by the background
/// collector every `metrics.collection_interval`, and a request serves the latest result:
///
/// - `write_cache.staged_fold_*` / `staged_*` figures derived from the per-shard checkpoint set
///   (`staged_fold_bytes`, `_byte_sum`, `_pending_bytes`, `_in_progress_part_bytes`, `_gap_bytes`,
///   `staged_authority_age_secs`, `staged_shard_authority_age_secs`, `staged_fold_shards`,
///   `staged_fold_shards_beyond_max_age`, `staged_range_count`, `staged_mutations_awaiting_fold`);
/// - the six volume-derived `write_cache.ledger_repair_*` figures (`rotation_complete`,
///   `stalest_age_secs`, `observed_shards`, `swept_shards`, `rotation_reachable`,
///   `projected_rotation_secs`);
/// - `write_cache.ledger_entries` and `ledger_entries_age_secs`;
/// - `cache.st_size_fold_oldest_shard_scan_age_secs` (flattened into `cache`);
/// - `cache.in_progress_part_bytes` and `cache.in_progress_part_bytes_age_secs`, from one walk of
///   `mpus_in_progress/` per collection that also supplies `staged_fold_in_progress_part_bytes`,
///   and `cache.in_progress_part_bytes_drift_bytes`, that walk reconciled against the per-proxy
///   published figures.
///   Its cost is one listing per upload and one read per part record, and clients set how many
///   there are, up to the multipart write-cache limit.
///
/// Everything else — atomics, in-memory statistics, and single small-file reads such as
/// `size_state.json` — stays live on every request. That is not a matter of taste: the fleet
/// verification suite reads counters immediately after the action that moves them, and a counter
/// served from a snapshot up to one interval old would report "did not move" for a reason that
/// has nothing to do with the product.
///
/// # Ages keep counting
///
/// The age fields among the background-derived figures (`staged_authority_age_secs`,
/// `staged_shard_authority_age_secs`, `ledger_repair_stalest_age_secs`, `ledger_entries_age_secs`,
/// `st_size_fold_oldest_shard_scan_age_secs`, `in_progress_part_bytes_age_secs`) are advanced by `age_secs` at request time, so each
/// states the age of the data actually on display. A stalled collector therefore shows as ages
/// that keep rising rather than as figures that silently stop moving.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VolumeSnapshotGauges {
    /// Unix seconds at which the background collection whose figures this response carries
    /// finished. `null` before the first one has completed: the background-derived figures are
    /// then at their UNREADABLE sentinels, never at a zero.
    pub collected_at_unix_secs: Option<u64>,
    /// Seconds since `collected_at_unix_secs`. `null` exactly when that is. In the steady state
    /// this stays below `interval_secs` plus one collection's duration; a larger figure means the
    /// collector has stalled or its runs are overrunning their slot.
    pub age_secs: Option<u64>,
    /// The collection interval in force, from `metrics.collection_interval` after clamping.
    pub interval_secs: u64,
}

/// Cost and cadence of the full metrics collection itself.
///
/// # Why this exists at all
///
/// Nothing in the process counted a collection, timed one, or logged a slow one. The only evidence
/// that collection had become expensive was two fleet-suite assertions losing their readings
/// (the fleet test, 2026-09-21) — an external observer inferring an internal cost, with no way to
/// tell a slow endpoint from a broken one, a non-2xx, or a deliberately-published `null`. Those
/// have different owners and different severities.
///
/// The cost is mostly shared-volume I/O — one open and parse per shard document across
/// `checkpoints/` and `repair/`, plus the write-ledger count — so it scales with the volume rather
/// than with anything the operator can see locally.
///
/// # These describe the most recent BACKGROUND collection
///
/// A full collection runs on the background collector every `metrics.collection_interval`, and a
/// `/metrics` request no longer performs one: it assembles its in-process sections live and serves
/// the shared-volume figures from the latest background run (see [`VolumeSnapshotGauges`]). So
/// these figures describe that background run, not the request that returned them, and
/// `collections_total` counts background runs (plus any direct `collect_metrics` call), never
/// HTTP scrapes. `volume_snapshot.collected_at_unix_secs` says when the run described here ended.
///
/// A slow collection therefore costs **staleness**, not the response: the request still answers
/// immediately, with figures whose age is published beside them.
///
/// The OTLP export runs at the end of each background collection, so OTLP and `/metrics` now see
/// the same collections at the same cadence.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MetricsCollectionGauges {
    /// Full collections since process start. See [`COLLECTIONS_TOTAL`] for why this counts
    /// collections rather than HTTP scrapes.
    pub collections_total: u64,
    /// Wall-clock milliseconds the most recent full collection took.
    pub duration_ms_last: u64,
    /// High-water mark since process start. Read it beside `duration_ms_last`: one slow run is
    /// visible here for the life of the process, after `duration_ms_last` has moved on.
    pub duration_ms_max: u64,
    /// Collections exceeding `slow_threshold_ms`, since process start.
    ///
    /// The threshold is half the collection interval (see [`collection_slow_threshold_ms`]), so
    /// non-zero means some run used more than half of its slot — one doubling of its cost away from
    /// overrunning the slot entirely and skipping a tick, at which point the published figures are
    /// two intervals old.
    pub slow_collections_total: u64,
    /// The threshold the counter above is measured against, in milliseconds.
    ///
    /// Published so an operator can confirm from outside the process which threshold is in force,
    /// rather than inferring it — the same reason `inflight_memory.ceiling_bytes` and
    /// `tail_floor_bytes` are published. Derived from the configured `metrics.collection_interval`,
    /// so it changes when that does and is not a separate knob.
    pub slow_threshold_ms: u64,
}

/// In-flight buffered-byte ledger statistics.
///
/// Exposed as `inflight_memory.{reserved_bytes,ceiling_bytes,peak_reserved_bytes,
/// rejected_total,aborted_accumulations_total,ram_cache_max_bytes,
/// buffered_serve_sliced_streams_total, buffered_serve_degraded_<reason>_total}` in the
/// `/metrics` JSON.
///
/// **Relationship to `request_metrics.rejected_requests`**: `rejected_requests`
/// is the top-level counter incremented for *every* Shed_Response, regardless of
/// cause — it currently counts both `max_concurrent_requests` exhaustion and
/// in-flight ceiling exhaustion. `rejected_total` here is the subset of those
/// sheds specifically caused by the in-flight ceiling. An operator comparing the
/// two should expect `rejected_total <= request_metrics.rejected_requests`, with
/// the difference attributable to concurrency-limit sheds; the same relationship
/// the Phase C permit-metrics work already had to make explicit for
/// `permits_held`/`permits_held_peak` vs `rejected_requests`.
///
/// `ram_cache_max_bytes` is included alongside so an operator can compare the two
/// independent memory budgets side by side (Requirement 8.7) without a second
/// `/metrics` field lookup or a separate config read.
///
/// Spec: inflight-memory-accounting. Requirements: 8.1-8.7
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InflightMemoryMetrics {
    /// Current total bytes reserved across all live Buffering_Site reservations.
    /// Requirement: 8.1
    pub reserved_bytes: u64,
    /// Configured Inflight_Ceiling in bytes. `0` means Ledger_Disabled. Reported
    /// even while disabled so an operator can confirm the feature is off.
    /// Requirements: 8.2, 8.6
    pub ceiling_bytes: u64,
    /// High-water mark of `reserved_bytes` observed since process start.
    /// Requirement: 8.3
    pub peak_reserved_bytes: u64,
    /// Cumulative count of Admission_Check rejections. Requirement: 8.4
    pub rejected_total: u64,
    /// Cumulative count of Unknown_Size_Site accumulations aborted mid-stream
    /// because growing the reservation would have exceeded the ceiling.
    /// Requirement: 8.5
    pub aborted_accumulations_total: u64,
    /// Cumulative ledger grants: each admitted reservation and each successful growth of one.
    /// Monotonic, so a reservation held too briefly to appear in any sample of
    /// `reserved_bytes` still moves it. `0` while Ledger_Disabled, which accounts nothing.
    #[serde(default)]
    pub granted_total: u64,
    /// Cumulative bytes granted by those grants.
    #[serde(default)]
    pub granted_bytes_total: u64,
    /// Configured `cache.max_ram_cache_size`, reported alongside the ledger so
    /// the two independent memory budgets are directly comparable.
    /// Requirement: 8.7
    pub ram_cache_max_bytes: u64,
    /// Buffered-serve requests answered by streaming a slice of one contained cached
    /// extent instead of loading the whole extent into memory. Per-instance, since
    /// process start. Counted independently of the ledger, so it moves with
    /// `ceiling_bytes: 0`. See `http_proxy::BUFFERED_OPERATION_BOUND`.
    /// Spec: buffered-serve-memory-bound R1.4.
    #[serde(default)]
    pub buffered_serve_sliced_streams_total: u64,
    // Buffered-serve degrades, one counter per reason (buffered-serve-memory-bound
    // R6.2): a cached range that was NOT materialised into one buffer because the range
    // or an extent exceeded `http_proxy::BUFFERED_OPERATION_BOUND`. `chained_stream` was
    // served from cache by chaining slice streams; every `forward_*` and
    // `merge_backstop` was forwarded to S3 uncached. Per-instance, since process start.
    #[serde(default)]
    pub buffered_serve_degraded_chained_stream_total: u64,
    #[serde(default)]
    pub buffered_serve_degraded_forward_gap_total: u64,
    #[serde(default)]
    pub buffered_serve_degraded_forward_partial_total: u64,
    #[serde(default)]
    pub buffered_serve_degraded_forward_unresolved_total: u64,
    #[serde(default)]
    pub buffered_serve_degraded_forward_stream_failed_total: u64,
    #[serde(default)]
    pub buffered_serve_degraded_forward_partial_cache_merge_total: u64,
    #[serde(default)]
    pub buffered_serve_degraded_merge_backstop_total: u64,
    // Buffered serves by materialised size (buffered-serve-memory-bound R6.1), disjoint
    // buckets. `gt_8mib` should stay 0: every buffered serve is gated to the 8 MiB bound.
    #[serde(default)]
    pub buffered_serve_size_le_64kib_total: u64,
    #[serde(default)]
    pub buffered_serve_size_le_1mib_total: u64,
    #[serde(default)]
    pub buffered_serve_size_le_8mib_total: u64,
    #[serde(default)]
    pub buffered_serve_size_gt_8mib_total: u64,
    /// Largest single buffered-serve materialisation since process start, in bytes
    /// (buffered-serve-memory-bound R6.3). A high-water mark.
    ///
    /// Scope, for this field and the four buckets above: BOTH buffered serve paths, the
    /// ranged one and the no-`Range` whole-object one. See
    /// `http_proxy::note_buffered_serve_materialised` for why one gauge covers both.
    #[serde(default)]
    pub buffered_serve_peak_materialised_bytes: u64,
    /// How many of the bucketed buffered serves came from the no-`Range` whole-object
    /// path rather than the ranged one. Per-instance, since process start.
    #[serde(default)]
    pub buffered_serve_full_object_total: u64,
}

/// Write-cache (staging tier) observability metrics.
///
/// Exposed as `write_cache.{resident_bytes,staging_bound_bytes,inflight_bytes,
/// over_bound,staged_entries,staging_evictions_total,
/// staging_eviction_bytes_total,graduations_total,staged_authority_epoch,
/// graduation_lock_unavailable_total,graduation_noop_entries_total,
/// journal_unparseable_lines_retained,staged_authority_foreign_write_total,staged_authority_activation_blocker,
/// staged_authority_activation_blocking_instance,
/// staged_authority_activation_blocking_instance_count}` in the `/metrics` JSON.
///
/// - `resident_bytes`: Resident_Bytes — the staged figure the Staging_Bound is enforced
///   against, and the same fleet-wide figure `cache.write_cache_size` reports.
///   **Its DERIVATION depends on `staged_authority_epoch`** (cache-eviction-at-scale
///   § 16.6.2): at epoch `0` it is `SizeState::write_cache_size`, the event-sourced
///   incumbent, exactly as before; at epoch `1` it is recomputed from verified `.meta`
///   ground truth as `F` + post-cut pending credits + in-progress part bytes. Read the
///   epoch before comparing this figure across proxies or across time — expect a one-time
///   step at activation to the fold-derived figure. It can go up or down: the difference is
///   the incumbent counter's accumulated error, not data loss and not new data.
///
///   This sentence was true of the design and false of the code for one unit: the gauge read
///   the incumbent unconditionally while `cache.write_cache_size` resolved through the epoch,
///   so after activation the two diverged permanently and `over_bound` kept alarming on the
///   retired producer. Both now resolve through `CacheManager::staged_authority`, which is the
///   single resolution point, so they cannot disagree by construction rather than by agreement
///   between two call sites.
/// - `staging_bound_bytes`: the configured Staging_Bound
///   (`max_cache_size * write_cache_percent`), i.e.
///   `CacheManager::get_write_cache_capacity()`.
/// - `inflight_bytes`: Inflight_Bytes — this instance's current in-flight
///   `WriteReservation` total (`WriteCacheManager::current_usage()`),
///   per-instance and not shared.
/// - `over_bound`: `resident_bytes > staging_bound_bytes`, derived and exposed
///   explicitly so the condition needs no arithmetic to detect.
/// - `staged_entries`: a live (approximate) count of currently staged
///   (write-cached, ungraduated) entries.
/// - `graduations_total`: cumulative entries that left the staging tier by being
///   read (per-instance). Read it **with** `staged_entries`: a flat gauge alone
///   cannot distinguish an idle proxy from a broken graduation path, and
///   graduation is the mechanism by which the tier is supposed to drain. Together
///   with `staging_evictions_total` it accounts for the two ways an entry leaves
///   the tier — this one leaves the bytes on disk (they move to the read tier),
///   the other reclaims them. Not the accounting authority: the authoritative
///   decrement is applied to Size_State by the `Graduation` journal entry under
///   the consolidation lock, so two proxies racing the same key can both count
///   here while only one decrement lands.
/// - `staging_evictions_total` / `staging_eviction_bytes_total`: cumulative
///   objects evicted / bytes freed by staging (write-cache) eviction,
///   per-instance (`WriteCacheManager::evict_write_cached_object`).
///   Deliberately separate from `cache.evictions`, which reflects read-cache
///   eviction only and reports 0 regardless of how much staging eviction has
///   run — before this field existed, `cache.evictions: 0` was actively
///   misleading evidence that the write tier had never evicted anything.
///
/// Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.4
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WriteCacheMetrics {
    pub resident_bytes: u64,
    pub staging_bound_bytes: u64,
    pub inflight_bytes: u64,
    pub over_bound: bool,
    pub staged_entries: u64,
    pub staging_evictions_total: u64,
    pub staging_eviction_bytes_total: u64,
    pub graduations_total: u64,
    /// Streamed PUT/`UploadPart` write-cache setups (sink open, capacity reservation,
    /// `.tmp` creation, TTL lookup) abandoned because they did not finish within
    /// `WRITE_CACHE_SETUP_BOUND` (5 s), process-wide (`AtomicU64` in
    /// `crate::signed_put_handler`). **Per-instance.** Each one is an upload forwarded to
    /// S3 uncached because the cache volume was slow before the body read began. The
    /// in-body counterpart is `cache.tee_abandoned_total_put`.
    #[serde(default)]
    pub setup_abandoned_total_put: u64,
    /// `CompleteMultipartUpload`s left uncached because a Write_Ledger append failed before
    /// the `.meta` publish, process-wide (`AtomicU64` in `crate::signed_put_handler`).
    /// **Per-instance.** The upload itself succeeded on S3. A steady rise means the ledger
    /// directory is failing and multipart write-through caching is effectively off, while
    /// single-part PUTs keep caching.
    #[serde(default)]
    pub mpu_completion_ledger_declines_total: u64,
    /// Write_Ledger length: entries across every instance's ledger file.
    ///
    /// Read alongside `staged_entries` (R8.3). The two measure the same population by
    /// different routes — `staged_entries` is a per-instance in-memory gauge, this is a
    /// count of records on shared storage — so a persistent gap between them is the
    /// signal that compaction is not keeping up or that appends are being lost. Neither
    /// is the accounting authority; `resident_bytes` is.
    ///
    /// `None` is the UNREADABLE sentinel — see `WriteLedger::count_entries` for why 0
    /// was actively misleading here. Serialises as JSON `null`.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 2.6, 8.3;
    /// cache-eviction-at-scale § 16.6.3.
    pub ledger_entries: Option<u64>,
    /// How many seconds ago `ledger_entries` above was observed.
    ///
    /// `ledger_entries` is an observation of **bounded age**, not a count taken during this
    /// scrape. It used to be the latter, and producing it cost a whole-volume traversal of the
    /// ledger — `1 + B + (B × 256)` directory listings plus up to `B × 256 × instances` file
    /// reads on shared storage — on every scrape, every dashboard refresh and every OTLP export.
    /// The `/metrics` body is all-or-nothing, so a collection slow enough to time out costs a
    /// scraper every series rather than this one field.
    ///
    /// This field is what keeps that trade honest. Without it the staleness would be invisible,
    /// and an operator reading the same `ledger_entries` from two scrapes 15 seconds apart could
    /// not tell a stable figure from the same observation reported twice.
    ///
    /// `None` exactly when `ledger_entries` is `None`: no observation has been taken, or the last
    /// attempt could not read the volume. The bound is
    /// `write_ledger::LEDGER_COUNT_MAX_AGE_SECS`, and in the steady state the value here is well
    /// inside it because ledger compaction donates the figure it already computes.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.3.
    pub ledger_entries_age_secs: Option<u64>,

    /// The fold-derived staged figure, its components, its age, and its signed gap
    /// against the **incumbent** counter.
    ///
    /// **Observability only.** Nothing in this struct is read by an admission or eviction
    /// decision. See `crate::staged_fold` for the protocol and for why the two producers are
    /// published side by side before either is retired.
    ///
    /// The gap's comparand is the incumbent and not `resident_bytes` above, which at epoch 1
    /// *is* the fold's figure — comparing it against itself would publish a confident zero.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.1, 16.6.3, 16.6.7, 16.6.9.
    #[serde(flatten)]
    pub staged_fold: crate::staged_fold::StagedFoldGauges,
    /// The ledger-completeness repair rotation: whether § 16.6.6 precondition 3b holds, the age
    /// of the stalest completed sweep, and the count of staged ranges found on disk with no
    /// ledger entry.
    ///
    /// **`ranges_reappended_total` non-zero is a finding, not a routine reading.** Each one was a
    /// staged range excluded from the staged figure — the under-count direction § 16.6.2 rules
    /// out. See `crate::ledger_repair`.
    #[serde(flatten)]
    pub ledger_repair: crate::ledger_repair::LedgerRepairGauges,
    /// The scanner-concurrency limiter for the fold and repair passes. **Per-instance.** See
    /// [`ScannerGauges`].
    #[serde(flatten)]
    pub scanner: ScannerGauges,

    /// Torn ledger lines compaction moved to a `.ledger.torn` quarantine file instead of
    /// discarding, since process start. **Per-instance.**
    ///
    /// **Non-zero is a finding, not a routine reading.** Each line is a staged range whose ledger
    /// entry was destroyed by a partial write. Compaction used to drop such lines, which converted
    /// an incompleteness the reader had *reported* into silent permanent loss of those bytes from
    /// the staged figure — loss mode L5, and in the under-count direction § 16.6.2 rules out.
    ///
    /// Read it beside `ledger_repair_ranges_reappended_total`: the quarantine says an entry was
    /// lost, and that counter says the rotation found the range on disk and restored it. A rising
    /// quarantine count with a flat re-append count means the loss has been recorded and not yet
    /// repaired.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6, loss mode L5.
    pub ledger_torn_lines_quarantined_total: u64,

    /// Staged write-cache credits refused at a site that writes no `Write_Ledger` entry, since
    /// process start. **Per-instance.**
    ///
    /// **This should read zero forever.** `disk_cache::store_range`'s hybrid arm and
    /// `commit_incremental_range` credit no ledger entry, so a staged credit there would be a staged
    /// range the fold's candidate set cannot see — a structural per-write loss mode. It is
    /// unreachable by caller convention; this counter is what makes the convention observable rather
    /// than assumed. Non-zero means a new caller reached one of those sites with staged metadata, and
    /// the fix is to add the ledger append at that site.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6, the reachability check under the loss-mode table.
    pub unledgered_staged_credits_refused_total: u64,

    /// The staged-authority epoch: `0` while the incumbent event-sourced counter is the
    /// authority (Phase L), `1` once the fold is.
    ///
    /// **This is the field that says which derivation `resident_bytes` and
    /// `cache.write_cache_size` currently use**, so an operator comparing two proxies or two
    /// points in time has to read it before comparing the figures. Fleet-wide: it is read from
    /// a document on the shared volume, so all instances agree once each has observed the
    /// activation.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6.
    pub staged_authority_epoch: u64,

    /// Graduations refused because the key's metadata slot lock was held by another writer.
    ///
    /// Per-instance. Read as a ratio against `graduations_total`: a small ratio is the
    /// expected steady state, and one approaching 1 means the tier has stopped draining. See
    /// `cache::graduation_lock_unavailable_total`.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.5, 16.6.7.
    pub graduation_lock_unavailable_total: u64,

    /// `Graduation` journal entries parsed, applied as cleanup-only no-ops, and retired since
    /// process start.
    ///
    /// Per-instance. Zero before activation by construction. It rises after activation as the
    /// legacy channel drains and should then stop; one that keeps rising indefinitely means
    /// something is still *writing* Graduation entries, which after activation only a
    /// pre-C-behaving instance does — read it with `staged_authority_foreign_write_total`.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.7, 16.6.8.
    pub graduation_noop_entries_total: u64,

    /// Journal lines the last journal cleanup pass could not parse and kept in place.
    ///
    /// Per-instance gauge. Non-zero during a rolling upgrade is expected (a newer build's
    /// entries this build cannot apply) and should return to zero once every instance runs
    /// the newer build. Non-zero on a uniform fleet means torn or corrupt journal lines. See
    /// `journal_consolidator::retained_unparseable_journal_lines`.
    pub journal_unparseable_lines_retained: u64,

    /// `size_state.json` rewrites by a pre-C binary observed after activation.
    ///
    /// Per-instance. **Non-zero is an incident**, not a curiosity: it means a binary predating
    /// decision C is writing the shared accounting state. The activation gate refuses a pre-C
    /// writer it can see, but not one that sat idle for a day before activation: B8
    /// (`staged_authority::RECORDLESS_QUIET_RETIRE_SECS`) stops counting a record-less id once
    /// its ledger files are a day unmodified, so if that id was a live idle pre-C binary and it
    /// writes again after activation, this counter is what detects it, after the fact. The fold
    /// refuses to install while it is live, so
    /// `staged_shard_authority_age_secs` rises visibly rather than the figure becoming
    /// silently wrong. Rollback to a pre-C binary is not supported once the epoch is `1`;
    /// see `docs/UPGRADING.md`.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6.
    pub staged_authority_foreign_write_total: u64,
    /// Acquisitions of a `{instance}.pending.lock` that did not succeed on the first try.
    ///
    /// **Per-instance.** That lock is the only cross-instance lock on the write-through PUT
    /// admission path, and its holders include every peer's own credit append, the fold's
    /// post-pass GC and the startup reaper. Read with the two fields below and against
    /// `setup_abandoned_total_put`: an earlier measurement ranked a lock wait and the shard-checkpoint traversal
    /// as the two candidates for the same 5 s setup abandons, in the same window, and recorded
    /// that no published field discriminated them. This pair is what does.
    #[serde(default)]
    pub pending_lock_waits_total: u64,
    /// Summed milliseconds spent waiting for those locks, timeouts included.
    /// **Per-instance.**
    #[serde(default)]
    pub pending_lock_wait_ms_total: u64,
    /// Bounded acquisitions abandoned at their bound. **Per-instance.**
    ///
    /// Each one is a `.pending` file reported unreadable, which makes the post-cut term unknown
    /// for that pass. Admission then **admits** (an unreadable count fails open), so a non-zero
    /// reading means the staged count is being under-stated and the budget can be overshot —
    /// never that caching was refused.
    #[serde(default)]
    pub pending_lock_timeouts_total: u64,
    /// Why the staged-authority epoch has not activated, as the gate's stable blocker label
    /// (`ledger_without_ready_record`, `no_fresh_ready_records`, `rotation_incomplete`, ...).
    ///
    /// Per-instance: the last verdict **this** process evaluated, which it does only on
    /// non-idle cycles where it wins the global consolidation lock. `null` when no verdict has
    /// been evaluated yet, when the last one permitted activation, and once the epoch is `1`.
    /// Read it with `staged_authority_activation_blocking_instance`.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6 (the 2026-09-25 amendment, B6).
    pub staged_authority_activation_blocker: Option<String>,
    /// The instance id (`{hostname}:{pid}`) the activation blocker names. `null` when there is
    /// no blocker, and for the volume-wide blockers (`no_ready_records`,
    /// `no_fresh_ready_records`, `ready_records_unreadable`) that name no instance.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6 (the 2026-09-25 amendment, B6).
    pub staged_authority_activation_blocking_instance: Option<String>,
    /// How many instance ids are holding activation back: each ledger id no current `.ready`
    /// record vouches for and whose ledger files were modified inside a day (and no operator
    /// retirement marker covers), plus each fresh record failing a precondition.
    /// `staged_authority_activation_blocking_instance` is the first of them. Each is logged at
    /// INFO once when it starts holding the gate.
    ///
    /// Per-instance, like the blocker pair. `null` in the pair's null cases, and when the last
    /// verdict examined no id (`ready_records_unreadable`, `no_ready_records`); `0` for
    /// `no_fresh_ready_records` when no id holds the gate.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6 B9.
    pub staged_authority_activation_blocking_instance_count: Option<u64>,
    /// The last **count-driven** write-cache eviction pass's result, read from
    /// `metadata/_shard_state/write_cache_count_eviction_last_result.json`.
    ///
    /// # Most of R12.1, and it was already computed and already on disk
    ///
    /// The pass builds and installs this document on every count-driven run and nothing read it
    /// back, while its read-tier sibling `cache.eviction_last_result` was published. That
    /// asymmetry is the whole of this field: no new measurement, no new producer.
    ///
    /// It carries R12.1's pass count, candidates considered, entries and ranges evicted, and the
    /// shortfall decomposed with `unevictable_unreadable_ranges` as a **named component** of
    /// `shortfall_ranges`, plus `shortfall_reason` naming which of § 16.6.12 point 5's three
    /// states the pass was in.
    ///
    /// # Read the scope of each sub-field, because this document MIXES them
    ///
    /// The document itself is **fleet-wide**: whichever instance won the eviction lock wrote it,
    /// and every instance publishes the same copy. But `passes_total`, `ranges_freed_total` and
    /// `admission_declines_total` are that **writer's** per-process counters since its start, not
    /// fleet sums. `instance_id` is in the document precisely so those three can be attributed;
    /// summing them across a fleet's `/metrics` responses would count one instance's figures once
    /// per proxy.
    ///
    /// `null` before any count-driven pass has completed on this volume, which is state (c)'s
    /// primary signal and not an error.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.12 point 5. Requirements: 12.1.
    pub count_pass_last_result: Option<crate::eviction_result::WriteCacheCountPassResult>,
}

/// Rate-limit interval for the Resident_Bytes-over-Staging_Bound WARN
/// (Requirement 8.5). Mirrors the 60s window used by
/// `InflightLedger::log_rejection_rate_limited` and
/// `HttpProxy::shed_request`'s `CONCURRENCY_LAST_LOG` — one `warn!` per
/// window rather than one per `/metrics` poll or per consolidation cycle.
const OVER_BOUND_WARN_INTERVAL_SECS: u64 = 60;

/// Unix timestamp (seconds) of the last rate-limited `over_bound` WARN.
/// Process-wide: `collect_write_cache_metrics` is called from every
/// `/metrics` request, the dashboard's refresh, and OTLP export, so a
/// per-call limiter would still spam if two of those overlap within the
/// window.
static LAST_OVER_BOUND_WARN_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// A background collection is slow when it takes more than `1 / COLLECTION_SLOW_SHARE_DIVISOR` of
/// its slot. See [`collection_slow_threshold_ms`].
const COLLECTION_SLOW_SHARE_DIVISOR: u128 = 2;

/// Above this many milliseconds, one full collection is slow enough to be worth a WARN and a
/// counter: **half of `metrics.collection_interval`**.
///
/// # Why relative to the interval, and why half
///
/// Nothing waits on a collection any more. `/metrics` answers from the latest background run, so a
/// slow run costs staleness, not a response, and the question worth warning about is whether the
/// run still fits its slot. The collector skips a missed tick rather than overlapping runs, so a
/// run longer than the interval leaves the published figures two intervals old.
///
/// Half is the point at which the run is **one doubling of its cost** away from that. The cost is
/// a per-document loop over the shard population, measured on the fleet's EFS volume at ~1.4 ms
/// per document with no filesystem-layer caching
/// (measured), so it scales linearly with
/// the shard count at best, and doubling the shard population doubles it. A warning at half the
/// slot therefore fires with one such doubling of headroom left, rather than after the slot has
/// already been lost.
///
/// # The premise the fixed 2,000 ms rested on, and why it was dropped
///
/// The constant this replaced was justified by "a healthy collection on this cache is
/// milliseconds". Measured on the fleet on 2026-09-24, a full collection took 6,550-9,990 ms, so
/// the premise was false and the warning fired on every collection. Tying the threshold to the
/// interval makes it a statement about the one budget this path still has.
pub(crate) fn collection_slow_threshold_ms(interval: Duration) -> u64 {
    u64::try_from(interval.as_millis() / COLLECTION_SLOW_SHARE_DIVISOR).unwrap_or(u64::MAX)
}

/// Rate-limit window for the slow-collection WARN, matching
/// [`OVER_BOUND_WARN_INTERVAL_SECS`]. At the default 60 s interval this allows one line per run,
/// and a shorter interval cannot make it louder than that.
const COLLECTION_SLOW_WARN_INTERVAL_SECS: u64 = 60;

/// Collections of the full metrics set since process start.
///
/// **Counts COLLECTIONS, not HTTP scrapes**, and the name says so because the difference is
/// load-bearing: in production `collect_metrics` is driven by the background collector once per
/// `metrics.collection_interval`, and a `/metrics` request or dashboard refresh performs none. It
/// is the denominator for the duration figures beside it — a `duration_ms_max` with no idea how
/// many collections produced it is not a distribution.
static COLLECTIONS_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Wall-clock milliseconds the most recent collection took.
static COLLECTION_DURATION_MS_LAST: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// High-water mark of collection duration since process start.
///
/// **This is the field that would have caught the 2026-09-21 failure**, and the reason a
/// last-value gauge alone is not enough: a collection that outruns its client's deadline delivers
/// no body at all, so the slow collection is precisely the one nobody gets to read. The peak
/// survives into every later response, which is where someone investigating a lost sample will
/// actually look.
static COLLECTION_DURATION_MS_MAX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Collections that exceeded [`collection_slow_threshold_ms`], since process start.
static COLLECTION_SLOW_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Unix timestamp (seconds) of the last rate-limited slow-collection WARN.
static LAST_COLLECTION_SLOW_WARN_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Successful takeovers of a stale GLOBAL EVICTION lock, process-wide since start.
///
/// **Not the metadata slot lock.** That one cannot go stale and is never taken from a holder;
/// this is `locks/global_eviction.lock`, which does carry a payload with an `acquired_at_ms` and
/// is deliberately taken over once `cache.shared_storage.eviction_lock_timeout` has elapsed.
/// The two figures answer different questions and must not be added together.
///
/// A process-wide atomic rather than a field on `EvictionCoordinationStats`, because the
/// takeover happens inside a synchronous block holding a `std::sync::Mutex` guard, where the
/// `async` recorder cannot be awaited. Every path out of that block is an early `return`, so
/// there is no point after it at which an `await` could run either.
static STALE_EVICTION_LOCKS_RECOVERED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Record one successful takeover of a stale global eviction lock.
///
/// Synchronous by necessity — see `STALE_EVICTION_LOCKS_RECOVERED`.
pub fn record_stale_eviction_lock_recovered() {
    STALE_EVICTION_LOCKS_RECOVERED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Successful stale global-eviction-lock takeovers since process start.
pub fn stale_eviction_locks_recovered() -> u64 {
    STALE_EVICTION_LOCKS_RECOVERED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Hedged upstream request statistics.
///
/// Exposed as `hedged_requests.{issued,won,suppressed}` in the `/metrics` JSON.
/// - `issued`: count of hedges launched (= duplicate S3 requests billed).
/// - `won`: count of requests where the hedge, not the original, was the Winner.
/// - `suppressed`: count of would-be hedges not issued due to cost bounds.
///
/// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HedgingStats {
    pub issued: u64,
    pub won: u64,
    pub suppressed: u64,
}

/// Request processing metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMetrics {
    /// Cumulative request completions since process start, including semaphore rejections.
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    /// Completed 4xx responses. Semaphore overload rejections are also counted separately.
    pub client_error_requests: u64,
    /// Completed 5xx responses.
    pub server_error_requests: u64,
    /// Requests rejected before processing because `max_concurrent_requests` was reached.
    pub rejected_requests: u64,
    /// GET/HEAD responses served from cache, identified by `X-Cache: HIT`.
    pub cache_hit_requests: u64,
    /// GET/HEAD responses not served from cache.
    pub cache_miss_requests: u64,
    pub average_response_time_ms: u64,
    /// Lifetime-average request rate since the process started, not a rolling window.
    pub requests_per_second: f32,
    /// Active TCP connections. HTTP/1 keep-alive means this is not in-flight request count.
    pub active_requests: u64,
    pub max_concurrent_requests: u64,
    /// Configured total request-concurrency permits (`server.max_concurrent_requests`).
    /// Same underlying config value as `max_concurrent_requests` above, exposed under
    /// the `permits_*` name so it can be paired with `permits_held`/`permits_available`
    /// without mixing naming schemes. Requirement: TCA 5.3
    pub permits_total: u64,
    /// Request-concurrency permits currently held. Distinct from `active_requests`,
    /// which counts TCP connections (a connection can outlive the permit held for any
    /// single request on it, via HTTP/1 keep-alive). Requirement: TCA 5.1, 5.5
    pub permits_held: u64,
    /// Request-concurrency permits currently available (`total - held`).
    /// Requirement: TCA 5.2
    pub permits_available: u64,
    /// High-water mark of `permits_held` since process start. Requirement: TCA 5.4
    pub permits_held_peak: u64,
    /// Access-log records not written because the logger stayed busy for
    /// `logging::ACCESS_LOG_HANDOFF_WAIT` (30 s), or because 10,000 hand-offs were already
    /// queued (`logging::ACCESS_LOG_HANDOFF_LIMIT`). Per instance, since process start.
    #[serde(default)]
    pub access_log_records_dropped: u64,
}

/// Request type taxonomy mirroring S3's named request-metric subtypes.
///
/// Derived from the S3-style `AccessLogEntry.operation` string — **never** from the
/// raw HTTP method.  The raw HTTP verb alone is insufficient: a `GET` on a bucket
/// is a list, a `POST ?delete` is a delete, and `UploadPart` is a PUT despite the
/// surrounding multipart context.  Mapping from the operation semantics keeps the
/// proxy's per-type counters dimensionally comparable to S3's named request metrics.
///
/// `SelectRequests` is intentionally out of scope: the proxy does not serve
/// `SelectObjectContent`.
///
/// Request type tracked for per-bucket traffic.
///
/// Scope is intentionally limited to object reads (GET) and object/part writes
/// (PUT, including UploadPart). Other S3 operations — HEAD, DELETE, the multipart
/// lifecycle POSTs (Create/Complete), and LIST — are deliberately NOT counted.
///
/// Spec: per-bucket-metrics. Requirements: 1.2, 2.2
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    /// Object read: `GET <bucket>/<key>`, including range and presigned GETs.
    /// Mirrors S3 `GetRequests`. Bucket-level GETs with no object key
    /// (list-objects) are intentionally not counted.
    Get,
    /// Object or part write: `PUT <bucket>/<key>` (PutObject) and
    /// `PUT <bucket>/<key>?partNumber=..` (UploadPart). Mirrors S3 `PutRequests`.
    Put,
}

/// Attribution key for per-bucket traffic accounting.
///
/// Bucket-level when no prefix is configured or no prefix matches the object key.
/// Prefix-level (Some(prefix)) when prefix attribution is active.
///
/// `__other__` as the `bucket` field (with `prefix: None`) is the reserved overflow
/// sentinel — it is not a valid S3 DNS-compliant bucket name (leading underscore),
/// so it cannot collide with real traffic.
///
/// Spec: per-bucket-metrics. Requirements: 1.1, 5.2
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketTrafficKey {
    pub bucket: String,
    /// `None` = bucket-level aggregation. `Some(prefix)` = prefix-level attribution.
    pub prefix: Option<String>,
}

/// Per-bucket (or per-bucket+prefix) cumulative traffic counters.
///
/// All fields are monotonically increasing since proxy start (no resets).
/// Scope is limited to object reads (GET) and object/part writes (PUT/UploadPart);
/// see `RequestType`.
///
/// Spec: per-bucket-metrics. Requirements: 1.1, 1.2, 1.3
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketTrafficStats {
    /// Cumulative bytes sent to the client (response body). Mirrors S3 `BytesDownloaded`.
    pub bytes_served: u64,
    /// Cumulative bytes sent to the client that were served from cache (cache hits only).
    /// The difference `bytes_served - bytes_saved` is bytes fetched from S3.
    pub bytes_saved: u64,
    /// Cumulative bytes received from the client (request body for PUT/UploadPart).
    /// Mirrors S3 `BytesUploaded`. Zero for GET.
    pub bytes_uploaded: u64,
    /// Object reads (GET). Mirrors S3 `GetRequests`.
    pub get_requests: u64,
    /// Object/part writes (PutObject + UploadPart). Mirrors S3 `PutRequests`.
    pub put_requests: u64,
}

/// Resolve the traffic attribution key for a request.
///
/// If `configured_prefixes` is `Some` and non-empty, returns the longest configured
/// prefix that `object_key` starts with. If multiple prefixes match, the longest wins.
/// If no prefix matches, or if `configured_prefixes` is `None` or empty, returns a
/// bucket-level key (`prefix: None`).
///
/// An empty-string prefix in the configured list is treated as "no match" (the config
/// layer already rejects them with a warning, but this is the safety net).
///
/// The `bucket` string is used verbatim — normalization (lower-casing etc.) is the
/// caller's responsibility; none is applied here so the key matches the raw bucket
/// name used throughout the proxy.
///
/// Spec: per-bucket-metrics. Requirements: 5.2, 5.3, 5.4
pub fn resolve_traffic_key(
    bucket: &str,
    object_key: &str,
    configured_prefixes: Option<&[String]>,
) -> BucketTrafficKey {
    let best = configured_prefixes
        .unwrap_or(&[])
        .iter()
        .filter(|p| !p.is_empty() && object_key.starts_with(p.as_str()))
        .max_by_key(|p| p.len());

    BucketTrafficKey {
        bucket: bucket.to_string(),
        prefix: best.cloned(),
    }
}

/// Per-bucket cache hit/miss counters, split by GET vs HEAD.
/// Tracked for every bucket with cache traffic. The same struct is reused
/// for per-rule counters keyed by glob pattern (see `rule_cache_stats`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketCacheStats {
    pub hit_count: u64,
    pub miss_count: u64,
    pub head_hit_count: u64,
    pub head_miss_count: u64,
    pub get_hit_count: u64,
    pub get_miss_count: u64,
}

/// Metrics collector and manager
pub struct MetricsManager {
    start_time: SystemTime,
    cache_manager: Option<Arc<CacheManager>>,
    connection_pool: Option<Arc<tokio::sync::RwLock<ConnectionPoolManager>>>,
    compression_handler: Option<Arc<CompressionHandler>>,
    request_stats: Arc<RwLock<RequestStats>>,
    error_stats: Arc<RwLock<ErrorStats>>,
    range_storage_stats: Arc<RwLock<RangeStorageStats>>,
    eviction_coordination_stats: Arc<RwLock<EvictionCoordinationStats>>,
    signed_put_stats: Arc<RwLock<SignedPutStats>>,
    connection_keepalive_stats: Arc<RwLock<ConnectionKeepaliveStats>>,
    coalescing_stats: Arc<RwLock<CoalescingStats>>,
    /// Page-aligned range caching (widening) statistics.
    /// Spec: page-aligned-range-cache. Requirements: 8.1-8.6
    page_cache_stats: Arc<RwLock<PageCacheStats>>,
    /// Per-bucket cache hit/miss counters (all buckets, keyed by bucket name)
    bucket_cache_stats: Arc<RwLock<HashMap<String, BucketCacheStats>>>,
    /// Per-rule cache hit/miss counters keyed by the rule's glob pattern
    rule_cache_stats: Arc<RwLock<HashMap<String, BucketCacheStats>>>,
    /// Per-bucket traffic counters (keyed by BucketTrafficKey).
    /// Mirrors the bucket_cache_stats concurrency pattern.
    /// Spec: per-bucket-metrics. Requirements: 1.1, 1.4, 7.2
    bucket_traffic_stats: Arc<RwLock<HashMap<BucketTrafficKey, BucketTrafficStats>>>,
    /// Maximum number of distinct bucket+prefix series tracked.
    /// Beyond this, traffic folds into the __other__ overflow sentinel.
    max_bucket_traffic_series: usize,
    /// Per-bucket prefix configuration for prefix-level attribution.
    /// Populated by set_per_bucket_config from PerBucketMetricsConfig.bucket_prefixes.
    /// Spec: per-bucket-metrics. Requirements: 5.1, 5.2
    per_bucket_prefixes: HashMap<String, Vec<String>>,
    last_metrics: Arc<RwLock<Option<SystemMetrics>>>,
    otlp_exporter: Option<Arc<RwLock<OtlpExporter>>>,
    /// Active HTTP connections counter (shared with HttpProxy)
    active_connections: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// Maximum concurrent requests from config
    max_concurrent_requests: usize,
    /// Shared request-concurrency semaphore (shared with HttpProxy and the TLS
    /// listener). `available_permits()` gives permits_available directly;
    /// permits_held is `permits_total - permits_available`. Spec:
    /// transfer-concurrency-admission. Requirements: TCA 5.1, 5.2, 5.3
    request_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Shared hedging metrics counters (owned by the HedgeGovernor/startup, read here).
    /// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
    hedge_metrics: Option<Arc<HedgeMetrics>>,
    /// Shared in-flight buffered-byte ledger (shared with HttpProxy and every
    /// Buffering_Site). Spec: inflight-memory-accounting. Requirements: 8.1-8.6
    inflight_ledger: Option<Arc<crate::inflight_ledger::InflightLedger>>,
    /// Configured `cache.max_ram_cache_size`, reported alongside the ledger.
    /// Requirement: 8.7
    ram_cache_max_bytes: u64,
    /// `metrics.collection_interval` after clamping: the background collector's period, and the
    /// base the slow-collection threshold is derived from.
    collection_interval: Duration,
}

/// At most this many levels are published in each `scanner_*_by_level_*` object.
pub const SCANNER_LEVELS_PUBLISHED_MAX: usize = 64;

/// The scanner-concurrency limiter's state, one set of fields per scan kind (`_fold`, `_repair`),
/// published flat under `write_cache` the way `setup_abandoned_total_put` carries its kind as a
/// suffix. **Every field is per-instance**: each proxy tunes its own `k` from its own visits.
///
/// All zero, empty and `""` when no consolidator is wired in.
///
/// Spec: cache-eviction-at-scale § 16.6.13 (K2-K4).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerGauges {
    /// The concurrency `k` the next fold pass respects: it defers while `n ≥ k`.
    pub scanner_concurrency_k_fold: u32,
    /// The concurrency `k` the next repair pass respects.
    pub scanner_concurrency_k_repair: u32,
    /// The live peer-scanner count `n` the fold gate last read.
    pub scanner_concurrency_observed_fold: u32,
    /// The live peer-scanner count `n` the repair gate last read.
    pub scanner_concurrency_observed_repair: u32,
    /// Fold visits discarded because `n` changed across them.
    pub scanner_samples_discarded_total_fold: u64,
    /// Repair visits discarded because `n` changed across them.
    pub scanner_samples_discarded_total_repair: u64,
    /// Deferred consolidation CYCLES for the fold, not passes: a pass that stays deferred is
    /// counted once per cycle it was due.
    pub scan_deferred_total_fold: u64,
    /// Deferred consolidation cycles for the repair rotation.
    pub scan_deferred_total_repair: u64,
    /// Why the fold limiter's last pass left `k` where it is.
    pub scanner_last_adjust_reason_fold: String,
    /// Why the repair limiter's last pass left `k` where it is.
    pub scanner_last_adjust_reason_repair: String,
    /// `.meta` files read per second per visit (EWMA), by level `n + 1`, fold. Only levels with a
    /// kept sample, at most [`SCANNER_LEVELS_PUBLISHED_MAX`].
    pub scanner_rate_by_level_fold: BTreeMap<u32, f64>,
    /// As `scanner_rate_by_level_fold`, repair.
    pub scanner_rate_by_level_repair: BTreeMap<u32, f64>,
    /// Kept samples by level, fold. Same levels as `scanner_rate_by_level_fold`.
    pub scanner_samples_by_level_fold: BTreeMap<u32, u64>,
    /// Kept samples by level, repair.
    pub scanner_samples_by_level_repair: BTreeMap<u32, u64>,
    /// `Â[j] = j·ρ̂[j]`, the estimated fleet-wide `.meta` read rate at `j` concurrent scanners,
    /// fold: the curve `k` is the argmax of. Only levels with enough kept samples to be judged
    /// (`scanner_limiter::N_MIN_SAMPLES`), so a subset of `scanner_rate_by_level_fold`'s keys.
    pub scanner_aggregate_estimate_by_level_fold: BTreeMap<u32, f64>,
    /// As `scanner_aggregate_estimate_by_level_fold`, repair.
    pub scanner_aggregate_estimate_by_level_repair: BTreeMap<u32, f64>,
}

impl ScannerGauges {
    /// Build from the two limiters and the gate's per-kind counters.
    pub(crate) fn from_limiters(
        fold: &crate::scanner_limiter::ScannerLimiter,
        repair: &crate::scanner_limiter::ScannerLimiter,
    ) -> Self {
        use crate::journal_consolidator::{scan_deferred_total, scan_last_observed};
        use crate::scanner_registry::ScanKind;
        type Levels = (BTreeMap<u32, f64>, BTreeMap<u32, u64>, BTreeMap<u32, f64>);
        fn levels(l: &crate::scanner_limiter::ScannerLimiter) -> Levels {
            let mut rate = BTreeMap::new();
            let mut samples = BTreeMap::new();
            let mut aggregate = BTreeMap::new();
            for (j, r, s) in l.sampled_levels().take(SCANNER_LEVELS_PUBLISHED_MAX) {
                rate.insert(j, r);
                samples.insert(j, s);
                if let Some(a) = l.aggregate_estimate(j) {
                    aggregate.insert(j, a);
                }
            }
            (rate, samples, aggregate)
        }
        let (rate_f, samples_f, aggregate_f) = levels(fold);
        let (rate_r, samples_r, aggregate_r) = levels(repair);
        Self {
            scanner_concurrency_k_fold: fold.k(),
            scanner_concurrency_k_repair: repair.k(),
            scanner_concurrency_observed_fold: scan_last_observed(ScanKind::Fold),
            scanner_concurrency_observed_repair: scan_last_observed(ScanKind::Repair),
            scanner_samples_discarded_total_fold: fold.samples_discarded(),
            scanner_samples_discarded_total_repair: repair.samples_discarded(),
            scan_deferred_total_fold: scan_deferred_total(ScanKind::Fold),
            scan_deferred_total_repair: scan_deferred_total(ScanKind::Repair),
            scanner_last_adjust_reason_fold: fold.last_adjust_reason().as_str().to_string(),
            scanner_last_adjust_reason_repair: repair.last_adjust_reason().as_str().to_string(),
            scanner_rate_by_level_fold: rate_f,
            scanner_rate_by_level_repair: rate_r,
            scanner_samples_by_level_fold: samples_f,
            scanner_samples_by_level_repair: samples_r,
            scanner_aggregate_estimate_by_level_fold: aggregate_f,
            scanner_aggregate_estimate_by_level_repair: aggregate_r,
        }
    }
}

/// Where a collection's shared-volume figures come from.
///
/// `Fresh` is the full collection the background collector runs: it has just traversed the volume
/// and holds the checkpoint set and the pending read taken BEFORE it (see
/// `staged_fold::read_gauge_inputs`). `Snapshot` is a `/metrics` request or a dashboard refresh:
/// it performs no traversal and reuses the figures of the last full collection, or publishes their
/// UNREADABLE sentinels when there has not been one yet.
enum VolumeSource<'a> {
    Fresh(
        &'a crate::staged_fold::ShardCheckpointSet,
        &'a crate::staged_fold::PendingReadOutcome,
        PartBytesReading,
    ),
    Snapshot(Option<&'a SystemMetrics>),
}

/// `P` as one collection's walk of `mpus_in_progress/` found it, and that walk's age.
#[derive(Debug, Clone, Copy, Default)]
struct PartBytesReading {
    bytes: Option<u64>,
    age_secs: Option<u64>,
    drift_bytes: Option<i64>,
}

impl VolumeSource<'_> {
    /// Seconds since the snapshot was collected; `0` for a fresh collection or before the first
    /// snapshot, where there is nothing to age.
    fn age_secs(&self) -> u64 {
        match self {
            VolumeSource::Fresh(..) | VolumeSource::Snapshot(None) => 0,
            VolumeSource::Snapshot(Some(last)) => snapshot_age_secs(last),
        }
    }
}

/// Seconds between a full collection's `timestamp` and now. A clock step backwards reads as `0`
/// rather than wrapping.
fn snapshot_age_secs(last: &SystemMetrics) -> u64 {
    SystemTime::now()
        .duration_since(last.timestamp)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Advance an age figure taken at collection time by the time elapsed since.
fn aged(age_at_collection: Option<u64>, since_collection_secs: u64) -> Option<u64> {
    age_at_collection.map(|a| a.saturating_add(since_collection_secs))
}

/// The ledger-repair gauges a request publishes: the eight per-process fields read live, the six
/// volume-derived fields from the last full collection.
///
/// Before the first collection that is exactly [`crate::ledger_repair::LedgerRepairGauges::process_local`],
/// which is also what a failed traversal publishes: the six volume fields at their UNREADABLE
/// sentinels, never a reassuring zero, and the eight live figures intact.
fn ledger_repair_from_snapshot(
    snapshot: Option<&crate::ledger_repair::LedgerRepairGauges>,
    since_collection_secs: u64,
) -> crate::ledger_repair::LedgerRepairGauges {
    let live = crate::ledger_repair::LedgerRepairGauges::process_local();
    match snapshot {
        None => live,
        Some(s) => crate::ledger_repair::LedgerRepairGauges {
            ledger_repair_rotation_complete: s.ledger_repair_rotation_complete,
            ledger_repair_stalest_age_secs: aged(
                s.ledger_repair_stalest_age_secs,
                since_collection_secs,
            ),
            ledger_repair_observed_shards: s.ledger_repair_observed_shards,
            ledger_repair_swept_shards: s.ledger_repair_swept_shards,
            ledger_repair_rotation_reachable: s.ledger_repair_rotation_reachable,
            ledger_repair_projected_rotation_secs: s.ledger_repair_projected_rotation_secs,
            ..live
        },
    }
}

/// The staged-fold gauges a request publishes: `live` carries every per-process counter and the
/// small freshness-document read, and the checkpoint-derived figures come from the last full
/// collection, together, so they describe one membership.
///
/// The checkpoint-derived figures are taken as a group rather than recomputed against a fresher
/// pending read. `staged_fold_bytes` is `F + post-cut pending + P`, and pairing an old checkpoint
/// set with a newer pending read would mix memberships — a credit folded into a checkpoint the
/// snapshot does not hold, and already pruned from the pending files, would be counted by neither.
///
/// Before the first collection `live` is published unchanged, and its checkpoint-derived figures
/// are `null` (or `0` for the two counts), exactly as for a volume on which no fold has completed.
/// `volume_snapshot.collected_at_unix_secs: null` is what tells the two apart.
fn staged_fold_from_snapshot(
    live: crate::staged_fold::StagedFoldGauges,
    snapshot: Option<&crate::staged_fold::StagedFoldGauges>,
    since_collection_secs: u64,
) -> crate::staged_fold::StagedFoldGauges {
    match snapshot {
        None => live,
        Some(s) => crate::staged_fold::StagedFoldGauges {
            staged_fold_bytes: s.staged_fold_bytes,
            staged_fold_byte_sum: s.staged_fold_byte_sum,
            staged_fold_pending_bytes: s.staged_fold_pending_bytes,
            staged_fold_in_progress_part_bytes: s.staged_fold_in_progress_part_bytes,
            staged_fold_gap_bytes: s.staged_fold_gap_bytes,
            staged_authority_age_secs: aged(s.staged_authority_age_secs, since_collection_secs),
            staged_shard_authority_age_secs: aged(
                s.staged_shard_authority_age_secs,
                since_collection_secs,
            ),
            staged_fold_shards: s.staged_fold_shards,
            staged_fold_shards_beyond_max_age: s.staged_fold_shards_beyond_max_age,
            staged_range_count: s.staged_range_count,
            staged_mutations_awaiting_fold: s.staged_mutations_awaiting_fold,
            ..live
        },
    }
}

/// Spawn the background metrics collector: one full collection per `interval`, starting
/// immediately, until `shutdown` fires.
///
/// # What each run does
///
/// [`MetricsManager::collect_metrics`], which traverses the shared volume once, caches the result
/// for `/metrics` requests and dashboard refreshes to serve, and records the OTLP gauges. So the
/// OTLP exporter's periodic reader has fresh figures to send whether or not anything requests
/// `/metrics`.
///
/// # Runs never overlap
///
/// The loop awaits each collection before it awaits the next tick, so a second run cannot start
/// while the first is in progress; [`tokio::time::MissedTickBehavior::Skip`] then drops the ticks a
/// long run overran instead of firing them back to back to catch up. Two overlapping or
/// back-to-back runs would double the shared-volume load at exactly the moment the volume is
/// already too slow to keep up.
///
/// # The first run starts at once, and nothing waits for it
///
/// The first tick fires immediately, so the first snapshot lands one collection's duration after
/// startup. Startup does not block on it: on a large cache that is seconds of shared-volume reads,
/// and until it lands every background-derived figure publishes its UNREADABLE sentinel with
/// `volume_snapshot.collected_at_unix_secs: null`.
///
/// # The manager lock
///
/// Each run holds a READ guard on the manager for its whole duration. Requests take read guards
/// too, so they are not blocked. The only writers are the startup wiring calls (`set_*`), and
/// `tokio::sync::RwLock` is fair, so a writer queued behind a running collection also holds up new
/// readers until that run ends — bounded by one collection, once, during startup. This is no worse
/// than before, when every `/metrics` request held the same guard for a full collection.
///
/// A zero `interval` is replaced by one second here, because `tokio::time::interval` panics on a
/// zero period and a panic in a spawned task is swallowed, which would make the collector silently
/// not exist. Configuration already clamps the field (see `MetricsConfig::validate_and_clamp`);
/// this guards callers that construct the interval directly.
pub fn spawn_background_collector(
    metrics_manager: Arc<RwLock<MetricsManager>>,
    interval: Duration,
    mut shutdown: crate::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let period = if interval.is_zero() {
        Duration::from_secs(1)
    } else {
        interval
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = metrics_manager.read().await.collect_metrics().await;
                }
                _ = shutdown.wait_for_shutdown() => {
                    info!("Background metrics collector received shutdown signal");
                    break;
                }
            }
        }
    })
}

/// Internal request statistics tracking
#[derive(Debug)]
struct RequestStats {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    client_error_requests: u64,
    server_error_requests: u64,
    rejected_requests: u64,
    cache_hit_requests: u64,
    cache_miss_requests: u64,
    total_response_time_ms: u64,
    last_reset: SystemTime,
}

impl Default for RequestStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            client_error_requests: 0,
            server_error_requests: 0,
            rejected_requests: 0,
            cache_hit_requests: 0,
            cache_miss_requests: 0,
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
    cache_bypasses_by_reason: HashMap<String, u64>,
    // Cache coherency metrics
    cache_etag_validations_total: u64,
    cache_etag_mismatches_total: u64,
    cache_range_invalidations_total: u64,
    cache_orphaned_ranges_cleaned_total: u64,
    // Cache operation timing and error metrics
    cache_cleanup_failures_total: u64,
    // Part caching metrics - Requirements 8.1, 8.2, 8.3, 8.4, 8.5
    cache_part_hits: u64,
    cache_part_misses: u64,
    cache_part_stores: u64,
    cache_part_evictions: u64,
    cache_part_errors: u64,
    // Eager invalidation counter — read_cache_enabled=false purges
    read_cache_disabled_invalidations_total: u64,
    // TTL-driven revalidation counter — freshness expired, conditional GET/HEAD to S3
    ttl_revalidations_total: u64,
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
    /// Total time spent holding the eviction lock in milliseconds (Requirement 7.4)
    pub total_lock_hold_time_ms: u64,
    /// Number of evictions performed while holding the lock (Requirement 7.5)
    pub evictions_coordinated: u64,
    /// Number of evictions skipped because another instance holds the lock (Requirement 7.5)
    pub evictions_skipped_lock_held: u64,
    /// Number of **staging** eviction passes skipped because another instance held the
    /// global eviction lock.
    ///
    /// Counted separately from `evictions_skipped_lock_held` for the same reason
    /// `staging_evictions_total` is separate from `cache.evictions`: the two tiers share
    /// the lock but not the trigger, so folding them together makes it impossible to tell
    /// whether the write tier is being starved of the lock by read-tier eviction.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 7.4
    pub staging_evictions_skipped_lock_held: u64,
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
    /// Skipped-PUT counts keyed by reason.
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.1
    skipped_puts_by_reason: HashMap<String, u64>,
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
    /// Connections closed due to idle timeout. **Structurally zero, and published on
    /// `/metrics` as `connection_pool.idle_timeout_closures`.** Nothing writes it:
    /// idle reaping is owned by `hyper_util::client::legacy::Client`'s pool
    /// (`.pool_idle_timeout(config.idle_timeout)` in `S3Client::new`), which exposes no
    /// callback, event or counter for an idle expiry, so there is no observation point.
    /// `max_lifetime_closures` differs because the proxy enforces `max_lifetime` itself
    /// (`S3Client::enforce_max_lifetime`). Do not wire this to an approximation: a guess
    /// published under a count's name is worse than a visible zero. `docs/METRICS_REFERENCE.md`
    /// still tells operators to compare the two, which cannot work while this side is zero.
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
    /// Waiter conditional GET/HEAD returned `304 Not Modified` — the
    /// canonical "validated cache serve" outcome. Added by the
    /// download-coordination TTL correctness fix.
    pub waiter_conditional_304: u64,
    /// Waiter conditional GET/HEAD returned `200 OK` — object changed
    /// mid-flight between the fetcher's commit and the waiter's
    /// validation; the waiter serves S3's fresh body and updates the
    /// cache.
    pub waiter_conditional_200: u64,
    /// Waiter conditional GET/HEAD returned a `4xx` — credentials issue
    /// or client error. S3's response flows through to the client
    /// unchanged; the cache is preserved.
    pub waiter_conditional_4xx: u64,
    /// Waiter conditional GET/HEAD failed with a `5xx` from S3 or a
    /// transport error — the helper logs a warning and falls back to
    /// the existing cache entry (degraded path).
    pub waiter_conditional_error: u64,
}

/// Page-aligned range caching (widening) statistics tracking.
/// Spec: page-aligned-range-cache. Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6
#[derive(Debug, Default, Clone)]
pub struct PageCacheStats {
    /// Total requests widened into a page-aligned fetch (Requirement 8.1)
    pub widened_requests: u64,
    /// Bytes actually requested by the client across all widened requests
    /// (used with `bytes_prefetched` to derive the amplification ratio)
    pub bytes_requested: u64,
    /// Bytes fetched beyond what the client requested, summed across all
    /// widened requests (Requirement 8.2)
    pub bytes_prefetched: u64,
    /// Small_Reads served from an already-cached Page with no S3 fetch
    /// (Requirement 8.3)
    pub page_hits: u64,
    /// Requests declined widening because the Range was signed
    /// (Requirement 8.4)
    pub skipped_signed_range: u64,
    /// Widened/page fetches that failed and fell back to the client's
    /// original range (Requirement 8.5)
    pub fallbacks: u64,
    /// Pages successfully promoted to the RAM cache (Requirement 8.6)
    pub ram_page_promotions: u64,
    /// Pages not promoted to RAM because they exceeded the applicable RAM
    /// budget (Requirement 8.6, see Requirement 7.7)
    pub ram_page_promotion_skipped: u64,
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
            coalescing_stats: Arc::new(RwLock::new(CoalescingStats::default())),
            page_cache_stats: Arc::new(RwLock::new(PageCacheStats::default())),
            bucket_cache_stats: Arc::new(RwLock::new(HashMap::new())),
            rule_cache_stats: Arc::new(RwLock::new(HashMap::new())),
            bucket_traffic_stats: Arc::new(RwLock::new(HashMap::new())),
            max_bucket_traffic_series: 100, // config-compatible default; overridden via set_per_bucket_config
            per_bucket_prefixes: HashMap::new(),
            last_metrics: Arc::new(RwLock::new(None)),
            otlp_exporter: None,
            active_connections: None,
            max_concurrent_requests: 200,
            request_semaphore: None,
            hedge_metrics: None,
            inflight_ledger: None,
            ram_cache_max_bytes: 0,
            // `MetricsConfig::default().collection_interval`; overridden by
            // `set_collection_interval` from the loaded config.
            collection_interval: Duration::from_secs(60),
        }
    }

    /// Set the background collection interval, from `metrics.collection_interval`.
    ///
    /// Also sets the slow-collection threshold, which is derived from it.
    pub fn set_collection_interval(&mut self, interval: Duration) {
        self.collection_interval = interval;
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

    /// Enable per-bucket OTLP observable counters.
    ///
    /// Passes the shared `bucket_traffic_stats` handle to
    /// `OtlpExporter::initialize_per_bucket`. Has no effect when OTLP is disabled
    /// or `initialize_otlp` has not been called.
    ///
    /// Call this after `initialize_otlp` and before the metrics collection loop
    /// starts (i.e., during startup, before wrapping in `Arc<RwLock<...>>`).
    ///
    /// Spec: per-bucket-metrics. Requirements: 4.1, 4.3
    pub async fn enable_per_bucket_otlp(&self) {
        if let Some(otlp) = &self.otlp_exporter {
            let stats = self.bucket_traffic_stats.clone();
            otlp.write().await.initialize_per_bucket(stats);
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

    /// Set active connections counter and max concurrent requests for metrics reporting
    pub fn set_active_connections(
        &mut self,
        active_connections: Arc<std::sync::atomic::AtomicUsize>,
        max_concurrent_requests: usize,
    ) {
        self.active_connections = Some(active_connections);
        self.max_concurrent_requests = max_concurrent_requests;
    }

    /// Set the shared request-concurrency semaphore for permit observability
    /// (`permits_held`, `permits_available`). `HttpProxy::get_request_semaphore()`
    /// already exists (used by the TLS listener), so this holds it the same way
    /// `set_active_connections` holds the connection counter. Requirement: TCA 5.1, 5.2
    pub fn set_request_semaphore(&mut self, request_semaphore: Arc<tokio::sync::Semaphore>) {
        self.request_semaphore = Some(request_semaphore);
    }

    /// Set the shared hedge metrics reference.
    ///
    /// The `HedgeMetrics` instance is created at startup alongside the
    /// `HedgeGovernor` and shared with both `race_first_byte` (which increments
    /// counters) and `MetricsManager` (which reads them during collection).
    ///
    /// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
    pub fn set_hedge_metrics(&mut self, hedge_metrics: Arc<HedgeMetrics>) {
        self.hedge_metrics = Some(hedge_metrics);
    }

    /// Set the shared in-flight buffered-byte ledger for `/metrics` observability
    /// (`inflight_memory.*`). `HttpProxy::get_inflight_ledger()` holds the same
    /// `Arc`, mirroring how `set_request_semaphore` holds the permit semaphore.
    /// Requirement: 8.1-8.6
    pub fn set_inflight_ledger(&mut self, ledger: Arc<crate::inflight_ledger::InflightLedger>) {
        self.inflight_ledger = Some(ledger);
    }

    /// Set the configured `cache.max_ram_cache_size`, reported alongside the
    /// ledger so the two independent memory budgets are directly comparable.
    /// Requirement: 8.7
    pub fn set_ram_cache_max_bytes(&mut self, bytes: u64) {
        self.ram_cache_max_bytes = bytes;
    }

    /// Collect in-flight buffered-byte ledger stats.
    /// Returns a disabled snapshot (`ceiling_bytes: 0`) when the ledger has not
    /// been wired in, so `/metrics` always carries the section (Requirement 8.6).
    fn collect_inflight_memory_metrics(&self) -> InflightMemoryMetrics {
        match &self.inflight_ledger {
            Some(ledger) => InflightMemoryMetrics {
                reserved_bytes: ledger.reserved_bytes(),
                ceiling_bytes: ledger.ceiling_bytes(),
                peak_reserved_bytes: ledger.peak_reserved_bytes(),
                rejected_total: ledger.rejected_total(),
                aborted_accumulations_total: ledger.aborted_total(),
                granted_total: ledger.granted_total(),
                granted_bytes_total: ledger.granted_bytes_total(),
                ram_cache_max_bytes: self.ram_cache_max_bytes,
                buffered_serve_sliced_streams_total:
                    crate::http_proxy::buffered_serve_sliced_streams_total(),
                buffered_serve_degraded_chained_stream_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ChainedStream,
                    ),
                buffered_serve_degraded_forward_gap_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardGap,
                    ),
                buffered_serve_degraded_forward_partial_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardPartial,
                    ),
                buffered_serve_degraded_forward_unresolved_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardUnresolved,
                    ),
                buffered_serve_degraded_forward_stream_failed_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardStreamFailed,
                    ),
                buffered_serve_degraded_forward_partial_cache_merge_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardPartialCacheMerge,
                    ),
                buffered_serve_degraded_merge_backstop_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::MergeBackstop,
                    ),
                buffered_serve_size_le_64kib_total: crate::http_proxy::buffered_serve_size_buckets(
                )[0],
                buffered_serve_size_le_1mib_total: crate::http_proxy::buffered_serve_size_buckets()
                    [1],
                buffered_serve_size_le_8mib_total: crate::http_proxy::buffered_serve_size_buckets()
                    [2],
                buffered_serve_size_gt_8mib_total: crate::http_proxy::buffered_serve_size_buckets()
                    [3],
                buffered_serve_peak_materialised_bytes:
                    crate::http_proxy::buffered_serve_peak_materialised_bytes(),
                buffered_serve_full_object_total:
                    crate::http_proxy::buffered_serve_full_object_total(),
            },
            None => InflightMemoryMetrics {
                ram_cache_max_bytes: self.ram_cache_max_bytes,
                buffered_serve_sliced_streams_total:
                    crate::http_proxy::buffered_serve_sliced_streams_total(),
                buffered_serve_degraded_chained_stream_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ChainedStream,
                    ),
                buffered_serve_degraded_forward_gap_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardGap,
                    ),
                buffered_serve_degraded_forward_partial_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardPartial,
                    ),
                buffered_serve_degraded_forward_unresolved_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardUnresolved,
                    ),
                buffered_serve_degraded_forward_stream_failed_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardStreamFailed,
                    ),
                buffered_serve_degraded_forward_partial_cache_merge_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::ForwardPartialCacheMerge,
                    ),
                buffered_serve_degraded_merge_backstop_total:
                    crate::http_proxy::buffered_serve_degraded_total(
                        crate::http_proxy::BufferedServeDegradeReason::MergeBackstop,
                    ),
                buffered_serve_size_le_64kib_total: crate::http_proxy::buffered_serve_size_buckets(
                )[0],
                buffered_serve_size_le_1mib_total: crate::http_proxy::buffered_serve_size_buckets()
                    [1],
                buffered_serve_size_le_8mib_total: crate::http_proxy::buffered_serve_size_buckets()
                    [2],
                buffered_serve_size_gt_8mib_total: crate::http_proxy::buffered_serve_size_buckets()
                    [3],
                buffered_serve_peak_materialised_bytes:
                    crate::http_proxy::buffered_serve_peak_materialised_bytes(),
                buffered_serve_full_object_total:
                    crate::http_proxy::buffered_serve_full_object_total(),
                ..Default::default()
            },
        }
    }

    /// Collect write-cache (staging tier) observability metrics.
    ///
    /// `resident_bytes` is read from the Journal_Consolidator's in-memory
    /// Size_State (`SizeState::write_cache_size`) rather than from a separate
    /// walk, so it agrees with `cache.write_cache_size` and is fleet-wide.
    /// `inflight_bytes` and `staged_entries` come from the per-instance
    /// `WriteCacheManager` gauges. Returns all-zero when the cache manager is
    /// not wired in (mirrors `collect_inflight_memory_metrics`'s disabled shape).
    ///
    /// Also emits the Requirement 8.5 rate-limited WARN when
    /// `resident_bytes > staging_bound_bytes`. This is the natural single
    /// point for that check: both figures are already computed together
    /// here (rather than at the point Size_State's `write_cache_size` is
    /// updated in `journal_consolidator.rs`, which knows the new resident
    /// figure but not the Staging_Bound — that lives on `CacheManager`,
    /// reached only via `get_write_cache_capacity()`), and this function
    /// already runs on every `/metrics` poll, dashboard refresh, and OTLP
    /// export cycle, so the condition is checked as often as an operator or
    /// exporter would observe it.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.4, 8.5
    async fn collect_write_cache_metrics(&self, volume: &VolumeSource<'_>) -> WriteCacheMetrics {
        let Some(cache_manager) = &self.cache_manager else {
            return WriteCacheMetrics::default();
        };

        // The INCUMBENT counter, read for exactly two purposes: the Phase L gap gauge below, and
        // the fallback when there is no consolidator at all. It is deliberately NOT what
        // `resident_bytes` publishes any more — see the next statement.
        let incumbent_write_cache_size = match cache_manager.get_journal_consolidator().await {
            Some(consolidator) => consolidator
                .get_size_state_for_scrape()
                .await
                .map(|s| s.write_cache_size)
                .unwrap_or(0),
            None => 0,
        };
        // `resident_bytes` goes through the SINGLE resolution point, so it and the published
        // `cache.write_cache_size` cannot disagree.
        //
        // # This was reading the incumbent unconditionally, and its own field doc said otherwise
        //
        // The doc on `WriteCacheMetrics::resident_bytes` states that the derivation depends on
        // `staged_authority_epoch` and calls this "the same fleet-wide figure
        // `cache.write_cache_size` reports". It was not: this read the incumbent whatever the
        // epoch said, while `cache.write_cache_size` went through `CacheManager::staged_authority`
        // and became fold-derived at activation. After the flip the two `/metrics` fields
        // therefore diverged permanently, and `over_bound` below — plus its WARN — kept firing on
        // the retired over-stating producer, which over-states by a measured 4.28x-11.17x. Every
        // *decision* site was correctly gated, so this was observability rather than accounting,
        // but two divergent gauges for one figure plus a standing false alarm is worse than
        // either alone.
        //
        // Fixed as a gauge change rather than a doc change because the doc describes the
        // behaviour that was designed and the alarm is only meaningful against the authority.
        // `get_staging_resident_bytes` returns `None` only when there is no consolidator, in
        // which case the incumbent read above is 0 too.
        let resident_bytes = cache_manager
            .get_staging_resident_bytes()
            .await
            .unwrap_or(incumbent_write_cache_size);
        let staging_bound_bytes = cache_manager.get_write_cache_capacity();
        let (inflight_bytes, staged_entries) = cache_manager.get_write_cache_manager_gauges().await;
        let (staging_evictions_total, staging_eviction_bytes_total) =
            cache_manager.get_write_cache_eviction_counters().await;
        let graduations_total = cache_manager.get_write_cache_graduations_total().await;
        // `count_entries` FIRST, then the age — in that order, because the count may take a fresh
        // observation and an age read first would describe the figure that was replaced. This is
        // an observation of bounded age rather than a scrape-time count; see
        // `WriteLedger::count_entries` for why a whole-volume traversal per scrape was an
        // availability defect and not an optimisation.
        //
        // Background-derived: `count_entries` may traverse the whole ledger when its observation
        // has aged out, so only a full collection calls it. A request publishes the last full
        // collection's observation with its age advanced to now, or `null`/`null` before one.
        let since_collection = volume.age_secs();
        let (ledger_entries, ledger_entries_age_secs) = match volume {
            VolumeSource::Fresh(..) => match cache_manager.get_journal_consolidator().await {
                Some(consolidator) => {
                    let ledger = consolidator.write_ledger();
                    let count = ledger.count_entries().await;
                    (count, ledger.entry_count_age_secs().await)
                }
                None => (None, None),
            },
            VolumeSource::Snapshot(last) => match last {
                Some(l) => (
                    l.write_cache.ledger_entries,
                    aged(l.write_cache.ledger_entries_age_secs, since_collection),
                ),
                None => (None, None),
            },
        };
        // The fold's gauge set. The INCUMBENT is passed in, not `resident_bytes`, and that
        // distinction became load-bearing the moment `resident_bytes` started resolving through
        // the epoch: the gap gauge exists to compare the fold against the *other* producer, so
        // feeding it the fold's own figure would make it zero by construction at epoch 1 and
        // read as "no drift" rather than "nothing left to compare against". § 16.6.7 says the
        // gap is removed at activation because there is no second producer then; the gauge's own
        // doc records that it publishes `None` in that case.
        //
        // A request passes an EMPTY checkpoint set, which returns the function's `base` — every
        // per-process counter plus the small freshness-document read, and no pending or
        // `mpus_in_progress/` traversal — and then takes the checkpoint-derived figures from the
        // last full collection as a group. See `staged_fold_from_snapshot`.
        let staged_fold = match volume {
            VolumeSource::Fresh(shard_checkpoints, pending, part_bytes) => {
                crate::staged_fold::collect_staged_fold_gauges(
                    cache_manager.get_cache_dir(),
                    incumbent_write_cache_size,
                    pending,
                    shard_checkpoints,
                    part_bytes.bytes,
                )
                .await
            }
            VolumeSource::Snapshot(last) => {
                let live = crate::staged_fold::collect_staged_fold_gauges(
                    cache_manager.get_cache_dir(),
                    incumbent_write_cache_size,
                    &crate::staged_fold::PendingReadOutcome::default(),
                    &crate::staged_fold::ShardCheckpointSet::default(),
                    None,
                )
                .await;
                staged_fold_from_snapshot(
                    live,
                    last.map(|l| &l.write_cache.staged_fold),
                    since_collection,
                )
            }
        };
        // § 16.6.6 precondition 3b's progress and age, so an operator can watch the repair
        // rotation converge instead of only being told activation is blocked.
        //
        // # On a blocking pool, not on the async worker
        //
        // `collect_ledger_repair_gauges` is synchronous `std::fs`: one walk of `metadata/` and one
        // open + read + JSON parse per repair document, order of hundreds of small files on
        // NFS/EFS. Run inline it blocks a tokio worker thread for the whole traversal, so a slow
        // cache volume degrades the RUNTIME — every request that worker would otherwise have polled
        // — rather than just the scrape that asked for it. That is a strictly worse blast radius
        // than the lost sample, and it is reached by the same volume latency. Identical reasoning to
        // the `read_shard_checkpoints` move below; this is the sibling tree, and it was left behind.
        //
        // A `JoinError` publishes `LedgerRepairGauges::process_local()` and NOT
        // `Default::default()`. Eight of the fourteen fields — four process atomics and four
        // in-memory traversal estimates — do not touch the volume at all, so a blanket default
        // would publish four counters as `0` when they were readable and not zero, and
        // `ranges_reappended_total: 0` is the "no finding" reading of a field whose only purpose is
        // to surface a finding. The six volume-derived fields do take their defaults, which are the
        // UNREADABLE sentinels (`null`, and `false` for the activation predicate) rather than
        // reassuring figures. See that constructor for the field-by-field argument.
        //
        // "The task panicked" and "the volume has no repair documents" are different causes with
        // different owners, so the first is logged rather than folded silently into the second.
        //
        // Background-derived: only a full collection traverses. A request overlays the last full
        // collection's six volume figures onto the eight live per-process ones.
        let ledger_repair = if let VolumeSource::Snapshot(last) = volume {
            ledger_repair_from_snapshot(
                last.map(|l| &l.write_cache.ledger_repair),
                since_collection,
            )
        } else {
            let cache_dir = cache_manager.get_cache_dir().to_path_buf();
            match tokio::task::spawn_blocking(move || {
                crate::ledger_repair::collect_ledger_repair_gauges(&cache_dir)
            })
            .await
            {
                Ok(gauges) => gauges,
                Err(e) => {
                    warn!(
                        "Collecting the ledger-repair rotation gauges failed to complete: {}. The \
                         six volume-derived figures will report the unreadable sentinel for this \
                         collection rather than a zero, which would be indistinguishable from a \
                         rotation that has swept nothing; the per-process counters and estimates \
                         are unaffected and still published.",
                        e
                    );
                    crate::ledger_repair::LedgerRepairGauges::process_local()
                }
            }
        };
        let over_bound = resident_bytes > staging_bound_bytes;
        let activation_blocker =
            crate::staged_authority::activation_blocker(cache_manager.get_cache_dir());

        if over_bound {
            Self::log_over_bound_rate_limited(resident_bytes, staging_bound_bytes);
        }

        // In-memory: two mutex copies and four atomics, no shared-storage read.
        let scanner = match cache_manager.get_journal_consolidator().await {
            Some(consolidator) => {
                let (fold, repair) = consolidator.scanner_limiter_snapshots();
                ScannerGauges::from_limiters(&fold, &repair)
            }
            None => ScannerGauges::default(),
        };
        // The remedy is derived here, on both the fresh and the snapshot path, from the
        // published volume figures and the repair limiter's live state, so it always agrees
        // with the fields beside it.
        let ledger_repair = ledger_repair.with_rotation_remedy(
            scanner.scanner_concurrency_k_repair,
            scanner.scan_deferred_total_repair,
        );
        WriteCacheMetrics {
            resident_bytes,
            staging_bound_bytes,
            inflight_bytes,
            over_bound,
            staged_entries,
            staging_evictions_total,
            staging_eviction_bytes_total,
            graduations_total,
            ledger_entries,
            ledger_entries_age_secs,
            staged_fold,
            ledger_repair,
            scanner,
            ledger_torn_lines_quarantined_total: crate::write_ledger::torn_lines_quarantined_total(
            ),
            unledgered_staged_credits_refused_total:
                crate::disk_cache::unledgered_staged_credits_refused_total(),
            // From the latch rather than the document: this runs on every `/metrics` scrape,
            // the dashboard refresh and every OTLP export, and the consolidation cycle already
            // pays the shared-storage read once per cycle. § 16.6.6.
            staged_authority_epoch: crate::staged_authority::epoch_latched_for(
                cache_manager.get_cache_dir(),
            ),
            graduation_lock_unavailable_total: crate::cache::graduation_lock_unavailable_total(),
            setup_abandoned_total_put: crate::signed_put_handler::setup_abandoned_total_put(),
            mpu_completion_ledger_declines_total:
                crate::signed_put_handler::mpu_completion_ledger_declines_total(),
            graduation_noop_entries_total:
                crate::journal_consolidator::graduation_noop_entries_total(),
            journal_unparseable_lines_retained:
                crate::journal_consolidator::retained_unparseable_journal_lines(),
            staged_authority_foreign_write_total: crate::staged_authority::foreign_write_total(),
            pending_lock_waits_total: crate::staged_fold::pending_lock_wait_stats().0,
            pending_lock_wait_ms_total: crate::staged_fold::pending_lock_wait_stats().1,
            pending_lock_timeouts_total: crate::staged_fold::pending_lock_wait_stats().2,
            // In-memory, like the epoch latch above: the consolidator records the verdict it
            // evaluated, so a scrape pays no shared-storage read.
            staged_authority_activation_blocker: activation_blocker.0,
            staged_authority_activation_blocking_instance: activation_blocker.1,
            staged_authority_activation_blocking_instance_count:
                crate::staged_authority::activation_blocking_instance_count(
                    cache_manager.get_cache_dir(),
                ),
            // One small JSON read from the shared volume, the same shape and cost as
            // `cache.eviction_last_result`'s. The pass has been installing this document all
            // along; nothing read it back.
            // Spec: cache-eviction-at-scale § 16.6.12 point 5. Requirements: 12.1.
            count_pass_last_result: crate::eviction_result::WriteCacheCountPassResult::load(
                cache_manager.get_cache_dir(),
            ),
        }
    }

    /// Fold one finished collection into the process-wide figures, and return the snapshot to
    /// publish.
    ///
    /// Ordering is deliberate: every counter is updated *before* the snapshot is read, so the
    /// returned figures include this collection. A response that reports a peak excluding its own
    /// collection would be the one case where the peak is least trustworthy — see
    /// [`MetricsCollectionGauges::duration_ms_max`].
    ///
    /// `fetch_max` rather than a load-compare-store: `collect_metrics` is public and reentrant (the
    /// background collector and any direct caller), so two collections can finish concurrently and
    /// a read-then-write could drop the larger of the two peaks.
    fn note_collection_complete(
        elapsed: Duration,
        slow_threshold_ms: u64,
    ) -> MetricsCollectionGauges {
        use std::sync::atomic::Ordering;

        // Saturating, so a pathological stall cannot wrap the figure into a small, healthy-looking
        // number — which would hide exactly the collection worth seeing.
        let duration_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);

        let collections_total = COLLECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
        COLLECTION_DURATION_MS_LAST.store(duration_ms, Ordering::Relaxed);
        COLLECTION_DURATION_MS_MAX.fetch_max(duration_ms, Ordering::Relaxed);

        let slow_collections_total = if duration_ms > slow_threshold_ms {
            let total = COLLECTION_SLOW_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
            Self::log_slow_collection_rate_limited(duration_ms, slow_threshold_ms, total);
            total
        } else {
            COLLECTION_SLOW_TOTAL.load(Ordering::Relaxed)
        };

        MetricsCollectionGauges {
            collections_total,
            duration_ms_last: duration_ms,
            duration_ms_max: COLLECTION_DURATION_MS_MAX.load(Ordering::Relaxed),
            slow_collections_total,
            slow_threshold_ms,
        }
    }

    /// The process-wide collection figures as they stand, for a response that performed no
    /// collection of its own. Reads the same statics [`Self::note_collection_complete`] writes,
    /// without moving any of them.
    fn collection_gauges_now(slow_threshold_ms: u64) -> MetricsCollectionGauges {
        use std::sync::atomic::Ordering;
        MetricsCollectionGauges {
            collections_total: COLLECTIONS_TOTAL.load(Ordering::Relaxed),
            duration_ms_last: COLLECTION_DURATION_MS_LAST.load(Ordering::Relaxed),
            duration_ms_max: COLLECTION_DURATION_MS_MAX.load(Ordering::Relaxed),
            slow_collections_total: COLLECTION_SLOW_TOTAL.load(Ordering::Relaxed),
            slow_threshold_ms,
        }
    }

    /// One `warn!` per [`COLLECTION_SLOW_WARN_INTERVAL_SECS`] naming the duration and the threshold,
    /// on the same `AtomicU64` last-logged-second discipline as
    /// [`Self::log_over_bound_rate_limited`].
    ///
    /// The line names the likely cause rather than only the symptom, because the operator reading it
    /// cannot see the shared volume's latency from here and the collection's cost is almost entirely
    /// shared-storage reads.
    fn log_slow_collection_rate_limited(duration_ms: u64, threshold_ms: u64, slow_total: u64) {
        use std::sync::atomic::Ordering;
        let now_secs = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = LAST_COLLECTION_SLOW_WARN_SECS.load(Ordering::Relaxed);
        if now_secs >= last.saturating_add(COLLECTION_SLOW_WARN_INTERVAL_SECS)
            && LAST_COLLECTION_SLOW_WARN_SECS
                .compare_exchange(last, now_secs, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            warn!(
                "Background metrics collection took {}ms, over the {}ms threshold (half of \
                 metrics.collection_interval; {} slow collection(s) since start). The collection \
                 is mostly shared-volume I/O — the per-shard checkpoint and repair documents and \
                 the write-ledger count — so this usually means the cache volume is slow or the \
                 shard population has grown, rather than that the process is busy. /metrics still \
                 answers immediately; the shared-volume figures it serves are older, by the age \
                 shown in volume_snapshot.age_secs. A run longer than the interval skips the next \
                 tick.",
                duration_ms, threshold_ms, slow_total
            );
        }
    }

    /// Rate-limited WARN naming both Resident_Bytes and the Staging_Bound
    /// (Requirement 8.5): at most one `warn!` per
    /// `OVER_BOUND_WARN_INTERVAL_SECS` (60s), mirroring the
    /// `InflightLedger::log_rejection_rate_limited` / `CONCURRENCY_LAST_LOG`
    /// discipline elsewhere in this codebase — an AtomicU64 storing the last
    /// logged unix-second timestamp, checked-and-swapped to gate the line.
    fn log_over_bound_rate_limited(resident_bytes: u64, staging_bound_bytes: u64) {
        let now_secs = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last = LAST_OVER_BOUND_WARN_SECS.load(std::sync::atomic::Ordering::Relaxed);
        if now_secs >= last + OVER_BOUND_WARN_INTERVAL_SECS
            && LAST_OVER_BOUND_WARN_SECS
                .compare_exchange(
                    last,
                    now_secs,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
        {
            warn!(
                resident_bytes,
                staging_bound_bytes, "Write cache Resident_Bytes exceeds the Staging_Bound"
            );
        }
    }

    /// Collect hedging stats from the shared `HedgeMetrics` counters.
    /// Returns zeroes when hedge metrics have not been wired in.
    fn collect_hedging_metrics(&self) -> HedgingStats {
        match &self.hedge_metrics {
            Some(hm) => HedgingStats {
                issued: hm.issued.load(std::sync::atomic::Ordering::Relaxed),
                won: hm.won.load(std::sync::atomic::Ordering::Relaxed),
                suppressed: hm.suppressed.load(std::sync::atomic::Ordering::Relaxed),
            },
            None => HedgingStats::default(),
        }
    }

    /// Record a request completion with fixed-cardinality response and cache outcomes.
    ///
    /// `cache_hit` is supplied only for GET/HEAD requests. The method stores scalars
    /// rather than request paths, hosts, buckets, or object keys, so its memory use is
    /// fixed even when the proxy serves an unbounded object namespace.
    pub async fn record_response(
        &self,
        status: StatusCode,
        response_time: Duration,
        cache_hit: Option<bool>,
        rejected: bool,
    ) {
        let mut stats = self.request_stats.write().await;
        stats.total_requests = stats.total_requests.saturating_add(1);
        stats.total_response_time_ms = stats
            .total_response_time_ms
            .saturating_add(response_time.as_millis() as u64);

        if status.is_success() {
            stats.successful_requests = stats.successful_requests.saturating_add(1);
        } else {
            stats.failed_requests = stats.failed_requests.saturating_add(1);
        }
        if status.is_client_error() {
            stats.client_error_requests = stats.client_error_requests.saturating_add(1);
        }
        if status.is_server_error() {
            stats.server_error_requests = stats.server_error_requests.saturating_add(1);
        }
        if rejected {
            stats.rejected_requests = stats.rejected_requests.saturating_add(1);
        }
        match cache_hit {
            Some(true) => {
                stats.cache_hit_requests = stats.cache_hit_requests.saturating_add(1);
            }
            Some(false) => {
                stats.cache_miss_requests = stats.cache_miss_requests.saturating_add(1);
            }
            None => {}
        }
    }

    /// Perform one FULL collection: traverse the shared volume, assemble every section, cache the
    /// result, and record it for OTLP export.
    ///
    /// In production this is called by [`spawn_background_collector`] once per
    /// `metrics.collection_interval`, and by nothing on a request path. `/metrics` and the
    /// dashboard call [`Self::current_metrics`], which performs no traversal. Tests call this
    /// directly when they need every figure fresh.
    pub async fn collect_metrics(&self) -> SystemMetrics {
        // Started before any collection work, so the figure covers everything this collection
        // cost — including the shared-volume reads, which are nearly all of it. `Instant`, not
        // `SystemTime`: a wall-clock step would otherwise show up as a slow collection.
        let collection_started = std::time::Instant::now();

        // ONE traversal of the per-shard checkpoint set for the whole collection.
        //
        // # Why this is read here and not by the two collectors that need it
        //
        // `read_shard_checkpoints` is a `read_dir` of `checkpoints/`, then a `read_dir` per bucket,
        // then an open + read + JSON parse **per shard document** — order of hundreds of small
        // files on NFS/EFS. Two separate collectors each needed the set and each read it from its
        // own entry point, so a single scrape paid that traversal twice with no sharing:
        // `staged_fold::collect_staged_fold_gauges` for the staging figures, and
        // `st_size_fold::collect_st_size_fold_gauges` for the stalest scan age. The second one
        // entered the scrape path on 2026-09-20 and is what made an already-costly collection
        // materially worse.
        //
        // The body of `/metrics` is all-or-nothing — one `SystemMetrics`, one
        // `serde_json::to_string_pretty` — so a collection that outruns the client's deadline loses
        // **every** series at once rather than degrading. That is the failure this halves.
        //
        // # Sharing is strictly more correct, not merely cheaper
        //
        // Two traversals microseconds apart can straddle a peer's checkpoint install, so the
        // staging gauges and the scan age could describe two different memberships in one response.
        // One traversal makes that unconstructible. No published field changes value: both
        // consumers derive exactly what they derived before, from a set that is now shared instead
        // of read twice.
        //
        // `None` when there is no cache manager, which is the same condition under which both
        // consumers already returned their defaults.
        // # On a blocking pool, not on the async worker
        //
        // `read_shard_checkpoints` is synchronous `std::fs`, and it is the largest piece of
        // synchronous filesystem work in a collection. Run inline it blocks a tokio worker thread
        // for the whole traversal, so a slow cache volume degrades the RUNTIME — every request that
        // worker would otherwise have polled — rather than just the scrape that asked for it. That
        // is a strictly worse blast radius than the lost sample this unit started from, and it is
        // reached by the same volume latency.
        //
        // `tokio::fs` elsewhere in the collection is already `spawn_blocking` underneath, which is
        // why this is the call that needed moving and the write-ledger read did not.
        //
        // # The pending files are read HERE too, and FIRST
        //
        // The staged-fold gauges need the `.pending` files as well, and reading them after the
        // checkpoint set (inside the collector, as until the change) let a peer install plus the credit
        // owner's GC between the two reads drop every credit between the old and new cut from both
        // sources. `read_gauge_inputs` reads pending, then the checkpoint set on the blocking pool,
        // and owns the `JoinError` handling: a failed traversal yields the default set, which both
        // consumers publish as the unreadable sentinel rather than a zero.
        let (pending, shard_checkpoints) = match self.cache_manager.as_ref() {
            Some(cache_manager) => {
                crate::staged_fold::read_gauge_inputs(cache_manager.get_cache_dir()).await
            }
            None => Default::default(),
        };

        // `P`, walked ONCE for the whole collection, after the pending files and the checkpoint
        // set, the order the staged-fold gauges have always read them in. The cache section and
        // the staged-fold gauge set both publish this one figure, and a `/metrics` request or a
        // dashboard refresh serves it from this collection with its age, so neither walks.
        // `observe_in_progress` runs the walk on the blocking pool, spread over its own threads,
        // and joins a deciding reader's walk if one is already running.
        let part_bytes = match self.cache_manager.as_ref() {
            Some(cache_manager) => {
                let observation = crate::signed_put_handler::observe_in_progress(
                    cache_manager.get_cache_dir(),
                    Duration::ZERO,
                )
                .await;
                // The reconciliation, here and only here: this walk against the figure the
                // previous walk's composition supports. It reads no volume state, only the two
                // kept walks, and it is what arms the verdict that sends deciders back to the
                // walk when a published counter has missed an update.
                let reconciliation = crate::signed_put_handler::reconcile_in_progress_figures(
                    cache_manager.get_cache_dir(),
                );
                PartBytesReading {
                    bytes: observation.walk.bytes,
                    age_secs: observation.walk.bytes.map(|_| observation.age_secs()),
                    drift_bytes: reconciliation.drift_bytes,
                }
            }
            None => PartBytesReading::default(),
        };

        let mut metrics = self
            .assemble(VolumeSource::Fresh(
                &shard_checkpoints,
                &pending,
                part_bytes,
            ))
            .await;
        metrics.volume_snapshot = VolumeSnapshotGauges {
            collected_at_unix_secs: Some(
                metrics
                    .timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
            age_secs: Some(0),
            interval_secs: self.collection_interval.as_secs(),
        };

        // Recorded BEFORE the result is cached and before the OTLP export below, so the cached
        // copy — and the OTLP sample derived from it — report this collection rather than the
        // previous one.
        metrics.metrics_collection = Self::note_collection_complete(
            collection_started.elapsed(),
            collection_slow_threshold_ms(self.collection_interval),
        );

        // Cache the result. This is the snapshot `current_metrics` serves the shared-volume
        // figures from.
        {
            let mut last_metrics = self.last_metrics.write().await;
            *last_metrics = Some(metrics.clone());
        }

        // Record into the OTLP instruments. The exporter's periodic reader sends whatever was last
        // recorded, so this is what makes OTLP export independent of anyone requesting /metrics.
        if let Some(otlp_exporter) = &self.otlp_exporter {
            if let Err(e) = otlp_exporter.read().await.export_metrics(&metrics).await {
                warn!("Failed to export metrics to OTLP: {}", e);
            }
        }

        metrics
    }

    /// The metrics a `/metrics` request or dashboard refresh returns: every in-process section
    /// assembled now, and the shared-volume figures from the last full collection.
    ///
    /// Performs **no** shared-volume traversal and records nothing for OTLP. Its cost is the
    /// in-process reads plus a handful of single small-file reads (`size_state.json` and its
    /// siblings) that do not scale with the cache population. See [`VolumeSnapshotGauges`] for
    /// which figures are background-derived and how their age is published.
    pub async fn current_metrics(&self) -> SystemMetrics {
        let last = self.get_cached_metrics().await;
        // As a scrape: an expired staged-authority figure is served while one background refresh
        // re-derives it, rather than re-derived here behind `.pending` locks and a checkpoint
        // traversal. See `staged_fold::as_scrape`.
        let mut metrics =
            crate::staged_fold::as_scrape(self.assemble(VolumeSource::Snapshot(last.as_ref())))
                .await;
        metrics.volume_snapshot = VolumeSnapshotGauges {
            collected_at_unix_secs: last.as_ref().map(|l| {
                l.timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            }),
            age_secs: last.as_ref().map(snapshot_age_secs),
            interval_secs: self.collection_interval.as_secs(),
        };
        metrics.metrics_collection =
            Self::collection_gauges_now(collection_slow_threshold_ms(self.collection_interval));
        metrics
    }

    /// Assemble a `SystemMetrics`. Every section is read now except the shared-volume figures,
    /// whose source `volume` names. `metrics_collection` and `volume_snapshot` are left at their
    /// defaults for the caller to fill in.
    async fn assemble(&self, volume: VolumeSource<'_>) -> SystemMetrics {
        let uptime = self
            .start_time
            .elapsed()
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        // Collect cache metrics
        let cache_metrics = if let Some(cache_manager) = &self.cache_manager {
            self.collect_cache_metrics(cache_manager, &volume).await
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

        // Collect cache size metrics.
        //
        // Read through the cache manager at COLLECTION time rather than from a
        // tracker Arc wired in at startup. The tracker does not exist until
        // `CacheManager::initialize()` runs (`cache.rs`, the sole site that
        // populates `size_tracker`), and that happens inside
        // `HttpProxy::start()` — which `main()` spawns hundreds of lines after
        // it finishes wiring this struct. A startup wiring step therefore
        // observed `None` unconditionally and never retried, so `cache_size`
        // serialized as `null` for the entire life of every process from v1.1.x
        // until this was fixed. It read as the documented "component not wired
        // in" degraded state, which is why it went unnoticed.
        //
        // `get_cache_size_metrics()` re-reads the tracker on every call, so this
        // cannot go stale and cannot depend on startup ordering. It returns
        // `None` only while the cache manager genuinely has no tracker yet —
        // i.e. before initialization completes — which is the honest meaning of
        // a `null` here.
        let cache_size_metrics = match &self.cache_manager {
            Some(cache_manager) => cache_manager.get_cache_size_metrics().await,
            None => None,
        };

        // Collect consolidation metrics from journal consolidator
        // Requirements: 3.3, 3.4, 3.5 - Dashboard and metrics integration
        let consolidation_metrics = if let Some(cache_manager) = &self.cache_manager {
            self.collect_consolidation_metrics(cache_manager).await
        } else {
            None
        };

        // Collect cache-hit update buffer metrics. Read through the cache manager at COLLECTION
        // time for the same reason `cache_size` above is: the buffer is created by
        // `create_configured_disk_cache_manager`, which runs well after `main()` finishes wiring
        // this struct, so a startup wiring step would observe `None` forever.
        let cache_hit_buffer_metrics = match &self.cache_manager {
            Some(cache_manager) => Self::collect_cache_hit_buffer_metrics(cache_manager).await,
            None => None,
        };

        // Collect coalescing metrics
        // Requirements: 19.1, 19.2, 19.3, 19.4, 19.5
        let coalescing_metrics = self.collect_coalescing_metrics().await;

        // Collect page-aligned range caching (widening) metrics.
        // Spec: page-aligned-range-cache. Requirements: 8.1-8.6
        let page_cache_metrics = self.collect_page_cache_metrics().await;

        // Collect cache rules health metrics
        let cache_rules_metrics = self
            .cache_manager
            .as_ref()
            .map(|cache_manager| cache_manager.get_bucket_settings_manager().rules_health());

        // Collect request metrics
        let request_metrics = self.collect_request_metrics().await;

        // Collect per-bucket traffic stats (string-keyed for JSON).
        // Spec: per-bucket-metrics. Requirements: 3.1, 3.3
        let bucket_traffic = self.get_bucket_traffic_stats_json().await;

        SystemMetrics {
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
            cache_hit_buffer: cache_hit_buffer_metrics,
            fill_visibility: self.cache_manager.as_ref().map(|cache_manager| {
                FillVisibilityMetrics::from(cache_manager.fill_visibility_tracker().stats())
            }),
            orphan_reclaim: OrphanReclaimMetrics::current(),
            eviction_worklist: self
                .cache_manager
                .as_ref()
                .map(|cache_manager| {
                    EvictionWorklistMetrics::current(cache_manager.get_cache_dir())
                })
                .unwrap_or_default(),
            coalescing: coalescing_metrics,
            cache_rules: cache_rules_metrics,
            page_cache: page_cache_metrics,
            request_metrics,
            bucket_traffic,
            download_bandwidth: crate::bandwidth_limiter::global_limiter().snapshot(),
            hedged_requests: self.collect_hedging_metrics(),
            inflight_memory: self.collect_inflight_memory_metrics(),
            write_cache: self.collect_write_cache_metrics(&volume).await,
            // Atomic loads off the one `WriteLedger` this process constructs, so this costs no
            // I/O. Zeroes with no cache manager wired in, which is also the pre-first-append
            // reading, and a build with no write ledger has no batches to report either way.
            write_ledger: match self.cache_manager.as_ref() {
                Some(cache_manager) => match cache_manager.get_journal_consolidator().await {
                    Some(consolidator) => consolidator.write_ledger().stage_metrics(),
                    None => crate::write_ledger::WriteLedgerMetrics::default(),
                },
                None => crate::write_ledger::WriteLedgerMetrics::default(),
            },
            // Both filled in by the caller: `collect_metrics` once the collection it describes has
            // finished, `current_metrics` from the last full collection. Defaults rather than
            // `Option`s because the figures are always knowable by then.
            metrics_collection: MetricsCollectionGauges::default(),
            volume_snapshot: VolumeSnapshotGauges::default(),
        }
    }

    /// Collect cache-specific metrics
    async fn collect_cache_metrics(
        &self,
        cache_manager: &Arc<CacheManager>,
        volume: &VolumeSource<'_>,
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
        let ram_stats = cache_manager.ram_cache_stats().await;
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

        // `None` only on the degraded path above, where `get_cache_size_stats` failed and
        // we fell back to the stored statistics. Reported as zeros, which is what this
        // endpoint has always done in that case — the WARN above is the signal, not these
        // fields. `unwrap_or_default` is safe here precisely because it is unreachable on
        // the success path: `get_cache_size_stats` always returns `Some`.
        let sizes = stats.sizes.unwrap_or_default();

        // `statvfs`-derived consumed bytes on the cache volume, through the shared
        // helper so the fail-open posture and the not-the-capacity-authority caveat are
        // stated in one place rather than duplicated per call site. Fail-open to 0 here
        // because this endpoint publishes a number.
        // Spec: cache-eviction-at-scale. Requirements: 8.2.
        let consumed_bytes =
            crate::cache::volume_consumed_bytes(cache_manager.get_cache_dir()).unwrap_or(0);

        // Background-derived: the collection's one walk of `mpus_in_progress/`, or, on a request,
        // the last collection's figure with its age advanced to now.
        let part_bytes = match volume {
            VolumeSource::Fresh(_, _, reading) => *reading,
            VolumeSource::Snapshot(last) => {
                let last = last.and_then(|l| l.cache.as_ref());
                PartBytesReading {
                    bytes: last.and_then(|c| c.in_progress_part_bytes),
                    age_secs: aged(
                        last.and_then(|c| c.in_progress_part_bytes_age_secs),
                        volume.age_secs(),
                    ),
                    drift_bytes: last.and_then(|c| c.in_progress_part_bytes_drift_bytes),
                }
            }
        };

        Some(CacheMetrics {
            total_cache_size: sizes.total_cache_size,
            // Genuinely maintained on the stored statistics (set in the CacheManager
            // constructor), so unlike the size figures this one is valid on both paths.
            max_cache_size_limit: stats.max_cache_size_limit,
            read_cache_size: sizes.read_cache_size,
            read_cache_size_inverted_secs: sizes.read_cache_size_inverted_secs,
            write_cache_size: sizes.write_cache_size,
            ram_cache_size: stats.ram_cache_size,
            // Read here rather than from `CacheSizes`, so that none of the three
            // composite figures above can pick it up as a side effect. A walk of
            // in-flight uploads only — see `in_progress_part_bytes` for why its cost
            // does not scale with cache population.
            // Spec: cache-eviction-at-scale § 16.6.2.
            in_progress_part_bytes: part_bytes.bytes,
            in_progress_part_bytes_age_secs: part_bytes.age_secs,
            in_progress_part_bytes_drift_bytes: part_bytes.drift_bytes,
            // Read from shared storage, so every instance reports the same last pass
            // regardless of which one held the eviction lock.
            // Spec: cache-eviction-at-scale §§ 16.6.2, 16.6.7.
            eviction_last_result: crate::eviction_result::EvictionPassResult::load(
                cache_manager.get_cache_dir(),
            ),
            // Two process-local atomics plus the stalest scan age, derived from the checkpoint set
            // this collection already holds. It used to read that set itself, which is the
            // traversal `collect_metrics` now pays once for the whole collection — see the comment
            // on `shard_checkpoints` there. The pass that produces these figures is the only writer
            // of the per-shard byte aggregate, and until these were published its characteristic
            // failure — declining every tick on the overlap guard — was invisible.
            //
            // Background-derived for the scan age only. A request passes an empty set, which
            // yields the two live atomics and a `None` age, and then takes the age from the last
            // full collection, advanced to now.
            st_size_fold: match volume {
                VolumeSource::Fresh(shard_checkpoints, ..) => {
                    crate::st_size_fold::collect_st_size_fold_gauges(shard_checkpoints)
                }
                VolumeSource::Snapshot(last) => crate::st_size_fold::StSizeFoldGauges {
                    st_size_fold_oldest_shard_scan_age_secs: aged(
                        last.and_then(|l| l.cache.as_ref())
                            .and_then(|c| c.st_size_fold.st_size_fold_oldest_shard_scan_age_secs),
                        volume.age_secs(),
                    ),
                    ..crate::st_size_fold::collect_st_size_fold_gauges(
                        &crate::staged_fold::ShardCheckpointSet::default(),
                    )
                },
            },
            // Two process-local atomics carrying the configured floor and cap, two more counting
            // this process's rebuilds, and one read of the per-instance tail-delta files. The
            // floor and cap are published so an operator can confirm from outside the process
            // that the values configured are the values the rebuild reads.
            // Spec: cache-eviction-at-scale § 4.2, § 4.4.
            tail_partition: crate::cache_size_tracker::collect_tail_partition_gauges(
                cache_manager.get_cache_dir(),
            ),
            // The selection side: process-local counters plus the coverage ratio the last
            // selection pass computed. Nothing here reads the filesystem.
            // Spec: cache-eviction-at-scale § 4.2, § 4.5, § 5.3, § 5.4.
            tail_selection: crate::cache::collect_tail_selection_gauges(),
            eviction_executor: crate::cache::collect_eviction_executor_gauges(),
            // R12.3's rates. Two process-local accumulator reads plus the three backlog terms —
            // no filesystem access, because every numerator and denominator is either an atomic or
            // a figure already read above.
            //
            // `uptime` is recomputed here rather than threaded in from `collect_metrics`: it is the
            // ADMISSION denominator only, and the shed denominator is `active_micros_total`. See
            // `EvictionRateGauges` for why those differ.
            // Spec: cache-eviction-at-scale § 8. Requirements: 2.3, 12.3.
            eviction_rates: EvictionRateGauges::build(
                crate::cache::collect_eviction_shed_accumulator(),
                crate::journal_consolidator::collect_admission_accumulator(),
                self.start_time
                    .elapsed()
                    .unwrap_or(Duration::from_secs(0))
                    .as_secs(),
                sizes.total_cache_size,
                // The figure the trigger reads, from a process-local atomic the trigger path
                // refreshes. No shared-volume I/O is added to a scrape, fresh or snapshot: this
                // is the same load on both paths.
                crate::journal_consolidator::last_projected_cache_size(),
                stats.max_cache_size_limit,
                cache_manager.eviction_trigger_percent(),
            ),
            cache_hit_rate_percent: hit_rate,
            ram_cache_hit_rate_percent: ram_hit_rate_percent(stats.ram_cache_hit_rate),
            total_requests,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            incomplete_range_fallbacks: stats.incomplete_range_fallbacks,
            evictions: stats.evicted_entries,
            corruption_metadata_total: error_stats.corruption_metadata_total,
            corruption_missing_range_total: error_stats.corruption_missing_range_total,
            disk_full_events_total: disk_full_events_total(),
            metadata_lock_retry_exhausted_total:
                crate::metadata_lock_manager::metadata_lock_retry_exhausted_total(),
            tee_backpressure_events_total_get: crate::tee_stream::backpressure_events_total_get(),
            tee_backpressure_wait_ms_total_get: crate::tee_stream::backpressure_wait_ms_total_get(),
            tee_backpressure_events_total_put:
                crate::signed_request_proxy::tee_backpressure_events_total_put(),
            tee_backpressure_wait_ms_total_put:
                crate::signed_request_proxy::tee_backpressure_wait_ms_total_put(),
            tee_abandoned_total_put: crate::signed_put_handler::tee_abandoned_total_put(),
            metadata_heal_overwrite_total: crate::disk_cache::metadata_heal_overwrite_total(),
            upstream_idle_abort_total_mid_stream:
                crate::tee_stream::upstream_idle_abort_total_mid_stream(),
            upstream_idle_abort_total_pre_stream:
                crate::tee_stream::upstream_idle_abort_total_pre_stream(),
            upstream_idle_retry_total: crate::tee_stream::upstream_idle_retry_total(),

            dirent_type_resolution_failures_total:
                crate::disk_cache::dirent_type_resolution_failures_total(),
            metadata_read_stale_handles_total: crate::disk_cache::metadata_read_stale_handles_total(
            ),
            metadata_read_stale_handle_misses_total:
                crate::disk_cache::metadata_read_stale_handle_misses_total(),
            // New range storage metrics
            metadata_parse_duration_ms: avg_metadata_parse_duration,
            metadata_file_size_bytes: range_stats.metadata_file_size_bytes,
            range_file_count: range_stats.range_file_count,
            range_load_duration_ms: avg_range_load_duration,
            // Cache bypass metrics by reason
            cache_bypasses_by_reason: error_stats.cache_bypasses_by_reason.clone(),
            origin_marker_mismatch_bypasses_total:
                crate::origin_identity::origin_marker_mismatch_bypasses_total(),
            origin_marker_unparseable_bypasses_total:
                crate::origin_identity::origin_marker_unparseable_bypasses_total(),
            origin_marker_unresolved_bypasses_total:
                crate::origin_identity::origin_marker_unresolved_bypasses_total(),
            uncacheable_aws_host_bypasses_total:
                crate::origin_identity::uncacheable_aws_host_bypasses_total(),
            write_stamp_retried_total: crate::cache::write_stamp_retried_total(),
            write_stamp_retry_landed_total: crate::cache::write_stamp_retry_landed_total(),
            write_stamp_retry_overflow_keys: crate::cache::write_stamp_retry_overflow_keys(),
            write_stamp_retry_active_keys: crate::cache::write_stamp_retry_active_keys(),
            write_stamp_retry_abandoned_total: crate::cache::write_stamp_retry_abandoned_total(),
            write_stamp_log_pending_keys: crate::stamp_backlog::log_pending_keys(),
            write_stamp_log_replayed_total: crate::stamp_backlog::log_replayed_total(),
            write_stamp_log_torn_records_total: crate::stamp_backlog::log_torn_records_total(),
            write_stamp_log_write_failures_total: crate::stamp_backlog::log_write_failures_total(),
            write_stamp_log_memory_only: crate::stamp_backlog::log_memory_only(),
            write_stamp_pending_declined_total: crate::cache::write_stamp_pending_declined_total(),
            volume_stall_breaker_open: crate::volume_stall::breaker_open(),
            volume_stall_breaker_trips_total: crate::volume_stall::breaker_trips_total(),
            // Cache coherency metrics
            cache_etag_validations_total: error_stats.cache_etag_validations_total,
            cache_etag_mismatches_total: error_stats.cache_etag_mismatches_total,
            cache_range_invalidations_total: error_stats.cache_range_invalidations_total,
            cache_orphaned_ranges_cleaned_total: error_stats.cache_orphaned_ranges_cleaned_total,
            cache_cleanup_failures_total: error_stats.cache_cleanup_failures_total,
            // Part caching metrics - Requirements 8.1, 8.2, 8.3, 8.4, 8.5
            cache_part_hits: error_stats.cache_part_hits,
            cache_part_misses: error_stats.cache_part_misses,
            cache_part_stores: error_stats.cache_part_stores,
            cache_part_evictions: error_stats.cache_part_evictions,
            cache_part_errors: error_stats.cache_part_errors,
            // Write cache metrics - Requirement 11.4
            incomplete_uploads_evicted: stats.incomplete_uploads_evicted,
            // Per-tier RAM cache stats
            ram_cache_hits: ram_stats.as_ref().map_or(0, |r| r.hit_count),
            ram_cache_misses: ram_stats.as_ref().map_or(0, |r| r.miss_count),
            ram_cache_evictions: ram_stats.as_ref().map_or(0, |r| r.eviction_count),
            ram_cache_max_size: ram_stats.as_ref().map_or(0, |r| r.max_size),
            ram_cache_entries: ram_stats.as_ref().map_or(0, |r| r.entries_count),
            ram_cache_eligibility_skips_total: cache_manager.ram_eligibility_skips_total(),
            // Per-tier metadata cache stats
            metadata_cache_hits: metadata_snap.hits,
            metadata_cache_misses: metadata_snap.misses,
            metadata_cache_entries: metadata_entries,
            metadata_cache_max_entries: metadata_max_entries,
            metadata_cache_evictions: metadata_snap.evictions,
            metadata_cache_stale_refreshes: metadata_snap.stale_refreshes,
            // The HEAD path's own three. The snapshot has counted these since the tier was
            // written; nothing published them, so an operator asking whether the metadata tier
            // works for HEAD traffic had only the all-request aggregate above.
            metadata_cache_head_hits: metadata_snap.head_hits,
            metadata_cache_head_misses: metadata_snap.head_misses,
            metadata_cache_head_disk_hits: metadata_snap.head_disk_hits,
            // The non-HEAD disk reads after a RAM miss. Stale file handles are counted where
            // they happen, at the `.meta` reader — see `metadata_read_stale_handles_total`.
            metadata_cache_disk_hits: metadata_snap.disk_hits,
            // Bytes served from cache
            bytes_served_from_cache: stats.bytes_served_from_cache,
            // Eager invalidation and revalidation counters
            read_cache_disabled_invalidations_total: error_stats
                .read_cache_disabled_invalidations_total,
            ttl_revalidations_total: error_stats.ttl_revalidations_total,
            // Real volume-consumption reading, fail-open to 0 on error — see the field
            // doc. Compare against `total_cache_size` (above) for divergence, keeping in
            // mind the scope mismatch documented there.
            // Spec: cache-eviction-at-scale. Requirements: 8.2.
            consumed_bytes,
            // Registry read only: no statvfs, and no guard is created by a scrape.
            capacity_guard: crate::capacity_guard::collect_capacity_guard_gauges(
                cache_manager.disk_safety_free_space_floor(),
            ),
            // One atomic load. The read-fill soft stop's counter.
            read_fills_declined_max_cache_size_total: crate::cache::read_fill_declines_total(),
            // One atomic load. The unique-hostname prerequisite's detector.
            hostname_collisions_total: crate::cache::hostname_collisions_total(),
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
        connection_pool: &Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    ) -> Option<ConnectionPoolMetrics> {
        let pool_manager = connection_pool.read().await;

        let dns_refresh_count = pool_manager.get_dns_refresh_count();

        // Get keepalive stats (tracked at Hyper level, not ConnectionPoolManager level)
        let keepalive_stats = self.connection_keepalive_stats.read().await;
        let connections_created = keepalive_stats.connections_created.clone();
        let connections_reused = keepalive_stats.connections_reused.clone();
        let idle_timeout_closures = keepalive_stats.idle_timeout_closures;
        let max_lifetime_closures = keepalive_stats.max_lifetime_closures;
        let error_closures = keepalive_stats.error_closures;
        drop(keepalive_stats);

        // Get IP addresses from distributors
        let stats = pool_manager.get_ip_distribution_stats();
        let ip_addresses: Vec<String> = stats
            .endpoints
            .iter()
            .flat_map(|e| e.ips.iter().map(|ip| ip.ip.clone()))
            .collect();

        Some(ConnectionPoolMetrics {
            success_rate_percent: 1.0,
            dns_refresh_count,
            ip_addresses,
            connections_created,
            connections_reused,
            idle_timeout_closures,
            max_lifetime_closures,
            error_closures,
        })
    }

    /// Collect request processing metrics
    async fn collect_request_metrics(&self) -> RequestMetrics {
        let stats = self.request_stats.read().await;

        let average_response_time_ms = stats
            .total_response_time_ms
            .checked_div(stats.total_requests)
            .unwrap_or(0);

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

        // permits_held = total - available_permits(); available_permits() is a gauge,
        // so held is derived rather than tracked. Requirement: TCA 5.1, 5.2
        let permits_available = self
            .request_semaphore
            .as_ref()
            .map(|s| s.available_permits() as u64)
            .unwrap_or(0);
        let permits_total = self.max_concurrent_requests as u64;
        let permits_held = permits_total.saturating_sub(permits_available);

        RequestMetrics {
            total_requests: stats.total_requests,
            successful_requests: stats.successful_requests,
            failed_requests: stats.failed_requests,
            client_error_requests: stats.client_error_requests,
            server_error_requests: stats.server_error_requests,
            rejected_requests: stats.rejected_requests,
            cache_hit_requests: stats.cache_hit_requests,
            cache_miss_requests: stats.cache_miss_requests,
            average_response_time_ms,
            requests_per_second,
            active_requests: self
                .active_connections
                .as_ref()
                .map(|c| c.load(std::sync::atomic::Ordering::Relaxed) as u64)
                .unwrap_or(0),
            max_concurrent_requests: self.max_concurrent_requests as u64,
            permits_total,
            permits_held,
            permits_available,
            permits_held_peak: crate::http_proxy::permits_held_peak(),
            access_log_records_dropped: crate::logging::access_log_records_dropped_total(),
        }
    }

    /// Collect consolidation metrics from journal consolidator
    /// Requirements: 3.3, 3.4, 3.5 - Dashboard and metrics integration
    /// Read the cache-hit update buffer's own figures for the `cache_hit_buffer` section.
    ///
    /// `None` when the cache manager has no buffer, which is the honest meaning of a `null`
    /// section: nothing to report rather than nothing happening. See [`CacheHitBufferMetrics`]
    /// for why that distinction is the point of this section.
    ///
    /// An associated function rather than a method because it reads nothing off `self`; the
    /// caller already holds the cache manager.
    async fn collect_cache_hit_buffer_metrics(
        cache_manager: &Arc<CacheManager>,
    ) -> Option<CacheHitBufferMetrics> {
        let stats = cache_manager
            .get_cache_hit_update_buffer()
            .await?
            .get_stats()
            .await;
        Some(CacheHitBufferMetrics {
            pending_updates: stats.buffer_size as u64,
            updates_recorded_total: stats.updates_recorded,
            entries_flushed_total: stats.entries_flushed,
            flush_errors_total: stats.flush_errors,
            ram_records_dropped_total: cache_manager.ram_access_records_dropped(),
        })
    }

    async fn collect_consolidation_metrics(
        &self,
        cache_manager: &Arc<CacheManager>,
    ) -> Option<ConsolidationMetrics> {
        // Get the journal consolidator from cache manager
        let consolidator = cache_manager.get_journal_consolidator().await?;

        // `None` is an unreadable size state (the file exists but could not be read or
        // parsed). Publish no section rather than a document of zeros, which reads as an empty
        // cache; `/metrics` then carries `"consolidation": null`, as with no consolidator.
        // The consolidator's last in-memory reading, so a scrape reads no shared storage.
        let size_state = consolidator.get_size_state_for_scrape().await?;

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
            consolidation_lock_held_secs: consolidator.consolidation_lock_held_secs(),
            consolidation_lock_takeovers: consolidator.consolidation_lock_takeovers(),
        })
    }

    /// Collect page-aligned range caching (widening) metrics.
    /// Spec: page-aligned-range-cache. Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6
    async fn collect_page_cache_metrics(&self) -> PageCacheMetrics {
        let stats = self.page_cache_stats.read().await;

        let amplification_ratio = if stats.bytes_requested > 0 {
            (stats.bytes_requested + stats.bytes_prefetched) as f64 / stats.bytes_requested as f64
        } else {
            1.0
        };

        PageCacheMetrics {
            widened_requests: stats.widened_requests,
            bytes_prefetched: stats.bytes_prefetched,
            amplification_ratio,
            page_hits: stats.page_hits,
            skipped_signed_range: stats.skipped_signed_range,
            fallbacks: stats.fallbacks,
            ram_page_promotions: stats.ram_page_promotions,
            ram_page_promotion_skipped: stats.ram_page_promotion_skipped,
        }
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
            waiter_conditional_304: stats.waiter_conditional_304,
            waiter_conditional_200: stats.waiter_conditional_200,
            waiter_conditional_4xx: stats.waiter_conditional_4xx,
            waiter_conditional_error: stats.waiter_conditional_error,
        })
    }

    /// Handle metrics HTTP request
    pub async fn handle_metrics_request(
        &self,
        _req: Request<hyper::body::Incoming>,
    ) -> Result<Response<String>> {
        // No shared-volume traversal on the request path: the background collector owns it. See
        // `current_metrics` and `VolumeSnapshotGauges`.
        let metrics = self.current_metrics().await;

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

    /// Get error statistics. Test seam: `/metrics` reads `error_stats` directly.
    #[cfg(test)]
    pub(crate) async fn get_error_stats(&self) -> ErrorStats {
        self.error_stats.read().await.clone()
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
    /// Collect eviction coordination metrics
    /// Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
    async fn collect_eviction_coordination_metrics(&self) -> Option<EvictionCoordinationMetrics> {
        let stats = self.eviction_coordination_stats.read().await;

        Some(EvictionCoordinationMetrics {
            lock_acquisitions_successful: stats.lock_acquisitions_successful,
            lock_acquisitions_failed: stats.lock_acquisitions_failed,
            // Read from the process-wide atomic the takeover site increments, because that site
            // cannot await. `get_eviction_coordination_stats` returns the internal `stats`, which
            // do not carry this figure, so this published field is the only place it appears.
            stale_locks_recovered: stale_eviction_locks_recovered(),
            total_lock_hold_time_ms: stats.total_lock_hold_time_ms,
            evictions_coordinated: stats.evictions_coordinated,
            evictions_skipped_lock_held: stats.evictions_skipped_lock_held,
            // Populated and, until now, unserialised: the field existed on the internal stats
            // struct and was documented as a published counter, so a consumer reading it got
            // `null`.
            staging_evictions_skipped_lock_held: stats.staging_evictions_skipped_lock_held,
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

    /// Record a **staging** eviction pass skipped because another instance held the
    /// global eviction lock (R7.4).
    ///
    /// Deliberately not folded into `record_eviction_skipped_lock_held` — see the field
    /// doc on `EvictionCoordinationStats::staging_evictions_skipped_lock_held`.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 7.4
    pub async fn record_staging_eviction_skipped_lock_held(&self) {
        let mut stats = self.eviction_coordination_stats.write().await;
        stats.staging_evictions_skipped_lock_held += 1;
        debug!(
            "Recorded staging eviction skipped (lock held) (total: {})",
            stats.staging_evictions_skipped_lock_held
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
            skipped_puts_total: stats.skipped_puts_by_reason.clone(),
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

    /// Record an upload that was not write-through cached, keyed by reason.
    ///
    /// Covers the otherwise-silent skip sites — caching disabled by rule
    /// (`write_cache_disabled`), decoded length unknown (`unknown_length`),
    /// no room on the cache volume (`disk_safety`), object too large
    /// (`object_too_large`), and post-S3 commit failure (`commit_failed`) —
    /// so the fleet reports something other than all zeros when write-through
    /// caching has stopped. Callers keep their existing `debug!` lines; this is
    /// purely additive observability.
    ///
    /// # Arguments
    /// * `reason` - A stable reason key (see `docs/METRICS_REFERENCE.md` for
    ///   the maintained list of keys)
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.1
    pub async fn record_skipped_put(&self, reason: &str) {
        let mut stats = self.signed_put_stats.write().await;
        let count = stats
            .skipped_puts_by_reason
            .entry(reason.to_string())
            .or_insert(0);
        *count += 1;
        debug!(
            "Recorded skipped PUT: reason={} (total for this reason: {})",
            reason, count
        );
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

    /// Backward compatibility method for simple corrupted metadata recording
    pub async fn record_corrupted_metadata_simple(&self) {
        self.record_corrupted_metadata("unknown", "unknown", "unknown")
            .await;
    }

    /// Record corrupted metadata with a specific reason label (oversize, legacy, parse_failure).
    /// Spec: cache-metadata-resilience Req 3, Task 6
    pub async fn record_corrupted_metadata_with_reason(
        &self,
        cache_key: &str,
        file_path: &str,
        reason: &str,
    ) {
        let mut stats = self.error_stats.write().await;
        stats.corruption_metadata_total += 1;

        warn!(
            cache_key = %cache_key,
            file_path = %file_path,
            reason = %reason,
            total_corruptions = stats.corruption_metadata_total,
            "Metadata corruption detected (classified)"
        );
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
    /// Returns the current part caching metrics. Test seam, also used by `cache.rs`'s
    /// part-caching property test; `/metrics` reads `error_stats` directly.
    #[cfg(test)]
    pub(crate) async fn get_part_cache_stats(&self) -> (u64, u64, u64, u64, u64) {
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
        debug!("Recorded coalesce wait (total: {})", stats.waits_total);
    }

    /// Record a cache hit after waiting for coalescing
    /// Requirement 19.2: Track requests served from cache after waiting
    pub async fn record_coalesce_cache_hit(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.cache_hits_after_wait_total += 1;
        stats.s3_fetches_saved_total += 1;
        debug!(
            "Recorded coalesce cache hit (total hits: {}, fetches saved: {})",
            stats.cache_hits_after_wait_total, stats.s3_fetches_saved_total
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
            stats
                .wait_duration_sum_ms
                .checked_div(stats.wait_duration_count)
                .unwrap_or(0)
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

    /// Record a waiter's conditional request returning `304 Not Modified`.
    /// This is the "validated cache serve" outcome — the waiter's
    /// credentials were just validated by S3 and the cache entry is
    /// authoritative for the waiter.
    pub async fn record_coalesce_waiter_conditional_304(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.waiter_conditional_304 += 1;
        debug!(
            "Recorded coalesce waiter conditional 304 (total: {})",
            stats.waiter_conditional_304
        );
    }

    /// Record a waiter's conditional request returning `200 OK` — the
    /// object changed between the fetcher's commit and the waiter's
    /// validation, and S3 returned a fresh body.
    pub async fn record_coalesce_waiter_conditional_200(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.waiter_conditional_200 += 1;
        debug!(
            "Recorded coalesce waiter conditional 200 (total: {})",
            stats.waiter_conditional_200
        );
    }

    /// Record a waiter's conditional request returning a `4xx` status —
    /// credentials issue (401/403) or other client error. The response
    /// is returned to the client unchanged; the cache is preserved.
    pub async fn record_coalesce_waiter_conditional_4xx(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.waiter_conditional_4xx += 1;
        debug!(
            "Recorded coalesce waiter conditional 4xx (total: {})",
            stats.waiter_conditional_4xx
        );
    }

    /// Record a waiter's conditional request failing with a `5xx` from S3
    /// or a transport error. Triggers the degraded cache-serve fallback.
    pub async fn record_coalesce_waiter_conditional_error(&self) {
        let mut stats = self.coalescing_stats.write().await;
        stats.waiter_conditional_error += 1;
        debug!(
            "Recorded coalesce waiter conditional error (total: {})",
            stats.waiter_conditional_error
        );
    }

    // =========================================================================
    // Page-Aligned Range Caching (Widening) Metrics
    // Spec: page-aligned-range-cache. Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6
    // =========================================================================

    /// Record a request that was widened into a page-aligned fetch, along
    /// with the client-requested byte length and the total widened (Page)
    /// byte length, so the amplification ratio can be derived
    /// (Requirement 8.1, 8.2).
    pub async fn record_page_widened_request(&self, bytes_requested: u64, bytes_widened: u64) {
        let mut stats = self.page_cache_stats.write().await;
        stats.widened_requests += 1;
        stats.bytes_requested += bytes_requested;
        stats.bytes_prefetched += bytes_widened.saturating_sub(bytes_requested);
        debug!(
            "Recorded page widened request (total: {}, bytes_prefetched: {})",
            stats.widened_requests, stats.bytes_prefetched
        );
    }

    /// Record a Small_Read served from an already-cached Page with no S3
    /// fetch (Requirement 8.3).
    pub async fn record_page_hit(&self) {
        let mut stats = self.page_cache_stats.write().await;
        stats.page_hits += 1;
        debug!("Recorded page cache hit (total: {})", stats.page_hits);
    }

    /// Record a request declined widening because the Range was signed
    /// (Requirement 8.4).
    pub async fn record_page_skipped_signed_range(&self) {
        let mut stats = self.page_cache_stats.write().await;
        stats.skipped_signed_range += 1;
        debug!(
            "Recorded page widening skipped (signed range) (total: {})",
            stats.skipped_signed_range
        );
    }

    /// Record a widened/page fetch failure that fell back to the client's
    /// original range (Requirement 8.5).
    pub async fn record_page_fallback(&self) {
        let mut stats = self.page_cache_stats.write().await;
        stats.fallbacks += 1;
        debug!(
            "Recorded page widening fallback (total: {})",
            stats.fallbacks
        );
    }

    /// Record a Page successfully promoted to the RAM cache
    /// (Requirement 8.6).
    pub async fn record_ram_page_promotion(&self) {
        let mut stats = self.page_cache_stats.write().await;
        stats.ram_page_promotions += 1;
        debug!(
            "Recorded RAM page promotion (total: {})",
            stats.ram_page_promotions
        );
    }

    /// Record a Page that was not promoted to RAM because it exceeded the
    /// applicable RAM budget (Requirement 8.6, see Requirement 7.7).
    pub async fn record_ram_page_promotion_skipped(&self) {
        let mut stats = self.page_cache_stats.write().await;
        stats.ram_page_promotion_skipped += 1;
        debug!(
            "Recorded RAM page promotion skipped (total: {})",
            stats.ram_page_promotion_skipped
        );
    }
    /// Record a per-bucket cache hit. Tracked for all buckets.
    pub async fn record_bucket_cache_hit(&self, bucket: &str, is_head: bool) {
        let mut stats = self.bucket_cache_stats.write().await;
        let entry = stats.entry(bucket.to_string()).or_default();
        entry.hit_count += 1;
        if is_head {
            entry.head_hit_count += 1;
        } else {
            entry.get_hit_count += 1;
        }
    }

    /// Record a per-bucket cache miss. Tracked for all buckets.
    pub async fn record_bucket_cache_miss(&self, bucket: &str, is_head: bool) {
        let mut stats = self.bucket_cache_stats.write().await;
        let entry = stats.entry(bucket.to_string()).or_default();
        entry.miss_count += 1;
        if is_head {
            entry.head_miss_count += 1;
        } else {
            entry.get_miss_count += 1;
        }
    }

    /// Get per-bucket cache stats for all tracked buckets.
    /// Returns a snapshot of hit/miss counts keyed by bucket name. Test seam.
    #[cfg(test)]
    pub(crate) async fn get_bucket_cache_stats(&self) -> HashMap<String, BucketCacheStats> {
        let stats = self.bucket_cache_stats.read().await;
        stats.clone()
    }

    /// Configure per-bucket traffic accounting from the loaded config.
    /// Must be called before any recording starts.
    pub fn set_per_bucket_config(
        &mut self,
        max_series: usize,
        bucket_prefixes: HashMap<String, Vec<String>>,
    ) {
        self.max_bucket_traffic_series = max_series;
        self.per_bucket_prefixes = bucket_prefixes;
    }

    /// Return the configured prefix list for a bucket, if any.
    ///
    /// Used by the HTTP proxy completion site to resolve the traffic attribution key
    /// before calling `record_bucket_traffic`. Returns `None` when no prefixes are
    /// configured for the bucket (bucket-level attribution applies).
    ///
    /// Spec: per-bucket-metrics. Requirements: 5.1, 5.2
    pub fn bucket_prefixes_for(&self, bucket: &str) -> Option<&[String]> {
        self.per_bucket_prefixes.get(bucket).map(|v| v.as_slice())
    }

    /// Record traffic for a completed request.
    ///
    /// Non-blocking write to the per-bucket traffic map (O(1) critical section,
    /// mirrors bucket_cache_stats pattern). Empty bucket is a no-op at debug level.
    ///
    /// Overflow: once the map has `max_bucket_traffic_series` distinct series, any
    /// new series is folded into the `__other__` overflow sentinel (bucket:
    /// "__other__", prefix: None). Traffic is never dropped — only the series key
    /// changes. `warn!` once per process on first overflow activation.
    ///
    /// Only GET and PUT (object/part) are recorded — see `RequestType`.
    ///
    /// Spec: per-bucket-metrics. Requirements: 1.1, 1.2, 1.3, 1.4, 2.3, 2.4, 7.2, 7.3
    pub async fn record_bucket_traffic(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        request_type: RequestType,
        bytes_served: u64,
        bytes_saved: u64,
        bytes_uploaded: u64,
    ) {
        if bucket.is_empty() {
            debug!("record_bucket_traffic: empty bucket, skipping");
            return;
        }

        const OVERFLOW_SENTINEL: &str = "__other__";

        let mut stats = self.bucket_traffic_stats.write().await;

        // Determine the attribution key, applying the overflow cap.
        let key = BucketTrafficKey {
            bucket: bucket.to_string(),
            prefix: prefix.map(|p| p.to_string()),
        };

        let use_key = if stats.contains_key(&key) {
            // Existing series — fast path, no cap check needed.
            key
        } else {
            // New series: check if we are at or over the cap.
            let overflow_key = BucketTrafficKey {
                bucket: OVERFLOW_SENTINEL.to_string(),
                prefix: None,
            };
            if stats.len() >= self.max_bucket_traffic_series && !stats.contains_key(&overflow_key) {
                // First overflow activation — warn once.
                warn!(
                    max_series = self.max_bucket_traffic_series,
                    bucket = %bucket,
                    "Per-bucket traffic cap reached; further new series folded into __other__"
                );
            }
            if stats.len() >= self.max_bucket_traffic_series {
                overflow_key
            } else {
                key
            }
        };

        let entry = stats.entry(use_key).or_default();
        entry.bytes_served = entry.bytes_served.saturating_add(bytes_served);
        entry.bytes_saved = entry.bytes_saved.saturating_add(bytes_saved);
        entry.bytes_uploaded = entry.bytes_uploaded.saturating_add(bytes_uploaded);

        match request_type {
            RequestType::Get => entry.get_requests = entry.get_requests.saturating_add(1),
            RequestType::Put => entry.put_requests = entry.put_requests.saturating_add(1),
        }
    }

    /// Snapshot of per-bucket traffic stats (non-resetting).
    ///
    /// `overflow_active` is derived from the presence of the `__other__` key in the
    /// returned map — callers should check `map.contains_key(&BucketTrafficKey { bucket: "__other__", prefix: None })`.
    ///
    /// Spec: per-bucket-metrics. Requirements: 1.3, 7.3
    pub async fn get_bucket_traffic_stats(&self) -> HashMap<BucketTrafficKey, BucketTrafficStats> {
        self.bucket_traffic_stats.read().await.clone()
    }

    /// Snapshot of per-bucket traffic stats with string keys suitable for JSON serialization.
    ///
    /// Converts the `BucketTrafficKey` struct key (which cannot be a JSON object key
    /// directly) into a string key: `"bucket"` when no prefix is active, or
    /// `"bucket/prefix"` when prefix attribution is active.
    ///
    /// Spec: per-bucket-metrics. Requirements: 3.1, 3.3
    pub async fn get_bucket_traffic_stats_json(&self) -> HashMap<String, BucketTrafficStats> {
        self.bucket_traffic_stats
            .read()
            .await
            .iter()
            .map(|(k, v)| {
                let key = match &k.prefix {
                    Some(p) => format!("{}/{}", k.bucket, p),
                    None => k.bucket.clone(),
                };
                (key, v.clone())
            })
            .collect()
    }

    /// Record a per-rule cache hit. Key is the rule's glob pattern.
    pub async fn record_rule_cache_hit(&self, pattern: &str, is_head: bool) {
        let mut stats = self.rule_cache_stats.write().await;
        let entry = stats.entry(pattern.to_string()).or_default();
        entry.hit_count += 1;
        if is_head {
            entry.head_hit_count += 1;
        } else {
            entry.get_hit_count += 1;
        }
    }

    /// Record a per-rule cache miss. Key is the rule's glob pattern.
    pub async fn record_rule_cache_miss(&self, pattern: &str, is_head: bool) {
        let mut stats = self.rule_cache_stats.write().await;
        let entry = stats.entry(pattern.to_string()).or_default();
        entry.miss_count += 1;
        if is_head {
            entry.head_miss_count += 1;
        } else {
            entry.get_miss_count += 1;
        }
    }

    /// Get per-rule cache stats. Keys are rule glob patterns.
    pub async fn get_rule_cache_stats(&self) -> HashMap<String, BucketCacheStats> {
        let stats = self.rule_cache_stats.read().await;
        stats.clone()
    }

    /// Record a successful eager invalidation triggered by `read_cache_enabled=false`.
    pub async fn record_read_cache_disabled_invalidation(&self) {
        let mut stats = self.error_stats.write().await;
        stats.read_cache_disabled_invalidations_total += 1;
    }

    /// Record a TTL-driven revalidation (freshness expired, conditional request to S3).
    pub async fn record_ttl_revalidation(&self) {
        let mut stats = self.error_stats.write().await;
        stats.ttl_revalidations_total += 1;
    }

    /// Record connection creation
    /// Requirement 6.1: Track connections created per endpoint
    ///
    /// This method records when a new connection is created to an endpoint.
    ///
    /// # The key MUST be the signed Host, not an SNI name
    /// `connections_reused` is derived per key as
    /// `total_requests - connections_created` by
    /// [`record_request_to_endpoint`](Self::record_request_to_endpoint), which
    /// keys on `S3RequestContext::host`. A creation filed under any other
    /// spelling of the same endpoint — an SNI name, a dotted quad — is not merely
    /// mis-filed: it makes the real endpoint's derived reuse read high (up to a
    /// spurious 100%) and leaves a key in this map that no request produces.
    /// Callers that only hold an IP must resolve it with
    /// `ConnectionPoolManager::get_metrics_endpoint_key` and record nothing when
    /// that returns `None`.
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
    /// **This is the sole producer of `connections_reused`, and it writes the
    /// field absolutely (`insert`), not incrementally.** A second producer that
    /// incremented the same map would have its increments destroyed by the next
    /// request to that endpoint, so there can only be one writer here. A
    /// `record_connection_reused` recorder that did exactly that was deleted
    /// rather than wired, for this reason.
    ///
    /// # The canonical key is the signed Host, and BOTH producers now use it
    /// `total_requests` is keyed here on `S3RequestContext::host`
    /// (`src/s3_client.rs`, the port-stripped signed Host). That string is
    /// canonical: it is stable per logical endpoint and it is what an operator
    /// recognises on a dashboard. `connections_created` used to be keyed on the
    /// connector's `tls_hostname`, which is an SNI name and can be a dotted quad,
    /// and the two diverged on the default path in two traced ways — an IP
    /// excluded by `remove_ip` reverse-mapped to nothing, and an IP owned by more
    /// than one endpoint reverse-mapping non-deterministically over a `HashMap`.
    /// Where they diverged, `connections_created.get(endpoint)` read 0 and
    /// `connections_reused` was `insert`ed equal to `total_requests` — a
    /// published, growing, spurious 100% reuse — while creations accumulated
    /// under a key receiving few requests, whose own `connections_reused` the
    /// `saturating_sub` pinned at 0.
    ///
    /// `src/https_connector.rs` now resolves its key with
    /// `ConnectionPoolManager::get_metrics_endpoint_key`, which is deterministic
    /// and also resolves excluded IPs, and records **nothing** (with a WARN) when
    /// it cannot resolve one — rather than inventing a dotted-quad key that no
    /// request will ever produce. So `connections_created`'s key set is a subset
    /// of this map's key set, which is the invariant the subtraction needs.
    ///
    /// The residual bias is one-directional and bounded: a skipped creation makes
    /// that endpoint's `connections_reused` read high by one.
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

    /// Get connection keepalive statistics. Test seam: `/metrics` reads the stats directly.
    #[cfg(test)]
    pub(crate) async fn get_connection_keepalive_stats(&self) -> ConnectionKeepaliveStats {
        self.connection_keepalive_stats.read().await.clone()
    }

    /// Collect atomic metadata metrics
    /// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
    ///
    /// Nine fields are measurements: the four `metadata_lock_*` figures, read from the
    /// acquisition primitives' own atomics in `crate::metadata_lock_manager`, plus
    /// `range_file_corruption_detected`, `range_file_corruption_healed`,
    /// `range_file_corruption_heal_suppressed`, `range_file_identity_mismatch_detected` and
    /// `journal_validation_failures`, read from module
    /// atomics in `crate::disk_cache` and `crate::journal_consolidator`. The other 8 fields have
    /// no writer anywhere in the crate and are published as literals, so the wire shape stays
    /// stable while nothing pretends to measure them. `src/otlp.rs`'s
    /// `every_atomic_metadata_field_has_an_otlp_instrument` reads this function's source and
    /// fails when one of those literals is replaced by a real reading, because that field must
    /// then gain an OTLP instrument.
    async fn collect_atomic_metadata_metrics(&self) -> Option<AtomicMetadataMetrics> {
        Some(AtomicMetadataMetrics {
            metadata_lock_acquisitions_successful:
                crate::metadata_lock_manager::metadata_lock_acquisitions_successful(),
            metadata_lock_acquisitions_failed:
                crate::metadata_lock_manager::metadata_lock_acquisitions_failed(),
            metadata_lock_hold_time_ms_total:
                crate::metadata_lock_manager::metadata_lock_hold_time_ms_total(),
            metadata_lock_wait_budget_expired_total:
                crate::metadata_lock_manager::metadata_lock_wait_budget_expired_total(),
            range_file_corruption_detected: crate::disk_cache::range_file_corruption_detected(),
            range_file_corruption_healed: crate::disk_cache::range_file_corruption_healed(),
            range_file_corruption_heal_suppressed:
                crate::disk_cache::range_file_corruption_heal_suppressed(),
            range_file_identity_mismatch_detected:
                crate::disk_cache::range_file_identity_mismatch_detected(),
            journal_validation_failures: crate::journal_consolidator::journal_validation_failures(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// A collection is counted, timed, compared against the threshold, and reports its own cost.
    ///
    /// # Why the assertions are inequalities and deltas rather than exact values
    ///
    /// These figures live in process-global `AtomicU64`s, and roughly a dozen other tests in this
    /// binary call `collect_metrics()` concurrently — every one of which increments
    /// `COLLECTIONS_TOTAL` and stores a `duration_ms_last`. the testing guidance §
    /// "Never assert an exact delta on a process-global counter" is explicit that an exact
    /// before/after delta cannot be made safely here, and that even a `>` assertion can pass on
    /// someone else's increment. So this asserts only what no concurrent caller can falsify:
    ///
    /// - **`duration_ms_last` is read off the RETURN VALUE, never reloaded from the static.** The
    ///   returned snapshot carries the caller's own measurement, so a concurrent `store` cannot
    ///   move it. Reloading the static would have been the flaky version of this assertion.
    /// - **`duration_ms_max` uses `>=`.** It is a `fetch_max`, so it is monotonic and a fast
    ///   concurrent collection cannot pull it back down.
    /// - **`collections_total` only has to increase**, which holds whether or not others increment.
    ///
    /// # What this deliberately does NOT assert
    ///
    /// That a collection at exactly the threshold is *not* counted slow. The
    /// predicate is `>` rather than `>=` (see [`MetricsManager::note_collection_complete`]), but
    /// proving the boundary needs an exact cumulative delta, and on a loaded machine a genuinely
    /// slow real collection in a parallel test would increment the same counter and fail that
    /// assertion for a reason unrelated to the boundary. A flaky guard on the boundary is worth
    /// less than a reliable guard on the mechanism, so the boundary is left to the source predicate
    /// and stated here rather than tested unreliably.
    #[test]
    fn a_collection_is_counted_timed_and_compared_against_the_slow_threshold() {
        // The default interval's threshold. The derivation itself is pinned by
        // `the_slow_threshold_is_half_the_collection_interval`; this test is about the counters.
        let threshold = collection_slow_threshold_ms(Duration::from_secs(60));
        // Under the threshold, so it must not be counted slow.
        let fast = MetricsManager::note_collection_complete(Duration::from_millis(1), threshold);
        assert_eq!(
            fast.duration_ms_last, 1,
            "the snapshot must report the duration it was handed"
        );
        assert!(
            fast.collections_total >= 1,
            "the collection must be counted: {}",
            fast.collections_total
        );
        assert_eq!(
            fast.slow_threshold_ms, threshold,
            "the threshold must be published so an operator can confirm which one is in force \
             without reading the build"
        );

        // Well past the threshold. Synthetic rather than a real slow collection: the alternative is
        // stalling a shared volume for half a minute inside a unit test.
        let slow = MetricsManager::note_collection_complete(
            Duration::from_millis(threshold + 1_000),
            threshold,
        );
        assert_eq!(slow.duration_ms_last, threshold + 1_000);
        assert!(
            slow.collections_total > fast.collections_total,
            "every collection increments the counter: {} then {}",
            fast.collections_total,
            slow.collections_total
        );
        assert!(
            slow.slow_collections_total > fast.slow_collections_total,
            "a collection over the {}ms threshold must be counted slow: {} then {}",
            threshold,
            fast.slow_collections_total,
            slow.slow_collections_total
        );
        assert!(
            slow.duration_ms_max >= threshold + 1_000,
            "the peak must retain the slow collection — it is the field that survives into later \
             responses after duration_ms_last has moved on: {}",
            slow.duration_ms_max
        );
    }

    /// Passenger A: the slow-collection threshold is HALF of `metrics.collection_interval`, not a
    /// fixed figure, and the threshold a response publishes is the one derived from the interval
    /// the manager was configured with.
    ///
    /// Tests `collection_slow_threshold_ms` and its two call sites in `collect_metrics` /
    /// `current_metrics`. Red side: restoring the fixed 2,000 ms return value fails every
    /// assertion below, because none of the three intervals halves to 2,000.
    #[tokio::test]
    async fn the_slow_threshold_is_half_the_collection_interval() {
        assert_eq!(
            collection_slow_threshold_ms(Duration::from_secs(60)),
            30_000
        );
        assert_eq!(collection_slow_threshold_ms(Duration::from_secs(10)), 5_000);
        assert_eq!(
            collection_slow_threshold_ms(Duration::from_secs(86_400)),
            43_200_000
        );

        let mut manager = MetricsManager::new();
        manager.set_collection_interval(Duration::from_secs(10));
        assert_eq!(
            manager
                .current_metrics()
                .await
                .metrics_collection
                .slow_threshold_ms,
            5_000,
            "a response must publish the threshold derived from the configured interval"
        );
        assert_eq!(
            manager
                .collect_metrics()
                .await
                .metrics_collection
                .slow_threshold_ms,
            5_000,
            "a full collection must be judged against the same threshold it publishes"
        );
    }

    /// Red side 3: before the first background collection the volume-derived figures publish
    /// their UNREADABLE sentinels and `volume_snapshot` publishes `null`, never a zero; after one,
    /// `volume_snapshot` carries a collection time and an age.
    ///
    /// Tests `MetricsManager::current_metrics`'s `volume_snapshot` construction and the
    /// `VolumeSource::Snapshot(None)` arms of `ledger_repair_from_snapshot`,
    /// `staged_fold_from_snapshot` and the `ledger_entries` read in `collect_write_cache_metrics`.
    #[tokio::test]
    async fn volume_figures_are_sentinels_before_the_first_collection_and_aged_after() {
        let (cache_manager, _temp_dir) =
            build_cache_manager_with_write_cache(1_000_000, 10.0).await;
        let mut manager = MetricsManager::new();
        manager.set_cache_manager(cache_manager);

        let before = manager.current_metrics().await;
        assert_eq!(
            before.volume_snapshot.collected_at_unix_secs, None,
            "no collection has run, so there is no collection time to publish"
        );
        assert_eq!(
            before.volume_snapshot.age_secs, None,
            "an age of 0 before the first collection would read as a snapshot taken just now"
        );
        assert_eq!(before.volume_snapshot.interval_secs, 60);
        let wc = &before.write_cache;
        assert_eq!(wc.ledger_entries, None);
        assert_eq!(wc.ledger_entries_age_secs, None);
        assert_eq!(wc.ledger_repair.ledger_repair_observed_shards, None);
        assert_eq!(wc.ledger_repair.ledger_repair_rotation_reachable, None);
        assert_eq!(wc.ledger_repair.ledger_repair_projected_rotation_secs, None);
        assert_eq!(wc.ledger_repair.ledger_repair_stalest_age_secs, None);
        assert!(!wc.ledger_repair.ledger_repair_rotation_complete);
        assert_eq!(wc.staged_fold.staged_fold_bytes, None);
        assert_eq!(wc.staged_fold.staged_shard_authority_age_secs, None);
        assert_eq!(
            before
                .cache
                .as_ref()
                .expect("cache section")
                .st_size_fold
                .st_size_fold_oldest_shard_scan_age_secs,
            None
        );

        let collected = manager.collect_metrics().await;
        assert_eq!(collected.volume_snapshot.age_secs, Some(0));
        let after = manager.current_metrics().await;
        assert_eq!(
            after.volume_snapshot.collected_at_unix_secs,
            collected.volume_snapshot.collected_at_unix_secs,
            "a request must report the collection time of the snapshot it served"
        );
        assert!(
            matches!(after.volume_snapshot.age_secs, Some(age) if age <= 5),
            "a request after a collection must publish that collection's age: {:?}",
            after.volume_snapshot.age_secs
        );
    }

    /// Red side 2, and the assertion that protects the fleet suite: in-process figures stay LIVE
    /// on a request between background collections. Several suite groups read a counter
    /// immediately after the action that moves it (several fleet tests among them), and a
    /// counter served from a snapshot would report "did not move".
    ///
    /// Tests `MetricsManager::assemble`'s request-metrics read and `collect_write_cache_metrics`'
    /// `resident_bytes` read under `VolumeSource::Snapshot`. `resident_bytes` is the figure the fleet tests
    /// judge by, and it comes from `size_state.json` — a single small file, which is
    /// why it stays live rather than moving to the snapshot.
    #[tokio::test]
    async fn in_process_figures_stay_live_between_background_collections() {
        let (cache_manager, _temp_dir) =
            build_cache_manager_with_write_cache(1_000_000, 10.0).await;
        let mut manager = MetricsManager::new();
        manager.set_cache_manager(cache_manager.clone());

        // One full collection, so a request has a snapshot to serve from.
        let snapshot = manager.collect_metrics().await;
        assert_eq!(snapshot.request_metrics.total_requests, 0);
        assert_eq!(snapshot.write_cache.resident_bytes, 0);

        // Move two figures with no collection in between.
        manager
            .record_response(StatusCode::OK, Duration::from_millis(1), Some(true), false)
            .await;
        cache_manager
            .get_journal_consolidator()
            .await
            .unwrap()
            .atomic_update_size_delta(150_000, 150_000)
            .await
            .unwrap();

        let served = manager.current_metrics().await;
        assert!(
            served.volume_snapshot.age_secs.is_some(),
            "precondition: this response was served against the snapshot, not a fresh collection"
        );
        assert_eq!(
            served.request_metrics.total_requests, 1,
            "request_metrics must be read live, not from the last background collection"
        );
        assert_eq!(served.request_metrics.cache_hit_requests, 1);
        assert_eq!(
            served.write_cache.resident_bytes, 150_000,
            "write_cache.resident_bytes must be read live — T51g and T53c assert its delta"
        );
    }

    /// Red side 1 (behavioural half): the background collector collects on its own, with no
    /// request, repeatedly, and stops on shutdown.
    ///
    /// Tests `spawn_background_collector`. Its wiring into `main` is pinned separately by
    /// `main.rs`'s `the_background_metrics_collector_is_spawned_by_async_main`, because a collector
    /// that works and is never started is the failure this unit exists to fix.
    #[tokio::test]
    async fn the_background_collector_collects_without_any_request() {
        let manager = Arc::new(RwLock::new(MetricsManager::new()));
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        let handle = spawn_background_collector(
            manager.clone(),
            Duration::from_millis(50),
            crate::shutdown::ShutdownSignal::new(rx),
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let first = loop {
            if let Some(m) = manager.read().await.get_cached_metrics().await {
                break m.timestamp;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no background collection landed within 10s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        loop {
            let now = manager
                .read()
                .await
                .get_cached_metrics()
                .await
                .map(|m| m.timestamp);
            if now.is_some_and(|t| t != first) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the collector ran once and never again"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the collector must stop on shutdown")
            .expect("the collector task must not panic");
    }

    /// The overlay helpers: with no snapshot they publish exactly the live half, and with one they
    /// take the volume-derived fields from it and advance its ages.
    /// Every scanner field reaches the `write_cache` JSON object flat, and the per-level objects
    /// stop at [`SCANNER_LEVELS_PUBLISHED_MAX`] levels when the limiter holds more.
    #[test]
    fn scanner_gauges_serialise_every_field_and_bound_the_level_objects() {
        use crate::scanner_limiter::ScannerLimiter;
        let mut fold = ScannerLimiter::new(200);
        for n in 0..100u32 {
            fold.record_visit(n, n, 10, std::time::Duration::from_millis(100));
        }
        fold.record_visit(1, 2, 10, std::time::Duration::from_millis(100));
        let mut repair = ScannerLimiter::new(2);
        repair.record_visit(0, 0, 4, std::time::Duration::from_secs(1));
        let wc = WriteCacheMetrics {
            scanner: ScannerGauges::from_limiters(&fold, &repair),
            ..Default::default()
        };
        let v = serde_json::to_value(&wc).unwrap();
        let obj = v.as_object().unwrap();
        for kind in ["fold", "repair"] {
            for base in [
                "scanner_concurrency_k",
                "scanner_concurrency_observed",
                "scanner_samples_discarded_total",
                "scan_deferred_total",
                "scanner_last_adjust_reason",
                "scanner_rate_by_level",
                "scanner_samples_by_level",
            ] {
                let key = format!("{base}_{kind}");
                assert!(obj.contains_key(&key), "missing {key} in {v}");
            }
        }
        assert_eq!(obj["scanner_concurrency_k_fold"], 200);
        assert_eq!(obj["scanner_samples_discarded_total_fold"], 1);
        assert_eq!(
            obj["scanner_last_adjust_reason_repair"],
            "insufficient_samples"
        );
        let rate = obj["scanner_rate_by_level_fold"].as_object().unwrap();
        let samples = obj["scanner_samples_by_level_fold"].as_object().unwrap();
        assert_eq!(rate.len(), SCANNER_LEVELS_PUBLISHED_MAX);
        assert_eq!(samples.len(), SCANNER_LEVELS_PUBLISHED_MAX);
        assert!(rate.contains_key("1") && rate.contains_key("64") && !rate.contains_key("65"));
        assert_eq!(rate["1"], 100.0);
        let repair_rate = obj["scanner_rate_by_level_repair"].as_object().unwrap();
        assert_eq!(repair_rate.len(), 1);
        assert_eq!(obj["scanner_samples_by_level_repair"]["1"], 1);
    }

    /// `scanner_aggregate_estimate_by_level_*` publishes `Â[j] = j·ρ̂[j]` for exactly the levels
    /// the limiter can judge (three kept samples), and the rotation remedy reaches the
    /// `write_cache` object flat beside them.
    #[test]
    fn scanner_aggregate_estimate_is_level_times_rate_for_judged_levels_only() {
        use crate::scanner_limiter::ScannerLimiter;
        let mut repair = ScannerLimiter::new(8);
        // Level 2 (one peer scanning): three visits at 100 files/s, so Â[2] = 200.
        for _ in 0..3 {
            repair.record_visit(1, 1, 10, std::time::Duration::from_millis(100));
        }
        // Level 1: one visit, below the sample floor, so no estimate.
        repair.record_visit(0, 0, 10, std::time::Duration::from_millis(100));
        let fold = ScannerLimiter::new(8);
        let mut wc = WriteCacheMetrics {
            scanner: ScannerGauges::from_limiters(&fold, &repair),
            ..Default::default()
        };
        wc.ledger_repair = wc.ledger_repair.with_rotation_remedy(2, 0);
        let v = serde_json::to_value(&wc).unwrap();
        let obj = v.as_object().unwrap();
        let est = obj["scanner_aggregate_estimate_by_level_repair"]
            .as_object()
            .unwrap();
        assert_eq!(est.len(), 1, "only the judged level is published: {v}");
        assert!(
            (est["2"].as_f64().unwrap() - 200.0).abs() < 1e-9,
            "Â[2] must be 2 × 100, got {}",
            est["2"]
        );
        assert_eq!(obj["scanner_rate_by_level_repair"]["1"], 100.0);
        assert!(obj["scanner_aggregate_estimate_by_level_fold"]
            .as_object()
            .unwrap()
            .is_empty());
        assert_eq!(obj["ledger_repair_rotation_remedy"], "");
    }

    #[test]
    fn the_overlays_take_volume_fields_from_the_snapshot_and_age_them() {
        let live = crate::ledger_repair::LedgerRepairGauges::process_local();
        assert_eq!(ledger_repair_from_snapshot(None, 30), live);
        let snap = crate::ledger_repair::LedgerRepairGauges {
            ledger_repair_rotation_complete: true,
            ledger_repair_stalest_age_secs: Some(100),
            ledger_repair_observed_shards: Some(1405),
            ledger_repair_swept_shards: 915,
            ledger_repair_rotation_reachable: Some(true),
            ledger_repair_projected_rotation_secs: Some(84_300),
            ..crate::ledger_repair::LedgerRepairGauges::process_local()
        };
        let got = ledger_repair_from_snapshot(Some(&snap), 30);
        assert!(got.ledger_repair_rotation_complete);
        assert_eq!(got.ledger_repair_stalest_age_secs, Some(130));
        assert_eq!(got.ledger_repair_observed_shards, Some(1405));
        assert_eq!(got.ledger_repair_swept_shards, 915);
        assert_eq!(got.ledger_repair_rotation_reachable, Some(true));
        assert_eq!(got.ledger_repair_projected_rotation_secs, Some(84_300));

        let live_fold = crate::staged_fold::StagedFoldGauges {
            staged_fold_incomplete_total: 7,
            ..Default::default()
        };
        assert_eq!(
            staged_fold_from_snapshot(live_fold.clone(), None, 30),
            live_fold
        );
        let snap_fold = crate::staged_fold::StagedFoldGauges {
            staged_fold_bytes: Some(10),
            staged_shard_authority_age_secs: Some(5),
            staged_fold_shards: 3,
            // A per-process counter in the snapshot must NOT win over the live one.
            staged_fold_incomplete_total: 1,
            ..Default::default()
        };
        let got = staged_fold_from_snapshot(live_fold, Some(&snap_fold), 30);
        assert_eq!(got.staged_fold_bytes, Some(10));
        assert_eq!(got.staged_shard_authority_age_secs, Some(35));
        assert_eq!(got.staged_fold_shards, 3);
        assert_eq!(got.staged_fold_incomplete_total, 7);
    }

    /// Build a `CacheManager` with shared storage wired up just far enough
    /// for `collect_write_cache_metrics` to read a real Journal_Consolidator
    /// (so `get_journal_consolidator()` returns `Some` and
    /// `get_write_cache_capacity()` reflects `write_cache_percent`).
    ///
    /// Deliberately calls `create_configured_disk_cache_manager()` plus a
    /// direct `consolidator.initialize()` rather than the full
    /// `CacheManager::initialize()` coordinated path: the latter also starts
    /// `CacheSizeTracker`'s background validation task, which — because no
    /// `validation.json` exists yet in a fresh temp dir — runs an immediate
    /// filesystem-scan validation that calls `update_size_from_validation`
    /// and overwrites `write_cache_size` back to the scanned (zero) figure,
    /// racing with this test's own `atomic_update_size_delta` calls. Skipping
    /// it keeps the test deterministic without weakening what's under test:
    /// `collect_write_cache_metrics` reads through
    /// `get_journal_consolidator()` and `get_write_cache_capacity()`, neither
    /// of which depends on `CacheSizeTracker` or `WriteCacheManager` being
    /// initialized.
    ///
    /// Returns the manager plus the temp dir it owns (kept alive for the
    /// caller's duration).
    async fn build_cache_manager_with_write_cache(
        max_cache_size: u64,
        write_cache_percent: f32,
    ) -> (Arc<CacheManager>, tempfile::TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
            temp_dir.path().to_path_buf(),
            false, // RAM cache disabled — irrelevant to write_cache metrics
            0,
            max_cache_size,
            crate::cache::CacheEvictionAlgorithm::TinyLFU,
            1024,
            false,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            false,
            crate::config::SharedStorageConfig::default(),
            write_cache_percent,
            true, // write cache enabled
            Duration::from_secs(86400),
            crate::config::MetadataCacheConfig::default(),
            95,
            80,
            true,
            Duration::from_secs(60),
            1_048_576,
            false,
            Duration::from_secs(10),
            4,
            Duration::from_secs(5),
        ));

        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager
            .get_journal_consolidator()
            .await
            .expect("consolidator set by create_configured_disk_cache_manager")
            .initialize()
            .await
            .unwrap();

        (cache_manager, temp_dir)
    }

    /// `collect_write_cache_metrics` must read `resident_bytes` from
    /// Size_State, `staging_bound_bytes` from the configured
    /// Staging_Bound, and derive `over_bound` from the two — plus surface
    /// the staging-eviction counters, initially zero.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.4
    #[tokio::test]
    async fn test_collect_write_cache_metrics_reports_bound_and_eviction_counters() {
        let (cache_manager, _temp_dir) =
            build_cache_manager_with_write_cache(1_000_000, 10.0).await;

        let mut metrics_manager = MetricsManager::new();
        metrics_manager.set_cache_manager(cache_manager.clone());

        let metrics = metrics_manager.collect_metrics().await;

        // Staging_Bound = 10% of 1_000_000 = 100_000.
        assert_eq!(metrics.write_cache.staging_bound_bytes, 100_000);
        // Nothing staged yet: resident bytes and eviction counters start at zero.
        assert_eq!(metrics.write_cache.resident_bytes, 0);
        assert!(!metrics.write_cache.over_bound);
        assert_eq!(metrics.write_cache.staging_evictions_total, 0);
        assert_eq!(metrics.write_cache.staging_eviction_bytes_total, 0);

        // Push Resident_Bytes over the Staging_Bound directly through the
        // consolidator (the same component `resident_bytes` is read from),
        // and confirm `over_bound` flips without anything else changing.
        let consolidator = cache_manager.get_journal_consolidator().await.unwrap();
        consolidator
            .atomic_update_size_delta(150_000, 150_000)
            .await
            .unwrap();

        let metrics = metrics_manager.collect_metrics().await;
        assert_eq!(metrics.write_cache.resident_bytes, 150_000);
        assert_eq!(metrics.write_cache.staging_bound_bytes, 100_000);
        assert!(metrics.write_cache.over_bound);
    }

    /// A scrape (`/metrics`, the dashboard, `/health`) must answer inside a bound the fleet suite
    /// can rely on even when the staged-authority memo has expired and a peer holds a `.pending`
    /// file's lock.
    ///
    /// # What the scrape used to wait on
    ///
    /// At staged-authority epoch 1, `current_metrics` reaches `CacheManager::staged_authority`
    /// twice (through `get_cache_size_stats` and `get_staging_resident_bytes`). When the 5 s memo
    /// has expired, that re-derives the figure on the request: a locked read of every `.pending`
    /// file (`PendingStore::read_all`, an exclusive `fs4` lock per file with no timeout), then a
    /// synchronous traversal of the whole checkpoint set on the tokio worker. On the fleet that is
    /// ~1,500 shard documents on EFS plus a lock whose holder can be another proxy. A scrape that
    /// arrived more than 5 s after the last authority read — the normal case for a suite that reads
    /// `/metrics` between fixtures — paid all of it.
    ///
    /// # The fixture holds exactly what the reader waits on
    ///
    /// The lock is held through a separate open file description, which is what a peer's append or
    /// GC is. The fixture proves the hold is effective by showing an uncached derivation blocks on
    /// it, so a green result cannot come from a lock the reader never takes.
    ///
    /// # Why 2 s
    ///
    /// The suite's readers use `curl --connect-timeout 2 --max-time 10`. Without the lock the whole
    /// call is local small-file reads on a temp directory, milliseconds. Two seconds is two orders
    /// of magnitude above that and a fifth of the reader's deadline, so a pass means the scrape did
    /// not wait on the lock rather than that it happened to win a race with it.
    ///
    /// The value is asserted too: the scrape must publish the memoised fold figure, not fall back to
    /// the incumbent counter, which over-states and would make a stale-but-fast answer wrong.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_scrape_answers_while_a_peer_holds_the_pending_lock_behind_an_expired_memo() {
        const F_BYTES: u64 = 4_444_444;
        const INCUMBENT_BYTES: u64 = 91_000_000;
        let (cache_manager, temp_dir) =
            build_cache_manager_with_write_cache(10_000_000_000, 50.0).await;
        let dir = temp_dir.path();

        let consolidator = cache_manager.get_journal_consolidator().await.unwrap();
        consolidator
            .atomic_update_size_delta(INCUMBENT_BYTES as i64, INCUMBENT_BYTES as i64)
            .await
            .unwrap();
        crate::staged_fold::seed_shard_figure_for_tests(dir, "test-bucket", "3f", F_BYTES, 9);

        // A peer's pending file, so `read_all` has a lock to take.
        let (pending_path, lock_path) =
            crate::staged_fold::pending_paths_for_tests(dir, "peer-host:4242");
        std::fs::create_dir_all(pending_path.parent().unwrap()).unwrap();
        std::fs::write(&pending_path, b"").unwrap();

        assert!(
            crate::staged_authority::activate(dir, "host:test").unwrap(),
            "fixture: activation must flip the epoch"
        );
        assert_eq!(
            crate::staged_authority::read_epoch(dir),
            crate::staged_authority::EPOCH_FOLD,
            "fixture: the scrape only derives the fold figure at epoch 1"
        );

        // Prime the memo with no lock held, then expire it.
        crate::staged_fold::invalidate_authority_snapshot_for_tests(dir);
        let primed = crate::staged_fold::authority_figure_for_tests(dir)
            .await
            .expect("fixture: the fold figure must be derivable before the lock is taken");
        assert_eq!(primed.resident_staged_bytes, F_BYTES);
        assert!(
            crate::staged_fold::age_authority_snapshot_for_tests(dir, 3600),
            "fixture: there must be a memo to expire"
        );

        let mut metrics_manager = MetricsManager::new();
        metrics_manager.set_cache_manager(cache_manager.clone());

        // A peer holds the pending lock.
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::lock(&lock_file).unwrap();

        // Fixture precondition: the hold is effective, so the post-cut pending term — the input
        // an uncached derivation needs — cannot be read while it is held.
        //
        // The change altered both the shape of that failure and the right way to probe it. Before the change
        // the read waited on the lock with no timeout, so "effective" meant an uncached
        // derivation never returned, and this was a `tokio::time::timeout` around
        // `authority_figure_for_tests`. The acquisition is now bounded, so that derivation
        // *completes*, reports the file unreadable, and installs `None` over the primed memo —
        // which made this test measure the incumbent-counter fallback instead of its own
        // subject. The probe is therefore a direct, side-effect-free read of the input rather
        // than an indirect timing observation of a consumer of it.
        let uncached_cannot_derive =
            !crate::staged_fold::pending_read_all_complete_for_tests(dir).await;
        // The memo must still be expired for the scrape below. The probe above installs nothing
        // — see its own doc comment for why that matters — so this only re-ages defensively.
        crate::staged_fold::age_authority_snapshot_for_tests(dir, 3600);

        let started = std::time::Instant::now();
        let scrape =
            tokio::time::timeout(Duration::from_secs(2), metrics_manager.current_metrics()).await;
        let elapsed = started.elapsed();

        // Release before asserting, so the blocked readers can finish and the runtime can shut down.
        let _ = fs4::FileExt::unlock(&lock_file);
        drop(lock_file);

        assert!(
            uncached_cannot_derive,
            "fixture: an uncached authority derivation produced a figure while the pending lock \
             was held, so the lock is not what the reader waits on and this test proves nothing"
        );
        let metrics = scrape.unwrap_or_else(|_| {
            panic!(
                "a /metrics scrape did not answer within 2 s ({elapsed:?}) while a peer held a \
                 .pending lock and the authority memo had expired: the request path re-derives \
                 the staged authority figure itself and waits on shared-storage locks and I/O"
            )
        });
        assert_eq!(
            metrics.write_cache.resident_bytes, F_BYTES,
            "the scrape must publish the memoised fold figure, not the incumbent counter"
        );
        assert_eq!(
            metrics.cache.as_ref().map(|c| c.write_cache_size),
            Some(F_BYTES),
            "cache.write_cache_size must come from the same memoised figure"
        );
    }

    /// The `over_bound` WARN (Requirement 8.5) must be rate-limited: calling
    /// `collect_write_cache_metrics` repeatedly while Resident_Bytes stays
    /// over the Staging_Bound must not panic, error, or otherwise fail — the
    /// rate limiter's job is to suppress log volume, not to change the
    /// returned metrics. This exercises the code path directly; the log
    /// suppression itself is structurally identical to
    /// `InflightLedger::log_rejection_rate_limited`, which is not
    /// separately log-capture-tested either.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 8.5
    #[tokio::test]
    async fn test_collect_write_cache_metrics_over_bound_repeated_calls_stable() {
        let (cache_manager, _temp_dir) = build_cache_manager_with_write_cache(1_000, 10.0).await;

        let mut metrics_manager = MetricsManager::new();
        metrics_manager.set_cache_manager(cache_manager.clone());

        let consolidator = cache_manager.get_journal_consolidator().await.unwrap();
        consolidator
            .atomic_update_size_delta(500, 500)
            .await
            .unwrap();

        // Staging_Bound = 10% of 1_000 = 100; resident 500 is over it on every call.
        for _ in 0..5 {
            let metrics = metrics_manager.collect_metrics().await;
            assert!(metrics.write_cache.over_bound);
            assert_eq!(metrics.write_cache.resident_bytes, 500);
            assert_eq!(metrics.write_cache.staging_bound_bytes, 100);
        }
    }

    /// A size-state read that FAILS must not publish a `consolidation` section of zeros, which is
    /// indistinguishable from an empty cache. The section is `None`, and `/metrics` carries
    /// `"consolidation": null`, the same shape as a deployment with no consolidator.
    ///
    /// "Failed" is `JournalConsolidator::get_size_state` returning `None`: `size_state.json`
    /// exists but cannot be read or parsed. An ABSENT file is a real reading of an empty cache
    /// and still yields a section.
    ///
    /// # RED SIDE
    ///
    /// With `collect_consolidation_metrics` taking `get_size_state().await.unwrap_or_default()`
    /// again, the unreadable read yields `Some` of zeros and the second assertion fails.
    #[tokio::test]
    async fn a_failed_size_state_read_publishes_no_consolidation_section() {
        let (cache_manager, temp_dir) = build_cache_manager_with_write_cache(1_000_000, 10.0).await;
        let mut metrics_manager = MetricsManager::new();
        metrics_manager.set_cache_manager(cache_manager.clone());
        let consolidator = cache_manager.get_journal_consolidator().await.unwrap();
        consolidator
            .atomic_update_size_delta(4096, 0)
            .await
            .unwrap();

        let readable = metrics_manager
            .collect_consolidation_metrics(&cache_manager)
            .await;
        assert_eq!(
            readable.as_ref().map(|c| c.cache_size_bytes),
            Some(4096),
            "FIXTURE: a readable size state must publish its figures, or the None below proves \
             nothing about the failed read"
        );

        let size_state = temp_dir
            .path()
            .join("size_tracking")
            .join("size_state.json");
        std::fs::write(&size_state, b"{ not json").unwrap();
        assert!(
            consolidator.get_size_state().await.is_none(),
            "FIXTURE: the corrupted size state must be an unreadable read"
        );

        let failed = metrics_manager
            .collect_consolidation_metrics(&cache_manager)
            .await;
        assert!(
            failed.is_none(),
            "a failed size-state read must publish no consolidation section, not {:?}",
            failed
        );
        let wire = serde_json::to_value(metrics_manager.collect_metrics().await).unwrap();
        assert_eq!(
            wire.get("consolidation"),
            Some(&serde_json::Value::Null),
            "/metrics must carry \"consolidation\": null on a failed read"
        );
    }

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
    async fn test_cache_coherency_metrics() {
        let metrics_manager = MetricsManager::new();

        // Record cache coherency events
        metrics_manager
            .record_etag_mismatch("test-key", "etag1", "etag2")
            .await;
        let ranges = vec!["range1".to_string(), "range2".to_string()];
        metrics_manager
            .record_range_invalidation("test-key", &ranges, "etag_mismatch")
            .await;

        // Record error metrics
        metrics_manager
            .record_corrupted_metadata("test-key", "/path/to/meta", "parse error")
            .await;

        // Get error stats directly
        let error_stats = metrics_manager.get_error_stats().await;

        // Verify cache coherency metrics
        assert_eq!(error_stats.cache_etag_mismatches_total, 1);
        assert_eq!(error_stats.cache_range_invalidations_total, 1);

        // Verify error metrics
        assert_eq!(error_stats.corruption_metadata_total, 1);
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

        // Reuse is DERIVED, not counted: record_request_to_endpoint is the only
        // writer of connections_reused, and it writes total_requests minus
        // connections_created. Drive the production path (five requests against
        // the two creations above) rather than a recorder that no production code
        // calls — the deleted record_connection_reused incremented the same map
        // and so would have been clobbered by the next request anyway.
        for _ in 0..5 {
            metrics_manager
                .record_request_to_endpoint("s3.amazonaws.com")
                .await;
        }

        // Record closures
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
        // Assert BOTH terms and the relationship between them, not the derived
        // figure alone: a value of 3 is also what a recorder wired to creation
        // would produce, so checking reused == total - created is what makes this
        // an assertion about the derivation rather than about a number.
        let total = *stats.total_requests.get("s3.amazonaws.com").unwrap_or(&0);
        let created = *stats
            .connections_created
            .get("s3.amazonaws.com")
            .unwrap_or(&0);
        let reused = *stats
            .connections_reused
            .get("s3.amazonaws.com")
            .unwrap_or(&0);
        assert_eq!(total, 5, "five requests were recorded");
        assert_eq!(created, 2, "two connections were created");
        assert_eq!(reused, total - created);
        assert_eq!(reused, 3);
        // The endpoint that saw a creation but no request must report no reuse,
        // and must not be missing from the map in a way that reads as zero reuse
        // for the same reason as a saturated subtraction.
        assert_eq!(
            stats.connections_reused.get("s3.us-west-2.amazonaws.com"),
            None,
            "connections_reused is only written by record_request_to_endpoint"
        );
        // Nothing in the proxy writes idle_timeout_closures; see the field doc.
        assert_eq!(stats.idle_timeout_closures, 0);
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

    /// Per-rule attribution: rule cache hits/misses are recorded against the
    /// winning rule's glob pattern, with the HEAD/GET split tracked correctly.
    ///
    /// Validates: Requirements 6.1, 6.3
    #[tokio::test]
    async fn test_rule_cache_stats_attribution_by_pattern() {
        let metrics_manager = MetricsManager::new();

        let pattern = "**/credit-cards/**";

        // A GET hit and a HEAD hit attributed to the same rule pattern.
        metrics_manager.record_rule_cache_hit(pattern, false).await;
        metrics_manager.record_rule_cache_hit(pattern, true).await;
        // A GET miss attributed to the same rule pattern.
        metrics_manager.record_rule_cache_miss(pattern, false).await;

        let stats = metrics_manager.get_rule_cache_stats().await;

        // Attribution is keyed on the rule's glob pattern.
        let entry = stats
            .get(pattern)
            .expect("rule stats should be keyed by the winning rule's pattern");

        // Totals.
        assert_eq!(entry.hit_count, 2);
        assert_eq!(entry.miss_count, 1);

        // HEAD/GET split for hits: one HEAD hit, one GET hit.
        assert_eq!(entry.head_hit_count, 1);
        assert_eq!(entry.get_hit_count, 1);

        // HEAD/GET split for misses: the single miss was a GET, not a HEAD.
        assert_eq!(entry.head_miss_count, 0);
        assert_eq!(entry.get_miss_count, 1);

        // No attribution leaks to other patterns.
        assert_eq!(stats.len(), 1);
    }

    /// Multiple distinct rule patterns are tracked independently, so the
    /// dashboard can show hit/miss counts per rule (Requirement 6.3).
    ///
    /// Validates: Requirements 6.1, 6.3
    #[tokio::test]
    async fn test_rule_cache_stats_independent_per_pattern() {
        let metrics_manager = MetricsManager::new();

        let logs_rule = "**/logs/**";
        let static_rule = "prod-*/static/**";

        metrics_manager
            .record_rule_cache_hit(logs_rule, false)
            .await;
        metrics_manager
            .record_rule_cache_miss(static_rule, true)
            .await;

        let stats = metrics_manager.get_rule_cache_stats().await;

        let logs = stats.get(logs_rule).expect("logs rule should be tracked");
        assert_eq!(logs.hit_count, 1);
        assert_eq!(logs.get_hit_count, 1);
        assert_eq!(logs.miss_count, 0);

        let statics = stats
            .get(static_rule)
            .expect("static rule should be tracked");
        assert_eq!(statics.miss_count, 1);
        assert_eq!(statics.head_miss_count, 1);
        assert_eq!(statics.hit_count, 0);

        assert_eq!(stats.len(), 2);
    }

    /// Per-bucket counters are maintained for ALL buckets, including a bucket
    /// that no rule matched (resolution fell through to the global defaults).
    /// This demonstrates per-bucket tracking is not restricted to buckets
    /// referenced by a rule.
    ///
    /// Validates: Requirements 6.5
    #[tokio::test]
    async fn test_bucket_cache_stats_tracks_bucket_without_matching_rule() {
        let metrics_manager = MetricsManager::new();

        // This bucket is referenced by a rule (e.g. matched "prod-*/static/**").
        let rule_bucket = "prod-assets";
        // This bucket matched no rule at all; resolution used global defaults,
        // so there is no per-rule attribution for it. Per Requirement 6.5 the
        // per-bucket counter must still track it.
        let unruled_bucket = "ad-hoc-bucket";

        metrics_manager
            .record_bucket_cache_hit(rule_bucket, false)
            .await;

        // Record a GET hit, a HEAD hit, and a GET miss for the unruled bucket.
        metrics_manager
            .record_bucket_cache_hit(unruled_bucket, false)
            .await;
        metrics_manager
            .record_bucket_cache_hit(unruled_bucket, true)
            .await;
        metrics_manager
            .record_bucket_cache_miss(unruled_bucket, false)
            .await;

        let stats = metrics_manager.get_bucket_cache_stats().await;

        // The bucket with no matching rule is tracked.
        let unruled = stats
            .get(unruled_bucket)
            .expect("per-bucket counter must track buckets with no matching rule");
        assert_eq!(unruled.hit_count, 2);
        assert_eq!(unruled.miss_count, 1);
        assert_eq!(unruled.head_hit_count, 1);
        assert_eq!(unruled.get_hit_count, 1);
        assert_eq!(unruled.head_miss_count, 0);
        assert_eq!(unruled.get_miss_count, 1);

        // The rule-referenced bucket is also tracked, independently.
        let ruled = stats
            .get(rule_bucket)
            .expect("per-bucket counter must track rule-referenced buckets too");
        assert_eq!(ruled.hit_count, 1);
        assert_eq!(ruled.get_hit_count, 1);
        assert_eq!(ruled.miss_count, 0);

        assert_eq!(stats.len(), 2);
    }

    #[tokio::test]
    async fn test_record_read_cache_disabled_invalidation() {
        let mm = MetricsManager::new();

        // Initially zero.
        let stats = mm.error_stats.read().await;
        assert_eq!(stats.read_cache_disabled_invalidations_total, 0);
        drop(stats);

        // Record two invalidations.
        mm.record_read_cache_disabled_invalidation().await;
        mm.record_read_cache_disabled_invalidation().await;

        let stats = mm.error_stats.read().await;
        assert_eq!(stats.read_cache_disabled_invalidations_total, 2);
    }

    #[tokio::test]
    async fn test_record_ttl_revalidation() {
        let mm = MetricsManager::new();

        let stats = mm.error_stats.read().await;
        assert_eq!(stats.ttl_revalidations_total, 0);
        drop(stats);

        mm.record_ttl_revalidation().await;
        mm.record_ttl_revalidation().await;
        mm.record_ttl_revalidation().await;

        let stats = mm.error_stats.read().await;
        assert_eq!(stats.ttl_revalidations_total, 3);
    }

    // ── resolve_traffic_key prefix edge case tests ────────────────────────
    // Spec: per-bucket-metrics, task 2.3. Requirements: 5.4.

    #[test]
    fn test_resolve_traffic_key_exact_match() {
        // Object key exactly equals a configured prefix
        let prefixes = vec!["logs/".to_string(), "data/".to_string()];
        let result = resolve_traffic_key("my-bucket", "logs/", Some(&prefixes));
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: Some("logs/".to_string()),
            }
        );
    }

    #[test]
    fn test_resolve_traffic_key_longest_match_wins() {
        // Longer prefix wins over shorter when both match
        let prefixes = vec![
            "data/".to_string(),
            "data/raw/".to_string(),
            "data/raw/2024/".to_string(),
        ];
        let result = resolve_traffic_key("my-bucket", "data/raw/2024/file.csv", Some(&prefixes));
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: Some("data/raw/2024/".to_string()),
            }
        );
    }

    #[test]
    fn test_resolve_traffic_key_no_match() {
        // Key doesn't start with any configured prefix → bucket-level (prefix: None)
        let prefixes = vec!["logs/".to_string(), "data/".to_string()];
        let result = resolve_traffic_key("my-bucket", "images/photo.jpg", Some(&prefixes));
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: None,
            }
        );
    }

    #[test]
    fn test_resolve_traffic_key_empty_prefix_list() {
        // Empty prefix list → always bucket-level
        let prefixes: Vec<String> = vec![];
        let result = resolve_traffic_key("my-bucket", "anything/here.txt", Some(&prefixes));
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: None,
            }
        );
    }

    #[test]
    fn test_resolve_traffic_key_none_prefix_list() {
        // None configured_prefixes → bucket-level
        let result = resolve_traffic_key("my-bucket", "anything/here.txt", None);
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: None,
            }
        );
    }

    #[test]
    fn test_resolve_traffic_key_empty_string_prefix_rejected() {
        // An empty-string prefix is filtered out (treated as bucket-level per
        // the design Error Handling table: "zero-length prefix that matches
        // everything — equivalent to bucket-level. Validation rejects it.")
        let prefixes = vec!["".to_string(), "logs/".to_string()];
        let result = resolve_traffic_key("my-bucket", "logs/access.log", Some(&prefixes));
        // "logs/" matches; the empty string is skipped
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: Some("logs/".to_string()),
            }
        );
    }

    #[test]
    fn test_resolve_traffic_key_empty_string_only_prefix() {
        // When the only configured prefix is an empty string, it is filtered out,
        // so no prefix matches → bucket-level
        let prefixes = vec!["".to_string()];
        let result = resolve_traffic_key("my-bucket", "some/key.txt", Some(&prefixes));
        assert_eq!(
            result,
            BucketTrafficKey {
                bucket: "my-bucket".to_string(),
                prefix: None,
            }
        );
    }

    // ── Property 7: Prefix attribution correctness ────────────────────────
    // Spec: per-bucket-metrics, task 2.2.
    // **Validates: Requirements 5.2, 5.3, 5.4, 4.2**
    //
    // Three sub-properties tested:
    // 1. If the key starts_with a configured prefix, the resolved key carries
    //    that prefix (or a longer one that also matches).
    // 2. If no configured prefix matches, prefix is None (bucket-level).
    // 3. Longest-match wins: if multiple prefixes match, the longest is selected.

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// Property 7a: When a key matches at least one configured prefix, the
    /// resolved prefix is one that the key starts_with AND is the longest such
    /// prefix in the configured set.
    #[quickcheck]
    fn prop_prefix_attribution_longest_match(
        bucket: String,
        key_suffix: String,
        prefix_base: String,
    ) -> TestResult {
        // Constrain inputs to useful ranges — empty bucket is a no-op in practice
        if bucket.is_empty() || prefix_base.is_empty() {
            return TestResult::discard();
        }
        // Avoid pathologically large inputs
        if bucket.len() > 64 || prefix_base.len() > 64 || key_suffix.len() > 128 {
            return TestResult::discard();
        }

        // Build a prefix hierarchy: "a/", "a/b/", "a/b/c/" style from prefix_base
        let short_prefix = format!("{}/", prefix_base);
        let long_prefix = format!("{}/sub/", prefix_base);
        // Object key that matches both prefixes
        let object_key = format!("{}/sub/object{}", prefix_base, key_suffix);

        let prefixes = vec![short_prefix.clone(), long_prefix.clone()];
        let result = resolve_traffic_key(&bucket, &object_key, Some(&prefixes));

        // The key starts_with both, so longest-match must win
        assert_eq!(result.bucket, bucket);
        assert_eq!(result.prefix, Some(long_prefix));
        TestResult::passed()
    }

    /// Property 7b: When no configured prefix matches the key, the resolved
    /// prefix is None (bucket-level attribution).
    #[quickcheck]
    fn prop_prefix_attribution_no_match_bucket_level(
        bucket: String,
        object_key: String,
        prefix_a: String,
        prefix_b: String,
    ) -> TestResult {
        if bucket.is_empty() {
            return TestResult::discard();
        }
        // Avoid pathologically large inputs
        if bucket.len() > 64 || object_key.len() > 128 || prefix_a.len() > 64 || prefix_b.len() > 64
        {
            return TestResult::discard();
        }
        // Skip empty prefixes (filtered out by the function)
        if prefix_a.is_empty() || prefix_b.is_empty() {
            return TestResult::discard();
        }

        // Only proceed when the key genuinely doesn't match either prefix
        if object_key.starts_with(&prefix_a) || object_key.starts_with(&prefix_b) {
            return TestResult::discard();
        }

        let prefixes = vec![prefix_a, prefix_b];
        let result = resolve_traffic_key(&bucket, &object_key, Some(&prefixes));

        assert_eq!(result.bucket, bucket);
        assert_eq!(result.prefix, None);
        TestResult::passed()
    }

    /// Property 7c: The resolved prefix is always the longest matching prefix
    /// from the configured set — never a shorter one. For any resolved prefix P,
    /// there is no configured prefix Q where |Q| > |P| and key starts_with Q.
    #[quickcheck]
    fn prop_prefix_attribution_resolved_is_longest(
        bucket: String,
        object_key: String,
        raw_prefixes: Vec<String>,
    ) -> TestResult {
        if bucket.is_empty() {
            return TestResult::discard();
        }
        if bucket.len() > 64 || object_key.len() > 128 {
            return TestResult::discard();
        }
        // Need at least one non-empty prefix to be interesting
        let prefixes: Vec<String> = raw_prefixes
            .into_iter()
            .filter(|p| !p.is_empty() && p.len() <= 64)
            .take(10) // bound cardinality for test speed
            .collect();
        if prefixes.is_empty() {
            return TestResult::discard();
        }

        let result = resolve_traffic_key(&bucket, &object_key, Some(&prefixes));

        match &result.prefix {
            None => {
                // No match: assert that indeed no configured prefix matches the key
                for p in &prefixes {
                    assert!(
                        !object_key.starts_with(p.as_str()),
                        "prefix {:?} matches key {:?} but resolve returned None",
                        p,
                        object_key
                    );
                }
            }
            Some(resolved) => {
                // Resolved must itself match the key
                assert!(
                    object_key.starts_with(resolved.as_str()),
                    "resolved prefix {:?} does not match key {:?}",
                    resolved,
                    object_key
                );
                // No configured prefix that matches the key is longer
                for p in &prefixes {
                    if object_key.starts_with(p.as_str()) {
                        assert!(
                            p.len() <= resolved.len(),
                            "prefix {:?} (len {}) matches key {:?} and is longer than resolved {:?} (len {})",
                            p, p.len(), object_key, resolved, resolved.len()
                        );
                    }
                }
            }
        }

        assert_eq!(result.bucket, bucket);
        TestResult::passed()
    }

    // ── Property 1: Cumulative byte summation ────────────────────────────
    // Spec: per-bucket-metrics, task 3.2.
    // **Validates: Requirements 1.1, 2.1**
    //
    // For any sequence of N record_bucket_traffic calls to the same bucket, the
    // snapshot's bytes_served equals the sum of all bytes_served arguments, and
    // bytes_uploaded equals the sum of all bytes_uploaded arguments.

    /// Property 1: After N calls to record_bucket_traffic for the same bucket,
    /// bytes_served == Σ(bytes_served args) and bytes_uploaded == Σ(bytes_uploaded args).
    #[quickcheck]
    fn prop_cumulative_byte_summation(
        served_values: Vec<u16>,
        uploaded_values: Vec<u16>,
    ) -> TestResult {
        if served_values.is_empty() || uploaded_values.is_empty() {
            return TestResult::discard();
        }
        if served_values.len() > 100 || uploaded_values.len() > 100 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let metrics = MetricsManager::new();
            let bucket = "test-bucket";

            let n = served_values.len().max(uploaded_values.len());
            let mut expected_served: u64 = 0;
            let mut expected_uploaded: u64 = 0;

            for i in 0..n {
                let bs = served_values[i % served_values.len()] as u64;
                let bu = uploaded_values[i % uploaded_values.len()] as u64;
                expected_served += bs;
                expected_uploaded += bu;

                // GET for the served bytes, PUT for the uploaded bytes — type is
                // irrelevant to byte summation, but we alternate to exercise both.
                let rtype = if i % 2 == 0 {
                    RequestType::Get
                } else {
                    RequestType::Put
                };
                metrics
                    .record_bucket_traffic(bucket, None, rtype, bs, 0, bu)
                    .await;
            }

            let snapshot = metrics.get_bucket_traffic_stats().await;
            let key = BucketTrafficKey {
                bucket: bucket.to_string(),
                prefix: None,
            };
            let stats = snapshot.get(&key).expect("bucket should be in snapshot");

            assert_eq!(stats.bytes_served, expected_served);
            assert_eq!(stats.bytes_uploaded, expected_uploaded);
            TestResult::passed()
        })
    }

    // ── Property 3: Exactly-once recording ────────────────────────────────
    // Spec: per-bucket-metrics, task 3.4.
    // **Validates: Requirements 2.1, 2.3**
    //
    // One record_bucket_traffic call moves the snapshot by exactly its byte
    // volume and exactly one request-type increment — no more, no less.

    /// Property 3: a single call produces exactly the expected delta.
    #[quickcheck]
    fn prop_exactly_once_recording(
        bucket_seed: u8,
        bytes_served: u16,
        bytes_uploaded: u16,
        is_put: bool,
    ) -> TestResult {
        let bucket = format!("bucket-{}", bucket_seed % 10);
        let bytes_served = bytes_served as u64;
        let bytes_uploaded = bytes_uploaded as u64;
        let request_type = if is_put {
            RequestType::Put
        } else {
            RequestType::Get
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let metrics = MetricsManager::new();

            let snap_before = metrics.get_bucket_traffic_stats().await;
            metrics
                .record_bucket_traffic(&bucket, None, request_type, bytes_served, 0, bytes_uploaded)
                .await;
            let snap_after = metrics.get_bucket_traffic_stats().await;

            let key = BucketTrafficKey {
                bucket: bucket.clone(),
                prefix: None,
            };
            let before = snap_before.get(&key).cloned().unwrap_or_default();
            let after = snap_after
                .get(&key)
                .expect("bucket must appear after recording");

            assert_eq!(after.bytes_served - before.bytes_served, bytes_served);
            assert_eq!(after.bytes_uploaded - before.bytes_uploaded, bytes_uploaded);

            let get_delta = after.get_requests - before.get_requests;
            let put_delta = after.put_requests - before.put_requests;
            match request_type {
                RequestType::Get => {
                    assert_eq!(get_delta, 1, "get_requests should increment by 1");
                    assert_eq!(put_delta, 0, "put_requests should not move");
                }
                RequestType::Put => {
                    assert_eq!(put_delta, 1, "put_requests should increment by 1");
                    assert_eq!(get_delta, 0, "get_requests should not move");
                }
            }
            TestResult::passed()
        })
    }

    // ── Property 5: Counter monotonicity ─────────────────────────────────
    // Spec: per-bucket-metrics, task 3.5.
    // **Validates: Requirements 1.3, 4.4**
    //
    // Between two snapshots with no restart between them, no counter ever
    // decreases for any bucket present in the earlier snapshot.

    /// Property 5: no counter decreases between two snapshots.
    #[quickcheck]
    fn prop_counter_monotonicity(
        initial_calls: Vec<(u8, bool, u16, u16)>,
        additional_calls: Vec<(u8, bool, u16, u16)>,
    ) -> TestResult {
        // Each tuple: (bucket_idx, is_put, bytes_served, bytes_uploaded)
        if initial_calls.is_empty() {
            return TestResult::discard();
        }
        if initial_calls.len() > 50 || additional_calls.len() > 50 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let metrics = MetricsManager::new();

            for (bucket_idx, is_put, bs, bu) in &initial_calls {
                let bucket_name = format!("bucket-{}", bucket_idx % 5);
                let rtype = if *is_put {
                    RequestType::Put
                } else {
                    RequestType::Get
                };
                metrics
                    .record_bucket_traffic(&bucket_name, None, rtype, *bs as u64, 0, *bu as u64)
                    .await;
            }
            let s1 = metrics.get_bucket_traffic_stats().await;

            for (bucket_idx, is_put, bs, bu) in &additional_calls {
                let bucket_name = format!("bucket-{}", bucket_idx % 5);
                let rtype = if *is_put {
                    RequestType::Put
                } else {
                    RequestType::Get
                };
                metrics
                    .record_bucket_traffic(&bucket_name, None, rtype, *bs as u64, 0, *bu as u64)
                    .await;
            }
            let s2 = metrics.get_bucket_traffic_stats().await;

            for (key, stats_s1) in &s1 {
                let stats_s2 = s2
                    .get(key)
                    .expect("bucket present in S1 must still be present in S2");
                assert!(stats_s2.bytes_served >= stats_s1.bytes_served);
                assert!(stats_s2.bytes_uploaded >= stats_s1.bytes_uploaded);
                assert!(stats_s2.get_requests >= stats_s1.get_requests);
                assert!(stats_s2.put_requests >= stats_s1.put_requests);
            }
            TestResult::passed()
        })
    }

    // ── Property 6: Concurrent recording integrity ────────────────────────
    // Spec: per-bucket-metrics, task 3.6.
    // **Validates: Requirements 1.4**
    //
    // N concurrent calls of 1 GET request and B bytes → exactly N get_requests
    // and N×B bytes_served. Nothing lost under the RwLock.

    /// Property 6: N concurrent calls yield exactly N requests and N×B bytes.
    #[quickcheck]
    fn prop_concurrent_recording_integrity(n_raw: u8, b_raw: u16) -> TestResult {
        let n = (n_raw % 99) as u64 + 2; // 2..100 concurrent tasks
        let b = (b_raw % 10000) as u64 + 1; // 1..10000 bytes per call

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let metrics = std::sync::Arc::new(MetricsManager::new());
            let bucket = "concurrency-test-bucket";

            let mut handles = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let mm = metrics.clone();
                let bkt = bucket.to_string();
                handles.push(tokio::spawn(async move {
                    mm.record_bucket_traffic(&bkt, None, RequestType::Get, b, 0, 0)
                        .await;
                }));
            }
            for handle in handles {
                handle.await.expect("task should not panic");
            }

            let snapshot = metrics.get_bucket_traffic_stats().await;
            let key = BucketTrafficKey {
                bucket: bucket.to_string(),
                prefix: None,
            };
            let stats = snapshot.get(&key).expect("bucket should be present");

            assert_eq!(stats.get_requests, n, "expected {} get_requests", n);
            assert_eq!(stats.bytes_served, n * b, "expected {} bytes_served", n * b);
            assert_eq!(stats.bytes_uploaded, 0);
            assert_eq!(stats.put_requests, 0);
            TestResult::passed()
        })
    }

    // ── Property 8: Bounded cardinality with conservation ─────────────────
    // Spec: per-bucket-metrics, task 3.7.
    // **Validates: Requirements 7.2, 7.3**
    //
    // (1) Bound: distinct series ≤ max_series + 1.
    // (2) Conservation: Σ (get_requests + put_requests) across all series equals
    //     total calls; Σ bytes_served and Σ bytes_uploaded equal recorded totals.

    /// Property 8: bounded cardinality, count-conserving overflow.
    #[quickcheck]
    fn prop_bounded_cardinality_with_conservation(call_specs: Vec<(u8, u16, u16)>) -> TestResult {
        // call_specs: (bucket_index, bytes_served, bytes_uploaded)
        if call_specs.is_empty() || call_specs.len() > 200 {
            return TestResult::discard();
        }

        let max_series: usize = 5;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let mut metrics = MetricsManager::new();
            metrics.set_per_bucket_config(max_series, HashMap::new());

            let mut total_requests: u64 = 0;
            let mut total_bytes_served: u64 = 0;
            let mut total_bytes_uploaded: u64 = 0;

            for (bucket_idx, served, uploaded) in &call_specs {
                let bucket = format!("bucket-{}", bucket_idx);
                let bs = *served as u64;
                let bu = *uploaded as u64;
                metrics
                    .record_bucket_traffic(&bucket, None, RequestType::Get, bs, 0, bu)
                    .await;
                total_requests += 1;
                total_bytes_served += bs;
                total_bytes_uploaded += bu;
            }

            let snapshot = metrics.get_bucket_traffic_stats().await;

            // (1) Bound
            assert!(
                snapshot.len() <= max_series + 1,
                "cardinality bound violated: {} > {}",
                snapshot.len(),
                max_series + 1
            );

            // (2) Conservation
            let mut sum_requests: u64 = 0;
            let mut sum_bytes_served: u64 = 0;
            let mut sum_bytes_uploaded: u64 = 0;
            for stats in snapshot.values() {
                sum_requests += stats.get_requests + stats.put_requests;
                sum_bytes_served += stats.bytes_served;
                sum_bytes_uploaded += stats.bytes_uploaded;
            }
            assert_eq!(
                sum_requests, total_requests,
                "request conservation violated"
            );
            assert_eq!(
                sum_bytes_served, total_bytes_served,
                "bytes_served conservation violated"
            );
            assert_eq!(
                sum_bytes_uploaded, total_bytes_uploaded,
                "bytes_uploaded conservation violated"
            );
            TestResult::passed()
        })
    }

    // ── Unit test: recording mix across multiple buckets ───────────────────
    // Spec: per-bucket-metrics, task 3.8. Requirements: 1.5.
    //
    // Records a mix of GET and PUT (object/part) across 2 buckets and asserts
    // per-bucket byte totals and per-type counts.

    #[tokio::test]
    async fn test_recording_mix() {
        let metrics = MetricsManager::new();

        // Bucket A: 2 GETs (object reads), 1 PUT (object write).
        metrics
            .record_bucket_traffic("bucket-a", None, RequestType::Get, 100, 0, 0)
            .await;
        metrics
            .record_bucket_traffic("bucket-a", None, RequestType::Get, 200, 0, 0)
            .await;
        metrics
            .record_bucket_traffic("bucket-a", None, RequestType::Put, 0, 0, 500)
            .await;

        // Bucket B: 1 GET, 1 PUT (PutObject), 1 PUT (UploadPart).
        metrics
            .record_bucket_traffic("bucket-b", None, RequestType::Get, 300, 0, 0)
            .await;
        metrics
            .record_bucket_traffic("bucket-b", None, RequestType::Put, 0, 0, 1000)
            .await;
        metrics
            .record_bucket_traffic("bucket-b", None, RequestType::Put, 0, 0, 2000)
            .await;

        let snapshot = metrics.get_bucket_traffic_stats().await;

        let stats_a = snapshot
            .get(&BucketTrafficKey {
                bucket: "bucket-a".to_string(),
                prefix: None,
            })
            .expect("bucket-a should be in snapshot");
        assert_eq!(stats_a.bytes_served, 300); // 100 + 200
        assert_eq!(stats_a.bytes_uploaded, 500);
        assert_eq!(stats_a.get_requests, 2);
        assert_eq!(stats_a.put_requests, 1);

        let stats_b = snapshot
            .get(&BucketTrafficKey {
                bucket: "bucket-b".to_string(),
                prefix: None,
            })
            .expect("bucket-b should be in snapshot");
        assert_eq!(stats_b.bytes_served, 300);
        assert_eq!(stats_b.bytes_uploaded, 3000); // 1000 + 2000
        assert_eq!(stats_b.get_requests, 1);
        // UploadPart maps to Put, so two PUTs total.
        assert_eq!(stats_b.put_requests, 2);

        assert_eq!(snapshot.len(), 2);
    }

    // ── Property 9: /metrics JSON completeness ──────────────────────────
    // **Validates: Requirements 3.1**
    //
    // Every bucket that received ≥1 request appears in SystemMetrics.bucket_traffic
    // (or under __other__ when folded) with all four required fields present.

    #[quickcheck]
    fn prop_metrics_json_completeness(call_specs: Vec<(u8, u16, u16, bool)>) -> TestResult {
        // (bucket_index, bytes_served, bytes_uploaded, is_put)
        if call_specs.is_empty() || call_specs.len() > 150 {
            return TestResult::discard();
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let max_series: usize = 8;
            let mut metrics = MetricsManager::new();
            metrics.set_per_bucket_config(max_series, HashMap::new());

            let mut buckets_with_traffic: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for (bucket_idx, served, uploaded, is_put) in &call_specs {
                let bucket = format!("prop9-bucket-{}", bucket_idx);
                let rtype = if *is_put {
                    RequestType::Put
                } else {
                    RequestType::Get
                };
                metrics
                    .record_bucket_traffic(
                        &bucket,
                        None,
                        rtype,
                        *served as u64,
                        0,
                        *uploaded as u64,
                    )
                    .await;
                buckets_with_traffic.insert(bucket);
            }

            let system_metrics = metrics.collect_metrics().await;
            let bt = &system_metrics.bucket_traffic;
            let overflow_active = bt.contains_key("__other__");

            for bucket in &buckets_with_traffic {
                if !bt.contains_key(bucket) {
                    assert!(
                        overflow_active,
                        "bucket '{}' missing and __other__ not present — lost data",
                        bucket
                    );
                }
            }

            let json_str =
                serde_json::to_string(&system_metrics).expect("SystemMetrics must serialize");
            let json_value: serde_json::Value =
                serde_json::from_str(&json_str).expect("JSON must parse");
            let bt_json = json_value
                .get("bucket_traffic")
                .expect("JSON must have bucket_traffic");
            assert!(bt_json.is_object());

            let required_fields = [
                "bytes_served",
                "bytes_uploaded",
                "get_requests",
                "put_requests",
            ];
            for (key, entry) in bt_json.as_object().unwrap() {
                for field in &required_fields {
                    let val = entry.get(*field);
                    assert!(
                        val.and_then(|v| v.as_u64()).is_some(),
                        "bucket_traffic[\"{}\"].{} missing or not a non-negative integer",
                        key,
                        field
                    );
                }
            }

            assert!(!bt.is_empty());
            TestResult::passed()
        })
    }

    /// Task 8.3: `collect_metrics()` returns a JSON-serializable SystemMetrics
    /// whose bucket_traffic section is keyed by bucket name with the four counter
    /// fields present and populated.
    /// Spec: per-bucket-metrics. Requirements: 3.4
    #[tokio::test]
    async fn test_metrics_json_shape() {
        let metrics = MetricsManager::new();

        metrics
            .record_bucket_traffic("shape-test-bucket", None, RequestType::Get, 1024, 0, 0)
            .await;
        metrics
            .record_bucket_traffic("shape-test-bucket", None, RequestType::Put, 0, 0, 512)
            .await;

        let system_metrics = metrics.collect_metrics().await;
        assert!(!system_metrics.bucket_traffic.is_empty());

        let stats = system_metrics
            .bucket_traffic
            .get("shape-test-bucket")
            .expect("shape-test-bucket should appear in bucket_traffic");
        assert_eq!(stats.bytes_served, 1024);
        assert_eq!(stats.bytes_uploaded, 512);
        assert_eq!(stats.get_requests, 1);
        assert_eq!(stats.put_requests, 1);

        let json_str = serde_json::to_string_pretty(&system_metrics)
            .expect("SystemMetrics should serialize to JSON");
        let json_value: serde_json::Value =
            serde_json::from_str(&json_str).expect("JSON should parse back");
        let bt = json_value
            .get("bucket_traffic")
            .expect("JSON must have a bucket_traffic field");
        let entry = bt
            .get("shape-test-bucket")
            .expect("shape-test-bucket must be present in JSON bucket_traffic");

        for field in &[
            "bytes_served",
            "bytes_uploaded",
            "get_requests",
            "put_requests",
        ] {
            assert!(
                entry.get(field).and_then(|v| v.as_u64()).is_some(),
                "bucket_traffic entry must have numeric field '{}'",
                field
            );
        }
    }

    /// Task 6.5: Exactly-once across paths.
    ///
    /// Simulates the request-completion recording contract across serving paths:
    /// - HTTP proxy / TLS-terminating path: GET (miss, hit, coalesced, range, TLS)
    /// - Signed write-through path: PUT (PutObject) and PUT (UploadPart)
    ///
    /// Each call contributes exactly one request and its byte volume — no double
    /// counting regardless of path or cache outcome. (The production double-count
    /// guard is that the HTTP tail records GET only and the signed handler records
    /// PUT/UploadPart only; this test pins the per-call exactly-once contract those
    /// sites rely on.)
    ///
    /// Spec: per-bucket-metrics. Requirements: 2.5
    #[tokio::test]
    async fn test_exactly_once_across_paths() {
        let metrics = MetricsManager::new();
        let bucket = "exactly-once-bucket";

        // HTTP proxy GET cache miss (origin served 4096 bytes).
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Get, 4096, 0, 0)
            .await;
        // GET cache hit — still exactly one recording.
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Get, 4096, 4096, 0)
            .await;
        // GET coalesced fetch — the coalesced client records once at completion.
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Get, 4096, 0, 0)
            .await;
        // Range request (partial content) — 1024 bytes served.
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Get, 1024, 0, 0)
            .await;
        // Signed PUT write-through — 8192 bytes uploaded.
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Put, 0, 0, 8192)
            .await;
        // UploadPart (maps to Put) — 5 MiB part uploaded.
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Put, 0, 0, 5_242_880)
            .await;
        // TLS-terminating listener GET — 2048 bytes served.
        metrics
            .record_bucket_traffic(bucket, None, RequestType::Get, 2048, 0, 0)
            .await;

        let snapshot = metrics.get_bucket_traffic_stats().await;
        let stats = snapshot
            .get(&BucketTrafficKey {
                bucket: bucket.to_string(),
                prefix: None,
            })
            .expect("bucket should be present in snapshot");

        // 5 GETs (miss + hit + coalesced + range + TLS), 2 PUTs (write-through + UploadPart).
        assert_eq!(stats.get_requests, 5, "5 GET requests across all paths");
        assert_eq!(
            stats.put_requests, 2,
            "2 PUT requests (write-through + UploadPart)"
        );

        // bytes_served: 4096 + 4096 + 4096 + 1024 + 2048 = 15360
        assert_eq!(stats.bytes_served, 15360);
        // bytes_uploaded: 8192 + 5242880 = 5251072
        assert_eq!(stats.bytes_uploaded, 5_251_072);

        // Only one bucket series exists (no overflow, no spurious keys).
        assert_eq!(snapshot.len(), 1);
    }
    #[tokio::test]
    async fn test_request_metrics_classify_response_and_cache_outcomes() {
        let metrics = MetricsManager::new();
        metrics
            .record_response(StatusCode::OK, Duration::from_millis(10), Some(true), false)
            .await;
        metrics
            .record_response(
                StatusCode::NOT_FOUND,
                Duration::from_millis(20),
                Some(false),
                false,
            )
            .await;
        metrics
            .record_response(
                StatusCode::SERVICE_UNAVAILABLE,
                Duration::from_millis(30),
                None,
                true,
            )
            .await;

        let request_metrics = metrics.collect_metrics().await.request_metrics;
        assert_eq!(request_metrics.total_requests, 3);
        assert_eq!(request_metrics.successful_requests, 1);
        assert_eq!(request_metrics.failed_requests, 2);
        assert_eq!(request_metrics.client_error_requests, 1);
        assert_eq!(request_metrics.server_error_requests, 1);
        assert_eq!(request_metrics.rejected_requests, 1);
        assert_eq!(request_metrics.cache_hit_requests, 1);
        assert_eq!(request_metrics.cache_miss_requests, 1);
        assert_eq!(request_metrics.average_response_time_ms, 20);
    }

    /// Permit observability (Requirement TCA 5.1-5.5): `permits_held` and
    /// `permits_available` are derived from the live semaphore, `permits_total`
    /// from the configured value, and `permits_held` is distinct from
    /// `active_requests` (a separate, unrelated connection counter).
    #[tokio::test]
    async fn test_permit_metrics_derive_from_semaphore() {
        let mut metrics = MetricsManager::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
        metrics.set_request_semaphore(Arc::clone(&semaphore));
        metrics.set_active_connections(
            Arc::new(std::sync::atomic::AtomicUsize::new(999)), // deliberately different from permits held
            10,
        );

        // Nothing held yet.
        let request_metrics = metrics.collect_metrics().await.request_metrics;
        assert_eq!(request_metrics.permits_total, 10);
        assert_eq!(request_metrics.permits_held, 0);
        assert_eq!(request_metrics.permits_available, 10);
        // active_requests (connections) must not be conflated with permits_held.
        assert_eq!(request_metrics.active_requests, 999);

        // Hold 3 permits.
        let permits = semaphore.clone().try_acquire_many_owned(3).unwrap();
        let request_metrics = metrics.collect_metrics().await.request_metrics;
        assert_eq!(request_metrics.permits_held, 3);
        assert_eq!(request_metrics.permits_available, 7);
        assert_eq!(request_metrics.permits_total, 10);

        drop(permits);
        let request_metrics = metrics.collect_metrics().await.request_metrics;
        assert_eq!(request_metrics.permits_held, 0);
        assert_eq!(request_metrics.permits_available, 10);
    }

    /// With no semaphore wired (e.g. before `set_request_semaphore` is called),
    /// permit fields degrade to a held-count of zero rather than panicking.
    #[tokio::test]
    async fn test_permit_metrics_default_without_semaphore() {
        let metrics = MetricsManager::new();
        let request_metrics = metrics.collect_metrics().await.request_metrics;
        assert_eq!(request_metrics.permits_available, 0);
        assert_eq!(request_metrics.permits_held, request_metrics.permits_total);
    }
}

#[cfg(test)]
mod bandwidth_metrics_tests {
    use super::*;

    /// Assert that `collect_metrics()` includes the `download_bandwidth` section
    /// with the expected shape (Req 8.5).
    #[tokio::test]
    async fn metrics_includes_download_bandwidth_section() {
        let mm = MetricsManager::new();
        let metrics = mm.collect_metrics().await;

        // The field must be present (it is always populated, never None).
        // When the feature is disabled, `enabled` is false and ceiling is 0.
        assert!(
            !metrics.download_bandwidth.enabled
                || metrics.download_bandwidth.instance_ceiling_bps > 0,
            "disabled limiter should have enabled=false and ceiling_bps=0"
        );

        // Serialize to JSON and verify the field is present.
        let json = serde_json::to_string(&metrics).expect("metrics must serialize");
        assert!(
            json.contains("\"download_bandwidth\""),
            "/metrics JSON must contain the download_bandwidth key"
        );
        assert!(
            json.contains("\"instance_ceiling_bps\""),
            "download_bandwidth must contain instance_ceiling_bps"
        );
        assert!(
            json.contains("\"failopen_total\""),
            "download_bandwidth must contain failopen_total"
        );
        assert!(
            json.contains("\"class_bytes\""),
            "download_bandwidth must contain class_bytes"
        );
    }

    /// Test that HedgeMetrics events surface through collect_hedging_metrics.
    /// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
    #[tokio::test]
    async fn test_hedging_metrics_record_and_collect() {
        use crate::hedged_fetch::HedgeMetrics;
        use std::sync::Arc;

        let mut metrics_manager = MetricsManager::new();
        let hedge_metrics = Arc::new(HedgeMetrics::new());
        metrics_manager.set_hedge_metrics(hedge_metrics.clone());

        // Verify initial state is zero.
        let stats = metrics_manager.collect_hedging_metrics();
        assert_eq!(stats.issued, 0);
        assert_eq!(stats.won, 0);
        assert_eq!(stats.suppressed, 0);

        // Record some events on the shared HedgeMetrics, as hedged_fetch does.
        hedge_metrics.record_issued();
        hedge_metrics.record_issued();
        hedge_metrics.record_won();
        hedge_metrics.record_suppressed();
        hedge_metrics.record_suppressed();
        hedge_metrics.record_suppressed();

        let stats = metrics_manager.collect_hedging_metrics();
        assert_eq!(stats.issued, 2, "issued should be 2");
        assert_eq!(stats.won, 1, "won should be 1");
        assert_eq!(stats.suppressed, 3, "suppressed should be 3");
    }

    /// Test that the /metrics JSON carries the `hedged_requests` section.
    /// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
    #[tokio::test]
    async fn test_hedging_metrics_json_shape() {
        use crate::hedged_fetch::HedgeMetrics;
        use std::sync::Arc;

        let mut metrics_manager = MetricsManager::new();
        let hedge_metrics = Arc::new(HedgeMetrics::new());
        metrics_manager.set_hedge_metrics(hedge_metrics.clone());

        // Increment each counter so they are non-zero in the JSON.
        hedge_metrics.record_issued();
        hedge_metrics.record_won();
        hedge_metrics.record_suppressed();

        let system_metrics = metrics_manager.collect_metrics().await;

        // Verify the struct fields.
        assert_eq!(system_metrics.hedged_requests.issued, 1);
        assert_eq!(system_metrics.hedged_requests.won, 1);
        assert_eq!(system_metrics.hedged_requests.suppressed, 1);

        // Verify JSON serialization contains the hedged_requests section.
        let json_str =
            serde_json::to_string_pretty(&system_metrics).expect("SystemMetrics should serialize");
        let json_value: serde_json::Value =
            serde_json::from_str(&json_str).expect("JSON should parse");

        let hr = json_value
            .get("hedged_requests")
            .expect("JSON must have hedged_requests field");
        assert_eq!(hr.get("issued").and_then(|v| v.as_u64()), Some(1));
        assert_eq!(hr.get("won").and_then(|v| v.as_u64()), Some(1));
        assert_eq!(hr.get("suppressed").and_then(|v| v.as_u64()), Some(1));
    }

    /// Test that without hedge_metrics set, collect returns zeroes.
    /// Spec: hedged-upstream-requests. Requirements: 8.1, 8.2, 8.3
    #[tokio::test]
    async fn test_hedging_metrics_without_wiring() {
        let metrics_manager = MetricsManager::new();

        // No hedge_metrics set: nothing is wired to record into.
        let stats = metrics_manager.collect_hedging_metrics();
        assert_eq!(stats.issued, 0);
        assert_eq!(stats.won, 0);
        assert_eq!(stats.suppressed, 0);

        // SystemMetrics still has the field (zeroed).
        let system_metrics = metrics_manager.collect_metrics().await;
        assert_eq!(system_metrics.hedged_requests.issued, 0);
        assert_eq!(system_metrics.hedged_requests.won, 0);
        assert_eq!(system_metrics.hedged_requests.suppressed, 0);
    }

    /// Pin the scale of `cache.ram_cache_hit_rate_percent`. The source field
    /// (`CacheStatistics::ram_cache_hit_rate`) is a 0.0-1.0 fraction, and this
    /// metric name promises a percentage, so the conversion must be x100. It was
    /// published unscaled before 2.5.0.
    #[test]
    fn ram_cache_hit_rate_is_published_as_a_percentage() {
        assert_eq!(ram_hit_rate_percent(0.0), 0.0);
        assert_eq!(ram_hit_rate_percent(1.0), 100.0);
        assert_eq!(ram_hit_rate_percent(0.4), 40.0);
        assert!(
            (ram_hit_rate_percent(0.855) - 85.5).abs() < 0.01,
            "a 0.855 fraction must publish as ~85.5 percent"
        );
    }
}

/// R12.3's rate arithmetic, and specifically the WINDOW design § 8 corrects itself to.
///
/// # The healthy-idle fixture is the point of this module
///
/// § 8 measured a real pass that ran active for 78.8 of a 240 s window and sat idle the rest,
/// which is correct behaviour once eviction reaches its target. A fixture with one pass and no
/// idle time cannot tell the corrected denominator from the flawed one — both give the same
/// quotient — so `the_healthy_idle_cache_is_keeping_pace` builds the idle case deliberately and
/// `the_wall_clock_denominator_would_report_the_healthy_cache_as_falling_behind` computes the
/// rejected form's answer beside it, from the same fixture, so the divergence is on the record
/// rather than asserted about code that no longer exists.
///
/// These tests are arithmetic. They are necessary and NOT sufficient: a quotient of two constants
/// says nothing about whether a real pass moves the accumulators or whether the published field is
/// the accumulator's quotient. Both of those are asserted at the production end in
/// `tests/eviction_rate_wiring_test.rs`, and the wire spellings in
/// `tests/metrics_wire_shape_test.rs`.
#[cfg(test)]
mod eviction_rate_gauge_tests {
    use super::EvictionRateGauges;
    use crate::cache::EvictionShedAccumulator;
    use crate::journal_consolidator::AdmissionAccumulator;

    /// § 8's own measured pass: 78.8 s active inside a 240 s window, freeing 11.48 GiB.
    const ACTIVE_MICROS: u64 = 78_800_000;
    const WINDOW_SECS: u64 = 240;
    const FREED_BYTES: u64 = 11_480 * 1024 * 1024; // 11.48 GiB, as § 8 quotes it
    const FREED_RANGES: u64 = 11_480;

    /// An admission rate that sits BETWEEN the two candidate shed rates, which is what makes the
    /// fixture discriminating rather than merely idle.
    ///
    /// Active-window shed rate is `FREED_BYTES / 78.8 s` ≈ 152.9 MiB/s; the wall-clock form is
    /// `FREED_BYTES / 240 s` ≈ 50.2 MiB/s. At 100 MiB/s of ingest the deployment is genuinely
    /// bounded — eviction drains half again as fast as bytes arrive whenever it runs, so the duty
    /// cycle settles around 65 % — and the rejected form nonetheless reports it as falling behind.
    const ADMITTED_BYTES: u64 = 100 * 1024 * 1024 * WINDOW_SECS;
    const ADMITTED_RANGES: u64 = 100 * WINDOW_SECS;

    fn healthy_idle() -> EvictionRateGauges {
        EvictionRateGauges::build(
            EvictionShedAccumulator {
                shed_bytes_total: FREED_BYTES,
                shed_ranges_total: FREED_RANGES,
                active_micros_total: ACTIVE_MICROS,
            },
            AdmissionAccumulator {
                admission_bytes_total: ADMITTED_BYTES,
                admission_ranges_total: ADMITTED_RANGES,
            },
            WINDOW_SECS,
            /* total_cache_size */ 800,
            /* projected_cache_size */ None,
            /* max_cache_size */ 1000,
            /* eviction_trigger_percent */ 95,
        )
    }

    /// The corrected window: a cache that evicts for a third of the window and is comfortably
    /// bounded reads `keeping_pace = true`.
    ///
    /// This is the assertion the window red side reds. If it ever passes under a
    /// wall-clock-since-first-pass denominator, the fixture has lost its idle time and this module
    /// has stopped testing its own subject.
    #[test]
    fn the_healthy_idle_cache_is_keeping_pace() {
        let g = healthy_idle();
        let shed = g
            .eviction_shed_bytes_per_sec
            .expect("a completed pass gives a shed rate");
        let admitted = g
            .admission_bytes_per_sec
            .expect("a non-zero uptime gives an admission rate");
        assert_eq!(
            g.eviction_keeping_pace,
            Some(true),
            "§ 8's measured pass freed {FREED_BYTES} bytes in {ACTIVE_MICROS} us of ACTIVE time \
             inside a {WINDOW_SECS} s window while {ADMITTED_BYTES} bytes arrived. Shedding at \
             {shed:.0} B/s against {admitted:.0} B/s of ingest is keeping pace. A `false` here \
             means the denominator is no longer summed active intervals — a rate over wall-clock \
             reads eviction's correct idle time as falling behind, which is the false alarm § 8 \
             tells its own implementer to fix before shipping."
        );
        assert!(
            shed > admitted,
            "the fixture must have real headroom or the verdict is a coin flip: {shed} vs \
             {admitted}"
        );
    }

    /// The REJECTED form's answer, computed from the same fixture so the divergence is a
    /// measurement rather than a claim.
    ///
    /// Nothing in `src/` computes this; it exists so that the two windows' verdicts are both on
    /// the record and a future reader can see that they genuinely differ on this input. Without
    /// it, "a wall-clock denominator would alarm falsely" is an assertion about deleted code.
    #[test]
    fn the_wall_clock_denominator_would_report_the_healthy_cache_as_falling_behind() {
        let g = healthy_idle();
        let wall_clock_shed = g.eviction_shed_bytes_total as f64 / WINDOW_SECS as f64;
        let admitted = g.admission_bytes_per_sec.unwrap();
        assert!(
            wall_clock_shed < admitted,
            "the fixture is only discriminating if the rejected form FAILS on it: wall-clock \
             shed {wall_clock_shed} must be below ingest {admitted}, or both windows agree and \
             the corrected one is untested"
        );
        let wall_clock_ranges = g.eviction_shed_ranges_total as f64 / WINDOW_SECS as f64;
        assert!(
            wall_clock_ranges < g.admission_ranges_per_sec.unwrap(),
            "and in the ranges unit too, so the verdict flips whichever dimension is read"
        );
    }

    /// The published rates ARE the published counters' quotients, so a consumer can audit them.
    #[test]
    fn every_rate_is_the_quotient_of_the_counters_beside_it() {
        let g = healthy_idle();
        let active_secs = g.eviction_active_micros_total as f64 / 1_000_000.0;
        assert_eq!(
            g.eviction_shed_bytes_per_sec,
            Some(g.eviction_shed_bytes_total as f64 / active_secs)
        );
        assert_eq!(
            g.eviction_shed_ranges_per_sec,
            Some(g.eviction_shed_ranges_total as f64 / active_secs)
        );
        assert_eq!(
            g.admission_bytes_per_sec,
            Some(g.admission_bytes_total as f64 / WINDOW_SECS as f64)
        );
        assert_eq!(
            g.admission_ranges_per_sec,
            Some(g.admission_ranges_total as f64 / WINDOW_SECS as f64)
        );
    }

    /// Before any pass, every shed rate and the verdict are `null` — NOT zero, and NOT `false`.
    #[test]
    fn no_completed_pass_reads_unknown_rather_than_shedding_nothing() {
        let g = EvictionRateGauges::build(
            EvictionShedAccumulator::default(),
            AdmissionAccumulator {
                admission_bytes_total: 4096,
                admission_ranges_total: 4,
            },
            60,
            800,
            None,
            1000,
            95,
        );
        assert_eq!(
            g.eviction_shed_bytes_per_sec, None,
            "an empty denominator is not a rate of zero. `0.0` would state `shedding nothing` \
             about a cache that has had nothing to shed"
        );
        assert_eq!(g.eviction_shed_ranges_per_sec, None);
        assert_eq!(
            g.eviction_keeping_pace, None,
            "a verdict an operator alarms on must not read `false` for want of a measurement — \
             which is what a plain `bool` would publish on every cold start"
        );
        // The counters beside them still read as real zeros, which is what a counter's zero always
        // means, and the admission side is fully measured.
        assert_eq!(g.eviction_shed_bytes_total, 0);
        assert_eq!(g.admission_bytes_per_sec, Some(4096.0 / 60.0));
    }

    /// The verdict is `false` when it should be, so `Some(true)` above is not vacuous.
    #[test]
    fn a_cache_falling_behind_in_bytes_reads_false() {
        let g = EvictionRateGauges::build(
            EvictionShedAccumulator {
                shed_bytes_total: 1_000,
                shed_ranges_total: 1_000,
                active_micros_total: 1_000_000,
            },
            AdmissionAccumulator {
                admission_bytes_total: 10_000,
                admission_ranges_total: 10,
            },
            1,
            800,
            None,
            1000,
            95,
        );
        assert_eq!(g.eviction_shed_bytes_per_sec, Some(1_000.0));
        assert_eq!(g.admission_bytes_per_sec, Some(10_000.0));
        assert_eq!(g.eviction_keeping_pace, Some(false));
    }

    /// A cache keeping pace in BYTES while falling behind in RANGES reads `false`.
    ///
    /// § 8 requires two units because the two failure modes have different fixes — mean object
    /// size versus the range count — so a verdict computed from bytes alone would publish
    /// `true` for the range-starved cache, which is the exact substitution that makes a name
    /// disagree with the figure behind it.
    #[test]
    fn keeping_pace_in_bytes_is_not_enough_when_ranges_fall_behind() {
        let g = EvictionRateGauges::build(
            EvictionShedAccumulator {
                shed_bytes_total: 1_000_000,
                shed_ranges_total: 2,
                active_micros_total: 1_000_000,
            },
            AdmissionAccumulator {
                admission_bytes_total: 1_000,
                admission_ranges_total: 100,
            },
            1,
            800,
            None,
            1000,
            95,
        );
        assert_eq!(g.eviction_shed_bytes_per_sec, Some(1_000_000.0));
        assert_eq!(g.admission_bytes_per_sec, Some(1_000.0));
        assert_eq!(
            g.eviction_keeping_pace,
            Some(false),
            "shedding 2 ranges/s against 100 arriving is falling behind however many bytes \
             each one carried"
        );
    }

    /// `eviction_backlog_bytes` is signed and measured on the published total, so its healthy
    /// reading is negative, and it is `null` when eviction is off. A negative value does not mean
    /// no pass is due: the figure a pass fires on is `eviction_backlog_projected_bytes`.
    #[test]
    fn the_backlog_is_signed_and_negative_when_healthy() {
        let below_trigger = EvictionRateGauges::build(
            EvictionShedAccumulator::default(),
            AdmissionAccumulator::default(),
            1,
            /* total_cache_size */ 500,
            /* projected_cache_size */ None,
            /* max_cache_size */ 1000,
            /* trigger */ 95,
        );
        assert_eq!(
            below_trigger.eviction_backlog_bytes,
            Some(-450),
            "500 bytes against a 950-byte trigger is 450 bytes of HEADROOM, and the healthy \
             reading is negative. An unsigned gauge would have to discard it"
        );

        let over_trigger = EvictionRateGauges::build(
            EvictionShedAccumulator::default(),
            AdmissionAccumulator::default(),
            1,
            990,
            None,
            1000,
            95,
        );
        assert_eq!(over_trigger.eviction_backlog_bytes, Some(40));

        let eviction_disabled = EvictionRateGauges::build(
            EvictionShedAccumulator::default(),
            AdmissionAccumulator::default(),
            1,
            990,
            /* projected_cache_size */ Some(1_200),
            /* max_cache_size */ 0,
            95,
        );
        assert_eq!(
            eviction_disabled.eviction_backlog_bytes, None,
            "with no size bound there is no trigger to measure against, and 0 would read as \
             `exactly at the trigger`"
        );
        assert_eq!(
            eviction_disabled.eviction_backlog_projected_bytes, None,
            "and a projected reading cannot create a threshold that is not configured"
        );
    }

    /// The projected backlog is the PREDICATE, and the resident one can read healthy while it is
    /// positive. The two are computed from the same threshold and different numerators, so this
    /// pins the state an operator alarming on the resident field alone would miss.
    #[test]
    fn the_projected_backlog_is_positive_while_the_resident_one_reads_headroom() {
        let g = EvictionRateGauges::build(
            EvictionShedAccumulator::default(),
            AdmissionAccumulator::default(),
            1,
            /* total_cache_size */ 900,
            /* projected_cache_size */ Some(1_100),
            /* max_cache_size */ 1000,
            /* trigger */ 95,
        );
        assert_eq!(
            g.eviction_backlog_bytes,
            Some(-50),
            "900 resident bytes against a 950-byte trigger still reads as headroom"
        );
        assert_eq!(
            g.eviction_backlog_projected_bytes,
            Some(150),
            "while the figure the trigger compares is 150 bytes OVER it, which is why a pass \
             fires and the resident field alone cannot explain it"
        );
    }

    /// Before the first trigger evaluation the projected backlog is `null`, not a zero backlog
    /// and not the resident figure quietly reused.
    #[test]
    fn no_projected_reading_yet_reads_unknown_rather_than_no_backlog() {
        let g = EvictionRateGauges::build(
            EvictionShedAccumulator::default(),
            AdmissionAccumulator::default(),
            1,
            /* total_cache_size */ 990,
            /* projected_cache_size */ None,
            /* max_cache_size */ 1000,
            /* trigger */ 95,
        );
        assert_eq!(
            g.eviction_backlog_projected_bytes, None,
            "a proxy that has not evaluated the trigger has no reading, and 0 would read as \
             `exactly at the trigger`"
        );
        assert_eq!(
            g.eviction_backlog_bytes,
            Some(40),
            "the resident figure is still published, so the payload is never silent on capacity"
        );
    }

    /// The two `connection_keepalive` maps must be keyed by the SAME string.
    ///
    /// `connections_reused` is derived per key as
    /// `total_requests - connections_created`. The request side keys on
    /// `S3RequestContext::host` (the signed Host); the connector only has a `Uri`,
    /// which IP distribution may have rewritten to an IP literal, so it must
    /// reverse-map. The DIVERGENCE this constructs is the traced route 1: the IP
    /// the connection was made to has since been excluded by `remove_ip`, so
    /// `get_hostname_for_ip` — which searches only the live distributor sets —
    /// returns `None` and the old code fell back to the dotted quad.
    ///
    /// The assertion is on the two maps' KEY SETS, not on a value: a fixture where
    /// both producers already agree passes whatever the code does, so the value is
    /// not the predicate. RED SIDE: swap `get_metrics_endpoint_key` for
    /// `get_hostname_for_ip` below and this fails with the dotted quad in
    /// `connections_created` and absent from `connections_reused`.
    ///
    /// Scope: this exercises the key-derivation the connector calls, not the
    /// connector's TCP/TLS path, which cannot run in a unit test.
    #[tokio::test]
    async fn keepalive_maps_share_one_key_set_when_the_connected_ip_is_excluded() {
        use std::collections::BTreeSet;
        use std::net::{IpAddr, Ipv4Addr};

        const ENDPOINT: &str = "s3.us-west-2.amazonaws.com";
        let live = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let excluded = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        let config = crate::config::ConnectionPoolConfig::default();
        let mut manager =
            crate::connection_pool::ConnectionPoolManager::new_with_config(config).unwrap();
        manager.ip_distributors.insert(
            ENDPOINT.to_string(),
            crate::connection_pool::IpDistributor::new(vec![live, excluded]),
        );
        manager.seed_resolved_ips_for_test(ENDPOINT, vec![live, excluded]);

        // Construct the divergence: the IP the connection was made to is excluded.
        manager
            .get_distributor_mut(ENDPOINT)
            .unwrap()
            .remove_ip(excluded, "test: construct the key divergence");
        assert!(
            manager.get_hostname_for_ip(&excluded).is_none(),
            "fixture precondition: the live-set lookup must NOT resolve an excluded \
             IP, or this test proves nothing about the divergence"
        );

        // What the connector does at its recording site: it holds only the IP.
        let metrics = crate::metrics::MetricsManager::new();
        let key = manager
            .get_metrics_endpoint_key(&excluded)
            .unwrap_or_else(|| excluded.to_string());
        metrics.record_connection_created(&key).await;
        // What the request side does: it holds the signed Host.
        metrics.record_request_to_endpoint(ENDPOINT).await;

        let stats = metrics.get_connection_keepalive_stats().await;
        let created: BTreeSet<&str> = stats.connections_created.keys().map(|s| &**s).collect();
        let reused: BTreeSet<&str> = stats.connections_reused.keys().map(|s| &**s).collect();
        assert_eq!(
            created, reused,
            "the two connection_keepalive maps must share one key set; \
             created={:?} reused={:?}",
            created, reused
        );
    }
}
/// `cache.ram_cache_entries` publishes the RAM tier's live entry count.
///
/// # RED SIDE
///
/// With `collect_cache_metrics`' `ram_cache_entries: ram_stats...entries_count` line replaced by
/// `ram_cache_entries: 0`, the two-entry assertion fails with `left: 0, right: 2`.
#[cfg(test)]
mod ram_cache_entries_metric_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_published_entry_count_follows_the_ram_tier() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
            temp_dir.path().to_path_buf(),
            true, // RAM tier under test
            16 * 1024 * 1024,
            1_000_000_000,
            crate::cache::CacheEvictionAlgorithm::TinyLFU,
            1024,
            false,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            false,
            crate::config::SharedStorageConfig::default(),
            10.0,
            false,
            Duration::from_secs(86400),
            crate::config::MetadataCacheConfig::default(),
            95,
            80,
            true,
            Duration::from_secs(60),
            1_048_576,
            false,
            Duration::from_secs(10),
            4,
            Duration::from_secs(5),
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager
            .get_journal_consolidator()
            .await
            .expect("consolidator set by create_configured_disk_cache_manager")
            .initialize()
            .await
            .unwrap();
        let mut manager = MetricsManager::new();
        manager.set_cache_manager(cache_manager.clone());
        let empty = manager
            .collect_metrics()
            .await
            .cache
            .expect("cache section");
        assert_eq!(empty.ram_cache_entries, 0, "a fresh RAM tier holds nothing");
        for (i, key) in ["bucket/a", "bucket/b"].iter().enumerate() {
            assert!(
                cache_manager.promote_range_to_ram_cache_frame(
                    key,
                    (0, 1023),
                    vec![i as u8; 1024],
                    crate::compression::CompressionAlgorithm::None,
                    "\"e\"".to_string(),
                    String::new(),
                    1u64 << 40,
                ),
                "PRECONDITION MISSING: {key} was not promoted into the RAM tier"
            );
        }
        let two = manager
            .collect_metrics()
            .await
            .cache
            .expect("cache section");
        assert_eq!(
            two.ram_cache_entries, 2,
            "cache.ram_cache_entries must report the RAM tier's resident entry count"
        );
    }
}
/// `cache.read_cache_size_inverted_secs` publishes the age the read-size guard computed.
///
/// An excess of the staged figure over `total_size` is built through the existing seams
/// (`seed_shard_figure_for_tests`, `activate`, the consolidator's size state), so the first
/// fold-derived read is inverted and `get_cache_size_stats` reports an age of at least 1.
///
/// # RED SIDE
///
/// With `collect_cache_metrics`' `read_cache_size_inverted_secs: sizes.read_cache_size_inverted_secs`
/// line replaced by `read_cache_size_inverted_secs: 0`, the published-age assertion fails.
#[cfg(test)]
mod inverted_age_metric_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_published_age_is_the_guards_figure() {
        const STAGED_BYTES: u64 = 1_048_576;
        const TOTAL_BYTES: u64 = 4_096;
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = temp_dir.path();
        for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
            dir.to_path_buf(),
            false,
            0,
            1_000_000_000,
            crate::cache::CacheEvictionAlgorithm::TinyLFU,
            1024,
            false,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            false,
            crate::config::SharedStorageConfig::default(),
            10.0,
            false,
            Duration::from_secs(86400),
            crate::config::MetadataCacheConfig::default(),
            95,
            80,
            true,
            Duration::from_secs(60),
            1_048_576,
            false,
            Duration::from_secs(10),
            4,
            Duration::from_secs(5),
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        let consolidator = cache_manager
            .get_journal_consolidator()
            .await
            .expect("consolidator set by create_configured_disk_cache_manager");
        consolidator.initialize().await.unwrap();
        let mut state = consolidator.load_size_state().await.unwrap();
        state.total_size = TOTAL_BYTES;
        consolidator.persist_size_state(&state).await.unwrap();
        crate::staged_fold::seed_shard_figure_for_tests(dir, "test-bucket", "3f", STAGED_BYTES, 1);
        assert!(
            crate::staged_authority::activate(dir, "host:test").unwrap(),
            "PRECONDITION MISSING: the staged authority did not activate"
        );
        crate::staged_fold::invalidate_authority_snapshot_for_tests(dir);
        let mut manager = MetricsManager::new();
        manager.set_cache_manager(cache_manager.clone());
        let published = manager
            .collect_metrics()
            .await
            .cache
            .expect("cache section")
            .read_cache_size_inverted_secs;
        assert!(
            (1..60).contains(&published),
            "cache.read_cache_size_inverted_secs must carry the guard's age of the excess just \
             built (1 to 59 s), got {published}"
        );
    }
}
/// `inflight_memory.granted_total` and `granted_bytes_total` reach `/metrics` from the ledger.
///
/// # RED SIDE
///
/// With `collect_inflight_memory_metrics`' `granted_total: ledger.granted_total()` line replaced
/// by `granted_total: 0`, this fails with `left: 0, right: 1`.
#[cfg(test)]
mod inflight_grant_metric_tests {
    use super::*;
    #[tokio::test]
    async fn the_ledger_grant_counters_are_published() {
        let ledger = Arc::new(crate::inflight_ledger::InflightLedger::new(1_000_000));
        let mut manager = MetricsManager::new();
        manager.set_inflight_ledger(ledger.clone());
        let _held = ledger.try_reserve(4096).expect("should admit");
        let im = manager.collect_inflight_memory_metrics();
        assert_eq!(
            im.granted_total, 1,
            "inflight_memory.granted_total must be published"
        );
        assert_eq!(
            im.granted_bytes_total, 4096,
            "inflight_memory.granted_bytes_total must be published"
        );
    }
}
