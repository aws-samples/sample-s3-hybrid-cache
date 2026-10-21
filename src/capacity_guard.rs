//! Danger-band publication admission guard for cache **data** writes.
//!
//! Spec: `cache-eviction-at-scale` task 36, as amended 2026-09-05 against a finding of
//! `design-review-2026-09-05-staged-accounting-C.md` (amendments A1-A5), and § 16.6.2.
//!
//! # What this closes
//!
//! `CacheManager::disk_safety_refusal` is the codebase's only physical free-space
//! admission check, and it guards **two** call sites: the buffered write-through PUT and
//! the write-cache streaming sink (plus `CreateMultipartUpload`, R16.10). Every path that
//! publishes GET-response data was unguarded. `u4-get-publication.md` inventoried nine
//! distinct disk-write mechanisms that publish cache data bytes and found only those two
//! guarded, so a concurrent cross-proxy miss storm could drive a shared volume to zero
//! free space — at which point the `.meta` writes, journal appends, ledger appends and
//! `size_state.json` writes fail too, and the coordination machinery dies with the disk.
//!
//! # The three mechanisms, and why each is load-bearing
//!
//! **1. A demand-refreshed reading, single-flighted (A3).** A `statvfs` per fill is
//! wasteful at small-object miss rates (thousands/s). One process-wide reading is
//! refreshed *by a fill that observes it stale*, never by a timer, so an idle instance
//! performs zero checks by construction — matching the consolidation cycle's own
//! idle-detection posture. Concurrent fills observing a stale reading do **not** each
//! launch their own `statvfs`: one refresh is in flight per process and the others await
//! its result, which is what makes "one syscall per 5 s window" true rather than merely
//! intended.
//!
//! **2. A danger band derived from the floor (A1).** Outside the band a fill consults the
//! cached reading and nothing else. Inside it, every fill takes a **fresh** `statvfs`.
//! Cost concentrates exactly where precision matters, and the band introduces no new
//! tunable — see [`DANGER_BAND_FLOOR_MULTIPLIER`] for the sizing arithmetic, which is
//! **aggregate across writers**, not per-writer.
//!
//! **3. A fleet-wide outstanding-byte reservation inside the band (A2).** A fresh
//! `statvfs` *reads* free space; it does not *reserve* it. Several proxies can each read
//! the same fresh value, each admit a fill that individually fits, and collectively
//! exceed it — the race survives untouched inside the band, which is precisely where the
//! guard is supposed to bind. So inside the band a fill takes a reservation on the shared
//! volume, held until publication commits or aborts, summed across **every** instance
//! under one shared lock. Without this the guard is a time-of-check/time-of-use race with
//! extra steps.
//!
//! # Degradation shape: decline the CACHING, never the S3 operation
//!
//! Modelled on `WriteCacheManager::try_reserve`'s `size > max_object_size` refusal and on
//! `disk_safety_refusal`: a decline means the object is not cached, exactly as if caching
//! were disabled for that key. The client's GET still receives its bytes (the tee already
//! streams, and the page-widening assembly is done in memory rather than by a disk
//! re-read), and a PUT still reaches S3. Reactive `ENOSPC` handling stays where it is, as
//! the last-resort backstop.
//!
//! # Failure posture, and where it deliberately differs from `disk_safety_refusal`
//!
//! An unreadable `statvfs` fails **open**, same as `disk_safety_refusal` (R7.5): an
//! unavailable syscall is an environment problem, not evidence the disk is full, and
//! treating it as full would silently stop caching — the outage the write-cache spec
//! exists to fix.
//!
//! An unreadable *peer reservation* fails **closed**. That is not an inconsistency: it is
//! only reachable **inside** the band, where the volume is already near its floor, and
//! reading a peer's outstanding bytes as zero there is exactly the under-count that lets
//! the collective overshoot through. Declining one fill on a nearly-full volume is cheap;
//! admitting against an unknown fleet total is the failure this whole guard exists to
//! prevent. The two cases carry different reason labels
//! ([`DANGER_BAND_SKIP_REASON`] and [`DANGER_BAND_UNREADABLE_SKIP_REASON`]) so an
//! operator can tell them apart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Directory under `metadata/` holding shard state, per design § 3.2. Mirrors
/// `staged_fold`'s constant of the same name rather than importing it, so neither module
/// owns the other's layout.
const SHARD_STATE_DIR: &str = "_shard_state";

/// Directory under `_shard_state/` holding the per-instance outstanding-byte reservations.
const RESERVATIONS_DIR: &str = "capacity_reservations";

/// Extension for a per-instance reservation document.
const RESERVATION_EXT: &str = "resv";

/// Name of the single shared lock every instance takes for the read-sum-write.
///
/// **One lock path for the whole fleet, deliberately.** A per-instance lock would let two
/// instances read the same sum concurrently and both admit, which is the exact race A2
/// names. The lock is what makes the sum a reservation rather than a reading.
const RESERVATIONS_LOCK: &str = "_reservations.lock";

/// Schema version of a reservation document.
const RESERVATION_SCHEMA: u32 = 1;

/// How long a reservation stays counted without being refreshed.
///
/// A lease, so a crashed instance's outstanding bytes expire rather than permanently
/// shrinking the fleet's admissible headroom (A2). 120 s is comfortably longer than any
/// single publication takes — the longest is a full-object store of
/// `write_cache_max_object_size` bytes — and short enough that a crash costs at most one
/// lease of pessimism. It is not extended on progress: an over-long publication is
/// re-admitted by the next fill's own check, whereas extending would let a stuck writer
/// hold headroom indefinitely.
const RESERVATION_LEASE_SECS: u64 = 120;

/// Maximum age of the process-wide cached free-space reading before a fill refreshes it.
///
/// 5 s, matching the consolidation cycle. The exposure this creates is what
/// [`DANGER_BAND_FLOOR_MULTIPLIER`]'s arithmetic is sized against.
pub const FREE_SPACE_READING_MAX_AGE: Duration = Duration::from_secs(5);

/// Multiplier on the free-space floor that defines the danger band's outer edge: a fill is
/// outside the band while `available_cached > incoming + MULTIPLIER × floor`.
///
/// # THE SIZING ARITHMETIC IS AGGREGATE ACROSS WRITERS, NOT PER-WRITER (A1)
///
/// Outside the band a fill is admitted against a reading up to
/// [`FREE_SPACE_READING_MAX_AGE`] old, and **every** writer sharing the volume may do that
/// concurrently against its own stale reading. So the worst-case consumption between one
/// instance's refresh and the next is not that instance's fill rate — it is the sum across
/// writers, plus the largest single body that can be admitted on top of it:
///
/// ```text
///   Exposure  =  FREE_SPACE_READING_MAX_AGE × Σ(fill rate of every writer on the volume)
///              +  largest admitted body
///
///   Band      =  DANGER_BAND_FLOOR_MULTIPLIER × floor
///
///   For the band to be entered before the floor is breached:   Band ≥ Exposure
///   ⇒  floor  ≥  ( 5 s × Σ fill rate  +  largest body ) / DANGER_BAND_FLOOR_MULTIPLIER
/// ```
///
/// "Every writer sharing the volume" includes any non-proxy neighbour on the same filer,
/// which is why the sizing rule is stated in terms of the volume rather than the fleet.
///
/// **Worked against this project's own recorded figures, and the default does not cover
/// them.** At the recorded ~2.0 GiB/s aggregate fleet cache-miss ingest and a 256 MiB
/// `write_cache_max_object_size`:
///
/// ```text
///   Exposure = 5 s × 2.0 GiB/s + 256 MiB  =  10.25 GiB
///   floor    ≥ 10.25 GiB / 4              =  2.5625 GiB
/// ```
///
/// The 1 GiB default gives a 4 GiB band against a 10.25 GiB exposure, so a fleet running
/// at the recorded rate **can transit the band between refreshes**. That is stated rather
/// than hidden: the default is sized for moderate single-instance traffic and a small
/// volume, not for a fleet, and `cache.disk_safety_free_space_floor` exists so an operator
/// can size it. What absorbs the transit is the floor itself — the next refresh lands
/// inside the band and per-fill exact checks plus the fleet reservation take over.
///
/// 4 rather than some other integer because the band has to be a multiple of the one
/// deployment-dependent quantity already in the configuration; introducing a second knob
/// would let an operator widen the band without the floor, which is the one combination
/// the arithmetic above forbids.
pub const DANGER_BAND_FLOOR_MULTIPLIER: u64 = 4;

/// Reason label when the danger-band guard declines a publication on capacity grounds.
///
/// Distinct from `cache::DISK_SAFETY_SKIP_REASON` because the two answer different
/// questions: that one is "would this upload take the cache past `max_cache_size` or
/// under the floor", this one is "would this fill, plus every fill the fleet already has
/// outstanding, take the volume under the floor".
pub const DANGER_BAND_SKIP_REASON: &str = "disk_danger_band";

/// Reason label when the guard declines because the fleet's outstanding total could not be
/// read. See the module doc for why this arm fails closed while `statvfs` fails open.
pub const DANGER_BAND_UNREADABLE_SKIP_REASON: &str = "disk_danger_band_unreadable";

/// The **enumerated** set of publication boundaries this guard is wired at.
///
/// # Every path is named, because "all paths" is not actionable
///
/// `u4-get-publication.md` is the authoritative inventory and found **nine** distinct
/// disk-write mechanisms that publish cache data bytes. The whole point of that
/// investigation is that the set is closed and enumerable, so the guard's coverage is
/// stated per path rather than asserted wholesale. Disposition of all nine:
///
/// | # | Path | Entry point | Guarded here? |
/// |---|---|---|---|
/// | P1 | buffered range/part publication | `DiskCacheManager::store_range` | **yes** — [`PublicationPath::P1BufferedRange`] |
/// | P2 | full-object wrapper over P1 (test-only callers) | `DiskCacheManager::store_full_object_as_range` | **yes, transitively** — it delegates to `store_range` and does no write of its own, so P1's check covers it for free. Test-only reachability does not change that, and leaving a delegating wrapper unguarded would be a second entry point to the same bytes |
/// | P3a | GET-response incremental tee | `DiskCacheManager::begin_incremental_range_write` | **yes** — [`PublicationPath::P3aGetResponseTee`] |
/// | P3b | write-cache PUT sink | `CacheManager::open_write_cache_sink` | pre-existing `disk_safety_refusal`; **also** covered here, because the sink's range write goes through P3a |
/// | P3c | multipart part sink | `CacheManager::open_multipart_part_sink` → `begin_incremental_part_write` | **yes** — [`PublicationPath::P3cMultipartPartSink`] |
/// | P4 | production full-object GET publication | `CacheManager::store_full_object_as_range_new` | **yes** — [`PublicationPath::P4FullObjectStore`] |
/// | P5 | range GET-miss fill, incl. gap-repair partial fill | `RangeHandler::store_range_new_storage` | **yes, transitively** via P1; it is a thin delegate with no write of its own |
/// | P6 | page-widened publication | `handle_widened_suffix_response`, `fetch_and_cache_page_missing_ranges` | **yes, transitively** via P5 → P1 |
/// | P7 | write-through PUT, buffered + streaming | `store_put_as_write_cached_range_with_ttl`, `open_write_cache_sink` | pre-existing `disk_safety_refusal`; the streaming half also passes through P3a |
/// | P8 | `UploadPart` streaming | `finalize_and_record_cached_part` → `begin_incremental_part_write` | **yes** — same boundary as P3c |
/// | P9a | `CompleteMultipartUpload`, production | `SignedPutHandler::finalize_multipart_upload` | **no, and nothing to guard.** It renames already-published part files into `ranges/` and writes a `.meta`; it publishes no new data bytes, so there is no incoming figure to admit. The bytes were admitted at P8 |
/// | P9b | test-only completion twin | `CacheManager::complete_multipart_upload` | **no, deliberately.** Zero `src/` callers (only five integration-test files), and it performs its own `std::fs::write` rather than routing through a shared helper — so guarding it would mean adding a live capacity decision to code production never reaches, and would let a declining guard silently change what those five fixtures build. P2 is guarded for free because it delegates; this one is not free, and buys nothing |
///
/// So the boundaries that carry a check are the four variants below, and between them they
/// cover P1, P2, P3a, P3b, P3c, P4, P5, P6, P7 and P8. Deliberately uncovered: P9a
/// (publishes no new bytes) and P9b (test-only, would need its own check).
///
/// **Boundary, not caller.** Each check sits inside the function that creates or publishes
/// the bytes, not at its call sites — P5 has four call sites and P1 has nine, and a
/// caller-only check leaves every alternate path open. The mixed spawn/await shape at P5's
/// call sites (three `tokio::spawn`, one awaited synchronously for page assembly) is
/// exactly why: a decline has a different local effect at each, and the awaited one is
/// safe only because it assembles the Page in memory from the fetched buffers rather than
/// re-reading what it just stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationPath {
    /// **P1** — `DiskCacheManager::store_range`, the buffered range/part publication.
    /// Also covers **P2** (delegating wrapper), **P5** (`store_range_new_storage`) and
    /// **P6** (page widening, which routes through P5).
    P1BufferedRange,
    /// **P3a** — `DiskCacheManager::begin_incremental_range_write`, the GET-response
    /// incremental tee. Also the range write underneath **P3b**/**P7**'s streaming sink.
    P3aGetResponseTee,
    /// **P3c / P8** — `DiskCacheManager::begin_incremental_part_write`, the multipart
    /// `UploadPart` staging sink, which `u4` records as applying none of the write-cache
    /// sink's gates.
    P3cMultipartPartSink,
    /// **P4** — `CacheManager::store_full_object_as_range_new`, the production
    /// full-object GET publication.
    P4FullObjectStore,
}

impl PublicationPath {
    /// Short label for logs and metrics reasons.
    pub fn label(self) -> &'static str {
        match self {
            Self::P1BufferedRange => "P1_store_range",
            Self::P3aGetResponseTee => "P3a_get_response_tee",
            Self::P3cMultipartPartSink => "P3c_multipart_part_sink",
            Self::P4FullObjectStore => "P4_full_object_store",
        }
    }
}

/// Why a publication was declined, with the figures the decision was actually taken on.
///
/// The predicate is reported, not merely the verdict: `available_bytes` is the **fresh**
/// `statvfs` reading the comparison used, and `fleet_outstanding_bytes` is the summed
/// reservation total across every instance. A decline that says only "capacity" is
/// undiagnosable, which is the same reasoning § 16.6.2 applies to `shortfall_bytes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclineDetail {
    /// One of [`DANGER_BAND_SKIP_REASON`] / [`DANGER_BAND_UNREADABLE_SKIP_REASON`].
    pub reason: &'static str,
    /// Which enumerated publication boundary declined.
    pub path: PublicationPath,
    /// Bytes the publication wanted to write.
    pub incoming_bytes: u64,
    /// The fresh `statvfs` reading, or `None` when the fleet total was unreadable and no
    /// comparison was reached.
    pub available_bytes: Option<u64>,
    /// Σ live reservations across every instance, or `None` when unreadable.
    pub fleet_outstanding_bytes: Option<u64>,
    /// The configured floor the comparison reserved on top of everything else.
    pub floor_bytes: u64,
}

/// The outcome of an admission decision.
///
/// Three arms rather than two, because "admitted without a reservation" and "admitted
/// holding one" are materially different states and collapsing them would hide whether the
/// fleet-wide mechanism was engaged at all.
#[derive(Debug)]
pub enum Admission {
    /// Admitted outside the danger band. No `statvfs` beyond the amortised refresh, no
    /// reservation, no shared-volume I/O.
    OutsideBand,
    /// Admitted inside the danger band, holding a fleet-wide reservation that releases on
    /// drop. **Hold it until the publication commits or aborts**; dropping it early
    /// re-opens the window it exists to close.
    Reserved(FleetReservation),
    /// Declined. Skip the caching; do not touch the S3 operation.
    Declined(DeclineDetail),
}

impl Admission {
    /// The decline detail, if this is a decline.
    pub fn decline(&self) -> Option<&DeclineDetail> {
        match self {
            Self::Declined(d) => Some(d),
            _ => None,
        }
    }

    /// The reservation to hold for the publication's lifetime, if one was taken.
    pub(crate) fn into_reservation(self) -> Option<FleetReservation> {
        match self {
            Self::Reserved(r) => Some(r),
            _ => None,
        }
    }
}

/// One instance's outstanding-byte reservation, on the shared volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReservationDoc {
    schema: u32,
    instance_id: String,
    /// Bytes this instance currently has admitted-but-unpublished inside the band.
    outstanding_bytes: u64,
    /// Seconds since the epoch when this document was last written.
    updated_at: u64,
    /// Lease length, seconds. Past `updated_at + lease_secs` the bytes stop counting.
    lease_secs: u64,
}

impl ReservationDoc {
    fn is_live_at(&self, now_secs: u64) -> bool {
        self.updated_at.saturating_add(self.lease_secs) > now_secs
    }
}

/// An RAII handle for admitted-but-unpublished bytes counted fleet-wide.
///
/// Released on drop, including on cancellation and panic — the same shape as
/// `WriteCacheReservation`, and for the same reason: a publication that is dropped
/// mid-flight must not leave the fleet believing its bytes are still coming.
#[derive(Debug)]
pub struct FleetReservation {
    cache_dir: PathBuf,
    instance_id: String,
    bytes: u64,
    /// The admitting guard's `reserve_gate`. The release takes it too, so reserves and
    /// releases together put at most one thread per guard in the reservations lock wait.
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl FleetReservation {
    /// Bytes held.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for FleetReservation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let dir = self.cache_dir.clone();
        let instance = self.instance_id.clone();
        let bytes = self.bytes;
        let release = move || {
            if let Err(e) = release_blocking(&dir, &instance, bytes) {
                // A failed release leaves the bytes counted until the lease expires, which
                // is pessimistic rather than unsafe: the fleet admits less than it could
                // for at most one lease. Never the other direction.
                warn!(
                    "Capacity guard: could not release {} reserved bytes for {}: {}. They \
                     expire with the lease ({}s).",
                    bytes, instance, e, RESERVATION_LEASE_SECS
                );
            }
        };
        // Shared-volume I/O must not run on a tokio worker. Off the runtime (tests,
        // shutdown) there is nothing to protect, so run it inline.
        //
        // On the runtime the release takes the guard's `reserve_gate` first, as `admit`
        // does: `release_blocking` polls the lock for up to `RELEASE_LOCK_BUDGET`, and
        // ungated, N releases behind a slow peer held N pool threads for that long. An
        // uncontended gate is taken here and the release goes straight to the pool, as
        // before; a contended one is awaited by an async task, which holds no thread while
        // it queues. A queued release still waiting when the runtime shuts down is dropped,
        // and its bytes expire with the lease, the documented-safe outcome above.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => match Arc::clone(&self.gate).try_lock_owned() {
                Ok(gate_guard) => {
                    handle.spawn_blocking(move || {
                        release();
                        drop(gate_guard);
                    });
                }
                Err(_) => {
                    let gate = Arc::clone(&self.gate);
                    let blocking = handle.clone();
                    handle.spawn(async move {
                        let gate_guard = gate.lock_owned().await;
                        let _ = blocking
                            .spawn_blocking(move || {
                                release();
                                drop(gate_guard);
                            })
                            .await;
                    });
                }
            },
            Err(_) => release(),
        }
    }
}

/// Process-wide guards, one per cache directory.
///
/// A registry rather than a single `OnceLock` because the cached reading and the
/// single-flight refresh must be **shared across every `DiskCacheManager`** — those are
/// built fresh per operation by `CacheManager::create_configured_disk_cache_manager`, so a
/// per-manager reading would be refreshed on every fill and the amortisation would not
/// exist. Keyed on the cache directory so a process serving two caches (only tests do)
/// keeps their readings apart.
static GUARDS: LazyLock<Mutex<HashMap<PathBuf, Arc<DangerBandGuard>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The process-wide guard for `cache_dir`, created on first use.
///
/// `floor_bytes` is only read when the guard is created; a later call with a different
/// floor does not re-create it, because the floor comes from startup configuration and
/// cannot change within a process.
pub fn guard_for(cache_dir: &Path, floor_bytes: u64) -> Arc<DangerBandGuard> {
    let mut guards = match GUARDS.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    guards
        .entry(cache_dir.to_path_buf())
        .or_insert_with(|| Arc::new(DangerBandGuard::new(cache_dir.to_path_buf(), floor_bytes)))
        .clone()
}

/// Admit `incoming_bytes` at the enumerated publication boundary `path`, or decline.
///
/// The one-call entry point every boundary uses. A `floor_bytes` of `0` disables the guard
/// (an unconfigured `DiskCacheManager`, i.e. tests and early startup) and admits.
pub(crate) async fn admit_publication(
    cache_dir: &Path,
    floor_bytes: u64,
    incoming_bytes: u64,
    path: PublicationPath,
) -> Admission {
    if floor_bytes == 0 {
        return Admission::OutsideBand;
    }
    guard_for(cache_dir, floor_bytes)
        .admit(incoming_bytes, path)
        .await
}

/// Sentinel in [`DangerBandGuard::last_available`] meaning no `statvfs` has succeeded yet.
const NO_READING: u64 = u64::MAX;

/// The danger-band guard's figures, published as `cache.capacity_guard.*` on `/metrics` and
/// OTLP. Per-instance: every proxy keeps its own guard, its own counter and its own reading.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapacityGuardGauges {
    /// Cache publications (GET fills and full-object stores) this process declined inside the
    /// danger band, for either reason label. Monotonic per process; the client request is
    /// unaffected by a decline.
    pub declines_total: u64,
    /// The most recent free-space figure the guard read on the cache volume. `None` (JSON
    /// `null`, OTLP series absent) until the guard has taken a reading, which happens on the
    /// first fill after start, so `null` means "no fill yet", not "zero free".
    pub last_free_bytes: Option<u64>,
    /// `cache.disk_safety_free_space_floor` as configured. `0` means the guard is disabled.
    pub floor_bytes: u64,
}

/// Fold per-guard figures into one. Counters sum; the free-space reading is the smallest
/// present, which is the conservative choice for the one-cache-directory case production
/// runs and stays meaningful if a process ever holds two.
fn fold_gauges<'a>(
    guards: impl IntoIterator<Item = &'a DangerBandGuard>,
    configured_floor: u64,
) -> CapacityGuardGauges {
    let mut out = CapacityGuardGauges {
        floor_bytes: configured_floor,
        ..CapacityGuardGauges::default()
    };
    for guard in guards {
        let g = guard.gauges();
        out.declines_total = out.declines_total.saturating_add(g.declines_total);
        out.last_free_bytes = match (out.last_free_bytes, g.last_free_bytes) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    out
}

/// The process's capacity-guard figures for a metrics scrape. Reads the registry without
/// creating a guard, so a scrape before the first fill reports zero declines and a `null`
/// reading rather than taking a `statvfs` of its own.
pub fn collect_capacity_guard_gauges(configured_floor: u64) -> CapacityGuardGauges {
    let guards = match GUARDS.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    fold_gauges(guards.values().map(|g| g.as_ref()), configured_floor)
}

/// The guard itself: one cached free-space reading, one in-flight refresh, and the
/// fleet-wide reservation protocol.
#[derive(Debug)]
pub struct DangerBandGuard {
    cache_dir: PathBuf,
    floor_bytes: u64,
    instance_id: String,
    /// The process-wide reading and when it was taken. `std::sync::Mutex` because every
    /// hold is a field copy with no `await` inside it.
    reading: Mutex<Option<(u64, Instant)>>,
    /// Single-flight: one `statvfs` refresh in flight per process, the rest await it (A3).
    refresh_gate: tokio::sync::Mutex<()>,
    /// In-process gate in front of the reservations lock's blocking `flock`.
    ///
    /// The lock is fleet-wide and taken once per fill inside the danger band. Without the
    /// gate each concurrent in-band fill parked one blocking-pool thread in the kernel wait,
    /// so near full a convoy behind a slow peer could occupy most of the pool, which every
    /// `tokio::fs` call and `/health` also need. With it, waiters queue on this async mutex
    /// and at most one thread per guard (one guard per cache directory per process, from
    /// [`GUARDS`]) waits in the `flock`. The owned guard moves into the blocking closure, so
    /// it is released only after `reserve_blocking` has dropped the lock. Releases
    /// ([`FleetReservation`]'s `Drop`) take the same gate, so the bound covers both.
    reserve_gate: Arc<tokio::sync::Mutex<()>>,
    statvfs_calls: AtomicU64,
    band_entries: AtomicU64,
    declines: AtomicU64,
    /// The most recent successful `statvfs` free-space figure, cached refresh or in-band
    /// exact read alike. [`NO_READING`] until the first one succeeds.
    last_available: AtomicU64,
    /// Test-only pause inside the refresh gate, before the `statvfs`.
    ///
    /// # Why a seam is needed at all
    ///
    /// A3's property is "concurrent fills observing one stale reading produce ONE
    /// `statvfs`". Asserting it needs several fills to be *simultaneously* past the
    /// pre-gate freshness check, and on a current-thread runtime — which `#[tokio::test]`
    /// uses — that does not happen by itself: `fresh_enough` and `statvfs` contain no await,
    /// so the first task runs the whole refresh and returns before the second is polled, and
    /// the second then sees a fresh reading and never reaches the gate.
    ///
    /// **This was caught by running the red side.** A version of
    /// `a_stale_reading_is_refreshed_once_per_process_not_once_per_fill` without this seam
    /// PASSED against a build with the in-gate re-check deleted, because it was measuring
    /// sequential scheduling rather than single-flighting — the ninth instance in this
    /// repo of an assertion passing while reading an adjacent quantity, and the first one
    /// this seam exists to make impossible.
    ///
    /// The pause yields inside the gate, so every other fill piles up on it and must then
    /// take the re-check path. `None` in production and it is `#[cfg(test)]`, so there is no
    /// runtime cost and no production branch.
    #[cfg(test)]
    refresh_delay: Mutex<Option<Duration>>,
}

impl DangerBandGuard {
    fn new(cache_dir: PathBuf, floor_bytes: u64) -> Self {
        Self {
            cache_dir,
            floor_bytes,
            instance_id: instance_id(),
            reading: Mutex::new(None),
            refresh_gate: tokio::sync::Mutex::new(()),
            reserve_gate: Arc::new(tokio::sync::Mutex::new(())),
            statvfs_calls: AtomicU64::new(0),
            band_entries: AtomicU64::new(0),
            declines: AtomicU64::new(0),
            last_available: AtomicU64::new(NO_READING),
            #[cfg(test)]
            refresh_delay: Mutex::new(None),
        }
    }

    /// Pause inside the refresh gate for `delay` before the `statvfs`. See
    /// [`Self::refresh_delay`] for why this seam exists.
    #[cfg(test)]
    pub(crate) fn set_refresh_delay(&self, delay: Duration) {
        if let Ok(mut slot) = self.refresh_delay.lock() {
            *slot = Some(delay);
        }
    }

    /// A guard rooted at `cache_dir` with an explicit floor, bypassing the process
    /// registry. Tests only: two tests in one process must not share a reading.
    #[cfg(test)]
    pub(crate) fn for_test(cache_dir: PathBuf, floor_bytes: u64) -> Self {
        Self::new(cache_dir, floor_bytes)
    }

    /// `statvfs` calls this guard has made, cached refreshes and in-band exact checks
    /// together. The instrument that makes A3's single-flight claim checkable rather than
    /// asserted. Test seam: no metric publishes it.
    #[cfg(test)]
    pub(crate) fn statvfs_calls(&self) -> u64 {
        self.statvfs_calls.load(Ordering::Relaxed)
    }

    /// Fills that entered the danger band. Test seam: no metric publishes it.
    #[cfg(test)]
    pub(crate) fn band_entries(&self) -> u64 {
        self.band_entries.load(Ordering::Relaxed)
    }

    /// Publications declined.
    pub fn declines(&self) -> u64 {
        self.declines.load(Ordering::Relaxed)
    }

    /// The most recent free-space figure this guard read, or `None` before its first
    /// successful `statvfs`.
    pub fn last_available_bytes(&self) -> Option<u64> {
        match self.last_available.load(Ordering::Relaxed) {
            NO_READING => None,
            available => Some(available),
        }
    }

    /// This guard's published figures. [`collect_capacity_guard_gauges`] folds these across
    /// the process registry; a test reads them off a fresh guard so it never asserts a
    /// process-global figure.
    pub fn gauges(&self) -> CapacityGuardGauges {
        CapacityGuardGauges {
            declines_total: self.declines(),
            last_free_bytes: self.last_available_bytes(),
            floor_bytes: self.floor_bytes,
        }
    }

    /// The outer edge of the danger band for a fill of `incoming_bytes`.
    ///
    /// Exposed so a test asserts the same expression the admission path branches on rather
    /// than a re-derivation of it.
    pub(crate) fn band_edge(&self, incoming_bytes: u64) -> u64 {
        incoming_bytes.saturating_add(
            self.floor_bytes
                .saturating_mul(DANGER_BAND_FLOOR_MULTIPLIER),
        )
    }

    /// One `statvfs`, counted. `None` on any read failure.
    fn statvfs(&self) -> Option<u64> {
        self.statvfs_calls.fetch_add(1, Ordering::Relaxed);
        match fs4::available_space(&self.cache_dir) {
            Ok(available) => {
                // `NO_READING` is `u64::MAX`, which no real volume reports free; clamp so a
                // reading can never be mistaken for the sentinel.
                self.last_available
                    .store(available.min(NO_READING - 1), Ordering::Relaxed);
                Some(available)
            }
            Err(e) => {
                debug!(
                    "Capacity guard: could not read free space for {:?} ({}), proceeding \
                     (fail-open)",
                    self.cache_dir, e
                );
                None
            }
        }
    }

    /// The cached reading, refreshed on demand by this fill if it is stale.
    ///
    /// Single-flighted: the first caller to observe staleness takes the gate and performs
    /// the `statvfs`; concurrent callers block on the gate and then re-check, so they read
    /// the winner's value instead of issuing their own syscall (A3).
    async fn available_cached(&self) -> Option<u64> {
        if let Some(fresh) = self.fresh_enough() {
            return Some(fresh);
        }
        let _single_flight = self.refresh_gate.lock().await;
        // Re-check under the gate: a refresh may have completed while we waited, and
        // issuing a second syscall for the same window is the exact waste A3 forbids.
        if let Some(fresh) = self.fresh_enough() {
            return Some(fresh);
        }
        #[cfg(test)]
        {
            let delay = self.refresh_delay.lock().ok().and_then(|d| *d);
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
        }
        let available = self.statvfs()?;
        if let Ok(mut slot) = self.reading.lock() {
            *slot = Some((available, Instant::now()));
        }
        Some(available)
    }

    fn fresh_enough(&self) -> Option<u64> {
        let slot = self.reading.lock().ok()?;
        let (available, taken_at) = (*slot)?;
        (taken_at.elapsed() < FREE_SPACE_READING_MAX_AGE).then_some(available)
    }

    /// Admit or decline `incoming_bytes` at `path`.
    pub async fn admit(&self, incoming_bytes: u64, path: PublicationPath) -> Admission {
        if self.floor_bytes == 0 || incoming_bytes == 0 {
            return Admission::OutsideBand;
        }

        // Outside the band: the cached reading is the whole check.
        let Some(available_cached) = self.available_cached().await else {
            return Admission::OutsideBand; // fail open on an unreadable statvfs
        };
        if available_cached > self.band_edge(incoming_bytes) {
            return Admission::OutsideBand;
        }
        self.band_entries.fetch_add(1, Ordering::Relaxed);

        // Inside the band: a FRESH reading per fill, because a 5s-old figure is tens of
        // GiB of staleness at fleet ingest rates and no sane floor absorbs that.
        let Some(available) = self.statvfs() else {
            return Admission::OutsideBand; // fail open, as above
        };

        // ...and a fleet-wide reservation, because a fresh reading is not a reservation.
        let dir = self.cache_dir.clone();
        let instance = self.instance_id.clone();
        let floor = self.floor_bytes;
        let gate_guard = Arc::clone(&self.reserve_gate).lock_owned().await;
        let outcome = tokio::task::spawn_blocking(move || {
            let reserved = reserve_blocking(&dir, &instance, incoming_bytes, available, floor);
            drop(gate_guard);
            reserved
        })
        .await;

        match outcome {
            Ok(Ok(ReserveOutcome::Reserved { fleet_outstanding })) => {
                debug!(
                    "Capacity guard: admitted {} bytes at {} inside the danger band \
                     (available={}, fleet_outstanding={}, floor={})",
                    incoming_bytes,
                    path.label(),
                    available,
                    fleet_outstanding,
                    floor
                );
                Admission::Reserved(FleetReservation {
                    cache_dir: self.cache_dir.clone(),
                    instance_id: self.instance_id.clone(),
                    bytes: incoming_bytes,
                    gate: Arc::clone(&self.reserve_gate),
                })
            }
            Ok(Ok(ReserveOutcome::Declined { fleet_outstanding })) => {
                self.declines.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Capacity guard: declining to cache {} bytes at {} — only {} bytes free \
                     on the cache volume, and the fleet already has {} bytes admitted but \
                     unpublished, against a {} byte floor. The client request is unaffected.",
                    incoming_bytes,
                    path.label(),
                    available,
                    fleet_outstanding,
                    floor
                );
                Admission::Declined(DeclineDetail {
                    reason: DANGER_BAND_SKIP_REASON,
                    path,
                    incoming_bytes,
                    available_bytes: Some(available),
                    fleet_outstanding_bytes: Some(fleet_outstanding),
                    floor_bytes: floor,
                })
            }
            Ok(Err(e)) => {
                self.declines.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Capacity guard: declining to cache {} bytes at {} — the fleet's \
                     outstanding reservation total could not be read ({}). Inside the danger \
                     band an unknown fleet total is treated as insufficient headroom, which \
                     is the one place this guard fails closed. The client request is \
                     unaffected.",
                    incoming_bytes,
                    path.label(),
                    e
                );
                Admission::Declined(DeclineDetail {
                    reason: DANGER_BAND_UNREADABLE_SKIP_REASON,
                    path,
                    incoming_bytes,
                    available_bytes: Some(available),
                    fleet_outstanding_bytes: None,
                    floor_bytes: floor,
                })
            }
            Err(e) => {
                self.declines.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Capacity guard: declining to cache {} bytes at {} — the reservation task \
                     failed ({}). The client request is unaffected.",
                    incoming_bytes,
                    path.label(),
                    e
                );
                Admission::Declined(DeclineDetail {
                    reason: DANGER_BAND_UNREADABLE_SKIP_REASON,
                    path,
                    incoming_bytes,
                    available_bytes: Some(available),
                    fleet_outstanding_bytes: None,
                    floor_bytes: floor,
                })
            }
        }
    }
}

/// `{cache_dir}/metadata/_shard_state/capacity_reservations`.
pub(crate) fn reservations_dir(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(RESERVATIONS_DIR)
}

fn reservation_path(cache_dir: &Path, instance_id: &str) -> PathBuf {
    reservations_dir(cache_dir).join(format!("{}.{}", instance_id, RESERVATION_EXT))
}

fn reservations_lock_path(cache_dir: &Path) -> PathBuf {
    reservations_dir(cache_dir).join(RESERVATIONS_LOCK)
}

/// Instance identity: `hostname:pid`, matching `journal_consolidator::get_instance_id` so
/// an instance's files sort together. Dead-instance cleanup removes the
/// `{instance}.resv.tmp.{pid}` temp by name (`journal_consolidator::reap_dead_instance_temps`)
/// and a retired id's expired `.resv` ([`reap_retired_reservations`]).
fn instance_id() -> String {
    format!(
        "{}:{}",
        gethostname::gethostname()
            .to_string_lossy()
            .replace(['/', '\\'], "_"),
        std::process::id()
    )
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

enum ReserveOutcome {
    Reserved { fleet_outstanding: u64 },
    Declined { fleet_outstanding: u64 },
}

/// Read every instance's reservation, sum the live ones, decide, and — if admitting —
/// write this instance's updated total, all inside **one hold of the single shared lock**.
///
/// The read, the decision and the write must be under one hold of a lock every other
/// instance also takes, or two instances read the same sum and both admit (A2). This is
/// the same shape `PendingStore::gc` had to be corrected to, for the same reason.
///
/// Returns `Err` when the fleet total is unknown, which the caller turns into a decline.
fn reserve_blocking(
    cache_dir: &Path,
    instance_id: &str,
    incoming_bytes: u64,
    available_bytes: u64,
    floor_bytes: u64,
) -> std::io::Result<ReserveOutcome> {
    use std::io::Write;

    let dir = reservations_dir(cache_dir);
    std::fs::create_dir_all(&dir)?;
    let lock_path = reservations_lock_path(cache_dir);
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    fs4::FileExt::lock(&lock_file)?;
    let result = (|| -> std::io::Result<ReserveOutcome> {
        let now = now_secs();
        let (fleet_outstanding, own) = sum_live_reservations(&dir, instance_id, now)?;
        let required = incoming_bytes
            .saturating_add(fleet_outstanding)
            .saturating_add(floor_bytes);
        if available_bytes < required {
            return Ok(ReserveOutcome::Declined { fleet_outstanding });
        }
        let doc = ReservationDoc {
            schema: RESERVATION_SCHEMA,
            instance_id: instance_id.to_string(),
            outstanding_bytes: own.saturating_add(incoming_bytes),
            updated_at: now,
            lease_secs: RESERVATION_LEASE_SECS,
        };
        let path = reservation_path(cache_dir, instance_id);
        let body = serde_json::to_string(&doc)
            .map_err(|e| std::io::Error::other(format!("serialize reservation: {e}")))?;
        // Atomic replace with an instance-suffixed temp name, so a concurrent write from
        // another instance cannot truncate this one's temp file.
        let tmp = path.with_extension(format!("{}.tmp.{}", RESERVATION_EXT, std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(ReserveOutcome::Reserved { fleet_outstanding })
    })();
    let _ = fs4::FileExt::unlock(&lock_file);
    result
}

/// Σ live `outstanding_bytes` across every instance, plus this instance's own figure.
///
/// **Own bytes are included in the sum**, because they are as real as a peer's: this
/// instance's admitted-but-unpublished fills consume the same volume.
///
/// An unreadable or unparseable peer document is an `Err`, never a zero — see the module
/// doc for why this arm fails closed. An **expired** document is skipped, which is not the
/// same thing: an expired lease is a positive observation that those bytes are no longer
/// coming.
fn sum_live_reservations(dir: &Path, instance_id: &str, now: u64) -> std::io::Result<(u64, u64)> {
    let mut total: u64 = 0;
    let mut own: u64 = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let path = entry?.path();
        if !path.extension().is_some_and(|ext| ext == RESERVATION_EXT) {
            // Skips the lock file and any `.resv.tmp.{pid}` in flight — a temp name's
            // extension is the pid, not `resv`, so a half-written document can never be
            // summed.
            continue;
        }
        let content = std::fs::read_to_string(&path)?;
        let doc: ReservationDoc = serde_json::from_str(&content).map_err(|e| {
            std::io::Error::other(format!("unparseable reservation {}: {e}", path.display()))
        })?;
        if doc.schema != RESERVATION_SCHEMA {
            return Err(std::io::Error::other(format!(
                "unknown reservation schema {} in {}",
                doc.schema,
                path.display()
            )));
        }
        if doc.instance_id == instance_id {
            // Own figure is reported separately so the rewrite adds to it rather than
            // replacing it, but it still counts toward the fleet total.
            if doc.is_live_at(now) {
                own = doc.outstanding_bytes;
                total = total.saturating_add(doc.outstanding_bytes);
            }
            continue;
        }
        if doc.is_live_at(now) {
            total = total.saturating_add(doc.outstanding_bytes);
        }
    }
    Ok((total, own))
}

/// How long [`release_blocking`] waits for the shared reservations lock before giving up.
///
/// A bound rather than `fs4::FileExt::lock`'s untimed wait, because this acquisition is
/// reached from [`FleetReservation::drop`] — a `Drop` impl, i.e. during teardown — and the
/// blocking task it hands the release to is waited for by tokio's `Drop for BlockingPool`
/// with no timeout of its own. An untimed acquisition here is therefore an untimed hold on
/// **process exit**, on a lock file that lives on the shared volume and whose holder can be
/// a peer instance or a dead NFS client holding a lease.
///
/// Giving up is already the documented-safe outcome: see [`FleetReservation`]'s `Drop`, where
/// a failed release "leaves the bytes counted until the lease expires, which is pessimistic
/// rather than unsafe: the fleet admits less than it could for at most one lease. Never the
/// other direction." Two seconds against a 120 s lease, and the hold this waits behind is one
/// directory read plus one `fsync`, so the budget is generous for the contention it can meet.
const RELEASE_LOCK_BUDGET: Duration = Duration::from_secs(2);

/// Poll interval while waiting out [`RELEASE_LOCK_BUDGET`].
///
/// Each poll is one shared-storage round trip. The request path's slot-lock waits pay the same
/// cost (`acquire_slot_lock_within` polls rather than queueing in `flock`), so a
/// waiter holds no thread. Here it is cheap as well: at most one release per publication, and
/// polls only happen when the lock is actually contended.
const RELEASE_LOCK_POLL: Duration = Duration::from_millis(20);

/// Acquire `lock_file` exclusively within `budget`, polling instead of blocking in the kernel.
///
/// `Err` with [`std::io::ErrorKind::WouldBlock`] when the budget expires — distinguishable by
/// the caller from a genuine I/O failure, and in both cases the caller's contract is the same:
/// the release did not happen and the bytes expire with the lease.
fn lock_within(lock_file: &std::fs::File, budget: Duration) -> std::io::Result<()> {
    let deadline = Instant::now() + budget;
    loop {
        match fs4::FileExt::try_lock(lock_file) {
            Ok(()) => return Ok(()),
            Err(fs4::TryLockError::Error(e)) => return Err(e),
            Err(fs4::TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!("reservations lock still held after {budget:?}"),
                    ));
                }
                std::thread::sleep(RELEASE_LOCK_POLL);
            }
        }
    }
}

/// Give back `bytes` from this instance's reservation, under the same shared lock.
///
/// The lock is acquired within [`RELEASE_LOCK_BUDGET`] rather than waited for indefinitely;
/// see that constant for why an untimed wait here is a hold on process exit.
fn release_blocking(cache_dir: &Path, instance_id: &str, bytes: u64) -> std::io::Result<()> {
    use std::io::Write;

    let path = reservation_path(cache_dir, instance_id);
    if !path.exists() {
        return Ok(());
    }
    let lock_path = reservations_lock_path(cache_dir);
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    lock_within(&lock_file, RELEASE_LOCK_BUDGET)?;
    let result = (|| -> std::io::Result<()> {
        let content = std::fs::read_to_string(&path)?;
        let mut doc: ReservationDoc = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::other(format!("unparseable own reservation: {e}")))?;
        doc.outstanding_bytes = doc.outstanding_bytes.saturating_sub(bytes);
        doc.updated_at = now_secs();
        let body = serde_json::to_string(&doc)
            .map_err(|e| std::io::Error::other(format!("serialize reservation: {e}")))?;
        let tmp = path.with_extension(format!("{}.tmp.{}", RESERVATION_EXT, std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
    })();
    let _ = fs4::FileExt::unlock(&lock_file);
    result
}

/// Remove the expired `{instance}.resv` documents of the ids `is_retired` names, given when each
/// lease lapsed. Returns how many were removed. Blocking, for the dead-instance reaper.
///
/// Runs under the shared reservations lock, so no reserve or release interleaves. An expired
/// document already counts nothing in [`sum_live_reservations`], and [`release_blocking`] treats
/// an absent document as nothing to release, so removing one changes no admission decision; it
/// only stops every danger-band admission reading it. A document that cannot be read or parsed is
/// left in place, since admission fails closed on it and an operator has to see that.
pub(crate) fn reap_retired_reservations(
    cache_dir: &Path,
    now: u64,
    is_retired: &dyn Fn(&str, SystemTime) -> bool,
) -> u64 {
    let dir = reservations_dir(cache_dir);
    if !dir.is_dir() {
        return 0;
    }
    let Ok(lock_file) = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(reservations_lock_path(cache_dir))
    else {
        return 0;
    };
    if lock_within(&lock_file, RELEASE_LOCK_BUDGET).is_err() {
        return 0;
    }
    let mut removed = 0u64;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.extension().is_some_and(|ext| ext == RESERVATION_EXT) {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(doc) = serde_json::from_str::<ReservationDoc>(&content) else {
                continue;
            };
            if doc.schema != RESERVATION_SCHEMA || doc.is_live_at(now) {
                continue;
            }
            // The document must sit at its own id's path, so a mislabelled body cannot remove
            // another id's file.
            if path != reservation_path(cache_dir, &doc.instance_id) {
                continue;
            }
            let lapsed_at =
                UNIX_EPOCH + Duration::from_secs(doc.updated_at.saturating_add(doc.lease_secs));
            if is_retired(&doc.instance_id, lapsed_at) && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
    }
    let _ = fs4::FileExt::unlock(&lock_file);
    removed
}

/// This instance's live outstanding reservation. Test seam: nothing outside tests reads it.
///
/// `None` when the figure could not be read — never `0`, which is also the genuine
/// nothing-outstanding value.
#[cfg(test)]
pub(crate) fn own_outstanding_bytes(cache_dir: &Path, instance_id: &str) -> Option<u64> {
    let path = reservation_path(cache_dir, instance_id);
    let content = std::fs::read_to_string(path).ok()?;
    let doc: ReservationDoc = serde_json::from_str(&content).ok()?;
    doc.is_live_at(now_secs())
        .then_some(doc.outstanding_bytes)
        .or(Some(0))
}

/// Σ live reservations across the whole fleet. Test seam: nothing outside tests reads it.
///
/// `None` when any document could not be read, matching the admission path's own posture:
/// a partial sum is a confident wrong number.
#[cfg(test)]
pub(crate) fn fleet_outstanding_bytes(cache_dir: &Path) -> Option<u64> {
    let dir = reservations_dir(cache_dir);
    // Pass an instance id no document can carry, so nothing is split out as "own" and the
    // returned total is the whole fleet.
    sum_live_reservations(&dir, "\0::not-an-instance", now_secs())
        .ok()
        .map(|(total, _own)| total)
}

/// Write a reservation document as if `instance_id` were another proxy holding
/// `outstanding_bytes`. Test-only: the two-instance property cannot be exercised from one
/// process otherwise, and a single-process test cannot establish a fleet-wide claim.
#[cfg(test)]
pub(crate) fn plant_peer_reservation(
    cache_dir: &Path,
    instance_id: &str,
    outstanding_bytes: u64,
    age_secs: u64,
) -> std::io::Result<()> {
    use std::io::Write;
    let dir = reservations_dir(cache_dir);
    std::fs::create_dir_all(&dir)?;
    let doc = ReservationDoc {
        schema: RESERVATION_SCHEMA,
        instance_id: instance_id.to_string(),
        outstanding_bytes,
        updated_at: now_secs().saturating_sub(age_secs),
        lease_secs: RESERVATION_LEASE_SECS,
    };
    let path = reservation_path(cache_dir, instance_id);
    let mut f = std::fs::File::create(path)?;
    f.write_all(serde_json::to_string(&doc)?.as_bytes())?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// The instance id the guard under test will use.
    fn own_id() -> String {
        instance_id()
    }

    /// while a peer holds the reservations lock and many in-band fills wait for it,
    /// an unrelated blocking-pool task still runs, and every fill completes once the peer
    /// releases.
    ///
    /// # RED SIDE
    ///
    /// With `Arc::clone(&self.reserve_gate).lock_owned().await` in `admit` replaced by a
    /// fresh mutex's guard (no in-process serialisation), the 16 waiters park all 4 pool
    /// threads in `flock` and the probe's `spawn_blocking` never starts.
    #[test]
    fn in_band_fills_waiting_on_a_peer_reservation_lock_leave_the_blocking_pool_free() {
        let tmp = TempDir::new().unwrap();
        let available = fs4::available_space(tmp.path()).expect("statvfs on the temp volume");
        let lock_path = reservations_lock_path(tmp.path());
        std::fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        let peer = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::lock(&peer).unwrap();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let (probe, fills) = rt.block_on(async {
            // Band edge = incoming + 4 × available, so every fill is inside the band.
            let guard = Arc::new(DangerBandGuard::for_test(
                tmp.path().to_path_buf(),
                available,
            ));
            let fills: Vec<_> = (0..16)
                .map(|_| {
                    let g = Arc::clone(&guard);
                    tokio::spawn(async move {
                        g.admit(1024, PublicationPath::P3aGetResponseTee).await;
                    })
                })
                .collect();
            // Let the fills reach the lock before probing.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let probe = tokio::time::timeout(
                Duration::from_secs(2),
                tokio::task::spawn_blocking(|| 42u32),
            )
            .await;
            drop(peer);
            let fills =
                tokio::time::timeout(Duration::from_secs(10), futures::future::join_all(fills))
                    .await;
            (probe, fills)
        });
        rt.shutdown_background();
        assert!(
            matches!(probe, Ok(Ok(42))),
            "STARVED: a blocking-pool task did not run within 2 s while 16 in-band fills \
             waited on the reservations lock: {:?}",
            probe
        );
        assert!(
            fills.is_ok(),
            "16 in-band fills did not all complete within 10 s of the peer releasing"
        );
    }

    /// A fill comfortably outside the band takes no reservation and touches no shared
    /// storage.
    ///
    /// Implicates `DangerBandGuard::admit`'s `available_cached > self.band_edge(..)`
    /// branch. Asserts on `band_entries()` — the counter incremented on the *other* side
    /// of that branch — rather than on the absence of a reservation file, because an
    /// absent file is also what a failed write looks like.
    #[tokio::test]
    async fn a_fill_outside_the_band_takes_no_reservation() {
        let tmp = TempDir::new().unwrap();
        // Floor of 1 byte ⇒ a 4-byte band. Any real volume has more free space than that.
        let guard = DangerBandGuard::for_test(tmp.path().to_path_buf(), 1);
        let admission = guard.admit(1024, PublicationPath::P1BufferedRange).await;
        assert!(matches!(admission, Admission::OutsideBand));
        assert_eq!(guard.band_entries(), 0, "should not have entered the band");
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(0),
            "no reservation should exist"
        );
    }

    /// The published `cache.capacity_guard.*` figures move with the guard: three driven
    /// declines move `declines_total` by exactly three, the reading appears after the first
    /// `statvfs`, and the floor is the guard's own.
    ///
    /// Reads [`DangerBandGuard::gauges`] off a fresh `for_test` guard, never
    /// [`collect_capacity_guard_gauges`], so no process-global figure is asserted while other
    /// tests drive the registry in parallel. The registry fold is pinned separately below.
    #[tokio::test]
    async fn danger_band_declines_and_reading_are_published() {
        let tmp = TempDir::new().unwrap();
        let floor = u64::MAX / 8;
        let guard = DangerBandGuard::for_test(tmp.path().to_path_buf(), floor);
        let before = guard.gauges();
        assert_eq!(before.declines_total, 0);
        assert_eq!(before.last_free_bytes, None, "no statvfs has run yet");
        assert_eq!(before.floor_bytes, floor);
        for _ in 0..3 {
            let admission = guard.admit(1024, PublicationPath::P1BufferedRange).await;
            assert!(
                admission.decline().is_some(),
                "unsatisfiable floor must decline"
            );
        }
        let after = guard.gauges();
        assert_eq!(
            after.declines_total - before.declines_total,
            3,
            "three driven declines must move the published counter by exactly three"
        );
        assert_eq!(after.declines_total, guard.declines());
        let reading = after
            .last_free_bytes
            .expect("a reading must be published once the guard has read statvfs");
        assert_eq!(Some(reading), guard.last_available_bytes());
        assert_eq!(after.floor_bytes, floor);
    }

    /// `collect_capacity_guard_gauges`'s fold: declines sum, the smallest reading wins, the
    /// configured floor is reported even with no guard registered.
    #[tokio::test]
    async fn the_registry_fold_sums_declines_and_keeps_the_smallest_reading() {
        let empty = fold_gauges(std::iter::empty(), 42);
        assert_eq!(
            empty,
            CapacityGuardGauges {
                declines_total: 0,
                last_free_bytes: None,
                floor_bytes: 42
            }
        );
        let a_dir = TempDir::new().unwrap();
        let b_dir = TempDir::new().unwrap();
        let a = DangerBandGuard::for_test(a_dir.path().to_path_buf(), u64::MAX / 8);
        let b = DangerBandGuard::for_test(b_dir.path().to_path_buf(), u64::MAX / 8);
        a.admit(1024, PublicationPath::P1BufferedRange).await;
        b.admit(1024, PublicationPath::P1BufferedRange).await;
        b.admit(1024, PublicationPath::P4FullObjectStore).await;
        a.last_available.store(500, Ordering::Relaxed);
        b.last_available.store(700, Ordering::Relaxed);
        let folded = fold_gauges([&a, &b], 7);
        assert_eq!(folded.declines_total, 3);
        assert_eq!(folded.last_free_bytes, Some(500));
        assert_eq!(folded.floor_bytes, 7);
    }

    /// A floor larger than the volume puts every fill inside the band, where the exact
    /// per-fill check then declines it.
    ///
    /// Implicates `reserve_blocking`'s `available_bytes < required` comparison. Asserts on
    /// the reported `available_bytes` / `fleet_outstanding_bytes` — the two inputs to that
    /// expression — rather than on `Admission`'s variant alone, so a decline reached for
    /// some adjacent reason would not pass.
    #[tokio::test]
    async fn a_fill_inside_the_band_takes_the_exact_check_and_declines() {
        let tmp = TempDir::new().unwrap();
        // An unsatisfiable floor forces band entry and then guarantees the exact check
        // cannot fit the fill, which is the arm under test.
        let guard = DangerBandGuard::for_test(tmp.path().to_path_buf(), u64::MAX / 8);
        let admission = guard.admit(1024, PublicationPath::P1BufferedRange).await;
        assert_eq!(guard.band_entries(), 1, "should have entered the band");
        let detail = admission
            .decline()
            .expect("an unsatisfiable floor must decline");
        assert_eq!(detail.reason, DANGER_BAND_SKIP_REASON);
        assert_eq!(detail.incoming_bytes, 1024);
        assert_eq!(detail.fleet_outstanding_bytes, Some(0));
        assert!(detail.available_bytes.is_some());
        assert_eq!(guard.declines(), 1);
    }

    /// **The fleet-wide property, and the whole reason A2 exists.**
    ///
    /// A fill that fits against a fresh `statvfs` reading on its own must be declined once
    /// a *peer's* outstanding reservation is counted. A per-instance guard admits this
    /// fill; a fleet-wide one does not.
    ///
    /// Implicates `reserve_blocking`'s
    /// `incoming.saturating_add(fleet_outstanding).saturating_add(floor)` — the assertion
    /// reads `fleet_outstanding_bytes` from the same documents that expression sums, not a
    /// derived gauge. Constructed so the two figures **diverge**: this instance's own
    /// outstanding is 0 while the fleet's is large, so an implementation that summed only
    /// its own file would pass the admit arm and fail this test.
    #[tokio::test]
    async fn a_peer_reservation_declines_a_fill_that_would_fit_alone() {
        let tmp = TempDir::new().unwrap();
        let available = fs4::available_space(tmp.path()).expect("statvfs on the temp volume");
        // Floor of 0 would disable the guard, so use 1 byte and force band entry with a
        // band edge above the volume's free space.
        let floor = available; // band edge = incoming + 4 × available ⇒ always inside
        let guard = DangerBandGuard::for_test(tmp.path().to_path_buf(), floor);

        // Own figure and fleet figure are 0 and 0 here; the peer plant below is what makes
        // them diverge.
        assert_eq!(own_outstanding_bytes(tmp.path(), &own_id()), None);

        // A peer holds essentially the whole volume.
        plant_peer_reservation(tmp.path(), "peer-proxy:1", u64::MAX / 4, 0).unwrap();
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(u64::MAX / 4),
            "the peer's bytes must be visible to this instance"
        );

        let admission = guard.admit(1024, PublicationPath::P3aGetResponseTee).await;
        let detail = admission
            .decline()
            .expect("a peer's outstanding bytes must decline this fill");
        assert_eq!(detail.reason, DANGER_BAND_SKIP_REASON);
        assert_eq!(
            detail.fleet_outstanding_bytes,
            Some(u64::MAX / 4),
            "the decline must report the fleet total it acted on, not this instance's"
        );
    }

    /// An **expired** peer lease stops counting, so a crashed instance cannot shrink the
    /// fleet's headroom forever.
    ///
    /// Implicates `ReservationDoc::is_live_at`. The two arms are the same document at two
    /// ages, so nothing but the lease differs.
    #[test]
    fn an_expired_peer_reservation_stops_counting() {
        let tmp = TempDir::new().unwrap();
        plant_peer_reservation(tmp.path(), "peer-proxy:1", 4096, 0).unwrap();
        assert_eq!(fleet_outstanding_bytes(tmp.path()), Some(4096));
        plant_peer_reservation(
            tmp.path(),
            "peer-proxy:1",
            4096,
            RESERVATION_LEASE_SECS + 60,
        )
        .unwrap();
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(0),
            "an expired lease must not count"
        );
    }

    /// An unparseable peer reservation makes the fleet total UNREADABLE rather than short.
    ///
    /// Implicates `sum_live_reservations`' `serde_json::from_str(..).map_err(..)?`. The
    /// distinction matters because a short sum admits, and admitting against an unknown
    /// fleet total is the failure A2 exists to prevent.
    #[test]
    fn an_unparseable_peer_reservation_is_unreadable_not_zero() {
        let tmp = TempDir::new().unwrap();
        let dir = reservations_dir(tmp.path());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("peer-proxy:1.resv"), b"{not json").unwrap();
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            None,
            "an unparseable document must not read as zero outstanding bytes"
        );
    }

    /// A half-written reservation temp file never enters the sum.
    ///
    /// Implicates the extension filter in `sum_live_reservations`. `3f.resv.tmp.1234`'s
    /// extension is `1234`, not `resv`, which is the property being relied on — asserted
    /// rather than assumed, because the staged fold's own checkpoint reader carries a
    /// documented instance of exactly this trap.
    #[test]
    fn a_partial_reservation_never_enters_the_sum() {
        let tmp = TempDir::new().unwrap();
        let dir = reservations_dir(tmp.path());
        std::fs::create_dir_all(&dir).unwrap();
        plant_peer_reservation(tmp.path(), "peer-proxy:1", 512, 0).unwrap();
        std::fs::write(dir.join("peer-proxy:2.resv.tmp.4242"), b"{not json").unwrap();
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(512),
            "a temp file must be neither summed nor treated as unreadable"
        );
    }

    /// Dropping the reservation gives the bytes back.
    ///
    /// Implicates `FleetReservation::drop` → `release_blocking`. Asserted on the shared
    /// document rather than on an in-memory counter, because the document is what a peer
    /// reads.
    #[tokio::test]
    async fn dropping_a_reservation_returns_the_bytes_to_the_fleet() {
        let tmp = TempDir::new().unwrap();
        let id = own_id();
        reserve_blocking(tmp.path(), &id, 8192, u64::MAX, 0).unwrap();
        assert_eq!(fleet_outstanding_bytes(tmp.path()), Some(8192));
        {
            let _reservation = FleetReservation {
                cache_dir: tmp.path().to_path_buf(),
                instance_id: id.clone(),
                bytes: 8192,
                gate: Arc::new(tokio::sync::Mutex::new(())),
            };
        }
        // The drop hands the release to spawn_blocking; wait for it to land.
        for _ in 0..100 {
            if fleet_outstanding_bytes(tmp.path()) == Some(0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(0),
            "the reservation must be released on drop"
        );
    }

    /// a release queues on the guard's `reserve_gate` while another reserve or release
    /// holds it, and lands once the gate frees. So releases, like reserves, put at most one
    /// thread per guard in the reservations-lock wait.
    #[tokio::test]
    async fn a_release_queues_behind_the_reserve_gate_and_lands_when_it_frees() {
        let tmp = TempDir::new().unwrap();
        let id = own_id();
        reserve_blocking(tmp.path(), &id, 8192, u64::MAX, 0).unwrap();
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let held = Arc::clone(&gate).lock_owned().await;
        drop(FleetReservation {
            cache_dir: tmp.path().to_path_buf(),
            instance_id: id.clone(),
            bytes: 8192,
            gate: Arc::clone(&gate),
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(8192),
            "the release ran while another holder had the reserve gate"
        );
        drop(held);
        for _ in 0..100 {
            if fleet_outstanding_bytes(tmp.path()) == Some(0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(0),
            "the queued release must land once the gate frees"
        );
    }

    /// A held reservations lock must not make `FleetReservation::drop` wait forever.
    ///
    /// Implicates [`release_blocking`]'s acquisition. The lock file is on the shared volume,
    /// so its holder can be a peer instance; the release is handed to a blocking task that
    /// tokio's `Drop for BlockingPool` waits for with no timeout of its own, which makes an
    /// unbounded acquisition here an unbounded hold on process exit.
    ///
    /// **Not vacuous, and the reason is that returning at all is not the assertion.** A
    /// release returns in microseconds whenever nothing holds the lock, which is every
    /// ordinary run — so this holds the lock for the whole test and never releases it, and
    /// then asserts on *why* the drop returned: the budget must have been spent
    /// (`elapsed >= RELEASE_LOCK_BUDGET`, so it did not return by acquiring) and the shared
    /// document must be **unchanged** (so the release genuinely did not happen). Its pair in
    /// the other direction is
    /// [`dropping_a_reservation_returns_the_bytes_to_the_fleet`], where no holder exists and
    /// the bytes do come back.
    ///
    /// The drop runs on a thread of its own behind a channel watchdog, so an unbounded
    /// acquisition **fails** this test rather than wedging the suite.
    #[test]
    fn a_release_whose_lock_is_held_gives_up_within_the_budget_rather_than_waiting_forever() {
        let tmp = TempDir::new().unwrap();
        let id = own_id();
        reserve_blocking(tmp.path(), &id, 8192, u64::MAX, 0).unwrap();
        assert_eq!(fleet_outstanding_bytes(tmp.path()), Some(8192));

        // Stands in for a peer instance holding the lock mid-reserve. Never released.
        let lock_path = reservations_lock_path(tmp.path());
        let holder = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::lock(&holder).unwrap();

        // Off any runtime, so `Drop` takes its inline arm and runs `release_blocking` here.
        let (tx, rx) = std::sync::mpsc::channel();
        let dir = tmp.path().to_path_buf();
        let dropped_id = id.clone();
        std::thread::spawn(move || {
            let start = Instant::now();
            drop(FleetReservation {
                cache_dir: dir,
                instance_id: dropped_id,
                bytes: 8192,
                gate: Arc::new(tokio::sync::Mutex::new(())),
            });
            let _ = tx.send(start.elapsed());
        });

        let elapsed = rx
            .recv_timeout(RELEASE_LOCK_BUDGET + Duration::from_secs(8))
            .expect(
                "the drop must return while the lock is still held — an untimed acquisition \
                 never returns, and in `src/main.rs` that is process exit",
            );

        assert!(
            elapsed >= RELEASE_LOCK_BUDGET,
            "the drop returned after {elapsed:?}, faster than the {RELEASE_LOCK_BUDGET:?} \
             budget — so it returned by ACQUIRING the lock, not by giving up, and the holder \
             cannot have released it. The fixture is broken, not the code."
        );
        assert_eq!(
            fleet_outstanding_bytes(tmp.path()),
            Some(8192),
            "the release must not have happened: it gave up with the lock still held, and \
             the bytes expire with the lease"
        );
    }

    /// Concurrent fills observing one stale reading produce **one** `statvfs`, not one
    /// each (A3).
    ///
    /// Implicates the in-gate re-check in `available_cached`. Asserts on `statvfs_calls()`,
    /// which the syscall itself increments, rather than on elapsed time or on the reading's
    /// value — those would both pass with per-fill refreshes.
    ///
    /// **The `set_refresh_delay` seam is load-bearing and this test is worthless without
    /// it**: a version of this test with no delay PASSED against a build with the re-check
    /// deleted, because on a current-thread runtime the first fill completes its whole
    /// refresh before the second is polled and the second then never reaches the gate. See
    /// [`DangerBandGuard::refresh_delay`].
    #[tokio::test]
    async fn a_stale_reading_is_refreshed_once_per_process_not_once_per_fill() {
        let tmp = TempDir::new().unwrap();
        let guard = Arc::new(DangerBandGuard::for_test(tmp.path().to_path_buf(), 1));
        // Hold the gate long enough that all 32 fills pass their pre-gate freshness check
        // and queue on it, so every one of them must take the re-check path.
        guard.set_refresh_delay(Duration::from_millis(200));
        let mut handles = Vec::new();
        for _ in 0..32 {
            let g = Arc::clone(&guard);
            handles.push(tokio::spawn(async move {
                g.admit(1024, PublicationPath::P1BufferedRange).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // A 1-byte floor keeps every fill outside the band, so the only `statvfs` any of
        // them can make is the amortised refresh.
        assert_eq!(
            guard.statvfs_calls(),
            1,
            "32 concurrent fills must share one refresh"
        );
        assert_eq!(guard.band_entries(), 0);
    }

    /// An idle instance performs zero checks by construction: no fill, no `statvfs`.
    ///
    /// Implicates the absence of any timer — the refresh is only ever reached from
    /// `admit`. Cheap, and it is the property that makes the demand-refresh design
    /// preferable to a poller on an idle fleet.
    #[tokio::test]
    async fn an_idle_instance_performs_no_free_space_checks() {
        let tmp = TempDir::new().unwrap();
        let guard = DangerBandGuard::for_test(tmp.path().to_path_buf(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(guard.statvfs_calls(), 0);
    }

    /// A zero floor disables the guard entirely, which is what an unconfigured
    /// `DiskCacheManager` (tests, early startup) gets.
    #[tokio::test]
    async fn a_zero_floor_disables_the_guard() {
        let tmp = TempDir::new().unwrap();
        let admission =
            admit_publication(tmp.path(), 0, u64::MAX, PublicationPath::P4FullObjectStore).await;
        assert!(matches!(admission, Admission::OutsideBand));
    }

    /// The band edge is the expression the admission path branches on, stated once.
    #[test]
    fn the_band_edge_is_incoming_plus_four_floors() {
        let tmp = TempDir::new().unwrap();
        let guard = DangerBandGuard::for_test(tmp.path().to_path_buf(), 1024);
        assert_eq!(guard.band_edge(500), 500 + 4 * 1024);
        assert_eq!(DANGER_BAND_FLOOR_MULTIPLIER, 4);
        // No overflow panic at the extremes.
        assert_eq!(
            DangerBandGuard::for_test(tmp.path().to_path_buf(), u64::MAX).band_edge(u64::MAX),
            u64::MAX
        );
    }

    /// Every enumerated publication boundary is actually wired somewhere outside this
    /// module.
    ///
    /// Mechanical, in the shape of `staged_fold::tests::no_decision_path_reads_the_fold`:
    /// a variant that exists but is never named at a boundary is decoration, and the U4
    /// inventory's whole value is that the covered set is checkable rather than claimed.
    /// Removing the guard call from any one of the four boundaries reds this.
    #[test]
    fn every_enumerated_publication_path_is_wired_at_a_boundary() {
        let variants = [
            "P1BufferedRange",
            "P3aGetResponseTee",
            "P3cMultipartPartSink",
            "P4FullObjectStore",
        ];
        let mut sources = Vec::new();
        for entry in std::fs::read_dir("src").expect("src/ must be readable from the crate root") {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "rs")
                && path.file_name().is_some_and(|n| n != "capacity_guard.rs")
            {
                sources.push(std::fs::read_to_string(&path).unwrap());
            }
        }
        assert!(!sources.is_empty(), "found no sources under src/");
        for variant in variants {
            let hits = sources.iter().filter(|s| s.contains(variant)).count();
            assert!(
                hits > 0,
                "PublicationPath::{} is enumerated but never used at a boundary — either \
                 wire it or delete the variant",
                variant
            );
        }
        // And the boundaries call the guard rather than re-implementing it.
        let admit_sites = sources
            .iter()
            .filter(|s| s.contains("admit_publication("))
            .count();
        assert!(
            admit_sites >= 2,
            "expected admit_publication call sites in at least disk_cache.rs and cache.rs, \
             found {}",
            admit_sites
        );
    }
}
