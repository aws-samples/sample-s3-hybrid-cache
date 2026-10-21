//! The `st_size` fold — the readdir-plus-`stat` producer of the per-shard byte aggregate.
//!
//! # Why this is a separate module and a separate pass
//!
//! [`crate::staged_fold`] parses `.meta` files and is therefore bound to the rolling pass's
//! rotation cadence. This fold needs nothing but `readdir` and `stat`, so it runs on its own
//! interval and touches no `.meta` body. Riding it on the parsing pass's cursor would inherit
//! that cadence and re-create the need for aggregate deltas, which the design retired.
//!
//! The two passes are **two producers of one document**. The document is
//! [`crate::staged_fold::StagedShardCheckpoint`], whose `#[serde(flatten)]` `other` slot exists
//! precisely so this fold can land without a schema change. Neither producer may drop the
//! other's fields, and because both do a read-modify-write of one file, both take the
//! per-document advisory lock in
//! [`crate::staged_fold::lock_checkpoint_document`] — see `install_fold_fields` for why that is
//! not `global_consolidation.lock`.
//!
//! # What it writes, and what it deliberately does not
//!
//! Six fields: `entry_count`, `range_count`, `byte_sum`, `histogram`, `scanned_at` and
//! `scanned_by`. Three further fields belong to the *non-staged* half of the record and are
//! **left absent** rather than written as zero:
//!
//! - `tail_floor` and `tail_entries` describe a partition this pass does not write.
//! - `orphan_bytes_seen` is defined as "readdir, **diffed against the parse**". A `stat`-only
//!   fold cannot compute it, and a name-level approximation is a different quantity.
//!
//! An absent field is a **missing key** in `other`; a folded zero is a key present with value
//! `0`. `serde_json::Map::get` separates them, so a reader can always tell "this fold has not
//! computed that" from "this fold computed zero". Nothing here inserts a key it did not fold.
//!
//! # Orphans are published as a difference, never absorbed
//!
//! `byte_sum` sums **every** `.bin` file, orphans included, and nothing debits an orphan from
//! it. The orphan population is therefore the difference between this figure and a
//! `.meta`-sourced one, which is where the design wants it; this fold does not attempt to
//! identify or subtract it.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::staged_fold::{
    is_shard_level, lock_checkpoint_document, read_shard_checkpoints, Shard, ShardCheckpointSet,
    StagedShardCheckpoint,
};

/// Number of histogram classes: one below the first edge, thirty-two power-of-two classes, and
/// one open class above the last edge.
///
/// Class `i` of the design's numbering covers `[2^(i+9), 2^(i+10))` for `i = 0..=31`, so
/// `i = 0` is 512 B – 1 KiB and `i = 11` is 1 MiB – 2 MiB. Index 0 here is the design's
/// class `-1` (below 512 B), indices `1..=32` are design classes `0..=31`, and index 33 is the
/// open class at or above `2^41` (2 TiB).
///
/// 34 × 8 bytes is the 272 bytes per shard the design budgets, which is the arithmetic that
/// confirms 34 rather than some other count.
pub const HISTOGRAM_CLASSES: usize = 34;

/// The exponent of the first class edge: `2^9` = 512 B.
const FIRST_EDGE_SHIFT: u32 = 9;

/// Which histogram index a file size falls in.
///
/// Fixed, distribution-independent edges, so two shards' histograms are summable element-wise.
/// Deriving edges from the observed distribution would make two shards compute different edges
/// and their histograms unaddable, which is the property the global sum depends on.
pub(crate) fn histogram_class(size: u64) -> usize {
    if size < (1u64 << FIRST_EDGE_SHIFT) {
        return 0;
    }
    // `ilog2` of a value at or above 512 is at least 9, so the subtraction cannot underflow.
    let idx = (size.ilog2() - FIRST_EDGE_SHIFT) as usize + 1;
    idx.min(HISTOGRAM_CLASSES - 1)
}

/// One shard's folded figure: every field below is computed by this pass from `readdir` plus
/// `stat`. `unstatable` is the one that is not part of the installed checkpoint — see its
/// own comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardFigure {
    /// Objects, i.e. `.meta` files in this shard. `readdir` of the metadata leaves.
    pub entry_count: u64,
    /// `.bin` files in this shard. `readdir` of the range leaves.
    pub range_count: u64,
    /// Σ `st_size` over those `.bin` files.
    pub byte_sum: u64,
    /// `st_size` binned by [`histogram_class`]. Always [`HISTOGRAM_CLASSES`] long.
    pub histogram: Vec<u64>,
    /// `.bin` files named by a leaf listing that could not then be `stat`ed.
    ///
    /// Not a field of the checkpoint. It exists so a caller can tell a genuinely small shard
    /// from one whose files vanished under the walk — a file removed between the listing and
    /// the `stat` is normal (eviction races a fold), a large count is not.
    pub unstatable: u64,
}

impl Default for ShardFigure {
    fn default() -> Self {
        Self {
            entry_count: 0,
            range_count: 0,
            byte_sum: 0,
            histogram: vec![0; HISTOGRAM_CLASSES],
            unstatable: 0,
        }
    }
}

impl ShardFigure {
    fn record_range(&mut self, size: u64) {
        self.range_count = self.range_count.saturating_add(1);
        self.byte_sum = self.byte_sum.saturating_add(size);
        let idx = histogram_class(size);
        self.histogram[idx] = self.histogram[idx].saturating_add(1);
    }
}

/// The pass-scoped figure for `.bin` files sitting at the `ranges/` **root**.
///
/// **Pass-scoped and never absorbed into any shard, because a flat file names no shard.** Its
/// path carries neither a bucket nor an `L1`, so no `(bucket, L1)` checkpoint can honestly
/// claim it, and attributing it to one would make that shard's figure wrong in a way nothing
/// could detect. The same reasoning already applies to a legacy flat write-ledger file in
/// [`crate::staged_fold`], which is pass-scoped for exactly this reason.
///
/// The design's own requirement here is a **coverage** one — the population must be visible —
/// and a pass-scoped figure delivers that without corrupting a shard's authority.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyFlatFigure {
    /// `.bin` files directly under `ranges/`.
    pub range_count: u64,
    /// Σ `st_size` over them.
    pub byte_sum: u64,
    /// Whether the root listing itself failed, which makes the figure unknown rather than zero.
    pub unreadable: bool,
}

/// What happened to one shard's install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldFieldInstall {
    /// The six fields are on disk.
    Installed,
    /// A peer folded this shard after this pass started, so this pass's figure is the older of
    /// the two and was discarded. Newest-`scanned_at` wins.
    DeclinedNewerScan,
}

/// One pass's result.
#[derive(Debug, Clone, Default)]
pub struct StSizeFoldOutcome {
    /// Shards whose six fields this pass installed.
    pub installed: Vec<Shard>,
    /// Shards a peer had folded more recently.
    pub declined_newer: Vec<Shard>,
    /// Shards whose visit or install failed. Each keeps its previous document.
    pub failed: Vec<Shard>,
    /// The `ranges/` root population.
    pub legacy_flat: LegacyFlatFigure,
    /// How long the pass took.
    pub duration: Duration,
    /// Whether the pass stopped on its wall-clock budget with shards still unvisited.
    pub budget_exhausted: bool,
    /// Whether shard enumeration failed, which makes the pass's coverage unknown.
    pub enumeration_failed: bool,
}

/// Default interval between folds: one hour.
///
/// **A planning figure with ±2× headroom, not a measurement of a production cache.** The
/// hour comes from a synthetic scratch subtree of 1-byte files on a shared live EFS volume,
/// from a harness whose sibling occupancy measurement disagrees with an earlier table by an
/// unexplained 40% and is non-monotonic across occupancy — and EFS is not the shared-storage
/// backend the deployment guides recommend. Treat it as the starting point for the scaling
/// below rather than as the interval a large volume will actually run at.
pub const DEFAULT_SHARD_FOLD_INTERVAL_SECS: u64 = 3_600;

/// How many times the last fold's own duration the next interval must be, at minimum.
///
/// **This is what makes the interval scale with population rather than with a constant.** A
/// fixed hour breaks at large populations: the same traversal over roughly ten times the
/// design point's range count takes roughly ten hours, so an hourly schedule would ask for a
/// fold every hour that cannot finish inside ten. Scaling off the *measured* duration of the
/// last fold sizes the interval on occupancy — which is what the cost actually tracks — and
/// needs no rate constant, so there is no figure here to be withdrawn when a rate is
/// re-measured.
///
/// Two, so a volume spends at most about half its time folding. One would schedule the next
/// fold for the instant the last one ended.
const DURATION_SLACK: u64 = 2;

/// Seconds the last completed fold took. 0 until one completes in this process.
static LAST_FOLD_DURATION_SECS: AtomicU64 = AtomicU64::new(0);

/// When the last fold completed, seconds since the epoch. 0 until this process has either
/// completed a fold or seeded the value from the volume (see [`maybe_run_st_size_fold`]).
static LAST_FOLD_COMPLETED_SECS: AtomicU64 = AtomicU64::new(0);

/// A fold is running in this process right now.
static FOLD_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// How many times a scheduling tick found a fold already running.
static FOLDS_DECLINED_OVERLAP_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Seconds the last completed fold took.
pub fn last_fold_duration_secs() -> u64 {
    LAST_FOLD_DURATION_SECS.load(Ordering::Relaxed)
}

/// How many scheduling ticks were declined because a fold was already running.
///
/// A non-zero value is not an error: it is the never-overlap rule doing its job, and it is the
/// signal that the configured interval is shorter than the volume can sustain.
pub(crate) fn folds_declined_overlap_total() -> u64 {
    FOLDS_DECLINED_OVERLAP_TOTAL.load(Ordering::Relaxed)
}

/// The interval actually enforced: the configured value, raised to
/// [`DURATION_SLACK`] × the last fold's measured duration when that is larger.
///
/// Never lowers the configured interval. An operator who asks for a longer interval than the
/// volume needs gets what they asked for; an operator who asks for one the volume cannot
/// sustain gets the sustainable one and a WARN.
pub(crate) fn effective_interval_secs(configured_secs: u64) -> u64 {
    let floor = last_fold_duration_secs().saturating_mul(DURATION_SLACK);
    configured_secs.max(floor)
}

/// The three figures that make this pass observable.
///
/// # Why a declined-overlap counter is the one that matters
///
/// This pass's characteristic failure is **silent**. A tick that finds a pass already running
/// returns `None`, which is a normal outcome and not an error; it does not touch the cadence
/// timestamp, and it logs at `debug`. So a deployment where the pass never once completes looks
/// identical on every other signal to one where it completes on schedule — the figures on disk
/// are simply older, and nothing says why.
/// [`StSizeFoldGauges::st_size_folds_declined_overlap_total`] is the discriminator, which is why
/// it is exported rather than left as a process-local counter.
///
/// # Two are per-instance, one is fleet-wide
///
/// The duration and the decline count are process-local atomics, so they describe **this**
/// instance and a zero on one instance says nothing about the fleet. The scan age is read from
/// the shared volume and so is the same on every instance that can read it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StSizeFoldGauges {
    /// Seconds the last completed pass took, in this process. 0 before one completes, and 0 for
    /// a pass that finished inside a second — this is a whole-second figure, so a zero is not
    /// evidence that nothing ran. Read it with the scan age below.
    pub st_size_fold_last_duration_secs: u64,

    /// Scheduling ticks declined because a pass was already running, since process start.
    ///
    /// **Non-zero is not an error** — it is the never-overlap rule working, and it says the
    /// configured interval is shorter than this volume can sustain. A value that keeps climbing
    /// while the scan age below also climbs is the failure this gauge exists for: the pass is
    /// being asked for repeatedly and finishing never.
    pub st_size_folds_declined_overlap_total: u64,

    /// `max` over shards of `now − scanned_at`, seconds — the age of the **stalest** shard
    /// contributing to the per-shard aggregate.
    ///
    /// A maximum rather than a mean, for the same reason the rolling pass's own age gauge is:
    /// an aggregate is only as fresh as its stalest component, and a mean hides exactly the
    /// shard that is not being visited.
    ///
    /// `None` is the UNREADABLE-or-INCOMPLETE sentinel and never a zero. It means one of three
    /// things, all of which make a number a lie: a shard's document could not be read, no shard
    /// has a figure yet, or **at least one shard has no `scanned_at`** — a partial membership.
    /// A maximum taken over a subset of shards is indistinguishable from a fresher cache.
    pub st_size_fold_oldest_shard_scan_age_secs: Option<u64>,
}

/// Collect the three gauges. Safe to call from a metrics scrape.
///
/// # The caller supplies the document set, and that is what removed the doubled read
///
/// The scan age cannot be computed from process-local state: an instance's own last pass says
/// nothing about a shard some other instance folded, or about a shard no instance has reached.
/// It is therefore derived from the per-shard document set — one directory listing plus one small
/// file per shard on shared storage.
///
/// **That listing used to be paid TWICE per scrape**, because this collector read the set from its
/// own entry point while the staged fold's gauge collector read the same set from its own, and the
/// two could not share without a signature change. This is that signature change: the set arrives
/// as an argument, so `/metrics` traverses `checkpoints/` once and both collectors read the same
/// traversal.
///
/// Two consequences worth stating, because neither is visible from the type:
///
/// - **The figures are now mutually consistent.** Two reads microseconds apart could straddle a
///   peer's checkpoint install, so the staged fold's gauges and this scan age could describe two
///   different memberships. One traversal makes that unconstructible rather than merely unlikely.
/// - **This function no longer touches the filesystem at all.** Its cost is the caller's.
///
/// [`oldest_shard_scan_age_secs`] remains for callers that hold only a path and are not on a hot
/// path.
pub fn collect_st_size_fold_gauges(checkpoints: &ShardCheckpointSet) -> StSizeFoldGauges {
    StSizeFoldGauges {
        st_size_fold_last_duration_secs: last_fold_duration_secs(),
        st_size_folds_declined_overlap_total: folds_declined_overlap_total(),
        st_size_fold_oldest_shard_scan_age_secs: oldest_shard_scan_age_from_checkpoints(
            checkpoints,
        ),
    }
}

/// Age in seconds of the stalest shard figure this pass has installed, or `None`.
///
/// Reads the document set itself. Prefer [`oldest_shard_scan_age_from_checkpoints`] on any path
/// that already holds a set — the scrape path does, and reading it twice there is the cost this
/// split exists to remove.
///
/// See [`StSizeFoldGauges::st_size_fold_oldest_shard_scan_age_secs`] for what `None` means and
/// why it is not a zero.
pub fn oldest_shard_scan_age_secs(cache_dir: &Path) -> Option<u64> {
    oldest_shard_scan_age_from_checkpoints(&read_shard_checkpoints(cache_dir))
}

/// [`oldest_shard_scan_age_secs`] over a set the caller already holds.
///
/// The whole derivation is a fold over the set plus a clock read, which is what makes the
/// de-duplication above sound: there is no second input this could need, so a shared set cannot
/// give a different answer from a private one.
pub(crate) fn oldest_shard_scan_age_from_checkpoints(set: &ShardCheckpointSet) -> Option<u64> {
    if !set.is_complete() {
        return None;
    }
    let now = now_nanos();
    let mut stalest: Option<u64> = None;
    for (_shard, checkpoint) in set.iter() {
        // A shard with no `scanned_at` has never been folded, so the set is not a complete
        // membership and no maximum over it is honest. Absence is a missing key, which is
        // exactly what this pass's install discipline guarantees it will be.
        let scanned_at = checkpoint
            .other
            .get("scanned_at")
            .and_then(|v| v.as_u64())?;
        let age = now.saturating_sub(scanned_at) / 1_000_000_000;
        stalest = Some(stalest.map_or(age, |s: u64| s.max(age)));
    }
    stalest
}

/// [`oldest_shard_scan_age_secs`] off the async runtime: it lists `checkpoints/` and reads one
/// document per shard, which is blocking I/O on shared storage.
///
/// `None` (a cold volume, an unreadable document, a shard with no `scanned_at`, or a failed
/// task) means the volume cannot show it is fresh, and the caller folds.
async fn volume_scan_age_secs(cache_dir: &Path) -> Option<u64> {
    let dir = cache_dir.to_path_buf();
    tokio::task::spawn_blocking(move || oldest_shard_scan_age_secs(&dir))
        .await
        .ok()
        .flatten()
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Run a fold if the interval has elapsed and no fold is already running.
///
/// # Never overlapping itself is enforced by scheduling, not by the estimate being right
///
/// Two independent mechanisms, and the second is the one that holds when the first is
/// misconfigured:
///
/// 1. [`effective_interval_secs`] raises the interval to twice the last fold's measured
///    duration, so the *schedule* does not ask for an overlapping fold in the first place.
/// 2. `FOLD_IN_PROGRESS` is a compare-exchange, so even an interval of one second cannot
///    start a second fold. A tick that finds a fold running returns `None` immediately,
///    counts itself in [`folds_declined_overlap_total`], and **does not touch the cadence
///    timestamp** — so the next tick retries rather than the decline consuming an interval.
///
/// A fold still running when the next interval arrives therefore simply continues; the
/// arriving tick is declined and the pass is not interrupted, restarted, or duplicated.
///
/// # The interval is a property of the volume, not of the process
///
/// When the process-local cadence says a fold is due, including the first tick after a
/// process start, the gate reads the age of the stalest shard's `scanned_at` from the shared
/// volume. If that age is inside the interval, no fold runs and the process-local timestamp is
/// seeded to `now − age`, so the next fold falls due when the stalest shard ages past the
/// interval. A restart, or a rolling restart of the whole fleet, inside the interval therefore
/// folds nothing, and a peer's recent fold counts for this instance too.
///
/// A volume that cannot show it is fresh still folds: no shard documents (a cold volume), an
/// unreadable document, or any shard without a `scanned_at`. A pass that ran out of budget
/// leaves its unvisited shards at their older ages, so the next process to check folds.
///
/// The volume is read only when the process-local cadence says a fold is due, so it costs one
/// read of the shard documents per interval, not one per tick.
///
/// `None` means no fold ran, which is a normal outcome and not an error.
pub async fn maybe_run_st_size_fold(
    cache_dir: &Path,
    configured_interval: Duration,
    max_duration: Duration,
    instance_id: &str,
) -> Option<StSizeFoldOutcome> {
    let configured_secs = configured_interval.as_secs();
    let effective = effective_interval_secs(configured_secs);
    if effective > configured_secs {
        debug!(
            "st_size fold: configured interval {}s raised to {}s, which is {}x the last \
             fold's measured {}s. The volume cannot sustain the configured interval.",
            configured_secs,
            effective,
            DURATION_SLACK,
            last_fold_duration_secs()
        );
    }

    let now = unix_secs();
    let last = LAST_FOLD_COMPLETED_SECS.load(Ordering::Relaxed);
    if last != 0 && now.saturating_sub(last) < effective {
        return None;
    }

    // Process-local cadence says a fold is due. Before paying for one, ask the volume: if every
    // shard was folded inside the interval, by this process before a restart or by a peer, the
    // volume is not due and this process's cadence is re-seeded from it.
    if let Some(age) = volume_scan_age_secs(cache_dir).await {
        if age < effective {
            // Every instance reads the same volume age, so an unjittered seed puts the whole
            // fleet due at the same second and every instance walks the volume together.
            let jitter = seed_jitter_secs(instance_id, effective);
            let seeded = now.saturating_sub(age).saturating_add(jitter);
            LAST_FOLD_COMPLETED_SECS.store(seeded.max(1), Ordering::Relaxed);
            info!(
                "st_size fold: not due. The stalest shard on the volume was folded {}s ago, \
                 inside the {}s interval, so this process's cadence is seeded from the volume \
                 and the next fold is due in {}s ({}s of it per-instance jitter).",
                age,
                effective,
                (effective - age).saturating_add(jitter),
                jitter
            );
            return None;
        }
    }

    if FOLD_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        FOLDS_DECLINED_OVERLAP_TOTAL.fetch_add(1, Ordering::Relaxed);
        debug!(
            "st_size fold: a fold is already running, so this tick is declined. The cadence \
             timestamp is untouched, so the next tick retries."
        );
        return None;
    }

    // Clears the flag on every exit, including an abort: the consolidator runs this pass on its
    // own task and aborts that task at shutdown, and a flag left set would refuse every later
    // pass in the process.
    let _reset = scopeguard::guard((), |()| FOLD_IN_PROGRESS.store(false, Ordering::SeqCst));

    let outcome = run_st_size_fold(cache_dir, instance_id, max_duration).await;

    let finished = unix_secs();
    if outcome.enumeration_failed {
        // Nothing was folded, so this is not a completion. The duration is not recorded (it
        // measures a failed listing, not a traversal) and the cadence is stamped so the next
        // attempt falls due after the bounded backoff rather than a whole interval.
        let backoff = enumeration_failure_backoff_secs(effective);
        LAST_FOLD_COMPLETED_SECS.store(
            finished
                .saturating_add(backoff)
                .saturating_sub(effective)
                .max(1),
            Ordering::Relaxed,
        );
        warn!(
            "st_size fold: shard enumeration failed; retrying in {}s rather than after the {}s \
             interval",
            backoff, effective
        );
    } else {
        LAST_FOLD_DURATION_SECS.store(outcome.duration.as_secs(), Ordering::Relaxed);
        LAST_FOLD_COMPLETED_SECS.store(finished, Ordering::Relaxed);
    }
    Some(outcome)
}

/// Longest wait before retrying a pass whose shard enumeration failed: five minutes.
pub const ENUMERATION_FAILURE_BACKOFF_MAX_SECS: u64 = 300;

/// The retry delay after an enumeration failure: a tenth of the effective interval, at least
/// one second, at most [`ENUMERATION_FAILURE_BACKOFF_MAX_SECS`], and never past the interval
/// itself. A tenth keeps a short test interval testable and an hourly one at six minutes,
/// capped to five.
pub fn enumeration_failure_backoff_secs(effective_secs: u64) -> u64 {
    (effective_secs / 10)
        .clamp(1, ENUMERATION_FAILURE_BACKOFF_MAX_SECS)
        .min(effective_secs)
}

/// Per-instance delay added to a schedule seeded from the volume's age, seconds: stable for an
/// instance id, in `[0, effective / 10)`, so instances reading the same volume age fall due at
/// different moments. Derived from a BLAKE3 hash of the instance id, so it survives a restart.
///
/// Strictly below a tenth: an interval under ten seconds gets no jitter.
pub fn seed_jitter_secs(instance_id: &str, effective_secs: u64) -> u64 {
    let span = effective_secs / 10;
    if span == 0 {
        return 0;
    }
    let hash = blake3::hash(instance_id.as_bytes());
    let mut first = [0u8; 8];
    first.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(first) % span
}

/// Whether a fold is running in this process right now.
///
/// Lets a scheduler decline to ask for a fold while one runs, so a tick that could never start
/// one is not counted in [`folds_declined_overlap_total`]. The compare-exchange in
/// [`maybe_run_st_size_fold`] still decides; this is a peek.
pub fn fold_in_progress() -> bool {
    FOLD_IN_PROGRESS.load(Ordering::SeqCst)
}

/// One fold pass: enumerate shards, order them, visit each, install its six fields as it goes.
///
/// # Order: unreached shards first, then stalest first
///
/// The pass reads the checkpoint set once at its start (off the async runtime) and visits
/// shards in `fold_order`: shards with no checkpoint document, then shards whose document
/// has no `scanned_at`, then the rest by oldest `scanned_at`, with the shard key breaking ties.
/// A pass that runs out of budget therefore leaves the shards it did not reach at the front of
/// the next pass, and successive passes cover the whole population: with `k` shards per pass,
/// every shard has a figure within `ceil(N / k)` passes. If the checkpoint read fails, the pass
/// logs it and visits in key order.
///
/// Two instances whose passes start close together read the same set and so visit in the
/// same order. The per-document lock and [`install_fold_fields`]' newest-`scanned_at` rule
/// make the second visit to a shard either install a full recount over the first or discard
/// itself, so the overlap costs repeated traversal and never a wrong figure.
///
/// # Abortability
///
/// The shard is the unit. The wall-clock budget is checked at the **top** of the shard loop,
/// before a shard is selected and before any of its leaves are read, so a shard is visited
/// whole or not at all and no half-figure can be installed. A pass that runs out of budget
/// leaves a **mixed-age but well-defined** sum: every shard it reached carries a fresh
/// `scanned_at`, every shard it did not carries its previous one, and the per-shard ages are
/// what make the mix visible. Failing to complete inside the budget is slower convergence,
/// not a correctness failure.
///
/// `max_duration` of zero means unbounded.
pub async fn run_st_size_fold(
    cache_dir: &Path,
    instance_id: &str,
    max_duration: Duration,
) -> StSizeFoldOutcome {
    run_st_size_fold_capped(cache_dir, instance_id, max_duration, None).await
}

/// [`run_st_size_fold`] with an optional cap on shards visited per pass. The cap is a
/// deterministic stand-in for the wall-clock budget, so a test can bound a pass by count.
pub(crate) async fn run_st_size_fold_capped(
    cache_dir: &Path,
    instance_id: &str,
    max_duration: Duration,
    max_shards: Option<usize>,
) -> StSizeFoldOutcome {
    let started = Instant::now();
    let pass_start_nanos = now_nanos();
    let mut outcome = StSizeFoldOutcome::default();

    let shards = match enumerate_shards(cache_dir) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "st_size fold: could not enumerate shards under {:?}: {}. No shard is folded \
                 this pass; every previous figure stays on disk.",
                cache_dir, e
            );
            outcome.enumeration_failed = true;
            outcome.duration = started.elapsed();
            return outcome;
        }
    };

    let dir = cache_dir.to_path_buf();
    let checkpoints = match tokio::task::spawn_blocking(move || read_shard_checkpoints(&dir)).await
    {
        // The root listing failed: `read_shard_checkpoints` reports that as one unreadable
        // file and nothing read, so no document's absence or age is known.
        Ok(set) if set.files_read == 0 && set.files_unreadable > 0 => {
            warn!(
                "st_size fold: the checkpoint directory under {:?} could not be listed, so \
                 this pass visits shards in key order rather than unreached-then-stalest first",
                cache_dir
            );
            None
        }
        Ok(set) => Some(set),
        Err(e) => {
            warn!(
                "st_size fold: reading the checkpoint set failed ({}), so this pass visits \
                 shards in key order rather than unreached-then-stalest first",
                e
            );
            None
        }
    };
    let order = fold_order(&shards, checkpoints.as_ref());

    for (visited, shard) in order.iter().enumerate() {
        let over_count = max_shards.is_some_and(|cap| visited >= cap);
        if over_count || (!max_duration.is_zero() && started.elapsed() >= max_duration) {
            outcome.budget_exhausted = true;
            debug!(
                "st_size fold: budget of {:?} spent after {} shard(s). The rest keep their \
                 previous figures, come first in the next pass, and this pass's sum is \
                 mixed-age.",
                max_duration, visited
            );
            break;
        }

        let figure = match fold_shard(cache_dir, shard).await {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "st_size fold: could not fold shard {}: {}. Its previous figure stays on \
                     disk.",
                    shard, e
                );
                outcome.failed.push(shard.clone());
                continue;
            }
        };
        if figure.unstatable > 0 {
            debug!(
                "st_size fold: {} range file(s) in shard {} were listed but could not be \
                 stat'ed, so byte_sum is short by them",
                figure.unstatable, shard
            );
        }

        match install_fold_fields(cache_dir, shard, &figure, instance_id, pass_start_nanos) {
            Ok(FoldFieldInstall::Installed) => {
                debug!(
                    "st_size fold installed shard {}: entry_count={}, range_count={}, \
                     byte_sum={}",
                    shard, figure.entry_count, figure.range_count, figure.byte_sum
                );
                outcome.installed.push(shard.clone());
            }
            Ok(FoldFieldInstall::DeclinedNewerScan) => {
                debug!(
                    "st_size fold: shard {} carries a newer scan than this pass started with, \
                     so this pass's older figure was discarded",
                    shard
                );
                outcome.declined_newer.push(shard.clone());
            }
            Err(e) => {
                warn!(
                    "st_size fold: could not install shard {}'s figure: {}. Its previous \
                     document is intact.",
                    shard, e
                );
                outcome.failed.push(shard.clone());
            }
        }
    }

    outcome.legacy_flat = fold_legacy_flat_root(cache_dir);
    if outcome.legacy_flat.range_count > 0 {
        debug!(
            "st_size fold: the ranges/ root holds {} legacy flat range file(s) totalling {} \
             bytes. Pass-scoped: a flat file names no shard, so no shard's figure claims it.",
            outcome.legacy_flat.range_count, outcome.legacy_flat.byte_sum
        );
    }

    outcome.duration = started.elapsed();
    outcome
}

/// The order a pass visits `shards` in: shards with no checkpoint document, then shards whose
/// document has no `scanned_at`, then the rest by oldest `scanned_at`. The shard key breaks
/// every tie, so the order is deterministic. With no checkpoint set (the read failed), key
/// order.
pub(crate) fn fold_order(
    shards: &BTreeSet<Shard>,
    checkpoints: Option<&ShardCheckpointSet>,
) -> Vec<Shard> {
    let Some(set) = checkpoints else {
        return shards.iter().cloned().collect();
    };
    let mut keyed: Vec<((u8, u64), &Shard)> = shards
        .iter()
        .map(|shard| {
            let rank = match set.get(shard) {
                None => (0, 0),
                Some(c) => match c.other.get("scanned_at").and_then(|v| v.as_u64()) {
                    None => (1, 0),
                    Some(at) => (2, at),
                },
            };
            (rank, shard)
        })
        .collect();
    // `shards` iterates in key order and the sort is stable, so equal ranks stay in key order.
    keyed.sort_by_key(|(rank, _)| *rank);
    keyed.into_iter().map(|(_, s)| s.clone()).collect()
}

/// Every `(bucket, L1)` shard named by either tree.
///
/// The union of `ranges/` and `metadata/`, not either alone: a shard whose `.bin` files have
/// all been evicted still has `.meta` files to count, and a shard mid-write can have range
/// files before its metadata leaf exists. Taking one tree only would leave the other's shards
/// carrying a stale figure indefinitely, which is the one kind of staleness the per-shard ages
/// cannot show, because an unvisited shard looks exactly like a shard that was visited and
/// found unchanged.
///
/// `L1` names are filtered through [`is_shard_level`], so a stray directory cannot become a
/// shard no cache key could ever map to.
pub fn enumerate_shards(cache_dir: &Path) -> std::io::Result<BTreeSet<Shard>> {
    let mut out = BTreeSet::new();
    // A missing tree is an empty tree: neither existing is an empty cache, not a failure.
    for root in ["ranges", "metadata"] {
        shards_under_root(&cache_dir.join(root), &mut out)?;
    }
    Ok(out)
}

/// Insert every shard under one tree root (`ranges/` or `metadata/`) into `out`. A root that
/// does not exist contributes nothing.
fn shards_under_root(root_path: &Path, out: &mut BTreeSet<Shard>) -> std::io::Result<()> {
    let buckets = match std::fs::read_dir(root_path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for bucket_entry in buckets.flatten() {
        let bucket_path = bucket_entry.path();
        if !bucket_path.is_dir() {
            continue;
        }
        let bucket = match bucket_path.file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => continue,
        };
        // `_shard_state`, `_journals` and `_write_ledger` live under `metadata/` beside the
        // buckets. They are not buckets and hold no cache entries.
        if bucket.starts_with('_') {
            continue;
        }
        let l1s = match std::fs::read_dir(&bucket_path) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "st_size fold: could not list {:?}: {}. Shards under it are not folded \
                     this pass.",
                    bucket_path, e
                );
                continue;
            }
        };
        for l1_entry in l1s.flatten() {
            let l1_path = l1_entry.path();
            if !l1_path.is_dir() {
                continue;
            }
            let l1 = match l1_path.file_name() {
                Some(n) => n.to_string_lossy().to_string(),
                None => continue,
            };
            if !is_shard_level(&l1) {
                continue;
            }
            out.insert(Shard::new(bucket.clone(), l1));
        }
    }
    Ok(())
}

/// Fold one shard: `readdir` its metadata and range leaves, `stat` each range file.
///
/// The filesystem work runs on the blocking pool. It is synchronous `readdir` plus `stat`
/// against shared storage, where a single round trip can be milliseconds, and a shard at the
/// design point's occupancy holds thousands of entries — that is not work an async worker may
/// do, because a worker blocked in `stat` is a worker not serving requests.
pub async fn fold_shard(cache_dir: &Path, shard: &Shard) -> std::io::Result<ShardFigure> {
    let cache_dir = cache_dir.to_path_buf();
    let shard = shard.clone();
    tokio::task::spawn_blocking(move || fold_shard_blocking(&cache_dir, &shard))
        .await
        .map_err(|e| std::io::Error::other(format!("st_size fold shard task failed: {e}")))?
}

/// The synchronous body of [`fold_shard`].
///
/// **`readdir` plus `stat` only — never a `.meta` parse.** Keeping the parse out is what makes
/// this fold cheap enough to run on its own cadence, and `entry_count` needs only the count of
/// `.meta` names, not their contents.
fn fold_shard_blocking(cache_dir: &Path, shard: &Shard) -> std::io::Result<ShardFigure> {
    let mut figure = ShardFigure::default();

    // `entry_count`: `.meta` names under `metadata/{bucket}/{L1}/{L2}/`.
    let meta_shard = cache_dir
        .join("metadata")
        .join(&shard.bucket)
        .join(&shard.l1);
    for leaf in shard_leaves(&meta_shard)? {
        for name in leaf_file_names(&leaf)? {
            if is_folded_file(&name, ".meta") {
                figure.entry_count = figure.entry_count.saturating_add(1);
            }
        }
    }

    // `range_count`, `byte_sum`, `histogram`: `.bin` files under `ranges/{bucket}/{L1}/{L2}/`.
    let range_shard = cache_dir.join("ranges").join(&shard.bucket).join(&shard.l1);
    for leaf in shard_leaves(&range_shard)? {
        for name in leaf_file_names(&leaf)? {
            if !is_folded_file(&name, ".bin") {
                continue;
            }
            match std::fs::metadata(leaf.join(&name)) {
                Ok(md) => figure.record_range(md.len()),
                // Removed between the listing and the `stat`. Eviction races a fold by design,
                // so this is expected rather than exceptional — but it is counted, because a
                // large count means something other than eviction is removing files.
                Err(_) => figure.unstatable = figure.unstatable.saturating_add(1),
            }
        }
    }

    Ok(figure)
}

/// The `{L2}` leaf directories of one shard. An absent shard directory yields none.
fn shard_leaves(shard_dir: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    let entries = match std::fs::read_dir(shard_dir) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.push(path);
        }
    }
    Ok(out)
}

/// File names directly in one leaf directory.
fn leaf_file_names(leaf: &Path) -> std::io::Result<Vec<String>> {
    let entries = match std::fs::read_dir(leaf) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        if entry.path().is_dir() {
            continue;
        }
        if let Some(name) = entry.path().file_name() {
            out.push(name.to_string_lossy().to_string());
        }
    }
    Ok(out)
}

/// Whether a file name is one this fold counts.
///
/// **`*.tmp` is excluded BY NAME, in both of the two shapes the tree produces.** A partially
/// written file is not resident, and a fold that counted one would credit bytes that are about
/// to be renamed over or deleted. The two shapes are a trailing `.tmp` and the shared-volume
/// form with a hostname and pid appended (`{name}.tmp.{host}.{pid}`), whose *extension* is the
/// pid — so an extension test alone does not catch it and a substring test is what is needed.
fn is_folded_file(name: &str, suffix: &str) -> bool {
    name.ends_with(suffix) && !name.contains(".tmp")
}

/// The `ranges/` root population: `.bin` files sitting directly in `ranges/`.
///
/// See [`LegacyFlatFigure`] for why this is pass-scoped rather than attributed to a shard.
pub fn fold_legacy_flat_root(cache_dir: &Path) -> LegacyFlatFigure {
    let mut out = LegacyFlatFigure::default();
    let root = cache_dir.join("ranges");
    let entries = match std::fs::read_dir(&root) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return out,
        Err(e) => {
            warn!(
                "st_size fold: could not list the ranges/ root {:?}: {}. The legacy flat \
                 figure is unknown rather than zero.",
                root, e
            );
            out.unreadable = true;
            return out;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        let name = match path.file_name() {
            Some(n) => n.to_string_lossy().to_string(),
            None => continue,
        };
        if !is_folded_file(&name, ".bin") {
            continue;
        }
        match std::fs::metadata(&path) {
            Ok(md) => {
                out.range_count = out.range_count.saturating_add(1);
                out.byte_sum = out.byte_sum.saturating_add(md.len());
            }
            Err(_) => out.unreadable = true,
        }
    }
    out
}

/// Install this fold's six fields into a shard's checkpoint, preserving every other field.
///
/// # This is a read-modify-write of a document with two producers
///
/// The staged fold owns four struct fields of the same document and this fold owns six keys
/// inside its `other` map. Both must read the current document and write it back whole, so two
/// unsynchronised read-modify-writes lose one producer's update with no error and no torn
/// file — the losing write is a complete, parseable, wrong document. The atomic rename each
/// install already performs prevents a *torn* read; it does nothing about a lost update.
///
/// **The lock is per-document, and deliberately NOT `global_consolidation.lock`.** That
/// differs from the rolling pass's write phase, and the reasons are specific rather than
/// stylistic:
///
/// - `JournalConsolidator::try_acquire_global_consolidation_lock` returns `Ok(false)` when
///   this instance already holds it, so it is not re-entrant. The consolidation cycle holds it
///   across the work that would call into here, so a caller reached from inside a cycle could
///   never acquire it and would give up on every attempt.
/// - It is a method on the consolidator, and the staged fold — the *other* producer of this
///   document, which needs the same protection — has no consolidator handle at its install
///   site. A lock only one of two writers can take does not close a lost update.
/// - What needs serialising is one document's read-modify-write. A fleet-wide singleton would
///   serialise this pass against all consolidation for the duration of a whole-volume
///   traversal, which is a much larger cost than the race it would close.
///
/// The rolling pass's own use of the global lock is unaffected and remains correct for what it
/// protects: the derived state in `size_state.json`, which is a single shared document with no
/// per-document lock of its own.
///
/// # Newest-`scanned_at` wins, and it cannot decline this pass's own write
///
/// If the document on disk already carries a `scanned_at` later than this pass's start, a peer
/// folded the shard after this pass began and its figure is the newer one, so this pass
/// discards its own rather than overwriting. The comparison is against **the pass's start**,
/// and this pass visits each shard at most once, so the timestamp it compares against can
/// never be one it wrote itself. That clause matters: comparing against "any timestamp newer
/// than my last write" makes a pass decline its own previous shard and stop after one.
pub fn install_fold_fields(
    cache_dir: &Path,
    shard: &Shard,
    figure: &ShardFigure,
    instance_id: &str,
    pass_start_nanos: u64,
) -> std::io::Result<FoldFieldInstall> {
    // Held until the end of the function, which is after the install.
    let _lock = lock_checkpoint_document(cache_dir, shard);

    let mut checkpoint = match StagedShardCheckpoint::load(cache_dir, shard) {
        Ok(Some(c)) => c,
        Ok(None) => StagedShardCheckpoint::unfolded_for(shard),
        Err(e) => return Err(e),
    };

    if let Some(existing) = checkpoint.other.get("scanned_at").and_then(|v| v.as_u64()) {
        if existing > pass_start_nanos {
            return Ok(FoldFieldInstall::DeclinedNewerScan);
        }
    }

    insert_fold_fields(&mut checkpoint, figure, instance_id);
    checkpoint.install(cache_dir)?;
    Ok(FoldFieldInstall::Installed)
}

/// Write this fold's six fields into `checkpoint`, stamping `scanned_at` now.
fn insert_fold_fields(
    checkpoint: &mut StagedShardCheckpoint,
    figure: &ShardFigure,
    instance_id: &str,
) {
    checkpoint
        .other
        .insert("entry_count".to_string(), figure.entry_count.into());
    checkpoint
        .other
        .insert("range_count".to_string(), figure.range_count.into());
    checkpoint
        .other
        .insert("byte_sum".to_string(), figure.byte_sum.into());
    checkpoint.other.insert(
        "histogram".to_string(),
        serde_json::Value::Array(figure.histogram.iter().map(|c| (*c).into()).collect()),
    );
    checkpoint
        .other
        .insert("scanned_at".to_string(), now_nanos().into());
    checkpoint.other.insert(
        "scanned_by".to_string(),
        serde_json::Value::String(instance_id.to_string()),
    );
}

/// Nanoseconds since the epoch, the unit of `scanned_at` and of a fold's pass start.
pub(crate) fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The class edges are the design's, and the two annotated examples are the check.
    ///
    /// A failure implicates [`histogram_class`]'s `FIRST_EDGE_SHIFT` arithmetic.
    #[test]
    fn the_histogram_classes_are_the_designs_fixed_power_of_two_edges() {
        assert_eq!(histogram_class(0), 0, "below the first edge");
        assert_eq!(histogram_class(511), 0);
        // Design class 0 is [512, 1024) and sits at index 1.
        assert_eq!(histogram_class(512), 1);
        assert_eq!(histogram_class(1023), 1);
        assert_eq!(histogram_class(1024), 2);
        // Design class 11 is [1 MiB, 2 MiB) and sits at index 12.
        assert_eq!(histogram_class(1 << 20), 12);
        assert_eq!(histogram_class((1 << 21) - 1), 12);
        // Design class 31 is [2^40, 2^41). The open class starts at 2^41.
        assert_eq!(histogram_class(1 << 40), 32);
        assert_eq!(histogram_class(1 << 41), HISTOGRAM_CLASSES - 1);
        assert_eq!(histogram_class(u64::MAX), HISTOGRAM_CLASSES - 1);
    }

    /// The interval is raised to the measured duration and never lowered below the configured
    /// value.
    ///
    /// A failure implicates [`effective_interval_secs`].
    #[test]
    fn the_effective_interval_scales_with_the_last_folds_measured_duration() {
        LAST_FOLD_DURATION_SECS.store(0, Ordering::SeqCst);
        assert_eq!(
            effective_interval_secs(3_600),
            3_600,
            "with no measurement the configured value stands"
        );
        // A fold that took ten hours cannot be scheduled hourly.
        LAST_FOLD_DURATION_SECS.store(36_000, Ordering::SeqCst);
        assert_eq!(
            effective_interval_secs(3_600),
            72_000,
            "the hourly interval must scale to twice the measured ten hours"
        );
        // And a longer configured interval is honoured rather than shortened.
        assert_eq!(effective_interval_secs(200_000), 200_000);
        LAST_FOLD_DURATION_SECS.store(0, Ordering::SeqCst);
    }

    /// `*.tmp` is excluded in both of the shapes the tree produces.
    ///
    /// A failure implicates [`is_folded_file`].
    #[test]
    fn a_temp_file_is_excluded_by_name_in_both_shapes() {
        assert!(is_folded_file("key_0-1023.bin", ".bin"));
        assert!(!is_folded_file("key_0-1023.bin.tmp", ".bin"));
        assert!(
            !is_folded_file("key_0-1023.tmp.bin", ".bin"),
            "a `.tmp` anywhere in the name excludes it, not only a trailing one"
        );
        assert!(
            !is_folded_file("key.bin.tmp.host.4242", ".bin"),
            "the shared-volume temp shape does not even end in .bin, but must not be counted \
             through some other suffix rule either"
        );
        assert!(is_folded_file("key.meta", ".meta"));
        assert!(!is_folded_file("key.meta.tmp", ".meta"));
    }

    /// The three ranks in order, key order within a rank and oldest-first within the last.
    ///
    /// A failure implicates [`fold_order`]'s rank or its tie-break.
    #[test]
    fn the_fold_order_is_undocumented_then_unscanned_then_stalest_with_key_ties() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let install = |l1: &str, at: Option<u64>| {
            let mut c = StagedShardCheckpoint::unfolded_for(&Shard::new("b", l1));
            if let Some(at) = at {
                c.other.insert("scanned_at".to_string(), at.into());
            }
            c.install(dir).unwrap();
        };
        install("01", Some(50));
        install("02", Some(10));
        install("03", None);
        install("04", Some(10));
        install("05", None);
        let shards: BTreeSet<Shard> = ["00", "01", "02", "03", "04", "05", "06"]
            .iter()
            .map(|l1| Shard::new("b", *l1))
            .collect();
        let set = read_shard_checkpoints(dir);
        let got: Vec<String> = fold_order(&shards, Some(&set))
            .into_iter()
            .map(|s| s.l1)
            .collect();
        assert_eq!(got, ["00", "06", "03", "05", "02", "04", "01"]);
        let fallback: Vec<String> = fold_order(&shards, None)
            .into_iter()
            .map(|s| s.l1)
            .collect();
        assert_eq!(
            fallback,
            ["00", "01", "02", "03", "04", "05", "06"],
            "with no checkpoint set the order is key order"
        );
    }
}
