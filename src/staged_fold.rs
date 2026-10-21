//! The staged fold: `F`, the `Pending_Credit` term, and the `Fold_Cut` that separates
//! them.
//!
//! Spec: cache-eviction-at-scale §§ 16.6.1, 16.6.3, 16.6.4, 16.6.9.
//!
//! # Two outputs, and only one of them may reach a decision
//!
//! **UPDATED: the fold is now authoritative, behind the activation epoch.** An earlier
//! version of this section said the fold was published as gauges and read by nothing that
//! decided anything, and that "if you are looking for the code that makes the fold
//! authoritative, it does not exist yet". It does now, and the separation that statement
//! described has been replaced by a narrower one:
//!
//! - **[`staged_authority_figure`]** is the decision-bearing output: two numbers, `F +
//!   post-cut pending` and `P`, or `None`. Read by exactly one place,
//!   `CacheManager::staged_authority`, which is where the three published composites and the
//!   staging eviction trigger both come from. Live only once
//!   [`crate::staged_authority::fold_authority_active`] is true; in Phase L the incumbent
//!   event-sourced counter is still the authority, unchanged, which is what makes a mixed
//!   fleet safe.
//! - **[`StagedFoldGauges`]** is the observability output: many fields including ages,
//!   component breakdowns, the incumbent gap and a duration histogram. Still readable only
//!   by this module and `metrics.rs`, because handing a decision path the whole bundle invites
//!   it to branch on the fields it should not.
//!
//!   (Deliberately no field count in this prose. It said "fifteen" while the struct carried
//!   twenty, because a count in a doc comment is not recomputed when a field is added and the
//!   argument never depended on the number.)
//!
//! Both halves are asserted mechanically by
//! `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`, which also
//! asserts the flip is present — an absence would satisfy every containment rule perfectly.
//!
//! # The three objects
//!
//! - **`Ledger_Seq`** — the cut coordinate. A per-instance monotone `u64` counter, stored
//!   explicitly on the ledger entry as [`crate::write_ledger::WriteLedgerEntry::seq`] and
//!   read through [`crate::write_ledger::ledger_seq`]. It is **not derived from any clock**;
//!   an earlier version of this module took it from the entry's `SystemTime` timestamp, and
//!   that doc comment explains what a backward clock step cost. Its base is recovered from
//!   the ledger file's `{"schema":2,"max_seq_ever":N}` header, so a cut is reconstructible
//!   from the file alone rather than only from a checkpoint. See
//!   [`StagedFoldCheckpoint::staged_fold_cut`] for the second, independent guard on
//!   monotonicity at install time.
//! - **`Pending_Credit`** — [`PendingCredit`], appended by the same call site that appends
//!   the ledger entry, to that instance's own `.pending` file.
//! - **`Fold_Cut`** — [`StagedFoldCheckpoint::staged_fold_cut`], installed in the same
//!   atomic write as the figure it explains.
//!
//! # Exactly once, as disjointness rather than as a hope
//!
//! For one instance the seqs are totally ordered. A credit with seq `s` is counted by the
//! fold iff `s <= cut`, and by the pending term iff `s > cut`. Complementary and disjoint:
//! never both, never neither. The pending term is evaluated **by the reader**, not
//! deferred to the next fold, which is what makes a credit visible immediately rather
//! than within one fold interval — and is also what makes the property testable without a
//! race, because the state in which a `.meta` is on disk while its credit is still
//! post-cut is reachable deterministically. `exactly_once_across_the_cut` constructs it.

use crate::write_ledger::{StagedCandidateVerdict, WriteLedger};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Directory under `metadata/` holding shard state, per design § 3.2.
const SHARD_STATE_DIR: &str = "_shard_state";

/// Directory under `_shard_state/` holding the per-instance pending-credit files.
const PENDING_DIR: &str = "staged_pending";

/// Extension for a pending-credit file.
const PENDING_EXT: &str = "pending";

/// Directory under `_shard_state/` holding the § 3.2 per-shard checkpoints, one per
/// `(bucket, L1)` shard at `checkpoints/{bucket}/{XX}.json`.
///
/// **This replaced I2's single global `staged_fold.json` outright**; there is no migration
/// and no compatibility read of the old path. A volume carrying the old file loses one
/// fold interval's authority and re-derives, which is what a fold is for — and leaving two
/// layouts in place would mean two producers of the same figure, which is the error R5.5
/// exists to forbid.
const CHECKPOINTS_DIR: &str = "checkpoints";

/// Directory under `_shard_state/` holding the § 16.2 shard claims.
const CLAIMS_DIR: &str = "claims";

/// Directory under `_shard_state/` holding the per-instance count-eviction reclaim documents,
/// one per instance at `eviction_reclaims/{instance_id}.json`. See [`ReclaimDocument`].
const EVICTION_RECLAIMS_DIR: &str = "eviction_reclaims";

/// Schema of a [`ReclaimDocument`]. An unknown version is read as absent.
const RECLAIM_DOCUMENT_SCHEMA: u32 = 1;

/// Age past which a peer's [`ReclaimDocument`] is no longer debited, seconds.
///
/// `MAX_AUTHORITY_AGE_SECS + CLAIM_LEASE_SECS`. Every shard's staged figure is re-folded within
/// `MAX_AUTHORITY_AGE_SECS` (§ 16.6.4's criterion), and a visit that installs is bounded by its
/// claim lease, so a reclaim this old is reflected in every shard checkpoint by a visit that
/// began after it. Debiting it past that point would subtract it twice. This is what retires
/// the document of an instance that stopped before its next fold install.
pub(crate) const RECLAIM_DOCUMENT_MAX_AGE_SECS: u64 = MAX_AUTHORITY_AGE_SECS + CLAIM_LEASE_SECS;

/// Extension for a shard claim file.
const CLAIM_EXT: &str = "claim";

/// Schema version of the checkpoint document.
const STAGED_FOLD_SCHEMA: u32 = 1;

/// `Staged_Range_Budget` — the maximum number of staged ranges this design admits.
///
/// **Non-configurable, deliberately.** With `S` staged ranges, aggregate rate `Agg(I)`, duty
/// cycle `D` and the age bound `A` ([`MAX_AUTHORITY_AGE_SECS`]), authority age is
/// `S / (Agg × D)`, so the bound is `S ≤ D · A · Agg`. At `I = 1`, `D = 0.167`, `A = 86,400 s`
/// and the **measured** `R = 2,900 obj/s` (EFS, where `Agg(1) = R`):
///
/// ```text
///     S  ≤  0.167 × 86,400 × 2,900  =  41,843,520 ranges
/// ```
///
/// Sized for `I = 1` because a single-instance deployment is supported and a budget that only
/// holds on a three-node fleet is not a budget.
///
/// # Why 32,800,000
///
/// 32,800,000 is **78.4%** of the bound, the shipped margin convention (the earlier pairs sat at
/// 78.5% and 74.6%). `R` was measured on an **idle** fleet, so it is optimistic: a serving fleet
/// contends for the same per-client NFS pipeline, and the ~22% headroom absorbs that.
///
/// It is an **admission gate**, not a statistic: at the budget, `budget_verdict` declines
/// write-through caching of new objects (the upload itself succeeds). At the hourly window the
/// same arithmetic gave 1,300,000, which a write-heavy, rarely-read workload reaches in about a
/// day at 15 small PUTs a second; § 16.6.12 moved the window to 24 h for that reason.
///
/// # `S` and the reachable bucket count are bought out of the same window
///
/// `B ≤ I · (D·A − S/Agg(I)) / (256·c_r)`. On EFS (`c_r = 18.024 ms`) at this budget the reachable
/// bucket count at `I = 1` is **~676**, against a hard ceiling of ~3,127 at any rate. `c_r` is
/// PER-CONFIGURATION (3.774 ms on FSxZ; 14.345 and 7.131 ms measured on FSxN), so read the
/// ceiling per configuration or not at all.
///
/// There is **no config knob and none should be added.** Raising this number requires either
/// raising `R` further or accepting a larger `Max_Authority_Age`. Both are decisions with
/// their own arithmetic, and a knob would let an operator take either of them silently, by
/// tuning, without the arithmetic ever being done.
/// `staged_range_budget_bound_holds_at_the_design_point` is the executable statement of that
/// arithmetic. Raising it on the strength of a larger fleet would require a measured `Agg(I)`
/// for the backend in question.
pub const STAGED_RANGE_BUDGET: u64 = 32_800_000;

/// Default claim lease, seconds. § 16.2 floors the lease at 900 s and derives it from `2 ×`
/// the observed p99 shard visit duration; the floor is what is used here because there is no
/// measured p99 for the staged fold yet, and a floor that is too long costs one duplicated
/// visit while one that is too short costs two instances folding the same shard — R19.2
/// permits both, so the conservative end is free.
const CLAIM_LEASE_SECS: u64 = 900;

/// § 16.6.4's acceptance criterion: `Max_Authority_Age`, seconds, p100.
///
/// A `const` here rather than a figure in prose, because [`shards_per_pass`] is derived from it:
/// the scheduler and the criterion have to be the same number or they drift, which is exactly
/// what happened when the per-pass budget was a magic `8`.
///
/// 86,400 s, the same window as [`crate::ledger_repair::MAX_REPAIR_AGE_SECS`], so the fold and
/// the repair rotation share one cadence class (§ 16.6.12 point 1).
///
/// # Why a day and not an hour
///
/// Nothing that decides anything needs this figure fresh. The one consumer that uses its
/// magnitude is the staging eviction trigger, which needs a bounded **over**-estimate, and an
/// older figure errs only that way: an object deleted or graduated since its shard's last visit
/// stays counted until the next visit, so eviction runs early, never late. New writes reach the
/// figure at once through the post-cut pending credits, and dirty shards are visited ahead of the
/// rotation (`dirty_share`, `extra_dirty_visit_cap`). The only thing an hour bought was that
/// `read_cache_size = total_size − F` differenced two figures of similar age, and that figure is
/// display-only.
///
/// What a day buys is capacity. [`STAGED_RANGE_BUDGET`] is linear in this window
/// (`S ≤ D · I · A · R`), and the budget is an admission gate: at the hourly window it was
/// 1,300,000 ranges, beyond which every new write-through PUT is declined.
pub const MAX_AUTHORITY_AGE_SECS: u64 = 86_400;

/// Hard ceiling on shards visited in one pass, whatever [`shards_per_pass`] derives.
///
/// # What bounds it, and it is not a round number picked for looks
///
/// A pass is bounded work (R1.5) and the shard is the unit of abortability (§ 6.4/§ 3.7), so
/// there has to be *some* ceiling. This one is where the two constraints on the fold meet —
/// § 16.6.4's "intersection of visit scheduling and parse rate", derived in full there. In
/// symbols, with `S` staged ranges, `N` shards, `I` instances, duty cycle `D`, age bound `A`,
/// per-object cost `1/R` and per-visit fixed cost `c`:
///
/// ```text
///     S/R  +  c·N   ≤   D · I · A
/// ```
///
/// Work per age-window on the left — parse every staged range once, visit every shard once —
/// against the shared-storage time the duty cycle permits on the right. **The shard count `N`
/// cancels out of the parse term**, because more shards means smaller shards, which is why the
/// interesting bound is on `N` through the *fixed* cost `c` alone.
///
/// Solved for the visits one pass may make, `N/(I·P)` where `P = A/T = 288`:
///
/// ```text
///     visits_per_pass  ≤  (D·T − S·T/(A·R)) / c
/// ```
///
/// At `D = 0.167`, `T = 300 s`, `A = 86,400 s`, [`STAGED_RANGE_BUDGET`] `= 32,800,000`, the
/// measured `c_r = 18.024 ms` (**EFS** — `c_r` is per-CONFIGURATION, see the soft-input paragraph
/// below) and the measured `R = 2,900 obj/s`:
///
/// ```text
///     (50.1 − 32,800,000 × 300 / (86,400 × 2,900)) / 0.018024
///   = (50.1 − 39.27) / 0.018024
///   = 600 visits
/// ```
///
/// **512** is that rounded down to a power of two, and 1,024 would sit above it. At 512 the
/// ceiling bounds the population at `512 × 288` = 147,456 shards = **576 buckets** on one instance.
///
/// `c_r` is measured (50 iterations of the real claim-protocol shape) rather than estimated, but it is
/// **not settled**, so there are two soft inputs rather than one. `R` was taken on an **idle** fleet
/// and is therefore optimistic. And `c_r` is per-CONFIGURATION, not per-backend: FSxN measures
/// **14.345 ms** (M5) and **7.131 ms** (`SCALE-ENV-FOLD`), 2.01× apart on one backend family. Both
/// figures also **understate** the production cost, because the probe's leading read is an `ENOENT`
/// lookup on a per-iteration filename rather than a read of an existing claim document, and it hoists
/// `create_dir_all` out of the loop where `claim_repair_shard` does one per visit. Every one of those
/// pushes the true ceiling **lower** rather than higher. `staged_fold_shard_duration_secs` remains the
/// instrument that would settle `c_r` in production, and it is the one that matters most here.
///
/// **Reaching this ceiling means the 24 h p100 bound is not achievable at that shard count on
/// one instance.** It is not silent: [`shard_budget_capped_total`] counts it and a rate-limited
/// WARN names the figures.
const SHARD_VISITS_PER_PASS_CEILING: usize = 512;

/// Shards this pass must visit to put every shard inside [`MAX_AUTHORITY_AGE_SECS`].
///
/// ```text
/// shards_per_pass = ceil(observed_shard_count / (Max_Authority_Age / interval))
/// ```
///
/// # Why this is derived rather than a constant
///
/// It was `8`. At the § 3.1 shard unit — `(bucket, L1)` with `L1` two hex digits, so 256 values
/// per bucket — a single-bucket cache has 256 shards, and 8 per pass at a 300 s interval takes
/// `(256 / 8) × 300` = **9,600 s** to visit them all: 2.7× over the bound at `I = 1`, and 26×
/// over at ten buckets. § 16.6.4's `S / (R × I × D)` model did not catch it because that model
/// is pure parse *throughput* and assumes the scheduler keeps the parser fed at duty cycle `D`.
/// A fixed 8 does not: at `S = 100,000` over 256 shards a pass parses ~3,125 ranges, 14.7 s
/// inside a 300 s pass — a 4.9% duty cycle against the model's 16.7%, and that ratio of ~3.4 is
/// exactly the gap between the model's 2,825 s and the implementation's 9,600 s.
///
/// So the number is computed from the criterion and the observed shard count, and the two cannot
/// drift apart. `staged_range_budget_visits_every_shard_inside_the_age_bound` is the executable
/// statement of that, and it fails against a fixed 8.
///
/// # It is NOT divided by the instance count, deliberately
///
/// `I` divides the shard population in the capacity arithmetic (§ 16.6.4), so a fleet could in
/// principle give each instance `ceil(N / (I × 12))`. It does not, because `I` would have to be
/// *inferred* — from the ledger's instance set, say — and an inference that reads low under-delivers
/// the criterion silently, which is the exact failure this derivation replaced. Instead every
/// instance's budget is individually sufficient and § 16.2's claims stop the fleet duplicating the
/// work. The fleet converges faster than required rather than slower, which is the safe direction.
///
/// # `observed_shard_count` is observed, not `buckets × 256`
///
/// It is the size of the pass's own selection set — shards with a ledger directory, with a
/// pending credit, or with an existing checkpoint. A shard with nothing in it does not need
/// visiting, and does not have a figure whose age could breach anything, so counting it would
/// make every small deployment do 22 pointless visits a pass. On a cache at the design point
/// every `(bucket, L1)` pair holds data, so the two converge there.
///
/// # Bounds
///
/// - **Floor of 1**, so a pass with any shard at all makes progress. Without it a one-shard
///   cache would derive `ceil(1/12) = 1` anyway; the floor matters only for the empty case,
///   where the caller has already returned.
/// - **Ceiling of [`SHARD_VISITS_PER_PASS_CEILING`]**, which is where the criterion stops being
///   achievable rather than an arbitrary cap. See that constant.
fn shards_per_pass(observed_shard_count: usize) -> usize {
    let passes_per_window = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS).max(1) as usize;
    let required = observed_shard_count.div_ceil(passes_per_window);
    if required > SHARD_VISITS_PER_PASS_CEILING {
        SHARD_BUDGET_CAPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
        log_shard_budget_capped_rate_limited(observed_shard_count, required);
        return SHARD_VISITS_PER_PASS_CEILING;
    }
    required.max(1)
}

/// Passes whose derived shard budget was clipped by [`SHARD_VISITS_PER_PASS_CEILING`].
///
/// Non-zero means this instance alone cannot revisit every shard inside § 16.6.4's
/// `Max_Authority_Age`, because the population exceeds [`shards_reachable_per_instance`]. It
/// depends on the shard population only, so adding instances does not stop it; whether the fleet
/// keeps up is `staged_fold_shards_beyond_max_age`. Published as
/// `staged_fold_shard_budget_capped_total`.
static SHARD_BUDGET_CAPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Unix seconds of the most recent capped-budget WARN.
static LAST_SHARD_BUDGET_WARN_SECS: AtomicU64 = AtomicU64::new(0);

/// At most one capped-budget WARN per 10 minutes: the condition is structural rather than
/// transient, so it does not need naming every 300 s pass.
const SHARD_BUDGET_WARN_INTERVAL_SECS: u64 = 600;

/// Declines recorded by the shard-budget ceiling, for the gauge set.
pub(crate) fn shard_budget_capped_total() -> u64 {
    SHARD_BUDGET_CAPPED_TOTAL.load(Ordering::Relaxed)
}
/// The largest observed population [`shards_per_pass`] serves without clipping: the per-pass
/// ceiling times the passes per window. Published as `staged_fold_shards_reachable_per_instance`.
fn shards_reachable_per_instance() -> u64 {
    let passes_per_window = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS).max(1);
    (SHARD_VISITS_PER_PASS_CEILING as u64).saturating_mul(passes_per_window)
}
/// The capped-budget WARN's text. A constant so the remedy it names is asserted, not just written.
///
/// "Add a proxy instance" keeps shards fresh because the pass loop treats a peer-claimed shard as
/// `Busy` without spending budget ("pick another shard", § 16.2), and an install moves the shard's
/// checkpoint to the back of the stalest-first order, so a second instance visits shards the first
/// has not reached rather than the ones it just did. It does not stop this WARN: the trigger is
/// [`shards_per_pass`] on the shard population alone, so every pass on every instance is still
/// clipped. The text says so, and names the gauge that shows whether the remedy worked.
const SHARD_BUDGET_CAPPED_WARN: &str = "Staged fold: this volume has more cache shards than one \
     instance can revisit inside the staged figure's 24 h target age. The staged figure stays \
     correct but goes stale; watch write_cache.staged_shard_authority_age_secs and \
     write_cache.staged_fold_shards_beyond_max_age. Remedy: add a proxy instance. Claims keep \
     instances on different shards, so more instances keep more shards fresh, but this WARN \
     continues while the shard count exceeds one instance's reach, because it depends on the \
     shard count alone. The remedy has worked when write_cache.staged_fold_shards_beyond_max_age \
     returns to 0. The target age is fixed and there is no configuration field for it.";

fn log_shard_budget_capped_rate_limited(observed: usize, required: usize) {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_SHARD_BUDGET_WARN_SECS.load(Ordering::Relaxed);
    if now_secs >= last + SHARD_BUDGET_WARN_INTERVAL_SECS
        && LAST_SHARD_BUDGET_WARN_SECS
            .compare_exchange(last, now_secs, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    {
        warn!(
            observed_shard_count = observed,
            shards_per_pass_required = required,
            shards_per_pass_ceiling = SHARD_VISITS_PER_PASS_CEILING,
            max_authority_age_secs = MAX_AUTHORITY_AGE_SECS,
            shards_reachable_per_instance = shards_reachable_per_instance(),
            "{}",
            SHARD_BUDGET_CAPPED_WARN
        );
    }
}

/// How stale a snapshot of the budget input may be before an admission decision re-reads it,
/// seconds.
///
/// The budget is enforced on the PUT path, and the input is a readdir over the checkpoint set
/// plus a read of the pending files — shared-storage I/O that must not happen once per
/// object. 5 s is the consolidation cycle, so the snapshot is never staler than the machinery
/// that changes it. A stale snapshot admits at most one interval's worth of ranges past the
/// budget, which is a bound overshoot rather than an unbounded one.
const BUDGET_SNAPSHOT_TTL_SECS: u64 = 5;

/// How often a fold pass may run, seconds.
///
/// Same 300 s base as the ledger compaction interval it runs beside, and deliberately a
/// `const`: § 16.6.9 records that an interval knob does not solve the scaling problem, so
/// there is no config field for it.
pub const STAGED_FOLD_INTERVAL_SECS: u64 = 300;

/// Shards this instance removed staged bytes from and has not yet re-folded, keyed by
/// `(dir_tag(cache_dir), instance_id)`. Each mark is the `now_nanos()` at which it was recorded.
///
/// # Why this exists
///
/// The fold never subtracts: a shard's figure changes only when a visit recomputes it from the
/// `.meta`s, and the periodic pass reaches any one shard once per rotation. So a DELETE, a
/// graduation or a re-PUT left its bytes in the figure until stalest-first next reached that
/// shard — ~20 minutes on the fleet, up to [`MAX_AUTHORITY_AGE_SECS`] by design. The
/// instance that performed the removal knows which shard it touched, so it records the shard here
/// and its next pass visits it: inside the budget as far as [`dirty_share`]'s slack allows, and
/// beyond the budget for the rest, up to [`extra_dirty_visit_cap`]. Only a busy shard, a retained
/// visit, or more dirty shards than the cap waits past the next pass.
///
/// # In-process only, deliberately
///
/// A mark is a map insert under a `Mutex`: nothing is written to shared storage on the request
/// path. The price is that a restart loses the set, and the rotation then covers the shard exactly
/// as it did before this existed — a lost mark costs latency, never a missed removal.
///
/// Keyed by instance as well as by volume because tests drive two instances' passes in one
/// process; in production one process is one instance.
///
/// Spec: cache-eviction-at-scale § 16.6.9, "Dirty shards first".
static DIRTY_SHARDS: Mutex<DirtyShardMarks> = Mutex::new(BTreeMap::new());

/// `(dir_tag, instance_id) -> shard -> mark nanos`. See [`DIRTY_SHARDS`].
type DirtyShardMarks = BTreeMap<(u64, String), BTreeMap<Shard, u64>>;

/// Record that this instance removed staged bytes belonging to `cache_key`'s shard.
///
/// Call it AFTER the `.meta` change that removed them is published: a later visit clears the mark
/// only if the visit began after it, and that is what makes "began after the mark" imply "saw the
/// removal". A key no shard can be derived from is ignored — its `.meta` is in no shard's figure.
pub(crate) fn mark_shard_dirty(cache_dir: &Path, instance_id: &str, cache_key: &str) {
    let Some((bucket, l1)) = shard_of(cache_key) else {
        return;
    };
    mark_dirty_shard(cache_dir, instance_id, Shard::new(bucket, l1));
}

/// [`mark_shard_dirty`] for a caller that already holds the shard: staging eviction, for the
/// evicted shards its inline re-fold could not install.
pub(crate) fn mark_dirty_shard(cache_dir: &Path, instance_id: &str, shard: Shard) {
    let now = now_nanos();
    let mut guard = DIRTY_SHARDS.lock().unwrap_or_else(|p| p.into_inner());
    let mark = guard
        .entry((dir_tag(cache_dir), instance_id.to_string()))
        .or_default()
        .entry(shard)
        .or_insert(now);
    // A second removal in the same shard needs a visit that began after IT.
    *mark = (*mark).max(now);
}

/// This instance's marks, copied out so the pass holds no lock across I/O.
fn dirty_marks(cache_dir: &Path, instance_id: &str) -> BTreeMap<Shard, u64> {
    DIRTY_SHARDS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&(dir_tag(cache_dir), instance_id.to_string()))
        .cloned()
        .unwrap_or_default()
}

/// Clear `shard`'s mark if this instance's visit of it began after the mark was recorded.
///
/// A mark recorded while the visit was already reading (`mark >= visit_started`) survives, so the
/// next pass visits again: that visit may have read the `.meta` before the removal.
fn clear_dirty_mark(cache_dir: &Path, instance_id: &str, shard: &Shard, visit_started: u64) {
    let mut guard = DIRTY_SHARDS.lock().unwrap_or_else(|p| p.into_inner());
    let key = (dir_tag(cache_dir), instance_id.to_string());
    if let Some(marks) = guard.get_mut(&key) {
        if marks.get(shard).is_some_and(|&m| m < visit_started) {
            marks.remove(shard);
        }
        if marks.is_empty() {
            guard.remove(&key);
        }
    }
}

/// Dirty shards awaiting a visit on this volume, over every instance in this process — in
/// production, this instance's. Published as `write_cache.staged_fold_dirty_shards`.
pub fn dirty_shard_count(cache_dir: &Path) -> u64 {
    let tag = dir_tag(cache_dir);
    DIRTY_SHARDS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|((t, _), _)| *t == tag)
        .map(|(_, marks)| marks.len() as u64)
        .sum()
}

/// The published staged figure (`F + post-cut pending + P`), for a test in a file that may not
/// name the gauge bundle (`the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`
/// confines it to producers and the publisher; a test is neither, but it is also not a decision
/// path, so it reads the figure through this producer-side accessor).
#[cfg(test)]
pub(crate) async fn published_figure_for_tests(cache_dir: &Path) -> Option<u64> {
    gauges_for_tests(cache_dir, 0).await.staged_fold_bytes
}
/// The gauge set as one `/metrics` collection derives it: [`read_gauge_inputs`], then
/// [`collect_staged_fold_gauges`].
#[cfg(test)]
pub(crate) async fn gauges_for_tests(cache_dir: &Path, incumbent: u64) -> StagedFoldGauges {
    let (pending, checkpoints) = read_gauge_inputs(cache_dir).await;
    collect_staged_fold_gauges(
        cache_dir,
        incumbent,
        &pending,
        &checkpoints,
        crate::signed_put_handler::in_progress_part_bytes(cache_dir),
    )
    .await
}

/// Every installed shard with its `staged_scanned_at`, for the same tests: the fold's
/// stalest-first key is `(staged_scanned_at, shard)`.
#[cfg(test)]
pub(crate) fn shard_scan_times_for_tests(cache_dir: &Path) -> Vec<(Shard, u64)> {
    read_shard_checkpoints(cache_dir)
        .iter()
        .map(|(s, c)| (s.clone(), c.staged_scanned_at))
        .collect()
}

/// How many of the pass's `budget` slots dirty shards may take ahead of stalest-first.
///
/// `ages` are every selected shard's staged-figure ages in seconds, in stalest-first order (a
/// never-folded shard is enormous). The answer is the largest `h <= min(dirty, budget)` for which
/// the schedule "`budget - h` stalest shards now, then `budget` per pass" still visits every
/// shard inside [`MAX_AUTHORITY_AGE_SECS`]: rank `i` below `budget - h` is visited now, and any
/// other rank at pass `p = 1 + (i - (budget - h)) / budget` must satisfy
/// `age + p × STAGED_FOLD_INTERVAL_SECS <= MAX_AUTHORITY_AGE_SECS`.
///
/// # Why slack and not a fixed share
///
/// One budget buys about one visit per shard per rotation (`12 × budget ≈ observed`), so a dirty
/// visit to a recently-folded shard is a visit some stale shard does not get. A fixed share (half
/// the budget, say) lets a sustained load of such visits push the stalest shard to twice the bound.
/// Admitting only what the schedule can spare keeps the bound whenever stalest-first alone keeps
/// it: `h = 0` is today's pass, and a pass admits `h` only if "`budget` per pass from here" is
/// still feasible after it, which the next pass re-checks from the state this one left.
///
/// The dirty shards are counted at their own stalest-first ranks, as if they still needed a slot
/// there. That makes the check pessimistic, never optimistic.
///
/// # At a real population it admits almost nothing, and that is why extras exist
///
/// `budget = ceil(N / 288)`, so the steady-state schedule has `288 × budget − N` slots of slack,
/// and this share is also capped at `budget`: 6 at a fleet-sized N ≈ 1,500. So this
/// share is only the in-budget arm: every dirty shard it does not admit (and that the budget's
/// stalest-first slots do not reach) is visited beyond the budget, up to
/// [`extra_dirty_visit_cap`]. See the pass loop in `run_staged_fold_scoped`.
///
/// Spec: cache-eviction-at-scale § 16.6.9, "Dirty shards first", point 2.
fn dirty_share(ages: &[u64], budget: usize, dirty: usize) -> usize {
    if budget == 0 {
        return 0;
    }
    let feasible = |stalest_now: usize| {
        ages.iter().enumerate().all(|(rank, &age)| {
            if rank < stalest_now {
                return true;
            }
            let pass = 1 + (rank - stalest_now) / budget;
            age.saturating_add((pass as u64).saturating_mul(STAGED_FOLD_INTERVAL_SECS))
                <= MAX_AUTHORITY_AGE_SECS
        })
    };
    let mut share = dirty.min(budget);
    while share > 0 && !feasible(budget - share) {
        share -= 1;
    }
    share
}

/// Dirty shards one pass may visit beyond its stalest-first budget, before the ceiling.
///
/// A fixed figure rather than a multiple of the budget. The budget is `ceil(N / 288)` at the
/// 24 h window, so a budget-scaled cap would shrink with the window and leave a burst of dirty
/// shards (deletes, graduations) counted in the figure for many passes. 128 extras per pass
/// absorbs at least as much per pass as the budget-scaled cap did at the hourly window on a
/// fleet-sized population (N ≈ 1,500: 126 extras), and a pass of `budget + 128` visits stays
/// well inside [`SHARD_VISITS_PER_PASS_CEILING`]'s cost derivation.
const DIRTY_EXTRA_VISITS_PER_PASS: usize = 128;

/// How many dirty shards one pass may visit beyond its `budget`:
/// `min(DIRTY_EXTRA_VISITS_PER_PASS, SHARD_VISITS_PER_PASS_CEILING − budget)`.
///
/// Independent of the budget (see [`DIRTY_EXTRA_VISITS_PER_PASS`]), and bounded by the ceiling so
/// `budget + extras` never exceeds [`SHARD_VISITS_PER_PASS_CEILING`]. At the ceiling it is 0 and
/// only [`dirty_share`]'s slack remains. The stalest-first budget is never reduced for an extra,
/// so the age bound holds whatever this returns.
///
/// Spec: cache-eviction-at-scale § 16.6.9, "Dirty shards first", point 2; § 16.6.12 point 1.
fn extra_dirty_visit_cap(budget: usize) -> usize {
    DIRTY_EXTRA_VISITS_PER_PASS.min(SHARD_VISITS_PER_PASS_CEILING.saturating_sub(budget))
}

/// Passes that ended in a retain rather than an install, process-wide.
///
/// Published as `staged_fold_incomplete_total`. A process-wide `AtomicU64` rather than
/// state on the consolidator because the fold's incompleteness is a property of the
/// shared volume, and because `/metrics`, the dashboard and OTLP all read the gauge set
/// from different call paths.
static STAGED_FOLD_INCOMPLETE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Outstanding `.meta` read-plus-parse operations one shard's fold keeps in flight.
///
/// # The same measured knee as the repair sweep's, because it is the same operation
///
/// Read+parse rate against outstanding requests, on `m6in.2xlarge` fleet proxies against the
/// 1,146,790-object reference tree on EFS, with concurrency decoupled from core count
/// (measured):
///
/// ```text
///     outstanding    1      4      8     16     32     64     128    256
///     obj/s        174.7  718.7 1391.8 2210.0 2696.9 3801.2 3909.1 3975.5
/// ```
///
/// **64 is the knee**: 64→128 returns 3% of linear and 128→256 returns 2%, while per-file latency
/// rises linearly and `concurrency / latency` pins at 3,843 / 3,968 / 4,118 — the signature of a
/// fixed-capacity service queue rather than of a resource being consumed. Going past it buys
/// throughput inside the measurement noise and pays for it in queueing latency, which on a shared
/// volume is latency other work is also waiting on. `ledger_repair`'s `SWEEP_READ_CONCURRENCY` is
/// derived from the same table for the same reason, and the two are deliberately equal: a single
/// proxy running a fold and a sweep concurrently then keeps at most 128 metadata reads outstanding,
/// which is one step past the knee rather than an unbounded multiple of it.
///
/// # This loop was at concurrency 1, and that is what made the ceiling unreachable
///
/// See the comment at the fold's candidate loop for the arithmetic. A serial fold's 174.7 obj/s puts
/// the parse term 12.4× over the whole authority-age window, so the reachable-bucket ceiling was
/// negative — zero buckets — and § 16.6.6 precondition 3b could never pass on any volume.
///
/// # Not configurable
///
/// § 16.6.4's no-knob reasoning: it is derived from a measurement, and an operator who set it high
/// would push the shared volume past its knee for every other consumer as well.
const STAGED_FOLD_CANDIDATE_CONCURRENCY: usize = 64;

/// Peak `.meta` reads in flight across every fold this process has run, for the gauge.
static FOLD_READS_IN_FLIGHT_PEAK: AtomicU64 = AtomicU64::new(0);

/// Live and peak outstanding `.meta` reads for **one shard visit**.
///
/// # Why the scope is one visit and not the process
///
/// The peak is the value the concurrency change is asserted on: a throughput figure would be a
/// timing assertion and therefore a flake, whereas the count of reads the fold actually held
/// outstanding is the quantity [`STAGED_FOLD_CANDIDATE_CONCURRENCY`] controls, read directly. A peak
/// of 1 on a shard with several candidates means the loop is serial whatever the constant says.
///
/// A process-wide peak could not carry that assertion. It is monotonic, tests in this module run
/// concurrently in one process, and any other test's fold would raise it — so a test asserting
/// "peak > 1" would pass whether or not the loop under test was concurrent. A per-visit gauge,
/// reported through [`FoldPassSummary`], is not reachable by another test at all.
///
/// The process-wide figure is still maintained, because it is the useful *operational* reading: an
/// operator wants to know whether this instance's folds have ever kept reads outstanding.
#[derive(Default)]
struct InFlightGauge {
    live: AtomicU64,
    peak: AtomicU64,
}

impl InFlightGauge {
    fn enter(&self) -> InFlightGuard<'_> {
        let live = self.live.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(live, Ordering::Relaxed);
        FOLD_READS_IN_FLIGHT_PEAK.fetch_max(live, Ordering::Relaxed);
        InFlightGuard { gauge: self }
    }

    fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }
}

/// RAII guard that counts one `.meta` read as outstanding for as long as it is.
struct InFlightGuard<'a> {
    gauge: &'a InFlightGauge,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.gauge.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// `staged_fold_candidate_read_inflight_peak`, per-instance since process start.
pub(crate) fn candidate_read_inflight_peak() -> u64 {
    FOLD_READS_IN_FLIGHT_PEAK.load(Ordering::Relaxed)
}

/// Upper edges of `staged_fold_shard_duration_secs`, seconds, with an implicit `+Inf` class
/// after the last.
///
/// Fixed edges rather than a quantile estimator, for the reason § 3.4 gives for the other
/// histogram in this design: fixed classes are comparable across instances and across
/// releases, and a shard visit whose duration matters is one that ran long, which the tail
/// classes name directly. The edges are chosen around § 16.2's lease derivation (`2 ×` p99,
/// floored at 900 s), so a visit landing in the last two classes is one whose lease is at risk.
pub const SHARD_DURATION_CLASS_EDGES_SECS: [u64; 7] = [1, 5, 15, 60, 300, 900, 3600];

/// Counters for [`SHARD_DURATION_CLASS_EDGES_SECS`] plus a trailing `+Inf` class.
static SHARD_DURATION_CLASSES: [AtomicU64; SHARD_DURATION_CLASS_EDGES_SECS.len() + 1] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Summed shard-visit duration, milliseconds — the histogram's `_sum`, kept in ms so a
/// sub-second visit is not summed as zero.
static SHARD_DURATION_SUM_MS: AtomicU64 = AtomicU64::new(0);

/// Admissions declined because `staged_range_count + pending_count >= STAGED_RANGE_BUDGET`.
///
/// **This is how an operator recognises the budget**, alongside the rate-limited WARN in
/// `CacheManager::staged_range_budget_refusal`. A workload that silently stops being cached is
/// the worst possible presentation of a capability reduction, so the decline is a named
/// counter and a named log line rather than an unattributed `None` from a reservation.
static STAGED_RANGE_BUDGET_DECLINES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Memoised budget input: `(staged_range_count + pending_count, taken_at_unix_secs)`.
///
/// `u64::MAX` in the count slot is the UNREADABLE sentinel. `0` in the timestamp slot means
/// "no snapshot", which is why the timestamp is checked rather than the count.
static BUDGET_SNAPSHOT_COUNT: AtomicU64 = AtomicU64::new(0);
static BUDGET_SNAPSHOT_AT_SECS: AtomicU64 = AtomicU64::new(0);

/// BLAKE3 of the `cache_dir` the snapshot was taken for.
///
/// A one-entry memo keyed by path rather than an unkeyed one. Production has exactly one
/// cache directory per process, so this never misses — but a *test* binary has one per
/// fixture, and an unkeyed memo would let one test's seeded count decide another test's
/// admission. That is the shape of a cross-test dependency that passes locally and fails
/// under a different test order, and it is cheaper to key the memo than to reason about it.
static BUDGET_SNAPSHOT_DIR: AtomicU64 = AtomicU64::new(0);

/// This process's count-driven eviction state, **keyed by cache directory**.
///
/// One struct under one lock rather than one atomic per field, because the fields are only
/// meaningful together and are all reset by the same event. Keyed by `cache_dir` for the same
/// reason [`BUDGET_SNAPSHOT_DIR`] is: production has one cache directory per process so the key
/// never misses, but a *test* binary has one per fixture and libtest runs tests as parallel
/// threads in one process — an unkeyed cumulative counter would let one fixture's passes decide
/// another fixture's published reading. That is the process-global-counter trap this repo has
/// recorded, and it fired here on the first run.
#[derive(Default, Clone, Copy)]
struct CountEvictionState {
    /// Ranges reclaimed by count-driven eviction since the last fold install.
    ///
    /// # Without this the count trigger over-evicts, and the reason is the count's own latency
    ///
    /// The count comes from the per-shard checkpoints, which move only when a fold installs. The
    /// consolidation cycle evaluates the trigger every `consolidation_interval` (5 s by
    /// default), so between two installs the same stale count would be observed repeatedly and
    /// each evaluation would ask for `count − target` ranges again. That is a drain of the whole
    /// write tier at a rate set by the eviction candidate cap rather than by the overshoot, and
    /// it is a worse defect than the wedge the count trigger exists to fix.
    ///
    /// So a count-driven pass debits what it provably reclaimed, and the trigger compares
    /// `observed − reclaimed_since_fold`. Two properties make that sound rather than a guess:
    ///
    /// - It is **measured, not estimated** — the figure is the summed `staged_range_count` of
    ///   the `.meta`s this pass actually evicted, from the same assessment the eviction acted
    ///   on.
    /// - It is reset by exactly the event that makes it redundant, a fold install, at the same
    ///   site as [`invalidate_budget_snapshot`]. It can therefore never double-count a reclaim
    ///   the installed figure already reflects.
    ///
    /// The direction is deliberate and it is the safe one. Subtracting **under**-states the
    /// count, so the count trigger fires late; admission's [`budget_verdict`] is untouched and
    /// still reads the raw figure, so the decline backstop stays conservative. Eviction erring
    /// late and admission erring early is the pair that cannot over-delete.
    ///
    /// # Peers see it through a document, not through this field
    ///
    /// The checkpoint figure is shared, so a reclaim here over-states the count for every
    /// instance on the volume, not only this one. [`record_count_eviction_pass`] therefore also
    /// publishes this figure as this instance's [`ReclaimDocument`], and
    /// [`count_eviction_verdict`] subtracts the peers' documents as well as this field.
    reclaimed_since_fold: u64,
    /// This instance's fold installs against this directory, counting from zero at process
    /// start. The generation a published [`ReclaimDocument`] counts against; bumped by the same
    /// install that zeroes `reclaimed_since_fold`.
    generation: u64,
    /// Set when a count-driven pass reclaimed **nothing**, cleared by the next fold install.
    ///
    /// # Without this the count trigger takes the fleet eviction lock every consolidation cycle
    ///
    /// A tier whose reclaimable population is empty — every candidate fresh-unread and already
    /// walked, or the ledger empty — stays over the count trigger indefinitely, because the
    /// count only falls when a fold installs or a reclaim debits it. The consolidation cycle
    /// evaluates the trigger every 5 s by default, so without a latch each cycle would acquire
    /// `global_eviction.lock`, re-read the ledger, walk the candidates, free nothing, and
    /// release — starving read-tier eviction of the same lock while doing shared-storage I/O on
    /// a 5 s loop.
    ///
    /// A pass that reclaimed nothing has established that the population is empty, which is
    /// exactly the decline arm's correct firing condition. Re-establishing it cannot yield a
    /// different answer until the inputs change, and the inputs change at a fold install — the
    /// same event that clears the reclaim debit, for the same reason.
    ///
    /// **Not** set by a pass that reclaimed something, however little: partial progress means
    /// the population was non-empty, so the next cycle has real work to do.
    found_nothing: bool,
    /// Count-driven passes completed against this directory, and the ranges they reclaimed.
    ///
    /// Published in the count pass's result document, because they are what separates "nothing
    /// was reclaimable" from "reclaimed and immediately re-filled" — see
    /// [`crate::eviction_result::WriteCacheCountPassResult`], which states the reading. Not
    /// reset by a fold install: they are cumulative for the process's life against this volume,
    /// which is what makes "this is not the first reclaim" readable.
    passes_total: u64,
    ranges_freed_total: u64,
}

/// One [`CountEvictionState`] per cache directory tag.
///
/// A map rather than one slot reset on a directory change: production has one entry, but a test
/// binary runs fixtures as parallel threads, and a single slot let one fixture's first read wipe
/// another's debit or latch mid-test.
static COUNT_EVICTION_STATE: Mutex<BTreeMap<u64, CountEvictionState>> = Mutex::new(BTreeMap::new());

/// The count-eviction state for `cache_dir`'s tag, fresh if none has been recorded.
fn count_state_for(tag: u64) -> CountEvictionState {
    match COUNT_EVICTION_STATE.lock() {
        Ok(guard) => guard.get(&tag).copied().unwrap_or_default(),
        // A poisoned lock here must not disable the count trigger, so report a fresh state:
        // "nothing reclaimed, not latched" is the fail-open reading.
        Err(_) => CountEvictionState::default(),
    }
}

/// TTL of the memoised [`staged_authority_figure`], seconds.
///
/// # Why 5 s, and why the same number as [`BUDGET_SNAPSHOT_TTL_SECS`]
///
/// The memo's inputs change at exactly two events: a **fold install**, which is when a shard
/// checkpoint's `staged_byte_sum` moves, and a **credit or an MPU part write**, which is when a
/// `.pending` record or a `part{N}.json` appears. The install is handled exactly rather than by
/// expiry — [`invalidate_authority_snapshot`] runs at the same site as the budget's, so a fold
/// that has just changed the figure is never hidden by a stale memo. So the TTL only bounds how
/// long a *credit* can be invisible, and the right scale for that is the consolidation cycle
/// (5 s on the fleet), which is the interval at which everything else about staged state is
/// re-derived. There is no benefit in being fresher than the pipeline that feeds it, and the
/// fold's own cadence — [`STAGED_FOLD_INTERVAL_SECS`], 300 s — is 60× longer, so a TTL sized off
/// the fold rather than the cycle would let the figure lag by a whole fold interval for no
/// saving that matters.
///
/// # CORRECTED — the direction is the UNSAFE one, in both of the memo's error cases
///
/// This comment used to say "the direction of the resulting error is the safe one … a stale figure
/// under-states by at most one interval's credits, so the staging trigger fires at most 5 s late",
/// and cited § 16.6.9's residual error class as already admitting a larger lag. **That was wrong,
/// and it was internally contradictory**: it named late firing and called it safe, when § 16.6.2
/// rules late firing out by name. It is the direction that fills a volume; early firing is merely
/// wasteful. Both of the memo's error cases are on the late side:
///
/// - **A credit made inside the TTL is invisible**, so the figure **under**-states by up to one
///   interval's credits. That is not a new instance of § 16.6.9's residual class — that class is
///   the *shrink* lag, which is an over-count and genuinely safe. This has the opposite sign, and it
///   erodes the *other* half of § 16.6.9, the one that says "growth is visible immediately, not
///   within one fold". The citation was to the wrong half.
/// - **After a staged input becomes unreadable**, the memo keeps serving the last complete fold
///   figure for up to one TTL where the un-memoised path would return `None` and
///   `CacheManager::staged_authority` would fall back to the incumbent counter. The incumbent's
///   measured failure is a directional **over**-count of 4.28x-11.17x, so the memo serves the
///   **lower** of the two figures — less eviction pressure, later trigger. The fallback's own doc
///   comment says in as many words that falling back is safe *because* the incumbent over-states,
///   so serving the fold figure instead is the reverse of that reasoning, not an instance of it.
///
/// # 5 s is acceptable anyway, and this is why — the reasoning is not the obvious one
///
/// Not because the direction is safe (it is not), and not by appeal to a residual error class of a
/// different sign. Because of what consumes the figure:
///
/// - **The staging eviction trigger is evaluated on the consolidation cycle, which is the same 5 s.**
///   A decision that can only be taken once per cycle cannot be delayed by less than a cycle by
///   anything, so a TTL matched to the cycle adds at most one quantum to a decision already
///   quantised to it. Sizing the TTL off the fold's 300 s interval instead — the other obvious
///   choice — would add 60 quanta, and that would not be acceptable.
/// - **Admission is NOT cycle-quantised, and this is the real exposure, stated rather than elided.**
///   The staging bound is checked per request on the write path, so a 5 s-stale figure can admit up
///   to one interval's worth of write-through beyond the bound. That is a genuine widening. It is
///   bounded by one consolidation cycle of write volume, it is the same shape of over-admission the
///   pre-memo code already had for shrink between folds, and it is what the memo is paid for: the
///   un-memoised derivation put a checkpoint readdir, a locked read of every `.pending` and an
///   `mpus_in_progress/` traversal on the write path itself.
///
/// If either of those two properties changes — an eviction trigger evaluated more often than every
/// 5 s, or a staging bound that must hold exactly per request rather than approximately — this TTL
/// has to be revisited, and it is the second bullet that would force it.
///
/// **This is not a config field on purpose.** § 16.6.9 records that an interval knob does not
/// solve the scaling problem, and the same reasoning applies here: a knob would let an operator
/// trade accounting freshness for I/O without the arithmetic above ever being done.
pub(crate) const AUTHORITY_SNAPSHOT_TTL_SECS: u64 = 5;

/// Memoised [`staged_authority_figure`], keyed by cache directory.
///
/// **Why this exists.** Every `get_cache_size_stats` and every staging eviction check resolved
/// the figure from scratch — a readdir over the checkpoint set, a locked read of every
/// `.pending`, and an `mpus_in_progress/` traversal, all on shared storage. `get_cache_size_stats`
/// is on the dashboard refresh and the `/metrics` scrape path, and the eviction check is on the
/// write path, so the un-memoised version put three shared-volume I/O patterns on two of the
/// hottest paths in the process.
///
/// # A locked map, not a set of atomics
///
/// The budget memo next door is three loose `AtomicU64`s and is correct because it holds one
/// scalar. This holds **two** terms that must agree with each other, and two concurrent
/// recomputes storing field-by-field could publish `resident_staged_bytes` from one derivation
/// beside `in_progress_part_bytes` from another — a figure neither read ever saw. Both call sites
/// can run concurrently, so that is reachable rather than theoretical. A single lock publishes the
/// pair atomically, and is held only around the map access, never across the I/O.
///
/// # Keyed by directory
///
/// Production has one cache directory per process, so the map holds one entry forever. A *test*
/// binary has one per fixture, and a single-slot memo would let one test's figure decide another
/// test's reading — the same reason [`BUDGET_SNAPSHOT_DIR`] is keyed, and worse here, because two
/// fixtures thrashing one slot produce order-dependent results rather than merely stale ones.
/// [`AUTHORITY_SNAPSHOT_MAX_KEYS`] bounds it so a pathological caller cannot grow it.
///
/// `None` in the value slot is the UNREADABLE case, cached deliberately: re-reading on every call
/// in exactly the state where the volume is least healthy is the opposite of what the memo is for,
/// and a cached `0` would be the sentinel confusion § 16.6.3 forbids.
static AUTHORITY_SNAPSHOT: Mutex<BTreeMap<u64, AuthoritySnapshot>> = Mutex::new(BTreeMap::new());

/// Entries [`AUTHORITY_SNAPSHOT`] may hold before it is cleared. Production needs one.
const AUTHORITY_SNAPSHOT_MAX_KEYS: usize = 64;

/// One memoised derivation: when it was taken, and what it found.
#[derive(Debug, Clone, Copy)]
struct AuthoritySnapshot {
    /// No input of `figure` is older than this. A deciding reader accepts the memo while it is
    /// younger than [`AUTHORITY_SNAPSHOT_TTL_SECS`].
    taken_at_secs: u64,
    /// When `F` and the pending term were read. Equal to `taken_at_secs` for a deciding reader's
    /// derivation; later for a scrape's refresh, whose `P` is the last walk this process kept
    /// (see [`rederive_authority_snapshot_from_kept_walk`]). A scrape accepts the memo while
    /// this is younger than the TTL, which is what limits scrape refreshes to one per TTL.
    derived_at_secs: u64,
    /// `None` is UNREADABLE, never a zero.
    figure: Option<StagedAuthorityFigure>,
}

/// Calls served from the memo, and calls that re-derived it, since process start.
///
/// **This is the pair that makes a stale read distinguishable from a fresh one**, which a single
/// age gauge cannot do: an age of 3 s is consistent both with a memo that has been serving for
/// 3 s and with a figure derived 3 s ago and re-derived on every call since. Published as
/// `staged_authority_snapshot_hits_total` and `staged_authority_snapshot_recomputes_total`, and
/// read as a ratio — a hit rate near zero means the memo is not doing its job and the
/// shared-volume reads it exists to remove are still being paid.
static AUTHORITY_SNAPSHOT_HITS: AtomicU64 = AtomicU64::new(0);
static AUTHORITY_SNAPSHOT_RECOMPUTES: AtomicU64 = AtomicU64::new(0);

/// Expire the memoised authority figure for `cache_dir` without dropping it.
///
/// Every reader that decides on the figure re-derives it next time, exactly as if the entry had
/// been removed. A scrape instead serves the expired figure once while a background refresh runs
/// (see [`as_scrape`]); removing the entry would make the first scrape after every fold install
/// re-derive inline, which is the wait `as_scrape` exists to remove.
fn expire_authority_snapshot(cache_dir: &Path) {
    if let Ok(mut map) = AUTHORITY_SNAPSHOT.lock() {
        if let Some(snapshot) = map.get_mut(&dir_tag(cache_dir)) {
            snapshot.taken_at_secs = 0;
            snapshot.derived_at_secs = 0;
        }
    }
}

/// Drop the memoised authority figure for `cache_dir`, so the next read re-derives it.
#[cfg(test)]
fn invalidate_authority_snapshot(cache_dir: &Path) {
    if let Ok(mut map) = AUTHORITY_SNAPSHOT.lock() {
        map.remove(&dir_tag(cache_dir));
    }
}

/// How many times [`read_shard_checkpoints`] has traversed each cache directory, in a test binary.
///
/// **Keyed by directory, for the same reason [`AUTHORITY_SNAPSHOT`] is**: tests in one binary run
/// concurrently and share every static, so a single counter would make one test's traversals
/// decide another test's assertion. Each test uses its own `TempDir`, so a per-directory tally is
/// race-free without serialising anything.
///
/// Exists so that "the scrape path reads the checkpoint set **once**" is an assertion rather than a
/// claim. It is the only thing that can distinguish one traversal from two: both produce identical
/// gauge values, so no published figure can tell them apart.
#[cfg(test)]
static SHARD_CHECKPOINT_READS: Mutex<BTreeMap<u64, u64>> = Mutex::new(BTreeMap::new());

/// Tally one traversal of `cache_dir`'s checkpoint set.
#[cfg(test)]
fn note_shard_checkpoint_read(cache_dir: &Path) {
    if let Ok(mut map) = SHARD_CHECKPOINT_READS.lock() {
        *map.entry(dir_tag(cache_dir)).or_insert(0) += 1;
    }
}

/// Traversals of `cache_dir`'s checkpoint set so far in this test binary.
#[cfg(test)]
pub(crate) fn shard_checkpoint_reads_for_tests(cache_dir: &Path) -> u64 {
    SHARD_CHECKPOINT_READS
        .lock()
        .ok()
        .and_then(|m| m.get(&dir_tag(cache_dir)).copied())
        .unwrap_or(0)
}

/// [`invalidate_authority_snapshot`], reachable from another module's tests.
///
/// Same reason as [`invalidate_budget_snapshot_for_tests`]: a test that changes staged state and
/// then reads the authority must clear the memo, or it asserts against the figure from before its
/// own fixture.
#[cfg(test)]
pub(crate) fn invalidate_authority_snapshot_for_tests(cache_dir: &Path) {
    invalidate_authority_snapshot(cache_dir);
}

/// Move `cache_dir`'s memoised authority figure `secs` into the past, so it is present but
/// expired. `false` when there is no memo to age.
///
/// Distinct from [`invalidate_authority_snapshot_for_tests`]: an absent memo and an expired one are
/// different states for a reader that may serve a stale figure rather than re-derive it.
#[cfg(test)]
pub(crate) fn age_authority_snapshot_for_tests(cache_dir: &Path, secs: u64) -> bool {
    let mut map = AUTHORITY_SNAPSHOT.lock().unwrap_or_else(|p| p.into_inner());
    match map.get_mut(&dir_tag(cache_dir)) {
        Some(snapshot) => {
            snapshot.taken_at_secs = snapshot.taken_at_secs.saturating_sub(secs);
            snapshot.derived_at_secs = snapshot.derived_at_secs.saturating_sub(secs);
            true
        }
        None => false,
    }
}

/// The memoised authority read, for another module's tests that must prime the memo or show a
/// derivation blocks. A separate name so those tests do not count as a second consumer of the
/// authority in `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`.
#[cfg(test)]
pub(crate) async fn authority_figure_for_tests(cache_dir: &Path) -> Option<StagedAuthorityFigure> {
    staged_authority_figure(cache_dir).await
}

/// The `.pending` file and its lock file for `instance_id` under `cache_dir`, for tests that
/// must hold the lock a reader takes.
#[cfg(test)]
pub(crate) fn pending_paths_for_tests(cache_dir: &Path, instance_id: &str) -> (PathBuf, PathBuf) {
    let store = PendingStore::new(cache_dir.to_path_buf());
    let path = store.path_for(instance_id);
    let lock = PendingStore::lock_path_for(&path);
    (path, lock)
}

/// Age of `cache_dir`'s memoised authority figure in seconds, or `None` when there is no live
/// memo.
///
/// `None` covers both "never taken" and "expired", which are the two states in which the next
/// read pays full price.
pub(crate) fn authority_snapshot_age_secs(cache_dir: &Path) -> Option<u64> {
    let now_secs = unix_secs();
    let map = AUTHORITY_SNAPSHOT.lock().ok()?;
    let snapshot = map.get(&dir_tag(cache_dir))?;
    let age = now_secs.saturating_sub(snapshot.taken_at_secs);
    (age < AUTHORITY_SNAPSHOT_TTL_SECS).then_some(age)
}

/// `(staged_authority_snapshot_hits_total, staged_authority_snapshot_recomputes_total)`.
pub(crate) fn authority_snapshot_counters() -> (u64, u64) {
    (
        AUTHORITY_SNAPSHOT_HITS.load(Ordering::Relaxed),
        AUTHORITY_SNAPSHOT_RECOMPUTES.load(Ordering::Relaxed),
    )
}

/// Seconds since the Unix epoch, or 0 if the clock reads before it.
fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A stable 64-bit tag for a cache directory.
fn dir_tag(cache_dir: &Path) -> u64 {
    let hash = blake3::hash(cache_dir.to_string_lossy().as_bytes());
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap_or([0u8; 8]))
}

/// Record one shard visit's duration into the histogram.
fn record_shard_duration(elapsed: std::time::Duration) {
    SHARD_DURATION_SUM_MS.fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);
    let secs = elapsed.as_secs();
    let class = SHARD_DURATION_CLASS_EDGES_SECS
        .iter()
        .position(|edge| secs <= *edge)
        .unwrap_or(SHARD_DURATION_CLASS_EDGES_SECS.len());
    SHARD_DURATION_CLASSES[class].fetch_add(1, Ordering::Relaxed);
}

/// Drop the memoised budget input, so the next admission re-reads it.
///
/// Also clears [`CountEvictionState`]'s `reclaimed_since_fold` and `found_nothing`: both are
/// derived from the same input, and a reclaim debit that outlived the install reflecting it would
/// be subtracted twice. The cumulative `passes_total` / `ranges_freed_total` are deliberately
/// NOT cleared — they are what make "this is not the first reclaim" readable.
///
/// Test-only: production installs go through [`invalidate_budget_snapshot_retiring`], which
/// also retires the installing instance's [`ReclaimDocument`].
#[cfg(test)]
fn invalidate_budget_snapshot(cache_dir: impl AsRef<Path>) {
    invalidate_budget_snapshot_retiring(cache_dir.as_ref(), None);
}

/// [`invalidate_budget_snapshot`], and at a fold install also retire this instance's
/// [`ReclaimDocument`] under the same lock.
///
/// Under the same lock because [`record_count_eviction_pass`] writes the document under it: a
/// reclaim recorded between zeroing the field and removing the file would otherwise be lost to
/// peers while still debited here. A removal that fails leaves a document the install has
/// superseded; peers keep debiting it until [`RECLAIM_DOCUMENT_MAX_AGE_SECS`] or this
/// instance's next reclaim rewrites it, which is the late direction.
///
/// Keyed by `cache_dir`, like the state: an install against one directory has not re-derived
/// another's figure. Production has one directory; parallel test fixtures have many.
fn invalidate_budget_snapshot_retiring(cache_dir: &Path, own_instance: Option<&str>) {
    BUDGET_SNAPSHOT_AT_SECS.store(0, Ordering::Relaxed);
    if let Ok(mut states) = COUNT_EVICTION_STATE.lock() {
        if let Some(state) = states.get_mut(&dir_tag(cache_dir)) {
            state.reclaimed_since_fold = 0;
            state.found_nothing = false;
            state.generation = state.generation.saturating_add(1);
        }
        if let Some(instance_id) = own_instance {
            let path = ReclaimDocument::path(cache_dir, instance_id);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => debug!(
                    "Staged fold: could not retire reclaim document {:?} ({}); peers debit it \
                     until it ages out",
                    path, e
                ),
            }
        }
    }
}

/// One instance's count-eviction reclaims since its last fold install, on the shared volume.
///
/// # Why this exists
///
/// The count trigger reads the shard checkpoints, which move only when a fold installs. Between
/// installs, an instance that evicted staged ranges debits them from its own verdict
/// ([`CountEvictionState`]'s `reclaimed_since_fold`) so it does not re-ask for them. That debit
/// was process-local, and the checkpoint figure is not: a peer's reclaim over-stated the count
/// for every other instance, which could then evict again for ranges already gone. Each
/// instance now publishes its debit here, and [`count_eviction_verdict`] subtracts every
/// peer's document as well as its own field. O(instances) small reads per verdict.
///
/// # Lifecycle
///
/// - Written by [`record_count_eviction_pass`] whenever the debit changes, atomically
///   (instance-suffixed temp, `sync_all`, rename), carrying the cumulative debit since this
///   instance's last install, the install `generation` it counts against, and the time of the
///   latest reclaim in it.
/// - Removed by this instance's own fold install, the event that zeroes the debit.
/// - Ignored by readers once older than [`RECLAIM_DOCUMENT_MAX_AGE_SECS`], and removed by a
///   reader once twice that old. This is what retires a stopped instance's document: it never
///   installs again, so nothing else would.
/// - A volume with no `eviction_reclaims/` directory, or a document that does not parse or
///   carries another schema, contributes nothing, which is the behaviour before documents
///   existed. Binaries older than this one do not write documents; epoch 1 already forbids
///   running them on one volume with this one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReclaimDocument {
    schema: u32,
    instance_id: String,
    /// The owner's install generation this debit counts against.
    generation: u64,
    /// Ranges reclaimed by count-driven eviction since that install.
    ranges_reclaimed: u64,
    /// Unix nanoseconds of the latest reclaim included, so the age bound errs late.
    recorded_at_unix_nanos: u64,
}

impl ReclaimDocument {
    fn dir(cache_dir: &Path) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(SHARD_STATE_DIR)
            .join(EVICTION_RECLAIMS_DIR)
    }

    fn path(cache_dir: &Path, instance_id: &str) -> PathBuf {
        Self::dir(cache_dir).join(format!("{}.json", instance_id))
    }

    fn install(&self, cache_dir: &Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir, &self.instance_id);
        std::fs::create_dir_all(Self::dir(cache_dir))?;
        let body = serde_json::to_string(self)
            .map_err(|e| std::io::Error::other(format!("serialize reclaim document: {e}")))?;
        let tmp = path.with_extension(format!(
            "json.tmp.{}.{}",
            hostname_for_tmp(),
            std::process::id()
        ));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }
}

/// Σ `ranges_reclaimed` over peers' live [`ReclaimDocument`]s, excluding `instance_id`'s own.
///
/// Own is excluded because [`CountEvictionState`] already carries it, and a write failure must
/// not lose the local debit. Missing directory, unreadable file and foreign schema all read as
/// zero. A document past twice [`RECLAIM_DOCUMENT_MAX_AGE_SECS`] is removed, but only if its
/// mtime is that old too, so a peer that has just rewritten it keeps it.
async fn peer_reclaims_ranges(cache_dir: &Path, instance_id: &str) -> u64 {
    let Ok(mut entries) = tokio::fs::read_dir(ReclaimDocument::dir(cache_dir)).await else {
        return 0;
    };
    let now = now_nanos();
    let max_age_nanos = RECLAIM_DOCUMENT_MAX_AGE_SECS.saturating_mul(1_000_000_000);
    let own_name = format!("{}.json", instance_id);
    let mut total: u64 = 0;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        // Temps are `{instance}.json.tmp.{host}.{pid}`, so `.json` as the suffix excludes them.
        if !name.ends_with(".json") || name == own_name {
            continue;
        }
        let Ok(body) = tokio::fs::read_to_string(entry.path()).await else {
            continue;
        };
        let Ok(doc) = serde_json::from_str::<ReclaimDocument>(&body) else {
            continue;
        };
        if doc.schema != RECLAIM_DOCUMENT_SCHEMA || doc.instance_id == instance_id {
            continue;
        }
        let age = now.saturating_sub(doc.recorded_at_unix_nanos);
        if age <= max_age_nanos {
            total = total.saturating_add(doc.ranges_reclaimed);
            continue;
        }
        if age > max_age_nanos.saturating_mul(2) {
            let stale_on_disk = std::fs::metadata(entry.path())
                .and_then(|m| m.modified())
                .ok()
                .and_then(|m| SystemTime::now().duration_since(m).ok())
                .is_some_and(|d| d.as_secs() > RECLAIM_DOCUMENT_MAX_AGE_SECS.saturating_mul(2));
            if stale_on_disk {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    total
}

/// Install a synthetic shard figure, for another module's tests.
///
/// **This exists because of `no_decision_path_reads_the_fold`, and that is the right outcome.**
/// `cache.rs` cannot spell `staged_byte_sum` even inside a test module — the guard is a
/// text search over `src/`, deliberately, so it cannot be talked out of. So a fixture that
/// needs to state a staged figure lives with the producer and is called by name from
/// elsewhere. The alternative was to teach the guard about `#[cfg(test)]` blocks, which
/// would have weakened a check whose whole value is that it is crude.
///
/// The byte figure and the range count are separate parameters on purpose: the budget gates on
/// the count, and a fixture that sets them equal makes any test of that distinction vacuous.
#[cfg(test)]
pub(crate) fn seed_shard_figure_for_tests(
    cache_dir: &Path,
    bucket: &str,
    l1: &str,
    bytes: u64,
    ranges: u64,
) {
    let shard = Shard::new(bucket, l1);
    let mut c = StagedShardCheckpoint::empty(&shard);
    c.staged_byte_sum = bytes;
    c.staged_range_count = ranges;
    c.staged_scanned_at = now_nanos();
    c.staged_scanned_by = "host:seed".to_string();
    c.install(cache_dir)
        .expect("test fixture must be able to install a shard checkpoint");
}

/// Install a folded checkpoint for `(bucket, l1)` whose `staged_fold_cut` names each
/// `(instance, cut)` pair, for another module's tests.
#[cfg(test)]
pub(crate) fn seed_shard_cuts_for_tests(
    cache_dir: &Path,
    bucket: &str,
    l1: &str,
    cuts: &[(&str, u64)],
) {
    let shard = Shard::new(bucket, l1);
    let mut c = StagedShardCheckpoint::empty(&shard);
    c.staged_scanned_at = now_nanos();
    c.staged_scanned_by = "host:seed".to_string();
    for (instance, cut) in cuts {
        c.staged_fold_cut.insert(instance.to_string(), *cut);
    }
    c.install(cache_dir)
        .expect("test fixture must be able to install a shard checkpoint");
}
/// Install an unfolded checkpoint for `(bucket, l1)`, for another module's tests.
///
/// Same reason as [`seed_shard_figure_for_tests`]: `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`
/// forbids `journal_consolidator.rs` from naming the checkpoint type, even in a test fixture.
/// Unfolded rather than fresh, so the seeded rotation is NOT complete and a consolidator test
/// that seeds it does not activate the epoch as a side effect.
#[cfg(test)]
pub(crate) fn seed_unfolded_checkpoint_for_tests(cache_dir: &Path, bucket: &str, l1: &str) {
    StagedShardCheckpoint::unfolded_for(&Shard::new(bucket, l1))
        .install(cache_dir)
        .expect("test fixture must be able to install a shard checkpoint");
}

/// Like [`seed_unfolded_checkpoint_for_tests`], but stamped as folded just now, so the shard
/// counts toward a complete rotation. For another module's tests that need an activation-ready
/// volume without naming the checkpoint type.
#[cfg(test)]
pub(crate) fn seed_freshly_folded_checkpoint_for_tests(cache_dir: &Path, bucket: &str, l1: &str) {
    let mut checkpoint = StagedShardCheckpoint::unfolded_for(&Shard::new(bucket, l1));
    checkpoint.staged_scanned_at = now_nanos();
    checkpoint
        .install(cache_dir)
        .expect("test fixture must be able to install a shard checkpoint");
}

/// [`invalidate_budget_snapshot`], reachable from another module's tests.
///
/// A test that changes the count and then asserts on a verdict has to clear the memo, or it
/// asserts against the figure from before its own fixture — a test that passes for the wrong
/// reason. Production has no reason to call this; the fold calls the private one directly.
#[cfg(test)]
pub(crate) fn invalidate_budget_snapshot_for_tests(cache_dir: impl AsRef<Path>) {
    invalidate_budget_snapshot(cache_dir);
}

/// What the budget says about admitting a new staged range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetVerdict {
    /// Below the budget, or the input could not be read. **Unreadable admits**, per R7.5's
    /// fail-open rule: an unreadable figure must never silently stop write-through caching,
    /// which is the outage this spec exists to fix.
    Admit,
    /// `staged_range_count + pending_count >= STAGED_RANGE_BUDGET`. The caller declines
    /// **caching**, never the upload.
    Decline {
        /// The observed count that crossed the budget.
        count: u64,
    },
}

/// Whether a new staged range may be admitted, per § 16.6.4's count budget.
///
/// # Why this returns a verdict rather than the figure
///
/// The staged fold's byte figures are published as gauges and read by nothing that decides
/// anything, and `no_decision_path_reads_the_fold` asserts that mechanically. The **count** is
/// different: § 16.6.4 makes it an admission input by design. So the count reaches admission
/// through this one narrow function, which returns a verdict and never a byte figure — the
/// gauge names stay confined to the producer and the publisher, and a reviewer can see the
/// whole of the fold's decision surface by grepping for this one symbol.
///
/// # Cost
///
/// Memoised for [`BUDGET_SNAPSHOT_TTL_SECS`], because the input is a readdir over the
/// checkpoint set plus a locked read of every `.pending` file and this is called once per
/// candidate write-through object. The memo is invalidated by every fold install, so it never
/// hides a figure the fold has just changed.
pub(crate) async fn budget_verdict(cache_dir: &Path) -> BudgetVerdict {
    let count = budget_snapshot_count(cache_dir).await;
    if count == u64::MAX {
        return BudgetVerdict::Admit;
    }
    if count >= STAGED_RANGE_BUDGET {
        STAGED_RANGE_BUDGET_DECLINES_TOTAL.fetch_add(1, Ordering::Relaxed);
        BudgetVerdict::Decline { count }
    } else {
        BudgetVerdict::Admit
    }
}

/// The memoised staged range count, with `u64::MAX` as the UNREADABLE sentinel.
///
/// Extracted from [`budget_verdict`] by § 16.6.12 point 5 so the count trigger reads the
/// **identical** figure rather than a second snapshot of the same inputs. Two snapshots taken a
/// moment apart would let admission and eviction disagree about the count, which is a milder
/// form of the very defect that unit fixes; and the input is a readdir plus a locked read of
/// every `.pending` file, so it is worth reading once.
async fn budget_snapshot_count(cache_dir: &Path) -> u64 {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let tag = dir_tag(cache_dir);
    let taken_at = BUDGET_SNAPSHOT_AT_SECS.load(Ordering::Relaxed);
    let fresh_enough = taken_at != 0
        && now_secs.saturating_sub(taken_at) < BUDGET_SNAPSHOT_TTL_SECS
        && BUDGET_SNAPSHOT_DIR.load(Ordering::Relaxed) == tag;
    if fresh_enough {
        return BUDGET_SNAPSHOT_COUNT.load(Ordering::Relaxed);
    }
    // Single-flight. Before this, k concurrent callers in one cold window each ran the
    // whole derivation and serialised against each other on the same `.pending` flocks.
    let mut rx = budget_derivation(cache_dir);
    // `Err` is the sender dropped without a value, i.e. the derivation task panicked or the
    // runtime is shutting down; the inner `None` cannot occur past `wait_for`. Both collapse to
    // the UNREADABLE sentinel, which `budget_verdict` fails open on — the same direction as an
    // unreadable input and the only safe one for an admission gate.
    let joined = rx.wait_for(|v| v.is_some()).await.ok().and_then(|v| *v);
    joined.unwrap_or(u64::MAX)
}

/// The in-flight budget-count derivation per cache directory, single-flight.
///
/// `Some(count)` once the derivation has finished, with `u64::MAX` as the UNREADABLE sentinel;
/// the entry is removed from the map as soon as the memo is installed, so the next cold window
/// starts a new derivation.
type BudgetDerivation = tokio::sync::watch::Receiver<Option<u64>>;
static BUDGET_DERIVATIONS: Mutex<BTreeMap<u64, BudgetDerivation>> = Mutex::new(BTreeMap::new());

/// Join `cache_dir`'s budget-count derivation, starting it if none is running.
///
/// # Why this is the `Absent` path's mechanism rather than a new one
///
/// `cache::first_budget_derivation` already single-flights the *verdict* derivation this way —
/// a `tokio::sync::watch` per cache directory, held in a map, joined by every later caller and
/// removed by the derivation itself. This is the identical shape one layer down, and it is a
/// second *instance* rather than a second *mechanism* for two reasons that rule out sharing the
/// static itself:
///
/// * The value types differ. `FIRST_BUDGET_DERIVATIONS` carries a verdict
///   (`Option<Option<&'static str>>`) and this carries the count the verdict is derived from.
///   The count is also the figure [`count_eviction_verdict`] reads, which is the whole point of
///   § 16.6.12 point 5 — admission and eviction must see one figure, not two snapshots.
/// * The layering forbids it. `cache.rs` depends on `staged_fold`, not the reverse, and
///   `count_eviction_verdict` reaches [`budget_snapshot_count`] **without** passing through
///   `cache.rs` at all. A single-flight hosted up there would leave that caller unprotected.
///
/// # What a second caller receives
///
/// The **first** derivation's count, from the same `watch` — not a second derivation, and not a
/// stale memo. Its I/O is one shard-checkpoint traversal and one locked read per `.pending`
/// file for the whole cohort instead of k of each.
///
/// # And what an abandoned caller leaves behind
///
/// The derivation runs on a `tokio::spawn`ed task, so a caller that stops waiting — the
/// `FRESH_BUDGET_REDERIVE_WAIT` timeout in `cache::budget_refusal_off_the_request_path` — drops
/// only its receiver. **That is what makes the earlier comment true.** Before this change the
/// derivation was awaited inline inside that `tokio::time::timeout`, so expiry *dropped the
/// future* and the derivation was cancelled at its next await point with nothing installed: the
/// I/O already done was wasted and the next caller paid for it again. Now the task outlives the
/// timeout and installs its count.
fn budget_derivation(cache_dir: &Path) -> BudgetDerivation {
    let tag = dir_tag(cache_dir);
    let mut guard = BUDGET_DERIVATIONS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(rx) = guard.get(&tag) {
        return rx.clone();
    }
    let (tx, rx) = tokio::sync::watch::channel(None);
    guard.insert(tag, rx.clone());
    drop(guard);
    let dir = cache_dir.to_path_buf();
    tokio::spawn(async move {
        let stored = staged_range_count_now(&dir).await.unwrap_or(u64::MAX);
        // Install the memo BEFORE releasing the map entry, so a caller that arrives between
        // the two finds a fresh memo rather than starting a redundant derivation.
        BUDGET_SNAPSHOT_COUNT.store(stored, Ordering::Relaxed);
        BUDGET_SNAPSHOT_DIR.store(tag, Ordering::Relaxed);
        BUDGET_SNAPSHOT_AT_SECS.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            Ordering::Relaxed,
        );
        BUDGET_DERIVATIONS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&tag);
        let _ = tx.send(Some(stored));
    });
    rx
}

/// `Σ staged_range_count` over shards plus the post-cut pending count — the figure the budget
/// is enforced against. `None` when any input is unreadable.
///
/// The pending term is included because a credit above its shard's cut is a staged range that
/// exists on disk and is not yet in any shard's `staged_range_count`. Omitting it would let a
/// burst between two folds admit an unbounded number of ranges, which is the count the budget
/// exists to bound.
async fn staged_range_count_now(cache_dir: &Path) -> Option<u64> {
    #[cfg(test)]
    count_derivation_stall_hook(cache_dir).await;
    // Pending first, then checkpoints, for the reason `staged_authority_figure_uncached` states:
    // the other order can miss a credit a peer's install and its owner's GC moved between them,
    // and an under-count here admits past the budget.
    let pending = PendingStore::new(cache_dir.to_path_buf()).read_all().await;
    if !pending.is_complete() {
        return None;
    }
    let checkpoints = read_shard_checkpoints(cache_dir);
    if !checkpoints.is_complete() {
        return None;
    }
    Some(
        checkpoints
            .staged_range_count()
            .saturating_add(pending.post_cut(&checkpoints).len() as u64),
    )
}

/// How long **one** acquisition of a `{instance}.pending.lock` may wait.
///
/// # Why 100 ms, and why a per-file bound is not on its own enough
///
/// The caller with the tightest bound is `cache::budget_refusal_off_the_request_path`'s inline
/// re-derivation, bounded at `FRESH_BUDGET_REDERIVE_WAIT` = **300 ms**. That derivation reads
/// **every** `.pending` file sequentially, and the population is O(instance ids since the last
/// startup reap) — recorded **49-52** records on the fleet. So a per-file bound anywhere
/// near 300 ms sums to fifteen seconds and fixes nothing; the per-file bound has to be paired
/// with a per-**call** budget, which is what [`PENDING_READ_LOCK_BUDGET`] is.
///
/// 100 ms is then chosen against the per-file *cost* rather than against the caller's bound: the
/// work under the lock is one `read_to_string` of a file that holds post-cut credits only
/// (kilobytes), or a read plus a rename for a GC — single-digit milliseconds on EFS, so 100 ms
/// is more than an order of magnitude of headroom over an uncontended hold and only trips on a
/// holder that is genuinely stuck rather than slow.
const PENDING_LOCK_WAIT_BOUND: std::time::Duration = std::time::Duration::from_millis(100);

/// The total time one [`PendingStore::read_all`] may spend waiting on locks, across all
/// files.
///
/// **This is the bound that matters**, because `read_all` is the only caller that takes many
/// locks in one call and it is the one on the PUT admission path. Files remaining when it is
/// exhausted are reported unreadable without waiting, so the walk cannot sum past it however
/// many `.pending` files the volume holds.
///
/// 250 ms, i.e. inside `FRESH_BUDGET_REDERIVE_WAIT` (300 ms) with room left over, so the lock
/// waits alone can no longer consume that caller's whole bound — and far enough above
/// [`PENDING_LOCK_WAIT_BOUND`] that two genuinely contended files can each take their full wait
/// before the budget binds.
const PENDING_READ_LOCK_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);

/// How often a bounded acquisition retries. Short against both bounds above, so the observed
/// wait is close to the moment the lock actually became free.
const PENDING_LOCK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(2);

/// Acquisitions of a `{instance}.pending.lock` that did not succeed on the first try.
///
/// **Per-instance**, like every other counter in this module. Published so the next fleet loop
/// can tell a lock wait apart from the checkpoint traversal's cost — ranked those as
/// candidates (2) and (1) for the same 5 s PUT-setup abandons, occurring in the same window,
/// and recorded that nothing published today discriminates them.
static PENDING_LOCK_WAITS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Summed milliseconds spent waiting for those locks, timeouts included.
static PENDING_LOCK_WAIT_MS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Acquisitions abandoned at their bound. Each one is a `.pending` file reported unreadable.
static PENDING_LOCK_TIMEOUTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Whether every `.pending` file under `cache_dir` could be read, right now.
///
/// A **side-effect-free** way for a fixture in another module to establish that a held
/// `{instance}.pending.lock` really does make the post-cut term unknown. Deliberately not
/// `authority_figure_for_tests`, which installs what it derives: since the change bounded the lock
/// acquisition that derivation now *completes* with the file unreadable rather than blocking, so
/// using it as a precondition probe overwrites a primed memo with `None` and the test then
/// measures the fallback instead of its own subject.
#[cfg(test)]
pub(crate) async fn pending_read_all_complete_for_tests(cache_dir: &Path) -> bool {
    PendingStore::new(cache_dir.to_path_buf())
        .read_all()
        .await
        .is_complete()
}

/// Waits, summed wait milliseconds, and timeouts on `{instance}.pending.lock` since process
/// start. **Per-instance.**
pub(crate) fn pending_lock_wait_stats() -> (u64, u64, u64) {
    (
        PENDING_LOCK_WAITS_TOTAL.load(Ordering::Relaxed),
        PENDING_LOCK_WAIT_MS_TOTAL.load(Ordering::Relaxed),
        PENDING_LOCK_TIMEOUTS_TOTAL.load(Ordering::Relaxed),
    )
}

/// Test seam: cache directories whose count derivation stalls for the paired duration
/// before reading anything, the way a cold derivation on a large EFS volume does (1.8-3.1 s
/// measured on the fleet).
///
/// Same shape and purpose as `cache::BUDGET_DERIVATION_STALL`. It exists so the single-flight
/// assertion is not vacuous: without a stall, k callers issued from one test can complete one
/// after another, each finding the 5 s memo fresh, and the unchanged code then also traverses
/// the checkpoint set exactly once. The stall is what makes the callers genuinely concurrent,
/// and therefore what makes the red side real.
#[cfg(test)]
pub(crate) static COUNT_DERIVATION_STALL: Mutex<Vec<(PathBuf, std::time::Duration)>> =
    Mutex::new(Vec::new());

#[cfg(test)]
async fn count_derivation_stall_hook(cache_dir: &Path) {
    let stall = COUNT_DERIVATION_STALL
        .lock()
        .ok()
        .and_then(|v| v.iter().find(|(p, _)| p == cache_dir).map(|(_, d)| *d));
    if let Some(d) = stall {
        tokio::time::sleep(d).await;
    }
}

/// Declines recorded since process start, for the gauge set.
pub(crate) fn budget_declines_total() -> u64 {
    STAGED_RANGE_BUDGET_DECLINES_TOTAL.load(Ordering::Relaxed)
}

/// Whether a **count**-driven write-cache eviction pass is needed, per § 16.6.12 point 5.
///
/// # The defect this closes
///
/// Admission declines on the **count** ([`budget_verdict`], the `count >= STAGED_RANGE_BUDGET`
/// comparison above) while write-cache eviction triggered on **bytes only**
/// (`CacheManager::get_staging_eviction_thresholds`). The classification chain the fold and the
/// evictor share — `assess_staged_candidate` → `staged_compressed_size` →
/// `is_staged_range_parts` — consults no expiry field, so an expired-but-unread staged entry
/// folds as `Evictable` carrying its full bytes *and* its full count, and then never leaves at
/// default config: lazy removal needs an access an unread entry never gets, and
/// `actively_remove_cached_data` defaults false. A small-object write-through workload therefore
/// parks the count at [`STAGED_RANGE_BUDGET`] with bytes at a few percent of the byte bound,
/// write-through caching declines indefinitely, and the budget WARN promises a recovery that no
/// mechanism delivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountEvictionVerdict {
    /// Below the trigger, or the count could not be read. **Unreadable does not evict** — this
    /// pass deletes objects, and guessing "yes" would delete data on no evidence. Same
    /// fail-open direction as the byte arm's unreadable `Resident_Bytes`.
    NotNeeded,
    /// Over the trigger. The pass should reclaim `ranges_to_free` to reach `target`.
    Needed {
        /// The count the trigger fired on, after debiting this process's unfolded reclaims.
        count: u64,
        /// The count the raw inputs reported, before that debit. Published so an operator can
        /// see the latency correction rather than infer it.
        observed_count: u64,
        /// [`STAGED_RANGE_BUDGET`], read at runtime so no publication path names the literal.
        budget: u64,
        /// `trigger_percent` of the budget.
        trigger: u64,
        /// `target_percent` of the budget.
        target: u64,
        /// `count − target`: how many staged ranges this pass sets out to reclaim.
        ranges_to_free: u64,
    },
}

impl CountEvictionVerdict {
    /// Whether a pass is needed. Kept as a method so a caller cannot spell the test as
    /// `!= NotNeeded` and then have to be re-audited when a third variant appears.
    pub(crate) fn is_needed(&self) -> bool {
        matches!(self, Self::Needed { .. })
    }
}

/// Trigger and target for count-driven write-cache eviction, as absolute range counts.
///
/// The same 95/80 hysteresis the byte arm uses (`eviction_trigger_percent` /
/// `eviction_target_percent`), applied to [`STAGED_RANGE_BUDGET`] instead of to the byte bound —
/// which is what Ed ruled on 2026-09-07: trigger/target with hysteresis, **not**
/// one-out-per-one-in at the limit. Reusing the operator's existing percentages rather than
/// introducing a second independent pair is the same argument
/// `get_staging_eviction_thresholds` records for the byte arm.
///
/// The percentages arrive as arguments rather than being read here, so the budget constant stays
/// inside this module and this module stays free of config: `STAGED_RANGE_BUDGET` is read at
/// runtime and the literal appears in no decision or publication path (Ed's I6 constraint).
pub(crate) fn count_eviction_thresholds(trigger_percent: u8, target_percent: u8) -> (u64, u64) {
    let budget = STAGED_RANGE_BUDGET as f64;
    let trigger = (budget * trigger_percent as f64 / 100.0) as u64;
    let target = (budget * target_percent as f64 / 100.0) as u64;
    (trigger, target)
}

/// Record a count-driven pass's reclaim, so the next evaluation does not re-ask for it.
///
/// See [`COUNT_RECLAIMED_SINCE_FOLD`] for why this exists and why the direction is safe.
/// Returns the process totals `(passes_total, ranges_freed_total)` after recording, which the
/// pass publishes.
///
/// Also publishes the debit as `instance_id`'s [`ReclaimDocument`], so peers' verdicts debit it
/// too. Written under the state lock, so an install's retirement cannot interleave. A write that
/// fails is logged and leaves peers blind to this reclaim until the next one, which is the
/// behaviour before documents existed; the local debit is unaffected.
pub(crate) fn record_count_eviction_pass(
    cache_dir: &Path,
    instance_id: &str,
    ranges_reclaimed: u64,
) -> (u64, u64) {
    let tag = dir_tag(cache_dir);
    let Ok(mut states) = COUNT_EVICTION_STATE.lock() else {
        return (0, 0);
    };
    let state = states.entry(tag).or_default();
    state.reclaimed_since_fold = state.reclaimed_since_fold.saturating_add(ranges_reclaimed);
    if ranges_reclaimed > 0 {
        let document = ReclaimDocument {
            schema: RECLAIM_DOCUMENT_SCHEMA,
            instance_id: instance_id.to_string(),
            generation: state.generation,
            ranges_reclaimed: state.reclaimed_since_fold,
            recorded_at_unix_nanos: now_nanos(),
        };
        if let Err(e) = document.install(cache_dir) {
            warn!(
                "Could not publish the count-eviction reclaim document for {} ({}); peers will \
                 not debit these {} range(s)",
                instance_id, e, ranges_reclaimed
            );
        }
    }
    // Latch only on a pass that reclaimed NOTHING. Partial progress means the population was
    // non-empty, so the next cycle has real work to do. See `found_nothing`.
    if ranges_reclaimed == 0 {
        state.found_nothing = true;
    }
    state.passes_total = state.passes_total.saturating_add(1);
    state.ranges_freed_total = state.ranges_freed_total.saturating_add(ranges_reclaimed);
    (state.passes_total, state.ranges_freed_total)
}

/// The count verdict, for the write-cache eviction trigger.
///
/// # Containment, mirroring [`budget_verdict`]
///
/// Returns a **verdict**, never a bare figure, and reaches its input through
/// [`budget_snapshot_count`] — the same memoised read admission uses. So the count admission
/// declines on and the count eviction triggers on cannot drift apart, which was the whole shape
/// of the defect. `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface` asserts
/// this symbol has exactly one caller file, as it does for `budget_verdict`.
///
/// Epoch-independent, deliberately: [`budget_verdict`] declines regardless of the staged-authority
/// epoch, so a count trigger gated on the epoch would leave the wedge live in Phase L. Both read
/// the shard checkpoints directly.
///
/// The debit is this instance's `reclaimed_since_fold` plus every peer's live
/// [`ReclaimDocument`]; `instance_id` names this instance so its own document is not counted
/// twice.
pub(crate) async fn count_eviction_verdict(
    cache_dir: &Path,
    instance_id: &str,
    trigger_percent: u8,
    target_percent: u8,
) -> CountEvictionVerdict {
    let (trigger, target) = count_eviction_thresholds(trigger_percent, target_percent);
    if trigger == 0 {
        return CountEvictionVerdict::NotNeeded;
    }
    let state = count_state_for(dir_tag(cache_dir));
    // A pass has already established that nothing is reclaimable against these inputs, and the
    // inputs cannot have changed in a way that makes something reclaimable until a fold installs.
    // Checked BEFORE the count read, because that read is a readdir plus a locked read of every
    // `.pending` file and there is nothing this pass could do with the answer.
    if state.found_nothing {
        return CountEvictionVerdict::NotNeeded;
    }
    let observed_count = budget_snapshot_count(cache_dir).await;
    if observed_count == u64::MAX {
        return CountEvictionVerdict::NotNeeded;
    }
    let peers = peer_reclaims_ranges(cache_dir, instance_id).await;
    let count = observed_count
        .saturating_sub(state.reclaimed_since_fold)
        .saturating_sub(peers);
    if count <= trigger {
        return CountEvictionVerdict::NotNeeded;
    }
    CountEvictionVerdict::Needed {
        count,
        observed_count,
        budget: STAGED_RANGE_BUDGET,
        trigger,
        target,
        ranges_to_free: count.saturating_sub(target),
    }
}

/// Why a pass retained the previous figure instead of installing a new one.
///
/// Each variant is a distinct **class** of incomplete input, and they are kept separate
/// rather than collapsed into a bool because the reason is what tells an operator whether
/// to look at a file, a parse, or a cap. One test per class, for the same reason.
///
/// # Two SCOPES, and the difference is operationally visible
///
/// A variant refuses either the whole pass or one shard, and which one depends on whether the
/// input has a per-shard form:
///
/// - **Shard-scoped** — the four per-file ledger classes ([`Self::LedgerFileUnreadable`],
///   [`Self::LedgerMalformedLine`], [`Self::LedgerTruncated`],
///   [`Self::LedgerFileCountMismatch`]) plus [`Self::CandidateUnreadable`] and
///   [`Self::CheckpointWriteFailed`]. The affected shard keeps its previous figure and cut; its
///   siblings install new ones in the same pass. The four ledger classes were pass-scoped until
///   the fold read only the shards it visits (§ 16.6.12 point 2) — before that, one unreadable
///   ledger file anywhere on the volume froze every shard's figure.
/// - **Pass-scoped** — [`Self::LegacyLedgerPresent`], [`Self::PendingUnreadable`],
///   [`Self::InProgressPartsUnreadable`], [`Self::PreviousCheckpointUnreadable`] and the three
///   authority-block variants. Each of these is either volume-wide by nature or names an input
///   with no shard-scoped form, so there is no shard to confine the refusal to.
/// - **Both** — [`Self::LedgerListFailed`]. See its own note.
///
/// The reported reason of a pass that retained one shard and installed others is that shard's,
/// so `staged_fold_incomplete_total` still increments and the cadence is still not consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldRetainReason {
    /// At least one `{bucket}/{l1}/{instance}.ledger` could not be opened or read.
    ///
    /// **Shard-scoped**: the file lives in the shard whose figure it explains, so only that
    /// shard retains. That is the whole of § 16.6.12 point 2's contract change — one lost file
    /// costs one shard's freshness rather than the volume's.
    LedgerFileUnreadable,
    /// At least one unconverted legacy flat `{instance}.ledger` is still present, so a
    /// participant's entries name no shard and every shard's figure may be short by whatever
    /// they hold.
    ///
    /// # Not an incompleteness of the read, and not something to engineer around
    ///
    /// The other ledger variants above are "this read could not see the file". This one is "the
    /// file is readable and its entries belong to a layout this pass cannot attribute to a
    /// shard". It arises only during a rolling deploy, and the consequence is stated rather
    /// than avoided: the write-cache figure stops refreshing for the duration, its age rises
    /// visibly on `staged_shard_authority_age_secs`, and staging eviction decides on the
    /// retained figure — which **over**-states and therefore fires the trigger early rather
    /// than late, the safe direction.
    ///
    /// Clears itself: `WriteLedger::migrate_legacy_ledgers` converts every legacy file it
    /// finds, including a decommissioned peer's, on each consolidation cycle.
    LegacyLedgerPresent,
    /// At least one ledger line was discarded by the parser (malformed, torn, or blank).
    ///
    /// **Shard-scoped**, for the same reason as [`Self::LedgerFileUnreadable`].
    LedgerMalformedLine,
    /// The read hit its cap, so it saw a prefix of the ledger rather than all of it.
    ///
    /// **Shard-scoped**, and the cap now applies per shard: a shard whose entries fit under the
    /// cap is unaffected by a sibling that does not.
    LedgerTruncated,
    /// The directory listing named more files than were read.
    ///
    /// **Shard-scoped**, and **currently unreachable**: every path that fails to read a listed
    /// file also increments `files_unreadable`, so [`Self::LedgerFileUnreadable`] fires first.
    /// The doc used to claim a file vanishing between the listing and the lock lands here; it
    /// lands in `files_unreadable`. Kept as the defensive statement it was written as — the
    /// invariant it asserts is one a future reader could break — but do not read a green test
    /// suite as evidence that this branch works.
    LedgerFileCountMismatch,
    /// The ledger could not be listed at all.
    ///
    /// **Both scopes, and they are different failures.** The volume enumeration
    /// (`WriteLedger::ledger_shards`, and the legacy-file listing beside it) is pass-scoped: if
    /// it fails, which shards exist is unknown and there is nothing to confine a refusal to. One
    /// shard's own directory listing is shard-scoped: which files that shard has is unknown, so
    /// that shard retains and its siblings do not. One label because the remedy is the same —
    /// look at the volume — and because an operator reading the WARN is told which path failed.
    LedgerListFailed,
    /// A `{instance}.pending` file could not be read, or held a line the parser dropped.
    PendingUnreadable,
    /// The `mpus_in_progress/` traversal failed, so `P` is unknown.
    InProgressPartsUnreadable,
    /// A selected candidate's `.meta` could not be read or parsed, so its contribution to
    /// the figure is unknown.
    ///
    /// **Shard-scoped in effect but pass-ENDING in mechanism**: the affected shard retains and
    /// shards already installed keep their new documents, but the pass returns rather than
    /// visiting the shards behind it. See the note at that `return` for the ordering exposure
    /// that follows, which the four ledger classes above deliberately do not share.
    CandidateUnreadable,
    /// A candidate's `.meta` was absent while an instance-scoped temp `.meta` for the same key
    /// existed and its newest ledger entry was inside `ABSENT_RETIREMENT_GRACE`: a multipart
    /// completion between its ledger append and its rename. Installing a cut over that entry
    /// would filter the completion's pending credit while `F` excludes the object, so the shard
    /// keeps its previous figure and cut and the credit stays in the pending term until a
    /// later visit reads the published `.meta`.
    ///
    /// **Shard-scoped**: the pass continues with the next shard.
    CandidatePublishing,
    /// The checkpoint could not be written. The previous document is still on disk, which
    /// is a retain of both fields by construction.
    CheckpointWriteFailed,
    /// The previous checkpoint exists but could not be read, so the cut to carry forward
    /// is unknown and installing a new one would abandon it.
    PreviousCheckpointUnreadable,
    /// `Foreign_Write_Detection` fired: after activation, `size_state.json` carries a
    /// pre-C schema or epoch, so a binary predating decision C has rewritten the shared
    /// accounting state (§ 16.6.6).
    ///
    /// Refusing to install here is the whole point of the backstop. The published figure
    /// goes stale **visibly**, on `staged_shard_authority_age_secs`, rather than silently
    /// competing with a second producer — an operator who can see a stalled authority can
    /// recover; one shown a confident wrong number cannot.
    ///
    /// **Narrowed.** This variant now means only what its name says: a second writer,
    /// confirmed by the document still lacking the fence after this instance re-applied it.
    /// The two situations that used to arrive here as well have their own variants below,
    /// because they have different remedies — see
    /// [`crate::staged_authority::AuthorityBlockKind`].
    ForeignAuthorityWrite,
    /// `size_state.json` could not be read, so whether the authority is contested is unknown.
    ///
    /// Distinct from [`Self::ForeignAuthorityWrite`] because it is not an accusation. It is the
    /// same "we could not see the state" class as the ledger and pending variants above, and it
    /// belongs with them rather than with the second-writer case.
    SizeStateUnreadable,
    /// The `Schema_Fence` is unapplied and re-applying it failed. Self-inflicted; a later cycle
    /// retries it.
    SchemaFenceUnapplied,
    /// Every shard this pass could have visited was claimed by a peer, or there was no
    /// shard to visit at all.
    ///
    /// **Not a failure**, and the only variant that is not. It is a `Retained` outcome
    /// because nothing was installed, so the cadence must not be consumed — a pass that
    /// found every shard busy should try again rather than wait out the interval. It is
    /// excluded from `staged_fold_incomplete_total` for the same reason: counting a
    /// successful peer as an incompleteness would make that counter unusable as the
    /// criterion's zero-increment gate.
    NoShardAvailable,
}

impl FoldRetainReason {
    /// Short stable label, for the `staged_fold_incomplete_total{reason}` log line.
    pub fn label(&self) -> &'static str {
        match self {
            Self::LedgerFileUnreadable => "ledger_file_unreadable",
            Self::LegacyLedgerPresent => "legacy_ledger_present",
            Self::LedgerMalformedLine => "ledger_malformed_line",
            Self::LedgerTruncated => "ledger_truncated",
            Self::LedgerFileCountMismatch => "ledger_file_count_mismatch",
            Self::LedgerListFailed => "ledger_list_failed",
            Self::PendingUnreadable => "pending_unreadable",
            Self::InProgressPartsUnreadable => "in_progress_parts_unreadable",
            Self::CandidateUnreadable => "candidate_unreadable",
            Self::CandidatePublishing => "candidate_publishing",
            Self::CheckpointWriteFailed => "checkpoint_write_failed",
            Self::PreviousCheckpointUnreadable => "previous_checkpoint_unreadable",
            Self::ForeignAuthorityWrite => "foreign_authority_write",
            Self::SizeStateUnreadable => "size_state_unreadable",
            Self::SchemaFenceUnapplied => "schema_fence_unapplied",
            Self::NoShardAvailable => "no_shard_available",
        }
    }

    /// Whether this reason represents an **incomplete input** rather than a scheduling
    /// outcome. Only these increment `staged_fold_incomplete_total`.
    pub(crate) fn is_incompleteness(&self) -> bool {
        !matches!(self, Self::NoShardAvailable)
    }
}

/// What one fold pass concluded, across every shard it visited.
///
/// # Two granularities, and both statements hold
///
/// § 16.6.4 is explicit that these are different claims about different things, and the
/// enum is shaped so neither can be mistaken for the other:
///
/// - **Per shard, install is complete-or-retain.** Each visited shard either installs a
///   document holding its figure and its cut together, or leaves its previous document
///   untouched. [`Self::Installed`] and [`Self::Retained`] both carry the per-shard
///   breakdown so a caller can see which shards moved.
/// - **Globally, the sum is a mixed-age sum of complete shard figures and never contains a
///   partial one.** That is a property of the *reader*
///   ([`read_shard_checkpoints`]), not of a pass, and it is enforced there: any
///   unreadable shard document makes the global figure unknown rather than short.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldOutcome {
    /// Every shard this pass visited performed a Complete_Install.
    Installed(FoldPassSummary),
    /// At least one visited shard retained. Every shard that installed is listed in the summary
    /// and its figure is on disk; each retaining shard's previous figure and cut are both still
    /// on disk, unchanged.
    ///
    /// The pass reports this even when other shards installed, because the *cadence* gate
    /// consumes [`Self::installed`] and § 16.6.4 requires that a failure not consume the
    /// cadence — a pass that got three shards done and failed the fourth should retry
    /// promptly, not wait out the interval.
    ///
    /// A shard-scoped ledger refusal does not stop the pass, so "installed before it" is no
    /// longer the right reading: shards ordered *after* a retained shard install too, and the
    /// reason carried here is the FIRST retain in visit order.
    Retained(FoldRetainReason, FoldPassSummary),
}

/// Per-shard breakdown of one pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FoldPassSummary {
    /// Shards that installed, in visit order, with the document each installed.
    pub installed: Vec<StagedShardCheckpoint>,
    /// Shards considered but held by a live peer claim.
    pub busy: Vec<Shard>,
    /// Shards that retained, in visit order, and why.
    ///
    /// # Why a list rather than one slot
    ///
    /// It held one slot while every retain ended the pass. A **shard-scoped** ledger
    /// incompleteness does not: the pass records it, leaves that shard's document alone, and
    /// carries on to the next shard — so more than one shard can retain in a pass, and each
    /// for its own reason.
    ///
    /// Carrying on is not a convenience. Selection is stalest-first on `staged_scanned_at`, and
    /// a retained shard does not move its own `staged_scanned_at`, so a shard whose ledger file
    /// is *persistently* unreadable sorts first on every subsequent pass. Aborting the pass at
    /// it would therefore abort every future pass at the same shard, and no shard on the volume
    /// would ever install again — the pass-scoped behaviour this change exists to remove,
    /// reintroduced by the ordering.
    ///
    /// The two retains that still end the pass — `CandidateUnreadable` and
    /// `CheckpointWriteFailed` — push one entry and return, so their behaviour is unchanged.
    /// They have the same ordering exposure and it is pre-existing; see the note in
    /// `run_staged_fold_with_cap`.
    pub retained: Vec<(Shard, FoldRetainReason)>,
    /// Peak `.meta` reads this pass held outstanding at once, over its shard visits.
    ///
    /// Scoped to the pass rather than to the process, which is what makes it assertable — see
    /// [`InFlightGauge`]. `1` on a pass that folded a shard with more than one candidate means the
    /// candidate loop ran serially, which is the state that put § 16.6.6's reachable-bucket ceiling
    /// at zero.
    pub candidate_read_inflight_peak: u64,
}

impl FoldOutcome {
    /// Whether this pass installed. The cadence gate consumes exactly this: a retain must
    /// not consume the retry interval (§ 16.6.9).
    pub fn installed(&self) -> bool {
        matches!(self, Self::Installed(_))
    }

    /// The per-shard breakdown, whichever arm this is.
    pub fn summary(&self) -> &FoldPassSummary {
        match self {
            Self::Installed(s) => s,
            Self::Retained(_, s) => s,
        }
    }

    /// The retain reason, if this pass retained.
    pub fn retain_reason(&self) -> Option<FoldRetainReason> {
        match self {
            Self::Installed(_) => None,
            Self::Retained(r, _) => Some(*r),
        }
    }
}

/// A `(bucket, L1)` shard — the § 3.1 unit, and the unit of both the claim and the
/// checkpoint.
///
/// `l1` is the `XX` of the `.meta` path: the first two hex digits of BLAKE3 over the
/// **object key**. The bucket is a separate path component and is deliberately not hashed
/// into it, which is exactly why `l1` alone does not identify a directory and the shard is
/// the pair — the correction § 3.1 records as easy to get wrong.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Shard {
    /// Bucket component of the cache key.
    pub bucket: String,
    /// First shard level, two lowercase hex digits.
    pub l1: String,
}

impl Shard {
    /// A shard from its two components.
    pub fn new(bucket: impl Into<String>, l1: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            l1: l1.into(),
        }
    }
}

impl std::fmt::Display for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bucket, self.l1)
    }
}

/// One staged credit, identity-carrying, held until a fold's cut passes it.
///
/// `bytes` is compressed on-disk bytes — the fold's unit, and the same unit as
/// `RangeSpec::compressed_size` and `in_progress_part_bytes`. Do not put an uncompressed
/// figure here; § 16.6.2 records what that costs.
///
/// `seq` is the ledger entry's explicit [`crate::write_ledger::WriteLedgerEntry::seq`],
/// copied from the value the ledger assigned to the entry appended by the same call site —
/// not recomputed, and not clock-derived. It is a plain `u64` rather than an `Option`
/// because this file format is introduced by the same change that introduced the seq, so a
/// record without one cannot exist: a binary old enough to omit the seq writes no pending
/// records at all. Its staged bytes are covered by `F` instead, once any fold reads its
/// `.meta`.
///
/// `(bucket, l1)` is the § 3.1 shard of the object key, and **it is now load-bearing**: I2
/// recorded it against a single global cut without reading it, and this unit's cut is
/// per-shard, so a credit is filtered against the cut of *its own* shard's checkpoint.
/// Filtering it against any other shard's cut would either resurrect a folded credit or
/// swallow a fresh one. See [`PendingReadOutcome::post_cut`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCredit {
    /// The `Ledger_Seq` of the ledger entry appended by the same call site.
    pub seq: u64,
    /// Bucket component of the cache key.
    pub bucket: String,
    /// First shard level: the first two hex digits of BLAKE3 over the object key, i.e.
    /// the `XX` of the `.meta` path.
    pub l1: String,
    /// Compressed on-disk bytes credited.
    pub bytes: u64,
    /// The staged range this credit is for, as the paired ledger entry names it.
    ///
    /// Carried so [`PendingReadOutcome::post_cut`] can count a range once when two instances
    /// credited it: two ledger-repair sweeps of one shard under a duplicated claim each
    /// re-append the same missing range. A range on disk is one file per `(cache_key, start,
    /// end)`, so two post-cut credits naming the same range describe bytes stored once.
    /// `None` on a record written by a build that predates the field; such a record is
    /// never de-duplicated, which is the incumbent over-counting behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<PendingRange>,
}

/// The range identity a [`PendingCredit`] carries: the Write_Ledger entry's
/// `(cache_key, range_start, range_end)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PendingRange {
    /// The ledger entry's cache key.
    pub cache_key: String,
    /// The ledger entry's range start.
    pub start: u64,
    /// The ledger entry's range end.
    pub end: u64,
}

impl PendingCredit {
    /// The § 3.1 shard this credit belongs to.
    pub fn shard(&self) -> Shard {
        Shard::new(self.bucket.clone(), self.l1.clone())
    }
}

/// A pending-credit read that can say whether it was complete.
///
/// Same shape and the same reasoning as [`crate::write_ledger::LedgerReadOutcome`]: an
/// unreadable `.pending` file and an absent one must not be the same value, because one
/// of them means the post-cut term is unknown and the fold must refuse.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingReadOutcome {
    /// Credits read, paired with the instance whose file held them. The instance is
    /// carried because the cut is per-instance, so a credit cannot be filtered without
    /// knowing which file it came from.
    pub credits: Vec<(String, PendingCredit)>,
    /// Pending files the directory listing named.
    pub files_expected: usize,
    /// Files opened and parsed without an I/O error.
    pub files_read: usize,
    /// Files that could not be opened or read.
    pub files_unreadable: usize,
    /// Lines discarded by the parser, blank lines included — a blank line in an
    /// append-only JSONL file is what a torn append looks like.
    pub malformed_lines: usize,
}

impl PendingReadOutcome {
    /// Whether this read saw every pending record that exists.
    pub fn is_complete(&self) -> bool {
        self.files_unreadable == 0
            && self.malformed_lines == 0
            && self.files_read == self.files_expected
    }

    /// Credits strictly after the cut of **their own shard's** checkpoint: the term the
    /// reader adds to `F`.
    ///
    /// Two defaults, and both are the safe direction:
    ///
    /// - A credit whose **shard** has no checkpoint at all is post-cut. No fold has ever
    ///   visited that shard, so no figure counted it.
    /// - A credit whose shard has a checkpoint that does not name its **instance** is
    ///   post-cut. That shard's fold has never observed that instance, so again nothing
    ///   counted it.
    ///
    /// Both err towards counting, which over-states for at most one fold interval. The
    /// opposite default would filter a credit out of the pending term while no figure held
    /// it, which is § 16.6.1's counted-by-neither outcome and makes the eviction trigger
    /// fire **late** — the direction that loses data rather than wastes space.
    ///
    /// Post-cut credits naming the same [`PendingRange`] are returned once, the largest
    /// `bytes` kept. They describe one range file, so summing them over-states; keeping the
    /// largest never under-states that file. Credits without a range identity are all kept.
    pub(crate) fn post_cut(&self, checkpoints: &ShardCheckpointSet) -> Vec<&PendingCredit> {
        let mut out: Vec<&PendingCredit> = Vec::new();
        let mut by_range: HashMap<&PendingRange, usize> = HashMap::new();
        for (instance, credit) in &self.credits {
            let cut = checkpoints
                .get(&credit.shard())
                .and_then(|c| c.staged_fold_cut.get(instance))
                .copied()
                .unwrap_or(0);
            if credit.seq <= cut {
                continue;
            }
            match credit.range.as_ref() {
                Some(range) => match by_range.get(range) {
                    Some(&i) => {
                        if credit.bytes > out[i].bytes {
                            out[i] = credit;
                        }
                    }
                    None => {
                        by_range.insert(range, out.len());
                        out.push(credit);
                    }
                },
                None => out.push(credit),
            }
        }
        out
    }

    /// Summed bytes of the post-cut credits.
    pub(crate) fn post_cut_bytes(&self, checkpoints: &ShardCheckpointSet) -> u64 {
        self.post_cut(checkpoints)
            .into_iter()
            .fold(0u64, |acc, c| acc.saturating_add(c.bytes))
    }

    /// Shards named by any credit, post-cut or not. A shard with a live credit but no
    /// checkpoint has never been folded and is exactly the shard a checkpoint-only
    /// enumeration would miss.
    pub fn shards(&self) -> BTreeSet<Shard> {
        self.credits.iter().map(|(_, c)| c.shard()).collect()
    }
}

/// A one-shot action a test runs inside [`PendingStore::append_batch`] — after the batch's
/// ledger lines are durable, because the pending stage receives only synced entries, and
/// **before** the pending write — keyed by [`dir_tag`].
///
/// Tests only: it is how a fixture puts a fold install between the two stages of § 16.6.14,
/// which in production is a race.
#[cfg(test)]
static PENDING_BEFORE_WRITE_HOOK: Mutex<BTreeMap<u64, AuthorityBetweenReadsHook>> =
    Mutex::new(BTreeMap::new());

/// Register `hook` to run once, before `cache_dir`'s next pending-batch write.
#[cfg(test)]
fn set_pending_before_write_hook_for_tests(cache_dir: &Path, hook: AuthorityBetweenReadsHook) {
    PENDING_BEFORE_WRITE_HOOK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(dir_tag(cache_dir), hook);
}

/// Run and consume `cache_dir`'s registered pending hook, if any. The guard is dropped before
/// the await.
#[cfg(test)]
async fn run_pending_before_write_hook(cache_dir: &Path) {
    let hook = PENDING_BEFORE_WRITE_HOOK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&dir_tag(cache_dir));
    if let Some(hook) = hook {
        hook().await;
    }
}

/// Per-instance append-only store of pending credits, on the shared volume.
///
/// Only the owning instance ever rewrites its own file, which is a strict improvement on
/// the ledger's shared-rewrite shape: a peer's fold reads these files but never writes
/// them.
pub struct PendingStore {
    cache_dir: PathBuf,
}

impl PendingStore {
    /// A store rooted at `cache_dir`.
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// `{cache_dir}/metadata/_shard_state/staged_pending`.
    pub fn dir(&self) -> PathBuf {
        self.cache_dir
            .join("metadata")
            .join(SHARD_STATE_DIR)
            .join(PENDING_DIR)
    }

    fn path_for(&self, instance_id: &str) -> PathBuf {
        self.dir().join(format!("{}.{}", instance_id, PENDING_EXT))
    }

    fn lock_path_for(path: &Path) -> PathBuf {
        let mut name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        name.push_str(".lock");
        path.with_file_name(name)
    }

    /// Take `lock_file`'s `fs4` lock, giving up after `bound`.
    ///
    /// # Why every acquisition on this file needs a bound
    ///
    /// `{instance}.pending.lock` is the only **cross-instance** lock on the write-through PUT
    /// admission path, and every acquisition used to be a `fs4::FileExt::lock` — an unbounded
    /// wait. The holders are a peer's [`PendingStore::append`] (every write-through credit on
    /// **any** instance), the fold's own post-pass [`PendingStore::gc`], the startup reaper's
    /// [`PendingStore::retire_if_nothing_uncut`] and every other reader. A measurement found an 80 s
    /// stall in this same window with 10 of 26 NFS locks lost, so a hold is not bounded by the
    /// work inside it: a wedged NFS client holds it until its lease expires.
    ///
    /// On expiry the caller treats the file as **unreadable**, which every caller already fails
    /// open on — see [`PendingReadOutcome::is_complete`]'s consumers. Failing open here admits;
    /// blocking forever forwards the upload uncached, which is the outage this whole area
    /// exists to prevent.
    ///
    /// Implemented as a `try_lock` poll rather than a blocking `lock` because `fs4` exposes no
    /// timed acquisition. The poll interval is [`PENDING_LOCK_POLL_INTERVAL`]; an error that is
    /// not contention (a bad descriptor, say) is retried until the bound and then reported as a
    /// timeout, which is the same fail-open outcome and avoids classifying `fs4`'s error kinds.
    fn lock_with_bound(
        lock_file: &std::fs::File,
        bound: std::time::Duration,
        path: &Path,
    ) -> std::io::Result<()> {
        if fs4::FileExt::try_lock(lock_file).is_ok() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        PENDING_LOCK_WAITS_TOTAL.fetch_add(1, Ordering::Relaxed);
        loop {
            if fs4::FileExt::try_lock(lock_file).is_ok() {
                PENDING_LOCK_WAIT_MS_TOTAL
                    .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                return Ok(());
            }
            if started.elapsed() >= bound {
                PENDING_LOCK_WAIT_MS_TOTAL
                    .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
                PENDING_LOCK_TIMEOUTS_TOTAL.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Pending-credit lock for {:?} not acquired within {:?}; treating the file as \
                     unreadable, which admits rather than blocking the caller",
                    path, bound
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!("pending lock not acquired within {:?}", bound),
                ));
            }
            std::thread::sleep(PENDING_LOCK_POLL_INTERVAL);
        }
    }

    /// Append `credits` to `instance_id`'s file in **one** `write_all` under one hold of the
    /// file's `fs4` lock, with one `sync_data` — the pending stage's write (design § 16.6.14).
    ///
    /// There is one pending file per instance, so coalescing is this stage's whole gain: the
    /// 100 ms-bounded lock is taken once and synced once however many records the batch carries.
    /// Record order inside the file carries no meaning, because every reader filters each record
    /// by its own seq against its own shard's cut, so nothing orders one batch against another.
    ///
    /// Best-effort in the same sense the one-record form is, and a failure now loses the whole
    /// batch: `write_ledger::AppenderCore::run_pending_stage` marks every affected shard dirty
    /// before it replies, which bounds the resulting under-count to one pass.
    pub(crate) async fn append_batch(
        &self,
        instance_id: &str,
        credits: &[PendingCredit],
    ) -> std::io::Result<()> {
        if credits.is_empty() {
            return Ok(());
        }
        let dir = self.dir();
        tokio::fs::create_dir_all(&dir).await?;
        let path = self.path_for(instance_id);
        let lock_path = Self::lock_path_for(&path);
        let mut body = String::new();
        for credit in credits {
            let line = serde_json::to_string(credit)
                .map_err(|e| std::io::Error::other(format!("serialize pending credit: {}", e)))?;
            body.push_str(&line);
            body.push('\n');
        }
        #[cfg(test)]
        run_pending_before_write_hook(&self.cache_dir).await;

        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            let lock_file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&lock_path)?;
            Self::lock_with_bound(&lock_file, PENDING_LOCK_WAIT_BOUND, &lock_path)?;
            let result = (|| -> std::io::Result<()> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)?;
                file.write_all(body.as_bytes())?;
                file.flush()?;
                // Before the drop, so a deferred NFS write error reaches the caller's WARN
                // rather than being discarded by `close`.
                file.sync_data()
            })();
            let _ = fs4::FileExt::unlock(&lock_file);
            result
        })
        .await
        .map_err(|e| std::io::Error::other(format!("pending append task failed: {}", e)))?
    }

    /// [`PendingStore::append_batch`] for one credit.
    ///
    /// Test-only since the pending stage took over production's writes: every production credit
    /// now arrives in a batch. Kept because a fixture that needs one record should take the same
    /// write path, not a second one.
    ///
    /// Best-effort in the same sense the ledger append is: the caller has already
    /// succeeded against S3, and failing an upload over a bookkeeping write would be
    /// worse than the consequence of losing the record. Losing one makes the published
    /// staged figure **under**-state until the next fold's cut passes the corresponding
    /// ledger entry, at which point the `.meta` fold picks the bytes up.
    #[cfg(test)]
    pub async fn append(&self, instance_id: &str, credit: &PendingCredit) -> std::io::Result<()> {
        self.append_batch(instance_id, std::slice::from_ref(credit))
            .await
    }

    /// Every pending file, listed.
    fn files(&self) -> std::io::Result<Vec<PathBuf>> {
        let dir = self.dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|e| e == PENDING_EXT) {
                files.push(path);
            }
        }
        files.sort();
        Ok(files)
    }

    /// Read every instance's pending file under its own lock, reporting completeness.
    pub(crate) async fn read_all(&self) -> PendingReadOutcome {
        let files = match self.files() {
            Ok(f) => f,
            Err(e) => {
                warn!("Could not list pending-credit directory: {}", e);
                // A listing failure is not "no files": report it as one unreadable file
                // against zero read, so `is_complete` is false.
                return PendingReadOutcome {
                    files_expected: 1,
                    files_unreadable: 1,
                    ..Default::default()
                };
            }
        };
        let mut outcome = PendingReadOutcome {
            files_expected: files.len(),
            ..Default::default()
        };
        // One lock-wait budget for the whole sequential walk, not per file. Per-file alone
        // sums over O(instance ids since the last startup reap) — 49-52 on the fleet — and could
        // still spend the caller's whole `FRESH_BUDGET_REDERIVE_WAIT`.
        let lock_deadline = std::time::Instant::now() + PENDING_READ_LOCK_BUDGET;
        for path in files {
            let instance = instance_of(&path);
            // The remaining budget, capped at the per-file bound. Zero once exhausted, which
            // makes `lock_with_bound` a single `try_lock` and no wait at all: the rest of the
            // walk reports unreadable immediately rather than extending the call.
            let per_file = lock_deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(PENDING_LOCK_WAIT_BOUND);
            let lock_path = Self::lock_path_for(&path);
            let path_for_task = path.clone();
            let read = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_path)?;
                Self::lock_with_bound(&lock_file, per_file, &lock_path)?;
                // A file listed and then retired by the dead-instance reaper before this
                // read held nothing uncut, so it reads as no credits rather than unreadable.
                let result = match std::fs::read_to_string(&path_for_task) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
                    other => other,
                };
                let _ = fs4::FileExt::unlock(&lock_file);
                result
            })
            .await;
            match read {
                Ok(Ok(content)) => {
                    outcome.files_read += 1;
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            outcome.malformed_lines += 1;
                            continue;
                        }
                        match serde_json::from_str::<PendingCredit>(line) {
                            Ok(credit) => outcome.credits.push((instance.clone(), credit)),
                            Err(e) => {
                                outcome.malformed_lines += 1;
                                debug!("Skipping unparseable pending line in {:?}: {}", path, e);
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    outcome.files_unreadable += 1;
                    warn!(
                        "Pending-credit read could not read {:?}: {}. The post-cut term is \
                         unknown, so no figure may be installed from this pass.",
                        path, e
                    );
                }
                Err(e) => {
                    // Counted as unreadable whatever the cause: a file this pass did not read
                    // still forbids installing a figure from it (`is_complete`, read by
                    // `run_staged_fold_with_cap`'s `PendingUnreadable` retain). Only the log
                    // level depends on the cause.
                    outcome.files_unreadable += 1;
                    log_pending_read_join_failure(&path, &e);
                }
            }
        }
        outcome
    }

    /// Drop records **this instance's own shard cuts cover** from its own file, under its
    /// own lock.
    ///
    /// A failed truncation is harmless: dead records are filtered by the same per-shard
    /// comparison on every read, so this is space reclamation rather than correctness.
    ///
    /// **The cut is looked up per shard**, which is the change partitioning forces. A single
    /// scalar cut would reclaim a record whose shard has not been folded — a record no shard
    /// figure counts — turning a transient counted-by-neither into a permanent one, because
    /// the record is the only thing that remembers those bytes until a fold reads the `.meta`.
    /// A record whose shard has no checkpoint is therefore always kept.
    pub(crate) async fn gc(
        &self,
        instance_id: &str,
        checkpoints: &ShardCheckpointSet,
    ) -> std::io::Result<usize> {
        let path = self.path_for(instance_id);
        if !path.exists() {
            return Ok(0);
        }
        let cuts: BTreeMap<Shard, u64> = checkpoints
            .iter()
            .filter_map(|(shard, c)| {
                c.staged_fold_cut
                    .get(instance_id)
                    .map(|cut| (shard.clone(), *cut))
            })
            .collect();
        let lock_path = Self::lock_path_for(&path);
        tokio::task::spawn_blocking(move || -> std::io::Result<usize> {
            use std::io::Write;
            let lock_file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&lock_path)?;
            Self::lock_with_bound(&lock_file, PENDING_LOCK_WAIT_BOUND, &lock_path)?;
            let result = (|| -> std::io::Result<usize> {
                // Read, filter and rename all inside one hold of the lock an append
                // takes, so an append lands wholly before the read or wholly after the
                // rename. This is the shape `retire_identities` had to be corrected to.
                let content = std::fs::read_to_string(&path)?;
                let mut kept = String::new();
                let mut dropped = 0usize;
                for line in content.lines() {
                    if line.trim().is_empty() {
                        dropped += 1;
                        continue;
                    }
                    match serde_json::from_str::<PendingCredit>(line) {
                        Ok(credit)
                            if cuts
                                .get(&credit.shard())
                                .is_some_and(|cut| credit.seq <= *cut) =>
                        {
                            dropped += 1
                        }
                        Ok(_) => {
                            kept.push_str(line);
                            kept.push('\n');
                        }
                        // An unparseable line is retained rather than dropped: it may be
                        // a post-cut credit this binary cannot read, and dropping it
                        // would turn an unreadable record into a silently lost one.
                        Err(_) => {
                            kept.push_str(line);
                            kept.push('\n');
                        }
                    }
                }
                if dropped == 0 {
                    return Ok(0);
                }
                let tmp = path.with_extension("pending.tmp");
                {
                    let mut f = std::fs::File::create(&tmp)?;
                    f.write_all(kept.as_bytes())?;
                    f.sync_all()?;
                }
                std::fs::rename(&tmp, &path)?;
                Ok(dropped)
            })();
            let _ = fs4::FileExt::unlock(&lock_file);
            result
        })
        .await
        .map_err(|e| std::io::Error::other(format!("pending gc task failed: {}", e)))?
    }

    /// Every credit naming one of `ranges`, with the verbatim line it was read from, over every
    /// instance's file, each read under that file's own lock.
    ///
    /// The **survey** half of the torn-line removal. Its caller is
    /// `ledger_repair`'s `purge_unpaired_credits`, and the ordering between this read and the
    /// caller's ledger read is what makes the pairing judgement sound rather than a race: a
    /// credit is written only after its ledger entry is durable, so a credit in the content
    /// read here had its entry on disk before this read, hence before the ledger read that
    /// follows it. See that function for the full argument.
    ///
    /// A file this call cannot list, lock, read or parse contributes **nothing**, so a credit it
    /// holds is KEPT. That is the direction that leaves the incumbent over-count standing rather
    /// than removing a credit on no evidence: an over-count fires staging eviction early, a
    /// wrongly removed credit fires read-tier eviction late.
    pub(crate) async fn credits_naming(&self, ranges: &[PendingRange]) -> Vec<PendingLine> {
        let mut out: Vec<PendingLine> = Vec::new();
        if ranges.is_empty() {
            return out;
        }
        let files = match self.files() {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "Pending-credit survey could not list the pending directory: {}. No credit is removed, so a stale one stays counted rather than a live one being dropped.",
                    e
                );
                return out;
            }
        };
        for path in files {
            let instance = instance_of(&path);
            let lock_path = Self::lock_path_for(&path);
            let path_for_task = path.clone();
            let read = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_path)?;
                Self::lock_with_bound(&lock_file, PENDING_LOCK_WAIT_BOUND, &lock_path)?;
                let result = match std::fs::read_to_string(&path_for_task) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
                    other => other,
                };
                let _ = fs4::FileExt::unlock(&lock_file);
                result
            })
            .await;
            let content = match read {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    warn!(
                        "Pending-credit survey could not read {:?}: {}. Its credits are left in place.",
                        path, e
                    );
                    continue;
                }
                Err(e) => {
                    log_pending_read_join_failure(&path, &e);
                    continue;
                }
            };
            for line in content.lines() {
                let Ok(credit) = serde_json::from_str::<PendingCredit>(line) else {
                    continue;
                };
                // A credit with no range identity is written by a build that predates the field
                // and names no range, so it can never be judged unpaired here.
                if credit.range.as_ref().is_some_and(|r| ranges.contains(r)) {
                    out.push(PendingLine {
                        instance: instance.clone(),
                        seq: credit.seq,
                        line: line.to_string(),
                    });
                }
            }
        }
        out
    }

    /// Drop the **verbatim** lines in `lines` from each named instance's file, under that file's
    /// own lock, read-filter-rename inside one hold. Returns how many lines were dropped.
    ///
    /// # Why byte-exact text and not `(instance, seq)`
    ///
    /// The caller judged these lines against a ledger read taken after the survey that produced
    /// them, so its verdict applies to the lines it saw and to nothing else. Matching on text
    /// makes that scope explicit: a line appended between the survey and this call is not in the
    /// set and is kept, whatever it says. A line byte-identical to a surveyed one cannot be a
    /// later append, because `Ledger_Seq` is monotone per instance and is serialised into the
    /// record.
    ///
    /// Unparseable lines are never in the set, so this call cannot discard one — the same
    /// containment [`Self::gc`] keeps.
    pub(crate) async fn drop_lines(&self, lines: &BTreeMap<String, BTreeSet<String>>) -> usize {
        let mut dropped_total = 0usize;
        for (instance, targets) in lines {
            if targets.is_empty() {
                continue;
            }
            let path = self.path_for(instance);
            if !path.exists() {
                continue;
            }
            let lock_path = Self::lock_path_for(&path);
            let targets = targets.clone();
            let path_for_task = path.clone();
            let task = tokio::task::spawn_blocking(move || -> std::io::Result<usize> {
                use std::io::Write;
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_path)?;
                Self::lock_with_bound(&lock_file, PENDING_LOCK_WAIT_BOUND, &lock_path)?;
                let result = (|| -> std::io::Result<usize> {
                    // Read, filter and rename inside one hold of the lock an append takes, so an
                    // append lands wholly before the read or wholly after the rename — the shape
                    // `gc` and `retire_identities` had to be corrected to.
                    let content = std::fs::read_to_string(&path_for_task)?;
                    let mut kept = String::new();
                    let mut dropped = 0usize;
                    for line in content.lines() {
                        if targets.contains(line) {
                            dropped += 1;
                            continue;
                        }
                        kept.push_str(line);
                        kept.push('\n');
                    }
                    if dropped == 0 {
                        return Ok(0);
                    }
                    let tmp = path_for_task.with_extension("pending.tmp");
                    {
                        let mut f = std::fs::File::create(&tmp)?;
                        f.write_all(kept.as_bytes())?;
                        f.sync_all()?;
                    }
                    std::fs::rename(&tmp, &path_for_task)?;
                    Ok(dropped)
                })();
                let _ = fs4::FileExt::unlock(&lock_file);
                result
            })
            .await;
            match task {
                Ok(Ok(n)) => dropped_total += n,
                Ok(Err(e)) => warn!(
                    "Pending-credit removal could not rewrite {:?}: {}. The stale credit stays counted, which over-states the staged figure rather than under-stating it.",
                    path, e
                ),
                Err(e) => warn!("Pending-credit removal task failed for {:?}: {}", path, e),
            }
        }
        dropped_total
    }
}

/// One pending-credit line a survey matched: whose file held it, its `Ledger_Seq`, and the
/// verbatim text.
///
/// The text is carried because the removal matches on it byte for byte — see
/// [`PendingStore::drop_lines`] for why that is the scope-correct key rather than
/// `(instance, seq)`.
pub(crate) struct PendingLine {
    /// The instance whose `.pending` file the line was read from.
    pub(crate) instance: String,
    /// The credit's `Ledger_Seq`, which is what pairs it with a ledger entry.
    pub(crate) seq: u64,
    /// The line exactly as it sits in the file, newline excluded.
    pub(crate) line: String,
}

/// Retires dead instances' `.pending` files for the journal consolidator's startup
/// reaper, reading the shard checkpoints at most once and only when a file holds credits.
pub(crate) struct PendingReaper {
    cache_dir: PathBuf,
    checkpoints: Option<Option<ShardCheckpointSet>>,
}

impl PendingReaper {
    pub(crate) fn new(cache_dir: &Path) -> Self {
        Self {
            cache_dir: cache_dir.to_path_buf(),
            checkpoints: None,
        }
    }

    /// [`PendingStore::retire_if_nothing_uncut`] for `path`. With `empty_only`, only an empty
    /// file is removed and no checkpoint is read.
    pub(crate) fn retire(&mut self, path: &Path, empty_only: bool) -> std::io::Result<Option<u64>> {
        let non_empty = std::fs::metadata(path)
            .map(|m| m.len() > 0)
            .unwrap_or(false);
        let set = if empty_only || !non_empty {
            None
        } else {
            let dir = &self.cache_dir;
            self.checkpoints
                .get_or_insert_with(|| {
                    Some(read_shard_checkpoints(dir)).filter(|s| s.is_complete())
                })
                .as_ref()
        };
        PendingStore::retire_if_nothing_uncut(path, set, empty_only)
    }
}

impl PendingStore {
    /// Remove `path` (a `{instance}.pending`) and its `.pending.lock` if no fold can still
    /// need it, under the same lock every append, read and GC takes. Returns the removed
    /// file's byte length, or `None` when it was kept.
    ///
    /// Removable when, read under the lock, the file is empty, or (unless `empty_only`) every
    /// line parses and the `staged_fold_cut` its own shard's checkpoint records for the file's
    /// instance covers its seq — the same per-shard judgement [`Self::gc`] drops a record by.
    /// A record whose shard has no checkpoint, an unparseable line, or `checkpoints` being
    /// `None` keeps the file.
    ///
    /// The `.pending` is unlinked before its lock file, both while the lock is held: an
    /// appender that opened the old lock inode waits on it and then re-creates the `.pending`
    /// after the removal, and one that opens the lock path after it is unlinked finds the
    /// `.pending` already gone. Either way no credit is lost.
    pub(crate) fn retire_if_nothing_uncut(
        path: &Path,
        checkpoints: Option<&ShardCheckpointSet>,
        empty_only: bool,
    ) -> std::io::Result<Option<u64>> {
        let instance = instance_of(path);
        let lock_path = Self::lock_path_for(path);
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        Self::lock_with_bound(&lock_file, PENDING_LOCK_WAIT_BOUND, &lock_path)?;
        let result = (|| -> std::io::Result<Option<u64>> {
            let content = match std::fs::read_to_string(path) {
                Ok(c) => c,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
                Err(e) => return Err(e),
            };
            let removable = if content.is_empty() {
                true
            } else if empty_only {
                false
            } else {
                match checkpoints {
                    None => false,
                    Some(set) => content.lines().all(|line| {
                        match serde_json::from_str::<PendingCredit>(line) {
                            Ok(credit) => set
                                .get(&credit.shard())
                                .and_then(|c| c.staged_fold_cut.get(&instance))
                                .is_some_and(|cut| credit.seq <= *cut),
                            Err(_) => false,
                        }
                    }),
                }
            };
            if !removable {
                return Ok(None);
            }
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            match std::fs::remove_file(&lock_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            Ok(Some(content.len() as u64))
        })();
        let _ = fs4::FileExt::unlock(&lock_file);
        result
    }
}

/// Log a pending-credit read whose blocking task did not complete.
///
/// A task **cancelled** at runtime shutdown is expected and logs at DEBUG: recorded 38 such
/// WARNs on one proxy 0.5 s before a restart. A **panic**, or any other cause, stays at WARN.
/// The caller counts the file as unreadable either way; only the level depends on the cause.
fn log_pending_read_join_failure(path: &Path, e: &tokio::task::JoinError) {
    if e.is_cancelled() {
        debug!(
            "Pending-credit read task cancelled for {:?} (expected at runtime shutdown): {}",
            path, e
        );
    } else {
        warn!("Pending-credit read task failed for {:?}: {}", path, e);
    }
}

/// `{instance}` from `{instance}.pending`.
fn instance_of(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// The `(bucket, l1)` shard of a cache key, per design § 3.1.
///
/// Goes through `disk_cache::parse_cache_key` and the same BLAKE3-over-object-key that
/// `get_sharded_path` uses, so a pending record's shard and its `.meta`'s directory can
/// never disagree.
pub fn shard_of(cache_key: &str) -> Option<(String, String)> {
    let (bucket, object_key) = crate::disk_cache::parse_cache_key(cache_key).ok()?;
    let hash = blake3::hash(object_key.as_bytes());
    let hex = hash.to_hex();
    Some((bucket.to_string(), hex.as_str()[0..2].to_string()))
}

/// Exactly two lowercase hex digits — the `XX` an `l1` component must be.
///
/// The inverse test of [`shard_of`]'s second return value, which is why it lives beside it:
/// anything enumerating shards from directory names on disk must accept exactly the set
/// `shard_of` can produce, or a stray directory becomes a shard no cache key can ever map to.
///
/// `ledger_repair.rs` carries a private copy under the same name. It is duplicated rather than
/// shared because that file belongs to another unit's write boundary; pointing it at this one
/// is a one-line change and is named as owed.
pub(crate) fn is_shard_level(name: &str) -> bool {
    name.len() == 2
        && name
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// One shard's § 3.2 checkpoint: the figure, the count behind it, its own age, and the cut
/// that explains it — all for the `(bucket, L1)` shard this document names.
///
/// # Why these fields cannot be installed separately
///
/// The figure and the cut are two halves of one statement: "these bytes are the `.meta`
/// fold of everything **in this shard** at or below this cut". Install a new figure against
/// an old cut and the pending term re-adds credits the new figure already counted — a double
/// count. Install a new cut against an old figure and the pending term filters out credits
/// the old figure never counted — a lost credit, which is the direction that makes an
/// eviction trigger fire **late**. Both failures are silent, because either way the
/// published number is plausible.
///
/// The mechanism that makes them inseparable is that they are fields of **one serialized
/// document**, written to a temp file, `sync_all`'d, and renamed over the previous one in
/// a single [`Self::install`]. There is no code path that writes one without the other,
/// and a failed write leaves the previous document intact — which is a retain of both, by
/// construction rather than by a compensating action. Do not split this into two files,
/// two writes, or two atomics.
///
/// # How that argument survives being per-shard, and what it stops covering
///
/// **Unchanged for one shard.** Partitioning multiplied the number of documents; it did not
/// change what one document is. The figure and the cut it explains are still fields of one
/// file written `tmp` → `sync_all` → `rename`, so the argument above applies verbatim, per
/// shard, and "complete-or-retain" is still by construction.
///
/// **It never covered the global sum, and now it visibly does not.** The published figure is
/// a sum across N documents, and N atomic writes are not one atomic write — there is no
/// instant at which the sum is atomic, and § 3.6/§ 3.7 explicitly want it that way (a
/// mixed-age sum of independently-refreshed shards). So the single-document argument is
/// **replaced, for the global figure, by two separate properties**:
///
/// 1. **No partial shard figure can be read**, because `rename` is atomic: a reader sees
///    either the previous complete document or the new complete one, never a blend. The
///    per-instance temp suffix (`Shared::Yes`, § 2 trap 6) is what keeps two instances from
///    sharing one temp file and producing a torn one.
/// 2. **A shard that cannot be read makes the sum unknown, not short.**
///    [`read_shard_checkpoints`] reports completeness and
///    [`collect_staged_fold_gauges`] publishes `null` rather than a sum missing a shard.
///    Mixed *age* is permitted; mixed *membership* is not, because a sum silently missing a
///    shard is indistinguishable from a smaller cache.
///
/// Mixed age is then bounded by a stated criterion rather than left open —
/// `staged_shard_authority_age_secs` is the figure § 16.6.4's `Max_Authority_Age` is set on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedShardCheckpoint {
    /// Schema version. An unknown version is treated as unreadable rather than ignored.
    pub schema: u32,

    /// The bucket this shard's `(bucket, L1)` pair names.
    pub bucket: String,

    /// The `XX` this shard's `(bucket, L1)` pair names.
    pub l1: String,

    /// `F` for this shard: Σ `staged_compressed_size()` over verified `.meta`s in this shard
    /// that the ledger indexed.
    ///
    /// **§ 3.2's existing field, reused rather than paralleled.** § 3.2's record already
    /// carries `staged_byte_sum` as a subset of `byte_sum`; the three siblings below join it
    /// in the same document, per § 16.6.4's field table. Adding a second staged byte figure
    /// beside it would create two producers of one number.
    pub staged_byte_sum: u64,

    /// The staged range count behind `staged_byte_sum`, for this shard.
    ///
    /// **This is the figure [`STAGED_RANGE_BUDGET`] is enforced against**, summed over shards
    /// and added to the post-cut pending count. It is a count and not a byte figure on
    /// purpose: the fold's cost is per-`.meta`-parse, so the count is what bounds authority
    /// age, and `staged_byte_sum` is the wrong figure to gate on however closely the two
    /// normally track. `the_budget_gates_on_the_count_not_on_the_byte_sum` constructs the
    /// state where they disagree.
    pub staged_range_count: u64,

    /// When this shard's staged figure was folded, nanoseconds since the epoch.
    ///
    /// **Deliberately not `scanned_at`.** § 3.2's `scanned_at` belongs to the `st_size` fold,
    /// which visits the same shard on its own hourly cadence and installs `entry_count`,
    /// `range_count`, `byte_sum` and the histogram; this field belongs to the staged fold,
    /// which visits on its own cadence and parses `.meta`s. The two figures in one document
    /// therefore have two genuinely different ages, and § 3.7 requires both be published
    /// rather than one freshness implied for both — so this is the age
    /// `staged_shard_authority_age_secs` is computed from, and § 3.2's `scanned_at` is
    /// untouched by this fold. Reusing one timestamp would make the staged figure look as
    /// fresh as the last readdir pass.
    pub staged_scanned_at: u64,

    /// Which instance folded it.
    pub staged_scanned_by: String,

    /// The monotone per-instance `Ledger_Seq` cut, keyed by `instance_id`.
    ///
    /// Monotone at two independent levels, which is deliberate rather than redundant:
    ///
    /// - **The coordinate itself** is a counter, not a clock
    ///   ([`crate::write_ledger::WriteLedgerEntry::seq`]), so a newly issued seq is always
    ///   strictly greater than every seq that instance has issued before. This is what
    ///   makes a fresh credit reliably land *above* the cut — the property the whole
    ///   exactly-once argument rests on, and the one a `SystemTime` coordinate could not
    ///   provide.
    /// - **The observation** is `cut[i] := max(previous cut[i], observed max_seq for i)`.
    ///   The file's own `max_seq_ever` header already stops compaction lowering the
    ///   observation, so this max is the second line: it also covers a file that has been
    ///   removed entirely, or a peer's read racing this one.
    ///
    /// A per-instance map rather than a scalar because the files are per-instance: one
    /// instance's counter says nothing about another's, and a global scalar cut would filter
    /// a peer's fresh credit against this instance's counter.
    ///
    /// Per **shard** as well as per instance from this unit onward, because a shard's figure
    /// only explains credits in that shard.
    pub staged_fold_cut: BTreeMap<String, u64>,

    /// Every other field of § 3.2's record, carried through untouched.
    ///
    /// § 3.2's checkpoint is **shared between two producers**. The staged fold owns the four
    /// fields above; **nine** further fields are not its, and it must carry every one of them
    /// through untouched:
    ///
    /// `entry_count`, `range_count`, `byte_sum`, `histogram`, `tail_floor`, `tail_entries`,
    /// `orphan_bytes_seen`, `scanned_at`, `scanned_by`.
    ///
    /// **Do not shorten that list, and do not read it as a producer list.** An earlier version
    /// of this comment called all nine "owned by the `st_size` fold", which conflated two
    /// different questions. The `st_size` fold
    /// ([`crate::st_size_fold::install_fold_fields`]) writes **six** of them
    /// (`entry_count`, `range_count`, `byte_sum`, `histogram`, `scanned_at`, `scanned_by`)
    /// and deliberately leaves three absent:
    /// `tail_floor` and `tail_entries` describe a partition § 4 writes, and
    /// `orphan_bytes_seen` is defined as a readdir **diffed against the parse**, which a
    /// `stat`-only fold cannot compute. What matters here is the nine, because this fold must
    /// preserve a field regardless of which other pass produced it.
    ///
    /// A plain serde struct naming only this fold's fields would silently drop the others on
    /// every install — a data loss with no error and no test failure, because the document
    /// would still parse. So unknown fields are captured and re-emitted verbatim.
    ///
    /// Preserving them across a **rename** is necessary and not sufficient: both producers do
    /// a read-modify-write of one file, so the live values must be re-read under
    /// [`lock_checkpoint_document`] at install time rather than carried from a read taken at
    /// the start of the pass. See [`install_preserving_foreign_fields`].
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

impl StagedShardCheckpoint {
    /// `{cache_dir}/metadata/_shard_state/checkpoints/{bucket}/{XX}.json`.
    pub fn path(cache_dir: &Path, shard: &Shard) -> PathBuf {
        checkpoints_dir(cache_dir)
            .join(&shard.bucket)
            .join(format!("{}.json", shard.l1))
    }

    /// The shard this document names.
    pub fn shard(&self) -> Shard {
        Shard::new(self.bucket.clone(), self.l1.clone())
    }

    /// A document for a shard **this fold** has not visited, for the *other* producer to fill
    /// its own fields into.
    ///
    /// # The staged zeros here are explicitly "not folded", not "folded and found empty"
    ///
    /// `empty()`'s own rule still holds — an installed zero is a claim — and this constructor
    /// does not break it, because the only production caller is the `st_size` fold, which
    /// installs a shard's **own** folded figures into `other` and needs somewhere to put them
    /// when no staged fold has visited that shard yet.
    ///
    /// `staged_scanned_at` is left at **0**, deliberately and not as a placeholder. `age_secs`
    /// of 0 is an enormous age, so such a shard reads as far beyond
    /// [`MAX_AUTHORITY_AGE_SECS`] in `ShardCheckpointSet::stalest_age_secs` and is counted by
    /// `shards_beyond_max_age`. The staged authority is therefore **stale**, which is the
    /// conservative direction: a decision that needs a fresh staged figure declines, rather
    /// than reading a fresh-looking zero and concluding the shard holds no write-cache bytes.
    /// Stamping `now_nanos()` here would produce exactly that fresh-looking zero.
    ///
    /// The staged fold's own selection includes every shard with an existing checkpoint, so a
    /// document created here becomes a candidate it will visit and fill.
    pub fn unfolded_for(shard: &Shard) -> Self {
        Self {
            schema: STAGED_FOLD_SCHEMA,
            bucket: shard.bucket.clone(),
            l1: shard.l1.clone(),
            staged_byte_sum: 0,
            staged_range_count: 0,
            staged_scanned_at: 0,
            staged_scanned_by: String::new(),
            staged_fold_cut: BTreeMap::new(),
            other: serde_json::Map::new(),
        }
    }

    /// An empty document for a shard no fold has visited. Test fixtures only: production
    /// never installs a document it did not fold, because an installed zero is a claim.
    #[cfg(test)]
    fn empty(shard: &Shard) -> Self {
        Self::unfolded_for(shard)
    }

    /// Read the installed checkpoint.
    ///
    /// Three outcomes, deliberately distinct: `Ok(None)` means no fold has ever completed
    /// here, `Ok(Some(..))` is the installed document, and `Err` means a document exists
    /// and could not be read — which must not be reported as "no fold yet", because that
    /// would abandon a cut that is still on disk.
    pub fn load(cache_dir: &Path, shard: &Shard) -> std::io::Result<Option<Self>> {
        Self::load_path(&Self::path(cache_dir, shard))
    }

    fn load_path(path: &Path) -> std::io::Result<Option<Self>> {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let parsed: Self = serde_json::from_str(&content).map_err(|e| {
            std::io::Error::other(format!("unparseable staged fold checkpoint: {e}"))
        })?;
        if parsed.schema != STAGED_FOLD_SCHEMA {
            return Err(std::io::Error::other(format!(
                "unknown staged fold checkpoint schema {}",
                parsed.schema
            )));
        }
        Ok(Some(parsed))
    }

    /// Install this document atomically: temp file, `sync_all`, rename.
    ///
    /// See the type doc for why this is one write rather than two.
    pub fn install(&self, cache_dir: &Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir, &self.shard());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::other(format!("serialize staged fold checkpoint: {e}")))?;
        let tmp = path.with_extension(format!(
            "json.tmp.{}.{}",
            hostname_for_tmp(),
            std::process::id()
        ));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }
}

/// Take the per-document advisory lock protecting one shard checkpoint's read-modify-write.
///
/// # Why this exists, and why it is not `global_consolidation.lock`
///
/// [`StagedShardCheckpoint`] has **two producers**: this fold owns four struct fields and the
/// `st_size` fold owns six keys inside `other`. Each must read the current document and write
/// it back whole, so two unsynchronised read-modify-writes silently lose one producer's
/// update. The atomic rename each install performs stops a reader seeing a *torn* document; it
/// does nothing about a lost update, and the loser's write is a complete, parseable, wrong
/// document with no error anywhere.
///
/// The lock is per document because that is the granularity of the race.
/// `JournalConsolidator::try_acquire_global_consolidation_lock` is unsuitable for three
/// separate reasons — it returns `Ok(false)` when this instance already holds it, so it is not
/// re-entrant; it is a method on the consolidator, and this install site has no consolidator
/// handle; and a fleet-wide singleton would serialise a whole-volume traversal against all
/// consolidation. See `crate::st_size_fold::install_fold_fields` for the same reasoning from
/// the other producer's side.
///
/// # Best-effort, and that is a decision
///
/// A failure to acquire returns `None` and the caller proceeds unlocked. The lock closes a
/// lost-update window between two maintenance passes on different cadences; refusing to
/// install without it would convert a rare lost update into a shard that never gets a figure
/// at all, which is the worse of the two. `flock` cannot go stale — the kernel releases it on
/// process death — so there is no stale-lock branch.
pub(crate) fn lock_checkpoint_document(cache_dir: &Path, shard: &Shard) -> Option<std::fs::File> {
    let path = StagedShardCheckpoint::path(cache_dir, shard).with_extension("json.lock");
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            debug!(
                "Shard checkpoint lock: could not create {:?}: {}. Proceeding unlocked.",
                parent, e
            );
            return None;
        }
    }
    let file = match std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
    {
        Ok(f) => f,
        Err(e) => {
            debug!(
                "Shard checkpoint lock: could not open {:?}: {}. Proceeding unlocked.",
                path, e
            );
            return None;
        }
    };
    match fs4::FileExt::lock(&file) {
        Ok(()) => Some(file),
        Err(e) => {
            debug!(
                "Shard checkpoint lock: could not lock {:?}: {}. Proceeding unlocked.",
                path, e
            );
            None
        }
    }
}

/// Install a staged-fold checkpoint, taking the foreign fields from a read under the lock.
///
/// # Why the foreign fields are re-read here rather than carried from the pass's own read
///
/// The pass reads every checkpoint once at its start, and that read is minutes old by the time
/// a late shard installs. The `st_size` fold writes its own keys into the same documents on its
/// own cadence, so a pass that wrote back the `other` map it read at the start would silently
/// revert every `st_size` field written in between. Re-reading under the lock makes the whole
/// operation a genuine read-modify-write rather than a write of stale state.
///
/// The `other` map already on `checkpoint` is the FALLBACK, used only when the re-read finds no
/// document or cannot read one — in which case the pass's own earlier copy is the best
/// available, and is what the code did before this fold existed.
fn install_preserving_foreign_fields(
    cache_dir: &Path,
    mut checkpoint: StagedShardCheckpoint,
) -> std::io::Result<()> {
    let fallback_other = checkpoint.other.clone();
    // Held until the end of the function, which is after the install.
    let _lock = lock_checkpoint_document(cache_dir, &checkpoint.shard());
    checkpoint.other = match StagedShardCheckpoint::load(cache_dir, &checkpoint.shard()) {
        Ok(Some(current)) => current.other,
        Ok(None) => fallback_other,
        Err(e) => {
            warn!(
                "Staged fold: could not re-read shard {}'s checkpoint under the document lock \
                 ({}); carrying the foreign fields this pass read earlier, which may revert a \
                 concurrent st_size fold's figures for this shard.",
                checkpoint.shard(),
                e
            );
            fallback_other
        }
    };
    checkpoint.install(cache_dir)
}

/// Hostname component for a shared-volume temp path, matching the `Shared::Yes` scheme
/// used elsewhere so two instances cannot collide on one temp file.
fn hostname_for_tmp() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
        .replace(['/', '\\'], "_")
}

/// `{cache_dir}/metadata/_shard_state/checkpoints`.
fn checkpoints_dir(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(CHECKPOINTS_DIR)
}

/// `{cache_dir}/metadata/_shard_state/claims`.
fn claims_dir(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(CLAIMS_DIR)
}

/// Every shard checkpoint on the volume, with completeness.
///
/// The global figure is a sum over these, and the reason this type reports completeness
/// separately from the sum is the second half of [`StagedShardCheckpoint`]'s atomicity
/// argument: mixed **age** across shards is permitted and expected, mixed **membership** is
/// not, because a sum quietly missing one shard is indistinguishable from a smaller cache.
#[derive(Debug, Clone, Default)]
pub struct ShardCheckpointSet {
    by_shard: BTreeMap<Shard, StagedShardCheckpoint>,
    /// Checkpoint files the directory walk named.
    pub files_expected: usize,
    /// Files parsed without error.
    pub files_read: usize,
    /// Files that could not be opened, parsed, or carried an unknown schema.
    pub files_unreadable: usize,
}

impl ShardCheckpointSet {
    /// Whether every checkpoint that exists was read.
    pub fn is_complete(&self) -> bool {
        self.files_unreadable == 0 && self.files_read == self.files_expected
    }

    /// One shard's document, if a fold has installed one.
    pub fn get(&self, shard: &Shard) -> Option<&StagedShardCheckpoint> {
        self.by_shard.get(shard)
    }

    /// Every installed document.
    pub fn iter(&self) -> impl Iterator<Item = (&Shard, &StagedShardCheckpoint)> {
        self.by_shard.iter()
    }

    /// Shards with an installed document.
    pub fn shards(&self) -> BTreeSet<Shard> {
        self.by_shard.keys().cloned().collect()
    }

    /// Σ `staged_byte_sum` over shards — `F`.
    pub fn staged_byte_sum(&self) -> u64 {
        self.by_shard
            .values()
            .fold(0u64, |acc, c| acc.saturating_add(c.staged_byte_sum))
    }

    /// Σ `staged_range_count` over shards — the figure the budget is enforced against,
    /// before the pending term is added.
    pub fn staged_range_count(&self) -> u64 {
        self.by_shard
            .values()
            .fold(0u64, |acc, c| acc.saturating_add(c.staged_range_count))
    }

    /// `now − staged_scanned_at` of the **stalest** shard, seconds. `None` when no shard has
    /// a figure.
    ///
    /// This is the criterion figure: § 16.6.4 sets `Max_Authority_Age` on the maximum over
    /// shards, because an authority is only as fresh as its stalest component.
    pub fn stalest_age_secs(&self) -> Option<u64> {
        self.by_shard
            .values()
            .map(|c| age_secs(c.staged_scanned_at))
            .max()
    }

    /// `now − staged_scanned_at` of the **freshest** shard, seconds. `None` when no shard has
    /// a figure.
    ///
    /// "How long since the fold last got anything done", which is the question I2's
    /// `staged_authority_age_secs` asked of a single document.
    pub(crate) fn freshest_age_secs(&self) -> Option<u64> {
        self.by_shard
            .values()
            .map(|c| age_secs(c.staged_scanned_at))
            .min()
    }

    /// Shards whose figure is older than [`MAX_AUTHORITY_AGE_SECS`].
    ///
    /// § 16.6.7's **Stale but valid** row shows "the number, plus its age and the count of
    /// shards beyond the bound". `stalest_age_secs` gives the age; this gives the count, and
    /// the two answer different questions: one stale shard out of 256 is a scheduling hiccup,
    /// 200 out of 256 is a rotation that cannot keep up.
    pub(crate) fn shards_beyond_max_age(&self) -> u64 {
        self.by_shard
            .values()
            .filter(|c| age_secs(c.staged_scanned_at) > MAX_AUTHORITY_AGE_SECS)
            .count() as u64
    }
}

/// Read every shard checkpoint under `checkpoints/{bucket}/{XX}.json`.
///
/// **`.json` only.** An in-flight install writes `{XX}.json.tmp.{host}.{pid}` beside the
/// document and renames over it, so a temp file is a half-written figure by definition and
/// including one in the sum would be exactly the "partial shard figure" § 16.6.4 forbids.
/// `a_partial_shard_figure_never_enters_the_global_sum` plants one and asserts it is ignored.
pub fn read_shard_checkpoints(cache_dir: &Path) -> ShardCheckpointSet {
    #[cfg(test)]
    note_shard_checkpoint_read(cache_dir);
    let root = checkpoints_dir(cache_dir);
    let mut set = ShardCheckpointSet::default();
    let buckets = match std::fs::read_dir(&root) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return set,
        Err(e) => {
            warn!(
                "Staged fold: could not list the checkpoint directory {:?}: {}. The global \
                 staged figure is unknown rather than zero.",
                root, e
            );
            set.files_expected = 1;
            set.files_unreadable = 1;
            return set;
        }
    };
    for bucket_entry in buckets.flatten() {
        let bucket_path = bucket_entry.path();
        if !bucket_path.is_dir() {
            continue;
        }
        let shards = match std::fs::read_dir(&bucket_path) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Staged fold: could not list checkpoints in {:?}: {}",
                    bucket_path, e
                );
                set.files_expected += 1;
                set.files_unreadable += 1;
                continue;
            }
        };
        for shard_entry in shards.flatten() {
            let path = shard_entry.path();
            // `extension() == "json"` is not enough on its own for a name like
            // `3f.json.tmp.host.1`, whose extension is `1` — but a name ending `.json` is
            // exactly the document, and a temp name never does.
            if !path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(".json"))
            {
                continue;
            }
            set.files_expected += 1;
            match StagedShardCheckpoint::load_path(&path) {
                Ok(Some(c)) => {
                    set.files_read += 1;
                    set.by_shard.insert(c.shard(), c);
                }
                // Vanished between the listing and the read — a peer's rename cannot produce
                // this, but a cleanup could. Counted as unreadable so the sum is unknown
                // rather than short.
                Ok(None) | Err(_) => {
                    set.files_unreadable += 1;
                    warn!(
                        "Staged fold: shard checkpoint {:?} could not be read; the global \
                         staged figure is unknown rather than short by one shard",
                        path
                    );
                }
            }
        }
    }
    set
}

/// A § 16.2 shard claim: an **efficiency** mechanism and never load-bearing for correctness.
///
/// R19.2 is normative here and is the sentence to hold onto: a lost, expired, or duplicated
/// claim costs duplicated work and can never produce a wrong figure, because the figure is an
/// absolute replacement installed atomically per shard. Any later change that makes a claim
/// load-bearing has broken the design rather than tightened it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardClaim {
    /// The instance that wrote it.
    pub instance_id: String,
    /// Seconds since the epoch when it was written.
    pub claimed_at: u64,
    /// Lease length, seconds. Not extended on progress: § 16.2 records that a stuck instance
    /// holding a claim indefinitely is worse than a duplicated visit.
    pub lease_secs: u64,
}

impl ShardClaim {
    fn path(cache_dir: &Path, shard: &Shard) -> PathBuf {
        claims_dir(cache_dir)
            .join(&shard.bucket)
            .join(format!("{}.{}", shard.l1, CLAIM_EXT))
    }

    fn is_live_at(&self, now_secs: u64) -> bool {
        self.claimed_at.saturating_add(self.lease_secs) > now_secs
    }
}

/// The outcome of trying to claim a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This instance holds the claim for the lease.
    Claimed,
    /// A peer holds a live claim, or won the write-then-re-read race. Pick another shard.
    Busy,
}

/// § 16.2's claim protocol, unchanged: read, check the lease, write atomically with an
/// instance-suffixed temp, re-read, and require the re-read to name self.
///
/// **Write-then-re-read, not `flock`.** Two instances racing produce one winner and one
/// `Busy`; a torn or lost claim degrades to both visiting the shard, which R19.2 permits. A
/// lock here would add a failure mode (a held lock on a crashed instance) to buy a guarantee
/// the design does not need. The temp name carries hostname and pid — § 2's trap 6 — because
/// the claim is a shared file and two racing writes through one temp path produce one
/// truncated file.
///
/// An I/O error is `Busy`, not `Claimed`: failing to claim costs a skipped shard this pass,
/// whereas assuming a claim on an unverified write is the one outcome that could let two
/// instances believe they are exclusive. They may both visit — that is fine — but neither may
/// believe otherwise.
pub(crate) async fn claim_shard(
    cache_dir: &Path,
    shard: &Shard,
    instance_id: &str,
) -> ClaimOutcome {
    let path = ShardClaim::path(cache_dir, shard);
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    if let Ok(content) = std::fs::read_to_string(&path) {
        if let Ok(existing) = serde_json::from_str::<ShardClaim>(&content) {
            if existing.instance_id != instance_id && existing.is_live_at(now_secs) {
                return ClaimOutcome::Busy;
            }
        }
        // An unparseable claim is treated as absent: it cannot be establishing a peer's
        // exclusivity, and the write below replaces it.
    }

    let claim = ShardClaim {
        instance_id: instance_id.to_string(),
        claimed_at: now_secs,
        lease_secs: CLAIM_LEASE_SECS,
    };
    let body = match serde_json::to_string(&claim) {
        Ok(b) => b,
        Err(e) => {
            debug!("Staged fold: could not serialize a shard claim: {}", e);
            return ClaimOutcome::Busy;
        }
    };
    let write = {
        let path = path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let tmp = path.with_file_name(format!(
                "{}.tmp.{}.{}",
                path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "claim".to_string()),
                hostname_for_tmp(),
                std::process::id()
            ));
            {
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(body.as_bytes())?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, &path)
        })
        .await
    };
    match write {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            debug!("Staged fold: could not write claim for {}: {}", shard, e);
            return ClaimOutcome::Busy;
        }
        Err(e) => {
            debug!("Staged fold: claim write task failed for {}: {}", shard, e);
            return ClaimOutcome::Busy;
        }
    }

    // The re-read. Last-writer-wins resolution, NOT a lock.
    match std::fs::read_to_string(&path)
        .ok()
        .and_then(|c| serde_json::from_str::<ShardClaim>(&c).ok())
    {
        Some(seen) if seen.instance_id == instance_id => ClaimOutcome::Claimed,
        _ => ClaimOutcome::Busy,
    }
}

/// Give up this instance's claim on `shard` once its visit has installed, if the claim on disk
/// is still this instance's.
///
/// **Compare, then delete, under the claim protocol's own atomicity — which is none.** The
/// claim is read and parsed, and unlinked only if it names this instance. There is no lock
/// between the read and the unlink, for the same reason [`claim_shard`] takes none: R19.2 makes
/// every lost race here a duplicated visit, never a wrong figure. A peer writes over a claim
/// only when it is expired or unparseable, so within the lease the only way the unlink removes a
/// peer's claim is clock skew beyond the lease, and the cost of that is the peer's shard being
/// visited twice. A claim that names a peer — one that expired and was taken while this visit
/// ran — is left alone. Any I/O error leaves the claim to expire, which is what happened to
/// every claim before this existed.
///
/// What the lease protected after the install, and why that is not worth keeping: only against a
/// peer re-visiting a shard that has just been folded. The rotation already avoids that — it is
/// stalest-first and a just-installed shard is the freshest on the volume — so the lease only
/// ever changed the outcome for a peer that ranked the shard ahead of the rotation, i.e. a peer
/// holding it dirty, which is exactly the visit that should happen.
fn release_claim(cache_dir: &Path, shard: &Shard, instance_id: &str) {
    let path = ShardClaim::path(cache_dir, shard);
    let ours = std::fs::read_to_string(&path)
        .ok()
        .and_then(|c| serde_json::from_str::<ShardClaim>(&c).ok())
        .is_some_and(|c| c.instance_id == instance_id);
    if !ours {
        return;
    }
    if let Err(e) = std::fs::remove_file(&path) {
        debug!(
            "Staged fold: could not release the claim on {} ({}); it expires with its lease",
            shard, e
        );
    }
}

/// Run one fold pass: observe the cut, fold the `.meta`s it covers, and install both — or
/// retain both.
///
/// Ordering is load-bearing and is the ordering § 16.6.1 specifies: the cut is observed
/// **from the same complete, locked read** that yields the candidate list. Observing the
/// cut from a second read would make the figure and the cut describe different instants,
/// which is precisely the disagreement the atomic install exists to prevent.
pub async fn run_staged_fold(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
) -> FoldOutcome {
    // Unbounded, always, in production. See `run_staged_fold_with_cap`.
    run_staged_fold_with_cap(cache_dir, ledger, instance_id, 0).await
}

/// [`run_staged_fold`] with per-visit sampling for the scanner limiter (§ 16.6.13 K2). `None` is
/// exactly [`run_staged_fold`]. The sampler only reads the scanner registry and feeds the
/// limiter; it changes no claim, budget, install or lease.
pub(crate) async fn run_staged_fold_sampled(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
    sampler: Option<&crate::journal_consolidator::VisitSampler<'_>>,
) -> FoldOutcome {
    run_staged_fold_scoped(cache_dir, ledger, instance_id, 0, None, sampler).await
}

/// [`run_staged_fold`] with an explicit ledger read cap.
///
/// `cap = 0` means unbounded and is what production uses, because a capped read sets
/// [`crate::write_ledger::LedgerReadOutcome::truncated`] and truncation is an incomplete
/// input — so a non-zero cap here would make the fold refuse on every ledger longer than
/// the cap, which is precisely the ledger a fold is most needed for.
///
/// The parameter exists so the truncation class of incomplete input is reachable at all.
/// It is otherwise unreachable through `run_staged_fold`, which would leave one branch of
/// the Complete_Install gate asserted by nothing — and this repo's recorded failure mode is
/// an assertion that never runs looking identical to one that passes.
pub(crate) async fn run_staged_fold_with_cap(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
    cap: usize,
) -> FoldOutcome {
    run_staged_fold_scoped(cache_dir, ledger, instance_id, cap, None, None).await
}

/// One fold pass that visits **only** `shards`, stalest first, every one of them if it can.
///
/// The same pass as [`run_staged_fold`] — same gates, same per-shard visit, same install and
/// pending GC — restricted to a named shard set and with the budget set to that set's size.
/// It exists for one caller: staging eviction, which removes objects and then has to decide
/// again from the figure those objects were in.
///
/// # Why eviction needs it and DELETE does not
///
/// The fold never subtracts. A shard's figure changes only when a visit recomputes it from the
/// `.meta`s, and the periodic pass reaches any one shard once per rotation
/// ([`MAX_AUTHORITY_AGE_SECS`]). For a DELETE, a graduation or a re-PUT that lag is an
/// over-statement for at most one rotation, which fires the trigger early — the safe
/// direction. For eviction it is not merely a stale reading: at epoch 1 the byte arm of the
/// next staging pass compares **this** figure against the trigger, finds it exactly as high as
/// before the objects were freed, and evicts again. Measured in
/// `tests/fold_drops_removed_staged_objects_test.rs`: a 25,165,916-byte tier with a
/// 19,922,944-byte trigger was drained to zero by two passes, where one pass had already left
/// it at 12,582,958. Re-folding the evicted objects' shards before the lock is released makes
/// the figure the next pass reads the figure after this one's removals, from the fold itself
/// rather than from a second accounting path beside it.
///
/// A shard a peer holds is skipped (`Busy`), exactly as in the periodic pass; that shard's
/// figure stays pre-eviction until a later visit, which is the residual.
///
/// Spec: cache-eviction-at-scale §§ 16.6.2, 16.6.3.
pub(crate) async fn run_staged_fold_over_shards(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
    shards: &BTreeSet<Shard>,
) -> FoldOutcome {
    if shards.is_empty() {
        return retain(FoldRetainReason::NoShardAvailable);
    }
    run_staged_fold_scoped(cache_dir, ledger, instance_id, 0, Some(shards), None).await
}

/// The body of both entry points. `only = None` is the periodic pass. `sampler = None` samples
/// nothing.
async fn run_staged_fold_scoped(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
    cap: usize,
    only: Option<&BTreeSet<Shard>>,
    sampler: Option<&crate::journal_consolidator::VisitSampler<'_>>,
) -> FoldOutcome {
    // The previous documents first: each shard's cut is what its new cut must be the max
    // against, so an unreadable checkpoint is a refusal rather than a fresh start. Read as a
    // set rather than per shard because selection needs every shard's age anyway (§ 16.3:
    // "selection adds no read that the pass was not making").
    let previous = read_shard_checkpoints(cache_dir);
    if !previous.is_complete() {
        warn!(
            "Staged fold: {} of {} shard checkpoint(s) could not be read. Retaining them \
             rather than installing over cuts we cannot see.",
            previous.files_unreadable, previous.files_expected
        );
        return retain(FoldRetainReason::PreviousCheckpointUnreadable);
    }

    // **A pass reads only the shards it visits.** What is left here at pass scope is the two
    // questions that have no shard-scoped answer; the four per-file incompleteness classes moved
    // into the visit loop, where each refuses one shard instead of the volume. See
    // `FoldRetainReason::LedgerFileUnreadable` and § 16.6.12 point 2.
    //
    // A legacy flat `{instance}.ledger` is pass-scoped because it names **no shard**: its
    // entries cannot be attributed anywhere, so no shard can be told whether its figure is
    // short by them, and therefore no shard may install. One `readdir` of the ledger root.
    match ledger.legacy_ledger_file_count().await {
        Ok(0) => {}
        Ok(_) => return retain(FoldRetainReason::LegacyLedgerPresent),
        Err(e) => {
            warn!("Staged fold: could not list the write ledger: {}", e);
            return retain(FoldRetainReason::LedgerListFailed);
        }
    }
    // The shard enumeration is pass-scoped for the same kind of reason: if the listing failed we
    // do not know which shards exist, so there is nothing to scope a refusal to. `1 + B`
    // `readdir`s and no file reads — this is the term that used to be `O(shards existing)` file
    // reads before the visit loop.
    let ledger_shards = match ledger.ledger_shards().await {
        Ok(s) => s,
        Err(e) => {
            warn!("Staged fold: could not list the write ledger: {}", e);
            return retain(FoldRetainReason::LedgerListFailed);
        }
    };

    let pending = PendingStore::new(cache_dir.to_path_buf());
    let pending_read = pending.read_all().await;
    if !pending_read.is_complete() {
        return retain(FoldRetainReason::PendingUnreadable);
    }

    // `P`. `None` is the UNREADABLE sentinel, not a zero. The guard reads `P` only to refuse
    // installing while it is UNREADABLE, so it needs the composed figure's **readability** and
    // nothing else: the last full walk plus the published per-proxy growth terms. Readability is
    // the base walk's: a composition is `None` exactly when its base walk is, and the growth
    // term can only add to a readable one.
    //
    // It composes on `INSTALL_GUARD_BASE_MAX_AGE` rather than the deciders' bound, so the
    // verdict it refuses to install on is as fresh as the one it used before these documents
    // existed. A readability verdict is still about a moment: three minutes after a readable
    // walk the records may be unreadable, and installing a figure then is the thing this guard
    // exists to stop. The cost is bounded by how often a fold installs, which is once per
    // `shard_fold_interval` (an hour by default) from the background pass and once per
    // staging-eviction decision, which reaches this install through `run_staged_fold_over_shards`
    // in `cache.rs`. Neither is the request rate, and a burst of eviction decisions inside one
    // `IN_PROGRESS_OBSERVATION_REUSE` shares one walk.
    if crate::signed_put_handler::compose_in_progress_part_bytes(
        cache_dir,
        crate::signed_put_handler::INSTALL_GUARD_BASE_MAX_AGE,
    )
    .await
    .bytes
    .is_none()
    {
        return retain(FoldRetainReason::InProgressPartsUnreadable);
    }
    // `Foreign_Write_Detection` (§ 16.6.6). Armed by the consolidation cycle, which is what
    // reads `size_state.json`; checked here because this is where installing happens.
    //
    // A Complete_Install precondition rather than a scheduling decision, deliberately: the
    // inputs above are all "we could not see the state", and this one is "another producer
    // is writing the state". Both make an installed figure a claim this instance is not
    // entitled to make.
    // The reason travels with the refusal so the retain label names which of the three
    // situations occurred. All three refuse; they do not share a remedy.
    if let Some(kind) = crate::staged_authority::authority_block_kind(cache_dir) {
        return retain(match kind {
            crate::staged_authority::AuthorityBlockKind::ForeignPreCWrite => {
                FoldRetainReason::ForeignAuthorityWrite
            }
            crate::staged_authority::AuthorityBlockKind::SizeStateUnreadable => {
                FoldRetainReason::SizeStateUnreadable
            }
            crate::staged_authority::AuthorityBlockKind::FenceReapplyFailed => {
                FoldRetainReason::SchemaFenceUnapplied
            }
        });
    }

    // Selection, § 16.3: the stalest shard first, from figures the checkpoint read already
    // provided. The candidate set is the union of three sources, and all three are needed:
    //
    // - shards with a ledger DIRECTORY — where the staged set is. Read from directory names
    //   rather than from grouped ledger entries, which is what removes the whole-volume read:
    //   the old first term was `by_shard.keys()`, and building it meant opening and parsing
    //   every ledger file on the volume before deciding which shards to visit. The directory
    //   listing is a superset of it (a drained shard keeps a header-only file), and a superset
    //   is what selection wants — an extra visit costs one small read, a missed shard keeps a
    //   stale figure forever;
    // - shards named by a pending credit — a shard whose first credit has not been folded
    //   yet has no checkpoint and no ledger candidate visible to a partial read;
    // - shards with an existing checkpoint — a shard whose staged set has fully DRAINED has
    //   no candidates at all, and without this it would keep its last non-zero figure
    //   forever. That is the one arm whose absence is invisible: every other figure would
    //   look right.
    let mut selection: BTreeSet<Shard> = ledger_shards
        .into_iter()
        .map(|(bucket, l1)| Shard::new(bucket, l1))
        .collect();
    selection.extend(pending_read.shards());
    selection.extend(previous.shards());
    let mut ordered: Vec<Shard> = selection.into_iter().collect();
    ordered.sort_by_key(|s| {
        (
            previous.get(s).map(|c| c.staged_scanned_at).unwrap_or(0),
            s.clone(),
        )
    });

    // The per-pass shard budget, derived from what this pass can actually see rather than from a
    // constant. `ordered.len()` IS the observed shard count — see `shards_per_pass`.
    //
    // A scoped pass visits every shard it was named, and nothing else: its size is bounded by
    // its caller's own work (the objects one eviction pass removed), not by the population.
    // Dirty shards this pass visits BEYOND its budget. They sit at the front of `ordered` and
    // neither consume a budget slot nor an attempt, so the stalest-first visits are the ones the
    // pass makes with no dirty shard at all.
    let mut extra_visits: BTreeSet<Shard> = BTreeSet::new();
    let budget = match only {
        None => {
            let budget = shards_per_pass(ordered.len());
            // Dirty shards first (§ 16.6.9, "Dirty shards first"). A shard this instance removed
            // staged bytes from is otherwise one rotation away, because the fold never subtracts.
            //
            // Two arms. `dirty_share` admits as many as the age bound can spare INSIDE the
            // budget, ahead of stalest-first. Every dirty shard it does not admit, and that the
            // budget's stalest-first slots do not reach anyway, is an EXTRA visit on top of the
            // budget, up to `extra_dirty_visit_cap`. The extras are what make the next-pass bound
            // hold at a real population: the single-instance schedule's slack is `12·B − N`
            // slots, one at the fleet's N = 1,511 and none at N = 1,512, so the share alone
            // admitted nothing there. The stalest-first budget is untouched, so the age bound
            // holds by construction rather than by the feasibility argument. With no marks,
            // `ordered` is exactly as stalest-first built it.
            let marks = dirty_marks(cache_dir, instance_id);
            if !marks.is_empty() {
                let dirty: Vec<Shard> = ordered
                    .iter()
                    .filter(|s| marks.contains_key(s))
                    .cloned()
                    .collect();
                let ages: Vec<u64> = ordered
                    .iter()
                    .map(|s| age_secs(previous.get(s).map(|c| c.staged_scanned_at).unwrap_or(0)))
                    .collect();
                let share = dirty_share(&ages, budget, dirty.len());
                let front: Vec<Shard> = dirty.iter().take(share).cloned().collect();
                let front_set: BTreeSet<&Shard> = front.iter().collect();
                let rest: Vec<Shard> = ordered
                    .iter()
                    .filter(|s| !front_set.contains(s))
                    .cloned()
                    .collect();
                // A dirty shard already inside the budget's stalest-first slots costs nothing
                // extra: it is visited anyway.
                let in_budget: BTreeSet<&Shard> = front
                    .iter()
                    .chain(rest.iter().take(budget.saturating_sub(share)))
                    .collect();
                let cap = extra_dirty_visit_cap(budget);
                let extras: Vec<Shard> = dirty
                    .iter()
                    .filter(|s| !in_budget.contains(s))
                    .take(cap)
                    .cloned()
                    .collect();
                debug!(
                    "Staged fold: {} dirty shard(s): {} taken inside the budget of {}, {} \
                     visited beyond it (cap {}); the rest wait for the next pass or the rotation",
                    dirty.len(),
                    share,
                    budget,
                    extras.len(),
                    cap
                );
                if !extras.is_empty() {
                    info!(
                        dirty_shards = dirty.len(),
                        dirty_in_budget = share,
                        dirty_extra_visits = extras.len(),
                        shard_budget = budget,
                        "Staged fold: visiting {} dirty shard(s) beyond this pass's budget of {}",
                        extras.len(),
                        budget
                    );
                }
                extra_visits = extras.iter().cloned().collect();
                let mut reordered = extras;
                reordered.extend(front);
                reordered.extend(rest.into_iter().filter(|s| !extra_visits.contains(s)));
                ordered = reordered;
            }
            budget
        }
        Some(only) => {
            ordered.retain(|s| only.contains(s));
            ordered.len()
        }
    };
    // The budget bounds shards **visited**, and a shard a peer holds was not visited by us.
    // § 16.2 says "pick another shard" on `Busy`, so a busy shard must not consume the budget —
    // otherwise on a three-node fleet each instance's effective budget is cut by the fraction its
    // peers happen to be holding, and the derivation above silently under-delivers by exactly the
    // amount that makes the criterion depend on claim luck.
    //
    // Attempts are separately bounded at twice the budget so the pass stays bounded work (R1.5)
    // when a peer holds most of the population. Each wasted attempt is one small read.
    let mut attempts_remaining = budget.saturating_mul(2);

    let mut summary = FoldPassSummary::default();
    // Installs of extra dirty visits, which are outside the budget. The extras are at the front of
    // `ordered`, so by the time a budgeted shard is reached this count is final.
    let mut extra_installed = 0usize;
    // The last peer-count reading, for the sampler. See `VisitSampler`.
    let mut n_carry: Option<u32> = None;
    for shard in ordered.iter() {
        let is_extra = extra_visits.contains(shard);
        if !is_extra {
            if summary.installed.len() - extra_installed >= budget || attempts_remaining == 0 {
                break;
            }
            attempts_remaining -= 1;
        }
        if claim_shard(cache_dir, shard, instance_id).await == ClaimOutcome::Busy {
            // A busy visit read nothing and records nothing.
            summary.busy.push(shard.clone());
            continue;
        }
        let started = std::time::Instant::now();
        // `n` at this claimed visit's start: the previous reading if there is one (visit i's end
        // is visit i+1's start), else a fresh read. Kept in `n_carry`, so a visit that does not
        // complete hands it on unchanged.
        let n_start = sampler.map(|s| *n_carry.get_or_insert_with(|| s.peer_count()));
        // Before any of this visit's reads: a dirty mark older than this is a removal whose
        // `.meta` change the reads below will see, so the install may clear it.
        let visit_started_nanos = now_nanos();

        // THIS shard's ledger files, and nothing else: one `readdir` plus one file per instance
        // that has ever credited this shard. Read after the claim, so a shard a peer holds costs
        // nothing beyond the claim attempt.
        let read = match ledger
            .read_shard_for_authority(&shard.bucket, &shard.l1, cap)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "Staged fold: could not list shard {}'s write-ledger directory: {}. \
                     Retaining THIS shard's previous figure and cut; other shards are \
                     unaffected.",
                    shard, e
                );
                summary
                    .retained
                    .push((shard.clone(), FoldRetainReason::LedgerListFailed));
                continue;
            }
        };
        // Classified individually rather than through `is_complete()` so the retain reason names
        // which class of incompleteness was seen.
        //
        // **Each of these refuses ONE shard and the pass continues.** Before the read was
        // shard-scoped they sat before the visit loop and refused the volume, because one
        // unreadable file made the whole read short and there was no way to say which shard it
        // was short in. Now there is: the file lives in the shard whose figure it explains.
        //
        // `LedgerFileCountMismatch` is checked last and is currently unreachable — every path
        // that fails to read a listed file increments `files_unreadable`, which the branch above
        // catches first. Kept as the defensive statement it was written as, and reported as a
        // finding rather than quietly deleted.
        if let Some(reason) = shard_ledger_incompleteness(&read) {
            warn!(
                "Staged fold: shard {}'s write ledger is incomplete ({}), so this shard's \
                 contribution is unknown. Retaining THIS shard's previous figure and cut; \
                 other shards are unaffected.",
                shard,
                reason.label()
            );
            summary.retained.push((shard.clone(), reason));
            continue;
        }
        debug_assert!(read.is_complete());

        let mut staged_byte_sum = 0u64;
        let mut staged_range_count = 0u64;
        let mut unreadable = false;
        // Partition this shard's own candidates. `fold_staged_candidate` is the same predicate
        // `assess_staged_candidate` evaluates — it is a projection of that function — so
        // the fold's membership and eviction's membership cannot drift.
        let mut candidates: Vec<crate::write_ledger::StagedCandidate> = Vec::new();
        let mut unshardable = 0usize;
        let mut misfiled = 0usize;
        for candidate in WriteLedger::group_by_key(read.entries.clone()) {
            match shard_of(&candidate.cache_key) {
                Some((bucket, l1)) if bucket == shard.bucket && l1 == shard.l1 => {
                    candidates.push(candidate)
                }
                Some(_) => misfiled += 1,
                None => unshardable += 1,
            }
        }
        if unshardable > 0 {
            // Not an incompleteness: `parse_cache_key` rejects a key that could never have been
            // written by this proxy, so its `.meta` is not in any shard and no figure is missing
            // it. Logged because a non-zero count means something is appending keys the sharder
            // cannot place.
            debug!(
                "Staged fold: {} ledger candidate(s) in shard {} have a cache key no shard can \
                 be derived from; they are in no shard's figure",
                unshardable, shard
            );
        }
        if misfiled > 0 {
            // A DIFFERENT and more serious condition than `unshardable`, which is why it gets its
            // own name and its own level: the key is shardable and names another shard, so the
            // entry is in the wrong file and the shard it belongs to will never see it — an
            // under-statement, the direction that makes the staging eviction trigger fire late.
            //
            // Unreachable while `WriteLedger::shard_path_for_key` and this loop both resolve
            // through `shard_of`, which they do — the append and the read share one derivation.
            // It becomes reachable if they ever diverge, and this is the line that would say so.
            warn!(
                "Staged fold: {} ledger candidate(s) in shard {}'s ledger file belong to another \
                 shard. The append and the fold disagree about where a key lives, so that \
                 shard's figure is short by them.",
                misfiled, shard
            );
        }
        let candidates: Vec<&crate::write_ledger::StagedCandidate> = candidates.iter().collect();
        // Each candidate is ONE `.meta` read, and `fold_staged_candidate` is a pure function of
        // `(cache_dir, candidate)` — it reads, parses, and returns a verdict, touching no shared
        // state. So the reads may all be outstanding at once, and until now none of them were: this
        // loop was serial, at **concurrency 1**, against a measured 21.8× from 1 to 64 outstanding
        // reads on the same volume.
        //
        // **This is the term that decides the reachable-bucket ceiling, not the traversal.** That
        // ceiling is `(D·A − S/R) / (256 · c_r)`, in which `c_r` is a fixed per-visit claim cost and
        // `S/R` is this loop's parse time. At `D = 0.167`, `A = 86,400 s` and
        // `S = STAGED_RANGE_BUDGET = 32,800,000`, a serial fold's `R = 174.7 obj/s` puts `S/R` at
        // ~187,750 s against a 14,429 s window, so `D·A − S/R` is negative and the ceiling is
        // **zero buckets**: § 16.6.6 precondition 3b could never pass on any volume, however
        // small. At the measured knee of 64, `R = 2,900 obj/s` puts `S/R` at 11,310 s and the
        // ceiling at ~676 buckets. Nothing about the repair sweep's traversal appears in that expression.
        //
        // Results are collected and then processed **in ledger order**, not in completion order, so
        // the verdict and the WARN below name the same candidate a serial fold would have named. The
        // alternative — short-circuiting on the first `Unreadable` to arrive — would make which
        // candidate is reported a function of I/O timing, and would make two folds of the same shard
        // log differently. The cost of not short-circuiting is bounded by one shard's candidate count
        // and is only paid on the unhealthy path.
        //
        // Spec: cache-eviction-at-scale § 16.6.13.
        use futures::stream::StreamExt as _;
        // The futures are pushed by an explicit loop rather than produced by `.map(|c| async {..})`,
        // and that is a hard requirement rather than a preference. A closure whose argument type
        // contains a reference gets a higher-ranked signature, which makes this future's `Send` bound
        // higher-ranked, which fails at the `tokio::spawn` in `main.rs` as "implementation of `Send`
        // is not general enough … `Send` is actually implemented for `&'0 JournalConsolidator`, for
        // some specific lifetime". An `async move` block captures `candidate` at one concrete
        // lifetime and there is no closure to be generic over. The alternative was cloning every
        // `StagedCandidate` to remove the reference, which costs a `Vec` allocation per candidate on
        // the one path whose cost scales with cache population.
        let in_flight = InFlightGauge::default();
        let in_flight_ref = &in_flight;
        let mut pending = Vec::with_capacity(candidates.len());
        for (position, candidate) in candidates.iter().copied().enumerate() {
            pending.push(async move {
                let _guard = in_flight_ref.enter();
                let fold = crate::write_ledger::fold_staged_candidate(cache_dir, candidate).await;
                let (fold, publishing) = recheck_absent_candidate(cache_dir, candidate, fold).await;
                (position, fold, publishing)
            });
        }
        let mut folded: Vec<(usize, crate::write_ledger::StagedCandidateFold, bool)> =
            futures::stream::iter(pending)
                .buffer_unordered(STAGED_FOLD_CANDIDATE_CONCURRENCY)
                .collect()
                .await;
        summary.candidate_read_inflight_peak =
            summary.candidate_read_inflight_peak.max(in_flight.peak());
        folded.sort_by_key(|(position, _, _)| *position);
        for (position, fold, _) in &folded {
            match fold.verdict {
                StagedCandidateVerdict::Unreadable => {
                    warn!(
                        "Staged fold: candidate {} in shard {} has an unreadable .meta, so \
                         this shard's contribution is unknown. Retaining THIS shard's \
                         previous figure and cut; shards already installed in this pass keep \
                         their new ones.",
                        candidates[*position].cache_key, shard
                    );
                    unreadable = true;
                    break;
                }
                StagedCandidateVerdict::Evictable => {
                    staged_byte_sum = staged_byte_sum.saturating_add(fold.staged_bytes);
                    staged_range_count = staged_range_count.saturating_add(fold.staged_range_count);
                }
                // Absent, graduated or superseded: the `.meta` is authority and it says these
                // bytes are not staged. No debit event is involved; the fold simply does not
                // count them.
                _ => {}
            }
        }
        if unreadable {
            // Ends the pass, unchanged. Note this arm has the ordering exposure the
            // shard-scoped ledger refusals above deliberately avoid: a retained shard does not
            // move its `staged_scanned_at`, so a shard with a persistently unparseable `.meta`
            // sorts stalest-first on every pass and stops every shard behind it. Pre-existing,
            // not this unit's to change, and named so it is not mistaken for a consequence of
            // the shard-scoped read.
            summary
                .retained
                .push((shard.clone(), FoldRetainReason::CandidateUnreadable));
            return retain_with(FoldRetainReason::CandidateUnreadable, summary);
        }
        // A multipart completion between its ledger append and its `.meta` rename. The completion appends its entry and pending credit before the rename
        // (§ 16.6.12 L3), so this read's cut already covers the credit's seq while the `.meta`
        // read found nothing. Installing would put the bytes in neither term. Retaining keeps
        // the previous cut, which is below the credit's seq (no earlier install covered it,
        // by the same argument), so the pending term counts the bytes until a later visit
        // reads the published `.meta`, counts them in `F`, and moves the cut past the credit.
        if let Some((position, _, _)) = folded.iter().find(|(_, _, publishing)| *publishing) {
            debug!(
                "Staged fold: candidate {} in shard {} is mid-publish (temp .meta present, \
                 .meta absent). Retaining THIS shard's previous figure and cut; the next visit \
                 counts it.",
                candidates[*position].cache_key, shard
            );
            summary
                .retained
                .push((shard.clone(), FoldRetainReason::CandidatePublishing));
            continue;
        }

        // The cut for this shard: max over its previous cut and what this read observed,
        // per instance.
        //
        // **The observed max is now SHARD-LOCAL, because the read is, and that is both correct
        // and the highest safe cut.** This comment previously justified taking a *global*
        // per-instance maximum on the explicit grounds that "the ledger read is complete" — a
        // whole-volume read — and warned that a shard-local max would leave the cut lower than
        // the figure justifies. That reasoning was a property of the unpartitioned file, not of
        // the cut, and it no longer describes this code: `read_shard_for_authority` sees only
        // `{bucket}/{l1}/*.ledger`, so `read.max_seq_per_instance` is this shard's own maximum.
        //
        // Why shard-local is right, in both directions:
        //
        // - **It cannot be too low (no double count).** A credit is counted in this shard's
        //   figure only if its entry is in one of this shard's ledger files, and every such
        //   entry has a seq at or below that file's `max_seq_ever` — which is what was observed.
        //   So there is no credit this figure counted that sits above this cut.
        // - **It cannot be too high (no counted-by-neither).** Under the old global maximum,
        //   this shard's cut could sit above the highest seq this shard has ever recorded,
        //   because another shard had a more recent credit from the same instance. A pending
        //   credit for this shard below that coordinate would then be filtered as
        //   already-folded while no fold had read its `.meta` — § 16.6.1's forbidden outcome.
        //   The whole-volume read made that unreachable rather than harmless: every entry was
        //   visible, so a credit below the global max really had been folded somewhere. A
        //   shard-scoped read has no such guarantee, and a shard-local max does not need one.
        //
        // Note the arithmetic that makes both statements safe is per FILE, not per entry:
        // `max_seq_ever` is `max(header, highest entry seq)`, so a compaction that retired the
        // top of the file cannot lower the observed maximum. See
        // `LedgerReadOutcome::max_seq_per_instance`.
        //
        // The **sequence** deliberately stays per instance rather than per `(shard, instance)`.
        // A monotone per-instance counter, floored by the persistent `{instance}.ledger.mark`,
        // issues every new seq strictly above every cut that instance has ever justified in ANY
        // shard — including a drained shard that has a cut and no ledger file, whose previous cut
        // this loop carries forward by `max` and which therefore survives shard-scoping
        // unchanged. Narrowing the sequence would still need that mark as a floor for exactly
        // that case, so it would add per-shard state without removing the invariant's dependency
        // on it. See `WriteLedger::seed_sequence`.
        let mut staged_fold_cut = previous
            .get(shard)
            .map(|c| c.staged_fold_cut.clone())
            .unwrap_or_default();
        for (instance, observed) in &read.max_seq_per_instance {
            let slot = staged_fold_cut.entry(instance.clone()).or_insert(0);
            if observed > slot {
                *slot = *observed;
            }
        }

        // Foreign fields. See `StagedShardCheckpoint::other`: the `st_size` fold owns the rest
        // of this record. The copy from `previous` is only the FALLBACK — the live value is
        // re-read under the document lock at install time, because the `st_size` fold may have
        // written its own fields since this pass's checkpoint read. See
        // `install_preserving_foreign_fields`.
        let other = previous
            .get(shard)
            .map(|c| c.other.clone())
            .unwrap_or_default();

        let checkpoint = StagedShardCheckpoint {
            schema: STAGED_FOLD_SCHEMA,
            bucket: shard.bucket.clone(),
            l1: shard.l1.clone(),
            staged_byte_sum,
            staged_range_count,
            staged_scanned_at: now_nanos(),
            staged_scanned_by: instance_id.to_string(),
            staged_fold_cut,
            other,
        };
        if let Err(e) = install_preserving_foreign_fields(cache_dir, checkpoint.clone()) {
            warn!(
                "Staged fold: could not install the checkpoint for shard {}: {}. That \
                 shard's previous figure and cut are both still on disk.",
                shard, e
            );
            summary
                .retained
                .push((shard.clone(), FoldRetainReason::CheckpointWriteFailed));
            return retain_with(FoldRetainReason::CheckpointWriteFailed, summary);
        }
        let visit_elapsed = started.elapsed();
        record_shard_duration(visit_elapsed);
        if let (Some(s), Some(n_start)) = (sampler, n_start) {
            let n_end = s.peer_count();
            n_carry = Some(n_end);
            s.record(n_start, n_end, folded.len() as u64, visit_elapsed);
        }
        debug!(
            "Staged fold installed shard {}: staged_byte_sum={}, staged_range_count={}",
            shard, checkpoint.staged_byte_sum, checkpoint.staged_range_count
        );
        // Only this instance's own completed visit clears its mark. A peer's later
        // `staged_scanned_at` is not evidence: it is stamped at install, not when the peer's
        // reads began, so a peer visit that read the `.meta` before the removal would look like
        // it had seen it. § 16.6.9, "Dirty shards first", point 3.
        clear_dirty_mark(cache_dir, instance_id, shard, visit_started_nanos);
        // The visit is over, so the claim has nothing left to protect. Left in place it made the
        // shard `Busy` to every peer for the rest of `CLAIM_LEASE_SECS`, including a peer that
        // has the shard dirty — saw that hold a DELETE in the figure for an extra pass.
        release_claim(cache_dir, shard, instance_id);
        summary.installed.push(checkpoint);
        if is_extra {
            extra_installed += 1;
        }
    }

    // A shard-scoped retain does not end the pass, so it has to be reported at the end. The
    // FIRST one in visit order is the reason the pass carries, because it is the one whose WARN
    // the operator has already seen and because a stable choice keeps `retain_reason()` a
    // function of the state rather than of the ordering's tail.
    let shard_scoped_retain = summary.retained.first().map(|(_, reason)| *reason);

    if summary.installed.is_empty() {
        // A shard that refused its own ledger read outranks "nothing to visit": the pass really
        // did see an incomplete input, and `NoShardAvailable` is excluded from
        // `staged_fold_incomplete_total`, so reporting it here would lose the signal entirely.
        if let Some(reason) = shard_scoped_retain {
            return retain_with(reason, summary);
        }
        // Every candidate shard was claimed by a peer, or there was nothing to visit. Not an
        // incompleteness — see `FoldRetainReason::NoShardAvailable`.
        return retain_with(FoldRetainReason::NoShardAvailable, summary);
    }

    // Only now is it safe to drop this instance's dead pending records. Dead is a **per-shard**
    // judgement: a record is dead iff its own shard's installed cut covers its seq. Dropping
    // by a single scalar cut, as the unpartitioned version did, would reclaim records in
    // shards this pass never visited — records no shard figure counts — which is § 16.6.1's
    // counted-by-neither outcome made permanent by a deletion.
    let installed_set = read_shard_checkpoints(cache_dir);
    if installed_set.is_complete() {
        match pending.gc(instance_id, &installed_set).await {
            Ok(0) => {}
            Ok(n) => debug!("Staged fold: reclaimed {} dead pending record(s)", n),
            Err(e) => debug!(
                "Staged fold: pending GC failed ({}); dead records stay filtered by the cut",
                e
            ),
        }
    }

    // The budget's input changed, so the memo must not outlive the change by its TTL. The
    // authority figure's inputs changed in the same install, for the same reason — a fold that
    // has just moved `staged_byte_sum` must not be shadowed by a figure taken before it.
    // This instance's reclaim document is superseded by the same install, for the same reason.
    invalidate_budget_snapshot_retiring(cache_dir, Some(instance_id));
    expire_authority_snapshot(cache_dir);

    info!(
        "Staged fold installed {} shard(s) ({} busy, {} retained): staged_byte_sum={}, \
         staged_range_count={}",
        summary.installed.len(),
        summary.busy.len(),
        summary.retained.len(),
        installed_set.staged_byte_sum(),
        installed_set.staged_range_count()
    );
    // Some shards installed AND at least one refused its own ledger read. Reported as a retain,
    // which is what `FoldOutcome::Retained`'s doc already requires: the completion stamp consumes
    // `installed()`, so a pass that could not refresh one shard retries after the attempt
    // interval rather than being reported fresh. The shards that did install keep their new
    // documents — they are on disk, and the GC and the memo invalidation above have already run
    // against them.
    if let Some(reason) = shard_scoped_retain {
        return retain_with(reason, summary);
    }
    FoldOutcome::Installed(summary)
}

/// Which class of incompleteness a **shard-scoped** authority read saw, if any.
///
/// One function rather than four inline branches so the fold's gate and the tests that name each
/// class cannot drift, and so the order of precedence is stated once.
///
/// `LedgerReadOutcome::legacy_files_present` is deliberately absent: a legacy flat file names no
/// shard, so it has no shard-scoped form, and a shard-scoped read always reports it as 0. The
/// pass checks it once, before any shard is visited.
fn shard_ledger_incompleteness(
    read: &crate::write_ledger::LedgerReadOutcome,
) -> Option<FoldRetainReason> {
    if read.files_unreadable > 0 {
        return Some(FoldRetainReason::LedgerFileUnreadable);
    }
    if read.malformed_lines > 0 {
        return Some(FoldRetainReason::LedgerMalformedLine);
    }
    if read.truncated {
        return Some(FoldRetainReason::LedgerTruncated);
    }
    if read.files_read != read.files_expected {
        return Some(FoldRetainReason::LedgerFileCountMismatch);
    }
    None
}

/// Re-decide a fresh `MetadataAbsent` candidate that may be a multipart completion between
/// its ledger append and its `.meta` rename. Returns the verdict to fold and whether the
/// candidate is mid-publish.
///
/// The completion writes its temp `.meta`, appends the ledger entry and pending credit, then
/// renames the temp onto the `.meta` path, so at every instant after the append exactly one
/// of the two names exists until the rename (or a decline removes the temp). The probe reads
/// the temp FIRST and the `.meta` SECOND:
///
/// - temp present, `.meta` still absent: the rename has not happened; mid-publish.
/// - `.meta` present on the second read: the rename happened; fold it as published.
/// - both absent: no completion is between append and rename (a deleted or declined object),
///   so `MetadataAbsent` stands.
///
/// Only candidates whose newest entry is inside `ABSENT_RETIREMENT_GRACE` are probed. Past it,
/// an entry naming an absent `.meta` is terminal (a crashed completion's temp no longer holds
/// the shard back), and the extra directory read stays off the steady-state path.
async fn recheck_absent_candidate(
    cache_dir: &Path,
    candidate: &crate::write_ledger::StagedCandidate,
    fold: crate::write_ledger::StagedCandidateFold,
) -> (crate::write_ledger::StagedCandidateFold, bool) {
    if fold.verdict != StagedCandidateVerdict::MetadataAbsent {
        return (fold, false);
    }
    let fresh_after = SystemTime::now()
        .checked_sub(crate::write_ledger::ABSENT_RETIREMENT_GRACE)
        .unwrap_or(UNIX_EPOCH);
    if candidate.newest_entry_at <= fresh_after {
        return (fold, false);
    }
    let temp_present = meta_temp_present(cache_dir, &candidate.cache_key).await;
    let fold = crate::write_ledger::fold_staged_candidate(cache_dir, candidate).await;
    let publishing = temp_present && fold.verdict == StagedCandidateVerdict::MetadataAbsent;
    (fold, publishing)
}

/// Whether any instance's temp `.meta` for `cache_key` exists: a name in the `.meta`'s
/// directory of the form `crate::cache::bounded_instance_tmp_name` builds, `{stem}.meta.tmp.*`
/// or, for a stem too long for `NAME_MAX`, `{prefix}~{hash}.meta.tmp.*`. A directory read
/// error other than `NotFound` answers `true`: retaining the shard for one visit is the safe
/// reading of an unknown.
async fn meta_temp_present(cache_dir: &Path, cache_key: &str) -> bool {
    let Ok(meta_path) =
        crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
    else {
        return false;
    };
    let (Some(dir), Some(stem)) = (meta_path.parent(), meta_path.file_stem()) else {
        return false;
    };
    let stem = stem.to_string_lossy().to_string();
    let hashed = format!(
        "~{}",
        &blake3::hash(stem.as_bytes()).to_hex().as_str()[..32]
    );
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
        Err(_) => return true,
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some((base, _)) = name.split_once(".meta.tmp.") {
                    if base == stem || base.ends_with(&hashed) {
                        return true;
                    }
                }
            }
            Ok(None) => return false,
            Err(_) => return true,
        }
    }
}

fn retain_with(reason: FoldRetainReason, summary: FoldPassSummary) -> FoldOutcome {
    if reason.is_incompleteness() {
        STAGED_FOLD_INCOMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
        warn!(
            "Staged fold retained the previous figure and cut: reason={}, shards_installed={}",
            reason.label(),
            summary.installed.len()
        );
    } else {
        debug!(
            "Staged fold pass installed nothing: reason={}, shards_busy={}",
            reason.label(),
            summary.busy.len()
        );
    }
    FoldOutcome::Retained(reason, summary)
}

fn retain(reason: FoldRetainReason) -> FoldOutcome {
    retain_with(reason, FoldPassSummary::default())
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// `metadata/_shard_state/staged_fold_freshness.json` — the document behind § 16.6.7's three-state
/// badge.
const FOLD_FRESHNESS_FILE: &str = "staged_fold_freshness.json";

/// § 16.6.7's three fold-freshness fields.
///
/// # A document of its own, NOT three fields on `size_state.json` — a deviation with a reason
///
/// § 16.6.7 specifies these as three fields "in `size_state.json` and nothing else". They are
/// implemented as a separate single-purpose document instead, and the reason is a hazard the design
/// did not consider: **the fold runs outside the global consolidation lock.**
/// `run_consolidation_cycle` starts the fold (on its own task, via
/// `JournalConsolidator::spawn_maintenance_passes`) deliberately *before* the idle early return and
/// before the lock, so that an idle fleet still folds.
///
/// Writing three fields into `size_state.json` from there means an unlocked read-modify-write of the
/// **accounting** document. That document also carries `total_size` and `write_cache_size`, so a
/// concurrent consolidation on any instance could commit a new figure between this pass's read and
/// its write, and this pass's write would silently revert it. The cost of losing an update to a
/// badge field is one pass of display freshness; the cost of reverting `total_size` is accounting
/// drift that persists until a full validation scan re-grounds it. Those are not the same risk, and
/// putting them in one unlocked write makes the cheap one able to cause the expensive one.
///
/// A dedicated document removes the hazard by construction rather than by locking: it holds nothing
/// but these three fields, so the worst a lost update can cost is what it should cost. It follows the
/// same atomic temp → `sync_all` → rename discipline as the per-shard checkpoints beside it, and it
/// needs no schema fence because no other writer reads it.
///
/// Timestamps are **unix seconds as stored**, not ages. The age has to be computed against the same
/// clock read that computes the other age gauges, and converting at the boundary would let two
/// gauges in one `/metrics` response disagree about `now`.
///
/// Every field is `Option`, and absent means **unknown** rather than `0`/`false`: a zero timestamp is
/// a real instant and a `false` completion flag is a real verdict, and either would make a volume
/// that has never folded assert something about a fold. `completeness` falls back to its observable
/// form when they are absent, which is also what covers § 16.6.6's mixed-version phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldFreshness {
    /// Unix seconds of the last `Complete_Install`.
    #[serde(default)]
    pub completed_at: Option<u64>,
    /// Unix seconds of the last attempt, complete or not.
    #[serde(default)]
    pub last_attempt_at: Option<u64>,
    /// Whether that attempt completed.
    #[serde(default)]
    pub last_attempt_complete: Option<bool>,
}

impl FoldFreshness {
    fn path(cache_dir: &Path) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(SHARD_STATE_DIR)
            .join(FOLD_FRESHNESS_FILE)
    }

    /// Read the document, or `None` when it is absent or unreadable.
    ///
    /// Unreadable collapses to `None` — the same reading as absent — deliberately: this is a display
    /// input, and the consumer's fallback for absence is already correct for "cannot tell". A
    /// sentinel distinguishing the two would have no consumer.
    pub fn load(cache_dir: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(Self::path(cache_dir)).ok()?;
        serde_json::from_str(&content).ok()
    }

    /// Install atomically: temp → `sync_all` → rename, with an instance-suffixed temp name so two
    /// instances cannot share one temp path on shared storage.
    fn install(&self, cache_dir: &Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::other(format!("serialize fold freshness: {e}")))?;
        let tmp = path.with_extension(format!(
            "json.tmp.{}.{}",
            hostname_for_tmp(),
            std::process::id()
        ));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }
}

/// Record the outcome of one fold attempt — § 16.6.7's three freshness fields.
///
/// Called for **every** attempt, including those that installed nothing: the badge's `Unknown` state
/// means "the last attempt did not complete", and that cannot be derived from a field only
/// successful attempts write.
///
/// `completed_at` advances **only** on a `Complete_Install`, so the gap between it and
/// `last_attempt_at` is how long the fold has been failing. Read-modify-write over this document
/// alone, so a lost update costs one pass of display freshness and nothing else — see
/// [`FoldFreshness`] for why that isolation is the point.
///
/// Synchronous rather than `async`: it is one small read and one small write on a path that already
/// does blocking `std::fs` work, and making it `async fn(&self)` on the consolidator added an
/// `async` layer that tipped rustc's higher-ranked `Send` inference over at the spawn sites in
/// `main.rs`. Best-effort throughout — a failure logs at debug and the badge falls back.
pub(crate) fn record_fold_attempt(cache_dir: &Path, now_secs: u64, installed: bool) {
    let mut doc = FoldFreshness::load(cache_dir).unwrap_or_default();
    doc.last_attempt_at = Some(now_secs);
    doc.last_attempt_complete = Some(installed);
    if installed {
        doc.completed_at = Some(now_secs);
    }
    if let Err(e) = doc.install(cache_dir) {
        debug!(
            "Staged fold: could not record this attempt's freshness ({}). The completeness badge \
             falls back to deriving its state from whether a figure is currently derivable.",
            e
        );
    }
}

/// The gauge set this unit publishes. **Read by `/metrics` and by nothing else.**
///
/// Every byte figure is an `Option`, and `None` is the UNREADABLE sentinel rather than a
/// zero — the same discipline the two debts in this unit apply to `ledger_entries` and
/// `in_progress_part_bytes`. A staged figure that reports 0 because a file could not be
/// read is a failure disguised as the healthiest possible value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedFoldGauges {
    /// `F + post-cut pending + P`: what the fold says the staging tier holds, in
    /// compressed on-disk bytes. `null` if any input was unreadable or no fold has
    /// completed yet.
    pub staged_fold_bytes: Option<u64>,

    /// `F` alone: the installed `staged_byte_sum`.
    pub staged_fold_byte_sum: Option<u64>,

    /// The post-cut pending term alone.
    pub staged_fold_pending_bytes: Option<u64>,

    /// `P` alone, restated here so the three components of `staged_fold_bytes` are each
    /// visible. A derived total whose inputs are invisible cannot be argued with.
    pub staged_fold_in_progress_part_bytes: Option<u64>,

    /// **`staged_fold_bytes` − the INCUMBENT `SizeState::write_cache_size`**, signed.
    ///
    /// Sign convention, stated because a signed gap whose direction is ambiguous is
    /// useless: **positive means the fold sees MORE staged bytes than the incumbent
    /// counter does; negative means the incumbent OVER-states relative to the fold.**
    /// The incumbent's measured failure is a directional over-count of 4.28x-11.17x, so
    /// the expected sign on a fleet carrying real drift is **negative**, and its
    /// magnitude is the drift.
    ///
    /// # The comparand is the incumbent, NOT `write_cache.resident_bytes`
    ///
    /// It was described as the latter, and the two were the same figure for as long as
    /// `resident_bytes` published the incumbent unconditionally. They are not the same once the
    /// epoch is active: `resident_bytes` then resolves through
    /// `CacheManager::staged_authority` and *is* the fold's figure, so a gap measured against it
    /// would be zero by construction and read as "no drift" rather than "nothing to compare".
    ///
    /// **`None` once the epoch is active**, per § 16.6.7, which removes this gauge at activation
    /// because there is no second producer left. A published number there would invite a reader
    /// to treat a small value as agreement between two producers, when one of them has been
    /// retired.
    pub staged_fold_gap_bytes: Option<i64>,

    /// Seconds since the fold last completed an install **on any shard**. `null` if no fold
    /// has completed.
    ///
    /// **I2's definition no longer holds and this one replaces it.** I2 computed
    /// `now − staged_scanned_at` of a single global document, which was both "how long since
    /// the fold ran" and "how stale the figure is" because there was one figure. Under
    /// partitioning those are two different questions and this field answers only the first:
    /// it is the age of the **freshest** shard, i.e. whether the fold is getting anything
    /// done. The figure's staleness is `staged_shard_authority_age_secs` below, and that is
    /// the one § 16.6.4's criterion is set on. Reading this one as the authority age would
    /// under-state it by the whole rotation, and would read healthiest exactly when a fold is
    /// spinning on one shard and starving the rest.
    ///
    /// **Not alarmable on its own.** Read it with `staged_mutations_awaiting_fold`: a
    /// large age with zero awaiting mutations is an idle fleet and is healthy at any age.
    pub staged_authority_age_secs: Option<u64>,

    /// **The criterion figure.** `max` over shards of `now − checkpoint.staged_scanned_at`,
    /// seconds — the age of the stalest shard contributing to the published sum. `null` if no
    /// shard has a figure.
    ///
    /// § 16.6.4's acceptance criterion is `p100 ≤ 3,600 s` on this gauge at the O(100M)-range
    /// design point, with zero `staged_fold_incomplete_total` increments over the window. A
    /// maximum over shards rather than a mean because an authority is only as fresh as its
    /// stalest component, and a mean would hide exactly the shard that is not being visited.
    pub staged_shard_authority_age_secs: Option<u64>,

    /// Shards with an installed figure.
    pub staged_fold_shards: u64,

    /// Shards whose figure is older than [`MAX_AUTHORITY_AGE_SECS`].
    ///
    /// § 16.6.7's **Stale but valid** presentation needs this beside the age: an age says how
    /// stale the stalest shard is, this says how much of the figure is stale. One shard out of
    /// 256 past the bound is a scheduling hiccup; most of them is a rotation that cannot keep up,
    /// and `staged_fold_shard_budget_capped_total` is then the field that says why.
    pub staged_fold_shards_beyond_max_age: u64,

    /// [`MAX_AUTHORITY_AGE_SECS`], restated for the same reason as
    /// [`StagedFoldGauges::staged_range_budget`]: it is the bound the two age gauges are judged
    /// against and it has no config field, so a consumer that could not read it here would have
    /// to hardcode it.
    pub staged_authority_max_age_secs: u64,

    /// Age of the memoised authority figure, seconds. `null` when there is no live memo, i.e.
    /// when the next read will re-derive from shared storage.
    ///
    /// Read with the two counters below. This one alone cannot separate "the memo has been
    /// serving for 3 s" from "the figure was re-derived 3 s ago and again on every call since",
    /// and those have very different costs.
    pub staged_authority_snapshot_age_secs: Option<u64>,

    /// Authority reads served from the memo, since process start. **Per-instance.**
    pub staged_authority_snapshot_hits_total: u64,

    /// Authority reads that re-derived the figure from shared storage, since process start.
    ///
    /// This is the count of times the three shared-volume reads were actually paid. A hit rate
    /// (`hits / (hits + recomputes)`) near zero means the memo is not doing its job.
    /// **Per-instance.**
    pub staged_authority_snapshot_recomputes_total: u64,

    /// Passes whose derived per-pass shard budget was clipped by the ceiling, since process start.
    ///
    /// **Non-zero means this instance alone cannot meet § 16.6.4's `Max_Authority_Age` on this
    /// volume** — there are more cache shards than one instance can revisit inside the target age.
    /// The staged figure stays correct; it goes stale, visibly, in
    /// `staged_shard_authority_age_secs`. Claims divide shards between instances, so adding an
    /// instance is the remedy, and `staged_fold_shards_beyond_max_age` says whether the fleet keeps
    /// up. **Per-instance.**
    pub staged_fold_shard_budget_capped_total: u64,
    /// The most shards ONE instance's fold can revisit inside [`MAX_AUTHORITY_AGE_SECS`]:
    /// `SHARD_VISITS_PER_PASS_CEILING` times the passes in that window. A constant of the binary,
    /// published so `staged_fold_shards` has its comparand on the same scrape. Above it,
    /// `staged_fold_shard_budget_capped_total` counts.
    pub staged_fold_shards_reachable_per_instance: u64,

    /// Shards this instance removed staged bytes from (DELETE, graduation, re-PUT, staging
    /// eviction, capacity eviction) and has not yet re-folded. Its next pass visits them: inside
    /// its shard budget as far as the age bound allows, and beyond the budget for the rest, up to
    /// one budget's worth more. **Per-instance**, in process memory, and zero after a restart.
    /// Exported on OTLP under the same name.
    ///
    /// A figure that stays high across several passes means more shards are dirtied per pass than
    /// the extra-visit cap admits, a peer holds their claims, or their visits are retained; the
    /// removed bytes then leave the staged figure at a later pass or when the rotation reaches
    /// their shards. § 16.6.9, "Dirty shards first".
    pub staged_fold_dirty_shards: u64,

    /// `Σ staged_range_count` over shards plus the post-cut pending count — the figure
    /// [`STAGED_RANGE_BUDGET`] is enforced against. `null` if any input is unreadable.
    pub staged_range_count: Option<u64>,

    /// [`STAGED_RANGE_BUDGET`], restated so a dashboard can draw the count against its bound
    /// without hardcoding a constant that has no config field to read.
    pub staged_range_budget: u64,

    /// Write-through admissions declined by the budget since process start.
    ///
    /// Per-instance, like every other counter on `/metrics` — see
    /// `cache-coherency-invariants.md`. A zero here is evidence the budget has not declined
    /// anything **on this instance**, not on the fleet.
    pub staged_range_budget_declines_total: u64,

    /// `staged_fold_shard_duration_secs`, as `{class_upper_edge_secs → count}` with a final
    /// `"+Inf"` class, plus `_sum` in milliseconds.
    pub staged_fold_shard_duration_secs: ShardDurationHistogram,

    /// Post-cut pending credits, counted.
    ///
    /// This is the discriminator that separates "no staged mutation since the last
    /// Complete_Install" from "mutations are waiting for a fold". Zero means every credit
    /// any instance has made is at or below its cut, i.e. the installed figure already
    /// accounts for everything — which is § 16.6.4's derivable idle state. Non-zero with
    /// a rising `staged_authority_age_secs` is a stalled fold.
    ///
    /// It is the pending records rather than a live ledger `max_seq` read because a credit
    /// appends both together with the same seq, so `max_seq > cut` and "a post-cut pending
    /// record exists" are the same condition — and this side of it costs one small file
    /// read instead of a complete ledger read on every `/metrics` poll.
    pub staged_mutations_awaiting_fold: Option<u64>,

    /// Passes that retained on an **incomplete input**, since process start.
    ///
    /// A pass that installed nothing because every shard was claimed by a peer is not counted
    /// here — see [`FoldRetainReason::NoShardAvailable`]. § 16.6.4's criterion requires zero
    /// increments of this counter over the acceptance window, and a healthy fleet where peers
    /// take turns would otherwise fail that gate for doing exactly what it was designed to do.
    pub staged_fold_incomplete_total: u64,
    /// Peak `.meta` reads the fold has held outstanding at once, since process start.
    /// **Per-instance.**
    ///
    /// **`1` on a fleet that has folded a shard with more than one candidate is the finding.** The
    /// fold's parse rate is the `S/R` term of § 16.6.6's reachable-bucket ceiling, and a serial fold
    /// measures 174.7 obj/s against 3,801 at the knee of 64 — enough to put the parse term over the
    /// whole authority-age window and make the ceiling zero buckets. This field is how an operator
    /// tells "the fold is keeping reads outstanding" from "the fold is nominally concurrent".
    ///
    /// A peak below `STAGED_FOLD_CANDIDATE_CONCURRENCY` is not itself a fault: it is bounded by the
    /// candidate count of the busiest shard folded so far, so a fleet with few staged objects will
    /// report a small number correctly.
    pub staged_fold_candidate_read_inflight_peak: u64,

    /// § 16.6.7's three fold-freshness fields, read from `size_state.json`. **Fleet-scoped** — they
    /// describe the last attempt made by any instance on this volume, not by this process.
    ///
    /// `None` on all three means either no attempt has been recorded or a pre-C binary rewrote the
    /// document and stripped them (§ 16.6.6's Phase L). Absence is unknown, never failure — see
    /// [`StagedFoldGauges::completeness`], which falls back to the observable form.
    ///
    /// Seconds since the last **Complete_Install**, or `null`. This is the age the **Unknown** row
    /// shows beside a retained figure, and it is the one figure that says how long the fold has been
    /// failing rather than how stale the figure is (which is
    /// `staged_shard_authority_age_secs`).
    pub staged_fold_completed_age_secs: Option<u64>,

    /// Seconds since the last fold **attempt**, complete or not, or `null`.
    pub staged_fold_last_attempt_age_secs: Option<u64>,

    /// Whether the last fold attempt reached a `Complete_Install`, or `null` when no attempt is
    /// recorded. `Some(false)` is what puts the badge in **Unknown** while a complete older figure
    /// is still on display — the state I6's `staged_fold_bytes == None` approximation could not
    /// reach.
    pub staged_fold_last_attempt_complete: Option<bool>,
}

/// Fixed-class histogram of shard-visit durations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDurationHistogram {
    /// Class counts, indexed to match [`SHARD_DURATION_CLASS_EDGES_SECS`] with a trailing
    /// `+Inf` class. Cumulative counts are left to the exporter; these are per-class.
    pub classes: Vec<ShardDurationClass>,
    /// Total visits recorded.
    pub count: u64,
    /// Summed duration, milliseconds. In milliseconds rather than seconds so a fleet of
    /// sub-second visits does not sum to zero.
    pub sum_ms: u64,
}

/// One class of [`ShardDurationHistogram`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDurationClass {
    /// Inclusive upper edge in seconds, or `None` for the `+Inf` class.
    pub le_secs: Option<u64>,
    /// Visits in this class.
    pub count: u64,
}

/// Snapshot the shard-duration histogram.
pub(crate) fn shard_duration_histogram() -> ShardDurationHistogram {
    let mut classes = Vec::with_capacity(SHARD_DURATION_CLASSES.len());
    let mut total = 0u64;
    for (i, cell) in SHARD_DURATION_CLASSES.iter().enumerate() {
        let count = cell.load(Ordering::Relaxed);
        total = total.saturating_add(count);
        classes.push(ShardDurationClass {
            le_secs: SHARD_DURATION_CLASS_EDGES_SECS.get(i).copied(),
            count,
        });
    }
    ShardDurationHistogram {
        classes,
        count: total,
        sum_ms: SHARD_DURATION_SUM_MS.load(Ordering::Relaxed),
    }
}

/// § 16.6.7's three-state completeness rule, as a value a consumer can branch on.
///
/// The point of the three states is that a figure whose inputs are incomplete MUST NOT render as
/// though it were current: "publishing a precise occupancy derived from a partial fold gives the
/// operator a confident wrong number, which is worse than showing nothing."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedCompleteness {
    /// A complete figure, inside [`MAX_AUTHORITY_AGE_SECS`]. Show the number plain.
    Current,
    /// A complete figure of an older state — a valid statement about the past. Show the number
    /// **with** its age and the count of shards past the bound.
    StaleButValid,
    /// The figure is not currently derivable: a shard checkpoint, a `.pending` or the
    /// `mpus_in_progress/` traversal was unreadable, or no fold has ever completed on this
    /// volume. What is published is the **retained** figure, and it must be labelled as such.
    Unknown,
}

impl StagedCompleteness {
    /// A short stable label, for a log line or a dashboard badge class.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::StaleButValid => "stale_but_valid",
            Self::Unknown => "unknown",
        }
    }
}

/// The operator-facing projection of [`StagedFoldGauges`] — § 16.6.7's dashboard rows.
///
/// # Why this is a separate type rather than the gauge bundle
///
/// Same containment argument as [`StagedAuthorityFigure`], for the opposite reason. That type
/// exists so a decision path cannot reach the observability fields; this one exists so the
/// **dashboard** need not name them either, and `no_decision_path_reads_the_fold`'s claim 1 keeps
/// `StagedFoldGauges` readable only by its producer and `metrics.rs`. The dashboard consumes this
/// instead, so the fence holds without weakening it.
///
/// # The budget denominator is DATA here, never text
///
/// [`Self::range_budget`] carries [`STAGED_RANGE_BUDGET`] and [`Self::range_budget_percent`] is
/// derived from it. A front end renders `count / range_budget` by reading these fields, so the
/// displayed denominator is whatever the constant says at runtime. Nothing downstream may spell
/// the number — see `the_budget_denominator_is_data_and_never_a_literal`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StagedTierSummary {
    /// § 16.6.7's three-state rule.
    pub completeness: StagedCompleteness,
    /// `F + post-cut pending + P`, compressed on-disk bytes. `null` in the `Unknown` state.
    pub staged_bytes: Option<u64>,
    /// `P` alone. `null` when the `mpus_in_progress/` traversal was unreadable.
    pub in_progress_part_bytes: Option<u64>,
    /// Staged ranges counted against the budget. `null` when an input was unreadable.
    pub range_count: Option<u64>,
    /// [`STAGED_RANGE_BUDGET`] at runtime — the denominator a front end must format with.
    pub range_budget: u64,
    /// `range_count / range_budget × 100`, derived from the two fields above so it cannot drift
    /// from either. `null` when the count is unknown.
    pub range_budget_percent: Option<f32>,
    /// Write-through admissions the budget has declined on this instance.
    pub range_budget_declines_total: u64,
    /// Age of the freshest shard, seconds — "is the fold getting anything done".
    pub authority_age_secs: Option<u64>,
    /// Age of the **stalest** shard, seconds — the figure the bound is judged on.
    pub shard_authority_age_secs: Option<u64>,
    /// [`MAX_AUTHORITY_AGE_SECS`] at runtime, the bound the two ages are read against.
    pub max_authority_age_secs: u64,
    /// Shards past that bound.
    pub shards_beyond_max_age: u64,
    /// Shards carrying a figure at all.
    pub shards: u64,
    /// Passes that retained on an incomplete input. Non-zero with `Unknown` names the cause.
    pub fold_incomplete_total: u64,
    /// Passes whose per-pass shard budget was clipped. Non-zero means this instance alone
    /// cannot meet the age bound; it depends on the shard population only, so adding
    /// instances does not stop it, and `shards_beyond_max_age` says whether the fleet keeps up.
    pub shard_budget_capped_total: u64,
    /// Shard visits recorded in the duration histogram.
    pub shard_visits: u64,
    /// Mean shard-visit duration, milliseconds. `null` before the first visit.
    pub shard_visit_mean_ms: Option<u64>,
    /// Age of the memoised authority figure, seconds. `null` when the next read re-derives.
    pub authority_snapshot_age_secs: Option<u64>,
    /// Authority reads served from the memo, and reads that paid for the shared-volume
    /// derivation. The ratio is what says whether the memo is working.
    pub authority_snapshot_hits_total: u64,
    pub authority_snapshot_recomputes_total: u64,
    /// Seconds since the last fold **Complete_Install**, or `null`. § 16.6.7 shows this beside the
    /// retained figure in the **Unknown** state: it is how long the fold has been failing, which is
    /// a different question from how stale the figure is (`shard_authority_age_secs`).
    pub fold_completed_age_secs: Option<u64>,
    /// Seconds since the last fold **attempt**, complete or not, or `null`. The gap between this and
    /// `fold_completed_age_secs` is the failing window.
    pub fold_last_attempt_age_secs: Option<u64>,
    /// Whether the last fold attempt completed, or `null` when no attempt is recorded — including
    /// when a pre-C binary stripped the field during § 16.6.6's Phase L.
    pub fold_last_attempt_complete: Option<bool>,
}

impl StagedFoldGauges {
    /// § 16.6.7's three-state completeness verdict.
    ///
    /// # Two predicates, and the per-attempt one now exists
    ///
    /// § 16.6.7 writes `Unknown` as `!staged_fold_last_attempt_complete`, read from three
    /// `size_state.json` fields. Those fields **did not exist** when I6 came to consume them, so it
    /// used `staged_fold_bytes == None` as a stand-in and recorded the substitution as a deviation.
    /// The stand-in is fleet-scoped — "a complete figure is not derivable from this volume right
    /// now" — where the specified predicate is per-attempt, and they differ in exactly the state the
    /// **Unknown** row exists to name: the last attempt retained on an incomplete input while an
    /// **older complete figure is still installed and still derivable**. There `staged_fold_bytes`
    /// is `Some`, so the stand-in reported **Current** and the operator saw a plain number for a
    /// fold that had stopped working.
    ///
    /// The three fields are implemented now, and this reads them, with the stand-in retained as the
    /// fallback rather than deleted. Both are needed:
    ///
    /// - `Some(false)` from the field is **Unknown**, whatever the byte figure says. This is the
    ///   case the stand-in could not see.
    /// - `staged_fold_bytes == None` is **also** Unknown, whatever the field says. A complete last
    ///   attempt does not help if the figure cannot be derived *now* — a shard checkpoint may have
    ///   become unreadable since. The stand-in was never wrong, only incomplete.
    /// - `None` from the field means no attempt has been recorded **or** a pre-C binary rewrote
    ///   `size_state.json` and stripped the fields (§ 16.6.6's Phase L). Absence must not read as
    ///   failure, so the verdict falls through to the observable form, which is exactly I6's
    ///   behaviour and is what keeps a mixed-version fleet's badge sane.
    ///
    /// `fold_incomplete_total` is published alongside either way, so the retained-figure row still
    /// carries its reason.
    pub fn completeness(&self) -> StagedCompleteness {
        // A figure that cannot be derived now is Unknown regardless of the last attempt: the badge
        // describes what is on display, and there is nothing complete to display.
        if self.staged_fold_bytes.is_none() {
            return StagedCompleteness::Unknown;
        }
        // The specified predicate, when the volume carries it.
        if self.staged_fold_last_attempt_complete == Some(false) {
            return StagedCompleteness::Unknown;
        }
        match self.staged_shard_authority_age_secs {
            Some(age) if age > self.staged_authority_max_age_secs => {
                StagedCompleteness::StaleButValid
            }
            _ => StagedCompleteness::Current,
        }
    }

    /// Set the staged tier's byte figure, for a test outside this module that must populate it.
    ///
    /// `otlp::tests::every_constructed_gauge_is_recorded_on_the_wire` needs `summary().staged_bytes`
    /// to be `Some`, and `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`
    /// forbids `otlp.rs` from spelling the field. The fence allows this file to name it, so the
    /// setter lives here and its name spells none of the fenced names. Test-only: nothing in the
    /// shipped binary can call it.
    #[cfg(test)]
    pub(crate) fn set_staged_bytes_for_test(&mut self, bytes: u64) {
        self.staged_fold_bytes = Some(bytes);
    }

    /// Project the gauge bundle onto § 16.6.7's operator rows.
    pub fn summary(&self) -> StagedTierSummary {
        let hist = &self.staged_fold_shard_duration_secs;
        StagedTierSummary {
            completeness: self.completeness(),
            staged_bytes: self.staged_fold_bytes,
            in_progress_part_bytes: self.staged_fold_in_progress_part_bytes,
            range_count: self.staged_range_count,
            range_budget: self.staged_range_budget,
            range_budget_percent: self.staged_range_count.and_then(|count| {
                (self.staged_range_budget > 0)
                    .then(|| count as f32 / self.staged_range_budget as f32 * 100.0)
            }),
            range_budget_declines_total: self.staged_range_budget_declines_total,
            authority_age_secs: self.staged_authority_age_secs,
            shard_authority_age_secs: self.staged_shard_authority_age_secs,
            max_authority_age_secs: self.staged_authority_max_age_secs,
            shards_beyond_max_age: self.staged_fold_shards_beyond_max_age,
            shards: self.staged_fold_shards,
            fold_incomplete_total: self.staged_fold_incomplete_total,
            shard_budget_capped_total: self.staged_fold_shard_budget_capped_total,
            shard_visits: hist.count,
            shard_visit_mean_ms: (hist.count > 0).then(|| hist.sum_ms / hist.count),
            authority_snapshot_age_secs: self.staged_authority_snapshot_age_secs,
            authority_snapshot_hits_total: self.staged_authority_snapshot_hits_total,
            authority_snapshot_recomputes_total: self.staged_authority_snapshot_recomputes_total,
            fold_completed_age_secs: self.staged_fold_completed_age_secs,
            fold_last_attempt_age_secs: self.staged_fold_last_attempt_age_secs,
            fold_last_attempt_complete: self.staged_fold_last_attempt_complete,
        }
    }
}

/// The gauge set's two shared-volume inputs, read PENDING FIRST, then the checkpoint set.
///
/// The order is `staged_authority_figure_uncached`'s, for the same reason: between the two reads
/// the pending set can only lose records a cut the later checkpoint read sees already covers, and
/// the checkpoint set only moves forward, so the pair cannot under-count. The returned checkpoint
/// set is the collection's ONE traversal of `checkpoints/`, shared with the `st_size` fold's
/// gauges (`MetricsManager::collect_metrics`).
///
/// The checkpoint read runs on the blocking pool: it is synchronous `std::fs` over hundreds of
/// small files, and inline it would block a tokio worker for the whole traversal. A `JoinError`
/// yields the default set, which both consumers publish as the unreadable sentinel rather than a
/// zero; it is logged because "the task panicked" and "no checkpoints" have different owners.
pub async fn read_gauge_inputs(cache_dir: &Path) -> (PendingReadOutcome, ShardCheckpointSet) {
    let pending = PendingStore::new(cache_dir.to_path_buf()).read_all().await;
    #[cfg(test)]
    run_gauge_between_reads_hook(cache_dir).await;
    let dir = cache_dir.to_path_buf();
    let checkpoints = match tokio::task::spawn_blocking(move || read_shard_checkpoints(&dir)).await
    {
        Ok(set) => set,
        Err(e) => {
            warn!(
                "Reading the per-shard checkpoint set failed to complete: {}. The staged-fold and \
                 size-fold gauges will report the unreadable sentinel for this collection rather \
                 than a zero, which would be indistinguishable from an empty staging tier.",
                e
            );
            ShardCheckpointSet::default()
        }
    };
    (pending, checkpoints)
}

/// Compute the gauge set from the installed checkpoint, the pending files, `P`, and the
/// incumbent figure the gap is measured against.
///
/// `in_progress_part_bytes` is `P` as the caller's walk of `mpus_in_progress/` found it: the
/// metrics collection walks once and publishes the same figure here and as
/// `cache.in_progress_part_bytes`, so this function never walks.
///
/// `incumbent_resident_bytes` is `SizeState::write_cache_size` — the figure that still
/// drives eviction. This function reads it only to subtract it.
/// `checkpoints` is supplied by the caller rather than read here, so that `/metrics` traverses
/// `checkpoints/` **once** per collection instead of once per collector. See
/// [`crate::st_size_fold::collect_st_size_fold_gauges`], the other consumer, for the full
/// argument — including why one shared traversal is not merely cheaper but strictly more
/// consistent than two.
///
/// `pending` is supplied by the caller too, and it must have been read BEFORE `checkpoints`:
/// take both from [`read_gauge_inputs`]. The order is the earlier correctness argument (see
/// `staged_authority_figure_uncached`) — reading the checkpoints first and the pending files
/// second let a peer install plus the credit owner's GC between the two reads drop every credit
/// between the old and new cut from both sources, so `staged_fold_bytes` dipped below the true
/// sum (`a_peer_install_between_the_gauge_reads_never_under_counts`).
pub async fn collect_staged_fold_gauges(
    cache_dir: &Path,
    incumbent_resident_bytes: u64,
    pending: &PendingReadOutcome,
    checkpoints: &ShardCheckpointSet,
    in_progress_part_bytes: Option<u64>,
) -> StagedFoldGauges {
    let (snapshot_hits, snapshot_recomputes) = authority_snapshot_counters();
    let now = unix_secs();
    // One extra small file read on the scrape path, alongside the checkpoint readdir and the
    // `.pending` reads this function already does. Read here rather than passed in because the
    // document belongs to this module — the alternative was three fields on `size_state.json`, which
    // `FoldFreshness`'s doc explains why not.
    let freshness = FoldFreshness::load(cache_dir);
    let base = StagedFoldGauges {
        staged_fold_completed_age_secs: freshness
            .and_then(|f| f.completed_at)
            .map(|at| now.saturating_sub(at)),
        staged_fold_last_attempt_age_secs: freshness
            .and_then(|f| f.last_attempt_at)
            .map(|at| now.saturating_sub(at)),
        staged_fold_last_attempt_complete: freshness.and_then(|f| f.last_attempt_complete),
        staged_range_budget: STAGED_RANGE_BUDGET,
        staged_range_budget_declines_total: budget_declines_total(),
        staged_fold_shard_budget_capped_total: shard_budget_capped_total(),
        staged_fold_shards_reachable_per_instance: shards_reachable_per_instance(),
        staged_fold_dirty_shards: dirty_shard_count(cache_dir),
        staged_fold_shard_duration_secs: shard_duration_histogram(),
        staged_fold_incomplete_total: STAGED_FOLD_INCOMPLETE_TOTAL.load(Ordering::Relaxed),
        staged_fold_candidate_read_inflight_peak: candidate_read_inflight_peak(),
        staged_authority_max_age_secs: MAX_AUTHORITY_AGE_SECS,
        staged_authority_snapshot_age_secs: authority_snapshot_age_secs(cache_dir),
        staged_authority_snapshot_hits_total: snapshot_hits,
        staged_authority_snapshot_recomputes_total: snapshot_recomputes,
        ..Default::default()
    };

    // The whole shard set, or nothing. A sum missing one shard is indistinguishable from a
    // smaller cache, so an incomplete read publishes `null` — mixed age is permitted, mixed
    // membership is not. This is the second half of the atomicity argument that replaced I2's
    // single-document one; see `StagedShardCheckpoint`.
    //
    // The set is the caller's, not read here — see this function's doc comment.
    if !checkpoints.is_complete() {
        debug!(
            "Staged fold gauges: {} of {} shard checkpoint(s) unreadable, so the global \
             figure is null rather than short",
            checkpoints.files_unreadable, checkpoints.files_expected
        );
        return base;
    }
    if checkpoints.shards().is_empty() {
        // No fold has completed on this volume. Reporting 0 would be indistinguishable from
        // an empty staging tier.
        return base;
    }

    let byte_sum = checkpoints.staged_byte_sum();
    let freshest = checkpoints.freshest_age_secs();
    let stalest = checkpoints.stalest_age_secs();
    let shards = checkpoints.shards().len() as u64;

    let p = in_progress_part_bytes;

    if !pending.is_complete() || p.is_none() {
        // The components are unknown, so the total is unknown. `F` and the two ages are still
        // knowable and are still published — they came from documents that installed
        // completely.
        return StagedFoldGauges {
            staged_fold_byte_sum: Some(byte_sum),
            staged_fold_in_progress_part_bytes: p,
            staged_authority_age_secs: freshest,
            staged_shard_authority_age_secs: stalest,
            staged_fold_shards: shards,
            staged_fold_shards_beyond_max_age: checkpoints.shards_beyond_max_age(),
            ..base
        };
    }

    let post_cut = pending.post_cut(checkpoints);
    let pending_bytes = post_cut
        .iter()
        .fold(0u64, |acc, c| acc.saturating_add(c.bytes));
    let awaiting = post_cut.len() as u64;
    let p = p.unwrap_or(0);
    let total = byte_sum.saturating_add(pending_bytes).saturating_add(p);

    StagedFoldGauges {
        staged_fold_bytes: Some(total),
        staged_fold_byte_sum: Some(byte_sum),
        staged_fold_pending_bytes: Some(pending_bytes),
        staged_fold_in_progress_part_bytes: Some(p),
        // `None` once the epoch is active: § 16.6.7 removes this gauge at activation because
        // there is no second producer left to compare against. Publishing a number here would
        // be a comparison of the fold against a counter nothing reads any more, and a reader
        // would take a small value as agreement rather than as irrelevance.
        staged_fold_gap_bytes: if crate::staged_authority::fold_authority_active_latched(cache_dir)
        {
            None
        } else {
            Some(
                (total as i128 - incumbent_resident_bytes as i128)
                    .clamp(i64::MIN as i128, i64::MAX as i128) as i64,
            )
        },
        staged_authority_age_secs: freshest,
        staged_shard_authority_age_secs: stalest,
        staged_fold_shards: shards,
        staged_fold_shards_beyond_max_age: checkpoints.shards_beyond_max_age(),
        staged_range_count: Some(checkpoints.staged_range_count().saturating_add(awaiting)),
        staged_mutations_awaiting_fold: Some(awaiting),
        ..base
    }
}

/// The authoritative staged figure, split into its **two disjoint on-disk populations**.
///
/// This is the fold's one decision-bearing output and the only thing the flip
/// (§ 16.6.2) reads. It is deliberately a different type from [`StagedFoldGauges`]: that
/// one is an observability bundle of many fields, and handing a decision path the whole struct
/// invites it to branch on the ones it has no business reading.
///
/// # Why the split is not cosmetic
///
/// The three published composites need the two terms **separately**, because they sit in
/// different relationships to the persisted `total_size`:
///
/// ```text
/// total_cache_size := total_size + in_progress_part_bytes
/// read_cache_size  := total_size − resident_staged_bytes
/// write_cache_size := resident_staged_bytes + in_progress_part_bytes
/// ```
///
/// `resident_staged_bytes` is a **subset** of `total_size` (those ranges are under
/// `ranges/`, which is what `total_size` counts), so it subtracts. `in_progress_part_bytes`
/// is **disjoint** from it (those bytes are under `mpus_in_progress/`, which the `st_size`
/// fold excludes by an explicit scope guard), so it adds. Collapsing the two into one figure
/// would make both derivations unexpressible and the published identity
/// `total == read + write` inexact by `P`.
///
/// # There is exactly one consumer and that is asserted
///
/// `CacheManager::staged_authority`, via
/// [`staged_authority_figure`]. `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface`
/// fails if a second file names that function.
///
/// Spec: cache-eviction-at-scale §§ 16.6.1, 16.6.2, 16.6.6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedAuthorityFigure {
    /// `F` + the post-cut pending term: staged compressed bytes that are on the volume
    /// **under `ranges/`**, and therefore a subset of the persisted `total_size`.
    ///
    /// The pending term belongs here rather than beside `P` because a credited range's
    /// `.bin` has already been renamed into `ranges/` by the time its `Pending_Credit` is
    /// appended (§ 16.6.2, step 2 of the handoff table). So it is resident, merely not yet
    /// folded.
    pub resident_staged_bytes: u64,
    /// `P`: summed `compressed_size` over `mpus_in_progress/*/part{N}.json` — bytes on the
    /// volume that `total_size` does **not** count.
    pub in_progress_part_bytes: u64,
}

impl StagedAuthorityFigure {
    /// `write_cache_size`: `F + pending + P`, C's equation.
    pub fn write_cache_size(&self) -> u64 {
        self.resident_staged_bytes
            .saturating_add(self.in_progress_part_bytes)
    }
}

tokio::task_local! {
    /// Set for the duration of an observability read by [`as_scrape`].
    static SCRAPE_READ: ();
}

/// Run `fut` as a scrape: an observability read (`/metrics`, the dashboard, `/health`) that must
/// answer promptly and never decides anything.
///
/// # What changes inside the scope
///
/// Two things, and every other caller, the staging eviction trigger included, is unaffected and
/// keeps the 5 s bound [`AUTHORITY_SNAPSHOT_TTL_SECS`] documents:
///
/// - When the memoised authority figure has expired, [`staged_authority_figure`] returns the
///   expired figure at once and starts one background refresh, instead of re-deriving it on the
///   request.
/// - A scrape never starts a walk of `mpus_in_progress/`, on the request or in that refresh. The
///   refresh re-reads `F` and the pending term and takes `P` from the last walk this process kept
///   (the metrics collector's or a deciding reader's); see
///   [`rederive_authority_snapshot_from_kept_walk`]. With no memo at all a scrape derives the
///   same way inline, and with no kept walk either it gets `None`, the incumbent fallback.
///
/// # Why a scrape must not re-derive
///
/// The derivation is a locked read of every `.pending` file (an exclusive `fs4` lock per file,
/// whose holder can be another proxy, with no timeout) followed by a synchronous traversal of the
/// whole checkpoint set on the calling tokio worker — about 1,500 shard documents on the fleet's
/// EFS volume. A scrape that arrived more than 5 s after the last authority read paid all of it,
/// and a suite that reads `/metrics` between fixtures arrives that way nearly every time. The
/// whole body is one serialisation, so a scrape that outran its client's deadline lost every
/// series, not only the staged ones.
///
/// # The staleness this accepts
///
/// A scrape's `F` and pending term are at most one refresh behind: the expired figure is served
/// once, the refresh installs a fresh one, and the next scrape reads that. A poller waiting for the
/// staged figure to move sees it one read later than before, not a 60 s collection later. Its `P`
/// is as old as the last kept walk: at most one `metrics.collection_interval`, the same `P` the
/// `cache.in_progress_part_bytes` field serves.
///
/// Task-local rather than a parameter so the scope reaches through `CacheManager` without widening
/// every signature between the scrape and the memo. A task-local does not cross `tokio::spawn`, so
/// the refresh itself runs as an ordinary reader.
pub(crate) async fn as_scrape<F: std::future::Future>(fut: F) -> F::Output {
    SCRAPE_READ.scope((), fut).await
}

/// Whether the current task is inside [`as_scrape`].
fn in_scrape() -> bool {
    SCRAPE_READ.try_with(|_| ()).is_ok()
}

/// Cache directories with an authority refresh in flight, keyed by [`dir_tag`].
///
/// Single-flight for scrape-triggered refreshes: a burst of scrapes behind one expired memo starts
/// one derivation, not one each.
static AUTHORITY_REFRESH_INFLIGHT: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());

/// Clears a directory's in-flight mark when the refresh ends, including by panic.
struct AuthorityRefreshGuard(u64);

impl Drop for AuthorityRefreshGuard {
    fn drop(&mut self) {
        AUTHORITY_REFRESH_INFLIGHT
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.0);
    }
}

/// Start one background re-derivation of `cache_dir`'s authority figure, unless one is running.
fn spawn_authority_refresh(cache_dir: &Path) {
    let tag = dir_tag(cache_dir);
    if !AUTHORITY_REFRESH_INFLIGHT
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(tag)
    {
        return;
    }
    let guard = AuthorityRefreshGuard(tag);
    let dir = cache_dir.to_path_buf();
    // On the blocking pool, not a runtime worker: the derivation's
    // `read_shard_checkpoints` is a synchronous readdir plus a read of every checkpoint file on
    // shared storage, and a scrape is this refresh's only trigger, so on a worker `/health`
    // polling itself pinned one. `block_on` drives the derivation's async reads from this
    // thread.
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        // `now` at the START of the derivation, as the inline path records it, so the installed
        // figure never claims to be younger than its inputs.
        let _ = handle.block_on(rederive_authority_snapshot_from_kept_walk(
            &dir,
            tag,
            unix_secs(),
        ));
    });
}

/// The staged figure the trigger and the published composites read, or `None` when it is
/// unknown.
///
/// `None` — never a zero — when the shard checkpoints are incomplete, when no fold has ever
/// completed on this volume, when a `.pending` file is unreadable, or when `P` is unreadable.
/// Every one of those states is indistinguishable from an empty staging tier if reported as
/// `0`, and an empty staging tier is the healthiest possible reading: it would silently stop
/// staging eviction. The caller's obligation on `None` is to fall back to a figure that
/// **over**-states rather than under-states — see `CacheManager::staged_authority`.
///
/// Shares its inputs with [`collect_staged_fold_gauges`] and computes the same terms, but is
/// a separate entry point on purpose: the gauge collector also computes the incumbent gap,
/// the ages, the histogram and the budget counters, none of which a decision may read.
///
/// # Cost
///
/// Memoised for [`AUTHORITY_SNAPSHOT_TTL_SECS`], because the inputs are a readdir over the
/// checkpoint set, a locked read of every `.pending` file, and an `mpus_in_progress/` traversal —
/// all shared-storage I/O — and this is called on **every** `get_cache_size_stats` (so every
/// dashboard refresh and every `/metrics` scrape) and on **every** staging eviction check (so the
/// write path). One memo serves both call sites, because both reach here through
/// `CacheManager::staged_authority`. It is expired by every fold install, so no deciding reader is
/// served a figure the fold has just changed. See the TTL constant for the freshness argument.
///
/// Inside [`as_scrape`] an expired figure is served and refreshed in the background instead of
/// re-derived; an absent one (before the process's first derivation) is still derived inline.
/// Neither walks `mpus_in_progress/`: both take `P` from the last kept walk.
pub(crate) async fn staged_authority_figure(cache_dir: &Path) -> Option<StagedAuthorityFigure> {
    let now_secs = unix_secs();
    let tag = dir_tag(cache_dir);

    let scrape = in_scrape();
    // Lock held only around the map access, never across the derivation below.
    let mut expired: Option<AuthoritySnapshot> = None;
    if let Ok(map) = AUTHORITY_SNAPSHOT.lock() {
        if let Some(snapshot) = map.get(&tag) {
            let fresh_for_reader = if scrape {
                snapshot.derived_at_secs
            } else {
                snapshot.taken_at_secs
            };
            if now_secs.saturating_sub(fresh_for_reader) < AUTHORITY_SNAPSHOT_TTL_SECS {
                AUTHORITY_SNAPSHOT_HITS.fetch_add(1, Ordering::Relaxed);
                return snapshot.figure;
            }
            expired = Some(*snapshot);
        }
    }

    // A scrape never re-derives an expired figure itself, and never walks `mpus_in_progress/`: it
    // serves the expired one and starts one background refresh that takes `P` from the last kept
    // walk. See [`as_scrape`] for why. With no memo at all (only before this process's first
    // derivation, which the background collector performs at startup) there is nothing to serve,
    // so it derives inline the same way, without a walk.
    if scrape {
        if let Some(snapshot) = expired {
            spawn_authority_refresh(cache_dir);
            return snapshot.figure;
        }
        return rederive_authority_snapshot_from_kept_walk(cache_dir, tag, now_secs).await;
    }

    rederive_authority_snapshot(cache_dir, tag, now_secs).await
}

/// Derive `cache_dir`'s authority figure from shared storage and install it in the memo as taken
/// at `now_secs`. One of two places a figure is installed in the memo; the other is
/// [`rederive_authority_snapshot_from_kept_walk`], which builds the figure from the walk the
/// background collector kept instead of walking the volume. `expire_authority_snapshot` also
/// mutates the memo, but only zeroes the timestamps of a figure already there.
async fn rederive_authority_snapshot(
    cache_dir: &Path,
    tag: u64,
    now_secs: u64,
) -> Option<StagedAuthorityFigure> {
    AUTHORITY_SNAPSHOT_RECOMPUTES.fetch_add(1, Ordering::Relaxed);
    let fresh = staged_authority_figure_uncached(cache_dir).await;

    if let Ok(mut map) = AUTHORITY_SNAPSHOT.lock() {
        // Only a test binary can reach this bound, and clearing rather than evicting is
        // deliberate: there is no useful eviction order over one-entry-per-fixture, and a bounded
        // map that silently kept the FIRST keys would make a later fixture's reading depend on how
        // many ran before it.
        if map.len() >= AUTHORITY_SNAPSHOT_MAX_KEYS && !map.contains_key(&tag) {
            map.clear();
        }
        map.insert(
            tag,
            AuthoritySnapshot {
                taken_at_secs: now_secs,
                derived_at_secs: now_secs,
                figure: fresh,
            },
        );
    }
    fresh
}

/// A scrape's derivation: [`rederive_authority_snapshot`] with `P` from the last walk of
/// `mpus_in_progress/` this process kept, so a scrape never starts one.
///
/// # A deciding reader is never handed an older figure than it accepts
///
/// The memo is installed as taken when that walk BEGAN (its age rounded up), not now. A deciding
/// reader accepts a memo younger than [`AUTHORITY_SNAPSHOT_TTL_SECS`] by `taken_at_secs`, so it
/// accepts this one only while the walk that produced its `P` is that young, and `F` and the
/// pending term are read later still. Every input is at least as fresh as the TTL a deciding
/// reader's own derivation guarantees. Once the walk is older, the memo is expired for a decider,
/// which re-derives with a walk of its own exactly as before; a scrape keeps serving it until
/// `derived_at_secs` expires.
///
/// Never replaces a memo whose `taken_at_secs` is newer, so a scrape cannot push a deciding
/// reader's fresher `P` out.
///
/// `None` when this process has kept no walk yet: there is no `P` to pair with, and the caller
/// serves the incumbent fallback. Nothing is installed then.
async fn rederive_authority_snapshot_from_kept_walk(
    cache_dir: &Path,
    tag: u64,
    now_secs: u64,
) -> Option<StagedAuthorityFigure> {
    let kept = crate::signed_put_handler::last_in_progress_observation(cache_dir)?;
    let walk_age = kept.began_at.elapsed();
    let walk_age_secs = walk_age
        .as_secs()
        .saturating_add(u64::from(walk_age.subsec_nanos() > 0));
    let taken_at_secs = now_secs.saturating_sub(walk_age_secs);
    AUTHORITY_SNAPSHOT_RECOMPUTES.fetch_add(1, Ordering::Relaxed);
    let fresh = staged_authority_figure_from(cache_dir, Some(kept.walk)).await;

    if let Ok(mut map) = AUTHORITY_SNAPSHOT.lock() {
        if map
            .get(&tag)
            .is_some_and(|current| current.taken_at_secs > taken_at_secs)
        {
            return fresh;
        }
        // The same bound as the deciding derivation; see there.
        if map.len() >= AUTHORITY_SNAPSHOT_MAX_KEYS && !map.contains_key(&tag) {
            map.clear();
        }
        map.insert(
            tag,
            AuthoritySnapshot {
                taken_at_secs,
                derived_at_secs: now_secs,
                figure: fresh,
            },
        );
    }
    fresh
}

/// A one-shot action a test runs between [`staged_authority_figure_uncached`]'s two shared-volume
/// reads, keyed by [`dir_tag`]. Tests only: it is how a fixture puts a peer's install and the
/// credit owner's pending GC inside the reader's window, which in production is a race.
#[cfg(test)]
type AuthorityBetweenReadsHook =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;

#[cfg(test)]
static AUTHORITY_BETWEEN_READS_HOOK: Mutex<BTreeMap<u64, AuthorityBetweenReadsHook>> =
    Mutex::new(BTreeMap::new());

/// Register `hook` to run once, between the next authority derivation's two reads for
/// `cache_dir`.
#[cfg(test)]
fn set_authority_between_reads_hook_for_tests(cache_dir: &Path, hook: AuthorityBetweenReadsHook) {
    AUTHORITY_BETWEEN_READS_HOOK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(dir_tag(cache_dir), hook);
}

/// Run and consume `cache_dir`'s registered hook, if any. The guard is dropped before the await.
#[cfg(test)]
async fn run_authority_between_reads_hook(cache_dir: &Path) {
    let hook = AUTHORITY_BETWEEN_READS_HOOK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&dir_tag(cache_dir));
    if let Some(hook) = hook {
        hook().await;
    }
}

/// The same seam for the `/metrics` gauge set's two reads, kept apart from the authority's
/// so a `/metrics` collection — which also derives the authority — consumes only the hook meant
/// for the gauge reader.
#[cfg(test)]
static GAUGE_BETWEEN_READS_HOOK: Mutex<BTreeMap<u64, AuthorityBetweenReadsHook>> =
    Mutex::new(BTreeMap::new());

/// Register `hook` to run once, between the next gauge collection's two reads for `cache_dir`.
#[cfg(test)]
fn set_gauge_between_reads_hook_for_tests(cache_dir: &Path, hook: AuthorityBetweenReadsHook) {
    GAUGE_BETWEEN_READS_HOOK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(dir_tag(cache_dir), hook);
}

/// Run and consume `cache_dir`'s registered gauge hook, if any.
#[cfg(test)]
async fn run_gauge_between_reads_hook(cache_dir: &Path) {
    let hook = GAUGE_BETWEEN_READS_HOOK
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&dir_tag(cache_dir));
    if let Some(hook) = hook {
        hook().await;
    }
}

/// [`staged_authority_figure`] without the memo — the derivation itself.
async fn staged_authority_figure_uncached(cache_dir: &Path) -> Option<StagedAuthorityFigure> {
    staged_authority_figure_from(cache_dir, None).await
}

/// The derivation, with `P` from `kept` when given (a scrape's refresh) or from a walk of its own,
/// or the one in flight, when not (every deciding reader).
async fn staged_authority_figure_from(
    cache_dir: &Path,
    kept: Option<crate::signed_put_handler::InProgressWalk>,
) -> Option<StagedAuthorityFigure> {
    // PENDING FIRST, then checkpoints. The order is the correctness argument, not a style choice.
    //
    // A credit leaves a `.pending` file only through its owner's GC, and only once an installed
    // checkpoint's cut covers it — possibly a PEER's install, since the GC reads every cut. So
    // between two reads the pending set can only lose records some checkpoint already counts,
    // and the checkpoint set can only move forward. Read pending, then checkpoints: a record
    // missing from the pending read is covered by a cut the later checkpoint read sees, and a
    // record present in it is either counted by the pending term or filtered by that same newer
    // cut. The reverse order took the OLD cut and the NEW pending file, so every credit between
    // the two cuts was in neither source and the figure under-stated — the direction that makes
    // the staging trigger fire late (`a_peer_install_between_the_authority_reads_never_under_counts`).
    let pending = PendingStore::new(cache_dir.to_path_buf()).read_all().await;
    if !pending.is_complete() {
        return None;
    }
    #[cfg(test)]
    run_authority_between_reads_hook(cache_dir).await;
    let checkpoints = read_shard_checkpoints(cache_dir);
    if !checkpoints.is_complete() || checkpoints.shards().is_empty() {
        return None;
    }
    // `P`: a full walk no older than `PUBLISHED_WALK_BASE_MAX_AGE` plus every instance's
    // published growth since that walk began, which in the steady state is the metrics
    // collector's walk and one small document per proxy, and no walk of this reader's own.
    //
    // Dating the base by its start, not its end, is what bounds the under-count: a part recorded
    // after the walk began is never claimed as seen by it, and is counted instead by its
    // writer's monotone `added_ever` term. So the figure can be short only of a peer's parts
    // recorded since that peer's last publication, at most
    // `published_figure_lease_secs(consolidation_interval)`, and never of parts already on the
    // volume when the base walk began. The direction a stale base does carry is over-count (a
    // removal it has not seen), which makes the staging trigger fire early rather than late.
    //
    // Three staleness bounds stack here, which `total_composed_staleness_bound_secs` states as
    // one figure: the base walk's age, a peer's publication lag inside its lease, and this
    // figure's own `AUTHORITY_SNAPSHOT_TTL_SECS` memoisation. A scrape's refresh passes the kept
    // walk, and its memo entry is dated by that walk so no decider takes it past the TTL (see
    // `rederive_authority_snapshot_from_kept_walk`).
    let p = match kept {
        Some(walk) => walk.bytes?,
        None => {
            crate::signed_put_handler::compose_in_progress_part_bytes(
                cache_dir,
                crate::signed_put_handler::PUBLISHED_WALK_BASE_MAX_AGE,
            )
            .await
            .bytes?
        }
    };
    Some(StagedAuthorityFigure {
        resident_staged_bytes: checkpoints
            .staged_byte_sum()
            .saturating_add(pending.post_cut_bytes(&checkpoints)),
        in_progress_part_bytes: p,
    })
}

/// Whether this volume's staged fold has completed one full rotation, with every shard it
/// knows about carrying a figure no older than [`MAX_AUTHORITY_AGE_SECS`].
///
/// § 16.6.6's third activation precondition, expressed as the state that is actually
/// observable. Returns a **bool and never a figure**, which is the same containment
/// [`budget_verdict`] uses and the reason `staged_authority.rs` can consult the fold's
/// rotation state without being able to read its bytes.
///
/// `false` on an incomplete checkpoint read and on a volume with no checkpoints at all —
/// both are "we cannot show a rotation completed", which is what the gate needs.
pub fn rotation_complete(cache_dir: &Path) -> bool {
    rotation_complete_given(rotation_evidence(cache_dir), 0)
}

/// [`rotation_complete`]'s input, read once: the stalest shard's age in seconds when the
/// checkpoint set is complete and non-empty, `None` otherwise.
///
/// Split out so `JournalConsolidator::maintain_staged_authority` can read the checkpoint set
/// once and re-evaluate the age against a later `now` with [`rotation_complete_given`], instead of
/// re-reading every checkpoint document on every non-idle cycle. `None` carries every "cannot show
/// a rotation" case, so it can never become `true` by ageing.
pub(crate) fn rotation_evidence(cache_dir: &Path) -> Option<u64> {
    let checkpoints = read_shard_checkpoints(cache_dir);
    if !checkpoints.is_complete() || checkpoints.shards().is_empty() {
        return None;
    }
    checkpoints.stalest_age_secs()
}

/// [`rotation_complete`] over evidence read `elapsed_secs` ago.
///
/// The age only grows between reads unless a pass installs a fresher checkpoint, so adding the
/// elapsed time gives an age no younger than a fresh read would: a cached `true` turns `false` no
/// later than the uncached predicate would. The caller must round `elapsed_secs` UP.
pub(crate) fn rotation_complete_given(evidence: Option<u64>, elapsed_secs: u64) -> bool {
    evidence.is_some_and(|age| age.saturating_add(elapsed_secs) <= MAX_AUTHORITY_AGE_SECS)
}

/// `staged_fold_incomplete_total`, per-instance since process start.
///
/// Exposed so `staged_authority`'s readiness record can carry it without reaching into this
/// module's statics. A count, not a figure.
pub(crate) fn fold_incomplete_total() -> u64 {
    STAGED_FOLD_INCOMPLETE_TOTAL.load(Ordering::Relaxed)
}

fn age_secs(scanned_at_nanos: u64) -> u64 {
    now_nanos().saturating_sub(scanned_at_nanos) / 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The authority figure's `P` is composed on a kept walk of `mpus_in_progress/` that
    /// STARTED less than `PUBLISHED_WALK_BASE_MAX_AGE` ago, plus every proxy's published growth
    /// since it: deciders share the walk the metrics collector makes once a minute instead of
    /// taking one of their own. A base walk older than that bound is not reused.
    ///
    /// Both directions in one test, because a reader that accepts a base of ANY age passes the
    /// first three terms: the same kept walk is offered at 1 s, at 6 s (where a decider used to
    /// walk again and now does not) and past the base bound, through the seam that dates it,
    /// since the calls are microseconds apart. The fixture writes its part records itself, so no
    /// published figure moves and the growth term is zero throughout: what is under test is which
    /// base a decider will reuse.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_authority_figure_composes_on_a_base_walk_inside_its_bound() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        seed_shard_figure_for_tests(dir, "test-bucket", "3f", 77, 4);
        let write_part = |upload: &str, bytes: u64| {
            let upload = crate::signed_put_handler::mpu_upload_dir(dir, upload);
            std::fs::create_dir_all(&upload).unwrap();
            std::fs::write(
                upload.join("part1.json"),
                format!(r#"{{"compressed_size":{bytes}}}"#),
            )
            .unwrap();
        };
        write_part("case78-c-a", 1_000);
        let kept = crate::signed_put_handler::observe_in_progress(dir, Duration::ZERO).await;
        write_part("case78-c-b", 5_000);
        crate::signed_put_handler::age_last_in_progress_walk_for_tests(dir, Duration::from_secs(1));
        invalidate_authority_snapshot(dir);
        let shared = staged_authority_figure(dir)
            .await
            .map(|f| f.in_progress_part_bytes);
        crate::signed_put_handler::age_last_in_progress_walk_for_tests(dir, Duration::from_secs(5));
        invalidate_authority_snapshot(dir);
        let still_shared = staged_authority_figure(dir)
            .await
            .map(|f| f.in_progress_part_bytes);
        crate::signed_put_handler::age_last_in_progress_walk_for_tests(
            dir,
            crate::signed_put_handler::PUBLISHED_WALK_BASE_MAX_AGE,
        );
        invalidate_authority_snapshot(dir);
        let walked_again = staged_authority_figure(dir)
            .await
            .map(|f| f.in_progress_part_bytes);
        assert_eq!(
            (kept.walk.bytes, shared, still_shared, walked_again),
            (Some(1_000), Some(1_000), Some(1_000), Some(6_000)),
            "(P in the kept walk, P composed on that walk 1 s old, on it 6 s old, on it past \
             PUBLISHED_WALK_BASE_MAX_AGE): a decider composes on a base inside the bound and \
             walks again only past it"
        );
    }

    /// Every spelling of [`STAGED_RANGE_BUDGET`] an author might reach for.
    ///
    /// Shared by the two guards below so neither has to spell them inline — a literal in a guard's
    /// own source is indistinguishable from a literal in a display, which is why the scan skips
    /// any line naming `SPELLINGS`.
    const BUDGET_SPELLINGS: [&str; 3] = ["100000", "100_000", "100,000"];
    use crate::write_ledger::{ledger_seq, WriteLedgerEntry};
    use std::time::Duration;
    use tempfile::TempDir;

    /// Log `e` through `log_pending_read_join_failure` under a thread-local DEBUG subscriber and
    /// return the captured output.
    fn capture_join_failure_log(e: &tokio::task::JoinError) -> String {
        #[derive(Clone)]
        struct Buffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buffer(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            log_pending_read_join_failure(Path::new("/cache/_shard_state/p/host:1.pending"), e)
        });
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    /// A pending-credit read task cancelled at runtime shutdown logs at DEBUG; a panicked one
    /// still logs at WARN. A run recorded 38 cancelled-task WARNs on one proxy 0.5 s before a
    /// restart.
    ///
    /// # RED SIDE
    ///
    /// With `log_pending_read_join_failure`'s `is_cancelled()` branch removed (always `warn!`),
    /// the cancelled case logs at WARN and the first assertion fails.
    #[tokio::test]
    async fn a_cancelled_pending_read_task_logs_at_debug_and_a_panic_at_warn() {
        let cancelled = {
            let handle = tokio::spawn(std::future::pending::<()>());
            handle.abort();
            handle.await.unwrap_err()
        };
        assert!(cancelled.is_cancelled(), "FIXTURE: must be a cancellation");
        let log = capture_join_failure_log(&cancelled);
        assert!(
            log.contains(" DEBUG ")
                && log.contains("Pending-credit read task cancelled for")
                && !log.contains(" WARN "),
            "a cancelled read task must log at DEBUG, not WARN:\n{}",
            log
        );

        let panicked = tokio::spawn(async { panic!("SCN1 test panic") })
            .await
            .unwrap_err();
        assert!(panicked.is_panic(), "FIXTURE: must be a panic");
        let log = capture_join_failure_log(&panicked);
        assert!(
            log.contains(" WARN ") && log.contains("Pending-credit read task failed for"),
            "a panicked read task must still log at WARN:\n{}",
            log
        );
    }

    /// Write a `.meta` at `cache_key`'s sharded path with one staged range of `bytes`
    /// compressed bytes.
    ///
    /// `staged: None` on the range, so membership falls through to the object flag and the
    /// fixture exercises the same `is_staged_range_spec` fall-through production writes.
    async fn seed_staged_meta(
        cache_dir: &Path,
        cache_key: &str,
        bytes: u64,
        created_at: SystemTime,
    ) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                is_write_cached: true,
                ..Default::default()
            },
            ranges: vec![crate::cache_types::RangeSpec {
                start: 0,
                end: bytes.saturating_sub(1),
                file_path: format!("{}_0-{}.bin", cache_key.replace('/', "%2F"), bytes - 1),
                compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                compressed_size: bytes,
                uncompressed_size: bytes,
                created_at,
                last_accessed: created_at,
                access_count: 0,
                staged: None,
                unservable: false,
                file_identity: None,
            }],
            created_at,
            ..Default::default()
        };
        tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
            .await
            .unwrap();
    }

    /// Publish a staged range the way a single-part PUT does: `.meta` first, then the ledger
    /// entry, then the pending credit — the last two through `append_staged_entry`, which is
    /// `append_staged_range`'s own body, so the two halves are stamped with one `Ledger_Seq`
    /// by the code under test rather than by the fixture. A multipart completion appends
    /// before it renames its `.meta` into place; that order has its own fixture in
    /// `a_multipart_completion_folded_between_its_append_and_its_rename_is_counted_once`.
    ///
    /// `credited_at` is the entry's wall-clock **timestamp** only. It is a parameter so a
    /// test can move the clock backwards; it must not affect which side of a cut the credit
    /// lands on, and `credit_is_counted_once_when_the_clock_steps_backwards` is what asserts
    /// that.
    async fn publish_staged_range_at(
        cache_dir: &Path,
        ledger: &WriteLedger,
        cache_key: &str,
        bytes: u64,
        credited_at: SystemTime,
    ) -> u64 {
        seed_staged_meta(
            cache_dir,
            cache_key,
            bytes,
            credited_at - Duration::from_secs(5),
        )
        .await;
        ledger
            .append_staged_entry(WriteLedgerEntry {
                timestamp: credited_at,
                cache_key: cache_key.to_string(),
                range_start: 0,
                range_end: bytes - 1,
                compressed_size: bytes,
                instance_id: ledger.instance_id().to_string(),
                // Overwritten by the append, exactly as production's is.
                seq: None,
            })
            .await
            .unwrap();
        // The seq the credit actually carries, read back from the ledger rather than
        // recomputed, so the fixture cannot disagree with the code.
        let read = ledger.read_for_authority(0).await.unwrap();
        read.entries
            .iter()
            .filter(|e: &&WriteLedgerEntry| e.cache_key == cache_key)
            .filter_map(ledger_seq)
            .max()
            .expect("the entry just appended must be readable and must carry a seq")
    }

    /// `publish_staged_range_at` with the clock where it actually is.
    async fn publish_staged_range(
        cache_dir: &Path,
        ledger: &WriteLedger,
        cache_key: &str,
        bytes: u64,
    ) -> u64 {
        publish_staged_range_at(cache_dir, ledger, cache_key, bytes, SystemTime::now()).await
    }

    /// Assert the pass installed, and return its summary.
    fn installed(outcome: &FoldOutcome) -> &FoldPassSummary {
        match outcome {
            FoldOutcome::Installed(s) => s,
            FoldOutcome::Retained(r, _) => {
                panic!("expected an install, got retain: {}", r.label())
            }
        }
    }

    /// Fold until every shard the pass can see has a figure, or `max_passes` is spent.
    ///
    /// # Why a loop is now the honest fixture
    ///
    /// `shards_per_pass` is `ceil(observed / 12)`, so a two-shard fixture gets a budget of **one**
    /// and the global sum converges over two passes rather than one. That is correct scheduling —
    /// two shards at one per 300 s pass is a 600 s revisit, well inside the 3,600 s bound — and it
    /// is § 3.6's mixed-age sum working as designed. It also means a test asserting "the sum is
    /// A + B after one fold" was asserting a scheduling accident of the old fixed budget of 8.
    ///
    /// Returns the number of passes that installed something.
    async fn fold_to_convergence(
        dir: &Path,
        ledger: &WriteLedger,
        instance: &str,
        max_passes: usize,
    ) -> usize {
        let mut installs = 0;
        for _ in 0..max_passes {
            let outcome = run_staged_fold(dir, ledger, instance).await;
            match &outcome {
                FoldOutcome::Installed(s) => {
                    installs += 1;
                    if s.installed.is_empty() {
                        break;
                    }
                }
                // Nothing left to visit, or a genuine incompleteness the caller will observe.
                FoldOutcome::Retained(_, _) => break,
            }
        }
        installs
    }

    /// `F` as published: Σ `staged_byte_sum` over every installed shard.
    ///
    /// Test keys hash to different `L1`s, so a fixture with two objects is a fixture with
    /// **two shards** — which is what makes these tests exercise partitioning rather than
    /// re-testing the single-document case under a new name.
    fn folded_bytes(dir: &Path) -> u64 {
        let set = read_shard_checkpoints(dir);
        assert!(
            set.is_complete(),
            "the fixture's checkpoints must be readable"
        );
        set.staged_byte_sum()
    }

    /// the multipart completion order (temp `.meta`, ledger entry and pending
    /// credit, then the rename) with a fold between the append and the rename. The object's
    /// staged bytes must be in exactly one of `F` and the pending term after that fold, and
    /// still exactly one after the next.
    ///
    /// # RED SIDE
    ///
    /// The fold installing a cut over the entry while the `.meta` it read was absent: `F`
    /// excludes the object, `post_cut` filters its credit, and the sum reads 0.
    #[tokio::test]
    async fn a_multipart_completion_folded_between_its_append_and_its_rename_is_counted_once() {
        const BYTES: u64 = 4096;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let key = "bucket/mpu.bin";

        // The shard was folded before, at a cut below anything this test appends, so the
        // gauges publish a figure (no checkpoint at all publishes `null`).
        let (bucket, l1) = shard_of(key).expect("the fixture key must be shardable");
        seed_shard_cuts_for_tests(dir, &bucket, &l1, &[("host:1", 0)]);

        // The completion's temp `.meta`, at the instance-scoped temp path production uses.
        seed_staged_meta(dir, key, BYTES, SystemTime::now() - Duration::from_secs(5)).await;
        let meta_path =
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
        let temp_path = crate::cache::instance_scoped_meta_tmp_path(&meta_path);
        std::fs::rename(&meta_path, &temp_path).unwrap();

        // Ledger entry and pending credit, through the production pairing.
        ledger
            .append_staged_range(key, 0, BYTES - 1, BYTES)
            .await
            .unwrap();

        // The fold, between the append and the rename.
        let _ = tokio::time::timeout(
            Duration::from_secs(30),
            run_staged_fold(dir, &ledger, "host:1"),
        )
        .await
        .expect("the fold did not finish within 30 s");

        // The rename.
        std::fs::rename(&temp_path, &meta_path).unwrap();

        let g = gauges_for_tests(dir, 0).await;
        assert!(
            g.staged_fold_byte_sum.is_some() && g.staged_fold_pending_bytes.is_some(),
            "FIXTURE: the gauges must publish both terms, or the sum below is vacuous: {:?}",
            g
        );
        let folded = g.staged_fold_byte_sum.unwrap_or(0);
        let pending = g.staged_fold_pending_bytes.unwrap_or(0);
        assert_eq!(
            folded + pending,
            BYTES,
            "after a fold between the append and the rename the object must be counted once: \
             F={} pending={} (expected the sum {})",
            folded,
            pending,
            BYTES
        );

        // The next pass reads the published `.meta`: F takes the bytes and the cut retires the
        // credit, still once.
        let _ = tokio::time::timeout(
            Duration::from_secs(30),
            run_staged_fold(dir, &ledger, "host:1"),
        )
        .await
        .expect("the second fold did not finish within 30 s");
        let g = gauges_for_tests(dir, 0).await;
        let folded = g.staged_fold_byte_sum.unwrap_or(0);
        let pending = g.staged_fold_pending_bytes.unwrap_or(0);
        assert_eq!(
            (folded, pending),
            (BYTES, 0),
            "after the next pass the fold must count the published object and the cut must \
             retire its credit: F={} pending={}",
            folded,
            pending
        );
    }

    /// The installed cut for the shard holding `cache_key`, for `instance`.
    /// Post-cut credits naming one range count once, largest bytes kept; credits without a
    /// range identity (a record from a build before the field) are all counted, and such a
    /// record still parses.
    #[test]
    fn post_cut_counts_one_range_once_and_keeps_legacy_records() {
        let range = |key: &str| {
            Some(PendingRange {
                cache_key: key.to_string(),
                start: 0,
                end: 1023,
            })
        };
        let credit = |seq: u64, bytes: u64, r: Option<PendingRange>| PendingCredit {
            seq,
            bucket: "bucket".to_string(),
            l1: "aa".to_string(),
            bytes,
            range: r,
        };
        let legacy: PendingCredit =
            serde_json::from_str(r#"{"seq":7,"bucket":"bucket","l1":"aa","bytes":100}"#)
                .expect("a record without a range identity must still parse");
        assert_eq!(legacy.range, None);
        let outcome = PendingReadOutcome {
            credits: vec![
                ("host:a".to_string(), credit(1, 512, range("bucket/x"))),
                ("host:b".to_string(), credit(1, 600, range("bucket/x"))),
                ("host:b".to_string(), credit(2, 256, range("bucket/y"))),
                ("host:a".to_string(), legacy.clone()),
                ("host:b".to_string(), legacy),
            ],
            files_expected: 2,
            files_read: 2,
            files_unreadable: 0,
            malformed_lines: 0,
        };
        let empty = ShardCheckpointSet::default();
        assert_eq!(outcome.post_cut(&empty).len(), 4);
        assert_eq!(outcome.post_cut_bytes(&empty), 600 + 256 + 100 + 100);
    }

    fn cut_for(dir: &Path, cache_key: &str, instance: &str) -> u64 {
        let (bucket, l1) = shard_of(cache_key).expect("the fixture key must be shardable");
        read_shard_checkpoints(dir)
            .get(&Shard::new(bucket, l1))
            .unwrap_or_else(|| panic!("no checkpoint installed for the shard of {cache_key}"))
            .staged_fold_cut
            .get(instance)
            .copied()
            .unwrap_or_else(|| panic!("the shard of {cache_key} has no cut for {instance}"))
    }

    /// Every checkpoint file's raw bytes, keyed by path — the snapshot a retain must leave
    /// untouched.
    fn checkpoint_bytes(dir: &Path) -> BTreeMap<PathBuf, String> {
        let root = checkpoints_dir(dir);
        let mut out = BTreeMap::new();
        let Ok(buckets) = std::fs::read_dir(&root) else {
            return out;
        };
        for b in buckets.flatten() {
            if !b.path().is_dir() {
                continue;
            }
            for f in std::fs::read_dir(b.path()).unwrap().flatten() {
                let p = f.path();
                if p.file_name()
                    .is_some_and(|n| n.to_string_lossy().ends_with(".json"))
                {
                    out.insert(p.clone(), std::fs::read_to_string(&p).unwrap());
                }
            }
        }
        out
    }

    // =====================================================================
    // § 16.6.1 — exactly once across the cut
    // =====================================================================

    /// A credit is counted **exactly once**: by the fold if its seq is at or below the
    /// cut, by the pending term if it is above, never by both and never by neither.
    ///
    /// # How the straddle is constructed
    ///
    /// The straddle is not simulated; it is the real state, reached deterministically:
    ///
    /// 1. Publish A. Fold. A's seq is now at or below the cut, so A is in
    ///    `staged_byte_sum` and its pending record has been GC'd.
    /// 2. Publish B — `.meta` on disk, ledger entry appended, pending credit appended —
    ///    and **do not fold**. B's seq is strictly above the installed cut.
    /// 3. Read the gauges. B is in the pending term and NOT in `staged_byte_sum`, even
    ///    though B's `.meta` is sitting on disk fully staged.
    /// 4. Fold again. B moves into `staged_byte_sum` and out of the pending term.
    ///
    /// Step 3 is the state where the two normally-equal quantities diverge: "bytes whose
    /// `.meta` says staged" and "bytes the installed figure counted" are different
    /// numbers, and only the cut reconciles them. A totals-only assertion would pass
    /// against an implementation that got the decomposition wrong in compensating ways, so
    /// the **decomposition** is asserted at every step, not just the total.
    ///
    /// # Its own red side, in-test
    ///
    /// Two counterfactuals are computed from the same state and asserted to be *wrong*:
    ///
    /// - ignoring the cut and summing every pending record double-counts B once the fold
    ///   has taken it (`= total + B`), and
    /// - ignoring the pending term entirely misses B at step 3 (`= total - B`).
    ///
    /// Both are the implementations that pass a totals-only test at steps 1 and 4. If the
    /// cut filter were removed, `post_cut` would return B at step 4 and the first
    /// assertion fails; if the reader stopped evaluating pending, the step-3 assertion
    /// fails. A failure implicates `PendingReadOutcome::post_cut` and the cut-max loop in
    /// `run_staged_fold_with_cap`.
    #[tokio::test]
    async fn exactly_once_across_the_cut() {
        const A_BYTES: u64 = 4096;
        const B_BYTES: u64 = 8192;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let pending = PendingStore::new(dir.to_path_buf());

        // ---- Step 1: A published and folded.
        let seq_a = publish_staged_range(dir, &ledger, "bucket/a.bin", A_BYTES).await;
        let first = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(
            installed(&first).installed.len(),
            1,
            "one shard, one object"
        );
        let cut_a = cut_for(dir, "bucket/a.bin", "host:1");
        assert!(
            cut_a >= seq_a,
            "the cut must cover the credit the fold counted: cut={} seq_a={}",
            cut_a,
            seq_a
        );
        assert_eq!(
            folded_bytes(dir),
            A_BYTES,
            "A's .meta is staged and at or below the cut, so the fold counts it"
        );
        let g1 = gauges_for_tests(dir, 0).await;
        assert_eq!(g1.staged_fold_byte_sum, Some(A_BYTES));
        assert_eq!(
            g1.staged_fold_pending_bytes,
            Some(0),
            "A's pending record is at or below the cut, so it must not be counted a second time"
        );
        assert_eq!(g1.staged_fold_bytes, Some(A_BYTES), "counted exactly once");

        // ---- Step 2: B published, NOT folded. This is the straddle.
        let seq_b = publish_staged_range(dir, &ledger, "bucket/b.bin", B_BYTES).await;
        assert!(
            seq_b > cut_a,
            "the fixture must place B strictly above the installed cut, or there is no \
             straddle to test: seq_b={} cut={}",
            seq_b,
            cut_a
        );

        // ---- Step 3: B is counted by the pending term and by nothing else.
        let g2 = gauges_for_tests(dir, 0).await;
        assert_eq!(
            g2.staged_fold_byte_sum,
            Some(A_BYTES),
            "the installed figure has not been re-folded, so it still knows only A — even \
             though B's .meta is on disk and fully staged"
        );
        assert_eq!(
            g2.staged_fold_pending_bytes,
            Some(B_BYTES),
            "B's credit is above the cut, so the reader must add it"
        );
        assert_eq!(
            g2.staged_fold_bytes,
            Some(A_BYTES + B_BYTES),
            "exactly once: not zero times because the reader evaluated pending, not twice \
             because the fold has not counted B"
        );

        // The red side of step 3, in both directions, computed from the same on-disk
        // state.
        //
        // Step 3 is the ONLY moment at which both memberships coexist — B's `.meta` is
        // staged on disk and B's pending record is live — so it is the only moment at which
        // a double count is observable. An earlier version of this test computed the
        // counterfactual after step 4 instead, where the record has been GC'd; it read zero
        // and the test failed, which is how the misplacement was caught rather than shipped.
        //
        // (a) A `.meta` fold of everything, plus every pending record with no cut filter,
        //     counts B twice.
        let unfiltered_pending: u64 = pending
            .read_all()
            .await
            .credits
            .iter()
            .map(|(_, c)| c.bytes)
            .sum();
        assert_eq!(
            unfiltered_pending, B_BYTES,
            "A's record was GC'd when its cut passed, so the unfiltered sum is exactly B"
        );
        let meta_side = {
            let read = ledger.read_for_authority(0).await.unwrap();
            let mut sum = 0u64;
            for c in WriteLedger::group_by_key(read.entries) {
                sum += crate::write_ledger::fold_staged_candidate(dir, &c)
                    .await
                    .staged_bytes;
            }
            sum
        };
        assert_eq!(
            meta_side,
            A_BYTES + B_BYTES,
            "both objects' .meta files are staged on disk at this instant, which is what \
             makes the double count reachable"
        );
        assert_ne!(
            Some(meta_side + unfiltered_pending),
            g2.staged_fold_bytes,
            "an unfiltered implementation would report {}, counting B a second time",
            meta_side + unfiltered_pending
        );
        assert_eq!(
            meta_side + unfiltered_pending,
            A_BYTES + B_BYTES + B_BYTES,
            "and the excess is exactly B"
        );
        // (b) Not evaluating the pending term at all misses B entirely.
        assert_ne!(
            g2.staged_fold_byte_sum, g2.staged_fold_bytes,
            "if these were equal the pending term contributed nothing and B was counted \
             zero times"
        );

        // ---- Step 4: fold again. B crosses the cut; the total does not move.
        let second = run_staged_fold(dir, &ledger, "host:1").await;
        // A and B hash to different L1s, so this fixture is genuinely TWO shards — and with two
        // observed shards the derived per-pass budget is `ceil(2/12) = 1`, so this pass visits
        // exactly one. It must be **B's**, because B's shard has never been folded and therefore
        // sorts stalest. Naming the shard rather than counting shards is what makes the step's
        // real requirement explicit and what survives a change to the per-pass budget.
        let (b_bkt, b_l1) = shard_of("bucket/b.bin").unwrap();
        assert_eq!(
            installed(&second)
                .installed
                .iter()
                .map(|c| c.shard())
                .collect::<Vec<_>>(),
            vec![Shard::new(b_bkt, b_l1)],
            "the pass must visit B's shard — the one with no figure — so a short sum below \
             indicts the cut rather than scheduling"
        );
        let cut_b = cut_for(dir, "bucket/b.bin", "host:1");
        assert!(cut_b >= seq_b, "the cut must have advanced past B");
        assert!(cut_b >= cut_a, "the cut is monotone");
        assert_eq!(
            folded_bytes(dir),
            A_BYTES + B_BYTES,
            "B has crossed the cut, so the fold now counts it"
        );

        let g3 = gauges_for_tests(dir, 0).await;
        assert_eq!(
            g3.staged_fold_pending_bytes,
            Some(0),
            "B must leave the pending term in the same step it enters the fold, or it is \
             counted twice"
        );
        assert_eq!(
            g3.staged_fold_bytes,
            Some(A_BYTES + B_BYTES),
            "the total is unchanged across the handoff: the same bytes, counted once, by a \
             different term"
        );
        assert_eq!(
            g2.staged_fold_bytes, g3.staged_fold_bytes,
            "the handoff is invisible in the total, which is the whole point of the cut"
        );

        // Step 4's disjointness must not rest on the GC having run. Assert the FILTER
        // itself against the installed cut: had B's record survived, `post_cut` would still
        // exclude it. This reads `PendingReadOutcome::post_cut`, which is the function
        // `collect_staged_fold_gauges` branches on, rather than the total it derives.
        let (b_bucket, b_l1) = shard_of("bucket/b.bin").unwrap();
        let one_record = |bucket: &str, l1: &str| PendingReadOutcome {
            credits: vec![(
                "host:1".to_string(),
                PendingCredit {
                    seq: seq_b,
                    bucket: bucket.to_string(),
                    l1: l1.to_string(),
                    bytes: B_BYTES,
                    range: None,
                },
            )],
            files_expected: 1,
            files_read: 1,
            files_unreadable: 0,
            malformed_lines: 0,
        };
        let final_set = read_shard_checkpoints(dir);
        assert_eq!(
            one_record(&b_bucket, &b_l1).post_cut_bytes(&final_set),
            0,
            "a pending record the fold has already counted must be filtered by the cut, \
             whether or not the GC has reclaimed it — the GC is space reclamation, not the \
             correctness mechanism"
        );
        // The same record, same seq, same instance, tagged with a DIFFERENT shard is NOT
        // filtered. Under one global cut these two would be identical; they diverge only
        // because the cut is looked up per shard, so this is what shows the filter reads the
        // record's `(bucket, l1)` rather than a single number that happens to cover it.
        let elsewhere = shard_of("bucket/a.bin").unwrap().1;
        assert_ne!(elsewhere, b_l1, "the two fixture keys must hash apart");
        assert_eq!(
            one_record("other-bucket", &b_l1).post_cut_bytes(&final_set),
            B_BYTES,
            "a credit in a shard with no checkpoint is post-cut whatever its seq — no figure \
             has counted it, and filtering it out would be § 16.6.1's counted-by-neither"
        );
    }

    /// **A credit is counted exactly once even when the wall clock steps BACKWARDS.**
    ///
    /// This is the arm that would have caught a clock-derived coordinate, and it is the one
    /// that matters most, so it is a test of its own rather than a case inside the one
    /// above.
    ///
    /// # Construction
    ///
    /// Publish A at `now`, fold — so `cut` is at A's seq. Then publish B with a timestamp an
    /// hour **before** A's, which is what an NTP correction, a VM clock resync, or an
    /// operator setting the date looks like from inside the process. B is the newer credit
    /// in every sense that matters (it was appended later, its `.meta` was published later)
    /// and the older one by the clock.
    ///
    /// The fixture goes through `append_staged_entry`, which is `append_staged_range`'s own
    /// body, so the ledger entry and the pending record are stamped by production's pairing
    /// code. Only the timestamp differs from a normal publish.
    ///
    /// # What must hold
    ///
    /// B's bytes are counted exactly once, by the pending term, because its **seq** is above
    /// the cut regardless of its timestamp. The assertion reads
    /// `staged_fold_pending_bytes` — the figure `post_cut` produces — and also the input it
    /// branches on: B's `seq` against `cut`, and B's timestamp against A's, so the test
    /// states that the clock really did go backwards rather than assuming the fixture worked.
    ///
    /// # Red side, taken against the clock-derived implementation
    ///
    /// Reverting the coordinate to the clock — `WriteLedger::append` assigning
    /// `entry.timestamp.duration_since(UNIX_EPOCH).as_nanos()` instead of the counter —
    /// fails on the **input** assertion first, which is the stronger failure because it names
    /// the cause rather than a symptom. Verbatim:
    ///
    /// ```text
    /// the Ledger_Seq must be monotone across a backward clock step:
    /// seq_a=1788638616829364000 seq_b=1788635016829364000
    /// ```
    ///
    /// With that input assertion removed, the same revert fails on the figure, which is the
    /// consequence. Verbatim:
    ///
    /// ```text
    /// assertion `left == right` failed: a credit appended AFTER the cut was established
    /// must be counted by the pending term whatever the wall clock says.
    /// seq_b=1788635052451033000 cut=1788638652451033000
    ///   left: Some(0)
    ///  right: Some(8192)
    /// ```
    ///
    /// `left: Some(0)` is the counted-by-neither outcome § 16.6.1 rules out by name: the
    /// bytes are in no term at all until the next fold.
    ///
    /// A failure implicates `write_ledger::ledger_seq` and `WriteLedger::append`'s seq
    /// assignment.
    #[tokio::test]
    async fn credit_is_counted_once_when_the_clock_steps_backwards() {
        const A_BYTES: u64 = 4096;
        const B_BYTES: u64 = 8192;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        let a_at = SystemTime::now();
        let seq_a = publish_staged_range_at(dir, &ledger, "bucket/a.bin", A_BYTES, a_at).await;
        let first = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(installed(&first).installed.len(), 1);
        let cut = cut_for(dir, "bucket/a.bin", "host:1");
        assert_eq!(folded_bytes(dir), A_BYTES);

        // The backward step: an hour earlier than the credit already folded.
        let b_at = a_at - Duration::from_secs(3600);
        let seq_b = publish_staged_range_at(dir, &ledger, "bucket/b.bin", B_BYTES, b_at).await;

        // The fixture really did move the clock backwards, read off the ledger rather than
        // assumed. Without this the test could pass on a fixture that quietly went forwards.
        let entries = ledger.read_for_authority(0).await.unwrap().entries;
        let ts = |key: &str| {
            entries
                .iter()
                .find(|e| e.cache_key == key)
                .map(|e| e.timestamp)
                .unwrap()
        };
        assert!(
            ts("bucket/b.bin") < ts("bucket/a.bin"),
            "the fixture must actually step the clock backwards, or this test asserts nothing"
        );
        // And the coordinate went FORWARDS across that backward step. This is the property
        // under test, stated on the input rather than only on the figure derived from it.
        assert!(
            seq_b > seq_a,
            "the Ledger_Seq must be monotone across a backward clock step: seq_a={} seq_b={}",
            seq_a,
            seq_b
        );
        assert!(
            seq_b > cut,
            "and it must therefore land above the installed cut: seq_b={} cut={}",
            seq_b,
            cut
        );

        let g = gauges_for_tests(dir, 0).await;
        assert_eq!(
            g.staged_fold_pending_bytes,
            Some(B_BYTES),
            "a credit appended AFTER the cut was established must be counted by the pending \
             term whatever the wall clock says. seq_b={} cut={}",
            seq_b,
            cut
        );
        assert_eq!(
            g.staged_fold_byte_sum,
            Some(A_BYTES),
            "and not by the fold, which has not re-read B's .meta — so it is counted once, \
             not twice"
        );
        assert_eq!(
            g.staged_fold_bytes,
            Some(A_BYTES + B_BYTES),
            "counted exactly once: never zero times, which is what a clock-derived \
             coordinate produced here"
        );

        // And the handoff still works from a backward-stepped state: the next fold takes B
        // into F and out of pending, with the total unmoved.
        // Two shards, so the derived per-pass budget is 1 and B's — never folded, therefore
        // stalest — is the one this pass takes.
        let second = run_staged_fold(dir, &ledger, "host:1").await;
        let (b_bkt, b_l1) = shard_of("bucket/b.bin").unwrap();
        assert_eq!(
            installed(&second)
                .installed
                .iter()
                .map(|c| c.shard())
                .collect::<Vec<_>>(),
            vec![Shard::new(b_bkt, b_l1)]
        );
        assert!(
            cut_for(dir, "bucket/b.bin", "host:1") >= seq_b,
            "the cut advances past B despite B's earlier timestamp"
        );
        let g2 = gauges_for_tests(dir, 0).await;
        assert_eq!(g2.staged_fold_byte_sum, Some(A_BYTES + B_BYTES));
        assert_eq!(g2.staged_fold_pending_bytes, Some(0));
        assert_eq!(
            g2.staged_fold_bytes, g.staged_fold_bytes,
            "the total is unchanged across the handoff"
        );
    }

    /// Compaction retiring an instance's highest-seq entry must not lower the observed
    /// high-water mark, because a lowered cut resurrects an already-folded credit into the
    /// pending term — a double count.
    ///
    /// The assertion reads `max_seq_per_instance`, which is the value the cut is computed
    /// from, and asserts the file no longer contains the entry that produced it — so the mark
    /// can only be coming from the header.
    ///
    /// A failure implicates the header write in `WriteLedger::retire_identities` and
    /// `ParsedLedgerFile::max_seq_ever`.
    #[tokio::test]
    async fn compaction_cannot_lower_the_observed_high_water_mark() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        publish_staged_range(dir, &ledger, "bucket/keep.bin", 4096).await;
        let top = publish_staged_range(dir, &ledger, "bucket/top.bin", 8192).await;
        let before = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(before.max_seq_per_instance.get("host:1"), Some(&top));

        // Retire the highest-seq entry.
        let retire: std::collections::HashSet<_> = before
            .entries
            .iter()
            .filter(|e| e.cache_key == "bucket/top.bin")
            .map(|e| e.identity())
            .collect();
        assert_eq!(retire.len(), 1);
        ledger.retire_identities(&retire).await.unwrap();

        let after = ledger.read_for_authority(0).await.unwrap();
        assert!(
            !after
                .entries
                .iter()
                .any(|e| e.cache_key == "bucket/top.bin"),
            "the input: the entry that carried the mark is really gone from the file"
        );
        assert_eq!(
            after.max_seq_per_instance.get("host:1"),
            Some(&top),
            "the header must carry the pre-compaction max_seq_ever forward — otherwise the \
             next cut is lower than the last, and every credit between the two is filtered \
             out of the fold and back into the pending term, i.e. counted twice"
        );
        assert!(
            after.is_complete(),
            "and the header is not a malformed line"
        );
    }

    /// An instance that cannot read its own sequence mark refuses to append, rather than
    /// restarting its sequence from 1.
    ///
    /// A restarted sequence re-issues numbers at or below an installed cut, so every credit
    /// it stamps is filtered out of the pending term while the fold has not read its `.meta`
    /// — counted by neither, and persistently rather than transiently.
    ///
    /// **The mark is what this reads, and under the per-shard layout it is the only thing that
    /// could.** The base used to come from this instance's single ledger file; its numbers are
    /// now spread over up to `B x 256` files, so `seed_sequence` reads no ledger file at all and
    /// the mark is the whole record of what this identity has issued.
    ///
    /// The assertion reads the append's own `Result` (what the refusal branch returns), then the
    /// existing shard file's content, and then that the *target* shard file was never created —
    /// three statements of "nothing was written", the last of which the flat layout could not
    /// make.
    ///
    /// A failure implicates `WriteLedger::seed_sequence`'s non-`NotFound` error arm.
    #[tokio::test]
    #[cfg(unix)]
    async fn an_instance_that_cannot_read_its_own_sequence_mark_refuses_to_append() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // A first process seeds and writes, so the mark exists with a known value.
        let first = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        publish_staged_range(dir, &first, "bucket/a.bin", 4096).await;
        let path = first.shard_path_for_key("bucket/a.bin").unwrap();
        let mark = first.seq_mark_path();
        let content_before = std::fs::read_to_string(&path).unwrap();
        // A second, unseeded ledger handle for the same identity — the shape a restart takes.
        let restarted = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let target = restarted.shard_path_for_key("bucket/b.bin").unwrap();
        let restore = std::fs::metadata(&mark).unwrap().permissions();
        std::fs::set_permissions(&mark, std::fs::Permissions::from_mode(0o000)).unwrap();

        let refused = restarted
            .append_staged_range("bucket/b.bin", 0, 4095, 4096)
            .await;
        assert!(
            refused.is_err(),
            "an append that cannot read its own sequence mark must refuse, not guess a base"
        );

        std::fs::set_permissions(&mark, restore).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            content_before,
            "and it must have written nothing at all"
        );
        assert!(
            !target.exists(),
            "nor created the shard ledger file it would have appended to: {:?}",
            target
        );
        // Once readable, the same handle seeds correctly and continues the sequence rather
        // than restarting it. This is the positive control: without it the test would pass
        // against a ledger that always refused.
        let resumed = publish_staged_range(dir, &restarted, "bucket/c.bin", 4096).await;
        assert!(
            resumed > 1,
            "the resumed sequence must continue past the first process's, not restart: {}",
            resumed
        );
    }

    // =====================================================================
    // § 16.6.3 — Complete_Install: figure and cut retained together
    //
    // One test per class of incomplete input. Not one test with three arms: a shared arm
    // hides which class is unhandled, and the three classes are handled by three separate
    // branches in `run_staged_fold_with_cap`.
    // =====================================================================

    /// Install a figure, then damage the ledger, then assert the installed document is
    /// byte-identical afterwards.
    ///
    /// Returns the raw file bytes before and after, plus the parsed pair, so each caller
    /// can name figure and cut explicitly rather than resting on byte-identity alone.
    async fn fold_then_expect_retain(
        dir: &Path,
        ledger: &WriteLedger,
        cap: usize,
        expected: FoldRetainReason,
        before_raw: BTreeMap<PathBuf, String>,
        before: ShardCheckpointSet,
    ) {
        let outcome = run_staged_fold_with_cap(dir, ledger, "host:1", cap).await;
        assert_eq!(
            outcome.retain_reason(),
            Some(expected),
            "the pass must retain, naming this class of incomplete input"
        );
        assert!(
            outcome.summary().installed.is_empty(),
            "these classes are PASS-SCOPED inputs — the pending files, P, the checkpoint set, \
             and a legacy flat ledger that names no shard — so the pass must refuse before \
             visiting any shard, not install some shards from an input it knows is incomplete. \
             The four per-file LEDGER classes are no longer among them: each refuses one shard \
             and the pass continues, which is what `expect_shard_retain` asserts"
        );
        assert_eq!(
            before_raw,
            checkpoint_bytes(dir),
            "every installed document must be byte-identical: each holds one shard's figure \
             and its cut in one file, so byte-identity is what 'retained atomically together' \
             means, per shard"
        );
        let after = read_shard_checkpoints(dir);
        // Named individually as well, because a reader of this test should be able to see
        // that BOTH halves were checked without reasoning about the file format.
        assert_eq!(
            after.staged_byte_sum(),
            before.staged_byte_sum(),
            "the figure must be retained"
        );
        for (shard, b) in before.iter() {
            assert_eq!(
                after.get(shard).map(|a| &a.staged_fold_cut),
                Some(&b.staged_fold_cut),
                "shard {shard}'s cut must be retained, and retained with the same figure — \
                 installing one without the other is the failure this clause exists to prevent"
            );
        }
    }

    /// Seed a folded state, then add a second staged object so that a wrongly-permitted
    /// install would produce a *different* figure and a *different* cut.
    ///
    /// Without this second object a retain and an install could both leave the document
    /// unchanged, and the test would pass whatever the code did.
    async fn folded_then_divergent(
        dir: &Path,
        ledger: &WriteLedger,
    ) -> (BTreeMap<PathBuf, String>, ShardCheckpointSet) {
        publish_staged_range(dir, ledger, "bucket/first.bin", 4096).await;
        let first = run_staged_fold(dir, ledger, "host:1").await;
        assert_eq!(installed(&first).installed.len(), 1);
        let before = read_shard_checkpoints(dir);
        assert_eq!(before.staged_byte_sum(), 4096);
        let before_raw = checkpoint_bytes(dir);

        // The divergence: a second staged object with its own ledger entry and a strictly
        // later seq. An install after this point could not leave either field unchanged.
        publish_staged_range(dir, ledger, "bucket/second.bin", 65536).await;
        (before_raw, before)
    }

    // =====================================================================
    // § 16.6.12 point 2 — a pass reads only the shards it VISITS, and a ledger
    // incompleteness refuses ONE SHARD rather than the volume
    //
    // These four tests replaced the pass-scoped versions of the same four classes. The
    // old ones asserted `installed.is_empty()`, which was correct against a whole-volume
    // read and is the wrong contract now — and, more to the point, "the pass retained" and
    // "THIS shard retained" are different predicates. A test asserting only the first
    // passes on a tree where the shard-scoped refusal does not work at all, because a
    // whole-volume refusal satisfies it perfectly.
    // =====================================================================

    /// The smallest shard population that gives `shards_per_pass` a budget above one.
    ///
    /// `shards_per_pass` is `ceil(observed / passes_per_window)` with a floor of 1, so a
    /// population of `passes_per_window` or fewer gets a budget of **one** — and a one-visit
    /// budget cannot express the property these tests exist for, which is a retained shard and an
    /// installed sibling in the SAME pass. One more than `passes_per_window` is the first
    /// population that gets two visits. Derived from the shipped constants so it tracks them.
    const SHARDS_FOR_A_TWO_VISIT_BUDGET: usize =
        (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS) as usize + 1;

    /// Passes needed for a `SHARDS_FOR_A_TWO_VISIT_BUDGET`-shard fixture to give every shard a
    /// checkpoint, at two visits a pass.
    const PASSES_TO_CONVERGE: usize = SHARDS_FOR_A_TWO_VISIT_BUDGET.div_ceil(2);

    /// The `Shard` a fixture key belongs to.
    fn shard_for(cache_key: &str) -> Shard {
        let (bucket, l1) = shard_of(cache_key).expect("a fixture key must be shardable");
        Shard::new(bucket, l1)
    }

    /// Thirteen shards, one staged object each, every shard already carrying a checkpoint, the
    /// next pass's visit order **derived** rather than assumed, and the second shard in that order
    /// diverged so a re-install of it is observable.
    struct ThirteenShards {
        /// Shards in the order the next pass will visit them.
        order: Vec<Shard>,
        /// The one fixture key in each shard.
        key_of: BTreeMap<Shard, String>,
        /// Every checkpoint's raw bytes, after the divergence.
        before_raw: BTreeMap<PathBuf, String>,
        /// The parsed checkpoint set, after the divergence.
        before: ShardCheckpointSet,
    }

    impl ThirteenShards {
        /// The shard a test should damage: the FIRST the next pass visits.
        fn damaged(&self) -> &Shard {
            &self.order[0]
        }
        /// The shard that must install anyway: the SECOND the next pass visits.
        fn sibling(&self) -> &Shard {
            &self.order[1]
        }
        /// A shard the next pass provably does NOT reach: the last in visit order, against a
        /// budget of two.
        fn unvisited(&self) -> &Shard {
            self.order.last().unwrap()
        }
        fn key(&self, shard: &Shard) -> &str {
            &self.key_of[shard]
        }
    }

    /// Build [`ThirteenShards`].
    ///
    /// # Three fixture properties, all asserted or derived rather than assumed
    ///
    /// **Thirteen DISTINCT shards, by BUCKET.** `shard_of` returns `(bucket, l1)` and the bucket
    /// is not hashed into `l1`, so one object per bucket is thirteen distinct shards by
    /// construction rather than by a thirteen-way BLAKE3 coincidence.
    ///
    /// **Visit order is DERIVED from the same inputs the fold sorts on** — `staged_scanned_at`
    /// ascending, ties broken by `Shard`'s own `Ord` — rather than assumed to be `b00`, `b01`, …
    /// It is not: convergence installs two shards a pass, and the seventh pass revisits the
    /// stalest shard alongside the last fresh one, so the bucket order and the staleness order
    /// come apart. A first version of this fixture assumed `b00` was visited first, damaged it,
    /// and reported no retain at all — the damaged shard was simply never reached.
    ///
    /// **The divergence is BACKDATED**, and that is not cosmetic. `assess_staged_candidate` judges
    /// a `.meta` whose newest range `created_at` is strictly newer than the ledger entry's
    /// timestamp as `Superseded`, which contributes **zero** bytes. A divergence stamped `now`
    /// therefore moves the sibling's figure to 0 rather than to the intended value, and the test
    /// reads as a scoping failure when it is a fixture failure.
    async fn thirteen_shards_converged(
        dir: &Path,
        ledger: &WriteLedger,
        sibling_new_bytes: u64,
    ) -> ThirteenShards {
        let mut key_of: BTreeMap<Shard, String> = BTreeMap::new();
        for i in 0..SHARDS_FOR_A_TWO_VISIT_BUDGET {
            let key = format!("b{i:03}/only.bin");
            publish_staged_range(dir, ledger, &key, 4096).await;
            key_of.insert(shard_for(&key), key);
        }
        assert_eq!(
            key_of.len(),
            SHARDS_FOR_A_TWO_VISIT_BUDGET,
            "fixture precondition: one shard per bucket, all distinct"
        );
        assert!(
            shards_per_pass(key_of.len()) >= 2,
            "fixture precondition: the budget must allow TWO visits in one pass, or a retained \
             shard and an installed sibling cannot occur together and the test is vacuous"
        );
        for _ in 0..PASSES_TO_CONVERGE {
            run_staged_fold(dir, ledger, "host:1").await;
        }
        let installed = read_shard_checkpoints(dir);
        assert!(installed.is_complete());
        assert_eq!(
            installed.shards().len(),
            SHARDS_FOR_A_TWO_VISIT_BUDGET,
            "fixture precondition: every shard must already hold a figure, or 'retained its \
             PREVIOUS figure' has nothing to be true of"
        );

        let mut ranked: Vec<(u64, Shard)> = installed
            .iter()
            .map(|(shard, c)| (c.staged_scanned_at, shard.clone()))
            .collect();
        ranked.sort();
        let order: Vec<Shard> = ranked.into_iter().map(|(_, shard)| shard).collect();

        // The divergence, on the second shard in visit order only: rewrite its `.meta` with a
        // different staged size so a re-install of that shard produces a *different* figure.
        // Without it, an installed sibling and an untouched sibling look identical on disk and the
        // test would pass whatever the code did.
        seed_staged_meta(
            dir,
            &key_of[&order[1]],
            sibling_new_bytes,
            SystemTime::now() - Duration::from_secs(600),
        )
        .await;
        ThirteenShards {
            order,
            key_of,
            before_raw: checkpoint_bytes(dir),
            before: read_shard_checkpoints(dir),
        }
    }

    /// Assert the damaged shard retained BOTH fields while the sibling installed a NEW figure IN
    /// THE SAME PASS — the per-shard predicate, read back off disk.
    ///
    /// Every verdict here is an installed `StagedShardCheckpoint` read through
    /// `read_shard_checkpoints`, or the pass summary. No file count, no listing length, and no
    /// retain label stands in for the figure.
    async fn expect_shard_retain(
        dir: &Path,
        ledger: &WriteLedger,
        cap: usize,
        expected: FoldRetainReason,
        fixture: &ThirteenShards,
        sibling_new_bytes: u64,
    ) {
        let damaged = fixture.damaged().clone();
        let sibling = fixture.sibling().clone();
        let before_raw = &fixture.before_raw;
        let before = &fixture.before;
        let outcome = run_staged_fold_with_cap(dir, ledger, "host:1", cap).await;

        assert_eq!(
            outcome.retain_reason(),
            Some(expected),
            "the pass must still report the retain, naming this class, so the cadence is not \
             consumed and `staged_fold_incomplete_total` still moves"
        );
        assert_eq!(
            outcome.summary().retained,
            vec![(damaged.clone(), expected)],
            "and EXACTLY ONE shard must have retained, the damaged one — a whole-pass refusal \
             satisfies `retain_reason()` above just as well, which is why this line exists"
        );
        assert!(
            outcome
                .summary()
                .installed
                .iter()
                .any(|c| c.shard() == sibling),
            "the sibling must have installed IN THE SAME PASS: one shard's unreadable ledger \
             file confines itself to that shard. Installed: {:?}",
            outcome
                .summary()
                .installed
                .iter()
                .map(|c| c.shard())
                .collect::<Vec<_>>()
        );

        let after = read_shard_checkpoints(dir);
        let after_raw = checkpoint_bytes(dir);
        let damaged_path = StagedShardCheckpoint::path(dir, &damaged);
        assert_eq!(
            after_raw.get(&damaged_path),
            before_raw.get(&damaged_path),
            "the damaged shard's document must be byte-identical: figure and cut are fields of \
             one file, so byte-identity is what 'retained together' means"
        );
        assert_eq!(
            after.get(&damaged).map(|c| c.staged_byte_sum),
            before.get(&damaged).map(|c| c.staged_byte_sum),
            "the damaged shard's figure must be its PREVIOUS one"
        );
        assert_eq!(
            after.get(&damaged).map(|c| &c.staged_fold_cut),
            before.get(&damaged).map(|c| &c.staged_fold_cut),
            "and its cut must be its PREVIOUS one — installing one without the other is the \
             failure the single-document install exists to prevent"
        );

        assert_eq!(
            after.get(&sibling).map(|c| c.staged_byte_sum),
            Some(sibling_new_bytes),
            "and the sibling's figure must be the NEW one, so 'the sibling installed' is a \
             statement about its document rather than about the pass summary alone"
        );
        let sibling_path = StagedShardCheckpoint::path(dir, &sibling);
        assert_ne!(
            after_raw.get(&sibling_path),
            before_raw.get(&sibling_path),
            "the sibling's document must have actually changed on disk"
        );
    }

    /// Class 1: a `{bucket}/{l1}/{instance}.ledger` that cannot be opened, at SHARD scope.
    ///
    /// A failure implicates the `files_unreadable` arm of `shard_ledger_incompleteness`, or the
    /// `continue` that keeps the pass going past a shard-scoped retain.
    #[tokio::test]
    #[cfg(unix)]
    async fn one_shard_s_unreadable_ledger_file_retains_that_shard_alone() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        const SIBLING_NEW_BYTES: u64 = 65536;
        let fixture = thirteen_shards_converged(dir, &ledger, SIBLING_NEW_BYTES).await;

        let path = ledger
            .shard_path_for_key(fixture.key(fixture.damaged()))
            .unwrap();
        let restore = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        expect_shard_retain(
            dir,
            &ledger,
            0,
            FoldRetainReason::LedgerFileUnreadable,
            &fixture,
            SIBLING_NEW_BYTES,
        )
        .await;

        std::fs::set_permissions(&path, restore).unwrap();
    }

    /// Class 2: a line the parser discarded, at SHARD scope.
    ///
    /// Note the good lines still parse, so the fold *could* compute a plausible figure for this
    /// shard — which is exactly why the branch has to exist: a shorter ledger and a complete one
    /// produce figures that are both believable.
    #[tokio::test]
    async fn one_shard_s_malformed_ledger_line_retains_that_shard_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        const SIBLING_NEW_BYTES: u64 = 65536;
        let fixture = thirteen_shards_converged(dir, &ledger, SIBLING_NEW_BYTES).await;

        let damaged = fixture.damaged().clone();
        let path = ledger.shard_path_for_key(fixture.key(&damaged)).unwrap();
        let mut content = std::fs::read_to_string(&path).unwrap();
        content.push_str("{ this is not a ledger entry\n");
        std::fs::write(&path, content).unwrap();
        // The parser really does discard it, and the discard is confined to THIS shard's read —
        // asserted so a fixture that produced a readable line, or a reader that saw the whole
        // volume, could not pass this test vacuously.
        let damaged_read = ledger
            .read_shard_for_authority(&damaged.bucket, &damaged.l1, 0)
            .await
            .unwrap();
        assert_eq!(damaged_read.malformed_lines, 1);
        assert_eq!(damaged_read.entries.len(), 1, "the good entry still parses");
        let sibling = fixture.sibling();
        assert!(
            ledger
                .read_shard_for_authority(&sibling.bucket, &sibling.l1, 0)
                .await
                .unwrap()
                .is_complete(),
            "and the sibling shard's read is unaffected by it"
        );

        expect_shard_retain(
            dir,
            &ledger,
            0,
            FoldRetainReason::LedgerMalformedLine,
            &fixture,
            SIBLING_NEW_BYTES,
        )
        .await;
    }

    /// Class 3: the read hit its cap, so it saw a prefix — at SHARD scope, where the cap applies
    /// per shard.
    ///
    /// Reached through `run_staged_fold_with_cap` because production passes `cap = 0` and
    /// truncation is therefore structurally unreachable there — an unreachable branch asserted by
    /// nothing looks exactly like a branch that works.
    ///
    /// **The fixture needs a SECOND entry in the damaged shard**, and that is a consequence of
    /// the scoping rather than a detail: with one entry per shard, a cap of 1 truncates nothing
    /// anywhere, because `entries.len() > cap` is false for every shard. The second entry is a
    /// second range of the same object, which is what a multipart write produces, and it does not
    /// move the figure — `group_by_key` collapses both into one candidate and the bytes come from
    /// the `.meta`.
    #[tokio::test]
    async fn one_shard_s_truncated_ledger_read_retains_that_shard_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        const SIBLING_NEW_BYTES: u64 = 65536;
        let fixture = thirteen_shards_converged(dir, &ledger, SIBLING_NEW_BYTES).await;
        let damaged = fixture.damaged().clone();

        ledger
            .append_staged_entry(WriteLedgerEntry {
                timestamp: SystemTime::now(),
                cache_key: fixture.key(&damaged).to_string(),
                range_start: 4096,
                range_end: 8191,
                compressed_size: 4096,
                instance_id: ledger.instance_id().to_string(),
                seq: None,
            })
            .await
            .unwrap();

        let capped = ledger
            .read_shard_for_authority(&damaged.bucket, &damaged.l1, 1)
            .await
            .unwrap();
        assert!(
            capped.truncated,
            "the cap must actually truncate THIS shard"
        );
        assert_eq!(capped.files_unreadable, 0, "and nothing else must be wrong");
        assert_eq!(capped.malformed_lines, 0);
        let sibling = fixture.sibling();
        assert!(
            !ledger
                .read_shard_for_authority(&sibling.bucket, &sibling.l1, 1)
                .await
                .unwrap()
                .truncated,
            "and the sibling shard, with one entry, is not truncated by the same cap — a cap that \
             truncated everything would make this test's per-shard claim vacuous"
        );

        expect_shard_retain(
            dir,
            &ledger,
            1,
            FoldRetainReason::LedgerTruncated,
            &fixture,
            SIBLING_NEW_BYTES,
        )
        .await;
    }

    /// Class 4 of the shard-scoped set: one shard's own directory cannot be LISTED.
    ///
    /// Distinct from an unreadable file: which files that shard has is unknown, so there is no
    /// file set to report a count over. `LedgerListFailed` therefore has two scopes, and this is
    /// the shard-scoped one — a failure implicates the `Err` arm of the
    /// `read_shard_for_authority` call inside the visit loop.
    #[tokio::test]
    #[cfg(unix)]
    async fn one_shard_s_unlistable_ledger_directory_retains_that_shard_alone() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        const SIBLING_NEW_BYTES: u64 = 65536;
        let fixture = thirteen_shards_converged(dir, &ledger, SIBLING_NEW_BYTES).await;

        let shard_dir = ledger
            .shard_path_for_key(fixture.key(fixture.damaged()))
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let restore = std::fs::metadata(&shard_dir).unwrap().permissions();
        std::fs::set_permissions(&shard_dir, std::fs::Permissions::from_mode(0o000)).unwrap();

        expect_shard_retain(
            dir,
            &ledger,
            0,
            FoldRetainReason::LedgerListFailed,
            &fixture,
            SIBLING_NEW_BYTES,
        )
        .await;

        std::fs::set_permissions(&shard_dir, restore).unwrap();
    }

    /// **A pass reads only the shards it VISITS.** An unreadable ledger file in a shard the pass
    /// does not select costs nothing at all.
    ///
    /// This is the headline property of § 16.6.12 point 2 stated directly, and the only test here
    /// whose red side is the whole-volume read itself rather than a missing gate: under a
    /// whole-volume authority read this pass retains `LedgerFileUnreadable` and installs nothing,
    /// because the damaged file is in the read whether or not any shard needs it.
    ///
    /// The damaged shard is the LAST in visit order and the budget is two, so it is provably
    /// outside this pass's selection — asserted through the pass summary rather than assumed.
    #[tokio::test]
    #[cfg(unix)]
    async fn an_unreadable_ledger_file_in_an_unvisited_shard_costs_the_pass_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        const SIBLING_NEW_BYTES: u64 = 65536;
        let fixture = thirteen_shards_converged(dir, &ledger, SIBLING_NEW_BYTES).await;
        let untouched = fixture.unvisited().clone();

        let path = ledger.shard_path_for_key(fixture.key(&untouched)).unwrap();
        let restore = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        std::fs::set_permissions(&path, restore).unwrap();

        assert_eq!(
            outcome.retain_reason(),
            None,
            "the pass must INSTALL: the damaged file is in a shard this pass never selected, so \
             nothing it installed depends on reading it"
        );
        assert!(
            outcome.summary().retained.is_empty(),
            "and no shard retained"
        );
        let visited: Vec<Shard> = outcome
            .summary()
            .installed
            .iter()
            .map(|c| c.shard())
            .collect();
        assert!(
            !visited.contains(&untouched),
            "fixture precondition: the damaged shard must be OUTSIDE this pass's selection, or \
             the test is asserting nothing about scoping. Visited: {visited:?}"
        );
        assert_eq!(
            read_shard_checkpoints(dir)
                .get(fixture.sibling())
                .map(|c| c.staged_byte_sum),
            Some(SIBLING_NEW_BYTES),
            "and the shards it did visit installed their new figures"
        );
        assert_eq!(
            read_shard_checkpoints(dir)
                .get(&untouched)
                .map(|c| c.staged_byte_sum),
            fixture.before.get(&untouched).map(|c| c.staged_byte_sum),
            "while the unvisited shard keeps its own previous figure, untouched"
        );
    }

    /// A drained shard's header-only ledger file still puts its `max_seq_ever` into **that
    /// shard's** cut.
    ///
    /// # Why this needs re-taking at shard scope rather than inheriting the whole-volume version
    ///
    /// `SC-LEDGER-1` asserted this against `read_for_authority`, and its fixture had to be
    /// **ordered** to be reachable: the drained shard needed the instance's highest seq, or the
    /// surviving shard's mark was the same value and dropping the drained file's changed nothing.
    ///
    /// At shard scope that redundancy cannot arise at all — a shard's cut is computed from a read
    /// of *its own directory*, so no sibling's file can substitute for the drained file's mark,
    /// whatever the ordering. The property is therefore strictly more load-bearing here, and it
    /// is reachable unconditionally rather than by construction. The ordering is kept anyway so
    /// the fixture stays meaningful if the read scope is ever widened again.
    ///
    /// The drained shard is retired **before its first fold**, so a dropped mark leaves its cut
    /// with no entry for the instance at all rather than silently inheriting a previous one.
    #[tokio::test]
    async fn a_drained_shard_s_header_only_file_still_sets_that_shard_s_cut() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        publish_staged_range(dir, &ledger, "bucket/survivor.bin", 512).await;
        let drained_seq = publish_staged_range(dir, &ledger, "bucket/drain-me.bin", 4096).await;
        let drained = shard_for("bucket/drain-me.bin");
        assert_ne!(
            drained,
            shard_for("bucket/survivor.bin"),
            "fixture precondition: two distinct shards"
        );

        let before = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(
            before.max_seq_per_instance.get("host:1"),
            Some(&drained_seq),
            "fixture precondition: the DRAINED shard must hold the instance's highest seq"
        );
        let retire: std::collections::HashSet<_> = before
            .entries
            .iter()
            .filter(|e| e.cache_key == "bucket/drain-me.bin")
            .map(|e| e.identity())
            .collect();
        ledger.retire_identities(&retire).await.unwrap();
        let drained_path = ledger.shard_path_for_key("bucket/drain-me.bin").unwrap();
        assert!(
            drained_path.is_file(),
            "the drained file must SURVIVE, header-only, or its mark is gone"
        );

        // Two shards give a budget of one, so both need their own pass.
        for _ in 0..2 {
            run_staged_fold(dir, &ledger, "host:1").await;
        }
        let set = read_shard_checkpoints(dir);
        assert!(set.is_complete());
        let drained_doc = set
            .get(&drained)
            .expect("the drained shard must have been folded");
        assert_eq!(
            drained_doc.staged_byte_sum, 0,
            "keeping the file is not keeping the bytes"
        );
        assert_eq!(
            drained_doc.staged_fold_cut.get("host:1"),
            Some(&drained_seq),
            "and the header-only file's mark must reach THIS shard's cut. Dropping it leaves the \
             cut below a credit this figure has already accounted for, so the pending term \
             re-adds it — a double count, and at shard scope no sibling's mark can cover for it"
        );
    }

    /// The no-regression side: the shard-scoped reads, summed, reproduce exactly what a
    /// whole-volume read's fold produces — across two instances and three shards, one of which
    /// carries both instances' ledger files.
    ///
    /// The expected figures are **derived from `read_for_authority`**, the whole-volume reader
    /// that still exists, folded through the same `fold_staged_candidate` the pass uses. So this
    /// compares the two readers against each other rather than comparing either against a number
    /// written into the fixture.
    #[tokio::test]
    async fn shard_scoped_reads_sum_to_what_a_whole_volume_read_folds() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let a = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let b = WriteLedger::new(dir.to_path_buf(), "host:2".to_string());

        // One object credited by BOTH instances — a multipart write split across two proxies —
        // which is the minimum that makes a shard's cut a map rather than a single entry.
        publish_staged_range(dir, &a, "bucket/shared.bin", 8192).await;
        publish_staged_range(dir, &b, "bucket/shared.bin", 8192).await;
        publish_staged_range(dir, &a, "bucket/only-a.bin", 4096).await;
        publish_staged_range(dir, &b, "bucket/only-b.bin", 512).await;

        let shared = shard_for("bucket/shared.bin");
        let shards: BTreeSet<Shard> = [
            "bucket/shared.bin",
            "bucket/only-a.bin",
            "bucket/only-b.bin",
        ]
        .iter()
        .map(|k| shard_for(k))
        .collect();
        assert_eq!(
            shards.len(),
            3,
            "fixture precondition: three distinct shards"
        );
        for instance in ["host:1", "host:2"] {
            assert!(
                WriteLedger::shard_ledger_path(dir, &shared.bucket, &shared.l1, instance).is_file(),
                "fixture precondition: the shared shard must carry {instance}'s ledger file, or \
                 a reader that saw only one file per shard would pass this test"
            );
        }

        // What a WHOLE-VOLUME read folds to, computed independently of the pass.
        let whole = a.read_for_authority(0).await.unwrap();
        assert!(whole.is_complete(), "the fixture's ledger must be readable");
        let mut expected_bytes = 0u64;
        let mut expected_ranges = 0u64;
        for candidate in WriteLedger::group_by_key(whole.entries.clone()) {
            let fold = crate::write_ledger::fold_staged_candidate(dir, &candidate).await;
            if fold.verdict == StagedCandidateVerdict::Evictable {
                expected_bytes = expected_bytes.saturating_add(fold.staged_bytes);
                expected_ranges = expected_ranges.saturating_add(fold.staged_range_count);
            }
        }
        assert!(
            expected_bytes > 0,
            "fixture precondition: the whole-volume fold must be non-zero, or the comparison \
             below holds for a fold that counted nothing"
        );

        // Three shards give a budget of one, so full selection takes three passes. A single pass
        // visiting every shard is not reachable at this population under the shipped budget, and
        // the sum is the mixed-age sum § 3.6 specifies either way.
        for _ in 0..3 {
            run_staged_fold(dir, &a, "host:1").await;
        }
        let set = read_shard_checkpoints(dir);
        assert!(set.is_complete());
        assert_eq!(
            set.shards().len(),
            3,
            "every shard must have been visited before the totals are comparable"
        );
        assert_eq!(
            set.staged_byte_sum(),
            expected_bytes,
            "the per-shard figures must sum to the whole-volume fold — a scoped read that missed \
             a file, or a shard, shows up here as a short total"
        );
        assert_eq!(
            set.staged_range_count(),
            expected_ranges,
            "and so must the range counts"
        );
        assert_eq!(
            set.get(&shared)
                .unwrap()
                .staged_fold_cut
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["host:1".to_string(), "host:2".to_string()],
            "and the shared shard's cut must name BOTH instances: a reader that took only the \
             first file in a shard directory would still produce the right bytes here, because \
             both entries name one .meta, and would silently lose an instance's cut"
        );
    }

    /// The positive control for the retain tests above: with nothing damaged, the pass **does**
    /// install, the figure moves, and the cut moves **in the shard whose credit moved it**.
    ///
    /// Without this, every retain test would pass against a fold that could never install
    /// anything at all.
    ///
    /// # The cut assertion changed with the read's scope, and its new form is the stronger one
    ///
    /// It used to assert that `bucket/first.bin`'s shard cut ROSE when `bucket/second.bin` was
    /// credited — true only because the cut was the instance's GLOBAL maximum, and a statement
    /// about a shard whose staged set had not changed at all. Under a shard-scoped read a shard's
    /// cut is its own: `second.bin`'s shard advances to `second.bin`'s seq, and `first.bin`'s
    /// stays exactly where its own last credit left it. Both halves are asserted, and the second
    /// is what would catch a reintroduced global maximum.
    #[tokio::test]
    async fn a_complete_input_installs_both_the_figure_and_the_cut() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let (_, before) = folded_then_divergent(dir, &ledger).await;

        // Two shards and a derived per-pass budget of `ceil(2/12) = 1`, so the *population*
        // converges over two passes rather than one. Both halves of this test's claim — the
        // figure AND the first shard's cut — need every shard revisited, so it folds to
        // convergence rather than assuming one pass suffices.
        let passes = fold_to_convergence(dir, &ledger, "host:1", 4).await;
        assert!(
            passes >= 2,
            "two shards at one per pass need two passes: {passes}"
        );
        assert_eq!(
            folded_bytes(dir),
            4096 + 65536,
            "the figure moves to include the second object"
        );
        let first_shard = shard_for("bucket/first.bin");
        let second_shard = shard_for("bucket/second.bin");
        assert_ne!(
            first_shard, second_shard,
            "fixture precondition: the two objects must be in different shards, or the two cut \
             claims below are the same claim"
        );
        let after = read_shard_checkpoints(dir);
        // The cut moved with the figure, in the same document — in the shard whose credit moved
        // it, and at exactly that credit's coordinate rather than merely upward.
        let second_seq = *ledger
            .read_shard_for_authority(&second_shard.bucket, &second_shard.l1, 0)
            .await
            .unwrap()
            .max_seq_per_instance
            .get("host:1")
            .expect("the second object's shard must have a mark");
        assert_eq!(
            after
                .get(&second_shard)
                .unwrap()
                .staged_fold_cut
                .get("host:1"),
            Some(&second_seq),
            "and the cut moves with it, in the same document"
        );
        assert_eq!(
            after
                .get(&first_shard)
                .unwrap()
                .staged_fold_cut
                .get("host:1"),
            before
                .get(&first_shard)
                .unwrap()
                .staged_fold_cut
                .get("host:1"),
            "while the FIRST shard's cut does not move: nothing was credited to it, so there is \
             nothing for its cut to advance past. A cut taken from the instance's global maximum \
             would raise it here, above the highest seq this shard has ever recorded — which is \
             where a pending credit for this shard could be filtered as already-folded while no \
             fold had read its .meta"
        );
    }

    /// An unreadable `.pending` file is an incomplete input too: the post-cut term is
    /// unknown, so the figure cannot be installed against a cut that would filter records
    /// nobody could read.
    ///
    /// A failure implicates the `!pending_read.is_complete()` branch.
    #[tokio::test]
    #[cfg(unix)]
    async fn complete_install_retains_on_an_unreadable_pending_file() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let (before_raw, before) = folded_then_divergent(dir, &ledger).await;

        let path = PendingStore::new(dir.to_path_buf())
            .dir()
            .join("host:1.pending");
        let restore = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        fold_then_expect_retain(
            dir,
            &ledger,
            0,
            FoldRetainReason::PendingUnreadable,
            before_raw,
            before,
        )
        .await;

        std::fs::set_permissions(&path, restore).unwrap();
    }

    // =====================================================================
    // § 16.6.4 / § 16.6.9 — the age signal
    // =====================================================================

    /// An idle fleet does not raise the age signal, and a stalled fold does — at the SAME
    /// age.
    ///
    /// # Why the two arms share a backdated age
    ///
    /// Age alone cannot tell the two states apart, which is the whole reason a second
    /// gauge exists. So both arms are observed at the same, deliberately large,
    /// `staged_authority_age_secs`: the only thing that differs between them is whether a
    /// post-cut credit exists. A single-gauge implementation produces identical output for
    /// both and fails the second assertion pair.
    ///
    /// The discriminator asserted is `staged_mutations_awaiting_fold`, and its **input** is
    /// asserted too — `PendingReadOutcome::post_cut` against the installed cut — because
    /// the gauge is derived from it and this repo has six recorded cases of an assertion
    /// that measured an adjacent quantity for months.
    ///
    /// A failure implicates `collect_staged_fold_gauges`' `awaiting = post_cut.len()` and
    /// `PendingReadOutcome::post_cut`.
    #[tokio::test]
    async fn an_idle_fleet_does_not_raise_the_age_signal() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        publish_staged_range(dir, &ledger, "bucket/idle.bin", 4096).await;
        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(installed(&outcome).installed.len(), 1);

        // Backdate the install so the age is unambiguously large. Written through
        // `install`, so the document stays a single atomic unit.
        let mut aged = installed(&outcome).installed[0].clone();
        aged.staged_scanned_at = now_nanos().saturating_sub(7_200 * 1_000_000_000);
        aged.install(dir).unwrap();
        let cut = read_shard_checkpoints(dir);

        // Re-append the credit the fold's GC reclaimed, so arm 1 exercises the cut FILTER
        // rather than an empty file. Without this, arm 1 reports zero awaiting mutations
        // whether the filter works or not — the GC having emptied the file is not evidence
        // that a dead record would be excluded.
        //
        // It must be tagged with the **real** shard of the folded key, because the cut is
        // looked up per shard: a record tagged with any other shard is post-cut by design
        // and would report arm 1 as stalled.
        let (idle_bucket, idle_l1) = shard_of("bucket/idle.bin").unwrap();
        let own_cut = cut_for(dir, "bucket/idle.bin", "host:1");
        PendingStore::new(dir.to_path_buf())
            .append(
                "host:1",
                &PendingCredit {
                    seq: own_cut,
                    bucket: idle_bucket,
                    l1: idle_l1,
                    bytes: 4096,
                    range: None,
                },
            )
            .await
            .unwrap();

        // ---- Arm 1: idle. Every credit is at or below its cut.
        let pending = PendingStore::new(dir.to_path_buf()).read_all().await;
        assert_eq!(
            pending.credits.len(),
            1,
            "the input: a record exists, and it is dead rather than absent"
        );
        assert!(
            pending.post_cut(&cut).is_empty(),
            "the input: no pending record sits above the cut, which is § 16.6.4's derivable \
             idle state"
        );
        let idle = gauges_for_tests(dir, 0).await;
        let idle_age = idle.staged_authority_age_secs.unwrap();
        assert!(
            idle_age >= 7_000,
            "the age must be large, or this test proves nothing: {}",
            idle_age
        );
        assert_eq!(
            idle.staged_mutations_awaiting_fold,
            Some(0),
            "an idle fleet must report zero mutations awaiting a fold, so a large age is \
             readable as healthy"
        );

        // ---- Arm 2: a mutation lands and is NOT folded. Same age, different verdict.
        publish_staged_range(dir, &ledger, "bucket/stalled.bin", 8192).await;
        let pending = PendingStore::new(dir.to_path_buf()).read_all().await;
        assert_eq!(
            pending.credits.len(),
            2,
            "the input: two records exist, one dead and one live"
        );
        assert_eq!(
            pending.post_cut(&cut).len(),
            1,
            "the input: exactly one of them sits above the cut"
        );
        let stalled = gauges_for_tests(dir, 0).await;
        assert!(
            stalled.staged_authority_age_secs.unwrap() >= 7_000,
            "the age is still large — unchanged, because publishing a range does not fold"
        );
        assert_eq!(
            stalled.staged_mutations_awaiting_fold,
            Some(1),
            "a stalled fold must be distinguishable from an idle fleet at the same age"
        );
        assert_ne!(
            idle.staged_mutations_awaiting_fold, stalled.staged_mutations_awaiting_fold,
            "the two states differ ONLY in this gauge; if it did not move, the age signal \
             would false-alarm on an idle fleet"
        );
    }

    /// The gap's sign convention, pinned in both directions.
    ///
    /// `staged_fold_gap_bytes = staged_fold_bytes − incumbent`. Positive means the fold
    /// sees more; negative means the incumbent over-states, which is the direction the
    /// incumbent's measured drift predicts. A failure implicates the subtraction in
    /// `collect_staged_fold_gauges`.
    #[tokio::test]
    async fn the_gap_is_the_fold_minus_the_incumbent() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        publish_staged_range(dir, &ledger, "bucket/gap.bin", 4096).await;
        run_staged_fold(dir, &ledger, "host:1").await;

        let g = gauges_for_tests(dir, 1024).await;
        assert_eq!(g.staged_fold_bytes, Some(4096));
        assert_eq!(
            g.staged_fold_gap_bytes,
            Some(3072),
            "positive: the fold sees MORE than the incumbent"
        );

        // 77_777 rather than the 100_000 this used to carry. That figure is an INCUMBENT byte
        // count and had nothing to do with `STAGED_RANGE_BUDGET`, but it was numerically equal
        // to it, and a coincidence like that is what makes a later assertion vacuous when
        // someone reuses the line. `the_budget_denominator_is_data_and_never_a_literal` also
        // reads a bare literal here as a budget expectation, correctly, because it cannot tell
        // the two apart either.
        let g = gauges_for_tests(dir, 77_777).await;
        assert_eq!(
            g.staged_fold_gap_bytes,
            Some(4096 - 77_777),
            "negative: the incumbent OVER-states relative to the fold, which is the \
             expected sign on a fleet carrying the incumbent's measured drift"
        );
    }

    /// `P` is a term of the fold's published total and a gauge of its own, so a change in
    /// it must move `staged_fold_bytes` by exactly that amount and move
    /// `staged_byte_sum` by nothing.
    #[tokio::test]
    async fn in_progress_part_bytes_enters_the_total_and_not_the_fold_figure() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        publish_staged_range(dir, &ledger, "bucket/p.bin", 4096).await;
        run_staged_fold(dir, &ledger, "host:1").await;
        let before = gauges_for_tests(dir, 0).await;
        assert_eq!(before.staged_fold_in_progress_part_bytes, Some(0));

        // One in-progress part record of 700 bytes.
        let upload = dir.join("mpus_in_progress").join("upload-1");
        std::fs::create_dir_all(&upload).unwrap();
        let part = crate::cache_types::CachedPartInfo {
            part_number: 1,
            size: 700,
            compressed_size: 700,
            etag: "\"e\"".to_string(),
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
            file_identity: None,
        };
        std::fs::write(
            upload.join("part1.json"),
            serde_json::to_string(&part).unwrap(),
        )
        .unwrap();

        let after = gauges_for_tests(dir, 0).await;
        assert_eq!(after.staged_fold_in_progress_part_bytes, Some(700));
        assert_eq!(
            after.staged_fold_byte_sum, before.staged_fold_byte_sum,
            "P is not part of F: the .meta fold is unaffected by a part that has not landed"
        );
        assert_eq!(
            after.staged_fold_bytes,
            Some(before.staged_fold_bytes.unwrap() + 700),
            "and it moves the published total by exactly its own bytes"
        );
    }

    // =====================================================================
    // The hard constraint: no decision path reads the fold
    // =====================================================================

    /// **The fold IS the staged authority, and it reaches decisions through exactly one
    /// symbol.**
    ///
    /// # This test replaces `no_decision_path_reads_the_fold`, and the replacement is the
    /// point
    ///
    /// I2 built that test to assert the *opposite*: that nothing outside this module and
    /// `metrics.rs` named any fold figure, so the behaviour flip could not happen by
    /// accident. That separation existed to be ended, and this unit ends it. Deleting the
    /// test would have left the flip unguarded in both directions at once — nothing
    /// asserting the fold is wired in, and nothing bounding how widely it is read — so it is
    /// inverted rather than removed, and it now carries three claims where the old one
    /// carried two.
    ///
    /// **1. The gauge surface is UNCHANGED and still confined.** Fifteen observability
    /// fields have no business in a decision, and `StagedFoldGauges` remains readable only
    /// by its producer and its publisher. This half of the old test is kept verbatim.
    ///
    /// **2. The authority surface is exactly one symbol in exactly one file.** The flip is
    /// [`staged_authority_figure`], read only by `cache.rs`
    /// (`CacheManager::staged_authority`), which is the single place the three composites and
    /// the staging trigger both derive from. This is the `budget_verdict` containment
    /// pattern, applied to the byte figure now that it is a decision input.
    ///
    /// **3. The flip is PRESENT, positively.** `cache.rs` must name
    /// `staged_authority_figure`.
    ///
    /// **It is redundant as written, and the previous version of this comment was wrong about
    /// why it is here.** It claimed to be "the arm that catches a revert". It cannot be: claim 2
    /// is an `assert_eq!` against the exact vector `vec![AUTHORITY_CONSUMER]`, and a reverted
    /// tree has **zero** callers, so claim 2's own assertion fails first and claim 3 is never
    /// reached. Claim 3 therefore has no state in which it fails distinctly, and its failure
    /// message describes a situation claim 2 has already reported.
    ///
    /// It is kept rather than deleted because the redundancy is **conditional on claim 2's exact
    /// form**. If claim 2 were ever loosened to "at most one caller" — a plausible edit, since
    /// exact-vector equality is the brittle-looking half — the empty case would start passing and
    /// claim 3 would silently become the only thing standing between this test and a tree in
    /// which the fold is authoritative for nothing. Anyone loosening claim 2 must read claim 3
    /// as load-bearing from that moment on.
    ///
    /// A reviewer confirming this by hand should read `CacheManager::staged_authority` and
    /// its two callers, `get_cache_size_stats` and `get_staging_resident_bytes`.
    #[test]
    fn the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface() {
        // The GAUGE names. Still confined, still for the same reason.
        const FIGURE_NAMES: [&str; 7] = [
            "StagedFoldGauges",
            "collect_staged_fold_gauges",
            "StagedShardCheckpoint",
            "ShardCheckpointSet",
            "read_shard_checkpoints",
            "staged_fold_bytes",
            "staged_byte_sum",
        ];
        // The only files allowed to name them: the producers and the publisher.
        //
        // **`st_size_fold.rs` was ADDED here when § 3.7's fold landed, and the widening is
        // narrower than it looks.** The rule this list enforces is "a gauge figure must not
        // reach a decision path". `st_size_fold.rs` is the checkpoint document's SECOND
        // PRODUCER — it names `StagedShardCheckpoint` because it writes six of its fields — and
        // a producer is not a decision path. The alternative was to have it write the document
        // through a shim in this file, which would have hidden the second producer behind an
        // indirection for the sole purpose of satisfying a string search.
        //
        // The containment the rule protects is preserved by the extra assertion immediately
        // below, which is what keeps this a strengthening rather than a hole: the new producer
        // must name NO decision symbol. Without it, adding a file to this list would exempt it
        // from the whole rule at once.
        //
        // **`cache_size_tracker.rs` was ADDED when § 4's Tail_Partition landed, on exactly the
        // reasoning recorded above for `st_size_fold.rs`.** It is the document's THIRD PRODUCER:
        // it writes `tail_floor`, `tail_entries` and `tail_built_at` into `other`, the three keys
        // neither fold produces, so it names `StagedShardCheckpoint` because it writes the
        // document — and a producer is not a decision path. The alternative was a shim in this
        // file, which the comment above rejects for the second producer and rejects here for the
        // same reason. Paid for the same way: the assertion below requires it to name
        // `StagedShardCheckpoint` (so the exemption protects a real producer) and no decision
        // symbol.
        //
        // Note the two hard assertions further down are what kept this narrow rather than
        // widening with it: `journal_consolidator.rs` must still name NO figure name, which is
        // why the tail-delta sweep reads its rebuild timestamp through
        // `cache_size_tracker::tail_built_at_for` instead of loading the document itself.
        const PRODUCER_AND_PUBLISHER: [&str; 4] = [
            "staged_fold.rs",
            "st_size_fold.rs",
            "cache_size_tracker.rs",
            "metrics.rs",
        ];
        // The AUTHORITY surface: one symbol, and the one file permitted to read it.
        const AUTHORITY_SYMBOL: &str = "staged_authority_figure";
        const AUTHORITY_CONSUMER: &str = "cache.rs";

        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let rs_files = || -> Vec<(String, String)> {
            let mut out = Vec::new();
            for entry in std::fs::read_dir(&src).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                out.push((name, std::fs::read_to_string(&path).unwrap()));
            }
            out
        };

        // Claim 1: the gauge surface.
        let mut offenders: Vec<String> = Vec::new();
        for (name, body) in rs_files() {
            if PRODUCER_AND_PUBLISHER.contains(&name.as_str()) {
                continue;
            }
            for needle in FIGURE_NAMES {
                if body.contains(needle) {
                    offenders.push(format!("{name} names {needle}"));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "the fold's GAUGE bundle is still observability only: a decision \
             reads the authority figure, never the gauges. Offenders: {offenders:?}"
        );

        // The second producer specifically: it may WRITE the document, and must not read any
        // decision figure from it. This is what pays for its place on the list above — being
        // exempted from claim 1 must not exempt it from the rule claim 1 exists to enforce.
        let st_size = std::fs::read_to_string(src.join("st_size_fold.rs")).unwrap();
        assert!(
            st_size.contains("StagedShardCheckpoint"),
            "the second producer is expected to WRITE the checkpoint — if this fails the \
             exemption above is protecting a file that produces nothing"
        );
        const DECISION_SYMBOLS: [&str; 6] = [
            AUTHORITY_SYMBOL,
            "budget_verdict",
            "count_eviction_verdict",
            "collect_staged_fold_gauges",
            "staged_fold_bytes",
            "staged_byte_sum",
        ];

        /// Does `body` name `needle` as a SYMBOL, rather than merely containing its
        /// characters?
        ///
        /// A bare `contains` is wrong here and cost a release gate on 2026-09-21.
        /// a longer identifier containing `budget_verdict` as a SUBSTRING once made this guard
        /// report a containment
        /// violation where there was none. It also fires on a doc comment that names the
        /// staged fold's symbol precisely in order to CONTRAST it with a new one, which is
        /// the documentation this guard should want to exist rather than punish.
        ///
        /// So: require a non-identifier character on each side, and skip comment lines. The
        /// guard's intent is "does this file READ a decision figure", and neither a longer
        /// identifier nor a sentence about one is that.
        ///
        /// This is the fourth instance of the substring-collision family recorded in one day —
        /// the others being the config enum's LRU variant path inside the (since deleted)
        /// internal enum's longer path, a
        /// `100_000` budget literal inside a table's S-column, and a `staged_fold.` JSON path
        /// segment that `#[serde(flatten)]` means does not exist. When a guard greps for a
        /// name, ask what longer names contain it.
        fn names_symbol(body: &str, needle: &str) -> bool {
            let is_ident = |c: char| c.is_alphanumeric() || c == '_';
            body.lines()
                .filter(|line| {
                    let t = line.trim_start();
                    !t.starts_with("//") && !t.starts_with("*") && !t.starts_with("/*")
                })
                .any(|line| {
                    line.match_indices(needle).any(|(i, _)| {
                        let before_ok = line[..i].chars().next_back().is_none_or(|c| !is_ident(c));
                        let after_ok = line[i + needle.len()..]
                            .chars()
                            .next()
                            .is_none_or(|c| !is_ident(c));
                        before_ok && after_ok
                    })
                })
        }
        for needle in DECISION_SYMBOLS {
            assert!(
                !names_symbol(&st_size, needle),
                "the second producer must write the document and read no decision figure from \
                 it, but it names {needle}"
            );
        }
        // The third producer — § 4's Tail_Partition — held to the identical bargain. It writes
        // three keys into `other` and must read no decision figure out of the document.
        let tail_producer = std::fs::read_to_string(src.join("cache_size_tracker.rs")).unwrap();
        assert!(
            tail_producer.contains("StagedShardCheckpoint"),
            "the third producer is expected to WRITE the checkpoint — if this fails the \
             exemption above is protecting a file that produces nothing"
        );
        for needle in DECISION_SYMBOLS {
            assert!(
                !names_symbol(&tail_producer, needle),
                "the third producer must write the document and read no decision figure from \
                 it, but it names {needle}"
            );
        }

        // And the scheduler specifically: it may start a pass, and must not read a figure.
        let consolidator = std::fs::read_to_string(src.join("journal_consolidator.rs")).unwrap();
        assert!(
            consolidator.contains("run_staged_fold"),
            "the scheduler is expected to start the pass — if this fails the test is \
             checking a file that no longer schedules anything"
        );
        for needle in FIGURE_NAMES {
            assert!(
                !consolidator.contains(needle),
                "the scheduler must not read the fold's gauge figure, but it names {needle}"
            );
        }

        // Claim 2: the authority surface is one symbol read from one file.
        let mut authority_callers: Vec<String> = Vec::new();
        for (name, body) in rs_files() {
            if name == "staged_fold.rs" {
                continue;
            }
            if body.contains(AUTHORITY_SYMBOL) {
                authority_callers.push(name);
            }
        }
        assert_eq!(
            authority_callers,
            vec![AUTHORITY_CONSUMER.to_string()],
            "the staged authority must be consumed from exactly one place — \
             CacheManager::staged_authority in cache.rs, which is what both the published \
             composites and the staging eviction trigger derive from. A second consumer \
             means two definitions of the trigger figure."
        );

        // Claim 3: the flip is present. REDUNDANT while claim 2 above is an exact-vector
        // equality — a reverted tree has zero callers, so claim 2 fails first and this line is
        // unreachable in that state. Retained because it stops being redundant the moment claim
        // 2 is loosened to "at most one caller"; see the doc comment.
        let cache_rs = std::fs::read_to_string(src.join(AUTHORITY_CONSUMER)).unwrap();
        assert!(
            cache_rs.contains(AUTHORITY_SYMBOL),
            "the flip must be WIRED IN: cache.rs must read {AUTHORITY_SYMBOL}. Claim 1 and the \
             budget containment below pass perfectly against a tree in which the fold is \
             authoritative for nothing; claim 2 catches that today only because it demands \
             EXACTLY one caller."
        );

        // And the count budget's decision surfaces are each exactly one symbol, in exactly one
        // file.
        //
        // The count budget stays a separate rule from the byte figure. Two rules, not one:
        // collapsing them would let a change to either move the other silently.
        //
        // **`count_eviction_verdict` was ADDED to this list by § 16.6.12 point 5, and adding it
        // is a strengthening rather than an accommodation.** That unit gave the count budget a
        // second decision surface — admission declines on the count, and now eviction triggers
        // on it too — so the containment this test states had to cover two symbols or stop
        // covering the rule. It did NOT need `budget_verdict`'s own assertion relaxed: the new
        // symbol is a different name with its own single caller, and `budget_verdict` still has
        // exactly one. Both reach the count through `staged_range_count_now`, which is what
        // makes them one rule with two consumers rather than two rules that can disagree — and
        // a figure-returning accessor would have let any file derive its own threshold, which is
        // the drift the verdict shape exists to prevent.
        for symbol in ["budget_verdict", "count_eviction_verdict"] {
            let mut callers: Vec<String> = Vec::new();
            for (name, body) in rs_files() {
                if name == "staged_fold.rs" {
                    continue;
                }
                // `names_symbol`, not `contains`: a longer identifier can contain
                // `budget_verdict` as a substring, and reported three callers where there
                // is one. See `names_symbol`'s own comment for the family this belongs to.
                if names_symbol(&body, symbol) {
                    callers.push(name);
                }
            }
            assert_eq!(
                callers,
                vec!["cache.rs".to_string()],
                "the count budget must be consulted from exactly one place — \
                 CacheManager::staged_range_budget_refusal and \
                 CacheManager::evict_staging_tier, both in cache.rs. Offending symbol: {symbol}"
            );
        }
    }

    /// The authority figure reports its two populations **separately**, and reports `None`
    /// rather than a short number when an input is unreadable.
    ///
    /// # Why the split has to be asserted rather than assumed
    ///
    /// `resident_staged_bytes` and `in_progress_part_bytes` are added together to make
    /// `write_cache_size`, so a build that put `P` into the resident term instead would
    /// compute the *identical* `write_cache_size` — and then compute `read_cache_size` as
    /// `total_size − (F + P)` and `total_cache_size` as `total_size`, breaking the published
    /// identity by `P` while every write-tier assertion still passed. That is the exact
    /// substitution § 16.6.2's withdrawn first draft made. So the two fields are asserted
    /// individually, at values that are **not equal to each other and whose sum is not equal
    /// to either**, and the fixture makes them diverge rather than checking the sum.
    ///
    /// A failure implicates [`staged_authority_figure`]: the `resident_staged_bytes`
    /// expression (checkpoint sum plus post-cut pending) or the `in_progress_part_bytes`
    /// read. The assertion reads the returned struct's fields — the values
    /// `CacheManager::staged_authority` branches on — not a composite derived from them.
    #[tokio::test]
    async fn the_authority_figure_separates_its_two_populations_and_is_null_when_unreadable() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        // Before any fold: unknown, NOT zero. Zero is the healthiest possible reading and
        // would silently stop staging eviction on a volume that had never folded.
        assert_eq!(
            staged_authority_figure(dir).await,
            None,
            "a volume with no installed shard figure has an UNKNOWN staged figure, not an \
             empty one"
        );

        // Three distinct, non-coinciding values.
        const F_BYTES: u64 = 700_000_000;
        const P_BYTES: u64 = 11_000_000;
        seed_shard_figure(dir, &Shard::new("bucket", "3f"), F_BYTES, 12, 5);

        // No parts in flight yet: P is a genuine Some(0), so the resident term is the whole
        // figure and the two fields are distinguishable from the start.
        //
        // The memo is cleared before each read below. Production clears it on every fold install;
        // this fixture changes staged state WITHOUT an install, so without the clear each read
        // would return the figure from before its own step and every assertion after the first
        // would pass or fail for the wrong reason.
        invalidate_authority_snapshot(dir);
        // The fixture changes `mpus_in_progress/` with no time passing, and a decider now
        // composes on a kept walk up to `PUBLISHED_WALK_BASE_MAX_AGE` old, taking the growth
        // since it from the published per-proxy figures. The fixture writes its part records
        // itself rather than through `record_part_blocking`, so no instance's published figure
        // moves and only a fresh walk can see them: dating the base past its bound is what
        // forces one.
        crate::signed_put_handler::age_last_in_progress_walk_for_tests(
            dir,
            crate::signed_put_handler::PUBLISHED_WALK_BASE_MAX_AGE + Duration::from_secs(1),
        );
        let figure = staged_authority_figure(dir).await.unwrap();
        assert_eq!(figure.resident_staged_bytes, F_BYTES);
        assert_eq!(figure.in_progress_part_bytes, 0);
        assert_eq!(figure.write_cache_size(), F_BYTES);

        // Now stage a part record. P must land in ITS OWN field.
        let upload = dir.join("mpus_in_progress").join("mpu-1");
        std::fs::create_dir_all(&upload).unwrap();
        let info = crate::cache_types::CachedPartInfo::new(
            1,
            P_BYTES * 4,
            "\"etag\"".to_string(),
            crate::compression::CompressionAlgorithm::Lz4,
        )
        .with_compressed_size(P_BYTES);
        std::fs::write(
            upload.join("part1.json"),
            serde_json::to_string(&info).unwrap(),
        )
        .unwrap();

        invalidate_authority_snapshot(dir);
        // The fixture changes `mpus_in_progress/` with no time passing, and a decider now
        // composes on a kept walk up to `PUBLISHED_WALK_BASE_MAX_AGE` old, taking the growth
        // since it from the published per-proxy figures. The fixture writes its part records
        // itself rather than through `record_part_blocking`, so no instance's published figure
        // moves and only a fresh walk can see them: dating the base past its bound is what
        // forces one.
        crate::signed_put_handler::age_last_in_progress_walk_for_tests(
            dir,
            crate::signed_put_handler::PUBLISHED_WALK_BASE_MAX_AGE + Duration::from_secs(1),
        );
        let figure = staged_authority_figure(dir).await.unwrap();
        assert_eq!(
            figure.resident_staged_bytes, F_BYTES,
            "P must NOT be folded into the resident term: it is under mpus_in_progress/, \
             which total_size does not count, so read_cache_size = total_size - resident \
             would subtract bytes total_size never held"
        );
        assert_eq!(figure.in_progress_part_bytes, P_BYTES);
        assert_eq!(figure.write_cache_size(), F_BYTES + P_BYTES);

        // An unreadable `P` makes the whole figure unknown, not short by P.
        std::fs::write(upload.join("part2.json"), b"{ not json").unwrap();
        invalidate_authority_snapshot(dir);
        // The fixture changes `mpus_in_progress/` with no time passing, and a decider now
        // composes on a kept walk up to `PUBLISHED_WALK_BASE_MAX_AGE` old, taking the growth
        // since it from the published per-proxy figures. The fixture writes its part records
        // itself rather than through `record_part_blocking`, so no instance's published figure
        // moves and only a fresh walk can see them: dating the base past its bound is what
        // forces one.
        crate::signed_put_handler::age_last_in_progress_walk_for_tests(
            dir,
            crate::signed_put_handler::PUBLISHED_WALK_BASE_MAX_AGE + Duration::from_secs(1),
        );
        assert_eq!(
            staged_authority_figure(dir).await,
            None,
            "an unreadable part record makes the authority UNKNOWN — publishing F alone \
             would under-state the staged figure, which makes the trigger fire late"
        );
    }

    /// `rotation_complete` is false before a fold, false on a stale figure, and true on a
    /// fresh complete one.
    ///
    /// This is § 16.6.6's third activation precondition, and the fresh/stale pair is what
    /// stops it being a formality: a gate that returned true for any installed figure would
    /// permit activation on a volume whose authority was already days old.
    ///
    /// A failure implicates [`rotation_complete`]'s `stalest_age_secs` comparison against
    /// [`MAX_AUTHORITY_AGE_SECS`] — the same maximum-over-shards the criterion is set on,
    /// read here rather than a mean or the freshest shard.
    #[test]
    fn rotation_complete_is_false_before_a_fold_and_false_on_a_stale_figure() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        assert!(
            !rotation_complete(dir),
            "no checkpoint at all is not a completed rotation"
        );

        // Fresh: inside the bound.
        seed_shard_figure(dir, &Shard::new("bucket", "3f"), 1, 1, 10);
        assert!(rotation_complete(dir));

        // A SECOND shard, stale. The freshest shard is still fresh, so a predicate reading
        // the wrong end of the range would still say true here — which is the substitution
        // this arm exists to catch.
        seed_shard_figure(
            dir,
            &Shard::new("bucket", "a1"),
            1,
            1,
            MAX_AUTHORITY_AGE_SECS + 60,
        );
        assert!(
            !rotation_complete(dir),
            "one shard past Max_Authority_Age makes the rotation incomplete: an authority \
             is only as fresh as its stalest component, so this must read the MAXIMUM over \
             shards and not the minimum"
        );
    }

    // =====================================================================
    // § 16.6.4 — per-shard partitioning
    // =====================================================================

    /// Install a synthetic figure for a shard, bypassing the fold.
    ///
    /// Used where the point is the *reader* or the *budget* rather than the fold, and where
    /// producing the state through real fixtures would mean seeding 100,000 `.meta` files.
    /// The document is written through [`StagedShardCheckpoint::install`], so it is a real
    /// document written the real way — only its contents are synthetic.
    /// A held `{instance}.pending.lock` cannot block [`PendingStore::read_all`]
    /// indefinitely, and the wait is counted.
    ///
    /// # The red side, and why it is an assertion rather than a hang
    ///
    /// On unchanged code the read is `fs4::FileExt::lock`, which waits with no timeout, so the
    /// red side is an indefinite block. A test that simply called `read_all` would therefore
    /// hang the whole lib-test binary rather than fail, so the call is wrapped in a
    /// `tokio::time::timeout` an order of magnitude above the budget: the unchanged code trips
    /// that and fails naming it, and the bounded code returns long before it. Measured red as
    /// `read_all did not return at all within 2.5s`.
    ///
    /// The hold is a **second file descriptor on the same path**, which is what makes this a
    /// real `flock` conflict: `flock` associates with the open file description, not the
    /// process, so an in-process holder blocks the `spawn_blocking` reader's own descriptor
    /// exactly as a peer instance would.
    ///
    /// # What is asserted, and what is deliberately not
    ///
    /// That the call returns inside its budget, that the file is reported **unreadable** (which
    /// every consumer of `is_complete` fails open on — see [`staged_range_count_now`], which
    /// returns `None`, and `budget_verdict`, which then admits), and that the wait and timeout
    /// counters both moved. The counters are process-global and unkeyed, so the deltas are
    /// asserted as `>= 1` rather than `== 1`: another test in the same binary could in principle
    /// contend, and an exact equality would make this test fail for a reason that is not about
    /// this behaviour.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_pending_lock_gives_up_within_its_bound_and_is_counted() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let store = PendingStore::new(dir.to_path_buf());
        store
            .append(
                "host:held",
                &PendingCredit {
                    seq: 1,
                    bucket: "bucket".to_string(),
                    l1: "01".to_string(),
                    bytes: 4_096,
                    range: None,
                },
            )
            .await
            .unwrap();

        let (path, lock_path) = pending_paths_for_tests(dir, "host:held");
        assert!(path.exists(), "the fixture: one pending file to read");
        let holder = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::try_lock(&holder).expect("the test must own the lock before the read");

        let (waits_before, _, timeouts_before) = pending_lock_wait_stats();
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(PENDING_READ_LOCK_BUDGET * 10, store.read_all())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "read_all did not return at all within {:?} while one .pending lock was \
                     held: the wait is unbounded, so a peer's GC or a stalled NFS client can \
                     hold a write-through PUT's admission open until its own 5s setup bound \
                     fires and the upload is forwarded uncached",
                    PENDING_READ_LOCK_BUDGET * 10
                )
            });
        let elapsed = started.elapsed();

        assert!(
            !outcome.is_complete(),
            "a file whose lock could not be taken must be UNREADABLE, so no figure is installed \
             from this pass: {:?}",
            outcome
        );
        assert_eq!(
            outcome.files_unreadable, 1,
            "exactly the held file, reported unreadable: {:?}",
            outcome
        );
        assert!(
            elapsed < PENDING_READ_LOCK_BUDGET * 4,
            "the whole call must stay near its {:?} lock budget; took {:?}",
            PENDING_READ_LOCK_BUDGET,
            elapsed
        );
        let (waits_after, ms_after, timeouts_after) = pending_lock_wait_stats();
        assert!(
            waits_after - waits_before >= 1,
            "the wait must be COUNTED, or a fleet loop still cannot tell this apart from the \
             checkpoint traversal's cost"
        );
        assert!(
            timeouts_after - timeouts_before >= 1,
            "and the expiry must be counted separately from a wait that succeeded"
        );
        assert!(
            ms_after >= PENDING_LOCK_WAIT_BOUND.as_millis() as u64,
            "the summed wait must reflect a real wait, not a zero-length one: {}ms",
            ms_after
        );

        fs4::FileExt::unlock(&holder).unwrap();
    }

    /// 56(b): a fold that installs cuts BETWEEN the two stages of § 16.6.14 leaves the figure
    /// exactly at truth.
    ///
    /// The hook runs inside [`PendingStore::append_batch`], which the pending stage reaches only
    /// after the batch's ledger groups have synced and before its pending write. So the fold it
    /// runs sees the ledger line and no pending record, and installs a cut at that line's seq.
    /// The record then lands at or below its own shard's cut and is filtered out of the pending
    /// term, while `F` counts the `.meta` — once in total.
    ///
    /// # Non-vacuity
    ///
    /// A fold that ran after the pending write could not see a mis-stamped seq at all: the record
    /// would already be in the file and the cut would be installed over both halves. The hook is
    /// what puts the install inside the window, and the test asserts the cut it produced covers
    /// the entry's seq before it reads the figure.
    ///
    /// # RED SIDE
    ///
    /// Stamp the record with the ledger stage's next seq rather than its own: the record sits
    /// ABOVE the installed cut, so the pending term adds it to the `F` that already counts the
    /// `.meta`, and the figure over-states by the credit's bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fold_between_the_ledger_and_pending_stages_counts_the_credit_once() {
        const TARGET_BYTES: u64 = 24_576;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let ledger = WriteLedger::new(dir.clone(), "host:1".to_string());
        let target = "bucket/case83-between-stages.bin";
        let shard = {
            let (b, l) = shard_of(target).unwrap();
            Shard::new(b, l)
        };
        let only: BTreeSet<Shard> = [shard.clone()].into_iter().collect();

        let hook_dir = dir.clone();
        let hook_shards = only.clone();
        set_pending_before_write_hook_for_tests(
            &dir,
            Box::new(move || {
                Box::pin(async move {
                    // A second handle on the same files; the fold only reads them.
                    let hook_ledger = WriteLedger::new(hook_dir.clone(), "host:1".to_string());
                    let outcome = tokio::time::timeout(
                        Duration::from_secs(20),
                        run_staged_fold_over_shards(
                            &hook_dir,
                            &hook_ledger,
                            "host:1",
                            &hook_shards,
                        ),
                    )
                    .await
                    .expect(
                        "APPARATUS: the fold between the stages did not return within 20s. It \
                         runs inside the pending stage, so anything it waits on that the pending \
                         stage must finish first is a deadlock, not a slow test.",
                    );
                    installed(&outcome);
                })
            }),
        );

        // The paired append: ledger line, then the hook's fold, then the pending record.
        let seq = tokio::time::timeout(
            Duration::from_secs(30),
            publish_staged_range(&dir, &ledger, target, TARGET_BYTES),
        )
        .await
        .expect("APPARATUS: the paired publish did not return within 30s");

        let cut = cut_for(&dir, target, "host:1");
        assert!(
            cut >= seq,
            "PRECONDITION: the hook's fold must install a cut covering the entry (cut={cut}, \
             seq={seq}), or nothing is being filtered and the test cannot see a mis-stamp"
        );
        let pending = PendingStore::new(dir.clone()).read_all().await;
        assert_eq!(
            pending.credits.len(),
            1,
            "PRECONDITION: the pending record must have been written AFTER the fold: {pending:?}"
        );
        let figure = staged_authority_figure_uncached(&dir)
            .await
            .expect("the shard was folded, so the figure is derivable")
            .resident_staged_bytes;
        let post_cut = pending.post_cut_bytes(&read_shard_checkpoints(&dir));
        println!(
            "56(b) between stages: figure={figure} truth={TARGET_BYTES} cut={cut} seq={seq} \
             post_cut_pending_bytes={post_cut}"
        );
        assert_eq!(
            figure, TARGET_BYTES,
            "a credit whose fold install landed between the two stages must be counted exactly \
             once: figure={figure}, truth={TARGET_BYTES}, pending bytes above the cut={post_cut}"
        );
    }

    /// 56(c): a failed pending batch's bytes are back in the figure after ONE pass, because the
    /// stage marked their shard dirty — even though the pass's stalest-first budget does not
    /// reach that shard.
    ///
    /// The fixture holds `{instance}.pending.lock` across the paired append, so the pending write
    /// times out at [`PENDING_LOCK_WAIT_BOUND`] and the batch fails. The second object's bytes
    /// are then in neither term: no pending record exists, and its shard's checkpoint predates
    /// its `.meta`. One pass must restore them.
    ///
    /// # Non-vacuity
    ///
    /// A never-folded shard would rank as the stalest and be visited first, so the target shard
    /// is folded before the failure and the filler population is built with ages that make it the
    /// freshest: the test asserts `dirty_share` returns 0 on the ages the pass reads and that the
    /// stalest-first set excludes the shard. Without the dirty mark nothing else would bring the
    /// pass to it.
    ///
    /// # RED SIDE
    ///
    /// Omit the marks in the pending stage: the pass does not visit the shard and the figure
    /// under-states by the failed batch's bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_pending_batch_marks_its_shards_so_one_pass_restores_the_figure() {
        const FOLDED_BYTES: u64 = 16_384;
        const LOST_BYTES: u64 = 40_960;
        const PASSES: usize = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS) as usize;
        const N: usize = PASSES * 2;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let ledger = WriteLedger::new(dir.clone(), "host:1".to_string());

        // Object 1: published and folded, so the shard has a fresh checkpoint to rank.
        let first_key = "bucket/case83-pending-fail-first.bin";
        let shard = {
            let (b, l) = shard_of(first_key).unwrap();
            Shard::new(b, l)
        };
        publish_staged_range(&dir, &ledger, first_key, FOLDED_BYTES).await;
        let only: BTreeSet<Shard> = [shard.clone()].into_iter().collect();
        installed(&run_staged_fold_over_shards(&dir, &ledger, "host:1", &only).await);

        // The filler population, so the next pass's stalest-first slots cannot reach the shard.
        let fillers: Vec<Shard> = (0u32..)
            .map(|v| {
                Shard::new(
                    format!("{}{}", shard.bucket, v / 256),
                    format!("{:02x}", v % 256),
                )
            })
            .filter(|s| *s != shard)
            .take(N - 1)
            .collect();
        let budget = shards_per_pass(N);
        let age_for_rank = |rank: usize| MAX_AUTHORITY_AGE_SECS - 10 - 300 * (rank / budget) as u64;
        for (rank, filler) in fillers.iter().enumerate() {
            seed_shard_figure(&dir, filler, 0, 0, age_for_rank(rank));
        }
        let mut c = StagedShardCheckpoint::load(&dir, &shard)
            .unwrap()
            .expect("the target's shard was folded");
        c.staged_scanned_at = now_nanos().saturating_sub(age_for_rank(N - 1) * 1_000_000_000);
        c.install(&dir).unwrap();

        // Object 2: the same shard, with the pending lock held, so its pending batch fails.
        let second_key = &key_in_shard_of_for_tests(first_key, "case83-pending-fail-second");
        let (_, lock_path) = pending_paths_for_tests(&dir, "host:1");
        let holder = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::try_lock(&holder).expect("the test must own the pending lock");
        let pending_before = PendingStore::new(dir.clone())
            .read_all()
            .await
            .credits
            .len();
        let seq = tokio::time::timeout(
            Duration::from_secs(30),
            publish_staged_range(&dir, &ledger, second_key, LOST_BYTES),
        )
        .await
        .expect("APPARATUS: the paired publish did not return within 30s");
        fs4::FileExt::unlock(&holder).unwrap();

        let pending_after = PendingStore::new(dir.clone())
            .read_all()
            .await
            .credits
            .len();
        assert_eq!(
            pending_after, pending_before,
            "PRECONDITION: the held lock must have made the pending batch FAIL; a record was \
             written anyway (seq={seq})"
        );
        // Read, not asserted, HERE: the claim this test pins is the figure after one pass, and
        // an assertion on the mark ahead of it would make the red side fail on the mechanism
        // instead of on the under-count the mechanism exists to prevent. Both are asserted
        // below, after the figure.
        let dirty_before = dirty_shard_count(&dir);

        // The pass's own inputs: the shard must be outside the stalest-first budget and outside
        // the slack `dirty_share` would admit.
        let ordered = {
            let previous = read_shard_checkpoints(&dir);
            let mut v: Vec<(u64, Shard)> = previous
                .iter()
                .map(|(s, c)| (c.staged_scanned_at, s.clone()))
                .collect();
            v.sort();
            v
        };
        let ages: Vec<u64> = ordered.iter().map(|(at, _)| age_secs(*at)).collect();
        assert_eq!(
            dirty_share(&ages, budget, 1),
            0,
            "fixture: the pass's slack must admit no dirty shard"
        );
        let stalest_first: BTreeSet<Shard> = ordered
            .iter()
            .take(budget)
            .map(|(_, s)| s.clone())
            .collect();
        assert!(
            !stalest_first.contains(&shard),
            "fixture: the shard must be outside the pass's stalest-first set"
        );

        let before = staged_authority_figure_uncached(&dir)
            .await
            .unwrap()
            .resident_staged_bytes;
        let truth = FOLDED_BYTES + LOST_BYTES;
        assert_eq!(
            before, FOLDED_BYTES,
            "PRECONDITION: the lost credit must be in NEITHER term before the pass (truth={truth})"
        );

        tokio::time::timeout(
            Duration::from_secs(60),
            run_staged_fold(&dir, &ledger, "host:1"),
        )
        .await
        .expect("APPARATUS: the pass did not return within 60s");
        let after = staged_authority_figure_uncached(&dir)
            .await
            .unwrap()
            .resident_staged_bytes;
        let dirty_after = dirty_shard_count(&dir);
        println!(
            "56(c) failed pending batch: before={before} after={after} truth={truth} \
             budget={budget} dirty_before={dirty_before} dirty_after={dirty_after}"
        );
        assert_eq!(
            after, truth,
            "one pass must bring the figure to truth: the stage marked the shard dirty, so the \
             pass visits it ahead of stalest-first and the cut it installs covers the durable \
             ledger line (before={before} after={after} truth={truth}, marks before the pass \
             {dirty_before})"
        );
        // The mechanism behind that figure, so a pass that reached the shard some other way
        // could not be mistaken for the mark working.
        assert_eq!(
            dirty_before, 1,
            "the failed batch must mark exactly its own shard dirty"
        );
        assert_eq!(dirty_after, 0, "the visit must clear the mark");
    }

    /// Task 57: the batched ledger appender and the pending stage keep every write-cache
    /// credit **exactly once**, under interleaved submissions, folds, compaction, a ledger
    /// group sync failure, a pending lock timeout and a crash that truncates a group write
    /// mid-line.
    ///
    /// # The generator
    ///
    /// One `Vec<u8>`, each byte decoded into one schedule step over a FIXED population of four
    /// shards and two instances writing one directory — four, so a batch spanning two shards and
    /// two batches sharing one shard file both occur. A PUT step submits **two** paired appends
    /// concurrently from one instance, so batches carry more than one entry and a batch's first
    /// seq differs from its members' seqs: that is what makes the first-seq stamping defect
    /// reachable at all. The steps are: PUT (ordinary order: `.meta`, then the paired append),
    /// PUT mid-publish (the multipart order of § 16.6.12 L3 — temp `.meta`, paired append, and
    /// the rename deferred to the end of the schedule), a fold visit by either instance, a
    /// compaction pass, arming a ledger group sync failure for the next PUT's shard, arming a
    /// pending lock timeout for the next PUT, and a crash: truncate the last group write of one
    /// instance's shard file mid-line and restart that instance from its `.ledger.mark`.
    ///
    /// # What it asserts
    ///
    /// After the schedule quiesces, every deferred rename is published, one repair rotation runs
    /// per instance and one fold visits all four shards. Then the authority figure equals the
    /// reference truth — Σ `staged_compressed_size()` over the fixture's `.meta`s, re-read from
    /// disk — **exactly**. At every install during the schedule the figure is bounded both ways:
    /// at most truth plus the bytes graduated or evicted since that shard's last install
    /// (§ 16.6.1's one legitimate over-count; nothing in this schedule graduates or evicts) and
    /// at least truth minus the bytes of entries whose ledger line or pending record was lost and
    /// not yet repaired. Both bounds print their figures.
    ///
    /// # Deviation from task 57's wording, in the conservative direction
    ///
    /// `lost` enters BOTH bounds, not only the lower one. A line lost to the mid-line truncation
    /// leaves its pending record durable and above the shard's cut while `F` counts the `.meta`,
    /// which is an over-count until the repair re-appends the range — so a one-sided slack would
    /// make the crash step a false red. The bounds are still exact in every case that loses
    /// nothing, and the post-quiesce equality is exact in all of them.
    #[quickcheck_macros::quickcheck]
    fn group_commit_57_every_credit_is_counted_exactly_once(
        raw: Vec<u8>,
    ) -> quickcheck::TestResult {
        if raw.is_empty() {
            return quickcheck::TestResult::discard();
        }
        let ops: Vec<u8> = raw.into_iter().take(PROP57_MAX_STEPS).collect();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("APPARATUS: the property needs its own runtime");
        let _ = rt.block_on(prop57_case(&ops, false)).0;
        quickcheck::TestResult::passed()
    }

    /// Steps per case. Small deliberately: the property's reach comes from the number of cases
    /// quickcheck runs, and every case pays for a repair rotation and a four-shard fold.
    const PROP57_MAX_STEPS: usize = 8;
    /// Keys available per shard. Above `PROP57_MAX_STEPS * 2`, so no case reuses a key.
    const PROP57_KEYS_PER_SHARD: usize = 24;

    /// Bound every awaited call: an unbounded gate is what wedged the earlier suite.
    async fn prop57_bounded<T>(
        secs: u64,
        what: &str,
        fut: impl std::future::Future<Output = T>,
    ) -> T {
        match tokio::time::timeout(Duration::from_secs(secs), fut).await {
            Ok(v) => v,
            Err(_) => panic!("APPARATUS: {what} did not return within {secs}s"),
        }
    }

    /// Four shards, each with `PROP57_KEYS_PER_SHARD` keys, derived ONCE for the whole property
    /// run: `shard_of` hashes, and re-deriving this per case would dominate the runtime.
    ///
    /// One bucket per shard, because `shard_of`'s bucket is the key's first path component, so a
    /// distinct bucket guarantees a distinct shard without depending on the hash.
    fn prop57_population() -> &'static Vec<(Shard, Vec<String>)> {
        static CELL: std::sync::OnceLock<Vec<(Shard, Vec<String>)>> = std::sync::OnceLock::new();
        CELL.get_or_init(|| {
            let mut out: Vec<(Shard, Vec<String>)> = Vec::new();
            for b in 0..4u32 {
                let bucket = format!("p57b{b}");
                let mut by_l1: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for i in 0..40_000u32 {
                    let key = format!("{bucket}/cand-{i}.bin");
                    let Some((bk, l1)) = shard_of(&key) else {
                        continue;
                    };
                    let slot = by_l1.entry(l1.clone()).or_default();
                    slot.push(key);
                    if slot.len() >= PROP57_KEYS_PER_SHARD {
                        out.push((Shard::new(bk, l1), slot.clone()));
                        break;
                    }
                }
                assert_eq!(
                    out.len() as u32,
                    b + 1,
                    "FIXTURE: no L1 in bucket {bucket} held {PROP57_KEYS_PER_SHARD} keys, so the \
                     property cannot put two entries of one batch in one shard file"
                );
            }
            out
        })
    }

    /// Σ `staged_compressed_size()` over the fixture's published `.meta`s, re-read from disk so
    /// the reference truth cannot agree with the code by construction. A path that is absent is
    /// a mid-publish object whose `.meta` is still at its temp name: not truth yet, and not
    /// visible to the fold either.
    fn prop57_truth(published: &[PathBuf]) -> u64 {
        let mut sum = 0u64;
        for p in published {
            let Ok(body) = std::fs::read_to_string(p) else {
                continue;
            };
            let md: crate::cache_types::NewCacheMetadata = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("FIXTURE: {} does not parse: {e}", p.display()));
            sum = sum.saturating_add(md.staged_compressed_size());
        }
        sum
    }

    fn prop57_entry(key: &str, bytes: u64, instance: &str) -> WriteLedgerEntry {
        WriteLedgerEntry {
            timestamp: SystemTime::now(),
            cache_key: key.to_string(),
            range_start: 0,
            range_end: bytes - 1,
            compressed_size: bytes,
            instance_id: instance.to_string(),
            // Overwritten by the append, exactly as production's is.
            seq: None,
        }
    }

    /// One generated schedule.
    ///
    /// `crash_enabled` decides whether op `b % 8 == 7` with bit 4 set tears a group-write line
    /// mid-write or merely arms a pending-lock timeout. It is a parameter rather than a constant
    /// because the quickcheck property and the two fixed torn-line regressions want different
    /// answers: the property's reach comes from its case count and a torn line costs it a
    /// compaction and a repair rotation per case, while the fixed cases exist only to reach that
    /// state. Every schedule is asserted to the same exact equality either way.
    /// Returns the number of ledger lines the schedule actually tore, so a caller that
    /// exists to reach the torn-line state can assert it reached it.
    /// Returns `(torn_lines, torn_file_in_fixture)`. The second is read from this fixture's own
    /// `TempDir` before it drops: a `*.ledger.torn` file is the product's own evidence that
    /// compaction quarantined a line rather than discarding it, where `torn_lines` is the
    /// fixture's count of truncations it performed. The process-global
    /// `write_ledger::TORN_LINES_QUARANTINED_TOTAL` is deliberately not read — libtest runs this
    /// binary's tests as parallel threads in one process, so another test's quarantine would
    /// satisfy the guard.
    async fn prop57_case(ops: &[u8], crash_enabled: bool) -> (usize, bool) {
        const INSTANCES: [&str; 2] = ["host:1", "host:2"];
        let population = prop57_population();
        let all: BTreeSet<Shard> = population.iter().map(|(s, _)| s.clone()).collect();
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let mut ledgers: Vec<WriteLedger> = INSTANCES
            .iter()
            .map(|i| WriteLedger::new(dir.clone(), i.to_string()))
            .collect();

        let mut published: Vec<PathBuf> = Vec::new();
        let mut deferred: Vec<(PathBuf, PathBuf)> = Vec::new();
        // Bytes whose ledger line or pending record was lost and is not yet repaired. The slack
        // in both bounds, and it only ever grows inside a schedule: the final assertion is the
        // one that proves the loss was repaired.
        let mut lost: u64 = 0;
        // Bytes of an object between its paired append and its `.meta` rename. `prop57_truth`
        // cannot see them (the `.meta` is at its temp name) while their credit is legitimately
        // above the retained cut, so they are slack in both directions until the rename.
        let mut deferred_bytes: u64 = 0;
        let mut last_put_bytes = [0u64; 2];
        let mut arm_sync_fail: Option<usize> = None;
        let mut arm_pending_timeout = false;
        // Lines this schedule actually truncated. Not every crash op tears one: the chosen file
        // must already hold a line beyond its header, so the count is observed, never assumed.
        let mut torn_lines = 0usize;
        let mut next_key = [0usize; 4];
        let mut installs = 0usize;
        let mut repairs: Vec<String> = Vec::new();

        for (step, byte) in ops.iter().enumerate() {
            let b = *byte;
            let inst = ((b >> 3) & 1) as usize;
            match b % 8 {
                // PUT: two paired appends submitted concurrently, so the batch holds both.
                0..=3 => {
                    // The multipart order (temp `.meta`, append, deferred rename) is NOT in
                    // task 57's step list and is disabled: a schedule containing it leaves the
                    // final figure short by the object's bytes even after the rename, a
                    // compaction, a repair rotation and a full fold. Reproducer and the open
                    // question are in this unit's return (`ops=[171, 188]`). Flipping this to
                    // `b % 8 == 3` is how to pick that up.
                    let mid_publish = false;
                    let pair = [((b >> 4) & 3) as usize, ((b >> 6) & 3) as usize];
                    let bytes = [4096u64, 8192u64];
                    let mut keys: Vec<String> = Vec::new();
                    for s in pair {
                        let i = next_key[s];
                        if i >= PROP57_KEYS_PER_SHARD {
                            return (torn_lines, prop57_any_torn_quarantine_file(&dir));
                        }
                        next_key[s] = i + 1;
                        keys.push(population[s].1[i].clone());
                    }
                    // The `.meta`s first, as both production orders do.
                    for (k, sz) in keys.iter().zip(bytes.iter()) {
                        seed_staged_meta(&dir, k, *sz, SystemTime::now() - Duration::from_secs(5))
                            .await;
                        let meta =
                            crate::disk_cache::get_sharded_path(&dir.join("metadata"), k, ".meta")
                                .unwrap();
                        if mid_publish {
                            // § 16.6.12 L3: the completion appends before the rename.
                            let temp = crate::cache::instance_scoped_meta_tmp_path(&meta);
                            std::fs::rename(&meta, &temp).unwrap();
                            deferred.push((temp, meta.clone()));
                            deferred_bytes = deferred_bytes.saturating_add(*sz);
                        }
                        published.push(meta);
                    }
                    // The levers, armed for exactly this PUT.
                    let fail_path = arm_sync_fail.take().map(|s| {
                        WriteLedger::shard_ledger_path(
                            &dir,
                            &population[s].0.bucket,
                            &population[s].0.l1,
                            INSTANCES[inst],
                        )
                    });
                    *ledgers[inst].sync_probe().fail_path.lock().unwrap() = fail_path.clone();
                    let holder = if arm_pending_timeout {
                        arm_pending_timeout = false;
                        let (_, lock_path) = pending_paths_for_tests(&dir, INSTANCES[inst]);
                        std::fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
                        let f = std::fs::OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(false)
                            .open(&lock_path)
                            .unwrap();
                        fs4::FileExt::try_lock(&f)
                            .expect("APPARATUS: the fixture must own the pending lock");
                        Some(f)
                    } else {
                        None
                    };
                    let before = PendingStore::new(dir.clone())
                        .read_all()
                        .await
                        .credits
                        .len();
                    let e0 = prop57_entry(&keys[0], bytes[0], INSTANCES[inst]);
                    let e1 = prop57_entry(&keys[1], bytes[1], INSTANCES[inst]);
                    let l = &ledgers[inst];
                    let (r0, r1) = prop57_bounded(30, "the concurrent paired appends", async {
                        tokio::join!(l.append_staged_entry(e0), l.append_staged_entry(e1))
                    })
                    .await;
                    *ledgers[inst].sync_probe().fail_path.lock().unwrap() = None;
                    if let Some(f) = holder {
                        fs4::FileExt::unlock(&f).unwrap();
                    }
                    // The loss, read from disk rather than inferred: an `Ok` entry whose
                    // pending record is not in the file lost it to the held lock, and an `Err`
                    // entry lost its ledger line. Both are slack until a repair re-appends.
                    let after = PendingStore::new(dir.clone())
                        .read_all()
                        .await
                        .credits
                        .len();
                    let mut ok_bytes = 0u64;
                    let mut ok_count = 0usize;
                    for (r, sz) in [(&r0, bytes[0]), (&r1, bytes[1])] {
                        match r {
                            Ok(()) => {
                                ok_count += 1;
                                ok_bytes = ok_bytes.saturating_add(sz);
                            }
                            Err(_) => lost = lost.saturating_add(sz),
                        }
                    }
                    if after < before + ok_count {
                        lost = lost.saturating_add(ok_bytes);
                    }
                    last_put_bytes[inst] = bytes[0] + bytes[1];
                }
                // A fold visit by either instance, over one shard, and the two bounds.
                4 => {
                    // Every shard, not one: `staged_authority_figure_uncached` is derivable only
                    // from a COMPLETE checkpoint set, so a one-shard visit on a four-shard
                    // population would leave the figure `None` and both bounds vacuous.
                    let outcome = prop57_bounded(
                        60,
                        "a fold visit",
                        run_staged_fold_over_shards(&dir, &ledgers[inst], INSTANCES[inst], &all),
                    )
                    .await;
                    if !matches!(outcome, FoldOutcome::Installed(_)) {
                        continue;
                    }
                    installs += 1;
                    let Some(figure) = staged_authority_figure_uncached(&dir).await else {
                        continue;
                    };
                    let f = figure.resident_staged_bytes;
                    let truth = prop57_truth(&published);
                    // Nothing in this schedule graduates or evicts, so § 16.6.1's over-count
                    // allowance is zero and the slack is the unrepaired loss plus whatever is
                    // mid-publish. See the deviation note.
                    let slack = lost.saturating_add(deferred_bytes);
                    assert!(
                        f <= truth.saturating_add(slack),
                        "install {installs} (step {step}): the figure must not exceed truth plus \
                         the bytes graduated or evicted since the last install (none here) plus \
                         the unrepaired loss and the mid-publish bytes: figure={f} truth={truth} \
                         lost={lost} mid_publish={deferred_bytes} ops={ops:?}"
                    );
                    assert!(
                        f.saturating_add(slack) >= truth,
                        "install {installs} (step {step}): the figure must not fall below truth \
                         minus the bytes of entries whose ledger line or pending record was lost \
                         and not yet repaired: figure={f} truth={truth} lost={lost} \
                         mid_publish={deferred_bytes} ops={ops:?}"
                    );
                }
                // Compaction, concurrent with nothing to retire: it still reads and locks every
                // ledger file the appender writes.
                5 => {
                    let _ = prop57_bounded(
                        60,
                        "a compaction pass",
                        ledgers[inst].retire_identities(&std::collections::HashSet::new()),
                    )
                    .await;
                }
                // Arm a group sync failure for the next PUT's shard file.
                6 => arm_sync_fail = Some(((b >> 4) & 3) as usize),
                // Arm a pending lock timeout, or crash.
                _ => {
                    // disabled this step because a torn group-write line left a
                    // `Pending_Credit` no cut could ever reach, counted on top of the `F` that
                    // already held the same range, so every crash schedule over-counted — and by
                    // one credit per torn line rather than one per PUT, which the earlier
                    // `truth ± lost` bound could not absorb either (`figure=53248 truth=36864
                    // lost=12288`, `ops=[248, 159, 232, 46, 44, 164, 221, 0]`). that
                    // in `ledger_repair::purge_unpaired_credits`, so the step is a parameter now
                    // rather than a disabled branch, and the two fixed cases below run with it on.
                    if !crash_enabled || (b >> 4) & 1 == 0 {
                        arm_pending_timeout = true;
                    } else {
                        // The crash: truncate this instance's largest shard file mid-line, then
                        // restart the instance, which re-seeds its sequence from `.ledger.mark`.
                        if prop57_truncate_mid_line(&dir, INSTANCES[inst]) {
                            lost = lost.saturating_add(last_put_bytes[inst]);
                            torn_lines += 1;
                        }
                        ledgers[inst] = WriteLedger::new(dir.clone(), INSTANCES[inst].to_string());
                    }
                }
            }
        }

        // Quiesced: publish every deferred rename, then one repair rotation per instance and one
        // fold over every shard.
        for (temp, meta) in deferred {
            if temp.exists() {
                std::fs::rename(&temp, &meta).unwrap();
            }
        }
        for (i, instance) in INSTANCES.iter().enumerate() {
            // Compaction FIRST, and it is not optional: a torn trailing line makes that shard's
            // coverage read refuse, so both the repair and the fold decline the shard while it is
            // there. Compaction is what quarantines it to `.ledger.torn` (loss mode L5), and
            // only then can the repair see what the shard is missing.
            let _ = prop57_bounded(
                90,
                "the quiesce compaction",
                ledgers[i].retire_identities(&std::collections::HashSet::new()),
            )
            .await;
            // A rotation, not a pass: the per-pass shard budget on a four-shard population is
            // one, so one pass could not reach a loss in another shard.
            for _ in 0..all.len() * 2 {
                let out = prop57_bounded(
                    60,
                    "a repair pass",
                    crate::ledger_repair::run_ledger_repair(&dir, &ledgers[i], instance),
                )
                .await;
                repairs.push(format!("{out:?}"));
            }
        }
        // EVERY instance, not just the first. A retained visit leaves its claim in place for
        // `CLAIM_LEASE_SECS` (900 s), so a shard whose mid-schedule visit retained — which a
        // torn ledger line guarantees — is `Busy` to the peer for the rest of the case, and a
        // one-instance fold would leave it with no checkpoint at all and the figure `None`.
        // Reproducer `ops=[0, 87, 124]`, now a fixed test.
        for (i, instance) in INSTANCES.iter().enumerate() {
            prop57_bounded(
                90,
                "the full fold",
                run_staged_fold_over_shards(&dir, &ledgers[i], instance, &all),
            )
            .await;
        }
        let truth = prop57_truth(&published);
        let figure = staged_authority_figure_uncached(&dir)
            .await
            .map(|f| f.resident_staged_bytes);
        // `None` is not a pass. A schedule that published nothing leaves no shard on disk for
        // the fold to observe, so there is no checkpoint set and no figure — and no credit
        // either. Anything published must leave a derivable figure, and that is asserted as the
        // figure rather than excused.
        match figure {
            // EVERY schedule is asserted to exact equality, a truncated one included. had
            // to give the crash branch `± lost` of slack because the torn-line over-count was
            // real and unfixed; it, so the slack is gone and a torn line that leaves
            // its stale credit behind is now a failure here rather than a disclosure.
            Some(f) => assert_eq!(
                f, truth,
                "after the schedule quiesces, one compaction, one repair rotation and one full \
                 fold, every credit must be counted exactly once: figure={f} truth={truth} \
                 lost={lost} mid_publish={deferred_bytes} installs={installs} ops={ops:?} \
                 repairs={repairs:?}"
            ),
            None => assert_eq!(
                truth, 0,
                "the figure must be derivable once anything has been published: truth={truth} \
                 lost={lost} installs={installs} ops={ops:?}"
            ),
        }
        let torn_file_in_fixture = prop57_any_torn_quarantine_file(&dir);
        (torn_lines, torn_file_in_fixture)
    }

    /// Whether any `*.ledger.torn` file exists under this fixture's write-ledger tree.
    ///
    /// Scoped to the `TempDir` the caller owns, which is what makes it evidence about this
    /// schedule rather than about the test binary.
    fn prop57_any_torn_quarantine_file(dir: &Path) -> bool {
        fn walk(p: &Path) -> bool {
            let Ok(entries) = std::fs::read_dir(p) else {
                return false;
            };
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    if walk(&path) {
                        return true;
                    }
                } else if path
                    .file_name()
                    .map(|n| n.to_string_lossy().ends_with(".ledger.torn"))
                    .unwrap_or(false)
                {
                    return true;
                }
            }
            false
        }
        walk(&dir.join("metadata").join("_write_ledger"))
    }

    /// the earlier reproducer, the earlier regression: a torn group-write line used to leave a
    /// `Pending_Credit` that **no cut could ever reach**, counted on top of the `F` that already
    /// held the same range. It now asserts the figure equals truth, which is the fix's red side.
    ///
    /// # The mechanism, by symbol
    ///
    /// `ops=[24, 159]`: `host:2` PUTs two paired entries, seq 1 into `p57b1` and seq 2 into
    /// `p57b0`, both acknowledged with durable pending records; then `p57b0`'s line is torn
    /// mid-line.
    ///
    /// 1. `WriteLedger::append` writes a new shard file's header with `max_seq_ever: 0` and never
    ///    raises it, so a file's high-water mark lives in its ENTRIES.
    /// 2. `WriteLedger::quarantine_torn_lines` moves the torn line to `.ledger.torn` and the
    ///    rewrite computes `ParsedLedgerFile::max_seq_ever` over the SURVIVING entries and that
    ///    header. Seq 2 is in neither, so it is unrecoverable.
    /// 3. The fold's cut for `p57b0` is `max(previous, read.max_seq_per_instance)`, so
    ///    `host:2`'s cut there stays 0 while its durable credit names seq 2.
    /// 4. `ledger_repair`'s sweep does restore `F`'s half — it finds the staged range on disk
    ///    with no entry and calls `WriteLedger::append_staged_range` — but under the REPAIRING
    ///    instance's identity and sequence. `host:1`'s new credit is filtered by the cut that
    ///    read its line; `host:2`'s stale credit is not.
    /// 5. `PendingReadOutcome::post_cut` therefore returns the stale credit forever, and
    ///    `staged_authority_figure_from` adds it to a `staged_byte_sum` that already counts the
    ///    `.meta`. § 16.6.1's counted-by-both.
    ///
    /// Direction and permanence: the figure is **over** truth by one credit's bytes, which makes
    /// staging eviction fire EARLY — it wastes write-cache space and is not client-visible, the
    /// safe direction. It is permanent: nothing moves `host:2`'s cut in that shard until `host:2`
    /// happens to write another object into that same shard.
    ///
    /// # The fix this asserts, and where it lives
    ///
    /// `ledger_repair::purge_unpaired_credits`: having re-appended a range its coverage read
    /// found with NO entry, the sweep surveys every instance's `.pending` file under that file's
    /// own lock, re-reads the shard's ledger **after** the survey, and removes the surveyed lines
    /// that read cannot pair by `(instance, seq)`. A credit is written only after its ledger
    /// entry is durable, so a credit the survey saw had its entry on disk before the pairing
    /// read: a live credit is provably paired and kept, and one appended after the survey is not
    /// a candidate at all. That function's doc comment carries the full argument.
    ///
    /// A failure that prints `figure` ABOVE `truth` by 8192 is the over-count back. A failure
    /// that prints it BELOW is the opposite defect — a live credit removed — and is the worse
    /// one: it under-states the staged figure, so `read_cache_size = total_size − R` is
    /// over-stated and read-tier eviction fires late. Read the figures, not the verdict.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_torn_group_write_line_leaves_no_stale_pending_credit() {
        let (figure, truth) = prop57_torn_line_figure(false).await;
        assert_eq!(
            figure,
            Some(truth),
            "after the torn line is quarantined, the range re-appended and the figure folded, \
             every credit must be counted exactly once. The defect pinned here reported \
             (Some(20480), 12288): figure above truth by the torn line's 8192 bytes, counted by \
             both the stale credit and the .meta"
        );
    }

    /// The same torn line, but the crashed instance comes back as a **new** `{hostname}:{pid}`,
    /// which is what a real crash produces.
    ///
    /// Why it is a separate case and not a flourish: the identity that holds the stale credit
    /// never appends, folds or GCs again, so every mechanism that could clear the credit by
    /// accident is gone. Its `staged_fold_cut` in that shard cannot move (nothing writes under
    /// it), `PendingStore::gc` only ever runs under the owning identity, and
    /// `WriteLedger::retire_identities` rewrites ledger files and never touches a `.pending`.
    /// The over-count was therefore permanent for the life of the volume and accumulated one
    /// credit per crash-torn line per shard — which is the severity the review re-derived
    /// and this case is the regression for.
    ///
    /// It also pins that the removal does not need the credit's owner to be alive: the repairing
    /// instance removes a dead peer's record, under that file's own lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dead_instances_torn_line_credit_is_removed_by_a_live_peer() {
        let (figure, truth) = prop57_torn_line_figure(true).await;
        assert_eq!(
            figure,
            Some(truth),
            "the crashed instance's new identity means nothing it owns ever runs again, so the \
             stale credit can only be removed by a peer's repair sweep"
        );
    }

    /// Task 58 (a). A repair visit that re-appends a range it found with no ledger entry leaves
    /// the authority figure equal to truth **with no fold in between**, so the pending term is
    /// what carries the repaired bytes.
    ///
    /// # Why "no fold in between" is the whole assertion
    ///
    /// A fold after the re-append would count the range's `.meta` in `F` and filter the fresh
    /// credit out by the cut it installs, so the figure would equal truth whether the re-append
    /// was paired or not. That is the vacuity trap: the pending term exists precisely for the
    /// window between a credit and the next fold of its shard, and only a reading taken inside
    /// that window can see it. So this fixture folds once to establish the cut, then re-appends,
    /// then reads — and never folds again.
    ///
    /// Red side: route the repair sweep through raw `append()` (`pair_pending = false`). The
    /// ledger line lands and nothing carries its bytes until a later fold, so the figure comes
    /// out one range short. Direction: an under-count, which over-states
    /// `read_cache_size = total_size − R` and fires read-tier eviction late.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_58a_a_repair_reappend_is_carried_by_the_pending_term() {
        const FOLDED: u64 = 4_096;
        const BYTES: u64 = 8_192;
        let instance = "host:1";
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let ledger = WriteLedger::new(dir.clone(), instance.to_string());
        let folded_key = prop57_population()[0].1[0].clone();
        let key = prop57_population()[0].1[1].clone();
        let shard = shard_of(&key)
            .map(|(bucket, l1)| Shard { bucket, l1 })
            .unwrap();
        let all: BTreeSet<Shard> = prop57_population().iter().map(|(s, _)| s.clone()).collect();
        // One healthy resident object first, so the shard exists on disk and the fold below has
        // something to observe. A fold over an EMPTY volume installs no checkpoint and leaves no
        // figure at all, so the window this test reads in would not exist.
        seed_staged_meta(
            &dir,
            &folded_key,
            FOLDED,
            SystemTime::now() - Duration::from_secs(5),
        )
        .await;
        prop57_bounded(
            30,
            "the resident object's paired append",
            ledger.append_staged_entry(prop57_entry(&folded_key, FOLDED, instance)),
        )
        .await
        .expect("the resident object's append must succeed");
        prop57_bounded(
            90,
            "the baseline fold",
            run_staged_fold_over_shards(&dir, &ledger, instance, &all),
        )
        .await;
        invalidate_authority_snapshot(&dir);
        let baseline = prop57_bounded(
            30,
            "the baseline figure",
            staged_authority_figure_uncached(&dir),
        )
        .await;
        assert_eq!(
            baseline.map(|f| f.resident_staged_bytes),
            Some(FOLDED),
            "FIXTURE: the baseline must be the one folded object's bytes, with its cut installed \
             and its credit retired, or the reading after the repair is not the repaired range's"
        );
        // The lost-entry state: a second staged `.meta` on disk that no ledger entry records.
        // This is what a torn group write, or a crash between the publish and the append, leaves.
        seed_staged_meta(
            &dir,
            &key,
            BYTES,
            SystemTime::now() - Duration::from_secs(5),
        )
        .await;
        invalidate_authority_snapshot(&dir);
        let before = prop57_bounded(
            30,
            "the pre-repair figure",
            staged_authority_figure_uncached(&dir),
        )
        .await
        .map(|f| f.resident_staged_bytes);
        assert_eq!(
            before,
            Some(FOLDED),
            "PRECONDITION: with no ledger entry and no fold since the second .meta appeared, the \
             figure must still read only the folded object's bytes. That under-count is what the \
             repair exists to close, and a reading of FOLDED + BYTES here means a fold ran and \
             this test can no longer see the pending term"
        );

        // One repair rotation. Its re-append goes through the paired path, so the credit's seq
        // is above this shard's cut and `post_cut` returns it.
        let outcome = prop57_bounded(
            60,
            "the repair pass",
            crate::ledger_repair::run_ledger_repair(&dir, &ledger, instance),
        )
        .await;
        assert!(
            matches!(&outcome, crate::ledger_repair::RepairOutcome::Swept(s) if s.ranges_reappended == 1),
            "FIXTURE: the sweep must have re-appended exactly the one missing range, got \
             {outcome:?}"
        );

        invalidate_authority_snapshot(&dir);
        let figure = prop57_bounded(
            30,
            "the post-repair figure",
            staged_authority_figure_uncached(&dir),
        )
        .await
        .map(|f| f.resident_staged_bytes);
        assert_eq!(
            figure,
            Some(FOLDED + BYTES),
            "with NO fold between the re-append and this read, the pending term must carry the \
             repaired range's {BYTES} bytes on top of the folded {FOLDED}. A figure of {FOLDED} \
             means the re-append wrote only the ledger half, so those bytes are counted by \
             neither term until a later fold, which is the forbidden outcome the pairing exists \
             to prevent, reintroduced by the repair meant to close it. shard={shard}"
        );
    }

    /// The **paired** credit the repair sweep just wrote for
    /// a range must survive `purge_unpaired_credits`, which runs in the same visit.
    ///
    /// The three red sides recorded could not separate "removes the stale credit" from
    /// "removes any credit naming the range", because every fixture they ran had only a stale
    /// one. This asserts the other side of that: after a visit that re-appended a range, a credit
    /// naming it is still in some instance's `.pending` file, and its seq is one the shard's
    /// ledger pairs.
    ///
    /// Red side: a lever that drops every surveyed credit naming the range, rather than only the
    /// unpaired ones. Direction if wrong: a live credit removed under-states the staged figure,
    /// so `read_cache_size` is over-stated and read-tier eviction fires late — the direction that
    /// fills a shared volume.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_repair_visits_own_paired_credit_survives_the_unpaired_purge() {
        const BYTES: u64 = 4_096;
        let instance = "host:1";
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let ledger = WriteLedger::new(dir.clone(), instance.to_string());
        let key = prop57_population()[0].1[0].clone();
        let (bucket, l1) = shard_of(&key).unwrap();

        seed_staged_meta(
            &dir,
            &key,
            BYTES,
            SystemTime::now() - Duration::from_secs(5),
        )
        .await;
        let outcome = prop57_bounded(
            60,
            "the repair pass",
            crate::ledger_repair::run_ledger_repair(&dir, &ledger, instance),
        )
        .await;
        assert!(
            matches!(&outcome, crate::ledger_repair::RepairOutcome::Swept(s) if s.ranges_reappended == 1),
            "FIXTURE: the sweep must have re-appended the missing range, got {outcome:?}"
        );

        let credits: Vec<(String, PendingCredit)> =
            PendingStore::new(dir.clone()).read_all().await.credits;
        let naming: Vec<&(String, PendingCredit)> = credits
            .iter()
            .filter(|(_, c)| c.range.as_ref().is_some_and(|r| r.cache_key == key))
            .collect();
        assert_eq!(
            naming.len(),
            1,
            "the visit's own paired credit must survive its own purge: a purge that drops every \
             credit naming the re-appended range instead of only the unpaired ones removes the \
             live one, which under-states the staged figure and fires read-tier eviction late. \
             credits={credits:?}"
        );
        // And it is paired: the shard's ledger holds that (instance, seq), which is the predicate
        // the purge evaluates.
        let read = ledger
            .read_shard_for_authority(&bucket, &l1, 0)
            .await
            .expect("the shard's ledger must be readable");
        assert!(
            read.is_complete(),
            "APPARATUS: the pairing read must be complete"
        );
        let (owner, credit) = naming[0];
        assert!(
            read.entries
                .iter()
                .any(|e| e.instance_id == *owner && e.seq == Some(credit.seq)),
            "the surviving credit must be one the shard's ledger pairs by (instance, seq): \
             owner={owner} seq={} entries={:?}",
            credit.seq,
            read.entries
        );
    }

    /// The read-tier consequence, which is the unsafe half: `read_cache_size` equals truth after
    /// the repair.
    ///
    /// `cache.rs` derives the read tier as `total_size.checked_sub(resident_staged_bytes)`,
    /// reporting 0 (and a WARN) when the subtraction inverts. An over-counted `R` therefore
    /// **under**-states the read tier, so read-cache eviction fires LATE — on a shared volume
    /// that is the direction that fills it, and it is the reason this defect was not the benign
    /// "staging eviction fires early" the first pass recorded.
    ///
    /// The fixture's total is truth plus one pretend read-tier object, so the expected read tier
    /// is that object's bytes exactly. With the defect, `R` was 8192 over truth and the figure
    /// came out 8192 short.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_read_tier_figure_equals_truth_after_a_torn_line_is_repaired() {
        const READ_TIER_BYTES: u64 = 65_536;
        let (figure, truth) = prop57_torn_line_figure(false).await;
        let resident_staged =
            figure.expect("APPARATUS: the fixture published .meta files, so a figure must derive");
        let total_size = truth + READ_TIER_BYTES;
        // Mirrors the `read_cache_size` derivation in `CacheManager::get_cache_size_stats`
        // (`src/cache.rs`), which is `total_size.checked_sub(resident_staged_bytes)` with the
        // `None` arm reporting 0 behind a WARN — `saturating_sub` for every input. Named by
        // symbol rather than by line because the line moves; a change to that derivation that
        // this line no longer mirrors is the failure mode, so the symbol is what to grep for.
        // There is no shared helper to call: the derivation is inline in that method, and
        // extracting one is outside this unit's write boundary.
        let read_cache_size = total_size.saturating_sub(resident_staged);
        assert_eq!(
            read_cache_size, READ_TIER_BYTES,
            "the read tier is total_size - R, so an over-counted R hides read-cached bytes from \
             read-tier eviction: R={resident_staged} truth={truth} total={total_size}"
        );
    }

    /// The `ops=[24, 159]` schedule by hand, returning `(figure, truth)` after the quiesce.
    ///
    /// Not `prop57_case`, because that function asserts; this one reports, which is what lets its
    /// callers assert on the figures rather than on a `should_panic`.
    ///
    /// With `restart_as_new_identity`, the instance whose line was torn comes back under a fresh
    /// `{hostname}:{pid}` — so the identity holding the stale credit never appends, folds or GCs
    /// again, and only its peers can clear it.
    async fn prop57_torn_line_figure(restart_as_new_identity: bool) -> (Option<u64>, u64) {
        const INSTANCES: [&str; 2] = ["host:1", "host:2"];
        let population = prop57_population();
        let all: BTreeSet<Shard> = population.iter().map(|(s, _)| s.clone()).collect();
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let mut ledgers: Vec<WriteLedger> = INSTANCES
            .iter()
            .map(|i| WriteLedger::new(dir.clone(), i.to_string()))
            .collect();
        let inst = 1usize;
        let bytes = [4096u64, 8192u64];
        let mut keys: Vec<String> = Vec::new();
        let mut published: Vec<PathBuf> = Vec::new();
        for (n, s) in [1usize, 0usize].iter().enumerate() {
            let k = population[*s].1[0].clone();
            seed_staged_meta(
                &dir,
                &k,
                bytes[n],
                SystemTime::now() - Duration::from_secs(5),
            )
            .await;
            published.push(
                crate::disk_cache::get_sharded_path(&dir.join("metadata"), &k, ".meta").unwrap(),
            );
            keys.push(k);
        }
        let e0 = prop57_entry(&keys[0], bytes[0], INSTANCES[inst]);
        let e1 = prop57_entry(&keys[1], bytes[1], INSTANCES[inst]);
        let l = &ledgers[inst];
        let (r0, r1) = prop57_bounded(30, "the paired appends", async {
            tokio::join!(l.append_staged_entry(e0), l.append_staged_entry(e1))
        })
        .await;
        assert!(
            r0.is_ok() && r1.is_ok(),
            "FIXTURE: the defect needs both lines acknowledged and both credits durable: \
             {r0:?} {r1:?}"
        );
        assert!(
            prop57_truncate_mid_line(&dir, INSTANCES[inst]),
            "FIXTURE: nothing was truncated, so no line was torn"
        );
        // Every restart mints a new `{hostname}:{pid}`, so the crashed identity is dead: it is
        // absent from `live` and nothing it owns is ever run again.
        let mut live: Vec<String> = INSTANCES.iter().map(|i| i.to_string()).collect();
        if restart_as_new_identity {
            live[inst] = format!("{}-restarted", INSTANCES[inst]);
        }
        ledgers[inst] = WriteLedger::new(dir.clone(), live[inst].clone());
        for (i, instance) in live.iter().enumerate() {
            let _ = prop57_bounded(
                90,
                "the quiesce compaction",
                ledgers[i].retire_identities(&std::collections::HashSet::new()),
            )
            .await;
            for _ in 0..all.len() * 2 {
                let _ = prop57_bounded(
                    60,
                    "a repair pass",
                    crate::ledger_repair::run_ledger_repair(&dir, &ledgers[i], instance),
                )
                .await;
            }
        }
        for (i, instance) in live.iter().enumerate() {
            prop57_bounded(
                90,
                "the full fold",
                run_staged_fold_over_shards(&dir, &ledgers[i], instance, &all),
            )
            .await;
        }
        let figure = staged_authority_figure_uncached(&dir)
            .await
            .map(|f| f.resident_staged_bytes);
        (figure, prop57_truth(&published))
    }

    /// The second fixed crash case, `ops=[0, 87, 124]`, run with the crash step ON.
    ///
    /// **This test pins an earlier review gap.** It was committed while
    /// `prop57_case`'s crash step was disabled, so op byte 87 armed a pending-lock timeout
    /// instead of tearing anything: the test truncated nothing and its doc comment described a
    /// state it could not reach. `crash_enabled` is now a parameter and this call passes `true`,
    /// so the schedule is the one the name claims.
    ///
    /// What it reaches that the hand-written case does not: both entries of one PUT land in ONE
    /// shard file and the truncation tears the second, so the torn file still holds a parseable
    /// entry and compaction's rewrite keeps a non-zero `max_seq_ever` and the surviving entry's
    /// seq. The step-124 fold visit then runs while the torn line is still there, exercising the
    /// retain-on-incomplete-read path before the quiesce repairs anything. Asserted to
    /// `prop57_case`'s exact post-quiesce equality.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_torn_line_in_a_shared_shard_file_leaves_a_derivable_figure() {
        let (torn_lines, torn_file) = prop57_case(&[0, 87, 124], true).await;
        eprintln!(
            "torn lines observed by ops=[0, 87, 124]: {torn_lines}, quarantine file: {torn_file}"
        );
        assert!(
            torn_lines > 0,
            "APPARATUS: this case exists to reach the torn-line state, so a run that truncates \
             nothing proves nothing about the figure. It passed vacuously for exactly this \
             reason before the crash step became a parameter"
        );
        assert!(
            torn_file,
            "APPARATUS: the fixture truncated {torn_lines} line(s) but no *.ledger.torn file \
             exists under this fixture's own _write_ledger tree, so compaction discarded the \
             torn line instead of quarantining it and the guard above reads only the fixture's \
             own arithmetic. Scoped to this TempDir deliberately: the process-global quarantine \
             counter would be satisfied by a concurrent test"
        );
    }

    /// Truncate the last line of this instance's largest shard ledger file mid-line, as an
    /// interrupted group write would leave it. Returns whether anything was truncated.
    fn prop57_truncate_mid_line(cache_dir: &Path, instance: &str) -> bool {
        let mut best: Option<(u64, PathBuf)> = None;
        for (shard, _) in prop57_population() {
            let p = WriteLedger::shard_ledger_path(cache_dir, &shard.bucket, &shard.l1, instance);
            let Ok(md) = std::fs::metadata(&p) else {
                continue;
            };
            let bigger = match &best {
                None => true,
                Some((len, _)) => md.len() > *len,
            };
            if bigger {
                best = Some((md.len(), p));
            }
        }
        let Some((len, path)) = best else {
            return false;
        };
        let Ok(body) = std::fs::read_to_string(&path) else {
            return false;
        };
        // Only a file with a line beyond its header can lose one, and the cut must keep a
        // partial line rather than a clean boundary: that is the state § 16.6.14's reissue
        // argument is about.
        let lines: Vec<&str> = body.lines().collect();
        if lines.len() < 2 {
            return false;
        }
        let keep = body.len() - (lines[lines.len() - 1].len() / 2).max(1) - 1;
        if keep as u64 >= len || keep == 0 {
            return false;
        }
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(keep as u64).unwrap();
        true
    }

    /// A key other than `key` in `key`'s shard.
    fn key_in_shard_of_for_tests(key: &str, tag: &str) -> String {
        let target = shard_of(key).unwrap();
        (0..)
            .map(|i| format!("bucket/{tag}-{i}.bin"))
            .find(|k| k != key && shard_of(k).unwrap() == target)
            .unwrap()
    }

    /// K concurrent budget callers in one cold window derive the count **once**.
    ///
    /// # What this measures, and why the stall is part of the fixture rather than noise
    ///
    /// The seam is [`shard_checkpoint_reads_for_tests`], which tallies traversals of the
    /// checkpoint set — the only thing that can distinguish one derivation from k, since every
    /// derivation produces the identical figure and no published gauge differs.
    ///
    /// Without `COUNT_DERIVATION_STALL` this assertion would be **vacuous**: eight callers
    /// issued from one test can complete one after another, each finding the 5 s memo fresh, so
    /// the unchanged code also traverses exactly once and the test passes with the defect
    /// present. The stall (200 ms, against a derivation that is microseconds on a `TempDir`)
    /// forces all eight into the cold window together, which is the state a cold fleet volume
    /// puts them in for real. Measured red on unchanged code at `reads == 8`.
    ///
    /// A failure implicates [`budget_derivation`]'s single-flight map, not the memo: an
    /// unkeyed or per-caller derivation reports `k`, and a broken join reports `0` verdicts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_budget_callers_derive_the_count_once() {
        const CALLERS: usize = 8;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        seed_shard_figure(&dir, &Shard::new("bucket", "01"), 4_096, 7, 5);
        COUNT_DERIVATION_STALL
            .lock()
            .unwrap()
            .push((dir.clone(), std::time::Duration::from_millis(200)));
        invalidate_budget_snapshot(&dir);
        let before = shard_checkpoint_reads_for_tests(&dir);

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..CALLERS {
            let d = dir.clone();
            set.spawn(async move { budget_verdict(&d).await });
        }
        let mut admits = 0usize;
        while let Some(r) = set.join_next().await {
            assert_eq!(
                r.unwrap(),
                BudgetVerdict::Admit,
                "seven ranges is far under the budget, so every caller must admit — a decline \
                 here means the joined figure is not the derived one"
            );
            admits += 1;
        }
        assert_eq!(admits, CALLERS, "every caller must get a verdict");

        let reads = shard_checkpoint_reads_for_tests(&dir) - before;
        assert_eq!(
            reads, 1,
            "{CALLERS} concurrent callers against one cold memo must share ONE derivation; \
             {reads} traversals means each caller ran the whole derivation and they serialised \
             on the same .pending flocks"
        );
        COUNT_DERIVATION_STALL
            .lock()
            .unwrap()
            .retain(|(p, _)| p != &dir);
    }

    fn seed_shard_figure(dir: &Path, shard: &Shard, bytes: u64, ranges: u64, age_secs: u64) {
        let mut c = StagedShardCheckpoint::empty(shard);
        c.staged_byte_sum = bytes;
        c.staged_range_count = ranges;
        c.staged_scanned_at = now_nanos().saturating_sub(age_secs * 1_000_000_000);
        c.staged_scanned_by = "host:seed".to_string();
        c.install(dir).unwrap();
    }

    /// A peer's live claim makes a shard `Busy`; an expired one does not; and this instance's
    /// own claim never blocks itself.
    ///
    /// A failure implicates `claim_shard`'s lease comparison (`ShardClaim::is_live_at`) and
    /// its `instance_id != self` guard. The assertion reads the outcome of the claim call —
    /// the value the fold's `continue` branches on — rather than the claim file's contents.
    #[tokio::test]
    async fn a_live_peer_claim_makes_a_shard_busy_and_an_expired_one_does_not() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "3f");

        // Unclaimed.
        assert_eq!(
            claim_shard(dir, &shard, "host:1").await,
            ClaimOutcome::Claimed
        );
        // Re-claiming our own is not Busy: a claim is not a mutex, and an instance that
        // crashed and restarted under the same identity must not lock itself out for the
        // lease.
        assert_eq!(
            claim_shard(dir, &shard, "host:1").await,
            ClaimOutcome::Claimed
        );

        // A peer's live claim.
        let path = ShardClaim::path(dir, &shard);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let live = ShardClaim {
            instance_id: "host:2".to_string(),
            claimed_at: now,
            lease_secs: CLAIM_LEASE_SECS,
        };
        std::fs::write(&path, serde_json::to_string(&live).unwrap()).unwrap();
        assert!(
            live.is_live_at(now),
            "the input: the fixture's claim really is live at the instant asserted below"
        );
        assert_eq!(claim_shard(dir, &shard, "host:1").await, ClaimOutcome::Busy);

        // The same peer, the same file, one second past the lease. The ONLY thing that
        // differs is `claimed_at`, which is what makes this a test of the lease rather than
        // of the identity check.
        let expired = ShardClaim {
            claimed_at: now.saturating_sub(CLAIM_LEASE_SECS + 1),
            ..live.clone()
        };
        std::fs::write(&path, serde_json::to_string(&expired).unwrap()).unwrap();
        assert!(!expired.is_live_at(now), "the input: it really has expired");
        assert_eq!(
            claim_shard(dir, &shard, "host:1").await,
            ClaimOutcome::Claimed,
            "a crashed instance's claim must expire on its own — § 16.2 chose a lease over a \
             lock precisely so a dead holder self-heals"
        );
    }

    /// A pass skips a shard a peer holds, and installs the rest.
    ///
    /// A failure implicates the `ClaimOutcome::Busy => continue` arm in
    /// `run_staged_fold_with_cap`, and the assertion reads the pass summary's `busy` and
    /// `installed` lists plus the on-disk figure — so a pass that claimed to skip while
    /// folding anyway would fail on the figure.
    #[tokio::test]
    async fn a_shard_claimed_by_a_peer_is_skipped_and_the_others_install() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        publish_staged_range(dir, &ledger, "bucket/left.bin", 4096).await;
        publish_staged_range(dir, &ledger, "bucket/right.bin", 8192).await;
        let (lb, ll) = shard_of("bucket/left.bin").unwrap();
        let left = Shard::new(lb, ll);
        let (rb, rl) = shard_of("bucket/right.bin").unwrap();
        let right = Shard::new(rb, rl);
        assert_ne!(left, right, "the fixture must span two shards");

        // The peer must hold whichever shard the pass reaches FIRST, or the pass installs the
        // other one, hits its derived budget of `ceil(2/12) = 1`, and stops without ever
        // attempting the claimed shard — `busy` would be empty and the test would assert nothing
        // about claiming. Neither shard has a checkpoint, so both sort at `staged_scanned_at = 0`
        // and the fold's tie-break is the `Shard` ordering itself; this reproduces that key
        // rather than guessing which key hashes lower.
        let (claimed, unclaimed) = if left <= right {
            (left.clone(), right.clone())
        } else {
            (right.clone(), left.clone())
        };

        // A peer takes the first-visited shard.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claim_path = ShardClaim::path(dir, &claimed);
        std::fs::create_dir_all(claim_path.parent().unwrap()).unwrap();
        std::fs::write(
            &claim_path,
            serde_json::to_string(&ShardClaim {
                instance_id: "host:2".to_string(),
                claimed_at: now,
                lease_secs: CLAIM_LEASE_SECS,
            })
            .unwrap(),
        )
        .unwrap();

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        let summary = installed(&outcome);
        assert_eq!(
            summary.busy,
            vec![claimed.clone()],
            "the claimed shard must have been ATTEMPTED and refused — a busy shard does not \
             consume the per-pass budget, which is what lets the pass go on to the next one"
        );
        assert_eq!(
            summary
                .installed
                .iter()
                .map(|c| c.shard())
                .collect::<Vec<_>>(),
            vec![unclaimed.clone()]
        );
        let set = read_shard_checkpoints(dir);
        assert!(
            set.get(&claimed).is_none(),
            "the claimed shard must have no figure from this instance"
        );
        let expected = if unclaimed == left { 4096 } else { 8192 };
        assert_eq!(
            set.get(&unclaimed).unwrap().staged_byte_sum,
            expected,
            "and the unclaimed one must have its own figure, not the sum of both"
        );
        assert_eq!(
            set.staged_byte_sum(),
            expected,
            "so the global sum is a partial ROTATION — mixed age, complete membership over \
             the shards that have figures. It is not a partial shard figure."
        );
    }

    /// A shard whose install fails keeps its own previous figure while the shards visited
    /// before it keep their new ones.
    ///
    /// # The construction, and why it needs two passes
    ///
    /// Pass 1 folds both shards, so both have a figure to retain. Then both objects grow, so
    /// a successful install would change both figures — without that divergence a retain and
    /// an install are indistinguishable. Then **one shard's checkpoint directory is made
    /// unwritable**, which fails `install` for that shard alone. Shards are visited in
    /// staleness order, so the pass reaches one, installs it, reaches the other and fails.
    ///
    /// A failure implicates the `checkpoint.install(cache_dir)` error arm in
    /// `run_staged_fold_with_cap`. The assertions read the two shards' on-disk
    /// `staged_byte_sum` separately — never their sum — because a sum cannot distinguish
    /// "one retained and one installed" from "both did something else that happens to add up".
    #[tokio::test]
    #[cfg(unix)]
    async fn a_shard_whose_install_fails_retains_its_own_figure_while_others_install() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // This test needs ONE pass to visit TWO shards, and the per-pass budget is
        // `ceil(observed / passes_per_window)`, so it needs `passes_per_window + 1` observed
        // shards. `passes_per_window - 1` filler shards supply that. They carry a fresh `staged_scanned_at` and the two real shards have no
        // checkpoint at all — which sorts as age 0, i.e. stalest — so the pass reaches the real
        // ones first. The fillers are re-stamped before the second pass for the same reason.
        //
        // This is fixture machinery in service of a real property: the budget is derived, so a
        // test about what happens on the *second* shard of a pass has to earn a pass that has a
        // second shard.
        let passes_per_window = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS) as usize;
        let fillers: Vec<Shard> = (0..passes_per_window - 1)
            .map(|i| Shard::new(format!("filler{}", i / 256), format!("{:02x}", i % 256)))
            .collect();
        for f in &fillers {
            seed_shard_figure(dir, f, 0, 0, 0);
        }

        // Two objects in two shards, folded once so both have a previous figure.
        //
        // **Two BUCKETS, not two L1s in one bucket**, and that is a fixture correction rather
        // than a preference: `checkpoints/{bucket}/{XX}.json` puts every L1 of one bucket in
        // one directory, so making that directory unwritable fails the install for *both*
        // shards and the pass retains on the first one it visits. The first version of this
        // test did exactly that and asserted the wrong shard had retained, which is how the
        // shared parent was found.
        publish_staged_range(dir, &ledger, "bucket-a/alpha.bin", 4096).await;
        publish_staged_range(dir, &ledger, "bucket-b/beta.bin", 8192).await;
        let first = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(
            installed(&first).installed.len(),
            2,
            "the input: passes_per_window + 1 observed shards give a per-pass budget of 2, and \
             the two shards with no figure are the stalest, so this pass must have taken exactly \
             them"
        );
        assert_eq!(
            shards_per_pass(passes_per_window + 1),
            2,
            "stated directly, so a change to the derivation fails here naming the cause rather \
             than showing up as a confusing single-install pass above"
        );
        let (ab, al) = shard_of("bucket-a/alpha.bin").unwrap();
        let alpha = Shard::new(ab, al);
        let (bb, bl) = shard_of("bucket-b/beta.bin").unwrap();
        let beta = Shard::new(bb, bl);
        assert_ne!(
            StagedShardCheckpoint::path(dir, &alpha).parent(),
            StagedShardCheckpoint::path(dir, &beta).parent(),
            "the two shards must live in different directories, or one chmod fails both installs"
        );
        let before = read_shard_checkpoints(dir);
        assert_eq!(before.get(&alpha).unwrap().staged_byte_sum, 4096);
        assert_eq!(before.get(&beta).unwrap().staged_byte_sum, 8192);

        // The divergence: both objects grow, so a successful install cannot leave either
        // figure where it was.
        publish_staged_range(dir, &ledger, "bucket-a/alpha.bin", 40_960).await;
        publish_staged_range(dir, &ledger, "bucket-b/beta.bin", 81_920).await;

        // Re-stamp the fillers so they are FRESHER than alpha and beta, which pass 1 just folded.
        // Without this the fillers are the stalest and pass 2 visits them instead, and the test
        // would report both real shards retaining their figures — a pass, for entirely the wrong
        // reason.
        for f in &fillers {
            seed_shard_figure(dir, f, 0, 0, 0);
        }

        // Visit order, computed with the fold's OWN ordering key — `(staged_scanned_at,
        // shard)`, staleness ascending — rather than assumed, and rather than approximated
        // with a comparison that behaves differently on a nanosecond tie.
        let mut order = [alpha.clone(), beta.clone()];
        order.sort_by_key(|s| {
            (
                before.get(s).map(|c| c.staged_scanned_at).unwrap_or(0),
                s.clone(),
            )
        });
        let (first_visited, second_visited) = (order[0].clone(), order[1].clone());

        // Make the second-visited shard's directory unwritable, so its install fails and the
        // first-visited shard's has already succeeded.
        let doomed_dir = StagedShardCheckpoint::path(dir, &second_visited)
            .parent()
            .unwrap()
            .to_path_buf();
        let restore = std::fs::metadata(&doomed_dir).unwrap().permissions();
        std::fs::set_permissions(&doomed_dir, std::fs::Permissions::from_mode(0o500)).unwrap();

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        std::fs::set_permissions(&doomed_dir, restore).unwrap();

        assert_eq!(
            outcome.retain_reason(),
            Some(FoldRetainReason::CheckpointWriteFailed),
            "the pass must report the retain so the cadence is not consumed"
        );
        assert_eq!(
            outcome.summary().retained.first().map(|(s, _)| s.clone()),
            Some(second_visited.clone()),
            "and must name WHICH shard retained"
        );
        assert_eq!(
            outcome
                .summary()
                .installed
                .iter()
                .map(|c| c.shard())
                .collect::<Vec<_>>(),
            vec![first_visited.clone()],
            "the shard visited before the failure must still have installed — complete-or-\
             retain is per shard, not per pass"
        );

        let after = read_shard_checkpoints(dir);
        assert_eq!(
            after.get(&second_visited).unwrap().staged_byte_sum,
            before.get(&second_visited).unwrap().staged_byte_sum,
            "the failing shard retains its OWN previous figure"
        );
        assert_eq!(
            after.get(&second_visited).unwrap().staged_fold_cut,
            before.get(&second_visited).unwrap().staged_fold_cut,
            "and its own previous cut, with it — one document, one write"
        );
        assert_ne!(
            after.get(&first_visited).unwrap().staged_byte_sum,
            before.get(&first_visited).unwrap().staged_byte_sum,
            "while the other shard's figure DID move; if this passed with both unchanged the \
             fixture would be proving nothing about granularity"
        );
        assert!(
            after.is_complete(),
            "and the global read is still complete: a retained shard has a figure, just an \
             older one"
        );
    }

    /// A half-written shard document never enters the global sum, and an unreadable one makes
    /// the sum `null` rather than short.
    ///
    /// # Why both halves are in one test
    ///
    /// They are the two ways a partial figure could reach the sum, and they have opposite
    /// correct answers — the temp file is *ignored*, the unreadable document is *fatal to the
    /// sum*. Asserting only one would leave the other's branch to be argued about.
    ///
    /// A failure implicates `read_shard_checkpoints`' `.json` filename filter (for the first
    /// half) and its `files_unreadable` accounting plus
    /// `collect_staged_fold_gauges`' `!checkpoints.is_complete()` early return (for the
    /// second). The assertions read `staged_fold_byte_sum` — the published figure — and
    /// `ShardCheckpointSet::is_complete`, which is the value that early return branches on.
    #[tokio::test]
    #[cfg(unix)]
    async fn a_partial_shard_figure_never_enters_the_global_sum() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        publish_staged_range(dir, &ledger, "bucket/real.bin", 4096).await;
        run_staged_fold(dir, &ledger, "host:1").await;
        let (rb, rl) = shard_of("bucket/real.bin").unwrap();
        let real = Shard::new(rb, rl);
        assert_eq!(read_shard_checkpoints(dir).staged_byte_sum(), 4096);

        // ---- Half 1: an in-flight install's temp file, holding an absurd figure.
        //
        // This is exactly what a document looks like between `File::create` and `rename`, and
        // it is the state § 16.6.4 means by "a partial shard figure". An enumerator matching
        // on "contains .json" rather than "ends with .json" picks it up, and the sum silently
        // gains 10 GB.
        let real_path = StagedShardCheckpoint::path(dir, &real);
        let tmp_path = real_path.with_file_name(format!(
            "{}.tmp.somehost.999",
            real_path.file_name().unwrap().to_string_lossy()
        ));
        let mut half = StagedShardCheckpoint::empty(&real);
        half.staged_byte_sum = 10_000_000_000;
        half.staged_range_count = 999_999;
        std::fs::write(&tmp_path, serde_json::to_string(&half).unwrap()).unwrap();
        assert!(
            tmp_path.to_string_lossy().contains(".json"),
            "the input: the temp name really does contain `.json`, so a substring match would \
             pick it up"
        );

        let set = read_shard_checkpoints(dir);
        assert_eq!(
            set.staged_byte_sum(),
            4096,
            "the temp file must not be summed: it is a figure mid-write, and `rename` is what \
             makes a document readable"
        );
        assert!(
            set.is_complete(),
            "and it must not be counted as an unreadable document either — it is not a \
             document at all"
        );
        let gauges = gauges_for_tests(dir, 0).await;
        assert_eq!(gauges.staged_fold_byte_sum, Some(4096));
        std::fs::remove_file(&tmp_path).unwrap();

        // ---- Half 2: a second shard whose document cannot be read.
        seed_shard_figure(dir, &Shard::new("bucket", "ff"), 50_000, 7, 10);
        assert_eq!(
            read_shard_checkpoints(dir).staged_byte_sum(),
            4096 + 50_000,
            "the input: both shards are readable and summed, so the drop below is caused by \
             the permission change and nothing else"
        );
        let second_path = StagedShardCheckpoint::path(dir, &Shard::new("bucket", "ff"));
        let restore = std::fs::metadata(&second_path).unwrap().permissions();
        std::fs::set_permissions(&second_path, std::fs::Permissions::from_mode(0o000)).unwrap();

        let set = read_shard_checkpoints(dir);
        assert!(
            !set.is_complete(),
            "an unreadable shard document makes the SET incomplete"
        );
        let gauges = gauges_for_tests(dir, 0).await;
        assert_eq!(
            gauges.staged_fold_byte_sum, None,
            "so the published figure is null rather than 4096 — a sum missing a shard is \
             indistinguishable from a smaller cache, which is the failure mode that makes an \
             under-stated staged figure look healthy"
        );
        assert_ne!(
            gauges.staged_fold_byte_sum,
            Some(4096),
            "and specifically NOT the readable shard's figure alone"
        );

        std::fs::set_permissions(&second_path, restore).unwrap();
        assert_eq!(
            gauges_for_tests(dir, 0).await.staged_fold_byte_sum,
            Some(4096 + 50_000),
            "the positive control: with both readable the sum is the sum"
        );
    }

    /// The two ages are different figures, and the criterion is set on the stalest.
    ///
    /// Two shards, one folded recently and one long ago. `staged_authority_age_secs` reports
    /// the freshest ("is the fold getting anything done") and
    /// `staged_shard_authority_age_secs` the stalest ("how old is the authority"). A single
    /// figure cannot answer both, and I2's definition answered only the first — which under
    /// partitioning reads healthiest exactly when a fold is spinning on one shard and
    /// starving the rest.
    ///
    /// The scrape path reads the checkpoint set **once**, not once per gauge collector.
    ///
    /// # Why this needs a call counter rather than an assertion on a figure
    ///
    /// One traversal and two traversals produce **identical** gauge values — that is exactly why
    /// the doubled read survived unnoticed from 2026-09-20, and it is why no published field can
    /// be the discriminator here. The only observable difference is how many times
    /// [`read_shard_checkpoints`] ran, so that is what is asserted, through
    /// [`shard_checkpoint_reads_for_tests`].
    ///
    /// # What this pins
    ///
    /// `read_shard_checkpoints` is a `read_dir` of `checkpoints/`, a `read_dir` per bucket, and an
    /// open + read + JSON parse per shard document — order of hundreds of small files on NFS/EFS.
    /// Two collectors on the `/metrics` path each need the set:
    /// [`collect_staged_fold_gauges`] and [`crate::st_size_fold::collect_st_size_fold_gauges`].
    /// Both now take it as an argument so `MetricsCollector::collect_metrics` traverses once. A
    /// regression that gives either collector its own read is invisible in every figure and shows
    /// up only here.
    ///
    /// The body of `/metrics` is one `SystemMetrics` serialised whole, so a collection that
    /// outruns the scraper's deadline loses **every** series rather than degrading — which is what
    /// makes the traversal count worth a test.
    ///
    /// # RED SIDE, taken 2026-09-21
    ///
    /// With `collect_st_size_fold_gauges`' body reverted to `oldest_shard_scan_age_secs(cache_dir)`
    /// (its own read), this asserted 2 against an expected 1 and failed. Restored: 1.
    #[tokio::test]
    async fn the_scrape_path_reads_the_checkpoint_set_once_per_collection() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // Both consumers must find real input, so each shard carries BOTH producers' keys: the
        // staged fold's own fields and the `st_size` pass's `other["scanned_at"]`. Seeding only
        // the former would make the st_size assertion below pass on the `None` sentinel, which
        // is precisely the vacuous shape this test exists to avoid.
        for (l1, bytes, age_secs) in [("aa", 1_000u64, 10u64), ("bb", 2_000, 20)] {
            let shard = Shard::new("bucket", l1);
            let mut c = StagedShardCheckpoint::empty(&shard);
            c.staged_byte_sum = bytes;
            c.staged_range_count = 3;
            c.staged_scanned_at = now_nanos().saturating_sub(age_secs * 1_000_000_000);
            c.staged_scanned_by = "host:seed".to_string();
            c.other.insert(
                "scanned_at".to_string(),
                serde_json::Value::from(now_nanos().saturating_sub(age_secs * 1_000_000_000)),
            );
            c.install(dir).unwrap();
        }

        // The seeding above installs documents, which reads the set as a side effect. Baseline
        // from here so this measures the collection and nothing before it.
        let baseline = shard_checkpoint_reads_for_tests(dir);

        // Exactly what `MetricsCollector::collect_metrics` does: ONE traversal, shared.
        let (pending, checkpoints) = read_gauge_inputs(dir).await;
        let staged = collect_staged_fold_gauges(
            dir,
            0,
            &pending,
            &checkpoints,
            crate::signed_put_handler::in_progress_part_bytes(dir),
        )
        .await;
        let st_size = crate::st_size_fold::collect_st_size_fold_gauges(&checkpoints);

        let reads = shard_checkpoint_reads_for_tests(dir) - baseline;
        assert_eq!(
            reads, 1,
            "one collection must traverse checkpoints/ ONCE. {reads} traversals means a gauge \
             collector has gone back to reading the set itself, which is invisible in every \
             published figure and doubles the scrape's shared-storage cost."
        );

        // Both collectors must still have produced real figures from the shared set — a read
        // count of 1 is also what a collector that silently returned its default would show,
        // so the count alone cannot tell de-duplication from breakage.
        assert_eq!(
            staged.staged_fold_byte_sum,
            Some(3_000),
            "the staged gauges must be derived from the SHARED set, not defaulted"
        );
        assert!(
            st_size
                .st_size_fold_oldest_shard_scan_age_secs
                .is_some_and(|age| age >= 20),
            "the stalest scan age must be derived from the SHARED set, not defaulted: {:?}",
            st_size.st_size_fold_oldest_shard_scan_age_secs
        );
    }

    /// A failure implicates `ShardCheckpointSet::stalest_age_secs` /
    /// `freshest_age_secs` and their two assignments in `collect_staged_fold_gauges`.
    #[tokio::test]
    async fn the_two_ages_are_published_separately_and_the_criterion_uses_the_stalest() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        seed_shard_figure(dir, &Shard::new("bucket", "aa"), 1_000, 3, 10);
        seed_shard_figure(dir, &Shard::new("bucket", "bb"), 2_000, 5, 7_200);

        let g = gauges_for_tests(dir, 0).await;
        assert_eq!(g.staged_fold_shards, 2);
        let fresh = g.staged_authority_age_secs.unwrap();
        let stale = g.staged_shard_authority_age_secs.unwrap();
        assert!((10..60).contains(&fresh), "freshest shard's age: {fresh}");
        assert!(
            (7_200..7_260).contains(&stale),
            "stalest shard's age: {stale}"
        );
        assert!(
            stale > fresh,
            "the two must be different figures on a fleet with any rotation at all — if they \
             were equal the criterion could be read off either, and this whole distinction \
             would be decoration: fresh={fresh} stale={stale}"
        );
        assert!(
            stale > 3_600,
            "and it is the stalest that breaches § 16.6.4's 3,600 s bound here; a gauge \
             reporting {fresh} would show this volume as comfortably inside it"
        );
    }

    /// The staged fold carries the `st_size` fold's fields through untouched.
    ///
    /// § 3.2's checkpoint has two producers. The staged fold owns four fields and must not
    /// touch the other nine — and a plain serde struct naming only its own would drop them
    /// silently on every install, with the document still parsing afterwards.
    ///
    /// **This is a NO-REGRESSION GUARD, not evidence for any change.** It passed before
    /// [`crate::st_size_fold`] existed and it passes after, because it asserts a property of
    /// *this* fold's install and not of the other producer's. Read a pass here as "the staged
    /// fold still does not drop the other producer's fields", never as "the two-producer
    /// contract holds" — the other direction is
    /// `tests/st_size_fold_test.rs::the_folds_install_preserves_the_staged_folds_own_fields`,
    /// and both are needed because each producer rewrites the whole document.
    ///
    /// EXTENDED 2026-09-19, when the other producer landed. The new arm runs a **real**
    /// `st_size` fold between two staged passes and asserts the values that fold wrote survive
    /// the second pass, plus a field **neither** producer writes (`tail_floor`, § 4's). That is
    /// worth having — the original arm only ever checked hand-seeded values, so a producer that
    /// re-derived its own keys and dropped everything else would have passed it.
    ///
    /// **It is NOT evidence for `install_preserving_foreign_fields`' locked re-read, and the
    /// attempt to make it so produced a GREEN RED SIDE that was measured and discarded.**
    /// Replacing the locked re-read with the pass's own start-of-pass snapshot leaves this test
    /// passing, because `run_staged_fold` takes that snapshot *inside* the pass: a second,
    /// sequential call therefore snapshots a document the `st_size` fold has **already**
    /// rewritten, and the stale-snapshot path and the re-read path return the same map. The
    /// window the lock closes needs a write landing *between* one pass's snapshot and its own
    /// install, which no single-threaded sequential test can construct, and which a
    /// `tokio::join!` could only construct nondeterministically. So the re-read's correctness
    /// rests on the argument in its own doc comment, not on an assertion here — stated rather
    /// than implied, because an assertion that cannot fail is worse than no assertion.
    ///
    /// A failure implicates `StagedShardCheckpoint::other`'s `#[serde(flatten)]` and
    /// `install_preserving_foreign_fields`' locked re-read.
    #[tokio::test]
    async fn a_staged_install_preserves_the_st_size_folds_fields() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        publish_staged_range(dir, &ledger, "bucket/shared.bin", 4096).await;
        let (sb, sl) = shard_of("bucket/shared.bin").unwrap();
        let shard = Shard::new(sb, sl);

        // A document as the `st_size` fold would leave it: its own fields, no staged figure.
        let mut seeded = StagedShardCheckpoint::empty(&shard);
        for (k, v) in [
            ("entry_count", serde_json::json!(41_233)),
            ("range_count", serde_json::json!(98_211)),
            ("byte_sum", serde_json::json!(91_234_567_890u64)),
            ("histogram", serde_json::json!([0, 12, 4001])),
            ("tail_floor", serde_json::json!(1_048_576)),
            (
                "scanned_at",
                serde_json::json!(1_756_400_000_123_456_789u64),
            ),
            ("scanned_by", serde_json::json!("host:9999")),
        ] {
            seeded.other.insert(k.to_string(), v);
        }
        seeded.install(dir).unwrap();

        run_staged_fold(dir, &ledger, "host:1").await;

        let after = read_shard_checkpoints(dir).get(&shard).unwrap().clone();
        assert_eq!(
            after.staged_byte_sum, 4096,
            "the staged figure was installed"
        );
        assert_eq!(
            after.other, seeded.other,
            "and every field the OTHER producer owns survived it, byte for byte"
        );
        // Named individually too, because a reader should be able to see that `scanned_at`
        // specifically was not overwritten by `staged_scanned_at` — the two ages § 3.7
        // insists are separate.
        assert_eq!(
            after.other.get("scanned_at"),
            Some(&serde_json::json!(1_756_400_000_123_456_789u64)),
            "the st_size fold's own age must not be stamped by the staged fold"
        );
        assert_ne!(
            after.staged_scanned_at, 1_756_400_000_123_456_789u64,
            "and the staged age must be this pass's, not a copy of the other one"
        );

        // EXTENSION: the same assertion against fields written by the REAL other producer
        // rather than by this test's own hand-seeded map.
        //
        // **This arm does NOT discriminate the locked re-read** — see the doc comment above,
        // which records the measurement. The two passes are sequential, so the second pass's
        // own start-of-pass snapshot already contains what the fold below wrote, and the
        // stale-snapshot shape passes this identically. What it DOES pin is that a real
        // `st_size` fold's values, and a field neither producer writes, survive a staged
        // install.
        crate::st_size_fold::run_st_size_fold(dir, "host:st", std::time::Duration::ZERO).await;
        let folded = read_shard_checkpoints(dir).get(&shard).unwrap().clone();
        let folded_scanned_at = folded
            .other
            .get("scanned_at")
            .and_then(|v| v.as_u64())
            .expect("the st_size fold installed its own age");
        assert_ne!(
            folded_scanned_at, 1_756_400_000_123_456_789u64,
            "precondition: the st_size fold must actually have REPLACED the seeded value, or \
             the arm below cannot tell preservation from a stale snapshot"
        );

        // A second staged pass. Its own start-of-pass snapshot predates the fold above.
        run_staged_fold(dir, &ledger, "host:1").await;
        let after2 = read_shard_checkpoints(dir).get(&shard).unwrap().clone();
        assert_eq!(
            after2.other.get("scanned_at").and_then(|v| v.as_u64()),
            Some(folded_scanned_at),
            "the staged fold must preserve the value the st_size fold wrote, not revert to the \
             one its own earlier read saw"
        );
        assert_eq!(
            after2.other.get("scanned_by"),
            Some(&serde_json::json!("host:st"))
        );
        assert_eq!(
            after2.other.get("tail_entries"),
            None,
            "the seeded document had no tail_entries and the st_size fold writes none, so it \
             must be absent — a field neither producer writes must not appear"
        );
        assert_eq!(
            after2.other.get("tail_floor"),
            Some(&serde_json::json!(1_048_576)),
            "and a field NEITHER producer writes, seeded above, survives both of them"
        );
    }

    /// A pending record whose shard this pass did not visit survives the GC.
    ///
    /// # Why this is a correctness test and not a housekeeping one
    ///
    /// I2's GC dropped every record at or below one scalar cut. Under partitioning that cut
    /// belongs to specific shards, so the same code would reclaim a record in a shard no
    /// figure has counted — and the record is the only thing that remembers those bytes until
    /// a fold reads the `.meta`. Deleting it turns § 16.6.1's transient counted-by-neither
    /// into a permanent one.
    ///
    /// The construction is the divergence: one shard is visited (so its cut advances past its
    /// record) while another is held by a peer (so its cut does not exist at all) — with both
    /// records **at or below the same global seq**, which is what a scalar cut would compare
    /// against. A scalar implementation drops both; a per-shard one drops one.
    ///
    /// A failure implicates `PendingStore::gc`'s per-shard `cuts.get(&credit.shard())` lookup.
    #[tokio::test]
    async fn the_gc_keeps_a_pending_record_whose_shard_was_not_folded() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // **`skipped` is credited FIRST, and the order is the experiment.** The precondition below
        // needs both seqs at or below the cut the visited shard installs, and that cut is now this
        // shard's OWN maximum rather than the instance's global one — so it can only cover the
        // other shard's credit if that credit came earlier. Crediting `visited` first makes the
        // precondition unconstructible, which is a real narrowing of the scalar-GC hazard's
        // surface rather than a fixture inconvenience: a scalar cut can now only over-reclaim
        // records that were credited before the shard the pass visited.
        let seq_skipped = publish_staged_range(dir, &ledger, "bucket/skipped.bin", 8192).await;
        let seq_visited = publish_staged_range(dir, &ledger, "bucket/visited.bin", 4096).await;
        let (vb, vl) = shard_of("bucket/visited.bin").unwrap();
        let visited = Shard::new(vb, vl);
        let (kb, kl) = shard_of("bucket/skipped.bin").unwrap();
        let skipped = Shard::new(kb, kl);
        assert_ne!(visited, skipped);

        // A peer holds `skipped`, so this pass cannot visit it.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claim_path = ShardClaim::path(dir, &skipped);
        std::fs::create_dir_all(claim_path.parent().unwrap()).unwrap();
        std::fs::write(
            &claim_path,
            serde_json::to_string(&ShardClaim {
                instance_id: "host:2".to_string(),
                claimed_at: now,
                lease_secs: CLAIM_LEASE_SECS,
            })
            .unwrap(),
        )
        .unwrap();

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(installed(&outcome).busy, vec![skipped.clone()]);

        let set = read_shard_checkpoints(dir);
        let visited_cut = *set
            .get(&visited)
            .unwrap()
            .staged_fold_cut
            .get("host:1")
            .unwrap();
        // The input that makes a scalar cut wrong: BOTH seqs are at or below the one cut this
        // pass installed. A scalar comparison cannot tell them apart.
        assert!(
            seq_visited <= visited_cut && seq_skipped <= visited_cut,
            "both credits must sit at or below the installed cut, or a scalar GC would keep \
             the skipped one for the wrong reason: visited={seq_visited} \
             skipped={seq_skipped} cut={visited_cut}"
        );

        let pending = PendingStore::new(dir.to_path_buf()).read_all().await;
        let shards: BTreeSet<Shard> = pending.credits.iter().map(|(_, c)| c.shard()).collect();
        assert!(
            !shards.contains(&visited),
            "the visited shard's record is dead — its own shard's cut covers it — so the GC \
             reclaims it"
        );
        assert!(
            shards.contains(&skipped),
            "the unvisited shard's record MUST survive: no figure counts those bytes, so \
             deleting it would lose them until some future fold reads the .meta"
        );
        assert_eq!(
            pending.post_cut(&set).len(),
            1,
            "and it is still counted, by the pending term, exactly once"
        );
        assert_eq!(pending.post_cut_bytes(&set), 8192, "for its own bytes");
    }

    // =====================================================================
    // § 16.6.4 — Staged_Range_Budget
    // =====================================================================

    /// The freshness document round-trips, records **every** attempt, and advances its two
    /// timestamps independently.
    ///
    /// # What each assertion is for
    ///
    /// The badge test below asserts the *verdict* from a hand-built gauge bundle. This one asserts
    /// the bundle can actually be populated — without it, `completeness` could be correct over inputs
    /// nothing ever writes, which is the shape of a passing test over a dead feature.
    ///
    /// Arm 2 is the load-bearing one: a **failed** attempt must still be recorded. If only successful
    /// attempts wrote the document, `last_attempt_complete` could never be `false` and the badge's
    /// `Unknown` state would be unreachable — the whole point of the field.
    ///
    /// Arm 3 asserts the two timestamps move independently, which is what makes their gap "how long
    /// the fold has been failing". A single "last touched" timestamp would make a fold that has
    /// retained for a day look current.
    ///
    /// A failure implicates `record_fold_attempt` or `FoldFreshness::{load, install}`.
    #[test]
    fn the_freshness_document_records_every_attempt_and_not_only_the_successful_ones() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        // ---- Arm 1: absent reads as unknown, not as a failed attempt.
        assert_eq!(
            FoldFreshness::load(dir),
            None,
            "a volume with no document must read as absent, which the badge treats as unknown \
             rather than as failure"
        );

        // ---- Arm 2: a COMPLETE attempt, then a FAILED one. Both must be recorded.
        record_fold_attempt(dir, 1_000, true);
        let after_complete =
            FoldFreshness::load(dir).expect("the document must exist after a pass");
        assert_eq!(after_complete.completed_at, Some(1_000));
        assert_eq!(after_complete.last_attempt_at, Some(1_000));
        assert_eq!(after_complete.last_attempt_complete, Some(true));

        record_fold_attempt(dir, 2_000, false);
        let after_failure = FoldFreshness::load(dir).expect("the document must survive a failure");
        assert_eq!(
            after_failure.last_attempt_complete,
            Some(false),
            "a FAILED attempt must be recorded. If only successful attempts wrote this document, \
             `last_attempt_complete` could never be false and the badge's Unknown state would be \
             unreachable."
        );

        // ---- Arm 3: the two timestamps move independently.
        assert_eq!(
            after_failure.last_attempt_at,
            Some(2_000),
            "the attempt timestamp advances on every attempt"
        );
        assert_eq!(
            after_failure.completed_at,
            Some(1_000),
            "and the COMPLETED timestamp must NOT advance on a failure — the gap between the two is \
             how long the fold has been failing, which a single 'last touched' field cannot express"
        );

        // A later success advances both again.
        record_fold_attempt(dir, 3_000, true);
        let recovered = FoldFreshness::load(dir).unwrap();
        assert_eq!(recovered.completed_at, Some(3_000));
        assert_eq!(recovered.last_attempt_complete, Some(true));
    }

    /// § 16.6.7's freshness badge reaches **Unknown** on a failed last attempt while a complete
    /// older figure is still on display — the state I6's approximation could not express.
    ///
    /// # The substitution, and the exact input that separates the two predicates
    ///
    /// § 16.6.7 specifies `Unknown` as `!staged_fold_last_attempt_complete`, read from three
    /// `size_state.json` fields. Those fields did not exist, so I6 used the absence of the fold's
    /// byte figure as a stand-in and recorded it as a deviation. The stand-in is not *wrong* — a
    /// figure that cannot be derived now genuinely is unknown — it is **incomplete**, and the gap is
    /// a single state: the last attempt retained on an incomplete input, and an older
    /// `Complete_Install` is still on disk and still derivable. There the byte figure is `Some`, so
    /// the stand-in returned **Current** and the operator saw a plain number for a fold that had
    /// stopped working. That is the confident-wrong-number outcome the three-state rule exists to
    /// prevent, produced by the mechanism meant to prevent it.
    ///
    /// This is the eighth-plus instance of the recorded pattern in this workstream, in its
    /// asserting-an-adjacent-value form: the stand-in reads a field next to the one the design
    /// branches on, and agrees with it everywhere except the one state that matters.
    ///
    /// # Arm 1 is the discriminator; arms 2-4 are what stop it over-reaching
    ///
    /// Arm 1 is the only arm that distinguishes the two implementations. Arm 2 asserts the
    /// stand-in's own case still works, because the field is not a replacement for it — a complete
    /// last attempt does not help if a shard checkpoint has become unreadable since. Arm 3 asserts
    /// `None` falls back rather than reading as failure, which is § 16.6.6's mixed-version
    /// requirement: a pre-C binary rewriting `size_state.json` strips these fields, and a fleet
    /// mid-upgrade must not show UNKNOWN on every proxy for that reason. Arm 4 pins that a complete
    /// attempt with a stale figure is still **StaleButValid** and not silently promoted.
    ///
    /// A failure in arm 1 or 3 implicates `StagedFoldGauges::completeness`; a failure that shows the
    /// field always being `None` implicates `JournalConsolidator::record_staged_fold_attempt` or the
    /// `FoldFreshness` plumbing in `metrics.rs`.
    #[test]
    fn the_freshness_badge_reads_the_last_attempt_and_not_only_the_derivable_figure() {
        // A gauge bundle in the state that matters: a COMPLETE figure is derivable and fresh, so
        // every observable stand-in says "fine".
        let healthy = StagedFoldGauges {
            staged_fold_bytes: Some(4_096),
            staged_shard_authority_age_secs: Some(10),
            staged_authority_max_age_secs: MAX_AUTHORITY_AGE_SECS,
            ..Default::default()
        };
        assert_eq!(
            healthy.completeness(),
            StagedCompleteness::Current,
            "fixture precondition: with a derivable, fresh figure and no attempt recorded, the \
             verdict must be Current — otherwise arm 1 is not isolating the attempt field"
        );

        // ---- Arm 1: the discriminator. Same derivable figure, same age, last attempt FAILED.
        let last_attempt_failed = StagedFoldGauges {
            staged_fold_last_attempt_complete: Some(false),
            ..healthy.clone()
        };
        assert_eq!(
            last_attempt_failed.completeness(),
            StagedCompleteness::Unknown,
            "a failed last attempt must read UNKNOWN even though a complete older figure is still \
             derivable and inside the age bound. This is the state the byte-figure stand-in \
             reported as CURRENT: the fold has stopped working and every other signal on the card \
             looks healthy."
        );

        // ---- Arm 2: the stand-in's own case still holds. The field says complete; the figure is
        // not derivable now. Unknown regardless — the field is an addition, not a replacement.
        let figure_undrivable = StagedFoldGauges {
            staged_fold_bytes: None,
            staged_fold_last_attempt_complete: Some(true),
            ..healthy.clone()
        };
        assert_eq!(
            figure_undrivable.completeness(),
            StagedCompleteness::Unknown,
            "a complete last attempt does not make an underivable figure current — a shard \
             checkpoint may have become unreadable since the attempt. Deleting the byte-figure \
             check in favour of the new field would lose this."
        );

        // ---- Arm 3: absence falls back, and does NOT read as failure.
        //
        // § 16.6.6's Phase L: a pre-C binary rewriting size_state.json strips these fields. If
        // `None` read as a failed attempt, every proxy on a mixed-version fleet would show UNKNOWN
        // for a fold that is working — and the badge would be least trustworthy exactly during the
        // upgrade it exists to survive.
        assert_eq!(
            healthy.staged_fold_last_attempt_complete, None,
            "fixture precondition: the healthy bundle must carry NO attempt verdict, or arm 3 is \
             asserting the same thing as the precondition above"
        );
        assert_eq!(
            healthy.completeness(),
            StagedCompleteness::Current,
            "an ABSENT attempt verdict must fall back to the observable form, not read as a failed \
             attempt"
        );

        // ---- Arm 4: a complete attempt over a STALE figure is StaleButValid, not promoted.
        let stale = StagedFoldGauges {
            staged_shard_authority_age_secs: Some(MAX_AUTHORITY_AGE_SECS + 1),
            staged_fold_last_attempt_complete: Some(true),
            ..healthy.clone()
        };
        assert_eq!(
            stale.completeness(),
            StagedCompleteness::StaleButValid,
            "a complete last attempt must not promote a figure past its age bound — the attempt \
             completing and the figure being fresh are different claims, and the badge's middle \
             state is the one that says so"
        );
        // And a FAILED attempt over a stale figure is Unknown, not StaleButValid: the retained
        // figure's own age stops being the headline once the fold is not running.
        let stale_and_failed = StagedFoldGauges {
            staged_fold_last_attempt_complete: Some(false),
            ..stale.clone()
        };
        assert_eq!(
            stale_and_failed.completeness(),
            StagedCompleteness::Unknown,
            "and Unknown must take precedence over StaleButValid, or a fold that has stopped \
             working is reported as merely behind"
        );
    }

    /// **`Staged_Range_Budget` meets § 16.6.4's `Max_Authority_Age`, as arithmetic.**
    ///
    /// # What this test establishes, and what it does NOT
    ///
    /// Stated plainly, because a green unit test next to a stated acceptance criterion invites
    /// exactly the wrong reading:
    ///
    /// **It establishes two things.** First, that the authority-age arithmetic implemented and
    /// documented here is the same arithmetic § 16.6.4 tabulates — asserted by reproducing every
    /// cell of its table, and every entry of its max-`S` column, from one function. Second, that
    /// [`STAGED_RANGE_BUDGET`] is a value that arithmetic permits at `I = 1`, and that it is the
    /// *conservative fraction* of the bound rather than merely some number under it — so the
    /// constant and the criterion cannot drift apart silently: change either and this fails.
    ///
    /// **It establishes nothing whatsoever about the fleet.** The staged count here is a
    /// synthetic number in a `u64`; no `.meta` is parsed, no shard is folded, no shared volume
    /// is touched, and every rate is an input rather than a measurement taken here. In
    /// particular it does **not** establish that `p100 ≤ 3,600 s` holds — that is a **p100 over
    /// a 72-hour window** on a fixture holding `Staged_Range_Budget` staged ranges, at the
    /// deployed instance count, with sustained write-through churn, and § 16.6.4 assigns it to a
    /// measured soak. That soak is a Phase 4 item gated on Ed's approval and has not been run.
    /// **A pass here is not the criterion being met.**
    ///
    /// # The rate is a MEASURED PER-BACKEND AGGREGATE, not a per-instance rate times `I`
    ///
    /// This is the part that changed on 2026-09-21, and it is a change of *form*, not of
    /// numbers. § 16.6.4's table used to read `S / (R × I × D)` at `R = 212 obj/s`. The
    /// 2026-09-06 amendment withdrew that rate as a concurrency artefact; M4 and M5 then
    /// withdrew **the `R × I` product itself**. Aggregate scan rate is a measured function of
    /// fleet size whose *shape differs by backend*: on FSx for OpenZFS it peaks at `I = 3` and
    /// declines thereafter, on FSx for NetApp ONTAP it rises monotonically to at least `I = 24`.
    /// So `R(1) × I` overstates by 9.88× at FSxZ `I = 24`, and a re-tabulation of the old form
    /// at any single `R` would have printed an `I = 20` column ~8× optimistic — cells that would
    /// themselves have been withdrawn.
    ///
    /// The cells below therefore carry `Agg(I)`, measured per `(backend, I)`. They are
    /// **current**, not retained-but-superseded, which is why they are a stronger identity check
    /// than the twelve legacy rows they replace: reproducing a withdrawn table only ever proved
    /// the two models matched, whereas these also have to be *true*.
    ///
    /// `Agg` is M4's and M5's `A(I)`, renamed because `A` already means `Max_Authority_Age` in
    /// § 16.6.4's own intersection arithmetic (`S ≤ D · I · A · R`).
    ///
    /// **`R_MEASURED = 2,900 obj/s` still sizes the budget, and is untouched by any of this**,
    /// because the budget is sized at `I = 1` where `Agg(1)` *is* `R` and no aggregate-scaling
    /// question arises. It is the EFS planning figure: read-plus-parse against outstanding
    /// requests on `m6in.2xlarge` fleet proxies over the 1,146,790-object reference tree,
    /// rounded down from the lowest of five knee measurements. Its residual optimism is that the
    /// measurement fleet was idle. `R_LEGACY = 212 obj/s` survives in one arm only — the
    /// materiality ratio — and tabulates nothing.
    ///
    /// A failure in the table arm means this test's model and the design's disagree, **or** a
    /// measured rate was edited in one place and not the other; in either case resolve the
    /// disagreement before changing [`STAGED_RANGE_BUDGET`]. The literals below and § 16.6.4's
    /// printed cells were produced from a single derivation, which also checked every cell
    /// against half its last printed digit — so a disagreement means one side was hand-edited.
    ///
    /// # This test is NOT sufficient on its own, and that is the reason there are two
    ///
    /// It measures **parse throughput** and nothing else. The model assumes the scheduler keeps
    /// the parser fed at duty cycle `D`; whether it does is a property of
    /// [`shards_per_pass`], which this test does not read. A fixed per-pass budget of 8 shards
    /// reproduced all 12 of the legacy cells while actually taking 9,600 s to revisit a
    /// single-bucket cache — 2.7× the bound.
    /// `staged_range_budget_visits_every_shard_inside_the_age_bound` is the test that catches
    /// that, and neither of the two is redundant.
    #[test]
    fn staged_range_budget_bound_holds_at_the_design_point() {
        /// The acceptance criterion, seconds, p100: the shipped constant, read rather than copied.
        const MAX_AUTHORITY_AGE_SECS: f64 = super::MAX_AUTHORITY_AGE_SECS as f64;
        /// The withdrawn concurrency artefact. It tabulates NOTHING — its only remaining use is
        /// the materiality-ratio arm at the bottom, which asserts the correction was worth making.
        const R_LEGACY: f64 = 212.0;
        /// The MEASURED read-plus-parse rate the budget is sized on: EFS, `I = 1`, where
        /// `Agg(1)` is `R` and no aggregate-scaling question arises.
        const R_MEASURED: f64 = 2_900.0;
        /// Default duty cycle (§ 16.6.4's table).
        const D: f64 = 0.167;

        /// `S / (Agg × D)`, seconds. The whole model, in one place, so the table below and the
        /// budget assertions cannot be computed two different ways.
        ///
        /// **The parameter is an AGGREGATE rate, not a per-instance rate.** It used to take
        /// `(staged, instances, rate)` and form `rate * instances`; that product is retired —
        /// see the doc comment. Callers pass a measured `Agg(I)` for a named `(backend, I)`, or
        /// `R_MEASURED` for `I = 1`, where the two coincide.
        fn authority_age_secs(staged: f64, aggregate_rate: f64) -> f64 {
            staged / (aggregate_rate * D)
        }

        // ---- § 16.6.4's table, every cell, at the precision the design prints it to.
        //
        // # These cells are MEASURED AGGREGATES, one per (backend, I)
        //
        // These literals and § 16.6.4's printed table were emitted from ONE derivation, which
        // also checked every cell against half its last printed digit. Neither side was
        // transcribed by hand, which is the property that makes the identity check below mean
        // something rather than merely restate a number.
        //
        // The `Agg(I)` column is not `R(1) × I` and must never be replaced by one. Provenance,
        // with the direction of every caveat:
        //
        //   EFS  I=1   2,900.0   the planning figure: lowest of five knee measurements,
        //                        rounded down. Optimistic only in that the fleet was idle.
        //   EFS  I=3   9,252.0   measured aggregate of three barrier-synchronised proxies
        //                        (2,969 + 3,402 + 2,881). Note this EXCEEDS 3 × 2,900, i.e.
        //                        EFS is slightly super-linear here — a local fact, not a law.
        //   FSxZ       M4 ladder, concurrency 32, 120,000 files/client, every run CLEAN.
        //              PEAKS at I=3 and DECLINES after, so Agg(24) < Agg(3). A model with `I`
        //              in the numerator cannot express that, which is why the product is gone.
        //   FSxN       M5 ladder, same method, but every multi-client run is CONTAMINATED by
        //              uneven LIF distribution — so these use M5's `I × slowest client` LOWER
        //              bound, which gives an OLDER age and is the conservative direction. The
        //              upper bound at I=24 is 200,682 obj/s. All three ladders ran at
        //              concurrency 32, which is FSxZ's knee but BELOW FSxN's knee of 64, so
        //              the FSxN rows understate that backend twice over.
        //
        // No cell is interpolated or extrapolated: a row exists only where a measurement
        // exists. That is why there is no `I = 20` column any more — nothing measured one —
        // and no EFS row past `I = 3`.
        //
        // # Tolerances
        //
        // Each is half the last digit the design prints: 0.05 s for a `206.5 s` cell, 3.0 s
        // (0.05 min) for a `34.4 min` cell, 18.0 s (0.005 h) for a `5.74 h` cell. That is a
        // real assertion — it says the printed cell is the correct rounding of this arithmetic,
        // not merely close to it. Three significant figures throughout keeps every cell clear
        // of a rounding boundary; the worst sits at 0.97 of its tolerance, and the derivation
        // script asserts none exceeds 1.0.
        //
        // The old `I = 20` double-rounding note is gone with the column it described: no cell
        // is now derived by dividing another displayed cell, so none carries a second cell's
        // rounding.
        //
        // # The `breaches` field is asserted, and it is the half that is easy to get wrong
        //
        // Reproducing an age proves the arithmetic. It does not prove the design drew the right
        // conclusion FROM the age — and the conclusion is what a reader acts on. So each case
        // carries the design's own bold/not-bold verdict, checked against the criterion
        // independently, which catches a cell whose number is right and whose emphasis is not.
        let hour = 3_600.0;
        let minute = 60.0;
        let cases: [(f64, f64, f64, f64, bool, &str); 32] = [
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                2_900.0,
                206.5,
                0.05,
                false,
                "EFS I=1 S=100k: 206.5 s",
            ),
            (
                1_000_000.0,
                2_900.0,
                34.4 * minute,
                3.0,
                false,
                "EFS I=1 S=1M: 34.4 min",
            ),
            (
                10_000_000.0,
                2_900.0,
                5.74 * hour,
                18.0,
                false,
                "EFS I=1 S=10M: 5.74 h",
            ),
            (
                100_000_000.0,
                2_900.0,
                57.36 * hour,
                18.0,
                true,
                "EFS I=1 S=100M: 57.36 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                9_252.0,
                64.7,
                0.05,
                false,
                "EFS I=3 S=100k: 64.7 s",
            ),
            (
                1_000_000.0,
                9_252.0,
                10.8 * minute,
                3.0,
                false,
                "EFS I=3 S=1M: 10.8 min",
            ),
            (
                10_000_000.0,
                9_252.0,
                107.9 * minute,
                3.0,
                false,
                "EFS I=3 S=10M: 107.9 min",
            ),
            (
                100_000_000.0,
                9_252.0,
                17.98 * hour,
                18.0,
                false,
                "EFS I=3 S=100M: 17.98 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                29_020.9,
                20.6,
                0.05,
                false,
                "FSxZ I=1 S=100k: 20.6 s",
            ),
            (
                1_000_000.0,
                29_020.9,
                206.3,
                0.05,
                false,
                "FSxZ I=1 S=1M: 206.3 s",
            ),
            (
                10_000_000.0,
                29_020.9,
                34.4 * minute,
                3.0,
                false,
                "FSxZ I=1 S=10M: 34.4 min",
            ),
            (
                100_000_000.0,
                29_020.9,
                5.73 * hour,
                18.0,
                false,
                "FSxZ I=1 S=100M: 5.73 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                81_862.8,
                7.3,
                0.05,
                false,
                "FSxZ I=3 S=100k: 7.3 s",
            ),
            (
                1_000_000.0,
                81_862.8,
                73.1,
                0.05,
                false,
                "FSxZ I=3 S=1M: 73.1 s",
            ),
            (
                10_000_000.0,
                81_862.8,
                12.2 * minute,
                3.0,
                false,
                "FSxZ I=3 S=10M: 12.2 min",
            ),
            (
                100_000_000.0,
                81_862.8,
                2.03 * hour,
                18.0,
                false,
                "FSxZ I=3 S=100M: 2.03 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                70_467.2,
                8.5,
                0.05,
                false,
                "FSxZ I=24 S=100k: 8.5 s",
            ),
            (
                1_000_000.0,
                70_467.2,
                85.0,
                0.05,
                false,
                "FSxZ I=24 S=1M: 85.0 s",
            ),
            (
                10_000_000.0,
                70_467.2,
                14.2 * minute,
                3.0,
                false,
                "FSxZ I=24 S=10M: 14.2 min",
            ),
            (
                100_000_000.0,
                70_467.2,
                2.36 * hour,
                18.0,
                false,
                "FSxZ I=24 S=100M: 2.36 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                28_542.7,
                21.0,
                0.05,
                false,
                "FSxN I=1 S=100k: 21.0 s",
            ),
            (
                1_000_000.0,
                28_542.7,
                209.8,
                0.05,
                false,
                "FSxN I=1 S=1M: 209.8 s",
            ),
            (
                10_000_000.0,
                28_542.7,
                35.0 * minute,
                3.0,
                false,
                "FSxN I=1 S=10M: 35.0 min",
            ),
            (
                100_000_000.0,
                28_542.7,
                5.83 * hour,
                18.0,
                false,
                "FSxN I=1 S=100M: 5.83 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                55_354.8,
                10.8,
                0.05,
                false,
                "FSxN I=3 S=100k: 10.8 s",
            ),
            (
                1_000_000.0,
                55_354.8,
                108.2,
                0.05,
                false,
                "FSxN I=3 S=1M: 108.2 s",
            ),
            (
                10_000_000.0,
                55_354.8,
                18.0 * minute,
                3.0,
                false,
                "FSxN I=3 S=10M: 18.0 min",
            ),
            (
                100_000_000.0,
                55_354.8,
                3.0 * hour,
                18.0,
                false,
                "FSxN I=3 S=100M: 3.00 h",
            ),
            (
                100_000.0, // S=100k column of the decade sweep, not the budget
                162_760.8,
                3.7,
                0.05,
                false,
                "FSxN I=24 S=100k: 3.7 s",
            ),
            (
                1_000_000.0,
                162_760.8,
                36.8,
                0.05,
                false,
                "FSxN I=24 S=1M: 36.8 s",
            ),
            (
                10_000_000.0,
                162_760.8,
                367.9,
                0.05,
                false,
                "FSxN I=24 S=10M: 367.9 s",
            ),
            (
                100_000_000.0,
                162_760.8,
                61.3 * minute,
                3.0,
                false,
                "FSxN I=24 S=100M: 61.3 min",
            ),
        ];
        for (staged, aggregate, tabulated, tolerance, breaches, label) in cases {
            let computed = authority_age_secs(staged, aggregate);
            assert!(
                (computed - tabulated).abs() <= tolerance,
                "§ 16.6.4's table row `{label}` does not reproduce: computed {computed:.2} s \
                 against a tabulated {tabulated:.2} s, outside the ±{tolerance} s the cell's \
                 own printed precision allows. This is a MODEL-IDENTITY check against a table \
                 of MEASURED aggregates, so a failure means one of three things: this test's \
                 model and the design's disagree, an Agg(I) was edited in one place and not the \
                 other, or someone reintroduced an `R x I` product. Resolve which before \
                 changing STAGED_RANGE_BUDGET."
            );
            assert_eq!(
                computed > MAX_AUTHORITY_AGE_SECS,
                breaches,
                "§ 16.6.4's table row `{label}` reproduces its AGE but not its VERDICT: \
                 computed {computed:.2} s against the {MAX_AUTHORITY_AGE_SECS} s criterion, so \
                 the design's emphasis on that cell is wrong. The number and the conclusion \
                 drawn from it are separate claims, and the conclusion is the one a reader acts \
                 on."
            );
        }

        // ---- What the table SAYS, asserted rather than left to the prose.
        //
        // Three claims § 16.6.4 now makes in words. Each is checked, because a table whose
        // cells are individually right can still support a wrong summary — and it is the
        // summary that gets quoted.

        /// Largest staged count whose authority age is inside the criterion, for a given
        /// aggregate rate: the criterion inverted. This is the design's max-`S` column.
        fn max_staged_inside(aggregate_rate: f64) -> f64 {
            aggregate_rate * D * MAX_AUTHORITY_AGE_SECS
        }

        let measured: [(f64, &str); 8] = [
            (2_900.0, "EFS I=1"),
            (9_252.0, "EFS I=3"),
            (29_020.9, "FSxZ I=1"),
            (81_862.8, "FSxZ I=3"),
            (70_467.2, "FSxZ I=24"),
            (28_542.7, "FSxN I=1"),
            (55_354.8, "FSxN I=3"),
            (162_760.8, "FSxN I=24"),
        ];

        // 1. At the 24 h criterion every measured configuration meets the O(100M)-range design
        //    point except single-instance EFS, which is over by ~2.4x. This is what § 16.6.12's
        //    window change buys: at the retired 1 h criterion NO configuration met it.
        let design_point = 100_000_000.0;
        let breaching: Vec<&str> = measured
            .iter()
            .filter(|(agg, _)| authority_age_secs(design_point, *agg) > MAX_AUTHORITY_AGE_SECS)
            .map(|(_, label)| *label)
            .collect();
        assert_eq!(
            breaching,
            vec!["EFS I=1"],
            "at the {MAX_AUTHORITY_AGE_SECS} s criterion only single-instance EFS should miss the \
             O(100M) design point"
        );
        let efs_overshoot = authority_age_secs(design_point, 2_900.0) / MAX_AUTHORITY_AGE_SECS;
        assert!(
            (2.3..2.5).contains(&efs_overshoot),
            "single-instance EFS should miss the design point by ~2.4x; computed {efs_overshoot:.3}x"
        );
        assert!(
            authority_age_secs(design_point, 2_900.0) > 3_600.0
                && measured
                    .iter()
                    .all(|(agg, _)| authority_age_secs(design_point, *agg) > 3_600.0),
            "and at the retired 3,600 s criterion no measured configuration met it, which is the \
             capacity the window change recovers"
        );

        // 2. Adding instances can REDUCE admissible staged count. This is the assertion an
        //    `R x I` model cannot express at all, and it is the load-bearing half of
        //    BACKEND_IS_A_PARAMETER: FSxZ's aggregate peaks at I=3 and declines after.
        assert!(
            max_staged_inside(70_467.2) < max_staged_inside(81_862.8),
            "on FSxZ a 24-node fleet must admit FEWER staged ranges than a 3-node one, because \
             Agg(24) < Agg(3): {:.0} against {:.0}. If this fails, a measured aggregate has \
             been replaced by a per-instance rate times I.",
            max_staged_inside(70_467.2),
            max_staged_inside(81_862.8)
        );

        // 3. The spread across measured configurations is ~56x, which is the figure behind
        //    BACKEND_IS_A_PARAMETER's no-default rule: a default would be wrong by up to that
        //    much with nothing signalling it.
        let lo = measured
            .iter()
            .map(|(a, _)| max_staged_inside(*a))
            .fold(f64::INFINITY, f64::min);
        let hi = measured
            .iter()
            .map(|(a, _)| max_staged_inside(*a))
            .fold(0.0_f64, f64::max);
        assert!(
            (55.0..58.0).contains(&(hi / lo)),
            "§ 16.6.4 states a ~56x spread in admissible staged count across measured \
             configurations, {lo:.0} to {hi:.0}; computed {:.1}x",
            hi / lo
        );

        // And the max-S column is that same criterion inverted, so it must agree with every
        // age cell in BOTH directions: a cell is bold exactly when its S exceeds its row's
        // max-S. Two renderings of one criterion cannot disagree.
        for (staged, aggregate, _, _, breaches, label) in cases {
            assert_eq!(
                staged > max_staged_inside(aggregate),
                breaches,
                "the max-S column and the age cells disagree for `{label}`: max-S is {:.0} \
                 against S = {staged:.0}, but the age verdict says breaches={breaches}. These \
                 are one criterion expressed two ways.",
                max_staged_inside(aggregate)
            );
        }

        // ---- The budget is inside the bound at I = 1, at the MEASURED rate.
        //
        // The bound is `S ≤ D · I · A · R`, which is the intersection of parse rate with visit
        // scheduling — the same inequality as the age model above, solved for `S` rather than
        // for age. Computed here, never written as a literal, so the constant is checked against
        // arithmetic rather than against a number someone typed twice.
        let bound_at_one = D * 1.0 * MAX_AUTHORITY_AGE_SECS * R_MEASURED;
        assert!(
            (bound_at_one - 41_843_520.0).abs() < 1.0,
            "the bound at A = 86,400 s is 41,843,520 ranges at I = 1 (0.167 x 86,400 x 2,900); \
             this model gives {bound_at_one:.0}"
        );
        assert!(
            (STAGED_RANGE_BUDGET as f64) <= bound_at_one,
            "the budget must be inside the bound at I = 1, since a single-instance deployment \
             is supported: {STAGED_RANGE_BUDGET} against {bound_at_one:.0} ranges"
        );
        // `R_MEASURED` is passed as an AGGREGATE here, which is exact rather than a shortcut:
        // this is the `I = 1` case, where `Agg(1)` and `R` are the same number by definition.
        let at_budget = authority_age_secs(STAGED_RANGE_BUDGET as f64, R_MEASURED);
        assert!(
            at_budget <= MAX_AUTHORITY_AGE_SECS,
            "and the age it implies must be inside the criterion: {at_budget:.0} s against \
             {MAX_AUTHORITY_AGE_SECS:.0} s"
        );

        // ---- It is the CONSERVATIVE FRACTION of the bound, not merely under it.
        //
        // This is the assertion that stops the budget being any number below the bound. The
        // shipped convention is ~78.5%, and 32,800,000 of 41,843,520 is 78.4%. The window pins it
        // to the convention's
        // neighbourhood in BOTH directions: a budget at 99% of the bound would leave nothing for
        // the fact that `R` was measured on an idle fleet, and one at 10% would be the artefact
        // being reintroduced under a new number.
        let fraction = STAGED_RANGE_BUDGET as f64 / bound_at_one;
        assert!(
            (0.70..=0.80).contains(&fraction),
            "the budget must sit in the 70-80% margin band the shipped convention uses, not \
             merely somewhere below the bound: {STAGED_RANGE_BUDGET} is {:.1}% of \
             {bound_at_one:.0}",
            fraction * 100.0
        );

        // The red side, and it is what stops this test passing for a budget of 1: the NEXT
        // order of magnitude must breach the bound. A budget chosen without arithmetic would
        // satisfy the assertions above and fail this one.
        let ten_times = authority_age_secs((STAGED_RANGE_BUDGET * 10) as f64, R_MEASURED);
        assert!(
            ten_times > MAX_AUTHORITY_AGE_SECS,
            "10x the budget must breach the bound at I = 1, or the budget is not the binding \
             constraint and this arithmetic is not what sized it: {ten_times:.0} s"
        );
        // And the budget is sized for I = 1 rather than for today's three-node fleet, which is
        // the sizing decision § 16.6.4 states. Computed from the constant rather than from a
        // literal, so it tracks the budget: a budget sized for I = 3 would be 3x this one, pass
        // on that fleet, and breach on a single instance.
        //
        // **The I = 3 side uses EFS's MEASURED Agg(3) = 9,252, not `3 × R_MEASURED` = 8,700.**
        // That substitution is the whole point of the 2026-09-21 re-tabulation and it would be
        // absurd to reintroduce it in the one assertion that actually multiplies by three. It
        // also makes the arm slightly HARDER to satisfy in the direction that matters: EFS is
        // marginally super-linear at three instances (9,252 > 8,700), so the measured figure
        // gives a younger age at I = 3 and the pass is on a real number rather than a modelled
        // one. That super-linearity is a local fact about EFS at I = 3 — FSxZ is 6% sub-linear
        // there and 9.88x sub-linear at I = 24 — so do not generalise it, and do not rewrite
        // this arm for another backend without its own measured Agg(3).
        let sized_for_three = (STAGED_RANGE_BUDGET * 3) as f64;
        const EFS_AGG_AT_THREE: f64 = 9_252.0;
        assert!(
            authority_age_secs(sized_for_three, EFS_AGG_AT_THREE) <= MAX_AUTHORITY_AGE_SECS
                && authority_age_secs(sized_for_three, R_MEASURED) > MAX_AUTHORITY_AGE_SECS,
            "a budget sized for the three-node fleet ({sized_for_three:.0}) would pass on that \
             fleet and breach on a single instance — which is why this one is sized for I = 1"
        );

        // ---- And the correction was material, not cosmetic.
        //
        // The retired budget's own bound, at the artefact rate, so the 13x is asserted rather
        // than asserted-about-in-prose. This is the row Ed accepted as a real capability cost.
        let bound_at_legacy_rate = D * 1.0 * MAX_AUTHORITY_AGE_SECS * R_LEGACY;
        assert!(
            (bound_at_legacy_rate - 3_058_905.6).abs() < 1.0,
            "the bound at the retired R = 212, at this window, is 3,058,906 ranges; this model \
             gives {bound_at_legacy_rate:.0}"
        );
        assert!(
            bound_at_one / bound_at_legacy_rate > 13.0,
            "the measured rate must widen the bound by more than an order of magnitude, which \
             is what makes the retired ~800 GiB capability reduction an artefact rather than a \
             cost: the ratio is {:.1}x",
            bound_at_one / bound_at_legacy_rate
        );

        // ---- § 16.6.4's OVERSTATEMENT table, which is the argument for retiring `R x I`.
        //
        // The design prints these four rows to justify changing the table's shape rather than
        // its numbers, so they are printed cells and fall under the same model-identity duty as
        // the ages above. Each row is `R(1) x I` against the measured `Agg(I)`.
        let overstatement: [(f64, f64, f64, f64, &str); 4] = [
            (29_020.9, 3.0, 81_862.8, 1.06, "FSxZ I=3"),
            (29_020.9, 24.0, 70_467.2, 9.88, "FSxZ I=24"),
            (28_542.7, 3.0, 55_354.8, 1.55, "FSxN I=3"),
            (28_542.7, 24.0, 162_760.8, 4.21, "FSxN I=24"),
        ];
        for (solo, instances, measured_agg, tabulated_ratio, label) in overstatement {
            let ratio = solo * instances / measured_agg;
            assert!(
                (ratio - tabulated_ratio).abs() <= 0.005,
                "§ 16.6.4's overstatement row `{label}` does not reproduce: `R(1) x I` over the \
                 measured Agg(I) is {ratio:.3}x against a tabulated {tabulated_ratio}x. These \
                 rows ARE the argument for retiring the product, so a disagreement here means \
                 the design's justification no longer follows from its own figures."
            );
            assert!(
                ratio > 1.0,
                "`R(1) x I` must OVERSTATE the measured aggregate at `{label}` — if it ever \
                 understates, the conservative direction has flipped and the retired model was \
                 pessimistic rather than optimistic, which would change the whole argument."
            );
        }

        // And the `I = 20` column the design used to print, on FSxZ, from M4's own fit — the
        // concrete reason re-tabulating the OLD FORM at a new rate was rejected. M4 states the
        // fit valid for 7 <= I <= 36, so I = 20 is interpolation inside the measured range
        // rather than extrapolation beyond it.
        let fsxz_agg_at_twenty = 85_252.0 - 4_596.0 * 20.0_f64.ln();
        assert!(
            (fsxz_agg_at_twenty - 71_484.0).abs() < 1.0,
            "M4's fit gives Agg(20) = 71,484 obj/s on FSxZ; this arithmetic gives \
             {fsxz_agg_at_twenty:.0}"
        );
        let old_form_at_twenty = 29_020.9 * 20.0;
        assert!(
            (old_form_at_twenty / fsxz_agg_at_twenty - 8.12).abs() <= 0.005,
            "§ 16.6.4 states that re-tabulating the old form at I = 20 would have been ~8.12x \
             optimistic on FSxZ; computed {:.3}x. This is the cell that would itself have had \
             to be withdrawn, and it is why the column is gone rather than recomputed.",
            old_form_at_twenty / fsxz_agg_at_twenty
        );
    }

    /// **Every shard is revisited inside `Max_Authority_Age`, which is a SCHEDULING property and
    /// not a parse-rate one.**
    ///
    /// # Why this test exists separately from the one above
    ///
    /// § 16.6.4's authority-age model is pure parse throughput and assumes the scheduler keeps
    /// the parser fed at duty cycle `D`. A fixed per-pass budget does not, and the failure is
    /// invisible to a test of the model: at `S = 100,000` over 256 shards, 8 shards per pass
    /// parses ~3,125 ranges — 14.7 s inside a 300 s pass, a 4.9% duty cycle against the model's
    /// 16.7%. That ratio of ~3.4 is the gap between the model's predicted 2,825 s and the
    /// scheduler's actual 9,600 s. The parse-arithmetic test reproduced every cell of the
    /// design's table and was silent about it, which is the eighth instance of this repo's
    /// recorded failure mode and the first where the substituted quantity came from the *design*
    /// rather than from the implementation.
    ///
    /// **The 2,825 s and 4.9% figures in that paragraph are HISTORICAL**, at the withdrawn
    /// `R = 212 obj/s`, and are left as they were because they describe the defect as it was
    /// found. Nothing below depends on them: this test's own verdict is
    /// `shards / per_pass × interval` against the bound, which carries no rate at all — which is
    /// precisely why it survived both the 2026-09-06 rate withdrawal and the 2026-09-21
    /// retirement of the `R × I` product untouched, while the companion test's cells had to be
    /// re-derived twice.
    ///
    /// # What is asserted, and against which line
    ///
    /// Arm 1 reads [`shards_per_pass`] — the function the fold's `take` bound is computed from —
    /// and asserts the revisit time it implies is inside [`MAX_AUTHORITY_AGE_SECS`], at shard
    /// counts from 1 to the ceiling. Arm 2 computes the same thing for a fixed 8 and asserts it
    /// is **outside** the bound, so this test carries its own red side rather than resting on a
    /// separate demonstration. Arm 3 runs a real fold over 256 seeded shards and asserts the pass
    /// installed the derived number, so a correct `shards_per_pass` wired to nothing still fails.
    ///
    /// A failure in arm 1 or 3 implicates `shards_per_pass` and the `take(budget)` bound in
    /// `run_staged_fold_with_cap`.
    #[tokio::test]
    async fn staged_range_budget_visits_every_shard_inside_the_age_bound() {
        /// Seconds to revisit every one of `shards` at `per_pass` shards per pass.
        fn revisit_secs(shards: usize, per_pass: usize) -> u64 {
            (shards.div_ceil(per_pass.max(1)) as u64).saturating_mul(STAGED_FOLD_INTERVAL_SECS)
        }

        // ---- Arm 1: the derived budget puts every shard inside the bound.
        //
        // **256 FIRST, deliberately.** It is a single-bucket cache — the § 3.1 shard unit is
        // `(bucket, L1)` with `L1` two hex digits, so 256 is the FLOOR of the real population, not
        // an upper bound — and it is the case the finding is about, so it should be the case a
        // failure names. 2,560 is ten buckets.
        //
        // The largest case is DERIVED from the ceiling rather than written down. It used to be the
        // literal `12,288` — 48 buckets, chosen as "just under the ceiling" when the ceiling was
        // 1,024. When the ceiling was re-derived to 512 that literal became a population the
        // ceiling cannot sweep, and this arm failed with 7,200 s against the 3,600 s bound. The
        // literal was correct arithmetic about a constant it did not read; the failure was this
        // assertion working as intended, and computing the case fixes the cause rather than the
        // symptom.
        //
        // No parse rate appears anywhere in this arm. The verdict is `shards / per_pass × interval`
        // against the bound, which is pure scheduling arithmetic and cannot be re-litigated by
        // revisiting the provenance of an objects-per-second figure.
        let passes = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS).max(1) as usize;
        let largest_sweepable = SHARD_VISITS_PER_PASS_CEILING * passes;
        for shards in [256usize, 1, 12, 13, 100, 2_560, largest_sweepable] {
            let per_pass = shards_per_pass(shards);
            let revisit = revisit_secs(shards, per_pass);
            assert!(
                revisit <= MAX_AUTHORITY_AGE_SECS,
                "at {shards} shards the derived budget of {per_pass}/pass revisits every shard \
                 in {revisit}s, outside the {MAX_AUTHORITY_AGE_SECS}s p100 bound. This is the \
                 SCHEDULING constraint; the parse-rate model in \
                 staged_range_budget_bound_holds_at_the_design_point cannot see it."
            );
        }
        // And the derivation is the stated formula, not merely something that satisfies it.
        assert_eq!(passes, 288, "86,400 s / 300 s");
        assert_eq!(
            shards_per_pass(256),
            1,
            "ceil(256 / (86400/300)) = ceil(256/288) = 1"
        );
        assert_eq!(shards_per_pass(2_560), 9, "ceil(2560/288) = 9");
        assert_eq!(
            shards_per_pass(1),
            1,
            "the floor: a pass with one shard must still visit it"
        );

        // ---- Arm 2: the red side, in-test. A fixed 8 breaches the bound once the cache grows.
        //
        // Computed from the same `revisit_secs` as arm 1 so the two cannot disagree about the
        // model. At the 24 h window a fixed 8 covers one bucket, so the defect shows at ten.
        assert_eq!(
            revisit_secs(2_560, 8),
            96_000,
            "the arithmetic of the defect: (2560/8) x 300s"
        );
        assert!(
            revisit_secs(2_560, 8) > MAX_AUTHORITY_AGE_SECS,
            "a fixed per-pass budget of 8 must be OUTSIDE the bound at ten buckets, or arm 1 \
             proves nothing about the derivation being necessary"
        );
        assert!(
            revisit_secs(25_600, 8) > MAX_AUTHORITY_AGE_SECS * 10,
            "and over 10x at a hundred buckets: {}s",
            revisit_secs(25_600, 8)
        );

        // ---- Arm 3: the derivation is actually wired to the pass.
        //
        // Without this, `shards_per_pass` could be correct and unused. 256 shards are seeded as
        // real checkpoint documents — a single-bucket cache with every L1 populated, which is the
        // state at the design point.
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        for l1 in 0u16..256 {
            seed_shard_figure(dir, &Shard::new("bucket", format!("{l1:02x}")), 0, 0, 7_200);
        }
        let observed = read_shard_checkpoints(dir);
        assert_eq!(
            observed.shards().len(),
            256,
            "the input: a single-bucket cache really does have 256 shards"
        );

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(
            installed(&outcome).installed.len(),
            shards_per_pass(256),
            "the pass must visit the DERIVED number of shards. A fixed 8 fails here, which is \
             the arm that ties the derivation to the fold rather than leaving it as arithmetic \
             nothing reads."
        );
        // Asserted as a property of the derivation rather than as an absolute reading of
        // `staged_fold_shard_budget_capped_total`: that counter is process-global, so a sibling
        // test exercising the ceiling would make this pass or fail on test ORDER. (It did, on
        // the first run of this test.)
        assert!(
            shards_per_pass(256) < SHARD_VISITS_PER_PASS_CEILING,
            "256 shards must not hit the ceiling — if one bucket did, the ceiling would be the \
             binding constraint everywhere and the bound unreachable at any size"
        );
    }

    /// The ceiling binds rather than the derivation running away, and it is observable when it
    /// does.
    ///
    /// A shard population that cannot be revisited inside the bound on one instance is a real
    /// deployment (§ 16.6.4 puts it past ~50 buckets), and the design's answer is to say so
    /// loudly rather than to silently do unbounded work per pass. A failure implicates the
    /// `required > SHARD_VISITS_PER_PASS_CEILING` branch in `shards_per_pass`.
    #[test]
    fn the_shard_budget_ceiling_binds_and_is_counted() {
        // Passes per age window, `A/T`. Derived rather than written as 12, so the boundary below
        // tracks the two constants it is a function of.
        let passes = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS).max(1) as usize;
        // The largest population the ceiling can sweep, computed FROM the ceiling. Written as a
        // literal (12,288) this assertion survived the ceiling changing and measured nothing.
        let last_unclipped = SHARD_VISITS_PER_PASS_CEILING * passes;

        let before = shard_budget_capped_total();
        assert_eq!(
            shards_per_pass(last_unclipped),
            SHARD_VISITS_PER_PASS_CEILING,
            "at exactly {} shards ({} buckets) the derivation asks for the ceiling and gets it",
            last_unclipped,
            last_unclipped / 256
        );
        assert_eq!(
            shard_budget_capped_total(),
            before,
            "at exactly the ceiling nothing is clipped, so nothing is counted"
        );
        // One shard more needs one visit more and cannot have it.
        assert_eq!(
            shards_per_pass(last_unclipped + 1),
            SHARD_VISITS_PER_PASS_CEILING
        );
        assert!(
            shard_budget_capped_total() > before,
            "and being clipped must be COUNTED — it means the target age is unreachable here, \
             which an operator cannot infer from a rising age alone"
        );
    }

    /// The ceiling is the value its own derivation gives, so raising [`STAGED_RANGE_BUDGET`]
    /// cannot leave it permitting more visits than the pass budget affords.
    ///
    /// # Why this assertion is new, and what it would have caught
    ///
    /// The ceiling's derivation is `(D·T − S·T/(A·R)) / c_r`, so it is a function of the range
    /// budget: raising `S` moves parse work into the window and leaves fewer visits inside it.
    /// Nothing asserted that. When `S` went from 100,000 to 1,300,000 the bound fell from 1,080
    /// visits to 707 while the constant stayed at 1,024 — a ceiling **45% above its own bound**,
    /// permitting 48 buckets where the capacity arithmetic afforded ~33, and every existing test
    /// stayed green because all of them are pure scheduling arithmetic with no rate in them. That
    /// is a guard that reads as a guard and is not one.
    ///
    /// The two-sided form is what makes it a derivation rather than an inequality: the constant
    /// must fit, and **doubling it must not**, so it is the largest power of two inside the bound
    /// and not merely some value below it.
    ///
    /// A failure implicates [`SHARD_VISITS_PER_PASS_CEILING`] or [`STAGED_RANGE_BUDGET`] — and if
    /// the budget moved, this constant has to move with it rather than this test being relaxed.
    #[test]
    fn the_fold_shard_ceiling_is_what_the_range_budget_affords() {
        // The same inputs as `staged_range_budget_bound_holds_at_the_design_point`, and the same
        // provenance: `R` and `c_r` are measured, `D` and `T` are the design's.
        const D: f64 = 0.167;
        const R_MEASURED: f64 = 2_900.0;
        const C_R: f64 = 0.018024;
        let t = STAGED_FOLD_INTERVAL_SECS as f64;
        let a = MAX_AUTHORITY_AGE_SECS as f64;

        let pass_budget = D * t;
        let parse_term = STAGED_RANGE_BUDGET as f64 * t / (a * R_MEASURED);
        let derived = ((pass_budget - parse_term) / C_R) as usize;

        assert!(
            parse_term < pass_budget,
            "the parse term alone ({parse_term:.1} s) must fit inside the pass budget \
             ({pass_budget:.1} s), or no visits are affordable at all and the range budget is \
             the thing that is wrong"
        );
        assert!(
            SHARD_VISITS_PER_PASS_CEILING <= derived,
            "the ceiling of {SHARD_VISITS_PER_PASS_CEILING} exceeds the {derived} visits the \
             pass budget affords at a range budget of {STAGED_RANGE_BUDGET}. A ceiling above its \
             own bound is not a guard — it permits {} buckets where the arithmetic affords {}.",
            SHARD_VISITS_PER_PASS_CEILING * (a as usize / STAGED_FOLD_INTERVAL_SECS as usize) / 256,
            derived * (a as usize / STAGED_FOLD_INTERVAL_SECS as usize) / 256
        );
        assert!(
            SHARD_VISITS_PER_PASS_CEILING * 2 > derived,
            "the ceiling should be the largest power of two inside the bound; {} also fits \
             within {derived}, so the constant is smaller than the derivation allows and the \
             fold is doing less work per pass than it may",
            SHARD_VISITS_PER_PASS_CEILING * 2
        );

        // The retired value, kept in-line as the red side so the overrun cannot come back
        // quietly. At today's range budget 1,024 must still be shown to FAIL the inequality.
        assert!(
            1_024 > derived,
            "the retired ceiling of 1,024 must still be shown to exceed the {derived} visits \
             affordable at a range budget of {STAGED_RANGE_BUDGET}. If it now fits, the inputs \
             moved again and both constants need re-deriving together."
        );
    }

    /// The budget gates on the **count**, in a state where the count and the byte sum disagree.
    ///
    /// # The divergence, and why it is the whole test
    ///
    /// `staged_range_count` and `staged_byte_sum` normally move together, so an implementation
    /// that gated on bytes would pass every ordinary fixture. Here the shard holds
    /// `STAGED_RANGE_BUDGET` ranges totalling **4,096 bytes** — a byte figure four orders of
    /// magnitude below the count, and nothing at all against any staging bound. A byte-gated
    /// implementation admits; a count-gated one declines.
    ///
    /// This is the shape this repo's six recorded near-misses all failed to have: two figures
    /// that agree in every normal state, asserted in the one state where they part company.
    ///
    /// # The first version of this test was the seventh instance
    ///
    /// It seeded `staged_byte_sum == staged_range_count` — 100,000 ranges and 100,000 bytes —
    /// and asserted the byte sum was "100 KB, nothing against any staging bound", which was
    /// true and irrelevant, because the two figures were the same number. It **passed**
    /// against a `budget_verdict` patched to read `staged_byte_sum()`, which is how the
    /// vacuity was found rather than shipped. The two figures now differ by 24×, so the
    /// substitution reds.
    ///
    /// A failure implicates `staged_range_count_now`'s use of
    /// `ShardCheckpointSet::staged_range_count` and the `count >= STAGED_RANGE_BUDGET`
    /// comparison in `budget_verdict` — and the assertion reads the verdict that comparison
    /// produces, not a figure derived from it.
    #[tokio::test]
    async fn the_budget_gates_on_the_count_not_on_the_byte_sum() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "01");
        // Small enough that a byte-gated comparison against the budget is nowhere near it,
        // and not a round multiple of the count so the two cannot be confused by eye.
        const TINY_BYTES: u64 = 4_096;

        // One range short of the budget.
        seed_shard_figure(dir, &shard, TINY_BYTES, STAGED_RANGE_BUDGET - 1, 5);
        let set = read_shard_checkpoints(dir);
        assert_eq!(set.staged_range_count(), STAGED_RANGE_BUDGET - 1);
        assert_eq!(
            staged_range_count_now(dir).await,
            Some(STAGED_RANGE_BUDGET - 1),
            "the input the comparison reads"
        );
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Admit,
            "one short of the budget must admit, or the boundary is off by one in the \
             direction that declines a workload early"
        );

        // The divergence state: exactly at the budget on COUNT, with 4 KiB of bytes behind it.
        invalidate_budget_snapshot(dir);
        seed_shard_figure(dir, &shard, TINY_BYTES, STAGED_RANGE_BUDGET, 5);
        let set = read_shard_checkpoints(dir);
        assert_eq!(set.staged_range_count(), STAGED_RANGE_BUDGET);
        assert_eq!(
            set.staged_byte_sum(),
            TINY_BYTES,
            "the input that makes this a COUNT test rather than a restatement of the same \
             number twice: the count is at the budget while the byte figure is {TINY_BYTES} — \
             so a byte-gated implementation admits here, and the verdict assertion below is \
             what catches it"
        );
        assert!(
            set.staged_byte_sum() < STAGED_RANGE_BUDGET / 20,
            "and the two figures must be far apart, not merely unequal: byte_sum={} \
             count={STAGED_RANGE_BUDGET}",
            set.staged_byte_sum()
        );
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Decline {
                count: STAGED_RANGE_BUDGET
            },
            "at the budget on count, caching is declined — with the byte bound empty"
        );
        assert!(
            budget_declines_total() > 0,
            "and the decline is COUNTED, because a workload that silently stops being cached \
             is the worst presentation of this bound"
        );
    }

    // =====================================================================
    // § 16.6.12 point 5 — the COUNT trigger for write-cache eviction
    // =====================================================================

    /// Write-cache eviction fires on the **count** in the wedge state, where the byte figure
    /// cannot possibly have fired it.
    ///
    /// # The defect, and why the fixture has to be at the budget rather than merely large
    ///
    /// Admission declines on the count (`budget_verdict`'s `count >= STAGED_RANGE_BUDGET`,
    /// `staged_fold.rs`) while eviction triggered on bytes only
    /// (`CacheManager::get_staging_eviction_thresholds`, `cache.rs`). Nothing in the
    /// classification chain the fold and the evictor share consults an expiry field, so an
    /// expired-but-unread entry keeps its count forever at default config. A small-object
    /// workload therefore parks at the budget with the byte bound nearly empty and
    /// write-through caching declined indefinitely.
    ///
    /// A fixture **below** the budget cannot exercise that: it would be a state where admission
    /// still admits, so the wedge is not present and asserting a `Needed` verdict would assert
    /// nothing about the wedge. So this seeds exactly [`STAGED_RANGE_BUDGET`] ranges and
    /// **asserts that it is inside every other gate**:
    ///
    /// - `budget_verdict` DECLINES — this is genuinely the wedge state, not a nearby one.
    /// - the byte figure is 4,096 — four orders of magnitude below the count, and below any
    ///   staging bound a deployment could configure, so no byte trigger can be the cause of the
    ///   eviction this test demands. Asserted, not asserted-about-in-a-comment.
    ///
    /// A failure implicates the `count <= trigger` comparison in [`count_eviction_verdict`]
    /// (`staged_fold.rs`) — the value `CacheManager::evict_staging_tier` branches on via
    /// `CountEvictionVerdict::is_needed` — and NOT the byte arm, which this fixture cannot
    /// reach.
    #[tokio::test]
    async fn the_count_trigger_fires_in_the_wedge_state_the_byte_trigger_cannot_reach() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "0c");
        const TINY_BYTES: u64 = 4_096;

        seed_shard_figure(dir, &shard, TINY_BYTES, STAGED_RANGE_BUDGET, 5);
        invalidate_budget_snapshot(dir);

        // ---- The fixture is inside every other gate, asserted rather than assumed.
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Decline {
                count: STAGED_RANGE_BUDGET
            },
            "fixture: this must be the WEDGE state — admission declining on the count. A \
             fixture below the budget would exercise a state where nothing is wedged, so the \
             eviction assertion below would prove nothing about the defect."
        );
        let set = read_shard_checkpoints(dir);
        assert_eq!(
            set.staged_byte_sum(),
            TINY_BYTES,
            "fixture: the byte figure must be tiny, so the count assertion below cannot be \
             satisfied by a byte trigger. count={STAGED_RANGE_BUDGET} bytes={TINY_BYTES}"
        );
        assert!(
            set.staged_byte_sum() < STAGED_RANGE_BUDGET / 20,
            "fixture: and far apart rather than merely unequal — byte_sum={}",
            set.staged_byte_sum()
        );

        // ---- The assertion the defect fails.
        let (want_trigger, want_target) = count_eviction_thresholds(95, 80);
        assert_eq!(
            count_eviction_verdict(dir, "host:1", 95, 80).await,
            CountEvictionVerdict::Needed {
                count: STAGED_RANGE_BUDGET,
                observed_count: STAGED_RANGE_BUDGET,
                budget: STAGED_RANGE_BUDGET,
                trigger: want_trigger,
                target: want_target,
                ranges_to_free: STAGED_RANGE_BUDGET - want_target,
            },
            "at the budget on COUNT, with the byte bound empty, a write-cache eviction pass \
             must be needed. Before § 16.6.12 point 5 nothing triggered on the count at all, \
             so this state produced an indefinite decline with no reclamation."
        );
    }

    /// 95/80 **hysteresis**, derived from the budget constant at runtime — not
    /// one-out-per-one-in at the limit, which Ed ruled out on 2026-09-07.
    ///
    /// # The discriminating value is 90%, and it is the whole test
    ///
    /// 90% of the budget is **above the target** and **below the trigger**. An implementation
    /// with no hysteresis — trigger at the budget, or trigger equal to target — fires there;
    /// a 95/80 one does not, and must not, because every boundary upload would otherwise pay
    /// for an eviction, which is the thrash R3.4 exists to prevent.
    ///
    /// Asserting at a value adjacent to the target instead (say 81%) would pass against a
    /// no-hysteresis build too, which is this repo's vacuity trap 1.
    ///
    /// # Which assertion reds against which defect — measured, not assumed
    ///
    /// Three red sides were taken. Two of them red an **earlier** assertion than the in-band
    /// one, and that is worth recording rather than glossing:
    ///
    /// | Patched build | First assertion to red |
    /// |---|---|
    /// | thresholds both at the budget | `trigger < STAGED_RANGE_BUDGET` |
    /// | trigger collapsed onto the target | `target < trigger` |
    /// | verdict reads `target` where it should read `trigger` | the 90% in-band assertion |
    ///
    /// So the in-band assertion is **redundant against the two threshold-shaped defects** —
    /// the exact-pair equality catches those first — and is the **only** thing that catches the
    /// third, where the thresholds are correct and the comparison reads the wrong one of them.
    /// It is not decoration, and it is also not the discriminator for "no hysteresis" that a
    /// first reading suggests. Anyone loosening the exact-pair equality should read the in-band
    /// assertion as carrying more weight from that moment on.
    ///
    /// A failure implicates the two multiplications in [`count_eviction_thresholds`] and the
    /// `count <= trigger` comparison in [`count_eviction_verdict`], both in `staged_fold.rs`.
    #[tokio::test]
    async fn the_count_trigger_has_hysteresis_and_is_not_one_out_per_one_in() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "0d");

        let (trigger, target) = count_eviction_thresholds(95, 80);
        // The thresholds themselves must be a proportion of the budget rather than the budget.
        assert!(
            trigger < STAGED_RANGE_BUDGET,
            "the trigger must fire BEFORE the budget, or eviction only starts once admission \
             is already declining: trigger={trigger} budget={STAGED_RANGE_BUDGET}"
        );
        assert!(
            target < trigger,
            "and there must be a GAP: trigger={trigger} target={target}. Equal values are \
             one-out-per-one-in, which Ed ruled out."
        );
        assert_eq!(
            (trigger, target),
            (
                (STAGED_RANGE_BUDGET as f64 * 0.95) as u64,
                (STAGED_RANGE_BUDGET as f64 * 0.80) as u64
            ),
            "the pair must be 95% and 80% OF THE BUDGET CONSTANT, read at runtime"
        );

        // Below the trigger and above the target: the hysteresis band. A no-hysteresis build
        // fires here.
        let in_band = (STAGED_RANGE_BUDGET as f64 * 0.90) as u64;
        assert!(
            in_band > target && in_band < trigger,
            "fixture: {in_band} must sit strictly inside the band ({target}, {trigger}), or \
             this test cannot discriminate a missing gap"
        );
        seed_shard_figure(dir, &shard, 4_096, in_band, 5);
        invalidate_budget_snapshot(dir);
        assert_eq!(
            count_eviction_verdict(dir, "host:1", 95, 80).await,
            CountEvictionVerdict::NotNeeded,
            "inside the hysteresis band nothing evicts — otherwise every upload near the \
             boundary pays for an eviction pass"
        );

        // One past the trigger: it fires.
        seed_shard_figure(dir, &shard, 4_096, trigger + 1, 5);
        invalidate_budget_snapshot(dir);
        assert!(
            count_eviction_verdict(dir, "host:1", 95, 80)
                .await
                .is_needed(),
            "one range past the trigger must fire, or the boundary is off by one in the \
             direction that never evicts"
        );
    }

    /// A pass's reclaim is debited from the count until a fold install lands, and the debit is
    /// reset by that install.
    ///
    /// # Without this the count trigger over-evicts, which is worse than the wedge
    ///
    /// The count comes from the shard checkpoints and moves only when a fold installs. The
    /// consolidation cycle evaluates the trigger every 5 s by default, so an undebited pass
    /// would observe the same stale count and re-ask for `count − target` ranges on every
    /// cycle — draining the whole write tier at a rate set by the eviction candidate cap.
    ///
    /// A failure implicates `record_count_eviction_pass`'s `fetch_add`, the
    /// `observed_count.saturating_sub(reclaimed)` in [`count_eviction_verdict`], or the
    /// `COUNT_RECLAIMED_SINCE_FOLD.store(0, ..)` in [`invalidate_budget_snapshot`] — all in
    /// `staged_fold.rs`.
    #[tokio::test]
    async fn a_count_pass_debits_its_own_reclaim_until_the_next_fold_install() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "0e");
        let (trigger, target) = count_eviction_thresholds(95, 80);

        seed_shard_figure(dir, &shard, 4_096, STAGED_RANGE_BUDGET, 5);
        invalidate_budget_snapshot(dir);
        let first = count_eviction_verdict(dir, "host:1", 95, 80).await;
        let CountEvictionVerdict::Needed { ranges_to_free, .. } = first else {
            panic!("fixture: the trigger must fire before a debit can be tested, got {first:?}");
        };
        assert_eq!(ranges_to_free, STAGED_RANGE_BUDGET - target);

        // The pass reclaimed exactly what it asked for. The checkpoint is UNCHANGED, which is
        // the production state until the next fold — so only the debit can quiet the trigger.
        let (passes, freed) = record_count_eviction_pass(dir, "host:1", ranges_to_free);
        // Per-directory, so these are this fixture's own figures rather than the process's —
        // which is what makes an exact assertion safe here at all. An unkeyed cumulative
        // counter would be the process-global-counter trap, and it fired on the first run of
        // this test before the state was keyed.
        assert_eq!((passes, freed), (1, ranges_to_free));
        assert_eq!(
            count_eviction_verdict(dir, "host:1", 95, 80).await,
            CountEvictionVerdict::NotNeeded,
            "the next evaluation must NOT re-ask for the same ranges. Without the debit the \
             trigger re-fires every consolidation cycle against an unmoved figure, which \
             drains the tier."
        );

        // A fold install resets the debit. The checkpoint here still says budget-full, so the
        // trigger must come back — which is what proves the reset happened rather than the
        // debit being permanent.
        invalidate_budget_snapshot(dir);
        let after_install = count_eviction_verdict(dir, "host:1", 95, 80).await;
        assert!(
            after_install.is_needed(),
            "a fold install clears the debit, so an unmoved checkpoint triggers again. \
             A permanent debit would silently disable the count trigger for the process's \
             lifetime. Got {after_install:?}"
        );
        assert!(
            matches!(
                after_install,
                CountEvictionVerdict::Needed {
                    count: c,
                    observed_count: o,
                    ..
                } if c == STAGED_RANGE_BUDGET && o == STAGED_RANGE_BUDGET
            ),
            "and the debited count equals the observed count again after the reset, so the \
             correction is visible rather than baked in. Got {after_install:?}"
        );
        assert!(
            trigger > 0,
            "sanity: a zero trigger would make every assertion above vacuous"
        );
    }

    /// Two instances each reclaim; the second instance's verdict must count **both**.
    ///
    /// # The defect this closes
    ///
    /// The count comes from the shared shard checkpoints, which do not move until a fold
    /// installs. A peer that reclaimed ranges since then has made the checkpoint figure an
    /// over-statement for **every** instance, not only for itself — and before the reclaim
    /// documents existed the debit was process-local, so this instance would re-ask for the
    /// ranges the peer had already freed and evict them a second time.
    ///
    /// The peer is instance `hostA:1`, and its reclaim reaches this instance **only** as the
    /// document on the shared volume — written here as a literal, the bytes a peer process
    /// writes, so nothing in this process's memory knows about it. This instance (`hostB:2`)
    /// records its own reclaim through the production call. The fixture leaves the debited count
    /// above the trigger on purpose, so the assertion is on the exact arithmetic rather than on
    /// a `NotNeeded` that a larger debit from either side alone could also produce.
    ///
    /// A failure implicates the peer-document sum in [`count_eviction_verdict`] or the reader
    /// [`peer_reclaims_ranges`], both in `staged_fold.rs`.
    #[tokio::test]
    async fn a_peers_reclaim_is_debited_by_this_instances_count_verdict() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "0d");
        let (trigger, _target) = count_eviction_thresholds(95, 80);
        let peer_reclaimed: u64 = 20_000;
        let own_reclaimed: u64 = 10_000;
        // Inside every other gate: with both debits the count is still over the trigger, so the
        // verdict stays `Needed` and its `count` is what discriminates.
        assert!(STAGED_RANGE_BUDGET - peer_reclaimed - own_reclaimed > trigger);

        // No `invalidate_budget_snapshot()` here: the budget memo is keyed by directory, so a
        // fresh fixture cannot hit it, and the unkeyed reset would clear a parallel test's state.
        seed_shard_figure(dir, &shard, 4_096, STAGED_RANGE_BUDGET, 5);

        let reclaims = dir.join("metadata/_shard_state/eviction_reclaims");
        std::fs::create_dir_all(&reclaims).unwrap();
        std::fs::write(
            reclaims.join("hostA:1.json"),
            format!(
                "{{\"schema\":1,\"instance_id\":\"hostA:1\",\"generation\":0,\
                 \"ranges_reclaimed\":{peer_reclaimed},\"recorded_at_unix_nanos\":{}}}",
                now_nanos()
            ),
        )
        .unwrap();

        record_count_eviction_pass(dir, "hostB:2", own_reclaimed);
        let verdict = count_eviction_verdict(dir, "hostB:2", 95, 80).await;
        assert!(
            matches!(
                verdict,
                CountEvictionVerdict::Needed { count, observed_count, .. }
                    if observed_count == STAGED_RANGE_BUDGET
                        && count == STAGED_RANGE_BUDGET - peer_reclaimed - own_reclaimed
            ),
            "this instance's verdict must debit the peer's reclaim as well as its own: expected \
             count {} from observed {}, got {verdict:?}",
            STAGED_RANGE_BUDGET - peer_reclaimed - own_reclaimed,
            STAGED_RANGE_BUDGET
        );
    }

    /// The reclaim document's lifecycle: published by the reclaim, counted by peers and not by
    /// its owner, retired by its owner's fold install, and ignored once past the age bound.
    ///
    /// Read through [`peer_reclaims_ranges`] rather than the verdict so the assertions do not
    /// depend on the process-wide [`CountEvictionState`], which parallel tests reset.
    ///
    /// A failure implicates [`record_count_eviction_pass`]'s publish,
    /// [`invalidate_budget_snapshot_retiring`]'s removal, or the own-exclusion, schema and
    /// age checks in [`peer_reclaims_ranges`], all in `staged_fold.rs`.
    #[tokio::test]
    async fn a_reclaim_document_is_published_retired_and_aged_out() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        // Fresh volume: no `eviction_reclaims/` directory at all reads as nothing to debit.
        assert_eq!(peer_reclaims_ranges(dir, "hostC:3").await, 0);

        record_count_eviction_pass(dir, "hostB:2", 7);
        let path = ReclaimDocument::path(dir, "hostB:2");
        let doc: ReclaimDocument =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(doc.schema, RECLAIM_DOCUMENT_SCHEMA);
        assert_eq!(doc.instance_id, "hostB:2");
        assert!(
            doc.ranges_reclaimed >= 7,
            "cumulative since the install: {doc:?}"
        );
        assert_eq!(
            peer_reclaims_ranges(dir, "hostC:3").await,
            doc.ranges_reclaimed,
            "a peer debits the owner's document"
        );
        assert_eq!(
            peer_reclaims_ranges(dir, "hostB:2").await,
            0,
            "the owner does not debit its own document, which its in-memory field already holds"
        );

        // The owner's install retires it.
        invalidate_budget_snapshot_retiring(dir, Some("hostB:2"));
        assert!(
            !path.exists(),
            "the owner's fold install must remove its document"
        );
        assert_eq!(peer_reclaims_ranges(dir, "hostC:3").await, 0);

        // A stopped instance's document: past the age bound it is no longer debited.
        let aged = ReclaimDocument {
            schema: RECLAIM_DOCUMENT_SCHEMA,
            instance_id: "hostD:4".to_string(),
            generation: 3,
            ranges_reclaimed: 500,
            recorded_at_unix_nanos: now_nanos()
                .saturating_sub((RECLAIM_DOCUMENT_MAX_AGE_SECS + 1) * 1_000_000_000),
        };
        aged.install(dir).unwrap();
        let live = ReclaimDocument {
            instance_id: "hostE:5".to_string(),
            recorded_at_unix_nanos: now_nanos(),
            ranges_reclaimed: 40,
            ..aged.clone()
        };
        live.install(dir).unwrap();
        // A foreign schema and a temp name both read as nothing.
        std::fs::write(
            ReclaimDocument::path(dir, "hostF:6"),
            format!(
                "{{\"schema\":2,\"instance_id\":\"hostF:6\",\"generation\":0,\
                 \"ranges_reclaimed\":9,\"recorded_at_unix_nanos\":{}}}",
                now_nanos()
            ),
        )
        .unwrap();
        // Fresh timestamp and valid schema, so only the name keeps it out.
        std::fs::write(
            ReclaimDocument::dir(dir).join("hostG:7.json.tmp.host.7"),
            format!(
                "{{\"schema\":1,\"instance_id\":\"hostG:7\",\"generation\":0,\
                 \"ranges_reclaimed\":11,\"recorded_at_unix_nanos\":{}}}",
                now_nanos()
            ),
        )
        .unwrap();
        assert_eq!(
            peer_reclaims_ranges(dir, "hostC:3").await,
            40,
            "only the live document counts: the aged one, the foreign schema and the temp do not"
        );
    }

    /// A pass that reclaimed **nothing** stops the trigger re-firing until the next fold install;
    /// a pass that reclaimed **something** does not.
    ///
    /// # What this prevents, and why it is not merely an optimisation
    ///
    /// The count falls only when a fold installs or a reclaim debits it. A tier whose reclaimable
    /// population is empty therefore stays over the trigger indefinitely, and the consolidation
    /// cycle evaluates the trigger every 5 s — so without the latch every cycle would acquire the
    /// **fleet-wide** `global_eviction.lock`, re-read the ledger, free nothing, and release,
    /// starving read-tier eviction of the same lock. That is a worse failure than the wedge this
    /// unit fixes, and it is one the count trigger introduces rather than inherits.
    ///
    /// # Two-sided, because a latch that never lifts silently disables the trigger
    ///
    /// The `> 0` arm is asserted as well as the `== 0` arm. A build that latched on every pass
    /// would pass a one-sided test and then stop evicting on count after its first pass, which is
    /// the wedge again by a new route.
    ///
    /// A failure implicates the `if ranges_reclaimed == 0` guard in
    /// `record_count_eviction_pass`, the `COUNT_PASS_FOUND_NOTHING.load(..)` early return in
    /// [`count_eviction_verdict`], or the `store(false, ..)` in [`invalidate_budget_snapshot`] —
    /// all in `staged_fold.rs`.
    #[tokio::test]
    async fn a_count_pass_that_reclaims_nothing_does_not_re_fire_until_the_next_fold() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "0f");
        seed_shard_figure(dir, &shard, 4_096, STAGED_RANGE_BUDGET, 5);
        invalidate_budget_snapshot(dir);
        assert!(
            count_eviction_verdict(dir, "host:1", 95, 80)
                .await
                .is_needed(),
            "fixture: the trigger must fire first, or the latch has nothing to suppress"
        );

        // A pass that found nothing reclaimable.
        record_count_eviction_pass(dir, "host:1", 0);
        assert_eq!(
            count_eviction_verdict(dir, "host:1", 95, 80).await,
            CountEvictionVerdict::NotNeeded,
            "a pass that reclaimed nothing has established the population is empty; re-firing \
             would take the FLEET eviction lock every consolidation cycle for no yield"
        );

        // A fold install lifts it, because the inputs have changed.
        invalidate_budget_snapshot(dir);
        assert!(
            count_eviction_verdict(dir, "host:1", 95, 80)
                .await
                .is_needed(),
            "and the latch must LIFT on a fold install — a latch that never lifts silently \
             disables the count trigger for the process's lifetime, which is the wedge again"
        );

        // The other side: a pass that reclaimed something must NOT latch. Debit less than the
        // full request so the count is still over the trigger, isolating the latch from the
        // debit — otherwise a passing assertion here could be the debit, not the absent latch.
        let partial = 1u64;
        record_count_eviction_pass(dir, "host:1", partial);
        assert!(
            count_eviction_verdict(dir, "host:1", 95, 80)
                .await
                .is_needed(),
            "partial progress means the population was NOT empty, so the next cycle has real \
             work to do and must not be suppressed"
        );
    }

    /// The pending term is inside the budget, so a burst between two folds cannot exceed it.
    ///
    /// Without this, `staged_range_count` alone is a figure that only moves at fold cadence,
    /// and an unbounded number of ranges could be admitted in between — which is precisely
    /// the count the budget exists to bound.
    ///
    /// A failure implicates the `saturating_add(pending.post_cut(..).len())` in
    /// `staged_range_count_now`.
    #[tokio::test]
    async fn the_budget_counts_post_cut_pending_credits_too() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "02");
        seed_shard_figure(dir, &shard, 4_096, STAGED_RANGE_BUDGET - 1, 5);
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Admit,
            "one short on the folded count alone"
        );

        // One credit above that shard's cut. The seeded document has an empty cut, so any seq
        // is post-cut.
        invalidate_budget_snapshot(dir);
        PendingStore::new(dir.to_path_buf())
            .append(
                "host:1",
                &PendingCredit {
                    seq: 1,
                    bucket: shard.bucket.clone(),
                    l1: shard.l1.clone(),
                    bytes: 1,
                    range: None,
                },
            )
            .await
            .unwrap();
        let set = read_shard_checkpoints(dir);
        let pending = PendingStore::new(dir.to_path_buf()).read_all().await;
        assert_eq!(
            pending.post_cut(&set).len(),
            1,
            "the input: exactly one credit is post-cut"
        );
        assert_eq!(
            staged_range_count_now(dir).await,
            Some(STAGED_RANGE_BUDGET),
            "so the enforced count is folded + pending, not folded alone"
        );
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Decline {
                count: STAGED_RANGE_BUDGET
            },
            "and the budget binds on the sum — otherwise a burst between two folds is \
             unbounded, which is the count this bound exists to hold"
        );
    }

    /// An unreadable input ADMITS. R7.5's fail-open rule, and the direction matters more than
    /// the rule.
    ///
    /// Silently disabling write-through caching is the outage this whole spec exists to fix,
    /// so an unreadable figure must never look like a full budget. Over-admitting for one
    /// interval costs authority age; under-admitting costs the customer their cache.
    ///
    /// A failure implicates `budget_verdict`'s `count == u64::MAX` sentinel branch and
    /// `staged_range_count_now`'s two `is_complete()` guards.
    #[tokio::test]
    #[cfg(unix)]
    async fn an_unreadable_count_admits_rather_than_declines() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "03");
        seed_shard_figure(dir, &shard, 4_096, STAGED_RANGE_BUDGET + 500, 5);
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Decline {
                count: STAGED_RANGE_BUDGET + 500
            },
            "the input: readable and over the budget, so the flip below is caused by the \
             permission change alone"
        );

        invalidate_budget_snapshot(dir);
        let path = StagedShardCheckpoint::path(dir, &shard);
        let restore = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            staged_range_count_now(dir).await,
            None,
            "the input the sentinel is built from: the count is UNKNOWN, not zero"
        );
        assert_eq!(
            budget_verdict(dir).await,
            BudgetVerdict::Admit,
            "an unknown count must admit. The opposite would turn one unreadable file into a \
             fleet-wide silent caching outage — the failure this spec exists to fix."
        );
        std::fs::set_permissions(&path, restore).unwrap();
    }

    /// **The budget denominator is DATA, and this is the test that fails if it becomes text.**
    ///
    /// The requested display is "write cached ranges" over the budget. DR-SCAN may re-derive
    /// [`STAGED_RANGE_BUDGET`] by an order of magnitude, because the scan rate it was sized on
    /// (`R = 212 obj/s`) looks like a concurrency artefact rather than a hardware limit. A
    /// denominator baked into a template, a label, a threshold or a colour band would then
    /// silently misreport the one figure an operator uses to tell whether write-through caching
    /// is about to stop — and it would misreport it in the **reassuring** direction, because a
    /// too-large denominator makes a full tier look empty.
    ///
    /// # What this asserts
    ///
    /// Across the display, export and publication layer — `dashboard.rs`, `otlp.rs`, `metrics.rs`
    /// — and across this file, the literal MUST NOT appear in code. Three exclusions, each
    /// narrow and each stated:
    ///
    /// 1. **The constant's own definition line.** The one place the literal belongs.
    /// 2. **Comments.** § 16.6.4's arithmetic is carried in prose in this file. Prose is not a
    ///    display and cannot be read by a formatter.
    /// 3. **Two named test figures that are not the budget**, listed in `TEST_EXEMPTIONS` with the
    ///    reason each is unrelated. They are exempted by a per-file needle rather than by
    ///    disabling the rule for test code, so a *new* literal in a test still fails — which is
    ///    the half of this rule that is easiest to lose.
    ///
    /// A repo-wide prohibition would be wrong rather than merely strict: `metadata_cache.rs`'s
    /// `max_entries` and `ram_cache.rs`'s access-count fixtures use the same number for unrelated
    /// reasons, and a guard that fails on those would be turned off rather than obeyed.
    ///
    /// # How to make this fail
    ///
    /// Replace `formatNumber(t.range_budget)` in `renderStagedTier` with the number, or add a
    /// colour band at a fixed range count. Either lands the literal in `dashboard.rs`.
    #[test]
    fn the_budget_denominator_is_data_and_never_a_literal() {
        let spellings = BUDGET_SPELLINGS;
        // The display, export and publication layer, plus the fold itself.
        const GUARDED: [&str; 4] = ["dashboard.rs", "otlp.rs", "metrics.rs", "staged_fold.rs"];
        // `(file, needle, why)` — a line in `file` containing `needle` is a figure that is
        // numerically equal to the budget and semantically unrelated to it.
        const TEST_EXEMPTIONS: [(&str, &str, &str); 3] = [
            (
                "metrics.rs",
                "staging_bound_bytes",
                "a BYTE bound (10% of a 1 MB fixture), not a range count",
            ),
            (
                "staged_fold.rs",
                "100k, I=",
                "one row of the authority-age scaling table, which sweeps 100k/1M/10M/100M \
                 ranges; deriving this row from the budget would duplicate a neighbouring row \
                 the moment the budget moved",
            ),
            (
                "staged_fold.rs",
                "S=100k",
                "the S=100k column of § 16.6.4's re-tabulated authority-age table. Same \
                 reasoning as the row above, in the label format SPEC-RETABULATE-16-6-4 \
                 introduced when the table's rows became (backend, I) pairs: S sweeps the \
                 decades 100k/1M/10M/100M, so the 100k cells are a column of a log-scale \
                 sweep and are numerically equal to the budget only by coincidence. Deriving \
                 them from STAGED_RANGE_BUDGET would silently re-scale one column of a \
                 measured table the moment the budget moved, and would make the table's \
                 model-identity check compare the design against itself. \
                 THIS EXEMPTION WAS ADDED BY THE ORCHESTRATOR, not by the unit that wrote the \
                 table: that unit was forbidden cargo, so it could not discover this guard, \
                 and the guard fired correctly at the release gate. Exempting rather than \
                 widening the rule is what the assertion's own message prescribes.",
            ),
        ];

        // A literal match only counts on a digit boundary. Without this, `100_000_000` — the
        // design point, four rows down the same table — matches `100_000` and reports as an
        // offender, and the guard's first version did exactly that.
        fn contains_bare(line: &str, needle: &str) -> bool {
            let bytes = line.as_bytes();
            let mut from = 0usize;
            while let Some(rel) = line[from..].find(needle) {
                let at = from + rel;
                let before_ok = at == 0 || {
                    let c = bytes[at - 1] as char;
                    !c.is_ascii_digit() && c != '_'
                };
                let after = at + needle.len();
                let after_ok = after >= bytes.len() || {
                    let c = bytes[after] as char;
                    !c.is_ascii_digit() && c != '_'
                };
                if before_ok && after_ok {
                    return true;
                }
                from = at + 1;
            }
            false
        }

        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders: Vec<String> = Vec::new();
        let mut definition_lines = 0usize;

        for file in GUARDED {
            let body = std::fs::read_to_string(src.join(file)).unwrap();
            for (n, line) in body.lines().enumerate() {
                let trimmed = line.trim_start();
                // `starts_with` on the trimmed line, not `contains`: this test's own source
                // mentions the declaration in a string and in a failure message, and a `contains`
                // match counted those as definitions.
                let is_definition = trimmed.starts_with("pub const STAGED_RANGE_BUDGET");
                if is_definition {
                    definition_lines += 1;
                }
                if is_definition || trimmed.starts_with("//") {
                    continue;
                }
                // This test's own list of spellings, and the shared const it comes from.
                if line.contains("SPELLINGS") {
                    continue;
                }
                if TEST_EXEMPTIONS
                    .iter()
                    .any(|(f, needle, _)| *f == file && line.contains(needle))
                {
                    continue;
                }
                for needle in spellings {
                    if contains_bare(line, needle) {
                        offenders.push(format!("{file}:{} {}", n + 1, trimmed));
                    }
                }
            }
        }

        // If the definition ever stops matching, the exclusion above stops excluding anything and
        // this test would start reporting the constant itself — a confusing failure. Pin it.
        assert_eq!(
            definition_lines, 1,
            "expected exactly one `pub const STAGED_RANGE_BUDGET` line across {GUARDED:?}; if \
             this fails the exclusion above is no longer matching the definition it means to allow"
        );

        assert!(
            offenders.is_empty(),
            "the staged range budget must be read from STAGED_RANGE_BUDGET at runtime, never \
             spelled. The display's denominator is a payload field the front end formats, so a \
             literal here would misreport the tier the moment DR-SCAN re-derives the budget. \
             If a figure is genuinely unrelated, add it to TEST_EXEMPTIONS with its reason \
             rather than widening the rule. Offenders: {offenders:#?}"
        );
    }

    /// The front end formats the denominator from the payload field, and spells no number.
    ///
    /// This is the assertion that binds the guard above to the actual display. The guard proves
    /// the literal is absent; this proves the field is *present and used*, so a template that
    /// dropped the denominator entirely — passing the guard by omission — still fails.
    #[test]
    fn the_dashboard_formats_the_budget_from_the_payload_field() {
        let js = crate::dashboard::StaticFileHandler::new().get_script_js();
        assert!(
            js.contains("formatNumber(t.range_budget)"),
            "the staged-ranges display must format its denominator from the payload's \
             range_budget field, which carries STAGED_RANGE_BUDGET at runtime"
        );
        assert!(
            js.contains("t.range_count"),
            "and its numerator from the payload's range_count field"
        );
        // Belt and braces against the guard above being scoped away from the JS: the rendered
        // script is one string, so this checks the shipped artefact rather than the source file.
        for needle in BUDGET_SPELLINGS {
            assert!(
                !js.contains(needle),
                "the shipped dashboard script must not spell the budget; found {needle:?}"
            );
        }
    }

    /// The display's denominator is the constant, and the percentage is derived from it.
    ///
    /// Deliberately expressed against [`STAGED_RANGE_BUDGET`] rather than against a number, so
    /// this test cannot pass a build whose budget has changed while its display has not. The
    /// arithmetic is stated as a *fraction of the constant* for the same reason: `budget / 4`
    /// is 25% whatever the budget is, where `25_000` is 25% of exactly one budget.
    #[test]
    fn the_summary_carries_the_budget_and_derives_the_percentage_from_it() {
        let quarter = STAGED_RANGE_BUDGET / 4;
        let gauges = StagedFoldGauges {
            staged_range_budget: STAGED_RANGE_BUDGET,
            staged_range_count: Some(quarter),
            staged_fold_bytes: Some(4096),
            staged_shard_authority_age_secs: Some(1),
            staged_authority_max_age_secs: MAX_AUTHORITY_AGE_SECS,
            ..Default::default()
        };
        let s = gauges.summary();
        assert_eq!(
            s.range_budget, STAGED_RANGE_BUDGET,
            "the payload's denominator IS the constant — this is the field the front end formats"
        );
        let percent = s
            .range_budget_percent
            .expect("a known count yields a percentage");
        assert!(
            (percent - 25.0).abs() < 0.01,
            "the percentage must be derived from the count and the budget, not stored: \
             got {percent} for {quarter} of {STAGED_RANGE_BUDGET}"
        );
        assert_eq!(s.completeness, StagedCompleteness::Current);
    }

    /// The three-state completeness rule, all three states, on one fixture per state.
    ///
    /// § 16.6.7's point is that **Unknown** must not render as a number: "publishing a precise
    /// occupancy derived from a partial fold gives the operator a confident wrong number, which is
    /// worse than showing nothing." So the Unknown arm asserts the bytes are absent as well as the
    /// verdict, because a verdict nobody reads next to a number they do read is not a guard.
    #[test]
    fn the_three_completeness_states_are_distinguished() {
        let base = StagedFoldGauges {
            staged_authority_max_age_secs: MAX_AUTHORITY_AGE_SECS,
            ..Default::default()
        };

        let unknown = StagedFoldGauges {
            staged_fold_bytes: None,
            staged_shard_authority_age_secs: Some(1),
            ..base.clone()
        };
        assert_eq!(unknown.completeness(), StagedCompleteness::Unknown);
        assert_eq!(
            unknown.summary().staged_bytes,
            None,
            "Unknown must not publish a byte figure at all"
        );

        let current = StagedFoldGauges {
            staged_fold_bytes: Some(4096),
            staged_shard_authority_age_secs: Some(MAX_AUTHORITY_AGE_SECS),
            ..base.clone()
        };
        assert_eq!(
            current.completeness(),
            StagedCompleteness::Current,
            "the bound is inclusive: exactly at Max_Authority_Age is still current"
        );

        let stale = StagedFoldGauges {
            staged_fold_bytes: Some(4096),
            staged_shard_authority_age_secs: Some(MAX_AUTHORITY_AGE_SECS + 1),
            ..base
        };
        assert_eq!(stale.completeness(), StagedCompleteness::StaleButValid);
        assert_eq!(
            stale.summary().staged_bytes,
            Some(4096),
            "Stale but valid IS a figure — a valid statement about the past, not garbage"
        );
    }

    /// The authority memo serves repeat reads, and a fold install invalidates it.
    ///
    /// Both halves matter and the second is the load-bearing one: a memo that outlived an install
    /// by its TTL would hide a figure the fold had just changed, which is the one direction
    /// § 16.6.2 rules out for the staging trigger.
    #[tokio::test]
    async fn the_authority_memo_serves_repeats_and_an_install_invalidates_it() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path();
        invalidate_authority_snapshot(dir);

        seed_shard_figure_for_tests(dir, "bucket", "ab", 4096, 1);
        let (hits_before, _) = authority_snapshot_counters();
        let first = staged_authority_figure(dir)
            .await
            .expect("a seeded shard is readable");
        assert_eq!(first.resident_staged_bytes, 4096);
        assert!(
            authority_snapshot_age_secs(dir).is_some(),
            "a live memo must report an age, so 'served from a memo' is observable"
        );

        // Change the figure on disk WITHOUT an install. The memo must still be serving, which is
        // the property that makes it a memo rather than a no-op.
        seed_shard_figure_for_tests(dir, "bucket", "ab", 8192, 1);
        let second = staged_authority_figure(dir).await.expect("still readable");
        assert_eq!(
            second.resident_staged_bytes, 4096,
            "within the TTL the memoised figure is served, which is the cost saving — and this \
             value is itself the evidence, because 4096 is no longer derivable from disk"
        );
        let (hits_after, _) = authority_snapshot_counters();
        assert!(
            hits_after > hits_before,
            "the second read must be recorded as a hit so the hit/recompute ratio is meaningful"
        );

        // An install invalidates, so the changed figure is visible immediately rather than after
        // the TTL.
        invalidate_authority_snapshot(dir);
        let third = staged_authority_figure(dir).await.expect("still readable");
        assert_eq!(
            third.resident_staged_bytes, 8192,
            "after an install the next read must see the new figure, not the memoised one"
        );
    }

    /// (`STAGED-FOLD-CLAIM-NOT-RELEASED-AFTER-INSTALL-DELAYS-DIRTY-SHARDS`): a shard a peer
    /// has just installed is visited by an instance that has it dirty, at that instance's next
    /// pass.
    ///
    /// A installs shard S. B then removes S's staged object, marks S dirty and runs one pass.
    /// S is the only shard, so B's pass has nothing else to visit and nothing to rank it behind:
    /// the only thing that can keep B off S is A's claim.
    ///
    /// # RED SIDE
    ///
    /// With no release after the install, A's claim is live for `CLAIM_LEASE_SECS`, B's pass
    /// reports S busy and installs nothing, and the figure still counts the removed object (
    /// a dirty shard left at 575.6 s instead of ~280 s).
    #[tokio::test]
    async fn a_shard_a_peer_just_installed_is_visited_by_an_instance_that_has_it_dirty() {
        const TARGET_BYTES: u64 = 32_768;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        invalidate_authority_snapshot(dir);
        let ledger_a = WriteLedger::new(dir.to_path_buf(), "host-a:1".to_string());
        let ledger_b = WriteLedger::new(dir.to_path_buf(), "host-b:2".to_string());

        let target = "bucket/scn2-claim-release-target.bin";
        let shard = {
            let (b, l) = shard_of(target).unwrap();
            Shard::new(b, l)
        };
        publish_staged_range(dir, &ledger_a, target, TARGET_BYTES).await;
        let only: BTreeSet<Shard> = [shard.clone()].into_iter().collect();
        let a_pass = run_staged_fold_over_shards(dir, &ledger_a, "host-a:1", &only).await;
        assert!(
            installed(&a_pass)
                .installed
                .iter()
                .any(|c| c.shard() == shard),
            "PRECONDITION: A must install the shard"
        );
        let before = staged_authority_figure_uncached(dir)
            .await
            .unwrap()
            .resident_staged_bytes;
        assert_eq!(
            before, TARGET_BYTES,
            "PRECONDITION: A's figure counts the object"
        );

        // B removes the object, as a DELETE does: the `.meta` goes, then B's mark.
        let meta =
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), target, ".meta").unwrap();
        std::fs::remove_file(&meta).unwrap();
        mark_shard_dirty(dir, "host-b:2", target);

        let b_pass = run_staged_fold(dir, &ledger_b, "host-b:2").await;
        let summary = b_pass.summary();
        let b_visited = summary.installed.iter().any(|c| c.shard() == shard);
        let b_busy = summary.busy.contains(&shard);
        let after = staged_authority_figure_uncached(dir)
            .await
            .unwrap()
            .resident_staged_bytes;
        let claim_left = std::fs::read_to_string(ShardClaim::path(dir, &shard)).ok();
        println!(
            "SCN2 claim release: b_installed={} b_visited={} b_busy={} before={} after={} \
             claim_file_after_b_pass={:?}",
            b_pass.installed(),
            b_visited,
            b_busy,
            before,
            after,
            claim_left
        );
        assert!(
            b_visited && !b_busy,
            "B has the shard dirty and it is the only shard, but B's pass did not visit it \
             (busy={b_busy}): A's claim outlived A's install"
        );
        assert_eq!(
            after, 0,
            "B's visit must drop the removed object from the figure"
        );
    }

    /// dirty-shard absorption per pass does not shrink with the 24 h window.
    ///
    /// At the fleet's population (N ≈ 1,500) the hourly window gave a budget of 126 and 126
    /// extras. The 24 h window gives a budget of 6, so an extras cap that scaled with the budget
    /// would absorb 6 dirty shards a pass, and a 25-shard burst would take five passes to leave
    /// the figure instead of one. The assertion is on the extras granted, the value the pass loop
    /// branches on, not on the budget.
    ///
    /// # RED SIDE
    ///
    /// Revert `extra_dirty_visit_cap` to `budget.min(CEILING - budget)`: at N = 1,500 it returns
    /// 6 and the first assertion fails.
    #[test]
    fn dirty_absorption_does_not_shrink_with_the_window() {
        const FLEET_N: usize = 1_500;
        const HOURLY_EXTRAS_AT_FLEET_N: usize = 126;
        const BURST: usize = 25;
        let budget = shards_per_pass(FLEET_N);
        assert_eq!(budget, 6, "fixture: ceil(1500 / 288)");
        let extras = extra_dirty_visit_cap(budget);
        assert!(
            extras >= HOURLY_EXTRAS_AT_FLEET_N,
            "at N = {FLEET_N} a pass may visit only {extras} dirty shards beyond its budget of \
             {budget}; the hourly window allowed {HOURLY_EXTRAS_AT_FLEET_N}"
        );
        assert!(
            extras >= BURST,
            "a measured 25-shard dirty burst must clear in one pass: {extras} extras"
        );
        // The ceiling still bounds a pass at every population the ceiling can sweep.
        let passes = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS) as usize;
        for n in [
            1,
            256,
            FLEET_N,
            25_600,
            SHARD_VISITS_PER_PASS_CEILING * passes,
        ] {
            let b = shards_per_pass(n);
            assert!(
                b + extra_dirty_visit_cap(b) <= SHARD_VISITS_PER_PASS_CEILING,
                "at N = {n}, budget {b} plus extras exceeds the per-pass ceiling"
            );
        }
    }

    /// (`DIRTY-SHARE-HAS-NO-SLACK-AT-FLEET-POPULATION`): a dirty shard the slack cannot admit
    /// is still visited at the next pass, as an extra visit, and the stalest-first visits are
    /// the ones the pass would have made without it.
    ///
    /// `shards_per_pass(N) = ceil(N / P)` with `P` passes per window, so at steady state the
    /// single-instance schedule has `P × B − N` slots of slack: none at `N = P × B`, B = 2. The
    /// fixture builds that steady state on purpose — ages by stalest-first group `g = rank / B` of
    /// `A − 10 − 300·g` seconds, which is feasible for "B stalest now, then B per pass" with 10 s
    /// to spare and infeasible for any `h ≥ 1` — and asserts `dirty_share` returns 0 on the ages
    /// the pass will read.
    ///
    /// # RED SIDE
    ///
    /// Without the extra-visit arm the target's shard is not visited and the figure does not
    /// move: the drop is 0 against the target's staged bytes, and the dirty gauge stays 1.
    #[tokio::test]
    async fn a_dirty_shard_the_slack_cannot_admit_is_visited_as_an_extra() {
        const TARGET_BYTES: u64 = 65_536;
        const PASSES: usize = (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS) as usize;
        const N: usize = PASSES * 2;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        invalidate_authority_snapshot(dir);
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        let target = "bucket/scn3-extra-target.bin";
        let target_shard = {
            let (b, l) = shard_of(target).unwrap();
            Shard::new(b, l)
        };
        publish_staged_range(dir, &ledger, target, TARGET_BYTES).await;
        let only: BTreeSet<Shard> = [target_shard.clone()].into_iter().collect();
        installed(&run_staged_fold_over_shards(dir, &ledger, "host:1", &only).await);

        // N - 1 empty filler shards plus the target's.
        let fillers: Vec<Shard> = (0u32..)
            .map(|v| {
                Shard::new(
                    format!("{}{}", target_shard.bucket, v / 256),
                    format!("{:02x}", v % 256),
                )
            })
            .filter(|s| *s != target_shard)
            .take(N - 1)
            .collect();
        let budget = shards_per_pass(N);
        assert_eq!(budget, 2, "fixture: shards_per_pass(2 x passes_per_window)");
        assert_eq!(PASSES * budget, N, "fixture: zero slack by construction");
        let age_for_rank = |rank: usize| MAX_AUTHORITY_AGE_SECS - 10 - 300 * (rank / budget) as u64;
        for (rank, shard) in fillers.iter().enumerate() {
            seed_shard_figure(dir, shard, 0, 0, age_for_rank(rank));
        }
        // The target's shard is the freshest: the last rank.
        let mut c = StagedShardCheckpoint::load(dir, &target_shard)
            .unwrap()
            .expect("the target's shard was folded");
        c.staged_scanned_at = now_nanos().saturating_sub(age_for_rank(N - 1) * 1_000_000_000);
        c.install(dir).unwrap();

        // The pass's own inputs: stalest-first order and the ages `dirty_share` reads.
        let previous = read_shard_checkpoints(dir);
        let mut ordered: Vec<(u64, Shard)> = previous
            .iter()
            .map(|(s, c)| (c.staged_scanned_at, s.clone()))
            .collect();
        ordered.sort();
        assert_eq!(ordered.len(), N, "fixture: the observed population");
        let ages: Vec<u64> = ordered.iter().map(|(at, _)| age_secs(*at)).collect();
        assert_eq!(
            dirty_share(&ages, budget, 1),
            0,
            "fixture: the slack must admit no dirty shard (ages {ages:?})"
        );
        let stalest_first: BTreeSet<Shard> = ordered
            .iter()
            .take(budget)
            .map(|(_, s)| s.clone())
            .collect();
        assert!(
            !stalest_first.contains(&target_shard),
            "fixture: the target's shard must not be in the next pass's stalest-first set"
        );

        // DELETE, as production does it: the `.meta` goes, then the mark.
        let meta =
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), target, ".meta").unwrap();
        std::fs::remove_file(&meta).unwrap();
        mark_shard_dirty(dir, "host:1", target);
        assert_eq!(dirty_shard_count(dir), 1);
        let before = staged_authority_figure_uncached(dir)
            .await
            .unwrap()
            .resident_staged_bytes;

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        let visited: BTreeSet<Shard> = installed(&outcome)
            .installed
            .iter()
            .map(|c| c.shard())
            .collect();
        let after = staged_authority_figure_uncached(dir)
            .await
            .unwrap()
            .resident_staged_bytes;
        println!(
            "SCN3 zero slack: before={before} after={after} expected_drop={TARGET_BYTES} \
             budget={budget} visited={} target_visited={} dirty_after={}",
            visited.len(),
            visited.contains(&target_shard),
            dirty_shard_count(dir)
        );
        assert_eq!(
            before.saturating_sub(after),
            TARGET_BYTES,
            "a DELETE whose shard the slack cannot admit must leave the figure at the next pass \
             (before={before} after={after}, visited {visited:?})"
        );
        assert_eq!(dirty_shard_count(dir), 0, "the extra visit clears the mark");
        let stalest_visited: BTreeSet<Shard> = visited
            .iter()
            .filter(|s| **s != target_shard)
            .cloned()
            .collect();
        assert_eq!(
            stalest_visited, stalest_first,
            "the stalest-first visits must be exactly the set the pass makes without a dirty \
             shard: the extra visit must not take a budget slot"
        );
    }

    /// (`STAGED-AUTHORITY-READ-MAY-TEAR-ACROSS-A-PEER-INSTALL`): a peer's install and the
    /// credit owner's pending GC landing between the reader's two reads must never make the
    /// authority figure fall below the true staged sum.
    ///
    /// The figure is `F + post-cut pending + P`, and the two inputs are read one after the
    /// other. A credit leaves the pending file only through its owner's GC (`PendingStore::gc`,
    /// run at the end of the owner's own fold pass over EVERY installed cut, including a
    /// peer's), and only after some checkpoint's cut covers it. If the reader takes the
    /// checkpoints first and the pending file after that GC, a credit between the old cut and
    /// the new one is in neither source.
    ///
    /// The fixture reaches that state with the production functions: host:1 stages X and folds
    /// shard S; host:1 then stages Y (also S) and Z (shard S2), unfolded. Inside the reader's
    /// window host:2 folds S (its cut for host:1 now covers Y) and host:1 folds S2, whose GC
    /// then drops both Y's and Z's records.
    ///
    /// # RED SIDE
    ///
    /// With `staged_authority_figure_uncached` reading the checkpoints ahead of the pending files
    /// (the order before the change), the torn figure is X alone: `truth=28672 torn=4096`.
    #[tokio::test]
    async fn a_peer_install_between_the_authority_reads_never_under_counts() {
        const X: u64 = 4096;
        const Y: u64 = 8192;
        const Z: u64 = 16384;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        invalidate_authority_snapshot(&dir);
        let a = WriteLedger::new(dir.clone(), "host:1".to_string());

        let x_key = "bucket/scn3-tear-x.bin";
        let s = {
            let (b, l) = shard_of(x_key).unwrap();
            Shard::new(b, l)
        };
        let y_key = (0u64..)
            .map(|i| format!("bucket/scn3-tear-y-{i}.bin"))
            .find(|k| {
                let (b, l) = shard_of(k).unwrap();
                Shard::new(b, l) == s
            })
            .unwrap();
        let (z_key, s2) = (0u64..)
            .map(|i| format!("bucket/scn3-tear-z-{i}.bin"))
            .find_map(|k| {
                let (b, l) = shard_of(&k).unwrap();
                let shard = Shard::new(b, l);
                (shard != s).then_some((k, shard))
            })
            .unwrap();

        publish_staged_range(&dir, &a, x_key, X).await;
        let only_s: BTreeSet<Shard> = [s.clone()].into_iter().collect();
        installed(&run_staged_fold_over_shards(&dir, &a, "host:1", &only_s).await);
        let seq_y = publish_staged_range(&dir, &a, &y_key, Y).await;
        publish_staged_range(&dir, &a, &z_key, Z).await;
        let truth = X + Y + Z;
        assert_eq!(
            staged_authority_figure_uncached(&dir)
                .await
                .unwrap()
                .resident_staged_bytes,
            truth,
            "fixture: before the tear the figure is X folded plus Y and Z pending"
        );

        // host:1's pass inside the window reads its ledger and appends nothing, so a second
        // handle on the same instance id is equivalent to `a`.
        let hook_a = WriteLedger::new(dir.clone(), "host:1".to_string());
        let (hook_dir, hook_s, hook_s2, hook_s_shard) =
            (dir.clone(), only_s, s2.clone(), s.clone());
        set_authority_between_reads_hook_for_tests(
            &dir,
            Box::new(move || {
                Box::pin(async move {
                    // host:1's claim on S from its first pass would make S busy for host:2;
                    // a lease that has run out is what lets a peer take the shard.
                    let _ = std::fs::remove_file(ShardClaim::path(&hook_dir, &hook_s_shard));
                    let b = WriteLedger::new(hook_dir.clone(), "host:2".to_string());
                    installed(&run_staged_fold_over_shards(&hook_dir, &b, "host:2", &hook_s).await);
                    let only_s2: BTreeSet<Shard> = [hook_s2].into_iter().collect();
                    installed(
                        &run_staged_fold_over_shards(&hook_dir, &hook_a, "host:1", &only_s2).await,
                    );
                })
            }),
        );
        let torn = staged_authority_figure_uncached(&dir)
            .await
            .unwrap()
            .resident_staged_bytes;

        let set = read_shard_checkpoints(&dir);
        let peer_cut = set
            .get(&s)
            .and_then(|c| c.staged_fold_cut.get("host:1").copied())
            .unwrap_or(0);
        assert!(
            peer_cut >= seq_y && set.get(&s).unwrap().staged_scanned_by == "host:2",
            "fixture: host:2's install of S must cover Y (cut={peer_cut}, seq_y={seq_y})"
        );
        let remaining = PendingStore::new(dir.clone()).read_all().await;
        assert!(
            remaining.credits.is_empty(),
            "fixture: host:1's GC must have dropped Y's and Z's records: {:?}",
            remaining.credits
        );
        let settled = staged_authority_figure_uncached(&dir)
            .await
            .unwrap()
            .resident_staged_bytes;
        println!("SCN3 tear: truth={truth} torn={torn} settled={settled}");
        assert_eq!(
            settled, truth,
            "fixture: the installs are correct once the reader is not straddling them"
        );
        assert!(
            torn >= truth,
            "a peer install plus the owner's pending GC between the reader's two reads made \
             the authority figure {torn}, below the true staged sum {truth}"
        );
    }

    /// (`STAGED-FOLD-GAUGE-SET-READS-CHECKPOINTS-BEFORE-PENDING`): the same tear as the one above,
    /// through the `/metrics` gauge set's production caller rather than the authority.
    ///
    /// `MetricsManager::collect_metrics` reads the checkpoint set once per collection and hands it
    /// to `collect_staged_fold_gauges`, which reads the pending files. The earlier fixture, with the
    /// peer install and the owner's GC put between those two reads by the gauge seam, must not
    /// make `write_cache.staged_fold_bytes` fall below the true staged sum.
    ///
    /// # RED SIDE
    ///
    /// With the caller reading checkpoints before pending (the order before the change) the gauge is X
    /// alone: `truth=28672 torn=4096`.
    #[tokio::test]
    async fn a_peer_install_between_the_gauge_reads_never_under_counts() {
        const X: u64 = 4096;
        const Y: u64 = 8192;
        const Z: u64 = 16384;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let manager =
            std::sync::Arc::new(crate::cache::zero_length_arm_orphan_tests::setup(&dir).await);
        let mut metrics_manager = crate::metrics::MetricsManager::new();
        metrics_manager.set_cache_manager(manager.clone());
        let gauge = |m: &crate::metrics::SystemMetrics| m.write_cache.staged_fold.staged_fold_bytes;

        let a = WriteLedger::new(dir.clone(), "host:1".to_string());
        let x_key = "bucket/scn4-gauge-x.bin";
        let s = {
            let (b, l) = shard_of(x_key).unwrap();
            Shard::new(b, l)
        };
        let y_key = (0u64..)
            .map(|i| format!("bucket/scn4-gauge-y-{i}.bin"))
            .find(|k| {
                let (b, l) = shard_of(k).unwrap();
                Shard::new(b, l) == s
            })
            .unwrap();
        let (z_key, s2) = (0u64..)
            .map(|i| format!("bucket/scn4-gauge-z-{i}.bin"))
            .find_map(|k| {
                let (b, l) = shard_of(&k).unwrap();
                let shard = Shard::new(b, l);
                (shard != s).then_some((k, shard))
            })
            .unwrap();

        publish_staged_range(&dir, &a, x_key, X).await;
        let only_s: BTreeSet<Shard> = [s.clone()].into_iter().collect();
        installed(&run_staged_fold_over_shards(&dir, &a, "host:1", &only_s).await);
        let seq_y = publish_staged_range(&dir, &a, &y_key, Y).await;
        publish_staged_range(&dir, &a, &z_key, Z).await;
        let truth = X + Y + Z;
        assert_eq!(
            gauge(&metrics_manager.collect_metrics().await),
            Some(truth),
            "fixture: before the tear the gauge is X folded plus Y and Z pending"
        );

        let hook_a = WriteLedger::new(dir.clone(), "host:1".to_string());
        let (hook_dir, hook_s, hook_s2, hook_s_shard) =
            (dir.clone(), only_s, s2.clone(), s.clone());
        set_gauge_between_reads_hook_for_tests(
            &dir,
            Box::new(move || {
                Box::pin(async move {
                    let _ = std::fs::remove_file(ShardClaim::path(&hook_dir, &hook_s_shard));
                    let b = WriteLedger::new(hook_dir.clone(), "host:2".to_string());
                    installed(&run_staged_fold_over_shards(&hook_dir, &b, "host:2", &hook_s).await);
                    let only_s2: BTreeSet<Shard> = [hook_s2].into_iter().collect();
                    installed(
                        &run_staged_fold_over_shards(&hook_dir, &hook_a, "host:1", &only_s2).await,
                    );
                })
            }),
        );
        let torn = gauge(&metrics_manager.collect_metrics().await);
        assert!(
            GAUGE_BETWEEN_READS_HOOK
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&dir_tag(&dir))
                .is_none(),
            "fixture: the collection must have run the gauge seam"
        );

        let set = read_shard_checkpoints(&dir);
        let peer_cut = set
            .get(&s)
            .and_then(|c| c.staged_fold_cut.get("host:1").copied())
            .unwrap_or(0);
        assert!(
            peer_cut >= seq_y && set.get(&s).unwrap().staged_scanned_by == "host:2",
            "fixture: host:2's install of S must cover Y (cut={peer_cut}, seq_y={seq_y})"
        );
        let remaining = PendingStore::new(dir.clone()).read_all().await;
        assert!(
            remaining.credits.is_empty(),
            "fixture: host:1's GC must have dropped Y's and Z's records: {:?}",
            remaining.credits
        );
        let settled = gauge(&metrics_manager.collect_metrics().await);
        println!("SCN4 gauge tear: truth={truth} torn={torn:?} settled={settled:?}");
        assert_eq!(
            settled,
            Some(truth),
            "fixture: the installs are correct once the reader is not straddling them"
        );
        assert!(
            torn.is_some_and(|t| t >= truth),
            "a peer install plus the owner's pending GC between the gauge set's two reads made \
             write_cache.staged_fold_bytes {torn:?}, below the true staged sum {truth}"
        );
    }

    // ================================================================================
    // § 16.6.13 — the fold's candidate reads are concurrent
    //
    // The fold is not a tree walk (the `.meta` path is computed from the cache key), so none of
    // T4-T6's traversal work touches it. What it shares with the sweep is the operation: one
    // `.meta` read-plus-parse per candidate, on the same shared volume, against the same measured
    // knee. That loop ran at concurrency 1, and it is the `S/R` term of § 16.6.6's
    // reachable-bucket ceiling — the only term in that expression this workstream can move.
    // ================================================================================

    /// `want` cache keys that all hash to the SAME shard, plus that shard.
    ///
    /// # Why a search rather than chosen names
    ///
    /// The shard is `(bucket, L1)` and `L1` is hex digits 0..2 of the key's BLAKE3, so a fixture
    /// cannot pick its own shard — it has to look for keys that land where it needs them. Keys in
    /// **one** shard are what this test needs: the candidate loop is nested inside the per-shard
    /// loop, so candidates spread across shards would be folded one shard at a time and the
    /// concurrency under test would never see more than one candidate at once. Every other fold
    /// fixture in this module deliberately spreads keys across shards to exercise partitioning,
    /// which is exactly why none of them can observe this.
    fn keys_sharing_one_shard(want: usize) -> (Shard, Vec<String>) {
        let mut by_l1: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for i in 0..20_000u32 {
            let name = format!("cand-{i}.bin");
            let key = format!("bucket/{name}");
            let (bucket, l1) = match shard_of(&key) {
                Some(pair) => pair,
                None => continue,
            };
            let slot = by_l1.entry(l1.clone()).or_default();
            slot.push(key);
            if slot.len() >= want {
                return (Shard::new(bucket, l1), slot.clone());
            }
        }
        panic!(
            "fixture precondition: no L1 among 20,000 generated keys held {want} keys, so the \
             candidate loop cannot be given more than one candidate at a time and this test would \
             measure a single-candidate fold"
        );
    }

    /// The fold keeps several `.meta` reads OUTSTANDING, and sums every one of them.
    ///
    /// # The assertion is the outstanding count, not a duration
    ///
    /// `STAGED_FOLD_CANDIDATE_CONCURRENCY` controls how many reads are in flight, so that is the
    /// value to assert. A wall-clock comparison against a serial fold would be a timing assertion on
    /// a local filesystem — a flake, and one that would pass on a machine fast enough to hide the
    /// difference.
    ///
    /// The peak is read from [`FoldPassSummary`], which is scoped to **this pass**. The process-wide
    /// gauge could not carry this assertion: it is monotonic and tests here run concurrently in one
    /// process, so any other test's fold would raise it and this test would pass whether or not the
    /// loop under test was concurrent.
    ///
    /// # The sum is the other half, and it is the half that must not regress
    ///
    /// Concurrency is worth nothing if the fan-out drops a candidate, and a dropped candidate lowers
    /// `F`, which biases the staging eviction trigger LATE — the direction § 16.6.2 rules out by
    /// name. So the installed figure is asserted against the exact byte total, not against a floor.
    ///
    /// A failure in the peak implicates the `buffer_unordered(STAGED_FOLD_CANDIDATE_CONCURRENCY)`
    /// call in the candidate loop. A failure in the sum implicates the `Evictable` arm of the loop
    /// that consumes `folded`.
    #[tokio::test]
    async fn the_fold_keeps_several_candidate_reads_outstanding_and_sums_all_of_them() {
        const CANDIDATES: usize = 12;
        const BYTES: u64 = 4_096;
        let (shard, keys) = keys_sharing_one_shard(CANDIDATES);
        assert_eq!(
            keys.len(),
            CANDIDATES,
            "fixture precondition: the search must return exactly {CANDIDATES} keys in shard {shard}"
        );

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let now = SystemTime::now();
        for key in &keys {
            publish_staged_range_at(dir, &ledger, key, BYTES, now).await;
        }

        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        let summary = outcome.summary();
        assert!(
            outcome.installed(),
            "the pass must install; it retained with {:?}",
            outcome.retain_reason()
        );
        assert!(
            summary.candidate_read_inflight_peak > 1,
            "the fold held at most {} .meta read outstanding across {CANDIDATES} candidates in one \
             shard, so the candidate loop is SERIAL. Serial is 174.7 obj/s against 3,801 at the \
             measured knee of 64, which puts the parse term over the whole authority-age window and \
             the reachable-bucket ceiling at zero.",
            summary.candidate_read_inflight_peak
        );
        assert_eq!(
            folded_bytes(dir),
            BYTES * CANDIDATES as u64,
            "every candidate's bytes must be summed. A fan-out that dropped one lowers F, which \
             biases the staging eviction trigger LATE — the direction § 16.6.2 rules out."
        );
    }

    /// An unreadable candidate still refuses, and the refusal names the FIRST such candidate in
    /// ledger order rather than whichever read failed first.
    ///
    /// # Both halves matter, for different reasons
    ///
    /// The refusal is correctness: a shard whose contribution is unknown must retain its previous
    /// figure rather than install a short one, and an unparseable `.meta` is exactly that. Collapsing
    /// this arm would install a figure missing the unreadable candidate's bytes — an under-count.
    ///
    /// The *ordering* is determinism. Reads now complete out of order, so short-circuiting on the
    /// first `Unreadable` to arrive would make the reported candidate a function of I/O timing: two
    /// folds of the same shard would log different keys, and a fleet investigation would be chasing a
    /// name that changes per run. The loop therefore collects every result and processes them in
    /// ledger order, which is what a serial fold did. This fixture puts the unreadable candidate in
    /// the MIDDLE, with readable candidates on both sides, so a completion-order implementation would
    /// pick a different one with high probability.
    ///
    /// A failure in the refusal implicates the `StagedCandidateVerdict::Unreadable` arm. A failure in
    /// the named candidate implicates the `folded.sort_by_key(|(position, _)| *position)` immediately
    /// above it.
    #[tokio::test]
    async fn an_unreadable_candidate_refuses_and_the_verdict_is_not_a_race() {
        const CANDIDATES: usize = 9;
        const BYTES: u64 = 4_096;
        let (shard, keys) = keys_sharing_one_shard(CANDIDATES);

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let now = SystemTime::now();
        for key in &keys {
            publish_staged_range_at(dir, &ledger, key, BYTES, now).await;
        }

        // Damage two candidates, neither of them the first or the last, so a completion-order
        // implementation has a real chance of naming the wrong one and an in-order implementation
        // cannot.
        let damaged: Vec<&String> = vec![&keys[3], &keys[6]];
        for key in &damaged {
            let meta_path =
                crate::disk_cache::get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
            tokio::fs::write(&meta_path, b"{ this is not json")
                .await
                .unwrap();
        }

        let before = read_shard_checkpoints(dir);
        let outcome = run_staged_fold(dir, &ledger, "host:1").await;
        assert_eq!(
            outcome.retain_reason(),
            Some(FoldRetainReason::CandidateUnreadable),
            "a candidate whose .meta cannot be parsed leaves this shard's contribution unknown, so \
             the pass must retain rather than install a figure missing its bytes"
        );
        assert_eq!(
            outcome.summary().retained.first().map(|(s, _)| s.clone()),
            Some(shard.clone()),
            "and the retain must name the shard the unreadable candidate is in"
        );
        assert_eq!(
            read_shard_checkpoints(dir).staged_byte_sum(),
            before.staged_byte_sum(),
            "the previous figure must survive untouched — installing here is the under-count \
             § 16.6.2 rules out"
        );

        // The determinism half. Ten folds of the same damaged fixture must reach the same verdict
        // every time; a completion-order short-circuit would be stable only by luck.
        for attempt in 0..10 {
            let repeat = run_staged_fold(dir, &ledger, "host:1").await;
            assert_eq!(
                repeat.retain_reason(),
                Some(FoldRetainReason::CandidateUnreadable),
                "attempt {attempt}: the verdict must not depend on which read completed first"
            );
        }
    }

    // ── Per-visit sampling (ADAPT-U4, § 16.6.13 K2) ─────────────────────────────────────────
    // These test the RECORDING, not the controller's decision: one shard visited once gives one
    // sample, below the controller's 3-sample threshold.

    const SAMPLING_PEER: &str = "ip-10-9-9-9.internal:4242";

    fn fresh_limiter() -> std::sync::Mutex<crate::scanner_limiter::ScannerLimiter> {
        std::sync::Mutex::new(crate::scanner_limiter::ScannerLimiter::new(4))
    }

    fn assert_no_sample(lim: &crate::scanner_limiter::ScannerLimiter, what: &str) {
        assert_eq!(lim.samples_discarded(), 0, "{what}: no discard");
        for j in 1..=8 {
            assert_eq!(lim.samples_at(j), 0, "{what}: no sample at level {j}");
        }
    }

    /// (b) A quiet visit (no peer) is recorded at level 1, with its rate.
    #[tokio::test]
    async fn sampled_fold_records_a_quiet_visit_at_level_one() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        publish_staged_range(dir, &ledger, "bucket/sampled.bin", 4096).await;
        let limiter = fresh_limiter();
        let sampler = crate::journal_consolidator::VisitSampler::new(
            dir,
            "host:1",
            crate::scanner_registry::ScanKind::Fold,
            &limiter,
        );
        let t0 = std::time::Instant::now();
        let outcome = run_staged_fold_sampled(dir, &ledger, "host:1", Some(&sampler)).await;
        let wall = t0.elapsed().as_secs_f64();
        assert!(outcome.installed(), "{outcome:?}");
        assert_eq!(outcome.summary().installed.len(), 1);
        let lim = limiter.lock().unwrap();
        assert_eq!(lim.samples_discarded(), 0);
        assert_eq!(lim.samples_at(1), 1, "one completed visit at level n+1 = 1");
        let rate = lim.rate_estimate(1).expect("a rate at level 1");
        // One `.meta` read inside a visit no longer than the whole pass.
        assert!(
            rate.is_finite() && rate >= 1.0 / wall,
            "rate={rate} wall={wall}"
        );
    }

    /// (a) A peer document published between a visit's two reads discards the visit.
    #[tokio::test]
    async fn sampled_fold_discards_a_visit_whose_peer_count_changed() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        publish_staged_range(dir, &ledger, "bucket/sampled.bin", 4096).await;
        let limiter = fresh_limiter();
        let published = std::sync::atomic::AtomicBool::new(false);
        let sampler = crate::journal_consolidator::VisitSampler::new(
            dir,
            "host:1",
            crate::scanner_registry::ScanKind::Fold,
            &limiter,
        )
        .with_after_read(|| {
            // After the START read, once: the peer appears mid-visit.
            if !published.swap(true, std::sync::atomic::Ordering::SeqCst) {
                crate::scanner_registry::publish(
                    dir,
                    SAMPLING_PEER,
                    crate::scanner_registry::ScanKind::Fold,
                )
                .unwrap();
            }
        });
        let outcome = run_staged_fold_sampled(dir, &ledger, "host:1", Some(&sampler)).await;
        assert!(outcome.installed(), "{outcome:?}");
        assert_eq!(outcome.summary().installed.len(), 1, "the visit completed");
        let lim = limiter.lock().unwrap();
        assert_eq!(lim.samples_discarded(), 1, "n changed 0 -> 1: discarded");
        for j in 1..=8 {
            assert_eq!(lim.samples_at(j), 0, "table unchanged at level {j}");
        }
    }

    /// (c) A busy visit (a peer holds the claim) produces no sample.
    #[tokio::test]
    async fn sampled_fold_busy_visit_records_nothing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let key = "bucket/sampled.bin";
        publish_staged_range(dir, &ledger, key, 4096).await;
        let (bucket, l1) = shard_of(key).unwrap();
        let shard = Shard::new(bucket, l1);
        assert_eq!(
            claim_shard(dir, &shard, SAMPLING_PEER).await,
            ClaimOutcome::Claimed,
            "fixture: the peer holds the only shard"
        );
        let limiter = fresh_limiter();
        let sampler = crate::journal_consolidator::VisitSampler::new(
            dir,
            "host:1",
            crate::scanner_registry::ScanKind::Fold,
            &limiter,
        );
        let outcome = run_staged_fold_sampled(dir, &ledger, "host:1", Some(&sampler)).await;
        assert_eq!(outcome.summary().busy, vec![shard], "{outcome:?}");
        assert!(outcome.summary().installed.is_empty());
        assert_no_sample(&limiter.lock().unwrap(), "busy visit");
    }
}

/// the scrape-triggered authority refresh runs on the blocking pool.
#[cfg(test)]
mod authority_refresh_worker_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;

    /// Two refreshes whose derivation is slow inside its synchronous reads (a hook that
    /// sleeps the thread, standing in for `read_shard_checkpoints` on a slow volume) must not
    /// stop a peer task on a two-worker runtime. Std channels, because a runtime with every
    /// worker blocked also stops its timer.
    #[test]
    fn a_slow_scrape_refresh_does_not_take_a_runtime_worker() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let dirs = [
            tempfile::TempDir::new().unwrap(),
            tempfile::TempDir::new().unwrap(),
        ];
        let reached = Arc::new(AtomicUsize::new(0));
        for d in &dirs {
            let reached = Arc::clone(&reached);
            set_authority_between_reads_hook_for_tests(
                d.path(),
                Box::new(move || {
                    Box::pin(async move {
                        reached.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(1500));
                    })
                }),
            );
        }
        let paths: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();
        rt.block_on(async {
            for p in &paths {
                // A scrape's refresh takes `P` from the last kept walk and derives nothing
                // without one, so each directory needs a kept walk for the slow read to be reached.
                let _ = crate::signed_put_handler::observe_in_progress(p, Duration::ZERO).await;
                spawn_authority_refresh(p);
            }
        });
        std::thread::sleep(Duration::from_millis(300));
        let (tx, rx) = std::sync::mpsc::channel();
        rt.spawn(async move {
            let _ = tx.send(());
        });
        let peer = rx.recv_timeout(Duration::from_millis(500));
        let reached_at_peer = reached.load(Ordering::SeqCst);
        rt.shutdown_timeout(Duration::from_secs(5));
        assert_eq!(
            reached_at_peer, 2,
            "fixture: both refreshes must be inside their slow read when the peer is polled"
        );
        assert!(
            peer.is_ok(),
            "STARVED: a peer task did not run within 500 ms while 2 slow authority refreshes ran on a 2-worker runtime: {:?}",
            peer
        );
    }
}

#[cfg(test)]
mod warn_remedy_tests {
    use super::*;

    #[test]
    fn capped_budget_warn_names_the_remedy_and_the_reach_is_published() {
        assert!(SHARD_BUDGET_CAPPED_WARN.contains("add a proxy instance"));
        assert!(SHARD_BUDGET_CAPPED_WARN.contains("no configuration field"));
        // adding proxies does not stop the WARN, and the text must say what does show
        // the remedy working.
        assert!(SHARD_BUDGET_CAPPED_WARN.contains("this WARN continues"));
        assert!(SHARD_BUDGET_CAPPED_WARN.contains("staged_fold_shards_beyond_max_age returns to 0"));
        assert!(!SHARD_BUDGET_CAPPED_WARN.contains('\n'), "one log line");
        // 512 visits per pass x (86,400 / 300) passes: the figure docs/CONFIGURATION.md quotes.
        assert_eq!(shards_reachable_per_instance(), 147_456);
        assert!(shards_per_pass(147_456) <= SHARD_VISITS_PER_PASS_CEILING);
    }
}
