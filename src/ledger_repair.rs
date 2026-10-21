//! Ledger-completeness repair: design § 16.6.6 activation precondition 3b.
//!
//! # What this exists to stop, and why nothing else stops it
//!
//! The staged fold's candidate set comes from the Write_Ledger and from nowhere else
//! (`staged_fold::run_staged_fold_with_cap`, the `WriteLedger::group_by_key` loop). A staged
//! `.meta` that no ledger entry names therefore contributes **nothing** to `F`, and no
//! existing mechanism says so:
//!
//! - the authority readers classify **six** ledger-incompleteness classes between them —
//!   unreadable file, malformed line, truncation, listing failure, an unconverted legacy flat
//!   file, and a file-count mismatch. A ledger entry that was **never appended**, or whose whole
//!   file was lost, triggers none of them: the directory listing simply does not name what is not
//!   there. Nor is file-count mismatch a detector of a lost file — it is **unreachable** in the
//!   shipped code, because every path that fails to read a listed file increments
//!   `files_unreadable` and that gate is tested first.
//! - the fold's own per-shard install is complete-or-retain with respect to the inputs it
//!   *reads*, which is a different statement from complete with respect to what is on disk.
//!
//! So the failure is silent and it is in the **forbidden direction**: a missing entry lowers
//! `F`, lowers `write_cache_size`, and makes the staging eviction trigger fire LATE. § 16.6.2
//! rules that direction out by name — it is why the handoff boundary was deliberately given
//! an over-count — and § 16.6.1 rules out "a later pass will notice" as an answer to any
//! member of this class. Neither "the window is small" nor "the next fold fixes it" is
//! available here, because nothing in the next fold looks at anything the lost entry would
//! have pointed at.
//!
//! # Why this is a repair rather than only a detector
//!
//! Detection alone was considered and rejected. A detector makes the fold RETAIN, which is
//! direction-safe, but the only repair that then exists is
//! `CacheSizeTracker::reappend_missing_ledger_entries`, called from exactly one place —
//! inside `perform_full_validation`, the whole-cache scan this spec's own premise says cannot
//! complete at the O(100M)-range design point. A detector would therefore convert a silent
//! under-count into a **permanently** stalled authority on the very volumes this workstream
//! exists to serve, and the stall would clear only by running the thing that cannot run.
//!
//! Detection is also not cheap enough to be worth having on its own. Deciding whether an
//! enumerated `.meta` is staged requires **reading** it — the flag is inside the file — so a
//! detector pays the same per-shard enumeration and the same per-`.meta` read this repair
//! pays. Given identical cost, repairing is strictly better than reporting.
//!
//! # Matching is by RANGE IDENTITY, and that is the whole difference from the incumbent
//!
//! `reappend_missing_ledger_entries` matches by `cache_key`, which its own doc comment
//! records as deliberate (identity matching would re-append a duplicate per object per
//! scan). The cost is that **partial per-range loss inside an object that retained even one
//! entry is undetectable** — the object is "known", so every lost range stays lost. That was
//! carried as a D1 deferral.
//!
//! This rotation matches on `(cache_key, range_start, range_end)` instead. That is the range
//! identity, and it is deliberately **not** `WriteLedgerEntry::identity()`'s 5-tuple: the
//! 5-tuple includes the timestamp and the instance, which makes it *entry* identity, so a
//! legitimate re-PUT of the same extent produces a second identity and matching on it would
//! re-append a duplicate for every range on every sweep. `(key, start, end)` is the coarsest
//! key that answers "is this extent covered by any ledger entry at all", which is the
//! question coverage actually asks.
//!
//! # Bounded per visit, and it resumes rather than failing
//!
//! One visit enumerates one shard's `.meta` files, capped at [`REPAIR_METAS_PER_VISIT_CAP`].
//! A shard larger than the cap stores a resume cursor and continues on its next visit, so a
//! pathological shard slows the rotation instead of blocking it forever. A cap with no cursor
//! would make such a shard permanently un-sweepable and therefore permanently block
//! activation, which is safe and useless.
//!
//! The rotation window is [`MAX_REPAIR_AGE_SECS`] — 24 h, not the fold's 1 h — because a lost
//! append is a rare event (an I/O error on a best-effort write) while a sweep is
//! `O(objects in shard)` against the fold's `O(staged candidates in shard)`. Matching the
//! fold's window would make the rare case pay the common case's cadence. 24 h also lines up
//! with the existing full-validation freshness horizon, so the two repairs are on comparable
//! clocks rather than arbitrary ones.
//!
//! # Complete-or-refuse, applied PER SHARD to the input that decides an append
//!
//! Repairing from a partial view of the ledger would re-append entries that exist in a file the
//! pass could not open — duplicates, from exactly the input class the fold refuses to install on,
//! and worse here because the fold's refusal is reversible while an append is not. So the coverage
//! set is built from an authority read, and an incomplete read refuses.
//!
//! **The scope of that refusal is one shard, matching the scope of the sweep.** The coverage set is
//! read from `WriteLedger::read_shard_for_authority` inside the visit loop, so a shard whose own
//! ledger file is unreadable records no completed sweep while its siblings are swept normally. It
//! used to be one `read_for_authority` before the loop, which meant a single bad file anywhere on
//! the volume refused every shard — and because a refused pass writes no repair document and does
//! not consume the cadence, that stalled the rotation **permanently**, which in turn means
//! § 16.6.6 precondition 3b never passes and the staged-authority epoch never flips. One
//! unreadable file cost the whole activation, indefinitely.
//!
//! Narrowing it is sound because a range's ledger entry lives in that range's own shard directory:
//! the append resolves its file through `shard_of(cache_key)` and this sweep reaches `.meta` files
//! through `get_sharded_path`, and both take the first two hex digits of `blake3(object_key)`. The
//! two therefore agree by construction rather than by convention — and because it is only a
//! construction, [`classify_meta`] verifies it per file instead of trusting it. A `.meta` whose own
//! `cache_key` derives a different shard from the directory holding it is skipped and reported,
//! since this shard's ledger cannot answer for it and appending would duplicate.
//!
//! It is also cheaper in the case that matters. The old read was `1 + B + B×256` readdirs plus
//! every ledger file on the volume, paid once per pass however few shards the budget allowed; the
//! new one is one readdir plus that shard's own files, paid per shard actually visited. The two
//! converge only when a pass visits every shard, which the per-pass budget exists to prevent.
//!
//! Spec: cache-eviction-at-scale § 16.6.6 precondition 3b, § 16.6.3.

use crate::staged_fold::{PendingRange, PendingStore, Shard, MAX_AUTHORITY_AGE_SECS};
use crate::write_ledger::WriteLedger;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// `metadata/_shard_state`, mirrored here rather than imported because the sibling module
/// keeps it private.
const SHARD_STATE_DIR: &str = "_shard_state";

/// `metadata/_shard_state/repair` — one document per shard, recording its last completed
/// sweep.
const REPAIR_DIR: &str = "repair";

/// `metadata/_shard_state/repair_claims` — a claim namespace of its own.
///
/// Deliberately NOT the fold's claim directory. Sharing it would make a repair visit and a
/// fold visit mutually exclusive on the same shard, which buys nothing (the repair appends to
/// the ledger, the fold reads it, and the worst interleaving costs one pass of latency) and
/// costs the fold a shard whenever a slower repair sweep holds the lease.
const REPAIR_CLAIMS_DIR: &str = "repair_claims";

/// Document schema. An unknown value is treated as unreadable, never ignored.
const REPAIR_SCHEMA: u32 = 1;

/// Claim lease for a repair visit, seconds.
///
/// Longer than the fold's because a sweep is `O(objects in shard)` where a fold visit is
/// `O(staged candidates in shard)`. § 16.2's rule still applies and is why this is a lease at
/// all rather than a lock: a lost, expired or duplicated claim costs duplicated work and can
/// never produce a wrong figure, because the repair's only effect is an idempotent-by-coverage
/// append.
const REPAIR_CLAIM_LEASE_SECS: u64 = 1_800;

/// Target rotation window: every shard swept at least this recently.
///
/// 24 h, the same window as the fold's [`MAX_AUTHORITY_AGE_SECS`]: the two rotations share one
/// cadence class (§ 16.6.12 point 1). The window is how stale the *evidence* for a one-time
/// activation decision may be. Lengthening it makes the rotation reachable on a larger cache,
/// because the binding constraint is total work against `D · I · W` — see
/// [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`].
///
/// # Fixed, not derived
///
/// Why fixed? Because **deriving it from the observed cache would make precondition 3b
/// self-satisfying**: any cache would qualify by computing a window large enough to contain its own
/// rotation, which is the weakened-predicate resolution the amendment forbids. A fixed window means
/// "one rotation completed" means the same thing on every deployment. 86,400 s is chosen to match
/// the existing full-validation freshness horizon, so the two independent repairs of this invariant
/// sit on comparable clocks rather than arbitrary ones.
///
/// Changing it is therefore a deliberate code change with the consequence published, not a knob.
/// See [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`] for the no-knob reasoning in full.
pub const MAX_REPAIR_AGE_SECS: u64 = 86_400;

/// How often a pass is eligible, seconds. Matched to the fold's cadence so the two rotations
/// are driven from the same consolidation cycle without a second timer.
///
/// # Fixed, and shortening it is NOT a remedy for an unreachable rotation
///
/// Two independent reasons, and the second is the one that would otherwise get tried first:
///
/// - **`T` cancels out of the capacity arithmetic.** It scales the per-pass budget (`D·T`) and the
///   passes per window (`W/T`) by the same factor, so total affordable work `D·I·W` is independent
///   of it. Changing `T` changes granularity, never capacity.
/// - **Shortening it makes things strictly worse**, by the argument § 16.6.4 already makes for the
///   fold: every pass performs one *complete* authority read of every ledger file, so halving `T`
///   doubles that fixed per-pass cost while leaving the total `.meta` work unchanged — spending the
///   cheap term to buy nothing.
pub const LEDGER_REPAIR_INTERVAL_SECS: u64 = 300;

/// `.meta` files one visit will read before it stores a cursor and stops.
///
/// # Derived, and BOTH earlier values were derived against a rate that was a measurement
/// # artefact
///
/// A visit must fit the duty-cycle time a pass has, which is § 16.6.4's `D · T` = `0.167 × 300 s`
/// = **50.1 s** — the same budget the fold's ceiling is derived against. Worst case for a visit is a
/// fully populated shard, where all 4,096 L2 directories exist:
///
/// ```text
///     c_r  +  d · min(4096, m)  +  K/R   ≤   D · T
///     0.018024 s  +  0.002234 s/dir × 4096 dir  +  K/(2,900 obj/s)   ≤   50.1 s
///     K  ≤  (50.1 − 9.150 − 0.018) s × 2,900 obj/s   ≈   118,701 objects
/// ```
///
/// **65,536** is that rounded down to a power of two, matching § 16.6.4's `1,080 → 1,024`
/// convention.
///
/// # The history is worth keeping, because it is why this constant is derived in code
///
/// This was **20,000**, justified as "bounded work" — which it is, against nothing. It was then cut
/// to **8,192** on the arithmetic above evaluated at `R = 212 obj/s`, where 20,000 costs
/// `20,000/212` = 94.3 s, or 1.9× the whole pass budget. `R = 212` has since been **measured to be
/// a concurrency artefact**: it was recorded from a scan whose outstanding-read count was 1, and
/// the same reference tree on the same volume sustains 2,900 obj/s at the measured knee of 64
/// outstanding reads. At the
/// measured rate 20,000 costs **6.9 s** of the 50.1 s budget, so **the cut to 8,192 was unnecessary
/// in the first place** — it was correct arithmetic on an input that was 13.7× low.
///
/// The lesson this constant now carries: the reduction was accepted as a real capability cost for a
/// release, and it was an artefact of how the input was measured rather than of the hardware. A
/// bound is only as good as the provenance of its inputs, which is why they are named below.
///
/// # Inputs, now MEASURED rather than estimated
///
/// - **`R = 2,900 obj/s`** — read-plus-parse, rounded down from the lowest of five knee
///   measurements across three fleet proxies solo and concurrent (3,801 / 3,402 / 3,188 / 2,969 /
///   2,881). Measured on an **idle** fleet, so optimistic; a serving fleet contends for the same
///   per-client NFS pipeline. Reaching it requires the sweep to keep [`SWEEP_READ_CONCURRENCY`]
///   reads outstanding, which is why that constant and this one have to move together.
/// - **`c_r = 18.024 ms` (EFS)** — per-visit fixed cost, 50 iterations of the real claim-protocol
///   shape. The previous estimate was 10 ms, so this is the one measured input that moved a
///   conclusion **unfavourably on EFS**; on FSx for OpenZFS the same input moved it *favourably*
///   (`c_r = 3.774 ms`), and FSx for NetApp ONTAP measures **both 14.345 ms** (M5) **and 7.131 ms**
///   (`SCALE-ENV-FOLD`) — 2.01× apart on one backend family. **`c_r` is per-CONFIGURATION rather
///   than per-backend**, so every bound below is an EFS bound and none of them carries across.
/// - **`d = 2.234 ms`** — one L2 `readdir`, the mean of five runs' means. The 2 ms estimate was
///   good to within 10%.
///
/// A shard over the cap resumes from its stored cursor; it is never refused.
const REPAIR_METAS_PER_VISIT_CAP: u64 = 65_536;

/// Outstanding `.meta` reads and L2 `readdir`s one shard sweep keeps in flight.
///
/// # This is the measured knee, and it is deliberately NOT rounded up for headroom
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
/// **64 is the knee.** 64→128 returns 3% of linear and 128→256 returns 2%, while per-file latency
/// rises linearly (16.7 → 32.3 → 62.2 ms) and `concurrency / latency` pins at 3,843 / 3,968 / 4,118
/// — the signature of a fixed-capacity service queue rather than of a resource being consumed.
/// Going past the knee therefore buys throughput inside the measurement noise and pays for it in
/// queueing latency, which on a shared volume is latency other work is also waiting on.
///
/// # Async concurrency, NOT rayon, and the control is what settles it
///
/// A rayon control at 8 threads gives **1,442 obj/s**, within 3.6% of async-8's 1,392. At matched
/// concurrency the two mechanisms are indistinguishable, so the 2.76× from 8 to 64 is bought by
/// **outstanding requests** and rayon's coupling of concurrency to thread count is the limitation.
/// On an 8-vCPU proxy rayon's default pool cannot exceed 8 outstanding reads without oversubscribing
/// cores for work that is entirely I/O wait. `cache_size_tracker.rs`'s conclusion — that the gap is
/// a statement about NFS concurrency and cores are the wrong lever — is confirmed and extended.
///
/// # What it does not change
///
/// The **appends** stay serial: `WriteLedger::append` holds a process-wide `append_mutex` across a
/// blocking `fs4` flock, so nothing is gained by issuing them concurrently, and they are expected to
/// number zero on a healthy fleet. The **cursor** stays a single scalar L2 name, advanced only at a
/// window boundary over the sorted directory list. Those are the two real constraints on this
/// rotation and both survive a parallel-read / serial-append split — which is the split the
/// incumbent rayon `.meta` scan already implements and § 6.3 already prescribes.
const SWEEP_READ_CONCURRENCY: usize = 64;

/// The directory-stage width used until all three traversal estimates exist, `W/2`.
///
/// A neutral split rather than a predicted one. The first visit on a freshly started process has
/// measured nothing, and § 16.6.13's premise is that the backend is not predictable — so seeding
/// from any backend's figures would be exactly the prediction this section replaces. `W/2` keeps
/// **both** stages fed, which is the only property that matters before an estimate exists, and one
/// completed visit replaces it.
///
/// It is also the value the formula itself returns at `ô = o*` (see
/// [`compute_directory_concurrency`]), so the seed is the crossover point rather than a third
/// convention.
///
/// Tests pass it explicitly. That is deliberate: [`sweep_shard_capped`] takes the width as a
/// parameter rather than reading [`TRAVERSAL_ESTIMATES`], so no test can be perturbed by a
/// process-wide estimate another test moved, and no test's outcome depends on scheduling order.
const SWEEP_DIRECTORY_CONCURRENCY_SEED: usize = SWEEP_READ_CONCURRENCY / 2;

/// EWMA smoothing factor for the three traversal estimates, `α = 1/8`.
///
/// One anomalous shard visit moves an estimate by 12.5%, which is the damping § 16.6.13 (T5)
/// specifies. The split itself is a **continuous** function of these estimates and so has no
/// discontinuity to flap across; the EWMA, the volume-wide scope, and the once-per-pass
/// recomputation are the three measures that keep the *inputs* from making it oscillate anyway.
const TRAVERSAL_EWMA_ALPHA: f64 = 0.125;

/// The three quantities the unified sweep's split is computed from, **measured rather than
/// configured**.
///
/// # Volume-wide, not per-shard
///
/// Occupancy is a property of the BLAKE3 key distribution rather than of a particular shard: four
/// fixtures carrying six shard-level readings on independent shards and buckets agreed within
/// 1.1–1.9%.
/// A per-shard `ô` is available at no extra
/// cost — every visit already produces one — if that ever stops holding. It is not built now.
///
/// # No config knob, on § 16.6.4's reasoning
///
/// A derived value that tracks the volume is the entire point. A knob would let an operator set a
/// figure the volume contradicts, and the failure would be a silently mis-split pipeline rather
/// than a rejected configuration.
///
/// # What the two latencies are, and what they are NOT
///
/// Both are **in-flight durations at the width the stage was actually running at**, measured
/// around the individual operation. That is the right quantity for the Little's-law balance below
/// and it is *not* the same as a solo latency: at 64 outstanding reads EFS's per-file latency is
/// 16.7 ms while its aggregate rate is 3,801 obj/s, so a per-operation figure and a
/// throughput reciprocal differ by more than an order of magnitude and are not interchangeable.
/// Measuring the operation the stage performs, at the width it performs it, is what makes the
/// ratio self-consistent as the split moves.
///
/// Spec: cache-eviction-at-scale § 16.6.13 (T5).
struct TraversalEstimates {
    /// `d̂`, mean L2 `readdir` in-flight duration, milliseconds. `None` until one visit completes.
    l2_readdir_ms: Option<f64>,
    /// `r̂`, mean `.meta` read-plus-parse in-flight duration, milliseconds.
    meta_read_ms: Option<f64>,
    /// `ô`, `.meta` files per populated L2 directory.
    metas_per_l2_dir: Option<f64>,
}

static TRAVERSAL_ESTIMATES: std::sync::Mutex<TraversalEstimates> =
    std::sync::Mutex::new(TraversalEstimates {
        l2_readdir_ms: None,
        meta_read_ms: None,
        metas_per_l2_dir: None,
    });

/// Fold one completed shard visit's measurements into the volume-wide estimates.
///
/// Called once per visit that produced at least one sample, and never on a refusal: a refused
/// visit stopped part-way through for a reason unrelated to the backend's speed, so its partial
/// timings describe no operating point.
fn observe_shard_visit(
    l2_readdir_ms: Option<f64>,
    meta_read_ms: Option<f64>,
    metas_per_l2_dir: Option<f64>,
) {
    let Ok(mut est) = TRAVERSAL_ESTIMATES.lock() else {
        return;
    };
    est.l2_readdir_ms = ewma_blend(est.l2_readdir_ms, l2_readdir_ms);
    est.meta_read_ms = ewma_blend(est.meta_read_ms, meta_read_ms);
    est.metas_per_l2_dir = ewma_blend(est.metas_per_l2_dir, metas_per_l2_dir);
}

/// Fold one sample into an EWMA at [`TRAVERSAL_EWMA_ALPHA`].
///
/// A free function rather than a closure inside [`observe_shard_visit`] so it is testable without
/// touching [`TRAVERSAL_ESTIMATES`]. That matters more than it looks: the estimates are a
/// process-wide static, tests in this module run concurrently in one process, and a test that
/// asserted on the static could be perturbed by any other test that completed a shard visit — a
/// flake in whichever test happened to run second.
///
/// A missing, non-finite or negative sample leaves the estimate alone. `NaN` in particular must not
/// propagate: `NaN` reaches [`compute_directory_concurrency`], and `NaN as usize` is `0`, which is
/// exactly the starved stage the clamp there exists to prevent.
fn ewma_blend(current: Option<f64>, sample: Option<f64>) -> Option<f64> {
    let Some(sample) = sample else { return current };
    if !sample.is_finite() || sample < 0.0 {
        return current;
    }
    Some(match current {
        // First sample seeds outright. Blending against a zero would spend eight visits climbing to
        // a figure the first visit already knew.
        None => sample,
        Some(previous) => previous + TRAVERSAL_EWMA_ALPHA * (sample - previous),
    })
}

/// The three estimates, or `None` for any that has never been measured.
fn traversal_estimates() -> (Option<f64>, Option<f64>, Option<f64>) {
    match TRAVERSAL_ESTIMATES.lock() {
        Ok(est) => (est.l2_readdir_ms, est.meta_read_ms, est.metas_per_l2_dir),
        Err(_) => (None, None, None),
    }
}

/// The directory-stage width, from the measured estimates. **Recomputed at most once per pass.**
///
/// Once per pass rather than once per visit, deliberately: a pass visits several shards, and
/// recomputing between them would let one shard's timings move the next shard's split inside a
/// single scheduling decision. Together with the EWMA and the clamp, that is the third of
/// § 16.6.13 (T5)'s three damping measures.
fn directory_concurrency_for_pass() -> usize {
    let (d, r, o) = traversal_estimates();
    match (d, r, o) {
        (Some(d), Some(r), Some(o)) => {
            compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, o, r)
        }
        _ => SWEEP_DIRECTORY_CONCURRENCY_SEED,
    }
}

/// Balance the two stages by Little's law.
///
/// ```text
///     W_d  =  W · d̂ / (d̂ + ô · r̂)              W_f  =  W − W_d
/// ```
///
/// The directory stage supplies files at `ô · W_d / d̂` and the file stage consumes them at
/// `W_f / r̂`; equating the two and substituting `W_f = W − W_d` gives the expression above. At
/// `ô · r̂ = d̂` — which is exactly the crossover `ô = o* = d̂/r̂` — it returns `W/2`.
///
/// # Continuous, which is the structural half of the anti-flapping answer
///
/// There is no threshold here and therefore nothing to oscillate across: a 1% change in any input
/// moves `W_d` by about 1%. That is what makes it safe to drive from a noisy measurement at all,
/// and it is why § 16.6.13 (T4) unifying the two phases **removes** a strategy switch rather than
/// adding one.
///
/// # The clamp is not decoration
///
/// `[1, W−1]` guarantees neither stage is ever starved to zero, whatever the estimates say. A
/// `W_f` of 0 deadlocks the pipeline outright — the file stage could never drain what the
/// directory stage produced — and a `W_d` of 0 means no directory is ever enumerated, so the
/// sweep would report a complete visit of a shard it never looked at. Both are reachable from a
/// pathological estimate, so both are excluded here rather than assumed away.
///
/// A non-finite or non-positive input falls back to the seed rather than propagating a NaN into a
/// width, since `NaN as usize` is 0 and would hit exactly the starvation the clamp exists to stop.
fn compute_directory_concurrency(
    width: usize,
    l2_readdir_ms: f64,
    metas_per_l2_dir: f64,
    meta_read_ms: f64,
) -> usize {
    let seed = (width / 2).clamp(1, width.saturating_sub(1).max(1));
    if !(l2_readdir_ms.is_finite() && metas_per_l2_dir.is_finite() && meta_read_ms.is_finite()) {
        return seed;
    }
    if l2_readdir_ms <= 0.0 {
        return seed;
    }
    let file_work = metas_per_l2_dir.max(0.0) * meta_read_ms.max(0.0);
    let denominator = l2_readdir_ms + file_work;
    if denominator <= 0.0 {
        return seed;
    }
    let raw = (width as f64) * l2_readdir_ms / denominator;
    if !raw.is_finite() {
        return seed;
    }
    let upper = width.saturating_sub(1).max(1) as f64;
    (raw.round().clamp(1.0, upper)) as usize
}

/// Hard ceiling on shards visited in one pass, whatever [`shards_per_pass`] derives.
///
/// # Derived. The previous value of 8 was "mirroring the fold's ceiling discipline" and gated
/// # activation at nine buckets
///
/// **The defect, stated first.** The fold's 1,024 is derived; 8 was not. With
/// `P = W/T = 86,400/300 = 288` passes per window, a ceiling of 8 bounds the sweepable population
/// at `8 × 288 = 2,304` shards, and shards are `256 × buckets`:
///
/// ```text
///      9 buckets = 2,304 shards -> needs 8/pass -> exactly at the limit
///     10 buckets = 2,560 shards -> needs 9/pass -> CLAMPED, rotation never completes
/// ```
///
/// Precondition 3b requires a **complete** rotation, so above nine buckets 3b could never pass and
/// the epoch could never flip — the fleet would stay on the lossy incumbent counter indefinitely,
/// and silently, because a rotation that cannot complete is indistinguishable from one that has not
/// completed yet. That was tighter than § 16.6.4's ~50-bucket fold limit, making it the binding
/// constraint on the whole of decision C for no reason anyone had written down.
///
/// # The derivation
///
/// Same model and the same inputs as § 16.6.4, with `m = O/N` objects per shard and
/// `L(m) = min(4096, m)` populated L2 directories:
///
/// ```text
///     v · [ c_r  +  d·L(m)  +  min(K, m)/R ]   ≤   D · T
/// ```
///
/// The ceiling must not bind before *cost* does. Cost binds hardest on large shards, so the ceiling
/// can only ever matter when shards are small — which is exactly the many-bucket case that broke.
/// Setting it at the point where even the **cheapest possible** visit exhausts the pass budget (one
/// L2 directory holding one object):
///
/// ```text
///     ceiling  ≤  D·T / (c_r + d + 1/R)
///              =  50.1 s / (0.018024 + 0.002234 + 0.000345) s
///              =  2,431 visits
/// ```
///
/// **2,048** is that rounded down to a power of two, and it **SURVIVES** the re-measurement of all
/// three inputs — it is the one constant in this module the corrected rate did not move.
///
/// **Its margin tightened, though, and that is worth knowing rather than filing as unchanged.** The
/// bound was 2,996 visits on the estimated inputs and is 2,431 on the measured ones, so the ceiling
/// now trips **~16% before the arithmetic requires** rather than ~32%. The measured `c_r` did that:
/// at 18.024 ms **on EFS** against the estimated 10 ms it is 1.8× more expensive, and it dominates a
/// cheapest-possible visit. Both the bound and the margin are therefore EFS figures — a cheaper
/// configuration widens the margin and a dearer one narrows it, and `c_r` is per-CONFIGURATION even
/// within one backend family (see [`REPAIR_METAS_PER_VISIT_CAP`]'s inputs).
///
/// **The power-of-two convention leaves more slack than 16%, not less, so 16% is comfortably inside
/// it.** § 16.6.4's fold ceiling rounds 707 visits down to 512 and so sits **27.6%** below its own
/// bound. This paragraph used to cite that figure as "~4%", which was the margin of the fold's
/// *retired* 1,024 and made 16% read as four times looser than the convention it was being judged
/// against; corrected, 16% is the **tighter** of the two and needs no defending. What does still
/// need re-reading is the other half: the next unfavourable move in `c_r` is the one that would put
/// 2,048 above its own bound, and this constant reaches that point before the fold's does.
///
/// At 2,048 the ceiling bounds the population at `2,048 × 288` = 589,824 shards = **2,304 buckets**,
/// so it no longer binds in any realistic deployment.
///
/// # What binds instead, and this is the number to publish
///
/// The ceiling was never the real limit. Total work for one rotation, against the duty-cycle time
/// the window permits:
///
/// ```text
///     O/R  +  c_r·N  +  d·min(O, 4096·N)   ≤   D · I · W
/// ```
///
/// `O/R` dominates on a dense tree — a repair visit reads one `.meta` per **object**, where a fold
/// visit reads one per **staged candidate** — so the bound on object count alone is:
///
/// ```text
///     O  ≤  D · I · W · R  =  0.167 × I × 86,400 s × 2,900 obj/s  =  41.8M · I objects
/// ```
///
/// | Instances | Max objects (parse term only) | With a maximally sparse tree (`d` per object) |
/// |---|---|---|
/// | 1 | 41.8 M | 5.60 M |
/// | 3 (the verification fleet) | 125.5 M | 16.8 M |
/// | 8 | 334.7 M | 44.8 M |
///
/// The right-hand column adds `d = 2.234 ms` per object, the worst case where every L2 directory
/// holds exactly one object; on a dense tree the `readdir` term amortises to near zero and the left
/// column applies.
///
/// **Which term binds has changed, and it is now readdir rather than parse.** In the sparse column
/// `d = 2.234 ms` dominates `1/R = 0.345 ms` by 6.5×, so raising `R` further buys almost nothing
/// there. That is why [`SWEEP_READ_CONCURRENCY`] is applied to the L2 `readdir`s as well as to the
/// `.meta` reads: on the reference tree's 1.66 `.meta` per L2 directory, a serial readdir walk would
/// leave the sweep bounded by the term the measurement says is now the larger one.
///
/// # The rotation IS reachable at `I = 1`, and the previous claim that it needed a fleet was an
/// # artefact
///
/// At the O(100M)-range design point — order 10 M objects — the rotation clears at **`I = 1`** on a
/// dense tree with 4.2× margin, and at **`I = 2`** in the maximally sparse worst case. The published
/// claim was "not reachable at `I = 1` (3.06 M, a factor of 3.3 short) … it first clears at `I = 4`",
/// and that is **withdrawn**: it was computed correctly from `R = 212 obj/s`, which has since been
/// measured to be a concurrency artefact of a scan running one outstanding read.
/// The hardware was never the constraint.
///
/// **`I` genuinely multiplies, and that was tested rather than assumed.** All three proxies swept
/// disjoint cold shard windows on a wall-clock barrier — necessary, because enumeration is ~90 s and
/// a sweep ~5 s, so unaligned runs never overlap — and each sustained its solo rate (2,969 / 3,402 /
/// 2,881, aggregate 9,252 obj/s). What binds is the **per-client** NFS request pipeline; the NIC
/// (7.7 MB/s of ~1,490), instance CPU, `efs-proxy` CPU (0.14-0.21 cores) and the file system's
/// aggregate capacity are all ruled out by measurement. So multiplying by instance count is
/// legitimate rather than hopeful.
///
/// **Still an estimate, and its direction.** `R` was measured on an idle fleet, which is optimistic:
/// a serving fleet contends for the same pipeline, and load cannot be generated on a live shared
/// volume without perturbing it. Separately, the relation between this read-plus-parse rate and a
/// production scan's end-to-end rate is not established — async-8 and rayon-8 agree at ~1,400 obj/s
/// against a recorded end-to-end 212, of which ~1.35 ms/object is this tree's readdir and the
/// remainder is the scan's other per-file work. That gap is pessimistic for any implementation
/// inheriting it. A bound published with its inputs named, per R19.7.
///
/// An operator's levers, unchanged in kind but no longer needed to reach the design point:
///
/// - **More instances.** Works linearly: `I` multiplies the right-hand side, and § 16.2's claims
///   share the work without any instance needing to know `I`.
/// - **Lengthen [`MAX_REPAIR_AGE_SECS`].** Works linearly, and is legitimate here in a way that
///   raising the fold's age bound is not — see that constant. A code change, not a knob.
/// - **Shorten [`LEDGER_REPAIR_INTERVAL_SECS`]: does NOT work.** See that constant; `T` cancels, and
///   shortening it multiplies the per-pass ledger read for no gain.
/// - **Accept Phase L permanently.** The honest fallback. The incumbent counter over-states, so the
///   eviction trigger fires early rather than late — wasteful, not unsafe.
///
/// # No config knob, and the reasoning is § 16.6.4's
///
/// None of these three figures is configurable, for the reason that section gives for the fold: a
/// criterion that an operator can widen stops meaning the same thing on every deployment. Worse
/// here than for the fold, because 3b is a **gate**: a window knob would let an operator satisfy the
/// precondition by widening it until a partial rotation qualified, which is exactly the
/// weakened-predicate resolution this amendment forbids.
///
/// # It is not silent
///
/// [`shard_budget_capped_total`] counts every clamped pass, a rate-limited WARN names the figures,
/// and `/metrics` publishes `ledger_repair_rotation_reachable` plus
/// `ledger_repair_projected_rotation_secs` so an operator learns before waiting out a window. See
/// [`LedgerRepairGauges`].
const REPAIR_SHARD_VISITS_PER_PASS_CEILING: usize = 2_048;

/// Ranges re-appended by this instance since process start.
///
/// **Non-zero is a finding, not a routine reading.** Every increment is a staged range that
/// was on disk, was counted by nothing, and would have stayed invisible. Published as
/// `ledger_repair_ranges_reappended_total`.
static REPAIR_RANGES_REAPPENDED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Passes that refused on an incomplete input, since process start. Published as
/// `ledger_repair_incomplete_total`.
static REPAIR_INCOMPLETE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Sweeps that completed a whole shard, since process start.
static REPAIR_SWEEPS_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Passes whose derived shard budget was clipped by [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`].
///
/// **Non-zero means the repair rotation provably cannot complete inside
/// [`MAX_REPAIR_AGE_SECS`] on this volume**, so § 16.6.6 precondition 3b can never pass and the
/// staged-authority epoch can never flip. Published as
/// `ledger_repair_shard_budget_capped_total`, and mirrored by the
/// `ledger_repair_rotation_reachable` boolean, which is the field to alarm on.
static REPAIR_SHARD_BUDGET_CAPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Unix seconds of the most recent capped-budget WARN.
static LAST_REPAIR_BUDGET_WARN_SECS: AtomicU64 = AtomicU64::new(0);

/// At most one capped-budget WARN per 10 minutes: the condition is structural rather than
/// transient, so it does not need naming on every pass. Same interval, and the same reasoning, as
/// the fold's equivalent.
const REPAIR_BUDGET_WARN_INTERVAL_SECS: u64 = 600;

/// One shard's repair document: when its sweep last completed, and where an in-progress
/// sweep left off.
///
/// # Why the completed timestamp and the cursor are separate fields
///
/// They answer different questions and conflating them is how a partial sweep would come to
/// look like a rotation. `swept_at` advances **only** when a sweep reached the end of the
/// shard, so it is the only field the rotation predicate and the age gauge read. `resume_l2`
/// describes work in flight and says nothing about coverage. A single "last touched"
/// timestamp would let a shard that is repeatedly capped mid-sweep report a fresh rotation it
/// never completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairShardCheckpoint {
    /// Document schema. Unknown ⇒ unreadable.
    pub schema: u32,
    /// Bucket component of the shard.
    pub bucket: String,
    /// First shard level, two lowercase hex digits.
    pub l1: String,
    /// Unix nanos at which a sweep of this shard last **completed**. `0` means never.
    pub swept_at: u64,
    /// Instance that completed it.
    pub swept_by: String,
    /// The next L2 directory name an in-progress sweep should resume from, or `None` when no
    /// sweep is in progress.
    pub resume_l2: Option<String>,
    /// Ranges re-appended by the most recent visit.
    pub ranges_reappended: u64,
    /// `.meta` files read by the most recent visit.
    pub metas_examined: u64,
    /// L2 directories the most recent visit's enumerator yielded.
    ///
    /// **The missing half of `ô`.** Occupancy is `metas_examined / dirs_read`, and until this field
    /// existed the numerator was persisted with no denominator beside it, so no figure the proxy
    /// derived about its own volume could be recovered from the documents it writes. Both counts are
    /// produced by any traversal that runs, so this is free instrumentation rather than new work.
    ///
    /// **`#[serde(default)]` is a compatibility requirement, not tidiness.** A document written by
    /// an earlier binary has no `dirs_read`, and without a default it would fail to parse — which
    /// reaches `RepairRefusal::PreviousCheckpointUnreadable` and stalls the whole rotation on
    /// upgrade rather than degrading. A defaulted `0` reads as "not recorded by the writer", which
    /// is what it means.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.13 (T5, T6.3).
    #[serde(default)]
    pub dirs_read: u64,
    /// Fields written by a future binary, carried through rather than dropped, so a
    /// mixed-version fleet does not lose another writer's additions on our rename.
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

impl RepairShardCheckpoint {
    /// `{cache_dir}/metadata/_shard_state/repair/{bucket}/{XX}.json`.
    pub fn path(cache_dir: &Path, shard: &Shard) -> PathBuf {
        repair_dir(cache_dir)
            .join(&shard.bucket)
            .join(format!("{}.json", shard.l1))
    }

    /// The shard this document names.
    pub fn shard(&self) -> Shard {
        Shard::new(self.bucket.clone(), self.l1.clone())
    }

    fn load_path(path: &Path) -> std::io::Result<Option<Self>> {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let parsed: Self = serde_json::from_str(&content)
            .map_err(|e| std::io::Error::other(format!("unparseable repair checkpoint: {e}")))?;
        if parsed.schema != REPAIR_SCHEMA {
            return Err(std::io::Error::other(format!(
                "unknown repair checkpoint schema {}",
                parsed.schema
            )));
        }
        Ok(Some(parsed))
    }

    /// Install atomically: temp → `sync_all` → rename, with an instance-suffixed temp name so
    /// two instances cannot share one temp path on shared storage.
    pub fn install(&self, cache_dir: &Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir, &self.shard());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::other(format!("serialize repair checkpoint: {e}")))?;
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

fn hostname_for_tmp() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
        .replace(['/', '\\'], "_")
}

/// `{cache_dir}/metadata/_shard_state/repair`.
fn repair_dir(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(REPAIR_DIR)
}

/// `{cache_dir}/metadata/_shard_state/repair_claims`.
fn repair_claims_dir(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(SHARD_STATE_DIR)
        .join(REPAIR_CLAIMS_DIR)
}

/// Every repair document on the volume, with completeness.
///
/// Reports completeness separately from the documents for the same reason the fold's
/// equivalent does: a shard whose document cannot be read must make the rotation **unknown**
/// rather than silently absent from it. An unreadable document that counted as "not yet
/// swept" would be indistinguishable from a shard genuinely awaiting its first sweep, and the
/// second is a normal state that clears itself.
#[derive(Debug, Clone, Default)]
pub struct RepairCheckpointSet {
    by_shard: BTreeMap<Shard, RepairShardCheckpoint>,
    /// Documents the directory walk named.
    pub files_expected: usize,
    /// Documents parsed without error.
    pub files_read: usize,
    /// Documents that could not be opened, parsed, or carried an unknown schema.
    pub files_unreadable: usize,
}

impl RepairCheckpointSet {
    /// Whether every document that exists was read.
    pub fn is_complete(&self) -> bool {
        self.files_unreadable == 0 && self.files_read == self.files_expected
    }

    /// One shard's document, if a visit has installed one.
    pub fn get(&self, shard: &Shard) -> Option<&RepairShardCheckpoint> {
        self.by_shard.get(shard)
    }

    /// Shards with a document.
    pub fn shards(&self) -> BTreeSet<Shard> {
        self.by_shard.keys().cloned().collect()
    }

    /// Age of the **stalest completed sweep** across `population`, seconds.
    ///
    /// `None` when any shard in the population has no completed sweep at all, which is the
    /// honest answer: an age is a statement about coverage, and a shard that has never been
    /// swept has no coverage to age. Returning the maximum over only the shards that *do*
    /// have documents would report healthiest exactly when the rotation had not started.
    pub(crate) fn stalest_completed_age_secs(&self, population: &BTreeSet<Shard>) -> Option<u64> {
        let mut stalest = 0u64;
        for shard in population {
            let doc = self.by_shard.get(shard)?;
            if doc.swept_at == 0 {
                return None;
            }
            stalest = stalest.max(age_secs(doc.swept_at));
        }
        Some(stalest)
    }
}

/// How many times each traversal has run against each cache directory, in a test binary.
///
/// **Keyed by directory**, for the reason `staged_fold`'s equivalent tally is: tests in one binary
/// run concurrently and share every static, so a single counter would let one test's traversals
/// decide another test's assertion. Each test uses its own `TempDir`, so a per-directory tally is
/// race-free without serialising anything.
///
/// Exists so that "one `/metrics` collection reads the repair tree **once**" is an assertion rather
/// than a claim. It is the only thing that can distinguish one traversal from three: all three
/// produce **identical** gauge values, which is exactly why the duplication survived unnoticed, and
/// it is why no published field can be the discriminator here.
#[cfg(test)]
static REPAIR_TRAVERSAL_TALLY: std::sync::Mutex<BTreeMap<(&'static str, u64), u64>> =
    std::sync::Mutex::new(BTreeMap::new());

/// A stable per-directory key for [`REPAIR_TRAVERSAL_TALLY`].
#[cfg(test)]
fn tally_dir_tag(cache_dir: &Path) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    cache_dir.hash(&mut h);
    h.finish()
}

/// Tally one traversal named `which` against `cache_dir`.
#[cfg(test)]
fn note_traversal(which: &'static str, cache_dir: &Path) {
    if let Ok(mut map) = REPAIR_TRAVERSAL_TALLY.lock() {
        *map.entry((which, tally_dir_tag(cache_dir))).or_insert(0) += 1;
    }
}

/// Traversals named `which` against `cache_dir` so far in this test binary.
///
/// `which` is `"repair_docs"` for [`read_repair_checkpoints`] and `"shard_walk"` for
/// [`observed_shards_on_disk`].
#[cfg(test)]
pub(crate) fn traversals_for_tests(which: &'static str, cache_dir: &Path) -> u64 {
    REPAIR_TRAVERSAL_TALLY
        .lock()
        .ok()
        .and_then(|m| m.get(&(which, tally_dir_tag(cache_dir))).copied())
        .unwrap_or(0)
}

/// Read every repair document.
///
/// **An open + read + JSON parse per shard document**, order of hundreds of small files on NFS/EFS
/// — measured at ~1.4 ms per document on the fleet's EFS volume, where a hot re-read is no faster
/// because `lookupcache=pos` makes every open pay a round trip. A caller that needs the set more
/// than once in one operation must read it once and pass it on; see
/// [`collect_ledger_repair_gauges`], which used to pay this three times per `/metrics` scrape.
pub fn read_repair_checkpoints(cache_dir: &Path) -> RepairCheckpointSet {
    #[cfg(test)]
    note_traversal("repair_docs", cache_dir);
    let root = repair_dir(cache_dir);
    let mut set = RepairCheckpointSet::default();
    let buckets = match std::fs::read_dir(&root) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return set,
        Err(e) => {
            warn!(
                "Ledger repair: could not list the repair directory {:?}: {}. The repair \
                 rotation is UNKNOWN rather than incomplete.",
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
                    "Ledger repair: could not list repair documents in {:?}: {}",
                    bucket_path, e
                );
                set.files_expected += 1;
                set.files_unreadable += 1;
                continue;
            }
        };
        for shard_entry in shards.flatten() {
            let path = shard_entry.path();
            // A name ending `.json` is exactly the document; an in-flight
            // `{XX}.json.tmp.{host}.{pid}` never does. Same filter, and the same reason, as
            // the fold's document reader.
            if !path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(".json"))
            {
                continue;
            }
            set.files_expected += 1;
            match RepairShardCheckpoint::load_path(&path) {
                Ok(Some(c)) => {
                    set.files_read += 1;
                    set.by_shard.insert(c.shard(), c);
                }
                Ok(None) | Err(_) => {
                    set.files_unreadable += 1;
                    warn!(
                        "Ledger repair: repair document {:?} could not be read; the rotation \
                         is unknown rather than short by one shard",
                        path
                    );
                }
            }
        }
    }
    set
}

/// The shard population, read from the **metadata tree on disk**.
///
/// # Why from disk and not from the fold's selection union
///
/// The fold's population is the union of shards named by a ledger candidate, by a pending
/// credit, or by an existing fold document. A shard whose staged objects have ALL lost their
/// ledger entries appears in none of the three — no candidate, no credit, and no document
/// because it was never visited. That shard is precisely this rotation's subject, so deriving
/// the population from the fold's sources would exclude the worst case by construction.
///
/// `None` is the UNREADABLE sentinel and is not an empty population: a listing failure must
/// make the rotation unknown, never complete-over-nothing.
///
/// Cost is one readdir of `metadata/` plus one per bucket. Both are small, flat directories —
/// the expensive enumeration is the per-shard L2 walk in [`sweep_shard`], not this.
pub fn observed_shards_on_disk(cache_dir: &Path) -> Option<BTreeSet<Shard>> {
    #[cfg(test)]
    note_traversal("shard_walk", cache_dir);
    let root = cache_dir.join("metadata");
    let buckets = match std::fs::read_dir(&root) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(BTreeSet::new()),
        Err(e) => {
            warn!(
                "Ledger repair: could not list {:?}: {}. Reporting the shard population as \
                 UNREADABLE rather than empty.",
                root, e
            );
            return None;
        }
    };
    let mut shards = BTreeSet::new();
    for bucket_entry in buckets.flatten() {
        let bucket_path = bucket_entry.path();
        if !bucket_path.is_dir() {
            continue;
        }
        let bucket = bucket_entry.file_name().to_string_lossy().to_string();
        // `_journals` and `_shard_state` live beside the buckets under `metadata/`. Skipping
        // by leading underscore rather than by name so a future internal directory is
        // excluded without this function needing to learn about it.
        if bucket.starts_with('_') {
            continue;
        }
        let l1_entries = match std::fs::read_dir(&bucket_path) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Ledger repair: could not list {:?}: {}. Reporting the shard population \
                     as UNREADABLE rather than short by one bucket.",
                    bucket_path, e
                );
                return None;
            }
        };
        for l1_entry in l1_entries.flatten() {
            if !l1_entry.path().is_dir() {
                continue;
            }
            let l1 = l1_entry.file_name().to_string_lossy().to_string();
            if !is_shard_level(&l1) {
                continue;
            }
            shards.insert(Shard::new(bucket.clone(), l1));
        }
    }
    Some(shards)
}

/// Exactly two lowercase hex digits — the `XX` of a sharded path.
fn is_shard_level(name: &str) -> bool {
    name.len() == 2
        && name
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// Why a pass swept nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairRefusal {
    /// The ledger read was incomplete, so the coverage set would be a partial view and
    /// repairing from it would append duplicates.
    LedgerIncomplete,
    /// The metadata tree could not be listed, so the shard population is unknown.
    ShardPopulationUnreadable,
    /// A repair document exists and could not be read, so a cursor or a completed-sweep
    /// timestamp would be abandoned.
    PreviousCheckpointUnreadable,
    /// A shard's own directory tree could not be enumerated mid-sweep.
    ShardUnreadable,
    /// A `.meta` in the shard could not be read, so this shard's coverage is unknown.
    MetaUnreadable,
    /// A re-append failed, so coverage was not actually restored and the sweep must not be
    /// recorded as complete.
    LedgerAppendFailed,
    /// The repair document could not be installed.
    CheckpointWriteFailed,
    /// Every candidate shard was held by a peer, or there was no shard to visit.
    ///
    /// **Not a failure**, and the only variant that is not — same carve-out, and the same
    /// reason, as the fold's `NoShardAvailable`: a peer doing the work is the mechanism
    /// succeeding, and counting it as an incompleteness would make the counter unusable as a
    /// gate.
    NoShardAvailable,
}

impl RepairRefusal {
    /// Short stable label for logs and the gauge set.
    pub fn label(&self) -> &'static str {
        match self {
            Self::LedgerIncomplete => "ledger_incomplete",
            Self::ShardPopulationUnreadable => "shard_population_unreadable",
            Self::PreviousCheckpointUnreadable => "previous_checkpoint_unreadable",
            Self::ShardUnreadable => "shard_unreadable",
            Self::MetaUnreadable => "meta_unreadable",
            Self::LedgerAppendFailed => "ledger_append_failed",
            Self::CheckpointWriteFailed => "checkpoint_write_failed",
            Self::NoShardAvailable => "no_shard_available",
        }
    }

    /// Whether this represents an incomplete input rather than a scheduling outcome.
    pub(crate) fn is_incompleteness(&self) -> bool {
        !matches!(self, Self::NoShardAvailable)
    }
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairPassSummary {
    /// Shards whose sweep completed this pass.
    pub completed: Vec<Shard>,
    /// Shards whose sweep was capped and stored a cursor.
    pub advanced: Vec<Shard>,
    /// Shards held by a live peer claim.
    pub busy: Vec<Shard>,
    /// Shards skipped because their own write-ledger read was incomplete.
    ///
    /// Separate from [`Self::busy`] because the two have opposite meanings for the rotation: a
    /// busy shard is being swept by a peer, while one of these was swept by nobody. Each entry
    /// has already been counted on `ledger_repair_incomplete_total` and WARNed individually, so
    /// a pass that skipped one and completed another is a partial success rather than a refusal.
    pub ledger_incomplete: Vec<Shard>,
    /// Ranges re-appended across the pass.
    pub ranges_reappended: u64,
    /// `.meta` files read across the pass.
    pub metas_examined: u64,
    /// L2 directories enumerated across the pass.
    pub dirs_read: u64,
}

/// The outcome of one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairOutcome {
    /// At least one shard completed or advanced a sweep.
    Swept(RepairPassSummary),
    /// Nothing was swept, and this is why.
    Refused(RepairRefusal, RepairPassSummary),
}

impl RepairOutcome {
    /// Whether this pass completed at least one whole shard sweep. The cadence gate consumes
    /// exactly this, so a refusal does not consume the retry interval.
    pub(crate) fn made_progress(&self) -> bool {
        match self {
            Self::Swept(s) => !s.completed.is_empty() || !s.advanced.is_empty(),
            Self::Refused(..) => false,
        }
    }
}

fn refuse(reason: RepairRefusal, summary: RepairPassSummary) -> RepairOutcome {
    if reason.is_incompleteness() {
        REPAIR_INCOMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
        warn!(
            "Ledger repair refused: reason={}, shards_completed={}, ranges_reappended={}",
            reason.label(),
            summary.completed.len(),
            summary.ranges_reappended
        );
    } else {
        debug!(
            "Ledger repair pass swept nothing: reason={}, shards_busy={}",
            reason.label(),
            summary.busy.len()
        );
    }
    RepairOutcome::Refused(reason, summary)
}

/// Passes available inside one rotation window.
///
/// `W/T` = 86,400 / 300 = **288**. Floored at 1 so a window shorter than an interval still visits.
fn passes_per_window() -> usize {
    (MAX_REPAIR_AGE_SECS / LEDGER_REPAIR_INTERVAL_SECS).max(1) as usize
}

/// Shards to visit this pass, derived from the observed population so the rotation window is
/// met rather than assumed.
///
/// `ceil(observed / passes_per_window)`, clamped to
/// [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`]. Reaching the clamp is recorded rather than silent —
/// see [`rotation_reachable_for`], which is the predicate an operator alarms on.
fn shards_per_pass(observed: usize) -> usize {
    let required = observed.div_ceil(passes_per_window());
    if required > REPAIR_SHARD_VISITS_PER_PASS_CEILING {
        REPAIR_SHARD_BUDGET_CAPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
        log_repair_budget_capped_rate_limited(observed, required);
        return REPAIR_SHARD_VISITS_PER_PASS_CEILING;
    }
    required.max(1)
}

/// The capped-budget WARN's text. A constant so the remedy it names is asserted, not just written.
///
/// "Add a proxy instance" is true because the pass loop treats a peer-claimed shard as `Busy` and
/// moves on without spending budget, and a completed sweep's `swept_at` sorts the shard behind every
/// unswept one, so a second instance's stalest-first selection lands on shards the first has not
/// reached rather than on the ones it just did.
const REPAIR_BUDGET_CAPPED_WARN: &str = "Ledger repair: this volume has more cache shards than one \
     instance can sweep inside the 24 h repair rotation window. On a single-instance fleet the \
     rotation cannot complete, activation precondition 3b never passes, and the fleet stays on the \
     event-sourced counter, which is safe but over-states. Remedy: add a proxy instance. Each \
     instance sweeps up to its per-pass ceiling and claims keep instances on different shards, so \
     the fleet's reach grows with the instance count. Shortening the repair interval is NOT a remedy \
     (it multiplies the per-pass ledger read and leaves total work unchanged), and there is no \
     configuration field for it. ledger_repair_rotation_reachable judges one instance and stays \
     false after the remedy; watch ledger_repair_rotation_complete and \
     ledger_repair_stalest_age_secs to see the fleet complete the rotation.";
fn log_repair_budget_capped_rate_limited(observed: usize, required: usize) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_REPAIR_BUDGET_WARN_SECS.load(Ordering::Relaxed);
    if now >= last + REPAIR_BUDGET_WARN_INTERVAL_SECS
        && LAST_REPAIR_BUDGET_WARN_SECS
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    {
        warn!(
            observed_shard_count = observed,
            shards_per_pass_required = required,
            shards_per_pass_ceiling = REPAIR_SHARD_VISITS_PER_PASS_CEILING,
            max_repair_age_secs = MAX_REPAIR_AGE_SECS,
            approx_buckets = observed / 256,
            shards_reachable_per_instance = shards_reachable_per_instance(),
            "{}",
            REPAIR_BUDGET_CAPPED_WARN
        );
    }
}

/// Whether the repair rotation **can** complete inside [`MAX_REPAIR_AGE_SECS`] at the observed
/// shard population.
///
/// # Why this exists as its own field
///
/// A rotation that can never complete and one that has merely not completed yet produce identical
/// readings on every other gauge: `ledger_repair_rotation_complete` is `false` and
/// `ledger_repair_stalest_age_secs` is `null` or rising in both cases. Without this field an
/// operator cannot tell "wait" from "wait forever", and the difference is a year.
///
/// `false` means the derived per-pass budget was clamped by the ceiling, so the population strictly
/// exceeds `ceiling × passes_per_window` and no sequence of passes covers it inside the window.
/// `None` means the population could not be read, which is neither answer.
///
/// **This is a SCHEDULING statement, not a cost one.** It compares shard count against visits per
/// pass, and cannot see that an individual visit may overrun its time budget on a dense shard — that
/// side is the `O ≤ D·I·W·R` object bound in
/// [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`]'s derivation, which no runtime figure here measures.
/// A `true` here therefore means "not ruled out by scheduling", not "will complete".
///
/// **Takes a shard COUNT, never a directory, and that is deliberate.** A path-taking
/// `rotation_reachable(cache_dir)` wrapper existed until 2026-09-28 and was deleted with zero
/// callers: it walked the metadata tree to derive the count, and it was one of the five metadata
/// walks a single `/metrics` scrape used to pay. The hazard the deletion closes is a future caller
/// reaching for the shorter, path-taking name and silently reintroducing a metadata walk on the
/// scrape path. [`collect_ledger_repair_gauges`] calls this form over the population it has already
/// read; any new caller should do the same, and should NOT add a path-taking convenience wrapper.
///
/// Also split out so the unreachable branch can be asserted at all. Reaching it through the filesystem
/// would mean creating `ceiling × passes_per_window` = 589,824 shard directories in a fixture, which
/// is not a test anyone would run — so the branch would go unverified, and an unverified branch on a
/// permanent-failure alarm is the branch that matters.
fn rotation_reachable_for(observed: usize) -> bool {
    observed.div_ceil(passes_per_window()) <= REPAIR_SHARD_VISITS_PER_PASS_CEILING
}
/// The largest observed population [`rotation_reachable_for`] accepts: the per-pass ceiling times
/// the passes per window. Published as `ledger_repair_shards_reachable_per_instance`.
fn shards_reachable_per_instance() -> u64 {
    (REPAIR_SHARD_VISITS_PER_PASS_CEILING as u64).saturating_mul(passes_per_window() as u64)
}

/// Projected seconds for one full rotation at the observed population and the budget actually in
/// force, so a consumer can compare it against [`MAX_REPAIR_AGE_SECS`] directly.
///
/// `ceil(observed / shards_per_pass) × interval`. Where the budget is clamped this exceeds the
/// window, which is the same condition [`rotation_reachable_for`] reports as a boolean — published as a
/// figure as well because "projected 9 days against a 1 day window" tells an operator how far off
/// they are, where a boolean only says that they are.
///
/// `None` when the population could not be read. `Some(0)` on a volume with no shards.
///
/// **Takes a shard COUNT, never a directory**, with the same reasoning as
/// [`rotation_reachable_for`]: the path-taking `projected_rotation_secs(cache_dir)` wrapper was
/// deleted on 2026-09-28 with zero callers, because it walked the metadata tree and the scrape path
/// must not. Do not reintroduce one — a caller that reaches for the path-taking form silently puts a
/// metadata walk back on `/metrics`.
///
/// `Some(0)` on a volume with no shards is expressed here as a plain `0`; the `Option` lived on the
/// deleted wrapper, where `None` meant the population could not be read.
fn projected_rotation_secs_for(observed: usize) -> u64 {
    if observed == 0 {
        return 0;
    }
    // Deliberately NOT through `shards_per_pass`, which counts a clamp and logs. A gauge read
    // happens on every `/metrics` scrape and must not move a counter.
    let per_pass = observed
        .div_ceil(passes_per_window())
        .clamp(1, REPAIR_SHARD_VISITS_PER_PASS_CEILING);
    (observed.div_ceil(per_pass) as u64).saturating_mul(LEDGER_REPAIR_INTERVAL_SECS)
}

/// `ledger_repair_shard_budget_capped_total`, per-instance since process start.
pub(crate) fn shard_budget_capped_total() -> u64 {
    REPAIR_SHARD_BUDGET_CAPPED_TOTAL.load(Ordering::Relaxed)
}

/// Run one ledger-completeness repair pass with no sampling. Production runs
/// [`run_ledger_repair_sampled`] through the scanner gate; this is the unsampled entry point the
/// tests use.
#[cfg(test)]
pub(crate) async fn run_ledger_repair(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
) -> RepairOutcome {
    run_ledger_repair_sampled(cache_dir, ledger, instance_id, None).await
}

/// [`run_ledger_repair`] with per-visit sampling for the scanner limiter (§ 16.6.13 K2). `None`
/// is exactly [`run_ledger_repair`]. The sampler only reads the scanner registry and feeds the
/// limiter; it changes no claim, budget, install or lease.
pub(crate) async fn run_ledger_repair_sampled(
    cache_dir: &Path,
    ledger: &WriteLedger,
    instance_id: &str,
    sampler: Option<&crate::journal_consolidator::VisitSampler<'_>>,
) -> RepairOutcome {
    let summary = RepairPassSummary::default();

    // The population first: a listing failure must refuse rather than sweep an empty set,
    // because "no shards" and "cannot see the shards" have opposite meanings for a rotation
    // predicate.
    let population = match observed_shards_on_disk(cache_dir) {
        Some(p) => p,
        None => return refuse(RepairRefusal::ShardPopulationUnreadable, summary),
    };
    if population.is_empty() {
        return refuse(RepairRefusal::NoShardAvailable, summary);
    }

    let previous = read_repair_checkpoints(cache_dir);
    if !previous.is_complete() {
        return refuse(RepairRefusal::PreviousCheckpointUnreadable, summary);
    }

    // The coverage set is built PER SHARD, inside the visit loop below, from the same
    // complete-or-refuse reader the fold installs on. It used to be built here, once, from a
    // whole-volume read — see the module doc's "Complete-or-refuse" section for why the scope
    // moved and what makes the narrower scope sound.

    // Stalest first, and a shard mid-sweep ranks with the sweep it has not finished rather
    // than with the coverage it last completed — otherwise a capped shard would be revisited
    // last and could take the whole window to finish one sweep.
    let mut ordered: Vec<Shard> = population.iter().cloned().collect();
    ordered.sort_by_key(|s| {
        let doc = previous.get(s);
        let in_progress = doc.and_then(|d| d.resume_l2.as_ref()).is_some();
        (
            if in_progress { 0u8 } else { 1u8 },
            doc.map(|d| d.swept_at).unwrap_or(0),
            s.clone(),
        )
    });

    let budget = shards_per_pass(ordered.len());
    let mut attempts_remaining = budget.saturating_mul(2);
    let mut summary = summary;

    // **The directory/file split, computed ONCE for the whole pass** (§ 16.6.13 T5). Once per pass
    // rather than once per visit is the third of the three damping measures: a pass visits several
    // shards, and recomputing between them would let one shard's timings move the next shard's split
    // inside a single scheduling decision. The other two are the `α = 1/8` EWMA and the `[1, W−1]`
    // clamp, both in [`compute_directory_concurrency`].
    let dir_concurrency = directory_concurrency_for_pass();
    // The last peer-count reading, for the sampler. See `VisitSampler`.
    let mut n_carry: Option<u32> = None;

    for shard in ordered.iter() {
        if summary.completed.len() + summary.advanced.len() >= budget || attempts_remaining == 0 {
            break;
        }
        attempts_remaining -= 1;
        if claim_repair_shard(cache_dir, shard, instance_id).await == ClaimState::Busy {
            // A busy visit read nothing and records nothing.
            summary.busy.push(shard.clone());
            continue;
        }
        let started = std::time::Instant::now();
        // `n` at this claimed visit's start: visit i's end reading is visit i+1's start. Kept in
        // `n_carry`, so a skipped visit hands it on unchanged.
        let n_start = sampler.map(|s| *n_carry.get_or_insert_with(|| s.peer_count()));
        // THIS shard's coverage, and nothing else: one `readdir` of the shard's ledger
        // directory plus one file per instance that has ever credited it. Read after the claim,
        // so a shard a peer holds costs nothing beyond the claim attempt.
        //
        // # Why a shard-scoped coverage set is sound, and it rests on ONE equivalence
        //
        // A range's ledger entry is appended to `shard_of(cache_key)`'s directory
        // (`WriteLedger::shard_path_for_key`), while this sweep enumerates `.meta` files under
        // `metadata/{bucket}/{XX}/`, which `disk_cache::get_sharded_path` derives from
        // `blake3(object_key)` with `{XX}` its first two hex digits — the identical derivation
        // `staged_fold::shard_of` performs on the same parsed key. So a range whose `.meta` this
        // sweep can reach has its entry in the very directory this read covers, which makes
        // "uncovered in this shard" and "uncovered by any ledger entry at all" the same claim.
        //
        // That equivalence holds because two path builders agree, so it is CHECKED rather than
        // assumed: [`classify_meta`] compares `shard_of(metadata.cache_key)` against the shard
        // being swept and refuses to judge a `.meta` whose content disagrees with its location.
        // Without that check such a file would look uncovered here and be re-appended on every
        // sweep — the duplicate generation this module's own doc names as the danger.
        let read = match ledger
            .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "Ledger repair: could not list shard {}'s write-ledger directory: {}. \
                     Skipping THIS shard's sweep, so it records no completed rotation; its \
                     siblings are unaffected.",
                    shard, e
                );
                REPAIR_INCOMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
                summary.ledger_incomplete.push(shard.clone());
                continue;
            }
        };
        // `is_complete()` is a statement about the files the read was ASKED to cover, and this
        // read was asked to cover one shard — see `LedgerReadOutcome::is_complete`'s own scope
        // note. `truncated` cannot be set here because the cap is 0.
        if !read.is_complete() {
            warn!(
                "Ledger repair: shard {}'s write ledger is incomplete (files_unreadable={}, \
                 malformed_lines={}, files_read={}, files_expected={}), so its coverage set \
                 would be a partial view and a sweep would append a duplicate for every range \
                 sitting in what it could not read. Skipping THIS shard's sweep, so it records \
                 no completed rotation; its siblings are unaffected.",
                shard,
                read.files_unreadable,
                read.malformed_lines,
                read.files_read,
                read.files_expected
            );
            REPAIR_INCOMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
            summary.ledger_incomplete.push(shard.clone());
            continue;
        }
        // Range identity, not entry identity. See the module doc for why the timestamp and the
        // instance are deliberately absent from this key.
        let covered: BTreeSet<(String, u64, u64)> = read
            .entries
            .iter()
            .map(|e| (e.cache_key.clone(), e.range_start, e.range_end))
            .collect();
        let resume_from = previous
            .get(shard)
            .and_then(|d| d.resume_l2.clone())
            .filter(|_| true);
        let swept = match sweep_shard(
            cache_dir,
            shard,
            &covered,
            resume_from,
            ledger,
            dir_concurrency,
        )
        .await
        {
            Ok(s) => s,
            Err(reason) => return refuse(reason, summary),
        };

        summary.ranges_reappended = summary
            .ranges_reappended
            .saturating_add(swept.ranges_reappended);
        summary.metas_examined = summary.metas_examined.saturating_add(swept.metas_examined);
        summary.dirs_read = summary.dirs_read.saturating_add(swept.dirs_read);

        let complete = swept.next_l2.is_none();
        if let Err(e) =
            install_visit_document(cache_dir, shard, previous.get(shard), &swept, instance_id)
        {
            warn!(
                "Ledger repair: could not install the repair document for shard {}: {}. That \
                 shard's previous document is still on disk.",
                shard, e
            );
            return refuse(RepairRefusal::CheckpointWriteFailed, summary);
        }
        // The visit is installed, so the claim has nothing left to protect. Released after a
        // capped visit too: its cursor is in the installed document, which merges monotonically,
        // so a peer resuming it continues rather than repeats.
        release_repair_claim(cache_dir, shard, instance_id);
        if let (Some(s), Some(n_start)) = (sampler, n_start) {
            let n_end = s.peer_count();
            n_carry = Some(n_end);
            s.record(n_start, n_end, swept.metas_examined, started.elapsed());
        }
        if complete {
            REPAIR_SWEEPS_COMPLETED_TOTAL.fetch_add(1, Ordering::Relaxed);
            summary.completed.push(shard.clone());
        } else {
            summary.advanced.push(shard.clone());
        }
    }

    if summary.completed.is_empty() && summary.advanced.is_empty() {
        // A pass blocked entirely by incomplete ledgers is NOT `NoShardAvailable`. That variant
        // is the one carve-out that is deliberately not an incompleteness — a peer doing the
        // work is the mechanism succeeding — and reporting it here would hide a volume whose
        // every candidate shard has an unreadable ledger behind a label that means "healthy".
        //
        // Built directly rather than through `refuse()`: each of these shards has already
        // incremented `ledger_repair_incomplete_total` and WARNed with its own name and class,
        // so routing the pass verdict through the counting helper would count one event twice.
        if !summary.ledger_incomplete.is_empty() {
            warn!(
                "Ledger repair swept nothing: every candidate shard this pass reached had an \
                 incomplete write ledger ({} shard(s)), so the rotation did not advance. Each \
                 shard was reported individually above; this line is the pass verdict and is \
                 not counted again.",
                summary.ledger_incomplete.len()
            );
            return RepairOutcome::Refused(RepairRefusal::LedgerIncomplete, summary);
        }
        return refuse(RepairRefusal::NoShardAvailable, summary);
    }
    if summary.ranges_reappended > 0 {
        // INFO and not DEBUG: every one of these is a staged range that was on disk and
        // counted by nothing. A fleet in steady state should print this line never.
        info!(
            shards_completed = summary.completed.len(),
            shards_advanced = summary.advanced.len(),
            ranges_reappended = summary.ranges_reappended,
            metas_examined = summary.metas_examined,
            "Ledger repair re-appended staged ranges the Write_Ledger had no record of. Each \
             one was excluded from the staged figure, which biases the eviction trigger LATE \
             — the direction § 16.6.2 rules out. Expect write_cache_size to step UP by the \
             repaired bytes at the next fold."
        );
    } else {
        debug!(
            "Ledger repair: {} shard(s) completed, {} advanced, {} .meta read, no gap found",
            summary.completed.len(),
            summary.advanced.len(),
            summary.metas_examined
        );
    }
    RepairOutcome::Swept(summary)
}

/// Build one visit's repair document from the pass's `previous` read and install it, merged
/// monotonically against the document as it is on disk NOW, under a per-document lock.
///
/// `previous_doc` is the document as [`run_ledger_repair`] read it at the START of the pass,
/// which is minutes old by the time a late shard installs. Returns the document now on disk.
///
/// # Why the merge, and why under a lock
///
/// Since the change the repair runs on its own task on every instance, and cross-instance exclusion is
/// only the repair claim, which is not load-bearing: a lost, expired or duplicated claim lets two
/// instances sweep one shard. Before the change each then renamed a document built from its own stale
/// `previous`, and the later rename won. A capped visit carries `previous.swept_at` forward, so
/// installing after a peer's completed sweep replaced `(swept_at = <peer's completion>, None)`
/// with `(<older or 0>, Some(cursor))`: a shard just swept read as stale or never swept, the
/// rotation predicate (§ 16.6.6 precondition 3b) could fall from true to false, and the shard
/// was re-swept. Two capped visits from one basis could likewise move the cursor backwards.
///
/// The harm is bounded to progress (activation delayed, work repeated), never to a byte figure,
/// because the repair's only effect on the ledger is an idempotent-by-coverage append. It is
/// still wrong, and the fix is cheap: this visit already pays a claim write and a coverage read.
///
/// The merge is [`merge_repair_documents`]; see it for the rule. The lock serialises the
/// re-read and the rename, the same shape as `staged_fold::lock_checkpoint_document` for the
/// two folds' shared checkpoint document. It is best-effort for the same reason that one is:
/// failing to acquire proceeds unlocked, which still merges against a read taken microseconds
/// before the rename rather than minutes before it.
///
/// Pinned by `two_instances_sweeping_one_shard_never_move_its_repair_progress_backwards`.
fn install_visit_document(
    cache_dir: &Path,
    shard: &Shard,
    previous_doc: Option<&RepairShardCheckpoint>,
    swept: &ShardSweep,
    instance_id: &str,
) -> std::io::Result<RepairShardCheckpoint> {
    let complete = swept.next_l2.is_none();
    let doc = RepairShardCheckpoint {
        schema: REPAIR_SCHEMA,
        bucket: shard.bucket.clone(),
        l1: shard.l1.clone(),
        // Advances ONLY on a completed sweep. A capped visit carries the previous value
        // forward, so a shard that never finishes never reports a fresh rotation.
        swept_at: if complete {
            now_nanos()
        } else {
            previous_doc.map(|d| d.swept_at).unwrap_or(0)
        },
        swept_by: instance_id.to_string(),
        resume_l2: swept.next_l2.clone(),
        ranges_reappended: swept.ranges_reappended,
        metas_examined: swept.metas_examined,
        dirs_read: swept.dirs_read,
        other: previous_doc.map(|d| d.other.clone()).unwrap_or_default(),
    };
    let _lock = lock_repair_document(cache_dir, shard);
    let path = RepairShardCheckpoint::path(cache_dir, shard);
    let current = match RepairShardCheckpoint::load_path(&path) {
        Ok(current) => current,
        // Unreadable now though readable at pass start (the pass refuses otherwise). Overwrite
        // it, which is what every install did before the change, rather than leave the shard with a
        // document no pass can read.
        Err(e) => {
            debug!(
                "Ledger repair: repair document {:?} is unreadable at install ({}); \
                 replacing it without a merge",
                path, e
            );
            None
        }
    };
    match merge_repair_documents(current, doc) {
        Merged::KeepCurrent(current) => Ok(current),
        Merged::Install(doc) => {
            doc.install(cache_dir)?;
            Ok(doc)
        }
    }
}

/// The outcome of [`merge_repair_documents`].
#[derive(Debug, PartialEq, Eq)]
enum Merged {
    /// The on-disk document is further on than anything this visit can claim; leave it.
    KeepCurrent(RepairShardCheckpoint),
    /// Install this document.
    Install(RepairShardCheckpoint),
}

/// Merge this visit's document against the on-disk one so progress never moves backwards.
///
/// Progress orders as `(swept_at, resume_l2)`: `swept_at` is the last COMPLETED sweep, and at
/// equal `swept_at` a cursor further along the sorted L2 order is further on (`None` meaning no
/// sweep in progress). The rule:
///
/// - **The on-disk `swept_at` is newer**: a peer completed a sweep this visit's basis predates.
///   Keep the on-disk document whole. This visit is either capped (its `swept_at` is the stale
///   basis) or completed a nanosecond-close race the peer won, and in both cases the peer's
///   document already records a completed sweep at least as recent.
/// - **Equal `swept_at`**: both visits continue one sweep from the same completion. Take the
///   further cursor. Sound because a cursor is a low-water mark from the basis's resume point,
///   so the further one's prefix contains the nearer one's.
/// - **Otherwise** this visit is newer: install it.
///
/// `other` is taken from the on-disk document in every case, so a foreign field written since
/// the pass started is carried through rather than reverted.
fn merge_repair_documents(
    current: Option<RepairShardCheckpoint>,
    mut mine: RepairShardCheckpoint,
) -> Merged {
    let Some(current) = current else {
        return Merged::Install(mine);
    };
    if current.swept_at > mine.swept_at {
        return Merged::KeepCurrent(current);
    }
    if current.swept_at == mine.swept_at && current.resume_l2 > mine.resume_l2 {
        mine.resume_l2 = current.resume_l2;
    }
    mine.other = current.other;
    Merged::Install(mine)
}

/// Take the per-shard repair document's lock: an `flock` on `{XX}.json.lock` beside it.
///
/// Mirrors `staged_fold::lock_checkpoint_document`, on a separate file so a repair install and a
/// fold install never contend. [`read_repair_checkpoints`] admits only names ending `.json`, so
/// the lock file is invisible to it. Best-effort: `None` means proceed unlocked.
fn lock_repair_document(cache_dir: &Path, shard: &Shard) -> Option<std::fs::File> {
    let path = RepairShardCheckpoint::path(cache_dir, shard).with_extension("json.lock");
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            debug!(
                "Repair document lock: could not create {:?}: {}. Proceeding unlocked.",
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
                "Repair document lock: could not open {:?}: {}. Proceeding unlocked.",
                path, e
            );
            return None;
        }
    };
    match fs4::FileExt::lock(&file) {
        Ok(()) => Some(file),
        Err(e) => {
            debug!(
                "Repair document lock: could not lock {:?}: {}. Proceeding unlocked.",
                path, e
            );
            None
        }
    }
}

/// What one shard sweep observed.
struct ShardSweep {
    ranges_reappended: u64,
    metas_examined: u64,
    /// L2 directories the enumerator yielded on this visit.
    ///
    /// The denominator of `ô` (§ 16.6.13 T5) and the left-hand side of the T6.3 coverage invariant.
    /// Derived from the enumeration rather than counted where directories are opened — see
    /// [`SweepGate`].
    dirs_read: u64,
    /// The L2 directory to resume from, or `None` when the sweep reached the end.
    next_l2: Option<String>,
}

/// One staged range a `.meta` names that no ledger entry covers.
///
/// Exists because the read phase is now concurrent and the append phase is not: the reads produce
/// these, the single serial appender consumes them. Carrying the four values rather than the parsed
/// `NewCacheMetadata` keeps the window's peak memory proportional to the number of *findings*, which
/// on a healthy fleet is zero, rather than to the number of `.meta` files read.
struct MissingRange {
    cache_key: String,
    start: u64,
    end: u64,
    compressed_size: u64,
}

/// Enumerate one shard's `.meta` files and re-append any staged range the ledger does not
/// cover.
///
/// # The enumeration is two levels, and that is a property of the layout rather than a choice
///
/// `get_sharded_path` builds `metadata/{bucket}/{XX}/{YYY}/{key}.meta`, so a shard
/// `(bucket, XX)` is a directory of up to 4,096 `{YYY}` **directories**, not a directory of
/// `.meta` files. Only L2 directories that have had a file written exist, so the real cost is
/// `O(populated L2 dirs)` readdirs plus one read per `.meta`, which scales with the shard's
/// actual population rather than with 4,096.
///
/// Reading each `.meta` is unavoidable and is the dominant cost: staged-ness lives *inside*
/// the file (`object_metadata.is_write_cached`, then the per-range predicate), so a filename
/// listing cannot tell a lost staged entry from an ordinary read-cached object.
///
/// # Three phases per window: concurrent list, concurrent read, SERIAL append
///
/// This sweep was serial — one awaited `read_to_string` at a time with `std::fs::read_dir`
/// interleaved — which put its outstanding-read count at **1**, the level the measurement records
/// at 174.7 obj/s. It was therefore running roughly 20-33% *below* the 212 obj/s the constants were
/// budgeted against, and an order of magnitude below what the volume can serve. That was an
/// oversight and not a constraint: § 6.3 of the design already prescribes the opposite shape, and
/// the incumbent `.meta` scan already implements it.
///
/// The two constraints that are real, and how the split preserves both:
///
/// - **`append_mutex` + the blocking `fs4` flock** genuinely serialise appends. So appends stay in
///   one serial phase. They are also expected to number zero — every one is a finding — so there
///   was never throughput to win there.
/// - **The single scalar `resume_l2` cursor** requires a total order over L2 directories. So the
///   cursor is advanced only at a **window boundary** over the sorted list, which is a low-water
///   mark: every directory below it is fully swept. Out-of-order completion inside a window cannot
///   be observed by the cursor.
///
/// Neither of the other candidate objections applies. The incumbent scan's `staged_paths` mutex has
/// no analogue here — this sweep's accumulators are function locals plus one relaxed atomic — and
/// readdir interleaving is the *motivation* for concurrency on NFS rather than an obstacle to it.
/// The per-shard claim protocol is cross-**instance** exclusion and imposes no intra-instance
/// ordering.
async fn sweep_shard(
    cache_dir: &Path,
    shard: &Shard,
    covered: &BTreeSet<(String, u64, u64)>,
    resume_from: Option<String>,
    ledger: &WriteLedger,
    dir_concurrency: usize,
) -> std::result::Result<ShardSweep, RepairRefusal> {
    sweep_shard_capped(
        cache_dir,
        shard,
        covered,
        resume_from,
        ledger,
        REPAIR_METAS_PER_VISIT_CAP,
        dir_concurrency,
    )
    .await
}

/// List the L2 directory names under one shard root, `metadata/{bucket}/{XX}/`.
///
/// Extracted from [`sweep_shard_capped`] so the traversal's entry-type predicate is reachable from
/// a test directly, rather than only through a full sweep. A test that had to drive the whole
/// sweep to observe which entries the listing admits would be asserting on a count several stages
/// downstream of the branch, which is the adjacent-value substitution this workstream keeps
/// finding.
///
/// The `Vec<String>` is deliberately of names rather than paths, matching what the caller's resume
/// cursor is a position in.
///
/// Spec: cache-eviction-at-scale § 16.6.13 (T1).
fn list_l2_dirs(
    shard_root: &Path,
    shard: &Shard,
) -> std::result::Result<Vec<String>, RepairRefusal> {
    match std::fs::read_dir(shard_root) {
        Ok(entries) => Ok(entries
            .flatten()
            .filter(crate::disk_cache::is_shard_subdirectory)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect()),
        // A shard directory that has vanished is a real empty shard, not a failure: eviction
        // removes the last `.meta` and the tree with it.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => {
            warn!(
                "Ledger repair: could not list shard {} at {:?}: {}",
                shard, shard_root, e
            );
            Err(RepairRefusal::ShardUnreadable)
        }
    }
}

/// **The single enumerator.** Yield the `.meta` paths in one L2 directory, sorted.
///
/// # This is what makes T6's equivalence hold by CONSTRUCTION rather than by comparison
///
/// Every scheduling the sweep can run — every value of the directory/file split — consumes *this*
/// function and nothing else. There is exactly one place that decides which entries of an L2
/// directory are `.meta` files and exactly one place that orders them, so a missed `.meta`
/// requires this function to be wrong, which fails **every** split identically and cannot be a
/// silent under-count in one of them. A differential test comparing two splits is still worth
/// having (see `both_directory_splits_enumerate_the_same_paths`) but it can only ever cover a
/// fixture, and the tree that matters is live — so the construction is the load-bearing half and
/// the test is the check on top.
///
/// # The two error arms are not symmetric, deliberately
///
/// `NotFound` yields an empty vector: the directory vanished between the L1 listing and this read,
/// which eviction does routinely when it removes a shard's last `.meta`, and that is a real
/// absence rather than a failure to observe. **Every other error refuses.** Treating an unreadable
/// directory as empty is the silent under-count § 16.6.2 rules out by name: the sweep would report
/// a smaller `metas_examined`, the rotation would consider the shard covered, and `total_size`
/// would be re-grounded low. Neither split may soften this for throughput.
///
/// Sorted within the directory so the append order in the sweep's serial phase is a function of
/// the tree rather than of completion order.
///
/// Spec: cache-eviction-at-scale § 16.6.13 (T6).
async fn enumerate_l2_meta_paths(
    cache_dir: &Path,
    l2_path: &Path,
    shard: &Shard,
) -> std::result::Result<Vec<PathBuf>, RepairRefusal> {
    let mut paths: Vec<PathBuf> = Vec::new();
    // Write-order markers (`{object}.wtok`,) share these directories with the `.meta`
    // files. They are never `.meta` and never counted; this listing is the rotation that
    // reaps the stale ones, so a marker whose store never ran does not hold an inode forever.
    let mut markers: Vec<PathBuf> = Vec::new();
    let mut entries = match tokio::fs::read_dir(l2_path).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(paths),
        Err(e) => {
            warn!(
                "Ledger repair: could not list {:?}: {}. Shard {}'s coverage is unknown.",
                l2_path, e, shard
            );
            return Err(RepairRefusal::ShardUnreadable);
        }
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let path = entry.path();
                if path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().ends_with(".meta"))
                {
                    paths.push(path);
                } else if path
                    .extension()
                    .is_some_and(|e| e == crate::cache::WRITE_MARKER_EXTENSION)
                {
                    markers.push(path);
                }
            }
            Ok(None) => break,
            Err(e) => {
                warn!(
                    "Ledger repair: could not read an entry of {:?}: {}. Shard {}'s coverage is \
                     unknown.",
                    l2_path, e, shard
                );
                return Err(RepairRefusal::ShardUnreadable);
            }
        }
    }
    if !markers.is_empty() {
        let dir = cache_dir.to_path_buf();
        let reaped = tokio::task::spawn_blocking(move || {
            markers
                .iter()
                .filter(|m| {
                    crate::cache::reap_write_marker_if_stale(
                        &dir,
                        m,
                        crate::cache::WRITE_MARKER_REAP_AGE,
                    )
                })
                .count()
        })
        .await
        .unwrap_or(0);
        if reaped > 0 {
            debug!(
                "Ledger repair: reaped {} stale write-order markers in {:?}",
                reaped, l2_path
            );
        }
    }
    paths.sort();
    Ok(paths)
}

/// The cap gate's state, and the **only** source of this visit's two derived counts.
///
/// `yields` holds one entry per directory the gate admitted, carrying the number of `.meta` paths
/// [`enumerate_l2_meta_paths`] reported for it. Both figures the visit publishes are computed from
/// it and from nothing else:
///
/// - `dirs_read` is `yields.len()`
/// - `metas_examined` is `yields.iter().sum()`
///
/// That is § 16.6.13 (T6.2)'s "derived from the enumeration rather than accumulated by a parallel
/// counter", and the distinction is the whole point: a parallel counter incremented where files
/// are *read* agrees with itself whatever the enumeration did, so a skipped directory is invisible
/// to it. Derived this way, a skipped directory is arithmetically visible against the L1 listing —
/// see the two invariant checks at the end of [`sweep_shard_capped`].
#[derive(Default)]
struct SweepGate {
    /// Per-directory `.meta` counts, in tree order, exactly as the enumerator reported them.
    yields: Vec<u64>,
    /// The sorted-list index and name of the first directory the cap refused, if any. `None` means
    /// the visit reached the end of the shard.
    stopped_at: Option<(usize, String)>,
    /// Running total of admitted `.meta` yields, used for the cap decision **only**.
    ///
    /// Kept beside `yields` rather than re-summing it per directory, which would be `O(dirs²)`. The
    /// figure the visit *publishes* is still derived from `yields`, so this is a control variable and
    /// not the count — the distinction T6.2 draws.
    admitted_metas: u64,
}

/// One shard visit's raw stage timings, for [`observe_shard_visit`].
///
/// Atomics rather than a lock because the two stages both write to it from inside a fan-out; the
/// whole pipeline runs on one task so there is no contention to speak of, and a mutex here would
/// be a lock taken up to 65,536 times per visit for arithmetic that cannot race meaningfully.
#[derive(Default)]
struct VisitTiming {
    l2_readdir_nanos: AtomicU64,
    l2_readdirs: AtomicU64,
    meta_read_nanos: AtomicU64,
    meta_reads: AtomicU64,
}

impl VisitTiming {
    fn record_l2_readdir(&self, elapsed: std::time::Duration) {
        self.l2_readdir_nanos.fetch_add(
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.l2_readdirs.fetch_add(1, Ordering::Relaxed);
    }

    fn record_meta_read(&self, elapsed: std::time::Duration) {
        self.meta_read_nanos.fetch_add(
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.meta_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn mean_l2_readdir_ms(&self) -> Option<f64> {
        mean_ms(
            self.l2_readdir_nanos.load(Ordering::Relaxed),
            self.l2_readdirs.load(Ordering::Relaxed),
        )
    }

    fn mean_meta_read_ms(&self) -> Option<f64> {
        mean_ms(
            self.meta_read_nanos.load(Ordering::Relaxed),
            self.meta_reads.load(Ordering::Relaxed),
        )
    }
}

fn mean_ms(total_nanos: u64, samples: u64) -> Option<f64> {
    if samples == 0 {
        return None;
    }
    Some(total_nanos as f64 / samples as f64 / 1_000_000.0)
}

/// [`sweep_shard`]'s implementation, with the per-visit cap and the directory-stage width as
/// parameters.
///
/// The cap is a parameter so the resume-cursor path can be exercised at a cap of 1 rather than by
/// seeding 20,000 objects into a fixture. A test that had to build the production cap's worth of
/// state to reach the branch would not be run, and the branch would go unverified — which for a
/// cursor is the branch that matters, since a broken cursor either loops on one prefix forever or
/// skips the rest of the shard silently.
///
/// The width is a parameter for a second reason: it keeps the process-wide
/// [`TRAVERSAL_ESTIMATES`] off the tested path entirely. A test that read the static could be
/// perturbed by an estimate a concurrently running test moved, and the failure would present as a
/// flake in whichever test happened to run second.
async fn sweep_shard_capped(
    cache_dir: &Path,
    shard: &Shard,
    covered: &BTreeSet<(String, u64, u64)>,
    resume_from: Option<String>,
    ledger: &WriteLedger,
    metas_cap: u64,
    dir_concurrency: usize,
) -> std::result::Result<ShardSweep, RepairRefusal> {
    sweep_shard_recorded(
        cache_dir,
        shard,
        covered,
        resume_from,
        ledger,
        metas_cap,
        dir_concurrency,
        None,
    )
    .await
}

/// Read and parse one `.meta`, and decide which of the staged ranges it names no ledger entry
/// covers.
///
/// Extracted from the pipeline below so the per-file stage is one named thing rather than a block
/// nested three combinators deep. Pure with respect to shared state — `covered` is read-only for
/// the whole pass and `is_staged_range_spec` is a function — which is what makes the fan-out safe
/// rather than merely fast.
async fn classify_meta(
    meta_path: &Path,
    covered: &BTreeSet<(String, u64, u64)>,
    shard: &Shard,
) -> std::result::Result<Vec<MissingRange>, RepairRefusal> {
    let content = match tokio::fs::read_to_string(meta_path).await {
        Ok(c) => c,
        // Vanished between the listing and the read: eviction or invalidation removed it, which is
        // a real absence and not a failure to observe.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            warn!(
                "Ledger repair: could not read {:?}: {}. Shard {}'s coverage is unknown, so no \
                 sweep is recorded for it.",
                meta_path, e, shard
            );
            return Err(RepairRefusal::MetaUnreadable);
        }
    };
    let metadata: crate::cache_types::NewCacheMetadata = match serde_json::from_str(&content) {
        Ok(m) => m,
        Err(e) => {
            // A `.meta` that cannot be parsed might name staged ranges this pass cannot see.
            // Refusing keeps the direction safe: a shard whose coverage is unknown does not get a
            // completed-sweep timestamp, so it does not contribute a rotation it did not earn.
            warn!(
                "Ledger repair: could not parse {:?}: {}. Shard {}'s coverage is unknown.",
                meta_path, e, shard
            );
            return Err(RepairRefusal::MetaUnreadable);
        }
    };
    if !metadata.object_metadata.is_write_cached {
        return Ok(Vec::new());
    }
    // **The equivalence the shard-scoped coverage set rests on, checked rather than assumed.**
    //
    // The caller's `covered` set covers ONE shard's ledger files. Coverage is looked up by the
    // `.meta`'s own `cache_key` field, while the file was reached by walking this shard's
    // directory tree — two different routes to the same shard, which agree only because
    // `get_sharded_path` and `shard_of` both take `blake3(object_key)`'s first two hex digits.
    //
    // If they ever disagree for this file, its entry is in another shard's directory, the set
    // read here cannot see it, every one of its staged ranges looks uncovered, and the sweep
    // appends a duplicate for each — on every rotation, forever. So a `.meta` whose content
    // disagrees with its location is skipped and reported rather than judged.
    //
    // WARN and not `debug!`: this is unreachable by construction, so a single line means a
    // `.meta` has been written to, or moved to, a path its own key does not derive.
    match crate::staged_fold::shard_of(&metadata.cache_key) {
        Some((bucket, l1)) if bucket == shard.bucket && l1 == shard.l1 => {}
        other => {
            warn!(
                "Ledger repair: {:?} sits in shard {} but its cache key {:?} derives {:?}. This \
                 shard's ledger cannot answer whether that key's ranges are covered, so they are \
                 left alone — judging them here would append a duplicate for every range on \
                 every sweep. A .meta whose content disagrees with its location is a corruption \
                 finding, not a coverage gap.",
                meta_path, shard, metadata.cache_key, other
            );
            return Ok(Vec::new());
        }
    }
    let mut missing: Vec<MissingRange> = Vec::new();
    for range in &metadata.ranges {
        // Per-range membership through the single shared predicate, so this rotation's notion of
        // "staged" is the fold's and eviction's rather than a third copy.
        if !crate::cache_types::is_staged_range_spec(
            range,
            metadata.object_metadata.is_write_cached,
        ) {
            continue;
        }
        let identity = (metadata.cache_key.clone(), range.start, range.end);
        if covered.contains(&identity) {
            continue;
        }
        missing.push(MissingRange {
            cache_key: metadata.cache_key.clone(),
            start: range.start,
            end: range.end,
            compressed_size: range.compressed_size,
        });
    }
    Ok(missing)
}

/// [`sweep_shard_capped`]'s implementation, with an optional recorder for the paths it examined.
///
/// # ONE pipeline, replacing the two-phase window
///
/// The sweep used to run directory reads and file reads as two separate `W`-wide phases that did
/// not overlap: every readdir of a window had to complete before the first `.meta` of that window
/// was read, and every `.meta` had to complete before the next window's first readdir was issued.
/// Two full drains per window. It is now one pipeline: the directory stage keeps `W_d` readdirs
/// outstanding and the file stage keeps `W_f` reads outstanding, `W_d + W_f = W`, and polling the
/// file stage is what drives the directory stage — so both are in flight at once and there is no
/// barrier at a window boundary because there are no longer any windows.
///
/// **What this does and does not buy, stated because § 16.6.13's arithmetic overstates it.** That
/// section costs the two-phase sweep at `(L·d̂ + m·r̂)/W` against a unified `max(L·d̂, m·r̂)/W`,
/// making the penalty maximal at the crossover. The algebra is right; the premise is not. The `max`
/// form requires each stage to run at the **full** width `W` simultaneously, i.e. `2W` outstanding
/// operations, and `W` is the measured knee of *total* outstanding operations, where doubling to 128
/// buys 2.8% (see [`SWEEP_READ_CONCURRENCY`]). Under a shared budget with the balanced split below,
/// the unified pipeline's cost is `L·d̂/W_d = (L·d̂ + m·r̂)/W` — *identical* to the two-phase cost.
/// The two expressions also mix kinds: `d̂` is a per-operation latency while `r̂ = 1/R` is a
/// throughput reciprocal, and dividing the second by `W` again double-counts concurrency on the
/// file term.
///
/// So the modelled throughput gain is **zero**, and what this change actually removes is the two
/// full-drain barriers per window — a tail-latency effect on a backend whose per-operation latency
/// at the knee is 16.7 ms, real and not quantified by any recorded run.
///
/// The consequence for [`compute_directory_concurrency`] is worth being explicit about: T5 is what
/// stops T4 being **worse**, rather than the two together being faster. Two sequential phases
/// achieve the balanced cost by construction; one pipeline with a badly chosen split bottlenecks on
/// its slower stage and wastes the other stage's width.
///
/// # The directory stage is ORDERED, and that is load-bearing
///
/// `buffered` rather than `buffer_unordered`: it keeps `W_d` readdirs in flight while yielding their
/// results in **input order**. That is what makes the cap's stop point a property of the tree rather
/// than of I/O timing, which in turn is what makes the resume cursor a low-water mark over the
/// sorted L2 order and makes every value of `W_d` enumerate the same paths. With an unordered
/// directory stage the visit would stop at whichever directory happened to complete when the cap was
/// reached, so two runs of the same visit would cover different sets and the cursor would silently
/// skip the directories that finished late.
///
/// The **file** stage is unordered, because nothing downstream depends on read-completion order: the
/// serial append phase sorts back into tree order explicitly.
///
/// Spec: cache-eviction-at-scale § 16.6.13 (T4, T5, T6).
#[allow(clippy::too_many_arguments)]
async fn sweep_shard_recorded(
    cache_dir: &Path,
    shard: &Shard,
    covered: &BTreeSet<(String, u64, u64)>,
    resume_from: Option<String>,
    ledger: &WriteLedger,
    metas_cap: u64,
    dir_concurrency: usize,
    examined_paths: Option<&mut Vec<PathBuf>>,
) -> std::result::Result<ShardSweep, RepairRefusal> {
    use futures::stream::{StreamExt, TryStreamExt};

    let shard_root = cache_dir
        .join("metadata")
        .join(&shard.bucket)
        .join(&shard.l1);
    let mut l2_dirs: Vec<String> = list_l2_dirs(&shard_root, shard)?;
    // Sorted so the resume cursor is a position in a stable ordering. Without a total order a
    // cursor names nothing and a capped sweep could revisit the same prefix forever.
    l2_dirs.sort();
    // **The count the L1 listing named**, which is one side of the T6.3 invariant below. Captured
    // here, before anything is consumed, so the check compares the enumeration against the listing
    // rather than against itself.
    let l1_named = l2_dirs.len();

    let start = match &resume_from {
        Some(cursor) => l2_dirs.partition_point(|d| d < cursor),
        None => 0,
    };

    // The split. Clamped again here even though [`compute_directory_concurrency`] already clamps,
    // because this function is reachable from tests with an arbitrary width and a `W_f` of zero
    // deadlocks the pipeline rather than merely slowing it.
    let width = SWEEP_READ_CONCURRENCY;
    let w_d = dir_concurrency.clamp(1, width.saturating_sub(1).max(1));
    let w_f = width.saturating_sub(w_d).max(1);

    let gate = std::sync::Mutex::new(SweepGate::default());
    let gate_ref = &gate;
    let timing = VisitTiming::default();
    let timing_ref = &timing;

    // The queue is OWNED `(index, name)` pairs rather than borrowed out of `l2_dirs`. Not a style
    // choice: a closure taking `(usize, &String)` implements `FnOnce` only for the one lifetime the
    // borrow has, and the buffered future then needs it for *any* lifetime — which surfaces at the
    // spawn site in `main.rs` as "implementation of `FnOnce` is not general enough … must implement
    // `FnOnce<((usize, &'1 String),)>`, for any two lifetimes". Owned `String`s remove the lifetime
    // from the closure's signature entirely. The cost is up to 4,096 short string clones per visit.
    let queue: Vec<(usize, String)> = l2_dirs[start.min(l1_named)..]
        .iter()
        .enumerate()
        .map(|(offset, name)| (start + offset, name.clone()))
        .collect();

    let mut found: Vec<(usize, PathBuf, Vec<MissingRange>)> =
        futures::stream::iter(queue.into_iter().map(|(index, name)| {
            let l2_path = shard_root.join(&name);
            async move {
                let started = std::time::Instant::now();
                // **The single enumerator.** Every split consumes this and nothing else.
                let paths = enumerate_l2_meta_paths(cache_dir, &l2_path, shard).await?;
                timing_ref.record_l2_readdir(started.elapsed());
                Ok::<(usize, String, Vec<PathBuf>), RepairRefusal>((index, name, paths))
            }
        }))
        // The directory stage. ORDERED — see the doc comment.
        .buffered(w_d)
        // The cap gate. It stops the visit at an L2-directory BOUNDARY, so the cap's overshoot is one
        // directory rather than a whole window — the same guarantee the two-phase version's
        // per-window trim gave, obtained here without needing to know a window's yield in advance.
        // The first directory is always admitted, so a single directory holding more than the entire
        // cap still makes progress instead of looping on itself forever.
        //
        // Terminating the stream here is what makes the cap bound *work* rather than merely bound
        // the count: the directories past the stop point are never enumerated at all. Directories the
        // gate refuses are re-listed on the next visit, and at most `W_d` readdirs already in flight
        // when the gate fired are discarded — the same cost the two-phase version paid for its
        // window trim.
        .try_take_while(|entry: &(usize, String, Vec<PathBuf>)| {
            let admit = {
                let mut state = gate_ref.lock().expect("sweep gate mutex poisoned");
                let yielded = entry.2.len() as u64;
                let would_examine = state.admitted_metas.saturating_add(yielded);
                if !state.yields.is_empty() && would_examine > metas_cap {
                    state.stopped_at = Some((entry.0, entry.1.clone()));
                    false
                } else {
                    state.yields.push(yielded);
                    state.admitted_metas = would_examine;
                    true
                }
            };
            futures::future::ready(Ok::<bool, RepairRefusal>(admit))
        })
        // Flatten `(directory -> entries)` into a stream of `(directory index, path)`. The index
        // travels with the path so tree order is recoverable after the unordered read stage below.
        .map_ok(|(index, _name, paths)| {
            futures::stream::iter(
                paths
                    .into_iter()
                    .map(move |path| Ok::<(usize, PathBuf), RepairRefusal>((index, path))),
            )
        })
        .try_flatten()
        .map_ok(|(index, meta_path)| async move {
            let started = std::time::Instant::now();
            let missing = classify_meta(&meta_path, covered, shard).await?;
            timing_ref.record_meta_read(started.elapsed());
            Ok::<(usize, PathBuf, Vec<MissingRange>), RepairRefusal>((index, meta_path, missing))
        })
        // The file stage. Unordered, because the append phase sorts back into tree order.
        .try_buffer_unordered(w_f)
        // `try_collect` short-circuits on the first refusal, which is the serial version's
        // semantics: a shard whose coverage cannot be established gets no completed-sweep
        // timestamp, so it contributes no rotation it did not earn.
        .try_collect()
        .await?;

    let (yields, stopped_at) = {
        let state = gate.lock().expect("sweep gate mutex poisoned");
        (state.yields.clone(), state.stopped_at.clone())
    };

    // ---- The two derived counts. BOTH come from the enumerator's yields and from nothing else.
    let dirs_read = yields.len() as u64;
    let metas_examined: u64 = yields.iter().copied().fold(0u64, u64::saturating_add);

    // **T6.3 — `dirs_read` must equal the count the L1 listing named.** The two sides are derived
    // independently and that is what makes the check non-vacuous: `advance` is index arithmetic over
    // the sorted L1 listing, while `dirs_read` counts directories the enumerator actually yielded. A
    // directory dropped anywhere in the pipeline makes the second short of the first while every
    // other figure still looks self-consistent.
    //
    // A shortfall is REFUSED, not installed. `ShardUnreadable` already declines to record a
    // completed-sweep timestamp for a shard whose coverage is unknown, and that is exactly this
    // situation: treating a directory the sweep never saw as empty is the silent under-count
    // § 16.6.2 rules out by name. Neither split may soften this for throughput.
    let advance = match &stopped_at {
        Some((index, _)) => index.saturating_sub(start),
        None => l1_named.saturating_sub(start),
    } as u64;
    if dirs_read != advance {
        warn!(
            shard = %shard,
            dirs_read,
            l2_positions_advanced = advance,
            l1_listing_named = l1_named,
            resume_index = start,
            "Ledger repair: the sweep advanced past {} L2 directory position(s) but the enumerator \
             yielded {}. A directory the L1 listing named was not enumerated, so this shard's \
             coverage is UNKNOWN and treating it as covered would be a silent under-count. Refusing \
             the visit; no completed-sweep timestamp is recorded.",
            advance,
            dirs_read
        );
        return Err(RepairRefusal::ShardUnreadable);
    }

    // **T6.2 — `metas_examined` must equal what was actually read.** The left side is the sum over
    // directories of the `.meta` entries the enumerator yielded; the right side is the number of
    // results the file stage produced. Equal by construction unless a file was dropped between the
    // two, which is the one failure an enumeration-derived count cannot see on its own.
    if metas_examined != found.len() as u64 {
        warn!(
            shard = %shard,
            metas_enumerated = metas_examined,
            metas_read = found.len(),
            dirs_read,
            "Ledger repair: the enumerator yielded {} .meta path(s) and the read stage returned {}. \
             A file was dropped between enumeration and read, so this shard's coverage is UNKNOWN. \
             Refusing the visit.",
            metas_examined,
            found.len()
        );
        return Err(RepairRefusal::ShardUnreadable);
    }

    // Tree order, because `Ledger_Seq` is assigned by the append itself, so discovery order cannot
    // affect correctness — but a reproducible order makes two runs of the same visit comparable.
    // With reads completing out of order this sort is the only thing supplying it.
    found.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));

    if let Some(record) = examined_paths {
        // The recorder observes the production data structure, so a differential test comparing two
        // splits compares what the sweep really enumerated rather than a parallel
        // re-implementation of it.
        record.extend(found.iter().map(|(_, path, _)| path.clone()));
    }

    // ---- Append SERIALLY, in tree order.
    //
    // Serial because `WriteLedger::append` holds a process-wide `append_mutex` across a blocking
    // `fs4` flock: concurrent appends would queue on it and buy nothing. It is also the phase that
    // is expected never to execute at all — every increment of `REPAIR_RANGES_REAPPENDED_TOTAL` is
    // a finding rather than a routine reading — so there is no throughput argument for
    // parallelising it even if the lock allowed it.
    //
    // It runs AFTER the two invariant checks above, deliberately: a visit whose coverage arithmetic
    // does not add up appends nothing at all.
    let mut ranges_reappended = 0u64;
    // The range identities this visit re-appended, for the pending-credit removal below. Empty on
    // a healthy volume, which is where the removal's cost lives: no finding, no survey.
    let mut reappended: Vec<PendingRange> = Vec::new();
    for (_, _, missing) in &found {
        for range in missing {
            // `append_staged_range` and not a raw append: it writes the ledger entry and its paired
            // `Pending_Credit` with the same `Ledger_Seq`, which is what keeps the repaired bytes on
            // exactly one side of every shard's cut. Writing only the ledger half would leave the
            // bytes counted by neither term until the next fold — § 16.6.1's forbidden outcome,
            // reintroduced by the repair meant to prevent it.
            if let Err(e) = ledger
                .append_staged_range(
                    &range.cache_key,
                    range.start,
                    range.end,
                    range.compressed_size,
                )
                .await
            {
                warn!(
                    "Ledger repair: could not re-append {} [{}-{}]: {}. Coverage was NOT restored, \
                     so this shard records no completed sweep and the rotation stays incomplete.",
                    range.cache_key, range.start, range.end, e
                );
                return Err(RepairRefusal::LedgerAppendFailed);
            }
            ranges_reappended += 1;
            reappended.push(PendingRange {
                cache_key: range.cache_key.clone(),
                start: range.start,
                end: range.end,
            });
            REPAIR_RANGES_REAPPENDED_TOTAL.fetch_add(1, Ordering::Relaxed);
            warn!(
                cache_key = %range.cache_key,
                range_start = range.start,
                range_end = range.end,
                compressed_size = range.compressed_size,
                shard = %shard,
                "Ledger repair: a staged range on disk had NO Write_Ledger entry. It was excluded \
                 from the staged figure and invisible to staging eviction. Re-appended."
            );
        }
    }

    // ---- Remove the stale pending credits of every range just re-appended.
    //
    // Runs AFTER the appends, which is what makes this visit's own fresh credits safe from it.
    purge_unpaired_credits(cache_dir, shard, ledger, &reappended).await;

    // ---- Fold this visit's measurements into the volume-wide estimates (T5).
    //
    // Only on a visit that reached this point: a refused visit stopped part-way through for a
    // reason unrelated to the backend's speed, so its partial timings describe no operating point.
    //
    // The occupancy sample's denominator is directories **admitted**, which includes any that
    // enumerated zero `.meta` files because they had just been emptied. That biases `ô` low and so
    // `W_d` high — the directory-bound direction, which is what the two-phase version did
    // unconditionally, so the bias is toward the incumbent behaviour rather than away from it.
    if dirs_read > 0 {
        observe_shard_visit(
            timing.mean_l2_readdir_ms(),
            timing.mean_meta_read_ms(),
            Some(metas_examined as f64 / dirs_read as f64),
        );
    }

    Ok(ShardSweep {
        ranges_reappended,
        metas_examined,
        dirs_read,
        next_l2: stopped_at.map(|(_, name)| name),
    })
}

/// Remove every pending credit that names a range this visit re-appended and that **no ledger
/// entry pairs**.
///
/// # The state this exists for
///
/// A group write torn by a crash leaves its `Pending_Credit` durable and its ledger line gone.
/// `WriteLedger::append` writes a shard file's header with `max_seq_ever: 0` and never raises it,
/// and compaction rebuilds the header from the entries that survived, so the torn line's
/// `Ledger_Seq` is unrecoverable and the losing instance's `staged_fold_cut` for that shard stays
/// **below** its own durable credit. The sweep above then restores `F`'s half under the
/// *repairing* instance's identity, so `PendingReadOutcome::post_cut` returns the stale credit on
/// top of a folded shard figure that already counts the `.meta` — § 16.6.1's counted-by-both, and
/// permanent, because nothing retires a dead `{hostname}:{pid}`'s pending file and the pending GC
/// drops a record only when its own shard's cut covers it.
///
/// Both consumers are wrong, in opposite directions: staging eviction fires early (wasteful), and
/// `read_cache_size = total_size − resident_staged_bytes` is under-stated, so **read-tier
/// eviction fires late** — the direction that fills a shared volume.
///
/// # Why this cannot drop a credit that IS paired
///
/// Two independent arguments, and the first is the one the interleaving turns on.
///
/// 1. **Ordering.** A credit is written only after its ledger entry is durable: the appender's
///    pending stage receives synced entries only, and `WriteLedger::append_staged_range` writes
///    the entry before the credit. So for every line the survey read, that line's paired entry
///    was on disk **before** the survey, and the survey happens **before** the ledger read at
///    step 2 — therefore a paired line's entry is in that read and the line is kept. The removal
///    then matches the surveyed text byte for byte, so a credit appended after the survey is not
///    in the set at all and is kept whatever it says. Both halves of a concurrent re-PUT of the
///    same range are covered by those two sentences: its credit is either visible to the survey,
///    and then provably paired, or invisible to it, and then never a candidate.
/// 2. **The range is already counted exactly once.** Every candidate names a range for which
///    *this pass* has just appended a fresh ledger entry and its paired credit, and whose `.meta`
///    the sweep read from disk. So the range's bytes are held by the fresh credit until a fold's
///    cut passes it and by `F` thereafter. A credit removed here was a second count of a range
///    recorded once.
///
/// # What it declines to do
///
/// A shard whose fresh ledger read is incomplete, a pending file that cannot be listed, locked,
/// read or parsed: nothing is removed. Every refusal leaves the over-count standing, which fires
/// staging eviction early; removing a live credit would fire read-tier eviction late. The
/// asymmetry is deliberate.
///
/// Spec: cache-eviction-at-scale §§ 16.6.1, 16.6.6.
async fn purge_unpaired_credits(
    cache_dir: &Path,
    shard: &Shard,
    ledger: &WriteLedger,
    ranges: &[PendingRange],
) {
    if ranges.is_empty() {
        return;
    }
    let store = PendingStore::new(cache_dir.to_path_buf());
    // 1. The survey, each instance's file under its own lock.
    let candidates = store.credits_naming(ranges).await;
    if candidates.is_empty() {
        return;
    }
    // 2. A FRESH shard ledger read, strictly after the survey and after this visit's appends.
    //    Not the `covered` set the sweep was given: that was read before the appends, so it
    //    pairs neither this visit's own new entries nor a concurrent writer's.
    let read = match ledger
        .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
        .await
    {
        Ok(r) if r.is_complete() => r,
        Ok(r) => {
            warn!(
                shard = %shard,
                files_unreadable = r.files_unreadable,
                malformed_lines = r.malformed_lines,
                "Ledger repair: the pairing read of shard {}'s write ledger is incomplete, so a \
                 credit it cannot see would look unpaired. No pending credit is removed; the \
                 stale ones stay counted until a later pass reads the shard whole.",
                shard
            );
            return;
        }
        Err(e) => {
            warn!(
                "Ledger repair: could not re-read shard {}'s write ledger to pair its pending \
                 credits: {}. No pending credit is removed.",
                shard, e
            );
            return;
        }
    };
    let paired: BTreeSet<(&str, u64)> = read
        .entries
        .iter()
        .filter_map(|e| e.seq.map(|s| (e.instance_id.as_str(), s)))
        .collect();
    // 3. Drop only the surveyed lines that read cannot pair.
    let mut unpaired: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for candidate in &candidates {
        if paired.contains(&(candidate.instance.as_str(), candidate.seq)) {
            continue;
        }
        unpaired
            .entry(candidate.instance.clone())
            .or_default()
            .insert(candidate.line.clone());
    }
    if unpaired.is_empty() {
        return;
    }
    let instances: Vec<&String> = unpaired.keys().collect();
    let dropped = store.drop_lines(&unpaired).await;
    if dropped > 0 {
        warn!(
            shard = %shard,
            dropped,
            instances = ?instances,
            "Ledger repair: {} pending credit(s) named a range this sweep re-appended and no \
             ledger entry paired them — the signature of a group write torn by a crash. They \
             were counted on top of the same range's .meta bytes, over-stating the staged figure \
             and under-stating the read tier. Removed.",
            dropped
        );
    }
}

/// Whether the ledger-completeness repair rotation has covered every observed shard within
/// [`MAX_REPAIR_AGE_SECS`].
///
/// § 16.6.6's activation precondition **3b**, expressed as observable state. Returns a bool
/// and never a figure, the same containment the fold's rotation predicate uses.
///
/// `false` on any of: an unreadable metadata tree, an unreadable repair document, a shard with
/// no completed sweep, or a stalest completed sweep beyond the window. All four are "we cannot
/// show a rotation completed", which is what the gate needs.
pub fn repair_rotation_complete(cache_dir: &Path) -> bool {
    repair_rotation_complete_from(
        observed_shards_on_disk(cache_dir).as_ref(),
        &read_repair_checkpoints(cache_dir),
    )
}

/// [`repair_rotation_complete`]'s predicate, over an already-read population and document set.
///
/// Split out so a caller that needs several of these figures in one operation reads the tree once
/// rather than once per figure. `population` is `None` for the UNREADABLE sentinel, exactly as
/// [`observed_shards_on_disk`] returns it, so the "cannot see the shards" branch survives the split
/// — collapsing it to an empty set would turn an unreadable tree into a vacuous `true`, which is the
/// one answer this predicate must never give.
fn repair_rotation_complete_from(
    population: Option<&BTreeSet<Shard>>,
    docs: &RepairCheckpointSet,
) -> bool {
    RepairReadinessEvidence::from_read(population, docs).rotation_complete_after(0)
}

/// Both repair readiness figures' input, reduced from one population walk and one document read.
///
/// The single definition behind [`repair_rotation_complete_from`] and
/// [`repair_stalest_age_secs_from`], so `JournalConsolidator::maintain_staged_authority` can hold
/// it across cycles and re-evaluate the age against a later `now` without re-reading the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepairReadinessEvidence {
    /// The population was unreadable, a document was unreadable, or some observed shard has no
    /// completed sweep. Never becomes complete by ageing.
    Unavailable,
    /// No shard has ever been written, so there is no coverage to claim and nothing that could be
    /// missing. Vacuously complete — and reachable only on a volume with no cached objects at all,
    /// where activation changes nothing either way.
    EmptyPopulation,
    /// Every observed shard has a completed sweep; the stalest is this many seconds old.
    StalestAge(u64),
}

impl RepairReadinessEvidence {
    /// Reduce one read. `population` is `None` for the UNREADABLE sentinel.
    pub(crate) fn from_read(
        population: Option<&BTreeSet<Shard>>,
        docs: &RepairCheckpointSet,
    ) -> Self {
        let Some(population) = population else {
            return Self::Unavailable;
        };
        if population.is_empty() {
            return Self::EmptyPopulation;
        }
        if !docs.is_complete() {
            return Self::Unavailable;
        }
        match docs.stalest_completed_age_secs(population) {
            Some(age) => Self::StalestAge(age),
            None => Self::Unavailable,
        }
    }

    /// Precondition 3b over evidence read `elapsed_secs` ago. The caller must round UP, so the
    /// age is never younger than a fresh read would report and a cached `true` turns `false` no
    /// later than the uncached predicate would.
    pub(crate) fn rotation_complete_after(self, elapsed_secs: u64) -> bool {
        match self {
            Self::Unavailable => false,
            Self::EmptyPopulation => true,
            Self::StalestAge(age) => age.saturating_add(elapsed_secs) <= MAX_REPAIR_AGE_SECS,
        }
    }

    /// The published stalest age over evidence read `elapsed_secs` ago.
    pub(crate) fn stalest_age_secs_after(self, elapsed_secs: u64) -> Option<u64> {
        match self {
            Self::Unavailable => None,
            Self::EmptyPopulation => Some(0),
            Self::StalestAge(age) => Some(age.saturating_add(elapsed_secs)),
        }
    }
}

/// Age of the stalest completed sweep across the observed population, seconds.
///
/// `None` is UNREADABLE-or-incomplete, never a healthy zero — the same sentinel discipline the
/// fold's gauges use. Published so an operator can watch precondition 3b converge instead of
/// being told only that activation is blocked.
pub fn repair_stalest_age_secs(cache_dir: &Path) -> Option<u64> {
    repair_stalest_age_secs_from(
        observed_shards_on_disk(cache_dir).as_ref(),
        &read_repair_checkpoints(cache_dir),
    )
}

/// [`repair_stalest_age_secs`]'s derivation, over an already-read population and document set.
///
/// Same reason for existing as [`repair_rotation_complete_from`], and the same `None`-is-UNREADABLE
/// contract on `population`.
fn repair_stalest_age_secs_from(
    population: Option<&BTreeSet<Shard>>,
    docs: &RepairCheckpointSet,
) -> Option<u64> {
    RepairReadinessEvidence::from_read(population, docs).stalest_age_secs_after(0)
}

/// `ledger_repair_ranges_reappended_total`, per-instance since process start.
pub(crate) fn ranges_reappended_total() -> u64 {
    REPAIR_RANGES_REAPPENDED_TOTAL.load(Ordering::Relaxed)
}

/// `ledger_repair_incomplete_total`, per-instance since process start.
pub(crate) fn incomplete_total() -> u64 {
    REPAIR_INCOMPLETE_TOTAL.load(Ordering::Relaxed)
}

/// `ledger_repair_sweeps_completed_total`, per-instance since process start.
pub(crate) fn sweeps_completed_total() -> u64 {
    REPAIR_SWEEPS_COMPLETED_TOTAL.load(Ordering::Relaxed)
}

/// The repair rotation's gauge set. Read by `/metrics` and by nothing else.
///
/// Field names carry the `ledger_repair_` prefix because the bundle is `#[serde(flatten)]`ed into
/// the `write_cache` object beside the fold's own flattened gauges. An unprefixed `rotation_complete`
/// or `incomplete_total` there would be ambiguous about which rotation it describes, and the two
/// have different windows.
///
/// Every field is either a count or a bool. There is deliberately **no byte figure** here: the
/// repair's effect on bytes is visible through the staged figure it restores, and publishing a
/// second byte figure beside it would invite a consumer to add the two.
/// `Eq` is deliberately absent: the three adaptive estimates below are `f64`, matching the
/// `*_duration_ms: f64` convention `metrics.rs` already uses for measured latencies. Publishing them
/// as scaled integers to keep `Eq` would put a unit in a field name that no other latency in the
/// tree carries.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LedgerRepairGauges {
    /// Whether § 16.6.6 precondition 3b currently holds on this volume.
    pub ledger_repair_rotation_complete: bool,
    /// Age of the stalest completed sweep, seconds. `null` when unreadable or when a shard has
    /// never been swept.
    pub ledger_repair_stalest_age_secs: Option<u64>,
    /// Shards observed in the metadata tree. `null` when the tree could not be listed.
    pub ledger_repair_observed_shards: Option<u64>,
    /// Shards with a completed sweep.
    pub ledger_repair_swept_shards: u64,
    /// **Non-zero is a finding.** Staged ranges found on disk with no ledger entry.
    pub ledger_repair_ranges_reappended_total: u64,
    /// Passes refused on an incomplete input.
    pub ledger_repair_incomplete_total: u64,
    /// Completed shard sweeps.
    pub ledger_repair_sweeps_completed_total: u64,
    /// **Whether the rotation CAN complete inside the window at this shard population.**
    ///
    /// `false` is a permanent condition and the field to alarm on: § 16.6.6 precondition 3b can then
    /// never pass, so the staged-authority epoch never flips and the fleet stays on the
    /// event-sourced counter indefinitely. Every other field reads identically whether the rotation
    /// is impossible or merely pending, which is why this one exists. `null` means the shard
    /// population could not be read. See [`rotation_reachable_for`] for what it does and does not
    /// claim — the figure is derived from a shard COUNT the scrape already holds, never from a fresh
    /// metadata walk, and the path-taking wrapper that used to do one was deleted for that reason.
    pub ledger_repair_rotation_reachable: Option<bool>,
    /// Projected seconds for one full rotation at the observed population and the budget in force.
    ///
    /// Compare against 86,400 (the window). Larger means unreachable, and by how much — the
    /// magnitude a boolean cannot carry. `null` when the population could not be read.
    pub ledger_repair_projected_rotation_secs: Option<u64>,
    /// Passes whose per-pass shard budget was clipped by the ceiling. Non-zero implies
    /// `ledger_repair_rotation_reachable == Some(false)`. **Per-instance.**
    pub ledger_repair_shard_budget_capped_total: u64,
    /// The most shards ONE instance's sweep can cover inside [`MAX_REPAIR_AGE_SECS`]:
    /// `REPAIR_SHARD_VISITS_PER_PASS_CEILING` times the passes in that window. A constant of the
    /// binary, published so `ledger_repair_observed_shards` has its comparand on the same scrape.
    /// `ledger_repair_rotation_reachable` is exactly `observed <= this`. Claims divide shards
    /// between instances, so the fleet's reach is larger by about the instance count.
    pub ledger_repair_shards_reachable_per_instance: u64,
    /// `ô` — `.meta` files per L2 directory, EWMA over completed shard visits. **Per-instance.**
    ///
    /// The quantity that decides which regime this volume's sweep is in. Below the crossover
    /// `o* = d̂/r̂` the sweep is directory-bound, above it file-bound; `o*` is 6.5 on EFS, 12.1 on
    /// FSx for OpenZFS and 31.3 on FSx for NetApp ONTAP, so no deployment may assume its regime and
    /// a deployment can cross it by growing. `null` until one visit has completed.
    ///
    /// **Not configurable, by design** (§ 16.6.4's no-knob reasoning): a knob would let an operator
    /// set a figure the volume contradicts.
    pub ledger_repair_metas_per_l2_dir: Option<f64>,
    /// `d̂` — mean L2 `readdir` in-flight duration, milliseconds, EWMA. **Per-instance.**
    ///
    /// An in-flight duration at the width the directory stage was running at, **not** a solo
    /// latency and not a throughput reciprocal. Read it beside `ledger_repair_meta_read_ms`: their
    /// ratio is this volume's own `o*`.
    pub ledger_repair_l2_readdir_ms: Option<f64>,
    /// `r̂` — mean `.meta` read-plus-parse in-flight duration, milliseconds, EWMA. **Per-instance.**
    pub ledger_repair_meta_read_ms: Option<f64>,
    /// `W_d` — outstanding L2 `readdir`s the next pass will keep in flight, of
    /// `W_d + W_f = 64`. **Per-instance.**
    ///
    /// The value the sweep's split branches on. `32` means no estimate exists yet (the neutral seed);
    /// a value near 64 means directory-bound and near 1 means file-bound. Clamped to `[1, 63]`, so
    /// neither stage can be starved by a transient estimate.
    pub ledger_repair_directory_concurrency: u64,
    /// What to change when the rotation is not completing, selected by which term is binding.
    /// `""` when neither applies, or when the population could not be read. Selected by
    /// `rotation_remedy_for` and set at assembly from the repair limiter's `k` and deferral count
    /// (`LedgerRepairGauges::with_rotation_remedy`), so it is never stale against them.
    #[serde(default)]
    pub ledger_repair_rotation_remedy: String,
}

/// The population remedy: the shard population is too large for the window.
const ROTATION_REMEDY_POPULATION: &str = "Repair rotation projected at {projected}s against a \
{window}s window; the staged-authority epoch cannot activate until one rotation completes. \
Remedies, in order of effect: (1) add proxy instances, since the shard population divides \
between them and this scales linearly; (2) raise MAX_REPAIR_AGE_SECS, which scales linearly and \
is a code change rather than a setting, so that one completed rotation means the same thing on \
every deployment; (3) accept the event-sourced counter permanently: it over-states, so eviction \
fires early rather than late, which is wasteful and not unsafe. Shortening \
LEDGER_REPAIR_INTERVAL_SECS is not a remedy: it multiplies the per-pass ledger read and leaves \
the total metadata work unchanged.";

/// The limiter remedy: scheduling headroom remains, and scanner concurrency is the binding term.
const ROTATION_REMEDY_LIMITER: &str = "Rotation progress is behind schedule while scheduling \
headroom remains: scanner concurrency is limited to k={k} and the limiter deferred {deferred} \
consolidation cycles. The limiter bypasses itself at 0.75 of the window; if this persists past \
that point, the measured aggregate curve (scanner_aggregate_estimate_by_level_repair) is the \
evidence to read.";

/// Select the rotation remedy text (cache-eviction-at-scale § 16.6.13 Problem 3).
///
/// - `reachable == Some(false)`: the population is binding, whatever the stalest age.
/// - `reachable == Some(true)` and the stalest sweep is more than twice the projected rotation
///   old: the rotation has headroom but is not keeping to it, so the limiter is binding. Twice
///   allows one full rotation of slack, and the floor of one pass interval keeps a tiny
///   population's projection from reading every pass gap as a delay.
/// - Otherwise `""`: nothing is binding, or the population or ages could not be read.
///
/// "Add proxy instances" appears only in the population text. When the limiter is binding,
/// adding instances adds scanners the limiter will defer, so naming it there would be wrong.
pub(crate) fn rotation_remedy_for(
    reachable: Option<bool>,
    projected_rotation_secs: Option<u64>,
    stalest_age_secs: Option<u64>,
    k: u32,
    deferred: u64,
) -> String {
    match (reachable, projected_rotation_secs, stalest_age_secs) {
        (Some(false), projected, _) => ROTATION_REMEDY_POPULATION
            .replace(
                "{projected}",
                &projected.map_or_else(|| "unknown".to_string(), |p| p.to_string()),
            )
            .replace("{window}", &MAX_REPAIR_AGE_SECS.to_string()),
        (Some(true), Some(projected), Some(stalest))
            if stalest > projected.max(LEDGER_REPAIR_INTERVAL_SECS).saturating_mul(2) =>
        {
            ROTATION_REMEDY_LIMITER
                .replace("{k}", &k.to_string())
                .replace("{deferred}", &deferred.to_string())
        }
        _ => String::new(),
    }
}

impl LedgerRepairGauges {
    /// Fill [`LedgerRepairGauges::ledger_repair_rotation_remedy`] from this value's own volume
    /// fields and the repair limiter's `k` and deferral count.
    pub(crate) fn with_rotation_remedy(mut self, k: u32, deferred: u64) -> Self {
        self.ledger_repair_rotation_remedy = rotation_remedy_for(
            self.ledger_repair_rotation_reachable,
            self.ledger_repair_projected_rotation_secs,
            self.ledger_repair_stalest_age_secs,
            k,
            deferred,
        );
        self
    }

    /// The eight **per-process** fields, with the six **volume-derived** ones at their sentinels.
    ///
    /// This is also the set to publish when the volume-side traversal did not complete at all —
    /// `MetricsCollector::collect_metrics` uses it for the `JoinError` arm of the `spawn_blocking`
    /// that owns the traversal.
    ///
    /// # Why this is not `Default::default()`, which is the error it exists to prevent
    ///
    /// The fourteen fields divide cleanly by where their value comes from, and a blanket default
    /// gets one half right and the other half wrong:
    ///
    /// - **Six come from the cache volume**: `rotation_complete`, `stalest_age_secs`,
    ///   `observed_shards`, `swept_shards`, `rotation_reachable`, `projected_rotation_secs`. For
    ///   these the defaults ARE the right answer, and deliberately so — the four `Option`s default
    ///   to `None`, which every consumer already reads as "this volume could not be read" and
    ///   publishes as `null` rather than as a figure, and `rotation_complete` defaults to `false`,
    ///   which is the conservative direction for an activation gate that must never claim coverage
    ///   it cannot show. `swept_shards` is the one field with no sentinel available: it is a bare
    ///   `u64`, so a failure publishes `0`, which is indistinguishable from "no shard has been
    ///   swept". **`observed_shards: null` is the discriminator for it** — the two come from the
    ///   same traversal, so a consumer that sees a null population must not read `swept_shards` as
    ///   a count. Widening it to `Option<u64>` would change a published wire shape and is not this
    ///   change.
    /// - **Eight do not touch the volume at all**: four process atomics
    ///   (`ranges_reappended_total`, `incomplete_total`, `sweeps_completed_total`,
    ///   `shard_budget_capped_total`) and four from the in-memory traversal estimator
    ///   (`metas_per_l2_dir`, `l2_readdir_ms`, `meta_read_ms`, `directory_concurrency`). These are
    ///   readable whatever the volume is doing, so zeroing them on a traversal failure would
    ///   publish four counters as `0` when they were not zero — and a `ranges_reappended_total` of
    ///   `0` is not a neutral reading, it is the "no finding" reading of the field whose whole
    ///   purpose is to surface staged ranges with no ledger entry. `directory_concurrency` is
    ///   clamped to `[1, 63]` and seeds at 32, so it is never legitimately `0` either.
    ///
    /// Reading this constructor's fields through the same helpers the success path uses is what
    /// keeps the two in step: [`collect_ledger_repair_gauges`] builds on it with `..`, so a field
    /// added to the per-process half cannot be wired in one path and forgotten in the other.
    pub(crate) fn process_local() -> Self {
        let (l2_readdir_ms, meta_read_ms, metas_per_l2_dir) = traversal_estimates();
        LedgerRepairGauges {
            ledger_repair_rotation_complete: false,
            ledger_repair_stalest_age_secs: None,
            ledger_repair_observed_shards: None,
            ledger_repair_swept_shards: 0,
            ledger_repair_rotation_reachable: None,
            ledger_repair_projected_rotation_secs: None,
            ledger_repair_ranges_reappended_total: ranges_reappended_total(),
            ledger_repair_incomplete_total: incomplete_total(),
            ledger_repair_sweeps_completed_total: sweeps_completed_total(),
            ledger_repair_shard_budget_capped_total: shard_budget_capped_total(),
            ledger_repair_shards_reachable_per_instance: shards_reachable_per_instance(),
            ledger_repair_metas_per_l2_dir: metas_per_l2_dir,
            ledger_repair_l2_readdir_ms: l2_readdir_ms,
            ledger_repair_meta_read_ms: meta_read_ms,
            ledger_repair_directory_concurrency: directory_concurrency_for_pass() as u64,
            // Derived at assembly by `with_rotation_remedy`, never collected.
            ledger_repair_rotation_remedy: String::new(),
        }
    }
}

/// Collect the repair rotation's gauges.
///
/// # ONE walk of the metadata tree and ONE read of the repair documents, per call
///
/// This function used to perform **five** metadata walks and **three** full reads of the repair
/// document set in a single invocation: it read the population and the documents at the top, then
/// called four path-taking helpers that each re-read one or both. Nothing memoised between them,
/// and at the measured ~1.4 ms per document on the fleet's EFS volume the two duplicate document
/// reads alone were ~2.4 s of a `/metrics` collection that has no memo and no deadline of its own.
///
/// The body of `/metrics` is one `SystemMetrics` serialised whole, so a collection that outruns the
/// scraper's deadline loses **every** series at once rather than degrading. That is the failure this
/// removes most of: 66 direct scrapes on 2026-09-24 all took 6.55-9.99 s against the 10 s deadline
/// every reader in this repo uses, and 16 of them returned nothing at all.
///
/// **No published value changes.** The four helpers' `_from`/`_for` inner forms compute exactly what
/// the path-taking forms computed, from a set that is now read once instead of three times — and
/// reading once is also strictly more consistent, since three traversals microseconds apart can
/// straddle a peer's document install and describe three different memberships in one response.
///
/// # What this does NOT fix
///
/// The remaining cost is still `O(shards)` with no caching and no bound, so the crossover returns at
/// a larger shard population. A bound, or an explicit degradation that publishes the expensive
/// sections as `null` past a budget, is the durable answer and is not this change.
pub(crate) fn collect_ledger_repair_gauges(cache_dir: &Path) -> LedgerRepairGauges {
    // The two traversals, once each. Everything below derives from these — nothing here may call a
    // path-taking helper, which is what the duplication was.
    let population = observed_shards_on_disk(cache_dir);
    let docs = read_repair_checkpoints(cache_dir);
    let swept_shards = docs
        .shards()
        .iter()
        .filter(|s| docs.get(s).is_some_and(|d| d.swept_at != 0))
        .count() as u64;
    LedgerRepairGauges {
        ledger_repair_rotation_complete: repair_rotation_complete_from(population.as_ref(), &docs),
        ledger_repair_stalest_age_secs: repair_stalest_age_secs_from(population.as_ref(), &docs),
        ledger_repair_observed_shards: population.as_ref().map(|p| p.len() as u64),
        ledger_repair_swept_shards: swept_shards,
        // `rotation_reachable` and `projected_rotation_secs` already had count-taking inner forms,
        // split out so their unreachable branches could be asserted at all. Calling those directly
        // is what removes two of the five walks; the `None` population still yields `None` for both,
        // exactly as the path-taking wrappers do via `?`.
        ledger_repair_rotation_reachable: population
            .as_ref()
            .map(|p| rotation_reachable_for(p.len())),
        ledger_repair_projected_rotation_secs: population
            .as_ref()
            .map(|p| projected_rotation_secs_for(p.len())),
        // The eight per-process fields, from the one constructor that defines them, so the success
        // path and the traversal-failure path cannot drift apart.
        ..LedgerRepairGauges::process_local()
    }
}

/// A repair claim, in its own namespace. Same write-then-re-read protocol and the same R19.2
/// property as the fold's claim: never load-bearing for correctness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RepairClaim {
    instance_id: String,
    claimed_at: u64,
    lease_secs: u64,
}

impl RepairClaim {
    fn path(cache_dir: &Path, shard: &Shard) -> PathBuf {
        repair_claims_dir(cache_dir)
            .join(&shard.bucket)
            .join(format!("{}.claim", shard.l1))
    }

    fn is_live_at(&self, now_secs: u64) -> bool {
        self.claimed_at.saturating_add(self.lease_secs) > now_secs
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimState {
    Claimed,
    Busy,
}

/// Claim a shard for a repair sweep. An I/O error is `Busy`, never `Claimed`.
async fn claim_repair_shard(cache_dir: &Path, shard: &Shard, instance_id: &str) -> ClaimState {
    let path = RepairClaim::path(cache_dir, shard);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Ok(content) = std::fs::read_to_string(&path) {
        if let Ok(existing) = serde_json::from_str::<RepairClaim>(&content) {
            if existing.instance_id != instance_id && existing.is_live_at(now) {
                return ClaimState::Busy;
            }
        }
    }
    let claim = RepairClaim {
        instance_id: instance_id.to_string(),
        claimed_at: now,
        lease_secs: REPAIR_CLAIM_LEASE_SECS,
    };
    let body = match serde_json::to_string(&claim) {
        Ok(b) => b,
        Err(e) => {
            debug!("Ledger repair: could not serialize a claim: {}", e);
            return ClaimState::Busy;
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
            debug!("Ledger repair: could not write claim for {}: {}", shard, e);
            return ClaimState::Busy;
        }
        Err(e) => {
            debug!(
                "Ledger repair: claim write task failed for {}: {}",
                shard, e
            );
            return ClaimState::Busy;
        }
    }
    // Re-read and require it to name self, so two racing instances produce one winner.
    match std::fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str::<RepairClaim>(&content) {
            Ok(seen) if seen.instance_id == instance_id => ClaimState::Claimed,
            _ => ClaimState::Busy,
        },
        Err(_) => ClaimState::Busy,
    }
}

/// Release this instance's repair claim on `shard` once its visit is installed.
///
/// Same rule as `staged_fold::release_claim`: unlink only a claim that still names this
/// instance, so a claim a peer took after ours lapsed is left alone. Any I/O error leaves the
/// claim to expire with its lease, which is what every claim did before this existed.
///
/// The lease only has to exclude a peer while the sweep runs. After the install the shard is the
/// freshest on the volume, so the stalest-first rotation already keeps peers away from it; the
/// lease only blocked a peer that ranked it first, for up to [`REPAIR_CLAIM_LEASE_SECS`].
///
/// The first-claim race in [`claim_repair_shard`] (both instances win when each re-reads before
/// the other's rename lands) is not closed here: a duplicated sweep's re-appends are counted
/// once since the change, so its cost is repeated work, not a wrong figure.
fn release_repair_claim(cache_dir: &Path, shard: &Shard, instance_id: &str) {
    let path = RepairClaim::path(cache_dir, shard);
    let ours = std::fs::read_to_string(&path)
        .ok()
        .and_then(|c| serde_json::from_str::<RepairClaim>(&c).ok())
        .is_some_and(|c| c.instance_id == instance_id);
    if !ours {
        return;
    }
    if let Err(e) = std::fs::remove_file(&path) {
        debug!(
            "Ledger repair: could not release the claim on {} ({}); it expires with its lease",
            shard, e
        );
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn age_secs(nanos: u64) -> u64 {
    now_nanos().saturating_sub(nanos) / 1_000_000_000
}

/// The repair window is never shorter than the fold's. The two share one cadence class
/// (§ 16.6.12 point 1); a repair window shorter than the fold's would demand that activation
/// evidence be fresher than the figure it gates.
const _: () = assert!(MAX_REPAIR_AGE_SECS >= MAX_AUTHORITY_AGE_SECS);

#[cfg(test)]
mod tests {
    use super::*;

    /// The two remedy selectors (§ 16.6.13 Problem 3, U6 red side (b)): the limiter text
    /// applies only when the rotation is reachable and behind schedule, and it never tells the
    /// operator to add instances; the population text applies whenever the rotation is
    /// unreachable, and it does.
    #[test]
    fn rotation_remedy_names_the_binding_term() {
        // Reachable, stalest sweep far past twice the projection: the limiter is binding.
        let limiter = rotation_remedy_for(Some(true), Some(3_600), Some(20_000), 2, 17);
        assert!(
            limiter.starts_with("Rotation progress is behind schedule"),
            "limiter case must select the limiter text, got {limiter:?}"
        );
        assert!(
            limiter.contains("k=2") && limiter.contains("deferred 17"),
            "limiter text must carry k and the deferral count, got {limiter:?}"
        );
        assert!(
            !limiter.contains("add proxy instances"),
            "limiter text must not name adding instances, got {limiter:?}"
        );

        // Unreachable: the population is binding, whatever the stalest age.
        for stalest in [None, Some(10), Some(200_000)] {
            let population = rotation_remedy_for(Some(false), Some(172_800), stalest, 2, 17);
            assert!(
                population.contains("add proxy instances")
                    && population.contains("projected at 172800s")
                    && population.contains("86400s window"),
                "population case must select the population text, got {population:?}"
            );
        }

        // Reachable and on schedule, or unreadable: no remedy.
        assert_eq!(
            rotation_remedy_for(Some(true), Some(3_600), Some(7_000), 2, 17),
            ""
        );
        assert_eq!(
            rotation_remedy_for(Some(true), Some(3_600), None, 2, 17),
            ""
        );
        assert_eq!(rotation_remedy_for(None, None, Some(200_000), 2, 17), "");
        // A tiny population's projection is floored at one pass interval, so a normal pass gap
        // is not read as a delay.
        assert_eq!(
            rotation_remedy_for(
                Some(true),
                Some(10),
                Some(LEDGER_REPAIR_INTERVAL_SECS),
                1,
                0
            ),
            ""
        );

        // The assembly helper reads the gauge's own volume fields.
        let g = LedgerRepairGauges {
            ledger_repair_rotation_reachable: Some(true),
            ledger_repair_projected_rotation_secs: Some(3_600),
            ledger_repair_stalest_age_secs: Some(20_000),
            ..LedgerRepairGauges::process_local()
        }
        .with_rotation_remedy(3, 5);
        assert!(g.ledger_repair_rotation_remedy.contains("k=3"));
    }

    /// The per-pass duty-cycle budget model of § 16.6.4, restated so the derivations in
    /// [`REPAIR_METAS_PER_VISIT_CAP`] and [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`] are executable
    /// rather than prose.
    ///
    /// These are design-time inputs, not runtime configuration: the shipped constants are the
    /// *results* of the model, and the assertions below pin that they still satisfy it.
    ///
    /// # All three cost inputs are now MEASURED, and one of them moved unfavourably
    ///
    /// § 16.6.4's amendment of 2026-09-06 replaced the estimates with measurements taken on
    /// `m6in.2xlarge` fleet proxies against the 1,146,790-object reference tree
    /// (measured):
    ///
    /// | Input | Was | Now | Direction |
    /// |---|---|---|---|
    /// | `R` | 212 obj/s, a **concurrency artefact** of a scan running one outstanding read | 2,900 obj/s at the measured knee of 64 | favourable, 13.7x |
    /// | `c_r` | 10 ms estimated | 18.024 ms measured (**EFS**) | **unfavourable, 1.8x**. Per-CONFIGURATION: 3.774 ms on FSxZ is *favourable*; FSxN measures both 14.345 and 7.131 ms |
    /// | `d` | 2 ms estimated | 2.234 ms measured | the estimate was good to 10% |
    ///
    /// `R`'s residual optimism is that the measurement fleet was idle; a serving fleet contends for
    /// the same per-client NFS pipeline. Reaching it also requires the sweep to keep
    /// [`SWEEP_READ_CONCURRENCY`] reads outstanding, which is a property of the implementation
    /// rather than of the volume — so these bounds are conditional on that and not merely on the
    /// hardware.
    mod budget_model {
        /// Duty cycle: the share of wall-clock time a pass may spend on shared storage.
        pub const D: f64 = 0.167;
        /// Pass interval, seconds.
        pub const T: f64 = 300.0;
        /// Rotation window, seconds.
        pub const W: f64 = 86_400.0;
        /// Objects read and parsed per second — the MEASURED knee rate.
        pub const R: f64 = 2_900.0;
        /// The superseded rate, retained so the red sides below can show what it predicted.
        ///
        /// **Do not use it in a forward derivation.** It was measured at one outstanding read, and
        /// it is end-to-end rather than read-plus-parse, so substituting it into this model both
        /// under-states the rate by 13.7x and double-counts readdir against the `DIR` term below.
        pub const R_ARTEFACT: f64 = 212.0;
        /// Per-visit fixed cost, seconds: six shared-storage round trips, two of them `fsync`.
        /// Measured over 50 iterations of the real claim-protocol shape.
        pub const C_R: f64 = 0.018024;
        /// One L2 `readdir`, seconds. Mean of five runs' means.
        pub const DIR: f64 = 0.002234;
        /// L2 directories in a fully populated shard.
        pub const L2_MAX: f64 = 4096.0;

        /// Shared-storage seconds one pass may spend.
        pub(crate) fn pass_budget_secs() -> f64 {
            D * T
        }
    }

    /// The shipped per-visit cap FITS the per-pass duty-cycle budget, and is the largest power of
    /// two that does.
    ///
    /// # Why this is asserted rather than left as a comment
    ///
    /// `REPAIR_METAS_PER_VISIT_CAP` has been wrong in **both** directions, and each time the error
    /// was correct arithmetic on an input nobody had checked:
    ///
    /// - It was **20,000**, justified as "bounded work" — which it is, against nothing. "Bounded"
    ///   and "affordable" are different claims and only the second one matters here.
    /// - It was then cut to **8,192** on this model evaluated at `R = 212 obj/s`, where 20,000
    ///   costs 94.3 s against a 50.1 s pass budget. `R = 212` has since been measured to be a
    ///   concurrency artefact, and at the measured 2,900 obj/s the same 20,000 costs **6.9 s**. So
    ///   the cut was unnecessary in the first place, and the cap re-derives to 118,701 → **65,536**.
    ///
    /// The lesson, and the reason the arithmetic lives in a test rather than only in a doc comment:
    /// a bound is only as good as the provenance of its inputs, and a bound written as prose is
    /// never re-evaluated when an input is corrected.
    ///
    /// A failure implicates [`REPAIR_METAS_PER_VISIT_CAP`] or the model in [`budget_model`].
    #[test]
    fn the_per_visit_cap_fits_the_pass_budget() {
        use budget_model::*;
        let worst_visit = |cap: f64, rate: f64| C_R + DIR * L2_MAX + cap / rate;
        let budget = pass_budget_secs();

        let shipped = worst_visit(REPAIR_METAS_PER_VISIT_CAP as f64, R);
        assert!(
            shipped <= budget,
            "a worst-case visit at the shipped cap of {} costs {:.1} s against a {:.1} s pass \
             budget. A cap is only meaningful against the budget that bounds it.",
            REPAIR_METAS_PER_VISIT_CAP,
            shipped,
            budget
        );

        // The cap is the largest power of two that fits, so it is derived rather than merely small
        // enough. Doubling it must not fit. **This is the red side**, and it is the only assertion
        // here that can fail on a cap that is too LARGE.
        let doubled = worst_visit((REPAIR_METAS_PER_VISIT_CAP * 2) as f64, R);
        assert!(
            doubled > budget,
            "the cap should be the largest power of two inside the budget; {} also fits ({:.1} s \
             vs {:.1} s), so the constant is smaller than the derivation allows",
            REPAIR_METAS_PER_VISIT_CAP * 2,
            doubled,
            budget
        );

        // ---- The two retired values, and what each one now costs.
        //
        // The previous version of this test asserted that 20,000 must FAIL the inequality, as the
        // in-line red side for the cut to 8,192. At the measured rate that assertion is false and
        // the finding is its inverse: 20,000 fits comfortably, so the reduction was an artefact.
        // Asserting the inverse keeps the history executable instead of deleting it.
        let retired_20k = worst_visit(20_000.0, R);
        assert!(
            retired_20k <= budget,
            "at the measured rate the retired cap of 20,000 must now FIT ({retired_20k:.1} s of \
             {budget:.1} s) — that is the finding: the cut to 8,192 was correct arithmetic on an \
             input that was 13.7x low. If this fails, the rate moved back down and the whole \
             amendment needs re-reading."
        );
        let retired_20k_at_artefact = worst_visit(20_000.0, R_ARTEFACT);
        assert!(
            retired_20k_at_artefact > budget,
            "and at the artefact rate it must still be shown to breach ({:.1} s), because that is \
             what made the reduction look necessary and the record of it is the point",
            retired_20k_at_artefact
        );
        let retired_8k = worst_visit(8_192.0, R);
        assert!(
            retired_8k < budget / 2.0,
            "the retired cap of 8,192 must be shown to be far INSIDE the budget ({retired_8k:.1} s \
             of {budget:.1} s), i.e. under-derived rather than merely conservative — a visit cap \
             8x smaller than affordable makes the rotation 8x slower for nothing"
        );
    }

    /// The object-count bound published in [`REPAIR_SHARD_VISITS_PER_PASS_CEILING`]'s table is what
    /// the model actually gives.
    ///
    /// # Why the table is asserted
    ///
    /// The ceiling was never the real limit; total `.meta` work against the window is. That bound —
    /// `O ≤ D·I·W·R` — is the number an operator has to plan against, and it lives in a doc comment
    /// where nothing checks it. A table of figures nobody recomputes drifts from its own model
    /// silently, and this one carries the headline claim about whether the O(100M)-range design
    /// point is reachable at all.
    ///
    /// # The headline claim REVERSED, and that is the reason to keep asserting it
    ///
    /// The previous version of this test asserted that the design point is **unreachable** at
    /// `I = 1` (3.06 M objects, a factor of 3.3 short) and first clears at `I = 4`. Every one of
    /// those assertions was correct arithmetic on `R = 212 obj/s`, and `R = 212` was a concurrency
    /// artefact. At the measured 2,900 obj/s the dense bound is 41.8 M · I, so it clears at
    /// `I = 1` with 4.2x of margin and the "activation needs a fleet rather than a node" claim is
    /// withdrawn.
    ///
    /// The sparse column is now the interesting one, and it is where the binding term changed: with
    /// `d = 2.234 ms` against `1/R = 0.345 ms`, readdir dominates parse by 6.5x, so the sparse bound
    /// is 5.60 M · I and clears the design point at `I = 2` rather than `I = 1`. Raising `R` further
    /// buys almost nothing there, which is why the sweep applies its concurrency to the `readdir`s
    /// as well as to the reads.
    ///
    /// A failure implicates the table, not the code: no production path reads these figures.
    #[test]
    fn the_published_object_bound_matches_the_model() {
        use budget_model::*;
        // Parse term only, and with a maximally sparse tree that adds one `readdir` per object.
        let bound = |i: f64| D * i * W * R;
        let bound_sparse = |i: f64| D * i * W / (DIR + 1.0 / R);

        // Tolerance is half the last digit the table prints: 0.05 M on a figure given to one
        // decimal place. A real assertion that the printed cell is the correct rounding of this
        // arithmetic, not a loose one.
        for (instances, dense_m, sparse_m) in
            [(1.0, 41.8, 5.60), (3.0, 125.5, 16.8), (8.0, 334.7, 44.8)]
        {
            let got = bound(instances) / 1e6;
            assert!(
                (got - dense_m).abs() < 0.05,
                "I={}: the table says {:.1} M objects, the model gives {:.2} M",
                instances,
                dense_m,
                got
            );
            let got_sparse = bound_sparse(instances) / 1e6;
            assert!(
                (got_sparse - sparse_m).abs() < 0.05,
                "I={}: the table's sparse-tree column says {:.2} M objects, the model gives {:.2} M",
                instances,
                sparse_m,
                got_sparse
            );
        }

        // The headline claims, each asserted against the design point of order 10 M objects.
        const DESIGN_POINT_OBJECTS: f64 = 10e6;
        assert!(
            bound(1.0) > DESIGN_POINT_OBJECTS,
            "the doc claims the design point IS reachable at I=1 on a dense tree; the model gives \
             {:.2} M against {:.0} M",
            bound(1.0) / 1e6,
            DESIGN_POINT_OBJECTS / 1e6
        );
        assert!(
            bound(1.0) / DESIGN_POINT_OBJECTS > 4.0,
            "and with the 4.2x margin the doc states, not marginally: {:.1}x",
            bound(1.0) / DESIGN_POINT_OBJECTS
        );
        // The sparse worst case is the one that still needs a second instance, and asserting both
        // sides of it is what stops the dense figure being read as the whole answer.
        assert!(
            bound_sparse(1.0) < DESIGN_POINT_OBJECTS,
            "the doc claims the maximally sparse worst case does NOT clear the design point at \
             I=1: the model gives {:.2} M against {:.0} M",
            bound_sparse(1.0) / 1e6,
            DESIGN_POINT_OBJECTS / 1e6
        );
        assert!(
            bound_sparse(2.0) > DESIGN_POINT_OBJECTS,
            "and that it clears at I=2: the model gives {:.2} M",
            bound_sparse(2.0) / 1e6
        );

        // ---- The withdrawn claim, kept executable so it cannot be re-asserted by accident.
        //
        // At the artefact rate the design point really was unreachable until I=4. Keeping this
        // arithmetic here is what makes the withdrawal a statement about the INPUT rather than a
        // change of mind about the model — the model is the same model in both branches.
        let bound_at_artefact = |i: f64| D * i * W * R_ARTEFACT;
        assert!(
            bound_at_artefact(1.0) < DESIGN_POINT_OBJECTS
                && bound_at_artefact(3.0) < DESIGN_POINT_OBJECTS
                && bound_at_artefact(4.0) > DESIGN_POINT_OBJECTS,
            "the retired claim — unreachable at I=1 ({:.2} M), still short at I=3 ({:.2} M), first \
             clearing at I=4 ({:.2} M) — must still follow from the artefact rate. It was correct \
             arithmetic on a wrong input, and the model is unchanged.",
            bound_at_artefact(1.0) / 1e6,
            bound_at_artefact(3.0) / 1e6,
            bound_at_artefact(4.0) / 1e6
        );

        // And the readdir term really is the one that binds in the sparse case, which is the claim
        // that justifies applying `SWEEP_READ_CONCURRENCY` to the L2 listings and not only to the
        // `.meta` reads.
        // Bound to a local rather than asserted inline: both operands are `const`, so the
        // expression folds and clippy's `assertions_on_constants` rejects it as an assertion with a
        // constant value. The binding keeps the check runtime-evaluated and therefore reportable.
        let readdir_over_parse = DIR / (1.0 / R);
        assert!(
            readdir_over_parse > 6.0,
            "the doc claims readdir dominates parse by 6.5x in the sparse case; the ratio is \
             {readdir_over_parse:.1}x"
        );
    }

    /// The per-pass shard ceiling is derived from the cheapest possible visit, and is large enough
    /// that a realistic bucket count is never clamped.
    ///
    /// # The defect this pins
    ///
    /// The ceiling was 8, justified only as "mirroring the fold's ceiling discipline". With
    /// `passes_per_window` = 288 that bounds the sweepable population at `8 × 288` = 2,304 shards =
    /// **9 buckets**. Above that the derived budget was clamped, the rotation could never complete,
    /// and § 16.6.6 precondition 3b — which requires a COMPLETE rotation — could never pass. The
    /// epoch would never flip and the fleet would stay on the lossy incumbent counter indefinitely.
    ///
    /// # The fixture is chosen to REACH the guard, which the realistic case does not
    ///
    /// A single-bucket cache needs `ceil(256/288)` = 1 visit per pass and is nowhere near any
    /// ceiling, so a fixture built from the realistic case passes at a ceiling of 8 and proves
    /// nothing. The population has to exceed `ceiling × passes_per_window` for the clamp to be
    /// reachable at all — so this asserts across bucket counts that straddle the OLD limit, and the
    /// 10-bucket row is the one that fails at 8.
    #[test]
    fn the_shard_ceiling_does_not_clamp_a_realistic_bucket_count() {
        use budget_model::*;
        let p = passes_per_window();
        assert_eq!(p, 288, "the derivations assume W/T = 288 passes per window");

        // Bucket counts straddling the retired 9-bucket limit. Every one must be sweepable inside
        // one window, which is what precondition 3b needs.
        for buckets in [1usize, 9, 10, 16, 50, 100, 256, 1_024] {
            let shards = buckets * 256;
            let per_pass = shards
                .div_ceil(p)
                .clamp(1, REPAIR_SHARD_VISITS_PER_PASS_CEILING);
            assert!(
                per_pass * p >= shards,
                "{} buckets ({} shards) needs {} visits/pass but the budget gives {}, so the \
                 rotation cannot complete in one window and precondition 3b can never pass",
                buckets,
                shards,
                shards.div_ceil(p),
                per_pass
            );
            assert!(
                rotation_reachable_for(shards),
                "{} buckets ({} shards) must be reported as a reachable rotation",
                buckets,
                shards
            );
        }

        // The ceiling equals the largest power of two inside the cheapest-visit bound.
        let cheapest_visit = C_R + DIR + 1.0 / R;
        let derived = (pass_budget_secs() / cheapest_visit) as usize;
        assert!(
            REPAIR_SHARD_VISITS_PER_PASS_CEILING <= derived,
            "the ceiling {} exceeds what the pass budget affords even for the cheapest possible \
             visit ({} visits at {:.2} ms each)",
            REPAIR_SHARD_VISITS_PER_PASS_CEILING,
            derived,
            cheapest_visit * 1000.0
        );
        assert!(
            REPAIR_SHARD_VISITS_PER_PASS_CEILING * 2 > derived,
            "the ceiling should be the largest power of two inside the bound; {} also fits within \
             {}, so it is smaller than the derivation allows",
            REPAIR_SHARD_VISITS_PER_PASS_CEILING * 2,
            derived
        );

        // ---- The MARGIN, which is the part of this constant the re-measurement actually moved.
        //
        // 2,048 survives — it is the one constant in this module the corrected rate left alone —
        // but its headroom did not. The bound was 2,996 visits on the estimated inputs and is 2,431
        // on the measured ones, because `c_r` is 1.8x more expensive than estimated and it dominates
        // a cheapest-possible visit. So the ceiling now trips ~16% before the arithmetic requires
        // rather than ~32%.
        //
        // Asserted because "survives" is the kind of verdict that stops being re-examined. The next
        // unfavourable move in `c_r` is the one that puts 2,048 above its own bound, and this is the
        // assertion that says so rather than a doc comment nobody recomputes.
        let margin = 1.0 - (REPAIR_SHARD_VISITS_PER_PASS_CEILING as f64 / derived as f64);
        assert!(
            (0.10..0.25).contains(&margin),
            "the ceiling's margin against its own bound is {:.1}%, outside the 10-25% band the \
             doc comment records as ~16%. Below 10% the constant is about to breach; above 25% the \
             inputs improved and it is now under-derived.",
            margin * 100.0
        );
        // The retired margin, at the estimated inputs, so the tightening is executable rather than
        // prose. This is the red side of the assertion above: it shows the band it pins is a
        // consequence of the measured inputs and not a band any inputs would satisfy.
        let derived_at_estimates =
            (pass_budget_secs() / (0.010 + 0.002 + 1.0 / R_ARTEFACT)) as usize;
        let retired_margin =
            1.0 - (REPAIR_SHARD_VISITS_PER_PASS_CEILING as f64 / derived_at_estimates as f64);
        assert!(
            retired_margin > margin + 0.10,
            "the estimated inputs gave a materially looser margin ({:.1}% against {:.1}%), which is \
             the tightening this constant's doc comment records. If the two are now close, one of \
             the two input sets was changed without the other.",
            retired_margin * 100.0,
            margin * 100.0
        );
    }

    /// A population the ceiling cannot sweep is reported as **unreachable**, distinctly from one that
    /// is merely pending — and the boolean and the projected figure never disagree.
    ///
    /// # The failure mode this closes
    ///
    /// A rotation that can never complete and one that has not completed yet read identically on
    /// every other gauge: `rotation_complete` is `false` and `stalest_age_secs` is `null` or rising in
    /// both. An operator cannot tell "wait" from "wait forever", and the difference is a year of
    /// running on the lossy counter while believing the flip is imminent.
    ///
    /// Asserted over the arithmetic rather than a directory tree: the unreachable branch begins at
    /// `2,048 × 288` = 589,824 shards, and a fixture creating that many directories is a test nobody
    /// runs — so the branch would go unverified, which for a permanent-failure alarm is the branch
    /// that matters.
    ///
    /// The last assertion is the one that keeps the two fields honest: they are computed separately,
    /// so a change to either could make the boolean say "fine" while the figure says nine days.
    #[test]
    fn an_unsweepable_population_is_unreachable_and_not_merely_pending() {
        let p = passes_per_window();
        let last_reachable = REPAIR_SHARD_VISITS_PER_PASS_CEILING * p;

        // Two-sided, one shard apart, so the boundary is asserted and not merely a large-vs-small
        // comparison that any monotone function would satisfy.
        assert!(
            rotation_reachable_for(last_reachable),
            "exactly {} shards is the largest sweepable population and must be reachable",
            last_reachable
        );
        assert!(
            !rotation_reachable_for(last_reachable + 1),
            "one shard beyond {} cannot be swept inside the window and MUST be reported \
             unreachable — otherwise it is indistinguishable from a rotation that is merely pending",
            last_reachable
        );

        // The projection carries the magnitude a boolean cannot.
        assert!(
            projected_rotation_secs_for(last_reachable) <= MAX_REPAIR_AGE_SECS,
            "a reachable population must project to within the window"
        );
        assert!(
            projected_rotation_secs_for(last_reachable + 1) > MAX_REPAIR_AGE_SECS,
            "an unreachable population must project BEYOND the window, which is how an operator \
             sees how far off they are rather than only that they are"
        );

        // Empty and small volumes are reachable, not accidentally unreachable.
        assert!(rotation_reachable_for(0));
        assert_eq!(projected_rotation_secs_for(0), 0);

        // The boolean and the figure are computed independently and must never disagree. Sampled
        // across the whole range including both sides of the boundary.
        for observed in (0..3_000).chain([
            last_reachable - 1,
            last_reachable,
            last_reachable + 1,
            600_000,
        ]) {
            assert_eq!(
                rotation_reachable_for(observed),
                projected_rotation_secs_for(observed) <= MAX_REPAIR_AGE_SECS,
                "at {} shards the reachability boolean and the projected rotation disagree; a \
                 consumer reading one would be told the opposite of the other",
                observed
            );
        }
    }

    /// A clamped pass is COUNTED, so the permanent condition is visible in a counter as well as in
    /// the boolean.
    ///
    /// The counter is process-global, so this asserts a DELTA against a baseline read here rather
    /// than an absolute — an absolute would race every other test in this binary.
    #[test]
    fn a_clamped_pass_is_counted() {
        let before = shard_budget_capped_total();
        // Below the ceiling: no clamp, no count.
        let _ = shards_per_pass(256);
        assert_eq!(
            shard_budget_capped_total(),
            before,
            "a population inside the budget must not count a clamp"
        );
        // Beyond it: clamped, counted, and the returned budget is the ceiling.
        let unsweepable = REPAIR_SHARD_VISITS_PER_PASS_CEILING * passes_per_window() + 1;
        assert_eq!(
            shards_per_pass(unsweepable),
            REPAIR_SHARD_VISITS_PER_PASS_CEILING
        );
        assert!(
            shard_budget_capped_total() > before,
            "a clamped pass must increment ledger_repair_shard_budget_capped_total, or the \
             permanent condition is visible only in a boolean that nothing counts"
        );
    }

    /// Reading the gauges must not move the clamp counter.
    ///
    /// `/metrics` is scraped continuously, so a gauge read with a side effect on a counter would
    /// make that counter a function of scrape rate rather than of passes. `projected_rotation_secs`
    /// deliberately re-derives the budget instead of calling `shards_per_pass`, and this is the
    /// executable statement of that.
    ///
    /// # The assertion has to run at the CLAMPED population, and a filesystem fixture cannot
    ///
    /// The first version of this read the gauges over a real one-shard temp directory. It passed with
    /// `projected_rotation_secs_for` rewired to call `shards_per_pass` — because the counting side
    /// effect only fires when the budget is **clamped**, and one shard is nowhere near the ceiling.
    /// The guard was unreachable from the fixture, so the test proved only that a non-clamping path
    /// does not count. Reaching the clamp through the filesystem would need 589,825 shard
    /// directories.
    ///
    /// So the side-effect assertion runs over the arithmetic at an unsweepable count, where the
    /// side effect would actually fire, and the filesystem read is kept as a second arm that covers
    /// the whole `collect_ledger_repair_gauges` path.
    #[test]
    fn reading_the_gauges_does_not_move_the_clamp_counter() {
        // Arm 1, the discriminator: an unsweepable population, where a call into `shards_per_pass`
        // WOULD count a clamp.
        let unsweepable = REPAIR_SHARD_VISITS_PER_PASS_CEILING * passes_per_window() + 1;
        assert!(
            !rotation_reachable_for(unsweepable),
            "fixture precondition: the population must be clamped, or the counting side effect \
             cannot fire and this test cannot discriminate"
        );
        let before = shard_budget_capped_total();
        for _ in 0..5 {
            let _ = projected_rotation_secs_for(unsweepable);
            let _ = rotation_reachable_for(unsweepable);
        }
        assert_eq!(
            shard_budget_capped_total(),
            before,
            "reading the projection at a CLAMPED population must not count a clamp; otherwise the \
             counter measures /metrics scrape rate rather than passes"
        );

        // Arm 2: the whole gauge-collection path over a real volume.
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        std::fs::create_dir_all(dir.join("metadata").join("bucket").join("ab")).unwrap();
        let before = shard_budget_capped_total();
        for _ in 0..5 {
            let _ = collect_ledger_repair_gauges(dir);
        }
        assert_eq!(
            shard_budget_capped_total(),
            before,
            "collecting gauges must be side-effect free"
        );
    }

    /// The gauges report reachability and a projection on a real volume, so the fields are wired
    /// rather than merely defined.
    #[test]
    fn the_gauges_publish_reachability_and_a_projection() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // One bucket, two shards.
        std::fs::create_dir_all(dir.join("metadata").join("bucket").join("ab")).unwrap();
        std::fs::create_dir_all(dir.join("metadata").join("bucket").join("cd")).unwrap();

        let g = collect_ledger_repair_gauges(dir);
        assert_eq!(g.ledger_repair_observed_shards, Some(2));
        assert_eq!(
            g.ledger_repair_rotation_reachable,
            Some(true),
            "a two-shard volume is trivially sweepable and must report reachable"
        );
        assert_eq!(
            g.ledger_repair_projected_rotation_secs,
            Some(2 * LEDGER_REPAIR_INTERVAL_SECS),
            "two shards at one visit per pass project to two intervals"
        );

        // An unreadable tree gives `null` for both, never a reassuring answer.
        let broken = TempDir::new().unwrap();
        std::fs::write(broken.path().join("metadata"), b"not a directory").unwrap();
        let g = collect_ledger_repair_gauges(broken.path());
        assert_eq!(
            (
                g.ledger_repair_rotation_reachable,
                g.ledger_repair_projected_rotation_secs
            ),
            (None, None),
            "an unreadable population must report null for both, not `reachable: true`"
        );
    }

    /// One `/metrics` collection walks `metadata/` **once** and reads the repair documents **once**.
    ///
    /// # Why this needs a call counter rather than an assertion on a figure
    ///
    /// One traversal and five produce **identical** gauge values — that is exactly why the
    /// duplication survived from 2026-09-06 unnoticed, and it is why no published field can be the
    /// discriminator. The only observable difference is how many times the two readers ran, so that
    /// is what is asserted, through [`traversals_for_tests`].
    ///
    /// # What this pins
    ///
    /// `read_repair_checkpoints` is an open + read + JSON parse per shard document, measured at
    /// ~1.4 ms per document on the fleet's EFS volume with a hot re-read no faster. Three of them
    /// per scrape was ~2.4 s of a collection whose body is one `SystemMetrics` serialised whole, so
    /// exceeding the scraper's deadline loses **every** series rather than degrading. A regression
    /// that gives any figure back its own path-taking helper is invisible in every published value
    /// and shows up only here.
    ///
    /// # RED SIDE
    ///
    /// With `ledger_repair_rotation_complete` reverted to `repair_rotation_complete(cache_dir)` —
    /// one of the four original call sites — this reports 2 document reads and 2 walks against an
    /// expected 1 and 1.
    #[tokio::test]
    async fn one_gauge_collection_reads_the_repair_tree_once() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // Real input on BOTH sides: a shard in the metadata tree AND a completed repair document
        // for it. Seeding only the metadata tree would leave `stalest_age_secs` at its `None`
        // sentinel, and the non-vacuity assertions below could then pass on a defaulted set.
        seed_staged_meta(dir, "bucket/a.bin", &[(0, 1023, 512)]).await;
        for _ in 0..4 {
            if repair_rotation_complete(dir) {
                break;
            }
            run_ledger_repair(dir, &ledger, "host:1").await;
        }
        assert!(
            repair_rotation_complete(dir),
            "fixture precondition: the shard must have a completed sweep, or the figures below \
             are sentinels and this test cannot tell de-duplication from breakage"
        );

        // Baseline AFTER the fixture, which reads both trees many times as a side effect, so this
        // measures the collection and nothing before it.
        let docs_before = traversals_for_tests("repair_docs", dir);
        let walks_before = traversals_for_tests("shard_walk", dir);

        let g = collect_ledger_repair_gauges(dir);

        let docs = traversals_for_tests("repair_docs", dir) - docs_before;
        let walks = traversals_for_tests("shard_walk", dir) - walks_before;
        assert_eq!(
            (docs, walks),
            (1, 1),
            "one gauge collection must read the repair documents ONCE and walk metadata/ ONCE. \
             Got {docs} document read(s) and {walks} walk(s), which means a figure has gone back \
             to a path-taking helper that re-reads the tree — invisible in every published value \
             and, at ~1.4 ms per document, seconds of a scrape that loses every series when it \
             outruns the reader's deadline."
        );

        // A read count of 1 is also what a collector returning its defaults would show, so the
        // count alone cannot distinguish de-duplication from breakage. Each of the four figures
        // that changed derivation is asserted to be real.
        assert_eq!(
            g.ledger_repair_observed_shards,
            Some(1),
            "the population must come from the SHARED walk, not be defaulted"
        );
        assert!(
            g.ledger_repair_rotation_complete,
            "rotation_complete must be derived from the shared set: the fixture has a completed \
             sweep covering the whole population"
        );
        assert!(
            g.ledger_repair_stalest_age_secs.is_some(),
            "the stalest age must be derived from the shared set, not left at its null sentinel"
        );
        assert_eq!(
            (
                g.ledger_repair_rotation_reachable,
                g.ledger_repair_projected_rotation_secs
            ),
            (Some(true), Some(LEDGER_REPAIR_INTERVAL_SECS)),
            "reachability and the projection must be derived from the shared population"
        );
        assert_eq!(
            g.ledger_repair_swept_shards, 1,
            "and the swept count from the shared document set"
        );
    }

    /// The de-duplicated collector publishes **exactly** what the five-traversal one published.
    ///
    /// The de-duplication is a pure cost change, so the thing worth asserting is that it is one.
    /// This re-derives the six volume figures the way the old body did — through the four
    /// **path-taking** helpers, each re-reading the tree — and requires agreement across five
    /// fixture states, including both UNREADABLE arms. Reading the diff cannot establish this; a
    /// `?` moved from a wrapper into an inner form is exactly the kind of change that preserves
    /// every value on a populated volume and inverts one on an unreadable one.
    ///
    /// The **per-process** fields are deliberately not compared: four are global atomics another
    /// test in the same binary may bump between the two calls, so comparing them would be a flake
    /// rather than an assertion. They are covered by
    /// `a_failed_traversal_publishes_sentinels_and_keeps_the_per_process_figures`.
    #[tokio::test]
    async fn the_deduplicated_collector_agrees_with_the_five_traversal_derivation() {
        /// The six volume-derived figures, as the pre-de-duplication body derived them: four
        /// path-taking helpers plus the population and the document set read again.
        fn old_shape(
            dir: &Path,
        ) -> (
            bool,
            Option<u64>,
            Option<u64>,
            u64,
            Option<bool>,
            Option<u64>,
        ) {
            let population = observed_shards_on_disk(dir);
            let docs = read_repair_checkpoints(dir);
            let swept = docs
                .shards()
                .iter()
                .filter(|s| docs.get(s).is_some_and(|d| d.swept_at != 0))
                .count() as u64;
            (
                repair_rotation_complete(dir),
                repair_stalest_age_secs(dir),
                population.as_ref().map(|p| p.len() as u64),
                swept,
                // The path-taking `rotation_reachable`/`projected_rotation_secs` wrappers these two
                // arms used were deleted on 2026-09-28 (zero callers, and a metadata walk on the
                // scrape path). Their bodies are inlined here over `old_shape`'s OWN separately-read
                // population, which is what keeps this arm a second reading rather than a second
                // call. Read these two tuple slots as WEAKER than the other four from this point on:
                // they now share the `_for` arithmetic with `new_shape`, so they can only catch the
                // collector reading a different population, not different arithmetic.
                population.as_ref().map(|p| rotation_reachable_for(p.len())),
                population
                    .as_ref()
                    .map(|p| projected_rotation_secs_for(p.len())),
            )
        }
        fn new_shape(
            dir: &Path,
        ) -> (
            bool,
            Option<u64>,
            Option<u64>,
            u64,
            Option<bool>,
            Option<u64>,
        ) {
            let g = collect_ledger_repair_gauges(dir);
            (
                g.ledger_repair_rotation_complete,
                g.ledger_repair_stalest_age_secs,
                g.ledger_repair_observed_shards,
                g.ledger_repair_swept_shards,
                g.ledger_repair_rotation_reachable,
                g.ledger_repair_projected_rotation_secs,
            )
        }

        // State 1: nothing at all — `metadata/` absent is an EMPTY population, not an unreadable
        // one, and the two have opposite meanings for the rotation predicate.
        let empty = TempDir::new().unwrap();
        assert_eq!(
            new_shape(empty.path()),
            old_shape(empty.path()),
            "an empty volume must read identically"
        );

        // State 2: shards present, no repair document — the rotation is incomplete and the age is
        // the null sentinel.
        let unswept = TempDir::new().unwrap();
        let dir = unswept.path();
        seed_staged_meta(dir, "bucket/a.bin", &[(0, 1023, 512)]).await;
        seed_staged_meta(dir, "bucket/z.bin", &[(0, 1023, 512)]).await;
        let observed = new_shape(dir);
        assert_eq!(observed, old_shape(dir), "an unswept population must agree");
        assert_eq!(
            (observed.0, observed.1),
            (false, None),
            "fixture precondition: this state must be incomplete with a null age, or the \
             comparison above is over two sentinels and discriminates nothing"
        );

        // State 3: the same volume, swept — every figure now has a real value, so this is the arm
        // that would catch an inner form that silently returned a default.
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        for _ in 0..6 {
            if repair_rotation_complete(dir) {
                break;
            }
            run_ledger_repair(dir, &ledger, "host:1").await;
        }
        let swept = new_shape(dir);
        assert_eq!(swept, old_shape(dir), "a swept population must agree");
        assert!(
            swept.0 && swept.1.is_some() && swept.3 > 0,
            "fixture precondition: this state must be complete with a real age and a non-zero \
             swept count, got {swept:?}"
        );

        // State 4: an unreadable metadata tree. `None` from the walk is UNREADABLE, and the
        // `repair_rotation_complete` wrapper's `else { false }` must not become a vacuous `true`.
        let broken_meta = TempDir::new().unwrap();
        std::fs::write(broken_meta.path().join("metadata"), b"not a directory").unwrap();
        let broken = new_shape(broken_meta.path());
        assert_eq!(
            broken,
            old_shape(broken_meta.path()),
            "an unreadable metadata tree must agree"
        );
        assert_eq!(
            (broken.0, broken.2),
            (false, None),
            "fixture precondition: the unreadable arm must actually be reached"
        );

        // State 5: a readable population with an unreadable repair document set — `is_complete()`
        // is false, which is the branch that distinguishes "not swept" from "cannot tell".
        let broken_docs = TempDir::new().unwrap();
        let dir = broken_docs.path();
        seed_staged_meta(dir, "bucket/a.bin", &[(0, 1023, 512)]).await;
        let repair_root = repair_dir(dir);
        std::fs::create_dir_all(repair_root.join("bucket")).unwrap();
        std::fs::write(repair_root.join("bucket").join("aa.json"), b"{ not json").unwrap();
        let torn = new_shape(dir);
        assert_eq!(
            torn,
            old_shape(dir),
            "an unreadable repair document must agree"
        );
        assert!(
            !read_repair_checkpoints(dir).is_complete(),
            "fixture precondition: the document set must be incomplete, or the branch under test \
             is not reached"
        );
    }

    /// A traversal that never completed publishes the volume figures as sentinels and leaves the
    /// **per-process** figures alone.
    ///
    /// # This is the assertion that protects the design decision, not a wiring check
    ///
    /// `collect_ledger_repair_gauges` now runs inside `spawn_blocking`, so `/metrics` has a
    /// `JoinError` arm it did not have before, and that arm has to publish something for all
    /// fourteen fields. `LedgerRepairGauges::default()` is the obvious thing to write there and it
    /// is wrong: eight of the fourteen never touch the cache volume, so it would publish four
    /// process counters as `0` when they were readable and non-zero. A `0` in
    /// `ranges_reappended_total` is not a neutral reading — it is the "no finding" reading of the
    /// field whose only purpose is to surface staged ranges with no ledger entry — and this project
    /// has recorded the zero-versus-sentinel substitution ten times.
    ///
    /// # Why the discriminator is `directory_concurrency` and not just the atomics
    ///
    /// `ledger_repair_directory_concurrency` is clamped to `[1, 63]` and seeds at 32, so it is
    /// **never legitimately `0`** on any volume in any state. That makes it a discriminator that
    /// needs no global mutation at all, and therefore cannot be perturbed by another test running
    /// concurrently in the same binary. The three counters bumped below are a second, independent
    /// arm; `shard_budget_capped_total` is deliberately NOT bumped, because
    /// `reading_the_gauges_does_not_move_the_clamp_counter` asserts an exact equality on it.
    ///
    /// # RED SIDE
    ///
    /// With `LedgerRepairGauges::process_local`'s body replaced by `Self::default()`, this fails on
    /// `directory_concurrency` (`left: 0, right: 32`).
    #[test]
    fn a_failed_traversal_publishes_sentinels_and_keeps_the_per_process_figures() {
        // Distinctive bumps on the three counters no other test asserts an absolute value for.
        REPAIR_RANGES_REAPPENDED_TOTAL.fetch_add(7, Ordering::Relaxed);
        REPAIR_INCOMPLETE_TOTAL.fetch_add(5, Ordering::Relaxed);
        REPAIR_SWEEPS_COMPLETED_TOTAL.fetch_add(3, Ordering::Relaxed);

        let g = LedgerRepairGauges::process_local();

        // The six volume-derived fields: the UNREADABLE sentinel, never a figure. `false` for the
        // activation predicate is the conservative direction — it must not claim coverage it cannot
        // show.
        assert_eq!(
            (
                g.ledger_repair_observed_shards,
                g.ledger_repair_stalest_age_secs,
                g.ledger_repair_rotation_reachable,
                g.ledger_repair_projected_rotation_secs,
                g.ledger_repair_rotation_complete,
            ),
            (None, None, None, None, false),
            "a traversal that did not complete must publish null for every volume figure it has a \
             sentinel for, and must not report the activation precondition as satisfied"
        );
        // `swept_shards` is the one volume field with no sentinel available. It publishes 0, which
        // IS ambiguous — `observed_shards: null` above is the discriminator a consumer must read
        // alongside it, and that pairing is the reason this assertion is stated rather than elided.
        assert_eq!(g.ledger_repair_swept_shards, 0);

        // The eight per-process fields are readable whatever the volume is doing, so they must be
        // the live values. This is what a blanket `default()` gets wrong.
        assert!(
            g.ledger_repair_directory_concurrency >= 1,
            "directory_concurrency is clamped to [1, 63] and seeds at 32, so a 0 here means the \
             failure arm zeroed a figure that never needed the volume — got {}",
            g.ledger_repair_directory_concurrency
        );
        assert!(
            g.ledger_repair_ranges_reappended_total >= 7
                && g.ledger_repair_incomplete_total >= 5
                && g.ledger_repair_sweeps_completed_total >= 3,
            "the process counters must survive a traversal failure. Got reappended={}, \
             incomplete={}, sweeps={} against bumps of 7/5/3 — a zero here is the \
             zero-versus-sentinel error, and `ranges_reappended_total: 0` in particular is the \
             'no finding' reading of a finding-only field",
            g.ledger_repair_ranges_reappended_total,
            g.ledger_repair_incomplete_total,
            g.ledger_repair_sweeps_completed_total
        );
        assert_eq!(
            g.ledger_repair_shard_budget_capped_total,
            shard_budget_capped_total(),
            "and the clamp counter must be the live reading, not a zero"
        );
    }

    use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
    use tempfile::TempDir;

    /// The shard a cache key belongs to, through the production deriver.
    ///
    /// Not recomputed locally: the whole safety argument for a shard-scoped coverage set is that
    /// this function and `get_sharded_path` agree, so a fixture that re-derived the shard by hand
    /// could pass against a tree in which they had diverged.
    fn shard_of_key(cache_key: &str) -> Shard {
        let (bucket, l1) = crate::staged_fold::shard_of(cache_key).unwrap();
        Shard::new(bucket, l1)
    }

    /// Write a `.meta` at `cache_key`'s sharded path with `ranges` staged extents.
    ///
    /// `staged: None` on each range, so membership falls through to the object flag and the
    /// fixture exercises the same `is_staged_range_spec` fall-through production writes.
    async fn seed_staged_meta(cache_dir: &Path, cache_key: &str, ranges: &[(u64, u64, u64)]) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();
        let now = SystemTime::now();
        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                is_write_cached: true,
                ..Default::default()
            },
            ranges: ranges
                .iter()
                .map(|(start, end, size)| RangeSpec {
                    start: *start,
                    end: *end,
                    file_path: format!("r_{start}-{end}.bin"),
                    compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                    compressed_size: *size,
                    uncompressed_size: *size,
                    created_at: now,
                    last_accessed: now,
                    access_count: 0,
                    staged: None,
                    unservable: false,
                    file_identity: None,
                })
                .collect(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            ..Default::default()
        };
        tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
            .await
            .unwrap();
    }

    /// A `.meta` for an object that has GRADUATED — on disk, not staged.
    async fn seed_graduated_meta(cache_dir: &Path, cache_key: &str, ranges: &[(u64, u64, u64)]) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();
        let now = SystemTime::now();
        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                is_write_cached: false,
                ..Default::default()
            },
            ranges: ranges
                .iter()
                .map(|(start, end, size)| RangeSpec {
                    start: *start,
                    end: *end,
                    file_path: format!("r_{start}-{end}.bin"),
                    compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                    compressed_size: *size,
                    uncompressed_size: *size,
                    created_at: now,
                    last_accessed: now,
                    access_count: 0,
                    staged: Some(false),
                    unservable: false,
                    file_identity: None,
                })
                .collect(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            ..Default::default()
        };
        tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
            .await
            .unwrap();
    }

    /// A graduated object one of whose ranges still records `staged: Some(true)`.
    ///
    /// The state exists because the object flag and the per-range membership are written at
    /// different moments, and the per-range predicate deliberately never second-guesses a recorded
    /// `Some(true)`. It is the only state in which the object-level guard in [`sweep_shard_capped`]
    /// is load-bearing.
    async fn seed_graduated_meta_with_staged_range(
        cache_dir: &Path,
        cache_key: &str,
        start: u64,
        end: u64,
        size: u64,
    ) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();
        let now = SystemTime::now();
        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                is_write_cached: false,
                ..Default::default()
            },
            ranges: vec![RangeSpec {
                start,
                end,
                file_path: format!("r_{start}-{end}.bin"),
                compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                compressed_size: size,
                uncompressed_size: size,
                created_at: now,
                last_accessed: now,
                access_count: 0,
                staged: Some(true),
                unservable: false,
                file_identity: None,
            }],
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            ..Default::default()
        };
        tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
            .await
            .unwrap();
    }

    /// Every `(cache_key, start, end)` the ledger currently covers.
    async fn covered_identities(ledger: &WriteLedger) -> BTreeSet<(String, u64, u64)> {
        let read = ledger.read_for_authority(0).await.unwrap();
        read.entries
            .iter()
            .map(|e| (e.cache_key.clone(), e.range_start, e.range_end))
            .collect()
    }

    /// A staged `.meta` that NO ledger entry names is found and re-appended.
    ///
    /// # This is the gap the whole module exists for
    ///
    /// The fold's candidate set is `WriteLedger::group_by_key(read.entries)` and nothing else, so
    /// such an object contributes nothing to `F` — it lowers the staged figure and makes the
    /// eviction trigger fire LATE, the direction § 16.6.2 rules out by name. Nothing signalled it:
    /// the authority readers catch unreadable, malformed, truncated, a listing failure and a legacy
    /// flat file, and an entry that was never appended is none of those. Nor does the sixth class,
    /// file-count mismatch, catch it — that branch is unreachable, so it catches nothing at all.
    ///
    /// The assertion is on the ledger's own coverage before and after, i.e. the input the fold
    /// actually reads — not on a gauge derived from it.
    ///
    /// A failure implicates [`sweep_shard`]'s coverage test or its `append_staged_range` call.
    #[tokio::test]
    async fn a_staged_meta_with_no_ledger_entry_is_reappended() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // On disk and staged, with nothing in the ledger — the lost-append state.
        seed_staged_meta(dir, "bucket/orphan.bin", &[(0, 1023, 512)]).await;
        assert!(
            covered_identities(&ledger).await.is_empty(),
            "fixture precondition: the ledger must start with no coverage, or the sweep has \
             nothing to find and the test proves nothing"
        );

        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert!(
            matches!(outcome, RepairOutcome::Swept(ref s) if s.ranges_reappended == 1),
            "the sweep must find and re-append exactly the one uncovered staged range, got {:?}",
            outcome
        );
        assert!(
            covered_identities(&ledger)
                .await
                .contains(&("bucket/orphan.bin".to_string(), 0, 1023)),
            "after the repair the ledger must cover the range, because the ledger is the only \
             thing the fold's candidate set is built from"
        );
    }

    /// **A repair claim is released once its visit is installed**, so a peer's pass right after
    /// can sweep the shard instead of finding it `Busy` for the rest of the lease.
    ///
    /// Same shape as the fold's `release_claim`. The lease only has to exclude a
    /// peer WHILE the sweep runs; after the install, the rotation's stalest-first order already
    /// keeps peers away from a just-swept shard, so the lease only ever blocked a peer that had a
    /// reason to rank it first.
    ///
    /// # RED SIDE
    ///
    /// With no release, A's claim is live for [`REPAIR_CLAIM_LEASE_SECS`] and B's pass over the
    /// one-shard population is `Refused(NoShardAvailable)` with the shard in `busy`.
    #[tokio::test]
    async fn a_peer_pass_after_a_completed_sweep_is_not_busy() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        seed_staged_meta(dir, "bucket/released.bin", &[(0, 1023, 512)]).await;

        let ledger_a = WriteLedger::new(dir.to_path_buf(), "host:a".to_string());
        let outcome_a = run_ledger_repair(dir, &ledger_a, "host:a").await;
        assert!(
            matches!(outcome_a, RepairOutcome::Swept(ref s) if s.completed.len() == 1),
            "FIXTURE: instance A must complete a sweep of the one shard, got {:?}",
            outcome_a
        );

        let ledger_b = WriteLedger::new(dir.to_path_buf(), "host:b".to_string());
        let outcome_b = run_ledger_repair(dir, &ledger_b, "host:b").await;
        assert!(
            matches!(outcome_b, RepairOutcome::Swept(ref s) if s.busy.is_empty() && s.completed.len() == 1),
            "instance B's pass right after A's completed sweep must not find the shard Busy, got {:?}",
            outcome_b
        );
    }

    /// A claim that names a peer is not released by this instance.
    #[tokio::test]
    async fn release_repair_claim_leaves_a_peers_claim() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let shard = Shard::new("bucket", "ab".to_string());
        assert_eq!(
            claim_repair_shard(dir, &shard, "host:peer").await,
            ClaimState::Claimed
        );
        release_repair_claim(dir, &shard, "host:me");
        assert_eq!(
            claim_repair_shard(dir, &shard, "host:me").await,
            ClaimState::Busy,
            "releasing must only unlink a claim that names this instance"
        );
        release_repair_claim(dir, &shard, "host:peer");
        assert_eq!(
            claim_repair_shard(dir, &shard, "host:me").await,
            ClaimState::Claimed
        );
    }

    /// A staged range the ledger ALREADY covers is not re-appended.
    ///
    /// Without this the module would be a duplicate generator: it visits every shard on a rotation,
    /// so an unconditional append would add one entry per staged range per sweep, growing the
    /// ledger without bound.
    ///
    /// A failure implicates the `covered.contains(&identity)` skip in [`sweep_shard`].
    #[tokio::test]
    async fn an_already_covered_staged_range_is_not_reappended() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        seed_staged_meta(dir, "bucket/known.bin", &[(0, 1023, 512)]).await;
        ledger
            .append_staged_range("bucket/known.bin", 0, 1023, 512)
            .await
            .unwrap();
        let before = covered_identities(&ledger).await.len();

        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert!(
            matches!(outcome, RepairOutcome::Swept(ref s) if s.ranges_reappended == 0),
            "a covered range must not be re-appended, got {:?}",
            outcome
        );
        assert_eq!(
            covered_identities(&ledger).await.len(),
            before,
            "the coverage set must be unchanged; a growing ledger on every rotation is the \
             failure mode matching by identity-with-timestamp would produce"
        );
    }

    /// **Partial per-range loss inside an object that kept one entry is repaired.**
    ///
    /// # This is the assertion that distinguishes this rotation from the incumbent repair
    ///
    /// `CacheSizeTracker::reappend_missing_ledger_entries` matches by `cache_key` and its own doc
    /// records that as deliberate. The consequence is that an object retaining even ONE entry is
    /// "known", so every other lost range of that object stays lost forever — carried as a D1
    /// deferral. A `cache_key`-keyed matcher passes this fixture's first range and silently skips
    /// the second.
    ///
    /// Matching on `(cache_key, range_start, range_end)` is what closes it. Deliberately NOT
    /// `WriteLedgerEntry::identity()`'s 5-tuple, which includes the timestamp and the instance and
    /// so is *entry* identity: a legitimate re-PUT of the same extent produces a second identity,
    /// and matching on that would re-append a duplicate for every range on every sweep — which is
    /// exactly why the incumbent chose `cache_key`.
    ///
    /// The fixture makes the two answers differ: one object, two staged ranges, one covered. A
    /// key-keyed matcher re-appends 0; an identity-keyed one re-appends 1.
    ///
    /// A failure implicates the key shape of the `covered` set in [`run_ledger_repair`].
    #[tokio::test]
    async fn partial_per_range_loss_within_a_known_object_is_repaired() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // Two staged ranges on one object; only the FIRST is in the ledger.
        seed_staged_meta(
            dir,
            "bucket/partial.bin",
            &[(0, 1023, 512), (1024, 2047, 600)],
        )
        .await;
        ledger
            .append_staged_range("bucket/partial.bin", 0, 1023, 512)
            .await
            .unwrap();

        // The precondition that makes this test the discriminator it claims to be: the object IS
        // known by cache key, so a key-keyed matcher stops here.
        let staged_keys = ledger.staged_keys().await.unwrap();
        assert!(
            staged_keys.contains("bucket/partial.bin"),
            "fixture precondition: the object must be present by cache KEY, otherwise a \
             key-keyed matcher would also repair it and this test would not discriminate"
        );

        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert!(
            matches!(outcome, RepairOutcome::Swept(ref s) if s.ranges_reappended == 1),
            "the second range must be repaired even though the object is known by key, got {:?}",
            outcome
        );
        let covered = covered_identities(&ledger).await;
        assert!(
            covered.contains(&("bucket/partial.bin".to_string(), 1024, 2047)),
            "the uncovered extent must now be covered — this is the D1 case a cache_key-keyed \
             repair cannot see"
        );
        assert!(
            covered.contains(&("bucket/partial.bin".to_string(), 0, 1023)),
            "and the already-covered extent must still be covered exactly once"
        );
    }

    /// Ledger debt 16: what a lost per-range ledger record does to the write-tier figure and to
    /// staging eviction while a sibling entry survives, and which pass bounds it.
    ///
    /// The fold and eviction both judge a candidate object through [`assess_staged_candidate`],
    /// which reads the `.meta` and counts **every** staged range in it. So a surviving sibling
    /// entry carries the object: when the lost record is not the object's newest write, the
    /// figure and the eviction verdict are exactly what they would be with no loss (arm 1).
    ///
    /// The case that is not harmless is losing the record of the object's **newest** range. The
    /// surviving entry is then older than that range's `created_at`, so
    /// `is_superseded(newest_entry_at, newest_range_created_at)` holds and the whole object reads
    /// `Superseded`: `staged_bytes` 0, not evictable. Every staged byte of the object leaves `F`,
    /// which lowers `write_cache_size` and fires the staging eviction trigger late, the direction
    /// § 16.6.2 rules out (arm 2). `reappend_missing_ledger_entries` cannot see it, because the
    /// object is known by `cache_key`.
    ///
    /// Bounded by this module's rotation: `run_ledger_repair` matches on range identity,
    /// re-appends the lost extent stamped now, and the next assessment is `Evictable` with the
    /// full figure again (arm 2, after the sweep). Each shard is swept at least once per
    /// [`MAX_REPAIR_AGE_SECS`] (24 h) when the rotation keeps pace, which
    /// `write_cache.ledger_repair_rotation_complete` reports.
    ///
    /// A lost `.bin` under a surviving record is the opposite direction and not this debt: the
    /// `.meta` still names the range, so the fold counts its bytes (an over-count, which fires the
    /// trigger early) and eviction still selects the object (arm 3).
    #[tokio::test]
    async fn ledger_debt_16_a_lost_newest_range_record_hides_the_object_until_the_repair_sweep() {
        use crate::write_ledger::{assess_staged_candidate, StagedCandidateVerdict};

        async fn assess(
            dir: &Path,
            ledger: &WriteLedger,
            key: &str,
        ) -> (StagedCandidateVerdict, u64) {
            let read = ledger.read_for_authority(0).await.unwrap();
            let candidate = WriteLedger::group_by_key(read.entries)
                .into_iter()
                .find(|c| c.cache_key == key)
                .expect("PRECONDITION: the surviving entry keeps the object a candidate");
            let a = assess_staged_candidate(dir, &candidate).await;
            (a.verdict, a.staged_bytes)
        }

        // One staged object, two ranges written in order: A, then B. `created_at` is set
        // explicitly so the order is the fixture's, not the clock's resolution.
        async fn seed_ordered(dir: &Path, key: &str, a_at: SystemTime, b_at: SystemTime) {
            let meta_path =
                crate::disk_cache::get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
            tokio::fs::create_dir_all(meta_path.parent().unwrap())
                .await
                .unwrap();
            let range = |start: u64, end: u64, size: u64, at: SystemTime| RangeSpec {
                start,
                end,
                file_path: format!("r_{start}-{end}.bin"),
                compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                compressed_size: size,
                uncompressed_size: size,
                created_at: at,
                last_accessed: at,
                access_count: 0,
                staged: None,
                unservable: false,
                file_identity: None,
            };
            let metadata = NewCacheMetadata {
                cache_key: key.to_string(),
                object_metadata: ObjectMetadata {
                    is_write_cached: true,
                    ..Default::default()
                },
                ranges: vec![range(0, 1023, 512, a_at), range(1024, 2047, 600, b_at)],
                created_at: a_at,
                expires_at: b_at + std::time::Duration::from_secs(3600),
                ..Default::default()
            };
            tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
                .await
                .unwrap();
        }

        let full = 512 + 600;
        let past = SystemTime::now() - std::time::Duration::from_secs(60);

        // Arm 1: B is written and recorded, A's record is lost. The survivor is the newest
        // entry, so the object is carried in full.
        {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
            let key = "bucket/debt16-older-lost.bin";
            seed_ordered(dir, key, past, past).await;
            ledger
                .append_staged_range(key, 1024, 2047, 600)
                .await
                .unwrap();
            assert_eq!(
                assess(dir, &ledger, key).await,
                (StagedCandidateVerdict::Evictable, full),
                "losing a record that is not the object's newest must change neither the verdict \
                 nor the figure: the surviving entry carries every staged range in the .meta"
            );
        }

        // Arm 2: A is recorded, then B is written and its record is lost.
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let key = "bucket/debt16-newest-lost.bin";
        ledger.append_staged_range(key, 0, 1023, 512).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let b_at = SystemTime::now();
        seed_ordered(dir, key, past, b_at).await;
        assert_eq!(
            assess(dir, &ledger, key).await,
            (StagedCandidateVerdict::Superseded, 0),
            "CONSEQUENCE: with the newest range's record lost, the surviving older entry makes \
             the whole object read Superseded, so it contributes 0 to the write-tier figure and \
             staging eviction skips it"
        );
        assert!(
            ledger.staged_keys().await.unwrap().contains(key),
            "PRECONDITION: the object is known by cache_key, so reappend_missing_ledger_entries \
             would leave it alone"
        );

        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert!(
            matches!(outcome, RepairOutcome::Swept(ref s) if s.ranges_reappended == 1),
            "the repair sweep must re-append exactly the lost extent, got {:?}",
            outcome
        );
        // The re-appended entry is stamped at the sweep, after B was written, so it is now the
        // object's newest entry and the `Superseded` test no longer holds.
        assert_eq!(
            assess(dir, &ledger, key).await,
            (StagedCandidateVerdict::Evictable, full),
            "BOUND: after the range-identity sweep the object is evictable and counted in full"
        );

        // Arm 3: a lost `.bin` is invisible to the fold and to eviction's verdict, because both
        // read only the `.meta`. This fixture writes no range files at all, so every arm above
        // already ran with both `.bin`s absent: their bytes stay in the figure, an over-count,
        // the safe direction.
        let bin = dir.join("ranges").join("r_1024-2047.bin");
        assert!(
            !bin.exists(),
            "PRECONDITION: the fixture never wrote B's .bin"
        );
        assert_eq!(
            assess(dir, &ledger, key).await,
            (StagedCandidateVerdict::Evictable, full),
            "a range whose .bin is gone is still counted from the .meta"
        );
    }

    /// A graduated object is not re-appended — including when one of its ranges still records
    /// itself as staged.
    ///
    /// Re-appending a graduated object would give staging eviction a candidate whose bytes were
    /// never credited to `write_cache_size`, so freeing it would debit what nothing added. It would
    /// also disagree with the fold, whose `fold_staged_candidate` returns `Graduated` on the
    /// object-level flag and contributes zero — two mechanisms with two answers about one object.
    ///
    /// # The second arm is the whole test, and the first arm proved nothing on its own
    ///
    /// The first version of this used a fixture whose ranges carried `staged: Some(false)`, and it
    /// **passed with the object-level `is_write_cached` guard deleted**. `is_staged_range_spec`
    /// returns `false` for a `Some(false)` range whatever the object says, so the per-range filter
    /// was doing all the work and the guard the test named was unreachable. The red side came back
    /// green, which is the only reason it was noticed.
    ///
    /// So the arms are now chosen to separate the two predicates:
    ///
    /// - `staged: None` exercises the migration fall-through, where membership comes from the
    ///   object flag via `classify_new_range_as_staged`;
    /// - **`staged: Some(true)` on a graduated object** is the one state where the per-range
    ///   predicate says "staged" and only the object-level guard says otherwise. That is the arm
    ///   that fails when the guard is removed.
    #[tokio::test]
    async fn a_graduated_object_is_not_reappended() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // Arm 1: the migration fall-through. `staged: None` on a graduated object.
        seed_graduated_meta(dir, "bucket/done.bin", &[(0, 1023, 512)]).await;
        // Arm 2: a range that records itself as staged on an object that has graduated. The
        // per-range predicate honours the recorded `Some(true)` and never second-guesses it, so
        // ONLY the object-level guard excludes this.
        seed_graduated_meta_with_staged_range(dir, "bucket/mixed.bin", 0, 2047, 700).await;

        // The precondition that makes arm 2 the discriminator: assert the per-range predicate
        // really does say "staged" here, so a pass is attributable to the object-level guard.
        let mixed_path =
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), "bucket/mixed.bin", ".meta")
                .unwrap();
        let mixed: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&mixed_path).unwrap()).unwrap();
        assert!(
            crate::cache_types::is_staged_range_spec(
                &mixed.ranges[0],
                mixed.object_metadata.is_write_cached
            ),
            "fixture precondition: the per-range predicate must classify this range as STAGED, or \
             the per-range filter excludes it and the object-level guard is never reached — which \
             is exactly how the first version of this test passed with that guard deleted"
        );
        assert!(!mixed.object_metadata.is_write_cached);

        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert!(
            matches!(outcome, RepairOutcome::Swept(ref s) if s.ranges_reappended == 0),
            "a graduated object contributes no staged range however its ranges record themselves, \
             got {:?}",
            outcome
        );
        assert!(
            covered_identities(&ledger).await.is_empty(),
            "re-appending a graduated object would hand staging eviction a candidate whose bytes \
             were never credited, so freeing it would debit what nothing added — and would \
             disagree with fold_staged_candidate, which returns Graduated on the same object"
        );
    }

    /// An INCOMPLETE ledger read refuses **that shard's** sweep rather than repairing from a
    /// partial view.
    ///
    /// # Why refusing is the only safe answer here
    ///
    /// The coverage set is the ledger. A read that could not open one instance's file reports
    /// fewer covered identities than exist, so every range in the unread file looks uncovered and
    /// the repair appends a duplicate for each. That is the same input class the fold already
    /// refuses to install on, and it is worse here because the fold's refusal is reversible while
    /// an append is not.
    ///
    /// The incompleteness is a **malformed line appended to a real ledger file** — the torn-write
    /// signature `LedgerReadOutcome::malformed_lines` exists to count, and one of the five
    /// reachable classes the fold itself refuses to install on.
    ///
    /// # The torn line MUST be in the ORPHAN'S OWN shard, and that is the fixture's whole point
    ///
    /// The coverage read is now per shard, so a torn line in some *other* shard's ledger file
    /// leaves this orphan's shard perfectly readable and the sweep repairs it — correctly. An
    /// earlier version of this fixture tore `bucket/other.bin`'s file, which is a different shard,
    /// and would now assert a refusal that should not happen. The torn line therefore goes in the
    /// orphan's own shard file, reached by crediting a **different extent of the same key** so the
    /// entry lands in that file without covering the range under test.
    ///
    /// A first version put a *directory* at `host2.ledger`, expecting the read to fail on it. It
    /// does not: `WriteLedger::ledger_files_in` filters on `path.is_file()`, so a directory is
    /// never listed, never counted in `files_expected`, and the read reports **complete**. The
    /// test passed the pass and failed the refusal — a fixture that could not produce the state
    /// it named.
    ///
    /// A failure implicates the `read.is_complete()` gate inside [`run_ledger_repair`]'s visit
    /// loop.
    #[tokio::test]
    async fn an_incomplete_ledger_read_refuses_rather_than_repairing() {
        use std::io::Write;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        seed_staged_meta(dir, "bucket/orphan.bin", &[(0, 1023, 512)]).await;
        // A real ledger file IN THE ORPHAN'S OWN SHARD, so the shard-scoped read has a file to
        // fail on. A different extent of the same key: it lands in the same `(shard, instance)`
        // file without covering `(0, 1023)`, which is the range this pass must find uncovered.
        ledger
            .append_staged_range("bucket/orphan.bin", 4096, 8191, 16)
            .await
            .unwrap();

        // A torn trailing line on a real, listed, readable file.
        // Resolved through the production path builder: the layout is per `(shard, instance)`.
        let ledger_file = ledger.shard_path_for_key("bucket/orphan.bin").unwrap();
        assert!(
            ledger_file.is_file(),
            "fixture precondition: the ledger file must exist and be a FILE — \
             shard_ledger_files() filters on is_file(), so anything else is invisible to the \
             read and produces no incompleteness at all"
        );
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&ledger_file)
            .unwrap();
        writeln!(f, "{{\"partial\": tru").unwrap();
        drop(f);
        let shard = shard_of_key("bucket/orphan.bin");
        let read = ledger
            .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
            .await
            .unwrap();
        assert!(
            !read.is_complete() && read.malformed_lines > 0,
            "fixture precondition: the SHARD-SCOPED read must actually be incomplete, or the \
             sweep proceeds and this test asserts a refusal that cannot happen. Got \
             malformed_lines={} files_read={} files_expected={}",
            read.malformed_lines,
            read.files_read,
            read.files_expected
        );

        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert_eq!(
            outcome,
            RepairOutcome::Refused(
                RepairRefusal::LedgerIncomplete,
                RepairPassSummary {
                    ledger_incomplete: vec![shard.clone()],
                    ..Default::default()
                }
            ),
            "an incomplete ledger read must refuse, and must name the shard it refused: every \
             range in the unread file looks uncovered, so repairing would append a duplicate for \
             each one. The pass verdict is LedgerIncomplete and NOT NoShardAvailable, which is \
             the label that means healthy."
        );
        assert!(
            !covered_identities(&ledger).await.contains(&(
                "bucket/orphan.bin".to_string(),
                0,
                1023
            )),
            "and nothing may be appended on a refused pass"
        );
    }

    /// A pairing read that is not `is_complete` removes
    /// **nothing**.
    ///
    /// # Why this needs a fault seam
    ///
    /// The branch needs a shard whose *coverage* read succeeds — otherwise
    /// [`run_ledger_repair`] refuses before the purge runs at all — and whose later *pairing*
    /// read does not. No on-disk fixture produces that: both reads list the same files, so
    /// anything that tears one tears the other. `LedgerSyncProbe`'s
    /// `incomplete_authority_reads_from` reports authority reads from a given call index onward
    /// as incomplete, without touching a file, which is the only way in.
    ///
    /// Red side: a lever that takes the incomplete read's outcome anyway, i.e.
    /// `Ok(r) if r.is_complete() => r` relaxed to `Ok(r) => r`. Direction if wrong: the credit
    /// the read could not pair may be live, and removing a live credit under-states the staged
    /// figure, so the read tier is over-stated and read-tier eviction fires late — the direction
    /// that fills a shared volume. Leaving a stale one over-states the staged figure and fires
    /// staging eviction early, which is the bounded direction.
    #[tokio::test]
    async fn an_incomplete_pairing_read_removes_no_pending_credit() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let key = "bucket/pairing-incomplete.bin";
        seed_staged_meta(dir, key, &[(0, 1023, 512)]).await;
        // A STALE credit naming the range this pass re-appends, owned by another instance and
        // carrying a seq no ledger file holds. With a complete pairing read the purge removes
        // exactly this line, which is what makes the assertion below a claim about the
        // completeness gate and not about the survey.
        let shard = shard_of_key(key);
        let stale = crate::staged_fold::PendingCredit {
            seq: 999_999,
            bucket: shard.bucket.clone(),
            l1: shard.l1.clone(),
            bytes: 512,
            range: Some(PendingRange {
                cache_key: key.to_string(),
                start: 0,
                end: 1023,
            }),
        };
        PendingStore::new(dir.to_path_buf())
            .append_batch("host:9", &[stale])
            .await
            .unwrap();
        // The coverage read is call 0 and must stay complete; every read from call 1 on, the
        // pairing read included, is reported incomplete.
        *ledger
            .sync_probe()
            .incomplete_authority_reads_from
            .lock()
            .unwrap() = Some(1);
        let outcome = run_ledger_repair(dir, &ledger, "host:1").await;
        assert!(
            matches!(&outcome, RepairOutcome::Swept(s) if s.ranges_reappended == 1),
            "APPARATUS: the coverage read must have stayed complete and the sweep must have \
             re-appended the one missing range, or the purge was never reached: {outcome:?}"
        );
        assert!(
            ledger
                .sync_probe()
                .authority_reads_made_incomplete
                .load(Ordering::SeqCst)
                > 0,
            "APPARATUS: no authority read was faulted, so this fixture never reached the \
             incomplete-pairing-read branch it asserts about"
        );
        let credits = PendingStore::new(dir.to_path_buf())
            .read_all()
            .await
            .credits;
        assert!(
            credits
                .iter()
                .any(|(owner, c)| owner == "host:9" && c.seq == 999_999),
            "an incomplete pairing read must remove nothing: a credit it cannot pair may be \
             stale or may be one the read could not see, and removing a live credit \
             under-states the staged figure and fires read-tier eviction late, where leaving a \
             stale one over-states it and fires staging eviction early. credits={credits:?}"
        );
    }
    /// One shard's unreadable ledger file does NOT refuse a sibling shard's sweep.
    ///
    /// # Why this is the assertion that justifies scoping the read
    ///
    /// Before the coverage read was per shard, a single torn line anywhere on the volume made
    /// `run_ledger_repair` return `Refused(LedgerIncomplete)` before visiting any shard at all. A
    /// refused pass writes no repair document and does not consume the cadence, so the rotation
    /// never advanced — which means § 16.6.6 precondition 3b never passes and the staged-authority
    /// epoch never flips. One bad file blocked activation permanently.
    ///
    /// The verdict here is deliberately **not** the pass's return value. It is whether the clean
    /// shard's orphaned range ends up covered: pre-change it cannot, because no shard is ever
    /// swept. Asserting the outcome enum alone would pass on a tree that swept the clean shard and
    /// appended nothing.
    ///
    /// Passes are looped rather than counted. Two shards give a per-pass budget of one and both
    /// sort equal on `swept_at`, so which one a given pass reaches depends on shard-name order,
    /// i.e. on a hash. A fixture that assumed an order would be asserting on `blake3`.
    #[tokio::test]
    async fn one_shards_unreadable_ledger_does_not_refuse_a_siblings_sweep() {
        use std::io::Write;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // Two orphans in two different shards. `torn` also gets a same-key credit at another
        // extent, which is the ledger file the torn line goes into.
        seed_staged_meta(dir, "bucket/torn.bin", &[(0, 1023, 512)]).await;
        seed_staged_meta(dir, "bucket/clean.bin", &[(0, 2047, 1024)]).await;
        let torn_shard = shard_of_key("bucket/torn.bin");
        let clean_shard = shard_of_key("bucket/clean.bin");
        assert_ne!(
            torn_shard, clean_shard,
            "fixture precondition: the two keys must be in DIFFERENT shards, or there is no \
             sibling for the damage to spare and this test proves nothing"
        );
        ledger
            .append_staged_range("bucket/torn.bin", 4096, 8191, 16)
            .await
            .unwrap();
        let ledger_file = ledger.shard_path_for_key("bucket/torn.bin").unwrap();
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&ledger_file)
            .unwrap();
        writeln!(f, "{{\"partial\": tru").unwrap();
        drop(f);

        let mut saw_swept = false;
        let mut saw_the_torn_shard_skipped = false;
        for _ in 0..4 {
            match run_ledger_repair(dir, &ledger, "host:1").await {
                RepairOutcome::Swept(s) => {
                    saw_swept = true;
                    if s.ledger_incomplete.contains(&torn_shard) {
                        saw_the_torn_shard_skipped = true;
                    }
                }
                RepairOutcome::Refused(_, s) => {
                    if s.ledger_incomplete.contains(&torn_shard) {
                        saw_the_torn_shard_skipped = true;
                    }
                }
            }
        }

        let covered = covered_identities(&ledger).await;
        assert!(
            covered.contains(&("bucket/clean.bin".to_string(), 0, 2047)),
            "THE VERDICT: the healthy shard's uncovered range must be repaired even though a \
             sibling shard's ledger is torn. If this fails, one bad file on the volume is still \
             refusing every shard, and the repair rotation — and therefore activation — is \
             blocked by it indefinitely"
        );
        assert!(
            !covered.contains(&("bucket/torn.bin".to_string(), 0, 1023)),
            "and the TORN shard's range must NOT be repaired: its coverage set is a partial view, \
             so appending would duplicate whatever the unread portion of the file holds"
        );
        assert!(
            saw_swept,
            "at least one pass must report Swept, or the loop above only ever refused and the \
             assertion about the clean shard passed for some other reason"
        );
        assert!(
            saw_the_torn_shard_skipped,
            "and the torn shard must have been reached and named in ledger_incomplete, or it was \
             simply never visited and the fixture never exercised the skip"
        );
    }

    /// A `.meta` whose own `cache_key` derives a DIFFERENT shard from the directory holding it is
    /// skipped, not repaired — because this shard's coverage set cannot answer for it.
    ///
    /// # This is the one way a shard-scoped coverage set can generate duplicates
    ///
    /// The scoping is sound because `get_sharded_path` and `staged_fold::shard_of` take the same
    /// two hex digits of the same hash, so a `.meta` reachable by walking shard `S` names a key
    /// whose ledger entry is in `S`'s own directory. That is a property of two builders agreeing,
    /// not an invariant anything enforces — so if a `.meta` is ever written to, or moved to, a path
    /// its key does not derive, its entry lives in another shard's file, this shard's read cannot
    /// see it, and every one of its staged ranges looks uncovered.
    ///
    /// The consequence without the guard is not one bad append. The rotation revisits every shard
    /// forever, so it is **one duplicate entry per staged range per rotation, without bound** —
    /// which is the failure the module doc names and the reason `an_already_covered_staged_range_is
    /// _not_reappended` exists for the ordinary case.
    ///
    /// The assertion counts RAW ENTRIES rather than reading `covered_identities`, and that
    /// distinction is the test. The coverage set is a `BTreeSet` keyed on range identity, so it
    /// collapses duplicates by construction: a duplicate append is completely invisible in it.
    ///
    /// A failure implicates the `shard_of(metadata.cache_key)` guard in [`classify_meta`].
    #[tokio::test]
    async fn a_meta_whose_key_derives_another_shard_is_skipped_rather_than_duplicated() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // The key whose ranges are at stake, and the shard we will file its `.meta` under.
        let subject = "bucket/subject.bin";
        let elsewhere = "bucket/elsewhere.bin";
        let subject_shard = shard_of_key(subject);
        let host_shard = shard_of_key(elsewhere);
        assert_ne!(
            subject_shard, host_shard,
            "fixture precondition: the two keys must derive DIFFERENT shards, or the `.meta` is \
             filed where it belongs and the guard under test is never reached"
        );

        // A staged `.meta` naming `subject`, written at `elsewhere`'s sharded path — content and
        // location disagreeing, which is the state the guard exists for.
        let misfiled =
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), elsewhere, ".meta").unwrap();
        seed_staged_meta(dir, subject, &[(0, 1023, 512)]).await;
        let content = tokio::fs::read_to_string(
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), subject, ".meta").unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::remove_file(
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), subject, ".meta").unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(misfiled.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&misfiled, &content).await.unwrap();

        // Enough passes that the host shard is certainly visited more than once — a duplicate
        // generator produces one entry per visit, so a single pass could not distinguish
        // "appended once" from "appends every time".
        for _ in 0..6 {
            run_ledger_repair(dir, &ledger, "host:1").await;
        }

        let entries = ledger.read_for_authority(0).await.unwrap().entries;
        let appended = entries
            .iter()
            .filter(|e| e.cache_key == subject && e.range_start == 0 && e.range_end == 1023)
            .count();
        assert_eq!(
            appended,
            0,
            "a `.meta` whose key derives another shard must be reported and left alone. Got {} \
             raw ledger entr(y/ies) for {}, which is a duplicate generated on every rotation — \
             note the coverage set would show exactly ONE covered range either way, because it is \
             keyed on range identity and collapses duplicates. Full ledger: {:?}",
            appended,
            subject,
            entries
                .iter()
                .map(|e| (e.cache_key.clone(), e.range_start, e.range_end))
                .collect::<Vec<_>>()
        );
    }

    /// The rotation predicate is false before a sweep, true after one covering every observed
    /// shard, and false again once a shard exists that has not been swept.
    ///
    /// # The third arm is the one that matters
    ///
    /// § 16.6.6 precondition 3b is a statement about **coverage**, so a new shard appearing must
    /// un-satisfy it. An implementation that took the maximum age over the shards that happen to
    /// have documents would report healthiest exactly when the rotation had not reached a shard —
    /// the same failure the fold's own document reader avoids by making an unreadable shard
    /// unknown rather than absent.
    ///
    /// A failure implicates [`RepairCheckpointSet::stalest_completed_age_secs`]'s `?` on a missing
    /// document, or [`observed_shards_on_disk`].
    #[tokio::test]
    async fn the_rotation_predicate_tracks_coverage_and_not_merely_the_shards_it_has_swept() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        seed_staged_meta(dir, "bucket/a.bin", &[(0, 1023, 512)]).await;
        assert!(
            !repair_rotation_complete(dir),
            "a shard with no completed sweep cannot show a rotation, so 3b must not hold"
        );

        // Sweep until every observed shard is covered. One shard exists, so one pass suffices, but
        // the loop is bounded rather than assuming that.
        for _ in 0..4 {
            if repair_rotation_complete(dir) {
                break;
            }
            run_ledger_repair(dir, &ledger, "host:1").await;
        }
        assert!(
            repair_rotation_complete(dir),
            "after every observed shard has a completed sweep, 3b must hold"
        );

        // A brand-new shard: coverage is no longer total, so the predicate must fall.
        //
        // The two keys are chosen so their BLAKE3 first byte differs — asserted below rather than
        // assumed, because if they landed in the same shard the existing document would cover both
        // and this arm would pass vacuously.
        seed_staged_meta(dir, "bucket/z.bin", &[(0, 1023, 512)]).await;
        let population = observed_shards_on_disk(dir).unwrap();
        assert_eq!(
            population.len(),
            2,
            "fixture precondition: the two keys must land in DIFFERENT shards, or the new object \
             is already covered by the existing document and this arm proves nothing"
        );
        assert!(
            !repair_rotation_complete(dir),
            "a newly observed shard un-satisfies 3b until it is swept. Taking the age over only \
             the shards that have documents would report complete here."
        );
    }

    /// An unreadable metadata tree makes the rotation UNKNOWN, never complete.
    ///
    /// `None` from [`observed_shards_on_disk`] is the UNREADABLE sentinel and an empty population
    /// is a real answer; conflating them would let a volume whose tree cannot be listed report a
    /// satisfied precondition and activate on no evidence.
    #[test]
    fn an_unreadable_metadata_tree_blocks_the_rotation_predicate() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // A file where `metadata/` should be a directory: `read_dir` fails with NotADirectory
        // rather than NotFound, so this exercises the error arm and not the absent arm.
        std::fs::write(dir.join("metadata"), b"not a directory").unwrap();

        assert_eq!(
            observed_shards_on_disk(dir),
            None,
            "a listing failure is UNREADABLE, not an empty population"
        );
        assert!(
            !repair_rotation_complete(dir),
            "an unreadable tree must not satisfy 3b: activation would then be permitted on no \
             evidence at all"
        );
        assert_eq!(
            repair_stalest_age_secs(dir),
            None,
            "and the age must be null rather than a reassuring zero"
        );
    }

    /// Internal directories under `metadata/` are not mistaken for buckets, and a non-hex
    /// directory is not mistaken for a shard level.
    ///
    /// Counting an internal directory as a shard would put an entry in the population that no
    /// sweep can ever complete, so precondition 3b would never hold and activation would be
    /// blocked forever with nothing explaining why.
    ///
    /// # The fixture has to make the two filters separable, and the first version did not
    ///
    /// `_journals` and `_shard_state`'s real children are `checkpoints`, `claims`, `repair`,
    /// `ready` and `staged_pending` — none of which are two hex digits. So a fixture built from
    /// the real layout is excluded by [`is_shard_level`] alone, and it **passed with the
    /// `starts_with('_')` guard deleted**. The red side came back green, which is the only reason
    /// it was noticed.
    ///
    /// So the fixture now plants `_shard_state/ab`, a name that DOES satisfy `is_shard_level`.
    /// That is the only state in which the underscore guard is the thing doing the work — and it
    /// is not far-fetched, since the guard exists precisely so a future internal directory does
    /// not have to be lucky about its name.
    #[test]
    fn internal_directories_are_not_counted_as_shards() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let metadata = dir.join("metadata");
        std::fs::create_dir_all(metadata.join("_journals")).unwrap();
        std::fs::create_dir_all(metadata.join("_shard_state").join("checkpoints")).unwrap();
        // The discriminator: an internal directory whose child WOULD pass `is_shard_level`, so
        // only the leading-underscore rule excludes it.
        std::fs::create_dir_all(metadata.join("_shard_state").join("ab")).unwrap();
        assert!(
            is_shard_level("ab"),
            "fixture precondition: the planted name must satisfy is_shard_level, or that filter \
             excludes it and the underscore guard is never reached"
        );
        std::fs::create_dir_all(metadata.join("bucket").join("ab").join("cde")).unwrap();
        // Not two lowercase hex digits, so not a shard level.
        std::fs::create_dir_all(metadata.join("bucket").join("zz")).unwrap();
        std::fs::create_dir_all(metadata.join("bucket").join("abc")).unwrap();
        std::fs::create_dir_all(metadata.join("bucket").join("AB")).unwrap();

        let population = observed_shards_on_disk(dir).unwrap();
        assert_eq!(
            population,
            [Shard::new("bucket", "ab")].into_iter().collect(),
            "only two-lowercase-hex-digit directories under a non-underscore bucket are shards. \
             An internal directory counted as a shard would sit in the population unswept \
             forever, blocking precondition 3b permanently."
        );
    }

    /// A **FILE** at `metadata/{bucket}/` is not counted as a shard.
    ///
    /// This is the property the origin marker's placement rests on. `_origin.json` sits at
    /// exactly that level (`public-security-review-readiness` R2), and it is safe there
    /// because every production enumeration of `metadata/{bucket}/*` filters to directories
    /// before doing anything with an entry. Nothing pinned that before this test: the
    /// sibling [`internal_directories_are_not_counted_as_shards`] plants only
    /// **directories**, and `bucket_settings`'s legacy-scan tests do plant a file at bucket
    /// level but never run an L1 enumerator over it.
    ///
    /// Counting the marker as a shard would put an entry in the population that no sweep can
    /// ever complete, so § 16.6.6 precondition 3b would never hold and activation would be
    /// blocked forever with nothing explaining why — the same consequence the sibling test
    /// describes, reached by a different route.
    ///
    /// # `_origin.json` ALONE cannot make this test able to fail
    ///
    /// It is 12 characters, so [`is_shard_level`] rejects it whatever its type — it is
    /// excluded twice over, and a fixture planting only the marker would pass with the
    /// `is_dir()` check deleted. That is the exact shape the sibling test's docstring records
    /// paying for ("the red side came back green, which is the only reason it was noticed").
    ///
    /// So this fixture also plants a **file named `ab`**, a name that DOES satisfy
    /// `is_shard_level`. That is the only state in which `!l1_entry.path().is_dir()` is the
    /// filter doing the work, and it is not far-fetched: it is what a `.meta` misplaced by a
    /// path-construction bug, or a future two-character internal file, would look like.
    #[test]
    fn a_file_at_bucket_level_is_not_counted_as_a_shard() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let metadata = dir.join("metadata");
        let bucket = metadata.join("bucket");
        std::fs::create_dir_all(&bucket).unwrap();

        // The real artifact whose safety this test exists to pin.
        std::fs::write(
            bucket.join(crate::origin_identity::MARKER_FILE_NAME),
            br#"{"schema":1,"origin":"minio-a.example.com"}"#,
        )
        .unwrap();
        // The discriminator: a FILE whose name passes `is_shard_level`, so only the
        // directory check excludes it.
        std::fs::write(bucket.join("ab"), b"not a shard").unwrap();
        assert!(
            is_shard_level("ab"),
            "fixture precondition: the planted file's name must satisfy is_shard_level, or \
             that filter excludes it and the is_dir() check is never reached"
        );
        assert!(
            !is_shard_level(crate::origin_identity::MARKER_FILE_NAME),
            "fixture precondition: the marker's own name is ALSO rejected by is_shard_level, \
             which is why it cannot be the discriminator on its own"
        );
        // A real shard, so the assertion below distinguishes "the files were ignored" from
        // "the enumeration returned nothing at all".
        std::fs::create_dir_all(bucket.join("cd").join("ef0")).unwrap();

        let population = observed_shards_on_disk(dir).unwrap();
        assert_eq!(
            population,
            [Shard::new("bucket", "cd")].into_iter().collect(),
            "only DIRECTORIES two lowercase hex digits long are shards. The origin marker is \
             a file at bucket level precisely because every enumerator here filters on \
             is_dir(); a file counted as a shard would sit in the population unswept forever, \
             blocking precondition 3b permanently."
        );
    }

    /// A capped sweep stores a resume cursor, does NOT advance the completed-sweep timestamp, and
    /// finishes on a later visit.
    ///
    /// # Why the two fields cannot be one
    ///
    /// If a capped visit advanced `swept_at`, a shard that is repeatedly capped mid-sweep would
    /// report a fresh rotation it never completed — and 3b would be satisfied by a shard whose
    /// coverage was never established. `swept_at` therefore advances only on a sweep that reached
    /// the end, and `resume_l2` carries the in-flight position.
    ///
    /// Driven through [`sweep_shard`] with a cap of 1 rather than by seeding 20,000 objects.
    #[tokio::test]
    async fn a_capped_sweep_resumes_and_does_not_claim_a_completed_rotation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        // Two objects in the same shard but different L2 directories, so the sweep has two L2
        // steps and a cursor has somewhere to point.
        let mut keys = Vec::new();
        for i in 0..200 {
            let key = format!("bucket/obj-{i}.bin");
            let hash = blake3::hash(format!("obj-{i}.bin").as_bytes());
            let hex = hash.to_hex();
            keys.push((
                key,
                hex.as_str()[0..2].to_string(),
                hex.as_str()[2..5].to_string(),
            ));
        }
        // Find two keys sharing an L1 and differing in L2.
        let mut chosen: Option<(String, String)> = None;
        for a in 0..keys.len() {
            for b in (a + 1)..keys.len() {
                if keys[a].1 == keys[b].1 && keys[a].2 != keys[b].2 {
                    chosen = Some((keys[a].0.clone(), keys[b].0.clone()));
                    break;
                }
            }
            if chosen.is_some() {
                break;
            }
        }
        let (key_a, key_b) = chosen.expect(
            "fixture precondition: two of 200 keys must share an L1 and differ in L2, or the \
             cursor has no second step to resume at",
        );
        seed_staged_meta(dir, &key_a, &[(0, 1023, 512)]).await;
        seed_staged_meta(dir, &key_b, &[(0, 1023, 512)]).await;
        let shard = {
            let (bucket, l1) = crate::staged_fold::shard_of(&key_a).unwrap();
            Shard::new(bucket, l1)
        };

        // A cap of one `.meta` forces the cursor path.
        let covered = BTreeSet::new();
        let first = sweep_shard_capped(
            dir,
            &shard,
            &covered,
            None,
            &ledger,
            1,
            SWEEP_DIRECTORY_CONCURRENCY_SEED,
        )
        .await
        .unwrap();
        assert!(
            first.next_l2.is_some(),
            "a sweep stopped by the cap must report where to resume, or the remaining L2 \
             directories are never visited"
        );
        assert_eq!(first.metas_examined, 1);

        // Resuming from the cursor reaches the end.
        let covered = covered_identities(&ledger).await;
        let second = sweep_shard_capped(
            dir,
            &shard,
            &covered,
            first.next_l2.clone(),
            &ledger,
            100,
            SWEEP_DIRECTORY_CONCURRENCY_SEED,
        )
        .await
        .unwrap();
        assert!(
            second.next_l2.is_none(),
            "resuming from the cursor must complete the sweep"
        );
        assert!(
            second.metas_examined >= 1,
            "and it must actually visit the directories the first visit skipped"
        );
        assert_eq!(
            covered_identities(&ledger).await.len(),
            2,
            "both objects must end up covered exactly once across the two visits"
        );
    }

    /// A repair document's progress, as an ordering: `(swept_at, resume_l2)`.
    ///
    /// `swept_at` is the last COMPLETED sweep, so a larger one is always further on. At equal
    /// `swept_at`, `None` means no sweep in progress and `Some(cursor)` means a sweep begun after
    /// that completion has reached `cursor` in the sorted L2 order, so `None < Some(a) < Some(b)`
    /// for `a < b`, which is exactly `Option<String>`'s derived order.
    fn repair_progress(doc: &RepairShardCheckpoint) -> (u64, Option<String>) {
        (doc.swept_at, doc.resume_l2.clone())
    }

    /// Read the installed document, and assert its progress is not behind any installed before.
    fn assert_progress_not_backwards(
        dir: &Path,
        shard: &Shard,
        high_water: &mut Option<(u64, Option<String>)>,
        step: &str,
    ) -> RepairShardCheckpoint {
        let path = RepairShardCheckpoint::path(dir, shard);
        let installed = RepairShardCheckpoint::load_path(&path)
            .unwrap()
            .expect("an install must leave a document on disk");
        let now = repair_progress(&installed);
        if let Some(prior) = high_water.as_ref() {
            assert!(
                now >= *prior,
                "{step}: the installed repair document's progress moved BACKWARDS, from \
                 (swept_at={}, resume_l2={:?}) to (swept_at={}, resume_l2={:?}). A peer's later \
                 rename replaced a document it never read.",
                prior.0,
                prior.1,
                now.0,
                now.1
            );
        }
        if high_water.as_ref().is_none_or(|p| now > *p) {
            *high_water = Some(now);
        }
        installed
    }

    /// Every Write_Ledger entry in `shard` names a staged range on disk, with its size, and
    /// carries a seq; no instance appended one range twice. Returns the entry count and the
    /// keys covered.
    async fn assert_reappended_entries_valid(
        dir: &Path,
        shard: &Shard,
        staged: &BTreeMap<String, (u64, u64, u64)>,
    ) -> (usize, BTreeSet<String>) {
        let reader = WriteLedger::new(dir.to_path_buf(), "host:reader".to_string());
        let read = reader
            .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
            .await
            .unwrap();
        assert!(
            read.is_complete(),
            "the shard's ledger must read complete after two interleaved passes, or an append \
             was torn: {:?}",
            read
        );
        let mut per_instance: BTreeSet<(String, String, u64, u64)> = BTreeSet::new();
        for e in &read.entries {
            let (start, end, size) = staged.get(&e.cache_key).copied().unwrap_or_else(|| {
                panic!(
                    "ledger entry names {:?}, which is not a staged object on disk",
                    e.cache_key
                )
            });
            assert_eq!(
                (e.range_start, e.range_end, e.compressed_size),
                (start, end, size),
                "a re-appended entry must name the staged range and size the .meta records"
            );
            assert!(
                e.seq.is_some(),
                "every appended entry must carry a Ledger_Seq"
            );
            assert!(
                per_instance.insert((
                    e.instance_id.clone(),
                    e.cache_key.clone(),
                    e.range_start,
                    e.range_end
                )),
                "instance {} appended {} [{}-{}] twice",
                e.instance_id,
                e.cache_key,
                e.range_start,
                e.range_end
            );
        }
        let covered: BTreeSet<String> = read.entries.iter().map(|e| e.cache_key.clone()).collect();
        (read.entries.len(), covered)
    }

    /// Two instances sweeping the SAME shard, their repair-document installs interleaved, must
    /// never move the installed document's progress backwards, and every Write_Ledger entry either
    /// pass re-appended must be valid.
    ///
    /// # Why this can happen at all
    ///
    /// Since the change the repair runs on its own task on every instance. Cross-instance exclusion is
    /// only the repair claim, which the module calls non-load-bearing ("a lost, expired or
    /// duplicated claim costs duplicated work"), and the document is installed by a bare atomic
    /// rename. [`run_ledger_repair`] reads every repair document ONCE at pass start and builds each
    /// shard's new document from that read, so two passes that both read before either installs
    /// each write a document derived from state the other has already replaced.
    ///
    /// # How the interleaving is constructed
    ///
    /// Deterministically, from the production pieces in the order a duplicated claim produces:
    /// both instances take their `previous` from [`read_repair_checkpoints`] and their coverage
    /// set from `read_shard_for_authority` before either sweeps; each sweeps with
    /// [`sweep_shard_capped`]; then both install through [`install_visit_document`], the function
    /// [`run_ledger_repair`] calls, in the order that loses an update. A cap stands in for a shard
    /// larger than [`REPAIR_METAS_PER_VISIT_CAP`], as the other capped tests here do.
    ///
    /// - **Arm 1**: B completes the sweep and installs; A, capped mid-sweep from the same stale
    ///   `previous`, installs after it.
    /// - **Arm 2**: both are capped from the same basis; B gets further and installs first.
    ///
    /// # RED SIDE
    ///
    /// On the code before the change (an earlier commit, a bare rename of a document built from the stale
    /// `previous`) arm 1 fails at A's install, regressing `(swept_at=<B's completion>, None)` to
    /// `(0, Some(<A's cursor>))`, so a shard a peer had just completed reads as never swept. With
    /// only the equal-`swept_at` cursor clause of [`merge_repair_documents`] removed, arm 1 passes
    /// and arm 2 fails, the cursor moving from the third L2 directory back to the second.
    #[tokio::test]
    async fn two_instances_sweeping_one_shard_never_move_its_repair_progress_backwards() {
        let (shard, keys) = shard_with_many_l2_dirs(3);
        let staged: BTreeMap<String, (u64, u64, u64)> =
            keys.iter().map(|k| (k.clone(), (0, 1023, 512))).collect();
        let mut sorted_l2: Vec<String> = keys
            .iter()
            .map(|k| {
                let object = k.split_once('/').unwrap().1;
                blake3::hash(object.as_bytes()).to_hex().as_str()[2..5].to_string()
            })
            .collect();
        sorted_l2.sort();

        // `(cap_a, cap_b)` per arm.
        for (arm, cap_a, cap_b) in [("arm 1", 1u64, 100u64), ("arm 2", 1, 2)] {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            for key in &keys {
                seed_staged_meta(dir, key, &[(0, 1023, 512)]).await;
            }
            let ledger_a = WriteLedger::new(dir.to_path_buf(), "host:a".to_string());
            let ledger_b = WriteLedger::new(dir.to_path_buf(), "host:b".to_string());

            // Both passes read before either writes.
            let previous_a = read_repair_checkpoints(dir);
            let previous_b = read_repair_checkpoints(dir);
            let covered = |read: crate::write_ledger::LedgerReadOutcome| {
                read.entries
                    .iter()
                    .map(|e| (e.cache_key.clone(), e.range_start, e.range_end))
                    .collect::<BTreeSet<_>>()
            };
            let covered_a = covered(
                ledger_a
                    .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
                    .await
                    .unwrap(),
            );
            let covered_b = covered(
                ledger_b
                    .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
                    .await
                    .unwrap(),
            );
            let resume_a = previous_a.get(&shard).and_then(|d| d.resume_l2.clone());
            let resume_b = previous_b.get(&shard).and_then(|d| d.resume_l2.clone());
            let seed = SWEEP_DIRECTORY_CONCURRENCY_SEED;
            let swept_a =
                sweep_shard_capped(dir, &shard, &covered_a, resume_a, &ledger_a, cap_a, seed)
                    .await
                    .unwrap();
            let swept_b =
                sweep_shard_capped(dir, &shard, &covered_b, resume_b, &ledger_b, cap_b, seed)
                    .await
                    .unwrap();
            assert_eq!(
                swept_a.next_l2.as_deref(),
                Some(sorted_l2[1].as_str()),
                "FIXTURE ({arm}): A must be capped after the first L2 directory"
            );
            let expect_b = if cap_b >= 3 {
                None
            } else {
                Some(sorted_l2[2].as_str())
            };
            assert_eq!(
                swept_b.next_l2.as_deref(),
                expect_b,
                "FIXTURE ({arm}): B must get further than A"
            );

            let mut high_water = None;
            install_visit_document(dir, &shard, previous_b.get(&shard), &swept_b, "host:b")
                .unwrap();
            let after_b = assert_progress_not_backwards(
                dir,
                &shard,
                &mut high_water,
                &format!("{arm}, B's install"),
            );
            install_visit_document(dir, &shard, previous_a.get(&shard), &swept_a, "host:a")
                .unwrap();
            let after_a = assert_progress_not_backwards(
                dir,
                &shard,
                &mut high_water,
                &format!("{arm}, A's install after B's"),
            );
            assert_eq!(
                repair_progress(&after_a),
                repair_progress(&after_b),
                "{arm}: A's install carried less progress than B's, so the document must still \
                 say what B installed"
            );

            let (entries, covered_keys) =
                assert_reappended_entries_valid(dir, &shard, &staged).await;
            // The installed progress must be backed by coverage: every staged key in an L2
            // directory before the installed cursor (all of them when no sweep is in progress)
            // has a ledger entry, and no key past it does, since neither pass reached one.
            let expected: BTreeSet<String> = keys
                .iter()
                .filter(|k| {
                    let object = k.split_once('/').unwrap().1;
                    let l2 = blake3::hash(object.as_bytes()).to_hex().as_str()[2..5].to_string();
                    after_a.resume_l2.as_ref().is_none_or(|cursor| l2 < *cursor)
                })
                .cloned()
                .collect();
            assert_eq!(
                covered_keys, expected,
                "{arm}: the installed document claims progress the ledger's coverage does not back"
            );
            assert_eq!(
                entries as u64,
                swept_a.ranges_reappended + swept_b.ranges_reappended,
                "{arm}: every re-append either pass reported must be in the ledger, and nothing \
                 else"
            );
        }
    }

    /// Two instances that both find the same staged range missing from the Write_Ledger each
    /// re-append it, and the published staged figure must still count that range ONCE.
    ///
    /// # Why the duplicate is reachable
    ///
    /// [`claim_repair_shard`] reads the claim, writes its own, then re-reads. Two instances that
    /// both read before either writes both see no live claim; if the first re-reads before the
    /// second's rename lands, both re-read their own claim and both proceed. The lease
    /// ([`REPAIR_CLAIM_LEASE_SECS`]) also lapses under a sweep longer than it. So two sweeps of one shard can each build their coverage set before
    /// either appends, and each re-appends every range the ledger lacks: two ledger entries and two
    /// `Pending_Credit`s for one range on disk.
    ///
    /// # What each consumer does with it
    ///
    /// The fold groups ledger entries by key and takes bytes from the `.meta`, so an installed
    /// figure counts the range once and eviction retires both identities. The pending term summed
    /// every post-cut credit, so until a fold's cut passed both seqs the published figure held the
    /// range's bytes twice.
    ///
    /// # RED SIDE
    ///
    /// On the code before the change (a `Pending_Credit` carried no range identity, and
    /// [`crate::staged_fold::PendingReadOutcome::post_cut`] summed every post-cut credit) the
    /// published figure is 1024 against the 512 on disk.
    #[tokio::test]
    async fn two_instances_reappending_one_missing_range_publish_it_once() {
        let (shard, keys) = shard_with_many_l2_dirs(1);
        let key = keys[0].clone();
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        seed_staged_meta(dir, &key, &[(0, 1023, 512)]).await;
        // A checkpoint for the shard with no figure and no cut, so the figure is readable and
        // every credit is post-cut.
        crate::staged_fold::seed_shard_figure_for_tests(dir, &shard.bucket, &shard.l1, 0, 0);

        let ledger_a = WriteLedger::new(dir.to_path_buf(), "host:a".to_string());
        let ledger_b = WriteLedger::new(dir.to_path_buf(), "host:b".to_string());
        // Both passes build their coverage set before either appends: a duplicated claim.
        let covered_a: BTreeSet<(String, u64, u64)> = ledger_a
            .read_shard_for_authority(&shard.bucket, &shard.l1, 0)
            .await
            .unwrap()
            .entries
            .iter()
            .map(|e| (e.cache_key.clone(), e.range_start, e.range_end))
            .collect();
        let covered_b = covered_a.clone();
        let seed = SWEEP_DIRECTORY_CONCURRENCY_SEED;
        let swept_a = sweep_shard_capped(dir, &shard, &covered_a, None, &ledger_a, 100, seed)
            .await
            .unwrap();
        let swept_b = sweep_shard_capped(dir, &shard, &covered_b, None, &ledger_b, 100, seed)
            .await
            .unwrap();
        assert_eq!(
            (swept_a.ranges_reappended, swept_b.ranges_reappended),
            (1, 1),
            "FIXTURE: both instances must re-append the one missing range"
        );

        let figure = crate::staged_fold::published_figure_for_tests(dir).await;
        assert_eq!(
            figure,
            Some(512),
            "one staged range of 512 bytes re-appended by two instances must be published once"
        );
    }

    /// Pick a shard holding at least `want` distinct L2 directories, and return
    /// `(shard, keys sorted by L2)`.
    ///
    /// The L2 level is hash-derived (`get_sharded_path` takes hex digits 2..5 of the key's BLAKE3),
    /// so a fixture cannot choose its own directory layout — it has to search for keys that land
    /// where it needs them. Returned in L2 order because two of the assertions below are about
    /// **tree order**, and deriving the expectation from the same hash the production path uses is
    /// what stops the test asserting its own arithmetic.
    fn shard_with_many_l2_dirs(want: usize) -> (Shard, Vec<String>) {
        let mut by_l1: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for i in 0..40_000u32 {
            let name = format!("obj-{i}.bin");
            let hex = blake3::hash(name.as_bytes()).to_hex();
            let l1 = hex.as_str()[0..2].to_string();
            let l2 = hex.as_str()[2..5].to_string();
            let slot = by_l1.entry(l1.clone()).or_default();
            // One key per L2 directory, so every key contributes a distinct directory.
            slot.entry(l2).or_insert_with(|| format!("bucket/{name}"));
            if slot.len() >= want {
                let keys: Vec<String> = slot.values().take(want).cloned().collect();
                return (Shard::new("bucket", l1), keys);
            }
        }
        panic!(
            "fixture precondition: no L1 among 40,000 generated keys held {want} distinct L2 \
             directories, so the window boundary cannot be crossed and this test would measure a \
             single-window sweep"
        );
    }

    /// A sweep spanning MORE than one read window covers every directory, and the resume cursor is
    /// a low-water mark rather than "wherever the fan-out happened to finish".
    ///
    /// # The fixture has to cross a window boundary, and the realistic case does not
    ///
    /// The sweep reads and lists concurrently in windows of [`SWEEP_READ_CONCURRENCY`] L2
    /// directories, and the cursor is advanced only at a window boundary. **Every pre-existing test
    /// in this module uses one or two L2 directories**, so all of them run inside a single window
    /// and none of them can observe the cursor arithmetic at all: a sweep that advanced the cursor
    /// by the whole window instead of by the directories it actually consumed would pass every one
    /// of them while silently skipping directories on any real shard.
    ///
    /// So this fixture builds `SWEEP_READ_CONCURRENCY + 6` directories — the smallest count that
    /// crosses the boundary with something on the far side — and asserts the precondition rather
    /// than assuming the hash cooperated. This is the third recorded instance in this workstream of
    /// a fixture built from the realistic case failing to reach the guard under test, and the first
    /// where the guard is a windowing arithmetic that did not exist when the other fixtures were
    /// written.
    ///
    /// # Two properties, and the cap is what makes the first one reachable
    ///
    /// Arm 1 drives the sweep with a cap that forces a **mid-window** stop, which is the only state
    /// where "advance by the window" and "advance by what was consumed" differ. Without the cap the
    /// two are identical and the arm proves nothing.
    ///
    /// Arm 2 asserts appends are issued in **tree order** rather than completion order. Reads
    /// complete out of order by construction now, so without the explicit sort in phase 3 the
    /// ledger's sequence would be a function of I/O timing.
    ///
    /// **Arm 2's red side is also the only direct evidence in the suite that the fan-out is real.**
    /// Removing the sort produces a thoroughly shuffled order over these 70 directories — a serial
    /// implementation would emit tree order with or without the sort, so the shuffle is the
    /// concurrency being observed rather than asserted. It is recorded here because a timing
    /// assertion would be the obvious way to test "is it actually parallel" and would be a flake.
    ///
    /// A failure in arm 1 implicates the `next_index = next_index.saturating_add(consumed_dirs)`
    /// line at the end of `sweep_shard_capped`'s window loop, or the window-trim loop above it. A
    /// failure in arm 2 implicates the `found.sort_by(...)` immediately before the append phase.
    #[tokio::test]
    async fn a_sweep_spanning_several_read_windows_covers_every_directory_in_tree_order() {
        // ---- Arm 1: capped, multi-window, resumed to completion.
        let want = SWEEP_READ_CONCURRENCY + 6;
        let (shard, keys) = shard_with_many_l2_dirs(want);
        assert!(
            keys.len() > SWEEP_READ_CONCURRENCY,
            "fixture precondition: {} directories does not cross the {}-directory window \
             boundary, so the cursor arithmetic is unreachable and this test cannot fail for the \
             reason it exists",
            keys.len(),
            SWEEP_READ_CONCURRENCY
        );

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        for key in &keys {
            seed_staged_meta(dir, key, &[(0, 1023, 512)]).await;
        }

        // A cap well below one window forces a mid-window stop on every visit, so the sweep has to
        // resume across the boundary many times.
        const CAP: u64 = 10;
        let mut cursor: Option<String> = None;
        let mut visits = 0usize;
        loop {
            visits += 1;
            assert!(
                visits <= want,
                "the sweep did not terminate within {want} visits at a cap of {CAP} over \
                 {} directories — a cursor that fails to advance loops on one prefix forever",
                keys.len()
            );
            let covered = covered_identities(&ledger).await;
            let sweep = sweep_shard_capped(
                dir,
                &shard,
                &covered,
                cursor.clone(),
                &ledger,
                CAP,
                SWEEP_DIRECTORY_CONCURRENCY_SEED,
            )
            .await
            .expect("the sweep must not refuse on a well-formed fixture");
            match sweep.next_l2 {
                Some(next) => {
                    if let Some(previous) = &cursor {
                        assert!(
                            next > *previous,
                            "the cursor must advance strictly over the sorted L2 order; it went \
                             {previous} -> {next}, which either repeats work forever or names a \
                             directory already swept"
                        );
                    }
                    cursor = Some(next);
                }
                None => break,
            }
        }
        assert!(
            visits > 1,
            "fixture precondition: the cap must actually stop the sweep at least once, or the \
             resume path is never entered"
        );

        let covered = covered_identities(&ledger).await;
        assert_eq!(
            covered.len(),
            keys.len(),
            "every staged range must be covered exactly once across the resumed visits. A cursor \
             advanced by the window size rather than by the directories consumed skips the \
             remainder of each window silently, and the count is the only thing that sees it."
        );
        for key in &keys {
            assert!(
                covered.contains(&(key.clone(), 0, 1023)),
                "{key} was in the tree and was never re-appended, so a directory was skipped"
            );
        }

        // ---- Arm 2: one uncapped visit over the same layout, appends in tree order.
        let tmp2 = TempDir::new().unwrap();
        let dir2 = tmp2.path();
        let ledger2 = WriteLedger::new(dir2.to_path_buf(), "host:1".to_string());
        for key in &keys {
            seed_staged_meta(dir2, key, &[(0, 1023, 512)]).await;
        }
        let sweep = sweep_shard_capped(
            dir2,
            &shard,
            &BTreeSet::new(),
            None,
            &ledger2,
            u64::MAX,
            SWEEP_DIRECTORY_CONCURRENCY_SEED,
        )
        .await
        .unwrap();
        assert!(sweep.next_l2.is_none(), "an uncapped visit must complete");
        assert_eq!(sweep.ranges_reappended as usize, keys.len());

        // `Ledger_Seq` is assigned by the append itself, so ordering by seq IS append order.
        let read = ledger2.read_for_authority(0).await.unwrap();
        let mut entries = read.entries.clone();
        entries.sort_by_key(|e| e.seq.expect("every appended entry carries a seq"));
        let appended: Vec<String> = entries.iter().map(|e| e.cache_key.clone()).collect();
        // `keys` comes back from the fixture in L2 order, which is the tree order the sweep walks.
        assert_eq!(
            appended, keys,
            "appends must be issued in tree order, not in read-completion order. With {} \
             concurrent reads in flight, completion order is a function of I/O timing, so a \
             missing sort here makes two runs of the same repair produce different ledgers.",
            SWEEP_READ_CONCURRENCY
        );
    }

    /// The pre-T1 predicate, kept as the differential test's **reference implementation**.
    ///
    /// This is deliberately a second copy of a one-line filter rather than a call into
    /// production: its whole purpose is to be the thing [`list_l2_dirs`] is compared against, and
    /// a reference that called the code under test would compare a function with itself.
    fn stat_based_l2_listing(shard_root: &Path) -> Vec<String> {
        std::fs::read_dir(shard_root)
            .unwrap()
            .flatten()
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect()
    }

    /// **T1's one behavioural difference, and the assertion that makes § 16.6.13's no-symlink
    /// invariant load-bearing rather than decorative.**
    ///
    /// `Path::is_dir` follows symlinks and [`std::fs::DirEntry::file_type`] does not, so a symlink
    /// to a directory is the single input on which the two disagree: the old filter **included** it
    /// and swept its `.meta` files, and this one **excludes** it. An excluded directory is an
    /// under-count of the accounting this sweep feeds, which is § 16.6.2's unsafe direction — so
    /// T1's "never worse" is true only on a tree with no symlinks, and that is why the invariant is
    /// stated rather than inferred.
    ///
    /// Asserts the disagreement in **both** directions on one fixture, because either half alone is
    /// satisfiable by a listing that is simply broken: `file_type()` must exclude the link while
    /// the stat-based reference includes it. The real L2 directory beside it is the control — it
    /// must survive both, or the test proves only that the listing returns fewer things.
    ///
    /// A `DT_LNK` arm is deliberately absent from production. Given the invariant there is nothing
    /// for it to do, and this test documents the excluded state rather than making it supported.
    #[test]
    fn t1_a_symlinked_l2_directory_is_excluded_where_the_stat_based_filter_included_it() {
        let tmp = TempDir::new().unwrap();
        let shard_root = tmp.path().join("metadata").join("bucket").join("ab");
        std::fs::create_dir_all(shard_root.join("real")).unwrap();
        // The link target lives OUTSIDE the shard, which is the operator action the invariant
        // forbids: grafting capacity in from another filesystem rather than growing the volume.
        let elsewhere = tmp.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, shard_root.join("linked")).unwrap();

        let mut old = stat_based_l2_listing(&shard_root);
        old.sort();
        assert_eq!(
            old,
            vec!["linked".to_string(), "real".to_string()],
            "reference implementation: `Path::is_dir` follows the link, so the pre-T1 filter \
             INCLUDED a symlinked L2 directory. If this half fails, the fixture is not \
             reproducing the disagreement and the other half proves nothing."
        );

        let mut new = list_l2_dirs(&shard_root, &Shard::new("bucket", "ab")).unwrap();
        new.sort();
        assert_eq!(
            new,
            vec!["real".to_string()],
            "`file_type()` reports the entry itself, so a symlinked L2 directory is `is_symlink()` \
             and not `is_dir()` — it is EXCLUDED, and every object beneath it becomes invisible to \
             the sweep. That is an under-count, so this listing is correct only under \
             § 16.6.13's no-symlink invariant. The real directory beside it must still be listed, \
             or this is a broken listing rather than the documented exclusion."
        );
    }

    /// **Equivalence.** On a tree obeying the invariant, the changed listing yields exactly the
    /// same directories as the stat-based one it replaces.
    ///
    /// Compares sorted **vectors**, not counts: two different sets can have the same cardinality,
    /// so a count assertion would pass on a listing that swapped one directory for another. The
    /// fixture deliberately mixes the cases the filter has to separate — plain directories, files
    /// at the same level (including one named like a `.meta`), a dotted name, and nesting below
    /// L2 — because a filter that admitted files would still match on a directories-only fixture.
    #[test]
    fn t1_l2_listing_is_equivalent_to_the_stat_based_one_on_a_symlink_free_tree() {
        let tmp = TempDir::new().unwrap();
        let shard_root = tmp.path().join("metadata").join("bucket").join("cd");
        for d in ["000", "01f", "fff", ".hidden"] {
            std::fs::create_dir_all(shard_root.join(d)).unwrap();
        }
        // Mixed depth: content below L2 must not change what the L2 listing itself reports.
        std::fs::write(shard_root.join("000").join("obj.meta"), b"{}").unwrap();
        std::fs::create_dir_all(shard_root.join("01f").join("deeper")).unwrap();
        // Files at L2 level, one named like a `.meta`, so a filter that admitted files would fail.
        std::fs::write(shard_root.join("stray.meta"), b"{}").unwrap();
        std::fs::write(shard_root.join("notadir"), b"x").unwrap();

        let mut expected = stat_based_l2_listing(&shard_root);
        expected.sort();
        let mut actual = list_l2_dirs(&shard_root, &Shard::new("bucket", "cd")).unwrap();
        actual.sort();
        assert_eq!(
            actual, expected,
            "the changed traversal must yield the SAME set of L2 directories as the stat-based \
             one on a tree obeying the invariant. A traversal that misses one silently \
             under-counts every `.meta` beneath it."
        );
        assert_eq!(
            actual,
            vec![
                ".hidden".to_string(),
                "000".to_string(),
                "01f".to_string(),
                "fff".to_string()
            ],
            "pinned independently of the reference so both implementations agreeing on a WRONG \
             answer still fails"
        );
    }

    // ================================================================================
    // § 16.6.13 T4/T5/T6 — the unified pipeline, the continuous split, and one enumerator
    // ================================================================================

    /// The three measured backends' `(d̂, r̂)` pairs, from § 16.6.13's crossover table.
    ///
    /// `o*` is `d̂/r̂`: **6.5** on EFS, **12.1** on FSx for OpenZFS, **31.3** on FSx for NetApp
    /// ONTAP. Named here so the fixtures below can state which side of which crossover they sit on
    /// instead of asserting against a bare number.
    const MEASURED_BACKENDS: [(&str, f64, f64); 3] = [
        ("EFS", 2.234, 0.345),
        ("FSx for OpenZFS", 0.387, 0.0320),
        ("FSx for NetApp ONTAP", 0.688, 0.0220),
    ];

    /// The highest `o*` of the three measured backends, so a fixture can be placed above **every**
    /// one of them rather than above whichever happened to be checked.
    ///
    /// `31.3` on FSx for NetApp ONTAP. A fixture below this is not necessarily file-bound anywhere;
    /// a fixture above it is file-bound everywhere measured.
    const HIGHEST_MEASURED_CROSSOVER: f64 = 31.3;

    /// `W_d` is **exactly** `W/2` at the crossover, on every measured backend.
    ///
    /// # Why this is the assertion and not a throughput figure
    ///
    /// The split branches on `W_d`, computed from `d̂`, `r̂` and `ô`. Asserting a throughput number
    /// that correlates with `W_d` would be the adjacent-value substitution this workstream keeps
    /// finding — and it would be a timing assertion, so it would flake. `W/2` at `ô = o*` is an
    /// exact algebraic consequence of the formula (`W·d̂/(d̂ + d̂) = W/2`), so it holds to the unit on
    /// every backend and pins the formula rather than a proxy for it.
    ///
    /// A failure implicates the `raw` expression in [`compute_directory_concurrency`].
    #[test]
    fn the_split_is_exactly_half_the_width_at_the_crossover() {
        for (name, d, r) in MEASURED_BACKENDS {
            let crossover = d / r;
            let w_d = compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, crossover, r);
            assert_eq!(
                w_d,
                SWEEP_READ_CONCURRENCY / 2,
                "{name}: at ô = o* = {crossover} the two stages demand equal service, so the split \
                 must be exactly half of {}. Got {w_d}.",
                SWEEP_READ_CONCURRENCY
            );
            // And the crossover really is where the two stages balance, rather than a number that
            // happens to produce W/2 — assert the underlying equality the formula rests on.
            assert!(
                (crossover * r - d).abs() < 1e-9,
                "{name}: fixture precondition — ô·r̂ must equal d̂ at the crossover, or this test is \
                 not evaluating the balance point at all"
            );
        }
    }

    /// The split reproduces § 16.6.13's own published table, and the reference tree is on the
    /// DIRECTORY-bound side of every measured crossover while the design point is on the FILE-bound
    /// side.
    ///
    /// # This is the vacuity trap the section warns about, made explicit
    ///
    /// The reference tree's occupancy is `ô ≈ 1.66`, which sits **below every backend's `o*`** — 3.9×
    /// below EFS's, 7.3× below FSxZ's, 18.8× below FSxN's. A fixture built from realistic occupancy
    /// can therefore only ever exercise the directory-bound branch, and a test built from it would
    /// pass while never evaluating the file-bound regime at all. So this asserts both ends: `ô` =
    /// 1.66 (below every crossover) and `ô` = 95 (the design point, above every crossover).
    ///
    /// The expected values are § 16.6.13's table, which was derived independently of this code. A
    /// disagreement is a real finding in one of the two.
    #[test]
    fn the_split_reproduces_the_published_table_on_both_sides_of_the_crossover() {
        // (backend, d̂, r̂, ô, expected W_d)
        let table: [(&str, f64, f64, f64, usize); 4] = [
            ("EFS, reference tree", 2.234, 0.345, 1.66, 51),
            ("EFS, design point", 2.234, 0.345, 95.0, 4),
            ("FSxN, reference tree", 0.688, 0.0220, 1.66, 61),
            ("FSxN, design point", 0.688, 0.0220, 95.0, 16),
        ];
        for (label, d, r, o, expected) in table {
            let w_d = compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, o, r);
            assert_eq!(
                w_d, expected,
                "{label}: § 16.6.13's table gives W_d = {expected} at ô = {o}, d̂ = {d} ms, \
                 r̂ = {r} ms. This implementation gives {w_d}."
            );
        }
        // The regimes, stated as a property rather than left implicit in the numbers above.
        for (name, d, r) in MEASURED_BACKENDS {
            let below = compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, 1.66, r);
            let above = compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, 95.0, r);
            assert!(
                below > SWEEP_READ_CONCURRENCY / 2,
                "{name}: the reference tree's ô = 1.66 is below o* = {}, so the sweep is \
                 directory-bound there and more than half the width must go to directory reads. \
                 Got {below}.",
                d / r
            );
            assert!(
                above < SWEEP_READ_CONCURRENCY / 2,
                "{name}: the design point's ô = 95 is above o* = {}, so the sweep is file-bound \
                 there and less than half the width must go to directory reads. Got {above}.",
                d / r
            );
        }
    }

    /// The split is a CONTINUOUS, monotonically falling function of occupancy — which is what means
    /// there is no discontinuity to flap across.
    ///
    /// Two properties over a decade either side of EFS's crossover: `W_d` never rises as `ô` rises,
    /// and consecutive samples never jump by more than a few units, so no input value is a cliff.
    /// A switch-based design would show a step of tens of units at one point; this must not.
    ///
    /// A failure implicates the `raw` expression or the `round` in
    /// [`compute_directory_concurrency`].
    #[test]
    fn the_split_falls_continuously_and_never_steps() {
        let (d, r) = (2.234_f64, 0.345_f64);
        let mut previous = usize::MAX;
        let mut largest_step = 0usize;
        let mut o = 0.1_f64;
        while o <= 200.0 {
            let w_d = compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, o, r);
            assert!(
                w_d <= previous,
                "W_d must never rise as occupancy rises: at ô = {o} it went {previous} -> {w_d}. A \
                 non-monotonic split would move work toward whichever stage is already the \
                 bottleneck."
            );
            if previous != usize::MAX {
                largest_step = largest_step.max(previous - w_d);
            }
            previous = w_d;
            o *= 1.05;
        }
        assert!(
            largest_step <= 3,
            "the largest single step in W_d across a 2,000x sweep of occupancy was {largest_step}. \
             A continuous function of a measured quantity must not contain a cliff — a cliff is \
             precisely the discontinuity T5 exists to avoid, and an estimate sitting on one would \
             flap."
        );
    }

    /// A pathological estimate cannot starve either stage.
    ///
    /// The clamp is not decoration. `W_f = 0` deadlocks the pipeline outright — the file stage could
    /// never drain what the directory stage produced — and `W_d = 0` means no directory is ever
    /// enumerated, so the sweep would report a complete visit of a shard it never looked at. Both are
    /// reachable from an estimate this function must accept: occupancy zero drives the raw value to
    /// the full width, and a very large occupancy drives it below one.
    ///
    /// `NaN` has its own arm because `NaN as usize` is `0` in Rust, so an unchecked `NaN` would
    /// produce exactly the starved directory stage rather than an obvious failure.
    ///
    /// A failure implicates the `clamp` or one of the three guard arms in
    /// [`compute_directory_concurrency`].
    #[test]
    fn a_pathological_estimate_cannot_starve_either_stage() {
        let w = SWEEP_READ_CONCURRENCY;
        let seed = SWEEP_DIRECTORY_CONCURRENCY_SEED;
        let cases: [(&str, f64, f64, f64, usize); 8] = [
            (
                "zero occupancy would take the whole width",
                2.0,
                0.0,
                0.3,
                w - 1,
            ),
            (
                "zero read latency would take the whole width",
                2.0,
                5.0,
                0.0,
                w - 1,
            ),
            ("enormous occupancy would take nothing", 2.0, 1e12, 0.3, 1),
            (
                "enormous read latency would take nothing",
                2.0,
                5.0,
                1e12,
                1,
            ),
            (
                "a zero readdir latency is not an operating point",
                0.0,
                5.0,
                0.3,
                seed,
            ),
            ("NaN must not become zero", f64::NAN, 5.0, 0.3, seed),
            (
                "infinite occupancy is not a measurement",
                2.0,
                f64::INFINITY,
                0.3,
                seed,
            ),
            (
                "a negative estimate is not a measurement",
                2.0,
                -5.0,
                0.3,
                w - 1,
            ),
        ];
        for (why, d, o, r, expected) in cases {
            let w_d = compute_directory_concurrency(w, d, o, r);
            assert_eq!(
                w_d, expected,
                "{why}: compute_directory_concurrency({w}, d̂={d}, ô={o}, r̂={r}) gave {w_d}, \
                 expected {expected}"
            );
            assert!(
                (1..w).contains(&w_d),
                "{why}: W_d = {w_d} is outside [1, {}], so one stage is starved",
                w - 1
            );
        }
    }

    /// The EWMA moves an estimate by exactly one eighth of the gap, and refuses a sample that is not
    /// a measurement.
    ///
    /// Tested through [`ewma_blend`] rather than through [`TRAVERSAL_ESTIMATES`], because the
    /// estimates are a process-wide static and tests in this module run concurrently in one process:
    /// a test asserting on the static could be perturbed by any other test that completed a shard
    /// visit, and would flake in whichever ran second.
    ///
    /// A failure implicates [`ewma_blend`] or [`TRAVERSAL_EWMA_ALPHA`].
    #[test]
    fn the_ewma_moves_an_estimate_by_one_eighth_of_the_gap() {
        assert_eq!(
            ewma_blend(None, Some(8.0)),
            Some(8.0),
            "the first sample must seed outright. Blending against a zero would spend eight visits \
             climbing to a figure the first visit already knew, and the split would be wrong for \
             all of them."
        );
        let blended = ewma_blend(Some(8.0), Some(16.0)).expect("a finite sample must produce one");
        assert!(
            (blended - 9.0).abs() < 1e-12,
            "α = 1/8 means one anomalous visit moves the estimate by 12.5% of the gap: 8 -> 16 must \
             give 9.0, got {blended}"
        );
        // A sample that is not a measurement leaves the estimate alone. The NaN arm is the one that
        // matters: NaN reaches `compute_directory_concurrency`, and `NaN as usize` is 0.
        for (why, sample) in [
            ("a missing sample", None),
            ("NaN", Some(f64::NAN)),
            ("infinity", Some(f64::INFINITY)),
            ("a negative duration", Some(-1.0)),
        ] {
            assert_eq!(
                ewma_blend(Some(8.0), sample),
                Some(8.0),
                "{why} must leave the estimate unchanged rather than poisoning it"
            );
        }
    }

    /// Seed an L2 layout DIRECTLY, so a fixture can choose its own occupancy.
    ///
    /// # Why this bypasses `get_sharded_path`
    ///
    /// The L2 level is hash-derived, so a fixture that seeds through the production path cannot
    /// choose how many `.meta` files land in one directory — it can only search for keys that happen
    /// to collide, and finding 40 keys in one cell of a 1,048,576-cell space needs tens of millions
    /// of candidates. Since the sweep **lists** the tree and never re-derives a path from a key,
    /// writing the layout directly exercises exactly the same code with an occupancy the test
    /// controls. That is the only way to put a fixture on the far side of the crossover at all.
    ///
    /// Objects are seeded NOT write-cached, so nothing is appended to the ledger and the fixture
    /// stays fast. The enumeration under test is independent of staged-ness: `classify_meta` returns
    /// `Ok(vec![])` for a graduated object, which is still a result, so every enumerated path
    /// appears in the sweep's output either way.
    ///
    /// Returns the absolute `.meta` paths, sorted, which is what the sweep's own ordering should
    /// reproduce.
    async fn seed_l2_layout(
        cache_dir: &Path,
        shard: &Shard,
        dirs: usize,
        metas_per_dir: usize,
    ) -> Vec<PathBuf> {
        let shard_root = cache_dir
            .join("metadata")
            .join(&shard.bucket)
            .join(&shard.l1);
        let now = SystemTime::now();
        let mut seeded = Vec::new();
        for d in 0..dirs {
            // Three lowercase hex digits, matching the production L2 name shape.
            let l2 = format!("{:03x}", d);
            let l2_path = shard_root.join(&l2);
            tokio::fs::create_dir_all(&l2_path).await.unwrap();
            for m in 0..metas_per_dir {
                let cache_key = format!("{}/seeded-{l2}-{m}.bin", shard.bucket);
                let metadata = NewCacheMetadata {
                    cache_key: cache_key.clone(),
                    object_metadata: ObjectMetadata {
                        is_write_cached: false,
                        ..Default::default()
                    },
                    ranges: Vec::new(),
                    created_at: now,
                    expires_at: now + std::time::Duration::from_secs(3600),
                    ..Default::default()
                };
                let path = l2_path.join(format!("seeded-{m}.meta"));
                tokio::fs::write(&path, serde_json::to_string(&metadata).unwrap())
                    .await
                    .unwrap();
                seeded.push(path);
            }
            // A non-`.meta` file in every directory, so "the enumerator filters by suffix" is
            // exercised rather than assumed. A sweep that returned these would fail to parse them.
            tokio::fs::write(l2_path.join("stray.bin"), b"not metadata")
                .await
                .unwrap();
        }
        seeded.sort();
        seeded
    }

    /// **The differential test (T6.1).** Both directory/file splits enumerate the same paths, and it
    /// compares the sorted path VECTORS rather than counts.
    ///
    /// # Vectors, not counts, and the reason is not pedantry
    ///
    /// Two different sets can have equal cardinality. A count comparison passes when one split
    /// enumerates directory 7 twice and directory 8 not at all, which is precisely the class of
    /// defect an ordered directory stage exists to prevent. The vectors are compared element by
    /// element.
    ///
    /// # `W_d = 1` and `W_d = W−1` are the extremes T6 names
    ///
    /// At `W_d = 1` the directory stage is serial and 63 reads are outstanding; at `W_d = W−1` the
    /// file stage is serial and 63 readdirs are outstanding. Those are the two schedulings, and they
    /// are the *same code* at two parameter values — which is what makes the equivalence hold by
    /// construction and this test a check on top rather than the proof.
    ///
    /// # Both arms are run CAPPED as well as uncapped, and the capped arm is the stronger one
    ///
    /// Uncapped, both splits necessarily enumerate the whole shard, so agreement is weak evidence.
    /// Capped, the two must also agree on *where to stop*, which is the property that depends on the
    /// directory stage yielding in tree order rather than in completion order. A `buffer_unordered`
    /// directory stage passes the uncapped arm and fails the capped one.
    ///
    /// A failure implicates the `.buffered(w_d)` call in `sweep_shard_recorded` — specifically its
    /// being `buffered` rather than `buffer_unordered` — or the cap gate immediately below it.
    #[tokio::test]
    async fn both_directory_splits_enumerate_the_same_paths() {
        const DIRS: usize = SWEEP_READ_CONCURRENCY + 6;
        // The fixture must exceed the total width, or both splits run in one batch and the
        // scheduling difference is unobservable. A COMPILE-time assertion rather than a runtime one:
        // both sides are constants, so a later edit that shrinks the fixture should fail the build
        // rather than produce a test that passes for the wrong reason.
        const _: () = assert!(
            DIRS > SWEEP_READ_CONCURRENCY,
            "fixture precondition: the directory count must exceed SWEEP_READ_CONCURRENCY"
        );
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let shard = Shard::new("bucket", "ab");
        let seeded = seed_l2_layout(dir, &shard, DIRS, 1).await;

        let narrow_w_d = 1;
        let wide_w_d = SWEEP_READ_CONCURRENCY - 1;

        // ---- Arm 1: uncapped. Both splits must enumerate the whole shard.
        let mut narrow: Vec<PathBuf> = Vec::new();
        let narrow_sweep = sweep_shard_recorded(
            dir,
            &shard,
            &BTreeSet::new(),
            None,
            &ledger,
            u64::MAX,
            narrow_w_d,
            Some(&mut narrow),
        )
        .await
        .expect("an uncapped sweep of a well-formed fixture must not refuse");
        let mut wide: Vec<PathBuf> = Vec::new();
        let wide_sweep = sweep_shard_recorded(
            dir,
            &shard,
            &BTreeSet::new(),
            None,
            &ledger,
            u64::MAX,
            wide_w_d,
            Some(&mut wide),
        )
        .await
        .expect("an uncapped sweep of a well-formed fixture must not refuse");
        narrow.sort();
        wide.sort();
        assert_eq!(
            narrow.len(),
            seeded.len(),
            "fixture precondition: the sweep enumerated {} of {} seeded .meta files, so the \
             comparison below would be between two equally incomplete answers",
            narrow.len(),
            seeded.len()
        );
        assert_eq!(
            narrow, seeded,
            "the sweep must enumerate exactly the .meta files on disk — no more (the stray .bin in \
             every directory must be filtered) and no fewer"
        );
        assert_eq!(
            narrow, wide,
            "W_d = {narrow_w_d} and W_d = {wide_w_d} must enumerate the same path VECTOR. They are \
             the same code consuming the same enumerator at two widths, so a difference here means \
             the split changed *what* is swept rather than only how fast."
        );
        assert_eq!(
            (narrow_sweep.dirs_read, narrow_sweep.metas_examined),
            (wide_sweep.dirs_read, wide_sweep.metas_examined),
            "the two splits must also agree on the derived counts they publish"
        );

        // ---- Arm 2: capped mid-shard. Both splits must also agree on WHERE TO STOP.
        const CAP: u64 = 10;
        let mut narrow_capped: Vec<PathBuf> = Vec::new();
        let narrow_stop = sweep_shard_recorded(
            dir,
            &shard,
            &BTreeSet::new(),
            None,
            &ledger,
            CAP,
            narrow_w_d,
            Some(&mut narrow_capped),
        )
        .await
        .expect("a capped sweep must not refuse");
        let mut wide_capped: Vec<PathBuf> = Vec::new();
        let wide_stop = sweep_shard_recorded(
            dir,
            &shard,
            &BTreeSet::new(),
            None,
            &ledger,
            CAP,
            wide_w_d,
            Some(&mut wide_capped),
        )
        .await
        .expect("a capped sweep must not refuse");
        narrow_capped.sort();
        wide_capped.sort();
        assert!(
            narrow_stop.next_l2.is_some(),
            "fixture precondition: a cap of {CAP} over {DIRS} single-file directories must stop the \
             sweep, or this arm is the uncapped arm again and proves nothing about the stop point"
        );
        assert!(
            narrow_capped.len() < seeded.len(),
            "fixture precondition: the capped arm enumerated everything, so there is no stop point \
             to disagree about"
        );
        assert_eq!(
            narrow_capped, wide_capped,
            "capped, the two splits must enumerate the same path vector — which requires them to \
             stop at the same directory. A directory stage that yielded in completion order would \
             stop wherever I/O timing put it, so these vectors would differ run to run."
        );
        assert_eq!(
            narrow_stop.next_l2, wide_stop.next_l2,
            "and they must store the same resume cursor. A cursor that is a function of completion \
             order silently skips the directories that finished late."
        );
    }

    /// `dirs_read` and `metas_examined` are derived from the enumeration, and occupancy is their
    /// ratio — asserted on fixtures BELOW and ABOVE every measured crossover.
    ///
    /// # The fixtures are the point of this test
    ///
    /// `ô` = 1 sits below all three backends' crossovers (6.5 / 12.1 / 31.3) and is the only regime a
    /// realistically-shaped fixture can reach. `ô` = 40 sits above all three. `ô` = 6.5 is EFS's
    /// crossover exactly, which is where T4's phase-split penalty is maximal and where a
    /// strategy switch would otherwise have to decide. All three are checked, because a test built
    /// only from the realistic case would exercise one branch and read as covering both.
    ///
    /// # What each half asserts
    ///
    /// Uncapped: `dirs_read` equals the directory count the L1 listing named and `metas_examined`
    /// equals the seeded file count — so both figures are the enumeration's, not a parallel counter's.
    /// Capped: `metas_examined` stops at a directory boundary at or above the cap, `dirs_read` is
    /// consistent with it, and a cursor is stored. The T6.3 invariant inside the sweep would have
    /// refused the visit if `dirs_read` disagreed with the positions advanced, so a returned
    /// `ShardSweep` is itself the evidence that check passed.
    ///
    /// A failure implicates `dirs_read`/`metas_examined`'s derivation from `yields` at the end of
    /// `sweep_shard_recorded`, or the cap gate that fills `yields`.
    #[tokio::test]
    async fn the_derived_counts_come_from_the_enumeration_on_both_sides_of_the_crossover() {
        // (label, dirs, metas per dir, ô, which side of the measured crossovers)
        let fixtures: [(&str, usize, usize, f64); 3] = [
            ("below every crossover (the realistic shape)", 12, 1, 1.0),
            ("at EFS's crossover, where T4's penalty peaks", 4, 6, 6.0),
            ("above every measured crossover", 4, 40, 40.0),
        ];
        for (label, dirs, metas_per_dir, occupancy) in fixtures {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
            let shard = Shard::new("bucket", "cd");
            let seeded = seed_l2_layout(dir, &shard, dirs, metas_per_dir).await;

            let sweep = sweep_shard_recorded(
                dir,
                &shard,
                &BTreeSet::new(),
                None,
                &ledger,
                u64::MAX,
                SWEEP_DIRECTORY_CONCURRENCY_SEED,
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{label}: an uncapped sweep must not refuse, got {e:?}"));

            assert_eq!(
                sweep.dirs_read, dirs as u64,
                "{label}: dirs_read must equal the {dirs} L2 directories the L1 listing named. It \
                 is the denominator of ô and the left-hand side of the coverage invariant, so a \
                 short value means a directory was silently treated as empty."
            );
            assert_eq!(
                sweep.metas_examined,
                seeded.len() as u64,
                "{label}: metas_examined must equal the {} .meta files the enumerator yielded",
                seeded.len()
            );
            assert!(
                sweep.next_l2.is_none(),
                "{label}: an uncapped sweep must reach the end of the shard"
            );

            let observed = sweep.metas_examined as f64 / sweep.dirs_read as f64;
            assert!(
                (observed - occupancy).abs() < 1e-9,
                "{label}: ô is metas_examined/dirs_read, which must be {occupancy} on this \
                 fixture. Got {observed}."
            );

            // The regime this fixture is in, stated against the measured crossovers rather than
            // against a bare number, so the assertion says which branch it reaches.
            for (name, d, r) in MEASURED_BACKENDS {
                let w_d = compute_directory_concurrency(SWEEP_READ_CONCURRENCY, d, observed, r);
                let expected_directory_bound = observed < d / r;
                assert_eq!(
                    w_d > SWEEP_READ_CONCURRENCY / 2,
                    expected_directory_bound,
                    "{label} on {name}: ô = {observed} against o* = {}, so the split must be \
                     {} half the width. Got W_d = {w_d}.",
                    d / r,
                    if expected_directory_bound {
                        "above"
                    } else {
                        "at or below"
                    }
                );
            }

            // ---- Capped, over the same layout: the counts must still be the enumeration's.
            let cap = metas_per_dir as u64;
            let capped = sweep_shard_recorded(
                dir,
                &shard,
                &BTreeSet::new(),
                None,
                &ledger,
                cap,
                SWEEP_DIRECTORY_CONCURRENCY_SEED,
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{label}: a capped sweep must not refuse, got {e:?}"));
            assert_eq!(
                capped.dirs_read, 1,
                "{label}: a cap of {cap} .meta files over directories holding {metas_per_dir} each \
                 must stop after exactly one directory. Overshoot is bounded at one directory, \
                 which is what lets the cursor be an L2 name rather than a name plus an offset."
            );
            assert_eq!(
                capped.metas_examined, cap,
                "{label}: and metas_examined must be that one directory's yield"
            );
            assert_eq!(
                capped.next_l2.as_deref(),
                Some("001"),
                "{label}: the cursor must name the next directory in sorted order, not the one just \
                 finished — otherwise the resumed visit re-sweeps it forever"
            );
        }
    }

    /// The FILE-bound extreme is reachable, and a directory holding more than the entire cap still
    /// makes progress.
    ///
    /// # The second half is the non-termination guard
    ///
    /// The gate always admits its first directory. Without that, a cap smaller than a single
    /// directory's occupancy would refuse every directory, store a cursor naming the directory it
    /// refused, and the next visit would refuse the same one — a sweep that never terminates and a
    /// rotation that never completes, reported as a healthy capped visit each time. At `ô` = 40 with
    /// a cap of 5 this is exactly the state, and it is unreachable by any fixture built at the
    /// reference tree's `ô` = 1.66 because there a cap below one directory's yield means a cap of
    /// zero.
    ///
    /// A failure implicates the `!state.yields.is_empty()` condition in the cap gate.
    #[tokio::test]
    async fn a_directory_larger_than_the_whole_cap_still_makes_progress() {
        const DIRS: usize = 4;
        const METAS_PER_DIR: usize = 40;
        const CAP: u64 = 5;
        // Both preconditions are relations between constants, so they are checked at COMPILE time: a
        // later edit that shrinks the fixture below the crossover, or raises the cap above one
        // directory's yield, should fail the build rather than leave a test that passes while
        // reaching neither the file-bound regime nor the always-admit-the-first rule.
        const _: () = assert!(
            METAS_PER_DIR as f64 > HIGHEST_MEASURED_CROSSOVER,
            "fixture precondition: ô must exceed the highest measured crossover (31.3, FSx for \
             NetApp ONTAP), or this fixture is not in the file-bound regime on any backend"
        );
        const _: () = assert!(
            CAP < METAS_PER_DIR as u64,
            "fixture precondition: the cap must be smaller than one directory's yield, or the \
             always-admit-the-first rule is never reached"
        );

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let shard = Shard::new("bucket", "ef");
        let seeded = seed_l2_layout(dir, &shard, DIRS, METAS_PER_DIR).await;

        let mut cursor: Option<String> = None;
        let mut visits = 0usize;
        let mut total_examined = 0u64;
        let mut total_dirs = 0u64;
        loop {
            visits += 1;
            assert!(
                visits <= DIRS + 1,
                "the sweep did not terminate in {} visits at a cap of {CAP} over {DIRS} \
                 directories of {METAS_PER_DIR} files. A gate that refused its first directory \
                 loops on one prefix forever.",
                DIRS + 1
            );
            let sweep = sweep_shard_recorded(
                dir,
                &shard,
                &BTreeSet::new(),
                cursor.clone(),
                &ledger,
                CAP,
                1,
                None,
            )
            .await
            .expect("the sweep must not refuse on a well-formed fixture");
            assert_eq!(
                sweep.metas_examined, METAS_PER_DIR as u64,
                "each visit must consume exactly the one directory it is allowed, overshooting the \
                 cap by at most that directory"
            );
            total_examined += sweep.metas_examined;
            total_dirs += sweep.dirs_read;
            match sweep.next_l2 {
                Some(next) => {
                    if let Some(previous) = &cursor {
                        assert!(
                            next > *previous,
                            "the cursor must advance strictly over the sorted L2 order; it went \
                             {previous} -> {next}"
                        );
                    }
                    cursor = Some(next);
                }
                None => break,
            }
        }
        assert_eq!(
            total_dirs, DIRS as u64,
            "every L2 directory must be enumerated exactly once across the resumed visits"
        );
        assert_eq!(
            total_examined,
            seeded.len() as u64,
            "and every .meta with it. A visit that advanced its cursor past a directory it had not \
             enumerated would leave those files uncovered while reporting a completed rotation."
        );
    }

    // ── Per-visit sampling (ADAPT-U4, § 16.6.13 K2) ─────────────────────────────────────────
    // These test the RECORDING, not the controller's decision.

    const SAMPLING_PEER: &str = "ip-10-9-9-9.internal:4242";

    fn fresh_limiter() -> std::sync::Mutex<crate::scanner_limiter::ScannerLimiter> {
        std::sync::Mutex::new(crate::scanner_limiter::ScannerLimiter::new(4))
    }

    /// (b) A quiet repair visit is recorded at level 1, with its rate.
    #[tokio::test]
    async fn sampled_repair_records_a_quiet_visit_at_level_one() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        seed_staged_meta(dir, "bucket/sampled.bin", &[(0, 1023, 512)]).await;
        let limiter = fresh_limiter();
        let sampler = crate::journal_consolidator::VisitSampler::new(
            dir,
            "host:1",
            crate::scanner_registry::ScanKind::Repair,
            &limiter,
        );
        let t0 = std::time::Instant::now();
        let outcome = run_ledger_repair_sampled(dir, &ledger, "host:1", Some(&sampler)).await;
        let wall = t0.elapsed().as_secs_f64();
        assert!(
            matches!(outcome, RepairOutcome::Swept(ref s) if s.metas_examined == 1),
            "{outcome:?}"
        );
        let lim = limiter.lock().unwrap();
        assert_eq!(lim.samples_discarded(), 0);
        assert_eq!(lim.samples_at(1), 1, "one completed visit at level n+1 = 1");
        let rate = lim.rate_estimate(1).expect("a rate at level 1");
        assert!(
            rate.is_finite() && rate >= 1.0 / wall,
            "rate={rate} wall={wall}"
        );
    }

    /// (a) A peer document published between a repair visit's two reads discards the visit.
    #[tokio::test]
    async fn sampled_repair_discards_a_visit_whose_peer_count_changed() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        seed_staged_meta(dir, "bucket/sampled.bin", &[(0, 1023, 512)]).await;
        let limiter = fresh_limiter();
        let published = std::sync::atomic::AtomicBool::new(false);
        let sampler = crate::journal_consolidator::VisitSampler::new(
            dir,
            "host:1",
            crate::scanner_registry::ScanKind::Repair,
            &limiter,
        )
        .with_after_read(|| {
            if !published.swap(true, std::sync::atomic::Ordering::SeqCst) {
                crate::scanner_registry::publish(
                    dir,
                    SAMPLING_PEER,
                    crate::scanner_registry::ScanKind::Repair,
                )
                .unwrap();
            }
        });
        let outcome = run_ledger_repair_sampled(dir, &ledger, "host:1", Some(&sampler)).await;
        assert!(matches!(outcome, RepairOutcome::Swept(_)), "{outcome:?}");
        let lim = limiter.lock().unwrap();
        assert_eq!(lim.samples_discarded(), 1, "n changed 0 -> 1: discarded");
        for j in 1..=8 {
            assert_eq!(lim.samples_at(j), 0, "table unchanged at level {j}");
        }
    }

    /// (c) A busy repair visit produces no sample.
    #[tokio::test]
    async fn sampled_repair_busy_visit_records_nothing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
        let key = "bucket/sampled.bin";
        seed_staged_meta(dir, key, &[(0, 1023, 512)]).await;
        let shard = shard_of_key(key);
        assert!(
            matches!(
                claim_repair_shard(dir, &shard, SAMPLING_PEER).await,
                ClaimState::Claimed
            ),
            "fixture: the peer holds the only shard"
        );
        let limiter = fresh_limiter();
        let sampler = crate::journal_consolidator::VisitSampler::new(
            dir,
            "host:1",
            crate::scanner_registry::ScanKind::Repair,
            &limiter,
        );
        let outcome = run_ledger_repair_sampled(dir, &ledger, "host:1", Some(&sampler)).await;
        assert!(
            matches!(outcome, RepairOutcome::Refused(_, ref s) if s.busy == vec![shard.clone()]),
            "{outcome:?}"
        );
        let lim = limiter.lock().unwrap();
        assert_eq!(lim.samples_discarded(), 0, "busy visit: no discard");
        for j in 1..=8 {
            assert_eq!(lim.samples_at(j), 0, "busy visit: no sample at level {j}");
        }
    }
}

#[cfg(test)]
mod warn_remedy_tests {
    use super::*;

    #[test]
    fn capped_budget_warn_names_the_remedy_and_the_reach_matches_the_predicate() {
        assert!(REPAIR_BUDGET_CAPPED_WARN.contains("add a proxy instance"));
        assert!(REPAIR_BUDGET_CAPPED_WARN.contains("no configuration field"));
        // The figure docs/CONFIGURATION.md quotes, and exactly the boundary of the predicate.
        let reach = shards_reachable_per_instance();
        assert_eq!(reach, 589_824);
        assert!(rotation_reachable_for(reach as usize));
        assert!(!rotation_reachable_for(reach as usize + 1));
    }
}

/// the repair rotation's directory listing reaps stale write-order markers and never
/// reports one as a `.meta`.
#[cfg(test)]
mod marker_reap_tests {
    use super::*;

    #[tokio::test]
    async fn the_l2_listing_reaps_a_stale_marker_and_counts_only_meta_files() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path();
        std::fs::create_dir_all(dir.join("locks")).unwrap();
        let key = "case19-bucket/object.bin";
        let meta =
            crate::disk_cache::get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
        std::fs::create_dir_all(meta.parent().unwrap()).unwrap();
        std::fs::write(&meta, "{}").unwrap();
        let marker = crate::cache::write_marker_path(&meta);
        std::fs::write(
            &marker,
            serde_json::to_string(&crate::cache::WriteMarker {
                token: "t".to_string(),
                cache_key: key.to_string(),
            })
            .unwrap(),
        )
        .unwrap();
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
        filetime::set_file_mtime(&marker, filetime::FileTime::from_system_time(old)).unwrap();
        let l2 = meta.parent().unwrap().to_path_buf();
        let shard = Shard::new(
            "case19-bucket".to_string(),
            l2.parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        );

        let paths = enumerate_l2_meta_paths(dir, &l2, &shard).await.ok();

        assert_eq!(
            (paths, marker.exists()),
            (Some(vec![meta.clone()]), false),
            "(listed paths, stale marker still present)"
        );
    }
}
