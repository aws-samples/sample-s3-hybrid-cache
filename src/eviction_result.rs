//! The last read-tier eviction pass's result, on shared storage.
//!
//! Spec: `cache-eviction-at-scale` § 16.6.2 (the compensating instrument) and § 16.6.7
//! (the shared, fleet-scoped signal).
//!
//! # Why this exists, and why it is a hard requirement rather than observability polish
//!
//! § 16.6.2 puts in-progress multipart-part bytes (`P`) inside the write-tier figure the
//! eviction trigger is measured against. That deliberately overrides § 3.7's scope guard,
//! whose reasoning — "visible-and-unevictable is worse than invisible, because eviction
//! cannot free a staged part" — is sound. The override is safe **only** with the
//! compensating instrument § 16.6.2 names: the eviction executor must publish
//! `unevictable_in_progress_bytes` as a **named component of `shortfall_bytes`**, so a
//! trigger that fires and frees nothing is diagnosable rather than mysterious. Without it
//! § 3.7's objection stands.
//!
//! # Why it is persisted rather than kept in memory
//!
//! The existing eviction counters are process-local while **any** proxy may win the
//! fleet-wide eviction lock, so one proxy's `/metrics` can show no activity while another
//! is evicting shared data (§ 16.6.7). A local counter cannot answer a
//! fleet question. This document is written by whichever instance held the lock, to the
//! shared volume, so every instance reports the same last pass.
//!
//! Before this, the pass's figures reached exactly one `info!` line and nothing else —
//! `journal_consolidator.rs` records that putting them on `/metrics` was open work.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Directory under `metadata/` holding shard state, per design § 3.2.
const SHARD_STATE_DIR: &str = "_shard_state";

/// Filename of the shared last-eviction-result document.
const EVICTION_LAST_RESULT: &str = "eviction_last_result.json";

/// Schema version of the document.
const SCHEMA: u32 = 1;

/// One read-tier eviction pass's outcome, with its shortfall decomposed.
///
/// # The decomposition is an identity, not two adjacent gauges
///
/// ```text
///   shortfall_bytes  ==  unevictable_in_progress_bytes  +  unexplained_shortfall_bytes
/// ```
///
/// That is what "a named component of `shortfall_bytes`" means here, and it is asserted
/// rather than described (`the_shortfall_decomposition_is_an_identity`). A pass that asked
/// for bytes and freed none is then readable in one line: if the shortfall is entirely the
/// in-progress component, eviction did everything it could and the trigger fired on bytes
/// no evictor can reclaim; if it is entirely unexplained, the candidate set was exhausted
/// or the algorithm is not finding what it should. Publishing the total alone cannot
/// distinguish those, which is exactly § 16.6.2's objection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionPassResult {
    /// Schema version, so a future layout change is rejected rather than misread.
    pub schema: u32,
    /// The instance that ran the pass — it held the fleet-wide eviction lock.
    pub instance_id: String,
    /// Seconds since the epoch at which the pass finished.
    pub completed_at: u64,
    /// `current_size − target_size`: what the pass set out to free.
    pub bytes_requested: u64,
    /// What it actually freed.
    pub bytes_freed: u64,
    /// `bytes_requested − bytes_freed`, saturating. `0` means the target was reached.
    pub shortfall_bytes: u64,
    /// **The named component of `shortfall_bytes`** attributable to multipart-upload parts
    /// staged under `mpus_in_progress/` — § 16.6.2's `P`. These bytes are inside the figure
    /// the trigger is measured against and cannot be freed by eviction at any candidate
    /// count, because eviction's candidates are published ranges.
    ///
    /// `min(P, shortfall_bytes)`, so it is genuinely a part of the total rather than an
    /// unrelated figure printed beside it. The raw, unclamped `P` is published separately
    /// as `cache.in_progress_part_bytes`, so nothing is lost by the clamp.
    ///
    /// `None` is the UNREADABLE sentinel, inherited from `in_progress_part_bytes` — a
    /// failed `mpus_in_progress/` traversal is not the same observation as "no upload is in
    /// flight", and `0` cannot say which it was. When it is `None` the decomposition is
    /// unavailable and `unexplained_shortfall_bytes` is `None` too, rather than silently
    /// absorbing the whole shortfall.
    pub unevictable_in_progress_bytes: Option<u64>,
    /// The rest of the shortfall: bytes eviction could in principle have freed and did
    /// not. `None` when the in-progress component is unreadable, because a residual
    /// computed against an unknown term is not a residual.
    pub unexplained_shortfall_bytes: Option<u64>,
    /// Whether the pass reached its target.
    pub reached_target: bool,
    /// Ranges evicted, for context on whether the pass did any work at all.
    pub ranges_evicted: u64,
    /// How long the pass was **actively running**, in milliseconds: from the point it had a
    /// non-zero byte target through to this document being built. R12.1's pass duration.
    ///
    /// # It is the ACTIVE interval, and that is the whole reason it exists
    ///
    /// R12.3 wants an eviction rate that is verifiable in production. Design § 8 specifies an
    /// average over the whole window and then corrects itself in the same section: a measured
    /// pass ran active for 78.8 of 240 s and sat idle the rest, which is correct behaviour, so a
    /// rate divided by wall-clock-since-start reads that idle time as falling behind and would
    /// report a cache that is keeping pace comfortably as failing. The denominator therefore has
    /// to be summed **active** intervals, and this is that interval for one pass.
    ///
    /// **Not `eviction_coordination.total_lock_hold_time_ms`.** That is cumulative across passes,
    /// it includes lock acquisition and the post-lock re-check that returns without evicting, and
    /// it keeps counting for a pass that found nothing to free. Dividing `bytes_freed` by it would
    /// mix a per-pass numerator with a fleet-lifetime denominator.
    ///
    /// **It includes pre-eviction journal consolidation** when that was not skipped, because that
    /// consolidation is work this pass does before it can select candidates. A consumer computing
    /// a rate is therefore measuring the pass's cost, not the executor's inner loop in isolation.
    ///
    /// `None` means **not measured**, not "instant": it is what a document written by a build
    /// predating this field deserialises to. A rate consumer MUST skip such a sample rather than
    /// treat it as a zero denominator. New documents always carry a value.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 12.1, 12.3.
    #[serde(default)]
    pub duration_ms: Option<u64>,
    /// Which pass wrote the document. `legacy`: the lock holder selected and deleted. `authored`:
    /// the lock holder wrote a work list for every proxy's executors and deleted nothing, so
    /// `bytes_freed` and `ranges_evicted` are what it **listed** toward `bytes_requested`, and
    /// the bytes leave the disk as executors run.
    ///
    /// A document from a build without the field reads as `legacy`, which is what it was.
    #[serde(default)]
    pub mode: EvictionMode,
}

/// The pass that wrote an [`EvictionPassResult`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionMode {
    /// The single-proxy pass: the lock holder selected and deleted.
    #[default]
    Legacy,
    /// The DR-EVICT author: the lock holder listed work for executors and deleted nothing.
    Authored,
}

impl EvictionPassResult {
    /// The DR-EVICT author's result: `shortfall` requested, `listed_bytes` over `items_listed`
    /// extents listed for executors, nothing deleted by the author itself.
    pub fn authored(
        instance_id: String,
        shortfall: u64,
        listed_bytes: u64,
        items_listed: u64,
        in_progress_part_bytes: Option<u64>,
        active_duration: Option<Duration>,
    ) -> Self {
        Self {
            mode: EvictionMode::Authored,
            ..Self::new(
                instance_id,
                shortfall,
                listed_bytes,
                items_listed,
                in_progress_part_bytes,
                active_duration,
            )
        }
    }

    /// Build a result, computing the shortfall and its decomposition.
    ///
    /// `in_progress_part_bytes` is `P` as read from `mpus_in_progress/`, with `None`
    /// meaning unreadable.
    ///
    /// `active_duration` is how long the pass ran — see [`Self::duration_ms`] for what
    /// "active" means and why `None` is a distinct reading from zero.
    pub fn new(
        instance_id: String,
        bytes_requested: u64,
        bytes_freed: u64,
        ranges_evicted: u64,
        in_progress_part_bytes: Option<u64>,
        active_duration: Option<Duration>,
    ) -> Self {
        let shortfall_bytes = bytes_requested.saturating_sub(bytes_freed);
        let unevictable_in_progress_bytes = in_progress_part_bytes.map(|p| p.min(shortfall_bytes));
        let unexplained_shortfall_bytes =
            unevictable_in_progress_bytes.map(|c| shortfall_bytes.saturating_sub(c));
        Self {
            schema: SCHEMA,
            instance_id,
            completed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            bytes_requested,
            bytes_freed,
            shortfall_bytes,
            unevictable_in_progress_bytes,
            unexplained_shortfall_bytes,
            reached_target: shortfall_bytes == 0,
            ranges_evicted,
            duration_ms: active_duration.map(|d| d.as_millis() as u64),
            mode: EvictionMode::Legacy,
        }
    }

    /// `{cache_dir}/metadata/_shard_state/eviction_last_result.json`.
    pub fn path(cache_dir: &Path) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(SHARD_STATE_DIR)
            .join(EVICTION_LAST_RESULT)
    }

    /// Install atomically: temp file, `sync_all`, rename — the same shape the staged fold's
    /// own per-shard checkpoint install uses, with the same per-instance temp suffix so two
    /// instances cannot collide on one temp path.
    ///
    /// (Deliberately described rather than named: `staged_fold`'s
    /// `no_decision_path_reads_the_fold` asserts by string match over every file in `src/`
    /// that nothing outside the fold's producer and publisher names its types, and that
    /// test is worth more than a cross-reference. It is a real guard against the behaviour
    /// flip happening by accident, so it must not be relaxed to accommodate a comment.)
    pub fn install(&self, cache_dir: &Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::other(format!("serialize eviction result: {e}")))?;
        let tmp = path.with_extension(format!(
            "json.tmp.{}.{}",
            gethostname::gethostname()
                .to_string_lossy()
                .replace(['/', '\\'], "_"),
            std::process::id()
        ));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }

    /// Read the installed document, or `None` if no pass has completed on this volume.
    ///
    /// A read or parse failure is `None` with a WARN rather than an error: this is an
    /// observability document, and a `/metrics` scrape must not fail because of it.
    pub fn load(cache_dir: &Path) -> Option<Self> {
        let path = Self::path(cache_dir);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                debug!(
                    "Could not read {}: {}. Reporting no last eviction result.",
                    path.display(),
                    e
                );
                return None;
            }
        };
        match serde_json::from_str::<Self>(&content) {
            Ok(parsed) if parsed.schema == SCHEMA => Some(parsed),
            Ok(parsed) => {
                warn!(
                    "Unknown eviction result schema {} in {}; ignoring",
                    parsed.schema,
                    path.display()
                );
                None
            }
            Err(e) => {
                warn!("Unparseable eviction result in {}: {}", path.display(), e);
                None
            }
        }
    }

    /// A one-line summary naming the shortfall and its components, for the pass's own log.
    pub fn summary(&self) -> String {
        format!(
            "bytes_requested={} bytes_freed={} shortfall_bytes={} \
             unevictable_in_progress_bytes={} unexplained_shortfall_bytes={} \
             reached_target={} ranges_evicted={} duration_ms={}",
            self.bytes_requested,
            self.bytes_freed,
            self.shortfall_bytes,
            self.unevictable_in_progress_bytes
                .map_or_else(|| "UNREADABLE".to_string(), |v| v.to_string()),
            self.unexplained_shortfall_bytes
                .map_or_else(|| "UNREADABLE".to_string(), |v| v.to_string()),
            self.reached_target,
            self.ranges_evicted,
            self.duration_ms
                .map_or_else(|| "UNMEASURED".to_string(), |v| v.to_string()),
        )
    }
}

/// Filename of the shared last-**count**-eviction-result document.
const WRITE_CACHE_COUNT_LAST_RESULT: &str = "write_cache_count_eviction_last_result.json";

/// `shortfall_reason`: the target was met, and this is not a repeat reclaim.
pub const COUNT_REASON_TARGET_REACHED: &str = "target_reached";
/// `shortfall_reason`: the ordered candidate list held nothing that assessed `Evictable`.
/// State (a) — informational, the decline backstop is working as designed.
pub const COUNT_REASON_NO_RECLAIMABLE_CANDIDATES: &str = "no_reclaimable_candidates";
/// `shortfall_reason`: this pass reclaimed, and so did an earlier one, yet the count was back
/// at the trigger. State (b) — actionable: the hysteresis gap or the budget is wrong.
pub const COUNT_REASON_RECLAIMED_AND_REFILLED: &str = "reclaimed_and_refilled";
/// `shortfall_reason`: candidates were reclaimable and the list ran out before the target.
pub const COUNT_REASON_CANDIDATES_EXHAUSTED: &str = "candidates_exhausted";
/// `shortfall_reason`: the shortfall is wholly attributable to candidates whose `.meta` could
/// not be read, which this pass refuses to act on.
pub const COUNT_REASON_UNREADABLE_CANDIDATE_METADATA: &str = "unreadable_candidate_metadata";

/// One **count**-driven write-cache eviction pass's outcome, with its shortfall decomposed.
///
/// The count analogue of [`EvictionPassResult`], and for the same reason § 16.6.2 gives for
/// that one: without it the retained decline backstop is indistinguishable from a silent cap.
///
/// # The decomposition is an identity, exactly as the byte path's is
///
/// ```text
///   shortfall_ranges  ==  unevictable_unreadable_ranges  +  unexplained_shortfall_ranges
/// ```
///
/// The named component is the count analogue of `unevictable_in_progress_bytes`: ranges the
/// ledger names for candidates whose authoritative `.meta` could not be read. Those are inside
/// the figure the count trigger is measured against and this pass cannot reclaim them at any
/// candidate count, because it deliberately refuses to evict on a failed authority read — and
/// unlike a fresh-unread candidate, walking further down the list does not help.
///
/// **There is no `None` arm and that is not an omission.** `EvictionPassResult` needs one
/// because `P` itself can be unreadable; here every input is known by construction, because a
/// pass whose ledger read was incomplete aborts before evicting anything and so never publishes.
///
/// # Distinguishing the three states § 16.6.12 point 5 requires, from these figures alone
///
/// | State | Reading |
/// |---|---|
/// | (c) the trigger never fired | this document is **absent**, or `passes_total` is flat across reads while `admission_declines_total` climbs |
/// | (a) nothing was reclaimable because nothing had expired | `ranges_freed == 0`, `reclaimable_candidates == 0`, `expired_candidates == 0`, `shortfall_reason == "no_reclaimable_candidates"` |
/// | (b) reclaimed and immediately re-filled | `ranges_freed > 0` **and** `ranges_freed_total > ranges_freed` **and** `passes_total > 1`, i.e. `shortfall_reason == "reclaimed_and_refilled"` |
///
/// `admission_declines_total` is in the document for exactly one purpose: it is what ties the
/// two mechanisms together, so (c) is readable rather than inferred from an absence. Declines
/// climbing while passes do not is the wiring/threshold/read defect the design names.
///
/// Spec: cache-eviction-at-scale § 16.6.12 point 5.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteCacheCountPassResult {
    /// Schema version, so a future layout change is rejected rather than misread.
    pub schema: u32,
    /// The instance that ran the pass — it held the fleet-wide eviction lock.
    pub instance_id: String,
    /// Seconds since the epoch at which the pass finished.
    pub completed_at: u64,
    /// `STAGED_RANGE_BUDGET`, read from its constant at runtime. Published so the trigger and
    /// target below are checkable against the budget rather than taken on trust.
    pub count_budget: u64,
    /// `eviction_trigger_percent` of the budget.
    pub count_trigger: u64,
    /// `eviction_target_percent` of the budget.
    pub count_target: u64,
    /// The count the trigger fired on, after debiting this process's not-yet-folded reclaims.
    pub count_at_start: u64,
    /// The count the raw inputs reported, before that debit. Equal to `count_at_start` on the
    /// first pass after a fold install.
    pub observed_count_at_start: u64,
    /// `count_at_start − count_target`: what the pass set out to reclaim.
    pub ranges_requested: u64,
    /// What it actually reclaimed, summed from the `.meta` assessments it acted on.
    pub ranges_freed: u64,
    /// `ranges_requested − ranges_freed`, saturating. `0` means the target was reached.
    pub shortfall_ranges: u64,
    /// **The named component of `shortfall_ranges`** — see the type doc. `min(..,
    /// shortfall_ranges)`, so it is genuinely a part of the total rather than a figure printed
    /// beside it.
    pub unevictable_unreadable_ranges: u64,
    /// The rest of the shortfall: ranges eviction could in principle have reclaimed and did not.
    pub unexplained_shortfall_ranges: u64,
    /// Whether the pass reached its target.
    pub reached_target: bool,
    /// Objects evicted, for context on whether the pass did any work at all.
    pub objects_evicted: u64,
    /// Candidates the pass walked.
    pub candidates_considered: u64,
    /// Of those, how many assessed `Evictable`. **Zero is state (a)'s discriminator**: it says
    /// the population was genuinely empty of reclaimable entries, not that the pass failed to
    /// act on them.
    pub reclaimable_candidates: u64,
    /// Of those, how many were ahead of R2.4's `now − put_ttl` watermark, i.e. expired-unread.
    /// Zero alongside `reclaimable_candidates == 0` is "nothing had expired".
    pub expired_candidates: u64,
    /// Count-driven passes this instance has completed since process start.
    pub passes_total: u64,
    /// Ranges those passes reclaimed.
    pub ranges_freed_total: u64,
    /// `staged_range_budget_declines_total` on this instance at pass time. See the type doc.
    pub admission_declines_total: u64,
    /// One of the `COUNT_REASON_*` constants.
    pub shortfall_reason: String,
}

impl WriteCacheCountPassResult {
    /// Build a result, computing the shortfall, its decomposition, and its reason.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_id: String,
        count_budget: u64,
        count_trigger: u64,
        count_target: u64,
        count_at_start: u64,
        observed_count_at_start: u64,
        ranges_requested: u64,
        ranges_freed: u64,
        unreadable_candidate_ranges: u64,
        objects_evicted: u64,
        candidates_considered: u64,
        reclaimable_candidates: u64,
        expired_candidates: u64,
        passes_total: u64,
        ranges_freed_total: u64,
        admission_declines_total: u64,
    ) -> Self {
        let shortfall_ranges = ranges_requested.saturating_sub(ranges_freed);
        let unevictable_unreadable_ranges = unreadable_candidate_ranges.min(shortfall_ranges);
        let unexplained_shortfall_ranges =
            shortfall_ranges.saturating_sub(unevictable_unreadable_ranges);
        // Refill is checked FIRST and independently of the shortfall, because a pass that met
        // its target and is the second one to reclaim is the (b) signature — the target being
        // met is what makes the re-trigger informative rather than reassuring.
        let shortfall_reason =
            if ranges_freed > 0 && passes_total > 1 && ranges_freed_total > ranges_freed {
                COUNT_REASON_RECLAIMED_AND_REFILLED
            } else if shortfall_ranges == 0 {
                COUNT_REASON_TARGET_REACHED
            } else if reclaimable_candidates == 0 {
                COUNT_REASON_NO_RECLAIMABLE_CANDIDATES
            } else if unevictable_unreadable_ranges >= shortfall_ranges {
                COUNT_REASON_UNREADABLE_CANDIDATE_METADATA
            } else {
                COUNT_REASON_CANDIDATES_EXHAUSTED
            };
        Self {
            schema: SCHEMA,
            instance_id,
            completed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            count_budget,
            count_trigger,
            count_target,
            count_at_start,
            observed_count_at_start,
            ranges_requested,
            ranges_freed,
            shortfall_ranges,
            unevictable_unreadable_ranges,
            unexplained_shortfall_ranges,
            reached_target: shortfall_ranges == 0,
            objects_evicted,
            candidates_considered,
            reclaimable_candidates,
            expired_candidates,
            passes_total,
            ranges_freed_total,
            admission_declines_total,
            shortfall_reason: shortfall_reason.to_string(),
        }
    }

    /// `{cache_dir}/metadata/_shard_state/write_cache_count_eviction_last_result.json`.
    ///
    /// A separate document from [`EvictionPassResult::path`] rather than extra fields on it:
    /// the two passes have different triggers, different targets and different units, and one
    /// document would make the last writer's units the only readable ones.
    pub fn path(cache_dir: &Path) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(SHARD_STATE_DIR)
            .join(WRITE_CACHE_COUNT_LAST_RESULT)
    }

    /// Install atomically: temp file, `sync_all`, rename — same shape and same per-instance
    /// temp suffix as [`EvictionPassResult::install`].
    pub fn install(&self, cache_dir: &Path) -> std::io::Result<()> {
        use std::io::Write;
        let path = Self::path(cache_dir);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(self).map_err(|e| {
            std::io::Error::other(format!("serialize write-cache count eviction result: {e}"))
        })?;
        let tmp = path.with_extension(format!(
            "json.tmp.{}.{}",
            gethostname::gethostname()
                .to_string_lossy()
                .replace(['/', '\\'], "_"),
            std::process::id()
        ));
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)
    }

    /// Read the installed document, or `None` if no count-driven pass has completed here.
    ///
    /// **Absence is a reading, not an error** — it is state (c)'s primary signal.
    pub fn load(cache_dir: &Path) -> Option<Self> {
        let path = Self::path(cache_dir);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                debug!(
                    "Could not read {}: {}. Reporting no last count-eviction result.",
                    path.display(),
                    e
                );
                return None;
            }
        };
        match serde_json::from_str::<Self>(&content) {
            Ok(parsed) if parsed.schema == SCHEMA => Some(parsed),
            Ok(parsed) => {
                warn!(
                    "Unknown write-cache count eviction result schema {} in {}; ignoring",
                    parsed.schema,
                    path.display()
                );
                None
            }
            Err(e) => {
                warn!(
                    "Unparseable write-cache count eviction result in {}: {}",
                    path.display(),
                    e
                );
                None
            }
        }
    }

    /// A one-line summary naming the shortfall, its component and its reason.
    pub fn summary(&self) -> String {
        format!(
            "count_budget={} count_trigger={} count_target={} count_at_start={} \
             observed_count_at_start={} ranges_requested={} ranges_freed={} \
             shortfall_ranges={} unevictable_unreadable_ranges={} \
             unexplained_shortfall_ranges={} reached_target={} objects_evicted={} \
             candidates_considered={} reclaimable_candidates={} expired_candidates={} \
             passes_total={} ranges_freed_total={} admission_declines_total={} reason={}",
            self.count_budget,
            self.count_trigger,
            self.count_target,
            self.count_at_start,
            self.observed_count_at_start,
            self.ranges_requested,
            self.ranges_freed,
            self.shortfall_ranges,
            self.unevictable_unreadable_ranges,
            self.unexplained_shortfall_ranges,
            self.reached_target,
            self.objects_evicted,
            self.candidates_considered,
            self.reclaimable_candidates,
            self.expired_candidates,
            self.passes_total,
            self.ranges_freed_total,
            self.admission_declines_total,
            self.shortfall_reason,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// **The identity § 16.6.2 requires**: the in-progress component and the residual sum
    /// to the total, so the in-progress figure is a *component of* the shortfall rather
    /// than a gauge printed next to it.
    ///
    /// Implicates `EvictionPassResult::new`'s three assignments. Asserted as an identity
    /// over several shapes rather than on one hand-picked case, because a single case with
    /// `P == shortfall` is satisfied by code that sets the component to the whole
    /// shortfall and the residual to zero unconditionally.
    /// # The identity ALONE would not catch a wrong split, so the expected values are named
    ///
    /// An implementation that set the component to the whole shortfall and the residual to
    /// zero satisfies `component + residual == shortfall` in every case. So each case below
    /// carries the **expected component and residual explicitly**, written out rather than
    /// re-derived from `min(P, shortfall)` — a re-derivation would just restate the
    /// implementation and pass against any version of it. The `(100, 0, Some(40))` row is
    /// the one that discriminates: only a real split gives `40 / 60`.
    #[test]
    fn the_shortfall_decomposition_is_an_identity() {
        // (requested, freed, P, expected component, expected residual)
        let cases = [
            // The shortfall is entirely bytes eviction could never free.
            (100u64, 0u64, Some(100u64), 100u64, 0u64),
            // THE DISCRIMINATING ROW: partly unevictable, partly genuinely unexplained.
            (100, 0, Some(40), 40, 60),
            // No staged parts at all, so the whole shortfall is unexplained.
            (100, 0, Some(0), 0, 100),
            // P larger than the shortfall: the component is clamped to the total it is a
            // component of, and the raw figure stays available as
            // `cache.in_progress_part_bytes`.
            (100, 0, Some(500), 100, 0),
            // Target reached: nothing to decompose.
            (100, 100, Some(70), 0, 0),
            // Freed more than asked — saturates rather than underflowing.
            (100, 250, Some(70), 0, 0),
            (0, 0, Some(0), 0, 0),
        ];
        for (requested, freed, p, want_component, want_residual) in cases {
            let r = EvictionPassResult::new("host:1".into(), requested, freed, 3, p, None);
            let component = r.unevictable_in_progress_bytes.unwrap();
            let residual = r.unexplained_shortfall_bytes.unwrap();
            assert_eq!(
                (component, residual),
                (want_component, want_residual),
                "wrong split for requested={requested} freed={freed} p={p:?}"
            );
            assert_eq!(
                component + residual,
                r.shortfall_bytes,
                "components must sum to the shortfall for requested={requested} \
                 freed={freed} p={p:?}"
            );
            assert_eq!(r.shortfall_bytes, requested.saturating_sub(freed));
            assert!(
                component <= r.shortfall_bytes,
                "the component must never exceed the total it is a component of"
            );
            assert_eq!(r.reached_target, r.shortfall_bytes == 0);
        }
    }

    /// **The case § 16.6.2 exists for**: a pass that fires and frees nothing, where the
    /// whole shortfall is bytes eviction could never have reclaimed.
    ///
    /// Without the component both this and a genuine candidate-exhaustion failure publish
    /// the identical `shortfall_bytes=8388608, bytes_freed=0`. The assertion is on the
    /// component being the *whole* shortfall, which is the statement an operator acts on.
    #[test]
    fn a_pass_that_frees_nothing_is_attributable_to_in_progress_parts() {
        let r = EvictionPassResult::new("host:1".into(), 8_388_608, 0, 0, Some(8_388_608), None);
        assert_eq!(r.shortfall_bytes, 8_388_608);
        assert_eq!(r.unevictable_in_progress_bytes, Some(8_388_608));
        assert_eq!(
            r.unexplained_shortfall_bytes,
            Some(0),
            "nothing is left unexplained: every missing byte is a staged part"
        );
        assert!(!r.reached_target);
        assert!(r
            .summary()
            .contains("unevictable_in_progress_bytes=8388608"));
    }

    /// An unreadable `P` makes the decomposition unavailable rather than attributing the
    /// whole shortfall to one side.
    ///
    /// Implicates the `Option::map` chain in `new`. A `0` here would say "none of the
    /// shortfall is staged parts", which is a confident claim made on no evidence — the
    /// same collapse `in_progress_part_bytes` added its sentinel to avoid.
    #[test]
    fn an_unreadable_in_progress_figure_leaves_the_decomposition_unavailable() {
        let r = EvictionPassResult::new("host:1".into(), 4096, 0, 1, None, None);
        assert_eq!(r.shortfall_bytes, 4096);
        assert_eq!(r.unevictable_in_progress_bytes, None);
        assert_eq!(r.unexplained_shortfall_bytes, None);
        assert!(r
            .summary()
            .contains("unevictable_in_progress_bytes=UNREADABLE"));
    }

    /// DR-EVICT task 45/49: a document written before `mode` existed reads as `legacy`; the
    /// author's result says `authored` on disk and keeps the shortfall identity.
    #[test]
    fn a_document_without_mode_reads_as_legacy_and_the_author_writes_authored() {
        let legacy = EvictionPassResult::new("i".into(), 10, 4, 1, Some(0), None);
        let mut json = serde_json::to_value(&legacy).unwrap();
        assert_eq!(json["mode"], "legacy");
        json.as_object_mut().unwrap().remove("mode");
        let parsed: EvictionPassResult = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.mode, EvictionMode::Legacy);

        let tmp = TempDir::new().unwrap();
        let authored = EvictionPassResult::authored("i".into(), 100, 96, 24, Some(0), None);
        authored.install(tmp.path()).unwrap();
        let on_disk: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(EvictionPassResult::path(tmp.path())).unwrap(),
        )
        .unwrap();
        assert_eq!(on_disk["mode"], "authored");
        let loaded = EvictionPassResult::load(tmp.path()).unwrap();
        assert_eq!(loaded.mode, EvictionMode::Authored);
        assert_eq!(
            loaded.shortfall_bytes,
            loaded.unevictable_in_progress_bytes.unwrap()
                + loaded.unexplained_shortfall_bytes.unwrap()
        );
        assert_eq!((loaded.bytes_requested, loaded.bytes_freed), (100, 96));
    }

    /// Install then load round-trips, and an absent document is `None` rather than a
    /// zeroed result.
    #[test]
    fn install_and_load_round_trip() {
        let tmp = TempDir::new().unwrap();
        assert!(EvictionPassResult::load(tmp.path()).is_none());
        let r = EvictionPassResult::new(
            "host:7".into(),
            500,
            200,
            9,
            Some(120),
            Some(Duration::from_millis(1234)),
        );
        r.install(tmp.path()).unwrap();
        assert_eq!(EvictionPassResult::load(tmp.path()), Some(r));
    }

    /// An unknown schema is ignored rather than misread as the current layout.
    #[test]
    fn an_unknown_schema_is_ignored() {
        let tmp = TempDir::new().unwrap();
        let path = EvictionPassResult::path(tmp.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            br#"{"schema":99,"instance_id":"h:1","completed_at":0,"bytes_requested":0,
                 "bytes_freed":0,"shortfall_bytes":0,"unevictable_in_progress_bytes":null,
                 "unexplained_shortfall_bytes":null,"reached_target":true,"ranges_evicted":0}"#,
        )
        .unwrap();
        assert!(EvictionPassResult::load(tmp.path()).is_none());
    }

    // =====================================================================
    // § 16.6.12 point 5 — the COUNT pass's result document
    // =====================================================================

    /// Build a count result with sensible defaults, overriding only what a case cares about.
    #[allow(clippy::too_many_arguments)]
    fn count_result(
        requested: u64,
        freed: u64,
        unreadable_ranges: u64,
        reclaimable_candidates: u64,
        expired_candidates: u64,
        passes_total: u64,
        ranges_freed_total: u64,
    ) -> WriteCacheCountPassResult {
        WriteCacheCountPassResult::new(
            "host:1".into(),
            1_300_000,
            1_235_000,
            1_040_000,
            1_240_000,
            1_240_000,
            requested,
            freed,
            unreadable_ranges,
            freed / 4,
            42,
            reclaimable_candidates,
            expired_candidates,
            passes_total,
            ranges_freed_total,
            7,
        )
    }

    /// **The identity § 16.6.12 point 5 requires**, in the count's units: the named component
    /// and the residual sum to the total, so the unreadable-metadata figure is a *component of*
    /// the shortfall rather than a gauge printed beside it.
    ///
    /// # The identity alone would not catch a wrong split, so the values are named
    ///
    /// An implementation that set the component to the whole shortfall and the residual to zero
    /// satisfies `component + residual == shortfall` in every case. So each row carries the
    /// expected component and residual **written out**, not re-derived from `min(..)` — a
    /// re-derivation would restate the implementation and pass against any version of it. The
    /// `(100, 0, 40)` row is the discriminating one: only a real split gives `40 / 60`.
    ///
    /// A failure implicates the three assignments in `WriteCacheCountPassResult::new`
    /// (`eviction_result.rs`).
    #[test]
    fn the_count_shortfall_decomposition_is_an_identity() {
        // (requested, freed, unreadable_ranges, expected component, expected residual)
        let cases = [
            // The shortfall is entirely ranges whose `.meta` could not be read.
            (100u64, 0u64, 100u64, 100u64, 0u64),
            // THE DISCRIMINATING ROW.
            (100, 0, 40, 40, 60),
            // Nothing unreadable, so the whole shortfall is unexplained.
            (100, 0, 0, 0, 100),
            // More unreadable ranges than the shortfall: clamped to the total it is part of.
            (100, 0, 500, 100, 0),
            // Target reached: nothing to decompose.
            (100, 100, 70, 0, 0),
            // Reclaimed more than asked — saturates rather than underflowing.
            (100, 250, 70, 0, 0),
            (0, 0, 0, 0, 0),
        ];
        for (requested, freed, unreadable, want_component, want_residual) in cases {
            let r = count_result(requested, freed, unreadable, 5, 5, 1, freed);
            assert_eq!(
                (
                    r.unevictable_unreadable_ranges,
                    r.unexplained_shortfall_ranges
                ),
                (want_component, want_residual),
                "requested={requested} freed={freed} unreadable={unreadable}"
            );
            assert_eq!(
                r.unevictable_unreadable_ranges + r.unexplained_shortfall_ranges,
                r.shortfall_ranges,
                "and the identity holds: requested={requested} freed={freed}"
            );
            assert_eq!(r.reached_target, r.shortfall_ranges == 0);
        }
    }

    /// The three states § 16.6.12 point 5 names are distinguishable **from the published
    /// figures alone**.
    ///
    /// Without this the retained decline backstop is indistinguishable from a silent cap, which
    /// is the design's own objection. Each state is asserted on the figures a reader actually
    /// has, not on a description of them.
    ///
    /// A failure implicates the `shortfall_reason` chain in `WriteCacheCountPassResult::new`,
    /// or `load` returning something other than `None` for an absent document — both in
    /// `eviction_result.rs`.
    #[test]
    fn the_three_backstop_states_are_distinguishable_from_the_published_figures() {
        let tmp = TempDir::new().unwrap();

        // ---- (c) the count trigger never fired at all.
        //
        // Primary signal: no document. This is asserted BEFORE anything is installed, so it is
        // the genuine never-fired state rather than a simulation of one.
        assert!(
            WriteCacheCountPassResult::load(tmp.path()).is_none(),
            "(c): an absent document is the never-fired reading, and must not be reported as \
             a zeroed pass — a zeroed pass says the trigger fired and found nothing, which is \
             state (a) and has a different remedy"
        );

        // ---- (a) nothing was reclaimable because nothing had expired.
        let a = count_result(195_000, 0, 0, 0, 0, 1, 0);
        assert_eq!(a.ranges_freed, 0);
        assert_eq!(a.reclaimable_candidates, 0);
        assert_eq!(a.expired_candidates, 0);
        assert_eq!(
            a.shortfall_reason, COUNT_REASON_NO_RECLAIMABLE_CANDIDATES,
            "(a): informational — the backstop is working, and the figures say WHY nothing \
             was freed rather than only that nothing was"
        );

        // ---- (b) reclaimed and immediately re-filled.
        //
        // This pass met its target, and an earlier one also reclaimed. A count back at the
        // trigger after a successful reclaim is the actionable case: the gap or the budget is
        // wrong. `ranges_freed_total > ranges_freed` is what says "not the first reclaim".
        let b = count_result(195_000, 195_000, 0, 900, 900, 4, 700_000);
        assert!(b.reached_target);
        assert!(b.ranges_freed > 0);
        assert!(b.ranges_freed_total > b.ranges_freed);
        assert!(b.passes_total > 1);
        assert_eq!(
            b.shortfall_reason, COUNT_REASON_RECLAIMED_AND_REFILLED,
            "(b): actionable — reclamation demonstrably works and the count came straight \
             back, so the hysteresis gap or the budget is wrong"
        );

        // ---- And the three reasons are genuinely different strings, or none of the above
        // discriminates anything.
        let first_success = count_result(195_000, 195_000, 0, 900, 900, 1, 195_000);
        assert_eq!(first_success.shortfall_reason, COUNT_REASON_TARGET_REACHED);
        let all_three = [
            a.shortfall_reason.clone(),
            b.shortfall_reason.clone(),
            first_success.shortfall_reason.clone(),
        ];
        assert_eq!(
            all_three
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3,
            "the three readings must be three distinct values: {all_three:?}"
        );

        // ---- (c)'s secondary signal, for a volume that HAS published once: declines climbing
        // while passes do not. Both figures are in the document, which is the point of
        // publishing `admission_declines_total` at all.
        b.install(tmp.path()).unwrap();
        let loaded = WriteCacheCountPassResult::load(tmp.path()).expect("round-trips");
        assert_eq!(loaded, b);
        assert_eq!(
            loaded.admission_declines_total, 7,
            "(c): the decline counter must be IN the document, so 'declines climbing while \
             passes_total is flat' is readable from this file alone rather than requiring a \
             second source"
        );
        assert_eq!(
            loaded.count_budget, 1_300_000,
            "and the budget is published as data, so the trigger and target are checkable \
             against it rather than taken on trust"
        );
    }

    /// An unknown schema is ignored rather than misread as the current layout.
    #[test]
    fn an_unknown_count_schema_is_ignored() {
        let tmp = TempDir::new().unwrap();
        let path = WriteCacheCountPassResult::path(tmp.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, br#"{"schema":99}"#).unwrap();
        assert!(WriteCacheCountPassResult::load(tmp.path()).is_none());
    }
}
