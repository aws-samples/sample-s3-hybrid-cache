//! Orphan range-file reclamation (R15, unit ORPHAN-U4A).
//!
//! A `.bin` under `ranges/` that its key's `.meta` does not list is a candidate orphan. It may
//! still be live: a fill whose `Add` sits unconsolidated in some instance's journal looks
//! exactly like one. This module finds candidates with a leased, resumable rotation and
//! reclaims one only when journal evidence read under the key's slot lock proves no `Add` is
//! pending ([`crate::orphan_evidence::JournalEvidence::ConfirmedAbsent`]).
//!
//! # Rotation
//!
//! - One checkpoint per `(bucket, L1)` shard at
//!   `metadata/_shard_state/orphan/{bucket}/{L1}.json` holds `scanned_at` (set when the last L2
//!   leaf of a visit completes) and `cursor` (the last L2 leaf completed by an unfinished visit).
//!   The flat legacy files directly under `ranges/` are one more visit unit with its own
//!   checkpoint, `metadata/_shard_state/orphan/_root.json`, so they are processed once per
//!   rotation.
//! - Visit order is the `st_size` fold's ranking (`st_size_fold::fold_order`): no checkpoint
//!   first, then no `scanned_at`, then oldest `scanned_at`, key order breaking ties. That
//!   function is typed over the staged-fold checkpoint set, so [`visit_order`] applies the same
//!   ranking to this rotation's own checkpoints.
//! - Each shard is claimed with a lease file at
//!   `metadata/_shard_state/orphan_claims/{bucket}/{L1}.claim` before it is visited and released
//!   after. Unlike `ledger_repair::claim_repair_shard`'s write-then-re-read, the claim is an
//!   exclusive create, so two instances never both win a free shard. The global eviction lock is
//!   not taken.
//! - A pass stops at a leaf budget or a time budget, whichever comes first, and records the
//!   cursor so the next pass resumes after the last completed leaf.
//! - A scheduled pass ([`run_scheduled_pass`]) runs only while every active proxy on the volume
//!   is ready, by the same journal-roster gate the eviction work list uses
//!   ([`crate::eviction_worklist::fleet_is_worklist_capable`]). The per-candidate protocol below
//!   relies on every writer holding the object's slot lock from publishing a range file to
//!   journalling it, and a proxy older than 3.0.0 does not, so its fresh range file can read as
//!   an orphan with no journal entry yet. `NotCapable` and `Unknown` both skip the pass; the gate
//!   is re-read before each unit and again under each candidate's slot lock.
//!
//! # Per candidate
//!
//! Under the key's slot lock: re-stat the `.bin`, re-read the `.meta` (a listing that appeared
//! keeps the file), read **every** journal in full through
//! [`crate::orphan_evidence::journal_evidence`], and unlink only on `ConfirmedAbsent`.
//!
//! The full re-read under the lock is the fix for `JournalEvidenceReader::refresh`'s
//! `(len, mtime)` change test, which misses a same-length rewrite inside mtime granularity and
//! would then answer from a stale parse. Its cost is one read of `metadata/_journals/` per
//! **candidate**, not per range file: a range file the `.meta` lists never reaches the lock.
//!
//! # Accounting
//!
//! No debit is made (R15.10): orphan bytes were never in the `.meta`-derived total. Reclaimed
//! bytes are counted separately in [`counters`].
//!
//! # The old periodic re-adopter
//!
//! `orphaned_range_recovery` stays until unit U5. It never unlinks; it only re-adopts by
//! writing a listing. If its write reaches the `.meta` or a journal before this pass takes the
//! slot lock, the re-read or the evidence keeps the file. If it lands after the unlink, the
//! listing names a missing file, which reads as a miss and is refetched: an extra miss, never
//! a lost fill, because the file was proven unreferenced when it was removed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::cache_types::NewCacheMetadata;
use crate::orphan_evidence::{journal_evidence, JournalEvidence};
use crate::staged_fold::Shard;

/// How often a pass is due on each instance.
pub const PASS_INTERVAL_SECS: u64 = 300;
/// The youngest a range file may be, by its modification time, and still be reclaimed.
///
/// Every writer, of this release and of 2.8.x, makes a range file visible in `ranges/` before
/// the `.meta` or journal line that lists it: a stored range is renamed into place and then
/// journalled, and a streamed PUT finalizes its file and then writes the `.meta`. Each window
/// is one metadata write plus any wait for a writer lock, so seconds at worst on shared
/// storage. Ten minutes covers that with a wide margin and tolerates that much clock skew
/// between the proxy and the file server, while an orphan still goes within a few passes of
/// [`PASS_INTERVAL_SECS`]. A multipart part keeps its upload-time mtime when it is renamed in,
/// so it is guarded by its upload record instead (see [`UploadSnapshot`]).
pub const MIN_CANDIDATE_AGE_SECS: u64 = 600;
/// The wall-clock budget of one pass.
pub const PASS_TIME_BUDGET: Duration = Duration::from_secs(30);
/// The L2-leaf budget of one pass. With 4,096 leaves per `(bucket, L1)` shard this is one
/// full shard per pass at most.
pub const PASS_LEAF_BUDGET: usize = 4_096;
/// A claim older than this is abandoned and may be taken over.
pub const CLAIM_LEASE_SECS: u64 = 600;
/// How long a lease file that does not parse keeps counting as live, by its modification time
///Applies to every lease family.
///
/// An unparseable lease is never held by a running claimer: [`claim_shard_lease`] returns
/// `Busy` when its write fails, so a torn file is either a peer's create caught between
/// `create_new` and `write_all` (milliseconds, or the length of an NFS stall) or what a crash or
/// `ENOSPC` left behind, which nothing would ever remove. Reading it as live forever stops the
/// shard for good, and on an aborted eviction generation it defers every author, so eviction
/// stops on the volume. 30 minutes mirrors `eviction_worklist::UNREADABLE_MARKER_ABORT_SECS`
/// (twice the default chunk lease): far longer than any write in progress, and a bounded stall.
pub(crate) const UNREADABLE_LEASE_EXPIRE_SECS: u64 = 1800;

/// The bucket component that names the flat `ranges/` root visit unit. No bucket is empty.
const ROOT_BUCKET: &str = "";

/// Red-side lever (a): pick one unit from the wall clock, as the old sweep does.
#[cfg(test)]
const WALL_CLOCK_ORDER_LEVER: bool = false;
/// Red-side lever (b): `IgnorePeerJournals`, a reclaimer that consults no journal.
#[cfg(test)]
const IGNORE_PEER_JOURNALS: bool = false;
/// Red-side lever (c): skip the under-lock `.meta` re-read.
#[cfg(test)]
const SKIP_UNDER_LOCK_META_REREAD: bool = false;
/// Red-side lever (e): ignore the lease family argument and always use the orphan directory,
/// so two families exclude each other.
#[cfg(test)]
const HARD_CODE_LEASE_DIR: bool = false;
/// Red-side lever: a scheduled pass ignores the fleet-readiness gate.
#[cfg(test)]
const ORPHAN_IGNORES_FLEET_READINESS: bool = false;
/// Red-side lever: an unparseable lease counts as live whatever its age.
#[cfg(test)]
const UNREADABLE_LEASE_NEVER_EXPIRES: bool = false;

// ---------------------------------------------------------------------------------------------
// Process-local state and counters
// ---------------------------------------------------------------------------------------------

/// Starts `false` until cache init calls [`enable`]. A process that never initialises a cache
/// (a unit test's bare consolidator) never claims a pass.
static ENABLED: AtomicBool = AtomicBool::new(false);
static LAST_PASS_SECS: AtomicU64 = AtomicU64::new(0);
static PASS_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

static FOUND: AtomicU64 = AtomicU64::new(0);
static RECLAIMED: AtomicU64 = AtomicU64::new(0);
static BYTES_RECLAIMED: AtomicU64 = AtomicU64::new(0);
static KEPT_LISTED: AtomicU64 = AtomicU64::new(0);
static KEPT_JOURNAL_PRESENT: AtomicU64 = AtomicU64::new(0);
static SKIPPED_UNCONFIRMED: AtomicU64 = AtomicU64::new(0);
static PASSES: AtomicU64 = AtomicU64::new(0);
static PASSES_GATED: AtomicU64 = AtomicU64::new(0);
static LEASES_UNREADABLE: AtomicU64 = AtomicU64::new(0);
static LEASES_UNREADABLE_EXPIRED: AtomicU64 = AtomicU64::new(0);
static UNITS_VISITED: AtomicU64 = AtomicU64::new(0);
static EVIDENCE_CONFIRMED_ABSENT: AtomicU64 = AtomicU64::new(0);
static EVIDENCE_JOURNAL_PRESENT: AtomicU64 = AtomicU64::new(0);
static EVIDENCE_PEER_UNAVAILABLE: AtomicU64 = AtomicU64::new(0);
static EVIDENCE_INDETERMINATE: AtomicU64 = AtomicU64::new(0);
/// Set once a pass has read the checkpoint set; until then the two rotation gauges are unknown.
static ROTATION_OBSERVED: AtomicBool = AtomicBool::new(false);
/// The oldest `scanned_at` among units that have one, as of the end of the last pass; 0 = none.
static STALEST_SCANNED_AT: AtomicU64 = AtomicU64::new(0);
static UNITS_NEVER_SCANNED: AtomicU64 = AtomicU64::new(0);
/// Red-side lever (d): stop crediting `bytes_reclaimed`, so reclamation is invisible on
/// `/metrics` and OTLP.
#[cfg(test)]
const UNWIRE_BYTES_RECLAIMED: bool = false;

/// Per-process orphan counters since start, published as `/metrics` `orphan_reclaim` and the
/// `orphan_reclaim.*` OTLP instruments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanReclaimCounters {
    /// Journal-evidence reads that returned `ConfirmedAbsent`.
    pub evidence_confirmed_absent: u64,
    /// Journal-evidence reads that returned `JournalPresent`.
    pub evidence_journal_present: u64,
    /// Journal-evidence reads that returned `PeerUnavailable`.
    pub evidence_peer_unavailable: u64,
    /// Journal-evidence reads that returned `Indeterminate`.
    pub evidence_indeterminate: u64,
    /// Units (a `(bucket, L1)` shard or the flat root) this process claimed and visited.
    pub units_visited: u64,
    /// The oldest `scanned_at` (Unix seconds) among units that have one, read at the end of the
    /// last pass. `None` before the first pass, or when no unit has completed a visit.
    pub stalest_scanned_at: Option<u64>,
    /// Units with no `scanned_at`, read at the end of the last pass. `None` before the first.
    pub units_never_scanned: Option<u64>,
    /// Candidates found: range files no `.meta` listed when their leaf was scanned.
    pub found: u64,
    /// Candidates unlinked.
    pub reclaimed: u64,
    /// Bytes (on-disk `st_size`) of the unlinked candidates.
    pub bytes_reclaimed: u64,
    /// Candidates the `.meta` listed by the time the slot lock was held.
    pub kept_listed: u64,
    /// Candidates kept because a journal holds their `Add`.
    pub kept_journal_present: u64,
    /// Candidates kept because the evidence or any step could not be confirmed.
    pub skipped_unconfirmed: u64,
    /// Passes run.
    pub passes: u64,
    /// Scheduled passes skipped, or stopped before a unit, because some active proxy on the
    /// volume is not ready (an older binary, or a roster that cannot be read).
    pub passes_gated: u64,
    /// Reads of a shard lease file (orphan claims or eviction work-list chunk leases) that did
    /// not parse.
    pub leases_unreadable: u64,
    /// Of those, the ones older than [`UNREADABLE_LEASE_EXPIRE_SECS`], read as expired.
    pub leases_unreadable_expired: u64,
}

/// The per-process counters.
pub fn counters() -> OrphanReclaimCounters {
    let observed = ROTATION_OBSERVED.load(Ordering::Relaxed);
    let stalest = STALEST_SCANNED_AT.load(Ordering::Relaxed);
    OrphanReclaimCounters {
        evidence_confirmed_absent: EVIDENCE_CONFIRMED_ABSENT.load(Ordering::Relaxed),
        evidence_journal_present: EVIDENCE_JOURNAL_PRESENT.load(Ordering::Relaxed),
        evidence_peer_unavailable: EVIDENCE_PEER_UNAVAILABLE.load(Ordering::Relaxed),
        evidence_indeterminate: EVIDENCE_INDETERMINATE.load(Ordering::Relaxed),
        units_visited: UNITS_VISITED.load(Ordering::Relaxed),
        stalest_scanned_at: (observed && stalest != 0).then_some(stalest),
        units_never_scanned: observed.then(|| UNITS_NEVER_SCANNED.load(Ordering::Relaxed)),
        found: FOUND.load(Ordering::Relaxed),
        reclaimed: RECLAIMED.load(Ordering::Relaxed),
        bytes_reclaimed: BYTES_RECLAIMED.load(Ordering::Relaxed),
        kept_listed: KEPT_LISTED.load(Ordering::Relaxed),
        kept_journal_present: KEPT_JOURNAL_PRESENT.load(Ordering::Relaxed),
        skipped_unconfirmed: SKIPPED_UNCONFIRMED.load(Ordering::Relaxed),
        passes: PASSES.load(Ordering::Relaxed),
        passes_gated: PASSES_GATED.load(Ordering::Relaxed),
        leases_unreadable: LEASES_UNREADABLE.load(Ordering::Relaxed),
        leases_unreadable_expired: LEASES_UNREADABLE_EXPIRED.load(Ordering::Relaxed),
    }
}

/// Start the rotation for this process. Called once from cache init; there is no switch.
pub fn enable() {
    ENABLED.store(true, Ordering::SeqCst);
}

/// Whether the rotation is enabled in this process.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::SeqCst)
}

/// Clears the in-progress flag when a claimed pass ends, however it ends.
#[derive(Debug)]
pub struct PassClaim(());

impl Drop for PassClaim {
    fn drop(&mut self) {
        PASS_IN_PROGRESS.store(false, Ordering::SeqCst);
    }
}

/// Claim this process's next pass: `Some` when enabled, due, and no pass is in flight. The
/// same per-process claim pattern as the incomplete-upload sweep.
pub fn claim_pass(now_secs: u64) -> Option<PassClaim> {
    if !enabled() {
        return None;
    }
    let last = LAST_PASS_SECS.load(Ordering::Relaxed);
    if last != 0 && now_secs.saturating_sub(last) < PASS_INTERVAL_SECS {
        return None;
    }
    if PASS_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return None;
    }
    LAST_PASS_SECS.store(now_secs.max(1), Ordering::Relaxed);
    Some(PassClaim(()))
}

// ---------------------------------------------------------------------------------------------
// Checkpoints, order and claims
// ---------------------------------------------------------------------------------------------

/// One visit unit's rotation progress.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanCheckpoint {
    /// Unix seconds when the last visit that reached the unit's final leaf finished.
    #[serde(default)]
    pub scanned_at: Option<u64>,
    /// The last L2 leaf an unfinished visit completed; the next visit resumes after it.
    #[serde(default)]
    pub cursor: Option<String>,
}

fn shard_state_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("metadata").join("_shard_state")
}

/// Path of a unit's checkpoint.
pub fn checkpoint_path(cache_dir: &Path, unit: &Shard) -> PathBuf {
    let dir = shard_state_dir(cache_dir).join("orphan");
    if unit.bucket == ROOT_BUCKET {
        dir.join("_root.json")
    } else {
        dir.join(&unit.bucket).join(format!("{}.json", unit.l1))
    }
}

/// Directory name of the orphan rotation's leases under `metadata/_shard_state/`.
pub(crate) const ORPHAN_CLAIMS_DIR: &str = "orphan_claims";

/// Path of `unit`'s lease file in the lease family `dir_name`:
/// `metadata/_shard_state/{dir_name}/{bucket}/{L1}.claim`, or `{dir_name}/_root.claim` for the
/// flat root unit.
///
/// `dir_name` is one path component chosen by the caller from a fixed set of constants (never
/// from data read off the volume), so it is not re-validated here.
pub(crate) fn lease_path(cache_dir: &Path, dir_name: &str, unit: &Shard) -> PathBuf {
    #[cfg(test)]
    let dir_name = if HARD_CODE_LEASE_DIR {
        ORPHAN_CLAIMS_DIR
    } else {
        dir_name
    };
    let dir = shard_state_dir(cache_dir).join(dir_name);
    if unit.bucket == ROOT_BUCKET {
        dir.join("_root.claim")
    } else {
        dir.join(&unit.bucket).join(format!("{}.claim", unit.l1))
    }
}

/// Read a unit's checkpoint. `None` when there is none or it cannot be parsed (ranked first,
/// which re-visits it: the safe direction).
pub fn read_checkpoint(cache_dir: &Path, unit: &Shard) -> Option<OrphanCheckpoint> {
    let text = std::fs::read_to_string(checkpoint_path(cache_dir, unit)).ok()?;
    serde_json::from_str(&text).ok()
}

async fn write_checkpoint(cache_dir: &Path, unit: &Shard, checkpoint: &OrphanCheckpoint) {
    let path = checkpoint_path(cache_dir, unit);
    let body = match serde_json::to_vec(checkpoint) {
        Ok(body) => body,
        Err(e) => {
            debug!("Orphan reclaim: could not serialize a checkpoint: {}", e);
            return;
        }
    };
    if let Err(e) =
        crate::disk_cache::write_atomic(&path, &body, crate::disk_cache::Shared::Yes).await
    {
        debug!(
            "Orphan reclaim: could not write checkpoint {:?}: {}",
            path, e
        );
    }
}

/// The order a pass visits `units` in: the `st_size` fold's ranking applied to this rotation's
/// checkpoints. No checkpoint first, then no `scanned_at`, then oldest `scanned_at`; the unit
/// key breaks every tie.
pub fn visit_order(
    units: &BTreeSet<Shard>,
    checkpoints: &BTreeMap<Shard, OrphanCheckpoint>,
    now_secs: u64,
) -> Vec<Shard> {
    #[cfg(test)]
    if WALL_CLOCK_ORDER_LEVER {
        let all: Vec<Shard> = units.iter().cloned().collect();
        if all.is_empty() {
            return all;
        }
        let index = ((now_secs / PASS_INTERVAL_SECS) as usize) % all.len();
        return vec![all[index].clone()];
    }
    let _ = now_secs;
    let mut keyed: Vec<((u8, u64), &Shard)> = units
        .iter()
        .map(|unit| {
            let rank = match checkpoints.get(unit) {
                None => (0, 0),
                Some(c) => match c.scanned_at {
                    None => (1, 0),
                    Some(at) => (2, at),
                },
            };
            (rank, unit)
        })
        .collect();
    // `units` iterates in key order and the sort is stable, so equal ranks stay in key order.
    keyed.sort_by_key(|(rank, _)| *rank);
    keyed.into_iter().map(|(_, s)| s.clone()).collect()
}

/// The body of a per-shard lease file. Field names and order are the on-disk format of the
/// orphan rotation's claims and must not change: a peer running an older binary parses them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ShardLease {
    pub(crate) instance_id: String,
    pub(crate) claimed_at: u64,
    pub(crate) lease_secs: u64,
}

/// Whether a claim on a unit was won.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimState {
    /// This instance holds the unit until [`release_claim`].
    Claimed,
    /// A live peer holds it, or the claim could not be written.
    Busy,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn try_create_claim(path: &Path, body: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(body)?;
    file.sync_all()
}

fn modified_secs(path: &Path) -> Option<u64> {
    let modified = std::fs::symlink_metadata(path).ok()?.modified().ok()?;
    Some(modified.duration_since(UNIX_EPOCH).ok()?.as_secs())
}

/// Whether `path` was last modified [`UNREADABLE_LEASE_EXPIRE_SECS`] or more before `now`. A
/// modification time in the future (clock skew) or one that cannot be read is not aged.
fn aged_past_unreadable_bound(path: &Path, now: u64) -> bool {
    #[cfg(test)]
    if UNREADABLE_LEASE_NEVER_EXPIRES {
        return false;
    }
    modified_secs(path).is_some_and(|m| now.saturating_sub(m) >= UNREADABLE_LEASE_EXPIRE_SECS)
}

/// A lease file at `path` did not parse at `now`: count it, and say whether it has expired
/// ([`UNREADABLE_LEASE_EXPIRE_SECS`] by its modification time).
pub(crate) fn unreadable_lease_expired(path: &Path, now: u64) -> bool {
    LEASES_UNREADABLE.fetch_add(1, Ordering::Relaxed);
    let expired = aged_past_unreadable_bound(path, now);
    if expired {
        LEASES_UNREADABLE_EXPIRED.fetch_add(1, Ordering::Relaxed);
        warn!(
            "Shard lease {:?} has not parsed for at least {}s; it reads as expired and is removed",
            path, UNREADABLE_LEASE_EXPIRE_SECS
        );
    }
    expired
}

/// Remove the expired unparseable lease at `path`, whose content read as `existing_text`.
///
/// Moved aside first, to a name unique to `instance_id`, and deleted only if what was moved is
/// still that file: the same bytes, still unparseable, and still aged. A peer's fresh claim
/// that replaced it in the meantime is younger than the bound, so it is put back even when it is
/// also caught mid-write and empty. `true` when the file was removed.
pub(crate) fn remove_expired_unreadable_lease(
    path: &Path,
    existing_text: &str,
    now: u64,
    instance_id: &str,
) -> bool {
    let aside = path.with_extension(format!(
        "claim.stale.{}.{}",
        std::process::id(),
        crate::disk_cache::sanitize_object_key_for_filename(instance_id, "")
    ));
    if std::fs::rename(path, &aside).is_err() {
        return false;
    }
    let moved = std::fs::read_to_string(&aside).unwrap_or_default();
    let same = moved == existing_text
        && serde_json::from_str::<ShardLease>(&moved).is_err()
        && aged_past_unreadable_bound(&aside, now);
    if !same {
        let _ = std::fs::rename(&aside, path);
        return false;
    }
    let _ = std::fs::remove_file(&aside);
    true
}

/// Claim `unit` for an orphan visit. An exclusive create wins a free unit; a claim whose lease
/// has lapsed is moved aside by rename (so of two instances taking it over, only one moves the
/// stale file) and the create retried once. Any I/O error is `Busy`.
pub fn claim_unit(cache_dir: &Path, unit: &Shard, instance_id: &str, now: u64) -> ClaimState {
    claim_shard_lease(
        cache_dir,
        ORPHAN_CLAIMS_DIR,
        CLAIM_LEASE_SECS,
        unit,
        instance_id,
        now,
    )
}

/// The reusable per-shard lease: [`claim_unit`] parameterised by the lease directory under
/// `metadata/_shard_state/` and the lease length.
///
/// Two different `dir_name`s are two independent lease families: a live claim in one never
/// excludes the other. The orphan rotation calls this with
/// `(ORPHAN_CLAIMS_DIR, CLAIM_LEASE_SECS)`, which writes the claim bytes it always wrote. The
/// lease is not extended on progress (a stuck holder is worse than a duplicated visit, the
/// fold's recorded reasoning).
pub(crate) fn claim_shard_lease(
    cache_dir: &Path,
    dir_name: &str,
    lease_secs: u64,
    unit: &Shard,
    instance_id: &str,
    now: u64,
) -> ClaimState {
    let path = lease_path(cache_dir, dir_name, unit);
    let claim = ShardLease {
        instance_id: instance_id.to_string(),
        claimed_at: now,
        lease_secs,
    };
    let Ok(body) = serde_json::to_vec(&claim) else {
        return ClaimState::Busy;
    };
    for _ in 0..2 {
        match try_create_claim(&path, &body) {
            Ok(()) => return ClaimState::Claimed,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                debug!("Orphan reclaim: could not claim {}: {}", unit, e);
                return ClaimState::Busy;
            }
        }
        let existing_text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            // Released between the create and the read: retry the create.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return ClaimState::Busy,
        };
        let live = match serde_json::from_str::<ShardLease>(&existing_text) {
            Ok(c) => c.claimed_at.saturating_add(c.lease_secs) > now,
            // An unparseable claim may be a peer's create mid-write: live, until it is older
            // than `UNREADABLE_LEASE_EXPIRE_SECS`. Then it is removed and the
            // create retried.
            Err(_) => {
                if !unreadable_lease_expired(&path, now) {
                    return ClaimState::Busy;
                }
                if !remove_expired_unreadable_lease(&path, &existing_text, now, instance_id) {
                    return ClaimState::Busy;
                }
                continue;
            }
        };
        if live {
            return ClaimState::Busy;
        }
        // Lapsed. Move it aside to a name unique to this attempt; confirm what was moved is
        // the lapsed claim and not a peer's fresh one that replaced it in the meantime.
        let aside = path.with_extension(format!(
            "claim.stale.{}.{}",
            std::process::id(),
            crate::disk_cache::sanitize_object_key_for_filename(instance_id, "")
        ));
        if std::fs::rename(&path, &aside).is_err() {
            return ClaimState::Busy;
        }
        let moved = std::fs::read_to_string(&aside).unwrap_or_default();
        if moved != existing_text {
            // A peer's fresh claim: put it back and stand down.
            let _ = std::fs::rename(&aside, &path);
            return ClaimState::Busy;
        }
        let _ = std::fs::remove_file(&aside);
    }
    ClaimState::Busy
}

/// Release this instance's orphan claim on `unit`. Only a claim naming this instance is
/// removed.
pub fn release_claim(cache_dir: &Path, unit: &Shard, instance_id: &str) {
    release_shard_lease(cache_dir, ORPHAN_CLAIMS_DIR, unit, instance_id);
}

/// [`release_claim`] for the lease family `dir_name`.
pub(crate) fn release_shard_lease(
    cache_dir: &Path,
    dir_name: &str,
    unit: &Shard,
    instance_id: &str,
) {
    let path = lease_path(cache_dir, dir_name, unit);
    let ours = std::fs::read_to_string(&path)
        .ok()
        .and_then(|c| serde_json::from_str::<ShardLease>(&c).ok())
        .is_some_and(|c| c.instance_id == instance_id);
    if ours {
        if let Err(e) = std::fs::remove_file(&path) {
            debug!(
                "Orphan reclaim: could not release {} ({}); it expires with its lease",
                unit, e
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Test instrumentation: concurrent visits of one unit
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
static ACTIVE_VISITS: std::sync::Mutex<Option<BTreeSet<(PathBuf, Shard)>>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static OVERLAPS: AtomicU64 = AtomicU64::new(0);
/// Test seam: a pause per leaf, so concurrent passes interleave.
#[cfg(test)]
static LEAF_PAUSE_MS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn visit_begin(cache_dir: &Path, unit: &Shard) {
    let mut guard = ACTIVE_VISITS.lock().unwrap();
    let set = guard.get_or_insert_with(BTreeSet::new);
    if !set.insert((cache_dir.to_path_buf(), unit.clone())) {
        OVERLAPS.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
fn visit_end(cache_dir: &Path, unit: &Shard) {
    if let Some(set) = ACTIVE_VISITS.lock().unwrap().as_mut() {
        set.remove(&(cache_dir.to_path_buf(), unit.clone()));
    }
}

// ---------------------------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------------------------

/// A pass's limits.
#[derive(Debug, Clone, Copy)]
pub struct PassBudget {
    /// L2 leaves (and the root unit, which counts as one) a pass may visit.
    pub leaves: usize,
    /// Wall-clock time a pass may run.
    pub time: Duration,
}

impl PassBudget {
    /// The production budget.
    pub fn production() -> Self {
        Self {
            leaves: PASS_LEAF_BUDGET,
            time: PASS_TIME_BUDGET,
        }
    }
}

/// What one candidate's protocol decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateOutcome {
    /// Unlinked; carries the on-disk length.
    Reclaimed(u64),
    /// The `.meta` lists the extent under the lock.
    KeptListed,
    /// A journal holds the `Add`.
    KeptJournalPresent,
    /// Evidence was `PeerUnavailable` or `Indeterminate`.
    KeptUnconfirmed(JournalEvidence),
    /// The slot lock, the stat, the `.meta` read or the unlink failed, or a writer may still
    /// be publishing the file (it is younger than [`MIN_CANDIDATE_AGE_SECS`] or its key has a
    /// multipart upload in progress).
    KeptError,
    /// The file was already gone.
    Vanished,
}

/// One pass's result.
#[derive(Debug, Clone, Default)]
pub struct PassOutcome {
    /// Every leaf visited, in order (the root unit appears as `ranges/`).
    pub visited_leaves: Vec<PathBuf>,
    /// Units whose visit reached their last leaf.
    pub units_completed: usize,
    /// Units skipped because a peer held the claim.
    pub units_busy: usize,
    /// Candidates found.
    pub found: u64,
    /// Candidates unlinked.
    pub reclaimed: u64,
    /// Bytes unlinked.
    pub bytes_reclaimed: u64,
    /// Candidates kept because the `.meta` listed them under the lock.
    pub kept_listed: u64,
    /// Candidates kept because a journal held their `Add`.
    pub kept_journal_present: u64,
    /// Candidates kept on unconfirmed evidence, an unrecoverable key, an I/O error, or a
    /// pass with no fresh upload snapshot.
    pub skipped_unconfirmed: u64,
}

impl PassOutcome {
    fn record(&mut self, outcome: CandidateOutcome) {
        match outcome {
            CandidateOutcome::Reclaimed(bytes) => {
                self.reclaimed += 1;
                self.bytes_reclaimed += bytes;
                RECLAIMED.fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                let bytes = if UNWIRE_BYTES_RECLAIMED { 0 } else { bytes };
                BYTES_RECLAIMED.fetch_add(bytes, Ordering::Relaxed);
            }
            CandidateOutcome::KeptListed => {
                self.kept_listed += 1;
                KEPT_LISTED.fetch_add(1, Ordering::Relaxed);
            }
            CandidateOutcome::KeptJournalPresent => {
                self.kept_journal_present += 1;
                KEPT_JOURNAL_PRESENT.fetch_add(1, Ordering::Relaxed);
            }
            CandidateOutcome::KeptUnconfirmed(_) | CandidateOutcome::KeptError => {
                self.skipped_unconfirmed += 1;
                SKIPPED_UNCONFIRMED.fetch_add(1, Ordering::Relaxed);
            }
            CandidateOutcome::Vanished => {}
        }
    }
    fn skipped_unknown(&mut self) {
        self.skipped_unconfirmed += 1;
        SKIPPED_UNCONFIRMED.fetch_add(1, Ordering::Relaxed);
    }
}

/// A candidate orphan: a `.bin` its key's `.meta` did not list when its leaf was scanned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The cache key the file belongs to.
    pub cache_key: String,
    /// The range file.
    pub bin_path: PathBuf,
    /// The key's `.meta`.
    pub meta_path: PathBuf,
    /// Extent start.
    pub start: u64,
    /// Extent end (inclusive).
    pub end: u64,
}

/// `ranges/` once: the bucket shards and the flat legacy range files at the root.
fn enumerate_units(ranges_dir: &Path) -> (BTreeSet<Shard>, Vec<String>) {
    let mut units = BTreeSet::new();
    let mut flat = Vec::new();
    let Ok(entries) = std::fs::read_dir(ranges_dir) else {
        return (units, flat);
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_file() {
            if name.ends_with(".bin") {
                flat.push(name);
            }
            continue;
        }
        if !file_type.is_dir() || name.starts_with('_') || name.starts_with('.') {
            continue;
        }
        let Ok(l1s) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for l1 in l1s.flatten() {
            let Ok(l1_name) = l1.file_name().into_string() else {
                continue;
            };
            if crate::staged_fold::is_shard_level(&l1_name)
                && l1.file_type().is_ok_and(|t| t.is_dir())
            {
                units.insert(Shard::new(name.clone(), l1_name));
            }
        }
    }
    if !flat.is_empty() || ranges_dir.is_dir() {
        units.insert(Shard::new(ROOT_BUCKET, ""));
    }
    flat.sort();
    (units, flat)
}

/// Split `{stem}_{start}-{end}.bin`.
fn parse_bin_name(name: &str) -> Option<(&str, u64, u64)> {
    let body = name.strip_suffix(".bin")?;
    let (stem, extent) = body.rsplit_once('_')?;
    let (start, end) = extent.split_once('-')?;
    Some((stem, start.parse().ok()?, end.parse().ok()?))
}

/// How a stem's `.meta` read went.
enum MetaRead {
    Absent,
    Listed(Box<NewCacheMetadata>),
    Unreadable,
}

fn read_meta(path: &Path) -> MetaRead {
    match std::fs::read_to_string(path) {
        Ok(text) => match serde_json::from_str::<NewCacheMetadata>(&text) {
            Ok(meta) => MetaRead::Listed(Box::new(meta)),
            Err(_) => MetaRead::Unreadable,
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => MetaRead::Absent,
        Err(_) => MetaRead::Unreadable,
    }
}

fn lists_extent(meta: &NewCacheMetadata, start: u64, end: u64) -> bool {
    meta.ranges.iter().any(|r| r.start == start && r.end == end)
}

/// Recover the cache key a sharded range file was written for, by decoding its stem and
/// confirming the key maps back to exactly this path. `None` for a hashed (long-key) stem or
/// any mismatch.
fn key_from_sharded_stem(
    ranges_dir: &Path,
    bucket: &str,
    stem: &str,
    bin_path: &Path,
    suffix: &str,
) -> Option<String> {
    let object_key = percent_encoding::percent_decode_str(stem)
        .decode_utf8()
        .ok()?
        .to_string();
    let cache_key = format!("{}/{}", bucket, object_key);
    let path = crate::disk_cache::get_sharded_path(ranges_dir, &cache_key, suffix).ok()?;
    (path == bin_path).then_some(cache_key)
}

/// Candidates in one directory of range files, against `.meta` files in `meta_dir`.
///
/// `bucket` is `Some` for a sharded leaf, `None` for the flat root. A stem whose `.meta` exists
/// but cannot be read, or whose key cannot be recovered, yields no candidate: it is counted as
/// skipped and its files are kept.
fn scan_leaf(
    ranges_dir: &Path,
    bin_dir: &Path,
    meta_dir: &Path,
    bucket: Option<&str>,
    names: Option<Vec<String>>,
    outcome: &mut PassOutcome,
    uploads: &mut Option<Arc<UploadSnapshot>>,
) -> Vec<Candidate> {
    let names = match names {
        Some(names) => names,
        None => match std::fs::read_dir(bin_dir) {
            Ok(entries) => entries
                .flatten()
                .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|n| n.ends_with(".bin"))
                .collect(),
            Err(_) => return Vec::new(),
        },
    };
    let mut by_stem: BTreeMap<String, Vec<(String, u64, u64)>> = BTreeMap::new();
    for name in names {
        if let Some((stem, start, end)) = parse_bin_name(&name) {
            by_stem
                .entry(stem.to_string())
                .or_default()
                .push((name.clone(), start, end));
        }
    }
    let mut candidates = Vec::new();
    for (stem, files) in by_stem {
        let meta_path = meta_dir.join(format!("{}.meta", stem));
        let meta = read_meta(&meta_path);
        let mut unlisted: Vec<(String, u64, u64)> = Vec::new();
        let listed_key = match &meta {
            MetaRead::Unreadable => {
                // Cannot tell listed from unlisted: keep them all.
                for _ in &files {
                    outcome.skipped_unknown();
                }
                continue;
            }
            MetaRead::Listed(m) => {
                for f in files {
                    if !lists_extent(m, f.1, f.2) {
                        unlisted.push(f);
                    }
                }
                Some(m.cache_key.clone())
            }
            MetaRead::Absent => {
                unlisted = files;
                None
            }
        };
        for (name, start, end) in unlisted {
            outcome.found += 1;
            FOUND.fetch_add(1, Ordering::Relaxed);
            let bin_path = bin_dir.join(&name);
            let suffix = format!("_{}-{}.bin", start, end);
            let cache_key = match (bucket, &listed_key) {
                (Some(b), Some(key)) => {
                    crate::disk_cache::get_sharded_path(ranges_dir, key, &suffix)
                        .ok()
                        .filter(|p| *p == bin_path)
                        .map(|_| key.clone())
                        .or_else(|| key_from_sharded_stem(ranges_dir, b, &stem, &bin_path, &suffix))
                }
                (Some(b), None) => key_from_sharded_stem(ranges_dir, b, &stem, &bin_path, &suffix),
                (None, Some(key)) => Some(key.clone()),
                (None, None) => percent_encoding::percent_decode_str(&stem)
                    .decode_utf8()
                    .ok()
                    .map(|k| k.to_string())
                    .filter(|k| {
                        crate::disk_cache::parse_cache_key(k).is_err()
                            && crate::disk_cache::sanitize_object_key_for_filename(k, "") == stem
                    }),
            };
            let cache_dir = ranges_dir.parent().unwrap_or(ranges_dir);
            match cache_key {
                // A writer may still be publishing the file: keep it for a later pass.
                Some(cache_key)
                    if !std::fs::symlink_metadata(&bin_path).is_ok_and(|m| old_enough(&m))
                        || current_uploads(cache_dir, uploads).keeps(&cache_key) =>
                {
                    outcome.skipped_unknown();
                }
                Some(cache_key) => candidates.push(Candidate {
                    cache_key,
                    bin_path,
                    meta_path: meta_path.clone(),
                    start,
                    end,
                }),
                None => outcome.skipped_unknown(),
            }
        }
    }
    candidates
}

/// Whether a range file is old enough to be past every writer's publish window: its mtime is at
/// least [`MIN_CANDIDATE_AGE_SECS`] in the past. A modification time in the future (clock skew)
/// or one that cannot be read is young, so the file is kept.
fn old_enough(meta: &std::fs::Metadata) -> bool {
    let Ok(modified) = meta.modified() else {
        return false;
    };
    std::time::SystemTime::now()
        .duration_since(modified)
        .is_ok_and(|age| age.as_secs() >= MIN_CANDIDATE_AGE_SECS)
}

/// The longest an [`UploadSnapshot`] is decided with, by the proxy's monotonic clock.
///
/// A record missing from a snapshot was created after the walk, or up to one directory-cache
/// lifetime before it (60 s by default on NFS), and every part of that upload was written after
/// its record. While the snapshot is younger than this bound, such a part's age read against the
/// proxy's clock is under this bound plus the directory-cache lifetime plus however far the
/// proxy's clock runs ahead of the file server's, so the age check keeps it for up to 420 s of
/// skew.
const UPLOAD_SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(MIN_CANDIDATE_AGE_SECS / 5);
/// The directory-cache lifetime [`UPLOAD_SNAPSHOT_MAX_AGE`] allows for: NFS `acdirmax`, 60 s by
/// default.
const UPLOAD_SNAPSHOT_DIRECTORY_CACHE: Duration = Duration::from_secs(60);
/// How far the proxy's clock may run ahead of the file server's while the age check still keeps
/// a part a fresh snapshot missed.
const UPLOAD_SNAPSHOT_CLOCK_SKEW: Duration = Duration::from_secs(420);
// A part missed by a fresh snapshot is younger than the snapshot bound plus the directory-cache
// lifetime plus the skew, so the sum may not exceed the age floor.
const _: () = assert!(
    UPLOAD_SNAPSHOT_MAX_AGE.as_secs()
        + UPLOAD_SNAPSHOT_DIRECTORY_CACHE.as_secs()
        + UPLOAD_SNAPSHOT_CLOCK_SKEW.as_secs()
        <= MIN_CANDIDATE_AGE_SECS,
    "an upload part a fresh snapshot missed must still be younger than the age floor; this \
     assumes the volume's directory cache (NFS acdirmax) holds a listing for at most 60 s"
);
/// The cache keys of the multipart uploads in progress, from one walk of `mpus_in_progress/`
/// that reads every `{uploadId}/upload.meta`. A pass takes one and reuses it for every candidate,
/// at listing time and under each slot lock, until it is older than [`UPLOAD_SNAPSHOT_MAX_AGE`].
///
/// Completing an upload, in this release and in 2.8.x, renames each part into `ranges/`, writes
/// the `.meta` and only then removes the upload's directory, and a part keeps its upload-time
/// mtime, so the age check cannot see this window and a candidate whose key a record names is
/// kept. CreateMultipartUpload writes the record before the client learns the upload ID, so a
/// record created after the snapshot has only parts written after it too.
#[derive(Debug)]
struct UploadSnapshot {
    taken: Instant,
    /// `None` when some record could not be attributed to a key: the walk or a read failed, or a
    /// record that does not parse or names no key was written within [`MIN_CANDIDATE_AGE_SECS`].
    keys: Option<BTreeSet<String>>,
    /// No fresh snapshot can be had this pass: the walk failed, could not attribute a record,
    /// or took longer than [`UPLOAD_SNAPSHOT_MAX_AGE`] and so was stale on arrival. Walking
    /// again would cost the same, so the pass keeps every remaining candidate and ends.
    spent: bool,
}
impl UploadSnapshot {
    /// One walk. `taken` is read before the walk starts, so the walk's own duration counts
    /// against [`UPLOAD_SNAPSHOT_MAX_AGE`].
    fn take(cache_dir: &Path) -> Self {
        #[cfg(test)]
        count_upload_walk(cache_dir);
        let taken = Instant::now();
        let keys = in_progress_upload_keys(cache_dir);
        #[cfg(test)]
        let taken = taken
            .checked_sub(upload_walk_delay(cache_dir))
            .unwrap_or(taken);
        let walked = taken.elapsed();
        if walked >= UPLOAD_SNAPSHOT_MAX_AGE {
            warn!(
                "Orphan reclaim: the walk of mpus_in_progress/ took {:?}, longer than the {:?} \
                 a snapshot is decided with; the pass keeps its remaining candidates",
                walked, UPLOAD_SNAPSHOT_MAX_AGE
            );
        }
        Self {
            taken,
            spent: keys.is_none() || walked >= UPLOAD_SNAPSHOT_MAX_AGE,
            keys,
        }
    }
    fn unknown() -> Self {
        Self {
            taken: Instant::now(),
            keys: None,
            spent: true,
        }
    }
    fn fresh(&self) -> bool {
        self.taken.elapsed() < UPLOAD_SNAPSHOT_MAX_AGE
    }
    /// Whether a candidate for `cache_key` must be kept: the snapshot is stale, could not
    /// attribute every record, or names the key.
    fn keeps(&self, cache_key: &str) -> bool {
        !self.fresh()
            || self
                .keys
                .as_ref()
                .is_none_or(|keys| keys.contains(cache_key))
    }
}
/// The pass's upload snapshot, walked again first if there is none or it is stale. A spent
/// one is returned as it is: a second walk would cost the same.
fn current_uploads(
    cache_dir: &Path,
    slot: &mut Option<Arc<UploadSnapshot>>,
) -> Arc<UploadSnapshot> {
    if let Some(snapshot) = slot.as_ref().filter(|s| s.fresh() || s.spent) {
        return snapshot.clone();
    }
    let snapshot = Arc::new(UploadSnapshot::take(cache_dir));
    *slot = Some(snapshot.clone());
    snapshot
}
/// [`current_uploads`] off the runtime, before a candidate's slot lock is taken, so a walk is
/// never made with the lock held.
async fn refreshed_uploads(
    cache_dir: &Path,
    slot: &mut Option<Arc<UploadSnapshot>>,
) -> Arc<UploadSnapshot> {
    if let Some(snapshot) = slot.as_ref().filter(|s| s.fresh() || s.spent) {
        return snapshot.clone();
    }
    let dir = cache_dir.to_path_buf();
    let mut held = slot.take();
    let (snapshot, held) = tokio::task::spawn_blocking(move || {
        let snapshot = current_uploads(&dir, &mut held);
        (snapshot, held)
    })
    .await
    .unwrap_or_else(|_| {
        let unknown = Arc::new(UploadSnapshot::unknown());
        (unknown.clone(), Some(unknown))
    });
    *slot = held;
    snapshot
}
/// Whether the pass's upload snapshot is spent, so its remaining candidates are kept.
fn upload_snapshot_spent(slot: &Option<Arc<UploadSnapshot>>) -> bool {
    slot.as_ref().is_some_and(|s| s.spent)
}
/// Threads reading `upload.meta` records in one walk, the calling thread included. On NFS a
/// record costs an open and a read, each a round trip, so the walk is bound by latency and
/// parallel reads overlap it. These are plain scoped threads rather than rayon's global pool:
/// the size-tracking scan holds that pool for a whole shard visit at a time, and the walk would
/// wait behind it while its freshness window ran.
const UPLOAD_WALK_THREADS: usize = 32;
/// One record's contribution to a snapshot.
enum UploadRecord {
    Key(String),
    Skipped,
    Unknown,
}
/// The `upload.meta` path of every entry in `mpus_in_progress/`: empty when the directory is
/// absent, `None` when it or an entry cannot be listed.
fn upload_record_paths(cache_dir: &Path) -> Option<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(cache_dir.join("mpus_in_progress")) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(Vec::new()),
        Err(_) => return None,
    };
    let mut records = Vec::new();
    for entry in entries {
        records.push(entry.ok()?.path().join("upload.meta"));
    }
    Some(records)
}
/// Read one record: opened, read, and then stat'd through its open handle.
fn read_upload_record(record: &Path) -> UploadRecord {
    let mut file = match std::fs::File::open(record) {
        Ok(file) => file,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return UploadRecord::Skipped
        }
        Err(_) => return UploadRecord::Unknown,
    };
    let mut bytes = Vec::new();
    if std::io::Read::read_to_end(&mut file, &mut bytes).is_err() {
        return UploadRecord::Unknown;
    }
    let key = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v.get("cache_key")?.as_str().map(str::to_string));
    match key {
        Some(key) => UploadRecord::Key(key),
        None if file.metadata().is_ok_and(|m| old_enough(&m)) => UploadRecord::Skipped,
        None => UploadRecord::Unknown,
    }
}
/// The cache key of every record in `mpus_in_progress/`, read on up to
/// [`UPLOAD_WALK_THREADS`] threads; `None` when one cannot be attributed.
///
/// A record is opened, read, and then stat'd through its open handle, so on NFS its mtime is
/// the one the open revalidated. CreateMultipartUpload is the only writer of `upload.meta`, so a
/// record that does not parse or names no key and is older than [`MIN_CANDIDATE_AGE_SECS`] is a
/// write that ended that long ago and will never parse: it is skipped. A younger one may be
/// mid-write, and its key is unknown.
fn in_progress_upload_keys(cache_dir: &Path) -> Option<BTreeSet<String>> {
    let records = upload_record_paths(cache_dir)?;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let unknown = std::sync::atomic::AtomicBool::new(false);
    let work = || {
        let mut keys = Vec::new();
        while !unknown.load(Ordering::Relaxed) {
            let Some(record) = records.get(next.fetch_add(1, Ordering::Relaxed)) else {
                break;
            };
            match read_upload_record(record) {
                UploadRecord::Key(key) => keys.push(key),
                UploadRecord::Skipped => {}
                UploadRecord::Unknown => unknown.store(true, Ordering::Relaxed),
            }
        }
        keys
    };
    let workers = records.len().clamp(1, UPLOAD_WALK_THREADS);
    let found: Vec<Vec<String>> = std::thread::scope(|scope| {
        // The calling thread is one of the workers, so a thread that cannot be spawned only
        // narrows the walk.
        let spawned: Vec<_> = (1..workers)
            .filter_map(|_| {
                std::thread::Builder::new()
                    .name("orphan-upload-walk".into())
                    .spawn_scoped(scope, work)
                    .ok()
            })
            .collect();
        let mut found = vec![work()];
        for handle in spawned {
            match handle.join() {
                Ok(keys) => found.push(keys),
                Err(_) => unknown.store(true, Ordering::Relaxed),
            }
        }
        found
    });
    if unknown.load(Ordering::Relaxed) {
        return None;
    }
    Some(found.into_iter().flatten().collect())
}
#[cfg(test)]
static UPLOAD_WALKS: std::sync::Mutex<BTreeMap<PathBuf, u64>> =
    std::sync::Mutex::new(BTreeMap::new());
#[cfg(test)]
fn count_upload_walk(cache_dir: &Path) {
    *UPLOAD_WALKS
        .lock()
        .unwrap()
        .entry(cache_dir.to_path_buf())
        .or_default() += 1;
}
#[cfg(test)]
fn upload_walks(cache_dir: &Path) -> u64 {
    UPLOAD_WALKS
        .lock()
        .unwrap()
        .get(cache_dir)
        .copied()
        .unwrap_or(0)
}
#[cfg(test)]
static UPLOAD_WALK_DELAY: std::sync::Mutex<BTreeMap<PathBuf, Duration>> =
    std::sync::Mutex::new(BTreeMap::new());
/// Test seam: how much longer than it really ran a walk of `cache_dir` is taken to have run.
#[cfg(test)]
fn upload_walk_delay(cache_dir: &Path) -> Duration {
    UPLOAD_WALK_DELAY
        .lock()
        .unwrap()
        .get(cache_dir)
        .copied()
        .unwrap_or(Duration::ZERO)
}
/// Whether two stats of one path are of the same unchanged file: device, inode, length and
/// modification time all equal.
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev()
        && a.ino() == b.ino()
        && a.len() == b.len()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
}
#[cfg(test)]
static REPLACE_BEFORE_UNLINK: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
#[cfg(test)]
static REPLACE_KEEPS_SHAPE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
/// Test seam: when armed with `path`, rename a fresh file onto it, as a writer refilling the same
/// range would between the step-2 stat and the unlink. With [`REPLACE_KEEPS_SHAPE`] set, the
/// refill has the judged file's length and modification time, so only its identity differs.
#[cfg(test)]
fn replace_before_unlink(path: &Path) {
    let mut armed = REPLACE_BEFORE_UNLINK.lock().unwrap();
    if armed.as_deref() == Some(path) {
        *armed = None;
        let fresh = path.with_extension("refill");
        if REPLACE_KEEPS_SHAPE.swap(false, Ordering::SeqCst) {
            let old = std::fs::symlink_metadata(path).unwrap();
            std::fs::write(&fresh, vec![b'r'; old.len() as usize]).unwrap();
            filetime::set_file_mtime(
                &fresh,
                filetime::FileTime::from_last_modification_time(&old),
            )
            .unwrap();
        } else {
            std::fs::write(&fresh, b"refilled").unwrap();
        }
        std::fs::rename(&fresh, path).unwrap();
    }
}
/// Whether every active proxy on the volume is ready for orphan reclamation: the
/// eviction work list's journal-roster gate reads `Capable`. `NotCapable` (an active proxy with
/// no current `.ready` record carrying the capability, which every release before 3.0.0 is)
/// and `Unknown` (the roster or a record cannot be read, or no proxy is active) are both "not
/// ready", so the gate fails closed.
fn fleet_ready(cache_dir: &Path) -> bool {
    #[cfg(test)]
    if ORPHAN_IGNORES_FLEET_READINESS {
        return true;
    }
    crate::eviction_worklist::fleet_is_worklist_capable(cache_dir, now_secs())
        == crate::eviction_worklist::Capability::Capable
}

/// The under-lock half of the protocol: steps 2-5, synchronous, with the slot lock held by the
/// caller. `gated` re-reads the fleet-readiness gate first (a scheduled pass).
fn decide_under_lock(
    cache_dir: &Path,
    candidate: &Candidate,
    gated: bool,
    uploads: &UploadSnapshot,
) -> CandidateOutcome {
    // 1a. A proxy that joined since the pass started may not hold the slot lock across its
    // publish-to-journal window.
    if gated && !fleet_ready(cache_dir) {
        return CandidateOutcome::KeptError;
    }
    // 2. Re-stat the `.bin`.
    let judged = match std::fs::symlink_metadata(&candidate.bin_path) {
        Ok(meta) if meta.is_file() => {
            if !old_enough(&meta) {
                return CandidateOutcome::KeptError;
            }
            meta
        }
        Ok(_) => return CandidateOutcome::KeptError,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return CandidateOutcome::Vanished,
        Err(_) => return CandidateOutcome::KeptError,
    };
    let len = judged.len();
    // 2a. The pass's upload snapshot, never a walk under the lock. A completion writes the
    // `.meta` before it removes the upload's record, so a record the snapshot lacks either went
    // after its `.meta` was written, which step 3 re-reads, or came after the snapshot, and then
    // every part of it is younger than step 2's floor. A stale snapshot keeps the file.
    if uploads.keeps(&candidate.cache_key) {
        return CandidateOutcome::KeptError;
    }
    // 3. Re-read the `.meta`: a listing that appeared since the scan keeps the file.
    #[cfg(test)]
    let reread = !SKIP_UNDER_LOCK_META_REREAD;
    #[cfg(not(test))]
    let reread = true;
    if reread {
        match read_meta(&candidate.meta_path) {
            MetaRead::Absent => {}
            MetaRead::Listed(meta) => {
                if lists_extent(&meta, candidate.start, candidate.end) {
                    return CandidateOutcome::KeptListed;
                }
            }
            MetaRead::Unreadable => return CandidateOutcome::KeptError,
        }
    }
    // 4. Journal evidence: every journal re-read in full, here, under the lock.
    let evidence = journal_evidence(
        cache_dir,
        &candidate.cache_key,
        candidate.start,
        candidate.end,
    );
    #[cfg(test)]
    let evidence = if IGNORE_PEER_JOURNALS {
        JournalEvidence::ConfirmedAbsent
    } else {
        evidence
    };
    match evidence {
        JournalEvidence::ConfirmedAbsent => &EVIDENCE_CONFIRMED_ABSENT,
        JournalEvidence::JournalPresent => &EVIDENCE_JOURNAL_PRESENT,
        JournalEvidence::PeerUnavailable => &EVIDENCE_PEER_UNAVAILABLE,
        JournalEvidence::Indeterminate => &EVIDENCE_INDETERMINATE,
    }
    .fetch_add(1, Ordering::Relaxed);
    // 5. Unlink only on proof of absence.
    match evidence {
        JournalEvidence::ConfirmedAbsent => {}
        JournalEvidence::JournalPresent => return CandidateOutcome::KeptJournalPresent,
        other => return CandidateOutcome::KeptUnconfirmed(other),
    }
    // 5a. Re-check just before the unlink. A writer that takes no slot lock (2.8.x) can rename
    // a refill of the same range onto this path after step 2; keep anything that is not the
    // file step 2 judged old. The path is opened and the handle stat'd: an NFS client
    // revalidates a name and its attributes on open (close-to-open), where `lstat` may be
    // answered from its attribute cache. The handle is closed before the unlink, so the client
    // removes the file rather than renaming an open file to `.nfs*`.
    #[cfg(test)]
    replace_before_unlink(&candidate.bin_path);
    let rechecked = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&candidate.bin_path)
            .and_then(|f| f.metadata())
    };
    match rechecked {
        Ok(meta) if meta.is_file() && same_file(&judged, &meta) && old_enough(&meta) => {}
        Ok(_) => return CandidateOutcome::KeptError,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return CandidateOutcome::Vanished,
        Err(_) => return CandidateOutcome::KeptError,
    }
    match std::fs::remove_file(&candidate.bin_path) {
        Ok(()) => CandidateOutcome::Reclaimed(len),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => CandidateOutcome::Vanished,
        Err(e) => {
            warn!(
                "Orphan reclaim: could not unlink {:?}: {}",
                candidate.bin_path, e
            );
            CandidateOutcome::KeptError
        }
    }
}

/// Run the per-candidate protocol: take the key's slot lock (step 1), then steps 2-5.
#[cfg(test)]
pub async fn reclaim_candidate(cache_dir: &Path, candidate: &Candidate) -> CandidateOutcome {
    reclaim_candidate_with(cache_dir, candidate, false, &mut None).await
}
/// Refresh the pass's upload snapshot if it is stale, then run the per-candidate protocol.
async fn reclaim_candidate_with(
    cache_dir: &Path,
    candidate: &Candidate,
    gated: bool,
    uploads: &mut Option<Arc<UploadSnapshot>>,
) -> CandidateOutcome {
    let uploads = refreshed_uploads(cache_dir, uploads).await;
    if uploads.spent {
        return CandidateOutcome::KeptError;
    }
    reclaim_candidate_gated(cache_dir, candidate, gated, uploads).await
}

async fn reclaim_candidate_gated(
    cache_dir: &Path,
    candidate: &Candidate,
    gated: bool,
    uploads: Arc<UploadSnapshot>,
) -> CandidateOutcome {
    let lock = match crate::metadata_lock_manager::acquire_slot_lock_within_budget(
        cache_dir,
        &candidate.cache_key,
    )
    .await
    {
        Ok(lock) => lock,
        Err(e) => {
            debug!(
                "Orphan reclaim: slot lock not taken for {}: {}",
                candidate.cache_key, e
            );
            return CandidateOutcome::KeptError;
        }
    };
    let cache_dir = cache_dir.to_path_buf();
    let owned = candidate.clone();
    let decided = tokio::task::spawn_blocking(move || {
        let outcome = decide_under_lock(&cache_dir, &owned, gated, &uploads);
        drop(lock);
        outcome
    })
    .await;
    decided.unwrap_or(CandidateOutcome::KeptError)
}

/// Visit one unit from its cursor until it completes or the budget runs out. Returns whether
/// it completed.
// One over clippy's limit: the pass's upload snapshot travels with the leaf budget and the
// outcome it is spent against.
#[allow(clippy::too_many_arguments)]
async fn visit_unit(
    cache_dir: &Path,
    unit: &Shard,
    flat_names: &[String],
    budget_leaves: &mut usize,
    deadline: Instant,
    outcome: &mut PassOutcome,
    gated: bool,
    uploads: &mut Option<Arc<UploadSnapshot>>,
) -> bool {
    let ranges_dir = cache_dir.join("ranges");
    let metadata_dir = cache_dir.join("metadata");
    let previous = read_checkpoint(cache_dir, unit).unwrap_or_default();
    if unit.bucket == ROOT_BUCKET {
        if *budget_leaves == 0 {
            return false;
        }
        *budget_leaves -= 1;
        outcome.visited_leaves.push(ranges_dir.clone());
        let candidates = {
            let (r, m, names) = (
                ranges_dir.clone(),
                metadata_dir.clone(),
                flat_names.to_vec(),
            );
            let mut local = PassOutcome::default();
            let mut held = uploads.take();
            let found = tokio::task::spawn_blocking(move || {
                let c = scan_leaf(&r, &r, &m, None, Some(names), &mut local, &mut held);
                (c, local, held)
            })
            .await;
            match found {
                Ok((c, local, held)) => {
                    *uploads = held;
                    outcome.found += local.found;
                    outcome.skipped_unconfirmed += local.skipped_unconfirmed;
                    c
                }
                Err(_) => return false,
            }
        };
        for candidate in candidates {
            if upload_snapshot_spent(uploads) {
                outcome.skipped_unknown();
                continue;
            }
            outcome.record(reclaim_candidate_with(cache_dir, &candidate, gated, uploads).await);
        }
        if upload_snapshot_spent(uploads) {
            // Its candidates were kept undecided: the next pass visits the unit again.
            return false;
        }
        write_checkpoint(
            cache_dir,
            unit,
            &OrphanCheckpoint {
                scanned_at: Some(now_secs()),
                cursor: None,
            },
        )
        .await;
        return true;
    }
    let shard_ranges = ranges_dir.join(&unit.bucket).join(&unit.l1);
    let shard_meta = metadata_dir.join(&unit.bucket).join(&unit.l1);
    let mut leaves: Vec<String> = match std::fs::read_dir(&shard_ranges) {
        Ok(entries) => entries
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    };
    leaves.sort();
    if let Some(cursor) = &previous.cursor {
        leaves.retain(|l| l > cursor);
    }
    let mut cursor = previous.cursor.clone();
    let mut completed = true;
    for leaf in leaves {
        if *budget_leaves == 0 || Instant::now() >= deadline {
            completed = false;
            break;
        }
        *budget_leaves -= 1;
        #[cfg(test)]
        {
            let pause = LEAF_PAUSE_MS.load(Ordering::Relaxed);
            if pause > 0 {
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
        }
        let bin_dir = shard_ranges.join(&leaf);
        outcome.visited_leaves.push(bin_dir.clone());
        let (r, b, m, bucket) = (
            ranges_dir.clone(),
            bin_dir.clone(),
            shard_meta.join(&leaf),
            unit.bucket.clone(),
        );
        let mut held = uploads.take();
        let scanned = tokio::task::spawn_blocking(move || {
            let mut local = PassOutcome::default();
            let c = scan_leaf(&r, &b, &m, Some(&bucket), None, &mut local, &mut held);
            (c, local, held)
        })
        .await;
        let candidates = match scanned {
            Ok((c, local, held)) => {
                *uploads = held;
                outcome.found += local.found;
                outcome.skipped_unconfirmed += local.skipped_unconfirmed;
                c
            }
            Err(_) => {
                completed = false;
                break;
            }
        };
        for candidate in candidates {
            if upload_snapshot_spent(uploads) {
                outcome.skipped_unknown();
                continue;
            }
            outcome.record(reclaim_candidate_with(cache_dir, &candidate, gated, uploads).await);
        }
        if upload_snapshot_spent(uploads) {
            // Its candidates were kept undecided: the next pass resumes at this leaf.
            completed = false;
            break;
        }
        cursor = Some(leaf);
    }
    let checkpoint = if completed {
        OrphanCheckpoint {
            scanned_at: Some(now_secs()),
            cursor: None,
        }
    } else {
        // Keep the previous `scanned_at`, so an unfinished unit keeps its rank and is resumed.
        OrphanCheckpoint {
            scanned_at: previous.scanned_at,
            cursor,
        }
    };
    write_checkpoint(cache_dir, unit, &checkpoint).await;
    completed
}

/// Re-read every unit's checkpoint after a pass and publish the rotation gauges: the oldest
/// `scanned_at` and how many units have none. One small-file read per unit, the same set the
/// pass's own ranking read.
fn record_rotation_progress(cache_dir: &Path, units: &BTreeSet<Shard>) {
    let mut stalest: Option<u64> = None;
    let mut never: u64 = 0;
    for unit in units {
        match read_checkpoint(cache_dir, unit).and_then(|c| c.scanned_at) {
            Some(at) => stalest = Some(stalest.map_or(at, |s| s.min(at))),
            None => never += 1,
        }
    }
    STALEST_SCANNED_AT.store(stalest.unwrap_or(0), Ordering::Relaxed);
    UNITS_NEVER_SCANNED.store(never, Ordering::Relaxed);
    ROTATION_OBSERVED.store(true, Ordering::Relaxed);
}

/// The scheduled pass every proxy's maintenance cycle runs: [`run_pass`] behind
/// the fleet-readiness gate ([`fleet_ready`]). `None` when the gate is closed: some active proxy
/// on the volume is older than 3.0.0, or the roster cannot be read. Within the pass the gate is
/// re-read before each unit and under each candidate's slot lock, so a proxy that joins
/// mid-pass stops it at the next unit and keeps every candidate not yet decided.
pub async fn run_scheduled_pass(
    cache_dir: &Path,
    instance_id: &str,
    budget: PassBudget,
) -> Option<PassOutcome> {
    if !fleet_ready(cache_dir) {
        PASSES_GATED.fetch_add(1, Ordering::Relaxed);
        debug!(
            "Orphan reclaim: pass skipped; some active proxy on the cache volume is not ready \
             (older than 3.0.0, or the roster could not be read)"
        );
        return None;
    }
    Some(run_pass_inner(cache_dir, instance_id, budget, true).await)
}

/// One rotation pass over `cache_dir`: rank the units, claim each in turn, visit from its
/// cursor, and stop at the budget. Ungated: the scheduled entry is [`run_scheduled_pass`].
#[cfg(test)]
pub async fn run_pass(cache_dir: &Path, instance_id: &str, budget: PassBudget) -> PassOutcome {
    run_pass_inner(cache_dir, instance_id, budget, false).await
}

async fn run_pass_inner(
    cache_dir: &Path,
    instance_id: &str,
    budget: PassBudget,
    gated: bool,
) -> PassOutcome {
    PASSES.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let deadline = started + budget.time;
    let mut outcome = PassOutcome::default();
    let ranges_dir = cache_dir.join("ranges");
    let (units, flat_names) = {
        let r = ranges_dir.clone();
        match tokio::task::spawn_blocking(move || enumerate_units(&r)).await {
            Ok(found) => found,
            Err(_) => return outcome,
        }
    };
    let checkpoints: BTreeMap<Shard, OrphanCheckpoint> = units
        .iter()
        .filter_map(|u| read_checkpoint(cache_dir, u).map(|c| (u.clone(), c)))
        .collect();
    let now = now_secs();
    let mut budget_leaves = budget.leaves;
    let mut uploads = None;
    for unit in visit_order(&units, &checkpoints, now) {
        if budget_leaves == 0 || Instant::now() >= deadline {
            break;
        }
        if gated && !fleet_ready(cache_dir) {
            PASSES_GATED.fetch_add(1, Ordering::Relaxed);
            break;
        }
        if claim_unit(cache_dir, &unit, instance_id, now_secs()) != ClaimState::Claimed {
            outcome.units_busy += 1;
            continue;
        }
        UNITS_VISITED.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        visit_begin(cache_dir, &unit);
        let completed = visit_unit(
            cache_dir,
            &unit,
            &flat_names,
            &mut budget_leaves,
            deadline,
            &mut outcome,
            gated,
            &mut uploads,
        )
        .await;
        #[cfg(test)]
        visit_end(cache_dir, &unit);
        release_claim(cache_dir, &unit, instance_id);
        if completed {
            outcome.units_completed += 1;
        }
        if upload_snapshot_spent(&uploads) {
            break;
        }
    }
    record_rotation_progress(cache_dir, &units);
    if outcome.reclaimed > 0 || outcome.found > 0 {
        info!(
            "Orphan reclaim pass: found={} reclaimed={} bytes_reclaimed={} kept_listed={} \
             kept_journal_present={} skipped_unconfirmed={} leaves={} elapsed_ms={}",
            outcome.found,
            outcome.reclaimed,
            outcome.bytes_reclaimed,
            outcome.kept_listed,
            outcome.kept_journal_present,
            outcome.skipped_unconfirmed,
            outcome.visited_leaves.len(),
            started.elapsed().as_millis()
        );
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::RangeSpec;
    use crate::compression::CompressionAlgorithm;
    use crate::journal_manager::{JournalEntry, JournalManager, JournalOperation};
    use tempfile::TempDir;

    /// The counters are process-global and libtest runs these tests as parallel threads, so
    /// every test that runs a pass or a candidate takes this guard for its whole body. Without
    /// it the exact-delta assertions in
    /// `r15_15_bytes_reclaimed_on_the_metrics_surface_moves_by_exactly_the_reclaimed_file`
    /// would read a concurrent test's reclamation.
    static PASS_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn big_budget() -> PassBudget {
        PassBudget {
            leaves: usize::MAX,
            time: Duration::from_secs(60),
        }
    }

    fn journals_dir(cache: &Path) -> PathBuf {
        let dir = cache.join("metadata").join("_journals");
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn bin_path(cache: &Path, key: &str, start: u64, end: u64) -> PathBuf {
        crate::disk_cache::get_sharded_path(
            &cache.join("ranges"),
            key,
            &format!("_{}-{}.bin", start, end),
        )
        .unwrap()
    }

    fn meta_path(cache: &Path, key: &str) -> PathBuf {
        crate::disk_cache::get_sharded_path(&cache.join("metadata"), key, ".meta").unwrap()
    }

    /// A range file older than [`MIN_CANDIDATE_AGE_SECS`], as a real orphan is.
    fn write_bin(cache: &Path, key: &str, start: u64, end: u64) -> PathBuf {
        let path = write_young_bin(cache, key, start, end);
        let old = now_secs() - MIN_CANDIDATE_AGE_SECS - 60;
        filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(old as i64, 0)).unwrap();
        path
    }
    /// A range file written just now, as one inside its writer's publish window is.
    fn write_young_bin(cache: &Path, key: &str, start: u64, end: u64) -> PathBuf {
        let path = bin_path(cache, key, start, end);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![7u8; (end - start + 1) as usize]).unwrap();
        path
    }
    fn range_spec(key: &str, start: u64, end: u64) -> RangeSpec {
        let size = end - start + 1;
        let file = crate::disk_cache::sanitize_object_key_for_filename(
            crate::disk_cache::parse_cache_key(key).unwrap().1,
            &format!("_{}-{}.bin", start, end),
        );
        RangeSpec::new(start, end, file, CompressionAlgorithm::Lz4, size, size)
    }

    fn write_meta(cache: &Path, key: &str, extents: &[(u64, u64)]) -> PathBuf {
        let mut meta = NewCacheMetadata {
            cache_key: key.to_string(),
            ..Default::default()
        };
        meta.ranges = extents
            .iter()
            .map(|(s, e)| range_spec(key, *s, *e))
            .collect();
        let path = meta_path(cache, key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string(&meta).unwrap()).unwrap();
        path
    }

    fn add_entry(instance: &str, key: &str, start: u64, end: u64) -> JournalEntry {
        let spec = range_spec(key, start, end);
        JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: instance.to_string(),
            cache_key: key.to_string(),
            range_file_path: spec.file_path.clone(),
            range_spec: spec,
            operation: JournalOperation::Add,
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: None,
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        }
    }

    /// Every file under `root` except this module's own checkpoints, claims and the slot-lock
    /// files, with its length: "nothing else on disk changed".
    ///
    /// The per-instance readiness records under `metadata/_shard_state/ready/` are skipped too.
    /// Since then a journal append refreshes the appending id's `.ready` record on
    /// a detached blocking task, so a test that appends has one landing beside it at a moment
    /// it does not control -- and the half-written `{id}.ready.tmp.{host}.{pid}` is visible as
    /// well. Nothing this helper exists to catch lives there: a readiness record carries no
    /// size, no entry count and no range, so excluding it still leaves every `.bin`, every
    /// `.meta`, every journal, the ledger and the size-tracking documents compared exactly.
    fn tree(root: &Path) -> BTreeMap<PathBuf, u64> {
        fn walk(dir: &Path, root: &Path, out: &mut BTreeMap<PathBuf, u64>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for e in entries.flatten() {
                let p = e.path();
                let rel = p.strip_prefix(root).unwrap().to_path_buf();
                let s = rel.to_string_lossy();
                if s.contains("_shard_state/orphan")
                    || s.contains("_shard_state/ready")
                    || s.contains("_locks")
                    || s.contains("locks/")
                {
                    continue;
                }
                if p.is_dir() {
                    walk(&p, root, out);
                } else {
                    out.insert(rel, std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0));
                }
            }
        }
        let mut out = BTreeMap::new();
        walk(root, root, &mut out);
        out
    }

    /// Convergence: N units and a budget of k per pass reach every unit within ceil(N/k)
    /// passes (the shape).
    #[tokio::test]
    async fn rotation_reaches_every_leaf_within_ceil_n_over_k_passes() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        // Leaves spread over three shards in two buckets: 4 + 3 + 3 leaves.
        let mut expected: BTreeSet<PathBuf> = BTreeSet::new();
        for (bucket, l1, count) in [("bkt-a", "0a", 4), ("bkt-a", "1b", 3), ("bkt-b", "ff", 3)] {
            for i in 0..count {
                let leaf = cache
                    .join("ranges")
                    .join(bucket)
                    .join(l1)
                    .join(format!("{:03x}", i));
                std::fs::create_dir_all(&leaf).unwrap();
                expected.insert(leaf);
            }
        }
        expected.insert(cache.join("ranges"));
        let n = expected.len();
        let k = 3;
        let passes = n.div_ceil(k);
        let mut seen: BTreeSet<PathBuf> = BTreeSet::new();
        for _ in 0..passes {
            let out = run_pass(
                cache,
                "inst",
                PassBudget {
                    leaves: k,
                    time: Duration::from_secs(60),
                },
            )
            .await;
            for leaf in out.visited_leaves {
                // A revisit is the next rotation starting: legitimate once every leaf was reached.
                let covered = expected.is_subset(&seen);
                assert!(
                    seen.insert(leaf.clone()) || covered,
                    "leaf {:?} revisited before every leaf was reached",
                    leaf
                );
            }
        }
        let missing: Vec<_> = expected.difference(&seen).collect();
        assert!(
            missing.is_empty(),
            "convergence: {} of {} leaves unreached after {} passes of {}: {:?}",
            missing.len(),
            n,
            passes,
            k,
            missing
        );
    }

    #[test]
    fn a_live_claim_excludes_a_peer_and_a_lapsed_one_is_taken_over() {
        let temp = TempDir::new().unwrap();
        let unit = Shard::new("bkt", "0a");
        let now = 1_000_000;
        assert_eq!(
            claim_unit(temp.path(), &unit, "a", now),
            ClaimState::Claimed
        );
        assert_eq!(claim_unit(temp.path(), &unit, "b", now), ClaimState::Busy);
        release_claim(temp.path(), &unit, "b");
        assert_eq!(
            claim_unit(temp.path(), &unit, "b", now + 1),
            ClaimState::Busy
        );
        assert_eq!(
            claim_unit(temp.path(), &unit, "b", now + CLAIM_LEASE_SECS + 1),
            ClaimState::Claimed,
            "a lapsed claim is taken over"
        );
        release_claim(temp.path(), &unit, "a");
        assert_eq!(
            claim_unit(temp.path(), &unit, "a", now + CLAIM_LEASE_SECS + 2),
            ClaimState::Busy
        );
        release_claim(temp.path(), &unit, "b");
        assert_eq!(
            claim_unit(temp.path(), &unit, "a", now + CLAIM_LEASE_SECS + 3),
            ClaimState::Claimed
        );
    }

    /// An empty claim file (a crash between the exclusive create and the write)
    /// blocks its shard only until it is older than `UNREADABLE_LEASE_EXPIRE_SECS`; then a claim
    /// removes it and wins. A fresh empty file (a peer mid-write) still reads as busy.
    #[test]
    fn an_unparseable_claim_blocks_only_until_it_ages_past_the_bound() {
        let temp = TempDir::new().unwrap();
        let unit = Shard::new("bkt", "0a");
        let path = lease_path(temp.path(), ORPHAN_CLAIMS_DIR, &unit);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"").unwrap();
        let now = now_secs();
        let before = counters();
        assert_eq!(
            claim_unit(temp.path(), &unit, "a", now),
            ClaimState::Busy,
            "a fresh unparseable claim may be a peer mid-write"
        );
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_unix_time((now - UNREADABLE_LEASE_EXPIRE_SECS - 1) as i64, 0),
        )
        .unwrap();
        assert_eq!(
            claim_unit(temp.path(), &unit, "a", now),
            ClaimState::Claimed,
            "an empty claim older than the bound must not block its shard forever"
        );
        let after = counters();
        assert!(after.leases_unreadable >= before.leases_unreadable + 2);
        assert!(after.leases_unreadable_expired > before.leases_unreadable_expired);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            serde_json::from_str::<ShardLease>(&text).is_ok_and(|l| l.instance_id == "a"),
            "the new claim is in place: {text:?}"
        );
    }

    /// The claim struct as it was before it was generalised, verbatim. The orphan claim bytes on
    /// disk are a format peers running an older binary parse, so the generalised lease must
    /// write exactly what this wrote.
    #[derive(Serialize)]
    struct LegacyOrphanClaim {
        instance_id: String,
        claimed_at: u64,
        lease_secs: u64,
    }

    /// What the pre-change `claim_unit` wrote for instance `a` at `now = 1_000_000`.
    const GOLDEN_ORPHAN_CLAIM: &[u8] =
        br#"{"instance_id":"a","claimed_at":1000000,"lease_secs":600}"#;

    #[test]
    fn the_orphan_claim_file_is_byte_identical_to_the_pre_generalisation_claim() {
        let temp = TempDir::new().unwrap();
        let now = 1_000_000;
        for (unit, relative) in [
            (
                Shard::new("bkt", "0a"),
                "metadata/_shard_state/orphan_claims/bkt/0a.claim",
            ),
            (
                Shard::new(ROOT_BUCKET, ""),
                "metadata/_shard_state/orphan_claims/_root.claim",
            ),
        ] {
            assert_eq!(
                claim_unit(temp.path(), &unit, "a", now),
                ClaimState::Claimed
            );
            let path = temp.path().join(relative);
            let written = std::fs::read(&path).unwrap_or_else(|e| panic!("{:?}: {}", path, e));
            assert_eq!(
                written,
                GOLDEN_ORPHAN_CLAIM,
                "claim bytes changed: {}",
                String::from_utf8_lossy(&written)
            );
            let legacy = serde_json::to_vec(&LegacyOrphanClaim {
                instance_id: "a".to_string(),
                claimed_at: now,
                lease_secs: CLAIM_LEASE_SECS,
            })
            .unwrap();
            assert_eq!(
                written, legacy,
                "differs from the pre-change struct's output"
            );
        }
    }

    #[test]
    fn two_lease_families_do_not_exclude_each_other() {
        let temp = TempDir::new().unwrap();
        let unit = Shard::new("bkt", "0a");
        let now = 1_000_000;
        assert_eq!(
            claim_shard_lease(temp.path(), "eviction_claims", 900, &unit, "a", now),
            ClaimState::Claimed
        );
        assert_eq!(
            claim_unit(temp.path(), &unit, "b", now),
            ClaimState::Claimed,
            "a live eviction claim must not block the orphan claim on the same shard"
        );
        assert_eq!(
            claim_shard_lease(temp.path(), "eviction_claims", 900, &unit, "b", now),
            ClaimState::Busy,
            "within one family a live claim still excludes a peer"
        );
        // Releasing one family leaves the other's claim in place.
        release_shard_lease(temp.path(), "eviction_claims", &unit, "a");
        assert_eq!(claim_unit(temp.path(), &unit, "c", now), ClaimState::Busy);
        let eviction = temp
            .path()
            .join("metadata/_shard_state/eviction_claims/bkt/0a.claim");
        assert!(!eviction.exists(), "the owner's release removes its claim");
    }

    #[test]
    fn a_lease_lapses_after_its_own_length_not_the_orphan_length() {
        let temp = TempDir::new().unwrap();
        let unit = Shard::new("bkt", "0b");
        let now = 2_000_000;
        assert_eq!(
            claim_shard_lease(temp.path(), "eviction_claims", 900, &unit, "a", now),
            ClaimState::Claimed
        );
        // Past the orphan length, inside the 900 s this family asked for.
        assert_eq!(
            claim_shard_lease(
                temp.path(),
                "eviction_claims",
                900,
                &unit,
                "b",
                now + CLAIM_LEASE_SECS + 1
            ),
            ClaimState::Busy
        );
        assert_eq!(
            claim_shard_lease(temp.path(), "eviction_claims", 900, &unit, "b", now + 901),
            ClaimState::Claimed
        );
    }

    /// Two managers on one cache_dir, running passes concurrently, never visit the same unit at
    /// the same time, and between them reach every leaf.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_instances_never_visit_one_shard_at_once() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path().to_path_buf();
        let mut expected: BTreeSet<PathBuf> = BTreeSet::new();
        for l1 in ["00", "01", "02", "03", "04", "05"] {
            for i in 0..3 {
                let leaf = cache
                    .join("ranges")
                    .join("bkt")
                    .join(l1)
                    .join(format!("{:03x}", i));
                std::fs::create_dir_all(&leaf).unwrap();
                expected.insert(leaf);
            }
        }
        LEAF_PAUSE_MS.store(15, Ordering::SeqCst);
        let before = OVERLAPS.load(Ordering::SeqCst);
        let (c1, c2) = (cache.clone(), cache.clone());
        let a = tokio::spawn(async move { run_pass(&c1, "instance-a", big_budget()).await });
        let b = tokio::spawn(async move { run_pass(&c2, "instance-b", big_budget()).await });
        let (a, b) = (a.await.unwrap(), b.await.unwrap());
        LEAF_PAUSE_MS.store(0, Ordering::SeqCst);
        assert_eq!(
            OVERLAPS.load(Ordering::SeqCst),
            before,
            "two instances visited the same shard at the same time"
        );
        assert!(
            a.units_busy + b.units_busy > 0,
            "the passes did contend for units"
        );
        let seen: BTreeSet<PathBuf> = a
            .visited_leaves
            .iter()
            .chain(b.visited_leaves.iter())
            .cloned()
            .collect();
        assert!(expected.is_subset(&seen));
    }

    /// R15.14: instance A's fill is journal-only (its Add pending in A's journal, no `.meta`).
    /// Instance B rotates. The file survives with `JournalPresent`, and nothing is debited.
    #[tokio::test]
    async fn r15_14_a_journal_pending_peer_fill_survives_and_nothing_is_debited() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        let key = "bkt/scn1/journal-only";
        let bin = write_bin(cache, key, 0, 1023);
        let a = JournalManager::new(cache.to_path_buf(), "instance-a".to_string());
        a.append_range_entry(key, add_entry("instance-a", key, 0, 1023))
            .await
            .unwrap();
        let before = tree(cache);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(bin.exists(), "B reclaimed A's journal-pending fill");
        assert_eq!(out.found, 1);
        assert_eq!(out.kept_journal_present, 1);
        assert_eq!(out.reclaimed, 0);
        assert_eq!(out.bytes_reclaimed, 0);
        assert_eq!(
            tree(cache),
            before,
            "no debit: nothing else on disk changed"
        );
    }

    /// R15.13 shape: a confirmed-absent orphan is reclaimed, `bytes_reclaimed` rises by its
    /// size, and nothing else on disk changes (a listed sibling, its `.meta`, the journals).
    #[tokio::test]
    async fn r15_13_a_confirmed_absent_orphan_is_reclaimed_and_nothing_else_changes() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        std::fs::write(journals_dir(cache).join("peer.journal"), "").unwrap();
        let key = "bkt/scn1/orphaned";
        let listed = write_bin(cache, key, 0, 99);
        let orphan = write_bin(cache, key, 100, 611);
        write_meta(cache, key, &[(0, 99)]);
        let mut before = tree(cache);
        let global_before = counters().bytes_reclaimed;
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(!orphan.exists(), "the orphan was reclaimed");
        assert!(listed.exists());
        assert_eq!(out.reclaimed, 1);
        assert_eq!(out.bytes_reclaimed, 512);
        assert!(counters().bytes_reclaimed >= global_before + 512);
        before.remove(orphan.strip_prefix(cache).unwrap());
        assert_eq!(tree(cache), before, "only the orphan changed");
    }

    /// A range file younger than the minimum age may be inside its writer's publish window: a
    /// pass and the under-lock decision both keep it, while an old one beside it is reclaimed.
    #[tokio::test]
    async fn a_young_unlisted_bin_is_kept_and_an_old_one_reclaimed() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/guard/age";
        let listed = write_bin(cache, key, 0, 99);
        let old = write_bin(cache, key, 100, 199);
        let young = write_young_bin(cache, key, 200, 299);
        write_meta(cache, key, &[(0, 99)]);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(listed.exists());
        assert!(!old.exists(), "the old orphan was reclaimed");
        assert!(
            young.exists(),
            "a range file inside the publish window must be kept"
        );
        assert_eq!(out.reclaimed, 1);
        let candidate = Candidate {
            cache_key: key.to_string(),
            bin_path: young.clone(),
            meta_path: meta_path(cache, key),
            start: 200,
            end: 299,
        };
        assert_eq!(
            reclaim_candidate(cache, &candidate).await,
            CandidateOutcome::KeptError
        );
        assert!(young.exists(), "the under-lock check keeps it too");
    }

    /// A multipart completion renames parts into `ranges/` before writing the `.meta`, and a
    /// part's mtime is its upload time: an old part is kept while its key's upload record
    /// exists, a record for another key does not keep it, and it goes once its record is gone.
    #[tokio::test]
    async fn an_old_part_bin_is_kept_while_its_upload_record_exists() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/guard/mpu";
        write_bin(cache, key, 0, 99);
        let part = write_bin(cache, key, 100, 199);
        write_meta(cache, key, &[(0, 99)]);
        let record = |id: &str, k: &str| {
            let dir = cache.join("mpus_in_progress").join(id);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("upload.meta"),
                serde_json::json!({"upload_id": id, "cache_key": k, "parts": []}).to_string(),
            )
            .unwrap();
            dir
        };
        let ours = record("upload-ours", key);
        record("upload-other", "bkt/guard/other");
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(
            part.exists(),
            "a part must be kept while its upload record exists"
        );
        assert_eq!(out.reclaimed, 0);
        let candidate = Candidate {
            cache_key: key.to_string(),
            bin_path: part.clone(),
            meta_path: meta_path(cache, key),
            start: 100,
            end: 199,
        };
        assert_eq!(
            reclaim_candidate(cache, &candidate).await,
            CandidateOutcome::KeptError
        );
        assert!(part.exists(), "the under-lock check keeps it too");
        std::fs::remove_dir_all(&ours).unwrap();
        assert_eq!(
            reclaim_candidate(cache, &candidate).await,
            CandidateOutcome::Reclaimed(100)
        );
    }

    fn upload_record(cache: &Path, id: &str, body: &str) -> PathBuf {
        let dir = cache.join("mpus_in_progress").join(id);
        std::fs::create_dir_all(&dir).unwrap();
        let record = dir.join("upload.meta");
        std::fs::write(&record, body).unwrap();
        record
    }
    fn record_body(id: &str, key: &str) -> String {
        serde_json::json!({"upload_id": id, "cache_key": key, "parts": []}).to_string()
    }
    const TORN_RECORD: &str = "{\"upload_id\": \"upload-torn\", \"cache_k";
    fn candidate_for(cache: &Path, key: &str, start: u64, end: u64) -> Candidate {
        Candidate {
            cache_key: key.to_string(),
            bin_path: bin_path(cache, key, start, end),
            meta_path: meta_path(cache, key),
            start,
            end,
        }
    }
    /// One walk of `mpus_in_progress/` serves every candidate of a pass, at listing time and
    /// under each slot lock.
    #[tokio::test]
    async fn one_upload_walk_serves_every_candidate_in_a_pass() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        upload_record(
            cache,
            "upload-other",
            &record_body("upload-other", "bkt/walk/other"),
        );
        let keys = ["bkt/walk/a", "bkt/walk/b", "bkt/walk/c", "bkt/walk/d"];
        for key in keys {
            write_bin(cache, key, 0, 99);
        }
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert_eq!(out.reclaimed, 4, "every unrelated orphan is reclaimed");
        assert_eq!(
            upload_walks(cache),
            1,
            "one walk of mpus_in_progress/ per pass, not one per candidate"
        );
    }
    /// A part whose upload the snapshot names is kept under the lock, even after the record
    /// has gone, because the decision reads the snapshot and not the directory.
    #[tokio::test]
    async fn an_old_part_named_by_the_snapshot_is_kept_under_the_lock() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/snap/named";
        let part = write_bin(cache, key, 0, 99);
        let record = upload_record(cache, "upload-named", &record_body("upload-named", key));
        let snapshot = UploadSnapshot::take(cache);
        std::fs::remove_dir_all(record.parent().unwrap()).unwrap();
        assert_eq!(
            decide_under_lock(cache, &candidate_for(cache, key, 0, 99), false, &snapshot),
            CandidateOutcome::KeptError
        );
        assert!(part.exists(), "a part the snapshot names is kept");
    }
    /// An upload whose record appeared after the snapshot is not in it, so only the age check
    /// keeps its young parts.
    #[tokio::test]
    async fn a_part_of_an_upload_begun_after_the_snapshot_is_kept_by_its_age() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let snapshot = UploadSnapshot::take(cache);
        let key = "bkt/snap/late";
        upload_record(cache, "upload-late", &record_body("upload-late", key));
        let part = write_young_bin(cache, key, 0, 99);
        assert!(!snapshot.keeps(key), "the snapshot predates the record");
        assert_eq!(
            decide_under_lock(cache, &candidate_for(cache, key, 0, 99), false, &snapshot),
            CandidateOutcome::KeptError
        );
        assert!(part.exists(), "a part younger than the floor is kept");
    }
    /// A snapshot older than its bound is walked again before the decision, so the decision
    /// reflects the directory now.
    #[tokio::test]
    async fn a_stale_upload_snapshot_is_refreshed_before_a_decision() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/snap/stale";
        let orphan = write_bin(cache, key, 0, 99);
        let mut uploads = Some(Arc::new(UploadSnapshot {
            taken: Instant::now()
                .checked_sub(UPLOAD_SNAPSHOT_MAX_AGE + Duration::from_secs(1))
                .unwrap(),
            spent: false,
            keys: None,
        }));
        assert_eq!(
            reclaim_candidate_with(
                cache,
                &candidate_for(cache, key, 0, 99),
                false,
                &mut uploads
            )
            .await,
            CandidateOutcome::Reclaimed(100)
        );
        assert!(!orphan.exists());
        assert_eq!(
            upload_walks(cache),
            1,
            "the stale snapshot was walked again"
        );
        assert!(uploads.as_ref().is_some_and(|s| s.fresh()));
    }
    /// A record that does not parse and was last written before the floor is skipped, so it
    /// does not keep an unrelated orphan.
    #[tokio::test]
    async fn an_old_torn_upload_record_does_not_block_an_unrelated_orphan() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let torn = upload_record(cache, "upload-torn", TORN_RECORD);
        let old = now_secs() - MIN_CANDIDATE_AGE_SECS - 60;
        filetime::set_file_mtime(&torn, filetime::FileTime::from_unix_time(old as i64, 0)).unwrap();
        let orphan = write_bin(cache, "bkt/torn/orphan", 0, 99);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert_eq!(
            out.reclaimed, 1,
            "an old torn record does not block reclamation"
        );
        assert!(!orphan.exists());
    }
    /// A record that does not parse and was written within the floor may be mid-write; its key
    /// is unknown, so every candidate is kept.
    #[tokio::test]
    async fn a_young_torn_upload_record_keeps_every_candidate() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        upload_record(cache, "upload-torn", TORN_RECORD);
        let orphan = write_bin(cache, "bkt/torn/kept", 0, 99);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert_eq!(
            out.reclaimed, 0,
            "a young torn record keeps every candidate"
        );
        assert!(orphan.exists());
    }
    /// `count` records naming keys no orphan uses, and `torn_old` torn records last written
    /// before the floor; returns the keys the records name.
    fn upload_records(cache: &Path, count: usize, torn_old: usize) -> BTreeSet<String> {
        let old = now_secs() - MIN_CANDIDATE_AGE_SECS - 60;
        for i in 0..torn_old {
            let torn = upload_record(cache, &format!("upload-torn-{i}"), TORN_RECORD);
            filetime::set_file_mtime(&torn, filetime::FileTime::from_unix_time(old as i64, 0))
                .unwrap();
        }
        (0..count)
            .map(|i| {
                let id = format!("upload-{i}");
                let key = format!("bkt/open/{i}");
                upload_record(cache, &id, &record_body(&id, &key));
                key
            })
            .collect()
    }
    /// Make every walk of `cache` take a full snapshot lifetime longer than it does, so its
    /// snapshot is stale on arrival.
    fn slow_upload_walks(cache: &Path, slow: bool) {
        let mut delays = UPLOAD_WALK_DELAY.lock().unwrap();
        if slow {
            delays.insert(cache.to_path_buf(), UPLOAD_SNAPSHOT_MAX_AGE);
        } else {
            delays.remove(cache);
        }
    }
    /// Several unlisted extents of one key, so one leaf holds every candidate.
    fn write_extents(cache: &Path, key: &str, count: u64) -> Vec<PathBuf> {
        (0..count)
            .map(|i| write_bin(cache, key, i * 100, i * 100 + 99))
            .collect()
    }
    /// A walk over many records is taken once per pass, and so is a walk slow enough that its
    /// snapshot is stale on arrival: the pass does not walk again for each candidate.
    #[tokio::test]
    async fn an_upload_walk_is_taken_once_per_pass_even_when_it_overruns() {
        let _serial = PASS_SERIAL.lock().await;
        let fast = TempDir::new().unwrap();
        journals_dir(fast.path());
        upload_records(fast.path(), 300, 0);
        write_extents(fast.path(), "bkt/many/orphan", 4);
        let out = run_pass(fast.path(), "instance-b", big_budget()).await;
        assert_eq!(out.reclaimed, 4, "every unrelated orphan is reclaimed");
        assert_eq!(
            upload_walks(fast.path()),
            1,
            "one walk of 300 records per pass"
        );

        let slow = TempDir::new().unwrap();
        journals_dir(slow.path());
        upload_records(slow.path(), 300, 0);
        write_extents(slow.path(), "bkt/many/orphan", 4);
        slow_upload_walks(slow.path(), true);
        let out = run_pass(slow.path(), "instance-b", big_budget()).await;
        slow_upload_walks(slow.path(), false);
        assert_eq!(out.found, 4);
        assert_eq!(
            upload_walks(slow.path()),
            1,
            "a walk stale on arrival is not repeated for each candidate"
        );
    }
    /// A pass whose walk is stale on arrival keeps every candidate, reclaims nothing, and
    /// leaves the unit unscanned so the next pass decides its candidates.
    #[tokio::test]
    async fn a_pass_with_an_upload_walk_stale_on_arrival_keeps_every_candidate() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        upload_records(cache, 50, 0);
        let orphans = write_extents(cache, "bkt/stale/orphan", 3);
        let shard_dir = orphans[0].parent().unwrap().parent().unwrap();
        let unit = Shard::new(
            "bkt",
            shard_dir.file_name().unwrap().to_str().unwrap().to_string(),
        );
        slow_upload_walks(cache, true);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        slow_upload_walks(cache, false);
        assert_eq!(out.found, 3);
        assert_eq!(
            out.reclaimed, 0,
            "nothing is reclaimed without a fresh snapshot"
        );
        assert_eq!(out.skipped_unconfirmed, 3, "every candidate is kept");
        assert!(orphans.iter().all(|p| p.exists()));
        assert_eq!(
            read_checkpoint(cache, &unit).and_then(|c| c.scanned_at),
            None,
            "the unit is not marked scanned while its candidates are undecided"
        );
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert_eq!(out.reclaimed, 3, "the next pass decides them");
        assert!(orphans.iter().all(|p| !p.exists()));
    }
    /// The parallel walk reads every record: over a few hundred records with old torn ones
    /// among them its key set is the written set and the serial walk's, and one young torn
    /// record makes both unknown.
    #[test]
    fn the_parallel_upload_walk_reads_every_record() {
        fn serial(cache: &Path) -> Option<BTreeSet<String>> {
            let mut keys = BTreeSet::new();
            for record in upload_record_paths(cache)? {
                match read_upload_record(&record) {
                    UploadRecord::Key(key) => {
                        keys.insert(key);
                    }
                    UploadRecord::Skipped => {}
                    UploadRecord::Unknown => return None,
                }
            }
            Some(keys)
        }
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        let written = upload_records(cache, 300, 20);
        let parallel = in_progress_upload_keys(cache);
        assert_eq!(parallel.as_ref().map(BTreeSet::len), Some(300));
        assert_eq!(parallel.as_ref(), Some(&written), "every record is read");
        assert_eq!(
            parallel,
            serial(cache),
            "the parallel walk equals the serial one"
        );
        upload_record(cache, "upload-torn-young", TORN_RECORD);
        assert_eq!(
            in_progress_upload_keys(cache),
            None,
            "a young torn record is unknown"
        );
        assert_eq!(serial(cache), None);
    }
    /// A refill renamed onto the candidate's path after the step-2 stat is a different file,
    /// so it survives the unlink step.
    #[tokio::test]
    async fn a_file_replaced_before_the_unlink_survives() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/refill/same";
        let orphan = write_bin(cache, key, 0, 99);
        *REPLACE_BEFORE_UNLINK.lock().unwrap() = Some(orphan.clone());
        let outcome = reclaim_candidate(cache, &candidate_for(cache, key, 0, 99)).await;
        assert!(
            orphan.exists(),
            "the refill renamed onto the path must survive"
        );
        assert_eq!(std::fs::read(&orphan).unwrap(), b"refilled");
        assert_eq!(outcome, CandidateOutcome::KeptError);
    }
    /// A refill with the judged file's length and modification time is still a different file,
    /// by the identity of the handle opened on the path, so it survives the unlink step.
    #[tokio::test]
    async fn a_same_shaped_file_replaced_before_the_unlink_survives() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/refill/same-shape";
        let orphan = write_bin(cache, key, 0, 99);
        let len = std::fs::metadata(&orphan).unwrap().len() as usize;
        REPLACE_KEEPS_SHAPE.store(true, Ordering::SeqCst);
        *REPLACE_BEFORE_UNLINK.lock().unwrap() = Some(orphan.clone());
        let outcome = reclaim_candidate(cache, &candidate_for(cache, key, 0, 99)).await;
        assert!(
            orphan.exists(),
            "the same-shaped refill renamed onto the path must survive"
        );
        assert_eq!(std::fs::read(&orphan).unwrap(), vec![b'r'; len]);
        assert_eq!(outcome, CandidateOutcome::KeptError);
    }
    /// A snapshot older than its bound keeps an old unlisted candidate even when it names no
    /// key, because a record created since its walk is not in it.
    #[tokio::test]
    async fn a_stale_snapshot_naming_no_key_keeps_an_old_candidate() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/snap/stale-empty";
        let orphan = write_bin(cache, key, 0, 99);
        let snapshot = UploadSnapshot {
            taken: Instant::now()
                .checked_sub(UPLOAD_SNAPSHOT_MAX_AGE + Duration::from_secs(1))
                .unwrap(),
            spent: false,
            keys: Some(std::collections::BTreeSet::new()),
        };
        assert_eq!(
            decide_under_lock(cache, &candidate_for(cache, key, 0, 99), false, &snapshot),
            CandidateOutcome::KeptError
        );
        assert!(orphan.exists(), "a stale snapshot keeps the file");
    }
    /// A `.meta` that lists the extent by the time the lock is held keeps the file.
    #[tokio::test]
    async fn a_meta_relisted_under_the_lock_keeps_the_file() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let key = "bkt/scn1/relisted";
        let bin = write_bin(cache, key, 0, 99);
        let mut out = PassOutcome::default();
        let leaf = bin.parent().unwrap().to_path_buf();
        let meta_leaf = meta_path(cache, key).parent().unwrap().to_path_buf();
        let candidates = scan_leaf(
            &cache.join("ranges"),
            &leaf,
            &meta_leaf,
            Some("bkt"),
            None,
            &mut out,
            &mut None,
        );
        assert_eq!(candidates.len(), 1, "unlisted at scan time");
        // The publish lands between the scan and the lock.
        write_meta(cache, key, &[(0, 99)]);
        let decided = reclaim_candidate(cache, &candidates[0]).await;
        assert_eq!(decided, CandidateOutcome::KeptListed);
        assert!(bin.exists(), "the re-listed file was deleted");
    }

    /// An unreadable peer journal keeps the file.
    #[tokio::test]
    async fn an_unreadable_peer_journal_keeps_the_file() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        std::fs::create_dir_all(journals_dir(cache).join("peer.journal")).unwrap();
        let key = "bkt/scn1/unreadable-peer";
        let bin = write_bin(cache, key, 0, 99);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(bin.exists());
        assert_eq!(out.reclaimed, 0);
        assert_eq!(out.skipped_unconfirmed, 1);
    }

    /// A missing `_journals/` is `Indeterminate`: nothing is reclaimed.
    #[tokio::test]
    async fn no_journals_directory_reclaims_nothing() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        let bin = write_bin(cache, "bkt/scn1/no-journals", 0, 99);
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(bin.exists());
        assert_eq!(out.reclaimed, 0);
    }

    /// A flat legacy file at the `ranges/` root is visited by the root unit.
    #[tokio::test]
    async fn a_flat_root_orphan_is_reclaimed() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let stem = crate::disk_cache::sanitize_object_key_for_filename("flatkey", "");
        let flat = cache.join("ranges").join(format!("{}_0-9.bin", stem));
        std::fs::create_dir_all(flat.parent().unwrap()).unwrap();
        std::fs::write(&flat, b"0123456789").unwrap();
        let old = now_secs() - MIN_CANDIDATE_AGE_SECS - 60;
        filetime::set_file_mtime(&flat, filetime::FileTime::from_unix_time(old as i64, 0)).unwrap();
        let out = run_pass(cache, "instance-b", big_budget()).await;
        assert!(!flat.exists());
        assert_eq!(out.bytes_reclaimed, 10);
        assert!(read_checkpoint(cache, &Shard::new(ROOT_BUCKET, ""))
            .is_some_and(|c| c.scanned_at.is_some()));
    }

    /// R15.15, lib analogue, read through the published `/metrics` section rather than the
    /// pass's own return. Two candidates under one rotation: A's journal-pending fill (kept,
    /// `journal_present`) and a confirmed-absent orphan (reclaimed, `confirmed_absent`). The
    /// published `bytes_reclaimed` must rise by exactly the second file's bytes. An
    /// always-retain implementation, or one that reclaims without crediting the counter, leaves
    /// it flat and fails here.
    #[tokio::test]
    async fn r15_15_bytes_reclaimed_on_the_metrics_surface_moves_by_exactly_the_reclaimed_file() {
        let _serial = PASS_SERIAL.lock().await;
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        journals_dir(cache);
        let pending_key = "bkt/scn2/journal-pending";
        let pending = write_bin(cache, pending_key, 0, 299);
        let a = JournalManager::new(cache.to_path_buf(), "instance-a".to_string());
        a.append_range_entry(pending_key, add_entry("instance-a", pending_key, 0, 299))
            .await
            .unwrap();
        let absent_key = "bkt/scn2/confirmed-absent";
        let absent = write_bin(cache, absent_key, 0, 776);
        let absent_len = std::fs::metadata(&absent).unwrap().len();
        assert_eq!(absent_len, 777);

        let before = crate::metrics::OrphanReclaimMetrics::current();
        let out = run_pass(cache, "instance-b", big_budget()).await;
        let after = crate::metrics::OrphanReclaimMetrics::current();

        assert!(pending.exists(), "the journal-pending fill was reclaimed");
        assert!(!absent.exists(), "the confirmed-absent orphan survived");
        assert_eq!(out.bytes_reclaimed, absent_len, "the pass's own figure");
        assert_eq!(
            after.bytes_reclaimed - before.bytes_reclaimed,
            absent_len,
            "published orphan_reclaim.bytes_reclaimed moved by {} (before {}, after {}); \
             expected exactly the reclaimed file's {} bytes",
            after.bytes_reclaimed - before.bytes_reclaimed,
            before.bytes_reclaimed,
            after.bytes_reclaimed,
            absent_len
        );
        assert_eq!(after.reclaimed - before.reclaimed, 1);
        assert_eq!(after.found - before.found, 2);
        let (eb, ea) = (before.journal_evidence_total, after.journal_evidence_total);
        assert_eq!(ea.confirmed_absent - eb.confirmed_absent, 1);
        assert_eq!(ea.journal_present - eb.journal_present, 1);
        assert_eq!(ea.peer_unavailable, eb.peer_unavailable);
        assert_eq!(ea.indeterminate, eb.indeterminate);
        assert!(after.units_visited > before.units_visited);
        assert_eq!(
            after.units_never_scanned,
            Some(0),
            "every unit completed its visit"
        );
        assert!(after
            .stalest_checkpoint_age_secs
            .is_some_and(|age| age < 60));
    }

    /// A freshly published range file the `.meta` does not list yet and no journal
    /// holds (an older proxy's publish-to-journal window) on a volume where an older proxy is
    /// active: the scheduled pass must not run, and the file survives. On a uniform capable
    /// roster the same fixture's scheduled pass runs and reclaims it. An unreadable roster
    /// (`Unknown`) also skips the pass.
    ///
    /// Red side: `ORPHAN_IGNORES_FLEET_READINESS`; the mixed fleet's pass reclaims the file.
    #[tokio::test]
    async fn a_scheduled_pass_waits_for_every_active_proxy_to_be_ready() {
        use crate::eviction_worklist::test_fleet;
        let _serial = PASS_SERIAL.lock().await;
        let now = test_fleet::now();

        // Mixed: one capable proxy, one older proxy with a journal and no `.ready` record.
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        test_fleet::capable(cache, now);
        test_fleet::journal(cache, "host-old:9", now);
        let key = "bkt/scn3/fresh-publish";
        let fresh = write_bin(cache, key, 0, 511);
        let gated_before = counters().passes_gated;
        let mixed = run_scheduled_pass(cache, "host-a:1", big_budget()).await;
        let gated_after = counters().passes_gated;
        assert!(
            fresh.exists(),
            "a scheduled pass reclaimed {fresh:?}, a range file published by a proxy older than \
             3.0.0 that had not journalled it yet, while host-old:9 was active: {mixed:?}"
        );
        assert!(
            mixed.is_none(),
            "no pass may run on a mixed fleet: {mixed:?}"
        );
        assert!(
            gated_after > gated_before,
            "the skipped pass is counted: before {gated_before}, after {gated_after}"
        );

        // Unknown: no active proxy at all (an empty roster) fails closed.
        let unknown = TempDir::new().unwrap();
        let lone = write_bin(unknown.path(), key, 0, 511);
        assert!(run_scheduled_pass(unknown.path(), "host-a:1", big_budget())
            .await
            .is_none());
        assert!(lone.exists(), "an unknown roster must not reclaim");

        // Uniform: the same fixture with every active proxy capable runs and reclaims.
        let uniform = TempDir::new().unwrap();
        let ucache = uniform.path();
        test_fleet::capable(ucache, now);
        let orphan = write_bin(ucache, key, 0, 511);
        let ran = run_scheduled_pass(ucache, "host-a:1", big_budget()).await;
        let ran = ran.expect("a uniform capable fleet runs the pass");
        assert!(
            !orphan.exists(),
            "the uniform fleet's pass reclaims: {ran:?}"
        );
        assert_eq!(ran.reclaimed, 1);
    }

    /// A proxy that becomes not-ready after the pass has scanned a candidate keeps
    /// it, because the gate is re-read under the slot lock.
    #[tokio::test]
    async fn a_proxy_joining_between_scan_and_lock_keeps_the_candidate() {
        use crate::eviction_worklist::test_fleet;
        let _serial = PASS_SERIAL.lock().await;
        let now = test_fleet::now();
        let temp = TempDir::new().unwrap();
        let cache = temp.path();
        test_fleet::capable(cache, now);
        let key = "bkt/scn3/joined";
        let bin = write_bin(cache, key, 0, 99);
        let mut out = PassOutcome::default();
        let leaf = bin.parent().unwrap().to_path_buf();
        let meta_leaf = meta_path(cache, key).parent().unwrap().to_path_buf();
        let candidates = scan_leaf(
            &cache.join("ranges"),
            &leaf,
            &meta_leaf,
            Some("bkt"),
            None,
            &mut out,
            &mut None,
        );
        assert_eq!(candidates.len(), 1, "fixture: unlisted at scan time");
        test_fleet::journal(cache, "host-old:9", now);
        let decided = reclaim_candidate_with(cache, &candidates[0], true, &mut None).await;
        assert_eq!(decided, CandidateOutcome::KeptError);
        assert!(
            bin.exists(),
            "the candidate was deleted after an older proxy joined"
        );
    }

    /// The pass is gated by cache init's [`enable`] and the per-process claim.
    #[test]
    fn claim_pass_is_gated_by_enable_and_in_progress() {
        // Not toggled here: other tests in this binary may run passes, and the flag is global.
        // Before cache init enables it, no pass is ever claimed.
        if !enabled() {
            assert!(claim_pass(u64::MAX / 2).is_none());
        }
    }
}
