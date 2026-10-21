//! The staged figure's **activation epoch**, the `Schema_Fence`, and
//! `Foreign_Write_Detection`.
//!
//! Spec: cache-eviction-at-scale § 16.6.6.
//!
//! # What this module decides
//!
//! One bit: whether the fold-derived staged figure is **authoritative** on this volume.
//! Everything that changed behaviour in decision C is gated on that bit, and the bit is a
//! document on shared storage rather than a build-time constant, because a fleet upgrades
//! one node at a time and the two phases have to coexist.
//!
//! | Phase | Epoch | `write_cache_size`'s producer | `Graduation` entries | `total_cache_size` |
//! |---|---|---|---|---|
//! | **L** (legacy) | absent or `0` | the event-sourced incumbent, exactly as before | applied as debits | `total_size` |
//! | **Fold** | `1` | `F + post-cut pending + P`, from the fold | parsed, applied as a **no-op**, counted, retired | `total_size + P` |
//!
//! Phase L is the default on first boot of a C-capable binary, which is what makes a mixed
//! fleet safe: every node — old and new — is reading and writing the incumbent figure, so
//! there is exactly one authority and it is the one that existed before.
//!
//! # The harmful ordering this exists to block
//!
//! A new node folds the current `.meta` state (a graduated object already excluded from
//! `F`), an old node then consolidates that object's still-pending `Graduation` entry and
//! subtracts those bytes from the new **absolute** figure. The result is an
//! **under**-count, which *suppresses* staging eviction — the direction that fills a volume
//! rather than the direction that wastes space. § 16.6.2's whole direction-safety argument
//! is that an over-count is tolerable and an under-count is not, so this ordering is the
//! one mixed-version hazard that had to be closed rather than bounded.
//!
//! Three mechanisms, in decreasing strength:
//!
//! 1. **The activation gate** (`activation_assessment_current`) — evidence, not a time window. The
//!    epoch cannot flip while any instance that has appended a staged credit has not also
//!    published a `.ready` record. An old binary cannot hide from that check, because
//!    appending a credit *is* what creates its ledger file.
//! 2. **`Schema_Fence`** ([`SIZE_STATE_SCHEMA_FOLD`]) — activation bumps
//!    `size_state.json`'s schema, so a pre-C binary cannot read the figure it would
//!    otherwise subtract from. **Its efficacy is conditional; see that constant's doc.**
//! 3. **[`Foreign_Write_Detection`](detect_foreign_size_state)** — does not catch a pre-C
//!    consolidation cycle. Both binaries write `size_state.json` under
//!    `locks/global_consolidation.lock`, so the re-applied fence always survives the re-read
//!    and the rewrite is filed as this instance's own unapplied bump. It fires only if a
//!    pre-C out-of-cycle writer, holding `size_state.lock` alone, lands between the bump and
//!    the re-read.
//!
//! # Rollback rule
//!
//! **Once the epoch is `1`, running a pre-C binary against the same cache volume is not
//! supported**, for any node and for any length of time. The supported rollback is the
//! fresh-volume path in `docs/UPGRADING.md#rolling-back-from-300`: stop every proxy, point
//! `cache_dir` at a fresh, empty volume, and start the older release there.
//!
//! An in-place variant has been sketched (stop the entire fleet, reset
//! `staged_authority_epoch.json` to `0`, delete `size_state.json` so the old binary
//! re-grounds `write_cache_size` from its own validation scan, start the old binaries).
//! **It is untested and unsupported**: whether a pre-C binary then handles the already
//! no-op'd `Graduation` history, the `_shard_state/` and `_write_ledger/` trees, and the
//! conditional [`SIZE_STATE_SCHEMA_FOLD`] fence correctly has not been established. Do not
//! document it to operators.
//!
//! [`epoch_latched`] latches at `1` for the process lifetime because of this rule: a running
//! process never needs to observe `1 → 0`, since any document edit that produced it would
//! require the fleet to have been stopped first.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Directory under `metadata/` holding shard state, per design § 3.2. Same literal as
/// [`crate::staged_fold`]'s; duplicated rather than shared because three modules already
/// carry their own copy (`capacity_guard`, `eviction_result`, `staged_fold`) and a fourth
/// is cheaper than a cross-module constant whose only purpose is a string.
const SHARD_STATE_DIR: &str = "_shard_state";

/// Filename of the epoch document.
const EPOCH_FILE: &str = "staged_authority_epoch.json";

/// Directory under `_shard_state/` holding the per-instance `.ready` records.
const READY_DIR: &str = "ready";

/// Extension for a `.ready` record.
const READY_EXT: &str = "ready";

/// Schema of the epoch and `.ready` documents. An unknown schema is treated as **blocking**
/// rather than ignored: a record this binary cannot fully understand is not evidence that
/// its author is safe to activate alongside.
pub(crate) const AUTHORITY_DOC_SCHEMA: u32 = 1;

/// **Phase L.** The fold publishes gauges; the event-sourced incumbent is the authority.
/// This is the value an absent or unparseable document reads as, which is the safe default:
/// it is the behaviour that existed before decision C.
pub const EPOCH_LEGACY: u64 = 0;

/// **Phase Fold.** The fold is authoritative and `Graduation` is a cleanup-only no-op.
pub const EPOCH_FOLD: u64 = 1;

/// `size_state.json`'s schema **before** activation: absent, i.e. `0` under
/// `#[serde(default)]`.
///
/// A C-capable binary deliberately keeps writing this value throughout Phase L, so that a
/// node can still be rolled back to a pre-C binary during the upgrade window. The bump is
/// part of the activation transaction and nothing else.
pub const SIZE_STATE_SCHEMA_LEGACY: u32 = 0;

/// `size_state.json`'s schema **after** activation — the `Schema_Fence`.
///
/// # THE FENCE'S EFFICACY IS CONDITIONAL, AND THE CONDITION IS NOT TESTABLE FROM HERE
///
/// The fence blocks the harmful mixed-version ordering **if and only if the real 2.8.0
/// binary rejects an unknown `size_state.json` schema** rather than parsing it
/// permissively. That is a property of an artefact this code cannot inspect: no amount of
/// new code establishes how an already-built binary's deserializer behaves. Serde's default
/// for an unknown *extra* field is in fact to **ignore** it, so the pessimistic reading is
/// the likely one — a pre-C `SizeState` has no `schema` field, will not have
/// `deny_unknown_fields`, and may therefore parse this document happily and subtract from
/// the figure exactly as if the fence were not there.
///
/// **Where it gets tested:** task 31 step 1e of `cache-eviction-at-scale`, against the real
/// 2.8.0 artefact, at the Phase 4 fleet gate. Phase 1 of this workstream reported the
/// property UNSATISFIED for precisely this reason, and design § 16.6.11 departure 3 records
/// it as an open risk to be reported at the release gate rather than described as closed.
///
/// **So do not trust this fence further than it deserves.** 2.8.0 and 2.8.4 are permissive:
/// both parsed the fenced document and rewrote it without the field. What remains is
/// `activation_assessment_current`'s evidence gate (prevention) and the stated rollback
/// prohibition (procedure). Detection ([`detect_foreign_size_state`]) does not cover the
/// pre-C consolidation path. That is a materially weaker position than prevention alone, and
/// it must be reported as such.
pub const SIZE_STATE_SCHEMA_FOLD: u32 = 2;

/// Age past which a `fold_capable` `.ready` record is **retired** from the activation verdict,
/// seconds. Design § 16.6.6, 2026-09-25 amendment, B4.
///
/// # Why a record ever stops counting
///
/// Instance identity is `{hostname}:{pid}`, so every restart mints a new id, and nothing retires
/// the old one's record. A process that exits before its record reads all true leaves it frozen
/// at its last publish, and the gate used to check that frozen copy for ever. A retired record
/// still satisfies precondition 1 for its instance — it proved that instance ran C-capable code,
/// which is all precondition 1 exists to establish — but its per-record preconditions are not
/// evaluated.
///
/// # Why this value
///
/// [`crate::ledger_repair::MAX_REPAIR_AGE_SECS`] (24 h) is the longest window any record field
/// is evaluated against, so a record older than it asserts nothing about the current rotations
/// that a fresh record, reading the same shared volume, does not re-derive. Every precondition
/// field except `fold_incomplete_total` is volume-derived. It must also be at least
/// [`crate::staged_fold::MAX_AUTHORITY_AGE_SECS`]: that is what guarantees a retired record's
/// `fold_incomplete_total` observation predates every shard's latest Complete_Install whenever a
/// fresh record asserts `rotation_complete`. Checked at compile time below.
///
/// Not configurable, on the same no-knob reasoning as 3b's window: this is a gate, and a knob
/// would let an operator satisfy it by widening or narrowing until the answer changed.
pub const READY_RECORD_RETIRE_AGE_SECS: u64 = crate::ledger_repair::MAX_REPAIR_AGE_SECS;

const _: () = assert!(
    READY_RECORD_RETIRE_AGE_SECS >= crate::staged_fold::MAX_AUTHORITY_AGE_SECS,
    "a record must not retire before a full fold rotation window has passed"
);

/// How far past a retiring record's `written_at` its instance's latest ledger append may lie
/// before the record stops vouching for the instance, seconds. Design § 16.6.6 B7.
///
/// # Not a clock-skew allowance
///
/// Both figures are the same host's wall clock: an id is `{hostname}:{pid}`, so a pid-reusing
/// writer runs where the record was written, and an entry `timestamp` is stamped by the appending
/// process. What the margin covers is a C-capable process's OWN appends after its last
/// publication. A record is refreshed only on a non-idle consolidation cycle whose lock attempt
/// this instance wins, so a live writer's last append can post-date its last `written_at`, and
/// with no margin every such process would block the gate for ever once it died. An hour is 720
/// consolidation intervals at the 5 s default, and it also absorbs a backward clock step of up to
/// an hour on one host.
///
/// The price is the escape it leaves: a pre-C writer whose every surviving entry lies within an
/// hour of the dead record passes. Not configurable, on the same reasoning as
/// [`READY_RECORD_RETIRE_AGE_SECS`].
pub const READY_RECORD_APPEND_MARGIN_SECS: u64 = 3_600;

/// How long every ledger file named for an id must have gone unmodified before an id that no
/// `.ready` record vouches for stops counting against activation, seconds. Design § 16.6.6 B8;
/// Ed's ruling of 2026-09-25 on `ED_QUESTION_2026_09_25_RECORDLESS_LEDGER_IDS`.
///
/// # What it weakens, stated because Ed accepted it knowingly
///
/// A record-less id is either a dead process or a live idle pre-C binary, and its files cannot
/// tell the two apart. After one quiet day it is treated as the first. If it was the second and it
/// writes again **before** activation, its append moves the file's mtime and it blocks again at
/// once. If it writes again **after** activation, nothing re-evaluates the gate, and
/// `Foreign_Write_Detection` ([`detect_foreign_size_state`]) does not catch its consolidation
/// rewrites either. It runs unreported until it is stopped.
///
/// # Why mtime
///
/// See [`crate::write_ledger::WriteLedger::latest_modified_in`]: the population is header-only
/// files, where an entry timestamp says nothing, and mtime is never earlier than the last append.
///
/// Not configurable, on the same reasoning as [`READY_RECORD_RETIRE_AGE_SECS`]; an operator who
/// needs an id gone sooner retires it explicitly (`s3-proxy retire-instance`).
pub const RECORDLESS_QUIET_RETIRE_SECS: u64 = 86_400;

/// How long a `LEGACY` reading is cached before the document is re-read.
///
/// Only the `LEGACY` reading is cached; `FOLD` latches permanently (see [`epoch_latched_for`]).
/// Matched to the consolidation cycle rather than to the fold interval so activation on a
/// peer becomes visible here within seconds rather than within five minutes.
const EPOCH_RECHECK_INTERVAL_SECS: u64 = 5;

/// One cache directory's cached epoch reading.
#[derive(Debug, Clone, Copy)]
struct EpochCell {
    epoch: u64,
    checked_at_secs: u64,
}

/// The per-cache-directory epoch cache.
///
/// # Keyed by directory rather than process-global, and it is not only for tests
///
/// The obvious implementation is one `AtomicU64` for the process, since a proxy serves one
/// cache volume. Two reasons not to:
///
/// - **It would make the latch a shared mutable global across tests.** Lib tests run as
///   threads of one process, so a single test that activated would silently flip the epoch
///   for every test running beside it — including the ones asserting Phase L behaviour. That
///   is not a testing inconvenience; it is a class of nondeterminism that this repo has
///   repeatedly found hiding wrong assertions.
/// - **Every caller already has the directory in hand**, so keying costs a hash lookup and
///   buys the property honestly. `apply_journal_entries` has `self.cache_dir`,
///   `collect_write_cache_metrics` has `cache_manager.get_cache_dir()`, and
///   `CacheManager::staged_authority` has `self.cache_dir`.
///
/// A `BTreeMap` under a `std::sync::RwLock` rather than anything cleverer: reads dominate
/// overwhelmingly, entries are bounded by the number of cache directories a process serves
/// (one, in production), and the lookup sits beside per-entry JSON parsing that costs orders
/// of magnitude more.
static EPOCH_STATE: std::sync::RwLock<std::collections::BTreeMap<PathBuf, EpochCell>> =
    std::sync::RwLock::new(std::collections::BTreeMap::new());

/// `staged_authority_foreign_write_total` — pre-C rewrites of `size_state.json` observed
/// after activation. Per-instance, like every other `/metrics` counter, so a process-global
/// counter is the right shape here.
static FOREIGN_WRITE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Cache directories whose authority is currently blocked, and **why**.
///
/// Read by the fold as a Complete_Install precondition. A map rather than a set, and that is
/// the whole of the fix for the misattribution described on [`AuthorityBlockKind`]: the fold
/// needs one bit ("may I install?"), but the operator needs to know which of three unrelated
/// situations produced it, and a set can only carry the bit.
///
/// Keyed by directory for the same reason [`EPOCH_STATE`] is.
static AUTHORITY_BLOCK: std::sync::RwLock<std::collections::BTreeMap<PathBuf, AuthorityBlockKind>> =
    std::sync::RwLock::new(std::collections::BTreeMap::new());

/// Why the staged authority is currently blocked on a volume.
///
/// # Three situations that used to produce one message, and only one of them was true
///
/// Before this enum, every one of these armed the same flag and emitted the same WARN —
/// "a binary that predates decision C has rewritten it" — because all three present as a
/// `size_state.json` that does not carry the fence. Two of the three have nothing to do with a
/// pre-C binary, and in the common case no pre-C binary existed anywhere on the fleet:
///
/// - **A transient read failure.** `get_size_state` swallows any load error into
///   `SizeState::default()`, whose `schema`/`staged_authority_epoch` are `0`/`0` — byte-for-byte
///   the pre-C shape. One unreadable read therefore accused a binary that was not there.
/// - **Our own fence bump never landed.** If the bump failed at activation the epoch is `1`
///   while `size_state.json` still carries `0`/`0`, written by *us*. Indistinguishable from a
///   foreign write by content alone, and behaviour separates it only partly: the fence is
///   re-applied and the document re-read. If it sticks, either it was ours and unapplied or a
///   pre-C consolidation cycle rewrote it between our writes; the two look the same, because a
///   pre-C consolidation cycle writes under the same global lock. Only a document that still
///   lacks the fence after a successful re-apply implies a second writer.
/// - **A genuine pre-C write**, which is the only case the original message described.
///
/// All three fail safe, so the cost was never a wrong figure — it was that an operator could
/// not tell which of three remedies applied, and the message named the one remedy (stop the
/// fleet, per the rollback rule) that is most expensive and least often correct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityBlockKind {
    /// `size_state.json` was read successfully, carries a pre-C schema/epoch, and **still did
    /// after this instance re-applied the fence**. Something else is writing it.
    ForeignPreCWrite,
    /// `size_state.json` could not be read or parsed, so whether the authority is contested is
    /// unknown. Not an accusation.
    SizeStateUnreadable,
    /// The `Schema_Fence` is not applied and this instance's attempt to re-apply it failed.
    /// Self-inflicted, recoverable, and nothing else is implicated.
    FenceReapplyFailed,
}

impl AuthorityBlockKind {
    /// Short stable label for logs and the retain reason.
    pub fn label(&self) -> &'static str {
        match self {
            Self::ForeignPreCWrite => "foreign_pre_c_write",
            Self::SizeStateUnreadable => "size_state_unreadable",
            Self::FenceReapplyFailed => "fence_reapply_failed",
        }
    }
}

/// The epoch document.
///
/// Small, and written under the **global consolidation lock** by [`activate`], which is the
/// only writer. The extra fields exist so an operator can see who activated and when
/// without correlating logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityEpochDoc {
    /// Document schema. Unknown ⇒ treated as unreadable ⇒ [`EPOCH_LEGACY`].
    pub schema: u32,
    /// [`EPOCH_LEGACY`] or [`EPOCH_FOLD`].
    pub epoch: u64,
    /// Unix seconds at which the epoch last changed.
    pub activated_at: u64,
    /// The instance that flipped it.
    pub activated_by: String,
    /// The binary version that flipped it, for the operator rather than for any check.
    pub binary_version: String,
}

/// A per-instance readiness assertion, published to shared storage.
///
/// This is the evidence `activation_assessment_current` gates on. It exists so activation is a
/// statement about **observed fleet state** rather than about elapsed time — the previous
/// design's rule was "upgrade all nodes within hours", which is unfalsifiable and was
/// deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyRecord {
    /// Document schema. Unknown ⇒ blocks activation.
    pub schema: u32,
    /// The instance publishing it. Must match a `{instance}.ledger` stem for the
    /// correspondence check to pass.
    pub instance_id: String,
    /// This binary's version. **Recorded for the operator, not compared.** See
    /// [`ReadyRecord::fold_capable`] for why a version comparison cannot be the gate.
    pub binary_version: String,
    /// Whether the publishing binary implements decision C at all.
    ///
    /// # Why this is the check and a version comparison is not
    ///
    /// § 16.6.6's second precondition is "no `.ready` record names a pre-C version". A
    /// literal version comparison cannot implement it here: the public version is bumped at
    /// the **release**, not at the unit that introduces the behaviour, so a C-capable
    /// binary and a pre-C binary can and do report the same `CARGO_PKG_VERSION` during
    /// development. Comparing versions would therefore either block activation on a
    /// perfectly capable fleet or admit an incapable one, depending on where the bump had
    /// reached.
    ///
    /// So the precondition is satisfied structurally instead: **a pre-C binary publishes no
    /// `.ready` record at all**, because it has no code to do so. Its ledger file, however,
    /// exists the moment it appends a staged credit. The load-bearing precondition is
    /// therefore the ledger↔ready **correspondence** in `activation_assessment_current`, and this
    /// field is the belt to that braces — it catches a future binary that publishes records
    /// while deliberately not implementing the fold.
    pub fold_capable: bool,
    /// Whether this instance has observed one full fold rotation with a Complete_Install on
    /// every shard it knows about, within [`crate::staged_fold::MAX_AUTHORITY_AGE_SECS`].
    pub rotation_complete: bool,
    /// This instance's `staged_fold_incomplete_total` at publication. Non-zero blocks.
    pub fold_incomplete_total: u64,
    /// Whether the **ledger-completeness repair** rotation has covered every observed shard
    /// within [`crate::ledger_repair::MAX_REPAIR_AGE_SECS`] — § 16.6.6 precondition **3b**.
    ///
    /// # This is a different rotation from [`Self::rotation_complete`], and that was the bug
    ///
    /// Precondition 3 has two clauses and they are about two different mechanisms.
    /// [`Self::rotation_complete`] plus [`Self::fold_incomplete_total`] together implement the
    /// first — "`staged_fold_incomplete_total` has been zero for one full rotation" — which is
    /// a statement about the **fold**. This field implements the second — "ledger-completeness
    /// repair has completed one rotation by range identity" — which is a statement about
    /// whether the ledger the fold reads is known to be **complete with respect to the disk**.
    ///
    /// The fold's own rotation cannot substitute for it. The fold's candidate set comes only
    /// from the ledger, so a staged `.meta` that no entry names is absent from every fold
    /// figure while every fold reports a flawless rotation. See [`crate::ledger_repair`].
    ///
    /// # `#[serde(default)]` gives `false`, which blocks, which is the right default
    ///
    /// A record written by a binary that predates this field deserialises with `false` and so
    /// blocks activation. That is deliberate: such a binary ran no repair rotation, so there is
    /// no evidence for 3b and the gate must not infer any.
    ///
    /// **Do not express this by bumping [`AUTHORITY_DOC_SCHEMA`] instead.** That constant also
    /// versions [`AuthorityEpochDoc`], and an unknown epoch-document schema reads as
    /// [`EPOCH_LEGACY`] — so bumping it would silently de-activate a volume that had already
    /// flipped, reverting the authority to the incumbent with no log saying so. The
    /// `#[serde(default)]` route blocks a *pending* activation; the schema route would undo a
    /// *completed* one.
    #[serde(default)]
    pub repair_rotation_complete: bool,
    /// Age of the stalest completed repair sweep, seconds, or `None` when unreadable or when
    /// some shard has never been swept.
    ///
    /// **Recorded for the operator, not compared.** [`Self::repair_rotation_complete`] is the
    /// gate; this is what lets someone watch 3b converge rather than only being told activation
    /// is blocked. Gating on the number as well would be a second definition of the same
    /// predicate.
    #[serde(default)]
    pub repair_stalest_age_secs: Option<u64>,
    /// Whether the publishing binary contains the parallel eviction work list (DR-EVICT,
    /// `cache-eviction-at-scale` task 44). It follows the binary alone, which is the whole
    /// truth: there is no config switch, so every binary with the work list runs its executor.
    ///
    /// Read by `eviction_worklist::fleet_is_worklist_capable`: the author may hand
    /// deletion to executors only when every active instance can execute a chunk.
    ///
    /// # `#[serde(default)]` gives `false`, which is the safe direction
    ///
    /// A record written by a binary that predates this field deserialises with `false`, so an
    /// older proxy reads as not capable and the author keeps deleting itself. The field is
    /// unknown to such a binary too (no `deny_unknown_fields`), so the record stays readable in
    /// both directions. Do not bump `AUTHORITY_DOC_SCHEMA` for it; see
    /// [`Self::repair_rotation_complete`] for why that constant is not available.
    #[serde(default)]
    pub evict_worklist_capable: bool,
    /// Unix seconds of publication.
    pub written_at: u64,
}

/// Why activation is not permitted yet.
///
/// Each variant is a distinct operator action, which is the point of not collapsing them
/// into a bool: "wait" and "a node is not upgraded" are different situations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationBlocker {
    /// Already at [`EPOCH_FOLD`]. Not a failure.
    AlreadyActive,
    /// The `ready/` directory could not be listed, or a record could not be read. Absence
    /// of evidence is not evidence, so this blocks.
    ReadyRecordsUnreadable,
    /// No instance has published a record. On a fresh volume this is the normal state and
    /// clears once a rotation completes.
    NoReadyRecords,
    /// Every `.ready` record on the volume is **retired** — older than
    /// [`READY_RECORD_RETIRE_AGE_SECS`] — so no record carries current evidence. Retired records
    /// still vouch that their instance runs C-capable code, but activation needs at least one
    /// current record asserting the preconditions, and there is none.
    ///
    /// Normally unreachable in production, because the evaluating instance publishes its own
    /// record immediately before evaluating. Seen when that publication failed.
    NoFreshReadyRecords,
    /// An instance has a ledger file — so it has appended a staged credit — and has
    /// published no `.ready` record. **This is the check a pre-C binary cannot evade.**
    LedgerWithoutReadyRecord(String),
    /// A record carries a schema this binary does not know, or asserts it is not
    /// fold-capable.
    NotFoldCapable(String),
    /// An instance has not completed a full rotation.
    RotationIncomplete(String),
    /// An instance has seen an incomplete fold input.
    FoldIncompleteObserved(String),
    /// An instance cannot show that the **ledger-completeness repair** rotation has covered
    /// every shard — § 16.6.6 precondition 3b.
    ///
    /// A distinct blocker from [`Self::RotationIncomplete`] because the operator action is
    /// different: that one means "the fold is not keeping up", this one means "we cannot yet
    /// show the ledger the fold reads is complete with respect to the disk". Collapsing them
    /// would send an operator to the wrong gauge.
    RepairRotationIncomplete(String),
}

impl ActivationBlocker {
    /// The instance this blocker names, if it names one.
    ///
    /// `None` for the volume-wide blockers ([`Self::AlreadyActive`],
    /// [`Self::ReadyRecordsUnreadable`], [`Self::NoReadyRecords`]).
    pub fn instance(&self) -> Option<&str> {
        match self {
            Self::AlreadyActive
            | Self::ReadyRecordsUnreadable
            | Self::NoReadyRecords
            | Self::NoFreshReadyRecords => None,
            Self::LedgerWithoutReadyRecord(id)
            | Self::NotFoldCapable(id)
            | Self::RotationIncomplete(id)
            | Self::FoldIncompleteObserved(id)
            | Self::RepairRotationIncomplete(id) => Some(id),
        }
    }

    /// A short, stable label for logs and metrics.
    pub fn label(&self) -> &'static str {
        match self {
            Self::AlreadyActive => "already_active",
            Self::ReadyRecordsUnreadable => "ready_records_unreadable",
            Self::NoReadyRecords => "no_ready_records",
            Self::NoFreshReadyRecords => "no_fresh_ready_records",
            Self::LedgerWithoutReadyRecord(_) => "ledger_without_ready_record",
            Self::NotFoldCapable(_) => "not_fold_capable",
            Self::RotationIncomplete(_) => "rotation_incomplete",
            Self::FoldIncompleteObserved(_) => "fold_incomplete_observed",
            Self::RepairRotationIncomplete(_) => "repair_rotation_incomplete",
        }
    }
}

/// The activation gate's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationVerdict {
    /// Every precondition of § 16.6.6 holds.
    Permitted,
    /// Named reason it does not.
    Blocked(ActivationBlocker),
}

/// `{cache_dir}/metadata/_shard_state`.
fn shard_state_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("metadata").join(SHARD_STATE_DIR)
}

/// `{cache_dir}/metadata/_shard_state/staged_authority_epoch.json`.
pub(crate) fn epoch_path(cache_dir: &Path) -> PathBuf {
    shard_state_dir(cache_dir).join(EPOCH_FILE)
}

/// `{cache_dir}/metadata/_shard_state/ready`.
pub(crate) fn ready_dir(cache_dir: &Path) -> PathBuf {
    shard_state_dir(cache_dir).join(READY_DIR)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Write `body` to `path` atomically through an instance-specific temp path.
///
/// Instance-specific because this is shared storage and a fixed temp name is the
/// fleet-wide collision § 16.6.5 spent a whole sub-section on. Same `{name}.tmp.{host}.{pid}`
/// scheme the staged fold's checkpoint install uses.
///
/// The sibling type is deliberately not named here: `staged_fold`'s
/// `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface` is a plain
/// substring search over `src/` that does not exempt comments, on purpose, so it cannot be
/// talked out of. Naming a gauge type in prose would read as a decision path consuming it.
fn install_atomic(path: &Path, body: &str) -> std::io::Result<()> {
    let tmp = write_instance_tmp(path, body)?;
    std::fs::rename(&tmp, path)
}

/// Write `body` to `path`'s instance-specific temp sibling, `{name}.tmp.{host}.{pid}`, synced,
/// creating the parent directory. Returns the temp path.
fn write_instance_tmp(path: &Path, body: &str) -> std::io::Result<PathBuf> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let host = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
        .replace(['/', '\\'], "_");
    let tmp = path.with_file_name(format!(
        "{}.tmp.{}.{}",
        path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "doc".to_string()),
        host,
        std::process::id()
    ));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    Ok(tmp)
}

/// Install `body` at `path` only if nothing is there: `Ok(true)` when this call created it,
/// `Ok(false)` when a document already existed.
///
/// A hard link from the instance-specific temp file, so the document appears whole or not at
/// all and an existing one is never replaced. Where the filesystem refuses hard links, falls back
/// to an existence check and a rename, which can replace a document installed between the two;
/// for the one caller, that replaces a real readiness record with a startup record until the next
/// publish, which blocks nobody under design § 16.6.6 B10.2.
fn install_if_absent(path: &Path, body: &str) -> std::io::Result<bool> {
    let tmp = write_instance_tmp(path, body)?;
    let outcome = match std::fs::hard_link(&tmp, path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(_) if path.exists() => Ok(false),
        Err(_) => {
            return std::fs::rename(&tmp, path).map(|()| true);
        }
    };
    let _ = std::fs::remove_file(&tmp);
    outcome
}

/// Read the epoch document from disk, ignoring the latch.
///
/// `Ok(None)` means no document exists — Phase L on a volume that has never activated.
/// `Err` means one exists and could not be read or carries an unknown schema, which callers
/// resolve to [`EPOCH_LEGACY`]: refusing to fold-derive on an unreadable epoch keeps the
/// incumbent authority, which is the direction-safe answer.
pub(crate) fn load_epoch_doc(cache_dir: &Path) -> std::io::Result<Option<AuthorityEpochDoc>> {
    let path = epoch_path(cache_dir);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let doc: AuthorityEpochDoc = serde_json::from_str(&content)
        .map_err(|e| std::io::Error::other(format!("unparseable staged authority epoch: {e}")))?;
    if doc.schema != AUTHORITY_DOC_SCHEMA {
        return Err(std::io::Error::other(format!(
            "unknown staged authority epoch schema {}",
            doc.schema
        )));
    }
    Ok(Some(doc))
}

/// The epoch for this volume, refreshing the process latch.
///
/// Cheap on the hot path: once the answer is [`EPOCH_FOLD`] this performs **no I/O at all**,
/// and while it is [`EPOCH_LEGACY`] the document is read at most once per
/// [`EPOCH_RECHECK_INTERVAL_SECS`].
pub(crate) fn read_epoch(cache_dir: &Path) -> u64 {
    let now = now_secs();
    if let Ok(map) = EPOCH_STATE.read() {
        if let Some(cell) = map.get(cache_dir) {
            // Latched. No I/O, ever again, for this directory.
            if cell.epoch == EPOCH_FOLD {
                return EPOCH_FOLD;
            }
            if now
                < cell
                    .checked_at_secs
                    .saturating_add(EPOCH_RECHECK_INTERVAL_SECS)
            {
                return EPOCH_LEGACY;
            }
        }
    }
    let observed = match load_epoch_doc(cache_dir) {
        Ok(Some(doc)) if doc.epoch == EPOCH_FOLD => {
            // Announced at INFO because it is the single most consequential state change in
            // this workstream: from this line onward the published figures change derivation
            // and rollback is no longer supported.
            info!(
                activated_at = doc.activated_at,
                activated_by = %doc.activated_by,
                binary_version = %doc.binary_version,
                cache_dir = %cache_dir.display(),
                "Staged authority epoch is 1: the fold-derived staged figure is now \
                 authoritative on this volume, Graduation entries are cleanup-only \
                 no-ops, and running a release before 3.0.0 against this cache volume is \
                 not supported; to roll back, move the fleet to a fresh cache volume (see \
                 docs/UPGRADING.md, Rolling back from 3.0.0)."
            );
            EPOCH_FOLD
        }
        Ok(_) => EPOCH_LEGACY,
        Err(e) => {
            // Unreadable ⇒ Phase L. An epoch we cannot read must not be assumed to be 1:
            // that would make the fold authoritative on no evidence.
            debug!(
                "Staged authority epoch unreadable ({}), staying in Phase L (the incumbent \
                 counter remains the authority)",
                e
            );
            EPOCH_LEGACY
        }
    };
    if let Ok(mut map) = EPOCH_STATE.write() {
        let cell = map.entry(cache_dir.to_path_buf()).or_insert(EpochCell {
            epoch: EPOCH_LEGACY,
            checked_at_secs: 0,
        });
        // A plain assignment, and NOT a `max`, deliberately.
        //
        // The first draft wrote `cell.epoch = cell.epoch.max(observed)` and described it as
        // "the rollback rule in code". It is not: **this line is unreachable with a `FOLD`
        // cell**, because the short-circuit at the top of this function returns before
        // consulting the freshness window at all. So `max` and `=` are equivalent in every
        // reachable state, and the `max` read as a second guard while being incapable of
        // guarding anything.
        //
        // That was established by running the red side, not by reading the code: replacing
        // the `max` with `=` left `the_epoch_latches_and_never_falls_back_to_legacy` GREEN,
        // twice — once with the freshness window deliberately expired to try to reach it. A
        // guard whose removal no test notices is not a guard, and dressing it up as one costs
        // a future reader the time it took to find out.
        //
        // **The short-circuit is the whole of the monotonicity**, and that is the line to
        // read if this property ever needs re-establishing. It is sound because the only
        // transition is `0 -> 1`: same-volume rollback is unsupported, and even the untested
        // in-place variant in the module doc starts with a full fleet stop, so a live process
        // cannot observe `1 -> 0`.
        cell.epoch = observed;
        cell.checked_at_secs = now;
        return cell.epoch;
    }
    observed
}

/// The latched epoch for this directory **without touching the filesystem**.
///
/// For synchronous call sites that cannot afford a shared-storage read — notably
/// `apply_journal_entries`, which is `&self`, non-`async`, and runs once per journal entry.
/// Reads [`EPOCH_LEGACY`] until some call site has refreshed the latch via [`read_epoch`],
/// which the consolidation cycle does once per cycle, before anything else in the cycle reads
/// it.
///
/// Defaulting to `LEGACY` is safe in exactly one direction, and it is the right one: a stale
/// `LEGACY` applies a legacy debit the fold would have ignored, which leaves the incumbent
/// counter correct for whatever is still reading it. A stale `FOLD` would skip a debit the
/// incumbent authority still needed, which **under**-counts — the unsafe direction. Hence the
/// latch is monotone upward only.
pub(crate) fn epoch_latched_for(cache_dir: &Path) -> u64 {
    EPOCH_STATE
        .read()
        .ok()
        .and_then(|m| m.get(cache_dir).map(|c| c.epoch))
        .unwrap_or(EPOCH_LEGACY)
}

/// Whether the fold is the authority on this volume, refreshing the latch.
pub fn fold_authority_active(cache_dir: &Path) -> bool {
    read_epoch(cache_dir) == EPOCH_FOLD
}

/// Whether the fold is the authority, from the latch alone. See [`epoch_latched_for`].
pub(crate) fn fold_authority_active_latched(cache_dir: &Path) -> bool {
    epoch_latched_for(cache_dir) == EPOCH_FOLD
}

/// Force the next [`read_epoch`] for this directory to re-read the document.
///
/// Tests only. A `LEGACY` reading is memoised for [`EPOCH_RECHECK_INTERVAL_SECS`], so a test
/// that writes an epoch document and immediately re-reads gets the cached answer and asserts
/// nothing about the document it just wrote — a vacuous pass that looks identical to a real
/// one. This makes the re-read happen.
#[cfg(test)]
pub(crate) fn expire_epoch_check_for_tests(cache_dir: &Path) {
    if let Ok(mut map) = EPOCH_STATE.write() {
        if let Some(cell) = map.get_mut(cache_dir) {
            cell.checked_at_secs = 0;
        }
    }
}

/// Publish this instance's `.ready` record.
///
/// Idempotent and cheap: one small atomic write, called once per consolidation cycle. The
/// record is overwritten rather than appended, so it always describes current state — a
/// stale assertion of readiness from an instance that has since started failing folds would
/// be worse than none.
pub(crate) fn publish_ready_record(
    cache_dir: &Path,
    instance_id: &str,
    rotation_complete: bool,
    fold_incomplete_total: u64,
    repair_rotation_complete: bool,
    repair_stalest_age_secs: Option<u64>,
) -> std::io::Result<()> {
    let record = ReadyRecord {
        schema: AUTHORITY_DOC_SCHEMA,
        instance_id: instance_id.to_string(),
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        fold_capable: true,
        rotation_complete,
        fold_incomplete_total,
        repair_rotation_complete,
        repair_stalest_age_secs,
        evict_worklist_capable: true,
        written_at: now_secs(),
    };
    let body = serde_json::to_string_pretty(&record)
        .map_err(|e| std::io::Error::other(format!("serialize ready record: {e}")))?;
    let path = ready_dir(cache_dir).join(format!("{}.{}", instance_id, READY_EXT));
    install_atomic(&path, &body)?;
    note_ready_published(cache_dir, instance_id, record.written_at);
    Ok(())
}

/// Publish this instance's **startup** `.ready` record if it has none: C-capable, both rotations
/// not yet shown, `fold_incomplete_total` as given. Design § 16.6.6 B10.3.
///
/// Called by `WriteLedger::append` before this process's first ledger entry, so an id that has
/// appended is never record-less. `Ok(true)` when written, `Ok(false)` when a record already
/// existed; an existing record is never replaced, because it may be a real publication by
/// `maintain_staged_authority`, which in turn overwrites this one as before.
///
/// The `false` flags block nobody: under B10.2 a peer's rotation flags are not consulted, and the
/// evaluating instance uses its own current read.
pub(crate) fn publish_startup_ready_record(
    cache_dir: &Path,
    instance_id: &str,
    fold_incomplete_total: u64,
) -> std::io::Result<bool> {
    let record = ReadyRecord {
        schema: AUTHORITY_DOC_SCHEMA,
        instance_id: instance_id.to_string(),
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        fold_capable: true,
        rotation_complete: false,
        fold_incomplete_total,
        repair_rotation_complete: false,
        repair_stalest_age_secs: None,
        evict_worklist_capable: true,
        written_at: now_secs(),
    };
    let body = serde_json::to_string_pretty(&record)
        .map_err(|e| std::io::Error::other(format!("serialize ready record: {e}")))?;
    let path = ready_dir(cache_dir).join(format!("{}.{}", instance_id, READY_EXT));
    let installed = install_if_absent(&path, &body)?;
    if installed {
        note_ready_published(cache_dir, instance_id, record.written_at);
    }
    Ok(installed)
}

/// When this process last wrote each instance id's `.ready` record, per cache directory, in unix
/// seconds. In-process only: it decides whether a journal append refreshes the record
/// ([`refresh_ready_record_after_journal_append`]), never whether a record is current, which the
/// readers judge from `written_at` on the volume.
#[allow(clippy::type_complexity)]
static READY_PUBLISHED_AT: std::sync::Mutex<
    std::collections::BTreeMap<PathBuf, std::collections::BTreeMap<String, u64>>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

fn note_ready_published(cache_dir: &Path, instance_id: &str, at: u64) {
    if let Ok(mut map) = READY_PUBLISHED_AT.lock() {
        map.entry(cache_dir.to_path_buf())
            .or_default()
            .insert(instance_id.to_string(), at);
    }
}

/// How old this process's own write of an instance's `.ready` record may be before a journal
/// append under that id refreshes it: a quarter of the capability gate's window, so a record an
/// append finds unrefreshed is still well inside the window. At most one small record write per
/// 15 minutes per appending id, and usually none, because the appending proxy's own non-idle
/// cycles publish more often than that.
pub(crate) const JOURNAL_APPEND_READY_REFRESH_SECS: u64 =
    crate::eviction_worklist::CAPABILITY_WINDOW_SECS / 4;

/// Red-side lever: a journal append never refreshes the appending id's `.ready` record.
#[cfg(test)]
const JOURNAL_APPEND_SKIPS_READY_REFRESH: bool = false;

/// Red-side lever: the throttle is bypassed, so every append writes the
/// record — one small write per append rather than one per
/// [`JOURNAL_APPEND_READY_REFRESH_SECS`], silently.
#[cfg(test)]
const JOURNAL_APPEND_IGNORES_REFRESH_THROTTLE: bool = false;

/// After a journal append under `instance_id`, refresh that id's `.ready` record (the
/// lightweight one `JournalConsolidator::publish_lightweight_ready_record` writes) when this
/// process last wrote it more than [`JOURNAL_APPEND_READY_REFRESH_SECS`] ago, or never.
///
/// # Why a journal append
///
/// `eviction_worklist::fleet_is_worklist_capable` puts an id in its roster when its journal's
/// mtime is recent and requires a record no older than `CAPABILITY_WINDOW_SECS`. The proxy's
/// own record is refreshed on its non-idle consolidation cycles, so a proxy that has been quiet
/// for over an hour holds an hour-old record. Its next append puts it back in the roster at
/// once, and a peer's cycle that runs before this proxy's next cycle reads `NotCapable` (its
/// abort tick aborts the live generation, its author falls back to the legacy pass). That peer
/// then applies the entries and deletes the journal, so this proxy's own cycle is idle and
/// writes nothing, and the next sporadic append repeats it. The append is the one write every
/// such schedule shares, and refreshing here keeps idle cycles write-free.
///
/// # Cost
///
/// Never awaited: the write runs on a detached blocking task, so the append (a PUT's or a cache
/// hit's) is not delayed by it, and a failure only logs. Below the threshold an append pays one
/// uncontended lock and two map lookups. The stamp is taken before the write, so concurrent
/// appends start one refresh between them; a failed write restores the previous stamp, so the
/// next append retries.
pub(crate) fn refresh_ready_record_after_journal_append(cache_dir: &Path, instance_id: &str) {
    #[cfg(test)]
    if JOURNAL_APPEND_SKIPS_READY_REFRESH {
        return;
    }
    let now = now_secs();
    let previous = {
        let Ok(mut map) = READY_PUBLISHED_AT.lock() else {
            return;
        };
        let previous = map
            .get(cache_dir)
            .and_then(|ids| ids.get(instance_id))
            .copied();
        #[cfg(test)]
        let throttled = !JOURNAL_APPEND_IGNORES_REFRESH_THROTTLE;
        #[cfg(not(test))]
        let throttled = true;
        if throttled
            && previous.is_some_and(|at| now.saturating_sub(at) < JOURNAL_APPEND_READY_REFRESH_SECS)
        {
            return;
        }
        map.entry(cache_dir.to_path_buf())
            .or_default()
            .insert(instance_id.to_string(), now);
        previous
    };
    let (dir, id) = (cache_dir.to_path_buf(), instance_id.to_string());
    let restore = move |dir: &Path, id: &str| {
        if let Ok(mut map) = READY_PUBLISHED_AT.lock() {
            if let Some(ids) = map.get_mut(dir) {
                if ids.get(id) == Some(&now) {
                    match previous {
                        Some(at) => {
                            ids.insert(id.to_string(), at);
                        }
                        None => {
                            ids.remove(id);
                        }
                    }
                }
            }
        }
    };
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        restore(&dir, &id);
        return;
    };
    let fold_incomplete_total = crate::staged_fold::fold_incomplete_total();
    drop(runtime.spawn_blocking(move || {
        if let Err(e) = publish_ready_record(&dir, &id, false, fold_incomplete_total, false, None) {
            debug!(
                "Could not refresh the ready record for {} after a journal append: {}",
                id, e
            );
            restore(&dir, &id);
        }
    }));
}

/// The PRODUCTION branch of the append refresh throttle. The existing
/// coverage reaches it with no stamp at all (`previous == None`), which every process has only
/// before its first publication; on the fleet `initialize` stamps at startup, so the branch that
/// decides anything is `Some(at)` against the window.
#[cfg(test)]
mod journal_append_ready_refresh_tests {
    use super::*;
    use std::time::Duration;

    /// Whether `path` appears within `limit`. Returns as soon as it does, so the positive half
    /// is not slowed by the limit the negative half needs.
    async fn appears_within(path: &Path, limit: Duration) -> bool {
        let deadline = std::time::Instant::now() + limit;
        while std::time::Instant::now() < deadline {
            if path.exists() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        path.exists()
    }

    /// Red sides: `JOURNAL_APPEND_SKIPS_READY_REFRESH` (the throttle stuck: the first append
    /// writes nothing, so a quiet proxy's record stays an hour old and a peer reads the fleet
    /// `NotCapable`) and `JOURNAL_APPEND_IGNORES_REFRESH_THROTTLE` (the throttle bypassed: the
    /// second append writes again, one record write per append).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_append_refreshes_a_stamped_record_past_the_window_and_not_again_inside_it() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        const ID: &str = "host-case3:7";
        let path = ready_dir(dir).join(format!("{}.{}", ID, READY_EXT));

        // The production precondition: this process HAS published the record, and that
        // publication has aged past the refresh window.
        publish_ready_record(dir, ID, false, 0, false, None).unwrap();
        assert!(path.exists(), "fixture: the record this process published");
        note_ready_published(dir, ID, now_secs() - JOURNAL_APPEND_READY_REFRESH_SECS - 1);
        std::fs::remove_file(&path).unwrap();
        assert!(!path.exists(), "fixture: the record is gone");

        refresh_ready_record_after_journal_append(dir, ID);
        assert!(
            appears_within(&path, Duration::from_secs(5)).await,
            "an append past the window must refresh the record: {:?} was not written",
            path
        );
        let refreshed: ReadyRecord =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(
            refreshed.evict_worklist_capable,
            "the refreshed record is the lightweight capable one: {refreshed:?}"
        );
        assert!(
            now_secs().saturating_sub(refreshed.written_at) < JOURNAL_APPEND_READY_REFRESH_SECS,
            "and it is stamped now, not at the old publication: {refreshed:?}"
        );

        // Inside the window, the next append writes nothing: the refresh is at most one small
        // record write per JOURNAL_APPEND_READY_REFRESH_SECS per appending id.
        std::fs::remove_file(&path).unwrap();
        refresh_ready_record_after_journal_append(dir, ID);
        assert!(
            !appears_within(&path, Duration::from_millis(600)).await,
            "a second append inside the window must write nothing: {:?} was rewritten",
            path
        );
    }
}

/// Every `.ready` record on the volume.
///
/// `Err` on a directory-listing failure or on any single unreadable record, because a
/// partial view of the fleet's readiness is not evidence of readiness. This is the same
/// complete-or-refuse discipline § 16.6.3 applies to the ledger read, applied to the
/// activation gate: the failure mode being avoided is a record that could not be read
/// looking identical to an instance that is ready.
pub(crate) fn read_ready_records(cache_dir: &Path) -> std::io::Result<Vec<ReadyRecord>> {
    let dir = ready_dir(cache_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if !path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().ends_with(READY_EXT))
        {
            // A temp file mid-install. Not a record.
            continue;
        }
        let content = std::fs::read_to_string(&path)?;
        let record: ReadyRecord = serde_json::from_str(&content).map_err(|e| {
            std::io::Error::other(format!("unparseable ready record {}: {e}", path.display()))
        })?;
        out.push(record);
    }
    Ok(out)
}

/// § 16.6.6's activation preconditions, evaluated against observed fleet state.
///
/// `ledger_instances` is the set of instance IDs that have a `{instance}.ledger` file — i.e.
/// every instance that has ever appended a staged credit on this volume. It is passed in
/// rather than read here so a test can construct the divergent state (a ledger with no
/// matching record) directly.
///
/// A record older than [`READY_RECORD_RETIRE_AGE_SECS`] that proves C-capable code is
/// **retired**: it still satisfies precondition 1 for its instance, provided no entry in that
/// instance's ledger files is later than the record's `written_at` plus
/// [`READY_RECORD_APPEND_MARGIN_SECS`], and is otherwise ignored; at least one non-retired record
/// is required ([`ActivationBlocker::NoFreshReadyRecords`]). The ledger files are read (through
/// [`crate::write_ledger::WriteLedger::latest_append_secs_in`]) only for ids that no
/// current record vouches for. Design § 16.6.6 amendments B4 and B7.
///
/// An id still unvouched after that — no record, or only retired records B7 declined — stops
/// counting once every ledger file named for it has gone unmodified for
/// [`RECORDLESS_QUIET_RETIRE_SECS`] (B8), or sooner while an operator's retirement marker
/// covers it (B9). One ledger listing serves both reads.
///
/// The verdict of [`activation_assessment`], which also names every holder.
///
/// Test seam: production evaluates the gate through [`activation_assessment_current`].
#[cfg(test)]
pub(crate) fn activation_verdict(
    cache_dir: &Path,
    ledger_instances: &BTreeSet<String>,
) -> ActivationVerdict {
    activation_assessment(cache_dir, ledger_instances).verdict
}

/// The activation verdict and **every** instance holding the gate. Design § 16.6.6, the B9
/// paragraph "Every holder is named".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationAssessment {
    /// Whether the gate permits activation, and if not, the first blocker.
    pub verdict: ActivationVerdict,
    /// Every instance-naming blocker, in a stable order: each ledger id nothing vouches for, in
    /// ledger-id order, then each fresh record failing a check, in instance-id order, one per
    /// record. When the verdict names an instance it is the first entry here.
    ///
    /// `None` when no id was examined: [`ActivationBlocker::AlreadyActive`],
    /// [`ActivationBlocker::ReadyRecordsUnreadable`] and [`ActivationBlocker::NoReadyRecords`]
    /// return before any. `Some(empty)` for `Permitted`, and for `NoFreshReadyRecords` when every
    /// ledger id is vouched for.
    pub holders: Option<Vec<ActivationBlocker>>,
}

impl ActivationAssessment {
    fn unexamined(blocker: ActivationBlocker) -> Self {
        Self {
            verdict: ActivationVerdict::Blocked(blocker),
            holders: None,
        }
    }
}

/// The first per-record precondition `record` fails, if any. Precondition 2, then 3a, then 3b.
fn record_blocker(record: &ReadyRecord) -> Option<ActivationBlocker> {
    // Precondition 2. See `ReadyRecord::fold_capable` for why this is a capability assertion
    // rather than a version comparison.
    if record.schema != AUTHORITY_DOC_SCHEMA || !record.fold_capable {
        return Some(ActivationBlocker::NotFoldCapable(
            record.instance_id.clone(),
        ));
    }
    // Precondition 3a: "`staged_fold_incomplete_total` has been zero for one full rotation". Two
    // checks because the sentence is a conjunction of two observable things — a completed
    // rotation, and no incompleteness seen over it.
    if !record.rotation_complete {
        return Some(ActivationBlocker::RotationIncomplete(
            record.instance_id.clone(),
        ));
    }
    if record.fold_incomplete_total != 0 {
        return Some(ActivationBlocker::FoldIncompleteObserved(
            record.instance_id.clone(),
        ));
    }
    // Precondition 3b: "ledger-completeness repair has completed one rotation by range
    // identity". A SEPARATE rotation from 3a's, over a different mechanism — see
    // `ReadyRecord::repair_rotation_complete` for why the fold's rotation cannot stand in for it,
    // and `ledger_repair` for what it repairs.
    //
    // This comment used to read "Precondition 3, both halves" above the two 3a checks, which
    // asserted an implementation that did not exist. 3b had no code, no field on this record,
    // and no blocker.
    if !record.repair_rotation_complete {
        return Some(ActivationBlocker::RepairRotationIncomplete(
            record.instance_id.clone(),
        ));
    }
    None
}

/// The per-process preconditions a record carries: precondition 2 (a known schema and
/// `fold_capable`), then 3a's incompleteness half (`fold_incomplete_total`). Design § 16.6.6
/// B10.1: every other precondition field is a snapshot of shared documents.
fn record_process_blocker(record: &ReadyRecord) -> Option<ActivationBlocker> {
    if record.schema != AUTHORITY_DOC_SCHEMA || !record.fold_capable {
        return Some(ActivationBlocker::NotFoldCapable(
            record.instance_id.clone(),
        ));
    }
    if record.fold_incomplete_total != 0 {
        return Some(ActivationBlocker::FoldIncompleteObserved(
            record.instance_id.clone(),
        ));
    }
    None
}

/// The evaluating instance's own readiness, read from the shared volume by the call that is
/// asking for a verdict, plus its live `fold_incomplete_total`. Design § 16.6.6 B10.2.
///
/// `rotation_complete` and `repair_rotation_complete` are the two volume-derived preconditions
/// (3a's rotation half and 3b). Handed to [`activation_assessment_current`], they are the only
/// place the verdict reads either rotation from: the same flags on other records are a dead or
/// idle process's snapshot of the same documents and are not consulted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentReadiness {
    /// The evaluating instance, named by any blocker these figures produce.
    pub instance_id: String,
    /// Precondition 3a's rotation half, from this call's read.
    pub rotation_complete: bool,
    /// Precondition 3b, from this call's read.
    pub repair_rotation_complete: bool,
    /// This process's `staged_fold_incomplete_total`, read live.
    pub fold_incomplete_total: u64,
}

impl CurrentReadiness {
    /// The first precondition these figures fail, in [`record_blocker`]'s order: 3a's rotation
    /// half, 3a's incompleteness half, then 3b.
    fn blocker(&self) -> Option<ActivationBlocker> {
        if !self.rotation_complete {
            return Some(ActivationBlocker::RotationIncomplete(
                self.instance_id.clone(),
            ));
        }
        if self.fold_incomplete_total != 0 {
            return Some(ActivationBlocker::FoldIncompleteObserved(
                self.instance_id.clone(),
            ));
        }
        if !self.repair_rotation_complete {
            return Some(ActivationBlocker::RepairRotationIncomplete(
                self.instance_id.clone(),
            ));
        }
        None
    }
}

/// [`activation_verdict`], with every holder. See [`ActivationAssessment`].
///
/// Supplies no current read, so every non-retired record is checked in full (design § 16.6.6
/// B4). The consolidator calls [`activation_assessment_current`] instead; this is a test seam.
#[cfg(test)]
pub(crate) fn activation_assessment(
    cache_dir: &Path,
    ledger_instances: &BTreeSet<String>,
) -> ActivationAssessment {
    assess(cache_dir, ledger_instances, None)
}

/// The activation verdict and every holder, with the evaluating instance's current read: the
/// volume-derived preconditions are evaluated against `current` alone, and every non-retired
/// record is checked for the per-process ones. Design § 16.6.6 B10.2.
pub(crate) fn activation_assessment_current(
    cache_dir: &Path,
    ledger_instances: &BTreeSet<String>,
    current: &CurrentReadiness,
) -> ActivationAssessment {
    assess(cache_dir, ledger_instances, Some(current))
}

fn assess(
    cache_dir: &Path,
    ledger_instances: &BTreeSet<String>,
    current: Option<&CurrentReadiness>,
) -> ActivationAssessment {
    if read_epoch(cache_dir) == EPOCH_FOLD {
        return ActivationAssessment::unexamined(ActivationBlocker::AlreadyActive);
    }
    let records = match read_ready_records(cache_dir) {
        Ok(r) => r,
        Err(e) => {
            debug!("Activation gate: ready records unreadable: {}", e);
            return ActivationAssessment::unexamined(ActivationBlocker::ReadyRecordsUnreadable);
        }
    };
    if records.is_empty() {
        return ActivationAssessment::unexamined(ActivationBlocker::NoReadyRecords);
    }
    // Precondition 1, and the load-bearing one. An instance that has appended a staged
    // credit necessarily created its own ledger file; a pre-C binary has no code to publish
    // a `.ready` record. So an unmatched ledger file IS a pre-C (or otherwise unready)
    // participant, and this is the check it cannot evade.
    //
    // Retired records count here, and that is the whole of their remaining role: a retired
    // record is by construction `fold_capable` with a known schema, so its instance ran
    // C-capable code and is not the writer this check exists to refuse. An id with NO record
    // is untouched by record retirement; it stops blocking only by B8's one-day quiet rule below.
    //
    // A retired record vouches only while its instance has not appended since it was written
    // (B7): a pre-C binary that reuses the dead pid appends under the same id, and must then
    // count as record-less. The ledger is read only for ids that nothing current vouches for.
    let now = now_secs();
    let mut vouched: BTreeSet<&str> = records
        .iter()
        .filter(|r| !is_retired(r, now))
        .map(|r| r.instance_id.as_str())
        .collect();
    let mut retired_written_at: std::collections::BTreeMap<&str, u64> =
        std::collections::BTreeMap::new();
    for record in records.iter().filter(|r| is_retired(r, now)) {
        let newest = retired_written_at
            .entry(record.instance_id.as_str())
            .or_insert(0);
        *newest = (*newest).max(record.written_at);
    }
    // One whole-volume ledger listing, taken only when some ledger id is not vouched for by a
    // fresh record, and shared by B7's read and B8's stat below.
    let needs_listing = ledger_instances
        .iter()
        .any(|id| !vouched.contains(id.as_str()));
    let files = if needs_listing {
        Some(crate::write_ledger::WriteLedger::ledger_files_blocking(
            cache_dir,
        ))
    } else {
        None
    };
    let to_check: BTreeSet<String> = ledger_instances
        .iter()
        .filter(|id| !vouched.contains(id.as_str()) && retired_written_at.contains_key(id.as_str()))
        .cloned()
        .collect();
    if !to_check.is_empty() {
        let evidence = match &files {
            Some(Ok(f)) => Some(crate::write_ledger::WriteLedger::latest_append_secs_in(
                f, &to_check,
            )),
            _ => None,
        };
        for id in &to_check {
            use crate::write_ledger::AppendEvidence;
            let written_at = retired_written_at[id.as_str()];
            let quiet = match evidence.as_ref().map(|e| e.get(id)) {
                Some(Some(AppendEvidence::NoEntries)) | Some(None) => true,
                Some(Some(AppendEvidence::LatestAt(t))) => {
                    *t <= written_at.saturating_add(READY_RECORD_APPEND_MARGIN_SECS)
                }
                Some(Some(AppendEvidence::Indeterminate)) | None => false,
            };
            if quiet {
                if let Some(listed) = ledger_instances.get(id) {
                    vouched.insert(listed.as_str());
                }
            } else {
                debug!(
                    "Activation gate: {} has only a retired ready record and its ledger shows, \
                     or cannot rule out, an append since; not vouched for",
                    id
                );
            }
        }
    }
    // B8: an id nothing vouches for — no record, or only retired records B7 declined — stops
    // counting once every ledger file named for it has gone unmodified for
    // RECORDLESS_QUIET_RETIRE_SECS. mtime rather than B7's entry timestamp: see
    // `WriteLedger::latest_modified_in`. Fails closed on a listing or stat failure, and on an id
    // the stat listing finds no file for.
    let unvouched: BTreeSet<String> = ledger_instances
        .iter()
        .filter(|id| !vouched.contains(id.as_str()))
        .cloned()
        .collect();
    if !unvouched.is_empty() {
        if let Some(Ok(f)) = &files {
            let modified = crate::write_ledger::WriteLedger::latest_modified_in(f, &unvouched);
            // B9: an operator's marker covers its id while no file named for it has been
            // modified after the newest mtime the marker recorded. Both sides are the file
            // server's clock.
            let markers = read_retirement_markers(cache_dir);
            let wall = SystemTime::now();
            for id in &unvouched {
                use crate::write_ledger::ModifiedEvidence;
                let quiet = match modified.get(id) {
                    Some(ModifiedEvidence::LatestAt(t)) => {
                        let retired = markers
                            .get(id)
                            .is_some_and(|recorded| *recorded >= unix_nanos(*t));
                        if retired {
                            debug!(
                                "Activation gate: {} is covered by an operator retirement marker \
                                 and its ledger files are unmodified since; it no longer counts \
                                 against activation (design § 16.6.6 B9)",
                                id
                            );
                        }
                        retired
                            || wall
                                .duration_since(*t)
                                .is_ok_and(|age| age.as_secs() > RECORDLESS_QUIET_RETIRE_SECS)
                    }
                    _ => false,
                };
                if quiet {
                    debug!(
                        "Activation gate: {} has no current ready record and none of its ledger \
                         files has been modified for over {} s; it no longer counts against \
                         activation (design § 16.6.6 B8)",
                        id, RECORDLESS_QUIET_RETIRE_SECS
                    );
                    if let Some(listed) = ledger_instances.get(id) {
                        vouched.insert(listed.as_str());
                    }
                }
            }
        }
    }
    // Every holder, in a stable order: ledger ids first (BTreeSet order), so when any exists the
    // verdict is precondition 1's, exactly as when this returned on the first one.
    let mut holders: Vec<ActivationBlocker> = ledger_instances
        .iter()
        .filter(|id| !vouched.contains(id.as_str()))
        .map(|id| ActivationBlocker::LedgerWithoutReadyRecord(id.clone()))
        .collect();
    // Retirement (§ 16.6.6 amendment B4). A record older than READY_RECORD_RETIRE_AGE_SECS that
    // proves C-capable code is excluded from the per-record checks below: every precondition
    // field it carries except `fold_incomplete_total` is volume-derived and is re-derived by
    // any current record, and the design's (iii) bounds what retiring the counter can lose.
    // Nothing is deleted. A stale record that does NOT prove C-capable code is kept, so it
    // still blocks as `NotFoldCapable`.
    let mut fresh: Vec<&ReadyRecord> = records.iter().filter(|r| !is_retired(r, now)).collect();
    // Instance-id order, so the named holder does not depend on `readdir` order.
    fresh.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
    // Activation needs current evidence. Without this, retiring every record would leave the
    // loop below with nothing to check and fall through to `Permitted` on no evidence at all.
    let no_fresh = fresh.is_empty();
    match current {
        None => holders.extend(fresh.into_iter().filter_map(record_blocker)),
        // B10.2. The rotations come from `now_read` alone. Every fresh record, the evaluating
        // instance's own included, is checked for what only its process could know. The
        // evaluating instance gets one blocker, in instance-id order like the rest: its record's
        // precondition 2 first, then its current figures, whose live counter is at least the one
        // its record carries. It is evaluated even when it has no record on the volume.
        Some(now_read) => {
            let mut per_instance: Vec<(&str, ActivationBlocker)> = fresh
                .into_iter()
                .filter(|r| r.instance_id != now_read.instance_id)
                .filter_map(|r| record_process_blocker(r).map(|b| (r.instance_id.as_str(), b)))
                .collect();
            let own_record = records
                .iter()
                .filter(|r| !is_retired(r, now))
                .find(|r| r.instance_id == now_read.instance_id);
            let own = match own_record.and_then(record_process_blocker) {
                Some(b @ ActivationBlocker::NotFoldCapable(_)) => Some(b),
                _ => now_read.blocker(),
            };
            if let Some(b) = own {
                per_instance.push((now_read.instance_id.as_str(), b));
            }
            per_instance.sort_by(|a, b| a.0.cmp(b.0));
            holders.extend(per_instance.into_iter().map(|(_, b)| b));
        }
    }
    let verdict = match holders.first() {
        Some(first @ ActivationBlocker::LedgerWithoutReadyRecord(_)) => {
            ActivationVerdict::Blocked(first.clone())
        }
        _ if no_fresh => ActivationVerdict::Blocked(ActivationBlocker::NoFreshReadyRecords),
        Some(first) => ActivationVerdict::Blocked(first.clone()),
        None => ActivationVerdict::Permitted,
    };
    ActivationAssessment {
        verdict,
        holders: Some(holders),
    }
}

/// Unix nanoseconds of `t`, 0 before the epoch. The unit a retirement marker records mtimes in.
fn unix_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Whether `record` is retired from the activation verdict at `now` (unix seconds).
///
/// Retired means: it proves C-capable code (known schema, `fold_capable`) **and** it was written
/// more than [`READY_RECORD_RETIRE_AGE_SECS`] ago. A `written_at` in the future — clock skew
/// between hosts — reads as age 0, i.e. current, which is the direction that keeps checking it.
fn is_retired(record: &ReadyRecord, now: u64) -> bool {
    record.schema == AUTHORITY_DOC_SCHEMA
        && record.fold_capable
        && now.saturating_sub(record.written_at) > READY_RECORD_RETIRE_AGE_SECS
}

/// `metadata/_shard_state/retired_instances`, holding one operator retirement marker per id.
const RETIRED_DIR: &str = "retired_instances";

/// Extension of an operator retirement marker, `{instance_id}.retired`.
const RETIRED_EXT: &str = "retired";

/// Schema of [`RetirementMarker`]. A marker with any other value is ignored, which blocks.
const RETIREMENT_MARKER_SCHEMA: u32 = 1;

/// How recently a ledger file named for an id may have been modified before `retire-instance`
/// refuses to retire it, seconds. Design § 16.6.6 B9.
///
/// An hour, so an operator cannot retire a live writer by mistyping an id: a live instance that
/// stages anything touches its ledger files far more often than that. It does not protect an
/// idle live pre-C writer, which is exactly the case Ed's one-day rule already accepts; the marker
/// narrows that exposure from one day to whenever the operator chose to act.
pub const RETIRE_INSTANCE_REFUSAL_WINDOW_SECS: u64 = 3_600;

/// `{cache_dir}/metadata/_shard_state/retired_instances`.
pub fn retired_instances_dir(cache_dir: &Path) -> PathBuf {
    shard_state_dir(cache_dir).join(RETIRED_DIR)
}

/// An operator's statement that a ledger id is gone, written by `s3-proxy retire-instance`.
/// Design § 16.6.6 B9.
///
/// It covers its id in `activation_assessment_current` only while no ledger file named for the id has been
/// modified after [`Self::ledger_last_modified_ns`]; both that figure and the verdict's reading
/// are the file server's mtime, so no clock skew enters. Nothing removes a marker: once a later
/// modification voids it, it is inert, and an operator may delete it by hand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetirementMarker {
    /// [`RETIREMENT_MARKER_SCHEMA`].
    pub schema: u32,
    /// The retired id. Must equal the marker's file stem, or the marker is ignored.
    pub instance_id: String,
    /// Unix seconds at which the operator retired it, on the operator host's clock. Recorded,
    /// not compared.
    pub retired_at: u64,
    /// The host the command ran on. Recorded, not compared.
    pub retired_by: String,
    /// The binary that wrote it. Recorded, not compared.
    pub binary_version: String,
    /// The newest mtime over every ledger file named for the id when it was retired, unix
    /// nanoseconds. A later mtime on any of them voids the marker.
    pub ledger_last_modified_ns: u64,
}

/// Why `retire-instance` refused. Nothing is written in any of these cases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetireRefusal {
    /// The id is empty, or contains a path separator, `..` or a control character.
    UnsafeInstanceId,
    /// The ledger directories could not be listed.
    LedgerUnlisted(String),
    /// No ledger file is named for the id: nothing to retire, and most likely a typo.
    NoLedgerFiles,
    /// A ledger file named for the id could not be `stat`ed.
    LedgerUnreadable,
    /// A ledger file named for the id was modified `age_secs` ago, inside
    /// [`RETIRE_INSTANCE_REFUSAL_WINDOW_SECS`]. A modification time ahead of this host's clock
    /// reads as age 0.
    RecentlyModified { age_secs: u64 },
    /// The marker could not be written.
    WriteFailed(String),
}

impl std::fmt::Display for RetireRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsafeInstanceId => write!(
                f,
                "the instance id is empty or contains '/', '\\\\', '..' or a control character"
            ),
            Self::LedgerUnlisted(e) => {
                write!(f, "the write-ledger directories could not be listed: {e}")
            }
            Self::NoLedgerFiles => write!(
                f,
                "no write-ledger file is named for this id, so it is not holding activation; \
                 check the id against write_cache.staged_authority_activation_blocking_instance"
            ),
            Self::LedgerUnreadable => write!(
                f,
                "a write-ledger file named for this id could not be stat'ed, so whether it is \
                 still being written is unknown"
            ),
            Self::RecentlyModified { age_secs } => write!(
                f,
                "a write-ledger file named for this id was modified {age_secs} s ago, inside the \
                 {RETIRE_INSTANCE_REFUSAL_WINDOW_SECS} s refusal window, so it may be a live \
                 writer; stop that process first, or wait"
            ),
            Self::WriteFailed(e) => write!(f, "the retirement marker could not be written: {e}"),
        }
    }
}

/// Retire `instance_id` from the activation gate: the body of `s3-proxy retire-instance`.
///
/// Refuses, writing nothing, on any [`RetireRefusal`]. Otherwise writes
/// `metadata/_shard_state/retired_instances/{instance_id}.retired` through [`install_atomic`], the
/// helper the `.ready` records and the epoch document use, and returns what it wrote. Blocking
/// `std::fs`. There is deliberately no network endpoint for this.
pub fn retire_instance(
    cache_dir: &Path,
    instance_id: &str,
) -> Result<RetirementMarker, RetireRefusal> {
    use crate::write_ledger::{ModifiedEvidence, WriteLedger};
    if !crate::path_safety::is_safe_path_component(instance_id) {
        return Err(RetireRefusal::UnsafeInstanceId);
    }
    let files = WriteLedger::ledger_files_blocking(cache_dir)
        .map_err(|e| RetireRefusal::LedgerUnlisted(e.to_string()))?;
    let ids: BTreeSet<String> = [instance_id.to_string()].into_iter().collect();
    let newest = match WriteLedger::latest_modified_in(&files, &ids).get(instance_id) {
        Some(ModifiedEvidence::LatestAt(t)) => *t,
        Some(ModifiedEvidence::Indeterminate) => return Err(RetireRefusal::LedgerUnreadable),
        Some(ModifiedEvidence::NoFiles) | None => return Err(RetireRefusal::NoLedgerFiles),
    };
    let age_secs = SystemTime::now()
        .duration_since(newest)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if age_secs < RETIRE_INSTANCE_REFUSAL_WINDOW_SECS {
        return Err(RetireRefusal::RecentlyModified { age_secs });
    }
    let marker = RetirementMarker {
        schema: RETIREMENT_MARKER_SCHEMA,
        instance_id: instance_id.to_string(),
        retired_at: now_secs(),
        retired_by: hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        ledger_last_modified_ns: unix_nanos(newest),
    };
    let body = serde_json::to_string_pretty(&marker)
        .map_err(|e| RetireRefusal::WriteFailed(e.to_string()))?;
    let path = retired_instances_dir(cache_dir).join(format!("{instance_id}.{RETIRED_EXT}"));
    install_atomic(&path, &body).map_err(|e| RetireRefusal::WriteFailed(e.to_string()))?;
    Ok(marker)
}

/// Every valid retirement marker, as `instance_id → ledger_last_modified_ns`.
///
/// Fails toward blocking: a listing failure reads as no markers, and a marker that cannot be
/// read, does not parse, carries an unknown schema, or names an id other than its file stem is
/// skipped.
fn read_retirement_markers(cache_dir: &Path) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(retired_instances_dir(cache_dir)) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != RETIRED_EXT) {
            continue;
        }
        let Some(stem) = path.file_stem().map(|s| s.to_string_lossy().to_string()) else {
            continue;
        };
        let Ok(content) = std::fs::read_to_string(&path) else {
            debug!(
                "Activation gate: retirement marker {:?} unreadable, ignored",
                path
            );
            continue;
        };
        match serde_json::from_str::<RetirementMarker>(&content) {
            Ok(m) if m.schema == RETIREMENT_MARKER_SCHEMA && m.instance_id == stem => {
                out.insert(stem, m.ledger_last_modified_ns);
            }
            _ => debug!(
                "Activation gate: retirement marker {:?} unparseable, of unknown schema, or \
                 naming another id; ignored",
                path
            ),
        }
    }
    out
}

/// Write the epoch document at [`EPOCH_FOLD`].
///
/// # The caller MUST hold the global consolidation lock
///
/// § 16.6.6 requires it, and the reason is that activation is a two-document transaction:
/// this document and the `Schema_Fence` bump on `size_state.json`. There is no way to make
/// two atomic renames one transaction, so the lock is what makes the pair indivisible from
/// the fleet's point of view. The consolidation cycle is the only caller and it already
/// holds the lock.
///
/// Returns `Ok(true)` when this call performed the flip, `Ok(false)` when it was already
/// flipped. Does **not** evaluate the preconditions — `activation_assessment_current` is a separate
/// call so that a caller can log the blocker without a side effect.
pub(crate) fn activate(cache_dir: &Path, instance_id: &str) -> std::io::Result<bool> {
    if let Ok(Some(doc)) = load_epoch_doc(cache_dir) {
        if doc.epoch == EPOCH_FOLD {
            return Ok(false);
        }
    }
    let doc = AuthorityEpochDoc {
        schema: AUTHORITY_DOC_SCHEMA,
        epoch: EPOCH_FOLD,
        activated_at: now_secs(),
        activated_by: instance_id.to_string(),
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let body = serde_json::to_string_pretty(&doc)
        .map_err(|e| std::io::Error::other(format!("serialize authority epoch: {e}")))?;
    install_atomic(&epoch_path(cache_dir), &body)?;
    if let Ok(mut map) = EPOCH_STATE.write() {
        map.insert(
            cache_dir.to_path_buf(),
            EpochCell {
                epoch: EPOCH_FOLD,
                checked_at_secs: now_secs(),
            },
        );
    }
    Ok(true)
}

/// `Foreign_Write_Detection`: whether this `size_state.json` was written by a pre-C binary.
///
/// Evaluated **only** after activation. The signature is the one § 16.6.6 names: a
/// `size_state.json` whose epoch field is missing or `0`, or whose schema is not the fence
/// value, on a volume whose epoch document says `1`. A pre-C binary cannot write either
/// field, so it cannot produce the post-activation shape however it parses the document.
///
/// The predicate only. Its caller files a pre-C rewrite as this instance's own unapplied bump
/// whenever the re-application survives the re-read. Against a pre-C consolidation cycle it
/// always does, because both binaries hold the global consolidation lock, so the operator
/// normally sees one INFO line, not a stalled authority.
pub(crate) fn detect_foreign_size_state(state_schema: u32, state_epoch: u64) -> bool {
    state_schema != SIZE_STATE_SCHEMA_FOLD || state_epoch != EPOCH_FOLD
}

/// Arm the fold's refusal for this cache directory, recording which situation caused it.
///
/// Idempotent per kind: re-arming with the same kind logs nothing, so a persistent condition
/// does not print every cycle. A **change** of kind does log, because that is a different
/// situation and the previous message no longer describes it.
pub(crate) fn record_authority_block(cache_dir: &Path, kind: AuthorityBlockKind, detail: &str) {
    if matches!(kind, AuthorityBlockKind::ForeignPreCWrite) {
        FOREIGN_WRITE_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    let changed = match AUTHORITY_BLOCK.write() {
        Ok(mut map) => map.insert(cache_dir.to_path_buf(), kind) != Some(kind),
        Err(_) => false,
    };
    if !changed {
        return;
    }
    // `kind.label()` is carried as a structured field so the three situations are greppable in the
    // journal, not only distinguishable by prose. The fold's retain label is the `/metrics` side of
    // the same distinction; this is the log side.
    match kind {
        AuthorityBlockKind::ForeignPreCWrite => warn!(
            block_kind = kind.label(),
            detail,
            cache_dir = %cache_dir.display(),
            "size_state.json carries a pre-C schema/epoch after the staged authority epoch was \
             activated, AND STILL DID after this instance re-applied the Schema_Fence — so this \
             is a second writer, not an unapplied fence of our own. A binary that predates \
             decision C has rewritten it. The staged fold will REFUSE to install until this \
             clears, so the published figure will go stale visibly rather than becoming \
             silently wrong. Rollback to a pre-C binary is not supported once the epoch is 1 — \
             see docs/UPGRADING.md."
        ),
        AuthorityBlockKind::SizeStateUnreadable => warn!(
            block_kind = kind.label(),
            detail,
            cache_dir = %cache_dir.display(),
            "size_state.json could not be read, so whether the staged authority is contested is \
             UNKNOWN. This is NOT evidence of a pre-C binary: an unreadable document and a \
             pre-C document are indistinguishable by content, because the in-memory default \
             carries schema 0 / epoch 0 itself. The fold refuses to install while the state \
             cannot be seen. Remedy is to make the file readable (check permissions and the \
             shared volume), NOT the fleet-stop rollback procedure."
        ),
        AuthorityBlockKind::FenceReapplyFailed => warn!(
            block_kind = kind.label(),
            detail,
            cache_dir = %cache_dir.display(),
            "the size_state.json Schema_Fence is not applied and re-applying it failed. This is \
             SELF-INFLICTED and no other binary is implicated: the activation flip landed and \
             its paired fence write did not. Every consolidation cycle retries it, so a \
             transient lock timeout or I/O error clears on its own; a persistent one means the \
             size_state lock or the volume is unavailable. The fold refuses to install \
             meanwhile. Do NOT follow the pre-C rollback procedure for this."
        ),
    }
}

/// Clear the arming after a clean observation.
pub(crate) fn clear_authority_block(cache_dir: &Path) {
    if let Ok(mut map) = AUTHORITY_BLOCK.write() {
        map.remove(cache_dir);
    }
}

/// Why the authority is blocked on this volume, if it is. `Some` means the fold must refuse to
/// install (see `staged_fold`), and the kind is the fold's retain label.
pub(crate) fn authority_block_kind(cache_dir: &Path) -> Option<AuthorityBlockKind> {
    AUTHORITY_BLOCK
        .read()
        .ok()
        .and_then(|m| m.get(cache_dir).copied())
}

/// `staged_authority_foreign_write_total`, per-instance since process start.
pub(crate) fn foreign_write_total() -> u64 {
    FOREIGN_WRITE_TOTAL.load(Ordering::Relaxed)
}

/// The last activation blocker this process evaluated for a volume: its label and the instance
/// it names. Absent until a verdict has been recorded, and absent after a `Permitted` or
/// `AlreadyActive` verdict.
///
/// Keyed by directory for the same reason [`EPOCH_STATE`] is.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedBlocker {
    label: &'static str,
    instance: Option<String>,
    /// How many ids hold the gate; `None` when the verdict examined none.
    holder_count: Option<u64>,
    /// The ids already logged as holders, so each is logged once when it starts holding.
    holders: BTreeSet<String>,
}
static ACTIVATION_BLOCKER: std::sync::RwLock<std::collections::BTreeMap<PathBuf, RecordedBlocker>> =
    std::sync::RwLock::new(std::collections::BTreeMap::new());

/// Record the activation verdict just evaluated for `cache_dir`, and log at INFO when the
/// blocker's label or the instance it names differs from the last recorded one.
///
/// The gate used to report its blocker at DEBUG only, so a volume that could never activate
/// looked identical to one that was converging. INFO on change rather than on every cycle,
/// because the gate is evaluated every non-idle cycle and a persistent blocker would otherwise
/// print every 5 s.
///
/// `Permitted` and `AlreadyActive` clear the record and log nothing here: the activation path
/// has its own INFO line. Returns whether this call logged.
///
/// Records no holder list; see [`record_activation_assessment`] for the form that does, which is
/// the one production calls. Test seam.
#[cfg(test)]
pub(crate) fn record_activation_verdict(cache_dir: &Path, verdict: &ActivationVerdict) -> bool {
    record_activation_assessment(
        cache_dir,
        &ActivationAssessment {
            verdict: verdict.clone(),
            holders: None,
        },
    )
}

/// Record the activation assessment just evaluated for `cache_dir`, and log at INFO when the
/// blocker's label or the instance it names differs from the last recorded one. This is what
/// feeds `staged_authority_activation_blocker` on `/metrics` (through [`activation_blocker`]).
///
/// The gate used to report its blocker at DEBUG only, so a volume that could never activate
/// looked identical to one that was converging. INFO on change rather than on every cycle,
/// because the gate is evaluated every non-idle cycle and a persistent blocker would otherwise
/// print every 5 s. `Permitted` and `AlreadyActive` clear the record and log nothing here: the
/// activation path has its own INFO line.
///
/// Also records every holder: records the holder count for `/metrics` and
/// logs each id at INFO once, when it starts holding the gate. Design § 16.6.6 B9.
///
/// An id stops being "already logged" when a verdict no longer lists it, or when a `Permitted` or
/// `AlreadyActive` verdict clears the record, so an id that stops holding and later holds again
/// is logged again. A verdict that examined no id (`holders: None`) keeps the logged set, so a
/// transient `ready_records_unreadable` does not re-log every holder when it clears. Returns
/// whether this call logged anything.
pub(crate) fn record_activation_assessment(
    cache_dir: &Path,
    assessment: &ActivationAssessment,
) -> bool {
    let (label, instance) = match &assessment.verdict {
        ActivationVerdict::Permitted
        | ActivationVerdict::Blocked(ActivationBlocker::AlreadyActive) => {
            if let Ok(mut map) = ACTIVATION_BLOCKER.write() {
                map.remove(cache_dir);
            }
            return false;
        }
        ActivationVerdict::Blocked(b) => (b.label(), b.instance().map(str::to_string)),
    };
    let mut map = match ACTIVATION_BLOCKER.write() {
        Ok(map) => map,
        Err(_) => return false,
    };
    let previous = map.get(cache_dir).cloned();
    let (holder_count, holders, newly) = match &assessment.holders {
        Some(list) => {
            let ids: BTreeSet<String> = list
                .iter()
                .filter_map(|b| b.instance().map(str::to_string))
                .collect();
            let already = previous.as_ref().map(|p| &p.holders);
            let newly: Vec<ActivationBlocker> = list
                .iter()
                .filter(|b| {
                    b.instance()
                        .is_some_and(|id| !already.is_some_and(|a| a.contains(id)))
                })
                .cloned()
                .collect();
            (Some(list.len() as u64), ids, newly)
        }
        None => (
            None,
            previous
                .as_ref()
                .map(|p| p.holders.clone())
                .unwrap_or_default(),
            Vec::new(),
        ),
    };
    map.insert(
        cache_dir.to_path_buf(),
        RecordedBlocker {
            label,
            instance: instance.clone(),
            holder_count,
            holders,
        },
    );
    drop(map);

    for holder in &newly {
        let id = holder.instance().unwrap_or("none");
        info!(
            reason = holder.label(),
            holding_instance = id,
            cache_dir = %cache_dir.display(),
            "Staged authority epoch activation is held back by instance {}: reason={}. Logged \
             once when an instance starts holding the gate; \
             write_cache.staged_authority_activation_blocking_instance_count on /metrics counts \
             every holder. A ledger id with no current ready record stops holding it once none of \
             its ledger files has been modified for a day, or sooner with `s3-proxy --config \
             <path> retire-instance {}`, which refuses if its ledger was modified within the last \
             hour.",
            id,
            holder.label(),
            id,
        );
    }

    let changed = previous
        .as_ref()
        .is_none_or(|p| p.label != label || p.instance != instance);
    if !changed {
        return !newly.is_empty();
    }
    info!(
        reason = label,
        blocking_instance = instance.as_deref().unwrap_or("none"),
        cache_dir = %cache_dir.display(),
        "Staged authority epoch activation is blocked: reason={}, blocking_instance={}. Phase L \
         continues and the incumbent counter remains the authority. Logged when the reason or \
         the named instance changes; the same figures are on /metrics as \
         write_cache.staged_authority_activation_blocker and \
         write_cache.staged_authority_activation_blocking_instance.",
        label,
        instance.as_deref().unwrap_or("none"),
    );
    true
}

/// The recorded activation blocker for `cache_dir`, as `(label, blocking_instance)`, for
/// `/metrics`.
///
/// `(None, None)` when no verdict has been evaluated yet, when the last verdict was `Permitted`,
/// and whenever the epoch is latched at [`EPOCH_FOLD`]: once active there is nothing blocking,
/// and the consolidator stops evaluating the gate, so a record left from before activation
/// (for example a peer activated while this process still held a blocker) would otherwise be
/// published forever. `blocking_instance` is also `None` for the volume-wide blockers.
pub(crate) fn activation_blocker(cache_dir: &Path) -> (Option<String>, Option<String>) {
    if fold_authority_active_latched(cache_dir) {
        return (None, None);
    }
    ACTIVATION_BLOCKER
        .read()
        .ok()
        .and_then(|m| m.get(cache_dir).cloned())
        .map(|r| (Some(r.label.to_string()), r.instance))
        .unwrap_or((None, None))
}

/// How many instance ids hold the activation gate on `cache_dir`, for `/metrics`: the length of
/// the last recorded [`ActivationAssessment::holders`]. Design § 16.6.6 B9.
///
/// `None` in exactly [`activation_blocker`]'s null cases, and also when the last verdict examined
/// no id (`ready_records_unreadable`, `no_ready_records`). `Some(0)` for `no_fresh_ready_records`
/// when every ledger id is vouched for: blocked, and by no instance.
pub(crate) fn activation_blocking_instance_count(cache_dir: &Path) -> Option<u64> {
    if fold_authority_active_latched(cache_dir) {
        return None;
    }
    ACTIVATION_BLOCKER
        .read()
        .ok()
        .and_then(|m| m.get(cache_dir).and_then(|r| r.holder_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A record asserting readiness on every precondition, so a test that wants to pin one
    /// blocker varies exactly that field. `repair_rotation_complete: true` is part of "ready"
    /// now — see [`ready_without_repair_rotation`] for the opposite.
    fn ready(instance: &str, rotation_complete: bool, incomplete: u64) -> ReadyRecord {
        ReadyRecord {
            schema: AUTHORITY_DOC_SCHEMA,
            instance_id: instance.to_string(),
            binary_version: "test".to_string(),
            fold_capable: true,
            rotation_complete,
            fold_incomplete_total: incomplete,
            repair_rotation_complete: true,
            repair_stalest_age_secs: Some(10),
            evict_worklist_capable: false,
            written_at: now_secs(),
        }
    }

    /// A record identical to [`ready`] except that precondition 3b is not satisfied.
    fn ready_without_repair_rotation(instance: &str) -> ReadyRecord {
        ReadyRecord {
            repair_rotation_complete: false,
            repair_stalest_age_secs: None,
            evict_worklist_capable: false,
            ..ready(instance, true, 0)
        }
    }

    fn write_ready(dir: &Path, record: &ReadyRecord) {
        let path = ready_dir(dir).join(format!("{}.{}", record.instance_id, READY_EXT));
        install_atomic(&path, &serde_json::to_string(record).unwrap()).unwrap();
    }

    fn instances(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    /// A volume with no epoch document is Phase L, and Phase L is the incumbent's behaviour.
    ///
    /// A failure implicates [`read_epoch`]'s `Ok(_) => EPOCH_LEGACY` arm. The assertion
    /// reads the value every gated call site branches on — the epoch itself — not a derived
    /// figure.
    #[test]
    fn an_absent_epoch_document_reads_as_phase_l() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(read_epoch(tmp.path()), EPOCH_LEGACY);
        assert!(!fold_authority_active(tmp.path()));
    }

    /// An epoch document that exists and cannot be understood is Phase L, **not** Phase
    /// Fold.
    ///
    /// This is the direction-safety arm: an unreadable epoch must not make the fold
    /// authoritative on no evidence. A failure implicates [`read_epoch`]'s `Err` arm and
    /// [`load_epoch_doc`]'s schema check.
    #[test]
    fn an_unreadable_or_unknown_schema_epoch_reads_as_phase_l() {
        // TWO temp dirs, not one, and this is not tidiness. A `LEGACY` reading is memoised
        // for `EPOCH_RECHECK_INTERVAL_SECS`, so writing a second document into the same
        // directory and re-reading within that window returns the CACHED answer without
        // touching the file — the second assertion would pass without the second document
        // ever being parsed, which is a vacuous assertion of exactly the shape this repo
        // keeps finding.
        let garbage = TempDir::new().unwrap();
        install_atomic(&epoch_path(garbage.path()), "not json at all").unwrap();
        assert_eq!(read_epoch(garbage.path()), EPOCH_LEGACY);

        // Parseable, but a schema from the future. Note `epoch: EPOCH_FOLD` — the document
        // asks for activation and is refused on the schema alone, which is the property.
        let future = TempDir::new().unwrap();
        let doc = AuthorityEpochDoc {
            schema: AUTHORITY_DOC_SCHEMA + 99,
            epoch: EPOCH_FOLD,
            activated_at: 1,
            activated_by: "peer".to_string(),
            binary_version: "future".to_string(),
        };
        install_atomic(
            &epoch_path(future.path()),
            &serde_json::to_string(&doc).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_epoch(future.path()),
            EPOCH_LEGACY,
            "an epoch document this binary cannot fully understand must not activate the \
             fold — an unknown schema is not evidence of readiness"
        );

        // And the control: the SAME document with a known schema DOES activate, in a third
        // directory. Without this, both assertions above are satisfied by a `read_epoch` that
        // can only ever return LEGACY.
        let known = TempDir::new().unwrap();
        let mut ok = doc.clone();
        ok.schema = AUTHORITY_DOC_SCHEMA;
        install_atomic(
            &epoch_path(known.path()),
            &serde_json::to_string(&ok).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_epoch(known.path()),
            EPOCH_FOLD,
            "control: a well-formed document must activate, or the two refusals above prove \
             nothing about the schema check"
        );
    }

    /// The gate refuses while an instance that has appended a staged credit has published no
    /// readiness record — **the check a pre-C binary cannot evade** — and permits once it
    /// has.
    ///
    /// A failure implicates the `ready_ids.contains` loop in [`activation_verdict`]. The
    /// state is constructed so the two arms differ in exactly one thing: whether
    /// `host:legacy` has a record. Both arms are asserted, so a gate that always permitted
    /// and a gate that always blocked both fail.
    #[test]
    fn activation_is_blocked_while_a_ledger_file_has_no_ready_record() {
        let tmp = TempDir::new().unwrap();
        write_ready(tmp.path(), &ready("host:new", true, 0));

        // `host:legacy` has a ledger file — it has credited staged ranges — and no record.
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:new", "host:legacy"])),
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host:legacy".to_string()
            ))
        );

        // The ONLY change: `host:legacy` publishes a record.
        write_ready(tmp.path(), &ready("host:legacy", true, 0));
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:new", "host:legacy"])),
            ActivationVerdict::Permitted
        );
    }

    /// An unknown record schema, an incomplete rotation and an observed incomplete fold each
    /// block, one at a time.
    ///
    /// A failure implicates the per-record loop in [`activation_verdict`]. Each case is
    /// asserted separately rather than as one composite so the verdict names which
    /// precondition failed, which is the operator's next action.
    #[test]
    fn each_per_record_precondition_blocks_on_its_own() {
        let tmp = TempDir::new().unwrap();

        let mut not_capable = ready("host:a", true, 0);
        not_capable.fold_capable = false;
        write_ready(tmp.path(), &not_capable);
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:a"])),
            ActivationVerdict::Blocked(ActivationBlocker::NotFoldCapable("host:a".to_string()))
        );

        write_ready(tmp.path(), &ready("host:a", false, 0));
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:a"])),
            ActivationVerdict::Blocked(ActivationBlocker::RotationIncomplete("host:a".to_string()))
        );

        write_ready(tmp.path(), &ready("host:a", true, 3));
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:a"])),
            ActivationVerdict::Blocked(ActivationBlocker::FoldIncompleteObserved(
                "host:a".to_string()
            ))
        );

        write_ready(tmp.path(), &ready("host:a", true, 0));
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:a"])),
            ActivationVerdict::Permitted
        );
    }

    /// An empty volume does not activate itself.
    ///
    /// Without this the gate would be **vacuously** satisfiable: with no ledger files and no
    /// records, the correspondence loop has nothing to check and every per-record loop is
    /// empty, so a first boot on a fresh cache would flip the epoch immediately — before any
    /// fold had ever completed. A failure implicates [`activation_verdict`]'s
    /// `records.is_empty()` guard.
    #[test]
    fn a_volume_with_no_ready_records_does_not_activate_vacuously() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(
            activation_verdict(tmp.path(), &BTreeSet::new()),
            ActivationVerdict::Blocked(ActivationBlocker::NoReadyRecords)
        );
    }

    /// Activation flips once, latches, and a second call is a no-op rather than a rewrite.
    ///
    /// A failure implicates [`activate`]'s `load_epoch_doc` short-circuit and its
    /// `EPOCH_LATCH.store`.
    #[test]
    fn activation_flips_once_and_latches() {
        let tmp = TempDir::new().unwrap();
        assert!(activate(tmp.path(), "host:a").unwrap());
        assert!(
            !activate(tmp.path(), "host:a").unwrap(),
            "a second activation must report it did nothing, not rewrite the document"
        );
        assert_eq!(epoch_latched_for(tmp.path()), EPOCH_FOLD);
        assert_eq!(read_epoch(tmp.path()), EPOCH_FOLD);
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host:a"])),
            ActivationVerdict::Blocked(ActivationBlocker::AlreadyActive)
        );

        // The latch is per DIRECTORY: activating one volume must not activate another. Lib
        // tests share a process, so a process-global latch would make every Phase L
        // assertion in this crate nondeterministic.
        let other = TempDir::new().unwrap();
        assert_eq!(
            epoch_latched_for(other.path()),
            EPOCH_LEGACY,
            "the epoch latch must be scoped to its own cache directory"
        );

        // A PEER's activation is observed on the `0 -> 1` re-read — the direction that IS
        // reachable through the memo, and the one the fleet actually takes: one instance
        // activates and the others notice.
        //
        // Asserted here because the monotonicity test above deliberately cannot cover it: that
        // one starts from a `FOLD` cell and never reaches the write path. This starts from a
        // `LEGACY` cell, which is the state every instance is in until a peer flips the
        // document, and it is the only state in which `read_epoch`'s cache-update line decides
        // anything.
        assert_eq!(
            read_epoch(other.path()),
            EPOCH_LEGACY,
            "fixture: the second volume must be read once, so a LEGACY cell exists to update"
        );
        let peer_doc = AuthorityEpochDoc {
            schema: AUTHORITY_DOC_SCHEMA,
            epoch: EPOCH_FOLD,
            activated_at: now_secs(),
            activated_by: "host:peer".to_string(),
            binary_version: "peer".to_string(),
        };
        install_atomic(
            &epoch_path(other.path()),
            &serde_json::to_string(&peer_doc).unwrap(),
        )
        .unwrap();
        // Without this the memo returns the cached LEGACY and the assertion below would pass
        // against a build that never re-reads at all.
        expire_epoch_check_for_tests(other.path());
        assert_eq!(
            read_epoch(other.path()),
            EPOCH_FOLD,
            "an instance in Phase L must observe a peer's activation on its next re-read, or \
             a fleet activates one node at a time forever"
        );
    }

    /// The latch survives the document becoming unreadable, and does **not** fall back to
    /// Phase L.
    ///
    /// This is the rollback rule expressed in code: a running process never observes `1 → 0`,
    /// because doing so would require the process to have been stopped. Constructed so the
    /// two figures diverge — the document says one thing, the latch another — which is the
    /// only state in which the latch's monotonicity is observable at all.
    ///
    /// Monotonicity has **two** implementations in [`read_epoch`] and this asserts both
    /// separately: the freshness short-circuit at the top, and the `max` at the bottom that
    /// decides on a genuine re-read. A failure names which one. See
    /// [`expire_epoch_check_for_tests`] for why one assertion was not enough and how that was
    /// discovered.
    #[test]
    fn the_epoch_latches_and_never_falls_back_to_legacy() {
        let tmp = TempDir::new().unwrap();
        assert!(activate(tmp.path(), "host:a").unwrap());
        assert_eq!(read_epoch(tmp.path()), EPOCH_FOLD);

        // Now make the document say Phase L, which is what a manual rollback writes. A
        // running process must ignore it.
        let doc = AuthorityEpochDoc {
            schema: AUTHORITY_DOC_SCHEMA,
            epoch: EPOCH_LEGACY,
            activated_at: 0,
            activated_by: "operator".to_string(),
            binary_version: "rollback".to_string(),
        };
        install_atomic(
            &epoch_path(tmp.path()),
            &serde_json::to_string(&doc).unwrap(),
        )
        .unwrap();
        // Arm 1: the freshness short-circuit. The cell says FOLD and is inside its window, so
        // the document is not even read.
        assert_eq!(
            read_epoch(tmp.path()),
            EPOCH_FOLD,
            "the latch is monotone: a rollback requires a full fleet STOP, so a live \
             process must not honour a 1 -> 0 edit"
        );

        // Arm 2: the same claim after the freshness window has expired, so the assertion does
        // not rest on the memo simply not having timed out yet.
        //
        // NOT a second mechanism, and the difference is worth stating because the first draft
        // of this test claimed it was. `read_epoch`'s short-circuit returns on a `FOLD` cell
        // BEFORE consulting the freshness window, so there is exactly one implementation of
        // monotonicity and this arm reaches the same line. It is kept because it rules out a
        // narrower defect — a short-circuit gated on freshness rather than on the epoch —
        // which arm 1 alone cannot distinguish. The `max` that used to sit at the bottom of
        // `read_epoch` was removed once the red side showed it could not fail; see the comment
        // there.
        expire_epoch_check_for_tests(tmp.path());
        assert_eq!(
            read_epoch(tmp.path()),
            EPOCH_FOLD,
            "the latch must hold after the memo's freshness window has expired, not only \
             while the cached reading is still fresh"
        );
        // And the document really does say 0, so the assertion above is about the latch
        // rather than about a failed write.
        assert_eq!(
            load_epoch_doc(tmp.path()).unwrap().unwrap().epoch,
            EPOCH_LEGACY
        );

        // And the control that makes the assertion above about the LATCH rather than about
        // `read_epoch` being unable to return LEGACY at all: the identical rollback document,
        // in a directory that never activated, reads LEGACY.
        let fresh = TempDir::new().unwrap();
        install_atomic(
            &epoch_path(fresh.path()),
            &serde_json::to_string(&doc).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_epoch(fresh.path()),
            EPOCH_LEGACY,
            "control: the same document read on a never-activated volume is Phase L"
        );
    }

    /// `Foreign_Write_Detection` fires on a pre-C `size_state.json` shape and not on the
    /// post-activation shape.
    ///
    /// Two-sided deliberately: a detector that always fired would stall the authority
    /// forever, which is as bad as one that never fires. A failure implicates
    /// [`detect_foreign_size_state`].
    #[test]
    fn a_pre_c_size_state_shape_is_detected_and_the_post_activation_shape_is_not() {
        // Pre-C: no schema, no epoch — both read as 0 under `serde(default)`.
        assert!(detect_foreign_size_state(
            SIZE_STATE_SCHEMA_LEGACY,
            EPOCH_LEGACY
        ));
        // Half-written: the fence bumped but the epoch field lost.
        assert!(detect_foreign_size_state(
            SIZE_STATE_SCHEMA_FOLD,
            EPOCH_LEGACY
        ));
        // The post-activation shape.
        assert!(!detect_foreign_size_state(
            SIZE_STATE_SCHEMA_FOLD,
            EPOCH_FOLD
        ));

        // And the arming: the fold refuses while a foreign write is live, and stops
        // refusing once it clears. `foreign_write_total` is process-global (it is a
        // per-instance `/metrics` counter), so the assertion is a DELTA against a baseline
        // read here rather than an absolute — an absolute would be a race against every other
        // test in this binary.
        let tmp = TempDir::new().unwrap();
        let before = foreign_write_total();
        assert!(!authority_block_kind(tmp.path()).is_some());
        record_authority_block(
            tmp.path(),
            AuthorityBlockKind::ForeignPreCWrite,
            "test fixture",
        );
        assert!(authority_block_kind(tmp.path()).is_some());
        assert!(foreign_write_total() > before);

        // Scoped to its own volume, same reason the epoch latch is.
        let other = TempDir::new().unwrap();
        assert!(!authority_block_kind(other.path()).is_some());

        clear_authority_block(tmp.path());
        assert!(!authority_block_kind(tmp.path()).is_some());
    }

    /// A `size_state.json` that could not be READ must not arm the same block, or emit the same
    /// message, as one that was read and found to carry a pre-C shape.
    ///
    /// # Why this needs its own test rather than being obvious
    ///
    /// The two are indistinguishable by content, and that is not a coincidence of the fixture:
    /// `SizeState::default()` — which is what a swallowed load error produces — carries
    /// `schema: 0` and `staged_authority_epoch: 0`, which is exactly the pre-C shape
    /// [`detect_foreign_size_state`] looks for. So an unreadable document used to accuse a binary
    /// that was not on the fleet, and send the operator at the fleet-stop rollback procedure in
    /// `docs/UPGRADING.md` for what is usually a permissions problem.
    ///
    /// The assertion is on the **kind**, which is what selects the message and the fold's retain
    /// label. Asserting only that the fold refuses would pass in every case — the refusal is
    /// correct in all three, and is not the thing that was wrong.
    ///
    /// A failure implicates [`record_authority_block`]'s per-kind idempotence or
    /// [`authority_block_kind`].
    #[test]
    fn an_unreadable_size_state_is_not_recorded_as_a_pre_c_write() {
        let unreadable = TempDir::new().unwrap();
        let foreign = TempDir::new().unwrap();
        let self_inflicted = TempDir::new().unwrap();

        record_authority_block(
            unreadable.path(),
            AuthorityBlockKind::SizeStateUnreadable,
            "load failed",
        );
        record_authority_block(
            foreign.path(),
            AuthorityBlockKind::ForeignPreCWrite,
            "persisted through a re-application",
        );
        record_authority_block(
            self_inflicted.path(),
            AuthorityBlockKind::FenceReapplyFailed,
            "lock unavailable",
        );

        // All three refuse — that half was already right and is not what this pins.
        assert!(authority_block_kind(unreadable.path()).is_some());
        assert!(authority_block_kind(foreign.path()).is_some());
        assert!(authority_block_kind(self_inflicted.path()).is_some());

        // The three are DISTINGUISHABLE, which is the property.
        assert_eq!(
            authority_block_kind(unreadable.path()),
            Some(AuthorityBlockKind::SizeStateUnreadable),
            "an unreadable size_state.json must not be recorded as a pre-C write: the two are \
             identical by content, because the in-memory default IS the pre-C shape, so only the \
             recorded kind can tell them apart"
        );
        assert_eq!(
            authority_block_kind(foreign.path()),
            Some(AuthorityBlockKind::ForeignPreCWrite)
        );
        assert_eq!(
            authority_block_kind(self_inflicted.path()),
            Some(AuthorityBlockKind::FenceReapplyFailed),
            "a fence re-application that failed is self-inflicted and must not implicate another \
             binary"
        );

        // The labels an operator actually sees must also be distinct, on BOTH surfaces.
        //
        // The first version of this asserted only `AuthorityBlockKind::label`, and that was the
        // wrong predicate: at the time nothing in production called it, so three distinct values
        // there were compatible with all three situations reporting one label on `/metrics`. The
        // figure that reaches the metric is `FoldRetainReason::label`, mapped from the kind in
        // `staged_fold`'s refusal gate — so that is what has to be asserted, and the kind's own
        // label is asserted because it is now a structured field on the WARN.
        use crate::staged_fold::FoldRetainReason;
        let metric_labels = [
            FoldRetainReason::ForeignAuthorityWrite.label(),
            FoldRetainReason::SizeStateUnreadable.label(),
            FoldRetainReason::SchemaFenceUnapplied.label(),
        ];
        assert_eq!(
            metric_labels.iter().collect::<BTreeSet<_>>().len(),
            3,
            "the three situations must reach staged_fold_incomplete_total under three DIFFERENT \
             reason labels, or the metric re-merges what the enum split and the operator is back \
             to one message for three remedies"
        );
        let log_labels = [
            AuthorityBlockKind::SizeStateUnreadable.label(),
            AuthorityBlockKind::ForeignPreCWrite.label(),
            AuthorityBlockKind::FenceReapplyFailed.label(),
        ];
        assert_eq!(
            log_labels.iter().collect::<BTreeSet<_>>().len(),
            3,
            "and the same on the journal side, where the kind's label is a structured field"
        );

        // Only the genuine foreign write moves the foreign-write counter. A counter that
        // incremented for all three would report a pre-C binary on an unreadable file.
        let before = foreign_write_total();
        let other = TempDir::new().unwrap();
        record_authority_block(
            other.path(),
            AuthorityBlockKind::SizeStateUnreadable,
            "load failed",
        );
        assert_eq!(
            foreign_write_total(),
            before,
            "staged_authority_foreign_write_total must count foreign writes only; an unreadable \
             document incrementing it is the same misattribution one layer down"
        );

        clear_authority_block(unreadable.path());
        clear_authority_block(foreign.path());
        clear_authority_block(self_inflicted.path());
        clear_authority_block(other.path());
    }

    /// § 16.6.6 precondition **3b** blocks activation, and it is a DIFFERENT blocker from the
    /// fold's own rotation.
    ///
    /// # What this catches that no existing test could
    ///
    /// Every other precondition test varies a field that already existed. This one varies
    /// `repair_rotation_complete` while holding `rotation_complete: true` and
    /// `fold_incomplete_total: 0` — a record that satisfies precondition 3a completely. Before
    /// this amendment `activation_verdict` carried a comment reading "Precondition 3, both halves"
    /// above the two 3a checks and had no 3b check at all, so this fixture was `Permitted`.
    ///
    /// A failure implicates `activation_verdict`'s 3b arm. The assertion reads the verdict the
    /// consolidator branches on, not a derived figure.
    #[test]
    fn an_incomplete_repair_rotation_blocks_activation_distinctly() {
        let tmp = TempDir::new().unwrap();
        write_ready(tmp.path(), &ready_without_repair_rotation("host-a"));
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host-a"])),
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-a".to_string()
            )),
            "precondition 3b — ledger-completeness repair having completed one rotation by range \
             identity — must block on its own. The fold's rotation cannot stand in for it: the \
             fold's candidate set comes only from the ledger, so a staged .meta no entry names is \
             absent from every fold figure while the fold reports a flawless rotation."
        );

        // The SAME record with 3b satisfied is permitted, so the fixture is not blocking for an
        // unrelated reason and the assertion above is attributable to 3b alone.
        let ok = TempDir::new().unwrap();
        write_ready(ok.path(), &ready("host-a", true, 0));
        assert_eq!(
            activation_verdict(ok.path(), &instances(&["host-a"])),
            ActivationVerdict::Permitted
        );

        // 3a and 3b are distinct blockers, not one figure read twice.
        assert_ne!(
            ActivationBlocker::RotationIncomplete("host-a".to_string()).label(),
            ActivationBlocker::RepairRotationIncomplete("host-a".to_string()).label(),
            "the two rotations need distinct labels: 'the fold is not keeping up' and 'the ledger \
             is not known to be complete' send an operator to different gauges"
        );
    }

    /// A `.ready` record written by a binary that predates precondition 3b deserialises as NOT
    /// having completed the repair rotation, and therefore blocks.
    ///
    /// # Why this is the safe default and why it is not expressed as a schema bump
    ///
    /// Such a binary ran no repair rotation, so there is no evidence for 3b and the gate must not
    /// infer any. `#[serde(default)]` on a `bool` gives `false`, which blocks.
    ///
    /// The alternative — bumping [`AUTHORITY_DOC_SCHEMA`] — would be actively unsafe, and the last
    /// assertion here is what stops someone doing it later. That constant also versions
    /// [`AuthorityEpochDoc`], and [`read_epoch`] maps an unknown epoch-document schema to
    /// [`EPOCH_LEGACY`]. So bumping it would silently DE-ACTIVATE a volume that had already
    /// flipped, handing the authority back to the incumbent with nothing in the log saying so. The
    /// serde route blocks a pending activation; the schema route would undo a completed one.
    #[test]
    fn a_ready_record_without_the_repair_field_blocks_rather_than_defaulting_open() {
        // A record as an older binary would have written it: every field it knew about asserts
        // readiness, and the 3b field is simply absent.
        let body = serde_json::json!({
            "schema": AUTHORITY_DOC_SCHEMA,
            "instance_id": "host-a",
            "binary_version": "2.9.13",
            "fold_capable": true,
            "rotation_complete": true,
            "fold_incomplete_total": 0,
            "written_at": now_secs(),
        })
        .to_string();
        let parsed: ReadyRecord = serde_json::from_str(&body).unwrap();
        assert!(
            !parsed.repair_rotation_complete,
            "an absent 3b field must read as NOT complete. A default of true would let a binary \
             that never ran the repair rotation vote for activation."
        );
        assert_eq!(parsed.repair_stalest_age_secs, None);

        let tmp = TempDir::new().unwrap();
        let path = ready_dir(tmp.path()).join(format!("host-a.{}", READY_EXT));
        install_atomic(&path, &body).unwrap();
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host-a"])),
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-a".to_string()
            ))
        );

        // The epoch document still parses at this schema, which is the property that makes the
        // serde route correct and a schema bump wrong. If someone bumps `AUTHORITY_DOC_SCHEMA`,
        // this fails and the message says why.
        let epoch_doc = AuthorityEpochDoc {
            schema: AUTHORITY_DOC_SCHEMA,
            epoch: EPOCH_FOLD,
            activated_at: now_secs(),
            activated_by: "host-a".to_string(),
            binary_version: "test".to_string(),
        };
        let activated = TempDir::new().unwrap();
        install_atomic(
            &epoch_path(activated.path()),
            &serde_json::to_string(&epoch_doc).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_epoch(activated.path()),
            EPOCH_FOLD,
            "an already-activated volume must still read as Phase Fold. If this fails because \
             AUTHORITY_DOC_SCHEMA was bumped to express the new .ready field, that bump has \
             silently de-activated every volume that had already flipped — use serde defaults on \
             the record instead."
        );
    }

    /// A record written `age_secs` ago, asserting every precondition.
    fn aged(instance: &str, age_secs: u64) -> ReadyRecord {
        ReadyRecord {
            written_at: now_secs().saturating_sub(age_secs),
            ..ready(instance, true, 0)
        }
    }

    /// Comfortably past the bound, so second-boundary timing cannot flip a verdict.
    const BEYOND: u64 = READY_RECORD_RETIRE_AGE_SECS + 600;

    /// A dead pid's record frozen with a false flag, beside the same host's newer pid with a
    /// fresh all-true record, both with ledger files: activates. The fleet shape from the
    /// 2026-09-25 probe — every restart mints a new id and the old one's record never changes.
    ///
    /// # RED SIDE
    ///
    /// On the pre-amendment gate, which checks every record, the dead pid's frozen
    /// `repair_rotation_complete: false` returns `RepairRotationIncomplete("host-a:100")` for
    /// ever.
    #[test]
    fn a_dead_pids_stale_record_retires_and_the_live_pids_fresh_record_activates() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(
            dir,
            &ReadyRecord {
                repair_rotation_complete: false,
                repair_stalest_age_secs: None,
                evict_worklist_capable: false,
                ..aged("host-a:100", BEYOND)
            },
        );
        write_ready(dir, &ready("host-a:200", true, 0));
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-a:200"])),
            ActivationVerdict::Permitted,
            "a fold_capable record older than READY_RECORD_RETIRE_AGE_SECS must stop counting \
             against activation while its instance still satisfies the ledger correspondence"
        );
    }

    /// A ledger id with no record at all still blocks, naming it, beside fresh all-true records
    /// and a retired record. Record-less ids are the half that waits on Ed's ruling.
    #[test]
    fn a_record_less_ledger_id_still_blocks_beside_fresh_and_retired_records() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", BEYOND));
        write_ready(dir, &ready("host-a:200", true, 0));
        write_ready(dir, &ready("host-b:300", true, 0));
        assert_eq!(
            activation_verdict(
                dir,
                &instances(&["host-a:100", "host-a:200", "host-b:300", "host-old:5"])
            ),
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            )),
            "an id that published no record may be a live pre-C writer; retirement must never \
             cover it"
        );
    }

    /// Every record stale: blocked, with a reason naming that, even when every stale record
    /// asserts all preconditions. Retired evidence alone must never activate.
    ///
    /// # RED SIDE
    ///
    /// The pre-amendment gate returns `Permitted` for the all-true case: it activated on stale
    /// records alone.
    #[test]
    fn every_record_stale_blocks_with_no_fresh_ready_records() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", BEYOND));
        write_ready(dir, &aged("host-b:200", BEYOND));
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-b:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::NoFreshReadyRecords),
            "stale all-true records are not current evidence"
        );

        let flagged = TempDir::new().unwrap();
        write_ready(
            flagged.path(),
            &ReadyRecord {
                rotation_complete: false,
                ..aged("host-a:100", BEYOND)
            },
        );
        assert_eq!(
            activation_verdict(flagged.path(), &instances(&["host-a:100"])),
            ActivationVerdict::Blocked(ActivationBlocker::NoFreshReadyRecords),
        );
    }

    /// A fresh record with a false flag blocks, naming its instance, whether or not a retired
    /// record sits beside it: every current record is checked in full.
    #[test]
    fn a_fresh_record_with_a_false_flag_still_blocks() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", BEYOND));
        write_ready(dir, &ready("host-a:200", false, 0));
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-a:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::RotationIncomplete(
                "host-a:200".to_string()
            ))
        );

        let incomplete = TempDir::new().unwrap();
        write_ready(incomplete.path(), &ready("host-a:200", true, 3));
        assert_eq!(
            activation_verdict(incomplete.path(), &instances(&["host-a:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::FoldIncompleteObserved(
                "host-a:200".to_string()
            ))
        );

        let repair = TempDir::new().unwrap();
        write_ready(repair.path(), &ready_without_repair_rotation("host-a:200"));
        assert_eq!(
            activation_verdict(repair.path(), &instances(&["host-a:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-a:200".to_string()
            ))
        );
    }

    /// A stale record that is NOT evidence of C-capable code — an unknown schema, or
    /// `fold_capable: false` — is never retired, so it keeps blocking.
    #[test]
    fn a_stale_record_that_is_not_fold_capable_is_never_retired() {
        for record in [
            ReadyRecord {
                fold_capable: false,
                ..aged("host-a:100", BEYOND)
            },
            ReadyRecord {
                schema: AUTHORITY_DOC_SCHEMA + 1,
                ..aged("host-a:100", BEYOND)
            },
        ] {
            let tmp = TempDir::new().unwrap();
            write_ready(tmp.path(), &record);
            write_ready(tmp.path(), &ready("host-a:200", true, 0));
            assert_eq!(
                activation_verdict(tmp.path(), &instances(&["host-a:100", "host-a:200"])),
                ActivationVerdict::Blocked(ActivationBlocker::NotFoldCapable(
                    "host-a:100".to_string()
                ))
            );
        }
    }

    /// A record just inside the bound is current and is checked in full.
    #[test]
    fn a_record_inside_the_retirement_bound_still_counts() {
        let tmp = TempDir::new().unwrap();
        write_ready(
            tmp.path(),
            &ReadyRecord {
                repair_rotation_complete: false,
                ..aged("host-a:100", READY_RECORD_RETIRE_AGE_SECS - 600)
            },
        );
        write_ready(tmp.path(), &ready("host-a:200", true, 0));
        assert_eq!(
            activation_verdict(tmp.path(), &instances(&["host-a:100", "host-a:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-a:100".to_string()
            ))
        );
    }

    /// The evaluating instance's current read, asserting every precondition.
    fn now_read(instance: &str) -> CurrentReadiness {
        CurrentReadiness {
            instance_id: instance.to_string(),
            rotation_complete: true,
            repair_rotation_complete: true,
            fold_incomplete_total: 0,
        }
    }

    /// The volume's shape: dead pids' records under 24 h old, frozen with a rotation flag
    /// false, beside the evaluating instance's fresh all-true record. With the evaluating
    /// instance's current read supplied, the frozen flags do not hold the gate. Design § 16.6.6
    /// B10.2.
    ///
    /// # RED SIDE
    ///
    /// On the B4 gate, which checks every fresh record's rotation flags, this is
    /// `Blocked(RepairRotationIncomplete("host-a:100"))`: the first dead record, frozen.
    #[test]
    fn dead_pids_fresh_records_with_false_rotation_flags_do_not_hold_the_gate() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(
            dir,
            &ReadyRecord {
                repair_rotation_complete: false,
                repair_stalest_age_secs: None,
                evict_worklist_capable: false,
                ..aged("host-a:100", 2 * 3_600)
            },
        );
        write_ready(
            dir,
            &ReadyRecord {
                rotation_complete: false,
                ..aged("host-b:101", 5 * 3_600)
            },
        );
        write_ready(dir, &ready("host-c:200", true, 0));
        let ledger = instances(&["host-a:100", "host-b:101", "host-c:200"]);
        assert_eq!(
            activation_assessment_current(dir, &ledger, &now_read("host-c:200")).verdict,
            ActivationVerdict::Permitted,
            "a dead process's rotation flags are its snapshot of shared documents; the \
             evaluating instance's current read of the same documents supersedes them"
        );
        // The same volume with no current read is B4's gate, unchanged.
        assert_eq!(
            activation_verdict(dir, &ledger),
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-a:100".to_string()
            ))
        );
    }

    /// Preservation for the lightweight record a proxy republishes on every non-idle cycle and
    /// at startup (both rotation flags `false`, no repair age): on its own it neither blocks nor
    /// permits activation. `assess` reads a peer's record only through `record_process_blocker`
    /// (schema, `fold_capable`, `fold_incomplete_total`), and takes both rotations from the
    /// evaluating instance's current read alone.
    #[test]
    fn a_lightweight_record_neither_blocks_nor_permits_activation_on_its_own() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let lightweight = |instance: &str| ReadyRecord {
            rotation_complete: false,
            repair_rotation_complete: false,
            repair_stalest_age_secs: None,
            evict_worklist_capable: true,
            ..ready(instance, false, 0)
        };
        write_ready(dir, &lightweight("host-a:100"));
        write_ready(dir, &lightweight("host-c:200"));
        let ledger = instances(&["host-a:100", "host-c:200"]);
        assert_eq!(
            activation_assessment_current(dir, &ledger, &now_read("host-c:200")).verdict,
            ActivationVerdict::Permitted,
            "a peer's lightweight record must not block an evaluator whose current read is complete"
        );
        assert_eq!(
            activation_assessment_current(
                dir,
                &ledger,
                &CurrentReadiness {
                    rotation_complete: false,
                    ..now_read("host-c:200")
                }
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::RotationIncomplete(
                "host-c:200".to_string()
            )),
            "lightweight records must not permit activation when the evaluator's read is incomplete"
        );
    }

    /// The evaluating instance's own current read blocks when it says a rotation is incomplete,
    /// naming the evaluating instance, even when every record on the volume says complete.
    ///
    /// # RED SIDE
    ///
    /// On the B4 gate the all-true volume is `Permitted`: frozen `true` flags would activate
    /// against a current read that says the rotation is not complete.
    #[test]
    fn the_evaluating_instances_current_read_blocks_on_an_incomplete_rotation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", 2 * 3_600));
        write_ready(dir, &ready("host-c:200", true, 0));
        let ledger = instances(&["host-a:100", "host-c:200"]);
        assert_eq!(
            activation_assessment_current(
                dir,
                &ledger,
                &CurrentReadiness {
                    repair_rotation_complete: false,
                    ..now_read("host-c:200")
                }
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-c:200".to_string()
            ))
        );
        assert_eq!(
            activation_assessment_current(
                dir,
                &ledger,
                &CurrentReadiness {
                    rotation_complete: false,
                    ..now_read("host-c:200")
                }
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::RotationIncomplete(
                "host-c:200".to_string()
            ))
        );

        // And beside a dead record frozen false, it is still the current read that is named.
        write_ready(
            dir,
            &ReadyRecord {
                repair_rotation_complete: false,
                repair_stalest_age_secs: None,
                evict_worklist_capable: false,
                ..aged("host-a:100", 2 * 3_600)
            },
        );
        assert_eq!(
            activation_assessment_current(
                dir,
                &ledger,
                &CurrentReadiness {
                    repair_rotation_complete: false,
                    ..now_read("host-c:200")
                }
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-c:200".to_string()
            ))
        );
    }

    /// A fresh record with a non-zero `fold_incomplete_total` blocks, naming it, with and
    /// without a current read: that counter is per-process, so no other instance's read
    /// re-derives it. Also the evaluating instance's own live counter.
    #[test]
    fn a_fresh_record_with_an_observed_incomplete_fold_blocks_before_and_after() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(
            dir,
            &ReadyRecord {
                fold_incomplete_total: 2,
                ..aged("host-a:100", 2 * 3_600)
            },
        );
        write_ready(dir, &ready("host-c:200", true, 0));
        let ledger = instances(&["host-a:100", "host-c:200"]);
        let expected = ActivationVerdict::Blocked(ActivationBlocker::FoldIncompleteObserved(
            "host-a:100".to_string(),
        ));
        assert_eq!(activation_verdict(dir, &ledger), expected);
        assert_eq!(
            activation_assessment_current(dir, &ledger, &now_read("host-c:200")).verdict,
            expected
        );

        let own = TempDir::new().unwrap();
        write_ready(own.path(), &ready("host-c:200", true, 0));
        assert_eq!(
            activation_assessment_current(
                own.path(),
                &instances(&["host-c:200"]),
                &CurrentReadiness {
                    fold_incomplete_total: 1,
                    ..now_read("host-c:200")
                }
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::FoldIncompleteObserved(
                "host-c:200".to_string()
            )),
            "the live counter is checked, not only the one the record carries"
        );
    }

    /// A current read is not a fresh record: with every record retired it still blocks as
    /// `NoFreshReadyRecords`, and a record-less ledger id still blocks. B10.2 (i) and (ii).
    #[test]
    fn a_current_read_keeps_both_evidence_floors_and_precondition_1() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", BEYOND));
        assert_eq!(
            activation_assessment_current(
                dir,
                &instances(&["host-a:100"]),
                &now_read("host-c:200")
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::NoFreshReadyRecords)
        );

        let empty = TempDir::new().unwrap();
        assert_eq!(
            activation_assessment_current(empty.path(), &BTreeSet::new(), &now_read("host-c:200"))
                .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::NoReadyRecords)
        );

        let recordless = TempDir::new().unwrap();
        write_ready(recordless.path(), &ready("host-c:200", true, 0));
        assert_eq!(
            activation_assessment_current(
                recordless.path(),
                &instances(&["host-c:200", "host-old:5"]),
                &now_read("host-c:200")
            )
            .verdict,
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            ))
        );
    }

    /// A startup record is created only when absent, and never replaces a published one.
    /// Design § 16.6.6 B10.3.
    #[test]
    fn a_startup_record_is_written_only_when_absent() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        assert!(publish_startup_ready_record(dir, "host-c:200", 0).unwrap());
        let written = read_ready_records(dir).unwrap();
        assert_eq!(
            written.len(),
            1,
            "exactly the record, and no temp file left"
        );
        let startup = &written[0];
        assert!(startup.fold_capable && startup.schema == AUTHORITY_DOC_SCHEMA);
        assert!(!startup.rotation_complete && !startup.repair_rotation_complete);
        assert_eq!(
            (
                startup.fold_incomplete_total,
                startup.repair_stalest_age_secs
            ),
            (0, None)
        );
        assert_eq!(std::fs::read_dir(ready_dir(dir)).unwrap().count(), 1);

        // A real publication is never replaced by a later startup call.
        publish_ready_record(dir, "host-c:200", true, 0, true, Some(10)).unwrap();
        assert!(!publish_startup_ready_record(dir, "host-c:200", 0).unwrap());
        let after = read_ready_records(dir).unwrap();
        assert!(after[0].rotation_complete && after[0].repair_rotation_complete);
        assert_eq!(std::fs::read_dir(ready_dir(dir)).unwrap().count(), 1);

        // Beside a current read, the startup record's false flags hold nobody.
        write_ready(dir, &ready("host-c:200", true, 0));
        assert!(publish_startup_ready_record(dir, "host-d:300", 0).unwrap());
        assert_eq!(
            activation_assessment_current(
                dir,
                &instances(&["host-c:200", "host-d:300"]),
                &now_read("host-c:200")
            )
            .verdict,
            ActivationVerdict::Permitted
        );
    }

    /// Write a schema-3 shard ledger file for `instance` holding one entry per timestamp in
    /// `appended_at_secs`, plus any `extra_lines` verbatim.
    fn write_ledger_file(
        dir: &Path,
        instance: &str,
        appended_at_secs: &[u64],
        extra_lines: &[&str],
    ) {
        let path =
            crate::write_ledger::WriteLedger::shard_ledger_path(dir, "bucket", "00", instance);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = String::from("{\"schema\":3,\"max_seq_ever\":0}\n");
        for (i, secs) in appended_at_secs.iter().enumerate() {
            let entry = crate::write_ledger::WriteLedgerEntry {
                timestamp: std::time::UNIX_EPOCH + std::time::Duration::from_secs(*secs),
                cache_key: format!("bucket/object-{i}"),
                range_start: 0,
                range_end: 1023,
                compressed_size: 1024,
                instance_id: instance.to_string(),
                seq: None,
            };
            body.push_str(&serde_json::to_string(&entry).unwrap());
            body.push('\n');
        }
        for line in extra_lines {
            body.push_str(line);
            body.push('\n');
        }
        std::fs::write(path, body).unwrap();
    }

    /// A pid-reusing pre-C writer: a dead C-capable pid left a retired record, and entries have
    /// since been appended under the same id. The id no longer counts as vouched for, so the
    /// gate blocks naming it, as it does for any record-less ledger id.
    ///
    /// # RED SIDE
    ///
    /// On the gate, which lets any retired record satisfy precondition 1 regardless of the
    /// ledger, the fresh record beside it makes this `Permitted`.
    #[test]
    fn a_retired_records_instance_that_appended_since_blocks() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", BEYOND));
        write_ready(dir, &ready("host-a:200", true, 0));
        // Appended now, i.e. BEYOND seconds after the retired record was written: far past any
        // margin a dead process's own late appends could need.
        write_ledger_file(dir, "host-a:100", &[now_secs()], &[]);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-a:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-a:100".to_string()
            )),
            "an id appended to after its only record retired may be a pre-C binary that reused \
             the pid; the retired record must not vouch for it"
        );
    }

    /// The liveness half: a dead C-capable pid whose own last appends landed shortly after its
    /// last publication still retires, and a header-only file (entries compacted away) is no
    /// evidence of anything. Without this the rule would block permanently on the dead ids
    /// retirement exists for.
    #[test]
    fn a_retired_records_own_late_appends_and_a_drained_file_still_retire() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let dead = aged("host-a:100", BEYOND);
        write_ready(dir, &dead);
        write_ready(dir, &aged("host-b:300", BEYOND));
        write_ready(dir, &ready("host-a:200", true, 0));
        write_ledger_file(
            dir,
            "host-a:100",
            &[dead.written_at - 30, dead.written_at + 60],
            &[],
        );
        write_ledger_file(dir, "host-b:300", &[], &[]);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-a:200", "host-b:300"])),
            ActivationVerdict::Permitted,
        );
    }

    /// Fails closed: an unparseable line in a file named for a retiring id may be the append
    /// that would have shown a later writer, so the id is not vouched for.
    #[test]
    fn an_unparseable_ledger_line_under_a_retiring_id_blocks() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let dead = aged("host-a:100", BEYOND);
        write_ready(dir, &dead);
        write_ready(dir, &ready("host-a:200", true, 0));
        write_ledger_file(
            dir,
            "host-a:100",
            &[dead.written_at - 30],
            &["{\"timestamp\":17"],
        );
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-a:200"])),
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-a:100".to_string()
            )),
        );
    }

    const DAY: u64 = 86_400;

    /// Backdate the modification time of `path` to `age_secs` before now.
    fn backdate(path: &Path, age_secs: u64) {
        let when = SystemTime::now() - std::time::Duration::from_secs(age_secs);
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    /// A header-only shard ledger file for `instance` (a dead id after compaction, B3), last
    /// modified `age_secs` ago. Returns its path.
    fn quiet_ledger_file(dir: &Path, instance: &str, age_secs: u64) -> PathBuf {
        write_ledger_file(dir, instance, &[], &[]);
        let path =
            crate::write_ledger::WriteLedger::shard_ledger_path(dir, "bucket", "00", instance);
        backdate(&path, age_secs);
        path
    }

    /// A record-less id whose ledger files were last modified just over a day ago (25 hours),
    /// beside a fresh all-true record: activates. The 2026-09-25 fleet shape — 32 record-less ids, all last
    /// written before 2026-09-12. Design § 16.6.6 B8.
    ///
    /// # RED SIDE
    ///
    /// On the gate every record-less id blocks for ever, so this is
    /// `LedgerWithoutReadyRecord("host-old:5")`.
    #[test]
    fn a_record_less_id_quiet_for_just_over_a_day_retires_beside_a_fresh_record() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &ready("host-a:200", true, 0));
        quiet_ledger_file(dir, "host-old:5", DAY + 3600);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:200", "host-old:5"])),
            ActivationVerdict::Permitted,
            "a record-less id whose ledger has not been modified for a day must stop counting"
        );
    }

    /// A record-less id modified 23 hours ago still blocks, naming it — and so does one whose
    /// ledger has one quiet file and one recent one: every file named for the id must be quiet.
    #[test]
    fn a_record_less_id_modified_inside_a_day_still_blocks() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &ready("host-a:200", true, 0));
        quiet_ledger_file(dir, "host-old:5", DAY - 3600);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:200", "host-old:5"])),
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            )),
        );

        let mixed = TempDir::new().unwrap();
        let dir = mixed.path();
        write_ready(dir, &ready("host-a:200", true, 0));
        quiet_ledger_file(dir, "host-old:5", 8 * DAY);
        let legacy = crate::write_ledger::WriteLedger::legacy_path_for(dir, "host-old:5");
        std::fs::write(&legacy, "").unwrap();
        backdate(&legacy, DAY - 3600);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:200", "host-old:5"])),
            ActivationVerdict::Blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            )),
            "one recently modified file named for the id is enough to keep it blocking"
        );
    }

    /// A quiet record-less id adds no evidence: with no fresh record on the volume, the verdict
    /// is `NoFreshReadyRecords`.
    ///
    /// # RED SIDE
    ///
    /// On the gate the record-less id is reported first:
    /// `LedgerWithoutReadyRecord("host-old:5")`.
    #[test]
    fn a_quiet_record_less_id_without_a_fresh_record_blocks_with_no_fresh_ready_records() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", BEYOND));
        quiet_ledger_file(dir, "host-old:5", 8 * DAY);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-old:5"])),
            ActivationVerdict::Blocked(ActivationBlocker::NoFreshReadyRecords),
        );
    }

    /// An id B7 declines — its only record retired and entries appended since — counts as
    /// record-less, so the same one-day rule applies to it.
    ///
    /// # RED SIDE
    ///
    /// On the gate: `LedgerWithoutReadyRecord("host-a:100")`.
    #[test]
    fn a_b7_declined_id_quiet_for_eight_days_retires() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &aged("host-a:100", 10 * DAY));
        write_ready(dir, &ready("host-a:200", true, 0));
        write_ledger_file(dir, "host-a:100", &[now_secs() - 9 * DAY], &[]);
        let path =
            crate::write_ledger::WriteLedger::shard_ledger_path(dir, "bucket", "00", "host-a:100");
        backdate(&path, 8 * DAY);
        assert_eq!(
            activation_verdict(dir, &instances(&["host-a:100", "host-a:200"])),
            ActivationVerdict::Permitted,
        );
    }

    /// Every holder is collected in a stable order — unvouched ledger ids in id order, then fresh
    /// failing records in id order — and the verdict names the first. A quiet id is not a holder.
    #[test]
    fn every_holder_is_collected_and_the_verdict_names_the_first() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &ready("host-c:3", false, 0));
        write_ready(dir, &ready_without_repair_rotation("host-b:2"));
        write_ready(dir, &ready("host-a:1", true, 0));
        quiet_ledger_file(dir, "host-old:9", 8 * DAY);
        quiet_ledger_file(dir, "host-old:5", DAY - 3600);
        let assessment = activation_assessment(
            dir,
            &instances(&["host-a:1", "host-old:5", "host-old:9", "host-z:7"]),
        );
        assert_eq!(
            assessment.holders,
            Some(vec![
                ActivationBlocker::LedgerWithoutReadyRecord("host-old:5".to_string()),
                ActivationBlocker::LedgerWithoutReadyRecord("host-z:7".to_string()),
                ActivationBlocker::RepairRotationIncomplete("host-b:2".to_string()),
                ActivationBlocker::RotationIncomplete("host-c:3".to_string()),
            ])
        );
        assert_eq!(
            assessment.verdict,
            blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            ))
        );

        // No unvouched ledger id: the first failing fresh record, in id order.
        let only_records = activation_assessment(dir, &instances(&["host-a:1"]));
        assert_eq!(
            only_records.verdict,
            blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-b:2".to_string()
            ))
        );
        assert_eq!(only_records.holders.map(|h| h.len()), Some(2));

        // Examined no id: no holder list.
        let empty = TempDir::new().unwrap();
        assert_eq!(
            activation_assessment(empty.path(), &instances(&["host-a:1"])).holders,
            None
        );
    }

    /// Each id is logged at INFO once when it starts holding the gate, and the count reaches the
    /// gauge; its null cases hold.
    ///
    /// # RED SIDE
    ///
    /// With the "already logged" filter in `record_activation_assessment` removed, the repeated
    /// assessment logs both holders again and the second assertion fails.
    #[test]
    fn each_newly_holding_id_logs_once_and_the_count_is_published() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let lw = |id: &str| ActivationBlocker::LedgerWithoutReadyRecord(id.to_string());
        let two = ActivationAssessment {
            verdict: blocked(lw("host-a:1")),
            holders: Some(vec![lw("host-a:1"), lw("host-b:2")]),
        };
        assert_eq!(activation_blocking_instance_count(dir), None);

        let (logged, log) = capture_info(|| record_activation_assessment(dir, &two));
        assert!(logged);
        assert!(
            log.contains("held back by instance host-a:1")
                && log.contains("held back by instance host-b:2")
                && log.contains("retire-instance host-b:2"),
            "{log}"
        );
        assert_eq!(activation_blocking_instance_count(dir), Some(2));

        let (logged, log) = capture_info(|| record_activation_assessment(dir, &two));
        assert!(
            !logged && log.is_empty(),
            "no id is new, so nothing is logged; got: {log}"
        );

        let three = ActivationAssessment {
            verdict: blocked(lw("host-a:1")),
            holders: Some(vec![lw("host-a:1"), lw("host-b:2"), lw("host-c:3")]),
        };
        let (_, log) = capture_info(|| record_activation_assessment(dir, &three));
        assert!(
            log.contains("held back by instance host-c:3")
                && !log.contains("held back by instance host-a:1"),
            "only the new holder is logged; got: {log}"
        );
        assert_eq!(activation_blocking_instance_count(dir), Some(3));

        // A verdict that examined no id publishes null and keeps the logged set.
        record_activation_verdict(dir, &blocked(ActivationBlocker::ReadyRecordsUnreadable));
        assert_eq!(activation_blocking_instance_count(dir), None);
        let (_, log) = capture_info(|| record_activation_assessment(dir, &three));
        assert!(!log.contains("held back by"), "{log}");

        // Blocked by no instance: 0, not null.
        record_activation_assessment(
            dir,
            &ActivationAssessment {
                verdict: blocked(ActivationBlocker::NoFreshReadyRecords),
                holders: Some(vec![]),
            },
        );
        assert_eq!(activation_blocking_instance_count(dir), Some(0));

        record_activation_verdict(dir, &ActivationVerdict::Permitted);
        assert_eq!(activation_blocking_instance_count(dir), None);

        record_activation_assessment(dir, &three);
        activate(dir, "peer:1").unwrap();
        assert_eq!(
            activation_blocking_instance_count(dir),
            None,
            "null once the epoch is 1"
        );
    }

    /// `retire-instance` refuses a live-looking id, an unknown id and an unsafe id, and writes
    /// nothing in any of them.
    ///
    /// # RED SIDE
    ///
    /// With the refusal-window comparison in `retire_instance` removed, the first case writes a
    /// marker and the first assertion fails.
    #[test]
    fn retire_instance_refuses_a_recently_modified_unknown_or_unsafe_id() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        quiet_ledger_file(dir, "host-live:1", 600);
        match retire_instance(dir, "host-live:1") {
            Err(RetireRefusal::RecentlyModified { age_secs }) => {
                assert!((590..=660).contains(&age_secs), "{age_secs}")
            }
            other => panic!("a ledger modified 10 minutes ago must be refused, got {other:?}"),
        }
        assert_eq!(
            retire_instance(dir, "host-none:2"),
            Err(RetireRefusal::NoLedgerFiles)
        );
        assert_eq!(
            retire_instance(dir, "../host"),
            Err(RetireRefusal::UnsafeInstanceId)
        );
        assert!(
            !retired_instances_dir(dir).exists(),
            "a refusal must write nothing"
        );
        let message = RetireRefusal::RecentlyModified { age_secs: 600 }.to_string();
        assert!(
            message.contains("600 s ago") && message.contains("3600 s"),
            "{message}"
        );
    }

    /// Retiring an id quiet for 2 hours writes its marker and lets the gate activate; a later
    /// modification of its ledger voids the marker and it blocks again.
    ///
    /// # RED SIDE
    ///
    /// With the marker lookup in `activation_assessment` removed, the verdict after retirement is
    /// still `LedgerWithoutReadyRecord("host-old:5")`.
    #[test]
    fn a_retired_id_stops_blocking_until_its_ledger_is_modified_again() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        write_ready(dir, &ready("host-a:200", true, 0));
        let ledger = quiet_ledger_file(dir, "host-old:5", 2 * 3_600);
        let ids = instances(&["host-a:200", "host-old:5"]);
        assert_eq!(
            activation_verdict(dir, &ids),
            blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            ))
        );

        let marker = retire_instance(dir, "host-old:5").expect("quiet for 2 h, so retired");
        let path = retired_instances_dir(dir).join("host-old:5.retired");
        let on_disk: RetirementMarker =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(on_disk, marker);
        assert_eq!(on_disk.schema, RETIREMENT_MARKER_SCHEMA);
        assert_eq!(
            on_disk.ledger_last_modified_ns,
            unix_nanos(std::fs::metadata(&ledger).unwrap().modified().unwrap())
        );
        assert_eq!(activation_verdict(dir, &ids), ActivationVerdict::Permitted);

        // A pid-reusing writer appends: the marker no longer covers the id.
        backdate(&ledger, 0);
        assert_eq!(
            activation_verdict(dir, &ids),
            blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                "host-old:5".to_string()
            ))
        );
    }

    /// A marker naming another id than its file, or of an unknown schema, is ignored.
    #[test]
    fn a_mismatched_or_unknown_schema_marker_is_ignored() {
        for marker in [
            RetirementMarker {
                schema: RETIREMENT_MARKER_SCHEMA,
                instance_id: "host-other:6".to_string(),
                retired_at: 0,
                retired_by: "ops".to_string(),
                binary_version: "test".to_string(),
                ledger_last_modified_ns: u64::MAX,
            },
            RetirementMarker {
                schema: RETIREMENT_MARKER_SCHEMA + 1,
                instance_id: "host-old:5".to_string(),
                retired_at: 0,
                retired_by: "ops".to_string(),
                binary_version: "test".to_string(),
                ledger_last_modified_ns: u64::MAX,
            },
        ] {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            write_ready(dir, &ready("host-a:200", true, 0));
            quiet_ledger_file(dir, "host-old:5", 2 * 3_600);
            install_atomic(
                &retired_instances_dir(dir).join("host-old:5.retired"),
                &serde_json::to_string(&marker).unwrap(),
            )
            .unwrap();
            assert_eq!(
                activation_verdict(dir, &instances(&["host-a:200", "host-old:5"])),
                blocked(ActivationBlocker::LedgerWithoutReadyRecord(
                    "host-old:5".to_string()
                ))
            );
        }
    }

    /// Run `f` under a thread-local INFO subscriber and return what it logged.
    fn capture_info<R>(f: impl FnOnce() -> R) -> (R, String) {
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
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        let out = tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        (out, String::from_utf8(bytes).unwrap())
    }

    fn blocked(b: ActivationBlocker) -> ActivationVerdict {
        ActivationVerdict::Blocked(b)
    }

    /// The gate's blocker is logged at INFO once per change of label or named instance, and a
    /// repeated verdict logs nothing.
    ///
    /// # RED SIDE
    ///
    /// With `record_activation_verdict`'s `previous == next` early return removed, the repeated
    /// verdict logs a second time and the second assertion fails.
    #[test]
    fn an_activation_blocker_change_logs_once_and_a_repeat_does_not() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let first = blocked(ActivationBlocker::LedgerWithoutReadyRecord(
            "host-a:101".to_string(),
        ));

        let (logged, log) = capture_info(|| record_activation_verdict(dir, &first));
        assert!(logged, "the first recorded blocker must log");
        assert!(
            log.contains("INFO")
                && log.contains("reason=ledger_without_ready_record")
                && log.contains("blocking_instance=host-a:101"),
            "the INFO line must name the label and the instance; got: {log}"
        );

        let (logged, log) = capture_info(|| record_activation_verdict(dir, &first));
        assert!(
            !logged && log.is_empty(),
            "an unchanged blocker must not log again; got: {log}"
        );

        // Same label, different instance: a change.
        let other = blocked(ActivationBlocker::LedgerWithoutReadyRecord(
            "host-b:202".to_string(),
        ));
        let (logged, log) = capture_info(|| record_activation_verdict(dir, &other));
        assert!(
            logged && log.contains("blocking_instance=host-b:202"),
            "{log}"
        );

        // Same instance, different label: a change.
        let relabelled = blocked(ActivationBlocker::RotationIncomplete(
            "host-b:202".to_string(),
        ));
        let (logged, log) = capture_info(|| record_activation_verdict(dir, &relabelled));
        assert!(
            logged && log.contains("reason=rotation_incomplete"),
            "{log}"
        );

        // Permitted clears silently; the next blocker logs again.
        let (logged, _) =
            capture_info(|| record_activation_verdict(dir, &ActivationVerdict::Permitted));
        assert!(!logged);
        let (logged, _) = capture_info(|| record_activation_verdict(dir, &relabelled));
        assert!(logged, "a blocker after a Permitted is a change");
    }

    /// The `/metrics` pair carries the recorded label and instance, and each of its `null` cases.
    ///
    /// # RED SIDE
    ///
    /// With `activation_blocker`'s latched-epoch early return removed, the last assertion fails:
    /// an activated volume keeps publishing the blocker recorded before activation.
    #[test]
    fn the_activation_blocker_gauge_carries_the_label_and_its_null_cases() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        assert_eq!(
            activation_blocker(dir),
            (None, None),
            "null before any verdict is evaluated"
        );

        record_activation_verdict(
            dir,
            &blocked(ActivationBlocker::RepairRotationIncomplete(
                "host-a:7".to_string(),
            )),
        );
        assert_eq!(
            activation_blocker(dir),
            (
                Some("repair_rotation_incomplete".to_string()),
                Some("host-a:7".to_string())
            )
        );

        record_activation_verdict(dir, &blocked(ActivationBlocker::NoReadyRecords));
        assert_eq!(
            activation_blocker(dir),
            (Some("no_ready_records".to_string()), None),
            "a volume-wide blocker names no instance"
        );

        record_activation_verdict(dir, &ActivationVerdict::Permitted);
        assert_eq!(activation_blocker(dir), (None, None), "null when permitted");

        record_activation_verdict(dir, &blocked(ActivationBlocker::NoReadyRecords));
        record_activation_verdict(dir, &blocked(ActivationBlocker::AlreadyActive));
        assert_eq!(
            activation_blocker(dir),
            (None, None),
            "null when already active"
        );

        // A blocker recorded, then the epoch latched by a peer's activation.
        record_activation_verdict(dir, &blocked(ActivationBlocker::NoReadyRecords));
        activate(dir, "peer:1").unwrap();
        assert_eq!(
            activation_blocker(dir),
            (None, None),
            "null once the epoch is 1, whatever was recorded before"
        );
    }

    /// Task 44, and the repo's config-compatibility rule for a new field: a record written by
    /// code that predates `evict_worklist_capable` parses, reads as not capable, and a newer
    /// record is still readable by the older struct shape (no `deny_unknown_fields`).
    #[test]
    fn a_ready_record_without_the_worklist_field_reads_as_not_capable() {
        let old = serde_json::json!({
            "schema": AUTHORITY_DOC_SCHEMA,
            "instance_id": "host-a:1",
            "binary_version": "2.9.13",
            "fold_capable": true,
            "rotation_complete": true,
            "fold_incomplete_total": 0,
            "repair_rotation_complete": true,
            "repair_stalest_age_secs": 5,
            "written_at": 1_790_000_000u64,
        })
        .to_string();
        let parsed: ReadyRecord = serde_json::from_str(&old).unwrap();
        assert!(
            !parsed.evict_worklist_capable,
            "an absent field must read as NOT capable; true would let an older proxy vote for a \
             work list it cannot execute"
        );
        assert!(
            parsed.fold_capable,
            "the rest of the record must be unaffected"
        );

        // Unknown fields from a later binary are tolerated.
        let newer = old.replace("\"schema\"", "\"a_future_field\":1,\"schema\"");
        assert!(serde_json::from_str::<ReadyRecord>(&newer).is_ok());

        // And the field round-trips when present.
        let mut record = parsed;
        record.evict_worklist_capable = true;
        let back: ReadyRecord =
            serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
        assert!(back.evict_worklist_capable);
    }

    /// Both publishers set the flag: a binary that contains the work list always runs it.
    #[test]
    fn both_publishers_advertise_worklist_capability() {
        let tmp = TempDir::new().unwrap();
        assert!(publish_startup_ready_record(tmp.path(), "host-s:1", 0).unwrap());
        let recs = read_ready_records(tmp.path()).unwrap();
        assert_eq!(recs.len(), 1);
        assert!(recs[0].evict_worklist_capable, "startup record");
        publish_ready_record(tmp.path(), "host-s:1", true, 0, true, Some(1)).unwrap();
        let recs = read_ready_records(tmp.path()).unwrap();
        assert_eq!(recs.len(), 1);
        assert!(recs[0].evict_worklist_capable, "periodic record");
    }
}
