//! The Write_Ledger: staging-tier residency discovery without a filesystem walk.
//!
//! Append-only files under `cache_dir/metadata/_write_ledger/`, one JSON object per line,
//! deliberately mirroring the journal layout under `metadata/_journals/` — per-instance
//! files so appends never contend across instances, merged by whoever reads.
//!
//! # The file layout is per `(shard, instance)`
//!
//! ```text
//! metadata/_write_ledger/{bucket}/{l1}/{instance}.ledger
//! ```
//!
//! A **directory** per shard, `{l1}` being the two hex digits
//! [`crate::staged_fold::shard_of`] derives, so a ledger file names exactly the shard whose
//! `.meta` files its entries describe. That confines a lost or unreadable ledger file to one
//! shard instead of to the whole volume, and it is what lets a later change read only the
//! shards a pass visits.
//!
//! **The directory form is deliberate and it is not the design's literal
//! `{bucket}-{l1}.{instance}.ledger`.** That filename cannot be parsed back:
//! `disk_cache::validate_bucket_segment` permits both `-` and `.` in a bucket name, and an
//! instance id is `{hostname}:{pid}` where the hostname is routinely an FQDN — so
//! `my.bucket-ab.host.example.com:1.ledger` has no unambiguous split from either end. The
//! directory form follows the shipped `_shard_state/checkpoints/{bucket}/{XX}.json`
//! precedent and leaves [`WriteLedger::instance_of`] as a plain `file_stem()`, which deletes
//! that parsing-bug class rather than managing it.
//!
//! A flat `metadata/_write_ledger/{instance}.ledger` is a **legacy** file written by a
//! binary that predates the split. [`WriteLedger::migrate_legacy_ledgers`] converts it, and
//! the staged fold retains rather than installing while any exists — see that function.
//!
//! # Why this exists
//!
//! Staging eviction used to find its candidates with `WalkDir` over the whole of
//! `metadata/`, reading and parsing **every** `.meta` to recover the write-cached
//! subset and discarding the rest. That is O(cache) work to select O(evicted) victims,
//! it ran on the request path of every refused PUT, and at ~1,800 cached objects it
//! measured 7-9 seconds per refusal independent of the object's own size.
//!
//! An append-only log is not a compromise here, it is the correct structure. **An
//! unread entry cannot change its own eviction rank**: its last-access time *is* its
//! write time, so LRU over never-read entries degenerates to FIFO. An index earns its
//! keep only when the policy reorders entries relative to arrival — true of the read
//! tier, false of this one. So insertion order is the candidate order, and that is
//! exactly what a log expresses.
//!
//! # The one structural difference from the journal: lifetime
//!
//! Journal entries die at the next consolidation. Ledger entries live until the entry
//! **graduates** (first read) or is **evicted**. That is what makes this a record of
//! staging *residency* rather than of pending *change*, and it has one important
//! consequence for how appends are implemented.
//!
//! `JournalManager::append_range_entry` appends by read-modify-write: it reads the whole
//! file, concatenates one line, writes a temp file, and renames. That is fine for a
//! journal, whose file is truncated every cycle and so stays short. It would be wrong
//! here: a ledger file is as long as the staged set, so a rewrite-per-append is O(staged)
//! work on every write-through PUT — hundreds of KB over NFS per upload on a cache
//! holding thousands of small staged objects.
//!
//! So this module uses a **true `O_APPEND` write** instead. That is safe here for a
//! reason that does not hold for most NFS append patterns: each instance writes only its
//! own file, so there is no cross-instance append contention to lose a write, and the
//! only other writer of a given file is compaction, which is excluded by the same
//! `fs4` exclusive lock. Within a process, one appender task per [`WriteLedger`] issues every
//! `Ledger_Seq` and writes each batch's lines one shard file at a time per file, so no two
//! appends interleave inside one `write` call (design § 16.6.14).
//!
//! # Hints, not authority
//!
//! Every entry is a **hint**. The `.meta` stays authoritative, and a candidate popped
//! from the ledger is verified against it before anything is deleted — see
//! [`StagedCandidateVerdict`]. That single decision covers every staleness case at once:
//! absent, graduated, superseded by a later write, and already evicted are all just
//! "skip".
//!
//! Two consequences worth stating explicitly:
//!
//! - **A lost append is not a correctness problem.** The entry becomes invisible to
//!   staging eviction until a Validation_Scan re-appends it, and it is never *served*
//!   wrongly, because the serve path does not consult the ledger at all.
//! - **The ledger is not a second authority for size.** `compressed_size` is carried for
//!   eviction-target arithmetic only. `SizeState` remains the accounting authority, and
//!   no read path may treat a ledger figure as a size of record (R2.5).
//!
//! Spec: write-cache-accounting-and-eviction. Requirements: 2.1, 2.2, 2.3, 2.5, 2.7

use crate::error::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::SystemTime;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Directory under `metadata/` holding the ledger files, `{bucket}/{l1}/{instance}.ledger`.
const LEDGER_DIR: &str = "_write_ledger";

/// Torn ledger lines moved to a `.ledger.torn` quarantine file by compaction, since process start.
///
/// **Non-zero is a finding, not a routine reading**, and it is the only signal that a staged range's
/// ledger entry was destroyed by a partial write. Before quarantine existed, compaction discarded
/// such lines and nothing counted them, so the loss was invisible in every gauge — loss mode L5. See
/// [`WriteLedger::quarantine_torn_lines`].
///
/// Published as `write_cache.ledger_torn_lines_quarantined_total`. **Per-instance**, like every other
/// counter on `/metrics`: a zero here means this instance has quarantined nothing, not that the fleet
/// has.
static TORN_LINES_QUARANTINED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `write_cache.ledger_torn_lines_quarantined_total`, per-instance since process start.
pub(crate) fn torn_lines_quarantined_total() -> u64 {
    TORN_LINES_QUARANTINED_TOTAL.load(AtomicOrdering::Relaxed)
}

/// How many entries the two append stages carried per batch, and how often the pending stage's
/// write failed. **Per-instance**, since process start.
///
/// # What the ratio says
///
/// `append_batched_entries_total / append_batches_total` is the mean batch size. One hold of a
/// shard ledger file's lock carries a whole batch's lines for that shard, so the ratio is what
/// separates a commit rate set by batching from one set by the per-hold latency: at 1 the stage
/// is draining one entry per hold and the rate is the one-entry-per-hold rate, and above 1 each
/// hold is amortised over that many entries. It is 1 whenever the queue is idle, because a batch
/// is whatever arrived while the previous one was being written, so a low ratio on a lightly
/// loaded instance says nothing is wrong.
///
/// `pending_batch_failures_total` is the counter to alert on. Each failure is a lock timeout, a
/// write error or a sync error that lost up to one batch of `Pending_Credit` records whose ledger
/// lines are durable, so the staged figure under-states those bytes until the next staged-fold
/// pass visits the shards the stage marked dirty. Non-zero is a finding; it is also the only
/// numeric signal a failed pending batch produces.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriteLedgerMetrics {
    /// Batches the append stage drained. The denominator of the mean batch size.
    pub append_batches_total: u64,
    /// Entries in those batches. The numerator of the mean batch size, and the count of
    /// submissions the stage has taken off its queue.
    pub append_batched_entries_total: u64,
    /// Batches the pending-credit stage drained, each one lock hold on
    /// `{instance}.pending.lock`.
    pub pending_batches_total: u64,
    /// Those pending batches whose write failed. **Non-zero is a finding, not a routine
    /// reading.**
    pub pending_batch_failures_total: u64,
}

/// How long the shutdown drain waits for each stage, in total.
///
/// Well inside the 15 s `shutdown.rs` allows the consolidator's whole shutdown, because the
/// final consolidation cycle runs after the drain and needs the rest of it.
pub(crate) const STAGE_DRAIN_BOUND: std::time::Duration = std::time::Duration::from_secs(3);

/// How often the drain re-reads the stages' depth.
///
/// A poll rather than a notification: the drain runs once per process, at shutdown, and a
/// condition variable here would have to be correct against a decrement that lands between the
/// check and the wait. The cost is bounded by [`STAGE_DRAIN_BOUND`].
const STAGE_DRAIN_POLL: std::time::Duration = std::time::Duration::from_millis(2);

/// One `WriteLedger`'s stage state: what each stage still owes an answer for, and what it has
/// batched.
///
/// Per ledger rather than per process, which in production is the same thing — one `WriteLedger`
/// is constructed per proxy — and which a test can assert an exact delta on. A process-global
/// counter cannot be asserted exactly by a test that shares the process with other tests that
/// also append.
///
/// The two depths are kept apart rather than summed because the stages must be drained in order:
/// the append stage is the pending stage's only producer, so a pending depth of zero means
/// nothing while the append stage still holds an entry it is about to hand over.
#[derive(Debug, Default)]
struct StageCounters {
    /// Enqueued on the append stage and neither answered nor handed over.
    ledger: AtomicU64,
    /// Handed to the pending stage and not yet answered.
    pending: AtomicU64,
    /// Batches the append stage has drained from its queue.
    append_batches: AtomicU64,
    /// Entries in those batches.
    append_batched_entries: AtomicU64,
    /// Batches the pending stage has drained.
    pending_batches: AtomicU64,
    /// Those pending batches whose write failed.
    pending_batch_failures: AtomicU64,
}

/// What a shutdown drain left unanswered.
///
/// Both zero is the outcome the drain exists for: every submitted entry received its real
/// outcome, so no caller saw a closed channel and declined a credit whose ledger line is
/// durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageDrainOutcome {
    /// Submissions the append stage had not answered or handed over at the bound.
    pub ledger_unanswered: u64,
    /// Credits the pending stage had not answered at the bound.
    pub pending_unanswered: u64,
}

impl StageDrainOutcome {
    /// Whether both stages answered everything they had accepted.
    pub fn is_drained(&self) -> bool {
        self.ledger_unanswered == 0 && self.pending_unanswered == 0
    }
}

/// Extension for a ledger file, matched when merging across instances.
const LEDGER_EXT: &str = "ledger";

/// How old a whole-volume entry-count observation may be before [`WriteLedger::count_entries`]
/// re-takes it rather than reporting it.
///
/// # Why the gauge is an observation of bounded age rather than a scrape-time count
///
/// `count_entries` used to traverse the whole volume on **every** `/metrics` scrape, every
/// dashboard refresh and every OTLP export: `1 + B + (B × 256)` directory listings for `B`
/// buckets, then a whole-file read and line-parse of up to `B × 256 × instances` small files on
/// shared storage. Nothing memoised it, so the cost scaled with **scrape frequency** — which an
/// operator cannot reduce without giving up their monitoring, and which no other field on
/// `/metrics` charges them for.
///
/// That is an availability defect and not an optimisation, because the `/metrics` body is
/// all-or-nothing: a collection slow enough to exceed a scraper's timeout costs that scraper
/// **every** series, not this one field. OTLP and the HTTP endpoint then degrade asymmetrically —
/// OTLP exports from inside the same collection so it merely arrives late, while the HTTP body is
/// lost outright — so an operator comparing the two concludes the scrape endpoint is broken.
///
/// # Why 300 seconds specifically, and why that is not a new contract
///
/// It is the cadence this gauge is **already documented to be read against**.
/// `docs/METRICS_REFERENCE.md` tells operators that a *persistent* gap between `staged_entries`
/// and `ledger_entries` is the signal, that a transient gap is normal, and that compaction runs
/// on a 5-minute interval. So the useful resolution of this figure was already one compaction
/// interval; bounding the observation to the same interval takes nothing away.
///
/// It is also `LEDGER_COMPACTION_INTERVAL_SECS`'s value, which is deliberate:
/// [`WriteLedger::retire_identities`] **donates** the figure it already computes (whole-volume,
/// under each file's lock, off the request path), so on a fleet where compaction is running the
/// scrape path re-takes nothing at all and the traversal disappears rather than merely becoming
/// less frequent. The scrape-path re-take is the fallback for the cases compaction does not
/// cover: a freshly started process, and a drained ledger, for which
/// `maybe_compact_write_ledger` returns before calling `retire_identities`.
///
/// **The age is published**, as `write_cache.ledger_entries_age_secs`, so the staleness is
/// visible rather than inferred — an operator reading the same value from two scrapes 15 seconds
/// apart can tell "stable" from "the same observation twice".
const LEDGER_COUNT_MAX_AGE_SECS: u64 = 300;

/// A whole-volume observation of the ledger's entry count, and when it was taken.
///
/// Only a **successful** whole-volume count is ever stored. An observation that could not see
/// the whole volume is not recorded at all, rather than being recorded as a failure, which is
/// what keeps the UNREADABLE sentinel un-sticky: a scrape taken while the volume is unreadable
/// re-attempts the traversal and publishes `None` from that attempt, and recovery is visible on
/// the next scrape instead of being latched for an interval.
#[derive(Debug, Clone, Copy)]
struct LedgerCountObservation {
    count: u64,
    observed_at: std::time::Instant,
}

/// One staged range, recorded when its write-cache credit is applied.
///
/// The identity is `(cache_key, range_start, range_end, timestamp, instance_id)`,
/// mirroring the 5-tuple `JournalConsolidator::cleanup_consolidated_entries` uses to
/// retire journal entries. `timestamp` and `instance_id` are part of it because a key
/// can legitimately appear more than once — a re-PUT of the same extent produces a
/// second entry, and the older one is then superseded rather than duplicated.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteLedgerEntry {
    /// When the staged write was credited. This is the eviction sort key, and it is
    /// stamped **after** the `.meta` write, which is what makes the supersede check in
    /// [`is_superseded`] a strict `>` comparison rather than `>=`.
    #[serde(with = "ledger_time")]
    pub timestamp: SystemTime,

    /// `{bucket}/{object_key}`.
    pub cache_key: String,

    /// Range start offset, part of the entry identity.
    pub range_start: u64,

    /// Range end offset (inclusive), part of the entry identity.
    pub range_end: u64,

    /// Compressed bytes this range occupies on disk.
    ///
    /// **For eviction-target arithmetic only** (R2.5). It tells the evictor roughly how
    /// much freeing this candidate would reclaim so it can stop once it has freed
    /// enough; the running total is then reconciled against what eviction actually
    /// deleted. Never report this as a cache size, and never fold it into Size_State.
    pub compressed_size: u64,

    /// Which instance credited the write. Part of the entry identity; also what lets
    /// compaction attribute an entry to a file.
    pub instance_id: String,

    /// `Ledger_Seq`: this entry's position in its own instance's file.
    ///
    /// **Assigned by [`WriteLedger::append`], never by the caller, and never derived from a
    /// clock.** A caller-supplied value is overwritten. See [`WriteLedger::next_seq`] for
    /// how it is generated and what makes it monotone.
    ///
    /// # Why it is not the timestamp
    ///
    /// It was, for one iteration of this module, and that was a defect rather than a
    /// simplification. `std::time::SystemTime` is documented as non-monotonic and can step
    /// backwards (NTP correction, a VM clock resync, an operator setting the date). A
    /// backward step makes a *newly appended* credit's coordinate land at or below a cut
    /// that was established before the credit's `.meta` existed — so the reader filters the
    /// pending record out as already-folded while the fold has not seen the `.meta` either.
    /// Those bytes are then in **neither** term, which is the one outcome § 16.6.1 rules
    /// out by name. The `unwrap_or(0)` fallback that a pre-epoch reading took was the same
    /// class and worse: seq 0 is at or below every cut, so the bytes were invisible by
    /// construction rather than transiently.
    ///
    /// # `None` means "written by a binary that had no seq"
    ///
    /// `serde(default)`, so an entry appended by a pre-upgrade binary deserialises as
    /// `None` — design § 16.6.6's mixed-version phase. A `None` entry contributes **no**
    /// observation to the cut (see [`LedgerReadOutcome::max_seq_per_instance`]), because an
    /// unknown position must not be allowed to move a high-water mark in either direction.
    /// Its bytes are still counted: the fold reads its `.meta` like any other candidate.
    ///
    /// Spec: cache-eviction-at-scale §§ 16.6.1, 16.6.6.
    #[serde(default)]
    pub seq: Option<u64>,
}

/// What the ledger files named for one instance say about when it last appended.
///
/// Produced by [`WriteLedger::latest_append_secs_in`] for § 16.6.6 B7.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendEvidence {
    /// No entry under this id: no file, or every file is header-only.
    NoEntries,
    /// The latest entry `timestamp` found, unix seconds.
    LatestAt(u64),
    /// A file named for this id could not be read, or holds a line that is neither an entry nor
    /// a header. Its latest append is unknown.
    Indeterminate,
}

/// When the ledger files named for one instance were last modified, on the file server's clock.
///
/// Produced by [`WriteLedger::latest_modified_in`] for § 16.6.6 B8 (the one-day quiet rule) and B9
/// (the operator retirement marker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModifiedEvidence {
    /// No file in the listing is named for this id.
    NoFiles,
    /// The newest modification time over every file named for this id.
    LatestAt(SystemTime),
    /// A file named for this id could not be `stat`ed, so its newest modification is unknown.
    Indeterminate,
}

impl ModifiedEvidence {
    /// Two files' evidence for one id: the later time, and unknown wins over any figure.
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Indeterminate, _) | (_, Self::Indeterminate) => Self::Indeterminate,
            (Self::LatestAt(a), Self::LatestAt(b)) => Self::LatestAt(a.max(b)),
            (Self::LatestAt(a), Self::NoFiles) | (Self::NoFiles, Self::LatestAt(a)) => {
                Self::LatestAt(a)
            }
            (Self::NoFiles, Self::NoFiles) => Self::NoFiles,
        }
    }
}

impl AppendEvidence {
    /// Two files' evidence for one id: the later append, and unknown wins over any figure.
    fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Indeterminate, _) | (_, Self::Indeterminate) => Self::Indeterminate,
            (Self::LatestAt(a), Self::LatestAt(b)) => Self::LatestAt(a.max(b)),
            (Self::LatestAt(a), Self::NoEntries) | (Self::NoEntries, Self::LatestAt(a)) => {
                Self::LatestAt(a)
            }
            (Self::NoEntries, Self::NoEntries) => Self::NoEntries,
        }
    }
}

impl WriteLedgerEntry {
    /// The entry's identity tuple, for set membership during compaction.
    pub fn identity(&self) -> (String, u64, u64, SystemTime, String) {
        (
            self.cache_key.clone(),
            self.range_start,
            self.range_end,
            self.timestamp,
            self.instance_id.clone(),
        )
    }
}

/// An entry's `Ledger_Seq`, or `None` if it was written by a binary that had no seq field.
///
/// A plain field read. It is a named function rather than a field access because two call
/// sites depend on it being the *same* coordinate — `LedgerReadOutcome::observe_seq`, which
/// derives a fold cut, and the `Pending_Credit` stamped by the credit site — and because
/// the previous version of this function computed the coordinate from the entry's
/// timestamp. Keeping the name makes that substitution impossible to reintroduce quietly:
/// there is one place to look.
///
/// **Not the timestamp.** See [`WriteLedgerEntry::seq`] for what a clock-derived coordinate
/// cost. `timestamp` remains on the entry and is still load-bearing for two things that are
/// not the ordering coordinate: the oldest-first *eviction candidate order*
/// ([`WriteLedger::oldest_first`], R2.3) and the supersede comparison against a `.meta`'s
/// `created_at` ([`is_superseded`]). Both compare a ledger entry against wall-clock facts
/// recorded elsewhere, which is what a wall clock is for. Neither decides which side of a
/// cut a credit falls on.
///
/// Spec: cache-eviction-at-scale § 16.6.1.
pub(crate) fn ledger_seq(entry: &WriteLedgerEntry) -> Option<u64> {
    entry.seq
}

/// The error [`WriteLedger::append`] returns for a cache key no shard can be derived from.
///
/// # Why this is now an error rather than a silently-filed entry
///
/// Under the flat layout an unshardable key still had a file to append to, and the fold then
/// discarded it as `unshardable` — so **it was already counted by nothing**, silently. Under
/// the per-shard layout there is no file to append to at all, so the append has to say
/// something. Refusing names the condition at the site that can see it.
///
/// A named constructor rather than an inline `format!` so the message has one definition and
/// is findable.
///
/// The caller, `JournalConsolidator::record_staged_range`, treats it like every other append
/// failure: it logs at WARN and returns `false`, and its callers then **decline the
/// write-cache credit** while the range stays cached and counted in `total_size`. That
/// matches the staged fold, which discards an unshardable key and so counts it nowhere
/// either. Declining the whole caching decision was not chosen because the `.bin` and `.meta`
/// are already published when the append runs.
pub(crate) fn unshardable_key_error(cache_key: &str) -> ProxyError {
    ProxyError::CacheError(format!(
        "Write-ledger append refused: cache key {:?} has no parseable shard, so there is no \
         shard ledger file to append to. The range is left unindexed and its write-cache \
         credit is declined.",
        cache_key
    ))
}

/// Red-side lever: the appender ignores a shared identity, which is the tree
/// before this unit: two processes on one `{host}:{pid}` both issue out of one sequence.
#[cfg(test)]
const LEDGER_IGNORES_A_SHARED_IDENTITY: bool = false;

/// One WARN per process for the shared-identity append refusal: the condition does not clear
/// until the deployment is fixed, and the appender meets it on every batch.
static IDENTITY_SHARED_REFUSAL_LOGGED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// A ledger file's header line: `{"schema":3,"max_seq_ever":N}`.
///
/// Schema 3 is the per-`(shard, instance)` layout; schema 2 was the flat per-instance file.
/// The shape is unchanged, and `N` is now that **shard file's** own high-water mark rather
/// than the instance's. A file's mark is still `max(header, highest entry seq)` — see
/// [`ParsedLedgerFile::max_seq_ever`] — so the split changes which files a mark is spread
/// across and nothing about how one is derived.
///
/// # What it is for
///
/// Without it a cut is only reconstructible from a checkpoint, because compaction can
/// retire an instance's highest-seq entry and a complete read of the remaining file then
/// observes a *lower* maximum. The header carries the pre-compaction high-water mark
/// forward, so `max_seq_ever` never decreases and a cut can be rebuilt from the ledger file
/// alone.
///
/// # It is written by compaction and by file creation, not by every append
///
/// Updating it per append would make an append a read-modify-write of a file as long as the
/// staged set, which is precisely the O(staged)-per-PUT shape this module exists to avoid.
/// So the header is written when the file is created (seeding the sequence) and rewritten by
/// [`WriteLedger::retire_identities`], and it is legitimately *stale* in between.
///
/// # When the header and the entries disagree
///
/// **Take the maximum of the two. Never the header alone, never the entries alone.**
/// Disagreement is the normal state and is expected in both directions:
///
/// - entries higher than the header — appends since the last compaction, which is every
///   file in steady state. The entries are authority for what has been written.
/// - header higher than every entry — compaction retired the top of the file. The header is
///   authority for what was once written.
///
/// Trusting the header alone would place a fresh append at or below the cut (a lost credit,
/// the forbidden direction); trusting the entries alone would let compaction lower a cut and
/// resurrect an already-folded credit into the pending term (a double count). The max is the
/// only reading that is safe against both, and it is what
/// [`ParsedLedgerFile::max_seq_ever`] computes. A header that cannot be parsed at all is treated as absent, so the entries'
/// maximum stands — which cannot lose a credit, only over-count one.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct LedgerHeader {
    schema: u32,
    max_seq_ever: u64,
}

/// Schema version of a ledger file carrying a header and per-entry seqs.
///
/// 2 was the flat `{instance}.ledger`; 3 is `{bucket}/{l1}/{instance}.ledger`. Written, and
/// deliberately still **not validated on read** — [`WriteLedger::parse_content`] accepts any
/// `schema` value as a header — because the layout change is expressed by the file's *path*
/// and a pre-upgrade binary looks only at the flat path, where it finds nothing.
const LEDGER_SCHEMA: u32 = 3;

/// Extension of the per-instance sequence mark, `{instance}.ledger.mark`.
///
/// **Not `.ledger`-suffixed as its final component**, so `Path::extension` returns `mark` and
/// the `extension() == "ledger"` filter in [`WriteLedger::shard_ledger_files`] and
/// [`WriteLedger::legacy_ledger_files`] excludes it by construction, exactly as it does the
/// `.torn` quarantine.
const SEQ_MARK_SUFFIX: &str = ".ledger.mark";

/// How far ahead of the issued sequence the persistent mark is reserved.
///
/// The mark is written **write-ahead**: before a seq at or above the recorded mark is issued,
/// the mark is raised to `seq + SEQ_MARK_STRIDE - 1`. So the file is rewritten once per
/// `SEQ_MARK_STRIDE` appends rather than once per append, and it is an upper bound on every
/// seq this instance has ever issued even if the process dies without warning.
///
/// The cost of the reservation is a **gap** in the sequence when a process reusing an instance
/// identity re-seeds: it starts above the reserved block rather than above the last issued
/// number. A gap is harmless to a cut — the cut is a threshold, not a count — and the only
/// thing it costs is that "how many credits has this instance made" stops being readable
/// straight off the sequence for a reused identity. Losing a credit is not on the menu, which
/// is the trade being made.
const SEQ_MARK_STRIDE: u64 = 1024;

/// How many submissions [`WriteLedger::append`]'s queue holds before a caller's `send` waits.
///
/// A full queue makes the caller wait, which is the back-pressure the per-append mutex already
/// applied to the commit tail, now bounded in memory. It never sheds: a shed submission would be
/// a declined credit, an under-count only the repair rotation restores, and the queue exists to
/// remove exactly that. 4096 entries is over a minute of the highest staged-commit rate measured
/// on shared storage, and a few MB of queued entries at most.
const LEDGER_QUEUE_CAPACITY: usize = 4096;

/// The most submissions one appender batch takes off the queue.
///
/// No timer and no minimum: a batch is whatever arrived while the previous one was being
/// written, so it is 1 when idle (no added latency) and grows with the backlog. 64 is far below
/// [`SEQ_MARK_STRIDE`], so one batch raises the write-ahead mark at most once, and it bounds what
/// one torn group write can lose per shard file.
const LEDGER_BATCH_MAX: usize = 64;

/// The most shard groups of one batch written at once.
///
/// Each group is a separate file with its own `fs4` lock and its own sync, so groups are
/// independent and writing them concurrently is where the throughput comes from: with keys
/// spread over 256 shards, a batch of 16 touches about 15.5 files, and writing those one after
/// another would cost 15.5 holds. 16 bounds the blocking threads and in-flight NFS writes one
/// batch occupies, and covers one full round of a batch of up to 16.
const LEDGER_GROUP_CONCURRENCY: usize = 16;

const _: () = assert!(LEDGER_BATCH_MAX as u64 <= SEQ_MARK_STRIDE);

/// The most paired credits the pending stage writes in one `write_all`.
///
/// There is one pending file per instance, so coalescing is the whole gain there: a batch shares
/// one hold of `{instance}.pending.lock` and one `sync_data`, however many records it carries.
/// Far above [`LEDGER_BATCH_MAX`] on purpose — the stage drains every hand-over that arrived
/// while the previous pending write was in flight, so its batch is a multiple of the ledger
/// stage's — and it bounds what one failed pending write can lose (design § 16.6.14).
const PENDING_BATCH_MAX: usize = 1024;

/// Hand-overs the pending stage's queue holds, under [`LEDGER_QUEUE_CAPACITY`]'s rule: a full
/// queue makes the ledger stage wait, and nothing is shed. A shed hand-over would be a pending
/// record lost with no shard marked dirty, which is the one under-count this stage exists to
/// bound.
const PENDING_QUEUE_CAPACITY: usize = LEDGER_QUEUE_CAPACITY;

/// How old an entry must be before [`assess_staged_candidate`] retires it, whatever the verdict.
///
/// `CompleteMultipartUpload` appends its entries before it renames the `.meta` into place,
/// so for the length of one rename a live object's entries are judged against whatever
/// `.meta` is on disk: none, or on an overwrite whose invalidation failed, the previous
/// object's. An assessment landing in that window must not retire them. The window is one
/// rename on shared storage, normally milliseconds; 60 s covers a stalled NFS rename and
/// fleet clock skew (the entry is stamped by the appending instance, the cutoff by the
/// assessing one) with a wide margin. The cost of the grace is that a terminal entry
/// (absent, graduated, superseded) survives one more compaction or eviction pass, skipped by
/// its verdict each time.
pub(crate) const ABSENT_RETIREMENT_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

/// Whether a ledger entry stamped `entry_timestamp` describes a copy that a later write has
/// since replaced.
///
/// A free function rather than a method so [`assess_staged_candidate`] — which holds a
/// [`StagedCandidate`] rather than a single entry — evaluates the *same* predicate its unit
/// test pins. An inline copy at the call site would let the two drift, leaving the test
/// asserting something production no longer does.
///
/// The ordering this relies on is established by the credit sites: the range's `created_at`
/// is stamped when its `RangeSpec` is built, and the ledger entry is appended after that. (The
/// single-part site also writes the `.meta` before the append; the multipart completion appends
/// before renaming its `.meta` into place, but its ranges were built earlier still.) So for the
/// *current* entry the range's
/// `created_at` precedes (or at coarse clock resolution equals) the entry's timestamp, while
/// a re-PUT writes a strictly later `.meta` and therefore a strictly later `created_at`.
///
/// Strict `>` so that equal timestamps — a clock too coarse to separate the `.meta` write
/// from the append — read as "not superseded" and the entry is kept. That is the safe
/// direction: keeping a live entry costs one verification next pass, whereas dropping it
/// would make a genuinely staged object invisible to eviction until the next
/// Validation_Scan. Same asymmetry as the `Remove`-arm timestamp guard in
/// `JournalConsolidator::apply_journal_entries`.
pub(crate) fn is_superseded(
    entry_timestamp: SystemTime,
    newest_range_created_at: SystemTime,
) -> bool {
    newest_range_created_at > entry_timestamp
}

/// Nanoseconds-since-epoch encoding for `SystemTime`.
///
/// **Deliberately NOT the journal's `systemtime_serde`, which stores whole seconds.**
/// Two things here need sub-second resolution and the journal does not:
///
/// - `timestamp` is part of [`WriteLedgerEntry::identity`], and compaction matches
///   identities held in memory against identities read back from the file. A
///   second-granularity encoding makes those two forms of the same entry unequal, so
///   compaction silently retains nothing. The journal escapes this only because both
///   sides of its comparison have already been through a file.
/// - the supersede check compares a `.meta` range's `created_at` against this timestamp,
///   and a re-PUT completing inside the same second would otherwise be indistinguishable
///   from the original write.
///
/// `u64` nanoseconds runs out in 2554, which is not a constraint worth engineering
/// around.
mod ledger_time {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = time
            .duration_since(UNIX_EPOCH)
            .map_err(serde::ser::Error::custom)?;
        (duration.as_nanos() as u64).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let nanos = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + std::time::Duration::from_nanos(nanos))
    }
}

/// What verifying a ledger candidate against its authoritative `.meta` concluded.
///
/// Everything except [`Self::Evictable`] is a skip. Collapsing all four staleness cases
/// into one verification step is the decision that makes the ledger safe to treat as a
/// hint, so this enum exists mostly to make the *reason* observable — a ledger that is
/// almost all skips is a compaction problem, and that is only visible if the reasons are
/// counted separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedCandidateVerdict {
    /// Still staged, still the current copy: evict it.
    Evictable,
    /// No `.meta` on disk. Already evicted, or never landed.
    MetadataAbsent,
    /// `.meta` exists but is no longer flagged staged — it graduated on a first read.
    Graduated,
    /// A later write replaced the copy this entry describes.
    Superseded,
    /// The `.meta` could not be read or parsed. Skipped rather than acted on: deleting
    /// an object whose metadata we failed to read would be acting on no information.
    Unreadable,
}

impl StagedCandidateVerdict {
    /// Short stable label, for logs and skip counters.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Evictable => "evictable",
            Self::MetadataAbsent => "metadata_absent",
            Self::Graduated => "graduated",
            Self::Superseded => "superseded",
            Self::Unreadable => "unreadable",
        }
    }
}

/// Outcome of a compaction pass, for logging and the `ledger_entries` gauge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerCompactionStats {
    /// Entries read across every instance file before compaction.
    pub entries_before: u64,
    /// Entries retained.
    pub entries_after: u64,
    /// Ledger files rewritten.
    pub files_rewritten: u64,
    /// Ledger files removed because nothing was retained.
    pub files_removed: u64,
    /// Ledger files read under their lock and then left exactly as they were, because a rewrite
    /// would have reproduced the same content.
    ///
    /// Published so the saving is visible rather than inferred. Before this existed every file
    /// whose kept set was non-empty was rewritten whether or not it held a retired identity, which
    /// a fleet measurement put at **134 locked rewrites to drop 2 entries** — a cost tracking the
    /// file population rather than the work to be done. `files_rewritten` alone cannot show that,
    /// because a falling rewrite count reads the same whether the skipped files needed nothing or
    /// were wrongly skipped; the pair, against `entries_dropped`, can.
    pub files_unchanged: u64,
}

impl LedgerCompactionStats {
    /// Entries dropped by this pass.
    pub(crate) fn entries_dropped(&self) -> u64 {
        self.entries_before.saturating_sub(self.entries_after)
    }
}

/// A ledger read that can say whether it was complete.
///
/// # Why a bare `Vec` was not enough
///
/// The hint reader returns `Vec<WriteLedgerEntry>` and folds an unopenable file, a torn
/// trailing line and a syntactically malformed line all into "fewer entries". So an
/// unreadable ledger and a genuinely empty one are the same value, and every failure
/// pushes a caller toward the healthy-looking conclusion: eviction reads emptiness as "a
/// fresh upgrade, nothing staged yet", and the `ledger_entries` gauge reads it as
/// "drained". A caller that intends to treat the ledger as an *authority* — to install a
/// figure derived from it, or to advance a cut — must be able to refuse instead, and that
/// requires the completeness of the read to survive the return.
///
/// Every field is the count of something the read had to discard or could not see, so
/// [`Self::is_complete`] is the single predicate an installer checks.
///
/// Spec: cache-eviction-at-scale § 16.6.3.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LedgerReadOutcome {
    /// Entries read, merged across instances and sorted oldest-first.
    pub entries: Vec<WriteLedgerEntry>,

    /// Ledger files the directory listing named.
    pub files_expected: usize,

    /// Files opened and parsed without an I/O error.
    pub files_read: usize,

    /// Files that could not be opened or read. A file that vanished between the listing
    /// and the lock counts here too: for an authority read the conservative reading of
    /// "a file I was told about and did not see" is incomplete, not benign.
    pub files_unreadable: usize,

    /// Lines discarded by the parser. Counts a torn trailing line, a syntactically
    /// malformed line, and a blank line — a blank line in an append-only JSONL file is
    /// itself the signature of a torn write, and it used to be discarded without even a
    /// log.
    pub malformed_lines: usize,

    /// The cap was hit, so this view is a prefix of the ledger rather than the whole of
    /// it. Truncation used to be silent.
    pub truncated: bool,

    /// Highest `Ledger_Seq` observed per instance, keyed by `instance_id`.
    ///
    /// Per instance because the files are per instance: one instance's sequence says nothing
    /// about another's, and a single global scalar cut would filter a peer's fresh credit
    /// against this instance's counter.
    ///
    /// Observed as `max(header.max_seq_ever, highest entry seq)` over each **file**, not
    /// per entry — see [`ParsedLedgerFile::max_seq_ever`] and [`LedgerHeader`]. Entries with
    /// no `seq` (written by a pre-upgrade binary) contribute nothing: an unknown position
    /// must not move a high-water mark in either direction.
    ///
    /// An instance whose file has neither a header nor any seq-bearing entry is **absent
    /// from this map** rather than present with 0, which is what lets a caller tell "this
    /// instance has no known position" from "this instance is at position 0".
    ///
    /// Observed over everything read, **before** any cap truncation, so a capped read still
    /// reports where each instance's file actually ended.
    pub max_seq_per_instance: BTreeMap<String, u64>,

    /// Legacy flat `{instance}.ledger` files still present, i.e. participants that have not
    /// been converted to the per-shard layout.
    ///
    /// **Not part of [`Self::is_complete`]**, deliberately. Every other field here is
    /// "something this read could not see"; this one is "a participant whose entries name no
    /// shard". Both make an installed figure short, and they have different remedies — one is
    /// retry, the other is finish the rolling deploy — so the fold refuses on it under its own
    /// name rather than being folded into a completeness verdict.
    pub legacy_files_present: usize,
}

impl LedgerReadOutcome {
    /// Whether this read saw everything it was **asked to cover**: every listed file opened,
    /// no line discarded, and no truncation.
    ///
    /// This is the completeness half of § 16.6.3's Complete_Install gate. The other
    /// halves (pending files, the in-progress-MPU traversal, and per-candidate verify
    /// verdicts) are not the ledger's to report.
    ///
    /// # Scope: "the whole ledger" is no longer the only thing this can mean
    ///
    /// There are two authority readers, and this predicate is relative to the one that produced
    /// the value rather than absolute. [`WriteLedger::read_for_authority`] is asked to cover the
    /// whole volume, so a complete outcome from it means what it always did.
    /// [`WriteLedger::read_shard_for_authority`] is asked to cover one shard, so a complete
    /// outcome from it means that shard's ledger was wholly readable and says nothing about any
    /// other shard — which is exactly the precondition for installing that shard's figure.
    ///
    /// **No second predicate was added, deliberately.** The fields are all counts scoped to the
    /// read's own file set (`files_expected` is the caller's listing length), so one predicate
    /// over them is already scope-correct; a `is_complete_for_volume()` beside it would be true
    /// or false depending on data this value does not carry. Every pre-existing caller reads a
    /// value produced by `read_for_authority` and therefore means precisely what it meant.
    pub fn is_complete(&self) -> bool {
        self.files_unreadable == 0
            && self.malformed_lines == 0
            && !self.truncated
            && self.files_read == self.files_expected
    }
}

/// One ledger file, parsed: its entries, its discarded-line count, and its header's
/// `max_seq_ever` if it had a readable header.
#[derive(Debug, Default)]
struct ParsedLedgerFile {
    entries: Vec<WriteLedgerEntry>,
    malformed: usize,
    /// The malformed lines themselves, verbatim, in file order.
    ///
    /// # Why the text and not just the count — loss mode L5
    ///
    /// A torn line is an incompleteness the reader **reports**: `malformed` feeds
    /// [`LedgerReadOutcome::is_complete`], so the fold retains rather than installing a figure it
    /// knows is short. Compaction then rebuilt the file from the successfully parsed entries only,
    /// which erased the report — the next read saw a clean file, `is_complete` returned true, and
    /// the fold installed a figure permanently missing those bytes. A reported incompleteness became
    /// **silent permanent loss**, and in the forbidden direction: a lower `F` makes the staging
    /// eviction trigger fire late.
    ///
    /// A count cannot fix that, because a rewrite has to be able to put the lines back. So the text
    /// is carried, and compaction quarantines it — see `retire_identities`.
    ///
    /// Blank lines are counted in `malformed` but deliberately **not** collected here: a blank line
    /// is what a torn write looks like once its partial bytes are gone, so there is nothing in it to
    /// preserve, and quarantining whitespace would produce a file that alarms without evidence.
    malformed_lines_text: Vec<String>,
    /// `None` means the file carried no readable header — a legacy schema-1 file, or one
    /// whose header line was itself torn. Treated as absent, so the entries' own maximum
    /// stands; see [`LedgerHeader`] for why that direction is the safe one.
    header_max_seq_ever: Option<u64>,
}

impl ParsedLedgerFile {
    /// The file's high-water mark: `max(header, highest entry seq)`.
    ///
    /// `None` only when the file has neither a header nor a single seq-bearing entry, i.e.
    /// a file written entirely by a pre-upgrade binary. That is distinct from `Some(0)`,
    /// which would claim a known position of zero.
    ///
    /// Entries with no seq are skipped rather than read as 0: an unknown position must not
    /// participate in a maximum.
    fn max_seq_ever(&self) -> Option<u64> {
        let from_entries = self.entries.iter().filter_map(ledger_seq).max();
        match (self.header_max_seq_ever, from_entries) {
            (Some(h), Some(e)) => Some(h.max(e)),
            (Some(h), None) => Some(h),
            (None, Some(e)) => Some(e),
            (None, None) => None,
        }
    }
}

/// What one file's locked read-filter-rewrite concluded, returned out of the blocking
/// closure so the stats are computed from what was read **under the lock** rather than
/// from a pre-lock snapshot.
#[derive(Debug, Default)]
struct LedgerFileRewrite {
    entries_before: u64,
    entries_after: u64,
    removed: bool,
    rewritten: bool,
    /// Read under the lock and deliberately left alone, distinct from the `default()` this returns
    /// for a file that had vanished. Both leave `removed` and `rewritten` false, and conflating
    /// them would count a file that disappeared as one this pass chose to preserve.
    unchanged: bool,
    malformed_lines: usize,
}

/// Per-instance append-only record of staged (write-cached, un-graduated) ranges.
pub struct WriteLedger {
    cache_dir: PathBuf,
    instance_id: String,
    /// The appender's state: the sequence, its guards and the sync seam. Shared with the
    /// appender task, which outlives any one `append` call.
    core: std::sync::Arc<AppenderCore>,

    /// The appender's queue, or `None` until the first [`Self::append`] spawns the task.
    ///
    /// A plain `std` mutex, held only to clone or install the sender, never across an await.
    submit: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<LedgerSubmission>>>,

    /// The append stage task, for [`Self::abort_stages`]. Tests only.
    #[cfg(test)]
    appender_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The most recent successful whole-volume entry count, or `None` if none has been taken.
    ///
    /// A `Mutex` rather than a pair of atomics because it is also the **single-flight** gate:
    /// [`Self::count_entries`] holds it across the traversal, so N concurrent scrapes produce one
    /// traversal and the losers read the winner's result instead of stampeding the volume. That
    /// matters more than it looks — the HTTP endpoint, the dashboard and the OTLP exporter all
    /// call the same collection, so concurrent collections are the normal case rather than a
    /// pathology.
    ///
    /// See [`LEDGER_COUNT_MAX_AGE_SECS`] for why this exists and why a running count cannot
    /// replace it.
    entry_count: Mutex<Option<LedgerCountObservation>>,
}

/// One [`WriteLedger::append`] call waiting in the appender's queue.
struct LedgerSubmission {
    entry: WriteLedgerEntry,
    /// The entry's shard file, resolved before it was enqueued, so an unshardable key never
    /// reaches the queue.
    path: PathBuf,
    /// Whether this entry's credit has a `Pending_Credit` half, and so whether the pending
    /// stage rather than the ledger stage owns its reply.
    pair_pending: bool,
    reply: tokio::sync::oneshot::Sender<Result<u64>>,
    /// Set when this entry's reply is handed to the pending-credit stage, which happens only
    /// after its own group's write and sync returned `Ok`. Shared with the waiting
    /// [`WriteLedger::submit`], which reads it to attribute a dropped reply — see
    /// [`WriteLedger::stage_owning_reply`].
    handed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// The appender's state, owned jointly by the [`WriteLedger`] and its appender task.
///
/// # The batched appender (design § 16.6.14)
///
/// One task per `WriteLedger` drains the queue in batches of up to [`LEDGER_BATCH_MAX`]. For
/// each batch, holding `append_mutex` and `sequence` throughout, it seeds the sequence if
/// needed, issues `next … next+b−1` in drain order, raises the mark write-ahead to cover the
/// batch's **last** seq, groups the lines by shard file, writes up to
/// [`LEDGER_GROUP_CONCURRENCY`] groups at once (one `write_all` and one sync per group), and
/// waits for every group to resolve before the next batch is issued. That wait is the barrier
/// § 16.6.1's cut depends on: no seq lower than one visible in a shard file is still to be
/// written to that shard. Each entry's reply is its own group's outcome.
struct AppenderCore {
    cache_dir: PathBuf,
    instance_id: String,
    /// Serialises `Ledger_Seq` assignment and the batch's writes. The appender task is the only
    /// caller, so this is uncontended; it is held for the whole batch so the issue-then-write
    /// order is a property of one guard rather than of the task's loop shape.
    append_mutex: Mutex<()>,

    /// The `Ledger_Seq` allocator, or `None` until it has been seeded.
    ///
    /// Guarded by `append_mutex` rather than being an atomic, deliberately: seeding is an
    /// I/O step that must not race with an issue, and an atomic would let a second appender
    /// issue a number while the first was still reading to find the base.
    sequence: Mutex<Option<SequenceState>>,

    /// Whether this process has made sure a `.ready` record exists for this id, which the
    /// appender does before its first batch. Design § 16.6.6 B10.3. Set only once the record is
    /// known to be on the volume, so a failed publish is retried on the next batch.
    ready_record_ensured: std::sync::atomic::AtomicBool,

    /// This instance's append-sync seam. Empty outside tests.
    sync_probe: std::sync::Arc<LedgerSyncProbe>,

    /// The pending stage's queue, or `None` until the first paired entry spawns the stage.
    ///
    /// A plain `std` mutex, held only to clone or install the sender, never across an await.
    pending: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<PendingSubmission>>>,
    /// This ledger's stage depths and batching counters. See [`StageCounters`].
    ///
    /// An `Arc` because the pending stage task outlives any one call and is spawned with
    /// owned state rather than a borrow of this core.
    stages: std::sync::Arc<StageCounters>,
    /// The pending stage task, for [`WriteLedger::abort_stages`]. Tests only.
    #[cfg(test)]
    pending_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// One durable entry's `Pending_Credit`, handed to the pending stage, which owns the caller's
/// reply from that point.
///
/// Only an entry whose own group's write and sync returned `Ok` is ever handed over, which is
/// what keeps "a pending record implies a durable ledger line" true under batching.
struct PendingSubmission {
    credit: crate::staged_fold::PendingCredit,
    reply: tokio::sync::oneshot::Sender<Result<u64>>,
}

/// One shard file's share of a batch: its lines in ascending seq order and the batch indices
/// they came from.
struct LedgerGroup {
    path: PathBuf,
    lines: String,
    members: Vec<usize>,
}

/// A group's batch indices and its outcome: the write's own result, or the join error's text if
/// the blocking task panicked.
type GroupOutcome = (Vec<usize>, std::result::Result<std::io::Result<()>, String>);

/// One submission's reply, held while its group is written.
struct LedgerReply {
    reply: tokio::sync::oneshot::Sender<Result<u64>>,
    seq: u64,
    /// Set early only for an entry that failed before any write (serialisation).
    outcome: Option<Result<u64>>,
    /// This entry's `Pending_Credit`, for a paired entry: present means the pending stage takes
    /// the reply over once this entry's group has synced.
    pending: Option<crate::staged_fold::PendingCredit>,
    /// This submission's hand-over flag, carried over from its [`LedgerSubmission`].
    handed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Per-instance record of [`WriteLedger::append`]'s data sync. Carries fields only under
/// `cfg(test)`, where a test counts syncs and injects a sync failure.
#[derive(Debug, Default)]
pub(crate) struct LedgerSyncProbe {
    #[cfg(test)]
    pub(crate) syncs: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(crate) fail: std::sync::atomic::AtomicBool,
    /// Fail the sync of this one shard file only.
    #[cfg(test)]
    pub(crate) fail_path: std::sync::Mutex<Option<PathBuf>>,
    /// Park the first group written to this file, before it takes the file's lock, until the
    /// gate opens. Consumed by the group it parks.
    #[cfg(test)]
    pub(crate) park: std::sync::Mutex<Option<(PathBuf, std::sync::Arc<tests::GroupGate>)>>,
    /// At each group's line `write_all`, read this `.ledger.mark` and record its value.
    #[cfg(test)]
    pub(crate) observe_mark: std::sync::Mutex<Option<PathBuf>>,
    #[cfg(test)]
    pub(crate) observed_marks: std::sync::Mutex<Vec<u64>>,
    /// Panic in the group written to this file, after its `write_all`. Consumed by that group.
    #[cfg(test)]
    pub(crate) panic_after_write: std::sync::Mutex<Option<PathBuf>>,
    /// `(first, last)` seq of every batch issued.
    #[cfg(test)]
    pub(crate) batches: std::sync::Mutex<Vec<(u64, u64)>>,
    /// Park the pending-credit stage's next batch, before it writes, until the gate opens.
    /// Consumed by the batch it parks.
    ///
    /// The only way a test can hold one credit in the pending stage while the append stage's
    /// depth is already zero, which is the state the shutdown drain's fixpoint is about.
    #[cfg(test)]
    pub(crate) pending_park: std::sync::Mutex<Option<std::sync::Arc<tests::GroupGate>>>,
    /// Report every authority read from this 0-based call index onward as incomplete. `None`
    /// is off.
    ///
    /// The second half of the repair test needs a shard whose **coverage** read is complete and
    /// whose later **pairing** read is not. No on-disk fixture produces that: both reads list
    /// the same files, so anything that breaks one breaks the other.
    #[cfg(test)]
    pub(crate) incomplete_authority_reads_from: std::sync::Mutex<Option<u64>>,
    /// Authority reads seen, and how many of them this seam reported incomplete. The second is
    /// a test's non-vacuity check: zero means the fixture never reached the branch.
    #[cfg(test)]
    pub(crate) authority_reads: AtomicU64,
    #[cfg(test)]
    pub(crate) authority_reads_made_incomplete: AtomicU64,
}

impl LedgerSyncProbe {
    /// Count this authority read and, if the seam is armed for it, report it as incomplete.
    ///
    /// `files_unreadable` rather than a flag of its own, so the read's own
    /// [`LedgerReadOutcome::is_complete`] decides, exactly as it does for a real unreadable
    /// file. A caller that refuses on incompleteness cannot tell the two apart, which is the
    /// point.
    #[cfg(test)]
    fn fault_authority_read(&self, outcome: &mut LedgerReadOutcome) {
        let n = self.authority_reads.fetch_add(1, AtomicOrdering::SeqCst);
        if (*self.incomplete_authority_reads_from.lock().unwrap()).is_some_and(|from| n >= from) {
            outcome.files_unreadable += 1;
            self.authority_reads_made_incomplete
                .fetch_add(1, AtomicOrdering::SeqCst);
        }
    }
}

/// `sync_data` on a ledger file after an append, before the file is dropped.
fn sync_append(file: &std::fs::File, path: &Path, probe: &LedgerSyncProbe) -> std::io::Result<()> {
    #[cfg(test)]
    if probe.fail.load(std::sync::atomic::Ordering::SeqCst)
        || probe.fail_path.lock().unwrap().as_deref() == Some(path)
    {
        return Err(std::io::Error::other("injected ledger sync failure"));
    }
    #[cfg(not(test))]
    let _ = path;
    file.sync_data()?;
    #[cfg(test)]
    probe
        .syncs
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    #[cfg(not(test))]
    let _ = probe;
    Ok(())
}

/// The sequence allocator's state: the next number to issue, and how far the persistent mark
/// has been reserved ahead of it.
///
/// # The sequence stays PER INSTANCE, not per shard file, and that is load-bearing
///
/// A per-shard sequence looks natural once the files are per shard, and it is **unsafe while
/// the fold's cut is derived from a per-instance maximum**. `read_for_authority` reports
/// `max_seq_per_instance` as the maximum across every one of that instance's files, and the
/// fold applies it as the cut for *every* shard. So if shard `A`'s sequence stood at 5,000 and
/// shard `B`'s at 10, `B`'s next credits would be compared against a cut of 5,000 and filtered
/// as already-folded while no fold had read their `.meta` — bytes in neither term, which is
/// § 16.6.1's forbidden outcome, permanently rather than transiently.
///
/// One monotone per-instance counter makes every credit's coordinate strictly above every cut
/// that counter has ever justified, for every shard, including a shard whose file does not
/// exist yet. That is the whole invariant, and it holds without the fold changing at all.
#[derive(Debug, Clone, Copy)]
struct SequenceState {
    /// The next `Ledger_Seq` to issue.
    next: u64,
    /// The highest seq the on-disk mark file currently guarantees is not reissued.
    mark_reserved: u64,
}

impl WriteLedger {
    /// Create a ledger writer for `instance_id`, which must be the same
    /// `{hostname}:{pid}` identity the journal uses so an instance's files sort together
    /// by eye and dead-instance cleanup can match them.
    pub fn new(cache_dir: PathBuf, instance_id: String) -> Self {
        let core = std::sync::Arc::new(AppenderCore {
            cache_dir: cache_dir.clone(),
            instance_id: instance_id.clone(),
            append_mutex: Mutex::new(()),
            sequence: Mutex::new(None),
            ready_record_ensured: std::sync::atomic::AtomicBool::new(false),
            sync_probe: std::sync::Arc::new(LedgerSyncProbe::default()),
            pending: std::sync::Mutex::new(None),
            stages: std::sync::Arc::new(StageCounters::default()),
            #[cfg(test)]
            pending_task: std::sync::Mutex::new(None),
        });
        Self {
            cache_dir,
            instance_id,
            core,
            submit: std::sync::Mutex::new(None),
            #[cfg(test)]
            appender_task: std::sync::Mutex::new(None),
            entry_count: Mutex::new(None),
        }
    }

    /// This instance's append-sync seam.
    #[cfg(test)]
    pub(crate) fn sync_probe(&self) -> &LedgerSyncProbe {
        &self.core.sync_probe
    }

    /// Submissions sent to the appender and not yet taken into a batch.
    #[cfg(test)]
    pub(crate) fn queued_len(&self) -> usize {
        self.submit
            .lock()
            .unwrap()
            .as_ref()
            .map(|tx| tx.max_capacity() - tx.capacity())
            .unwrap_or(0)
    }

    /// The appender's queue, spawning the appender task on first use.
    ///
    /// Respawned only if the previous task has ended (its receiver is gone), which happens only
    /// if the runtime that ran it shut down or the task panicked outside a group write.
    fn appender(&self) -> tokio::sync::mpsc::Sender<LedgerSubmission> {
        let mut slot = self.submit.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = slot.as_ref() {
            if !tx.is_closed() {
                return tx.clone();
            }
        }
        let (tx, rx) = tokio::sync::mpsc::channel(LEDGER_QUEUE_CAPACITY);
        let task = tokio::spawn(AppenderCore::run(std::sync::Arc::clone(&self.core), rx));
        #[cfg(test)]
        {
            *self.appender_task.lock().unwrap() = Some(task);
        }
        #[cfg(not(test))]
        drop(task);
        *slot = Some(tx.clone());
        tx
    }

    /// End both stage tasks at once, the way dropping the runtime does at process exit.
    ///
    /// The seam [`Self::drain_stages`] is asserted against: with the drain in place every
    /// submission has been answered before this runs, and without it an entry whose ledger
    /// group already synced loses its reply here and its caller sees the dropped-reply `Err`.
    /// A test can therefore observe the exact divergence the drain removes, rather than
    /// arguing about what a dropped runtime would have done.
    #[cfg(test)]
    pub(crate) fn abort_stages(&self) {
        if let Some(t) = self.appender_task.lock().unwrap().take() {
            t.abort();
        }
        if let Some(t) = self.core.pending_task.lock().unwrap().take() {
            t.abort();
        }
    }

    /// This instance's identity, `{hostname}:{pid}`.
    ///
    /// Exposed so the fold keys its `.pending` GC and its cut lookup on the *same*
    /// identity that names this instance's `.ledger` files. Re-deriving it from
    /// `gethostname()` and `process::id()` at the fold would produce the same string today
    /// and is exactly the kind of second derivation that later diverges.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// `{cache_dir}/metadata/_write_ledger`.
    ///
    /// `pub(crate)` rather than `pub`: nothing outside the crate needs it, and `pub` would
    /// stop `dead_code` reporting it if the last caller ever went away.
    pub(crate) fn ledger_dir(&self) -> PathBuf {
        self.cache_dir.join("metadata").join(LEDGER_DIR)
    }

    /// The legacy flat path for an arbitrary instance, for converting a peer's file.
    pub(crate) fn legacy_path_for(cache_dir: &Path, instance_id: &str) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(LEDGER_DIR)
            .join(format!("{}.{}", instance_id, LEDGER_EXT))
    }

    /// `_write_ledger/{bucket}/{l1}` — the directory holding one shard's ledger files.
    pub(crate) fn shard_dir(cache_dir: &Path, bucket: &str, l1: &str) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(LEDGER_DIR)
            .join(bucket)
            .join(l1)
    }

    /// `_write_ledger/{bucket}/{l1}/{instance}.ledger` for an arbitrary instance.
    ///
    /// The one construction of a shard ledger path. Five `#[cfg(test)]` sites across
    /// `staged_fold.rs`, `ledger_repair.rs` and `journal_consolidator.rs` used to re-derive
    /// the flat name by hand; they call this instead, so the layout has exactly one
    /// definition and a future move cannot leave a test asserting against a path production
    /// no longer writes.
    pub(crate) fn shard_ledger_path(
        cache_dir: &Path,
        bucket: &str,
        l1: &str,
        instance_id: &str,
    ) -> PathBuf {
        Self::shard_dir(cache_dir, bucket, l1).join(format!("{}.{}", instance_id, LEDGER_EXT))
    }

    /// This instance's ledger file for the shard `cache_key` belongs to.
    ///
    /// `None` when [`crate::staged_fold::shard_of`] cannot place the key — see
    /// [`unshardable_key_error`] for what the append does with that.
    pub(crate) fn shard_path_for_key(&self, cache_key: &str) -> Option<PathBuf> {
        let (bucket, l1) = crate::staged_fold::shard_of(cache_key)?;
        Some(Self::shard_ledger_path(
            &self.cache_dir,
            &bucket,
            &l1,
            &self.instance_id,
        ))
    }

    /// `_write_ledger/{instance}.ledger.mark` — this instance's persistent sequence mark.
    ///
    /// Flat rather than per shard, because what it bounds is per instance: see
    /// [`AppenderCore::seed_sequence`].
    #[cfg(test)]
    pub(crate) fn seq_mark_path(&self) -> PathBuf {
        Self::seq_mark_path_for(&self.cache_dir, &self.instance_id)
    }

    /// `{instance}.ledger.mark` for an arbitrary instance, for the conversion to raise.
    fn seq_mark_path_for(cache_dir: &Path, instance_id: &str) -> PathBuf {
        cache_dir
            .join("metadata")
            .join(LEDGER_DIR)
            .join(format!("{}{}", instance_id, SEQ_MARK_SUFFIX))
    }

    /// `{instance}` from `{instance}.ledger`, for attributing a file's high-water mark.
    ///
    /// Taken from the file name rather than from an entry's `instance_id`, because a file
    /// with no seq-bearing entries still has a name — and because the header belongs to the
    /// file, not to any entry in it.
    fn instance_of(path: &Path) -> String {
        path.file_stem()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Lock path guarding a ledger file against concurrent append and compaction.
    fn lock_path_for(ledger_path: &Path) -> PathBuf {
        let mut name = ledger_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        name.push_str(".lock");
        ledger_path.with_file_name(name)
    }

    /// Record that a staged range now exists.
    ///
    /// Called from the sites that credit `SizeAccumulator::add_write_cache`, which is
    /// the definition of "a staged range was just published". Keeping the two together
    /// is what makes the ledger's coverage checkable: a credit without an append is a
    /// staged entry eviction cannot see.
    ///
    /// A failure never fails the request, because the upload has already succeeded and S3
    /// already holds the object. The caller (`JournalConsolidator::record_staged_range`)
    /// logs it at WARN and its callers decline the matching write-cache credit, so no
    /// write-tier credit exists without a ledger entry. The ledger-repair rotation and a
    /// Validation_Scan (R2.7) re-append the entry from the `.meta`. A peer whose identity is
    /// not shared can re-append the range, but a pass refused on a shared identity keeps its
    /// claim on the shard, so the entry can stay unindexed until the hostnames are made
    /// distinct.
    pub async fn append_staged_range(
        &self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
        compressed_size: u64,
    ) -> Result<()> {
        self.append_staged_entry(WriteLedgerEntry {
            // Wall clock, for the eviction candidate order and the supersede comparison —
            // NOT for the cut coordinate, which `append` assigns. See
            // [`WriteLedgerEntry::seq`].
            timestamp: SystemTime::now(),
            cache_key: cache_key.to_string(),
            range_start,
            range_end,
            compressed_size,
            instance_id: self.instance_id.clone(),
            // Overwritten by `append`. Set here only so the literal is complete.
            seq: None,
        })
        .await
    }

    /// [`Self::append_staged_range`]'s body: append the ledger entry paired with its
    /// `Pending_Credit`.
    ///
    /// `pub(crate)` and taking a whole entry so a test can vary the entry's **timestamp**
    /// while running the identical pairing code production runs. That matters for exactly
    /// one property: the pairing must be indifferent to the clock. A fixture that reached
    /// past this function and appended the two halves itself would be asserting its own
    /// arithmetic rather than this function's.
    ///
    /// # The two halves are stamped by the appender, not here
    ///
    /// The ledger stage hands this entry to the pending stage once its own group's sync has
    /// returned `Ok`, and the pending stage answers this call once the record is durable or its
    /// own write has failed. So both halves carry the *same* `Ledger_Seq` without this call site
    /// ever seeing it: one credit, one coordinate, on exactly one side of every cut.
    /// `staged_fold`'s exactly-once argument is that seq, so the two must never be stamped
    /// independently.
    ///
    /// The hand-over is after the sync and never at submit. A pending record whose ledger entry
    /// does not exist could never be passed by a cut, because no read could observe its seq, so
    /// the pending term would count those bytes forever.
    pub(crate) async fn append_staged_entry(&self, entry: WriteLedgerEntry) -> Result<()> {
        self.submit(entry, true).await.map(|_| ())
    }

    /// Append one entry to this instance's ledger file with a true `O_APPEND` write,
    /// assigning it a `Ledger_Seq`.
    ///
    /// Returns the seq assigned. Any `seq` the caller put on `entry` is **overwritten** —
    /// the ledger owns the sequence, because it is the only thing that can know the next
    /// value for this file.
    ///
    /// See the module docs for why this does not use the journal's read-modify-write
    /// shape: a ledger file is as long as the staged set, so rewriting it per append
    /// would be O(staged) work per PUT. Only the first append after start reads the file,
    /// to seed the sequence; every append after that is a pure `O_APPEND` write.
    ///
    /// # Refuses to append when it cannot establish its own sequence
    ///
    /// If this instance's sequence mark, or a legacy flat file it still owns, exists and
    /// cannot be read, this returns `Err` and appends nothing, rather than starting the
    /// sequence over from 1. § 16.6.1 pairs this rule with the seq for a reason: a silently
    /// reset sequence re-issues numbers that are already at or below an installed cut, so
    /// every credit it stamps is filtered out of the pending term while the fold has not seen
    /// its `.meta` either. That is the counted-by-neither outcome, and unlike a lost append it
    /// would not be transient — it would persist for as long as the reset sequence took to
    /// climb back past the cut.
    ///
    /// Declining the append instead leaves the range unindexed, which the fold sees as an
    /// **under**-count repaired by range identity on the next validation pass. Both are
    /// bad; only one is bounded and logged.
    ///
    /// # Refuses to append while another live process holds this instance identity
    ///
    /// The same rule for the same reason. `{instance}.ledger.mark` is the only record of what
    /// this identity has issued, and the allocator that reads it is per process: two processes
    /// on one `{host}:{pid}` seed from one mark, reserve blocks over each other and issue the
    /// same `Ledger_Seq` to different ranges. A credit stamped with a number a `Fold_Cut` has
    /// already passed is filtered out of the pending term while no fold has read its `.meta`,
    /// and — unlike the reset-sequence case — the entry IS in the ledger file, so the repair
    /// sweep's coverage test reads the range as indexed and never repairs it. Refusing is the
    /// bounded outcome. A peer whose identity is not shared can re-append the range, but this
    /// process's refused repair pass keeps its claim on the shard, so the entry can stay
    /// unindexed until the hostnames are made distinct. See
    /// `signed_put_handler::identity_shared_with_a_live_peer`, which the
    /// hostname detector latches, and which also stops this process admitting write-cache
    /// entries at all.
    ///
    /// # Refuses an unshardable key
    ///
    /// The entry's `cache_key` selects the file, so a key
    /// [`crate::staged_fold::shard_of`] cannot place has nowhere to go — see
    /// [`unshardable_key_error`].
    pub async fn append(&self, entry: WriteLedgerEntry) -> Result<u64> {
        self.submit(entry, false).await
    }

    /// Enqueue one entry and wait for whichever stage owns its reply.
    ///
    /// `pair_pending` chooses that stage: the ledger stage answers a raw append as soon as the
    /// entry's group has synced, and the pending stage answers a paired one, after its
    /// `Pending_Credit` is durable or the stage's own write has failed. Design § 16.6.14.
    async fn submit(&self, entry: WriteLedgerEntry, pair_pending: bool) -> Result<u64> {
        // The shard comes from the entry, so the file is chosen, and an unshardable key
        // refused, before anything is enqueued.
        let path = self
            .shard_path_for_key(&entry.cache_key)
            .ok_or_else(|| unshardable_key_error(&entry.cache_key))?;
        let (reply, outcome) = tokio::sync::oneshot::channel();
        // THIS submission's hand-over state, set by the hand-over itself. It is what the
        // dropped-reply error is attributed from, because it is the only thing that answers the
        // question the operator needs answered: was this entry's ledger line durable when its
        // reply was lost? Sender closedness cannot answer it — at process teardown both stages
        // have ended, so "which senders are closed" is the same value for every entry.
        let handed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Counted in before the send, so the drain can never read a depth that omits a
        // submission already on the queue. Decremented by whichever stage answers it.
        self.core
            .stages
            .ledger
            .fetch_add(1, AtomicOrdering::Relaxed);
        // A full queue makes this wait; nothing is shed.
        if self
            .appender()
            .send(LedgerSubmission {
                entry,
                path,
                pair_pending,
                reply,
                handed: std::sync::Arc::clone(&handed),
            })
            .await
            .is_err()
        {
            self.core
                .stages
                .ledger
                .fetch_sub(1, AtomicOrdering::Relaxed);
            return Err(ProxyError::CacheError(
                "Write-ledger appender has stopped; the entry was not appended".to_string(),
            ));
        }
        outcome.await.map_err(|_| {
            ProxyError::CacheError(format!(
                "The {} stage ended without replying; the entry's outcome is unknown",
                Self::stage_owning_reply(handed.load(std::sync::atomic::Ordering::Acquire))
            ))
        })?
    }

    /// Which stage dropped a submission's reply, for the error [`Self::submit`] raises when the
    /// reply channel closes without an answer.
    ///
    /// Read from **that submission's own** hand-over state: `handed_to_pending` is set by
    /// `reply_per_group` immediately before it gives the reply to the pending stage, and only an
    /// entry whose own group's write and sync returned `Ok` is ever handed over. So `true` means
    /// the ledger line is durable and `false` means the append stage still owned the reply, for
    /// every entry, independently of what any other entry or any stage task was doing.
    ///
    /// # Why not sender closedness
    ///
    /// It was, and it was wrong in the one case that happens in production. Attributing from
    /// "which stage's sender is closed" is exact only while exactly one stage has ended; at
    /// ordinary process teardown **both** have, and the expression then named the pending-credit
    /// stage for every paired entry — including one the append stage dropped before hand-over,
    /// whose line may never have been written at all.
    ///
    /// The distinction is not cosmetic. The append stage dropping a reply means the entry's line
    /// may never have been written; the pending stage dropping one means the line **is** durable
    /// and only the `Pending_Credit` is in doubt, so the bytes are counted by the next cut that
    /// passes the line and a declined credit under-states them meanwhile.
    fn stage_owning_reply(handed_to_pending: bool) -> &'static str {
        if handed_to_pending {
            "write-ledger pending-credit"
        } else {
            "write-ledger append"
        }
    }

    /// This ledger's batching counters, for `/metrics` and the OTLP export.
    ///
    /// Plain atomic loads; no file is read and no lock is taken, so a scrape costs nothing.
    pub fn stage_metrics(&self) -> WriteLedgerMetrics {
        let s = &self.core.stages;
        WriteLedgerMetrics {
            append_batches_total: s.append_batches.load(AtomicOrdering::Relaxed),
            append_batched_entries_total: s.append_batched_entries.load(AtomicOrdering::Relaxed),
            pending_batches_total: s.pending_batches.load(AtomicOrdering::Relaxed),
            pending_batch_failures_total: s.pending_batch_failures.load(AtomicOrdering::Relaxed),
        }
    }

    /// Drain both append stages: wait until every submission they hold has been answered, or
    /// until `bound` elapses.
    ///
    /// # Why shutdown needs this
    ///
    /// A submission's reply comes from a task. When the runtime goes away those tasks are
    /// dropped, and a dropped task's `oneshot::Sender` closing is what `append_staged_range`
    /// reports as a failure — so `record_staged_range` returns `false` and its caller declines
    /// the write-cache credit. For an entry whose ledger line is already durable that is the
    /// wrong answer in the worst direction: the staged fold counts those bytes at the next cut
    /// that passes the line, while the incumbent counter never credited them, so the two
    /// derivations of the same figure diverge. Answering every submission before the runtime
    /// stops removes the case rather than compensating for it.
    ///
    /// # Order, and why it is not symmetric
    ///
    /// The append stage first, then the pending stage. The append stage is the pending stage's
    /// only producer, so a pending depth of zero proves nothing while the append stage still
    /// holds an entry it is about to hand over. Once the append stage's depth reaches zero every
    /// submission has been answered or handed over. The pending stage's depth does not only
    /// fall from there: a submission accepted on the append stage after its loop exited is
    /// handed over and raises it again, which is what the fixpoint below handles.
    ///
    /// # Past the bound
    ///
    /// Nothing is cancelled and no queue is closed. An entry still held at the bound keeps
    /// exactly what it would have got without the drain: its real reply if its stage reaches it
    /// before the runtime stops, and the dropped-reply `Err` above if it does not. The returned
    /// [`StageDrainOutcome`] carries both depths so the caller can log what it left behind.
    ///
    /// # A fixpoint, not one pass over the two stages
    ///
    /// A submission that arrives *during* the drain is drained too, which is the point: a request
    /// still in flight at shutdown is exactly the entry at risk. One pass over `[ledger,
    /// pending]` does not deliver that, and the gap is not academic: an entry accepted on the
    /// **ledger** stage after that stage's own loop has exited — a write-through PUT whose S3
    /// response lands during the milliseconds the pending loop runs — would be left with the
    /// closed-channel `Err` this drain exists to remove. So the two loops sit inside an outer
    /// loop that returns only when the ledger depth and then the pending depth read zero in
    /// two sequential loads, not one atomic snapshot, or the deadline passes.
    ///
    /// The two depths are read ledger-first, and that order is what makes "both zero" sound
    /// rather than a sampling artefact: [`Self::hand_to_pending`] raises the pending depth
    /// **before** `reply_per_group` lowers the ledger one, so an entry moving between the stages
    /// is counted by the pending depth at every instant the ledger depth no longer holds it.
    ///
    /// It also means the drain can run for the whole bound under sustained load, which is why
    /// the bound is a bound and not a deadline on an idle queue.
    pub async fn drain_stages(&self, bound: std::time::Duration) -> StageDrainOutcome {
        let deadline = tokio::time::Instant::now() + bound;
        loop {
            for counter in [&self.core.stages.ledger, &self.core.stages.pending] {
                while counter.load(AtomicOrdering::Relaxed) > 0 {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        break;
                    }
                    tokio::time::sleep(STAGE_DRAIN_POLL.min(deadline - now)).await;
                }
            }
            // Ledger first: see the order argument above. A submission that arrived while the
            // pending loop ran shows up here and sends the drain round again.
            let ledger_unanswered = self.core.stages.ledger.load(AtomicOrdering::Relaxed);
            let pending_unanswered = self.core.stages.pending.load(AtomicOrdering::Relaxed);
            if (ledger_unanswered == 0 && pending_unanswered == 0)
                || tokio::time::Instant::now() >= deadline
            {
                return StageDrainOutcome {
                    ledger_unanswered,
                    pending_unanswered,
                };
            }
        }
    }

    /// Both stages' depths, for the drain's own tests: what the append stage has neither
    /// answered nor handed over, and what the pending stage still owes.
    #[cfg(test)]
    pub(crate) fn stage_depths(&self) -> (u64, u64) {
        (
            self.core.stages.ledger.load(AtomicOrdering::Relaxed),
            self.core.stages.pending.load(AtomicOrdering::Relaxed),
        )
    }

    /// Raise another instance's sequence mark to at least `floor`, for the legacy conversion.
    ///
    /// `max(existing, incoming)` rather than a blind overwrite: a pre-upgrade peer can recreate
    /// its flat file after a conversion, and a second conversion of that file must not lower a
    /// mark an earlier one established.
    fn raise_seq_mark_blocking(
        cache_dir: &Path,
        instance_id: &str,
        floor: u64,
    ) -> std::io::Result<u64> {
        use std::io::Write;
        let path = Self::seq_mark_path_for(cache_dir, instance_id);
        let existing = match std::fs::read_to_string(&path) {
            Ok(content) => serde_json::from_str::<LedgerHeader>(content.trim())
                .map(|h| h.max_seq_ever)
                .map_err(|e| std::io::Error::other(format!("parse sequence mark: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };
        let raised = existing.max(floor);
        let header = LedgerHeader {
            schema: LEDGER_SCHEMA,
            max_seq_ever: raised,
        };
        let mut body = serde_json::to_string(&header)
            .map_err(|e| std::io::Error::other(format!("serialize sequence mark: {e}")))?;
        body.push('\n');
        let tmp = path.with_extension("mark.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(raised)
    }

    /// Every instance that has a ledger file on this volume, by instance ID.
    ///
    /// # This is the staged-authority activation gate's evidence set
    ///
    /// § 16.6.6's load-bearing activation precondition is that every instance with a ledger
    /// file has also published a `.ready` record, and the reason it is load-bearing is that
    /// **appending a staged credit is what creates the ledger file**. A pre-C binary has no
    /// code to publish a readiness record, so an unmatched ledger file is exactly the
    /// participant the gate must refuse to activate alongside — and it cannot hide from this
    /// enumeration without also having credited nothing.
    ///
    /// The ID is the file stem: `{instance_id}.ledger`, the same construction
    /// [`Self::shard_ledger_path`] writes, and unchanged by the split because the instance is
    /// still the file *name* and the shard is the directory. An `Err` means a directory could
    /// not be listed, which callers must treat as "the participant set is unknown" and NOT as
    /// "there are none" — an empty set would satisfy the correspondence check vacuously.
    ///
    /// Counts legacy flat files too. A pre-upgrade participant is precisely the one the gate
    /// must refuse to activate alongside, and its file is flat.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.6.
    pub(crate) async fn ledger_instances(&self) -> Result<std::collections::BTreeSet<String>> {
        Ok(self
            .all_ledger_files()
            .await?
            .iter()
            .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
            .collect())
    }

    /// Every `{instance}.ledger` file on the volume, shard and legacy flat alike: the same set
    /// [`Self::all_ledger_files`] lists, from `std::fs` so the activation gate can take it on the
    /// blocking pool. `1 + B + S` `readdir`s and no file opens. `Err` when a directory could not
    /// be listed.
    ///
    /// Taken once per activation verdict and shared between § 16.6.6 B7's read
    /// ([`Self::latest_append_secs_in`]) and B8/B9's stat ([`Self::latest_modified_in`]).
    pub(crate) fn ledger_files_blocking(cache_dir: &Path) -> Result<Vec<PathBuf>> {
        let root = cache_dir.join("metadata").join(LEDGER_DIR);
        let mut files = Self::ledger_files_in(&root)?;
        for bucket_dir in Self::subdirs_of(&root)? {
            for l1_dir in Self::subdirs_of(&bucket_dir)? {
                files.extend(Self::ledger_files_in(&l1_dir)?);
            }
        }
        Ok(files)
    }

    /// The newest modification time across every file in `files` named for each of
    /// `instance_ids`, for § 16.6.6 B8 and B9.
    ///
    /// # mtime, not the entry timestamp, and why that is the safe direction here
    ///
    /// B8's population is header-only files, where [`Self::latest_append_secs_in`] can only say
    /// [`AppendEvidence::NoEntries`] — which a live pre-C writer whose last entry was compacted
    /// away also produces. An append sets the mtime, and the only other writers of a ledger file
    /// (compaction's rewrite, legacy conversion) run after the entries they carry were appended,
    /// so the mtime is never earlier than the last append. It can only overstate recency, which
    /// keeps an id blocking longer. One `stat` per file named for a requested id; no opens.
    ///
    /// Per id: [`ModifiedEvidence::NoFiles`] when no file in `files` names it (it was removed or
    /// converted after the id was listed), [`ModifiedEvidence::Indeterminate`] when a file named
    /// for it could not be `stat`ed. A file removed between the listing and the `stat` is
    /// skipped, as [`Self::latest_append_secs_in`] skips it.
    pub(crate) fn latest_modified_in(
        files: &[PathBuf],
        instance_ids: &std::collections::BTreeSet<String>,
    ) -> std::collections::BTreeMap<String, ModifiedEvidence> {
        let mut evidence: std::collections::BTreeMap<String, ModifiedEvidence> = instance_ids
            .iter()
            .map(|id| (id.clone(), ModifiedEvidence::NoFiles))
            .collect();
        for path in files {
            let Some(id) = path.file_stem().map(|s| s.to_string_lossy().to_string()) else {
                continue;
            };
            let Some(slot) = evidence.get_mut(&id) else {
                continue;
            };
            let observed = match std::fs::metadata(path).and_then(|m| m.modified()) {
                Ok(t) => ModifiedEvidence::LatestAt(t),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    debug!("Could not stat write-ledger file {:?}: {}", path, e);
                    ModifiedEvidence::Indeterminate
                }
            };
            *slot = slot.combine(observed);
        }
        evidence
    }

    /// The latest entry `timestamp`, in unix seconds, across every file in `files` named for
    /// each of `instance_ids`. `files` is [`Self::ledger_files_blocking`]'s listing, the same set
    /// [`Self::ledger_instances`] uses.
    ///
    /// This is § 16.6.6 B7's append-time evidence: whether an instance has appended since its
    /// `.ready` record was written. The entry timestamp rather than the file's mtime, because
    /// compaction rewrites a file when it drops an entry (so a dead instance's file gets a new
    /// mtime after it died) while it never rewrites a timestamp, and because the timestamp is the
    /// appending host's clock, the same clock that wrote the record.
    ///
    /// Per id: [`AppendEvidence::NoEntries`] when no file names it or every file named for it is
    /// header-only; [`AppendEvidence::Indeterminate`] when a file named for it could not be read or
    /// holds a line that is neither an entry nor a header (it may be the latest append). Blocking
    /// `std::fs`; the one caller runs on the blocking pool.
    pub(crate) fn latest_append_secs_in(
        files: &[PathBuf],
        instance_ids: &std::collections::BTreeSet<String>,
    ) -> std::collections::BTreeMap<String, AppendEvidence> {
        let mut evidence: std::collections::BTreeMap<String, AppendEvidence> = instance_ids
            .iter()
            .map(|id| (id.clone(), AppendEvidence::NoEntries))
            .collect();
        for path in files {
            let Some(id) = path.file_stem().map(|s| s.to_string_lossy().to_string()) else {
                continue;
            };
            let Some(slot) = evidence.get_mut(&id) else {
                continue;
            };
            let observed = match std::fs::read_to_string(path) {
                Ok(content) => {
                    let parsed = Self::parse_content(path, &content);
                    if parsed.malformed > 0 {
                        AppendEvidence::Indeterminate
                    } else {
                        parsed
                            .entries
                            .iter()
                            .map(|e| {
                                e.timestamp
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0)
                            })
                            .max()
                            .map_or(AppendEvidence::NoEntries, AppendEvidence::LatestAt)
                    }
                }
                // Removed between the listing and the read: compaction deletes only a legacy
                // file with no high-water mark, so there was nothing to observe.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => AppendEvidence::NoEntries,
                Err(e) => {
                    debug!("Could not read write-ledger file {:?}: {}", path, e);
                    AppendEvidence::Indeterminate
                }
            };
            *slot = slot.combine(observed);
        }
        evidence
    }

    /// One directory's `*.ledger` files, or `Err` if it could not be listed.
    ///
    /// `is_file()` excludes the shard directories themselves, and `extension() == "ledger"`
    /// excludes every sibling the layout puts beside a ledger file: `.lock`, `.ledger.torn`,
    /// `.ledger.tmp` and `.ledger.mark` all report a final extension that is not `ledger`.
    fn ledger_files_in(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        if !dir.exists() {
            return Ok(files);
        }
        let read_dir = std::fs::read_dir(dir).map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to read write-ledger directory {:?}: {}",
                dir, e
            ))
        })?;
        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Failed to read write-ledger directory entry: {}", e);
                    continue;
                }
            };
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == LEDGER_EXT) {
                files.push(path);
            }
        }
        Ok(files)
    }

    /// Immediate subdirectories of `dir`.
    fn subdirs_of(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut dirs = Vec::new();
        if !dir.exists() {
            return Ok(dirs);
        }
        let read_dir = std::fs::read_dir(dir).map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to read write-ledger directory {:?}: {}",
                dir, e
            ))
        })?;
        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Failed to read write-ledger directory entry: {}", e);
                    continue;
                }
            };
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            }
        }
        Ok(dirs)
    }

    /// Every **shard** ledger file, `{bucket}/{l1}/{instance}.ledger`, across all instances.
    ///
    /// Two levels of listing rather than one: `1 + B + B×256` readdirs against the flat
    /// layout's 1. That is the cost of confining a ledger file to a shard, and it is paid by
    /// the whole-volume readers only — [`Self::read_shard_for_authority`] lists one shard
    /// directory, and [`Self::ledger_shards`] lists `1 + B` directories and opens nothing.
    /// **Neither the fold nor the completeness-repair rotation calls this any more**; its
    /// remaining callers are the hint readers, compaction, and staging eviction's candidate
    /// merge, none of which runs per fold pass. Compaction is the one that pays it on a fixed
    /// interval, and it pays the listing to visit every file under its lock — which is what makes
    /// "an entry this pass never read is never removed" a statement about the volume rather than
    /// about a rotation. See `retire_identities`.
    async fn shard_ledger_files(&self) -> Result<Vec<PathBuf>> {
        let root = self.ledger_dir();
        let mut files = Vec::new();
        for bucket_dir in Self::subdirs_of(&root)? {
            for l1_dir in Self::subdirs_of(&bucket_dir)? {
                files.extend(Self::ledger_files_in(&l1_dir)?);
            }
        }
        Ok(files)
    }

    /// Every **legacy** flat ledger file, `_write_ledger/{instance}.ledger`.
    ///
    /// Non-empty means at least one participant has not been converted, which is what
    /// [`Self::migrate_legacy_ledgers`] acts on and what the staged fold retains for.
    async fn legacy_ledger_files(&self) -> Result<Vec<PathBuf>> {
        Self::ledger_files_in(&self.ledger_dir())
    }

    /// How many unconverted legacy flat files exist: **one** `readdir` of the ledger root.
    ///
    /// Exists so a caller that no longer performs a whole-volume authority read can still make
    /// the volume-wide legacy check. There is no shard-scoped form of this question — a legacy
    /// file names no shard — so a fold pass asks it once, cheaply, rather than inferring it
    /// from a read it no longer does.
    pub(crate) async fn legacy_ledger_file_count(&self) -> Result<usize> {
        Ok(self.legacy_ledger_files().await?.len())
    }

    /// Every shard that has a ledger **directory**, without opening a single ledger file.
    ///
    /// `1 + B` `readdir`s, where `B` is the bucket count — not `S = B × 256`, and no file
    /// reads at all. This is what lets a fold pass choose which shards to visit before paying
    /// for any of them, which is the whole of § 16.6.12 point 2.
    ///
    /// # It is a SUPERSET of "shards with ledger entries", and that is the safe direction
    ///
    /// A shard whose entries have all been retired keeps a header-only file (see
    /// `retire_identities`), so its directory survives and it is still selected — which is
    /// required, because a shard whose staged set has fully drained must still be visited to
    /// have its figure moved to zero. Extra visits cost one `readdir` and one file read each;
    /// a missed shard keeps a stale figure forever.
    ///
    /// An `l1` component that is not two lowercase hex digits is skipped, so a stray directory
    /// cannot become a phantom shard that gets claimed, visited, and given a zero-figure
    /// checkpoint naming a shard the `.meta` tree has never heard of.
    pub(crate) async fn ledger_shards(&self) -> Result<Vec<(String, String)>> {
        let root = self.ledger_dir();
        let mut shards = Vec::new();
        for bucket_dir in Self::subdirs_of(&root)? {
            let Some(bucket) = bucket_dir
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
            else {
                continue;
            };
            for l1_dir in Self::subdirs_of(&bucket_dir)? {
                let Some(l1) = l1_dir.file_name().map(|n| n.to_string_lossy().to_string()) else {
                    continue;
                };
                if !crate::staged_fold::is_shard_level(&l1) {
                    continue;
                }
                shards.push((bucket.clone(), l1));
            }
        }
        Ok(shards)
    }

    /// Shard files and legacy flat files together.
    ///
    /// This is what the **hint** readers and compaction use, so a legacy file's entries stay
    /// visible to staging eviction and retirable by compaction for the whole conversion
    /// window. [`Self::read_for_authority`] deliberately does not use it: an authority read
    /// must not silently mix a shard-attributed file with one whose shard is unknown, and it
    /// reports the legacy file's *presence* instead so the fold can retain.
    async fn all_ledger_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = self.shard_ledger_files().await?;
        files.extend(self.legacy_ledger_files().await?);
        Ok(files)
    }

    /// Convert every legacy flat `{instance}.ledger` into the per-shard layout.
    ///
    /// # Posture: idempotent read-old / write-new, then retire the source
    ///
    /// Chosen over a one-shot startup conversion because the conversion must be safe to
    /// **re-run** — a pre-upgrade peer can recreate its flat file after we have converted it —
    /// and over "reconstruct it from the journals" because the ledger is not reconstructible:
    /// a `Ledger_Seq` is the cut coordinate and exists nowhere else, so a reconstruction would
    /// renumber and invalidate every installed cut.
    ///
    /// Steps, in this order, per file:
    ///
    /// 1. read the legacy file under its own existing `{instance}.ledger.lock`;
    /// 2. raise that instance's sequence mark to the legacy file's own high-water mark;
    /// 3. append each entry into `{bucket}/{l1}/{instance}.ledger` **preserving its original
    ///    `seq`** — no entry is ever renumbered;
    /// 4. remove the legacy file **last**.
    ///
    /// Every interleaving therefore leaves each entry in at least one place, and a crash at
    /// any point leaves a state a re-run repairs.
    ///
    /// # Why re-running cannot double-count
    ///
    /// **The ledger is an index, not a figure.** `assess_staged_candidate` derives
    /// `staged_bytes` from `metadata.staged_compressed_size()` and `staged_range_count` from
    /// the `.meta`'s own `ranges`, never from an entry's `compressed_size`; `group_by_key`
    /// collapses every entry naming one key into **one** candidate, hence one `.meta` read;
    /// `max_seq_ever` is a max, so duplicating it is a no-op; and `retire_identities` takes a
    /// `HashSet`, so a duplicate identity retires once. No field of `StagedCandidate` sums an
    /// entry's `compressed_size`, so a duplicate entry doubles nothing at all.
    ///
    /// # Every instance's file, not only this one's
    ///
    /// A decommissioned pre-upgrade instance's flat file has nobody left to convert it, and
    /// while it exists the fold retains — so converting only our own would stall the figure
    /// indefinitely. Converting a *live* peer's file is safe for the same reason a re-run is:
    /// the peer keeps appending, we keep converting, and no entry is lost or renumbered.
    ///
    /// Best-effort throughout. A failure is logged and leaves the legacy file in place, which
    /// keeps the fold retaining — visible, and in the over-stating direction.
    pub async fn migrate_legacy_ledgers(&self) {
        let legacy = match self.legacy_ledger_files().await {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "Write-ledger migration: could not list legacy ledger files: {}",
                    e
                );
                return;
            }
        };
        for path in legacy {
            let instance = Self::instance_of(&path);
            let instance_for_task = instance.clone();
            let cache_dir = self.cache_dir.clone();
            let converted =
                tokio::task::spawn_blocking(move || -> std::io::Result<(usize, usize)> {
                    let lock_path = Self::lock_path_for(&path);
                    let lock_file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    fs4::FileExt::lock(&lock_file)?;
                    let result =
                        Self::convert_one_legacy_file(&cache_dir, &instance_for_task, &path);
                    let _ = fs4::FileExt::unlock(&lock_file);
                    result
                })
                .await;

            match converted {
                Ok(Ok((entries, unshardable))) => {
                    info!(
                        instance = %instance,
                        entries,
                        unshardable,
                        "Write-ledger migration: converted a legacy flat ledger to the \
                         per-shard layout and removed the source. Entries kept their original \
                         Ledger_Seq, so every installed cut remains valid."
                    );
                }
                Ok(Err(e)) => warn!(
                    "Write-ledger migration: failed to convert legacy ledger for {}: {}. Left \
                     in place; the staged fold will keep retaining until it is converted.",
                    instance, e
                ),
                Err(e) => warn!("Write-ledger migration task failed for {}: {}", instance, e),
            }
        }
    }

    /// [`Self::migrate_legacy_ledgers`]'s body for one file, with its lock already held.
    ///
    /// Returns `(entries converted, entries dropped as unshardable)`.
    fn convert_one_legacy_file(
        cache_dir: &Path,
        instance_id: &str,
        legacy_path: &Path,
    ) -> std::io::Result<(usize, usize)> {
        let content = match std::fs::read_to_string(legacy_path) {
            Ok(c) => c,
            // Converted by a peer between the listing and the lock.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
            Err(e) => return Err(e),
        };
        let parsed = Self::parse_content(legacy_path, &content);

        // The mark FIRST, so a crash after the entries land cannot leave a process reusing this
        // identity free to reissue numbers the legacy entries already used.
        Self::raise_seq_mark_blocking(cache_dir, instance_id, parsed.max_seq_ever().unwrap_or(0))?;

        // Malformed lines are carried into the shard layout's quarantine rather than dropped —
        // loss mode L5 applies to the conversion exactly as it does to compaction, and the
        // conversion is destructive in the same way.
        if !parsed.malformed_lines_text.is_empty() {
            Self::quarantine_torn_lines(legacy_path, &parsed.malformed_lines_text)?;
        }

        let mut by_shard: BTreeMap<PathBuf, Vec<String>> = BTreeMap::new();
        let mut unshardable = 0usize;
        let mut converted = 0usize;
        for entry in &parsed.entries {
            let Some((bucket, l1)) = crate::staged_fold::shard_of(&entry.cache_key) else {
                // Already counted by nothing under the flat layout: the fold discarded it as
                // `unshardable` because its `.meta` is in no shard.
                unshardable += 1;
                continue;
            };
            let line = match serde_json::to_string(entry) {
                Ok(json) => json,
                Err(e) => {
                    return Err(std::io::Error::other(format!(
                        "serialize legacy ledger entry: {e}"
                    )))
                }
            };
            by_shard
                .entry(Self::shard_ledger_path(
                    cache_dir,
                    &bucket,
                    &l1,
                    instance_id,
                ))
                .or_default()
                .push(line);
            converted += 1;
        }

        for (shard_path, lines) in by_shard {
            Self::append_lines_to_shard(&shard_path, &lines)?;
        }

        // LAST. Until this succeeds every entry is in both places, which is the interleaving
        // that cannot lose one.
        match std::fs::remove_file(legacy_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok((converted, unshardable))
    }

    /// Append already-serialised entry lines to one shard ledger file under its own lock.
    ///
    /// Writes the schema header when the file is created, exactly as [`Self::append`] does, so
    /// a converted file is indistinguishable from one this binary wrote from scratch.
    fn append_lines_to_shard(shard_path: &Path, lines: &[String]) -> std::io::Result<()> {
        use std::io::Write;
        if let Some(dir) = shard_path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let lock_path = Self::lock_path_for(shard_path);
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        fs4::FileExt::lock(&lock_file)?;
        let result = (|| -> std::io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(shard_path)?;
            if file.metadata()?.len() == 0 {
                let header = LedgerHeader {
                    schema: LEDGER_SCHEMA,
                    max_seq_ever: 0,
                };
                let mut head = serde_json::to_string(&header)
                    .map_err(|e| std::io::Error::other(format!("serialize header: {e}")))?;
                head.push('\n');
                file.write_all(head.as_bytes())?;
            }
            for line in lines {
                file.write_all(line.as_bytes())?;
                file.write_all(b"\n")?;
            }
            file.flush()
        })();
        let _ = fs4::FileExt::unlock(&lock_file);
        result
    }

    /// `{instance}.ledger.torn` — the quarantine file beside a ledger file.
    ///
    /// **Not `.ledger`-suffixed, deliberately.** `shard_ledger_files()` and `legacy_ledger_files()`
    /// both select on `extension() == "ledger"`, and `Path::extension` on
    /// `host.example.com:1.ledger.torn` returns `torn` however deep the directory is, so
    /// the quarantine is excluded from every read by construction rather than by a filter someone
    /// has to remember to add. That matters because the quarantined lines are unparseable: if they
    /// were enumerated they would be re-counted as malformed on every read and `is_complete` would
    /// be false forever, which is the deadlock the quarantine exists to avoid.
    fn torn_path_for(ledger_path: &Path) -> PathBuf {
        let mut name = ledger_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        name.push_str(".torn");
        ledger_path.with_file_name(name)
    }

    /// Append torn lines to the quarantine file rather than discarding them at compaction.
    ///
    /// # Loss mode L5, and why quarantine rather than either obvious alternative
    ///
    /// A torn line is a **reported** incompleteness: `LedgerReadOutcome::malformed_lines` makes
    /// `is_complete()` false, so the staged fold retains the previous figure instead of installing
    /// one it knows is short, and the repair rotation refuses the pass. Both are the safe direction.
    /// Compaction then rebuilt the file from the parsed entries only, so the next read saw a clean
    /// file and the fold installed a figure permanently missing those bytes. **A reported
    /// incompleteness became silent permanent loss**, biasing the eviction trigger late.
    ///
    /// Two alternatives were considered and both are worse:
    ///
    /// - **Carry the lines through into the rewritten ledger.** The report then persists — and so
    ///   does `is_complete() == false`, forever, because nothing can ever parse that line. The fold
    ///   would retain on every pass and the repair rotation would refuse on every pass, which
    ///   deadlocks the very rotation that could restore the lost coverage. Loud, and permanently
    ///   stuck.
    /// - **Refuse to compact a file with a torn line.** Same deadlock, and it additionally stops the
    ///   file being compacted at all, so it grows without bound.
    ///
    /// Quarantine keeps the evidence and clears the deadlock: the ledger file becomes parseable, so
    /// the fold installs and — this is the part that makes the loss recoverable rather than merely
    /// visible — `ledger_repair`'s rotation can run, and it matches on
    /// `(cache_key, range_start, range_end)` read from the `.meta` files on disk. A staged range
    /// whose entry the torn line destroyed is found there and re-appended. A peer whose
    /// identity is not shared can do that, but a pass refused on a shared identity keeps its
    /// claim on the shard, so the entry can stay unindexed until the hostnames are made
    /// distinct ([`Self::append`]'s second rule). So the line
    /// **survives** compaction and its accounting consequence is **repaired afterwards**, which
    /// needs the clean read quarantine restores.
    ///
    /// It is not silent: [`torn_lines_quarantined_total`] counts every line and the caller WARNs
    /// with the quarantine path.
    ///
    /// Appends rather than truncates, so a second compaction does not erase the first one's
    /// evidence. The caller holds the ledger file's `fs4` lock, which is also what serialises this.
    fn quarantine_torn_lines(ledger_path: &Path, lines: &[String]) -> std::io::Result<()> {
        use std::io::Write;
        let torn_path = Self::torn_path_for(ledger_path);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&torn_path)?;
        for line in lines {
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        TORN_LINES_QUARANTINED_TOTAL.fetch_add(lines.len() as u64, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Parse ledger file content, returning the entries and how many lines were
    /// discarded.
    ///
    /// A malformed line is never fatal, matching every journal parse site. On a mixed
    /// version fleet that is also what keeps an unknown future field from breaking an
    /// older instance's read.
    ///
    /// **One parser, two readers.** The hint reader throws the discard count away and
    /// keeps that skip-not-fatal semantics; the authority reader carries the count in
    /// [`LedgerReadOutcome`] so an installer can refuse. Sharing the parser is what stops
    /// the two from ever disagreeing about what a line means.
    fn parse_content(path: &Path, content: &str) -> ParsedLedgerFile {
        let mut parsed = ParsedLedgerFile::default();
        for line in content.lines() {
            if line.trim().is_empty() {
                // Counted, not silently skipped: a blank line in an append-only JSONL
                // file is what a torn write looks like once the partial bytes are gone.
                parsed.malformed += 1;
                debug!("Skipping blank write-ledger line: file={:?}", path);
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<WriteLedgerEntry>(line) {
                parsed.entries.push(entry);
                continue;
            }
            // Not an entry. Before calling it malformed, try the header — a header line is
            // a legitimate part of a schema-2 file, and counting it as a discarded line
            // would make `LedgerReadOutcome::is_complete` permanently false and so make the
            // fold refuse to install forever. `LedgerHeader`'s two fields are both
            // required and an entry's are all absent from it, so the two shapes cannot be
            // confused in either direction.
            //
            // Tried on every line rather than only the first: compaction writes it first,
            // but a file that somehow carries it elsewhere should still be read rather
            // than declared torn.
            match serde_json::from_str::<LedgerHeader>(line) {
                Ok(header) => {
                    let seen = parsed.header_max_seq_ever.get_or_insert(0);
                    if header.max_seq_ever > *seen {
                        *seen = header.max_seq_ever;
                    }
                }
                Err(e) => {
                    parsed.malformed += 1;
                    // Kept verbatim so compaction can carry it forward rather than erase it. See
                    // `ParsedLedgerFile::malformed_lines_text`.
                    parsed.malformed_lines_text.push(line.to_string());
                    debug!(
                        "Skipping unparseable write-ledger line: file={:?}, error={}",
                        path, e
                    );
                }
            }
        }
        parsed
    }

    /// The hint read of one ledger file: an unopenable file is an empty one.
    ///
    /// Callers that need to tell those apart must use [`Self::read_for_authority`].
    async fn read_file(path: &Path) -> Vec<WriteLedgerEntry> {
        match tokio::fs::read_to_string(path).await {
            Ok(content) => Self::parse_content(path, &content).entries,
            Err(e) => {
                debug!("Failed to read write-ledger file {:?}: {}", path, e);
                Vec::new()
            }
        }
    }

    /// Blocking read of one ledger file, for use with the file's `fs4` lock already held.
    ///
    /// Blocking rather than async because the only two callers run inside
    /// `spawn_blocking` closures that hold the lock — an `.await` there would either be
    /// impossible or would park a blocking thread while holding a cross-instance lock.
    fn read_file_locked(path: &Path) -> std::io::Result<ParsedLedgerFile> {
        let content = std::fs::read_to_string(path)?;
        Ok(Self::parse_content(path, &content))
    }

    /// Oldest-first ordering over ledger entries: insertion order across the fleet, with
    /// a stable tiebreak so two entries stamped in the same instant order the same way on
    /// every instance rather than by filesystem order.
    ///
    /// **A legitimate use of `timestamp`, and not the ordering coordinate.** This is the
    /// eviction candidate order (R2.3), which is a question about wall-clock age across
    /// instances — a per-instance sequence number could not answer it, since two instances'
    /// sequences are incomparable. A clock stepping backwards misorders candidates, which
    /// costs eviction fidelity and nothing else. The `Ledger_Seq` cut coordinate is
    /// [`WriteLedgerEntry::seq`] and is deliberately not this.
    fn oldest_first(a: &WriteLedgerEntry, b: &WriteLedgerEntry) -> std::cmp::Ordering {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| a.cache_key.cmp(&b.cache_key))
            .then_with(|| a.range_start.cmp(&b.range_start))
    }

    /// All entries from every instance, merged and sorted **oldest first**.
    ///
    /// This is the candidate order (R2.3): insertion order across the fleet. The sort is
    /// what "merging the per-instance heads" amounts to when the files are small enough
    /// to read whole, which they are — the ledger is O(staged), bounded by
    /// `write_cache_percent` of the cache.
    ///
    /// `cap` bounds how many entries are returned after sorting, so a pathologically
    /// long ledger cannot make one eviction pass unbounded. `0` means no cap.
    ///
    /// # This is the HINT reader
    ///
    /// It takes no lock and reports no completeness: an unopenable file contributes
    /// nothing, a discarded line is invisible, and hitting `cap` is not signalled. That
    /// is the right trade for eviction candidate selection, where every entry is a hint
    /// verified against its `.meta` before anything is deleted, and where refusing to act
    /// because one file was briefly unreadable would be worse than acting on a shorter
    /// list. A caller that needs to *install* something derived from the ledger must use
    /// [`Self::read_for_authority`] instead.
    ///
    /// Test-only since the compaction pass moved to a shard rotation and reads one unit at a
    /// time through [`Self::read_files_hint`]; the tests use it as a whole-volume view.
    #[cfg(test)]
    pub(crate) async fn read_merged_oldest_first(
        &self,
        cap: usize,
    ) -> Result<Vec<WriteLedgerEntry>> {
        let files = self.all_ledger_files().await?;
        let mut all = Vec::new();
        for file in &files {
            all.extend(Self::read_file(file).await);
        }

        all.sort_by(Self::oldest_first);

        if cap > 0 && all.len() > cap {
            all.truncate(cap);
        }
        Ok(all)
    }

    /// The AUTHORITY read: every file under its own `fs4` lock, with completeness
    /// reported rather than discarded.
    ///
    /// Differs from the hint reader ([`Self::read_files_hint`]) in exactly two ways, and both are
    /// the point:
    ///
    /// - each file is read while holding the same lock appends and compaction take, so
    ///   the content cannot be a partial view of an append in flight; and
    /// - what the read could not see survives the return in [`LedgerReadOutcome`], so
    ///   "complete and empty" and "failed and therefore empty" are different values.
    ///
    /// `cap` behaves as it does for the hint reader, except that hitting it sets
    /// [`LedgerReadOutcome::truncated`]. `max_seq_per_instance` is observed over
    /// everything read, before truncation.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.3.
    pub async fn read_for_authority(&self, cap: usize) -> Result<LedgerReadOutcome> {
        let files = self.shard_ledger_files().await?;
        let legacy = self.legacy_ledger_files().await?;
        if !legacy.is_empty() {
            warn!(
                legacy_files = legacy.len(),
                "Write-ledger authority read found unconverted legacy flat ledger file(s). \
                 Their entries name no shard, so a figure installed now would be short by \
                 whatever they hold. The caller must retain rather than install."
            );
        }
        let mut outcome = Self::read_files_for_authority(files, cap).await;
        outcome.legacy_files_present = legacy.len();
        Ok(outcome)
    }

    /// The AUTHORITY read for **one shard**: `{bucket}/{l1}/*.ledger` and nothing else.
    ///
    /// # Why this exists beside [`Self::read_for_authority`] rather than replacing it
    ///
    /// A fold pass visits a bounded number of shards, so reading every shard's ledger file
    /// before the visit loop made the pass's cost `O(shards existing)` rather than
    /// `O(shards visited)` — and that term is what bounds `Staged_Range_Budget`
    /// (§ 16.6.12 point 2, `dr-scan-measurements.md` § 3(f)). This reader lists one shard
    /// directory and opens only the files in it: one `readdir` plus one file per instance
    /// that has ever credited that shard.
    ///
    /// The whole-volume reader stays for **one** consumer that genuinely needs the whole volume:
    /// staging eviction's candidate merge, which ranks candidates against each other in
    /// fleet-wide insertion order and so cannot be assembled a shard at a time. It runs on a
    /// trigger rather than per pass. The ledger-completeness repair rotation used to be the
    /// second such consumer and now calls **this** reader instead, once per shard it visits —
    /// see `ledger_repair`'s "Complete-or-refuse" section for why its scope could narrow and
    /// what checks the equivalence that makes it safe.
    ///
    /// # What this read's completeness means, and what it deliberately cannot see
    ///
    /// [`LedgerReadOutcome::is_complete`] is a statement about the files the read was **asked
    /// to cover**, and this read was asked to cover one shard. So a complete read here means
    /// "every ledger file in this shard opened, no line discarded, no truncation" — which is
    /// exactly the precondition for installing *this shard's* figure, and says nothing about
    /// any other shard. That is the whole point: one shard's unreadable ledger file used to
    /// retain the entire pass.
    ///
    /// [`LedgerReadOutcome::legacy_files_present`] is left at **0** here, and that is not an
    /// omission. A legacy flat `{instance}.ledger` sits at the ledger root and names no shard,
    /// so it cannot be attributed to this shard or to any other — there is no shard-scoped
    /// answer to give. Its presence is a property of the volume and the caller checks it once
    /// per pass; see `staged_fold::run_staged_fold_with_cap`.
    ///
    /// `cap` behaves as it does for the whole-volume reader, applied to **this shard's**
    /// entries.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.3, § 16.6.12 point 2.
    pub(crate) async fn read_shard_for_authority(
        &self,
        bucket: &str,
        l1: &str,
        cap: usize,
    ) -> Result<LedgerReadOutcome> {
        let files = Self::ledger_files_in(&Self::shard_dir(&self.cache_dir, bucket, l1))?;
        #[allow(unused_mut)]
        let mut outcome = Self::read_files_for_authority(files, cap).await;
        #[cfg(test)]
        self.sync_probe().fault_authority_read(&mut outcome);
        Ok(outcome)
    }

    /// The per-file half of an authority read, shared by the whole-volume and the per-shard
    /// reader.
    ///
    /// Shared rather than duplicated because every field it sets is a completeness claim, and
    /// two copies of "what counts as unreadable" would eventually disagree — at which point a
    /// shard-scoped read and a whole-volume read of the same file could reach different
    /// verdicts about it. `files_expected` is the caller's listing length, so the scope of the
    /// read is the scope of its completeness.
    async fn read_files_for_authority(files: Vec<PathBuf>, cap: usize) -> LedgerReadOutcome {
        let mut outcome = LedgerReadOutcome {
            files_expected: files.len(),
            ..Default::default()
        };

        for path in files {
            let lock_path = Self::lock_path_for(&path);
            let path_for_task = path.clone();
            let instance = Self::instance_of(&path);
            let read = tokio::task::spawn_blocking(move || -> std::io::Result<ParsedLedgerFile> {
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_path)?;
                fs4::FileExt::lock(&lock_file)?;
                let result = Self::read_file_locked(&path_for_task);
                let _ = fs4::FileExt::unlock(&lock_file);
                result
            })
            .await;

            match read {
                Ok(Ok(parsed)) => {
                    outcome.files_read += 1;
                    outcome.malformed_lines += parsed.malformed;
                    // Observed per FILE, from `max(header, highest entry seq)`, rather than
                    // per entry. Per-entry observation cannot see the header, so a
                    // compaction that retired the top of the file would lower the observed
                    // maximum and let an already-folded credit fall back above the cut.
                    if let Some(max_seq) = parsed.max_seq_ever() {
                        let slot = outcome
                            .max_seq_per_instance
                            .entry(instance.clone())
                            .or_insert(0);
                        if max_seq > *slot {
                            *slot = max_seq;
                        }
                    }
                    outcome.entries.extend(parsed.entries);
                }
                Ok(Err(e)) => {
                    outcome.files_unreadable += 1;
                    warn!(
                        "Write-ledger authority read could not read {:?}: {}. \
                         This read is incomplete and must not be installed as a figure.",
                        path, e
                    );
                }
                Err(e) => {
                    outcome.files_unreadable += 1;
                    warn!(
                        "Write-ledger authority read task failed for {:?}: {}. \
                         This read is incomplete and must not be installed as a figure.",
                        path, e
                    );
                }
            }
        }

        outcome.entries.sort_by(Self::oldest_first);
        if cap > 0 && outcome.entries.len() > cap {
            outcome.entries.truncate(cap);
            outcome.truncated = true;
        }
        outcome
    }

    /// Total entries across every instance file. Backs the `ledger_entries` gauge
    /// (R8.3), which is what makes "the staging tier is not draining" visible.
    ///
    /// # `None` is the UNREADABLE sentinel, and it is not a stylistic preference
    ///
    /// This used to return `u64` and produce `0` both from a directory-listing failure
    /// and from every file being unopenable. `0` is also the healthy drained value, so
    /// every failure pushed the gauge toward the reassuring end: a shared volume that had
    /// gone unreadable published the same number as a staging tier that had fully
    /// graduated. An operator cannot tell those apart, and neither can an alarm.
    ///
    /// `None` serialises as JSON `null`, which is distinguishable from `0` by a
    /// dashboard, by a `python3` parse, and by eye.
    ///
    /// A file that is listed and then read successfully but holds a malformed line still
    /// contributes its parseable entries: this is a length gauge, not the authority read,
    /// and `LedgerReadOutcome::malformed_lines` is where that class is expressed.
    ///
    /// # It is an observation of bounded age, NOT a scrape-time count
    ///
    /// The whole-volume traversal this needs is `1 + B + (B × 256)` listings and up to
    /// `B × 256 × instances` file reads on shared storage, and it used to run on every scrape.
    /// It now runs at most once per [`LEDGER_COUNT_MAX_AGE_SECS`], and usually not at all,
    /// because [`Self::retire_identities`] donates the figure it already computes. The age of
    /// whatever is reported is published beside it as `write_cache.ledger_entries_age_secs` —
    /// see [`Self::entry_count_age_secs`] — so bounded staleness is visible rather than silent.
    ///
    /// **A running count cannot replace this**, and that is a property of the volume rather than
    /// of the instrumentation. Four of the paths that mutate the counted set are in this process
    /// (`append`, `retire_identities`, `migrate_legacy_ledgers`, and legacy-file removal) and
    /// **five are in other processes**: a peer's append, a peer's compaction — which rewrites
    /// *this* instance's files, by design, under the global consolidation lock — a peer's legacy
    /// conversion, a peer's file removal between our listing and our read, and out-of-band volume
    /// state. The figure is fleet-wide, so a per-process counter would drift silently and
    /// permanently on every multi-instance deployment.
    ///
    /// **A count in the file header cannot either.** The header is written at file creation (as
    /// `max_seq_ever: 0`) and by compaction, and is deliberately stale in between — see
    /// [`LedgerHeader`]. `max_seq_ever` survives that because a reader takes
    /// `max(header, entries)`; a count has no such second term short of counting the entries,
    /// which is the traversal. Its staleness would run in the **under**-count direction, which
    /// for a length gauge is the reassuring one, and making it correct would mean a
    /// read-modify-write of a file as long as the staged set on every append — exactly the
    /// O(staged)-per-PUT shape this module exists to avoid.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.3.
    pub async fn count_entries(&self) -> Option<u64> {
        let mut slot = self.entry_count.lock().await;
        if let Some(observation) = *slot {
            if observation.observed_at.elapsed().as_secs() < LEDGER_COUNT_MAX_AGE_SECS {
                return Some(observation.count);
            }
        }
        // A failed traversal deliberately leaves any previous observation in place rather than
        // overwriting it with a failure. It cannot be served — control only reaches here when it
        // was already absent or already past the max age — so this is not a route to reporting a
        // stale figure; it just means the next call re-attempts rather than latching.
        let counted = self.count_entries_by_traversal().await?;
        *slot = Some(LedgerCountObservation {
            count: counted,
            observed_at: std::time::Instant::now(),
        });
        Some(counted)
    }

    /// How many seconds ago the currently-reported entry count was observed, or `None` if no
    /// successful observation has been taken yet.
    ///
    /// Published as `write_cache.ledger_entries_age_secs`. **Read it after `count_entries`**, not
    /// before: that call may take a fresh observation, and an age read first would describe the
    /// figure that was replaced.
    ///
    /// `None` here and `None` for `ledger_entries` mean the same thing and arrive together — no
    /// observation exists. They are published as two fields rather than one because a *stale*
    /// observation has an age and a readable count, and that is the state an operator most needs
    /// to be able to name.
    pub(crate) async fn entry_count_age_secs(&self) -> Option<u64> {
        self.entry_count
            .lock()
            .await
            .map(|observation| observation.observed_at.elapsed().as_secs())
    }

    /// Record a whole-volume count observed by a caller that had already traversed the volume.
    ///
    /// The donor is [`Self::retire_identities`], whose read is strictly **better** than the
    /// traversal in [`Self::count_entries_by_traversal`]: it holds each file's `fs4` lock across
    /// its read, so no file can be a partial view of an append in flight, where the count's own
    /// traversal takes no lock at all.
    ///
    /// Only ever called with a count the donor knows covers the whole volume. A pass that could
    /// not read a file donates nothing, so a short figure cannot be installed as a real one.
    async fn record_entry_count(&self, count: u64) {
        *self.entry_count.lock().await = Some(LedgerCountObservation {
            count,
            observed_at: std::time::Instant::now(),
        });
    }

    /// Test-only: install an entry-count observation of a chosen age, so the
    /// [`LEDGER_COUNT_MAX_AGE_SECS`] branch in [`Self::count_entries`] can be exercised on both
    /// sides without a five-minute wait.
    ///
    /// `expect` rather than a silent skip when the monotonic clock's origin is more recent than
    /// `age`: that is an apparatus failure, and a test that skipped on it would pass while
    /// asserting nothing — the class this repository has five recorded instances of.
    #[cfg(test)]
    async fn install_entry_count_observation_aged(&self, count: u64, age: std::time::Duration) {
        let observed_at = std::time::Instant::now()
            .checked_sub(age)
            .expect("monotonic clock origin is more recent than the age this test needs");
        *self.entry_count.lock().await = Some(LedgerCountObservation { count, observed_at });
    }

    /// The whole-volume traversal behind [`Self::count_entries`], unchanged from when that
    /// function was the traversal.
    ///
    /// Split out so the sentinel discipline lives in one place and the memo in another. Every
    /// state this returns is load-bearing and none of them may be collapsed:
    ///
    /// - `None` — the listing failed, or a file could not be read. UNREADABLE.
    /// - `Some(0)` — the volume was fully readable and holds no entries. Drained.
    /// - `Some(n)` — a real count.
    ///
    /// A file that vanished between the listing and the read is a real zero **for that file**,
    /// because it was compacted away. A file that reads but holds a malformed line still
    /// contributes its parseable entries, because this is a length gauge and not the authority
    /// read — `LedgerReadOutcome::malformed_lines` is where that class is expressed.
    async fn count_entries_by_traversal(&self) -> Option<u64> {
        let files = match self.all_ledger_files().await {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    "Write-ledger count: could not list ledger files: {}. Reporting \
                     UNREADABLE rather than 0, which is also the drained value.",
                    e
                );
                return None;
            }
        };
        let mut total = 0u64;
        for file in &files {
            match tokio::fs::read_to_string(file).await {
                Ok(content) => {
                    total += Self::parse_content(file, &content).entries.len() as u64;
                }
                // A file that vanished between the listing and the read has been
                // compacted away, which is a real zero for that file rather than a
                // failure to observe it.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(
                        "Write-ledger count: could not read {:?}: {}. Reporting UNREADABLE \
                         rather than a short count.",
                        file, e
                    );
                    return None;
                }
            }
        }
        Some(total)
    }

    /// The set of `cache_key`s currently present in any ledger file.
    ///
    /// Used by the Validation_Scan re-append (R2.7 / R6.6) to find staged objects that
    /// no ledger knows about.
    pub(crate) async fn staged_keys(&self) -> Result<HashSet<String>> {
        let files = self.all_ledger_files().await?;
        let mut keys = HashSet::new();
        for file in &files {
            for entry in Self::read_file(file).await {
                keys.insert(entry.cache_key);
            }
        }
        Ok(keys)
    }

    /// Remove exactly the entries named in `retire` from every ledger file, leaving
    /// everything else untouched.
    ///
    /// # Why this names what to REMOVE rather than what to keep
    ///
    /// This replaced a `rewrite_retaining(retain)` that deleted every entry absent from
    /// the caller's set, which made the caller's read completeness load-bearing — and
    /// task 77 is what happens when it is not complete. `evict_staging_tier_locked`
    /// reads at most `STAGING_EVICTION_CANDIDATE_CAP` (10,000) entries and built its set
    /// from that truncated vector, so on a ledger longer than the cap every unread entry
    /// was silently deleted. The Write_Ledger is the only index driving staging
    /// eviction, and since Phase F `reserve_for_sizing` no longer refuses on capacity,
    /// so losing entries means losing the ability to reclaim the objects they name.
    ///
    /// A retire-set cannot express that failure. An entry the caller never read is not
    /// in the set, so it is not removed — the safe answer arrives by construction rather
    /// than by the caller having read everything.
    ///
    /// # What the inversion did NOT fix, and what does
    ///
    /// This comment used to claim the inversion "closes the read-then-rewrite window".
    /// It does not, and the distinction is worth stating precisely because the two holes
    /// present identically as "an entry this pass never saw was deleted":
    ///
    /// - the **cap-shaped** hole is about entries beyond the caller's read limit, and the
    ///   inversion does close it: an entry that is not named is not removed;
    /// - the **time-shaped** hole is about this function's *own* read. Naming removals
    ///   changed which entries are marked for deletion; it did not make the rewrite
    ///   incremental. The rewrite is still a whole-file replace, so it can only retain
    ///   entries its own read observed, and an append committing after that read was
    ///   erased by the rename — silently, and without appearing in
    ///   [`LedgerCompactionStats::entries_dropped`].
    ///
    /// The time-shaped hole is closed by lock scope, not by set polarity: the read, the
    /// keep/drop filter, the temp write and the rename all happen inside a single hold of
    /// the file's `fs4` lock — the same lock an append takes — so an append is serialised
    /// either wholly before the read or wholly after the rename, and is never lost. The
    /// stats are likewise computed from what was read under that lock.
    ///
    /// A read error inside the lock therefore aborts that file, leaving it untouched. It
    /// used to produce zero entries, hence an empty keep set, hence deletion of a file
    /// whose contents had never been read — reported as a successful compaction.
    ///
    /// # Fleet-wide by design
    ///
    /// Rewrites **all** instances' files, not just this one's: a decommissioned
    /// instance's file would otherwise accumulate forever with nobody to tidy it. Each
    /// file is rewritten under its own append lock.
    ///
    /// The previous version of this comment justified that with "the only caller runs
    /// under the global consolidation lock, so exactly one instance fleet-wide is
    /// compacting at a time". That has not been true since Phase E: there are two
    /// callers holding two **different** lock files — `evict_staging_tier_locked` under
    /// `global_eviction.lock` and `maybe_compact_write_ledger` under
    /// `global_consolidation.lock` — so an eviction pass and a compaction pass can
    /// interleave. Two things make that interleaving benign. The per-file lock now spans
    /// each pass's whole read-decide-rename sequence, so the two cannot interleave within
    /// one file at all; and naming removals means the worst case across files is that an
    /// entry one pass already removed is named again by the other, which is a no-op.
    ///
    /// Each pass's *caller-side* window — between the caller's own read and its call to
    /// this function — is a separate and wider thing that lock scope here cannot address.
    /// The retire-set inversion is what makes that one safe, and it genuinely does.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 2.6, 3.1
    pub async fn retire_identities(
        &self,
        retire: &HashSet<(String, u64, u64, SystemTime, String)>,
    ) -> Result<LedgerCompactionStats> {
        // Legacy flat files included: a decommissioned pre-upgrade instance's file has nobody
        // else to tidy it, and retiring a terminal entry there is one fewer entry the
        // conversion has to carry.
        let started = std::time::Instant::now();
        let files = self.all_ledger_files().await?;
        let (stats, files_unreadable) = Self::retire_in_files(files, retire).await;

        // Donate the figure to the `ledger_entries` gauge, so the scrape path does not have to
        // traverse the volume for it. This pass has just read every ledger file on the volume
        // under its own lock, which is a STRICTLY better read than the gauge's own unlocked
        // traversal, and it runs off the request path on the compaction interval.
        //
        // `entries_after`, not `entries_before`: the gauge is the ledger's length now, and this
        // pass may have shortened it.
        //
        // Nothing is donated when a file could not be read, which makes a donation-path failure
        // indistinguishable from no donation at all — deliberately. The gauge then falls back to
        // its own traversal, which reaches the same files and publishes the UNREADABLE sentinel
        // from there. Two routes to `None` would be two places for that discipline to drift.
        if files_unreadable == 0 {
            self.record_entry_count(stats.entries_after).await;
        }

        // `elapsed_ms` and `unchanged` are here because neither was observable before, and the
        // cost this pass carries is `O(files on the volume)` locked reads whatever it drops. A
        // fleet checkpoint could size the rewrite count from these logs and could not size the
        // wall clock from anything at all, so a compaction getting slower as the shard population
        // grows toward the design point was invisible in production.
        //
        // `debug!` on the no-drop path rather than nothing, so a pass that reclaims nothing still
        // reports what it cost. It runs once per `LEDGER_COMPACTION_INTERVAL_SECS`, so this is not
        // a hot log line even at DEBUG.
        if stats.entries_dropped() > 0 {
            info!(
                "Write-ledger compacted: before={}, after={}, dropped={}, rewritten={}, \
                 unchanged={}, removed={}, files_read={}, elapsed_ms={}",
                stats.entries_before,
                stats.entries_after,
                stats.entries_dropped(),
                stats.files_rewritten,
                stats.files_unchanged,
                stats.files_removed,
                stats.files_rewritten + stats.files_unchanged + stats.files_removed,
                started.elapsed().as_millis()
            );
        } else {
            debug!(
                "Write-ledger compaction dropped nothing: files_read={}, unchanged={}, \
                 rewritten={}, removed={}, elapsed_ms={}",
                stats.files_rewritten + stats.files_unchanged + stats.files_removed,
                stats.files_unchanged,
                stats.files_rewritten,
                stats.files_removed,
                started.elapsed().as_millis()
            );
        }
        Ok(stats)
    }

    /// [`Self::retire_identities`] over a caller-chosen set of ledger files.
    ///
    /// The per-file guarantees are the same ones `retire_identities` documents, because the
    /// per-file loop is shared: each file is read, filtered, written and renamed inside one hold
    /// of its own `fs4` lock, and only named identities are removed. What differs is scope. An
    /// identity named here that lives in a file not in `files` is simply not retired this time,
    /// which is the safe direction: an entry is only ever kept too long, never removed unread.
    ///
    /// Exists for the consolidation cycle's compaction pass, which visits a bounded number of
    /// shards per pass and so must not pay for every ledger file on the volume to retire what
    /// it assessed in those shards. Neither logs nor donates the `ledger_entries` figure: a
    /// partial read is not the ledger's length.
    pub(crate) async fn retire_identities_in(
        &self,
        files: Vec<PathBuf>,
        retire: &HashSet<(String, u64, u64, SystemTime, String)>,
    ) -> LedgerCompactionStats {
        Self::retire_in_files(files, retire).await.0
    }

    /// One compaction unit's files: `{bucket}/{l1}/*.ledger`.
    pub(crate) fn shard_ledger_files_of(&self, bucket: &str, l1: &str) -> Result<Vec<PathBuf>> {
        Self::ledger_files_in(&Self::shard_dir(&self.cache_dir, bucket, l1))
    }
    /// Remove the header-only per-shard files `{bucket}/{l1}/{instance}.ledger`, with their
    /// `.ledger.lock`, of the ids `is_retired` names given each file's mtime. Returns how many
    /// `.ledger` files were removed. Blocking `std::fs`, for the dead-instance reaper.
    ///
    /// A file is removed only when, read under its lock (the one every append and compaction
    /// takes), it holds a header and nothing else. The header is all such a file carries, and
    /// nothing needs it once its id is retired and has no pending credit left, which the caller's
    /// `is_retired` establishes:
    ///
    /// - The sequence floor is the root `{instance}.ledger.mark`, which this never touches:
    ///   [`AppenderCore::seed_sequence`] reads the mark and the legacy flat file, never a shard file.
    /// - The header feeds the shard's `Fold_Cut` for the id, and a cut only filters that id's
    ///   pending credits. With none left, and none to come from a retired id, no cut decides
    ///   anything. A process that later reuses the id issues sequence numbers above its mark, so
    ///   above every cut the id ever justified.
    ///
    /// The lock file is unlinked after the ledger file while still held: a compaction waiting on
    /// the old inode then reads the ledger file as absent and does nothing. Root-level files and
    /// `.ledger.torn` quarantines are left alone.
    pub(crate) fn reap_retired_header_only_files(
        cache_dir: &Path,
        is_retired: &dyn Fn(&str, SystemTime) -> bool,
    ) -> u64 {
        let root = cache_dir.join("metadata").join(LEDGER_DIR);
        let mut removed = 0u64;
        let Ok(buckets) = Self::subdirs_of(&root) else {
            return 0;
        };
        for bucket_dir in buckets {
            let Ok(l1_dirs) = Self::subdirs_of(&bucket_dir) else {
                continue;
            };
            for l1_dir in l1_dirs {
                let Ok(files) = Self::ledger_files_in(&l1_dir) else {
                    continue;
                };
                for path in files {
                    let Ok(modified) = std::fs::metadata(&path).and_then(|m| m.modified()) else {
                        continue;
                    };
                    if !is_retired(&Self::instance_of(&path), modified) {
                        continue;
                    }
                    match Self::remove_if_header_only(&path) {
                        Ok(true) => removed += 1,
                        Ok(false) => {}
                        Err(e) => warn!(
                            "Failed to reap a retired instance's write-ledger file {:?}: {}",
                            path, e
                        ),
                    }
                }
            }
        }
        removed
    }
    /// Remove `path` and its lock file if, read under the lock, it is a header and nothing else.
    fn remove_if_header_only(path: &Path) -> std::io::Result<bool> {
        let lock_path = Self::lock_path_for(path);
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        fs4::FileExt::lock(&lock_file)?;
        let result = (|| -> std::io::Result<bool> {
            let content = match std::fs::read_to_string(path) {
                Ok(c) => c,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(e) => return Err(e),
            };
            let parsed = Self::parse_content(path, &content);
            if !parsed.entries.is_empty()
                || parsed.malformed > 0
                || parsed.header_max_seq_ever.is_none()
            {
                return Ok(false);
            }
            std::fs::remove_file(path)?;
            let _ = std::fs::remove_file(&lock_path);
            Ok(true)
        })();
        let _ = fs4::FileExt::unlock(&lock_file);
        result
    }

    /// Every legacy flat `{instance}.ledger` at the ledger root.
    pub(crate) async fn legacy_ledger_files_list(&self) -> Result<Vec<PathBuf>> {
        self.legacy_ledger_files().await
    }

    /// The hint read over a caller-chosen set of files: no lock and no completeness, so an
    /// unopenable file contributes nothing and a discarded line is invisible.
    pub(crate) async fn read_files_hint(files: &[PathBuf]) -> Vec<WriteLedgerEntry> {
        let mut all = Vec::new();
        for file in files {
            all.extend(Self::read_file(file).await);
        }
        all
    }

    /// The per-file loop shared by [`Self::retire_identities`] and
    /// [`Self::retire_identities_in`]. Returns the stats and how many files could not be read.
    async fn retire_in_files(
        files: Vec<PathBuf>,
        retire: &HashSet<(String, u64, u64, SystemTime, String)>,
    ) -> (LedgerCompactionStats, usize) {
        let mut stats = LedgerCompactionStats::default();
        // Whether this pass saw the whole volume. Only a pass that read every file it listed may
        // donate its figure to the `ledger_entries` gauge — `stats.entries_after` sums the files
        // it succeeded on, so donating after a failure would install a SHORT count as a real one,
        // and short is the reassuring direction for a length gauge. A file that was NotFound is
        // deliberately NOT counted here: it was compacted away between the listing and the lock,
        // which is a real zero for that file and the same carve-out `count_entries_by_traversal`
        // makes. Local to this function rather than a `LedgerCompactionStats` field because
        // nothing else needs it and a published field invites someone to read it as a health
        // figure, which it is not.
        let mut files_unreadable = 0usize;
        // Shared into each blocking closure. Cloned once rather than per file, since the
        // set is the same for every file and can hold one identity per staged range.
        let retire = std::sync::Arc::new(retire.clone());

        for path in files {
            let lock_path = Self::lock_path_for(&path);
            let path_for_task = path.clone();
            let retire = std::sync::Arc::clone(&retire);
            let rewrite =
                tokio::task::spawn_blocking(move || -> std::io::Result<LedgerFileRewrite> {
                    use std::io::Write;

                    let lock_file = std::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    fs4::FileExt::lock(&lock_file)?;

                    let result = (|| -> std::io::Result<LedgerFileRewrite> {
                        // INSIDE the lock. An append takes this same lock, so it commits
                        // either wholly before this read or wholly after the rename
                        // below; there is no interval in which it can commit a line this
                        // read cannot see and the rename then discards.
                        let parsed = match std::fs::read_to_string(&path_for_task) {
                            Ok(content) => Self::parse_content(&path_for_task, &content),
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // Removed by another pass between the listing and the
                                // lock. Nothing to compact and nothing to report.
                                return Ok(LedgerFileRewrite::default());
                            }
                            // Any other read error aborts this file untouched.
                            // Proceeding would compute an empty keep set from a file
                            // that was never read and delete it.
                            Err(e) => return Err(e),
                        };
                        // Computed BEFORE anything is dropped, so retiring the highest-seq
                        // entry cannot lower the file's high-water mark.
                        let max_seq_ever = parsed.max_seq_ever();
                        let malformed_lines = parsed.malformed;
                        // **Loss mode L5.** Quarantine BEFORE the rewrite, and fail the whole
                        // rewrite if the quarantine write fails, so the destructive step cannot
                        // happen without the evidence having been preserved first. Ordering is the
                        // whole guarantee here: a rewrite-then-quarantine would lose the lines on
                        // any error in between, which is the state this fix exists to remove.
                        if !parsed.malformed_lines_text.is_empty() {
                            Self::quarantine_torn_lines(
                                &path_for_task,
                                &parsed.malformed_lines_text,
                            )?;
                        }
                        let entries = parsed.entries;
                        let entries_before = entries.len() as u64;
                        let kept: Vec<WriteLedgerEntry> = entries
                            .into_iter()
                            .filter(|e| !retire.contains(&e.identity()))
                            .collect();
                        let entries_after = kept.len() as u64;

                        if kept.is_empty() && max_seq_ever.is_none() {
                            // Remove rather than truncate. Unlike the primary journal,
                            // which is truncated so appends keep a stable inode, a ledger
                            // file is recreated by the next append anyway, and removing it
                            // keeps `ledger_files()` free of empties.
                            //
                            // ONLY when there is no high-water mark to preserve, i.e. a
                            // legacy file with no header and no seq-bearing entry. Removing
                            // a file that *had* one would discard it, and a later process
                            // reusing this instance identity would then seed its sequence
                            // from 1 — re-issuing numbers an installed cut has already
                            // passed, which puts those credits in neither the fold nor the
                            // pending term. A drained schema-2 file becomes a header-only
                            // file instead, which is one line of disk for a guarantee.
                            match std::fs::remove_file(&path_for_task) {
                                Ok(()) => {}
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => return Err(e),
                            }
                            return Ok(LedgerFileRewrite {
                                entries_before,
                                entries_after,
                                removed: true,
                                rewritten: false,
                                unchanged: false,
                                malformed_lines,
                            });
                        }

                        // **Nothing was dropped and nothing needs quarantining, so nothing is
                        // written.**
                        //
                        // This is the whole of the compaction scope decision, and it is a
                        // no-semantic-change one by construction rather than by argument. The read
                        // is still whole-volume and still under each file's lock, so no entry can
                        // be missed and the removal-set polarity is untouched — what goes away is
                        // only the write: a temp create, a `write_all`, an `fsync` and a `rename`
                        // per file, per interval, over NFS. A fleet measurement put that at 134
                        // locked rewrites to drop 2 entries, rising with the file population while
                        // the entries dropped fell — a cost tracking how many files exist rather
                        // than how much work there is, which is the wrong shape at the design
                        // point.
                        //
                        // # Why the HEADER does not have to be current for this to be safe
                        //
                        // A file's on-disk header is normally STALE, by design: `append` writes
                        // `max_seq_ever: 0` when it creates a file and never raises it, because
                        // `ParsedLedgerFile::max_seq_ever` takes the max against the entries, so
                        // the entries stand until a compaction raises it. Requiring the header to
                        // already be current would therefore have skipped almost nothing — every
                        // file with an un-compacted append would still be rewritten, which is
                        // precisely the population the fleet measurement counted. An earlier draft
                        // of this gate did require it, and a byte-comparison test is what caught
                        // that it changed nothing.
                        //
                        // Leaving it stale is safe because the header is only load-bearing once the
                        // entries no longer carry the maximum, and entries are removed **here and
                        // nowhere else**. Every path that removes one computes `max_seq_ever` before
                        // dropping anything and writes it into the header inside the same hold of
                        // the file's lock. So: entries dropped implies the header raised, and no
                        // entries dropped implies the entries still carry the mark. A stale header
                        // over a full set of entries is not an observable state.
                        //
                        // Skipping is also strictly SAFER than rewriting, for a reason worth
                        // stating: the rewrite loop below drops any kept entry that fails to
                        // serialise (`Err(_) => continue`), so a file rewritten for no reason is a
                        // file where that can happen for no reason.
                        //
                        // The gate is `malformed_lines`, the COUNT, and not `malformed_lines_text`.
                        // A blank line is counted but deliberately not collected, so gating on the
                        // text would leave blank lines in place forever — and `malformed_lines`
                        // feeds `LedgerReadOutcome::is_complete`, so that shard's fold would retain
                        // on every pass, permanently. Rewriting is what removes them.
                        if entries_after == entries_before && malformed_lines == 0 {
                            return Ok(LedgerFileRewrite {
                                entries_before,
                                entries_after,
                                removed: false,
                                rewritten: false,
                                unchanged: true,
                                malformed_lines: 0,
                            });
                        }

                        let mut body = String::new();
                        // The header first, carrying the PRE-compaction mark forward, so
                        // retiring the highest-seq entry cannot lower a cut.
                        if let Some(max_seq_ever) = max_seq_ever {
                            let header = LedgerHeader {
                                schema: LEDGER_SCHEMA,
                                max_seq_ever,
                            };
                            match serde_json::to_string(&header) {
                                Ok(json) => {
                                    body.push_str(&json);
                                    body.push('\n');
                                }
                                Err(e) => {
                                    return Err(std::io::Error::other(format!(
                                        "serialize ledger header: {e}"
                                    )))
                                }
                            }
                        }
                        for entry in &kept {
                            match serde_json::to_string(entry) {
                                Ok(json) => {
                                    body.push_str(&json);
                                    body.push('\n');
                                }
                                Err(_) => continue,
                            }
                        }
                        let tmp = path_for_task.with_extension("ledger.tmp");
                        {
                            let mut f = std::fs::File::create(&tmp)?;
                            f.write_all(body.as_bytes())?;
                            f.sync_all()?;
                        }
                        std::fs::rename(&tmp, &path_for_task)?;
                        Ok(LedgerFileRewrite {
                            entries_before,
                            entries_after,
                            removed: false,
                            rewritten: true,
                            unchanged: false,
                            malformed_lines,
                        })
                    })();

                    let _ = fs4::FileExt::unlock(&lock_file);
                    result
                })
                .await;

            match rewrite {
                Ok(Ok(outcome)) => {
                    stats.entries_before += outcome.entries_before;
                    stats.entries_after += outcome.entries_after;
                    if outcome.removed {
                        stats.files_removed += 1;
                    }
                    if outcome.rewritten {
                        stats.files_rewritten += 1;
                    }
                    if outcome.unchanged {
                        stats.files_unchanged += 1;
                    }
                    if outcome.malformed_lines > 0 {
                        warn!(
                            torn_lines = outcome.malformed_lines,
                            ledger = ?path,
                            quarantine = ?Self::torn_path_for(&path),
                            "Write-ledger compaction found unparseable line(s) and moved them to \
                             the quarantine file rather than discarding them. Each one is a staged \
                             range whose ledger entry was destroyed by a partial write, so its \
                             bytes are missing from the staged figure until the ledger-repair \
                             rotation re-appends them by range identity from the .meta on disk \
                             (a peer whose identity is not shared can do this, but a pass \
                             refused on a shared identity keeps its claim on the shard, so the \
                             entry can stay unindexed until the hostnames are made distinct). \
                             This is a FINDING and not a routine reading: it means an append was \
                             torn, which points at host or volume durability. Watch \
                             ledger_torn_lines_quarantined_total."
                        );
                    }
                }
                Ok(Err(e)) => {
                    files_unreadable += 1;
                    warn!(
                        "Failed to compact write-ledger file {:?}: {}. Left untouched.",
                        path, e
                    );
                }
                Err(e) => {
                    files_unreadable += 1;
                    warn!("Write-ledger compaction task failed for {:?}: {}", path, e);
                }
            }
        }
        (stats, files_unreadable)
    }

    /// Group entries by cache key, preserving each key's oldest timestamp.
    ///
    /// Eviction acts per object — `evict_write_cached_object` takes a cache key — while
    /// credits, and therefore entries, are per range. A multipart object contributes one
    /// entry per part. Collapsing them here means one verification and one eviction per
    /// object instead of one per part, and it keeps the oldest-first order intact
    /// because a key's position is its earliest entry.
    pub fn group_by_key(entries: Vec<WriteLedgerEntry>) -> Vec<StagedCandidate> {
        let mut order: Vec<String> = Vec::new();
        let mut grouped: HashMap<String, StagedCandidate> = HashMap::new();

        for entry in entries {
            match grouped.get_mut(&entry.cache_key) {
                Some(existing) => {
                    // Keep the NEWEST entry timestamp as the supersede reference, so a
                    // re-PUT that added a later entry is not judged against the older
                    // one and wrongly skipped as superseded.
                    if entry.timestamp > existing.newest_entry_at {
                        existing.newest_entry_at = entry.timestamp;
                    }
                    if entry.timestamp < existing.oldest_entry_at {
                        existing.oldest_entry_at = entry.timestamp;
                    }
                    existing.identities.push(entry.identity());
                }
                None => {
                    order.push(entry.cache_key.clone());
                    grouped.insert(
                        entry.cache_key.clone(),
                        StagedCandidate {
                            cache_key: entry.cache_key.clone(),
                            oldest_entry_at: entry.timestamp,
                            newest_entry_at: entry.timestamp,
                            identities: vec![entry.identity()],
                        },
                    );
                }
            }
        }

        order
            .into_iter()
            .filter_map(|k| grouped.remove(&k))
            .collect()
    }
}

/// Order candidates for staging eviction: expired-unread first, oldest-first within each
/// group (R2.4).
///
/// A free function rather than an inline `partition` inside `evict_staging_tier_locked`
/// so the ordering can be asserted without a `CacheManager`, a shared volume, or a
/// populated cache — the same reasoning as [`assess_staged_candidate`] above.
///
/// `watermark` is `now - put_ttl`. A candidate whose oldest entry predates it has passed
/// its staging TTL without being read, so it is the cheapest thing to reclaim: nothing
/// has demonstrated demand for it and it would expire anyway.
///
/// # Two properties, and the second is the one that is easy to lose
///
/// - Expired-unread ranks ahead of fresh-unread.
/// - A fresh-unread candidate is **still a candidate**, at lower priority. It is not
///   exempt. An implementation that filtered instead of partitioning would satisfy the
///   first property and silently break the second, leaving the tier unable to reclaim
///   anything at all whenever every entry is younger than `put_ttl` — which is the
///   steady state of a write-heavy fleet.
///
/// # The sort is deliberate, and it was added because the guarantee was conditional
///
/// `partition` preserves *input* order, so an earlier version of this relied on the
/// caller already having sorted oldest-first. In production it had — the merged read
/// yields entries oldest-first, and [`WriteLedger::group_by_key`] places each key at its
/// first appearance, so candidate order is ascending `oldest_entry_at` — which is why
/// the omission was invisible. But that made a stated requirement hold by accident of a
/// caller's behaviour, on a `pub` function, with nothing checking it. Feeding
/// unsorted candidates silently produced input order inside each group.
///
/// Sorting explicitly costs a comparison sort over the staged-object count, against a
/// pass that then does one `.meta` read per candidate — so it is free in context and
/// removes an unstated precondition. `sort_by_key` is stable, so candidates with equal
/// timestamps keep their relative order rather than being shuffled arbitrarily.
///
/// Under a uniform `put_ttl` the write order already *is* expiry order, so the partition
/// changes nothing; it earns its keep when per-key `put_ttl` rules make the two differ.
///
/// Spec: write-cache-accounting-and-eviction. Requirements: 2.4
pub fn order_staging_candidates(
    mut candidates: Vec<StagedCandidate>,
    watermark: SystemTime,
) -> Vec<StagedCandidate> {
    candidates.sort_by_key(|c| c.oldest_entry_at);
    let (expired, fresh): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|c| c.oldest_entry_at < watermark);
    expired.into_iter().chain(fresh).collect()
}

/// One object's worth of ledger entries, the unit staging eviction acts on.
#[derive(Debug, Clone)]
pub struct StagedCandidate {
    pub cache_key: String,
    /// Earliest entry for this key. The eviction sort key, and what the
    /// expired-versus-fresh watermark is compared against.
    pub oldest_entry_at: SystemTime,
    /// Latest entry for this key, used as the supersede reference so a re-PUT is not
    /// mistaken for a stale entry.
    pub newest_entry_at: SystemTime,
    /// Identities of the entries that produced this candidate, so compaction can retire
    /// exactly the entries an eviction consumed.
    pub identities: Vec<(String, u64, u64, SystemTime, String)>,
}

/// A candidate's verdict together with the staged bytes its `.meta` accounts for.
///
/// Exists so the staged fold can get a figure out of the **same** `.meta` read that
/// produced the verdict. Two alternatives were rejected: re-reading the `.meta` after the
/// verdict doubles the fold's I/O on the one figure whose cost scales with cache
/// population, and computing the figure in the fold from its own copy of the staged
/// predicate would let eviction's notion of "staged" and the fold's drift apart silently.
///
/// Spec: cache-eviction-at-scale §§ 16.6.1, 16.6.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedCandidateFold {
    /// The verdict, identical to what [`assess_staged_candidate`] returns.
    pub verdict: StagedCandidateVerdict,
    /// `staged_compressed_size()` over this object's `.meta`. Zero unless the verdict is
    /// [`StagedCandidateVerdict::Evictable`] — a graduated, superseded, absent or
    /// unreadable object contributes nothing to `F`.
    pub staged_bytes: u64,
    /// Count of ranges behind `staged_bytes`.
    pub staged_range_count: u64,
}

/// A candidate's verdict, its staged figure, **and which of its ledger entries are terminal**.
///
/// # Why the retirement set is per-range rather than per-key — loss mode L6
///
/// The verdict is a statement about the **object**; retirement acts on **entries**, and an object
/// has one entry per staged range. Conflating the two converts partial loss into total loss:
///
/// A multipart object holds extents A and B, each with a ledger entry. A re-PUT replaces A, and A's
/// ledger append is lost (a failed append never fails the upload — `journal_consolidator.rs`'s
/// `record_staged_range` logs at WARN and the write-cache credit is declined). The `.meta` now carries a newer `created_at` on
/// A, so the key-level `Superseded` comparison — candidate's newest entry against the `.meta`'s
/// newest range — fires for the **whole key**. Both retirement sites then deleted **every** identity
/// the candidate carried, including B's, which is a live staged range whose copy nothing replaced.
/// B's bytes then leave `F`, which lowers `write_cache_size` and biases the staging eviction trigger
/// **LATE** — the one direction § 16.6.2 rules out by name.
///
/// So the assessment answers the retirement question once per entry, against the extent that entry
/// actually names. An entry is terminal when the extent it names is **absent** from the `.meta`,
/// has **graduated** to the read tier, or has been **superseded** by a later write of that same
/// extent. Anything else survives.
///
/// # What this does NOT do, and why the distinction matters for precondition 3b
///
/// It does not make the repair unnecessary. A lost append is still lost, and finding it still needs
/// the range-identity sweep in `ledger_repair`. What it removes is the **escalation**: the sweep now
/// has one lost range to re-append rather than an object's entire entry set, and the survivors are
/// never uncounted in the first place.
///
/// This is also why 3b still guarantees what its name says. The repair that L6 defeated *by
/// construction* is `reappend_missing_ledger_entries`, which matches on `cache_key`: an object
/// retaining even one entry is "known" to it, so a lost sibling is permanently invisible. 3b's
/// rotation matches on `(cache_key, range_start, range_end)`, which is not defeated — it finds the
/// lost extent whether or not its siblings survived. The two repairs are not interchangeable, and
/// the design already records the cache-key one as strictly weaker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedCandidateAssessment {
    /// The object-level verdict. **Unchanged in meaning and in derivation** — eviction's decision
    /// about whether to act on this object is exactly what it was.
    pub verdict: StagedCandidateVerdict,
    /// `staged_compressed_size()` over this object's `.meta`.
    pub staged_bytes: u64,
    /// Count of ranges behind `staged_bytes`.
    pub staged_range_count: u64,
    /// The subset of `candidate.identities` whose own extent is terminal, so retiring them cannot
    /// lose a live staged range.
    ///
    /// Empty on [`StagedCandidateVerdict::Unreadable`]: a `.meta` that could not be read may be
    /// perfectly valid and transiently unavailable, and retiring on the strength of one failed read
    /// would drop a live object's only eviction hint.
    pub retirable: Vec<(String, u64, u64, SystemTime, String)>,
    /// The identity of the object this verdict was reached on, from the same `.meta` read. Set
    /// only on [`StagedCandidateVerdict::Evictable`]. Staging eviction passes it to the eviction,
    /// which declines if the key's `.meta` is a different object by the time it holds the slot.
    pub assessed_identity: Option<crate::write_cache_manager::StagedObjectIdentity>,
}

/// [`assess_staged_candidate`]'s verdict and staged byte figure, without the retirement set.
///
/// Kept as a `Copy` projection of [`assess_staged_candidate`] because the fold calls it per
/// candidate and wants neither the allocation nor the retirement set.
pub(crate) async fn fold_staged_candidate(
    cache_dir: &Path,
    candidate: &StagedCandidate,
) -> StagedCandidateFold {
    let assessment = assess_staged_candidate(cache_dir, candidate).await;
    StagedCandidateFold {
        verdict: assessment.verdict,
        staged_bytes: assessment.staged_bytes,
        staged_range_count: assessment.staged_range_count,
    }
}

/// Assess a candidate against its authoritative `.meta`: verdict, staged figure, and the
/// **range-scoped** set of entries that can be retired.
///
/// One `.meta` read produces all three, deliberately. Re-reading for the retirement set would
/// double the I/O on the one figure whose cost scales with cache population, and computing the
/// retirement decision anywhere else would let it drift from the verdict it has to agree with.
///
/// Lazy verification is what makes ledger entries safe to treat as hints, and it is one
/// decision covering every staleness case (R2.2). Free function rather than a method so
/// it can be unit-tested against a seeded cache directory without a `WriteLedger`.
pub async fn assess_staged_candidate(
    cache_dir: &Path,
    candidate: &StagedCandidate,
) -> StagedCandidateAssessment {
    // **Every retirement below is gated on the entry being older than `ABSENT_RETIREMENT_GRACE`.**
    // A multipart completion appends its entries before renaming its `.meta` into place (L3), so
    // for one rename a live object's fresh entries are judged against whatever `.meta` is on disk:
    // none, or, on an overwrite whose invalidation failed, the PREVIOUS object's. That old document
    // can read as graduated, empty, or lacking the new entry's extent, and each of those used to
    // retire the fresh entry, leaving the new object to publish staged with no entry.
    //
    // Age is the guard rather than "the entry is older than the document it is judged against",
    // because the document's times do not bound the window soundly: a read fill can add a range to
    // the old document during the rename, raising its newest `created_at` above the fresh entry's
    // timestamp, and those times are stamped by other instances' clocks. Age alone bounds the
    // window. The verdict is unchanged, so eviction and the fold act exactly as before; the only
    // cost is that a terminal entry retires one grace later.
    let cutoff = SystemTime::now()
        .checked_sub(ABSENT_RETIREMENT_GRACE)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let past_grace = |entry_at: &SystemTime| *entry_at <= cutoff;

    // A terminal object-level verdict: nothing about the object survives, so every entry for it is
    // terminal too, once past the grace. `retire_all` is correct here in a way it is NOT for
    // `Superseded` — see the struct doc.
    let retire_all = |v: StagedCandidateVerdict| StagedCandidateAssessment {
        verdict: v,
        staged_bytes: 0,
        staged_range_count: 0,
        retirable: candidate
            .identities
            .iter()
            .filter(|(_, _, _, entry_at, _)| past_grace(entry_at))
            .cloned()
            .collect(),
        assessed_identity: None,
    };
    // Read failures retire nothing.
    let retire_none = |v: StagedCandidateVerdict| StagedCandidateAssessment {
        verdict: v,
        staged_bytes: 0,
        staged_range_count: 0,
        retirable: Vec::new(),
        assessed_identity: None,
    };

    let metadata_base = cache_dir.join("metadata");
    let meta_path =
        match crate::disk_cache::get_sharded_path(&metadata_base, &candidate.cache_key, ".meta") {
            Ok(p) => p,
            Err(_) => return retire_none(StagedCandidateVerdict::Unreadable),
        };

    // **No `exists()` probe before this read.** Its entire information content was the `ENOENT`
    // the read itself returns, so it was one fully additive round trip per candidate on a path
    // that is serial by construction — against exactly one useful round trip.
    //
    // **The error kind is discriminated explicitly, and that is load-bearing rather than tidy.**
    // `NotFound` is the only kind that means the object is gone, and it is the only one that may
    // reach `retire_all`: that arm is object-scoped, and it is the arm § 16.6.2's range-scoped
    // retirement rule depends on for its correctness. Collapsing a permissions failure, an I/O
    // error or a directory-in-the-file's-place onto `MetadataAbsent` would retire the entries of a
    // live staged object on one failed read of shared storage — destructive, and in the
    // under-count direction § 16.6.2 rules out. Every other kind retires nothing.
    //
    // Dropping the probe also **narrows** one pre-existing hazard rather than widening it:
    // `Path::exists()` reports `false` for an unreadable parent directory as well as for a
    // genuinely absent file, so the old code reached `retire_all` on a traversal-permission
    // failure. The read reports that as `PermissionDenied`, which now correctly retires nothing.
    //
    // Spec: cache-eviction-at-scale § 16.6.13 (T3).
    let content = match tokio::fs::read_to_string(&meta_path).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // An absent `.meta` retires only entries older than the grace (see `past_grace`
            // above): inside a completion's rename a live object's fresh entries name a `.meta`
            // that does not exist yet. The verdict is still `MetadataAbsent`, so eviction skips
            // the object and the fold counts nothing for it.
            return retire_all(StagedCandidateVerdict::MetadataAbsent);
        }
        Err(_) => return retire_none(StagedCandidateVerdict::Unreadable),
    };
    let metadata: crate::cache_types::NewCacheMetadata = match serde_json::from_str(&content) {
        Ok(m) => m,
        Err(_) => return retire_none(StagedCandidateVerdict::Unreadable),
    };

    // Graduated: the first read cleared the flag and the `Graduation` journal entry has
    // already debited these bytes from `write_cache_size`. Evicting here would be
    // legitimate as read-tier eviction but is not this evictor's job, and the write-cache
    // debit would be wrong.
    //
    // Object-scoped by construction: the flag is a property of the object, so when it is clear no
    // range of the object is staged and every entry really is terminal.
    if !metadata.object_metadata.is_write_cached {
        return retire_all(StagedCandidateVerdict::Graduated);
    }

    // Superseded: a later write replaced the copy these entries describe. Compare
    // against the NEWEST entry for the key — comparing against the oldest would judge a
    // re-PUT's own fresh entry as stale.
    //
    // Goes through `is_superseded` rather than repeating the
    // comparison here, so the predicate its unit test pins is the predicate this function
    // actually evaluates. An inline copy would let the two drift, and the test would then
    // be asserting something production no longer does — which is a test that passes while
    // proving nothing.
    let newest_range_created_at = metadata
        .ranges
        .iter()
        .map(|r| r.created_at)
        .max()
        .unwrap_or(metadata.created_at);
    if is_superseded(candidate.newest_entry_at, newest_range_created_at) {
        // **The verdict stays key-scoped and the retirement does not.** The verdict is what
        // eviction reads to decide whether to act on the object, and that decision is unchanged:
        // an object whose newest extent is newer than every entry naming it is not this evictor's
        // to free. But "the newest extent was replaced" says nothing about the other extents, and
        // deleting their entries is L6 — see the struct doc.
        return StagedCandidateAssessment {
            verdict: StagedCandidateVerdict::Superseded,
            staged_bytes: 0,
            staged_range_count: 0,
            retirable: terminal_identities(candidate, &metadata)
                .into_iter()
                .filter(|(_, _, _, entry_at, _)| past_grace(entry_at))
                .collect(),
            assessed_identity: None,
        };
    }

    // An entry whose `.meta` claims no ranges has nothing to reclaim. Treat as absent
    // rather than evictable so the evictor's freed-bytes accounting cannot be credited
    // for a no-op deletion.
    if metadata.ranges.is_empty() {
        return retire_all(StagedCandidateVerdict::MetadataAbsent);
    }

    // The figure comes from the same parsed `.meta` the verdict came from, through
    // `staged_compressed_size()` — the per-range predicate, not the object-level flag. A
    // mixed-tier object (some ranges staged, some graduated) is therefore counted for its
    // staged ranges only, which the object-level `compressed_size` hint on the ledger
    // entry cannot express.
    StagedCandidateAssessment {
        verdict: StagedCandidateVerdict::Evictable,
        staged_bytes: metadata.staged_compressed_size(),
        staged_range_count: metadata
            .ranges
            .iter()
            .filter(|r| {
                crate::cache_types::is_staged_range_spec(
                    r,
                    metadata.object_metadata.is_write_cached,
                )
            })
            .count() as u64,
        // Deliberately empty. An `Evictable` object is still staged, so no entry naming it is
        // terminal *on the strength of this verdict*. The eviction site retires the identities it
        // CONSUMED, after a successful free, which is a different statement and stays where it is.
        retirable: Vec::new(),
        assessed_identity: Some(crate::write_cache_manager::StagedObjectIdentity::of(
            &metadata,
        )),
    }
}

impl AppenderCore {
    /// `{cache_dir}/metadata/_write_ledger`.
    fn ledger_dir(&self) -> PathBuf {
        self.cache_dir.join("metadata").join(LEDGER_DIR)
    }

    /// This instance's legacy flat ledger file; see [`WriteLedger::legacy_path_for`].
    fn legacy_path(&self) -> PathBuf {
        WriteLedger::legacy_path_for(&self.cache_dir, &self.instance_id)
    }

    /// This instance's `.ledger.mark`; see [`WriteLedger::seq_mark_path`].
    fn seq_mark_path(&self) -> PathBuf {
        WriteLedger::seq_mark_path_for(&self.cache_dir, &self.instance_id)
    }

    /// The appender task: drain the queue in batches until every sender is gone.
    async fn run(
        core: std::sync::Arc<Self>,
        mut rx: tokio::sync::mpsc::Receiver<LedgerSubmission>,
    ) {
        let mut batch = Vec::with_capacity(LEDGER_BATCH_MAX);
        loop {
            if rx.recv_many(&mut batch, LEDGER_BATCH_MAX).await == 0 {
                return;
            }
            core.append_batch(&mut batch).await;
            batch.clear();
        }
    }

    /// Issue, write and answer one batch. Every submission in `batch` gets exactly one reply.
    async fn append_batch(&self, batch: &mut Vec<LedgerSubmission>) {
        // Counted at the drain, not at the write: the ratio these two publish is the mean size
        // of a batch the stage took off its queue, and a batch that fails to seed or to raise
        // the mark was still one drain of the queue. Both are incremented once per batch, so a
        // reader can divide them.
        self.stages
            .append_batches
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.stages
            .append_batched_entries
            .fetch_add(batch.len() as u64, AtomicOrdering::Relaxed);
        // Held across seeding, issuing AND writing, so no second batch can be issued a number
        // while this one is still working out the base or still writing.
        let _guard = self.append_mutex.lock().await;
        // Another live process is publishing under this process's exact `{host}:{pid}`, so
        // `{instance}.ledger.mark` is not this process's alone and neither is the sequence
        // seeded from it. See [`WriteLedger::append`]'s second refusal rule.
        let identity_shared =
            crate::signed_put_handler::identity_shared_with_a_live_peer(&self.cache_dir);
        #[cfg(test)]
        let identity_shared = identity_shared && !LEDGER_IGNORES_A_SHARED_IDENTITY;
        if identity_shared {
            if !IDENTITY_SHARED_REFUSAL_LOGGED.swap(true, AtomicOrdering::AcqRel) {
                warn!(
                    "Write-ledger REFUSING to append: another live process holds this proxy's \
                     identity {}, so the sequence mark both would seed from is shared and the \
                     numbers they issue would collide. A colliding number is filtered out of the \
                     pending term by a cut no fold has honoured, which is worse than an \
                     unindexed range. Give each proxy on this volume its own hostname.",
                    self.instance_id
                );
            }
            let refusal = ProxyError::CacheError(format!(
                "Write-ledger refusing to append: instance identity {} is shared with another \
                 live process",
                self.instance_id
            ));
            for sub in batch.drain(..) {
                let _ = sub.reply.send(Err(refusal.clone()));
            }
            return;
        }
        // Before any file is opened, so this id is never named by a ledger file with no
        // `.ready` record behind it. Once per process in practice; see the method.
        self.ensure_ready_record().await;
        let mut sequence = self.sequence.lock().await;
        let mut state = match *sequence {
            Some(state) => state,
            None => match self.seed_sequence().await {
                Ok(state) => state,
                Err(e) => {
                    // Left unseeded, so the next batch retries the seed.
                    for sub in batch.drain(..) {
                        let _ = sub.reply.send(Err(e.clone()));
                        self.stages.ledger.fetch_sub(1, AtomicOrdering::Relaxed);
                    }
                    return;
                }
            },
        };
        let first = state.next;
        let last = first + batch.len() as u64 - 1;
        // Write-ahead: the persistent mark must already bound the batch's LAST number before
        // any line is written, or a crash would let a process reusing this instance identity
        // reissue one of them. One raise covers the batch because it is far below the stride.
        if last > state.mark_reserved {
            match self.write_seq_mark(last + SEQ_MARK_STRIDE - 1).await {
                Ok(reserved) => state.mark_reserved = reserved,
                Err(e) => {
                    // Nothing was issued, so nothing is burned; the stored state is untouched.
                    for sub in batch.drain(..) {
                        let _ = sub.reply.send(Err(e.clone()));
                        self.stages.ledger.fetch_sub(1, AtomicOrdering::Relaxed);
                    }
                    return;
                }
            }
        }
        #[cfg(test)]
        self.sync_probe.batches.lock().unwrap().push((first, last));

        // Issue in drain order and group by shard file, ascending seq within each group.
        let mut groups: Vec<LedgerGroup> = Vec::new();
        let mut group_of: HashMap<PathBuf, usize> = HashMap::new();
        let mut replies: Vec<LedgerReply> = Vec::with_capacity(batch.len());
        for (i, sub) in batch.drain(..).enumerate() {
            let LedgerSubmission {
                mut entry,
                path,
                pair_pending,
                reply,
                handed,
            } = sub;
            let seq = first + i as u64;
            entry.seq = Some(seq);
            // The `Pending_Credit` half of this credit, stamped with the seq THIS entry's line
            // carries — built here because this is the only place that knows it, and handed over
            // only once this entry's group has synced. The range identity goes with it, so two
            // credits for one range (two repair sweeps under a duplicated claim) are counted
            // once by the pending term.
            let pending = if pair_pending {
                crate::staged_fold::shard_of(&entry.cache_key).map(|(bucket, l1)| {
                    crate::staged_fold::PendingCredit {
                        seq,
                        bucket,
                        l1,
                        bytes: entry.compressed_size,
                        range: Some(crate::staged_fold::PendingRange {
                            cache_key: entry.cache_key.clone(),
                            start: entry.range_start,
                            end: entry.range_end,
                        }),
                    }
                })
            } else {
                None
            };
            // Serialised after its seq is set, because the line's content depends on it.
            let outcome = match serde_json::to_string(&entry) {
                Ok(mut line) => {
                    line.push('\n');
                    let g = *group_of.entry(path.clone()).or_insert_with(|| {
                        groups.push(LedgerGroup {
                            path,
                            lines: String::new(),
                            members: Vec::new(),
                        });
                        groups.len() - 1
                    });
                    groups[g].lines.push_str(&line);
                    groups[g].members.push(i);
                    debug!(
                        "Write-ledger append issued: key={}, range={}-{}, size={}, seq={}",
                        entry.cache_key,
                        entry.range_start,
                        entry.range_end,
                        entry.compressed_size,
                        seq
                    );
                    None
                }
                Err(e) => Some(Err(ProxyError::CacheError(format!(
                    "Failed to serialize write-ledger entry: {}",
                    e
                )))),
            };
            replies.push(LedgerReply {
                reply,
                seq,
                outcome,
                pending,
                handed,
            });
        }

        let writes = Self::write_groups(groups, std::sync::Arc::clone(&self.sync_probe));
        // The barrier: every group of this batch resolves, Ok, Err or panic, before the guards
        // are released and the next batch can be issued.
        let resolved = writes.await;
        // Burned, not reused, for every outcome including a panicked group: a failure at the
        // sync, a short `write_all` or a panic after it can follow bytes that did reach the
        // file, and reissuing a number would put two entries under one seq. A gap is harmless
        // to the cut; a reuse is not something the cut argument covers.
        state.next = last + 1;
        *sequence = Some(state);
        self.reply_per_group(resolved, replies).await;
    }

    /// Write every group, up to [`LEDGER_GROUP_CONCURRENCY`] at once, and return each group's
    /// members with its outcome once all have resolved.
    async fn write_groups(
        groups: Vec<LedgerGroup>,
        probe: std::sync::Arc<LedgerSyncProbe>,
    ) -> Vec<GroupOutcome> {
        use futures::StreamExt;
        futures::stream::iter(groups.into_iter().map(|group| {
            let probe = std::sync::Arc::clone(&probe);
            async move {
                let LedgerGroup {
                    path,
                    lines,
                    members,
                } = group;
                let joined =
                    tokio::task::spawn_blocking(move || write_group(&path, &lines, &probe)).await;
                (members, joined.map_err(|e| e.to_string()))
            }
        }))
        .buffer_unordered(LEDGER_GROUP_CONCURRENCY)
        .collect()
        .await
    }

    /// Answer each entry with its own group's outcome: `Ok(seq)` iff that group's write and
    /// sync returned `Ok`. Groups are separate files with separate syncs, so one group's
    /// failure says nothing about another's durability.
    ///
    /// A paired entry that reached `Ok` is not answered here at all: it is handed to the pending
    /// stage, which answers it once its `Pending_Credit` is durable or that write has failed.
    async fn reply_per_group(&self, resolved: Vec<GroupOutcome>, mut replies: Vec<LedgerReply>) {
        for (members, outcome) in resolved {
            for i in members {
                let r = &mut replies[i];
                r.outcome = Some(match &outcome {
                    Ok(Ok(())) => Ok(r.seq),
                    Ok(Err(e)) => Err(ProxyError::CacheError(format!(
                        "Failed to append write-ledger entry: {}",
                        e
                    ))),
                    Err(e) => Err(ProxyError::CacheError(format!(
                        "Write-ledger append task failed: {}",
                        e
                    ))),
                });
            }
        }
        for r in replies {
            let outcome = r.outcome.unwrap_or_else(|| {
                Err(ProxyError::CacheError(
                    "Write-ledger entry was issued but not written".to_string(),
                ))
            });
            match (&outcome, r.pending) {
                // Durable line, paired credit: the pending stage owns the reply from here. The
                // hand-over raises the pending depth BEFORE this entry leaves the append
                // stage's, so a concurrent drain never reads both as zero while the entry is
                // unanswered.
                (Ok(_), Some(credit)) => {
                    // Stamped BEFORE the hand-over, so an entry whose reply the pending stage
                    // drops the instant it receives it is still attributed to that stage. The
                    // line is durable by this point: only an `Ok` group reaches this arm.
                    r.handed.store(true, AtomicOrdering::Release);
                    self.hand_to_pending(credit, r.reply).await
                }
                _ => {
                    let _ = r.reply.send(outcome);
                }
            }
            self.stages.ledger.fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }

    /// The pending stage's queue, spawning the stage task on first use. Same respawn rule as
    /// [`WriteLedger::appender`]: only if the previous task's receiver is gone.
    fn pending_stage(&self) -> tokio::sync::mpsc::Sender<PendingSubmission> {
        let mut slot = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tx) = slot.as_ref() {
            if !tx.is_closed() {
                return tx.clone();
            }
        }
        let (tx, rx) = tokio::sync::mpsc::channel(PENDING_QUEUE_CAPACITY);
        let task = tokio::spawn(Self::run_pending_stage(
            self.cache_dir.clone(),
            self.instance_id.clone(),
            std::sync::Arc::clone(&self.stages),
            std::sync::Arc::clone(&self.sync_probe),
            rx,
        ));
        #[cfg(test)]
        {
            *self.pending_task.lock().unwrap() = Some(task);
        }
        #[cfg(not(test))]
        drop(task);
        *slot = Some(tx.clone());
        tx
    }

    /// Hand one durable entry's credit to the pending stage.
    ///
    /// A full queue makes this wait, so the ledger stage carries the pending stage's
    /// back-pressure rather than shedding a credit. If the stage has stopped altogether, this
    /// answers the caller itself — its ledger line is durable, so the credit stands — and marks
    /// the credit's shard dirty, exactly as a failed batch does.
    async fn hand_to_pending(
        &self,
        credit: crate::staged_fold::PendingCredit,
        reply: tokio::sync::oneshot::Sender<Result<u64>>,
    ) {
        let seq = credit.seq;
        // Raised before the send and before the caller's entry leaves the append stage's depth,
        // so the drain sees the work move from one stage to the other rather than vanish.
        self.stages.pending.fetch_add(1, AtomicOrdering::Relaxed);
        if let Err(e) = self
            .pending_stage()
            .send(PendingSubmission { credit, reply })
            .await
        {
            let PendingSubmission { credit, reply } = e.0;
            crate::staged_fold::mark_dirty_shard(
                &self.cache_dir,
                &self.instance_id,
                credit.shard(),
            );
            warn!(
                "Pending-credit stage has stopped; the credit for seq={} (bytes={}) was not \
                 written. Its shard is marked dirty, so the next staged-fold pass visits it and \
                 the cut it installs covers the entry's durable ledger line.",
                seq, credit.bytes
            );
            let _ = reply.send(Ok(seq));
            self.stages.pending.fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }

    /// The pending stage (design § 16.6.14): one task per `WriteLedger`, draining handed-over
    /// credits in batches of up to [`PENDING_BATCH_MAX`] and writing each batch with one lock
    /// hold, one `write_all` and one `sync_data`.
    ///
    /// It replies only after that write, which is what keeps `append_staged_entry`'s contract:
    /// its caller returns once the pending record is durable, or once this stage has failed.
    /// Nothing orders one pending batch against another, so the stage runs beside the ledger
    /// stage rather than behind it: record order in the file carries no meaning, because every
    /// reader filters each record by its own seq against its own shard's cut.
    ///
    /// # Why a failed batch marks shards dirty
    ///
    /// One failure loses up to [`PENDING_BATCH_MAX`] records where one record was lost before.
    /// Every lost record's ledger line is durable, so the fold counts its `.meta` once that
    /// shard's cut passes the line, and the figure **under**-states those bytes until then —
    /// the same direction and the same repair as one lost record, but larger per event. Marking
    /// every affected shard dirty (§ 16.6.9) puts those shards ahead of stalest-first at the
    /// next pass, and that visit begins after the ledger line was durable, so the cut it
    /// installs covers it. That bounds the under-count to one pass rather than one rotation.
    ///
    /// # Why a failed batch still answers `Ok`
    ///
    /// The credit the caller asked for is the ledger line, and that line is durable:
    /// `record_staged_range` must still return `true`, or the write-cache credit would be
    /// declined for bytes the fold is about to count anyway. That is what one lost pending
    /// record has always done.
    async fn run_pending_stage(
        cache_dir: PathBuf,
        instance_id: String,
        stages: std::sync::Arc<StageCounters>,
        probe: std::sync::Arc<LedgerSyncProbe>,
        mut rx: tokio::sync::mpsc::Receiver<PendingSubmission>,
    ) {
        #[cfg(not(test))]
        let _ = &probe;
        let store = crate::staged_fold::PendingStore::new(cache_dir.clone());
        let mut batch: Vec<PendingSubmission> = Vec::with_capacity(LEDGER_BATCH_MAX);
        loop {
            if rx.recv_many(&mut batch, PENDING_BATCH_MAX).await == 0 {
                return;
            }
            stages.pending_batches.fetch_add(1, AtomicOrdering::Relaxed);
            // Test seam only: hold this batch, and so this credit's reply, inside the pending
            // stage while the append stage's depth is already back to zero. On a blocking
            // thread, so the park costs no runtime worker.
            #[cfg(test)]
            {
                // The guard is taken and dropped in its own statement: holding it across the
                // await below would make this future `!Send`.
                let gate = probe.pending_park.lock().unwrap().take();
                if let Some(gate) = gate {
                    let _ = tokio::task::spawn_blocking(move || gate.wait()).await;
                }
            }
            let credits: Vec<crate::staged_fold::PendingCredit> =
                batch.iter().map(|s| s.credit.clone()).collect();
            if let Err(e) = store.append_batch(&instance_id, &credits).await {
                stages
                    .pending_batch_failures
                    .fetch_add(1, AtomicOrdering::Relaxed);
                // Before any reply, so a caller that has observed its own return has also
                // observed the mark its shard needs.
                let shards: std::collections::BTreeSet<crate::staged_fold::Shard> =
                    credits.iter().map(|c| c.shard()).collect();
                for shard in &shards {
                    crate::staged_fold::mark_dirty_shard(&cache_dir, &instance_id, shard.clone());
                }
                warn!(
                    "Failed to append {} pending credit(s) as one batch: {}. {} shard(s) are \
                     marked dirty, so the next staged-fold pass visits them ahead of \
                     stalest-first and the cut it installs covers these entries' durable \
                     ledger lines. The staged figure under-states these bytes until then.",
                    credits.len(),
                    e,
                    shards.len()
                );
            }
            for sub in batch.drain(..) {
                let _ = sub.reply.send(Ok(sub.credit.seq));
                stages.pending.fetch_sub(1, AtomicOrdering::Relaxed);
            }
        }
    }

    /// Make sure this id has a `.ready` record before its first ledger entry: write the startup
    /// record ([`crate::staged_authority::publish_startup_ready_record`]) unless one exists.
    /// Design § 16.6.6 B10.3.
    ///
    /// # Why here
    ///
    /// Every entry under this id is written by the appender, so a record published from
    /// inside it precedes every append by construction: the repair re-append on the first
    /// consolidation cycle, a write-through credit, a multipart completion, a validation re-append.
    /// `maintain_staged_authority`, the other publisher, runs only on a non-idle cycle whose lock
    /// attempt this instance wins, so without this a process can append for its whole life and
    /// leave a record-less id, which holds activation for a day after it stops (B8).
    ///
    /// Best-effort, as the append is: a failed publish logs and leaves the flag clear, so the next
    /// append retries, and meanwhile the id is record-less, which is the behaviour without it.
    async fn ensure_ready_record(&self) {
        use std::sync::atomic::Ordering;
        if self.ready_record_ensured.load(Ordering::Acquire) {
            return;
        }
        let cache_dir = self.cache_dir.clone();
        let instance_id = self.instance_id.clone();
        let fold_incomplete_total = crate::staged_fold::fold_incomplete_total();
        let published = tokio::task::spawn_blocking(move || {
            crate::staged_authority::publish_startup_ready_record(
                &cache_dir,
                &instance_id,
                fold_incomplete_total,
            )
        })
        .await;
        match published {
            Ok(Ok(_)) => self.ready_record_ensured.store(true, Ordering::Release),
            Ok(Err(e)) => warn!(
                "Could not publish the startup staged-authority ready record for {}: {}. The \
                 append proceeds; this id holds staged-authority activation until a record is \
                 published, and the next append retries.",
                self.instance_id, e
            ),
            Err(e) => warn!(
                "Startup staged-authority ready record publication for {} did not complete: {}",
                self.instance_id, e
            ),
        }
    }

    /// Seed the sequence, returning the allocator state with the first seq to issue.
    ///
    /// # The floor, and the case a per-file header cannot cover
    ///
    /// The base is `max(sequence mark, legacy flat file's own high-water mark)`. Neither term
    /// is redundant:
    ///
    /// - the **mark** (`{instance}.ledger.mark`) is the only O(1) record of what this
    ///   instance identity has ever issued. Under the flat layout the instance's own single
    ///   file answered that; under the per-shard layout its numbers are spread over up to
    ///   `B × 256` files, and reading them to seed would be exactly the whole-volume read this
    ///   layout exists to make unnecessary. **`seed` reads no ledger file at all.**
    /// - the **legacy flat file** is read because a process that reuses an instance identity
    ///   across the upgrade — `{hostname}:{pid}`, and pids are recycled — can find a flat
    ///   file whose mark predates any `.mark` file. Its `max_seq_ever` is then the only
    ///   record, and it may be higher.
    ///
    /// The case that makes this load-bearing rather than defensive: a shard whose write-cache
    /// population has fully drained keeps its checkpoint, and that checkpoint carries
    /// `staged_fold_cut[i] = 900`. A per-shard sequence seeded from that shard's own (absent
    /// or header-only) file would issue `1`, and the credit's pending record would be filtered
    /// by `1 > 900 == false` as already-folded while no fold had read its `.meta` — counted by
    /// neither term. The floor makes every number this identity issues strictly greater than
    /// every cut it has ever justified, **for every shard, including one with no file**.
    ///
    /// # Refusal
    ///
    /// A read failure other than "not found", on either input, propagates: see
    /// [`WriteLedger::append`]'s refusal rule. `sequence` is left unseeded on failure, so the next
    /// append retries rather than inheriting a guess.
    async fn seed_sequence(&self) -> Result<SequenceState> {
        let mark_path = self.seq_mark_path();
        let legacy_path = self.legacy_path();
        let floor = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
            let from_mark = match std::fs::read_to_string(&mark_path) {
                Ok(content) => serde_json::from_str::<LedgerHeader>(content.trim())
                    .map(|h| h.max_seq_ever)
                    // A mark file that exists and cannot be parsed is NOT treated as absent.
                    // Absent means "this identity has issued nothing"; unparseable means "it
                    // has issued something and we cannot see how far", and only one of those
                    // is safe to answer with 0.
                    .map_err(|e| std::io::Error::other(format!("parse sequence mark: {e}")))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e),
            };
            let from_legacy = match std::fs::read_to_string(&legacy_path) {
                Ok(content) => WriteLedger::parse_content(&legacy_path, &content)
                    .max_seq_ever()
                    .unwrap_or(0),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e),
            };
            Ok(from_mark.max(from_legacy))
        })
        .await;

        let base = match floor {
            Ok(Ok(base)) => base,
            Ok(Err(e)) => {
                warn!(
                    "Write-ledger REFUSING to append: this instance's sequence floor could \
                     not be read ({}), so the next Ledger_Seq is unknown. Appending would \
                     restart the sequence and stamp credits that a cut has already passed, \
                     making their bytes invisible to both the fold and the pending term. The \
                     range is left unindexed instead.",
                    e
                );
                return Err(ProxyError::CacheError(format!(
                    "Write-ledger sequence unseedable, refusing to append: {}",
                    e
                )));
            }
            Err(e) => {
                return Err(ProxyError::CacheError(format!(
                    "Write-ledger seed task failed: {}",
                    e
                )))
            }
        };

        // Reserve the first block before issuing anything out of it.
        let mark_reserved = self.write_seq_mark(base + SEQ_MARK_STRIDE).await?;
        Ok(SequenceState {
            next: base + 1,
            mark_reserved,
        })
    }

    /// Raise the persistent sequence mark to `reserved_through`, returning it.
    ///
    /// Temp-write and rename, so a torn write cannot leave a mark **lower** than the previous
    /// one — which is the only direction that matters, since a low mark is what lets a reused
    /// identity reissue numbers a cut has already passed.
    ///
    /// A failure refuses the append. That is stricter than strictly necessary — the number
    /// about to be issued is still correct in this process — but the mark is what makes it
    /// correct across a restart, and the volume that just failed this tiny write is the same
    /// volume the ledger append is about to go to.
    async fn write_seq_mark(&self, reserved_through: u64) -> Result<u64> {
        let path = self.seq_mark_path();
        let dir = self.ledger_dir();
        let written = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            std::fs::create_dir_all(&dir)?;
            let header = LedgerHeader {
                schema: LEDGER_SCHEMA,
                max_seq_ever: reserved_through,
            };
            let mut body = serde_json::to_string(&header)
                .map_err(|e| std::io::Error::other(format!("serialize sequence mark: {e}")))?;
            body.push('\n');
            let tmp = path.with_extension("mark.tmp");
            {
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(body.as_bytes())?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, &path)
        })
        .await;

        match written {
            Ok(Ok(())) => Ok(reserved_through),
            Ok(Err(e)) => {
                warn!(
                    "Write-ledger REFUSING to append: could not write the sequence mark {:?} \
                     ({}). Without it a process reusing this instance identity could reissue \
                     numbers an installed cut has already passed.",
                    self.seq_mark_path(),
                    e
                );
                Err(ProxyError::CacheError(format!(
                    "Write-ledger sequence mark unwritable, refusing to append: {}",
                    e
                )))
            }
            Err(e) => Err(ProxyError::CacheError(format!(
                "Write-ledger sequence mark task failed: {}",
                e
            ))),
        }
    }
}
/// One group's blocking write: `create_dir_all`, the file's `fs4` lock, the header if the file
/// is empty, one `write_all` of every line, `flush`, `sync_append`, unlock.
fn write_group(path: &Path, lines: &str, probe: &LedgerSyncProbe) -> std::io::Result<()> {
    use std::io::Write;

    #[cfg(test)]
    {
        let gate = {
            let mut park = probe.park.lock().unwrap();
            let hit = matches!(park.as_ref(), Some((p, _)) if p.as_path() == path);
            if hit {
                park.take().map(|(_, g)| g)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.wait();
        }
    }

    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(WriteLedger::lock_path_for(path))?;
    // Blocking lock, unlike the journal's `try_lock`. The only contenders are compaction and
    // the fold's read, which hold it for one short pass, and unlike the journal there is no
    // fresh-file fallback to spill to. Waiting is cheap and bounded.
    fs4::FileExt::lock(&lock_file)?;

    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        // A shard file's first line is its header, so every file this layout creates carries
        // its own high-water mark from the outset and a cut is reconstructible from the file
        // alone. `max_seq_ever: 0` is not a claim of a known position:
        // `ParsedLedgerFile::max_seq_ever` takes the max against the entries, so the entries
        // stand until compaction raises it.
        if file.metadata()?.len() == 0 {
            let header = LedgerHeader {
                schema: LEDGER_SCHEMA,
                max_seq_ever: 0,
            };
            let mut head = serde_json::to_string(&header)
                .map_err(|e| std::io::Error::other(format!("serialize header: {e}")))?;
            head.push('\n');
            file.write_all(head.as_bytes())?;
        }
        #[cfg(test)]
        if let Some(mark) = probe.observe_mark.lock().unwrap().clone() {
            let seen = std::fs::read_to_string(&mark)
                .ok()
                .and_then(|c| serde_json::from_str::<LedgerHeader>(c.trim()).ok())
                .map(|h| h.max_seq_ever)
                .unwrap_or(0);
            probe.observed_marks.lock().unwrap().push(seen);
        }
        // All the group's lines in one write, inside one lock hold, so a reader holding the
        // lock sees all of this batch's lines for this shard or none of them.
        file.write_all(lines.as_bytes())?;
        file.flush()?;
        #[cfg(test)]
        {
            let mut panic_at = probe.panic_after_write.lock().unwrap();
            if panic_at.as_deref() == Some(path) {
                panic_at.take();
                drop(panic_at);
                panic!("injected panic after the group write_all");
            }
        }
        // Before the file is dropped, so a deferred NFS write error (which `close` would report
        // and `drop` discards) fails this group instead of returning `Ok` for credits that never
        // reached storage, and so a local power loss cannot lose an acknowledged entry. The
        // callers decline their credits on `Err`.
        sync_append(&file, path, probe)
    })();

    let _ = fs4::FileExt::unlock(&lock_file);
    result
}

/// The subset of a candidate's identities whose own extent the `.meta` shows is terminal.
///
/// Three terminal cases, each per-extent:
///
/// - **absent** — no range in the `.meta` has this entry's `(start, end)`. The extent this entry
///   describes is gone, so the entry can never become evictable.
/// - **graduated** — the range exists but the shared per-range predicate says it is read-tier now.
///   Its bytes were debited from `write_cache_size` already.
/// - **superseded** — the range exists, is staged, and its `created_at` is strictly newer than this
///   entry's timestamp, so a later write replaced the copy this entry describes.
///
/// The comparison goes through [`is_superseded`] and the membership through
/// `is_staged_range_spec`, so this function introduces no third copy of either predicate.
///
/// Matching is on `(start, end)` exactly, which is the same range identity `ledger_repair`'s
/// rotation uses. A partially-overlapping rewrite therefore reads as "absent" and retires, which is
/// the safe direction: the entry names bytes no range file holds under that extent, so keeping it
/// would give staging eviction a candidate it cannot free.
///
/// This is the terminal set as the `.meta` shows it. The caller removes entries younger than
/// [`ABSENT_RETIREMENT_GRACE`] before retiring any, because the `.meta` read may be a previous
/// object's document still on disk during a multipart completion's rename.
fn terminal_identities(
    candidate: &StagedCandidate,
    metadata: &crate::cache_types::NewCacheMetadata,
) -> Vec<(String, u64, u64, SystemTime, String)> {
    candidate
        .identities
        .iter()
        .filter(|(_, start, end, entry_at, _)| {
            match metadata
                .ranges
                .iter()
                .find(|r| r.start == *start && r.end == *end)
            {
                None => true,
                Some(range) => {
                    !crate::cache_types::is_staged_range_spec(
                        range,
                        metadata.object_metadata.is_write_cached,
                    ) || is_superseded(*entry_at, range.created_at)
                }
            }
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn entry(key: &str, start: u64, end: u64, size: u64, secs_ago: u64) -> WriteLedgerEntry {
        WriteLedgerEntry {
            timestamp: SystemTime::now() - Duration::from_secs(secs_ago),
            cache_key: key.to_string(),
            range_start: start,
            range_end: end,
            compressed_size: size,
            instance_id: "host:1".to_string(),
            // Overwritten by `append`, which owns the sequence.
            seq: None,
        }
    }

    #[tokio::test]
    async fn append_then_read_round_trips() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        ledger
            .append_staged_range("bucket/a.bin", 0, 1023, 512)
            .await
            .unwrap();

        let entries = ledger.read_merged_oldest_first(0).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cache_key, "bucket/a.bin");
        assert_eq!(entries[0].range_end, 1023);
        assert_eq!(entries[0].compressed_size, 512);
        assert_eq!(ledger.count_entries().await, Some(1));
    }

    /// The ledger refuses to append while another live process holds this
    /// instance's identity.
    ///
    /// Two processes on one `{host}:{pid}` seed their sequence from one
    /// `{instance}.ledger.mark` and reserve blocks over each other, so both issue the same
    /// `Ledger_Seq` to different ranges. A credit carrying a number a `Fold_Cut` has already
    /// passed is filtered out of the pending term while no fold has read its `.meta`, and the
    /// entry is in the ledger file, so the repair sweep's coverage test reads the range as
    /// indexed and never repairs it. An unindexed range is the bounded alternative, which is the
    /// trade [`WriteLedger::append`]'s sequence-floor refusal already makes.
    ///
    /// The first append is the positive control: without it a refusal that came from an
    /// unwritable fixture would read the same way.
    #[tokio::test]
    async fn the_ledger_refuses_to_append_while_its_identity_is_shared() {
        let _guard = crate::cache::hostname_flag_test_guard().await;
        crate::signed_put_handler::reset_noted_collision_peers_for_test();
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        let control = tokio::time::timeout(
            Duration::from_secs(30),
            ledger.append_staged_range("bucket/control.bin", 0, 1023, 512),
        )
        .await
        .expect("the control append must not hang");
        crate::signed_put_handler::set_identity_shared_for_test(tmp.path());
        let refused = tokio::time::timeout(
            Duration::from_secs(30),
            ledger.append_staged_range("bucket/shared.bin", 0, 1023, 512),
        )
        .await
        .expect("a refused append must not hang");
        let indexed: Vec<String> = ledger
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.cache_key)
            .collect();
        crate::signed_put_handler::reset_noted_collision_peers_for_test();
        assert_eq!(
            (control.is_ok(), refused.is_ok(), indexed),
            (true, false, vec!["bucket/control.bin".to_string()]),
            "(the control append, the append under a shared identity, the ranges the ledger \
             now indexes): while another live process holds this identity the sequence is not \
             this process's to issue, so the entry must be refused rather than written with a \
             number the peer may also use"
        );
    }

    /// L4: every append data-syncs its file before returning `Ok`, once per append.
    #[tokio::test]
    async fn append_syncs_before_returning_ok() {
        use std::sync::atomic::Ordering;
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        for i in 1..=3u64 {
            ledger
                .append(entry(&format!("bucket/s{i}.bin"), 0, 9, 10, 0))
                .await
                .unwrap();
            assert_eq!(ledger.sync_probe().syncs.load(Ordering::SeqCst), i);
        }
    }

    /// L4: a failed sync fails the append, writes no `Pending_Credit` for it, and burns the
    /// seq so a later append cannot reissue a number whose line may already be in the file.
    #[tokio::test]
    async fn sync_failure_fails_the_append_and_writes_no_pending_credit() {
        use std::sync::atomic::Ordering;
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        let first = ledger
            .append(entry("bucket/before.bin", 0, 9, 10, 0))
            .await
            .unwrap();

        ledger.sync_probe().fail.store(true, Ordering::SeqCst);
        let failed = ledger
            .append_staged_range("bucket/failed.bin", 0, 1023, 512)
            .await;
        assert!(failed.is_err(), "a failed sync must fail the append");
        let pending = crate::staged_fold::PendingStore::new(tmp.path().to_path_buf())
            .read_all()
            .await;
        assert!(
            pending.credits.is_empty(),
            "no Pending_Credit may follow a failed append, got {:?}",
            pending.credits
        );

        ledger.sync_probe().fail.store(false, Ordering::SeqCst);
        let after = ledger
            .append(entry("bucket/after.bin", 0, 9, 10, 0))
            .await
            .unwrap();
        assert_eq!(
            after,
            first + 2,
            "the failed append's seq is burned, not reissued"
        );
    }

    /// Appending must not rewrite the file, because a ledger is as long as the staged
    /// set. Asserted behaviourally: many appends all survive, which a
    /// read-modify-write that lost a race would not guarantee, and which a truncating
    /// open would break outright.
    #[tokio::test]
    async fn appends_accumulate_rather_than_replace() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        for i in 0..50u64 {
            ledger
                .append_staged_range(&format!("bucket/obj{}.bin", i), 0, 99, 100)
                .await
                .unwrap();
        }

        assert_eq!(ledger.count_entries().await, Some(50));
    }

    #[tokio::test]
    async fn merged_read_is_oldest_first_across_instances() {
        let tmp = TempDir::new().unwrap();
        let a = WriteLedger::new(tmp.path().to_path_buf(), "hostA:1".to_string());
        let b = WriteLedger::new(tmp.path().to_path_buf(), "hostB:2".to_string());

        // Write out of order and from two instances; the merge must still be oldest-first.
        a.append(entry("bucket/new.bin", 0, 9, 10, 10))
            .await
            .unwrap();
        b.append(entry("bucket/old.bin", 0, 9, 10, 100))
            .await
            .unwrap();
        a.append(entry("bucket/mid.bin", 0, 9, 10, 50))
            .await
            .unwrap();

        let entries = a.read_merged_oldest_first(0).await.unwrap();
        let keys: Vec<&str> = entries.iter().map(|e| e.cache_key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["bucket/old.bin", "bucket/mid.bin", "bucket/new.bin"]
        );
    }

    #[tokio::test]
    async fn unparseable_lines_are_skipped_not_fatal() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        ledger
            .append_staged_range("bucket/good.bin", 0, 9, 10)
            .await
            .unwrap();

        // Append garbage plus a line carrying an unknown field, which is what an older
        // instance would see from a newer release.
        let path = ledger.shard_path_for_key("bucket/good.bin").unwrap();
        let mut content = tokio::fs::read_to_string(&path).await.unwrap();
        content.push_str("this is not json\n");
        content.push_str("{\"timestamp\":1,\"cache_key\":\"b/x\",\"range_start\":0,\"range_end\":1,\"compressed_size\":2,\"instance_id\":\"h:1\",\"future_field\":true}\n");
        tokio::fs::write(&path, content).await.unwrap();

        let entries = ledger.read_merged_oldest_first(0).await.unwrap();
        // The good line and the forward-compatible line both parse; only garbage is dropped.
        assert_eq!(entries.len(), 2);
    }

    /// An entry's identity must survive serialisation, because compaction matches
    /// identities held in memory against identities read back from the file.
    ///
    /// Regression guard for a real bug caught by
    /// `compaction_retains_only_named_identities_and_removes_empty_files` on this
    /// module's first run: the timestamp was encoded as whole seconds, so the two forms
    /// of the same entry were unequal and compaction retained **nothing**. That is a
    /// silent failure — it drops live entries and reports success — so it gets its own
    /// direct assertion rather than resting on the compaction test noticing.
    #[test]
    fn identity_survives_serialisation() {
        let original = entry("bucket/a.bin", 0, 4095, 1234, 7);
        let line = serde_json::to_string(&original).unwrap();
        let round_tripped: WriteLedgerEntry = serde_json::from_str(&line).unwrap();

        assert_eq!(
            original.identity(),
            round_tripped.identity(),
            "identity must be stable across serialisation or compaction retains nothing"
        );
        assert_eq!(original, round_tripped);
    }

    /// Renamed in intent as well as behaviour: a drained ledger file is now **kept** as a
    /// header-only file rather than removed, because the header carries the instance's
    /// `max_seq_ever` and removing the file would discard it. A later process reusing the
    /// same instance identity would then seed its sequence from 1 and re-issue numbers an
    /// installed fold cut has already passed, putting those credits in neither the fold nor
    /// the pending term.
    ///
    /// A file with no mark to preserve — legacy, no header and no seq-bearing entry — is
    /// still removed; that arm is covered by `unparseable_lines_are_skipped_not_fatal`'s
    /// sibling fixtures rather than here. Under the per-shard layout every file this binary
    /// creates carries a header from its first line, so "drained becomes header-only" is now the
    /// normal end state of a shard file rather than an edge case.
    #[tokio::test]
    async fn compaction_removes_only_named_identities_and_keeps_a_drained_file_for_its_mark() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        let keep = entry("bucket/keep.bin", 0, 9, 10, 20);
        let drop = entry("bucket/drop.bin", 0, 9, 10, 30);
        ledger.append(keep.clone()).await.unwrap();
        ledger.append(drop.clone()).await.unwrap();
        assert_eq!(ledger.count_entries().await, Some(2));

        // Compaction still VISITS every one of this instance's shard files, under each file's
        // lock, which is what makes "an entry this pass never read is never removed" a statement
        // about the volume. What it no longer does is WRITE the ones it changes nothing in.
        //
        // Derived from the production path builder rather than hardcoded, because whether these
        // two keys share an `l1` is not what this test is about — if they do, there is one file
        // and nothing to leave unchanged.
        let keep_file = ledger.shard_path_for_key("bucket/keep.bin").unwrap();
        let drop_file = ledger.shard_path_for_key("bucket/drop.bin").unwrap();
        let files: std::collections::BTreeSet<_> =
            [keep_file.clone(), drop_file.clone()].into_iter().collect();
        let file_count = files.len() as u64;

        let retire: HashSet<_> = [drop.identity()].into_iter().collect();
        let stats = ledger.retire_identities(&retire).await.unwrap();
        assert_eq!(stats.entries_before, 2);
        assert_eq!(stats.entries_after, 1);
        assert_eq!(stats.entries_dropped(), 1);
        // Exactly the file that held the named identity is rewritten; the rest are read and left
        // alone. `entries_before`/`entries_after` above are unaffected, which is the point: the
        // skipped file is still fully read and still fully counted.
        assert_eq!(
            stats.files_rewritten, 1,
            "only the file holding the named identity needs writing"
        );
        assert_eq!(
            stats.files_unchanged,
            file_count - 1,
            "and every other file is read under its lock and left byte-for-byte alone"
        );

        let remaining = ledger.read_merged_oldest_first(0).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].cache_key, "bucket/keep.bin");

        // Retiring the last entry drains the file of entries but KEEPS it, so its
        // `max_seq_ever` survives.
        let retire_rest: HashSet<_> = [keep.identity()].into_iter().collect();
        let stats = ledger.retire_identities(&retire_rest).await.unwrap();
        assert_eq!(
            stats.files_removed, 0,
            "a schema-3 file carries a high-water mark and must not be deleted"
        );
        assert_eq!(
            stats.files_rewritten, 1,
            "the file holding the last entry is rewritten to header-only form"
        );
        assert_eq!(
            stats.files_unchanged,
            file_count - 1,
            "and an ALREADY header-only file is left alone rather than rewritten to the same \
             single line every interval — which is where most of the measured 134-rewrites-to-\
             drop-2 cost came from"
        );
        assert_eq!(
            ledger.count_entries().await,
            Some(0),
            "no entries remain — the surviving line is the header, which is not an entry"
        );
        // The mark survived, which is the property the retention exists for. Asserted on
        // `max_seq_per_instance`, the value a cut is computed from, with the entries gone so
        // it can only be coming from the header.
        let drained = ledger.read_for_authority(0).await.unwrap();
        assert!(drained.entries.is_empty());
        assert_eq!(drained.max_seq_per_instance.get("host:1"), Some(&2));
        assert!(
            drained.is_complete(),
            "a header-only file is completely read, not a torn one"
        );
    }

    /// Retiring an empty set must be a no-op, not a wipe. Under the retain-set API the
    /// equivalent call (`rewrite_retaining(&HashSet::new())`) deleted every file, so
    /// the two APIs differ most sharply on the empty set — and the empty set is exactly
    /// what a pass that evicted nothing produces.
    #[tokio::test]
    async fn retiring_nothing_keeps_everything() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        ledger
            .append(entry("bucket/a.bin", 0, 9, 10, 20))
            .await
            .unwrap();
        ledger
            .append(entry("bucket/b.bin", 0, 9, 10, 30))
            .await
            .unwrap();

        let stats = ledger.retire_identities(&HashSet::new()).await.unwrap();
        assert_eq!(stats.entries_dropped(), 0);
        assert_eq!(stats.files_removed, 0);
        assert_eq!(
            stats.files_rewritten, 0,
            "and nothing may be WRITTEN either: an empty retire set changes no file, so every \
             file is read under its lock and left alone"
        );
        assert_eq!(
            ledger.count_entries().await,
            Some(2),
            "an empty retire set names nothing, so nothing may be removed"
        );
    }

    /// A ledger file holding none of the named identities is left **byte-for-byte** alone, while
    /// the file that does hold one is rewritten.
    ///
    /// # Why this is asserted on bytes and not on `files_rewritten`
    ///
    /// `files_rewritten` is the adjacent figure, not the predicate. A rewrite count can fall
    /// because a pass correctly skipped files it did not need to touch, or because it wrongly
    /// skipped a file that held a retired entry — and the second is the direction that leaves the
    /// ledger growing without bound, which is what compaction exists to prevent. So the surviving
    /// content is the verdict: the untouched file's bytes must be identical, and the touched file's
    /// must have lost exactly the named entry.
    ///
    /// This is the observable half of the compaction scope decision. A fleet measurement recorded
    /// **134 locked file rewrites to drop 2 entries**, rising with the shard-file population while
    /// the entries dropped fell — every file whose kept set was non-empty was rewritten whether or
    /// not it held anything named. The read stays whole-volume on purpose; only the write is
    /// conditional.
    ///
    /// A failure implicates the skip gate in [`WriteLedger::retire_identities`].
    #[tokio::test]
    async fn a_file_holding_no_named_identity_is_not_rewritten_at_all() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        // Two keys chosen so their shards differ, asserted rather than assumed — with one file
        // there is nothing to leave alone and the test would pass vacuously.
        let untouched_key = "bucket/untouched.bin";
        let compacted_key = "bucket/compacted.bin";
        let survivor = entry(untouched_key, 0, 9, 10, 20);
        let doomed = entry(compacted_key, 0, 9, 10, 30);
        ledger.append(survivor.clone()).await.unwrap();
        ledger.append(doomed.clone()).await.unwrap();

        let untouched = ledger.shard_path_for_key(untouched_key).unwrap();
        let compacted = ledger.shard_path_for_key(compacted_key).unwrap();
        assert_ne!(
            untouched, compacted,
            "fixture precondition: the two keys must land in different shard files, or there is \
             no unnamed file for the skip to spare"
        );
        let before = std::fs::read_to_string(&untouched).unwrap();

        assert!(
            before.contains("\"max_seq_ever\":0") && before.contains("\"seq\":1"),
            "fixture precondition: the untouched file must carry a STALE header — `append` writes \
             `max_seq_ever: 0` and never raises it. This is what makes the skip worth having: a \
             gate that also demanded a current header would rewrite every file with an \
             un-compacted append, i.e. exactly the population the fleet measured. Got: {before}"
        );

        let stats = ledger
            .retire_identities(&[doomed.identity()].into_iter().collect())
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(&untouched).unwrap(),
            before,
            "THE VERDICT: a file naming none of the retired identities must be byte-identical \
             afterwards — stale header included"
        );
        assert_eq!(
            ledger
                .read_for_authority(0)
                .await
                .unwrap()
                .max_seq_per_instance
                .get("host:1"),
            Some(&2),
            "and the stale header costs nothing, because the observed mark is max(header, \
             entries) and the entries were never removed. The header only has to be current once \
             entries are dropped, which happens under this same lock and raises it"
        );
        assert_eq!(stats.files_rewritten, 1);
        assert_eq!(stats.files_unchanged, 1);
        assert_eq!(
            stats.entries_before, 2,
            "the skipped file was still fully READ — the read stays whole-volume, which is what \
             makes an entry this pass never saw impossible to remove"
        );
        assert_eq!(stats.entries_dropped(), 1);

        // And the entry SET is exactly right on both sides of the skip.
        let remaining: Vec<_> = ledger
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| (e.cache_key, e.range_start, e.range_end))
            .collect();
        assert_eq!(
            remaining,
            vec![(untouched_key.to_string(), 0, 9)],
            "the named entry is gone and the unnamed one survives"
        );
    }

    /// TASK 77 — retiring a handful of entries must not delete the ones the caller
    /// never read.
    ///
    /// # The defect
    ///
    /// Found 2026-08-27 by the AWS Security Agent diff scan of `src/` against 2.6.3.
    /// `evict_staging_tier_locked` reads at most `STAGING_EVICTION_CANDIDATE_CAP`
    /// (10,000) entries, builds its identity set from that **post-truncation** vector,
    /// and passed `all_identities - retired` to a retain-set rewrite. Every entry beyond
    /// the cap was therefore absent from the retain set and deleted, having never been
    /// looked at — from **every instance's** ledger file, since the rewrite globs them
    /// all.
    ///
    /// The Write_Ledger is the only index of staged bytes driving staging eviction, and
    /// since Phase F `reserve_for_sizing` no longer refuses on capacity, so a truncated
    /// ledger means those staged objects are invisible to eviction and the tier grows
    /// with no control but the fail-open Disk_Safety_Bound.
    ///
    /// # Why the test above cannot catch it
    ///
    /// `compaction_retains_only_named_identities_and_removes_empty_files` uses two
    /// entries read with `cap = 0` — the one case where the caller's read is complete by
    /// construction. It asserts the retain-set semantics faithfully and is blind to the
    /// question of whether those semantics are safe for a caller that read a partial
    /// set. This test supplies the partial read the cap creates.
    ///
    /// # Shown failing first
    ///
    /// Measured 2026-08-27 by temporarily restoring the old `rewrite_retaining(retain)`
    /// and driving it exactly as the old caller did — capped read, then
    /// `all_read - retired` as the retain set. On this fixture **1 of 25 entries
    /// survived**: the 4 retired went as intended, and the 20 that were never read went
    /// with them, leaving only the single read-but-not-retired entry. That is the defect
    /// in one line, and it is why the assertion below is worth its length.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 2.6, 3.1
    #[tokio::test]
    async fn retiring_a_capped_candidate_set_preserves_the_entries_never_read() {
        const TOTAL: usize = 25;
        // Stands in for STAGING_EVICTION_CANDIDATE_CAP. The real constant is 10,000;
        // the property is about the read being PARTIAL, not about the specific bound,
        // and a 25-entry fixture makes the arithmetic readable.
        const SIMULATED_CAP: usize = 5;

        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        // Distinct ages so the oldest-first sort is deterministic: entry i is
        // (TOTAL - i) * 100 seconds old, so entry 0 is oldest and sorts first.
        for i in 0..TOTAL {
            ledger
                .append(entry(
                    &format!("bucket/obj-{:02}.bin", i),
                    0,
                    1023,
                    1024,
                    ((TOTAL - i) * 100) as u64,
                ))
                .await
                .unwrap();
        }
        assert_eq!(
            ledger.count_entries().await,
            Some(TOTAL as u64),
            "fixture: every append must have landed, or the cap arithmetic below is \
             measuring the wrong thing"
        );

        // Exactly what the eviction pass does: a CAPPED read.
        let candidates = ledger
            .read_merged_oldest_first(SIMULATED_CAP)
            .await
            .unwrap();
        assert_eq!(
            candidates.len(),
            SIMULATED_CAP,
            "fixture: the read must actually truncate. If it returned all {} entries \
             the caller's view would be complete and this test could not fail",
            TOTAL
        );

        // The pass evicts some of what it read and retires those entries. Four of the
        // five are deliberately left un-retired, so the assertion also covers the
        // read-but-not-walked case the old retain-set comment reasoned about.
        let retired: HashSet<_> = candidates.iter().take(4).map(|e| e.identity()).collect();

        let stats = ledger.retire_identities(&retired).await.unwrap();

        assert_eq!(
            stats.entries_dropped(),
            4,
            "exactly the retired entries may be dropped"
        );
        assert_eq!(
            ledger.count_entries().await,
            Some((TOTAL - 4) as u64),
            "the {} entries beyond the simulated cap were never read and must survive. \
             A count of {} means the retain-set rewrite treated 'not in the set I \
             happened to read' as 'delete', so the ledger loses every entry past the \
             cap — and with it every staged object staging eviction could have \
             reclaimed",
            TOTAL - SIMULATED_CAP,
            SIMULATED_CAP - 4
        );

        // Named survivors, so a count that happens to match cannot pass this.
        let remaining: HashSet<String> = ledger
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.cache_key)
            .collect();
        assert!(
            remaining.contains("bucket/obj-04.bin"),
            "the fifth entry was read but NOT retired, so it must survive"
        );
        assert!(
            remaining.contains("bucket/obj-24.bin"),
            "the newest entry is far beyond the cap and must survive"
        );
        for i in 0..4 {
            let key = format!("bucket/obj-{:02}.bin", i);
            assert!(
                !remaining.contains(&key),
                "{} was retired and must be gone",
                key
            );
        }
    }

    /// Retiring across a second instance's file, which is the case that makes the
    /// blast radius fleet-wide rather than local: `ledger_files()` globs every
    /// `*.ledger` in the directory, so a rewrite driven by one instance's partial read
    /// reaches files it never read from.
    #[tokio::test]
    async fn retiring_touches_other_instances_files_only_for_named_identities() {
        let tmp = TempDir::new().unwrap();
        let mine = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        let theirs = WriteLedger::new(tmp.path().to_path_buf(), "host:2".to_string());

        let a = entry("bucket/mine.bin", 0, 1023, 1024, 300);
        let b = entry("bucket/theirs-keep.bin", 0, 1023, 1024, 200);
        let c = entry("bucket/theirs-retire.bin", 0, 1023, 1024, 100);
        mine.append(a.clone()).await.unwrap();
        theirs.append(b.clone()).await.unwrap();
        theirs.append(c.clone()).await.unwrap();
        assert_eq!(mine.count_entries().await, Some(3));

        // Retire one entry that lives in the OTHER instance's file.
        let retired: HashSet<_> = [c.identity()].into_iter().collect();
        mine.retire_identities(&retired).await.unwrap();

        let remaining: HashSet<String> = mine
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.cache_key)
            .collect();
        assert_eq!(
            remaining.len(),
            2,
            "only the one named identity may go, whichever file holds it"
        );
        assert!(remaining.contains("bucket/mine.bin"));
        assert!(
            remaining.contains("bucket/theirs-keep.bin"),
            "an entry in another instance's file that was not named must survive — \
             this is the fleet-wide half of the defect"
        );
        assert!(!remaining.contains("bucket/theirs-retire.bin"));
    }

    /// An entry appended after the caller's read must survive the retire, because a
    /// retire names what to remove rather than what to keep. Under the retain-set
    /// rewrite this entry was read by the rewrite's own fresh re-read, found absent
    /// from the retain set, and dropped — a lost append with no lock covering the
    /// read-decide-rename window.
    #[tokio::test]
    async fn an_append_racing_the_retire_is_not_lost() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        let old = entry("bucket/old.bin", 0, 1023, 1024, 500);
        ledger.append(old.clone()).await.unwrap();

        // The caller reads here, seeing only `old`.
        let seen = ledger.read_merged_oldest_first(0).await.unwrap();
        assert_eq!(seen.len(), 1);

        // A concurrent upload appends while the pass is still working.
        ledger
            .append(entry("bucket/raced.bin", 0, 1023, 1024, 1))
            .await
            .unwrap();

        // The pass now retires what it evicted.
        let retired: HashSet<_> = [old.identity()].into_iter().collect();
        ledger.retire_identities(&retired).await.unwrap();

        let remaining: HashSet<String> = ledger
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.cache_key)
            .collect();
        assert_eq!(
            remaining,
            ["bucket/raced.bin".to_string()].into_iter().collect(),
            "the entry appended after the caller's read must survive: it was never a \
             candidate, so nothing decided it should go"
        );
    }

    /// An append that commits **inside** `retire_identities`' own read-to-rename window
    /// must survive it.
    ///
    /// # Why this is not the test above
    ///
    /// `an_append_racing_the_retire_is_not_lost` places its racing append *before*
    /// `retire_identities` is entered, so the retire's own read sees it and it survives
    /// however the lock is scoped. That test pins the caller-side window and the
    /// retire-set inversion, both of which are real — but it passes identically against a
    /// build with no lock over the read, so it is no evidence about this window. This test
    /// puts the append strictly between the read and the rename.
    ///
    /// # How the interleaving is made deterministic
    ///
    /// Timing alone cannot do it: after both passes are unblocked, whichever wins the lock
    /// decides the outcome, and if the retire wins the appended entry survives even on the
    /// buggy build. So the ordering is pinned with the lock itself:
    ///
    /// 1. the test acquires the ledger file's `fs4` lock and holds it;
    /// 2. `retire_identities` is spawned. On the buggy build it reads the file immediately
    ///    — the read is not under the lock — and only then blocks. On the fixed build it
    ///    blocks before reading anything;
    /// 3. the test appends a line, then releases the lock;
    /// 4. the retire proceeds.
    ///
    /// The append at step 3 is written with a raw `O_APPEND` write rather than through
    /// [`WriteLedger::append`], because `append` takes the very lock this test is holding
    /// and would deadlock. The bytes are the same bytes `append` writes — the same
    /// `serde_json` line plus a newline — so what is being appended is not a fixture
    /// artefact. What the lock hold stands in for is a real appender whose commit landed
    /// inside the window; the whole purpose of the lock is that such a commit must not be
    /// lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_append_committing_inside_the_retire_window_is_not_lost() {
        use std::sync::Arc;

        let tmp = TempDir::new().unwrap();
        let ledger = Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));

        let old = entry("bucket/old.bin", 0, 1023, 1024, 500);
        ledger.append(old.clone()).await.unwrap();

        let path = ledger.shard_path_for_key("bucket/old.bin").unwrap();
        let lock_path = WriteLedger::lock_path_for(&path);

        // Step 1: hold the file's lock, so the retire cannot reach its rename.
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::lock(&lock_file).unwrap();

        // Step 2: the retire starts. Buggy build: reads now, outside the lock.
        let retired: HashSet<_> = [old.identity()].into_iter().collect();
        let ledger_for_pass = Arc::clone(&ledger);
        let pass = tokio::spawn(async move { ledger_for_pass.retire_identities(&retired).await });

        // Long enough that a build reading outside the lock has certainly read by now.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Step 3: an append commits inside the window, byte-identical to a real one.
        let raced = entry("bucket/raced.bin", 0, 1023, 1024, 1);
        {
            use std::io::Write;
            let mut line = serde_json::to_string(&raced).unwrap();
            line.push('\n');
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(line.as_bytes()).unwrap();
            f.flush().unwrap();
        }
        fs4::FileExt::unlock(&lock_file).unwrap();
        drop(lock_file);

        // Step 4.
        pass.await.unwrap().unwrap();

        let remaining: HashSet<String> = ledger
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.cache_key)
            .collect();

        assert!(
            remaining.contains("bucket/raced.bin"),
            "an append that committed inside the retire's read-to-rename window was \
             erased by the whole-file rewrite: the read must be inside the same lock hold \
             as the rename. remaining={:?}",
            remaining
        );
        assert!(
            !remaining.contains("bucket/old.bin"),
            "the named identity must still be retired"
        );
    }

    /// The authority read reports what it could not see, so an unreadable ledger and an
    /// empty one are different values.
    #[tokio::test]
    async fn authority_read_distinguishes_complete_empty_from_unreadable() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        // No ledger directory at all: complete, and empty.
        let empty = ledger.read_for_authority(0).await.unwrap();
        assert!(empty.entries.is_empty());
        assert_eq!(empty.files_expected, 0);
        assert!(
            empty.is_complete(),
            "a ledger that has never been written is completely read"
        );

        ledger
            .append(entry("bucket/a.bin", 0, 9, 10, 30))
            .await
            .unwrap();
        ledger
            .append(entry("bucket/b.bin", 0, 9, 10, 10))
            .await
            .unwrap();

        let full = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(full.entries.len(), 2);
        // One file per distinct SHARD per instance, derived from the production path builder
        // rather than hardcoded: the two keys may or may not hash to the same `l1`, and which
        // it is is not what this test is about.
        let expected_files = [
            ledger.shard_path_for_key("bucket/a.bin").unwrap(),
            ledger.shard_path_for_key("bucket/b.bin").unwrap(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
        assert_eq!(full.files_expected, expected_files);
        assert_eq!(full.files_read, expected_files);
        assert!(full.is_complete());
        // Oldest-first, as the hint reader orders it.
        assert_eq!(full.entries[0].cache_key, "bucket/a.bin");
        // One instance, and its position is the highest `Ledger_Seq` it issued — read off
        // the entry, NOT computed from the entry's timestamp. A timestamp-derived
        // expectation is what this assertion checked until the coordinate was found to be
        // non-monotonic; recomputing it here would let the same substitution back in
        // through the test.
        assert_eq!(full.max_seq_per_instance.len(), 1);
        let newest = full
            .entries
            .iter()
            .filter_map(ledger_seq)
            .max()
            .expect("both entries were appended by this binary, so both carry a seq");
        assert_eq!(full.max_seq_per_instance.get("host:1"), Some(&newest));
        // And the sequence is a counter starting at 1, not a clock reading.
        assert_eq!(newest, 2, "two appends, so seqs 1 and 2");

        // A discarded line makes the read incomplete rather than merely shorter.
        let path = ledger.shard_path_for_key("bucket/a.bin").unwrap();
        let mut content = tokio::fs::read_to_string(&path).await.unwrap();
        content.push_str("this is not json\n");
        tokio::fs::write(&path, content).await.unwrap();

        let torn = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(torn.entries.len(), 2, "the good lines still parse");
        assert_eq!(torn.malformed_lines, 1);
        assert!(
            !torn.is_complete(),
            "a discarded line must make the read refuse to be an authority"
        );

        // Truncation is signalled rather than silent.
        let capped = ledger.read_for_authority(1).await.unwrap();
        assert_eq!(capped.entries.len(), 1);
        assert!(capped.truncated);
        assert!(!capped.is_complete());
    }

    /// A ledger file that cannot be read must be left alone, not deleted.
    ///
    /// The read used to be outside the lock and returned a bare `Vec`, so a read failure
    /// produced zero entries, hence an empty keep set, hence `remove_file` — a whole
    /// file's entries erased without having been read, reported as a successful
    /// compaction.
    #[tokio::test]
    #[cfg(unix)]
    async fn an_unreadable_ledger_file_is_not_deleted() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        ledger
            .append(entry("bucket/keep.bin", 0, 9, 10, 30))
            .await
            .unwrap();

        let path = ledger.shard_path_for_key("bucket/keep.bin").unwrap();
        let original = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        // Root ignores the mode bits, so there is no unreadable file to provoke anything
        // with. Restore and skip rather than assert something the fixture cannot create.
        if std::fs::read_to_string(&path).is_ok() {
            std::fs::set_permissions(&path, original).unwrap();
            return;
        }

        // Nothing is named for retirement, so on a working build this is a no-op anyway;
        // the point is what the unreadable file provokes.
        let retire: HashSet<(String, u64, u64, SystemTime, String)> = HashSet::new();
        let stats = ledger.retire_identities(&retire).await.unwrap();

        // Best-effort: on a build that deleted the file there is nothing to chmod back,
        // and the assertion below is the diagnosis, not this `unwrap`.
        let _ = std::fs::set_permissions(&path, original);

        assert!(
            path.exists(),
            "a ledger file that could not be read must be left untouched"
        );
        assert_eq!(
            stats.files_removed, 0,
            "an unreadable file must not be reported as a successful compaction"
        );
        assert_eq!(ledger.count_entries().await, Some(1));
    }

    // ── LEDGER-ENTRIES-SCRAPE-COST ──────────────────────────────────────────────────────────
    //
    // `count_entries` used to traverse the whole volume on every `/metrics` scrape. These six
    // assertions pin the replacement: a bounded-age observation, donated by the compaction pass
    // that already traverses, with all three of UNREADABLE / real-zero / real-count still
    // distinguishable.
    //
    // FIXTURE SHAPE IS LOAD-BEARING. Every one of these seeds several buckets, several L1 shard
    // directories and more than one instance, because the cost being removed is
    // `1 + B + (B × 256)` listings and `B × 256 × instances` reads. A one-bucket, one-instance
    // fixture would exercise neither term, and an assertion that the traversal is gone would be
    // asserting it about a traversal that never happened.

    /// Seed a ledger file the way a PEER INSTANCE would: written straight to disk at the
    /// shard path, header line first, with no help from our own `append`.
    ///
    /// Bypassing `append` is the point rather than a shortcut. Five of the paths that mutate the
    /// counted set are in other processes, and this is what three of them look like from here.
    fn seed_peer_shard_ledger(
        cache_dir: &Path,
        bucket: &str,
        l1: &str,
        instance: &str,
        entries: u64,
    ) -> PathBuf {
        let dir = WriteLedger::shard_dir(cache_dir, bucket, l1);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.{}", instance, LEDGER_EXT));
        let mut body = serde_json::to_string(&LedgerHeader {
            schema: LEDGER_SCHEMA,
            max_seq_ever: 0,
        })
        .unwrap();
        body.push('\n');
        for i in 0..entries {
            let mut e = entry(
                &format!("{}/{}-{}-{}.bin", bucket, l1, instance, i),
                0,
                99,
                100,
                60,
            );
            e.instance_id = instance.to_string();
            e.seq = Some(i + 1);
            body.push_str(&serde_json::to_string(&e).unwrap());
            body.push('\n');
        }
        std::fs::write(&path, body).unwrap();
        path
    }

    /// Three buckets × two L1 shards × two instances = 12 files, 5 entries each = 60 entries.
    fn seed_multi_shard_multi_instance(cache_dir: &Path) -> u64 {
        let mut total = 0;
        for bucket in ["bucket-a", "bucket-b", "bucket-c"] {
            for l1 in ["ab", "cd"] {
                for instance in ["hostA:1", "hostB:2"] {
                    seed_peer_shard_ledger(cache_dir, bucket, l1, instance, 5);
                    total += 5;
                }
            }
        }
        total
    }

    /// Within the max age the figure is REPORTED, not re-counted.
    ///
    /// Asserted by mutating the volume out of band — a peer instance adding a whole new shard
    /// file, mutating path 5 — and requiring the published figure not to move. That reads exactly
    /// the value the short-circuit in `count_entries` reads, and its failure would be caused by
    /// that branch: with the branch deleted the call traverses, sees the peer's file, and reports
    /// the larger number.
    #[tokio::test]
    async fn a_fresh_entry_count_observation_is_reported_rather_than_recounted() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "hostA:1".to_string());
        let seeded = seed_multi_shard_multi_instance(tmp.path());

        assert_eq!(
            ledger.count_entries().await,
            Some(seeded),
            "the first call has no observation, so it must take one"
        );
        let age = ledger.entry_count_age_secs().await;
        assert!(
            age.is_some(),
            "an observation was just taken, so its age must be publishable"
        );

        // A peer adds a shard file. Nothing in this process knows.
        seed_peer_shard_ledger(tmp.path(), "bucket-d", "ef", "hostC:3", 7);

        assert_eq!(
            ledger.count_entries().await,
            Some(seeded),
            "within LEDGER_COUNT_MAX_AGE_SECS the observation must be reported, not re-taken — \
             a peer's out-of-band append must not be visible yet"
        );
    }

    /// Past the max age the figure IS re-counted.
    ///
    /// The other side of the same branch. A deliberately wrong installed count means the
    /// assertion cannot pass by the real count happening to match.
    #[tokio::test]
    async fn an_observation_past_the_max_age_is_re_counted() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "hostA:1".to_string());
        let seeded = seed_multi_shard_multi_instance(tmp.path());

        ledger
            .install_entry_count_observation_aged(
                999_999,
                Duration::from_secs(LEDGER_COUNT_MAX_AGE_SECS + 1),
            )
            .await;

        assert_eq!(
            ledger.count_entries().await,
            Some(seeded),
            "an observation older than LEDGER_COUNT_MAX_AGE_SECS must be replaced by a fresh \
             traversal, not reported"
        );
        assert_eq!(
            ledger.entry_count_age_secs().await,
            Some(0),
            "the age must describe the observation that was just published"
        );
    }

    /// Compaction DONATES its figure, so the scrape path reports the post-compaction length
    /// immediately even though its own observation is still inside the max age.
    ///
    /// This is the assertion that makes the traversal genuinely disappear rather than merely
    /// becoming less frequent, and it is also the answer to "assert the count after each
    /// mutating path, not only after an append": the mutation here is `retire_identities`.
    ///
    /// Its failure would be caused by the `files_unreadable == 0` donation at the end of
    /// `retire_identities` — with the donation removed, the still-fresh observation is served and
    /// the pre-compaction length is reported.
    #[tokio::test]
    async fn compaction_donates_its_count_so_the_scrape_path_does_not_traverse() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "hostA:1".to_string());
        let seeded = seed_multi_shard_multi_instance(tmp.path());

        // Populate the observation, so anything the compaction does NOT donate would be masked
        // by a fresh memo rather than re-counted.
        assert_eq!(ledger.count_entries().await, Some(seeded));

        // Retire one entry belonging to a PEER's file, which compaction is entitled to do.
        let all = ledger.read_merged_oldest_first(0).await.unwrap();
        let victim = all
            .iter()
            .find(|e| e.instance_id == "hostB:2")
            .expect("the fixture seeds hostB:2");
        let mut retire = HashSet::new();
        retire.insert(victim.identity());

        let stats = ledger.retire_identities(&retire).await.unwrap();
        assert_eq!(
            stats.entries_dropped(),
            1,
            "fixture precondition: the pass must actually have dropped the named entry"
        );

        assert_eq!(
            ledger.count_entries().await,
            Some(seeded - 1),
            "compaction must donate the length it already computed, so the gauge reflects the \
             post-compaction volume without the scrape path traversing it"
        );
    }

    /// A compaction pass that could not read a file DONATES NOTHING, so a short count cannot be
    /// installed as a real one.
    ///
    /// `entries_after` sums only the files the pass succeeded on, and short is the reassuring
    /// direction for a length gauge — the exact shape of the sentinel defect `count_entries`
    /// returns `Option` for. The discriminator is two clearly different values rather than a
    /// margin: `None` (correct, the gauge falls back to its own traversal and that traversal hits
    /// the same unreadable file) against `Some(readable_only)`.
    ///
    /// Its failure would be caused by the `files_unreadable == 0` guard on the donation.
    #[tokio::test]
    #[cfg(unix)]
    async fn a_compaction_pass_that_could_not_read_a_file_donates_nothing() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "hostA:1".to_string());
        seed_peer_shard_ledger(tmp.path(), "bucket-a", "ab", "hostA:1", 3);
        let blocked = seed_peer_shard_ledger(tmp.path(), "bucket-b", "cd", "hostB:2", 4);

        // Stale, so nothing can be served from the observation and the verdict comes from this
        // pass alone.
        ledger
            .install_entry_count_observation_aged(
                0,
                Duration::from_secs(LEDGER_COUNT_MAX_AGE_SECS + 1),
            )
            .await;

        let original = std::fs::metadata(&blocked).unwrap().permissions();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Root ignores the mode bits, so there is no unreadable file to provoke anything with.
        // Same apparatus carve-out as `an_unreadable_ledger_file_is_not_deleted`.
        if std::fs::read_to_string(&blocked).is_ok() {
            std::fs::set_permissions(&blocked, original).unwrap();
            return;
        }

        let _ = ledger.retire_identities(&HashSet::new()).await;
        let counted = ledger.count_entries().await;
        let _ = std::fs::set_permissions(&blocked, original);

        assert_eq!(
            counted, None,
            "a pass that could not read one of two ledger files must donate nothing, so the \
             gauge reports UNREADABLE rather than the 3 entries it could see"
        );
    }

    /// The three states stay distinguishable, and a stale observation never masks the UNREADABLE
    /// one.
    ///
    /// The last arm is the one worth having: a real count is taken, aged out, and the volume is
    /// then broken. Reporting the aged `Some` there would be the memo converting a visible
    /// failure into an invisible staleness — precisely what publishing a bounded-age figure must
    /// not be allowed to do.
    #[tokio::test]
    async fn a_real_zero_and_an_unreadable_volume_stay_distinguishable() {
        // Real zero: a readable volume with nothing in it.
        let empty = TempDir::new().unwrap();
        let drained = WriteLedger::new(empty.path().to_path_buf(), "hostA:1".to_string());
        assert_eq!(
            drained.count_entries().await,
            Some(0),
            "a readable, drained ledger is Some(0) and must never be None"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let tmp = TempDir::new().unwrap();
            let ledger = WriteLedger::new(tmp.path().to_path_buf(), "hostA:1".to_string());
            let seeded = seed_multi_shard_multi_instance(tmp.path());
            assert_eq!(ledger.count_entries().await, Some(seeded));

            // A bucket directory that exists and cannot be listed makes `subdirs_of` fail, which
            // is the listing-failure arm.
            let bucket_dir = WriteLedger::shard_dir(tmp.path(), "bucket-a", "ab")
                .parent()
                .unwrap()
                .to_path_buf();
            let original = std::fs::metadata(&bucket_dir).unwrap().permissions();
            std::fs::set_permissions(&bucket_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
            if std::fs::read_dir(&bucket_dir).is_ok() {
                std::fs::set_permissions(&bucket_dir, original).unwrap();
                return;
            }

            // Age the good observation out. Without this the memo would legitimately serve it.
            ledger
                .install_entry_count_observation_aged(
                    seeded,
                    Duration::from_secs(LEDGER_COUNT_MAX_AGE_SECS + 1),
                )
                .await;

            let counted = ledger.count_entries().await;
            let _ = std::fs::set_permissions(&bucket_dir, original);

            assert_eq!(
                counted, None,
                "an unreadable volume must publish the UNREADABLE sentinel, not a stale count \
                 that happens to still be in memory"
            );
        }
    }

    #[tokio::test]
    async fn grouping_collapses_an_objects_parts_into_one_candidate() {
        let entries = vec![
            entry("bucket/mpu.bin", 0, 99, 100, 30),
            entry("bucket/mpu.bin", 100, 199, 100, 29),
            entry("bucket/other.bin", 0, 9, 10, 28),
        ];

        let candidates = WriteLedger::group_by_key(entries);
        assert_eq!(candidates.len(), 2);

        // Oldest-first preserved: the multipart object's earliest part decides its place.
        assert_eq!(candidates[0].cache_key, "bucket/mpu.bin");
        assert_eq!(candidates[0].identities.len(), 2);
        assert!(candidates[0].newest_entry_at > candidates[0].oldest_entry_at);
        assert_eq!(candidates[1].cache_key, "bucket/other.bin");
    }

    /// Design test 4, first half: expired-unread ranks ahead of fresh-unread, and
    /// oldest-first order survives inside each group.
    ///
    /// Shown failing first against `expired.into_iter().chain(fresh)` reversed to
    /// `fresh.into_iter().chain(expired)`, which reds the first assertion.
    ///
    /// The fixture supplies the input **out of order** — interleaved across the two
    /// groups, and not oldest-first within either. An implementation that returned its
    /// input unchanged would pass a fixture that was already correctly ordered, so the
    /// scrambling is what gives the assertion teeth.
    ///
    /// This is also the assertion that found the ordering guarantee was conditional: it
    /// failed against the original `partition`-only implementation with
    /// `["expired-old", "expired-new", "fresh-new", "fresh-old"]` — the two groups
    /// correctly separated, but input order preserved *inside* the fresh group rather
    /// than oldest-first. The requirement held in production only because the caller
    /// happened to pre-sort. See `order_staging_candidates`.
    #[test]
    fn ordering_puts_expired_unread_before_fresh_unread_oldest_first_within_each() {
        let put_ttl = Duration::from_secs(3600);
        let watermark = SystemTime::now() - put_ttl;

        // secs_ago relative to now: > 3600 is expired, < 3600 is fresh.
        let candidates = WriteLedger::group_by_key(vec![
            entry("bucket/expired-old", 0, 99, 100, 7200),
            entry("bucket/fresh-new", 0, 99, 100, 60),
            entry("bucket/expired-new", 0, 99, 100, 5400),
            entry("bucket/fresh-old", 0, 99, 100, 1800),
        ]);
        assert_eq!(candidates.len(), 4, "fixture: four distinct keys");

        let ordered = order_staging_candidates(candidates, watermark);
        let keys: Vec<&str> = ordered.iter().map(|c| c.cache_key.as_str()).collect();

        assert_eq!(
            keys,
            vec![
                "bucket/expired-old",
                "bucket/expired-new",
                "bucket/fresh-old",
                "bucket/fresh-new",
            ],
            "expired-unread must precede fresh-unread, oldest-first within each group"
        );
    }

    /// Design test 4, second half, and the property that is easy to lose: a fresh-unread
    /// candidate is NOT exempt from eviction, only deprioritised.
    ///
    /// Asserted with an all-fresh input, where a filtering implementation returns an
    /// empty list while a partitioning one returns everything. Without this, replacing
    /// the partition with `candidates.retain(|c| c.oldest_entry_at < watermark)` would
    /// pass the test above and leave the staging tier unable to reclaim anything
    /// whenever every entry is younger than `put_ttl` — the steady state of a
    /// write-heavy fleet, so the failure would be both silent and permanent.
    #[test]
    fn ordering_deprioritises_fresh_candidates_rather_than_exempting_them() {
        let watermark = SystemTime::now() - Duration::from_secs(3600);

        let candidates = WriteLedger::group_by_key(vec![
            entry("bucket/fresh-a", 0, 99, 100, 300),
            entry("bucket/fresh-b", 0, 99, 100, 120),
        ]);

        let ordered = order_staging_candidates(candidates, watermark);

        assert_eq!(
            ordered.len(),
            2,
            "every candidate must survive ordering: fresh entries are ranked last, \
             never filtered out, or a tier whose entries are all younger than put_ttl \
             could never reclaim anything"
        );
        assert_eq!(ordered[0].cache_key, "bucket/fresh-a", "oldest first");
    }

    /// The watermark boundary is `<`, so a candidate exactly at it counts as fresh.
    /// Pinned because the comparison is one character and flipping it would move a
    /// candidate between groups with no other test noticing.
    #[test]
    fn a_candidate_exactly_at_the_watermark_is_fresh() {
        let mut candidates = WriteLedger::group_by_key(vec![
            entry("bucket/at-watermark", 0, 99, 100, 0),
            entry("bucket/one-second-older", 0, 99, 100, 0),
        ]);
        // Set the two timestamps explicitly rather than via `secs_ago`, so the boundary
        // is exact rather than approximately one second wide.
        let watermark = SystemTime::now();
        candidates[0].oldest_entry_at = watermark;
        candidates[1].oldest_entry_at = watermark - Duration::from_secs(1);

        let ordered = order_staging_candidates(candidates, watermark);

        assert_eq!(
            ordered[0].cache_key, "bucket/one-second-older",
            "strictly-older-than-watermark is expired; exactly-at-watermark is not, so \
             flipping the comparison to `<=` would move a candidate between groups"
        );
    }

    /// The supersede comparison must be strict, so a clock too coarse to separate the
    /// `.meta` write from the ledger append reads as "not superseded" and keeps the
    /// entry. Dropping it instead would hide a live staged object from eviction.
    #[test]
    fn supersede_check_is_strict_so_equal_timestamps_keep_the_entry() {
        let t = SystemTime::now();
        assert!(!is_superseded(t, t));
        assert!(!is_superseded(t, t - Duration::from_secs(1)));
        assert!(is_superseded(t, t + Duration::from_secs(1)));
    }

    // =====================================================================
    // Lazy verification (R2.2) — the decision that makes entries safe as hints
    // =====================================================================

    /// Write a `.meta` for `cache_key` at its sharded path.
    async fn seed_meta(
        cache_dir: &std::path::Path,
        cache_key: &str,
        is_write_cached: bool,
        range_created_at: SystemTime,
        with_range: bool,
    ) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();

        let ranges = if with_range {
            vec![crate::cache_types::RangeSpec {
                start: 0,
                end: 9,
                file_path: format!("{}_0-9.bin", cache_key.replace('/', "%2F")),
                compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                compressed_size: 10,
                uncompressed_size: 10,
                created_at: range_created_at,
                last_accessed: range_created_at,
                access_count: 0,
                staged: None,
                unservable: false,
                file_identity: None,
            }]
        } else {
            Vec::new()
        };

        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                is_write_cached,
                ..Default::default()
            },
            ranges,
            created_at: range_created_at,
            ..Default::default()
        };
        tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
            .await
            .unwrap();
    }

    fn candidate_for(e: &WriteLedgerEntry) -> StagedCandidate {
        WriteLedger::group_by_key(vec![e.clone()]).remove(0)
    }

    /// A candidate whose `.meta` is still flagged staged and not superseded is evictable.
    /// The positive control for the four negative cases below — without it, a verifier
    /// that returned a skip for everything would pass all of them.
    #[tokio::test]
    async fn verify_says_evictable_for_a_live_staged_entry() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/live.bin", 0, 9, 10, 10);
        // `.meta` written before the ledger entry, which is the real ordering.
        seed_meta(
            tmp.path(),
            &e.cache_key,
            true,
            e.timestamp - Duration::from_secs(1),
            true,
        )
        .await;

        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate_for(&e))
                .await
                .verdict,
            StagedCandidateVerdict::Evictable
        );
    }

    /// Absent `.meta` — already evicted, or the append outlived the object.
    #[tokio::test]
    async fn verify_skips_when_metadata_is_absent() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/gone.bin", 0, 9, 10, 10);

        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate_for(&e))
                .await
                .verdict,
            StagedCandidateVerdict::MetadataAbsent
        );
    }

    /// Graduated: the first read cleared the flag and the `Graduation` journal entry has
    /// already debited these bytes. Evicting here would debit them twice, which drives
    /// `write_cache_size` to undershoot — the dangerous direction, because it silently
    /// over-admits rather than refusing.
    #[tokio::test]
    async fn verify_skips_a_graduated_entry() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/graduated.bin", 0, 9, 10, 10);
        seed_meta(
            tmp.path(),
            &e.cache_key,
            false,
            e.timestamp - Duration::from_secs(1),
            true,
        )
        .await;

        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate_for(&e))
                .await
                .verdict,
            StagedCandidateVerdict::Graduated
        );
    }

    /// Superseded: a re-PUT replaced the copy this entry describes, so the entry refers to
    /// bytes that are already gone. Acting on it would evict the *new* copy.
    /// A `.meta` with an arbitrary set of ranges, each with its own `created_at`.
    ///
    /// [`seed_meta`] writes at most one range, fixed at `(0, 9)`. That is enough for every
    /// object-level verdict, and it is **structurally unable** to exercise anything range-scoped: a
    /// one-range object has one entry, so per-key and per-range retirement are the same set and a
    /// test built on it passes whichever the code does. Loss mode L6 is a statement about an
    /// object's *other* extents, so it needs at least two.
    /// `staged` is the per-range recorded membership (`RangeSpec::staged`). `None` is the
    /// pre-R12.2 shape, where the predicate falls back to the object flag — which means a fixture
    /// passing `None` **cannot** put the two into disagreement, and disagreement is exactly the
    /// state that separates object-scoped from range-scoped retirement.
    async fn seed_meta_ranges(
        cache_dir: &std::path::Path,
        cache_key: &str,
        is_write_cached: bool,
        staged: Option<bool>,
        ranges: &[(u64, u64, SystemTime)],
    ) {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();
        let oldest = ranges
            .iter()
            .map(|(_, _, at)| *at)
            .min()
            .unwrap_or_else(SystemTime::now);
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                is_write_cached,
                ..Default::default()
            },
            ranges: ranges
                .iter()
                .map(|(start, end, created_at)| crate::cache_types::RangeSpec {
                    start: *start,
                    end: *end,
                    file_path: format!("r_{start}-{end}.bin"),
                    compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                    compressed_size: 512,
                    uncompressed_size: 512,
                    created_at: *created_at,
                    last_accessed: *created_at,
                    access_count: 0,
                    staged,
                    unservable: false,
                    file_identity: None,
                })
                .collect(),
            created_at: oldest,
            ..Default::default()
        };
        tokio::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap())
            .await
            .unwrap();
    }

    /// **Loss mode L5.** A torn ledger line SURVIVES compaction in a quarantine file instead of
    /// being discarded, and the ledger file it came from becomes readable again.
    ///
    /// # The defect
    ///
    /// A torn line is an incompleteness the reader **reports**: it feeds
    /// [`LedgerReadOutcome::malformed_lines`], so `is_complete()` is false and the staged fold
    /// retains the previous figure rather than installing one it knows is short. Compaction then
    /// rebuilt the file from the successfully parsed entries only, with nothing but a WARN. The next
    /// read saw a clean file, `is_complete()` returned true, and the fold installed a figure
    /// permanently missing the torn entry's bytes. **A reported incompleteness became silent
    /// permanent loss**, and in the direction § 16.6.2 rules out by name: a lower `F` makes the
    /// staging eviction trigger fire late.
    ///
    /// # Both halves are asserted, because either one alone is a different bug
    ///
    /// Arm 1 is the fixture precondition: the file must actually read as incomplete before
    /// compaction. Without it a green run could mean the parser never classified the line as torn,
    /// and the test would be quarantining nothing while passing.
    ///
    /// Arm 2 asserts the line is **in the quarantine file, verbatim** — the evidence survives.
    ///
    /// Arm 3 asserts the compacted ledger reads as **complete** — the deadlock is cleared. This is
    /// the half that rules out the two obvious alternatives: carrying the line through into the
    /// rewritten ledger, or refusing to compact the file at all, would both leave `is_complete()`
    /// false forever, and a permanently-false `is_complete()` stops the repair rotation, which is
    /// the only mechanism that can restore the lost coverage. Loud and permanently stuck is not
    /// better than the bug.
    ///
    /// Arm 4 asserts the surviving entries were not collateral damage, and arm 5 that the loss is
    /// counted rather than only logged.
    ///
    /// A failure in arm 2 or 5 implicates the `Self::quarantine_torn_lines(...)` call in
    /// `retire_identities`'s blocking closure, or `ParsedLedgerFile::malformed_lines_text` not being
    /// populated by `parse_content`. A failure in arm 3 implicates `torn_path_for` — a quarantine
    /// file that `ledger_files()` picks up would be re-counted as malformed on every read.
    #[tokio::test]
    async fn a_torn_ledger_line_is_quarantined_by_compaction_rather_than_discarded() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        let path = ledger.shard_path_for_key("bucket/torn.bin").unwrap();
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();

        // A file holding two good entries around one torn line. Written by hand because the append
        // path cannot produce a torn line — that is the point of the mode: it is what a partial
        // write leaves behind.
        //
        // The torn line is NON-BLANK deliberately. A blank line is also counted as malformed, but
        // there is nothing in it to preserve, so a blank-line fixture would exercise the counting
        // and not the quarantine.
        const TORN: &str = r#"{"cache_key":"bucket/lost.bin","range_start":0,"range_e"#;
        let mut first = entry("bucket/kept-a.bin", 0, 1023, 512, 300);
        first.seq = Some(1);
        let mut second = entry("bucket/kept-b.bin", 0, 1023, 512, 240);
        second.seq = Some(2);
        let body = format!(
            "{}\n{}\n{}\n",
            serde_json::to_string(&first).unwrap(),
            TORN,
            serde_json::to_string(&second).unwrap()
        );
        tokio::fs::write(&path, &body).await.unwrap();

        // ---- Arm 1: the fixture precondition. The file must read as INCOMPLETE.
        let before = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(
            before.malformed_lines, 1,
            "fixture precondition: the torn line must be classified as malformed, or nothing \
             downstream is being exercised"
        );
        assert!(
            !before.is_complete(),
            "fixture precondition: a torn line must make the authority read incomplete — that is \
             the report compaction was erasing"
        );
        assert_eq!(
            before.entries.len(),
            2,
            "and both good entries must be read"
        );

        let quarantined_before = torn_lines_quarantined_total();

        // Compact with an EMPTY retire set: this test is about the torn line, not about retirement,
        // and an empty set proves the quarantine is a property of compaction rather than of
        // whatever it was asked to remove.
        ledger.retire_identities(&HashSet::new()).await.unwrap();

        // ---- Arm 2: the line survives, verbatim, in the quarantine.
        let torn_path = WriteLedger::torn_path_for(&path);
        let quarantined = tokio::fs::read_to_string(&torn_path)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "the torn line must be preserved at {torn_path:?}, not discarded: {e}. \
                 Discarding it converts an incompleteness the reader REPORTED into silent \
                 permanent loss of a staged range's bytes."
                )
            });
        assert!(
            quarantined.contains(TORN),
            "the quarantine must hold the line VERBATIM so an operator can see what was lost; it \
             holds {quarantined:?}"
        );

        // ---- Arm 3: the ledger reads as complete again, so the repair rotation is not deadlocked.
        let after = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(
            after.malformed_lines, 0,
            "the compacted ledger must no longer carry the torn line. If it does, is_complete() is \
             false forever, the fold retains on every pass, and the repair rotation refuses on \
             every pass — which disables the one mechanism that can re-append the lost range."
        );
        assert!(
            after.is_complete(),
            "and the read must therefore be complete, which is what lets the repair rotation run \
             and restore the lost coverage by range identity"
        );

        // ---- Arm 4: the good entries were not collateral damage.
        let keys: std::collections::BTreeSet<String> =
            after.entries.iter().map(|e| e.cache_key.clone()).collect();
        assert_eq!(
            keys,
            [
                "bucket/kept-a.bin".to_string(),
                "bucket/kept-b.bin".to_string()
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<String>>(),
            "quarantining a torn line must not disturb the entries around it"
        );
        assert_eq!(
            after.max_seq_per_instance.get("host:1"),
            Some(&2),
            "and the file's high-water mark must survive, or a later process reusing this identity \
             re-issues numbers an installed cut has already passed"
        );

        // ---- Arm 5: the loss is COUNTED, not only logged. A delta, because the counter is
        // process-global and an absolute would race every other test in this binary.
        assert_eq!(
            torn_lines_quarantined_total(),
            quarantined_before + 1,
            "a quarantined line must increment ledger_torn_lines_quarantined_total; a WARN alone is \
             not observable on /metrics, and this is the only signal that a staged range's entry was \
             destroyed by a partial write"
        );
    }

    /// **Loss mode L6.** A `Superseded` verdict retires only the entries whose OWN extent was
    /// replaced, never the object's live siblings.
    ///
    /// # The defect, and why every existing test was blind to it
    ///
    /// A multipart object holds one ledger entry per extent. `Superseded` is decided by comparing
    /// the candidate's newest entry against the `.meta`'s **newest range**, so a re-PUT of one
    /// extent makes the verdict fire for the whole key. Both retirement sites then deleted every
    /// identity the candidate carried. If the re-PUT's own ledger append was lost — it never fails
    /// the upload; `journal_consolidator.rs`'s `record_staged_range` logs at WARN — the
    /// object's *other* extents are perfectly live, still staged, and their entries were deleted
    /// anyway. Partial loss became total loss, `F` dropped by the survivors' bytes, and the staging
    /// eviction trigger fires **LATE** — the direction § 16.6.2 rules out by name.
    ///
    /// Every pre-existing verdict test in this module seeds **one** range. With one range there is
    /// one identity, so "retire the key's identities" and "retire this extent's identity" are the
    /// same set, and the fixture cannot distinguish the two whatever the code does. Arm 2 asserts
    /// that explicitly rather than leaving it as a claim — it is the recorded failure mode of this
    /// workstream, now on its fourth instance.
    ///
    /// # What is asserted
    ///
    /// Arm 1: two extents, one re-PUT with its append lost. The verdict stays `Superseded` — the
    /// eviction decision is deliberately unchanged — and `retirable` holds the replaced extent's
    /// entry **only**.
    ///
    /// Arm 2: the single-extent case, where `retirable` is the whole (one-element) set. This pins
    /// that the narrowing did not change the object-level behaviour, and demonstrates why arm 1
    /// needs two extents.
    ///
    /// Arm 3: a graduated object with two extents still retires **both**. The object flag is a
    /// property of the object, so when it is clear no extent is staged and narrowing would be wrong
    /// — this is the guard against over-applying the fix.
    ///
    /// A failure in arm 1 implicates the `retirable: terminal_identities(candidate, &metadata)` line
    /// in `assess_staged_candidate`'s `Superseded` branch, or `terminal_identities` itself. A failure
    /// in arm 3 implicates that function's `retire_all` closure.
    #[tokio::test]
    async fn a_superseded_reput_retires_only_the_replaced_extent_not_its_siblings() {
        // ---- Arm 1: two extents, one replaced, the other live.
        let tmp = TempDir::new().unwrap();
        let key = "bucket/multipart.bin";
        let extent_a = entry(key, 0, 1023, 512, 300);
        let extent_b = entry(key, 1024, 2047, 512, 240);

        // The `.meta` after a re-PUT of extent A whose ledger append was lost: A's range is newer
        // than every entry for the key, B's is older than its own entry and untouched.
        let reput_at = SystemTime::now();
        let b_created = extent_b.timestamp - Duration::from_secs(10);
        seed_meta_ranges(
            tmp.path(),
            key,
            true,
            None,
            &[(0, 1023, reput_at), (1024, 2047, b_created)],
        )
        .await;

        let candidate =
            WriteLedger::group_by_key(vec![extent_a.clone(), extent_b.clone()]).remove(0);
        assert_eq!(
            candidate.identities.len(),
            2,
            "fixture precondition: the candidate must carry TWO identities, or per-key and \
             per-range retirement are the same set and this test cannot fail for its own reason"
        );

        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(
            assessment.verdict,
            StagedCandidateVerdict::Superseded,
            "fixture precondition: the key-level verdict must still be Superseded — the eviction \
             decision is deliberately unchanged, and if this is Evictable the fixture is not \
             reproducing L6 at all"
        );
        assert_eq!(
            assessment.retirable,
            vec![extent_a.identity()],
            "only the REPLACED extent's entry is terminal. Retiring extent B's entry deletes the \
             staging record of a live range nothing replaced, which removes its bytes from F and \
             makes the eviction trigger fire late."
        );
        assert!(
            !assessment.retirable.contains(&extent_b.identity()),
            "extent B is still staged and still the current copy; its entry must survive"
        );

        // ---- Arm 2: one extent. The set is the whole set, and that is why arm 1 needs two.
        let tmp_single = TempDir::new().unwrap();
        let single_key = "bucket/single.bin";
        let single = entry(single_key, 0, 1023, 512, 300);
        seed_meta_ranges(
            tmp_single.path(),
            single_key,
            true,
            None,
            &[(0, 1023, SystemTime::now())],
        )
        .await;
        let single_candidate = WriteLedger::group_by_key(vec![single.clone()]).remove(0);
        let single_assessment = assess_staged_candidate(tmp_single.path(), &single_candidate).await;
        assert_eq!(
            single_assessment.verdict,
            StagedCandidateVerdict::Superseded
        );
        assert_eq!(
            single_assessment.retirable, single_candidate.identities,
            "with one extent the range-scoped set IS the key's set, so the single-range fixture \
             every other verdict test uses cannot distinguish the two implementations"
        );

        // ---- Arm 3: a graduated object retires BOTH extents. Narrowing must not go this far.
        //
        // **The ranges record `staged: Some(true)` while the object flag is CLEAR**, and that
        // disagreement is what makes this arm a discriminator rather than a restatement. With
        // `staged: None` the per-range predicate falls back to the object flag, so a narrowed
        // implementation would reach the same all-terminal answer by a different route and the arm
        // would pass whatever the code did — vacuous, and vacuous in the direction that matters,
        // since this is the guard against over-applying the L6 fix.
        //
        // The state is not invented for the test: graduation clears the object flag and leaves each
        // range's recorded membership alone, which is the shape the task-76 double-debit finding was
        // about, and a mixed-version fleet can hold `.meta` files written either way. When the two
        // disagree for a GRADUATED object the object flag wins, because the `Graduation` entry has
        // already debited these bytes from `write_cache_size` — so keeping any of these entries
        // would leave staging eviction candidates for bytes nothing is counting.
        let tmp_grad = TempDir::new().unwrap();
        let grad_key = "bucket/graduated.bin";
        let grad_a = entry(grad_key, 0, 1023, 512, 300);
        let grad_b = entry(grad_key, 1024, 2047, 512, 240);
        let old = grad_a.timestamp - Duration::from_secs(60);
        seed_meta_ranges(
            tmp_grad.path(),
            grad_key,
            // The object flag is CLEAR: it graduated on its first read.
            false,
            // But the ranges still claim staged membership.
            Some(true),
            &[(0, 1023, old), (1024, 2047, old)],
        )
        .await;
        let grad_candidate = WriteLedger::group_by_key(vec![grad_a, grad_b]).remove(0);
        let grad_assessment = assess_staged_candidate(tmp_grad.path(), &grad_candidate).await;
        assert_eq!(grad_assessment.verdict, StagedCandidateVerdict::Graduated);
        assert_eq!(
            grad_assessment.retirable, grad_candidate.identities,
            "a graduated object has no staged extent at all — the flag is a property of the \
             OBJECT — so every entry naming it is terminal and all of them must be retired. \
             Narrowing here would leave entries for bytes already debited from write_cache_size."
        );
    }

    #[tokio::test]
    async fn verify_skips_a_superseded_entry() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/rewritten.bin", 0, 9, 10, 60);
        // Range created AFTER the ledger entry — the signature of a later write.
        seed_meta(
            tmp.path(),
            &e.cache_key,
            true,
            e.timestamp + Duration::from_secs(30),
            true,
        )
        .await;

        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate_for(&e))
                .await
                .verdict,
            StagedCandidateVerdict::Superseded
        );
    }

    /// A re-PUT's own fresh entry must NOT be judged superseded.
    ///
    /// This is why `assess_staged_candidate` compares against the candidate's **newest**
    /// entry rather than its oldest. Comparing against the oldest would make every
    /// re-PUT permanently unevictable: the old entry is superseded, the new one would be
    /// judged against the old timestamp and also look superseded, so the object would sit
    /// in the ledger forever while never being a candidate.
    #[tokio::test]
    async fn verify_does_not_call_a_re_puts_own_new_entry_superseded() {
        let tmp = TempDir::new().unwrap();
        let old = entry("bucket/repup.bin", 0, 9, 10, 120);
        let mut new = old.clone();
        new.timestamp = old.timestamp + Duration::from_secs(60);

        // The `.meta` reflects the re-PUT: its range is newer than the OLD entry but older
        // than the NEW one.
        seed_meta(
            tmp.path(),
            &old.cache_key,
            true,
            old.timestamp + Duration::from_secs(30),
            true,
        )
        .await;

        let candidate = WriteLedger::group_by_key(vec![old, new]).remove(0);
        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate)
                .await
                .verdict,
            StagedCandidateVerdict::Evictable,
            "grouping keeps the newest timestamp as the supersede reference precisely so a \
             re-PUT stays evictable"
        );
    }

    /// A staged `.meta` claiming no ranges has nothing to reclaim, so it must not be
    /// reported evictable — otherwise the evictor credits a no-op deletion toward its
    /// freed-bytes target and stops short of actually freeing anything.
    #[tokio::test]
    async fn verify_skips_a_staged_entry_with_no_ranges() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/empty.bin", 0, 9, 10, 10);
        seed_meta(
            tmp.path(),
            &e.cache_key,
            true,
            e.timestamp - Duration::from_secs(1),
            false,
        )
        .await;

        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate_for(&e))
                .await
                .verdict,
            StagedCandidateVerdict::MetadataAbsent
        );
    }

    /// An unreadable `.meta` is skipped **and not retired**, because one failed read on
    /// shared storage is not evidence the object is gone. Retiring it would drop a live
    /// staged object's only eviction hint.
    #[tokio::test]
    async fn verify_reports_unreadable_rather_than_guessing() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/corrupt.bin", 0, 9, 10, 10);
        let meta_path = crate::disk_cache::get_sharded_path(
            &tmp.path().join("metadata"),
            &e.cache_key,
            ".meta",
        )
        .unwrap();
        tokio::fs::create_dir_all(meta_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&meta_path, b"not json at all")
            .await
            .unwrap();

        assert_eq!(
            assess_staged_candidate(tmp.path(), &candidate_for(&e))
                .await
                .verdict,
            StagedCandidateVerdict::Unreadable
        );
    }

    /// **T3, absent half.** A genuinely absent `.meta` must still reach `MetadataAbsent` and still
    /// retire every identity, with the `exists()` probe that used to precede the read gone.
    ///
    /// This is the **control** for the half below, not the discriminator: it passes whether the
    /// error kind is discriminated or collapsed, because `ENOENT` is the kind a naive
    /// `Err(_) => MetadataAbsent` would map here anyway. Its job is to prove that removing the
    /// probe did not lose the verdict — without it, an implementation that reached `Unreadable` for
    /// everything would satisfy the discriminating half on its own.
    ///
    /// Asserts the retirement set as well as the verdict, because `retire_all` is the
    /// **object-scoped** arm § 16.6.2's range-scoped retirement rule depends on, and the verdict
    /// alone does not say which arm produced it.
    #[tokio::test]
    async fn t3_absent_metadata_still_reaches_metadata_absent_and_retires_all() {
        let tmp = TempDir::new().unwrap();
        // Older than `ABSENT_RETIREMENT_GRACE`, so the grace does not hold it back.
        let e = entry(
            "bucket/absent.bin",
            0,
            9,
            10,
            ABSENT_RETIREMENT_GRACE.as_secs() + 60,
        );
        let candidate = candidate_for(&e);
        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(
            assessment.verdict,
            StagedCandidateVerdict::MetadataAbsent,
            "ENOENT from the read is the whole information content the removed `exists()` probe \
             carried, so an absent `.meta` must still reach MetadataAbsent"
        );
        assert_eq!(
            assessment.retirable, candidate.identities,
            "MetadataAbsent is the object-scoped `retire_all` arm: nothing about the object \
             survives, so every identity is terminal"
        );
    }

    /// an absent `.meta` retires only entries older than `ABSENT_RETIREMENT_GRACE`.
    ///
    /// A multipart completion appends before it renames its `.meta` into place, so an
    /// assessment landing inside that rename sees fresh entries and no `.meta`. Retiring them
    /// would leave the object, once published, staged with no entry. One candidate carries a
    /// fresh and an old identity, so both halves are judged in the same read: the old one must
    /// still go (the grace must not stop absent-object compaction altogether), the fresh one
    /// must stay.
    #[tokio::test]
    async fn absent_metadata_retires_only_entries_older_than_the_grace() {
        let tmp = TempDir::new().unwrap();
        let old = entry(
            "bucket/in-flight.bin",
            0,
            9,
            10,
            ABSENT_RETIREMENT_GRACE.as_secs() + 60,
        );
        let fresh = entry("bucket/in-flight.bin", 10, 19, 10, 0);
        let candidate = WriteLedger::group_by_key(vec![old.clone(), fresh.clone()]).remove(0);
        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(assessment.verdict, StagedCandidateVerdict::MetadataAbsent);
        let retired: Vec<(u64, u64)> = assessment
            .retirable
            .iter()
            .map(|(_, s, e, _, _)| (*s, *e))
            .collect();
        assert_eq!(
            retired,
            vec![(0, 9)],
            "only the entry older than the grace may be retired on an absent .meta"
        );
    }

    /// a fresh entry judged against an OLD `.meta` is not retired inside the grace.
    ///
    /// An overwrite whose invalidation failed leaves the previous object's `.meta` on disk while
    /// the new multipart completion appends its entries and renames its own `.meta` into place.
    /// An unlocked assessment in that window reads the OLD document. Three of its arms used to
    /// retire the new entry on the spot: `Graduated` (the old object had graduated), an empty
    /// `ranges` list, and the per-extent "absent" case reached through `Superseded` (the old
    /// document gained a range newer than the new entry, for instance a read fill). The new
    /// object then published staged with no entry.
    ///
    /// Each arm pairs a fresh entry with one older than the grace, so the same read shows the
    /// fresh one kept and the dead one still retired: the guard must not stop retirement.
    #[tokio::test]
    async fn a_fresh_entry_judged_against_an_old_meta_is_not_retired_inside_the_grace() {
        let aged = ABSENT_RETIREMENT_GRACE.as_secs() + 60;
        let old_doc_at = SystemTime::now() - Duration::from_secs(600);
        let retired_extents = |a: &StagedCandidateAssessment| -> Vec<(u64, u64)> {
            a.retirable.iter().map(|(_, s, e, _, _)| (*s, *e)).collect()
        };
        // Every arm is judged before anything is asserted, so one run reports each arm that
        // retires a fresh entry rather than stopping at the first.
        let mut failures: Vec<String> = Vec::new();

        // ---- Arm 1: the old object graduated.
        let tmp = TempDir::new().unwrap();
        let key = "bucket/overwrite-graduated.bin";
        let dead = entry(key, 0, 9, 10, aged);
        let fresh = entry(key, 10, 19, 10, 0);
        seed_meta_ranges(tmp.path(), key, false, None, &[(0, 9, old_doc_at)]).await;
        let candidate = WriteLedger::group_by_key(vec![dead, fresh]).remove(0);
        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(assessment.verdict, StagedCandidateVerdict::Graduated);
        if retired_extents(&assessment) != vec![(0, 9)] {
            failures.push(format!(
                "Graduated arm: a fresh entry judged against an old graduated .meta was retired: retired {:?}, expected [(0, 9)]",
                retired_extents(&assessment)
            ));
        }

        // ---- Arm 2: the old document names no ranges.
        let tmp = TempDir::new().unwrap();
        let key = "bucket/overwrite-empty.bin";
        let dead = entry(key, 0, 9, 10, aged);
        let fresh = entry(key, 10, 19, 10, 0);
        // `seed_meta` with no range: the document's own `created_at` is old, so the key-level
        // supersede check does not fire and the empty-ranges arm is the one reached.
        seed_meta(tmp.path(), key, true, old_doc_at, false).await;
        let candidate = WriteLedger::group_by_key(vec![dead, fresh]).remove(0);
        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(assessment.verdict, StagedCandidateVerdict::MetadataAbsent);
        if retired_extents(&assessment) != vec![(0, 9)] {
            failures.push(format!(
                "empty-ranges arm: a fresh entry judged against an old .meta was retired: retired {:?}, expected [(0, 9)]",
                retired_extents(&assessment)
            ));
        }

        // ---- Arm 3: the old document is staged and gained a range newer than the fresh entry,
        // so the key-level verdict is Superseded and the fresh entry's extent is absent from it.
        let tmp = TempDir::new().unwrap();
        let key = "bucket/overwrite-absent-extent.bin";
        let dead = entry(key, 20, 29, 10, aged);
        let fresh = entry(key, 10, 19, 10, 0);
        let fill_at = SystemTime::now() + Duration::from_secs(1);
        seed_meta_ranges(
            tmp.path(),
            key,
            true,
            None,
            &[(0, 9, old_doc_at), (100, 109, fill_at)],
        )
        .await;
        let candidate = WriteLedger::group_by_key(vec![dead, fresh]).remove(0);
        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(assessment.verdict, StagedCandidateVerdict::Superseded);
        if retired_extents(&assessment) != vec![(20, 29)] {
            failures.push(format!(
                "absent-extent arm: a fresh entry whose extent the old .meta lacks was retired: retired {:?}, expected [(20, 29)]",
                retired_extents(&assessment)
            ));
        }

        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// **T3, discriminating half — this is the assertion that can fail.** A `.meta` path that is
    /// present but unreadable for a reason OTHER than absence must NOT reach `MetadataAbsent`, and
    /// must retire nothing.
    ///
    /// Without explicit error-kind discrimination, dropping the `exists()` probe collapses every
    /// `io::Error` onto `MetadataAbsent` and therefore onto `retire_all` — which would delete the
    /// ledger entries of a **live** staged object on one failed read of shared storage. That is
    /// destructive and in the under-count direction § 16.6.2 rules out, so the discrimination is a
    /// correctness requirement rather than tidiness.
    ///
    /// # Why a directory, and not `chmod 000`
    ///
    /// The fixture puts a **directory** at the `.meta` path, which yields
    /// `ErrorKind::IsADirectory` (`EISDIR`, raw 21) — distinct from `NotFound` (raw 2). A
    /// permission-denied fixture would have been the obvious choice and is the wrong one:
    /// `chmod 000` does not restrain uid 0, so the test would pass **vacuously** anywhere it ran
    /// as root, silently reporting a `NotFound`-vs-other discrimination it never exercised. The
    /// directory is uid-independent, and the precondition below asserts it rather than trusting it.
    ///
    /// Note this is a state the old `exists()` code also handled correctly — `exists()` is `true`
    /// for a directory, so it fell through to the same read. The regression this guards is not in
    /// the code being replaced; it is in the obvious way to replace it.
    #[tokio::test]
    async fn t3_present_but_unreadable_metadata_must_not_reach_metadata_absent() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/eisdir.bin", 0, 9, 10, 10);
        let meta_path = crate::disk_cache::get_sharded_path(
            &tmp.path().join("metadata"),
            &e.cache_key,
            ".meta",
        )
        .unwrap();
        // A directory exactly where the `.meta` should be.
        tokio::fs::create_dir_all(&meta_path).await.unwrap();

        // The fixture's own precondition: the read must fail with a kind that is NOT NotFound, so
        // a platform on which this produced ENOENT fails here naming the fixture rather than
        // passing while proving nothing about discrimination.
        let observed = tokio::fs::read_to_string(&meta_path).await.unwrap_err();
        assert_ne!(
            observed.kind(),
            std::io::ErrorKind::NotFound,
            "fixture precondition: reading a directory must NOT report NotFound, or this test \
             cannot distinguish ENOENT from other error kinds. Observed {observed:?}"
        );

        let candidate = candidate_for(&e);
        let assessment = assess_staged_candidate(tmp.path(), &candidate).await;
        assert_eq!(
            assessment.verdict,
            StagedCandidateVerdict::Unreadable,
            "a non-ENOENT read failure is not evidence the object is gone. Reaching \
             MetadataAbsent here means the error kind was collapsed, and MetadataAbsent is the \
             retire_all arm — so {} live entries would be retired on one failed read",
            assessment.retirable.len()
        );
        assert!(
            assessment.retirable.is_empty(),
            "a failed read must retire NOTHING — retiring here drops a live staged object's only \
             eviction hint, which is destructive and in the under-count direction"
        );
    }

    // =====================================================================
    // The per-`(shard, instance)` layout, its migration, and the sequence floor.
    //
    // The tests that assert the fold's INSTALLED figures live in
    // `tests/write_ledger_shard_layout_test.rs`, not here: `staged_fold.rs`'s
    // `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface` greps `src/*.rs`
    // for the gauge symbols and permits exactly two files to name them, and that containment is
    // worth more than the convenience of a private path helper. What stays here is what needs
    // module-private access — `torn_path_for`, `parse_content` — and nothing here asserts a
    // ledger-file count.
    // =====================================================================

    /// Seed a staged `.meta` and append its ledger entry, as a credit site does.
    ///
    /// The range is back-dated 5 s so the ledger entry stamped `now` is not judged superseded
    /// by the copy it describes.
    async fn credit(
        cache_dir: &std::path::Path,
        ledger: &WriteLedger,
        cache_key: &str,
        bytes: u64,
    ) {
        seed_meta_ranges(
            cache_dir,
            cache_key,
            true,
            Some(true),
            &[(
                0,
                bytes.saturating_sub(1),
                SystemTime::now() - Duration::from_secs(5),
            )],
        )
        .await;
        ledger
            .append_staged_range(cache_key, 0, bytes.saturating_sub(1), bytes)
            .await
            .unwrap();
    }

    /// A cache key no shard can be derived from is refused by the append, not filed somewhere.
    ///
    /// Under the flat layout such an entry was written and then discarded by the fold as
    /// `unshardable`, so it was already counted by nothing — silently. The refusal names the
    /// condition at the site that can see it. Whether an unshardable key should make the
    /// caching decision itself decline is deliberately NOT decided here; see
    /// [`unshardable_key_error`].
    #[tokio::test]
    async fn an_unshardable_key_is_refused_rather_than_filed_anywhere() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        // No `/`, so `parse_cache_key` cannot split a bucket from an object key.
        let refused = ledger.append_staged_range("no-slash-here", 0, 9, 10).await;
        assert!(
            refused.is_err(),
            "an unshardable key has no shard ledger file to append to, so the append must refuse"
        );
        let read = ledger.read_for_authority(0).await.unwrap();
        assert!(
            read.entries.is_empty() && read.files_expected == 0,
            "and nothing may have been written anywhere: {:?}",
            read
        );
        assert!(
            ledger.read_merged_oldest_first(0).await.unwrap().is_empty(),
            "including at the legacy flat path"
        );
    }

    /// A torn line in ONE shard's ledger file is quarantined to that shard's own `.torn`, and the
    /// sibling shard's files are left alone.
    ///
    /// The single-file fixture this replaces could not express the second half at all.
    ///
    /// A failure in the last assertion implicates `torn_path_for`: a quarantine file that
    /// `shard_ledger_files()` picked up would be re-counted as malformed on every read, and
    /// `is_complete()` would be false forever.
    #[tokio::test]
    async fn a_torn_line_in_one_shard_file_is_quarantined_to_that_shard_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

        credit(dir, &ledger, "bucket/torn-shard.bin", 4096).await;
        credit(dir, &ledger, "bucket/clean-shard.bin", 512).await;
        let torn_file = ledger.shard_path_for_key("bucket/torn-shard.bin").unwrap();
        let clean_file = ledger.shard_path_for_key("bucket/clean-shard.bin").unwrap();
        assert_ne!(
            torn_file, clean_file,
            "fixture precondition: two shard files"
        );
        let clean_before = std::fs::read_to_string(&clean_file).unwrap();

        let mut content = std::fs::read_to_string(&torn_file).unwrap();
        content.push_str("{\"partial\": tru\n");
        std::fs::write(&torn_file, content).unwrap();
        let read = ledger.read_for_authority(0).await.unwrap();
        assert_eq!(
            read.malformed_lines, 1,
            "fixture precondition: the line must really be discarded"
        );

        // A compaction that names nothing still rewrites, which is what quarantines the line.
        ledger.retire_identities(&HashSet::new()).await.unwrap();

        let torn_quarantine = WriteLedger::torn_path_for(&torn_file);
        assert!(
            std::fs::read_to_string(&torn_quarantine)
                .unwrap()
                .contains("partial"),
            "the torn line must be preserved beside its OWN shard file: {:?}",
            torn_quarantine
        );
        assert!(
            !WriteLedger::torn_path_for(&clean_file).exists(),
            "and the sibling shard must have no quarantine file at all"
        );
        // Not byte-identical: compaction is whole-volume, so it rewrites every shard file and
        // legitimately raises the sibling's header to its own high-water mark. What must be
        // untouched is its ENTRIES — the torn line belongs to one shard and cannot cost another
        // one a record.
        let clean_entries_before = WriteLedger::parse_content(&clean_file, &clean_before).entries;
        let clean_after = std::fs::read_to_string(&clean_file).unwrap();
        assert_eq!(
            WriteLedger::parse_content(&clean_file, &clean_after).entries,
            clean_entries_before,
            "the sibling shard's entries must be untouched"
        );
        assert!(
            ledger.read_for_authority(0).await.unwrap().is_complete(),
            "and the read is clean again, so the fold is not deadlocked on it"
        );
    }

    // ---- Group commit (design § 16.6.14): pinning tests (a)-(d) ----

    /// A fault-hook gate: the group written to one file parks before taking the file's lock
    /// until the test opens it.
    #[derive(Debug, Default)]
    pub(crate) struct GroupGate {
        parked: std::sync::atomic::AtomicBool,
        open: std::sync::Mutex<bool>,
        cv: std::sync::Condvar,
        /// Set when [`GATE_MAX_PARK`] ended the park instead of [`GroupGate::release`].
        ///
        /// the earlier review follow-up: the bound is a wall-clock one, so a park that expires
        /// rather than being released would let a test pass **vacuously** — the window it was
        /// asserting over had already closed. [`GroupGate::assert_released`] turns that into a
        /// failure with its own message, so the deadline is never silent.
        timed_out: std::sync::atomic::AtomicBool,
    }

    /// The longest a gated group may park. Far above any local-disk append, and bounded at
    /// all, which is the point: see [`GroupGate::wait`].
    const GATE_MAX_PARK: Duration = Duration::from_secs(20);

    impl GroupGate {
        /// Park until the test opens the gate, or [`GATE_MAX_PARK`] passes.
        ///
        /// # The bound is load-bearing, not a convenience
        ///
        /// `Runtime::drop` waits for every `spawn_blocking` task that has already started, and
        /// this hook runs inside one. An unbounded park therefore does not fail the test that
        /// forgot to open the gate: it hangs that test's own teardown, with the panic message
        /// already swallowed, and takes the rest of the binary with it. A bounded park turns the
        /// same mistake into one test failing with its real message.
        pub(crate) fn wait(&self) {
            self.parked.store(true, std::sync::atomic::Ordering::SeqCst);
            let deadline = std::time::Instant::now() + GATE_MAX_PARK;
            let mut open = self.open.lock().unwrap();
            while !*open {
                let left = deadline.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    self.timed_out
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
                let (guard, _) = self.cv.wait_timeout(open, left).unwrap();
                open = guard;
            }
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.cv.notify_all();
        }

        /// Fail if the park ended at [`GATE_MAX_PARK`] rather than at [`GroupGate::release`].
        ///
        /// Called after the gated work has been joined, so the flag is settled by then.
        fn assert_released(&self) {
            assert!(
                !self.timed_out.load(std::sync::atomic::Ordering::SeqCst),
                "APPARATUS: the gated group parked for the full {GATE_MAX_PARK:?} and was \
                 released by the deadline, not by the test. Whatever window this test asserts \
                 over had closed before the assertions ran, so its result says nothing."
            );
        }

        async fn until_parked(&self) {
            for _ in 0..5000 {
                if self.parked.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("the gated group never parked");
        }
    }

    fn park_group(ledger: &WriteLedger, path: &Path) -> std::sync::Arc<GroupGate> {
        let gate = std::sync::Arc::new(GroupGate::default());
        *ledger.sync_probe().park.lock().unwrap() =
            Some((path.to_path_buf(), std::sync::Arc::clone(&gate)));
        gate
    }
    /// Park the pending-credit stage's next batch, before it writes, until the gate opens.
    ///
    /// The state it produces is the one no other seam can: a credit whose ledger line is
    /// durable, whose reply the pending stage owes, and whose append-stage depth is already
    /// zero.
    fn park_pending_stage(ledger: &WriteLedger) -> std::sync::Arc<GroupGate> {
        let gate = std::sync::Arc::new(GroupGate::default());
        *ledger.sync_probe().pending_park.lock().unwrap() = Some(std::sync::Arc::clone(&gate));
        gate
    }
    /// Wait until `path`'s shard file holds at least one seq, i.e. that group's write and sync
    /// have landed.
    async fn until_durable(path: &Path) -> std::collections::BTreeSet<u64> {
        for _ in 0..5000 {
            let seqs = seqs_locked(path);
            if !seqs.is_empty() {
                return seqs;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        std::collections::BTreeSet::new()
    }

    /// Bound an awaited call under test. A lost reply then fails one test with a message
    /// instead of parking the suite on a oneshot that will never be answered.
    async fn bounded<F: std::future::Future>(what: &str, fut: F) -> F::Output {
        match tokio::time::timeout(Duration::from_secs(30), fut).await {
            Ok(v) => v,
            Err(_) => panic!("{what} did not complete within 30s"),
        }
    }

    async fn until_queued(ledger: &WriteLedger, n: usize) {
        for _ in 0..5000 {
            if ledger.queued_len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!(
            "expected {} queued submissions, saw {}",
            n,
            ledger.queued_len()
        );
    }

    /// `n` cache keys in pairwise different shards.
    fn keys_in_distinct_shards(tag: &str, n: usize) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for i in 0.. {
            let key = format!("bucket/{tag}-{i}.bin");
            let (_, l1) = crate::staged_fold::shard_of(&key).unwrap();
            if seen.insert(l1) {
                out.push(key);
                if out.len() == n {
                    break;
                }
            }
        }
        out
    }

    /// A key other than `key` in `key`'s shard.
    fn key_in_shard_of(key: &str, tag: &str) -> String {
        let target = crate::staged_fold::shard_of(key).unwrap();
        (0..)
            .map(|i| format!("bucket/{tag}-{i}.bin"))
            .find(|k| k != key && crate::staged_fold::shard_of(k).unwrap() == target)
            .unwrap()
    }

    /// The seqs in one shard file, read under that file's `fs4` lock as the fold reads it.
    fn seqs_locked(path: &Path) -> std::collections::BTreeSet<u64> {
        // The shard directory is created by the group under test, after the gate parks it, so a
        // sample taken while that group is parked would fail on the lock file's own open rather
        // than report the shard file's seqs. The file itself may legitimately be absent.
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap();
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(WriteLedger::lock_path_for(path))
            .unwrap();
        fs4::FileExt::lock(&lock).unwrap();
        let content = std::fs::read_to_string(path).unwrap_or_default();
        let _ = fs4::FileExt::unlock(&lock);
        WriteLedger::parse_content(path, &content)
            .entries
            .iter()
            .filter_map(|e| e.seq)
            .collect()
    }

    /// (a) Prefix. Batch `k`'s group for shard S is parked; batch `k+1` carries another entry for
    /// S. Every sample of S's file, taken under its lock, must be downward-closed over the FINAL
    /// set of seqs issued to S that returned `Ok`. The final set, not the set at sample time: at
    /// sample time `k`'s seq has not returned yet, and a sample `{k+1}` would pass against that.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_a_shard_file_stays_a_prefix_of_its_ok_seqs() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        let k1 = "bucket/prefix-first.bin".to_string();
        let k2 = key_in_shard_of(&k1, "prefix-second");
        let path = ledger.shard_path_for_key(&k1).unwrap();
        assert_eq!(
            ledger.shard_path_for_key(&k2).unwrap(),
            path,
            "both entries must be on shard S, or the test passes without the barrier"
        );

        let gate = park_group(&ledger, &path);
        let l1 = std::sync::Arc::clone(&ledger);
        let h1 = tokio::spawn(async move { l1.append(entry(&k1, 0, 9, 10, 0)).await });
        gate.until_parked().await;
        let l2 = std::sync::Arc::clone(&ledger);
        let h2 = tokio::spawn(async move { l2.append(entry(&k2, 0, 9, 10, 0)).await });

        let mut samples = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < deadline && !h2.is_finished() {
            samples.push(seqs_locked(&path));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        samples.push(seqs_locked(&path));
        gate.release();
        let s1 = bounded("batch k's append", h1).await.unwrap().unwrap();
        let s2 = bounded("batch k+1's append", h2).await.unwrap().unwrap();
        gate.assert_released();
        samples.push(seqs_locked(&path));

        assert!(s1 < s2, "batch k must be issued before batch k+1");
        let ok: std::collections::BTreeSet<u64> = [s1, s2].into_iter().collect();
        for sample in &samples {
            for &seen in sample {
                for &lower in ok.range(..seen) {
                    assert!(
                        sample.contains(&lower),
                        "shard file shows seq {} while seq {}, issued earlier to the same shard \
                         and returned Ok, is absent: sample {:?}, final Ok set {:?}",
                        seen,
                        lower,
                        sample,
                        ok
                    );
                }
            }
        }
        assert_eq!(samples.last().unwrap(), &ok);
    }

    /// (b) Per-group error. One batch spans shards A and B, and only B's sync fails. A's caller
    /// gets `Ok`, B's gets `Err` (so `record_staged_range` returns `false` for B and declines its
    /// credit), and the next batch's seqs exceed every seq of this one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_b_a_failed_group_fails_only_its_own_entries() {
        use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
        let tmp = TempDir::new().unwrap();
        let consolidator = std::sync::Arc::new(JournalConsolidator::new(
            tmp.path().to_path_buf(),
            std::sync::Arc::new(crate::journal_manager::JournalManager::new(
                tmp.path().to_path_buf(),
                "test-instance".to_string(),
            )),
            std::sync::Arc::new(crate::metadata_lock_manager::MetadataLockManager::new(
                tmp.path().to_path_buf(),
                Duration::from_secs(30),
                3,
            )),
            ConsolidationConfig::default(),
        ));
        let ledger = std::sync::Arc::clone(consolidator.write_ledger());
        let keys = keys_in_distinct_shards("pergroup", 4);
        let (p, a, b, c) = (
            keys[0].clone(),
            keys[1].clone(),
            keys[2].clone(),
            keys[3].clone(),
        );
        let path_p = ledger.shard_path_for_key(&p).unwrap();
        let path_b = ledger.shard_path_for_key(&b).unwrap();

        // Park a first batch so A and B queue up behind it and drain as one batch.
        let gate = park_group(&ledger, &path_p);
        let lp = std::sync::Arc::clone(&ledger);
        let hp = tokio::spawn(async move { lp.append(entry(&p, 0, 9, 10, 0)).await });
        gate.until_parked().await;
        *ledger.sync_probe().fail_path.lock().unwrap() = Some(path_b.clone());
        let ca = std::sync::Arc::clone(&consolidator);
        let ka = a.clone();
        let ha = tokio::spawn(async move { ca.record_staged_range(&ka, 0, 1023, 512).await });
        until_queued(&ledger, 1).await;
        let cb = std::sync::Arc::clone(&consolidator);
        let kb = b.clone();
        let hb = tokio::spawn(async move { cb.record_staged_range(&kb, 0, 1023, 512).await });
        until_queued(&ledger, 2).await;
        gate.release();

        bounded("the parked batch's append", hp)
            .await
            .unwrap()
            .unwrap();
        let a_ok = bounded("A's record_staged_range", ha).await.unwrap();
        let b_ok = bounded("B's record_staged_range", hb).await.unwrap();
        gate.assert_released();
        let batches = ledger.sync_probe().batches.lock().unwrap().clone();
        assert_eq!(batches.len(), 2, "batches: {:?}", batches);
        let (first, last) = batches[1];
        assert_eq!(
            last - first + 1,
            2,
            "A and B must share one batch, or a batch-wide reply is indistinguishable from a \
             per-group one: {:?}",
            batches
        );
        assert!(
            a_ok,
            "A's group wrote and synced, so A's caller must get Ok (record_staged_range true)"
        );
        assert!(
            !b_ok,
            "B's group failed its sync, so B's caller must get Err (record_staged_range false)"
        );

        *ledger.sync_probe().fail_path.lock().unwrap() = None;
        let next = bounded(
            "the next batch's append",
            ledger.append(entry(&c, 0, 9, 10, 0)),
        )
        .await
        .unwrap();
        assert!(
            next > last,
            "the next batch's seq {} must exceed every seq of the failed batch ({}..={})",
            next,
            first,
            last
        );
    }

    /// (c) Mark. A batch straddling a `SEQ_MARK_STRIDE` boundary: at each group's `write_all`
    /// the `.ledger.mark` on disk must already be at least the batch's LAST seq.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_c_the_mark_covers_the_batch_last_seq_before_any_line() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        // Position the allocator just below a stride boundary: next 1019, mark 1024.
        let reserved = ledger.core.write_seq_mark(SEQ_MARK_STRIDE).await.unwrap();
        *ledger.core.sequence.lock().await = Some(SequenceState {
            next: SEQ_MARK_STRIDE - 5,
            mark_reserved: reserved,
        });
        let keys = keys_in_distinct_shards("mark", 12);
        let path_p = ledger.shard_path_for_key(&keys[0]).unwrap();
        let gate = park_group(&ledger, &path_p);
        let lp = std::sync::Arc::clone(&ledger);
        let kp = keys[0].clone();
        let hp = tokio::spawn(async move { lp.append(entry(&kp, 0, 9, 10, 0)).await });
        gate.until_parked().await;
        *ledger.sync_probe().observe_mark.lock().unwrap() = Some(ledger.seq_mark_path());
        let mut handles = Vec::new();
        for key in &keys[1..] {
            let key = key.clone();
            let l = std::sync::Arc::clone(&ledger);
            handles.push(tokio::spawn(async move {
                l.append(entry(&key, 0, 9, 10, 0)).await
            }));
        }
        until_queued(&ledger, 11).await;
        gate.release();
        bounded("the parked batch's append", hp)
            .await
            .unwrap()
            .unwrap();
        for h in handles {
            bounded("a straddling batch's append", h)
                .await
                .unwrap()
                .unwrap();
        }

        gate.assert_released();
        let batches = ledger.sync_probe().batches.lock().unwrap().clone();
        let (first, last) = batches[1];
        assert!(
            first <= SEQ_MARK_STRIDE && last > SEQ_MARK_STRIDE && batches.len() == 2,
            "the second batch must straddle the stride boundary {}, or a first-seq mark \
             passes: {:?}",
            SEQ_MARK_STRIDE,
            batches
        );
        let observed = ledger.sync_probe().observed_marks.lock().unwrap().clone();
        assert_eq!(
            observed.len(),
            12,
            "one observation per group: {:?}",
            observed
        );
        for &mark in &observed[1..] {
            assert!(
                mark >= last,
                "the mark on disk was {} at a group write of batch {}..={}; it must already \
                 cover the batch's last seq",
                mark,
                first,
                last
            );
        }
    }

    /// (d) Burn on panic. A group's blocking task panics after its `write_all`, so its line is in
    /// the file while its caller gets `Err`. The next batch's seq must exceed every seq in the
    /// file.
    #[tokio::test]
    async fn group_commit_d_a_panicked_group_burns_its_seqs() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());
        let path_a = ledger.shard_path_for_key("bucket/burn-a.bin").unwrap();
        *ledger.sync_probe().panic_after_write.lock().unwrap() = Some(path_a.clone());
        let panicked = bounded(
            "the panicked group's append",
            ledger.append(entry("bucket/burn-a.bin", 0, 9, 10, 0)),
        )
        .await;
        assert!(panicked.is_err(), "a panicked group must fail its entry");
        let in_file = seqs_locked(&path_a);
        assert!(
            !in_file.is_empty(),
            "the panic must follow the write, or there is nothing to reissue"
        );
        let next = bounded(
            "the reissuing append",
            ledger.append(entry("bucket/burn-a.bin", 10, 19, 10, 0)),
        )
        .await
        .unwrap();
        let max_in_file = *in_file.iter().max().unwrap();
        assert!(
            next > max_in_file,
            "reused seq: the next batch was issued {} while the file already holds {:?}",
            next,
            in_file
        );
    }

    /// Local throughput report (not an acceptance): N concurrent callers over a temp directory
    /// on local disk, and the mean batch size.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn group_commit_local_throughput_report() {
        const CALLERS: usize = 64;
        const APPENDS_PER_CALLER: usize = 32;
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "bench:1".to_string(),
        ));
        bounded(
            "the warmup append",
            ledger.append(entry("bench/warmup.bin", 0, 1023, 1024, 0)),
        )
        .await
        .unwrap();
        let started = std::time::Instant::now();
        let mut tasks = Vec::new();
        for caller in 0..CALLERS {
            let ledger = std::sync::Arc::clone(&ledger);
            tasks.push(tokio::spawn(async move {
                for i in 0..APPENDS_PER_CALLER {
                    ledger
                        .append(entry(
                            &format!("bench/obj-{caller}-{i}.bin"),
                            0,
                            1023,
                            1024,
                            0,
                        ))
                        .await
                        .unwrap();
                }
            }));
        }
        // Bounded at the JOIN rather than inside the measured loop: a wedged append parks its
        // own task, so the join is where it becomes visible, and a per-append timeout would add
        // a timer to every iteration of the thing being measured.
        for t in tasks {
            match tokio::time::timeout(Duration::from_secs(120), t).await {
                Ok(r) => r.unwrap(),
                Err(_) => panic!("a throughput caller did not finish within 120s"),
            }
        }
        let elapsed = started.elapsed();
        let total = CALLERS * APPENDS_PER_CALLER;
        let batches = ledger.sync_probe().batches.lock().unwrap().len() - 1;
        println!(
            "GROUP_COMMIT appends={} elapsed_ms={} appends_per_s={:.0} batches={} mean_batch={:.2}",
            total,
            elapsed.as_millis(),
            total as f64 / elapsed.as_secs_f64(),
            batches,
            total as f64 / batches as f64
        );
    }

    // ---- The pending stage (design § 16.6.14): pinning test (a) ----

    /// 56(a) Ordering. The ledger group for shard S is parked at its write, so the pending stage
    /// cannot have been handed S's entry: every sample of the pending file taken in that window
    /// must name only seqs S's ledger file already holds, at that same instant.
    ///
    /// # Non-vacuity
    ///
    /// Without the park the samples would all be taken after both halves exist, and a record
    /// handed over at submit would pass. So the window is asserted rather than assumed: at least
    /// one sample must be taken while S's ledger file holds NO seq at all, which is exactly the
    /// instant at which a pending record would have no line behind it. The gate's own release is
    /// asserted too ([`GroupGate::assert_released`]), because a park that expired would close the
    /// window without saying so.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_56a_a_pending_record_never_precedes_its_ledger_line() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        let key = "bucket/pending-order.bin".to_string();
        let path = ledger.shard_path_for_key(&key).unwrap();
        let store = crate::staged_fold::PendingStore::new(tmp.path().to_path_buf());

        let gate = park_group(&ledger, &path);
        let l = std::sync::Arc::clone(&ledger);
        let k = key.clone();
        // The paired path, which is what has a pending half at all.
        let h = tokio::spawn(async move { l.append_staged_entry(entry(&k, 0, 9, 10, 0)).await });
        gate.until_parked().await;

        let mut samples: Vec<(
            std::collections::BTreeSet<u64>,
            std::collections::BTreeSet<u64>,
        )> = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_millis(300);
        while std::time::Instant::now() < deadline && !h.is_finished() {
            let pending: std::collections::BTreeSet<u64> = store
                .read_all()
                .await
                .credits
                .iter()
                .map(|(_, c)| c.seq)
                .collect();
            samples.push((pending, seqs_locked(&path)));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        gate.release();
        bounded("the paired append", h)
            .await
            .unwrap()
            .expect("the paired append must succeed once the gate opens");
        gate.assert_released();

        for (i, (pending, in_file)) in samples.iter().enumerate() {
            let orphans: Vec<&u64> = pending.difference(in_file).collect();
            assert!(
                orphans.is_empty(),
                "sample {}: the pending file names seq(s) {:?} that shard S's ledger file does \
                 not hold ({:?}): a pending record was written before its ledger line, and a cut \
                 can never pass a line that does not exist",
                i,
                orphans,
                in_file
            );
        }
        assert!(
            samples.iter().any(|(_, in_file)| in_file.is_empty()),
            "APPARATUS: no sample was taken while S's ledger file was still empty, so a record \
             handed over at submit would not have been caught: {:?}",
            samples
        );
        let after: std::collections::BTreeSet<u64> = store
            .read_all()
            .await
            .credits
            .iter()
            .map(|(_, c)| c.seq)
            .collect();
        assert_eq!(after.len(), 1, "one paired append, one pending record");
        assert_eq!(
            after,
            seqs_locked(&path),
            "once the gate opens both halves must hold exactly the same seq"
        );
    }

    /// Task 58 (b). Ten concurrent submissions with a delayed first group must be carried by
    /// fewer than ten batches, with every one of the ten entries counted and on disk.
    ///
    /// # Why the delayed first group is the whole fixture
    ///
    /// A batch is whatever arrived while the previous one was being written, so ten submissions
    /// against an idle, fast ledger are drained one at a time: ten batches of one, which is the
    /// correct reading and is indistinguishable from a stage that does not batch at all. The
    /// parked group creates the only state in which the two differ, and the batch assertion is
    /// made in that state.
    ///
    /// The counters are read as a delta over `stage_metrics()`, which reads THIS ledger's own
    /// atomics — the same fields `/metrics` publishes. A process-global counter could not be
    /// asserted exactly here, because every other test in this binary appends too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_58b_a_delayed_group_makes_one_batch_carry_many_entries() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        let keys = keys_in_distinct_shards("counters", 11);
        let before = ledger.stage_metrics();
        assert_eq!(
            (
                before.append_batches_total,
                before.append_batched_entries_total
            ),
            (0, 0),
            "a fresh ledger has batched nothing"
        );

        // The parked first group holds the appender's barrier, so everything submitted while it
        // is parked accumulates on the queue and drains as few batches rather than ten.
        let path0 = ledger.shard_path_for_key(&keys[0]).unwrap();
        let gate = park_group(&ledger, &path0);
        let l0 = std::sync::Arc::clone(&ledger);
        let k0 = keys[0].clone();
        let h0 = tokio::spawn(async move { l0.append_staged_entry(entry(&k0, 0, 9, 10, 0)).await });
        gate.until_parked().await;

        let mut handles = vec![h0];
        for (i, key) in keys[1..10].iter().enumerate() {
            let l = std::sync::Arc::clone(&ledger);
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                l.append_staged_entry(entry(&k, 0, 9, 10, 0)).await
            }));
            until_queued(&ledger, i + 1).await;
        }
        gate.release();
        for (i, h) in handles.into_iter().enumerate() {
            bounded("a submission", h)
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("submission {i} must succeed: {e}"));
        }
        gate.assert_released();

        let after = ledger.stage_metrics();
        let batches = after.append_batches_total - before.append_batches_total;
        let entries = after.append_batched_entries_total - before.append_batched_entries_total;
        assert_eq!(
            entries, 10,
            "ten submissions must be counted as ten batched entries, not as batches: \
             batches={batches}, entries={entries}"
        );
        assert!(
            batches < 10,
            "ten submissions behind a parked group must be carried by fewer than ten batches, \
             or each hold of a shard file's lock is amortised over one entry and the ratio \
             published as the mean batch size is 1 whatever the load: batches={batches}, \
             entries={entries}, probe={:?}",
            ledger.sync_probe().batches.lock().unwrap()
        );
        let mut lines = 0usize;
        for key in &keys[..10] {
            let p = ledger.shard_path_for_key(key).unwrap();
            lines += seqs_locked(&p).len();
        }
        assert_eq!(
            lines, 10,
            "the ten entries must be on disk, one line each: a batch that coalesced lines away \
             would publish the same counters and lose the records"
        );
        assert!(
            after.pending_batches_total > 0,
            "ten paired submissions must have gone through the pending stage at least once"
        );
        assert_eq!(
            after.pending_batch_failures_total, 0,
            "no pending batch failed in this fixture"
        );
    }

    /// Task 58 (c). Submissions in flight when shutdown begins each receive `Ok`, with their
    /// ledger line and their pending record on disk, because the drain answers them before the
    /// stage tasks end.
    ///
    /// # The three things that make this non-vacuous
    ///
    /// A submission that has already resolved before the drain starts is answered on any tree,
    /// so the fixture parks a group and asserts that nothing in the batch under test has been
    /// answered when the drain is entered. The stage tasks are then ended explicitly
    /// ([`WriteLedger::abort_stages`]) the moment the drain returns, which is what a dropped
    /// runtime does at process exit.
    ///
    /// And the batch under test carries **three** groups, one parked and two that sync, so two
    /// of its callers are owed a reply for a ledger line that is already on disk — asserted,
    /// before the drain. `reply_per_group` runs only once every group of a batch has resolved,
    /// which is what lets a durable line and an unsent reply coexist. That is the state task 58
    /// words the red side around, and the earlier fixture could not produce it: its only
    /// in-flight entry was the parked one, whose file is empty, so a tree that dropped the queue
    /// failed the test for an entry with no durable line at all.
    ///
    /// The caller loop judges the two synced callers first for the same reason.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_58c_a_submission_in_flight_at_shutdown_gets_its_real_outcome() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        let store = crate::staged_fold::PendingStore::new(tmp.path().to_path_buf());
        let keys = keys_in_distinct_shards("shutdown", 4);
        // Batch 1 is one entry and is parked. Its only job is to hold the append stage while
        // batch 2's three entries queue behind it, so they share a batch.
        let hold = park_group(&ledger, &ledger.shard_path_for_key(&keys[0]).unwrap());
        let l = std::sync::Arc::clone(&ledger);
        let k0 = keys[0].clone();
        let h0 = tokio::spawn(async move { l.append_staged_entry(entry(&k0, 0, 9, 10, 0)).await });
        hold.until_parked().await;
        // Batch 2: three groups, the first of them parked.
        let gate = park_group(&ledger, &ledger.shard_path_for_key(&keys[1]).unwrap());
        let mut handles = Vec::new();
        for (i, key) in keys[1..].iter().enumerate() {
            let l = std::sync::Arc::clone(&ledger);
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                l.append_staged_entry(entry(&k, 0, 9, 10, 0)).await
            }));
            until_queued(&ledger, i + 1).await;
        }
        hold.release();
        bounded("the holding submission", h0)
            .await
            .unwrap()
            .expect("FIXTURE: the holding submission must succeed");
        gate.until_parked().await;
        // The two unparked groups of batch 2 are durable BEFORE the drain is entered, and their
        // callers are still owed a reply. This is the precondition the red side needs.
        for key in &keys[2..] {
            let durable = until_durable(&ledger.shard_path_for_key(key).unwrap()).await;
            assert!(
                !durable.is_empty(),
                "APPARATUS: {key}'s group must have synced before the drain, or no caller in \
                 this fixture is owed a reply for a durable ledger line and the red side is not \
                 the one task 58 words"
            );
        }
        assert!(
            handles.iter().all(|h| !h.is_finished()),
            "APPARATUS: a submission resolved before the drain was entered, so this fixture \
             would pass on a tree that does not drain at all"
        );
        // Shutdown begins. The parked group is released from another task, so the drain is
        // genuinely waiting on work in flight rather than on an already-quiet queue.
        let g = std::sync::Arc::clone(&gate);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            g.release();
        });
        let drained = bounded(
            "the shutdown drain",
            ledger.drain_stages(Duration::from_secs(10)),
        )
        .await;
        // The runtime going away, brought forward to here.
        ledger.abort_stages();
        hold.assert_released();
        gate.assert_released();
        let mut outcomes: Vec<(usize, Result<()>)> = Vec::new();
        for (i, h) in handles.into_iter().enumerate() {
            outcomes.push((i + 1, bounded("a submission's outcome", h).await.unwrap()));
        }
        // Durable-line-first: keys[2] and keys[3] synced before the drain was entered, keys[1]
        // was the parked group. So the first caller this loop judges is one whose ledger line
        // was already on disk while its reply was owed.
        outcomes.sort_by_key(|(i, _)| usize::from(*i == 1));
        for (i, outcome) in outcomes {
            let on_disk = seqs_locked(&ledger.shard_path_for_key(&keys[i]).unwrap());
            outcome.unwrap_or_else(|e| {
                panic!(
                    "submission {i} was answered Err at shutdown ({e}) while its ledger file \
                     holds {on_disk:?}: a durable line whose credit the caller declines is \
                     counted by the staged fold at the next cut and never by the incumbent \
                     counter, so the two derivations of one figure diverge"
                )
            });
        }
        let mut seqs = std::collections::BTreeSet::new();
        for key in keys.iter() {
            seqs.extend(seqs_locked(&ledger.shard_path_for_key(key).unwrap()));
        }
        assert_eq!(seqs.len(), 4, "four entries, four ledger lines: {seqs:?}");
        // Corroboration, after each caller's own outcome: the assertion that matters is what
        // a caller was told, and the drain's own report is this unit's figure for the same
        // state. Asserting the report first would hide the Err a durable line's caller got
        // behind a summary.
        assert!(
            drained.is_drained(),
            "the drain must leave nothing unanswered: {:?}",
            drained
        );
        let records: std::collections::BTreeSet<u64> = store
            .read_all()
            .await
            .credits
            .iter()
            .map(|(_, c)| c.seq)
            .collect();
        assert_eq!(
            records, seqs,
            "every answered entry's pending record must be durable too, stamped with its own \
             line's seq"
        );
    }
    /// The shutdown drain is a **fixpoint**: a submission accepted on
    /// the ledger stage *after* that stage's own drain loop has completed is still answered.
    ///
    /// # Why the hold is load-bearing
    ///
    /// A late submission that lands before the ledger loop's first check is drained by a
    /// single-pass drain too, so such a fixture passes on both trees. This one holds the drain
    /// inside its **pending** phase — one credit parked in the pending stage, the ledger stage's
    /// depth already zero, both asserted — and only then enqueues the late entry. That is
    /// exactly the window one pass over `[ledger, pending]` cannot cover.
    ///
    /// The late entry's own group is parked too, and released 100 ms after the pending stage, so
    /// a single-pass drain provably returns (and `abort_stages` provably runs) before the entry
    /// could have been answered by luck.
    ///
    /// Red side: the drain's two loops without the outer fixpoint loop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_58c_a_submission_arriving_after_the_ledger_drain_is_still_answered() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        let keys = keys_in_distinct_shards("fixpoint", 2);
        // One credit held in the pending stage: its ledger line is durable, the ledger stage is
        // empty, and the pending stage owes the reply.
        let pending_gate = park_pending_stage(&ledger);
        let l = std::sync::Arc::clone(&ledger);
        let k0 = keys[0].clone();
        let first =
            tokio::spawn(async move { l.append_staged_entry(entry(&k0, 0, 9, 10, 0)).await });
        pending_gate.until_parked().await;
        assert_eq!(
            ledger.stage_depths(),
            (0, 1),
            "APPARATUS: the drain must be entered with the append stage already empty and one \
             credit outstanding in the pending stage, or the late submission below is not in \
             the window a single pass misses"
        );
        let l = std::sync::Arc::clone(&ledger);
        let drain = tokio::spawn(async move { l.drain_stages(Duration::from_secs(10)).await });
        // The ledger loop's first check reads zero and exits without sleeping, so once the drain
        // has yielded at all it is inside its pending phase.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !drain.is_finished(),
            "APPARATUS: the drain returned before the late submission was made, so this fixture \
             never tested a late arrival"
        );
        let late_gate = park_group(&ledger, &ledger.shard_path_for_key(&keys[1]).unwrap());
        let l = std::sync::Arc::clone(&ledger);
        let k1 = keys[1].clone();
        let late =
            tokio::spawn(async move { l.append_staged_entry(entry(&k1, 0, 9, 10, 0)).await });
        late_gate.until_parked().await;
        let pg = std::sync::Arc::clone(&pending_gate);
        let lg = std::sync::Arc::clone(&late_gate);
        tokio::spawn(async move {
            pg.release();
            tokio::time::sleep(Duration::from_millis(100)).await;
            lg.release();
        });
        let drained = bounded("the fixpoint drain", drain).await.unwrap();
        // The runtime going away, brought forward to here.
        ledger.abort_stages();
        pending_gate.assert_released();
        late_gate.assert_released();
        bounded("the held submission", first)
            .await
            .unwrap()
            .expect("FIXTURE: the credit held in the pending stage must still succeed");
        let outcome = bounded("the late submission", late).await.unwrap();
        let on_disk = seqs_locked(&ledger.shard_path_for_key(&keys[1]).unwrap());
        outcome.unwrap_or_else(|e| {
            panic!(
                "a submission accepted after the ledger stage's own drain loop had completed \
                 was answered Err ({e}), its ledger file holding {on_disk:?}: a single pass over \
                 the two stages leaves the entry most at risk — one accepted while shutdown is \
                 already running — with the closed-channel reply the drain exists to remove"
            )
        });
        assert!(
            drained.is_drained(),
            "the fixpoint drain must leave nothing unanswered: {drained:?}"
        );
    }
    /// A dropped reply names the stage that **owed** it, taken from
    /// that submission's own hand-over state, so the attribution is exact in the case process
    /// teardown actually produces: both stages ended at once.
    ///
    /// # The two cases, and which one discriminates
    ///
    /// Case 1, handed over: the pending stage holds the credit, the ledger line is durable
    /// (asserted), and the error must name the pending-credit stage. Sender closedness gets this
    /// one right too, so it is the corroborating half.
    ///
    /// Case 2, **not** handed over: the entry's group is parked inside the append stage, so the
    /// pending stage never saw it — and the pending stage has been spawned and is ended by the
    /// same `abort_stages`, which is what makes this the both-ended case rather than one only
    /// the append stage left. Attributing from sender closedness names the pending-credit stage
    /// here, reporting a durable ledger line for an entry that may hold no line at all.
    ///
    /// Red side: `if !pair_pending || (append_ended && !pending_ended)` over the two senders,
    /// which fails case 2 and passes case 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_58c_a_dropped_reply_names_the_stage_that_owed_it() {
        let tmp = TempDir::new().unwrap();
        let ledger = std::sync::Arc::new(WriteLedger::new(
            tmp.path().to_path_buf(),
            "host:1".to_string(),
        ));
        let keys = keys_in_distinct_shards("attribution", 2);
        // Case 1. Handed over, so the pending stage owes the reply and the line is durable.
        let pending_gate = park_pending_stage(&ledger);
        let l = std::sync::Arc::clone(&ledger);
        let k0 = keys[0].clone();
        let handed =
            tokio::spawn(async move { l.append_staged_entry(entry(&k0, 0, 9, 10, 0)).await });
        pending_gate.until_parked().await;
        assert_eq!(
            ledger.stage_depths(),
            (0, 1),
            "APPARATUS: case 1's entry must have left the append stage for the pending stage"
        );
        assert!(
            !seqs_locked(&ledger.shard_path_for_key(&keys[0]).unwrap()).is_empty(),
            "APPARATUS: case 1's ledger line must be durable, or 'the line is durable and only \
             the credit is in doubt' is not the claim under test"
        );
        // Case 2. Parked inside the append stage, so it was never handed over.
        let append_gate = park_group(&ledger, &ledger.shard_path_for_key(&keys[1]).unwrap());
        let l = std::sync::Arc::clone(&ledger);
        let k1 = keys[1].clone();
        let not_handed =
            tokio::spawn(async move { l.append_staged_entry(entry(&k1, 0, 9, 10, 0)).await });
        append_gate.until_parked().await;
        // Process teardown: both stage tasks end at once, which is the case the old attribution
        // got wrong and the only case an operator reads this error in.
        ledger.abort_stages();
        let e1 = bounded("case 1's outcome", handed)
            .await
            .unwrap()
            .expect_err("APPARATUS: case 1's reply must have been dropped by the teardown");
        let e2 = bounded("case 2's outcome", not_handed)
            .await
            .unwrap()
            .expect_err("APPARATUS: case 2's reply must have been dropped by the teardown");
        pending_gate.release();
        append_gate.release();
        pending_gate.assert_released();
        append_gate.assert_released();
        assert!(
            e2.to_string().contains("write-ledger append stage ended"),
            "an entry the APPEND stage dropped before hand-over must be attributed to that \
             stage, even though the pending stage has also ended: naming the pending-credit \
             stage tells the operator the ledger line is durable and only the credit is in \
             doubt, when the line may never have been written. Got: {e2}"
        );
        assert!(
            e1.to_string()
                .contains("write-ledger pending-credit stage ended"),
            "a handed-over entry's dropped reply must be attributed to the pending-credit \
             stage, whose loss leaves a durable line with an unwritten credit. Got: {e1}"
        );
    }
}
