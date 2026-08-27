//! The Write_Ledger: staging-tier residency discovery without a filesystem walk.
//!
//! Per-instance append-only files under `cache_dir/metadata/_write_ledger/`, one JSON
//! object per line, deliberately mirroring the journal layout under
//! `metadata/_journals/` — per-instance files so appends never contend across
//! instances, merged by whoever reads.
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
//! `fs2` exclusive lock. Within a process, `append_mutex` serialises appends as the
//! journal's does.
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
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Directory under `metadata/` holding the per-instance ledger files.
const LEDGER_DIR: &str = "_write_ledger";

/// Extension for a ledger file, matched when merging across instances.
const LEDGER_EXT: &str = "ledger";

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
    /// [`WriteLedgerEntry::is_superseded_by`] a strict `>` comparison rather than `>=`.
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

/// Whether a ledger entry stamped `entry_timestamp` describes a copy that a later write has
/// since replaced.
///
/// A free function rather than a method so [`verify_staged_candidate`] — which holds a
/// [`StagedCandidate`] rather than a single entry — evaluates the *same* predicate its unit
/// test pins. An inline copy at the call site would let the two drift, leaving the test
/// asserting something production no longer does.
///
/// The ordering this relies on is established by the credit sites: the `.meta` is written
/// first, the ledger entry is appended second. So for the *current* entry the range's
/// `created_at` precedes (or at coarse clock resolution equals) the entry's timestamp, while
/// a re-PUT writes a strictly later `.meta` and therefore a strictly later `created_at`.
///
/// Strict `>` so that equal timestamps — a clock too coarse to separate the `.meta` write
/// from the append — read as "not superseded" and the entry is kept. That is the safe
/// direction: keeping a live entry costs one verification next pass, whereas dropping it
/// would make a genuinely staged object invisible to eviction until the next
/// Validation_Scan. Same asymmetry as the `Remove`-arm timestamp guard in
/// `JournalConsolidator::apply_journal_entries`.
pub fn is_superseded(entry_timestamp: SystemTime, newest_range_created_at: SystemTime) -> bool {
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
}

impl LedgerCompactionStats {
    /// Entries dropped by this pass.
    pub fn entries_dropped(&self) -> u64 {
        self.entries_before.saturating_sub(self.entries_after)
    }
}

/// Per-instance append-only record of staged (write-cached, un-graduated) ranges.
pub struct WriteLedger {
    cache_dir: PathBuf,
    instance_id: String,
    /// Serialises this process's appends, mirroring `JournalManager::append_mutex`.
    /// Without it two concurrent PUTs can interleave inside one `write` call.
    append_mutex: Mutex<()>,
}

impl WriteLedger {
    /// Create a ledger writer for `instance_id`, which must be the same
    /// `{hostname}:{pid}` identity the journal uses so an instance's files sort together
    /// by eye and dead-instance cleanup can match them.
    pub fn new(cache_dir: PathBuf, instance_id: String) -> Self {
        Self {
            cache_dir,
            instance_id,
            append_mutex: Mutex::new(()),
        }
    }

    /// `{cache_dir}/metadata/_write_ledger`.
    ///
    /// `pub(crate)` rather than `pub`: nothing outside the crate needs it, and `pub` would
    /// stop `dead_code` reporting it if the last caller ever went away.
    pub(crate) fn ledger_dir(&self) -> PathBuf {
        self.cache_dir.join("metadata").join(LEDGER_DIR)
    }

    /// This instance's ledger file.
    fn primary_path(&self) -> PathBuf {
        self.ledger_dir()
            .join(format!("{}.{}", self.instance_id, LEDGER_EXT))
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
    /// Appends are best-effort by design. A failure is logged at WARN and swallowed by
    /// the caller, because the upload has already succeeded and S3 already holds the
    /// object — failing the request over a bookkeeping write would be strictly worse
    /// than losing the hint. R2.7 is the guarantee that makes that acceptable: the entry
    /// simply becomes invisible to staging eviction until a Validation_Scan re-appends
    /// it.
    pub async fn append_staged_range(
        &self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
        compressed_size: u64,
    ) -> Result<()> {
        let entry = WriteLedgerEntry {
            timestamp: SystemTime::now(),
            cache_key: cache_key.to_string(),
            range_start,
            range_end,
            compressed_size,
            instance_id: self.instance_id.clone(),
        };
        self.append(entry).await
    }

    /// Append one entry to this instance's ledger file with a true `O_APPEND` write.
    ///
    /// See the module docs for why this does not use the journal's read-modify-write
    /// shape: a ledger file is as long as the staged set, so rewriting it per append
    /// would be O(staged) work per PUT.
    pub async fn append(&self, entry: WriteLedgerEntry) -> Result<()> {
        let dir = self.ledger_dir();
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to create write-ledger directory {:?}: {}",
                dir, e
            ))
        })?;

        // Serialise before taking any lock, as the journal does, to keep the hold short.
        let mut line = serde_json::to_string(&entry).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize write-ledger entry: {}", e))
        })?;
        line.push('\n');

        let path = self.primary_path();
        let lock_path = Self::lock_path_for(&path);

        let _guard = self.append_mutex.lock().await;

        let write_result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use fs2::FileExt;
            use std::io::Write;

            let lock_file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&lock_path)?;
            // Blocking lock, unlike the journal's `try_lock`. The only contender is
            // compaction, which holds the lock for one short rewrite, and unlike the
            // journal there is no fresh-file fallback to spill to — an append that gave
            // up here would lose the hint. Waiting is cheap and bounded.
            lock_file.lock_exclusive()?;

            let result = (|| -> std::io::Result<()> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)?;
                file.write_all(line.as_bytes())?;
                file.flush()
            })();

            let _ = fs2::FileExt::unlock(&lock_file);
            result
        })
        .await;

        match write_result {
            Ok(Ok(())) => {
                debug!(
                    "Write-ledger append: key={}, range={}-{}, size={}",
                    entry.cache_key, entry.range_start, entry.range_end, entry.compressed_size
                );
                Ok(())
            }
            Ok(Err(e)) => Err(ProxyError::CacheError(format!(
                "Failed to append write-ledger entry: {}",
                e
            ))),
            Err(e) => Err(ProxyError::CacheError(format!(
                "Write-ledger append task failed: {}",
                e
            ))),
        }
    }

    /// Every ledger file across all instances.
    async fn ledger_files(&self) -> Result<Vec<PathBuf>> {
        let dir = self.ledger_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();
        let read_dir = std::fs::read_dir(&dir).map_err(|e| {
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

    /// Read one ledger file, skipping unparseable lines at `debug!`.
    ///
    /// A malformed line is never fatal, matching every journal parse site. On a mixed
    /// version fleet that is also what keeps an unknown future field from breaking an
    /// older instance's read.
    async fn read_file(path: &Path) -> Vec<WriteLedgerEntry> {
        let content = match tokio::fs::read_to_string(path).await {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read write-ledger file {:?}: {}", path, e);
                return Vec::new();
            }
        };

        let mut entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<WriteLedgerEntry>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    debug!(
                        "Skipping unparseable write-ledger line: file={:?}, error={}",
                        path, e
                    );
                }
            }
        }
        entries
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
    pub async fn read_merged_oldest_first(&self, cap: usize) -> Result<Vec<WriteLedgerEntry>> {
        let files = self.ledger_files().await?;
        let mut all = Vec::new();
        for file in &files {
            all.extend(Self::read_file(file).await);
        }

        all.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                // Stable tiebreak so two entries stamped in the same second have a
                // deterministic order across instances rather than filesystem order.
                .then_with(|| a.cache_key.cmp(&b.cache_key))
                .then_with(|| a.range_start.cmp(&b.range_start))
        });

        if cap > 0 && all.len() > cap {
            all.truncate(cap);
        }
        Ok(all)
    }

    /// Total entries across every instance file. Backs the `ledger_entries` gauge
    /// (R8.3), which is what makes "the staging tier is not draining" visible.
    pub async fn count_entries(&self) -> u64 {
        let files = match self.ledger_files().await {
            Ok(f) => f,
            Err(e) => {
                debug!("Failed to list write-ledger files for count: {}", e);
                return 0;
            }
        };
        let mut total = 0u64;
        for file in &files {
            total += Self::read_file(file).await.len() as u64;
        }
        total
    }

    /// The set of `cache_key`s currently present in any ledger file.
    ///
    /// Used by the Validation_Scan re-append (R2.7 / R6.6) to find staged objects that
    /// no ledger knows about.
    pub async fn staged_keys(&self) -> Result<HashSet<String>> {
        let files = self.ledger_files().await?;
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
    /// than by the caller having read everything. The same inversion closes the
    /// read-then-rewrite window: an append that lands after the caller's read is not
    /// named either, so it survives, where the retain form read it in the fresh re-read
    /// below and dropped it for being absent from the set.
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
    /// interleave. The per-file lock serialises the renames but not the
    /// read-decide-rename sequences, so two passes could each drop the other's
    /// retained set under the old semantics. Naming removals instead makes the
    /// interleaving benign: the worst case is that an entry one pass already removed is
    /// named again by the other, which is a no-op.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 2.6, 3.1
    pub async fn retire_identities(
        &self,
        retire: &HashSet<(String, u64, u64, SystemTime, String)>,
    ) -> Result<LedgerCompactionStats> {
        let files = self.ledger_files().await?;
        let mut stats = LedgerCompactionStats::default();

        for path in files {
            let entries = Self::read_file(&path).await;
            stats.entries_before += entries.len() as u64;

            let kept: Vec<WriteLedgerEntry> = entries
                .into_iter()
                .filter(|e| !retire.contains(&e.identity()))
                .collect();
            stats.entries_after += kept.len() as u64;

            let lock_path = Self::lock_path_for(&path);
            let path_for_task = path.clone();
            let rewrite = tokio::task::spawn_blocking(move || -> std::io::Result<bool> {
                use fs2::FileExt;
                use std::io::Write;

                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_path)?;
                lock_file.lock_exclusive()?;

                let result = (|| -> std::io::Result<bool> {
                    if kept.is_empty() {
                        // Remove rather than truncate. Unlike the primary journal, which
                        // is truncated so appends keep a stable inode, a ledger file is
                        // recreated by the next append anyway, and removing it keeps
                        // `ledger_files()` free of empties.
                        match std::fs::remove_file(&path_for_task) {
                            Ok(()) => Ok(true),
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
                            Err(e) => Err(e),
                        }
                    } else {
                        let mut body = String::new();
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
                        Ok(false)
                    }
                })();

                let _ = fs2::FileExt::unlock(&lock_file);
                result
            })
            .await;

            match rewrite {
                Ok(Ok(true)) => stats.files_removed += 1,
                Ok(Ok(false)) => stats.files_rewritten += 1,
                Ok(Err(e)) => warn!("Failed to compact write-ledger file {:?}: {}", path, e),
                Err(e) => warn!("Write-ledger compaction task failed for {:?}: {}", path, e),
            }
        }

        if stats.entries_dropped() > 0 {
            info!(
                "Write-ledger compacted: before={}, after={}, dropped={}, rewritten={}, removed={}",
                stats.entries_before,
                stats.entries_after,
                stats.entries_dropped(),
                stats.files_rewritten,
                stats.files_removed
            );
        }
        Ok(stats)
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
                    existing.hinted_bytes =
                        existing.hinted_bytes.saturating_add(entry.compressed_size);
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
                            hinted_bytes: entry.compressed_size,
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
/// populated cache — the same reasoning as [`verify_staged_candidate`] above.
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
    /// Summed `compressed_size` of this key's entries. A hint for target arithmetic
    /// only — the evictor reconciles against what it actually freed.
    pub hinted_bytes: u64,
    /// Identities of the entries that produced this candidate, so compaction can retire
    /// exactly the entries an eviction consumed.
    pub identities: Vec<(String, u64, u64, SystemTime, String)>,
}

/// Verify a candidate against its authoritative `.meta`.
///
/// Lazy verification is what makes ledger entries safe to treat as hints, and it is one
/// decision covering every staleness case (R2.2). Free function rather than a method so
/// it can be unit-tested against a seeded cache directory without a `WriteLedger`.
pub async fn verify_staged_candidate(
    cache_dir: &Path,
    candidate: &StagedCandidate,
) -> StagedCandidateVerdict {
    let metadata_base = cache_dir.join("metadata");
    let meta_path =
        match crate::disk_cache::get_sharded_path(&metadata_base, &candidate.cache_key, ".meta") {
            Ok(p) => p,
            Err(_) => return StagedCandidateVerdict::Unreadable,
        };

    if !meta_path.exists() {
        return StagedCandidateVerdict::MetadataAbsent;
    }

    let content = match tokio::fs::read_to_string(&meta_path).await {
        Ok(c) => c,
        Err(_) => return StagedCandidateVerdict::Unreadable,
    };
    let metadata: crate::cache_types::NewCacheMetadata = match serde_json::from_str(&content) {
        Ok(m) => m,
        Err(_) => return StagedCandidateVerdict::Unreadable,
    };

    // Graduated: the first read cleared the flag and the `Graduation` journal entry has
    // already debited these bytes from `write_cache_size`. Evicting here would be
    // legitimate as read-tier eviction but is not this evictor's job, and the write-cache
    // debit would be wrong.
    if !metadata.object_metadata.is_write_cached {
        return StagedCandidateVerdict::Graduated;
    }

    // Superseded: a later write replaced the copy these entries describe. Compare
    // against the NEWEST entry for the key — comparing against the oldest would judge a
    // re-PUT's own fresh entry as stale.
    //
    // Goes through `WriteLedgerEntry::is_superseded_by` rather than repeating the
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
        return StagedCandidateVerdict::Superseded;
    }

    // An entry whose `.meta` claims no ranges has nothing to reclaim. Treat as absent
    // rather than evictable so the evictor's freed-bytes accounting cannot be credited
    // for a no-op deletion.
    if metadata.ranges.is_empty() {
        return StagedCandidateVerdict::MetadataAbsent;
    }

    StagedCandidateVerdict::Evictable
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
        assert_eq!(ledger.count_entries().await, 1);
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

        assert_eq!(ledger.count_entries().await, 50);
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
        let path = ledger.ledger_dir().join("host:1.ledger");
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

    #[tokio::test]
    async fn compaction_removes_only_named_identities_and_removes_empty_files() {
        let tmp = TempDir::new().unwrap();
        let ledger = WriteLedger::new(tmp.path().to_path_buf(), "host:1".to_string());

        let keep = entry("bucket/keep.bin", 0, 9, 10, 20);
        let drop = entry("bucket/drop.bin", 0, 9, 10, 30);
        ledger.append(keep.clone()).await.unwrap();
        ledger.append(drop.clone()).await.unwrap();
        assert_eq!(ledger.count_entries().await, 2);

        let retire: HashSet<_> = [drop.identity()].into_iter().collect();
        let stats = ledger.retire_identities(&retire).await.unwrap();
        assert_eq!(stats.entries_before, 2);
        assert_eq!(stats.entries_after, 1);
        assert_eq!(stats.entries_dropped(), 1);
        assert_eq!(stats.files_rewritten, 1);

        let remaining = ledger.read_merged_oldest_first(0).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].cache_key, "bucket/keep.bin");

        // Retiring the last entry removes the file rather than leaving an empty one.
        let retire_rest: HashSet<_> = [keep.identity()].into_iter().collect();
        let stats = ledger.retire_identities(&retire_rest).await.unwrap();
        assert_eq!(stats.files_removed, 1);
        assert_eq!(ledger.count_entries().await, 0);
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
            ledger.count_entries().await,
            2,
            "an empty retire set names nothing, so nothing may be removed"
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
            TOTAL as u64,
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
            (TOTAL - 4) as u64,
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
        assert_eq!(mine.count_entries().await, 3);

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
        assert_eq!(candidates[0].hinted_bytes, 200);
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
            verify_staged_candidate(tmp.path(), &candidate_for(&e)).await,
            StagedCandidateVerdict::Evictable
        );
    }

    /// Absent `.meta` — already evicted, or the append outlived the object.
    #[tokio::test]
    async fn verify_skips_when_metadata_is_absent() {
        let tmp = TempDir::new().unwrap();
        let e = entry("bucket/gone.bin", 0, 9, 10, 10);

        assert_eq!(
            verify_staged_candidate(tmp.path(), &candidate_for(&e)).await,
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
            verify_staged_candidate(tmp.path(), &candidate_for(&e)).await,
            StagedCandidateVerdict::Graduated
        );
    }

    /// Superseded: a re-PUT replaced the copy this entry describes, so the entry refers to
    /// bytes that are already gone. Acting on it would evict the *new* copy.
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
            verify_staged_candidate(tmp.path(), &candidate_for(&e)).await,
            StagedCandidateVerdict::Superseded
        );
    }

    /// A re-PUT's own fresh entry must NOT be judged superseded.
    ///
    /// This is why `verify_staged_candidate` compares against the candidate's **newest**
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
            verify_staged_candidate(tmp.path(), &candidate).await,
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
            verify_staged_candidate(tmp.path(), &candidate_for(&e)).await,
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
            verify_staged_candidate(tmp.path(), &candidate_for(&e)).await,
            StagedCandidateVerdict::Unreadable
        );
    }
}
